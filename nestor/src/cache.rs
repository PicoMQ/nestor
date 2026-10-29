//! Foyer setup. `CacheConfig` describes the RAM tier and optional disk tier for blocks. `Blocks`
//! owns the hybrid cache and its disk admission. Under `DiskAdmission::Reused` it counts the reads
//! delivered for each block in a bounded history, writes a block to disk at its second read, and
//! foyer's admission filter keeps every other block off disk. `lru` builds the small in-memory LRUs
//! used for bookkeeping: object metadata, readahead positions and read counts.

use std::hash::Hash;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};

use foyer::{
    BlockEngineConfig, Cache, CacheBuilder, CacheProperties, Compression, DeviceBuilder,
    FsDeviceBuilder, HybridCache, HybridCacheBuilder, HybridCachePolicy, LruConfig, RecoverMode,
    S3FifoConfig, Source, Statistics, StorageFilter, StorageFilterCondition, StorageFilterResult,
};
use mixtrics::metrics::BoxedRegistry;

use crate::block::BlockSize;
use crate::key::{Block, BlockKey};

const MIB: usize = 1024 * 1024;
const REUSED_READS: u8 = 2;
const MIN_LRU_SHARD_ENTRIES: usize = 1024;
const MAX_LRU_SHARDS: usize = 8;
const MIN_SHARD_BYTES: usize = 32 * MIB;
const MAX_BUFFER_POOL: usize = 256 * MIB;
#[cfg(target_os = "linux")]
const URING_DEPTH: u32 = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[cfg_attr(
    feature = "serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "snake_case")
)]
pub enum DiskIo {
    #[default]
    Auto,
    Uring,
    Psync,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[cfg_attr(
    feature = "serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "snake_case")
)]
pub enum DiskAdmission {
    #[default]
    Fetched,
    Evicted,
    Reused,
}

#[derive(Debug, Clone)]
pub struct DiskConfig {
    pub path: PathBuf,
    pub capacity: usize,
    pub admission: DiskAdmission,
    pub region_size: usize,
    pub flushers: usize,
    pub reclaimers: usize,
    pub buffer_pool_size: Option<usize>,
    pub direct_io: bool,
    pub compression: Compression,
    pub recover: RecoverMode,
    pub io: DiskIo,
    pub runtime_threads: Option<usize>,
}

impl DiskConfig {
    pub fn new(path: impl Into<PathBuf>, capacity: usize) -> Self {
        Self {
            path: path.into(),
            capacity,
            admission: DiskAdmission::Fetched,
            region_size: 64 * MIB,
            flushers: 2,
            reclaimers: 2,
            buffer_pool_size: None,
            direct_io: true,
            compression: Compression::None,
            recover: RecoverMode::Quiet,
            io: DiskIo::Auto,
            runtime_threads: None,
        }
    }

    pub fn runtime_threads(&self) -> usize {
        self.runtime_threads
            .unwrap_or(self.flushers + self.reclaimers)
    }

    pub fn buffer_pool(&self) -> usize {
        self.buffer_pool_size.unwrap_or_else(|| {
            (self.capacity / 16)
                .min(MAX_BUFFER_POOL)
                .max(self.flushers * self.region_size)
        })
    }
}

#[derive(Debug, Clone)]
pub struct CacheConfig {
    pub memory: usize,
    pub shards: usize,
    pub disk: Option<DiskConfig>,
}

impl CacheConfig {
    pub fn memory(memory: usize) -> Self {
        Self {
            memory,
            shards: default_shards(memory),
            disk: None,
        }
    }

    pub fn disk(mut self, disk: DiskConfig) -> Self {
        self.disk = Some(disk);
        self
    }

    pub fn shards(mut self, shards: usize) -> Self {
        self.shards = shards.max(1);
        self
    }
}

fn default_shards(memory: usize) -> usize {
    let cores = std::thread::available_parallelism().map_or(8, |n| n.get());
    (cores * 2).min(memory / MIN_SHARD_BYTES).max(1)
}

#[derive(Clone)]
pub(crate) struct Blocks {
    cache: HybridCache<BlockKey, Block>,
    reads: Option<Reads>,
}

impl Blocks {
    pub async fn open(
        config: &CacheConfig,
        metrics: Option<BoxedRegistry>,
        smallest: BlockSize,
    ) -> foyer::Result<Self> {
        let reused = config
            .disk
            .as_ref()
            .is_some_and(|disk| disk.admission == DiskAdmission::Reused);
        let reads = reused.then(|| Reads::new(tracked(config.memory, smallest)));

        Ok(Self {
            cache: hybrid(config, metrics, reads.clone()).await?,
            reads,
        })
    }

    pub fn track(&self, block_size: BlockSize) {
        if let Some(reads) = &self.reads {
            reads.fit(tracked(self.cache.memory().capacity(), block_size));
        }
    }

    pub async fn get(&self, key: &BlockKey) -> Option<Block> {
        let entry = self.cache.get(key).await.ok()??;

        if let Some(reads) = &self.reads
            && entry.source() == Source::Disk
        {
            reads.keep(self.cache.memory().hash(key));
        }
        Some(entry.value().clone())
    }

    pub fn contains(&self, key: &BlockKey) -> bool {
        self.cache.contains(key)
    }

    pub fn insert(&self, key: BlockKey, block: Block) {
        self.cache.insert(key, block);
    }

    pub fn record_read(&self, key: &BlockKey, block: &Block) -> bool {
        let Some(reads) = &self.reads else {
            return false;
        };
        if reads.record(self.cache.memory().hash(key)) != REUSED_READS {
            return false;
        }

        let cached = self
            .cache
            .memory()
            .get(key)
            .is_some_and(|entry| entry.value().data.as_ptr() == block.data.as_ptr());
        if cached {
            self.cache.storage_writer(key.clone()).insert(block.clone());
        }
        cached
    }

    pub fn remove(&self, key: &BlockKey) {
        if let Some(reads) = &self.reads {
            reads.forget(self.cache.memory().hash(key));
        }
        self.cache.remove(key);
    }

    pub fn memory_usage(&self) -> usize {
        self.cache.memory().usage()
    }

    pub fn memory_capacity(&self) -> usize {
        self.cache.memory().capacity()
    }

    pub fn has_disk(&self) -> bool {
        self.cache.is_hybrid()
    }

    pub async fn close(&self) -> foyer::Result<()> {
        self.cache.close().await
    }
}

#[derive(Clone)]
struct Reads(Lru<u64, AtomicU8>);

impl Reads {
    fn new(capacity: usize) -> Self {
        Self(lru("nestor-reads", capacity))
    }

    fn fit(&self, capacity: usize) {
        if capacity > self.0.capacity()
            && let Err(e) = self.0.resize(capacity)
        {
            tracing::warn!(error = %e, capacity, "read counts could not grow");
        }
    }

    fn record(&self, hash: u64) -> u8 {
        let Some(entry) = self.0.get(&hash) else {
            self.0.insert(hash, AtomicU8::new(1));
            return 1;
        };

        match entry
            .value()
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |reads| {
                reads.checked_add(1)
            }) {
            Ok(previous) => previous + 1,
            Err(saturated) => saturated,
        }
    }

    fn keep(&self, hash: u64) {
        match self.0.get(&hash) {
            Some(entry) => {
                entry.value().fetch_max(REUSED_READS, Ordering::Relaxed);
            }
            None => {
                self.0.insert(hash, AtomicU8::new(REUSED_READS));
            }
        }
    }

    fn count(&self, hash: u64) -> u8 {
        self.0
            .get(&hash)
            .map_or(0, |entry| entry.value().load(Ordering::Relaxed))
    }

    fn forget(&self, hash: u64) {
        self.0.remove(&hash);
    }
}

impl std::fmt::Debug for Reads {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Reads").finish_non_exhaustive()
    }
}

fn tracked(memory: usize, block_size: BlockSize) -> usize {
    2 * memory / block_size.usize()
}

#[derive(Debug)]
struct Reused(Reads);

impl StorageFilterCondition for Reused {
    fn filter(&self, _: &Arc<Statistics>, hash: u64, _: usize) -> StorageFilterResult {
        if self.0.count(hash) >= REUSED_READS {
            StorageFilterResult::Admit
        } else {
            StorageFilterResult::Reject
        }
    }
}

async fn hybrid(
    config: &CacheConfig,
    metrics: Option<BoxedRegistry>,
    reads: Option<Reads>,
) -> foyer::Result<HybridCache<BlockKey, Block>> {
    let mut builder = HybridCacheBuilder::new()
        .with_name("nestor")
        .with_policy(policy(config.disk.as_ref().map(|disk| disk.admission)));
    if let Some(registry) = metrics {
        builder = builder.with_metrics_registry(registry);
    }
    let memory = builder
        .memory(config.memory)
        .with_shards(config.shards)
        .with_eviction_config(S3FifoConfig::default())
        .with_weighter(|key: &BlockKey, block: &Block| {
            block.weight() + key.object.len() + std::mem::size_of::<BlockKey>()
        });

    let storage = memory.storage();
    let Some(disk) = &config.disk else {
        return storage.build().await;
    };

    let device = FsDeviceBuilder::new(&disk.path).with_capacity(disk.capacity);
    #[cfg(target_os = "linux")]
    let device = device.with_direct(disk.direct_io);
    let device = device.build()?;

    let mut engine = BlockEngineConfig::new(device)
        .with_block_size(disk.region_size)
        .with_flushers(disk.flushers)
        .with_reclaimers(disk.reclaimers)
        .with_buffer_pool_size(disk.buffer_pool())
        .with_indexer_shards(config.shards.max(64));
    if let Some(reads) = reads {
        engine = engine.with_admission_filter(StorageFilter::new().with_condition(Reused(reads)));
    }

    let threads = disk.runtime_threads();
    if threads == 0 {
        return Err(foyer::Error::new(
            foyer::ErrorKind::Config,
            "disk runtime_threads must be at least 1",
        ));
    }
    let next_thread = Arc::new(AtomicUsize::new(0));
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(threads)
        .thread_name_fn(move || format!("foyer-{}", next_thread.fetch_add(1, Ordering::Relaxed)))
        .enable_all()
        .build()
        .map_err(foyer::Error::io_error)?;

    storage
        .with_engine_config(engine)
        .with_recover_mode(disk.recover)
        .with_compression(disk.compression)
        .with_spawner(runtime.into())
        .with_io_engine_config(io_engine(disk.io)?)
        .build()
        .await
}

fn policy(admission: Option<DiskAdmission>) -> HybridCachePolicy {
    match admission {
        Some(DiskAdmission::Fetched | DiskAdmission::Reused) => HybridCachePolicy::WriteOnInsertion,
        Some(DiskAdmission::Evicted) | None => HybridCachePolicy::WriteOnEviction,
    }
}

fn io_engine(io: DiskIo) -> foyer::Result<Box<dyn foyer::IoEngineConfig>> {
    match io {
        DiskIo::Psync => Ok(Box::new(foyer::PsyncIoEngineConfig::new())),
        DiskIo::Uring => uring_engine(true),
        DiskIo::Auto => uring_engine(false),
    }
}

#[cfg(target_os = "linux")]
fn uring_engine(required: bool) -> foyer::Result<Box<dyn foyer::IoEngineConfig>> {
    match io_uring::IoUring::new(URING_DEPTH) {
        Ok(_) => Ok(Box::new(foyer::UringIoEngineConfig::new())),
        Err(e) if required => Err(foyer::Error::new(
            foyer::ErrorKind::Config,
            format!("io_uring unavailable: {e}"),
        )),
        Err(e) => {
            tracing::warn!(error = %e, "io_uring unavailable, disk tier uses psync");
            Ok(Box::new(foyer::PsyncIoEngineConfig::new()))
        }
    }
}

#[cfg(not(target_os = "linux"))]
fn uring_engine(required: bool) -> foyer::Result<Box<dyn foyer::IoEngineConfig>> {
    if required {
        Err(foyer::Error::new(
            foyer::ErrorKind::Config,
            "io_uring requires Linux",
        ))
    } else {
        Ok(Box::new(foyer::PsyncIoEngineConfig::new()))
    }
}

pub(crate) type Lru<K, V> = Cache<K, V, ahash::RandomState, CacheProperties>;

pub(crate) fn lru<K, V>(name: &'static str, capacity: usize) -> Lru<K, V>
where
    K: Hash + Eq + Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    let capacity = capacity.max(1);

    CacheBuilder::new(capacity)
        .with_name(name)
        .with_shards((capacity / MIN_LRU_SHARD_ENTRIES).clamp(1, MAX_LRU_SHARDS))
        .with_hash_builder(ahash::RandomState::new())
        .with_eviction_config(LruConfig::default())
        .with_weighter(|_: &K, _: &V| 1)
        .build()
}

#[cfg(test)]
mod tests {
    use foyer::HybridCachePolicy;

    use super::{
        CacheConfig, DiskAdmission, DiskConfig, DiskIo, Lru, MAX_BUFFER_POOL, MIB, MIN_SHARD_BYTES,
        lru, policy, tracked,
    };
    use crate::block::BlockSize;

    #[test]
    fn shards_never_fall_under_the_minimum_size() {
        assert_eq!(CacheConfig::memory(MIB).shards, 1);
        let config = CacheConfig::memory(64 * MIN_SHARD_BYTES);
        assert!(config.memory / config.shards >= MIN_SHARD_BYTES);
        assert_eq!(CacheConfig::memory(MIB).shards(0).shards, 1);
    }

    #[test]
    fn buffer_pool_follows_capacity_within_bounds() {
        let mut small = DiskConfig::new("/tmp", 256 * MIB);
        small.region_size = 16 * MIB;
        assert_eq!(small.buffer_pool(), 2 * 16 * MIB);
        assert_eq!(DiskConfig::new("/tmp", 4096 * MIB).buffer_pool(), 256 * MIB);
        assert_eq!(
            DiskConfig::new("/tmp", 64 * 1024 * MIB).buffer_pool(),
            MAX_BUFFER_POOL
        );
        let mut pinned = DiskConfig::new("/tmp", 256 * MIB);
        pinned.buffer_pool_size = Some(MIB);
        assert_eq!(pinned.buffer_pool(), MIB);
    }

    #[test]
    fn a_small_lru_holds_its_capacity() {
        let cache: Lru<u64, u64> = lru("test", 8);
        for key in 0..8 {
            cache.insert(key, key);
        }
        assert!((0..8).all(|key| cache.get(&key).is_some()));
    }

    #[test]
    fn read_counts_cover_twice_the_blocks_ram_holds() {
        let block_size = BlockSize::new(MIB as u32).unwrap();
        assert_eq!(tracked(1024 * MIB, block_size), 2048);
    }

    #[test]
    fn admission_sets_the_foyer_policy() {
        assert_eq!(
            policy(Some(DiskAdmission::Fetched)),
            HybridCachePolicy::WriteOnInsertion
        );
        assert_eq!(
            policy(Some(DiskAdmission::Evicted)),
            HybridCachePolicy::WriteOnEviction
        );
        assert_eq!(
            policy(Some(DiskAdmission::Reused)),
            HybridCachePolicy::WriteOnInsertion
        );
        assert_eq!(
            DiskConfig::new("/tmp", MIB).admission,
            DiskAdmission::Fetched
        );
    }

    #[test]
    fn runtime_threads_follow_flushers_and_reclaimers_unless_set() {
        let mut disk = DiskConfig::new("/tmp", MIB);
        assert_eq!(disk.io, DiskIo::Auto);
        assert_eq!(disk.runtime_threads(), 4);
        disk.flushers = 6;
        assert_eq!(disk.runtime_threads(), 8);
        disk.runtime_threads = Some(2);
        assert_eq!(disk.runtime_threads(), 2);
    }
}
