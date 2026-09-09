//! Foyer setup. `CacheConfig` describes the RAM tier and optional disk tier for blocks,
//! `object_lru` is the small in-memory LRU used for per-object bookkeeping.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use foyer::{
    BlockEngineConfig, Cache, CacheBuilder, CacheProperties, Compression, DeviceBuilder,
    FsDeviceBuilder, HybridCache, HybridCacheBuilder, HybridCachePolicy, LruConfig, RecoverMode,
    S3FifoConfig,
};
use mixtrics::metrics::BoxedRegistry;

use crate::key::{Block, BlockKey, ObjectKey};

pub type BlockCache = HybridCache<BlockKey, Block>;

const MIB: usize = 1024 * 1024;
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

#[derive(Debug, Clone)]
pub struct DiskConfig {
    pub path: PathBuf,
    pub capacity: usize,
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

pub async fn build(
    config: &CacheConfig,
    metrics: Option<BoxedRegistry>,
) -> foyer::Result<BlockCache> {
    let policy = if config.disk.is_some() {
        HybridCachePolicy::WriteOnInsertion
    } else {
        HybridCachePolicy::WriteOnEviction
    };
    let mut builder = HybridCacheBuilder::new()
        .with_name("nestor")
        .with_policy(policy);
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

    let engine = BlockEngineConfig::new(device)
        .with_block_size(disk.region_size)
        .with_flushers(disk.flushers)
        .with_reclaimers(disk.reclaimers)
        .with_buffer_pool_size(disk.buffer_pool())
        .with_indexer_shards(config.shards.max(64));

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

pub(crate) type ObjectCache<V> = Cache<ObjectKey, V, ahash::RandomState, CacheProperties>;

pub(crate) fn object_lru<V: Send + Sync + 'static>(
    name: &'static str,
    capacity: usize,
) -> ObjectCache<V> {
    CacheBuilder::new(capacity.max(1))
        .with_name(name)
        .with_hash_builder(ahash::RandomState::new())
        .with_eviction_config(LruConfig::default())
        .with_weighter(|_: &ObjectKey, _: &V| 1)
        .build()
}

#[cfg(test)]
mod tests {
    use super::{CacheConfig, DiskConfig, DiskIo, MAX_BUFFER_POOL, MIB, MIN_SHARD_BYTES};

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
