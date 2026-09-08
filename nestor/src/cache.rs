//! Foyer setup. `CacheConfig` describes the RAM tier and optional disk tier for blocks,
//! `object_lru` is the small in-memory LRU used for per-object bookkeeping.

use std::path::PathBuf;

use bytes::Bytes;
use foyer::{
    BlockEngineConfig, Cache, CacheBuilder, CacheProperties, Compression, DeviceBuilder,
    FsDeviceBuilder, HybridCache, HybridCacheBuilder, HybridCachePolicy, LruConfig, RecoverMode,
    S3FifoConfig,
};
use mixtrics::metrics::BoxedRegistry;

use crate::key::{BlockKey, ObjectKey};

pub type BlockCache = HybridCache<BlockKey, Bytes>;

const MIB: usize = 1024 * 1024;
/// Each RAM shard evicts on its own, so a shard has to hold a good number of blocks.
const MIN_SHARD_BYTES: usize = 32 * MIB;
const MAX_BUFFER_POOL: usize = 256 * MIB;

#[derive(Debug, Clone)]
pub struct DiskConfig {
    pub path: PathBuf,
    pub capacity: usize,
    pub region_size: usize,
    pub flushers: usize,
    pub reclaimers: usize,
    /// Write buffer shared by the flushers, derived from capacity and region size when unset.
    pub buffer_pool_size: Option<usize>,
    pub direct_io: bool,
    pub compression: Compression,
    pub recover: RecoverMode,
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
        }
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

/// Two shards per core for contention, capped so no shard falls under `MIN_SHARD_BYTES`.
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
        .with_weighter(|key: &BlockKey, value: &Bytes| {
            value.len() + key.object.len() + std::mem::size_of::<BlockKey>()
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

    let storage = storage
        .with_engine_config(engine)
        .with_recover_mode(disk.recover)
        .with_compression(disk.compression);

    storage.with_io_engine_config(io_engine()).build().await
}

/// io_uring when the kernel and the container's seccomp profile allow it, psync otherwise.
#[cfg(target_os = "linux")]
fn io_engine() -> Box<dyn foyer::IoEngineConfig> {
    match io_uring::IoUring::new(2) {
        Ok(_) => Box::new(foyer::UringIoEngineConfig::new()),
        Err(e) => {
            tracing::warn!(error = %e, "io_uring unavailable, disk tier uses psync");
            Box::new(foyer::PsyncIoEngineConfig::new())
        }
    }
}

#[cfg(not(target_os = "linux"))]
fn io_engine() -> Box<dyn foyer::IoEngineConfig> {
    Box::new(foyer::PsyncIoEngineConfig::new())
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
    use super::{CacheConfig, DiskConfig, MAX_BUFFER_POOL, MIB, MIN_SHARD_BYTES};

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
}
