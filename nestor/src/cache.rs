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

#[derive(Debug, Clone)]
pub struct DiskConfig {
    pub path: PathBuf,
    pub capacity: usize,
    pub region_size: usize,
    pub flushers: usize,
    pub reclaimers: usize,
    pub buffer_pool_size: usize,
    pub direct_io: bool,
    pub compression: Compression,
    pub recover: RecoverMode,
}

impl DiskConfig {
    pub fn new(path: impl Into<PathBuf>, capacity: usize) -> Self {
        Self {
            path: path.into(),
            capacity,
            region_size: 64 * 1024 * 1024,
            flushers: 2,
            reclaimers: 2,
            buffer_pool_size: 256 * 1024 * 1024,
            direct_io: true,
            compression: Compression::None,
            recover: RecoverMode::Quiet,
        }
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
            shards: default_shards(),
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

fn default_shards() -> usize {
    std::thread::available_parallelism()
        .map_or(16, |n| n.get() * 2)
        .next_power_of_two()
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
        .with_buffer_pool_size(disk.buffer_pool_size)
        .with_indexer_shards(config.shards.max(64));

    let storage = storage
        .with_engine_config(engine)
        .with_recover_mode(disk.recover)
        .with_compression(disk.compression);

    #[cfg(target_os = "linux")]
    let io_engine: Box<dyn foyer::IoEngineConfig> = Box::new(foyer::UringIoEngineConfig::new());
    #[cfg(not(target_os = "linux"))]
    let io_engine: Box<dyn foyer::IoEngineConfig> = Box::new(foyer::PsyncIoEngineConfig::new());
    let storage = storage.with_io_engine_config(io_engine);

    storage.build().await
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
