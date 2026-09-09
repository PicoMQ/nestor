//! Targets: nestor embedded in the bench process, a nestor binary over S3, or the origin proxy alone.

use std::ops::Range;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use eyre::WrapErr;
use futures::StreamExt;
use futures::stream::BoxStream;
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use nestor::{
    BlockSize, CacheConfig, Consistency, DiskConfig, FetchPolicy, Namespace, NamespaceId, Nestor,
};
use nestor_e2e::metrics::Metrics;
use nestor_store::{NestorStore, ObjectStoreOrigin};
use object_store::path::Path;
use object_store::{GetOptions, GetRange, ObjectStore, ObjectStoreExt, PutPayload};
use serde::{Deserialize, Serialize};
use tokio::process::Command;

pub struct Read {
    pub first_byte: Duration,
    pub stream: BoxStream<'static, eyre::Result<Bytes>>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NestorCounters {
    pub hits: u64,
    pub misses: u64,
    pub joined: u64,
    pub stale: u64,
    pub origin_requests: u64,
    pub origin_bytes: u64,
    pub retries: u64,
    pub hedges: u64,
    pub hedge_wins: u64,
    pub heads: u64,
}

impl NestorCounters {
    fn parse(text: &str) -> Self {
        let m = Metrics::parse(text);
        Self {
            hits: m.counter("nestor_blocks_hit_total"),
            misses: m.counter("nestor_blocks_miss_total"),
            joined: m.counter("nestor_blocks_joined_total"),
            stale: m.counter("nestor_blocks_stale_total"),
            origin_requests: m.counter("nestor_origin_requests_total"),
            origin_bytes: m.counter("nestor_origin_bytes_total"),
            retries: m.counter("nestor_origin_retries_total"),
            hedges: m.counter("nestor_hedges_total"),
            hedge_wins: m.counter("nestor_hedge_wins_total"),
            heads: m.counter("nestor_meta_heads_total"),
        }
    }

    pub fn delta(self, since: Self) -> Self {
        Self {
            hits: self.hits - since.hits,
            misses: self.misses - since.misses,
            joined: self.joined - since.joined,
            stale: self.stale - since.stale,
            origin_requests: self.origin_requests - since.origin_requests,
            origin_bytes: self.origin_bytes - since.origin_bytes,
            retries: self.retries - since.retries,
            hedges: self.hedges - since.hedges,
            hedge_wins: self.hedge_wins - since.hedge_wins,
            heads: self.heads - since.heads,
        }
    }
}

#[async_trait]
pub trait Target: Send + Sync {
    fn name(&self) -> &str;

    async fn read(&self, key: &str, range: Range<u64>) -> eyre::Result<Read>;

    async fn write(&self, key: &str, data: Bytes) -> eyre::Result<()>;

    async fn delete(&self, key: &str) -> eyre::Result<()>;

    async fn nestor(&self) -> Option<NestorCounters>;

    async fn rss_bytes(&self) -> Option<u64>;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LibraryConfig {
    pub memory: usize,
    pub disk_path: Option<PathBuf>,
    pub disk_capacity: usize,
    pub block: u32,
    pub fetch_window: u32,
    pub read_window: u32,
    pub readahead: u32,
    pub fetch: FetchPolicy,
    pub immutable: bool,
}

pub struct Library {
    nestor: Nestor,
    ns: NamespaceId,
    store: NestorStore,
    metrics: PrometheusHandle,
}

impl Library {
    pub async fn start(config: &LibraryConfig, origin: Arc<dyn ObjectStore>) -> eyre::Result<Self> {
        let metrics = PrometheusBuilder::new()
            .install_recorder()
            .wrap_err("install metrics recorder")?;
        let mut cache = CacheConfig::memory(config.memory);
        if let Some(path) = &config.disk_path {
            std::fs::create_dir_all(path)?;
            cache = cache.disk(DiskConfig::new(path, config.disk_capacity));
        }
        let consistency = if config.immutable {
            Consistency::Immutable
        } else {
            Consistency::Etag {
                ttl: Duration::from_secs(60),
            }
        };
        let block = BlockSize::new(config.block)
            .ok_or_else(|| eyre::eyre!("block size {} out of range", config.block))?;
        let namespace = Namespace::new(
            "bench",
            Arc::new(ObjectStoreOrigin::new(Arc::clone(&origin))),
        )
        .block_size(block)
        .fetch_window(config.fetch_window)
        .read_window(config.read_window)
        .readahead(config.readahead)
        .consistency(consistency)
        .fetch(config.fetch);
        let nestor = Nestor::builder(cache).namespace(namespace).build().await?;
        let ns = nestor.namespace("bench").expect("registered");
        let store = NestorStore::new(nestor.clone(), ns, origin);
        Ok(Self {
            nestor,
            ns,
            store,
            metrics,
        })
    }

    pub async fn close(&self) -> eyre::Result<()> {
        self.nestor.close().await?;
        Ok(())
    }
}

#[async_trait]
impl Target for Library {
    fn name(&self) -> &'static str {
        "library"
    }

    async fn read(&self, key: &str, range: Range<u64>) -> eyre::Result<Read> {
        let started = Instant::now();
        let mut stream = self.nestor.get(self.ns, key, range).await?;
        stream.ready().await?;
        Ok(Read {
            first_byte: started.elapsed(),
            stream: stream
                .map(|chunk| chunk.map_err(eyre::Report::from))
                .boxed(),
        })
    }

    async fn write(&self, key: &str, data: Bytes) -> eyre::Result<()> {
        self.store
            .put(&Path::from(key), PutPayload::from_bytes(data))
            .await?;
        Ok(())
    }

    async fn delete(&self, key: &str) -> eyre::Result<()> {
        self.store.delete(&Path::from(key)).await?;
        Ok(())
    }

    async fn nestor(&self) -> Option<NestorCounters> {
        Some(NestorCounters::parse(&self.metrics.render()))
    }

    async fn rss_bytes(&self) -> Option<u64> {
        memory_stats::memory_stats().map(|m| m.physical_mem as u64)
    }
}

pub struct Endpoint {
    name: String,
    store: Arc<dyn ObjectStore>,
    metrics_url: Option<String>,
    container: Option<String>,
}

impl Endpoint {
    pub fn new(
        name: impl Into<String>,
        store: Arc<dyn ObjectStore>,
        metrics_url: Option<String>,
        container: Option<String>,
    ) -> Self {
        Self {
            name: name.into(),
            store,
            metrics_url,
            container,
        }
    }
}

#[async_trait]
impl Target for Endpoint {
    fn name(&self) -> &str {
        &self.name
    }

    async fn read(&self, key: &str, range: Range<u64>) -> eyre::Result<Read> {
        let started = Instant::now();
        let result = self
            .store
            .get_opts(
                &Path::from(key),
                GetOptions {
                    range: Some(GetRange::Bounded(range)),
                    ..GetOptions::default()
                },
            )
            .await?;
        Ok(Read {
            first_byte: started.elapsed(),
            stream: result
                .into_stream()
                .map(|chunk| chunk.map_err(eyre::Report::from))
                .boxed(),
        })
    }

    async fn write(&self, key: &str, data: Bytes) -> eyre::Result<()> {
        self.store
            .put(&Path::from(key), PutPayload::from_bytes(data))
            .await?;
        Ok(())
    }

    async fn delete(&self, key: &str) -> eyre::Result<()> {
        self.store.delete(&Path::from(key)).await?;
        Ok(())
    }

    async fn nestor(&self) -> Option<NestorCounters> {
        let url = self.metrics_url.as_ref()?;
        let text = reqwest::get(format!("{url}/metrics"))
            .await
            .ok()?
            .text()
            .await
            .ok()?;
        Some(NestorCounters::parse(&text))
    }

    async fn rss_bytes(&self) -> Option<u64> {
        let container = self.container.as_ref()?;
        let output = Command::new("docker")
            .args([
                "stats",
                "--no-stream",
                "--format",
                "{{.MemUsage}}",
                container,
            ])
            .output()
            .await
            .ok()?;
        let text = String::from_utf8_lossy(&output.stdout);
        let used = text.split('/').next()?.trim();
        byte_unit::Byte::parse_str(used, true)
            .ok()
            .map(|b| b.as_u64())
    }
}
