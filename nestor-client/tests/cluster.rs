//! Runs several in process nestor nodes over one shared origin and reads through the cluster client.

use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::ops::Range;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use futures::TryStreamExt;
use http::Uri;
use nestor::{
    BlockSize, CacheConfig, GetOptions, GetResponse, HedgeConfig, Namespace, NamespaceConfig,
    Nestor, ObjectMeta, Origin, OriginError, ReadRange,
};
use nestor_client::{Cluster, ClusterConfig, ClusterOrigin, Membership};
use nestor_s3::{Addressing, Auth, OriginConfig, Origins, S3Config, S3Error, S3Service};
use nestor_store::ObjectStoreOrigin;
use object_store::ObjectStoreExt;
use object_store::memory::InMemory;
use object_store::path::Path;
use tokio::net::TcpListener;

const BLOCK: u32 = 64 * 1024;
const BLOCK_LEN: u64 = BLOCK as u64;
const BLOCK_USIZE: usize = BLOCK as usize;
const BUCKET: &str = "data";

#[derive(Clone)]
struct NodeOrigin {
    inner: Arc<ObjectStoreOrigin>,
    requests: Arc<AtomicUsize>,
    ranges: Arc<Mutex<Vec<Range<u64>>>>,
    delay: Duration,
}

impl NodeOrigin {
    fn new(store: &Arc<InMemory>, delay: Duration) -> Self {
        Self {
            inner: Arc::new(ObjectStoreOrigin::new(Arc::clone(store) as _)),
            requests: Arc::new(AtomicUsize::new(0)),
            ranges: Arc::new(Mutex::new(Vec::new())),
            delay,
        }
    }

    fn blocks_seen(&self) -> BTreeSet<u32> {
        let bs = BlockSize::new(BLOCK).unwrap();
        self.ranges
            .lock()
            .unwrap()
            .iter()
            .flat_map(|r| bs.blocks(r))
            .collect()
    }
}

#[async_trait]
impl Origin for NodeOrigin {
    async fn get(&self, object: &str, options: GetOptions) -> Result<GetResponse, OriginError> {
        tokio::time::sleep(self.delay).await;
        self.requests.fetch_add(1, Ordering::Relaxed);
        if let Some(range) = &options.range {
            self.ranges.lock().unwrap().push(range.clone());
        }
        self.inner.get(object, options).await
    }

    async fn head(&self, object: &str) -> Result<ObjectMeta, OriginError> {
        tokio::time::sleep(self.delay).await;
        self.requests.fetch_add(1, Ordering::Relaxed);
        self.inner.head(object).await
    }
}

impl Origins for NodeOrigin {
    fn origin(&self, _bucket: &str) -> Result<Arc<dyn Origin>, S3Error> {
        Ok(Arc::new(self.clone()))
    }
}

struct Nodes {
    store: Arc<InMemory>,
    addrs: Vec<SocketAddr>,
    origins: Vec<NodeOrigin>,
}

impl Nodes {
    async fn start(delays: &[Duration]) -> Self {
        let store = Arc::new(InMemory::new());
        let mut addrs = Vec::new();
        let mut origins = Vec::new();
        for delay in delays {
            let origin = NodeOrigin::new(&store, *delay);
            let nestor = Nestor::builder(CacheConfig::memory(64 << 20))
                .build()
                .await
                .unwrap();
            let config = S3Config {
                origin: OriginConfig::anonymous(Uri::from_static("http://127.0.0.1:1"), "cluster"),
                origins: Some(Arc::new(origin.clone())),
                auth: Auth::Anonymous,
                addressing: Addressing::Path,
                buckets: NamespaceConfig::default()
                    .block_size(BlockSize::new(BLOCK).unwrap())
                    .hedge(None)
                    .readahead(0),
                populate_max: None,
            };
            let router = S3Service::new(nestor, config).router();
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            addrs.push(listener.local_addr().unwrap());
            tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
            origins.push(origin);
        }
        Self {
            store,
            addrs,
            origins,
        }
    }

    async fn put(&self, key: &str, len: usize) -> Bytes {
        let body: Bytes = (0..len).map(|i| (i % 251) as u8).collect();
        self.store
            .put(&Path::from(key), body.clone().into())
            .await
            .unwrap();
        body
    }

    async fn cluster(&self, config: ClusterConfig) -> ClusterOrigin {
        let cluster = Cluster::new(Membership::Static(self.addrs.clone()), config)
            .await
            .unwrap();
        cluster.origin(BUCKET)
    }

    fn origin_requests(&self) -> usize {
        self.origins
            .iter()
            .map(|o| o.requests.load(Ordering::Relaxed))
            .sum()
    }
}

fn config() -> ClusterConfig {
    ClusterConfig {
        block_size: BlockSize::new(BLOCK).unwrap(),
        hedge: None,
        ..ClusterConfig::default()
    }
}

async fn collect(response: GetResponse) -> Bytes {
    let chunks: Vec<Bytes> = response.body.try_collect().await.unwrap();
    chunks.concat().into()
}

async fn read(origin: &ClusterOrigin, key: &str, range: Option<Range<u64>>) -> (Range<u64>, Bytes) {
    let response = origin
        .get(
            key,
            GetOptions {
                range,
                ..GetOptions::default()
            },
        )
        .await
        .unwrap();
    let range = response.range.clone();
    (range, collect(response).await)
}

#[tokio::test(flavor = "multi_thread")]
async fn routed_reads_are_byte_exact() {
    let nodes = Nodes::start(&[Duration::ZERO; 3]).await;
    let len = 5 * BLOCK_USIZE + 12_345;
    let body = nodes.put("obj", len).await;
    let origin = nodes.cluster(config()).await;

    let (range, bytes) = read(&origin, "obj", None).await;
    assert_eq!(range, 0..len as u64);
    assert_eq!(bytes, body);

    let ranges = [
        0..1,
        (BLOCK_LEN - 1)..(BLOCK_LEN + 1),
        1000..(3 * BLOCK_LEN + 17),
        (4 * BLOCK_LEN)..(len as u64),
        (len as u64 - 5)..(len as u64 + 1_000_000),
    ];
    for r in ranges {
        let (got, bytes) = read(&origin, "obj", Some(r.clone())).await;
        let expected = r.start..r.end.min(len as u64);
        assert_eq!(got, expected, "{r:?}");
        assert_eq!(
            bytes,
            body.slice(expected.start as usize..expected.end as usize),
            "{r:?}"
        );
    }

    let meta = origin.head("obj").await.unwrap();
    assert_eq!(meta.size, len as u64);
    assert!(matches!(
        origin.head("missing").await,
        Err(OriginError::NotFound)
    ));
}

#[tokio::test(flavor = "multi_thread")]
async fn every_block_has_exactly_one_owner() {
    let nodes = Nodes::start(&[Duration::ZERO; 3]).await;
    let blocks = 64u32;
    nodes.put("wide", blocks as usize * BLOCK_USIZE).await;
    let origin = nodes.cluster(config()).await;
    read(&origin, "wide", None).await;

    let seen: Vec<BTreeSet<u32>> = nodes.origins.iter().map(NodeOrigin::blocks_seen).collect();
    let total: usize = seen.iter().map(BTreeSet::len).sum();
    let union: BTreeSet<u32> = seen.iter().flatten().copied().collect();
    assert_eq!(union.len(), blocks as usize);
    assert_eq!(
        total, blocks as usize,
        "a block was fetched by more than one node"
    );
    assert!(seen.iter().filter(|s| !s.is_empty()).count() >= 2);

    let before = nodes.origin_requests();
    read(&origin, "wide", None).await;
    assert_eq!(
        nodes.origin_requests(),
        before,
        "second read should be served from node caches"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn local_nestor_reads_through_the_cluster() {
    let nodes = Nodes::start(&[Duration::ZERO; 3]).await;
    let len = 9 * BLOCK_USIZE + 999;
    let body = nodes.put("tiered", len).await;
    let cluster_origin = nodes
        .cluster(ClusterConfig {
            block_size: BlockSize::new(2 * BLOCK).unwrap(),
            ..config()
        })
        .await;

    let local = Nestor::builder(CacheConfig::memory(16 << 20))
        .namespace(
            Namespace::new(BUCKET, Arc::new(cluster_origin))
                .block_size(BlockSize::new(BLOCK).unwrap())
                .fetch_window(3)
                .hedge(None),
        )
        .build()
        .await
        .unwrap();
    let ns = local.namespace(BUCKET).unwrap();

    let full = local.read(ns, "tiered", 0..len as u64).await.unwrap();
    assert_eq!(full, body);
    let middle = local
        .read(ns, "tiered", (BLOCK_LEN + 7)..(6 * BLOCK_LEN - 3))
        .await
        .unwrap();
    assert_eq!(middle, body.slice(BLOCK_USIZE + 7..6 * BLOCK_USIZE - 3));
    let stream = local
        .get(ns, "tiered", ReadRange::Suffix(100))
        .await
        .unwrap();
    let tail: Vec<Bytes> = stream.try_collect().await.unwrap();
    assert_eq!(tail.concat(), body[len - 100..]);
}

#[tokio::test(flavor = "multi_thread")]
async fn hedge_beats_a_slow_node() {
    let nodes = Nodes::start(&[Duration::from_secs(2), Duration::ZERO]).await;
    nodes.put("hedged", 8 * BLOCK_USIZE).await;
    let origin = nodes
        .cluster(ClusterConfig {
            hedge: Some(HedgeConfig {
                factor: 2.0,
                min: Duration::from_millis(30),
                max: Duration::from_millis(60),
            }),
            ..config()
        })
        .await;

    let started = Instant::now();
    read(&origin, "hedged", None).await;
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "{:?}",
        started.elapsed()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn warm_makes_owners_hot() {
    let nodes = Nodes::start(&[Duration::ZERO; 3]).await;
    let len = 6 * BLOCK_USIZE;
    nodes.put("warm", len).await;
    let origin = nodes.cluster(config()).await;

    origin.warm("warm", len as u64).await.unwrap();
    let union: BTreeSet<u32> = nodes
        .origins
        .iter()
        .flat_map(NodeOrigin::blocks_seen)
        .collect();
    assert_eq!(union, (0..6).collect());

    let before = nodes.origin_requests();
    read(&origin, "warm", None).await;
    assert_eq!(nodes.origin_requests(), before);
}

#[tokio::test(flavor = "multi_thread")]
async fn dead_node_fails_over() {
    let nodes = Nodes::start(&[Duration::ZERO; 2]).await;
    let len = 16 * BLOCK_USIZE;
    let body = nodes.put("failover", len).await;
    let dead = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let dead_addr = dead.local_addr().unwrap();
    drop(dead);

    let mut addrs = nodes.addrs.clone();
    addrs.push(dead_addr);
    let cluster = Cluster::new(Membership::Static(addrs), config())
        .await
        .unwrap();
    assert_eq!(cluster.node_count(), 3);
    let origin = cluster.origin(BUCKET);

    let (_, bytes) = read(&origin, "failover", None).await;
    assert_eq!(bytes, body);
    let (_, bytes) = read(&origin, "failover", None).await;
    assert_eq!(bytes, body);
}
