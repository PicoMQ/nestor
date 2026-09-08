//! Three nodes behind one DNS name and a gateway routing across them. Ownership is checked on the
//! node metrics endpoints, and a node is stopped and started to exercise failover and refresh.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use nestor_e2e::compose::Compose;
use nestor_e2e::data::{MIB, body, payload, slice};
use nestor_e2e::metrics::Metrics;
use nestor_e2e::{env, init, s3, step, wait};
use object_store::ObjectStoreExt;
use object_store::aws::AmazonS3;
use object_store::path::Path;

const ORIGIN_BYTES: &str = "nestor_origin_bytes_total";

struct Stack {
    origin: Arc<AmazonS3>,
    gateway: Arc<AmazonS3>,
    nodes: Vec<String>,
}

impl Stack {
    async fn connect() -> Self {
        init();
        let rustfs = env("NESTOR_E2E_RUSTFS", "http://127.0.0.1:19000");
        let gateway = env("NESTOR_E2E_GATEWAY", "http://127.0.0.1:19001");
        let nodes: Vec<String> = (1..=3)
            .map(|i| {
                env(
                    &format!("NESTOR_E2E_NODE{i}"),
                    &format!("http://127.0.0.1:1910{i}"),
                )
            })
            .collect();
        wait::healthy(&format!("{gateway}/-/health"), Duration::from_secs(120)).await;
        Self {
            origin: s3::client(&rustfs),
            gateway: s3::client(&gateway),
            nodes,
        }
    }

    async fn node_origin_bytes(&self) -> Vec<u64> {
        let mut bytes = Vec::with_capacity(self.nodes.len());
        for node in &self.nodes {
            bytes.push(Metrics::scrape(node).await.counter(ORIGIN_BYTES));
        }
        bytes
    }

    async fn read(&self, key: &Path) -> Bytes {
        body(self.gateway.get(key).await.expect("gateway get")).await
    }
}

#[tokio::test]
#[ignore = "needs the cluster compose stack"]
async fn blocks_are_spread_across_nodes_and_fetched_once() {
    let stack = Stack::connect().await;
    let key = Path::from("cluster/wide");
    let data = payload(64 * MIB, 64);
    stack
        .origin
        .put(&key, data.clone().into())
        .await
        .expect("put");

    let before = stack.node_origin_bytes().await;
    assert_eq!(stack.read(&key).await, data);
    let after = stack.node_origin_bytes().await;
    let fetched: Vec<u64> = before.iter().zip(&after).map(|(b, a)| a - b).collect();
    step!(?fetched, "origin bytes fetched per node");
    assert!(
        fetched.iter().all(|f| *f > 0),
        "every node should own some blocks"
    );
    let total: u64 = fetched.iter().sum();
    assert_eq!(
        total,
        (64 * MIB) as u64,
        "each byte should be fetched from the origin once"
    );

    assert_eq!(stack.read(&key).await, data);
    assert_eq!(
        stack.node_origin_bytes().await,
        after,
        "second read is served from node caches"
    );
    step!("second read served by the nodes");

    let boundary = (2 * MIB as u64 - 100)..(2 * MIB as u64 + 100);
    let got = stack
        .gateway
        .get_range(&key, boundary.clone())
        .await
        .expect("boundary range");
    assert_eq!(got, slice(&data, &boundary));
    step!("range across a routing boundary matches");
}

#[tokio::test]
#[ignore = "needs the cluster compose stack"]
async fn writes_warm_the_owning_nodes() {
    let stack = Stack::connect().await;
    let key = Path::from("cluster/warm");
    let data = payload(8 * MIB, 8);
    let before: u64 = stack.node_origin_bytes().await.iter().sum();
    stack
        .gateway
        .put(&key, data.clone().into())
        .await
        .expect("put through gateway");
    step!("PUT forwarded to origin, warming spawned");

    wait::until("owners to warm", Duration::from_secs(20), || async {
        let now: u64 = stack.node_origin_bytes().await.iter().sum();
        now - before >= (8 * MIB) as u64
    })
    .await;
    let warmed: u64 = stack.node_origin_bytes().await.iter().sum();
    step!(bytes = warmed - before, "nodes warmed before any read");

    assert_eq!(stack.read(&key).await, data);
    let after_read: u64 = stack.node_origin_bytes().await.iter().sum();
    assert_eq!(
        after_read, warmed,
        "read after warming needs no origin traffic"
    );
}

#[tokio::test]
#[ignore = "needs the cluster compose stack"]
async fn a_stopped_node_is_routed_around_and_rejoins() {
    let stack = Stack::connect().await;
    let compose = Compose::for_scenario("cluster");
    let objects: Vec<(Path, Bytes)> = (0..4)
        .map(|i| {
            (
                Path::from(format!("cluster/rejoin-{i}")),
                payload(16 * MIB, 100 + i),
            )
        })
        .collect();
    for (key, data) in &objects {
        stack
            .origin
            .put(key, data.clone().into())
            .await
            .expect("put");
    }

    compose.stop("node2").await;
    step!("node2 stopped");
    assert_eq!(stack.read(&objects[0].0).await, objects[0].1);
    assert_eq!(stack.read(&objects[1].0).await, objects[1].1);
    step!("reads succeed with node2 down");

    compose.start("node2").await;
    wait::healthy(
        &format!("{}/metrics", stack.nodes[1]),
        Duration::from_secs(60),
    )
    .await;
    step!("node2 back, waiting for the gateway to pick it up");
    let node2_before = stack.node_origin_bytes().await[1];
    wait::until(
        "node2 to receive traffic",
        Duration::from_secs(30),
        || async {
            for (key, data) in &objects[2..] {
                assert_eq!(&stack.read(key).await, data);
            }
            stack.node_origin_bytes().await[1] > node2_before
        },
    )
    .await;
    step!("node2 owns blocks again");
}
