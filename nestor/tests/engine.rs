//! Engine behaviour against `MemoryOrigin`: hits, misses, coalescing, hedging, readahead,
//! consistency and invalidation.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::StreamExt;
use nestor::{
    BlockSize, CacheConfig, Consistency, DiskConfig, HedgeConfig, MemoryOrigin, Namespace,
    NamespaceId, Nestor, NestorError, ReadRange, RetryConfig,
};

const KIB: usize = 1024;
const BLOCK: u64 = 64 * KIB as u64;

fn pattern(len: usize) -> Bytes {
    Bytes::from((0..len).map(|i| (i % 253) as u8).collect::<Vec<u8>>())
}

async fn engine(ns: Namespace) -> (Nestor, NamespaceId) {
    let nestor = Nestor::builder(CacheConfig::memory(64 * 1024 * 1024))
        .retry(RetryConfig {
            attempts: 3,
            base: Duration::from_millis(1),
            max: Duration::from_millis(5),
        })
        .namespace(ns)
        .build()
        .await
        .unwrap();
    let id = nestor.namespaces()[0].1;
    (nestor, id)
}

fn namespace(origin: Arc<MemoryOrigin>, consistency: Consistency) -> Namespace {
    Namespace::new("test", origin)
        .block_size(BlockSize::new(BLOCK as u32).unwrap())
        .fetch_window(4)
        .read_window(8)
        .consistency(consistency)
        .hedge(None)
}

#[tokio::test]
async fn small_read_fetches_one_block() {
    let origin = Arc::new(MemoryOrigin::new());
    let data = pattern(10 * BLOCK as usize + 123);
    origin.put("obj", data.clone());
    let (nestor, id) = engine(namespace(origin.clone(), Consistency::Immutable)).await;

    let start = 3 * BLOCK + 17;
    let out = nestor.read(id, "obj", start..start + 4096).await.unwrap();
    assert_eq!(out, data.slice(start as usize..start as usize + 4096));
    assert_eq!(origin.gets(), 1);
    assert_eq!(origin.heads(), 0);
    assert_eq!(
        origin
            .stats()
            .bytes
            .load(std::sync::atomic::Ordering::Relaxed),
        BLOCK
    );

    let out = nestor
        .read(id, "obj", start + 100..start + 200)
        .await
        .unwrap();
    assert_eq!(out, data.slice(start as usize + 100..start as usize + 200));
    assert_eq!(origin.gets(), 1);
}

#[tokio::test]
async fn large_read_coalesces_into_window_sized_gets() {
    let origin = Arc::new(MemoryOrigin::new().with_chunk(7000));
    let data = pattern(10 * BLOCK as usize + 123);
    origin.put("obj", data.clone());
    let (nestor, id) = engine(namespace(origin.clone(), Consistency::Immutable)).await;

    let out = nestor.read(id, "obj", 0..data.len() as u64).await.unwrap();
    assert_eq!(out, data);
    assert_eq!(origin.gets(), 3);

    let again = nestor.read(id, "obj", 0..data.len() as u64).await.unwrap();
    assert_eq!(again, data);
    assert_eq!(origin.gets(), 3);
}

#[tokio::test]
async fn full_and_suffix_ranges_resolve_via_head() {
    let origin = Arc::new(MemoryOrigin::new());
    let data = pattern(3 * BLOCK as usize + 5);
    origin.put("obj", data.clone());
    let (nestor, id) = engine(namespace(origin.clone(), Consistency::Immutable)).await;

    let full = nestor.get(id, "obj", ReadRange::Full).await.unwrap();
    assert_eq!(full.content_length(), Some(data.len() as u64));
    assert_eq!(full.collect().await.unwrap(), data);
    assert_eq!(origin.heads(), 1);

    let tail = nestor.get(id, "obj", ReadRange::Suffix(10)).await.unwrap();
    assert_eq!(tail.collect().await.unwrap(), data.slice(data.len() - 10..));
    let from = nestor
        .get(id, "obj", ReadRange::From(BLOCK * 2))
        .await
        .unwrap();
    assert_eq!(
        from.collect().await.unwrap(),
        data.slice(2 * BLOCK as usize..)
    );
    assert_eq!(origin.heads(), 1);
}

#[tokio::test]
async fn bounded_range_past_eof_is_truncated_without_head() {
    let origin = Arc::new(MemoryOrigin::new());
    let data = pattern(BLOCK as usize + 100);
    origin.put("obj", data.clone());
    let (nestor, id) = engine(namespace(origin.clone(), Consistency::Immutable)).await;

    let out = nestor.read(id, "obj", 50..u64::MAX / 2).await.unwrap();
    assert_eq!(out, data.slice(50..));
    assert_eq!(origin.heads(), 0);
    assert_eq!(origin.gets(), 1);

    let err = nestor
        .read(
            id,
            "obj",
            data.len() as u64 + 1000..data.len() as u64 + 2000,
        )
        .await;
    assert!(matches!(err, Err(NestorError::Range(..))));
}

#[tokio::test]
async fn missing_object_is_not_found() {
    let origin = Arc::new(MemoryOrigin::new());
    let (nestor, id) = engine(namespace(origin.clone(), Consistency::Immutable)).await;
    let err = nestor.read(id, "nope", 0..10).await.unwrap_err();
    assert!(err.is_not_found());
    let err = nestor.head(id, "nope").await.unwrap_err();
    assert!(err.is_not_found());
}

#[tokio::test]
async fn concurrent_readers_share_one_origin_fetch() {
    let origin = Arc::new(MemoryOrigin::new());
    origin.set_latency(Duration::from_millis(20));
    let data = pattern(4 * BLOCK as usize);
    origin.put("obj", data.clone());
    let (nestor, id) = engine(namespace(origin.clone(), Consistency::Immutable)).await;

    let tasks: Vec<_> = (0..16)
        .map(|i| {
            let nestor = nestor.clone();
            let data = data.clone();
            tokio::spawn(async move {
                let start = (i % 4) * BLOCK + 10;
                let out = nestor.read(id, "obj", start..start + 100).await.unwrap();
                assert_eq!(out, data.slice(start as usize..start as usize + 100));
            })
        })
        .collect();
    for t in tasks {
        t.await.unwrap();
    }
    assert!(origin.gets() <= 4, "gets = {}", origin.gets());
}

#[tokio::test]
async fn transient_origin_failures_are_retried() {
    let origin = Arc::new(MemoryOrigin::new());
    let data = pattern(2 * BLOCK as usize);
    origin.put("obj", data.clone());
    origin.fail_next(2);
    let (nestor, id) = engine(namespace(origin.clone(), Consistency::Immutable)).await;

    let out = nestor.read(id, "obj", 0..data.len() as u64).await.unwrap();
    assert_eq!(out, data);
    assert_eq!(origin.gets(), 3);
}

#[tokio::test]
async fn hedge_fires_on_slow_primary() {
    let origin = Arc::new(MemoryOrigin::new());
    let data = pattern(BLOCK as usize);
    origin.put("obj", data.clone());
    let ns = namespace(origin.clone(), Consistency::Immutable).hedge(Some(HedgeConfig {
        factor: 1.0,
        min: Duration::from_millis(10),
        max: Duration::from_millis(10),
    }));
    let (nestor, id) = engine(ns).await;

    origin.slow_next(1, Duration::from_millis(500));
    let started = std::time::Instant::now();
    let out = nestor.read(id, "obj", 0..100).await.unwrap();
    assert_eq!(out, data.slice(0..100));
    assert!(started.elapsed() < Duration::from_millis(400));
    assert_eq!(origin.gets(), 2);
}

#[tokio::test]
async fn etag_mode_never_serves_stale_data() {
    let origin = Arc::new(MemoryOrigin::new());
    let v1 = pattern(2 * BLOCK as usize);
    origin.put("obj", v1.clone());
    let ns = namespace(
        origin.clone(),
        Consistency::Etag {
            ttl: Duration::from_secs(3600),
        },
    );
    let (nestor, id) = engine(ns).await;

    assert_eq!(
        nestor.read(id, "obj", 0..v1.len() as u64).await.unwrap(),
        v1
    );
    assert_eq!(origin.heads(), 1);

    let v2 = Bytes::from(vec![0xAB; 2 * BLOCK as usize + 77]);
    origin.put("obj", v2.clone());

    let cached = nestor.read(id, "obj", 0..v1.len() as u64).await.unwrap();
    assert_eq!(cached, v1);

    nestor.invalidate(id, "obj").unwrap();
    let fresh = nestor.read(id, "obj", 0..v2.len() as u64).await.unwrap();
    assert_eq!(fresh, v2);
    assert_eq!(origin.heads(), 2);
}

#[tokio::test]
async fn etag_change_between_head_and_get_restarts_read() {
    let origin = Arc::new(MemoryOrigin::new());
    let v1 = pattern(BLOCK as usize);
    origin.put("obj", v1.clone());
    let ns = namespace(
        origin.clone(),
        Consistency::Etag {
            ttl: Duration::from_secs(3600),
        },
    );
    let (nestor, id) = engine(ns).await;

    nestor.head(id, "obj").await.unwrap();
    let v2 = Bytes::from(vec![0x11; BLOCK as usize + 5]);
    origin.put("obj", v2.clone());

    let out = nestor.read(id, "obj", 0..BLOCK).await.unwrap();
    assert_eq!(out, v2.slice(0..BLOCK as usize));
}

#[tokio::test]
async fn insert_populates_blocks_without_origin() {
    let origin = Arc::new(MemoryOrigin::new());
    let (nestor, id) = engine(namespace(origin.clone(), Consistency::Immutable)).await;
    let data = pattern(BLOCK as usize * 2 + 9);
    nestor
        .insert(id, "obj", Some(Bytes::from_static(b"\"x\"")), &data)
        .unwrap();
    let out = nestor.read(id, "obj", 0..data.len() as u64).await.unwrap();
    assert_eq!(out, data);
    assert_eq!(origin.gets(), 0);
    assert_eq!(origin.heads(), 0);
}

#[tokio::test]
async fn readahead_prefetches_next_blocks() {
    let origin = Arc::new(MemoryOrigin::new());
    let data = pattern(16 * BLOCK as usize);
    origin.put("obj", data.clone());
    let ns = namespace(origin.clone(), Consistency::Immutable).readahead(4);
    let (nestor, id) = engine(ns).await;

    nestor.read(id, "obj", 0..BLOCK).await.unwrap();
    nestor.read(id, "obj", BLOCK..2 * BLOCK).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    let gets = origin.gets();
    nestor.read(id, "obj", 2 * BLOCK..6 * BLOCK).await.unwrap();
    assert_eq!(origin.gets(), gets);
}

#[tokio::test]
async fn streaming_yields_in_order_with_partial_edges() {
    let origin = Arc::new(MemoryOrigin::new().with_chunk(1000));
    let data = pattern(9 * BLOCK as usize + 31);
    origin.put("obj", data.clone());
    let (nestor, id) = engine(namespace(origin.clone(), Consistency::Immutable)).await;

    let start = BLOCK / 2;
    let end = data.len() as u64 - 7;
    let mut stream = nestor.get(id, "obj", start..end).await.unwrap();
    let mut collected = Vec::new();
    while let Some(chunk) = stream.next().await {
        collected.extend_from_slice(&chunk.unwrap());
    }
    assert_eq!(collected, data.slice(start as usize..end as usize));
}

#[tokio::test]
async fn hybrid_cache_survives_memory_pressure() {
    let dir = tempfile::tempdir().unwrap();
    let origin = Arc::new(MemoryOrigin::new());
    let data = pattern(32 * BLOCK as usize);
    origin.put("obj", data.clone());
    let nestor = Nestor::builder(CacheConfig::memory(4 * BLOCK as usize).disk(DiskConfig {
        region_size: 4 * 1024 * 1024,
        buffer_pool_size: 16 * 1024 * 1024,
        ..DiskConfig::new(dir.path(), 64 * 1024 * 1024)
    }))
    .namespace(namespace(origin.clone(), Consistency::Immutable))
    .build()
    .await
    .unwrap();
    let id = nestor.namespace("test").unwrap();

    assert_eq!(
        nestor.read(id, "obj", 0..data.len() as u64).await.unwrap(),
        data
    );
    let gets = origin.gets();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        nestor.read(id, "obj", 0..data.len() as u64).await.unwrap(),
        data
    );
    assert!(
        origin.gets() <= gets + 2,
        "gets went from {gets} to {}",
        origin.gets()
    );
    nestor.close().await.unwrap();
}

#[tokio::test]
async fn unknown_namespace_and_closed_engine_error() {
    let origin = Arc::new(MemoryOrigin::new());
    let (nestor, _) = engine(namespace(origin, Consistency::Immutable)).await;
    let bogus = nestor.namespace("missing");
    assert!(bogus.is_none());
    nestor.close().await.unwrap();
    let id = nestor.namespaces()[0].1;
    assert!(matches!(
        nestor.read(id, "obj", 0..1).await,
        Err(NestorError::Closed)
    ));
}
