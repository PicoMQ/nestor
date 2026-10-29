//! Engine behaviour against `MemoryOrigin`: hits, misses, coalescing, hedging, readahead,
//! consistency and invalidation.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use bytes::Bytes;
use futures::StreamExt;
use nestor::{
    BlockSize, CacheConfig, Consistency, DiskAdmission, DiskConfig, FetchOverrides, FetchPolicy,
    GetOptions, GetResponse, HedgeConfig, MemoryOrigin, Namespace, NamespaceId, Nestor,
    NestorError, ObjectMeta, Origin, OriginError, ReadOptions, ReadRange,
};

const KIB: usize = 1024;
const BLOCK: u64 = 64 * KIB as u64;

fn pattern(len: usize) -> Bytes {
    Bytes::from((0..len).map(|i| (i % 253) as u8).collect::<Vec<u8>>())
}

async fn engine(ns: Namespace) -> (Nestor, NamespaceId) {
    let nestor = Nestor::builder(CacheConfig::memory(64 * 1024 * 1024))
        .namespace(ns)
        .build()
        .await
        .unwrap();
    let id = nestor.namespaces()[0].1;
    (nestor, id)
}

fn namespace(origin: Arc<dyn Origin>, consistency: Consistency) -> Namespace {
    Namespace::new("test", origin)
        .block_size(BlockSize::new(BLOCK as u32).unwrap())
        .fetch_window(4)
        .read_window(8)
        .consistency(consistency)
        .fetch(
            FetchPolicy::default()
                .hedge(None)
                .backoff(Duration::from_millis(1), Duration::from_millis(5)),
        )
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
async fn full_range_learns_size_from_its_first_get() {
    let origin = Arc::new(MemoryOrigin::new());
    let data = pattern(3 * BLOCK as usize + 5);
    origin.put("obj", data.clone());
    let (nestor, id) = engine(namespace(origin.clone(), Consistency::Immutable)).await;

    let full = nestor.get(id, "obj", ReadRange::Full).await.unwrap();
    assert_eq!(full.content_length(), Some(data.len() as u64));
    assert_eq!(full.collect().await.unwrap(), data);
    assert_eq!(origin.heads(), 0);
    assert_eq!(origin.gets(), 1);

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
    assert_eq!(origin.heads(), 0);
    assert_eq!(origin.gets(), 1);
}

#[tokio::test]
async fn suffix_on_an_unknown_object_needs_a_head() {
    let origin = Arc::new(MemoryOrigin::new());
    let data = pattern(BLOCK as usize + 5);
    origin.put("obj", data.clone());
    let (nestor, id) = engine(namespace(origin.clone(), Consistency::Immutable)).await;

    let tail = nestor.get(id, "obj", ReadRange::Suffix(10)).await.unwrap();
    assert_eq!(tail.collect().await.unwrap(), data.slice(data.len() - 10..));
    assert_eq!(origin.heads(), 1);
}

#[tokio::test]
async fn expired_meta_revalidates_with_a_conditional_get() {
    let origin = Arc::new(MemoryOrigin::new());
    let data = pattern(2 * BLOCK as usize);
    origin.put("obj", data.clone());
    let ns = namespace(
        origin.clone(),
        Consistency::Etag {
            ttl: Duration::from_millis(20),
        },
    );
    let (nestor, id) = engine(ns).await;

    assert_eq!(
        nestor.read(id, "obj", 0..BLOCK).await.unwrap(),
        data.slice(..BLOCK as usize)
    );
    let bytes = || {
        origin
            .stats()
            .bytes
            .load(std::sync::atomic::Ordering::Relaxed)
    };
    let fetched = bytes();
    tokio::time::sleep(Duration::from_millis(40)).await;

    assert_eq!(
        nestor.read(id, "obj", 0..BLOCK).await.unwrap(),
        data.slice(..BLOCK as usize)
    );
    assert_eq!(origin.gets(), 2, "one conditional GET to revalidate");
    assert_eq!(bytes(), fetched, "an unchanged object moves no bytes");
    assert_eq!(origin.heads(), 0);

    let v2 = Bytes::from(vec![0xCD; 2 * BLOCK as usize]);
    origin.put("obj", v2.clone());
    tokio::time::sleep(Duration::from_millis(40)).await;
    assert_eq!(
        nestor.read(id, "obj", 0..BLOCK).await.unwrap(),
        v2.slice(..BLOCK as usize)
    );
    assert_eq!(
        origin.gets(),
        3,
        "the revalidating GET carries the new version"
    );
    assert_eq!(origin.heads(), 0);
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
async fn failed_get_of_unknown_size_is_settled_by_a_head() {
    let origin = Arc::new(MemoryOrigin::new());
    let data = pattern(BLOCK as usize + 100);
    origin.put("obj", data.clone());
    let (nestor, id) = engine(namespace(origin.clone(), Consistency::Immutable)).await;

    let err = nestor.read(id, "obj", 5 * BLOCK..6 * BLOCK).await;
    assert!(matches!(err, Err(NestorError::Range(..))));
    assert_eq!(origin.gets(), 1);
    assert_eq!(origin.heads(), 1);

    let second = Arc::new(MemoryOrigin::new());
    second.put("obj", data.clone());
    let (nestor, id) = engine(namespace(second.clone(), Consistency::Immutable)).await;
    second.fail_next(1);
    let out = nestor.read(id, "obj", 10..20).await.unwrap();
    assert_eq!(out, data.slice(10..20));
    assert_eq!(second.gets(), 2);
    assert_eq!(second.heads(), 1);
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
    assert_eq!(origin.gets(), 2);
    assert_eq!(origin.heads(), 2);
}

#[tokio::test]
async fn hedge_fires_on_slow_primary() {
    let origin = Arc::new(MemoryOrigin::new());
    let data = pattern(BLOCK as usize);
    origin.put("obj", data.clone());
    let mut ns = namespace(origin.clone(), Consistency::Immutable);
    ns.config.fetch.hedge = Some(
        HedgeConfig::factor(1.0, Duration::from_millis(10), Duration::from_millis(10)).unwrap(),
    );
    let (nestor, id) = engine(ns).await;

    origin.slow_next(1, Duration::from_millis(500));
    let started = std::time::Instant::now();
    let out = nestor.read(id, "obj", 0..100).await.unwrap();
    assert_eq!(out, data.slice(0..100));
    assert!(started.elapsed() < Duration::from_millis(400));
    assert_eq!(origin.gets(), 2);
}

#[tokio::test]
async fn quantile_hedge_fires_after_a_fast_history() {
    let origin = Arc::new(MemoryOrigin::new());
    let data = pattern(BLOCK as usize);
    for i in 0..6 {
        origin.put(format!("obj{i}"), data.clone());
    }
    let mut ns = namespace(origin.clone(), Consistency::Immutable);
    ns.config.fetch.hedge = Some(
        HedgeConfig::quantile(0.99, Duration::from_millis(5), Duration::from_secs(1)).unwrap(),
    );
    let (nestor, id) = engine(ns).await;

    for i in 0..5 {
        nestor.read(id, &format!("obj{i}"), 0..100).await.unwrap();
    }
    assert_eq!(origin.gets(), 5);

    origin.slow_next(1, Duration::from_millis(500));
    let started = std::time::Instant::now();
    let out = nestor.read(id, "obj5", 0..100).await.unwrap();
    assert_eq!(out, data.slice(0..100));
    assert!(started.elapsed() < Duration::from_millis(400));
    assert_eq!(origin.gets(), 7);
}

#[tokio::test]
async fn hedge_takes_over_when_the_primary_fails() {
    let origin = Arc::new(MemoryOrigin::new());
    let data = pattern(BLOCK as usize);
    origin.put("obj", data.clone());
    let mut ns = namespace(origin.clone(), Consistency::Immutable);
    ns.config.fetch.hedge = Some(
        HedgeConfig::factor(1.0, Duration::from_millis(20), Duration::from_millis(20)).unwrap(),
    );
    let (nestor, id) = engine(ns).await;

    origin.set_latency(Duration::from_millis(300));
    origin.fail_next(1);
    let started = std::time::Instant::now();
    let out = nestor.read(id, "obj", 0..100).await.unwrap();
    assert_eq!(out, data.slice(0..100));
    assert!(
        started.elapsed() < Duration::from_millis(500),
        "{:?}",
        started.elapsed()
    );
    assert_eq!(origin.gets(), 2);
}

#[tokio::test]
async fn body_hedge_fetches_only_the_remaining_range() {
    let origin = Arc::new(MemoryOrigin::new().with_chunk(BLOCK as usize));
    let data = pattern(4 * BLOCK as usize);
    origin.put("obj", data.clone());
    let mut ns = namespace(origin.clone(), Consistency::Immutable);
    ns.config.fetch.hedge = Some(
        HedgeConfig::factor(1.0, Duration::from_millis(20), Duration::from_millis(20)).unwrap(),
    );
    let (nestor, id) = engine(ns).await;

    origin.stall_next(1, Duration::from_millis(500));
    let started = std::time::Instant::now();
    let out = nestor.read(id, "obj", 0..data.len() as u64).await.unwrap();
    assert_eq!(out, data);
    assert!(
        started.elapsed() < Duration::from_millis(400),
        "{:?}",
        started.elapsed()
    );
    assert_eq!(origin.gets(), 2);
    assert_eq!(
        origin
            .stats()
            .bytes
            .load(std::sync::atomic::Ordering::Relaxed),
        4 * BLOCK + 3 * BLOCK
    );
}

#[tokio::test]
async fn disabled_hedge_waits_out_a_stalled_body() {
    let origin = Arc::new(MemoryOrigin::new().with_chunk(BLOCK as usize));
    let data = pattern(2 * BLOCK as usize);
    origin.put("obj", data.clone());
    let (nestor, id) = engine(namespace(origin.clone(), Consistency::Immutable)).await;

    origin.stall_next(1, Duration::from_millis(100));
    let started = std::time::Instant::now();
    let out = nestor.read(id, "obj", 0..data.len() as u64).await.unwrap();
    assert_eq!(out, data);
    assert!(started.elapsed() >= Duration::from_millis(100));
    assert_eq!(origin.gets(), 1);
}

fn is_timeout(err: &NestorError) -> bool {
    matches!(err, NestorError::Origin(OriginError::Timeout(_)))
}

#[tokio::test]
async fn first_byte_timeout_retries_then_fails() {
    let origin = Arc::new(MemoryOrigin::new());
    origin.put("obj", pattern(BLOCK as usize));
    let mut ns = namespace(origin.clone(), Consistency::Immutable);
    ns.config.fetch = ns
        .config
        .fetch
        .attempts(2)
        .first_byte(Duration::from_millis(20));
    let (nestor, id) = engine(ns).await;

    origin.slow_next(usize::MAX, Duration::from_secs(5));
    let started = std::time::Instant::now();
    let err = nestor.read(id, "obj", 0..100).await.unwrap_err();
    assert!(is_timeout(&err), "{err}");
    assert!(started.elapsed() < Duration::from_millis(500));
    assert_eq!(origin.gets(), 2);
}

#[tokio::test]
async fn deadline_caps_the_retry_budget() {
    let origin = Arc::new(MemoryOrigin::new());
    origin.put("obj", pattern(BLOCK as usize));
    let mut ns = namespace(origin.clone(), Consistency::Immutable);
    ns.config.fetch = ns
        .config
        .fetch
        .attempts(100)
        .first_byte(Duration::from_millis(10))
        .deadline(Duration::from_millis(60));
    let (nestor, id) = engine(ns).await;

    origin.slow_next(usize::MAX, Duration::from_secs(5));
    let err = nestor.read(id, "obj", 0..100).await.unwrap_err();
    assert!(is_timeout(&err), "{err}");
    assert!(origin.gets() < 100);
}

#[tokio::test]
async fn read_overrides_loosen_the_namespace_policy() {
    let origin = Arc::new(MemoryOrigin::new());
    let data = pattern(BLOCK as usize);
    origin.put("obj", data.clone());
    let mut ns = namespace(origin.clone(), Consistency::Immutable);
    ns.config.fetch = ns
        .config
        .fetch
        .attempts(1)
        .first_byte(Duration::from_millis(20));
    let (nestor, id) = engine(ns).await;

    origin.slow_next(usize::MAX, Duration::from_millis(80));
    let err = nestor.read(id, "obj", 0..100).await.unwrap_err();
    assert!(is_timeout(&err), "{err}");

    let options = ReadOptions::range(0..100).fetch(FetchOverrides {
        first_byte: Some(Duration::from_secs(1)),
        ..FetchOverrides::default()
    });
    let stream = nestor.get_opts(id, "obj", options).await.unwrap();
    assert_eq!(stream.collect().await.unwrap(), data.slice(0..100));
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
    assert_eq!(origin.heads(), 0);
    assert_eq!(origin.gets(), 1);

    let v2 = Bytes::from(vec![0xAB; 2 * BLOCK as usize + 77]);
    origin.put("obj", v2.clone());

    let cached = nestor.read(id, "obj", 0..v1.len() as u64).await.unwrap();
    assert_eq!(cached, v1);

    nestor.invalidate(id, "obj").unwrap();
    let fresh = nestor.read(id, "obj", 0..v2.len() as u64).await.unwrap();
    assert_eq!(fresh, v2);
    assert_eq!(origin.heads(), 0);
    assert_eq!(origin.gets(), 2);
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
async fn restart_serves_recovered_blocks_with_metadata() {
    let dir = tempfile::tempdir().unwrap();
    let data = pattern(6 * BLOCK as usize + 99);
    let disk = || DiskConfig {
        region_size: 4 * 1024 * 1024,
        direct_io: false,
        ..DiskConfig::new(dir.path(), 64 * 1024 * 1024)
    };

    let origin = Arc::new(MemoryOrigin::new());
    let etag = origin.put("obj", data.clone());
    let first = Nestor::builder(CacheConfig::memory(64 * 1024 * 1024).disk(disk()))
        .namespace(namespace(origin.clone(), Consistency::Immutable))
        .build()
        .await
        .unwrap();
    let id = first.namespace("test").unwrap();
    assert_eq!(
        first.read(id, "obj", 0..data.len() as u64).await.unwrap(),
        data
    );
    first.close().await.unwrap();

    let offline = Arc::new(MemoryOrigin::new());
    offline.fail_next(usize::MAX);
    let second = Nestor::builder(CacheConfig::memory(64 * 1024 * 1024).disk(disk()))
        .namespace(namespace(offline.clone(), Consistency::Immutable))
        .build()
        .await
        .unwrap();
    let id = second.namespace("test").unwrap();

    let mut stream = second
        .get(id, "obj", 2 * BLOCK + 7..3 * BLOCK)
        .await
        .unwrap();
    let meta = stream.ready().await.unwrap();
    assert_eq!(meta.size, data.len() as u64);
    assert_eq!(meta.etag.as_ref(), Some(&etag));
    assert_eq!(
        stream.collect().await.unwrap(),
        data.slice(2 * BLOCK as usize + 7..3 * BLOCK as usize)
    );
    assert_eq!(offline.gets(), 0);
    assert_eq!(offline.heads(), 0);
    second.close().await.unwrap();
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn faulty_origin_never_truncates_a_read() {
    let origin = Arc::new(MemoryOrigin::new());
    let objects: Vec<Bytes> = (0..8)
        .map(|i| pattern(5 * BLOCK as usize + 1000 * i + 17))
        .collect();
    for (i, data) in objects.iter().enumerate() {
        origin.put(format!("obj{i}"), data.clone());
    }
    let mut ns = namespace(origin.clone(), Consistency::Immutable);
    ns.config.fetch = ns.config.fetch.attempts(6).hedge(Some(
        HedgeConfig::factor(1.0, Duration::from_millis(1), Duration::from_millis(2)).unwrap(),
    ));
    ns.config.fetch_window = 2;
    let (nestor, id) = engine(ns).await;
    origin.set_latency(Duration::from_millis(3));
    origin.fail_every(7);

    let tasks: Vec<_> = (0..16u64)
        .map(|reader| {
            let nestor = nestor.clone();
            let objects = objects.clone();
            tokio::spawn(async move {
                for step in 0..200u64 {
                    let x = (reader * 7919 + step * 104_729) % 1_000_003;
                    let i = (x % 8) as usize;
                    let len = objects[i].len() as u64;
                    let start = (x * 31) % (len - 1);
                    let end = (start + 1 + (x * 17) % (2 * BLOCK)).min(len);
                    match nestor.read(id, &format!("obj{i}"), start..end).await {
                        Ok(out) => assert_eq!(out, objects[i].slice(start as usize..end as usize)),
                        Err(NestorError::Origin(_)) => {}
                        Err(e) => panic!("{e}"),
                    }
                }
            })
        })
        .collect();
    for task in tasks {
        task.await.unwrap();
    }
}

#[tokio::test]
async fn probe_and_head_are_retried() {
    let origin = Arc::new(MemoryOrigin::new());
    let data = pattern(3 * BLOCK as usize);
    origin.put("obj", data.clone());
    let (nestor, id) = engine(namespace(origin.clone(), Consistency::Immutable)).await;

    origin.fail_next(1);
    let stream = nestor
        .get(id, "obj", ReadRange::From(BLOCK + 5))
        .await
        .unwrap();
    let out = stream.collect().await.unwrap();
    assert_eq!(out, data.slice(BLOCK as usize + 5..));
    assert_eq!(origin.gets(), 2);
    assert_eq!(origin.heads(), 1);

    let second = Arc::new(MemoryOrigin::new());
    second.put("obj", data.clone());
    let (nestor, id) = engine(namespace(second.clone(), Consistency::Immutable)).await;
    second.fail_next(1);
    assert_eq!(
        nestor.head(id, "obj").await.unwrap().size,
        data.len() as u64
    );
    assert_eq!(second.heads(), 2);
}

#[tokio::test]
async fn snapshot_reports_live_cache_state() {
    let origin = Arc::new(MemoryOrigin::new());
    let data = pattern(BLOCK as usize);
    origin.put("obj", data.clone());
    let (nestor, id) = engine(namespace(origin, Consistency::Immutable)).await;

    nestor.insert(id, "obj", None, &data).unwrap();
    let snap = nestor.snapshot();
    assert_eq!(snap.namespaces.len(), 1);
    assert_eq!(snap.namespaces[0].name.as_ref(), "test");
    assert_eq!(snap.namespaces[0].id, id);
    assert_eq!(snap.namespaces[0].config.block_size.bytes(), BLOCK);
    assert_eq!(snap.cache.memory_cap, 64 * 1024 * 1024);
    assert!(snap.cache.disk_cap.is_none());
    assert!(snap.cache.meta_used >= 1);
    assert!(snap.cache.meta_cap >= snap.cache.meta_used);
    assert!(snap.cache.memory_used > 0);
    assert_eq!(snap.cache.inflight, 0);
}

struct FixedRangeOrigin {
    data: Bytes,
    start: u64,
    fail_at: Option<u64>,
    failures: AtomicUsize,
    gets: AtomicUsize,
}

impl FixedRangeOrigin {
    fn new(data: Bytes, start: u64) -> Self {
        Self {
            data,
            start,
            fail_at: None,
            failures: AtomicUsize::new(0),
            gets: AtomicUsize::new(0),
        }
    }

    fn failing(mut self, at: u64, times: usize) -> Self {
        self.fail_at = Some(at);
        self.failures = AtomicUsize::new(times);
        self
    }

    fn gets(&self) -> usize {
        self.gets.load(Ordering::Relaxed)
    }
}

#[async_trait::async_trait]
impl Origin for FixedRangeOrigin {
    async fn get(&self, _: &str, _: GetOptions) -> Result<GetResponse, OriginError> {
        self.gets.fetch_add(1, Ordering::Relaxed);
        let size = self.data.len() as u64;
        let failing = self
            .failures
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |left| {
                left.checked_sub(1)
            })
            .is_ok();
        let end = self.fail_at.filter(|_| failing).unwrap_or(size);

        let head = futures::stream::once(std::future::ready(Ok(self
            .data
            .slice(self.start as usize..end as usize))));
        let body = if failing {
            let error = futures::stream::once(async {
                tokio::time::sleep(Duration::from_millis(50)).await;
                Err(OriginError::io(std::io::Error::other("connection reset")))
            });
            head.chain(error).boxed()
        } else {
            head.boxed()
        };

        Ok(GetResponse {
            meta: ObjectMeta::new(size, None),
            range: self.start..size,
            body,
        })
    }

    async fn head(&self, _: &str) -> Result<ObjectMeta, OriginError> {
        Ok(ObjectMeta::new(self.data.len() as u64, None))
    }
}

#[tokio::test]
async fn whole_object_responses_fill_every_block() {
    let origin = Arc::new(MemoryOrigin::new().with_whole_objects());
    let data = pattern(10 * BLOCK as usize + 7);
    origin.put("obj", data.clone());
    let (nestor, id) = engine(namespace(origin.clone(), Consistency::Immutable)).await;

    let start = 5 * BLOCK;
    let out = nestor.read(id, "obj", start..start + 10).await.unwrap();
    assert_eq!(out, data.slice(start as usize..start as usize + 10));

    assert_eq!(
        nestor.read(id, "obj", 0..data.len() as u64).await.unwrap(),
        data
    );
    assert_eq!(origin.gets(), 1);
}

#[tokio::test]
async fn responses_start_on_a_block_boundary_at_or_before_the_request() {
    let data = pattern(4 * BLOCK as usize);
    let request = 2 * BLOCK..2 * BLOCK + 10;

    for start in [BLOCK + 1, 3 * BLOCK] {
        let origin = Arc::new(FixedRangeOrigin::new(data.clone(), start));
        let (nestor, id) = engine(namespace(origin, Consistency::Immutable)).await;

        let err = nestor.read(id, "obj", request.clone()).await.unwrap_err();
        assert!(
            matches!(err, NestorError::Origin(OriginError::ShortRead { .. })),
            "{start}: {err:?}"
        );
    }

    let origin = Arc::new(FixedRangeOrigin::new(data.clone(), BLOCK));
    let (nestor, id) = engine(namespace(origin, Consistency::Immutable)).await;
    let out = nestor.read(id, "obj", request.clone()).await.unwrap();
    assert_eq!(out, data.slice(2 * BLOCK as usize..2 * BLOCK as usize + 10));
}

#[tokio::test]
async fn an_error_after_the_requested_blocks_still_answers_the_read() {
    let data = pattern(4 * BLOCK as usize);
    let origin = Arc::new(FixedRangeOrigin::new(data.clone(), 0).failing(2 * BLOCK, 1));
    let (nestor, id) = engine(namespace(origin.clone(), Consistency::Immutable)).await;

    let out = nestor.read(id, "obj", 0..BLOCK).await.unwrap();
    assert_eq!(out, data.slice(0..BLOCK as usize));

    let out = nestor.read(id, "obj", BLOCK..2 * BLOCK).await.unwrap();
    assert_eq!(out, data.slice(BLOCK as usize..2 * BLOCK as usize));
    assert_eq!(origin.gets(), 1);
}

#[tokio::test]
async fn a_reader_waiting_on_a_failed_trailing_block_fetches_it_itself() {
    let data = pattern(4 * BLOCK as usize);
    let origin = Arc::new(FixedRangeOrigin::new(data.clone(), 0).failing(2 * BLOCK, 1));
    let (nestor, id) = engine(namespace(origin.clone(), Consistency::Immutable)).await;

    nestor.read(id, "obj", 0..BLOCK).await.unwrap();
    let out = nestor.read(id, "obj", 3 * BLOCK..4 * BLOCK).await.unwrap();

    assert_eq!(out, data.slice(3 * BLOCK as usize..4 * BLOCK as usize));
    assert_eq!(origin.gets(), 2);
}

#[tokio::test]
async fn a_hedged_backup_may_answer_with_the_whole_object() {
    let origin = Arc::new(
        MemoryOrigin::new()
            .with_chunk(BLOCK as usize)
            .with_whole_objects(),
    );
    let data = pattern(4 * BLOCK as usize);
    origin.put("obj", data.clone());
    let mut ns = namespace(origin.clone(), Consistency::Immutable);
    ns.config.fetch.hedge = Some(
        HedgeConfig::factor(1.0, Duration::from_millis(20), Duration::from_millis(20)).unwrap(),
    );
    let (nestor, id) = engine(ns).await;

    origin.stall_next(1, Duration::from_millis(500));
    let started = std::time::Instant::now();
    let out = nestor.read(id, "obj", 2 * BLOCK..4 * BLOCK).await.unwrap();
    assert_eq!(out, data.slice(2 * BLOCK as usize..4 * BLOCK as usize));
    assert!(
        started.elapsed() < Duration::from_millis(400),
        "{:?}",
        started.elapsed()
    );
    assert_eq!(origin.gets(), 2);
}

const OBJECT: u64 = 4 * BLOCK;

fn disk(dir: &std::path::Path, admission: DiskAdmission) -> DiskConfig {
    DiskConfig {
        admission,
        region_size: 4 * 1024 * 1024,
        direct_io: false,
        ..DiskConfig::new(dir, 64 * 1024 * 1024)
    }
}

async fn hybrid(disk: DiskConfig, ns: Namespace) -> (Nestor, NamespaceId) {
    let nestor = Nestor::builder(CacheConfig::memory(64 * 1024 * 1024).disk(disk))
        .namespace(ns)
        .build()
        .await
        .unwrap();
    let id = nestor.namespace("test").unwrap();
    (nestor, id)
}

async fn kept_on_disk(disk: DiskConfig, objects: &[&str]) -> Vec<bool> {
    let offline = Arc::new(MemoryOrigin::new());
    offline.fail_next(usize::MAX);
    let (nestor, id) = hybrid(disk, namespace(offline, Consistency::Immutable)).await;

    let mut kept = Vec::new();
    for object in objects {
        kept.push(nestor.read(id, object, 0..OBJECT).await.is_ok());
    }
    nestor.close().await.unwrap();
    kept
}

fn stored(objects: &[&str]) -> Arc<MemoryOrigin> {
    let origin = Arc::new(MemoryOrigin::new());
    for object in objects {
        origin.put(*object, pattern(OBJECT as usize));
    }
    origin
}

#[tokio::test]
async fn disk_holds_the_blocks_its_admission_allows() {
    for (admission, once_kept) in [
        (DiskAdmission::Fetched, true),
        (DiskAdmission::Evicted, true),
        (DiskAdmission::Reused, false),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let origin = stored(&["once", "twice"]);
        let (nestor, id) = hybrid(
            disk(dir.path(), admission),
            namespace(origin, Consistency::Immutable),
        )
        .await;

        nestor.read(id, "once", 0..OBJECT).await.unwrap();
        nestor.read(id, "twice", 0..OBJECT).await.unwrap();
        nestor.read(id, "twice", 0..OBJECT).await.unwrap();
        nestor.close().await.unwrap();

        let kept = kept_on_disk(disk(dir.path(), admission), &["once", "twice"]).await;
        assert_eq!(kept, [once_kept, true], "{admission:?}");
    }
}

#[tokio::test]
async fn reused_admission_keeps_a_scan_off_the_disk() {
    let dir = tempfile::tempdir().unwrap();
    let origin = stored(&["hot"]);
    origin.put("scan", pattern(32 * BLOCK as usize));
    let nestor = Nestor::builder(
        CacheConfig::memory(8 * BLOCK as usize).disk(disk(dir.path(), DiskAdmission::Reused)),
    )
    .namespace(namespace(origin, Consistency::Immutable))
    .build()
    .await
    .unwrap();
    let id = nestor.namespace("test").unwrap();

    nestor.read(id, "hot", 0..OBJECT).await.unwrap();
    nestor.read(id, "hot", 0..OBJECT).await.unwrap();
    nestor.read(id, "scan", 0..32 * BLOCK).await.unwrap();
    nestor.close().await.unwrap();

    let kept = kept_on_disk(disk(dir.path(), DiskAdmission::Reused), &["hot", "scan"]).await;
    assert_eq!(kept, [true, false]);
}

#[tokio::test]
async fn readahead_is_not_a_read() {
    let dir = tempfile::tempdir().unwrap();
    let origin = stored(&["obj"]);
    let ns = namespace(origin, Consistency::Immutable).readahead(4);
    let (nestor, id) = hybrid(disk(dir.path(), DiskAdmission::Reused), ns).await;

    for index in 0..OBJECT / BLOCK {
        let start = index * BLOCK;
        nestor.read(id, "obj", start..start + BLOCK).await.unwrap();
    }
    nestor.close().await.unwrap();

    let kept = kept_on_disk(disk(dir.path(), DiskAdmission::Reused), &["obj"]).await;
    assert_eq!(kept, [false]);
}

#[tokio::test]
async fn a_read_that_learns_the_size_counts_once() {
    let dir = tempfile::tempdir().unwrap();
    let origin = stored(&["obj"]);
    let (nestor, id) = hybrid(
        disk(dir.path(), DiskAdmission::Reused),
        namespace(origin, Consistency::Immutable),
    )
    .await;

    let stream = nestor.get(id, "obj", ReadRange::Full).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    stream.collect().await.unwrap();
    nestor.close().await.unwrap();

    let kept = kept_on_disk(disk(dir.path(), DiskAdmission::Reused), &["obj"]).await;
    assert_eq!(kept, [false]);
}

#[tokio::test]
async fn readers_that_wait_on_a_fetch_count_as_reads() {
    let dir = tempfile::tempdir().unwrap();
    let origin = stored(&["obj"]);
    origin.set_latency(Duration::from_millis(50));
    let (nestor, id) = hybrid(
        disk(dir.path(), DiskAdmission::Reused),
        namespace(origin.clone(), Consistency::Immutable),
    )
    .await;

    let (first, second) = tokio::join!(
        nestor.read(id, "obj", 0..OBJECT),
        nestor.read(id, "obj", 0..OBJECT)
    );
    first.unwrap();
    second.unwrap();
    assert_eq!(origin.gets(), 1);
    nestor.close().await.unwrap();

    let kept = kept_on_disk(disk(dir.path(), DiskAdmission::Reused), &["obj"]).await;
    assert_eq!(kept, [true]);
}

#[tokio::test]
async fn invalidate_forgets_the_reads_of_a_block() {
    let dir = tempfile::tempdir().unwrap();
    let origin = stored(&["obj"]);
    let (nestor, id) = hybrid(
        disk(dir.path(), DiskAdmission::Reused),
        namespace(origin, Consistency::Immutable),
    )
    .await;

    nestor.read(id, "obj", 0..OBJECT).await.unwrap();
    nestor.invalidate(id, "obj").unwrap();
    nestor.read(id, "obj", 0..OBJECT).await.unwrap();
    nestor.close().await.unwrap();

    let kept = kept_on_disk(disk(dir.path(), DiskAdmission::Reused), &["obj"]).await;
    assert_eq!(kept, [false]);
}

#[tokio::test]
async fn inserted_blocks_need_two_reads_to_reach_disk() {
    let dir = tempfile::tempdir().unwrap();
    let data = pattern(OBJECT as usize);
    let (nestor, id) = hybrid(
        disk(dir.path(), DiskAdmission::Reused),
        namespace(Arc::new(MemoryOrigin::new()), Consistency::Immutable),
    )
    .await;

    nestor.insert(id, "once", None, &data).unwrap();
    nestor.insert(id, "twice", None, &data).unwrap();
    nestor.read(id, "once", 0..OBJECT).await.unwrap();
    nestor.read(id, "twice", 0..OBJECT).await.unwrap();
    nestor.read(id, "twice", 0..OBJECT).await.unwrap();
    nestor.close().await.unwrap();

    let kept = kept_on_disk(disk(dir.path(), DiskAdmission::Reused), &["once", "twice"]).await;
    assert_eq!(kept, [false, true]);
}
