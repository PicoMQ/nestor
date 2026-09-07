//! Criterion benchmarks for hot and cold reads.

use std::sync::Arc;

use bytes::Bytes;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use nestor::{BlockSize, CacheConfig, Consistency, MemoryOrigin, Namespace, NamespaceId, Nestor};

const OBJECT: &str = "bench/object";
const SIZE: usize = 64 * 1024 * 1024;

async fn setup(block: u32) -> (Nestor, NamespaceId) {
    let origin = Arc::new(MemoryOrigin::new().with_chunk(1 << 20));
    let data: Vec<u8> = (0..SIZE).map(|i| (i % 251) as u8).collect();
    origin.put(OBJECT, Bytes::from(data));
    let ns = Namespace::new("bench", origin)
        .block_size(BlockSize::new(block).unwrap())
        .consistency(Consistency::Immutable)
        .hedge(None);
    let nestor = Nestor::builder(CacheConfig::memory(256 * 1024 * 1024))
        .namespace(ns)
        .build()
        .await
        .unwrap();
    let id = nestor.namespace("bench").unwrap();
    nestor.read(id, OBJECT, 0..SIZE as u64).await.unwrap();
    (nestor, id)
}

fn hit_path(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("hit");
    for block in [256 * 1024, 1 << 20, 4 << 20] {
        let (nestor, id) = rt.block_on(setup(block));
        group.throughput(Throughput::Bytes(4096));
        group.bench_with_input(BenchmarkId::new("read_4k", block), &block, |b, _| {
            b.to_async(&rt).iter(|| async {
                nestor
                    .read(id, OBJECT, 10 * 1024 * 1024..10 * 1024 * 1024 + 4096)
                    .await
                    .unwrap()
            });
        });
        group.throughput(Throughput::Bytes(SIZE as u64));
        group.bench_with_input(BenchmarkId::new("read_full", block), &block, |b, _| {
            b.to_async(&rt)
                .iter(|| async { nestor.read(id, OBJECT, 0..SIZE as u64).await.unwrap() });
        });
    }
    group.finish();
}

criterion_group!(benches, hit_path);
criterion_main!(benches);
