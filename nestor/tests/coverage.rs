//! Property tests asserting reads are byte-exact for any range, block size and object size.

use std::sync::Arc;

use bytes::Bytes;
use nestor::{BlockSize, CacheConfig, Consistency, FetchPolicy, MemoryOrigin, Namespace, Nestor};
use proptest::prelude::*;

fn pattern(len: usize) -> Bytes {
    Bytes::from(
        (0..len)
            .map(|i| (i.wrapping_mul(31) % 251) as u8)
            .collect::<Vec<u8>>(),
    )
}

fn read_matches(
    block: u32,
    chunk: usize,
    size: usize,
    ranges: Vec<(usize, usize)>,
    fetch_window: u32,
) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async move {
        let origin = Arc::new(MemoryOrigin::new().with_chunk(chunk));
        let data = pattern(size);
        origin.put("obj", data.clone());
        let ns = Namespace::new("p", origin.clone())
            .block_size(BlockSize::new(block).unwrap())
            .fetch_window(fetch_window)
            .read_window(fetch_window * 2)
            .consistency(Consistency::Immutable)
            .fetch(FetchPolicy::default().hedge(None));
        let nestor = Nestor::builder(CacheConfig::memory(256 << 20))
            .namespace(ns)
            .build()
            .await
            .unwrap();
        let id = nestor.namespace("p").unwrap();
        for (start, len) in ranges {
            let start = start.min(size.saturating_sub(1));
            let end = (start + len.max(1)).min(size);
            if start >= end {
                continue;
            }
            let out = nestor
                .read(id, "obj", start as u64..end as u64)
                .await
                .unwrap();
            prop_assert_eq!(out, data.slice(start..end));
        }
        Ok(())
    })
    .unwrap();
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(48))]

    #[test]
    fn random_ranges_read_exact_bytes(
        block_shift in 16u32..19,
        chunk in 1usize..200_000,
        size in 1usize..2_000_000,
        ranges in prop::collection::vec((0usize..2_000_000, 1usize..600_000), 1..8),
        fetch_window in 1u32..6,
    ) {
        read_matches(1 << block_shift, chunk, size, ranges, fetch_window);
    }
}
