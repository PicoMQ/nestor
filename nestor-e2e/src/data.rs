//! Deterministic payloads so any byte of any object can be recomputed and compared.

use std::ops::Range;

use bytes::Bytes;
use futures::TryStreamExt;
use object_store::GetResult;

pub const KIB: usize = 1024;
pub const MIB: usize = 1024 * KIB;

pub fn payload(len: usize, seed: u64) -> Bytes {
    let mut state = seed ^ 0x9e37_79b9_7f4a_7c15;
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state >> 24) as u8
        })
        .collect()
}

pub async fn body(result: GetResult) -> Bytes {
    let chunks: Vec<Bytes> = result.into_stream().try_collect().await.expect("body");
    chunks.concat().into()
}

pub fn slice(bytes: &Bytes, range: &Range<u64>) -> Bytes {
    bytes.slice(range.start as usize..range.end as usize)
}
