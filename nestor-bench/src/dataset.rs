//! Seeded objects with the geometry of s3stream segments. Nestor never parses contents, so only
//! block boundaries, the index and the footer matter, and every byte is a function of the seed.

use std::ops::Range;
use std::sync::Arc;

use bytes::{BufMut, Bytes, BytesMut};
use clap::ValueEnum;
use futures::{StreamExt, TryStreamExt};
use nestor_e2e::data::{KIB, MIB};
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use serde::{Deserialize, Serialize};

pub const FOOTER: u64 = 48;
pub const INDEX_ENTRY: u64 = 36;
const DATA_BLOCK: u64 = MIB as u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Profile {
    StreamSet,
    Stream,
}

impl Profile {
    fn name(self) -> &'static str {
        match self {
            Self::StreamSet => "streamset",
            Self::Stream => "stream",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatasetParams {
    pub seed: u64,
    pub objects: usize,
    pub profile: Profile,
    pub streams: u32,
}

#[derive(Debug, Clone)]
pub struct Block {
    pub stream: u32,
    pub range: Range<u64>,
}

#[derive(Debug, Clone)]
pub struct Object {
    pub key: String,
    pub seed: u64,
    pub size: u64,
    pub blocks: Vec<Block>,
    pub index: Range<u64>,
    pub footer: Range<u64>,
}

impl Object {
    pub fn open_ranges(&self) -> [Range<u64>; 2] {
        [self.footer.clone(), self.index.clone()]
    }

    pub fn stream_range(&self, stream: u32) -> Option<Range<u64>> {
        let mut blocks = self.blocks.iter().filter(|b| b.stream == stream);
        let first = blocks.next()?;
        let last = blocks.next_back().unwrap_or(first);
        Some(first.range.start..last.range.end)
    }
}

#[derive(Debug, Clone)]
pub struct Dataset {
    pub params: DatasetParams,
    pub objects: Vec<Object>,
}

impl Dataset {
    pub fn generate(params: DatasetParams) -> Self {
        let objects = (0..params.objects)
            .map(|i| match params.profile {
                Profile::StreamSet => stream_set(&params, i),
                Profile::Stream => stream(&params, i),
            })
            .collect();
        Self { params, objects }
    }

    pub fn total_bytes(&self) -> u64 {
        self.objects.iter().map(|o| o.size).sum()
    }

    pub fn stream_ranges(&self, stream: u32) -> Vec<(usize, Range<u64>)> {
        self.objects
            .iter()
            .enumerate()
            .filter_map(|(i, o)| o.stream_range(stream).map(|r| (i, r)))
            .collect()
    }

    pub fn bytes(&self, object: usize, range: &Range<u64>) -> Bytes {
        fill(self.objects[object].seed, range)
    }

    pub async fn upload(
        &self,
        store: Arc<dyn ObjectStore>,
        concurrency: usize,
        force: bool,
    ) -> eyre::Result<usize> {
        let uploaded = futures::stream::iter(self.objects.iter())
            .map(|object| {
                let store = Arc::clone(&store);
                async move {
                    let path = Path::from(object.key.as_str());
                    if !force
                        && let Ok(meta) = store.head(&path).await
                        && meta.size == object.size
                    {
                        return Ok::<usize, eyre::Report>(0);
                    }
                    let payload = fill(object.seed, &(0..object.size));
                    store.put(&path, PutPayload::from_bytes(payload)).await?;
                    Ok(1)
                }
            })
            .buffer_unordered(concurrency)
            .try_fold(0, |acc, n| async move { Ok(acc + n) })
            .await?;
        Ok(uploaded)
    }
}

fn key(params: &DatasetParams, index: usize) -> String {
    format!(
        "bench/{}/{:016x}/{index:06}",
        params.profile.name(),
        params.seed
    )
}

fn object_seed(params: &DatasetParams, index: usize) -> u64 {
    splitmix(params.seed ^ (index as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15))
}

fn stream_set(params: &DatasetParams, index: usize) -> Object {
    let seed = object_seed(params, index);
    let mut rng = Rng::new(seed);
    let target = (16 + rng.below(49)) * MIB as u64;

    let mut contributions: Vec<(u32, u64)> = Vec::new();
    let mut total = 0;
    while total < target {
        let size = match rng.below(100) {
            0..70 => 4 * KIB as u64 + rng.below(60 * KIB as u64),
            70..95 => 64 * KIB as u64 + rng.below(960 * KIB as u64),
            _ => MIB as u64 + rng.below(7 * MIB as u64),
        };
        contributions.push((rng.below(u64::from(params.streams)) as u32, size));
        total += size;
    }
    contributions.sort_by_key(|(stream, _)| *stream);
    contributions.dedup_by(|(b, size_b), (a, size_a)| {
        if a == b {
            *size_a += *size_b;
            true
        } else {
            false
        }
    });

    let mut blocks = Vec::new();
    let mut offset = 0;
    for (stream, size) in contributions {
        let mut remaining = size;
        while remaining > 0 {
            let len = remaining.min(DATA_BLOCK);
            blocks.push(Block {
                stream,
                range: offset..offset + len,
            });
            offset += len;
            remaining -= len;
        }
    }
    finish(key(params, index), seed, blocks, offset)
}

fn stream(params: &DatasetParams, index: usize) -> Object {
    let seed = object_seed(params, index);
    let mut rng = Rng::new(seed);
    let size = MIB as u64 + rng.below(7 * MIB as u64);
    let stream = (index as u64 % u64::from(params.streams)) as u32;
    let mut blocks = Vec::new();
    let mut offset = 0;
    while offset < size {
        let len = (size - offset).min(DATA_BLOCK);
        blocks.push(Block {
            stream,
            range: offset..offset + len,
        });
        offset += len;
    }
    finish(key(params, index), seed, blocks, offset)
}

fn finish(key: String, seed: u64, blocks: Vec<Block>, data_end: u64) -> Object {
    let index = data_end..data_end + blocks.len() as u64 * INDEX_ENTRY;
    let footer = index.end..index.end + FOOTER;
    Object {
        key,
        seed,
        size: footer.end,
        blocks,
        index,
        footer,
    }
}

pub fn fill(seed: u64, range: &Range<u64>) -> Bytes {
    let len = (range.end - range.start) as usize;
    let first_word = range.start / 8;
    let last_word = range.end.div_ceil(8);
    let mut buf = BytesMut::with_capacity(((last_word - first_word) * 8) as usize);
    for word in first_word..last_word {
        buf.put_u64_le(splitmix(seed ^ word.wrapping_mul(0xbf58_476d_1ce4_e5b9)));
    }
    let skip = (range.start % 8) as usize;
    buf.freeze().slice(skip..skip + len)
}

fn splitmix(mut z: u64) -> u64 {
    z = z.wrapping_add(0x9e37_79b9_7f4a_7c15);
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Self {
        Self(seed)
    }

    pub fn draw(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        splitmix(self.0)
    }

    pub fn below(&mut self, n: u64) -> u64 {
        self.draw() % n
    }

    pub fn unit(&mut self) -> f64 {
        (self.draw() >> 11) as f64 / (1u64 << 53) as f64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn params(profile: Profile) -> DatasetParams {
        DatasetParams {
            seed: 7,
            objects: 4,
            profile,
            streams: 100,
        }
    }

    #[test]
    fn layout_is_contiguous_and_deterministic() {
        let a = Dataset::generate(params(Profile::StreamSet));
        let b = Dataset::generate(params(Profile::StreamSet));
        for (x, y) in a.objects.iter().zip(&b.objects) {
            assert_eq!(x.key, y.key);
            assert_eq!(x.size, y.size);
            let mut offset = 0;
            for block in &x.blocks {
                assert_eq!(block.range.start, offset);
                assert!(block.range.end - block.range.start <= DATA_BLOCK);
                offset = block.range.end;
            }
            assert_eq!(x.index.start, offset);
            assert_eq!(x.footer.end, x.size);
            assert!(x.blocks.windows(2).all(|w| w[0].stream <= w[1].stream));
        }
    }

    #[test]
    fn stream_profile_stays_small() {
        let ds = Dataset::generate(params(Profile::Stream));
        for object in &ds.objects {
            assert!(object.size < 9 * MIB as u64);
            assert_eq!(
                object
                    .blocks
                    .iter()
                    .map(|b| b.stream)
                    .collect::<std::collections::HashSet<_>>()
                    .len(),
                1
            );
        }
    }

    #[test]
    fn fill_slices_agree_with_the_whole() {
        let whole = fill(42, &(0..4096));
        for range in [0..1u64, 3..11, 8..16, 1000..4096, 4095..4096] {
            assert_eq!(
                fill(42, &range),
                whole.slice(range.start as usize..range.end as usize)
            );
        }
    }
}
