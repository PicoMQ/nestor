//! Block size math and byte-range resolution. Every offset maps to a block index, `ReadRange` turns
//! the caller's request into concrete bounds once the object size is known.

use std::ops::Range;

use crate::error::NestorError;

pub const MIN_BLOCK_SIZE: u32 = 64 * 1024;
pub const MAX_BLOCK_SIZE: u32 = 16 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BlockSize {
    shift: u8,
}

impl BlockSize {
    pub const fn new(bytes: u32) -> Option<Self> {
        if bytes < MIN_BLOCK_SIZE || bytes > MAX_BLOCK_SIZE || !bytes.is_power_of_two() {
            return None;
        }
        Some(Self {
            shift: bytes.trailing_zeros() as u8,
        })
    }

    pub const fn bytes(self) -> u64 {
        1u64 << self.shift
    }

    pub const fn usize(self) -> usize {
        1usize << self.shift
    }

    pub const fn index(self, offset: u64) -> u32 {
        (offset >> self.shift) as u32
    }

    pub const fn offset(self, index: u32) -> u64 {
        (index as u64) << self.shift
    }

    pub const fn count(self, size: u64) -> u32 {
        let blocks = size.div_ceil(self.bytes());
        if blocks > u32::MAX as u64 {
            u32::MAX
        } else {
            blocks as u32
        }
    }

    pub fn blocks(self, range: &Range<u64>) -> Range<u32> {
        if range.start >= range.end {
            return 0..0;
        }
        self.index(range.start)..self.count(range.end)
    }

    pub fn block_range(self, index: u32, size: Option<u64>) -> Range<u64> {
        self.span(index, 1, size)
    }

    pub fn span(self, first: u32, count: u32, size: Option<u64>) -> Range<u64> {
        let start = self.offset(first);
        let end = self.offset(first + count);
        match size {
            Some(size) => start..end.min(size),
            None => start..end,
        }
    }

    pub(crate) fn first_group(self, request: &ReadRange, window: u32) -> Option<Range<u32>> {
        let (start, end) = match request {
            ReadRange::Full => (0, u32::MAX),
            ReadRange::From(start) => (self.index(*start), u32::MAX),
            ReadRange::Bounded(r) => (self.index(r.start), self.count(r.end)),
            ReadRange::Suffix(_) => return None,
        };
        let take = aligned_take(start, end.max(start.saturating_add(1)), window);
        Some(start..start + take)
    }

    pub fn slice_within(self, index: u32, range: &Range<u64>) -> Range<usize> {
        let block_start = self.offset(index);
        let block_end = block_start + self.bytes();
        let start = range.start.clamp(block_start, block_end) - block_start;
        let end = range.end.clamp(block_start, block_end) - block_start;
        start as usize..end as usize
    }
}

impl Default for BlockSize {
    fn default() -> Self {
        Self { shift: 20 }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum ReadRange {
    #[default]
    Full,
    Bounded(Range<u64>),
    From(u64),
    Suffix(u64),
}

impl ReadRange {
    pub fn needs_size(&self) -> bool {
        !matches!(self, Self::Bounded(_))
    }

    pub fn resolve(&self, size: u64) -> Result<Range<u64>, NestorError> {
        let range = match self {
            Self::Full => 0..size,
            Self::Bounded(r) => {
                if r.start >= r.end || r.start >= size {
                    return Err(NestorError::Range(r.clone(), size));
                }
                r.start..r.end.min(size)
            }
            Self::From(start) => {
                if *start >= size {
                    return Err(NestorError::Range(*start..u64::MAX, size));
                }
                *start..size
            }
            Self::Suffix(len) => {
                if *len == 0 {
                    return Err(NestorError::Range(size..size, size));
                }
                size.saturating_sub(*len)..size
            }
        };
        Ok(range)
    }
}

impl From<Range<u64>> for ReadRange {
    fn from(r: Range<u64>) -> Self {
        Self::Bounded(r)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct FetchGroup {
    pub first: u32,
    pub count: u32,
}

pub(crate) fn group_misses(misses: impl IntoIterator<Item = u32>, window: u32) -> Vec<FetchGroup> {
    let window = window.max(1);
    let mut groups: Vec<FetchGroup> = Vec::new();
    for idx in misses {
        match groups.last_mut() {
            Some(g) if g.first + g.count == idx && idx % window != 0 => g.count += 1,
            _ => groups.push(FetchGroup {
                first: idx,
                count: 1,
            }),
        }
    }
    groups
}

pub(crate) fn aligned_take(next: u32, end: u32, window: u32) -> u32 {
    let window = window.max(1);
    let boundary = (next / window + 1).saturating_mul(window);
    boundary.min(end) - next
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_invalid_sizes() {
        assert!(BlockSize::new(0).is_none());
        assert!(BlockSize::new(3 * 1024 * 1024).is_none());
        assert!(BlockSize::new(MIN_BLOCK_SIZE / 2).is_none());
        assert!(BlockSize::new(MAX_BLOCK_SIZE * 2).is_none());
        assert_eq!(BlockSize::new(1 << 20).unwrap().bytes(), 1 << 20);
    }

    #[test]
    fn indexes_and_ranges() {
        let bs = BlockSize::new(1 << 20).unwrap();
        assert_eq!(bs.index(0), 0);
        assert_eq!(bs.index((1 << 20) - 1), 0);
        assert_eq!(bs.index(1 << 20), 1);
        assert_eq!(bs.blocks(&(0..1)), 0..1);
        assert_eq!(bs.blocks(&(0..(1 << 20))), 0..1);
        assert_eq!(bs.blocks(&(0..(1 << 20) + 1)), 0..2);
        assert_eq!(bs.blocks(&(5..5)), 0..0);
        assert_eq!(bs.count(u64::MAX), u32::MAX);
        assert_eq!(
            bs.block_range(2, Some(2 * (1 << 20) + 10)),
            (2 << 20)..(2 << 20) + 10
        );
        assert_eq!(bs.slice_within(1, &(100..(1 << 20) + 50)), 0..50);
        assert_eq!(bs.slice_within(0, &(100..(1 << 20) + 50)), 100..(1 << 20));
    }

    #[test]
    fn groups_consecutive_and_aligns_to_window() {
        let g = group_misses([0, 1, 2, 4, 5, 9], 2);
        assert_eq!(
            g,
            vec![
                FetchGroup { first: 0, count: 2 },
                FetchGroup { first: 2, count: 1 },
                FetchGroup { first: 4, count: 2 },
                FetchGroup { first: 9, count: 1 },
            ]
        );
        let g = group_misses([3, 4, 5, 6, 7, 8], 4);
        assert_eq!(
            g,
            vec![
                FetchGroup { first: 3, count: 1 },
                FetchGroup { first: 4, count: 4 },
                FetchGroup { first: 8, count: 1 },
            ]
        );
        assert!(group_misses(std::iter::empty(), 4).is_empty());
    }

    #[test]
    fn aligned_take_stops_at_boundaries() {
        assert_eq!(aligned_take(0, 11, 4), 4);
        assert_eq!(aligned_take(3, 11, 4), 1);
        assert_eq!(aligned_take(8, 11, 4), 3);
        assert_eq!(aligned_take(u32::MAX - 1, u32::MAX, 4), 1);
    }

    #[test]
    fn first_group_follows_the_request() {
        let bs = BlockSize::new(1 << 20).unwrap();
        assert_eq!(bs.first_group(&ReadRange::Full, 4), Some(0..4));
        assert_eq!(bs.first_group(&ReadRange::From(5 << 20), 4), Some(5..8));
        assert_eq!(
            bs.first_group(&ReadRange::Bounded((1 << 20) + 7..(2 << 20) + 1), 4),
            Some(1..3)
        );
        assert_eq!(bs.first_group(&ReadRange::Bounded(9..9), 4), Some(0..1));
        assert_eq!(bs.first_group(&ReadRange::Suffix(10), 4), None);
    }

    #[test]
    fn resolves_ranges() {
        assert_eq!(ReadRange::Full.resolve(10).unwrap(), 0..10);
        assert_eq!(ReadRange::Bounded(2..100).resolve(10).unwrap(), 2..10);
        assert!(ReadRange::Bounded(10..12).resolve(10).is_err());
        assert!(ReadRange::Bounded(5..5).resolve(10).is_err());
        assert_eq!(ReadRange::From(3).resolve(10).unwrap(), 3..10);
        assert!(ReadRange::From(10).resolve(10).is_err());
        assert_eq!(ReadRange::Suffix(4).resolve(10).unwrap(), 6..10);
        assert_eq!(ReadRange::Suffix(40).resolve(10).unwrap(), 0..10);
    }
}
