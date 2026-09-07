//! Cache key types and their foyer encoding. A `BlockKey` carries a content tag next to the block
//! index, so blocks of a changed object never alias the old ones.

use std::hash::{BuildHasher, Hasher};
use std::io::{Read, Write};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use foyer::{Code, Error};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NamespaceId(pub(crate) u32);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ObjectKey {
    pub namespace: NamespaceId,
    pub object: Bytes,
}

impl ObjectKey {
    pub fn new(namespace: NamespaceId, object: &str) -> Self {
        Self {
            namespace,
            object: Bytes::copy_from_slice(object.as_bytes()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BlockKey {
    pub namespace: NamespaceId,
    pub tag: u64,
    pub index: u32,
    pub object: Bytes,
}

impl BlockKey {
    pub fn new(object: &ObjectKey, tag: u64, index: u32) -> Self {
        Self {
            namespace: object.namespace,
            tag,
            index,
            object: object.object.clone(),
        }
    }
}

const HEADER: usize = 4 + 8 + 4 + 4;

impl Code for BlockKey {
    fn encode(&self, writer: &mut impl Write) -> foyer::Result<()> {
        let mut buf = BytesMut::with_capacity(HEADER + self.object.len());
        buf.put_u32_le(self.namespace.0);
        buf.put_u64_le(self.tag);
        buf.put_u32_le(self.index);
        buf.put_u32_le(self.object.len() as u32);
        buf.put_slice(&self.object);
        writer.write_all(&buf).map_err(Error::io_error)
    }

    fn decode(reader: &mut impl Read) -> foyer::Result<Self> {
        let mut header = [0u8; HEADER];
        reader.read_exact(&mut header).map_err(Error::io_error)?;
        let mut header = &header[..];
        let namespace = header.get_u32_le();
        let tag = header.get_u64_le();
        let index = header.get_u32_le();
        let len = header.get_u32_le() as usize;
        let mut object = BytesMut::zeroed(len);
        reader.read_exact(&mut object).map_err(Error::io_error)?;
        Ok(Self {
            namespace: NamespaceId(namespace),
            tag,
            index,
            object: object.freeze(),
        })
    }

    fn estimated_size(&self) -> usize {
        HEADER + self.object.len()
    }
}

const TAG_SEEDS: [u64; 4] = [
    0x4e65_7374_6f72_0001,
    0x4e65_7374_6f72_0002,
    0x4e65_7374_6f72_0003,
    0x4e65_7374_6f72_0004,
];

/// Hash of the `ETag`, or of the size when there is none. Never 0, so it cannot collide with
/// `IMMUTABLE_TAG`.
pub fn content_tag(etag: Option<&Bytes>, size: u64) -> u64 {
    let mut hasher =
        ahash::RandomState::with_seeds(TAG_SEEDS[0], TAG_SEEDS[1], TAG_SEEDS[2], TAG_SEEDS[3])
            .build_hasher();
    match etag {
        Some(etag) => hasher.write(etag),
        None => hasher.write_u64(size),
    }
    hasher.finish().max(1)
}

/// Tag used by namespaces that never revalidate.
pub const IMMUTABLE_TAG: u64 = 0;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_roundtrip() {
        let key = BlockKey {
            namespace: NamespaceId(7),
            tag: 0xdead_beef,
            index: 42,
            object: Bytes::from_static(b"bucket/some/object.parquet"),
        };
        let mut buf = Vec::new();
        key.encode(&mut buf).unwrap();
        assert_eq!(buf.len(), key.estimated_size());
        let decoded = BlockKey::decode(&mut &buf[..]).unwrap();
        assert_eq!(decoded, key);
    }

    #[test]
    fn tag_never_collides_with_immutable() {
        assert_ne!(
            content_tag(Some(&Bytes::from_static(b"\"abc\"")), 0),
            IMMUTABLE_TAG
        );
        assert_ne!(content_tag(None, 0), IMMUTABLE_TAG);
    }
}
