//! Cache key and value types and their foyer encoding.

use std::hash::{BuildHasher, Hasher};
use std::io::{Read, Write};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use foyer::{Code, Error};

use crate::namespace::Consistency;
use crate::origin::ObjectMeta;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Block {
    pub meta: ObjectMeta,
    pub data: Bytes,
}

impl Block {
    pub fn new(meta: ObjectMeta, data: Bytes) -> Self {
        Self { meta, data }
    }

    pub fn empty(meta: ObjectMeta) -> Self {
        Self::new(meta, Bytes::new())
    }

    pub fn weight(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.data.len()
            + self.meta.etag.as_ref().map_or(0, Bytes::len)
    }
}

const META_HEADER: usize = 8 + 8 + 2;

impl Code for Block {
    fn encode(&self, writer: &mut impl Write) -> foyer::Result<()> {
        let etag = self.meta.etag.as_deref().unwrap_or_default();
        let mut header = BytesMut::with_capacity(META_HEADER + etag.len() + 4);
        header.put_u64_le(self.meta.size);
        header.put_u64_le(self.meta.last_modified.map_or(0, unix_millis));
        header.put_u16_le(etag.len() as u16);
        header.put_slice(etag);
        header.put_u32_le(self.data.len() as u32);
        writer.write_all(&header).map_err(Error::io_error)?;
        writer.write_all(&self.data).map_err(Error::io_error)
    }

    fn decode(reader: &mut impl Read) -> foyer::Result<Self> {
        let mut header = [0u8; META_HEADER];
        reader.read_exact(&mut header).map_err(Error::io_error)?;
        let mut header = &header[..];
        let size = header.get_u64_le();
        let modified = header.get_u64_le();
        let etag_len = header.get_u16_le() as usize;
        let etag = if etag_len > 0 {
            let mut etag = BytesMut::zeroed(etag_len);
            reader.read_exact(&mut etag).map_err(Error::io_error)?;
            Some(etag.freeze())
        } else {
            None
        };
        let mut len = [0u8; 4];
        reader.read_exact(&mut len).map_err(Error::io_error)?;
        let mut data = BytesMut::zeroed(u32::from_le_bytes(len) as usize);
        reader.read_exact(&mut data).map_err(Error::io_error)?;
        Ok(Self {
            meta: ObjectMeta {
                size,
                etag,
                last_modified: (modified > 0).then(|| UNIX_EPOCH + Duration::from_millis(modified)),
            },
            data: data.freeze(),
        })
    }

    fn estimated_size(&self) -> usize {
        META_HEADER + self.meta.etag.as_ref().map_or(0, Bytes::len) + 4 + self.data.len()
    }
}

fn unix_millis(time: SystemTime) -> u64 {
    time.duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NamespaceId(pub(crate) u32);

impl NamespaceId {
    pub fn as_u32(self) -> u32 {
        self.0
    }
}

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

pub const IMMUTABLE_TAG: u64 = 0;

pub(crate) fn block_tag(consistency: Consistency, meta: &ObjectMeta) -> u64 {
    if consistency.is_immutable() {
        IMMUTABLE_TAG
    } else {
        content_tag(meta.etag.as_ref(), meta.size)
    }
}

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
    fn block_roundtrip() {
        let modified = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let block = Block::new(
            ObjectMeta {
                size: 12_345,
                etag: Some(Bytes::from_static(b"\"abc\"")),
                last_modified: Some(modified),
            },
            Bytes::from_static(b"payload"),
        );
        let mut buf = Vec::new();
        block.encode(&mut buf).unwrap();
        assert_eq!(buf.len(), block.estimated_size());
        assert_eq!(Block::decode(&mut &buf[..]).unwrap(), block);

        let bare = Block::empty(ObjectMeta {
            size: 0,
            etag: None,
            last_modified: None,
        });
        let mut buf = Vec::new();
        bare.encode(&mut buf).unwrap();
        assert_eq!(Block::decode(&mut &buf[..]).unwrap(), bare);
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
