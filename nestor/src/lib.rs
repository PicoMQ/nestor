//! Read-through block cache for object storage. Objects are split into fixed-size blocks held in a
//! foyer hybrid cache, a read of any byte range resolves to the blocks covering it and misses are
//! fetched from the namespace's `Origin`.

pub mod block;
mod body;
pub mod cache;
pub mod error;
mod fetch;
mod inflight;
pub mod key;
pub mod latency;
pub mod memory;
mod meta;
pub mod metrics;
pub mod namespace;
mod nestor;
pub mod origin;
pub mod policy;
mod readahead;
mod reader;

pub use block::{BlockSize, MAX_BLOCK_SIZE, MIN_BLOCK_SIZE, ReadRange};
pub use cache::{CacheConfig, DiskConfig, DiskIo};
pub use error::{NestorError, OriginError, Result};
pub use foyer::{Compression, RecoverMode};
pub use key::NamespaceId;
pub use latency::Latency;
pub use memory::MemoryOrigin;
pub use mixtrics::metrics::BoxedRegistry;
pub use namespace::{Consistency, Namespace, NamespaceConfig};
pub use nestor::{
    CacheSnapshot, NamespaceSnapshot, Nestor, NestorBuilder, NodeSnapshot, ReadOptions,
};
pub use origin::{GetOptions, GetResponse, ObjectMeta, Origin, Precondition, Preconditions};
pub use policy::{
    FetchOverrides, FetchPolicy, HedgeAfter, HedgeConfig, HedgeConfigError, PolicyParseError,
};
pub use reader::ReadStream;
