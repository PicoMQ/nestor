//! Object metadata cache. Entries record when they were fetched so namespaces with an `ETag` TTL can
//! revalidate.

use std::time::{Duration, Instant};

use crate::cache::{ObjectCache, object_lru};
use crate::key::ObjectKey;
use crate::origin::ObjectMeta;

#[derive(Debug, Clone)]
pub(crate) struct MetaEntry {
    pub meta: ObjectMeta,
    pub at: Instant,
}

pub(crate) struct MetaCache {
    cache: ObjectCache<MetaEntry>,
}

impl MetaCache {
    pub fn new(capacity: usize) -> Self {
        Self {
            cache: object_lru("nestor-meta", capacity),
        }
    }

    pub fn get(&self, key: &ObjectKey, ttl: Option<Duration>) -> Option<ObjectMeta> {
        let entry = self.cache.get(key)?;
        if let Some(ttl) = ttl
            && entry.at.elapsed() > ttl
        {
            self.cache.remove(key);
            return None;
        }
        Some(entry.meta.clone())
    }

    pub fn put(&self, key: ObjectKey, meta: ObjectMeta) {
        self.cache.insert(
            key,
            MetaEntry {
                meta,
                at: Instant::now(),
            },
        );
    }

    pub fn remove(&self, key: &ObjectKey) {
        self.cache.remove(key);
    }
}
