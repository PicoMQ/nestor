//! Object metadata cache. Entries record when they were fetched so namespaces with an `ETag` TTL can
//! revalidate, a stale entry keeps its `ETag` around for a conditional refresh.

use std::time::{Duration, Instant};

use crate::cache::{ObjectCache, object_lru};
use crate::key::ObjectKey;
use crate::origin::ObjectMeta;

#[derive(Debug, Clone)]
pub(crate) struct MetaEntry {
    pub meta: ObjectMeta,
    pub at: Instant,
}

pub(crate) enum MetaLookup {
    Fresh(ObjectMeta),
    Stale(ObjectMeta),
    Missing,
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

    pub fn lookup(&self, key: &ObjectKey, ttl: Option<Duration>) -> MetaLookup {
        let Some(entry) = self.cache.get(key) else {
            return MetaLookup::Missing;
        };
        match ttl {
            Some(ttl) if entry.at.elapsed() > ttl => MetaLookup::Stale(entry.meta.clone()),
            _ => MetaLookup::Fresh(entry.meta.clone()),
        }
    }

    pub fn get(&self, key: &ObjectKey, ttl: Option<Duration>) -> Option<ObjectMeta> {
        match self.lookup(key, ttl) {
            MetaLookup::Fresh(meta) => Some(meta),
            MetaLookup::Stale(_) | MetaLookup::Missing => None,
        }
    }

    pub fn any(&self, key: &ObjectKey) -> Option<ObjectMeta> {
        self.cache.get(key).map(|entry| entry.meta.clone())
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

    pub fn usage(&self) -> usize {
        self.cache.usage()
    }

    pub fn capacity(&self) -> usize {
        self.cache.capacity()
    }
}
