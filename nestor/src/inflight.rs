//! Request coalescing. The first caller to register a `BlockKey` owns the fetch, later callers get
//! a handle that resolves with the owner's result. An owner dropped without resolving cancels its
//! waiters.

use std::collections::HashMap;
use std::hash::{BuildHasher, Hash};
use std::sync::{Arc, Mutex};

use tokio::sync::watch;

use crate::error::NestorError;
use crate::key::{Block, BlockKey};

type Outcome<T> = Result<T, Arc<NestorError>>;
type Receiver<T> = watch::Receiver<Option<Outcome<T>>>;
type Shard<K, T> = Mutex<HashMap<K, Receiver<T>, ahash::RandomState>>;

#[derive(Clone)]
pub(crate) struct SlotHandle<T = Block> {
    rx: Receiver<T>,
}

impl<T: Clone> SlotHandle<T> {
    pub fn resolved(result: Outcome<T>) -> Self {
        let (_tx, rx) = watch::channel(Some(result));
        Self { rx }
    }

    pub async fn wait(mut self) -> Outcome<T> {
        loop {
            if let Some(result) = self.rx.borrow_and_update().as_ref() {
                return result.clone();
            }
            if self.rx.changed().await.is_err() {
                return Err(Arc::new(NestorError::Cancelled));
            }
        }
    }
}

pub(crate) struct Slot<K: Hash + Eq = BlockKey, T = Block> {
    key: K,
    tx: watch::Sender<Option<Outcome<T>>>,
    map: Arc<Inflight<K, T>>,
    resolved: bool,
}

impl<K: Hash + Eq, T> Slot<K, T> {
    pub fn key(&self) -> &K {
        &self.key
    }

    pub fn resolve(mut self, result: Outcome<T>) {
        self.settle(result);
    }

    fn settle(&mut self, result: Outcome<T>) {
        if self.resolved {
            return;
        }
        self.resolved = true;
        self.map.remove(&self.key);
        self.tx.send_replace(Some(result));
    }
}

impl<K: Hash + Eq, T> Drop for Slot<K, T> {
    fn drop(&mut self) {
        self.settle(Err(Arc::new(NestorError::Cancelled)));
    }
}

pub(crate) enum Registration<K: Hash + Eq = BlockKey, T = Block> {
    Owner(Slot<K, T>, SlotHandle<T>),
    Waiter(SlotHandle<T>),
}

pub(crate) struct Inflight<K: Hash + Eq = BlockKey, T = Block> {
    shards: Box<[Shard<K, T>]>,
    hasher: ahash::RandomState,
}

impl<K: Hash + Eq, T> Inflight<K, T> {
    pub fn new(shards: usize) -> Arc<Self> {
        let shards = shards.max(1).next_power_of_two();
        Arc::new(Self {
            shards: (0..shards)
                .map(|_| Mutex::new(HashMap::with_hasher(ahash::RandomState::new())))
                .collect(),
            hasher: ahash::RandomState::new(),
        })
    }

    fn shard(&self, key: &K) -> &Shard<K, T> {
        let mut h = self.hasher.build_hasher();
        key.hash(&mut h);
        let idx = (std::hash::Hasher::finish(&h) as usize) & (self.shards.len() - 1);
        &self.shards[idx]
    }

    fn remove(&self, key: &K) {
        self.shard(key)
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(key);
    }

    pub fn len(&self) -> usize {
        self.shards
            .iter()
            .map(|s| s.lock().unwrap_or_else(|e| e.into_inner()).len())
            .sum()
    }
}

impl<K: Hash + Eq + Clone, T> Inflight<K, T> {
    pub fn register(self: &Arc<Self>, key: K) -> Registration<K, T> {
        let mut shard = self.shard(&key).lock().unwrap_or_else(|e| e.into_inner());
        if let Some(rx) = shard.get(&key) {
            return Registration::Waiter(SlotHandle { rx: rx.clone() });
        }
        let (tx, rx) = watch::channel(None);
        shard.insert(key.clone(), rx.clone());
        let slot = Slot {
            key,
            tx,
            map: Arc::clone(self),
            resolved: false,
        };
        Registration::Owner(slot, SlotHandle { rx })
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;
    use crate::key::NamespaceId;
    use crate::origin::ObjectMeta;

    fn block(data: &'static [u8]) -> Block {
        Block::new(
            ObjectMeta::new(data.len() as u64, None),
            Bytes::from_static(data),
        )
    }

    fn key(i: u32) -> BlockKey {
        BlockKey {
            namespace: NamespaceId(0),
            tag: 0,
            index: i,
            object: Bytes::from_static(b"o"),
        }
    }

    #[tokio::test]
    async fn owner_resolves_waiters() {
        let inflight: Arc<Inflight> = Inflight::new(4);
        let Registration::Owner(slot, own) = inflight.register(key(1)) else {
            panic!("expected owner");
        };
        let Registration::Waiter(waiter) = inflight.register(key(1)) else {
            panic!("expected waiter");
        };
        assert_eq!(inflight.len(), 1);
        let task = tokio::spawn(waiter.wait());
        slot.resolve(Ok(block(b"data")));
        assert_eq!(
            task.await.unwrap().unwrap().data,
            Bytes::from_static(b"data")
        );
        assert_eq!(own.wait().await.unwrap().data, Bytes::from_static(b"data"));
        assert_eq!(inflight.len(), 0);
    }

    #[tokio::test]
    async fn dropped_owner_cancels_waiters() {
        let inflight: Arc<Inflight> = Inflight::new(4);
        let Registration::Owner(slot, _) = inflight.register(key(2)) else {
            panic!("expected owner");
        };
        let Registration::Waiter(waiter) = inflight.register(key(2)) else {
            panic!("expected waiter");
        };
        drop(slot);
        assert!(matches!(
            waiter.wait().await.unwrap_err().as_ref(),
            NestorError::Cancelled
        ));
        assert_eq!(inflight.len(), 0);
    }
}
