//! Request coalescing. The first caller to register a `BlockKey` owns the fetch, later callers get
//! a handle that resolves with the owner's result. An owner dropped without resolving cancels its
//! waiters.

use std::collections::HashMap;
use std::hash::{BuildHasher, Hash};
use std::sync::{Arc, Mutex};

use tokio::sync::watch;

use crate::error::NestorError;
use crate::key::{Block, BlockKey};

pub(crate) type BlockResult = Result<Block, Arc<NestorError>>;

type Receiver = watch::Receiver<Option<BlockResult>>;

#[derive(Clone)]
pub(crate) struct SlotHandle {
    rx: Receiver,
}

impl SlotHandle {
    pub fn resolved(result: BlockResult) -> Self {
        let (_tx, rx) = watch::channel(Some(result));
        Self { rx }
    }

    pub async fn wait(mut self) -> BlockResult {
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

pub(crate) struct Slot {
    key: BlockKey,
    tx: watch::Sender<Option<BlockResult>>,
    map: Arc<Inflight>,
    resolved: bool,
}

impl Slot {
    pub fn key(&self) -> &BlockKey {
        &self.key
    }

    pub fn resolve(mut self, result: BlockResult) {
        self.settle(result);
    }

    fn settle(&mut self, result: BlockResult) {
        if self.resolved {
            return;
        }
        self.resolved = true;
        self.map.remove(&self.key);
        self.tx.send_replace(Some(result));
    }
}

impl Drop for Slot {
    fn drop(&mut self) {
        self.settle(Err(Arc::new(NestorError::Cancelled)));
    }
}

pub(crate) enum Registration {
    Owner(Slot, SlotHandle),
    Waiter(SlotHandle),
}

pub(crate) struct Inflight {
    shards: Box<[Mutex<HashMap<BlockKey, Receiver, ahash::RandomState>>]>,
    hasher: ahash::RandomState,
}

impl Inflight {
    pub fn new(shards: usize) -> Arc<Self> {
        let shards = shards.max(1).next_power_of_two();
        Arc::new(Self {
            shards: (0..shards)
                .map(|_| Mutex::new(HashMap::with_hasher(ahash::RandomState::new())))
                .collect(),
            hasher: ahash::RandomState::new(),
        })
    }

    fn shard(&self, key: &BlockKey) -> &Mutex<HashMap<BlockKey, Receiver, ahash::RandomState>> {
        let mut h = self.hasher.build_hasher();
        key.hash(&mut h);
        let idx = (std::hash::Hasher::finish(&h) as usize) & (self.shards.len() - 1);
        &self.shards[idx]
    }

    pub fn register(self: &Arc<Self>, key: BlockKey) -> Registration {
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

    fn remove(&self, key: &BlockKey) {
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
        let inflight = Inflight::new(4);
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
        let inflight = Inflight::new(4);
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
