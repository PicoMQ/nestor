//! Sequential access detection. A read starting where the previous read of the same object ended is
//! a signal to prefetch.

use std::ops::Range;

use crate::cache::{ObjectCache, object_lru};
use crate::key::ObjectKey;

pub(crate) struct Readahead {
    last_end: ObjectCache<u64>,
}

impl Readahead {
    pub fn new(capacity: usize) -> Self {
        Self {
            last_end: object_lru("nestor-readahead", capacity),
        }
    }

    pub fn observe(&self, key: &ObjectKey, range: &Range<u64>) -> bool {
        let previous = self.last_end.get(key).map(|e| *e.value());
        self.last_end.insert(key.clone(), range.end);
        previous == Some(range.start)
    }

    pub fn forget(&self, key: &ObjectKey) {
        self.last_end.remove(key);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::key::NamespaceId;

    #[test]
    fn detects_sequential_reads() {
        let ra = Readahead::new(16);
        let key = ObjectKey::new(NamespaceId(0), "obj");
        assert!(!ra.observe(&key, &(0..100)));
        assert!(ra.observe(&key, &(100..200)));
        assert!(!ra.observe(&key, &(500..600)));
        assert!(ra.observe(&key, &(600..700)));
        ra.forget(&key);
        assert!(!ra.observe(&key, &(700..800)));
    }
}
