use std::hash::Hash;

use lru::LruCache;

pub(crate) struct KVStorage<K>
where
    K: Eq + Hash + Copy,
{
    lru: LruCache<K, ()>,

    #[cfg(feature = "trace_io")]
    pub(crate) total_wait: u64,
}

impl<K> KVStorage<K>
where
    K: Eq + Hash + Copy,
{
    pub(crate) fn new(cap: usize) -> Self {
        Self {
            lru: LruCache::new(cap),

            #[cfg(feature = "trace_io")]
            total_wait: 0,
        }
    }

    pub(crate) fn get(&mut self, key: K) -> bool {
        if self.lru.contains(&key) {
            true
        } else {
            if cfg!(feature = "trace_io") {
                self.total_wait += 1;
            }
            self.lru.put(key, ());
            false
        }
    }

    pub(crate) fn set(&mut self, key: K) {
        self.lru.put(key, ());
    }
}
