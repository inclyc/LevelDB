use std::hash::Hash;

use lru::LruCache;

pub(crate) struct KVStorage<K, V> {
    lru: LruCache<K, V>,

    lru_base: LruCache<K, V>,

    #[cfg(feature = "trace_io")]
    pub(crate) total_wait: u64,
}

impl<K, V> KVStorage<K, V>
where
    K: Eq + Hash,
    V: Copy,
{
    pub(crate) fn new(cap: usize) -> Self {
        Self {
            lru: LruCache::new(cap),
            lru_base: LruCache::unbounded(),
            #[cfg(feature = "trace_io")]
            total_wait: 0,
        }
    }

    pub(crate) fn get(&mut self, key: K) -> Option<V> {
        if self.lru.contains(&key) {
            self.lru.get(&key).copied()
        } else {
            if cfg!(feature = "trace_io") {
                self.total_wait += 1;
            }
            self.lru_base.get(&key).copied()
        }
    }

    pub(crate) fn put(&mut self, key: K, value: V) {
        if let Some((k, v)) = self.lru.push(key, value) {
            self.lru_base.put(k, v);
        }
    }
}
