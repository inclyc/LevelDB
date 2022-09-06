use lru::LruCache;
use std::hash::Hash;

pub struct KVStorage<K, V> {
    lru: LruCache<K, V>,

    lru_base: LruCache<K, V>,

    #[cfg(feature = "trace_io")]
    pub cache_miss: u64,
}

impl<K, V> KVStorage<K, V>
where
    K: Eq + Hash,
    V: Copy,
{
    pub fn new(cap: usize) -> Self {
        Self {
            lru: LruCache::new(cap),
            lru_base: LruCache::unbounded(),
            #[cfg(feature = "trace_io")]
            cache_miss: 0,
        }
    }

    pub fn get(&mut self, key: K) -> Option<V> {
        if self.lru.contains(&key) {
            self.lru.get(&key).copied()
        } else {
            if cfg!(feature = "trace_io") && self.lru_base.contains(&key) {
                self.cache_miss += 1;
            }
            self.lru_base.get(&key).copied()
        }
    }

    pub fn put(&mut self, key: K, value: V) {
        if let Some((k, v)) = self.lru.push(key, value) {
            self.lru_base.put(k, v);
        }
    }
}
