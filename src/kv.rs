use std::{hash::Hash, thread, time::Duration};

use lru::LruCache;

pub(crate) struct KVStorage<K>
where
    K: Eq + Hash + Copy,
{
    lru: LruCache<K, ()>,

    read_delay: Duration,

    #[cfg(feature = "trace_io")]
    pub(crate) total_wait: u64,
}

impl<K> KVStorage<K>
where
    K: Eq + Hash + Copy,
{
    pub(crate) fn new(cap: usize, read_delay: Duration) -> Self {
        Self {
            lru: LruCache::new(cap),

            read_delay,

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
            if cfg!(feature = "simulate_kv") {
                thread::sleep(self.read_delay);
            }
            self.lru.put(key, ());
            false
        }
    }

    pub(crate) fn set(&mut self, key: K) {
        self.lru.put(key, ());
    }
}
