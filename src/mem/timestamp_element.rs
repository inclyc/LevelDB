use std::{collections::HashMap, hash::Hash};

pub struct TimestampElement<K, V> {
    timestamp: u64,
    dict: HashMap<K, V>,
}

impl<K: Eq + Hash, V> TimestampElement<K, V> {
    pub fn new(timestamp: u64, pk: K, value: V) -> TimestampElement<K, V> {
        let mut dict = HashMap::new();
        dict.insert(pk, value);
        TimestampElement { timestamp, dict }
    }
    pub fn insert(&mut self, k: K, v: V) -> Option<V> {
        self.dict.insert(k, v)
    }

    pub fn get(&self, k: &K) -> Option<&V> {
        self.dict.get(k)
    }

    #[inline]
    pub fn get_timestamp(&self) -> u64 {
        self.timestamp
    }
}
