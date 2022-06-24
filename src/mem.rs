use std::{
    collections::{HashMap, VecDeque},
    hash::Hash,
};

struct TimestampElement<K, V> {
    timestamp: u64,
    value: HashMap<K, V>,
}

struct MemoryBuffer<K, V> {
    levels: VecDeque<VecDeque<TimestampElement<K, V>>>,
}

impl<K: Eq + Hash, V> TimestampElement<K, V> {
    fn new(timestamp: u64, pk: K, value: V) -> TimestampElement<K, V> {
        let mut map = HashMap::new();
        map.insert(pk, value);
        TimestampElement {
            timestamp: timestamp,
            value: map,
        }
    }
}

impl<K: Eq + Hash, V> MemoryBuffer<K, V> {
    fn new(lvlsize: usize) -> MemoryBuffer<K, V> {
        MemoryBuffer {
            levels: VecDeque::with_capacity(lvlsize),
        }
    }

    fn _write_level(te: &mut VecDeque<TimestampElement<K, V>>, timestamp: u64, pk: K, value: V) {
        match te.back_mut() {
            Some(data) => {
                if data.timestamp == timestamp {
                    data.value.insert(pk, value);
                } else {
                    // this is a new timestamp here
                    te.push_back(TimestampElement::new(timestamp, pk, value))
                }
            }
            None => te.push_back(TimestampElement::new(timestamp, pk, value)),
        }
    }

    fn write(&mut self, timestamp: u64, pk: K, value: V) {
        let te = self
            .levels
            .get_mut(timestamp.trailing_zeros().try_into().unwrap());
        MemoryBuffer::_write_level(te.unwrap(), timestamp, pk, value)
    }
}
