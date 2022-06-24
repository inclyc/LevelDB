use std::{
    collections::{BTreeSet, HashMap, VecDeque},
    f32::consts::E,
    hash::Hash,
};
mod metrics;
use metrics::Metrics;
struct TimestampElement<K, V> {
    timestamp: u64,
    dict: HashMap<K, V>,
}

struct MemoryBuffer<K, V> {
    levels: Vec<Option<VecDeque<TimestampElement<K, V>>>>,
    lvlsize: usize,
}

struct QueryResult<V> {
    result: BTreeSet<Metrics<V>>,
}

impl<K: Eq + Hash, V> TimestampElement<K, V> {
    fn new(timestamp: u64, pk: K, value: V) -> TimestampElement<K, V> {
        let mut dict = HashMap::new();
        dict.insert(pk, value);
        TimestampElement { timestamp, dict }
    }
}
impl<K: Eq + Hash, V> MemoryBuffer<K, V> {
    fn new(lvlsize: usize) -> MemoryBuffer<K, V> {
        let mut levels = Vec::with_capacity(lvlsize);
        levels.resize_with(lvlsize, || None);
        MemoryBuffer { levels, lvlsize }
    }

    fn _write_level(te: &mut VecDeque<TimestampElement<K, V>>, timestamp: u64, pk: K, value: V) {
        match te.back_mut() {
            Some(data) => {
                if data.timestamp == timestamp {
                    data.dict.insert(pk, value);
                } else {
                    // this is a new timestamp here
                    te.push_back(TimestampElement::new(timestamp, pk, value))
                }
            }
            None => te.push_back(TimestampElement::new(timestamp, pk, value)),
        }
    }

    fn write(&mut self, timestamp: u64, pk: K, value: V) {
        let lvl = timestamp.trailing_zeros() as usize;
        let level = self.levels.get_mut(lvl).unwrap();
        match level {
            None => {
                let mut te = VecDeque::new();
                MemoryBuffer::_write_level(&mut te, timestamp, pk, value);
                *level = Some(te);
            }
            Some(te) => MemoryBuffer::_write_level(te, timestamp, pk, value),
        };
    }

    fn query(&self, pk: K, level: usize) -> QueryResult<&V> {
        let mut result = BTreeSet::new();
        for idx in level..self.lvlsize {
            if let Some(te) = self.levels.get(idx).unwrap() {
                for ele in te {
                    match ele.dict.get(&pk) {
                        Some(value) => {
                            result.insert(Metrics {
                                timestamp: ele.timestamp,
                                value,
                            });
                        }
                        None => (),
                    }
                }
            }
        }
        QueryResult { result }
    }
}

#[cfg(test)]
mod test {
    use super::MemoryBuffer;

    #[test]
    fn test_write() {
        let mut buffer = MemoryBuffer::new(64);
        buffer.write(1, "string", 100);
        buffer.write(1, "string", 110);
        buffer.write(2, "string", 100);
        buffer.write(3, "string", 100);
        buffer.write(5, "string", 100);
        buffer.write(4, "string", 100);
        buffer.write(6, "string", 100);
        buffer.write(7, "string", 100);
        let result = buffer.query("string", 1).result;
        assert_eq!(result.len(), 3); // 2 4 6
        let result = buffer.query("string", 2).result;
        assert_eq!(result.len(), 1) // 4
    }
}
