use std::{
    collections::{BTreeSet, HashMap, VecDeque},
    hash::Hash,
};
mod metrics;
use metrics::Metrics;
struct TimestampElement<K, V> {
    timestamp: u64,
    dict: HashMap<K, V>,
}

impl<K: Eq + Hash, V> TimestampElement<K, V> {
    fn new(timestamp: u64, pk: K, value: V) -> TimestampElement<K, V> {
        let mut dict = HashMap::new();
        dict.insert(pk, value);
        TimestampElement { timestamp, dict }
    }
}

struct MemoryBuffer<K, V> {
    levels: Vec<Option<VecDeque<TimestampElement<K, V>>>>,
    lvlsize: usize,
}

struct QueryResult<V> {
    result: BTreeSet<Metrics<V>>,
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
    fn test_write_query() {
        let mut buffer = MemoryBuffer::new(64);
        let test_pk = "some_pk";
        for i in 1..8 {
            buffer.write(i, test_pk, i);
        }
        assert_eq!(buffer.query(test_pk, 1).result.len(), 3); // 2 4 6
        assert_eq!(buffer.query(test_pk, 2).result.len(), 1); // 4
        assert_eq!(buffer.query(test_pk, 0).result.len(), 7);
        for i in 1..8 {
            assert_eq!(buffer.query("some_other_pk", i).result.len(), 0);
        }
    }
}
