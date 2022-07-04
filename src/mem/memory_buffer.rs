use super::metrics::Metrics;
use super::timestamp_element::TimestampElement;
use std::{
    collections::{BTreeSet, VecDeque},
    hash::Hash,
};

pub struct MemoryBuffer<K, V> {
    levels: Vec<Option<VecDeque<TimestampElement<K, V>>>>,
    lvlsize: usize,
    level_limit_generator: Option<fn(usize) -> usize>,
}

impl<K: Eq + Hash, V> MemoryBuffer<K, V> {
    pub fn new(
        lvlsize: usize,
        level_limit_generator: Option<fn(usize) -> usize>,
    ) -> MemoryBuffer<K, V> {
        let mut levels = Vec::with_capacity(lvlsize);
        levels.resize_with(lvlsize, || None);
        MemoryBuffer {
            levels,
            lvlsize,
            level_limit_generator,
        }
    }

    fn _write_level(te: &mut VecDeque<TimestampElement<K, V>>, timestamp: u64, pk: K, value: V) {
        match te.back_mut() {
            Some(data) => {
                if data.get_timestamp() == timestamp {
                    data.insert(pk, value);
                } else {
                    // 这是一个新的时间戳
                    te.push_back(TimestampElement::new(timestamp, pk, value))
                }
            }
            None => te.push_back(TimestampElement::new(timestamp, pk, value)),
        }
    }

    /// 写入到这个时序数据库
    /// Write to this TSDB
    pub fn write(&mut self, timestamp: u64, pk: K, value: V) {
        let lvl = timestamp.trailing_zeros() as usize; // 时间戳级别，定义为时间戳末尾0的数量

        // 这个级别所对应的双端队列，队列可能不存在，levels这个Vec在初始化时全部用None填充了
        let level = self.levels.get_mut(lvl).unwrap();
        match level {
            None => {
                // 这个级别以前从未达到过，需要新创建一个level
                let mut te = match self.level_limit_generator {
                    Some(g) => VecDeque::with_capacity(g(lvl)),
                    None => VecDeque::new(),
                };
                MemoryBuffer::_write_level(&mut te, timestamp, pk, value);
                *level = Some(te);
            }
            Some(te) => MemoryBuffer::_write_level(te, timestamp, pk, value),
        };
    }

    /// 按照主键(pk)和层级(level)查询内存区数据
    /// Query in-memory buffer data by Primary Key(pk) and Level(level)
    /// 返回查询结果
    pub fn query(&self, pk: K, level: usize) -> BTreeSet<Metrics<&V>> {
        let mut result = BTreeSet::new();
        for idx in level..self.lvlsize {
            if let Some(te) = self.levels.get(idx).unwrap() {
                for ele in te {
                    match ele.get(&pk) {
                        Some(value) => {
                            result.insert(Metrics {
                                timestamp: ele.get_timestamp(),
                                value,
                            });
                        }
                        None => (),
                    }
                }
            }
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::MemoryBuffer;

    #[test]
    fn test_write_query() {
        let mut buffer = MemoryBuffer::new(64, None);
        let test_pk = "some_pk";
        for i in 1..8 {
            buffer.write(i, test_pk, i);
        }
        assert_eq!(buffer.query(test_pk, 1).len(), 3); // 2 4 6
        assert_eq!(buffer.query(test_pk, 2).len(), 1); // 4
        assert_eq!(buffer.query(test_pk, 0).len(), 7);
        for i in 1..8 {
            assert_eq!(buffer.query("some_other_pk", i).len(), 0);
        }
    }
    #[test]
    fn test_many_write() {
        let mut buffer = MemoryBuffer::new(64, None);
        let test_pk = "some_pk";
        for i in 1..10000000 {
            buffer.write(i, test_pk, i);
        } // 1.39s --release
        assert_eq!(buffer.query(test_pk, 5).len(), 312499);
    }

    #[test]
    fn bench_write() {
        let mut buffer = MemoryBuffer::new(64, None);
        let test_pk = 123i32;
        let n = 1000000;
        let start = std::time::Instant::now();
        for i in 1..n {
            buffer.write(i, test_pk, i);
        }
        let end = start.elapsed();
        println!("{:?}", end.as_nanos() as f64 / n as f64);
    }
}
