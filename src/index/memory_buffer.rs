use super::metric::Metric;
use std::collections::{BTreeSet, VecDeque};

pub struct MemoryBuffer<V> {
    levels: Vec<Option<VecDeque<Metric<V>>>>,
    lvlsize: usize,
    level_limit_generator: Option<fn(usize) -> usize>,
}

impl<V> MemoryBuffer<V> {
    pub fn new(
        lvlsize: usize,
        level_limit_generator: Option<fn(usize) -> usize>,
    ) -> MemoryBuffer<V> {
        let mut levels = Vec::with_capacity(lvlsize);
        levels.resize_with(lvlsize, || None);
        MemoryBuffer {
            levels,
            lvlsize,
            level_limit_generator,
        }
    }

    fn _write_level(te: &mut VecDeque<Metric<V>>, timestamp: u64, value: V) {
        match te.back_mut() {
            Some(data) => {
                if data.timestamp() == timestamp {
                    data.set_value(value);
                } else {
                    // 这是一个新的时间戳
                    te.push_back(Metric::new(timestamp, value))
                }
            }
            None => te.push_back(Metric::new(timestamp, value)),
        }
    }

    /// 写入到这个时序数据库
    /// Write to this TSDB
    pub fn write(&mut self, timestamp: u64, value: V) {
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
                MemoryBuffer::_write_level(&mut te, timestamp, value);
                *level = Some(te);
            }
            Some(te) => MemoryBuffer::_write_level(te, timestamp, value),
        };
    }

    /// 按照主键(pk)和层级(level)查询内存区数据
    /// Query in-memory buffer data by Primary Key(pk) and Level(level)
    /// 返回查询结果
    pub fn query(&self, level: usize) -> BTreeSet<&Metric<V>> {
        let mut result = BTreeSet::new();
        for idx in level..self.lvlsize {
            if let Some(te) = self.levels.get(idx).unwrap() {
                for ele in te {
                    result.insert(ele);
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
        for i in 1..8 {
            buffer.write(i, i);
        }
        assert_eq!(buffer.query(1).len(), 3); // 2 4 6
        assert_eq!(buffer.query(2).len(), 1); // 4
        assert_eq!(buffer.query(0).len(), 7);
    }
    #[test]
    fn test_many_write() {
        let mut buffer = MemoryBuffer::new(64, None);
        for i in 1..10000000 {
            buffer.write(i, i);
        } // 1.39s --release
        assert_eq!(buffer.query(5).len(), 312499);
    }

    #[test]
    fn bench_write() {
        let mut buffer = MemoryBuffer::new(64, None);
        let n = 1000000;
        let start = std::time::Instant::now();
        for i in 1..n {
            buffer.write(i, i);
        }
        let end = start.elapsed();
        println!("{:?}", end.as_nanos() as f64 / n as f64);
    }
}
