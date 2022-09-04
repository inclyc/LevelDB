use crate::traits::{Semigroup, TimestampPush};

#[cfg(feature = "trace_io")]
use crate::kv::KVStorage;

pub struct Line<V> {
    data: KVStorage<u64, V>,

    start: u64,

    end: u64,

    agg_fn: fn(V, V) -> V,
}

impl<V: Copy> Line<V> {
    fn get_idx(&self, timestamp: u64) -> u64 {
        timestamp - self.start
    }

    pub fn new(length: usize, start: u64, agg_fn: fn(V, V) -> V) -> Line<V> {
        let data = KVStorage::new(length);
        // 一开始 end == start, 这时表示为空
        // 半开半闭语义 [start, end)
        Self {
            data,
            start,
            end: start,
            agg_fn,
        }
    }

    pub fn get(&mut self, timestamp: u64) -> Option<V> {
        self.data.get(self.get_idx(timestamp))
    }

    pub fn put(&mut self, timestamp: u64, value: V) {
        self.data.put(self.get_idx(timestamp), value)
    }
}

impl<V> Line<V> {
    /// 这个 Line 结构保存的时间的范围
    pub fn get_range(&self) -> (u64, u64) {
        (self.start, self.end)
    }

    pub fn check_range(&self, timestamp: u64) -> bool {
        let (start, end) = self.get_range();
        start <= timestamp && timestamp < end
    }
}

impl<V> Semigroup<V> for Line<V> {
    fn agg_fn(&self) -> fn(V, V) -> V {
        self.agg_fn
    }
}

impl<V: Copy> Line<V> {
    pub fn insert_or_update(&mut self, timestamp: u64, value: V) {
        let new_value = match self.get(timestamp) {
            Some(origin_value) => (self.agg_fn)(origin_value, value),
            None => value,
        };
        self.put(timestamp, new_value)
    }
}

impl<V: Copy> TimestampPush<V> for Line<V> {
    fn push(&mut self, timestamp: u64, value: V) {
        if timestamp + 1 < self.end {
            panic!("line: append a timestamp lower than given before");
        } else {
            self.end = timestamp + 1;
            self.insert_or_update(timestamp, value);
        }
    }
}

#[cfg(test)]
mod test {
    use crate::traits::TimestampPush;

    #[test]
    fn test_basic() {
        let n = 1000;
        let mut p = super::Line::new(10, 0, |a, b| a + b);
        for i in 0..n {
            p.push(i, i);
            assert_eq!(p.get(i), Some(i))
        }
    }
}
