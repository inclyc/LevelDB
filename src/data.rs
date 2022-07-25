//! 数据区

use std::collections::VecDeque;

use crate::aggregate::Semigroup;

pub trait DataPartition<V> {
    fn push(&mut self, timestamp: u64, value: V);

    fn query(&self, timestamp: u64, r: u64) -> (V, u64);
}

struct DataLine<V> {
    data: VecDeque<Option<V>>,

    start: u64,

    end: u64,

    agg_fn: fn(V, V) -> V,
}

impl<V> DataLine<V> {
    fn get_idx(&self, timestamp: u64) -> usize {
        (timestamp - self.start) as usize
    }

    pub fn new(length: usize, start: u64, agg_fn: fn(V, V) -> V) -> DataLine<V> {
        let mut data = VecDeque::new();
        data.resize_with(length, || None);
        // 一开始 end == start, 这时表示为空
        // 半开半闭语义 [start, end)
        DataLine {
            data,
            start,
            end: start,
            agg_fn,
        }
    }

    /// 获取并移除队列首部元素，取得所有权
    pub fn pop_front(&mut self) -> Option<V> {
        let ret = self.data.pop_front();
        self.start += 1;
        ret.unwrap_or(None)
    }

    /// 查询位于 timestamp 处的具体数值
    pub fn query_value(&self, timestamp: u64) -> &Option<V> {
        let idx = self.get_idx(timestamp);
        self.data.get(idx.try_into().unwrap()).unwrap()
    }

    /// 这个 Line 结构保存的时间的范围
    pub fn get_range(&self) -> (u64, u64) {
        (self.start, self.end)
    }

    pub fn check_range(&self, timestamp: u64) -> bool {
        let (start, end) = self.get_range();
        start <= timestamp && timestamp < end
    }

    pub fn padding(&mut self, timestamp: u64) {
        let idx = self.get_idx(timestamp);
        // Padding
        while idx >= self.data.len() {
            self.data.push_back(None);
        }
    }
}

impl<V> Semigroup<V> for DataLine<V> {
    fn agg_fn(&self) -> fn(V, V) -> V {
        self.agg_fn
    }
}

impl<V: Copy> DataLine<V> {
    pub fn insert_or_update(&mut self, timestamp: u64, value: V) {
        let idx = self.get_idx(timestamp);
        let x = self.data.get_mut(idx).unwrap();
        match x {
            Some(origin_value) => {
                *origin_value = (self.agg_fn)(*origin_value, value);
            }
            None => {
                *x = Some(value);
            }
        }
    }
    pub fn append(&mut self, timestamp: u64, value: V) {
        if self.end > timestamp + 1 {
            panic!("line: append a timestamp lower than given before");
        } else {
            self.end = timestamp + 1;
            self.padding(timestamp);
            self.insert_or_update(timestamp, value);
        }
    }
}

struct DataPart<V> {
    data: Vec<DataLine<V>>,
}

impl<V> DataPart<V> {
    fn new(start: u64, size_fn: fn(u64) -> usize, agg_fn: fn(V, V) -> V) -> DataPart<V> {
        let mut data: Vec<DataLine<V>> = Vec::new();
        for i in 0..=64 {
            data.push(DataLine::new(size_fn(i), start >> i, agg_fn));
        }
        DataPart { data }
    }
}

impl<V: Copy> DataPart<V> {
    fn append(&mut self, timestamp: u64, value: V) {
        let lvl = timestamp.trailing_zeros();
        for i in 0..lvl {
            let x = self.data.get_mut(i as usize).unwrap();
            x.append(timestamp >> i, value);
        }
    }
}

impl<V: Copy> DataPartition<V> for DataPart<V> {
    fn push(&mut self, timestamp: u64, value: V) {
        self.append(timestamp, value);
    }

    fn query(&self, timestamp: u64, r: u64) -> (V, u64) {
        todo!()
    }
}

#[cfg(test)]
mod test {
    #[test]
    fn test_basic() {
        let n = 1000;
        let mut p = super::DataLine::new(100, 0, |a, b| a + b);
        for i in 0..n {
            p.append(i, i);
            assert!(p.query_value(i).unwrap() == i)
        }
    }
    #[test]
    fn test_pop_front() {
        let n = 100;
        let mut p = super::DataLine::new(100, 0, |a, b| a + b);
        for i in 0..n {
            p.append(i, i);
        }
        for i in 0..n {
            let x = p.pop_front().unwrap();
            assert_eq!(i, x);
        }
        for i in 0..n {
            p.append(i + n, i);
        }
    }
}
