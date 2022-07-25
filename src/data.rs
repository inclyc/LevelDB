//! 数据区

use crate::line::Line;

pub trait DataPartition<V> {
    fn push(&mut self, timestamp: u64, value: V);

    fn query(&self, timestamp: u64, r: u64) -> (V, u64);
}

struct DataPart<V> {
    data: Vec<Line<V>>,
}

impl<V> DataPart<V> {
    pub fn new(start: u64, size_fn: fn(u64) -> usize, agg_fn: fn(V, V) -> V) -> DataPart<V> {
        let mut data: Vec<Line<V>> = Vec::new();
        for i in 0..=64 {
            data.push(Line::new(size_fn(i), start >> i, agg_fn));
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
