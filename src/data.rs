//! 数据区

use crate::line::Line;

pub trait SkipQuery<V> {
    fn push(&mut self, timestamp: u64, value: V);

    fn query(&self, timestamp: u64, r: u64) -> Option<(V, u64)>;
}

struct DataPart<V> {
    data: Vec<Line<V>>,
}

impl<V> DataPart<V> {
    pub fn new(start: u64, size_fn: fn(u64) -> usize, agg_fn: fn(V, V) -> V) -> DataPart<V> {
        let mut data: Vec<Line<V>> = Vec::new();
        for i in 0..64 {
            data.push(Line::new(size_fn(i), start >> i, agg_fn));
        }
        DataPart { data }
    }
}

impl<V: Copy> DataPart<V> {
    fn append(&mut self, timestamp: u64, value: V) {
        let lvl = timestamp.trailing_zeros();
        for i in 0..64 {
            let x = self.data.get_mut(i as usize).unwrap();
            x.append(timestamp >> i, value);
        }
    }
}

impl<V: Copy> SkipQuery<V> for DataPart<V> {
    fn push(&mut self, timestamp: u64, value: V) {
        self.append(timestamp, value);
    }

    fn query(&self, timestamp: u64, r: u64) -> Option<(V, u64)> {
        let lvl = timestamp.trailing_zeros();
        for i in (0..=lvl).rev() {
            let x = self.data.get(i as usize).unwrap();
            if x.check_range(timestamp) {
                let (_, end) = x.get_range();
                let level_r = std::cmp::min((1u64 << lvl) + timestamp, end);
                if level_r <= r {
                    match x.query_value(timestamp >> i) {
                        Some(v) => {
                            return Some((*v, level_r));
                        }
                        None => (),
                    }
                }
            }
        }
        None
    }
}
#[cfg(test)]
mod test {
    use super::{DataPart, SkipQuery};

    #[test]
    fn basic() {
        let mut x = DataPart::new(1, |_| 10, |a, b| a + b);
        let n = 1000;
        for i in 1..n {
            x.append(i, i);
        }
        let r = x.query(4, 30);
        assert_eq!(r.unwrap().0, 22);
        assert_eq!(r.unwrap().1, 8);
    }
}
