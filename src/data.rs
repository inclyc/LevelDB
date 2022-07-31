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
        let (_, global_r) = self.data.get(0).unwrap().get_range();
        for i in (0..=lvl).rev() {
            let x = self.data.get(i as usize).unwrap();
            if x.check_range(timestamp >> i) {
                let level_r = std::cmp::min((1u64 << i) + timestamp, global_r);
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

    fn answer(l: u64, r: u64) -> u64 {
        let mut sum = 0;
        for i in l..r {
            sum += i;
        }
        return sum;
    }
    #[test]
    fn correct() {
        let mut x = DataPart::new(1, |_| 10, |a, b| a + b);
        let n = 1000;
        for i in 1..n {
            x.append(i, i);
        }
        for i in 1..n {
            let (sum, r) = x.query(i, 1000).unwrap();
            assert_eq!(answer(i, r), sum);
            let (sum, r) = x.query(i, i + 1).unwrap();
            assert_eq!(answer(i, r), sum);
            let (sum, r) = x.query(i, i + 100).unwrap();
            assert_eq!(answer(i, r), sum);
        }
    }
}
