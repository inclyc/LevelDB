//! 数据区

use crate::line::Line;
use crate::traits::{
    ConstrainedQuery, GreedyQuery, RangeQuery, Semigroup, SuffixQuery, TimestampPush,
};

pub struct DataPart<V> {
    data: Vec<Line<V>>,
}

impl<V> DataPart<V> {
    pub fn new(start: u64, size_fn: fn(u64) -> usize, agg_fn: fn(V, V) -> V) -> DataPart<V> {
        let mut data: Vec<Line<V>> = Vec::with_capacity(64);
        for i in 0..64 {
            data.push(Line::new(size_fn(i), start >> i, agg_fn));
        }
        DataPart { data }
    }
}

impl<V> Semigroup<V> for DataPart<V> {
    fn agg_fn(&self) -> fn(V, V) -> V {
        self.data[0].agg_fn()
    }
}

impl<V: Copy> TimestampPush<V> for DataPart<V> {
    fn push(&mut self, timestamp: u64, value: V) {
        for i in 0..64 {
            self.data[i].push(timestamp >> i, value);
        }
    }
}

impl<V: Copy> ConstrainedQuery<V> for DataPart<V> {
    fn constrained_query(&self, timestamp: u64, r: u64) -> Option<(V, u64)> {
        let lvl = timestamp.trailing_zeros();
        let (_, global_r) = self.data[0].get_range();
        for i in (0..=lvl).rev() {
            let x = &self.data[i as usize];
            if x.check_range(timestamp >> i) {
                let level_r = std::cmp::min((1u64 << i) + timestamp, global_r);
                if level_r <= r {
                    if let Some(v) = x[timestamp >> i] {
                        return Some((v, level_r));
                    }
                }
            }
        }
        None
    }
}

impl<V: Copy> GreedyQuery<V> for DataPart<V> {
    fn greedy_query(&self, timestamp: u64) -> Option<(V, u64)> {
        self.constrained_query(timestamp, u64::MAX)
    }
}

impl<V: Copy> RangeQuery<V> for DataPart<V> {}
impl<V: Copy> SuffixQuery<V> for DataPart<V> {}

#[cfg(test)]
mod test {
    use crate::traits::{ConstrainedQuery, RangeQuery, SuffixQuery, TimestampPush};

    use super::DataPart;

    #[test]
    fn basic() {
        let mut x = DataPart::new(1, |_| 10, |a, b| a + b);
        let n = 1000;
        for i in 1..n {
            x.push(i, i);
        }
        let r = x.constrained_query(4, 30);
        assert_eq!(r.unwrap().0, 22);
        assert_eq!(r.unwrap().1, 8);
    }

    fn answer(l: u64, r: u64) -> u64 {
        (r - l) * (l + r - 1) / 2
    }

    #[test]
    fn correct() {
        let mut x = DataPart::new(1, |_| 10, |a, b| a + b);
        let n = 1000;
        for i in 1..n {
            x.push(i, i);
        }
        for i in 1..n {
            let (sum, r) = x.constrained_query(i, n).unwrap();
            assert_eq!(answer(i, r), sum);
            let (sum, r) = x.constrained_query(i, i + 1).unwrap();
            assert_eq!(answer(i, r), sum);
            let (sum, r) = x.constrained_query(i, i + 100).unwrap();
            assert_eq!(answer(i, r), sum);
        }

        for i in 1..n {
            for j in (i + 1)..n {
                assert_eq!(Some(answer(i, j)), x.range_query(i, j))
            }
        }

        for i in 1..n {
            assert_eq!(answer(i, n), x.suffix_query(i).unwrap().0)
        }
    }
}
