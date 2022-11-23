//! 数据区

use std::cmp;

use crate::line::Line;
use crate::traits::{
    ConstrainedQuery, GreedyQuery, RangeQuery, Semigroup, SuffixQuery, TimestampPush,
};

pub struct DataPart<V> {
    data: Vec<Line<V>>,
}

impl<V: Copy> DataPart<V> {
    pub fn new(start: u64, size_fn: impl Fn(u64) -> usize, agg_fn: fn(V, V) -> V) -> DataPart<V> {
        let mut data: Vec<Line<V>> = Vec::with_capacity(64);
        for i in 0..64 {
            data.push(Line::new(size_fn(i), start >> i, agg_fn));
        }
        Self { data }
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
            if timestamp >> i == 0 {
                break;
            }
            self.data[i].push(timestamp >> i, value);
        }
    }
}

impl<V: Copy> ConstrainedQuery<V> for DataPart<V> {
    fn constrained_query(&mut self, timestamp: u64, r: u64) -> Option<(V, u64)> {
        let lvl = timestamp.trailing_zeros();
        let (_, global_r) = self.data[0].get_range();
        for i in (0..=lvl).rev() {
            let x = &mut self.data[i as usize];
            if x.check_range(timestamp >> i) {
                let level_r = cmp::min((1u64 << i) + timestamp, global_r);
                if level_r <= r {
                    if let Some(v) = x.get(timestamp >> i) {
                        return Some((v, level_r));
                    }
                }
            }
        }
        None
    }
}

impl<V: Copy> GreedyQuery<V> for DataPart<V> {
    fn greedy_query(&mut self, timestamp: u64) -> Option<(V, u64)> {
        self.constrained_query(timestamp, u64::MAX)
    }
}

impl<V: Copy> RangeQuery<V> for DataPart<V> {}
impl<V: Copy> SuffixQuery<V> for DataPart<V> {}

#[cfg(test)]
mod test {
    use super::DataPart;
    use crate::traits::{ConstrainedQuery, RangeQuery, SuffixQuery, TimestampPush};
    use rand::{
        distributions::Standard,
        prelude::{Distribution, StdRng},
        Rng, SeedableRng,
    };
    use serde_derive::Deserialize;
    use statrs::distribution::{Normal, Uniform};
    use std::{mem::swap, time::Instant};
    extern crate csv;
    extern crate serde;
    extern crate serde_derive;
    #[test]
    fn basic() {
        let mut x = DataPart::new(1, |_| 10, |a, b| a + b);
        let n = 1000;
        for i in 1..n {
            x.push(i, i);
        }
        let r = x.constrained_query(4, 30);
        assert_eq!(r, Some((22, 8)));
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
        let mut arr = Vec::with_capacity(n as usize);
        for i in (n..=1).rev() {
            arr.push(i);
        }
        for &i in arr.iter() {
            assert_eq!(answer(i, n), x.suffix_query(i).unwrap().0)
        }
    }

    #[test]
    fn bench_lru_size() {
        for i in 10..100 {
            let mut x = DataPart::new(1, |_| i, |a, b| a + b);
            let n = 100;
            for i in 1..n {
                x.push(i, i)
            }
            for l in 1..n {
                for r in (l + 1)..n {
                    assert_eq!(Some(answer(l, r)), x.range_query(l, r))
                }
            }
            for line in x.data {
                eprint!("{} ", line.cache_miss())
            }
            eprintln!();
        }
    }

    #[test]
    fn bench_lru_rand() {
        for i in 10..150 {
            let mut x = DataPart::new(1, |_| i, |a, b| a + b);
            let n = 100;
            for i in 1..n {
                x.push(i, i)
            }
            let test_case = 1000;
            for _ in 0..test_case {
                let l: f64 = StdRng::from_entropy().sample(Standard);
                let r: f64 = StdRng::from_entropy().sample(Standard);
                let mut l = (l * (n as f64)) as u64 + 1;
                let mut r = (r * (n as f64)) as u64 + 1;
                if l == r {
                    continue;
                }
                if l > r {
                    swap(&mut l, &mut r);
                }
                assert_eq!(Some(answer(l, r)), x.range_query(l, r))
            }
            for line in x.data {
                eprint!("{} ", line.cache_miss())
            }
            eprintln!();
        }
    }

    #[test]
    fn bench_lru_consist() {
        let mut rng = StdRng::from_entropy();
        for sigma in 2..150 {
            let mut x = DataPart::new(1, |_| 50, |a, b| a + b);
            let n = 1000;
            for i in 1..n {
                x.push(i, i)
            }
            let test_case = 1000;
            let normal = Normal::new((n as f64) / 2f64, sigma as f64).unwrap();
            for _ in 0..test_case {
                let mut l = (normal.sample(&mut rng) as u64).clamp(1, n);
                let mut r = (normal.sample(&mut rng) as u64).clamp(1, n);
                if l == r {
                    continue;
                }
                if l > r {
                    (l, r) = (r, l);
                }
                assert_eq!(Some(answer(l, r)), x.range_query(l, r));
            }
            for line in x.data {
                eprint!("{} ", line.cache_miss())
            }
            eprintln!();
        }
    }
    #[test]
    fn bench_speed() {
        let n = 1e5 as u64;
        let mut rng = StdRng::from_entropy();
        let mut x = DataPart::new(1, |_| ((n / 2) as usize), |a, b| a + b);
        let begin = Instant::now();
        for i in 1..n {
            x.push(i, i)
        }
        eprintln!("{:?}ns", begin.elapsed().as_nanos() / (n as u128));
        let test_case = 1000;
        let uniform = Uniform::new(1f64, n as f64).unwrap();
        let begin = Instant::now();
        for _ in 0..test_case {
            let mut l = (uniform.sample(&mut rng) as u64).clamp(1, n);
            let mut r = (uniform.sample(&mut rng) as u64).clamp(1, n);
            if l == r {
                continue;
            }
            if l > r {
                (l, r) = (r, l);
            }
            x.range_query(l, r);
        }

        eprintln!("{:?}ns", begin.elapsed().as_nanos() / (test_case as u128));
        let mut sum_cache_miss = 0;
        for line in x.data {
            sum_cache_miss += line.cache_miss()
        }
        eprintln!("Cache Misses: {}", sum_cache_miss);
    }
    #[test]
    fn bench_compare()
    {
        #[derive(Debug, Deserialize, Eq, PartialEq)]
        struct WriteRecord {
            timestamp: u64,
            data: u64,
        }
        #[derive(Debug, Deserialize, Eq, PartialEq)]
        struct QueryRecord {
            l: u64,
            r: u64,
        }

        let mut write_list: Vec<WriteRecord> = vec!();
        let mut max_timestamp: u64 = 0;

        let mut rdr = csv::Reader::from_path("test/data.csv").unwrap();
        for result in rdr.deserialize() {
            let record: WriteRecord = result.unwrap();
            max_timestamp = std::cmp::max(record.timestamp, max_timestamp);
            write_list.push(record);
        }

        let begin = Instant::now();
        let mut x = DataPart::new(1, |_| max_timestamp as usize, |a, b| a + b); 
        for record in write_list.iter() {
            x.push(record.timestamp, record.data)
        }
        eprintln!("{:?}ns", begin.elapsed().as_nanos() / (write_list.len() as u128));

        let mut query_list: Vec<QueryRecord> = vec!();

        let mut rdr = csv::Reader::from_path("test/query.csv").unwrap();
        for result in rdr.deserialize() {
            let record: QueryRecord = result.unwrap();
            query_list.push(record);
        }

        let begin = Instant::now();
        for record in query_list.iter() {
            x.range_query(record.l, record.r);
        }
        eprintln!("{:?}ns", begin.elapsed().as_nanos() / (query_list.len() as u128));
    }
}
