use crate::tree::Tree;
use std::collections::VecDeque;

pub struct Line<V> {
    /// 数据用循环队列保存
    data: VecDeque<Option<V>>,

    /// 聚合操作缓存数组
    aggregate: VecDeque<Option<V>>,

    /// 整个数据结构的“下标”应该无限增长
    /// 同时也是开始记录的时间
    start: u64,

    /// 目前记录的最后一个时间戳
    end: u64,

    /// 单位元
    identity: V,

    /// 聚合函数
    agg_fn: fn(V, V) -> V,
}

impl<V> Line<V> {
    fn get_idx(&self, timestamp: u64) -> usize {
        (timestamp - self.start) as usize
    }

    pub fn new(length: usize, start: u64, end: u64, identity: V, agg_fn: fn(V, V) -> V) -> Line<V> {
        let mut data = VecDeque::new();
        let mut aggregate = VecDeque::new();

        data.resize_with(length, || None);
        aggregate.resize_with(length, || None);

        Line {
            data,
            aggregate,
            start,
            end,
            identity,
            agg_fn,
        }
    }

    /// 获取并移除队列首部元素，取得所有权
    pub fn pop_front(&mut self) -> Option<V> {
        let ret = self.data.pop_front();
        self.aggregate.pop_front();
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
}

impl<V: Copy> Tree<V> for Line<V> {
    fn agg(&self, timestamp: u64) -> Option<V> {
        *self.aggregate.get(self.get_idx(timestamp)).unwrap()
    }

    fn set_agg(&mut self, timestamp: u64, value: V) {
        let idx = self.get_idx(timestamp);
        let agg = self.aggregate.get_mut(idx).unwrap();
        match agg {
            Some(origin_value) => *origin_value = value,
            None => *agg = Some(value),
        }
    }

    fn value(&self, timestamp: u64) -> Option<V> {
        *self.data.get(self.get_idx(timestamp)).unwrap()
    }

    fn check_bound(&self, timestamp: u64) -> bool {
        let (l, r) = self.get_range();
        l <= timestamp && timestamp < r
    }

    fn identity(&self) -> V {
        self.identity
    }

    fn agg_fn(&self) -> fn(V, V) -> V {
        self.agg_fn
    }
}

impl<V: Copy> Line<V> {
    pub fn append(&mut self, timestamp: u64, value: V) {
        let idx = self.get_idx(timestamp);
        while idx >= self.data.len() {
            self.data.push_back(None);
            self.aggregate.push_back(None);
        }
        if self.end > timestamp + 1 {
            panic!("line: append a timestamp lower than given before");
        } else {
            self.end = timestamp + 1;
        }
        let x = self.data.get_mut(idx).unwrap();
        match x {
            Some(origin_value) => {
                *origin_value = (self.agg_fn)(*origin_value, value);
            }
            None => {
                *x = Some(value);
            }
        }
        self.update_aggregate(timestamp, value)
    }
}

#[cfg(test)]
mod tests {
    use crate::tree::Tree;
    use rand::prelude::*;
    use std::time::Instant;

    use super::Line;

    fn get_sum_line<T: std::ops::Add<Output = T>>(n: usize, identity: T) -> Line<T> {
        Line::new(n, 0, 1, identity, |a, b| a + b)
    }

    fn get_max_line<T: Ord>(n: usize, identity: T) -> Line<T> {
        Line::new(n, 0, 1, identity, |a, b| std::cmp::max(a, b))
    }

    #[test]
    fn test_max_query_range() {
        let mut line = get_max_line(300, -10);
        for i in 1..100 {
            line.append(i as u64, i)
        }
        assert_eq!(line.query_range(2, 20), 19);
    }

    #[test]
    fn test_query_range() {
        let mut line = get_sum_line(100, 0);
        line.append(1, 2);
        line.append(2, 3);
        line.append(3, 4);
        assert_eq!(line.query_range(1, 3), 5);
    }

    #[test]
    fn test_pop_front() {
        let mut line = get_sum_line(100, 0);
        line.append(1, 2);
        line.append(2, 3);
        line.append(3, 4);
        line.pop_front(); // no "1" now
        assert_eq!(line.query_value(2).unwrap(), 3);
        assert_eq!(line.query_agg(2), 7);
    }

    #[test]
    fn test_many_pop_front() {
        let mut line = get_sum_line(100, 0);
        let mut sum = 0;
        for i in 1..100 {
            line.append(i, i);
            sum += i;
        }
        for i in 100..1000000 {
            let front = line.pop_front();
            match front {
                Some(front_value) => {
                    sum -= front_value;
                }
                None => (),
            }
            line.append(i, i);
            assert_eq!(line.query_value(i).unwrap(), i);
            assert_eq!(line.query_value(i - 1).unwrap(), i - 1);
            assert_eq!(line.query_value(1 + i - 100).unwrap(), 1 + i - 100);
            sum += i;
            assert_eq!(line.query_agg(1 + i - 100), sum);
        }
    }

    #[test]
    fn test_many_write() {
        let mut line = get_sum_line(1000000, 0);
        let mut sum: u64 = 0;
        for i in 1..1000000 {
            line.append(i, i);
            sum += i;
            assert_eq!(line.query_agg(1), sum);
        }

        for i in 1..100 {
            assert_eq!(line.query_value(i).unwrap(), i);
        }
    }

    #[test]
    fn test_uncontinuously_write() {
        let mut line = get_sum_line(1000000, 0);
        let mut sum = 0;
        for i in 1..10000 {
            line.append(i * 2, i);
            sum += i;
            assert_eq!(line.query_agg(2), sum);
        }
    }

    fn _write_n(l: &mut Line<u32>, n: u32) {
        for i in 0..n {
            l.append(i as u64, i % 100);
        }
    }

    fn _write_pop_n(l: &mut Line<u32>, n: u32) {
        for i in 0..(n / 2) {
            l.append(i as u64, i % 100);
        }
        for i in (n / 2)..n {
            l.append(i as u64, i % 100);
            l.pop_front();
        }
    }

    fn _query_n(l: &Line<u32>, n: u32) {
        for _ in 0..n {
            l.query_value(random::<u64>() % (n as u64));
        }
    }

    #[test]
    fn bench_write() {
        for _n in [100, 1000, 10000, 100000, 1000000, 10000000, 100000000].iter() {
            let n = *_n;
            let mut l = get_sum_line(n as usize, 0);
            let now = Instant::now();
            _write_n(&mut l, n); // 199ns

            // print time elasped
            println!("{}", now.elapsed().as_nanos() as f64 / n as f64);

            let mut l = get_sum_line(n as usize, 0);
            let now = Instant::now();
            _write_pop_n(&mut l, n);

            // print time elasped
            println!("{}", now.elapsed().as_nanos() as f64 / n as f64);
        }
    }
    #[test]
    fn bench_read() {
        for _n in [100, 1000, 10000, 100000, 1000000, 10000000, 100000000].iter() {
            let n = *_n;
            let mut l = get_sum_line(n as usize, 0);
            _write_n(&mut l, n);
            let now = Instant::now();
            _query_n(&l, n);
            // print time elasped
            println!("{}", now.elapsed().as_nanos() as f64 / n as f64);
        }
    }
}
