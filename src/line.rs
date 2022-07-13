use std::{collections::VecDeque, ops::AddAssign};

pub struct Line<V> {
    // 数据用循环队列保存
    data: VecDeque<Option<V>>,

    // 聚合操作缓存数组
    aggregate: VecDeque<Option<V>>,

    // 整个数据结构的“下标”应该无限增长
    offset: u64,

    // 长度
    current_timestamp: u64,
}

impl<V: AddAssign + Copy> Line<V> {
    pub fn new(length: usize, offset: u64, current_timestamp: u64) -> Line<V> {
        let mut data = VecDeque::new();
        let mut aggregate = VecDeque::new();

        data.resize_with(length, || None);
        aggregate.resize_with(length, || None);

        Line {
            data,
            aggregate,
            offset,
            current_timestamp,
        }
    }

    /// 获取并移除队列首部元素，取得所有权
    pub fn pop_front(&mut self) -> Option<V> {
        let ret = self.data.pop_front();
        match ret {
            Some(v) => {
                self.offset += 1;
                self.aggregate.pop_front();
                v
            }
            None => None,
        }
    }

    pub fn update_aggregate(&mut self, mut timestamp: u64, value: V) {
        let mut index = timestamp - self.offset;
        loop {
            let agg = self.aggregate.get_mut(index.try_into().unwrap()).unwrap();
            match agg {
                Some(origin_value) => *origin_value += value,
                None => *agg = Some(value),
            }
            // 如果timestamp巧好为0,则trailing_zeros()会panic
            if timestamp == 0 {
                break;
            }
            let step = 1 << (timestamp.trailing_zeros()); // timestamp != 0

            // 全程要求 current_index >= 0
            // 下一步的timestamp为 updating_timestamp - step
            // 换算成 current_index = updating_timestamp - step - offset >= 0
            // 用加法而不是用0作比较是因为这里是无符号整数，updating_timestamp始终 >= 0
            if timestamp < step + self.offset {
                // i.e timestamp - step - offset < 0, 数组更新边界
                break;
            }
            timestamp -= step;
            index = timestamp - self.offset; // > 0
        }
    }

    pub fn append(&mut self, timestamp: u64, value: V) {
        let target_index = timestamp - self.offset;
        while target_index >= self.data.len().try_into().unwrap() {
            self.data.push_back(None);
            self.aggregate.push_back(None);
        }
        if self.current_timestamp > timestamp {
            panic!("line: append a timestamp lower than given before");
        } else {
            self.current_timestamp = timestamp;
        }
        let x = self.data.get_mut(target_index.try_into().unwrap()).unwrap();
        match x {
            Some(origin_value) => {
                *origin_value = value;
            }
            None => {
                *x = Some(value);
            }
        }
        self.update_aggregate(timestamp, value)
    }

    pub fn query_value(&self, timestamp: u64) -> &Option<V> {
        let index = timestamp - self.offset;
        self.data.get(index.try_into().unwrap()).unwrap()
    }

    /// 聚合查询某一Timestamp之后的值，timestamp必须已经插入过（存在）
    /// panic: 如果timestamp这个时间戳从来没有插入过，则此函数panic
    /// O(logn)
    pub fn query_agg(&self, timestamp: u64) -> V {
        let mut aggregating_index = timestamp - self.offset;
        let mut result = self
            .aggregate
            .get(aggregating_index.try_into().unwrap())
            .unwrap()
            .unwrap();
        let mut aggregating_timestamp = timestamp;
        loop {
            let step = 1 << (aggregating_timestamp.trailing_zeros());
            // next = timestamp + step <= self.current_timestamp
            if aggregating_timestamp + step > (self.current_timestamp as u64) {
                break;
            }
            aggregating_timestamp = aggregating_timestamp + step;
            aggregating_index = aggregating_timestamp - self.offset;
            let agg = self
                .aggregate
                .get(aggregating_index.try_into().unwrap())
                .unwrap();
            match agg {
                Some(v) => {
                    result += *v;
                }
                None => (),
            }
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use rand::prelude::*;
    use std::time::Instant;

    use super::Line;
    #[test]
    fn test_line() {}

    #[test]
    fn test_pop_front() {
        let mut line = Line::new(100, 0, 0);
        line.append(1, 2);
        line.append(2, 3);
        line.append(3, 4);
        line.pop_front(); // no "1" now
        assert_eq!(line.query_value(2).unwrap(), 3);
        assert_eq!(line.query_agg(2), 7);
    }

    #[test]
    fn test_many_pop_front() {
        let mut line = Line::new(100, 0, 0);
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
        let mut line = Line::new(1000000, 0, 0);
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
        let mut line = Line::new(1000000, 0, 0);
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
        for _n in [
            100u32,
            1000u32,
            10000u32,
            100000u32,
            1000000u32,
            10000000u32,
            100000000u32,
        ]
        .iter()
        {
            let n = *_n;
            let mut l = Line::new(n as usize, 0, 0);
            let now = Instant::now();
            _write_n(&mut l, n); // 199ns

            // print time elasped
            println!("{}", now.elapsed().as_nanos() as f64 / n as f64);

            let mut l = Line::new(n as usize, 0, 0);
            let now = Instant::now();
            _write_pop_n(&mut l, n);

            // print time elasped
            println!("{}", now.elapsed().as_nanos() as f64 / n as f64);
        }
    }
    #[test]
    fn bench_read() {
        for _n in [
            100u32,
            1000u32,
            10000u32,
            100000u32,
            1000000u32,
            10000000u32,
            100000000u32,
        ]
        .iter()
        {
            let n = *_n;
            let mut l = Line::new(n as usize, 0, 0);
            _write_n(&mut l, n);
            let now = Instant::now();
            _query_n(&l, n);
            // print time elasped
            println!("{}", now.elapsed().as_nanos() as f64 / n as f64);
        }
    }
}
