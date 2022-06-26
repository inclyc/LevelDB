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

    pub fn append(&mut self, timestamp: u64, value: V) {
        let target_index = timestamp - self.offset;
        while target_index >= self.data.len().try_into().unwrap() {
            self.data.push_back(None);
            self.aggregate.push_back(None);
        }
        let mut current_index = target_index;
        let mut updating_timestamp = timestamp;
        if self.current_timestamp > timestamp {
            panic!("line: append a timestamp lower than before given");
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
        loop {
            let agg = self
                .aggregate
                .get_mut(current_index.try_into().unwrap())
                .unwrap();
            match agg {
                Some(origin_value) => *origin_value += value,
                None => *agg = Some(value),
            }
            if updating_timestamp == 0 {
                break;
            }
            let step = 1 << (updating_timestamp.trailing_zeros());

            // 全程要求 current_index >= 0
            // 下一步的timestamp为 updating_timestamp - step
            // 换算成 current_index = updating_timestamp - step - offset >= 0
            // 用加法而不是用0作比较是因为这里是无符号整数，updating_timestamp始终 >= 0
            if updating_timestamp < step + self.offset {
                // i.e timestamp - step - offset < 0, buf we use u64 here
                break;
            }
            updating_timestamp -= step;
            current_index = updating_timestamp - self.offset; // > 0
        }
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
        for i in 1..10000 {
            line.append(i * 2, i);
        }
        assert_eq!(line.query_agg(2), 49995000);
    }
}
