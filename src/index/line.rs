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

    pub fn append(&mut self, timestamp: u64, value: V) {
        let target_index = timestamp - self.offset;
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
            let step = 1 << (updating_timestamp.trailing_zeros());

            // 全程要求 current_index > 0
            // 下一步的timestamp为 updating_timestamp - step
            // 换算成 current_index = updating_timestamp - step - offset > 0
            // 用加法而不是用0作比较是因为这里是无符号整数，updating_timestamp始终大于0
            if updating_timestamp <= step + self.offset {
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

    pub fn query_agg(&self, timestamp: u64) -> V {
        let mut aggregating_index = timestamp - self.offset;
        let mut result = self
            .data
            .get(aggregating_index.try_into().unwrap())
            .unwrap()
            .unwrap();
        let mut aggregating_timestamp = timestamp;
        loop {
            let step = 1 << (aggregating_timestamp.trailing_zeros());
            // next = timestamp - offset + step < self.current_timestamp
            if aggregating_timestamp + step >= self.offset + (self.current_timestamp as u64) {
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
        return result;
    }
}

#[cfg(test)]
mod tests {
    use super::Line;
    #[test]
    fn test_line() {
        let mut line = Line::new(100, 0, 0);
        line.append(1, 2);
        line.append(2, 3);
        line.append(3, 4);
        assert_eq!(line.query_value(1).unwrap(), 2);
        assert_eq!(line.query_agg(1), 9);
    }

    #[test]
    fn test_many_write() {
        let mut line = Line::new(1000000, 0, 0);
        for i in 1..1000000 {
            line.append(i, i);
        }
        assert_eq!(line.query_agg(1), 499999500000);
        for i in 1..100 {
            assert_eq!(line.query_value(i).unwrap(), i);
        }
    }
}
