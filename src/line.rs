pub(crate) struct Line<V> {
    data: std::collections::VecDeque<Option<V>>,

    start: u64,

    end: u64,

    agg_fn: fn(V, V) -> V,
}

impl<V> Line<V> {
    fn get_idx(&self, timestamp: u64) -> usize {
        (timestamp - self.start) as usize
    }

    pub fn new(length: usize, start: u64, agg_fn: fn(V, V) -> V) -> Line<V> {
        let mut data = std::collections::VecDeque::with_capacity(length);
        data.resize_with(length, || None);
        // 一开始 end == start, 这时表示为空
        // 半开半闭语义 [start, end)
        Line {
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
        &self.data[self.get_idx(timestamp)]
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

impl<V> crate::traits::Semigroup<V> for Line<V> {
    fn agg_fn(&self) -> fn(V, V) -> V {
        self.agg_fn
    }
}

impl<V: Copy> Line<V> {
    pub fn insert_or_update(&mut self, timestamp: u64, value: V) {
        let idx = self.get_idx(timestamp);
        let x = &mut self.data[idx];
        *x = Some(match x {
            Some(origin_value) => (self.agg_fn)(*origin_value, value),
            None => value,
        });
    }
}

impl<V: Copy> crate::traits::TimestampPush<V> for Line<V> {
    fn push(&mut self, timestamp: u64, value: V) {
        if timestamp + 1 < self.end {
            panic!("line: append a timestamp lower than given before");
        } else {
            self.end = timestamp + 1;
            self.padding(timestamp);
            self.insert_or_update(timestamp, value);
        }
    }
}

#[cfg(test)]
mod test {
    use crate::traits::TimestampPush;

    #[test]
    fn test_basic() {
        let n = 1000;
        let mut p = super::Line::new(100, 0, |a, b| a + b);
        for i in 0..n {
            p.push(i, i);
            assert!(p.query_value(i).unwrap() == i)
        }
    }

    #[test]
    fn test_pop_front() {
        let n = 100;
        let mut p = super::Line::new(100, 0, |a, b| a + b);
        for i in 0..n {
            p.push(i, i);
        }
        for i in 0..n {
            assert_eq!(Some(i), p.pop_front());
        }
        for i in 0..n {
            p.push(i + n, i);
        }
    }
}
