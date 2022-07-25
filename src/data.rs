//! 数据区

use std::collections::VecDeque;

pub trait DataPartition<V> {
    fn push(timestamp: u64, value: V);

    fn query(timestamp: u64, r: u64);
}

struct DataLine<V> {
    data: VecDeque<Option<V>>,

    start: u64,

    end: u64,
}

impl<V> DataLine<V> {
    fn get_idx(&self, timestamp: u64) -> usize {
        (timestamp - self.start) as usize
    }

    pub fn new(length: usize, start: u64, end: u64) -> DataLine<V> {
        let mut data = VecDeque::new();
        data.resize_with(length, || None);

        DataLine { data, start, end }
    }

    /// 获取并移除队列首部元素，取得所有权
    pub fn pop_front(&mut self) -> Option<V> {
        let ret = self.data.pop_front();
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

struct DataPart<V> {
    data: Vec<DataLine<V>>,
}

#[cfg(test)]
mod test {
    #[test]
    fn test_basic() {}
}
