pub trait Tree<V: Copy> {
    /// 查询聚合值（缓存）
    fn agg(&self, timestamp: u64) -> Option<V>;

    /// 插入并设置值为 value
    fn set_agg(&mut self, timestamp: u64, value: V);

    /// 查询值
    fn value(&self, timestamp: u64) -> Option<V>;

    /// 检查时间戳是否越界
    fn check_bound(&self, timestamp: u64) -> bool;

    // 返回单位元
    fn identity(&self) -> V;

    fn agg_fn(&self) -> fn(V, V) -> V;

    /// 如果timestamp处对应的值不存在，应该返回单位元
    fn agg_or_identity(&self, timestamp: u64) -> V {
        match self.agg(timestamp) {
            Some(v) => v,
            None => self.identity(),
        }
    }

    /// 如果timestamp处对应的值不存在，应该返回单位元
    fn value_or_identity(&self, timestamp: u64) -> V {
        match self.value(timestamp) {
            Some(v) => v,
            None => self.identity(),
        }
    }

    /// 插入，类似set_agg, 但是这次用+=
    fn add_agg(&mut self, timestamp: u64, value: V) {
        self.set_agg(
            timestamp,
            self.agg_fn()(value, self.agg_or_identity(timestamp)),
        )
    }

    /// 聚合查询某一Timestamp之后的值
    /// 如果 timestamp 这个时间戳从来没有插入过，不会影响结果
    /// 查询不到任何值，返回单位元
    /// O(logn)
    fn query_agg(&self, timestamp: u64) -> V {
        let mut timestamp = timestamp;
        let mut result = self.agg_or_identity(timestamp);
        loop {
            let step = 1 << (timestamp.trailing_zeros());
            if !self.check_bound(timestamp + step) {
                break;
            }
            timestamp = timestamp + step;
            result = self.agg_fn()(result, self.agg_or_identity(timestamp));
        }
        result
    }

    /// 查询时间戳区间 [start, end) 的所有数据(聚合值)
    /// 查询不到任何值，返回单位元
    fn query_range(&self, start: u64, end: u64) -> V {
        if start == end {
            return self.identity();
        }
        if start + 1 == end {
            self.value_or_identity(start)
        } else {
            let step = 1 << (start.trailing_zeros());
            if step + start > end {
                self.agg_fn()(
                    self.query_range(start + 1, end),
                    self.value_or_identity(start),
                )
            } else {
                self.agg_fn()(
                    self.query_range(start + step, end),
                    self.agg_or_identity(start),
                )
            }
        }
    }

    /// 将某个位置的聚合值更新
    /// += V
    fn update_aggregate(&mut self, timestamp: u64, value: V) {
        let mut timestamp = timestamp;
        loop {
            self.add_agg(timestamp, value);
            if timestamp == 0 {
                break;
            }
            let step = 1 << (timestamp.trailing_zeros()); // timestamp != 0
            if !self.check_bound(timestamp ^ step) {
                break;
            }
            timestamp ^= step;
        }
    }
}
