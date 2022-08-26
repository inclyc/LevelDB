pub trait Semigroup<V> {
    fn agg_fn(&self) -> fn(V, V) -> V;
}

pub trait Monoid<V>: Semigroup<V> {
    // 返回单位元
    fn identity(&self) -> V;
}

pub trait CheckBound<V> {
    fn check_bound(&self, timestamp: u64) -> bool;
}

pub trait TimestampPush<V> {
    /// timestamp 处设置值 value
    fn push(&mut self, timestamp: u64, value: V);
}

pub trait ConstrainedQuery<V> {
    /// 查询 [timestamp, x) 的聚合值
    /// 满足 x <= r
    /// 同时 x 应尽量大
    fn constrained_query(&self, timestamp: u64, r: u64) -> Option<(V, u64)>;
}

pub trait GreedyQuery<V> {
    /// 查询 [timestamp , x) 的聚合值
    /// 无约束条件， x 越大越好
    /// 当 x 越界的时候返回 None
    fn greedy_query(&self, timestamp: u64) -> Option<(V, u64)>;
}

pub trait SuffixQuery<V>: GreedyQuery<V> + Semigroup<V> {
    fn suffix_query(&self, timestamp: u64) -> Option<(V, u64)> {
        self.greedy_query(timestamp)
            .map(|(v, r)| match self.suffix_query(r) {
                Some((fv, fr)) => (self.agg_fn()(fv, v), fr),
                None => (v, r),
            })
    }
}

pub trait RangeQuery<V>: ConstrainedQuery<V> + Semigroup<V> {
    fn range_query(&self, l: u64, r: u64) -> Option<V> {
        self.constrained_query(l, r).and_then(|(sum, cqr)| {
            if cqr == r {
                Some(sum)
            } else {
                self.range_query(cqr, r)
                    .map(|half| self.agg_fn()(sum, half))
            }
        })
    }
}
