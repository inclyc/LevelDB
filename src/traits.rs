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
        match self.greedy_query(timestamp) {
            Some((v, r)) => {
                let f = self.agg_fn();
                match self.suffix_query(r) {
                    Some((fv, fr)) => Some((f(fv, v), fr)),
                    None => Some((v, r)),
                }
            }
            None => None,
        }
    }
}

pub trait RangeQuery<V>: ConstrainedQuery<V> + Semigroup<V> {
    fn range_query(&self, l: u64, r: u64) -> Option<V> {
        match self.constrained_query(l, r) {
            Some((sum, cqr)) => {
                if cqr == r {
                    Some(sum)
                } else {
                    match self.range_query(cqr, r) {
                        Some(half) => Some(self.agg_fn()(sum, half)),
                        None => None,
                    }
                }
            }
            None => None,
        }
    }
}
