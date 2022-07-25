pub trait Semigroup<V> {
    fn agg_fn(&self) -> fn(V, V) -> V;
}

pub trait Aggregate<V>: Semigroup<V> {
    // 返回单位元
    fn identity(&self) -> V;
    /// 查询聚合值（缓存）
    fn agg(&self, timestamp: u64) -> Option<V>;

    /// 插入并设置值为 value
    fn set_agg(&mut self, timestamp: u64, value: V);

    /// 检查时间戳是否越界
    fn check_bound(&self, timestamp: u64) -> bool;
}
