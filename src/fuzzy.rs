use crate::{line::Line, tree::Tree};

/// 模糊查询数据结构
/// 1. 插入： timestamp时间戳，增加值value
/// 2. 查询: 精度（层级）+ 时间戳L , R
pub struct Fuzzy<V> {
    /// 储存模糊查询的一系列 Line
    /// 每个级别升高，相当于时间戳右移
    level: Vec<Line<V>>,

    agg_fn: fn(V, V) -> V,
}

impl<V: Copy> Fuzzy<V> {
    /// 向 Fuzzy 数据结构中加入 timestamp, value 数值。
    /// 如果 timestamp 已经存在，则应用 +=  语义
    /// 如果 timestamp 不存在，在需要的情况下合理的扩宽
    /// panic: 如果timestamp是一个过去的时间，此函数panic
    pub fn append(&mut self, timestamp: u64, value: V) {
        todo!()
    }

    /// 按照 level 指定的精度查询 timestamp 后缀聚合
    /// 空数据结构，返回单位元
    pub fn query(&self, level: usize, timestamp: u64) -> V {
        todo!()
    }

    /// 获取 level 所对应的时间戳的范围
    /// 半开半闭区间， [L, R)
    pub fn get_range(&self, level: usize) -> (u64, u64) {
        todo!()
    }
}
