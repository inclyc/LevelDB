/// query的查询结果为此类型的集合
pub struct Metrics<V> {
    pub timestamp: u64,
    pub value: V,
}

impl<V> PartialEq for Metrics<V> {
    fn eq(&self, other: &Self) -> bool {
        self.timestamp == other.timestamp
    }
}

impl<V> Eq for Metrics<V> {}

impl<V> PartialOrd for Metrics<V> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.timestamp.partial_cmp(&other.timestamp)
    }
}

impl<V> Ord for Metrics<V> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.timestamp.cmp(&other.timestamp)
    }
}
