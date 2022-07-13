pub struct Metric<T> {
    timestamp: u64,
    value: T
}

impl<V> Metric<V> {
    pub fn new(timestamp: u64, value: V) -> Metric<V> {
        Metric { timestamp, value }
    }
    pub fn value(&self) -> &V {
        &self.value
    }
    
    pub fn set_value(&mut self, value: V) {
        self.value = value;
    }

    #[inline]
    pub fn timestamp(&self) -> u64 {
        self.timestamp
    }
}

impl<V> PartialEq for Metric<V> {
    fn eq(&self, other: &Self) -> bool {
        self.timestamp == other.timestamp
    }
}

impl<V> Eq for Metric<V> {}

impl<V> PartialOrd for Metric<V> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.timestamp.partial_cmp(&other.timestamp)
    }
}

impl<V> Ord for Metric<V> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.timestamp.cmp(&other.timestamp)
    }
}
