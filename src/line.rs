use std::{
    cell::RefCell,
    collections::VecDeque,
    fs::File,
    io::{self, Write},
    ops::{Index, IndexMut},
    sync::Mutex,
    time::Duration,
};

use lazy_static::lazy_static;

use crate::traits::{Semigroup, TimestampPush};

#[cfg(feature = "trace_io")]
use crate::kv::KVStorage;

#[cfg(feature = "trace_io")]
lazy_static! {
    static ref KV: Mutex<KVStorage<(i32, u64)>> =
        KVStorage::new(500, Duration::from_micros(2)).into();
}

#[cfg(feature = "trace_io")]
pub(crate) struct Stat {
    id: i32,
    read: RefCell<Vec<usize>>,
    write: Vec<usize>,
    miss: RefCell<Vec<usize>>,
    resize: Vec<usize>,
}

#[cfg(feature = "trace_io")]
impl Stat {
    fn new(id: i32) -> Self {
        Stat {
            id,
            read: Vec::new().into(),
            write: Vec::new(),
            miss: Vec::new().into(),
            resize: Vec::new(),
        }
    }

    pub(crate) fn dump(&self, file: &mut File) -> io::Result<()> {
        writeln!(file, "line_id {}", self.id)?;

        write!(file, "read")?;
        for &x in self.read.borrow().iter() {
            write!(file, " {}", x)?;
        }
        writeln!(file)?;

        write!(file, "write")?;
        for &x in &self.write {
            write!(file, " {}", x)?;
        }
        writeln!(file)?;

        write!(file, "miss")?;
        for &x in self.miss.borrow().iter() {
            write!(file, " {}", x)?;
        }
        writeln!(file)?;

        write!(file, "resize")?;
        for &x in &self.resize {
            write!(file, " {}", x)?;
        }
        writeln!(file)?;

        Ok(())
    }
}

pub(crate) struct Line<V> {
    data: VecDeque<Option<V>>,

    start: u64,

    end: u64,

    agg_fn: fn(V, V) -> V,

    #[cfg(feature = "trace_io")]
    pub(crate) stat: Stat,
}

impl<V> Line<V> {
    fn get_idx(&self, timestamp: u64) -> usize {
        (timestamp - self.start) as usize
    }

    pub fn new(id: i32, length: usize, start: u64, agg_fn: fn(V, V) -> V) -> Line<V> {
        let mut data = VecDeque::with_capacity(length);
        data.resize_with(length, || None);
        // 一开始 end == start, 这时表示为空
        // 半开半闭语义 [start, end)
        Line {
            data,
            start,
            end: start,
            agg_fn,

            #[cfg(feature = "trace_io")]
            stat: Stat::new(id),
        }
    }

    /// 获取并移除队列首部元素，取得所有权
    pub fn pop_front(&mut self) -> Option<V> {
        let ret = self.data.pop_front();
        self.start += 1;
        ret.unwrap_or(None)
    }
}

// 查询位于 timestamp 处的具体数值

impl<V> Index<u64> for Line<V> {
    type Output = Option<V>;

    fn index(&self, timestamp: u64) -> &Self::Output {
        if cfg!(feature = "trace_io") {
            let mut read_vec = self.stat.read.borrow_mut();
            if timestamp as usize >= read_vec.len() {
                read_vec.resize(timestamp as usize + 1, 0);
            }
            read_vec[timestamp as usize] += 1;

            if !KV.lock().unwrap().get((self.stat.id, timestamp)) {
                let mut miss_vec = self.stat.miss.borrow_mut();
                if timestamp as usize >= miss_vec.len() {
                    miss_vec.resize(timestamp as usize + 1, 0);
                }
                miss_vec[timestamp as usize] += 1;
            }
        }

        &self.data[self.get_idx(timestamp)]
    }
}

impl<V> IndexMut<u64> for Line<V> {
    fn index_mut(&mut self, timestamp: u64) -> &mut Self::Output {
        if cfg!(feature = "trace_io") {
            let vec = &mut self.stat.write;
            if timestamp as usize >= vec.len() {
                vec.resize(timestamp as usize + 1, 0);
            }

            vec[timestamp as usize] += 1;

            KV.lock().unwrap().set((self.stat.id, timestamp));
        }

        let idx = self.get_idx(timestamp);
        &mut self.data[idx]
    }
}

impl<V> Line<V> {
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
        if idx >= self.data.len() {
            if cfg!(feature = "trace_io") {
                self.stat.resize.push(idx + 1 - self.data.len());
            }

            self.data.resize_with(idx + 1, || None);
        }
    }
}

impl<V> Semigroup<V> for Line<V> {
    fn agg_fn(&self) -> fn(V, V) -> V {
        self.agg_fn
    }
}

impl<V: Copy> Line<V> {
    pub fn insert_or_update(&mut self, timestamp: u64, value: V) {
        self[timestamp] = Some(match self[timestamp] {
            Some(origin_value) => (self.agg_fn)(origin_value, value),
            None => value,
        })
    }
}

impl<V: Copy> TimestampPush<V> for Line<V> {
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
        let mut p = super::Line::new(-1, 100, 0, |a, b| a + b);
        for i in 0..n {
            p.push(i, i);
            assert_eq!(p[i], Some(i))
        }
    }

    #[test]
    fn test_pop_front() {
        let n = 100;
        let mut p = super::Line::new(-1, 100, 0, |a, b| a + b);
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
