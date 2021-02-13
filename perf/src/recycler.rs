use rand::{thread_rng, Rng};
use std::sync::atomic::AtomicBool;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};

#[derive(Debug, Default)]
struct RecyclerStats {
    total: AtomicUsize,
    freed: AtomicUsize,
    reuse: AtomicUsize,
    max_gc: AtomicUsize,
}

#[derive(Clone, Default)]
pub struct Recycler<T> {
    recycler: Arc<RecyclerX<T>>,
}

#[derive(Debug)]
pub struct RecyclerX<T> {
    gc: Mutex<Vec<T>>,
    stats: RecyclerStats,
    id: usize,
    pub outstanding_len: AtomicUsize,
    limit: Option<usize>,
}

impl<T: Default> Default for RecyclerX<T> {
    fn default() -> RecyclerX<T> {
        let id = thread_rng().gen_range(0, 1000);
        trace!("new recycler..{}", id);
        RecyclerX {
            gc: Mutex::new(vec![]),
            stats: RecyclerStats::default(),
            id,
            limit: None,
            outstanding_len: AtomicUsize::default(),
        }
    }
}

impl<T: Default> RecyclerX<T> {
    fn new(limit: Option<usize>) -> Self {
        RecyclerX {
            limit,
            ..Self::default()
        }
    }
}

pub trait Reset {
    fn len(&self) -> usize;
    fn reset(&mut self);
    fn warm(&mut self, size_hint: usize);
    fn set_recycler(&mut self, recycler: Weak<RecyclerX<Self>>)
    where
        Self: std::marker::Sized;
}

lazy_static! {
    static ref WARM_RECYCLERS: AtomicBool = AtomicBool::new(false);
}

pub fn enable_recycler_warming() {
    WARM_RECYCLERS.store(true, Ordering::Relaxed);
}

fn warm_recyclers() -> bool {
    WARM_RECYCLERS.load(Ordering::Relaxed)
}

impl<T: Default + Reset + Sized> Recycler<T> {
    pub fn warmed(num: usize, size_hint: usize, limit: Option<usize>) -> Self {
        assert!(num <= limit.unwrap_or(std::usize::MAX));
        let new = Self {
            recycler: Arc::new(RecyclerX::new(limit)),
        };
        if warm_recyclers() {
            let warmed_items: Vec<_> = (0..num)
                .map(|_| {
                    let mut item = new.allocate("warming").unwrap();
                    item.warm(size_hint);
                    item
                })
                .collect();
            warmed_items
                .into_iter()
                .for_each(|i| new.recycler.recycle(i));
        }
        new
    }

    pub fn allocate(&self, name: &'static str) -> Option<T> {
        let new = self
            .recycler
            .gc
            .lock()
            .expect("recycler lock in pb fn allocate")
            .pop();

        if let Some(mut x) = new {
            self.recycler.stats.reuse.fetch_add(1, Ordering::Relaxed);
            x.reset();
            return Some(x);
        }

        let total = self.recycler.stats.total.fetch_add(1, Ordering::Relaxed);
        info!(
            "RECYCLER: allocating new: total {} {:?} id: {} reuse: {} max_gc: {}",
            total,
            name,
            self.recycler.id,
            self.recycler.stats.reuse.load(Ordering::Relaxed),
            self.recycler.stats.max_gc.load(Ordering::Relaxed),
        );

        let mut t = T::default();
        t.set_recycler(Arc::downgrade(&self.recycler));
        let should_allocate = self
            .recycler
            .limit
            .map(|limit| self.recycler.outstanding_len.load(Ordering::SeqCst) + t.len() <= limit)
            .unwrap_or(true);
        if should_allocate {
            self.recycler
                .outstanding_len
                .fetch_add(t.len(), Ordering::SeqCst);
            Some(t)
        } else {
            None
        }
    }
}

impl<T: Default + Reset> RecyclerX<T> {
    pub fn recycle(&self, x: T) {
        let len = {
            let mut gc = self.gc.lock().expect("recycler lock in pub fn recycle");
            gc.push(x);
            gc.len()
        };

        let max_gc = self.stats.max_gc.load(Ordering::Relaxed);
        if len > max_gc {
            // this is not completely accurate, but for most cases should be fine.
            let _ = self.stats.max_gc.compare_exchange(
                max_gc,
                len,
                Ordering::Relaxed,
                Ordering::Relaxed,
            );
        }
        let total = self.stats.total.load(Ordering::Relaxed);
        let reuse = self.stats.reuse.load(Ordering::Relaxed);
        let freed = self.stats.total.fetch_add(1, Ordering::Relaxed);
        if self.gc.lock().unwrap().len() % 1000 == 0 {
            datapoint_info!(
                "recycler",
                ("gc_len", len as i64, i64),
                (
                    "outstanding_len",
                    self.outstanding_len.load(Ordering::Relaxed) as i64,
                    i64
                ),
                ("total", total as i64, i64),
                ("freed", freed as i64, i64),
                ("reuse", reuse as i64, i64),
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    impl Reset for u64 {
        fn reset(&mut self) {
            *self = 10;
        }
        fn len(&self) -> usize {
            1
        }
        fn warm(&mut self, _size_hint: usize) {}
        fn set_recycler(&mut self, _recycler: Weak<RecyclerX<Self>>) {}
    }

    #[test]
    fn test_recycler() {
        let recycler = Recycler::default();
        let mut y: u64 = recycler.allocate("test_recycler1");
        assert_eq!(y, 0);
        y = 20;
        let recycler2 = recycler.clone();
        recycler2.recycler.recycle(y);
        assert_eq!(recycler.recycler.gc.lock().unwrap().len(), 1);
        let z = recycler.allocate("test_recycler2");
        assert_eq!(z, 10);
        assert_eq!(recycler.recycler.gc.lock().unwrap().len(), 0);
    }
}
