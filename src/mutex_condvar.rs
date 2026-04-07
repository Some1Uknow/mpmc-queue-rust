use std::collections::VecDeque;
use std::sync::{Condvar, Mutex, MutexGuard, PoisonError};

use crate::BoundedQueue;

struct Inner<T> {
    buf: VecDeque<T>,
    capacity: usize,
}

impl<T> Inner<T> {
    fn is_full(&self) -> bool {
        self.buf.len() == self.capacity
    }

    fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }
}

pub struct MutexCondvarQueue<T> {
    inner: Mutex<Inner<T>>,
    not_empty: Condvar,
    not_full: Condvar,
}

impl<T> MutexCondvarQueue<T> {
    pub fn with_capacity(capacity: usize) -> Self {
        assert!(capacity > 0, "capacity must be > 0");

        Self {
            inner: Mutex::new(Inner {
                buf: VecDeque::with_capacity(capacity),
                capacity,
            }),
            not_empty: Condvar::new(),
            not_full: Condvar::new(),
        }
    }

    fn lock_inner(&self) -> std::sync::MutexGuard<'_, Inner<T>> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn wait_while_full<'a>(&self, guard: MutexGuard<'a, Inner<T>>) -> MutexGuard<'a, Inner<T>> {
        self.not_full
            .wait_while(guard, |inner| inner.is_full())
            .unwrap_or_else(PoisonError::into_inner)
    }

    fn wait_while_empty<'a>(&self, guard: MutexGuard<'a, Inner<T>>) -> MutexGuard<'a, Inner<T>> {
        self.not_empty
            .wait_while(guard, |inner| inner.is_empty())
            .unwrap_or_else(PoisonError::into_inner)
    }
}

impl<T: Send> BoundedQueue<T> for MutexCondvarQueue<T> {
    fn new(capacity: usize) -> Self {
        Self::with_capacity(capacity)
    }

    fn push(&self, item: T) {
        let mut guard = self.wait_while_full(self.lock_inner());
        guard.buf.push_back(item);
        self.not_empty.notify_one();
    }

    fn pop(&self) -> T {
        let mut guard = self.wait_while_empty(self.lock_inner());
        let item = guard
            .buf
            .pop_front()
            .expect("queue was checked as non-empty");
        self.not_full.notify_one();
        item
    }

    fn try_push(&self, item: T) -> Result<(), T> {
        let mut guard = self.lock_inner();

        if guard.is_full() {
            return Err(item);
        }

        guard.buf.push_back(item);
        self.not_empty.notify_one();
        Ok(())
    }

    fn try_pop(&self) -> Option<T> {
        let mut guard = self.lock_inner();
        let item = guard.buf.pop_front();

        if item.is_some() {
            self.not_full.notify_one();
        }

        item
    }
}
