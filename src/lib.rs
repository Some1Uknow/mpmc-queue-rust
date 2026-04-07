mod mutex_condvar;

pub use mutex_condvar::MutexCondvarQueue;

pub trait BoundedQueue<T: Send>: Send + Sync {
    fn new(capacity: usize) -> Self
    where
        Self: Sized;

    fn push(&self, item: T);
    fn pop(&self) -> T;
    fn try_push(&self, item: T) -> Result<(), T>;
    fn try_pop(&self) -> Option<T>;
}
