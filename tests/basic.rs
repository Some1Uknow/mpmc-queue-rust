use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use mpmc_rust::{BoundedQueue, MutexCondvarQueue};

struct DropTracker {
    drops: Arc<AtomicUsize>,
}

impl Drop for DropTracker {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
#[should_panic(expected = "capacity must be > 0")]
fn rejects_zero_capacity() {
    let _ = MutexCondvarQueue::<u32>::with_capacity(0);
}

#[test]
fn try_ops_work_on_empty_and_full_queue() {
    let queue = MutexCondvarQueue::with_capacity(2);

    assert_eq!(queue.try_pop(), None);
    assert_eq!(queue.try_push(10), Ok(()));
    assert_eq!(queue.try_push(20), Ok(()));
    assert_eq!(queue.try_push(30), Err(30));
    assert_eq!(queue.try_pop(), Some(10));
    assert_eq!(queue.try_pop(), Some(20));
    assert_eq!(queue.try_pop(), None);
}

#[test]
fn preserves_fifo_order_single_threaded() {
    let queue = MutexCondvarQueue::with_capacity(3);

    queue.push(1);
    queue.push(2);
    queue.push(3);

    assert_eq!(queue.pop(), 1);
    assert_eq!(queue.pop(), 2);
    assert_eq!(queue.pop(), 3);
}

#[test]
fn trait_constructor_works() {
    let queue = <MutexCondvarQueue<u32> as BoundedQueue<u32>>::new(1);

    assert_eq!(queue.try_push(7), Ok(()));
    assert_eq!(queue.try_pop(), Some(7));
}

#[test]
fn try_push_returns_original_item_when_full() {
    let queue = MutexCondvarQueue::with_capacity(1);

    assert_eq!(queue.try_push(String::from("first")), Ok(()));
    assert_eq!(
        queue.try_push(String::from("second")),
        Err(String::from("second"))
    );
}

#[test]
fn dropping_queue_drops_remaining_items_once() {
    let drops = Arc::new(AtomicUsize::new(0));

    {
        let queue = MutexCondvarQueue::with_capacity(2);
        queue.push(DropTracker {
            drops: Arc::clone(&drops),
        });
        queue.push(DropTracker {
            drops: Arc::clone(&drops),
        });
    }

    assert_eq!(drops.load(Ordering::SeqCst), 2);
}
