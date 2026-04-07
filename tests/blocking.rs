use std::sync::{mpsc, Arc};
use std::thread;
use std::time::Duration;

use mpmc_rust::{BoundedQueue, MutexCondvarQueue};

#[test]
fn pop_blocks_until_item_arrives() {
    let queue = Arc::new(MutexCondvarQueue::with_capacity(1));
    let (started_tx, started_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();

    let worker_queue = Arc::clone(&queue);
    let handle = thread::spawn(move || {
        started_tx.send(()).unwrap();
        let value = worker_queue.pop();
        done_tx.send(value).unwrap();
    });

    started_rx.recv().unwrap();
    thread::sleep(Duration::from_millis(50));
    assert!(done_rx.try_recv().is_err());

    queue.push(42);

    assert_eq!(done_rx.recv_timeout(Duration::from_secs(1)).unwrap(), 42);
    handle.join().unwrap();
}

#[test]
fn push_blocks_until_space_arrives() {
    let queue = Arc::new(MutexCondvarQueue::with_capacity(1));
    queue.push(7);

    let (started_tx, started_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();

    let worker_queue = Arc::clone(&queue);
    let handle = thread::spawn(move || {
        started_tx.send(()).unwrap();
        worker_queue.push(9);
        done_tx.send(()).unwrap();
    });

    started_rx.recv().unwrap();
    thread::sleep(Duration::from_millis(50));
    assert!(done_rx.try_recv().is_err());

    assert_eq!(queue.pop(), 7);

    done_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(queue.pop(), 9);
    handle.join().unwrap();
}
