use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;

use mpmc_rust::{BoundedQueue, MutexCondvarQueue};

fn run_unique_item_case(producers: usize, consumers: usize, per_producer: usize, capacity: usize) {
    let total = producers * per_producer;

    let queue = Arc::new(MutexCondvarQueue::with_capacity(capacity));
    let seen = Arc::new(Mutex::new(vec![0usize; total]));
    let pop_tickets = Arc::new(AtomicUsize::new(0));

    let mut consumer_handles = Vec::new();
    for _ in 0..consumers {
        let worker_queue = Arc::clone(&queue);
        let worker_seen = Arc::clone(&seen);
        let worker_tickets = Arc::clone(&pop_tickets);

        consumer_handles.push(thread::spawn(move || loop {
            let ticket = worker_tickets.fetch_add(1, Ordering::AcqRel);
            if ticket >= total {
                break;
            }

            let value = worker_queue.pop();
            let mut guard = worker_seen.lock().unwrap();
            guard[value] += 1;
        }));
    }

    let mut producer_handles = Vec::new();
    for producer_id in 0..producers {
        let worker_queue = Arc::clone(&queue);
        producer_handles.push(thread::spawn(move || {
            let start = producer_id * per_producer;
            let end = start + per_producer;

            for value in start..end {
                worker_queue.push(value);
            }
        }));
    }

    for handle in producer_handles {
        handle.join().unwrap();
    }

    for handle in consumer_handles {
        handle.join().unwrap();
    }

    let guard = seen.lock().unwrap();
    assert!(guard.iter().all(|count| *count == 1));
}

#[test]
fn many_producers_many_consumers_no_loss_no_duplication() {
    run_unique_item_case(4, 4, 2_000, 64);
}

#[test]
fn many_producers_single_consumer_no_loss_no_duplication() {
    run_unique_item_case(8, 1, 1_500, 32);
}

#[test]
fn single_producer_many_consumers_no_loss_no_duplication() {
    run_unique_item_case(1, 8, 1_500, 32);
}

#[test]
fn capacity_one_still_works_under_contention() {
    run_unique_item_case(4, 4, 500, 1);
}

#[test]
fn repeated_stress_matrix() {
    let cases = [
        (1, 1, 400, 1),
        (1, 4, 400, 8),
        (4, 1, 400, 8),
        (4, 4, 400, 16),
        (8, 2, 300, 32),
        (2, 8, 300, 32),
    ];

    for _ in 0..10 {
        for (producers, consumers, per_producer, capacity) in cases {
            run_unique_item_case(producers, consumers, per_producer, capacity);
        }
    }
}
