use std::env;
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use mpmc_rust::{BoundedQueue, MutexCondvarQueue};

const SENTINEL: u64 = u64::MAX;
const DEFAULT_DURATION_SECS: u64 = 3;
const SYMMETRIC_PAIRS: &[usize] = &[1, 2, 4, 8, 16];
const CAPACITIES: &[usize] = &[64, 256, 1024];
const ASYMMETRIC_CASES: &[(usize, usize, usize)] = &[(8, 2, 256), (2, 8, 256)];

#[derive(Clone, Copy)]
struct Scenario {
    producers: usize,
    consumers: usize,
    capacity: usize,
    duration_secs: u64,
}

impl Scenario {
    const fn new(producers: usize, consumers: usize, capacity: usize, duration_secs: u64) -> Self {
        Self {
            producers,
            consumers,
            capacity,
            duration_secs,
        }
    }

    fn label(&self) -> String {
        format!(
            "p{}_c{}_cap{}",
            self.producers, self.consumers, self.capacity
        )
    }
}

struct SharedState {
    stop: AtomicBool,
    measure: AtomicBool,
    push_ops: AtomicU64,
    pop_ops: AtomicU64,
    start_barrier: Barrier,
}

impl SharedState {
    fn new(worker_count: usize) -> Self {
        Self {
            stop: AtomicBool::new(false),
            measure: AtomicBool::new(false),
            push_ops: AtomicU64::new(0),
            pop_ops: AtomicU64::new(0),
            start_barrier: Barrier::new(worker_count),
        }
    }

    fn measuring(&self) -> bool {
        self.measure.load(Ordering::Relaxed)
    }
}

struct ScenarioResult {
    queue_name: &'static str,
    scenario: Scenario,
    measured_secs: f64,
    total_wall_secs: f64,
    push_ops: u64,
    pop_ops: u64,
}

impl ScenarioResult {
    fn throughput_ops_per_sec(&self) -> f64 {
        (self.push_ops + self.pop_ops) as f64 / self.measured_secs
    }
}

impl fmt::Display for ScenarioResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{},{},{},{},{},{:.3},{:.3},{},{},{:.2}",
            self.queue_name,
            self.scenario.label(),
            self.scenario.producers,
            self.scenario.consumers,
            self.scenario.capacity,
            self.measured_secs,
            self.total_wall_secs,
            self.push_ops,
            self.pop_ops,
            self.throughput_ops_per_sec()
        )
    }
}

fn main() {
    let duration_override = env_duration_override();
    let scenario_filter = env::var("SCENARIO_FILTER").ok();

    println!(
        "queue,scenario,producers,consumers,capacity,measured_secs,total_wall_secs,push_ops,pop_ops,throughput_ops_per_sec"
    );

    for scenario in scenarios(duration_override) {
        if let Some(filter) = scenario_filter.as_deref() {
            if !scenario.label().contains(filter) {
                continue;
            }
        }

        let result = run_scenario::<MutexCondvarQueue<u64>>("mutex_condvar", scenario);
        println!("{result}");
    }
}

fn scenarios(duration_override: Option<u64>) -> Vec<Scenario> {
    let duration_secs = duration_override.unwrap_or(DEFAULT_DURATION_SECS);
    let mut scenarios = Vec::new();

    for &capacity in CAPACITIES {
        for &pairs in SYMMETRIC_PAIRS {
            scenarios.push(Scenario::new(pairs, pairs, capacity, duration_secs));
        }
    }

    for &(producers, consumers, capacity) in ASYMMETRIC_CASES {
        scenarios.push(Scenario::new(producers, consumers, capacity, duration_secs));
    }

    scenarios
}

fn env_duration_override() -> Option<u64> {
    env::var("BENCH_DURATION_SECS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
}

fn run_scenario<Q>(queue_name: &'static str, scenario: Scenario) -> ScenarioResult
where
    Q: BoundedQueue<u64> + 'static,
{
    let queue = Arc::new(Q::new(scenario.capacity));
    let shared = Arc::new(SharedState::new(
        scenario.producers + scenario.consumers + 1,
    ));

    let mut consumer_handles = Vec::with_capacity(scenario.consumers);
    for _ in 0..scenario.consumers {
        consumer_handles.push(spawn_consumer(Arc::clone(&queue), Arc::clone(&shared)));
    }

    let mut producer_handles = Vec::with_capacity(scenario.producers);
    for producer_id in 0..scenario.producers {
        producer_handles.push(spawn_producer(
            Arc::clone(&queue),
            Arc::clone(&shared),
            producer_id,
        ));
    }

    let start = Instant::now();
    shared.measure.store(true, Ordering::Relaxed);
    shared.start_barrier.wait();
    thread::sleep(Duration::from_secs(scenario.duration_secs));
    shared.measure.store(false, Ordering::Relaxed);
    shared.stop.store(true, Ordering::Relaxed);

    for handle in producer_handles {
        handle.join().unwrap();
    }

    // Consumers may be blocked on an empty queue, so push one sentinel per consumer.
    for _ in 0..scenario.consumers {
        queue.push(SENTINEL);
    }

    for handle in consumer_handles {
        handle.join().unwrap();
    }

    ScenarioResult {
        queue_name,
        scenario,
        measured_secs: scenario.duration_secs as f64,
        total_wall_secs: start.elapsed().as_secs_f64(),
        push_ops: shared.push_ops.load(Ordering::Relaxed),
        pop_ops: shared.pop_ops.load(Ordering::Relaxed),
    }
}

fn spawn_consumer<Q>(queue: Arc<Q>, shared: Arc<SharedState>) -> thread::JoinHandle<()>
where
    Q: BoundedQueue<u64> + 'static,
{
    thread::spawn(move || {
        let mut local_pop_ops = 0;
        shared.start_barrier.wait();

        loop {
            let value = queue.pop();
            if value == SENTINEL {
                break;
            }

            if shared.measuring() {
                local_pop_ops += 1;
            }
        }

        shared.pop_ops.fetch_add(local_pop_ops, Ordering::Relaxed);
    })
}

fn spawn_producer<Q>(
    queue: Arc<Q>,
    shared: Arc<SharedState>,
    producer_id: usize,
) -> thread::JoinHandle<()>
where
    Q: BoundedQueue<u64> + 'static,
{
    thread::spawn(move || {
        let mut local_push_ops = 0;
        let mut next_value = producer_id as u64;
        shared.start_barrier.wait();

        while !shared.stop.load(Ordering::Relaxed) {
            queue.push(next_value);
            if shared.measuring() {
                local_push_ops += 1;
            }

            next_value = next_producer_value(next_value, producer_id);
        }

        shared.push_ops.fetch_add(local_push_ops, Ordering::Relaxed);
    })
}

fn next_producer_value(current: u64, producer_id: usize) -> u64 {
    let next = current.wrapping_add(1024);
    if next == SENTINEL {
        producer_id as u64
    } else {
        next
    }
}
