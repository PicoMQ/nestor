//! Time to first byte over a sliding window, the input to hedge timing. A log2 histogram with
//! eight sub-buckets per octave puts a quantile at most 12.5% above the true value and never
//! below it. Three generations rotate every `ROTATE_EVERY` and reads cover the current and
//! previous one, a window of one to two periods.

use std::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

const MIN_SHIFT: u32 = 16;
const SUB_BITS: u32 = 3;
const SUB_BUCKETS: usize = 1 << SUB_BITS;
const OCTAVES: usize = 24;
const BUCKETS: usize = SUB_BUCKETS * OCTAVES;
const GENERATIONS: usize = 3;
const ROTATE_EVERY: Duration = Duration::from_secs(60);

#[derive(Debug)]
struct Generation {
    counts: [AtomicU32; BUCKETS],
    count: AtomicU64,
    sum_nanos: AtomicU64,
}

impl Generation {
    const fn new() -> Self {
        Self {
            counts: [const { AtomicU32::new(0) }; BUCKETS],
            count: AtomicU64::new(0),
            sum_nanos: AtomicU64::new(0),
        }
    }

    fn record(&self, nanos: u64) {
        self.counts[bucket(nanos)].fetch_add(1, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
        self.sum_nanos.fetch_add(nanos, Ordering::Relaxed);
    }

    fn clear(&self) {
        for count in &self.counts {
            count.store(0, Ordering::Relaxed);
        }
        self.count.store(0, Ordering::Relaxed);
        self.sum_nanos.store(0, Ordering::Relaxed);
    }
}

#[derive(Debug)]
pub struct Latency {
    generations: [Generation; GENERATIONS],
    current: AtomicUsize,
    rotated_at_nanos: AtomicU64,
    epoch: Instant,
}

impl Default for Latency {
    fn default() -> Self {
        Self {
            generations: [const { Generation::new() }; GENERATIONS],
            current: AtomicUsize::new(0),
            rotated_at_nanos: AtomicU64::new(0),
            epoch: Instant::now(),
        }
    }
}

impl Latency {
    pub fn observe(&self, sample: Duration) {
        self.observe_at(sample, Instant::now());
    }

    pub fn mean(&self) -> Option<Duration> {
        self.mean_at(Instant::now())
    }

    pub fn quantile(&self, quantile: f64) -> Option<Duration> {
        self.quantile_at(quantile, Instant::now())
    }

    fn observe_at(&self, sample: Duration, now: Instant) {
        self.rotate(now);
        let nanos = u64::try_from(sample.as_nanos()).unwrap_or(u64::MAX);
        self.generations[self.current.load(Ordering::Acquire)].record(nanos);
    }

    fn mean_at(&self, now: Instant) -> Option<Duration> {
        self.rotate(now);
        let (current, previous) = self.window();
        let count = current.count.load(Ordering::Relaxed) + previous.count.load(Ordering::Relaxed);
        if count == 0 {
            return None;
        }
        let sum =
            current.sum_nanos.load(Ordering::Relaxed) + previous.sum_nanos.load(Ordering::Relaxed);
        Some(Duration::from_nanos(sum / count))
    }

    fn quantile_at(&self, quantile: f64, now: Instant) -> Option<Duration> {
        self.rotate(now);
        let (current, previous) = self.window();
        let mut counts = [0u64; BUCKETS];
        let mut total = 0u64;
        for (slot, (a, b)) in counts
            .iter_mut()
            .zip(current.counts.iter().zip(previous.counts.iter()))
        {
            *slot = u64::from(a.load(Ordering::Relaxed)) + u64::from(b.load(Ordering::Relaxed));
            total += *slot;
        }
        if total == 0 {
            return None;
        }
        let target = rank(quantile, total);
        let mut seen = 0u64;
        for (index, count) in counts.iter().enumerate() {
            seen += count;
            if seen >= target {
                return Some(Duration::from_nanos(upper_edge(index)));
            }
        }
        Some(Duration::from_nanos(upper_edge(BUCKETS - 1)))
    }

    fn window(&self) -> (&Generation, &Generation) {
        let current = self.current.load(Ordering::Acquire);
        let previous = (current + GENERATIONS - 1) % GENERATIONS;
        (&self.generations[current], &self.generations[previous])
    }

    fn rotate(&self, now: Instant) {
        let elapsed = u64::try_from(now.duration_since(self.epoch).as_nanos()).unwrap_or(u64::MAX);
        let last = self.rotated_at_nanos.load(Ordering::Acquire);
        let period = ROTATE_EVERY.as_nanos() as u64;
        let periods = elapsed.saturating_sub(last) / period;
        if periods == 0 {
            return;
        }
        if self
            .rotated_at_nanos
            .compare_exchange(last, elapsed, Ordering::AcqRel, Ordering::Relaxed)
            .is_err()
        {
            return;
        }
        let mut current = self.current.load(Ordering::Acquire);
        for _ in 0..periods.min(GENERATIONS as u64) {
            current = (current + 1) % GENERATIONS;
            self.generations[current].clear();
        }
        self.current.store(current, Ordering::Release);
    }
}

#[allow(clippy::cast_sign_loss)]
fn rank(quantile: f64, total: u64) -> u64 {
    let scaled = (quantile.clamp(0.0, 1.0) * total as f64).ceil();
    (scaled as u64).max(1)
}

fn bucket(nanos: u64) -> usize {
    let value = nanos.max(1 << MIN_SHIFT);
    let log = value.ilog2();
    let octave = (log - MIN_SHIFT) as usize;
    let sub = ((value >> (log - SUB_BITS)) & (SUB_BUCKETS as u64 - 1)) as usize;
    (octave * SUB_BUCKETS + sub).min(BUCKETS - 1)
}

fn upper_edge(index: usize) -> u64 {
    let octave = (index / SUB_BUCKETS) as u32;
    let sub = (index % SUB_BUCKETS) as u64;
    (SUB_BUCKETS as u64 + sub + 1) << (octave + MIN_SHIFT - SUB_BITS)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(latency: &Latency, offset: Duration) -> Instant {
        latency.epoch + offset
    }

    #[test]
    fn buckets_bound_the_sample_from_above_within_an_eighth() {
        for nanos in [
            1,
            65_536,
            70_000,
            1_000_000,
            123_456_789,
            5_000_000_000,
            1 << 39,
        ] {
            let edge = upper_edge(bucket(nanos));
            assert!(edge > nanos, "{nanos} {edge}");
            assert!(
                edge as f64 <= nanos.max(1 << MIN_SHIFT) as f64 * 1.125 + 1.0,
                "{nanos} {edge}"
            );
        }
        assert_eq!(bucket(u64::MAX), BUCKETS - 1);
    }

    #[test]
    fn empty_window_has_no_estimate() {
        let latency = Latency::default();
        assert_eq!(latency.mean(), None);
        assert_eq!(latency.quantile(0.99), None);
    }

    #[test]
    fn mean_is_exact_and_quantile_is_conservative() {
        let latency = Latency::default();
        for millis in 1..=100 {
            latency.observe(Duration::from_millis(millis));
        }
        assert_eq!(latency.mean(), Some(Duration::from_micros(50_500)));
        let p50 = latency.quantile(0.5).unwrap();
        let p99 = latency.quantile(0.99).unwrap();
        assert!(
            p50 >= Duration::from_millis(50) && p50 <= Duration::from_millis(57),
            "{p50:?}"
        );
        assert!(
            p99 >= Duration::from_millis(99) && p99 <= Duration::from_millis(112),
            "{p99:?}"
        );
        assert!(latency.quantile(1.0).unwrap() >= Duration::from_millis(100));
    }

    #[test]
    fn window_covers_one_to_two_periods() {
        let latency = Latency::default();
        latency.observe_at(Duration::from_millis(10), at(&latency, Duration::ZERO));
        let one = at(&latency, ROTATE_EVERY + Duration::from_secs(1));
        assert!(latency.mean_at(one).is_some());
        latency.observe_at(Duration::from_millis(30), one);
        assert_eq!(latency.mean_at(one), Some(Duration::from_millis(20)));
        let two = at(&latency, 2 * ROTATE_EVERY + Duration::from_secs(2));
        assert_eq!(latency.mean_at(two), Some(Duration::from_millis(30)));
        let idle = at(&latency, 10 * ROTATE_EVERY);
        assert_eq!(latency.mean_at(idle), None);
    }

    #[test]
    fn concurrent_observers_are_all_counted() {
        let latency = std::sync::Arc::new(Latency::default());
        let threads: Vec<_> = (0..8)
            .map(|_| {
                let latency = latency.clone();
                std::thread::spawn(move || {
                    for _ in 0..1000 {
                        latency.observe(Duration::from_millis(5));
                    }
                })
            })
            .collect();
        for thread in threads {
            thread.join().unwrap();
        }
        let (current, previous) = latency.window();
        assert_eq!(
            current.count.load(Ordering::Relaxed) + previous.count.load(Ordering::Relaxed),
            8000
        );
        assert_eq!(latency.mean(), Some(Duration::from_millis(5)));
    }
}
