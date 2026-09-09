//! Scenarios as phases of consumer event lists. A phase runs its consumers to completion, or until
//! its deadline when it repeats, and the executor snapshots counters at each boundary.

use std::ops::Range;
use std::time::Duration;

use clap::ValueEnum;
use nestor_e2e::data::MIB;
use serde::{Deserialize, Serialize};

use crate::dataset::{Dataset, Object, Rng};
use crate::proxy::Faults;
use crate::report::{PhaseReport, Report};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Params {
    pub consumers: u32,
    #[serde(with = "humantime_serde")]
    pub duration: Duration,
    pub hot_set: u64,
    #[serde(with = "humantime_serde")]
    pub stagger: Duration,
    pub block: u64,
}

#[derive(Debug, Clone)]
pub enum Op {
    Read {
        key: String,
        object: Option<usize>,
        range: Range<u64>,
    },
    Write {
        key: String,
        len: u64,
    },
    Delete {
        key: String,
    },
    Missing {
        key: String,
    },
}

#[derive(Debug, Clone)]
pub struct Event {
    pub at: Duration,
    pub op: Op,
}

impl Event {
    fn now(op: Op) -> Self {
        Self {
            at: Duration::ZERO,
            op,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    RestartTarget,
}

#[derive(Debug, Clone)]
pub struct Phase {
    pub name: &'static str,
    pub faults: Faults,
    pub action: Option<Action>,
    pub repeat: bool,
    pub deadline: Option<Duration>,
    pub consumers: Vec<Vec<Event>>,
}

impl Phase {
    fn once(name: &'static str, consumers: Vec<Vec<Event>>) -> Self {
        Self {
            name,
            faults: Faults::default(),
            action: None,
            repeat: false,
            deadline: None,
            consumers,
        }
    }

    fn until(name: &'static str, deadline: Duration, consumers: Vec<Vec<Event>>) -> Self {
        Self {
            repeat: true,
            deadline: Some(deadline),
            ..Self::once(name, consumers)
        }
    }

    fn faults(mut self, faults: Faults) -> Self {
        self.faults = faults;
        self
    }

    fn action(mut self, action: Action) -> Self {
        self.action = Some(action);
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Scenario {
    /// Footer, index and first block of one unseen object.
    ColdOpen,
    /// One consumer through one object, block by block.
    Sequential,
    /// All consumers open the same cold object at once.
    Fanout,
    /// A second consumer follows the first after `stagger`.
    FanoutStaggered,
    /// Each consumer reads one stream's slices across the stream set objects.
    StreamRead,
    /// Whole reads of post-compaction objects, needs the `stream` profile.
    SmallObjects,
    /// Steady hot-set readers, then a full-object scan burst, then the hot set again.
    ScanPollution,
    /// Write, read, delete, read.
    Delete,
    /// Random hot-set reads for `duration` after one warming pass.
    WorkingSet,
    /// Cold reads with 40 ms origin latency and a 1% two second tail.
    Tail,
    /// Cold reads with 5% origin failures.
    Errors,
    /// Every consumer opens a distinct cold object at t=0.
    Herd,
    /// Warm half the hot set, restart the target, read it again.
    Restart,
    /// Many sequential readers for `duration`, for memory.
    Concurrency,
    /// Mixed hot reads, fan-out and scans for `duration`.
    Soak,
}

impl Scenario {
    pub fn phases(self, ds: &Dataset, p: &Params) -> Vec<Phase> {
        let n = ds.objects.len();
        let hot = hot_objects(ds, p.hot_set).max(1);
        let k = p.consumers as usize;
        let readers = |objects: Range<usize>| readers(ds, objects, k);
        match self {
            Self::ColdOpen => vec![Phase::once("open", vec![open(ds, 0)])],
            Self::Sequential => vec![Phase::once("sequential", vec![sequential(ds, 0)])],
            Self::Fanout => vec![Phase::once(
                "fanout",
                (0..k).map(|_| sequential(ds, 0)).collect(),
            )],
            Self::FanoutStaggered => vec![
                Phase::once("first", vec![sequential(ds, 1 % n)]),
                Phase::once("second", vec![delayed(sequential(ds, 1 % n), p.stagger)]),
            ],
            Self::StreamRead => vec![Phase::once(
                "stream",
                busiest_streams(ds, k)
                    .into_iter()
                    .map(|stream| stream_read(ds, stream))
                    .collect(),
            )],
            Self::SmallObjects => vec![Phase::once("small", readers(0..n.min(hot)))],
            Self::ScanPollution => vec![
                Phase::once("warm", readers(0..hot)),
                Phase::once("steady", readers(0..hot)),
                Phase::once(
                    "scan",
                    partition(hot..n, 4)
                        .into_iter()
                        .map(|objects| scan(ds, objects))
                        .collect(),
                ),
                Phase::once("after", readers(0..hot)),
            ],
            Self::Delete => vec![Phase::once("delete", vec![delete_cycle(ds)])],
            Self::WorkingSet => vec![
                Phase::once("warm", readers(0..hot)),
                Phase::until(
                    "measure",
                    p.duration,
                    (0..k)
                        .map(|c| random_objects(ds, 0..hot, 64, c as u64))
                        .collect(),
                ),
            ],
            Self::Tail => vec![Phase::once("tail", readers(0..n)).faults(Faults {
                latency: Duration::from_millis(40),
                slow_rate: 0.01,
                slow: Duration::from_secs(2),
                fail_rate: 0.0,
            })],
            Self::Errors => vec![Phase::once("errors", readers(0..n)).faults(Faults {
                fail_rate: 0.05,
                ..Faults::default()
            })],
            Self::Herd => vec![Phase::once(
                "herd",
                (0..k.min(n)).map(|i| sequential(ds, i)).collect(),
            )],
            Self::Restart => {
                let half = 0..(hot / 2).max(1);
                vec![
                    Phase::once("warm", readers(half.clone())),
                    Phase::once("recovered", readers(half)).action(Action::RestartTarget),
                ]
            }
            Self::Concurrency => vec![Phase::until(
                "concurrency",
                p.duration,
                (0..k)
                    .map(|c| catchup(ds, (c % n..n).chain(0..c % n)))
                    .collect(),
            )],
            Self::Soak => vec![
                Phase::once("warm", readers(0..hot)),
                Phase::until(
                    "soak",
                    p.duration,
                    (0..k)
                        .map(|c| match c % 4 {
                            0 => scan(ds, (hot..n).cycle().skip(c).take(8)),
                            1 => sequential(ds, hot % n),
                            _ => random_objects(ds, 0..hot, 32, c as u64),
                        })
                        .collect(),
                ),
            ],
        }
    }

    pub fn check(self, ds: &Dataset, p: &Params, report: &Report) -> Vec<String> {
        let mut failures = Vec::new();
        let mut expect = |cond: bool, message: String| {
            if !cond {
                failures.push(message);
            }
        };
        for phase in &report.phases {
            expect(
                phase.corrupt == 0,
                format!("{}: {} corrupt reads", phase.name, phase.corrupt),
            );
            expect(
                phase.errors == 0 || phase.faults.fail_rate > 0.0,
                format!("{}: {} read errors", phase.name, phase.errors),
            );
        }
        self.check_counters(ds, p, report, &mut expect);
        failures
    }

    fn check_counters(
        self,
        ds: &Dataset,
        p: &Params,
        report: &Report,
        expect: &mut impl FnMut(bool, String),
    ) {
        let Some(first) = report.phases.first() else {
            return;
        };
        if first.nestor.is_none() {
            return;
        }
        let requests = |phase: &PhaseReport| phase.origin_requests();
        match self {
            Self::ColdOpen => expect(
                requests(first) <= 2,
                format!("cold open took {} origin requests", requests(first)),
            ),
            Self::Sequential | Self::Fanout => {
                let size = ds.objects[0].size;
                let fetched = first.origin_bytes();
                expect(
                    fetched >= size && fetched <= size + size / 20,
                    format!("fetched {fetched} bytes for a {size} byte object"),
                );
                expect(
                    requests(first) <= blocks(size, p.block),
                    format!(
                        "{} origin requests for {} blocks",
                        requests(first),
                        blocks(size, p.block)
                    ),
                );
            }
            Self::FanoutStaggered => {
                if let Some(second) = report.phases.get(1) {
                    expect(
                        requests(second) == 0,
                        format!(
                            "staggered reader caused {} origin requests",
                            requests(second)
                        ),
                    );
                }
            }
            Self::SmallObjects => {
                let limit: u64 = ds
                    .objects
                    .iter()
                    .take(hot_objects(ds, p.hot_set).max(1))
                    .map(|o| blocks(o.size, p.block))
                    .sum();
                expect(
                    requests(first) <= limit,
                    format!("{} origin requests for {limit} blocks", requests(first)),
                );
            }
            Self::Errors => {
                let injected = first.origin.map_or(0, |o| o.injected);
                expect(injected > 0, "no failures were injected".into());
                expect(
                    first.errors * 20 <= injected,
                    format!(
                        "{} reads failed out of {injected} injected failures, retries absorbed too few",
                        first.errors
                    ),
                );
            }
            Self::Restart => {
                if let Some(recovered) = report.phases.get(1) {
                    let warm = requests(first);
                    expect(
                        requests(recovered) * 5 <= warm,
                        format!(
                            "{} origin requests after restart, {warm} while warming",
                            requests(recovered)
                        ),
                    );
                }
            }
            Self::Delete => {
                expect(
                    first.missing == 1,
                    format!("{} missing reads, expected 1", first.missing),
                );
            }
            _ => {}
        }
    }
}

fn blocks(size: u64, block: u64) -> u64 {
    size.div_ceil(block)
}

fn hot_objects(ds: &Dataset, hot_set: u64) -> usize {
    let mut total = 0;
    ds.objects
        .iter()
        .take_while(|o| {
            total += o.size;
            total <= hot_set
        })
        .count()
}

fn readers(ds: &Dataset, objects: Range<usize>, k: usize) -> Vec<Vec<Event>> {
    partition(objects, k)
        .into_iter()
        .map(|objects| catchup(ds, objects))
        .collect()
}

fn delete_cycle(ds: &Dataset) -> Vec<Event> {
    let key = format!("bench/scratch/{:016x}", ds.params.seed);
    let len = 3 * MIB as u64;
    vec![
        Event::now(Op::Write {
            key: key.clone(),
            len,
        }),
        Event::now(Op::Read {
            key: key.clone(),
            object: None,
            range: 0..len,
        }),
        Event::now(Op::Delete { key: key.clone() }),
        Event::now(Op::Missing { key }),
    ]
}

fn read(ds: &Dataset, object: usize, range: Range<u64>) -> Event {
    Event::now(Op::Read {
        key: ds.objects[object].key.clone(),
        object: Some(object),
        range,
    })
}

fn open(ds: &Dataset, object: usize) -> Vec<Event> {
    let o = &ds.objects[object];
    let mut events: Vec<Event> = o
        .open_ranges()
        .into_iter()
        .map(|r| read(ds, object, r))
        .collect();
    if let Some(first) = o.blocks.first() {
        events.push(read(ds, object, first.range.clone()));
    }
    events
}

fn sequential(ds: &Dataset, object: usize) -> Vec<Event> {
    let o = &ds.objects[object];
    o.open_ranges()
        .into_iter()
        .chain(o.blocks.iter().map(|b| b.range.clone()))
        .map(|r| read(ds, object, r))
        .collect()
}

fn catchup(ds: &Dataset, objects: impl IntoIterator<Item = usize>) -> Vec<Event> {
    objects
        .into_iter()
        .flat_map(|o| sequential(ds, o))
        .collect()
}

fn scan(ds: &Dataset, objects: impl IntoIterator<Item = usize>) -> Vec<Event> {
    objects
        .into_iter()
        .map(|o| read(ds, o, 0..ds.objects[o].size))
        .collect()
}

fn stream_read(ds: &Dataset, stream: u32) -> Vec<Event> {
    ds.stream_ranges(stream)
        .into_iter()
        .flat_map(|(object, range)| {
            let o: &Object = &ds.objects[object];
            o.open_ranges()
                .into_iter()
                .chain(std::iter::once(range))
                .map(move |r| read(ds, object, r))
        })
        .collect()
}

fn random_objects(ds: &Dataset, from: Range<usize>, count: usize, seed: u64) -> Vec<Event> {
    let mut rng = Rng::new(ds.params.seed ^ seed);
    let span = (from.end - from.start) as u64;
    (0..count)
        .flat_map(|_| sequential(ds, from.start + rng.below(span) as usize))
        .collect()
}

fn delayed(mut events: Vec<Event>, by: Duration) -> Vec<Event> {
    if let Some(first) = events.first_mut() {
        first.at = by;
    }
    events
}

fn partition(objects: Range<usize>, parts: usize) -> Vec<Vec<usize>> {
    let parts = parts.max(1);
    let mut out: Vec<Vec<usize>> = vec![Vec::new(); parts];
    for (i, object) in objects.enumerate() {
        out[i % parts].push(object);
    }
    out.retain(|p| !p.is_empty());
    out
}

fn busiest_streams(ds: &Dataset, count: usize) -> Vec<u32> {
    let mut seen = std::collections::HashMap::<u32, usize>::new();
    for o in &ds.objects {
        for b in &o.blocks {
            *seen.entry(b.stream).or_default() += 1;
        }
    }
    let mut streams: Vec<(u32, usize)> = seen.into_iter().collect();
    streams.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
    streams.into_iter().take(count).map(|(s, _)| s).collect()
}
