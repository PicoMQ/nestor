//! Runs phases against a target: one task per consumer replaying its events, counters snapshotted
//! at phase boundaries, RSS sampled throughout.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use eyre::WrapErr;
use futures::StreamExt;
use hdrhistogram::Histogram;
use nestor_e2e::wait;
use tokio::process::Command;
use tokio::task::JoinSet;

use crate::dataset::{Dataset, fill};
use crate::proxy::{Faults, Proxy};
use crate::report::{Latency, PhaseReport, histogram};
use crate::target::Target;
use crate::workload::{Action, Event, Op, Phase};

#[derive(Debug, Clone)]
pub struct Stack {
    pub compose: PathBuf,
    pub service: String,
    pub health: String,
}

impl Stack {
    async fn restart(&self) -> eyre::Result<()> {
        self.compose(&["stop"]).await?;
        self.compose(&["start"]).await?;
        wait::healthy(&self.health, Duration::from_secs(120)).await;
        Ok(())
    }

    async fn recreate(&self) -> eyre::Result<()> {
        self.compose(&["rm", "--stop", "--force", "--volumes"])
            .await?;
        self.compose(&["up", "--detach", "--wait"]).await?;
        wait::healthy(&self.health, Duration::from_secs(120)).await;
        Ok(())
    }

    async fn compose(&self, args: &[&str]) -> eyre::Result<()> {
        let status = Command::new("docker")
            .args(["compose", "-f"])
            .arg(&self.compose)
            .args(args)
            .arg(&self.service)
            .status()
            .await
            .wrap_err("docker compose")?;
        eyre::ensure!(
            status.success(),
            "docker compose {} {} failed",
            args.join(" "),
            self.service
        );
        Ok(())
    }
}

pub struct Runner {
    pub target: Arc<dyn Target>,
    pub proxy: Arc<Proxy>,
    pub dataset: Arc<Dataset>,
    pub verify: bool,
    pub stack: Option<Stack>,
}

pub struct Outcome {
    pub phases: Vec<PhaseReport>,
    pub rss_max: Option<u64>,
}

struct ConsumerStats {
    ttfb: Histogram<u64>,
    ttlb: Histogram<u64>,
    reads: u64,
    bytes: u64,
    errors: u64,
    corrupt: u64,
    missing: u64,
}

impl ConsumerStats {
    fn new() -> Self {
        Self {
            ttfb: histogram(),
            ttlb: histogram(),
            reads: 0,
            bytes: 0,
            errors: 0,
            corrupt: 0,
            missing: 0,
        }
    }

    fn record(&mut self, first: Duration, last: Duration, bytes: u64) {
        let micros = |d: Duration| (d.as_micros() as u64).max(1);
        self.ttfb.saturating_record(micros(first));
        self.ttlb.saturating_record(micros(last));
        self.reads += 1;
        self.bytes += bytes;
    }

    fn merge(&mut self, other: Self) {
        self.ttfb.add(other.ttfb).expect("same bounds");
        self.ttlb.add(other.ttlb).expect("same bounds");
        self.reads += other.reads;
        self.bytes += other.bytes;
        self.errors += other.errors;
        self.corrupt += other.corrupt;
        self.missing += other.missing;
    }
}

impl Runner {
    pub async fn run(&self, phases: Vec<Phase>) -> eyre::Result<Outcome> {
        if let Some(stack) = &self.stack {
            stack.recreate().await?;
        }
        let rss_max = Arc::new(AtomicU64::new(0));
        let sampler = {
            let target = Arc::clone(&self.target);
            let rss_max = Arc::clone(&rss_max);
            tokio::spawn(async move {
                loop {
                    if let Some(rss) = target.rss_bytes().await {
                        rss_max.fetch_max(rss, Ordering::Relaxed);
                    }
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            })
        };

        let mut reports = Vec::with_capacity(phases.len());
        for phase in phases {
            reports.push(self.phase(phase).await?);
        }
        sampler.abort();
        if let Some(rss) = self.target.rss_bytes().await {
            rss_max.fetch_max(rss, Ordering::Relaxed);
        }

        let rss_max = rss_max.load(Ordering::Relaxed);
        Ok(Outcome {
            phases: reports,
            rss_max: (rss_max > 0).then_some(rss_max),
        })
    }

    async fn phase(&self, phase: Phase) -> eyre::Result<PhaseReport> {
        tracing::info!(
            phase = phase.name,
            consumers = phase.consumers.len(),
            repeat = phase.repeat,
            "phase start"
        );
        if let Some(Action::RestartTarget) = phase.action {
            if let Some(stack) = &self.stack {
                stack.restart().await?;
            } else {
                tracing::warn!("target cannot be restarted, skipping the restart");
            }
        }
        self.proxy.set_faults(phase.faults);
        self.proxy.reset();
        let nestor_before = self.target.nestor().await;

        let started = Instant::now();
        let deadline = phase.deadline.map(|d| started + d);
        let mut tasks = JoinSet::new();
        for events in phase.consumers {
            let target = Arc::clone(&self.target);
            let dataset = Arc::clone(&self.dataset);
            let verify = self.verify;
            let repeat = phase.repeat;
            tasks.spawn(
                async move { consume(target, dataset, events, repeat, deadline, verify).await },
            );
        }
        let mut stats = ConsumerStats::new();
        while let Some(result) = tasks.join_next().await {
            stats.merge(result.wrap_err("consumer task")?);
        }
        let duration = started.elapsed();

        let origin = self.proxy.snapshot();
        let nestor = match (nestor_before, self.target.nestor().await) {
            (Some(before), Some(after)) => Some(after.delta(before)),
            _ => None,
        };
        self.proxy.set_faults(Faults::default());

        let report = PhaseReport {
            name: phase.name.to_owned(),
            faults: phase.faults,
            duration_s: duration.as_secs_f64(),
            reads: stats.reads,
            bytes: stats.bytes,
            errors: stats.errors,
            corrupt: stats.corrupt,
            missing: stats.missing,
            throughput_mib_s: stats.bytes as f64 / (1024.0 * 1024.0) / duration.as_secs_f64(),
            ttfb: Latency::from_histogram(&stats.ttfb),
            ttlb: Latency::from_histogram(&stats.ttlb),
            origin,
            nestor,
        };
        tracing::info!(
            phase = phase.name,
            reads = report.reads,
            errors = report.errors,
            corrupt = report.corrupt,
            origin_requests = report.origin.requests,
            ttlb_p99_ms = report.ttlb.p99_ms,
            "phase done"
        );
        Ok(report)
    }
}

async fn consume(
    target: Arc<dyn Target>,
    dataset: Arc<Dataset>,
    events: Vec<Event>,
    repeat: bool,
    deadline: Option<Instant>,
    verify: bool,
) -> ConsumerStats {
    let mut stats = ConsumerStats::new();
    let started = Instant::now();
    loop {
        for event in &events {
            if deadline.is_some_and(|d| Instant::now() >= d) {
                return stats;
            }
            tokio::time::sleep_until((started + event.at).into()).await;
            execute(&target, &dataset, &event.op, verify, &mut stats).await;
        }
        if !repeat {
            return stats;
        }
    }
}

async fn execute(
    target: &Arc<dyn Target>,
    dataset: &Dataset,
    op: &Op,
    verify: bool,
    stats: &mut ConsumerStats,
) {
    match op {
        Op::Read { key, object, range } => {
            let started = Instant::now();
            let read = match target.read(key, range.clone()).await {
                Ok(read) => read,
                Err(error) => {
                    tracing::warn!(%error, key, ?range, "read failed");
                    stats.errors += 1;
                    return;
                }
            };
            let mut stream = read.stream;
            let mut got = 0u64;
            let mut chunks = Vec::new();
            while let Some(chunk) = stream.next().await {
                match chunk {
                    Ok(chunk) => {
                        got += chunk.len() as u64;
                        if verify {
                            chunks.push(chunk);
                        }
                    }
                    Err(error) => {
                        tracing::warn!(%error, key, ?range, "stream failed");
                        stats.errors += 1;
                        return;
                    }
                }
            }
            let expected = range.end - range.start;
            if got != expected {
                tracing::warn!(key, ?range, got, "short read");
                stats.corrupt += 1;
                return;
            }
            if verify && let Some(object) = object {
                let want = dataset.bytes(*object, range);
                if chunks.concat() != want {
                    tracing::warn!(key, ?range, "bytes differ");
                    stats.corrupt += 1;
                    return;
                }
            }
            stats.record(read.first_byte, started.elapsed(), got);
        }
        Op::Write { key, len } => {
            let data = fill(scratch_seed(key), &(0..*len));
            if let Err(error) = target.write(key, data).await {
                tracing::warn!(%error, key, "write failed");
                stats.errors += 1;
            }
        }
        Op::Delete { key } => {
            if let Err(error) = target.delete(key).await {
                tracing::warn!(%error, key, "delete failed");
                stats.errors += 1;
            }
        }
        Op::Missing { key } => match target.read(key, 0..1).await {
            Ok(mut read) => match read.stream.next().await {
                Some(Err(_)) | None => stats.missing += 1,
                Some(Ok(_)) => {
                    tracing::warn!(key, "deleted key still served");
                    stats.corrupt += 1;
                }
            },
            Err(_) => stats.missing += 1,
        },
    }
}

fn scratch_seed(key: &str) -> u64 {
    key.bytes().fold(0xcbf2_9ce4_8422_2325u64, |h, b| {
        (h ^ u64::from(b)).wrapping_mul(0x0100_0000_01b3)
    })
}
