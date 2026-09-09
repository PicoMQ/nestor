//! One JSON document per run. Field names are the contract between runs, `compare` diffs two.

use std::fmt::Write;
use std::path::Path;
use std::time::Duration;

use hdrhistogram::Histogram;
use serde::{Deserialize, Serialize};

use crate::dataset::DatasetParams;
use crate::proxy::{Faults, OriginCounters};
use crate::target::NestorCounters;
use crate::workload::{Params, Scenario};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Report {
    pub target: String,
    pub scenario: Scenario,
    pub dataset: DatasetParams,
    pub params: Params,
    pub config: serde_json::Value,
    pub started_unix: u64,
    pub phases: Vec<PhaseReport>,
    pub rss_max_mib: Option<f64>,
    pub failures: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PhaseReport {
    pub name: String,
    pub faults: Faults,
    pub duration_s: f64,
    pub reads: u64,
    pub bytes: u64,
    pub errors: u64,
    pub corrupt: u64,
    pub missing: u64,
    pub throughput_mib_s: f64,
    pub ttfb: Latency,
    pub ttlb: Latency,
    pub origin: OriginCounters,
    pub nestor: Option<NestorCounters>,
}

impl PhaseReport {
    pub fn hit_rate(&self) -> Option<f64> {
        let n = self.nestor?;
        let total = n.hits + n.misses + n.joined;
        (total > 0).then(|| n.hits as f64 / total as f64)
    }

    pub fn amplification(&self) -> Option<f64> {
        (self.bytes > 0).then(|| self.origin.bytes as f64 / self.bytes as f64)
    }
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct Latency {
    pub p50_ms: f64,
    pub p90_ms: f64,
    pub p99_ms: f64,
    pub p999_ms: f64,
    pub max_ms: f64,
}

impl Latency {
    pub fn from_histogram(h: &Histogram<u64>) -> Self {
        let ms = |v: u64| v as f64 / 1000.0;
        Self {
            p50_ms: ms(h.value_at_quantile(0.5)),
            p90_ms: ms(h.value_at_quantile(0.9)),
            p99_ms: ms(h.value_at_quantile(0.99)),
            p999_ms: ms(h.value_at_quantile(0.999)),
            max_ms: ms(h.max()),
        }
    }
}

pub fn histogram() -> Histogram<u64> {
    Histogram::new_with_bounds(1, Duration::from_secs(600).as_micros() as u64, 3)
        .expect("histogram bounds")
}

impl Report {
    pub fn write(&self, path: &Path) -> eyre::Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(path, serde_json::to_vec_pretty(self)?)?;
        Ok(())
    }

    pub fn read(path: &Path) -> eyre::Result<Self> {
        Ok(serde_json::from_slice(&std::fs::read(path)?)?)
    }

    pub fn summary(&self) -> String {
        let mut out = format!("{} / {:?}\n", self.target, self.scenario);
        for phase in &self.phases {
            let _ = write!(
                out,
                "  {:<12} {:>7} reads {:>9.1} MiB/s  ttfb p50 {:>8.3} p99 {:>8.3}  ttlb p99 {:>8.3} ms  origin {:>6} req {:>9.1} MiB",
                phase.name,
                phase.reads,
                phase.throughput_mib_s,
                phase.ttfb.p50_ms,
                phase.ttfb.p99_ms,
                phase.ttlb.p99_ms,
                phase.origin.requests,
                phase.origin.bytes as f64 / (1024.0 * 1024.0),
            );
            if let Some(rate) = phase.hit_rate() {
                let _ = write!(out, "  hit {:.1}%", rate * 100.0);
            }
            if let Some(amp) = phase.amplification() {
                let _ = write!(out, "  amp {amp:.2}x");
            }
            if phase.origin.max_inflight > 1 {
                let _ = write!(out, "  inflight {}", phase.origin.max_inflight);
            }
            if phase.origin.injected > 0 {
                let _ = write!(out, "  injected {}", phase.origin.injected);
            }
            if phase.errors > 0 {
                let _ = write!(out, "  errors {}", phase.errors);
            }
            if phase.corrupt > 0 {
                let _ = write!(out, "  corrupt {}", phase.corrupt);
            }
            out.push('\n');
        }
        if let Some(rss) = self.rss_max_mib {
            let _ = writeln!(out, "  rss max {rss:.0} MiB");
        }
        for failure in &self.failures {
            let _ = writeln!(out, "  FAIL {failure}");
        }
        out
    }
}

pub fn compare(a: &Report, b: &Report) -> String {
    let mut out = format!(
        "{:<28} {:>14} {:>14} {:>8}\n",
        "", a.target, b.target, "b/a"
    );
    let mut row = |name: String, x: f64, y: f64| {
        let ratio = if x > 0.0 { y / x } else { f64::NAN };
        let _ = writeln!(out, "{name:<28} {x:>14.2} {y:>14.2} {ratio:>8.2}");
    };
    for pa in &a.phases {
        let Some(pb) = b.phases.iter().find(|p| p.name == pa.name) else {
            continue;
        };
        let n = &pa.name;
        row(
            format!("{n} throughput MiB/s"),
            pa.throughput_mib_s,
            pb.throughput_mib_s,
        );
        row(format!("{n} ttfb p50 ms"), pa.ttfb.p50_ms, pb.ttfb.p50_ms);
        row(format!("{n} ttfb p99 ms"), pa.ttfb.p99_ms, pb.ttfb.p99_ms);
        row(format!("{n} ttlb p99 ms"), pa.ttlb.p99_ms, pb.ttlb.p99_ms);
        row(
            format!("{n} origin requests"),
            pa.origin.requests as f64,
            pb.origin.requests as f64,
        );
        row(
            format!("{n} origin MiB"),
            pa.origin.bytes as f64 / (1024.0 * 1024.0),
            pb.origin.bytes as f64 / (1024.0 * 1024.0),
        );
        if let (Some(x), Some(y)) = (pa.hit_rate(), pb.hit_rate()) {
            row(format!("{n} hit rate"), x, y);
        }
    }
    if let (Some(x), Some(y)) = (a.rss_max_mib, b.rss_max_mib) {
        row("rss max MiB".into(), x, y);
    }
    out
}
