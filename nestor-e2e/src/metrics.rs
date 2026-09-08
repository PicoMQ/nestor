//! Scrapes a nestor `/metrics` endpoint and sums a counter across its label sets.

#![allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]

use std::collections::HashMap;

pub struct Metrics {
    samples: HashMap<String, u64>,
}

impl Metrics {
    pub async fn scrape(base: &str) -> Self {
        let text = reqwest::get(format!("{base}/metrics"))
            .await
            .expect("metrics request")
            .error_for_status()
            .expect("metrics status")
            .text()
            .await
            .expect("metrics body");
        Self::parse(&text)
    }

    pub fn parse(text: &str) -> Self {
        let samples = text
            .lines()
            .filter(|line| !line.starts_with('#') && !line.trim().is_empty())
            .filter_map(|line| {
                let (name, value) = line.rsplit_once(' ')?;
                let value = value.trim().parse::<f64>().ok()?;
                (value >= 0.0).then(|| (name.to_owned(), value.round() as u64))
            })
            .collect();
        Self { samples }
    }

    pub fn counter(&self, name: &str) -> u64 {
        self.samples
            .iter()
            .filter(|(key, _)| key.as_str() == name || key.starts_with(&format!("{name}{{")))
            .map(|(_, value)| value)
            .sum()
    }
}

#[cfg(test)]
mod tests {
    use super::Metrics;

    #[test]
    fn sums_across_labels() {
        let text = "# TYPE nestor_blocks_hit_total counter\n\
                    nestor_blocks_hit_total{namespace=\"a\"} 3\n\
                    nestor_blocks_hit_total{namespace=\"b\"} 4\n\
                    nestor_blocks_miss_total{namespace=\"a\"} 1\n";
        let metrics = Metrics::parse(text);
        assert_eq!(metrics.counter("nestor_blocks_hit_total"), 7);
        assert_eq!(metrics.counter("nestor_blocks_miss_total"), 1);
        assert_eq!(metrics.counter("nestor_blocks_stale_total"), 0);
    }
}
