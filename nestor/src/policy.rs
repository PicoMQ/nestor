//! How a miss goes to the origin: attempts, backoff, timeouts and hedging.

use std::time::{Duration, Instant};

use crate::error::OriginError;
use crate::latency::Latency;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum HedgeAfter {
    Factor(f64),
    Quantile(f64),
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct HedgeConfig {
    pub after: HedgeAfter,
    pub min: Duration,
    pub max: Duration,
}

#[derive(Debug, thiserror::Error, PartialEq)]
pub enum HedgeConfigError {
    #[error("hedge.factor must be greater than 0, got {0}")]
    Factor(f64),
    #[error("hedge.quantile must be in (0, 1], got {0}")]
    Quantile(f64),
    #[error("hedge.min {min:?} exceeds hedge.max {max:?}")]
    Bounds { min: Duration, max: Duration },
    #[error("hedge.factor and hedge.quantile cannot both be set")]
    Exclusive,
}

impl Default for HedgeConfig {
    fn default() -> Self {
        Self {
            after: HedgeAfter::Factor(3.0),
            min: Duration::from_millis(50),
            max: Duration::from_secs(2),
        }
    }
}

impl HedgeConfig {
    pub fn factor(factor: f64, min: Duration, max: Duration) -> Result<Self, HedgeConfigError> {
        if factor.is_nan() || factor <= 0.0 {
            return Err(HedgeConfigError::Factor(factor));
        }
        Self::new(HedgeAfter::Factor(factor), min, max)
    }

    pub fn quantile(quantile: f64, min: Duration, max: Duration) -> Result<Self, HedgeConfigError> {
        if quantile.is_nan() || quantile <= 0.0 || quantile > 1.0 {
            return Err(HedgeConfigError::Quantile(quantile));
        }
        Self::new(HedgeAfter::Quantile(quantile), min, max)
    }

    fn new(after: HedgeAfter, min: Duration, max: Duration) -> Result<Self, HedgeConfigError> {
        if min > max {
            return Err(HedgeConfigError::Bounds { min, max });
        }
        Ok(Self { after, min, max })
    }

    pub fn delay(&self, latency: &Latency) -> Duration {
        let estimate = match self.after {
            HedgeAfter::Factor(factor) => latency.mean().map(|mean| mean.mul_f64(factor)),
            HedgeAfter::Quantile(quantile) => latency.quantile(quantile),
        };
        estimate.map_or(self.max, |delay| delay.clamp(self.min, self.max))
    }
}

#[cfg(feature = "serde")]
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct RawHedge {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    factor: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    quantile: Option<f64>,
    #[serde(default, with = "humantime_serde::option")]
    min: Option<Duration>,
    #[serde(default, with = "humantime_serde::option")]
    max: Option<Duration>,
}

#[cfg(feature = "serde")]
impl TryFrom<RawHedge> for HedgeConfig {
    type Error = HedgeConfigError;

    fn try_from(raw: RawHedge) -> Result<Self, HedgeConfigError> {
        let defaults = Self::default();
        let min = raw.min.unwrap_or(defaults.min);
        let max = raw.max.unwrap_or(defaults.max);
        match (raw.factor, raw.quantile) {
            (Some(_), Some(_)) => Err(HedgeConfigError::Exclusive),
            (None, Some(quantile)) => Self::quantile(quantile, min, max),
            (Some(factor), None) => Self::factor(factor, min, max),
            (None, None) => Self::new(defaults.after, min, max),
        }
    }
}

#[cfg(feature = "serde")]
impl From<HedgeConfig> for RawHedge {
    fn from(config: HedgeConfig) -> Self {
        let (factor, quantile) = match config.after {
            HedgeAfter::Factor(factor) => (Some(factor), None),
            HedgeAfter::Quantile(quantile) => (None, Some(quantile)),
        };
        Self {
            factor,
            quantile,
            min: Some(config.min),
            max: Some(config.max),
        }
    }
}

#[cfg(feature = "serde")]
impl serde::Serialize for HedgeConfig {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        RawHedge::from(*self).serialize(serializer)
    }
}

#[cfg(feature = "serde")]
impl<'de> serde::Deserialize<'de> for HedgeConfig {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        RawHedge::deserialize(deserializer)?
            .try_into()
            .map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
#[cfg_attr(
    feature = "serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(deny_unknown_fields, default)
)]
pub struct FetchPolicy {
    pub attempts: u32,
    #[cfg_attr(feature = "serde", serde(with = "humantime_serde"))]
    pub backoff: Duration,
    #[cfg_attr(feature = "serde", serde(with = "humantime_serde"))]
    pub backoff_max: Duration,
    #[cfg_attr(feature = "serde", serde(with = "humantime_serde"))]
    pub first_byte: Duration,
    #[cfg_attr(feature = "serde", serde(with = "humantime_serde"))]
    pub attempt: Duration,
    #[cfg_attr(feature = "serde", serde(with = "humantime_serde"))]
    pub deadline: Duration,
    #[cfg_attr(feature = "serde", serde(deserialize_with = "hedge_toggle"))]
    pub hedge: Option<HedgeConfig>,
}

impl Default for FetchPolicy {
    fn default() -> Self {
        Self {
            attempts: 3,
            backoff: Duration::from_millis(50),
            backoff_max: Duration::from_secs(2),
            first_byte: Duration::from_secs(5),
            attempt: Duration::from_secs(30),
            deadline: Duration::from_secs(60),
            hedge: Some(HedgeConfig::default()),
        }
    }
}

impl FetchPolicy {
    pub fn attempts(mut self, attempts: u32) -> Self {
        self.attempts = attempts.max(1);
        self
    }

    pub fn backoff(mut self, base: Duration, max: Duration) -> Self {
        self.backoff = base;
        self.backoff_max = max;
        self
    }

    pub fn first_byte(mut self, timeout: Duration) -> Self {
        self.first_byte = timeout;
        self
    }

    pub fn attempt(mut self, timeout: Duration) -> Self {
        self.attempt = timeout;
        self
    }

    pub fn deadline(mut self, timeout: Duration) -> Self {
        self.deadline = timeout;
        self
    }

    pub fn hedge(mut self, hedge: Option<HedgeConfig>) -> Self {
        self.hedge = hedge;
        self
    }

    pub fn with(self, overrides: &FetchOverrides) -> Self {
        Self {
            attempts: overrides.attempts.unwrap_or(self.attempts).max(1),
            backoff: overrides.backoff.unwrap_or(self.backoff),
            backoff_max: overrides.backoff_max.unwrap_or(self.backoff_max),
            first_byte: overrides.first_byte.unwrap_or(self.first_byte),
            attempt: overrides.attempt.unwrap_or(self.attempt),
            deadline: overrides.deadline.unwrap_or(self.deadline),
            hedge: match overrides.hedge {
                None => self.hedge,
                Some(false) => None,
                Some(true) => self.hedge.or_else(|| Some(HedgeConfig::default())),
            },
        }
    }

    pub(crate) fn backoff_for(&self, attempt: u32) -> Duration {
        self.backoff
            .saturating_mul(1u32 << attempt.min(16))
            .min(self.backoff_max)
    }
}

pub(crate) struct Retry {
    policy: FetchPolicy,
    attempt: u32,
    deadline: Instant,
}

impl Retry {
    pub fn new(policy: FetchPolicy) -> Self {
        Self {
            policy,
            attempt: 0,
            deadline: Instant::now() + policy.deadline,
        }
    }

    pub fn policy(&self) -> &FetchPolicy {
        &self.policy
    }

    pub fn attempt_deadline(&self) -> Instant {
        (Instant::now() + self.policy.attempt).min(self.deadline)
    }

    pub async fn failed(&mut self, error: OriginError) -> Result<(), OriginError> {
        self.attempt += 1;
        if !error.is_retryable() || self.attempt >= self.policy.attempts {
            return Err(error);
        }
        let delay = self.policy.backoff_for(self.attempt - 1);
        if Instant::now() + delay >= self.deadline {
            return Err(OriginError::Timeout(self.policy.deadline));
        }
        tokio::time::sleep(delay).await;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct FetchOverrides {
    pub attempts: Option<u32>,
    pub backoff: Option<Duration>,
    pub backoff_max: Option<Duration>,
    pub first_byte: Option<Duration>,
    pub attempt: Option<Duration>,
    pub deadline: Option<Duration>,
    pub hedge: Option<bool>,
}

impl FetchOverrides {
    pub fn is_empty(&self) -> bool {
        *self == Self::default()
    }

    pub fn parse(text: &str) -> Result<Self, PolicyParseError> {
        let mut overrides = Self::default();
        for pair in text.split_whitespace() {
            let (key, value) = pair
                .split_once('=')
                .ok_or_else(|| PolicyParseError::Syntax(pair.to_owned()))?;
            let duration = || {
                humantime::parse_duration(value)
                    .map_err(|_| PolicyParseError::Value(key.to_owned(), value.to_owned()))
            };
            match key {
                "attempts" => {
                    overrides.attempts =
                        Some(value.parse().map_err(|_| {
                            PolicyParseError::Value(key.to_owned(), value.to_owned())
                        })?);
                }
                "backoff" => overrides.backoff = Some(duration()?),
                "backoff_max" => overrides.backoff_max = Some(duration()?),
                "first_byte" => overrides.first_byte = Some(duration()?),
                "attempt" => overrides.attempt = Some(duration()?),
                "deadline" => overrides.deadline = Some(duration()?),
                "hedge" => {
                    overrides.hedge = Some(match value {
                        "on" | "true" => true,
                        "off" | "false" => false,
                        _ => return Err(PolicyParseError::Value(key.to_owned(), value.to_owned())),
                    });
                }
                _ => return Err(PolicyParseError::Key(key.to_owned())),
            }
        }
        Ok(overrides)
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum PolicyParseError {
    #[error("expected key=value, got {0:?}")]
    Syntax(String),
    #[error("unknown fetch setting {0:?}")]
    Key(String),
    #[error("bad value {1:?} for {0}")]
    Value(String, String),
}

#[cfg(feature = "serde")]
fn hedge_toggle<'de, D>(deserializer: D) -> Result<Option<HedgeConfig>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::Deserialize;

    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Toggle {
        Enabled(bool),
        Config(HedgeConfig),
    }
    Ok(match Toggle::deserialize(deserializer)? {
        Toggle::Enabled(false) => None,
        Toggle::Enabled(true) => Some(HedgeConfig::default()),
        Toggle::Config(config) => Some(config),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn overrides_apply_over_the_namespace_policy() {
        let base = FetchPolicy::default().hedge(None);
        let overrides = FetchOverrides::parse("attempts=5 first_byte=250ms hedge=on").unwrap();
        let policy = base.with(&overrides);
        assert_eq!(policy.attempts, 5);
        assert_eq!(policy.first_byte, Duration::from_millis(250));
        assert_eq!(policy.deadline, base.deadline);
        assert_eq!(policy.hedge, Some(HedgeConfig::default()));

        let off = FetchOverrides::parse("hedge=off").unwrap();
        assert_eq!(FetchPolicy::default().with(&off).hedge, None);
        assert!(FetchOverrides::parse("").unwrap().is_empty());
    }

    #[test]
    fn parse_rejects_bad_input() {
        assert_eq!(
            FetchOverrides::parse("attempts"),
            Err(PolicyParseError::Syntax("attempts".into()))
        );
        assert_eq!(
            FetchOverrides::parse("nope=1"),
            Err(PolicyParseError::Key("nope".into()))
        );
        assert_eq!(
            FetchOverrides::parse("deadline=soon"),
            Err(PolicyParseError::Value("deadline".into(), "soon".into()))
        );
        assert!(FetchOverrides::parse("attempts=0").is_ok());
        assert_eq!(
            FetchPolicy::default()
                .with(&FetchOverrides::parse("attempts=0").unwrap())
                .attempts,
            1
        );
    }

    #[test]
    fn delay_is_clamped_and_falls_back_to_max() {
        let cfg = HedgeConfig::default();
        let latency = Latency::default();
        assert_eq!(cfg.delay(&latency), cfg.max);
        latency.observe(Duration::from_millis(1));
        assert_eq!(cfg.delay(&latency), cfg.min);
        let slow = Latency::default();
        slow.observe(Duration::from_secs(10));
        assert_eq!(cfg.delay(&slow), cfg.max);
        let mid = Latency::default();
        mid.observe(Duration::from_millis(100));
        assert_eq!(cfg.delay(&mid), Duration::from_millis(300));
    }

    #[test]
    fn quantile_delay_follows_the_tail() {
        let cfg =
            HedgeConfig::quantile(0.99, Duration::from_millis(1), Duration::from_secs(2)).unwrap();
        let latency = Latency::default();
        assert_eq!(cfg.delay(&latency), cfg.max);
        for _ in 0..98 {
            latency.observe(Duration::from_millis(10));
        }
        latency.observe(Duration::from_millis(500));
        latency.observe(Duration::from_millis(500));
        let delay = cfg.delay(&latency);
        assert!(
            delay >= Duration::from_millis(500) && delay <= Duration::from_millis(570),
            "{delay:?}"
        );
    }

    #[test]
    fn constructors_validate() {
        let (min, max) = (Duration::from_millis(50), Duration::from_secs(2));
        assert_eq!(
            HedgeConfig::factor(0.0, min, max),
            Err(HedgeConfigError::Factor(0.0))
        );
        assert_eq!(
            HedgeConfig::quantile(0.0, min, max),
            Err(HedgeConfigError::Quantile(0.0))
        );
        assert_eq!(
            HedgeConfig::quantile(1.5, min, max),
            Err(HedgeConfigError::Quantile(1.5))
        );
        assert_eq!(
            HedgeConfig::factor(2.0, max, min),
            Err(HedgeConfigError::Bounds { min: max, max: min })
        );
        assert!(HedgeConfig::quantile(1.0, min, max).is_ok());
    }

    #[test]
    fn backoff_grows_and_caps() {
        let policy =
            FetchPolicy::default().backoff(Duration::from_millis(10), Duration::from_millis(50));
        assert_eq!(policy.backoff_for(0), Duration::from_millis(10));
        assert_eq!(policy.backoff_for(1), Duration::from_millis(20));
        assert_eq!(policy.backoff_for(5), Duration::from_millis(50));
    }

    #[cfg(feature = "serde")]
    #[test]
    fn hedge_toggle_forms() {
        let off: FetchPolicy = toml::from_str("hedge = false").unwrap();
        assert_eq!(off.hedge, None);
        let on: FetchPolicy = toml::from_str("hedge = true").unwrap();
        assert_eq!(on.hedge, Some(HedgeConfig::default()));
        let custom: FetchPolicy = toml::from_str("hedge = { factor = 2.0 }").unwrap();
        assert_eq!(custom.hedge.unwrap().after, HedgeAfter::Factor(2.0));
        let quantile: FetchPolicy =
            toml::from_str(r#"hedge = { quantile = 0.99, min = "10ms" }"#).unwrap();
        assert_eq!(
            quantile.hedge,
            Some(
                HedgeConfig::quantile(0.99, Duration::from_millis(10), Duration::from_secs(2))
                    .unwrap()
            )
        );
        let omitted: FetchPolicy = toml::from_str("attempts = 4").unwrap();
        assert_eq!(omitted.hedge, Some(HedgeConfig::default()));
        assert_eq!(omitted.attempts, 4);
        for bad in [
            "hedge = { factor = 2.0, quantile = 0.99 }",
            "hedge = { quantile = 0 }",
            "hedge = { factor = 0 }",
            r#"hedge = { min = "3s", max = "2s" }"#,
            "hedge = { window = 1 }",
        ] {
            assert!(toml::from_str::<FetchPolicy>(bad).is_err(), "{bad}");
        }
        let serialized = toml::to_string(&quantile).unwrap();
        let roundtrip: FetchPolicy = toml::from_str(&serialized).unwrap();
        assert_eq!(roundtrip, quantile);
    }
}
