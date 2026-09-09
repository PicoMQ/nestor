//! How a miss goes to the origin: attempts, backoff, timeouts and hedging.

use std::time::{Duration, Instant};

use crate::error::OriginError;

#[derive(Debug, Clone, Copy, PartialEq)]
#[cfg_attr(
    feature = "serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(deny_unknown_fields, default)
)]
pub struct HedgeConfig {
    pub factor: f64,
    #[cfg_attr(feature = "serde", serde(with = "humantime_serde"))]
    pub min: Duration,
    #[cfg_attr(feature = "serde", serde(with = "humantime_serde"))]
    pub max: Duration,
}

impl Default for HedgeConfig {
    fn default() -> Self {
        Self {
            factor: 3.0,
            min: Duration::from_millis(50),
            max: Duration::from_secs(2),
        }
    }
}

impl HedgeConfig {
    pub fn delay(&self, observed: Option<Duration>) -> Duration {
        match observed {
            None => self.max,
            Some(ttfb) => ttfb.mul_f64(self.factor).clamp(self.min, self.max),
        }
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
    fn delay_is_clamped() {
        let cfg = HedgeConfig::default();
        assert_eq!(cfg.delay(None), cfg.max);
        assert_eq!(cfg.delay(Some(Duration::from_millis(1))), cfg.min);
        assert_eq!(cfg.delay(Some(Duration::from_secs(10))), cfg.max);
        assert_eq!(
            cfg.delay(Some(Duration::from_millis(100))),
            Duration::from_millis(300)
        );
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
        assert!((custom.hedge.unwrap().factor - 2.0).abs() < f64::EPSILON);
        let omitted: FetchPolicy = toml::from_str("attempts = 4").unwrap();
        assert_eq!(omitted.hedge, Some(HedgeConfig::default()));
        assert_eq!(omitted.attempts, 4);
    }
}
