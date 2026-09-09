//! `object_store` client settings for an origin: connection only, the engine owns retries and timeouts.

use std::time::Duration;

use object_store::{ClientOptions, RetryConfig};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Transport {
    pub connect_timeout: Duration,
    pub pool_idle_timeout: Duration,
    pub pool_max_idle_per_host: usize,
    pub allow_http: bool,
}

impl Default for Transport {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(5),
            pool_idle_timeout: Duration::from_secs(90),
            pool_max_idle_per_host: 64,
            allow_http: true,
        }
    }
}

impl Transport {
    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    pub fn client_options(&self) -> ClientOptions {
        ClientOptions::new()
            .with_connect_timeout(self.connect_timeout)
            .with_timeout_disabled()
            .with_pool_idle_timeout(self.pool_idle_timeout)
            .with_pool_max_idle_per_host(self.pool_max_idle_per_host)
            .with_allow_http(self.allow_http)
    }

    pub fn retry_config(&self) -> RetryConfig {
        RetryConfig {
            max_retries: 0,
            ..RetryConfig::default()
        }
    }
}
