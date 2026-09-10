//! `object_store` client settings for an origin: connection only, the engine owns retries and timeouts.

use std::time::Duration;

use object_store::client::{HttpClient, HttpConnector, ReqwestConnector};
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

    /// Builds the HTTP client once. Construction loads the TLS root store, tens of milliseconds
    /// on Linux, so stores that share a transport should share the client too.
    pub fn shared_client(&self) -> Result<SharedClient, object_store::Error> {
        ReqwestConnector::default()
            .connect(&self.client_options())
            .map(SharedClient)
    }
}

/// An [`HttpConnector`] that hands every store the same already built client.
#[derive(Debug, Clone)]
pub struct SharedClient(HttpClient);

impl HttpConnector for SharedClient {
    fn connect(&self, _: &ClientOptions) -> Result<HttpClient, object_store::Error> {
        Ok(self.0.clone())
    }
}
