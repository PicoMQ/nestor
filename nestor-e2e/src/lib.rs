//! Shared pieces for the scenarios: endpoint settings, S3 clients, payloads, metrics scraping,
//! readiness polling and docker compose control.

pub mod compose;
pub mod data;
pub mod metrics;
pub mod s3;
pub mod wait;

use std::sync::Once;

pub const ACCESS_KEY: &str = "nestor";
pub const SECRET_KEY: &str = "nestornestor";
pub const BUCKET: &str = "nestor";
pub const REGION: &str = "us-east-1";

pub fn env(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_owned())
}

pub fn init() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        tracing_subscriber::fmt()
            .with_env_filter(
                tracing_subscriber::EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| "info,nestor_e2e=info".into()),
            )
            .with_test_writer()
            .init();
    });
}

#[macro_export]
macro_rules! step {
    ($($arg:tt)*) => {
        tracing::info!(target: "nestor_e2e", $($arg)*)
    };
}
