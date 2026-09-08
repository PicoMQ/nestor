//! One cluster member: its S3 client per bucket, in flight count for bounded load, a down window
//! after connection failures and the latency estimate hedging is timed against.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use nestor::{Latency, Origin};
use nestor_store::ObjectStoreOrigin;
use object_store::StaticCredentialProvider;
use object_store::aws::{AmazonS3Builder, AwsCredential, AwsCredentialProvider};

use crate::config::ClusterConfig;
use crate::error::ClusterError;
use crate::router;

const REGION: &str = "cluster";

pub(crate) struct Node {
    addr: SocketAddr,
    seed: u64,
    endpoint: String,
    credentials: Option<AwsCredentialProvider>,
    load_limit: usize,
    inflight: AtomicUsize,
    epoch: Instant,
    down_until_nanos: AtomicU64,
    latency: Latency,
    buckets: RwLock<HashMap<Arc<str>, Arc<dyn Origin>>>,
}

impl Node {
    pub fn new(addr: SocketAddr, config: &ClusterConfig) -> Self {
        let scheme = if config.tls { "https" } else { "http" };
        let credentials = config.credentials.as_ref().map(|c| {
            Arc::new(StaticCredentialProvider::new(AwsCredential {
                key_id: c.access_key.clone(),
                secret_key: c.secret_key.clone(),
                token: None,
            })) as AwsCredentialProvider
        });
        Self {
            addr,
            seed: router::seed(&addr),
            endpoint: format!("{scheme}://{addr}"),
            credentials,
            load_limit: config.load_limit.max(1),
            inflight: AtomicUsize::new(0),
            epoch: Instant::now(),
            down_until_nanos: AtomicU64::new(0),
            latency: Latency::default(),
            buckets: RwLock::new(HashMap::new()),
        }
    }

    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    pub fn seed(&self) -> u64 {
        self.seed
    }

    pub fn latency(&self) -> &Latency {
        &self.latency
    }

    pub fn is_up(&self) -> bool {
        let until = self.down_until_nanos.load(Ordering::Relaxed);
        until == 0 || self.elapsed_nanos() >= until
    }

    pub fn mark_down(&self, window: Duration) {
        let until = self
            .elapsed_nanos()
            .saturating_add(window.as_nanos() as u64)
            .max(1);
        self.down_until_nanos.fetch_max(until, Ordering::Relaxed);
    }

    pub fn inflight(&self) -> usize {
        self.inflight.load(Ordering::Relaxed)
    }

    pub fn try_acquire(self: &Arc<Self>) -> Option<Load> {
        let mut current = self.inflight.load(Ordering::Relaxed);
        loop {
            if current >= self.load_limit {
                return None;
            }
            match self.inflight.compare_exchange_weak(
                current,
                current + 1,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return Some(Load(Arc::clone(self))),
                Err(actual) => current = actual,
            }
        }
    }

    pub fn acquire(self: &Arc<Self>) -> Load {
        self.inflight.fetch_add(1, Ordering::Relaxed);
        Load(Arc::clone(self))
    }

    pub fn origin(&self, bucket: &Arc<str>) -> Result<Arc<dyn Origin>, ClusterError> {
        if let Some(origin) = self
            .buckets
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(bucket)
        {
            return Ok(Arc::clone(origin));
        }
        let mut builder = AmazonS3Builder::new()
            .with_bucket_name(bucket.as_ref())
            .with_region(REGION)
            .with_endpoint(&self.endpoint)
            .with_allow_http(true);
        builder = match &self.credentials {
            Some(provider) => builder.with_credentials(Arc::clone(provider)),
            None => builder.with_skip_signature(true),
        };
        let store = builder.build().map_err(|source| ClusterError::Client {
            node: self.addr.to_string(),
            source,
        })?;
        let origin: Arc<dyn Origin> = Arc::new(ObjectStoreOrigin::new(Arc::new(store)));
        self.buckets
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .insert(Arc::clone(bucket), Arc::clone(&origin));
        Ok(origin)
    }

    fn elapsed_nanos(&self) -> u64 {
        self.epoch.elapsed().as_nanos() as u64
    }
}

impl std::fmt::Debug for Node {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Node")
            .field("addr", &self.addr)
            .field("inflight", &self.inflight())
            .field("up", &self.is_up())
            .finish_non_exhaustive()
    }
}

pub(crate) struct Load(Arc<Node>);

impl Load {
    pub fn node(&self) -> &Arc<Node> {
        &self.0
    }
}

impl Drop for Load {
    fn drop(&mut self) {
        self.0.inflight.fetch_sub(1, Ordering::Relaxed);
    }
}
