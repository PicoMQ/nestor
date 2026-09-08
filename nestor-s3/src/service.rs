//! `S3Service` wires Nestor, auth, addressing and the forwarder into an axum router. Buckets
//! register as namespaces on first use.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use axum::Router;
use axum::routing::any;
use nestor::{Namespace, NamespaceConfig, NamespaceId, Nestor};

use crate::addressing::Addressing;
use crate::auth::Auth;
use crate::error::S3Error;
use crate::forward::Forwarder;
use crate::origin::{OriginConfig, Origins};
use crate::routes;

#[derive(Clone)]
pub struct S3Config {
    pub origin: OriginConfig,
    pub origins: Option<Arc<dyn Origins>>,
    pub auth: Auth,
    pub addressing: Addressing,
    pub buckets: NamespaceConfig,
    pub populate_max: Option<usize>,
}

impl std::fmt::Debug for S3Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Config")
            .field("origin", &self.origin)
            .field("origins", &self.origins.is_some())
            .field("auth", &self.auth)
            .field("addressing", &self.addressing)
            .field("buckets", &self.buckets)
            .field("populate_max", &self.populate_max)
            .finish()
    }
}

pub struct S3Service {
    pub(crate) nestor: Nestor,
    pub(crate) forwarder: Forwarder,
    pub(crate) origins: Arc<dyn Origins>,
    pub(crate) auth: Auth,
    pub(crate) addressing: Addressing,
    pub(crate) populate_max: Option<usize>,
    buckets: NamespaceConfig,
    namespaces: RwLock<HashMap<String, NamespaceId>>,
}

impl S3Service {
    pub fn new(nestor: Nestor, config: S3Config) -> Arc<Self> {
        let origins = config
            .origins
            .unwrap_or_else(|| Arc::new(config.origin.clone()));
        Arc::new(Self {
            nestor,
            forwarder: Forwarder::new(config.origin),
            origins,
            auth: config.auth,
            addressing: config.addressing,
            populate_max: config.populate_max,
            buckets: config.buckets,
            namespaces: RwLock::new(HashMap::new()),
        })
    }

    pub fn router(self: &Arc<Self>) -> Router {
        Router::new()
            .route("/-/health", any(routes::health))
            .fallback(any(routes::handle))
            .with_state(Arc::clone(self))
    }

    pub fn nestor(&self) -> &Nestor {
        &self.nestor
    }

    pub(crate) fn namespace(&self, bucket: &str) -> Result<NamespaceId, S3Error> {
        if let Some(id) = self
            .namespaces
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(bucket)
        {
            return Ok(*id);
        }
        let origin = self.origins.origin(bucket)?;
        let id = self
            .nestor
            .register(Namespace::new(bucket, origin).config(self.buckets));
        self.namespaces
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .insert(bucket.to_owned(), id);
        Ok(id)
    }
}

impl std::fmt::Debug for S3Service {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Service")
            .field("forwarder", &self.forwarder)
            .field("addressing", &self.addressing)
            .field("buckets", &self.buckets)
            .finish_non_exhaustive()
    }
}
