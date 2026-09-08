//! `Origin` backed by the cluster, one per bucket. Plug it into a local `Nestor` for a RAM tier in
//! front of the shared cluster.

use std::sync::Arc;

use async_trait::async_trait;
use nestor::{GetOptions, GetResponse, ObjectMeta, Origin, OriginError};

use crate::cluster::Cluster;

#[derive(Clone)]
pub struct ClusterOrigin {
    cluster: Arc<Cluster>,
    bucket: Arc<str>,
}

impl ClusterOrigin {
    pub(crate) fn new(cluster: Arc<Cluster>, bucket: Arc<str>) -> Self {
        Self { cluster, bucket }
    }

    pub fn cluster(&self) -> &Arc<Cluster> {
        &self.cluster
    }

    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    pub async fn warm(&self, key: &str, size: u64) -> Result<(), OriginError> {
        self.cluster.warm(&self.bucket, &Arc::from(key), size).await
    }
}

#[async_trait]
impl Origin for ClusterOrigin {
    async fn get(&self, object: &str, options: GetOptions) -> Result<GetResponse, OriginError> {
        self.cluster
            .get(&self.bucket, &Arc::from(object), options)
            .await
    }

    async fn head(&self, object: &str) -> Result<ObjectMeta, OriginError> {
        self.cluster.head(&self.bucket, object).await
    }
}

impl std::fmt::Debug for ClusterOrigin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClusterOrigin")
            .field("bucket", &self.bucket)
            .field("nodes", &self.cluster.node_count())
            .finish()
    }
}
