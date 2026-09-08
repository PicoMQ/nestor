//! Serves bucket reads from a remote cluster instead of the origin, making this binary the local
//! tier in front of shared nodes. Writes still go to the origin, optionally warming the owners.

use std::sync::Arc;

use eyre::{Context, Report};
use nestor::Origin;
use nestor_client::Cluster;
use nestor_s3::{Origins, S3Error};

use crate::config;

pub struct ClusterOrigins {
    cluster: Arc<Cluster>,
    warm_on_write: bool,
}

impl ClusterOrigins {
    pub async fn connect(config: &config::Cluster) -> Result<Arc<Self>, Report> {
        let cluster = Cluster::new(config.membership()?, config.config()?)
            .await
            .wrap_err("connecting to cluster")?;
        tracing::info!(nodes = cluster.node_count(), "cluster ready");
        Ok(Arc::new(Self {
            cluster,
            warm_on_write: config.warm_on_write,
        }))
    }
}

impl Origins for ClusterOrigins {
    fn origin(&self, bucket: &str) -> Result<Arc<dyn Origin>, S3Error> {
        Ok(Arc::new(self.cluster.origin(bucket)))
    }

    fn written(&self, bucket: &str, key: &str, size: u64) {
        if !self.warm_on_write {
            return;
        }
        let origin = self.cluster.origin(bucket);
        let key = key.to_owned();
        tokio::spawn(async move {
            if let Err(e) = origin.warm(&key, size).await {
                tracing::debug!(bucket = origin.bucket(), key, error = %e, "cluster warm failed");
            }
        });
    }
}
