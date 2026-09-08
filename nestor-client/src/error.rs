//! Errors raised by the cluster itself. Node responses surface as `OriginError`.

use nestor::OriginError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ClusterError {
    #[error("cluster has no nodes")]
    NoNodes,
    #[error("resolving {host}: {source}")]
    Resolve {
        host: String,
        #[source]
        source: std::io::Error,
    },
    #[error("building client for node {node}: {source}")]
    Client {
        node: String,
        #[source]
        source: object_store::Error,
    },
}

impl From<ClusterError> for OriginError {
    fn from(e: ClusterError) -> Self {
        Self::io(e)
    }
}
