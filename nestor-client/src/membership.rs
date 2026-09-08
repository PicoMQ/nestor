//! Where the node list comes from. A static list never changes, a DNS name is re-resolved on an
//! interval so scale out and node loss show up without restarts.

use std::net::SocketAddr;
use std::time::Duration;

use crate::error::ClusterError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Membership {
    Static(Vec<SocketAddr>),
    Dns {
        host: String,
        port: u16,
        refresh: Duration,
    },
}

impl Membership {
    pub fn dns(host: impl Into<String>, port: u16) -> Self {
        Self::Dns {
            host: host.into(),
            port,
            refresh: Duration::from_secs(10),
        }
    }

    pub fn refresh(self, interval: Duration) -> Self {
        match self {
            Self::Dns { host, port, .. } => Self::Dns {
                host,
                port,
                refresh: interval,
            },
            fixed @ Self::Static(_) => fixed,
        }
    }

    pub(crate) fn refresh_interval(&self) -> Option<Duration> {
        match self {
            Self::Static(_) => None,
            Self::Dns { refresh, .. } => Some(*refresh),
        }
    }

    pub(crate) async fn resolve(&self) -> Result<Vec<SocketAddr>, ClusterError> {
        match self {
            Self::Static(addrs) => Ok(addrs.clone()),
            Self::Dns { host, port, .. } => {
                let mut addrs: Vec<SocketAddr> = tokio::net::lookup_host((host.as_str(), *port))
                    .await
                    .map_err(|source| ClusterError::Resolve {
                        host: host.clone(),
                        source,
                    })?
                    .collect();
                addrs.sort_unstable();
                addrs.dedup();
                Ok(addrs)
            }
        }
    }
}
