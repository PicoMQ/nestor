//! The cluster handle: current node snapshot, membership refresh and the entry points that hand out
//! per bucket origins.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex, RwLock};

use nestor_store::SharedClient;
use tokio::task::JoinHandle;

use crate::config::ClusterConfig;
use crate::error::ClusterError;
use crate::membership::Membership;
use crate::node::Node;
use crate::origin::ClusterOrigin;
use crate::router::Ranked;

pub struct Cluster {
    config: ClusterConfig,
    client: SharedClient,
    membership: Membership,
    nodes: RwLock<Arc<[Arc<Node>]>>,
    refresher: Mutex<Option<JoinHandle<()>>>,
}

impl Cluster {
    pub async fn new(
        membership: Membership,
        config: ClusterConfig,
    ) -> Result<Arc<Self>, ClusterError> {
        let addrs = membership.resolve().await?;
        if addrs.is_empty() {
            return Err(ClusterError::NoNodes);
        }
        let client = config
            .transport
            .shared_client()
            .map_err(ClusterError::Transport)?;
        let nodes: Arc<[Arc<Node>]> = addrs
            .into_iter()
            .map(|addr| Arc::new(Node::new(addr, &config, client.clone())))
            .collect();
        let cluster = Arc::new(Self {
            config,
            client,
            membership,
            nodes: RwLock::new(nodes),
            refresher: Mutex::new(None),
        });
        if let Some(interval) = cluster.membership.refresh_interval() {
            let weak = Arc::downgrade(&cluster);
            let handle = tokio::spawn(async move {
                loop {
                    tokio::time::sleep(interval).await;
                    let Some(cluster) = weak.upgrade() else {
                        return;
                    };
                    match cluster.membership.resolve().await {
                        Ok(addrs) if addrs.is_empty() => {
                            tracing::warn!("membership resolved to no nodes, keeping current set");
                        }
                        Ok(addrs) => cluster.apply(addrs),
                        Err(e) => tracing::warn!(error = %e, "membership refresh failed"),
                    }
                }
            });
            *cluster.refresher.lock().unwrap_or_else(|e| e.into_inner()) = Some(handle);
        }
        Ok(cluster)
    }

    pub fn config(&self) -> &ClusterConfig {
        &self.config
    }

    pub fn node_count(&self) -> usize {
        self.snapshot().len()
    }

    pub fn origin(self: &Arc<Self>, bucket: impl Into<Arc<str>>) -> ClusterOrigin {
        ClusterOrigin::new(Arc::clone(self), bucket.into())
    }

    pub(crate) fn rank(&self, object: u64, block: u32) -> Ranked {
        Ranked::new(&self.snapshot(), object, block)
    }

    fn snapshot(&self) -> Arc<[Arc<Node>]> {
        Arc::clone(&self.nodes.read().unwrap_or_else(|e| e.into_inner()))
    }

    fn apply(&self, mut addrs: Vec<SocketAddr>) {
        addrs.sort_unstable();
        let current = self.snapshot();
        let mut added = 0usize;
        let next: Arc<[Arc<Node>]> = addrs
            .iter()
            .map(|addr| {
                if let Some(node) = current.iter().find(|node| node.addr() == *addr) {
                    Arc::clone(node)
                } else {
                    added += 1;
                    Arc::new(Node::new(*addr, &self.config, self.client.clone()))
                }
            })
            .collect();
        let removed = current
            .iter()
            .filter(|node| addrs.binary_search(&node.addr()).is_err())
            .count();
        if added > 0 || removed > 0 {
            tracing::info!(
                added,
                removed,
                total = next.len(),
                "cluster membership changed"
            );
            *self.nodes.write().unwrap_or_else(|e| e.into_inner()) = next;
        }
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        if let Some(handle) = self
            .refresher
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
        {
            handle.abort();
        }
    }
}

impl std::fmt::Debug for Cluster {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Cluster")
            .field("membership", &self.membership)
            .field("nodes", &self.node_count())
            .field("block_size", &self.config.block_size)
            .finish_non_exhaustive()
    }
}
