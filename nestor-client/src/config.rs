//! Cluster settings. `block_size` is the routing unit and should be a multiple of the block size the
//! nodes cache with so every routed request lands on whole node blocks.

use std::time::Duration;

use nestor::{BlockSize, HedgeConfig};
use nestor_store::Transport;

#[derive(Debug, Clone, PartialEq)]
pub struct ClusterConfig {
    pub block_size: BlockSize,
    pub read_window: u32,
    pub load_limit: usize,
    pub down_for: Duration,
    pub hedge: Option<HedgeConfig>,
    pub tls: bool,
    pub credentials: Option<Credentials>,
    pub transport: Transport,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            block_size: BlockSize::default(),
            read_window: 16,
            load_limit: 256,
            down_for: Duration::from_secs(5),
            hedge: Some(HedgeConfig::default()),
            tls: false,
            credentials: None,
            transport: Transport::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Credentials {
    pub access_key: String,
    pub secret_key: String,
}
