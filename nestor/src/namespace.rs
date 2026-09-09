//! A namespace is an origin plus the tunables that govern how its objects are cached.
//! `NamespaceState` is the registered form with runtime state attached.

use std::sync::Arc;
use std::time::Duration;

use crate::block::BlockSize;
use crate::fetch::Latency;
use crate::key::NamespaceId;
use crate::metrics::NamespaceMetrics;
use crate::origin::Origin;
use crate::policy::FetchPolicy;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(
    feature = "serde",
    derive(serde::Deserialize),
    serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)
)]
pub enum Consistency {
    Immutable,
    Etag {
        #[cfg_attr(feature = "serde", serde(with = "humantime_serde"))]
        ttl: Duration,
    },
}

impl Consistency {
    pub const fn is_immutable(self) -> bool {
        matches!(self, Self::Immutable)
    }

    pub const fn meta_ttl(self) -> Option<Duration> {
        match self {
            Self::Immutable => None,
            Self::Etag { ttl } => Some(ttl),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct NamespaceConfig {
    pub block_size: BlockSize,
    pub fetch_window: u32,
    pub read_window: u32,
    pub consistency: Consistency,
    pub readahead: u32,
    pub fetch: FetchPolicy,
}

impl Default for NamespaceConfig {
    fn default() -> Self {
        Self {
            block_size: BlockSize::default(),
            fetch_window: 8,
            read_window: 16,
            consistency: Consistency::Etag {
                ttl: Duration::from_secs(60),
            },
            readahead: 8,
            fetch: FetchPolicy::default(),
        }
    }
}

impl NamespaceConfig {
    pub fn block_size(mut self, block_size: BlockSize) -> Self {
        self.block_size = block_size;
        self
    }

    pub fn fetch_window(mut self, blocks: u32) -> Self {
        self.fetch_window = blocks.max(1);
        self
    }

    pub fn read_window(mut self, blocks: u32) -> Self {
        self.read_window = blocks.max(1);
        self
    }

    pub fn consistency(mut self, consistency: Consistency) -> Self {
        self.consistency = consistency;
        self
    }

    pub fn readahead(mut self, blocks: u32) -> Self {
        self.readahead = blocks;
        self
    }

    pub fn fetch(mut self, policy: FetchPolicy) -> Self {
        self.fetch = policy;
        self
    }
}

#[derive(Clone)]
pub struct Namespace {
    pub name: Arc<str>,
    pub origin: Arc<dyn Origin>,
    pub config: NamespaceConfig,
}

impl Namespace {
    pub fn new(name: impl Into<Arc<str>>, origin: Arc<dyn Origin>) -> Self {
        Self {
            name: name.into(),
            origin,
            config: NamespaceConfig::default(),
        }
    }

    pub fn config(mut self, config: NamespaceConfig) -> Self {
        self.config = config;
        self
    }

    pub fn block_size(mut self, block_size: BlockSize) -> Self {
        self.config = self.config.block_size(block_size);
        self
    }

    pub fn fetch_window(mut self, blocks: u32) -> Self {
        self.config = self.config.fetch_window(blocks);
        self
    }

    pub fn read_window(mut self, blocks: u32) -> Self {
        self.config = self.config.read_window(blocks);
        self
    }

    pub fn consistency(mut self, consistency: Consistency) -> Self {
        self.config = self.config.consistency(consistency);
        self
    }

    pub fn readahead(mut self, blocks: u32) -> Self {
        self.config = self.config.readahead(blocks);
        self
    }

    pub fn fetch(mut self, policy: FetchPolicy) -> Self {
        self.config = self.config.fetch(policy);
        self
    }
}

impl std::fmt::Debug for Namespace {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Namespace")
            .field("name", &self.name)
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

pub(crate) struct NamespaceState {
    pub id: NamespaceId,
    pub name: Arc<str>,
    pub origin: Arc<dyn Origin>,
    pub config: NamespaceConfig,
    pub latency: Latency,
    pub metrics: NamespaceMetrics,
}

impl NamespaceState {
    pub fn new(id: NamespaceId, namespace: Namespace) -> Self {
        let metrics = NamespaceMetrics::new(&namespace.name);
        Self {
            id,
            name: namespace.name,
            origin: namespace.origin,
            config: namespace.config,
            latency: Latency::default(),
            metrics,
        }
    }

    pub fn meta_ttl(&self) -> Option<Duration> {
        self.config.consistency.meta_ttl()
    }
}
