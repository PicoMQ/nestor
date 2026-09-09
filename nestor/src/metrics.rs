//! Metric names and the per-namespace handles used on the hot path. Handles are resolved once at
//! registration, recording is an atomic add.

use metrics::{Counter, Histogram, counter, histogram};

pub const BLOCKS_HIT: &str = "nestor_blocks_hit_total";
pub const BLOCKS_MISS: &str = "nestor_blocks_miss_total";
pub const BLOCKS_JOINED: &str = "nestor_blocks_joined_total";
pub const BLOCKS_STALE: &str = "nestor_blocks_stale_total";
pub const ORIGIN_REQUESTS: &str = "nestor_origin_requests_total";
pub const ORIGIN_BYTES: &str = "nestor_origin_bytes_total";
pub const ORIGIN_ERRORS: &str = "nestor_origin_errors_total";
pub const ORIGIN_RETRIES: &str = "nestor_origin_retries_total";
pub const ORIGIN_TIMEOUTS: &str = "nestor_origin_timeouts_total";
pub const ORIGIN_TTFB: &str = "nestor_origin_ttfb_seconds";
pub const HEDGES: &str = "nestor_hedges_total";
pub const HEDGE_WINS: &str = "nestor_hedge_wins_total";
pub const READAHEAD_BLOCKS: &str = "nestor_readahead_blocks_total";
pub const META_HEADS: &str = "nestor_meta_heads_total";
pub const BYTES_SERVED: &str = "nestor_bytes_served_total";

pub const LABEL_NAMESPACE: &str = "namespace";

pub(crate) struct NamespaceMetrics {
    pub hits: Counter,
    pub misses: Counter,
    pub joined: Counter,
    pub stale: Counter,
    pub origin_requests: Counter,
    pub origin_bytes: Counter,
    pub origin_errors: Counter,
    pub origin_retries: Counter,
    pub origin_timeouts: Counter,
    pub origin_ttfb: Histogram,
    pub hedges: Counter,
    pub hedge_wins: Counter,
    pub readahead_blocks: Counter,
    pub meta_heads: Counter,
    pub bytes_served: Counter,
}

impl NamespaceMetrics {
    pub fn new(namespace: &str) -> Self {
        let ns = namespace.to_owned();
        Self {
            hits: counter!(BLOCKS_HIT, LABEL_NAMESPACE => ns.clone()),
            misses: counter!(BLOCKS_MISS, LABEL_NAMESPACE => ns.clone()),
            joined: counter!(BLOCKS_JOINED, LABEL_NAMESPACE => ns.clone()),
            stale: counter!(BLOCKS_STALE, LABEL_NAMESPACE => ns.clone()),
            origin_requests: counter!(ORIGIN_REQUESTS, LABEL_NAMESPACE => ns.clone()),
            origin_bytes: counter!(ORIGIN_BYTES, LABEL_NAMESPACE => ns.clone()),
            origin_errors: counter!(ORIGIN_ERRORS, LABEL_NAMESPACE => ns.clone()),
            origin_retries: counter!(ORIGIN_RETRIES, LABEL_NAMESPACE => ns.clone()),
            origin_timeouts: counter!(ORIGIN_TIMEOUTS, LABEL_NAMESPACE => ns.clone()),
            origin_ttfb: histogram!(ORIGIN_TTFB, LABEL_NAMESPACE => ns.clone()),
            hedges: counter!(HEDGES, LABEL_NAMESPACE => ns.clone()),
            hedge_wins: counter!(HEDGE_WINS, LABEL_NAMESPACE => ns.clone()),
            readahead_blocks: counter!(READAHEAD_BLOCKS, LABEL_NAMESPACE => ns.clone()),
            meta_heads: counter!(META_HEADS, LABEL_NAMESPACE => ns.clone()),
            bytes_served: counter!(BYTES_SERVED, LABEL_NAMESPACE => ns),
        }
    }
}
