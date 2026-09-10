//! Rendezvous hashing. Every node is scored against a block with a fixed seed hash, the highest
//! score owns the block, so every client computes the same owner and adding or removing a node
//! only moves the blocks that node wins or loses.

use std::hash::{BuildHasher, Hash, Hasher};
use std::net::SocketAddr;
use std::sync::Arc;

use ahash::RandomState;

use crate::node::{Load, Node};

const SEEDS: (u64, u64, u64, u64) = (
    0x4e65_7374_6f72_2031,
    0x4e65_7374_6f72_2032,
    0x4e65_7374_6f72_2033,
    0x4e65_7374_6f72_2034,
);

fn hasher() -> RandomState {
    RandomState::with_seeds(SEEDS.0, SEEDS.1, SEEDS.2, SEEDS.3)
}

pub(crate) fn seed(addr: &SocketAddr) -> u64 {
    hasher().hash_one(addr)
}

pub(crate) fn object_hash(bucket: &str, key: &str) -> u64 {
    let mut h = hasher().build_hasher();
    bucket.hash(&mut h);
    key.hash(&mut h);
    h.finish()
}

pub(crate) fn score(node_seed: u64, object: u64, block: u32) -> u64 {
    let mut h = hasher().build_hasher();
    h.write_u64(node_seed);
    h.write_u64(object);
    h.write_u32(block);
    h.finish()
}

pub(crate) struct Ranked {
    nodes: Vec<Arc<Node>>,
}

impl Ranked {
    pub fn new(nodes: &[Arc<Node>], object: u64, block: u32) -> Self {
        let mut scored: Vec<(u64, &Arc<Node>)> = nodes
            .iter()
            .map(|node| (score(node.seed(), object, block), node))
            .collect();
        scored.sort_unstable_by(|a, b| b.0.cmp(&a.0).then_with(|| a.1.addr().cmp(&b.1.addr())));
        Self {
            nodes: scored
                .into_iter()
                .map(|(_, node)| Arc::clone(node))
                .collect(),
        }
    }

    #[cfg(test)]
    pub fn owner(&self) -> Option<&Arc<Node>> {
        self.nodes.first()
    }

    pub fn primary(&self) -> Option<Load> {
        let up = self.nodes.iter().filter(|node| node.is_up());
        if let Some(load) = up.clone().find_map(|node| node.try_acquire()) {
            return Some(load);
        }
        up.clone()
            .next()
            .or_else(|| self.nodes.first())
            .map(Node::acquire)
    }

    pub fn secondary(&self, primary: &Node) -> Option<Load> {
        self.nodes
            .iter()
            .filter(|node| node.addr() != primary.addr() && node.is_up())
            .find_map(|node| node.try_acquire())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::net::{Ipv4Addr, SocketAddr};
    use std::sync::Arc;

    use super::{Ranked, object_hash};
    use crate::config::ClusterConfig;
    use crate::node::Node;

    fn nodes(count: u16) -> Vec<Arc<Node>> {
        let config = ClusterConfig::default();
        let client = config.transport.shared_client().unwrap();
        (0..count)
            .map(|i| {
                Arc::new(Node::new(
                    SocketAddr::from((Ipv4Addr::new(10, 0, 0, 1), 9000 + i)),
                    &config,
                    client.clone(),
                ))
            })
            .collect()
    }

    fn owners(nodes: &[Arc<Node>], blocks: u32) -> Vec<SocketAddr> {
        let object = object_hash("bucket", "key");
        (0..blocks)
            .map(|block| Ranked::new(nodes, object, block).owner().unwrap().addr())
            .collect()
    }

    #[test]
    fn deterministic_across_instances() {
        let a = nodes(5);
        let b = nodes(5);
        assert_eq!(owners(&a, 256), owners(&b, 256));
    }

    #[test]
    fn distribution_is_roughly_even() {
        let cluster = nodes(4);
        let mut counts: HashMap<SocketAddr, usize> = HashMap::new();
        for owner in owners(&cluster, 4096) {
            *counts.entry(owner).or_default() += 1;
        }
        assert_eq!(counts.len(), 4);
        for count in counts.values() {
            assert!((800..=1250).contains(count), "{count}");
        }
    }

    #[test]
    fn adding_a_node_moves_about_one_share() {
        let before = owners(&nodes(4), 4096);
        let after = owners(&nodes(5), 4096);
        let moved = before.iter().zip(&after).filter(|(a, b)| a != b).count();
        assert!((600..=1050).contains(&moved), "{moved}");
    }

    #[test]
    fn removing_a_node_spreads_its_blocks() {
        let full = nodes(4);
        let object = object_hash("bucket", "key");
        let removed = full[0].addr();
        let without: Vec<_> = full[1..].to_vec();
        let mut landed: HashMap<SocketAddr, usize> = HashMap::new();
        for block in 0..4096 {
            if Ranked::new(&full, object, block).owner().unwrap().addr() == removed {
                let next = Ranked::new(&without, object, block).owner().unwrap().addr();
                *landed.entry(next).or_default() += 1;
            }
        }
        assert_eq!(landed.len(), 3);
        let max = landed.values().max().unwrap();
        let min = landed.values().min().unwrap();
        assert!(max - min < max / 2, "{landed:?}");
    }

    #[test]
    fn bounded_load_spills_to_next_choice() {
        let config = ClusterConfig {
            load_limit: 1,
            ..ClusterConfig::default()
        };
        let client = config.transport.shared_client().unwrap();
        let cluster: Vec<Arc<Node>> = (0..3)
            .map(|i| {
                Arc::new(Node::new(
                    SocketAddr::from((Ipv4Addr::LOCALHOST, 9000 + i)),
                    &config,
                    client.clone(),
                ))
            })
            .collect();
        let ranked = Ranked::new(&cluster, object_hash("b", "k"), 0);
        let first = ranked.primary().unwrap();
        let second = ranked.primary().unwrap();
        let third = ranked.primary().unwrap();
        let over = ranked.primary().unwrap();
        assert_ne!(first.node().addr(), second.node().addr());
        assert_ne!(second.node().addr(), third.node().addr());
        assert_eq!(over.node().addr(), first.node().addr());
        drop(first);
        assert_eq!(cluster.iter().map(|n| n.inflight()).sum::<usize>(), 3);
    }

    #[test]
    fn down_nodes_are_skipped() {
        let cluster = nodes(3);
        let ranked = Ranked::new(&cluster, object_hash("b", "k"), 7);
        let owner = Arc::clone(ranked.owner().unwrap());
        owner.mark_down(std::time::Duration::from_secs(60));
        let picked = ranked.primary().unwrap();
        assert_ne!(picked.node().addr(), owner.addr());
        assert!(ranked.secondary(picked.node()).is_some());
    }
}
