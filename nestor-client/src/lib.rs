//! Turns a cluster of `nestor` nodes into one logical cache. Every block of an object is routed to
//! the node that wins rendezvous hashing for it, so all clients agree on an owner with no
//! coordination, and bounded load spills hot blocks to the next choice.

mod cluster;
mod config;
mod error;
mod membership;
mod node;
mod origin;
mod read;
mod router;

pub use cluster::Cluster;
pub use config::{ClusterConfig, Credentials};
pub use error::ClusterError;
pub use membership::Membership;
pub use origin::ClusterOrigin;
