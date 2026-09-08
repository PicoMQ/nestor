//! S3-compatible HTTP frontend. GET and HEAD are served from Nestor, every other request is re-
//! signed and forwarded to the origin, with the cache invalidated or populated as the write
//! completes.

mod addressing;
mod auth;
mod body;
mod error;
mod forward;
mod headers;
mod origin;
mod routes;
mod service;
pub mod sigv4;

pub use addressing::{Addressing, Target};
pub use auth::Auth;
pub use error::S3Error;
pub use origin::{OriginConfig, Origins};
pub use service::{S3Config, S3Service};
