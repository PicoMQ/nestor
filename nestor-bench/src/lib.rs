//! Measures nestor against an S3-compatible origin: a seeded dataset shaped like s3stream segments,
//! scenarios as consumer event lists, a counting origin proxy and JSON reports that `compare` diffs.

pub mod dataset;
pub mod proxy;
pub mod report;
pub mod run;
pub mod target;
pub mod workload;
