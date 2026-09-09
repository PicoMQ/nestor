//! `object_store` S3 clients for `RustFS` and for a nestor endpoint.

use std::sync::Arc;

use nestor_store::Transport;
use object_store::aws::{AmazonS3, AmazonS3Builder};

use crate::{ACCESS_KEY, BUCKET, REGION, SECRET_KEY};

pub fn client(endpoint: &str) -> Arc<AmazonS3> {
    Arc::new(
        builder(endpoint)
            .with_access_key_id(ACCESS_KEY)
            .with_secret_access_key(SECRET_KEY)
            .build()
            .expect("s3 client"),
    )
}

pub fn origin(endpoint: &str) -> Arc<AmazonS3> {
    let transport = Transport::default();
    Arc::new(
        builder(endpoint)
            .with_access_key_id(ACCESS_KEY)
            .with_secret_access_key(SECRET_KEY)
            .with_client_options(transport.client_options())
            .with_retry(transport.retry_config())
            .build()
            .expect("s3 client"),
    )
}

pub fn unsigned(endpoint: &str) -> Arc<AmazonS3> {
    Arc::new(
        builder(endpoint)
            .with_skip_signature(true)
            .build()
            .expect("s3 client"),
    )
}

fn builder(endpoint: &str) -> AmazonS3Builder {
    AmazonS3Builder::new()
        .with_bucket_name(BUCKET)
        .with_region(REGION)
        .with_endpoint(endpoint)
        .with_allow_http(true)
}
