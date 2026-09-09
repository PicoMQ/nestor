//! `object_store` S3 clients for `RustFS` and for a nestor endpoint.

use std::sync::Arc;

use nestor_store::Transport;
use object_store::Error;
use object_store::aws::{AmazonS3, AmazonS3Builder};

use crate::{ACCESS_KEY, BUCKET, REGION, SECRET_KEY};

pub fn client(endpoint: &str) -> Arc<AmazonS3> {
    client_at(endpoint, BUCKET)
}

pub fn client_at(endpoint: &str, bucket: &str) -> Arc<AmazonS3> {
    Arc::new(
        builder(endpoint, bucket)
            .with_access_key_id(ACCESS_KEY)
            .with_secret_access_key(SECRET_KEY)
            .build()
            .expect("s3 client"),
    )
}

pub fn origin(endpoint: &str) -> Arc<AmazonS3> {
    origin_at(endpoint, BUCKET)
}

pub fn origin_at(endpoint: &str, bucket: &str) -> Arc<AmazonS3> {
    let transport = Transport::default();
    Arc::new(
        builder(endpoint, bucket)
            .with_access_key_id(ACCESS_KEY)
            .with_secret_access_key(SECRET_KEY)
            .with_client_options(transport.client_options())
            .with_retry(transport.retry_config())
            .build()
            .expect("s3 client"),
    )
}

pub fn aws(endpoint: &str, bucket: &str) -> Result<Arc<AmazonS3>, Error> {
    let transport = Transport::default();
    AmazonS3Builder::from_env()
        .with_bucket_name(bucket)
        .with_region(REGION)
        .with_endpoint(endpoint.trim_end_matches('/'))
        .with_virtual_hosted_style_request(false)
        .with_client_options(transport.client_options())
        .with_retry(transport.retry_config())
        .build()
        .map(Arc::new)
}

pub fn unsigned(endpoint: &str) -> Arc<AmazonS3> {
    Arc::new(
        builder(endpoint, BUCKET)
            .with_skip_signature(true)
            .build()
            .expect("s3 client"),
    )
}

fn builder(endpoint: &str, bucket: &str) -> AmazonS3Builder {
    AmazonS3Builder::new()
        .with_bucket_name(bucket)
        .with_region(REGION)
        .with_endpoint(endpoint)
        .with_allow_http(true)
}
