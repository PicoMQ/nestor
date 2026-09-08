//! S3 clients for `RustFS` and for a nestor endpoint, both are plain `object_store` `AmazonS3`.

use std::sync::Arc;

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
