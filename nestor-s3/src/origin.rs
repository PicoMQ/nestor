//! Origin endpoint and credentials shared by the forwarder and the per-bucket Nestor origins.

use std::sync::Arc;

use http::Uri;
use http::uri::{Authority, Scheme};
use nestor::Origin;
use nestor_store::ObjectStoreOrigin;
use object_store::StaticCredentialProvider;
use object_store::aws::{AmazonS3Builder, AwsCredential, AwsCredentialProvider};

use crate::error::S3Error;

#[derive(Clone)]
pub struct OriginConfig {
    pub endpoint: Uri,
    pub region: String,
    pub credentials: Option<AwsCredentialProvider>,
    pub virtual_hosted: bool,
}

impl OriginConfig {
    pub fn anonymous(endpoint: Uri, region: impl Into<String>) -> Self {
        Self {
            endpoint,
            region: region.into(),
            credentials: None,
            virtual_hosted: false,
        }
    }

    pub fn with_static_credentials(
        mut self,
        access_key: impl Into<String>,
        secret_key: impl Into<String>,
        session_token: Option<String>,
    ) -> Self {
        self.credentials = Some(Arc::new(StaticCredentialProvider::new(AwsCredential {
            key_id: access_key.into(),
            secret_key: secret_key.into(),
            token: session_token,
        })));
        self
    }

    pub fn with_default_credentials(mut self) -> Result<Self, S3Error> {
        let probe = AmazonS3Builder::from_env()
            .with_region(self.region.clone())
            .with_bucket_name("nestor")
            .build()
            .map_err(|e| S3Error::internal(format!("failed to resolve AWS credentials: {e}")))?;
        self.credentials = Some(Arc::clone(probe.credentials()));
        Ok(self)
    }

    pub fn with_virtual_hosted(mut self, virtual_hosted: bool) -> Self {
        self.virtual_hosted = virtual_hosted;
        self
    }

    pub fn scheme(&self) -> &Scheme {
        self.endpoint.scheme().unwrap_or(&Scheme::HTTPS)
    }

    pub fn authority(&self) -> Option<&Authority> {
        self.endpoint.authority()
    }

    pub(crate) fn bucket_origin(&self, bucket: &str) -> Result<Arc<dyn Origin>, S3Error> {
        let mut builder = AmazonS3Builder::new()
            .with_bucket_name(bucket)
            .with_region(self.region.clone())
            .with_endpoint(self.endpoint.to_string().trim_end_matches('/'))
            .with_allow_http(true)
            .with_virtual_hosted_style_request(self.virtual_hosted);
        builder = match &self.credentials {
            Some(provider) => builder.with_credentials(Arc::clone(provider)),
            None => builder.with_skip_signature(true),
        };
        let store = builder
            .build()
            .map_err(|e| S3Error::internal(format!("failed to build origin client: {e}")))?;
        Ok(Arc::new(ObjectStoreOrigin::new(Arc::new(store))))
    }
}

impl std::fmt::Debug for OriginConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OriginConfig")
            .field("endpoint", &self.endpoint)
            .field("region", &self.region)
            .field("credentials", &self.credentials.is_some())
            .field("virtual_hosted", &self.virtual_hosted)
            .finish()
    }
}
