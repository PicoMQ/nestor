//! `Origin` backed by an `object_store::ObjectStore`.

use std::sync::Arc;
use std::time::SystemTime;

use async_trait::async_trait;
use bytes::Bytes;
use futures::TryStreamExt;
use nestor::{GetOptions, GetResponse, ObjectMeta, Origin, OriginError};
use object_store::path::Path;
use object_store::{GetRange, ObjectStore, ObjectStoreExt};

use crate::error::from_store;

#[derive(Clone)]
pub struct ObjectStoreOrigin {
    store: Arc<dyn ObjectStore>,
}

impl ObjectStoreOrigin {
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self { store }
    }

    pub fn store(&self) -> &Arc<dyn ObjectStore> {
        &self.store
    }
}

impl std::fmt::Debug for ObjectStoreOrigin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ObjectStoreOrigin({})", self.store)
    }
}

pub(crate) fn path_of(object: &str) -> Path {
    Path::parse(object).unwrap_or_else(|_| Path::from(object))
}

pub(crate) fn meta_from_store(meta: &object_store::ObjectMeta) -> ObjectMeta {
    ObjectMeta {
        size: meta.size,
        etag: meta
            .e_tag
            .as_deref()
            .map(|e| Bytes::copy_from_slice(e.as_bytes())),
        last_modified: Some(SystemTime::from(meta.last_modified)),
    }
}

#[async_trait]
impl Origin for ObjectStoreOrigin {
    async fn get(&self, object: &str, options: GetOptions) -> Result<GetResponse, OriginError> {
        let path = path_of(object);
        let get = object_store::GetOptions {
            range: options.range.map(GetRange::Bounded),
            if_match: options
                .if_match
                .map(|etag| String::from_utf8_lossy(&etag).into_owned()),
            ..Default::default()
        };
        let result = self.store.get_opts(&path, get).await.map_err(from_store)?;
        Ok(GetResponse {
            meta: meta_from_store(&result.meta),
            range: result.range.clone(),
            body: Box::pin(result.into_stream().map_err(from_store)),
        })
    }

    async fn head(&self, object: &str) -> Result<ObjectMeta, OriginError> {
        let meta = self
            .store
            .head(&path_of(object))
            .await
            .map_err(from_store)?;
        Ok(meta_from_store(&meta))
    }
}
