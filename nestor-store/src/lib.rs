//! `object_store` adapters in both directions. `ObjectStoreOrigin` makes any `ObjectStore` a Nestor
//! origin, `NestorStore` implements `ObjectStore` on top of Nestor so existing code gets caching
//! without API changes.

mod error;
mod origin;
mod store;
mod transport;

pub use origin::ObjectStoreOrigin;
pub use store::NestorStore;
pub use transport::Transport;

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use bytes::Bytes;
    use nestor::{BlockSize, CacheConfig, Consistency, FetchPolicy, Namespace, Nestor};
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::{GetOptions, GetRange, ObjectStore, ObjectStoreExt, PutPayload};

    use super::*;

    const BLOCK: u64 = 64 * 1024;

    async fn store(consistency: Consistency) -> (NestorStore, Arc<InMemory>) {
        let inner = Arc::new(InMemory::new());
        let origin = Arc::new(ObjectStoreOrigin::new(inner.clone()));
        let ns = Namespace::new("mem", origin)
            .block_size(BlockSize::new(BLOCK as u32).unwrap())
            .consistency(consistency)
            .fetch(FetchPolicy::default().hedge(None));
        let nestor = Nestor::builder(CacheConfig::memory(64 << 20))
            .namespace(ns)
            .build()
            .await
            .unwrap();
        let id = nestor.namespace("mem").unwrap();
        (NestorStore::new(nestor, id, inner.clone()), inner)
    }

    fn pattern(len: usize) -> Bytes {
        Bytes::from((0..len).map(|i| (i % 241) as u8).collect::<Vec<u8>>())
    }

    #[tokio::test]
    async fn put_then_read_through_cache() {
        let (store, inner) = store(Consistency::Etag {
            ttl: Duration::from_secs(60),
        })
        .await;
        let data = pattern(3 * BLOCK as usize + 11);
        let path = Path::from("a/b/c.bin");
        store
            .put(&path, PutPayload::from_bytes(data.clone()))
            .await
            .unwrap();

        assert_eq!(
            store.get_range(&path, 10..20).await.unwrap(),
            data.slice(10..20)
        );
        let full = store.get(&path).await.unwrap();
        assert_eq!(full.meta.size, data.len() as u64);
        assert_eq!(full.bytes().await.unwrap(), data);

        inner.delete(&path).await.unwrap();
        assert_eq!(
            store.get_range(&path, 0..5).await.unwrap(),
            data.slice(0..5)
        );
    }

    #[tokio::test]
    async fn writes_invalidate_cached_content() {
        let (store, _) = store(Consistency::Etag {
            ttl: Duration::from_secs(3600),
        })
        .await;
        let path = Path::from("k");
        let v1 = pattern(BLOCK as usize);
        store
            .put(&path, PutPayload::from_bytes(v1.clone()))
            .await
            .unwrap();
        assert_eq!(
            store.get_range(&path, 0..16).await.unwrap(),
            v1.slice(0..16)
        );

        let v2 = Bytes::from(vec![7u8; BLOCK as usize + 3]);
        store
            .put(&path, PutPayload::from_bytes(v2.clone()))
            .await
            .unwrap();
        assert_eq!(
            store.get_range(&path, 0..16).await.unwrap(),
            v2.slice(0..16)
        );
        assert_eq!(store.head(&path).await.unwrap().size, v2.len() as u64);

        store.delete(&path).await.unwrap();
        assert!(matches!(
            store.get_range(&path, 0..1).await,
            Err(object_store::Error::NotFound { .. })
        ));
    }

    #[tokio::test]
    async fn conditional_gets_and_ranges() {
        let (store, _) = store(Consistency::Etag {
            ttl: Duration::from_secs(60),
        })
        .await;
        let path = Path::from("cond");
        let data = pattern(2 * BLOCK as usize);
        store
            .put(&path, PutPayload::from_bytes(data.clone()))
            .await
            .unwrap();
        let meta = store.head(&path).await.unwrap();
        let etag = meta.e_tag.clone().unwrap();

        let not_modified = store
            .get_opts(
                &path,
                GetOptions {
                    if_none_match: Some(etag.clone()),
                    ..Default::default()
                },
            )
            .await;
        assert!(matches!(
            not_modified,
            Err(object_store::Error::NotModified { .. })
        ));

        let precondition = store
            .get_opts(
                &path,
                GetOptions {
                    if_match: Some("\"nope\"".into()),
                    ..Default::default()
                },
            )
            .await;
        assert!(matches!(
            precondition,
            Err(object_store::Error::Precondition { .. })
        ));

        let suffix = store
            .get_opts(
                &path,
                GetOptions {
                    range: Some(GetRange::Suffix(10)),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(suffix.range, data.len() as u64 - 10..data.len() as u64);
        assert_eq!(suffix.bytes().await.unwrap(), data.slice(data.len() - 10..));

        let head = store
            .get_opts(
                &path,
                GetOptions {
                    head: true,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(head.meta.size, data.len() as u64);
    }

    #[tokio::test]
    async fn multipart_upload_invalidates_on_complete() {
        let (store, _) = store(Consistency::Etag {
            ttl: Duration::from_secs(3600),
        })
        .await;
        let path = Path::from("mp");
        store
            .put(&path, PutPayload::from_static(b"old"))
            .await
            .unwrap();
        assert_eq!(
            store.get_range(&path, 0..3).await.unwrap(),
            Bytes::from_static(b"old")
        );

        let mut upload = store.put_multipart(&path).await.unwrap();
        upload
            .put_part(PutPayload::from_static(b"new-content"))
            .await
            .unwrap();
        upload.complete().await.unwrap();
        assert_eq!(
            store.get_range(&path, 0..11).await.unwrap(),
            Bytes::from_static(b"new-content")
        );
    }

    #[tokio::test]
    async fn listing_passes_through() {
        let (store, _) = store(Consistency::Immutable).await;
        for name in ["x/1", "x/2", "y/3"] {
            store
                .put(&Path::from(name), PutPayload::from_static(b"z"))
                .await
                .unwrap();
        }
        let listed: Vec<_> = store
            .list(Some(&Path::from("x")))
            .map(|m| m.unwrap().location.to_string())
            .collect()
            .await;
        assert_eq!(listed, vec!["x/1", "x/2"]);
        let delimited = store.list_with_delimiter(None).await.unwrap();
        assert_eq!(delimited.common_prefixes.len(), 2);
    }

    use futures::StreamExt;
}
