//! Nestor embedded in the test process, RAM plus a disk tier, with `NestorStore` giving ordinary
//! `object_store` code caching in front of `RustFS`. Origin traffic is counted on the `Origin`.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use futures::TryStreamExt;
use nestor::{
    BlockSize, CacheConfig, Consistency, DiskConfig, GetOptions, GetResponse, Namespace, Nestor,
    ObjectMeta, Origin, OriginError,
};
use nestor_e2e::data::{KIB, MIB, body, payload, slice};
use nestor_e2e::{env, init, s3, step, wait};
use nestor_store::{NestorStore, ObjectStoreOrigin};
use object_store::aws::AmazonS3;
use object_store::path::Path;
use object_store::{Error, ObjectStore, ObjectStoreExt, WriteMultipart};

#[derive(Clone)]
struct Counting {
    inner: Arc<ObjectStoreOrigin>,
    gets: Arc<AtomicUsize>,
}

#[async_trait]
impl Origin for Counting {
    async fn get(&self, object: &str, options: GetOptions) -> Result<GetResponse, OriginError> {
        self.gets.fetch_add(1, Ordering::Relaxed);
        self.inner.get(object, options).await
    }

    async fn head(&self, object: &str) -> Result<ObjectMeta, OriginError> {
        self.inner.head(object).await
    }
}

struct Embedded {
    rustfs: Arc<AmazonS3>,
    store: NestorStore,
    nestor: Nestor,
    gets: Arc<AtomicUsize>,
    _disk: tempfile::TempDir,
}

impl Embedded {
    async fn start(consistency: Consistency) -> Self {
        init();
        let rustfs = s3::client(&env("NESTOR_E2E_RUSTFS", "http://127.0.0.1:19000"));
        wait::bucket(rustfs.as_ref(), Duration::from_secs(60)).await;
        let gets = Arc::new(AtomicUsize::new(0));
        let origin = Counting {
            inner: Arc::new(ObjectStoreOrigin::new(Arc::clone(&rustfs) as _)),
            gets: Arc::clone(&gets),
        };
        let disk = tempfile::tempdir().expect("tempdir");
        let mut disk_config = DiskConfig::new(disk.path(), 256 * MIB);
        disk_config.region_size = 16 * MIB;
        disk_config.direct_io = false;
        let nestor = Nestor::builder(CacheConfig::memory(32 * MIB).disk(disk_config))
            .namespace(
                Namespace::new("rustfs", Arc::new(origin))
                    .block_size(BlockSize::new(256 * KIB as u32).unwrap())
                    .consistency(consistency),
            )
            .build()
            .await
            .expect("nestor");
        let ns = nestor.namespace("rustfs").unwrap();
        let store = NestorStore::new(nestor.clone(), ns, Arc::clone(&rustfs) as _);
        step!(
            block = "256 KiB",
            memory = "32 MiB",
            disk = "256 MiB",
            "embedded nestor ready"
        );
        Self {
            rustfs,
            store,
            nestor,
            gets,
            _disk: disk,
        }
    }

    fn gets(&self) -> usize {
        self.gets.load(Ordering::Relaxed)
    }

    async fn read(&self, key: &Path) -> Bytes {
        body(self.store.get(key).await.expect("get")).await
    }
}

#[tokio::test]
#[ignore = "needs the library compose stack"]
async fn reads_write_and_ranges_through_nestor_store() {
    let e = Embedded::start(Consistency::Etag {
        ttl: Duration::from_secs(60),
    })
    .await;
    let key = Path::from("library/object");
    let data = payload(3 * MIB + 321, 3);

    e.store
        .put(&key, data.clone().into())
        .await
        .expect("put via store");
    assert_eq!(
        body(e.rustfs.get(&key).await.expect("at rustfs")).await,
        data
    );
    step!("PUT went to RustFS and populated the cache");

    assert_eq!(e.read(&key).await, data);
    assert_eq!(e.gets(), 0, "populated object needs no origin GET");

    let ranges = [
        7..8u64,
        (MIB as u64 - 1)..(MIB as u64 + 1),
        (2 * MIB as u64)..(data.len() as u64),
    ];
    for range in &ranges {
        assert_eq!(
            e.store.get_range(&key, range.clone()).await.expect("range"),
            slice(&data, range),
            "{range:?}"
        );
    }
    let many = e.store.get_ranges(&key, &ranges).await.expect("get_ranges");
    assert_eq!(many.len(), 3);
    assert_eq!(many[2], slice(&data, &ranges[2]));
    assert_eq!(e.gets(), 0);
    step!("ranges served from cache");

    let head = e.store.head(&key).await.expect("head");
    assert_eq!(head.size, data.len() as u64);
}

#[tokio::test]
#[ignore = "needs the library compose stack"]
async fn misses_go_to_the_origin_once() {
    let e = Embedded::start(Consistency::Etag {
        ttl: Duration::from_secs(60),
    })
    .await;
    let key = Path::from("library/external");
    let data = payload(5 * MIB, 5);
    e.rustfs
        .put(&key, data.clone().into())
        .await
        .expect("put at rustfs");

    assert_eq!(e.read(&key).await, data);
    let after_first = e.gets();
    assert!(after_first > 0);
    step!(origin_gets = after_first, "first read fetched from RustFS");

    assert_eq!(e.read(&key).await, data);
    assert_eq!(e.gets(), after_first, "second read is a cache hit");
    step!("second read served from cache");
}

#[tokio::test]
#[ignore = "needs the library compose stack"]
async fn multipart_list_copy_and_delete() {
    let e = Embedded::start(Consistency::Etag {
        ttl: Duration::from_secs(60),
    })
    .await;
    let key = Path::from("library/multipart");
    let data = payload(24 * MIB, 24);
    let upload = e.store.put_multipart(&key).await.expect("multipart");
    let mut writer = WriteMultipart::new_with_chunk_size(upload, 8 * MIB);
    writer.write(&data);
    writer.finish().await.expect("finish");
    assert_eq!(e.read(&key).await, data);
    step!("multipart upload read back");

    let copy = Path::from("library/copy");
    e.store.copy(&key, &copy).await.expect("copy");
    assert_eq!(e.read(&copy).await, data);

    let listed: Vec<Path> = e
        .store
        .list(Some(&Path::from("library")))
        .map_ok(|m| m.location)
        .try_collect()
        .await
        .expect("list");
    assert!(listed.contains(&key) && listed.contains(&copy));

    e.store.delete(&copy).await.expect("delete");
    assert!(matches!(
        e.store.get(&copy).await,
        Err(Error::NotFound { .. })
    ));
    assert!(matches!(
        e.rustfs.get(&copy).await,
        Err(Error::NotFound { .. })
    ));
    step!("copy listed then deleted at both ends");
}

#[tokio::test]
#[ignore = "needs the library compose stack"]
async fn immutable_namespace_keeps_the_cached_version_until_invalidated() {
    let e = Embedded::start(Consistency::Immutable).await;
    let key = Path::from("library/immutable");
    let v1 = payload(MIB, 1);
    let v2 = payload(MIB, 2);
    e.rustfs.put(&key, v1.clone().into()).await.expect("v1");
    assert_eq!(e.read(&key).await, v1);

    e.rustfs.put(&key, v2.clone().into()).await.expect("v2");
    assert_eq!(
        e.read(&key).await,
        v1,
        "immutable mode never re-checks the origin"
    );
    step!("overwrite at origin not visible in immutable mode");

    e.nestor
        .invalidate(e.store.namespace(), key.as_ref())
        .expect("invalidate");
    assert_eq!(e.read(&key).await, v2);
    step!("explicit invalidate surfaces the new version");
}
