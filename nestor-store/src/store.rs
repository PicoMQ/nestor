//! `ObjectStore` that serves reads from Nestor and forwards writes to the wrapped store. Writes
//! invalidate the cached object, PUT bodies can populate it.

use std::ops::Range;
use std::sync::Arc;
use std::time::SystemTime;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use chrono::{DateTime, Utc};
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt, future};
use nestor::{
    NamespaceId, Nestor, ObjectMeta as NestorMeta, Precondition, Preconditions, ReadRange,
};
use object_store::path::Path;
use object_store::{
    Attributes, CopyOptions, Error, Extensions, GetOptions, GetRange, GetResult, GetResultPayload,
    ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions,
    PutPayload, PutResult, RenameOptions, Result, UploadPart,
};

use crate::error::to_store;

#[derive(Clone)]
pub struct NestorStore {
    nestor: Nestor,
    ns: NamespaceId,
    inner: Arc<dyn ObjectStore>,
    populate: bool,
}

impl NestorStore {
    pub fn new(nestor: Nestor, ns: NamespaceId, inner: Arc<dyn ObjectStore>) -> Self {
        Self {
            nestor,
            ns,
            inner,
            populate: true,
        }
    }

    pub fn populate_on_write(mut self, populate: bool) -> Self {
        self.populate = populate;
        self
    }

    pub fn nestor(&self) -> &Nestor {
        &self.nestor
    }

    pub fn namespace(&self) -> NamespaceId {
        self.ns
    }

    pub fn inner(&self) -> &Arc<dyn ObjectStore> {
        &self.inner
    }

    fn invalidate(&self, location: &Path) {
        let _ = self.nestor.invalidate(self.ns, location.as_ref());
    }

    fn populate(&self, location: &Path, payload: &PutPayload, result: &PutResult) {
        if !self.populate {
            return;
        }
        let etag = result
            .e_tag
            .as_deref()
            .map(|e| Bytes::copy_from_slice(e.as_bytes()));
        let data = payload_bytes(payload);
        let _ = self.nestor.insert(self.ns, location.as_ref(), etag, &data);
    }

    async fn meta(&self, location: &Path) -> Result<NestorMeta> {
        self.nestor
            .head(self.ns, location.as_ref())
            .await
            .map_err(|e| to_store(e, location.as_ref()))
    }
}

fn payload_bytes(payload: &PutPayload) -> Bytes {
    let mut chunks = payload.iter();
    match (chunks.next(), chunks.next()) {
        (None, _) => Bytes::new(),
        (Some(single), None) => single.clone(),
        (Some(first), Some(second)) => {
            let mut buf = BytesMut::with_capacity(payload.content_length());
            buf.extend_from_slice(first);
            buf.extend_from_slice(second);
            for chunk in chunks {
                buf.extend_from_slice(chunk);
            }
            buf.freeze()
        }
    }
}

fn to_store_meta(location: Path, meta: &NestorMeta) -> ObjectMeta {
    ObjectMeta {
        location,
        last_modified: meta
            .last_modified
            .map_or(DateTime::<Utc>::UNIX_EPOCH, DateTime::<Utc>::from),
        size: meta.size,
        e_tag: meta
            .etag
            .as_ref()
            .map(|e| String::from_utf8_lossy(e).into_owned()),
        version: None,
    }
}

fn read_range(range: Option<GetRange>) -> ReadRange {
    match range {
        None => ReadRange::Full,
        Some(GetRange::Bounded(r)) => ReadRange::Bounded(r),
        Some(GetRange::Offset(o)) => ReadRange::From(o),
        Some(GetRange::Suffix(n)) => ReadRange::Suffix(n),
    }
}

fn preconditions(options: &GetOptions) -> Preconditions {
    Preconditions {
        if_match: options.if_match.clone(),
        if_none_match: options.if_none_match.clone(),
        if_modified_since: options.if_modified_since.map(SystemTime::from),
        if_unmodified_since: options.if_unmodified_since.map(SystemTime::from),
    }
}

impl std::fmt::Display for NestorStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "NestorStore({})", self.inner)
    }
}

impl std::fmt::Debug for NestorStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NestorStore")
            .field("namespace", &self.ns)
            .field("inner", &self.inner)
            .field("populate", &self.populate)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl ObjectStore for NestorStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        let result = self.inner.put_opts(location, payload.clone(), opts).await?;
        self.invalidate(location);
        self.populate(location, &payload, &result);
        Ok(result)
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        let upload = self.inner.put_multipart_opts(location, opts).await?;
        Ok(Box::new(InvalidatingUpload {
            inner: upload,
            store: self.clone(),
            location: location.clone(),
        }))
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        let key = location.as_ref();
        let preconditions = preconditions(&options);
        let check = |meta: &NestorMeta| match preconditions.evaluate(meta) {
            Precondition::NotModified => Err(Error::NotModified {
                path: key.to_owned(),
                source: "preconditions not modified".into(),
            }),
            Precondition::Failed => Err(Error::Precondition {
                path: key.to_owned(),
                source: "preconditions failed".into(),
            }),
            Precondition::Satisfied => Ok(()),
        };

        if options.head {
            let meta = self.meta(location).await?;
            check(&meta)?;
            let size = meta.size;
            return Ok(GetResult {
                payload: GetResultPayload::Stream(futures::stream::empty().boxed()),
                meta: to_store_meta(location.clone(), &meta),
                range: 0..size,
                attributes: Attributes::default(),
                extensions: Extensions::default(),
            });
        }

        let mut stream = self
            .nestor
            .get(self.ns, key, read_range(options.range))
            .await
            .map_err(|e| to_store(e, key))?;
        let meta = stream.ready().await.map_err(|e| to_store(e, key))?;
        check(meta)?;
        let meta = to_store_meta(location.clone(), meta);
        let range = stream.range();
        let path = key.to_owned();
        Ok(GetResult {
            payload: GetResultPayload::Stream(stream.map_err(move |e| to_store(e, &path)).boxed()),
            meta,
            range,
            attributes: Attributes::default(),
            extensions: Extensions::default(),
        })
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        let key = location.as_ref();
        future::try_join_all(ranges.iter().map(|r| async move {
            self.nestor
                .read(self.ns, key, r.clone())
                .await
                .map_err(|e| to_store(e, key))
        }))
        .await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        let store = self.clone();
        self.inner
            .delete_stream(locations)
            .inspect_ok(move |path| store.invalidate(path))
            .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.inner.copy_opts(from, to, options).await?;
        self.invalidate(to);
        Ok(())
    }

    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> Result<()> {
        self.inner.rename_opts(from, to, options).await?;
        self.invalidate(from);
        self.invalidate(to);
        Ok(())
    }
}

#[derive(Debug)]
struct InvalidatingUpload {
    inner: Box<dyn MultipartUpload>,
    store: NestorStore,
    location: Path,
}

#[async_trait]
impl MultipartUpload for InvalidatingUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        self.inner.put_part(data)
    }

    async fn complete(&mut self) -> Result<PutResult> {
        let result = self.inner.complete().await?;
        self.store.invalidate(&self.location);
        Ok(result)
    }

    async fn abort(&mut self) -> Result<()> {
        self.inner.abort().await
    }
}
