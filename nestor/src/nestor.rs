//! Engine entry point. `NestorBuilder` assembles the cache and namespaces, `Nestor` exposes head,
//! get, read, insert and invalidate.

use std::collections::HashMap;
use std::ops::Range;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};

use bytes::Bytes;
use mixtrics::metrics::BoxedRegistry;

use crate::block::ReadRange;
use crate::cache::{self, CacheConfig};
use crate::error::{NestorError, Result};
use crate::fetch::{Fetcher, Priority, ReadCtx};
use crate::key::{Block, BlockKey, IMMUTABLE_TAG, NamespaceId, ObjectKey, block_tag};
use crate::meta::{MetaCache, MetaLookup};
use crate::namespace::{Namespace, NamespaceState};
use crate::origin::ObjectMeta;
use crate::policy::{FetchOverrides, FetchPolicy};
use crate::readahead::Readahead;
use crate::reader::{ReadStream, Reader};

#[derive(Default)]
struct Registry {
    by_name: HashMap<Arc<str>, NamespaceId>,
    states: Vec<Arc<NamespaceState>>,
}

pub(crate) struct Engine {
    registry: RwLock<Registry>,
    pub(crate) fetcher: Arc<Fetcher>,
    pub(crate) meta: Arc<MetaCache>,
    readahead: Readahead,
    closed: AtomicBool,
}

impl Engine {
    fn namespace(&self, id: NamespaceId) -> Result<Arc<NamespaceState>> {
        self.registry
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .states
            .get(id.0 as usize)
            .cloned()
            .ok_or(NestorError::UnknownNamespace)
    }

    pub(crate) async fn resolve_ctx(
        &self,
        ns: &Arc<NamespaceState>,
        name: &Arc<str>,
        request: &ReadRange,
        policy: FetchPolicy,
    ) -> Result<Arc<ReadCtx>> {
        let key = ObjectKey::new(ns.id, name);
        let stale = match self.meta.lookup(&key, ns.meta_ttl()) {
            MetaLookup::Fresh(meta) => {
                return Ok(ReadCtx::new(
                    Arc::clone(ns),
                    key,
                    Arc::clone(name),
                    Some(meta),
                    policy,
                ));
            }
            MetaLookup::Stale(meta) => Some(meta),
            MetaLookup::Missing => None,
        };
        if ns.config.consistency.is_immutable() && !request.needs_size() {
            return Ok(ReadCtx::new(
                Arc::clone(ns),
                key,
                Arc::clone(name),
                None,
                policy,
            ));
        }
        let window = ns.config.fetch_window;
        if let Some(indexes) = ns.config.block_size.first_group(request, window) {
            return self.fetcher.probe(ns, name, indexes, stale, policy).await;
        }
        let meta = self.fetcher.head(ns, &key, name).await?;
        Ok(ReadCtx::new(
            Arc::clone(ns),
            key,
            Arc::clone(name),
            Some(meta),
            policy,
        ))
    }
}

#[derive(Clone)]
pub struct Nestor {
    engine: Arc<Engine>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReadOptions {
    pub range: ReadRange,
    pub fetch: FetchOverrides,
}

impl ReadOptions {
    pub fn range(range: impl Into<ReadRange>) -> Self {
        Self {
            range: range.into(),
            fetch: FetchOverrides::default(),
        }
    }

    pub fn fetch(mut self, overrides: FetchOverrides) -> Self {
        self.fetch = overrides;
        self
    }
}

pub struct NestorBuilder {
    cache: CacheConfig,
    namespaces: Vec<Namespace>,
    origin_concurrency: usize,
    readahead_concurrency: usize,
    hedge_concurrency: usize,
    meta_capacity: usize,
    metrics: Option<BoxedRegistry>,
}

impl NestorBuilder {
    pub fn new(cache: CacheConfig) -> Self {
        Self {
            cache,
            namespaces: Vec::new(),
            origin_concurrency: 64,
            readahead_concurrency: 16,
            hedge_concurrency: 16,
            meta_capacity: 100_000,
            metrics: None,
        }
    }

    pub fn namespace(mut self, namespace: Namespace) -> Self {
        self.namespaces.push(namespace);
        self
    }

    pub fn origin_concurrency(mut self, permits: usize) -> Self {
        self.origin_concurrency = permits;
        self
    }

    pub fn readahead_concurrency(mut self, permits: usize) -> Self {
        self.readahead_concurrency = permits;
        self
    }

    pub fn hedge_concurrency(mut self, permits: usize) -> Self {
        self.hedge_concurrency = permits;
        self
    }

    pub fn meta_capacity(mut self, entries: usize) -> Self {
        self.meta_capacity = entries;
        self
    }

    pub fn metrics_registry(mut self, registry: BoxedRegistry) -> Self {
        self.metrics = Some(registry);
        self
    }

    pub async fn build(self) -> Result<Nestor> {
        let cache = cache::build(&self.cache, self.metrics).await?;
        let meta = Arc::new(MetaCache::new(self.meta_capacity));
        let fetcher = Fetcher::new(
            cache,
            Arc::clone(&meta),
            self.origin_concurrency,
            self.readahead_concurrency,
            self.hedge_concurrency,
        );
        let engine = Arc::new(Engine {
            registry: RwLock::new(Registry::default()),
            fetcher,
            meta,
            readahead: Readahead::new(self.meta_capacity),
            closed: AtomicBool::new(false),
        });
        let nestor = Nestor { engine };
        for ns in self.namespaces {
            nestor.register(ns);
        }
        Ok(nestor)
    }
}

impl Nestor {
    pub fn builder(cache: CacheConfig) -> NestorBuilder {
        NestorBuilder::new(cache)
    }

    pub fn register(&self, namespace: Namespace) -> NamespaceId {
        let mut registry = self
            .engine
            .registry
            .write()
            .unwrap_or_else(|e| e.into_inner());
        if let Some(id) = registry.by_name.get(&namespace.name) {
            return *id;
        }
        let id = NamespaceId(registry.states.len() as u32);
        registry.by_name.insert(Arc::clone(&namespace.name), id);
        registry
            .states
            .push(Arc::new(NamespaceState::new(id, namespace)));
        id
    }

    pub fn namespace(&self, name: &str) -> Option<NamespaceId> {
        self.engine
            .registry
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .by_name
            .get(name)
            .copied()
    }

    pub fn namespaces(&self) -> Vec<(Arc<str>, NamespaceId)> {
        let registry = self
            .engine
            .registry
            .read()
            .unwrap_or_else(|e| e.into_inner());
        registry
            .states
            .iter()
            .map(|s| (Arc::clone(&s.name), s.id))
            .collect()
    }

    fn state(&self, ns: NamespaceId) -> Result<Arc<NamespaceState>> {
        if self.engine.closed.load(Ordering::Acquire) {
            return Err(NestorError::Closed);
        }
        self.engine.namespace(ns)
    }

    pub async fn head(&self, ns: NamespaceId, object: &str) -> Result<ObjectMeta> {
        let state = self.state(ns)?;
        let key = ObjectKey::new(ns, object);
        if let Some(meta) = self.engine.meta.get(&key, state.meta_ttl()) {
            return Ok(meta);
        }
        self.engine.fetcher.head(&state, &key, object).await
    }

    pub async fn get(
        &self,
        ns: NamespaceId,
        object: &str,
        range: impl Into<ReadRange>,
    ) -> Result<ReadStream> {
        self.get_opts(ns, object, ReadOptions::range(range)).await
    }

    pub async fn get_opts(
        &self,
        ns: NamespaceId,
        object: &str,
        options: ReadOptions,
    ) -> Result<ReadStream> {
        let state = self.state(ns)?;
        let ReadOptions {
            range: request,
            fetch,
        } = options;
        let policy = state.config.fetch.with(&fetch);
        let name: Arc<str> = Arc::from(object);
        let ctx = self
            .engine
            .resolve_ctx(&state, &name, &request, policy)
            .await?;
        let range = match (ctx.size(), &request) {
            (Some(size), _) => request.resolve(size)?,
            (None, ReadRange::Bounded(r)) => {
                if r.start >= r.end {
                    return Err(NestorError::Range(r.clone(), 0));
                }
                r.clone()
            }
            (None, _) => unreachable!("size is resolved for unbounded requests"),
        };

        if state.config.readahead > 0 && self.engine.readahead.observe(&ctx.key, &range) {
            self.spawn_readahead(&state, &ctx, &range);
        }

        Ok(Reader::new(Arc::clone(&self.engine), request, ctx, range).into_stream())
    }

    fn spawn_readahead(&self, state: &NamespaceState, ctx: &Arc<ReadCtx>, range: &Range<u64>) {
        let bs = state.config.block_size;
        let start = bs.count(range.end);
        let mut end = start + state.config.readahead;
        if let Some(size) = ctx.size() {
            end = end.min(bs.count(size));
        }
        if start >= end {
            return;
        }
        state
            .metrics
            .readahead_blocks
            .increment(u64::from(end - start));
        let fetcher = Arc::clone(&self.engine.fetcher);
        let ctx = Arc::clone(ctx);
        tokio::spawn(async move {
            drop(
                fetcher
                    .schedule(&ctx, start..end, Priority::Background)
                    .await,
            );
        });
    }

    pub async fn read(&self, ns: NamespaceId, object: &str, range: Range<u64>) -> Result<Bytes> {
        self.get(ns, object, range).await?.collect().await
    }

    pub fn insert(
        &self,
        ns: NamespaceId,
        object: &str,
        etag: Option<Bytes>,
        data: &Bytes,
    ) -> Result<()> {
        let state = self.state(ns)?;
        let meta = ObjectMeta::new(data.len() as u64, etag);
        let key = ObjectKey::new(ns, object);
        let tag = block_tag(state.config.consistency, &meta);
        let bs = state.config.block_size;
        let cache = &self.engine.fetcher.cache;
        for index in 0..bs.count(meta.size) {
            let range = bs.block_range(index, Some(meta.size));
            let block = data.slice(range.start as usize..range.end as usize);
            cache.insert(
                BlockKey::new(&key, tag, index),
                Block::new(meta.clone(), block),
            );
        }
        self.engine.meta.put(key, meta);
        Ok(())
    }

    pub fn invalidate(&self, ns: NamespaceId, object: &str) -> Result<()> {
        let state = self.state(ns)?;
        let key = ObjectKey::new(ns, object);
        self.engine.readahead.forget(&key);
        if state.config.consistency.is_immutable()
            && let Some(meta) = self.engine.meta.get(&key, None)
        {
            let bs = state.config.block_size;
            for index in 0..bs.count(meta.size) {
                self.engine
                    .fetcher
                    .cache
                    .remove(&BlockKey::new(&key, IMMUTABLE_TAG, index));
            }
        }
        self.engine.meta.remove(&key);
        Ok(())
    }

    pub async fn close(&self) -> Result<()> {
        if self.engine.closed.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        self.engine.fetcher.cache.close().await?;
        Ok(())
    }
}

impl std::fmt::Debug for Nestor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Nestor")
            .field("namespaces", &self.namespaces().len())
            .finish_non_exhaustive()
    }
}
