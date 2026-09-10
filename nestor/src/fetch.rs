//! Origin fetch scheduling: grouped, deduplicated, bounded and driven by the read's `FetchPolicy`.

use std::collections::VecDeque;
use std::ops::Range;
use std::pin::pin;
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use futures::future::join_all;
use tokio::sync::Semaphore;

use crate::block::group_misses;
use crate::body::HedgedBody;
use crate::cache::BlockCache;
use crate::error::{NestorError, OriginError};
use crate::inflight::{Inflight, Registration, Slot, SlotHandle};
use crate::key::{Block, BlockKey, IMMUTABLE_TAG, ObjectKey, block_tag, content_tag};
use crate::meta::MetaCache;
use crate::namespace::NamespaceState;
use crate::origin::{GetOptions, GetResponse, ObjectMeta};
use crate::policy::{FetchPolicy, Retry};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Priority {
    Foreground,
    Background,
}

pub(crate) struct ReadCtx {
    pub namespace: Arc<NamespaceState>,
    pub key: ObjectKey,
    pub name: Arc<str>,
    pub tag: u64,
    pub if_match: Option<Bytes>,
    pub meta: Option<ObjectMeta>,
    pub policy: FetchPolicy,
}

impl ReadCtx {
    pub fn new(
        namespace: Arc<NamespaceState>,
        key: ObjectKey,
        name: Arc<str>,
        meta: Option<ObjectMeta>,
        policy: FetchPolicy,
    ) -> Arc<Self> {
        let consistency = namespace.config.consistency;
        let (tag, if_match) = match &meta {
            Some(m) => (
                block_tag(consistency, m),
                if consistency.is_immutable() {
                    None
                } else {
                    m.etag.clone()
                },
            ),
            None => (IMMUTABLE_TAG, None),
        };
        Arc::new(Self {
            namespace,
            key,
            name,
            tag,
            if_match,
            meta,
            policy,
        })
    }

    pub fn size(&self) -> Option<u64> {
        self.meta.as_ref().map(|m| m.size)
    }
}

pub(crate) struct Fetcher {
    pub(crate) cache: BlockCache,
    inflight: Arc<Inflight>,
    meta: Arc<MetaCache>,
    foreground: Semaphore,
    background: Semaphore,
    hedges: Semaphore,
}

impl Fetcher {
    pub fn new(
        cache: BlockCache,
        meta: Arc<MetaCache>,
        origin_concurrency: usize,
        readahead_concurrency: usize,
        hedge_concurrency: usize,
    ) -> Arc<Self> {
        Arc::new(Self {
            cache,
            inflight: Inflight::new(64),
            meta,
            foreground: Semaphore::new(origin_concurrency.max(1)),
            background: Semaphore::new(readahead_concurrency.max(1)),
            hedges: Semaphore::new(hedge_concurrency),
        })
    }

    pub async fn schedule(
        self: &Arc<Self>,
        ctx: &Arc<ReadCtx>,
        indexes: Range<u32>,
        priority: Priority,
    ) -> Vec<SlotHandle> {
        let lookups = indexes.map(|index| {
            let key = BlockKey::new(&ctx.key, ctx.tag, index);
            async move {
                let found = self.cache.get(&key).await;
                (key, found)
            }
        });
        let results = join_all(lookups).await;

        let metrics = &ctx.namespace.metrics;
        let mut handles = Vec::with_capacity(results.len());
        let mut owners: Vec<Slot> = Vec::new();
        for (key, found) in results {
            match found {
                Ok(Some(entry)) => {
                    metrics.hits.increment(1);
                    handles.push(SlotHandle::resolved(Ok(entry.value().clone())));
                }
                Ok(None) | Err(_) => match self.inflight.register(key) {
                    Registration::Owner(slot, handle) => {
                        owners.push(slot);
                        handles.push(handle);
                    }
                    Registration::Waiter(handle) => {
                        metrics.joined.increment(1);
                        handles.push(handle);
                    }
                },
            }
        }

        if owners.is_empty() {
            return handles;
        }
        let landed = join_all(owners.iter().map(|slot| self.cache.get(slot.key()))).await;
        let owners: Vec<Slot> = owners
            .into_iter()
            .zip(landed)
            .filter_map(|(slot, found)| {
                if let Ok(Some(entry)) = found {
                    metrics.hits.increment(1);
                    slot.resolve(Ok(entry.value().clone()));
                    None
                } else {
                    metrics.misses.increment(1);
                    Some(slot)
                }
            })
            .collect();
        if owners.is_empty() {
            return handles;
        }
        let groups = group_misses(
            owners.iter().map(|s| s.key().index),
            ctx.namespace.config.fetch_window,
        );
        let mut owners = owners.into_iter();
        for group in groups {
            let slots: VecDeque<Slot> = owners.by_ref().take(group.count as usize).collect();
            tokio::spawn(Arc::clone(self).fetch_group(Arc::clone(ctx), slots, priority, None));
        }
        handles
    }

    pub async fn head(
        &self,
        ns: &Arc<NamespaceState>,
        key: &ObjectKey,
        name: &str,
    ) -> Result<ObjectMeta, NestorError> {
        let mut retry = Retry::new(ns.config.fetch);
        loop {
            ns.metrics.meta_heads.increment(1);
            let started = Instant::now();
            let by = (started + retry.policy().first_byte).min(retry.attempt_deadline());
            let result = tokio::time::timeout_at(by.into(), ns.origin.head(name))
                .await
                .unwrap_or_else(|_| {
                    ns.metrics.origin_timeouts.increment(1);
                    Err(OriginError::Timeout(by - started))
                });
            match result {
                Ok(meta) => {
                    self.meta.put(key.clone(), meta.clone());
                    return Ok(meta);
                }
                Err(e) => {
                    ns.metrics.origin_errors.increment(1);
                    if let Err(e) = retry.failed(e).await {
                        self.meta.remove(key);
                        return Err(e.into());
                    }
                    ns.metrics.origin_retries.increment(1);
                }
            }
        }
    }

    pub async fn probe(
        self: &Arc<Self>,
        ns: &Arc<NamespaceState>,
        name: &Arc<str>,
        indexes: Range<u32>,
        stale: Option<ObjectMeta>,
        policy: FetchPolicy,
    ) -> Result<Arc<ReadCtx>, NestorError> {
        let key = ObjectKey::new(ns.id, name);
        let ctx = ReadCtx::new(Arc::clone(ns), key.clone(), Arc::clone(name), None, policy);
        let bs = ns.config.block_size;
        let options = GetOptions {
            range: Some(bs.span(indexes.start, indexes.end - indexes.start, None)),
            if_match: None,
            if_none_match: stale.as_ref().and_then(|m| m.etag.clone()),
        };
        let build = |meta: ObjectMeta| {
            self.meta.put(key.clone(), meta.clone());
            ReadCtx::new(
                Arc::clone(ns),
                key.clone(),
                Arc::clone(name),
                Some(meta),
                policy,
            )
        };
        let mut retry = Retry::new(policy);
        loop {
            let deadline = retry.attempt_deadline();
            match self.origin_get(&ctx, options.clone(), deadline).await {
                Ok(response) => {
                    let ctx = build(response.meta.clone());
                    self.adopt(&ctx, indexes, response);
                    return Ok(ctx);
                }
                Err(OriginError::NotModified) => {
                    let meta = stale.ok_or(NestorError::Origin(OriginError::NotModified))?;
                    return Ok(build(meta));
                }
                Err(OriginError::NotFound) => {
                    self.meta.remove(&key);
                    return Err(NestorError::NotFound);
                }
                Err(e) => {
                    ns.metrics.origin_errors.increment(1);
                    let start = bs.offset(indexes.start);
                    if let Some(meta) = self.past_end(ns, &key, name, start).await {
                        return Ok(build(meta));
                    }
                    retry.failed(e).await?;
                    ns.metrics.origin_retries.increment(1);
                }
            }
        }
    }

    async fn past_end(
        &self,
        ns: &Arc<NamespaceState>,
        key: &ObjectKey,
        name: &str,
        start: u64,
    ) -> Option<ObjectMeta> {
        let meta = self.head(ns, key, name).await.ok()?;
        (start >= meta.size).then_some(meta)
    }

    fn adopt(self: &Arc<Self>, ctx: &Arc<ReadCtx>, indexes: Range<u32>, response: GetResponse) {
        let end = indexes
            .end
            .min(ctx.namespace.config.block_size.count(response.meta.size));
        let slots: VecDeque<Slot> = (indexes.start..end)
            .map_while(|index| {
                match self
                    .inflight
                    .register(BlockKey::new(&ctx.key, ctx.tag, index))
                {
                    Registration::Owner(slot, _) => Some(slot),
                    Registration::Waiter(_) => None,
                }
            })
            .collect();
        if !slots.is_empty() {
            tokio::spawn(Arc::clone(self).fetch_group(
                Arc::clone(ctx),
                slots,
                Priority::Foreground,
                Some(response),
            ));
        }
    }

    async fn fetch_group(
        self: Arc<Self>,
        ctx: Arc<ReadCtx>,
        mut slots: VecDeque<Slot>,
        priority: Priority,
        mut initial: Option<GetResponse>,
    ) {
        let semaphore = match priority {
            Priority::Foreground => &self.foreground,
            Priority::Background => &self.background,
        };
        let Ok(_permit) = semaphore.acquire().await else {
            return;
        };

        let bs = ctx.namespace.config.block_size;
        let metrics = &ctx.namespace.metrics;
        let mut retry = Retry::new(ctx.policy);
        let mut meta = ctx.meta.clone();

        while let Some(front) = slots.front() {
            let attempt_deadline = retry.attempt_deadline();
            let range = bs.span(
                front.key().index,
                slots.len() as u32,
                meta.as_ref().map(|m| m.size),
            );
            if let Some(known) = &meta
                && range.is_empty()
            {
                resolve_all(slots, &Ok(Block::empty(known.clone())));
                return;
            }

            let result = if let Some(response) = initial.take() {
                Ok(response)
            } else {
                let options = GetOptions {
                    range: Some(range.clone()),
                    if_match: ctx.if_match.clone(),
                    if_none_match: None,
                };
                self.origin_get(&ctx, options, attempt_deadline).await
            };
            match result {
                Ok(response) => {
                    meta = Some(response.meta.clone());
                    let accepted = tokio::time::timeout_at(
                        attempt_deadline.into(),
                        self.accept(&ctx, response, &mut slots, &range),
                    )
                    .await
                    .unwrap_or_else(|_| {
                        metrics.origin_timeouts.increment(1);
                        Err(OriginError::Timeout(ctx.policy.attempt))
                    });
                    match accepted {
                        Ok(()) => return,
                        Err(e) => {
                            metrics.origin_errors.increment(1);
                            if !Self::retry_or_fail(&ctx, &mut retry, &mut slots, e).await {
                                return;
                            }
                        }
                    }
                }
                Err(OriginError::NotFound) => {
                    self.meta.remove(&ctx.key);
                    resolve_all(slots, &Err(Arc::new(NestorError::NotFound)));
                    return;
                }
                Err(OriginError::PreconditionFailed) => {
                    self.meta.remove(&ctx.key);
                    metrics.stale.increment(1);
                    resolve_all(slots, &Err(Arc::new(NestorError::Stale)));
                    return;
                }
                Err(e) => {
                    metrics.origin_errors.increment(1);
                    if meta.is_none()
                        && let Some(known) = self
                            .past_end(&ctx.namespace, &ctx.key, &ctx.name, range.start)
                            .await
                    {
                        resolve_all(slots, &Ok(Block::empty(known)));
                        return;
                    }
                    if !Self::retry_or_fail(&ctx, &mut retry, &mut slots, e).await {
                        return;
                    }
                }
            }
        }
    }

    async fn accept(
        &self,
        ctx: &ReadCtx,
        response: GetResponse,
        slots: &mut VecDeque<Slot>,
        range: &Range<u64>,
    ) -> Result<(), OriginError> {
        self.meta.put(ctx.key.clone(), response.meta.clone());
        if ctx.tag != IMMUTABLE_TAG
            && content_tag(response.meta.etag.as_ref(), response.meta.size) != ctx.tag
        {
            ctx.namespace.metrics.stale.increment(1);
            resolve_all(std::mem::take(slots), &Err(Arc::new(NestorError::Stale)));
            return Ok(());
        }
        if response.range.start != range.start {
            return Err(OriginError::ShortRead {
                expected: range.end - range.start,
                got: 0,
            });
        }
        self.consume(ctx, response, slots, range.end).await
    }

    async fn retry_or_fail(
        ctx: &ReadCtx,
        retry: &mut Retry,
        slots: &mut VecDeque<Slot>,
        error: OriginError,
    ) -> bool {
        match retry.failed(error).await {
            Ok(()) => {
                ctx.namespace.metrics.origin_retries.increment(1);
                true
            }
            Err(error) => {
                resolve_all(std::mem::take(slots), &Err(Arc::new(error.into())));
                false
            }
        }
    }

    async fn origin_get(
        &self,
        ctx: &ReadCtx,
        options: GetOptions,
        attempt_deadline: Instant,
    ) -> Result<GetResponse, OriginError> {
        let metrics = &ctx.namespace.metrics;
        metrics.origin_requests.increment(1);
        let started = Instant::now();
        let headers_by = (started + ctx.policy.first_byte).min(attempt_deadline);
        tokio::time::timeout_at(headers_by.into(), self.get_hedged(ctx, options))
            .await
            .unwrap_or_else(|_| {
                metrics.origin_timeouts.increment(1);
                Err(OriginError::Timeout(headers_by - started))
            })
    }

    async fn get_hedged(
        &self,
        ctx: &ReadCtx,
        options: GetOptions,
    ) -> Result<GetResponse, OriginError> {
        let ns = &ctx.namespace;
        let started = Instant::now();
        let mut primary = pin!(ns.origin.get(&ctx.name, options.clone()));
        let Some(hedge) = &ctx.policy.hedge else {
            return observe_ttfb(ns, started, primary.await);
        };
        let delay = hedge.delay(&ns.ttfb);
        ns.metrics.hedge_headers.delay.set(delay.as_secs_f64());
        tokio::select! {
            result = &mut primary => observe_ttfb(ns, started, result),
            () = tokio::time::sleep(delay) => {
                let Ok(_permit) = self.hedges.try_acquire() else {
                    return observe_ttfb(ns, started, primary.await);
                };
                ns.metrics.hedge_headers.issued.increment(1);
                let hedged_at = Instant::now();
                let mut secondary = pin!(ns.origin.get(&ctx.name, options));
                tokio::select! {
                    result = &mut primary => match result {
                        Err(e) if e.is_retryable() => {
                            ns.metrics.hedge_headers.wins.increment(1);
                            observe_ttfb(ns, hedged_at, secondary.await)
                        }
                        result => observe_ttfb(ns, started, result),
                    },
                    result = &mut secondary => match result {
                        Err(e) if e.is_retryable() => observe_ttfb(ns, started, primary.await),
                        result => {
                            ns.metrics.hedge_headers.wins.increment(1);
                            observe_ttfb(ns, hedged_at, result)
                        }
                    },
                }
            }
        }
    }

    async fn consume(
        &self,
        ctx: &ReadCtx,
        response: GetResponse,
        slots: &mut VecDeque<Slot>,
        range_end: u64,
    ) -> Result<(), OriginError> {
        let ns = &ctx.namespace;
        let bs = ns.config.block_size;
        let meta = response.meta;
        let size = Some(meta.size);
        let mut body = HedgedBody::new(ctx, &self.hedges, response.body, range_end);

        while let Some(front) = slots.front() {
            let index = front.key().index;
            let expected = bs.block_range(index, size).count();
            if expected == 0 {
                let slot = slots.pop_front().expect("front exists");
                slot.resolve(Ok(Block::empty(meta.clone())));
                continue;
            }
            let block = body.next_block(index, expected).await?;
            let lap = body.lap();
            ns.block.observe(lap);
            ns.metrics.origin_block.record(lap.as_secs_f64());
            let slot = slots.pop_front().expect("front exists");
            ns.metrics.origin_bytes.increment(block.len() as u64);
            let block = Block::new(meta.clone(), block);
            self.cache.insert(slot.key().clone(), block.clone());
            slot.resolve(Ok(block));
        }
        Ok(())
    }
}

fn observe_ttfb(
    ns: &NamespaceState,
    started: Instant,
    result: Result<GetResponse, OriginError>,
) -> Result<GetResponse, OriginError> {
    if result.is_ok() {
        let ttfb = started.elapsed();
        ns.ttfb.observe(ttfb);
        ns.metrics.origin_ttfb.record(ttfb.as_secs_f64());
    }
    result
}

fn resolve_all(slots: VecDeque<Slot>, result: &Result<Block, Arc<NestorError>>) {
    for slot in slots {
        slot.resolve(result.clone());
    }
}
