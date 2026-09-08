//! Origin fetch scheduling. Misses are grouped into contiguous GETs, deduplicated through
//! `Inflight`, bounded by semaphores, retried with backoff and hedged when a request runs slower
//! than the namespace's latency estimate.

use std::collections::VecDeque;
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use futures::StreamExt;
use futures::future::join_all;
use tokio::sync::Semaphore;

use crate::block::group_misses;
use crate::cache::BlockCache;
use crate::error::{NestorError, OriginError};
use crate::inflight::{Inflight, Registration, Slot, SlotHandle};
use crate::key::{BlockKey, IMMUTABLE_TAG, ObjectKey, block_tag, content_tag};
use crate::meta::MetaCache;
use crate::namespace::NamespaceState;
use crate::origin::{GetOptions, GetResponse, ObjectMeta};

#[derive(Debug, Clone, Copy, PartialEq)]
#[cfg_attr(
    feature = "serde",
    derive(serde::Deserialize),
    serde(deny_unknown_fields, default)
)]
/// A hedge fires after `factor` times the observed time to first byte, clamped to `min..max`. With
/// no observation yet it fires after `max`.
pub struct HedgeConfig {
    pub factor: f64,
    #[cfg_attr(feature = "serde", serde(with = "humantime_serde"))]
    pub min: Duration,
    #[cfg_attr(feature = "serde", serde(with = "humantime_serde"))]
    pub max: Duration,
}

impl Default for HedgeConfig {
    fn default() -> Self {
        Self {
            factor: 3.0,
            min: Duration::from_millis(50),
            max: Duration::from_secs(2),
        }
    }
}

impl HedgeConfig {
    pub fn delay(&self, observed: Option<Duration>) -> Duration {
        match observed {
            None => self.max,
            Some(ttfb) => ttfb.mul_f64(self.factor).clamp(self.min, self.max),
        }
    }
}

const ALPHA_SHIFT: u32 = 3;

#[derive(Debug, Default)]
/// EWMA of origin time to first byte, `ALPHA_SHIFT` gives an alpha of 1/8.
pub struct Latency {
    ewma_nanos: AtomicU64,
}

impl Latency {
    pub fn observe(&self, sample: Duration) {
        let sample = sample.as_nanos().min(u128::from(u64::MAX)) as u64;
        let mut current = self.ewma_nanos.load(Ordering::Relaxed);
        loop {
            let next = if current == 0 {
                sample
            } else {
                current - (current >> ALPHA_SHIFT) + (sample >> ALPHA_SHIFT)
            };
            match self.ewma_nanos.compare_exchange_weak(
                current,
                next.max(1),
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return,
                Err(actual) => current = actual,
            }
        }
    }

    pub fn get(&self) -> Option<Duration> {
        match self.ewma_nanos.load(Ordering::Relaxed) {
            0 => None,
            nanos => Some(Duration::from_nanos(nanos)),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(
    feature = "serde",
    derive(serde::Deserialize),
    serde(deny_unknown_fields, default)
)]
pub struct RetryConfig {
    pub attempts: u32,
    #[cfg_attr(feature = "serde", serde(with = "humantime_serde"))]
    pub base: Duration,
    #[cfg_attr(feature = "serde", serde(with = "humantime_serde"))]
    pub max: Duration,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            attempts: 3,
            base: Duration::from_millis(50),
            max: Duration::from_secs(2),
        }
    }
}

impl RetryConfig {
    fn backoff(&self, attempt: u32) -> Duration {
        self.base
            .saturating_mul(1u32 << attempt.min(16))
            .min(self.max)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Foreground reads and background readahead draw from separate semaphores.
pub(crate) enum Priority {
    Foreground,
    Background,
}

/// What a fetch needs to know about the object being read, fixed for the life of one read.
pub(crate) struct ReadCtx {
    pub namespace: Arc<NamespaceState>,
    pub key: ObjectKey,
    pub name: Arc<str>,
    pub tag: u64,
    /// Sent with origin GETs so a changed object fails fast instead of mixing generations.
    pub if_match: Option<Bytes>,
    /// Unknown only for bounded reads in immutable namespaces, learned from the first response.
    pub meta: Option<ObjectMeta>,
}

impl ReadCtx {
    pub fn new(
        namespace: Arc<NamespaceState>,
        key: ObjectKey,
        name: Arc<str>,
        meta: Option<ObjectMeta>,
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
    retry: RetryConfig,
}

impl Fetcher {
    pub fn new(
        cache: BlockCache,
        meta: Arc<MetaCache>,
        origin_concurrency: usize,
        readahead_concurrency: usize,
        hedge_concurrency: usize,
        retry: RetryConfig,
    ) -> Arc<Self> {
        Arc::new(Self {
            cache,
            inflight: Inflight::new(64),
            meta,
            foreground: Semaphore::new(origin_concurrency.max(1)),
            background: Semaphore::new(readahead_concurrency.max(1)),
            hedges: Semaphore::new(hedge_concurrency),
            retry,
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
                        metrics.misses.increment(1);
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
        ns.metrics.meta_heads.increment(1);
        match ns.origin.head(name).await {
            Ok(meta) => {
                self.meta.put(key.clone(), meta.clone());
                Ok(meta)
            }
            Err(e) => {
                self.meta.remove(key);
                Err(e.into())
            }
        }
    }

    /// Learns an object's metadata from the GET that a read needs anyway instead of a HEAD. The
    /// response fills the blocks it covers, a stale `ETag` makes the request conditional so an
    /// unchanged object costs no body.
    pub async fn probe(
        self: &Arc<Self>,
        ns: &Arc<NamespaceState>,
        name: &Arc<str>,
        indexes: Range<u32>,
        stale: Option<ObjectMeta>,
    ) -> Result<Arc<ReadCtx>, NestorError> {
        let key = ObjectKey::new(ns.id, name);
        let ctx = ReadCtx::new(Arc::clone(ns), key.clone(), Arc::clone(name), None);
        let bs = ns.config.block_size;
        let options = GetOptions {
            range: Some(bs.span(indexes.start, indexes.end - indexes.start, None)),
            if_match: None,
            if_none_match: stale.as_ref().and_then(|m| m.etag.clone()),
        };
        let build = |meta: ObjectMeta| {
            self.meta.put(key.clone(), meta.clone());
            ReadCtx::new(Arc::clone(ns), key.clone(), Arc::clone(name), Some(meta))
        };
        match self.origin_get(&ctx, options).await {
            Ok(response) => {
                let ctx = build(response.meta.clone());
                self.adopt(&ctx, indexes, response);
                Ok(ctx)
            }
            Err(OriginError::NotModified) => {
                let meta = stale.ok_or(NestorError::Origin(OriginError::NotModified))?;
                Ok(build(meta))
            }
            Err(OriginError::InvalidRange) => Ok(build(self.head(ns, &key, name).await?)),
            Err(OriginError::NotFound) => {
                self.meta.remove(&key);
                Err(NestorError::NotFound)
            }
            Err(e) => Err(e.into()),
        }
    }

    /// Hands a probe response to the fetch pipeline for the leading blocks nobody else is already
    /// fetching. Blocks past the first foreign owner are left to that owner.
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
        let mut size = ctx.size();
        let mut attempt = 0u32;

        while let Some(front) = slots.front() {
            let range = bs.span(front.key().index, slots.len() as u32, size);
            if range.is_empty() {
                resolve_all(slots, &Ok(Bytes::new()));
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
                self.origin_get(&ctx, options).await
            };
            match result {
                Ok(response) => {
                    size = Some(response.meta.size);
                    match self.accept(&ctx, response, &mut slots, &range).await {
                        Ok(()) => return,
                        Err(e) => {
                            metrics.origin_errors.increment(1);
                            if !self.retry_or_fail(&ctx, &mut attempt, &mut slots, e).await {
                                return;
                            }
                        }
                    }
                }
                Err(OriginError::InvalidRange) => {
                    match ctx.namespace.origin.head(&ctx.name).await {
                        Ok(meta) => {
                            self.meta.put(ctx.key.clone(), meta);
                            resolve_all(slots, &Ok(Bytes::new()));
                        }
                        Err(e) => {
                            metrics.origin_errors.increment(1);
                            resolve_all(slots, &Err(Arc::new(e.into())));
                        }
                    }
                    return;
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
                    if !self.retry_or_fail(&ctx, &mut attempt, &mut slots, e).await {
                        return;
                    }
                }
            }
        }
    }

    /// Checks a response against the read's generation and alignment, then streams it into the
    /// slots. A stale generation fails the slots without retry, a misaligned start is retryable.
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
        let size = Some(response.meta.size);
        self.consume(ctx, response, slots, size).await
    }

    async fn retry_or_fail(
        &self,
        ctx: &ReadCtx,
        attempt: &mut u32,
        slots: &mut VecDeque<Slot>,
        error: OriginError,
    ) -> bool {
        if error.is_retryable() && *attempt < self.retry.attempts {
            let delay = self.retry.backoff(*attempt);
            *attempt += 1;
            ctx.namespace.metrics.origin_retries.increment(1);
            tokio::time::sleep(delay).await;
            return true;
        }
        resolve_all(std::mem::take(slots), &Err(Arc::new(error.into())));
        false
    }

    /// One origin GET with hedging, counted and timed for the namespace.
    async fn origin_get(
        &self,
        ctx: &ReadCtx,
        options: GetOptions,
    ) -> Result<GetResponse, OriginError> {
        let metrics = &ctx.namespace.metrics;
        metrics.origin_requests.increment(1);
        let started = Instant::now();
        let result = self.get_hedged(ctx, options).await;
        if result.is_ok() {
            let ttfb = started.elapsed();
            ctx.namespace.latency.observe(ttfb);
            metrics.origin_ttfb.record(ttfb.as_secs_f64());
        }
        result
    }

    async fn get_hedged(
        &self,
        ctx: &ReadCtx,
        options: GetOptions,
    ) -> Result<GetResponse, OriginError> {
        let origin = &ctx.namespace.origin;
        let mut primary = origin.get(&ctx.name, options.clone());
        let Some(hedge) = &ctx.namespace.config.hedge else {
            return primary.await;
        };
        let delay = hedge.delay(ctx.namespace.latency.get());
        tokio::select! {
            result = &mut primary => result,
            () = tokio::time::sleep(delay) => {
                let Ok(_permit) = self.hedges.try_acquire() else {
                    return primary.await;
                };
                ctx.namespace.metrics.hedges.increment(1);
                let secondary = origin.get(&ctx.name, options);
                tokio::select! {
                    result = &mut primary => result,
                    result = secondary => {
                        ctx.namespace.metrics.hedge_wins.increment(1);
                        result
                    }
                }
            }
        }
    }

    async fn consume(
        &self,
        ctx: &ReadCtx,
        response: GetResponse,
        slots: &mut VecDeque<Slot>,
        size: Option<u64>,
    ) -> Result<(), OriginError> {
        let bs = ctx.namespace.config.block_size;
        let metrics = &ctx.namespace.metrics;
        let mut body = response.body;
        let mut buf = BytesMut::new();

        while let Some(front) = slots.front() {
            let expected = bs.block_range(front.key().index, size).count();
            if expected == 0 {
                let slot = slots.pop_front().expect("front exists");
                slot.resolve(Ok(Bytes::new()));
                continue;
            }
            let Some(chunk) = body.next().await else {
                return Err(OriginError::ShortRead {
                    expected: expected as u64,
                    got: buf.len() as u64,
                });
            };
            let mut chunk = chunk?;
            while !chunk.is_empty() {
                let Some(front) = slots.front() else {
                    return Ok(());
                };
                let expected = bs.block_range(front.key().index, size).count();
                let need = expected - buf.len();
                let block = if buf.is_empty() && chunk.len() >= need {
                    chunk.split_to(need)
                } else {
                    let take = need.min(chunk.len());
                    if buf.capacity() < expected {
                        buf.reserve(expected - buf.capacity());
                    }
                    buf.extend_from_slice(&chunk.split_to(take));
                    if buf.len() < expected {
                        continue;
                    }
                    buf.split().freeze()
                };
                let slot = slots.pop_front().expect("front exists");
                metrics.origin_bytes.increment(block.len() as u64);
                self.cache.insert(slot.key().clone(), block.clone());
                slot.resolve(Ok(block));
            }
        }
        Ok(())
    }
}

fn resolve_all(slots: VecDeque<Slot>, result: &Result<Bytes, Arc<NestorError>>) {
    for slot in slots {
        slot.resolve(result.clone());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ewma_converges() {
        let l = Latency::default();
        for _ in 0..64 {
            l.observe(Duration::from_millis(40));
        }
        let v = l.get().unwrap();
        assert!(v >= Duration::from_millis(39) && v <= Duration::from_millis(41));
    }

    #[test]
    fn delay_is_clamped() {
        let cfg = HedgeConfig::default();
        assert_eq!(cfg.delay(None), cfg.max);
        assert_eq!(cfg.delay(Some(Duration::from_millis(1))), cfg.min);
        assert_eq!(cfg.delay(Some(Duration::from_secs(10))), cfg.max);
        assert_eq!(
            cfg.delay(Some(Duration::from_millis(100))),
            Duration::from_millis(300)
        );
    }
}
