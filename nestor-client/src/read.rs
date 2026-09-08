//! Routed requests. A range is split into cluster blocks, each block goes to its owner with a hedge
//! against the next ranked node, and the block bodies are chained back in order.

use std::future::Future;
use std::ops::Range;
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
use nestor::{GetOptions, GetResponse, ObjectMeta, Origin, OriginError};

use crate::cluster::Cluster;
use crate::error::ClusterError;
use crate::node::Load;
use crate::router::{Ranked, object_hash};

impl Cluster {
    pub(crate) async fn get(
        self: &Arc<Self>,
        bucket: &Arc<str>,
        key: &Arc<str>,
        options: GetOptions,
    ) -> Result<GetResponse, OriginError> {
        let block_size = self.config().block_size;
        let object = object_hash(bucket, key);
        let start = options.range.as_ref().map_or(0, |r| r.start);
        let limit = options.range.as_ref().map_or(u64::MAX, |r| r.end);
        if start >= limit {
            return Err(OriginError::InvalidRange);
        }

        let first_index = block_size.index(start);
        let first_range = start..limit.min(block_size.offset(first_index + 1));
        let first = self
            .block(
                bucket,
                key,
                object,
                first_index,
                first_range,
                options.if_match.clone(),
            )
            .await?;
        let end = limit.min(first.meta.size);
        let last_index = block_size.index(end.saturating_sub(1)).max(first_index);
        let if_match = options.if_match.or_else(|| first.meta.etag.clone());

        let cluster = Arc::clone(self);
        let bucket = Arc::clone(bucket);
        let key = Arc::clone(key);
        let rest = futures::stream::iter(first_index + 1..=last_index)
            .map(move |index| {
                let cluster = Arc::clone(&cluster);
                let bucket = Arc::clone(&bucket);
                let key = Arc::clone(&key);
                let if_match = if_match.clone();
                let range = block_size.block_range(index, Some(end));
                async move {
                    cluster
                        .block(&bucket, &key, object, index, range, if_match)
                        .await
                }
            })
            .buffered(self.config().read_window.max(1) as usize)
            .map_ok(|response| response.body)
            .try_flatten();

        Ok(GetResponse {
            meta: first.meta,
            range: start..end,
            body: Box::pin(first.body.chain(rest)),
        })
    }

    pub(crate) async fn head(
        &self,
        bucket: &Arc<str>,
        key: &str,
    ) -> Result<ObjectMeta, OriginError> {
        let ranked = self.rank(object_hash(bucket, key), 0);
        self.hedged(
            &ranked,
            bucket,
            |origin| async move { origin.head(key).await },
        )
        .await
    }

    pub async fn warm(
        self: &Arc<Self>,
        bucket: &Arc<str>,
        key: &Arc<str>,
        size: u64,
    ) -> Result<(), OriginError> {
        let block_size = self.config().block_size;
        let object = object_hash(bucket, key);
        futures::stream::iter(0..block_size.count(size))
            .map(|index| {
                let range = block_size.block_range(index, Some(size));
                async move { self.block(bucket, key, object, index, range, None).await }
            })
            .buffered(self.config().read_window.max(1) as usize)
            .try_for_each(|response| async move {
                response.body.try_for_each(|_| async { Ok(()) }).await
            })
            .await
    }

    async fn block(
        &self,
        bucket: &Arc<str>,
        key: &str,
        object: u64,
        index: u32,
        range: Range<u64>,
        if_match: Option<Bytes>,
    ) -> Result<GetResponse, OriginError> {
        let ranked = self.rank(object, index);
        let options = GetOptions {
            range: Some(range),
            if_match,
        };
        self.hedged(&ranked, bucket, move |origin| {
            let options = options.clone();
            async move { origin.get(key, options).await }
        })
        .await
    }

    async fn hedged<T, F, Fut>(
        &self,
        ranked: &Ranked,
        bucket: &Arc<str>,
        request: F,
    ) -> Result<T, OriginError>
    where
        F: Fn(Arc<dyn Origin>) -> Fut,
        Fut: Future<Output = Result<T, OriginError>>,
    {
        let Some(primary) = ranked.primary() else {
            return Err(ClusterError::NoNodes.into());
        };
        let primary_node = Arc::clone(primary.node());
        let mut first = std::pin::pin!(self.attempt(primary, bucket, &request));
        let result = match &self.config().hedge {
            None => first.await,
            Some(hedge) => {
                let delay = hedge.delay(primary_node.latency().get());
                tokio::select! {
                    result = &mut first => result,
                    () = tokio::time::sleep(delay) => match ranked.secondary(&primary_node) {
                        None => first.await,
                        Some(secondary) => {
                            let second = self.attempt(secondary, bucket, &request);
                            tokio::select! {
                                result = &mut first => result,
                                result = second => result,
                            }
                        }
                    },
                }
            }
        };
        match result {
            Err(OriginError::Io(_)) => match ranked.secondary(&primary_node) {
                Some(failover) => self.attempt(failover, bucket, &request).await,
                None => result,
            },
            other => other,
        }
    }

    async fn attempt<T, F, Fut>(
        &self,
        load: Load,
        bucket: &Arc<str>,
        request: &F,
    ) -> Result<T, OriginError>
    where
        F: Fn(Arc<dyn Origin>) -> Fut,
        Fut: Future<Output = Result<T, OriginError>>,
    {
        let origin = load.node().origin(bucket)?;
        let started = Instant::now();
        let result = request(origin).await;
        match &result {
            Ok(_) => load.node().latency().observe(started.elapsed()),
            Err(OriginError::Io(_)) => load.node().mark_down(self.config().down_for),
            Err(_) => {}
        }
        result
    }
}
