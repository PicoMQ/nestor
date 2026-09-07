//! Streaming reads. `Reader` walks the blocks covering a range with a bounded window in flight and
//! slices each block to the requested bytes. `ReadStream` is the public handle.

use std::collections::VecDeque;
use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::{Bytes, BytesMut};
use futures::stream::{BoxStream, Stream, StreamExt};

use crate::block::{ReadRange, aligned_take};
use crate::error::{NestorError, Result};
use crate::fetch::{Priority, ReadCtx};
use crate::inflight::SlotHandle;
use crate::nestor::Engine;
use crate::origin::ObjectMeta;

pub struct ReadStream {
    stream: BoxStream<'static, Result<Bytes>>,
    range: Range<u64>,
    size: Option<u64>,
    meta: Option<ObjectMeta>,
}

impl ReadStream {
    pub fn range(&self) -> &Range<u64> {
        &self.range
    }

    pub fn content_length(&self) -> Option<u64> {
        self.size.map(|_| self.range.end - self.range.start)
    }

    pub fn size(&self) -> Option<u64> {
        self.size
    }

    pub fn meta(&self) -> Option<&ObjectMeta> {
        self.meta.as_ref()
    }

    pub async fn collect(mut self) -> Result<Bytes> {
        let Some(first) = self.stream.next().await else {
            return Ok(Bytes::new());
        };
        let first = first?;
        let Some(second) = self.stream.next().await else {
            return Ok(first);
        };
        let mut buf = BytesMut::with_capacity(
            self.content_length()
                .map_or(first.len() * 2, |l| l as usize),
        );
        buf.extend_from_slice(&first);
        buf.extend_from_slice(&second?);
        while let Some(chunk) = self.stream.next().await {
            buf.extend_from_slice(&chunk?);
        }
        Ok(buf.freeze())
    }
}

impl Stream for ReadStream {
    type Item = Result<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.stream.as_mut().poll_next(cx)
    }
}

impl std::fmt::Debug for ReadStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReadStream")
            .field("range", &self.range)
            .field("size", &self.size)
            .finish_non_exhaustive()
    }
}

pub(crate) struct Reader {
    engine: Arc<Engine>,
    request: ReadRange,
    ctx: Arc<ReadCtx>,
    range: Range<u64>,
    size: Option<u64>,
    /// Block indexes. `first..end` cover the range, `next` is the next to schedule, `emit` the next
    /// to yield.
    first: u32,
    next: u32,
    end: u32,
    emit: u32,
    window: VecDeque<SlotHandle>,
    /// Set after one restart on a stale generation, a second stale is surfaced as an error.
    restarted: bool,
    done: bool,
}

impl Reader {
    pub fn new(
        engine: Arc<Engine>,
        request: ReadRange,
        ctx: Arc<ReadCtx>,
        range: Range<u64>,
    ) -> Self {
        let blocks = ctx.namespace.config.block_size.blocks(&range);
        Self {
            engine,
            request,
            size: ctx.size,
            ctx,
            range,
            first: blocks.start,
            next: blocks.start,
            end: blocks.end,
            emit: blocks.start,
            window: VecDeque::new(),
            restarted: false,
            done: false,
        }
    }

    pub fn into_stream(self) -> ReadStream {
        let range = self.range.clone();
        let size = self.size;
        let meta = self.size.and_then(|_| {
            self.engine
                .meta
                .get(&self.ctx.key, self.ctx.namespace.meta_ttl())
        });
        let stream = futures::stream::unfold(self, |mut reader| async move {
            let item = reader.next().await;
            item.map(|item| (item, reader))
        })
        .boxed();
        ReadStream {
            stream,
            range,
            size,
            meta,
        }
    }

    fn learn_size(&mut self) {
        if self.size.is_some() {
            return;
        }
        if let Some(meta) = self
            .engine
            .meta
            .get(&self.ctx.key, self.ctx.namespace.meta_ttl())
        {
            self.size = Some(meta.size);
            self.range.end = self.range.end.min(meta.size);
            self.end = self
                .end
                .min(self.ctx.namespace.config.block_size.count(meta.size));
        }
    }

    async fn fill(&mut self) {
        self.learn_size();
        let config = self.ctx.namespace.config;
        let limit = if self.size.is_some() {
            config.read_window
        } else {
            config.fetch_window
        } as usize;
        while self.window.len() < limit && self.next < self.end {
            let take = aligned_take(self.next, self.end, config.fetch_window);
            if !self.window.is_empty() && self.window.len() + take as usize > limit {
                return;
            }
            let handles = self
                .engine
                .fetcher
                .schedule(&self.ctx, self.next..self.next + take, Priority::Foreground)
                .await;
            self.next += take;
            self.window.extend(handles);
            if self.size.is_none() {
                return;
            }
        }
    }

    async fn restart(&mut self) -> Result<()> {
        self.restarted = true;
        self.window.clear();
        let ctx = self
            .engine
            .resolve_ctx(&self.ctx.namespace, &self.ctx.name, true)
            .await?;
        let size = ctx.size.ok_or(NestorError::Stale)?;
        self.range = self.request.resolve(size)?;
        let blocks = ctx.namespace.config.block_size.blocks(&self.range);
        self.ctx = ctx;
        self.size = Some(size);
        self.first = blocks.start;
        self.next = blocks.start;
        self.end = blocks.end;
        self.emit = blocks.start;
        Ok(())
    }

    async fn next(&mut self) -> Option<Result<Bytes>> {
        loop {
            if self.done {
                return None;
            }
            self.fill().await;
            let Some(handle) = self.window.pop_front() else {
                self.done = true;
                return None;
            };
            let index = self.emit;
            self.emit += 1;
            match handle.wait().await {
                Ok(block) => {
                    self.learn_size();
                    let bs = self.ctx.namespace.config.block_size;
                    let within = bs.slice_within(index, &self.range);
                    if block.is_empty() || within.start >= block.len() {
                        self.done = true;
                        if index == self.first {
                            let size = self.size.unwrap_or(0);
                            return Some(Err(NestorError::Range(self.range.clone(), size)));
                        }
                        return None;
                    }
                    let end = within.end.min(block.len());
                    let out = block.slice(within.start..end);
                    if block.len() < bs.usize() || index + 1 >= self.end {
                        self.done = true;
                    }
                    self.ctx
                        .namespace
                        .metrics
                        .bytes_served
                        .increment(out.len() as u64);
                    return Some(Ok(out));
                }
                Err(e) if e.is_stale() && !self.restarted && index == self.first => {
                    if let Err(e) = self.restart().await {
                        self.done = true;
                        return Some(Err(e));
                    }
                }
                Err(e) => {
                    self.done = true;
                    return Some(Err(e.into()));
                }
            }
        }
    }
}
