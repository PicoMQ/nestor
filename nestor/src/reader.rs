//! Streaming reads. `Reader` walks the blocks covering a range with a bounded window in flight and
//! slices each block to the requested bytes. `ReadStream` is the public handle.

use std::collections::VecDeque;
use std::ops::Range;
use std::pin::Pin;
use std::sync::{Arc, OnceLock};
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
    known: Arc<OnceLock<ObjectMeta>>,
    peeked: Option<Bytes>,
}

impl ReadStream {
    pub fn meta(&self) -> Option<&ObjectMeta> {
        self.known.get()
    }

    pub fn size(&self) -> Option<u64> {
        self.meta().map(|m| m.size)
    }

    pub fn range(&self) -> Range<u64> {
        match self.size() {
            Some(size) => self.range.start..self.range.end.min(size),
            None => self.range.clone(),
        }
    }

    pub fn content_length(&self) -> Option<u64> {
        self.size().map(|_| {
            let range = self.range();
            range.end - range.start
        })
    }

    pub async fn ready(&mut self) -> Result<&ObjectMeta> {
        if self.known.get().is_none()
            && self.peeked.is_none()
            && let Some(first) = self.next().await
        {
            self.peeked = Some(first?);
        }
        self.known
            .get()
            .ok_or_else(|| NestorError::Range(self.range.clone(), 0))
    }

    pub async fn collect(mut self) -> Result<Bytes> {
        let Some(first) = self.next().await else {
            return Ok(Bytes::new());
        };
        let first = first?;
        let Some(second) = self.next().await else {
            return Ok(first);
        };
        let mut buf = BytesMut::with_capacity(
            self.content_length()
                .map_or(first.len() * 2, |l| l as usize),
        );
        buf.extend_from_slice(&first);
        buf.extend_from_slice(&second?);
        while let Some(chunk) = self.next().await {
            buf.extend_from_slice(&chunk?);
        }
        Ok(buf.freeze())
    }
}

impl Stream for ReadStream {
    type Item = Result<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Some(chunk) = self.peeked.take() {
            return Poll::Ready(Some(Ok(chunk)));
        }
        self.stream.as_mut().poll_next(cx)
    }
}

impl std::fmt::Debug for ReadStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReadStream")
            .field("range", &self.range())
            .field("meta", &self.meta())
            .finish_non_exhaustive()
    }
}

pub(crate) struct Reader {
    engine: Arc<Engine>,
    request: ReadRange,
    ctx: Arc<ReadCtx>,
    range: Range<u64>,
    known: Arc<OnceLock<ObjectMeta>>,
    first: u32,
    next: u32,
    end: u32,
    emit: u32,
    window: VecDeque<SlotHandle>,
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
        let known = Arc::new(OnceLock::new());
        if let Some(meta) = &ctx.meta {
            let _ = known.set(meta.clone());
        }
        Self {
            engine,
            request,
            known,
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
        let known = Arc::clone(&self.known);
        let stream = futures::stream::unfold(self, |mut reader| async move {
            let item = reader.next().await;
            item.map(|item| (item, reader))
        })
        .boxed();
        ReadStream {
            stream,
            range,
            known,
            peeked: None,
        }
    }

    fn size(&self) -> Option<u64> {
        self.known.get().map(|m| m.size)
    }

    fn learn(&mut self, meta: Option<ObjectMeta>) {
        if self.known.get().is_some() {
            return;
        }
        let Some(meta) = meta.or_else(|| self.engine.meta.any(&self.ctx.key)) else {
            return;
        };
        self.range.end = self.range.end.min(meta.size);
        self.end = self
            .end
            .min(self.ctx.namespace.config.block_size.count(meta.size));
        let _ = self.known.set(meta);
    }

    async fn fill(&mut self) {
        self.learn(None);
        let config = self.ctx.namespace.config;
        let limit = if self.size().is_some() {
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
            if self.size().is_none() {
                return;
            }
        }
    }

    async fn restart(&mut self) -> Result<()> {
        self.restarted = true;
        self.window.clear();
        let ctx = self
            .engine
            .resolve_ctx(
                &self.ctx.namespace,
                &self.ctx.name,
                &self.request,
                self.ctx.policy,
            )
            .await?;
        let meta = ctx.meta.clone().ok_or(NestorError::Stale)?;
        self.range = self.request.resolve(meta.size)?;
        let blocks = ctx.namespace.config.block_size.blocks(&self.range);
        self.ctx = ctx;
        self.known = Arc::new(OnceLock::from(meta));
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
                    self.learn(Some(block.meta));
                    let block = block.data;
                    let bs = self.ctx.namespace.config.block_size;
                    let within = bs.slice_within(index, &self.range);
                    if block.is_empty() || within.start >= block.len() {
                        self.done = true;
                        if index == self.first {
                            let size = self.size().unwrap_or(0);
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
