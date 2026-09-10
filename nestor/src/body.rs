//! The body of an origin attempt cut into blocks. `BlockStream` buffers chunks until a whole block
//! is available. `HedgedBody` races the primary stream against a backup request for the range still
//! owed when a block takes longer than the namespace's hedge delay, and the first stream to complete
//! the next block carries on.

use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use futures::StreamExt;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use tokio::sync::{Semaphore, SemaphorePermit};

use crate::error::OriginError;
use crate::fetch::ReadCtx;
use crate::key::{IMMUTABLE_TAG, content_tag};
use crate::origin::{GetOptions, GetResponse};

pub(crate) struct BlockStream {
    body: BoxStream<'static, Result<Bytes, OriginError>>,
    buf: BytesMut,
    since: Instant,
}

impl BlockStream {
    pub fn new(body: BoxStream<'static, Result<Bytes, OriginError>>) -> Self {
        Self {
            body,
            buf: BytesMut::new(),
            since: Instant::now(),
        }
    }

    pub async fn next_block(&mut self, expected: usize) -> Result<Bytes, OriginError> {
        loop {
            if self.buf.len() >= expected {
                return Ok(self.buf.split_to(expected).freeze());
            }
            let Some(chunk) = self.body.next().await else {
                return Err(OriginError::ShortRead {
                    expected: expected as u64,
                    got: self.buf.len() as u64,
                });
            };
            let mut chunk = chunk?;
            if self.buf.is_empty() && chunk.len() >= expected {
                let block = chunk.split_to(expected);
                self.buf.extend_from_slice(&chunk);
                return Ok(block);
            }
            if self.buf.capacity() < expected {
                self.buf.reserve(expected - self.buf.len());
            }
            self.buf.extend_from_slice(&chunk);
        }
    }

    pub fn lap(&mut self) -> Duration {
        let now = Instant::now();
        let elapsed = now.duration_since(self.since);
        self.since = now;
        elapsed
    }
}

pub(crate) struct HedgedBody<'a> {
    ctx: &'a ReadCtx,
    hedges: &'a Semaphore,
    range_end: u64,
    primary: BlockStream,
    backup: Backup<'a>,
}

enum Backup<'a> {
    Idle,
    Opening {
        opening: BoxFuture<'a, Result<GetResponse, OriginError>>,
        permit: SemaphorePermit<'a>,
    },
    Streaming {
        stream: BlockStream,
        _permit: SemaphorePermit<'a>,
    },
}

enum Outcome {
    Primary(Result<Bytes, OriginError>),
    Backup(Result<Bytes, OriginError>),
    Opened(Result<GetResponse, OriginError>),
    Stalled,
}

impl<'a> HedgedBody<'a> {
    pub fn new(
        ctx: &'a ReadCtx,
        hedges: &'a Semaphore,
        body: BoxStream<'static, Result<Bytes, OriginError>>,
        range_end: u64,
    ) -> Self {
        Self {
            ctx,
            hedges,
            range_end,
            primary: BlockStream::new(body),
            backup: Backup::Idle,
        }
    }

    pub fn lap(&mut self) -> Duration {
        self.primary.lap()
    }

    pub async fn next_block(&mut self, index: u32, expected: usize) -> Result<Bytes, OriginError> {
        let ns = &self.ctx.namespace;
        let start = ns.config.block_size.offset(index);
        loop {
            let outcome = match &mut self.backup {
                Backup::Idle => {
                    let Some(hedge) = &self.ctx.policy.hedge else {
                        return self.primary.next_block(expected).await;
                    };
                    let delay = hedge.delay(&ns.block);
                    ns.metrics.hedge_body.delay.set(delay.as_secs_f64());
                    tokio::select! {
                        block = self.primary.next_block(expected) => Outcome::Primary(block),
                        () = tokio::time::sleep(delay) => Outcome::Stalled,
                    }
                }
                Backup::Opening { opening, .. } => tokio::select! {
                    block = self.primary.next_block(expected) => Outcome::Primary(block),
                    response = opening => Outcome::Opened(response),
                },
                Backup::Streaming { stream, .. } => tokio::select! {
                    block = self.primary.next_block(expected) => Outcome::Primary(block),
                    block = stream.next_block(expected) => Outcome::Backup(block),
                },
            };
            match outcome {
                Outcome::Primary(Ok(block)) => {
                    self.backup = Backup::Idle;
                    return Ok(block);
                }
                Outcome::Primary(Err(e)) if e.is_retryable() => {
                    match std::mem::replace(&mut self.backup, Backup::Idle) {
                        Backup::Idle => return Err(e),
                        Backup::Opening { opening, .. } => {
                            let response = opening.await;
                            self.primary = self.adopt(response, start).ok_or(e)?;
                        }
                        Backup::Streaming { stream, .. } => self.primary = stream,
                    }
                }
                Outcome::Primary(Err(e)) => return Err(e),
                Outcome::Stalled => {
                    let Ok(permit) = self.hedges.try_acquire() else {
                        return self.primary.next_block(expected).await;
                    };
                    ns.metrics.hedge_body.issued.increment(1);
                    let options = GetOptions {
                        range: Some(start..self.range_end),
                        if_match: self.ctx.if_match.clone(),
                        if_none_match: None,
                    };
                    self.backup = Backup::Opening {
                        opening: ns.origin.get(&self.ctx.name, options),
                        permit,
                    };
                }
                Outcome::Opened(response) => {
                    let Backup::Opening { permit, .. } =
                        std::mem::replace(&mut self.backup, Backup::Idle)
                    else {
                        unreachable!("opened a backup that was not opening");
                    };
                    if let Some(stream) = self.adopt(response, start) {
                        self.backup = Backup::Streaming {
                            stream,
                            _permit: permit,
                        };
                    }
                }
                Outcome::Backup(Ok(block)) => {
                    let Backup::Streaming { stream, .. } =
                        std::mem::replace(&mut self.backup, Backup::Idle)
                    else {
                        unreachable!("a block from a backup that was not streaming");
                    };
                    ns.metrics.hedge_body.wins.increment(1);
                    self.primary = stream;
                    return Ok(block);
                }
                Outcome::Backup(Err(_)) => self.backup = Backup::Idle,
            }
        }
    }

    fn adopt(&self, response: Result<GetResponse, OriginError>, start: u64) -> Option<BlockStream> {
        let response = response.ok()?;
        let tag = content_tag(response.meta.etag.as_ref(), response.meta.size);
        let consistent = self.ctx.tag == IMMUTABLE_TAG || tag == self.ctx.tag;
        (consistent && response.range.start == start).then(|| BlockStream::new(response.body))
    }
}

#[cfg(test)]
mod tests {
    use futures::stream;

    use super::*;

    fn stream_of(chunks: &[&'static [u8]]) -> BlockStream {
        let chunks: Vec<Result<Bytes, OriginError>> = chunks
            .iter()
            .map(|chunk| Ok(Bytes::from_static(chunk)))
            .collect();
        BlockStream::new(stream::iter(chunks).boxed())
    }

    #[tokio::test]
    async fn assembles_blocks_across_chunk_boundaries() {
        let mut stream = stream_of(&[b"ab", b"cdef", b"gh", b"i"]);
        assert_eq!(
            stream.next_block(3).await.unwrap(),
            Bytes::from_static(b"abc")
        );
        assert_eq!(
            stream.next_block(3).await.unwrap(),
            Bytes::from_static(b"def")
        );
        assert_eq!(
            stream.next_block(3).await.unwrap(),
            Bytes::from_static(b"ghi")
        );
    }

    #[tokio::test]
    async fn whole_chunk_is_passed_through_without_copy() {
        let mut stream = stream_of(&[b"abcdef"]);
        assert_eq!(
            stream.next_block(4).await.unwrap(),
            Bytes::from_static(b"abcd")
        );
        assert_eq!(
            stream.next_block(2).await.unwrap(),
            Bytes::from_static(b"ef")
        );
    }

    #[tokio::test]
    async fn short_body_is_a_short_read() {
        let mut stream = stream_of(&[b"ab"]);
        assert!(matches!(
            stream.next_block(4).await,
            Err(OriginError::ShortRead {
                expected: 4,
                got: 2
            })
        ));
    }
}
