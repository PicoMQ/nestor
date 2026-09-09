//! Request bodies on the forward path. Decodes `aws-chunked` framing and optionally captures the
//! plain bytes for cache population.

use std::io;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, ready};

use axum::body::Body;
use bytes::{Buf, Bytes, BytesMut};
use futures::{Stream, StreamExt};
use pin_project_lite::pin_project;

const MAX_HEADER_LINE: usize = 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    Header,
    Data(usize),
    DataCrlf,
    Trailers,
    Done,
}

pin_project! {
    pub struct AwsChunkedDecoder<S> {
        #[pin]
        inner: S,
        buf: BytesMut,
        state: Phase,
        eof: bool,
    }
}

impl<S> AwsChunkedDecoder<S> {
    pub fn new(inner: S) -> Self {
        Self {
            inner,
            buf: BytesMut::new(),
            state: Phase::Header,
            eof: false,
        }
    }
}

fn invalid(msg: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg)
}

fn parse_header(line: &[u8]) -> io::Result<usize> {
    let size = line.split(|b| *b == b';').next().unwrap_or(line);
    let text = std::str::from_utf8(size).map_err(|_| invalid("chunk size is not utf-8"))?;
    usize::from_str_radix(text.trim(), 16).map_err(|_| invalid("chunk size is not hex"))
}

fn find_crlf(buf: &[u8]) -> Option<usize> {
    buf.windows(2).position(|w| w == b"\r\n")
}

impl<S, E> Stream for AwsChunkedDecoder<S>
where
    S: Stream<Item = Result<Bytes, E>>,
    E: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    type Item = io::Result<Bytes>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        loop {
            match *this.state {
                Phase::Done => return Poll::Ready(None),
                Phase::Header => {
                    if let Some(pos) = find_crlf(this.buf) {
                        let line = this.buf.split_to(pos);
                        this.buf.advance(2);
                        let size = parse_header(&line)?;
                        *this.state = if size == 0 {
                            Phase::Trailers
                        } else {
                            Phase::Data(size)
                        };
                        continue;
                    }
                    if this.buf.len() > MAX_HEADER_LINE {
                        return Poll::Ready(Some(Err(invalid("chunk header too long"))));
                    }
                }
                Phase::Data(remaining) => {
                    if !this.buf.is_empty() {
                        let take = remaining.min(this.buf.len());
                        let out = this.buf.split_to(take).freeze();
                        *this.state = if take == remaining {
                            Phase::DataCrlf
                        } else {
                            Phase::Data(remaining - take)
                        };
                        return Poll::Ready(Some(Ok(out)));
                    }
                }
                Phase::DataCrlf => {
                    if this.buf.len() >= 2 {
                        if &this.buf[..2] != b"\r\n" {
                            return Poll::Ready(Some(Err(invalid("missing chunk terminator"))));
                        }
                        this.buf.advance(2);
                        *this.state = Phase::Header;
                        continue;
                    }
                }
                Phase::Trailers => {
                    this.buf.clear();
                    if *this.eof {
                        *this.state = Phase::Done;
                        return Poll::Ready(None);
                    }
                }
            }

            if *this.eof {
                return Poll::Ready(Some(Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "aws-chunked body ended early",
                ))));
            }
            match ready!(this.inner.as_mut().poll_next(cx)) {
                Some(Ok(chunk)) => this.buf.extend_from_slice(&chunk),
                Some(Err(e)) => return Poll::Ready(Some(Err(io::Error::other(e.into())))),
                None => *this.eof = true,
            }
        }
    }
}

#[derive(Default)]
struct Captured {
    chunks: Vec<Bytes>,
    len: usize,
    overflow: bool,
}

pub struct Capture {
    inner: Arc<Mutex<Captured>>,
    limit: usize,
}

impl Capture {
    pub(crate) fn new(limit: usize) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Captured::default())),
            limit,
        }
    }

    fn push(inner: &Mutex<Captured>, limit: usize, chunk: &Bytes) {
        let mut c = inner.lock().unwrap_or_else(|e| e.into_inner());
        if c.overflow {
            return;
        }
        if c.len + chunk.len() > limit {
            c.overflow = true;
            c.chunks.clear();
            return;
        }
        c.len += chunk.len();
        c.chunks.push(chunk.clone());
    }

    pub fn take(self) -> Option<Bytes> {
        let mut c = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        if c.overflow {
            return None;
        }
        let chunks = std::mem::take(&mut c.chunks);
        Some(match chunks.len() {
            0 => Bytes::new(),
            1 => chunks.into_iter().next().expect("one chunk"),
            _ => {
                let mut buf = BytesMut::with_capacity(c.len);
                for chunk in &chunks {
                    buf.extend_from_slice(chunk);
                }
                buf.freeze()
            }
        })
    }
}

pub fn request_body(body: Body, streaming: bool, capture: Option<&Capture>) -> Body {
    let tee = capture.map(|c| (Arc::clone(&c.inner), c.limit));
    match (streaming, tee) {
        (false, None) => body,
        (false, Some((inner, limit))) => {
            Body::from_stream(body.into_data_stream().map(move |chunk| {
                if let Ok(chunk) = &chunk {
                    Capture::push(&inner, limit, chunk);
                }
                chunk
            }))
        }
        (true, None) => Body::from_stream(AwsChunkedDecoder::new(body.into_data_stream())),
        (true, Some((inner, limit))) => Body::from_stream(
            AwsChunkedDecoder::new(body.into_data_stream()).map(move |chunk| {
                if let Ok(chunk) = &chunk {
                    Capture::push(&inner, limit, chunk);
                }
                chunk
            }),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::{StreamExt, stream};

    async fn decode(parts: Vec<&'static [u8]>) -> io::Result<Vec<u8>> {
        let s = stream::iter(
            parts
                .into_iter()
                .map(|p| Ok::<_, io::Error>(Bytes::from_static(p))),
        );
        let mut d = AwsChunkedDecoder::new(s);
        let mut out = Vec::new();
        while let Some(chunk) = d.next().await {
            out.extend_from_slice(&chunk?);
        }
        Ok(out)
    }

    #[tokio::test]
    async fn decodes_signed_chunks() {
        let body: &[u8] = b"5;chunk-signature=abc\r\nhello\r\n6;chunk-signature=def\r\n world\r\n0;chunk-signature=ghi\r\nx-amz-checksum-crc32:AAAA\r\nx-amz-trailer-signature:zzz\r\n\r\n";
        assert_eq!(decode(vec![body]).await.unwrap(), b"hello world");
    }

    #[tokio::test]
    async fn decodes_across_arbitrary_splits() {
        let body: &[u8] = b"5\r\nhello\r\n6\r\n world\r\n0\r\n\r\n";
        for split in 1..body.len() {
            let (a, b) = body.split_at(split);
            assert_eq!(
                decode(vec![a, b]).await.unwrap(),
                b"hello world",
                "split {split}"
            );
        }
    }

    #[tokio::test]
    async fn rejects_truncated_body() {
        let body: &[u8] = b"5\r\nhel";
        assert_eq!(
            decode(vec![body]).await.unwrap_err().kind(),
            io::ErrorKind::UnexpectedEof
        );
    }
}
