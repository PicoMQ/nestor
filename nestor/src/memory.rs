//! In-memory `Origin` for tests. Latency, slow responses, body stalls and failures can be injected
//! per call.

use std::collections::HashMap;
use std::ops::Range;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Mutex, RwLock};
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use futures::stream;

use crate::error::OriginError;
use crate::origin::{GetOptions, GetResponse, ObjectMeta, Origin};

#[derive(Clone)]
struct StoredObject {
    body: Bytes,
    etag: Bytes,
    modified: SystemTime,
}

#[derive(Debug, Default)]
pub struct OriginStats {
    pub gets: AtomicUsize,
    pub heads: AtomicUsize,
    pub bytes: AtomicU64,
}

pub struct MemoryOrigin {
    objects: RwLock<HashMap<String, StoredObject>>,
    stats: OriginStats,
    chunk: usize,
    latency: Mutex<Duration>,
    slow: Mutex<Option<(usize, Duration)>>,
    stall: Mutex<Option<(usize, Duration)>>,
    failures: Mutex<usize>,
    fail_every: AtomicUsize,
    versions: AtomicU64,
}

impl Default for MemoryOrigin {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryOrigin {
    pub fn new() -> Self {
        Self {
            objects: RwLock::new(HashMap::new()),
            stats: OriginStats::default(),
            chunk: 64 * 1024,
            latency: Mutex::new(Duration::ZERO),
            slow: Mutex::new(None),
            stall: Mutex::new(None),
            failures: Mutex::new(0),
            fail_every: AtomicUsize::new(0),
            versions: AtomicU64::new(1),
        }
    }

    pub fn with_chunk(mut self, chunk: usize) -> Self {
        self.chunk = chunk.max(1);
        self
    }

    pub fn put(&self, object: impl Into<String>, data: impl Into<Bytes>) -> Bytes {
        let version = self.versions.fetch_add(1, Ordering::Relaxed);
        let etag = Bytes::from(format!("\"v{version}\""));
        let stored = StoredObject {
            body: data.into(),
            etag: etag.clone(),
            modified: SystemTime::now(),
        };
        self.objects.write().unwrap().insert(object.into(), stored);
        etag
    }

    pub fn remove(&self, object: &str) {
        self.objects.write().unwrap().remove(object);
    }

    pub fn stats(&self) -> &OriginStats {
        &self.stats
    }

    pub fn gets(&self) -> usize {
        self.stats.gets.load(Ordering::Relaxed)
    }

    pub fn heads(&self) -> usize {
        self.stats.heads.load(Ordering::Relaxed)
    }

    pub fn set_latency(&self, latency: Duration) {
        *self.latency.lock().unwrap() = latency;
    }

    pub fn slow_next(&self, count: usize, latency: Duration) {
        *self.slow.lock().unwrap() = Some((count, latency));
    }

    pub fn stall_next(&self, count: usize, delay: Duration) {
        *self.stall.lock().unwrap() = Some((count, delay));
    }

    pub fn fail_next(&self, count: usize) {
        *self.failures.lock().unwrap() = count;
    }

    pub fn fail_every(&self, n: usize) {
        self.fail_every.store(n, Ordering::Relaxed);
    }

    fn take_failure(&self) -> bool {
        let mut f = self.failures.lock().unwrap();
        if *f > 0 {
            *f -= 1;
            return true;
        }
        let every = self.fail_every.load(Ordering::Relaxed);
        let requests =
            self.stats.gets.load(Ordering::Relaxed) + self.stats.heads.load(Ordering::Relaxed);
        every > 0 && requests.is_multiple_of(every)
    }

    fn delay(&self) -> Duration {
        let base = *self.latency.lock().unwrap();
        base + take_injected(&self.slow)
    }

    async fn simulate_network(&self) -> Result<(), OriginError> {
        let failing = self.take_failure();
        let delay = self.delay();
        if !delay.is_zero() {
            tokio::time::sleep(delay).await;
        }
        if failing {
            return Err(OriginError::io(std::io::Error::other("injected failure")));
        }
        Ok(())
    }

    fn lookup(&self, object: &str) -> Result<StoredObject, OriginError> {
        self.objects
            .read()
            .unwrap()
            .get(object)
            .cloned()
            .ok_or(OriginError::NotFound)
    }
}

fn take_injected(slot: &Mutex<Option<(usize, Duration)>>) -> Duration {
    let mut slot = slot.lock().unwrap();
    match slot.as_mut() {
        Some((remaining, delay)) if *remaining > 0 => {
            *remaining -= 1;
            *delay
        }
        _ => Duration::ZERO,
    }
}

fn meta_of(stored: &StoredObject) -> ObjectMeta {
    ObjectMeta {
        size: stored.body.len() as u64,
        etag: Some(stored.etag.clone()),
        last_modified: Some(stored.modified),
    }
}

fn resolve(range: Option<Range<u64>>, size: u64) -> Result<Range<u64>, OriginError> {
    match range {
        None => Ok(0..size),
        Some(r) if r.start >= size || r.start >= r.end => Err(OriginError::io(
            std::io::Error::other("range not satisfiable"),
        )),
        Some(r) => Ok(r.start..r.end.min(size)),
    }
}

#[async_trait]
impl Origin for MemoryOrigin {
    async fn get(&self, object: &str, options: GetOptions) -> Result<GetResponse, OriginError> {
        self.stats.gets.fetch_add(1, Ordering::Relaxed);
        self.simulate_network().await?;
        let stored = self.lookup(object)?;
        if let Some(expected) = &options.if_match
            && *expected != stored.etag
        {
            return Err(OriginError::PreconditionFailed);
        }
        if let Some(current) = &options.if_none_match
            && *current == stored.etag
        {
            return Err(OriginError::NotModified);
        }
        let meta = meta_of(&stored);
        let range = resolve(options.range, meta.size)?;
        let body = stored.body.slice(range.start as usize..range.end as usize);
        self.stats
            .bytes
            .fetch_add(body.len() as u64, Ordering::Relaxed);
        let chunk = self.chunk;
        let stall = take_injected(&self.stall);
        let chunks = (0..body.len())
            .step_by(chunk)
            .map(move |start| body.slice(start..(start + chunk).min(body.len())));
        let body = stream::iter(chunks.enumerate()).then(move |(i, chunk)| async move {
            if i == 1 && !stall.is_zero() {
                tokio::time::sleep(stall).await;
            }
            Ok(chunk)
        });
        Ok(GetResponse {
            meta,
            range,
            body: body.boxed(),
        })
    }

    async fn head(&self, object: &str) -> Result<ObjectMeta, OriginError> {
        self.stats.heads.fetch_add(1, Ordering::Relaxed);
        self.simulate_network().await?;
        Ok(meta_of(&self.lookup(object)?))
    }
}
