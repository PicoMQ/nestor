//! Counting and fault-injecting HTTP proxy in front of the origin. Every target reads through it,
//! so origin requests are counted the same way regardless of what is being measured.

use std::net::SocketAddr;
use std::ops::Sub;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use bytes::Bytes;
use eyre::WrapErr;
use http::{Method, Request, Response, StatusCode, Uri};
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, Empty, Full};
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::{TokioExecutor, TokioIo};
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;

use crate::dataset::Rng;

#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct Faults {
    #[serde(with = "humantime_serde")]
    pub latency: Duration,
    pub slow_rate: f64,
    #[serde(with = "humantime_serde")]
    pub slow: Duration,
    pub fail_rate: f64,
}

impl Faults {
    pub fn from_query(query: &str) -> eyre::Result<Self> {
        let mut faults = Self::default();
        for pair in query.split('&').filter(|p| !p.is_empty()) {
            let (key, value) = pair
                .split_once('=')
                .ok_or_else(|| eyre::eyre!("bad fault parameter {pair}"))?;
            match key {
                "latency" => faults.latency = humantime::parse_duration(value)?,
                "slow" => faults.slow = humantime::parse_duration(value)?,
                "slow_rate" => faults.slow_rate = value.parse()?,
                "fail_rate" => faults.fail_rate = value.parse()?,
                other => eyre::bail!("unknown fault parameter {other}"),
            }
        }
        Ok(faults)
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct OriginCounters {
    pub requests: u64,
    pub gets: u64,
    pub heads: u64,
    pub bytes: u64,
    pub errors: u64,
    pub injected: u64,
    pub max_inflight: u64,
}

impl Sub for OriginCounters {
    type Output = Self;

    fn sub(self, rhs: Self) -> Self {
        Self {
            requests: self.requests - rhs.requests,
            gets: self.gets - rhs.gets,
            heads: self.heads - rhs.heads,
            bytes: self.bytes - rhs.bytes,
            errors: self.errors - rhs.errors,
            injected: self.injected - rhs.injected,
            max_inflight: self.max_inflight,
        }
    }
}

#[derive(Default)]
struct Counters {
    requests: AtomicU64,
    gets: AtomicU64,
    heads: AtomicU64,
    bytes: AtomicU64,
    errors: AtomicU64,
    injected: AtomicU64,
    inflight: AtomicU64,
    max_inflight: AtomicU64,
}

impl Counters {
    fn snapshot(&self) -> OriginCounters {
        OriginCounters {
            requests: self.requests.load(Ordering::Relaxed),
            gets: self.gets.load(Ordering::Relaxed),
            heads: self.heads.load(Ordering::Relaxed),
            bytes: self.bytes.load(Ordering::Relaxed),
            errors: self.errors.load(Ordering::Relaxed),
            injected: self.injected.load(Ordering::Relaxed),
            max_inflight: self.max_inflight.load(Ordering::Relaxed),
        }
    }

    fn reset(&self) {
        for counter in [
            &self.requests,
            &self.gets,
            &self.heads,
            &self.bytes,
            &self.errors,
            &self.injected,
            &self.max_inflight,
        ] {
            counter.store(0, Ordering::Relaxed);
        }
    }

    fn enter(&self) {
        let now = self.inflight.fetch_add(1, Ordering::Relaxed) + 1;
        self.max_inflight.fetch_max(now, Ordering::Relaxed);
    }

    fn leave(&self) {
        self.inflight.fetch_sub(1, Ordering::Relaxed);
    }
}

pub struct Proxy {
    upstream: String,
    client: Client<HttpConnector, Incoming>,
    counters: Counters,
    faults: RwLock<Faults>,
    rng: Mutex<Rng>,
}

type ProxyBody = BoxBody<Bytes, hyper::Error>;

impl Proxy {
    pub fn new(upstream: impl Into<String>) -> Arc<Self> {
        Arc::new(Self {
            upstream: upstream.into(),
            client: Client::builder(TokioExecutor::new()).build_http(),
            counters: Counters::default(),
            faults: RwLock::new(Faults::default()),
            rng: Mutex::new(Rng::new(0x5eed)),
        })
    }

    pub fn snapshot(&self) -> OriginCounters {
        self.counters.snapshot()
    }

    pub fn reset(&self) {
        self.counters.reset();
    }

    pub fn faults(&self) -> Faults {
        *self.faults.read().unwrap_or_else(|e| e.into_inner())
    }

    pub fn set_faults(&self, faults: Faults) {
        *self.faults.write().unwrap_or_else(|e| e.into_inner()) = faults;
    }

    pub async fn serve(self: Arc<Self>, listen: SocketAddr) -> eyre::Result<()> {
        let listener = TcpListener::bind(listen)
            .await
            .wrap_err_with(|| format!("bind proxy on {listen}"))?;
        loop {
            let (stream, _) = listener.accept().await?;
            stream.set_nodelay(true)?;
            let proxy = Arc::clone(&self);
            tokio::spawn(async move {
                let service = service_fn(move |req| Arc::clone(&proxy).forward(req));
                let conn = http1::Builder::new()
                    .preserve_header_case(true)
                    .serve_connection(TokioIo::new(stream), service);
                if let Err(error) = conn.await {
                    tracing::debug!(%error, "proxy connection ended");
                }
            });
        }
    }

    pub async fn serve_control(self: Arc<Self>, listen: SocketAddr) -> eyre::Result<()> {
        let listener = TcpListener::bind(listen)
            .await
            .wrap_err_with(|| format!("bind proxy control on {listen}"))?;
        loop {
            let (stream, _) = listener.accept().await?;
            let proxy = Arc::clone(&self);
            tokio::spawn(async move {
                let service = service_fn(move |req| {
                    let proxy = Arc::clone(&proxy);
                    async move { Ok::<_, hyper::Error>(proxy.control(&req)) }
                });
                drop(
                    http1::Builder::new()
                        .serve_connection(TokioIo::new(stream), service)
                        .await,
                );
            });
        }
    }

    fn control(&self, req: &Request<Incoming>) -> Response<ProxyBody> {
        let text = |status: StatusCode, body: String| {
            Response::builder()
                .status(status)
                .header("content-type", "text/plain; version=0.0.4")
                .body(
                    Full::new(Bytes::from(body))
                        .map_err(|never| match never {})
                        .boxed(),
                )
                .expect("response")
        };
        match (req.method(), req.uri().path()) {
            (&Method::GET, "/metrics") => text(StatusCode::OK, self.render()),
            (&Method::POST, "/reset") => {
                self.reset();
                text(StatusCode::OK, String::new())
            }
            (&Method::PUT, "/faults") => {
                match Faults::from_query(req.uri().query().unwrap_or_default()) {
                    Ok(faults) => {
                        self.set_faults(faults);
                        text(StatusCode::OK, format!("{faults:?}\n"))
                    }
                    Err(error) => text(StatusCode::BAD_REQUEST, format!("{error}\n")),
                }
            }
            _ => text(StatusCode::NOT_FOUND, String::new()),
        }
    }

    pub fn render(&self) -> String {
        let c = self.snapshot();
        format!(
            "proxy_requests_total {}\nproxy_gets_total {}\nproxy_heads_total {}\nproxy_bytes_total {}\nproxy_errors_total {}\nproxy_injected_failures_total {}\nproxy_max_inflight {}\n",
            c.requests, c.gets, c.heads, c.bytes, c.errors, c.injected, c.max_inflight
        )
    }

    fn draw(&self) -> f64 {
        self.rng.lock().unwrap_or_else(|e| e.into_inner()).unit()
    }

    async fn forward(
        self: Arc<Self>,
        req: Request<Incoming>,
    ) -> Result<Response<ProxyBody>, hyper::Error> {
        let counters = &self.counters;
        counters.enter();
        counters.requests.fetch_add(1, Ordering::Relaxed);
        match *req.method() {
            Method::GET => counters.gets.fetch_add(1, Ordering::Relaxed),
            Method::HEAD => counters.heads.fetch_add(1, Ordering::Relaxed),
            _ => 0,
        };

        let faults = self.faults();
        if faults.fail_rate > 0.0 && self.draw() < faults.fail_rate {
            counters.injected.fetch_add(1, Ordering::Relaxed);
            counters.leave();
            return Ok(status(StatusCode::SERVICE_UNAVAILABLE));
        }
        let mut delay = faults.latency;
        if faults.slow_rate > 0.0 && self.draw() < faults.slow_rate {
            delay += faults.slow;
        }
        if !delay.is_zero() {
            tokio::time::sleep(delay).await;
        }

        let (mut parts, body) = req.into_parts();
        let path_and_query = parts
            .uri
            .path_and_query()
            .map_or("/", |pq| pq.as_str())
            .to_owned();
        let Ok(uri) = format!("http://{}{path_and_query}", self.upstream).parse::<Uri>() else {
            counters.leave();
            return Ok(status(StatusCode::BAD_REQUEST));
        };
        parts.uri = uri;
        let response = self.client.request(Request::from_parts(parts, body)).await;
        let response = match response {
            Ok(response) => response,
            Err(error) => {
                tracing::warn!(%error, "upstream request failed");
                counters.errors.fetch_add(1, Ordering::Relaxed);
                counters.leave();
                return Ok(status(StatusCode::BAD_GATEWAY));
            }
        };
        if response.status().is_server_error() {
            counters.errors.fetch_add(1, Ordering::Relaxed);
        }
        let guard = Leave(Arc::clone(&self));
        Ok(response.map(move |body| {
            body.map_frame(move |frame| {
                if let Some(data) = frame.data_ref() {
                    guard
                        .0
                        .counters
                        .bytes
                        .fetch_add(data.len() as u64, Ordering::Relaxed);
                }
                frame
            })
            .boxed()
        }))
    }
}

struct Leave(Arc<Proxy>);

impl Drop for Leave {
    fn drop(&mut self) {
        self.0.counters.leave();
    }
}

fn status(status: StatusCode) -> Response<ProxyBody> {
    Response::builder()
        .status(status)
        .body(Empty::new().map_err(|never| match never {}).boxed())
        .expect("response")
}
