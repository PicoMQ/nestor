use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use axum::Json;
use axum::Router;
use axum::extract::{Request, State};
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode, Uri, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use nestor::metrics::{
    BLOCKS_HIT, BLOCKS_JOINED, BLOCKS_MISS, BLOCKS_STALE, BYTES_SERVED, HEDGE_WINS, HEDGES,
    META_HEADS, ORIGIN_BYTES, ORIGIN_ERRORS, ORIGIN_REQUESTS, ORIGIN_RETRIES, ORIGIN_TIMEOUTS,
    READAHEAD_BLOCKS,
};
use nestor::{Consistency, HedgeAfter, NamespaceConfig, Nestor, NodeSnapshot};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::telemetry::Telemetry;

#[derive(rust_embed::RustEmbed)]
#[folder = "_dashboard/"]
struct Dashboard;

const DASHBOARD_HINT: &str = "<!doctype html><html><body style=\"font-family: sans-serif\">\
<h3>Nestor admin</h3>\
<p>This binary was built without the dashboard. Build it with\
<code> cd dashboard && npm install && npm run build</code> and recompile,\
or use the Docker image. The <code>/admin</code> API is available.</p>\
</body></html>";

#[derive(Clone)]
pub struct AdminState {
    nestor: Nestor,
    telemetry: Option<Telemetry>,
    s3: SocketAddr,
    metrics: Option<SocketAddr>,
    origin: String,
    serving: Arc<AtomicBool>,
}

impl AdminState {
    pub fn new(
        nestor: Nestor,
        telemetry: Option<Telemetry>,
        s3: SocketAddr,
        metrics: Option<SocketAddr>,
        origin: String,
    ) -> Self {
        Self {
            nestor,
            telemetry,
            s3,
            metrics,
            origin,
            serving: Arc::new(AtomicBool::new(true)),
        }
    }

    fn samples(&self) -> Vec<Sample> {
        self.telemetry
            .as_ref()
            .map(|t| parse_samples(&t.render_nestor()))
            .unwrap_or_default()
    }
}

pub fn router(state: AdminState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/ready", get(ready))
        .route("/admin/status", get(status))
        .route("/admin/namespaces", get(namespaces))
        .route("/", get(|| async { asset("index.html") }))
        .fallback(get(|uri: Uri| async move {
            asset(uri.path().trim_start_matches('/'))
        }))
        .layer(axum::middleware::from_fn(cors))
        .with_state(state)
}

async fn cors(request: Request, next: Next) -> Response {
    if request.method() == Method::OPTIONS {
        let mut response = StatusCode::NO_CONTENT.into_response();
        cors_headers(response.headers_mut());
        return response;
    }
    let mut response = next.run(request).await;
    cors_headers(response.headers_mut());
    response
}

fn cors_headers(headers: &mut HeaderMap) {
    headers.insert("Access-Control-Allow-Origin", HeaderValue::from_static("*"));
    headers.insert(
        "Access-Control-Allow-Methods",
        HeaderValue::from_static("GET, OPTIONS"),
    );
    headers.insert(
        "Access-Control-Allow-Headers",
        HeaderValue::from_static("authorization, content-type"),
    );
}

fn asset(path: &str) -> Response {
    let Some(file) = Dashboard::get(path) else {
        if path == "index.html" {
            return (
                [(header::CONTENT_TYPE, "text/html; charset=utf-8")],
                DASHBOARD_HINT,
            )
                .into_response();
        }
        return (StatusCode::NOT_FOUND, format!("no such path /{path}")).into_response();
    };
    let mime = match path.rsplit_once('.').map(|(_, ext)| ext) {
        Some("html") => "text/html; charset=utf-8",
        Some("js") => "text/javascript",
        Some("css") => "text/css",
        Some("svg") => "image/svg+xml",
        Some("map" | "json") => "application/json",
        _ => "application/octet-stream",
    };
    let cache = if path.starts_with("assets/") {
        "public, max-age=31536000, immutable"
    } else {
        "no-cache"
    };
    (
        [(header::CONTENT_TYPE, mime), (header::CACHE_CONTROL, cache)],
        file.data.into_owned(),
    )
        .into_response()
}

async fn health() -> &'static str {
    "ok"
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ReadyBody {
    ready: bool,
    serving: bool,
    s3: SocketAddr,
    metrics: Option<SocketAddr>,
}

async fn ready(State(state): State<AdminState>) -> (StatusCode, Json<ReadyBody>) {
    let serving = state.serving.load(Ordering::Relaxed);
    let body = ReadyBody {
        ready: serving,
        serving,
        s3: state.s3,
        metrics: state.metrics,
    };
    let status = if serving {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (status, Json(body))
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct StatusBody {
    listen: SocketAddr,
    metrics: Option<SocketAddr>,
    origin: String,
    cache: CacheBody,
    totals: CountersBody,
    hit_ratio: f64,
    amplification: f64,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CacheBody {
    memory_used: usize,
    memory_cap: usize,
    disk_cap: Option<usize>,
    meta_used: usize,
    meta_cap: usize,
    inflight: usize,
}

#[derive(Default, Serialize)]
#[serde(rename_all = "camelCase")]
struct CountersBody {
    hits: u64,
    misses: u64,
    joined: u64,
    stale: u64,
    origin_requests: u64,
    origin_bytes: u64,
    origin_errors: u64,
    origin_retries: u64,
    origin_timeouts: u64,
    hedges: u64,
    hedge_wins: u64,
    readahead_blocks: u64,
    meta_heads: u64,
    bytes_served: u64,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct NamespacesBody {
    namespaces: Vec<NamespaceBody>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct NamespaceBody {
    name: String,
    id: u32,
    block_size: u64,
    fetch_window: u32,
    read_window: u32,
    consistency: Value,
    readahead: u32,
    fetch: Value,
    counters: CountersBody,
    hit_ratio: f64,
}

async fn status(State(state): State<AdminState>) -> Json<StatusBody> {
    let snap = state.nestor.snapshot();
    let samples = state.samples();
    let totals = counters(&samples, None);
    Json(StatusBody {
        listen: state.s3,
        metrics: state.metrics,
        origin: state.origin.clone(),
        cache: cache_body(&snap),
        hit_ratio: hit_ratio(&totals),
        amplification: amplification(&totals),
        totals,
    })
}

async fn namespaces(State(state): State<AdminState>) -> Json<NamespacesBody> {
    let snap = state.nestor.snapshot();
    let samples = state.samples();
    Json(NamespacesBody {
        namespaces: snap
            .namespaces
            .iter()
            .map(|ns| {
                let counters = counters(&samples, Some(ns.name.as_ref()));
                NamespaceBody {
                    name: ns.name.to_string(),
                    id: ns.id.as_u32(),
                    block_size: ns.config.block_size.bytes(),
                    fetch_window: ns.config.fetch_window,
                    read_window: ns.config.read_window,
                    consistency: consistency_json(ns.config.consistency),
                    readahead: ns.config.readahead,
                    fetch: fetch_json(&ns.config),
                    hit_ratio: hit_ratio(&counters),
                    counters,
                }
            })
            .collect(),
    })
}

fn cache_body(snap: &NodeSnapshot) -> CacheBody {
    CacheBody {
        memory_used: snap.cache.memory_used,
        memory_cap: snap.cache.memory_cap,
        disk_cap: snap.cache.disk_cap,
        meta_used: snap.cache.meta_used,
        meta_cap: snap.cache.meta_cap,
        inflight: snap.cache.inflight,
    }
}

fn consistency_json(consistency: Consistency) -> Value {
    match consistency {
        Consistency::Immutable => json!({ "mode": "immutable" }),
        Consistency::Etag { ttl } => json!({
            "mode": "etag",
            "ttlSeconds": ttl.as_secs(),
        }),
    }
}

fn fetch_json(config: &NamespaceConfig) -> Value {
    let fetch = &config.fetch;
    json!({
        "attempts": fetch.attempts,
        "backoffMs": fetch.backoff.as_millis() as u64,
        "backoffMaxMs": fetch.backoff_max.as_millis() as u64,
        "firstByteMs": fetch.first_byte.as_millis() as u64,
        "attemptMs": fetch.attempt.as_millis() as u64,
        "deadlineMs": fetch.deadline.as_millis() as u64,
        "hedge": fetch.hedge.map(|h| {
            let delay = match h.after {
                HedgeAfter::Factor(factor) => json!({ "factor": factor }),
                HedgeAfter::Quantile(quantile) => json!({ "quantile": quantile }),
            };
            json!({
                "after": delay,
                "minMs": h.min.as_millis() as u64,
                "maxMs": h.max.as_millis() as u64,
            })
        }),
    })
}

fn hit_ratio(c: &CountersBody) -> f64 {
    let den = c.hits + c.misses + c.joined;
    if den == 0 {
        0.0
    } else {
        c.hits as f64 / den as f64
    }
}

fn amplification(c: &CountersBody) -> f64 {
    if c.bytes_served == 0 {
        0.0
    } else {
        c.origin_bytes as f64 / c.bytes_served as f64
    }
}

struct Sample {
    name: String,
    labels: HashMap<String, String>,
    value: u64,
}

fn parse_samples(text: &str) -> Vec<Sample> {
    text.lines().filter_map(parse_sample).collect()
}

fn parse_sample(line: &str) -> Option<Sample> {
    let line = line.trim();
    if line.is_empty() || line.starts_with('#') {
        return None;
    }
    let (rest, raw) = line.rsplit_once(' ')?;
    let value = raw.parse::<f64>().ok()?;
    let value = u64::try_from(value.round().max(0.0) as i128).ok()?;
    let (name, labels) = match rest.split_once('{') {
        Some((name, tail)) => (name, parse_labels(tail.trim_end_matches('}'))),
        None => (rest, HashMap::new()),
    };
    Some(Sample {
        name: name.to_owned(),
        labels,
        value,
    })
}

fn parse_labels(raw: &str) -> HashMap<String, String> {
    raw.split(',')
        .filter_map(|pair| {
            let (key, value) = pair.split_once('=')?;
            Some((
                key.trim().to_owned(),
                value.trim().trim_matches('"').to_owned(),
            ))
        })
        .collect()
}

fn metric(samples: &[Sample], name: &str, namespace: Option<&str>) -> u64 {
    samples
        .iter()
        .filter(|s| s.name == name)
        .filter(|s| match namespace {
            Some(ns) => s.labels.get("namespace").map(String::as_str) == Some(ns),
            None => true,
        })
        .map(|s| s.value)
        .sum()
}

fn counters(samples: &[Sample], namespace: Option<&str>) -> CountersBody {
    CountersBody {
        hits: metric(samples, BLOCKS_HIT, namespace),
        misses: metric(samples, BLOCKS_MISS, namespace),
        joined: metric(samples, BLOCKS_JOINED, namespace),
        stale: metric(samples, BLOCKS_STALE, namespace),
        origin_requests: metric(samples, ORIGIN_REQUESTS, namespace),
        origin_bytes: metric(samples, ORIGIN_BYTES, namespace),
        origin_errors: metric(samples, ORIGIN_ERRORS, namespace),
        origin_retries: metric(samples, ORIGIN_RETRIES, namespace),
        origin_timeouts: metric(samples, ORIGIN_TIMEOUTS, namespace),
        hedges: metric(samples, HEDGES, namespace),
        hedge_wins: metric(samples, HEDGE_WINS, namespace),
        readahead_blocks: metric(samples, READAHEAD_BLOCKS, namespace),
        meta_heads: metric(samples, META_HEADS, namespace),
        bytes_served: metric(samples, BYTES_SERVED, namespace),
    }
}

#[cfg(test)]
mod tests {
    use std::net::{Ipv4Addr, SocketAddr};
    use std::sync::Arc;

    use axum::body::{Body, to_bytes};
    use axum::http::Request;
    use nestor::{CacheConfig, MemoryOrigin, Namespace, Nestor};
    use tower::ServiceExt;

    use super::*;

    fn listen() -> SocketAddr {
        SocketAddr::from((Ipv4Addr::LOCALHOST, 9000))
    }

    async fn app() -> Router {
        let origin = Arc::new(MemoryOrigin::new());
        origin.put("obj", bytes::Bytes::from_static(b"hello"));
        let nestor = Nestor::builder(CacheConfig::memory(8 << 20))
            .namespace(Namespace::new("bucket", origin))
            .build()
            .await
            .unwrap();
        router(AdminState::new(
            nestor,
            None,
            listen(),
            None,
            "http://origin:9000".into(),
        ))
    }

    async fn get(path: &str) -> (StatusCode, String, HeaderMap) {
        let response = app()
            .await
            .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        let status = response.status();
        let headers = response.headers().clone();
        let body = String::from_utf8(
            to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap()
                .to_vec(),
        )
        .unwrap();
        (status, body, headers)
    }

    #[tokio::test]
    async fn health_and_ready_are_open() {
        let (status, body, _) = get("/health").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, "ok");

        let (status, body, _) = get("/ready").await;
        assert_eq!(status, StatusCode::OK);
        let ready: ReadyBody = serde_json::from_str(&body).unwrap();
        assert!(ready.ready);
        assert_eq!(ready.s3, listen());
    }

    #[tokio::test]
    async fn dashboard_is_served_at_root() {
        let (status, body, headers) = get("/").await;
        assert_eq!(status, StatusCode::OK);
        assert!(
            headers
                .get(header::CONTENT_TYPE)
                .unwrap()
                .to_str()
                .unwrap()
                .starts_with("text/html")
        );
        assert!(body.contains("Nestor"));
        if let Some(asset) = body
            .split("assets/")
            .nth(1)
            .and_then(|s| s.split('"').next())
        {
            let (status, _, cache) = get(&format!("/assets/{asset}")).await;
            assert_eq!(status, StatusCode::OK);
            assert_eq!(
                cache.get(header::CACHE_CONTROL).unwrap(),
                "public, max-age=31536000, immutable"
            );
        }
        let (status, _, _) = get("/assets/absent.js").await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn status_and_namespaces_are_json() {
        let (status, body, _) = get("/admin/status").await;
        assert_eq!(status, StatusCode::OK);
        let value: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(value["origin"], "http://origin:9000");
        assert_eq!(value["cache"]["inflight"], 0);
        assert!(value["cache"]["metaCap"].as_u64().unwrap() > 0);

        let (status, body, _) = get("/admin/namespaces").await;
        assert_eq!(status, StatusCode::OK);
        let value: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(value["namespaces"][0]["name"], "bucket");
        assert_eq!(value["namespaces"][0]["counters"]["hits"], 0);
    }

    #[test]
    fn parse_prometheus_text_by_namespace() {
        let samples = parse_samples(
            "# TYPE nestor_blocks_hit_total counter\n\
             nestor_blocks_hit_total{namespace=\"a\"} 3\n\
             nestor_blocks_hit_total{namespace=\"b\"} 4\n\
             nestor_blocks_miss_total{namespace=\"a\"} 1\n\
             nestor_hedges_total{namespace=\"a\",phase=\"headers\"} 2\n\
             nestor_hedges_total{namespace=\"a\",phase=\"body\"} 1\n",
        );
        assert_eq!(metric(&samples, BLOCKS_HIT, None), 7);
        assert_eq!(metric(&samples, BLOCKS_HIT, Some("a")), 3);
        assert_eq!(metric(&samples, HEDGES, Some("a")), 3);
        assert_eq!(metric(&samples, BLOCKS_STALE, Some("a")), 0);
    }
}
