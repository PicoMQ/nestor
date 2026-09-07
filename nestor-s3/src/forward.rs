//! Re-signs a request with the origin's credentials and proxies it upstream.

use axum::body::Body;
use chrono::Utc;
use http::header::{
    AUTHORIZATION, CONNECTION, CONTENT_ENCODING, CONTENT_LENGTH, HOST, HeaderMap, HeaderName,
    HeaderValue, TE, TRAILER, TRANSFER_ENCODING, UPGRADE,
};
use http::{Request, Response, Uri};
use hyper_rustls::HttpsConnector;
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::TokioExecutor;

use crate::addressing::Target;
use crate::body::{Capture, request_body};
use crate::error::S3Error;
use crate::origin::OriginConfig;
use crate::sigv4::{
    Credentials, X_AMZ_CONTENT_SHA256, X_AMZ_DATE, X_AMZ_SECURITY_TOKEN, sign, uri_encode,
};

const PRESIGN_PARAMS: &[&str] = &[
    "X-Amz-Algorithm",
    "X-Amz-Credential",
    "X-Amz-Date",
    "X-Amz-Expires",
    "X-Amz-SignedHeaders",
    "X-Amz-Signature",
    "X-Amz-Security-Token",
];

const X_AMZ_DECODED_CONTENT_LENGTH: HeaderName =
    HeaderName::from_static("x-amz-decoded-content-length");
const X_AMZ_TRAILER: HeaderName = HeaderName::from_static("x-amz-trailer");

fn is_hop_by_hop(name: &HeaderName) -> bool {
    name == CONNECTION
        || name == TE
        || name == TRAILER
        || name == TRANSFER_ENCODING
        || name == UPGRADE
        || name == "keep-alive"
        || name.as_str().starts_with("proxy-")
}

fn upstream_headers(incoming: &HeaderMap, streaming: bool) -> Result<HeaderMap, S3Error> {
    let mut headers = HeaderMap::with_capacity(incoming.len() + 4);
    for (name, value) in incoming {
        if is_hop_by_hop(name)
            || *name == HOST
            || *name == AUTHORIZATION
            || *name == CONTENT_LENGTH
            || *name == X_AMZ_DATE
            || *name == X_AMZ_CONTENT_SHA256
            || *name == X_AMZ_SECURITY_TOKEN
            || *name == X_AMZ_DECODED_CONTENT_LENGTH
            || *name == X_AMZ_TRAILER
            || name.as_str() == "expect"
        {
            continue;
        }
        if *name == CONTENT_ENCODING && streaming {
            let kept: Vec<&str> = value
                .to_str()
                .unwrap_or("")
                .split(',')
                .map(str::trim)
                .filter(|e| !e.is_empty() && !e.eq_ignore_ascii_case("aws-chunked"))
                .collect();
            if !kept.is_empty()
                && let Ok(v) = HeaderValue::from_str(&kept.join(","))
            {
                headers.append(CONTENT_ENCODING, v);
            }
            continue;
        }
        headers.append(name.clone(), value.clone());
    }

    let content_length = if streaming {
        let decoded = incoming
            .get(&X_AMZ_DECODED_CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<u64>().ok())
            .ok_or_else(|| S3Error::invalid_request("missing x-amz-decoded-content-length"))?;
        Some(decoded)
    } else {
        incoming
            .get(CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<u64>().ok())
    };
    if let Some(len) = content_length {
        headers.insert(CONTENT_LENGTH, HeaderValue::from(len));
    }
    Ok(headers)
}

pub struct Forwarder {
    client: Client<HttpsConnector<HttpConnector>, Body>,
    origin: OriginConfig,
}

impl Forwarder {
    pub fn new(origin: OriginConfig) -> Self {
        let mut http = HttpConnector::new();
        http.enforce_http(false);
        http.set_nodelay(true);
        let https = hyper_rustls::HttpsConnectorBuilder::new()
            .with_webpki_roots()
            .https_or_http()
            .enable_http1()
            .enable_http2()
            .wrap_connector(http);
        let client = Client::builder(TokioExecutor::new())
            .pool_max_idle_per_host(64)
            .build(https);
        Self { client, origin }
    }

    pub fn origin(&self) -> &OriginConfig {
        &self.origin
    }

    fn upstream_uri(&self, target: &Target, raw_query: &str) -> Result<(Uri, String), S3Error> {
        let authority = self
            .origin
            .authority()
            .ok_or_else(|| S3Error::internal("origin endpoint has no authority"))?;
        let (host, mut path) = match (&target.bucket, self.origin.virtual_hosted) {
            (Some(bucket), true) => (format!("{bucket}.{authority}"), String::from("/")),
            (Some(bucket), false) => (authority.to_string(), format!("/{bucket}")),
            (None, _) => (authority.to_string(), String::from("/")),
        };
        if let Some(key) = &target.key {
            if !path.ends_with('/') {
                path.push('/');
            }
            path.push_str(&uri_encode(key, false));
        } else if target.bucket.is_some() && !self.origin.virtual_hosted && !path.ends_with('/') {
            path.push('/');
        }
        let query = raw_query
            .split('&')
            .filter(|p| !p.is_empty())
            .filter(|p| {
                let name = p.split_once('=').map_or(*p, |(k, _)| k);
                !PRESIGN_PARAMS.iter().any(|x| x.eq_ignore_ascii_case(name))
            })
            .collect::<Vec<_>>()
            .join("&");
        let mut uri = format!("{}://{host}{path}", self.origin.scheme());
        if !query.is_empty() {
            uri.push('?');
            uri.push_str(&query);
        }
        let uri: Uri = uri
            .parse()
            .map_err(|_| S3Error::internal("failed to build upstream uri"))?;
        Ok((uri, host))
    }

    pub async fn forward(
        &self,
        req: Request<Body>,
        target: &Target,
        capture: Option<usize>,
    ) -> Result<(Response<Body>, Option<Capture>), S3Error> {
        let (parts, body) = req.into_parts();
        let raw_query = parts.uri.query().unwrap_or("");
        let (uri, host) = self.upstream_uri(target, raw_query)?;

        let streaming = parts
            .headers
            .get(&X_AMZ_CONTENT_SHA256)
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| v.starts_with("STREAMING-"));

        let mut headers = upstream_headers(&parts.headers, streaming)?;

        let capture = capture.map(Capture::new);
        let body = request_body(body, streaming, capture.as_ref());

        headers.insert(
            HOST,
            HeaderValue::from_str(&host).map_err(|_| S3Error::internal("invalid host"))?,
        );
        if let Some(provider) = &self.origin.credentials {
            let credential = provider.get_credential().await.map_err(|e| {
                S3Error::bad_gateway(format!("failed to obtain origin credentials: {e}"))
            })?;
            sign(
                &parts.method,
                &uri,
                &host,
                &mut headers,
                Credentials {
                    access_key: &credential.key_id,
                    secret_key: &credential.secret_key,
                    session_token: credential.token.as_deref(),
                },
                &self.origin.region,
                Utc::now(),
            );
        }

        let mut upstream = Request::builder()
            .method(parts.method.clone())
            .uri(uri)
            .version(http::Version::HTTP_11)
            .body(body)
            .map_err(|e| S3Error::internal(e.to_string()))?;
        *upstream.headers_mut() = headers;

        let response = self
            .client
            .request(upstream)
            .await
            .map_err(|e| S3Error::bad_gateway(format!("origin request failed: {e}")))?;

        let (mut parts, incoming) = response.into_parts();
        let names: Vec<HeaderName> = parts
            .headers
            .keys()
            .filter(|n| is_hop_by_hop(n))
            .cloned()
            .collect();
        for name in names {
            parts.headers.remove(name);
        }
        Ok((Response::from_parts(parts, Body::new(incoming)), capture))
    }
}

impl std::fmt::Debug for Forwarder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Forwarder")
            .field("endpoint", &self.origin.endpoint)
            .field("region", &self.origin.region)
            .field("virtual_hosted", &self.origin.virtual_hosted)
            .finish_non_exhaustive()
    }
}
