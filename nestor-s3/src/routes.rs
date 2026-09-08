//! Request routing. Authenticates, resolves the bucket, serves GET and HEAD from cache and forwards
//! everything else.

use std::sync::Arc;

use axum::body::Body;
use axum::extract::{Request, State};
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use chrono::Utc;
use futures::TryStreamExt;
use http::header::{CONTENT_LENGTH, CONTENT_RANGE, CONTENT_TYPE, ETAG, HeaderMap, HeaderValue};
use http::request::Parts;
use http::{Method, StatusCode};
use nestor::{NamespaceId, Precondition, ReadRange};
use quick_xml::Reader;
use quick_xml::escape::resolve_predefined_entity;
use quick_xml::events::Event;

use crate::addressing::Target;
use crate::auth::query_param;
use crate::error::S3Error;
use crate::headers::{content_headers, object_headers, parse_range, preconditions};
use crate::service::S3Service;

pub async fn health() -> &'static str {
    "ok"
}

const DELETE_OBJECTS_MAX_BODY: usize = 4 * 1024 * 1024;

pub async fn handle(State(service): State<Arc<S3Service>>, req: Request) -> Response {
    match dispatch(&service, req).await {
        Ok(response) => response,
        Err(e) => e.into_response(),
    }
}

async fn dispatch(service: &S3Service, req: Request) -> Result<Response, S3Error> {
    service
        .auth
        .verify(req.method(), req.uri(), req.headers(), Utc::now())?;
    let host = crate::auth::host_of(req.headers(), req.uri());
    let target = service.addressing.resolve(&host, req.uri().path())?;
    let raw_query = req.uri().query().unwrap_or("").to_owned();

    let cacheable = (req.method() == Method::GET || req.method() == Method::HEAD)
        && target.key.is_some()
        && raw_query.is_empty();
    if cacheable {
        let (parts, _) = req.into_parts();
        return serve(service, &parts, &target).await;
    }
    forward(service, req, target, &raw_query).await
}

async fn serve(service: &S3Service, req: &Parts, target: &Target) -> Result<Response, S3Error> {
    let bucket = target.bucket.as_deref().expect("cacheable requires bucket");
    let key = target.key.as_deref().expect("cacheable requires key");
    let ns = service.namespace(bucket)?;
    let nestor = &service.nestor;

    let meta = nestor
        .head(ns, key)
        .await
        .map_err(|e| S3Error::from_nestor(&e, key))?;

    let mut response = Response::builder();
    let headers = response.headers_mut().expect("fresh builder");
    object_headers(headers, &meta);
    headers.insert(
        CONTENT_TYPE,
        HeaderValue::from_static("application/octet-stream"),
    );

    match preconditions(&req.headers).evaluate(&meta) {
        Precondition::NotModified => {
            return response
                .status(StatusCode::NOT_MODIFIED)
                .body(Body::empty())
                .map_err(|e| S3Error::internal(e.to_string()));
        }
        Precondition::Failed => return Err(S3Error::precondition_failed(key)),
        Precondition::Satisfied => {}
    }

    let range = parse_range(&req.headers);
    let partial = range.is_some();
    let range = range.unwrap_or(ReadRange::Full);
    let Ok(resolved) = range.resolve(meta.size) else {
        let mut err = S3Error::invalid_range(key).into_response();
        if let Ok(v) = HeaderValue::from_str(&format!("bytes */{}", meta.size)) {
            err.headers_mut().insert(CONTENT_RANGE, v);
        }
        return Ok(err);
    };

    let headers = response.headers_mut().expect("fresh builder");
    content_headers(headers, &resolved, meta.size, partial);
    let status = if partial {
        StatusCode::PARTIAL_CONTENT
    } else {
        StatusCode::OK
    };

    if req.method == Method::HEAD {
        return response
            .status(status)
            .body(Body::empty())
            .map_err(|e| S3Error::internal(e.to_string()));
    }

    let stream = nestor
        .get(ns, key, ReadRange::Bounded(resolved))
        .await
        .map_err(|e| S3Error::from_nestor(&e, key))?;
    let body = Body::from_stream(stream.map_err(std::io::Error::other));
    response
        .status(status)
        .body(body)
        .map_err(|e| S3Error::internal(e.to_string()))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// How a forwarded request affects the cache once the origin accepts it.
enum Mutation {
    None,
    PutObject,
    DeleteObject,
    CompleteMultipart,
    DeleteObjects,
}

fn classify(method: &Method, target: &Target, raw_query: &str, copy: bool) -> Mutation {
    match (method, target.key.is_some()) {
        (&Method::PUT, true) => {
            if query_param(raw_query, "partNumber").is_some() {
                Mutation::None
            } else if copy || raw_query.is_empty() {
                Mutation::PutObject
            } else {
                Mutation::None
            }
        }
        (&Method::DELETE, true) if raw_query.is_empty() => Mutation::DeleteObject,
        (&Method::POST, true) if query_param(raw_query, "uploadId").is_some() => {
            Mutation::CompleteMultipart
        }
        (&Method::POST, false) if query_param(raw_query, "delete").is_some() => {
            Mutation::DeleteObjects
        }
        _ => Mutation::None,
    }
}

async fn forward(
    service: &S3Service,
    req: Request,
    target: Target,
    raw_query: &str,
) -> Result<Response, S3Error> {
    let copy = req.headers().contains_key("x-amz-copy-source");
    let mutation = classify(req.method(), &target, raw_query, copy);
    let payload_size = payload_size(req.headers());
    let capture = match mutation {
        Mutation::PutObject if !copy => service.populate_max,
        Mutation::DeleteObjects => Some(DELETE_OBJECTS_MAX_BODY),
        _ => None,
    };

    let (response, captured) = service.forwarder.forward(req, &target, capture).await?;
    if !response.status().is_success() {
        return Ok(response);
    }

    let ns = match &target.bucket {
        Some(bucket) => service.namespace(bucket)?,
        None => return Ok(response),
    };
    let nestor = &service.nestor;
    match mutation {
        Mutation::None => {}
        Mutation::PutObject => {
            let key = target.key.as_deref().expect("classified with key");
            let _ = nestor.invalidate(ns, key);
            if let Some(captured) = captured
                && let Some(data) = captured.take()
            {
                populate(nestor, ns, key, &response, &data);
            }
            if let Some(size) = payload_size
                && let Some(bucket) = &target.bucket
            {
                service.origins.written(bucket, key, size);
            }
        }
        Mutation::DeleteObject | Mutation::CompleteMultipart => {
            let key = target.key.as_deref().expect("classified with key");
            let _ = nestor.invalidate(ns, key);
        }
        Mutation::DeleteObjects => {
            if let Some(captured) = captured
                && let Some(body) = captured.take()
            {
                for key in delete_keys(&body) {
                    let _ = nestor.invalidate(ns, &key);
                }
            }
        }
    }
    Ok(response)
}

fn payload_size(headers: &HeaderMap) -> Option<u64> {
    headers
        .get("x-amz-decoded-content-length")
        .or_else(|| headers.get(CONTENT_LENGTH))
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse().ok())
}

fn populate(
    nestor: &nestor::Nestor,
    ns: NamespaceId,
    key: &str,
    response: &Response,
    data: &Bytes,
) {
    let etag = response
        .headers()
        .get(ETAG)
        .map(|v| Bytes::copy_from_slice(v.as_bytes()));
    let _ = nestor.insert(ns, key, etag, data);
}

fn delete_keys(xml: &[u8]) -> Vec<String> {
    let mut reader = Reader::from_reader(xml);
    reader.config_mut().trim_text(true);
    let mut keys = Vec::new();
    let mut current: Option<String> = None;
    loop {
        match reader.read_event() {
            Ok(Event::Start(e)) if e.local_name().as_ref() == "Key" => {
                current = Some(String::new());
            }
            Ok(Event::End(e)) if e.local_name().as_ref() == "Key" => {
                if let Some(key) = current.take() {
                    keys.push(key);
                }
            }
            Ok(Event::Text(t)) => {
                if let Some(key) = current.as_mut() {
                    key.push_str(&t.xml10_content());
                }
            }
            Ok(Event::GeneralRef(r)) => {
                if let Some(key) = current.as_mut() {
                    if let Ok(Some(c)) = r.resolve_char_ref() {
                        key.push(c);
                    } else if let Some(s) = resolve_predefined_entity(&r) {
                        key.push_str(s);
                    }
                }
            }
            Ok(Event::Eof) | Err(_) => break,
            Ok(_) => {}
        }
    }
    keys
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_delete_keys() {
        let xml = br#"<?xml version="1.0"?><Delete><Object><Key>a/b.txt</Key></Object><Object><Key>c&amp;d</Key><VersionId>1</VersionId></Object><Quiet>true</Quiet></Delete>"#;
        assert_eq!(delete_keys(xml), vec!["a/b.txt", "c&d"]);
    }

    #[test]
    fn classifies_mutations() {
        let obj = Target {
            bucket: Some("b".into()),
            key: Some("k".into()),
        };
        let bucket = Target {
            bucket: Some("b".into()),
            key: None,
        };
        assert_eq!(classify(&Method::PUT, &obj, "", false), Mutation::PutObject);
        assert_eq!(classify(&Method::PUT, &obj, "", true), Mutation::PutObject);
        assert_eq!(
            classify(&Method::PUT, &obj, "partNumber=1&uploadId=x", false),
            Mutation::None
        );
        assert_eq!(
            classify(&Method::PUT, &obj, "tagging", false),
            Mutation::None
        );
        assert_eq!(
            classify(&Method::DELETE, &obj, "", false),
            Mutation::DeleteObject
        );
        assert_eq!(
            classify(&Method::POST, &obj, "uploadId=x", false),
            Mutation::CompleteMultipart
        );
        assert_eq!(
            classify(&Method::POST, &obj, "uploads", false),
            Mutation::None
        );
        assert_eq!(
            classify(&Method::POST, &bucket, "delete", false),
            Mutation::DeleteObjects
        );
        assert_eq!(
            classify(&Method::GET, &bucket, "list-type=2", false),
            Mutation::None
        );
    }
}
