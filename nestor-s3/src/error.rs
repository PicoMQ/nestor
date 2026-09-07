//! S3 XML error responses.

use axum::body::Body;
use axum::response::{IntoResponse, Response};
use http::StatusCode;
use http::header::{CONTENT_LENGTH, CONTENT_TYPE};
use nestor::NestorError;

#[derive(Debug, Clone)]
pub struct S3Error {
    pub status: StatusCode,
    pub code: &'static str,
    pub message: String,
    pub resource: String,
}

impl S3Error {
    pub fn new(status: StatusCode, code: &'static str, message: impl Into<String>) -> Self {
        Self {
            status,
            code,
            message: message.into(),
            resource: String::new(),
        }
    }

    pub fn resource(mut self, resource: impl Into<String>) -> Self {
        self.resource = resource.into();
        self
    }

    pub fn no_such_key(key: &str) -> Self {
        Self::new(
            StatusCode::NOT_FOUND,
            "NoSuchKey",
            "The specified key does not exist.",
        )
        .resource(key)
    }

    pub fn no_such_bucket(bucket: &str) -> Self {
        Self::new(
            StatusCode::NOT_FOUND,
            "NoSuchBucket",
            "The specified bucket does not exist",
        )
        .resource(bucket)
    }

    pub fn invalid_bucket_name(bucket: &str) -> Self {
        Self::new(
            StatusCode::BAD_REQUEST,
            "InvalidBucketName",
            "The specified bucket is not valid.",
        )
        .resource(bucket)
    }

    pub fn invalid_range(key: &str) -> Self {
        Self::new(
            StatusCode::RANGE_NOT_SATISFIABLE,
            "InvalidRange",
            "The requested range is not satisfiable",
        )
        .resource(key)
    }

    pub fn precondition_failed(key: &str) -> Self {
        Self::new(
            StatusCode::PRECONDITION_FAILED,
            "PreconditionFailed",
            "At least one of the pre-conditions you specified did not hold",
        )
        .resource(key)
    }

    pub fn access_denied(message: impl Into<String>) -> Self {
        Self::new(StatusCode::FORBIDDEN, "AccessDenied", message)
    }

    pub fn signature_mismatch() -> Self {
        Self::new(
            StatusCode::FORBIDDEN,
            "SignatureDoesNotMatch",
            "The request signature we calculated does not match the signature you provided.",
        )
    }

    pub fn invalid_access_key() -> Self {
        Self::new(
            StatusCode::FORBIDDEN,
            "InvalidAccessKeyId",
            "The AWS Access Key Id you provided does not exist in our records.",
        )
    }

    pub fn request_time_too_skewed() -> Self {
        Self::new(
            StatusCode::FORBIDDEN,
            "RequestTimeTooSkewed",
            "The difference between the request time and the server's time is too large.",
        )
    }

    pub fn expired() -> Self {
        Self::new(StatusCode::FORBIDDEN, "AccessDenied", "Request has expired")
    }

    pub fn invalid_request(message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, "InvalidRequest", message)
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, "InternalError", message)
    }

    pub fn bad_gateway(message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_GATEWAY, "InternalError", message)
    }

    pub fn from_nestor(e: &NestorError, key: &str) -> Self {
        if e.is_not_found() {
            return Self::no_such_key(key);
        }
        if e.is_stale() {
            return Self::precondition_failed(key);
        }
        match e {
            NestorError::Range(..) => Self::invalid_range(key),
            NestorError::UnknownNamespace => Self::no_such_bucket(key),
            NestorError::Closed => Self::new(
                StatusCode::SERVICE_UNAVAILABLE,
                "ServiceUnavailable",
                "cache is shutting down",
            ),
            other => Self::bad_gateway(other.to_string()),
        }
    }

    pub fn body(&self) -> String {
        let mut out = String::with_capacity(256);
        out.push_str("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error><Code>");
        out.push_str(self.code);
        out.push_str("</Code><Message>");
        escape_into(&mut out, &self.message);
        out.push_str("</Message><Resource>");
        escape_into(&mut out, &self.resource);
        out.push_str("</Resource></Error>");
        out
    }
}

fn escape_into(out: &mut String, text: &str) {
    for ch in text.chars() {
        match ch {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&apos;"),
            c => out.push(c),
        }
    }
}

impl std::fmt::Display for S3Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {}: {}", self.status, self.code, self.message)
    }
}

impl std::error::Error for S3Error {}

impl IntoResponse for S3Error {
    fn into_response(self) -> Response {
        let body = self.body();
        Response::builder()
            .status(self.status)
            .header(CONTENT_TYPE, "application/xml")
            .header(CONTENT_LENGTH, body.len())
            .body(Body::from(body))
            .expect("static response is valid")
    }
}
