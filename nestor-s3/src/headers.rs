//! HTTP header parsing and formatting for GET and HEAD.

use std::ops::Range;
use std::time::SystemTime;

use chrono::{DateTime, Utc};
use http::header::{
    ACCEPT_RANGES, CONTENT_LENGTH, CONTENT_RANGE, ETAG, HeaderMap, HeaderName, HeaderValue,
    IF_MATCH, IF_MODIFIED_SINCE, IF_NONE_MATCH, IF_UNMODIFIED_SINCE, LAST_MODIFIED, RANGE,
};
use nestor::{FetchOverrides, ObjectMeta, Preconditions, ReadRange};

use crate::error::S3Error;

pub const X_NESTOR_FETCH: HeaderName = HeaderName::from_static("x-nestor-fetch");

pub fn fetch_overrides(headers: &HeaderMap) -> Result<FetchOverrides, S3Error> {
    let Some(value) = headers.get(&X_NESTOR_FETCH) else {
        return Ok(FetchOverrides::default());
    };
    let text = value
        .to_str()
        .map_err(|_| S3Error::invalid_request("x-nestor-fetch is not valid text"))?;
    FetchOverrides::parse(text)
        .map_err(|e| S3Error::invalid_request(format!("x-nestor-fetch: {e}")))
}

pub fn parse_range(headers: &HeaderMap) -> Option<ReadRange> {
    let value = headers.get(RANGE)?.to_str().ok()?.trim();
    let spec = value.strip_prefix("bytes=")?.trim();
    if spec.contains(',') {
        return None;
    }
    let (start, end) = spec.split_once('-')?;
    match (start.trim(), end.trim()) {
        ("", suffix) => suffix.parse().ok().map(ReadRange::Suffix),
        (start, "") => start.parse().ok().map(ReadRange::From),
        (start, end) => {
            let start: u64 = start.parse().ok()?;
            let end: u64 = end.parse().ok()?;
            (start <= end).then(|| ReadRange::Bounded(start..end.saturating_add(1)))
        }
    }
}

pub fn http_date(time: SystemTime) -> HeaderValue {
    let dt: DateTime<Utc> = time.into();
    HeaderValue::from_str(&dt.format("%a, %d %b %Y %H:%M:%S GMT").to_string())
        .expect("formatted date is ascii")
}

pub fn parse_http_date(value: &HeaderValue) -> Option<SystemTime> {
    let text = value.to_str().ok()?;
    DateTime::parse_from_rfc2822(text)
        .ok()
        .map(|dt| dt.with_timezone(&Utc).into())
}

pub fn preconditions(headers: &HeaderMap) -> Preconditions {
    let text = |name| headers.get(name)?.to_str().ok().map(str::to_owned);
    let date = |name| headers.get(name).and_then(parse_http_date);
    Preconditions {
        if_match: text(IF_MATCH),
        if_none_match: text(IF_NONE_MATCH),
        if_modified_since: date(IF_MODIFIED_SINCE),
        if_unmodified_since: date(IF_UNMODIFIED_SINCE),
    }
}

pub fn object_headers(out: &mut HeaderMap, meta: &ObjectMeta) {
    out.insert(ACCEPT_RANGES, HeaderValue::from_static("bytes"));
    if let Some(etag) = meta.etag.as_ref()
        && let Ok(v) = HeaderValue::from_bytes(etag)
    {
        out.insert(ETAG, v);
    }
    if let Some(modified) = meta.last_modified {
        out.insert(LAST_MODIFIED, http_date(modified));
    }
}

pub fn content_headers(out: &mut HeaderMap, range: &Range<u64>, size: u64, partial: bool) {
    out.insert(CONTENT_LENGTH, HeaderValue::from(range.end - range.start));
    if partial {
        let value = format!("bytes {}-{}/{}", range.start, range.end - 1, size);
        out.insert(CONTENT_RANGE, HeaderValue::from_str(&value).expect("ascii"));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn headers(name: http::header::HeaderName, value: &str) -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert(name, HeaderValue::from_str(value).unwrap());
        h
    }

    #[test]
    fn parses_range_forms() {
        assert_eq!(
            parse_range(&headers(RANGE, "bytes=0-99")),
            Some(ReadRange::Bounded(0..100))
        );
        assert_eq!(
            parse_range(&headers(RANGE, "bytes=500-")),
            Some(ReadRange::From(500))
        );
        assert_eq!(
            parse_range(&headers(RANGE, "bytes=-42")),
            Some(ReadRange::Suffix(42))
        );
        assert_eq!(parse_range(&headers(RANGE, "bytes=0-1,5-9")), None);
        assert_eq!(parse_range(&headers(RANGE, "items=0-1")), None);
        assert_eq!(parse_range(&headers(RANGE, "bytes=9-1")), None);
        assert_eq!(parse_range(&HeaderMap::new()), None);
    }

    #[test]
    fn preconditions_from_headers() {
        let now = SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(1_700_000_000);
        let mut h = headers(IF_MATCH, "\"abc\"");
        h.insert(IF_MODIFIED_SINCE, http_date(now));
        let parsed = preconditions(&h);
        assert_eq!(parsed.if_match.as_deref(), Some("\"abc\""));
        assert_eq!(parsed.if_none_match, None);
        assert_eq!(parsed.if_modified_since, Some(now));
        assert_eq!(parsed.if_unmodified_since, None);
        assert!(preconditions(&HeaderMap::new()).is_empty());
    }

    #[test]
    fn fetch_overrides_from_header() {
        let parsed = fetch_overrides(&headers(X_NESTOR_FETCH, "attempts=2 hedge=off")).unwrap();
        assert_eq!(parsed.attempts, Some(2));
        assert_eq!(parsed.hedge, Some(false));
        assert!(fetch_overrides(&HeaderMap::new()).unwrap().is_empty());
        let err = fetch_overrides(&headers(X_NESTOR_FETCH, "deadline=never")).unwrap_err();
        assert_eq!(err.status, http::StatusCode::BAD_REQUEST);
    }

    #[test]
    fn formats_content_range() {
        let mut out = HeaderMap::new();
        content_headers(&mut out, &(10..20), 100, true);
        assert_eq!(out.get(CONTENT_LENGTH).unwrap(), "10");
        assert_eq!(out.get(CONTENT_RANGE).unwrap(), "bytes 10-19/100");
    }
}
