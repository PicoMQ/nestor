//! Signature Version 4 verification of incoming requests, both the header and presigned query forms.

use chrono::{DateTime, Duration, Utc};
use http::{HeaderMap, Method, Uri};

use crate::error::S3Error;
use crate::sigv4::{
    Authorization, Canonical, UNSIGNED_PAYLOAD, canonical_request, parse_amz_date, percent_decode,
    scope, signature, signing_key, string_to_sign,
};

const MAX_SKEW_SECS: i64 = 15 * 60;
const MAX_PRESIGN_SECS: i64 = 7 * 24 * 3600;

#[derive(Debug, Clone)]
pub enum Auth {
    Anonymous,
    Static {
        access_key: String,
        secret_key: String,
    },
}

pub fn host_of(headers: &HeaderMap, uri: &Uri) -> String {
    headers
        .get(http::header::HOST)
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned)
        .or_else(|| uri.authority().map(|a| a.as_str().to_owned()))
        .unwrap_or_default()
}

pub fn query_param(raw_query: &str, name: &str) -> Option<String> {
    raw_query
        .split('&')
        .filter_map(|pair| pair.split_once('=').or(Some((pair, ""))))
        .find(|(k, _)| percent_decode(k) == name)
        .map(|(_, v)| percent_decode(v))
}

fn constant_time_eq(a: &str, b: &str) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.bytes()
        .zip(b.bytes())
        .fold(0u8, |acc, (x, y)| acc | (x ^ y))
        == 0
}

impl Auth {
    pub fn verify(
        &self,
        method: &Method,
        uri: &Uri,
        headers: &HeaderMap,
        now: DateTime<Utc>,
    ) -> Result<(), S3Error> {
        let Self::Static {
            access_key,
            secret_key,
        } = self
        else {
            return Ok(());
        };
        let raw_path = uri.path();
        let raw_query = uri.query().unwrap_or("");
        let host = host_of(headers, uri);

        let (auth, amz_date, payload_hash, skip_query) =
            if let Some(header) = headers.get(http::header::AUTHORIZATION) {
                let header = header
                    .to_str()
                    .map_err(|_| S3Error::invalid_request("malformed Authorization header"))?;
                let auth = Authorization::parse_header(header)
                    .ok_or_else(|| S3Error::invalid_request("unsupported Authorization header"))?;
                let amz_date = headers
                    .get("x-amz-date")
                    .or_else(|| headers.get(http::header::DATE))
                    .and_then(|v| v.to_str().ok())
                    .ok_or_else(|| S3Error::invalid_request("missing x-amz-date"))?
                    .to_owned();
                let payload_hash = headers
                    .get("x-amz-content-sha256")
                    .and_then(|v| v.to_str().ok())
                    .ok_or_else(|| S3Error::invalid_request("missing x-amz-content-sha256"))?
                    .to_owned();
                (auth, amz_date, payload_hash, None)
            } else if query_param(raw_query, "X-Amz-Algorithm").is_some() {
                let credential = query_param(raw_query, "X-Amz-Credential")
                    .ok_or_else(|| S3Error::invalid_request("missing X-Amz-Credential"))?;
                let signed = query_param(raw_query, "X-Amz-SignedHeaders")
                    .ok_or_else(|| S3Error::invalid_request("missing X-Amz-SignedHeaders"))?;
                let sig = query_param(raw_query, "X-Amz-Signature")
                    .ok_or_else(|| S3Error::invalid_request("missing X-Amz-Signature"))?;
                let amz_date = query_param(raw_query, "X-Amz-Date")
                    .ok_or_else(|| S3Error::invalid_request("missing X-Amz-Date"))?;
                let expires: i64 = query_param(raw_query, "X-Amz-Expires")
                    .and_then(|e| e.parse().ok())
                    .ok_or_else(|| S3Error::invalid_request("missing X-Amz-Expires"))?;
                if !(1..=MAX_PRESIGN_SECS).contains(&expires) {
                    return Err(S3Error::invalid_request("invalid X-Amz-Expires"));
                }
                let issued = parse_amz_date(&amz_date)
                    .ok_or_else(|| S3Error::invalid_request("invalid X-Amz-Date"))?;
                if issued + Duration::seconds(expires) < now {
                    return Err(S3Error::expired());
                }
                let auth = Authorization::from_query(&credential, &signed, &sig)
                    .ok_or_else(|| S3Error::invalid_request("invalid X-Amz-Credential"))?;
                (
                    auth,
                    amz_date,
                    UNSIGNED_PAYLOAD.to_owned(),
                    Some("X-Amz-Signature"),
                )
            } else {
                return Err(S3Error::access_denied("anonymous access is not allowed"));
            };

        if !constant_time_eq(&auth.access_key, access_key) {
            return Err(S3Error::invalid_access_key());
        }
        if skip_query.is_none() {
            let issued = parse_amz_date(&amz_date)
                .ok_or_else(|| S3Error::invalid_request("invalid x-amz-date"))?;
            if (now - issued).num_seconds().abs() > MAX_SKEW_SECS {
                return Err(S3Error::request_time_too_skewed());
            }
        }
        if !amz_date.starts_with(&auth.date) {
            return Err(S3Error::signature_mismatch());
        }

        let canonical = canonical_request(&Canonical {
            method: method.as_str(),
            raw_path,
            raw_query,
            headers,
            host: &host,
            signed_headers: &auth.signed_headers,
            payload_hash: &payload_hash,
            skip_query,
        });
        let sts = string_to_sign(&amz_date, &scope(&auth.date, &auth.region), &canonical);
        let key = signing_key(secret_key, &auth.date, &auth.region);
        let expected = signature(&key, &sts);
        if constant_time_eq(&expected, &auth.signature) {
            Ok(())
        } else {
            Err(S3Error::signature_mismatch())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::header::{AUTHORIZATION, HOST};

    const ACCESS: &str = "AKIAIOSFODNN7EXAMPLE";
    const SECRET: &str = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";

    fn auth() -> Auth {
        Auth::Static {
            access_key: ACCESS.into(),
            secret_key: SECRET.into(),
        }
    }

    fn now() -> DateTime<Utc> {
        parse_amz_date("20130524T000000Z").unwrap()
    }

    fn signed_headers() -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert(HOST, "examplebucket.s3.amazonaws.com".parse().unwrap());
        h.insert("range", "bytes=0-9".parse().unwrap());
        h.insert(
            "x-amz-content-sha256",
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
                .parse()
                .unwrap(),
        );
        h.insert("x-amz-date", "20130524T000000Z".parse().unwrap());
        h.insert(AUTHORIZATION, "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request,SignedHeaders=host;range;x-amz-content-sha256;x-amz-date,Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41".parse().unwrap());
        h
    }

    #[test]
    fn accepts_aws_documented_request() {
        let uri: Uri = "/test.txt".parse().unwrap();
        auth()
            .verify(&Method::GET, &uri, &signed_headers(), now())
            .unwrap();
    }

    #[test]
    fn rejects_tampering() {
        let uri: Uri = "/test.txt".parse().unwrap();
        let mut h = signed_headers();
        h.insert("range", "bytes=0-10".parse().unwrap());
        let err = auth().verify(&Method::GET, &uri, &h, now()).unwrap_err();
        assert_eq!(err.code, "SignatureDoesNotMatch");

        let other: Uri = "/other.txt".parse().unwrap();
        let err = auth()
            .verify(&Method::GET, &other, &signed_headers(), now())
            .unwrap_err();
        assert_eq!(err.code, "SignatureDoesNotMatch");

        let skewed = now() + Duration::hours(1);
        let err = auth()
            .verify(&Method::GET, &uri, &signed_headers(), skewed)
            .unwrap_err();
        assert_eq!(err.code, "RequestTimeTooSkewed");

        let err = auth()
            .verify(&Method::GET, &uri, &HeaderMap::new(), now())
            .unwrap_err();
        assert_eq!(err.code, "AccessDenied");
    }

    #[test]
    fn accepts_aws_documented_presigned_url() {
        let uri: Uri = "/test.txt?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20130524%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20130524T000000Z&X-Amz-Expires=86400&X-Amz-SignedHeaders=host&X-Amz-Signature=aeeed9bbccd4d02ee5c0109b86d86835f995330da4c265957d157751f604d404".parse().unwrap();
        let mut h = HeaderMap::new();
        h.insert(HOST, "examplebucket.s3.amazonaws.com".parse().unwrap());
        auth().verify(&Method::GET, &uri, &h, now()).unwrap();
        let err = auth()
            .verify(&Method::GET, &uri, &h, now() + Duration::days(2))
            .unwrap_err();
        assert_eq!(err.message, "Request has expired");
    }

    #[test]
    fn anonymous_mode_accepts_everything() {
        let uri: Uri = "/x".parse().unwrap();
        Auth::Anonymous
            .verify(&Method::PUT, &uri, &HeaderMap::new(), now())
            .unwrap();
    }
}
