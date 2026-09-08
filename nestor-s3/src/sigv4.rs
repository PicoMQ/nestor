//! AWS Signature Version 4 primitives: canonicalisation, signing key derivation and `Authorization`
//! parsing.

use std::fmt::Write as _;
use std::sync::Mutex;

use chrono::{DateTime, Utc};
use hmac::{Hmac, KeyInit, Mac};
use http::header::{AUTHORIZATION, CONTENT_TYPE, HOST, HeaderName, HeaderValue, RANGE};
use http::{HeaderMap, Method, Uri};
use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, percent_decode_str, utf8_percent_encode};
use sha2::{Digest, Sha256};

pub const ALGORITHM: &str = "AWS4-HMAC-SHA256";
pub const SERVICE: &str = "s3";
pub const UNSIGNED_PAYLOAD: &str = "UNSIGNED-PAYLOAD";
pub const TERMINATOR: &str = "aws4_request";

type HmacSha256 = Hmac<Sha256>;

pub const X_AMZ_DATE: HeaderName = HeaderName::from_static("x-amz-date");
pub const X_AMZ_CONTENT_SHA256: HeaderName = HeaderName::from_static("x-amz-content-sha256");
pub const X_AMZ_SECURITY_TOKEN: HeaderName = HeaderName::from_static("x-amz-security-token");

#[derive(Debug, Clone, Copy)]
pub struct Credentials<'a> {
    pub access_key: &'a str,
    pub secret_key: &'a str,
    pub session_token: Option<&'a str>,
}

const STRICT: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'_')
    .remove(b'.')
    .remove(b'~');
const PATH: &AsciiSet = &STRICT.remove(b'/');

pub fn uri_encode(input: &str, encode_slash: bool) -> String {
    let set = if encode_slash { STRICT } else { PATH };
    utf8_percent_encode(input, set).to_string()
}

pub fn percent_decode(input: &str) -> String {
    percent_decode_str(input).decode_utf8_lossy().into_owned()
}

pub fn canonical_uri(raw_path: &str) -> String {
    let path = if raw_path.is_empty() { "/" } else { raw_path };
    uri_encode(&percent_decode(path), false)
}

pub fn canonical_query(raw_query: &str, skip: Option<&str>) -> String {
    let mut pairs: Vec<(String, String)> = raw_query
        .split('&')
        .filter(|p| !p.is_empty())
        .map(|pair| {
            let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
            (
                uri_encode(&percent_decode(k), true),
                uri_encode(&percent_decode(v), true),
            )
        })
        .filter(|(k, _)| skip.is_none_or(|s| !k.eq_ignore_ascii_case(s)))
        .collect();
    pairs.sort();
    let mut out = String::with_capacity(raw_query.len());
    for (i, (k, v)) in pairs.iter().enumerate() {
        if i > 0 {
            out.push('&');
        }
        out.push_str(k);
        out.push('=');
        out.push_str(v);
    }
    out
}

fn normalize_value(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    let mut last_space = false;
    for ch in value.trim().chars() {
        if ch == ' ' || ch == '\t' {
            if !last_space {
                out.push(' ');
            }
            last_space = true;
        } else {
            out.push(ch);
            last_space = false;
        }
    }
    out
}

pub fn canonical_headers(headers: &HeaderMap, signed: &[String], host: &str) -> String {
    let mut out = String::new();
    for name in signed {
        let value = if name == "host" {
            normalize_value(host)
        } else {
            let mut values: Vec<String> = headers
                .get_all(name.as_str())
                .iter()
                .map(|v| normalize_value(&String::from_utf8_lossy(v.as_bytes())))
                .collect();
            values.sort();
            values.join(",")
        };
        out.push_str(name);
        out.push(':');
        out.push_str(&value);
        out.push('\n');
    }
    out
}

pub struct Canonical<'a> {
    pub method: &'a str,
    pub raw_path: &'a str,
    pub raw_query: &'a str,
    pub headers: &'a HeaderMap,
    pub host: &'a str,
    pub signed_headers: &'a [String],
    pub payload_hash: &'a str,
    pub skip_query: Option<&'a str>,
}

pub fn canonical_request(c: &Canonical<'_>) -> String {
    let mut out = String::with_capacity(512);
    out.push_str(c.method);
    out.push('\n');
    out.push_str(&canonical_uri(c.raw_path));
    out.push('\n');
    out.push_str(&canonical_query(c.raw_query, c.skip_query));
    out.push('\n');
    out.push_str(&canonical_headers(c.headers, c.signed_headers, c.host));
    out.push('\n');
    out.push_str(&c.signed_headers.join(";"));
    out.push('\n');
    out.push_str(c.payload_hash);
    out
}

pub fn sha256_hex(data: &[u8]) -> String {
    hex::encode(Sha256::digest(data))
}

pub fn scope(date: &str, region: &str) -> String {
    format!("{date}/{region}/{SERVICE}/{TERMINATOR}")
}

pub fn string_to_sign(amz_date: &str, scope: &str, canonical: &str) -> String {
    let mut out = String::with_capacity(256);
    out.push_str(ALGORITHM);
    out.push('\n');
    out.push_str(amz_date);
    out.push('\n');
    out.push_str(scope);
    out.push('\n');
    out.push_str(&sha256_hex(canonical.as_bytes()));
    out
}

fn hmac(key: &[u8], data: &[u8]) -> [u8; 32] {
    let mut mac = HmacSha256::new_from_slice(key).expect("hmac accepts any key length");
    mac.update(data);
    mac.finalize().into_bytes().into()
}

pub fn signing_key(secret: &str, date: &str, region: &str) -> [u8; 32] {
    let mut secret_key = String::with_capacity(4 + secret.len());
    secret_key.push_str("AWS4");
    secret_key.push_str(secret);
    let k_date = hmac(secret_key.as_bytes(), date.as_bytes());
    let k_region = hmac(&k_date, region.as_bytes());
    let k_service = hmac(&k_region, SERVICE.as_bytes());
    hmac(&k_service, TERMINATOR.as_bytes())
}

pub fn signature(key: &[u8; 32], string_to_sign: &str) -> String {
    hex::encode(hmac(key, string_to_sign.as_bytes()))
}

const SIGNING_KEYS_KEPT: usize = 8;

struct SigningKey {
    secret: String,
    day: String,
    region: String,
    key: [u8; 32],
}

/// Derived signing keys. A key is fixed for a secret, day and region, so the four HMACs of
/// `signing_key` run once per day instead of once per request.
#[derive(Default)]
pub struct SigningKeys {
    entries: Mutex<Vec<SigningKey>>,
}

impl SigningKeys {
    pub fn get(&self, secret: &str, day: &str, region: &str) -> [u8; 32] {
        let mut entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(entry) = entries
            .iter()
            .find(|e| e.day == day && e.region == region && e.secret == secret)
        {
            return entry.key;
        }
        let key = signing_key(secret, day, region);
        if entries.len() == SIGNING_KEYS_KEPT {
            entries.remove(0);
        }
        entries.push(SigningKey {
            secret: secret.to_owned(),
            day: day.to_owned(),
            region: region.to_owned(),
            key,
        });
        key
    }
}

impl std::fmt::Debug for SigningKeys {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SigningKeys").finish_non_exhaustive()
    }
}

pub fn amz_date(now: DateTime<Utc>) -> String {
    now.format("%Y%m%dT%H%M%SZ").to_string()
}

pub fn parse_amz_date(value: &str) -> Option<DateTime<Utc>> {
    chrono::NaiveDateTime::parse_from_str(value, "%Y%m%dT%H%M%SZ")
        .ok()
        .map(|dt| dt.and_utc())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Authorization {
    pub access_key: String,
    pub date: String,
    pub region: String,
    pub signed_headers: Vec<String>,
    pub signature: String,
}

impl Authorization {
    pub fn parse_header(value: &str) -> Option<Self> {
        let rest = value.strip_prefix(ALGORITHM)?.trim_start();
        let mut credential = None;
        let mut signed_headers = None;
        let mut signature = None;
        for part in rest.split(',') {
            let (k, v) = part.trim().split_once('=')?;
            match k {
                "Credential" => credential = Some(v),
                "SignedHeaders" => signed_headers = Some(v),
                "Signature" => signature = Some(v),
                _ => return None,
            }
        }
        Self::assemble(credential?, signed_headers?, signature?)
    }

    pub fn from_query(credential: &str, signed_headers: &str, signature: &str) -> Option<Self> {
        Self::assemble(credential, signed_headers, signature)
    }

    fn assemble(credential: &str, signed_headers: &str, signature: &str) -> Option<Self> {
        let mut parts = credential.split('/');
        let access_key = parts.next()?.to_owned();
        let date = parts.next()?.to_owned();
        let region = parts.next()?.to_owned();
        if parts.next()? != SERVICE || parts.next()? != TERMINATOR || parts.next().is_some() {
            return None;
        }
        Some(Self {
            access_key,
            date,
            region,
            signed_headers: signed_headers
                .split(';')
                .map(str::to_ascii_lowercase)
                .collect(),
            signature: signature.to_owned(),
        })
    }

    pub fn header_value(&self) -> String {
        let mut out = String::with_capacity(256);
        let _ = write!(
            out,
            "{ALGORITHM} Credential={}/{}, SignedHeaders={}, Signature={}",
            self.access_key,
            scope(&self.date, &self.region),
            self.signed_headers.join(";"),
            self.signature
        );
        out
    }
}

impl SigningKeys {
    /// Signs `headers` in place with an unsigned payload, the way the forwarder re-signs requests
    /// for the origin. The `Host` header must already be set.
    pub fn sign(
        &self,
        method: &Method,
        uri: &Uri,
        headers: &mut HeaderMap,
        creds: Credentials<'_>,
        region: &str,
        now: DateTime<Utc>,
    ) {
        let host = headers
            .get(HOST)
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default()
            .to_owned();
        let date = amz_date(now);
        let day = date[..8].to_owned();
        headers.insert(
            X_AMZ_DATE,
            HeaderValue::from_str(&date).expect("amz date is ascii"),
        );
        headers.insert(
            X_AMZ_CONTENT_SHA256,
            HeaderValue::from_static(UNSIGNED_PAYLOAD),
        );
        if let Some(token) = creds.session_token
            && let Ok(v) = HeaderValue::from_str(token)
        {
            headers.insert(X_AMZ_SECURITY_TOKEN, v);
        }

        let mut signed: Vec<String> = headers
            .keys()
            .filter(|n| {
                let s = n.as_str();
                s.starts_with("x-amz-")
                    || *n == HOST
                    || *n == CONTENT_TYPE
                    || s == "content-md5"
                    || *n == RANGE
            })
            .map(|n| n.as_str().to_owned())
            .collect();
        signed.sort();
        signed.dedup();

        let canonical = canonical_request(&Canonical {
            method: method.as_str(),
            raw_path: uri.path(),
            raw_query: uri.query().unwrap_or(""),
            headers,
            host: &host,
            signed_headers: &signed,
            payload_hash: UNSIGNED_PAYLOAD,
            skip_query: None,
        });
        let sts = string_to_sign(&date, &scope(&day, region), &canonical);
        let key = self.get(creds.secret_key, &day, region);
        let auth = Authorization {
            access_key: creds.access_key.to_owned(),
            date: day,
            region: region.to_owned(),
            signed_headers: signed,
            signature: signature(&key, &sts),
        };
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&auth.header_value()).expect("authorization is ascii"),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encodes_like_aws() {
        assert_eq!(uri_encode("a b/c~d-_.", false), "a%20b/c~d-_.");
        assert_eq!(uri_encode("a/b+c", true), "a%2Fb%2Bc");
        assert_eq!(canonical_uri("/bucket/a%20b"), "/bucket/a%20b");
        assert_eq!(canonical_uri("/bucket/a b"), "/bucket/a%20b");
        assert_eq!(canonical_uri(""), "/");
    }

    #[test]
    fn canonical_query_sorts_and_encodes() {
        assert_eq!(
            canonical_query("prefix=a%2Fb&delimiter=/&list-type=2&marker", None),
            "delimiter=%2F&list-type=2&marker=&prefix=a%2Fb"
        );
        assert_eq!(
            canonical_query("X-Amz-Signature=abc&a=1", Some("X-Amz-Signature")),
            "a=1"
        );
    }

    #[test]
    fn aws_documented_signing_key() {
        let key = signing_key(
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            "20130524",
            "us-east-1",
        );
        let sts = "AWS4-HMAC-SHA256\n20130524T000000Z\n20130524/us-east-1/s3/aws4_request\n7344ae5b7ee6c3e7e6b0fe0640412a37625d1fbfff95c48bbb2dc43964946972";
        assert_eq!(
            signature(&key, sts),
            "f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"
        );
    }

    #[test]
    fn signing_keys_are_derived_once_per_scope() {
        let keys = SigningKeys::default();
        let secret = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
        let first = keys.get(secret, "20130524", "us-east-1");
        assert_eq!(first, signing_key(secret, "20130524", "us-east-1"));
        assert_eq!(keys.get(secret, "20130524", "us-east-1"), first);
        assert_ne!(keys.get(secret, "20130525", "us-east-1"), first);
        assert_eq!(keys.entries.lock().unwrap().len(), 2);
        for day in 0..SIGNING_KEYS_KEPT {
            keys.get(secret, &format!("2014010{day}"), "eu-west-1");
        }
        assert_eq!(keys.entries.lock().unwrap().len(), SIGNING_KEYS_KEPT);
    }

    #[test]
    fn parses_authorization_header() {
        let header = "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, SignedHeaders=host;range;x-amz-content-sha256;x-amz-date, Signature=fe5f80f77d5fa3beca038a248ff027d0445342fe2855ddc963176630326f1024";
        let auth = Authorization::parse_header(header).unwrap();
        assert_eq!(auth.access_key, "AKIAIOSFODNN7EXAMPLE");
        assert_eq!(auth.date, "20130524");
        assert_eq!(auth.region, "us-east-1");
        assert_eq!(
            auth.signed_headers,
            vec!["host", "range", "x-amz-content-sha256", "x-amz-date"]
        );
        assert_eq!(auth.header_value(), header);
        assert!(Authorization::parse_header("AWS abc:def").is_none());
    }

    #[test]
    fn aws_documented_get_object_example() {
        let mut headers = HeaderMap::new();
        headers.insert("range", "bytes=0-9".parse().unwrap());
        headers.insert(
            "x-amz-content-sha256",
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
                .parse()
                .unwrap(),
        );
        headers.insert("x-amz-date", "20130524T000000Z".parse().unwrap());
        let signed = vec![
            "host".to_owned(),
            "range".to_owned(),
            "x-amz-content-sha256".to_owned(),
            "x-amz-date".to_owned(),
        ];
        let canonical = canonical_request(&Canonical {
            method: "GET",
            raw_path: "/test.txt",
            raw_query: "",
            headers: &headers,
            host: "examplebucket.s3.amazonaws.com",
            signed_headers: &signed,
            payload_hash: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            skip_query: None,
        });
        let sts = string_to_sign(
            "20130524T000000Z",
            &scope("20130524", "us-east-1"),
            &canonical,
        );
        let key = signing_key(
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            "20130524",
            "us-east-1",
        );
        assert_eq!(
            signature(&key, &sts),
            "f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"
        );
    }
}
