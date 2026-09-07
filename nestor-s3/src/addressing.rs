//! Bucket and key extraction from the request, path style or virtual hosted. Bucket names are
//! validated before they reach the origin.

use crate::error::S3Error;
use crate::sigv4::percent_decode;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Addressing {
    Path,
    VirtualHosted { domain: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Target {
    pub bucket: Option<String>,
    pub key: Option<String>,
}

impl Addressing {
    pub fn resolve(&self, host: &str, raw_path: &str) -> Result<Target, S3Error> {
        let host = host.split(':').next().unwrap_or(host);
        let path = raw_path.strip_prefix('/').unwrap_or(raw_path);
        if let Self::VirtualHosted { domain } = self
            && let Some(bucket) = host.strip_suffix(domain.as_str())
            && let Some(bucket) = bucket.strip_suffix('.')
            && !bucket.is_empty()
        {
            return Ok(Target {
                bucket: Some(validated_bucket(bucket)?),
                key: non_empty(path).map(percent_decode),
            });
        }
        let (bucket, key) = match path.split_once('/') {
            Some((bucket, key)) => (bucket, non_empty(key)),
            None => (path, None),
        };
        Ok(Target {
            bucket: non_empty(bucket).map(validated_bucket).transpose()?,
            key: key.map(percent_decode),
        })
    }
}

fn validated_bucket(name: &str) -> Result<String, S3Error> {
    let bytes = name.as_bytes();
    let well_formed = (3..=63).contains(&bytes.len())
        && bytes
            .iter()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || *b == b'-' || *b == b'.')
        && bytes.first().is_some_and(u8::is_ascii_alphanumeric)
        && bytes.last().is_some_and(u8::is_ascii_alphanumeric)
        && !name.contains("..");
    if well_formed {
        Ok(name.to_owned())
    } else {
        Err(S3Error::invalid_bucket_name(name))
    }
}

fn non_empty(s: &str) -> Option<&str> {
    (!s.is_empty()).then_some(s)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn path_style() {
        let a = Addressing::Path;
        assert_eq!(
            a.resolve("localhost:9000", "/bucket/dir/a%20b.txt")
                .unwrap(),
            Target {
                bucket: Some("bucket".into()),
                key: Some("dir/a b.txt".into()),
            }
        );
        assert_eq!(
            a.resolve("localhost", "/bucket").unwrap(),
            Target {
                bucket: Some("bucket".into()),
                key: None,
            }
        );
        assert_eq!(
            a.resolve("localhost", "/bucket/").unwrap(),
            Target {
                bucket: Some("bucket".into()),
                key: None,
            }
        );
        assert_eq!(
            a.resolve("localhost", "/").unwrap(),
            Target {
                bucket: None,
                key: None,
            }
        );
    }

    #[test]
    fn rejects_malformed_bucket_names() {
        let a = Addressing::Path;
        for name in [
            "ab",
            "Upper",
            "has_underscore",
            "-leading",
            "trailing-",
            "a..b",
            "evil.host:80",
        ] {
            let err = a.resolve("localhost", &format!("/{name}/key")).unwrap_err();
            assert_eq!(err.code, "InvalidBucketName", "{name}");
        }
        let v = Addressing::VirtualHosted {
            domain: "s3.local".into(),
        };
        assert!(v.resolve("Bad_Bucket.s3.local", "/k").is_err());
    }

    #[test]
    fn virtual_hosted_falls_back_to_path() {
        let a = Addressing::VirtualHosted {
            domain: "s3.local".into(),
        };
        assert_eq!(
            a.resolve("photos.s3.local:8080", "/2026/x.jpg").unwrap(),
            Target {
                bucket: Some("photos".into()),
                key: Some("2026/x.jpg".into()),
            }
        );
        assert_eq!(
            a.resolve("s3.local", "/photos/2026/x.jpg").unwrap(),
            Target {
                bucket: Some("photos".into()),
                key: Some("2026/x.jpg".into()),
            }
        );
    }
}
