//! The `Origin` trait a namespace reads from, the metadata it returns, and HTTP-style preconditions
//! evaluated against that metadata.

use std::ops::Range;
use std::time::SystemTime;

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;

use crate::error::OriginError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectMeta {
    pub size: u64,
    pub etag: Option<Bytes>,
    pub last_modified: Option<SystemTime>,
}

impl ObjectMeta {
    pub fn new(size: u64, etag: Option<Bytes>) -> Self {
        Self {
            size,
            etag,
            last_modified: Some(SystemTime::now()),
        }
    }

    pub fn etag_matches(&self, candidates: &str) -> bool {
        let etag = self
            .etag
            .as_deref()
            .and_then(|e| std::str::from_utf8(e).ok())
            .map(|e| e.trim_start_matches("W/"));
        candidates.split(',').map(str::trim).any(|candidate| {
            candidate == "*" || etag.is_some_and(|e| candidate.trim_start_matches("W/") == e)
        })
    }

    fn modified_after(&self, instant: SystemTime) -> Option<bool> {
        let modified = whole_seconds(self.last_modified?);
        Some(modified > whole_seconds(instant))
    }
}

fn whole_seconds(instant: SystemTime) -> u64 {
    instant
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Preconditions {
    pub if_match: Option<String>,
    pub if_none_match: Option<String>,
    pub if_modified_since: Option<SystemTime>,
    pub if_unmodified_since: Option<SystemTime>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Precondition {
    Satisfied,
    NotModified,
    Failed,
}

impl Preconditions {
    pub fn is_empty(&self) -> bool {
        *self == Self::default()
    }

    pub fn evaluate(&self, meta: &ObjectMeta) -> Precondition {
        if let Some(candidates) = &self.if_match
            && !meta.etag_matches(candidates)
        {
            return Precondition::Failed;
        }
        if let Some(since) = self.if_unmodified_since
            && meta.modified_after(since) == Some(true)
        {
            return Precondition::Failed;
        }
        if let Some(candidates) = &self.if_none_match {
            return if meta.etag_matches(candidates) {
                Precondition::NotModified
            } else {
                Precondition::Satisfied
            };
        }
        if let Some(since) = self.if_modified_since
            && meta.modified_after(since) == Some(false)
        {
            return Precondition::NotModified;
        }
        Precondition::Satisfied
    }
}

#[derive(Debug, Clone, Default)]
pub struct GetOptions {
    pub range: Option<Range<u64>>,
    pub if_match: Option<Bytes>,
    pub if_none_match: Option<Bytes>,
}

pub struct GetResponse {
    pub meta: ObjectMeta,
    pub range: Range<u64>,
    pub body: BoxStream<'static, Result<Bytes, OriginError>>,
}

#[async_trait]
pub trait Origin: Send + Sync + 'static {
    async fn get(&self, object: &str, options: GetOptions) -> Result<GetResponse, OriginError>;

    async fn head(&self, object: &str) -> Result<ObjectMeta, OriginError>;
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{ObjectMeta, Precondition, Preconditions, SystemTime};
    use bytes::Bytes;

    fn meta(modified: SystemTime) -> ObjectMeta {
        ObjectMeta {
            size: 10,
            etag: Some(Bytes::from_static(b"\"abc\"")),
            last_modified: Some(modified),
        }
    }

    fn with(f: impl FnOnce(&mut Preconditions)) -> Preconditions {
        let mut p = Preconditions::default();
        f(&mut p);
        p
    }

    #[test]
    fn etag_candidates() {
        let m = meta(SystemTime::UNIX_EPOCH);
        assert!(m.etag_matches("\"abc\""));
        assert!(m.etag_matches("\"zzz\", \"abc\""));
        assert!(m.etag_matches("W/\"abc\""));
        assert!(m.etag_matches("*"));
        assert!(!m.etag_matches("\"zzz\""));
        let untagged = ObjectMeta::new(1, None);
        assert!(untagged.etag_matches("*"));
        assert!(!untagged.etag_matches("\"abc\""));
    }

    #[test]
    fn evaluation_order() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let earlier = now - Duration::from_secs(60);
        let m = meta(now);
        let eval = |p: Preconditions| p.evaluate(&m);

        assert_eq!(eval(Preconditions::default()), Precondition::Satisfied);
        assert_eq!(
            eval(with(|p| p.if_none_match = Some("\"abc\"".into()))),
            Precondition::NotModified
        );
        assert_eq!(
            eval(with(|p| p.if_none_match = Some("\"zzz\"".into()))),
            Precondition::Satisfied
        );
        assert_eq!(
            eval(with(|p| p.if_match = Some("\"zzz\"".into()))),
            Precondition::Failed
        );
        assert_eq!(
            eval(with(|p| p.if_match = Some("*".into()))),
            Precondition::Satisfied
        );
        assert_eq!(
            eval(with(|p| p.if_modified_since = Some(now))),
            Precondition::NotModified
        );
        assert_eq!(
            eval(with(|p| p.if_modified_since = Some(earlier))),
            Precondition::Satisfied
        );
        assert_eq!(
            eval(with(|p| p.if_unmodified_since = Some(earlier))),
            Precondition::Failed
        );
        assert_eq!(
            eval(with(|p| {
                p.if_match = Some("\"zzz\"".into());
                p.if_none_match = Some("\"abc\"".into());
            })),
            Precondition::Failed
        );
        assert_eq!(
            eval(with(|p| {
                p.if_none_match = Some("\"zzz\"".into());
                p.if_modified_since = Some(now);
            })),
            Precondition::Satisfied
        );
    }
}
