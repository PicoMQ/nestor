//! Error types. `OriginError` is what an `Origin` reports, `NestorError` is what callers see.

use std::ops::Range;
use std::sync::Arc;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum OriginError {
    #[error("object not found")]
    NotFound,
    #[error("precondition failed")]
    PreconditionFailed,
    #[error("not modified")]
    NotModified,
    #[error("range not satisfiable")]
    InvalidRange,
    #[error("origin returned {got} bytes for a request of {expected}")]
    ShortRead { expected: u64, got: u64 },
    #[error(transparent)]
    Io(Box<dyn std::error::Error + Send + Sync + 'static>),
}

impl OriginError {
    pub fn io<E>(e: E) -> Self
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        Self::Io(Box::new(e))
    }

    pub fn is_retryable(&self) -> bool {
        matches!(self, Self::Io(_) | Self::ShortRead { .. })
    }
}

#[derive(Debug, Error)]
pub enum NestorError {
    #[error("object not found")]
    NotFound,
    #[error("unknown namespace")]
    UnknownNamespace,
    #[error("range {0:?} is not satisfiable for an object of {1} bytes")]
    Range(Range<u64>, u64),
    #[error("object changed while being read")]
    Stale,
    #[error("origin error: {0}")]
    Origin(#[source] OriginError),
    #[error("cache error: {0}")]
    Cache(#[source] foyer::Error),
    #[error("fetch was cancelled")]
    Cancelled,
    #[error("cache is closed")]
    Closed,
    #[error(transparent)]
    Shared(Arc<NestorError>),
}

impl NestorError {
    pub fn is_not_found(&self) -> bool {
        match self {
            Self::NotFound => true,
            Self::Shared(inner) => inner.is_not_found(),
            _ => false,
        }
    }

    pub fn is_stale(&self) -> bool {
        match self {
            Self::Stale => true,
            Self::Shared(inner) => inner.is_stale(),
            _ => false,
        }
    }
}

impl From<Arc<NestorError>> for NestorError {
    fn from(e: Arc<NestorError>) -> Self {
        Arc::try_unwrap(e).unwrap_or_else(Self::Shared)
    }
}

impl From<OriginError> for NestorError {
    fn from(e: OriginError) -> Self {
        match e {
            OriginError::NotFound => Self::NotFound,
            OriginError::PreconditionFailed => Self::Stale,
            other => Self::Origin(other),
        }
    }
}

impl From<foyer::Error> for NestorError {
    fn from(e: foyer::Error) -> Self {
        Self::Cache(e)
    }
}

pub type Result<T, E = NestorError> = std::result::Result<T, E>;
