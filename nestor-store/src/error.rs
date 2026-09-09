//! Error mapping between `object_store::Error` and Nestor's error types.

use nestor::{NestorError, OriginError};
use object_store::Error;

const STORE: &str = "nestor";

pub(crate) fn from_store(e: Error) -> OriginError {
    match e {
        Error::NotFound { .. } => OriginError::NotFound,
        Error::Precondition { .. } => OriginError::PreconditionFailed,
        Error::NotModified { .. } => OriginError::NotModified,
        other => OriginError::io(other),
    }
}

pub(crate) fn to_store(e: NestorError, path: &str) -> Error {
    if e.is_not_found() {
        return Error::NotFound {
            path: path.to_owned(),
            source: Box::new(e),
        };
    }
    if e.is_stale() {
        return Error::Precondition {
            path: path.to_owned(),
            source: Box::new(e),
        };
    }
    match e {
        NestorError::Origin(OriginError::Io(source)) => Error::Generic {
            store: STORE,
            source,
        },
        other => Error::Generic {
            store: STORE,
            source: Box::new(other),
        },
    }
}
