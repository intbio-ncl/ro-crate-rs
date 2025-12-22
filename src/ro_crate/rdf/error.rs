//! Error types for RDF conversion.

use std::fmt;

/// Errors that can occur during context resolution.
#[derive(Debug)]
pub enum ContextError {
    /// A required context was not found in the cache and remote fetching is disabled.
    MissingContext(String),
    /// Failed to fetch a remote context.
    FetchFailed { url: String, reason: String },
    /// Failed to parse JSON content.
    JsonParseError(String),
    /// The context document is invalid.
    InvalidContext(String),
}

impl fmt::Display for ContextError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ContextError::MissingContext(url) => {
                write!(f, "Context not found: {}", url)
            }
            ContextError::FetchFailed { url, reason } => {
                write!(f, "Failed to fetch context from {}: {}", url, reason)
            }
            ContextError::JsonParseError(msg) => {
                write!(f, "JSON parse error: {}", msg)
            }
            ContextError::InvalidContext(msg) => {
                write!(f, "Invalid context: {}", msg)
            }
        }
    }
}

impl std::error::Error for ContextError {}

/// Errors that can occur during RDF conversion.
#[derive(Debug)]
pub enum RdfError {
    /// Context resolution failed.
    Context(ContextError),
    /// Invalid IRI encountered.
    InvalidIri(String),
    /// Blank nodes are not supported.
    BlankNode(String),
}

impl fmt::Display for RdfError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RdfError::Context(e) => write!(f, "Context error: {}", e),
            RdfError::InvalidIri(iri) => write!(f, "Invalid IRI: {}", iri),
            RdfError::BlankNode(id) => write!(f, "Blank nodes not supported: {}", id),
        }
    }
}

impl std::error::Error for RdfError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            RdfError::Context(e) => Some(e),
            _ => None,
        }
    }
}

impl From<ContextError> for RdfError {
    fn from(e: ContextError) -> Self {
        RdfError::Context(e)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_context_error_display() {
        let err = ContextError::MissingContext("http://example.org/ctx".to_string());
        assert!(err.to_string().contains("http://example.org/ctx"));

        let err = ContextError::FetchFailed {
            url: "http://example.org".to_string(),
            reason: "timeout".to_string(),
        };
        assert!(err.to_string().contains("timeout"));
    }

    #[test]
    fn test_rdf_error_display() {
        let err = RdfError::BlankNode("_:b0".to_string());
        assert!(err.to_string().contains("_:b0"));

        let err = RdfError::InvalidIri("not a valid iri".to_string());
        assert!(err.to_string().contains("not a valid iri"));
    }

    #[test]
    fn test_context_error_to_rdf_error() {
        let ctx_err = ContextError::MissingContext("test".to_string());
        let rdf_err: RdfError = ctx_err.into();
        assert!(matches!(rdf_err, RdfError::Context(_)));
    }
}
