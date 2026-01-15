//! RDF support for RO-Crate (RDF ↔ RO-Crate conversion).
//!
//! The RO-Crate version is auto-detected from the context URL.
//! Requires the `rdf` feature flag.
//!
//! ```toml
//! [dependencies]
//! ro-crate-rs = { version = "0.4", features = ["rdf"] }
//! ```

pub mod context;
pub mod convert;
pub mod error;
pub mod graph;
pub mod rdf_io;
pub mod resolver;

// Re-exports
pub use context::ResolvedContext;
pub use convert::{rocrate_to_rdf, rocrate_to_rdf_with_options, ConversionOptions};
pub use error::{ContextError, RdfError};
pub use graph::RdfGraph;
pub use rdf_io::{rdf_graph_to_rocrate, rdf_to_rocrate, RdfFormat};
pub use resolver::ContextResolverBuilder;
