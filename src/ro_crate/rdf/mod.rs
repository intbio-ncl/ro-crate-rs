//! RDF support for RO-Crate.
//!
//! This module provides functionality for converting RO-Crates to RDF triples.
//! The RO-Crate version (1.1 or 1.2) is auto-detected from the context URL.
//!
//! # Feature Flag
//!
//! RDF conversion requires the `rdf` feature:
//!
//! ```toml
//! [dependencies]
//! ro-crate-rs = { version = "0.4", features = ["rdf"] }
//! ```
//!
//! # Example
//!
//! ```ignore
//! use rocraters::ro_crate::rdf::{rocrate_to_rdf, ContextResolverBuilder};
//!
//! // Convert an RO-Crate to RDF triples
//! let graph = rocrate_to_rdf(&rocrate, ContextResolverBuilder::default())?;
//!
//! println!("Generated {} triples", graph.len());
//! for triple in graph.iter() {
//!     println!("{}", triple);
//! }
//! ```

pub mod context;
pub mod convert;
pub mod error;
pub mod graph;
pub mod resolver;

// Re-exports
pub use context::ResolvedContext;
pub use convert::rocrate_to_rdf;
pub use error::{ContextError, RdfError};
pub use graph::RdfGraph;
pub use resolver::ContextResolverBuilder;
