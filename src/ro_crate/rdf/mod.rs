//! RDF support for RO-Crate.
//!
//! This module provides functionality for converting RO-Crates to/from RDF triples.
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
//! # Example: RO-Crate to RDF
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
//!
//! # Example: RDF to RO-Crate
//!
//! ```ignore
//! use rocraters::ro_crate::rdf::{rdf_to_rocrate, RdfFormat};
//!
//! // Parse RDF and convert to RO-Crate
//! let turtle_data = "..."; // Turtle format RDF
//! let rocrate = rdf_to_rocrate(turtle_data, RdfFormat::Turtle, None)?;
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
