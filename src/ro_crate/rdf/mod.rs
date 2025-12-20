//! RDF support for RO-Crate.
//!
//! # Feature Flag
//!
//! ```toml
//! [dependencies]
//! ro-crate-rs = { version = "0.4", features = ["rdf"] }
//! ```
//!
//! # Example
//!
//! ```ignore
//! use rocraters::ro_crate::rdf::ContextResolverBuilder;
//!
//! let resolved = ContextResolverBuilder::default()
//!     .resolve(&rocrate.context)?;
//!
//! let iri = resolved.expand_term("name");
//! ```

pub mod context;
pub mod error;
pub mod resolver;
