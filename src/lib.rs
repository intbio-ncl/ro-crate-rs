//! # ro-crate-rs
//!
//! 'ro-crate-rs' is a rust library for defining RO-Crates
//! (<https://www.researchobject.org/ro-crate/1.2/>) for research data. It enables
//! the reading, creation, modification, writing and archiving of ro-crate-metadata.json
//! files.
//!
//! This is not meant to be used for rapid prototyping, but more so for defined
//! pipelines, regular workflows or cases where validation or performance is
//! required. For rapid prototyping, either use the Python bindings for this library
//! or the native python library for RO-Crates (<https://github.com/ResearchObject/ro-crate-py/>).
//!
//! # Usage
//!
//! Create a basic crate with no entities:
//!
//! ```rust
//! let mut rocrate = RoCrate {
//!     context: RoCrateContext::ReferenceContext(
//!         "https://w3id.org/ro/crate/1.2/context".to_string(),
//!     ),
//!     graph: Vec::new(),
//! };
//!
//! let description = MetadataDescriptor {
//!     id: "ro-crate-metadata.json".to_string(),
//!     type_: DataType::Term("CreativeWork".to_string()),
//!     conforms_to: Id::Id("https://w3id.org/ro/crate/1.2".to_string()),
//!     about: Id::Id("./".to_string()),
//!     dynamic_entity: None,
//! };
//!
//! let root_data_entity = RootDataEntity {
//!     id: "./".to_string(),
//!     type_: DataType::Term("Dataset".to_string()),
//!     date_published: "2024".to_string(),
//!     license: Some(License::Id(Id::Id("MIT LICENSE".to_string())),
//!     dynamic_entity: None,
//! };
//! ```
//!

pub mod ro_crate;
