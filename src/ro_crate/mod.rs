//! Utility functions and structure for RO-Crate
//!
//! # Note
//! Serialisatoin and deserialisation of RO-Crates to json-ld files heavily leverages
//! the serde and serde-json library

pub mod constraints;
pub mod contextual_entity;
pub mod data_entity;
pub mod metadata_descriptor;
pub mod modify;
pub mod read;
pub mod rocrate;
pub mod root;
pub mod schema;
pub mod write;
