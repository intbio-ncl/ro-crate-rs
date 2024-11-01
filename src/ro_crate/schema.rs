//! Validation logic for matching keys to valid context

use crate::ro_crate::constraints::{Id, IdValue, License};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
const ROCRATE_SCHEMA_1_1: &str = include_str!("../resources/ro_crate_1_1.jsonld");

/// Represents the JSON-LD context of the RO-Crate schema.
///
/// This struct models the context information required for interpreting the JSON-LD encoded
/// RO-Crate metadata. It includes identifiers, names, version information, URLs, schema versions,
/// basis of the schema, licensing information, and the specific context definitions as a map.
#[derive(Serialize, Deserialize, Debug)]
pub struct RoCrateJSONLDContext {
    #[serde(rename = "@id")]
    pub id: String,
    pub name: Vec<String>,
    pub version: String,
    pub url: IdValue,
    #[serde(rename = "schemaVersion")]
    pub schema_version: IdValue,
    #[serde(rename = "isBasedOn")]
    pub is_based_on: Id,
    pub license: License,
    #[serde(rename = "@context")]
    pub context: HashMap<String, String>,
}

/// Loads in RO-Crate schema for validation.
///
/// This function fetches the JSON-LD context defining the RO-Crate schema, attempting to parse it
/// into an `RoCrateJSONLDContext` struct. It is a synchronous operation and may block the thread
/// during the request and subsequent parsing.
pub fn load_rocrate_schema() -> Result<RoCrateJSONLDContext, SchemaLoadError> {
    load_rocrate_schema_from_str(ROCRATE_SCHEMA_1_1)
}

pub fn load_rocrate_schema_from_str(
    json_str: &str,
) -> Result<RoCrateJSONLDContext, SchemaLoadError> {
    let context =
        serde_json::from_str(json_str).map_err(|e| SchemaLoadError::ParseError(e.to_string()))?;
    Ok(context)
}

#[derive(Debug)]
pub enum SchemaLoadError {
    ParseError(String),
}

impl fmt::Display for SchemaLoadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SchemaLoadError::ParseError(msg) => {
                write!(f, "Failed to parse JSON-LD schema: {}", msg)
            }
        }
    }
}

impl std::error::Error for SchemaLoadError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_load_rocrate_schema_from_str() {
        let mock_json = r#"
        {
            "@id": "https://w3id.org/ro/crate/1.1/context",
            "name": ["RO-Crate JSON-LD Context"],
            "version": "1.1.3",
            "url": {"@id": "https://w3id.org/ro/crate/1.1"},
            "schemaVersion": {"@id": "http://schema.org/version/10.0/"},
            "isBasedOn": [{"@id": "http://schema.org/version/10.0/"}],
            "license": {"@id": "https://creativecommons.org/publicdomain/zero/1.0/"},
            "@context": {
                "3DModel": "http://schema.org/3DModel",
                "APIReference": "http://schema.org/APIReference"
            }
        }
        "#;

        let result = load_rocrate_schema_from_str(mock_json);
        assert!(result.is_ok());

        let context = result.unwrap();
        assert_eq!(context.version, "1.1.3");
        assert!(context.context.contains_key("3DModel"));
    }
}
