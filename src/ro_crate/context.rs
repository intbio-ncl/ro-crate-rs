use log::debug;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use thiserror::Error;
use uuid::Uuid;

use crate::ro_crate::schema::{self, RoCrateSchemaVersion};
/// Defines the JSON-LD contexts in an RO-Crate, facilitating flexible context specification.
///
/// This enum models the `@context` field's variability in RO-Crates, enabling the use of external URLs,
/// combination of contexts, or embedded definitions directly within the crate. It supports:
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(untagged)]
pub enum RoCrateContext {
    /// A URI string for referencing external JSON-LD contexts (default should be
    /// ro-crate context).
    ReferenceContext(String),
    /// A combination of contexts for extended or customized vocabularies, represented as a list of items.
    ExtendedContext(Vec<ContextItem>),
    /// Directly embedded context definitions, ensuring crate portability by using a vector of hash maps for term definitions.    
    EmbeddedContext(Vec<HashMap<String, String>>),
}

/// Represents elements in the `@context` of an RO-Crate, allowing for different ways to define terms.
///
/// There are two types of items:
///
/// - `ReferenceItem`: A URL string that links to an external context definition. It's like a reference to a standard set of terms used across different crates.
///
/// - `EmbeddedContext`: A map containing definitions directly. This is for defining terms right within the crate, making it self-contained.
///
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(untagged)]
pub enum ContextItem {
    /// A URI string for referencing external JSON-LD contexts
    ReferenceItem(String),
    /// Directly embedded context definitions, ensureing crate protability by using a vector of
    /// hash maps for term definitions
    EmbeddedContext(HashMap<String, String>),
}

#[derive(Error, Debug)]
pub enum ContextError {
    #[error("{0}")]
    NotFound(String),
}

impl RoCrateContext {
    /// Adds a new context to the `RoCrateContext`.
    pub fn add_context(&mut self, new_context: &ContextItem) {
        match self {
            RoCrateContext::ReferenceContext(current) => {
                // Convert to `ExtendedContext` if necessary
                *self = RoCrateContext::ExtendedContext(vec![
                    ContextItem::ReferenceItem(current.clone()),
                    new_context.clone(),
                ]);
            }
            RoCrateContext::ExtendedContext(contexts) => {
                // Add the new item to the extended context
                contexts.push(new_context.clone());
            }
            RoCrateContext::EmbeddedContext(embedded_contexts) => {
                // Merge new key-value pairs into the existing embedded context
                if let ContextItem::EmbeddedContext(new_map) = new_context {
                    embedded_contexts.push(new_map.clone());
                }
            }
        }
    }

    /// Removes a context item from the `RoCrateContext`.
    pub fn remove_context(&mut self, target: &ContextItem) -> Result<(), String> {
        match self {
            RoCrateContext::ReferenceContext(_) => Err(
                "ReferenceContext cannot be removed as the crate requires a context.".to_string(),
            ),
            RoCrateContext::ExtendedContext(contexts) => {
                let initial_len = contexts.len();
                contexts.retain(|item| item != target);
                if contexts.len() < initial_len {
                    Ok(())
                } else {
                    Err("Context item not found in ExtendedContext.".to_string())
                }
            }
            RoCrateContext::EmbeddedContext(embedded_contexts) => {
                if let ContextItem::EmbeddedContext(target_map) = target {
                    let initial_len = embedded_contexts.len();
                    embedded_contexts.retain(|map| map != target_map);
                    if embedded_contexts.len() < initial_len {
                        Ok(())
                    } else {
                        Err("Target map not found in EmbeddedContext.".to_string())
                    }
                } else {
                    Err("Invalid target type for EmbeddedContext.".to_string())
                }
            }
        }
    }

    pub fn replace_context(
        &mut self,
        new_context: &ContextItem,
        old_context: &ContextItem,
    ) -> Result<(), ContextError> {
        match self.remove_context(old_context) {
            Ok(_removed) => {
                self.add_context(new_context);
                Ok(())
            }
            Err(e) => Err(ContextError::NotFound(e.to_string())),
        }
    }

    pub fn get_all_context(&self) -> Vec<String> {
        let mut valid_context: Vec<String> = Vec::new();
        debug!("Self: {:?}", self);
        match &self {
            RoCrateContext::EmbeddedContext(context) => {
                debug!("Found Embedded Context");
                for map in context {
                    for key in map.keys() {
                        valid_context.push(key.to_string());
                    }
                }
            }
            RoCrateContext::ExtendedContext(context) => {
                debug!("Found Extended Context");
                for map in context {
                    debug!("This is current map: {:?}", map);
                    match map {
                        ContextItem::EmbeddedContext(context) => {
                            debug!("Inside Embedded Context: {:?}", context);
                            for value in context.values() {
                                valid_context.push(value.to_string());
                            }
                        }
                        ContextItem::ReferenceItem(context) => {
                            debug!("Inside Reference Item: {:?}", context);
                            valid_context.push(context.to_string());
                        }
                    }
                }
            }
            RoCrateContext::ReferenceContext(context) => {
                debug!("Found Reference Context");
                valid_context.push(context.to_string());
            }
        }
        valid_context
    }

    pub fn get_specific_context(&self, context_key: &str) -> Option<String> {
        match self {
            RoCrateContext::ExtendedContext(context) => {
                for item in context {
                    match item {
                        ContextItem::EmbeddedContext(embedded) => {
                            if let Some(value) = embedded.get(context_key) {
                                return Some(value.clone());
                            }
                        }
                        ContextItem::ReferenceItem(_) => {
                            // Skip ReferenceItems as they don't contain key-value pairs
                        }
                    }
                }
                None
            }
            RoCrateContext::ReferenceContext(_reference) => None,
            RoCrateContext::EmbeddedContext(_context) => None,
        }
    }

    /// Adds a urn:uuid @base if the context has none, returning the UUID.
    ///
    /// Replaces an existing non-urn @base, or otherwise adds the @base to the first
    /// embedded term map, adding a map if there is none. Returns `None` for a legacy
    /// embedded-only context, which cannot hold an @base.
    pub fn add_urn_uuid(&mut self) -> Option<String> {
        if let Some(urn) = self.get_urn_uuid() {
            return Some(urn);
        }
        let base = format!("urn:uuid:{}", Uuid::now_v7());

        match self {
            RoCrateContext::ExtendedContext(items) => {
                let is_map = |item: &ContextItem| matches!(item, ContextItem::EmbeddedContext(_));
                let has_base = |item: &ContextItem| matches!(item, ContextItem::EmbeddedContext(map) if map.contains_key("@base"));
                let target = items
                    .iter()
                    .position(has_base)
                    .or_else(|| items.iter().position(is_map));

                match target.map(|index| &mut items[index]) {
                    Some(ContextItem::EmbeddedContext(map)) => {
                        map.insert("@base".to_string(), base);
                    }
                    _ => items.push(ContextItem::EmbeddedContext(HashMap::from([(
                        "@base".to_string(),
                        base,
                    )]))),
                }
            }
            RoCrateContext::ReferenceContext(reference) => {
                *self = RoCrateContext::ExtendedContext(vec![
                    ContextItem::ReferenceItem(reference.clone()),
                    ContextItem::EmbeddedContext(HashMap::from([("@base".to_string(), base)])),
                ]);
            }
            RoCrateContext::EmbeddedContext(context) => {
                debug!("EmbeddedContext legacy of {:?}", context);
            }
        }
        self.get_urn_uuid()
    }

    pub fn get_urn_uuid(&self) -> Option<String> {
        self.get_specific_context("@base")?
            .strip_prefix("urn:uuid:")
            .map(str::to_string)
    }

    /// Get the schema version from the context URLs
    pub fn get_schema_version(&self) -> Option<schema::RoCrateSchemaVersion> {
        self.get_all_context().iter().find_map(|context_url| {
            if context_url.contains("ro/crate/1.1") {
                Some(RoCrateSchemaVersion::V1_1)
            } else if context_url.contains("ro/crate/1.2") {
                Some(RoCrateSchemaVersion::V1_2)
            } else if context_url.contains("ro/crate/1.3") {
                Some(RoCrateSchemaVersion::V1_3)
            } else {
                None
            }
        })
    }
}

#[cfg(test)]
mod write_crate_tests {
    use crate::ro_crate::read::read_crate;
    use crate::ro_crate::schema;
    use std::path::Path;
    use std::path::PathBuf;

    #[test]
    fn get_urn_uuid_returns_none_for_non_uuid_base() {
        let context =
            super::RoCrateContext::ExtendedContext(vec![super::ContextItem::EmbeddedContext(
                std::collections::HashMap::from([(
                    "@base".to_string(),
                    "https://example.org/".to_string(),
                )]),
            )]);
        assert_eq!(context.get_urn_uuid(), None);
    }

    fn reference(url: &str) -> super::ContextItem {
        super::ContextItem::ReferenceItem(url.to_string())
    }

    fn embedded(entries: &[(&str, &str)]) -> super::ContextItem {
        super::ContextItem::EmbeddedContext(
            entries
                .iter()
                .map(|(key, value)| (key.to_string(), value.to_string()))
                .collect(),
        )
    }

    fn bases(context: &super::RoCrateContext) -> Vec<String> {
        match context {
            super::RoCrateContext::ExtendedContext(items) => items
                .iter()
                .filter_map(|item| match item {
                    super::ContextItem::EmbeddedContext(map) => map.get("@base").cloned(),
                    super::ContextItem::ReferenceItem(_) => None,
                })
                .collect(),
            _ => Vec::new(),
        }
    }

    #[test]
    fn add_urn_uuid_to_reference_contexts() {
        let mut context =
            super::RoCrateContext::ReferenceContext("https://w3id.org/ro/crate/1.3/context".into());
        let urn = context.add_urn_uuid().unwrap();
        assert_eq!(bases(&context), vec![format!("urn:uuid:{}", urn)]);

        let mut context = super::RoCrateContext::ExtendedContext(vec![
            reference("https://w3id.org/ro/crate/1.3/context"),
            reference("https://example.org/terms"),
        ]);
        let urn = context.add_urn_uuid().unwrap();
        assert_eq!(bases(&context), vec![format!("urn:uuid:{}", urn)]);
        assert_eq!(context.add_urn_uuid(), Some(urn));
    }

    #[test]
    fn add_urn_uuid_adds_one_base_to_several_maps() {
        let mut context = super::RoCrateContext::ExtendedContext(vec![
            reference("https://w3id.org/ro/crate/1.3/context"),
            embedded(&[("name", "http://schema.org/name")]),
            embedded(&[("author", "http://schema.org/author")]),
        ]);
        let urn = context.add_urn_uuid().unwrap();
        assert_eq!(bases(&context), vec![format!("urn:uuid:{}", urn)]);
        assert_eq!(
            context.get_specific_context("name").unwrap(),
            "http://schema.org/name"
        );
    }

    #[test]
    fn add_urn_uuid_replaces_a_non_urn_base() {
        let mut context = super::RoCrateContext::ExtendedContext(vec![
            reference("https://w3id.org/ro/crate/1.3/context"),
            embedded(&[("name", "http://schema.org/name")]),
            embedded(&[("@base", "https://example.org/")]),
        ]);
        let urn = context.add_urn_uuid().unwrap();
        assert_eq!(bases(&context), vec![format!("urn:uuid:{}", urn)]);
    }

    #[test]
    fn add_urn_uuid_keeps_an_existing_urn() {
        let existing = "urn:uuid:01234567-89ab-cdef-0123-456789abcdef";
        let mut context = super::RoCrateContext::ExtendedContext(vec![
            reference("https://w3id.org/ro/crate/1.3/context"),
            embedded(&[("@base", existing)]),
            embedded(&[("name", "http://schema.org/name")]),
        ]);
        assert_eq!(
            context.add_urn_uuid().as_deref(),
            existing.strip_prefix("urn:uuid:")
        );
        assert_eq!(bases(&context), vec![existing.to_string()]);
    }

    #[test]
    fn add_urn_uuid_cannot_extend_an_embedded_only_context() {
        let mut context =
            super::RoCrateContext::EmbeddedContext(vec![std::collections::HashMap::from([(
                "name".to_string(),
                "http://schema.org/name".to_string(),
            )])]);
        assert_eq!(context.add_urn_uuid(), None);
    }

    fn fixture_path(relative_path: &str) -> PathBuf {
        Path::new("tests/fixtures").join(relative_path)
    }

    #[test]
    fn test_get_all_context() {
        let path = fixture_path("_ro-crate-metadata-complex-context.json");
        let rocrate = read_crate(&path, 0).unwrap();

        let mut context = rocrate.context.get_all_context();
        context.sort();

        let mut fixture_vec = vec![
            "https://w3id.org/ro/crate/1.1/context",
            "https://criminalcharacters.com/vocab#education",
            "https://w3id.org/ro/terms/criminalcharacters#interests",
        ];
        fixture_vec.sort();

        assert_eq!(context, fixture_vec);
    }

    #[test]
    fn test_add_urn() {
        let path = fixture_path("_ro-crate-metadata-complex-context.json");
        let mut rocrate = read_crate(&path, 0).unwrap();

        let mut context = rocrate.context.get_all_context();
        assert_eq!(context.len(), 3);

        rocrate.context.add_urn_uuid();
        context = rocrate.context.get_all_context();

        assert_eq!(context.len(), 4);
    }

    #[test]
    fn test_get_context_from_key() {
        let path = fixture_path("_ro-crate-metadata-complex-context.json");
        let rocrate = read_crate(&path, 0).unwrap();

        let specific_context = rocrate.context.get_specific_context("education");

        assert_eq!(
            specific_context.unwrap(),
            "https://criminalcharacters.com/vocab#education"
        );
    }

    #[test]
    fn test_schema_version() {
        let path = fixture_path("_ro-crate-metadata-complex-context.json");
        let rocrate = read_crate(&path, 0).unwrap();

        let schema_version = rocrate.context.get_schema_version();

        assert_eq!(schema_version.unwrap(), schema::RoCrateSchemaVersion::V1_1);
    }

    #[test]
    fn test_schema_version_1_3() {
        let context = super::RoCrateContext::ReferenceContext(
            "https://w3id.org/ro/crate/1.3/context".to_string(),
        );

        assert_eq!(
            context.get_schema_version().unwrap(),
            schema::RoCrateSchemaVersion::V1_3
        );
    }
}
