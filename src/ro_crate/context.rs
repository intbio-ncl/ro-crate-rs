use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use uuid::Uuid;
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

#[derive(Debug)]
pub enum ContextError {
    NotFound(String),
}

impl fmt::Display for ContextError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ContextError::NotFound(e) => write!(f, "Could not find Context to replace {}", e),
        }
    }
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

    pub fn get_all_context(&mut self) -> Vec<String> {
        let mut valid_context: Vec<String> = Vec::new();
        println!("Self: {:?}", self);
        match &self {
            RoCrateContext::EmbeddedContext(context) => {
                println!("Found Embedded Context");
                for map in context {
                    for key in map.keys() {
                        valid_context.push(key.to_string());
                    }
                }
            }
            RoCrateContext::ExtendedContext(context) => {
                println!("Found Extended Context");
                for map in context {
                    println!("This is current map: {:?}", map);
                    match map {
                        ContextItem::EmbeddedContext(context) => {
                            println!("Inside Embedded Context: {:?}", context);
                            for value in context.values() {
                                valid_context.push(value.to_string());
                            }
                        }
                        ContextItem::ReferenceItem(context) => {
                            println!("Inside Reference Item: {:?}", context);
                            valid_context.push(context.to_string());
                        }
                    }
                }
            }
            RoCrateContext::ReferenceContext(context) => {
                println!("Found Reference Context");
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

    pub fn add_urn_uuid(&mut self) {
        match self {
            RoCrateContext::ExtendedContext(context) => {
                for x in context {
                    match x {
                        ContextItem::EmbeddedContext(ref mut embedded) => {
                            let urn_found = embedded
                                .get("@base")
                                .is_some_and(|value| value.starts_with("urn:uuid:"));
                            if !urn_found {
                                embedded.insert(
                                    "@base".to_string(),
                                    format!("urn:uuid:{}", Uuid::now_v7()),
                                );
                            }
                        }
                        ContextItem::ReferenceItem(_) => {
                            continue;
                        }
                    }
                }
            }
            RoCrateContext::ReferenceContext(reference) => {
                let mut base_map = HashMap::new();
                base_map.insert("@base".to_string(), format!("urn:uuid:{}", Uuid::now_v7()));

                *self = RoCrateContext::ExtendedContext(vec![
                    ContextItem::ReferenceItem(reference.clone()),
                    ContextItem::EmbeddedContext(base_map),
                ]);
            }
            RoCrateContext::EmbeddedContext(context) => {
                println!("EmbeddedContext legacy of {:?}", context)
            }
        }
    }

    pub fn get_urn_uuid(&self) -> Option<String> {
        let base = self.get_specific_context("@base");
        match base {
            Some(uuid) => Some(uuid.strip_prefix("urn:uuid:").unwrap().to_string()),
            None => None,
        }
    }
}

#[cfg(test)]
mod write_crate_tests {
    use crate::ro_crate::read::read_crate;
    use std::path::Path;
    use std::path::PathBuf;

    fn fixture_path(relative_path: &str) -> PathBuf {
        Path::new("tests/fixtures").join(relative_path)
    }

    #[test]
    fn test_get_all_context() {
        let path = fixture_path("_ro-crate-metadata-complex-context.json");
        let mut rocrate = read_crate(&path, 0).unwrap();

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
        let mut rocrate = read_crate(&path, 0).unwrap();

        let specific_context = rocrate.context.get_specific_context("education");

        assert_eq!(
            specific_context.unwrap(),
            "https://criminalcharacters.com/vocab#education"
        );
    }
}
