//! Defines the RoCrate data structure
//!
//! Includes implementations for crate modification and defines both the
//! RoCrate, RoCrateContext and GraphVector.
//!
//! # Note
//! This should definitly be split up in future implementations

use crate::ro_crate::constraints::{DynamicEntity, Id, IdValue};
use crate::ro_crate::contextual_entity::ContextualEntity;
use crate::ro_crate::data_entity::DataEntity;
use crate::ro_crate::metadata_descriptor::MetadataDescriptor;
use crate::ro_crate::modify::DynamicEntityManipulation;
use crate::ro_crate::root::RootDataEntity;
use serde::de::Error as SerdeError;
use serde::ser::Serializer;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{Error as SerdeJsonError, Value};
use std::collections::HashMap;
use std::fmt;
use std::path::Path;
use url::Url;

/// Represents a Research Object Crate (RO-Crate) metadata structure.
///
/// An RO-Crate is a lightweight approach to packaging research data
/// with their associated metadata in a machine-readable format. This struct
/// models the root of an RO-Crate JSON-LD document, containing both the
/// contextual information and the actual data entities (graph).
#[derive(Serialize, Deserialize, Debug)]
pub struct RoCrate {
    /// JSON-LD context defining the terms used in the RO-Crate.
    ///
    /// This field specifies the context for interpreting the JSON-LD document,
    /// often pointing to a remote JSON-LD context file or an inline definition
    /// that maps terms to IRIs (Internationalized Resource Identifiers).
    #[serde(rename = "@context")]
    pub context: RoCrateContext,

    /// The main content of the RO-Crate, represented as a graph of entities.
    ///
    /// This vector contains the entities (e.g., datasets, people, organizations)
    /// involved in the research output. Each entity is described in a structured
    /// format, allowing for easy machine processing and interoperability.
    #[serde(rename = "@graph")]
    pub graph: Vec<GraphVector>,
}

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
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(untagged)]
pub enum ContextItem {
    /// A URI string for referencing external JSON-LD contexts
    ReferenceItem(String),
    /// Directly embedded context definitions, ensureing crate protability by using a vector of
    /// hash maps for term definitions
    EmbeddedContext(HashMap<String, String>),
}

/// This allows direct manipulation of each node of the GraphVector
impl RoCrate {
    /// Creates a new struct with a given context and empty Graph vec (i.e no entities)
    pub fn new(context: RoCrateContext, _graph: Vec<GraphVector>) -> RoCrate {
        RoCrate {
            context,
            graph: Vec::new(),
        }
    }

    /// Removes an entity from the RO-Crate graph based on its `@id`.
    ///
    /// This method iterates through the graph and retains only those entities whose `@id`
    /// does not match the specified `id_to_remove`. If `rec` is `true`, it additionally
    /// performs recursive removal of related entities.
    pub fn remove_by_id(&mut self, id_to_remove: &str, rec: bool) {
        self.graph
            .retain(|graph_vector: &GraphVector| match graph_vector {
                GraphVector::MetadataDescriptor(descriptor) => &descriptor.id != id_to_remove,
                GraphVector::RootDataEntity(entity) => &entity.id != id_to_remove,
                GraphVector::DataEntity(entity) => &entity.id != id_to_remove,
                GraphVector::ContextualEntity(entity) => &entity.id != id_to_remove,
                GraphVector::FallbackValue(_) => true,
            });

        if rec == true {
            self.remove_id_recursive(id_to_remove)
        }
    }

    /// Supports the removal process by looking for and removing related entities.
    ///
    /// This function is called for deeper cleaning, making sure that any entity that
    /// could be connected to the one being removed is also taken out if it matches the ID.
    fn remove_id_recursive(&mut self, id: &str) {
        for graph_vector in &mut self.graph {
            if let GraphVector::RootDataEntity(data_entity) = graph_vector {
                data_entity.remove_matching_value(id);
            }
            if let GraphVector::MetadataDescriptor(data_entity) = graph_vector {
                data_entity.remove_matching_value(id);
            }
            if let GraphVector::DataEntity(data_entity) = graph_vector {
                data_entity.remove_matching_value(id);
            }
            if let GraphVector::ContextualEntity(data_entity) = graph_vector {
                data_entity.remove_matching_value(id);
            }
        }
    }

    /// Updates the ID of an entity and any related entities within the crate.
    ///
    /// Looks through all entities, updating any that match `id_old` to `id_new`. If any entity is updated,
    /// it returns a confirmation. This is useful for keeping the crate's links accurate if an entity's ID changes.
    pub fn update_id_recursive(&mut self, id_old: &str, id_new: &str) -> Option<()> {
        let mut any_updates = false;

        for graph_vector in &mut self.graph {
            let updated = match graph_vector {
                GraphVector::ContextualEntity(entity) => {
                    if entity.id == id_old {
                        entity.id = id_new.to_string();
                    }
                    entity.update_matching_id(id_old, id_new)
                }
                GraphVector::DataEntity(entity) => {
                    if entity.id == id_old {
                        entity.id = id_new.to_string();
                    }
                    entity.update_matching_id(id_old, id_new)
                }
                GraphVector::RootDataEntity(entity) => {
                    if entity.id == id_old {
                        entity.id = id_new.to_string();
                    }
                    entity.update_matching_id(id_old, id_new)
                }
                GraphVector::MetadataDescriptor(descriptor) => {
                    if descriptor.id == id_old {
                        descriptor.id = id_new.to_string();
                    }
                    descriptor.update_matching_id(id_old, id_new)
                }
                _ => None,
            };

            if updated.is_some() {
                any_updates = true;
            }
        }

        if any_updates {
            Some(())
        } else {
            None
        }
    }

    /// Finds the index of a particular entity in the RO-Crate graph based on its `@id`.
    ///
    /// Returns the index of the first entity that matches the given `@id`.
    /// Returns `None` if no match is found.
    pub fn find_id_index(&mut self, id: &str) -> Option<usize> {
        self.graph
            .iter()
            .enumerate()
            .find_map(|(index, graph_vector)| match graph_vector {
                GraphVector::MetadataDescriptor(descriptor) if descriptor.id == id => Some(index),
                GraphVector::RootDataEntity(entity) if entity.id == id => Some(index),
                GraphVector::DataEntity(entity) if entity.id == id => Some(index),
                GraphVector::ContextualEntity(entity) if entity.id == id => Some(index),
                _ => None,
            })
    }
    /// Finds ID based upon ID string input and returns a reference to it.
    ///
    /// If it cannot find an entity, it will return None
    pub fn find_id(&mut self, id: &str) -> Option<&GraphVector> {
        self.find_id_index(id)
            .and_then(|index| self.graph.get(index))
    }

    /// Removes a specific field from a dynamic entity within the RO-Crate graph.
    ///
    /// This method finds the entity by `id` and then removes the field specified by `key`
    /// from its dynamic entity data.
    pub fn remove_dynamic_entity_field(&mut self, id: &str, key: &str) {
        let index = self.find_id_index(id);
        if let Some(index) = index {
            if let Some(graph_vector) = self.graph.get_mut(index) {
                match graph_vector {
                    GraphVector::MetadataDescriptor(descriptor) => {
                        if let Some(dynamic_entity) = &mut descriptor.dynamic_entity {
                            dynamic_entity.remove(key);
                        }
                    }
                    GraphVector::RootDataEntity(entity) => {
                        if let Some(dynamic_entity) = &mut entity.dynamic_entity {
                            dynamic_entity.remove(key);
                        }
                    }
                    GraphVector::DataEntity(entity) => {
                        if let Some(dynamic_entity) = &mut entity.dynamic_entity {
                            dynamic_entity.remove(key);
                        }
                    }
                    GraphVector::ContextualEntity(entity) => {
                        if let Some(dynamic_entity) = &mut entity.dynamic_entity {
                            dynamic_entity.remove(key);
                        }
                    }
                    GraphVector::FallbackValue(_) => {} // other variants...
                };
            }
        }
    }

    /// Adds new information to an entity identified by ID. The new info is given as a
    /// map (key-value pairs) and is added to the entity's dynamic_entity hashmap.
    pub fn add_dynamic_entity_field(&mut self, id: &str, values: HashMap<String, DynamicEntity>) {
        if let Some(index) = self.find_id_index(id) {
            if let Some(graph_vector) = self.graph.get_mut(index) {
                match graph_vector {
                    GraphVector::MetadataDescriptor(descriptor) => {
                        descriptor.add_dynamic_fields(values)
                    }
                    GraphVector::RootDataEntity(entity) => entity.add_dynamic_fields(values),
                    GraphVector::DataEntity(entity) => entity.add_dynamic_fields(values),
                    GraphVector::ContextualEntity(entity) => entity.add_dynamic_fields(values),
                    GraphVector::FallbackValue(_) => {} // other variants...
                };
            }
        }
    }

    /// Searches for and returns values associated with a specific property key across all entities.
    ///
    /// This method scans every entity within the RO-Crate for a given property key and compiles a list of all unique values
    /// associated with that key. If an entity contains the specified property, its value(s) are added to the return list.
    /// This is useful for aggregating information from across the dataset that shares a common property.
    pub fn get_property_value(&self, key: String) -> Vec<String> {
        let mut property_values: Vec<String> = Vec::new();
        for graph_vector in &self.graph {
            match graph_vector {
                GraphVector::ContextualEntity(ref _entity) => {
                    let keys = _entity.search_keys(&key.to_string(), false);
                    if keys.len() != 0 {
                        property_values.extend(keys);
                    }
                }
                GraphVector::DataEntity(ref _entity) => {
                    let keys = _entity.search_keys(&key.to_string(), false);
                    if keys.len() != 0 {
                        property_values.extend(keys);
                    }
                }
                GraphVector::RootDataEntity(_entity) => {
                    let keys = _entity.search_keys(&key.to_string(), false);
                    if keys.len() != 0 {
                        property_values.extend(keys);
                    }
                }
                GraphVector::MetadataDescriptor(_entity) => {
                    let keys = _entity.search_keys(&key.to_string(), false);
                    if keys.len() != 0 {
                        property_values.extend(keys);
                    }
                }
                GraphVector::FallbackValue(_entity) => {
                    todo!();
                }
            }
        }
        dedup_vec(&mut property_values);
        property_values
    }

    /// Retrieves all distinct property values from dynamic entities within the RO-Crate.
    ///
    /// Unlike `get_property_value`, this method does not target a specific property key. Instead, it gathers
    /// all values from all properties across every entity, providing a comprehensive overview of the data contained
    /// within the crate. This method is particularly useful for extracting a dataset-wide summary or for auditing
    /// the diversity of information stored.
    ///
    /// DOES NOT GET DEFINED STRUCT FIELDS
    pub fn get_all_property_values(&self) -> Vec<String> {
        // Empty string for function argument.
        let mut property_values: Vec<String> = Vec::new();
        let key: String = String::new();

        for graph_vector in &self.graph {
            match graph_vector {
                GraphVector::ContextualEntity(ref _entity) => {
                    let keys = _entity.search_keys(&key, true);
                    if keys.len() != 0 {
                        property_values.extend(keys);
                    }
                }
                GraphVector::DataEntity(ref _entity) => {
                    let keys = _entity.search_keys(&key, true);
                    if keys.len() != 0 {
                        property_values.extend(keys);
                    }
                }
                GraphVector::RootDataEntity(_entity) => {
                    let keys = _entity.search_keys(&key, true);
                    if keys.len() != 0 {
                        property_values.extend(keys);
                    }
                }
                GraphVector::MetadataDescriptor(_entity) => {
                    let keys = _entity.search_keys(&key, true);
                    if keys.len() != 0 {
                        property_values.extend(keys);
                    }
                }
                GraphVector::FallbackValue(_entity) => {
                    todo!();
                }
            }
        }
        dedup_vec(&mut property_values);
        property_values
    }

    /// Retrieves a list of context keys from the RO-Crate's context.
    ///
    /// This method examines the RO-Crate's context (either embedded directly in the crate or extended)
    /// and compiles a list of all keys (properties or terms) defined. It's useful for understanding the
    /// scope of metadata vocabularies used within the crate.
    pub fn get_context_items(&self) -> Vec<String> {
        let mut valid_context: Vec<String> = Vec::new();

        match &self.context {
            RoCrateContext::EmbeddedContext(context) => {
                for map in context {
                    for (key, _value) in map {
                        valid_context.push(key.to_string());
                    }
                }
            }
            RoCrateContext::ExtendedContext(context) => {
                for map in context {
                    match map {
                        ContextItem::EmbeddedContext(context) => {
                            for (key, _value) in context {
                                valid_context.push(key.to_string());
                            }
                        }
                        _ => (),
                    }
                }
            }
            RoCrateContext::ReferenceContext(context) => {
                valid_context.push(context.to_string());
            }
        }
        valid_context
    }

    /// TODO
    pub fn add_context(&self) {}

    /// Updates or adds a new data entity in the RO-Crate by ID.
    ///
    /// If an entity with the specified ID exists, it is overwritten with `new_data`. If no entity with the
    /// given ID exists, `new_data` is added to the crate. This method ensures that any added or updated data
    /// entity is correctly referenced in the root data entity's `hasPart` property if it is not already listed.
    pub fn overwite_by_id(&mut self, id: &str, new_data: GraphVector) {
        if let Some(index) = self.find_id_index(id) {
            self.graph[index] = new_data;
            if let GraphVector::DataEntity(_entity) = &self.graph[index] {
                self.add_data_to_partof_root(id)
            }
        } else {
            self.graph.push(new_data);
            if let Some(index) = self.find_id_index(id) {
                if let GraphVector::DataEntity(_entity) = &self.graph[index] {
                    self.add_data_to_partof_root(id)
                }
            }
        }
    }

    /// Ensures a data entity is included in the `hasPart` property of the root data entity.
    ///
    /// Before adding a new data entity, this method checks if the entity is already referenced in the root
    /// data entity's `hasPart` property. If not, it adds a reference to ensure the data entity is correctly
    /// part of the overall data structure of the RO-Crate.
    pub fn add_data_to_partof_root(&mut self, target_id: &str) {
        if let Some(index) = self.find_id_index("./") {
            if let Some(GraphVector::RootDataEntity(root)) = self.graph.get_mut(index) {
                let dynamic_entity = root.dynamic_entity.get_or_insert_with(HashMap::new);

                match dynamic_entity.get_mut("hasPart") {
                    Some(DynamicEntity::EntityId(Id::IdArray(ref mut id_array))) => {
                        // Check if the target_id is already present
                        if !id_array.iter().any(|id_value| id_value.id == target_id) {
                            id_array.push(IdValue {
                                id: target_id.to_owned(),
                            });
                        }
                    }
                    _ => {
                        dynamic_entity.insert(
                            "hasPart".to_string(),
                            DynamicEntity::EntityId(Id::IdArray(vec![IdValue {
                                id: target_id.to_owned(),
                            }])),
                        );
                    }
                };
            };
        };
    }

    /// Retrieves a list of all entity IDs within the RO-Crate.
    ///
    /// This method compiles a list of the IDs of all entities contained within the RO-Crate. It is useful
    /// for operations that need to process or reference every entity in the crate, such as data validation
    /// or integrity checks.
    pub fn get_all_ids(&self) -> Vec<&String> {
        let mut id_vec: Vec<&String> = Vec::new();

        for graph_vector in self.graph.iter() {
            match graph_vector {
                GraphVector::MetadataDescriptor(entity) => id_vec.push(&entity.id),
                GraphVector::RootDataEntity(entity) => id_vec.push(&entity.id),
                GraphVector::DataEntity(entity) => id_vec.push(&entity.id),
                GraphVector::ContextualEntity(entity) => id_vec.push(&entity.id),
                GraphVector::FallbackValue(_) => {} // other variants...
            };
        }
        id_vec
    }
}

/// Removes duplicates from a vector, leaving only unique elements.
///
/// This function sorts the vector and then removes any consecutive duplicate elements, ensuring that
/// each element is unique. It requires the elements to implement the `Ord` trait to allow sorting.
///
/// # Arguments
/// * `vec` - A mutable reference to the vector from which duplicates will be removed.
///
/// # Examples
/// ```
/// let mut numbers = vec![3, 1, 2, 3, 4, 2];
/// dedup_vec(&mut numbers);
/// assert_eq!(numbers, vec![1, 2, 3, 4]);
/// ```
fn dedup_vec<T: Ord>(vec: &mut Vec<T>) {
    vec.sort();
    vec.dedup();
}

impl Default for RoCrate {
    /// Provides a default instance of `RoCrate` with a predefined context URL and an empty graph.
    ///
    /// The context URL points to the standard RO-Crate JSON-LD context, setting up a new `RoCrate` with
    /// the necessary context for interpreting the crate according to the RO-Crate specifications.
    fn default() -> Self {
        RoCrate {
            context: RoCrateContext::ReferenceContext(String::from(
                "https://w3id.org/ro/crate/1.1/context",
            )),
            graph: Vec::new(),
        }
    }
}

/// Implements the `Display` trait for `RoCrate` to enable pretty printing.
///
/// This implementation provides a human-readable representation of an `RoCrate` instance, showing the
/// context and a summary of the graph content. It is useful for debugging purposes or when logging crate
/// information in a human-readable format.
///
/// # Examples
/// ```
/// let ro_crate = RoCrate::default();
/// println!("{}", ro_crate);
/// // Outputs: RO-Crate: context="https://w3id.org/ro/crate/1.1/context", graph=[]
impl fmt::Display for RoCrate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "RO-Crate: context={:?}, graph={:?}",
            self.context, self.graph
        )
    }
}

/// Represents the various types of entities contained within the graph of an RO-Crate.
///
/// An RO-Crate organizes digital resources as a graph of interconnected entities, including
/// data entities, contextual entities, metadata descriptions, and root. This enum encapsulates
/// the different entity types that can be found in an RO-Crate's graph, allowing for flexible
/// serialization and handling of the graph's contents.
///
/// Variants:
/// - `DataEntity`: Represents a data entity, which is a digital resource described by the crate.
/// - `ContextualEntity`: Represents a contextual entity that provides context for the data entities and the crate.
/// - `FallbackValue`: A generic value used when the specific type of an entity is unknown or not listed.
///     NOTE: this should never actualy be hit since everything should be handled.
///
/// - `MetadataDescriptor`: Contains metadata about the crate itself or its entities.
/// - `RootDataEntity`: The root data entity, representing the crate's primary content.
#[derive(Debug)]
pub enum GraphVector {
    DataEntity(DataEntity),
    ContextualEntity(ContextualEntity),
    FallbackValue(Value),
    MetadataDescriptor(MetadataDescriptor),
    RootDataEntity(RootDataEntity),
}

impl Serialize for GraphVector {
    /// Serializes the `GraphVector` enum into a format suitable for JSON representation.
    ///
    /// This custom implementation of `Serialize` ensures that each variant of `GraphVector` is
    /// correctly serialized into JSON, adhering to the structure expected by consumers of RO-Crate metadata.
    /// It matches on the enum variant and delegates serialization to the inner entity's own `Serialize` implementation.
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // Match each variant of the enum and serialize the inner data directly
        match self {
            GraphVector::MetadataDescriptor(md) => md.serialize(serializer),
            GraphVector::RootDataEntity(rde) => rde.serialize(serializer),
            GraphVector::DataEntity(de) => de.serialize(serializer),
            GraphVector::ContextualEntity(ce) => ce.serialize(serializer),
            GraphVector::FallbackValue(fv) => fv.serialize(serializer),
        }
    }
}

impl<'de> Deserialize<'de> for GraphVector {
    /// Custom deserialization implementation for `GraphVector`.
    ///
    /// This implementation provides a tailored deserialization process for the `GraphVector` enum,
    /// based on the `@id` field present in the JSON data. The `@id` field determines which specific
    /// variant of `GraphVector` to instantiate - `MetadataDescriptor`, `RootDataEntity`, `DataEntity`,
    /// or `ContextualEntity`.
    fn deserialize<D>(deserializer: D) -> Result<GraphVector, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = Value::deserialize(deserializer)?;

        try_deserialize_into_graph_vector(&value)
            .or_else(|e| {
                return Err(e);
            })
            .map_err(|e: SerdeJsonError| {
                // Use the error type from the deserializer's context
                D::Error::custom(format!("Failed to deserialize: {}", e))
            })
    }
}

/// Attempts to deserialize a JSON `Value` into a `GraphVector` variant.
///
/// This function inspects the `@id` field within the given JSON `Value` to determine the type of entity it represents
/// and then deserializes the value into the corresponding `GraphVector` variant. It uses the entity's `@id` to distinguish
/// between different types of entities, such as metadata, root data entities, data entities, and contextual entities.
fn try_deserialize_into_graph_vector(value: &Value) -> Result<GraphVector, SerdeJsonError> {
    if let Some(id) = value.get("@id").and_then(Value::as_str) {
        match id {
            "ro-crate-metadata.json" => {
                MetadataDescriptor::deserialize(value).map(GraphVector::MetadataDescriptor)
            }
            "./" => RootDataEntity::deserialize(value).map(GraphVector::RootDataEntity),
            _ => {
                if is_valid_url_or_path(id) {
                    DataEntity::deserialize(value).map(GraphVector::DataEntity)
                } else {
                    ContextualEntity::deserialize(value).map(GraphVector::ContextualEntity)
                }
            }
        }
    } else {
        Err(serde::de::Error::custom("Missing or invalid '@id' field"))
    }
}

/// Checks if a given string is a valid URL or a path to an existing file.
///
/// This function is used as part of the custom deserialization process in `GraphVector`
/// to distinguish between `DataEntity` and `ContextualEntity`. It validates whether the
/// provided string (`s`) is a well-formed URL or a path that corresponds to an existing file.
fn is_valid_url_or_path(s: &str) -> bool {
    Url::parse(s).is_ok() || Path::new(s).exists()
}

// Tests to make

// Parses valid into dataEntity's if a file
// Parses valid into contextual entities if a valid URL

// Check that Strict spec prevents invalid crates from being loaded
// Check that Strict spec false allows invalid crates to be loaded

// Check that RoCrate is serialisble into all 3 varieties of context

// Impl RoCrate tests
// Check that remove by ID works on 4 main GraphVector Enums
// Check that remove ID doesnt't work on FallbackValues as they are invalid
// Check that remove ID recursive true removes  every ID from a complex json struct - will need a fixture
// Check that remove ID doesnt fail when there is no ID
// Check that remove id recursive loop through every type of enum
// Check that find_id_index finds valid ID and returns index
// Check that find_id_index if no valid ID returns none
// Check that remove_dynamic_entity_field works on every dynamic entity of the seperate GraphVector Enums
// Check that remove_dynamic_entity_field doesnt work on fallback values

// Impl Deserialise graphvector
// Check that deserialisation uses fallback if invalid crate object.
// Check that if invalid JSON then failed to deserialise error

// Check that try_deserilaise into graph vector gets correct ID
// Check that corect match arms and called when id matches valid crate objects
