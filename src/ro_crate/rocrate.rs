//! Defines the RoCrate data structure
//!
//! Includes implementations for crate modification and defines both the
//! RoCrate, RoCrateContext and GraphVector.
//!
//! # Note
//! This should definitly be split up in future implementations

use crate::ro_crate::constraints::EntityValue;
use crate::ro_crate::graph_vector::GraphVector;
use crate::ro_crate::modify::DynamicEntityManipulation;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;

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

    /// Returns entity based on ID
    pub fn get_entity(&self, id: &str) -> Option<&GraphVector> {
        for entity in &self.graph {
            if let Some(matching_entity) = entity.get_entity(id) {
                return Some(matching_entity);
            }
        }
        None
    }

    /// Returns entity based on ID
    pub fn get_entity_mutable(&mut self, id: &str) -> Option<&mut GraphVector> {
        for entity in &mut self.graph {
            if let Some(matching_entity) = entity.get_entity_mutable(id) {
                return Some(matching_entity);
            }
        }
        None
    }

    /// Retrieves a list of all entity IDs within the RO-Crate.
    ///
    /// This method compiles a list of the IDs of all entities contained within the RO-Crate. It is useful
    /// for operations that need to process or reference every entity in the crate, such as data validation
    /// or integrity checks.
    pub fn get_all_ids(&self) -> Vec<&String> {
        let mut id_vec: Vec<&String> = Vec::new();

        for graph_vector in self.graph.iter() {
            id_vec.push(graph_vector.get_id());
        }
        id_vec
    }

    /// Finds the index of a particular entity in the RO-Crate graph based on its `@id`.
    ///
    /// Returns the index of the first entity that matches the given `@id`.
    /// Returns `None` if no match is found.
    pub fn find_entity_index(&mut self, id: &str) -> Option<usize> {
        self.graph
            .iter()
            .enumerate()
            .find_map(|(index, graph_vector)| {
                if graph_vector.get_id() == id {
                    Some(index)
                } else {
                    None
                }
            })
    }

    /// Finds ID based upon ID string input and returns a reference to it.
    ///
    /// If it cannot find an entity, it will return None
    pub fn find_entity(&mut self, id: &str) -> Option<&GraphVector> {
        self.find_entity_index(id)
            .and_then(|index| self.graph.get(index))
    }

    /// Removes an entity from the RO-Crate graph based on its `@id`.
    ///
    /// This method iterates through the graph and retains only those entities whose `@id`
    /// does not match the specified `id_to_remove`. If `rec` is `true`, it additionally
    /// performs recursive removal of related entities.
    pub fn remove_by_id(&mut self, id_to_remove: &str, rec: bool) {
        self.graph
            .retain(|graph_vector: &GraphVector| match graph_vector {
                GraphVector::MetadataDescriptor(descriptor) => descriptor.id != id_to_remove,
                GraphVector::RootDataEntity(entity) => entity.id != id_to_remove,
                GraphVector::DataEntity(entity) => entity.id != id_to_remove,
                GraphVector::ContextualEntity(entity) => entity.id != id_to_remove,
            });

        if rec {
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
    pub fn update_id_recursive(&mut self, id_old: &str, id_new: &str) {
        for graph_vector in &mut self.graph {
            if graph_vector.get_id() == id_old {
                graph_vector.update_id(id_new.to_string());
                graph_vector.update_id_link(id_old, id_new);
            } else {
                graph_vector.update_id_link(id_old, id_new);
            };
        }
    }

    /// Ensures a data entity is included in the `hasPart` property of the root data entity.
    ///
    /// Before adding a new data entity, this method checks if the entity is already referenced in the root
    /// data entity's `hasPart` property. If not, it adds a reference to ensure the data entity is correctly
    /// part of the overall data structure of the RO-Crate.
    pub fn add_data_to_partof_root(&mut self, id: &str) {
        if let Some(GraphVector::RootDataEntity(root)) = self.get_entity_mutable(id) {
            root.add_entity_to_partof(id.to_string());
        };
    }

    pub fn get_all_properties(&self) -> Vec<String> {
        let mut properties: Vec<String> = Vec::new();
        for graph_vector in &self.graph {
            let keys = graph_vector.get_all_properties();
            properties.extend(keys);
        }

        dedup_vec(&mut properties);
        properties
    }

    pub fn get_all_property_values(&self, property: &str) -> Vec<(String, EntityValue)> {
        let mut property_values: Vec<(String, EntityValue)> = Vec::new();
        for graph_vector in &self.graph {
            match graph_vector.get_specific_property(property) {
                None => {}
                Some(value) => property_values.push(value),
            }
        }
        property_values
    }

    pub fn get_specific_value(&self, value: &str) -> Vec<(String, String)> {
        let mut property_values: Vec<(String, String)> = Vec::new();
        for graph_vector in &self.graph {
            match graph_vector.get_specific_value(value) {
                None => {}
                Some(value) => property_values.push(value),
            }
        }
        property_values
    }

    pub fn overwrite_by_id(&mut self, id: &str, entity: GraphVector) -> bool {
        if let Some(index) = self.find_entity_index(id) {
            self.graph[index] = entity;
            true
        } else {
            false
        }
    }

    pub fn add_dynamic_entity_property(
        &mut self,
        id: &str,
        property: HashMap<String, EntityValue>,
    ) -> bool {
        if let Some(index) = self.find_entity_index(id) {
            self.graph[index].add_dynamic_entity_field(property);
            true
        } else {
            false
        }
    }

    pub fn remove_dynamic_entity_property(&mut self, id: &str, property: &str) -> bool {
        if let Some(index) = self.find_entity_index(id) {
            self.graph[index].remove_dynamic_entity_field(property);
            true
        } else {
            false
        }
    }
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
