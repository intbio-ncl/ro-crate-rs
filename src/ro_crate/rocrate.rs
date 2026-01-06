//! Defines the RoCrate data structure
//!
//! Includes implementations for crate modification and defines both the
//! RoCrate, RoCrateContext and GraphVector.
//!
//! # Note
//! This should definitly be split up in future implementations

use crate::ro_crate::constraints::{EntityValue, Id};
use crate::ro_crate::context::{ContextItem, RoCrateContext};
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
                    for key in map.keys() {
                        valid_context.push(key.to_string());
                    }
                }
            }
            RoCrateContext::ExtendedContext(context) => {
                for map in context {
                    if let ContextItem::EmbeddedContext(context) = map {
                        for key in context.keys() {
                            valid_context.push(key.to_string());
                        }
                    }
                }
            }
            RoCrateContext::ReferenceContext(context) => {
                valid_context.push(context.to_string());
            }
        }
        valid_context
    }

    /// Adds a context item to the RO-Crate.
    pub fn add_context(&mut self, context: ContextItem) {
        self.context.add_context(&context);
    }

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

    /// Gets all properties found within RO-Crate
    pub fn get_all_properties(&self) -> Vec<String> {
        let mut properties: Vec<String> = Vec::new();
        for graph_vector in &self.graph {
            let keys = graph_vector.get_all_properties();
            properties.extend(keys);
        }

        dedup_vec(&mut properties);
        properties
    }

    /// Gets all the values of a specific property from within the RO-Crate
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

    /// Gets ID and Key of the specific value searched
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

    /// Overwrites an entity with another entity by ID
    pub fn overwrite_by_id(&mut self, id: &str, entity: GraphVector) -> bool {
        if let Some(index) = self.find_entity_index(id) {
            self.graph[index] = entity;
            true
        } else {
            false
        }
    }

    /// Adds a new key:value pair to a entity based on ID
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

    /// Removes a key:value pair of an entity based on its ID
    pub fn remove_dynamic_entity_property(&mut self, id: &str, property: &str) -> bool {
        if let Some(index) = self.find_entity_index(id) {
            self.graph[index].remove_dynamic_entity_field(property);
            true
        } else {
            false
        }
    }

    /// Get the schema version of the RO-Crate based on the context
    pub fn get_rocrate_version(&self) -> Option<crate::ro_crate::schema::RoCrateSchemaVersion> {
        self.context.get_schema_version()
    }

    #[cfg(feature = "parquet")]
    pub fn to_parquet(&mut self) {}

    /// Returns all entities that are referenced rocrates
    pub fn get_subcrates(&self) -> Vec<&GraphVector> {
        // DataEntity::get_property_value() searches recursive, so if we have a DataEntity with
        // a nested custom field that also contains on any lower level `subjectOf`
        // we also get a match. But in this case we only want to get first level matches
        self.graph
            .iter()
            .filter_map(|entry| {
                if let GraphVector::DataEntity(data_entitiy) = entry {
                    if let Some(dynamic) = &data_entitiy.dynamic_entity {
                        if let Some(entity) = dynamic.get("conformsTo") {
                            // There are two variants here:
                            // Either we have an EntityId entry with just one Id
                            // that matches an ro-crate specification or we have an
                            // entity vec where some entities may match an ro-crate
                            // specification
                            match entity {
                                EntityValue::EntityId(Id::Id(crate_path)) => {
                                    // Do not match the root ro-crates metadata json file
                                    if crate_path.contains("https://w3id.org/ro/crate")
                                        && (entry.get_id() != "ro-crate-metdata.json")
                                    {
                                        return Some(entry);
                                    }
                                }
                                EntityValue::EntityVec(entity_values) => {
                                    for value in entity_values {
                                        if let EntityValue::EntityId(Id::Id(crate_path)) = value {
                                            // Do not match the root ro-crates metadata json file
                                            if crate_path.contains("https://w3id.org/ro/crate")
                                                && (entry.get_id() != "ro-crate-metdata.json")
                                            {
                                                return Some(entry);
                                            }
                                        }
                                    }
                                }
                                _ => {}
                            }
                        }
                    }
                }
                None
            })
            .collect()
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
                "https://w3id.org/ro/crate/1.2/context",
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
/// // Outputs: RO-Crate: context="https://w3id.org/ro/crate/1.2/context", graph=[]
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
