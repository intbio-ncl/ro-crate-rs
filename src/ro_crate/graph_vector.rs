use crate::ro_crate::constraints::{DataType, EntityValue};
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
use std::path::Path;
use url::Url;

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
/// - `MetadataDescriptor`: Contains metadata about the crate itself or its entities.
/// - `RootDataEntity`: The root data entity, representing the crate's primary content.
#[derive(Debug, Clone)]
pub enum GraphVector {
    DataEntity(DataEntity),
    ContextualEntity(ContextualEntity),
    MetadataDescriptor(MetadataDescriptor),
    RootDataEntity(RootDataEntity),
}

impl GraphVector {
    /// Updates or adds a new data entity in the RO-Crate by ID.
    ///
    /// If an entity with the specified ID exists, it is overwritten with `new_data`. If no entity with the
    /// given ID exists, `new_data` is added to the crate. This method ensures that any added or updated data
    /// entity is correctly referenced in the root data entity's `hasPart` property if it is not already listed.
    pub fn overwrite(&mut self, new_data: GraphVector) -> bool {
        match self {
            GraphVector::DataEntity(ref mut existing_entity) => {
                if let GraphVector::DataEntity(new_entity) = new_data {
                    *existing_entity = new_entity;
                    return true;
                }
            }
            GraphVector::ContextualEntity(ref mut existing_entity) => {
                if let GraphVector::ContextualEntity(new_entity) = new_data {
                    *existing_entity = new_entity;
                    return true;
                }
            }
            GraphVector::RootDataEntity(ref mut existing_entity) => {
                if let GraphVector::RootDataEntity(new_entity) = new_data {
                    *existing_entity = new_entity;
                    return true;
                }
            }
            GraphVector::MetadataDescriptor(ref mut existing_entity) => {
                if let GraphVector::MetadataDescriptor(new_entity) = new_data {
                    *existing_entity = new_entity;
                    return true;
                }
            }
        }
        false
    }

    /// Get entity
    pub fn get_entity(&self, id: &str) -> Option<&GraphVector> {
        match self {
            GraphVector::MetadataDescriptor(entity) if id == entity.id => Some(self),
            GraphVector::RootDataEntity(entity) if id == entity.id => Some(self),
            GraphVector::DataEntity(entity) if id == entity.id => Some(self),
            GraphVector::ContextualEntity(entity) if id == entity.id => Some(self),
            _ => None,
        }
    }

    /// Get entity
    pub fn get_entity_mutable(&mut self, id: &str) -> Option<&mut GraphVector> {
        match self {
            GraphVector::MetadataDescriptor(entity) if id == entity.id => Some(self),
            GraphVector::RootDataEntity(entity) if id == entity.id => Some(self),
            GraphVector::DataEntity(entity) if id == entity.id => Some(self),
            GraphVector::ContextualEntity(entity) if id == entity.id => Some(self),
            _ => None,
        }
    }
    /// Retrieves a list of all entity IDs within the RO-Crate.
    ///
    /// This method compiles a list of the IDs of all entities contained within the RO-Crate. It is useful
    /// for operations that need to process or reference every entity in the crate, such as data validation
    /// or integrity checks.
    pub fn get_id(&self) -> &String {
        match self {
            GraphVector::MetadataDescriptor(entity) => &entity.id,
            GraphVector::RootDataEntity(entity) => &entity.id,
            GraphVector::DataEntity(entity) => &entity.id,
            GraphVector::ContextualEntity(entity) => &entity.id,
        }
    }

    pub fn update_id(&mut self, new_id: String) {
        match self {
            GraphVector::MetadataDescriptor(entity) => entity.id = new_id,
            GraphVector::RootDataEntity(entity) => entity.id = new_id,
            GraphVector::DataEntity(entity) => entity.id = new_id,
            GraphVector::ContextualEntity(entity) => entity.id = new_id,
        };
    }

    pub fn update_id_link(&mut self, old_id: &str, new_id: &str) {
        match self {
            GraphVector::MetadataDescriptor(entity) => entity.update_matching_id(old_id, new_id),
            GraphVector::RootDataEntity(entity) => entity.update_matching_id(old_id, new_id),
            GraphVector::ContextualEntity(entity) => entity.update_matching_id(old_id, new_id),
            GraphVector::DataEntity(entity) => entity.update_matching_id(old_id, new_id),
        };
    }

    pub fn get_type(&self) -> &DataType {
        match self {
            GraphVector::MetadataDescriptor(entity) => &entity.type_,
            GraphVector::RootDataEntity(entity) => &entity.type_,
            GraphVector::DataEntity(entity) => &entity.type_,
            GraphVector::ContextualEntity(entity) => &entity.type_,
        }
    }

    pub fn add_type(&mut self, new_type: String) {
        match self {
            GraphVector::MetadataDescriptor(entity) => entity.type_.add_type(new_type),
            GraphVector::RootDataEntity(entity) => entity.type_.add_type(new_type),
            GraphVector::DataEntity(entity) => entity.type_.add_type(new_type),
            GraphVector::ContextualEntity(entity) => entity.type_.add_type(new_type),
        }
    }

    pub fn remove_type(&mut self, remove_type: String) -> Result<DataType, String> {
        match self {
            GraphVector::MetadataDescriptor(entity) => entity.type_.remove_type(remove_type),
            GraphVector::RootDataEntity(entity) => entity.type_.remove_type(remove_type),
            GraphVector::DataEntity(entity) => entity.type_.remove_type(remove_type),
            GraphVector::ContextualEntity(entity) => entity.type_.remove_type(remove_type),
        }
    }

    /// Removes a specific field from a dynamic entity within the RO-Crate graph.
    ///
    /// This method finds the entity by `id` and then removes the field specified by `key`
    /// from its dynamic entity data.
    pub fn remove_dynamic_entity_field(&mut self, key: &str) {
        match self {
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
        };
    }

    /// Adds new information to an entity identified by ID. The new info is given as a
    /// map (key-value pairs) and is added to the entity's dynamic_entity hashmap.
    pub fn add_dynamic_entity_field(&mut self, values: HashMap<String, EntityValue>) {
        match self {
            GraphVector::MetadataDescriptor(descriptor) => descriptor.add_dynamic_fields(values),
            GraphVector::RootDataEntity(entity) => entity.add_dynamic_fields(values),
            GraphVector::DataEntity(entity) => entity.add_dynamic_fields(values),
            GraphVector::ContextualEntity(entity) => entity.add_dynamic_fields(values),
        };
    }

    /// Adds new information to an entity identified by ID. The new info is given as a
    /// map (key-value pairs) and is added to the entity's dynamic_entity hashmap.
    pub fn update_dynamic_entity_field(&mut self, key: String, values: EntityValue) {
        match self {
            GraphVector::MetadataDescriptor(descriptor) => {
                if let Some(dynamic_entity) = &mut descriptor.dynamic_entity {
                    dynamic_entity.insert(key, values);
                }
            }
            GraphVector::RootDataEntity(entity) => {
                if let Some(dynamic_entity) = &mut entity.dynamic_entity {
                    dynamic_entity.insert(key, values);
                }
            }
            GraphVector::DataEntity(entity) => {
                if let Some(dynamic_entity) = &mut entity.dynamic_entity {
                    dynamic_entity.insert(key, values);
                }
            }
            GraphVector::ContextualEntity(entity) => {
                if let Some(dynamic_entity) = &mut entity.dynamic_entity {
                    dynamic_entity.insert(key, values);
                }
            }
        };
    }

    pub fn get_specific_property(&self, property: &str) -> Option<(String, EntityValue)> {
        match self {
            GraphVector::MetadataDescriptor(entity) => entity.get_property_value(property),
            GraphVector::RootDataEntity(entity) => entity.get_property_value(property),
            GraphVector::DataEntity(entity) => entity.get_property_value(property),
            GraphVector::ContextualEntity(entity) => entity.get_property_value(property),
        }
    }
    /// Searches for and returns values associated with a specific property key across all entities.
    ///
    /// This method scans every entity within the RO-Crate for a given property key and compiles a list of all unique values
    /// associated with that key. If an entity contains the specified property, its value(s) are added to the return list.
    /// This is useful for aggregating information from across the dataset that shares a common property.
    pub fn get_all_properties(&self) -> Vec<String> {
        let mut properties: Vec<String> = Vec::new();

        match self {
            GraphVector::ContextualEntity(entity) => {
                let keys = entity.get_all_keys();
                if !keys.is_empty() {
                    properties.extend(keys);
                }
            }
            GraphVector::DataEntity(entity) => {
                let keys = entity.get_all_keys();
                if !keys.is_empty() {
                    properties.extend(keys);
                }
            }
            GraphVector::RootDataEntity(entity) => {
                let keys = entity.get_all_keys();
                if !keys.is_empty() {
                    properties.extend(keys);
                }
            }
            GraphVector::MetadataDescriptor(entity) => {
                let keys = entity.get_all_keys();
                if !keys.is_empty() {
                    properties.extend(keys);
                }
            }
        }

        properties
    }

    pub fn get_specific_value(&self, value: &str) -> Option<(String, String)> {
        if let Some(entity_value) = EntityValue::parse(value) {
            match self {
                GraphVector::MetadataDescriptor(entity) => entity.find_value_details(&entity_value),
                GraphVector::RootDataEntity(entity) => entity.find_value_details(&entity_value),
                GraphVector::DataEntity(entity) => entity.find_value_details(&entity_value),
                GraphVector::ContextualEntity(entity) => entity.find_value_details(&entity_value),
            }
        } else {
            eprintln!("Failed to parse value into EntityValue: {}", value);
            None
        }
    }
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
