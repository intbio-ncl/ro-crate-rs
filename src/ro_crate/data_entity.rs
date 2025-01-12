//! Create a Data entity.
//!
//! A data entity is a computional research object such as a file that is
//! required to fully understand or reproduce a research outcome.

use crate::ro_crate::constraints::*;
use crate::ro_crate::modify::*;
use serde::{
    de::{self, MapAccess, Visitor},
    Deserialize, Deserializer, Serialize, Serializer,
};
use std::collections::HashMap;
use std::fmt;

/// Represents a data entity with an identifier, type, and dynamic properties.
///
/// `DataEntity` is designed to encapsulate an entity with a unique identifier (`id`),
/// a specific type (`type_`), and a set of dynamic properties (`dynamic_entity`).
/// This struct is used to handle Data entities that have a predefined structure along with
/// additional properties that may vary.
#[derive(Debug, Clone)]
pub struct DataEntity {
    /// URI to data
    pub id: String,
    /// Defined type, if file MUST be type 'File'
    pub type_: DataType,
    /// Additional metadata
    pub dynamic_entity: Option<HashMap<String, EntityValue>>,
}

/// Provides functionality for manipulating dynamic properties of a `DataEntity`.
///
/// This trait implementation allows for adding, modifying, and removing dynamic properties
/// stored in the `dynamic_entity` field of `DataEntity`.
impl DynamicEntityManipulation for DataEntity {
    fn dynamic_entity(&mut self) -> &mut Option<HashMap<String, EntityValue>> {
        &mut self.dynamic_entity
    }

    fn dynamic_entity_immut(&self) -> &Option<HashMap<String, EntityValue>> {
        &self.dynamic_entity
    }
}

impl DataEntity {
    /// Gets ID and value of specific target property
    pub fn get_property_value(&self, property: &str) -> Option<(String, EntityValue)> {
        // Check the `type` field if it matches the property.
        match property {
            "@type" => Some((
                self.id.clone(),
                EntityValue::EntityDataType(self.type_.clone()),
            )),
            _ => self
                .search_properties_for_value(property)
                .map(|value| (self.id.clone(), value)),
        }
    }
    /// Searches through every value in the struct to find the key for a matching input value.
    ///
    /// # Arguments
    /// * `target_value` - The value to search for, as an `EntityValue`.
    ///
    /// # Returns
    /// An `Option<String>` containing the key if the value exists, or `None` otherwise.
    pub fn find_value_details(&self, target_value: &EntityValue) -> Option<(String, String)> {
        // Check dynamic fields
        if let Some(dynamic_entity) = &self.dynamic_entity {
            if let Some(key) = search_dynamic_entity_for_key(dynamic_entity, target_value) {
                return Some((self.id.clone(), key));
            }
        }

        None
    }
    pub fn get_linked_ids(&self) -> Vec<Id> {
        let mut ids = Vec::new();


        // Check for Id in the dynamic entity
        if let Some(dynamic_entity) = &self.dynamic_entity {
            for value in dynamic_entity.values() {
                Self::extract_ids_from_entity_value(value, &mut ids);
            }
        }

        ids
    }

    /// Recursive helper to extract `Id` values from `EntityValue`.
    fn extract_ids_from_entity_value(value: &EntityValue, ids: &mut Vec<Id>) {
        match value {
            EntityValue::EntityId(id) => {
                ids.push(id.clone());
            }
            EntityValue::EntityVec(vec) => {
                for v in vec {
                    Self::extract_ids_from_entity_value(v, ids);
                }
            }
            EntityValue::EntityObject(map) => {
                for v in map.values() {
                    Self::extract_ids_from_entity_value(v, ids);
                }
            }
            EntityValue::EntityVecObject(vec_map) => {
                for map in vec_map {
                    for v in map.values() {
                        Self::extract_ids_from_entity_value(v, ids);
                    }
                }
            }
            EntityValue::NestedDynamicEntity(nested_value) => {
                Self::extract_ids_from_entity_value(nested_value, ids);
            }
            _ => {}
        }
    }

}

/// Custom display formatting for `DataEntity`
impl fmt::Display for DataEntity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ContextualEntity: id={}, type_={:?}, dynamic_entity={:?}",
            self.id, self.type_, self.dynamic_entity
        )
    }
}

/// Provides custom serialization for `DataEntity`.
///
/// Implements custom serialization logic as defined in the `CustomSerialize` trait.
/// This allows `DataEntity` to be serialized with custom rules, especially for the
/// dynamic properties in the `dynamic_entity` field.
impl CustomSerialize for DataEntity {
    fn dynamic_entity(&self) -> Option<&HashMap<String, EntityValue>> {
        self.dynamic_entity.as_ref()
    }

    fn id(&self) -> &String {
        &self.id
    }

    fn type_(&self) -> &DataType {
        &self.type_
    }
}

/// Custom serde serialization implementation for `DataEntity`.
///
/// Delegates the serialization process to the `custom_serialize` method
/// provided by the `CustomSerialize` trait implementation.
impl Serialize for DataEntity {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.custom_serialize(serializer)
    }
}

/// Custom deserialization implementation for `DataEntity`.
///
/// This method provides a tailored approach to convert serialized data
/// (like JSON) into a `DataEntity` instance. It employs a `DataEntityVisitor`
/// for map-based deserialization.
///
/// The method expects the serialized data to be in a map format (key-value pairs),
/// which is typical for formats like JSON. It specifically looks for `@id` and `@type`
/// keys to fill the corresponding fields of `DataEntity`. All other keys are treated
/// as dynamic properties and are stored in a `HashMap`.
impl<'de> Deserialize<'de> for DataEntity {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct DataEntityVisitor;

        impl<'de> Visitor<'de> for DataEntityVisitor {
            type Value = DataEntity;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a map representing a DataEntity")
            }

            fn visit_map<A>(self, mut map: A) -> Result<DataEntity, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut id = None;
                let mut type_ = None;
                let mut dynamic_entity: HashMap<String, EntityValue> = HashMap::new();

                while let Some(key) = map.next_key::<String>()? {
                    match key.as_str() {
                        "@id" => id = Some(map.next_value()?),
                        "@type" => type_ = Some(map.next_value()?),
                        _ => {
                            let value: EntityValue = map.next_value()?;
                            dynamic_entity.insert(key, value);
                        }
                    }
                }

                let id = id.ok_or_else(|| de::Error::missing_field("@id"))?;
                let type_ = type_.ok_or_else(|| de::Error::missing_field("@type"))?;

                Ok(DataEntity {
                    id,
                    type_,
                    dynamic_entity: Some(dynamic_entity),
                })
            }
        }

        deserializer.deserialize_map(DataEntityVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_data_entity_creation() {
        let entity = DataEntity {
            id: "entity_id".to_string(),
            type_: DataType::Term("ExampleType".to_string()),
            dynamic_entity: Some(HashMap::new()),
        };
        assert_eq!(entity.id, "entity_id");
        assert!(matches!(entity.type_, DataType::Term(ref t) if t == "ExampleType"));
        assert!(entity.dynamic_entity.is_some());
    }

    #[test]
    fn test_add_and_remove_dynamic_entity() {
        let mut entity = DataEntity {
            id: "entity_id".to_string(),
            type_: DataType::Term("ExampleType".to_string()),
            dynamic_entity: None,
        };

        // Adding a dynamic entity
        entity.add_string_value("key".to_string(), "value".to_string());
        assert_eq!(
            entity.dynamic_entity().unwrap().get("key"),
            Some(&EntityValue::EntityString("value".to_string()))
        );

        // Removing a dynamic entity
        entity.remove_field("key");
        assert!(entity.dynamic_entity().unwrap().get("key").is_none());
    }

    #[test]
    fn test_serialization() {
        let entity = DataEntity {
            id: "entity_id".to_string(),
            type_: DataType::Term("ExampleType".to_string()),
            dynamic_entity: None,
        };

        let serialized = serde_json::to_string(&entity).unwrap();
        assert!(serialized.contains("entity_id"));
        assert!(serialized.contains("ExampleType"));
    }

    #[test]
    fn test_deserialization() {
        let json_data = r#"
            {
                "@id": "entity_id",
                "@type": "ExampleType"
            }
        "#;
        let deserialized: DataEntity = serde_json::from_str(json_data).unwrap();
        assert_eq!(deserialized.id, "entity_id");
        assert!(matches!(deserialized.type_, DataType::Term(ref t) if t == "ExampleType"));
    }

    #[test]
    fn test_dynamic_entity_serialization() {
        let mut entity = DataEntity {
            id: "entity_id".to_string(),
            type_: DataType::Term("ExampleType".to_string()),
            dynamic_entity: None,
        };
        entity.add_string_value("key".to_string(), "value".to_string());

        let serialized = serde_json::to_string(&entity).unwrap();
        assert!(serialized.contains("\"key\":\"value\""));
    }

    #[test]
    fn test_dynamic_entity_deserialization() {
        let json_data = r#"
            {
                "@id": "entity_id",
                "@type": "ExampleType",
                "key": "value"
            }
        "#;
        let deserialized: DataEntity = serde_json::from_str(json_data).unwrap();
        assert_eq!(
            deserialized.dynamic_entity.unwrap().get("key"),
            Some(&EntityValue::EntityString("value".to_string()))
        );
    }

    #[test]
    fn test_deserialization_with_missing_fields() {
        let json_data = r#"
            {
                "@id": "entity_id"
            }
        "#;
        let result: Result<DataEntity, _> = serde_json::from_str(json_data);
        assert!(result.is_err()); // Expecting an error due to missing @type field
    }
}
