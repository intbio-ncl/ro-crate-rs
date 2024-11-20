//! Stuctures for contrained types within an RO-Crate.
//!
//! Focuses on MUST fields as definied by the specification, as well as defining
//! the available types for any additional fields added to an entity object
//! (EntityValue)

use serde::de::{self, MapAccess, SeqAccess, Visitor};
use serde::ser::{SerializeMap, SerializeSeq};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::HashMap;
use std::fmt;

/// ID definition in both single and vec forat
#[derive(Debug, Clone, PartialEq)]
pub enum Id {
    /// Direct ID value for values of {"@id": "id"}
    Id(String),
    /// Array of ID values for values such as [{"@id": "id1"}, {"@id": "id2"}]
    IdArray(Vec<String>),
}

/// Returns true or false depending on whether ID value matched.
/// Useful for matching GraphVector index
impl Id {
    pub fn contains_id(&self, target_id: &str) -> bool {
        match self {
            Id::Id(id_value) => id_value == target_id,
            Id::IdArray(id_values) => id_values.iter().any(|id_val| id_val == target_id),
        }
    }
}
impl Serialize for Id {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Id::Id(id_value) => {
                // Serialize a single ID as `{"@id": "string"}`
                let mut map = serializer.serialize_map(Some(1))?;
                map.serialize_entry("@id", id_value)?;
                map.end()
            }
            Id::IdArray(id_values) => {
                // Start a sequence for the array of maps
                let mut seq = serializer.serialize_seq(Some(id_values.len()))?;

                for id in id_values {
                    // Create a temporary map with `{"@id": "value"}` for each ID
                    let mut temp_map = HashMap::new();
                    temp_map.insert("@id", id);

                    // Serialize the temporary map as an element in the sequence
                    seq.serialize_element(&temp_map)?;
                }

                seq.end()
            }
        }
    }
}

impl<'de> Deserialize<'de> for Id {
    fn deserialize<D>(deserializer: D) -> Result<Id, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct IdVisitor;

        impl<'de> Visitor<'de> for IdVisitor {
            type Value = Id;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("an @id entry or an array of @id entries")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Id, A::Error>
            where
                A: MapAccess<'de>,
            {
                let key: Option<String> = map.next_key()?;
                let id_value: String = map.next_value()?;

                if let Some(key) = key {
                    if key == "@id" {
                        return Ok(Id::Id(id_value));
                    }
                }

                Err(de::Error::missing_field("@id"))
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Id, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let mut ids = Vec::new();

                while let Some(map) = seq.next_element::<HashMap<String, String>>()? {
                    if let Some(id) = map.get("@id") {
                        ids.push(id.clone());
                    } else {
                        return Err(de::Error::missing_field("@id"));
                    }
                }

                Ok(Id::IdArray(ids))
            }
        }

        deserializer.deserialize_any(IdVisitor)
    }
}
/// Enables License as a defined license or referenced license
/// Required in RoCrate due to MUST specification of root
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(untagged)]
pub enum License {
    /// Direct ID value for a single license contextual entity
    Id(Id),
    /// Basic license without URI
    Description(String),
}

/// Enables DataType as a single datatype or multiple
/// Required in RoCrate due to MUST specification of all entites
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[serde(untagged)]
pub enum DataType {
    /// Basic Datatype for entities with only one datatype
    Term(String),
    /// Vector of datatypes for entities that can be described with multiple
    /// datatypes
    TermArray(Vec<String>),
}

impl DataType {
    pub fn add_type(&self, new_type: String) {
        match self {
            DataType::Term(existing_type) => {
                DataType::TermArray(vec![existing_type.clone(), new_type]);
            }
            DataType::TermArray(existing_types) => {
                let mut updated_types = existing_types.clone();
                updated_types.push(new_type);
                DataType::TermArray(updated_types);
            }
        }
    }

    pub fn remove_type(&self, remove: String) -> Result<DataType, String> {
        match self {
            DataType::Term(_) => Err(
                "Cannot remove the only data type; at least one data type is required.".to_string(),
            ),
            DataType::TermArray(existing_types) => {
                if existing_types.len() == 1 && existing_types[0] == remove {
                    return Err(
                        "Cannot remove the only data type; at least one data type is required."
                            .to_string(),
                    );
                }

                // Find the position of the type to remove
                if let Some(index) = existing_types.iter().position(|n| n == &remove) {
                    // Clone the existing types and remove the specified one
                    let mut updated_types = existing_types.clone();
                    updated_types.remove(index);

                    // If only one type remains, convert it to `DataType::Term`
                    if updated_types.len() == 1 {
                        Ok(DataType::Term(updated_types[0].clone()))
                    } else {
                        Ok(DataType::TermArray(updated_types))
                    }
                } else {
                    // Return an error if the type to remove wasn't found
                    Err(format!("Data type '{}' not found in the list.", remove))
                }
            }
        }
    }

    pub fn modify_type(&self) {}
}

/// Allow a vec of ids for modification and creation.
/// Allow a new field of struct id thats not a vec
/// Fallback suboptimal but catch all
#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
#[serde(untagged)]
pub enum EntityValue {
    EntityString(String),
    EntityVecString(Vec<String>),
    EntityId(Id),
    EntityLicense(License),
    Entityi64(i64),
    Entityf64(f64),
    EntityVeci64(Vec<i64>),
    EntityVecf64(Vec<f64>),
    EntityDataType(DataType),
    // Option to capture nulls before nesting
    EntityBool(Option<bool>),
    EntityVec(Vec<EntityValue>),
    EntityObject(HashMap<String, EntityValue>),
    EntityVecObject(Vec<HashMap<String, EntityValue>>),
    NestedDynamicEntity(Box<EntityValue>),
    Fallback(Option<serde_json::Value>),
}

impl fmt::Display for EntityValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            EntityValue::EntityString(value) => write!(f, "EntityString({})", value),
            EntityValue::EntityVecString(values) => write!(f, "EntityVecString({:?})", values),
            EntityValue::EntityId(value) => write!(f, "EntityId({:?})", value),
            EntityValue::EntityLicense(value) => write!(f, "EntityLicense({:?})", value),
            EntityValue::Entityi64(value) => write!(f, "Entityi64({})", value),
            EntityValue::Entityf64(value) => write!(f, "Entityf64({})", value),
            EntityValue::EntityVeci64(values) => write!(f, "EntityVeci64({:?})", values),
            EntityValue::EntityVecf64(values) => write!(f, "EntityVecf64({:?})", values),
            EntityValue::EntityDataType(value) => write!(f, "EntityDataType({:?})", value),
            EntityValue::EntityBool(value) => write!(f, "EntityBool({:?})", value),
            EntityValue::EntityVec(values) => write!(f, "EntityVec({:?})", values),
            EntityValue::EntityObject(object) => write!(f, "EntityObject({:?})", object),
            EntityValue::EntityVecObject(objects) => write!(f, "EntityVecObject({:?})", objects),
            EntityValue::NestedDynamicEntity(value) => write!(f, "NestedDynamicEntity({})", value),
            EntityValue::Fallback(value) => write!(f, "Fallback({:?})", value),
        }
    }
}

impl EntityValue {
    /// Parses a `&str` into an `EntityValue` without using `serde_json`.
    ///
    /// This function manually checks the input format to determine the appropriate variant.
    /// TODO: This is a work in progress and will probably miss values, grep is always
    /// an alternative
    pub fn parse(input: &str) -> Option<EntityValue> {
        // Handle plain JSON strings (with or without wrapping quotes)
        if let Ok(parsed) = serde_json::from_str::<String>(input) {
            return Some(EntityValue::EntityString(parsed));
        }
        if let Ok(parsed) = serde_json::from_str::<String>(&format!(r#""{}""#, input)) {
            return Some(EntityValue::EntityString(parsed));
        }

        // Handle integers
        if let Ok(parsed) = serde_json::from_str::<i64>(input) {
            return Some(EntityValue::Entityi64(parsed));
        }

        // Handle floating-point numbers
        if let Ok(parsed) = serde_json::from_str::<f64>(input) {
            return Some(EntityValue::Entityf64(parsed));
        }

        // Handle boolean values
        if let Ok(parsed) = serde_json::from_str::<bool>(input) {
            return Some(EntityValue::EntityBool(Some(parsed)));
        }

        // Handle arrays of strings
        if let Ok(parsed) = serde_json::from_str::<Vec<String>>(input) {
            return Some(EntityValue::EntityVecString(parsed));
        }

        // Handle arrays of integers
        if let Ok(parsed) = serde_json::from_str::<Vec<i64>>(input) {
            return Some(EntityValue::EntityVeci64(parsed));
        }

        // Handle arrays of floating-point numbers
        if let Ok(parsed) = serde_json::from_str::<Vec<f64>>(input) {
            return Some(EntityValue::EntityVecf64(parsed));
        }

        // Handle objects (key-value pairs)
        if let Ok(parsed) = serde_json::from_str::<HashMap<String, EntityValue>>(input) {
            return Some(EntityValue::EntityObject(parsed));
        }

        // Handle raw JSON fallback
        if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(input) {
            return Some(EntityValue::Fallback(Some(parsed)));
        }

        // If none of the parsing attempts succeeded, return None
        None
    }
}

/// Tests
#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // Tests for Id
    #[test]
    fn test_contains_id_single() {
        let id = Id::Id("test_id".to_string());
        assert!(id.contains_id("test_id"));
        assert!(!id.contains_id("other_id"));
    }

    #[test]
    fn test_contains_id_in_array() {
        let ids = Id::IdArray(vec!["test_id1".to_string(), "test_id2".to_string()]);
        assert!(ids.contains_id("test_id1"));
        assert!(ids.contains_id("test_id2"));
        assert!(!ids.contains_id("other_id"));
    }

    #[test]
    fn test_contains_id_empty_string() {
        let id = Id::Id("".to_string());
        assert!(!id.contains_id("test_id"));
    }

    // Serialization/Deserialization Tests for Id
    #[test]
    fn test_serialization_deserialization_id() {
        let id = Id::Id("test_id".to_string());
        let serialized = serde_json::to_string(&id).unwrap();
        let deserialized: Id = serde_json::from_str(&serialized).unwrap();
        assert_eq!(serialized, r#"{"@id":"test_id"}"#);
        assert!(deserialized.contains_id("test_id"));
    }

    // Tests for EntityValue
    #[test]
    fn test_dynamic_entity_fallback() {
        let json_value = json!({"unexpected": "data"});
        let entity = EntityValue::Fallback(Some(json_value.clone()));

        match entity {
            EntityValue::Fallback(Some(value)) => assert_eq!(value, json_value),
            _ => panic!("Fallback variant with expected value was not found"),
        }
    }

    #[test]
    fn test_dynamic_entity_string() {
        let entity = EntityValue::EntityString("test".to_string());
        match entity {
            EntityValue::EntityString(value) => assert_eq!(value, "test"),
            _ => panic!("EntityString variant expected"),
        }
    }

    #[test]
    fn test_dynamic_entity_bool() {
        let entity = EntityValue::EntityBool(Some(true));
        match entity {
            EntityValue::EntityBool(Some(value)) => assert!(value),
            _ => panic!("EntityBool variant expected"),
        }
    }

    #[test]
    fn test_dynamic_entity_id() {
        let id = Id::Id("entity_id".to_string());
        let entity = EntityValue::EntityId(id.clone());
        match entity {
            EntityValue::EntityId(e_id) => assert_eq!(e_id, id),
            _ => panic!("EntityId variant expected"),
        }
    }

    #[test]
    fn test_dynamic_entity_i64() {
        let entity = EntityValue::Entityi64(42);
        match entity {
            EntityValue::Entityi64(value) => assert_eq!(value, 42),
            _ => panic!("Entityi64 variant expected"),
        }
    }

    #[test]
    fn test_dynamic_entity_f64() {
        let entity = EntityValue::Entityf64(3.14);
        match entity {
            EntityValue::Entityf64(value) => assert!((value - 3.14).abs() < f64::EPSILON),
            _ => panic!("Entityf64 variant expected"),
        }
    }

    #[test]
    fn test_dynamic_entity_fallback_empty() {
        let json_value = json!({});
        let entity = EntityValue::Fallback(Some(json_value.clone()));
        match entity {
            EntityValue::Fallback(Some(value)) => assert_eq!(value, json_value),
            _ => panic!("Fallback variant with expected value was not found"),
        }
    }

    // Tests for License
    #[test]
    fn test_license_id() {
        let license = License::Id(Id::Id("license_id".to_string()));
        match license {
            License::Id(Id::Id(id_value)) => assert_eq!(id_value, "license_id"),
            _ => panic!("License::Id variant expected"),
        }
    }

    #[test]
    fn test_license_description() {
        let license = License::Description("Creative Commons".to_string());
        match license {
            License::Description(desc) => assert_eq!(desc, "Creative Commons"),
            _ => panic!("License::Description variant expected"),
        }
    }

    // Tests for DataType
    #[test]
    fn test_data_type_term() {
        let data_type = DataType::Term("Text".to_string());
        match data_type {
            DataType::Term(term) => assert_eq!(term, "Text"),
            _ => panic!("DataType::Term variant expected"),
        }
    }

    #[test]
    fn test_data_type_term_array() {
        let data_type = DataType::TermArray(vec!["Text".to_string(), "Image".to_string()]);
        match data_type {
            DataType::TermArray(terms) => assert_eq!(terms, vec!["Text", "Image"]),
            _ => panic!("DataType::TermArray variant expected"),
        }
    }
}
