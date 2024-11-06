//! Stuctures for contrained types within an RO-Crate.
//!
//! Focuses on MUST fields as definied by the specification, as well as defining
//! the available types for any additional fields added to an entity object
//! (DynamicEntity)

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Defines a basic ID structure for crate
///
/// Within programs functions as {id: String}, on serialisation will become
/// {"@id": "String"}
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct IdValue {
    #[serde(rename = "@id")]
    pub id: String,
}

/// ID definition in both single and vec format
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[serde(untagged)]
pub enum Id {
    /// Direct ID value for values of {"@id": "id"}
    Id(IdValue),
    /// Array of ID values for values such as [{"@id": "id1"}, {"@id": "id2"}]
    IdArray(Vec<IdValue>),
}

/// Returns true or false depending on whether ID value matched.
/// Useful for matching GraphVector index
impl Id {
    pub fn contains_id(&self, target_id: &str) -> bool {
        match self {
            Id::Id(id_value) => &id_value.id == target_id,
            Id::IdArray(id_values) => id_values.iter().any(|id_val| &id_val.id == target_id),
        }
    }
}

/// Enables License as a defined license or referenced license
/// Required in RoCrate due to MUST specification of root
#[derive(Serialize, Deserialize, Debug)]
#[serde(untagged)]
pub enum License {
    /// Direct ID value for a single license contextual entity
    Id(Id),
    /// Basic license without URI
    Description(String),
}

/// Enables DataType as a single datatype or multiple
/// Required in RoCrate due to MUST specification of all entites
#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum DataType {
    /// Basic Datatype for entities with only one datatype
    Term(String),
    /// Vector of datatypes for entities that can be described with multiple
    /// datatypes
    TermArray(Vec<String>),
}

/// Allow a vec of ids for modification and creation.
/// Allow a new field of struct id thats not a vec
/// Fallback suboptimal but catch all
///
/// NOTE: Need to properly test but I don't think EntityIdVec is called anymore
#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
#[serde(untagged)]
pub enum DynamicEntity {
    EntityString(String),
    EntityVecString(Vec<String>),
    EntityId(Id),
    EntityIdVec(Vec<Id>),
    EntityBool(bool),
    Entityi64(i64),
    Entityf64(f64),
    EntityVeci64(Vec<i64>),
    EntityVecf64(Vec<f64>),
    EntityVec(Vec<DynamicEntity>),
    EntityObject(HashMap<String, DynamicEntity>),
    EntityVecObject(Vec<HashMap<String, DynamicEntity>>),
    NestedDynamicEntity(Box<DynamicEntity>),
    Fallback(Option<serde_json::Value>),
}

/// Tests
#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // Tests for Id
    #[test]
    fn test_contains_id_single() {
        let id = Id::Id(IdValue {
            id: "test_id".to_string(),
        });
        assert!(id.contains_id("test_id"));
        assert!(!id.contains_id("other_id"));
    }

    #[test]
    fn test_contains_id_in_array() {
        let ids = Id::IdArray(vec![
            IdValue {
                id: "test_id1".to_string(),
            },
            IdValue {
                id: "test_id2".to_string(),
            },
        ]);
        assert!(ids.contains_id("test_id1"));
        assert!(ids.contains_id("test_id2"));
        assert!(!ids.contains_id("other_id"));
    }

    #[test]
    fn test_contains_id_empty_string() {
        let id = Id::Id(IdValue { id: "".to_string() });
        assert!(!id.contains_id("test_id"));
    }

    // Serialization/Deserialization Tests for Id
    #[test]
    fn test_serialization_deserialization_id() {
        let id = Id::Id(IdValue {
            id: "test_id".to_string(),
        });
        let serialized = serde_json::to_string(&id).unwrap();
        let deserialized: Id = serde_json::from_str(&serialized).unwrap();
        assert_eq!(serialized, r#"{"@id":"test_id"}"#);
        assert!(deserialized.contains_id("test_id"));
    }

    // Tests for DynamicEntity
    #[test]
    fn test_dynamic_entity_fallback() {
        let json_value = json!({"unexpected": "data"});
        let entity = DynamicEntity::Fallback(Some(json_value.clone()));

        match entity {
            DynamicEntity::Fallback(Some(value)) => assert_eq!(value, json_value),
            _ => panic!("Fallback variant with expected value was not found"),
        }
    }

    #[test]
    fn test_dynamic_entity_string() {
        let entity = DynamicEntity::EntityString("test".to_string());
        match entity {
            DynamicEntity::EntityString(value) => assert_eq!(value, "test"),
            _ => panic!("EntityString variant expected"),
        }
    }

    #[test]
    fn test_dynamic_entity_bool() {
        let entity = DynamicEntity::EntityBool(true);
        match entity {
            DynamicEntity::EntityBool(value) => assert!(value),
            _ => panic!("EntityBool variant expected"),
        }
    }

    #[test]
    fn test_dynamic_entity_id() {
        let id = Id::Id(IdValue {
            id: "entity_id".to_string(),
        });
        let entity = DynamicEntity::EntityId(id.clone());
        match entity {
            DynamicEntity::EntityId(e_id) => assert_eq!(e_id, id),
            _ => panic!("EntityId variant expected"),
        }
    }

    #[test]
    fn test_dynamic_entity_i64() {
        let entity = DynamicEntity::Entityi64(42);
        match entity {
            DynamicEntity::Entityi64(value) => assert_eq!(value, 42),
            _ => panic!("Entityi64 variant expected"),
        }
    }

    #[test]
    fn test_dynamic_entity_f64() {
        let entity = DynamicEntity::Entityf64(3.14);
        match entity {
            DynamicEntity::Entityf64(value) => assert!((value - 3.14).abs() < f64::EPSILON),
            _ => panic!("Entityf64 variant expected"),
        }
    }

    #[test]
    fn test_dynamic_entity_fallback_empty() {
        let json_value = json!({});
        let entity = DynamicEntity::Fallback(json_value.clone());
        match entity {
            DynamicEntity::Fallback(value) => assert_eq!(value, json_value),
            _ => panic!("Fallback variant expected"),
        }
    }

    // Tests for License
    #[test]
    fn test_license_id() {
        let license = License::Id(Id::Id(IdValue {
            id: "license_id".to_string(),
        }));
        match license {
            License::Id(Id::Id(id_value)) => assert_eq!(id_value.id, "license_id"),
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
