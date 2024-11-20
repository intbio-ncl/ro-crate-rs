//! Create the metadata descriptor fo the object.
//!
//! This describes the crate as a whole, specifying it as a versioned crate and
//! the location of the root data entity

use crate::ro_crate::constraints::EntityValue;
use crate::ro_crate::constraints::{DataType, Id};
use crate::ro_crate::modify::*;
use serde::ser::SerializeMap;
use serde::{
    de::{self, MapAccess, Visitor},
    Deserialize, Deserializer, Serialize, Serializer,
};
use std::collections::HashMap;
use std::fmt;

/// Represents the metadata descriptor for entire RoCrate.
///
/// `MetadataDescriptor` is designed to encapsulate metadata information for an ro-crate.
/// It includes the fields that MUST be in included - `id`, `type_`, `conforms_to`, `about`,
/// and a set of dynamic properties for descriptive extension.
#[derive(Debug, Clone)]
pub struct MetadataDescriptor {
    /// ID that MUST be `ro-crate-metadata.json`
    pub id: String,
    /// Type that MUST be `CreativeWork`
    pub type_: DataType,
    /// SHOULD be a versioned permalink URI of the RO-Crate specification that the
    /// crated crate conforms to
    pub conforms_to: Id,
    /// Should reference the root data entity buy using `./`
    pub about: Id,
    /// Optional dynamic entity for further population of key:value pairs
    pub dynamic_entity: Option<HashMap<String, EntityValue>>,
}

/// Custom display formatting for `MetadataDescriptor`
impl fmt::Display for MetadataDescriptor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Metadata Description: ID={}, Type={:?}, conforms_to={:?}, about={:?}",
            self.id, self.type_, self.conforms_to, self.about
        )
    }
}

impl DynamicEntityManipulation for MetadataDescriptor {
    fn dynamic_entity(&mut self) -> &mut Option<HashMap<String, EntityValue>> {
        &mut self.dynamic_entity
    }
    fn dynamic_entity_immut(&self) -> &Option<HashMap<String, EntityValue>> {
        &self.dynamic_entity
    }
}

impl MetadataDescriptor {
    pub fn get_property_value(&self, property: &str) -> Option<(String, EntityValue)> {
        // Check the `type` field if it matches the property.
        match property {
            "@type" => Some((
                self.id.clone(),
                EntityValue::EntityDataType(self.type_.clone()),
            )),
            "conformsTo" => Some((
                self.id.clone(),
                EntityValue::EntityId(self.conforms_to.clone()),
            )),
            "about" => Some((self.id.clone(), EntityValue::EntityId(self.about.clone()))),
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
}

/// Implementation of the `CustomSerialize` trait for `MetadataDescriptor`.
///
/// This implementation provides custom serialization logic for `MetadataDescriptor`,
/// enabling serialization of both its static and dynamic fields.
impl CustomSerialize for MetadataDescriptor {
    fn dynamic_entity(&self) -> Option<&HashMap<String, EntityValue>> {
        self.dynamic_entity.as_ref()
    }

    fn id(&self) -> &String {
        &self.id
    }

    fn type_(&self) -> &DataType {
        &self.type_
    }

    fn conforms_to(&self) -> &Id {
        &self.conforms_to
    }

    fn about(&self) -> &Id {
        &self.about
    }
}

/// Serialize implementation for `MetadataDescriptor`.
///
/// Delegates serialization logic to `custom_serialize` method from `CustomSerialize` trait.
/// This ensures that both static and dynamic fields of `MetadataDescriptor` are serialized correctly.
impl Serialize for MetadataDescriptor {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.custom_serialize(serializer)
    }
}

/// A custom serialization trait extending the standard `Serialize` trait for MetadataDescriptor.
///
/// `CustomSerialize` is designed to provide additional serialization methods
/// for complex data structures, particularly those that include dynamic properties
/// alongside standard serializable fields. This trait is particularly useful for types
/// that contain a mix of static fields (like `id`, `type_`, etc.) and a flexible
/// collection of dynamic properties (like a `HashMap` for additional data).
pub trait CustomSerialize: Serialize {
    fn dynamic_entity(&self) -> Option<&HashMap<String, EntityValue>>;
    fn id(&self) -> &String;
    fn type_(&self) -> &DataType;
    fn conforms_to(&self) -> &Id;
    fn about(&self) -> &Id;

    fn custom_serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(None)?;

        map.serialize_entry("@id", self.id())?;
        map.serialize_entry("@type", self.type_())?;
        map.serialize_entry("conformsTo", self.conforms_to())?;
        map.serialize_entry("about", self.about())?;

        if let Some(dynamic_entity) = self.dynamic_entity() {
            for (k, v) in dynamic_entity {
                map.serialize_entry(k, v)?;
            }
        }

        map.end()
    }
}

/// Custom deserialization implementation for `MetadataDescriptor`.
///
/// This method provides a specialized approach to convert serialized data
/// (like JSON) into a `MetadataDescriptor` instance. It uses a `MetadataDescriptorVisitor`
/// to manage the map-based deserialization process.
///
/// The method expects the serialized data to be in a map format (key-value pairs),
/// aligning with common serialized formats like JSON. It specifically extracts
/// `@id`, `@type`, `conformsTo`, and `about` keys to populate the corresponding fields
/// of `MetadataDescriptor`. All other keys are treated as dynamic properties and are
/// stored in the `dynamic_entity` hashmap.
impl<'de> Deserialize<'de> for MetadataDescriptor {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct MetadataDescriptorVisitor;

        impl<'de> Visitor<'de> for MetadataDescriptorVisitor {
            type Value = MetadataDescriptor;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a map representing a MetadataDescriptorEntity")
            }

            fn visit_map<A>(self, mut map: A) -> Result<MetadataDescriptor, A::Error>
            where
                A: MapAccess<'de>,
            {
                // Fields initialization
                let mut id = None;
                let mut type_ = None;
                let mut conforms_to = None;
                let mut about = None;
                let mut dynamic_entity: HashMap<String, EntityValue> = HashMap::new();

                // Iterating through the map
                while let Some(key) = map.next_key::<String>()? {
                    match key.as_str() {
                        "@id" => id = Some(map.next_value()?),
                        "@type" => type_ = Some(map.next_value()?),
                        "conformsTo" => conforms_to = Some(map.next_value()?),
                        "about" => about = Some(map.next_value()?),
                        _ => {
                            let value: EntityValue = map.next_value()?;
                            dynamic_entity.insert(key, value);
                        }
                    }
                }

                // Finalizing the object
                let id = id.ok_or_else(|| de::Error::missing_field("@id"))?;
                let type_ = type_.ok_or_else(|| de::Error::missing_field("@type"))?;
                let conforms_to =
                    conforms_to.ok_or_else(|| de::Error::missing_field("conformsTo"))?;
                let about = about.ok_or_else(|| de::Error::missing_field("about"))?;

                Ok(MetadataDescriptor {
                    id,
                    type_,
                    conforms_to,
                    about,
                    dynamic_entity: Some(dynamic_entity),
                })
            }
        }

        deserializer.deserialize_map(MetadataDescriptorVisitor)
    }
}

/// Tests
#[cfg(test)]
mod tests {
    use super::*;

    // Test for creating a MetadataDescriptor instance
    #[test]
    fn test_metadata_descriptor_creation() {
        let metadata = MetadataDescriptor {
            id: "metadata_id".to_string(),
            type_: DataType::Term("MetadataType".to_string()),
            conforms_to: Id::Id("conformsTo_id".to_string()),
            about: Id::Id("about_id".to_string()),
            dynamic_entity: Some(HashMap::new()),
        };

        assert_eq!(metadata.id, "metadata_id");
        assert!(matches!(metadata.type_, DataType::Term(ref t) if t == "MetadataType"));
        assert_eq!(metadata.conforms_to, Id::Id("conformsTo_id".to_string()));
        assert_eq!(metadata.about, Id::Id("about_id".to_string()));
        assert!(metadata.dynamic_entity.unwrap().is_empty());
    }

    // Test for manipulating dynamic properties of MetadataDescriptor
    #[test]
    fn test_dynamic_entity_manipulation() {
        let mut metadata = MetadataDescriptor {
            id: "metadata_id".to_string(),
            type_: DataType::Term("MetadataType".to_string()),
            conforms_to: Id::Id("conformsTo_id".to_string()),
            about: Id::Id("about_id".to_string()),
            dynamic_entity: Some(HashMap::new()),
        };

        // Adding a dynamic entity
        metadata.add_string_value("key1".to_string(), "value1".to_string());
        assert_eq!(
            metadata.dynamic_entity.as_ref().unwrap().get("key1"),
            Some(&EntityValue::EntityString("value1".to_string()))
        );

        // Removing a dynamic entity
        metadata.remove_field("key1");
        assert!(metadata
            .dynamic_entity
            .as_ref()
            .unwrap()
            .get("key1")
            .is_none());
    }

    // Test serialization of MetadataDescriptor
    #[test]
    fn test_serialization() {
        let metadata = MetadataDescriptor {
            id: "metadata_id".to_string(),
            type_: DataType::Term("MetadataType".to_string()),
            conforms_to: Id::Id("conformsTo_id".to_string()),
            about: Id::Id("about_id".to_string()),
            dynamic_entity: Some(HashMap::new()),
        };

        let serialized = serde_json::to_string(&metadata).unwrap();
        assert_eq!(
            serialized,
            r#"{"@id":"metadata_id","@type":"MetadataType","conformsTo":{"@id":"conformsTo_id"},"about":{"@id":"about_id"}}"#
        );
    }

    // Test deserialization of MetadataDescriptor
    #[test]
    fn test_deserialization() {
        let json_data = r#"
            {
                "@id": "metadata_id",
                "@type": "MetadataType",
                "conformsTo": {"@id": "conformsTo_id"},
                "about": {"@id": "about_id"}
            }
        "#;
        let deserialized: MetadataDescriptor = serde_json::from_str(json_data).unwrap();

        assert_eq!(deserialized.id, "metadata_id");
        assert!(matches!(deserialized.type_, DataType::Term(ref t) if t == "MetadataType"));
        assert_eq!(
            deserialized.conforms_to,
            Id::Id("conformsTo_id".to_string())
        );
        assert_eq!(deserialized.about, Id::Id("about_id".to_string()));
    }

    // Test deserialization error with missing fields
    #[test]
    fn test_deserialization_with_missing_fields() {
        let json_data = r#"
            {
                "@id": "metadata_id",
                "@type": "MetadataType"
            }
        "#;
        let result: Result<MetadataDescriptor, _> = serde_json::from_str(json_data);
        assert!(result.is_err());
    }
}
