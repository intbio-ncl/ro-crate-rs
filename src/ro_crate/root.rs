//! Defines the Root data entity for RO-Crates

use crate::ro_crate::constraints::EntityValue;
use crate::ro_crate::constraints::{DataType, License};
use crate::ro_crate::modify::{search_dynamic_entity_for_key, DynamicEntityManipulation};
use serde::ser::SerializeMap;
use serde::{
    de::{self, MapAccess, Visitor},
    Deserialize, Deserializer, Serialize, Serializer,
};
use std::collections::HashMap;
use std::fmt;

use super::constraints::Id;

// The Root data entity struct.
//
// The root data entity is a dataset that represents the RO-Crate as a whole; a
// research object that incldues the data entities and related contextual entities.
//
// # Note
// Should update the type_ and id to follow requirements for future unless spec
// change.
#[derive(Debug, Clone)]
pub struct RootDataEntity {
    // A string that SHOULD be ./ and MUST end with /
    pub id: String,
    // A string that MUST be Dataset
    pub type_: DataType,
    // SHOULD identify the dataset to humans well enough to disambiguate it from
    // other RO-Crates
    pub name: String,
    // SHOULD further elaborate on the name to provide a summary of the context in which
    // the dataset is important
    pub description: String,
    // MUST be a string in ISO 8601 Date format and SHOULD be specified to at least
    // the precision of the day.
    pub date_published: String,
    // Should link to a contextual entity in the RO-Crate metadata file with a
    // name and description.
    pub license: License,
    // Optional Hashmap to enable key/value pair addition depending on crate
    // information.
    pub dynamic_entity: Option<HashMap<String, EntityValue>>,
}

/// Implements the 'Display' trait for RootDataEntity to enable pretty printing
impl fmt::Display for RootDataEntity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Metadata Description: ID={}, Type={:?}, Name={:?}, Description={:?}, Date Published={:?}, License={:?}",
            self.id, self.type_, self.name, self.description, self.date_published, self.license
        )
    }
}
impl RootDataEntity {
    /// Adds the ID of an entity to the part of vec of the RootDataEntity
    pub fn add_entity_to_partof(&mut self, id_target: String) {
        let dynamic_entity = self.dynamic_entity.get_or_insert_with(HashMap::new);

        match dynamic_entity.get_mut("hasPart") {
            // Checks to see if array already present and if the id is already
            // in that array
            Some(EntityValue::EntityId(Id::IdArray(id_array))) => {
                if !id_array.iter().any(|id| id == &id_target) {
                    id_array.push(id_target);
                }
            }
            // Insert a new IdArray with the provided `id` as the first element if `hasPart` does not exist or is not an `IdArray`
            _ => {
                dynamic_entity.insert(
                    "hasPart".to_string(),
                    EntityValue::EntityId(Id::IdArray(vec![id_target])),
                );
            }
        }
    }

    /// Gets value and id of specific property
    pub fn get_property_value(&self, property: &str) -> Option<(String, EntityValue)> {
        // Check the `type` field if it matches the property.
        match property {
            "@type" => Some((
                self.id.clone(),
                EntityValue::EntityDataType(self.type_.clone()),
            )),
            "name" => Some((
                self.id.clone(),
                EntityValue::EntityString(self.name.clone()),
            )),
            "description" => Some((
                self.id.clone(),
                EntityValue::EntityString(self.description.clone()),
            )),
            "license" => Some((
                self.id.clone(),
                EntityValue::EntityLicense(self.license.clone()),
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
}

impl DynamicEntityManipulation for RootDataEntity {
    /// Implements modification of dynamic entity
    fn dynamic_entity(&mut self) -> &mut Option<HashMap<String, EntityValue>> {
        &mut self.dynamic_entity
    }
    /// Allows immutable dynamic entities to be used
    fn dynamic_entity_immut(&self) -> &Option<HashMap<String, EntityValue>> {
        &self.dynamic_entity
    }
}

impl CustomSerialize for RootDataEntity {
    fn dynamic_entity(&self) -> Option<&HashMap<String, EntityValue>> {
        self.dynamic_entity.as_ref()
    }

    fn id(&self) -> &String {
        &self.id
    }

    fn type_(&self) -> &DataType {
        &self.type_
    }

    fn name(&self) -> &String {
        &self.name
    }

    fn description(&self) -> &String {
        &self.description
    }

    fn date_published(&self) -> &String {
        &self.date_published
    }

    fn license(&self) -> &License {
        &self.license
    }
}

impl Serialize for RootDataEntity {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.custom_serialize(serializer)
    }
}

// Custom serailiser for root entity.
pub trait CustomSerialize: Serialize {
    fn dynamic_entity(&self) -> Option<&HashMap<String, EntityValue>>;
    fn id(&self) -> &String;
    fn type_(&self) -> &DataType;
    fn name(&self) -> &String;
    fn description(&self) -> &String;
    fn date_published(&self) -> &String;
    fn license(&self) -> &License;

    fn custom_serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(None)?;

        map.serialize_entry("@id", self.id())?;
        map.serialize_entry("@type", self.type_())?;
        map.serialize_entry("name", self.name())?;
        map.serialize_entry("description", self.description())?;
        map.serialize_entry("datePublished", self.date_published())?;
        map.serialize_entry("license", self.license())?;

        if let Some(dynamic_entity) = self.dynamic_entity() {
            for (k, v) in dynamic_entity {
                map.serialize_entry(k, v)?;
            }
        }

        map.end()
    }
}

impl<'de> Deserialize<'de> for RootDataEntity {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct RootDataEntityVisitor;

        impl<'de> Visitor<'de> for RootDataEntityVisitor {
            type Value = RootDataEntity;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a map representing a RootDataEntity")
            }

            fn visit_map<A>(self, mut map: A) -> Result<RootDataEntity, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut id = None;
                let mut type_ = None;
                let mut name = None;
                let mut description = None;
                let mut date_published = None;
                let mut license = None;
                let mut dynamic_entity: HashMap<String, EntityValue> = HashMap::new();

                while let Some(key) = map.next_key::<String>()? {
                    match key.as_str() {
                        "@id" => id = Some(map.next_value()?),
                        "@type" => type_ = Some(map.next_value()?),
                        "name" => name = Some(map.next_value()?),
                        "description" => description = Some(map.next_value()?),
                        "datePublished" => date_published = map.next_value()?,
                        "license" => license = map.next_value()?,
                        _ => {
                            let value: EntityValue = map.next_value()?;
                            dynamic_entity.insert(key, value);
                        }
                    }
                }

                let id = id.ok_or_else(|| de::Error::missing_field("@id"))?;
                let type_ = type_.ok_or_else(|| de::Error::missing_field("@type"))?;
                let name = name.ok_or_else(|| de::Error::missing_field("name"))?;
                let description =
                    description.ok_or_else(|| de::Error::missing_field("description"))?;
                let date_published =
                    date_published.ok_or_else(|| de::Error::missing_field("datePublished"))?;
                let license = license.ok_or_else(|| de::Error::missing_field("license"))?;

                Ok(RootDataEntity {
                    id,
                    type_,
                    name,
                    description,
                    date_published,
                    license,
                    dynamic_entity: Some(dynamic_entity),
                })
            }
        }

        deserializer.deserialize_map(RootDataEntityVisitor)
    }
}
