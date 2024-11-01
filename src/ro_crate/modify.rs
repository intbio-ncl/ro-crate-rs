//! Module for interacting with dynamic entities
//!
//! A dynamic entity is simply a field that is not a MUST within a defined
//! RO-Crate entity. It can be added, modified or deleted.

use crate::ro_crate::constraints::*;
use serde::ser::SerializeMap;
use serde::{Serialize, Serializer};
use serde_json::Value;
use std::collections::HashMap;

/// A trait for manipulating dynamic entities within an RO-Crate object.
///
/// Provides methods for adding, removing, and searching dynamic fields and values
/// within entities of an RO-Crate. These dynamic entities are often represented as
/// flexible structures, allowing for a varied and extensible set of metadata.
///
/// # Note
/// Additions need to be updated due to evolution of the DynamicEntity type. There
/// will be some redundency.
pub trait DynamicEntityManipulation: Serialize {
    /// Gets a mutable reference to the dynamic entity's underlying HashMap.
    fn dynamic_entity(&mut self) -> &mut Option<HashMap<String, DynamicEntity>>;

    /// Gets an immutable reference to the dynamic entity's underlying HashMap.
    fn dynamic_entity_immut(&self) -> &Option<HashMap<String, DynamicEntity>>;

    /// Adds dynamic fields to the entity
    ///
    /// # Arguments
    /// * 'values' - A 'Hashmap' containing a key and a valid DynamicEntity type
    fn add_dynamic_fields(&mut self, values: HashMap<String, DynamicEntity>) {
        for (key, value) in values {
            match value {
                DynamicEntity::EntityString(s) => self.add_string_value(key, s),
                DynamicEntity::EntityId(id) => self.add_id_value(key, id),
                _ => todo!(),
            }
        }
    }

    /// Adds a string value to the dynamic entity.
    ///
    /// # Arguments
    /// * `key` - The field name as a `String`.
    /// * `value` - The string value to be added.
    fn add_string_value(&mut self, key: String, value: String) {
        // Use `entry` API for a more concise approach
        // This will automatically create the HashMap if it doesn't exist
        self.dynamic_entity()
            .get_or_insert_with(HashMap::new)
            .insert(key, DynamicEntity::EntityString(value));
    }

    /// Adds an identifier value to the dynamic entity.
    ///
    /// # Arguments
    /// * `key` - The field name as a `String`.
    /// * `value` - The `Id` value to be added.    
    fn add_id_value(&mut self, key: String, value: Id) {
        self.dynamic_entity()
            .get_or_insert_with(HashMap::new)
            .insert(key, DynamicEntity::EntityId(value));
    }

    /// Removes a field from the dynamic entity.
    ///
    /// # Arguments
    /// * `key` - The name of the field to remove.
    fn remove_field(&mut self, key: &str) {
        if let Some(dynamic_entity) = self.dynamic_entity() {
            dynamic_entity.remove(key);
        }
    }

    /// Searches for a specific value within the dynamic entity.
    ///
    /// # Arguments
    /// * `search_value` - The `DynamicEntity` value to search for.
    ///
    /// # Returns
    /// `true` if the value is found, otherwise `false`.
    fn search_value(&self, search_value: &DynamicEntity) -> bool {
        if let Some(dynamic_entity) = self.dynamic_entity_immut() {
            for (_key, value) in dynamic_entity.iter() {
                if value == search_value {
                    return true;
                }
            }
        }
        return false;
    }

    /// Finds keys within the RO-Crate matching a specified key or retrieves all keys.
    ///
    /// # Arguments
    /// * `search_key` - A `String` specifying the key to search for.
    /// * `get_all` - A boolean indicating whether to return all keys or only those matching `search_key`.
    ///
    /// # Returns
    /// A vector of strings containing the keys found.
    fn search_keys(&self, search_key: &String, get_all: bool) -> Vec<String> {
        let mut key_vec: Vec<String> = Vec::new();

        /// recursive function for traversing nested objects.
        /// Should probably move out from nest
        fn search_obj(
            object: &HashMap<String, DynamicEntity>,
            search_key: &String,
            get_all: bool,
        ) -> Vec<String> {
            let mut key_vec: Vec<String> = Vec::new();
            for (_key, value) in object.iter() {
                if get_all || _key == search_key {
                    key_vec.push(_key.clone()); // Cloning the key
                }
                if let DynamicEntity::EntityObject(inner_object) = value {
                    let inner_keys = search_obj(inner_object, search_key, get_all);
                    if !inner_keys.is_empty() {
                        key_vec.extend(inner_keys);
                    }
                }
            }
            key_vec
        }

        if let Some(dynamic_entity) = self.dynamic_entity_immut() {
            for (_key, value) in dynamic_entity.iter() {
                if let DynamicEntity::EntityObject(object) = value {
                    let obvec = search_obj(object, search_key, get_all);
                    if !obvec.is_empty() {
                        key_vec.extend(obvec);
                    }
                }
                if get_all {
                    key_vec.push(_key.to_string());
                }
            }
        }
        key_vec
    }

    /// Method to remove a matching value from the dynamic_entity field.
    ///
    /// This can be both a useful and risky tool to use to clean a crate of a particular ID reference
    /// Only ID related entity types and fallback values are allowed to be removed, as these encapsulate
    /// usage of ID which is expected to be 99% of recursive removal. Other types which could be regularly
    /// duplicated and non specific are ignored.
    ///
    /// # Arguements
    /// * 'target_id' - A string representing the id value to be removed
    ///
    /// This is used as part of RoCrate implmentation for removing by ID
    ///
    /// Types that are not allowed to be recursively removed:
    /// Bool  
    /// f64
    /// i64
    ///
    /// It does not allow you to remove values from fields that are defined as MUST within the Ro-Crate spec.
    fn remove_matching_value(&mut self, target_id: &str) {
        if let Some(dynamic_entity) = self.dynamic_entity() {
            let mut keys_to_remove = Vec::new();

            // Collect keys where Fallback values need modification
            let mut fallback_keys_to_modify = Vec::new();
            let mut updates = Vec::new();

            for (key, value) in dynamic_entity.iter() {
                match value {
                    DynamicEntity::EntityString(s) if s == target_id => {
                        keys_to_remove.push(key.clone());
                    }
                    DynamicEntity::EntityId(Id::IdArray(id_values)) => {
                        let filtered_values: Vec<IdValue> = id_values
                            .iter()
                            .filter(|id_val| id_val.id != target_id)
                            .cloned()
                            .collect();

                        if filtered_values.len() != id_values.len() {
                            updates.push((key.clone(), Id::IdArray(filtered_values)));
                        }
                    }
                    DynamicEntity::EntityId(Id::Id(id_value)) if id_value.id == target_id => {
                        keys_to_remove.push(key.clone());
                    }
                    DynamicEntity::Fallback(fallback_values) => {
                        println!("Exploring fallback {:?}", fallback_values);
                        fallback_keys_to_modify.push(key.clone());
                    }
                    // Handle other DynamicEntity types if necessary
                    _ => {
                        //println!("Unknown type {:?}?", value);
                    }
                }
            }

            // Potentially depreciated need to test
            for key in fallback_keys_to_modify {
                if let Some(DynamicEntity::Fallback(fallback_value)) = dynamic_entity.get_mut(&key)
                {
                    remove_matching_value_from_json(fallback_value, target_id);
                }
            }

            // For entity ID's in a vec
            for (key, updated_id) in updates {
                if let Some(DynamicEntity::EntityId(id)) = dynamic_entity.get_mut(&key) {
                    *id = updated_id;
                }
            }

            // Remove the keys that have direct string matches
            for key in keys_to_remove {
                dynamic_entity.remove(&key);
            }
        }
    }
    /// Updates all occurrences of a specific ID with a new ID within the dynamic entity fields.
    ///
    /// This method traverses the dynamic entity fields of an object implementing `DynamicEntityManipulation`,
    /// looking for any entity IDs that match `id_old` and replaces them with `id_new`. It supports updating
    /// both single IDs and IDs within arrays.
    ///
    /// # Arguments
    /// * `id_old` - A string slice representing the old ID to be replaced.
    /// * `id_new` - A string slice representing the new ID to replace with.
    ///
    /// # Returns
    /// Returns `Some(())` if at least one ID was updated, indicating that the operation made changes to the entity.
    /// Returns `None` if no matching IDs were found or no updates were made.
    ///
    /// # Examples
    /// Assuming `entity` is a member of the GraphVector Enum (e.g. GraphVector::DataEntity)
    /// and contains an ID that matches `id_old`:
    /// ```
    /// if entity.update_matching_id("old_id", "new_id").is_some() {
    ///     println!("ID updated successfully.");
    /// } else {
    ///     println!("No matching ID found or no update needed.");
    /// }
    /// ```
    /// # Notes
    /// This function is essential for maintaining referential integrity within the RO-Crate when IDs of entities
    /// are changed. It ensures that all references to the updated entity reflect the new ID.
    fn update_matching_id(&mut self, id_old: &str, id_new: &str) -> Option<()> {
        let mut updated = false;

        if let Some(dynamic_entity) = &mut self.dynamic_entity() {
            for (_key, value) in dynamic_entity.iter_mut() {
                match value {
                    DynamicEntity::EntityId(Id::IdArray(ids)) => {
                        for id in ids {
                            if id.id == id_old {
                                id.id = id_new.to_string();
                                updated = true;
                            }
                        }
                    }
                    DynamicEntity::EntityId(Id::Id(id)) => {
                        if id.id == id_old {
                            id.id = id_new.to_string();
                            updated = true;
                        }
                    }
                    DynamicEntity::EntityIdVec(ids) => {
                        for _id in ids {
                            //if id.id == id_old {
                            //    id.id = id_new.to_string();
                            //    updated = true;
                            //}
                            // TOOD:
                        }
                    }
                    _ => (),
                }
            }
        }

        if updated {
            Some(())
        } else {
            None
        }
    }
}

/// Recursively removes matching values from fallback serailised json objects
///
/// This allows for nested id's to be removed indefinitely from complicated json objects that don't conform
/// to a statically defined DynamicEntity Type. Both single objects and array objects are searched.
///
/// # Arguments
/// * `value` - A mutable reference to a serde_json `Value` that represents the JSON structure to be cleaned.
/// * `target_value` - The value to be searched for and removed from the JSON structure.
///
/// # Notes
/// Potentially depreciated after DynamicEntity Type expansion
/// TODO: Need to test
fn remove_matching_value_from_json(value: &mut Value, target_value: &str) {
    match value {
        Value::Object(obj) => {
            let keys_to_remove: Vec<String> = obj
                .iter()
                .filter_map(|(k, v)| {
                    if v == target_value {
                        Some(k.clone())
                    } else {
                        None
                    }
                })
                .collect();

            for key in keys_to_remove {
                obj.remove(&key);
            }

            for v in obj.values_mut() {
                remove_matching_value_from_json(v, target_value);
            }
        }
        Value::Array(arr) => {
            let mut i = 0;
            while i < arr.len() {
                if &arr[i] == target_value {
                    arr.remove(i);
                } else {
                    remove_matching_value_from_json(&mut arr[i], target_value);
                    i += 1;
                }
            }
        }
        _ => {}
    }
}

/// A trait for custom serialization of complex data structures.
///
/// `CustomSerialize` extends the standard `Serialize` trait to provide additional
/// functionality for serializing data structures that contain both static and dynamic /// fields.
///
/// # Note
/// This is utilised by both DataEntity and ContextualEntity
pub trait CustomSerialize: Serialize {
    fn dynamic_entity(&self) -> Option<&HashMap<String, DynamicEntity>>;
    fn id(&self) -> &String;
    fn type_(&self) -> &DataType;

    fn custom_serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(None)?;

        map.serialize_entry("@id", self.id())?;
        map.serialize_entry("@type", self.type_())?;

        if let Some(dynamic_entity) = self.dynamic_entity() {
            for (k, v) in dynamic_entity {
                map.serialize_entry(k, v)?;
            }
        }

        map.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    struct TestEntity {
        pub id: String,
        pub type_: DataType,
        pub dynamic_entity: Option<HashMap<String, DynamicEntity>>,
    }

    impl DynamicEntityManipulation for TestEntity {
        fn dynamic_entity(&mut self) -> &mut Option<HashMap<String, DynamicEntity>> {
            &mut self.dynamic_entity
        }
        fn dynamic_entity_immut(&self) -> &Option<HashMap<String, DynamicEntity>> {
            &self.dynamic_entity
        }
    }

    impl CustomSerialize for TestEntity {
        fn dynamic_entity(&self) -> Option<&HashMap<String, DynamicEntity>> {
            self.dynamic_entity.as_ref()
        }

        fn id(&self) -> &String {
            &self.id
        }

        fn type_(&self) -> &DataType {
            &self.type_
        }
    }

    impl Serialize for TestEntity {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            self.custom_serialize(serializer)
        }
    }

    #[test]
    fn test_add_string_value() {
        let mut entity = TestEntity {
            id: "test_id".to_string(),
            type_: DataType::Term("test_type".to_string()),
            dynamic_entity: None,
        };

        entity.add_string_value("key1".to_string(), "value1".to_string());
        assert_eq!(
            entity.dynamic_entity.unwrap().get("key1").unwrap(),
            &DynamicEntity::EntityString("value1".to_string())
        );
    }

    #[test]
    fn test_remove_field() {
        let mut entity = TestEntity {
            id: "test_id".to_string(),
            type_: DataType::Term("test_type".to_string()),
            dynamic_entity: Some(HashMap::from([(
                "key1".to_string(),
                DynamicEntity::EntityString("value1".to_string()),
            )])),
        };

        entity.remove_field("key1");
        assert!(entity.dynamic_entity.unwrap().get("key1").is_none());
    }

    #[test]
    fn test_remove_matching_value() {
        let mut entity = TestEntity {
            id: "test_id".to_string(),
            type_: DataType::Term("test_type".to_string()),
            dynamic_entity: Some(HashMap::from([
                (
                    "key1".to_string(),
                    DynamicEntity::EntityString("value1".to_string()),
                ),
                (
                    "key2".to_string(),
                    DynamicEntity::EntityString("value1".to_string()),
                ),
            ])),
        };

        entity.remove_matching_value("value1");
        assert!(entity.dynamic_entity.unwrap().is_empty());
    }

    #[test]
    fn test_custom_serialize() {
        let entity = TestEntity {
            id: "test_id".to_string(),
            type_: DataType::Term("test_type".to_string()),
            dynamic_entity: Some(HashMap::from([(
                "key1".to_string(),
                DynamicEntity::EntityString("value1".to_string()),
            )])),
        };

        let serialized = serde_json::to_string(&entity).unwrap();
        let expected_json = json!({
            "@id": "test_id",
            "@type": "test_type",
            "key1": "value1",
        })
        .to_string();

        assert_eq!(serialized, expected_json);
    }
}
