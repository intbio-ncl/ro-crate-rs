//! Utility functions for rust type conversions and python accessiblity

use ::rocraters::ro_crate::contextual_entity::ContextualEntity;
use ::rocraters::ro_crate::data_entity::DataEntity;
use ::rocraters::ro_crate::{
    constraints::{DataType, EntityValue, Id, License},
    metadata_descriptor::MetadataDescriptor,
    root::RootDataEntity,
};
use pyo3::exceptions::PyTypeError;
use pyo3::{
    prelude::*,
    types::{PyBool, PyDict, PyFloat, PyList, PyString},
};
use serde_json::Value;
use std::collections::HashMap;

pub trait EntityTrait {
    fn id(&self) -> &str;
    fn data_type(&self) -> &DataType;
    fn dynamic_entity(&self) -> &Option<HashMap<String, EntityValue>>;
}

impl EntityTrait for DataEntity {
    fn id(&self) -> &str {
        &self.id
    }

    fn data_type(&self) -> &DataType {
        &self.type_
    }

    fn dynamic_entity(&self) -> &Option<HashMap<String, EntityValue>> {
        &self.dynamic_entity
    }
}

impl EntityTrait for ContextualEntity {
    fn id(&self) -> &str {
        &self.id
    }

    fn data_type(&self) -> &DataType {
        &self.type_
    }

    fn dynamic_entity(&self) -> &Option<HashMap<String, EntityValue>> {
        &self.dynamic_entity
    }
}

/// Converts base entities (data and contextual) to python dicts
pub fn base_entity_to_pydict<T: EntityTrait>(py: Python, entity: &T) -> PyResult<PyObject> {
    let py_dict = PyDict::new(py);

    // Now use the shared trait methods to access fields.
    py_dict.set_item("id", entity.id())?;

    match entity.data_type() {
        DataType::Term(term) => {
            py_dict.set_item("type", term)?;
        }
        DataType::TermArray(terms) => {
            let py_terms = terms
                .iter()
                .map(|term| PyString::new(py, term))
                .collect::<Vec<_>>();
            py_dict.set_item("type", &py_terms)?;
        }
    }

    // Directly add dynamic_entity entries to the base dictionary
    if let Some(dynamic_entity) = entity.dynamic_entity() {
        for (key, value) in dynamic_entity.iter() {
            // Convert each EntityValue to a PyObject and insert it directly into py_dict
            py_dict.set_item(key, convert_dynamic_entity_to_pyobject(py, value))?;
        }
    }

    Ok(py_dict.into())
}

/// Converts root metadata entity to py dict
pub fn root_entity_to_pydict(py: Python, entity: &RootDataEntity) -> PyResult<PyObject> {
    let py_dict = PyDict::new(py);

    py_dict.set_item("id", &entity.id)?;

    match &entity.type_ {
        DataType::Term(term) => {
            py_dict.set_item("type", term).unwrap();
        }
        DataType::TermArray(terms) => {
            let py_terms = terms
                .iter()
                .map(|term| PyString::new(py, term))
                .collect::<Vec<_>>();
            py_dict.set_item("type", py_terms).unwrap();
        }
    }
    py_dict.set_item("name", &entity.name)?;
    py_dict.set_item("description", &entity.description)?;
    py_dict.set_item("datePublished", &entity.date_published)?;

    let license_py_object = convert_license_to_pyobject(py, &entity.license);
    py_dict.set_item("license", license_py_object).unwrap();

    // Directly add dynamic_entity entries to the base dictionary
    if let Some(dynamic_entity) = &entity.dynamic_entity {
        for (key, value) in dynamic_entity.iter() {
            // Convert each EntityValue to a PyObject and insert it directly into py_dict
            py_dict.set_item(key, convert_dynamic_entity_to_pyobject(py, value))?;
        }
    }

    Ok(py_dict.into())
}

/// Converts metadata descriptor to pydict
pub fn metadata_descriptor_to_pydict(
    py: Python,
    descriptor: &MetadataDescriptor,
) -> PyResult<PyObject> {
    let py_dict = PyDict::new(py);

    py_dict.set_item("id", &descriptor.id)?;

    match &descriptor.type_ {
        DataType::Term(term) => {
            py_dict.set_item("type", term).unwrap();
        }
        DataType::TermArray(terms) => {
            let py_terms = terms
                .iter()
                .map(|term| PyString::new(py, term))
                .collect::<Vec<_>>();
            py_dict.set_item("type", py_terms).unwrap();
        }
    }

    let py_object = convert_id_to_pyobject(py, &descriptor.conforms_to)
        .expect("Failed to convert Id to PyObject");
    py_dict.set_item("conformsTo", py_object).unwrap();

    let py_object =
        convert_id_to_pyobject(py, &descriptor.about).expect("Failed to convert Id to PyObject");
    py_dict.set_item("about", py_object).unwrap();

    // Directly add dynamic_entity entries to the base dictionary
    if let Some(dynamic_entity) = &descriptor.dynamic_entity {
        for (key, value) in dynamic_entity.iter() {
            // Convert each EntityValue to a PyObject and insert it directly into py_dict
            py_dict.set_item(key, convert_dynamic_entity_to_pyobject(py, value))?;
        }
    }

    Ok(py_dict.into())
}

/// Converts a license type to a pyobject
pub fn convert_license_to_pyobject(py: Python, license_opt: &License) -> PyObject {
    match license_opt {
        License::Id(id_enum) => convert_id_to_pyobject(py, id_enum).unwrap(),
        License::Description(description) => PyString::new(py, description)
            .into_pyobject(py)
            .unwrap()
            .into(),
    }
}

/// Converts dynamic entities into pyobjects for dict representation
pub fn convert_dynamic_entity_to_pyobject(py: Python, value: &EntityValue) -> PyObject {
    match value {
        EntityValue::EntityString(s) => PyString::new(py, s).into(),
        EntityValue::EntityVecString(vec) => {
            let py_list = PyList::new(py, vec.iter().map(|s| PyString::new(py, s))).unwrap();
            py_list.into()
        }
        EntityValue::EntityId(id) => convert_id_to_pyobject(py, id).unwrap(),
        EntityValue::EntityLicense(license) => match license {
            License::Id(id) => convert_id_to_pyobject(py, id).unwrap(),
            License::Description(s) => PyString::new(py, s).into(),
        },
        EntityValue::EntityDataType(data_type) => match data_type {
            DataType::Term(s) => PyString::new(py, s).into(),
            DataType::TermArray(vec) => {
                let py_list = PyList::new(py, vec.iter().map(|s| PyString::new(py, s))).unwrap();
                py_list.into()
            }
        },
        EntityValue::EntityBool(b) => b.into_py(py),
        EntityValue::Entityi64(num) => (num).into_pyobject(py).unwrap().into(),
        EntityValue::Entityf64(num) => PyFloat::new(py, *num).into(),
        EntityValue::EntityVeci64(vec) => {
            let py_list =
                PyList::new(py, vec.iter().map(|&num| (num).into_pyobject(py).unwrap())).unwrap();
            py_list.into()
        }
        EntityValue::EntityVecf64(vec) => {
            let py_list = PyList::new(py, vec.iter().map(|&num| PyFloat::new(py, num))).unwrap();
            py_list.into()
        }
        EntityValue::EntityVec(vec) => {
            let py_list = PyList::new(
                py,
                vec.iter()
                    .map(|entity| convert_dynamic_entity_to_pyobject(py, entity)),
            )
            .unwrap();
            py_list.into()
        }
        EntityValue::EntityObject(map) => {
            let py_dict = PyDict::new(py);
            for (key, val) in map {
                py_dict
                    .set_item(key, convert_dynamic_entity_to_pyobject(py, val))
                    .unwrap();
            }
            py_dict.into()
        }
        EntityValue::EntityVecObject(vec) => {
            let py_list = PyList::new(
                py,
                vec.iter().map(|map| {
                    let py_dict = PyDict::new(py);
                    for (key, val) in map {
                        py_dict
                            .set_item(key, convert_dynamic_entity_to_pyobject(py, val))
                            .unwrap();
                    }
                    py_dict.into_pyobject(py).unwrap() // Explicitly convert to PyObject
                }),
            )
            .unwrap();
            py_list.into() // Convert the PyList to PyObject
        }
        EntityValue::NestedDynamicEntity(boxed_entity) => {
            convert_dynamic_entity_to_pyobject(py, boxed_entity)
        }
        EntityValue::EntityNull(n) => {
            match n {
                Some(value) => PyString::new(py, "Null").into(), // If it's a bool, convert it
                None => py.None().into_py(py), // If it's None, keep it as None in Python
            }
        }
        EntityValue::EntityNone(n) => {
            match n {
                Some(value) => PyString::new(py, "Null").into(), // If it's a bool, convert it
                None => py.None().into_py(py), // If it's None, keep it as None in Python
            }
        }
        EntityValue::Fallback(value_option) => {
            // Convert serde_json::Value to PyObject
            if let Some(value) = value_option {
                // Convert serde_json::Value to PyObject when there's a value
                convert_serde_json_value_to_pyobject(py, value)
            } else {
                // Handle the case where Fallback contains None (i.e., represents null)
                convert_serde_json_value_to_pyobject(py, &serde_json::Value::Null)
            }
        }
    }
}

// Function to handle conversion of serde_json::Value
pub fn convert_serde_json_value_to_pyobject(py: Python, value: &Value) -> PyObject {
    match value {
        Value::String(s) => PyString::new(py, s).into(),
        Value::Number(num) => {
            if let Some(i) = num.as_i64() {
                i.into_pyobject(py).unwrap().into()
            } else if let Some(f) = num.as_f64() {
                PyFloat::new(py, f).into()
            } else {
                PyString::new(py, &num.to_string()).into()
            }
        }
        // Handle other serde_json::Value types as needed
        // ...
        _ => PyString::new(py, "Unsupported serde_json::Value type").into(),
    }
}

/// Converts an id value to pyobject
fn convert_id_to_pyobject(py: Python, id: &Id) -> PyResult<PyObject> {
    match id {
        Id::Id(id_value) => {
            let py_dict = PyDict::new(py);
            py_dict.set_item("id", PyString::new(py, id_value))?;
            Ok(py_dict.into_pyobject(py).unwrap().into())
        }
        Id::IdArray(id_values) => {
            let dicts: Vec<PyObject> = id_values
                .iter()
                .map(|id_val| {
                    let py_dict = PyDict::new(py);
                    py_dict
                        .set_item("id", PyString::new(py, id_val))
                        .expect("Failed to set 'id' key");
                    py_dict.into_pyobject(py).unwrap().into()
                })
                .collect();

            let py_list = PyList::new(py, &dicts).unwrap().into();
            Ok(py_list)
        }
    }
}

//New type pattern for DataEntity
pub struct DataEntityWrapper(pub DataEntity);
impl<'py> FromPyObject<'py> for DataEntityWrapper {
    fn extract_bound(obj: &Bound<'py, PyAny>) -> PyResult<Self> {
        let py_dict: Bound<'_, PyDict> = obj.clone().downcast_into::<PyDict>().unwrap();

        // Extract the "id" and "type_" fields explicitly
        let id: String = match py_dict.get_item("id") {
            Ok(str) => str.unwrap().to_string(),
            Err(e) => return Err(e),
        };

        let type_ = create_data_type_from_dict(&py_dict)?;

        // Initialize an empty HashMap to hold dynamic_entity entries
        let mut dynamic_entity_map: HashMap<String, EntityValue> = HashMap::new();

        // Iterate over the dictionary, excluding "id" and "type" keys
        for (key, value) in py_dict.into_iter() {
            let key_str: String = key.extract()?; // Extract key as String
            if key_str != "id" && key_str != "type" {
                let dynamic_entity = convert_pyobject_to_dynamic_entity(&value)?;
                // Convert value to EntityValue and insert into the map
                dynamic_entity_map.insert(key_str, dynamic_entity);
            }
        }

        // Construct DataEntity, wrapping all dynamic entities in Some if not empty, else None
        let dynamic_entity = if !dynamic_entity_map.is_empty() {
            Some(dynamic_entity_map)
        } else {
            None
        };

        Ok(DataEntityWrapper(DataEntity {
            id,
            type_,
            dynamic_entity,
        }))
    }
}

//New type pattern for ContextualEntity
pub struct ContextualEntityWrapper(pub ContextualEntity);
impl<'py> FromPyObject<'py> for ContextualEntityWrapper {
    fn extract_bound(obj: &Bound<'py, PyAny>) -> PyResult<Self> {
        let py_dict: Bound<'_, PyDict> = obj.clone().downcast_into::<PyDict>().unwrap();

        // Extract the "id" and "type_" fields explicitly
        let id: String = match py_dict.get_item("id") {
            Ok(str) => str.unwrap().to_string(),
            Err(e) => return Err(e),
        };
        let type_ = create_data_type_from_dict(&py_dict)?;

        // Initialize an empty HashMap to hold dynamic_entity entries
        let mut dynamic_entity_map: HashMap<String, EntityValue> = HashMap::new();

        // Iterate over the dictionary, excluding "id" and "type" keys
        for (key, value) in py_dict.into_iter() {
            let key_str: String = key.extract()?; // Extract key as String
            if key_str != "id" && key_str != "type" {
                let dynamic_entity = convert_pyobject_to_dynamic_entity(&value)?;
                // Convert value to EntityValue and insert into the map
                dynamic_entity_map.insert(key_str, dynamic_entity);
            }
        }

        // Construct DataEntity, wrapping all dynamic entities in Some if not empty, else None
        let dynamic_entity = if !dynamic_entity_map.is_empty() {
            Some(dynamic_entity_map)
        } else {
            None
        };

        Ok(ContextualEntityWrapper(ContextualEntity {
            id,
            type_,
            dynamic_entity,
        }))
    }
}

pub struct RootDataEntityWrapper(pub RootDataEntity);
impl<'py> FromPyObject<'py> for RootDataEntityWrapper {
    fn extract_bound(obj: &Bound<'py, PyAny>) -> PyResult<Self> {
        let py_dict: Bound<'_, PyDict> = obj.clone().downcast_into::<PyDict>().unwrap();

        // Extract the "id" and "type_" fields explicitly
        let id: String = match py_dict.get_item("id") {
            Ok(str) => str.unwrap().to_string(),
            Err(e) => return Err(e),
        };
        let type_ = create_data_type_from_dict(&py_dict)?;

        let name: String = match py_dict.get_item("name") {
            Ok(str) => str.unwrap().to_string(),
            Err(e) => return Err(e),
        };

        let description = match py_dict.get_item("description") {
            Ok(str) => str.unwrap().to_string(),
            Err(e) => return Err(e),
        };

        let license = match py_dict.get_item("license") {
            Ok(license_obj) => convert_pyobject_to_license(&license_obj.unwrap())?,
            Err(e) => return Err(e),
        };

        let date_published = match py_dict.get_item("datePublished") {
            Ok(str) => str.unwrap().to_string(),
            Err(e) => return Err(e),
        };

        // Initialize an empty HashMap to hold dynamic_entity entries
        let mut dynamic_entity_map: HashMap<String, EntityValue> = HashMap::new();

        // Iterate over the dictionary, excluding "id" and "type" keys
        for (key, value) in py_dict.into_iter() {
            let key_str: String = key.extract()?; // Extract key as String
            if key_str != "id"
                && key_str != "type"
                && key_str != "name"
                && key_str != "description"
                && key_str != "datePublished"
                && key_str != "license"
            {
                let dynamic_entity = convert_pyobject_to_dynamic_entity(&value)?;
                // Convert value to EntityValue and insert into the map
                dynamic_entity_map.insert(key_str, dynamic_entity);
            }
        }

        // Construct DataEntity, wrapping all dynamic entities in Some if not empty, else None
        let dynamic_entity = if !dynamic_entity_map.is_empty() {
            Some(dynamic_entity_map)
        } else {
            None
        };

        Ok(RootDataEntityWrapper(RootDataEntity {
            id,
            type_,
            name,
            description,
            date_published,
            license,
            dynamic_entity,
        }))
    }
}

pub struct MetadataDescriptorWrapper(pub MetadataDescriptor);
impl<'py> FromPyObject<'py> for MetadataDescriptorWrapper {
    fn extract_bound(obj: &Bound<'py, PyAny>) -> PyResult<Self> {
        let py_dict: Bound<'_, PyDict> = obj.clone().downcast_into::<PyDict>().unwrap(); // Safely cast the PyAny to PyDict

        // Extract the "id" and "type_" fields explicitly
        let id: String = match py_dict.get_item("id") {
            Ok(str) => str.unwrap().to_string(),
            Err(e) => return Err(e),
        };
        let type_ = create_data_type_from_dict(&py_dict)?;

        // This won't work because it cant pick the key TODO
        let conforms_to = if let Ok(value) = py_dict.get_item("conformsTo") {
            convert_dict_to_id(&value.unwrap())?
        } else {
            todo!()
        };

        let about = if let Ok(about_check) = py_dict.get_item("about") {
            convert_dict_to_id(&about_check.unwrap())?
        } else {
            todo!()
        };

        // Initialize an empty HashMap to hold dynamic_entity entries
        let mut dynamic_entity_map: HashMap<String, EntityValue> = HashMap::new();

        // Iterate over the dictionary, excluding "id" and "type" keys
        for (key, value) in py_dict.into_iter() {
            let key_str: String = key.extract()?; // Extract key as String
            if key_str != "id" && key_str != "type" && key_str != "conformsTo" && key_str != "about"
            {
                let dynamic_entity = convert_pyobject_to_dynamic_entity(&value)?;
                // Convert value to EntityValue and insert into the map
                dynamic_entity_map.insert(key_str, dynamic_entity);
            }
        }

        // Construct DataEntity, wrapping all dynamic entities in Some if not empty, else None
        let dynamic_entity = if !dynamic_entity_map.is_empty() {
            Some(dynamic_entity_map)
        } else {
            None
        };

        Ok(MetadataDescriptorWrapper(MetadataDescriptor {
            id,
            type_,
            conforms_to,
            about,
            dynamic_entity,
        }))
    }
}

fn create_data_type_from_dict(input: &Bound<'_, PyDict>) -> PyResult<DataType> {
    if let Ok(value) = input.get_item("type") {
        if let Ok(s) = value.clone().unwrap().extract::<&str>() {
            Ok(DataType::Term(s.to_string()))
        } else if let Ok(arr) = value.unwrap().extract::<Vec<String>>() {
            Ok(DataType::TermArray(
                arr.into_iter().map(String::from).collect(),
            ))
        } else {
            Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "The 'type' key must be associated with a string or a list of strings",
            ))
        }
    } else {
        Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "Dictionary must contain the 'type' key",
        ))
    }
}

fn convert_pyobject_to_license(input: &Bound<'_, PyAny>) -> Result<License, PyErr> {
    // Attempt to extract the input as an Id using the previously defined function
    match convert_dict_to_id(input) {
        Ok(id) => Ok(License::Id(id)),
        Err(_) => {
            // If it fails, then try to extract a description as a fallback
            if let Ok(description) = input.extract::<String>() {
                return Ok(License::Description(description));
            }
            // If both attempts fail, return a custom PyTypeError
            Err(PyTypeError::new_err("Input cannot be converted to License"))
        }
    }
}

fn convert_dict_to_id(input: &Bound<'_, PyAny>) -> PyResult<Id> {
    // Check if input is a single object with "id"
    // Converts to pydidct then checks id
    if let Ok(py_dict) = input.downcast::<PyDict>() {
        if let Ok(id_str) = py_dict.get_item("id") {
            return Ok(Id::Id(id_str.unwrap().to_string()));
        }
    }

    // Check if input is a list of objects each with "id"
    if let Ok(py_list) = input.downcast::<PyList>() {
        let mut id_values: Vec<String> = Vec::new();
        for item in py_list {
            if let Ok(py_dict) = item.downcast::<PyDict>() {
                if let Ok(id_str) = py_dict.get_item("id") {
                    id_values.push(id_str.unwrap().to_string());
                } else {
                    return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "List items must be dictionaries with an 'id' key",
                    ));
                }
            }
        }
        if !id_values.is_empty() {
            return Ok(Id::IdArray(id_values));
        }
    }

    // If neither case matches, return an error
    Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
        "Input must be a dictionary with an 'id' key or a list of such dictionaries",
    ))
}

// converts a PyObject to any required dynamic entity
fn convert_pyobject_to_dynamic_entity(obj: &Bound<'_, PyAny>) -> PyResult<EntityValue> {
    // String
    if let Ok(s) = obj.extract::<String>() {
        return Ok(EntityValue::EntityString(s));
    }
    // Vec<String>
    if let Ok(vec_str) = obj.extract::<Vec<String>>() {
        return Ok(EntityValue::EntityVecString(vec_str));
    }
    // Boolean
    if let Ok(b) = obj.extract::<bool>() {
        return Ok(EntityValue::EntityBool(b));
    }
    // i64
    if let Ok(i) = obj.extract::<i64>() {
        return Ok(EntityValue::Entityi64(i));
    }
    // f64
    if let Ok(f) = obj.extract::<f64>() {
        return Ok(EntityValue::Entityf64(f));
    }
    // Vec<i64>
    if let Ok(vec_i64) = obj.extract::<Vec<i64>>() {
        return Ok(EntityValue::EntityVeci64(vec_i64));
    }
    // Vec<f64>
    if let Ok(vec_f64) = obj.extract::<Vec<f64>>() {
        return Ok(EntityValue::EntityVecf64(vec_f64));
    }
    // Id or Vec<Id>
    if let Ok(id) = convert_dict_to_id(obj) {
        return Ok(EntityValue::EntityId(id));
    }

    // Check if the object is None
    if obj.is_none() {
        // Directly return if obj is Python None
        return Ok(EntityValue::EntityString("None".to_string()));
    }

    if let Ok(list) = obj.downcast::<PyList>() {
        let mut vec = Vec::new();
        for item in list {
            let entity = convert_pyobject_to_dynamic_entity(&item)?;
            vec.push(entity);
        }
        return Ok(EntityValue::EntityVec(vec));
    }

    // HashMap<String, EntityValue> or Vec<HashMap<String, EntityValue>>
    if let Ok(dict) = obj.downcast::<PyDict>() {
        let mut map: HashMap<String, EntityValue> = HashMap::new();
        for (k, v) in dict {
            let key: String = k.extract()?;
            let value: EntityValue = convert_pyobject_to_dynamic_entity(&v)?;
            map.insert(key, value);
        }
        Ok(EntityValue::EntityObject(map))
    } else {
        Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "Data type unavailable",
        ))
    }
}
