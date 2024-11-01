//! Python bindings for ro-crate-rs core

mod utils;
extern crate chrono;
use ::rocraters::ro_crate::constraints::*;
use ::rocraters::ro_crate::metadata_descriptor::MetadataDescriptor;
use ::rocraters::ro_crate::root::RootDataEntity;
use ::rocraters::ro_crate::{
    read::{read_crate, read_crate_obj},
    rocrate::{ContextItem, GraphVector, RoCrate, RoCrateContext},
    write::{write_crate as rs_write_crate, zip_crate as rs_zip_crate},
};
use chrono::prelude::*;
use pyo3::exceptions::PyIOError;
use pyo3::{
    prelude::*,
    types::{PyDict, PyList, PyString},
};
use std::collections::HashMap;
use std::path::Path;

/// PyO3 compatible wrapper around RoCrate struct
#[pyclass]
struct PyRoCrate {
    inner: RoCrate,
}

/// PyO3 compatible wrapper around RoCrateContext struct
#[pyclass]
#[derive(Clone)]
struct PyRoCrateContext {
    inner: RoCrateContext,
}

/// CrateContext methods
#[pymethods]
impl PyRoCrateContext {
    /// Crates a RoCrate Context from just a string
    ///
    /// Used for creating a base RoCrate vocab
    #[staticmethod]
    fn from_string(context: &PyString) -> Self {
        PyRoCrateContext {
            inner: RoCrateContext::ReferenceContext(context.to_string()),
        }
    }

    /// Creates heterogenous context
    ///
    /// Allows for a Reference, Embedded and Extended RoCrate context.
    #[staticmethod]
    fn from_list(context: &PyList) -> PyResult<Self> {
        let mut context_items = Vec::new();
        for obj in context.iter() {
            // Check if obj is a string or a dict
            if let Ok(string) = obj.extract::<String>() {
                context_items.push(ContextItem::ReferenceItem(string));
            } else if let Ok(dict) = obj.extract::<&PyDict>() {
                let mut map = HashMap::new();
                for (key, val) in dict.into_iter() {
                    let key_str: String = key.extract()?;
                    let val_str: String = val.extract()?;
                    map.insert(key_str, val_str);
                }
                context_items.push(ContextItem::EmbeddedContext(map));
            } else {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                    "List elements must be either strings or dictionaries",
                ));
            }
        }
        Ok(PyRoCrateContext {
            inner: RoCrateContext::ExtendedContext(context_items),
        })
    }
}

#[pymethods]
impl PyRoCrate {
    /// Crates a new empty RoCrate
    #[new]
    fn new(context: &PyRoCrateContext) -> Self {
        PyRoCrate {
            inner: RoCrate {
                context: context.inner.clone(),
                graph: (Vec::new()),
            },
        }
    }

    #[staticmethod]
    fn new_default() -> Self {
        PyRoCrate::default()
    }

    /// Gets a specified entity based upon ID
    fn get_entity(&mut self, py: Python, id: &str) -> PyResult<PyObject> {
        match self.inner.find_id(id) {
            Some(GraphVector::DataEntity(data_entity)) => {
                utils::base_entity_to_pydict(py, data_entity)
            }
            Some(GraphVector::ContextualEntity(data_entity)) => {
                utils::base_entity_to_pydict(py, data_entity)
            }
            Some(GraphVector::RootDataEntity(root_entity)) => {
                utils::root_entity_to_pydict(py, root_entity)
            }
            Some(GraphVector::MetadataDescriptor(descriptor)) => {
                utils::metadata_descriptor_to_pydict(py, descriptor)
            }
            // Handle other variants or None case
            _ => Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "ID not found or unsupported GraphVector variant",
            )),
        }
    }

    /// Update a data entity with new data
    ///
    /// Lazy update of data entity, finds id and overwrites the index.
    /// Strongly recommended to extract index data, modify, then rewrite the
    /// modified index data as the update.
    fn update_data(&mut self, py: Python, py_obj: PyObject) -> PyResult<()> {
        // Needs to check if data entity first - then parse as contextual if fail
        // if data then append to partOf vec in root.
        let data_entity_wrapper: utils::DataEntityWrapper = py_obj.extract(py)?;
        let data_entity = data_entity_wrapper.0; // Access the inner DataEntity
        let id = data_entity.id.clone();
        let update = GraphVector::DataEntity(data_entity);

        self.inner.overwite_by_id(&id, update);

        Ok(())
    }

    /// Update a contextual entity with new data
    ///
    /// Lazy update of contextual entity, finds id and overwrites the index.
    /// Strongly recommended to extract index data, modify, then rewrite the
    /// modified index data as the update.
    fn update_contextual(&mut self, py: Python, py_obj: PyObject) -> PyResult<()> {
        // Needs to check if data entity first - then parse as contextual if fail
        // if data then append to partOf vec in root.
        let contextual_entity_wrapper: utils::ContextualEntityWrapper = py_obj.extract(py)?;
        let contextual_entity = contextual_entity_wrapper.0; // Access the inner DataEntity
        let id = contextual_entity.id.clone();
        let update = GraphVector::ContextualEntity(contextual_entity);

        self.inner.overwite_by_id(&id, update);

        Ok(())
    }

    /// Update a root entity with new data
    ///
    /// Lazy update of root entity, finds id and overwrites the index.
    /// Strongly recommended to extract index data, modify, then rewrite the
    /// modified index data as the update.
    fn update_root(&mut self, py: Python, py_obj: PyObject) -> PyResult<()> {
        // Needs to check if data entity first - then parse as contextual if fail
        // if data then append to partOf vec in root.
        let root_entity_wrapper: utils::RootDataEntityWrapper = py_obj.extract(py)?;
        let root_entity = root_entity_wrapper.0; // Access the inner DataEntity
        let id = root_entity.id.clone();
        let update = GraphVector::RootDataEntity(root_entity);

        self.inner.overwite_by_id(&id, update);

        Ok(())
    }

    /// Update the metadata descriptor with new data
    ///
    /// Lazy update of metadata descriptor, finds id and overwrites the index.
    /// Strongly recommended to extract index data, modify, then rewrite the
    /// modified index data as the update.
    fn update_descriptor(&mut self, py: Python, py_obj: PyObject) -> PyResult<()> {
        // Needs to check if data entity first - then parse as contextual if fail
        // if data then append to partOf vec in root.
        let descriptor_wrapper: utils::MetadataDescriptorWrapper = py_obj.extract(py)?;
        let descriptor = descriptor_wrapper.0; // Access the inner DataEntity
        let id = descriptor.id.clone();
        let update = GraphVector::MetadataDescriptor(descriptor);

        self.inner.overwite_by_id(&id, update);

        Ok(())
    }

    /// Overwrites an ID with new ID
    ///
    /// Overvwrites an ID with a New ID, and recursively changes every instance
    /// of the old ID within the RO-Crate.
    fn replace_id(&mut self, id_old: &str, id_new: &str) -> PyResult<()> {
        self.inner.update_id_recursive(id_old, id_new);
        Ok(())
    }

    /// Entity deletion both recursive and not
    fn delete_entity(&mut self, id: &str, recursive: bool) -> PyResult<()> {
        self.inner.remove_by_id(id, recursive);
        Ok(())
    }

    /// Writes ro-crate back to ro-crate-metadata.json
    fn write(&self, file_path: Option<String>) -> PyResult<()> {
        let path = file_path.unwrap_or_else(|| "ro-crate-metadata.json".to_string());
        rs_write_crate(&self.inner, path);
        Ok(())
    }

    /// Print's full crate
    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("PyRoCrate(data: '{:#?}')", self.inner))
    }
}

impl From<RoCrate> for PyRoCrate {
    /// Allows simple conversion into rust_struct on read
    fn from(rust_struct: RoCrate) -> Self {
        PyRoCrate { inner: rust_struct }
    }
}

/// Reads a crate into memory allowing manipulation
#[pyfunction]
fn read(relative_path: &str, validation_level: i8) -> PyResult<PyRoCrate> {
    let path = Path::new(relative_path).to_path_buf();
    let rocrate = read_crate(&path, validation_level)
        .map_err(|e| PyIOError::new_err(format!("Failed to read crate: {:#?}", e)))?;
    Ok(PyRoCrate::from(rocrate))
}

/// Reads a json object of a crate into memory allowing manipulation
///
/// Useful for json from browsers/ applications
#[pyfunction]
fn read_obj(obj: &str, validation_level: i8) -> PyResult<PyRoCrate> {
    let rocrate = read_crate_obj(obj, validation_level)
        .map_err(|e| PyIOError::new_err(format!("Failed to read crate: {:#?}", e)))?;
    Ok(PyRoCrate::from(rocrate))
}

/// Targets a ro-crate and zips directory contents
#[pyfunction]
fn zip(crate_path: &str, external: bool, validation_level: i8) -> PyResult<()> {
    let path = Path::new(crate_path).to_path_buf();
    let _ = rs_zip_crate(&path, external, validation_level);
    Ok(())
}

impl Default for PyRoCrate {
    /// Creates a new RoCrate with default requirements
    fn default() -> PyRoCrate {
        let mut rocrate = PyRoCrate {
            inner: RoCrate {
                context: RoCrateContext::ReferenceContext(
                    "https://w3id.org/ro/crate/1.1/context".to_string(),
                ),
                graph: Vec::new(),
            },
        };

        let description = MetadataDescriptor {
            id: "ro-crate-metadata.json".to_string(),
            type_: DataType::Term("CreativeWork".to_string()),
            conforms_to: Id::Id(IdValue {
                id: "https://w3id.org/ro/crate/1.1".to_string(),
            }),
            about: Id::Id(IdValue {
                id: "./".to_string(),
            }),
            dynamic_entity: None,
        };

        let time = Utc::now().to_rfc3339().to_string();

        let root_data_entity = RootDataEntity {
            id: "./".to_string(),
            type_: DataType::Term("Dataset".to_string()),
            name: format!("Default Crate: {time}"),
            description: "Default crate description".to_string(),
            date_published: Utc::now().to_rfc3339().to_string(),
            license: License::Id(Id::Id(IdValue {
                id: "https://creativecommons.org/licenses/by-nc/4.0/deed.en".to_string(),
            })),
            dynamic_entity: None,
        };
        rocrate
            .inner
            .graph
            .push(GraphVector::MetadataDescriptor(description));
        rocrate
            .inner
            .graph
            .push(GraphVector::RootDataEntity(root_data_entity));
        rocrate
    }
}

/// A lightweight Python library for Ro-Crate manipulation implemented in Rust.
#[pymodule]
fn rocraters(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<PyRoCrate>()?;
    m.add_class::<PyRoCrateContext>()?;
    m.add_function(wrap_pyfunction!(read, m)?)?;
    m.add_function(wrap_pyfunction!(read_obj, m)?)?;
    m.add_function(wrap_pyfunction!(zip, m)?)?;
    Ok(())
}