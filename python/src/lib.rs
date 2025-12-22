//! Python bindings for ro-crate-rs core

mod utils;
extern crate chrono;
use ::rocraters::ro_crate::constraints::*;
use ::rocraters::ro_crate::context::{ContextItem, RoCrateContext};
use ::rocraters::ro_crate::convert::{to_df, write_csv, write_parquet as wr_par};
use ::rocraters::ro_crate::graph_vector::GraphVector;
use ::rocraters::ro_crate::metadata_descriptor::MetadataDescriptor;
use ::rocraters::ro_crate::object_storage::relative_to_object_store;
use ::rocraters::ro_crate::read::parse_zip;
use ::rocraters::ro_crate::root::RootDataEntity;
use ::rocraters::ro_crate::{
    read::{read_crate, read_crate_obj},
    rocrate::RoCrate,
    write::{write_crate as rs_write_crate, zip_crate as rs_zip_crate},
};
use chrono::prelude::*;
use pyo3::exceptions::PyIOError;
use pyo3::{
    prelude::*,
    types::{PyDict, PyList, PyString},
};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

/// PyO3 compatible wrapper around RoCrate struct
#[pyclass]
#[derive(Debug)]
struct PyRoCrate {
    inner: RoCrate,
}

/// PyO3 compatible wrapper around RoCrateContext struct
#[pyclass]
#[derive(Clone, Debug)]
struct PyRoCrateContext {
    inner: RoCrateContext,
}

/// CrateContext methods
#[pymethods]
impl PyRoCrateContext {
    /// Creates a context from a single string reference.
    ///
    /// # Arguments
    /// * `context` - A string reference to a context URL or term.
    ///
    /// # Returns
    /// A `PyRoCrateContext` containing a reference context.
    #[staticmethod]
    fn from_string(context: &str) -> Self {
        PyRoCrateContext {
            inner: RoCrateContext::ReferenceContext(context.to_string()),
        }
    }

    /// Creates heterogenous context
    ///
    /// Allows for a Reference, Embedded and Extended RoCrate context.
    ///
    /// Arguments
    /// * `context` - A Python list (`PyList`) whose elements are either:
    ///     - String references representing context URLs/terms.
    ///     - Python dictionaries representing embedded context mappings.
    ///
    /// # Errors
    /// Raises a `ValueError` if the list contains elements that are not strings or dicts.
    ///
    /// # Returns
    /// A `PyRoCrateContext` containing an extended context.
    #[staticmethod]
    fn from_list<'py>(context: Bound<'py, PyList>) -> PyResult<Self> {
        let mut context_items = Vec::new();
        for obj in context.iter() {
            // Check if obj is a string or a dict
            if let Ok(string) = obj.extract() {
                context_items.push(ContextItem::ReferenceItem(string));
            } else if let Ok(dict) = obj.clone().downcast_into::<PyDict>() {
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
    /// Creates a new, empty RO-Crate with the specified context.
    ///
    /// # Arguments
    /// * `context` - A `PyRoCrateContext` specifying how the crate’s context is defined.
    ///
    /// # Returns
    /// A new, empty `PyRoCrate`.
    #[new]
    fn new(context: &PyRoCrateContext) -> Self {
        PyRoCrate {
            inner: RoCrate {
                context: context.inner.clone(),
                graph: (Vec::new()),
            },
        }
    }

    /// Creates a new default RO-Crate.
    ///
    /// # Returns
    /// A new `PyRoCrate` populated with default metadata and a root dataset entity.
    #[staticmethod]
    fn new_default() -> Self {
        PyRoCrate::default()
    }

    /// Returns all contexts in the crate
    ///
    /// # Returns
    /// A Python list of context strings.
    fn get_all_context(&mut self, py: Python) -> PyResult<Py<PyList>> {
        let test2 = self.inner.context.get_all_context();
        let py_list = PyList::new(py, test2.iter().map(|s| s.as_str()))?;
        Ok(py_list.into())
    }

    /// Returns a specific context entry for the given key.
    ///
    /// # Arguments
    /// * `context` - A Python string (or other PyObject) specifying the key.
    ///
    /// # Returns
    /// A Python string representing the requested context entry.
    fn get_specific_context(&mut self, py: Python, context: PyObject) -> PyResult<Py<PyString>> {
        let context_str: &str = context.extract(py)?;
        let specific_context = self
            .inner
            .context
            .get_specific_context(context_str)
            .unwrap();
        Ok(PyString::new(py, &specific_context).into())
    }

    /// Returns the crate's URN UUID.
    ///
    /// # Returns
    /// A Python string containing the UUIDv7 (URN).
    fn get_urn_uuid(&mut self, py: Python) -> PyResult<Py<PyString>> {
        let urn = self.inner.context.get_urn_uuid().unwrap();
        Ok(PyString::new(py, &urn).into())
    }

    /// Adds a URN UUID to crate
    ///
    /// # Returns
    /// A Python string containing the UUIDv7 (URN)
    fn add_urn_uuid(&mut self, py: Python) -> PyResult<Py<PyString>> {
        self.inner.context.add_urn_uuid();
        let urn = self.inner.context.get_urn_uuid().unwrap();
        Ok(PyString::new(py, &urn).into())
    }

    /// Retrieves an entity by its ID.
    ///
    /// # Arguments
    /// * `id` - A string representing the entity's identifier.
    ///
    /// # Returns
    /// A Python dictionary representing the entity, if found. Otherwise raises `ValueError`.
    fn get_entity(&mut self, py: Python, id: &str) -> PyResult<PyObject> {
        match self.inner.get_entity(id) {
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
    /// # Arguments
    /// * `py_obj` - A Python object representing a DataEntity (dictionary-like).
    ///
    /// # Returns
    /// `Ok(())` on success; an error otherwise.
    fn update_data(&mut self, py: Python, py_obj: PyObject) -> PyResult<()> {
        // Needs to check if data entity first - then parse as contextual if fail
        // if data then append to partOf vec in root.
        let data_entity_wrapper: utils::DataEntityWrapper = py_obj.extract(py)?;
        let data_entity = data_entity_wrapper.0; // Access the inner DataEntity
        let id = data_entity.id.clone();
        let update = GraphVector::DataEntity(data_entity.clone());

        if !self.inner.overwrite_by_id(&id, update) {
            let add = GraphVector::DataEntity(data_entity);
            self.inner.graph.push(add)
        };

        Ok(())
    }

    /// Update a contextual entity with new data
    ///
    /// Lazy update of contextual entity, finds id and overwrites the index.
    /// Strongly recommended to extract index data, modify, then rewrite the
    /// modified index data as the update.
    /// # Arguments
    /// * `py_obj` - A Python object representing a ContextualEntity (dictionary-like).
    ///
    /// # Returns
    /// `Ok(())` on success; an error otherwise.
    fn update_contextual(&mut self, py: Python, py_obj: PyObject) -> PyResult<()> {
        // Needs to check if data entity first - then parse as contextual if fail
        // if data then append to partOf vec in root.
        let contextual_entity_wrapper: utils::ContextualEntityWrapper = py_obj.extract(py)?;
        let contextual_entity = contextual_entity_wrapper.0; // Access the inner DataEntity
        let id = contextual_entity.id.clone();
        let update = GraphVector::ContextualEntity(contextual_entity.clone());

        if !self.inner.overwrite_by_id(&id, update) {
            let add = GraphVector::ContextualEntity(contextual_entity);
            self.inner.graph.push(add);
        };

        Ok(())
    }

    /// Update a root entity with new data
    ///
    /// Lazy update of root entity, finds id and overwrites the index.
    /// Strongly recommended to extract index data, modify, then rewrite the
    /// modified index data as the update.
    /// # Arguments
    /// * `py_obj` - A Python object representing a RootDataEntity (dictionary-like).
    ///
    /// # Returns
    /// `Ok(())` on success; an error otherwise.
    fn update_root(&mut self, py: Python, py_obj: PyObject) -> PyResult<()> {
        // Needs to check if data entity first - then parse as contextual if fail
        // if data then append to partOf vec in root.
        let root_entity_wrapper: utils::RootDataEntityWrapper = py_obj.extract(py)?;
        let root_entity = root_entity_wrapper.0; // Access the inner DataEntity
        let id = root_entity.id.clone();
        let update = GraphVector::RootDataEntity(root_entity.clone());

        if !self.inner.overwrite_by_id(&id, update) {
            let add = GraphVector::RootDataEntity(root_entity);
            self.inner.graph.push(add);
        };

        Ok(())
    }

    /// Update the metadata descriptor with new data
    ///
    /// Lazy update of metadata descriptor, finds id  and overwrites the index.
    /// Strongly recommended to extract index data, modify, then rewrite the
    /// modified index data as the update.
    /// # Arguments
    /// * `py_obj` - A Python object representing a MetadataDescriptor (dictionary-like).
    ///
    /// # Returns
    /// `Ok(())` on success; an error otherwise.
    fn update_descriptor(&mut self, py: Python, py_obj: PyObject) -> PyResult<()> {
        // Needs to check if data entity first - then parse as contextual if fail
        // if data then append to partOf vec in root.
        let descriptor_wrapper: utils::MetadataDescriptorWrapper = py_obj.extract(py)?;
        let descriptor = descriptor_wrapper.0; // Access the inner DataEntity
        let id = descriptor.id.clone();
        let update = GraphVector::MetadataDescriptor(descriptor.clone());

        if !self.inner.overwrite_by_id(&id, update) {
            let add = GraphVector::MetadataDescriptor(descriptor);
            self.inner.graph.push(add);
        }
        Ok(())
    }

    /// Overwrites an ID with new ID
    ///
    /// Overvwrites an ID with a New ID, and recursively changes every instance
    /// of the old ID within the RO-Crate.
    /// # Arguments
    /// * `id_old` - The old identifier string.
    /// * `id_new` - The new identifier string.
    ///
    /// # Returns
    /// `Ok(())` on success.
    fn replace_id(&mut self, id_old: &str, id_new: &str) -> PyResult<()> {
        self.inner.update_id_recursive(id_old, id_new);
        Ok(())
    }

    /// Entity deletion both recursive and not
    /// # Arguments
    /// * `id` - The entity identifier to remove.
    /// * `recursive` - A boolean indicating whether related references should also be removed.
    ///
    /// # Returns
    /// `Ok(())` on success.
    fn delete_entity(&mut self, id: &str, recursive: bool) -> PyResult<()> {
        self.inner.remove_by_id(id, recursive);
        Ok(())
    }

    /// Writes ro-crate back to ro-crate-metadata.json
    /// # Arguments
    /// * `file_path` - An optional string path to write the crate JSON.
    ///
    /// # Returns
    /// `Ok(())` on success; an error otherwise.
    fn write(&self, file_path: Option<String>) -> PyResult<()> {
        let path = file_path.unwrap_or_else(|| "ro-crate-metadata.json".to_string());
        rs_write_crate(&self.inner, path);
        Ok(())
    }

    /// Print's full crate
    /// # Returns
    /// A string showing the internal state of the `PyRoCrate`.
    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("PyRoCrate(data: '{:#?}')", self))
    }
}

impl From<RoCrate> for PyRoCrate {
    /// Allows simple conversion into rust_struct on read
    fn from(rust_struct: RoCrate) -> Self {
        PyRoCrate { inner: rust_struct }
    }
}

/// Reads a crate into memory allowing manipulation
/// # Arguments
/// * `relative_path` - Path (string) to an existing RO-Crate JSON or folder.
/// * `validation_level` - An integer specifying how strictly the crate is validated.
///                        0-2, 0 no validation, 1 warnings, 2 strict
///
/// # Errors
/// Raises an `IOError` if it cannot read or parse the crate.
///
/// # Returns
/// A `PyRoCrate` on success.
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
/// # Arguments
/// * `obj` - JSON string object of RO-Crate JSON.
/// * `validation_level` - An integer specifying how strictly the crate is validated.
///                        0-2, 0 no validation, 1 warnings, 2 strict
///
/// # Errors
/// Raises an `IOError` if it cannot read or parse the crate.
///
/// # Returns
/// A `PyRoCrate` on success.

#[pyfunction]
fn read_object(obj: &str, validation_level: i8) -> PyResult<PyRoCrate> {
    let rocrate = read_crate_obj(obj, validation_level)
        .map_err(|e| PyIOError::new_err(format!("Failed to read crate: {:#?}", e)))?;
    Ok(PyRoCrate::from(rocrate))
}
/// Reads a Zip of a crate into memory allowing manipulation
///
/// Useful for understanding large archives
/// # Arguments
/// * `path` - Path (string) to an existing RO-Crate ZIP.
/// * `validation_level` - An integer specifying how strictly the crate is validated.
///                        0-2, 0 no validation, 1 warnings, 2 strict
///
/// # Errors
/// Raises an `IOError` if it cannot read or parse the crate.
///
/// # Returns
/// A `PyRoCrate` on success.
#[pyfunction]
fn read_zip(path: &str, validation_level: i8) -> PyResult<PyRoCrate> {
    let rocrate = parse_zip(path, validation_level)
        .map_err(|e| PyIOError::new_err(format!("Failed to read crate: {:#?}", e)))?;
    Ok(PyRoCrate::from(rocrate))
}

/// Writes to parquet
/// # Arguments
/// * `rocrate` - The `PyRoCrate` to be written.
/// * `path` - The output path to the Parquet file.
///
/// # Returns
/// No explicit return; writes the file or raises an error.
#[pyfunction]
fn write_parquet(rocrate: &PyRoCrate, path: &str) {
    let mut df = to_df(&rocrate.inner);

    let target_path = PathBuf::from(path);
    wr_par(&mut df, target_path);
}

/// Enables objects URLs to be prefixed against ro-crate location to rewrite for
/// object storage
/// # Arguments
/// * `rocrate` - The `PyRoCrate` whose entity IDs should be updated.
/// * `object_root` - A string prefix (e.g., S3/GCS bucket URL) to prepend to entity paths.
///
/// # Returns
/// No explicit return; mutates the `PyRoCrate` in-place.
#[pyfunction]
fn prefix_object_id(rocrate: &mut PyRoCrate, object_root: &str) {
    relative_to_object_store(&mut rocrate.inner, object_root);
}

/// Targets a ro-crate and zips directory contents
/// Flatten removes directory structure in Zip
/// # Arguments
/// * `crate_path` - The path to the RO-Crate directory or JSON file.
/// * `external` - Whether to include external files (outside the directory) in the ZIP.
/// * `validation_level` - The validation strictness level (0-2, 0 no validation, 1 warning, 2
/// strict).
/// * `flatten` - If true, flattens the directory structure inside the ZIP.
/// * `unique` - If true, ensures unique file names within the ZIP.
///
/// # Returns
/// `Ok(())` on success; raises an error otherwise.
#[pyfunction]
fn zip(
    crate_path: &str,
    external: bool,
    validation_level: i8,
    flatten: bool,
    unique: bool,
) -> PyResult<()> {
    let path = Path::new(crate_path).to_path_buf();
    let _ = rs_zip_crate(&path, external, validation_level, flatten, unique);
    Ok(())
}

impl Default for PyRoCrate {
    /// Creates a new RoCrate with default requirements
    fn default() -> PyRoCrate {
        let mut rocrate = PyRoCrate {
            inner: RoCrate {
                context: RoCrateContext::ReferenceContext(
                    "https://w3id.org/ro/crate/1.2/context".to_string(),
                ),
                graph: Vec::new(),
            },
        };

        let description = MetadataDescriptor {
            id: "ro-crate-metadata.json".to_string(),
            type_: DataType::Term("CreativeWork".to_string()),
            conforms_to: Id::Id("https://w3id.org/ro/crate/1.2".to_string()),
            about: Id::Id("./".to_string()),
            dynamic_entity: None,
        };

        let time = Utc::now().to_rfc3339().to_string();

        let root_data_entity = RootDataEntity {
            id: "./".to_string(),
            type_: DataType::Term("Dataset".to_string()),
            name: format!("Default Crate: {time}"),
            description: "Default crate description".to_string(),
            date_published: Utc::now().to_rfc3339().to_string(),
            license: License::Id(Id::Id(
                "https://creativecommons.org/licenses/by-nc/4.0/deed.en".to_string(),
            )),
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
fn rocraters(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyRoCrate>()?;
    m.add_class::<PyRoCrateContext>()?;
    m.add_function(wrap_pyfunction!(read, m)?)?;
    m.add_function(wrap_pyfunction!(read_object, m)?)?;
    m.add_function(wrap_pyfunction!(read_zip, m)?)?;
    m.add_function(wrap_pyfunction!(zip, m)?)?;
    m.add_function(wrap_pyfunction!(write_parquet, m)?)?;
    m.add_function(wrap_pyfunction!(prefix_object_id, m)?)?;
    Ok(())
}
