//! Python bindings for ro-crate-rs core

#[cfg(feature = "rdf")]
mod rdf;
mod utils;
extern crate chrono;
use ::rocraters::ro_crate::constraints::*;
use ::rocraters::ro_crate::context::{ContextItem, RoCrateContext};
#[cfg(feature = "parquet")]
use ::rocraters::ro_crate::convert::{to_df, write_csv as wr_csv, write_parquet as wr_par};
use ::rocraters::ro_crate::graph_vector::GraphVector;
use ::rocraters::ro_crate::metadata_descriptor::MetadataDescriptor;
use ::rocraters::ro_crate::object_storage::relative_to_object_store;
use ::rocraters::ro_crate::read::{
    load_remote, parse_zip, validate_crate_keys, CrateReadError, ValidationResult,
};
use ::rocraters::ro_crate::root::RootDataEntity;
use ::rocraters::ro_crate::schema::{load_rocrate_schema, RoCrateSchemaVersion};
#[cfg(feature = "subcrate-resolution")]
use ::rocraters::ro_crate::subcrate_resolution::{
    fetch_subcrates_recursive_with_options, fetch_subcrates_with_options,
    try_fetch_subcrates_recursive_with_options, try_fetch_subcrates_with_options, FetchOptions,
};
use ::rocraters::ro_crate::{
    read::{read_crate, read_crate_obj},
    rocrate::RoCrate,
    write::{write_crate as rs_write_crate, zip_crate as rs_zip_crate},
};
use chrono::prelude::*;
use pyo3::exceptions::{PyIOError, PyValueError};
use pyo3::{
    prelude::*,
    types::{PyDict, PyList},
    Py, PyAny,
};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

/// Maps a schema version onto its "major.minor" string
fn schema_version_to_str(version: RoCrateSchemaVersion) -> Option<&'static str> {
    match version {
        RoCrateSchemaVersion::V1_1 => Some("1.1"),
        RoCrateSchemaVersion::V1_2 => Some("1.2"),
        RoCrateSchemaVersion::V1_3 => Some("1.3"),
        _ => None,
    }
}

fn schema_version_from_str(version: &str) -> PyResult<RoCrateSchemaVersion> {
    match version {
        "1.1" => Ok(RoCrateSchemaVersion::V1_1),
        "1.2" => Ok(RoCrateSchemaVersion::V1_2),
        "1.3" => Ok(RoCrateSchemaVersion::V1_3),
        other => Err(PyValueError::new_err(format!(
            "Unsupported RO-Crate version '{other}': expected 1.1, 1.2 or 1.3"
        ))),
    }
}

/// Converts a string (reference) or dict (embedded) into a context item
fn context_item_from_pyobject(obj: &Bound<'_, PyAny>) -> PyResult<ContextItem> {
    if let Ok(reference) = obj.extract::<String>() {
        return Ok(ContextItem::ReferenceItem(reference));
    }
    if let Ok(embedded) = obj.extract::<HashMap<String, String>>() {
        return Ok(ContextItem::EmbeddedContext(embedded));
    }
    Err(PyValueError::new_err(
        "Context items must be either strings or dictionaries of strings",
    ))
}

fn entity_not_found(id: &str) -> PyErr {
    PyValueError::new_err(format!("Entity '{id}' not found"))
}

fn read_error(error: CrateReadError) -> PyErr {
    PyIOError::new_err(format!("Failed to read crate: {:#?}", error))
}

fn parse_url(url: &str) -> PyResult<url::Url> {
    url::Url::parse(url).map_err(|e| PyValueError::new_err(format!("Invalid URL '{url}': {e}")))
}

fn build_validation_report(
    py: Python<'_>,
    is_valid: bool,
    invalid_keys: Vec<String>,
    invalid_ids: Vec<String>,
    invalid_types: Vec<String>,
    error_type: Option<&str>,
    error_message: Option<String>,
) -> PyResult<Py<PyDict>> {
    let report = PyDict::new(py);
    report.set_item("is_valid", is_valid)?;
    report.set_item("invalid_keys", invalid_keys)?;
    report.set_item("invalid_ids", invalid_ids)?;
    report.set_item("invalid_types", invalid_types)?;
    report.set_item("error_type", error_type)?;
    report.set_item("error_message", error_message)?;
    Ok(report.unbind())
}

fn validation_result_to_report(py: Python<'_>, result: ValidationResult) -> PyResult<Py<PyDict>> {
    match result {
        ValidationResult::Valid => {
            build_validation_report(py, true, Vec::new(), Vec::new(), Vec::new(), None, None)
        }
        ValidationResult::Invalid(validation) => build_validation_report(
            py,
            false,
            validation.invalid_keys,
            validation.invalid_ids,
            validation.invalid_types,
            None,
            None,
        ),
        ValidationResult::Error(error_message) => build_validation_report(
            py,
            false,
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Some("SchemaError"),
            Some(error_message),
        ),
    }
}

fn crate_read_error_to_report(py: Python<'_>, error: CrateReadError) -> PyResult<Py<PyDict>> {
    match error {
        CrateReadError::IoError(err) => build_validation_report(
            py,
            false,
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Some("IoError"),
            Some(err.to_string()),
        ),
        CrateReadError::JsonError(err) => build_validation_report(
            py,
            false,
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Some("JsonError"),
            Some(err.to_string()),
        ),
        CrateReadError::VocabNotValid(err) => build_validation_report(
            py,
            false,
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Some("VocabNotValid"),
            Some(err),
        ),
        CrateReadError::SchemaError(err) => build_validation_report(
            py,
            false,
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Some("SchemaError"),
            Some(err),
        ),
        CrateReadError::ReqwestError(err) => build_validation_report(
            py,
            false,
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Some("ReqwestError"),
            Some(err.to_string()),
        ),
        CrateReadError::ZipError(err) => build_validation_report(
            py,
            false,
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Some("ZipError"),
            Some(err.to_string()),
        ),
    }
}

/// PyO3 compatible wrapper around RoCrate struct
#[pyclass]
#[derive(Debug)]
struct PyRoCrate {
    inner: RoCrate,
}

/// PyO3 compatible wrapper around RoCrateContext struct
#[pyclass(skip_from_py_object)]
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
        let context_items = context
            .iter()
            .map(|obj| context_item_from_pyobject(&obj))
            .collect::<PyResult<Vec<_>>>()?;
        Ok(PyRoCrateContext {
            inner: RoCrateContext::ExtendedContext(context_items),
        })
    }

    /// Creates a fully embedded context
    ///
    /// # Arguments
    /// * `context` - A list of dictionaries mapping terms to IRIs.
    ///
    /// # Returns
    /// A `PyRoCrateContext` containing an embedded context.
    #[staticmethod]
    fn from_embedded(context: Vec<HashMap<String, String>>) -> Self {
        PyRoCrateContext {
            inner: RoCrateContext::EmbeddedContext(context),
        }
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

    /// Returns all contexts in the crate.
    ///
    /// # Returns
    /// A Python list of dictionaries using the `@context` key.
    fn get_all_context(&mut self, py: Python) -> PyResult<Py<PyList>> {
        let py_list = PyList::empty(py);

        match &self.inner.context {
            RoCrateContext::ReferenceContext(reference) => {
                let context_dict = PyDict::new(py);
                context_dict.set_item("@context", reference)?;
                py_list.append(context_dict)?;
            }
            RoCrateContext::ExtendedContext(context_items) => {
                for item in context_items {
                    let context_dict = PyDict::new(py);
                    match item {
                        ContextItem::ReferenceItem(reference) => {
                            context_dict.set_item("@context", reference)?;
                        }
                        ContextItem::EmbeddedContext(embedded) => {
                            let embedded_dict = PyDict::new(py);
                            for (key, value) in embedded {
                                embedded_dict.set_item(key, value)?;
                            }
                            context_dict.set_item("@context", embedded_dict)?;
                        }
                    }
                    py_list.append(context_dict)?;
                }
            }
            RoCrateContext::EmbeddedContext(context_maps) => {
                for embedded in context_maps {
                    let context_dict = PyDict::new(py);
                    let embedded_dict = PyDict::new(py);
                    for (key, value) in embedded {
                        embedded_dict.set_item(key, value)?;
                    }
                    context_dict.set_item("@context", embedded_dict)?;
                    py_list.append(context_dict)?;
                }
            }
        }

        Ok(py_list.into())
    }

    /// Returns a specific context entry for the given key.
    ///
    /// # Arguments
    /// * `context` - The embedded context key to look up (e.g. "@base").
    ///
    /// # Returns
    /// The value of the context entry, or `None` if it is not defined.
    fn get_specific_context(&self, context: &str) -> Option<String> {
        self.inner.context.get_specific_context(context)
    }

    /// Returns the keys of embedded contexts, or the reference for a
    /// reference context.
    fn get_context_items(&self) -> Vec<String> {
        self.inner.get_context_items()
    }

    /// Adds a context item to the crate.
    ///
    /// # Arguments
    /// * `item` - A context URL string or a dictionary of term mappings.
    fn add_context(&mut self, item: &Bound<'_, PyAny>) -> PyResult<()> {
        self.inner.add_context(context_item_from_pyobject(item)?);
        Ok(())
    }

    /// Removes a context item from the crate.
    ///
    /// # Arguments
    /// * `item` - A context URL string or a dictionary of term mappings.
    ///
    /// # Errors
    /// Raises a `ValueError` if the item is not present or cannot be removed.
    fn remove_context(&mut self, item: &Bound<'_, PyAny>) -> PyResult<()> {
        self.inner
            .context
            .remove_context(&context_item_from_pyobject(item)?)
            .map_err(PyValueError::new_err)
    }

    /// Replaces a context item with another.
    ///
    /// # Arguments
    /// * `new` - The context item to add.
    /// * `old` - The context item to remove.
    ///
    /// # Errors
    /// Raises a `ValueError` if `old` is not present.
    fn replace_context(&mut self, new: &Bound<'_, PyAny>, old: &Bound<'_, PyAny>) -> PyResult<()> {
        self.inner
            .context
            .replace_context(
                &context_item_from_pyobject(new)?,
                &context_item_from_pyobject(old)?,
            )
            .map_err(|e| PyValueError::new_err(e.to_string()))
    }

    /// Returns the RO-Crate specification version declared by the context.
    ///
    /// # Returns
    /// "1.1", "1.2", "1.3" or `None` if it cannot be determined.
    fn get_rocrate_version(&self) -> Option<&'static str> {
        self.inner
            .get_rocrate_version()
            .and_then(schema_version_to_str)
    }

    /// Returns the crate's URN UUID.
    ///
    /// # Returns
    /// The UUID from a `urn:uuid:` `@base`, or `None` if there is none.
    fn get_urn_uuid(&self) -> Option<String> {
        self.inner.context.get_urn_uuid()
    }

    /// Adds a URN UUID to crate
    ///
    /// # Returns
    /// A Python string containing the UUIDv7 (URN)
    ///
    /// # Errors
    /// Raises a `ValueError` if the context cannot hold an `@base`.
    fn add_urn_uuid(&mut self) -> PyResult<String> {
        self.inner
            .context
            .add_urn_uuid()
            .ok_or_else(|| PyValueError::new_err("Context does not support a urn:uuid @base"))
    }

    /// Retrieves an entity by its ID.
    ///
    /// # Arguments
    /// * `id` - A string representing the entity's identifier.
    ///
    /// # Returns
    /// A Python dictionary representing the entity, if found. Otherwise raises `ValueError`.
    fn get_entity(&self, py: Python, id: &str) -> PyResult<Py<PyAny>> {
        match self.inner.get_entity(id) {
            Some(entity) => utils::graph_vector_to_pyobject(py, entity),
            None => Err(PyValueError::new_err(
                "ID not found or unsupported GraphVector variant",
            )),
        }
    }
    /// Returns all entities in the graph as a Python list of dicts.
    fn to_list(&self, py: Python) -> PyResult<Py<PyList>> {
        let out = self
            .inner
            .graph
            .iter()
            .map(|entity| utils::graph_vector_to_pyobject(py, entity))
            .collect::<PyResult<Vec<_>>>()?;

        Ok(PyList::new(py, out)?.into())
    }

    /// Returns the IDs of every entity in the graph.
    fn get_all_ids(&self) -> Vec<String> {
        self.inner.get_all_ids().into_iter().cloned().collect()
    }

    /// Returns every property key used across the crate (deduplicated).
    fn get_all_properties(&self) -> Vec<String> {
        self.inner.get_all_properties()
    }

    /// Returns the value of a property for every entity that has it.
    ///
    /// # Arguments
    /// * `property` - The property key to search for (e.g. "name").
    ///
    /// # Returns
    /// A list of `(entity_id, value)` tuples.
    fn get_all_property_values(&self, py: Python, property: &str) -> Vec<(String, Py<PyAny>)> {
        self.inner
            .get_all_property_values(property)
            .into_iter()
            .map(|(id, value)| (id, utils::convert_dynamic_entity_to_pyobject(py, &value)))
            .collect()
    }

    /// Finds entities whose (non-required) properties hold a specific value.
    ///
    /// # Arguments
    /// * `value` - The value to search for.
    ///
    /// # Returns
    /// A list of `(entity_id, property)` tuples.
    fn get_specific_value(&self, value: &str) -> Vec<(String, String)> {
        self.inner.get_specific_value(value)
    }

    /// Returns every ID an entity links to.
    ///
    /// # Arguments
    /// * `id` - The entity to inspect.
    ///
    /// # Errors
    /// Raises a `ValueError` if the entity does not exist.
    fn get_linked_ids(&self, id: &str) -> PyResult<Vec<String>> {
        let entity = self
            .inner
            .get_entity(id)
            .ok_or_else(|| entity_not_found(id))?;
        Ok(utils::ids_to_strings(entity.get_linked_ids()))
    }

    /// Returns the data entities that reference other RO-Crates (subcrates).
    fn get_subcrates(&self, py: Python) -> PyResult<Py<PyList>> {
        let out = self
            .inner
            .get_subcrates()
            .into_iter()
            .map(|entity| utils::graph_vector_to_pyobject(py, entity))
            .collect::<PyResult<Vec<_>>>()?;

        Ok(PyList::new(py, out)?.into())
    }

    /// Retrieves the subcrates referenced by this crate.
    ///
    /// Local subcrate paths are resolved relative to the current working
    /// directory; remote subcrates are fetched over HTTP only when allow_remote is true.
    ///
    /// # Arguments
    /// * `recursive` - Also fetch the subcrates of each subcrate.
    /// * `strict` - Raise if a subcrate cannot be resolved, rather than skipping it.
    ///
    /// # Errors
    /// Raises an `IOError` when `strict` and a subcrate cannot be resolved.
    #[cfg(feature = "subcrate-resolution")]
    #[pyo3(signature = (recursive = false, strict = true, allow_remote = false, local_root = None))]
    fn fetch_subcrates(
        &self,
        py: Python<'_>,
        recursive: bool,
        strict: bool,
        allow_remote: bool,
        local_root: Option<String>,
    ) -> PyResult<Vec<PyRoCrate>> {
        let rocrate = &self.inner;
        let options = FetchOptions {
            allow_remote,
            local_root: local_root.map(PathBuf::from),
        };
        let subcrates = py
            .detach(|| match (recursive, strict) {
                (false, true) => fetch_subcrates_with_options(rocrate, options),
                (true, true) => fetch_subcrates_recursive_with_options(rocrate, options),
                (false, false) => Ok(try_fetch_subcrates_with_options(rocrate, options)),
                (true, false) => Ok(try_fetch_subcrates_recursive_with_options(rocrate, options)),
            })
            .map_err(|e| PyIOError::new_err(format!("Failed to fetch subcrates: {e}")))?;

        Ok(subcrates.into_iter().map(PyRoCrate::from).collect())
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
    fn update_data(&mut self, py: Python, py_obj: Py<PyAny>) -> PyResult<()> {
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
    fn update_contextual(&mut self, py: Python, py_obj: Py<PyAny>) -> PyResult<()> {
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
    fn update_root(&mut self, py: Python, py_obj: Py<PyAny>) -> PyResult<()> {
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
    fn update_descriptor(&mut self, py: Python, py_obj: Py<PyAny>) -> PyResult<()> {
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

    /// Adds a type to an entity's `@type`.
    ///
    /// # Errors
    /// Raises a `ValueError` if the entity does not exist.
    fn add_type(&mut self, id: &str, data_type: String) -> PyResult<()> {
        let entity = self
            .inner
            .get_entity_mutable(id)
            .ok_or_else(|| entity_not_found(id))?;
        entity.add_type(data_type);
        Ok(())
    }

    /// Removes a type from an entity's `@type`.
    ///
    /// # Errors
    /// Raises a `ValueError` if the entity does not exist, the type is not
    /// present, or it is the entity's only type.
    fn remove_type(&mut self, id: &str, data_type: String) -> PyResult<()> {
        let entity = self
            .inner
            .get_entity_mutable(id)
            .ok_or_else(|| entity_not_found(id))?;
        entity
            .remove_type(data_type)
            .map(|_| ())
            .map_err(PyValueError::new_err)
    }

    /// Adds or overwrites properties on an entity.
    ///
    /// # Arguments
    /// * `id` - The entity to update.
    /// * `properties` - A dictionary of property keys to values. Values use
    ///   the same conventions as `update_data` (e.g. `{"id": "#ref"}` for a link).
    ///
    /// # Returns
    /// `True` if the entity was found and updated, `False` otherwise.
    fn add_dynamic_entity_property(
        &mut self,
        id: &str,
        properties: &Bound<'_, PyDict>,
    ) -> PyResult<bool> {
        if self.inner.get_entity(id).is_none() {
            return Ok(false);
        }
        let mut values = HashMap::new();
        for (key, value) in properties {
            values.insert(
                key.extract::<String>()?,
                utils::convert_pyobject_to_dynamic_entity(&value)?,
            );
        }
        Ok(self.inner.add_dynamic_entity_property(id, values))
    }

    /// Removes a property from an entity.
    ///
    /// # Returns
    /// `True` if the entity was found, `False` otherwise.
    fn remove_dynamic_entity_property(&mut self, id: &str, property: &str) -> bool {
        self.inner.remove_dynamic_entity_property(id, property)
    }

    /// Adds an entity ID to the root data entity's `hasPart`.
    fn add_data_to_partof_root(&mut self, id: &str) {
        self.inner.add_data_to_partof_root(id);
    }

    /// Validates the crate's keys and local IDs against its RO-Crate version.
    ///
    /// Returns the same report dictionary shape as `validate`.
    fn validate(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        validation_result_to_report(py, validate_crate_keys(&self.inner))
    }

    /// Serialises the crate to an RO-Crate JSON-LD string.
    ///
    /// # Arguments
    /// * `pretty` - Indent the output.
    #[pyo3(signature = (pretty = true))]
    fn to_json(&self, pretty: bool) -> PyResult<String> {
        let json = if pretty {
            serde_json::to_string_pretty(&self.inner)
        } else {
            serde_json::to_string(&self.inner)
        };
        json.map_err(|e| PyValueError::new_err(e.to_string()))
    }

    /// Writes ro-crate back to ro-crate-metadata.json
    /// # Arguments
    /// * `file_path` - An optional string path to write the crate JSON.
    ///
    /// # Errors
    /// Raises an `IOError` if the file cannot be written.
    #[pyo3(signature = (file_path = None))]
    fn write(&self, file_path: Option<String>) -> PyResult<()> {
        let path = file_path.unwrap_or_else(|| "ro-crate-metadata.json".to_string());
        rs_write_crate(&self.inner, path)
            .map_err(|e| PyIOError::new_err(format!("Failed to write crate: {e}")))
    }

    /// Converts the crate to RDF and serialises it.
    ///
    /// # Arguments
    /// * `format` - turtle, ntriples, nquads or rdfxml.
    /// * `base` - Base IRI for relative IDs when the context has no `@base`.
    /// * `allow_relative` - Leave unresolvable relative IRIs as they are.
    /// * `allow_remote` - Fetch unknown context documents over HTTP.
    /// * `contexts` - Context documents to preload, as `{url: json_string}`.
    ///
    /// # Errors
    /// Raises a `ValueError` if the crate cannot be converted.
    #[cfg(feature = "rdf")]
    #[pyo3(signature = (format = "turtle", base = None, allow_relative = false, allow_remote = false, contexts = None))]
    fn to_rdf(
        &self,
        py: Python<'_>,
        format: &str,
        base: Option<String>,
        allow_relative: bool,
        allow_remote: bool,
        contexts: Option<HashMap<String, String>>,
    ) -> PyResult<String> {
        let format = rdf::parse_format(format)?;
        self.to_rdf_graph(py, base, allow_relative, allow_remote, contexts)?
            .inner
            .to_string(format)
            .map_err(rdf::rdf_error)
    }

    /// Converts the crate to an RDF graph.
    ///
    /// Takes the same options as `to_rdf`. The graph keeps the crate's
    /// context, so `graph.to_rocrate()` round-trips custom terms.
    #[cfg(feature = "rdf")]
    #[pyo3(signature = (base = None, allow_relative = false, allow_remote = false, contexts = None))]
    fn to_rdf_graph(
        &self,
        py: Python<'_>,
        base: Option<String>,
        allow_relative: bool,
        allow_remote: bool,
        contexts: Option<HashMap<String, String>>,
    ) -> PyResult<rdf::PyRdfGraph> {
        let resolver = rdf::build_resolver(allow_remote, contexts)?;
        let options = rdf::conversion_options(base, allow_relative)?;
        let rocrate = &self.inner;
        let graph = py
            .detach(|| {
                ::rocraters::ro_crate::rdf::rocrate_to_rdf_with_options(rocrate, resolver, options)
            })
            .map_err(rdf::rdf_error)?;
        Ok(rdf::PyRdfGraph { inner: graph })
    }

    /// Expands a term or relative ID to a full IRI using the crate's context.
    ///
    /// # Arguments
    /// * `term` - The term to expand (e.g. "name" or "schema:Person").
    /// * `strict` - Raise instead of returning a term that cannot be made absolute.
    /// * `allow_remote` - Fetch unknown context documents over HTTP.
    /// * `contexts` - Context documents to preload, as `{url: json_string}`.
    #[cfg(feature = "rdf")]
    #[pyo3(signature = (term, strict = false, allow_remote = false, contexts = None))]
    fn expand_term(
        &self,
        py: Python<'_>,
        term: &str,
        strict: bool,
        allow_remote: bool,
        contexts: Option<HashMap<String, String>>,
    ) -> PyResult<String> {
        let resolved = rdf::resolve_context(py, &self.inner.context, allow_remote, contexts)?;
        if strict {
            resolved.expand_term_checked(term, false)
        } else {
            resolved.expand_term(term)
        }
        .map_err(rdf::rdf_error)
    }

    /// Compacts a full IRI to the shortest term in the crate's context.
    ///
    /// Takes the same `allow_remote` and `contexts` options as `expand_term`.
    #[cfg(feature = "rdf")]
    #[pyo3(signature = (iri, allow_remote = false, contexts = None))]
    fn compact_iri(
        &self,
        py: Python<'_>,
        iri: &str,
        allow_remote: bool,
        contexts: Option<HashMap<String, String>>,
    ) -> PyResult<String> {
        let resolved = rdf::resolve_context(py, &self.inner.context, allow_remote, contexts)?;
        Ok(resolved.compact_iri(iri))
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
    let rocrate = read_crate(&path, validation_level).map_err(read_error)?;
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
    let rocrate = read_crate_obj(obj, validation_level).map_err(read_error)?;
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
    let rocrate = parse_zip(path, validation_level).map_err(read_error)?;
    Ok(PyRoCrate::from(rocrate))
}

/// Reads a crate from a URL into memory allowing manipulation
///
/// # Arguments
/// * `url` - URL that returns an RO-Crate JSON-LD document.
/// * `validation_level` - An integer specifying how strictly the crate is validated.
///                        0-2, 0 no validation, 1 warnings, 2 strict
///
/// # Errors
/// Raises a `ValueError` for an invalid URL, or an `IOError` if it cannot
/// fetch or parse the crate.
#[pyfunction]
fn read_remote(py: Python<'_>, url: &str, validation_level: i8) -> PyResult<PyRoCrate> {
    let url = parse_url(url)?;
    let rocrate = py
        .detach(|| load_remote(url, validation_level))
        .map_err(read_error)?;
    Ok(PyRoCrate::from(rocrate))
}

/// Validates an RO-Crate and returns a structured report dictionary.
///
/// The report shape is:
/// {
///   "is_valid": bool,
///   "invalid_keys": list[str],
///   "invalid_ids": list[str],
///   "invalid_types": list[str],
///   "error_type": str | None,
///   "error_message": str | None,
/// }
#[pyfunction]
fn validate(py: Python<'_>, relative_path: &str) -> PyResult<Py<PyDict>> {
    let path = Path::new(relative_path).to_path_buf();
    match read_crate(&path, 0) {
        Ok(rocrate) => validation_result_to_report(py, validate_crate_keys(&rocrate)),
        Err(err) => crate_read_error_to_report(py, err),
    }
}

/// Validates a JSON object string containing an RO-Crate and returns a report dictionary.
#[pyfunction]
fn validate_object(py: Python<'_>, obj: &str) -> PyResult<Py<PyDict>> {
    match read_crate_obj(obj, 0) {
        Ok(rocrate) => validation_result_to_report(py, validate_crate_keys(&rocrate)),
        Err(err) => crate_read_error_to_report(py, err),
    }
}

/// Validates a ZIP-contained RO-Crate and returns a structured report dictionary.
#[pyfunction]
fn validate_zip(py: Python<'_>, path: &str) -> PyResult<Py<PyDict>> {
    match parse_zip(path, 0) {
        Ok(rocrate) => validation_result_to_report(py, validate_crate_keys(&rocrate)),
        Err(err) => crate_read_error_to_report(py, err),
    }
}

/// Validates a crate fetched from a URL and returns a structured report dictionary.
///
/// # Errors
/// Raises a `ValueError` for an invalid URL; fetch failures are reported in the dictionary.
#[pyfunction]
fn validate_remote(py: Python<'_>, url: &str) -> PyResult<Py<PyDict>> {
    let url = parse_url(url)?;
    match py.detach(|| load_remote(url, 0)) {
        Ok(rocrate) => validation_result_to_report(py, validate_crate_keys(&rocrate)),
        Err(err) => crate_read_error_to_report(py, err),
    }
}

/// Loads the bundled RO-Crate JSON-LD context for a specification version.
///
/// # Arguments
/// * `version` - "1.1", "1.2" or "1.3".
///
/// # Returns
/// The context document as a dictionary; term mappings are under "@context".
#[pyfunction]
#[pyo3(signature = (version = "1.3"))]
fn load_schema(py: Python<'_>, version: &str) -> PyResult<Py<PyAny>> {
    let schema = load_rocrate_schema(schema_version_from_str(version)?)
        .map_err(|e| PyValueError::new_err(e.to_string()))?;
    let value = serde_json::to_value(&schema).map_err(|e| PyValueError::new_err(e.to_string()))?;
    Ok(utils::convert_serde_json_value_to_pyobject(py, &value))
}

/// The tabular conversion keys every row by the crate's urn:uuid
#[cfg(feature = "parquet")]
fn require_urn_uuid(rocrate: &PyRoCrate) -> PyResult<()> {
    match rocrate.inner.context.get_urn_uuid() {
        Some(_) => Ok(()),
        None => Err(PyValueError::new_err(
            "Crate has no urn:uuid @base in its context: call add_urn_uuid() first",
        )),
    }
}

/// Writes to parquet
/// # Arguments
/// * `rocrate` - The `PyRoCrate` to be written. Must have a urn:uuid `@base`.
/// * `path` - The output path to the Parquet file.
///
/// # Errors
/// Raises a `ValueError` if the crate has no urn:uuid, or an `IOError` if the
/// file cannot be written.
#[pyfunction]
#[cfg(feature = "parquet")]
fn write_parquet(rocrate: &PyRoCrate, path: &str) -> PyResult<()> {
    require_urn_uuid(rocrate)?;
    let mut df = to_df(&rocrate.inner);
    wr_par(&mut df, PathBuf::from(path)).map_err(|e| PyIOError::new_err(e.to_string()))
}

/// Writes the crate as a uuid/id/key/value CSV table
/// # Arguments
/// * `rocrate` - The `PyRoCrate` to be written. Must have a urn:uuid `@base`.
/// * `path` - The output path to the CSV file.
///
/// # Errors
/// Raises a `ValueError` if the crate has no urn:uuid, or an `IOError` if the
/// file cannot be written.
#[pyfunction]
#[cfg(feature = "parquet")]
fn write_csv(rocrate: &PyRoCrate, path: &str) -> PyResult<()> {
    require_urn_uuid(rocrate)?;
    let mut df = to_df(&rocrate.inner);
    wr_csv(&mut df, PathBuf::from(path)).map_err(|e| PyIOError::new_err(e.to_string()))
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
/// * `include_hidden` - Hidden files or folders to package anyway, relative to the crate
///   directory.
///
/// # Errors
/// Raises an `IOError` if the crate cannot be read or the archive written.
#[pyfunction]
#[pyo3(signature = (crate_path, external, validation_level, flatten, unique, include_hidden = None))]
fn zip(
    crate_path: &str,
    external: bool,
    validation_level: i8,
    flatten: bool,
    unique: bool,
    include_hidden: Option<Vec<PathBuf>>,
) -> PyResult<()> {
    let path = Path::new(crate_path).to_path_buf();
    rs_zip_crate(
        &path,
        external,
        validation_level,
        flatten,
        unique,
        &include_hidden.unwrap_or_default(),
    )
    .map_err(|e| PyIOError::new_err(format!("Failed to zip crate: {e}")))
}

impl Default for PyRoCrate {
    /// Creates a new RoCrate with default requirements
    fn default() -> PyRoCrate {
        let mut rocrate = PyRoCrate {
            inner: RoCrate {
                context: RoCrateContext::ReferenceContext(
                    "https://w3id.org/ro/crate/1.3/context".to_string(),
                ),
                graph: Vec::new(),
            },
        };

        let description = MetadataDescriptor {
            id: "ro-crate-metadata.json".to_string(),
            type_: DataType::Term("CreativeWork".to_string()),
            conforms_to: Id::Id("https://w3id.org/ro/crate/1.3".to_string()),
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
    m.add_function(wrap_pyfunction!(read_remote, m)?)?;
    m.add_function(wrap_pyfunction!(validate, m)?)?;
    m.add_function(wrap_pyfunction!(validate_object, m)?)?;
    m.add_function(wrap_pyfunction!(validate_zip, m)?)?;
    m.add_function(wrap_pyfunction!(validate_remote, m)?)?;
    m.add_function(wrap_pyfunction!(load_schema, m)?)?;
    m.add_function(wrap_pyfunction!(zip, m)?)?;
    #[cfg(feature = "parquet")]
    m.add_function(wrap_pyfunction!(write_parquet, m)?)?;
    #[cfg(feature = "parquet")]
    m.add_function(wrap_pyfunction!(write_csv, m)?)?;
    m.add_function(wrap_pyfunction!(prefix_object_id, m)?)?;
    #[cfg(feature = "rdf")]
    {
        m.add_class::<rdf::PyRdfGraph>()?;
        m.add_function(wrap_pyfunction!(rdf::from_rdf, m)?)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_uses_latest_spec() {
        let rocrate = PyRoCrate::default();

        assert_eq!(
            rocrate.inner.get_rocrate_version(),
            Some(::rocraters::ro_crate::schema::RoCrateSchemaVersion::V1_3)
        );
        let metadata = rocrate.inner.graph.iter().find_map(|entity| match entity {
            GraphVector::MetadataDescriptor(metadata) => Some(metadata),
            _ => None,
        });
        assert_eq!(
            metadata.unwrap().conforms_to,
            Id::Id("https://w3id.org/ro/crate/1.3".to_string())
        );
    }
}
