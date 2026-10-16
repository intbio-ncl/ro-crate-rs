//! RDF bindings for ro-crate-rs core (requires the `rdf` feature)

use crate::PyRoCrate;
use ::rocraters::ro_crate::context::RoCrateContext;
use ::rocraters::ro_crate::rdf::{
    rdf_graph_to_rocrate, rdf_to_rocrate, ContextResolverBuilder, ConversionOptions, RdfFormat,
    RdfGraph, ResolvedContext,
};
use pyo3::exceptions::{PyIOError, PyValueError};
use pyo3::prelude::*;
use std::collections::HashMap;
use std::fs::File;

/// Parses an RDF format name or file extension
pub fn parse_format(format: &str) -> PyResult<RdfFormat> {
    match format.to_ascii_lowercase().as_str() {
        "turtle" | "ttl" => Ok(RdfFormat::Turtle),
        "ntriples" | "n-triples" | "nt" => Ok(RdfFormat::NTriples),
        "nquads" | "n-quads" | "nq" => Ok(RdfFormat::NQuads),
        "rdfxml" | "rdf/xml" | "rdf" | "xml" => Ok(RdfFormat::RdfXml),
        other => Err(PyValueError::new_err(format!(
            "Unsupported RDF format '{other}': expected turtle, ntriples, nquads or rdfxml"
        ))),
    }
}

/// Builds a context resolver from preloaded context documents and remote access
pub fn build_resolver(
    allow_remote: bool,
    contexts: Option<HashMap<String, String>>,
) -> PyResult<ContextResolverBuilder> {
    let mut resolver = ContextResolverBuilder::new();
    for (url, json) in contexts.unwrap_or_default() {
        resolver = resolver.with_context(&url, &json).map_err(rdf_error)?;
    }
    if allow_remote {
        resolver = resolver.allow_remote();
    }
    Ok(resolver)
}

/// Resolves a crate context, releasing the GIL while remote contexts are fetched
pub fn resolve_context(
    py: Python<'_>,
    context: &RoCrateContext,
    allow_remote: bool,
    contexts: Option<HashMap<String, String>>,
) -> PyResult<ResolvedContext> {
    let resolver = build_resolver(allow_remote, contexts)?;
    py.detach(|| resolver.resolve(context)).map_err(rdf_error)
}

/// Maps the python keyword arguments onto core conversion options
pub fn conversion_options(
    base: Option<String>,
    allow_relative: bool,
) -> PyResult<ConversionOptions> {
    match (base, allow_relative) {
        (Some(_), true) => Err(PyValueError::new_err(
            "`base` and `allow_relative` are mutually exclusive",
        )),
        (Some(base), false) => Ok(ConversionOptions::WithBase(base)),
        (None, true) => Ok(ConversionOptions::AllowRelative),
        (None, false) => Ok(ConversionOptions::Strict),
    }
}

pub fn rdf_error(error: impl std::fmt::Display) -> PyErr {
    PyValueError::new_err(error.to_string())
}

/// PyO3 compatible wrapper around RdfGraph struct
#[pyclass]
pub struct PyRdfGraph {
    pub inner: RdfGraph,
}

#[pymethods]
impl PyRdfGraph {
    /// Serialises the graph to a string.
    ///
    /// # Arguments
    /// * `format` - turtle, ntriples, nquads or rdfxml.
    #[pyo3(signature = (format = "turtle"))]
    fn serialize(&self, format: &str) -> PyResult<String> {
        self.inner
            .to_string(parse_format(format)?)
            .map_err(rdf_error)
    }

    /// Writes the serialised graph to a file.
    ///
    /// # Arguments
    /// * `path` - Output file path.
    /// * `format` - turtle, ntriples, nquads or rdfxml.
    #[pyo3(signature = (path, format = "turtle"))]
    fn write(&self, path: &str, format: &str) -> PyResult<()> {
        let format = parse_format(format)?;
        let file = File::create(path).map_err(|e| PyIOError::new_err(e.to_string()))?;
        self.inner.write(file, format).map_err(rdf_error)
    }

    /// Converts the graph back into an RO-Crate using the context it was
    /// created with.
    fn to_rocrate(&self) -> PyResult<PyRoCrate> {
        let rocrate = rdf_graph_to_rocrate(self.inner.clone()).map_err(rdf_error)?;
        Ok(PyRoCrate::from(rocrate))
    }

    /// Number of triples in the graph
    fn __len__(&self) -> usize {
        self.inner.len()
    }

    fn __repr__(&self) -> String {
        format!("PyRdfGraph(triples: {})", self.inner.len())
    }
}

/// Parses RDF into an RO-Crate
///
/// # Arguments
/// * `data` - RDF document as a string.
/// * `format` - turtle, ntriples, nquads or rdfxml.
/// * `base` - Optional base IRI for resolving relative IRIs.
///
/// # Errors
/// Raises a `ValueError` if the RDF cannot be parsed or has no root entities.
#[pyfunction]
#[pyo3(signature = (data, format = "turtle", base = None))]
pub fn from_rdf(data: &str, format: &str, base: Option<&str>) -> PyResult<PyRoCrate> {
    let rocrate = rdf_to_rocrate(data, parse_format(format)?, base).map_err(rdf_error)?;
    Ok(PyRoCrate::from(rocrate))
}
