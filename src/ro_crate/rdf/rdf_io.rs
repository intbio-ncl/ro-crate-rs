//! RDF serialization and parsing support for RdfGraph.
//!
//! This module provides functionality to serialize RdfGraph instances to various RDF formats
//! and parse RDF data into RO-Crate structures using the oxrdfio library.

use std::collections::{HashMap, HashSet};
use std::io::Write;

use oxrdf::{NamedNode, NamedOrBlankNode, Quad, Term, Triple};
use oxrdfio::{RdfFormat as OxRdfFormat, RdfParser, RdfSerializer};

use super::context::ResolvedContext;
use super::error::RdfError;
use super::graph::RdfGraph;
use super::resolver::ContextResolverBuilder;
use crate::ro_crate::constraints::{DataType, EntityValue, Id, License};
use crate::ro_crate::context::RoCrateContext;
use crate::ro_crate::contextual_entity::ContextualEntity;
use crate::ro_crate::data_entity::DataEntity;
use crate::ro_crate::graph_vector::GraphVector;
use crate::ro_crate::metadata_descriptor::MetadataDescriptor;
use crate::ro_crate::rocrate::RoCrate;
use crate::ro_crate::root::RootDataEntity;

/// Supported RDF serialization formats.
///
/// This enum wraps oxrdfio's `RdfFormat` type rather than re-exporting it directly for:
///
/// 1. **API stability**: If oxrdfio changes their format enum, our public API remains stable
/// 2. **Subset selection**: Only exposes formats we actively support and test for RO-Crate use
/// 3. **Custom documentation**: Allows RO-Crate specific documentation and examples
///
/// Use this type to specify the desired format when serializing or parsing RDF data.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RdfFormat {
    /// Turtle format (.ttl) - compact and human-readable
    Turtle,
    /// N-Triples format (.nt) - simple line-based format
    NTriples,
    /// N-Quads format (.nq) - N-Triples with named graphs
    NQuads,
    /// RDF/XML format (.rdf, .xml) - XML serialization
    RdfXml,
}

impl RdfFormat {
    /// Converts this RdfFormat to the corresponding oxrdfio format.
    fn to_oxrdf_format(self) -> OxRdfFormat {
        match self {
            RdfFormat::Turtle => OxRdfFormat::Turtle,
            RdfFormat::NTriples => OxRdfFormat::NTriples,
            RdfFormat::NQuads => OxRdfFormat::NQuads,
            RdfFormat::RdfXml => OxRdfFormat::RdfXml,
        }
    }
}

impl RdfGraph {
    /// Writes the RDF graph to the provided writer in the specified format.
    ///
    /// # Arguments
    ///
    /// * `writer` - The writer to output the serialized RDF to
    /// * `format` - The RDF format to use for serialization
    ///
    /// # Errors
    ///
    /// Returns `RdfError::Serialization` if writing fails.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use std::fs::File;
    /// use rocraters::ro_crate::rdf::{RdfGraph, RdfFormat};
    ///
    /// let graph = /* ... */;
    /// let file = File::create("output.ttl")?;
    /// graph.write(file, RdfFormat::Turtle)?;
    /// ```
    pub fn write<W: Write>(&self, writer: W, format: RdfFormat) -> Result<(), RdfError> {
        let mut serializer =
            RdfSerializer::from_format(format.to_oxrdf_format()).for_writer(writer);

        // Serialize all triples in the graph
        for triple in self.iter() {
            serializer
                .serialize_triple(triple)
                .map_err(|e: std::io::Error| RdfError::Serialization(e.to_string()))?;
        }

        // Finish serialization
        serializer
            .finish()
            .map_err(|e: std::io::Error| RdfError::Serialization(e.to_string()))?;

        Ok(())
    }

    /// Serializes the RDF graph to a string in the specified format.
    ///
    /// This is a convenience method that uses `write()` internally with a
    /// byte buffer and converts the result to a UTF-8 string.
    ///
    /// # Arguments
    ///
    /// * `format` - The RDF format to use for serialization
    ///
    /// # Errors
    ///
    /// Returns `RdfError::Serialization` if serialization fails or the output
    /// is not valid UTF-8.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use rocraters::ro_crate::rdf::{RdfGraph, RdfFormat};
    ///
    /// let graph = /* ... */;
    /// let turtle = graph.to_string(RdfFormat::Turtle)?;
    /// println!("{}", turtle);
    /// ```
    pub fn to_string(&self, format: RdfFormat) -> Result<String, RdfError> {
        let mut buffer = Vec::new();
        self.write(&mut buffer, format)?;

        String::from_utf8(buffer)
            .map_err(|e| RdfError::Serialization(format!("Invalid UTF-8 in output: {}", e)))
    }
}

/// Parse RDF triples from input string.
///
/// # Arguments
///
/// * `input` - The RDF data as a string
/// * `format` - The RDF format of the input
/// * `base` - Optional base IRI for resolving relative IRIs
///
/// # Errors
///
/// Returns `RdfError::ParseError` if parsing fails.
fn parse_rdf(input: &str, format: RdfFormat, base: Option<&str>) -> Result<Vec<Triple>, RdfError> {
    let mut parser = RdfParser::from_format(format.to_oxrdf_format());

    if let Some(base_iri) = base {
        parser = parser
            .with_base_iri(base_iri)
            .map_err(|e| RdfError::ParseError(format!("Invalid base IRI: {}", e)))?;
    }

    let reader = input.as_bytes();
    let quads: Result<Vec<Quad>, _> = parser.for_reader(reader).collect();

    let quads = quads.map_err(|e| RdfError::ParseError(format!("Failed to parse RDF: {}", e)))?;

    // Convert Quads to Triples (ignoring graph information)
    let triples = quads
        .into_iter()
        .map(|q| Triple {
            subject: q.subject,
            predicate: q.predicate,
            object: q.object,
        })
        .collect();

    Ok(triples)
}

/// Find the metadata descriptor and root data entity IRIs from RDF triples.
///
/// Implements the RO-Crate 1.2 spec algorithm:
/// 1. Find metadata file IRI containing "ro-crate-metadata.json"
/// 2. Find the root crate IRI via schema:about predicate
/// 3. Verify the root is a schema:Dataset
///
/// # Arguments
///
/// * `triples` - The parsed RDF triples
///
/// # Errors
///
/// Returns `RdfError::MissingRootEntities` if required entities are not found.
///
/// NOTE: This function might be not so performant on very large graphs due to multiple passes.
fn find_root_entities(triples: &[Triple]) -> Result<(String, String), RdfError> {
    // Define important IRIs
    let schema_about = NamedNode::new_unchecked("http://schema.org/about");
    let rdf_type = NamedNode::new_unchecked("http://www.w3.org/1999/02/22-rdf-syntax-ns#type");
    let schema_dataset = NamedNode::new_unchecked("http://schema.org/Dataset");

    // Step 1: Find metadata file IRI containing "ro-crate-metadata.json"
    let mut metadata_iri: Option<String> = None;
    for triple in triples {
        if let NamedOrBlankNode::NamedNode(subject) = &triple.subject {
            let subject_str = subject.as_str();
            if subject_str.contains("ro-crate-metadata.json") {
                metadata_iri = Some(subject_str.to_string());
                break;
            }
        }
    }

    let metadata_iri = metadata_iri.ok_or_else(|| {
        RdfError::MissingRootEntities(
            "Could not find metadata file IRI containing 'ro-crate-metadata.json'".to_string(),
        )
    })?;

    // Step 2: Find root crate IRI via schema:about predicate
    let mut root_iri: Option<String> = None;
    for triple in triples {
        if let NamedOrBlankNode::NamedNode(subject) = &triple.subject {
            if subject.as_str() == metadata_iri && triple.predicate == schema_about {
                if let Term::NamedNode(object) = &triple.object {
                    root_iri = Some(object.as_str().to_string());
                    break;
                }
            }
        }
    }

    let root_iri = root_iri.ok_or_else(|| {
        RdfError::MissingRootEntities(
            "Could not find root crate IRI via schema:about predicate".to_string(),
        )
    })?;

    // Step 3: Verify the root is a schema:Dataset
    let mut is_dataset = false;
    for triple in triples {
        if let NamedOrBlankNode::NamedNode(subject) = &triple.subject {
            if subject.as_str() == root_iri && triple.predicate == rdf_type {
                if let Term::NamedNode(object) = &triple.object {
                    if object == &schema_dataset {
                        is_dataset = true;
                        break;
                    }
                }
            }
        }
    }

    if !is_dataset {
        return Err(RdfError::MissingRootEntities(format!(
            "Root IRI '{}' is not a schema:Dataset",
            root_iri
        )));
    }

    Ok((metadata_iri, root_iri))
}

/// Entity classification for determining whether an entity is a DataEntity or ContextualEntity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EntityType {
    Data,
    Contextual,
}

/// Convert an RDF Term to an EntityValue.
///
/// # Arguments
///
/// * `term` - The RDF term to convert
///
/// # Returns
///
/// An EntityValue representing the RDF term
fn term_to_entity_value(term: &Term) -> EntityValue {
    match term {
        Term::NamedNode(node) => EntityValue::EntityId(Id::Id(node.as_str().to_string())),
        Term::BlankNode(node) => EntityValue::EntityId(Id::Id(format!("_:{}", node.as_str()))),
        Term::Literal(literal) => {
            // Try to parse as number or boolean first
            let value_str = literal.value();

            // Try boolean
            if value_str == "true" || value_str == "false" {
                if let Ok(bool_val) = value_str.parse::<bool>() {
                    return EntityValue::EntityBool(bool_val);
                }
            }

            // Try integer
            if let Ok(int_val) = value_str.parse::<i64>() {
                return EntityValue::Entityi64(int_val);
            }

            // Try float
            if let Ok(float_val) = value_str.parse::<f64>() {
                return EntityValue::Entityf64(float_val);
            }

            // Default to string
            EntityValue::EntityString(value_str.to_string())
        }
    }
}

/// Extract all properties for a subject IRI from triples (without context compaction).
///
/// This is used internally for graph traversal where property names don't matter.
/// For final entity building, use `extract_entity_properties_with_context` instead.
fn extract_entity_properties_simple(
    subject_iri: &str,
    triples: &[Triple],
) -> HashMap<String, EntityValue> {
    let rdf_type = NamedNode::new_unchecked("http://www.w3.org/1999/02/22-rdf-syntax-ns#type");
    let mut properties: HashMap<String, Vec<EntityValue>> = HashMap::new();

    for triple in triples {
        if let NamedOrBlankNode::NamedNode(subject) = &triple.subject {
            if subject.as_str() == subject_iri {
                // Skip rdf:type as it's handled separately
                if triple.predicate == rdf_type {
                    continue;
                }

                // Use full IRI as property name (no compaction needed for traversal)
                let property_name = triple.predicate.as_str().to_string();
                let value = term_to_entity_value(&triple.object);

                properties.entry(property_name).or_default().push(value);
            }
        }
    }

    // Convert Vec<EntityValue> to EntityValue (single or array)
    let mut result = HashMap::new();
    for (key, values) in properties {
        if values.len() == 1 {
            result.insert(key, values.into_iter().next().unwrap());
        } else if values.len() > 1 {
            result.insert(key, EntityValue::EntityVec(values));
        }
    }

    result
}

/// Extract all IRI references from an EntityValue.
///
/// This iteratively searches through EntityValue structures to find all
/// IRI references (EntityId values). Uses an explicit stack to avoid
/// stack overflow on deeply nested structures.
///
/// Note: This extracts ALL IRIs including vocabulary terms. Filtering
/// (e.g., checking if IRIs are actual entity subjects) should be done
/// by the caller.
fn extract_iris_from_entity_value(value: &EntityValue, iris: &mut HashSet<String>) {
    let mut stack: Vec<&EntityValue> = vec![value];

    while let Some(current) = stack.pop() {
        match current {
            EntityValue::EntityId(id) => {
                match id {
                    Id::Id(iri) => {
                        // Only skip blank nodes - vocabulary filtering happens in caller
                        if !iri.starts_with("_:") {
                            iris.insert(iri.clone());
                        }
                    }
                    Id::IdArray(iris_vec) => {
                        for iri in iris_vec {
                            if !iri.starts_with("_:") {
                                iris.insert(iri.clone());
                            }
                        }
                    }
                }
            }
            EntityValue::EntityVec(vec) => {
                stack.extend(vec.iter());
            }
            EntityValue::EntityObject(map) => {
                stack.extend(map.values());
            }
            EntityValue::EntityVecObject(vec_map) => {
                for map in vec_map {
                    stack.extend(map.values());
                }
            }
            EntityValue::NestedDynamicEntity(nested) => {
                stack.push(nested.as_ref());
            }
            _ => {}
        }
    }
}

/// Check if an IRI is a vocabulary term based on context prefixes.
///
/// Vocabulary IRIs (types and properties from vocabularies like schema.org)
/// should not be followed during graph traversal as they are not entities.
fn is_vocabulary_iri(iri: &str, context: &ResolvedContext) -> bool {
    // Check against all known vocabulary namespaces from context
    for namespace in context.prefixes.values() {
        if iri.starts_with(namespace) {
            return true;
        }
    }
    // Check against @vocab if set
    if let Some(vocab) = &context.vocab {
        if iri.starts_with(vocab) {
            return true;
        }
    }
    false
}

/// Check if an IRI exists as a subject in the triples.
///
/// An IRI that is referenced but doesn't exist as a subject is a "dangling reference"
/// pointing to an external resource not defined in this RO-Crate.
fn is_entity_subject(iri: &str, triples: &[Triple]) -> bool {
    triples
        .iter()
        .any(|t| matches!(&t.subject, NamedOrBlankNode::NamedNode(n) if n.as_str() == iri))
}

/// Walk the graph from root, collecting all reachable entity IRIs.
///
/// This performs a breadth-first traversal of the RDF graph starting from
/// the root entity, following all IRI references to build a complete set
/// of reachable entities.
///
/// Vocabulary IRIs (from context prefixes) are skipped during traversal.
/// Dangling references (non-vocabulary IRIs without properties in the graph)
/// trigger a warning per RO-Crate spec recommendation (SHOULD have info about
/// contextual entities).
///
/// # Arguments
///
/// * `root_iri` - The root entity IRI to start from
/// * `triples` - The RDF triples representing the graph
/// * `context` - The resolved context for vocabulary detection
///
/// # Returns
///
/// A HashSet of all reachable entity IRIs that exist in the graph
fn collect_reachable_entities(
    root_iri: &str,
    triples: &[Triple],
    context: &ResolvedContext,
) -> HashSet<String> {
    let mut reachable = HashSet::new();
    let mut to_process = vec![root_iri.to_string()];
    let mut processed = HashSet::new();

    while let Some(current_iri) = to_process.pop() {
        if processed.contains(&current_iri) {
            continue;
        }
        processed.insert(current_iri.clone());
        reachable.insert(current_iri.clone());

        // Extract properties for this entity
        let properties = extract_entity_properties_simple(&current_iri, triples);

        // Find all referenced IRIs
        let mut referenced_iris = HashSet::new();
        for value in properties.values() {
            extract_iris_from_entity_value(value, &mut referenced_iris);
        }

        // Add newly discovered IRIs to the processing queue
        for iri in referenced_iris {
            if processed.contains(&iri) {
                continue;
            }

            // Skip vocabulary IRIs (types/properties from known vocabularies)
            if is_vocabulary_iri(&iri, context) {
                continue;
            }

            // Check if this IRI exists as a subject (is an entity in the graph)
            if is_entity_subject(&iri, triples) {
                to_process.push(iri);
            } else {
                // Dangling reference - external resource not defined in this RO-Crate
                // Per spec: "The RO-Crate SHOULD contain additional information about Contextual Entities"
                log::warn!(
                    "Dangling reference '{}': referenced but not defined in the graph. \
                     Consider adding a contextual entity for this reference.",
                    iri
                );
            }
        }
    }

    reachable
}

/// Check if a type string represents a File or Dataset type per RO-Crate spec.
///
/// Handles compact terms, prefixed forms, and expanded IRIs:
/// - "File" is an RO-Crate alias for `http://schema.org/MediaObject`
/// - "Dataset" maps to `http://schema.org/Dataset`
///
/// Note: After context compaction, the type may appear as:
/// - Compact term: "File", "Dataset"
/// - Vocab-stripped: "MediaObject" (if @vocab is http://schema.org/)
/// - Prefixed: "schema:MediaObject", "schema:Dataset"
/// - Full IRI: "http://schema.org/MediaObject", "http://schema.org/Dataset"
fn is_file_or_dataset_type(type_str: &str) -> bool {
    // Compact RO-Crate terms
    type_str == "File"
        || type_str == "Dataset"
        // Vocab-stripped or plain schema.org terms
        || type_str == "MediaObject"
        // Prefixed forms
        || type_str == "schema:MediaObject"
        || type_str == "schema:Dataset"
        // Expanded IRIs (http)
        || type_str == "http://schema.org/MediaObject"
        || type_str == "http://schema.org/Dataset"
        // Expanded IRIs (https)
        || type_str == "https://schema.org/MediaObject"
        || type_str == "https://schema.org/Dataset"
}

/// Determine if an IRI should be a DataEntity or ContextualEntity.
///
/// Per RO-Crate 1.2 spec (https://www.researchobject.org/ro-crate/specification/1.2/data-entities.html):
/// An entity which has `File` or `Dataset` as one of its @type values:
/// - Is considered to be a Data Entity if its @id is an absolute URI or a relative URI.
/// - MAY have an @id which is a local identifier beginning with `#`, in which case it is
///   NOT considered to be a Data Entity.
///
/// Note: In RO-Crate context, `File` is an alias for `http://schema.org/MediaObject`.
///
/// # Arguments
///
/// * `id` - The entity IRI to classify
/// * `types` - The rdf:type values for this entity
///
/// # Returns
///
/// The EntityType (Data or Contextual)
fn classify_entity(id: &str, types: &[String]) -> EntityType {
    let has_data_type = types.iter().any(|t| is_file_or_dataset_type(t));

    if has_data_type && !id.starts_with('#') {
        EntityType::Data
    } else {
        EntityType::Contextual
    }
}

/// Infer base IRI from metadata file IRI by stripping the filename.
///
/// # Arguments
///
/// * `metadata_iri` - The metadata file IRI (should contain "ro-crate-metadata.json")
///
/// # Returns
///
/// The base IRI if it can be inferred, None otherwise
///
/// # Example
///
/// ```ignore
/// let base = infer_base_from_metadata("http://example.org/ro-crate-metadata.json");
/// assert_eq!(base, Some("http://example.org/".to_string()));
/// ```
fn infer_base_from_metadata(metadata_iri: &str) -> Option<String> {
    // Check if this is a metadata file (supports prefixed filenames like "my-project-ro-crate-metadata.json")
    if metadata_iri.contains("ro-crate-metadata.json") {
        // Find the last '/' to get the directory portion
        // This correctly handles: http://example.org/crate/my-project-ro-crate-metadata.json
        // returning: http://example.org/crate/
        if let Some(pos) = metadata_iri.rfind('/') {
            return Some(metadata_iri[..=pos].to_string());
        }
    }
    None
}

/// Helper function to extract types from triples for a given IRI.
///
/// Returns a tuple of (DataType, Vec<String>) where the Vec contains raw type strings
/// needed for entity classification.
fn extract_types(
    iri: &str,
    triples: &[Triple],
    context: &ResolvedContext,
) -> (DataType, Vec<String>) {
    let rdf_type = NamedNode::new_unchecked("http://www.w3.org/1999/02/22-rdf-syntax-ns#type");
    let mut types = Vec::new();

    for triple in triples {
        if let NamedOrBlankNode::NamedNode(subject) = &triple.subject {
            if subject.as_str() == iri && triple.predicate == rdf_type {
                if let Term::NamedNode(type_node) = &triple.object {
                    let compacted_type = context.compact_iri(type_node.as_str());
                    types.push(compacted_type);
                }
            }
        }
    }

    let data_type = if types.is_empty() {
        DataType::Term("Thing".to_string())
    } else if types.len() == 1 {
        DataType::Term(types[0].clone())
    } else {
        DataType::TermArray(types.clone())
    };

    (data_type, types)
}

/// Helper function to compact entity properties using ResolvedContext.
fn compact_entity_properties(
    properties: &mut HashMap<String, EntityValue>,
    context: &ResolvedContext,
) {
    for value in properties.values_mut() {
        compact_entity_value_with_context(value, context);
    }
}

/// Helper function to add "./" prefix to relative paths (for RO-Crate compliance).
fn ensure_relative_prefix(iri: &str) -> String {
    // If it's already a relative path or an absolute URI, return as-is
    if iri.starts_with("./")
        || iri.starts_with("../")
        || iri.starts_with("http://")
        || iri.starts_with("https://")
        || iri.starts_with("#")
        || iri.starts_with("_:")
    {
        iri.to_string()
    } else if !iri.contains(':') {
        // It's a bare path (like "data.csv"), add "./" prefix
        format!("./{}", iri)
    } else {
        // It's something else (like "schema:Person"), return as-is
        iri.to_string()
    }
}

/// Helper function to compact an EntityValue recursively using ResolvedContext.
fn compact_entity_value_with_context(value: &mut EntityValue, context: &ResolvedContext) {
    match value {
        EntityValue::EntityId(id) => {
            match id {
                Id::Id(iri) => {
                    // Skip blank nodes
                    if !iri.starts_with("_:") {
                        let compacted = context.compact_iri(iri);
                        // Log if compaction didn't work for an HTTP IRI
                        if compacted == *iri && iri.starts_with("http") {
                            log::warn!("No context mapping for IRI '{}', using raw URI", iri);
                        }
                        // Add "./" prefix for RO-Crate relative paths
                        *iri = ensure_relative_prefix(&compacted);
                    }
                }
                Id::IdArray(iris_vec) => {
                    for iri in iris_vec.iter_mut() {
                        if !iri.starts_with("_:") {
                            let compacted = context.compact_iri(iri);
                            // Log if compaction didn't work for an HTTP IRI
                            if compacted == *iri && iri.starts_with("http") {
                                log::warn!("No context mapping for IRI '{}', using raw URI", iri);
                            }
                            // Add "./" prefix for RO-Crate relative paths
                            *iri = ensure_relative_prefix(&compacted);
                        }
                    }
                }
            }
        }
        EntityValue::EntityVec(vec) => {
            for v in vec.iter_mut() {
                compact_entity_value_with_context(v, context);
            }
        }
        EntityValue::EntityObject(map) => {
            for v in map.values_mut() {
                compact_entity_value_with_context(v, context);
            }
        }
        EntityValue::EntityVecObject(vec_map) => {
            for map in vec_map.iter_mut() {
                for v in map.values_mut() {
                    compact_entity_value_with_context(v, context);
                }
            }
        }
        EntityValue::NestedDynamicEntity(nested) => {
            compact_entity_value_with_context(nested, context);
        }
        _ => {}
    }
}

/// Helper function to extract entity properties with compacted predicate names.
fn extract_entity_properties_with_context(
    subject_iri: &str,
    triples: &[Triple],
    context: &ResolvedContext,
) -> HashMap<String, EntityValue> {
    let rdf_type = NamedNode::new_unchecked("http://www.w3.org/1999/02/22-rdf-syntax-ns#type");
    let mut properties: HashMap<String, Vec<EntityValue>> = HashMap::new();

    for triple in triples {
        if let NamedOrBlankNode::NamedNode(subject) = &triple.subject {
            if subject.as_str() == subject_iri {
                // Skip rdf:type as it's handled separately
                if triple.predicate == rdf_type {
                    continue;
                }

                // Use context to compact the predicate IRI
                let property_name = context.compact_iri(triple.predicate.as_str());
                let value = term_to_entity_value(&triple.object);

                properties.entry(property_name).or_default().push(value);
            }
        }
    }

    // Convert Vec<EntityValue> to EntityValue (single or array)
    let mut result = HashMap::new();
    for (key, values) in properties {
        if values.len() == 1 {
            result.insert(key, values.into_iter().next().unwrap());
        } else if values.len() > 1 {
            result.insert(key, EntityValue::EntityVec(values));
        }
    }

    result
}

/// Build MetadataDescriptor from triples.
fn build_metadata_descriptor(
    metadata_iri: &str,
    root_iri: &str,
    triples: &[Triple],
    context: &ResolvedContext,
) -> MetadataDescriptor {
    let mut metadata_properties =
        extract_entity_properties_with_context(metadata_iri, triples, context);
    let (metadata_type, _) = extract_types(metadata_iri, triples, context);

    // Remove 'about' from dynamic_entity as it's a required field
    let about = metadata_properties
        .remove("about")
        .and_then(|v| match v {
            EntityValue::EntityId(id) => Some(id),
            _ => None,
        })
        .unwrap_or_else(|| Id::Id(root_iri.to_string()));

    // Compact the metadata ID
    let compacted_id = if metadata_iri.contains("ro-crate-metadata.json") {
        "ro-crate-metadata.json".to_string()
    } else {
        context.compact_iri(metadata_iri)
    };

    // Compact properties
    compact_entity_properties(&mut metadata_properties, context);

    MetadataDescriptor {
        id: compacted_id,
        type_: metadata_type,
        conforms_to: Id::Id("https://w3id.org/ro/crate/1.2/context".to_string()),
        about,
        dynamic_entity: Some(metadata_properties),
    }
}

/// Build RootDataEntity from triples.
fn build_root_entity(
    root_iri: &str,
    triples: &[Triple],
    context: &ResolvedContext,
) -> RootDataEntity {
    let mut root_properties = extract_entity_properties_with_context(root_iri, triples, context);
    let (root_type, _) = extract_types(root_iri, triples, context);

    // Extract SHOULD fields with warnings for missing values
    let name = root_properties
        .remove("name")
        .and_then(|v| match v {
            EntityValue::EntityString(s) => Some(s),
            _ => None,
        })
        .unwrap_or_else(|| {
            log::warn!(
                "Root entity '{}' missing 'name' property (SHOULD per RO-Crate spec)",
                root_iri
            );
            String::new()
        });

    let description = root_properties
        .remove("description")
        .and_then(|v| match v {
            EntityValue::EntityString(s) => Some(s),
            _ => None,
        })
        .unwrap_or_else(|| {
            log::warn!(
                "Root entity '{}' missing 'description' property (SHOULD per RO-Crate spec)",
                root_iri
            );
            String::new()
        });

    let date_published = root_properties
        .remove("datePublished")
        .and_then(|v| match v {
            EntityValue::EntityString(s) => Some(s),
            _ => None,
        })
        .unwrap_or_else(|| {
            log::warn!(
                "Root entity '{}' missing 'datePublished' property (SHOULD per RO-Crate spec)",
                root_iri
            );
            String::new()
        });

    let license = root_properties
        .remove("license")
        .map(|v| match v {
            EntityValue::EntityId(id) => License::Id(id),
            EntityValue::EntityString(s) => License::Description(s),
            _ => License::Description(String::new()),
        })
        .unwrap_or_else(|| {
            log::warn!(
                "Root entity '{}' missing 'license' property (SHOULD per RO-Crate spec)",
                root_iri
            );
            License::Description(String::new())
        });

    // Compact root ID
    let compacted_id = ensure_relative_prefix(&context.compact_iri(root_iri));

    // Compact properties
    compact_entity_properties(&mut root_properties, context);

    RootDataEntity {
        id: compacted_id,
        type_: root_type,
        name,
        description,
        date_published,
        license,
        dynamic_entity: Some(root_properties),
    }
}

/// Build a DataEntity or ContextualEntity from triples.
fn build_graph_entity(iri: &str, triples: &[Triple], context: &ResolvedContext) -> GraphVector {
    let mut properties = extract_entity_properties_with_context(iri, triples, context);
    let (entity_type, type_strings) = extract_types(iri, triples, context);

    // Compact entity ID
    let compacted_id = ensure_relative_prefix(&context.compact_iri(iri));

    // Compact properties
    compact_entity_properties(&mut properties, context);

    // Classify entity based on types and IRI patterns
    match classify_entity(&compacted_id, &type_strings) {
        EntityType::Data => GraphVector::DataEntity(DataEntity {
            id: compacted_id,
            type_: entity_type,
            dynamic_entity: Some(properties),
        }),
        EntityType::Contextual => GraphVector::ContextualEntity(ContextualEntity {
            id: compacted_id,
            type_: entity_type,
            dynamic_entity: Some(properties),
        }),
    }
}

/// Internal implementation: Convert RDF triples to RoCrate using a ResolvedContext.
fn rdf_to_rocrate_with_context(
    triples: Vec<Triple>,
    context: ResolvedContext,
) -> Result<RoCrate, RdfError> {
    // Find root entities
    let (metadata_iri, root_iri) = find_root_entities(&triples)?;

    // Build metadata descriptor and root entity
    let metadata_descriptor =
        build_metadata_descriptor(&metadata_iri, &root_iri, &triples, &context);
    let root_entity = build_root_entity(&root_iri, &triples, &context);

    // Walk the graph to collect reachable entities
    let reachable_iris = collect_reachable_entities(&root_iri, &triples, &context);

    // Find all entity IRIs in the graph (exclude metadata and root)
    let mut all_iris = HashSet::new();
    for triple in &triples {
        if let NamedOrBlankNode::NamedNode(subject) = &triple.subject {
            let iri = subject.as_str();
            if iri != metadata_iri && iri != root_iri {
                all_iris.insert(iri.to_string());
            }
        }
    }

    // Warn about unreferenced entities
    for iri in &all_iris {
        if !reachable_iris.contains(iri) {
            log::warn!(
                "Entity '{}' is not reachable from root and will be excluded",
                iri
            );
        }
    }

    // Build graph starting with metadata and root
    let mut graph = vec![
        GraphVector::MetadataDescriptor(metadata_descriptor),
        GraphVector::RootDataEntity(root_entity),
    ];

    // Build entities for each reachable IRI
    for iri in reachable_iris {
        // Skip metadata and root as they're already added
        if iri == metadata_iri || iri == root_iri {
            continue;
        }

        graph.push(build_graph_entity(&iri, &triples, &context));
    }

    // Create RoCrate with RO-Crate 1.2 default context
    let ro_context =
        RoCrateContext::ReferenceContext("https://w3id.org/ro/crate/1.2/context".to_string());

    Ok(RoCrate {
        context: ro_context,
        graph,
    })
}

/// Convert an RdfGraph to a RoCrate structure.
///
/// This function uses the RdfGraph's stored context for IRI compaction,
/// ensuring that the same context used for RoCrate → RDF conversion
/// is used for the reverse RDF → RoCrate conversion.
///
/// # Arguments
///
/// * `graph` - The RdfGraph containing triples and context
///
/// # Errors
///
/// Returns `RdfError` if conversion fails or required entities are missing.
///
/// # Example
///
/// ```ignore
/// use rocraters::ro_crate::rdf::{rocrate_to_rdf, rdf_graph_to_rocrate, ContextResolverBuilder};
///
/// // Convert RoCrate to RDF
/// let rdf_graph = rocrate_to_rdf(&rocrate, ContextResolverBuilder::default())?;
///
/// // Convert back to RoCrate using the same context
/// let restored_crate = rdf_graph_to_rocrate(rdf_graph)?;
/// ```
pub fn rdf_graph_to_rocrate(graph: RdfGraph) -> Result<RoCrate, RdfError> {
    // Convert HashSet<Triple> to Vec<Triple> for processing
    let triples: Vec<Triple> = graph.triples.into_iter().collect();

    // Use the graph's stored context for compaction
    rdf_to_rocrate_with_context(triples, graph.context)
}

/// Convert RDF data to a RoCrate structure.
///
/// This is the main entry point for parsing RDF into RO-Crate format.
/// Implements full entity extraction, graph walking, and entity classification.
///
/// # Arguments
///
/// * `input` - The RDF data as a string
/// * `format` - The RDF format of the input
/// * `base` - Optional base IRI for resolving relative IRIs during parsing
///
/// # Errors
///
/// Returns `RdfError` if parsing fails or required entities are missing.
///
/// # Example
///
/// ```ignore
/// use rocraters::ro_crate::rdf::{rdf_to_rocrate, RdfFormat};
///
/// let rdf_data = "..."; // Turtle format RDF
/// let crate = rdf_to_rocrate(rdf_data, RdfFormat::Turtle, None)?;
/// ```
pub fn rdf_to_rocrate(
    input: &str,
    format: RdfFormat,
    base: Option<&str>,
) -> Result<RoCrate, RdfError> {
    // Parse RDF into triples
    let triples = parse_rdf(input, format, base)?;

    // Find root entities to infer base IRI
    let (metadata_iri, _) = find_root_entities(&triples)?;

    // Determine the base IRI for context resolution
    let base_iri = base
        .map(|b| b.to_string())
        .or_else(|| infer_base_from_metadata(&metadata_iri))
        .unwrap_or_else(|| "http://example.org/".to_string());

    // Create a ResolvedContext with RO-Crate 1.2 default context + base IRI
    let ro_context =
        RoCrateContext::ReferenceContext("https://w3id.org/ro/crate/1.2/context".to_string());

    let resolver = ContextResolverBuilder::default();
    let mut context = resolver
        .resolve(&ro_context)
        .map_err(|e| RdfError::ParseError(format!("Failed to resolve context: {}", e)))?;

    // Set the base IRI in the context for proper compaction
    context.base = Some(base_iri);

    // Use the internal implementation with the resolved context
    rdf_to_rocrate_with_context(triples, context)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ro_crate::context::RoCrateContext;
    use crate::ro_crate::rdf::context::ResolvedContext;
    use oxrdf::{Literal, NamedNode, Triple};

    fn create_test_graph() -> RdfGraph {
        let ctx = ResolvedContext::new(RoCrateContext::ReferenceContext(
            "https://w3id.org/ro/crate/1.2/context".to_string(),
        ));
        let mut graph = RdfGraph::new(ctx);

        let subject = NamedNode::new_unchecked("http://example.org/subject");
        let predicate = NamedNode::new_unchecked("http://example.org/predicate");
        let object = Literal::new_simple_literal("test value");

        graph.insert(Triple::new(subject, predicate, object));
        graph
    }

    #[test]
    fn test_write_turtle() {
        let graph = create_test_graph();
        let result = graph.to_string(RdfFormat::Turtle);
        assert!(result.is_ok());

        let turtle = result.unwrap();
        assert!(turtle.contains("example.org"));
        assert!(turtle.contains("test value"));
    }

    #[test]
    fn test_write_ntriples() {
        let graph = create_test_graph();
        let result = graph.to_string(RdfFormat::NTriples);
        assert!(result.is_ok());

        let ntriples = result.unwrap();
        assert!(ntriples.contains("example.org"));
        assert!(ntriples.contains("test value"));
    }

    #[test]
    fn test_write_to_buffer() {
        let graph = create_test_graph();
        let mut buffer = Vec::new();
        let result = graph.write(&mut buffer, RdfFormat::Turtle);
        assert!(result.is_ok());
        assert!(!buffer.is_empty());
    }

    #[test]
    fn test_empty_graph() {
        let ctx = ResolvedContext::new(RoCrateContext::ReferenceContext(
            "https://w3id.org/ro/crate/1.2/context".to_string(),
        ));
        let graph = RdfGraph::new(ctx);
        let result = graph.to_string(RdfFormat::Turtle);
        assert!(result.is_ok());
    }

    #[test]
    fn test_format_conversion() {
        assert_eq!(RdfFormat::Turtle.to_oxrdf_format(), OxRdfFormat::Turtle);
        assert_eq!(RdfFormat::NTriples.to_oxrdf_format(), OxRdfFormat::NTriples);
        assert_eq!(RdfFormat::NQuads.to_oxrdf_format(), OxRdfFormat::NQuads);
        assert_eq!(RdfFormat::RdfXml.to_oxrdf_format(), OxRdfFormat::RdfXml);
    }

    #[test]
    fn test_parse_rdf_turtle() {
        let turtle = r#"
            @prefix schema: <http://schema.org/> .

            <http://example.org/ro-crate-metadata.json> a schema:CreativeWork ;
                schema:about <http://example.org/> .

            <http://example.org/> a schema:Dataset ;
                schema:name "Test Dataset" .
        "#;

        let result = parse_rdf(turtle, RdfFormat::Turtle, None);
        assert!(result.is_ok());
        let triples = result.unwrap();
        assert!(triples.len() > 0);
    }

    #[test]
    fn test_find_root_entities_success() {
        let turtle = r#"
            @prefix schema: <http://schema.org/> .
            @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

            <http://example.org/ro-crate-metadata.json> a schema:CreativeWork ;
                schema:about <http://example.org/> .

            <http://example.org/> a schema:Dataset ;
                schema:name "Test Dataset" .
        "#;

        let triples = parse_rdf(turtle, RdfFormat::Turtle, None).unwrap();
        let result = find_root_entities(&triples);
        assert!(result.is_ok());

        let (metadata_iri, root_iri) = result.unwrap();
        assert_eq!(metadata_iri, "http://example.org/ro-crate-metadata.json");
        assert_eq!(root_iri, "http://example.org/");
    }

    #[test]
    fn test_find_root_entities_missing_metadata() {
        let turtle = r#"
            @prefix schema: <http://schema.org/> .

            <http://example.org/> a schema:Dataset ;
                schema:name "Test Dataset" .
        "#;

        let triples = parse_rdf(turtle, RdfFormat::Turtle, None).unwrap();
        let result = find_root_entities(&triples);
        assert!(result.is_err());
        assert!(matches!(result, Err(RdfError::MissingRootEntities(_))));
    }

    #[test]
    fn test_find_root_entities_not_dataset() {
        let turtle = r#"
            @prefix schema: <http://schema.org/> .

            <http://example.org/ro-crate-metadata.json> a schema:CreativeWork ;
                schema:about <http://example.org/> .

            <http://example.org/> a schema:CreativeWork ;
                schema:name "Not a Dataset" .
        "#;

        let triples = parse_rdf(turtle, RdfFormat::Turtle, None).unwrap();
        let result = find_root_entities(&triples);
        assert!(result.is_err());
        assert!(matches!(result, Err(RdfError::MissingRootEntities(_))));
    }

    #[test]
    fn test_rdf_to_rocrate_minimal() {
        let turtle = r#"
            @prefix schema: <http://schema.org/> .

            <http://example.org/ro-crate-metadata.json> a schema:CreativeWork ;
                schema:about <./> .

            <./> a schema:Dataset ;
                schema:name "Test Dataset" ;
                schema:description "A test dataset" .
        "#;

        let result = rdf_to_rocrate(turtle, RdfFormat::Turtle, Some("http://example.org/"));
        assert!(result.is_ok());

        let crate_obj = result.unwrap();
        assert_eq!(crate_obj.graph.len(), 2);

        // Check that we have a MetadataDescriptor and RootDataEntity
        let has_metadata = crate_obj
            .graph
            .iter()
            .any(|g| matches!(g, GraphVector::MetadataDescriptor(_)));
        let has_root = crate_obj
            .graph
            .iter()
            .any(|g| matches!(g, GraphVector::RootDataEntity(_)));
        assert!(has_metadata);
        assert!(has_root);
    }

    #[test]
    fn test_parse_rdf_invalid_format() {
        let invalid_turtle = "this is not valid turtle";
        let result = parse_rdf(invalid_turtle, RdfFormat::Turtle, None);
        assert!(result.is_err());
        assert!(matches!(result, Err(RdfError::ParseError(_))));
    }

    #[test]
    fn test_classify_entity() {
        // Per RO-Crate 1.2 spec: only File (MediaObject) or Dataset types determine Data Entity
        // Without File/Dataset type, entity is Contextual regardless of @id
        assert_eq!(classify_entity("./data.txt", &[]), EntityType::Contextual);
        assert_eq!(classify_entity("../file.csv", &[]), EntityType::Contextual);
        assert_eq!(
            classify_entity("http://example.org/data.json", &[]),
            EntityType::Contextual
        );

        // DataEntity: File type (compact form) with non-# @id
        assert_eq!(
            classify_entity("./data.txt", &["File".to_string()]),
            EntityType::Data
        );
        assert_eq!(
            classify_entity("http://example.org/data", &["File".to_string()]),
            EntityType::Data
        );

        // DataEntity: MediaObject (same as File in RO-Crate) with non-# @id
        assert_eq!(
            classify_entity("./data.txt", &["MediaObject".to_string()]),
            EntityType::Data
        );
        assert_eq!(
            classify_entity(
                "http://example.org/data",
                &["schema:MediaObject".to_string()]
            ),
            EntityType::Data
        );

        // DataEntity: File type with expanded IRI (http://schema.org/MediaObject)
        assert_eq!(
            classify_entity("./data.txt", &["http://schema.org/MediaObject".to_string()]),
            EntityType::Data
        );

        // DataEntity: Dataset type
        assert_eq!(
            classify_entity("http://example.org/item", &["Dataset".to_string()]),
            EntityType::Data
        );
        assert_eq!(
            classify_entity("./subdir/", &["http://schema.org/Dataset".to_string()]),
            EntityType::Data
        );

        // ContextualEntity: File/Dataset with # prefix @id (local identifier)
        assert_eq!(
            classify_entity("#file1", &["File".to_string()]),
            EntityType::Contextual
        );
        assert_eq!(
            classify_entity("#dataset1", &["Dataset".to_string()]),
            EntityType::Contextual
        );
        assert_eq!(
            classify_entity("#media1", &["MediaObject".to_string()]),
            EntityType::Contextual
        );

        // ContextualEntity: non-File/Dataset types
        assert_eq!(classify_entity("#person1", &[]), EntityType::Contextual);
        assert_eq!(
            classify_entity(
                "http://orcid.org/0000-0001-2345-6789",
                &["Person".to_string()]
            ),
            EntityType::Contextual
        );
        assert_eq!(
            classify_entity("http://example.org/alice", &["Person".to_string()]),
            EntityType::Contextual
        );
    }

    #[test]
    fn test_extract_entity_properties_simple() {
        let turtle = r#"
            @prefix schema: <http://schema.org/> .

            <http://example.org/person> schema:name "Alice" ;
                schema:email "alice@example.org" ;
                schema:age "30" .
        "#;

        let triples = parse_rdf(turtle, RdfFormat::Turtle, None).unwrap();
        let properties = extract_entity_properties_simple("http://example.org/person", &triples);

        // Properties will have full IRIs as keys (not compacted)
        assert!(properties.contains_key("http://schema.org/name"));
        assert!(properties.contains_key("http://schema.org/email"));
        assert!(properties.contains_key("http://schema.org/age"));

        match properties.get("http://schema.org/name") {
            Some(EntityValue::EntityString(s)) => assert_eq!(s, "Alice"),
            _ => panic!("Expected name to be a string"),
        }
    }

    #[test]
    fn test_extract_entity_properties_multiple_values() {
        let turtle = r#"
            @prefix schema: <http://schema.org/> .

            <http://example.org/dataset> schema:author <http://example.org/alice> ;
                schema:author <http://example.org/bob> .
        "#;

        let triples = parse_rdf(turtle, RdfFormat::Turtle, None).unwrap();
        let properties = extract_entity_properties_simple("http://example.org/dataset", &triples);

        assert!(properties.contains_key("http://schema.org/author"));

        match properties.get("http://schema.org/author") {
            Some(EntityValue::EntityVec(vec)) => {
                assert_eq!(vec.len(), 2);
            }
            _ => panic!("Expected author to be a vector"),
        }
    }

    #[test]
    fn test_collect_reachable_entities() {
        let turtle = r#"
            @prefix schema: <http://schema.org/> .

            <http://example.org/ro-crate-metadata.json> a schema:CreativeWork ;
                schema:about <http://example.org/> .

            <http://example.org/> a schema:Dataset ;
                schema:name "Test Dataset" ;
                schema:author <http://example.org/alice> ;
                schema:hasPart <http://example.org/data.csv> .

            <http://example.org/alice> a schema:Person ;
                schema:name "Alice" .

            <http://example.org/data.csv> a schema:MediaObject ;
                schema:name "Data File" .

            <http://example.org/unreferenced> a schema:Thing ;
                schema:name "Not Referenced" .
        "#;

        let triples = parse_rdf(turtle, RdfFormat::Turtle, None).unwrap();

        // Create a context with schema.org prefix for vocabulary filtering
        let ro_context =
            RoCrateContext::ReferenceContext("https://w3id.org/ro/crate/1.2/context".to_string());
        let resolver = ContextResolverBuilder::default();
        let context = resolver.resolve(&ro_context).unwrap();

        let reachable = collect_reachable_entities("http://example.org/", &triples, &context);

        // Root should be reachable
        assert!(reachable.contains("http://example.org/"));

        // Referenced entities should be reachable
        assert!(reachable.contains("http://example.org/alice"));
        assert!(reachable.contains("http://example.org/data.csv"));

        // Unreferenced entity should not be reachable
        assert!(!reachable.contains("http://example.org/unreferenced"));
    }

    #[test]
    fn test_rdf_to_rocrate_full() {
        let turtle = r#"
            @prefix schema: <http://schema.org/> .

            <http://example.org/ro-crate-metadata.json> a schema:CreativeWork ;
                schema:about <./> ;
                schema:conformsTo <https://w3id.org/ro/crate/1.2/context> .

            <./> a schema:Dataset ;
                schema:name "My Research Dataset" ;
                schema:description "A dataset for testing" ;
                schema:datePublished "2024-01-01" ;
                schema:license "MIT" ;
                schema:author <#alice> ;
                schema:hasPart <./data.csv> .

            <#alice> a schema:Person ;
                schema:name "Alice Smith" ;
                schema:email "alice@example.org" .

            <./data.csv> a schema:MediaObject ;
                schema:name "Data File" ;
                schema:encodingFormat "text/csv" .
        "#;

        let result = rdf_to_rocrate(turtle, RdfFormat::Turtle, Some("http://example.org/"));
        assert!(result.is_ok());

        let crate_obj = result.unwrap();

        // Should have 4 entities: metadata, root, person, data file
        assert_eq!(crate_obj.graph.len(), 4);

        // Check metadata descriptor
        let has_metadata = crate_obj
            .graph
            .iter()
            .any(|g| matches!(g, GraphVector::MetadataDescriptor(_)));
        assert!(has_metadata);

        // Check root entity
        let has_root = crate_obj
            .graph
            .iter()
            .any(|g| matches!(g, GraphVector::RootDataEntity(_)));
        assert!(has_root);

        // Check data entity
        let has_data = crate_obj
            .graph
            .iter()
            .any(|g| matches!(g, GraphVector::DataEntity(_)));
        assert!(has_data);

        // Check contextual entity
        let has_contextual = crate_obj
            .graph
            .iter()
            .any(|g| matches!(g, GraphVector::ContextualEntity(_)));
        assert!(has_contextual);
    }

    #[test]
    fn test_rdf_to_rocrate_with_unreferenced_entities() {
        let turtle = r#"
            @prefix schema: <http://schema.org/> .

            <http://example.org/ro-crate-metadata.json> a schema:CreativeWork ;
                schema:about <./> .

            <./> a schema:Dataset ;
                schema:name "Test Dataset" ;
                schema:description "Testing" ;
                schema:datePublished "2024-01-01" ;
                schema:license "MIT" ;
                schema:author <#alice> .

            <#alice> a schema:Person ;
                schema:name "Alice" .

            <#bob> a schema:Person ;
                schema:name "Bob - Not Referenced" .
        "#;

        // This should log a warning about Bob not being reachable
        let result = rdf_to_rocrate(turtle, RdfFormat::Turtle, Some("http://example.org/"));
        assert!(result.is_ok());

        let crate_obj = result.unwrap();

        // Should have 3 entities: metadata, root, and alice (bob excluded)
        assert_eq!(crate_obj.graph.len(), 3);
    }

    #[test]
    fn test_term_to_entity_value() {
        use oxrdf::Literal;

        // Test string literal
        let literal = Term::Literal(Literal::new_simple_literal("test"));
        match term_to_entity_value(&literal) {
            EntityValue::EntityString(s) => assert_eq!(s, "test"),
            _ => panic!("Expected EntityString"),
        }

        // Test integer literal
        let literal = Term::Literal(Literal::new_simple_literal("42"));
        match term_to_entity_value(&literal) {
            EntityValue::Entityi64(i) => assert_eq!(i, 42),
            _ => panic!("Expected Entityi64"),
        }

        // Test boolean literal
        let literal = Term::Literal(Literal::new_simple_literal("true"));
        match term_to_entity_value(&literal) {
            EntityValue::EntityBool(b) => assert!(b),
            _ => panic!("Expected EntityBool"),
        }

        // Test named node
        let node = Term::NamedNode(NamedNode::new_unchecked("http://example.org/thing"));
        match term_to_entity_value(&node) {
            EntityValue::EntityId(Id::Id(s)) => assert_eq!(s, "http://example.org/thing"),
            _ => panic!("Expected EntityId"),
        }
    }

    #[test]
    fn test_infer_base_from_metadata() {
        // Standard case
        let base = infer_base_from_metadata("http://example.org/ro-crate-metadata.json");
        assert_eq!(base, Some("http://example.org/".to_string()));

        // With subdirectory
        let base = infer_base_from_metadata("http://example.org/path/to/ro-crate-metadata.json");
        assert_eq!(base, Some("http://example.org/path/to/".to_string()));

        // Prefixed filename (allowed by RO-Crate spec)
        let base = infer_base_from_metadata("http://example.org/my-project-ro-crate-metadata.json");
        assert_eq!(base, Some("http://example.org/".to_string()));

        // Prefixed with subdirectory
        let base = infer_base_from_metadata("http://example.org/crate/v1-ro-crate-metadata.json");
        assert_eq!(base, Some("http://example.org/crate/".to_string()));

        // Missing filename - not a metadata file
        let base = infer_base_from_metadata("http://example.org/");
        assert_eq!(base, None);

        // Just filename (no path) - returns None since no base URL
        let base = infer_base_from_metadata("ro-crate-metadata.json");
        assert_eq!(base, None);
    }

    #[test]
    fn test_rdf_to_rocrate_with_compaction() {
        let turtle = r#"
            @prefix schema: <http://schema.org/> .

            <http://example.org/ro-crate-metadata.json> a schema:CreativeWork ;
                schema:about <http://example.org/> ;
                schema:conformsTo <https://w3id.org/ro/crate/1.2/context> .

            <http://example.org/> a schema:Dataset ;
                schema:name "Test Dataset" ;
                schema:description "A test dataset" ;
                schema:datePublished "2024-01-01" ;
                schema:license "MIT" ;
                schema:author <http://example.org/person/alice> ;
                schema:hasPart <http://example.org/data.csv> .

            <http://example.org/person/alice> a schema:Person ;
                schema:name "Alice Smith" ;
                schema:email "alice@example.org" .

            <http://example.org/data.csv> a schema:MediaObject ;
                schema:name "Data File" ;
                schema:encodingFormat "text/csv" .
        "#;

        let result = rdf_to_rocrate(turtle, RdfFormat::Turtle, Some("http://example.org/"));
        assert!(result.is_ok());

        let crate_obj = result.unwrap();

        // Check that metadata ID was compacted to local filename
        let metadata = crate_obj.graph.iter().find_map(|g| {
            if let GraphVector::MetadataDescriptor(md) = g {
                Some(md)
            } else {
                None
            }
        });
        assert!(metadata.is_some());
        assert_eq!(metadata.unwrap().id, "ro-crate-metadata.json");

        // Check that root ID was compacted to ./
        let root = crate_obj.graph.iter().find_map(|g| {
            if let GraphVector::RootDataEntity(root) = g {
                Some(root)
            } else {
                None
            }
        });
        assert!(root.is_some());
        assert_eq!(root.unwrap().id, "./");

        // Check that data entity ID was compacted
        let data_entity = crate_obj.graph.iter().find_map(|g| {
            if let GraphVector::DataEntity(data) = g {
                if data.id.contains("data.csv") {
                    Some(data)
                } else {
                    None
                }
            } else {
                None
            }
        });
        assert!(data_entity.is_some());
        assert_eq!(data_entity.unwrap().id, "./data.csv");

        // Check that contextual entity ID was compacted
        let contextual = crate_obj.graph.iter().find_map(|g| {
            if let GraphVector::ContextualEntity(ctx) = g {
                if ctx.id.contains("alice") {
                    Some(ctx)
                } else {
                    None
                }
            } else {
                None
            }
        });
        assert!(contextual.is_some());
        assert_eq!(contextual.unwrap().id, "./person/alice");

        // Check that references in properties were compacted
        let root = crate_obj.graph.iter().find_map(|g| {
            if let GraphVector::RootDataEntity(root) = g {
                Some(root)
            } else {
                None
            }
        });
        if let Some(root) = root {
            if let Some(props) = &root.dynamic_entity {
                // Check author reference
                if let Some(EntityValue::EntityId(Id::Id(author_id))) = props.get("author") {
                    assert_eq!(author_id, "./person/alice");
                }
                // Check hasPart reference
                if let Some(EntityValue::EntityId(Id::Id(part_id))) = props.get("hasPart") {
                    assert_eq!(part_id, "./data.csv");
                }
            }
        }
    }

    #[test]
    fn test_roundtrip_rocrate_to_rdf_to_rocrate() {
        use crate::ro_crate::rdf::convert::{rocrate_to_rdf_with_options, ConversionOptions};
        use crate::ro_crate::rdf::resolver::ContextResolverBuilder;

        // Create a minimal RO-Crate
        let metadata = MetadataDescriptor {
            id: "ro-crate-metadata.json".to_string(),
            type_: DataType::Term("CreativeWork".to_string()),
            conforms_to: Id::Id("https://w3id.org/ro/crate/1.2/context".to_string()),
            about: Id::Id("./".to_string()),
            dynamic_entity: Some(HashMap::new()),
        };

        let root = RootDataEntity {
            id: "./".to_string(),
            type_: DataType::Term("Dataset".to_string()),
            name: "Test Dataset".to_string(),
            description: "A test dataset for round-trip".to_string(),
            date_published: "2024-01-15".to_string(),
            license: License::Description("MIT".to_string()),
            dynamic_entity: Some(HashMap::new()),
        };

        let original_crate = RoCrate {
            context: RoCrateContext::ReferenceContext(
                "https://w3id.org/ro/crate/1.2/context".to_string(),
            ),
            graph: vec![
                GraphVector::MetadataDescriptor(metadata),
                GraphVector::RootDataEntity(root),
            ],
        };

        // Convert to RDF with a base IRI
        let resolver = ContextResolverBuilder::default();
        let rdf_graph = rocrate_to_rdf_with_options(
            &original_crate,
            resolver,
            ConversionOptions::WithBase("http://example.org/".to_string()),
        )
        .expect("RO-Crate to RDF conversion failed");

        // Serialize to Turtle
        let turtle = rdf_graph
            .to_string(RdfFormat::Turtle)
            .expect("RDF serialization failed");

        // Convert back to RO-Crate
        let restored_crate =
            rdf_to_rocrate(&turtle, RdfFormat::Turtle, Some("http://example.org/"))
                .expect("RDF to RO-Crate conversion failed");

        // Verify structure is preserved
        assert_eq!(restored_crate.graph.len(), 2);

        // Check metadata descriptor
        let restored_metadata = restored_crate.graph.iter().find_map(|g| {
            if let GraphVector::MetadataDescriptor(md) = g {
                Some(md)
            } else {
                None
            }
        });
        assert!(restored_metadata.is_some());
        let restored_metadata = restored_metadata.unwrap();
        assert_eq!(restored_metadata.id, "ro-crate-metadata.json");

        // Check root entity
        let restored_root = restored_crate.graph.iter().find_map(|g| {
            if let GraphVector::RootDataEntity(root) = g {
                Some(root)
            } else {
                None
            }
        });
        assert!(restored_root.is_some());
        let restored_root = restored_root.unwrap();
        assert_eq!(restored_root.id, "./");
        assert_eq!(restored_root.name, "Test Dataset");
        assert_eq!(restored_root.description, "A test dataset for round-trip");
        assert_eq!(restored_root.date_published, "2024-01-15");
    }

    #[test]
    fn test_rdf_graph_to_rocrate_uses_context() {
        use crate::ro_crate::rdf::convert::{rocrate_to_rdf_with_options, ConversionOptions};
        use crate::ro_crate::rdf::rdf_graph_to_rocrate;
        use crate::ro_crate::rdf::resolver::ContextResolverBuilder;

        // Create a minimal RO-Crate with schema.org terms
        let metadata = MetadataDescriptor {
            id: "ro-crate-metadata.json".to_string(),
            type_: DataType::Term("CreativeWork".to_string()),
            conforms_to: Id::Id("https://w3id.org/ro/crate/1.2/context".to_string()),
            about: Id::Id("./".to_string()),
            dynamic_entity: Some(HashMap::new()),
        };

        let mut root_props = HashMap::new();
        root_props.insert(
            "author".to_string(),
            EntityValue::EntityId(Id::Id("./person/alice".to_string())),
        );

        let root = RootDataEntity {
            id: "./".to_string(),
            type_: DataType::Term("Dataset".to_string()),
            name: "Test Dataset".to_string(),
            description: "Testing context preservation in rdf_graph_to_rocrate".to_string(),
            date_published: "2024-01-20".to_string(),
            license: License::Description("MIT".to_string()),
            dynamic_entity: Some(root_props),
        };

        let original_crate = RoCrate {
            context: RoCrateContext::ReferenceContext(
                "https://w3id.org/ro/crate/1.2/context".to_string(),
            ),
            graph: vec![
                GraphVector::MetadataDescriptor(metadata),
                GraphVector::RootDataEntity(root),
            ],
        };

        let resolver = ContextResolverBuilder::default();
        let rdf_graph = rocrate_to_rdf_with_options(
            &original_crate,
            resolver,
            ConversionOptions::WithBase("http://example.org/".to_string()),
        )
        .expect("RO-Crate to RDF conversion failed");

        // Verify graph has triples
        assert!(!rdf_graph.is_empty(), "RDF graph should not be empty");

        // Verify context is stored in graph
        assert!(
            !rdf_graph.context.terms.is_empty() || !rdf_graph.context.prefixes.is_empty(),
            "RDF graph should preserve context"
        );

        let restored_crate =
            rdf_graph_to_rocrate(rdf_graph).expect("RDF graph to RO-Crate conversion failed");

        // Verify context was used for compaction
        let root = restored_crate.graph.iter().find_map(|g| {
            if let GraphVector::RootDataEntity(root) = g {
                Some(root)
            } else {
                None
            }
        });
        assert!(root.is_some(), "Root entity should exist");
        let root = root.unwrap();

        // Verify compacted properties (schema.org terms compacted to short names)
        assert_eq!(root.name, "Test Dataset", "Name should be preserved");
        assert_eq!(
            root.description, "Testing context preservation in rdf_graph_to_rocrate",
            "Description should be preserved"
        );

        // Verify author reference is compacted with "./" prefix
        if let Some(props) = &root.dynamic_entity {
            if let Some(EntityValue::EntityId(Id::Id(author_id))) = props.get("author") {
                // Should be compacted to "./person/alice" not full IRI
                assert_eq!(
                    author_id, "./person/alice",
                    "Author reference should use compacted IRI with ./ prefix"
                );
            }
        }

        // Verify metadata descriptor
        let metadata = restored_crate.graph.iter().find_map(|g| {
            if let GraphVector::MetadataDescriptor(md) = g {
                Some(md)
            } else {
                None
            }
        });
        assert!(metadata.is_some(), "Metadata descriptor should exist");
        let metadata = metadata.unwrap();
        assert_eq!(
            metadata.id, "ro-crate-metadata.json",
            "Metadata ID should be compacted"
        );

        assert!(
            !restored_crate.graph.is_empty(),
            "Restored crate should have entities"
        );
    }

    #[test]
    fn test_all_compaction_via_context() {
        let turtle = r#"
            @prefix schema: <http://schema.org/> .

            <http://example.org/ro-crate-metadata.json> a schema:CreativeWork ;
                schema:about <http://example.org/> ;
                schema:conformsTo <https://w3id.org/ro/crate/1.2/context> .

            <http://example.org/> a schema:Dataset ;
                schema:name "Test Dataset" ;
                schema:description "Testing compaction" .
        "#;

        let result = rdf_to_rocrate(turtle, RdfFormat::Turtle, Some("http://example.org/"));
        assert!(result.is_ok(), "RDF parsing should succeed");

        let crate_obj = result.unwrap();

        // Verify predicates are compacted (schema:name → name)
        let root = crate_obj.graph.iter().find_map(|g| {
            if let GraphVector::RootDataEntity(root) = g {
                Some(root)
            } else {
                None
            }
        });
        assert!(root.is_some(), "Root entity should exist");
        let root = root.unwrap();
        assert_eq!(
            root.name, "Test Dataset",
            "Predicate 'schema:name' should be compacted to 'name'"
        );
        assert_eq!(
            root.description, "Testing compaction",
            "Predicate 'schema:description' should be compacted to 'description'"
        );

        // Verify types are compacted (schema:Dataset → Dataset)
        assert_eq!(
            root.type_,
            DataType::Term("Dataset".to_string()),
            "Type 'http://schema.org/Dataset' should be compacted to 'Dataset'"
        );

        // Verify entity IDs are compacted (http://example.org/ → ./)
        assert_eq!(
            root.id, "./",
            "Entity ID should be compacted to base-relative with './' prefix"
        );
    }

    #[test]
    fn test_schema_org_compaction() {
        let turtle = r#"
            @prefix schema: <http://schema.org/> .

            <http://example.org/ro-crate-metadata.json> a schema:CreativeWork ;
                schema:about <http://example.org/> ;
                schema:conformsTo <https://w3id.org/ro/crate/1.2/context> .

            <http://example.org/> a schema:Dataset ;
                schema:name "Dataset Name" ;
                schema:description "Dataset Description" ;
                schema:datePublished "2024-01-01" ;
                schema:author <http://example.org/person/alice> ;
                schema:license "MIT" .

            <http://example.org/person/alice> a schema:Person ;
                schema:name "Alice Smith" ;
                schema:email "alice@example.org" ;
                schema:affiliation <http://example.org/org/university> .

            <http://example.org/org/university> a schema:Organization ;
                schema:name "University" .
        "#;

        let result = rdf_to_rocrate(turtle, RdfFormat::Turtle, Some("http://example.org/"));
        assert!(result.is_ok(), "RDF parsing should succeed");

        let crate_obj = result.unwrap();

        // Check that all schema.org predicates are compacted
        let person = crate_obj.graph.iter().find_map(|g| {
            if let GraphVector::ContextualEntity(ctx) = g {
                if ctx.id.contains("alice") {
                    Some(ctx)
                } else {
                    None
                }
            } else {
                None
            }
        });
        assert!(person.is_some(), "Person entity should exist");
        let person = person.unwrap();

        // Verify type is compacted: http://schema.org/Person → Person
        assert_eq!(
            person.type_,
            DataType::Term("Person".to_string()),
            "Type 'http://schema.org/Person' should compact to 'Person'"
        );

        // Verify predicates are compacted
        if let Some(props) = &person.dynamic_entity {
            // schema:name → name
            assert!(
                props.contains_key("name"),
                "Property 'schema:name' should be compacted to 'name'"
            );
            // schema:email → email
            assert!(
                props.contains_key("email"),
                "Property 'schema:email' should be compacted to 'email'"
            );
            // schema:affiliation → affiliation
            assert!(
                props.contains_key("affiliation"),
                "Property 'schema:affiliation' should be compacted to 'affiliation'"
            );
        }

        // Check Organization type compaction
        let org = crate_obj.graph.iter().find_map(|g| {
            if let GraphVector::ContextualEntity(ctx) = g {
                if ctx.id.contains("university") {
                    Some(ctx)
                } else {
                    None
                }
            } else {
                None
            }
        });
        assert!(org.is_some(), "Organization entity should exist");
        let org = org.unwrap();
        assert_eq!(
            org.type_,
            DataType::Term("Organization".to_string()),
            "Type 'http://schema.org/Organization' should compact to 'Organization'"
        );
    }

    #[test]
    fn test_entity_id_relative_prefix() {
        // Test that entity IDs are properly compacted with "./" prefix
        // Per RO-Crate spec: only File (MediaObject) or Dataset types are Data Entities
        let turtle = r#"
            @prefix schema: <http://schema.org/> .

            <http://example.org/ro-crate-metadata.json> a schema:CreativeWork ;
                schema:about <http://example.org/> ;
                schema:conformsTo <https://w3id.org/ro/crate/1.2/context> .

            <http://example.org/> a schema:Dataset ;
                schema:name "Dataset" ;
                schema:hasPart <http://example.org/data.csv>,
                               <http://example.org/subdir/file.txt>,
                               <http://example.org/images/photo.jpg> .

            <http://example.org/data.csv> a schema:MediaObject ;
                schema:name "Data File" .

            <http://example.org/subdir/file.txt> a schema:MediaObject ;
                schema:name "Text File" .

            <http://example.org/images/photo.jpg> a schema:MediaObject ;
                schema:name "Photo" .
        "#;

        let result = rdf_to_rocrate(turtle, RdfFormat::Turtle, Some("http://example.org/"));
        assert!(result.is_ok(), "RDF parsing should succeed");

        let crate_obj = result.unwrap();

        // Check that all data entity IDs have "./" prefix (all are MediaObject = File)
        let data_entities: Vec<_> = crate_obj
            .graph
            .iter()
            .filter_map(|g| {
                if let GraphVector::DataEntity(data) = g {
                    Some(data.id.as_str())
                } else {
                    None
                }
            })
            .collect();

        assert!(
            data_entities.contains(&"./data.csv"),
            "Entity ID 'http://example.org/data.csv' should compact to './data.csv'"
        );
        assert!(
            data_entities.contains(&"./subdir/file.txt"),
            "Entity ID 'http://example.org/subdir/file.txt' should compact to './subdir/file.txt'"
        );
        assert!(data_entities.contains(&"./images/photo.jpg"),
                "Entity ID 'http://example.org/images/photo.jpg' should compact to './images/photo.jpg'");

        // Also check that references in properties have "./" prefix
        let root = crate_obj.graph.iter().find_map(|g| {
            if let GraphVector::RootDataEntity(root) = g {
                Some(root)
            } else {
                None
            }
        });
        assert!(root.is_some(), "Root entity should exist");

        if let Some(props) = &root.unwrap().dynamic_entity {
            if let Some(EntityValue::EntityVec(parts)) = props.get("hasPart") {
                let part_ids: Vec<String> = parts
                    .iter()
                    .filter_map(|v| {
                        if let EntityValue::EntityId(Id::Id(id)) = v {
                            Some(id.clone())
                        } else {
                            None
                        }
                    })
                    .collect();

                assert!(
                    part_ids.contains(&"./data.csv".to_string()),
                    "hasPart reference should have './' prefix"
                );
                assert!(
                    part_ids.contains(&"./subdir/file.txt".to_string()),
                    "hasPart reference should have './' prefix"
                );
                assert!(
                    part_ids.contains(&"./images/photo.jpg".to_string()),
                    "hasPart reference should have './' prefix"
                );
            }
        }
    }

    #[test]
    fn test_type_first_classification() {
        // Per RO-Crate 1.2 spec: only File or Dataset types determine Data Entity classification
        // File is an alias for http://schema.org/MediaObject in RO-Crate context

        // Test 1: File type (compact) → DataEntity
        assert_eq!(
            classify_entity("http://example.org/item", &["File".to_string()]),
            EntityType::Data,
            "Entity with 'File' type should be classified as DataEntity"
        );

        // Test 2: Dataset type (compact) → DataEntity
        assert_eq!(
            classify_entity("http://example.org/collection", &["Dataset".to_string()]),
            EntityType::Data,
            "Entity with 'Dataset' type should be classified as DataEntity"
        );

        // Test 3: Expanded File IRI (http://schema.org/MediaObject) → DataEntity
        assert_eq!(
            classify_entity(
                "http://example.org/resource",
                &["http://schema.org/MediaObject".to_string()]
            ),
            EntityType::Data,
            "Entity with expanded MediaObject IRI should be classified as DataEntity"
        );

        // Test 4: Expanded Dataset IRI → DataEntity
        assert_eq!(
            classify_entity(
                "http://example.org/dataset",
                &["http://schema.org/Dataset".to_string()]
            ),
            EntityType::Data,
            "Entity with expanded Dataset IRI should be classified as DataEntity"
        );

        // Test 5: MediaObject (vocab-stripped or plain) → DataEntity
        assert_eq!(
            classify_entity("http://example.org/file", &["MediaObject".to_string()]),
            EntityType::Data,
            "Entity with 'MediaObject' type should be classified as DataEntity (same as File)"
        );

        // Test 6: Prefixed form schema:MediaObject → DataEntity
        assert_eq!(
            classify_entity(
                "http://example.org/file",
                &["schema:MediaObject".to_string()]
            ),
            EntityType::Data,
            "Entity with 'schema:MediaObject' type should be classified as DataEntity"
        );

        // Test 7: File with # prefix @id → ContextualEntity (spec exception)
        assert_eq!(
            classify_entity("#myfile", &["File".to_string()]),
            EntityType::Contextual,
            "File with local identifier (#) should be ContextualEntity"
        );

        // Test 8: Other schema.org types (ImageObject, Person, etc.) are NOT Data Entities
        // These would need File or Dataset type to be Data Entities
        assert_eq!(
            classify_entity("http://example.org/picture", &["ImageObject".to_string()]),
            EntityType::Contextual,
            "ImageObject alone is not a DataEntity type per spec (needs File or Dataset)"
        );

        assert_eq!(
            classify_entity(
                "http://orcid.org/0000-0001-2345-6789",
                &["Person".to_string()]
            ),
            EntityType::Contextual,
            "Person type should be ContextualEntity"
        );

        assert_eq!(
            classify_entity("http://example.org/org", &["Organization".to_string()]),
            EntityType::Contextual,
            "Organization type should be ContextualEntity"
        );

        // Test 9: No type → ContextualEntity (no heuristics based on @id)
        assert_eq!(
            classify_entity("./data.csv", &[]),
            EntityType::Contextual,
            "Without File/Dataset type, entity is Contextual regardless of @id"
        );

        // Test 10: Unknown type → ContextualEntity
        assert_eq!(
            classify_entity("http://example.org/something", &["CustomType".to_string()]),
            EntityType::Contextual,
            "Unknown type should be ContextualEntity"
        );

        // Test 11: Multiple types including File → DataEntity
        assert_eq!(
            classify_entity(
                "./photo.jpg",
                &["File".to_string(), "ImageObject".to_string()]
            ),
            EntityType::Data,
            "Entity with both File and ImageObject should be DataEntity (File takes precedence)"
        );
    }

    #[test]
    fn test_unknown_iri_handling() {
        // Use a custom namespace that's not in RO-Crate context
        let turtle = r#"
            @prefix schema: <http://schema.org/> .
            @prefix custom: <http://custom.example.org/vocab/> .

            <http://example.org/ro-crate-metadata.json> a schema:CreativeWork ;
                schema:about <http://example.org/> ;
                schema:conformsTo <https://w3id.org/ro/crate/1.2/context> .

            <http://example.org/> a schema:Dataset ;
                schema:name "Dataset" ;
                custom:customProperty "custom value" ;
                custom:relatedResource <http://custom.example.org/resource/123> .
        "#;

        let result = rdf_to_rocrate(turtle, RdfFormat::Turtle, Some("http://example.org/"));
        assert!(
            result.is_ok(),
            "RDF parsing should succeed even with unknown IRIs"
        );

        let crate_obj = result.unwrap();

        // Check that custom predicates are preserved as full IRIs
        let root = crate_obj.graph.iter().find_map(|g| {
            if let GraphVector::RootDataEntity(root) = g {
                Some(root)
            } else {
                None
            }
        });
        assert!(root.is_some(), "Root entity should exist");

        if let Some(props) = &root.unwrap().dynamic_entity {
            // Custom properties that can't be compacted should remain as full IRIs
            // Note: The actual behavior depends on how ResolvedContext handles unknown IRIs
            // It should either keep the full IRI or use the last segment
            let has_custom_prop = props
                .keys()
                .any(|k| k.contains("custom") || k.contains("http://custom.example.org"));
            assert!(
                has_custom_prop,
                "Unknown predicates should be preserved (either as full IRI or fallback)"
            );
        }
    }

    #[test]
    fn test_compaction_integration() {
        // Per RO-Crate 1.2 spec: only File (MediaObject) or Dataset types are Data Entities
        // All other types become ContextualEntity
        let turtle = r#"
            @prefix schema: <http://schema.org/> .

            <http://example.org/ro-crate-metadata.json> a schema:CreativeWork ;
                schema:about <http://example.org/> ;
                schema:conformsTo <https://w3id.org/ro/crate/1.2/context> .

            <http://example.org/> a schema:Dataset ;
                schema:name "Integration Test Dataset" ;
                schema:description "Testing compaction of RDF to RO-Crate" ;
                schema:datePublished "2024-12-23" ;
                schema:license "Apache-2.0" ;
                schema:author <http://example.org/person/bob> ;
                schema:hasPart <http://example.org/data/results.csv>,
                               <http://example.org/docs/readme.md> ;
                schema:mentions <http://example.org/software/tool> .

            <http://example.org/person/bob> a schema:Person ;
                schema:name "Bob Johnson" ;
                schema:email "bob@example.org" .

            <http://example.org/data/results.csv> a schema:MediaObject ;
                schema:name "Results" ;
                schema:encodingFormat "text/csv" .

            <http://example.org/docs/readme.md> a schema:MediaObject ;
                schema:name "README" .

            <http://example.org/software/tool> a schema:SoftwareSourceCode ;
                schema:name "Analysis Tool" ;
                schema:programmingLanguage "Python" .
        "#;

        let result = rdf_to_rocrate(turtle, RdfFormat::Turtle, Some("http://example.org/"));
        assert!(result.is_ok(), "Integration test should parse successfully");

        let crate_obj = result.unwrap();

        let root = crate_obj
            .graph
            .iter()
            .find_map(|g| {
                if let GraphVector::RootDataEntity(root) = g {
                    Some(root)
                } else {
                    None
                }
            })
            .expect("Root should exist");

        assert_eq!(
            root.type_,
            DataType::Term("Dataset".to_string()),
            "schema:Dataset should compact to 'Dataset'"
        );
        assert_eq!(
            root.name, "Integration Test Dataset",
            "schema:name should compact to 'name'"
        );

        assert_eq!(root.id, "./", "Root ID should be './'");

        // Person is ContextualEntity (not File/Dataset type)
        let person = crate_obj
            .graph
            .iter()
            .find_map(|g| {
                if let GraphVector::ContextualEntity(ctx) = g {
                    if ctx.id.contains("bob") {
                        Some(ctx)
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .expect("Person should exist");
        assert_eq!(
            person.id, "./person/bob",
            "Person ID should have './' prefix"
        );

        // CSV file is DataEntity (MediaObject = File)
        let csv_file = crate_obj
            .graph
            .iter()
            .find_map(|g| {
                if let GraphVector::DataEntity(data) = g {
                    if data.id.contains("results.csv") {
                        Some(data)
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .expect("CSV file should exist");
        assert_eq!(
            csv_file.id, "./data/results.csv",
            "Data file ID should have './' prefix"
        );

        assert_eq!(
            person.type_,
            DataType::Term("Person".to_string()),
            "Person type correctly identified"
        );

        match &csv_file.type_ {
            DataType::Term(t) => {
                assert!(
                    t == "MediaObject" || t == "File",
                    "Data file should have MediaObject or File type, got: {}",
                    t
                );
            }
            _ => panic!("Expected Term type for data file"),
        }

        // SoftwareSourceCode is NOT a Data Entity (not File or Dataset)
        // So it should be a ContextualEntity
        let software = crate_obj.graph.iter().find_map(|g| {
            if let GraphVector::ContextualEntity(ctx) = g {
                if ctx.id.contains("tool") {
                    Some(ctx)
                } else {
                    None
                }
            } else {
                None
            }
        });
        assert!(
            software.is_some(),
            "Software tool should exist as ContextualEntity"
        );
        assert_eq!(
            software.unwrap().type_,
            DataType::Term("SoftwareSourceCode".to_string()),
            "SoftwareSourceCode is ContextualEntity (not File/Dataset)"
        );

        // README with MediaObject type is DataEntity
        let readme = crate_obj
            .graph
            .iter()
            .find_map(|g| {
                if let GraphVector::DataEntity(data) = g {
                    if data.id.contains("readme") {
                        Some(data)
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .expect("README should exist as DataEntity (MediaObject type)");
        assert!(
            readme.id.ends_with(".md"),
            ".md file with MediaObject type classified as DataEntity"
        );
    }
}
