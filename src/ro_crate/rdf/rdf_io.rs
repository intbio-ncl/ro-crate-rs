//! RDF serialization and parsing support for RdfGraph.
//!
//! This module provides functionality to serialize RdfGraph instances to various RDF formats
//! and parse RDF data into RO-Crate structures using the oxrdfio library.

use std::collections::{HashMap, HashSet};
use std::io::Write;

use oxrdf::{NamedNode, NamedOrBlankNode, Quad, Term, Triple};
use oxrdfio::{RdfFormat as OxRdfFormat, RdfParser, RdfSerializer};

use super::error::RdfError;
use super::graph::RdfGraph;
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
/// This enum wraps oxrdfio's format types and provides a convenient interface
/// for specifying the desired output format when serializing RDF graphs.
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
        let mut serializer = RdfSerializer::from_format(format.to_oxrdf_format())
            .for_writer(writer);

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
    let quads: Result<Vec<Quad>, _> = parser
        .for_reader(reader)
        .collect();

    let quads = quads.map_err(|e| RdfError::ParseError(format!("Failed to parse RDF: {}", e)))?;

    // Convert Quads to Triples (ignoring graph information)
    let triples = quads.into_iter().map(|q| Triple {
        subject: q.subject,
        predicate: q.predicate,
        object: q.object,
    }).collect();

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
            "Could not find metadata file IRI containing 'ro-crate-metadata.json'".to_string()
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
            "Could not find root crate IRI via schema:about predicate".to_string()
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
        return Err(RdfError::MissingRootEntities(
            format!("Root IRI '{}' is not a schema:Dataset", root_iri)
        ));
    }

    Ok((metadata_iri, root_iri))
}

/// Entity classification for determining whether an entity is a DataEntity or ContextualEntity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EntityType {
    Data,
    Contextual,
}

/// Extract the local name from a URI.
///
/// For example: "http://schema.org/author" -> "author"
fn extract_local_name(uri: &str) -> String {
    if let Some(pos) = uri.rfind(&['/', '#'][..]) {
        uri[pos + 1..].to_string()
    } else {
        uri.to_string()
    }
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
        Term::NamedNode(node) => {
            EntityValue::EntityId(Id::Id(node.as_str().to_string()))
        }
        Term::BlankNode(node) => {
            EntityValue::EntityId(Id::Id(format!("_:{}", node.as_str())))
        }
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

/// Extract all properties for a subject IRI from triples.
///
/// This function collects all predicates and objects for a given subject,
/// grouping multiple values for the same predicate into arrays.
///
/// # Arguments
///
/// * `subject_iri` - The subject IRI to extract properties for
/// * `triples` - The RDF triples to search
///
/// # Returns
///
/// A HashMap mapping property names to EntityValues
fn extract_entity_properties(subject_iri: &str, triples: &[Triple]) -> HashMap<String, EntityValue> {
    let rdf_type = NamedNode::new_unchecked("http://www.w3.org/1999/02/22-rdf-syntax-ns#type");
    let mut properties: HashMap<String, Vec<EntityValue>> = HashMap::new();

    for triple in triples {
        if let NamedOrBlankNode::NamedNode(subject) = &triple.subject {
            if subject.as_str() == subject_iri {
                // Skip rdf:type as it's handled separately
                if triple.predicate == rdf_type {
                    continue;
                }

                let property_name = extract_local_name(triple.predicate.as_str());
                let value = term_to_entity_value(&triple.object);

                properties.entry(property_name)
                    .or_default()
                    .push(value);
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
/// This recursively searches through EntityValue structures to find all
/// IRI references (EntityId values).
fn extract_iris_from_entity_value(value: &EntityValue, iris: &mut HashSet<String>) {
    match value {
        EntityValue::EntityId(id) => {
            match id {
                Id::Id(iri) => {
                    // Skip blank nodes and special IRIs
                    if !iri.starts_with("_:") && !iri.starts_with("http://www.w3.org/")
                        && !iri.starts_with("http://schema.org/") {
                        iris.insert(iri.clone());
                    }
                }
                Id::IdArray(iris_vec) => {
                    for iri in iris_vec {
                        if !iri.starts_with("_:") && !iri.starts_with("http://www.w3.org/")
                            && !iri.starts_with("http://schema.org/") {
                            iris.insert(iri.clone());
                        }
                    }
                }
            }
        }
        EntityValue::EntityVec(vec) => {
            for v in vec {
                extract_iris_from_entity_value(v, iris);
            }
        }
        EntityValue::EntityObject(map) => {
            for v in map.values() {
                extract_iris_from_entity_value(v, iris);
            }
        }
        EntityValue::EntityVecObject(vec_map) => {
            for map in vec_map {
                for v in map.values() {
                    extract_iris_from_entity_value(v, iris);
                }
            }
        }
        EntityValue::NestedDynamicEntity(nested) => {
            extract_iris_from_entity_value(nested, iris);
        }
        _ => {}
    }
}

/// Walk the graph from root, collecting all reachable entity IRIs.
///
/// This performs a breadth-first traversal of the RDF graph starting from
/// the root entity, following all IRI references to build a complete set
/// of reachable entities.
///
/// # Arguments
///
/// * `root_iri` - The root entity IRI to start from
/// * `triples` - The RDF triples representing the graph
///
/// # Returns
///
/// A HashSet of all reachable entity IRIs
fn collect_reachable_entities(root_iri: &str, triples: &[Triple]) -> HashSet<String> {
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
        let properties = extract_entity_properties(&current_iri, triples);

        // Find all referenced IRIs
        let mut referenced_iris = HashSet::new();
        for value in properties.values() {
            extract_iris_from_entity_value(value, &mut referenced_iris);
        }

        // Add newly discovered IRIs to the processing queue
        for iri in referenced_iris {
            if !processed.contains(&iri) {
                to_process.push(iri);
            }
        }
    }

    reachable
}

/// Determine if an IRI should be a DataEntity or ContextualEntity.
///
/// Classification rules:
/// - DataEntity: IRI starts with './', is a relative file path, or looks like a file URL
/// - ContextualEntity: IRI starts with '#', is a blank node, or is an external URI
///
/// # Arguments
///
/// * `id` - The entity IRI to classify
///
/// # Returns
///
/// The EntityType (Data or Contextual)
fn classify_entity(id: &str) -> EntityType {
    // DataEntity patterns
    if id.starts_with("./") || id.starts_with("../") {
        return EntityType::Data;
    }

    // Check for file-like URLs
    if id.starts_with("http://") || id.starts_with("https://") {
        // If it has a file extension, treat as data
        if id.contains('.') {
            let last_segment = id.split('/').next_back().unwrap_or("");
            if last_segment.contains('.') && !last_segment.ends_with('/') {
                // Check for common file extensions
                let extensions = [".txt", ".csv", ".json", ".xml", ".html", ".pdf",
                                ".jpg", ".png", ".gif", ".zip", ".tar", ".gz"];
                for ext in &extensions {
                    if last_segment.ends_with(ext) {
                        return EntityType::Data;
                    }
                }
            }
        }
    }

    // ContextualEntity patterns (default)
    EntityType::Contextual
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
    // Find the last occurrence of "ro-crate-metadata.json"
    if let Some(pos) = metadata_iri.rfind("ro-crate-metadata.json") {
        Some(metadata_iri[..pos].to_string())
    } else {
        None
    }
}

/// Compact an IRI relative to a base IRI.
///
/// If the IRI starts with the base, strip the base and prefix with `./`.
/// If the IRI equals the base (no path after), return `./`.
/// Otherwise, return the IRI unchanged.
///
/// # Arguments
///
/// * `iri` - The IRI to compact
/// * `base` - The base IRI to compact relative to
///
/// # Returns
///
/// The compacted IRI
///
/// # Example
///
/// ```ignore
/// let compacted = compact_iri("http://example.org/data.csv", "http://example.org/");
/// assert_eq!(compacted, "./data.csv");
/// ```
fn compact_iri(iri: &str, base: &str) -> String {
    if iri.starts_with(base) {
        let remainder = &iri[base.len()..];
        if remainder.is_empty() || remainder == "/" {
            "./".to_string()
        } else {
            format!("./{}", remainder)
        }
    } else {
        // Cannot compact, return as-is
        iri.to_string()
    }
}

/// Compact all IRIs in an EntityValue recursively.
///
/// This modifies the EntityValue in-place, replacing absolute IRIs
/// with relative paths where possible.
///
/// # Arguments
///
/// * `value` - The EntityValue to compact
/// * `base` - The base IRI to compact relative to
fn compact_entity_value(value: &mut EntityValue, base: &str) {
    match value {
        EntityValue::EntityId(id) => {
            match id {
                Id::Id(iri) => {
                    // Skip blank nodes and schema.org/w3.org IRIs
                    if !iri.starts_with("_:") && !iri.starts_with("http://www.w3.org/")
                        && !iri.starts_with("http://schema.org/")
                        && !iri.starts_with("https://w3id.org/") {
                        let compacted = compact_iri(iri, base);
                        if compacted != *iri && !iri.starts_with(base) {
                            log::warn!("Cannot compact IRI '{}' - does not start with base '{}'", iri, base);
                        }
                        *iri = compacted;
                    }
                }
                Id::IdArray(iris_vec) => {
                    for iri in iris_vec.iter_mut() {
                        if !iri.starts_with("_:") && !iri.starts_with("http://www.w3.org/")
                            && !iri.starts_with("http://schema.org/")
                            && !iri.starts_with("https://w3id.org/") {
                            let compacted = compact_iri(iri, base);
                            if compacted != *iri && !iri.starts_with(base) {
                                log::warn!("Cannot compact IRI '{}' - does not start with base '{}'", iri, base);
                            }
                            *iri = compacted;
                        }
                    }
                }
            }
        }
        EntityValue::EntityVec(vec) => {
            for v in vec.iter_mut() {
                compact_entity_value(v, base);
            }
        }
        EntityValue::EntityObject(map) => {
            for v in map.values_mut() {
                compact_entity_value(v, base);
            }
        }
        EntityValue::EntityVecObject(vec_map) => {
            for map in vec_map.iter_mut() {
                for v in map.values_mut() {
                    compact_entity_value(v, base);
                }
            }
        }
        EntityValue::NestedDynamicEntity(nested) => {
            compact_entity_value(nested, base);
        }
        _ => {}
    }
}

/// Compact all IRIs in a HashMap of entity properties.
///
/// This modifies the properties in-place, compacting all IRI references.
///
/// # Arguments
///
/// * `properties` - The properties HashMap to compact
/// * `base` - The base IRI to compact relative to
fn compact_properties(properties: &mut HashMap<String, EntityValue>, base: &str) {
    for value in properties.values_mut() {
        compact_entity_value(value, base);
    }
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
/// * `base` - Optional base IRI for resolving relative IRIs
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
pub fn rdf_to_rocrate(input: &str, format: RdfFormat, base: Option<&str>) -> Result<RoCrate, RdfError> {
    let rdf_type = NamedNode::new_unchecked("http://www.w3.org/1999/02/22-rdf-syntax-ns#type");

    // Parse RDF into triples
    let triples = parse_rdf(input, format, base)?;

    // Find root entities
    let (metadata_iri, root_iri) = find_root_entities(&triples)?;

    // Helper function to extract type from triples
    let extract_type = |iri: &str| -> DataType {
        let mut types = Vec::new();
        for triple in &triples {
            if let NamedOrBlankNode::NamedNode(subject) = &triple.subject {
                if subject.as_str() == iri && triple.predicate == rdf_type {
                    if let Term::NamedNode(type_node) = &triple.object {
                        types.push(extract_local_name(type_node.as_str()));
                    }
                }
            }
        }

        if types.is_empty() {
            DataType::Term("Thing".to_string())
        } else if types.len() == 1 {
            DataType::Term(types[0].clone())
        } else {
            DataType::TermArray(types)
        }
    };

    // Extract properties for MetadataDescriptor
    let mut metadata_properties = extract_entity_properties(&metadata_iri, &triples);
    let metadata_type = extract_type(&metadata_iri);

    // Remove 'about' from dynamic_entity as it's a required field
    let about = metadata_properties.remove("about")
        .and_then(|v| match v {
            EntityValue::EntityId(id) => Some(id),
            _ => None,
        })
        .unwrap_or_else(|| Id::Id(root_iri.clone()));

    let metadata_descriptor = MetadataDescriptor {
        id: metadata_iri.clone(),
        type_: metadata_type,
        conforms_to: Id::Id("https://w3id.org/ro/crate/1.2/context".to_string()),
        about: about.clone(),
        dynamic_entity: Some(metadata_properties),
    };

    // Extract properties for RootDataEntity
    let mut root_properties = extract_entity_properties(&root_iri, &triples);
    let root_type = extract_type(&root_iri);

    // Extract required fields with fallbacks
    let name = root_properties.remove("name")
        .and_then(|v| match v {
            EntityValue::EntityString(s) => Some(s),
            _ => None,
        })
        .unwrap_or_else(|| "Unnamed Dataset".to_string());

    let description = root_properties.remove("description")
        .and_then(|v| match v {
            EntityValue::EntityString(s) => Some(s),
            _ => None,
        })
        .unwrap_or_else(|| "No description available".to_string());

    let date_published = root_properties.remove("datePublished")
        .and_then(|v| match v {
            EntityValue::EntityString(s) => Some(s),
            _ => None,
        })
        .unwrap_or_else(|| "1970-01-01".to_string());

    let license = root_properties.remove("license")
        .map(|v| match v {
            EntityValue::EntityId(id) => License::Id(id),
            EntityValue::EntityString(s) => License::Description(s),
            _ => License::Description("Unknown".to_string()),
        })
        .unwrap_or_else(|| License::Description("Unknown".to_string()));

    let root_entity = RootDataEntity {
        id: root_iri.clone(),
        type_: root_type,
        name,
        description,
        date_published,
        license,
        dynamic_entity: Some(root_properties),
    };

    // Walk the graph to collect reachable entities
    let reachable_iris = collect_reachable_entities(&root_iri, &triples);

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
            log::warn!("Entity '{}' is not reachable from root and will be excluded", iri);
        }
    }

    // Infer or use provided base IRI for compaction
    let compaction_base = base
        .map(|b| b.to_string())
        .or_else(|| infer_base_from_metadata(&metadata_iri));

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

        let entity_type = extract_type(&iri);
        let properties = extract_entity_properties(&iri, &triples);

        // Classify and create appropriate entity
        match classify_entity(&iri) {
            EntityType::Data => {
                let data_entity = DataEntity {
                    id: iri,
                    type_: entity_type,
                    dynamic_entity: Some(properties),
                };
                graph.push(GraphVector::DataEntity(data_entity));
            }
            EntityType::Contextual => {
                let contextual_entity = ContextualEntity {
                    id: iri,
                    type_: entity_type,
                    dynamic_entity: Some(properties),
                };
                graph.push(GraphVector::ContextualEntity(contextual_entity));
            }
        }
    }

    // Apply IRI compaction if we have a base
    if let Some(base_iri) = compaction_base {
        for entity in &mut graph {
            match entity {
                GraphVector::MetadataDescriptor(ref mut md) => {
                    // Compact metadata ID to local filename
                    md.id = "ro-crate-metadata.json".to_string();

                    // Compact about reference
                    match &mut md.about {
                        Id::Id(iri) => {
                            *iri = compact_iri(iri, &base_iri);
                        }
                        Id::IdArray(iris) => {
                            for iri in iris.iter_mut() {
                                *iri = compact_iri(iri, &base_iri);
                            }
                        }
                    }

                    // Compact properties
                    if let Some(ref mut props) = md.dynamic_entity {
                        compact_properties(props, &base_iri);
                    }
                }
                GraphVector::RootDataEntity(ref mut root) => {
                    // Compact root ID to ./
                    root.id = compact_iri(&root.id, &base_iri);

                    // Compact properties
                    if let Some(ref mut props) = root.dynamic_entity {
                        compact_properties(props, &base_iri);
                    }
                }
                GraphVector::DataEntity(ref mut data) => {
                    // Compact data entity ID
                    data.id = compact_iri(&data.id, &base_iri);

                    // Compact properties
                    if let Some(ref mut props) = data.dynamic_entity {
                        compact_properties(props, &base_iri);
                    }
                }
                GraphVector::ContextualEntity(ref mut ctx) => {
                    // Compact contextual entity ID
                    ctx.id = compact_iri(&ctx.id, &base_iri);

                    // Compact properties
                    if let Some(ref mut props) = ctx.dynamic_entity {
                        compact_properties(props, &base_iri);
                    }
                }
            }
        }
    }

    // Create RoCrate with RO-Crate 1.2 default context
    let context = RoCrateContext::ReferenceContext(
        "https://w3id.org/ro/crate/1.2/context".to_string()
    );

    Ok(RoCrate { context, graph })
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
        assert_eq!(
            RdfFormat::Turtle.to_oxrdf_format(),
            OxRdfFormat::Turtle
        );
        assert_eq!(
            RdfFormat::NTriples.to_oxrdf_format(),
            OxRdfFormat::NTriples
        );
        assert_eq!(
            RdfFormat::NQuads.to_oxrdf_format(),
            OxRdfFormat::NQuads
        );
        assert_eq!(
            RdfFormat::RdfXml.to_oxrdf_format(),
            OxRdfFormat::RdfXml
        );
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
        let has_metadata = crate_obj.graph.iter().any(|g| matches!(g, GraphVector::MetadataDescriptor(_)));
        let has_root = crate_obj.graph.iter().any(|g| matches!(g, GraphVector::RootDataEntity(_)));
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
    fn test_extract_local_name() {
        assert_eq!(extract_local_name("http://schema.org/author"), "author");
        assert_eq!(extract_local_name("http://schema.org/name"), "name");
        assert_eq!(extract_local_name("http://example.org#id"), "id");
        assert_eq!(extract_local_name("plain"), "plain");
    }

    #[test]
    fn test_classify_entity() {
        // DataEntity patterns
        assert_eq!(classify_entity("./data.txt"), EntityType::Data);
        assert_eq!(classify_entity("../file.csv"), EntityType::Data);
        assert_eq!(classify_entity("http://example.org/data.json"), EntityType::Data);
        assert_eq!(classify_entity("https://example.org/file.pdf"), EntityType::Data);

        // ContextualEntity patterns
        assert_eq!(classify_entity("#person1"), EntityType::Contextual);
        assert_eq!(classify_entity("http://orcid.org/0000-0001-2345-6789"), EntityType::Contextual);
        assert_eq!(classify_entity("https://schema.org/Person"), EntityType::Contextual);
    }

    #[test]
    fn test_extract_entity_properties() {
        let turtle = r#"
            @prefix schema: <http://schema.org/> .

            <http://example.org/person> schema:name "Alice" ;
                schema:email "alice@example.org" ;
                schema:age "30" .
        "#;

        let triples = parse_rdf(turtle, RdfFormat::Turtle, None).unwrap();
        let properties = extract_entity_properties("http://example.org/person", &triples);

        assert!(properties.contains_key("name"));
        assert!(properties.contains_key("email"));
        assert!(properties.contains_key("age"));

        match properties.get("name") {
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
        let properties = extract_entity_properties("http://example.org/dataset", &triples);

        assert!(properties.contains_key("author"));

        match properties.get("author") {
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
        let reachable = collect_reachable_entities("http://example.org/", &triples);

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
        let has_metadata = crate_obj.graph.iter().any(|g| matches!(g, GraphVector::MetadataDescriptor(_)));
        assert!(has_metadata);

        // Check root entity
        let has_root = crate_obj.graph.iter().any(|g| matches!(g, GraphVector::RootDataEntity(_)));
        assert!(has_root);

        // Check data entity
        let has_data = crate_obj.graph.iter().any(|g| matches!(g, GraphVector::DataEntity(_)));
        assert!(has_data);

        // Check contextual entity
        let has_contextual = crate_obj.graph.iter().any(|g| matches!(g, GraphVector::ContextualEntity(_)));
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

        // Missing filename
        let base = infer_base_from_metadata("http://example.org/");
        assert_eq!(base, None);

        // Just filename
        let base = infer_base_from_metadata("ro-crate-metadata.json");
        assert_eq!(base, Some("".to_string()));
    }

    #[test]
    fn test_compact_iri() {
        let base = "http://example.org/";

        // Standard compaction
        assert_eq!(compact_iri("http://example.org/data.csv", base), "./data.csv");
        assert_eq!(compact_iri("http://example.org/path/to/file.txt", base), "./path/to/file.txt");

        // Base with trailing slash
        assert_eq!(compact_iri("http://example.org/", base), "./");

        // Base without path
        let base2 = "http://example.org/subdir/";
        assert_eq!(compact_iri("http://example.org/subdir/", base2), "./");
        assert_eq!(compact_iri("http://example.org/subdir/file.txt", base2), "./file.txt");

        // Non-compactable IRI (different domain)
        assert_eq!(
            compact_iri("http://other.org/file.txt", base),
            "http://other.org/file.txt"
        );

        // Non-compactable IRI (different path)
        assert_eq!(
            compact_iri("http://example.org/other/file.txt", "http://example.org/base/"),
            "http://example.org/other/file.txt"
        );
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
                "https://w3id.org/ro/crate/1.2/context".to_string()
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
            ConversionOptions::WithBase("http://example.org/".to_string())
        ).expect("RO-Crate to RDF conversion failed");

        // Serialize to Turtle
        let turtle = rdf_graph.to_string(RdfFormat::Turtle)
            .expect("RDF serialization failed");

        // Convert back to RO-Crate
        let restored_crate = rdf_to_rocrate(&turtle, RdfFormat::Turtle, Some("http://example.org/"))
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
}
