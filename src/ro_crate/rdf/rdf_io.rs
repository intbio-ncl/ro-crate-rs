//! RDF serialization and parsing for `RdfGraph` using oxrdfio.

use std::collections::{HashMap, HashSet};
use std::io::Write;

#[cfg(test)]
use oxrdf::NamedNode;
use oxrdf::{NamedOrBlankNode, Term, Triple};
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

const RDF_TYPE_IRI: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
const SCHEMA_ABOUT_IRI: &str = "http://schema.org/about";
const SCHEMA_DATASET_IRI: &str = "http://schema.org/Dataset";
const ROCRATE_CONTEXT_IRI: &str = "https://w3id.org/ro/crate/1.2/context";

const XSD_STRING: &str = "http://www.w3.org/2001/XMLSchema#string";
const XSD_BOOLEAN: &str = "http://www.w3.org/2001/XMLSchema#boolean";
const XSD_INTEGER: &str = "http://www.w3.org/2001/XMLSchema#integer";
const XSD_INT: &str = "http://www.w3.org/2001/XMLSchema#int";
const XSD_LONG: &str = "http://www.w3.org/2001/XMLSchema#long";
const XSD_SHORT: &str = "http://www.w3.org/2001/XMLSchema#short";
const XSD_BYTE: &str = "http://www.w3.org/2001/XMLSchema#byte";
const XSD_NON_NEGATIVE_INTEGER: &str = "http://www.w3.org/2001/XMLSchema#nonNegativeInteger";
const XSD_NON_POSITIVE_INTEGER: &str = "http://www.w3.org/2001/XMLSchema#nonPositiveInteger";
const XSD_POSITIVE_INTEGER: &str = "http://www.w3.org/2001/XMLSchema#positiveInteger";
const XSD_NEGATIVE_INTEGER: &str = "http://www.w3.org/2001/XMLSchema#negativeInteger";
const XSD_UNSIGNED_LONG: &str = "http://www.w3.org/2001/XMLSchema#unsignedLong";
const XSD_UNSIGNED_INT: &str = "http://www.w3.org/2001/XMLSchema#unsignedInt";
const XSD_UNSIGNED_SHORT: &str = "http://www.w3.org/2001/XMLSchema#unsignedShort";
const XSD_UNSIGNED_BYTE: &str = "http://www.w3.org/2001/XMLSchema#unsignedByte";
const XSD_DOUBLE: &str = "http://www.w3.org/2001/XMLSchema#double";
const XSD_FLOAT: &str = "http://www.w3.org/2001/XMLSchema#float";
const XSD_DECIMAL: &str = "http://www.w3.org/2001/XMLSchema#decimal";

/// Supported RDF serialization formats for RO-Crate I/O.
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
    /// Writes the RDF graph to `writer` in the chosen `format`.
    /// Returns `RdfError::Serialization` on write/flush failure.
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

    /// Serializes the RDF graph to a UTF-8 string in the chosen `format`.
    /// Returns `RdfError::Serialization` on serialization or UTF-8 failure.
    pub fn to_string(&self, format: RdfFormat) -> Result<String, RdfError> {
        let mut buffer = Vec::new();
        self.write(&mut buffer, format)?;

        String::from_utf8(buffer)
            .map_err(|e| RdfError::Serialization(format!("Invalid UTF-8 in output: {}", e)))
    }
}

/// Parse RDF triples from input string.
/// Returns `RdfError::ParseError` if parsing fails.
fn parse_rdf(input: &str, format: RdfFormat, base: Option<&str>) -> Result<Vec<Triple>, RdfError> {
    let mut parser = RdfParser::from_format(format.to_oxrdf_format());

    if let Some(base_iri) = base {
        parser = parser
            .with_base_iri(base_iri)
            .map_err(|e| RdfError::ParseError(format!("Invalid base IRI: {}", e)))?;
    }

    parser
        .for_slice(input.as_bytes())
        .map(|quad| {
            quad.map(Triple::from)
                .map_err(|e| RdfError::ParseError(format!("Failed to parse RDF: {}", e)))
        })
        .collect()
}

/// Find the metadata descriptor and root data entity IRIs from RDF triples.
///
/// Implements the RO-Crate 1.2 spec SPARQL query pattern:
/// ```sparql
/// PREFIX schema: <http://schema.org/>
/// SELECT ?crate ?metadatafile
/// WHERE {
///   ?crate        a                  schema:Dataset .
///   ?metadatafile schema:about       ?crate .
///   filter(contains(str(?metadatafile), "ro-crate-metadata.json"))
/// }
/// ```
///
/// # Arguments
///
/// * `triples` - The parsed RDF triples
///
/// # Errors
///
/// Returns `RdfError::MissingRootEntities` if required entities are not found.
#[cfg(test)]
fn find_root_entities(triples: &[Triple]) -> Result<(String, String), RdfError> {
    let schema_about = NamedNode::new_unchecked("http://schema.org/about");
    let rdf_type = NamedNode::new_unchecked("http://www.w3.org/1999/02/22-rdf-syntax-ns#type");
    let schema_dataset = NamedNode::new_unchecked("http://schema.org/Dataset");

    // Step 1: Find all (?metadatafile, ?crate) pairs where:
    //   - ?metadatafile contains "ro-crate-metadata.json"
    //   - ?metadatafile schema:about ?crate
    let mut candidates: Vec<(String, String)> = Vec::new();
    for triple in triples {
        if triple.predicate == schema_about {
            if let NamedOrBlankNode::NamedNode(subject) = &triple.subject {
                let metadatafile = subject.as_str();
                if metadatafile.contains("ro-crate-metadata.json") {
                    if let Term::NamedNode(object) = &triple.object {
                        candidates.push((metadatafile.to_string(), object.as_str().to_string()));
                    }
                }
            }
        }
    }

    if candidates.is_empty() {
        return Err(RdfError::MissingRootEntities(
            "Could not find metadata file with schema:about referencing a crate".to_string(),
        ));
    }

    // Step 2: Filter candidates where ?crate is a schema:Dataset
    let mut result: Option<(String, String)> = None;
    for (metadatafile, crate_iri) in &candidates {
        for triple in triples {
            if let NamedOrBlankNode::NamedNode(subject) = &triple.subject {
                if subject.as_str() == crate_iri
                    && triple.predicate == rdf_type
                    && triple.object == Term::NamedNode(schema_dataset.clone())
                {
                    result = Some((metadatafile.clone(), crate_iri.clone()));
                    break;
                }
            }
        }
        if result.is_some() {
            break;
        }
    }

    result.ok_or_else(|| {
        RdfError::MissingRootEntities(
            "Could not find root crate (schema:Dataset) referenced by metadata file".to_string(),
        )
    })
}

/// Entity classification for determining whether an entity is a DataEntity or ContextualEntity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EntityType {
    Data,
    Contextual,
}

#[derive(Debug, Default)]
struct IndexedEntity {
    properties: HashMap<String, Vec<EntityValue>>,
    outgoing_refs: Vec<String>,
    type_iris: Vec<String>,
}

#[derive(Debug, Default)]
struct IndexedGraph {
    entities: HashMap<String, IndexedEntity>,
    metadata_about_candidates: Vec<(String, String)>,
}

impl IndexedGraph {
    fn from_triples<I>(triples: I) -> Self
    where
        I: IntoIterator<Item = Triple>,
    {
        let mut graph = Self::default();

        for triple in triples {
            let subject = subject_to_string(&triple.subject);
            let predicate = triple.predicate.as_str();
            let entity = graph.entities.entry(subject.clone()).or_default();

            if predicate == RDF_TYPE_IRI {
                if let Term::NamedNode(type_node) = &triple.object {
                    entity.type_iris.push(type_node.as_str().to_string());
                }
            } else {
                entity
                    .properties
                    .entry(predicate.to_string())
                    .or_default()
                    .push(term_to_entity_value(&triple.object));
                push_outgoing_ref(&triple.object, &mut entity.outgoing_refs);

                if predicate == SCHEMA_ABOUT_IRI
                    && subject.contains("ro-crate-metadata.json")
                    && let Term::NamedNode(object) = &triple.object
                {
                    graph
                        .metadata_about_candidates
                        .push((subject.clone(), object.as_str().to_string()));
                }
            }
        }

        graph
    }

    fn find_root_entities(&self) -> Result<(String, String), RdfError> {
        if self.metadata_about_candidates.is_empty() {
            return Err(RdfError::MissingRootEntities(
                "Could not find metadata file with schema:about referencing a crate".to_string(),
            ));
        }

        for (metadata_iri, crate_iri) in &self.metadata_about_candidates {
            if self.entities.get(crate_iri).is_some_and(|entity| {
                entity
                    .type_iris
                    .iter()
                    .any(|type_iri| type_iri == SCHEMA_DATASET_IRI)
            }) {
                return Ok((metadata_iri.clone(), crate_iri.clone()));
            }
        }

        Err(RdfError::MissingRootEntities(
            "Could not find root crate (schema:Dataset) referenced by metadata file".to_string(),
        ))
    }
}

fn subject_to_string(subject: &NamedOrBlankNode) -> String {
    match subject {
        NamedOrBlankNode::NamedNode(node) => node.as_str().to_string(),
        NamedOrBlankNode::BlankNode(node) => format!("_:{}", node.as_str()),
    }
}

fn push_outgoing_ref(term: &Term, outgoing_refs: &mut Vec<String>) {
    match term {
        Term::NamedNode(node) => outgoing_refs.push(node.as_str().to_string()),
        Term::BlankNode(node) => outgoing_refs.push(format!("_:{}", node.as_str())),
        Term::Literal(_) => {}
    }
}

fn collapse_values(values: Vec<EntityValue>) -> Option<EntityValue> {
    match <[EntityValue; 1]>::try_from(values) {
        Ok([single]) => Some(single),
        Err(values) if values.is_empty() => None,
        Err(values) => Some(EntityValue::EntityVec(values)),
    }
}

struct ContextCompactor<'a> {
    context: &'a ResolvedContext,
    exact_terms: HashMap<&'a str, &'a str>,
    prefixes: Vec<(&'a str, &'a str)>,
    cache: HashMap<String, String>,
}

impl<'a> ContextCompactor<'a> {
    fn new(context: &'a ResolvedContext) -> Self {
        let exact_terms = context
            .terms
            .iter()
            .map(|(term, iri)| (iri.as_str(), term.as_str()))
            .collect();

        let mut prefixes: Vec<_> = context
            .prefixes
            .iter()
            .map(|(prefix, namespace)| (prefix.as_str(), namespace.as_str()))
            .collect();
        prefixes.sort_unstable_by(|(_, left), (_, right)| right.len().cmp(&left.len()));

        Self {
            context,
            exact_terms,
            prefixes,
            cache: HashMap::new(),
        }
    }

    fn compact(&mut self, iri: &str) -> String {
        if let Some(compacted) = self.cache.get(iri) {
            return compacted.clone();
        }

        let compacted = if let Some(term) = self.exact_terms.get(iri) {
            (*term).to_string()
        } else if let Some((prefix, namespace)) = self
            .prefixes
            .iter()
            .find(|(_, namespace)| iri.starts_with(*namespace))
        {
            format!("{}:{}", prefix, &iri[namespace.len()..])
        } else if let Some(vocab) = &self.context.vocab {
            if iri.starts_with(vocab) {
                iri[vocab.len()..].to_string()
            } else if let Some(base) = &self.context.base {
                compact_against_base(base, iri)
            } else {
                iri.to_string()
            }
        } else if let Some(base) = &self.context.base {
            compact_against_base(base, iri)
        } else {
            iri.to_string()
        };

        self.cache.insert(iri.to_string(), compacted.clone());
        compacted
    }
}

fn compact_against_base(base: &str, iri: &str) -> String {
    let base_no_slash = base.trim_end_matches('/');
    if let Some(fragment_part) = iri.strip_prefix(base_no_slash)
        && fragment_part.starts_with('#')
    {
        return fragment_part.to_string();
    }

    if let Some(relative) = iri.strip_prefix(base) {
        if relative.is_empty() {
            return "./".to_string();
        }
        return relative.to_string();
    }

    iri.to_string()
}

fn compact_types_with_compactor(
    type_iris: &[String],
    compactor: &mut ContextCompactor<'_>,
) -> (DataType, Vec<String>) {
    let types: Vec<String> = type_iris
        .iter()
        .map(|type_iri| compactor.compact(type_iri))
        .collect();

    let data_type = if types.is_empty() {
        DataType::Term("Thing".to_string())
    } else if types.len() == 1 {
        DataType::Term(types[0].clone())
    } else {
        DataType::TermArray(types.clone())
    };

    (data_type, types)
}

fn compact_entity_value_with_compactor(
    value: &mut EntityValue,
    compactor: &mut ContextCompactor<'_>,
) {
    match value {
        EntityValue::EntityId(id) => match id {
            Id::Id(iri) => {
                if !iri.starts_with("_:") {
                    let compacted = compactor.compact(iri);
                    if compacted == *iri && iri.starts_with("http") {
                        log::warn!("No context mapping for IRI '{}', using raw URI", iri);
                    }
                    *iri = ensure_relative_prefix(&compacted);
                }
            }
            Id::IdArray(iris_vec) => {
                for iri in iris_vec.iter_mut() {
                    if !iri.starts_with("_:") {
                        let compacted = compactor.compact(iri);
                        if compacted == *iri && iri.starts_with("http") {
                            log::warn!("No context mapping for IRI '{}', using raw URI", iri);
                        }
                        *iri = ensure_relative_prefix(&compacted);
                    }
                }
            }
        },
        EntityValue::EntityVec(vec) => {
            for nested in vec.iter_mut() {
                compact_entity_value_with_compactor(nested, compactor);
            }
        }
        EntityValue::EntityObject(map) => {
            for nested in map.values_mut() {
                compact_entity_value_with_compactor(nested, compactor);
            }
        }
        EntityValue::EntityVecObject(vec_map) => {
            for map in vec_map.iter_mut() {
                for nested in map.values_mut() {
                    compact_entity_value_with_compactor(nested, compactor);
                }
            }
        }
        EntityValue::NestedDynamicEntity(nested) => {
            compact_entity_value_with_compactor(nested, compactor);
        }
        _ => {}
    }
}

fn build_properties_from_indexed_entity(
    raw_properties: HashMap<String, Vec<EntityValue>>,
    compactor: &mut ContextCompactor<'_>,
) -> HashMap<String, EntityValue> {
    raw_properties
        .into_iter()
        .filter_map(|(key, values)| {
            let mut value = collapse_values(values)?;
            compact_entity_value_with_compactor(&mut value, compactor);
            Some((compactor.compact(&key), value))
        })
        .collect()
}

fn build_metadata_descriptor_from_index(
    metadata_iri: &str,
    root_iri: &str,
    entity: IndexedEntity,
    compactor: &mut ContextCompactor<'_>,
) -> MetadataDescriptor {
    let mut metadata_properties =
        build_properties_from_indexed_entity(entity.properties, compactor);
    let (metadata_type, _) = compact_types_with_compactor(&entity.type_iris, compactor);

    let about = metadata_properties
        .remove("about")
        .and_then(|value| match value {
            EntityValue::EntityId(id) => Some(id),
            _ => None,
        })
        .unwrap_or_else(|| Id::Id(root_iri.to_string()));

    let compacted_id = if metadata_iri.contains("ro-crate-metadata.json") {
        "ro-crate-metadata.json".to_string()
    } else {
        compactor.compact(metadata_iri)
    };

    MetadataDescriptor {
        id: compacted_id,
        type_: metadata_type,
        conforms_to: Id::Id(ROCRATE_CONTEXT_IRI.to_string()),
        about,
        dynamic_entity: Some(metadata_properties),
    }
}

fn build_root_entity_from_index(
    root_iri: &str,
    entity: IndexedEntity,
    compactor: &mut ContextCompactor<'_>,
) -> RootDataEntity {
    let mut root_properties = build_properties_from_indexed_entity(entity.properties, compactor);
    let (root_type, _) = compact_types_with_compactor(&entity.type_iris, compactor);

    let name = root_properties
        .remove("name")
        .and_then(|value| match value {
            EntityValue::EntityString(name) => Some(name),
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
        .and_then(|value| match value {
            EntityValue::EntityString(description) => Some(description),
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
        .and_then(|value| match value {
            EntityValue::EntityString(date_published) => Some(date_published),
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
        .map(|value| match value {
            EntityValue::EntityId(id) => License::Id(id),
            EntityValue::EntityString(description) => License::Description(description),
            _ => License::Description(String::new()),
        })
        .unwrap_or_else(|| {
            log::warn!(
                "Root entity '{}' missing 'license' property (SHOULD per RO-Crate spec)",
                root_iri
            );
            License::Description(String::new())
        });

    RootDataEntity {
        id: ensure_relative_prefix(&compactor.compact(root_iri)),
        type_: root_type,
        name,
        description,
        date_published,
        license,
        dynamic_entity: Some(root_properties),
    }
}

fn build_graph_entity_from_index(
    iri: &str,
    entity: IndexedEntity,
    compactor: &mut ContextCompactor<'_>,
) -> GraphVector {
    let properties = build_properties_from_indexed_entity(entity.properties, compactor);
    let (entity_type, type_strings) = compact_types_with_compactor(&entity.type_iris, compactor);
    let compacted_id = ensure_relative_prefix(&compactor.compact(iri));

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

fn collect_reachable_entities_indexed(
    root_iri: &str,
    graph: &IndexedGraph,
    context: &ResolvedContext,
) -> HashSet<String> {
    let mut reachable = HashSet::new();
    let mut to_process = vec![root_iri.to_string()];

    while let Some(current_iri) = to_process.pop() {
        if !reachable.insert(current_iri.clone()) {
            continue;
        }

        let Some(entity) = graph.entities.get(&current_iri) else {
            continue;
        };

        for iri in &entity.outgoing_refs {
            if reachable.contains(iri) || is_vocabulary_iri(iri, context) {
                continue;
            }

            if graph.entities.contains_key(iri) {
                to_process.push(iri.clone());
            } else {
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

fn rdf_index_to_rocrate_with_context(
    mut graph_index: IndexedGraph,
    context: ResolvedContext,
) -> Result<RoCrate, RdfError> {
    let (metadata_iri, root_iri) = graph_index.find_root_entities()?;
    let reachable_iris = collect_reachable_entities_indexed(&root_iri, &graph_index, &context);

    for iri in graph_index.entities.keys() {
        if iri != &metadata_iri && iri != &root_iri && !reachable_iris.contains(iri) {
            log::warn!(
                "Entity '{}' is not reachable from root and will be excluded",
                iri
            );
        }
    }

    let mut compactor = ContextCompactor::new(&context);
    let metadata_descriptor = build_metadata_descriptor_from_index(
        &metadata_iri,
        &root_iri,
        graph_index.entities.remove(&metadata_iri).ok_or_else(|| {
            RdfError::MissingRootEntities(format!(
                "Metadata entity '{}' disappeared during indexing",
                metadata_iri
            ))
        })?,
        &mut compactor,
    );
    let root_entity = build_root_entity_from_index(
        &root_iri,
        graph_index.entities.remove(&root_iri).ok_or_else(|| {
            RdfError::MissingRootEntities(format!(
                "Root entity '{}' disappeared during indexing",
                root_iri
            ))
        })?,
        &mut compactor,
    );

    let mut graph = vec![
        GraphVector::MetadataDescriptor(metadata_descriptor),
        GraphVector::RootDataEntity(root_entity),
    ];

    for iri in reachable_iris {
        if iri == metadata_iri || iri == root_iri {
            continue;
        }

        if let Some(entity) = graph_index.entities.remove(&iri) {
            graph.push(build_graph_entity_from_index(&iri, entity, &mut compactor));
        }
    }

    Ok(RoCrate {
        context: RoCrateContext::ReferenceContext(ROCRATE_CONTEXT_IRI.to_string()),
        graph,
    })
}

/// Convert an RDF Term to an EntityValue.
///
/// Typed literals are parsed by datatype; untyped/unknown literals fall back
/// to lexical heuristics.
fn term_to_entity_value(term: &Term) -> EntityValue {
    match term {
        Term::NamedNode(node) => EntityValue::EntityId(Id::Id(node.as_str().to_string())),
        Term::BlankNode(node) => EntityValue::EntityId(Id::Id(format!("_:{}", node.as_str()))),
        Term::Literal(literal) => {
            let value_str = literal.value();
            let datatype = literal.datatype().as_str();

            // Check datatype FIRST before falling back to lexical heuristics
            match datatype {
                // xsd:string - always return as string, never parse
                XSD_STRING => EntityValue::EntityString(value_str.to_string()),

                // xsd:boolean - handle "true", "false", "1", "0"
                XSD_BOOLEAN => match value_str {
                    "true" | "1" => EntityValue::EntityBool(true),
                    "false" | "0" => EntityValue::EntityBool(false),
                    _ => {
                        log::warn!(
                            "Invalid xsd:boolean value '{}', returning as string",
                            value_str
                        );
                        EntityValue::EntityString(value_str.to_string())
                    }
                },

                // xsd:integer and variants (int, long, short, byte, etc.)
                XSD_INTEGER
                | XSD_INT
                | XSD_LONG
                | XSD_SHORT
                | XSD_BYTE
                | XSD_NON_NEGATIVE_INTEGER
                | XSD_NON_POSITIVE_INTEGER
                | XSD_POSITIVE_INTEGER
                | XSD_NEGATIVE_INTEGER
                | XSD_UNSIGNED_LONG
                | XSD_UNSIGNED_INT
                | XSD_UNSIGNED_SHORT
                | XSD_UNSIGNED_BYTE => value_str.parse::<i64>().map_or_else(
                    |_| {
                        log::warn!(
                            "Failed to parse '{}' as integer, returning as string",
                            value_str
                        );
                        EntityValue::EntityString(value_str.to_string())
                    },
                    EntityValue::Entityi64,
                ),

                // xsd:double, xsd:float, xsd:decimal - parse as f64
                XSD_DOUBLE | XSD_FLOAT | XSD_DECIMAL => value_str.parse::<f64>().map_or_else(
                    |_| {
                        log::warn!(
                            "Failed to parse '{}' as float, returning as string",
                            value_str
                        );
                        EntityValue::EntityString(value_str.to_string())
                    },
                    EntityValue::Entityf64,
                ),

                // Unknown datatype or plain literal - fall back to lexical heuristics
                // Note: Plain literals in RDF 1.1 have implicit xsd:string type,
                // but oxrdf may return a different default. We apply heuristics here
                // for backwards compatibility with untyped data.
                _ => parse_literal_heuristically(value_str),
            }
        }
    }
}

/// Parse a literal value using lexical heuristics (fallback for untyped literals).
///
/// This function attempts to parse the string as boolean, integer, or float
/// in that order, falling back to string if all parsing attempts fail.
fn parse_literal_heuristically(value_str: &str) -> EntityValue {
    // Try boolean
    if let Ok(bool_val) = value_str.parse::<bool>() {
        return EntityValue::EntityBool(bool_val);
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

/// Extract all properties for a subject IRI from triples (without context compaction).
///
/// This is used internally for graph traversal where property names don't matter.
/// For final entity building, use `extract_entity_properties_with_context` instead.
#[cfg(test)]
fn extract_entity_properties_simple(
    subject_iri: &str,
    triples: &[Triple],
) -> HashMap<String, EntityValue> {
    let rdf_type = NamedNode::new_unchecked("http://www.w3.org/1999/02/22-rdf-syntax-ns#type");
    let mut properties: HashMap<String, Vec<EntityValue>> = HashMap::new();

    for triple in triples {
        let subject_str = match &triple.subject {
            NamedOrBlankNode::NamedNode(n) => n.as_str().to_string(),
            NamedOrBlankNode::BlankNode(n) => format!("_:{}", n.as_str()),
        };
        if subject_str == subject_iri {
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

    // Convert Vec<EntityValue> to EntityValue (single or array)
    properties
        .into_iter()
        .filter_map(|(key, values)| match <[EntityValue; 1]>::try_from(values) {
            Ok([single]) => Some((key, single)),
            Err(values) if values.is_empty() => None,
            Err(values) => Some((key, EntityValue::EntityVec(values))),
        })
        .collect()
}

/// Extract all IRI references from an EntityValue.
///
/// Uses an explicit stack to avoid recursion. Includes vocabulary IRIs and
/// blank nodes; callers can filter to entity subjects as needed.
#[cfg(test)]
fn extract_iris_from_entity_value(value: &EntityValue, iris: &mut HashSet<String>) {
    let mut stack: Vec<&EntityValue> = vec![value];

    while let Some(current) = stack.pop() {
        match current {
            EntityValue::EntityId(id) => {
                match id {
                    Id::Id(iri) => {
                        // Include blank nodes as well - they are valid entity references
                        iris.insert(iri.clone());
                    }
                    Id::IdArray(iris_vec) => {
                        for iri in iris_vec {
                            // Include blank nodes as well - they are valid entity references
                            iris.insert(iri.clone());
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
///
/// Handles both named nodes and blank nodes (with `_:` prefix).
#[cfg(test)]
fn is_entity_subject(iri: &str, triples: &[Triple]) -> bool {
    triples.iter().any(|t| {
        let subject_str = match &t.subject {
            NamedOrBlankNode::NamedNode(n) => n.as_str().to_string(),
            NamedOrBlankNode::BlankNode(n) => format!("_:{}", n.as_str()),
        };
        subject_str == iri
    })
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
#[cfg(test)]
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

/// Internal implementation: Convert RDF triples to RoCrate using a ResolvedContext.
fn rdf_to_rocrate_with_context<I>(triples: I, context: ResolvedContext) -> Result<RoCrate, RdfError>
where
    I: IntoIterator<Item = Triple>,
{
    rdf_index_to_rocrate_with_context(IndexedGraph::from_triples(triples), context)
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
    // Use the graph's stored context for compaction
    rdf_to_rocrate_with_context(graph.triples, graph.context)
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
    let graph_index = IndexedGraph::from_triples(parse_rdf(input, format, base)?);

    // Find root entities to infer base IRI
    let (metadata_iri, _) = graph_index.find_root_entities()?;

    // Determine the base IRI for context resolution (None if not determinable)
    let base_iri = base
        .map(|b| b.to_string())
        .or_else(|| infer_base_from_metadata(&metadata_iri));

    // Create a ResolvedContext with RO-Crate 1.2 default context + base IRI
    let ro_context = RoCrateContext::ReferenceContext(ROCRATE_CONTEXT_IRI.to_string());

    let resolver = ContextResolverBuilder::default();
    let mut context = resolver
        .resolve(&ro_context)
        .map_err(|e| RdfError::ParseError(format!("Failed to resolve context: {}", e)))?;

    // Set the base IRI in the context for proper compaction (if available)
    context.base = base_iri;

    // Use the internal implementation with the resolved context
    rdf_index_to_rocrate_with_context(graph_index, context)
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

        // Test simple/string literal - "42" with xsd:string type stays as string
        // (new_simple_literal creates xsd:string typed literal in RDF 1.1)
        let literal = Term::Literal(Literal::new_simple_literal("test"));
        match term_to_entity_value(&literal) {
            EntityValue::EntityString(s) => assert_eq!(s, "test"),
            _ => panic!("Expected EntityString"),
        }

        // Test xsd:string typed literal - numeric-looking values stay as strings
        // This is the key fix: "42"^^xsd:string should NOT be parsed as integer
        let literal = Term::Literal(Literal::new_simple_literal("42"));
        match term_to_entity_value(&literal) {
            EntityValue::EntityString(s) => assert_eq!(s, "42"),
            _ => panic!("Expected EntityString for '42'^^xsd:string"),
        }

        // Test xsd:string typed literal with boolean-looking value stays as string
        // "true"^^xsd:string should NOT be parsed as boolean
        let literal = Term::Literal(Literal::new_simple_literal("true"));
        match term_to_entity_value(&literal) {
            EntityValue::EntityString(s) => assert_eq!(s, "true"),
            _ => panic!("Expected EntityString for 'true'^^xsd:string"),
        }

        // Test named node
        let node = Term::NamedNode(NamedNode::new_unchecked("http://example.org/thing"));
        match term_to_entity_value(&node) {
            EntityValue::EntityId(Id::Id(s)) => assert_eq!(s, "http://example.org/thing"),
            _ => panic!("Expected EntityId"),
        }
    }

    #[test]
    fn test_term_to_entity_value_typed_literals() {
        use oxrdf::Literal;

        let xsd_integer = NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#integer");
        let xsd_boolean = NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#boolean");
        let xsd_double = NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#double");
        let xsd_string = NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#string");

        // Test xsd:integer typed literal
        let literal = Term::Literal(Literal::new_typed_literal("42", xsd_integer.clone()));
        match term_to_entity_value(&literal) {
            EntityValue::Entityi64(i) => assert_eq!(i, 42),
            _ => panic!("Expected Entityi64 for '42'^^xsd:integer"),
        }

        // Test xsd:boolean typed literal with "true"
        let literal = Term::Literal(Literal::new_typed_literal("true", xsd_boolean.clone()));
        match term_to_entity_value(&literal) {
            EntityValue::EntityBool(b) => assert!(b),
            _ => panic!("Expected EntityBool(true) for 'true'^^xsd:boolean"),
        }

        // Test xsd:boolean typed literal with "false"
        let literal = Term::Literal(Literal::new_typed_literal("false", xsd_boolean.clone()));
        match term_to_entity_value(&literal) {
            EntityValue::EntityBool(b) => assert!(!b),
            _ => panic!("Expected EntityBool(false) for 'false'^^xsd:boolean"),
        }

        // Test xsd:boolean typed literal with "1" (should be true)
        let literal = Term::Literal(Literal::new_typed_literal("1", xsd_boolean.clone()));
        match term_to_entity_value(&literal) {
            EntityValue::EntityBool(b) => assert!(b),
            _ => panic!("Expected EntityBool(true) for '1'^^xsd:boolean"),
        }

        // Test xsd:boolean typed literal with "0" (should be false)
        let literal = Term::Literal(Literal::new_typed_literal("0", xsd_boolean.clone()));
        match term_to_entity_value(&literal) {
            EntityValue::EntityBool(b) => assert!(!b),
            _ => panic!("Expected EntityBool(false) for '0'^^xsd:boolean"),
        }

        // Test xsd:double typed literal
        let literal = Term::Literal(Literal::new_typed_literal("3.14", xsd_double.clone()));
        match term_to_entity_value(&literal) {
            EntityValue::Entityf64(f) => assert!((f - 3.14).abs() < 0.0001),
            _ => panic!("Expected Entityf64 for '3.14'^^xsd:double"),
        }

        // Test xsd:string explicitly - should NOT parse as number
        let literal = Term::Literal(Literal::new_typed_literal("42", xsd_string.clone()));
        match term_to_entity_value(&literal) {
            EntityValue::EntityString(s) => assert_eq!(s, "42"),
            _ => panic!("Expected EntityString for '42'^^xsd:string"),
        }
    }

    #[test]
    fn test_term_to_entity_value_language_tagged() {
        use oxrdf::Literal;

        // Language-tagged literals should use heuristic parsing (fallback)
        // since they don't have an XSD datatype
        let literal = Term::Literal(Literal::new_language_tagged_literal_unchecked(
            "hello", "en",
        ));
        match term_to_entity_value(&literal) {
            EntityValue::EntityString(s) => assert_eq!(s, "hello"),
            _ => panic!("Expected EntityString for language-tagged literal"),
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
        use crate::ro_crate::rdf::convert::{ConversionOptions, rocrate_to_rdf_with_options};
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
        use crate::ro_crate::rdf::convert::{ConversionOptions, rocrate_to_rdf_with_options};
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
        assert!(
            data_entities.contains(&"./images/photo.jpg"),
            "Entity ID 'http://example.org/images/photo.jpg' should compact to './images/photo.jpg'"
        );

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

    #[test]
    fn test_blank_node_subject_collected() {
        use oxrdf::{BlankNode, NamedOrBlankNode, Term};

        let bn = BlankNode::new_unchecked("Geometry-1");
        let rdf_type = NamedNode::new_unchecked("http://www.w3.org/1999/02/22-rdf-syntax-ns#type");
        let schema_geo = NamedNode::new_unchecked("http://schema.org/GeoCoordinates");

        let triples = vec![Triple::new(
            NamedOrBlankNode::BlankNode(bn.clone()),
            rdf_type,
            Term::NamedNode(schema_geo),
        )];

        // Verify the blank node subject can be identified using the same logic as in the code
        let subjects: Vec<String> = triples
            .iter()
            .map(|t| match &t.subject {
                NamedOrBlankNode::NamedNode(n) => n.as_str().to_string(),
                NamedOrBlankNode::BlankNode(n) => format!("_:{}", n.as_str()),
            })
            .collect();

        assert!(subjects.contains(&"_:Geometry-1".to_string()));
    }

    #[test]
    fn test_is_entity_subject_with_blank_node() {
        use oxrdf::{BlankNode, NamedOrBlankNode, Term};

        let bn = BlankNode::new_unchecked("TestBlankNode");
        let rdf_type = NamedNode::new_unchecked("http://www.w3.org/1999/02/22-rdf-syntax-ns#type");
        let schema_thing = NamedNode::new_unchecked("http://schema.org/Thing");

        let triples = vec![Triple::new(
            NamedOrBlankNode::BlankNode(bn.clone()),
            rdf_type,
            Term::NamedNode(schema_thing),
        )];

        // Test that is_entity_subject works with blank nodes
        assert!(is_entity_subject("_:TestBlankNode", &triples));
        assert!(!is_entity_subject("_:NonExistent", &triples));
        assert!(!is_entity_subject("http://example.org/named", &triples));
    }

    #[test]
    fn test_extract_entity_properties_simple_with_blank_node() {
        use oxrdf::{BlankNode, NamedOrBlankNode, Term};

        let bn = BlankNode::new_unchecked("TestEntity");
        let name_pred = NamedNode::new_unchecked("http://schema.org/name");
        let name_value = Literal::new_simple_literal("Test Name");

        let triples = vec![Triple::new(
            NamedOrBlankNode::BlankNode(bn.clone()),
            name_pred,
            Term::Literal(name_value),
        )];

        // Test that extract_entity_properties_simple works with blank node subjects
        let properties = extract_entity_properties_simple("_:TestEntity", &triples);
        assert!(properties.contains_key("http://schema.org/name"));
    }

    #[test]
    fn test_blank_node_roundtrip() {
        use crate::ro_crate::rdf::convert::{ConversionOptions, rocrate_to_rdf_with_options};
        use crate::ro_crate::rdf::resolver::ContextResolverBuilder;
        use crate::ro_crate::read::read_crate_obj;
        use oxrdf::NamedOrBlankNode;

        // Create an RoCrate JSON with a blank node entity
        let json = r#"{
            "@context": "https://w3id.org/ro/crate/1.1/context",
            "@graph": [
                {
                    "@id": "ro-crate-metadata.json",
                    "@type": "CreativeWork",
                    "about": {"@id": "./"},
                    "conformsTo": {"@id": "https://w3id.org/ro/crate/1.1"}
                },
                {
                    "@id": "./",
                    "@type": "Dataset",
                    "name": "Test Crate",
                    "description": "A test crate with blank node",
                    "datePublished": "2024-01-01",
                    "license": {"@id": "https://creativecommons.org/licenses/by/4.0/"},
                    "contentLocation": {"@id": "_:place1"}
                },
                {
                    "@id": "_:place1",
                    "@type": "Place",
                    "name": "Test Location"
                }
            ]
        }"#;

        // Parse to RoCrate (validation_level 0 = no validation)
        let crate1 = read_crate_obj(json, 0).expect("Failed to parse RoCrate");

        // Verify blank node entity exists in original
        let has_blank_entity_original = crate1.graph.iter().any(|e| match e {
            GraphVector::ContextualEntity(ce) => ce.id == "_:place1",
            GraphVector::DataEntity(de) => de.id == "_:place1",
            _ => false,
        });
        assert!(
            has_blank_entity_original,
            "Original crate should have blank node entity"
        );

        // Convert to RDF with a base IRI
        let resolver = ContextResolverBuilder::default();
        let graph = rocrate_to_rdf_with_options(
            &crate1,
            resolver,
            ConversionOptions::WithBase("http://example.org/".to_string()),
        )
        .expect("Failed to convert to RDF");

        // Verify blank node triple exists in RDF
        let has_blank_subject = graph.iter().any(
            |t| matches!(&t.subject, NamedOrBlankNode::BlankNode(bn) if bn.as_str() == "place1"),
        );
        assert!(
            has_blank_subject,
            "RDF graph should have blank node subject"
        );

        // Convert back to RoCrate
        let crate2 = rdf_graph_to_rocrate(graph).expect("Failed to convert back to RoCrate");

        // Verify blank node entity preserved after roundtrip
        let has_blank_entity_roundtrip = crate2.graph.iter().any(|e| match e {
            GraphVector::ContextualEntity(ce) => ce.id == "_:place1",
            GraphVector::DataEntity(de) => de.id == "_:place1",
            _ => false,
        });
        assert!(
            has_blank_entity_roundtrip,
            "Roundtrip crate should preserve blank node entity"
        );
    }
}
