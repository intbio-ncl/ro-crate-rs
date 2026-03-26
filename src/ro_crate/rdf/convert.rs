//! RoCrate to RDF conversion.

use std::collections::HashMap;

use log::warn;
use oxrdf::{BlankNode, Literal, NamedNode, NamedOrBlankNode, Term, Triple};

use crate::ro_crate::constraints::{DataType, EntityValue, Id, License};
use crate::ro_crate::graph_vector::GraphVector;
use crate::ro_crate::rocrate::RoCrate;

use super::context::ResolvedContext;
use super::error::RdfError;
use super::graph::RdfGraph;
use super::resolver::ContextResolverBuilder;

/// Configuration for how relative IRIs are handled during RDF conversion.
#[derive(Debug, Clone, Default)]
pub enum ConversionOptions {
    /// Strict mode: fail on any relative IRI that cannot be resolved.
    /// Use when the context should define @base or all IRIs should be absolute.
    #[default]
    Strict,

    /// Permissive mode: allow relative IRIs to pass through unresolved.
    /// Use for debugging or when relative IRIs are acceptable in output.
    AllowRelative,

    /// Resolve relative IRIs against the provided base IRI as a fallback.
    /// The @base defined in the document's context takes precedence over this.
    WithBase(String),
}

impl ConversionOptions {
    /// Creates options with a specified base IRI for resolving relative references.
    pub fn with_base(base: impl Into<String>) -> Self {
        Self::WithBase(base.into())
    }

    /// Returns the base IRI if configured.
    pub fn base_iri(&self) -> Option<&str> {
        match self {
            Self::WithBase(base) => Some(base.as_str()),
            _ => None,
        }
    }

    /// Returns whether relative IRIs should be allowed through unresolved.
    pub fn allow_relative(&self) -> bool {
        matches!(self, Self::AllowRelative)
    }
}

/// Internal converter holding shared state for RDF conversion.
struct RdfConverter<'a> {
    ctx: &'a ResolvedContext,
    allow_relative: bool,
    rdf_type: NamedNode,
    xsd_integer: NamedNode,
    xsd_double: NamedNode,
    xsd_boolean: NamedNode,
    named_node_cache: HashMap<String, NamedNode>,
}

impl<'a> RdfConverter<'a> {
    fn new(ctx: &'a ResolvedContext, options: &ConversionOptions) -> Self {
        Self {
            ctx,
            allow_relative: options.allow_relative(),
            rdf_type: NamedNode::new_unchecked("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"),
            xsd_integer: NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#integer"),
            xsd_double: NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#double"),
            xsd_boolean: NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#boolean"),
            named_node_cache: HashMap::new(),
        }
    }

    /// Expands and validates a term to a NamedNode.
    fn named_node(&mut self, term: &str) -> Result<NamedNode, RdfError> {
        if let Some(node) = self.named_node_cache.get(term) {
            return Ok(node.clone());
        }

        let expanded = self
            .ctx
            .expand_term_checked(term, self.allow_relative)
            .map_err(|e| RdfError::InvalidIri(e.to_string()))?;

        let node = if self.allow_relative {
            // Skip validation for relative IRIs
            NamedNode::new_unchecked(expanded)
        } else {
            NamedNode::new(&expanded).map_err(|e| RdfError::InvalidIri(e.to_string()))?
        };

        self.named_node_cache.insert(term.to_string(), node.clone());
        Ok(node)
    }

    /// Converts an Id to one or more NamedNodes.
    fn id_to_nodes(&mut self, id: &Id) -> Result<Vec<NamedNode>, RdfError> {
        match id {
            Id::Id(s) => Ok(vec![self.named_node(s)?]),
            Id::IdArray(ids) => ids.iter().map(|s| self.named_node(s)).collect(),
        }
    }

    /// Converts an ID string to an RDF Term, handling blank nodes.
    fn id_string_to_term(&mut self, id: &str) -> Result<Term, RdfError> {
        if let Some(local_name) = id.strip_prefix("_:") {
            Ok(Term::BlankNode(BlankNode::new_unchecked(local_name)))
        } else {
            Ok(Term::NamedNode(self.named_node(id)?))
        }
    }

    /// Converts an Id to one or more Terms, handling blank nodes.
    fn id_to_terms(&mut self, id: &Id) -> Result<Vec<Term>, RdfError> {
        match id {
            Id::Id(single) => Ok(vec![self.id_string_to_term(single)?]),
            Id::IdArray(arr) => arr.iter().map(|s| self.id_string_to_term(s)).collect(),
        }
    }

    /// Creates a typed literal.
    fn typed_literal<T: ToString>(&self, value: T, datatype: &NamedNode) -> Literal {
        Literal::new_typed_literal(value.to_string(), datatype.clone())
    }

    /// Converts an entity @id to an RDF subject.
    fn id_to_subject(&mut self, id: &str) -> Result<NamedOrBlankNode, RdfError> {
        if let Some(local_name) = id.strip_prefix("_:") {
            return Ok(NamedOrBlankNode::BlankNode(BlankNode::new_unchecked(
                local_name,
            )));
        }
        Ok(NamedOrBlankNode::NamedNode(self.named_node(id)?))
    }

    /// Adds type triples for the given DataType.
    fn add_type_triples(
        &mut self,
        graph: &mut RdfGraph,
        subject: &NamedOrBlankNode,
        type_: &DataType,
    ) -> Result<(), RdfError> {
        let types: Vec<&str> = match type_ {
            DataType::Term(t) => vec![t.as_str()],
            DataType::TermArray(ts) => ts.iter().map(|s| s.as_str()).collect(),
        };
        for t in types {
            graph.insert(Triple::new(
                subject.clone(),
                self.rdf_type.clone(),
                self.named_node(t)?,
            ));
        }
        Ok(())
    }

    /// Adds a string literal triple.
    /// Skips creating the triple if the value is empty, preserving "missing" semantics.
    fn add_string_triple(
        &mut self,
        graph: &mut RdfGraph,
        subject: &NamedOrBlankNode,
        predicate: &str,
        value: &str,
    ) -> Result<(), RdfError> {
        if value.is_empty() {
            return Ok(());
        }
        graph.insert(Triple::new(
            subject.clone(),
            self.named_node(predicate)?,
            Literal::new_simple_literal(value),
        ));
        Ok(())
    }

    /// Adds triples for an Id value (single or array).
    fn add_id_triples(
        &mut self,
        graph: &mut RdfGraph,
        subject: &NamedOrBlankNode,
        predicate: &str,
        id: &Id,
    ) -> Result<(), RdfError> {
        let pred = self.named_node(predicate)?;
        for node in self.id_to_nodes(id)? {
            graph.insert(Triple::new(subject.clone(), pred.clone(), node));
        }
        Ok(())
    }

    /// Adds a triple for a License value.
    fn add_license_triple(
        &mut self,
        graph: &mut RdfGraph,
        subject: &NamedOrBlankNode,
        license: &License,
    ) -> Result<(), RdfError> {
        let pred = self.named_node("license")?;
        match license {
            License::Id(id) => {
                for node in self.id_to_nodes(id)? {
                    graph.insert(Triple::new(subject.clone(), pred.clone(), node));
                }
            }
            License::Description(desc) => {
                graph.insert(Triple::new(
                    subject.clone(),
                    pred,
                    Literal::new_simple_literal(desc),
                ));
            }
        }
        Ok(())
    }

    /// Adds triples from dynamic_entity HashMap.
    fn add_dynamic_triples(
        &mut self,
        graph: &mut RdfGraph,
        subject: &NamedOrBlankNode,
        dynamic: &HashMap<String, EntityValue>,
    ) -> Result<(), RdfError> {
        for (key, value) in dynamic {
            let pred = self.named_node(key)?;
            self.add_entity_value_triples(graph, subject, &pred, value, key)?;
        }
        Ok(())
    }

    fn collect_entity_value_terms(
        &mut self,
        value: &EntityValue,
        predicate_name: &str,
        terms: &mut Vec<Term>,
    ) -> Result<(), RdfError> {
        match value {
            EntityValue::EntityString(s) => {
                terms.push(Term::Literal(Literal::new_simple_literal(s)));
            }
            EntityValue::EntityVecString(ss) => {
                terms.extend(
                    ss.iter()
                        .map(|value| Term::Literal(Literal::new_simple_literal(value))),
                );
            }
            EntityValue::Entityi64(n) => {
                terms.push(Term::Literal(self.typed_literal(n, &self.xsd_integer)));
            }
            EntityValue::Entityf64(n) => {
                terms.push(Term::Literal(self.typed_literal(n, &self.xsd_double)));
            }
            EntityValue::EntityVeci64(ns) => {
                terms.extend(
                    ns.iter()
                        .map(|value| Term::Literal(self.typed_literal(value, &self.xsd_integer))),
                );
            }
            EntityValue::EntityVecf64(ns) => {
                terms.extend(
                    ns.iter()
                        .map(|value| Term::Literal(self.typed_literal(value, &self.xsd_double))),
                );
            }
            EntityValue::EntityBool(b) => {
                terms.push(Term::Literal(self.typed_literal(b, &self.xsd_boolean)));
            }
            EntityValue::EntityId(id) => terms.extend(self.id_to_terms(id)?),
            EntityValue::EntityLicense(license) => match license {
                License::Id(id) => {
                    terms.extend(self.id_to_nodes(id)?.into_iter().map(Term::NamedNode));
                }
                License::Description(desc) => {
                    terms.push(Term::Literal(Literal::new_simple_literal(desc)));
                }
            },
            EntityValue::EntityDataType(dt) => {
                let types: Vec<&str> = match dt {
                    DataType::Term(t) => vec![t.as_str()],
                    DataType::TermArray(ts) => ts.iter().map(|value| value.as_str()).collect(),
                };
                for type_name in types {
                    terms.push(Term::NamedNode(self.named_node(type_name)?));
                }
            }
            EntityValue::EntityVec(values) => {
                for nested in values {
                    self.collect_entity_value_terms(nested, predicate_name, terms)?;
                }
            }
            EntityValue::EntityObject(obj) => {
                warn!(
                    "Skipping nested object for predicate '{}': {:?} (would require blank node)",
                    predicate_name, obj
                );
            }
            EntityValue::EntityVecObject(objs) => {
                warn!(
                    "Skipping {} nested objects for predicate '{}' (would require blank nodes)",
                    objs.len(),
                    predicate_name
                );
            }
            EntityValue::NestedDynamicEntity(nested) => {
                warn!(
                    "Skipping nested dynamic entity for predicate '{}': {:?}",
                    predicate_name, nested
                );
            }
            EntityValue::EntityNull(_) => {
                warn!("Skipping null value for predicate '{}'", predicate_name);
            }
            EntityValue::EntityNone(_) => {}
            EntityValue::Fallback(val) => {
                warn!(
                    "Skipping fallback value for predicate '{}': {:?}",
                    predicate_name, val
                );
            }
        }

        Ok(())
    }

    /// Converts an EntityValue to RDF Term(s).
    fn entity_value_to_terms(
        &mut self,
        value: &EntityValue,
        predicate_name: &str,
    ) -> Result<Vec<Term>, RdfError> {
        let mut terms = Vec::new();
        self.collect_entity_value_terms(value, predicate_name, &mut terms)?;
        Ok(terms)
    }

    fn add_entity_value_triples(
        &mut self,
        graph: &mut RdfGraph,
        subject: &NamedOrBlankNode,
        predicate: &NamedNode,
        value: &EntityValue,
        predicate_name: &str,
    ) -> Result<(), RdfError> {
        match value {
            EntityValue::EntityVec(values) => {
                for nested in values {
                    self.add_entity_value_triples(
                        graph,
                        subject,
                        predicate,
                        nested,
                        predicate_name,
                    )?;
                }
            }
            _ => {
                for term in self.entity_value_to_terms(value, predicate_name)? {
                    graph.insert(Triple::new(subject.clone(), predicate.clone(), term));
                }
            }
        }

        Ok(())
    }

    /// Converts a single entity to RDF triples.
    fn add_entity_to_graph(
        &mut self,
        entity: &GraphVector,
        graph: &mut RdfGraph,
    ) -> Result<(), RdfError> {
        let subject = self.id_to_subject(entity.get_id())?;

        self.add_type_triples(graph, &subject, entity.get_type())?;

        match entity {
            GraphVector::MetadataDescriptor(d) => {
                self.add_id_triples(graph, &subject, "conformsTo", &d.conforms_to)?;
                self.add_id_triples(graph, &subject, "about", &d.about)?;
                if let Some(dyn_ent) = &d.dynamic_entity {
                    self.add_dynamic_triples(graph, &subject, dyn_ent)?;
                }
            }
            GraphVector::RootDataEntity(r) => {
                self.add_string_triple(graph, &subject, "name", &r.name)?;
                self.add_string_triple(graph, &subject, "description", &r.description)?;
                self.add_string_triple(graph, &subject, "datePublished", &r.date_published)?;
                self.add_license_triple(graph, &subject, &r.license)?;
                if let Some(dyn_ent) = &r.dynamic_entity {
                    self.add_dynamic_triples(graph, &subject, dyn_ent)?;
                }
            }
            GraphVector::DataEntity(d) => {
                if let Some(dyn_ent) = &d.dynamic_entity {
                    self.add_dynamic_triples(graph, &subject, dyn_ent)?;
                }
            }
            GraphVector::ContextualEntity(c) => {
                if let Some(dyn_ent) = &c.dynamic_entity {
                    self.add_dynamic_triples(graph, &subject, dyn_ent)?;
                }
            }
        }

        Ok(())
    }
}

fn estimate_id_triples(id: &Id) -> usize {
    match id {
        Id::Id(_) => 1,
        Id::IdArray(values) => values.len(),
    }
}

fn estimate_license_triples(license: &License) -> usize {
    match license {
        License::Id(id) => estimate_id_triples(id),
        License::Description(_) => 1,
    }
}

fn estimate_entity_value_terms(value: &EntityValue) -> usize {
    match value {
        EntityValue::EntityString(_)
        | EntityValue::Entityi64(_)
        | EntityValue::Entityf64(_)
        | EntityValue::EntityBool(_)
        | EntityValue::EntityLicense(License::Description(_)) => 1,
        EntityValue::EntityVecString(values) => values.len(),
        EntityValue::EntityId(id) => estimate_id_triples(id),
        EntityValue::EntityVeci64(values) => values.len(),
        EntityValue::EntityVecf64(values) => values.len(),
        EntityValue::EntityLicense(License::Id(id)) => estimate_id_triples(id),
        EntityValue::EntityDataType(data_type) => match data_type {
            DataType::Term(_) => 1,
            DataType::TermArray(values) => values.len(),
        },
        EntityValue::EntityVec(values) => values.iter().map(estimate_entity_value_terms).sum(),
        EntityValue::EntityObject(_)
        | EntityValue::EntityVecObject(_)
        | EntityValue::NestedDynamicEntity(_)
        | EntityValue::EntityNull(_)
        | EntityValue::EntityNone(_)
        | EntityValue::Fallback(_) => 0,
    }
}

fn estimate_dynamic_triples(dynamic: &HashMap<String, EntityValue>) -> usize {
    dynamic.values().map(estimate_entity_value_terms).sum()
}

fn estimate_entity_triples(entity: &GraphVector) -> usize {
    let type_count = match entity.get_type() {
        DataType::Term(_) => 1,
        DataType::TermArray(types) => types.len(),
    };

    let fixed_count = match entity {
        GraphVector::MetadataDescriptor(d) => {
            estimate_id_triples(&d.conforms_to) + estimate_id_triples(&d.about)
        }
        GraphVector::RootDataEntity(r) => {
            usize::from(!r.name.is_empty())
                + usize::from(!r.description.is_empty())
                + usize::from(!r.date_published.is_empty())
                + estimate_license_triples(&r.license)
        }
        GraphVector::DataEntity(_) | GraphVector::ContextualEntity(_) => 0,
    };

    let dynamic_count = match entity {
        GraphVector::MetadataDescriptor(d) => d
            .dynamic_entity
            .as_ref()
            .map_or(0, estimate_dynamic_triples),
        GraphVector::RootDataEntity(r) => r
            .dynamic_entity
            .as_ref()
            .map_or(0, estimate_dynamic_triples),
        GraphVector::DataEntity(d) => d
            .dynamic_entity
            .as_ref()
            .map_or(0, estimate_dynamic_triples),
        GraphVector::ContextualEntity(c) => c
            .dynamic_entity
            .as_ref()
            .map_or(0, estimate_dynamic_triples),
    };

    type_count + fixed_count + dynamic_count
}

/// Converts an RoCrate to RDF triples.
/// Returns an `RdfGraph` containing the triples and resolved context.
pub fn rocrate_to_rdf(
    crate_: &RoCrate,
    resolver: ContextResolverBuilder,
) -> Result<RdfGraph, RdfError> {
    rocrate_to_rdf_with_options(crate_, resolver, ConversionOptions::default())
}

/// Converts an RoCrate to RDF triples with custom options.
pub fn rocrate_to_rdf_with_options(
    crate_: &RoCrate,
    resolver: ContextResolverBuilder,
    options: ConversionOptions,
) -> Result<RdfGraph, RdfError> {
    let mut context = resolver.resolve(&crate_.context)?;

    // Only use WithBase as a fallback if the document doesn't define @base
    if context.base.is_none() {
        if let Some(base) = options.base_iri() {
            context.base = Some(base.to_string());
        }
    }

    let estimated_triples = crate_.graph.iter().map(estimate_entity_triples).sum();
    let mut converter = RdfConverter::new(&context, &options);
    let mut graph = RdfGraph::with_capacity(context.clone(), estimated_triples);

    for entity in &crate_.graph {
        converter.add_entity_to_graph(entity, &mut graph)?;
    }

    Ok(graph)
}

/// RDF type predicate IRI (used in tests).
#[cfg(test)]
const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";

// Helper for tests - wraps the internal converter methods
#[cfg(test)]
fn id_to_subject(
    id: &str,
    ctx: &ResolvedContext,
    options: &ConversionOptions,
) -> Result<NamedOrBlankNode, RdfError> {
    RdfConverter::new(ctx, options).id_to_subject(id)
}

#[cfg(test)]
fn entity_value_to_terms(
    value: &EntityValue,
    predicate_name: &str,
    ctx: &ResolvedContext,
    options: &ConversionOptions,
) -> Result<Vec<Term>, RdfError> {
    RdfConverter::new(ctx, options).entity_value_to_terms(value, predicate_name)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ro_crate::read::read_crate;
    use std::path::{Path, PathBuf};

    fn fixture_path(relative_path: &str) -> PathBuf {
        Path::new("tests/fixtures").join(relative_path)
    }

    fn test_context_with_base() -> ResolvedContext {
        let mut ctx =
            ResolvedContext::new(crate::ro_crate::context::RoCrateContext::ReferenceContext(
                "https://w3id.org/ro/crate/1.2/context".to_string(),
            ));
        ctx.base = Some("http://example.org/crate/".to_string());
        ctx
    }

    #[test]
    fn test_id_to_subject_with_base() {
        let ctx = test_context_with_base();
        let options = ConversionOptions::default();

        let subject = id_to_subject("./", &ctx, &options).unwrap();
        assert!(matches!(subject, NamedOrBlankNode::NamedNode(_)));

        // Verify the IRI is properly resolved
        if let NamedOrBlankNode::NamedNode(node) = subject {
            assert_eq!(node.as_str(), "http://example.org/crate/");
        }
    }

    #[test]
    fn test_id_to_subject_relative_without_base_strict() {
        let ctx = ResolvedContext::new(crate::ro_crate::context::RoCrateContext::ReferenceContext(
            "https://w3id.org/ro/crate/1.2/context".to_string(),
        ));
        let options = ConversionOptions::default(); // strict mode

        let result = id_to_subject("./", &ctx, &options);
        assert!(result.is_err());
        assert!(matches!(result, Err(RdfError::InvalidIri(_))));
    }

    #[test]
    fn test_id_to_subject_relative_without_base_allow() {
        let ctx = ResolvedContext::new(crate::ro_crate::context::RoCrateContext::ReferenceContext(
            "https://w3id.org/ro/crate/1.2/context".to_string(),
        ));
        let options = ConversionOptions::AllowRelative;

        // AllowRelative permits relative IRIs without validation
        let result = id_to_subject("./", &ctx, &options);
        assert!(result.is_ok());
        if let Ok(NamedOrBlankNode::NamedNode(node)) = result {
            assert_eq!(node.as_str(), "./");
        }
    }

    #[test]
    fn test_id_to_subject_accepts_blank_nodes() {
        let ctx = test_context_with_base();
        let options = ConversionOptions::default();
        let mut converter = RdfConverter::new(&ctx, &options);

        let result = converter.id_to_subject("_:Geometry-1");
        assert!(result.is_ok());

        let subject = result.unwrap();
        match subject {
            NamedOrBlankNode::BlankNode(bn) => {
                assert_eq!(bn.as_str(), "Geometry-1");
            }
            _ => panic!("Expected BlankNode, got NamedNode"),
        }
    }

    #[test]
    fn test_id_to_subject_absolute_iri() {
        let ctx = test_context_with_base();
        let options = ConversionOptions::default();

        let subject = id_to_subject("http://example.org/thing", &ctx, &options).unwrap();
        if let NamedOrBlankNode::NamedNode(node) = subject {
            assert_eq!(node.as_str(), "http://example.org/thing");
        }
    }

    #[test]
    fn test_entity_value_to_terms_string() {
        let ctx = test_context_with_base();
        let options = ConversionOptions::default();

        let value = EntityValue::EntityString("test".to_string());
        let terms = entity_value_to_terms(&value, "testPred", &ctx, &options).unwrap();
        assert_eq!(terms.len(), 1);
    }

    #[test]
    fn test_entity_value_to_terms_id() {
        let ctx = test_context_with_base();
        let options = ConversionOptions::default();

        let value = EntityValue::EntityId(Id::Id("http://example.org/entity".to_string()));
        let terms = entity_value_to_terms(&value, "testPred", &ctx, &options).unwrap();
        assert_eq!(terms.len(), 1);
        assert!(matches!(terms[0], Term::NamedNode(_)));
    }

    #[test]
    fn test_entity_value_blank_node_object() {
        let ctx = test_context_with_base();
        let options = ConversionOptions::default();
        let mut converter = RdfConverter::new(&ctx, &options);

        let value = EntityValue::EntityId(Id::Id("_:Geometry-1".to_string()));
        let terms = converter.entity_value_to_terms(&value, "geo").unwrap();

        assert_eq!(terms.len(), 1);
        match &terms[0] {
            Term::BlankNode(bn) => {
                assert_eq!(bn.as_str(), "Geometry-1");
            }
            _ => panic!("Expected Term::BlankNode, got {:?}", terms[0]),
        }
    }

    #[test]
    fn test_entity_value_to_terms_skips_objects() {
        let ctx = test_context_with_base();
        let options = ConversionOptions::default();

        let value = EntityValue::EntityObject(HashMap::new());
        let terms = entity_value_to_terms(&value, "nested", &ctx, &options).unwrap();
        assert!(terms.is_empty(), "Nested objects should be skipped");
    }

    // Integration tests

    /// Default base IRI for test fixtures
    const TEST_BASE_IRI: &str = "http://example.org/test-crate/";

    #[test]
    fn test_rocrate_to_rdf_minimal_1_1() {
        let path = fixture_path("_ro-crate-metadata-minimal.json");
        let crate_ = read_crate(&path, 0).expect("Failed to read crate");

        let graph = rocrate_to_rdf_with_options(
            &crate_,
            ContextResolverBuilder::default(),
            ConversionOptions::with_base(TEST_BASE_IRI),
        )
        .expect("Failed to convert to RDF");

        // Minimal crate has 3 entities: descriptor, root, license
        // Each entity has at least a type triple
        assert!(!graph.is_empty());
        assert!(
            graph.len() >= 3,
            "Expected at least 3 triples, got {}",
            graph.len()
        );

        // Verify we have rdf:type triples
        let type_triples: Vec<_> = graph
            .iter()
            .filter(|t| t.predicate.as_str() == RDF_TYPE)
            .collect();
        assert_eq!(
            type_triples.len(),
            3,
            "Expected 3 type triples (descriptor, root, license)"
        );
    }

    #[test]
    fn test_rocrate_to_rdf_minimal_1_2() {
        let path = fixture_path("_ro-crate-metadata-minimal-1_2.json");
        let crate_ = read_crate(&path, 0).expect("Failed to read crate");

        let graph = rocrate_to_rdf_with_options(
            &crate_,
            ContextResolverBuilder::default(),
            ConversionOptions::with_base(TEST_BASE_IRI),
        )
        .expect("Failed to convert to RDF");

        assert!(!graph.is_empty());
        assert!(
            graph.len() >= 3,
            "Expected at least 3 triples, got {}",
            graph.len()
        );

        // Verify type triples
        let type_triples: Vec<_> = graph
            .iter()
            .filter(|t| t.predicate.as_str() == RDF_TYPE)
            .collect();
        assert_eq!(type_triples.len(), 3);
    }

    #[test]
    fn test_rocrate_to_rdf_with_dynamic_properties() {
        let path = fixture_path("_ro-crate-metadata-dynamic.json");
        let crate_ = read_crate(&path, 0).expect("Failed to read crate");

        let graph = rocrate_to_rdf_with_options(
            &crate_,
            ContextResolverBuilder::default(),
            ConversionOptions::with_base(TEST_BASE_IRI),
        )
        .expect("Failed to convert to RDF");

        assert!(!graph.is_empty());

        // The dynamic fixture has additionalType with array values
        // Verify we got more triples than minimal
        assert!(
            graph.len() > 3,
            "Expected more than 3 triples for dynamic crate"
        );
    }

    #[test]
    fn test_rocrate_to_rdf_preserves_iris() {
        let path = fixture_path("_ro-crate-metadata-minimal.json");
        let crate_ = read_crate(&path, 0).expect("Failed to read crate");

        let graph = rocrate_to_rdf_with_options(
            &crate_,
            ContextResolverBuilder::default(),
            ConversionOptions::with_base(TEST_BASE_IRI),
        )
        .expect("Failed to convert to RDF");

        // Check that schema.org terms are expanded
        let has_schema_org = graph.iter().any(|t| {
            t.predicate.as_str().starts_with("http://schema.org/")
                || t.predicate.as_str().starts_with("https://schema.org/")
        });
        assert!(
            has_schema_org,
            "Expected schema.org predicates to be expanded"
        );
    }

    #[test]
    fn test_rocrate_to_rdf_deduplicates() {
        let path = fixture_path("_ro-crate-metadata-minimal.json");
        let crate_ = read_crate(&path, 0).expect("Failed to read crate");

        let graph = rocrate_to_rdf_with_options(
            &crate_,
            ContextResolverBuilder::default(),
            ConversionOptions::with_base(TEST_BASE_IRI),
        )
        .expect("Failed to convert to RDF");

        let initial_len = graph.len();

        // Convert again and check same count (no duplicates accumulated)
        let graph2 = rocrate_to_rdf_with_options(
            &crate_,
            ContextResolverBuilder::default(),
            ConversionOptions::with_base(TEST_BASE_IRI),
        )
        .expect("Failed to convert to RDF");

        assert_eq!(
            graph2.len(),
            initial_len,
            "Triple count should be consistent"
        );
    }

    #[test]
    fn test_rocrate_to_rdf_type_expansion() {
        let path = fixture_path("_ro-crate-metadata-minimal.json");
        let crate_ = read_crate(&path, 0).expect("Failed to read crate");

        let graph = rocrate_to_rdf_with_options(
            &crate_,
            ContextResolverBuilder::default(),
            ConversionOptions::with_base(TEST_BASE_IRI),
        )
        .expect("Failed to convert to RDF");

        // Find type triples and verify expansion
        let type_triples: Vec<_> = graph
            .iter()
            .filter(|t| t.predicate.as_str() == RDF_TYPE)
            .collect();

        // Check Dataset type is expanded to schema.org
        let has_dataset = type_triples.iter().any(|t| {
            if let Term::NamedNode(n) = &t.object {
                n.as_str().contains("schema.org") && n.as_str().contains("Dataset")
            } else {
                false
            }
        });
        assert!(
            has_dataset,
            "Expected Dataset type to be expanded to schema.org/Dataset"
        );

        // Check CreativeWork type is expanded
        let has_creative_work = type_triples.iter().any(|t| {
            if let Term::NamedNode(n) = &t.object {
                n.as_str().contains("schema.org") && n.as_str().contains("CreativeWork")
            } else {
                false
            }
        });
        assert!(
            has_creative_work,
            "Expected CreativeWork type to be expanded to schema.org/CreativeWork"
        );
    }

    #[test]
    fn test_rocrate_to_rdf_fails_without_base() {
        let path = fixture_path("_ro-crate-metadata-minimal.json");
        let crate_ = read_crate(&path, 0).expect("Failed to read crate");

        // Without a base IRI, conversion should fail for relative entity IDs
        let result = rocrate_to_rdf(&crate_, ContextResolverBuilder::default());
        assert!(
            result.is_err(),
            "Expected error when converting without base IRI"
        );
    }
}
