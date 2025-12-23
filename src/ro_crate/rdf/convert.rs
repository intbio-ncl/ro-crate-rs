//! RoCrate to RDF conversion.

use std::collections::HashMap;

use log::warn;
use oxrdf::{Literal, NamedNode, NamedOrBlankNode, Term, Triple};

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

    /// Resolve relative IRIs against the provided base IRI.
    /// This takes precedence over @base defined in the context.
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
        }
    }

    /// Expands and validates a term to a NamedNode.
    fn named_node(&self, term: &str) -> Result<NamedNode, RdfError> {
        let expanded = self
            .ctx
            .expand_term_checked(term, self.allow_relative)
            .map_err(RdfError::InvalidIri)?;
        NamedNode::new(&expanded).map_err(|e| RdfError::InvalidIri(e.to_string()))
    }

    /// Converts an Id to one or more NamedNodes.
    fn id_to_nodes(&self, id: &Id) -> Result<Vec<NamedNode>, RdfError> {
        match id {
            Id::Id(s) => Ok(vec![self.named_node(s)?]),
            Id::IdArray(ids) => ids.iter().map(|s| self.named_node(s)).collect(),
        }
    }

    /// Creates a typed literal.
    fn typed_literal<T: ToString>(&self, value: T, datatype: &NamedNode) -> Literal {
        Literal::new_typed_literal(value.to_string(), datatype.clone())
    }

    /// Converts an entity @id to an RDF subject.
    fn id_to_subject(&self, id: &str) -> Result<NamedOrBlankNode, RdfError> {
        if id.starts_with("_:") {
            return Err(RdfError::BlankNode(id.to_string()));
        }
        Ok(NamedOrBlankNode::NamedNode(self.named_node(id)?))
    }

    /// Adds type triples for the given DataType.
    fn add_type_triples(
        &self,
        triples: &mut Vec<Triple>,
        subject: &NamedOrBlankNode,
        type_: &DataType,
    ) -> Result<(), RdfError> {
        let types: Vec<&str> = match type_ {
            DataType::Term(t) => vec![t.as_str()],
            DataType::TermArray(ts) => ts.iter().map(|s| s.as_str()).collect(),
        };
        for t in types {
            triples.push(Triple::new(
                subject.clone(),
                self.rdf_type.clone(),
                self.named_node(t)?,
            ));
        }
        Ok(())
    }

    /// Adds a string literal triple.
    fn add_string_triple(
        &self,
        triples: &mut Vec<Triple>,
        subject: &NamedOrBlankNode,
        predicate: &str,
        value: &str,
    ) -> Result<(), RdfError> {
        triples.push(Triple::new(
            subject.clone(),
            self.named_node(predicate)?,
            Literal::new_simple_literal(value),
        ));
        Ok(())
    }

    /// Adds triples for an Id value (single or array).
    fn add_id_triples(
        &self,
        triples: &mut Vec<Triple>,
        subject: &NamedOrBlankNode,
        predicate: &str,
        id: &Id,
    ) -> Result<(), RdfError> {
        let pred = self.named_node(predicate)?;
        for node in self.id_to_nodes(id)? {
            triples.push(Triple::new(subject.clone(), pred.clone(), node));
        }
        Ok(())
    }

    /// Adds a triple for a License value.
    fn add_license_triple(
        &self,
        triples: &mut Vec<Triple>,
        subject: &NamedOrBlankNode,
        license: &License,
    ) -> Result<(), RdfError> {
        let pred = self.named_node("license")?;
        match license {
            License::Id(id) => {
                for node in self.id_to_nodes(id)? {
                    triples.push(Triple::new(subject.clone(), pred.clone(), node));
                }
            }
            License::Description(desc) => {
                triples.push(Triple::new(
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
        &self,
        triples: &mut Vec<Triple>,
        subject: &NamedOrBlankNode,
        dynamic: &HashMap<String, EntityValue>,
    ) -> Result<(), RdfError> {
        for (key, value) in dynamic {
            let pred = self.named_node(key)?;
            for term in self.entity_value_to_terms(value, key)? {
                triples.push(Triple::new(subject.clone(), pred.clone(), term));
            }
        }
        Ok(())
    }

    /// Converts an EntityValue to RDF Term(s).
    fn entity_value_to_terms(
        &self,
        value: &EntityValue,
        predicate_name: &str,
    ) -> Result<Vec<Term>, RdfError> {
        match value {
            // String literals
            EntityValue::EntityString(s) => Ok(vec![Term::Literal(Literal::new_simple_literal(s))]),
            EntityValue::EntityVecString(ss) => Ok(ss
                .iter()
                .map(|s| Term::Literal(Literal::new_simple_literal(s)))
                .collect()),

            // Numeric literals
            EntityValue::Entityi64(n) => {
                Ok(vec![Term::Literal(self.typed_literal(n, &self.xsd_integer))])
            }
            EntityValue::Entityf64(n) => {
                Ok(vec![Term::Literal(self.typed_literal(n, &self.xsd_double))])
            }
            EntityValue::EntityVeci64(ns) => Ok(ns
                .iter()
                .map(|n| Term::Literal(self.typed_literal(n, &self.xsd_integer)))
                .collect()),
            EntityValue::EntityVecf64(ns) => Ok(ns
                .iter()
                .map(|n| Term::Literal(self.typed_literal(n, &self.xsd_double)))
                .collect()),

            // Boolean
            EntityValue::EntityBool(b) => {
                Ok(vec![Term::Literal(self.typed_literal(b, &self.xsd_boolean))])
            }

            // References (Id, License, DataType)
            EntityValue::EntityId(id) => Ok(self
                .id_to_nodes(id)?
                .into_iter()
                .map(Term::NamedNode)
                .collect()),
            EntityValue::EntityLicense(license) => match license {
                License::Id(id) => Ok(self
                    .id_to_nodes(id)?
                    .into_iter()
                    .map(Term::NamedNode)
                    .collect()),
                License::Description(desc) => {
                    Ok(vec![Term::Literal(Literal::new_simple_literal(desc))])
                }
            },
            EntityValue::EntityDataType(dt) => {
                let terms: Vec<&str> = match dt {
                    DataType::Term(t) => vec![t.as_str()],
                    DataType::TermArray(ts) => ts.iter().map(|s| s.as_str()).collect(),
                };
                terms
                    .into_iter()
                    .map(|t| Ok(Term::NamedNode(self.named_node(t)?)))
                    .collect()
            }

            // Nested values
            EntityValue::EntityVec(values) => {
                let mut terms = Vec::new();
                for v in values {
                    terms.extend(self.entity_value_to_terms(v, predicate_name)?);
                }
                Ok(terms)
            }

            // Skipped values (log and return empty)
            EntityValue::EntityObject(obj) => {
                warn!(
                    "Skipping nested object for predicate '{}': {:?} (would require blank node)",
                    predicate_name, obj
                );
                Ok(vec![])
            }
            EntityValue::EntityVecObject(objs) => {
                warn!(
                    "Skipping {} nested objects for predicate '{}' (would require blank nodes)",
                    objs.len(),
                    predicate_name
                );
                Ok(vec![])
            }
            EntityValue::NestedDynamicEntity(nested) => {
                warn!(
                    "Skipping nested dynamic entity for predicate '{}': {:?}",
                    predicate_name, nested
                );
                Ok(vec![])
            }
            EntityValue::EntityNull(_) => {
                warn!("Skipping null value for predicate '{}'", predicate_name);
                Ok(vec![])
            }
            EntityValue::EntityNone(_) => Ok(vec![]),
            EntityValue::Fallback(val) => {
                warn!(
                    "Skipping fallback value for predicate '{}': {:?}",
                    predicate_name, val
                );
                Ok(vec![])
            }
        }
    }

    /// Converts a single entity to RDF triples.
    fn entity_to_triples(&self, entity: &GraphVector) -> Result<Vec<Triple>, RdfError> {
        let mut triples = Vec::new();
        let subject = self.id_to_subject(entity.get_id())?;

        self.add_type_triples(&mut triples, &subject, entity.get_type())?;

        match entity {
            GraphVector::MetadataDescriptor(d) => {
                self.add_id_triples(&mut triples, &subject, "conformsTo", &d.conforms_to)?;
                self.add_id_triples(&mut triples, &subject, "about", &d.about)?;
                if let Some(dyn_ent) = &d.dynamic_entity {
                    self.add_dynamic_triples(&mut triples, &subject, dyn_ent)?;
                }
            }
            GraphVector::RootDataEntity(r) => {
                self.add_string_triple(&mut triples, &subject, "name", &r.name)?;
                self.add_string_triple(&mut triples, &subject, "description", &r.description)?;
                self.add_string_triple(&mut triples, &subject, "datePublished", &r.date_published)?;
                self.add_license_triple(&mut triples, &subject, &r.license)?;
                if let Some(dyn_ent) = &r.dynamic_entity {
                    self.add_dynamic_triples(&mut triples, &subject, dyn_ent)?;
                }
            }
            GraphVector::DataEntity(d) => {
                if let Some(dyn_ent) = &d.dynamic_entity {
                    self.add_dynamic_triples(&mut triples, &subject, dyn_ent)?;
                }
            }
            GraphVector::ContextualEntity(c) => {
                if let Some(dyn_ent) = &c.dynamic_entity {
                    self.add_dynamic_triples(&mut triples, &subject, dyn_ent)?;
                }
            }
        }

        Ok(triples)
    }
}

/// Converts an RoCrate to RDF triples.
///
/// # Arguments
/// * `crate_` - The RoCrate to convert
/// * `resolver` - The context resolver builder (will auto-detect RO-Crate version)
///
/// # Returns
/// An `RdfGraph` containing the triples and resolved context.
///
/// # Example
/// ```ignore
/// use rocraters::ro_crate::rdf::{rocrate_to_rdf, ContextResolverBuilder};
///
/// let graph = rocrate_to_rdf(&rocrate, ContextResolverBuilder::default())?;
/// for triple in graph.iter() {
///     println!("{}", triple);
/// }
/// ```
pub fn rocrate_to_rdf(
    crate_: &RoCrate,
    resolver: ContextResolverBuilder,
) -> Result<RdfGraph, RdfError> {
    rocrate_to_rdf_with_options(crate_, resolver, ConversionOptions::default())
}

/// Converts an RoCrate to RDF triples with custom options.
///
/// # Arguments
/// * `crate_` - The RoCrate to convert
/// * `resolver` - The context resolver builder (will auto-detect RO-Crate version)
/// * `options` - Conversion options controlling behavior like relative IRI handling
///
/// # Returns
/// An `RdfGraph` containing the triples and resolved context.
pub fn rocrate_to_rdf_with_options(
    crate_: &RoCrate,
    resolver: ContextResolverBuilder,
    options: ConversionOptions,
) -> Result<RdfGraph, RdfError> {
    let mut context = resolver.resolve(&crate_.context)?;

    if let Some(base) = options.base_iri() {
        context.base = Some(base.to_string());
    }

    let converter = RdfConverter::new(&context, &options);
    let triples: Vec<Triple> = crate_
        .graph
        .iter()
        .map(|entity| converter.entity_to_triples(entity))
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .flatten()
        .collect();

    let mut graph = RdfGraph::new(context);
    for triple in triples {
        graph.insert(triple);
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
        let mut ctx = ResolvedContext::new(
            crate::ro_crate::context::RoCrateContext::ReferenceContext(
                "https://w3id.org/ro/crate/1.2/context".to_string(),
            ),
        );
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
        let ctx = ResolvedContext::new(
            crate::ro_crate::context::RoCrateContext::ReferenceContext(
                "https://w3id.org/ro/crate/1.2/context".to_string(),
            ),
        );
        let options = ConversionOptions::default(); // strict mode

        let result = id_to_subject("./", &ctx, &options);
        assert!(result.is_err());
        assert!(matches!(result, Err(RdfError::InvalidIri(_))));
    }

    #[test]
    fn test_id_to_subject_relative_without_base_allow() {
        let ctx = ResolvedContext::new(
            crate::ro_crate::context::RoCrateContext::ReferenceContext(
                "https://w3id.org/ro/crate/1.2/context".to_string(),
            ),
        );
        let options = ConversionOptions::AllowRelative;

        // Should fail because "./" is not a valid absolute IRI
        let result = id_to_subject("./", &ctx, &options);
        assert!(result.is_err());
    }

    #[test]
    fn test_id_to_subject_rejects_blank_nodes() {
        let ctx = test_context_with_base();
        let options = ConversionOptions::default();

        let result = id_to_subject("_:b0", &ctx, &options);
        assert!(matches!(result, Err(RdfError::BlankNode(_))));
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
