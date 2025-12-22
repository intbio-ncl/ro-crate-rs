//! RoCrate to RDF conversion.

use std::collections::HashMap;

use oxrdf::{Literal, NamedNode, NamedOrBlankNode, Term, Triple};

use crate::ro_crate::constraints::{DataType, EntityValue, Id, License};
use crate::ro_crate::graph_vector::GraphVector;
use crate::ro_crate::rocrate::RoCrate;

use super::context::ResolvedContext;
use super::error::RdfError;
use super::graph::RdfGraph;
use super::resolver::ContextResolverBuilder;

/// RDF type predicate IRI.
const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";

/// XSD namespace for typed literals.
const XSD_INTEGER: &str = "http://www.w3.org/2001/XMLSchema#integer";
const XSD_DOUBLE: &str = "http://www.w3.org/2001/XMLSchema#double";
const XSD_BOOLEAN: &str = "http://www.w3.org/2001/XMLSchema#boolean";

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
pub fn rocrate_to_rdf(crate_: &RoCrate, resolver: ContextResolverBuilder) -> Result<RdfGraph, RdfError> {
    let context = resolver.resolve(&crate_.context)?;
    let mut graph = RdfGraph::new(context);

    for entity in &crate_.graph {
        let triples = entity_to_triples(entity, &graph.context)?;
        for triple in triples {
            graph.insert(triple);
        }
    }

    Ok(graph)
}

/// Converts a single entity to RDF triples.
fn entity_to_triples(entity: &GraphVector, ctx: &ResolvedContext) -> Result<Vec<Triple>, RdfError> {
    let mut triples = Vec::new();

    let subject = id_to_subject(entity.get_id(), ctx)?;

    // Add rdf:type triple(s)
    add_type_triples(&mut triples, &subject, entity.get_type(), ctx);

    // Add entity-specific static fields
    match entity {
        GraphVector::MetadataDescriptor(descriptor) => {
            // conformsTo
            add_id_triples(&mut triples, &subject, "conformsTo", &descriptor.conforms_to, ctx);
            // about
            add_id_triples(&mut triples, &subject, "about", &descriptor.about, ctx);
            // dynamic_entity
            if let Some(dynamic) = &descriptor.dynamic_entity {
                add_dynamic_triples(&mut triples, &subject, dynamic, ctx)?;
            }
        }
        GraphVector::RootDataEntity(root) => {
            // name
            add_string_triple(&mut triples, &subject, "name", &root.name, ctx);
            // description
            add_string_triple(&mut triples, &subject, "description", &root.description, ctx);
            // datePublished
            add_string_triple(&mut triples, &subject, "datePublished", &root.date_published, ctx);
            // license
            add_license_triple(&mut triples, &subject, &root.license, ctx);
            // dynamic_entity
            if let Some(dynamic) = &root.dynamic_entity {
                add_dynamic_triples(&mut triples, &subject, dynamic, ctx)?;
            }
        }
        GraphVector::DataEntity(data) => {
            if let Some(dynamic) = &data.dynamic_entity {
                add_dynamic_triples(&mut triples, &subject, dynamic, ctx)?;
            }
        }
        GraphVector::ContextualEntity(contextual) => {
            if let Some(dynamic) = &contextual.dynamic_entity {
                add_dynamic_triples(&mut triples, &subject, dynamic, ctx)?;
            }
        }
    }

    Ok(triples)
}

/// Converts an entity @id to an RDF subject.
fn id_to_subject(id: &str, ctx: &ResolvedContext) -> Result<NamedOrBlankNode, RdfError> {
    if id.starts_with("_:") {
        return Err(RdfError::BlankNode(id.to_string()));
    }
    let expanded = ctx.expand_term(id);
    Ok(NamedOrBlankNode::NamedNode(NamedNode::new_unchecked(expanded)))
}

/// Converts a term to a NamedNode using context expansion.
fn term_to_named_node(term: &str, ctx: &ResolvedContext) -> NamedNode {
    NamedNode::new_unchecked(ctx.expand_term(term))
}

/// Adds rdf:type triples for an entity.
fn add_type_triples(triples: &mut Vec<Triple>, subject: &NamedOrBlankNode, type_: &DataType, ctx: &ResolvedContext) {
    let rdf_type_pred = NamedNode::new_unchecked(RDF_TYPE);

    match type_ {
        DataType::Term(t) => {
            let type_iri = term_to_named_node(t, ctx);
            triples.push(Triple::new(subject.clone(), rdf_type_pred, type_iri));
        }
        DataType::TermArray(types) => {
            for t in types {
                let type_iri = term_to_named_node(t, ctx);
                triples.push(Triple::new(subject.clone(), rdf_type_pred.clone(), type_iri));
            }
        }
    }
}

/// Adds a string literal triple.
fn add_string_triple(triples: &mut Vec<Triple>, subject: &NamedOrBlankNode, predicate: &str, value: &str, ctx: &ResolvedContext) {
    let pred = term_to_named_node(predicate, ctx);
    let literal = Literal::new_simple_literal(value);
    triples.push(Triple::new(subject.clone(), pred, literal));
}

/// Adds triples for an Id value (single or array).
fn add_id_triples(triples: &mut Vec<Triple>, subject: &NamedOrBlankNode, predicate: &str, id: &Id, ctx: &ResolvedContext) {
    let pred = term_to_named_node(predicate, ctx);

    match id {
        Id::Id(id_str) => {
            let obj = term_to_named_node(id_str, ctx);
            triples.push(Triple::new(subject.clone(), pred, obj));
        }
        Id::IdArray(ids) => {
            for id_str in ids {
                let obj = term_to_named_node(id_str, ctx);
                triples.push(Triple::new(subject.clone(), pred.clone(), obj));
            }
        }
    }
}

/// Adds a triple for a License value.
fn add_license_triple(triples: &mut Vec<Triple>, subject: &NamedOrBlankNode, license: &License, ctx: &ResolvedContext) {
    let pred = term_to_named_node("license", ctx);

    match license {
        License::Id(id) => {
            match id {
                Id::Id(id_str) => {
                    let obj = term_to_named_node(id_str, ctx);
                    triples.push(Triple::new(subject.clone(), pred, obj));
                }
                Id::IdArray(ids) => {
                    for id_str in ids {
                        let obj = term_to_named_node(id_str, ctx);
                        triples.push(Triple::new(subject.clone(), pred.clone(), obj));
                    }
                }
            }
        }
        License::Description(desc) => {
            let literal = Literal::new_simple_literal(desc);
            triples.push(Triple::new(subject.clone(), pred, literal));
        }
    }
}

/// Adds triples from dynamic_entity HashMap.
fn add_dynamic_triples(
    triples: &mut Vec<Triple>,
    subject: &NamedOrBlankNode,
    dynamic: &HashMap<String, EntityValue>,
    ctx: &ResolvedContext,
) -> Result<(), RdfError> {
    for (key, value) in dynamic {
        let pred = term_to_named_node(key, ctx);
        let terms = entity_value_to_terms(value, ctx)?;
        for term in terms {
            triples.push(Triple::new(subject.clone(), pred.clone(), term));
        }
    }
    Ok(())
}

/// Converts an EntityValue to RDF Term(s).
fn entity_value_to_terms(value: &EntityValue, ctx: &ResolvedContext) -> Result<Vec<Term>, RdfError> {
    match value {
        EntityValue::EntityString(s) => {
            Ok(vec![Term::Literal(Literal::new_simple_literal(s))])
        }
        EntityValue::EntityVecString(strings) => {
            Ok(strings.iter().map(|s| Term::Literal(Literal::new_simple_literal(s))).collect())
        }
        EntityValue::Entityi64(n) => {
            let lit = Literal::new_typed_literal(n.to_string(), NamedNode::new_unchecked(XSD_INTEGER));
            Ok(vec![Term::Literal(lit)])
        }
        EntityValue::Entityf64(n) => {
            let lit = Literal::new_typed_literal(n.to_string(), NamedNode::new_unchecked(XSD_DOUBLE));
            Ok(vec![Term::Literal(lit)])
        }
        EntityValue::EntityVeci64(nums) => {
            Ok(nums.iter().map(|n| {
                let lit = Literal::new_typed_literal(n.to_string(), NamedNode::new_unchecked(XSD_INTEGER));
                Term::Literal(lit)
            }).collect())
        }
        EntityValue::EntityVecf64(nums) => {
            Ok(nums.iter().map(|n| {
                let lit = Literal::new_typed_literal(n.to_string(), NamedNode::new_unchecked(XSD_DOUBLE));
                Term::Literal(lit)
            }).collect())
        }
        EntityValue::EntityBool(b) => {
            let lit = Literal::new_typed_literal(b.to_string(), NamedNode::new_unchecked(XSD_BOOLEAN));
            Ok(vec![Term::Literal(lit)])
        }
        EntityValue::EntityId(id) => {
            match id {
                Id::Id(id_str) => {
                    let node = term_to_named_node(id_str, ctx);
                    Ok(vec![Term::NamedNode(node)])
                }
                Id::IdArray(ids) => {
                    Ok(ids.iter().map(|id_str| {
                        Term::NamedNode(term_to_named_node(id_str, ctx))
                    }).collect())
                }
            }
        }
        EntityValue::EntityLicense(license) => {
            match license {
                License::Id(id) => {
                    match id {
                        Id::Id(id_str) => {
                            let node = term_to_named_node(id_str, ctx);
                            Ok(vec![Term::NamedNode(node)])
                        }
                        Id::IdArray(ids) => {
                            Ok(ids.iter().map(|id_str| {
                                Term::NamedNode(term_to_named_node(id_str, ctx))
                            }).collect())
                        }
                    }
                }
                License::Description(desc) => {
                    Ok(vec![Term::Literal(Literal::new_simple_literal(desc))])
                }
            }
        }
        EntityValue::EntityDataType(dt) => {
            match dt {
                DataType::Term(t) => {
                    let node = term_to_named_node(t, ctx);
                    Ok(vec![Term::NamedNode(node)])
                }
                DataType::TermArray(types) => {
                    Ok(types.iter().map(|t| {
                        Term::NamedNode(term_to_named_node(t, ctx))
                    }).collect())
                }
            }
        }
        EntityValue::EntityVec(values) => {
            let mut terms = Vec::new();
            for v in values {
                terms.extend(entity_value_to_terms(v, ctx)?);
            }
            Ok(terms)
        }
        // Skip values that can't be represented in RDF or are not expected in valid RO-Crates
        EntityValue::EntityObject(_) |
        EntityValue::EntityVecObject(_) |
        EntityValue::NestedDynamicEntity(_) |
        EntityValue::EntityNull(_) |
        EntityValue::EntityNone(_) |
        EntityValue::Fallback(_) => {
            Ok(vec![])
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::{Path, PathBuf};
    use crate::ro_crate::read::read_crate;

    fn fixture_path(relative_path: &str) -> PathBuf {
        Path::new("tests/fixtures").join(relative_path)
    }

    #[test]
    fn test_id_to_subject() {
        let ctx = ResolvedContext::new(crate::ro_crate::context::RoCrateContext::ReferenceContext(
            "https://w3id.org/ro/crate/1.2/context".to_string(),
        ));

        let subject = id_to_subject("./", &ctx).unwrap();
        assert!(matches!(subject, NamedOrBlankNode::NamedNode(_)));
    }

    #[test]
    fn test_id_to_subject_rejects_blank_nodes() {
        let ctx = ResolvedContext::new(crate::ro_crate::context::RoCrateContext::ReferenceContext(
            "https://w3id.org/ro/crate/1.2/context".to_string(),
        ));

        let result = id_to_subject("_:b0", &ctx);
        assert!(matches!(result, Err(RdfError::BlankNode(_))));
    }

    #[test]
    fn test_entity_value_to_terms_string() {
        let ctx = ResolvedContext::new(crate::ro_crate::context::RoCrateContext::ReferenceContext(
            "https://w3id.org/ro/crate/1.2/context".to_string(),
        ));

        let value = EntityValue::EntityString("test".to_string());
        let terms = entity_value_to_terms(&value, &ctx).unwrap();
        assert_eq!(terms.len(), 1);
    }

    #[test]
    fn test_entity_value_to_terms_id() {
        let ctx = ResolvedContext::new(crate::ro_crate::context::RoCrateContext::ReferenceContext(
            "https://w3id.org/ro/crate/1.2/context".to_string(),
        ));

        let value = EntityValue::EntityId(Id::Id("http://example.org/entity".to_string()));
        let terms = entity_value_to_terms(&value, &ctx).unwrap();
        assert_eq!(terms.len(), 1);
        assert!(matches!(terms[0], Term::NamedNode(_)));
    }

    // Integration tests

    #[test]
    fn test_rocrate_to_rdf_minimal_1_1() {
        let path = fixture_path("_ro-crate-metadata-minimal.json");
        let crate_ = read_crate(&path, 0).expect("Failed to read crate");

        let graph = rocrate_to_rdf(&crate_, ContextResolverBuilder::default())
            .expect("Failed to convert to RDF");

        // Minimal crate has 3 entities: descriptor, root, license
        // Each entity has at least a type triple
        assert!(!graph.is_empty());
        assert!(graph.len() >= 3, "Expected at least 3 triples, got {}", graph.len());

        // Verify we have rdf:type triples
        let type_triples: Vec<_> = graph.iter()
            .filter(|t| t.predicate.as_str() == RDF_TYPE)
            .collect();
        assert_eq!(type_triples.len(), 3, "Expected 3 type triples (descriptor, root, license)");
    }

    #[test]
    fn test_rocrate_to_rdf_minimal_1_2() {
        let path = fixture_path("_ro-crate-metadata-minimal-1_2.json");
        let crate_ = read_crate(&path, 0).expect("Failed to read crate");

        let graph = rocrate_to_rdf(&crate_, ContextResolverBuilder::default())
            .expect("Failed to convert to RDF");

        assert!(!graph.is_empty());
        assert!(graph.len() >= 3, "Expected at least 3 triples, got {}", graph.len());

        // Verify type triples
        let type_triples: Vec<_> = graph.iter()
            .filter(|t| t.predicate.as_str() == RDF_TYPE)
            .collect();
        assert_eq!(type_triples.len(), 3);
    }

    #[test]
    fn test_rocrate_to_rdf_with_dynamic_properties() {
        let path = fixture_path("_ro-crate-metadata-dynamic.json");
        let crate_ = read_crate(&path, 0).expect("Failed to read crate");

        let graph = rocrate_to_rdf(&crate_, ContextResolverBuilder::default())
            .expect("Failed to convert to RDF");

        assert!(!graph.is_empty());

        // The dynamic fixture has additionalType with array values
        // Verify we got more triples than minimal
        assert!(graph.len() > 3, "Expected more than 3 triples for dynamic crate");
    }

    #[test]
    fn test_rocrate_to_rdf_preserves_iris() {
        let path = fixture_path("_ro-crate-metadata-minimal.json");
        let crate_ = read_crate(&path, 0).expect("Failed to read crate");

        let graph = rocrate_to_rdf(&crate_, ContextResolverBuilder::default())
            .expect("Failed to convert to RDF");

        // Check that schema.org terms are expanded
        let has_schema_org = graph.iter().any(|t| {
            t.predicate.as_str().starts_with("http://schema.org/") ||
            t.predicate.as_str().starts_with("https://schema.org/")
        });
        assert!(has_schema_org, "Expected schema.org predicates to be expanded");
    }

    #[test]
    fn test_rocrate_to_rdf_deduplicates() {
        let path = fixture_path("_ro-crate-metadata-minimal.json");
        let crate_ = read_crate(&path, 0).expect("Failed to read crate");

        let graph = rocrate_to_rdf(&crate_, ContextResolverBuilder::default())
            .expect("Failed to convert to RDF");

        let initial_len = graph.len();

        // Convert again and check same count (no duplicates accumulated)
        let graph2 = rocrate_to_rdf(&crate_, ContextResolverBuilder::default())
            .expect("Failed to convert to RDF");

        assert_eq!(graph2.len(), initial_len, "Triple count should be consistent");
    }

    #[test]
    fn test_rocrate_to_rdf_type_expansion() {
        let path = fixture_path("_ro-crate-metadata-minimal.json");
        let crate_ = read_crate(&path, 0).expect("Failed to read crate");

        let graph = rocrate_to_rdf(&crate_, ContextResolverBuilder::default())
            .expect("Failed to convert to RDF");

        // Find type triples and verify expansion
        let type_triples: Vec<_> = graph.iter()
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
        assert!(has_dataset, "Expected Dataset type to be expanded to schema.org/Dataset");

        // Check CreativeWork type is expanded
        let has_creative_work = type_triples.iter().any(|t| {
            if let Term::NamedNode(n) = &t.object {
                n.as_str().contains("schema.org") && n.as_str().contains("CreativeWork")
            } else {
                false
            }
        });
        assert!(has_creative_work, "Expected CreativeWork type to be expanded to schema.org/CreativeWork");
    }
}
