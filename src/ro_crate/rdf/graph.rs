//! RDF graph types for RO-Crate conversion.

use std::collections::HashSet;

use oxrdf::Triple;

use super::context::ResolvedContext;

/// Result of converting an RoCrate to RDF.
///
/// Contains the set of RDF triples and the resolved context used for the conversion.
/// The context is preserved for potential serialization or further processing.
#[derive(Debug)]
pub struct RdfGraph {
    /// The RDF triples. Uses HashSet for automatic deduplication.
    pub triples: HashSet<Triple>,
    /// The resolved context used for the conversion.
    pub context: ResolvedContext,
}

impl RdfGraph {
    /// Creates a new RdfGraph with the given context.
    pub fn new(context: ResolvedContext) -> Self {
        Self {
            triples: HashSet::new(),
            context,
        }
    }

    /// Adds a triple to the graph.
    pub fn insert(&mut self, triple: Triple) {
        self.triples.insert(triple);
    }

    /// Returns the number of triples in the graph.
    pub fn len(&self) -> usize {
        self.triples.len()
    }

    /// Returns true if the graph contains no triples.
    pub fn is_empty(&self) -> bool {
        self.triples.is_empty()
    }

    /// Returns an iterator over the triples.
    pub fn iter(&self) -> impl Iterator<Item = &Triple> {
        self.triples.iter()
    }
}

impl IntoIterator for RdfGraph {
    type Item = Triple;
    type IntoIter = std::collections::hash_set::IntoIter<Triple>;

    fn into_iter(self) -> Self::IntoIter {
        self.triples.into_iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ro_crate::context::RoCrateContext;
    use oxrdf::{Literal, NamedNode};

    #[test]
    fn test_rdf_graph_new() {
        let ctx = ResolvedContext::new(RoCrateContext::ReferenceContext(
            "https://w3id.org/ro/crate/1.2/context".to_string(),
        ));
        let graph = RdfGraph::new(ctx);
        assert!(graph.is_empty());
    }

    #[test]
    fn test_rdf_graph_insert_deduplicates() {
        let ctx = ResolvedContext::new(RoCrateContext::ReferenceContext(
            "https://w3id.org/ro/crate/1.2/context".to_string(),
        ));
        let mut graph = RdfGraph::new(ctx);

        let subject = NamedNode::new_unchecked("http://example.org/subject");
        let predicate = NamedNode::new_unchecked("http://example.org/predicate");
        let object = Literal::new_simple_literal("value");

        let triple = Triple::new(subject.clone(), predicate.clone(), object.clone());
        graph.insert(triple.clone());
        graph.insert(triple.clone()); // Duplicate

        assert_eq!(graph.len(), 1);
    }
}
