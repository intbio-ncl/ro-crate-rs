//! Core context types for JSON-LD to RDF conversion.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::ro_crate::context::RoCrateContext;

/// Holds resolved term and prefix mappings from JSON-LD contexts.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResolvedContext {
    /// Map from short term to full IRI (e.g., "name" -> "http://schema.org/name")
    pub terms: HashMap<String, String>,

    /// Map from prefix to namespace (e.g., "schema" -> "http://schema.org/")
    /// Used for prefixed terms like "schema:Person"
    pub prefixes: HashMap<String, String>,

    /// The @base IRI for resolving relative references
    /// If None, relative references are not resolved
    pub base: Option<String>,

    /// The @vocab IRI used as default namespace for unknown terms
    pub vocab: Option<String>,

    /// Original context preserved for roundtrip serialization
    pub original: RoCrateContext,
}

impl ResolvedContext {
    /// Creates a new empty resolved context.
    pub fn new(original: RoCrateContext) -> Self {
        Self {
            terms: HashMap::new(),
            prefixes: HashMap::new(),
            base: None,
            vocab: None,
            original,
        }
    }

    /// Expands a term to its full IRI.
    ///
    /// Resolution order:
    /// 1. Already a full IRI (contains "://") -> return as-is (urn: is a special case and will be catched by 5.)
    /// 2. Direct term mapping
    /// 3. Prefixed term (prefix:local)
    /// 4. @vocab fallback
    /// 5. Return unchanged
    pub fn expand_term(&self, term: &str) -> String {
        // 1. Already a full IRI
        if term.contains("://") {
            return term.to_string();
        }

        // 2. Direct term mapping
        if let Some(iri) = self.terms.get(term) {
            return iri.clone();
        }

        // 3. Prefixed term (prefix:local)
        if let Some(colon_pos) = term.find(':') {
            let prefix = &term[..colon_pos];
            let local = &term[colon_pos + 1..];

            if let Some(namespace) = self.prefixes.get(prefix) {
                return format!("{}{}", namespace, local);
            }
        }

        // 4. @vocab fallback
        if let Some(vocab) = &self.vocab {
            return format!("{}{}", vocab, term);
        }

        // 5. Return unchanged
        term.to_string()
    }

    /// Compacts a full IRI back to a short term.
    ///
    /// Resolution order:
    /// 1. Exact term match
    /// 2. Prefix match (longest namespace wins)
    /// 3. @vocab strip
    /// 4. @base relative
    /// 5. Return unchanged
    pub fn compact_iri(&self, iri: &str) -> String {
        // 1. Exact term match
        for (term, term_iri) in &self.terms {
            if term_iri == iri {
                return term.clone();
            }
        }

        // 2. Prefix match (prefer longer namespace)
        let mut best: Option<(&str, &str, usize)> = None;
        for (prefix, namespace) in &self.prefixes {
            if iri.starts_with(namespace) {
                let len = namespace.len();
                if best.is_none() || len > best.unwrap().2 {
                    best = Some((prefix, namespace, len));
                }
            }
        }
        if let Some((prefix, namespace, _)) = best {
            let local = &iri[namespace.len()..];
            return format!("{}:{}", prefix, local);
        }

        // 3. @vocab strip
        if let Some(vocab) = &self.vocab {
            if iri.starts_with(vocab) {
                return iri[vocab.len()..].to_string();
            }
        }

        // 4. @base relative
        if let Some(base) = &self.base {
            if iri.starts_with(base) {
                let relative = &iri[base.len()..];
                if relative.is_empty() {
                    return "./".to_string();
                }
                return relative.to_string();
            }
        }

        // 5. Return unchanged
        iri.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_context() -> ResolvedContext {
        let mut ctx = ResolvedContext::new(RoCrateContext::ReferenceContext(
            "https://w3id.org/ro/crate/1.1/context".to_string(),
        ));
        ctx.terms
            .insert("name".to_string(), "http://schema.org/name".to_string());
        ctx.terms.insert(
            "description".to_string(),
            "http://schema.org/description".to_string(),
        );
        ctx.prefixes
            .insert("schema".to_string(), "http://schema.org/".to_string());
        ctx.base = Some("http://example.org/crate/".to_string());
        ctx
    }

    #[test]
    fn test_expand_direct_term() {
        let ctx = test_context();
        assert_eq!(ctx.expand_term("name"), "http://schema.org/name");
    }

    #[test]
    fn test_expand_prefixed_term() {
        let ctx = test_context();
        assert_eq!(ctx.expand_term("schema:Person"), "http://schema.org/Person");
    }

    #[test]
    fn test_expand_with_vocab() {
        let mut ctx = test_context();
        ctx.vocab = Some("http://schema.org/".to_string());

        assert_eq!(
            ctx.expand_term("unknownTerm"),
            "http://schema.org/unknownTerm"
        );
    }

    #[test]
    fn test_expand_full_iri_unchanged() {
        let ctx = test_context();
        assert_eq!(
            ctx.expand_term("http://example.org/thing"),
            "http://example.org/thing"
        );
    }

    #[test]
    fn test_expand_urn_unchanged() {
        let ctx = test_context();
        assert_eq!(ctx.expand_term("urn:uuid:123"), "urn:uuid:123");
    }

    #[test]
    fn test_expand_unknown_term_unchanged() {
        let ctx = test_context();
        // No @vocab set, unknown term returns unchanged
        assert_eq!(ctx.expand_term("unknownTerm"), "unknownTerm");
    }

    #[test]
    fn test_compact_exact_match() {
        let ctx = test_context();
        assert_eq!(ctx.compact_iri("http://schema.org/name"), "name");
    }

    #[test]
    fn test_compact_prefix() {
        let ctx = test_context();
        assert_eq!(ctx.compact_iri("http://schema.org/Person"), "schema:Person");
    }

    #[test]
    fn test_compact_base_relative() {
        let ctx = test_context();
        assert_eq!(
            ctx.compact_iri("http://example.org/crate/file.txt"),
            "file.txt"
        );
        assert_eq!(ctx.compact_iri("http://example.org/crate/"), "./");
    }

    #[test]
    fn test_compact_unknown() {
        let ctx = test_context();
        assert_eq!(
            ctx.compact_iri("http://other.org/thing"),
            "http://other.org/thing"
        );
    }
}
