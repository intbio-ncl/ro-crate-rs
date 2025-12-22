//! Context resolution for JSON-LD to RDF conversion.
//!
//! # Example
//!
//! ```ignore
//! let resolved = ContextResolverBuilder::default()
//!     .resolve(&rocrate.context)?;
//! ```

use std::collections::{HashMap, HashSet};

use crate::ro_crate::context::{ContextItem, RoCrateContext};
use crate::ro_crate::schema::{RoCrateSchemaVersion, ROCRATE_SCHEMA_1_1, ROCRATE_SCHEMA_1_2};

use super::context::ResolvedContext;
use super::error::ContextError;

/// URL for the RO-Crate 1.1 JSON-LD context.
pub const ROCRATE_1_1_CONTEXT_URL: &str = "https://w3id.org/ro/crate/1.1/context";

/// URL for the RO-Crate 1.2 JSON-LD context.
pub const ROCRATE_1_2_CONTEXT_URL: &str = "https://w3id.org/ro/crate/1.2/context";

/// Builder for resolving RO-Crate contexts.
///
/// Accumulates pre-cached contexts and configuration, then resolves
/// an `RoCrateContext` into a `ResolvedContext` in a single pass.
/// The RO-Crate version (1.1 or 1.2) is auto-detected from the context URL,
/// defaulting to 1.2 if not specified.
///
/// # Example
///
/// ```ignore
/// // Auto-detects RO-Crate version from context
/// let resolved = ContextResolverBuilder::default()
///     .resolve(&rocrate.context)?;
///
/// // Or add custom contexts
/// let resolved = ContextResolverBuilder::new()
///     .with_context("https://custom.org/ctx", custom_json)?
///     .resolve(&rocrate.context)?;
/// ```
pub struct ContextResolverBuilder {
    cache: HashMap<String, CachedContext>,
    allow_remote: bool,
    client: reqwest::blocking::Client,
}

#[derive(Clone)]
struct CachedContext {
    terms: HashMap<String, String>,
    prefixes: HashMap<String, String>,
    nested_urls: Vec<String>,
}

impl Default for ContextResolverBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl ContextResolverBuilder {
    /// Creates an empty builder with no pre-cached contexts.
    pub fn new() -> Self {
        Self {
            cache: HashMap::new(),
            allow_remote: false,
            client: reqwest::blocking::Client::builder()
                .user_agent("ro-crate-rs/0.4")
                .timeout(std::time::Duration::from_secs(30))
                .build()
                .expect("Failed to create HTTP client"),
        }
    }

    /// Adds a context from JSON, associated with the given URL.
    pub fn with_context(mut self, url: &str, json: &str) -> Result<Self, ContextError> {
        let cached = parse_context_json(json)?;
        self.cache.insert(url.to_string(), cached);
        Ok(self)
    }

    /// Sets whether remote context fetching is allowed.
    pub fn allow_remote(mut self, allow: bool) -> Self {
        self.allow_remote = allow;
        self
    }

    /// Resolves an RO-Crate context into a `ResolvedContext`.
    ///
    /// Auto-detects the RO-Crate version from the context URL and preloads
    /// the appropriate schema. Defaults to 1.2 if no version is detected.
    ///
    /// Consumes the builder and returns the resolved context.
    pub fn resolve(mut self, context: &RoCrateContext) -> Result<ResolvedContext, ContextError> {
        // Auto-detect and preload the appropriate RO-Crate schema
        self.preload_rocrate_schema(context);

        let mut resolved = ResolvedContext::new(context.clone());
        let mut visited = HashSet::new();

        self.process_context(context, &mut resolved, &mut visited, true)?;

        Ok(resolved)
    }

    /// Auto-detects the RO-Crate version from context URLs and preloads the schema.
    fn preload_rocrate_schema(&mut self, context: &RoCrateContext) {
        let version = context.get_schema_version().unwrap_or(RoCrateSchemaVersion::V1_2);

        match version {
            RoCrateSchemaVersion::V1_1 => {
                self.cache
                    .entry(ROCRATE_1_1_CONTEXT_URL.to_string())
                    .or_insert_with(|| parse_context_json(ROCRATE_SCHEMA_1_1).unwrap());
            }
            RoCrateSchemaVersion::V1_2 => {
                self.cache
                    .entry(ROCRATE_1_2_CONTEXT_URL.to_string())
                    .or_insert_with(|| parse_context_json(ROCRATE_SCHEMA_1_2).unwrap());
            }
        }
    }

    fn process_context(
        &mut self,
        context: &RoCrateContext,
        resolved: &mut ResolvedContext,
        visited: &mut HashSet<String>,
        is_root: bool,
    ) -> Result<(), ContextError> {
        match context {
            RoCrateContext::ReferenceContext(url) => {
                self.process_remote_urls(url, resolved, visited)?;
            }
            RoCrateContext::ExtendedContext(items) => {
                for item in items {
                    match item {
                        ContextItem::ReferenceItem(url) => {
                            self.process_remote_urls(url, resolved, visited)?;
                        }
                        ContextItem::EmbeddedContext(map) => {
                            self.process_embedded(map, resolved, is_root);
                        }
                    }
                }
            }
            RoCrateContext::EmbeddedContext(maps) => {
                for map in maps {
                    self.process_embedded(map, resolved, is_root);
                }
            }
        }
        Ok(())
    }

    fn process_remote_urls(
        &mut self,
        start_url: &str,
        resolved: &mut ResolvedContext,
        visited: &mut HashSet<String>,
    ) -> Result<(), ContextError> {
        let mut queue = vec![start_url.to_string()];
        let mut to_apply: Vec<CachedContext> = Vec::new();

        // Collect all contexts breadth-first
        while let Some(url) = queue.pop() {
            if visited.contains(&url) {
                continue;
            }
            visited.insert(url.clone());

            let cached = self.get_or_fetch(&url)?;

            // Queue nested contexts
            for nested_url in &cached.nested_urls {
                if !visited.contains::<String>(nested_url) {
                    queue.push(nested_url.clone());
                }
            }

            to_apply.push(cached);
        }

        // Apply in reverse order (base contexts first, then extensions)
        for cached in to_apply.into_iter().rev() {
            for (term, iri) in cached.terms {
                resolved.terms.insert(term, iri);
            }
            for (prefix, namespace) in cached.prefixes {
                resolved.prefixes.insert(prefix, namespace);
            }
        }

        Ok(())
    }

    fn get_or_fetch(&mut self, url: &str) -> Result<CachedContext, ContextError> {
        if let Some(cached) = self.cache.get(url) {
            return Ok(cached.clone());
        }

        if !self.allow_remote {
            return Err(ContextError::MissingContext(url.to_string()));
        }

        let cached = self.fetch_remote(url)?;
        self.cache.insert(url.to_string(), cached.clone());
        Ok(cached)
    }

    fn fetch_remote(&self, url: &str) -> Result<CachedContext, ContextError> {
        let response = self
            .client
            .get(url)
            .header("Accept", "application/ld+json, application/json")
            .send()
            .map_err(|e| ContextError::FetchFailed {
                url: url.to_string(),
                reason: e.to_string(),
            })?;

        if !response.status().is_success() {
            return Err(ContextError::FetchFailed {
                url: url.to_string(),
                reason: format!("HTTP {}", response.status()),
            });
        }

        let body = response.text().map_err(|e| ContextError::FetchFailed {
            url: url.to_string(),
            reason: e.to_string(),
        })?;

        parse_context_json(&body)
    }

    fn process_embedded(
        &self,
        map: &HashMap<String, String>,
        resolved: &mut ResolvedContext,
        is_root: bool,
    ) {
        for (key, value) in map {
            match key.as_str() {
                "@base" if is_root => resolved.base = Some(value.clone()),
                "@vocab" if is_root => resolved.vocab = Some(value.clone()),
                // Handle other keywords, e.g. @type, @context, etc.
                _ if key.starts_with('@') => {}
                _ => {
                    if value.ends_with('/') || value.ends_with('#') {
                        resolved.prefixes.insert(key.clone(), value.clone());
                    }
                    resolved.terms.insert(key.clone(), value.clone());
                }
            }
        }
    }
}

fn parse_context_json(json: &str) -> Result<CachedContext, ContextError> {
    let value: serde_json::Value =
        serde_json::from_str(json).map_err(|e| ContextError::JsonParseError(e.to_string()))?;

    let context_value = value.get("@context").unwrap_or(&value);
    parse_context_value(context_value)
}

fn parse_context_value(value: &serde_json::Value) -> Result<CachedContext, ContextError> {
    let mut cached = CachedContext {
        terms: HashMap::new(),
        prefixes: HashMap::new(),
        nested_urls: Vec::new(),
    };

    match value {
        serde_json::Value::String(url) => {
            cached.nested_urls.push(url.clone());
        }
        serde_json::Value::Array(items) => {
            for item in items {
                let nested = parse_context_value(item)?;
                cached.terms.extend(nested.terms);
                cached.prefixes.extend(nested.prefixes);
                cached.nested_urls.extend(nested.nested_urls);
            }
        }
        serde_json::Value::Object(map) => {
            for (key, val) in map {
                parse_context_entry(key, val, &mut cached);
            }
        }
        _ => {
            return Err(ContextError::InvalidContext(
                "Context must be string, array, or object".to_string(),
            ));
        }
    }

    Ok(cached)
}

fn parse_context_entry(key: &str, value: &serde_json::Value, cached: &mut CachedContext) {
    if key.starts_with('@') {
        return;
    }

    let iri = match value {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Object(map) => match map.get("@id").and_then(|v| v.as_str()) {
            Some(s) => s.to_string(),
            None => return,
        },
        _ => return,
    };

    if iri.ends_with('/') || iri.ends_with('#') {
        cached.prefixes.insert(key.to_string(), iri.clone());
    }
    cached.terms.insert(key.to_string(), iri);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_is_empty() {
        let builder = ContextResolverBuilder::new();
        assert!(builder.cache.is_empty());
    }

    #[test]
    fn test_auto_detect_1_1_context() {
        let context = RoCrateContext::ReferenceContext(ROCRATE_1_1_CONTEXT_URL.to_string());
        let resolved = ContextResolverBuilder::default().resolve(&context).unwrap();

        assert!(resolved.terms.contains_key("name"));
        assert!(resolved.terms.contains_key("description"));
        assert!(resolved.terms.contains_key("author"));
        assert!(resolved.terms.contains_key("hasPart"));
    }

    #[test]
    fn test_auto_detect_1_2_context() {
        let context = RoCrateContext::ReferenceContext(ROCRATE_1_2_CONTEXT_URL.to_string());
        let resolved = ContextResolverBuilder::default().resolve(&context).unwrap();

        assert!(resolved.terms.contains_key("name"));
        assert!(resolved.terms.contains_key("description"));
    }

    #[test]
    fn test_default_to_1_2_when_no_version() {
        // Use a custom embedded context with no RO-Crate URL
        let mut embedded = HashMap::new();
        embedded.insert("customTerm".to_string(), "http://example.org/custom".to_string());

        let context = RoCrateContext::EmbeddedContext(vec![embedded]);
        // This should succeed because 1.2 is preloaded as default
        let resolved = ContextResolverBuilder::default().resolve(&context).unwrap();

        assert!(resolved.terms.contains_key("customTerm"));
    }

    #[test]
    fn test_resolve_extended_with_base() {
        let mut embedded = HashMap::new();
        embedded.insert("@base".to_string(), "urn:uuid:1234-5678".to_string());
        embedded.insert(
            "customTerm".to_string(),
            "http://example.org/custom".to_string(),
        );

        let context = RoCrateContext::ExtendedContext(vec![
            ContextItem::ReferenceItem(ROCRATE_1_1_CONTEXT_URL.to_string()),
            ContextItem::EmbeddedContext(embedded),
        ]);

        let resolved = ContextResolverBuilder::default().resolve(&context).unwrap();

        assert_eq!(resolved.base, Some("urn:uuid:1234-5678".to_string()));
        assert!(resolved.terms.contains_key("name"));
        assert!(resolved.terms.contains_key("customTerm"));
    }

    #[test]
    fn test_remote_does_not_override_base_vocab() {
        let remote_json = r#"{
            "@context": {
                "@base": "http://remote.org/",
                "@vocab": "http://remote.org/vocab#",
                "remoteTerm": "http://remote.org/term"
            }
        }"#;

        let mut embedded = HashMap::new();
        embedded.insert("@base".to_string(), "urn:uuid:local".to_string());

        let context = RoCrateContext::ExtendedContext(vec![
            ContextItem::ReferenceItem("http://example.org/remote".to_string()),
            ContextItem::EmbeddedContext(embedded),
        ]);

        let resolved = ContextResolverBuilder::new()
            .with_context("http://example.org/remote", remote_json)
            .unwrap()
            .resolve(&context)
            .unwrap();

        assert_eq!(resolved.base, Some("urn:uuid:local".to_string()));
        assert!(resolved.vocab.is_none());
        assert!(resolved.terms.contains_key("remoteTerm"));
    }

    #[test]
    fn test_missing_context_error() {
        let context = RoCrateContext::ReferenceContext("http://unknown.org/ctx".to_string());
        let result = ContextResolverBuilder::new().resolve(&context);

        assert!(matches!(result, Err(ContextError::MissingContext(_))));
    }

    #[test]
    fn test_circular_reference_skipped() {
        let ctx_a = r#"{"@context": ["http://example.org/b", {"a": "http://a.org/a"}]}"#;
        let ctx_b = r#"{"@context": ["http://example.org/a", {"b": "http://b.org/b"}]}"#;

        let context = RoCrateContext::ReferenceContext("http://example.org/a".to_string());

        let resolved = ContextResolverBuilder::new()
            .with_context("http://example.org/a", ctx_a)
            .unwrap()
            .with_context("http://example.org/b", ctx_b)
            .unwrap()
            .resolve(&context)
            .unwrap();

        assert!(resolved.terms.contains_key("a"));
        assert!(resolved.terms.contains_key("b"));
    }

    #[test]
    fn test_nested_contexts() {
        let base = r#"{"@context": {"baseTerm": "http://base.org/term"}}"#;
        let extended =
            r#"{"@context": ["http://example.org/base", {"extTerm": "http://ext.org/term"}]}"#;

        let context = RoCrateContext::ReferenceContext("http://example.org/extended".to_string());

        let resolved = ContextResolverBuilder::new()
            .with_context("http://example.org/base", base)
            .unwrap()
            .with_context("http://example.org/extended", extended)
            .unwrap()
            .resolve(&context)
            .unwrap();

        assert!(resolved.terms.contains_key("baseTerm"));
        assert!(resolved.terms.contains_key("extTerm"));
    }

    #[test]
    fn test_expand_term_direct() {
        let context = RoCrateContext::ReferenceContext(ROCRATE_1_1_CONTEXT_URL.to_string());
        let resolved = ContextResolverBuilder::default().resolve(&context).unwrap();

        assert_eq!(resolved.expand_term("name"), "http://schema.org/name");
    }

    #[test]
    fn test_expand_term_prefix() {
        let context = RoCrateContext::ReferenceContext(ROCRATE_1_1_CONTEXT_URL.to_string());
        let resolved = ContextResolverBuilder::default().resolve(&context).unwrap();

        let expanded = resolved.expand_term("schema:Person");
        assert!(expanded.contains("Person"));
    }

    #[test]
    fn test_expand_term_vocab_fallback() {
        let mut embedded = HashMap::new();
        embedded.insert("@vocab".to_string(), "http://schema.org/".to_string());

        let context = RoCrateContext::ExtendedContext(vec![ContextItem::EmbeddedContext(embedded)]);

        let resolved = ContextResolverBuilder::new().resolve(&context).unwrap();

        assert_eq!(
            resolved.expand_term("unknownTerm"),
            "http://schema.org/unknownTerm"
        );
    }

    #[test]
    fn test_expand_term_full_iri_unchanged() {
        let context = RoCrateContext::ReferenceContext(ROCRATE_1_1_CONTEXT_URL.to_string());
        let resolved = ContextResolverBuilder::default().resolve(&context).unwrap();

        assert_eq!(
            resolved.expand_term("http://other.org/x"),
            "http://other.org/x"
        );
        assert_eq!(resolved.expand_term("urn:uuid:123"), "urn:uuid:123");
    }
}
