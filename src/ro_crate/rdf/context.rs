//! Core context types for JSON-LD to RDF conversion.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use url::Url;

use crate::ro_crate::context::RoCrateContext;
use crate::ro_crate::rdf::error::ContextError;

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

/// Resolves a relative IRI against a base IRI.
///
/// Uses the `url` crate for URLs with schemes, falls back to manual
/// resolution for filesystem-like paths.
///
/// # Errors
/// Returns `ContextError::RelativeResolutionError` if the relative path
/// attempts to navigate above the root (more `../` than path segments).
fn resolve_relative_iri(base: &str, relative: &str) -> Result<String, ContextError> {
    // Handle fragment-only references: base + #fragment
    if relative.starts_with('#') {
        let base_without_fragment = base.split('#').next().unwrap_or(base);
        let base_clean = base_without_fragment.trim_end_matches('/');
        return Ok(format!("{}{}", base_clean, relative));
    }

    // Try URL-based resolution for bases with schemes
    if let Ok(base_url) = Url::parse(base) {
        // Validate parent directory references before resolution
        if relative.contains("../") {
            let parent_refs = relative.matches("../").count();
            let path_segments: usize = base_url
                .path_segments()
                .map(|segs| segs.filter(|s| !s.is_empty()).count())
                .unwrap_or(0);

            if parent_refs > path_segments {
                return Err(ContextError::RelativeResolutionError {
                    base: base.to_string(),
                    relative: relative.to_string(),
                });
            }
        }

        // join() should never fail for a valid parsed base URL
        let resolved = base_url
            .join(relative)
            .expect("URL join should succeed for valid base URL");
        return Ok(resolved.to_string());
    }

    // Manual resolution for paths without schemes
    let result = if relative == "./" {
        base.to_string()
    } else if let Some(rel_path) = relative.strip_prefix("./") {
        let base_dir = base.rfind('/').map(|i| &base[..=i]).unwrap_or(base);
        format!("{}{}", base_dir, rel_path)
    } else if relative.starts_with("../") {
        let mut base_parts: Vec<&str> = base.split('/').collect();
        let mut rel_remaining = relative;

        if !base_parts.is_empty() {
            base_parts.pop();
        }

        while rel_remaining.starts_with("../") {
            rel_remaining = &rel_remaining[3..];
            if !base_parts.is_empty() {
                base_parts.pop();
            }
        }

        let base_rebuilt = base_parts.join("/");
        if base_rebuilt.is_empty() {
            rel_remaining.to_string()
        } else {
            format!("{}/{}", base_rebuilt, rel_remaining)
        }
    } else {
        let base_dir = base.rfind('/').map(|i| &base[..=i]).unwrap_or(base);
        format!("{}{}", base_dir, relative)
    };

    // If the result is still relative, we navigated above the root
    if !is_absolute_iri(&result) && !result.starts_with('/') {
        return Err(ContextError::RelativeResolutionError {
            base: base.to_string(),
            relative: relative.to_string(),
        });
    }

    Ok(result)
}

/// Checks if a term is an absolute IRI with a valid URI scheme.
///
/// According to RFC 3986, a URI scheme:
/// - Starts with a letter (a-zA-Z)
/// - Followed by any combination of letters, digits, plus (+), hyphen (-), or period (.)
/// - Ends with a colon (:)
///
/// Examples: `http://`, `https://`, `urn:`, `mailto:`, `file://`, `data:`
fn is_absolute_iri(term: &str) -> bool {
    let Some(colon_pos) = term.find(':') else {
        return false;
    };

    // Empty scheme is invalid
    if colon_pos == 0 {
        return false;
    }

    let scheme = &term[..colon_pos];
    let mut chars = scheme.chars();

    // First character must be a letter
    let Some(first) = chars.next() else {
        return false;
    };
    if !first.is_ascii_alphabetic() {
        return false;
    }

    // Rest must be letters, digits, +, -, or .
    chars.all(|c| c.is_ascii_alphanumeric() || c == '+' || c == '-' || c == '.')
}

/// Checks if a term has a relative path component that needs base resolution.
///
/// Returns true for:
/// - `./` (current directory)
/// - `../` (parent directory)
/// - `#` (fragment)
/// - `/` (root-relative or contains path segments)
///
/// Plain terms like `unknownTerm` and `file.txt` return false -
/// they should fall through to @vocab resolution.
fn has_relative_component(term: &str) -> bool {
    if is_absolute_iri(term) {
        return false;
    }
    term.starts_with("./")
        || term.starts_with("../")
        || term.starts_with('#')
        || term.starts_with('/')
        || term.contains('/')
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
    /// Prefers explicit term/prefix mappings, then absolute IRIs, then @base/@vocab.
    /// Returns `ContextError::RelativeResolutionError` if resolution would
    /// navigate above the root.
    pub fn expand_term(&self, term: &str) -> Result<String, ContextError> {
        // 1. Direct term mapping
        if let Some(iri) = self.terms.get(term) {
            return Ok(iri.clone());
        }

        // 2. Prefixed term (prefix:local) with known prefix
        if let Some(colon_pos) = term.find(':') {
            let prefix = &term[..colon_pos];
            let local = &term[colon_pos + 1..];

            if let Some(namespace) = self.prefixes.get(prefix) {
                return Ok(format!("{}{}", namespace, local));
            }
        }

        // 3. Absolute IRI (now that we've ruled out known prefixes)
        if is_absolute_iri(term) {
            return Ok(term.to_string());
        }

        // 4. Path-like relative IRIs -> resolve against @base
        if has_relative_component(term) {
            if let Some(base) = &self.base {
                return resolve_relative_iri(base, term);
            }
            return Ok(term.to_string());
        }

        // 5. Simple terms -> @vocab fallback
        if let Some(vocab) = &self.vocab {
            return Ok(format!("{}{}", vocab, term));
        }

        // 6. No @vocab - treat remaining unknown terms as relative IRIs
        // This handles cases like "file.json" which are filenames
        if let Some(base) = &self.base {
            return resolve_relative_iri(base, term);
        }

        // 7. Return unchanged
        Ok(term.to_string())
    }

    /// Expands a term to its full IRI with strict validation.
    ///
    /// Errors if a relative IRI is encountered without @base and `allow_relative`
    /// is false, or if the result remains relative.
    pub fn expand_term_checked(
        &self,
        term: &str,
        allow_relative: bool,
    ) -> Result<String, ContextError> {
        // 1. Direct term mapping
        if let Some(iri) = self.terms.get(term) {
            return Ok(iri.clone());
        }

        // 2. Prefixed term (prefix:local) with known prefix
        if let Some(colon_pos) = term.find(':') {
            let prefix = &term[..colon_pos];
            let local = &term[colon_pos + 1..];

            if let Some(namespace) = self.prefixes.get(prefix) {
                return Ok(format!("{}{}", namespace, local));
            }
        }

        // 3. Absolute IRI (now that we've ruled out known prefixes)
        if is_absolute_iri(term) {
            return Ok(term.to_string());
        }

        // 4. Path-like relative IRIs -> resolve against @base
        if has_relative_component(term) {
            if let Some(base) = &self.base {
                return resolve_relative_iri(base, term);
            }
            if !allow_relative {
                return Err(ContextError::InvalidContext(format!(
                    "Cannot resolve relative IRI '{}': no @base defined in context",
                    term
                )));
            }
            return Ok(term.to_string());
        }

        // 5. Simple terms -> @vocab fallback
        if let Some(vocab) = &self.vocab {
            return Ok(format!("{}{}", vocab, term));
        }

        // 6. No @vocab - treat remaining unknown terms as relative IRIs
        // This handles cases like "file.json" which are filenames
        if let Some(base) = &self.base {
            return resolve_relative_iri(base, term);
        }

        // 7. No @base available for unknown term
        if !allow_relative {
            return Err(ContextError::InvalidContext(format!(
                "Cannot resolve term '{}': no @vocab or @base defined in context",
                term
            )));
        }

        Ok(term.to_string())
    }

    /// Compacts a full IRI back to a short term, using exact term/prefix matches
    /// first, then @vocab/@base when possible.
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
                if best.map_or(true, |(_, _, best_len)| len > best_len) {
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
            // Handle fragments: base might have trailing slash but IRI has fragment without it
            // e.g., base="http://example.org/crate/" and iri="http://example.org/crate#section"
            // This is the inverse of resolve_relative_iri which strips trailing slash before #
            let base_no_slash = base.trim_end_matches('/');
            if let Some(fragment_part) = iri.strip_prefix(base_no_slash) {
                if fragment_part.starts_with('#') {
                    return fragment_part.to_string(); // Returns "#section"
                }
            }

            // Original logic for non-fragment cases
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
        assert_eq!(ctx.expand_term("name").unwrap(), "http://schema.org/name");
    }

    #[test]
    fn test_expand_prefixed_term() {
        let ctx = test_context();
        assert_eq!(
            ctx.expand_term("schema:Person").unwrap(),
            "http://schema.org/Person"
        );
    }

    #[test]
    fn test_expand_with_vocab() {
        let mut ctx = test_context();
        ctx.vocab = Some("http://schema.org/".to_string());

        assert_eq!(
            ctx.expand_term("unknownTerm").unwrap(),
            "http://schema.org/unknownTerm"
        );
    }

    #[test]
    fn test_expand_full_iri_unchanged() {
        let ctx = test_context();
        assert_eq!(
            ctx.expand_term("http://example.org/thing").unwrap(),
            "http://example.org/thing"
        );
    }

    #[test]
    fn test_expand_urn_unchanged() {
        let ctx = test_context();
        assert_eq!(ctx.expand_term("urn:uuid:123").unwrap(), "urn:uuid:123");
    }

    #[test]
    fn test_expand_various_schemes_unchanged() {
        let ctx = test_context();
        // All these should be returned as-is (not expanded)
        assert_eq!(
            ctx.expand_term("mailto:user@example.com").unwrap(),
            "mailto:user@example.com"
        );
        assert_eq!(
            ctx.expand_term("tel:+1234567890").unwrap(),
            "tel:+1234567890"
        );
        assert_eq!(
            ctx.expand_term("data:text/plain,hello").unwrap(),
            "data:text/plain,hello"
        );
    }

    #[test]
    fn test_expand_unknown_term_with_base() {
        let ctx = test_context();
        // No @vocab set but @base is set, unknown term resolves against base
        assert_eq!(
            ctx.expand_term("unknownTerm").unwrap(),
            "http://example.org/crate/unknownTerm"
        );
    }

    #[test]
    fn test_expand_unknown_term_without_base_or_vocab() {
        let ctx = ResolvedContext::new(RoCrateContext::ReferenceContext(
            "https://w3id.org/ro/crate/1.1/context".to_string(),
        ));
        // No @vocab and no @base, unknown term returns unchanged
        assert_eq!(ctx.expand_term("unknownTerm").unwrap(), "unknownTerm");
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
    fn test_compact_fragment_with_trailing_slash_base() {
        // Test fragment compaction when base has trailing slash
        // This is the inverse of resolve_relative_iri which strips trailing slash before #
        // base="http://example.org/crate/" but expanded IRI is "http://example.org/crate#section"
        let ctx = test_context();
        assert_eq!(
            ctx.compact_iri("http://example.org/crate#section"),
            "#section"
        );
    }

    #[test]
    fn test_fragment_roundtrip() {
        // Verify that fragments roundtrip correctly with trailing slash base
        let ctx = test_context();
        let original = "#section";
        let expanded = ctx.expand_term(original).unwrap();
        assert_eq!(expanded, "http://example.org/crate#section");
        let compacted = ctx.compact_iri(&expanded);
        assert_eq!(compacted, original);
    }

    #[test]
    fn test_compact_unknown() {
        let ctx = test_context();
        assert_eq!(
            ctx.compact_iri("http://other.org/thing"),
            "http://other.org/thing"
        );
    }

    // Tests for relative IRI resolution

    #[test]
    fn test_resolve_relative_fragment() {
        let base = "http://example.org/crate/";
        // Trailing slash is stripped before fragment
        assert_eq!(
            resolve_relative_iri(base, "#section1").unwrap(),
            "http://example.org/crate#section1"
        );
    }

    #[test]
    fn test_resolve_relative_fragment_no_trailing_slash() {
        let base = "http://example.org/crate";
        assert_eq!(
            resolve_relative_iri(base, "#section1").unwrap(),
            "http://example.org/crate#section1"
        );
    }

    #[test]
    fn test_resolve_relative_fragment_replaces_existing() {
        let base = "http://example.org/crate/#old";
        assert_eq!(
            resolve_relative_iri(base, "#new").unwrap(),
            "http://example.org/crate#new"
        );
    }

    #[test]
    fn test_resolve_relative_current_dir() {
        let base = "http://example.org/crate/";
        assert_eq!(
            resolve_relative_iri(base, "./").unwrap(),
            "http://example.org/crate/"
        );
    }

    #[test]
    fn test_resolve_relative_dot_slash_path() {
        let base = "http://example.org/crate/";
        assert_eq!(
            resolve_relative_iri(base, "./file.txt").unwrap(),
            "http://example.org/crate/file.txt"
        );
    }

    #[test]
    fn test_resolve_relative_parent_dir() {
        let base = "http://example.org/crate/subdir/";
        assert_eq!(
            resolve_relative_iri(base, "../file.txt").unwrap(),
            "http://example.org/crate/file.txt"
        );
    }

    #[test]
    fn test_resolve_relative_plain_filename() {
        let base = "http://example.org/crate/";
        assert_eq!(
            resolve_relative_iri(base, "data.csv").unwrap(),
            "http://example.org/crate/data.csv"
        );
    }

    #[test]
    fn test_is_absolute_iri() {
        // Standard schemes with authority
        assert!(is_absolute_iri("http://example.org"));
        assert!(is_absolute_iri("https://example.org/path"));
        assert!(is_absolute_iri("ftp://server.com"));
        assert!(is_absolute_iri("file:///path/to/file"));

        // URN schemes (no //)
        assert!(is_absolute_iri("urn:uuid:123-456"));
        assert!(is_absolute_iri("urn:isbn:0451450523"));

        // Other schemes without //
        assert!(is_absolute_iri("mailto:user@example.com"));
        assert!(is_absolute_iri("tel:+1234567890"));
        assert!(is_absolute_iri("data:text/plain;base64,SGVsbG8="));

        // Schemes with special chars (+, -, .)
        assert!(is_absolute_iri("coap+tcp://server.com"));
        assert!(is_absolute_iri("h2c://example.org"));

        // Prefixed terms look like schemes but are handled separately
        assert!(is_absolute_iri("schema:Person")); // Looks valid, but prefix check comes first

        // Not absolute IRIs
        assert!(!is_absolute_iri("./file.txt"));
        assert!(!is_absolute_iri("../parent"));
        assert!(!is_absolute_iri("#fragment"));
        assert!(!is_absolute_iri("/absolute/path"));
        assert!(!is_absolute_iri("path/to/file"));
        assert!(!is_absolute_iri("file.txt"));
        assert!(!is_absolute_iri("unknownTerm"));
        assert!(!is_absolute_iri(":invalid")); // Empty scheme
        assert!(!is_absolute_iri("123:invalid")); // Scheme must start with letter
    }

    #[test]
    fn test_has_relative_component() {
        // Path-like relative IRIs
        assert!(has_relative_component("./"));
        assert!(has_relative_component("./file.txt"));
        assert!(has_relative_component("../parent"));
        assert!(has_relative_component("#fragment"));
        assert!(has_relative_component("/absolute/path"));
        assert!(has_relative_component("path/to/file.txt"));
        assert!(has_relative_component("subdir/file.txt"));

        // Plain terms are NOT relative IRIs (should go to @vocab)
        assert!(!has_relative_component("file.txt"));
        assert!(!has_relative_component("unknownTerm"));
        assert!(!has_relative_component("Person"));

        // Terms with colons but no slashes are not relative
        assert!(!has_relative_component("schema:Person"));
        assert!(!has_relative_component("urn:uuid:123"));

        // Note: has_relative_component is called AFTER is_absolute_iri in expand logic,
        // so http://example.org would be caught by is_absolute_iri first.
        // has_relative_component itself just checks for path-like patterns.
    }

    // Tests for expand_term_checked

    #[test]
    fn test_expand_term_checked_with_base() {
        let ctx = test_context();
        assert_eq!(
            ctx.expand_term_checked("./file.txt", false).unwrap(),
            "http://example.org/crate/file.txt"
        );
    }

    #[test]
    fn test_expand_term_checked_no_base_strict() {
        let mut ctx = test_context();
        ctx.base = None;
        let result = ctx.expand_term_checked("./file.txt", false);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("no @base defined"));
    }

    #[test]
    fn test_expand_term_checked_no_base_allow_relative() {
        let mut ctx = test_context();
        ctx.base = None;
        assert_eq!(
            ctx.expand_term_checked("./file.txt", true).unwrap(),
            "./file.txt"
        );
    }

    #[test]
    fn test_expand_term_checked_absolute_iri() {
        let ctx = test_context();
        assert_eq!(
            ctx.expand_term_checked("http://example.org/thing", false)
                .unwrap(),
            "http://example.org/thing"
        );
    }

    #[test]
    fn test_expand_term_checked_urn() {
        let ctx = test_context();
        assert_eq!(
            ctx.expand_term_checked("urn:uuid:123-456", false).unwrap(),
            "urn:uuid:123-456"
        );
    }

    #[test]
    fn test_expand_relative_with_base() {
        let ctx = test_context();
        // expand_term should also resolve relative IRIs when base is available
        assert_eq!(
            ctx.expand_term("./file.txt").unwrap(),
            "http://example.org/crate/file.txt"
        );
    }

    #[test]
    fn test_resolve_relative_above_root_error() {
        let base = "http://example.org/crate/";
        let result = resolve_relative_iri(base, "../../../file.txt");
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            ContextError::RelativeResolutionError { .. }
        ));
    }
}
