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

/// Resolves a relative IRI against a base IRI.
///
/// This implements a simplified version of RFC 3986 reference resolution.
fn resolve_relative_iri(base: &str, relative: &str) -> String {
    // Handle fragment-only references: base + #fragment
    if relative.starts_with('#') {
        // Remove any existing fragment from base before appending
        let base_without_fragment = base.split('#').next().unwrap_or(base);
        // Strip trailing slash before fragment (http://example.org/ + #x -> http://example.org#x)
        let base_clean = base_without_fragment.trim_end_matches('/');
        return format!("{}{}", base_clean, relative);
    }

    // Handle "./" (current directory) - returns the base as-is
    if relative == "./" {
        return base.to_string();
    }

    // Handle relative paths starting with "./"
    if relative.starts_with("./") {
        let rel_path = &relative[2..];
        // Remove trailing filename from base if present
        let base_dir = if let Some(last_slash) = base.rfind('/') {
            &base[..=last_slash]
        } else {
            base
        };
        return format!("{}{}", base_dir, rel_path);
    }

    // Handle parent directory references
    if relative.starts_with("../") {
        let mut base_parts: Vec<&str> = base.split('/').collect();
        let mut rel_remaining = relative;

        // Remove the last component (file or empty) from base
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
            return rel_remaining.to_string();
        }
        return format!("{}/{}", base_rebuilt, rel_remaining);
    }

    // For other relative references, append to base directory
    let base_dir = if let Some(last_slash) = base.rfind('/') {
        &base[..=last_slash]
    } else {
        base
    };
    format!("{}{}", base_dir, relative)
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

/// Checks if a term is a relative IRI that needs base resolution.
///
/// A relative IRI is one that:
/// - Starts with `./` (current directory)
/// - Starts with `../` (parent directory)
/// - Starts with `#` (fragment)
/// - Starts with `/` (root-relative)
/// - Contains `/` (path-like)
///
/// Plain terms like `unknownTerm` and `file.txt` are NOT relative IRIs -
/// they should fall through to @vocab resolution.
fn is_relative_iri(term: &str) -> bool {
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
    /// Resolution order:
    /// 1. Direct term mapping
    /// 2. Prefixed term (prefix:local) with known prefix
    /// 3. Absolute IRI with valid scheme (http:, urn:, mailto:, etc.) -> return as-is
    /// 4. For path-like relative IRIs (./file, path/to) -> resolve against @base
    /// 5. For simple terms -> @vocab fallback
    /// 6. For remaining terms -> resolve against @base (treats as relative IRI)
    /// 7. Return unchanged
    pub fn expand_term(&self, term: &str) -> String {
        // 1. Direct term mapping
        if let Some(iri) = self.terms.get(term) {
            return iri.clone();
        }

        // 2. Prefixed term (prefix:local) with known prefix
        if let Some(colon_pos) = term.find(':') {
            let prefix = &term[..colon_pos];
            let local = &term[colon_pos + 1..];

            if let Some(namespace) = self.prefixes.get(prefix) {
                return format!("{}{}", namespace, local);
            }
        }

        // 3. Absolute IRI (now that we've ruled out known prefixes)
        if is_absolute_iri(term) {
            return term.to_string();
        }

        // 4. Path-like relative IRIs -> resolve against @base
        if is_relative_iri(term) {
            if let Some(base) = &self.base {
                return resolve_relative_iri(base, term);
            }
            return term.to_string();
        }

        // 5. Simple terms -> @vocab fallback
        if let Some(vocab) = &self.vocab {
            return format!("{}{}", vocab, term);
        }

        // 6. No @vocab - treat remaining unknown terms as relative IRIs
        // This handles cases like "file.json" which are filenames
        if let Some(base) = &self.base {
            return resolve_relative_iri(base, term);
        }

        // 7. Return unchanged
        term.to_string()
    }

    /// Expands a term to its full IRI with strict validation.
    ///
    /// Unlike `expand_term`, this method returns an error if:
    /// - A relative IRI is encountered and no @base is set
    /// - `allow_relative` is false and the result is still a relative IRI
    ///
    /// # Arguments
    /// * `term` - The term to expand
    /// * `allow_relative` - If true, relative IRIs without a base are allowed (returned as-is)
    ///
    /// # Returns
    /// The expanded IRI or an error if expansion fails
    pub fn expand_term_checked(&self, term: &str, allow_relative: bool) -> Result<String, String> {
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
        if is_relative_iri(term) {
            if let Some(base) = &self.base {
                return Ok(resolve_relative_iri(base, term));
            }
            if !allow_relative {
                return Err(format!(
                    "Cannot resolve relative IRI '{}': no @base defined in context",
                    term
                ));
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
            return Ok(resolve_relative_iri(base, term));
        }

        // 7. No @base available for unknown term
        if !allow_relative {
            return Err(format!(
                "Cannot resolve term '{}': no @vocab or @base defined in context",
                term
            ));
        }

        Ok(term.to_string())
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
    fn test_expand_various_schemes_unchanged() {
        let ctx = test_context();
        // All these should be returned as-is (not expanded)
        assert_eq!(
            ctx.expand_term("mailto:user@example.com"),
            "mailto:user@example.com"
        );
        assert_eq!(ctx.expand_term("tel:+1234567890"), "tel:+1234567890");
        assert_eq!(
            ctx.expand_term("data:text/plain,hello"),
            "data:text/plain,hello"
        );
    }

    #[test]
    fn test_expand_unknown_term_with_base() {
        let ctx = test_context();
        // No @vocab set but @base is set, unknown term resolves against base
        assert_eq!(
            ctx.expand_term("unknownTerm"),
            "http://example.org/crate/unknownTerm"
        );
    }

    #[test]
    fn test_expand_unknown_term_without_base_or_vocab() {
        let ctx = ResolvedContext::new(RoCrateContext::ReferenceContext(
            "https://w3id.org/ro/crate/1.1/context".to_string(),
        ));
        // No @vocab and no @base, unknown term returns unchanged
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

    // Tests for relative IRI resolution

    #[test]
    fn test_resolve_relative_fragment() {
        let base = "http://example.org/crate/";
        // Trailing slash is stripped before fragment
        assert_eq!(
            resolve_relative_iri(base, "#section1"),
            "http://example.org/crate#section1"
        );
    }

    #[test]
    fn test_resolve_relative_fragment_no_trailing_slash() {
        let base = "http://example.org/crate";
        assert_eq!(
            resolve_relative_iri(base, "#section1"),
            "http://example.org/crate#section1"
        );
    }

    #[test]
    fn test_resolve_relative_fragment_replaces_existing() {
        let base = "http://example.org/crate/#old";
        assert_eq!(
            resolve_relative_iri(base, "#new"),
            "http://example.org/crate#new"
        );
    }

    #[test]
    fn test_resolve_relative_current_dir() {
        let base = "http://example.org/crate/";
        assert_eq!(resolve_relative_iri(base, "./"), "http://example.org/crate/");
    }

    #[test]
    fn test_resolve_relative_dot_slash_path() {
        let base = "http://example.org/crate/";
        assert_eq!(
            resolve_relative_iri(base, "./file.txt"),
            "http://example.org/crate/file.txt"
        );
    }

    #[test]
    fn test_resolve_relative_parent_dir() {
        let base = "http://example.org/crate/subdir/";
        assert_eq!(
            resolve_relative_iri(base, "../file.txt"),
            "http://example.org/crate/file.txt"
        );
    }

    #[test]
    fn test_resolve_relative_plain_filename() {
        let base = "http://example.org/crate/";
        assert_eq!(
            resolve_relative_iri(base, "data.csv"),
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
    fn test_is_relative_iri() {
        // Path-like relative IRIs
        assert!(is_relative_iri("./"));
        assert!(is_relative_iri("./file.txt"));
        assert!(is_relative_iri("../parent"));
        assert!(is_relative_iri("#fragment"));
        assert!(is_relative_iri("/absolute/path"));
        assert!(is_relative_iri("path/to/file.txt"));
        assert!(is_relative_iri("subdir/file.txt"));

        // Plain terms are NOT relative IRIs (should go to @vocab)
        assert!(!is_relative_iri("file.txt"));
        assert!(!is_relative_iri("unknownTerm"));
        assert!(!is_relative_iri("Person"));

        // Terms with colons but no slashes are not relative
        assert!(!is_relative_iri("schema:Person"));
        assert!(!is_relative_iri("urn:uuid:123"));

        // Note: is_relative_iri is called AFTER is_absolute_iri in expand logic,
        // so http://example.org would be caught by is_absolute_iri first.
        // is_relative_iri itself just checks for path-like patterns.
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
        assert!(result.unwrap_err().contains("no @base defined"));
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
            ctx.expand_term_checked("http://example.org/thing", false).unwrap(),
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
            ctx.expand_term("./file.txt"),
            "http://example.org/crate/file.txt"
        );
    }
}
