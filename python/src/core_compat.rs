//! Workarounds for core library calls that currently panic or have no effect.
//!
//! Each function names the core call it stands in for, so it can be replaced
//! by that call once the core library is fixed.

use ::rocraters::ro_crate::context::{ContextItem, RoCrateContext};
use std::collections::HashMap;
use uuid::Uuid;

/// Adds a urn:uuid @base if there is none, returning the UUID, or `None` if the
/// context cannot hold one.
///
/// `RoCrateContext::add_urn_uuid` does nothing for an extended context without
/// embedded terms, and adds an @base to every embedded map that lacks one even
/// when the crate already has a urn.
pub fn add_urn_uuid(context: &mut RoCrateContext) -> Option<String> {
    if let Some(urn) = context.get_urn_uuid() {
        return Some(urn);
    }
    context.add_urn_uuid();
    if let (None, RoCrateContext::ExtendedContext(items)) = (context.get_urn_uuid(), &mut *context)
    {
        items.push(ContextItem::EmbeddedContext(HashMap::from([(
            "@base".to_string(),
            format!("urn:uuid:{}", Uuid::now_v7()),
        )])));
    }
    context.get_urn_uuid()
}
