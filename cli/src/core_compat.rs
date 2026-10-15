//! Workarounds for core library calls that currently panic or have no effect.
//!
//! Each function names the core call it stands in for, so it can be replaced
//! by that call once the core library is fixed.

use rocraters::ro_crate::constraints::{EntityValue, Id};
use rocraters::ro_crate::context::{ContextItem, RoCrateContext};
use rocraters::ro_crate::graph_vector::GraphVector;
use rocraters::ro_crate::read::{parse_zip, CrateReadError};
use rocraters::ro_crate::rocrate::RoCrate;
use std::collections::HashMap;
use uuid::Uuid;

/// Sets a property on an entity, returning false if there is no such entity.
///
/// Writes any supported entity value into the selected entity.
pub fn set_property(rocrate: &mut RoCrate, id: &str, key: String, value: EntityValue) -> bool {
    let Some(entity) = rocrate.get_entity_mutable(id) else {
        return false;
    };
    let dynamic_entity = match entity {
        GraphVector::DataEntity(entity) => &mut entity.dynamic_entity,
        GraphVector::ContextualEntity(entity) => &mut entity.dynamic_entity,
        GraphVector::RootDataEntity(entity) => &mut entity.dynamic_entity,
        GraphVector::MetadataDescriptor(entity) => &mut entity.dynamic_entity,
    };
    dynamic_entity
        .get_or_insert_with(HashMap::new)
        .insert(key, value);
    true
}

/// Removes a type from an entity.
///
/// `GraphVector::remove_type` returns the reduced type without storing it.
pub fn remove_type(entity: &mut GraphVector, data_type: String) -> Result<(), String> {
    let updated = entity.remove_type(data_type)?;
    match entity {
        GraphVector::DataEntity(entity) => entity.type_ = updated,
        GraphVector::ContextualEntity(entity) => entity.type_ = updated,
        GraphVector::RootDataEntity(entity) => entity.type_ = updated,
        GraphVector::MetadataDescriptor(entity) => entity.type_ = updated,
    }
    Ok(())
}

/// Adds an ID to the root data entity's hasPart.
///
/// `RoCrate::add_data_to_partof_root` looks the ID up as if it were the root, so
/// never finds it, and `RootDataEntity::add_entity_to_partof` replaces a single
/// existing hasPart ID instead of keeping it.
pub fn add_to_root_has_part(rocrate: &mut RoCrate, id: &str) {
    let root = rocrate.graph.iter_mut().find_map(|entity| match entity {
        GraphVector::RootDataEntity(root) => Some(root),
        _ => None,
    });
    let Some(root) = root else {
        return;
    };

    if let Some(dynamic_entity) = &mut root.dynamic_entity {
        if let Some(EntityValue::EntityId(Id::Id(existing))) = dynamic_entity.get("hasPart") {
            let existing = vec![existing.clone()];
            dynamic_entity.insert(
                "hasPart".to_string(),
                EntityValue::EntityId(Id::IdArray(existing)),
            );
        }
    }
    root.add_entity_to_partof(id.to_string());
}

/// Returns the UUID of a urn:uuid @base.
pub fn urn_uuid(context: &RoCrateContext) -> Option<String> {
    context.get_urn_uuid()
}

/// Adds a urn:uuid @base if there is none, returning the UUID, or `None` if the
/// context cannot hold one.
///
/// `RoCrateContext::add_urn_uuid` does nothing for an extended context without
/// embedded terms, and adds an @base to every embedded map that lacks one even
/// when the crate already has a urn.
pub fn add_urn_uuid(context: &mut RoCrateContext) -> Option<String> {
    if let Some(urn) = urn_uuid(context) {
        return Some(urn);
    }
    context.add_urn_uuid();
    if let (None, RoCrateContext::ExtendedContext(items)) = (urn_uuid(context), &mut *context) {
        items.push(ContextItem::EmbeddedContext(HashMap::from([(
            "@base".to_string(),
            format!("urn:uuid:{}", Uuid::now_v7()),
        )])));
    }
    urn_uuid(context)
}

/// Reads a zipped crate.
pub fn parse_zip_checked(path: &str, validation_level: i8) -> Result<RoCrate, CrateReadError> {
    parse_zip(path, validation_level)
}

/// Returns true for IRIs with no path to resolve against, such as urn:uuid:...
#[cfg(feature = "rdf")]
pub fn is_opaque_iri(iri: &str) -> bool {
    url::Url::parse(iri).is_ok_and(|url| url.cannot_be_a_base())
}

/// Rejects an opaque @base before RDF conversion.
///
/// The core RDF resolver panics when resolving a relative IRI against an opaque
/// @base such as urn:uuid:. `fallback` is the base used when the context has none.
#[cfg(feature = "rdf")]
pub fn ensure_hierarchical_base(
    context: &RoCrateContext,
    fallback: Option<&str>,
) -> Result<(), String> {
    let base = match context {
        RoCrateContext::EmbeddedContext(maps) => {
            maps.iter().find_map(|map| map.get("@base").cloned())
        }
        _ => context.get_specific_context("@base"),
    }
    .or(fallback.map(str::to_string));

    match base {
        Some(base) if is_opaque_iri(&base) => Err(format!(
            "Cannot resolve relative IRIs against non-hierarchical @base '{}'",
            base
        )),
        _ => Ok(()),
    }
}
