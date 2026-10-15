//! Workarounds for core library calls that currently panic or have no effect.
//!
//! Each function names the core call it stands in for, so it can be replaced
//! by that call once the core library is fixed.

use rocraters::ro_crate::constraints::{EntityValue, Id};
use rocraters::ro_crate::context::{ContextItem, RoCrateContext};
use rocraters::ro_crate::graph_vector::GraphVector;
use rocraters::ro_crate::rocrate::RoCrate;
use std::collections::HashMap;
use uuid::Uuid;

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
