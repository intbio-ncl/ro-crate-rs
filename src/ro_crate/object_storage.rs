use log::debug;

use crate::ro_crate::rocrate::RoCrate;
use crate::ro_crate::write::is_not_url;

use super::graph_vector::GraphVector;

/// For use once zipped crates are uploaded to object store.
/// Presumes all files have been made relative to the root, and prefixes
/// the new root object store path
pub fn relative_to_object_store(rocrate: &mut RoCrate, object_root: &str) {
    for entity in &mut rocrate.graph {
        match entity {
            GraphVector::DataEntity(ref mut entity) => {
                if is_not_url(&entity.id) {
                    let root = object_root.to_string();
                    entity.id = root + &entity.id
                };
            }
            GraphVector::ContextualEntity(ref mut entity) => {
                if is_not_url(&entity.id) {
                    let root = object_root.to_string();
                    entity.id = root + &entity.id
                };
            }
            GraphVector::RootDataEntity(ref mut entity) => {
                if is_not_url(&entity.id) {
                    let root = object_root.to_string();
                    entity.id = root + &entity.id
                };
            }
            GraphVector::MetadataDescriptor(ref mut entity) => {
                if is_not_url(&entity.id) {
                    let root = object_root.to_string();
                    entity.id = root + &entity.id
                };
                debug!("Entity ID => {} ", entity);
            }
        }
    }
}

#[cfg(test)]
mod write_crate_tests {
    use crate::ro_crate::read::read_crate;
    use std::path::Path;
    use std::path::PathBuf;

    use super::relative_to_object_store;

    fn fixture_path(relative_path: &str) -> PathBuf {
        Path::new("tests/fixtures").join(relative_path)
    }

    #[test]
    fn test_rel_to_obj() {
        let path = fixture_path("_ro-crate-metadata-relative.json");
        let mut rocrate = read_crate(&path, 0).unwrap();
        rocrate.context.add_urn_uuid();
        relative_to_object_store(&mut rocrate, "gcs://test_object/namespace/");
        println!("End crate is now: {}", rocrate);
    }
}
