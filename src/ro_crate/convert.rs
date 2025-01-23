// Aim of this module is to enable ro-crate to parquet conversion
//
// The overall structure is as follows (df of strings)
// | urn:uuid | @id | @type | key | value |
//
// This provides poor data compression, but maximal data access and description
// with minimal need for much parsing.
//
// If storage becomes an issue - then that's a good thing and this whole project
// is succeeding
use crate::ro_crate::context::{ContextItem, RoCrateContext};
use crate::ro_crate::rocrate::RoCrate;
use polars::prelude::*;

use super::graph_vector::GraphVector;

pub fn to_df(rocrate: &RoCrate) -> DataFrame {
    // Get uuid
    let uuid = rocrate.context.get_urn_uuid().unwrap();

    // Build the context
    let mut crate_frame = CrateFrame {
        uuid,
        id: Vec::new(),
        etype: Vec::new(),
        key: Vec::new(),
        value: Vec::new(),
    };

    frame_context(&mut crate_frame, &rocrate.context);
    frame_graph(&mut crate_frame, &rocrate);

    let df = DataFrame::new(vec![
        Series::new(
            "uuid".into(),
            vec![crate_frame.uuid.clone(); crate_frame.id.len()],
        )
        .into(),
        Series::new("id".into(), crate_frame.id.clone()).into(),
        Series::new("type".into(), crate_frame.etype.clone()).into(),
        Series::new("key".into(), crate_frame.key.clone()).into(),
        Series::new("value".into(), crate_frame.value.clone()).into(),
    ])
    .unwrap();
    // Iterate through the graph
    df
}

struct CrateFrame {
    uuid: String,
    id: Vec<String>,
    etype: Vec<String>,
    key: Vec<String>,
    value: Vec<String>,
}

impl CrateFrame {
    fn push_data(&mut self, id: &str, etype: &str, key: &str, value: &str) {
        self.id.push(String::from(id));
        self.etype.push(String::from(etype));
        self.key.push(String::from(key));
        self.value.push(String::from(value));
    }
}

/// Converts the RoCrate context to the start rows of the df
fn frame_context(crate_frame: &mut CrateFrame, context: &RoCrateContext) {
    match context {
        RoCrateContext::ExtendedContext(extended) => {
            for x in extended {
                match x {
                    ContextItem::ReferenceItem(reference) => {
                        let id = crate_frame.uuid.clone();
                        crate_frame.push_data(&id, "@context", "ro-crate", reference);
                    }
                    ContextItem::EmbeddedContext(embedded) => {
                        for (key, value) in embedded {
                            let id = crate_frame.uuid.clone();
                            crate_frame.push_data(&id, "@context", key, value);
                        }
                    }
                }
            }
        }
        RoCrateContext::ReferenceContext(reference) => {
            let id = crate_frame.uuid.clone();
            crate_frame.push_data(&id, "@context", "ro-crate", reference);
        }
        RoCrateContext::EmbeddedContext(embedded) => {
            println!("legacy - shouldnt be used")
        }
    }
}

fn frame_graph(crate_frame: &mut CrateFrame, rocrate: &RoCrate) {
    let ids = rocrate.get_all_ids();
    for id in ids {
        let entity = rocrate.get_entity(id).unwrap();
        match entity {
            GraphVector::MetadataDescriptor(data) => {
                let d_id = &data.id;
                let d_type = &data.type_;

                let d_conforms = &data.conforms_to;
                let about = &data.about;

                if let Some(dynamic_entity) = &data.dynamic_entity {
                    for (key, value) in dynamic_entity {
                        println!("dynamic entity: {}:{}", key, value);
                    }
                }
            }
            GraphVector::RootDataEntity(data) => {
                let d_id = &data.id;
                let d_type = &data.type_;

                let d_name = &data.name;
                let d_descrption = &data.description;
                let d_date_published = &data.date_published;
                let d_license = &data.license;

                if let Some(dynamic_entity) = &data.dynamic_entity {
                    for (key, value) in dynamic_entity {
                        println!("dynamic entity: {}:{}", key, value);
                    }
                }
            }
            GraphVector::ContextualEntity(data) => {
                let d_id = &data.id;
                let d_type = &data.type_;
                if let Some(dynamic_entity) = &data.dynamic_entity {
                    for (key, value) in dynamic_entity {
                        println!("dynamic entity: {}:{}", key, value);
                    }
                }
            }
            GraphVector::DataEntity(data) => {
                let d_id = &data.id;
                let d_type = &data.type_;
                if let Some(dynamic_entity) = &data.dynamic_entity {
                    for (key, value) in dynamic_entity {
                        println!("dynamic entity: {}:{}", key, value);
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod write_crate_tests {
    use crate::ro_crate::convert::to_df;
    use crate::ro_crate::read::read_crate;
    use std::path::Path;
    use std::path::PathBuf;

    fn fixture_path(relative_path: &str) -> PathBuf {
        Path::new("tests/fixtures").join(relative_path)
    }

    #[test]
    fn test_create_df() {
        let path = fixture_path("_ro-crate-metadata-dynamic.json");
        let mut rocrate = read_crate(&path, 0).unwrap();
        rocrate.context.add_urn_uuid();
        println!("Crate: {:?}", rocrate);
        let df = to_df(&rocrate);
    }
}
