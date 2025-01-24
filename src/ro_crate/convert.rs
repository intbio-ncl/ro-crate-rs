// Aim of this module is to enable ro-crate to parquet conversion
//
// The overall structure is as follows (df of strings)
// | urn:uuid | @id | key | value |
//
// This provides poor data compression, but maximal data access and description
// with minimal need for much parsing.
//
// If storage becomes an issue - then that's a good thing and this whole project
// is succeeding
use crate::ro_crate::constraints::{DataType, EntityValue, Id, License};
use crate::ro_crate::context::{ContextItem, RoCrateContext};
use crate::ro_crate::graph_vector::GraphVector;
use crate::ro_crate::rocrate::RoCrate;
use polars::prelude::*;
use std::collections::HashMap;
use std::path::PathBuf;

pub fn to_df(rocrate: &RoCrate) -> DataFrame {
    // Get uuid
    let uuid = rocrate.context.get_urn_uuid().unwrap();

    // Build the context
    let mut crate_frame = CrateFrame {
        uuid,
        id: Vec::new(),
        key: Vec::new(),
        value: Vec::new(),
    };

    frame_context(&mut crate_frame, &rocrate.context);
    frame_graph(&mut crate_frame, rocrate);

    DataFrame::new(vec![
        Series::new(
            "uuid".into(),
            vec![crate_frame.uuid.clone(); crate_frame.id.len()],
        )
        .into(),
        Series::new("id".into(), crate_frame.id.clone()).into(),
        Series::new("key".into(), crate_frame.key.clone()).into(),
        Series::new("value".into(), crate_frame.value.clone()).into(),
    ])
    .unwrap()
}

pub fn write_csv(df: &mut DataFrame, path: PathBuf) {
    let mut file = std::fs::File::create(path).unwrap();
    CsvWriter::new(&mut file).finish(df).unwrap();
}
pub fn write_parquet(df: &mut DataFrame, path: PathBuf) {
    let mut file = std::fs::File::create(path).unwrap();
    ParquetWriter::new(&mut file).finish(df).unwrap();
}

pub fn to_parquet(df: &mut DataFrame) -> Result<u64, PolarsError> {
    // Create an in-memory buffer (Vec<u8>)
    let mut buffer: Vec<u8> = Vec::new();

    // Write the DataFrame to the Parquet buffer
    ParquetWriter::new(&mut buffer).finish(df)
}

struct CrateFrame {
    uuid: String,
    id: Vec<String>,
    key: Vec<String>,
    value: Vec<String>,
}

impl CrateFrame {
    fn push_data(&mut self, id: &str, key: &str, value: &str) {
        self.id.push(String::from(id));
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
                        crate_frame.push_data("@context", "ro-crate", reference);
                    }
                    ContextItem::EmbeddedContext(embedded) => {
                        for (key, value) in embedded {
                            crate_frame.push_data("@context", key, value);
                        }
                    }
                }
            }
        }
        RoCrateContext::ReferenceContext(reference) => {
            crate_frame.push_data("@context", "ro-crate", reference);
        }
        RoCrateContext::EmbeddedContext(_embedded) => {
            println!("legacy - shouldnt be used")
        }
    }
}

fn push_id_logic(crate_frame: &mut CrateFrame, object_id: &str, id_type: &Id, key: &str) {
    match id_type {
        Id::Id(id) => {
            crate_frame.push_data(object_id, key, id);
        }
        Id::IdArray(id_array) => {
            for id in id_array {
                crate_frame.push_data(object_id, key, id);
            }
        }
    }
}
fn push_datatype_logic(crate_frame: &mut CrateFrame, object_id: &str, datatype: &DataType) {
    match datatype {
        DataType::Term(data_type) => {
            crate_frame.push_data(object_id, "@type", data_type);
        }
        DataType::TermArray(data_types) => {
            for data_type in data_types {
                crate_frame.push_data(object_id, "@type", data_type);
            }
        }
    }
}

fn push_license_logic(crate_frame: &mut CrateFrame, object_id: &str, license: &License) {
    match license {
        License::Id(id) => {
            push_id_logic(crate_frame, object_id, id, "license");
        }
        License::Description(description) => {
            crate_frame.push_data(object_id, "license", description);
        }
    }
}

fn push_dynamic_entity_logic(
    crate_frame: &mut CrateFrame,
    object_id: &str,
    dynamic_entity: &HashMap<String, EntityValue>,
) {
    for (key, value) in dynamic_entity {
        match value {
            EntityValue::EntityString(entity) => {
                crate_frame.push_data(object_id, key, entity);
            }
            EntityValue::EntityVecString(entity) => {
                for x in entity {
                    crate_frame.push_data(object_id, key, x);
                }
            }
            EntityValue::EntityId(entity) => {
                push_id_logic(crate_frame, object_id, entity, key);
            }
            EntityValue::Entityi64(entity) => {
                crate_frame.push_data(object_id, key, &entity.to_string());
            }
            EntityValue::Entityf64(entity) => {
                crate_frame.push_data(object_id, key, &entity.to_string());
            }
            EntityValue::EntityVeci64(entity) => {
                for x in entity {
                    crate_frame.push_data(object_id, key, &x.to_string());
                }
            }
            EntityValue::EntityVecf64(entity) => {
                for x in entity {
                    crate_frame.push_data(object_id, key, &x.to_string());
                }
            }
            EntityValue::EntityLicense(entity) => {
                push_license_logic(crate_frame, object_id, entity);
            }
            EntityValue::EntityDataType(entity) => {
                push_datatype_logic(crate_frame, object_id, entity);
            }
            EntityValue::EntityBool(entity) => {
                crate_frame.push_data(object_id, key, &entity.to_string());
            }
            _ => {}
        }
    }
}

fn frame_graph(crate_frame: &mut CrateFrame, rocrate: &RoCrate) {
    for entity in &rocrate.graph {
        match entity {
            GraphVector::MetadataDescriptor(data) => {
                let d_id = &data.id;
                let d_type = &data.type_;
                let d_conforms = &data.conforms_to;
                let d_about = &data.about;

                crate_frame.push_data("@graph", "@id", d_id);
                push_datatype_logic(crate_frame, d_id, d_type);
                push_id_logic(crate_frame, d_id, d_conforms, "conformsTo");
                push_id_logic(crate_frame, d_id, d_about, "conformsTo");

                if let Some(dynamic_entity) = &data.dynamic_entity {
                    push_dynamic_entity_logic(crate_frame, d_id, dynamic_entity);
                }
            }
            GraphVector::RootDataEntity(data) => {
                let d_id = &data.id;

                let d_type = &data.type_;

                let d_name = &data.name;
                let d_description = &data.description;
                let d_date_published = &data.date_published;
                let d_license = &data.license;

                crate_frame.push_data("@graph", "@id", d_id);
                crate_frame.push_data(d_id, "name", d_name);
                crate_frame.push_data(d_id, "description", d_description);
                crate_frame.push_data(d_id, "datePublished", d_date_published);
                push_datatype_logic(crate_frame, d_id, d_type);
                push_license_logic(crate_frame, d_id, d_license);

                if let Some(dynamic_entity) = &data.dynamic_entity {
                    push_dynamic_entity_logic(crate_frame, d_id, dynamic_entity);
                }
            }
            GraphVector::ContextualEntity(data) => {
                let d_id = &data.id;
                let d_type = &data.type_;
                crate_frame.push_data("@graph", "@id", d_id);
                push_datatype_logic(crate_frame, d_id, d_type);
                if let Some(dynamic_entity) = &data.dynamic_entity {
                    push_dynamic_entity_logic(crate_frame, d_id, dynamic_entity);
                }
            }
            GraphVector::DataEntity(data) => {
                let d_id = &data.id;
                let d_type = &data.type_;
                crate_frame.push_data("@graph", "@id", d_id);
                push_datatype_logic(crate_frame, d_id, d_type);

                if let Some(dynamic_entity) = &data.dynamic_entity {
                    push_dynamic_entity_logic(crate_frame, d_id, dynamic_entity);
                }
            }
        }
    }
}

#[cfg(test)]
mod write_crate_tests {
    use crate::ro_crate::convert::to_df;
    use crate::ro_crate::convert::write_csv;
    use crate::ro_crate::convert::write_parquet;
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
        let mut df = to_df(&rocrate);
        let path_csv = fixture_path("test-ro-crate.csv");
        let path_parquet = fixture_path("test-ro-crate.parquet");
        write_csv(&mut df, path_csv);
        write_parquet(&mut df, path_parquet);
    }

    #[test]
    #[ignore]
    fn test_big_rocrate() {
        let path = Path::new("examples").join("ro-crate-metadata_big.json");
        println!("Path created");
        let mut rocrate = read_crate(&path, 0).unwrap();
        println!("Crate loaded");
        rocrate.context.add_urn_uuid();
        println!("UUID added");
        let mut df = to_df(&rocrate);
        println!("Created dataframe");
        let path_csv = fixture_path("test-ro-crate.csv");
        let path_parquet = fixture_path("test-ro-crate.parquet");
        write_csv(&mut df, path_csv);
        println!("CSV created");
        write_parquet(&mut df, path_parquet);
        println!("Parquet created");
    }
}
