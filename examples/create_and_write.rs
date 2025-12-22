use ro_crate_rs::ro_crate::constraints::*;
use ro_crate_rs::ro_crate::contextual_entity::ContextualEntity;
use ro_crate_rs::ro_crate::data_entity::DataEntity;
use ro_crate_rs::ro_crate::metadata_descriptor::MetadataDescriptor;
use ro_crate_rs::ro_crate::read::{read_crate, CrateReadError};
use ro_crate_rs::ro_crate::rocrate::GraphVector;
use ro_crate_rs::ro_crate::rocrate::{RoCrate, RoCrateContext};
use ro_crate_rs::ro_crate::root::RootDataEntity;
use ro_crate_rs::ro_crate::write::write_crate;
use uuid::Uuid;

fn main() {
    let mut i = 0;

    while i < 500 {
        // Create empty RoCrate
        let mut rocrate = RoCrate {
            context: RoCrateContext::ReferenceContext(
                "https://w3id.org/ro/crate/1.2/context".to_string(),
            ),
            graph: Vec::new(),
        };

        // Set new MetadataDescriptor
        let description = MetadataDescriptor {
            id: "ro-crate-metadata.json".to_string(),
            type_: DataType::Term("CreativeWork".to_string()),
            conforms_to: Id::Id(IdValue {
                id: "https://w3id.org/ro/crate/1.2".to_string(),
            }),
            about: Id::Id(IdValue {
                id: "./".to_string(),
            }),
            dynamic_entity: None,
        };

        // Create new RootDataEntity
        let root_data_entity = RootDataEntity {
            id: "./".to_string(),
            type_: DataType::Term("Dataset".to_string()),
            name: "Test Crate".to_string(),
            description: "Crate for testing the RO-Crate rust library and seeing how it functions"
                .to_string(),
            date_published: Option::Some("2024".to_string()),
            license: Some(License::Id(Id::Id(IdValue {
                id: "MIT LICENSE".to_string(),
            }))),
            dynamic_entity: None,
        };

        //
        let entity_types = Vec::from(["File".to_string(), "DigitalDocument".to_string()]);

        let mut data_entity = DataEntity {
            id: "output/data_entity.txt".to_string(),
            type_: DataType::TermArray(entity_types),
            dynamic_entity: None,
        };

        let mut contextual_entity: ContextualEntity = ContextualEntity {
            id: "#JohnDoe".to_string(),
            type_: DataType::Term("Person".to_string()),
            dynamic_entity: None,
        };

        rocrate
            .graph
            .push(GraphVector::MetadataDescriptor(description));
        rocrate
            .graph
            .push(GraphVector::RootDataEntity(root_data_entity));
        rocrate.graph.push(GraphVector::DataEntity(data_entity));
        rocrate
            .graph
            .push(GraphVector::ContextualEntity(contextual_entity));

        for _ in 0..100 {
            let uuid = Uuid::new_v4(); // Generate a random UUID
            rocrate
                .graph
                .push(GraphVector::ContextualEntity(ContextualEntity {
                    id: uuid.to_string(),
                    type_: DataType::Term("Person".to_string()),
                    dynamic_entity: None,
                })); // Print the UUID, or do something else with it
        }

        write_crate(&rocrate, "ro-crate-metadata_3.json".to_string());

        let crate_name = crate_path("ro-crate-metadata_3.json");

        match read_crate(&crate_name, 0) {
            Ok(mut rocrate) => {}
            Err(CrateReadError::IoError(err)) => {
                eprintln!("IO error occurred: {}", err);
            }
            Err(CrateReadError::JsonError(err)) => {
                eprintln!("JSON deserialization error occurred: {}", err);
            }
            _ => {}
        }
        i = i + 1;
    }
}
