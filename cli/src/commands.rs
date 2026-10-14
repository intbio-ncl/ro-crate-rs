//! Handlers for commands that map directly onto the core library

use crate::args::{
    AddFieldCommand, ContextCommand, EntityIdCommand, EntityTypeCommand, PrefixIdsCommand,
    ReadLinksCommand, ReadListCommand, ReplaceIdCommand, SchemaCommand, SubcratesCommand,
    UpsertCommand, ValidateCrateCommand,
};
use crate::core_compat;
use crate::crate_io::{
    arg_or_stdin, fail, load_crate, load_for_update, parse_context_item, parse_entity_value,
    print_list, read_target, save_crate,
};
use rocraters::ro_crate::constraints::Id;
use rocraters::ro_crate::graph_vector::GraphVector;
use rocraters::ro_crate::object_storage::relative_to_object_store;
use rocraters::ro_crate::read::{validate_crate_keys, CrateReadError, ValidationResult};
use rocraters::ro_crate::schema::{load_rocrate_schema, RoCrateSchemaVersion};
use serde::Serialize;
use serde_json::to_string_pretty;

/// Maps a schema version onto its "major.minor" string
fn schema_version_to_str(version: RoCrateSchemaVersion) -> Option<&'static str> {
    match version {
        RoCrateSchemaVersion::V1_1 => Some("1.1"),
        RoCrateSchemaVersion::V1_2 => Some("1.2"),
        RoCrateSchemaVersion::V1_3 => Some("1.3"),
        _ => None,
    }
}

fn schema_version_from_str(version: &str) -> RoCrateSchemaVersion {
    match version {
        "1.1" => RoCrateSchemaVersion::V1_1,
        "1.2" => RoCrateSchemaVersion::V1_2,
        "1.3" => RoCrateSchemaVersion::V1_3,
        other => fail(format!(
            "Unsupported RO-Crate version '{}': expected 1.1, 1.2 or 1.3",
            other
        )),
    }
}

fn entity_not_found(id: &str) -> ! {
    fail(format!("Entity '{}' not found", id))
}

pub fn add_field(command: AddFieldCommand) {
    let target = &command.target.target_crate;
    let mut rocrate = load_for_update(target);
    let value = parse_entity_value(&command.value, command.json);

    if !core_compat::set_property(&mut rocrate, &command.id, command.key, value) {
        entity_not_found(&command.id);
    }
    save_crate(&rocrate, target);
}

pub fn add_type(command: EntityTypeCommand) {
    let target = &command.target.target_crate;
    let mut rocrate = load_for_update(target);
    match rocrate.get_entity_mutable(&command.id) {
        Some(entity) => entity.add_type(command.data_type),
        None => entity_not_found(&command.id),
    }
    save_crate(&rocrate, target);
}

pub fn remove_type(command: EntityTypeCommand) {
    let target = &command.target.target_crate;
    let mut rocrate = load_for_update(target);
    match rocrate.get_entity_mutable(&command.id) {
        Some(entity) => {
            if let Err(e) = core_compat::remove_type(entity, command.data_type) {
                fail(e);
            }
        }
        None => entity_not_found(&command.id),
    }
    save_crate(&rocrate, target);
}

pub fn replace_id(command: ReplaceIdCommand) {
    let target = &command.target.target_crate;
    let mut rocrate = load_for_update(target);
    rocrate.update_id_recursive(&command.old_id, &command.new_id);
    save_crate(&rocrate, target);
}

pub fn add_has_part(command: EntityIdCommand) {
    let target = &command.target.target_crate;
    let mut rocrate = load_for_update(target);
    core_compat::add_to_root_has_part(&mut rocrate, &command.id);
    save_crate(&rocrate, target);
}

pub fn upsert(command: UpsertCommand) {
    let target = &command.target.target_crate;
    let mut rocrate = load_for_update(target);
    let entity: GraphVector = serde_json::from_str(&arg_or_stdin(&command.entity))
        .unwrap_or_else(|e| fail(format!("Invalid entity: {}", e)));

    let id = entity.get_id().clone();
    if !rocrate.overwrite_by_id(&id, entity.clone()) {
        rocrate.graph.push(entity);
    }
    save_crate(&rocrate, target);
}

pub fn prefix_ids(command: PrefixIdsCommand) {
    let target = &command.target.target_crate;
    let mut rocrate = load_for_update(target);
    relative_to_object_store(&mut rocrate, &command.object_root);
    save_crate(&rocrate, target);
}

pub fn read_ids(command: ReadListCommand) {
    let rocrate = load_crate(&command.target.target_crate, 1);
    let ids: Vec<String> = rocrate.get_all_ids().into_iter().cloned().collect();
    print_list(&ids, command.json);
}

pub fn read_properties(command: ReadListCommand) {
    let rocrate = load_crate(&command.target.target_crate, 1);
    print_list(&rocrate.get_all_properties(), command.json);
}

pub fn read_links(command: ReadLinksCommand) {
    let rocrate = load_crate(&command.target.target_crate, 1);
    let entity = rocrate
        .get_entity(&command.id)
        .unwrap_or_else(|| entity_not_found(&command.id));

    let links: Vec<String> = entity
        .get_linked_ids()
        .into_iter()
        .flat_map(|id| match id {
            Id::Id(id) => vec![id],
            Id::IdArray(ids) => ids,
        })
        .collect();
    print_list(&links, command.json);
}

pub fn context(command: ContextCommand) {
    match command {
        ContextCommand::Show(command) => {
            let rocrate = load_crate(&command.target.target_crate, 1);
            println!("{}", to_string_pretty(&rocrate.context).unwrap());
        }
        ContextCommand::List(command) => {
            let rocrate = load_crate(&command.target.target_crate, 1);
            print_list(&rocrate.context.get_all_context(), command.json);
        }
        ContextCommand::Keys(command) => {
            let rocrate = load_crate(&command.target.target_crate, 1);
            print_list(&rocrate.get_context_items(), command.json);
        }
        ContextCommand::Get(command) => {
            let rocrate = load_crate(&command.target.target_crate, 1);
            match rocrate.context.get_specific_context(&command.key) {
                Some(value) => println!("{}", value),
                None => fail(format!("Context key '{}' not found", command.key)),
            }
        }
        ContextCommand::Add(command) => {
            let target = &command.target.target_crate;
            let mut rocrate = load_for_update(target);
            rocrate.add_context(parse_context_item(&command.item));
            save_crate(&rocrate, target);
        }
        ContextCommand::Remove(command) => {
            let target = &command.target.target_crate;
            let mut rocrate = load_for_update(target);
            if let Err(e) = rocrate
                .context
                .remove_context(&parse_context_item(&command.item))
            {
                fail(e);
            }
            save_crate(&rocrate, target);
        }
        ContextCommand::Replace(command) => {
            let target = &command.target.target_crate;
            let mut rocrate = load_for_update(target);
            if let Err(e) = rocrate.context.replace_context(
                &parse_context_item(&command.new),
                &parse_context_item(&command.old),
            ) {
                fail(e);
            }
            save_crate(&rocrate, target);
        }
        ContextCommand::AddUrn(command) => {
            let target = &command.target.target_crate;
            let mut rocrate = load_for_update(target);
            match core_compat::add_urn_uuid(&mut rocrate.context) {
                Some(urn) => {
                    save_crate(&rocrate, target);
                    println!("{}", urn);
                }
                None => fail("Context does not support a urn:uuid @base"),
            }
        }
        ContextCommand::Urn(command) => {
            let rocrate = load_crate(&command.target.target_crate, 1);
            match core_compat::urn_uuid(&rocrate.context) {
                Some(urn) => println!("{}", urn),
                None => fail("Crate has no urn:uuid @base"),
            }
        }
        ContextCommand::Version(command) => {
            let rocrate = load_crate(&command.target.target_crate, 0);
            match rocrate
                .get_rocrate_version()
                .and_then(schema_version_to_str)
            {
                Some(version) => println!("{}", version),
                None => fail("Could not determine the RO-Crate version from the context"),
            }
        }
    }
}

pub fn subcrates(command: SubcratesCommand) {
    match command {
        SubcratesCommand::List(command) => {
            let rocrate = load_crate(&command.target.target_crate, 1);
            let subcrates = rocrate.get_subcrates();
            if command.json {
                println!("{}", to_string_pretty(&subcrates).unwrap());
            } else {
                for subcrate in subcrates {
                    println!("{}", subcrate.get_id());
                }
            }
        }
        #[cfg(feature = "subcrate-resolution")]
        SubcratesCommand::Fetch(command) => {
            use rocraters::ro_crate::subcrate_resolution::{
                fetch_subcrates_recursive_with_options, fetch_subcrates_with_options,
                try_fetch_subcrates_recursive_with_options, try_fetch_subcrates_with_options,
                FetchOptions,
            };

            let rocrate = load_crate(&command.target.target_crate, 1);
            let options = FetchOptions {
                allow_remote: command.allow_remote,
            };
            let subcrates = match (command.recursive, command.lenient) {
                (false, false) => fetch_subcrates_with_options(&rocrate, options),
                (true, false) => fetch_subcrates_recursive_with_options(&rocrate, options),
                (false, true) => Ok(try_fetch_subcrates_with_options(&rocrate, options)),
                (true, true) => Ok(try_fetch_subcrates_recursive_with_options(
                    &rocrate, options,
                )),
            }
            .unwrap_or_else(|e| fail(format!("Failed to fetch subcrates: {}", e)));
            println!("{}", to_string_pretty(&subcrates).unwrap());
        }
    }
}

pub fn schema(command: SchemaCommand) {
    let schema = load_rocrate_schema(schema_version_from_str(&command.spec_version))
        .unwrap_or_else(|e| fail(e));

    if command.json {
        println!("{}", to_string_pretty(&schema).unwrap());
    } else if command.terms {
        let mut terms: Vec<(&String, &String)> = schema.context.iter().collect();
        terms.sort();
        for (term, iri) in terms {
            println!("{}\t{}", term, iri);
        }
    } else {
        println!("@id: {}", schema.id);
        println!("version: {}", schema.version);
        println!("terms: {}", schema.context.len());
    }
}

/// Mirrors the report returned by the python `validate` functions
#[derive(Serialize)]
struct ValidationReport {
    is_valid: bool,
    invalid_keys: Vec<String>,
    invalid_ids: Vec<String>,
    invalid_types: Vec<String>,
    error_type: Option<&'static str>,
    error_message: Option<String>,
}

impl ValidationReport {
    fn error(error_type: &'static str, error_message: String) -> Self {
        ValidationReport {
            is_valid: false,
            invalid_keys: Vec::new(),
            invalid_ids: Vec::new(),
            invalid_types: Vec::new(),
            error_type: Some(error_type),
            error_message: Some(error_message),
        }
    }
}

impl From<ValidationResult> for ValidationReport {
    fn from(result: ValidationResult) -> Self {
        match result {
            ValidationResult::Valid => ValidationReport {
                is_valid: true,
                invalid_keys: Vec::new(),
                invalid_ids: Vec::new(),
                invalid_types: Vec::new(),
                error_type: None,
                error_message: None,
            },
            ValidationResult::Invalid(validation) => ValidationReport {
                is_valid: false,
                invalid_keys: validation.invalid_keys,
                invalid_ids: validation.invalid_ids,
                invalid_types: validation.invalid_types,
                error_type: None,
                error_message: None,
            },
            ValidationResult::Error(message) => ValidationReport::error("SchemaError", message),
        }
    }
}

impl From<CrateReadError> for ValidationReport {
    fn from(error: CrateReadError) -> Self {
        let error_type = match &error {
            CrateReadError::IoError(_) => "IoError",
            CrateReadError::JsonError(_) => "JsonError",
            CrateReadError::VocabNotValid(_) => "VocabNotValid",
            CrateReadError::SchemaError(_) => "SchemaError",
            CrateReadError::ReqwestError(_) => "ReqwestError",
        };
        ValidationReport::error(error_type, error.to_string())
    }
}

/// Validates a crate, exiting with a failure status if it is not valid
pub fn validate(command: ValidateCrateCommand) {
    let report = match read_target(&command.target_crate, 0) {
        Ok(rocrate) => ValidationReport::from(validate_crate_keys(&rocrate)),
        Err(e) => ValidationReport::from(e),
    };

    if command.json {
        println!("{}", to_string_pretty(&report).unwrap());
    } else if report.is_valid {
        println!("Crate Valid");
    } else {
        println!("Crate not valid");
        if let Some(message) = &report.error_message {
            println!("{}: {}", report.error_type.unwrap_or("Error"), message);
        }
        for (label, values) in [
            ("Invalid keys", &report.invalid_keys),
            ("Invalid IDs", &report.invalid_ids),
            ("Invalid types", &report.invalid_types),
        ] {
            if !values.is_empty() {
                println!("{}: {}", label, values.join(", "));
            }
        }
    }

    if !report.is_valid {
        std::process::exit(1);
    }
}

#[cfg(feature = "parquet")]
pub fn convert(command: crate::args::ConvertCommand) {
    use crate::args::ConvertCommand;
    use rocraters::ro_crate::convert::{to_df, write_csv, write_parquet};
    use std::path::PathBuf;

    let (command, write): (_, fn(_, PathBuf) -> _) = match command {
        ConvertCommand::Csv(command) => (command, write_csv),
        ConvertCommand::Parquet(command) => (command, write_parquet),
    };

    let rocrate = load_crate(&command.target.target_crate, 1);
    if core_compat::urn_uuid(&rocrate.context).is_none() {
        fail("Crate has no urn:uuid @base in its context: run `rocraters context add-urn` first");
    }
    let mut df = to_df(&rocrate);
    write(&mut df, PathBuf::from(&command.output))
        .unwrap_or_else(|e| fail(format!("Failed to write {}: {}", command.output, e)));
}
