//! Cli binding logic

use ::serde::Serialize;
use args::{
    AddCommand, ContextType, CrateAction, DeleteCommand, ModifyCommand, PackageCommand,
    ReadCommand, ValidateCommand,
};
use chrono::Utc;
use clap::Parser;
use constraints::{DataType, EntityValue, Id, License};
use data_entity::DataEntity;
use json_to_table::json_to_table;
use read::{crate_path, read_crate};
use rocraters::ro_crate::graph_vector::GraphVector;
use rocraters::ro_crate::rocrate::{ContextItem, RoCrate, RoCrateContext};
use rocraters::ro_crate::{constraints, data_entity, metadata_descriptor, read, root, write};
use serde_json::Value as JsonValue;
use serde_json::{json, to_string_pretty};
use std::collections::HashMap;
use std::io::{self, Write};
use std::path::PathBuf;
use tabled::settings::{object::Rows, Style, Width};
use write::{write_crate, zip_crate};
pub mod args;

fn main() {
    let args = args::RoCrateArgs::parse();

    match args.crate_action {
        CrateAction::Init(init_command) => {
            if init_command.default {
                let mut rocrate = RoCrate::default();
                println!("{:?}", rocrate);
                if init_command.minimal {
                    rocrate = create_default_crate(rocrate);
                }

                write_crate(&rocrate, "ro-crate-metadata.json".to_string())
            } else {
                if let Some(input) = init_command.context_type {
                    println!("Recieved context_type {}", input);
                    let mut rocrate = create_rocrate_with_context(input);
                    if init_command.minimal {
                        rocrate = create_default_crate(rocrate)
                    }
                    write_crate(&rocrate, "ro-crate-metadata.json".to_string())
                } else {
                    println!("What context type is required? Input number. \n1.) Reference \n2.) Extended \n3.) Embedded");
                }
            }
        }
        CrateAction::Add(add_command) => {
            let mut rocrate = open_and_load_crate(&add_command.target_crate);
            rocrate = add_entity(rocrate, &add_command);

            write_crate(&rocrate, add_command.target_crate)
        }
        CrateAction::Delete(delete_command) => {
            let mut rocrate = open_and_load_crate(&delete_command.target_crate);

            rocrate = delete_entity(rocrate, &delete_command);

            write_crate(&rocrate, delete_command.target_crate)
        }
        CrateAction::Modify(modify_command) => match modify_command {
            ModifyCommand::AddIdValue(add_id_value_command) => {
                let mut rocrate = open_and_load_crate(&add_id_value_command.target_crate);
                let mut values: HashMap<String, EntityValue> = HashMap::new();
                values.insert(
                    add_id_value_command.key,
                    EntityValue::EntityId(Id::Id(add_id_value_command.value)),
                );

                rocrate.add_dynamic_entity_property(&add_id_value_command.id, values);

                write_crate(&rocrate, add_id_value_command.target_crate)
            }
            ModifyCommand::AddIdVecValues(add_id_vec_values_command) => {
                let mut rocrate = open_and_load_crate(&add_id_vec_values_command.target_crate);
                let mut values: HashMap<String, EntityValue> = HashMap::new();
                let mut id_vec: Vec<String> = Vec::new();
                for id_value in add_id_vec_values_command.values {
                    id_vec.push(id_value.to_string());
                }
                values.insert(
                    add_id_vec_values_command.key,
                    EntityValue::EntityId(Id::IdArray(id_vec)),
                );

                rocrate.add_dynamic_entity_property(&add_id_vec_values_command.id, values);

                write_crate(&rocrate, add_id_vec_values_command.target_crate)
            }
            ModifyCommand::AddMultiple(add_multiple_command) => {
                let mut rocrate = open_and_load_crate(&add_multiple_command.target_crate);
                let id = &add_multiple_command.id;

                while let Some(dynamic_entity) = add_dynamic_entity() {
                    rocrate.add_dynamic_entity_property(id, dynamic_entity);

                    println!("Exit? (Y/n)");
                    let mut answer = String::new();
                    io::stdin().read_line(&mut answer).unwrap();
                    if answer.trim().eq_ignore_ascii_case("Y") {
                        break;
                    }
                }

                write_crate(&rocrate, add_multiple_command.target_crate)
            }
            ModifyCommand::RemoveField(remove_field_command) => {
                let mut rocrate = open_and_load_crate(&remove_field_command.target_crate);
                rocrate.remove_dynamic_entity_property(
                    &remove_field_command.id,
                    &remove_field_command.field,
                );

                write_crate(&rocrate, remove_field_command.target_crate)
            }
        },
        CrateAction::Read(read_command) => match read_command {
            ReadCommand::Crate(read_crate_command) => {
                let rocrate = open_and_load_crate(&read_crate_command.target_crate);

                if read_crate_command.raw_struct {
                    println!("{:#?}", rocrate)
                } else {
                    match to_string_pretty(&rocrate) {
                        Ok(json_ld) => {
                            let mut table = json_to_table(&json!(&rocrate.graph)).into_table();
                            table.with(Style::modern_rounded());
                            if read_crate_command.fit {
                                table.modify(Rows::new(1..), Width::truncate(200).suffix("..."));
                            } else {
                                table.modify(Rows::new(1..), Width::truncate(79).suffix("..."));
                            }
                            println!("{}", table)
                        }
                        Err(e) => eprintln!("Failed to display crate: {}", e),
                    }
                }
            }
            ReadCommand::Entity(read_entity_command) => {
                let mut rocrate = open_and_load_crate(&read_entity_command.target_crate);
                let id = &read_entity_command.id;
                let index = rocrate.find_entity_index(id);

                if let Some(index) = index {
                    if let Some(graph_vector) = rocrate.graph.get_mut(index) {
                        if read_entity_command.raw_struct {
                            println!("{:#?}", &graph_vector);
                        } else {
                            match to_string_pretty(&graph_vector) {
                                Ok(_json_ld) => {
                                    let mut table =
                                        json_to_table(&json!(&graph_vector)).into_table();
                                    table.with(Style::modern_rounded());
                                    if read_entity_command.fit {
                                        table.modify(
                                            Rows::new(1..),
                                            Width::truncate(200).suffix("..."),
                                        );
                                    } else {
                                        table.modify(
                                            Rows::new(1..),
                                            Width::truncate(79).suffix("..."),
                                        );
                                    }
                                    println!("{}", table)
                                }
                                Err(e) => eprintln!("Failed to display entity: {}", e),
                            }
                        }
                    }
                }
            }
            ReadCommand::Fields(read_fields_command) => {
                let rocrate = open_and_load_crate(&read_fields_command.target_crate);
                let values =
                    get_field_values_with_count(&rocrate.graph, &read_fields_command.field);
                print_as_table(
                    values,
                    "@id",
                    &read_fields_command.field.to_string(),
                    "Count",
                );
            }
            ReadCommand::Value(read_value_command) => {
                let rocrate = open_and_load_crate(&read_value_command.target_crate);
                let values = search_and_print_struct(
                    &rocrate.graph,
                    &read_value_command.value,
                    read_value_command.location,
                );
                print_as_table(values, "Object ID", "Value", "Count");
            }
        },
        CrateAction::Package(package_command) => match package_command {
            PackageCommand::Zip(zip_command) => {
                let path: PathBuf;
                if zip_command.target_crate == "./" {
                    path = std::env::current_dir().unwrap();
                } else {
                    path = crate_path(zip_command.target_crate.as_str());
                }
                println!("{:?}", path);
                let _ = zip_crate(&path, true, 1);
            }
        },
        CrateAction::Validate(validate_command) => match validate_command {
            ValidateCommand::Basic(basic) => {
                let crate_name = crate_path(&basic.target_crate);
                match read_crate(&crate_name, 2) {
                    Ok(rocrate) => println!("Crate Valid"),
                    Err(e) => println!("Crate not valid: {:?}", e),
                }
            }
        },
    }
}

/// Input requires target_crate file string
fn open_and_load_crate(input: &str) -> RoCrate {
    let target_crate = crate_path(input);
    match read_crate(&target_crate, 1) {
        Ok(ro_crate) => ro_crate,
        Err(e) => {
            eprintln!("Error processing crate: {:?}", e);
            std::process::exit(1)
        }
    }
}

fn create_rocrate_with_context(context_type: ContextType) -> RoCrate {
    match context_type {
        ContextType::Reference => {
            println!("Please input context:");
            let mut answer = String::new();
            io::stdin().read_line(&mut answer).unwrap();
            let answer = answer.trim().to_string();
            let rocrate = RoCrate::new(RoCrateContext::ReferenceContext(answer), Vec::new());
            rocrate
        }
        ContextType::Embedded => {
            println!("Starting extended add");

            // Get the primary context (probably ro-crate spec)
            println!("Please primary context (default to ro-crate spac (1)):");
            io::stdout().flush().unwrap();
            let mut answer = String::new();
            io::stdin().read_line(&mut answer).unwrap();

            match answer.trim().parse::<u8>() {
                Ok(num) => {
                    if num == 1 {
                        answer.clear();
                        answer.push_str("https://w3id.org/ro/crate/1.1/context");
                    }
                }
                Err(e) => println!("Error: {}", e),
            }
            let answer = ContextItem::ReferenceItem(answer.trim().to_string());

            let mut key_value_pairs: HashMap<String, String> = HashMap::new();
            key_value_pairs = loop_input(key_value_pairs);
            let embedded_context_item = ContextItem::EmbeddedContext(key_value_pairs);
            println!("{:?}", embedded_context_item);
            let mut context_items = Vec::new();
            context_items.push(answer);
            context_items.push(embedded_context_item);

            let rocrate = RoCrate::new(RoCrateContext::ExtendedContext(context_items), Vec::new());
            rocrate
        }
        ContextType::Extended => {
            println!("Starting Embedded add");
            let mut key_value_pairs: HashMap<String, String> = HashMap::new();
            key_value_pairs = loop_input(key_value_pairs);
            let embedded_context_item = ContextItem::EmbeddedContext(key_value_pairs);
            println!("{:?}", embedded_context_item);
            let mut context_items = Vec::new();
            context_items.push(embedded_context_item);

            let rocrate = RoCrate::new(RoCrateContext::ExtendedContext(context_items), Vec::new());
            rocrate
        }
    }
}

fn loop_input(mut key_value_pairs: HashMap<String, String>) -> HashMap<String, String> {
    loop {
        println!("Would you like to add a key-value pair? (Y/n)");
        let mut answer = String::new();
        io::stdin().read_line(&mut answer).unwrap();
        if answer.trim().eq_ignore_ascii_case("n") {
            break;
        }

        print!("Enter key: ");
        io::stdout().flush().unwrap();
        let mut key = String::new();
        io::stdin().read_line(&mut key).unwrap();

        print!("Enter value: ");
        io::stdout().flush().unwrap();
        let mut value = String::new();
        io::stdin().read_line(&mut value).unwrap();

        key_value_pairs.insert(key.trim().to_string(), value.trim().to_string());
    }
    key_value_pairs
}

fn create_default_crate(mut rocrate: RoCrate) -> RoCrate {
    let description = metadata_descriptor::MetadataDescriptor {
        id: "ro-crate-metadata.json".to_string(),
        type_: DataType::Term("CreativeWork".to_string()),
        conforms_to: Id::Id("https://w3id.org/ro/crate/1.1".to_string()),
        about: Id::Id("./".to_string()),
        dynamic_entity: None,
    };

    let root_data_entity = root::RootDataEntity {
        id: "./".to_string(),
        type_: DataType::Term("Dataset".to_string()),
        date_published: Utc::now().to_rfc3339(),
        name: "Default Crate Name".to_string(),
        description: "Default crate description".to_string(),
        license: License::Description(String::from("Private")),
        dynamic_entity: None,
    };

    rocrate
        .graph
        .push(GraphVector::MetadataDescriptor(description));
    rocrate
        .graph
        .push(GraphVector::RootDataEntity(root_data_entity));

    rocrate
}

fn add_entity(mut rocrate: RoCrate, input: &AddCommand) -> RoCrate {
    let datatype = if input.datatype.len() == 1 {
        DataType::Term(input.datatype[0].clone())
    } else {
        DataType::TermArray(input.datatype.clone())
    };

    let entities = if input.extra_information {
        add_dynamic_entity()
    } else {
        None
    };

    let data_entity = DataEntity {
        id: input.id.to_string(),
        type_: datatype,
        dynamic_entity: entities,
    };

    rocrate.graph.push(GraphVector::DataEntity(data_entity));
    rocrate
}

/// Adds a dynamic entity to a entity that's in the process of being made
fn add_dynamic_entity() -> Option<HashMap<String, EntityValue>> {
    let mut dynamic_entity: HashMap<String, EntityValue> = HashMap::new();

    let field_type = prompt_for_types();

    let key = read_input("Enter Key:");

    match field_type {
        // string
        1 => {
            let value = read_input("Enter value:");
            dynamic_entity.insert(key, EntityValue::EntityString(value));
            Some(dynamic_entity)
        }
        2 => {
            let value = read_input("Enter value:");
            dynamic_entity.insert(key, EntityValue::EntityId(Id::Id(value)));
            Some(dynamic_entity)
        }
        3 => {
            let mut id_vec: Vec<String> = Vec::new();
            loop {
                let value = read_input("Enter value {}:");
                id_vec.push(value);
                let value = read_input("Add more? (Y/N)");
                if value == "N" {
                    break;
                }
            }
            dynamic_entity.insert(key, EntityValue::EntityId(Id::IdArray(id_vec)));
            Some(dynamic_entity)
        }
        4 => {
            let value = read_input("Enter value:");
            let ivalue = parse_i64(value);
            match ivalue {
                Ok(value) => {
                    dynamic_entity.insert(key, EntityValue::Entityi64(value));
                }
                Err(e) => println!("An error occurred: {}", e),
            }

            Some(dynamic_entity)
        }
        5 => {
            let value = read_input("Enter value:");
            let ivalue = parse_f64(value);
            match ivalue {
                Ok(value) => {
                    dynamic_entity.insert(key, EntityValue::Entityf64(value));
                }
                Err(e) => println!("An error occurred: {}", e),
            }

            Some(dynamic_entity)
        }
        6 => {
            let value = read_input("Enter value:");
            let bvalue = parse_bool(value);
            match bvalue {
                Ok(value) => {
                    dynamic_entity.insert(key, EntityValue::EntityBool(Some(value)));
                }
                Err(e) => println!("An error occurred: {}", e),
            }

            Some(dynamic_entity)
        }
        0 => None,
        _ => {
            println!("Invalid field type.");
            None
        }
    }
}

fn read_input(prompt: &str) -> String {
    println!("{}", prompt);
    io::stdout().flush().expect("Failed to flush stdout");

    let mut input = String::new();
    io::stdin()
        .read_line(&mut input)
        .expect("Failed to read line");
    input.trim().to_string()
}

fn parse_i64(value: String) -> Result<i64, String> {
    match value.trim().parse::<i64>() {
        Ok(num) => Ok(num),
        Err(e) => Err(e.to_string()),
    }
}
fn parse_f64(value: String) -> Result<f64, String> {
    match value.trim().parse::<f64>() {
        Ok(num) => Ok(num),
        Err(e) => Err(e.to_string()),
    }
}
fn parse_bool(value: String) -> Result<bool, String> {
    match value.trim().parse::<bool>() {
        Ok(bool) => Ok(bool),
        Err(e) => Err(e.to_string()),
    }
}

fn prompt_for_types() -> u8 {
    loop {
        println!("Please select one of the following entity types to add");
        println!("1 - String");
        println!("2 - ID");
        println!("3 - A list of IDs");
        println!("4 - An integer (both positive/negative)");
        println!("5 - A floating point number (both positive/negative)");
        println!("6 - A boolean (true/false)");

        let mut answer = String::new();
        io::stdout().flush().unwrap();
        io::stdin().read_line(&mut answer).unwrap();

        match answer.trim().parse::<u8>() {
            Ok(num) if num >= 1 && num <= 6 => return num,
            _ => println!("Please enter a number between 1 and 5."),
        }
    }
}

/// Deletes an entity from the target rocrate
fn delete_entity(mut rocrate: RoCrate, input: &DeleteCommand) -> RoCrate {
    rocrate.remove_by_id(&input.id, input.recursive);
    rocrate
}

/// NOTE: This is massively suboptimal but it's a very quick and easy way to just get the values
/// without having to spend the effort to think of how to parse it all agian
fn get_field_values_with_count<T: Serialize>(
    object: &T,
    field_name: &str,
) -> Vec<(String, String, isize)> {
    let mut collected_values = HashMap::new();
    let json = serde_json::to_value(object).unwrap();
    collect_field_values_recursive(&json, field_name, &mut collected_values);

    collected_values
        .into_iter()
        .map(|((id, value), count)| (id, value, count))
        .collect()
}

/// Collects field values recursively, now including "@id" for each match.
fn collect_field_values_recursive(
    json: &JsonValue,
    field_name: &str,
    collected_values: &mut HashMap<(String, String), isize>,
) {
    match json {
        JsonValue::Object(obj) => {
            // Check if the object contains "@id"
            let current_id = obj
                .get("@id")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();

            for (key, value) in obj {
                if key == field_name {
                    let value_str = value.to_string();
                    let key = (current_id.clone(), value_str);
                    *collected_values.entry(key).or_insert(0) += 1;
                }
                // Continue recursive search within the object
                collect_field_values_recursive(value, field_name, collected_values);
            }
        }
        JsonValue::Array(arr) => {
            for item in arr {
                collect_field_values_recursive(item, field_name, collected_values);
            }
        }
        _ => {}
    }
}

/// For fun
fn print_as_table(
    data: Vec<(String, String, isize)>,
    header_1: &str,
    header_2: &str,
    header_3: &str,
) {
    // Determine the maximum width for each column
    let max_width_id = data.iter().map(|(s, _, _)| s.len()).max().unwrap_or(0);
    let max_width_type = data.iter().map(|(_, s, _)| s.len()).max().unwrap_or(0);

    // Print the header
    println!(
        "{:<width_id$} | {:<width_type$} | {}",
        header_1,
        header_2,
        header_3,
        width_id = max_width_id,
        width_type = max_width_type
    );
    println!(
        "{:-<width_id$}-|-{:-<width_type$}-|------",
        "",
        "",
        width_id = max_width_id,
        width_type = max_width_type
    );

    // Print each row
    for (id, value, count) in data {
        println!(
            "{:<width_id$} | {:<width_type$} | {}",
            id,
            value,
            count,
            width_id = max_width_id,
            width_type = max_width_type
        );
    }
}

fn search_and_print_struct<T: Serialize>(
    object: &T,
    search_value: &str,
    location: bool,
) -> Vec<(String, String, isize)> {
    let json = serde_json::to_value(object).unwrap();
    let mut occurrences = HashMap::new();
    search_and_print_recursive(&json, search_value, &mut occurrences, location);

    // Convert occurrences to a vector of tuples for printing
    occurrences
        .into_iter()
        .map(|((id, value), count)| (id, value, count))
        .collect()
}

fn search_and_print_recursive(
    json: &JsonValue,
    search_value: &str,
    occurrences: &mut HashMap<(String, String), isize>,
    location: bool,
) {
    match json {
        JsonValue::Object(obj) => {
            // Retrieve @id if it exists in the current object
            let current_id = obj
                .get("@id")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();

            for (_key, value) in obj {
                // Check if this value matches the search_value
                if value == search_value {
                    let key = (current_id.clone(), search_value.to_string());
                    *occurrences.entry(key).or_insert(0) += 1;

                    if location {
                        println!(
                            "Found in object:\n{}\n",
                            serde_json::to_string_pretty(&json).unwrap()
                        );
                    }
                }
                // Recursively search the object
                search_and_print_recursive(value, search_value, occurrences, location);
            }
        }
        JsonValue::Array(arr) => {
            for item in arr {
                search_and_print_recursive(item, search_value, occurrences, location);
            }
        }
        _ => {
            // For simple values, compare directly
            if json == search_value {
                let key = ("N/A".to_string(), search_value.to_string());
                *occurrences.entry(key).or_insert(0) += 1;

                if location {
                    println!(
                        "Found in value:\n{}\n",
                        serde_json::to_string_pretty(&json).unwrap()
                    );
                }
            }
        }
    }
}
