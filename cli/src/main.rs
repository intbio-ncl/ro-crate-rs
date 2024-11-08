//! Cli binding logic

use args::{
    AddCommand, ContextType, CrateAction, DeleteCommand, ModifyCommand, PackageCommand,
    ReadCommand, ValidateCommand,
};
use chrono::Utc;
use clap::Parser;
use constraints::{DataType, DynamicEntity, Id, IdValue, License};
use data_entity::DataEntity;
use read::{crate_path, read_crate};
use rocraters::ro_crate::rocrate::{ContextItem, GraphVector, RoCrate, RoCrateContext};
use rocraters::ro_crate::{constraints, data_entity, metadata_descriptor, read, root, write};
use serde::Serialize;
use serde_json::to_string_pretty;
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::io::{self, Write};
use std::path::PathBuf;
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
        CrateAction::Modify(modify_command) => {
            match modify_command {
                ModifyCommand::AddString(add_string_command) => {
                    let mut rocrate = open_and_load_crate(&add_string_command.target_crate);
                    let mut values: HashMap<String, DynamicEntity> = HashMap::new();
                    values.insert(
                        add_string_command.key,
                        DynamicEntity::EntityString(add_string_command.value),
                    );

                    rocrate.add_dynamic_entity_field(&add_string_command.id, values);

                    write_crate(&rocrate, add_string_command.target_crate)
                }
                ModifyCommand::AddIdValue(add_id_value_command) => {
                    let mut rocrate = open_and_load_crate(&add_id_value_command.target_crate);
                    let mut values: HashMap<String, DynamicEntity> = HashMap::new();
                    values.insert(
                        add_id_value_command.key,
                        DynamicEntity::EntityId(Id::Id(IdValue {
                            id: add_id_value_command.value,
                        })),
                    );

                    rocrate.add_dynamic_entity_field(&add_id_value_command.id, values);

                    write_crate(&rocrate, add_id_value_command.target_crate)
                }
                ModifyCommand::AddIdVecValues(add_id_vec_values_command) => {
                    let mut rocrate = open_and_load_crate(&add_id_vec_values_command.target_crate);
                    let mut values: HashMap<String, DynamicEntity> = HashMap::new();
                    let mut id_vec = Vec::new();
                    for id_value in add_id_vec_values_command.values {
                        id_vec.push(Id::Id(IdValue {
                            id: id_value.to_string(),
                        }));
                    }
                    values.insert(
                        add_id_vec_values_command.key,
                        DynamicEntity::EntityIdVec(id_vec),
                    );

                    rocrate.add_dynamic_entity_field(&add_id_vec_values_command.id, values);

                    write_crate(&rocrate, add_id_vec_values_command.target_crate)
                }
                ModifyCommand::AddMultiple(add_multiple_command) => {
                    let mut rocrate = open_and_load_crate(&add_multiple_command.target_crate);
                    let id = &add_multiple_command.id;

                    loop {
                        if let Some(dynamic_entity) = add_dynamic_entity() {
                            rocrate.add_dynamic_entity_field(id, dynamic_entity);

                            println!("Exit? (Y/n)");
                            let mut answer = String::new();
                            io::stdin().read_line(&mut answer).unwrap();
                            if answer.trim().eq_ignore_ascii_case("Y") {
                                break;
                            }
                        } else {
                            // If add_dynamic_entity() returns None, break the loop
                            break;
                        }
                    }

                    write_crate(&rocrate, add_multiple_command.target_crate)
                }
                ModifyCommand::RemoveField(remove_field_command) => {
                    let mut rocrate = open_and_load_crate(&remove_field_command.target_crate);
                    rocrate.remove_dynamic_entity_field(
                        &remove_field_command.id,
                        &remove_field_command.field,
                    );

                    write_crate(&rocrate, remove_field_command.target_crate)
                }
            }
        }
        CrateAction::Read(read_command) => match read_command {
            ReadCommand::Crate(read_crate_command) => {
                let rocrate = open_and_load_crate(&read_crate_command.target_crate);

                if read_crate_command.raw_struct {
                    println!("{:#?}", rocrate)
                } else {
                    match to_string_pretty(&rocrate) {
                        Ok(json_ld) => {
                            println!("{}", json_ld)
                        }
                        Err(e) => eprintln!("Failed to display crate: {}", e),
                    }
                }
            }
            ReadCommand::Entity(read_entity_command) => {
                let mut rocrate = open_and_load_crate(&read_entity_command.target_crate);
                let id = &read_entity_command.id;
                let index = rocrate.find_id_index(id);

                if let Some(index) = index {
                    if let Some(graph_vector) = rocrate.graph.get_mut(index) {
                        if read_entity_command.raw_struct {
                            println!("{:#?}", &graph_vector);
                        } else {
                            match to_string_pretty(&graph_vector) {
                                Ok(json_ld) => {
                                    println!("{}", json_ld);
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
                print_as_table(values, "Type".to_string(), "Count".to_string())
            }
            ReadCommand::Value(read_value_command) => {
                let rocrate = open_and_load_crate(&read_value_command.target_crate);
                let values = search_and_print_struct(
                    &rocrate.graph,
                    &read_value_command.value,
                    read_value_command.location,
                );
                let data: Vec<(String, isize)> = vec![(read_value_command.value, values)];
                print_as_table(data, "Value".to_string(), "Count".to_string())
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
fn open_and_load_crate(input: &String) -> RoCrate {
    let target_crate = crate_path(&input);
    match read_crate(&target_crate, 1) {
        Ok(ro_crate) => {
            // Process ro_crate if read successfully
            // ...
            ro_crate
        }
        Err(e) => {
            // Handle the error
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
            io::stdout().flush().unwrap(); // Make sure 'Enter key' is printed before input
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
        io::stdout().flush().unwrap(); // Make sure 'Enter key' is printed before input
        let mut key = String::new();
        io::stdin().read_line(&mut key).unwrap();

        print!("Enter value: ");
        io::stdout().flush().unwrap(); // Make sure 'Enter value' is printed before input
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
        conforms_to: Id::Id(IdValue {
            id: "https://w3id.org/ro/crate/1.1".to_string(),
        }),
        about: Id::Id(IdValue {
            id: "./".to_string(),
        }),
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
fn add_dynamic_entity() -> Option<HashMap<String, DynamicEntity>> {
    let mut dynamic_entity: HashMap<String, DynamicEntity> = HashMap::new();

    let field_type = prommpt_for_types();

    let key = read_input("Enter Key:");

    match field_type {
        // string
        1 => {
            let value = read_input("Enter value:");
            dynamic_entity.insert(key, DynamicEntity::EntityString(value));
            Some(dynamic_entity)
        }
        2 => {
            let value = read_input("Enter value:");
            dynamic_entity.insert(key, DynamicEntity::EntityId(Id::Id(IdValue { id: value })));
            Some(dynamic_entity)
        }
        3 => {
            let mut id_vec: Vec<Id> = Vec::new();
            loop {
                let value = read_input("Enter value {}:");
                id_vec.push(Id::Id(IdValue { id: value }));
                let value = read_input("Add more? (Y/N)");
                if value == "N" {
                    break;
                }
            }
            dynamic_entity.insert(key, DynamicEntity::EntityIdVec(id_vec));
            Some(dynamic_entity)
        }
        4 => {
            let value = read_input("Enter value:");
            let ivalue = parse_i64(value);
            match ivalue {
                Ok(value) => {
                    dynamic_entity.insert(key, DynamicEntity::Entityi64(value));
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
                    dynamic_entity.insert(key, DynamicEntity::Entityf64(value));
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
                    dynamic_entity.insert(key, DynamicEntity::EntityBool(Some(value)));
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

fn prommpt_for_types() -> u8 {
    loop {
        println!("Please select one of the following entity types to add");
        println!("1 - String");
        println!("2 - ID");
        println!("3 - A list of IDs");
        println!("4 - An integer (both positive/negative)");
        println!("5 - A floating point number (both positive/negative)");
        println!("6 - A boolean (true/false)");

        let mut answer = String::new();
        io::stdout().flush().unwrap(); // Ensure prompt is displayed before input
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
fn get_field_values_with_count<T: Serialize>(object: &T, field_name: &str) -> Vec<(String, isize)> {
    let mut collected_values = HashMap::new();
    let json = serde_json::to_value(object).unwrap();
    collect_field_values_recursive(&json, field_name, &mut collected_values);

    collected_values.into_iter().collect()
}

fn collect_field_values_recursive(
    json: &JsonValue,
    field_name: &str,
    collected_values: &mut HashMap<String, isize>,
) {
    match json {
        JsonValue::Object(obj) => {
            for (key, value) in obj {
                if key == field_name {
                    let value_str = value.to_string();
                    *collected_values.entry(value_str).or_insert(0) += 1;
                }
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
fn print_as_table(data: Vec<(String, isize)>, header_1: String, header_2: String) {
    // Determine the maximum width of the first column
    let max_width = data.iter().map(|(s, _)| s.len()).max().unwrap_or(0);

    // Print the header
    println!("{:<width$} | {header_2}", header_1, width = max_width);
    println!("{:-<width$}-|------", "", width = max_width);

    // Print each row
    for (item, count) in data {
        println!("{:<width$} | {}", item, count, width = max_width);
    }
}

fn search_and_print_struct<T: Serialize>(object: &T, search_value: &str, location: bool) -> isize {
    let json = serde_json::to_value(object).unwrap();

    let occurrences = search_and_print_recursive(&json, search_value, 0, location);
    occurrences
}

/// Method for searching based upon untyped serde value
fn search_and_print_recursive(
    json: &JsonValue,
    search_value: &str,
    mut occurrences: isize,
    location: bool,
) -> isize {
    match json {
        JsonValue::Object(obj) => {
            for (_key, value) in obj {
                if value == search_value {
                    occurrences += 1;
                    if location {
                        println!("Found in object:\n{}\n", to_string_pretty(&json).unwrap());
                    }
                    // Stop searching this branch after a match is found
                }
                occurrences =
                    search_and_print_recursive(value, search_value, occurrences, location);
            }
        }
        JsonValue::Array(arr) => {
            for item in arr {
                occurrences = search_and_print_recursive(item, search_value, occurrences, location);
            }
        }
        // For simple values, compare directly
        _ => {
            if json == search_value {
                occurrences += 1;
                if location {
                    println!("Found in value:\n{}\n", to_string_pretty(&json).unwrap());
                }
            }
        }
    }
    occurrences
}
