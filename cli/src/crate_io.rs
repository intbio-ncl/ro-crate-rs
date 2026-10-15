//! Loading, saving and argument parsing shared by cli commands

use rocraters::ro_crate::constraints::EntityValue;
use rocraters::ro_crate::context::ContextItem;
use rocraters::ro_crate::read::{
    load_remote, parse_zip, read_crate, read_crate_obj, CrateReadError,
};
use rocraters::ro_crate::rocrate::RoCrate;
use rocraters::ro_crate::write::write_crate;
use std::collections::HashMap;
use std::fmt::Display;
use std::io::{self, Read};
use std::path::PathBuf;

/// Prints an error to stderr and exits with a failure status
pub fn fail(message: impl Display) -> ! {
    eprintln!("{}", message);
    std::process::exit(1)
}

fn is_remote(target: &str) -> bool {
    target.starts_with("http://") || target.starts_with("https://")
}

/// Reads a crate from a metadata file, a .zip archive, an http(s) URL or `-` (stdin)
pub fn read_target(target: &str, validation_level: i8) -> Result<RoCrate, CrateReadError> {
    if target == "-" {
        read_crate_obj(&read_stdin()?, validation_level)
    } else if is_remote(target) {
        let url =
            url::Url::parse(target).map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
        load_remote(url, validation_level)
    } else if target.ends_with(".zip") {
        parse_zip(target, validation_level)
    } else {
        read_crate(&PathBuf::from(target), validation_level)
    }
}

/// Loads a crate for a read-only command, exiting on failure
pub fn load_crate(target: &str, validation_level: i8) -> RoCrate {
    read_target(target, validation_level)
        .unwrap_or_else(|e| fail(format!("Error processing crate: {:?}", e)))
}

/// Loads a crate that will be written back, so it must be a metadata file
pub fn load_for_update(target: &str) -> RoCrate {
    if target == "-" || is_remote(target) || target.ends_with(".zip") {
        fail("Cannot update Zip, remote or stdin crates: target a metadata file");
    }
    load_crate(target, 1)
}

pub fn save_crate(rocrate: &RoCrate, target: &str) {
    write_crate(rocrate, target.to_string())
        .unwrap_or_else(|e| fail(format!("Failed to write crate: {}", e)))
}

pub fn read_stdin() -> io::Result<String> {
    let mut buffer = String::new();
    io::stdin().read_to_string(&mut buffer)?;
    Ok(buffer)
}

/// Returns the argument, or stdin when the argument is `-`
pub fn arg_or_stdin(value: &str) -> String {
    if value == "-" {
        read_stdin().unwrap_or_else(|e| fail(format!("Failed to read stdin: {}", e)))
    } else {
        value.to_string()
    }
}

/// Parses a context URL, or a JSON object of term definitions
pub fn parse_context_item(item: &str) -> ContextItem {
    if item.trim_start().starts_with('{') {
        match serde_json::from_str::<HashMap<String, String>>(item) {
            Ok(map) => ContextItem::EmbeddedContext(map),
            Err(e) => fail(format!("Invalid embedded context '{}': {}", item, e)),
        }
    } else {
        ContextItem::ReferenceItem(item.to_string())
    }
}

/// Parses a field value as a plain string, or as JSON
pub fn parse_entity_value(value: &str, json: bool) -> EntityValue {
    if !json {
        return EntityValue::EntityString(value.to_string());
    }
    serde_json::from_str(value)
        .unwrap_or_else(|e| fail(format!("Invalid JSON value '{}': {}", value, e)))
}

/// Prints one item per line, or a JSON array
pub fn print_list(items: &[String], json: bool) {
    if json {
        println!("{}", serde_json::to_string_pretty(items).unwrap());
    } else {
        for item in items {
            println!("{}", item);
        }
    }
}
