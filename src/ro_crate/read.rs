//! Allows RO-Crates (ro-crate-metadata.json) files to be read into the
//! RoCrate data structure

use crate::ro_crate::rocrate::RoCrate;
use crate::ro_crate::schema::load_rocrate_schema;
use log::error;
use log::info;
use log::warn;
use serde_json;
use thiserror::Error;
use std::collections::HashSet;
use std::fs;
use std::io;
use std::io::Read;
use std::path::{Path, PathBuf};
use zip::ZipArchive;

/// Reads and deserialises an RO-Crate from a specified file path.
///
/// This function attempts to load an RO-Crate from a JSON file located at `crate_path`.
/// If `valid` is `2`, it also validates the crate's keys against the RO-Crate schema.
///
/// # Arguments
/// * `crate_path` - A reference to the `PathBuf` indicating the file path of the RO-Crate to read.
/// * `valid` - A boolean flag indicating whether to validate the crate's keys against the schema.
pub fn read_crate(crate_path: &PathBuf, validation_level: i8) -> Result<RoCrate, CrateReadError> {
    match fs::read_to_string(crate_path) {
        Ok(data) => match serde_json::from_str::<RoCrate>(&data) {
            Ok(rocrate) => {
                if validation_level == 0 {
                    Ok(rocrate)
                } else {
                    match validity_wrapper(&rocrate, validation_level) {
                        Ok(_) => Ok(rocrate),
                        Err(e) => Err(e),
                    }
                }
            }
            Err(e) => {
                // This is a simplified method for being able to parse None
                // values without having to rebuild a custom Value deserialiser.
                // None is handled explicitly because it is not JSON synatx but
                // could be common with python workflows. As such, it is replaced by
                // null as this enables the value parser to continue
                //
                // This will error and highlight the specific point of failure,
                // however will try and continue.
                //
                // DO NOT REMOVE UNLESS BUILDING CUSTOM DESERIALISER FOR GRAPHVECTOR
                error!("Error in parsing {}", e);
                let normalised = data.replace(": None", ": null");
                match serde_json::from_str::<RoCrate>(&normalised) {
                    Ok(rocrate) => {
                        if validation_level == 0 {
                            Ok(rocrate)
                        } else {
                            match validity_wrapper(&rocrate, validation_level) {
                                Ok(_) => Ok(rocrate),
                                Err(e) => Err(e),
                            }
                        }
                    }
                    Err(e) => {
                        error!("1 | failed at rocrate parse");
                        Err(CrateReadError::from(e))
                    }
                }
            }
        },
        Err(e) => {
            error!("Failed at reading to string");
            Err(CrateReadError::from(e))
        }
    }
}

/// Reads and deserialises an RO-Crate from a json object string.
///
/// This function attempts to load an RO-Crate from a JSON object string.
/// If 'valid' is '2', it also validates the crate's keys against the RO-Crate schema.
///
/// # Arguments
/// * 'crate_obj' - A str containing a json object
/// * 'valid' - A boolean flag indiciating whether to validate the crate's keys against the schema.
pub fn read_crate_obj(crate_obj: &str, validation_level: i8) -> Result<RoCrate, CrateReadError> {
    match serde_json::from_str::<RoCrate>(crate_obj) {
        Ok(rocrate) => {
            if validation_level == 0 {
                Ok(rocrate)
            } else {
                match validity_wrapper(&rocrate, validation_level) {
                    Ok(_) => Ok(rocrate),
                    Err(e) => Err(e),
                }
            }
        }
        Err(e) => {
            // This is a simplified method for being able to parse None
            // values without having to rebuild a custom Value deserialiser.
            // None is handled explicitly because it is not JSON synatx but
            // could be common with python workflows. As such, it is replaced by
            // null as this enables the value parser to continue
            //
            // This will error and highlight the specific point of failure,
            // however will try and continue.
            //
            // DO NOT REMOVE UNLESS BUILDING CUSTOM DESERIALISER FOR GRAPHVECTOR
            error!("Error in parsing {}", e);
            let normalised = crate_obj.replace(": None", ": null");
            match serde_json::from_str::<RoCrate>(&normalised) {
                Ok(rocrate) => {
                    if validation_level == 0 {
                        Ok(rocrate)
                    } else {
                        match validity_wrapper(&rocrate, validation_level) {
                            Ok(_) => Ok(rocrate),
                            Err(e) => Err(e),
                        }
                    }
                }
                Err(e) => {
                    error!("1 | failed at rocrate parse");
                    Err(CrateReadError::from(e))
                }
            }
        }
    }
}

/// Reads and deserialises an RO-Crate from a remote location.
///
/// This function attempts to load an RO-Crate from remote location.
/// If 'valid' is '2', it also validates the crate's keys against the RO-Crate schema.
///
/// # Arguments
/// * 'url' - An url containing a valid ro-crate json object
/// * 'valid' - A boolean flag indiciating whether to validate the crate's keys against the schema.
pub fn load_remote(url: url::Url, validation_level: i8) -> Result<RoCrate, CrateReadError> {
    let content = reqwest::blocking::get(url)
        .map_err(|e| CrateReadError::ReqwestError(e))?
        .text()
        .map_err(|e| CrateReadError::ReqwestError(e))?;

    read_crate_obj(&content, validation_level)
}

/// Validation logic
fn validity_wrapper(rocrate: &RoCrate, validation_level: i8) -> Result<&RoCrate, CrateReadError> {
    match validate_crate_keys(rocrate) {
        ValidationResult::Valid => Ok(rocrate),
        ValidationResult::Invalid(validation) => {
            if validation_level == 1 {
                warn!(
                    "Warning: Invalid keys: {:?}, Invalid IDs: {:?}, Invalid types: {:?}",
                    validation.invalid_keys, validation.invalid_ids, validation.invalid_types
                );
                Ok(rocrate)
            } else {
                // Return an error describing the invalid keys
                Err(CrateReadError::VocabNotValid(format!(
                    "Invalid keys: {:?}, Invalid IDs: {:?}, Invalid types: {:?}",
                    validation.invalid_keys, validation.invalid_ids, validation.invalid_types
                )))
            }
        }
        ValidationResult::Error(err_msg) => {
            // Return the error from schema loading
            Err(CrateReadError::SchemaError(err_msg))
        }
    }
}

/// Constructs a `PathBuf` from a given file path string.
///
/// This utility function converts a string slice representing a path into a `PathBuf`,
/// facilitating file system operations with the path.
///
/// # Arguments
/// * `path` - A string slice representing the path to be converted.
pub fn crate_path(path: &str) -> PathBuf {
    Path::new(path).to_path_buf()
}

/// Enumerates potential errors encountered while reading and validating an RO-Crate.
///
/// This enum provides detailed categorization of errors that can occur during the process of
/// reading an RO-Crate from a file and optionally validating its keys against the schema.
///
/// Variants:
/// - `IoError`: Encapsulates errors related to input/output operations, typically file reading issues.
/// - `JsonError`: Covers errors arising from parsing the crate's JSON content.
/// - `VocabNotValid`: Indicates that the crate's keys did not validate against the expected vocabulary, including a message detailing the issue.
#[derive(Error, Debug)]
pub enum CrateReadError {
    #[error("{0}")]
    IoError(#[from] io::Error),
    #[error("{0}")]
    JsonError(#[from] serde_json::Error),
    #[error("{0}")]
    VocabNotValid(String),
    #[error("{0}")]
    SchemaError(String),
    ReqwestError(reqwest::Error),
}

impl std::fmt::Display for CrateReadError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            CrateReadError::IoError(ref err) => write!(f, "IO error: {}", err),
            CrateReadError::JsonError(ref err) => write!(f, "Json error: {}", err),
            CrateReadError::VocabNotValid(ref err) => write!(f, "Vocab not valid: {}", err),
            CrateReadError::SchemaError(ref err) => write!(f, "Schema not valid: {}", err),
            CrateReadError::ReqwestError(ref err) => write!(f, "Reqwest error: {}", err),
        }
    }
}

/// Implements the standard Error trait for ZipError.
///
/// This allows `ZipError` to integrate with Rust's error handling ecosystem, enabling it to be
/// returned and handled in contexts where a standard error type is expected.
impl std::error::Error for CrateReadError {}

impl PartialEq for CrateReadError {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            // We don't compare the actual io::Error or serde_json::Error, just the variant type
            (CrateReadError::IoError(_), CrateReadError::IoError(_)) => true,
            (CrateReadError::JsonError(_), CrateReadError::JsonError(_)) => true,
            // For `VocabNotValid`, we compare the actual error message
            (CrateReadError::VocabNotValid(a), CrateReadError::VocabNotValid(b)) => a == b,
            _ => false,
        }
    }
}

/// Validates that the keys in a given RO-Crate match those defined in the base schema vocabulary.
///
/// This function checks the crate's properties against the official RO-Crate context and any embedded vocabularies.
/// It does not validate properties by dereferencing URIs but rather checks if the properties' keys are recognized.
pub fn validate_crate_keys(rocrate: &RoCrate) -> ValidationResult {
    let version = match rocrate.get_rocrate_version() {
        Some(v) => v,
        None => {
            return ValidationResult::Error(
                "Could not determine RO-Crate schema version from context".to_string(),
            );
        }
    };
    match load_rocrate_schema(version) {
        Ok(crate_metadata) => {
            let crate_context: Vec<String> = crate_metadata.context.keys().cloned().collect();
            let custom_context = rocrate.get_context_items();
            let vals = RoCrate::get_all_properties(rocrate);

            // Convert vec1 and vec2 to HashSets for efficient lookup
            let set1: HashSet<_> = crate_context.into_iter().collect();
            let set2: HashSet<_> = custom_context.into_iter().collect();

            let mut validation = CrateValidation {
                invalid_keys: Vec::new(),
                invalid_ids: Vec::new(),
                invalid_types: Vec::new(),
            };

            for item in &vals {
                if !set1.contains(item) && !set2.contains(item) {
                    validation.invalid_keys.push(item.clone());
                }
            }

            if validation.is_valid() {
                ValidationResult::Valid
            } else {
                ValidationResult::Invalid(validation)
            }
        }
        Err(e) => ValidationResult::Error(format!("Failed to load Ro-Crate schema: {}", e)),
    }
}

pub struct CrateValidation {
    pub invalid_keys: Vec<String>,
    pub invalid_ids: Vec<String>,
    pub invalid_types: Vec<String>,
}

impl CrateValidation {
    // Method to check if all validation results are empty (valid crate)
    pub fn is_valid(&self) -> bool {
        self.invalid_keys.is_empty() && self.invalid_ids.is_empty() && self.invalid_types.is_empty()
    }

    // Method to check if any of the vectors have invalid data
    pub fn has_any_invalid(&self) -> bool {
        !self.invalid_keys.is_empty()
            || !self.invalid_ids.is_empty()
            || !self.invalid_types.is_empty()
    }

    // Method to display the invalid data
    pub fn report_invalid(&self) {
        if !self.invalid_keys.is_empty() {
            info!("Invalid keys: {:?}", self.invalid_keys);
        }
        if !self.invalid_ids.is_empty() {
            info!("Invalid IDs: {:?}", self.invalid_ids);
        }
        if !self.invalid_types.is_empty() {
            info!("Invalid types: {:?}", self.invalid_types);
        }
    }
}

/// Extracts an ro-crate-metadata.json file from a zipped ro-crate
pub fn parse_zip(zip_path: &str, validation_level: i8) -> Result<RoCrate, CrateReadError> {
    // 1. Open the .zip file
    let file = fs::File::open(zip_path).unwrap();
    let mut archive = ZipArchive::new(file).unwrap();

    // 2. Retrieve the file by name
    let mut file_in_zip = archive.by_name("ro-crate-metadata.json").unwrap();

    // 3. Read the file contents into memory
    let mut buffer = Vec::new();
    file_in_zip.read_to_end(&mut buffer)?;

    match serde_json::from_slice::<RoCrate>(&buffer) {
        Ok(rocrate) => {
            if validation_level == 0 {
                Ok(rocrate)
            } else {
                match validity_wrapper(&rocrate, validation_level) {
                    Ok(_) => Ok(rocrate),
                    Err(e) => Err(e),
                }
            }
        }
        Err(e) => Err(CrateReadError::from(e)),
    }
}

pub enum ValidationResult {
    Valid,
    Invalid(CrateValidation),
    Error(String),
}

#[cfg(test)]
mod tests {

    use super::*;

    fn fixture_path(relative_path: &str) -> PathBuf {
        Path::new("tests/fixtures").join(relative_path)
    }

    #[test]
    fn test_read_crate_success() {
        let path = fixture_path("_ro-crate-metadata-minimal.json");

        let crate_result = read_crate(&path, 0);
        println!("{:?}", crate_result);
        assert!(crate_result.is_ok());
    }

    #[test]
    fn test_read_crate_valid() {
        let path = fixture_path("_ro-crate-metadata-minimal.json");

        let crate_result = read_crate(&path, 2);
        assert!(crate_result.is_ok());
    }

    #[test]
    fn test_parse_zip() {
        let zip = parse_zip("tests/fixtures/zip_test/fixtures.zip", 0);
        assert!(zip.is_ok());
    }

    #[test]
    fn test_read_crate_invalid() {
        let path = fixture_path("_ro-crate-metadata-broken-schema.json");

        let crate_result = read_crate(&path, 2).unwrap_err();
        match crate_result {
            CrateReadError::VocabNotValid(_) => (),
            _ => panic!(),
        }
    }

    #[test]
    fn test_read_crate_invalid_error() {
        let path = fixture_path("_ro-crate-metadata-broken-schema.json");

        let crate_result = read_crate(&path, 2);

        println!("{:?}", crate_result);
    }

    #[test]
    fn test_read_crate_file_not_found() {
        let path = fixture_path("non_existent_file.json");

        let crate_result = read_crate(&path, 0);
        match crate_result {
            Err(CrateReadError::IoError(ref e)) if e.kind() == io::ErrorKind::NotFound => (),
            _ => panic!("Expected file not found error"),
        }
    }

    #[test]
    fn test_read_crate_invalid_json() {
        let path = fixture_path("invalid.json");

        let crate_result = read_crate(&path, 0);
        match crate_result {
            Err(CrateReadError::JsonError(_)) => (),
            _ => panic!("Expected JSON parsing error"),
        }
    }

    #[test]
    fn test_crate_read_error_from_io_error() {
        let io_error = io::Error::new(io::ErrorKind::Other, "io error");
        let crate_error: CrateReadError = io_error.into();
        matches!(crate_error, CrateReadError::IoError(_));
    }

    // RO-Crate 1.2 tests

    #[test]
    fn test_read_crate_1_2_success() {
        let path = fixture_path("_ro-crate-metadata-minimal-1_2.json");

        let crate_result = read_crate(&path, 0);
        println!("{:?}", crate_result);
        assert!(crate_result.is_ok());
    }

    #[test]
    fn test_read_crate_1_2_dynamic() {
        let path = fixture_path("_ro-crate-metadata-dynamic-1_2.json");

        let crate_result = read_crate(&path, 0);
        println!("{:?}", crate_result);
        assert!(crate_result.is_ok());
    }

    #[test]
    fn test_read_crate_1_2_complex_context() {
        let path = fixture_path("_ro-crate-metadata-complex-context-1_2.json");

        let crate_result = read_crate(&path, 0);
        println!("{:?}", crate_result);
        assert!(crate_result.is_ok());
    }
}
