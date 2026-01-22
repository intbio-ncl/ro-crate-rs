//! Module for writing RoCrate structures to file.
//!
//! Allows basic ro-crate-metadata.json file creation, as well as archiving
//! via zip.

use crate::ro_crate::read::{read_crate, CrateReadError};
use crate::ro_crate::rocrate::RoCrate;
use dirs;
use log::{debug, error};
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use thiserror::Error;
use url::Url;
use walkdir::WalkDir;
use zip::{write::SimpleFileOptions, ZipWriter};

#[derive(Error, Debug)]
pub enum WriteError {
    #[error("IO error: {0}")]
    IoError(#[from] io::Error),
    #[error("IO error: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("Directory vector is empty")]
    EmptyDirectoryVector,
    #[error("File name not found")]
    FileNameNotFound,
    #[error("Failed to convert file name")]
    FileNameConversionFailed,
    #[error("Path error: {0}")]
    PathError(#[from] std::path::StripPrefixError),
    #[error("Zip operation Error: {0}")]
    ZipOperationError(String),
    #[error("Context Error: {0}")]
    ContextError(String),
    #[error("Read Error: {0}")]
    ReadError(#[from] CrateReadError),
}

/// Serializes and writes an RO-Crate object to a JSON file.
///
/// This function serializes the given `RoCrate` object into a pretty-printed JSON format and writes it
/// to a file with the specified `name`. The function uses basic error handling, printing error messages
/// to standard error without returning or propagating them, which is noted as an area for future improvement.
///
///
/// # Arguments
/// * `rocrate` - A reference to the `RoCrate` object to serialize.
/// * `name` - The name of the file to which the serialized JSON should be written.
///
/// # Notes
/// Current error handling within this function is minimal, relying on printing to stderr. It is recommended
/// to update this function to return a `Result` type in future revisions for better error handling and integration
/// with calling code.
pub fn write_crate(rocrate: &RoCrate, name: String) -> Result<(), WriteError> {
    let json_ld = serde_json::to_string_pretty(&rocrate)?;
    let mut file = File::create(name)?;
    writeln!(file, "{}", json_ld)?;
    Ok(())
}

/// Serializes an RO-Crate object and writes it directly to a zip file.
///
/// This method allows for a modified RO-Crate to be efficiently serialized and saved into a zip archive
/// without overwriting the original data. It preserves file paths that are
/// relative or absolute in the original crate, whilst mapping the new relatives of the zip file.
/// The function also supports the potential remapping of all data entity IDs within the crate.
///
/// # Arguments
/// * `rocrate` - A reference to the `RoCrate` object to serialize and save.
/// * `name` - The name under which the serialized crate will be stored in the zip file.
/// * `zip` - A mutable reference to the `ZipWriter` used for writing to the zip file.
/// * `options` - ZipFile options to use when creating the new file in the zip archive.
///
/// # Returns
/// A `Result<(), WriteError>` indicating the success or failure of the operation.
fn write_crate_to_zip(
    rocrate: &RoCrate,
    name: String,
    zip_data: &mut RoCrateZip,
) -> Result<(), WriteError> {
    // Attempt to serialize the RoCrate object to a pretty JSON string
    let json_ld = serde_json::to_string_pretty(&rocrate)
        .map_err(|e| WriteError::ZipOperationError(e.to_string()))?;

    // Start a new file in the zip archive with the given name and options
    zip_data
        .zip
        .start_file(name, zip_data.options)
        .map_err(|e| WriteError::ZipOperationError(e.to_string()))?;

    zip_data
        .zip
        .write_all(json_ld.as_bytes())
        .map_err(|e| WriteError::ZipOperationError(e.to_string()))?;

    // If everything succeeded, return Ok(())
    Ok(())
}

/// Writes the contents of an RO-Crate directory to a zip file.
///
/// This function compresses an entire RO-Crate directory, including all files within the directory structure,
/// into a single zip archive. It's designed to include every file present, without checking their relevance
/// to the crate's metadata, based on the principle that all files in the directory are part of the research
/// data or experiment. If external is true, it will grab and copy external data files
/// to a new `external` folder within the zip. This can increase storage costs, but allows
/// exhaustive capture of data state.
///
/// # Arguments
/// * `crate_path` - The path to the RO-Crate file within crate to zip.
/// * `external` - A boolean flag indicating whether to apply special handling for external resources.
///
/// # Returns
/// A `Result<(), WriteError>` reflecting the success or failure of the operation.
///
/// # Notes
/// The function currently zips everything in the given directory, without analyzing the crate's metadata
/// to selectively include files. This approach ensures no potentially relevant data is omitted but may include
/// unnecessary files. Future versions might consider more selective zipping based on the crate's actual contents.
///
/// # Examples
/// ```
/// let crate_path = Path::new("/path/to/ro-crate-directory/ro-crate-metadata.json");
/// zip_crate(crate_path, false)?;
/// ```
pub fn zip_crate(
    crate_path: &Path,
    external: bool,
    validation_level: i8,
    flatten: bool,
    unique: bool,
) -> Result<(), WriteError> {
    // After prepping create the initial zip file
    let mut zip_paths = construct_paths(crate_path)?;
    debug!("{:?}", &zip_paths);

    // Opens target crate ready for update
    let mut rocrate = read_crate(&zip_paths.absolute_path, validation_level)?;

    // Attach unique identifier if not already present
    rocrate.context.add_urn_uuid();
    // This saves a modified copy with the updated urn -> prevents duplicate if already
    // present
    write_crate(
        &rocrate,
        zip_paths
            .absolute_path
            .canonicalize()?
            .to_string_lossy()
            .to_string(),
    )?;
    if unique {
        let base_id = rocrate
            .context
            .get_specific_context("@base")
            .ok_or_else(|| WriteError::ContextError("`@base` not found".to_string()))?;

        let stripped_id = format!(
            "{}.zip",
            base_id
                .strip_prefix("urn:uuid:")
                .ok_or_else(|| WriteError::ContextError(
                    "`urn:uuid:` prefix not found".to_string()
                ))?
        );
        zip_paths.zip_file_name = zip_paths.root_path.join(stripped_id);
    }
    debug!("ZIP PATH NAME {:?}", zip_paths.zip_file_name);

    let mut zip_data = build_zip(&zip_paths)?;

    let _ = directory_walk(&mut rocrate, &zip_paths, &mut zip_data, flatten);

    if external {
        zip_data = zip_crate_external(&mut rocrate, zip_data, &zip_paths)?
    }

    let _ = write_crate_to_zip(
        &rocrate,
        "ro-crate-metadata.json".to_string(),
        &mut zip_data,
    );

    let _ = zip_data.zip.finish();

    Ok(())
}
#[derive(Debug)]
pub struct RoCrateZipPaths {
    absolute_path: PathBuf,
    root_path: PathBuf,
    zip_file_name: PathBuf,
}

fn construct_paths(crate_path: &Path) -> Result<RoCrateZipPaths, WriteError> {
    // TODO: add multiple options for walking/compression e.g follow symbolic links etc.
    let absolute_path = crate_path.canonicalize()?;
    let root_path = absolute_path
        .parent()
        .ok_or_else(|| WriteError::FileNameNotFound)?
        .to_path_buf();

    let zip_file_base_name = root_path
        .file_name()
        .ok_or(WriteError::FileNameNotFound)?
        .to_str()
        .ok_or(WriteError::FileNameConversionFailed)?;

    let zip_file_name = root_path.join(format!("{}.zip", zip_file_base_name));
    Ok(RoCrateZipPaths {
        absolute_path,
        root_path,
        zip_file_name,
    })
}

fn build_zip(path_information: &RoCrateZipPaths) -> Result<RoCrateZip, WriteError> {
    let file = File::create(&path_information.zip_file_name).map_err(WriteError::IoError)?;
    let zip = ZipWriter::new(file);

    // Can change this to deflated for standard compression
    let options = SimpleFileOptions::default()
        .compression_method(zip::CompressionMethod::Deflated)
        .unix_permissions(0o755)
        .large_file(true);

    Ok(RoCrateZip { zip, options })
}

pub struct RoCrateZip {
    zip: ZipWriter<File>,
    options: SimpleFileOptions,
}

/// Sole focus must be on present data
/// As defined by the ro-crate-metadata file, it looks at the root and then
/// every file within it belongs to the crate. Whilst not everything is
/// described in the ro-crate-metadata itself as per spec, it absolutely should
/// get everything that is within the crate
fn directory_walk(
    rocrate: &mut RoCrate,
    zip_paths: &RoCrateZipPaths,
    zip_data: &mut RoCrateZip,
    flatten: bool,
) -> Result<Vec<PathBuf>, WriteError> {
    let mut data_vec: Vec<PathBuf> = Vec::new();
    let contained = get_noncontained_data_entites(rocrate, zip_paths, true)?;

    for entry in WalkDir::new(&zip_paths.root_path)
        .min_depth(0)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().is_file())
    // Consider only files, not directories
    {
        let path = entry.path();

        if path == zip_paths.zip_file_name {
            continue;
        }

        if path == zip_paths.absolute_path {
            continue;
        }

        let file_name: String = if flatten {
            path.file_name()
                .ok_or(WriteError::FileNameNotFound)?
                .to_str()
                .ok_or(WriteError::FileNameConversionFailed)?
                .to_string()
        } else {
            path.strip_prefix(&zip_paths.root_path)
                .map_err(WriteError::PathError)?
                .to_str()
                .ok_or(WriteError::FileNameConversionFailed)?
                .to_string()
        };

        let mut file = fs::File::open(path).map_err(WriteError::IoError)?;

        zip_data
            .zip
            .start_file(&file_name, zip_data.options)
            .map_err(|e| WriteError::ZipOperationError(e.to_string()))?;

        // Once copy the absolute path and relative path needs to be checked
        let abs_path = path.canonicalize()?;
        if abs_path.is_file() {
            data_vec.push(abs_path.clone());
        };

        let copy_result = io::copy(&mut file, &mut zip_data.zip).map_err(WriteError::IoError);

        match copy_result {
            Ok(_) => {
                for (key, value) in &contained {
                    if abs_path == value.clone() {
                        rocrate.update_id_recursive(key, &file_name)
                    }
                }
            }
            Err(e) => error!("{e}"),
        }
    }
    debug!("0 | Rocrate: {:?}", rocrate);
    Ok(data_vec)
}

/// Packages an RO-Crate and its external files into a zip archive, updating IDs as necessary.
///
/// This function is designed for RO-Crates that reference external files. It packages the crate
/// and any external files into a single zip archive, ensuring that all data entities, whether
/// internal or external to the crate directory, are included. Additionally, it updates the IDs
/// of packaged entities to reflect their new paths within the archive.
///
/// # Arguments
/// * `rocrate` - A mutable reference to the `RoCrate` object being packaged.
/// * `crate_path` - The filesystem path to the directory containing the RO-Crate's metadata and data entities.
/// * `zip` - A `ZipWriter<File>` for writing to the zip archive.
/// * `options` - `SimpleFileOptions` determining how files are added to the archive (e.g., compression level).
///
/// # Returns
/// Returns a `Result` containing the updated `ZipWriter<File>` on success, or a `WriteError` on failure,
/// encapsulating any errors that occurred during the operation.
pub fn zip_crate_external(
    rocrate: &mut RoCrate,
    mut zip_data: RoCrateZip,
    crate_path: &RoCrateZipPaths,
) -> Result<RoCrateZip, WriteError> {
    // This parses all the IDs and generates a list of paths that are not
    // contained within the crate itself.
    let noncontained = get_noncontained_data_entites(rocrate, crate_path, false)?;

    // if noncontained is not empty, means data entities are external
    // therefore we need to package them
    if !noncontained.is_empty() {
        for (id, path) in noncontained {
            // norels = path to file, then we use external path to get folder then add basename
            let file_name = path
                .file_name()
                .ok_or(WriteError::FileNameNotFound)?
                .to_str()
                .ok_or(WriteError::FileNameConversionFailed)?;
            let zip_entry_name = format!("external/{}", file_name);

            if path.is_dir() {
                // It's a directory -> recursively add all of its contents
                add_directory_recursively(&path, &zip_entry_name, &mut zip_data)?;

                rocrate.update_id_recursive(&id, &zip_entry_name);
            } else if path.is_file() {
                let mut file = fs::File::open(&path).map_err(WriteError::IoError)?;

                zip_data
                    .zip
                    .start_file(&zip_entry_name, zip_data.options)
                    .map_err(|e| WriteError::ZipOperationError(e.to_string()))?;

                let copy_result =
                    io::copy(&mut file, &mut zip_data.zip).map_err(WriteError::IoError);
                match copy_result {
                    Ok(_) => {
                        rocrate.update_id_recursive(&id, &zip_entry_name);
                    }
                    Err(e) => return Err(e),
                }
            } else {
                debug!("Skipping non-file, non-directory: {:?}", path);
            }
        }
    }

    Ok(zip_data)
}

/// Gets all the described data entities of a crate and filters for
/// files that are not in the crate structure
fn get_noncontained_data_entites(
    rocrate: &mut RoCrate,
    crate_path: &RoCrateZipPaths,
    inverse: bool,
) -> Result<HashMap<String, PathBuf>, WriteError> {
    // Get all IDs for the target crate
    // Assumed that all data entities are discrete objects, and that not file
    // has been referenced without being described
    let mut ids = rocrate.get_all_ids();

    // Pop all non-urls and remove root/ metadata descriptor
    ids.retain(|id| is_not_url(id));

    get_noncontained_paths(ids, &crate_path.root_path, inverse)
}

/// Identifies file paths that are not relative to the given RO-Crate directory.
///
/// When preparing an RO-Crate for zipping, it's important to include all related files, even those
/// not stored within the crate's directory. This function helps identify such external files.
///
/// # Arguments
/// * `ids` - A vector of strings representing the IDs (paths) to check.
/// * `crate_dir` - The base directory of the RO-Crate.
///
fn get_noncontained_paths(
    ids: Vec<&String>,
    crate_dir: &Path,
    inverse: bool,
) -> Result<HashMap<String, PathBuf>, WriteError> {
    let mut nonrels: HashMap<String, PathBuf> = HashMap::new();

    // Get the absolute path of the crate directory
    let rocrate_path = crate_dir.canonicalize()?;
    debug!("crate path {:?} and target id {:?}", rocrate_path, ids);

    // Iterate over all the ids, check if the paths are relative to the crate.
    // EVERYTHING NEEDS TO BE WITHIN THE CRATE
    for id in ids.iter() {
        // Skip IDs that are fragment references (i.e., starting with '#')
        if id.starts_with('#') {
            continue;
        }

        // Resolve the absolute path of the current ID
        if let Some(path) = Path::new(id).canonicalize().ok() {
            // Path exists and was canonicalized
            debug!("Absolute path: {:?}", path);
            // Check if the path is outside the base crate directory
            if is_outside_base_folder(&rocrate_path, &path) || inverse {
                nonrels.insert(id.to_string(), path);
            }
        } else if Path::new(id).is_absolute() {
            // Path is absolute but doesn't exist - still flag if outside crate
            debug!("Non-existent absolute path: {:?}", id);
            let path = Path::new(id).to_path_buf();
            if is_outside_base_folder(&rocrate_path, &path) || inverse {
                nonrels.insert(id.to_string(), path);
            }
        } else {
            debug!("ID: {:?}", id);
            let expanded_path = match Path::new(id).canonicalize() {
                Ok(resolved) => resolved,
                Err(e) if e.kind() == io::ErrorKind::NotFound => resolve_tilde_path(id),
                Err(e) => {
                    error!("{e}");
                    continue;
                }
            };
            debug!("Pre Resolved path: {:?}", expanded_path);

            // If the expanded path is absolute (e.g., tilde-expanded), handle it like other
            // absolute paths - check if outside base folder even if it doesn't exist
            if expanded_path.is_absolute() {
                if is_outside_base_folder(&rocrate_path, &expanded_path) || inverse {
                    nonrels.insert(id.to_string(), expanded_path);
                }
                continue;
            }

            let resolved_path = rocrate_path.join(expanded_path).canonicalize();
            debug!("Resolved path: {:?}", resolved_path);
            match resolved_path {
                Ok(abs_path) => {
                    debug!("Can confirm: {:?}", abs_path);
                    if abs_path.exists() {
                        debug!("Exists: {:?}", abs_path);
                        if is_outside_base_folder(&rocrate_path, &abs_path) || inverse {
                            nonrels.insert(id.to_string(), abs_path);
                        }
                    } else {
                        //debug!("8 | Failed to resolve ID: {:?}", id);
                    }
                }
                Err(e) => {
                    error!("{}", e)
                }
            }
        }
    }

    Ok(nonrels)
}

fn resolve_tilde_path(path: &str) -> PathBuf {
    if let Some(home_dir) = dirs::home_dir() {
        if path.starts_with("~") {
            return home_dir.join(path.strip_prefix("~/").unwrap_or(""));
        }
    }
    Path::new(path).to_path_buf()
}
/// Converts a relative path to an absolute one, if possible.
///
/// This utility function is useful for obtaining the absolute path representation of a file or directory.
///
/// # Arguments
/// * `relative_path` - The path to be converted to its absolute form.
///
/// # Returns
/// An `Option<PathBuf>` containing the absolute path, if the conversion was successful; otherwise, `None`.
//fn get_absolute_path(relative_path: &Path) -> Option<PathBuf> {
//    fs::canonicalize(relative_path).ok()
//}

/// Determines whether a given string is not a URL.
///
/// This function checks if the provided string represents a file path rather than a URL. It's particularly
/// useful when filtering a list of identifiers to distinguish between web resources and local files.
///
/// # Arguments
/// * `path` - The string to check.
///
/// # Returns
/// `true` if the string is likely a file path; otherwise, `false`.
///
/// # Examples
/// ```
/// assert!(is_not_url("/path/to/file"));
/// assert!(!is_not_url("http://example.com"));
/// ```
pub fn is_not_url(path: &str) -> bool {
    // Check if the path is likely a Windows extended-length path
    let is_extended_windows_path = path.starts_with(r"\\?\");

    // Check if the path is likely a normal file path
    let is_normal_file_path = path.starts_with(r"\\") // UNC path
        || path.chars().next().map(|c| c.is_alphabetic() && path.chars().nth(1) == Some(':')).unwrap_or(false) // Drive letter, e.g., C:\
        || path.starts_with('/') // Unix-style path
        || path.starts_with('.') // Relative path
        || path.starts_with("file:");

    // If it looks like a file path, return true early
    if path.contains("ro-crate-metadata.json") || path == "./" {
        return false;
    }

    if is_extended_windows_path || is_normal_file_path {
        return true;
    }

    Url::parse(path).is_err()
}

/// Checks if a given file path lies outside of a specified base folder.
///
/// This function is critical in identifying external resources that need special handling when
/// preparing an RO-Crate for packaging or distribution.
///
/// # Arguments
/// * `base_folder` - The base directory against which to compare.
/// * `file_path` - The path of the file to check.
///
/// # Returns
/// `true` if the file is outside the base folder; otherwise, `false`.
///
/// # Examples
/// ```
/// let base_folder = Path::new("/path/to/base");
/// let file_path = Path::new("/path/to/base/subdir/file");
/// assert!(!is_outside_base_folder(base_folder, file_path));
/// ```
fn is_outside_base_folder(base_folder: &Path, file_path: &Path) -> bool {
    // Compare the given file path with the base folder path
    debug!("Base folder: {:?} | file path {:?}", base_folder, file_path);
    !file_path.starts_with(base_folder)
}

/// Recursively adds an entire directory (and subdirectories) into the ZIP under `zip_prefix/…`.
fn add_directory_recursively(
    base_dir: &Path,
    zip_prefix: &str,
    zip_data: &mut RoCrateZip,
) -> Result<(), WriteError> {
    // WalkDir will yield subdirectories and files.
    for entry in WalkDir::new(base_dir).into_iter().filter_map(|e| e.ok()) {
        let p = entry.path();
        debug!("p = {:?}", p);

        // Figure out the path we want inside the zip. That’s basically:
        //   zip_prefix + (path relative to base_dir)
        let relative_subpath = match p.strip_prefix(base_dir) {
            Ok(sub) => sub,
            Err(e) => return Err(WriteError::PathError(e)),
        };

        // For example: "mydir/subdir/file.txt"
        let zip_entry_name = format!("{}/{}", zip_prefix, relative_subpath.display());
        debug!("zp entry name: {:?}", zip_entry_name);
        if p.is_dir() {
            // Optional: add an explicit directory entry in the archive:
            zip_data
                .zip
                .add_directory(zip_entry_name, zip_data.options)
                .map_err(|e| WriteError::ZipOperationError(e.to_string()))?;
        } else if p.is_file() {
            let mut file = fs::File::open(p).map_err(WriteError::IoError)?;
            debug!("FILE: {:?}", file);
            zip_data
                .zip
                .start_file(&zip_entry_name, zip_data.options)
                .map_err(|e| WriteError::ZipOperationError(e.to_string()))?;
            io::copy(&mut file, &mut zip_data.zip).map_err(WriteError::IoError)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod write_crate_tests {
    use super::*;
    use crate::ro_crate::read::{parse_zip, read_crate};
    use dirs::home_dir;
    use std::collections::HashMap;
    use std::fs;
    use std::path::Path;
    use std::path::PathBuf;

    use std::io::Write;

    use serde_json::{json, Value};

    fn minimal_test_experiment_rocrate(tempdir: PathBuf) -> Value {
        let mut data = std::fs::File::create_new(tempdir.join("data.csv")).unwrap();
        data.write_all(
            b"subject,object
1,2",
        )
        .unwrap();
        json!({
           "@context": "https://w3id.org/ro/crate/1.1/context",
           "@graph": [
              {
                 "@type": "CreativeWork",
                 "@id": "ro-crate-metadata.json",
                 "conformsTo": {"@id": "https://w3id.org/ro/crate/1.1"},
                 "about": {"@id": "./"}
              },
              {
                 "@id": "./",
                 "identifier": "https://doi.org/10.4225/59/59672c09f4a4b",
                 "@type": "Dataset",
                 "datePublished": "2017",
                 "name": "Data files associated with the manuscript:Effects of facilitated family case conferencing for ...",
                 "description": "Palliative care planning for nursing home residents with advanced dementia ...",
                 "license": {"@id": "https://creativecommons.org/licenses/by-nc-sa/3.0/au/"},
                 "hasPart": [
                     {
                         "@id": "../external.txt"
                     },
                     {
                         "@id": tempdir.join("data.csv").canonicalize().unwrap(),
                     },
                     {
                         "@id": "text_1.txt"
                     }
                 ]
              },
              {
                 "@id": "https://creativecommons.org/licenses/by-nc-sa/3.0/au/",
                 "@type": "CreativeWork",
                 "description": "This work is licensed under the Creative Commons Attribution-NonCommercial-ShareAlike 3.0 Australia License. To view a copy of this license, visit http://creativecommons.org/licenses/by-nc-sa/3.0/au/ or send a letter to Creative Commons, PO Box 1866, Mountain View, CA 94042, USA.",
                 "identifier": "https://creativecommons.org/licenses/by-nc-sa/3.0/au/",
                 "name": "Attribution-NonCommercial-ShareAlike 3.0 Australia (CC BY-NC-SA 3.0 AU)",
                 "value": null
              },
              {
                 "@id": "../external.txt",
                 "@type": "File",
                 "name": "External data"
              },
                    {
                 "@id": tempdir.join("data.csv").canonicalize().unwrap(), // TODO: Fix this
                 "@type": "File",
                 "name": "CSV data"
              },      {
                 "@id": "text_1.txt",
                 "@type": "File",
                 "name": "text data"
              }
           ]
        })
    }

    pub fn create_tempcrate(value_fn: &dyn Fn(PathBuf) -> Value) -> tempfile::TempDir {
        let tempdir = tempfile::tempdir().unwrap();
        let json = value_fn(tempdir.path().to_path_buf());
        let mut ro_crate =
            std::fs::File::create_new(tempdir.path().join("ro-crate-metadata.json")).unwrap();
        ro_crate
            .write_all(&serde_json::to_vec(&json).unwrap())
            .unwrap();
        tempdir
    }

    fn ro_crate_minimal(_: PathBuf) -> serde_json::Value {
        serde_json::json!(
        { "@context": "https://w3id.org/ro/crate/1.1/context",
          "@graph": [

         {
            "@type": "CreativeWork",
            "@id": "ro-crate-metadata.json",
            "conformsTo": {"@id": "https://w3id.org/ro/crate/1.1"},
            "about": {"@id": "./"}
         },
         {
            "@id": "./",
            "identifier": "https://doi.org/10.4225/59/59672c09f4a4b",
            "@type": "Dataset",
            "datePublished": "2017",
            "name": "Data files associated with the manuscript:Effects of facilitated family case conferencing for ...",
            "description": "Palliative care planning for nursing home residents with advanced dementia ...",
            "license": {"@id": "https://creativecommons.org/licenses/by-nc-sa/3.0/au/"},
            "value": null
         },
         {
          "@id": "https://creativecommons.org/licenses/by-nc-sa/3.0/au/",
          "@type": "CreativeWork",
          "description": "This work is licensed under the Creative Commons Attribution-NonCommercial-ShareAlike 3.0 Australia License. To view a copy of this license, visit http://creativecommons.org/licenses/by-nc-sa/3.0/au/ or send a letter to Creative Commons, PO Box 1866, Mountain View, CA 94042, USA.",
          "identifier": "https://creativecommons.org/licenses/by-nc-sa/3.0/au/",
          "name": "Attribution-NonCommercial-ShareAlike 3.0 Australia (CC BY-NC-SA 3.0 AU)",
          "value": null
         }
         ]
        }
            )
    }

    #[test]
    fn test_construct_paths() {
        let workdir = tempfile::tempdir().unwrap();

        // Create test_experiment dir
        let dir_path = workdir.path().join("test_experiment");
        std::fs::create_dir(&dir_path).unwrap();

        let file_path = workdir
            .path()
            .join("test_experiment/_ro-crate-metadata-minimal.json");
        let _file = std::fs::File::create_new(&file_path).unwrap();

        let paths = construct_paths(&file_path).unwrap();

        assert_eq!(paths.absolute_path, file_path.canonicalize().unwrap());
        assert_eq!(paths.root_path, dir_path.canonicalize().unwrap());
        assert_eq!(
            paths.zip_file_name,
            dir_path.canonicalize().unwrap().join("test_experiment.zip")
        );
    }

    #[test]
    fn test_directory_walk() {
        let workdir = tempfile::tempdir().unwrap();

        // Create test_experiment dir
        let dir_path = workdir.path().join("test_experiment");
        std::fs::create_dir(&dir_path).unwrap();

        // Create test files
        let file_path = workdir
            .path()
            .join("test_experiment/_ro-crate-metadata-minimal.json");
        let mut file = std::fs::File::create_new(&file_path).unwrap();
        file.write_all(&serde_json::to_vec(&ro_crate_minimal(dir_path.clone())).unwrap())
            .unwrap();
        let data = workdir.path().join("test_experiment/data.csv");
        std::fs::File::create_new(&data).unwrap();
        let text = workdir.path().join("test_experiment/text_1.txt");
        std::fs::File::create_new(&text).unwrap();

        let zip_paths = RoCrateZipPaths {
            absolute_path: file_path.clone(),
            root_path: dir_path.clone(),
            zip_file_name: dir_path.clone().join(PathBuf::from("test_experiment.zip")),
        };

        let mut zip_data = RoCrateZip {
            zip: ZipWriter::new(File::create(&zip_paths.zip_file_name).unwrap()),
            options: SimpleFileOptions::default()
                .compression_method(zip::CompressionMethod::Deflated),
        };

        let mut rocrate = read_crate(&file_path, 0).unwrap();

        let mut directory_contents =
            directory_walk(&mut rocrate, &zip_paths, &mut zip_data, false).unwrap();

        let mut test_vec: Vec<PathBuf> = vec![
            dir_path.join(PathBuf::from("data.csv")),
            dir_path.join(PathBuf::from("text_1.txt")),
        ];

        directory_contents.sort();
        test_vec.sort();

        assert_eq!(directory_contents, test_vec);
    }

    fn user_root_unix(mut path_types: HashMap<String, bool>) -> HashMap<String, bool> {
        if !cfg!(windows) {
            // root path
            let mut tempfile = tempfile::NamedTempFile::new().unwrap();
            tempfile.write_all(b"hello root temp").unwrap();
            path_types.insert(
                tempfile
                    .path()
                    .canonicalize()
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .to_string(),
                true,
            );

            // tilde path
            let home = home_dir().unwrap();
            let home_dir = tempfile::tempdir_in(home).unwrap();
            let mut file =
                std::fs::File::create_new(&home_dir.path().join("tempfile.txt")).unwrap();
            file.write_all(b"hello user temp").unwrap();
            let temp_base = home_dir.path().file_name().unwrap().to_string_lossy();
            path_types.insert(format!("~/{}/tempfile.txt", temp_base), true);
        }
        path_types
    }

    #[test]
    fn test_get_noncontained_paths() {
        let workdir = tempfile::tempdir().unwrap();

        // Create test_experiment dir
        let dir_path = workdir.path().join("test_experiment");
        std::fs::create_dir(&dir_path).unwrap();

        // Create test files
        let crate_path = workdir
            .path()
            .join("test_experiment/_ro-crate-metadata-minimal.json");
        let mut file = std::fs::File::create_new(&crate_path).unwrap();
        file.write_all(&serde_json::to_vec(&ro_crate_minimal(dir_path.clone())).unwrap())
            .unwrap();
        let data = workdir.path().join("test_experiment/data.csv");
        std::fs::File::create_new(&data).unwrap();
        let text = workdir.path().join("test_experiment/text_1.txt");
        std::fs::File::create_new(&text).unwrap();
        let readme = workdir.path().join("README.md");
        std::fs::File::create_new(&readme).unwrap();
        let external = workdir.path().join("external.txt");
        let mut file = std::fs::File::create_new(&external).unwrap();
        file.write_all("external data test".as_bytes()).unwrap();
        let invalid = workdir.path().join("invalid.json");
        let mut file = std::fs::File::create_new(&invalid).unwrap();
        file.write_all("Invalid JSON".as_bytes()).unwrap();

        let mut path_types: HashMap<String, bool> = HashMap::new();

        path_types.insert("../invalid.json".to_string(), true); // Windows File Path
        path_types.insert("../external.txt".to_string(), true); // Windows File Path
        path_types.insert("./data.csv".to_string(), false); // macOS File Path
        path_types.insert("./text_1.txt".to_string(), false); // Linux Absolute Path
        path_types.insert("text_1.txt".to_string(), false); // Linux Absolute Path
        path_types.insert("#fragment".to_string(), false); // Relative Path

        path_types = user_root_unix(path_types); //Check tilde paths

        // abs path but not relative
        let abs_not = workdir
            .path()
            .join(PathBuf::from("README.md"))
            .to_str()
            .unwrap()
            .to_string();
        path_types.insert(abs_not, true);

        // abs path rel
        let abs_is = dir_path
            .join(PathBuf::from("data.csv"))
            .canonicalize()
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();
        path_types.insert(abs_is, false);

        for (key, value) in path_types {
            let mut input_vec: Vec<&String> = Vec::new();
            let target = key.to_string();
            input_vec.push(&target);

            let test = get_noncontained_paths(input_vec.clone(), &dir_path, false).unwrap();
            println!("{:?}", test);
            if test.is_empty() {
                println!("Test is empty for relative ID: {}", key);
                assert_eq!(value, false)
            } else {
                println!("Test is successful for relative ID: {}", key);
                assert_eq!(value, true)
            }
        }
    }
    #[test]
    fn test_zip_crate_external_func() {
        let workdir = tempfile::tempdir().unwrap();

        // Create test_experiment dir
        let dir_path = workdir.path().join("test_experiment");
        std::fs::create_dir(&dir_path).unwrap();

        // Create test files
        let crate_path = workdir
            .path()
            .join("test_experiment/_ro-crate-metadata-minimal.json");
        let mut file = std::fs::File::create_new(&crate_path).unwrap();
        file.write_all(&serde_json::to_vec(&ro_crate_minimal(dir_path.clone())).unwrap())
            .unwrap();
        let data = workdir.path().join("test_experiment/data.csv");
        std::fs::File::create_new(&data).unwrap();
        let text = workdir.path().join("test_experiment/text_1.txt");
        std::fs::File::create_new(&text).unwrap();
        let readme = workdir.path().join("README.md");
        std::fs::File::create_new(&readme).unwrap();

        let mut rocrate = read_crate(&crate_path, 0).unwrap();
        let zip_paths = RoCrateZipPaths {
            absolute_path: crate_path,
            root_path: dir_path.clone(),
            zip_file_name: dir_path.join(PathBuf::from("test_experiment.zip")),
        };

        let zip_data = RoCrateZip {
            zip: ZipWriter::new(File::create(&zip_paths.zip_file_name).unwrap()),
            options: SimpleFileOptions::default()
                .compression_method(zip::CompressionMethod::Deflated),
        };

        assert!(zip_crate_external(&mut rocrate, zip_data, &zip_paths).is_ok());
    }

    #[test]
    fn test_write_crate_success() {
        let tempfile = create_tempcrate(&ro_crate_minimal);
        let rocrate = read_crate(
            &tempfile.path().join("ro-crate-metadata.json").to_path_buf(),
            0,
        )
        .unwrap();
        let file_name = tempfile.path().join("test_rocrate_output.json");

        // Call the function to write the crate to a file
        write_crate(&rocrate, file_name.to_string_lossy().to_string()).unwrap();

        // Check if the file is created
        assert!(Path::new(&file_name).exists());

        // Read the file content and verify if it matches the expected JSON
        let file_content = fs::read_to_string(file_name).expect("Failed to read file");
        let expected_json = serde_json::to_string_pretty(&rocrate).expect("Failed to serialize");
        println!("{}", file_content);
        assert_eq!(file_content.trim_end(), expected_json);
    }

    #[test]
    fn test_zip_crate_basic() {
        let path = create_tempcrate(&minimal_test_experiment_rocrate);
        let ro_path = path.path().join("ro-crate-metadata.json");
        let base = path.path().file_name().unwrap().to_string_lossy();
        let zip_path = format!("{}/{}.zip", path.path().to_string_lossy(), base);

        let zipped = zip_crate(&ro_path, false, 0, false, false);
        println!("{:?}", zipped);
        assert!(parse_zip(&zip_path, 0).is_ok());
    }

    #[test]
    fn test_zip_crate_external_full() {
        let path = create_tempcrate(&minimal_test_experiment_rocrate);
        let ro_path = path.path().join("ro-crate-metadata.json");
        let base = path.path().file_name().unwrap().to_string_lossy();
        let zip_path = format!("{}/{}.zip", path.path().to_string_lossy(), base);

        let zipped = zip_crate(&ro_path, true, 0, false, false);
        println!("{:?}", zipped);
        assert!(parse_zip(&zip_path, 0).is_ok());
    }

    #[test]
    fn test_zip_crate_external_full_unique() {
        let path = create_tempcrate(&minimal_test_experiment_rocrate);
        let ro_path = path.path().join("ro-crate-metadata.json");

        let zipped = zip_crate(&ro_path, true, 0, false, true);
        println!("{:?}", zipped);
        assert!(zipped.is_ok())
    }

    #[test]
    fn test_is_not_url() {
        let mut url_types: HashMap<&str, bool> = HashMap::new();

        // A highly expansive and overkill URL/file path fixture generated with
        // GPT
        // URLs (false)
        url_types.insert("http://example.com", false); // HTTP
        url_types.insert("https://example.com", false); // HTTPS
        url_types.insert("ftp://ftp.example.com", false); // FTP
        url_types.insert("sftp://example.com", false); // SFTP
        url_types.insert("ws://example.com/socket", false); // WS
        url_types.insert("wss://example.com/socket", false); // WSS
        url_types.insert("data:text/html,<html>Hello!</html>", false); // DATA
        url_types.insert("blob:https://example.com/uuid", false); // BLOB
        url_types.insert("mailto:someone@example.com?subject=Hello", false); // MAILTO
        url_types.insert("tel:+1234567890", false); // TEL
        url_types.insert("sms:+1234567890?body=Hello", false); // SMS
        url_types.insert("jdbc:mysql://localhost:3306/database", false); // JDBC
        url_types.insert("urn:uuid:123e4567-e89b-12d3-a456-426614174000", false); // URN
        url_types.insert("ldap://example.com:389/dc=example,dc=com", false); // LDAP
        url_types.insert("ssh://user@server.com", false); // SSH
        url_types.insert("rtsp://media.example.com/video", false); // RTSP
        url_types.insert("mms://stream.example.com", false); // MMS
        url_types.insert("magnet:?xt=urn:btih:hash1234", false); // MAGNET
        url_types.insert("geo:37.7749,-122.4194", false); // GEO
        url_types.insert(
            "bitcoin:1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa?amount=0.01",
            false,
        ); // BITCOIN
        url_types.insert("ipfs://bafybeic3cphk4a", false); // IPFS
        url_types.insert("irc://irc.libera.chat/#rust", false); // IRC
        url_types.insert("git://github.com/user/repo.git", false); // GIT
        url_types.insert("telnet://192.168.1.1", false); // TELNET
        url_types.insert("news:comp.lang.rust", false); // NEWS
        url_types.insert("about:blank", false); // ABOUT
        url_types.insert("chrome://settings/", false); // CHROME
        url_types.insert("javascript:alert('Hello')", false); // JAVASCRIPT
        url_types.insert("s3://bucket-name/object-key", false); // AWS S3 Object
        url_types.insert("gs://bucket-name/object-key", false); // Google Cloud Storage Object
        url_types.insert("azure://container-name/blob-name", false); // Azure Blob Storage
        url_types.insert("swift://container-name/object-name", false); // OpenStack Swift Object
        url_types.insert("wasabi://bucket-name/object-key", false); // Wasabi Object Storage
        url_types.insert("minio://bucket-name/object-key", false); // MinIO Object Storage
        url_types.insert("aliyun://bucket-name/object-key", false); // Alibaba Cloud OSS
        url_types.insert("digitalocean://bucket-name/object-key", false); // DigitalOcean Spaces
        url_types.insert("ibmcloud://bucket-name/object-key", false); // IBM Cloud Object Storage
        url_types.insert("backblaze://bucket-name/object-key", false); // Backblaze B2 Storage
        url_types.insert("rackspace://container-name/object-name", false); // Rackspace Cloud Files
        url_types.insert("oracle://bucket-name/object-key", false); // Oracle Cloud Object Storage

        // Fragment ref
        url_types.insert("#test", true); // Fragment ref
        url_types.insert(
            "main.nf#main/FAMOSAB_WRROCMETATEST:WRROCMETATEST:FASTP",
            true,
        );

        // File Paths (true)
        url_types.insert("file:///C:/Windows/System32/drivers/etc/hosts", true); // Windows File Path
        url_types.insert("file:///Users/user/Documents/notes.txt", true); // macOS File Path
        url_types.insert("/home/user/Documents/report.pdf", true); // Linux Absolute Path
        url_types.insert("C:\\Users\\User\\Downloads\\file.txt", true); // Windows Backslash Path
        url_types.insert("../relative/path/to/file.txt", true); // Relative Path
        url_types.insert("./current/directory/file.txt", true); // Current Directory Path
        url_types.insert("/var/log/syslog", true); // Unix Log File
        url_types.insert("~/.ssh/config", true); // Unix Home Directory
        url_types.insert("~/Documents/resume.docx", true); // Unix Home Directory Expanded
        url_types.insert("/mnt/data/project/files", true); // Mounted Drive Path
        url_types.insert("E:\\Music\\playlist.m3u", true); // External Drive on Windows
        url_types.insert("\\\\network\\share\\folder\\file.txt", true); // UNC Path on Windows
        url_types.insert("/etc/nginx/nginx.conf", true); // Configuration File Path
        url_types.insert("/opt/app/bin/start.sh", true); // Unix Application Path
        url_types.insert("/dev/null", true); // Unix Special Device Path
        url_types.insert("C:/Program Files/App/app.exe", true); // Windows Program Path
        url_types.insert("/usr/local/bin/script", true); // Unix Local Bin Path
        url_types.insert("D:/Projects/Code/main.rs", true); // Windows Drive Path

        for (key, value) in url_types {
            let test = is_not_url(key);
            println!("Func result: {}, testing: {}, {}", test, key, value);
            assert_eq!(test, value);
        }
    }
}
