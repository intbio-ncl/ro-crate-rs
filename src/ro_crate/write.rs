//! Module for writing RoCrate structures to file.
//!
//! Allows basic ro-crate-metadata.json file creation, as well as archiving
//! via zip.

use crate::ro_crate::read::read_crate;
use crate::ro_crate::rocrate::RoCrate;
use std::fmt;
use std::fs::{self, File};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use url::Url;
use walkdir::WalkDir;
use zip::{write::FileOptions, ZipWriter};

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
pub fn write_crate(rocrate: &RoCrate, name: String) {
    match serde_json::to_string_pretty(&rocrate) {
        Ok(json_ld) => match File::create(name) {
            Ok(mut file) => {
                if writeln!(file, "{}", json_ld).is_err() {
                    eprintln!("Failed to write to the file.");
                }
            }
            Err(e) => eprintln!("Failed to create file: {}", e),
        },
        Err(e) => eprintln!("Serialization failed: {}", e),
    }
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
/// A `Result<(), ZipError>` indicating the success or failure of the operation.
fn write_crate_to_zip(
    rocrate: &RoCrate,
    name: String,
    zip: &mut ZipWriter<File>,
    options: FileOptions,
) -> Result<(), ZipError> {
    // Attempt to serialize the RoCrate object to a pretty JSON string
    let json_ld = serde_json::to_string_pretty(&rocrate)
        .map_err(|e| ZipError::ZipOperationError(e.to_string()))?;

    // Start a new file in the zip archive with the given name and options
    zip.start_file(name, options)
        .map_err(|e| ZipError::ZipOperationError(e.to_string()))?;

    // Write the serialized JSON data to the file in the zip archive
    zip.write_all(json_ld.as_bytes())
        .map_err(|e| ZipError::ZipOperationError(e.to_string()))?;

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
/// A `Result<(), ZipError>` reflecting the success or failure of the operation.
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
pub fn zip_crate(crate_path: &Path, external: bool, validation_level: i8) -> Result<(), ZipError> {
    // TODO: add multile options for walking/compression e.g follow symbolic links etc.
    let crate_abs = get_absolute_path(crate_path).unwrap();
    let root = crate_abs.parent().unwrap();

    let zip_file_base_name = root
        .file_name()
        .ok_or(ZipError::FileNameNotFound)?
        .to_str()
        .ok_or(ZipError::FileNameConversionFailed)?;

    let zip_file_name = root.join(format!("{}.zip", zip_file_base_name));

    let file = File::create(&zip_file_name).map_err(ZipError::IoError)?;
    let mut zip = ZipWriter::new(file);

    // Can change this to deflated for standard compression
    let options = FileOptions::default().compression_method(zip::CompressionMethod::Deflated);

    // Opens target crate ready for update
    let mut rocrate = read_crate(&crate_abs.to_path_buf(), validation_level).unwrap();

    for entry in WalkDir::new(root)
        .min_depth(0)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().is_file())
    // Consider only files, not directories
    {
        let path = entry.path();

        if path == zip_file_name {
            continue;
        }

        let relative_path = path.strip_prefix(root).map_err(ZipError::from)?;

        let relative_path_str = relative_path
            .to_str()
            .ok_or(ZipError::FileNameConversionFailed)?;

        let mut file = fs::File::open(path).map_err(ZipError::IoError)?;
        zip.start_file(relative_path_str, options)
            .map_err(|e| ZipError::ZipOperationError(e.to_string()))?;
        io::copy(&mut file, &mut zip).map_err(ZipError::IoError)?;

        // Once copy the absolute path and relative path needs to be checked
        let abs_path = get_absolute_path(path).unwrap();

        // I need to update the rocrate with the relative paths of all the
        update_zip_ids(&mut rocrate, abs_path, relative_path_str);
        // absolute paths,
    }

    // TODO: Known issue, this zip external logic needs to be executed before
    // you walk the directory, since this looks at the rocrate and determines
    if external {
        zip = zip_crate_external(&mut rocrate, &crate_abs, zip, options)?
    }
    let _ = write_crate_to_zip(
        &rocrate,
        "ro-crate-metadata.json".to_string(),
        &mut zip,
        options,
    );

    zip.finish()
        .map_err(|e| ZipError::ZipOperationError(e.to_string()))?;

    Ok(())
}

#[derive(Debug)]
pub enum ZipError {
    EmptyDirectoryVector,
    FileNameNotFound,
    FileNameConversionFailed,
    PathError(std::path::StripPrefixError),
    ZipOperationError(String),
    IoError(io::Error),
}

impl fmt::Display for ZipError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            ZipError::EmptyDirectoryVector => write!(f, "Directory vector is empty"),
            ZipError::FileNameNotFound => write!(f, "File name not found"),
            ZipError::FileNameConversionFailed => write!(f, "Failed to convert file name"),
            ZipError::ZipOperationError(ref msg) => write!(f, "Zip operation Error: {}", msg),
            ZipError::PathError(ref err) => write!(f, "Path error: {}", err),
            ZipError::IoError(ref err) => write!(f, "IO error: {}", err),
        }
    }
}

/// Implements the standard Error trait for ZipError.
///
/// This allows `ZipError` to integrate with Rust's error handling ecosystem, enabling it to be
/// returned and handled in contexts where a standard error type is expected.
impl std::error::Error for ZipError {}

/// Converts an `io::Error` into a `ZipError`.
///
/// This is particularly useful when dealing with file I/O operations that may fail,
/// allowing these errors to be seamlessly converted and handled as `ZipError`s.
impl From<io::Error> for ZipError {
    fn from(err: io::Error) -> ZipError {
        ZipError::IoError(err)
    }
}

/// Converts a `std::path::StripPrefixError` into a `ZipError`.
///
/// This conversion is necessary when manipulating file paths, especially when needing
/// to work with relative paths and encountering errors stripping prefixes from them.
impl From<std::path::StripPrefixError> for ZipError {
    fn from(err: std::path::StripPrefixError) -> ZipError {
        ZipError::PathError(err)
    }
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
/// * `options` - `FileOptions` determining how files are added to the archive (e.g., compression level).
///
/// # Returns
/// Returns a `Result` containing the updated `ZipWriter<File>` on success, or a `ZipError` on failure,
/// encapsulating any errors that occurred during the operation.
pub fn zip_crate_external(
    rocrate: &mut RoCrate,
    crate_path: &Path,
    mut zip: ZipWriter<File>,
    options: FileOptions,
) -> Result<ZipWriter<File>, ZipError> {
    // Get all IDs for the target crate
    let mut ids = rocrate.get_all_ids();

    // Pop all non-urls
    ids.retain(|id| is_not_url(id));
    let nonrels = get_nonrelative_paths(&ids, crate_path);

    // if nonrels is not empty, means data entities are external
    // therefore we need to package them
    if !nonrels.is_empty() {
        for external in nonrels {
            // norels = path to file, then we use external path to get folder then add basename
            let file_name = external
                .file_name()
                .ok_or(ZipError::FileNameNotFound)?
                .to_str()
                .ok_or(ZipError::FileNameConversionFailed)?;
            let zip_entry_name = format!("external/{}", file_name);

            let mut file = fs::File::open(&external).map_err(ZipError::IoError)?;

            zip.start_file(&zip_entry_name, options)
                .map_err(|e| ZipError::ZipOperationError(e.to_string()))?;

            let copy_result = io::copy(&mut file, &mut zip).map_err(ZipError::IoError);
            match copy_result {
                Ok(_) => {
                    update_zip_ids(rocrate, external, &zip_entry_name);
                }
                Err(e) => return Err(e),
            }
        }
    }

    Ok(zip)
}
/// Updates the identifiers of entities within an RO-Crate to match their new paths in a zip archive.
///
/// This function is essential when packaging an RO-Crate and its associated files into a zip archive.
/// It ensures that all references to data entities within the crate reflect their new locations within
/// the archive. The function handles various path formats, including extended-length paths on Windows.
///
/// # Arguments
/// * `rocrate` - A mutable reference to the RO-Crate being updated.
/// * `id` - The original filesystem path of the entity.
/// * `zip_id` - The new identifier for the entity within the zip archive.
///
/// # Note
/// The function includes specific checks for path anomalies, such as Windows extended-length path prefixes.
/// It currently focuses only on Windows extended-length path prefixes and needs to be updated in
/// line with new ids.
fn update_zip_ids(rocrate: &mut RoCrate, id: PathBuf, zip_id: &str) {
    let id_str = id.to_str().unwrap_or_default();

    // Try updating based on a direct match
    rocrate.update_id_recursive(id_str, zip_id);

    // Handle Windows extended-length path prefixes (\\?\)
    if id_str.starts_with(r"\\?\") {
        let stripped_id = &id_str[4..];

        // Attempt to update using the stripped path
        rocrate.update_id_recursive(stripped_id, zip_id);

        // Handle paths with '\\' by replacing them with a single '\'
        if id_str.contains("\\\\") {
            let normalized_id = stripped_id.replace("\\\\", "\\");
            rocrate.update_id_recursive(&normalized_id, zip_id);
        }
    }
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
/// # Returns
/// A vector of `PathBuf` objects representing files that are outside the crate's base directory.
fn get_nonrelative_paths(ids: &Vec<&String>, crate_dir: &Path) -> Vec<PathBuf> {
    let mut nonrels: Vec<PathBuf> = Vec::new();

    // Get the absolute path of the crate directory
    let rocrate_path = get_absolute_path(crate_dir).unwrap();

    // Iterate over all the ids, check if the paths are relative to the crate.
    for id in ids.iter() {
        // Skip IDs that are fragment references (i.e., starting with '#')
        if id.starts_with('#') {
            continue;
        }

        // Resolve the absolute path of the current ID
        if let Some(path) = get_absolute_path(Path::new(id)) {
            // Check if the path exists
            if path.exists() {
                // Check if the path is outside the base crate directory
                if is_outside_base_folder(&rocrate_path, &path) {
                    nonrels.push(path);
                }
            }
        }
    }

    nonrels
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
fn get_absolute_path(relative_path: &Path) -> Option<PathBuf> {
    match fs::canonicalize(relative_path) {
        Ok(path) => Some(path),
        Err(_e) => None,
    }
}
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
fn is_not_url(path: &str) -> bool {
    // Check if the path is likely a Windows extended-length path
    let is_extended_windows_path = path.starts_with(r"\\?\");

    // Check if the path is likely a normal file path
    let is_normal_file_path = path.starts_with(r"\\") // UNC path
        || path.chars().next().map(|c| c.is_alphabetic() && path.chars().nth(1) == Some(':')).unwrap_or(false) // Drive letter, e.g., C:\
        || path.starts_with('/') // Unix-style path
        || path.starts_with('.'); // Relative path

    // If it looks like a file path, return true early
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
    !file_path.starts_with(base_folder)
}

#[cfg(test)]
mod write_crate_tests {
    use super::*;
    use crate::ro_crate::read::read_crate;
    use std::fs;
    use std::path::Path;
    use std::path::PathBuf;

    fn fixture_path(relative_path: &str) -> PathBuf {
        Path::new("tests/fixtures").join(relative_path)
    }

    #[test]
    fn test_write_crate_success() {
        let path = fixture_path("_ro-crate-metadata-minimal.json");
        let rocrate = read_crate(&path, 0).unwrap();
        let file_name = "test_rocrate_output.json";

        // Call the function to write the crate to a file
        write_crate(&rocrate, file_name.to_string());

        // Check if the file is created
        assert!(Path::new(file_name).exists());

        // Read the file content and verify if it matches the expected JSON
        let file_content = fs::read_to_string(file_name).expect("Failed to read file");
        let expected_json = serde_json::to_string_pretty(&rocrate).expect("Failed to serialize");
        assert_eq!(file_content.trim_end(), expected_json);

        // Clean up: Remove the created file after the test
        fs::remove_file(file_name).expect("Failed to remove test file");
    }
}
