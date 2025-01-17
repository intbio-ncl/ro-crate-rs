//! Module for writing RoCrate structures to file.
//!
//! Allows basic ro-crate-metadata.json file creation, as well as archiving
//! via zip.

use crate::ro_crate::read::read_crate;
use crate::ro_crate::rocrate::RoCrate;
use dirs;
use std::error::Error;
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
    mut zip_data: RoCrateZip,
) -> Result<(), ZipError> {
    // Attempt to serialize the RoCrate object to a pretty JSON string
    let json_ld = serde_json::to_string_pretty(&rocrate)
        .map_err(|e| ZipError::ZipOperationError(e.to_string()))?;

    // Start a new file in the zip archive with the given name and options
    zip_data
        .zip
        .start_file(name, zip_data.options)
        .map_err(|e| ZipError::ZipOperationError(e.to_string()))?;

    // Write the serialized JSON data to the file in the zip archive
    zip_data
        .zip
        .write_all(json_ld.as_bytes())
        .map_err(|e| ZipError::ZipOperationError(e.to_string()))?;

    zip_data
        .zip
        .finish()
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
    // After prepping create the initial zip file
    let zip_paths = construct_paths(crate_path).unwrap();

    let mut zip_data = build_zip(&zip_paths).unwrap();

    // Opens target crate ready for update
    let mut rocrate = read_crate(&zip_paths.absolute_path, validation_level).unwrap();

    let zipped_files = directory_walk(&zip_paths, &mut zip_data);
    println!("{:?}", zipped_files);

    // TODO: Known issue, this zip external logic needs to be executed before
    // you walk the directory, since this looks at the rocrate and determines
    if external {
        zip_data = zip_crate_external(&mut rocrate, zip_data, &zip_paths.absolute_path)?
    }

    let _ = write_crate_to_zip(&rocrate, "ro-crate-metadata.json".to_string(), zip_data);

    Ok(())
}

struct RoCrateZipPaths {
    absolute_path: PathBuf,
    root_path: PathBuf,
    zip_file_name: PathBuf,
}

fn construct_paths(crate_path: &Path) -> Result<RoCrateZipPaths, Box<dyn std::error::Error>> {
    // TODO: add multile options for walking/compression e.g follow symbolic links etc.
    let absolute_path = get_absolute_path(crate_path).unwrap();
    let root_path = absolute_path.parent().unwrap().to_path_buf();

    let zip_file_base_name = root_path
        .file_name()
        .ok_or(ZipError::FileNameNotFound)?
        .to_str()
        .ok_or(ZipError::FileNameConversionFailed)?;

    let zip_file_name = root_path.join(format!("{}.zip", zip_file_base_name));

    Ok(RoCrateZipPaths {
        absolute_path,
        root_path,
        zip_file_name,
    })
}

fn build_zip(path_information: &RoCrateZipPaths) -> Result<RoCrateZip, Box<dyn std::error::Error>> {
    let file = File::create(&path_information.zip_file_name).map_err(ZipError::IoError)?;
    let zip = ZipWriter::new(file);

    // Can change this to deflated for standard compression
    let options = FileOptions::default().compression_method(zip::CompressionMethod::Deflated);

    Ok(RoCrateZip { zip, options })
}

pub struct RoCrateZip {
    zip: ZipWriter<File>,
    options: FileOptions,
}

/// Sole focus must be on present data
/// As defined by the ro-crate-metadata file, it looks at the root and then
/// every file within it belongs to the crate. Whilst not everything is
/// described in the ro-crate-metadata itself as per spec, it absolutely should
/// get everything that is within the crate
fn directory_walk(
    zip_paths: &RoCrateZipPaths,
    zip_data: &mut RoCrateZip,
) -> Result<Vec<PathBuf>, Box<dyn std::error::Error>> {
    let mut data_vec: Vec<PathBuf> = Vec::new();

    for entry in WalkDir::new(&zip_paths.root_path)
        .min_depth(0)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().is_file())
    // Consider only files, not directories
    {
        let path = entry.path();
        println!("Entry path: {:?}", path);

        if path == zip_paths.zip_file_name {
            println!("Hit source file name: {:?}", path);
            continue;
        }

        if path == zip_paths.absolute_path {
            println!("Hit ro-metadata-file: {:?}", path);
            continue;
        }

        let relative_path = path
            .strip_prefix(&zip_paths.root_path)
            .map_err(ZipError::from)?;
        println!("Relative Path: {:?}", relative_path);

        let relative_path_str = relative_path
            .to_str()
            .ok_or(ZipError::FileNameConversionFailed)?;

        println!("Relative path string: {}", relative_path_str);

        let mut file = fs::File::open(path).map_err(ZipError::IoError)?;

        zip_data
            .zip
            .start_file(relative_path_str, zip_data.options)
            .map_err(|e| ZipError::ZipOperationError(e.to_string()))?;

        io::copy(&mut file, &mut zip_data.zip).map_err(ZipError::IoError)?;

        // Once copy the absolute path and relative path needs to be checked
        let abs_path = get_absolute_path(path).unwrap();
        println!("Abs path check {:?}", abs_path);
        if abs_path.is_file() {
            data_vec.push(abs_path);
        };

        // TODO: Doesn't work - I need to update the rocrate with the relative paths of all the
        // absolute paths,
        //update_zip_ids(rocrate, abs_path, relative_path_str);
    }
    Ok(data_vec)
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
    mut zip_data: RoCrateZip,
    crate_path: &Path,
) -> Result<RoCrateZip, ZipError> {
    // Get all IDs for the target crate
    // Assumed that all data entities are discrete objects, and that not file
    // has been referenced without being described
    let mut ids = rocrate.get_all_ids();
    println!("IDs prior to pop: {:?}", ids);

    // Pop all non-urls and remove root/ metadata descriptor
    ids.retain(|id| is_not_url(id));
    println!("IDs after pop: {:?}", ids);

    let noncontained = get_noncontained_paths(ids, crate_path);

    // if noncontained is not empty, means data entities are external
    // therefore we need to package them
    if !noncontained.is_empty() {
        for external in noncontained {
            // norels = path to file, then we use external path to get folder then add basename
            let file_name = external
                .file_name()
                .ok_or(ZipError::FileNameNotFound)?
                .to_str()
                .ok_or(ZipError::FileNameConversionFailed)?;
            let zip_entry_name = format!("external/{}", file_name);

            let mut file = fs::File::open(&external).map_err(ZipError::IoError)?;

            zip_data
                .zip
                .start_file(&zip_entry_name, zip_data.options)
                .map_err(|e| ZipError::ZipOperationError(e.to_string()))?;

            let copy_result = io::copy(&mut file, &mut zip_data.zip).map_err(ZipError::IoError);
            match copy_result {
                Ok(_) => {
                    update_zip_ids(rocrate, external, &zip_entry_name);
                }
                Err(e) => return Err(e),
            }
        }
    }

    Ok(zip_data)
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
    println!("ID String that is updating: {}", id_str);

    // Try updating based on a direct match
    rocrate.update_id_recursive(id_str, zip_id);

    // Handle Windows extended-length path prefixes (\\?\)
    if id_str.starts_with(r"\\?\") {
        let stripped_id = &id_str[4..];
        println!("ID to be stripped: {}, Strip ID: {}", id_str, stripped_id);

        // Attempt to update using the stripped path
        rocrate.update_id_recursive(stripped_id, zip_id);

        // Handle paths with '\\' by replacing them with a single '\'
        if id_str.contains("\\\\") {
            println!("Doubled windows path {}", id_str);
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
fn get_noncontained_paths(ids: Vec<&String>, crate_dir: &Path) -> Vec<PathBuf> {
    let mut nonrels: Vec<PathBuf> = Vec::new();

    // Get the absolute path of the crate directory
    let rocrate_path = get_absolute_path(crate_dir).unwrap();
    println!("Absolute path to crate directory: {:?}", rocrate_path);

    // Iterate over all the ids, check if the paths are relative to the crate.
    // EVERYTHING NEEDS TO BE WITHIN THE CRATE
    for id in ids.iter() {
        // Skip IDs that are fragment references (i.e., starting with '#')
        if id.starts_with('#') {
            continue;
        }
        println!("1 | ID: {}", id);

        // Resolve the absolute path of the current ID
        if let Some(path) = get_absolute_path(Path::new(id)) {
            println!("2 | Absolute Path: {:?}", path);
            // Check if the path exists
            if path.exists() {
                println!("3 | Path exists: {:?}", path);
                // Check if the path is outside the base crate directory
                if is_outside_base_folder(&rocrate_path, &path) {
                    println!("4 | Path exists outside: {:?}", &path);
                    nonrels.push(path);
                } else {
                    println!("5 | is inside of crate! {:?}", &path);
                }
            }
        } else {
            // Attempt to canonicalize the path
            // Attempt to canonicalize the path
            // Attempt to canonicalize the path
            let path = match Path::new(id).canonicalize() {
                Ok(resolved) => Ok(resolved),
                Err(e) if e.kind() == io::ErrorKind::NotFound => {
                    println!("Path not found, attempting to resolve tilde: {:?}", id);
                    Ok(resolve_tilde_path(id))
                }
                Err(e) => {
                    println!("Unexpected error while resolving path {:?}: {}", id, e);
                    Err(e) // Return the error for further handling
                }
            };

            let resolved_path = rocrate_path.join(path.unwrap()).canonicalize();

            println!("resolved path: {:?}", resolved_path);
            match resolved_path {
                Ok(abs_path) => {
                    if abs_path.exists() {
                        println!("6 | Rebuilt path and found ID: {:?}", id);
                        if is_outside_base_folder(&rocrate_path, &abs_path) {
                            println!("7 | Found outside of based folder: {:?}", &abs_path);
                            nonrels.push(abs_path)
                        } else {
                            println!("8 | Not outside of crate folder!! {:?}", &abs_path);
                        }
                    } else {
                        println!("8 | Failed to resolve ID: {:?}", id);
                    }
                }
                Err(e) => println!("{}", e),
            }
        }
    }

    nonrels
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
    !file_path.starts_with(base_folder)
}

#[cfg(test)]
mod write_crate_tests {
    use super::*;
    use crate::ro_crate::read::read_crate;
    use std::collections::HashMap;
    use std::env;
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
        println!("{}", file_content);
        assert_eq!(file_content.trim_end(), expected_json);

        // Clean up: Remove the created file after the test
        fs::remove_file(file_name).expect("Failed to remove test file");
    }

    #[test]
    fn test_zip_crate_basic() {
        let path = fixture_path("test_experiment/_ro-crate-metadata-minimal.json");

        let zipped = zip_crate(&path, false, 0);
        println!("{:?}", zipped);
    }

    #[test]
    fn test_zip_crate_external_full() {
        let path = fixture_path("test_experiment/_ro-crate-metadata-minimal.json");

        let zipped = zip_crate(&path, true, 0);
        println!("{:?}", zipped);
    }

    #[test]
    fn test_construct_paths() {
        let cwd = env::current_dir().unwrap();
        let path = fixture_path("test_experiment/_ro-crate-metadata-minimal.json");

        let paths = construct_paths(&path).unwrap();

        assert_eq!(paths.absolute_path, cwd.join(&path));
        assert_eq!(
            paths.root_path,
            cwd.join(PathBuf::from("tests/fixtures/test_experiment"))
        );
        assert_eq!(
            paths.zip_file_name,
            cwd.join(PathBuf::from(
                "tests/fixtures/test_experiment/test_experiment.zip"
            ))
        );
    }

    #[test]
    fn test_directory_walk() {
        let cwd = env::current_dir().unwrap();
        let path = fixture_path("test_experiment/_ro-crate-metadata-minimal.json");

        let zip_paths = RoCrateZipPaths {
            absolute_path: cwd.join(&path),
            root_path: cwd.join(PathBuf::from("tests/fixtures/test_experiment")),
            zip_file_name: cwd.join(PathBuf::from(
                "tests/fixtures/test_experiment/test_experiment.zip",
            )),
        };

        let mut zip_data = RoCrateZip {
            zip: ZipWriter::new(File::create(&zip_paths.zip_file_name).unwrap()),
            options: FileOptions::default().compression_method(zip::CompressionMethod::Deflated),
        };

        let directory_contents = directory_walk(&zip_paths, &mut zip_data).unwrap();

        let test_vec: Vec<PathBuf> = vec![
            PathBuf::from("/home/matt/dev/ial/ro-crate-rs/tests/fixtures/test_experiment/data.csv"),
            PathBuf::from(
                "/home/matt/dev/ial/ro-crate-rs/tests/fixtures/test_experiment/text_1.txt",
            ),
        ];

        assert_eq!(directory_contents, test_vec);
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

    #[test]
    fn test_get_noncontained_paths() {
        let mut path_types: HashMap<&str, bool> = HashMap::new();
        let cwd = env::current_dir().unwrap();
        let crate_path = cwd.join(PathBuf::from("tests/fixtures/test_experiment"));

        path_types.insert("../invalid.json", true); // Windows File Path
        path_types.insert("./data.csv", false); // macOS File Path
        path_types.insert("./text_1.txt", false); // Linux Absolute Path
        path_types.insert("text_1.txt", false); // Linux Absolute Path
        path_types.insert("/var/log/syslog", true); // Windows Backslash Path
        path_types.insert("~/.cargo/env", true); // Relative Path
        path_types.insert("#fragment", false); // Relative Path

        // abs path but not relative
        let abs_not = cwd
            .join(PathBuf::from("README.md"))
            .to_str()
            .unwrap()
            .to_string();
        path_types.insert(&abs_not, true);

        // abs path rel
        let abs_is = cwd
            .join(crate_path.join(PathBuf::from("data.csv")))
            .to_str()
            .unwrap()
            .to_string();
        path_types.insert(&abs_is, false);

        for (key, value) in path_types {
            let mut input_vec: Vec<&String> = Vec::new();
            let target = key.to_string();
            input_vec.push(&target);

            let test = get_noncontained_paths(input_vec.clone(), &crate_path);
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
    fn test_zip_crate_external() {
        let cwd = env::current_dir().unwrap();
        let path = fixture_path("test_experiment/_ro-crate-metadata-minimal.json");

        let mut rocrate = read_crate(&path, 0).unwrap();
        let zip_paths = RoCrateZipPaths {
            absolute_path: cwd.join(&path),
            root_path: cwd.join(PathBuf::from("tests/fixtures/test_experiment")),
            zip_file_name: cwd.join(PathBuf::from(
                "tests/fixtures/test_experiment/test_experiment.zip",
            )),
        };

        let zip_data = RoCrateZip {
            zip: ZipWriter::new(File::create(&zip_paths.zip_file_name).unwrap()),
            options: FileOptions::default().compression_method(zip::CompressionMethod::Deflated),
        };

        let zipped = zip_crate_external(&mut rocrate, zip_data, &zip_paths.absolute_path);
    }
}
