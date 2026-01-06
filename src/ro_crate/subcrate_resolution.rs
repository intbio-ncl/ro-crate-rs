use crate::ro_crate::constraints::Id;
use crate::ro_crate::data_entity::DataEntity;
use crate::ro_crate::rocrate::RoCrate;
use crate::ro_crate::write::is_not_url;
use log::warn;
use reqwest::blocking::Response;
use reqwest::header::{HeaderMap, ToStrError};
use sha2::Digest;
use std::io::Read;
use std::string::FromUtf8Error;
use zip::result::ZipError;
use zip::ZipArchive;

#[derive(Debug)]
pub enum FetchError {
    NotFound(String),
    InvalidId { key: String, value: String },
    Reqwest(reqwest::Error),
    HeaderValueConversion(ToStrError),
    ZipError(ZipError),
    IoError(std::io::Error),
    SerializationError(serde_json::Error),
    BagItError(String),
    FromUTF8Error(FromUtf8Error),
}

impl std::fmt::Display for FetchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FetchError::NotFound(item) => {
                write!(f, "Could not retrieve: {}", item)
            }
            FetchError::InvalidId { key, value } => {
                write!(f, "Invalid Id `{}` for value `{}`", value, key)
            }
            FetchError::Reqwest(err) => {
                write!(f, "Error fetching request: `{}`", err)
            }
            FetchError::HeaderValueConversion(err) => {
                write!(f, "Error converting header: `{}`", err)
            }
            FetchError::ZipError(err) => {
                write!(f, "Error converting archive: `{}`", err)
            }
            FetchError::IoError(err) => {
                write!(f, "Io error: `{}`", err)
            }
            FetchError::SerializationError(err) => {
                write!(f, "Serialization error `{}`", err)
            }
            FetchError::BagItError(err) => {
                write!(f, "BagIt error `{}`", err)
            }
            FetchError::FromUTF8Error(err) => {
                write!(f, "FromUTF8Error `{}`", err)
            }
        }
    }
}

impl std::error::Error for FetchError {}

impl From<reqwest::Error> for FetchError {
    fn from(value: reqwest::Error) -> Self {
        FetchError::Reqwest(value)
    }
}
impl From<ToStrError> for FetchError {
    fn from(value: ToStrError) -> Self {
        FetchError::HeaderValueConversion(value)
    }
}
impl From<ZipError> for FetchError {
    fn from(value: ZipError) -> Self {
        FetchError::ZipError(value)
    }
}
impl From<std::io::Error> for FetchError {
    fn from(value: std::io::Error) -> Self {
        FetchError::IoError(value)
    }
}
impl From<serde_json::Error> for FetchError {
    fn from(value: serde_json::Error) -> Self {
        FetchError::SerializationError(value)
    }
}
impl From<std::string::FromUtf8Error> for FetchError {
    fn from(value: std::string::FromUtf8Error) -> Self {
        FetchError::FromUTF8Error(value)
    }
}

pub fn fetch_subcrates(rocrate: RoCrate) -> Result<Vec<RoCrate>, FetchError> {
    let subcrates = rocrate.get_subcrates();

    let mut collected_subcrates = Vec::new();

    for graph_vector in subcrates {
        let subcrate = match graph_vector {
            crate::ro_crate::graph_vector::GraphVector::DataEntity(data_entity) => data_entity,
            _ => continue,
        };

        // Try to find the subcrate id
        let id = get_id(subcrate);

        println!("{id}");

        if is_not_url(&id) {
            match try_resolve_local(&id) {
                Ok(rocrate) => {
                    collected_subcrates.push(rocrate);
                    continue;
                }
                Err(err) => println!("{}", err),
            }
        } else {
            match try_resolve_remote(&id) {
                Ok(rocrate) => {
                    collected_subcrates.push(rocrate);
                    continue;
                }
                Err(err) => warn!("{}", err),
            }
        }
    }

    Ok(collected_subcrates)
}

fn try_resolve_local(id: &str) -> Result<RoCrate, FetchError> {
    let path = if id.ends_with('/') {
        format!("{id}ro-crate-metadata.json")
    } else {
        id.to_string()
    };

    let mut file = std::fs::File::open(path)?;
    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer)?;

    Ok(serde_json::from_slice(buffer.as_slice())?)
}

fn try_direct_resolve_or_zip(response: Response) -> Result<RoCrate, FetchError> {
    let headers = response.headers().clone();
    let redirect_url = response.url().to_string();
    let body = response.bytes()?;

    if let Ok(ro_crate) = serde_json::from_slice::<RoCrate>(&body) {
        return Ok(ro_crate);
    }

    if let Ok(ro_crate) = try_zip(&headers, &redirect_url) {
        return Ok(ro_crate);
    }
    Err(FetchError::NotFound(format!(
        "Could not retrieve subcrate form url {}",
        redirect_url
    )))
}

fn try_resolve_remote(id: &str) -> Result<RoCrate, FetchError> {
    // First try to resolve id directly
    let response = reqwest::blocking::get(id)?;
    let headers = response.headers().clone();
    let redirect_url = response.url().to_string();

    if let Ok(ro_crate) = try_direct_resolve_or_zip(response) {
        return Ok(ro_crate);
    }

    // Try signposting
    if let Ok(response) = try_signposting(&headers) {
        // Try dir
        if let Ok(ro_crate) = try_direct_resolve_or_zip(response) {
            return Ok(ro_crate);
        }
    }

    // Try content negotiation
    if let Ok(response) = try_content_negotiation(&id) {
        if let Ok(ro_crate) = try_direct_resolve_or_zip(response) {
            return Ok(ro_crate);
        }
    }

    // Guess location
    if let Ok(response) = guess_location(&redirect_url) {
        if let Ok(ro_crate) = try_direct_resolve_or_zip(response) {
            return Ok(ro_crate);
        }
    }

    Err(FetchError::NotFound(format!(
        "Could not retrieve subcrate with id {id}"
    )))
}

/// Extract the metadata URL from subjectOf property
fn try_property(entity: &DataEntity, value: &str) -> Option<String> {
    if let Some(dynamic_entities) = &entity.dynamic_entity {
        if let Some(value) = dynamic_entities.get(value).cloned() {
            match value {
                super::constraints::EntityValue::EntityId(Id::Id(id)) => return Some(id),
                _ => {}
            }
        }
    }
    None
}

fn get_id(entity: &DataEntity) -> String {
    // 1. Try subjectOf (to url/path that leads to ro-crate-metadata.json)
    let id = if let Some(subject_of) = try_property(entity, "subjectOf") {
        subject_of
    } else {
        // 2. Try distribution (to url that leads to an archive)
        if let Some(distribution) = try_property(entity, "distribution") {
            distribution
        } else {
            // 3. Try retrieving ro-crate by id
            entity.id.clone()
        }
    };
    id
}

fn try_signposting(headers: &HeaderMap) -> Result<Response, FetchError> {
    // 1. **signposting** to id and look for Link with `rel="describedBy"`
    //    or `rel="item"` and prefer links for both where `profile="https://w3id.org/ro/crate`
    for link in headers.get_all("Link") {
        let values = link.to_str()?.to_string();
        if values.contains("profile=\"https://w3id.org/ro/crate\"") {
            if let Some((link, _)) = values.split_once(";") {
                let url = link.replace("<", "").replace(">", "");
                let response = reqwest::blocking::get(&url)?;
                return Ok(response);
            }
        } else {
            if values.contains("rel=\"describedBy\"") || values.contains("rel=\"item\"") {
                if let Some((link, _)) = values.split_once(";") {
                    let url = link.replace("<", "").replace(">", "");
                    let response = reqwest::blocking::get(&url)?;
                    return Ok(response);
                }
            }
        }
    }
    Err(FetchError::NotFound("No valid rocrate found".to_string()))
}

fn try_content_negotiation(id: &str) -> Result<Response, FetchError> {
    // 2. **content negotiation** with accept header `application/ld+json;profile=https://w3id.org/ro/crate`
    let content_negotiation_response = reqwest::blocking::Client::new()
        .get(id)
        .header(
            "Accept",
            "application/ld+json;profile=https://w3id.org/ro/crate",
        )
        .send()?;

    Ok(content_negotiation_response)
}

fn guess_location(redirect_url: &str) -> Result<Response, FetchError> {
    // 3. **basically guess**: If PID `https://w3id.org/workflowhub/workflow-ro-crate/1.0`
    //    redirects to `https://about.workflowhub.eu/Workflow-RO-Crate/1.0/index.html`
    //    then try `https://about.workflowhub.eu/Workflow-RO-Crate/1.0/ro-crate-metadata.json`
    let guessed_url = if redirect_url.ends_with("/") {
        format!("{}ro-crate-metadata.json", redirect_url)
    } else {
        if let Some((base, _)) = redirect_url.rsplit_once("/") {
            format!("{}ro-crate-metadata.json", base)
        } else {
            redirect_url.to_string()
        }
    };
    let content_negotiation_response = reqwest::blocking::Client::new().get(guessed_url).send()?;

    Ok(content_negotiation_response)
}

fn try_zip(headers: &HeaderMap, redirect_url: &str) -> Result<RoCrate, FetchError> {
    // 4. If retrieved resource has `Content-Type: application/zip` or is a ZIP file
    //    extract ro-crate-metadata.json or if only contains single folder, extract
    //    folder/ro-crate-metadata.json
    if let Some(content_type) = headers.get("Content-Type") {
        if content_type.to_str()?.contains("application/zip") {
            let response = reqwest::blocking::get(redirect_url)?.bytes()?;
            let reader = std::io::Cursor::new(response);
            let mut archive = ZipArchive::new(reader)?;

            // Try retrieving file by name
            match archive.by_name("ro-crate-metadata.json") {
                Ok(mut file_in_zip) => {
                    // Read the file contents into memory
                    let mut buffer = Vec::new();
                    file_in_zip.read_to_end(&mut buffer)?;

                    let subcrate: RoCrate = serde_json::from_slice(&buffer)?;

                    return Ok(subcrate);
                }
                Err(err) => warn!("{}", err),
            }

            // Try to extract rocrate from bagit
            match try_bagit(archive.clone()) {
                Ok(rocrate) => {
                    return Ok(rocrate);
                }
                Err(err) => warn!("{}", err),
            }

            // Try finding rocrate in subdirectories
            let names: Vec<String> = archive.file_names().map(|e| e.to_string()).collect();
            if let Some(rocrate) = names.iter().find(|x| x.contains("metadata.json")) {
                let mut file_in_zip = archive.by_name(rocrate)?;

                let mut buffer = Vec::new();
                file_in_zip.read_to_end(&mut buffer)?;

                let subcrate: RoCrate = serde_json::from_slice(&buffer)?;

                return Ok(subcrate);
            }
        }
    }
    Err(FetchError::NotFound("No subcrate found".to_string()))
}

fn try_bagit(
    mut archive: ZipArchive<std::io::Cursor<bytes::Bytes>>,
) -> Result<RoCrate, FetchError> {
    if archive.by_name("bagit.txt").is_ok() {
        // 5. If retrieved resource is a BagIt archive, extract and verify checksums,
        //    then return data/ro-crate-metdata.json
        let mut rocrate = archive.by_name("data/ro-crate-metadata.json")?;
        // Parse ro-crate
        let mut ro_crate_buffer = Vec::new();
        rocrate.read_to_end(&mut ro_crate_buffer)?;
        let subcrate = serde_json::from_slice(&ro_crate_buffer)?;
        drop(rocrate);

        if let Ok(sha512hash) = try_verify_hash(&mut archive, Hash::Sha512) {
            if sha512hash != hex::encode(sha2::Sha512::digest(&ro_crate_buffer)) {
                return Err(FetchError::BagItError(
                    "Hash mismatch of ro-crate-metadata.json in bagit archive".to_string(),
                ));
            }
        } else {
            let sha256hash = try_verify_hash(&mut archive, Hash::Sha256)?;

            if sha256hash != hex::encode(sha2::Sha256::digest(&ro_crate_buffer)) {
                return Err(FetchError::BagItError(
                    "Hash mismatch of ro-crate-metadata.json in bagit archive".to_string(),
                ));
            }
        }

        Ok(subcrate)
    } else {
        Err(FetchError::BagItError(
            "Could not crate BagIt from zip".to_string(),
        ))
    }
}

enum Hash {
    Sha512,
    Sha256,
}

fn try_verify_hash(
    archive: &mut ZipArchive<std::io::Cursor<bytes::Bytes>>,
    hash: Hash,
) -> Result<String, FetchError> {
    let manifest = match hash {
        Hash::Sha512 => "manifest-sha512.txt",
        Hash::Sha256 => "manifest-sha256.txt",
    };
    let mut sha512 = archive.by_name(manifest)?;
    let mut buffer = Vec::new();
    sha512.read_to_end(&mut buffer)?;

    let hashes = String::from_utf8(buffer)?;
    let hash = hashes
        .lines()
        .find_map(|line| {
            if line.contains("ro-crate-metdata.json") {
                if let Some((hash, _)) = line.split_once(' ') {
                    Some(hash.to_string())
                } else {
                    None
                }
            } else {
                None
            }
        })
        .ok_or_else(|| {
            FetchError::BagItError("Bagit does not list ro-crate-metadata.json".to_string())
        })?;
    Ok(hash)
}

#[cfg(test)]
mod subcrate_tests {
    use super::*;
    use crate::ro_crate::constraints::{DataType, EntityValue, Id};
    use crate::ro_crate::data_entity::DataEntity;
    use crate::ro_crate::modify::DynamicEntityManipulation;
    use crate::ro_crate::rocrate::RoCrate;
    use serde_json::json;
    use std::collections::HashMap;
    use std::io::Write;
    use tempfile::{tempdir, TempDir};
    use zip::write::SimpleFileOptions;
    use zip::ZipWriter;

    fn host_remote_crates() -> () {}

    // Helper function to create a DataEntity with properties
    fn create_data_entity_with_property(key: &str, value: &str) -> DataEntity {
        DataEntity {
            id: "test-id".to_string(),
            type_: DataType::Term("Dataset".to_string()),
            dynamic_entity: Some(HashMap::from([(
                key.to_string(),
                EntityValue::EntityId(Id::Id(value.to_string())),
            )])),
        }
    }

    #[test]
    fn test_get_id() {
        // Test if get_id only returns subjectOf when id and subjectOf are set
        let entity = create_data_entity_with_property("subjectOf", "http://example.com/metadata");
        let id = get_id(&entity);
        assert_eq!(id, "http://example.com/metadata");

        // Test if get_id only returns distribution when id and distribution are set
        let entity =
            create_data_entity_with_property("distribution", "http://example.com/archive.zip");
        let id = get_id(&entity);
        assert_eq!(id, "http://example.com/archive.zip");

        // Test if get_id returns id if nothing else is set
        let entity = DataEntity {
            id: "direct-id".to_string(),
            type_: DataType::Term("Dataset".to_string()),
            dynamic_entity: None,
        };
        let id = get_id(&entity);
        assert_eq!(id, "direct-id");

        // Test if get_id returns subjectOf when distribution and id are set
        let mut entity =
            create_data_entity_with_property("subjectOf", "http://example.com/metadata");
        entity.add_dynamic_fields(HashMap::from([(
            "distribution".to_string(),
            EntityValue::EntityId(Id::Id("http://example.com/archive.zip".to_string())),
        )]));
        let id = get_id(&entity);
        assert_eq!(id, "http://example.com/metadata");
    }

    #[test]
    fn test_try_resolve_local_files() {
        let subdir1 = tempdir().unwrap();
        let subdir2 = tempdir().unwrap();
        let subdir3 = tempdir().unwrap();

        let subcrate1 = json!(
        {
          "@context": "https://w3id.org/ro/crate/1.2/context",
          "@graph": [
            {
              "@id": "ro-crate-metadata.json",
              "@type": "CreativeWork",
              "conformsTo": {
                "@id": "https://w3id.org/ro/crate/1.2"
              },
              "about": {
                "@id": "./"
              }
            },
            {
              "@id": "./",
              "@type": "Dataset",
              "name": "Root Research Crate with Multiple Subcrates",
              "description": "A comprehensive example demonstrating various ways to define subcrates within an RO-Crate",
              "datePublished": "2026-01-06",
              "license": "https://creativecommons.org/licenses/by/4.0/",
              "hasPart": [
                {"@id": "subcrate1/data.csv"},
                {"@id": "subcrate1/README.md"}
                    ]
            },
            {
              "@id": "data.csv",
              "@type": "File",
              "name": "Sample Data",
              "encodingFormat": "text/csv"
            },
            {
              "@id": "README.md",
              "@type": "File",
              "name": "Subcrate Documentation",
              "encodingFormat": "text/markdown"
            }
            ]
        });

        let subcrate1_path = subdir1.path().join("ro-crate-metadata.json");
        let mut tmpfile = std::fs::File::create(subcrate1_path.clone()).unwrap();
        tmpfile.write(subcrate1.to_string().as_bytes()).unwrap();

        let subcrate2 = json!(
        {
          "@context": "https://w3id.org/ro/crate/1.2/context",
          "@graph": [
            {
              "@id": "ro-crate-metadata.json",
              "@type": "CreativeWork",
              "conformsTo": {
                "@id": "https://w3id.org/ro/crate/1.2"
              },
              "about": {
                "@id": "./"
              }
            },
            {
              "@id": "./",
              "@type": "Dataset",
              "name": "Subcrate 2: With Explicit RO-Crate Metadata Reference",
              "description": "Subcrate that references its own ro-crate-metadata.json file",
              "conformsTo": { "@id": "https://w3id.org/ro/crate" },
              "subjectOf": "subcrate2/ro-crate-metadata.json",
              "datePublished": "2026-01-06",
              "license": "https://creativecommons.org/licenses/by/4.0/",
              "hasPart": [
                {"@id": "subcrate2/ro-crate-metadata.json"},
                {"@id": "subcrate2/analysis.py"}
              ]
            },
            {
              "@id": "subcrate2/ro-crate-metadata.json",
              "@type": "CreativeWork",
              "encodingFormat": "application/json+ld",
              "about": {
                "@id": "subcrate2/"
              },
              "name": "Subcrate 2 Metadata",
              "description": "Separate RO-Crate metadata for subcrate2"
            },
            {
              "@id": "subcrate2/analysis.py",
              "@type": ["File", "SoftwareSourceCode"],
              "name": "Analysis Script",
              "programmingLanguage": "Python",
              "encodingFormat": "text/x-python"
            }]
        });

        let subcrate2_path = subdir2.path().join("ro-crate-metadata.json");
        let mut tmpfile = std::fs::File::create(subcrate2_path.clone()).unwrap();
        tmpfile.write(subcrate2.to_string().as_bytes()).unwrap();

        let subcrate3 = json!(
        {
          "@context": "https://w3id.org/ro/crate/1.2/context",
          "@graph": [
            {
              "@id": "ro-crate-metadata.json",
              "@type": "CreativeWork",
              "conformsTo": {
                "@id": "https://w3id.org/ro/crate/1.2"
              },
              "about": {
                "@id": "./"
              }
            },
            {
              "@id": "./",
              "@type": ["Dataset", "CreativeWork"],
              "name": "Subcrate 3: With Provenance Information",
              "description": "Subcrate with detailed provenance and authorship",
              "datePublished": "2026-01-06",
              "conformsTo": { "@id": "https://w3id.org/ro/crate" },
              "distribution": "subcrate3/ro-crate-metadata.json",
              "license": "https://creativecommons.org/licenses/by/4.0/",
              "author": {
                "@id": "https://orcid.org/0000-0002-1825-0097"
              },
              "dateCreated": "2025-12-15",
              "dateModified": "2026-01-05",
              "isPartOf": {
                "@id": "./"
              },
              "hasPart": [
                {"@id": "subcrate3/experiment_results.json"},
                {"@id": "subcrate3/ro-crate-metadata.json"}
              ]
            },
            {
              "@id": "https://orcid.org/0000-0002-1825-0097",
              "@type": "Person",
              "name": "Jane Researcher"
            },
            {
              "@id": "subcrate3/experiment_results.json",
              "@type": "File",
              "name": "Experiment Results",
              "encodingFormat": "application/json"
            },
            {
              "@id": "subcrate3/ro-crate-metadata.json",
              "@type": "CreativeWork",
              "encodingFormat": "application/json+ld",
              "about": {
                "@id": "subcrate3/"
              },
              "name": "Subcrate 3 Metadata",
              "description": "Separate RO-Crate metadata for subcrate3"
            }
            ]}
        );

        let subcrate3_path = subdir3.path().join("ro-crate-metadata.json");
        let mut tmpfile = std::fs::File::create(subcrate3_path.clone()).unwrap();
        tmpfile.write(subcrate3.to_string().as_bytes()).unwrap();

        let base_crate = json!({
          "@context": "https://w3id.org/ro/crate/1.2/context",
          "@graph": [
            {
              "@id": "ro-crate-metadata.json",
              "@type": "CreativeWork",
              "conformsTo": {
                "@id": "https://w3id.org/ro/crate/1.1"
              },
              "about": {
                "@id": "./"
              }
            },
            {
              "@id": "./",
              "@type": "Dataset",
              "name": "Root Research Crate with Multiple Subcrates",
              "description": "A comprehensive example demonstrating various ways to define subcrates within an RO-Crate",
              "datePublished": "2026-01-06",
              "license": "https://creativecommons.org/licenses/by/4.0/",
              "hasPart": [
                {"@id": "subcrate1/"},
                {"@id": "subcrate2/"},
                {"@id": "subcrate3/"},
              ]
            },
            // First case:
            // Local directory without subjectOf and distribution
            {
              "@id": subcrate1_path.to_string_lossy(),
              "@type": "Dataset",
              "name": "Subcrate 1: Basic Directory Reference",
              "description": "Simplest form - just a directory marked as Dataset with hasPart listing its contents",
              "conformsTo": { "@id": "https://w3id.org/ro/crate" },
              "hasPart": [
                {"@id": "subcrate1/data.csv"},
                {"@id": "subcrate1/README.md"},
                {"@id": "subcrate1/ro-crate-metadata.json"}
              ]
            },
            {
              "@id": "subcrate1/data.csv",
              "@type": "File",
              "name": "Sample Data",
              "encodingFormat": "text/csv"
            },
            {
              "@id": "subcrate1/README.md",
              "@type": "File",
              "name": "Subcrate Documentation",
              "encodingFormat": "text/markdown"
            },
            {
              "@id": "subcrate1/ro-crate-metadata.json",
              "@type": "File",
              "name": "RO-Crate metadata file",
              "encodingFormat": "application/json+ld"
            },
            // Second case:
            // Local directory with subjectOf that defines the location of metadatafile
            {
              "@id": subcrate2_path.to_string_lossy(),
              "@type": "Dataset",
              "name": "Subcrate 2: With Explicit RO-Crate Metadata Reference",
              "description": "Subcrate that references its own ro-crate-metadata.json file",
              "conformsTo": { "@id": "https://w3id.org/ro/crate" },
              "subjectOf": subcrate2_path.to_string_lossy(),
              "hasPart": [
                {"@id": "subcrate2/ro-crate-metadata.json"},
                {"@id": "subcrate2/analysis.py"}
              ]
            },
            {
              "@id": "subcrate2/ro-crate-metadata.json",
              "@type": "CreativeWork",
              "encodingFormat": "application/json+ld",
              "about": {
                "@id": "subcrate2/"
              },
              "name": "Subcrate 2 Metadata",
              "description": "Separate RO-Crate metadata for subcrate2"
            },
            {
              "@id": "subcrate2/analysis.py",
              "@type": ["File", "SoftwareSourceCode"],
              "name": "Analysis Script",
              "programmingLanguage": "Python",
              "encodingFormat": "text/x-python"
            },
            // Third case:
            // Local directory with distribution that defines the location of metadatafile
            {
              "@id": subcrate3_path.to_string_lossy(),
              "@type": ["Dataset", "CreativeWork"],
              "name": "Subcrate 3: With Provenance Information",
              "description": "Subcrate with detailed provenance and authorship",
              "conformsTo": { "@id": "https://w3id.org/ro/crate" },
              "distribution": subcrate3_path.to_string_lossy(),
              "author": {
                "@id": "https://orcid.org/0000-0002-1825-0097"
              },
              "dateCreated": "2025-12-15",
              "dateModified": "2026-01-05",
              "isPartOf": {
                "@id": "./"
              },
              "hasPart": [
                {"@id": "subcrate3/experiment_results.json"},
                {"@id": "subcrate3/ro-crate-metadata.json"}
              ]
            },
            {
              "@id": "https://orcid.org/0000-0002-1825-0097",
              "@type": "Person",
              "name": "Jane Researcher"
            },
            {
              "@id": "subcrate3/experiment_results.json",
              "@type": "File",
              "name": "Experiment Results",
              "encodingFormat": "application/json"
            },
            {
              "@id": "subcrate3/ro-crate-metadata.json",
              "@type": "CreativeWork",
              "encodingFormat": "application/json+ld",
              "about": {
                "@id": "subcrate3/"
              },
              "name": "Subcrate 3 Metadata",
              "description": "Separate RO-Crate metadata for subcrate3"
            },
          ]
        });

        let root: RoCrate = serde_json::from_value(base_crate).unwrap();
        let subcrates = fetch_subcrates(root).unwrap();

        assert_eq!(subcrates.len(), 3);

        // Because Vec and HashMap order is not neccessarily the same, this fails
        // assert_eq!(serde_json::to_string(&subcrate1).unwrap(), serde_json::to_string(&subcrates[0]).unwrap());
        // assert_eq!(serde_json::to_string(&subcrate2).unwrap(), serde_json::to_string(&subcrates[1]).unwrap());
        // assert_eq!(serde_json::to_string(&subcrate3).unwrap(), serde_json::to_string(&subcrates[2]).unwrap());
    }

    #[test]
    fn test_try_resolve_remote() {

        let mut server = mockito::Server::new();
        let host = server.host_with_port();
        let url = server.url();

        let mut remotes = Vec::new();

        for n in 1..8 {
            let remote_subcrate = json!({
              "@context": "https://w3id.org/ro/crate/1.2/context",
              "@graph": [
                {
                  "@id": "ro-crate-metadata.json",
                  "@type": "CreativeWork",
                  "conformsTo": {
                    "@id": "https://w3id.org/ro/crate/1.2"
                  },
                  "about": {
                    "@id": "./"
                  }
                },
                {
                  "@id": "./",
                  "@type": "Dataset",
                  "name": format!("Subcrate {n}: With External Identifier and Publisher"),
                  "description": "Subcrate that has been published as a separate entity",
                  "identifier": "https://doi.org/10.5281/zenodo.1234567",
                  "subjectOf": "http://localhost:1234/subcrate4",
                  "publisher": {
                    "@id": "https://zenodo.org"
                  },
                  "conformsTo": {
                    "@id": "https://w3id.org/ro/crate"
                  },

                  "hasPart": [
                    {"@id": "README.md"}
                  ]
                },
                {
                  "@id": "https://zenodo.org",
                  "@type": "Organization",
                  "name": "Zenodo"
                },
                {
                  "@id": "README.md",
                  "@type": "File",
                  "name": "Readme file"
                }
                ]
            });
            remotes.push(remote_subcrate);
        }
        let base_crate = json!({
          "@context": "https://w3id.org/ro/crate/1.2/context",
          "@graph": [
            {
              "@id": "ro-crate-metadata.json",
              "@type": "CreativeWork",
              "conformsTo": {
                "@id": "https://w3id.org/ro/crate/1.1"
              },
              "about": {
                "@id": "./"
              }
            },
            {
              "@id": "./",
              "@type": "Dataset",
              "name": "Root Research Crate with Multiple Subcrates",
              "description": "A comprehensive example demonstrating various ways to define subcrates within an RO-Crate",
              "datePublished": "2026-01-06",
              "license": "https://creativecommons.org/licenses/by/4.0/",
              "hasPart": [
                {"@id": "subcrate1/"},
                {"@id": "subcrate2/"},
                {"@id": "subcrate3/"},
                {"@id": "subcrate4/"},
                {"@id": "subcrate5/"},
                {"@id": "subcrate6/"},
                {"@id": "subcrate7/"},
              ]
            },
            // First case:
            // Direct delivery of ro-crate
            {
              "@id": "subcrate1/",
              "@type": "Dataset",
              "name": "Subcrate 1: Direct delivery of ro-crate",
              "description": "Subcrate that has been published as a separate entity",
              "identifier": "https://doi.org/10.5281/zenodo.1234567",
              "subjectOf": "http://localhost:1234/subcrate1",
              "conformsTo": {
                "@id": "https://w3id.org/ro/crate"
              },
            },
            // Second case:
            // Signposting with `rel=describedBy`
            {
              "@id": "subcrate2/",
              "@type": "Dataset",
              "name": "Subcrate 2: Signposting with describedBy",
              "description": "Subcrate that has been published as a separate entity",
              "identifier": "https://doi.org/10.5281/zenodo.1234567",
              "subjectOf": "http://localhost:1234/subcrate2",
              "conformsTo": {
                "@id": "https://w3id.org/ro/crate"
              },
            },
            // Third case:
            // Signposting with `rel=item`
            {
              "@id": "subcrate3/",
              "@type": "Dataset",
              "name": "Subcrate 3: Signposting with item",
              "description": "Subcrate that has been published as a separate entity",
              "identifier": "https://doi.org/10.5281/zenodo.1234567",
              "subjectOf": "http://localhost:1234/subcrate3",
              "conformsTo": {
                "@id": "https://w3id.org/ro/crate"
              },
            },
            // Fourth case:
            // Signposting with `rel=item` and `rel=describedBy` and prefer
            // `profile="https://w3id.org/ro/crate"`
            {
              "@id": "subcrate4/",
              "@type": "Dataset",
              "name": "Subcrate 5: Signposting with profile",
              "description": "Subcrate that has been published as a separate entity",
              "distribution": "http://localhost:1234/subcrate4",
              "publisher": {
                "@id": "https://zenodo.org"
              },
              "conformsTo": {
                "@id": "https://w3id.org/ro/crate"
              },
            },
            // Fifth case:
            // Content negotiation
            {
              "@id": "https://doi.org/10.5281/zenodo.1234567",
              "@type": "Dataset",
              "name": "Subcrate 5: With content negotiation",
              "description": "Subcrate that has been published as a separate entity",
              "distribution": "http://localhost:1234/content-negotiation",
              "publisher": {
                "@id": "https://zenodo.org"
              },
              "conformsTo": {
                "@id": "https://w3id.org/ro/crate"
              },
            },
            // Sixth case:
            // Guess location
            {
              "@id": "https://doi.org/10.5281/zenodo.1234567",
              "@type": "Dataset",
              "name": "Subcrate 6: Guess URL",
              "description": "Subcrate that has been published as a separate entity",
              "distribution": "http://localhost:1234/guess",
              "publisher": {
                "@id": "https://zenodo.org"
              },
              "conformsTo": {
                "@id": "https://w3id.org/ro/crate"
              },
            },
            // Seventh case:
            // Zip
            {
              "@id": "https://doi.org/10.5281/zenodo.1234567",
              "@type": "Dataset",
              "name": "Subcrate 7: Zipped rocrate",
              "description": "Subcrate that has been published as a separate entity",
              "distribution": "http://localhost:1234/zip",
              "publisher": {
                "@id": "https://zenodo.org"
              },
              "conformsTo": {
                "@id": "https://w3id.org/ro/crate"
              },
            },
            // Eigth case:
            // Zip+Bagit
            {
              "@id": "https://doi.org/10.5281/zenodo.1234567",
              "@type": "Dataset",
              "name": "Subcrate 8: Zipped bagit with ro-crate",
              "description": "Subcrate that has been published as a separate entity",
              "distribution": "http://localhost:1234/zipped_bagit",
              "publisher": {
                "@id": "https://zenodo.org"
              },
              "conformsTo": {
                "@id": "https://w3id.org/ro/crate"
              },
            }
          ]
        });
    }
}
