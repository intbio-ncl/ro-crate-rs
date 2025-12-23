use crate::ro_crate::constraints::Id;
use crate::ro_crate::data_entity::DataEntity;
use crate::ro_crate::rocrate::RoCrate;
use crate::ro_crate::write::is_not_url;
use log::{debug, warn};
use reqwest::header::{HeaderMap, ToStrError};
use std::io::{Bytes, Cursor, Read};
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

        if is_not_url(&id) {

            match try_resolve_local(&id) {
                Ok(rocrate) => {
                    collected_subcrates.push(rocrate);
                    continue;
                }
                Err(err) => warn!("{}", err),
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

    Ok(vec![])
}

fn try_resolve_local(id: &str) -> Result<RoCrate, FetchError> {
    todo!()
}

fn try_resolve_remote(id: &str) -> Result<RoCrate, FetchError> {
    let response = reqwest::blocking::get(id)?;
    let headers = response.headers().clone();
    let redirect_url = response.url().to_string();
    let body = response.bytes()?;

    if let Ok(ro_crate) = serde_json::from_slice::<RoCrate>(&body) {
        return Ok(ro_crate);
    }

    if let Ok(ro_crate) = try_signposting(&headers) {
        return Ok(ro_crate);
    }

    if let Ok(ro_crate) = try_content_negotiation(&id) {
        return Ok(ro_crate);
    }

    if let Ok(ro_crate) = guess_location(&redirect_url) {
        return Ok(ro_crate);
    }

    if let Ok(ro_crate) = try_zip(&headers, &redirect_url) {
        return Ok(ro_crate);
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
    // TODO:
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

fn try_signposting(headers: &HeaderMap) -> Result<RoCrate, FetchError> {
    // 1. **signposting** to id and look for Link with `rel="describedBy"`
    //    or `rel="item"` and prefer links for both where `profile="https://w3id.org/ro/crate`
    for link in headers.get_all("Link") {
        let values = link.to_str()?.to_string();
        if values.contains("profile=\"https://w3id.org/ro/crate\"") {
            if let Some((link, _)) = values.split_once(";") {
                let url = link.replace("<", "").replace(">", "");
                let rocrate: RoCrate = reqwest::blocking::get(&url)?.json()?;
                return Ok(rocrate);
            }
        } else {
            if values.contains("rel=\"describedBy\"") || values.contains("rel=\"item\"") {
                if let Some((link, _)) = values.split_once(";") {
                    let url = link.replace("<", "").replace(">", "");
                    let rocrate: RoCrate = reqwest::blocking::get(&url)?.json()?;
                    return Ok(rocrate);
                }
            }
        }
    }
    Err(FetchError::NotFound("No valid rocrate found".to_string()))
}

fn try_content_negotiation(id: &str) -> Result<RoCrate, FetchError> {
    // 2. **content negotiation** with accept header `application/ld+json;profile=https://w3id.org/ro/crate`
    let content_negotiation_response = reqwest::blocking::Client::new()
        .get(id)
        .header(
            "Accept",
            "application/ld+json;profile=https://w3id.org/ro/crate",
        )
        .send()?;

    Ok(content_negotiation_response.json::<RoCrate>()?)
}

fn guess_location(redirect_url: &str) -> Result<RoCrate, FetchError> {
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

    Ok(content_negotiation_response.json::<RoCrate>()?)
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

            // Retrieve the file by name
            if let Ok(mut file_in_zip) = archive.by_name("ro-crate-metadata.json") {
                // Read the file contents into memory
                let mut buffer = Vec::new();
                file_in_zip.read_to_end(&mut buffer)?;

                let subcrate: RoCrate = serde_json::from_slice(&buffer)?;

                return Ok(subcrate);
            }
            if let Ok(mut bagit) = archive.by_name("bagit.txt") {
                // 5. If retrieved resource is a BagIt archive, extract and verify checksums,
                //    then return data/ro-crate-metdata.json
                let mut buffer = Vec::new();
                bagit.read_to_end(&mut buffer)?;

                let subcrate: RoCrate = serde_json::from_slice(&buffer)?;
                return Ok(subcrate);
            }
            // Handle directories
            let names: Vec<String> = archive.file_names().map(|e| e.to_string()).collect();
            if let Some(bagit) = names.iter().find(|x| x.contains("bagit.txt")) {
                todo!("Handle bagit");
            }
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
