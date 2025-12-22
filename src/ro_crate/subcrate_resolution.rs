use crate::ro_crate::constraints::Id;
use crate::ro_crate::data_entity::DataEntity;
use crate::ro_crate::rocrate::RoCrate;
use crate::ro_crate::write::is_not_url;
use log::debug;
use reqwest::header::ToStrError;
use zip::ZipArchive;
use zip::result::ZipError;
use std::io::Read;

#[derive(Debug)]
pub enum FetchError {
    NotFound(String),
    InvalidId { key: String, value: String },
    Reqwest(reqwest::Error),
    HeaderValueConversion(ToStrError),
    ZipError(ZipError),
    IoError(std::io::Error),
    SerializationError(serde_json::Error)
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

    'subcrate_loop: for graph_vector in subcrates {
        let subcrate = match graph_vector {
            crate::ro_crate::graph_vector::GraphVector::DataEntity(data_entity) => data_entity,
            _ => continue,
        };
        // TODO:
        // 1. Try subjectOf (to url/path that leads to ro-crate-metadata.json)
        let id = if let Some(subject_of) = try_property(subcrate, "subjectOf") {
            subject_of
        } else {
            // 2. Try distribution (to url that leads to an archive)
            if let Some(distribution) = try_property(subcrate, "distribution") {
                distribution
            } else {
                // 3. Try retrieving ro-crate by id
                subcrate.id.clone()
            }
        };

        if is_not_url(&id) {
            todo!("Resolve locally")
        } else {
            let response = reqwest::blocking::get(&id)?;
            let headers = response.headers().clone();
            let redirect_url = response.url().to_string();
            match response.json::<RoCrate>() {
                Ok(ro_crate) => collected_subcrates.push(ro_crate),
                Err(err) => {
                    debug!("{}", err);

                    // 1. **signposting** to id and look for Link with `rel="describedBy"`
                    //    or `rel="item"` and prefer links for both where `profile="https://w3id.org/ro/crate`
                    for link in headers.get_all("Link") {
                        let values = link.to_str()?.to_string();
                        if values.contains("profile=\"https://w3id.org/ro/crate\"") {
                            if let Some((link, _)) = values.split_once(";") {
                                let url = link.replace("<", "").replace(">", "");
                                let response: RoCrate = reqwest::blocking::get(&url)?.json()?;
                                collected_subcrates.push(response);
                                continue 'subcrate_loop;
                            }
                        } else {
                            if values.contains("rel=\"describedBy\"")
                                || values.contains("rel=\"item\"")
                            {
                                if let Some((link, _)) = values.split_once(";") {
                                    let url = link.replace("<", "").replace(">", "");
                                    let response: RoCrate = reqwest::blocking::get(&url)?.json()?;
                                    collected_subcrates.push(response);
                                    continue 'subcrate_loop;
                                }
                            }
                        }
                    }
                    // 2. **content negotiation** with accept header `application/ld+json;profile=https://w3id.org/ro/crate`
                    let content_negotiation_response = reqwest::blocking::Client::new()
                        .get(&id)
                        .header(
                            "Accept",
                            "application/ld+json;profile=https://w3id.org/ro/crate",
                        )
                        .send()?;

                    if let Ok(response) = content_negotiation_response.json::<RoCrate>() {
                        collected_subcrates.push(response);
                        continue 'subcrate_loop;
                    };
                    // 3. **basically guess**: If PID `https://w3id.org/workflowhub/workflow-ro-crate/1.0`
                    //    redirects to `https://about.workflowhub.eu/Workflow-RO-Crate/1.0/index.html`
                    //    then try `https://about.workflowhub.eu/Workflow-RO-Crate/1.0/ro-crate-metadata.json`
                    let guessed_url = if redirect_url.ends_with("/") {
                        format!("{}ro-crate-metadata.json", redirect_url)
                    } else {
                        if let Some((base, _)) = redirect_url.rsplit_once("/") {
                            format!("{}ro-crate-metadata.json", base)
                        } else {
                            redirect_url.clone()
                        }
                    };
                    let content_negotiation_response =
                        reqwest::blocking::Client::new().get(guessed_url).send()?;

                    if let Ok(response) = content_negotiation_response.json::<RoCrate>() {
                        collected_subcrates.push(response);
                        continue 'subcrate_loop;
                    };

                    // 4. If retrieved resource has `Content-Type: application/zip` or is a ZIP file
                    //    extract ro-crate-metadata.json or if only contains single folder, extract
                    //    folder/ro-crate-metadata.json
                    if let Some(content_type) = headers.get("Content-Type") {
                        if content_type.to_str()?.contains("application/zip") {
                            let response= reqwest::blocking::get(&redirect_url)?.bytes()?;
                            let reader = std::io::Cursor::new(response);
                            let mut archive = ZipArchive::new(reader)?;

                            // 2. Retrieve the file by name
                            let mut file_in_zip =
                                archive.by_name("ro-crate-metadata.json")?;

                            // 3. Read the file contents into memory
                            let mut buffer = Vec::new();
                            file_in_zip.read_to_end(&mut buffer)?;

                            let subcrate: RoCrate = serde_json::from_slice(&buffer)?;
                            collected_subcrates.push(subcrate);
                            continue 'subcrate_loop
                        }
                    }
                    // 5. If retrieved resource is a BagIt archive, extract and verify checksums,
                    //    then return data/ro-crate-metdata.json
                    todo!("Bagit support")
                    // 6. If returned file is json-ld and has a root data entity, this is the
                    //     ro-crate metadata file
                }
            };
        }
    }

    Ok(vec![])
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
