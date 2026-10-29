use crate::ro_crate::constraints::Id;
use crate::ro_crate::data_entity::DataEntity;
use crate::ro_crate::read::{MAX_ARCHIVE_BYTES, MAX_METADATA_BYTES, read_with_limit};
use crate::ro_crate::rocrate::RoCrate;
use crate::ro_crate::write::is_not_url;
use log::warn;
use reqwest::blocking::Response;
use reqwest::header::{HeaderMap, ToStrError};
use sha2::Digest;
use std::collections::{HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::string::FromUtf8Error;
use zip::ZipArchive;
use zip::result::ZipError;

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
    RegexError(regex::Error),
    TraversalLimit(String),
    RemoteAccessDisabled(String),
    LocalAccessDenied(PathBuf),
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
            FetchError::RegexError(err) => {
                write!(f, "RegexError `{}`", err)
            }
            FetchError::TraversalLimit(reason) => write!(f, "Subcrate traversal limit: {reason}"),
            FetchError::RemoteAccessDisabled(id) => {
                write!(f, "Remote subcrate access is disabled for {id}")
            }
            FetchError::LocalAccessDenied(path) => {
                write!(
                    f,
                    "Local subcrate path is outside the allowed root: {}",
                    path.display()
                )
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
impl From<regex::Error> for FetchError {
    fn from(value: regex::Error) -> Self {
        FetchError::RegexError(value)
    }
}

/// Network access is disabled by default because subcrate locators are supplied
/// by crate metadata. Callers processing trusted remote references can opt in.
#[derive(Clone, Debug, Default)]
pub struct FetchOptions {
    pub allow_remote: bool,
    /// Directory that local subcrate paths must remain inside. Defaults to the
    /// current working directory.
    pub local_root: Option<PathBuf>,
}

/// Fails when a subcrate cannot be resolved
pub fn fetch_subcrates_recursive(rocrate: &RoCrate) -> Result<Vec<RoCrate>, FetchError> {
    fetch_subcrates_recursive_with_options(rocrate, FetchOptions::default())
}

pub fn fetch_subcrates_recursive_with_options(
    rocrate: &RoCrate,
    options: FetchOptions,
) -> Result<Vec<RoCrate>, FetchError> {
    walk_subcrates(rocrate, true, options)
}

/// Does not fail when a subcrate cannot be resolved
pub fn try_fetch_subcrates_recursive(rocrate: &RoCrate) -> Vec<RoCrate> {
    try_fetch_subcrates_recursive_with_options(rocrate, FetchOptions::default())
}

pub fn try_fetch_subcrates_recursive_with_options(
    rocrate: &RoCrate,
    options: FetchOptions,
) -> Vec<RoCrate> {
    walk_subcrates(rocrate, false, options).expect("lenient subcrate traversal cannot fail")
}

const MAX_SUBCRATES: usize = 256;
const MAX_SUBCRATE_DEPTH: usize = 16;

fn walk_subcrates(
    rocrate: &RoCrate,
    strict: bool,
    options: FetchOptions,
) -> Result<Vec<RoCrate>, FetchError> {
    let mut flattened = Vec::new();
    let mut seen = HashSet::new();
    let mut queue = subcrate_ids(rocrate)
        .into_iter()
        .map(|id| (id, 1))
        .collect::<VecDeque<_>>();

    while let Some((id, depth)) = queue.pop_front() {
        let key = normalize_subcrate_id(&id, &options);
        if !seen.insert(key) {
            continue;
        }
        if seen.len() > MAX_SUBCRATES || depth > MAX_SUBCRATE_DEPTH {
            let reason = format!(
                "at most {MAX_SUBCRATES} distinct subcrates and {MAX_SUBCRATE_DEPTH} levels"
            );
            if strict {
                return Err(FetchError::TraversalLimit(reason));
            }
            warn!("{reason}");
            break;
        }

        let subcrate = resolve_subcrate(&id, &options);
        match subcrate {
            Ok(subcrate) => {
                queue.extend(
                    subcrate_ids(&subcrate)
                        .into_iter()
                        .map(|id| (id, depth + 1)),
                );
                flattened.push(subcrate);
            }
            Err(err) if strict => return Err(err),
            Err(err) => warn!("{err}"),
        }
    }
    Ok(flattened)
}

fn normalize_subcrate_id(id: &str, options: &FetchOptions) -> String {
    if is_not_url(id) {
        let path = local_subcrate_path(id, options.local_root.as_deref().unwrap_or(Path::new(".")));
        path.canonicalize()
            .unwrap_or(path)
            .to_string_lossy()
            .to_string()
    } else {
        url::Url::parse(id)
            .map(|url| url.to_string())
            .unwrap_or_else(|_| id.to_string())
    }
}

fn subcrate_ids(rocrate: &RoCrate) -> Vec<String> {
    rocrate
        .get_subcrates()
        .into_iter()
        .filter_map(|entity| match entity {
            crate::ro_crate::graph_vector::GraphVector::DataEntity(data) => Some(get_id(data)),
            _ => None,
        })
        .collect()
}

/// Fails when a subcrate cannot be resolved
pub fn fetch_subcrates(rocrate: &RoCrate) -> Result<Vec<RoCrate>, FetchError> {
    fetch_subcrates_with_options(rocrate, FetchOptions::default())
}

pub fn fetch_subcrates_with_options(
    rocrate: &RoCrate,
    options: FetchOptions,
) -> Result<Vec<RoCrate>, FetchError> {
    let subcrates = rocrate.get_subcrates();

    let mut collected_subcrates = Vec::new();

    for graph_vector in subcrates {
        let subcrate = match graph_vector {
            crate::ro_crate::graph_vector::GraphVector::DataEntity(data_entity) => data_entity,
            _ => continue,
        };

        // Try to find the subcrate id
        let id = get_id(subcrate);

        collected_subcrates.push(resolve_subcrate(&id, &options)?);
    }

    Ok(collected_subcrates)
}

/// Does not fail when a subcrate is not found
pub fn try_fetch_subcrates(rocrate: &RoCrate) -> Vec<RoCrate> {
    try_fetch_subcrates_with_options(rocrate, FetchOptions::default())
}

pub fn try_fetch_subcrates_with_options(rocrate: &RoCrate, options: FetchOptions) -> Vec<RoCrate> {
    let subcrates = rocrate.get_subcrates();

    let mut collected_subcrates = Vec::new();

    for graph_vector in subcrates {
        let subcrate = match graph_vector {
            crate::ro_crate::graph_vector::GraphVector::DataEntity(data_entity) => data_entity,
            _ => continue,
        };

        // Try to find the subcrate id
        let id = get_id(subcrate);

        match resolve_subcrate(&id, &options) {
            Ok(rocrate) => collected_subcrates.push(rocrate),
            Err(err) => warn!("{err}"),
        }
    }

    collected_subcrates
}

fn resolve_subcrate(id: &str, options: &FetchOptions) -> Result<RoCrate, FetchError> {
    if is_not_url(id) {
        try_resolve_local(id, options)
    } else if options.allow_remote {
        try_resolve_remote(id)
    } else {
        Err(FetchError::RemoteAccessDisabled(id.to_string()))
    }
}

fn try_resolve_local(id: &str, options: &FetchOptions) -> Result<RoCrate, FetchError> {
    let root = options
        .local_root
        .as_deref()
        .unwrap_or(Path::new("."))
        .canonicalize()?;
    let path = local_subcrate_path(id, &root).canonicalize()?;
    if !path.starts_with(&root) {
        return Err(FetchError::LocalAccessDenied(path));
    }

    let mut file = std::fs::File::open(path)?;
    let buffer = read_with_limit(&mut file, MAX_METADATA_BYTES)?;

    Ok(serde_json::from_slice(buffer.as_slice())?)
}

fn local_subcrate_path(id: &str, root: &Path) -> PathBuf {
    let candidate = Path::new(id);
    let candidate = if candidate.is_absolute() {
        candidate.to_path_buf()
    } else {
        root.join(candidate)
    };
    if id.ends_with('/') {
        candidate.join("ro-crate-metadata.json")
    } else {
        candidate
    }
}

fn try_direct_resolve_or_zip(response: Response) -> Result<RoCrate, FetchError> {
    let headers = response.headers().clone();
    let redirect_url = response.url().to_string();
    let body = bytes::Bytes::from(read_with_limit(response, MAX_ARCHIVE_BYTES)?);

    if body.len() <= MAX_METADATA_BYTES
        && let Ok(ro_crate) = serde_json::from_slice::<RoCrate>(&body)
    {
        return Ok(ro_crate);
    }

    if let Ok(ro_crate) = try_zip(&headers, body) {
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
    if let Ok(ro_crate) = try_signposting(&headers).and_then(try_direct_resolve_or_zip) {
        return Ok(ro_crate);
    }

    // Try content negotiation
    if let Ok(ro_crate) = try_content_negotiation(id).and_then(try_direct_resolve_or_zip) {
        return Ok(ro_crate);
    }

    // Guess location
    if let Ok(ro_crate) = guess_location(&redirect_url).and_then(try_direct_resolve_or_zip) {
        return Ok(ro_crate);
    }

    Err(FetchError::NotFound(format!(
        "Could not retrieve subcrate with id {id}"
    )))
}

/// Extract the metadata URL from subjectOf property
fn try_property(entity: &DataEntity, value: &str) -> Option<String> {
    if let Some(dynamic_entities) = &entity.dynamic_entity
        && let Some(value) = dynamic_entities.get(value).cloned()
        && let super::constraints::EntityValue::EntityId(Id::Id(id)) = value
    {
        return Some(id);
    }
    None
}

fn get_id(entity: &DataEntity) -> String {
    // 1. Try subjectOf (to url/path that leads to ro-crate-metadata.json)
    if let Some(subject_of) = try_property(entity, "subjectOf") {
        subject_of
    } else {
        // 2. Try distribution (to url that leads to an archive)
        if let Some(distribution) = try_property(entity, "distribution") {
            distribution
        } else {
            // 3. Try retrieving ro-crate by id
            entity.id.clone()
        }
    }
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
        } else if (values.contains("rel=\"describedBy\"") || values.contains("rel=\"item\""))
            && let Some((link, _)) = values.split_once(";")
        {
            let url = link.replace("<", "").replace(">", "");
            let response = reqwest::blocking::get(&url)?;
            return Ok(response);
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
            "application/json+ld;profile=https://w3id.org/ro/crate",
        )
        .send()?;

    match content_negotiation_response.status().as_u16() {
        300 | 406 | 415 => {
            // Try zip
            let content_negotiation_response = reqwest::blocking::Client::new()
                .get(id)
                .header(
                    "Accept",
                    "application/zip;profile=https://w3id.org/ro/crate",
                )
                .send()?;
            Ok(content_negotiation_response)
        }
        200 => Ok(content_negotiation_response),
        _ => {
            if let Err(err) = content_negotiation_response.error_for_status() {
                Err(FetchError::Reqwest(err))
            } else {
                Err(FetchError::NotFound(format!("Subcrate {} not found", id)))
            }
        }
    }
}

fn guess_location(redirect_url: &str) -> Result<Response, FetchError> {
    let regex = regex::Regex::new(r"(/\w*\.\w*)$|/\w*$")?;
    // 3. **basically guess**: If PID `https://w3id.org/workflowhub/workflow-ro-crate/1.0`
    //    redirects to `https://about.workflowhub.eu/Workflow-RO-Crate/1.0/index.html`
    //    then try `https://about.workflowhub.eu/Workflow-RO-Crate/1.0/ro-crate-metadata.json`
    let guessed_url = if redirect_url.ends_with("/") {
        format!("{}ro-crate-metadata.json", redirect_url)
    } else if regex.is_match(redirect_url) {
        regex
            .replace(redirect_url, "/ro-crate-metadata.json")
            .to_string()
    } else {
        format!("{}/ro-crate-metadata.json", redirect_url)
    };

    let response = reqwest::blocking::Client::new().get(guessed_url).send()?;

    Ok(response)
}

fn try_zip(headers: &HeaderMap, bytes: bytes::Bytes) -> Result<RoCrate, FetchError> {
    // 4. If retrieved resource has `Content-Type: application/zip` or is a ZIP file
    //    extract ro-crate-metadata.json or if only contains single folder, extract
    //    folder/ro-crate-metadata.json
    if let Some(content_type) = headers.get("Content-Type")
        && content_type.to_str()?.contains("application/zip")
    {
        let reader = std::io::Cursor::new(bytes);
        let mut archive = ZipArchive::new(reader)?;

        // A BagIt manifest is an integrity assertion. If verification fails,
        // do not rediscover and return the same metadata via the generic ZIP path.
        if archive
            .file_names()
            .any(|name| matches!(name.as_deref(), Ok("bagit.txt")))
        {
            return try_bagit(archive);
        }

        // Try retrieving file by name
        match archive.by_name("ro-crate-metadata.json") {
            Ok(mut file_in_zip) => {
                // Read the file contents into memory
                let buffer = read_with_limit(&mut file_in_zip, MAX_METADATA_BYTES)?;

                let subcrate: RoCrate = serde_json::from_slice(&buffer)?;

                return Ok(subcrate);
            }
            Err(err) => warn!("{}", err),
        }

        // Try finding rocrate in subdirectories
        let names: Vec<String> = archive
            .file_names()
            .map(|e| e.map(|n| n.to_string()))
            .collect::<Result<_, _>>()?;
        if let Some(rocrate) = names
            .iter()
            .find(|name| name.ends_with("/ro-crate-metadata.json"))
        {
            let mut file_in_zip = archive.by_name(rocrate)?;

            let buffer = read_with_limit(&mut file_in_zip, MAX_METADATA_BYTES)?;

            let subcrate: RoCrate = serde_json::from_slice(&buffer)?;

            return Ok(subcrate);
        }
    }
    Err(FetchError::NotFound("No subcrate found".to_string()))
}

fn try_bagit(
    mut archive: ZipArchive<std::io::Cursor<bytes::Bytes>>,
) -> Result<RoCrate, FetchError> {
    if archive.by_name("bagit.txt").is_ok() {
        // 5. If retrieved resource is a BagIt archive, extract and verify checksums,
        //    then return data/ro-crate-metadata.json
        let mut rocrate = archive.by_name("data/ro-crate-metadata.json")?;
        // Parse ro-crate
        let ro_crate_buffer = read_with_limit(&mut rocrate, MAX_METADATA_BYTES)?;
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
    let buffer = read_with_limit(&mut sha512, MAX_METADATA_BYTES)?;

    let hashes = String::from_utf8(buffer)?;
    let hash = hashes
        .lines()
        .find_map(|line| {
            if line.contains("ro-crate-metadata.json") {
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
    use std::collections::HashMap;

    #[test]
    fn zip_metadata_is_bounded_after_decompression() {
        use std::io::Write;

        let mut writer = zip::ZipWriter::new(std::io::Cursor::new(Vec::<u8>::new()));
        writer
            .start_file(
                "ro-crate-metadata.json",
                zip::write::SimpleFileOptions::default()
                    .compression_method(zip::CompressionMethod::Deflated),
            )
            .unwrap();
        writer
            .write_all(&vec![b'a'; MAX_METADATA_BYTES + 1])
            .unwrap();
        let body = bytes::Bytes::from(writer.finish().unwrap().into_inner());
        let mut headers = HeaderMap::new();
        headers.insert(
            reqwest::header::CONTENT_TYPE,
            reqwest::header::HeaderValue::from_static("application/zip"),
        );

        assert!(matches!(
            try_zip(&headers, body),
            Err(FetchError::IoError(error)) if error.kind() == std::io::ErrorKind::InvalidData
        ));
    }

    #[test]
    fn local_subcrate_cannot_escape_allowed_root() {
        let root = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let secret = outside.path().join("secret.json");
        std::fs::write(&secret, b"{}").unwrap();
        let options = FetchOptions {
            local_root: Some(root.path().to_path_buf()),
            ..Default::default()
        };

        assert!(matches!(
            try_resolve_local(secret.to_str().unwrap(), &options),
            Err(FetchError::LocalAccessDenied(_))
        ));

        #[cfg(unix)]
        {
            std::os::unix::fs::symlink(&secret, root.path().join("linked.json")).unwrap();
            assert!(matches!(
                try_resolve_local("linked.json", &options),
                Err(FetchError::LocalAccessDenied(_))
            ));
        }
    }

    #[test]
    fn remote_subcrate_requires_opt_in() {
        let root: RoCrate = serde_json::from_value(serde_json::json!({
            "@context": "https://w3id.org/ro/crate/1.1/context",
            "@graph": [
                {
                    "@id": "ro-crate-metadata.json",
                    "@type": "CreativeWork",
                    "conformsTo": {"@id": "https://w3id.org/ro/crate/1.1"},
                    "about": {"@id": "./"}
                },
                {
                    "@id": "http://127.0.0.1:12345/private",
                    "@type": "Dataset",
                    "conformsTo": {"@id": "https://w3id.org/ro/crate/1.1"}
                }
            ]
        }))
        .unwrap();

        assert!(matches!(
            fetch_subcrates(&root),
            Err(FetchError::RemoteAccessDisabled(_))
        ));
        assert!(try_fetch_subcrates(&root).is_empty());
    }

    #[test]
    fn recursive_fetch_stops_at_self_referential_subcrate() {
        let directory = tempfile::tempdir().unwrap();
        let metadata_path = directory.path().join("ro-crate-metadata.json");
        let crate_json = serde_json::json!({
            "@context": "https://w3id.org/ro/crate/1.1/context",
            "@graph": [
                {
                    "@id": "ro-crate-metadata.json",
                    "@type": "CreativeWork",
                    "conformsTo": {"@id": "https://w3id.org/ro/crate/1.1"},
                    "about": {"@id": "./"}
                },
                {
                    "@id": metadata_path.to_string_lossy(),
                    "@type": "Dataset",
                    "conformsTo": {"@id": "https://w3id.org/ro/crate/1.1"}
                }
            ]
        });
        std::fs::write(&metadata_path, crate_json.to_string()).unwrap();
        let root: RoCrate = serde_json::from_value(crate_json).unwrap();

        let options = FetchOptions {
            local_root: Some(directory.path().to_path_buf()),
            ..Default::default()
        };
        assert_eq!(
            fetch_subcrates_recursive_with_options(&root, options.clone())
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            try_fetch_subcrates_recursive_with_options(&root, options).len(),
            1
        );
    }

    #[test]
    fn recursive_fetch_enforces_depth_limit() {
        let directory = tempfile::tempdir().unwrap();
        let paths: Vec<_> = (0..=MAX_SUBCRATE_DEPTH + 1)
            .map(|index| directory.path().join(format!("crate-{index}.json")))
            .collect();

        for (index, path) in paths.iter().enumerate() {
            let mut graph = vec![serde_json::json!({
                "@id": "ro-crate-metadata.json",
                "@type": "CreativeWork",
                "conformsTo": {"@id": "https://w3id.org/ro/crate/1.1"},
                "about": {"@id": "./"}
            })];
            if let Some(next) = paths.get(index + 1) {
                graph.push(serde_json::json!({
                    "@id": next.to_string_lossy(),
                    "@type": "Dataset",
                    "conformsTo": {"@id": "https://w3id.org/ro/crate/1.1"}
                }));
            }
            let metadata = serde_json::json!({
                "@context": "https://w3id.org/ro/crate/1.1/context",
                "@graph": graph
            });
            std::fs::write(path, metadata.to_string()).unwrap();
        }

        let root: RoCrate = serde_json::from_slice(&std::fs::read(&paths[0]).unwrap()).unwrap();
        let options = FetchOptions {
            local_root: Some(directory.path().to_path_buf()),
            ..Default::default()
        };
        assert!(matches!(
            fetch_subcrates_recursive_with_options(&root, options.clone()),
            Err(FetchError::TraversalLimit(_))
        ));
        assert_eq!(
            try_fetch_subcrates_recursive_with_options(&root, options).len(),
            MAX_SUBCRATE_DEPTH
        );
    }

    #[test]
    fn bagit_with_bad_manifest_is_rejected() {
        use std::io::Write;
        use zip::write::SimpleFileOptions;

        let mut writer = zip::ZipWriter::new(std::io::Cursor::new(Vec::<u8>::new()));
        let options = SimpleFileOptions::default();
        writer.start_file("bagit.txt", options).unwrap();
        writer.write_all(b"BagIt-Version: 1.0\n").unwrap();
        writer.start_file("manifest-sha256.txt", options).unwrap();
        writer
            .write_all(format!("{}  data/ro-crate-metadata.json\n", "0".repeat(64)).as_bytes())
            .unwrap();
        writer
            .start_file("data/ro-crate-metadata.json", options)
            .unwrap();
        writer
            .write_all(br#"{"@context":"https://w3id.org/ro/crate/1.3/context","@graph":[]}"#)
            .unwrap();

        let body = bytes::Bytes::from(writer.finish().unwrap().into_inner());
        let mut headers = HeaderMap::new();
        headers.insert(
            reqwest::header::CONTENT_TYPE,
            reqwest::header::HeaderValue::from_static("application/zip"),
        );

        assert!(matches!(
            try_zip(&headers, body),
            Err(FetchError::BagItError(_))
        ));
    }

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
}
