use std::io::{self, Seek};
use std::path::Path;
use std::{collections::HashMap, io::Read};
use sha1::Sha1;
use sha2::{Sha256, Sha512, Digest};
use md5::Md5;

#[derive(Debug)]
pub enum BagItError {
    Io(io::Error),
    InvalidStructure(String),
    ChecksumMismatch {
        path: String,
        expected: String,
        actual: String,
    },
    MissingFile(String),
    InvalidManifest(String),
    UnsupportedAlgorithm(String),
    InvalidBagDeclaration(String),
    EncodingError(String),
    FileNotFound(String),
    InvalidIndex(usize)
}

impl std::fmt::Display for BagItError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BagItError::Io(item) => {
                write!(f, "IO error: {}", item)
            }
            BagItError::InvalidStructure(item)=> {
                write!(f, "Invalid bag structure: `{}`", item)
            }
            BagItError::ChecksumMismatch{ path, expected, actual } => {
                write!(f, "Checksum mismatch for {path}: expected {expected}, got {actual}")
            }
            BagItError::MissingFile(err) => {
                write!(f, "Missing required file: {}", err)
            }
            BagItError::InvalidManifest(err) => {
                write!(f, "Invalid manifest format: {}", err)
            }
            BagItError::UnsupportedAlgorithm(err) => {
                write!(f, "Unsupported algorithm: `{}`", err)
            }
            BagItError::InvalidBagDeclaration(err) => {
                write!(f, "Invalid bagit.txt: `{}`", err)
            }
            BagItError::EncodingError(err) => {
                write!(f, "Encoding error: `{}`", err)
            }
            BagItError::InvalidIndex(err) => {
                write!(f, "Invalid file index: `{}`", err)
            }
            BagItError::FileNotFound(err) => {
                write!(f, "File not found in bag: `{}`", err)
            }
        }
    }
}

impl std::error::Error for BagItError {}

impl From<std::io::Error> for BagItError {
    fn from(value: std::io::Error) -> Self {
        BagItError::Io(value)
    }
}

pub type Result<T> = std::result::Result<T, BagItError>;

/// Represents the BagIt version and encoding from bagit.txt
#[derive(Debug, Clone)]
pub struct BagDeclaration {
    pub version: String,
    pub encoding: String,
}

/// Represents a single entry in a manifest file
#[derive(Debug, Clone)]
pub struct ManifestEntry {
    pub checksum: String,
    pub filepath: String,
}

/// Represents metadata from bag-info.txt
#[derive(Debug, Clone, Default)]
pub struct BagMetadata {
    pub fields: HashMap<String, Vec<String>>,
}

impl BagMetadata {
    pub fn get(&self, key: &str) -> Option<&Vec<String>> {
        self.fields.get(&key.to_lowercase())
    }
    
    pub fn get_first(&self, key: &str) -> Option<&String> {
        self.get(key).and_then(|v| v.first())
    }
}

/// Represents an entry in fetch.txt
#[derive(Debug, Clone)]
pub struct FetchEntry {
    pub url: String,
    pub length: Option<u64>,
    pub filepath: String,
}

/// Information about a file stored in the bag
#[derive(Debug, Clone)]
pub struct BagFile {
    name: String,
    data: Vec<u8>,
}

impl BagFile {
    /// Get the file name/path
    pub fn name(&self) -> &str {
        &self.name
    }
    
    /// Get the file size
    pub fn size(&self) -> u64 {
        self.data.len() as u64
    }
    
    /// Check if this is a payload file (in data/ directory)
    pub fn is_payload_file(&self) -> bool {
        self.name.starts_with("data/")
    }
    
    /// Check if this is a tag file
    pub fn is_tag_file(&self) -> bool {
        !self.is_payload_file()
    }
}

/// A reader for a file within the bag
pub struct BagFileReader {
    data: io::Cursor<Vec<u8>>,
}

impl BagFileReader {
    fn new(data: Vec<u8>) -> Self {
        Self {
            data: io::Cursor::new(data),
        }
    }
}

impl Read for BagFileReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.data.read(buf)
    }
}

impl Seek for BagFileReader {
    fn seek(&mut self, pos: io::SeekFrom) -> io::Result<u64> {
        self.data.seek(pos)
    }
}

/// Main BagIt archive reader (similar to ZipArchive)
pub struct BagArchive<R: Read> {
    files: Vec<BagFile>,
    file_indices: HashMap<String, usize>,
    _phantom: std::marker::PhantomData<R>,
}

impl<R: Read> BagArchive<R> {
    /// Create a new BagArchive from a reader
    /// 
    /// Note: This loads all files into memory. For filesystem-based bags,
    /// consider using `BagArchive::open()` with a Path.
    pub fn new(mut reader: R) -> Result<Self> {
        let mut buffer = Vec::new();
        reader.read_to_end(&mut buffer)?;
        
        // This is a simplified implementation - in practice, you'd need
        // to parse a tar/zip container or walk a directory structure
        Self::from_buffer(buffer)
    }
    
    fn from_buffer(buffer: Vec<u8>) -> Result<Self> {
        // Parse the buffer as needed - this is placeholder
        // Real implementation would depend on serialization format
        Ok(Self {
            files: Vec::new(),
            file_indices: HashMap::new(),
            _phantom: std::marker::PhantomData,
        })
    }
    
    /// Create archive from a HashMap of files (internal helper)
    fn from_files(files_map: HashMap<String, Vec<u8>>) -> Self {
        let mut files = Vec::new();
        let mut file_indices = HashMap::new();
        
        for (name, data) in files_map {
            let index = files.len();
            file_indices.insert(name.clone(), index);
            files.push(BagFile { name, data });
        }
        
        Self {
            files,
            file_indices,
            _phantom: std::marker::PhantomData,
        }
    }
    
    /// Get the number of files in the bag
    pub fn len(&self) -> usize {
        self.files.len()
    }
    
    /// Check if the bag is empty
    pub fn is_empty(&self) -> bool {
        self.files.is_empty()
    }
    
    /// Get file metadata by index
    pub fn by_index(&self, index: usize) -> Result<&BagFile> {
        self.files.get(index)
            .ok_or(BagItError::InvalidIndex(index))
    }
    
    /// Get file metadata by name
    pub fn by_name(&self, name: &str) -> Result<&BagFile> {
        let index = self.file_indices.get(name)
            .ok_or_else(|| BagItError::FileNotFound(name.to_string()))?;
        self.by_index(*index)
    }
    
    /// Open a file by index for reading
    pub fn by_index_reader(&self, index: usize) -> Result<BagFileReader> {
        let file = self.by_index(index)?;
        Ok(BagFileReader::new(file.data.clone()))
    }
    
    /// Open a file by name for reading
    pub fn by_name_reader(&self, name: &str) -> Result<BagFileReader> {
        let file = self.by_name(name)?;
        Ok(BagFileReader::new(file.data.clone()))
    }
    
    /// Get an iterator over all file names
    pub fn file_names(&self) -> impl Iterator<Item = &str> {
        self.files.iter().map(|f| f.name.as_str())
    }
    
    /// Parse the bagit.txt declaration
    pub fn declaration(&self) -> Result<BagDeclaration> {
        let mut reader = self.by_name_reader("bagit.txt")?;
        let mut content = String::new();
        reader.read_to_string(&mut content)?;
        
        let mut version = None;
        let mut encoding = None;
        
        for line in content.lines() {
            if line.starts_with("BagIt-Version:") {
                version = Some(line.split(':').nth(1)
                    .ok_or_else(|| BagItError::InvalidBagDeclaration("Missing version value".into()))?
                    .trim()
                    .to_string());
            } else if line.starts_with("Tag-File-Character-Encoding:") {
                encoding = Some(line.split(':').nth(1)
                    .ok_or_else(|| BagItError::InvalidBagDeclaration("Missing encoding value".into()))?
                    .trim()
                    .to_string());
            }
        }
        
        Ok(BagDeclaration {
            version: version.ok_or_else(|| BagItError::InvalidBagDeclaration("Missing BagIt-Version".into()))?,
            encoding: encoding.ok_or_else(|| BagItError::InvalidBagDeclaration("Missing Tag-File-Character-Encoding".into()))?,
        })
    }
    
    /// Parse a manifest file
    pub fn manifest(&self, algorithm: &str) -> Result<Vec<ManifestEntry>> {
        let filename = format!("manifest-{}.txt", algorithm);
        let mut reader = self.by_name_reader(&filename)?;
        let mut content = String::new();
        reader.read_to_string(&mut content)?;
        
        let mut entries = Vec::new();
        
        for line in content.lines() {
            if line.trim().is_empty() {
                continue;
            }
            
            let parts: Vec<&str> = line.splitn(2, char::is_whitespace).collect();
            if parts.len() != 2 {
                return Err(BagItError::InvalidManifest(
                    format!("Invalid line format: {}", line)
                ));
            }
            
            entries.push(ManifestEntry {
                checksum: parts[0].trim().to_lowercase(),
                filepath: decode_filepath(parts[1].trim()),
            });
        }
        
        Ok(entries)
    }
    
    /// Parse tag manifest
    pub fn tag_manifest(&self, algorithm: &str) -> Result<Vec<ManifestEntry>> {
        let filename = format!("tagmanifest-{}.txt", algorithm);
        let mut reader = self.by_name_reader(&filename)?;
        let mut content = String::new();
        reader.read_to_string(&mut content)?;
        
        let mut entries = Vec::new();
        
        for line in content.lines() {
            if line.trim().is_empty() {
                continue;
            }
            
            let parts: Vec<&str> = line.splitn(2, char::is_whitespace).collect();
            if parts.len() != 2 {
                return Err(BagItError::InvalidManifest(
                    format!("Invalid line format: {}", line)
                ));
            }
            
            entries.push(ManifestEntry {
                checksum: parts[0].trim().to_lowercase(),
                filepath: decode_filepath(parts[1].trim()),
            });
        }
        
        Ok(entries)
    }
    
    /// Parse bag-info.txt metadata
    pub fn metadata(&self) -> Result<BagMetadata> {
        let mut reader = match self.by_name_reader("bag-info.txt") {
            Ok(r) => r,
            Err(BagItError::FileNotFound(_)) => return Ok(BagMetadata::default()),
            Err(e) => return Err(e),
        };
        
        let mut content = String::new();
        reader.read_to_string(&mut content)?;
        
        let mut metadata = BagMetadata::default();
        let mut current_key = String::new();
        let mut current_value = String::new();
        
        for line in content.lines() {
            if line.starts_with(char::is_whitespace) {
                // Continuation line
                current_value.push(' ');
                current_value.push_str(line.trim());
            } else if let Some(colon_pos) = line.find(':') {
                // Save previous field if exists
                if !current_key.is_empty() {
                    metadata.fields
                        .entry(current_key.to_lowercase())
                        .or_insert_with(Vec::new)
                        .push(current_value.clone());
                }
                
                // Start new field
                current_key = line[..colon_pos].trim().to_string();
                current_value = line[colon_pos + 1..].trim().to_string();
            }
        }
        
        // Save last field
        if !current_key.is_empty() {
            metadata.fields
                .entry(current_key.to_lowercase())
                .or_insert_with(Vec::new)
                .push(current_value);
        }
        
        Ok(metadata)
    }
    
    /// Parse fetch.txt
    pub fn fetch_entries(&self) -> Result<Vec<FetchEntry>> {
        let mut reader = match self.by_name_reader("fetch.txt") {
            Ok(r) => r,
            Err(BagItError::FileNotFound(_)) => return Ok(Vec::new()),
            Err(e) => return Err(e),
        };
        
        let mut content = String::new();
        reader.read_to_string(&mut content)?;
        
        let mut entries = Vec::new();
        
        for line in content.lines() {
            if line.trim().is_empty() {
                continue;
            }
            
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() < 3 {
                return Err(BagItError::InvalidManifest(
                    format!("Invalid fetch.txt line: {}", line)
                ));
            }
            
            let url = parts[0].to_string();
            let length = if parts[1] == "-" {
                None
            } else {
                Some(parts[1].parse::<u64>()
                    .map_err(|_| BagItError::InvalidManifest(
                        format!("Invalid length in fetch.txt: {}", parts[1])
                    ))?)
            };
            let filepath = decode_filepath(parts[2..].join(" ").as_str());
            
            entries.push(FetchEntry {
                url,
                length,
                filepath,
            });
        }
        
        Ok(entries)
    }
    
    /// Verify a file against its checksum
    pub fn verify_file(&self, path: &str, expected: &str, algorithm: &str) -> Result<()> {
        let mut reader = self.by_name_reader(path)?;
        let mut content = Vec::new();
        reader.read_to_end(&mut content)?;
        
        let actual = compute_checksum(&content, algorithm)?;
        
        if actual.to_lowercase() != expected.to_lowercase() {
            return Err(BagItError::ChecksumMismatch {
                path: path.to_string(),
                expected: expected.to_string(),
                actual,
            });
        }
        
        Ok(())
    }
    
    /// Verify all payload files in a manifest
    pub fn verify_manifest(&self, algorithm: &str) -> Result<()> {
        let entries = self.manifest(algorithm)?;
        
        for entry in entries {
            self.verify_file(&entry.filepath, &entry.checksum, algorithm)?;
        }
        
        Ok(())
    }
    
    /// Verify all tag files in a tag manifest
    pub fn verify_tag_manifest(&self, algorithm: &str) -> Result<()> {
        let entries = self.tag_manifest(algorithm)?;
        
        for entry in entries {
            self.verify_file(&entry.filepath, &entry.checksum, algorithm)?;
        }
        
        Ok(())
    }
    
    /// Check if the bag is complete according to BagIt 1.0 spec
    pub fn is_complete(&self) -> Result<bool> {
        // Check required files
        if self.by_name("bagit.txt").is_err() {
            return Ok(false);
        }
        
        // Must have at least one payload manifest
        let algorithms = self.manifest_algorithms();
        if algorithms.is_empty() {
            return Ok(false);
        }
        
        // All payload files must be in all manifests
        let mut all_payload_files = None;
        
        for algo in &algorithms {
            let entries = self.manifest(algo)?;
            let files: std::collections::HashSet<_> = 
                entries.iter().map(|e| e.filepath.clone()).collect();
            
            match &all_payload_files {
                None => all_payload_files = Some(files),
                Some(existing) => {
                    if existing != &files {
                        return Ok(false);
                    }
                }
            }
        }
        
        Ok(true)
    }
    
    /// Validate the entire bag
    pub fn validate(&self) -> Result<()> {
        // Parse declaration
        let _declaration = self.declaration()?;
        
        // Verify all manifests
        let algorithms = self.manifest_algorithms();
        if algorithms.is_empty() {
            return Err(BagItError::MissingFile("No manifest files found".into()));
        }
        
        for algo in &algorithms {
            self.verify_manifest(algo)?;
        }
        
        // Verify tag manifests if present
        let tag_algorithms = self.tag_manifest_algorithms();
        for algo in &tag_algorithms {
            self.verify_tag_manifest(algo)?;
        }
        
        Ok(())
    }
    
    /// Detect available manifest algorithms
    pub fn manifest_algorithms(&self) -> Vec<String> {
        let mut algorithms = Vec::new();
        
        for name in self.file_names() {
            if name.starts_with("manifest-") && name.ends_with(".txt") {
                if let Some(algo) = name.strip_prefix("manifest-").and_then(|s| s.strip_suffix(".txt")) {
                    algorithms.push(algo.to_string());
                }
            }
        }
        
        algorithms
    }
    
    /// Detect available tag manifest algorithms
    pub fn tag_manifest_algorithms(&self) -> Vec<String> {
        let mut algorithms = Vec::new();
        
        for name in self.file_names() {
            if name.starts_with("tagmanifest-") && name.ends_with(".txt") {
                if let Some(algo) = name.strip_prefix("tagmanifest-").and_then(|s| s.strip_suffix(".txt")) {
                    algorithms.push(algo.to_string());
                }
            }
        }
        
        algorithms
    }
}

impl BagArchive<std::fs::File> {
    /// Open a BagIt archive from a directory path
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        use std::fs;
        
        let base_path = path.as_ref();
        let mut files_map = HashMap::new();
        
        fn visit_dirs(dir: &Path, base: &Path, files: &mut HashMap<String, Vec<u8>>) -> io::Result<()> {
            if dir.is_dir() {
                for entry in fs::read_dir(dir)? {
                    let entry = entry?;
                    let path = entry.path();
                    
                    if path.is_dir() {
                        visit_dirs(&path, base, files)?;
                    } else {
                        let relative = path.strip_prefix(base)
                            .unwrap()
                            .to_string_lossy()
                            .replace('\\', "/");
                        
                        let content = fs::read(&path)?;
                        files.insert(relative, content);
                    }
                }
            }
            Ok(())
        }
        
        visit_dirs(base_path, base_path, &mut files_map)?;
        
        Ok(Self::from_files(files_map))
    }
}

/// Compute checksum for data using specified algorithm
pub fn compute_checksum(data: &[u8], algorithm: &str) -> Result<String> {
    match algorithm.to_lowercase().as_str() {
        "md5" => {
            let mut hasher = Md5::new();
            hasher.update(data);
            Ok(format!("{:x}", hasher.finalize()))
        }
        "sha1" => {
            let mut hasher = Sha1::new();
            hasher.update(data);
            Ok(format!("{:x}", hasher.finalize()))
        }
        "sha256" => {
            let mut hasher = Sha256::new();
            hasher.update(data);
            Ok(format!("{:x}", hasher.finalize()))
        }
        "sha512" => {
            let mut hasher = Sha512::new();
            hasher.update(data);
            Ok(format!("{:x}", hasher.finalize()))
        }
        _ => Err(BagItError::UnsupportedAlgorithm(algorithm.to_string())),
    }
}

/// Decode percent-encoded filepath
fn decode_filepath(path: &str) -> String {
    let mut result = String::new();
    let mut chars = path.chars().peekable();
    
    while let Some(ch) = chars.next() {
        if ch == '%' {
            let hex: String = chars.by_ref().take(2).collect();
            if let Ok(byte) = u8::from_str_radix(&hex, 16) {
                result.push(byte as char);
            } else {
                result.push(ch);
                result.push_str(&hex);
            }
        } else {
            result.push(ch);
        }
    }
    
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_decode_filepath() {
        assert_eq!(decode_filepath("data/file.txt"), "data/file.txt");
        assert_eq!(decode_filepath("data/file%20name.txt"), "data/file name.txt");
        assert_eq!(decode_filepath("data/file%0D%0A.txt"), "data/file\r\n.txt");
    }
    
    #[test]
    fn test_checksum_computation() {
        let data = b"hello world";
        
        let md5 = compute_checksum(data, "md5").unwrap();
        assert_eq!(md5, "5eb63bbbe01eeed093cb22bb8f5acdc3");
        
        let sha256 = compute_checksum(data, "sha256").unwrap();
        assert_eq!(sha256, "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9");
    }
    
    #[test]
    fn test_basic_bag_parsing() {
        let mut files = HashMap::new();
        
        files.insert("bagit.txt".to_string(), 
            b"BagIt-Version: 1.0\nTag-File-Character-Encoding: UTF-8\n".to_vec());
        
        files.insert("manifest-sha256.txt".to_string(),
            b"b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9 data/hello.txt\n".to_vec());
        
        files.insert("data/hello.txt".to_string(), b"hello world".to_vec());
        
        let archive = BagArchive::<std::fs::File>::from_files(files);
        
        assert_eq!(archive.len(), 3);
        
        let declaration = archive.declaration().unwrap();
        assert_eq!(declaration.version, "1.0");
        assert_eq!(declaration.encoding, "UTF-8");
        
        let manifest = archive.manifest("sha256").unwrap();
        assert_eq!(manifest.len(), 1);
        assert_eq!(manifest[0].filepath, "data/hello.txt");
        
        assert!(archive.validate().is_ok());
    }
    
    #[test]
    fn test_file_access() {
        let mut files = HashMap::new();
        
        files.insert("bagit.txt".to_string(), 
            b"BagIt-Version: 1.0\nTag-File-Character-Encoding: UTF-8\n".to_vec());
        
        files.insert("data/test.txt".to_string(), b"test content".to_vec());
        
        let archive = BagArchive::<std::fs::File>::from_files(files);
        
        // Access by name
        let file = archive.by_name("data/test.txt").unwrap();
        assert_eq!(file.name(), "data/test.txt");
        assert_eq!(file.size(), 12);
        assert!(file.is_payload_file());
        
        // Access by index
        let file_by_idx = archive.by_index(1).unwrap();
        assert_eq!(file_by_idx.size(), 12);
        
        // Read file content
        let mut reader = archive.by_name_reader("data/test.txt").unwrap();
        let mut content = String::new();
        reader.read_to_string(&mut content).unwrap();
        assert_eq!(content, "test content");
    }
}
