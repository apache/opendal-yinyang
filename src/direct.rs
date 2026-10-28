// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Read-only namespace over the objects already present in storage.
//!
//! Direct snapshots do not create Managed metadata, NodeIds, proofs, staging
//! state or transactions. An object key is split into UTF-8 path components
//! and each component receives a bounded reversible representation. Names that
//! cannot be represented by the Linux frontend fail the whole observation
//! instead of being filtered out. File opens pin the observed S3 version ID,
//! or, when versioning is unavailable, use conditional ETag reads for every
//! range so bytes from different object versions are never combined.

use opendal::{EntryMode, ErrorKind as OpenDalErrorKind, Operator};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::Arc;
use std::time::SystemTime;

const NAME_MAX: usize = 255;
const PATH_MAX: usize = 4095;
const INODE_DOMAIN: &[u8] = b"yinyang direct inode v1";
const REVISION_DOMAIN: &[u8] = b"yinyang direct snapshot v1";

/// A Direct namespace error category.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ErrorKind {
    /// The configuration or storage prerequisite is invalid.
    Invalid,
    /// An object key cannot be represented without loss.
    InvalidName,
    /// The requested path does not exist.
    NotFound,
    /// A path component is not a directory.
    NotDirectory,
    /// A directory was used as a file.
    IsDirectory,
    /// The pinned object version is no longer readable.
    Stale,
    /// The storage lacks a required Direct primitive.
    Unsupported,
    /// The storage request failed.
    Storage,
}

/// An error returned by the Direct namespace.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Error {
    kind: ErrorKind,
    message: String,
}

impl Error {
    pub fn kind(&self) -> ErrorKind {
        self.kind
    }

    fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    fn storage(error: opendal::Error) -> Self {
        Self::new(ErrorKind::Storage, error.to_string())
    }

    fn stale(error: opendal::Error) -> Self {
        Self::new(
            ErrorKind::Stale,
            format!("pinned object read is no longer current: {error}"),
        )
    }
}

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for Error {}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// A stable path identity within a Direct namespace.
///
/// The identity is derived from the reversible path and node kind. Object
/// replacement at the same path retains the identity; the file descriptor
/// separately pins the object version or ETag.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct DirectId([u8; 16]);

impl DirectId {
    pub fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }

    #[cfg(test)]
    pub(crate) const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }
}

/// A digest of one complete Direct namespace observation.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct DirectRevision([u8; 32]);

impl DirectRevision {
    pub fn to_bytes(self) -> [u8; 32] {
        self.0
    }

    pub fn to_hex(self) -> String {
        self.0.iter().map(|byte| format!("{byte:02x}")).collect()
    }
}

/// The kind of a node in a Direct snapshot.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DirectKind {
    File,
    Directory,
}

/// One immutable node observation in a Direct snapshot.
#[derive(Clone, Debug)]
pub struct DirectNode {
    id: DirectId,
    parent: Option<DirectId>,
    name: String,
    path: String,
    key: Option<String>,
    kind: DirectKind,
    size: u64,
    pin: Option<ObjectPin>,
    modified: Option<SystemTime>,
}

impl DirectNode {
    pub fn id(&self) -> DirectId {
        self.id
    }

    pub fn parent(&self) -> Option<DirectId> {
        self.parent
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    /// The original object key for files; directory nodes synthesize keys.
    pub fn key(&self) -> Option<&str> {
        self.key.as_deref()
    }

    pub fn kind(&self) -> DirectKind {
        self.kind
    }

    pub fn is_file(&self) -> bool {
        self.kind == DirectKind::File
    }

    pub fn is_directory(&self) -> bool {
        self.kind == DirectKind::Directory
    }

    pub fn size(&self) -> u64 {
        self.size
    }

    pub fn modified(&self) -> Option<SystemTime> {
        self.modified
    }
}

/// An operator-bound Direct namespace.
#[derive(Clone, Debug)]
pub struct DirectNamespace {
    operator: Operator,
}

impl DirectNamespace {
    /// Validate the selected backend prerequisites before any namespace read.
    pub fn new(operator: Operator) -> Result<Self> {
        validate_operator(&operator)?;
        Ok(Self { operator })
    }

    fn unchecked(operator: Operator) -> Self {
        Self { operator }
    }

    pub fn operator(&self) -> &Operator {
        &self.operator
    }

    /// Observe every visible object and synthesize the complete path tree.
    ///
    /// Recursive listing is read-only. The resulting snapshot never writes
    /// storage metadata and must be replaced as a whole by a later observation.
    pub async fn observe(&self) -> Result<DirectSnapshot> {
        let entries = self
            .operator
            .list_with("")
            .recursive(true)
            .await
            .map_err(Error::storage)?;
        let mut records = entries
            .into_iter()
            .map(|entry| {
                let metadata = entry.metadata();
                ObjectRecord {
                    key: entry.path().to_owned(),
                    size: metadata.content_length(),
                    etag: pin_value(metadata.etag()).map(str::to_owned),
                    version: pin_value(metadata.version()).map(str::to_owned),
                    modified: metadata.last_modified().map(SystemTime::from),
                    directory: metadata.mode() == EntryMode::DIR || entry.path().ends_with('/'),
                }
            })
            .collect::<Vec<_>>();
        records.sort_by(|left, right| {
            left.key
                .cmp(&right.key)
                .then(left.directory.cmp(&right.directory))
        });
        DirectSnapshot::build(self.clone(), records)
    }
}

/// Validate the S3-compatible read primitives required by Direct.
pub fn validate_operator(operator: &Operator) -> Result<()> {
    if operator.info().scheme() != "s3" {
        return Err(Error::new(
            ErrorKind::Unsupported,
            format!(
                "Direct volumes require an s3 storage profile, not {}",
                operator.info().scheme()
            ),
        ));
    }
    let capability = operator.info().capability();
    let mut missing = Vec::new();
    if !capability.stat {
        missing.push("stat");
    }
    if !capability.read {
        missing.push("read");
    }
    if !capability.list {
        missing.push("list");
    }
    if !capability.list_with_recursive {
        missing.push("recursive-list");
    }
    if !capability.read_with_version && !capability.read_with_if_match {
        missing.push("version-or-if-match-read");
    }
    if missing.is_empty() {
        Ok(())
    } else {
        Err(Error::new(
            ErrorKind::Unsupported,
            format!(
                "Direct volume storage is missing required capabilities: {}",
                missing.join(", ")
            ),
        ))
    }
}

/// One complete storage-native namespace observation.
#[derive(Clone, Debug)]
pub struct DirectSnapshot {
    namespace: DirectNamespace,
    revision: DirectRevision,
    root: DirectId,
    nodes: BTreeMap<DirectId, Arc<DirectNode>>,
    children: BTreeMap<DirectId, BTreeMap<String, DirectId>>,
    paths: BTreeMap<String, DirectId>,
}

impl DirectSnapshot {
    fn build(namespace: DirectNamespace, records: Vec<ObjectRecord>) -> Result<Self> {
        let root = derive_id("", DirectKind::Directory, None);
        let mut nodes = BTreeMap::new();
        let mut children = BTreeMap::new();
        let mut paths = BTreeMap::new();
        nodes.insert(
            root,
            Arc::new(DirectNode {
                id: root,
                parent: None,
                name: String::new(),
                path: String::new(),
                key: None,
                kind: DirectKind::Directory,
                size: 0,
                pin: None,
                modified: None,
            }),
        );
        children.insert(root, BTreeMap::new());
        paths.insert(String::new(), root);

        let mut seen = BTreeSet::new();
        for record in &records {
            if !seen.insert((record.key.clone(), record.directory)) {
                return Err(Error::new(
                    ErrorKind::Invalid,
                    format!("storage listed object key {:?} more than once", record.key),
                ));
            }
        }
        for record in &records {
            let components = represent_key(record)?;
            let parents = components.len().checked_sub(1).ok_or_else(|| {
                Error::new(
                    ErrorKind::InvalidName,
                    format!(
                        "object key {:?} cannot be represented as a non-root namespace node",
                        record.key
                    ),
                )
            })?;
            let parent = ensure_directory(
                root,
                &components[..parents],
                record,
                &mut nodes,
                &mut children,
                &mut paths,
            )?;
            let path = components.join("/");
            let kind = if record.directory {
                DirectKind::Directory
            } else {
                DirectKind::File
            };
            if let Some(existing) = paths.get(&path).copied() {
                let node = nodes.get(&existing).expect("a path always has a node");
                return Err(Error::new(
                    ErrorKind::Invalid,
                    format!(
                        "path collision for object key {:?}: {} conflicts with {}",
                        record.key,
                        path,
                        if node.is_file() {
                            format!("file object key {:?}", node.key.as_deref().unwrap_or(""))
                        } else {
                            "a directory or object prefix".to_owned()
                        }
                    ),
                ));
            }
            let pin = if record.directory {
                None
            } else {
                Some(ObjectPin::from_record(record).ok_or_else(|| {
                    Error::new(
                        ErrorKind::Unsupported,
                        format!(
                            "object {:?} exposes neither a version ID nor an ETag; Direct cannot pin it",
                            record.key
                        ),
                    )
                })?)
            };
            let id = derive_id(&path, kind, pin.as_ref());
            let name = components
                .last()
                .cloned()
                .expect("a non-root path has a name");
            let node = Arc::new(DirectNode {
                id,
                parent: Some(parent),
                name: name.clone(),
                path: path.clone(),
                key: (!record.directory).then(|| record.key.clone()),
                kind,
                size: if record.directory { 0 } else { record.size },
                pin,
                modified: record.modified,
            });
            nodes.insert(id, node);
            children
                .get_mut(&parent)
                .expect("the parent is an observed directory")
                .insert(name, id);
            children.insert(id, BTreeMap::new());
            paths.insert(path, id);
        }

        let mut hasher = blake3::Hasher::new();
        hasher.update(REVISION_DOMAIN);
        for record in &records {
            hash_record(&mut hasher, record);
        }
        let mut revision = [0_u8; 32];
        revision.copy_from_slice(hasher.finalize().as_bytes());
        Ok(Self {
            namespace,
            revision: DirectRevision(revision),
            root,
            nodes,
            children,
            paths,
        })
    }

    pub fn revision(&self) -> DirectRevision {
        self.revision
    }

    pub fn root(&self) -> DirectId {
        self.root
    }

    pub fn root_node(&self) -> &DirectNode {
        self.node(self.root).expect("the root always exists")
    }

    pub fn node(&self, id: DirectId) -> Option<&DirectNode> {
        self.nodes.get(&id).map(AsRef::as_ref)
    }

    pub fn resolve(&self, path: &str) -> Option<&DirectNode> {
        let path = path.trim_matches('/');
        self.paths
            .get(path)
            .and_then(|id| self.nodes.get(id))
            .map(AsRef::as_ref)
    }

    pub fn lookup(&self, parent: DirectId, name: &str) -> Result<Option<&DirectNode>> {
        let parent = self.node(parent).ok_or_else(|| {
            Error::new(
                ErrorKind::NotFound,
                "parent directory is not in this snapshot",
            )
        })?;
        if !parent.is_directory() {
            return Err(Error::new(
                ErrorKind::NotDirectory,
                format!("{:?} is not a directory", parent.path),
            ));
        }
        Ok(self
            .children
            .get(&parent.id)
            .and_then(|entries| entries.get(name))
            .and_then(|id| self.nodes.get(id))
            .map(AsRef::as_ref))
    }

    pub fn read_dir(&self, directory: DirectId) -> Result<Vec<&DirectNode>> {
        let directory = self
            .node(directory)
            .ok_or_else(|| Error::new(ErrorKind::NotFound, "directory is not in this snapshot"))?;
        if !directory.is_directory() {
            return Err(Error::new(
                ErrorKind::NotDirectory,
                format!("{:?} is not a directory", directory.path),
            ));
        }
        Ok(self
            .children
            .get(&directory.id)
            .into_iter()
            .flat_map(|entries| entries.values())
            .filter_map(|id| self.nodes.get(id))
            .map(AsRef::as_ref)
            .collect())
    }

    /// Open and pin the current object version at an observed file path.
    pub async fn open_file(&self, id: DirectId) -> Result<DirectFile> {
        let node = self
            .node(id)
            .ok_or_else(|| Error::new(ErrorKind::NotFound, "file is not in this snapshot"))?;
        if !node.is_file() {
            return Err(Error::new(
                ErrorKind::IsDirectory,
                format!("{:?} is a directory", node.path),
            ));
        }
        let key = node.key.clone().expect("a file node has an object key");
        let metadata =
            self.namespace
                .operator
                .stat(&key)
                .await
                .map_err(|error| match error.kind() {
                    OpenDalErrorKind::NotFound => Error::new(
                        ErrorKind::NotFound,
                        format!("object {key:?} no longer exists"),
                    ),
                    OpenDalErrorKind::IsADirectory => Error::new(
                        ErrorKind::IsDirectory,
                        format!("object {key:?} is now a directory"),
                    ),
                    _ => Error::storage(error),
                })?;
        if metadata.mode() == EntryMode::DIR {
            return Err(Error::new(
                ErrorKind::IsDirectory,
                format!("object {key:?} is now a directory"),
            ));
        }
        let observed = ObjectPin {
            version: pin_value(metadata.version()).map(str::to_owned),
            etag: pin_value(metadata.etag()).map(str::to_owned),
            size: metadata.content_length(),
            modified: metadata.last_modified().map(SystemTime::from),
        };
        if observed.version.is_none() && observed.etag.is_none() {
            return Err(Error::new(
                ErrorKind::Unsupported,
                format!(
                    "object {key:?} exposes neither a version ID nor an ETag; cannot pin read ranges"
                ),
            ));
        }
        let expected = node.pin.as_ref().expect("a file node has a pin");
        if !expected.matches(&observed) {
            return Err(Error::new(
                ErrorKind::Stale,
                format!(
                    "object {key:?} changed after this namespace observation; refresh the namespace before opening it"
                ),
            ));
        }
        Ok(DirectFile {
            operator: self.namespace.operator.clone(),
            id,
            key,
            length: metadata.content_length(),
            version: observed.version,
            etag: observed.etag,
            modified: metadata.last_modified().map(SystemTime::from),
        })
    }
}

/// One open Direct file with a storage-native version or ETag pin.
#[derive(Clone, Debug)]
pub struct DirectFile {
    operator: Operator,
    id: DirectId,
    key: String,
    length: u64,
    version: Option<String>,
    etag: Option<String>,
    modified: Option<SystemTime>,
}

impl DirectFile {
    #[cfg(test)]
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn for_test(
        operator: Operator,
        id: DirectId,
        key: String,
        length: u64,
        version: Option<String>,
        etag: Option<String>,
        modified: Option<SystemTime>,
    ) -> Self {
        Self {
            operator,
            id,
            key,
            length,
            version,
            etag,
            modified,
        }
    }

    pub fn id(&self) -> DirectId {
        self.id
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn length(&self) -> u64 {
        self.length
    }

    pub fn version(&self) -> Option<&str> {
        self.version.as_deref()
    }

    pub fn etag(&self) -> Option<&str> {
        self.etag.as_deref()
    }

    pub fn modified(&self) -> Option<SystemTime> {
        self.modified
    }

    /// Read exactly one contiguous range from the pinned object version.
    ///
    /// A missing pinned version, a changed ETag or a short response is an
    /// explicit [`ErrorKind::Stale`] error.
    pub async fn read(&self, offset: u64, length: usize) -> Result<Vec<u8>> {
        if offset >= self.length || length == 0 {
            return Ok(Vec::new());
        }
        let end = offset.saturating_add(length as u64).min(self.length);
        let mut read = self.operator.read_with(&self.key).range(offset..end);
        if let Some(version) = &self.version {
            read = read.version(version);
        }
        if let Some(etag) = &self.etag {
            read = read.if_match(etag);
        }
        let bytes = read.await.map_err(|error| match error.kind() {
            OpenDalErrorKind::ConditionNotMatch | OpenDalErrorKind::NotFound => Error::stale(error),
            _ => Error::storage(error),
        })?;
        let bytes = bytes.to_vec();
        let expected = (end - offset) as usize;
        if bytes.len() != expected {
            return Err(Error::new(
                ErrorKind::Stale,
                format!(
                    "pinned object {:?} returned {} of {expected} bytes for range {offset}..{end}",
                    self.key,
                    bytes.len()
                ),
            ));
        }
        Ok(bytes)
    }
}

#[derive(Clone, Debug)]
struct ObjectRecord {
    key: String,
    size: u64,
    etag: Option<String>,
    version: Option<String>,
    directory: bool,
    modified: Option<SystemTime>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ObjectPin {
    version: Option<String>,
    etag: Option<String>,
    size: u64,
    modified: Option<SystemTime>,
}

impl ObjectPin {
    fn from_record(record: &ObjectRecord) -> Option<Self> {
        let pin = Self {
            version: pin_value(record.version.as_deref()).map(str::to_owned),
            etag: pin_value(record.etag.as_deref()).map(str::to_owned),
            size: record.size,
            modified: record.modified,
        };
        (pin.version.is_some() || pin.etag.is_some()).then_some(pin)
    }

    fn matches(&self, observed: &Self) -> bool {
        match &self.version {
            Some(version) => observed.version.as_deref() == Some(version),
            None => {
                let etag_matches = match &self.etag {
                    Some(etag) => observed.etag.as_deref() == Some(etag),
                    None => false,
                };
                etag_matches
                    && self.size == observed.size
                    && match (self.modified, observed.modified) {
                        (Some(expected), Some(observed)) => expected == observed,
                        _ => true,
                    }
            }
        }
    }
}

fn hash_record(hasher: &mut blake3::Hasher, record: &ObjectRecord) {
    hasher.update(&(record.key.len() as u64).to_be_bytes());
    hasher.update(record.key.as_bytes());
    hasher.update(&[u8::from(record.directory)]);
    hasher.update(&record.size.to_be_bytes());
    for value in [&record.etag, &record.version] {
        match value {
            Some(value) => {
                hasher.update(&[1]);
                hasher.update(&(value.len() as u64).to_be_bytes());
                hasher.update(value.as_bytes());
            }
            None => {
                hasher.update(&[0]);
            }
        }
    }
    hash_time(hasher, record.modified);
}

fn hash_time(hasher: &mut blake3::Hasher, value: Option<SystemTime>) {
    match value.map(|time| time.duration_since(SystemTime::UNIX_EPOCH)) {
        Some(Ok(duration)) => {
            hasher.update(&[1]);
            hasher.update(&duration.as_secs().to_be_bytes());
            hasher.update(&duration.subsec_nanos().to_be_bytes());
        }
        Some(Err(error)) => {
            let duration = error.duration();
            hasher.update(&[2]);
            hasher.update(&duration.as_secs().to_be_bytes());
            hasher.update(&duration.subsec_nanos().to_be_bytes());
        }
        None => {
            hasher.update(&[0]);
        }
    }
}

fn pin_value(value: Option<&str>) -> Option<&str> {
    value.filter(|value| !value.is_empty() && *value != "null")
}

fn represent_key(record: &ObjectRecord) -> Result<Vec<String>> {
    if record.key.is_empty() {
        return Err(Error::new(
            ErrorKind::InvalidName,
            "the bucket root cannot be represented as a file object",
        ));
    }
    let mut raw = record.key.split('/').collect::<Vec<_>>();
    if record.directory {
        if raw.last() != Some(&"") {
            return Err(Error::new(
                ErrorKind::InvalidName,
                format!("directory entry {:?} does not end with '/'", record.key),
            ));
        }
        raw.pop();
        if raw.is_empty() {
            return Err(Error::new(
                ErrorKind::InvalidName,
                "a directory marker at the namespace root cannot be represented",
            ));
        }
        if record.size != 0 {
            return Err(Error::new(
                ErrorKind::Invalid,
                format!(
                    "directory marker {:?} contains {} bytes; Direct will not hide object data",
                    record.key, record.size
                ),
            ));
        }
    }
    if raw.iter().any(|component| component.is_empty()) {
        return Err(Error::new(
            ErrorKind::InvalidName,
            format!(
                "object key {:?} contains an empty path component",
                record.key
            ),
        ));
    }
    let mut encoded = Vec::with_capacity(raw.len());
    for component in raw {
        encoded.push(encode_component(component).map_err(|message| {
            Error::new(
                ErrorKind::InvalidName,
                format!(
                    "object key {:?} has component {:?}: {message}",
                    record.key, component
                ),
            )
        })?);
    }
    let path_len = encoded.iter().map(String::len).sum::<usize>() + encoded.len().saturating_sub(1);
    if path_len > PATH_MAX {
        return Err(Error::new(
            ErrorKind::InvalidName,
            format!(
                "object key {:?} maps to a {path_len}-byte path above the {PATH_MAX}-byte FUSE limit",
                record.key
            ),
        ));
    }
    Ok(encoded)
}

/// Encode one raw key component into one bounded Linux path component.
///
/// ASCII bytes outside controls, `%`, and the exact names `.`/`..` are
/// preserved. Non-ASCII UTF-8 bytes are preserved without case folding or
/// Unicode normalization. `%HH` encoding makes the mapping reversible.
pub fn encode_component(component: &str) -> std::result::Result<String, String> {
    if component.is_empty() {
        return Err("empty components cannot be represented".to_owned());
    }
    let dot = component == "." || component == "..";
    let mut encoded = Vec::with_capacity(component.len());
    for byte in component.bytes() {
        if byte == b'%' || byte < 0x20 || byte == 0x7f || dot {
            let escape = format!("%{byte:02X}");
            encoded.extend_from_slice(escape.as_bytes());
        } else {
            encoded.push(byte);
        }
    }
    let encoded = String::from_utf8(encoded).expect("valid UTF-8 bytes and ASCII escapes");
    if encoded.len() > NAME_MAX {
        return Err(format!(
            "encoded name is {} bytes, above NAME_MAX ({NAME_MAX})",
            encoded.len()
        ));
    }
    Ok(encoded)
}

/// Decode a component produced by [`encode_component`].
pub fn decode_component(component: &str) -> std::result::Result<String, String> {
    let bytes = component.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] != b'%' {
            decoded.push(bytes[index]);
            index += 1;
            continue;
        }
        if index + 2 >= bytes.len() {
            return Err("truncated percent escape".to_owned());
        }
        let high = hex(bytes[index + 1])?;
        let low = hex(bytes[index + 2])?;
        decoded.push((high << 4) | low);
        index += 3;
    }
    String::from_utf8(decoded).map_err(|_| "decoded component is not UTF-8".to_owned())
}

fn hex(byte: u8) -> std::result::Result<u8, String> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        _ => Err(format!("invalid percent escape byte {byte:#04x}")),
    }
}

fn ensure_directory(
    root: DirectId,
    components: &[String],
    record: &ObjectRecord,
    nodes: &mut BTreeMap<DirectId, Arc<DirectNode>>,
    children: &mut BTreeMap<DirectId, BTreeMap<String, DirectId>>,
    paths: &mut BTreeMap<String, DirectId>,
) -> Result<DirectId> {
    let mut parent = root;
    let mut path = String::new();
    for component in components {
        if !path.is_empty() {
            path.push('/');
        }
        path.push_str(component);
        if let Some(id) = paths.get(&path).copied() {
            let node = nodes.get(&id).expect("a path always has a node");
            if node.is_file() {
                return Err(Error::new(
                    ErrorKind::Invalid,
                    format!(
                        "file-prefix collision: object key {:?} requires directory {:?}, but file object key {:?} already owns that path",
                        record.key,
                        path,
                        node.key.as_deref().unwrap_or("")
                    ),
                ));
            }
            parent = id;
            continue;
        }
        let id = derive_id(&path, DirectKind::Directory, None);
        nodes.insert(
            id,
            Arc::new(DirectNode {
                id,
                parent: Some(parent),
                name: component.clone(),
                path: path.clone(),
                key: None,
                kind: DirectKind::Directory,
                size: 0,
                pin: None,
                modified: None,
            }),
        );
        children
            .get_mut(&parent)
            .expect("the current parent is a directory")
            .insert(component.clone(), id);
        children.insert(id, BTreeMap::new());
        paths.insert(path.clone(), id);
        parent = id;
    }
    Ok(parent)
}

fn derive_id(path: &str, kind: DirectKind, pin: Option<&ObjectPin>) -> DirectId {
    let mut hasher = blake3::Hasher::new();
    hasher.update(INODE_DOMAIN);
    hasher.update(&[match kind {
        DirectKind::File => 0,
        DirectKind::Directory => 1,
    }]);
    hasher.update(path.as_bytes());
    if let Some(pin) = pin {
        for value in [&pin.version, &pin.etag] {
            match value {
                Some(value) => {
                    hasher.update(&[1]);
                    hasher.update(&(value.len() as u64).to_be_bytes());
                    hasher.update(value.as_bytes());
                }
                None => {
                    hasher.update(&[0]);
                }
            }
        }
        hasher.update(&pin.size.to_be_bytes());
        hash_time(&mut hasher, pin.modified);
    }
    let mut id = [0_u8; 16];
    id.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
    DirectId(id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use opendal::services::Memory;

    fn namespace() -> DirectNamespace {
        let operator = Operator::new(Memory::default()).unwrap();
        DirectNamespace::unchecked(operator)
    }

    fn record(key: &str, directory: bool) -> ObjectRecord {
        ObjectRecord {
            key: key.to_owned(),
            size: 0,
            etag: Some("\"etag\"".to_owned()),
            version: None,
            directory,
            modified: None,
        }
    }

    #[test]
    fn component_mapping_is_reversible() {
        for value in [
            "plain.txt",
            "100%",
            ".",
            "..",
            "line\nbreak",
            "café",
            "e\u{301}",
            "Case",
        ] {
            let encoded = encode_component(value).unwrap();
            assert_eq!(decode_component(&encoded).unwrap(), value);
        }
        assert_eq!(encode_component("a%b").unwrap(), "a%25b");
        assert_eq!(encode_component(".").unwrap(), "%2E");
        assert_eq!(encode_component("..").unwrap(), "%2E%2E");
        assert_ne!(
            encode_component("café").unwrap(),
            encode_component("cafe\u{301}").unwrap()
        );
    }

    #[test]
    fn snapshot_preserves_case_and_normalization_distinctions() {
        let snapshot = DirectSnapshot::build(
            namespace(),
            vec![
                record("a", false),
                record("A", false),
                record("café", false),
                record("cafe\u{301}", false),
            ],
        )
        .unwrap();
        let names = snapshot
            .read_dir(snapshot.root())
            .unwrap()
            .into_iter()
            .map(|node| node.name().to_owned())
            .collect::<BTreeSet<_>>();
        assert_eq!(names.len(), 4);
        assert!(names.contains("a"));
        assert!(names.contains("A"));
    }

    #[test]
    fn file_prefix_collision_is_explicit() {
        let error = DirectSnapshot::build(
            namespace(),
            vec![record("file", false), record("file/child", false)],
        )
        .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Invalid);
        assert!(error.to_string().contains("file-prefix collision"));
        assert!(error.to_string().contains("\"file\""));
    }

    #[test]
    fn empty_component_and_name_limit_are_explicit() {
        let error =
            DirectSnapshot::build(namespace(), vec![record("dir//child", false)]).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidName);
        assert!(error.to_string().contains("dir//child"));

        let name = "x".repeat(NAME_MAX + 1);
        let error = DirectSnapshot::build(namespace(), vec![record(&name, false)]).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidName);
        assert!(error.to_string().contains("NAME_MAX"));
    }

    #[test]
    fn nonempty_directory_marker_is_not_hidden() {
        let mut marker = record("directory/", true);
        marker.size = 3;
        let error = DirectSnapshot::build(namespace(), vec![marker]).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Invalid);
        assert!(error.to_string().contains("will not hide object data"));
    }

    #[test]
    fn null_version_falls_back_to_etag_pinning() {
        let mut record = record("file", false);
        record.version = Some("null".to_owned());
        let pin = ObjectPin::from_record(&record).expect("ETag remains a valid pin");
        assert_eq!(pin.version, None);
        assert_eq!(pin.etag.as_deref(), Some("\"etag\""));

        record.etag = None;
        assert!(
            ObjectPin::from_record(&record).is_none(),
            "the synthetic null version is not an immutable pin"
        );
    }

    #[test]
    fn etag_pin_covers_size_and_available_modified_time() {
        let modified = SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(123);
        let mut record = record("file", false);
        record.size = 9;
        record.modified = Some(modified);
        let pin = ObjectPin::from_record(&record).unwrap();
        assert!(pin.matches(&pin));

        let longer = ObjectPin {
            size: 10,
            ..pin.clone()
        };
        assert!(!pin.matches(&longer));

        let newer = ObjectPin {
            modified: Some(modified + std::time::Duration::from_secs(1)),
            ..pin.clone()
        };
        assert!(!pin.matches(&newer));
    }

    #[test]
    fn list_etag_pin_accepts_head_version_and_strengthens_reads() {
        let modified = SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(123);
        let mut record = record("file", false);
        record.version = None;
        record.size = 9;
        record.modified = Some(modified);
        let list_pin = ObjectPin::from_record(&record).unwrap();
        let head_pin = ObjectPin {
            version: Some("real-version-id".to_owned()),
            etag: list_pin.etag.clone(),
            size: list_pin.size,
            modified: list_pin.modified,
        };
        assert!(
            list_pin.matches(&head_pin),
            "ListObjectsV2 may omit version IDs while HEAD returns one"
        );
    }

    #[test]
    fn file_and_directory_identity_are_stable_across_snapshots() {
        let first = DirectSnapshot::build(namespace(), vec![record("dir/file", false)]).unwrap();
        let file = first.resolve("dir/file").unwrap().id();
        let directory = first.resolve("dir").unwrap().id();
        let second = DirectSnapshot::build(
            namespace(),
            vec![record("dir/file", false), record("other", false)],
        )
        .unwrap();
        assert_eq!(second.resolve("dir/file").unwrap().id(), file);
        assert_eq!(second.resolve("dir").unwrap().id(), directory);
        assert_ne!(second.revision(), first.revision());
    }

    #[test]
    fn replaced_object_gets_a_new_file_identity() {
        let first = DirectSnapshot::build(namespace(), vec![record("file", false)]).unwrap();
        let old = first.resolve("file").unwrap().id();
        let mut replaced = record("file", false);
        replaced.etag = Some("\"replacement\"".to_owned());
        let second = DirectSnapshot::build(namespace(), vec![replaced]).unwrap();
        let fresh = second.resolve("file").unwrap().id();
        assert_ne!(
            fresh, old,
            "replacement must not reuse the inode of a pinned old descriptor"
        );
        assert_eq!(
            first.resolve("file").unwrap().path(),
            second.resolve("file").unwrap().path()
        );
    }
}
