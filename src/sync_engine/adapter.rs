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

use super::{Error, ErrorKind, Result};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::os::fd::BorrowedFd;
use std::path::{Path, PathBuf};

pub const SYNC_SCHEMA: &str = "yinyang.sync.status.v1";

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SyncModel {
    Managed,
    Direct,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EntryKind {
    File,
    Directory,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RemotePin {
    Managed {
        node: [u8; 16],
        revision: [u8; 24],
        digest: Option<[u8; 32]>,
        executable: bool,
        directory: bool,
    },
    Direct {
        observation: [u8; 16],
        version: Option<String>,
        etag: Option<String>,
        size: u64,
        modified: Option<u64>,
    },
}

impl RemotePin {
    pub fn matches(&self, observed: &Self) -> bool {
        match (self, observed) {
            (
                Self::Managed {
                    node,
                    digest,
                    executable,
                    directory,
                    ..
                },
                Self::Managed {
                    node: observed_node,
                    digest: observed_digest,
                    executable: observed_executable,
                    directory: observed_directory,
                    ..
                },
            ) => {
                node == observed_node
                    && digest == observed_digest
                    && executable == observed_executable
                    && directory == observed_directory
            }
            (
                Self::Direct {
                    version,
                    etag,
                    size,
                    modified,
                    ..
                },
                Self::Direct {
                    version: observed_version,
                    etag: observed_etag,
                    size: observed_size,
                    modified: observed_modified,
                    ..
                },
            ) => {
                if let (Some(version), Some(observed_version)) = (version, observed_version) {
                    return version == observed_version;
                }
                let etag_matches = match (etag, observed_etag) {
                    (Some(etag), Some(observed_etag)) => etag == observed_etag,
                    (None, None) => true,
                    _ => false,
                };
                etag_matches
                    && size == observed_size
                    && match (modified, observed_modified) {
                        (Some(modified), Some(observed_modified)) => modified == observed_modified,
                        _ => true,
                    }
            }
            _ => false,
        }
    }

    pub const fn stable_id(&self) -> [u8; 16] {
        match self {
            Self::Managed { node, .. } => *node,
            Self::Direct { observation, .. } => *observation,
        }
    }

    pub const fn revision(&self) -> Option<[u8; 24]> {
        match self {
            Self::Managed { revision, .. } => Some(*revision),
            Self::Direct { .. } => None,
        }
    }

    pub const fn is_managed(&self) -> bool {
        matches!(self, Self::Managed { .. })
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RemoteEntry {
    pub path: String,
    pub kind: EntryKind,
    pub length: u64,
    pub digest: Option<[u8; 32]>,
    pub executable: bool,
    pub pin: RemotePin,
}

impl RemoteEntry {
    pub fn same_state(&self, other: &Self) -> bool {
        self.path == other.path
            && self.kind == other.kind
            && self.length == other.length
            && self.digest == other.digest
            && self.executable == other.executable
            && self.pin.matches(&other.pin)
    }
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct RemoteCursor {
    pub bytes: Vec<u8>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RemoteBaseline {
    pub cursor: Option<RemoteCursor>,
    pub entries: BTreeMap<String, RemoteEntry>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RemoteObservation {
    pub cursor: RemoteCursor,
    pub entries: BTreeMap<String, RemoteEntry>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RemoteCapabilities {
    pub directories: bool,
    pub executable: bool,
    pub batch: bool,
    pub rename: bool,
    pub delta: bool,
    pub receipt_lookup: bool,
    pub max_batch: usize,
}

impl RemoteCapabilities {
    pub const fn for_model(model: SyncModel) -> Self {
        match model {
            SyncModel::Managed => Self {
                directories: true,
                executable: true,
                batch: true,
                rename: true,
                delta: true,
                receipt_lookup: true,
                max_batch: 4096,
            },
            SyncModel::Direct => Self {
                directories: false,
                executable: false,
                batch: false,
                rename: false,
                delta: false,
                receipt_lookup: false,
                max_batch: 1,
            },
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum RemoteOperation {
    Content,
    CreateFile,
    CreateDirectory,
    Remove,
    Rename,
    SetExecutable,
}

impl RemoteOperation {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Content => "content",
            Self::CreateFile => "create-file",
            Self::CreateDirectory => "create-directory",
            Self::Remove => "remove",
            Self::Rename => "rename",
            Self::SetExecutable => "set-executable",
        }
    }

    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "content" => Ok(Self::Content),
            "create-file" => Ok(Self::CreateFile),
            "create-directory" => Ok(Self::CreateDirectory),
            "remove" => Ok(Self::Remove),
            "rename" => Ok(Self::Rename),
            "set-executable" => Ok(Self::SetExecutable),
            _ => Err(Error::corrupt(format!("unknown sync operation {value:?}"))),
        }
    }
}

#[derive(Clone, Debug)]
pub enum RemoteRequest {
    CreateFile {
        path: String,
        source: PathBuf,
        executable: bool,
        parent: Option<RemoteEntry>,
    },
    ReplaceFile {
        path: String,
        source: PathBuf,
        baseline: RemoteEntry,
    },
    CreateDirectory {
        path: String,
        parent: RemoteEntry,
    },
    Remove {
        path: String,
        baseline: RemoteEntry,
    },
    Rename {
        from: String,
        to: String,
        baseline: RemoteEntry,
        new_parent: RemoteEntry,
        replace: bool,
    },
    SetExecutable {
        path: String,
        baseline: RemoteEntry,
        executable: bool,
    },
}

impl RemoteRequest {
    pub fn operation(&self) -> RemoteOperation {
        match self {
            Self::CreateFile { .. } => RemoteOperation::CreateFile,
            Self::ReplaceFile { .. } => RemoteOperation::Content,
            Self::CreateDirectory { .. } => RemoteOperation::CreateDirectory,
            Self::Remove { .. } => RemoteOperation::Remove,
            Self::Rename { .. } => RemoteOperation::Rename,
            Self::SetExecutable { .. } => RemoteOperation::SetExecutable,
        }
    }

    pub fn path(&self) -> &str {
        match self {
            Self::CreateFile { path, .. }
            | Self::ReplaceFile { path, .. }
            | Self::CreateDirectory { path, .. }
            | Self::Remove { path, .. }
            | Self::SetExecutable { path, .. } => path,
            Self::Rename { to, .. } => to,
        }
    }

    pub fn original_path(&self) -> Option<&str> {
        match self {
            Self::Rename { from, .. } => Some(from),
            _ => None,
        }
    }

    pub fn desired_path(&self) -> Option<&str> {
        match self {
            Self::Rename { to, .. } => Some(to),
            _ => None,
        }
    }

    pub fn set_source(&mut self, retained: PathBuf) {
        match self {
            Self::CreateFile { source, .. } | Self::ReplaceFile { source, .. } => {
                *source = retained;
            }
            _ => {}
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct FrozenRemoteRequest {
    pub id: [u8; 16],
    pub digest: [u8; 32],
    pub bytes: Vec<u8>,
    pub operation: RemoteOperation,
    pub path: String,
    pub original_path: Option<String>,
    pub desired_path: Option<String>,
}

#[derive(Clone, Debug)]
pub struct PreparedRemoteRequest {
    pub request: FrozenRemoteRequest,
    pub source: Option<PathBuf>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RemoteFailure {
    pub kind: ErrorKind,
    pub message: String,
    pub commit: Option<[u8; 16]>,
}

impl RemoteFailure {
    pub fn from_error(error: &Error) -> Self {
        Self {
            kind: error.kind(),
            message: error.to_string(),
            commit: error.commit_id().map(|commit| *commit.as_bytes()),
        }
    }

    pub fn into_error(self) -> Error {
        match self.commit {
            Some(commit) => Error::unknown(
                Some(yinyang_core::CommitId::from_bytes(commit)),
                self.message,
            ),
            None => Error::new(self.kind, self.message),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RemoteReceipt {
    pub commit: Option<[u8; 16]>,
    pub revision: Option<[u8; 24]>,
    pub ordinal: Option<u32>,
}

#[derive(Clone, Debug)]
pub enum RemoteOutcome {
    Applied {
        accepted: BTreeMap<String, Option<RemoteEntry>>,
        receipt: Option<RemoteReceipt>,
    },
    Conflict {
        observed: Option<RemoteEntry>,
    },
    Unknown {
        failure: RemoteFailure,
    },
    Retryable {
        failure: RemoteFailure,
    },
    Unsupported {
        failure: RemoteFailure,
    },
}

#[allow(async_fn_in_trait)]
pub trait RemoteAdapter: Clone + Send + Sync {
    fn model(&self) -> SyncModel;
    fn namespace_id(&self) -> String;
    fn capabilities(&self) -> RemoteCapabilities;

    async fn preflight(&self, state: &Path) -> Result<()>;

    async fn bind(&self, state: &Path) -> Result<()>;

    async fn observe(&self, baseline: Option<&RemoteBaseline>) -> Result<RemoteObservation>;

    async fn read_file(&self, entry: &RemoteEntry, output: BorrowedFd<'_>) -> Result<()>;

    async fn freeze(&self, request: RemoteRequest) -> Result<FrozenRemoteRequest>;

    async fn receipt(&self, request: &FrozenRemoteRequest) -> Result<Option<RemoteOutcome>>;

    async fn publish_batch(&self, requests: &[PreparedRemoteRequest])
    -> Result<Vec<RemoteOutcome>>;
}

pub fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

pub fn digest(bytes: &[u8]) -> [u8; 32] {
    *blake3::hash(bytes).as_bytes()
}
