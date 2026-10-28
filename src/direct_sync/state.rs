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

//! Durable Direct Sync binding, baseline, work and conflict state.

use crate::replica::{Error, Identity, LocalNode, Result};
use rusqlite::{Connection, OpenFlags, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

pub(super) const PROFILE: &str = "yinyang-direct-sync-1";

const TABLES: &[(&str, &[&str])] = &[
    (
        "binding",
        &[
            "singleton",
            "profile",
            "root",
            "root_dev",
            "root_ino",
            "state_dev",
            "state_ino",
            "namespace",
        ],
    ),
    ("baseline", &["path", "remote", "local"]),
    (
        "work",
        &[
            "id",
            "path",
            "operation",
            "state",
            "baseline_remote",
            "baseline_local",
            "desired_local",
            "desired_artifact",
            "error",
        ],
    ),
    (
        "installs",
        &[
            "id",
            "path",
            "operation",
            "state",
            "expected_local",
            "desired_local",
            "desired_remote",
            "temporary",
            "artifact",
            "error",
        ],
    ),
    (
        "artifacts",
        &[
            "id", "origin", "kind", "digest", "length", "phase", "name", "error",
        ],
    ),
    (
        "conflicts",
        &[
            "id",
            "path",
            "local",
            "remote",
            "local_artifact",
            "remote_artifact",
            "error",
        ],
    ),
];

const CREATE_SCHEMA: &str = "
    CREATE TABLE binding(
      singleton INTEGER PRIMARY KEY CHECK(singleton=1),
      profile TEXT NOT NULL,
      root TEXT NOT NULL,
      root_dev BLOB NOT NULL,
      root_ino BLOB NOT NULL,
      state_dev BLOB NOT NULL,
      state_ino BLOB NOT NULL,
      namespace TEXT NOT NULL
    );
    CREATE TABLE baseline(
      path TEXT PRIMARY KEY,
      remote BLOB NOT NULL,
      local BLOB NOT NULL
    );
    CREATE TABLE work(
      id BLOB PRIMARY KEY,
      path TEXT NOT NULL,
      operation TEXT NOT NULL,
      state TEXT NOT NULL,
      baseline_remote BLOB,
      baseline_local BLOB,
      desired_local BLOB,
      desired_artifact TEXT,
      error TEXT
    );
    CREATE INDEX work_path ON work(path);
    CREATE TABLE installs(
      id TEXT PRIMARY KEY,
      path TEXT NOT NULL,
      operation TEXT NOT NULL,
      state TEXT NOT NULL,
      expected_local BLOB,
      desired_local BLOB,
      desired_remote BLOB,
      temporary TEXT NOT NULL,
      artifact TEXT,
      error TEXT
    );
    CREATE INDEX installs_path ON installs(path);
    CREATE TABLE artifacts(
      id TEXT PRIMARY KEY,
      origin TEXT NOT NULL,
      kind TEXT NOT NULL,
      digest BLOB NOT NULL,
      length INTEGER NOT NULL,
      phase TEXT NOT NULL,
      name TEXT NOT NULL UNIQUE,
      error TEXT
    );
    CREATE TABLE conflicts(
      id TEXT PRIMARY KEY,
      path TEXT NOT NULL,
      local BLOB NOT NULL,
      remote BLOB NOT NULL,
      local_artifact TEXT,
      remote_artifact TEXT,
      error TEXT NOT NULL
    );
    CREATE INDEX conflicts_path ON conflicts(path);
";

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RemotePin {
    pub version: Option<String>,
    pub etag: Option<String>,
    pub size: u64,
    pub modified: Option<u64>,
}

impl RemotePin {
    pub fn cas_etag(&self) -> Option<&str> {
        self.etag.as_deref()
    }

    /// Match an observation using the same fallback rule as pinned Direct
    /// reads. Version IDs strengthen the pin when both sides expose one;
    /// otherwise ETag, size and known whole-second mtime identify the value.
    pub fn matches(&self, observed: &Self) -> bool {
        if let Some(expected) = &self.version {
            if let Some(observed) = &observed.version {
                return expected == observed;
            }
        }
        let etag_matches = match (&self.etag, &observed.etag) {
            (Some(expected), Some(observed)) => expected == observed,
            (None, None) => true,
            _ => false,
        };
        etag_matches
            && self.size == observed.size
            && match (self.modified, observed.modified) {
                (Some(expected), Some(observed)) => expected == observed,
                _ => true,
            }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct LocalFingerprint {
    pub length: u64,
    pub digest: [u8; 32],
    pub dev: u64,
    pub ino: u64,
    pub parent_dev: u64,
    pub parent_ino: u64,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DesiredFile {
    pub length: u64,
    pub digest: [u8; 32],
}

impl LocalFingerprint {
    pub fn from_node(node: &LocalNode) -> Result<Self> {
        let crate::replica::LocalKind::File { length, digest } = node.kind else {
            return Err(Error::invalid("local fingerprint requires a regular file"));
        };
        let parent = node.parent.ok_or_else(|| {
            Error::invalid(format!("local file {:?} has no parent identity", node.path))
        })?;
        Ok(Self {
            length,
            digest: *digest.as_bytes(),
            dev: node.identity.dev,
            ino: node.identity.ino,
            parent_dev: parent.dev,
            parent_ino: parent.ino,
        })
    }

    pub fn identity(&self) -> Identity {
        Identity {
            dev: self.dev,
            ino: self.ino,
        }
    }

    pub fn same_content(&self, length: u64, digest: [u8; 32]) -> bool {
        self.length == length && self.digest == digest
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BaselineEntry {
    pub path: String,
    pub remote: RemotePin,
    pub local: LocalFingerprint,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum RemoteOperation {
    Publish,
    Delete,
}

impl RemoteOperation {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Publish => "publish",
            Self::Delete => "delete",
        }
    }

    fn parse(value: &str) -> Result<Self> {
        match value {
            "publish" => Ok(Self::Publish),
            "delete" => Ok(Self::Delete),
            _ => Err(Error::corrupt(format!(
                "unknown Direct Sync work operation {value:?}"
            ))),
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum WorkState {
    Prepared,
    Unknown,
    Conflict,
    Failed,
}

impl WorkState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Prepared => "prepared",
            Self::Unknown => "unknown",
            Self::Conflict => "conflict",
            Self::Failed => "failed",
        }
    }

    fn parse(value: &str) -> Result<Self> {
        match value {
            "prepared" => Ok(Self::Prepared),
            "unknown" => Ok(Self::Unknown),
            "conflict" => Ok(Self::Conflict),
            "failed" => Ok(Self::Failed),
            _ => Err(Error::corrupt(format!(
                "unknown Direct Sync work state {value:?}"
            ))),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkRecord {
    pub id: [u8; 16],
    pub path: String,
    pub operation: RemoteOperation,
    pub state: WorkState,
    pub baseline_remote: Option<RemotePin>,
    pub baseline_local: Option<LocalFingerprint>,
    pub desired_local: Option<LocalFingerprint>,
    pub desired_artifact: Option<String>,
    pub error: Option<String>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum InstallOperation {
    CreateFile,
    ReplaceFile,
    DeleteFile,
}

impl InstallOperation {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::CreateFile => "create-file",
            Self::ReplaceFile => "replace-file",
            Self::DeleteFile => "delete-file",
        }
    }

    fn parse(value: &str) -> Result<Self> {
        match value {
            "create-file" => Ok(Self::CreateFile),
            "replace-file" => Ok(Self::ReplaceFile),
            "delete-file" => Ok(Self::DeleteFile),
            _ => Err(Error::corrupt(format!(
                "unknown Direct Sync install operation {value:?}"
            ))),
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum InstallState {
    Prepared,
    Dispatched,
    Applied,
    Conflict,
    Failed,
}

impl InstallState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Prepared => "prepared",
            Self::Dispatched => "dispatched",
            Self::Applied => "applied",
            Self::Conflict => "conflict",
            Self::Failed => "failed",
        }
    }

    fn parse(value: &str) -> Result<Self> {
        match value {
            "prepared" => Ok(Self::Prepared),
            "dispatched" => Ok(Self::Dispatched),
            "applied" => Ok(Self::Applied),
            "conflict" => Ok(Self::Conflict),
            "failed" => Ok(Self::Failed),
            _ => Err(Error::corrupt(format!(
                "unknown Direct Sync install state {value:?}"
            ))),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InstallRecord {
    pub id: String,
    pub path: String,
    pub operation: InstallOperation,
    pub state: InstallState,
    pub expected_local: Option<LocalFingerprint>,
    pub desired_local: Option<DesiredFile>,
    pub desired_remote: Option<RemotePin>,
    pub temporary: String,
    pub artifact: Option<String>,
    pub error: Option<String>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum ArtifactKind {
    Upload,
    LocalConflict,
    RemoteConflict,
    DisplacedLocal,
    Resolution,
}

impl ArtifactKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Upload => "upload",
            Self::LocalConflict => "local-conflict",
            Self::RemoteConflict => "remote-conflict",
            Self::DisplacedLocal => "displaced-local",
            Self::Resolution => "resolution",
        }
    }

    fn parse(value: &str) -> Result<Self> {
        match value {
            "upload" => Ok(Self::Upload),
            "local-conflict" => Ok(Self::LocalConflict),
            "remote-conflict" => Ok(Self::RemoteConflict),
            "displaced-local" => Ok(Self::DisplacedLocal),
            "resolution" => Ok(Self::Resolution),
            _ => Err(Error::corrupt(format!(
                "unknown Direct Sync artifact kind {value:?}"
            ))),
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum ArtifactPhase {
    Preparing,
    Retained,
    Conflict,
    Failed,
}

impl ArtifactPhase {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Preparing => "preparing",
            Self::Retained => "retained",
            Self::Conflict => "conflict",
            Self::Failed => "failed",
        }
    }

    fn parse(value: &str) -> Result<Self> {
        match value {
            "preparing" => Ok(Self::Preparing),
            "retained" => Ok(Self::Retained),
            "conflict" => Ok(Self::Conflict),
            "failed" => Ok(Self::Failed),
            _ => Err(Error::corrupt(format!(
                "unknown Direct Sync artifact phase {value:?}"
            ))),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ArtifactRecord {
    pub id: String,
    pub origin: String,
    pub kind: ArtifactKind,
    pub digest: [u8; 32],
    pub length: u64,
    pub phase: ArtifactPhase,
    pub name: String,
    pub error: Option<String>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum ConflictPresence {
    Present,
    Absent,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum EvidenceKind {
    File,
    Directory,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ConflictSide {
    pub presence: ConflictPresence,
    pub kind: Option<EvidenceKind>,
    pub length: Option<u64>,
    pub digest: Option<[u8; 32]>,
    pub pin: Option<RemotePin>,
}

impl ConflictSide {
    pub fn absent() -> Self {
        Self {
            presence: ConflictPresence::Absent,
            kind: None,
            length: None,
            digest: None,
            pin: None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConflictRecord {
    pub id: String,
    pub path: String,
    pub local: ConflictSide,
    pub remote: ConflictSide,
    pub local_artifact: Option<String>,
    pub remote_artifact: Option<String>,
    pub error: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Binding {
    pub root: PathBuf,
    pub root_identity: Identity,
    pub state_identity: Identity,
    pub namespace: String,
}

pub struct Store {
    connection: Connection,
}

impl Store {
    pub fn create(path: &Path, binding: &Binding) -> Result<Self> {
        if path.exists() {
            return Err(Error::invalid(format!(
                "Direct Sync state database already exists: {}",
                path.display()
            )));
        }
        let connection = Connection::open(path)
            .map_err(|error| Error::corrupt(format!("create Direct Sync database: {error}")))?;
        connection
            .execute_batch(
                "PRAGMA journal_mode=WAL;
                 PRAGMA synchronous=FULL;
                 PRAGMA foreign_keys=ON;",
            )
            .map_err(|error| Error::corrupt(format!("configure Direct Sync database: {error}")))?;
        connection
            .execute_batch(CREATE_SCHEMA)
            .map_err(|error| Error::corrupt(format!("create Direct Sync schema: {error}")))?;
        connection
            .execute(
                "INSERT INTO binding(
                    singleton, profile, root, root_dev, root_ino, state_dev, state_ino, namespace
                 ) VALUES(1, ?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                params![
                    PROFILE,
                    binding
                        .root
                        .to_str()
                        .ok_or_else(|| Error::unsupported("Direct Sync root is not UTF-8"))?,
                    binding.root_identity.dev.to_be_bytes().as_slice(),
                    binding.root_identity.ino.to_be_bytes().as_slice(),
                    binding.state_identity.dev.to_be_bytes().as_slice(),
                    binding.state_identity.ino.to_be_bytes().as_slice(),
                    binding.namespace,
                ],
            )
            .map_err(|error| Error::corrupt(format!("bind Direct Sync state: {error}")))?;
        validate_schema(&connection)?;
        Ok(Self { connection })
    }

    pub fn open(path: &Path) -> Result<Self> {
        let connection = Connection::open(path)
            .map_err(|error| Error::corrupt(format!("open Direct Sync database: {error}")))?;
        validate_schema(&connection)?;
        connection
            .execute_batch(
                "PRAGMA journal_mode=WAL;
                 PRAGMA synchronous=FULL;
                 PRAGMA foreign_keys=ON;",
            )
            .map_err(|error| Error::corrupt(format!("configure Direct Sync database: {error}")))?;
        Ok(Self { connection })
    }

    pub fn open_read_only(path: &Path) -> Result<Self> {
        let connection = Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .map_err(|error| Error::corrupt(format!("inspect Direct Sync database: {error}")))?;
        validate_schema(&connection)?;
        Ok(Self { connection })
    }

    pub fn binding(&self) -> Result<Binding> {
        let row = self
            .connection
            .query_row(
                "SELECT profile, root, root_dev, root_ino, state_dev, state_ino, namespace
                 FROM binding WHERE singleton=1",
                [],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, Vec<u8>>(2)?,
                        row.get::<_, Vec<u8>>(3)?,
                        row.get::<_, Vec<u8>>(4)?,
                        row.get::<_, Vec<u8>>(5)?,
                        row.get::<_, String>(6)?,
                    ))
                },
            )
            .optional()
            .map_err(|error| Error::corrupt(format!("read Direct Sync binding: {error}")))?
            .ok_or_else(|| Error::invalid("Direct Sync state is not initialized"))?;
        if row.0 != PROFILE {
            return Err(Error::corrupt(format!(
                "unsupported Direct Sync profile {:?}",
                row.0
            )));
        }
        Ok(Binding {
            root: PathBuf::from(row.1),
            root_identity: Identity {
                dev: decode_u64(&row.2, "root device")?,
                ino: decode_u64(&row.3, "root inode")?,
            },
            state_identity: Identity {
                dev: decode_u64(&row.4, "state device")?,
                ino: decode_u64(&row.5, "state inode")?,
            },
            namespace: row.6,
        })
    }

    pub fn baseline(&self) -> Result<BTreeMap<String, BaselineEntry>> {
        let mut statement = self
            .connection
            .prepare("SELECT path, remote, local FROM baseline ORDER BY path")
            .map_err(|error| Error::corrupt(format!("read Direct Sync baseline: {error}")))?;
        let rows = statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Vec<u8>>(1)?,
                    row.get::<_, Vec<u8>>(2)?,
                ))
            })
            .map_err(|error| Error::corrupt(format!("read Direct Sync baseline: {error}")))?;
        let mut baseline = BTreeMap::new();
        for row in rows {
            let (path, remote, local) =
                row.map_err(|error| Error::corrupt(format!("read Direct Sync baseline: {error}")))?;
            baseline.insert(
                path.clone(),
                BaselineEntry {
                    path,
                    remote: decode_json(&remote, "baseline remote pin")?,
                    local: decode_json(&local, "baseline local fingerprint")?,
                },
            );
        }
        Ok(baseline)
    }

    pub fn work(&self) -> Result<Vec<WorkRecord>> {
        let mut statement = self
            .connection
            .prepare(
                "SELECT id, path, operation, state, baseline_remote, baseline_local,
                        desired_local, desired_artifact, error
                 FROM work ORDER BY path, id",
            )
            .map_err(|error| Error::corrupt(format!("read Direct Sync work: {error}")))?;
        let rows = statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, Vec<u8>>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, Option<Vec<u8>>>(4)?,
                    row.get::<_, Option<Vec<u8>>>(5)?,
                    row.get::<_, Option<Vec<u8>>>(6)?,
                    row.get::<_, Option<String>>(7)?,
                    row.get::<_, Option<String>>(8)?,
                ))
            })
            .map_err(|error| Error::corrupt(format!("read Direct Sync work: {error}")))?;
        let mut work = Vec::new();
        for row in rows {
            let row =
                row.map_err(|error| Error::corrupt(format!("read Direct Sync work: {error}")))?;
            work.push(WorkRecord {
                id: decode_id(&row.0, "work identity")?,
                path: row.1,
                operation: RemoteOperation::parse(&row.2)?,
                state: WorkState::parse(&row.3)?,
                baseline_remote: decode_optional_json(row.4.as_deref(), "work baseline remote")?,
                baseline_local: decode_optional_json(row.5.as_deref(), "work baseline local")?,
                desired_local: decode_optional_json(row.6.as_deref(), "work desired local")?,
                desired_artifact: row.7,
                error: row.8,
            });
        }
        Ok(work)
    }

    pub fn insert_work(&mut self, record: &WorkRecord) -> Result<()> {
        let existed = self
            .connection
            .query_row(
                "SELECT 1 FROM work WHERE id=?1",
                params![record.id.as_slice()],
                |_| Ok(()),
            )
            .optional()
            .map_err(|error| Error::corrupt(format!("inspect Direct Sync work: {error}")))?
            .is_some();
        if existed {
            return Ok(());
        }
        let path_exists = self
            .connection
            .query_row(
                "SELECT id FROM work WHERE path=?1 LIMIT 1",
                params![record.path],
                |row| row.get::<_, Vec<u8>>(0),
            )
            .optional()
            .map_err(|error| Error::corrupt(format!("inspect Direct Sync work path: {error}")))?;
        if let Some(id) = path_exists {
            return Err(Error::conflict(format!(
                "Direct Sync path {:?} already has unresolved work {}",
                record.path,
                hex(&id)
            )));
        }
        self.connection
            .execute(
                "INSERT INTO work(
                    id, path, operation, state, baseline_remote, baseline_local,
                    desired_local, desired_artifact, error
                 ) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                params![
                    record.id.as_slice(),
                    record.path,
                    record.operation.as_str(),
                    record.state.as_str(),
                    record
                        .baseline_remote
                        .as_ref()
                        .map(encode_json)
                        .transpose()?,
                    record
                        .baseline_local
                        .as_ref()
                        .map(encode_json)
                        .transpose()?,
                    record.desired_local.as_ref().map(encode_json).transpose()?,
                    record.desired_artifact,
                    record.error,
                ],
            )
            .map_err(|error| Error::corrupt(format!("insert Direct Sync work: {error}")))?;
        Ok(())
    }

    pub fn update_work_state(
        &mut self,
        id: [u8; 16],
        state: WorkState,
        error: Option<&str>,
    ) -> Result<()> {
        self.connection
            .execute(
                "UPDATE work SET state=?2, error=?3 WHERE id=?1",
                params![id.as_slice(), state.as_str(), error],
            )
            .map_err(|error| Error::corrupt(format!("update Direct Sync work: {error}")))?;
        Ok(())
    }

    pub fn complete_work(&mut self, id: [u8; 16], baseline: Option<&BaselineEntry>) -> Result<()> {
        let transaction = self
            .connection
            .transaction()
            .map_err(|error| Error::corrupt(format!("begin Direct Sync completion: {error}")))?;
        if let Some(entry) = baseline {
            transaction
                .execute(
                    "INSERT INTO baseline(path, remote, local) VALUES(?1, ?2, ?3)
                     ON CONFLICT(path) DO UPDATE SET remote=excluded.remote, local=excluded.local",
                    params![
                        entry.path,
                        encode_json(&entry.remote)?,
                        encode_json(&entry.local)?,
                    ],
                )
                .map_err(|error| {
                    Error::corrupt(format!("advance Direct Sync baseline: {error}"))
                })?;
        } else {
            transaction
                .execute(
                    "DELETE FROM baseline WHERE path=(SELECT path FROM work WHERE id=?1)",
                    params![id.as_slice()],
                )
                .map_err(|error| Error::corrupt(format!("remove Direct Sync baseline: {error}")))?;
        }
        transaction
            .execute("DELETE FROM work WHERE id=?1", params![id.as_slice()])
            .map_err(|error| Error::corrupt(format!("complete Direct Sync work: {error}")))?;
        transaction
            .commit()
            .map_err(|error| Error::corrupt(format!("commit Direct Sync completion: {error}")))?;
        Ok(())
    }

    pub fn remove_baseline(&mut self, path: &str) -> Result<()> {
        self.connection
            .execute("DELETE FROM baseline WHERE path=?1", params![path])
            .map_err(|error| Error::corrupt(format!("remove Direct Sync baseline: {error}")))?;
        Ok(())
    }

    pub fn remove_work(&mut self, id: [u8; 16]) -> Result<()> {
        self.connection
            .execute("DELETE FROM work WHERE id=?1", params![id.as_slice()])
            .map_err(|error| Error::corrupt(format!("remove Direct Sync work: {error}")))?;
        Ok(())
    }

    pub fn installs(&self) -> Result<Vec<InstallRecord>> {
        let mut statement = self
            .connection
            .prepare(
                "SELECT id, path, operation, state, expected_local, desired_local,
                        desired_remote, temporary, artifact, error
                 FROM installs ORDER BY path, id",
            )
            .map_err(|error| Error::corrupt(format!("read Direct Sync installs: {error}")))?;
        let rows = statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, Option<Vec<u8>>>(4)?,
                    row.get::<_, Option<Vec<u8>>>(5)?,
                    row.get::<_, Option<Vec<u8>>>(6)?,
                    row.get::<_, String>(7)?,
                    row.get::<_, Option<String>>(8)?,
                    row.get::<_, Option<String>>(9)?,
                ))
            })
            .map_err(|error| Error::corrupt(format!("read Direct Sync installs: {error}")))?;
        let mut installs = Vec::new();
        for row in rows {
            let row =
                row.map_err(|error| Error::corrupt(format!("read Direct Sync installs: {error}")))?;
            installs.push(InstallRecord {
                id: row.0,
                path: row.1,
                operation: InstallOperation::parse(&row.2)?,
                state: InstallState::parse(&row.3)?,
                expected_local: decode_optional_json(row.4.as_deref(), "install expected local")?,
                desired_local: decode_optional_json(row.5.as_deref(), "install desired local")?,
                desired_remote: decode_optional_json(row.6.as_deref(), "install desired remote")?,
                temporary: row.7,
                artifact: row.8,
                error: row.9,
            });
        }
        Ok(installs)
    }

    pub fn insert_install(&mut self, record: &InstallRecord) -> Result<()> {
        self.connection
            .execute(
                "INSERT INTO installs(
                    id, path, operation, state, expected_local, desired_local, desired_remote,
                    temporary, artifact, error
                 ) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
                params![
                    record.id,
                    record.path,
                    record.operation.as_str(),
                    record.state.as_str(),
                    record
                        .expected_local
                        .as_ref()
                        .map(encode_json)
                        .transpose()?,
                    record.desired_local.as_ref().map(encode_json).transpose()?,
                    record
                        .desired_remote
                        .as_ref()
                        .map(encode_json)
                        .transpose()?,
                    record.temporary,
                    record.artifact,
                    record.error,
                ],
            )
            .map_err(|error| Error::corrupt(format!("insert Direct Sync install: {error}")))?;
        Ok(())
    }

    pub fn update_install(&mut self, record: &InstallRecord) -> Result<()> {
        self.connection
            .execute(
                "UPDATE installs SET operation=?2, state=?3, expected_local=?4, desired_local=?5,
                    desired_remote=?6, temporary=?7, artifact=?8, error=?9 WHERE id=?1",
                params![
                    record.id,
                    record.operation.as_str(),
                    record.state.as_str(),
                    record
                        .expected_local
                        .as_ref()
                        .map(encode_json)
                        .transpose()?,
                    record.desired_local.as_ref().map(encode_json).transpose()?,
                    record
                        .desired_remote
                        .as_ref()
                        .map(encode_json)
                        .transpose()?,
                    record.temporary,
                    record.artifact,
                    record.error,
                ],
            )
            .map_err(|error| Error::corrupt(format!("update Direct Sync install: {error}")))?;
        Ok(())
    }

    pub fn complete_install(
        &mut self,
        id: &str,
        baseline: Option<&BaselineEntry>,
        path: &str,
    ) -> Result<()> {
        let transaction = self
            .connection
            .transaction()
            .map_err(|error| Error::corrupt(format!("begin install completion: {error}")))?;
        if let Some(entry) = baseline {
            transaction
                .execute(
                    "INSERT INTO baseline(path, remote, local) VALUES(?1, ?2, ?3)
                     ON CONFLICT(path) DO UPDATE SET remote=excluded.remote, local=excluded.local",
                    params![
                        entry.path,
                        encode_json(&entry.remote)?,
                        encode_json(&entry.local)?,
                    ],
                )
                .map_err(|error| {
                    Error::corrupt(format!("advance Direct Sync baseline: {error}"))
                })?;
        } else {
            transaction
                .execute("DELETE FROM baseline WHERE path=?1", params![path])
                .map_err(|error| Error::corrupt(format!("remove Direct Sync baseline: {error}")))?;
        }
        transaction
            .execute("DELETE FROM installs WHERE id=?1", params![id])
            .map_err(|error| Error::corrupt(format!("complete Direct Sync install: {error}")))?;
        transaction
            .commit()
            .map_err(|error| Error::corrupt(format!("commit install completion: {error}")))?;
        Ok(())
    }

    pub fn artifacts(&self) -> Result<Vec<ArtifactRecord>> {
        let mut statement = self
            .connection
            .prepare(
                "SELECT id, origin, kind, digest, length, phase, name, error
                 FROM artifacts ORDER BY id",
            )
            .map_err(|error| Error::corrupt(format!("read Direct Sync artifacts: {error}")))?;
        let rows = statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, Vec<u8>>(3)?,
                    row.get::<_, i64>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, String>(6)?,
                    row.get::<_, Option<String>>(7)?,
                ))
            })
            .map_err(|error| Error::corrupt(format!("read Direct Sync artifacts: {error}")))?;
        let mut artifacts = Vec::new();
        for row in rows {
            let row = row
                .map_err(|error| Error::corrupt(format!("read Direct Sync artifacts: {error}")))?;
            artifacts.push(ArtifactRecord {
                id: row.0,
                origin: row.1,
                kind: ArtifactKind::parse(&row.2)?,
                digest: decode_digest(&row.3, "artifact digest")?,
                length: u64::try_from(row.4)
                    .map_err(|_| Error::corrupt("negative Direct Sync artifact length"))?,
                phase: ArtifactPhase::parse(&row.5)?,
                name: row.6,
                error: row.7,
            });
        }
        Ok(artifacts)
    }

    pub fn insert_artifact(&mut self, record: &ArtifactRecord) -> Result<()> {
        self.connection
            .execute(
                "INSERT INTO artifacts(id, origin, kind, digest, length, phase, name, error)
                 VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                params![
                    record.id,
                    record.origin,
                    record.kind.as_str(),
                    record.digest.as_slice(),
                    i64::try_from(record.length)
                        .map_err(|_| Error::invalid("artifact length exceeds SQLite range"))?,
                    record.phase.as_str(),
                    record.name,
                    record.error,
                ],
            )
            .map_err(|error| Error::corrupt(format!("insert Direct Sync artifact: {error}")))?;
        Ok(())
    }

    pub fn update_artifact(
        &mut self,
        id: &str,
        phase: ArtifactPhase,
        error: Option<&str>,
    ) -> Result<()> {
        self.connection
            .execute(
                "UPDATE artifacts SET phase=?2, error=?3 WHERE id=?1",
                params![id, phase.as_str(), error],
            )
            .map_err(|error| Error::corrupt(format!("update Direct Sync artifact: {error}")))?;
        Ok(())
    }

    pub fn conflicts(&self) -> Result<Vec<ConflictRecord>> {
        let mut statement = self
            .connection
            .prepare(
                "SELECT id, path, local, remote, local_artifact, remote_artifact, error
                 FROM conflicts ORDER BY path, id",
            )
            .map_err(|error| Error::corrupt(format!("read Direct Sync conflicts: {error}")))?;
        let rows = statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Vec<u8>>(2)?,
                    row.get::<_, Vec<u8>>(3)?,
                    row.get::<_, Option<String>>(4)?,
                    row.get::<_, Option<String>>(5)?,
                    row.get::<_, String>(6)?,
                ))
            })
            .map_err(|error| Error::corrupt(format!("read Direct Sync conflicts: {error}")))?;
        let mut conflicts = Vec::new();
        for row in rows {
            let row = row
                .map_err(|error| Error::corrupt(format!("read Direct Sync conflicts: {error}")))?;
            conflicts.push(ConflictRecord {
                id: row.0,
                path: row.1,
                local: decode_json(&row.2, "conflict local side")?,
                remote: decode_json(&row.3, "conflict remote side")?,
                local_artifact: row.4,
                remote_artifact: row.5,
                error: row.6,
            });
        }
        Ok(conflicts)
    }

    pub fn upsert_conflict(&mut self, record: &ConflictRecord) -> Result<()> {
        self.connection
            .execute(
                "INSERT INTO conflicts(
                    id, path, local, remote, local_artifact, remote_artifact, error
                 ) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7)
                 ON CONFLICT(id) DO UPDATE SET
                    path=excluded.path, local=excluded.local, remote=excluded.remote,
                    local_artifact=COALESCE(conflicts.local_artifact, excluded.local_artifact),
                    remote_artifact=COALESCE(conflicts.remote_artifact, excluded.remote_artifact),
                    error=excluded.error",
                params![
                    record.id,
                    record.path,
                    encode_json(&record.local)?,
                    encode_json(&record.remote)?,
                    record.local_artifact,
                    record.remote_artifact,
                    record.error,
                ],
            )
            .map_err(|error| Error::corrupt(format!("record Direct Sync conflict: {error}")))?;
        Ok(())
    }

    pub fn remove_conflict(&mut self, id: &str) -> Result<()> {
        self.connection
            .execute("DELETE FROM conflicts WHERE id=?1", params![id])
            .map_err(|error| Error::corrupt(format!("remove Direct Sync conflict: {error}")))?;
        Ok(())
    }

    pub fn conflict(&self, id: &str) -> Result<Option<ConflictRecord>> {
        Ok(self
            .conflicts()?
            .into_iter()
            .find(|conflict| conflict.id == id))
    }
}

fn validate_schema(connection: &Connection) -> Result<()> {
    let mut statement = connection
        .prepare(
            "SELECT name FROM sqlite_master
             WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name",
        )
        .map_err(|error| Error::corrupt(format!("inspect Direct Sync schema: {error}")))?;
    let actual_tables = statement
        .query_map([], |row| row.get::<_, String>(0))
        .map_err(|error| Error::corrupt(format!("inspect Direct Sync schema: {error}")))?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|error| Error::corrupt(format!("inspect Direct Sync schema: {error}")))?;
    let mut expected_tables = TABLES
        .iter()
        .map(|(table, _)| (*table).to_owned())
        .collect::<Vec<_>>();
    expected_tables.sort();
    if actual_tables != expected_tables {
        return Err(Error::corrupt(format!(
            "Direct Sync tables are {actual_tables:?}, expected {expected_tables:?}"
        )));
    }
    for (table, expected) in TABLES {
        let mut statement = connection
            .prepare(&format!("PRAGMA table_info({table})"))
            .map_err(|error| Error::corrupt(format!("inspect Direct Sync schema: {error}")))?;
        let columns = statement
            .query_map([], |row| row.get::<_, String>(1))
            .map_err(|error| Error::corrupt(format!("inspect Direct Sync schema: {error}")))?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|error| Error::corrupt(format!("inspect Direct Sync schema: {error}")))?;
        if columns.len() != expected.len()
            || columns
                .iter()
                .zip(expected.iter())
                .any(|(actual, expected)| actual != expected)
        {
            return Err(Error::corrupt(format!(
                "Direct Sync table {table:?} has columns {columns:?}, expected {expected:?}"
            )));
        }
    }
    Ok(())
}

fn encode_json<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    serde_json::to_vec(value)
        .map_err(|error| Error::corrupt(format!("encode Direct Sync state: {error}")))
}

fn decode_json<T: for<'de> Deserialize<'de>>(bytes: &[u8], context: &str) -> Result<T> {
    serde_json::from_slice(bytes)
        .map_err(|error| Error::corrupt(format!("decode {context}: {error}")))
}

fn decode_optional_json<T: for<'de> Deserialize<'de>>(
    bytes: Option<&[u8]>,
    context: &str,
) -> Result<Option<T>> {
    bytes.map(|bytes| decode_json(bytes, context)).transpose()
}

fn decode_u64(bytes: &[u8], context: &str) -> Result<u64> {
    let bytes: [u8; 8] = bytes
        .try_into()
        .map_err(|_| Error::corrupt(format!("{context} has {} bytes, expected 8", bytes.len())))?;
    Ok(u64::from_be_bytes(bytes))
}

fn decode_digest(bytes: &[u8], context: &str) -> Result<[u8; 32]> {
    bytes
        .try_into()
        .map_err(|_| Error::corrupt(format!("{context} has {} bytes, expected 32", bytes.len())))
}

fn decode_id(bytes: &[u8], context: &str) -> Result<[u8; 16]> {
    bytes
        .try_into()
        .map_err(|_| Error::corrupt(format!("{context} has {} bytes, expected 16", bytes.len())))
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}
