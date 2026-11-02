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

//! The single durable state schema for Managed and Direct Sync.

#[cfg(test)]
use super::ErrorKind;
use super::adapter::{
    EntryKind, RemoteCursor, RemoteEntry, RemoteFailure, RemoteReceipt, SyncModel,
};
use super::types::{ContentHash, Identity, LocalKind, LocalNode, LocalSnapshot, LocalStat};
use super::{Error, Result};
use crate::intent;
use crate::local::store;
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params, params_from_iter};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::io::Read;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};

const TABLES: &[store::TableColumns<'static>] = &[
    (
        "binding",
        &[
            "singleton",
            "profile",
            "model",
            "namespace",
            "namespace_digest",
            "root",
            "root_dev",
            "root_ino",
            "state_dev",
            "state_ino",
        ],
    ),
    ("baseline", &["path", "remote", "local"]),
    ("baseline_meta", &["singleton", "cursor"]),
    (
        "artifacts",
        &[
            "id",
            "origin",
            "name",
            "kind",
            "phase",
            "release_event",
            "remote_revision",
            "digest",
            "length",
            "stat_dev",
            "stat_ino",
            "stat_mtime",
            "stat_mtime_nsec",
            "stat_ctime",
            "stat_ctime_nsec",
            "settle_pending",
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
        "conflicts",
        &[
            "id",
            "path",
            "original_path",
            "desired_path",
            "local",
            "remote",
            "local_artifact",
            "remote_artifact",
            "remote_revision",
            "error",
        ],
    ),
    (intent::TABLE, intent::COLUMNS),
    (
        "work",
        &[
            "seq",
            "id",
            "path",
            "original_path",
            "desired_path",
            "operation",
            "accepted_local",
            "baseline",
            "request_id",
            "source_artifact",
            "conflict",
        ],
    ),
];

pub(super) const PROFILE: store::Profile = store::Profile {
    name: "yinyang-sync-engine-2",
    table: "binding",
    schema: TABLES,
};

const CREATE_SCHEMA: &str = "
    CREATE TABLE binding(
      singleton INTEGER PRIMARY KEY CHECK(singleton=1),
      profile TEXT NOT NULL,
      model TEXT NOT NULL,
      namespace TEXT NOT NULL,
      namespace_digest BLOB NOT NULL,
      root TEXT NOT NULL,
      root_dev BLOB NOT NULL,
      root_ino BLOB NOT NULL,
      state_dev BLOB NOT NULL,
      state_ino BLOB NOT NULL
    );
    CREATE TABLE baseline(
      path TEXT PRIMARY KEY,
      remote BLOB NOT NULL,
      local BLOB
    );
    CREATE TABLE baseline_meta(
      singleton INTEGER PRIMARY KEY CHECK(singleton=1),
      cursor BLOB NOT NULL
    );
    CREATE TABLE artifacts(
      id TEXT PRIMARY KEY,
      origin TEXT NOT NULL,
      name TEXT NOT NULL UNIQUE,
      kind TEXT NOT NULL,
      phase TEXT NOT NULL,
      release_event TEXT NOT NULL,
      remote_revision BLOB,
      digest BLOB,
      length INTEGER NOT NULL,
      stat_dev BLOB,
      stat_ino BLOB,
      stat_mtime INTEGER,
      stat_mtime_nsec INTEGER,
      stat_ctime INTEGER,
      stat_ctime_nsec INTEGER,
      settle_pending INTEGER NOT NULL DEFAULT 0,
      error TEXT
    );
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
    CREATE TABLE conflicts(
      id TEXT PRIMARY KEY,
      path TEXT NOT NULL,
      original_path TEXT,
      desired_path TEXT,
      local BLOB NOT NULL,
      remote BLOB NOT NULL,
      local_artifact TEXT,
      remote_artifact TEXT,
      remote_revision BLOB,
      error TEXT NOT NULL
    );
    CREATE INDEX conflicts_path ON conflicts(path);
    CREATE TABLE work(
      seq INTEGER NOT NULL,
      id TEXT PRIMARY KEY,
      path TEXT NOT NULL,
      original_path TEXT,
      desired_path TEXT,
      operation TEXT NOT NULL,
      accepted_local BLOB,
      baseline BLOB,
      request_id BLOB NOT NULL,
      source_artifact TEXT,
      conflict TEXT
    );
    CREATE INDEX work_path ON work(path);
";

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct Binding {
    pub model: SyncModel,
    pub namespace: String,
    pub namespace_digest: [u8; 32],
    pub root: PathBuf,
    pub root_identity: Identity,
    pub state_identity: Identity,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct ArtifactStat {
    pub dev: u64,
    pub ino: u64,
    pub mtime: i64,
    pub mtime_nsec: i64,
    pub ctime: i64,
    pub ctime_nsec: i64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct LocalFingerprint {
    pub length: u64,
    pub digest: [u8; 32],
    #[serde(default)]
    pub remote_digest: Option<[u8; 32]>,
    pub executable: bool,
    pub dev: u64,
    pub ino: u64,
    pub parent_dev: u64,
    pub parent_ino: u64,
    pub stat: Option<ArtifactStat>,
    pub recorded_ns: i64,
}

impl LocalFingerprint {
    pub fn from_node(node: &LocalNode) -> Result<Self> {
        let LocalKind::File { length, digest } = node.kind else {
            return Err(Error::invalid("local fingerprint requires a regular file"));
        };
        let parent = node.parent.ok_or_else(|| {
            Error::invalid(format!("local file {:?} has no parent identity", node.path))
        })?;
        let stat = node.stat.map(|stat| ArtifactStat {
            dev: stat.dev,
            ino: stat.ino,
            mtime: stat.mtime,
            mtime_nsec: stat.mtime_nsec,
            ctime: stat.ctime,
            ctime_nsec: stat.ctime_nsec,
        });
        Ok(Self {
            length,
            digest: *digest.as_bytes(),
            remote_digest: None,
            executable: node.executable,
            dev: node.identity.dev,
            ino: node.identity.ino,
            parent_dev: parent.dev,
            parent_ino: parent.ino,
            stat,
            recorded_ns: now_ns()?,
        })
    }

    pub const fn identity(&self) -> Identity {
        Identity {
            dev: self.dev,
            ino: self.ino,
        }
    }

    pub fn same_content(&self, length: u64, digest: [u8; 32], executable: bool) -> bool {
        self.length == length && self.digest == digest && self.executable == executable
    }
}

fn pair_local_with_remote(mut local: LocalFingerprint, remote: &RemoteEntry) -> LocalFingerprint {
    local.remote_digest = if remote.kind == EntryKind::File {
        remote.digest
    } else {
        None
    };
    local
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BaselineEntry {
    pub remote: RemoteEntry,
    pub local: Option<LocalFingerprint>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BaselineRecord {
    pub cursor: RemoteCursor,
    pub remote: BTreeMap<String, RemoteEntry>,
    pub local: BTreeMap<String, LocalFingerprint>,
    pub local_snapshot: LocalSnapshot,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ArtifactKind {
    ReplacedFile,
    RollbackDisplaced,
    QuarantinedFile,
    QuarantinedDirectory,
    UploadRetained,
    ConflictLocal,
    ConflictRemote,
    Resolution,
}

impl ArtifactKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ReplacedFile => "replaced-file",
            Self::RollbackDisplaced => "rollback-displaced",
            Self::QuarantinedFile => "quarantined-file",
            Self::QuarantinedDirectory => "quarantined-directory",
            Self::UploadRetained => "upload-retained",
            Self::ConflictLocal => "conflict-local",
            Self::ConflictRemote => "conflict-remote",
            Self::Resolution => "resolution",
        }
    }

    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "replaced-file" => Ok(Self::ReplacedFile),
            "rollback-displaced" => Ok(Self::RollbackDisplaced),
            "quarantined-file" => Ok(Self::QuarantinedFile),
            "quarantined-directory" => Ok(Self::QuarantinedDirectory),
            "upload-retained" => Ok(Self::UploadRetained),
            "conflict-local" => Ok(Self::ConflictLocal),
            "conflict-remote" => Ok(Self::ConflictRemote),
            "resolution" => Ok(Self::Resolution),
            _ => Err(Error::corrupt(format!(
                "unknown retained artifact kind {value:?}"
            ))),
        }
    }

    pub const fn release_event(self) -> ArtifactRelease {
        match self {
            Self::ReplacedFile | Self::RollbackDisplaced => ArtifactRelease::NextReconciliation,
            Self::UploadRetained | Self::Resolution => ArtifactRelease::PublicationDurable,
            Self::QuarantinedFile | Self::QuarantinedDirectory => {
                ArtifactRelease::ReconciliationDurable
            }
            Self::ConflictLocal | Self::ConflictRemote => ArtifactRelease::ExplicitResolution,
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ArtifactPhase {
    Preparing,
    Prepared,
    Moved,
    Retained,
    Conflict,
    Failed,
}

impl ArtifactPhase {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Preparing => "preparing",
            Self::Prepared => "prepared",
            Self::Moved => "moved",
            Self::Retained => "retained",
            Self::Conflict => "conflict",
            Self::Failed => "failed",
        }
    }

    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "preparing" => Ok(Self::Preparing),
            "prepared" => Ok(Self::Prepared),
            "moved" => Ok(Self::Moved),
            "retained" => Ok(Self::Retained),
            "conflict" => Ok(Self::Conflict),
            "failed" => Ok(Self::Failed),
            _ => Err(Error::corrupt(format!("unknown artifact phase {value:?}"))),
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ArtifactRelease {
    PublicationDurable,
    ReconciliationDurable,
    NextReconciliation,
    ExplicitResolution,
}

impl ArtifactRelease {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PublicationDurable => "publication_durable",
            Self::ReconciliationDurable => "reconciliation_durable",
            Self::NextReconciliation => "next_reconciliation",
            Self::ExplicitResolution => "explicit_resolution",
        }
    }

    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "publication_durable" => Ok(Self::PublicationDurable),
            "reconciliation_durable" => Ok(Self::ReconciliationDurable),
            "next_reconciliation" => Ok(Self::NextReconciliation),
            "explicit_resolution" => Ok(Self::ExplicitResolution),
            _ => Err(Error::corrupt(format!(
                "unknown artifact release event {value:?}"
            ))),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ArtifactRecord {
    pub id: String,
    pub origin: String,
    pub name: String,
    pub kind: ArtifactKind,
    pub phase: ArtifactPhase,
    pub release_event: ArtifactRelease,
    pub remote_revision: Option<[u8; 24]>,
    pub digest: Option<[u8; 32]>,
    pub length: u64,
    pub stat: Option<ArtifactStat>,
    pub settle_pending: bool,
    pub error: Option<String>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum InstallOperation {
    Create,
    Replace,
    Delete,
}

impl InstallOperation {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Create => "create",
            Self::Replace => "replace",
            Self::Delete => "delete",
        }
    }

    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "create" => Ok(Self::Create),
            "replace" => Ok(Self::Replace),
            "delete" => Ok(Self::Delete),
            _ => Err(Error::corrupt(format!(
                "unknown install operation {value:?}"
            ))),
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum InstallState {
    Prepared,
    Dispatched,
    Applied,
    Conflict,
    Failed,
}

impl InstallState {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Prepared => "prepared",
            Self::Dispatched => "dispatched",
            Self::Applied => "applied",
            Self::Conflict => "conflict",
            Self::Failed => "failed",
        }
    }

    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "prepared" => Ok(Self::Prepared),
            "dispatched" => Ok(Self::Dispatched),
            "applied" => Ok(Self::Applied),
            "conflict" => Ok(Self::Conflict),
            "failed" => Ok(Self::Failed),
            _ => Err(Error::corrupt(format!("unknown install state {value:?}"))),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct InstallRecord {
    pub id: String,
    pub path: String,
    pub operation: InstallOperation,
    pub state: InstallState,
    pub expected_local: Option<LocalFingerprint>,
    pub desired_local: Option<LocalFingerprint>,
    pub desired_remote: Option<RemoteEntry>,
    pub temporary: String,
    pub artifact: Option<String>,
    pub error: Option<String>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ConflictPresence {
    Present,
    Absent,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum EvidenceKind {
    File,
    Directory,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ConflictSide {
    pub presence: ConflictPresence,
    pub kind: Option<EvidenceKind>,
    pub length: Option<u64>,
    pub digest: Option<[u8; 32]>,
    pub executable: Option<bool>,
    pub remote: Option<RemoteEntry>,
}

impl ConflictSide {
    pub const fn absent() -> Self {
        Self {
            presence: ConflictPresence::Absent,
            kind: None,
            length: None,
            digest: None,
            executable: None,
            remote: None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ConflictRecord {
    pub id: String,
    pub path: String,
    pub original_path: Option<String>,
    pub desired_path: Option<String>,
    pub local: ConflictSide,
    pub remote: ConflictSide,
    pub local_artifact: Option<String>,
    pub remote_artifact: Option<String>,
    pub remote_revision: Option<[u8; 24]>,
    pub error: String,
}

pub(crate) use crate::intent::State as IntentState;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IntentRecord {
    pub id: String,
    pub request_id: [u8; 16],
    pub request_digest: [u8; 32],
    pub request: Vec<u8>,
    pub commit_id: Option<[u8; 16]>,
    pub state: IntentState,
    pub receipt_revision: Option<[u8; 24]>,
    pub receipt_ordinal: Option<u32>,
    pub failure: Option<RemoteFailure>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct WorkRecord {
    pub seq: u64,
    pub id: String,
    pub path: String,
    pub original_path: Option<String>,
    pub desired_path: Option<String>,
    pub operation: super::adapter::RemoteOperation,
    pub accepted_local: Option<LocalFingerprint>,
    pub baseline: Option<RemoteEntry>,
    pub request_id: [u8; 16],
    pub source_artifact: Option<String>,
    pub conflict: Option<String>,
}

pub(crate) struct Store {
    connection: Connection,
}

impl Store {
    pub fn create(path: &Path, binding: &Binding) -> Result<Self> {
        if path.exists() {
            return Err(Error::invalid(format!(
                "sync state database already exists: {}",
                path.display()
            )));
        }
        let mut connection = store::open_read_write(path, true).map_err(store_error)?;
        let transaction = connection
            .transaction()
            .map_err(|error| Error::corrupt(format!("begin sync initialization: {error}")))?;
        store::initialize(
            &transaction,
            &PROFILE,
            CREATE_SCHEMA,
            &binding_values(binding)?,
        )
        .map_err(store_error)?;
        transaction
            .execute_batch(intent::CREATE_TABLE)
            .map_err(|error| Error::corrupt(format!("create shared intent table: {error}")))?;
        transaction
            .commit()
            .map_err(|error| Error::corrupt(format!("commit sync initialization: {error}")))?;
        store::configure(&connection, store::Durability::Journal).map_err(store_error)?;
        validate_schema(&connection)?;
        Ok(Self { connection })
    }

    pub fn open(path: &Path) -> Result<Self> {
        let connection = store::open_read_write(path, false).map_err(store_error)?;
        validate_schema(&connection)?;
        store::configure(&connection, store::Durability::Journal).map_err(store_error)?;
        Ok(Self { connection })
    }

    pub fn open_read_only(path: &Path) -> Result<Self> {
        let connection = store::open_read_only(path).map_err(store_error)?;
        validate_schema(&connection)?;
        Ok(Self { connection })
    }

    pub fn binding(&self) -> Result<Binding> {
        let row = self
            .connection
            .query_row(
                "SELECT model,namespace,namespace_digest,root,root_dev,root_ino,state_dev,state_ino
                 FROM binding WHERE singleton=1",
                [],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, Vec<u8>>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, Vec<u8>>(4)?,
                        row.get::<_, Vec<u8>>(5)?,
                        row.get::<_, Vec<u8>>(6)?,
                        row.get::<_, Vec<u8>>(7)?,
                    ))
                },
            )
            .optional()
            .map_err(|error| Error::corrupt(format!("read sync binding: {error}")))?
            .ok_or_else(|| Error::invalid("sync state is not initialized"))?;
        let model = match row.0.as_str() {
            "managed" => SyncModel::Managed,
            "direct" => SyncModel::Direct,
            other => {
                return Err(Error::corrupt(format!(
                    "unknown sync binding model {other:?}"
                )));
            }
        };
        Ok(Binding {
            model,
            namespace: row.1,
            namespace_digest: decode_fixed(&row.2, "binding namespace digest")?,
            root: PathBuf::from(row.3),
            root_identity: Identity {
                dev: decode_u64(&row.4, "binding root device")?,
                ino: decode_u64(&row.5, "binding root inode")?,
            },
            state_identity: Identity {
                dev: decode_u64(&row.6, "binding state device")?,
                ino: decode_u64(&row.7, "binding state inode")?,
            },
        })
    }

    pub fn cursor(&self) -> Result<Option<RemoteCursor>> {
        let bytes = self
            .connection
            .query_row(
                "SELECT cursor FROM baseline_meta WHERE singleton=1",
                [],
                |row| row.get::<_, Vec<u8>>(0),
            )
            .optional()
            .map_err(|error| Error::corrupt(format!("read sync cursor: {error}")))?;
        Ok(bytes.map(|bytes| RemoteCursor { bytes }))
    }

    pub fn set_cursor(&mut self, cursor: &RemoteCursor) -> Result<()> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| Error::corrupt(format!("begin sync cursor: {error}")))?;
        transaction
            .execute(
                "INSERT OR REPLACE INTO baseline_meta(singleton,cursor) VALUES(1,?1)",
                params![cursor.bytes],
            )
            .map_err(|error| Error::corrupt(format!("write sync cursor: {error}")))?;
        transaction
            .commit()
            .map_err(|error| Error::corrupt(format!("commit sync cursor: {error}")))?;
        Ok(())
    }

    pub fn baseline(&self) -> Result<Option<BaselineRecord>> {
        let cursor = self.cursor()?;
        let Some(cursor) = cursor else {
            return Ok(None);
        };
        let mut statement = self
            .connection
            .prepare("SELECT path,remote,local FROM baseline ORDER BY path")
            .map_err(|error| Error::corrupt(format!("read sync baseline: {error}")))?;
        let rows = statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Vec<u8>>(1)?,
                    row.get::<_, Option<Vec<u8>>>(2)?,
                ))
            })
            .map_err(|error| Error::corrupt(format!("read sync baseline: {error}")))?;
        let mut remote_entries = BTreeMap::new();
        let mut local = BTreeMap::new();
        let mut local_snapshot = BTreeMap::new();
        for row in rows {
            let (path, remote, local_bytes) =
                row.map_err(|error| Error::corrupt(format!("read sync baseline: {error}")))?;
            let remote: RemoteEntry = decode_json(&remote, "baseline remote entry")?;
            if remote.path != path {
                return Err(Error::corrupt(
                    "sync baseline path disagrees with remote entry",
                ));
            }
            remote_entries.insert(path.clone(), remote);
            if let Some(bytes) = local_bytes {
                let fingerprint: LocalFingerprint =
                    decode_json(&bytes, "baseline local fingerprint")?;
                local.insert(path.clone(), fingerprint);
                if let Some(node) = fingerprint_to_node(&path, fingerprint) {
                    local_snapshot.insert(path, node);
                }
            }
        }
        Ok(Some(BaselineRecord {
            cursor,
            remote: remote_entries,
            local,
            local_snapshot: LocalSnapshot::from_nodes(local_snapshot),
        }))
    }

    pub fn save_baseline_entry(
        &self,
        path: &str,
        remote: &RemoteEntry,
        local: Option<LocalFingerprint>,
    ) -> Result<()> {
        let local = local.map(|local| pair_local_with_remote(local, remote));
        self.connection
            .execute(
                "INSERT INTO baseline(path,remote,local) VALUES(?1,?2,?3)
                 ON CONFLICT(path) DO UPDATE SET remote=excluded.remote,local=excluded.local",
                params![
                    path,
                    encode_json(remote)?,
                    local.as_ref().map(encode_json).transpose()?,
                ],
            )
            .map_err(|error| Error::corrupt(format!("write sync baseline entry: {error}")))?;
        Ok(())
    }

    pub fn remove_baseline_path(&self, path: &str) -> Result<()> {
        self.connection
            .execute("DELETE FROM baseline WHERE path=?1", params![path])
            .map_err(|error| Error::corrupt(format!("remove sync baseline entry: {error}")))?;
        Ok(())
    }

    pub fn remove_baseline_subtree(&self, path: &str) -> Result<()> {
        self.connection
            .execute(
                "DELETE FROM baseline
                 WHERE path=?1 OR substr(path,1,length(?1)+1)=?1||'/'",
                params![path],
            )
            .map_err(|error| Error::corrupt(format!("remove sync baseline subtree: {error}")))?;
        Ok(())
    }

    pub fn rename_baseline_subtree(&mut self, from: &str, to: &str) -> Result<()> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| Error::corrupt(format!("begin baseline rename: {error}")))?;
        let rows = {
            let mut statement = transaction
                .prepare("SELECT path,remote,local FROM baseline WHERE path=?1 OR substr(path,1,length(?1)+1)=?1||'/' ORDER BY path")
                .map_err(|error| Error::corrupt(format!("read renamed baseline: {error}")))?;
            statement
                .query_map(params![from], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, Vec<u8>>(1)?,
                        row.get::<_, Option<Vec<u8>>>(2)?,
                    ))
                })
                .map_err(|error| Error::corrupt(format!("read renamed baseline: {error}")))?
                .collect::<rusqlite::Result<Vec<_>>>()
                .map_err(|error| Error::corrupt(format!("read renamed baseline: {error}")))?
        };
        transaction
            .execute(
                "DELETE FROM baseline WHERE path=?1 OR substr(path,1,length(?1)+1)=?1||'/'",
                params![from],
            )
            .map_err(|error| Error::corrupt(format!("remove old baseline subtree: {error}")))?;
        for (path, remote_bytes, local) in rows {
            let destination = if path == from {
                to.to_owned()
            } else {
                format!("{to}{}", &path[from.len()..])
            };
            let mut remote: RemoteEntry = decode_json(&remote_bytes, "renamed baseline remote")?;
            remote.path = destination.clone();
            transaction
                .execute(
                    "INSERT INTO baseline(path,remote,local) VALUES(?1,?2,?3)",
                    params![destination, encode_json(&remote)?, local],
                )
                .map_err(|error| {
                    Error::corrupt(format!("write renamed baseline entry: {error}"))
                })?;
        }
        transaction
            .commit()
            .map_err(|error| Error::corrupt(format!("commit baseline rename: {error}")))?;
        Ok(())
    }

    pub fn save_scan_evidence(&self, nodes: &[&LocalNode]) -> Result<()> {
        let transaction = self
            .connection
            .unchecked_transaction()
            .map_err(|error| Error::corrupt(format!("begin scan evidence: {error}")))?;
        for node in nodes {
            let LocalKind::File { length, digest } = node.kind else {
                continue;
            };
            let existing = transaction
                .query_row(
                    "SELECT local FROM baseline WHERE path=?1 AND local IS NOT NULL",
                    params![node.path],
                    |row| row.get::<_, Vec<u8>>(0),
                )
                .optional()
                .map_err(|error| Error::corrupt(format!("read scan evidence: {error}")))?;
            let Some(existing) = existing else {
                continue;
            };
            let existing: LocalFingerprint = decode_json(&existing, "accepted local fingerprint")?;
            if !existing.same_content(length, *digest.as_bytes(), node.executable) {
                continue;
            }
            let mut fingerprint = LocalFingerprint::from_node(node)?;
            fingerprint.remote_digest = existing.remote_digest;
            transaction
                .execute(
                    "UPDATE baseline SET local=?2 WHERE path=?1",
                    params![node.path, encode_json(&fingerprint)?],
                )
                .map_err(|error| Error::corrupt(format!("write scan evidence: {error}")))?;
        }
        transaction
            .commit()
            .map_err(|error| Error::corrupt(format!("commit scan evidence: {error}")))?;
        Ok(())
    }

    pub fn artifacts(&self) -> Result<Vec<ArtifactRecord>> {
        let mut statement = self
            .connection
            .prepare(
                "SELECT id,origin,name,kind,phase,release_event,remote_revision,digest,length,
                        stat_dev,stat_ino,stat_mtime,stat_mtime_nsec,stat_ctime,stat_ctime_nsec,
                        settle_pending,error
                 FROM artifacts ORDER BY rowid,id",
            )
            .map_err(|error| Error::corrupt(format!("read artifacts: {error}")))?;
        let rows = statement
            .query_map([], artifact_from_row)
            .map_err(|error| Error::corrupt(format!("read artifacts: {error}")))?;
        rows.map(|row| row.map_err(|error| Error::corrupt(format!("read artifact: {error}"))))
            .collect()
    }

    pub fn insert_artifact(&self, artifact: &ArtifactRecord) -> Result<()> {
        self.connection
            .execute(
                "INSERT INTO artifacts(
                    id,origin,name,kind,phase,release_event,remote_revision,digest,length,
                    stat_dev,stat_ino,stat_mtime,stat_mtime_nsec,stat_ctime,stat_ctime_nsec,
                    settle_pending,error
                 ) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17)",
                params_from_iter(artifact_params(artifact)?),
            )
            .map_err(|error| Error::corrupt(format!("insert artifact: {error}")))?;
        Ok(())
    }

    pub fn update_artifact(&self, artifact: &ArtifactRecord) -> Result<()> {
        self.connection
            .execute(
                "UPDATE artifacts SET
                    origin=?2,name=?3,kind=?4,phase=?5,release_event=?6,remote_revision=?7,
                    digest=?8,length=?9,stat_dev=?10,stat_ino=?11,stat_mtime=?12,
                    stat_mtime_nsec=?13,stat_ctime=?14,stat_ctime_nsec=?15,
                    settle_pending=?16,error=?17
                 WHERE id=?1",
                params_from_iter(artifact_params(artifact)?),
            )
            .map_err(|error| Error::corrupt(format!("update artifact: {error}")))?;
        Ok(())
    }

    pub fn remove_artifact(&self, id: &str) -> Result<()> {
        self.connection
            .execute("DELETE FROM artifacts WHERE id=?1", params![id])
            .map_err(|error| Error::corrupt(format!("remove artifact: {error}")))?;
        Ok(())
    }

    pub fn installs(&self) -> Result<Vec<InstallRecord>> {
        let mut statement = self
            .connection
            .prepare(
                "SELECT id,path,operation,state,expected_local,desired_local,desired_remote,
                        temporary,artifact,error
                 FROM installs ORDER BY path,id",
            )
            .map_err(|error| Error::corrupt(format!("read installs: {error}")))?;
        let rows = statement
            .query_map([], install_from_row)
            .map_err(|error| Error::corrupt(format!("read installs: {error}")))?;
        rows.map(|row| row.map_err(|error| Error::corrupt(format!("read install: {error}"))))
            .collect()
    }

    pub fn insert_install(&self, install: &InstallRecord) -> Result<()> {
        self.connection
            .execute(
                "INSERT INTO installs(
                    id,path,operation,state,expected_local,desired_local,desired_remote,
                    temporary,artifact,error
                 ) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",
                params_from_iter(install_params(install)?),
            )
            .map_err(|error| Error::corrupt(format!("insert install: {error}")))?;
        Ok(())
    }

    pub fn update_install(&self, install: &InstallRecord) -> Result<()> {
        self.connection
            .execute(
                "UPDATE installs SET
                    path=?2,operation=?3,state=?4,expected_local=?5,desired_local=?6,
                    desired_remote=?7,temporary=?8,artifact=?9,error=?10
                 WHERE id=?1",
                params_from_iter(install_params(install)?),
            )
            .map_err(|error| Error::corrupt(format!("update install: {error}")))?;
        Ok(())
    }

    pub fn remove_install(&self, id: &str) -> Result<()> {
        self.connection
            .execute("DELETE FROM installs WHERE id=?1", params![id])
            .map_err(|error| Error::corrupt(format!("remove install: {error}")))?;
        Ok(())
    }

    pub fn complete_install(
        &mut self,
        id: &str,
        baseline: Option<(&RemoteEntry, LocalFingerprint, bool)>,
        path: &str,
    ) -> Result<()> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| Error::corrupt(format!("begin install completion: {error}")))?;
        if let Some((remote, local, keep_local)) = baseline {
            if keep_local {
                let local = pair_local_with_remote(local, remote);
                transaction
                    .execute(
                        "INSERT INTO baseline(path,remote,local) VALUES(?1,?2,?3)
                         ON CONFLICT(path) DO UPDATE SET remote=excluded.remote,local=excluded.local",
                        params![path, encode_json(remote)?, encode_json(&local)?],
                    )
                    .map_err(|error| {
                        Error::corrupt(format!("advance install baseline: {error}"))
                    })?;
            } else {
                transaction
                    .execute(
                        "INSERT INTO baseline(path,remote,local) VALUES(?1,?2,NULL)
                         ON CONFLICT(path) DO UPDATE SET remote=excluded.remote,local=NULL",
                        params![path, encode_json(remote)?],
                    )
                    .map_err(|error| Error::corrupt(format!("advance remote baseline: {error}")))?;
            }
        } else {
            transaction
                .execute("DELETE FROM baseline WHERE path=?1", params![path])
                .map_err(|error| Error::corrupt(format!("remove install baseline: {error}")))?;
        }
        transaction
            .execute("DELETE FROM installs WHERE id=?1", params![id])
            .map_err(|error| Error::corrupt(format!("complete install: {error}")))?;
        transaction
            .commit()
            .map_err(|error| Error::corrupt(format!("commit install completion: {error}")))?;
        Ok(())
    }

    pub fn conflicts(&self) -> Result<Vec<ConflictRecord>> {
        let mut statement = self
            .connection
            .prepare(
                "SELECT id,path,original_path,desired_path,local,remote,local_artifact,
                        remote_artifact,remote_revision,error
                 FROM conflicts ORDER BY path,id",
            )
            .map_err(|error| Error::corrupt(format!("read conflicts: {error}")))?;
        let rows = statement
            .query_map([], conflict_from_row)
            .map_err(|error| Error::corrupt(format!("read conflicts: {error}")))?;
        rows.map(|row| row.map_err(|error| Error::corrupt(format!("read conflict: {error}"))))
            .collect()
    }

    pub fn upsert_conflict(&self, conflict: &ConflictRecord) -> Result<()> {
        self.connection
            .execute(
                "INSERT INTO conflicts(
                    id,path,original_path,desired_path,local,remote,local_artifact,
                    remote_artifact,remote_revision,error
                 ) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)
                 ON CONFLICT(id) DO UPDATE SET
                    path=excluded.path,original_path=excluded.original_path,
                    desired_path=excluded.desired_path,local=excluded.local,remote=excluded.remote,
                    local_artifact=COALESCE(conflicts.local_artifact,excluded.local_artifact),
                    remote_artifact=COALESCE(conflicts.remote_artifact,excluded.remote_artifact),
                    remote_revision=excluded.remote_revision,error=excluded.error",
                params_from_iter(conflict_params(conflict)?),
            )
            .map_err(|error| Error::corrupt(format!("record conflict: {error}")))?;
        Ok(())
    }

    pub fn conflict(&self, id: &str) -> Result<Option<ConflictRecord>> {
        Ok(self
            .conflicts()?
            .into_iter()
            .find(|conflict| conflict.id == id))
    }

    pub fn remove_conflict(&self, id: &str) -> Result<()> {
        self.connection
            .execute("DELETE FROM conflicts WHERE id=?1", params![id])
            .map_err(|error| Error::corrupt(format!("remove conflict: {error}")))?;
        Ok(())
    }

    pub fn intents(&self) -> Result<Vec<IntentRecord>> {
        let mut statement = self
            .connection
            .prepare(
                "SELECT id,request_id,request_digest,request,commit_id,state,receipt_revision,
                        receipt_ordinal,failure,metadata
                 FROM intents ORDER BY id",
            )
            .map_err(|error| Error::corrupt(format!("read intents: {error}")))?;
        let rows = statement
            .query_map([], intent_from_row)
            .map_err(|error| Error::corrupt(format!("read intents: {error}")))?;
        rows.map(|row| row.map_err(|error| Error::corrupt(format!("read intent: {error}"))))
            .collect()
    }

    pub fn intent(&self, id: &str) -> Result<Option<IntentRecord>> {
        let mut statement = self
            .connection
            .prepare(
                "SELECT id,request_id,request_digest,request,commit_id,state,receipt_revision,
                        receipt_ordinal,failure,metadata
                 FROM intents WHERE id=?1",
            )
            .map_err(|error| Error::corrupt(format!("read intent: {error}")))?;
        statement
            .query_row(params![id.as_bytes()], intent_from_row)
            .optional()
            .map_err(|error| Error::corrupt(format!("read intent: {error}")))
    }

    pub fn insert_intent(&self, intent: &IntentRecord) -> Result<()> {
        self.connection
            .execute(
                "INSERT INTO intents(
                    id,request_id,request_digest,request,commit_id,state,receipt_revision,
                    receipt_ordinal,failure,metadata
                 ) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",
                params_from_iter(intent_params(intent)?),
            )
            .map_err(|error| Error::corrupt(format!("insert intent: {error}")))?;
        Ok(())
    }

    pub fn update_intent(&self, intent: &IntentRecord) -> Result<()> {
        self.connection
            .execute(
                "UPDATE intents SET
                    request_id=?2,request_digest=?3,request=?4,commit_id=?5,state=?6,
                    receipt_revision=?7,receipt_ordinal=?8,failure=?9,metadata=?10
                 WHERE id=?1",
                params_from_iter(intent_params(intent)?),
            )
            .map_err(|error| Error::corrupt(format!("update intent: {error}")))?;
        Ok(())
    }

    pub fn remove_intent(&self, id: &str) -> Result<()> {
        self.connection
            .execute("DELETE FROM intents WHERE id=?1", params![id.as_bytes()])
            .map_err(|error| Error::corrupt(format!("remove intent: {error}")))?;
        Ok(())
    }

    pub fn work(&self) -> Result<Vec<WorkRecord>> {
        let mut statement = self
            .connection
            .prepare(
                "SELECT seq,id,path,original_path,desired_path,operation,accepted_local,
                        baseline,request_id,source_artifact,conflict
                 FROM work ORDER BY path,seq",
            )
            .map_err(|error| Error::corrupt(format!("read work: {error}")))?;
        let rows = statement
            .query_map([], work_from_row)
            .map_err(|error| Error::corrupt(format!("read work: {error}")))?;
        rows.map(|row| row.map_err(|error| Error::corrupt(format!("read work: {error}"))))
            .collect()
    }

    pub fn insert_work(&mut self, work: &WorkRecord) -> Result<()> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| Error::corrupt(format!("begin work association: {error}")))?;
        let seq: i64 = transaction
            .query_row("SELECT COALESCE(MAX(seq),0)+1 FROM work", [], |row| {
                row.get(0)
            })
            .map_err(|error| Error::corrupt(format!("allocate work sequence: {error}")))?;
        transaction
            .execute(
                "INSERT INTO work(
                    seq,id,path,original_path,desired_path,operation,accepted_local,
                    baseline,request_id,source_artifact,conflict
                 ) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)",
                params![
                    seq,
                    work.id,
                    work.path,
                    work.original_path,
                    work.desired_path,
                    work.operation.as_str(),
                    work.accepted_local.as_ref().map(encode_json).transpose()?,
                    work.baseline.as_ref().map(encode_json).transpose()?,
                    work.request_id.as_slice(),
                    work.source_artifact,
                    work.conflict,
                ],
            )
            .map_err(|error| Error::corrupt(format!("insert work association: {error}")))?;
        transaction
            .commit()
            .map_err(|error| Error::corrupt(format!("commit work association: {error}")))?;
        Ok(())
    }

    pub fn remove_work(&self, id: &str) -> Result<()> {
        self.connection
            .execute("DELETE FROM work WHERE id=?1", params![id])
            .map_err(|error| Error::corrupt(format!("remove work association: {error}")))?;
        Ok(())
    }

    pub fn complete_publication(
        &mut self,
        work: &WorkRecord,
        updates: &[(&str, RemoteEntry, Option<LocalFingerprint>)],
        removed: &[&str],
        receipt: Option<&RemoteReceipt>,
        commit_id: Option<[u8; 16]>,
    ) -> Result<()> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| Error::corrupt(format!("begin publication completion: {error}")))?;
        for (path, remote, local) in updates {
            let local = local.map(|local| pair_local_with_remote(local, remote));
            transaction
                .execute(
                    "INSERT INTO baseline(path,remote,local) VALUES(?1,?2,?3)
                     ON CONFLICT(path) DO UPDATE SET remote=excluded.remote,local=excluded.local",
                    params![
                        path,
                        encode_json(remote)?,
                        local.as_ref().map(encode_json).transpose()?
                    ],
                )
                .map_err(|error| {
                    Error::corrupt(format!("advance publication baseline: {error}"))
                })?;
        }
        for path in removed {
            transaction
                .execute(
                    "DELETE FROM baseline WHERE path=?1 OR substr(path,1,length(?1)+1)=?1||'/'",
                    params![path],
                )
                .map_err(|error| Error::corrupt(format!("remove publication baseline: {error}")))?;
        }
        transaction
            .execute(
                "DELETE FROM intents WHERE id IN (
                   SELECT CAST(id AS BLOB) FROM work WHERE path=?1 AND id<>?2
                 )",
                params![work.path, work.id],
            )
            .map_err(|error| Error::corrupt(format!("remove prior intents: {error}")))?;
        transaction
            .execute(
                "DELETE FROM work WHERE path=?1 AND id<>?2",
                params![work.path, work.id],
            )
            .map_err(|error| Error::corrupt(format!("remove prior work: {error}")))?;
        transaction
            .execute(
                "UPDATE work SET source_artifact=NULL,conflict=NULL WHERE id=?1",
                params![work.id],
            )
            .map_err(|error| Error::corrupt(format!("complete work association: {error}")))?;
        transaction
            .execute(
                "UPDATE intents SET
                   state=?2,commit_id=?3,receipt_revision=?4,receipt_ordinal=?5,failure=NULL
                 WHERE id=CAST(?1 AS BLOB)",
                params![
                    work.id,
                    i64::from(IntentState::Committed.code()),
                    commit_id.map(|value| value.to_vec()),
                    receipt
                        .and_then(|receipt| receipt.revision)
                        .map(|value| value.to_vec()),
                    receipt.and_then(|receipt| receipt.ordinal).map(i64::from),
                ],
            )
            .map_err(|error| Error::corrupt(format!("complete intent: {error}")))?;
        transaction
            .commit()
            .map_err(|error| Error::corrupt(format!("commit publication completion: {error}")))?;
        Ok(())
    }

    pub fn rebind(
        &mut self,
        binding: &Binding,
        identities: &BTreeMap<Identity, Identity>,
        state: &Path,
    ) -> Result<()> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| Error::corrupt(format!("begin sync rebind: {error}")))?;
        let updated = transaction
            .execute(
                "UPDATE binding SET
                   root=?1,root_dev=?2,root_ino=?3,state_dev=?4,state_ino=?5
                 WHERE singleton=1 AND model=?6 AND namespace=?7 AND namespace_digest=?8",
                params![
                    binding.root.to_str().ok_or_else(|| Error::unsupported(
                        "restored sync root path is not UTF-8"
                    ))?,
                    binding.root_identity.dev.to_be_bytes().as_slice(),
                    binding.root_identity.ino.to_be_bytes().as_slice(),
                    binding.state_identity.dev.to_be_bytes().as_slice(),
                    binding.state_identity.ino.to_be_bytes().as_slice(),
                    match binding.model {
                        SyncModel::Managed => "managed",
                        SyncModel::Direct => "direct",
                    },
                    binding.namespace,
                    binding.namespace_digest.as_slice(),
                ],
            )
            .map_err(|error| Error::corrupt(format!("rebind sync binding: {error}")))?;
        if updated != 1 {
            return Err(Error::invalid(
                "restored sync state does not match the configured model and namespace",
            ));
        }

        let baseline = {
            let mut statement = transaction
                .prepare("SELECT path,local FROM baseline WHERE local IS NOT NULL")
                .map_err(|error| Error::corrupt(format!("read baseline identities: {error}")))?;
            statement
                .query_map([], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?))
                })
                .map_err(|error| Error::corrupt(format!("read baseline identities: {error}")))?
                .collect::<rusqlite::Result<Vec<_>>>()
                .map_err(|error| Error::corrupt(format!("read baseline identities: {error}")))?
        };
        for (path, bytes) in baseline {
            let mut fingerprint: LocalFingerprint =
                decode_json(&bytes, "baseline local fingerprint")?;
            rebind_fingerprint(&mut fingerprint, identities)?;
            transaction
                .execute(
                    "UPDATE baseline SET local=?2 WHERE path=?1",
                    params![path, encode_json(&fingerprint)?],
                )
                .map_err(|error| Error::corrupt(format!("rebind baseline identity: {error}")))?;
        }

        let work = {
            let mut statement = transaction
                .prepare("SELECT id,accepted_local FROM work WHERE accepted_local IS NOT NULL")
                .map_err(|error| Error::corrupt(format!("read work identities: {error}")))?;
            statement
                .query_map([], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?))
                })
                .map_err(|error| Error::corrupt(format!("read work identities: {error}")))?
                .collect::<rusqlite::Result<Vec<_>>>()
                .map_err(|error| Error::corrupt(format!("read work identities: {error}")))?
        };
        for (id, bytes) in work {
            let mut fingerprint: LocalFingerprint = decode_json(&bytes, "work accepted local")?;
            rebind_fingerprint(&mut fingerprint, identities)?;
            transaction
                .execute(
                    "UPDATE work SET accepted_local=?2 WHERE id=?1",
                    params![id, encode_json(&fingerprint)?],
                )
                .map_err(|error| Error::corrupt(format!("rebind work identity: {error}")))?;
        }

        let installs = {
            let mut statement = transaction
                .prepare(
                    "SELECT id,expected_local,desired_local FROM installs
                     WHERE expected_local IS NOT NULL OR desired_local IS NOT NULL",
                )
                .map_err(|error| Error::corrupt(format!("read install identities: {error}")))?;
            statement
                .query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, Option<Vec<u8>>>(1)?,
                        row.get::<_, Option<Vec<u8>>>(2)?,
                    ))
                })
                .map_err(|error| Error::corrupt(format!("read install identities: {error}")))?
                .collect::<rusqlite::Result<Vec<_>>>()
                .map_err(|error| Error::corrupt(format!("read install identities: {error}")))?
        };
        for (id, expected, desired) in installs {
            let expected = match expected.as_deref() {
                Some(bytes) => {
                    let mut fingerprint =
                        decode_json::<LocalFingerprint>(bytes, "install expected local")?;
                    rebind_fingerprint(&mut fingerprint, identities)?;
                    Some(encode_json(&fingerprint)?)
                }
                None => None,
            };
            let desired = match desired.as_deref() {
                Some(bytes) => {
                    let mut fingerprint =
                        decode_json::<LocalFingerprint>(bytes, "install desired local")?;
                    rebind_fingerprint(&mut fingerprint, identities)?;
                    Some(encode_json(&fingerprint)?)
                }
                None => None,
            };
            transaction
                .execute(
                    "UPDATE installs SET expected_local=?2,desired_local=?3 WHERE id=?1",
                    params![id, expected, desired],
                )
                .map_err(|error| Error::corrupt(format!("rebind install identity: {error}")))?;
        }

        let artifacts = {
            let mut statement = transaction
                .prepare(
                    "SELECT id,name,length,digest FROM artifacts
                     WHERE stat_dev IS NOT NULL ORDER BY id",
                )
                .map_err(|error| Error::corrupt(format!("read artifact identities: {error}")))?;
            statement
                .query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, Option<Vec<u8>>>(3)?,
                    ))
                })
                .map_err(|error| Error::corrupt(format!("read artifact identities: {error}")))?
                .collect::<rusqlite::Result<Vec<_>>>()
                .map_err(|error| Error::corrupt(format!("read artifact identities: {error}")))?
        };
        for (id, name, length, digest) in artifacts {
            let Some(digest) = digest else {
                continue;
            };
            let digest: [u8; 32] = decode_fixed(&digest, "artifact digest")?;
            let length = u64::try_from(length)
                .map_err(|_| Error::corrupt("restored artifact has a negative length"))?;
            let path = state.join("artifacts").join(&name);
            let Some(stat) = verified_artifact_stat(&path, length, &digest) else {
                continue;
            };
            transaction
                .execute(
                    "UPDATE artifacts SET
                       stat_dev=?2,stat_ino=?3,stat_mtime=?4,stat_mtime_nsec=?5,
                       stat_ctime=?6,stat_ctime_nsec=?7
                     WHERE id=?1",
                    params![
                        id,
                        stat.dev().to_be_bytes().as_slice(),
                        stat.ino().to_be_bytes().as_slice(),
                        stat.mtime(),
                        stat.mtime_nsec(),
                        stat.ctime(),
                        stat.ctime_nsec()
                    ],
                )
                .map_err(|error| Error::corrupt(format!("rebind artifact identity: {error}")))?;
        }

        transaction
            .commit()
            .map_err(|error| Error::corrupt(format!("commit sync rebind: {error}")))?;
        Ok(())
    }
}

fn verified_artifact_stat(
    path: &Path,
    expected_length: u64,
    expected_digest: &[u8; 32],
) -> Option<std::fs::Metadata> {
    let metadata = std::fs::symlink_metadata(path).ok()?;
    if !metadata.file_type().is_file() || metadata.len() != expected_length {
        return None;
    }
    let mut file = std::fs::File::open(path).ok()?;
    let mut hasher = blake3::Hasher::new();
    let mut length = 0_u64;
    let mut buffer = vec![0_u8; 64 * 1024];
    loop {
        let count = file.read(&mut buffer).ok()?;
        if count == 0 {
            break;
        }
        hasher.update(&buffer[..count]);
        length = length.checked_add(count as u64)?;
    }
    (length == expected_length && hasher.finalize().as_bytes() == expected_digest)
        .then_some(metadata)
}

fn rebind_fingerprint(
    fingerprint: &mut LocalFingerprint,
    identities: &BTreeMap<Identity, Identity>,
) -> Result<()> {
    let own = Identity {
        dev: fingerprint.dev,
        ino: fingerprint.ino,
    };
    let parent = Identity {
        dev: fingerprint.parent_dev,
        ino: fingerprint.parent_ino,
    };
    let own = identities.get(&own).copied().ok_or_else(|| {
        Error::corrupt(format!(
            "restored local file has no identity mapping for {own:?}"
        ))
    })?;
    let parent = identities.get(&parent).copied().ok_or_else(|| {
        Error::corrupt(format!(
            "restored local parent has no identity mapping for {parent:?}"
        ))
    })?;
    fingerprint.dev = own.dev;
    fingerprint.ino = own.ino;
    fingerprint.parent_dev = parent.dev;
    fingerprint.parent_ino = parent.ino;
    if let Some(stat) = &mut fingerprint.stat {
        stat.dev = own.dev;
        stat.ino = own.ino;
    }
    Ok(())
}

fn binding_values(binding: &Binding) -> Result<Vec<store::Binding>> {
    let model = match binding.model {
        SyncModel::Managed => "managed",
        SyncModel::Direct => "direct",
    };
    let root = binding
        .root
        .to_str()
        .ok_or_else(|| Error::unsupported("sync root path is not UTF-8"))?;
    Ok(vec![
        store::Binding::text("model", model),
        store::Binding::text("namespace", binding.namespace.clone()),
        store::Binding::blob("namespace_digest", binding.namespace_digest.to_vec()),
        store::Binding::text("root", root.to_owned()),
        store::Binding::blob("root_dev", binding.root_identity.dev.to_be_bytes().to_vec()),
        store::Binding::blob("root_ino", binding.root_identity.ino.to_be_bytes().to_vec()),
        store::Binding::blob(
            "state_dev",
            binding.state_identity.dev.to_be_bytes().to_vec(),
        ),
        store::Binding::blob(
            "state_ino",
            binding.state_identity.ino.to_be_bytes().to_vec(),
        ),
    ])
}

fn validate_schema(connection: &Connection) -> Result<()> {
    store::validate(connection, &PROFILE, None).map_err(store_error)
}

fn store_error(error: store::Error) -> Error {
    Error::new(error.kind().into(), error.to_string())
}

fn now_ns() -> Result<i64> {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| Error::invalid("sync clock is before the Unix epoch"))?
        .as_nanos();
    i64::try_from(nanos).map_err(|_| Error::invalid("sync clock exceeds SQLite range"))
}

pub(super) fn recorded_now() -> Result<i64> {
    now_ns()
}

fn fingerprint_to_node(path: &str, fingerprint: LocalFingerprint) -> Option<LocalNode> {
    let stat = fingerprint.stat.map(|stat| LocalStat {
        dev: stat.dev,
        ino: stat.ino,
        size: fingerprint.length,
        mtime: stat.mtime,
        mtime_nsec: stat.mtime_nsec,
        ctime: stat.ctime,
        ctime_nsec: stat.ctime_nsec,
    });
    Some(LocalNode {
        path: path.to_owned(),
        kind: LocalKind::File {
            length: fingerprint.length,
            digest: ContentHash::from_bytes(fingerprint.digest),
        },
        executable: fingerprint.executable,
        identity: fingerprint.identity(),
        parent: Some(Identity {
            dev: fingerprint.parent_dev,
            ino: fingerprint.parent_ino,
        }),
        stat,
    })
}

fn artifact_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<ArtifactRecord> {
    let remote_revision: Option<Vec<u8>> = row.get(6)?;
    let digest: Option<Vec<u8>> = row.get(7)?;
    let stat_dev: Option<Vec<u8>> = row.get(9)?;
    let stat_ino: Option<Vec<u8>> = row.get(10)?;
    let stat = match (stat_dev, stat_ino) {
        (Some(dev), Some(ino)) => Some(ArtifactStat {
            dev: decode_u64_sql(&dev)?,
            ino: decode_u64_sql(&ino)?,
            mtime: row.get(11)?,
            mtime_nsec: row.get(12)?,
            ctime: row.get(13)?,
            ctime_nsec: row.get(14)?,
        }),
        (None, None) => None,
        _ => return Err(corrupt_sql("incomplete artifact stat")),
    };
    Ok(ArtifactRecord {
        id: row.get(0)?,
        origin: row.get(1)?,
        name: row.get(2)?,
        kind: ArtifactKind::parse(&row.get::<_, String>(3)?)
            .map_err(|error| corrupt_sql(error.to_string()))?,
        phase: ArtifactPhase::parse(&row.get::<_, String>(4)?)
            .map_err(|error| corrupt_sql(error.to_string()))?,
        release_event: ArtifactRelease::parse(&row.get::<_, String>(5)?)
            .map_err(|error| corrupt_sql(error.to_string()))?,
        remote_revision: remote_revision
            .as_deref()
            .map(|bytes| decode_fixed(bytes, "artifact revision"))
            .transpose()
            .map_err(|error| corrupt_sql(error.to_string()))?,
        digest: digest
            .as_deref()
            .map(|bytes| decode_fixed(bytes, "artifact digest"))
            .transpose()
            .map_err(|error| corrupt_sql(error.to_string()))?,
        length: u64::try_from(row.get::<_, i64>(8)?)
            .map_err(|_| corrupt_sql("negative artifact length"))?,
        stat,
        settle_pending: row.get(15)?,
        error: row.get(16)?,
    })
}

fn artifact_params(artifact: &ArtifactRecord) -> Result<Vec<rusqlite::types::Value>> {
    use rusqlite::types::Value;
    Ok(vec![
        Value::Text(artifact.id.clone()),
        Value::Text(artifact.origin.clone()),
        Value::Text(artifact.name.clone()),
        Value::Text(artifact.kind.as_str().to_owned()),
        Value::Text(artifact.phase.as_str().to_owned()),
        Value::Text(artifact.release_event.as_str().to_owned()),
        artifact
            .remote_revision
            .map(|value| Value::Blob(value.to_vec()))
            .unwrap_or(Value::Null),
        artifact
            .digest
            .map(|value| Value::Blob(value.to_vec()))
            .unwrap_or(Value::Null),
        Value::Integer(
            i64::try_from(artifact.length)
                .map_err(|_| Error::invalid("artifact length exceeds SQLite range"))?,
        ),
        artifact
            .stat
            .map(|stat| Value::Blob(stat.dev.to_be_bytes().to_vec()))
            .unwrap_or(Value::Null),
        artifact
            .stat
            .map(|stat| Value::Blob(stat.ino.to_be_bytes().to_vec()))
            .unwrap_or(Value::Null),
        artifact
            .stat
            .map(|stat| Value::Integer(stat.mtime))
            .unwrap_or(Value::Null),
        artifact
            .stat
            .map(|stat| Value::Integer(stat.mtime_nsec))
            .unwrap_or(Value::Null),
        artifact
            .stat
            .map(|stat| Value::Integer(stat.ctime))
            .unwrap_or(Value::Null),
        artifact
            .stat
            .map(|stat| Value::Integer(stat.ctime_nsec))
            .unwrap_or(Value::Null),
        Value::Integer(i64::from(artifact.settle_pending)),
        artifact
            .error
            .clone()
            .map(Value::Text)
            .unwrap_or(Value::Null),
    ])
}

fn install_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<InstallRecord> {
    let expected = row.get::<_, Option<Vec<u8>>>(4)?;
    let desired = row.get::<_, Option<Vec<u8>>>(5)?;
    let remote = row.get::<_, Option<Vec<u8>>>(6)?;
    Ok(InstallRecord {
        id: row.get(0)?,
        path: row.get(1)?,
        operation: InstallOperation::parse(&row.get::<_, String>(2)?)
            .map_err(|error| corrupt_sql(error.to_string()))?,
        state: InstallState::parse(&row.get::<_, String>(3)?)
            .map_err(|error| corrupt_sql(error.to_string()))?,
        expected_local: expected
            .as_deref()
            .map(|bytes| decode_json(bytes, "install expected local"))
            .transpose()
            .map_err(|error| corrupt_sql(error.to_string()))?,
        desired_local: desired
            .as_deref()
            .map(|bytes| decode_json(bytes, "install desired local"))
            .transpose()
            .map_err(|error| corrupt_sql(error.to_string()))?,
        desired_remote: remote
            .as_deref()
            .map(|bytes| decode_json(bytes, "install desired remote"))
            .transpose()
            .map_err(|error| corrupt_sql(error.to_string()))?,
        temporary: row.get(7)?,
        artifact: row.get(8)?,
        error: row.get(9)?,
    })
}

fn install_params(install: &InstallRecord) -> Result<Vec<rusqlite::types::Value>> {
    use rusqlite::types::Value;
    Ok(vec![
        Value::Text(install.id.clone()),
        Value::Text(install.path.clone()),
        Value::Text(install.operation.as_str().to_owned()),
        Value::Text(install.state.as_str().to_owned()),
        install
            .expected_local
            .map(|value| encode_json(&value).map(Value::Blob))
            .transpose()?
            .unwrap_or(Value::Null),
        install
            .desired_local
            .map(|value| encode_json(&value).map(Value::Blob))
            .transpose()?
            .unwrap_or(Value::Null),
        install
            .desired_remote
            .as_ref()
            .map(|value| encode_json(value).map(Value::Blob))
            .transpose()?
            .unwrap_or(Value::Null),
        Value::Text(install.temporary.clone()),
        install
            .artifact
            .clone()
            .map(Value::Text)
            .unwrap_or(Value::Null),
        install
            .error
            .clone()
            .map(Value::Text)
            .unwrap_or(Value::Null),
    ])
}

fn conflict_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<ConflictRecord> {
    let local: Vec<u8> = row.get(4)?;
    let remote: Vec<u8> = row.get(5)?;
    let revision: Option<Vec<u8>> = row.get(8)?;
    Ok(ConflictRecord {
        id: row.get(0)?,
        path: row.get(1)?,
        original_path: row.get(2)?,
        desired_path: row.get(3)?,
        local: decode_json(&local, "conflict local")
            .map_err(|error| corrupt_sql(error.to_string()))?,
        remote: decode_json(&remote, "conflict remote")
            .map_err(|error| corrupt_sql(error.to_string()))?,
        local_artifact: row.get(6)?,
        remote_artifact: row.get(7)?,
        remote_revision: revision
            .as_deref()
            .map(|bytes| decode_fixed(bytes, "conflict revision"))
            .transpose()
            .map_err(|error| corrupt_sql(error.to_string()))?,
        error: row.get(9)?,
    })
}

fn conflict_params(conflict: &ConflictRecord) -> Result<Vec<rusqlite::types::Value>> {
    use rusqlite::types::Value;
    Ok(vec![
        Value::Text(conflict.id.clone()),
        Value::Text(conflict.path.clone()),
        conflict
            .original_path
            .clone()
            .map(Value::Text)
            .unwrap_or(Value::Null),
        conflict
            .desired_path
            .clone()
            .map(Value::Text)
            .unwrap_or(Value::Null),
        Value::Blob(encode_json(&conflict.local)?),
        Value::Blob(encode_json(&conflict.remote)?),
        conflict
            .local_artifact
            .clone()
            .map(Value::Text)
            .unwrap_or(Value::Null),
        conflict
            .remote_artifact
            .clone()
            .map(Value::Text)
            .unwrap_or(Value::Null),
        conflict
            .remote_revision
            .map(|revision| Value::Blob(revision.to_vec()))
            .unwrap_or(Value::Null),
        Value::Text(conflict.error.clone()),
    ])
}

fn intent_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<IntentRecord> {
    let id: Vec<u8> = row.get(0)?;
    let request_id: Vec<u8> = row.get(1)?;
    let request_digest: Vec<u8> = row.get(2)?;
    let commit_id: Option<Vec<u8>> = row.get(4)?;
    let revision: Option<Vec<u8>> = row.get(6)?;
    let failure: Option<Vec<u8>> = row.get(8)?;
    Ok(IntentRecord {
        id: String::from_utf8(id).map_err(|_| corrupt_sql("intent id is not UTF-8"))?,
        request_id: decode_fixed(&request_id, "intent request id")
            .map_err(|error| corrupt_sql(error.to_string()))?,
        request_digest: decode_fixed(&request_digest, "intent request digest")
            .map_err(|error| corrupt_sql(error.to_string()))?,
        request: row.get(3)?,
        commit_id: commit_id
            .as_deref()
            .map(|bytes| decode_fixed(bytes, "intent commit id"))
            .transpose()
            .map_err(|error| corrupt_sql(error.to_string()))?,
        state: IntentState::from_code(row.get(5)?)
            .ok_or_else(|| corrupt_sql("unknown intent state"))?,
        receipt_revision: revision
            .as_deref()
            .map(|bytes| decode_fixed(bytes, "intent receipt revision"))
            .transpose()
            .map_err(|error| corrupt_sql(error.to_string()))?,
        receipt_ordinal: row
            .get::<_, Option<i64>>(7)?
            .map(|value| {
                u32::try_from(value).map_err(|_| corrupt_sql("negative intent receipt ordinal"))
            })
            .transpose()?,
        failure: failure
            .as_deref()
            .map(|bytes| decode_json(bytes, "intent failure"))
            .transpose()
            .map_err(|error| corrupt_sql(error.to_string()))?,
    })
}

fn intent_params(intent: &IntentRecord) -> Result<Vec<rusqlite::types::Value>> {
    use rusqlite::types::Value;
    Ok(vec![
        Value::Blob(intent.id.as_bytes().to_vec()),
        Value::Blob(intent.request_id.to_vec()),
        Value::Blob(intent.request_digest.to_vec()),
        Value::Blob(intent.request.clone()),
        intent
            .commit_id
            .map(|value| Value::Blob(value.to_vec()))
            .unwrap_or(Value::Null),
        Value::Integer(i64::from(intent.state.code())),
        intent
            .receipt_revision
            .map(|value| Value::Blob(value.to_vec()))
            .unwrap_or(Value::Null),
        intent
            .receipt_ordinal
            .map(|value| Value::Integer(i64::from(value)))
            .unwrap_or(Value::Null),
        intent
            .failure
            .as_ref()
            .map(|failure| encode_json(failure).map(Value::Blob))
            .transpose()?
            .unwrap_or(Value::Null),
        Value::Blob(Vec::new()),
    ])
}

fn work_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<WorkRecord> {
    let accepted: Option<Vec<u8>> = row.get(6)?;
    let baseline: Option<Vec<u8>> = row.get(7)?;
    let request_id: Vec<u8> = row.get(8)?;
    Ok(WorkRecord {
        seq: u64::try_from(row.get::<_, i64>(0)?)
            .map_err(|_| corrupt_sql("negative work sequence"))?,
        id: row.get(1)?,
        path: row.get(2)?,
        original_path: row.get(3)?,
        desired_path: row.get(4)?,
        operation: super::adapter::RemoteOperation::parse(&row.get::<_, String>(5)?)
            .map_err(|error| corrupt_sql(error.to_string()))?,
        accepted_local: accepted
            .as_deref()
            .map(|bytes| decode_json(bytes, "work accepted local"))
            .transpose()
            .map_err(|error| corrupt_sql(error.to_string()))?,
        baseline: baseline
            .as_deref()
            .map(|bytes| decode_json(bytes, "work baseline"))
            .transpose()
            .map_err(|error| corrupt_sql(error.to_string()))?,
        request_id: decode_fixed(&request_id, "work request id")
            .map_err(|error| corrupt_sql(error.to_string()))?,
        source_artifact: row.get(9)?,
        conflict: row.get(10)?,
    })
}

fn encode_json<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    serde_json::to_vec(value).map_err(|error| Error::corrupt(format!("encode sync state: {error}")))
}

fn decode_json<T: for<'de> Deserialize<'de>>(bytes: &[u8], context: &str) -> Result<T> {
    serde_json::from_slice(bytes)
        .map_err(|error| Error::corrupt(format!("decode {context}: {error}")))
}

fn decode_fixed<const N: usize>(bytes: &[u8], context: &str) -> Result<[u8; N]> {
    bytes
        .try_into()
        .map_err(|_| Error::corrupt(format!("{context} has {} bytes, expected {N}", bytes.len())))
}

fn decode_u64(bytes: &[u8], context: &str) -> Result<u64> {
    Ok(u64::from_be_bytes(decode_fixed(bytes, context)?))
}

fn decode_u64_sql(bytes: &[u8]) -> rusqlite::Result<u64> {
    let bytes: [u8; 8] = bytes
        .try_into()
        .map_err(|_| corrupt_sql("invalid encoded u64 length"))?;
    Ok(u64::from_be_bytes(bytes))
}

fn corrupt_sql(message: impl Into<String>) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(
        0,
        rusqlite::types::Type::Blob,
        Box::new(Error::corrupt(message)),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn binding() -> Binding {
        Binding {
            model: SyncModel::Managed,
            namespace: "namespace".to_owned(),
            namespace_digest: [1; 32],
            root: PathBuf::from("/root"),
            root_identity: Identity { dev: 1, ino: 2 },
            state_identity: Identity { dev: 1, ino: 3 },
        }
    }

    #[test]
    fn old_profiles_and_partial_current_schemas_are_rejected() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("sync-engine.db");
        drop(Store::create(&path, &binding()).unwrap());
        Connection::open(&path)
            .unwrap()
            .execute(
                "UPDATE binding SET profile='yinyang-directory-replica-3' WHERE singleton=1",
                [],
            )
            .unwrap();
        assert_eq!(
            Store::open_read_only(&path).err().unwrap().kind(),
            ErrorKind::Unsupported
        );
        assert_eq!(
            Store::open(&path).err().unwrap().kind(),
            ErrorKind::Unsupported
        );

        Connection::open(&path)
            .unwrap()
            .execute_batch(
                "UPDATE binding SET profile='yinyang-sync-engine-2' WHERE singleton=1;
                 DROP TABLE work;",
            )
            .unwrap();
        assert_eq!(
            Store::open_read_only(&path).err().unwrap().kind(),
            ErrorKind::Corrupt
        );
    }
}
