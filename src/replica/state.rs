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

use super::{Error, Result};
use super::{LocalKind, LocalNode, LocalSnapshot, LocalStat};
use crate::replica::linux::{ContentHash, Identity};
use crate::runtime::schema;
use rusqlite::{Connection, OptionalExtension, params};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};
use yinyang_core::{Node, NodeId, NodeKind, Revision};

pub(super) const PROFILE: &str = "yinyang-directory-replica-2";
pub(super) type IdentityMap = BTreeMap<(u64, u64), (u64, u64)>;

const REQUIRED_SCHEMA: &[schema::TableColumns<'static>] = &[
    (
        "binding",
        &[
            "singleton",
            "profile",
            "filesystem",
            "root",
            "root_dev",
            "root_ino",
            "state_dev",
            "state_ino",
        ],
    ),
    ("baseline_meta", &["singleton", "revision"]),
    (
        "artifacts",
        &[
            "id",
            "origin",
            "name",
            "kind",
            "reason",
            "remote_revision",
            "phase",
            "error",
            "digest",
            "length",
            "stat_dev",
            "stat_ino",
            "stat_mtime",
            "stat_mtime_nsec",
            "stat_ctime",
            "stat_ctime_nsec",
        ],
    ),
    (
        "installs",
        &[
            "id",
            "path",
            "parent",
            "name",
            "temporary",
            "temporary_dev",
            "temporary_ino",
            "expected_digest",
            "expected_length",
            "expected_dev",
            "expected_ino",
            "parent_dev",
            "parent_ino",
            "replacement_digest",
            "replacement_length",
            "remote_revision",
            "artifact",
            "phase",
            "error",
        ],
    ),
];
const TABLE_NAMES: &[&str] = &[
    "binding",
    "baseline",
    "baseline_meta",
    "artifacts",
    "installs",
    "conflicts",
    "work",
];
const CONFLICTS_SCHEMA: schema::TableColumns<'static> = (
    "conflicts",
    &[
        "id",
        "path",
        "original_path",
        "desired_path",
        "local",
        "remote",
        "local_artifact",
        "remote_node",
        "remote_revision",
        "error",
    ],
);
const BASELINE_BASE: &[&str] = &[
    "path",
    "node",
    "directory",
    "executable",
    "remote_digest",
    "remote_length",
    "local_present",
    "local_digest",
    "local_length",
    "local_executable",
    "local_dev",
    "local_ino",
    "local_parent_dev",
    "local_parent_ino",
    "local_stat_size",
    "local_stat_mtime",
    "local_stat_mtime_nsec",
    "local_stat_ctime",
    "local_stat_ctime_nsec",
    "local_stat_recorded_ns",
];
const BASELINE_OPTIONAL: &[&str] = &["remote_revision"];
const WORK_BASE: &[&str] = &[
    "id",
    "path",
    "original_path",
    "desired_path",
    "operation",
    "local_kind",
    "local_digest",
    "local_length",
    "local_executable",
    "baseline_node",
    "baseline_revision",
    "remote_revision",
    "state",
    "error",
];
const WORK_OPTIONAL: &[&str] = &[
    "seq",
    "local_dev",
    "local_ino",
    "local_parent_dev",
    "local_parent_ino",
    "commit_id",
    "conflict",
    "source_artifact",
    "upload_artifact",
];
const WORK_UPGRADE_ORDER: &[&str] = &[
    "seq",
    "conflict",
    "source_artifact",
    "local_dev",
    "local_ino",
    "local_parent_dev",
    "local_parent_ino",
    "commit_id",
    "upload_artifact",
];
const WORK_COLUMNS: &[&str] = &[
    "seq",
    "id",
    "path",
    "original_path",
    "desired_path",
    "operation",
    "local_kind",
    "local_digest",
    "local_length",
    "local_executable",
    "local_dev",
    "local_ino",
    "local_parent_dev",
    "local_parent_ino",
    "baseline_node",
    "baseline_revision",
    "remote_revision",
    "commit_id",
    "conflict",
    "source_artifact",
    "upload_artifact",
    "state",
    "error",
];
const CREATE_CONFLICTS_TABLE: &str = "
    CREATE TABLE conflicts(
      id TEXT PRIMARY KEY,
      path TEXT NOT NULL,
      original_path TEXT,
      desired_path TEXT,
      local TEXT NOT NULL,
      remote TEXT NOT NULL,
      local_artifact TEXT,
      remote_node BLOB,
      remote_revision BLOB,
      error TEXT NOT NULL
    );";
const CREATE_WORK_TABLE: &str = "
    CREATE TABLE work(
      id BLOB PRIMARY KEY,
      seq INTEGER NOT NULL DEFAULT 0,
      path TEXT NOT NULL,
      original_path TEXT,
      desired_path TEXT,
      operation TEXT NOT NULL,
      local_kind TEXT,
      local_digest BLOB,
      local_length INTEGER,
      local_executable INTEGER,
      local_dev BLOB,
      local_ino BLOB,
      local_parent_dev BLOB,
      local_parent_ino BLOB,
      baseline_node BLOB,
      baseline_revision BLOB NOT NULL,
      remote_revision BLOB,
      commit_id BLOB,
      conflict TEXT,
      source_artifact TEXT,
      upload_artifact TEXT,
      state TEXT NOT NULL,
      error TEXT
    );";

const CREATE_SCHEMA: &str = "
    CREATE TABLE binding(
      singleton INTEGER PRIMARY KEY CHECK(singleton=1),
      profile TEXT NOT NULL,
      filesystem BLOB NOT NULL,
      root TEXT NOT NULL,
      root_dev BLOB NOT NULL,
      root_ino BLOB NOT NULL,
      state_dev BLOB NOT NULL,
      state_ino BLOB NOT NULL
    );
    CREATE TABLE baseline(
      path TEXT PRIMARY KEY,
      node BLOB NOT NULL,
      directory INTEGER NOT NULL,
      executable INTEGER NOT NULL,
      remote_digest BLOB,
      remote_length INTEGER,
      local_present INTEGER NOT NULL,
      local_digest BLOB,
      local_length INTEGER,
      local_executable INTEGER,
      local_dev BLOB,
      local_ino BLOB,
      local_parent_dev BLOB,
      local_parent_ino BLOB,
      local_stat_size INTEGER,
      local_stat_mtime INTEGER,
      local_stat_mtime_nsec INTEGER,
      local_stat_ctime INTEGER,
      local_stat_ctime_nsec INTEGER,
      local_stat_recorded_ns INTEGER,
      remote_revision BLOB
    );
    CREATE TABLE baseline_meta(
      singleton INTEGER PRIMARY KEY CHECK(singleton=1),
      revision BLOB NOT NULL
    );
    CREATE TABLE artifacts(
      id TEXT PRIMARY KEY,
      origin TEXT NOT NULL,
      name TEXT NOT NULL UNIQUE,
      kind TEXT NOT NULL,
      reason TEXT NOT NULL,
      remote_revision BLOB,
      phase TEXT NOT NULL,
      error TEXT,
      digest BLOB,
      length INTEGER NOT NULL,
      stat_dev BLOB,
      stat_ino BLOB,
      stat_mtime INTEGER,
      stat_mtime_nsec INTEGER,
      stat_ctime INTEGER,
      stat_ctime_nsec INTEGER,
      settle_pending INTEGER NOT NULL DEFAULT 0
    );
    CREATE TABLE installs(
      id TEXT PRIMARY KEY,
      path TEXT NOT NULL,
      parent TEXT NOT NULL,
      name TEXT NOT NULL,
      temporary TEXT NOT NULL,
      temporary_dev BLOB,
      temporary_ino BLOB,
      expected_digest BLOB NOT NULL,
      expected_length INTEGER NOT NULL,
      expected_dev BLOB NOT NULL,
      expected_ino BLOB NOT NULL,
      parent_dev BLOB NOT NULL,
      parent_ino BLOB NOT NULL,
      replacement_digest BLOB,
      replacement_length INTEGER,
      remote_revision BLOB,
      artifact TEXT NOT NULL,
      phase TEXT NOT NULL,
      error TEXT
    );
    CREATE TABLE conflicts(
      id TEXT PRIMARY KEY,
      path TEXT NOT NULL,
      original_path TEXT,
      desired_path TEXT,
      local TEXT NOT NULL,
      remote TEXT NOT NULL,
      local_artifact TEXT,
      remote_node BLOB,
      remote_revision BLOB,
      error TEXT NOT NULL
    );
    CREATE TABLE work(
      id BLOB PRIMARY KEY,
      seq INTEGER NOT NULL DEFAULT 0,
      path TEXT NOT NULL,
      original_path TEXT,
      desired_path TEXT,
      operation TEXT NOT NULL,
      local_kind TEXT,
      local_digest BLOB,
      local_length INTEGER,
      local_executable INTEGER,
      local_dev BLOB,
      local_ino BLOB,
      local_parent_dev BLOB,
      local_parent_ino BLOB,
      baseline_node BLOB,
      baseline_revision BLOB NOT NULL,
      remote_revision BLOB,
      commit_id BLOB,
      conflict TEXT,
      source_artifact TEXT,
      upload_artifact TEXT,
      state TEXT NOT NULL,
      error TEXT
    );";

#[derive(Clone, Debug)]
pub(super) struct Binding {
    pub filesystem: NodeId,
    pub root: String,
    pub root_dev: u64,
    pub root_ino: u64,
    pub state_dev: u64,
    pub state_ino: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum InstallPhase {
    Creating,
    Prepared,
    Exchanged,
    Retained,
    Conflict,
    Failed,
}

impl InstallPhase {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Creating => "creating",
            Self::Prepared => "prepared",
            Self::Exchanged => "exchanged",
            Self::Retained => "retained",
            Self::Conflict => "conflict",
            Self::Failed => "failed",
        }
    }

    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "creating" => Ok(Self::Creating),
            "prepared" => Ok(Self::Prepared),
            "exchanged" => Ok(Self::Exchanged),
            "retained" => Ok(Self::Retained),
            "conflict" => Ok(Self::Conflict),
            "failed" => Ok(Self::Failed),
            _ => Err(Error::corrupt(format!(
                "unknown replica install phase {value}"
            ))),
        }
    }
}

#[derive(Clone, Debug)]
pub(super) struct InstallRecord {
    pub id: String,
    pub path: String,
    pub parent: String,
    pub name: String,
    pub temporary: String,
    pub temporary_dev: Option<u64>,
    pub temporary_ino: Option<u64>,
    pub expected_digest: [u8; 32],
    pub expected_length: u64,
    pub expected_dev: u64,
    pub expected_ino: u64,
    pub parent_dev: u64,
    pub parent_ino: u64,
    pub replacement_digest: Option<[u8; 32]>,
    pub replacement_length: Option<u64>,
    pub remote_revision: Option<[u8; 24]>,
    pub artifact: String,
    pub phase: InstallPhase,
    pub error: Option<String>,
}

#[derive(Clone, Debug)]
pub(super) struct ArtifactRecord {
    pub id: String,
    pub origin: String,
    pub name: String,
    pub kind: String,
    pub reason: String,
    pub remote_revision: Option<[u8; 24]>,
    pub phase: String,
    pub error: Option<String>,
    pub digest: Option<[u8; 32]>,
    pub length: u64,
    pub stat: Option<ArtifactStat>,
    pub settle_pending: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct ArtifactStat {
    pub dev: u64,
    pub ino: u64,
    pub mtime: i64,
    pub mtime_nsec: i64,
    pub ctime: i64,
    pub ctime_nsec: i64,
}

#[derive(Clone, Debug)]
pub(super) struct BaselineNode {
    pub node: NodeId,
    pub directory: bool,
    pub executable: bool,
    pub digest: Option<[u8; 32]>,
    /// Remote revision at which this entry's accepted state was observed. It is
    /// the predicate revision for later staging and status; never a global
    /// latest revision substituted for the original observation.
    pub revision: Revision,
}

#[derive(Clone, Debug)]
pub(super) struct BaselineRecord {
    /// Global common baseline revision stored in `baseline_meta`.
    pub revision: Revision,
    pub remote: BTreeMap<String, BaselineNode>,
    pub local: LocalSnapshot,
    pub local_scan_hints: BTreeMap<String, LocalScanHint>,
}

#[derive(Clone, Debug)]
pub(super) struct LocalScanHint {
    pub node: LocalNode,
    pub recorded_ns: i64,
}

type LocalScanColumns = (
    Option<i64>,
    Option<i64>,
    Option<i64>,
    Option<i64>,
    Option<i64>,
    Option<i64>,
);

fn local_scan_columns(local: Option<&LocalNode>) -> Result<LocalScanColumns> {
    let Some(stat) = local.and_then(|node| node.stat) else {
        return Ok((None, None, None, None, None, None));
    };
    let recorded_ns = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| Error::invalid("record local scan time before the Unix epoch"))?
        .as_nanos();
    let recorded_ns = i64::try_from(recorded_ns)
        .map_err(|_| Error::invalid("local scan time exceeds SQLite range"))?;
    Ok((
        Some(
            i64::try_from(stat.size)
                .map_err(|_| Error::invalid("local stat size exceeds SQLite range"))?,
        ),
        Some(stat.mtime),
        Some(stat.mtime_nsec),
        Some(stat.ctime),
        Some(stat.ctime_nsec),
        Some(recorded_ns),
    ))
}

#[derive(Clone, Debug)]
pub(super) struct ConflictRecord {
    pub id: String,
    pub path: String,
    pub original_path: Option<String>,
    pub desired_path: Option<String>,
    pub local: String,
    pub remote: String,
    pub local_artifact: Option<String>,
    pub remote_node: Option<[u8; 16]>,
    pub remote_revision: Option<[u8; 24]>,
    pub error: String,
}

/// Directory ownership bookkeeping for one staged Sync edit. It records which
/// local path the edit belongs to (before dispatch) so a restart can retry only
/// associated work and keep orphaned staging unpublished.
#[derive(Clone, Debug)]
pub(super) struct WorkRecord {
    /// Monotonic insertion order, used to select the current request.
    pub seq: u64,
    pub id: [u8; 32],
    pub path: String,
    pub original_path: Option<String>,
    pub desired_path: Option<String>,
    pub operation: String,
    pub local_kind: Option<String>,
    pub local_digest: Option<[u8; 32]>,
    pub local_length: Option<u64>,
    pub local_executable: Option<bool>,
    pub local_dev: Option<u64>,
    pub local_ino: Option<u64>,
    pub local_parent_dev: Option<u64>,
    pub local_parent_ino: Option<u64>,
    pub baseline_node: Option<[u8; 16]>,
    pub baseline_revision: [u8; 24],
    pub remote_revision: Option<[u8; 24]>,
    /// Original commit identity, retained before a replay can clear it.
    pub commit_id: Option<[u8; 16]>,
    /// Conflict this edit resolves, if it was created by an explicit decision.
    pub conflict: Option<String>,
    /// Retained artifact holding the exact local input published by this edit.
    pub source_artifact: Option<String>,
    /// Retained copy of an ordinary local upload. It is released after the
    /// edit reaches a terminal receipt, but kept for Unknown/Retryable work.
    pub upload_artifact: Option<String>,
    pub state: String,
    pub error: Option<String>,
}

#[derive(Clone, Debug)]
struct Shape {
    initialized: bool,
    has_conflicts: bool,
    has_work: bool,
    baseline_remote_revision: bool,
    work_columns: BTreeSet<String>,
    work_missing: Vec<&'static str>,
}

pub(super) struct Store {
    connection: Connection,
    shape: Shape,
}

impl Store {
    pub fn open(path: &Path) -> Result<Self> {
        let exists = path
            .try_exists()
            .map_err(|e| Error::io("inspect", path, e))?;
        let mut connection = Connection::open_with_flags(
            path,
            if exists {
                rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
                    | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX
            } else {
                rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
                    | rusqlite::OpenFlags::SQLITE_OPEN_CREATE
                    | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX
            },
        )
        .map_err(|e| Error::io("open", path, e))?;
        connection
            .busy_timeout(std::time::Duration::from_secs(1))
            .map_err(|e| Error::io("configure replica timeout", path, e))?;
        let empty = schema::table_names(&connection)
            .map_err(|e| Error::io("read replica schema", path, e))?
            .is_empty();
        if empty {
            let transaction = connection
                .transaction()
                .map_err(|e| Error::io("begin replica initialization", path, e))?;
            transaction
                .execute_batch(CREATE_SCHEMA)
                .map_err(|e| Error::io("initialize replica state", path, e))?;
            transaction
                .commit()
                .map_err(|e| Error::io("commit replica initialization", path, e))?;
        } else {
            let shape = validate_profile(&connection)?;
            upgrade(&mut connection, &shape)?;
        }
        let shape = validate_profile(&connection)?;
        connection
            .execute_batch(
                "PRAGMA journal_mode=WAL;
                 PRAGMA synchronous=FULL;
                 PRAGMA foreign_keys=ON;",
            )
            .map_err(|e| Error::io("configure replica state", path, e))?;
        Ok(Self { connection, shape })
    }

    pub fn open_read_only(path: &Path) -> Result<Self> {
        use rusqlite::OpenFlags;
        let connection = Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .map_err(|e| Error::io("open", path, e))?;
        connection
            .busy_timeout(std::time::Duration::from_secs(1))
            .map_err(|e| Error::io("configure replica inspection", path, e))?;
        let shape = if schema::table_names(&connection)
            .map_err(|e| Error::io("read replica schema", path, e))?
            .is_empty()
        {
            Shape {
                initialized: false,
                has_conflicts: false,
                has_work: false,
                baseline_remote_revision: false,
                work_columns: BTreeSet::new(),
                work_missing: Vec::new(),
            }
        } else {
            validate_profile(&connection)?
        };
        connection
            .execute_batch("PRAGMA query_only=ON")
            .map_err(|e| Error::io("configure replica inspection", path, e))?;
        Ok(Self { connection, shape })
    }

    pub fn binding(&self) -> Result<Option<Binding>> {
        if !self.shape.initialized {
            return Ok(None);
        }
        self.connection
            .query_row(
                "SELECT filesystem,root,root_dev,root_ino,state_dev,state_ino
                 FROM binding WHERE singleton=1",
                [],
                |row| {
                    let filesystem: Vec<u8> = row.get(0)?;
                    let root_dev: Vec<u8> = row.get(2)?;
                    let root_ino: Vec<u8> = row.get(3)?;
                    let state_dev: Vec<u8> = row.get(4)?;
                    let state_ino: Vec<u8> = row.get(5)?;
                    Ok(Binding {
                        filesystem: NodeId::from_bytes(fixed(&filesystem, "filesystem")?),
                        root: row.get(1)?,
                        root_dev: number(&root_dev, "root device")?,
                        root_ino: number(&root_ino, "root inode")?,
                        state_dev: number(&state_dev, "state device")?,
                        state_ino: number(&state_ino, "state inode")?,
                    })
                },
            )
            .optional()
            .map_err(|e| Error::corrupt(format!("read replica binding: {e}")))
    }

    pub fn initialize_binding(&mut self, binding: &Binding) -> Result<bool> {
        let transaction = self
            .connection
            .transaction()
            .map_err(|e| Error::corrupt(format!("begin replica binding: {e}")))?;
        transaction
            .execute(
                "INSERT OR IGNORE INTO binding
                 VALUES(1,?1,?2,?3,?4,?5,?6,?7)",
                params![
                    PROFILE,
                    binding.filesystem.as_bytes().as_slice(),
                    binding.root,
                    binding.root_dev.to_be_bytes().as_slice(),
                    binding.root_ino.to_be_bytes().as_slice(),
                    binding.state_dev.to_be_bytes().as_slice(),
                    binding.state_ino.to_be_bytes().as_slice(),
                ],
            )
            .map_err(|e| Error::corrupt(format!("write replica binding: {e}")))?;
        let stored = transaction
            .query_row(
                "SELECT profile,filesystem,root,root_dev,root_ino,state_dev,state_ino
                 FROM binding WHERE singleton=1",
                [],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, Vec<u8>>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, Vec<u8>>(3)?,
                        row.get::<_, Vec<u8>>(4)?,
                        row.get::<_, Vec<u8>>(5)?,
                        row.get::<_, Vec<u8>>(6)?,
                    ))
                },
            )
            .map_err(|e| Error::corrupt(format!("read replica binding: {e}")))?;
        transaction
            .commit()
            .map_err(|e| Error::corrupt(format!("commit replica binding: {e}")))?;
        let matches = stored.0 == PROFILE
            && stored.1 == binding.filesystem.as_bytes()
            && stored.2 == binding.root
            && number(&stored.3, "root device")
                .map_err(|e| Error::corrupt(format!("decode root device: {e}")))?
                == binding.root_dev
            && number(&stored.4, "root inode")
                .map_err(|e| Error::corrupt(format!("decode root inode: {e}")))?
                == binding.root_ino
            && number(&stored.5, "state device")
                .map_err(|e| Error::corrupt(format!("decode state device: {e}")))?
                == binding.state_dev
            && number(&stored.6, "state inode")
                .map_err(|e| Error::corrupt(format!("decode state inode: {e}")))?
                == binding.state_ino;
        Ok(matches)
    }

    /// Move a restored database to the filesystem identities of the isolated
    /// root and state directories. Accepted fingerprints, commit identities
    /// and historical artifact digests stay unchanged.
    pub fn rebind(&mut self, binding: &Binding, identities: &IdentityMap) -> Result<()> {
        let transaction = self
            .connection
            .transaction()
            .map_err(|e| Error::corrupt(format!("begin replica rebind: {e}")))?;
        let updated = transaction
            .execute(
                "UPDATE binding SET
                   profile=?1,filesystem=?2,root=?3,root_dev=?4,root_ino=?5,
                   state_dev=?6,state_ino=?7
                 WHERE singleton=1",
                params![
                    PROFILE,
                    binding.filesystem.as_bytes().as_slice(),
                    binding.root,
                    binding.root_dev.to_be_bytes().as_slice(),
                    binding.root_ino.to_be_bytes().as_slice(),
                    binding.state_dev.to_be_bytes().as_slice(),
                    binding.state_ino.to_be_bytes().as_slice(),
                ],
            )
            .map_err(|e| Error::corrupt(format!("rebind replica: {e}")))?;
        if updated != 1 {
            return Err(Error::invalid(
                "sync state is not initialized; run sync init",
            ));
        }

        let baseline = {
            let mut statement = transaction
                .prepare(
                    "SELECT path,local_dev,local_ino,local_parent_dev,local_parent_ino
                     FROM baseline WHERE local_present=1",
                )
                .map_err(|e| Error::corrupt(format!("read baseline identities: {e}")))?;
            statement
                .query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, Option<Vec<u8>>>(1)?,
                        row.get::<_, Option<Vec<u8>>>(2)?,
                        row.get::<_, Option<Vec<u8>>>(3)?,
                        row.get::<_, Option<Vec<u8>>>(4)?,
                    ))
                })
                .map_err(|e| Error::corrupt(format!("read baseline identities: {e}")))?
                .collect::<rusqlite::Result<Vec<_>>>()
                .map_err(|e| Error::corrupt(format!("read baseline identities: {e}")))?
        };
        for (path, dev, ino, parent_dev, parent_ino) in baseline {
            let own = map_identity(identities, dev.as_deref(), ino.as_deref())?;
            let parent = map_identity(identities, parent_dev.as_deref(), parent_ino.as_deref())?;
            if own.is_none() && parent.is_none() {
                continue;
            }
            transaction
                .execute(
                    "UPDATE baseline SET
                       local_dev=?2,local_ino=?3,local_parent_dev=?4,local_parent_ino=?5
                     WHERE path=?1",
                    params![
                        path,
                        own.map(|value| value.0.to_be_bytes())
                            .as_ref()
                            .map(|value| value.as_slice())
                            .or(dev.as_deref()),
                        own.map(|value| value.1.to_be_bytes())
                            .as_ref()
                            .map(|value| value.as_slice())
                            .or(ino.as_deref()),
                        parent
                            .map(|parent| parent.0.to_be_bytes())
                            .as_ref()
                            .map(|value| value.as_slice())
                            .or(parent_dev.as_deref()),
                        parent
                            .map(|parent| parent.1.to_be_bytes())
                            .as_ref()
                            .map(|value| value.as_slice())
                            .or(parent_ino.as_deref()),
                    ],
                )
                .map_err(|e| Error::corrupt(format!("rebind baseline identity: {e}")))?;
        }

        let work = {
            let mut statement = transaction
                .prepare(
                    "SELECT id,local_dev,local_ino,local_parent_dev,local_parent_ino FROM work
                     WHERE local_kind IS NOT NULL",
                )
                .map_err(|e| Error::corrupt(format!("read work identities: {e}")))?;
            statement
                .query_map([], |row| {
                    Ok((
                        row.get::<_, Vec<u8>>(0)?,
                        row.get::<_, Option<Vec<u8>>>(1)?,
                        row.get::<_, Option<Vec<u8>>>(2)?,
                        row.get::<_, Option<Vec<u8>>>(3)?,
                        row.get::<_, Option<Vec<u8>>>(4)?,
                    ))
                })
                .map_err(|e| Error::corrupt(format!("read work identities: {e}")))?
                .collect::<rusqlite::Result<Vec<_>>>()
                .map_err(|e| Error::corrupt(format!("read work identities: {e}")))?
        };
        for (id, dev, ino, parent_dev, parent_ino) in work {
            let own = map_identity(identities, dev.as_deref(), ino.as_deref())?;
            let parent = map_identity(identities, parent_dev.as_deref(), parent_ino.as_deref())?;
            if own.is_none() && parent.is_none() {
                continue;
            }
            transaction
                .execute(
                    "UPDATE work SET
                       local_dev=?2,local_ino=?3,local_parent_dev=?4,local_parent_ino=?5
                     WHERE id=?1",
                    params![
                        id,
                        own.map(|value| value.0.to_be_bytes())
                            .as_ref()
                            .map(|value| value.as_slice())
                            .or(dev.as_deref()),
                        own.map(|value| value.1.to_be_bytes())
                            .as_ref()
                            .map(|value| value.as_slice())
                            .or(ino.as_deref()),
                        parent
                            .map(|parent| parent.0.to_be_bytes())
                            .as_ref()
                            .map(|value| value.as_slice())
                            .or(parent_dev.as_deref()),
                        parent
                            .map(|parent| parent.1.to_be_bytes())
                            .as_ref()
                            .map(|value| value.as_slice())
                            .or(parent_ino.as_deref()),
                    ],
                )
                .map_err(|e| Error::corrupt(format!("rebind work identity: {e}")))?;
        }

        let installs = {
            let mut statement = transaction
                .prepare(
                    "SELECT id,expected_dev,expected_ino,parent_dev,parent_ino,
                            temporary_dev,temporary_ino
                     FROM installs",
                )
                .map_err(|e| Error::corrupt(format!("read install identities: {e}")))?;
            statement
                .query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, Vec<u8>>(1)?,
                        row.get::<_, Vec<u8>>(2)?,
                        row.get::<_, Vec<u8>>(3)?,
                        row.get::<_, Vec<u8>>(4)?,
                        row.get::<_, Option<Vec<u8>>>(5)?,
                        row.get::<_, Option<Vec<u8>>>(6)?,
                    ))
                })
                .map_err(|e| Error::corrupt(format!("read install identities: {e}")))?
                .collect::<rusqlite::Result<Vec<_>>>()
                .map_err(|e| Error::corrupt(format!("read install identities: {e}")))?
        };
        for (id, expected_dev, expected_ino, parent_dev, parent_ino, temp_dev, temp_ino) in installs
        {
            let expected = map_identity(identities, Some(&expected_dev), Some(&expected_ino))?;
            let parent = map_identity(identities, Some(&parent_dev), Some(&parent_ino))?;
            let temporary = map_identity(identities, temp_dev.as_deref(), temp_ino.as_deref())?;
            let expected_dev = expected
                .map(|value| value.0.to_be_bytes().to_vec())
                .unwrap_or(expected_dev);
            let expected_ino = expected
                .map(|value| value.1.to_be_bytes().to_vec())
                .unwrap_or(expected_ino);
            let parent_dev = parent
                .map(|value| value.0.to_be_bytes().to_vec())
                .unwrap_or(parent_dev);
            let parent_ino = parent
                .map(|value| value.1.to_be_bytes().to_vec())
                .unwrap_or(parent_ino);
            let temporary_dev = temporary
                .map(|value| value.0.to_be_bytes().to_vec())
                .or(temp_dev);
            let temporary_ino = temporary
                .map(|value| value.1.to_be_bytes().to_vec())
                .or(temp_ino);
            transaction
                .execute(
                    "UPDATE installs SET
                       expected_dev=?2,expected_ino=?3,parent_dev=?4,parent_ino=?5,
                       temporary_dev=?6,temporary_ino=?7
                     WHERE id=?1",
                    params![
                        id,
                        expected_dev.as_slice(),
                        expected_ino.as_slice(),
                        parent_dev.as_slice(),
                        parent_ino.as_slice(),
                        temporary_dev.as_deref(),
                        temporary_ino.as_deref(),
                    ],
                )
                .map_err(|e| Error::corrupt(format!("rebind install identity: {e}")))?;
        }

        transaction
            .commit()
            .map_err(|e| Error::corrupt(format!("commit replica rebind: {e}")))?;
        Ok(())
    }

    #[allow(dead_code)]
    pub fn save_baseline(
        &mut self,
        revision: Revision,
        remote: &BTreeMap<String, Node>,
        local: &LocalSnapshot,
    ) -> Result<()> {
        let transaction = self
            .connection
            .transaction()
            .map_err(|e| Error::corrupt(format!("begin baseline: {e}")))?;
        transaction
            .execute("DELETE FROM baseline", [])
            .map_err(|e| Error::corrupt(format!("clear baseline: {e}")))?;
        for (path, node) in remote {
            let (directory, length, digest) = match node.kind() {
                NodeKind::Directory { .. } => (true, 0, None),
                NodeKind::File(file) => (
                    false,
                    file.content_id().length(),
                    Some(*file.content_id().digest()),
                ),
            };
            let (
                present,
                local_digest,
                local_length,
                local_mode,
                local_dev,
                local_ino,
                parent_dev,
                parent_ino,
            ) = match local.get(path) {
                Some(entry) => (
                    1_i64,
                    match entry.kind {
                        LocalKind::File { digest, .. } => Some(*digest.as_bytes()),
                        LocalKind::Directory => None,
                    },
                    match entry.kind {
                        LocalKind::File { length, .. } => {
                            Some(i64::try_from(length).map_err(|_| {
                                Error::invalid("local baseline length exceeds SQLite range")
                            })?)
                        }
                        LocalKind::Directory => None,
                    },
                    entry.executable,
                    Some(entry.identity.dev.to_be_bytes()),
                    Some(entry.identity.ino.to_be_bytes()),
                    entry.parent.map(|value| value.dev.to_be_bytes()),
                    entry.parent.map(|value| value.ino.to_be_bytes()),
                ),
                None => (
                    0_i64,
                    None::<[u8; 32]>,
                    None::<i64>,
                    false,
                    None::<[u8; 8]>,
                    None::<[u8; 8]>,
                    None::<[u8; 8]>,
                    None::<[u8; 8]>,
                ),
            };
            let (
                stat_size,
                stat_mtime,
                stat_mtime_nsec,
                stat_ctime,
                stat_ctime_nsec,
                stat_recorded_ns,
            ) = local_scan_columns(local.get(path))?;
            transaction
                .execute(
                    "INSERT INTO baseline
                     (path,node,directory,executable,remote_digest,remote_length,
                      local_present,local_digest,local_length,local_executable,
                      local_dev,local_ino,local_parent_dev,local_parent_ino,
                      local_stat_size,local_stat_mtime,local_stat_mtime_nsec,local_stat_ctime,
                      local_stat_ctime_nsec,local_stat_recorded_ns,remote_revision)
                     VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,
                            ?16,?17,?18,?19,?20,?21)",
                    params![
                        path,
                        node.id().as_bytes().as_slice(),
                        directory,
                        node.executable(),
                        digest.as_ref().map(|value| value.as_slice()),
                        i64::try_from(length).map_err(|_| Error::invalid(
                            "remote baseline length exceeds SQLite range"
                        ))?,
                        present,
                        local_digest.as_ref().map(|value| value.as_slice()),
                        local_length,
                        local_mode,
                        local_dev,
                        local_ino,
                        parent_dev,
                        parent_ino,
                        stat_size,
                        stat_mtime,
                        stat_mtime_nsec,
                        stat_ctime,
                        stat_ctime_nsec,
                        stat_recorded_ns,
                        revision.to_bytes().as_slice(),
                    ],
                )
                .map_err(|e| Error::corrupt(format!("write baseline: {e}")))?;
        }
        transaction
            .execute("DELETE FROM baseline_meta", [])
            .map_err(|e| Error::corrupt(format!("clear baseline revision: {e}")))?;
        transaction
            .execute(
                "INSERT INTO baseline_meta VALUES(1,?1)",
                params![revision.to_bytes().as_slice()],
            )
            .map_err(|e| Error::corrupt(format!("write baseline revision: {e}")))?;
        transaction
            .commit()
            .map_err(|e| Error::corrupt(format!("commit baseline: {e}")))?;
        Ok(())
    }

    pub fn save_baseline_path(
        &mut self,
        path: &str,
        node: &Node,
        local: Option<&LocalNode>,
        revision: Revision,
    ) -> Result<()> {
        let (directory, length, digest) = match node.kind() {
            NodeKind::Directory { .. } => (true, 0, None),
            NodeKind::File(file) => (
                false,
                file.content_id().length(),
                Some(*file.content_id().digest()),
            ),
        };
        self.save_baseline_entry(
            path,
            node.id(),
            directory,
            node.executable(),
            digest,
            length,
            local,
            revision,
        )
    }

    /// Write one baseline entry with explicit accepted remote facts. A
    /// namespace-only receipt advances path/revision but keeps the previously
    /// accepted content digest and executable bit.
    #[allow(clippy::too_many_arguments)]
    pub fn save_baseline_entry(
        &mut self,
        path: &str,
        node: NodeId,
        directory: bool,
        executable: bool,
        digest: Option<[u8; 32]>,
        length: u64,
        local: Option<&LocalNode>,
        revision: Revision,
    ) -> Result<()> {
        let (
            present,
            local_digest,
            local_length,
            local_mode,
            local_dev,
            local_ino,
            parent_dev,
            parent_ino,
        ) = match local {
            Some(entry) => (
                1_i64,
                match entry.kind {
                    LocalKind::File { digest, .. } => Some(*digest.as_bytes()),
                    LocalKind::Directory => None,
                },
                match entry.kind {
                    LocalKind::File { length, .. } => {
                        Some(i64::try_from(length).map_err(|_| {
                            Error::invalid("local baseline length exceeds SQLite range")
                        })?)
                    }
                    LocalKind::Directory => None,
                },
                Some(entry.executable),
                Some(entry.identity.dev.to_be_bytes()),
                Some(entry.identity.ino.to_be_bytes()),
                entry.parent.map(|value| value.dev.to_be_bytes()),
                entry.parent.map(|value| value.ino.to_be_bytes()),
            ),
            None => (0_i64, None, None, None, None, None, None, None),
        };
        let local_with_stat;
        let scan_source = match local {
            Some(node) if node.stat.is_none() => {
                let mut node = node.clone();
                node.stat = self.local_scan_hint(path)?.and_then(|hint| {
                    if hint.node.identity == node.identity && hint.node.kind == node.kind {
                        hint.node.stat
                    } else {
                        None
                    }
                });
                local_with_stat = node;
                Some(&local_with_stat)
            }
            _ => local,
        };
        let (stat_size, stat_mtime, stat_mtime_nsec, stat_ctime, stat_ctime_nsec, stat_recorded_ns) =
            local_scan_columns(scan_source)?;
        self.connection
            .execute(
                "INSERT OR REPLACE INTO baseline
                 (path,node,directory,executable,remote_digest,remote_length,
                  local_present,local_digest,local_length,local_executable,
                  local_dev,local_ino,local_parent_dev,local_parent_ino,
                  local_stat_size,local_stat_mtime,local_stat_mtime_nsec,local_stat_ctime,
                  local_stat_ctime_nsec,local_stat_recorded_ns,remote_revision)
                 VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,
                        ?16,?17,?18,?19,?20,?21)",
                params![
                    path,
                    node.as_bytes().as_slice(),
                    directory,
                    executable,
                    digest.as_ref().map(|value| value.as_slice()),
                    i64::try_from(length).map_err(|_| Error::invalid(
                        "remote baseline length exceeds SQLite range"
                    ))?,
                    present,
                    local_digest.as_ref().map(|value| value.as_slice()),
                    local_length,
                    local_mode,
                    local_dev,
                    local_ino,
                    parent_dev,
                    parent_ino,
                    stat_size,
                    stat_mtime,
                    stat_mtime_nsec,
                    stat_ctime,
                    stat_ctime_nsec,
                    stat_recorded_ns,
                    revision.to_bytes().as_slice(),
                ],
            )
            .map_err(|e| Error::corrupt(format!("write baseline entry: {e}")))?;
        Ok(())
    }

    pub fn remove_baseline_path(&mut self, path: &str) -> Result<()> {
        self.connection
            .execute("DELETE FROM baseline WHERE path=?1", params![path])
            .map_err(|e| Error::corrupt(format!("remove baseline entry: {e}")))?;
        Ok(())
    }

    /// Remove a baseline directory entry and every entry below it, used when a
    /// whole unchanged subtree is quarantined as one unit.
    pub fn remove_baseline_subtree(&mut self, path: &str) -> Result<()> {
        self.connection
            .execute(
                "DELETE FROM baseline
                 WHERE path=?1 OR substr(path,1,length(?1)+1)=?1||'/'",
                params![path],
            )
            .map_err(|e| Error::corrupt(format!("remove baseline subtree: {e}")))?;
        Ok(())
    }

    pub fn set_baseline_revision(&mut self, revision: Revision) -> Result<()> {
        self.connection
            .execute(
                "INSERT OR REPLACE INTO baseline_meta VALUES(1,?1)",
                params![revision.to_bytes().as_slice()],
            )
            .map_err(|e| Error::corrupt(format!("write baseline revision: {e}")))?;
        Ok(())
    }

    pub fn load_baseline(&self) -> Result<Option<BaselineRecord>> {
        if !self.shape.initialized {
            return Ok(None);
        }
        let revision: Option<Vec<u8>> = self
            .connection
            .query_row(
                "SELECT revision FROM baseline_meta WHERE singleton=1",
                [],
                |row| row.get(0),
            )
            .optional()
            .map_err(|e| Error::corrupt(format!("read baseline revision: {e}")))?;
        let Some(revision) = revision else {
            return Ok(None);
        };
        let global_revision: [u8; 24] = fixed(&revision, "baseline revision")
            .map_err(|e| Error::corrupt(format!("decode baseline revision: {e}")))?;
        let mut remote = BTreeMap::new();
        let mut local = BTreeMap::new();
        let mut local_scan_hints = BTreeMap::new();
        let remote_revision = if self.shape.baseline_remote_revision {
            "remote_revision"
        } else {
            "NULL AS remote_revision"
        };
        let mut statement = self
            .connection
            .prepare(&format!(
                "SELECT path,node,directory,executable,remote_digest,remote_length,
                        local_present,local_digest,local_length,local_executable,
                        local_dev,local_ino,local_parent_dev,local_parent_ino,
                        local_stat_size,local_stat_mtime,local_stat_mtime_nsec,
                        local_stat_ctime,local_stat_ctime_nsec,local_stat_recorded_ns,
                        {remote_revision}
                 FROM baseline ORDER BY path"
            ))
            .map_err(|e| Error::corrupt(format!("read baseline: {e}")))?;
        let rows = statement
            .query_map([], |row| -> rusqlite::Result<_> {
                let node: Vec<u8> = row.get(1)?;
                let remote_digest: Option<Vec<u8>> = row.get(4)?;
                let remote_length: Option<i64> = row.get(5)?;
                let local_digest: Option<Vec<u8>> = row.get(7)?;
                let local_length: Option<i64> = row.get(8)?;
                let local_dev: Option<Vec<u8>> = row.get(10)?;
                let local_ino: Option<Vec<u8>> = row.get(11)?;
                let local_parent_dev: Option<Vec<u8>> = row.get(12)?;
                let local_parent_ino: Option<Vec<u8>> = row.get(13)?;
                let stat_size: Option<i64> = row.get(14)?;
                let stat_mtime: Option<i64> = row.get(15)?;
                let stat_mtime_nsec: Option<i64> = row.get(16)?;
                let stat_ctime: Option<i64> = row.get(17)?;
                let stat_ctime_nsec: Option<i64> = row.get(18)?;
                let stat_recorded_ns: Option<i64> = row.get(19)?;
                let revision: Option<Vec<u8>> = row.get(20)?;
                Ok((
                    row.get::<_, String>(0)?,
                    NodeId::from_bytes(fixed(&node, "baseline node")?),
                    row.get::<_, bool>(2)?,
                    row.get::<_, bool>(3)?,
                    remote_digest
                        .as_deref()
                        .map(|value| fixed(value, "baseline remote digest"))
                        .transpose()?,
                    remote_length
                        .map(|value| {
                            u64::try_from(value).map_err(|_| corrupt_sql("negative remote length"))
                        })
                        .transpose()?,
                    row.get::<_, bool>(6)?,
                    local_digest
                        .as_deref()
                        .map(|value| fixed(value, "baseline local digest"))
                        .transpose()?,
                    local_length
                        .map(|value| {
                            u64::try_from(value).map_err(|_| corrupt_sql("negative local length"))
                        })
                        .transpose()?,
                    row.get::<_, Option<bool>>(9)?,
                    local_dev
                        .as_deref()
                        .map(|value| number(value, "baseline local device"))
                        .transpose()?,
                    local_ino
                        .as_deref()
                        .map(|value| number(value, "baseline local inode"))
                        .transpose()?,
                    local_parent_dev
                        .as_deref()
                        .map(|value| number(value, "baseline parent device"))
                        .transpose()?,
                    local_parent_ino
                        .as_deref()
                        .map(|value| number(value, "baseline parent inode"))
                        .transpose()?,
                    stat_size,
                    stat_mtime,
                    stat_mtime_nsec,
                    stat_ctime,
                    stat_ctime_nsec,
                    stat_recorded_ns,
                    revision
                        .as_deref()
                        .map(|value| fixed(value, "baseline entry revision"))
                        .transpose()?
                        .map(Revision::from_bytes)
                        .unwrap_or_else(|| Revision::from_bytes(global_revision)),
                ))
            })
            .map_err(|e| Error::corrupt(format!("read baseline: {e}")))?;
        for row in rows {
            let (
                path,
                node,
                directory,
                executable,
                remote_digest,
                _remote_length,
                local_present,
                local_digest,
                local_length,
                local_executable,
                local_dev,
                local_ino,
                local_parent_dev,
                local_parent_ino,
                stat_size,
                stat_mtime,
                stat_mtime_nsec,
                stat_ctime,
                stat_ctime_nsec,
                stat_recorded_ns,
                revision,
            ) = row.map_err(|e| Error::corrupt(format!("read baseline: {e}")))?;
            remote.insert(
                path.clone(),
                BaselineNode {
                    node,
                    directory,
                    executable,
                    digest: remote_digest,
                    revision,
                },
            );
            if local_present {
                let kind = if directory {
                    LocalKind::Directory
                } else {
                    LocalKind::File {
                        length: local_length
                            .ok_or_else(|| Error::corrupt("local baseline file lacks a length"))?,
                        digest: ContentHash::from_bytes(
                            local_digest.ok_or_else(|| {
                                Error::corrupt("local baseline file lacks a digest")
                            })?,
                        ),
                    }
                };
                let identity = Identity {
                    dev: local_dev
                        .ok_or_else(|| Error::corrupt("local baseline entry lacks a device"))?,
                    ino: local_ino
                        .ok_or_else(|| Error::corrupt("local baseline entry lacks an inode"))?,
                };
                let stat = match (
                    stat_size,
                    stat_mtime,
                    stat_mtime_nsec,
                    stat_ctime,
                    stat_ctime_nsec,
                ) {
                    (Some(size), Some(mtime), Some(mtime_nsec), Some(ctime), Some(ctime_nsec)) => {
                        Some(LocalStat {
                            dev: identity.dev,
                            ino: identity.ino,
                            size: u64::try_from(size)
                                .map_err(|_| Error::corrupt("negative local stat size"))?,
                            mtime,
                            mtime_nsec,
                            ctime,
                            ctime_nsec,
                        })
                    }
                    (None, None, None, None, None) => None,
                    _ => return Err(Error::corrupt("incomplete local scan stat evidence")),
                };
                let local_node = LocalNode {
                    path: path.clone(),
                    kind,
                    executable: local_executable.unwrap_or(false),
                    identity,
                    parent: match (local_parent_dev, local_parent_ino) {
                        (Some(dev), Some(ino)) => Some(Identity { dev, ino }),
                        _ => None,
                    },
                    stat,
                };
                if let (Some(_), Some(recorded_ns)) = (local_node.stat, stat_recorded_ns) {
                    local_scan_hints.insert(
                        path.clone(),
                        LocalScanHint {
                            node: local_node.clone(),
                            recorded_ns,
                        },
                    );
                }
                local.insert(path.clone(), local_node);
            }
        }
        Ok(Some(BaselineRecord {
            revision: Revision::from_bytes(global_revision),
            remote,
            local: LocalSnapshot::from_nodes(local),
            local_scan_hints,
        }))
    }

    pub fn local_scan_hint(&self, path: &str) -> Result<Option<LocalScanHint>> {
        if !self.shape.initialized {
            return Ok(None);
        }
        type RawHint = (
            bool,
            Option<Vec<u8>>,
            Option<i64>,
            Option<bool>,
            Option<Vec<u8>>,
            Option<Vec<u8>>,
            Option<Vec<u8>>,
            Option<Vec<u8>>,
            Option<i64>,
            Option<i64>,
            Option<i64>,
            Option<i64>,
            Option<i64>,
            Option<i64>,
        );
        let row: Option<RawHint> = self
            .connection
            .query_row(
                "SELECT directory,local_digest,local_length,local_executable,
                        local_dev,local_ino,local_parent_dev,local_parent_ino,
                        local_stat_size,local_stat_mtime,local_stat_mtime_nsec,
                        local_stat_ctime,local_stat_ctime_nsec,local_stat_recorded_ns
                 FROM baseline WHERE path=?1 AND local_present=1",
                params![path],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                        row.get(6)?,
                        row.get(7)?,
                        row.get(8)?,
                        row.get(9)?,
                        row.get(10)?,
                        row.get(11)?,
                        row.get(12)?,
                        row.get(13)?,
                    ))
                },
            )
            .optional()
            .map_err(|e| Error::corrupt(format!("read local scan evidence: {e}")))?;
        let Some((
            directory,
            local_digest,
            local_length,
            local_executable,
            local_dev,
            local_ino,
            local_parent_dev,
            local_parent_ino,
            stat_size,
            stat_mtime,
            stat_mtime_nsec,
            stat_ctime,
            stat_ctime_nsec,
            stat_recorded_ns,
        )) = row
        else {
            return Ok(None);
        };
        if directory {
            return Ok(None);
        }
        let digest = local_digest
            .as_deref()
            .ok_or_else(|| Error::corrupt("local scan evidence lacks a digest"))
            .and_then(|value| fixed(value, "local scan digest"))?;
        let length = local_length
            .ok_or_else(|| Error::corrupt("local scan evidence lacks a length"))
            .and_then(|value| {
                u64::try_from(value).map_err(|_| Error::corrupt("negative local scan length"))
            })?;
        let dev = local_dev
            .as_deref()
            .ok_or_else(|| Error::corrupt("local scan evidence lacks a device"))
            .and_then(|value| number(value, "local scan device"))?;
        let ino = local_ino
            .as_deref()
            .ok_or_else(|| Error::corrupt("local scan evidence lacks an inode"))
            .and_then(|value| number(value, "local scan inode"))?;
        let parent = match (local_parent_dev, local_parent_ino) {
            (Some(dev), Some(ino)) => Some(Identity {
                dev: number(&dev, "local scan parent device")?,
                ino: number(&ino, "local scan parent inode")?,
            }),
            (None, None) => None,
            _ => return Err(Error::corrupt("incomplete local scan parent identity")),
        };
        let stat = match (
            stat_size,
            stat_mtime,
            stat_mtime_nsec,
            stat_ctime,
            stat_ctime_nsec,
        ) {
            (Some(size), Some(mtime), Some(mtime_nsec), Some(ctime), Some(ctime_nsec)) => {
                Some(LocalStat {
                    dev,
                    ino,
                    size: u64::try_from(size)
                        .map_err(|_| Error::corrupt("negative local scan stat size"))?,
                    mtime,
                    mtime_nsec,
                    ctime,
                    ctime_nsec,
                })
            }
            (None, None, None, None, None) => None,
            _ => return Err(Error::corrupt("incomplete local scan stat evidence")),
        };
        Ok(Some(LocalScanHint {
            node: LocalNode {
                path: path.to_owned(),
                kind: LocalKind::File {
                    length,
                    digest: ContentHash::from_bytes(digest),
                },
                executable: local_executable.unwrap_or(false),
                identity: Identity { dev, ino },
                parent,
                stat,
            },
            recorded_ns: stat_recorded_ns
                .ok_or_else(|| Error::corrupt("local scan evidence lacks a timestamp"))?,
        }))
    }

    pub fn save_local_scan_evidence(&self, node: &LocalNode) -> Result<()> {
        let LocalKind::File { digest, .. } = node.kind else {
            return Ok(());
        };
        let (size, mtime, mtime_nsec, ctime, ctime_nsec, recorded_ns) =
            local_scan_columns(Some(node))?;
        self.connection
            .execute(
                "UPDATE baseline SET
                   local_stat_size=?2,local_stat_mtime=?3,local_stat_mtime_nsec=?4,
                   local_stat_ctime=?5,local_stat_ctime_nsec=?6,local_stat_recorded_ns=?7
                 WHERE path=?1 AND local_present=1 AND local_digest=?8",
                params![
                    node.path,
                    size,
                    mtime,
                    mtime_nsec,
                    ctime,
                    ctime_nsec,
                    recorded_ns,
                    digest.as_bytes().as_slice()
                ],
            )
            .map_err(|e| Error::corrupt(format!("write local scan evidence: {e}")))?;
        Ok(())
    }

    pub fn insert_conflict(&mut self, conflict: &ConflictRecord) -> Result<()> {
        self.connection
            .execute(
                "INSERT INTO conflicts VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",
                params![
                    conflict.id,
                    conflict.path,
                    conflict.original_path,
                    conflict.desired_path,
                    conflict.local,
                    conflict.remote,
                    conflict.local_artifact,
                    conflict.remote_node.as_ref().map(|value| value.as_slice()),
                    conflict
                        .remote_revision
                        .as_ref()
                        .map(|value| value.as_slice()),
                    conflict.error,
                ],
            )
            .map_err(|e| Error::corrupt(format!("write conflict: {e}")))?;
        Ok(())
    }

    pub fn conflicts(&self) -> Result<Vec<ConflictRecord>> {
        if !self.shape.has_conflicts {
            return Ok(Vec::new());
        }
        let mut query = self
            .connection
            .prepare(
                "SELECT id,path,original_path,desired_path,local,remote,local_artifact,
                        remote_node,remote_revision,error
                 FROM conflicts ORDER BY path,id",
            )
            .map_err(|e| Error::corrupt(format!("read conflicts: {e}")))?;
        query
            .query_map([], |row| {
                let remote_node: Option<Vec<u8>> = row.get(7)?;
                let remote_revision: Option<Vec<u8>> = row.get(8)?;
                Ok(ConflictRecord {
                    id: row.get(0)?,
                    path: row.get(1)?,
                    original_path: row.get(2)?,
                    desired_path: row.get(3)?,
                    local: row.get(4)?,
                    remote: row.get(5)?,
                    local_artifact: row.get(6)?,
                    remote_node: remote_node
                        .as_deref()
                        .map(|value| fixed(value, "conflict remote node"))
                        .transpose()?,
                    remote_revision: remote_revision
                        .as_deref()
                        .map(|value| fixed(value, "conflict remote revision"))
                        .transpose()?,
                    error: row.get(9)?,
                })
            })
            .map_err(|e| Error::corrupt(format!("read conflicts: {e}")))?
            .map(|row| row.map_err(|e| Error::corrupt(format!("read conflict: {e}"))))
            .collect()
    }

    pub fn remove_conflict(&mut self, id: &str) -> Result<()> {
        self.connection
            .execute("DELETE FROM conflicts WHERE id=?1", params![id])
            .map_err(|e| Error::corrupt(format!("remove conflict: {e}")))?;
        Ok(())
    }

    /// Retire work that belonged to a resolved conflict while keeping its
    /// history for audit; a later request at the path becomes the current one.
    pub fn supersede_conflict_work(&mut self, conflict: &str) -> Result<()> {
        self.connection
            .execute(
                "UPDATE work SET state='superseded' WHERE conflict=?1 AND state!='committed'",
                params![conflict],
            )
            .map_err(|e| Error::corrupt(format!("supersede conflict work: {e}")))?;
        Ok(())
    }

    /// Refresh volatile evidence of an active conflict without replacing its
    /// retained local artifact or changing its stable identity.
    pub fn refresh_conflict(&mut self, conflict: &ConflictRecord) -> Result<()> {
        self.connection
            .execute(
                "UPDATE conflicts SET
                   path=?2,original_path=?3,desired_path=?4,local=?5,remote=?6,
                   remote_node=?7,remote_revision=?8,error=?9
                 WHERE id=?1",
                params![
                    conflict.id,
                    conflict.path,
                    conflict.original_path,
                    conflict.desired_path,
                    conflict.local,
                    conflict.remote,
                    conflict.remote_node.as_ref().map(|value| value.as_slice()),
                    conflict
                        .remote_revision
                        .as_ref()
                        .map(|value| value.as_slice()),
                    conflict.error,
                ],
            )
            .map_err(|e| Error::corrupt(format!("refresh conflict: {e}")))?;
        Ok(())
    }

    /// Persist the directory association for a staged edit before dispatch.
    pub fn insert_work(&mut self, work: &WorkRecord) -> Result<()> {
        self.connection
            .execute(
                "INSERT OR REPLACE INTO work
                 (seq,id,path,original_path,desired_path,operation,local_kind,local_digest,
                  local_length,local_executable,local_dev,local_ino,local_parent_dev,
                  local_parent_ino,baseline_node,baseline_revision,remote_revision,
                  commit_id,conflict,source_artifact,upload_artifact,state,error)
                 VALUES((SELECT COALESCE(MAX(seq),0)+1 FROM work),
                        ?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,
                        ?16,?17,?18,?19,?20,?21,?22)",
                params![
                    work.id.as_slice(),
                    work.path,
                    work.original_path,
                    work.desired_path,
                    work.operation,
                    work.local_kind,
                    work.local_digest.as_ref().map(|v| v.as_slice()),
                    work.local_length
                        .map(i64::try_from)
                        .transpose()
                        .map_err(|_| Error::invalid("work length exceeds SQLite range"))?,
                    work.local_executable,
                    work.local_dev.map(|v| v.to_be_bytes()),
                    work.local_ino.map(|v| v.to_be_bytes()),
                    work.local_parent_dev.map(|v| v.to_be_bytes()),
                    work.local_parent_ino.map(|v| v.to_be_bytes()),
                    work.baseline_node.as_ref().map(|v| v.as_slice()),
                    work.baseline_revision.as_slice(),
                    work.remote_revision.as_ref().map(|v| v.as_slice()),
                    work.commit_id.as_ref().map(|v| v.as_slice()),
                    work.conflict,
                    work.source_artifact,
                    work.upload_artifact,
                    work.state,
                    work.error,
                ],
            )
            .map_err(|e| Error::corrupt(format!("write work association: {e}")))?;
        self.connection
            .execute(
                "DELETE FROM work
                 WHERE path=?1 AND id!=?2 AND state IN ('committed','superseded')",
                params![work.path, work.id.as_slice()],
            )
            .map_err(|e| Error::corrupt(format!("prune terminal work: {e}")))?;
        Ok(())
    }

    pub fn update_work(
        &mut self,
        id: &[u8; 32],
        state: &str,
        remote_revision: Option<Revision>,
        error: Option<&str>,
    ) -> Result<()> {
        self.connection
            .execute(
                "UPDATE work SET state=?2,remote_revision=?3,error=?4 WHERE id=?1",
                params![
                    id.as_slice(),
                    state,
                    remote_revision.map(|value| value.to_bytes().to_vec()),
                    error,
                ],
            )
            .map_err(|e| Error::corrupt(format!("update work association: {e}")))?;
        Ok(())
    }

    pub fn remove_work(&mut self, id: &[u8; 32]) -> Result<()> {
        self.connection
            .execute("DELETE FROM work WHERE id=?1", params![id.as_slice()])
            .map_err(|e| Error::corrupt(format!("remove work association: {e}")))?;
        Ok(())
    }

    /// Persist the original commit identity of an edit before a replay can
    /// clear it from the staging handle.
    pub fn set_work_commit_id(&mut self, id: &[u8; 32], commit_id: [u8; 16]) -> Result<()> {
        self.connection
            .execute(
                "UPDATE work SET commit_id=?2 WHERE id=?1",
                params![id.as_slice(), commit_id.as_slice()],
            )
            .map_err(|e| Error::corrupt(format!("set work commit id: {e}")))?;
        Ok(())
    }

    pub fn work(&self) -> Result<Vec<WorkRecord>> {
        if !self.shape.has_work {
            return Ok(Vec::new());
        }
        let projection = WORK_COLUMNS
            .iter()
            .map(|column| {
                if self.shape.work_columns.contains(*column) {
                    (*column).to_owned()
                } else if *column == "seq" {
                    "0 AS seq".to_owned()
                } else {
                    format!("NULL AS {column}")
                }
            })
            .collect::<Vec<_>>()
            .join(",");
        let mut query = self
            .connection
            .prepare(&format!("SELECT {projection} FROM work ORDER BY path,id"))
            .map_err(|e| Error::corrupt(format!("read work associations: {e}")))?;
        query
            .query_map([], |row| {
                let id: Vec<u8> = row.get(1)?;
                let local_digest: Option<Vec<u8>> = row.get(7)?;
                let local_length: Option<i64> = row.get(8)?;
                let local_dev: Option<Vec<u8>> = row.get(10)?;
                let local_ino: Option<Vec<u8>> = row.get(11)?;
                let local_parent_dev: Option<Vec<u8>> = row.get(12)?;
                let local_parent_ino: Option<Vec<u8>> = row.get(13)?;
                let baseline_node: Option<Vec<u8>> = row.get(14)?;
                let baseline_revision: Vec<u8> = row.get(15)?;
                let remote_revision: Option<Vec<u8>> = row.get(16)?;
                let commit_id: Option<Vec<u8>> = row.get(17)?;
                Ok(WorkRecord {
                    seq: u64::try_from(row.get::<_, i64>(0)?)
                        .map_err(|_| corrupt_sql("negative work sequence"))?,
                    id: fixed(&id, "work id")?,
                    path: row.get(2)?,
                    original_path: row.get(3)?,
                    desired_path: row.get(4)?,
                    operation: row.get(5)?,
                    local_kind: row.get(6)?,
                    local_digest: local_digest
                        .as_deref()
                        .map(|value| fixed(value, "work local digest"))
                        .transpose()?,
                    local_length: local_length
                        .map(|value| {
                            u64::try_from(value).map_err(|_| corrupt_sql("negative work length"))
                        })
                        .transpose()?,
                    local_executable: row.get(9)?,
                    local_dev: local_dev
                        .as_deref()
                        .map(|value| number(value, "work local device"))
                        .transpose()?,
                    local_ino: local_ino
                        .as_deref()
                        .map(|value| number(value, "work local inode"))
                        .transpose()?,
                    local_parent_dev: local_parent_dev
                        .as_deref()
                        .map(|value| number(value, "work parent device"))
                        .transpose()?,
                    local_parent_ino: local_parent_ino
                        .as_deref()
                        .map(|value| number(value, "work parent inode"))
                        .transpose()?,
                    baseline_node: baseline_node
                        .as_deref()
                        .map(|value| fixed(value, "work baseline node"))
                        .transpose()?,
                    baseline_revision: fixed(&baseline_revision, "work baseline revision")?,
                    remote_revision: remote_revision
                        .as_deref()
                        .map(|value| fixed(value, "work remote revision"))
                        .transpose()?,
                    commit_id: commit_id
                        .as_deref()
                        .map(|value| fixed(value, "work commit id"))
                        .transpose()?,
                    conflict: row.get(18)?,
                    source_artifact: row.get(19)?,
                    upload_artifact: row.get(20)?,
                    state: row.get(21)?,
                    error: row.get(22)?,
                })
            })
            .map_err(|e| Error::corrupt(format!("read work associations: {e}")))?
            .map(|row| row.map_err(|e| Error::corrupt(format!("read work association: {e}"))))
            .collect()
    }

    pub fn insert_artifact(&mut self, artifact: &ArtifactRecord) -> Result<()> {
        self.connection
            .execute(
                "INSERT INTO artifacts
                 (id,origin,name,kind,reason,remote_revision,phase,error,digest,length,
                  stat_dev,stat_ino,stat_mtime,stat_mtime_nsec,stat_ctime,stat_ctime_nsec,
                  settle_pending)
                 VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17)",
                params![
                    artifact.id,
                    artifact.origin,
                    artifact.name,
                    artifact.kind,
                    artifact.reason,
                    artifact.remote_revision.as_ref().map(|v| v.as_slice()),
                    artifact.phase,
                    artifact.error,
                    artifact.digest.as_ref().map(|digest| digest.as_slice()),
                    i64::try_from(artifact.length)
                        .map_err(|_| Error::invalid("artifact length exceeds SQLite range"))?,
                    artifact.stat.map(|stat| stat.dev.to_be_bytes().to_vec()),
                    artifact.stat.map(|stat| stat.ino.to_be_bytes().to_vec()),
                    artifact.stat.map(|stat| stat.mtime),
                    artifact.stat.map(|stat| stat.mtime_nsec),
                    artifact.stat.map(|stat| stat.ctime),
                    artifact.stat.map(|stat| stat.ctime_nsec),
                    artifact.settle_pending,
                ],
            )
            .map_err(|e| Error::corrupt(format!("write retained artifact: {e}")))?;
        Ok(())
    }

    pub fn update_artifact(&mut self, id: &str, phase: &str, error: Option<&str>) -> Result<()> {
        self.connection
            .execute(
                "UPDATE artifacts SET phase=?2,error=?3,settle_pending=(?2='retained') WHERE id=?1",
                params![id, phase, error],
            )
            .map_err(|e| Error::corrupt(format!("update retained artifact: {e}")))?;
        Ok(())
    }

    pub fn update_artifact_stat(
        &mut self,
        id: &str,
        length: u64,
        stat: ArtifactStat,
    ) -> Result<()> {
        self.connection
            .execute(
                "UPDATE artifacts SET
                   length=?2,stat_dev=?3,stat_ino=?4,stat_mtime=?5,stat_mtime_nsec=?6,
                   stat_ctime=?7,stat_ctime_nsec=?8
                 WHERE id=?1",
                params![
                    id,
                    i64::try_from(length)
                        .map_err(|_| Error::invalid("artifact length exceeds SQLite range"))?,
                    stat.dev.to_be_bytes().as_slice(),
                    stat.ino.to_be_bytes().as_slice(),
                    stat.mtime,
                    stat.mtime_nsec,
                    stat.ctime,
                    stat.ctime_nsec,
                ],
            )
            .map_err(|e| Error::corrupt(format!("update retained artifact stat: {e}")))?;
        Ok(())
    }

    pub fn update_artifact_digest(&mut self, id: &str, digest: [u8; 32]) -> Result<()> {
        self.connection
            .execute(
                "UPDATE artifacts SET digest=?2 WHERE id=?1",
                params![id, digest.as_slice()],
            )
            .map_err(|e| Error::corrupt(format!("update retained artifact digest: {e}")))?;
        Ok(())
    }

    pub fn remove_artifact(&mut self, id: &str) -> Result<()> {
        self.connection
            .execute("DELETE FROM artifacts WHERE id=?1", params![id])
            .map_err(|e| Error::corrupt(format!("remove retained artifact: {e}")))?;
        Ok(())
    }

    pub fn remove_install_by_artifact(&mut self, artifact: &str) -> Result<()> {
        self.connection
            .execute("DELETE FROM installs WHERE artifact=?1", params![artifact])
            .map_err(|e| Error::corrupt(format!("remove retained install: {e}")))?;
        Ok(())
    }

    pub fn insert_install(&mut self, install: &InstallRecord) -> Result<()> {
        self.connection
            .execute(
                "INSERT INTO installs
                 VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,
                        ?14,?15,?16,?17,?18,?19)",
                params![
                    install.id,
                    install.path,
                    install.parent,
                    install.name,
                    install.temporary,
                    install.temporary_dev.map(|v| v.to_be_bytes()),
                    install.temporary_ino.map(|v| v.to_be_bytes()),
                    install.expected_digest.as_slice(),
                    i64::try_from(install.expected_length)
                        .map_err(|_| Error::invalid("install length exceeds SQLite range"))?,
                    install.expected_dev.to_be_bytes().as_slice(),
                    install.expected_ino.to_be_bytes().as_slice(),
                    install.parent_dev.to_be_bytes().as_slice(),
                    install.parent_ino.to_be_bytes().as_slice(),
                    install.replacement_digest.as_ref().map(|v| v.as_slice()),
                    install
                        .replacement_length
                        .map(i64::try_from)
                        .transpose()
                        .map_err(|_| Error::invalid("replacement length exceeds SQLite range"))?,
                    install.remote_revision.as_ref().map(|v| v.as_slice()),
                    install.artifact,
                    install.phase.as_str(),
                    install.error,
                ],
            )
            .map_err(|e| Error::corrupt(format!("write install journal: {e}")))?;
        Ok(())
    }

    pub fn update_install(&mut self, install: &InstallRecord) -> Result<()> {
        self.connection
            .execute(
                "UPDATE installs SET
                   temporary_dev=?2,temporary_ino=?3,replacement_digest=?4,
                   replacement_length=?5,phase=?6,error=?7
                 WHERE id=?1",
                params![
                    install.id,
                    install.temporary_dev.map(|v| v.to_be_bytes()),
                    install.temporary_ino.map(|v| v.to_be_bytes()),
                    install.replacement_digest.as_ref().map(|v| v.as_slice()),
                    install
                        .replacement_length
                        .map(i64::try_from)
                        .transpose()
                        .map_err(|_| Error::invalid("replacement length exceeds SQLite range"))?,
                    install.phase.as_str(),
                    install.error,
                ],
            )
            .map_err(|e| Error::corrupt(format!("update install journal: {e}")))?;
        Ok(())
    }

    /// Re-baseline the expected inode digest of a retained install so a late
    /// writer into a retained inode can be explicitly acknowledged.
    pub fn retarget_install(&mut self, install: &InstallRecord) -> Result<()> {
        self.connection
            .execute(
                "UPDATE installs SET expected_digest=?2,expected_length=?3,
                   phase=?4,error=?5 WHERE id=?1",
                params![
                    install.id,
                    install.expected_digest.as_slice(),
                    i64::try_from(install.expected_length)
                        .map_err(|_| Error::invalid("install length exceeds SQLite range"))?,
                    install.phase.as_str(),
                    install.error,
                ],
            )
            .map_err(|e| Error::corrupt(format!("retarget install journal: {e}")))?;
        Ok(())
    }

    pub fn incomplete_installs(&self) -> Result<Vec<InstallRecord>> {
        self.installs_where("WHERE phase NOT IN ('retained','conflict','failed')")
    }

    pub fn installs(&self) -> Result<Vec<InstallRecord>> {
        self.installs_where("")
    }

    fn installs_where(&self, filter: &str) -> Result<Vec<InstallRecord>> {
        let mut query = self
            .connection
            .prepare(&format!(
                "SELECT id,path,parent,name,temporary,temporary_dev,temporary_ino,
                        expected_digest,expected_length,expected_dev,expected_ino,
                        parent_dev,parent_ino,replacement_digest,replacement_length,
                        remote_revision,artifact,phase,error
                 FROM installs
                 {filter}
                 ORDER BY id"
            ))
            .map_err(|e| Error::corrupt(format!("read install journal: {e}")))?;
        query
            .query_map([], install_from_row)
            .map_err(|e| Error::corrupt(format!("read install journal: {e}")))?
            .map(|row| row.map_err(|e| Error::corrupt(format!("read install journal: {e}"))))
            .collect()
    }

    pub fn artifacts(&self) -> Result<Vec<ArtifactRecord>> {
        let mut query = self
            .connection
            .prepare(
                "SELECT id,origin,name,kind,reason,remote_revision,phase,error,digest,length,
                        stat_dev,stat_ino,stat_mtime,stat_mtime_nsec,stat_ctime,stat_ctime_nsec,
                        settle_pending
                 FROM artifacts ORDER BY rowid,id",
            )
            .map_err(|e| Error::corrupt(format!("read retained artifacts: {e}")))?;
        query
            .query_map([], |row| {
                let digest = row.get::<_, Option<Vec<u8>>>(8)?;
                let length = row.get::<_, i64>(9)?;
                let stat_dev = row.get::<_, Option<Vec<u8>>>(10)?;
                let stat_ino = row.get::<_, Option<Vec<u8>>>(11)?;
                Ok(ArtifactRecord {
                    id: row.get(0)?,
                    origin: row.get(1)?,
                    name: row.get(2)?,
                    kind: row.get(3)?,
                    reason: row.get(4)?,
                    remote_revision: row
                        .get::<_, Option<Vec<u8>>>(5)?
                        .map(|value| fixed(&value, "artifact revision"))
                        .transpose()?,
                    phase: row.get(6)?,
                    error: row.get(7)?,
                    digest: digest
                        .as_deref()
                        .map(|value| fixed(value, "artifact digest"))
                        .transpose()?,
                    length: u64::try_from(length)
                        .map_err(|_| corrupt_sql("negative artifact length"))?,
                    stat: match (stat_dev, stat_ino) {
                        (Some(dev), Some(ino)) => Some(ArtifactStat {
                            dev: number(&dev, "artifact stat device")?,
                            ino: number(&ino, "artifact stat inode")?,
                            mtime: row.get(12)?,
                            mtime_nsec: row.get(13)?,
                            ctime: row.get(14)?,
                            ctime_nsec: row.get(15)?,
                        }),
                        (None, None) => None,
                        _ => return Err(corrupt_sql("incomplete artifact stat identity")),
                    },
                    settle_pending: row.get(16)?,
                })
            })
            .map_err(|e| Error::corrupt(format!("read retained artifacts: {e}")))?
            .map(|row| row.map_err(|e| Error::corrupt(format!("read retained artifact: {e}"))))
            .collect()
    }
}

fn install_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<InstallRecord> {
    let expected_digest: Vec<u8> = row.get(7)?;
    let expected_dev: Vec<u8> = row.get(9)?;
    let expected_ino: Vec<u8> = row.get(10)?;
    let parent_dev: Vec<u8> = row.get(11)?;
    let parent_ino: Vec<u8> = row.get(12)?;
    let replacement_digest: Option<Vec<u8>> = row.get(13)?;
    let replacement_length: Option<i64> = row.get(14)?;
    let remote_revision: Option<Vec<u8>> = row.get(15)?;
    let phase: String = row.get(17)?;
    Ok(InstallRecord {
        id: row.get(0)?,
        path: row.get(1)?,
        parent: row.get(2)?,
        name: row.get(3)?,
        temporary: row.get(4)?,
        temporary_dev: row
            .get::<_, Option<Vec<u8>>>(5)?
            .map(|value| number(&value, "temporary device"))
            .transpose()?,
        temporary_ino: row
            .get::<_, Option<Vec<u8>>>(6)?
            .map(|value| number(&value, "temporary inode"))
            .transpose()?,
        expected_digest: fixed(&expected_digest, "expected digest")?,
        expected_length: u64::try_from(row.get::<_, i64>(8)?)
            .map_err(|_| corrupt_sql("negative expected length"))?,
        expected_dev: number(&expected_dev, "expected device")?,
        expected_ino: number(&expected_ino, "expected inode")?,
        parent_dev: number(&parent_dev, "parent device")?,
        parent_ino: number(&parent_ino, "parent inode")?,
        replacement_digest: replacement_digest
            .as_deref()
            .map(|value| fixed(value, "replacement digest"))
            .transpose()?,
        replacement_length: replacement_length
            .map(|value| {
                u64::try_from(value).map_err(|_| corrupt_sql("negative replacement length"))
            })
            .transpose()?,
        remote_revision: remote_revision
            .as_deref()
            .map(|value| fixed(value, "remote revision"))
            .transpose()?,
        artifact: row.get(16)?,
        phase: InstallPhase::parse(&phase).map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(
                17,
                rusqlite::types::Type::Text,
                Box::new(error),
            )
        })?,
        error: row.get(18)?,
    })
}

fn fixed<const N: usize>(bytes: &[u8], name: &str) -> rusqlite::Result<[u8; N]> {
    bytes
        .try_into()
        .map_err(|_| corrupt_sql(format!("invalid {name} length")))
}

fn validate_profile(connection: &Connection) -> Result<Shape> {
    let profile: Option<String> = connection
        .query_row("SELECT profile FROM binding WHERE singleton=1", [], |row| {
            row.get(0)
        })
        .optional()
        .map_err(|error| Error::corrupt(format!("read replica profile: {error}")))?;
    if profile.as_deref().is_some_and(|profile| profile != PROFILE) {
        return Err(Error::unsupported(format!(
            "replica state uses unsupported profile {}",
            profile.as_deref().unwrap_or_default()
        )));
    }
    let tables = schema::table_names(connection)
        .map_err(|error| Error::corrupt(format!("read replica tables: {error}")))?;
    if tables
        .iter()
        .any(|table| !TABLE_NAMES.contains(&table.as_str()))
    {
        return Err(Error::corrupt(
            "replica state contains an unsupported table",
        ));
    }
    if !schema::columns_match(connection, REQUIRED_SCHEMA)
        .map_err(|error| Error::corrupt(format!("read replica schema: {error}")))?
    {
        return Err(Error::corrupt(
            "replica state has an unsupported required-table schema",
        ));
    }

    let baseline = schema::column_names(connection, "baseline")
        .map_err(|error| Error::corrupt(format!("read baseline schema: {error}")))?
        .ok_or_else(|| Error::corrupt("replica state is missing its baseline table"))?;
    let baseline_set = baseline.iter().map(String::as_str).collect::<BTreeSet<_>>();
    validate_columns("baseline", &baseline_set, BASELINE_BASE, BASELINE_OPTIONAL)?;
    let baseline_remote_revision = baseline_set.contains("remote_revision");

    let has_conflicts = tables.contains("conflicts");
    if has_conflicts
        && !schema::columns_match(connection, &[CONFLICTS_SCHEMA])
            .map_err(|error| Error::corrupt(format!("read conflict schema: {error}")))?
    {
        return Err(Error::corrupt(
            "replica state has an unsupported conflicts schema",
        ));
    }

    let has_work = tables.contains("work");
    let mut work_columns = BTreeSet::new();
    let mut work_missing = Vec::new();
    if has_work {
        let columns = schema::column_names(connection, "work")
            .map_err(|error| Error::corrupt(format!("read work schema: {error}")))?
            .ok_or_else(|| Error::corrupt("replica state is missing its work table"))?;
        work_columns.extend(columns);
        let work_set = work_columns
            .iter()
            .map(String::as_str)
            .collect::<BTreeSet<_>>();
        validate_columns("work", &work_set, WORK_BASE, WORK_OPTIONAL)?;
        work_missing = WORK_UPGRADE_ORDER
            .iter()
            .copied()
            .filter(|column| !work_columns.contains(*column))
            .collect();
    }

    Ok(Shape {
        initialized: true,
        has_conflicts,
        has_work,
        baseline_remote_revision,
        work_columns,
        work_missing,
    })
}

fn validate_columns(
    table: &str,
    actual: &BTreeSet<&str>,
    base: &[&str],
    optional: &[&str],
) -> Result<()> {
    if base.iter().any(|column| !actual.contains(column)) {
        return Err(Error::corrupt(format!(
            "replica {table} table is missing a required column"
        )));
    }
    if actual
        .iter()
        .any(|column| !base.contains(column) && !optional.contains(column))
    {
        return Err(Error::corrupt(format!(
            "replica {table} table contains an unsupported column"
        )));
    }
    Ok(())
}

fn upgrade(connection: &mut Connection, shape: &Shape) -> Result<()> {
    if shape.has_conflicts
        && shape.has_work
        && shape.baseline_remote_revision
        && shape.work_missing.is_empty()
    {
        return Ok(());
    }
    let transaction = connection
        .transaction()
        .map_err(|error| Error::corrupt(format!("begin replica upgrade: {error}")))?;
    if !shape.has_conflicts {
        transaction
            .execute_batch(CREATE_CONFLICTS_TABLE)
            .map_err(|error| Error::corrupt(format!("create conflicts table: {error}")))?;
    }
    if !shape.baseline_remote_revision {
        transaction
            .execute_batch("ALTER TABLE baseline ADD COLUMN remote_revision BLOB")
            .map_err(|error| Error::corrupt(format!("add baseline revision: {error}")))?;
    }
    if !shape.has_work {
        transaction
            .execute_batch(CREATE_WORK_TABLE)
            .map_err(|error| Error::corrupt(format!("create work table: {error}")))?;
    } else {
        for column in &shape.work_missing {
            let declaration = if *column == "seq" {
                "INTEGER NOT NULL DEFAULT 0"
            } else if matches!(*column, "conflict" | "source_artifact" | "upload_artifact") {
                "TEXT"
            } else {
                "BLOB"
            };
            transaction
                .execute_batch(&format!(
                    "ALTER TABLE work ADD COLUMN {column} {declaration}"
                ))
                .map_err(|error| Error::corrupt(format!("add work column {column}: {error}")))?;
        }
    }
    transaction
        .commit()
        .map_err(|error| Error::corrupt(format!("commit replica upgrade: {error}")))?;
    let upgraded = validate_profile(connection)?;
    if !upgraded.has_conflicts
        || !upgraded.has_work
        || !upgraded.baseline_remote_revision
        || !upgraded.work_missing.is_empty()
    {
        return Err(Error::corrupt(
            "replica upgrade did not produce the supported schema",
        ));
    }
    Ok(())
}

fn number(bytes: &[u8], name: &str) -> rusqlite::Result<u64> {
    Ok(u64::from_be_bytes(fixed(bytes, name)?))
}

fn map_identity(
    identities: &IdentityMap,
    dev: Option<&[u8]>,
    ino: Option<&[u8]>,
) -> Result<Option<(u64, u64)>> {
    let (Some(dev), Some(ino)) = (dev, ino) else {
        return Ok(None);
    };
    let old = (
        number(dev, "device").map_err(|e| Error::corrupt(format!("decode device: {e}")))?,
        number(ino, "inode").map_err(|e| Error::corrupt(format!("decode inode: {e}")))?,
    );
    if let Some(mapped) = identities.get(&old) {
        return Ok(Some(*mapped));
    }
    if identities.values().any(|value| *value == old) {
        return Err(Error::corrupt(format!(
            "stored local identity {}/{} is absent from the manifest but collides with a restored inode",
            old.0, old.1
        )));
    }
    Ok(None)
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
    use crate::replica::ErrorKind;

    fn create_database(path: &Path, profile: &str, schema: &str) {
        let connection = Connection::open(path).unwrap();
        connection.execute_batch(schema).unwrap();
        connection
            .execute(
                "INSERT INTO binding VALUES(1,?1,zeroblob(16),'/root',1,2,1,3)",
                [profile],
            )
            .unwrap();
    }

    #[test]
    fn unsupported_profile_is_rejected_before_sqlite_configuration() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("replica.db");
        create_database(&path, "yinyang-directory-replica-99", CREATE_SCHEMA);
        assert_eq!(
            Store::open_read_only(&path).err().unwrap().kind(),
            ErrorKind::Unsupported
        );
        assert_eq!(
            Store::open(&path).err().unwrap().kind(),
            ErrorKind::Unsupported
        );
        let connection = Connection::open(path).unwrap();
        let mode: String = connection
            .query_row("PRAGMA journal_mode", [], |row| row.get(0))
            .unwrap();
        assert_eq!(mode, "delete");
    }

    #[test]
    fn readable_legacy_shape_upgrades_after_validation() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("replica.db");
        create_database(&path, PROFILE, CREATE_SCHEMA);
        let connection = Connection::open(&path).unwrap();
        connection
            .execute_batch(
                "ALTER TABLE baseline DROP COLUMN remote_revision;
                 ALTER TABLE work DROP COLUMN seq;
                 ALTER TABLE work DROP COLUMN conflict;
                 ALTER TABLE work DROP COLUMN source_artifact;
                 ALTER TABLE work DROP COLUMN local_dev;
                 ALTER TABLE work DROP COLUMN local_ino;
                 ALTER TABLE work DROP COLUMN local_parent_dev;
                 ALTER TABLE work DROP COLUMN local_parent_ino;
                 ALTER TABLE work DROP COLUMN commit_id;
                 INSERT INTO work(
                   id,path,original_path,desired_path,operation,local_kind,
                   local_digest,local_length,local_executable,baseline_node,
                   baseline_revision,remote_revision,state,error
                 ) VALUES(
                   zeroblob(32),'legacy',NULL,NULL,'content',NULL,NULL,NULL,NULL,
                   NULL,zeroblob(24),NULL,'pending',NULL
                 );",
            )
            .unwrap();
        drop(connection);
        let store = Store::open_read_only(&path).unwrap();
        let work = store.work().unwrap();
        assert_eq!(work.len(), 1);
        assert_eq!(work[0].seq, 0);
        assert!(work[0].source_artifact.is_none());
        drop(store);

        let store = Store::open(&path).unwrap();
        let work = store.work().unwrap();
        assert_eq!(work.len(), 1);
        assert_eq!(work[0].path, "legacy");
        let connection = Connection::open(path).unwrap();
        let baseline: Vec<String> = {
            let mut statement = connection.prepare("PRAGMA table_info(baseline)").unwrap();
            statement
                .query_map([], |row| row.get(1))
                .unwrap()
                .collect::<rusqlite::Result<_>>()
                .unwrap()
        };
        assert!(baseline.iter().any(|column| column == "remote_revision"));
        let work: Vec<String> = {
            let mut statement = connection.prepare("PRAGMA table_info(work)").unwrap();
            statement
                .query_map([], |row| row.get(1))
                .unwrap()
                .collect::<rusqlite::Result<_>>()
                .unwrap()
        };
        for column in WORK_OPTIONAL {
            assert!(work.iter().any(|actual| actual == *column));
        }
    }

    #[test]
    fn known_profile_missing_a_required_column_is_not_repaired() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("replica.db");
        create_database(&path, PROFILE, CREATE_SCHEMA);
        Connection::open(&path)
            .unwrap()
            .execute_batch("ALTER TABLE work DROP COLUMN path")
            .unwrap();
        assert_eq!(
            Store::open_read_only(&path).err().unwrap().kind(),
            ErrorKind::Corrupt
        );
        assert_eq!(Store::open(&path).err().unwrap().kind(), ErrorKind::Corrupt);
        let connection = Connection::open(path).unwrap();
        let mode: String = connection
            .query_row("PRAGMA journal_mode", [], |row| row.get(0))
            .unwrap();
        assert_eq!(mode, "delete");
        let work: Vec<String> = {
            let mut statement = connection.prepare("PRAGMA table_info(work)").unwrap();
            statement
                .query_map([], |row| row.get(1))
                .unwrap()
                .collect::<rusqlite::Result<_>>()
                .unwrap()
        };
        assert!(!work.iter().any(|column| column == "path"));
    }
}
