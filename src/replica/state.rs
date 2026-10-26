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
use super::{LocalKind, LocalNode, LocalSnapshot};
use crate::replica::linux::{ContentHash, Identity};
use rusqlite::{Connection, OptionalExtension, params};
use std::collections::BTreeMap;
use std::path::Path;
use yinyang_core::{Node, NodeId, NodeKind, Revision};

pub(super) const PROFILE: &str = "yinyang-directory-replica-1";

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
    pub remote: BTreeMap<String, BaselineNode>,
    pub local: LocalSnapshot,
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
    pub id: [u8; 32],
    pub path: String,
    pub original_path: Option<String>,
    pub desired_path: Option<String>,
    pub operation: String,
    pub local_kind: Option<String>,
    pub local_digest: Option<[u8; 32]>,
    pub local_length: Option<u64>,
    pub local_executable: Option<bool>,
    pub baseline_node: Option<[u8; 16]>,
    pub baseline_revision: [u8; 24],
    pub remote_revision: Option<[u8; 24]>,
    /// Conflict this edit resolves, if it was created by an explicit decision.
    pub conflict: Option<String>,
    /// Retained artifact holding the exact local input published by this edit.
    pub source_artifact: Option<String>,
    pub state: String,
    pub error: Option<String>,
}

pub(super) struct Store {
    connection: Connection,
}

impl Store {
    pub fn open(path: &Path) -> Result<Self> {
        let connection = Connection::open(path).map_err(|e| Error::io("open", path, e))?;
        connection
            .execute_batch(
                "PRAGMA journal_mode=WAL;
                 PRAGMA synchronous=FULL;
                 PRAGMA foreign_keys=ON;
                 CREATE TABLE IF NOT EXISTS binding(
                   singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                   profile TEXT NOT NULL,
                   filesystem BLOB NOT NULL,
                   root TEXT NOT NULL,
                   root_dev BLOB NOT NULL,
                   root_ino BLOB NOT NULL,
                   state_dev BLOB NOT NULL,
                   state_ino BLOB NOT NULL
                 );
                 CREATE TABLE IF NOT EXISTS baseline(
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
                   remote_revision BLOB
                 );
                 CREATE TABLE IF NOT EXISTS baseline_meta(
                   singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                   revision BLOB NOT NULL
                 );
                 CREATE TABLE IF NOT EXISTS artifacts(
                   id TEXT PRIMARY KEY,
                   origin TEXT NOT NULL,
                   name TEXT NOT NULL UNIQUE,
                   kind TEXT NOT NULL,
                   reason TEXT NOT NULL,
                   remote_revision BLOB,
                   phase TEXT NOT NULL,
                   error TEXT
                 );
                 CREATE TABLE IF NOT EXISTS installs(
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
                 CREATE TABLE IF NOT EXISTS conflicts(
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
                 CREATE TABLE IF NOT EXISTS work(
                   id BLOB PRIMARY KEY,
                   path TEXT NOT NULL,
                   original_path TEXT,
                   desired_path TEXT,
                   operation TEXT NOT NULL,
                   local_kind TEXT,
                   local_digest BLOB,
                   local_length INTEGER,
                   local_executable INTEGER,
                   baseline_node BLOB,
                   baseline_revision BLOB NOT NULL,
                   remote_revision BLOB,
                   conflict TEXT,
                   source_artifact TEXT,
                   state TEXT NOT NULL,
                   error TEXT
                 );",
            )
            .map_err(|e| Error::io("initialize replica state", path, e))?;
        ensure_column(&connection, "baseline", "remote_revision", "BLOB")
            .map_err(|e| Error::io("migrate replica baseline", path, e))?;
        ensure_column(&connection, "work", "conflict", "TEXT")
            .map_err(|e| Error::io("migrate replica work", path, e))?;
        ensure_column(&connection, "work", "source_artifact", "TEXT")
            .map_err(|e| Error::io("migrate replica work", path, e))?;
        Ok(Self { connection })
    }

    pub fn open_read_only(path: &Path) -> Result<Self> {
        use rusqlite::OpenFlags;
        let connection = Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .map_err(|e| Error::io("open", path, e))?;
        connection
            .execute_batch("PRAGMA query_only=ON")
            .map_err(|e| Error::io("configure replica inspection", path, e))?;
        Ok(Self { connection })
    }

    pub fn binding(&self) -> Result<Option<Binding>> {
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
            transaction
                .execute(
                    "INSERT INTO baseline
                     (path,node,directory,executable,remote_digest,remote_length,
                      local_present,local_digest,local_length,local_executable,
                      local_dev,local_ino,local_parent_dev,local_parent_ino,remote_revision)
                     VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15)",
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
        self.connection
            .execute(
                "INSERT OR REPLACE INTO baseline
                 (path,node,directory,executable,remote_digest,remote_length,
                  local_present,local_digest,local_length,local_executable,
                  local_dev,local_ino,local_parent_dev,local_parent_ino,remote_revision)
                 VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15)",
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
        let _revision: [u8; 24] = fixed(&revision, "baseline revision")
            .map_err(|e| Error::corrupt(format!("decode baseline revision: {e}")))?;
        let mut remote = BTreeMap::new();
        let mut local = BTreeMap::new();
        let mut statement = self
            .connection
            .prepare(
                "SELECT path,node,directory,executable,remote_digest,remote_length,
                        local_present,local_digest,local_length,local_executable,
                        local_dev,local_ino,local_parent_dev,local_parent_ino,
                        remote_revision
                 FROM baseline ORDER BY path",
            )
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
                let revision: Vec<u8> = row.get(14)?;
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
                    Revision::from_bytes(fixed(&revision, "baseline revision")?),
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
                local.insert(
                    path.clone(),
                    LocalNode {
                        path: path.clone(),
                        kind,
                        executable: local_executable.unwrap_or(false),
                        identity,
                        parent: match (local_parent_dev, local_parent_ino) {
                            (Some(dev), Some(ino)) => Some(Identity { dev, ino }),
                            _ => None,
                        },
                    },
                );
            }
        }
        Ok(Some(BaselineRecord {
            remote,
            local: LocalSnapshot::from_nodes(local),
        }))
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
                 (id,path,original_path,desired_path,operation,local_kind,local_digest,
                  local_length,local_executable,baseline_node,baseline_revision,
                  remote_revision,conflict,source_artifact,state,error)
                 VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16)",
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
                    work.baseline_node.as_ref().map(|v| v.as_slice()),
                    work.baseline_revision.as_slice(),
                    work.remote_revision.as_ref().map(|v| v.as_slice()),
                    work.conflict,
                    work.source_artifact,
                    work.state,
                    work.error,
                ],
            )
            .map_err(|e| Error::corrupt(format!("write work association: {e}")))?;
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

    pub fn work(&self) -> Result<Vec<WorkRecord>> {
        let mut query = self
            .connection
            .prepare(
                "SELECT id,path,original_path,desired_path,operation,local_kind,local_digest,
                        local_length,local_executable,baseline_node,baseline_revision,
                        remote_revision,conflict,source_artifact,state,error
                 FROM work ORDER BY path,id",
            )
            .map_err(|e| Error::corrupt(format!("read work associations: {e}")))?;
        query
            .query_map([], |row| {
                let id: Vec<u8> = row.get(0)?;
                let local_digest: Option<Vec<u8>> = row.get(6)?;
                let local_length: Option<i64> = row.get(7)?;
                let baseline_node: Option<Vec<u8>> = row.get(9)?;
                let baseline_revision: Vec<u8> = row.get(10)?;
                let remote_revision: Option<Vec<u8>> = row.get(11)?;
                Ok(WorkRecord {
                    id: fixed(&id, "work id")?,
                    path: row.get(1)?,
                    original_path: row.get(2)?,
                    desired_path: row.get(3)?,
                    operation: row.get(4)?,
                    local_kind: row.get(5)?,
                    local_digest: local_digest
                        .as_deref()
                        .map(|value| fixed(value, "work local digest"))
                        .transpose()?,
                    local_length: local_length
                        .map(|value| {
                            u64::try_from(value).map_err(|_| corrupt_sql("negative work length"))
                        })
                        .transpose()?,
                    local_executable: row.get(8)?,
                    baseline_node: baseline_node
                        .as_deref()
                        .map(|value| fixed(value, "work baseline node"))
                        .transpose()?,
                    baseline_revision: fixed(&baseline_revision, "work baseline revision")?,
                    remote_revision: remote_revision
                        .as_deref()
                        .map(|value| fixed(value, "work remote revision"))
                        .transpose()?,
                    conflict: row.get(12)?,
                    source_artifact: row.get(13)?,
                    state: row.get(14)?,
                    error: row.get(15)?,
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
                 VALUES(?1,?2,?3,?4,?5,?6,?7,?8)",
                params![
                    artifact.id,
                    artifact.origin,
                    artifact.name,
                    artifact.kind,
                    artifact.reason,
                    artifact.remote_revision.as_ref().map(|v| v.as_slice()),
                    artifact.phase,
                    artifact.error,
                ],
            )
            .map_err(|e| Error::corrupt(format!("write retained artifact: {e}")))?;
        Ok(())
    }

    pub fn update_artifact(&mut self, id: &str, phase: &str, error: Option<&str>) -> Result<()> {
        self.connection
            .execute(
                "UPDATE artifacts SET phase=?2,error=?3 WHERE id=?1",
                params![id, phase, error],
            )
            .map_err(|e| Error::corrupt(format!("update retained artifact: {e}")))?;
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
                "SELECT id,origin,name,kind,reason,remote_revision,phase,error
                 FROM artifacts ORDER BY id",
            )
            .map_err(|e| Error::corrupt(format!("read retained artifacts: {e}")))?;
        query
            .query_map([], |row| {
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

/// Add a column to an existing table when an older database predates it.
/// `CREATE TABLE IF NOT EXISTS` only covers fresh databases.
fn ensure_column(
    connection: &Connection,
    table: &str,
    column: &str,
    declaration: &str,
) -> rusqlite::Result<()> {
    let mut statement = connection.prepare(&format!("PRAGMA table_info({table})"))?;
    let existing = statement
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    drop(statement);
    if existing.iter().any(|name| name == column) {
        return Ok(());
    }
    connection.execute_batch(&format!(
        "ALTER TABLE {table} ADD COLUMN {column} {declaration}"
    ))
}

fn number(bytes: &[u8], name: &str) -> rusqlite::Result<u64> {
    Ok(u64::from_be_bytes(fixed(bytes, name)?))
}

fn corrupt_sql(message: impl Into<String>) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(
        0,
        rusqlite::types::Type::Blob,
        Box::new(Error::corrupt(message)),
    )
}
