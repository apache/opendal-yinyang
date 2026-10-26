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
use rusqlite::{Connection, OptionalExtension, params};
use std::path::Path;
use yinyang_core::NodeId;

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
                   node BLOB,
                   directory INTEGER NOT NULL,
                   executable INTEGER NOT NULL,
                   remote_digest BLOB,
                   remote_length INTEGER,
                   local_digest BLOB,
                   local_length INTEGER,
                   local_dev BLOB NOT NULL,
                   local_ino BLOB NOT NULL
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
                   artifact TEXT NOT NULL REFERENCES artifacts(id),
                   phase TEXT NOT NULL,
                   error TEXT
                 );",
            )
            .map_err(|e| Error::io("initialize replica state", path, e))?;
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
            && number(&stored.3, "root device")? == binding.root_dev
            && number(&stored.4, "root inode")? == binding.root_ino
            && number(&stored.5, "state device")? == binding.state_dev
            && number(&stored.6, "state inode")? == binding.state_ino;
        Ok(matches)
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

    pub fn incomplete_installs(&self) -> Result<Vec<InstallRecord>> {
        let mut query = self
            .connection
            .prepare(
                "SELECT id,path,parent,name,temporary,temporary_dev,temporary_ino,
                        expected_digest,expected_length,expected_dev,expected_ino,
                        parent_dev,parent_ino,replacement_digest,replacement_length,
                        remote_revision,artifact,phase,error
                 FROM installs
                 WHERE phase NOT IN ('retained','conflict','failed')
                 ORDER BY id",
            )
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
