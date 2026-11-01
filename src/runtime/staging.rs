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

//! Durable local write staging. SQLite keeps each acknowledged write and its
//! generation atomic; exact dirty ranges overlay an authenticated base.
use super::*;
use borsh::{BorshDeserialize, BorshSerialize};
use rusqlite::{Connection, OpenFlags, OptionalExtension, params};
use std::fs::File;
use std::ops::Range;
use tokio::sync::Mutex;

pub(super) const CHUNK: u64 = 64 * 1024;
const PROFILE_V1: &str = "yinyang-stage-1";
const PROFILE_V2: &str = "yinyang-stage-2";
const PROFILE_V3: &str = "yinyang-stage-3";
const SCHEMA_V1: &[schema::TableColumns<'static>] = &[
    ("authority", &["singleton", "profile", "filesystem"]),
    ("handles", &["id", "record"]),
    ("chunks", &["handle", "position", "data"]),
    ("errors", &["sequence", "handle", "message"]),
];
const SCHEMA_V2: &[schema::TableColumns<'static>] = &[
    ("authority", &["singleton", "profile", "filesystem"]),
    ("handles", &["id", "record", "failure"]),
    ("chunks", &["handle", "position", "data"]),
    ("errors", &["sequence", "handle", "message", "failure"]),
];
const SCHEMA_V3: &[schema::TableColumns<'static>] = &[
    ("authority", &["singleton", "profile", "filesystem"]),
    ("bases", &["handle", "descriptor", "prefix"]),
    ("handles", &["id", "record", "failure"]),
    ("chunks", &["handle", "position", "data"]),
    ("errors", &["sequence", "handle", "message", "failure"]),
    ("segments", &["handle", "position", "data"]),
];

const CREATE_SCHEMA_V3: &str = "
    CREATE TABLE authority(
      singleton INTEGER PRIMARY KEY CHECK(singleton=1),
      profile TEXT NOT NULL,
      filesystem BLOB NOT NULL
    );
    CREATE TABLE bases(
      handle BLOB PRIMARY KEY,
      descriptor BLOB NOT NULL,
      prefix INTEGER NOT NULL
    );
    CREATE TABLE handles(id BLOB PRIMARY KEY, record BLOB NOT NULL, failure BLOB);
    CREATE TABLE chunks(
      handle BLOB NOT NULL,
      position INTEGER NOT NULL,
      data BLOB NOT NULL,
      PRIMARY KEY(handle,position)
    );
    CREATE TABLE errors(
      sequence INTEGER PRIMARY KEY AUTOINCREMENT,
      handle BLOB NOT NULL,
      message TEXT NOT NULL,
      failure BLOB
    );
    CREATE TABLE segments(
      handle BLOB NOT NULL,
      position INTEGER NOT NULL,
      data BLOB NOT NULL,
      PRIMARY KEY(handle,position)
    );";
const CREATE_SPARSE_TABLES: &str = "
    CREATE TABLE bases(
      handle BLOB PRIMARY KEY,
      descriptor BLOB NOT NULL,
      prefix INTEGER NOT NULL
    );
    CREATE TABLE segments(
      handle BLOB NOT NULL,
      position INTEGER NOT NULL,
      data BLOB NOT NULL,
      PRIMARY KEY(handle,position)
    );";

#[derive(Clone, BorshSerialize, BorshDeserialize)]
pub(super) struct Record {
    pub id: [u8; 16],
    pub node: [u8; 16],
    pub base: [u8; 24],
    pub length: u64,
    pub local: u64,
    pub remote: u64,
    pub writable: bool,
    pub ready: bool,
    pub plan: Option<Vec<u8>>,
    pub conflict: bool,
    pub error: Option<String>,
    // Stored in an additive SQLite column; keep existing staged record bytes readable.
    #[borsh(skip)]
    pub failure: Option<Failure>,
}
impl Record {
    pub fn status(&self) -> HandleStatus {
        HandleStatus {
            id: uuid::Uuid::from_bytes(self.id),
            node: NodeId::from_bytes(self.node),
            length: self.length,
            local_generation: self.local,
            remote_generation: self.remote,
            remote_revision: Revision::from_bytes(self.base),
            pending: self.local != self.remote,
            frozen: self.plan.is_some(),
            conflict: self.conflict,
            error: self
                .failure
                .clone()
                .or_else(|| self.error.clone().map(Failure::legacy)),
            frozen_commit_id: self
                .plan
                .as_deref()
                .and_then(|plan| yinyang_core::Transaction::inspect_identity(plan).ok())
                .map(|(_, commit, _)| commit),
        }
    }
}
#[derive(Clone)]
pub(super) struct Base {
    pub descriptor: Vec<u8>,
    pub prefix: u64,
}
pub(super) struct StagedRead {
    pub bytes: Vec<u8>,
    pub base: Vec<Range<u64>>,
}
#[derive(Clone)]
pub(super) struct Stage {
    db: Arc<Mutex<Connection>>,
    _lock: Arc<File>,
}

/// Exclusive process lease for one existing runtime staging protocol.
///
/// Acquisition may create the configured directory and `runtime.lock`, but it
/// never opens, creates, migrates, or recovers `staging.db`.
#[derive(Clone)]
pub(crate) struct StagingLease {
    lock: Arc<File>,
}

impl StagingLease {
    pub(crate) async fn acquire(path: PathBuf) -> Result<Self> {
        tokio::task::spawn_blocking(move || Self::acquire_sync(&path))
            .await
            .map_err(local)?
    }

    fn acquire_sync(path: &Path) -> Result<Self> {
        std::fs::create_dir_all(path).map_err(local)?;
        let lock = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path.join("runtime.lock"))
            .map_err(local)?;
        lock.try_lock().map_err(|e| match e {
            std::fs::TryLockError::WouldBlock => {
                Error::State(ErrorKind::Busy, "staging directory is already in use")
            }
            std::fs::TryLockError::Error(e) => local(e),
        })?;
        Ok(Self {
            lock: Arc::new(lock),
        })
    }

    fn file(&self) -> Arc<File> {
        Arc::clone(&self.lock)
    }
}

fn local(e: impl std::error::Error + 'static) -> Error {
    let cause: &dyn std::error::Error = &e;
    let kind = if let Some(error) = cause.downcast_ref::<rusqlite::Error>() {
        match error {
            rusqlite::Error::QueryReturnedNoRows => ErrorKind::NotFound,
            rusqlite::Error::SqliteFailure(code, _) => match code.code {
                rusqlite::ErrorCode::DiskFull => ErrorKind::NoSpace,
                rusqlite::ErrorCode::ReadOnly => ErrorKind::ReadOnly,
                rusqlite::ErrorCode::PermissionDenied => ErrorKind::PermissionDenied,
                rusqlite::ErrorCode::DatabaseCorrupt | rusqlite::ErrorCode::NotADatabase => {
                    ErrorKind::Corrupt
                }
                rusqlite::ErrorCode::DatabaseBusy | rusqlite::ErrorCode::DatabaseLocked => {
                    ErrorKind::Busy
                }
                _ => ErrorKind::Io,
            },
            _ => ErrorKind::Io,
        }
    } else if let Some(error) = cause.downcast_ref::<std::io::Error>() {
        match error.kind() {
            std::io::ErrorKind::StorageFull => ErrorKind::NoSpace,
            std::io::ErrorKind::PermissionDenied => ErrorKind::PermissionDenied,
            std::io::ErrorKind::ReadOnlyFilesystem => ErrorKind::ReadOnly,
            _ => ErrorKind::Io,
        }
    } else {
        ErrorKind::Io
    };
    Error::local_kind(kind, e)
}
fn decode(bytes: &[u8]) -> Result<Record> {
    borsh::from_slice(bytes).map_err(|e| Error::local_kind(ErrorKind::Corrupt, e))
}
fn save(c: &Connection, r: &Record) -> Result<()> {
    c.execute(
        "INSERT OR REPLACE INTO handles(id,record,failure) VALUES(?1,?2,?3)",
        params![
            r.id.as_slice(),
            borsh::to_vec(r).map_err(local)?,
            r.failure
                .as_ref()
                .map(borsh::to_vec)
                .transpose()
                .map_err(local)?
        ],
    )
    .map_err(local)?;
    Ok(())
}
fn has_failure(c: &Connection, table: &str) -> Result<bool> {
    c.query_row(
        "SELECT EXISTS(SELECT 1 FROM pragma_table_info(?1) WHERE name='failure')",
        [table],
        |r| r.get(0),
    )
    .map_err(local)
}

fn validate_schema(c: &Connection, profile: &str) -> Result<()> {
    let expected = match profile {
        PROFILE_V1 => SCHEMA_V1,
        PROFILE_V2 => SCHEMA_V2,
        PROFILE_V3 => SCHEMA_V3,
        _ => {
            return Err(Error::State(
                ErrorKind::Unsupported,
                "unknown staging profile",
            ));
        }
    };
    if !schema::matches(c, expected).map_err(local)? {
        return Err(Error::local_kind(
            ErrorKind::Corrupt,
            format!("staging profile {profile} has an unsupported schema"),
        ));
    }
    Ok(())
}

fn validate_profile(c: &Connection, filesystem: Option<NodeId>) -> Result<String> {
    let (profile, bound): (String, Vec<u8>) = c
        .query_row(
            "SELECT profile,filesystem FROM authority WHERE singleton=1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .map_err(|error| {
            Error::local_kind(ErrorKind::Corrupt, format!("read staging profile: {error}"))
        })?;
    validate_schema(c, &profile)?;
    if let Some(filesystem) = filesystem
        && bound != filesystem.as_bytes()
    {
        return Err(Error::Invalid("staging belongs to a different filesystem"));
    }
    Ok(profile)
}
fn decoded_record(bytes: &[u8], failure: Option<Vec<u8>>) -> Result<Record> {
    let mut r = decode(bytes)?;
    r.failure = failure
        .map(|b| borsh::from_slice(&b).map_err(|e| Error::local_kind(ErrorKind::Corrupt, e)))
        .transpose()?;
    Ok(r)
}
fn record(c: &Connection, id: [u8; 16]) -> Result<Record> {
    let (bytes, failure): (Vec<u8>, Option<Vec<u8>>) = c
        .query_row(
            "SELECT record,failure FROM handles WHERE id=?1",
            params![id.as_slice()],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .map_err(local)?;
    decoded_record(&bytes, failure)
}
fn chunk(c: &Connection, id: [u8; 16], index: u64) -> Result<Vec<u8>> {
    Ok(c.query_row(
        "SELECT data FROM chunks WHERE handle=?1 AND position=?2",
        params![id.as_slice(), index as i64],
        |r| r.get(0),
    )
    .optional()
    .map_err(local)?
    .unwrap_or_default())
}
fn put_chunk(c: &Connection, id: [u8; 16], index: u64, bytes: &[u8]) -> Result<()> {
    c.execute(
        "INSERT OR REPLACE INTO chunks VALUES(?1,?2,?3)",
        params![id.as_slice(), index as i64, bytes],
    )
    .map_err(local)?;
    Ok(())
}
fn base(c: &Connection, id: [u8; 16]) -> Result<Option<Base>> {
    let (descriptor, prefix): (Vec<u8>, i64) = match c
        .query_row(
            "SELECT descriptor,prefix FROM bases WHERE handle=?1",
            params![id.as_slice()],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()
        .map_err(local)?
    {
        Some(base) => base,
        None => return Ok(None),
    };
    let prefix = u64::try_from(prefix).map_err(|e| Error::local_kind(ErrorKind::Corrupt, e))?;
    ContentDescriptor::from_bytes(&descriptor)?;
    Ok(Some(Base { descriptor, prefix }))
}
fn put_base(c: &Connection, id: [u8; 16], descriptor: &[u8], prefix: u64) -> Result<()> {
    let descriptor = ContentDescriptor::from_bytes(descriptor)?;
    if prefix > descriptor.content_id().length() {
        return Err(Error::State(
            ErrorKind::Corrupt,
            "staging base prefix exceeds content length",
        ));
    }
    c.execute(
        "INSERT OR REPLACE INTO bases(handle,descriptor,prefix) VALUES(?1,?2,?3)",
        params![
            id.as_slice(),
            descriptor.to_bytes(),
            i64::try_from(prefix).map_err(local)?
        ],
    )
    .map_err(local)?;
    Ok(())
}
fn put_segment(c: &Connection, id: [u8; 16], position: u64, bytes: &[u8]) -> Result<()> {
    if bytes.is_empty() {
        return Ok(());
    }
    let end = segment_end(position, bytes)?;
    let chunk_start = position - position % CHUNK;
    if end > chunk_start + CHUNK {
        return Err(Error::State(
            ErrorKind::Invalid,
            "staging segment crosses a chunk boundary",
        ));
    }
    c.execute(
        "INSERT OR REPLACE INTO segments(handle,position,data) VALUES(?1,?2,?3)",
        params![
            id.as_slice(),
            i64::try_from(position).map_err(local)?,
            bytes
        ],
    )
    .map_err(local)?;
    Ok(())
}
fn delete_segment(c: &Connection, id: [u8; 16], position: u64) -> Result<()> {
    c.execute(
        "DELETE FROM segments WHERE handle=?1 AND position=?2",
        params![id.as_slice(), i64::try_from(position).map_err(local)?],
    )
    .map_err(local)?;
    Ok(())
}
fn records(c: &Connection) -> Result<Vec<Record>> {
    // Inspect must also work before an older staging database is opened/upgraded.
    let sql = if has_failure(c, "handles")? {
        "SELECT record,failure FROM handles ORDER BY id"
    } else {
        "SELECT record,NULL FROM handles ORDER BY id"
    };
    let mut query = c.prepare(sql).map_err(local)?;
    let rows = query
        .query_map([], |r| {
            Ok((r.get::<_, Vec<u8>>(0)?, r.get::<_, Option<Vec<u8>>>(1)?))
        })
        .map_err(local)?;
    let mut result = Vec::new();
    for row in rows {
        let (bytes, failure) = row.map_err(local)?;
        let record = decoded_record(&bytes, failure)?;
        if record.ready {
            result.push(record);
        }
    }
    Ok(result)
}
fn errors(c: &Connection) -> Result<Vec<WritebackError>> {
    let sql = if has_failure(c, "errors")? {
        "SELECT sequence,handle,message,failure FROM errors ORDER BY sequence"
    } else {
        "SELECT sequence,handle,message,NULL FROM errors ORDER BY sequence"
    };
    let mut query = c.prepare(sql).map_err(local)?;
    let rows = query
        .query_map([], |r| {
            Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, Vec<u8>>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, Option<Vec<u8>>>(3)?,
            ))
        })
        .map_err(local)?;
    rows.map(|r| {
        let (sequence, id, message, failure) = r.map_err(local)?;
        let error = match failure {
            Some(bytes) => {
                borsh::from_slice(&bytes).map_err(|e| Error::local_kind(ErrorKind::Corrupt, e))?
            }
            None => Failure::legacy(message),
        };
        Ok(WritebackError {
            sequence: sequence as u64,
            handle: uuid::Uuid::from_slice(&id).map_err(local)?,
            error,
            persisted: true,
        })
    })
    .collect()
}
fn validate_state(c: &Connection, profile: &str) -> Result<()> {
    let _ = records(c)?;
    let _ = errors(c)?;
    if profile != PROFILE_V3 {
        return Ok(());
    }
    let handles = {
        let mut query = c.prepare("SELECT id,record FROM handles").map_err(local)?;
        let rows = query
            .query_map([], |r| {
                Ok((r.get::<_, Vec<u8>>(0)?, r.get::<_, Vec<u8>>(1)?))
            })
            .map_err(local)?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(local)?;
        let mut result = BTreeMap::new();
        for (id, bytes) in rows {
            if id.len() != 16 {
                return Err(Error::State(
                    ErrorKind::Corrupt,
                    "staging handle id is malformed",
                ));
            }
            let record = decode(&bytes)?;
            if record.id.as_slice() != id.as_slice() || record.length > i64::MAX as u64 {
                return Err(Error::State(
                    ErrorKind::Corrupt,
                    "staging handle identity or length is invalid",
                ));
            }
            result.insert(id, record);
        }
        result
    };
    let filesystem: Vec<u8> = c
        .query_row(
            "SELECT filesystem FROM authority WHERE singleton=1",
            [],
            |r| r.get(0),
        )
        .map_err(local)?;
    let bases = {
        let mut query = c
            .prepare("SELECT handle,descriptor,prefix FROM bases ORDER BY handle")
            .map_err(local)?;
        query
            .query_map([], |r| {
                Ok((
                    r.get::<_, Vec<u8>>(0)?,
                    r.get::<_, Vec<u8>>(1)?,
                    r.get::<_, i64>(2)?,
                ))
            })
            .map_err(local)?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(local)?
    };
    let mut sparse = BTreeMap::<Vec<u8>, (u64, u64)>::new();
    for (handle, descriptor, prefix) in bases {
        let handle: [u8; 16] = handle
            .try_into()
            .map_err(|_| Error::State(ErrorKind::Corrupt, "staging base handle is malformed"))?;
        let descriptor = ContentDescriptor::from_bytes(&descriptor)?;
        if descriptor.filesystem().as_bytes() != filesystem.as_slice() {
            return Err(Error::State(
                ErrorKind::Corrupt,
                "staging base belongs to a different filesystem",
            ));
        }
        let prefix = positioned(prefix)?;
        if prefix > descriptor.content_id().length() {
            return Err(Error::State(
                ErrorKind::Corrupt,
                "staging base prefix exceeds content length",
            ));
        }
        let Some(record) = handles.get(handle.as_slice()) else {
            return Err(Error::State(
                ErrorKind::Corrupt,
                "staging base has no handle",
            ));
        };
        if !record.ready {
            return Err(Error::State(
                ErrorKind::Corrupt,
                "staging base belongs to an incomplete handle",
            ));
        }
        if prefix > record.length {
            return Err(Error::State(
                ErrorKind::Corrupt,
                "staging base prefix exceeds handle length",
            ));
        }
        let materialized = c
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM chunks WHERE handle=?1)",
                params![handle.as_slice()],
                |r| r.get::<_, bool>(0),
            )
            .map_err(local)?;
        if materialized {
            return Err(Error::State(
                ErrorKind::Corrupt,
                "sparse handle also has materialized chunks",
            ));
        }
        sparse.insert(handle.to_vec(), (prefix, record.length));
    }

    let segments = {
        let mut query = c
            .prepare(
                "SELECT handle,position,length(data) FROM segments
                 ORDER BY handle,position",
            )
            .map_err(local)?;
        query
            .query_map([], |r| {
                Ok((
                    r.get::<_, Vec<u8>>(0)?,
                    r.get::<_, i64>(1)?,
                    r.get::<_, i64>(2)?,
                ))
            })
            .map_err(local)?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(local)?
    };
    let mut previous = BTreeMap::<Vec<u8>, u64>::new();
    for (handle, position, length) in segments {
        let Some((_, handle_length)) = sparse.get(&handle) else {
            return Err(Error::State(
                ErrorKind::Corrupt,
                "staging segment has no sparse base",
            ));
        };
        let data_length = positioned(length)?;
        if data_length == 0 {
            return Err(Error::State(ErrorKind::Corrupt, "staging segment is empty"));
        }
        let position = positioned(position)?;
        let end = position.checked_add(data_length).ok_or(Error::State(
            ErrorKind::Corrupt,
            "staging segment length overflows",
        ))?;
        let chunk_start = position - position % CHUNK;
        if end > chunk_start + CHUNK {
            return Err(Error::State(
                ErrorKind::Corrupt,
                "staging segment crosses a chunk boundary",
            ));
        }
        if end > *handle_length {
            return Err(Error::State(
                ErrorKind::Corrupt,
                "staging segment exceeds handle length",
            ));
        }
        if let Some(previous_end) = previous.insert(handle, end)
            && previous_end > position
        {
            return Err(Error::State(ErrorKind::Corrupt, "staging segments overlap"));
        }
    }
    Ok(())
}
fn positioned(position: i64) -> Result<u64> {
    u64::try_from(position).map_err(|e| Error::local_kind(ErrorKind::Corrupt, e))
}
fn segment_end(position: u64, data: &[u8]) -> Result<u64> {
    position.checked_add(data.len() as u64).ok_or(Error::State(
        ErrorKind::Corrupt,
        "staging segment length overflows",
    ))
}
fn read_sparse(
    c: &Connection,
    id: [u8; 16],
    offset: u64,
    requested_end: u64,
    base: Base,
) -> Result<StagedRead> {
    let mut bytes = vec![0; (requested_end - offset) as usize];
    let rows = {
        let mut query = c
            .prepare(
                "SELECT position,data FROM segments
                 WHERE handle=?1 AND position<?2 AND position+length(data)>?3
                 ORDER BY position",
            )
            .map_err(local)?;
        query
            .query_map(
                params![
                    id.as_slice(),
                    i64::try_from(requested_end).map_err(local)?,
                    i64::try_from(offset).map_err(local)?
                ],
                |r| Ok((r.get::<_, i64>(0)?, r.get::<_, Vec<u8>>(1)?)),
            )
            .map_err(local)?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(local)?
    };

    let mut local = Vec::with_capacity(rows.len());
    for (position, data) in rows {
        let start = positioned(position)?;
        let row_end = segment_end(start, &data)?;
        let copy_start = start.max(offset);
        let copy_end = row_end.min(requested_end);
        if copy_start >= copy_end {
            continue;
        }
        let source_start = (copy_start - start) as usize;
        let source_end = (copy_end - start) as usize;
        let destination_start = (copy_start - offset) as usize;
        let destination_end = (copy_end - offset) as usize;
        bytes[destination_start..destination_end].copy_from_slice(&data[source_start..source_end]);
        local.push(copy_start..copy_end);
    }

    let base_end = requested_end.min(base.prefix);
    let mut holes = Vec::new();
    let mut cursor = offset;
    for span in local {
        let span_start = span.start.min(base_end);
        if cursor < span_start {
            holes.push(cursor..span_start);
        }
        cursor = cursor.max(span.end);
    }
    if cursor < base_end {
        holes.push(cursor..base_end);
    }
    holes.retain(|range| !range.is_empty());
    Ok(StagedRead { bytes, base: holes })
}
fn write_sparse(c: &Connection, id: [u8; 16], offset: u64, bytes: &[u8]) -> Result<()> {
    let end = offset
        .checked_add(bytes.len() as u64)
        .ok_or(Error::State(ErrorKind::TooLarge, "file length overflows"))?;
    let mut chunk_start = offset - offset % CHUNK;
    while chunk_start < end {
        let part_start = offset.max(chunk_start);
        let part_end = end.min(chunk_start + CHUNK);
        let source_start = (part_start - offset) as usize;
        let source_end = (part_end - offset) as usize;
        write_sparse_chunk(c, id, part_start, &bytes[source_start..source_end])?;
        chunk_start += CHUNK;
    }
    Ok(())
}
fn write_sparse_chunk(c: &Connection, id: [u8; 16], offset: u64, bytes: &[u8]) -> Result<()> {
    let end = offset
        .checked_add(bytes.len() as u64)
        .ok_or(Error::State(ErrorKind::TooLarge, "file length overflows"))?;
    let rows = {
        let mut query = c
            .prepare(
                "SELECT position,data FROM segments
                 WHERE handle=?1 AND position<?2 AND position+length(data)>?3
                 ORDER BY position",
            )
            .map_err(local)?;
        query
            .query_map(
                params![
                    id.as_slice(),
                    i64::try_from(end).map_err(local)?,
                    i64::try_from(offset).map_err(local)?
                ],
                |r| Ok((r.get::<_, i64>(0)?, r.get::<_, Vec<u8>>(1)?)),
            )
            .map_err(local)?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(local)?
    };

    for (position, data) in rows {
        let start = positioned(position)?;
        let row_end = segment_end(start, &data)?;
        delete_segment(c, id, start)?;
        if start < offset {
            put_segment(c, id, start, &data[..(offset - start) as usize])?;
        }
        if row_end > end {
            put_segment(c, id, end, &data[(end - start) as usize..])?;
        }
    }
    coalesce_segment(c, id, offset, bytes)
}
fn coalesce_segment(c: &Connection, id: [u8; 16], position: u64, bytes: &[u8]) -> Result<()> {
    let mut start = position;
    let mut data = bytes.to_vec();
    let chunk_start = position - position % CHUNK;
    let previous: Option<(i64, Vec<u8>)> = c
        .query_row(
            "SELECT position,data FROM segments
             WHERE handle=?1 AND position<?2 ORDER BY position DESC LIMIT 1",
            params![id.as_slice(), i64::try_from(position).map_err(local)?],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()
        .map_err(local)?;
    if let Some((previous_position, previous_data)) = previous {
        let previous_position = positioned(previous_position)?;
        if segment_end(previous_position, &previous_data)? == position
            && previous_position / CHUNK == position / CHUNK
        {
            delete_segment(c, id, previous_position)?;
            start = previous_position;
            data = previous_data.into_iter().chain(data).collect();
        }
    }
    let end = segment_end(start, &data)?;
    let next: Option<Vec<u8>> = c
        .query_row(
            "SELECT data FROM segments WHERE handle=?1 AND position=?2",
            params![id.as_slice(), i64::try_from(end).map_err(local)?],
            |r| r.get(0),
        )
        .optional()
        .map_err(local)?;
    if let Some(next) = next
        && end < chunk_start + CHUNK
    {
        delete_segment(c, id, end)?;
        data.extend(next);
    }
    put_segment(c, id, start, &data)?;
    Ok(())
}
impl Stage {
    pub async fn validate(path: PathBuf, fs: NodeId) -> Result<()> {
        tokio::task::spawn_blocking(move || {
            let c = Connection::open_with_flags(
                path,
                OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
            )
            .map_err(local)?;
            c.busy_timeout(std::time::Duration::from_secs(1))
                .map_err(local)?;
            let profile = validate_profile(&c, Some(fs))?;
            validate_state(&c, &profile)?;
            Ok(())
        })
        .await
        .map_err(local)?
    }

    pub async fn inspect(path: PathBuf) -> Result<RuntimeStatus> {
        tokio::task::spawn_blocking(move || {
            let c = Connection::open_with_flags(
                path,
                OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
            )
            .map_err(local)?;
            c.busy_timeout(std::time::Duration::from_secs(1))
                .map_err(local)?;
            let profile = validate_profile(&c, None)?;
            validate_state(&c, &profile)?;
            Ok(RuntimeStatus {
                handles: records(&c)?.iter().map(Record::status).collect(),
                errors: errors(&c)?,
            })
        })
        .await
        .map_err(local)?
    }
    pub async fn open(path: PathBuf, fs: NodeId) -> Result<Self> {
        tokio::task::spawn_blocking(move || {
            std::fs::create_dir_all(&path).map_err(local)?;
            let database = path.join("staging.db");
            if database.try_exists().map_err(local)? {
                let c = Connection::open_with_flags(
                    &database,
                    OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
                )
                .map_err(local)?;
                c.busy_timeout(std::time::Duration::from_secs(1))
                    .map_err(local)?;
                if !schema::matches(&c, &[]).map_err(local)? {
                    let profile = validate_profile(&c, Some(fs))?;
                    if profile == PROFILE_V3 {
                        validate_state(&c, &profile)?;
                    }
                }
            }
            let lease = StagingLease::acquire_sync(&path)?;
            let exists = database.try_exists().map_err(local)?;
            let mut c = Connection::open_with_flags(
                &database,
                if exists {
                    OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX
                } else {
                    OpenFlags::SQLITE_OPEN_READ_WRITE
                        | OpenFlags::SQLITE_OPEN_CREATE
                        | OpenFlags::SQLITE_OPEN_NO_MUTEX
                },
            )
            .map_err(local)?;
            c.busy_timeout(std::time::Duration::from_secs(1))
                .map_err(local)?;
            // Revalidate after taking the process lease. A writer that finished
            // between preflight and locking may have performed a supported
            // upgrade; accept the resulting known profile.
            let profile = if exists && !schema::matches(&c, &[]).map_err(local)? {
                Some(validate_profile(&c, Some(fs))?)
            } else {
                None
            };
            let tx = c.transaction().map_err(local)?;
            match profile.as_deref() {
                None => {
                    tx.execute_batch(CREATE_SCHEMA_V3).map_err(local)?;
                    tx.execute(
                        "INSERT INTO authority VALUES(1,?1,?2)",
                        params![PROFILE_V3, fs.as_bytes().as_slice()],
                    )
                    .map_err(local)?;
                }
                Some(PROFILE_V1) => {
                    let _ = records(&tx)?;
                    let _ = errors(&tx)?;
                    tx.execute_batch(
                        "ALTER TABLE handles ADD COLUMN failure BLOB;
                         ALTER TABLE errors ADD COLUMN failure BLOB;",
                    )
                    .map_err(local)?;
                    tx.execute_batch(CREATE_SPARSE_TABLES).map_err(local)?;
                    tx.execute(
                        "UPDATE authority SET profile=?1 WHERE singleton=1",
                        [PROFILE_V3],
                    )
                    .map_err(local)?;
                }
                Some(PROFILE_V2) => {
                    let _ = records(&tx)?;
                    let _ = errors(&tx)?;
                    tx.execute_batch(CREATE_SPARSE_TABLES).map_err(local)?;
                    tx.execute(
                        "UPDATE authority SET profile=?1 WHERE singleton=1",
                        [PROFILE_V3],
                    )
                    .map_err(local)?;
                }
                Some(PROFILE_V3) => {
                    let _ = records(&tx)?;
                    let _ = errors(&tx)?;
                }
                Some(_) => unreachable!("profile validation rejected unsupported state"),
            }
            let profile = validate_profile(&tx, Some(fs))?;
            validate_state(&tx, &profile)?;
            // Incomplete opens have never been exposed or accepted application writes.
            let all = {
                let mut q = tx.prepare("SELECT record FROM handles").map_err(local)?;
                q.query_map([], |r| r.get::<_, Vec<u8>>(0))
                    .map_err(local)?
                    .collect::<std::result::Result<Vec<_>, _>>()
                    .map_err(local)?
            };
            for bytes in all {
                let r = decode(&bytes)?;
                if !r.ready {
                    tx.execute(
                        "DELETE FROM chunks WHERE handle=?1",
                        params![r.id.as_slice()],
                    )
                    .map_err(local)?;
                    tx.execute(
                        "DELETE FROM segments WHERE handle=?1",
                        params![r.id.as_slice()],
                    )
                    .map_err(local)?;
                    tx.execute(
                        "DELETE FROM bases WHERE handle=?1",
                        params![r.id.as_slice()],
                    )
                    .map_err(local)?;
                    tx.execute("DELETE FROM handles WHERE id=?1", params![r.id.as_slice()])
                        .map_err(local)?;
                }
            }
            tx.commit().map_err(local)?;
            c.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;")
                .map_err(local)?;
            Ok(Self {
                db: Arc::new(Mutex::new(c)),
                _lock: lease.file(),
            })
        })
        .await
        .map_err(local)?
    }
    async fn call<T: Send + 'static>(
        &self,
        f: impl FnOnce(&mut Connection) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        // Acquire in submission order, before queueing blocking work. The worker
        // owns the guard even if its caller is cancelled; a later fsync cannot
        // overtake an already submitted write in the blocking thread pool.
        let mut connection = self.db.clone().lock_owned().await;
        tokio::task::spawn_blocking(move || f(&mut connection))
            .await
            .map_err(local)?
    }
    pub async fn load(&self, id: [u8; 16]) -> Result<Record> {
        self.call(move |c| record(c, id)).await
    }
    pub async fn save(&self, r: Record) -> Result<()> {
        self.call(move |c| save(c, &r)).await
    }
    pub async fn base(&self, id: [u8; 16]) -> Result<Option<Base>> {
        self.call(move |c| base(c, id)).await
    }
    pub async fn segment_ranges(&self, id: [u8; 16]) -> Result<Vec<(u64, u64)>> {
        self.call(move |c| {
            let mut query = c
                .prepare(
                    "SELECT position,length(data) FROM segments
                     WHERE handle=?1 ORDER BY position",
                )
                .map_err(local)?;
            query
                .query_map(params![id.as_slice()], |r| {
                    Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?))
                })
                .map_err(local)?
                .map(|row| {
                    let (position, length) = row.map_err(local)?;
                    Ok((positioned(position)?, positioned(length)?))
                })
                .collect()
        })
        .await
    }
    pub async fn segments(&self, id: [u8; 16]) -> Result<Vec<(u64, Vec<u8>)>> {
        self.call(move |c| {
            let mut query = c
                .prepare(
                    "SELECT position,data FROM segments
                     WHERE handle=?1 ORDER BY position",
                )
                .map_err(local)?;
            query
                .query_map(params![id.as_slice()], |r| {
                    Ok((r.get::<_, i64>(0)?, r.get::<_, Vec<u8>>(1)?))
                })
                .map_err(local)?
                .map(|row| {
                    let (position, data) = row.map_err(local)?;
                    Ok((positioned(position)?, data))
                })
                .collect()
        })
        .await
    }
    pub async fn bind_base(&self, id: [u8; 16], descriptor: Vec<u8>, prefix: u64) -> Result<()> {
        self.call(move |c| {
            let tx = c.transaction().map_err(local)?;
            let mut r = record(&tx, id)?;
            if r.ready {
                return Err(Error::State(ErrorKind::Busy, "handle is already ready"));
            }
            put_base(&tx, id, &descriptor, prefix)?;
            r.ready = true;
            save(&tx, &r)?;
            tx.commit().map_err(local)?;
            Ok(())
        })
        .await
    }
    pub async fn complete_base(&self, r: Record, descriptor: Vec<u8>) -> Result<()> {
        self.call(move |c| {
            let tx = c.transaction().map_err(local)?;
            put_base(&tx, r.id, &descriptor, r.length)?;
            tx.execute(
                "DELETE FROM chunks WHERE handle=?1",
                params![r.id.as_slice()],
            )
            .map_err(local)?;
            tx.execute(
                "DELETE FROM segments WHERE handle=?1",
                params![r.id.as_slice()],
            )
            .map_err(local)?;
            save(&tx, &r)?;
            tx.commit().map_err(local)?;
            Ok(())
        })
        .await
    }
    pub async fn read(&self, id: [u8; 16], offset: u64, length: usize) -> Result<StagedRead> {
        self.call(move |c| {
            let r = record(c, id)?;
            let end = offset.saturating_add(length as u64).min(r.length);
            if offset >= end {
                return Ok(StagedRead {
                    bytes: Vec::new(),
                    base: Vec::new(),
                });
            }
            if let Some(base) = base(c, id)? {
                return read_sparse(c, id, offset, end, base);
            }
            let mut result = Vec::new();
            let mut position = offset;
            while position < end {
                let index = position / CHUNK;
                let mut bytes = chunk(c, id, index)?;
                bytes.resize(CHUNK as usize, 0);
                let start = (position % CHUNK) as usize;
                let size = ((end - position).min(CHUNK - start as u64)) as usize;
                result.extend_from_slice(&bytes[start..start + size]);
                position += size as u64;
            }
            Ok(StagedRead {
                bytes: result,
                base: Vec::new(),
            })
        })
        .await
    }
    pub async fn write(&self, id: [u8; 16], offset: Option<u64>, bytes: Vec<u8>) -> Result<u64> {
        self.call(move |c| {
            let tx = c.transaction().map_err(local)?;
            let mut r = record(&tx, id)?;
            mutable(&r)?;
            let offset = offset.unwrap_or(r.length);
            let end = offset
                .checked_add(bytes.len() as u64)
                .filter(|n| *n <= i64::MAX as u64)
                .ok_or(Error::State(ErrorKind::TooLarge, "file length overflows"))?;
            if bytes.is_empty() {
                return Ok(offset);
            }
            if base(&tx, id)?.is_some() {
                write_sparse(&tx, id, offset, &bytes)?;
            } else {
                let mut position = offset;
                while position < end {
                    let index = position / CHUNK;
                    let mut block = chunk(&tx, id, index)?;
                    block.resize(CHUNK as usize, 0);
                    let start = (position % CHUNK) as usize;
                    let size = (end - position).min(CHUNK - start as u64) as usize;
                    let source = (position - offset) as usize;
                    block[start..start + size].copy_from_slice(&bytes[source..source + size]);
                    put_chunk(&tx, id, index, &block)?;
                    position += size as u64;
                }
            }
            r.length = r.length.max(end);
            r.local = r
                .local
                .checked_add(1)
                .ok_or(Error::Invalid("generation exhausted"))?;
            save(&tx, &r)?;
            tx.commit().map_err(local)?;
            Ok(offset)
        })
        .await
    }
    pub async fn truncate(&self, id: [u8; 16], length: u64) -> Result<()> {
        self.resize(id, length, false).await
    }
    pub async fn reset_content(&self, id: [u8; 16]) -> Result<()> {
        self.resize(id, 0, true).await
    }
    async fn resize(&self, id: [u8; 16], length: u64, replacement: bool) -> Result<()> {
        self.call(move |c| {
            if length > i64::MAX as u64 {
                return Err(Error::State(ErrorKind::TooLarge, "file length overflows"));
            }
            let tx = c.transaction().map_err(local)?;
            let mut r = record(&tx, id)?;
            mutable(&r)?;
            if length == r.length && !replacement {
                return Ok(());
            }
            if base(&tx, id)?.is_some() {
                if length < r.length {
                    tx.execute(
                        "DELETE FROM segments WHERE handle=?1 AND position>=?2",
                        params![id.as_slice(), i64::try_from(length).map_err(local)?],
                    )
                    .map_err(local)?;
                    let crossing: Option<(i64, Vec<u8>)> = tx
                        .query_row(
                            "SELECT position,data FROM segments
                             WHERE handle=?1 AND position<?2
                             ORDER BY position DESC LIMIT 1",
                            params![id.as_slice(), i64::try_from(length).map_err(local)?],
                            |r| Ok((r.get(0)?, r.get(1)?)),
                        )
                        .optional()
                        .map_err(local)?;
                    if let Some((position, data)) = crossing {
                        let position = positioned(position)?;
                        if segment_end(position, &data)? > length {
                            put_segment(&tx, id, position, &data[..(length - position) as usize])?;
                        }
                    }
                    tx.execute(
                        "UPDATE bases SET prefix=MIN(prefix,?2) WHERE handle=?1",
                        params![id.as_slice(), i64::try_from(length).map_err(local)?],
                    )
                    .map_err(local)?;
                }
            } else if length < r.length {
                tx.execute(
                    "DELETE FROM chunks WHERE handle=?1 AND position>=?2",
                    params![id.as_slice(), length.div_ceil(CHUNK) as i64],
                )
                .map_err(local)?;
                if !length.is_multiple_of(CHUNK) {
                    let mut block = chunk(&tx, id, length / CHUNK)?;
                    block.truncate((length % CHUNK) as usize);
                    put_chunk(&tx, id, length / CHUNK, &block)?;
                }
            }
            r.length = length;
            r.local = r
                .local
                .checked_add(1)
                .ok_or(Error::Invalid("generation exhausted"))?;
            save(&tx, &r)?;
            tx.commit().map_err(local)?;
            Ok(())
        })
        .await
    }
    pub async fn failure(&self, id: [u8; 16], failure: Failure) -> Result<()> {
        self.call(move |c| {
            let tx = c.transaction().map_err(local)?;
            let mut r = record(&tx, id)?;
            r.error = Some(failure.to_string());
            r.failure = Some(failure.clone());
            save(&tx, &r)?;
            tx.execute(
                "INSERT INTO errors(handle,message,failure) VALUES(?1,?2,?3)",
                params![
                    id.as_slice(),
                    failure.to_string(),
                    borsh::to_vec(&failure).map_err(local)?
                ],
            )
            .map_err(local)?;
            tx.commit().map_err(local)?;
            Ok(())
        })
        .await
    }
    pub async fn remove(&self, id: [u8; 16]) -> Result<()> {
        self.call(move |c| {
            let tx = c.transaction().map_err(local)?;
            tx.execute("DELETE FROM chunks WHERE handle=?1", params![id.as_slice()])
                .map_err(local)?;
            tx.execute(
                "DELETE FROM segments WHERE handle=?1",
                params![id.as_slice()],
            )
            .map_err(local)?;
            tx.execute("DELETE FROM bases WHERE handle=?1", params![id.as_slice()])
                .map_err(local)?;
            tx.execute("DELETE FROM handles WHERE id=?1", params![id.as_slice()])
                .map_err(local)?;
            tx.commit().map_err(local)?;
            Ok(())
        })
        .await
    }
    pub async fn list(&self) -> Result<Vec<Record>> {
        self.call(|c| records(c)).await
    }
    pub async fn status(&self) -> Result<RuntimeStatus> {
        self.call(|c| {
            Ok(RuntimeStatus {
                handles: records(c)?.iter().map(Record::status).collect(),
                errors: errors(c)?,
            })
        })
        .await
    }
    pub async fn acknowledge(&self, through: u64) -> Result<()> {
        self.call(move |c| {
            c.execute(
                "DELETE FROM errors WHERE sequence<=?1",
                params![through.min(i64::MAX as u64) as i64],
            )
            .map_err(local)?;
            Ok(())
        })
        .await
    }
}
fn mutable(r: &Record) -> Result<()> {
    if !r.ready || !r.writable {
        return Err(Error::State(ErrorKind::ReadOnly, "handle is not writable"));
    }
    if r.plan.is_some() {
        return Err(Error::State(
            ErrorKind::Frozen,
            "resolve or abort the frozen request before writing",
        ));
    }
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;

    fn raw_database(path: &std::path::Path, profile: &str, typed_failures: bool) -> NodeId {
        let fs = NodeId::generate();
        let failure = if typed_failures { ",failure BLOB" } else { "" };
        let c = Connection::open(path).unwrap();
        c.execute_batch(&format!(
            "CREATE TABLE authority(singleton INTEGER PRIMARY KEY, profile TEXT, filesystem BLOB);
             CREATE TABLE handles(id BLOB PRIMARY KEY, record BLOB{handle_failure});
             CREATE TABLE chunks(handle BLOB, position INTEGER, data BLOB, PRIMARY KEY(handle,position));
             CREATE TABLE errors(sequence INTEGER PRIMARY KEY AUTOINCREMENT, handle BLOB, message TEXT{error_failure});",
            handle_failure = failure,
            error_failure = failure,
        ))
        .unwrap();
        c.execute(
            "INSERT INTO authority VALUES(1,?1,?2)",
            params![profile, fs.as_bytes().as_slice()],
        )
        .unwrap();
        fs
    }

    fn names(path: &std::path::Path) -> Vec<String> {
        let mut names = std::fs::read_dir(path)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        names.sort();
        names
    }

    #[tokio::test]
    async fn lease_only_acquisition_fences_stage_open_without_creating_a_database() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("state");
        let lease = StagingLease::acquire(path.clone()).await.unwrap();
        assert_eq!(names(&path), ["runtime.lock"]);
        assert!(!path.join("staging.db").exists());

        let filesystem = NodeId::from_bytes([17; 16]);
        let error = Stage::open(path.clone(), filesystem).await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Busy);
        assert!(!path.join("staging.db").exists());

        drop(lease);
        let stage = Stage::open(path.clone(), filesystem).await.unwrap();
        assert!(path.join("staging.db").exists());
        drop(stage);
    }

    #[tokio::test]
    async fn unsupported_profile_is_rejected_without_creating_a_lease_or_changing_sqlite() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("staging.db");
        let fs = raw_database(&path, "yinyang-stage-99", true);
        assert_eq!(
            Stage::inspect(path.clone()).await.unwrap_err().kind(),
            ErrorKind::Unsupported
        );
        assert_eq!(
            Stage::validate(path.clone(), fs).await.unwrap_err().kind(),
            ErrorKind::Unsupported
        );
        assert_eq!(
            Stage::open(temp.path().to_owned(), fs)
                .await
                .err()
                .unwrap()
                .kind(),
            ErrorKind::Unsupported
        );
        assert!(!temp.path().join("runtime.lock").exists());
        let c = Connection::open(path).unwrap();
        let mode: String = c
            .query_row("PRAGMA journal_mode", [], |row| row.get(0))
            .unwrap();
        assert_eq!(mode, "delete");
        let profile: String = c
            .query_row(
                "SELECT profile FROM authority WHERE singleton=1",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(profile, "yinyang-stage-99");
    }

    #[tokio::test]
    async fn unsupported_profile_read_only_rejection_preserves_authoritative_bytes() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("staging.db");
        let _ = raw_database(&path, "yinyang-stage-99", true);
        Connection::open(&path)
            .unwrap()
            .execute_batch("PRAGMA journal_mode=WAL")
            .unwrap();
        let before = std::fs::read(&path).unwrap();

        assert_eq!(
            Stage::inspect(path.clone()).await.unwrap_err().kind(),
            ErrorKind::Unsupported
        );
        assert_eq!(std::fs::read(&path).unwrap(), before);
        assert!(!temp.path().join("runtime.lock").exists());
        let files = names(temp.path());
        eprintln!("unknown-profile read-only rejection files: {:?}", files);
        assert!(
            files.iter().all(|name| matches!(
                name.as_str(),
                "staging.db" | "staging.db-wal" | "staging.db-shm"
            )),
            "read-only profile admission created a non-SQLite file: {:?}",
            files
        );
        assert!(temp.path().join("staging.db-shm").exists());
        let wal = temp.path().join("staging.db-wal");
        assert!(
            wal.exists(),
            "SQLite did not create the observed WAL sidecar"
        );
        assert_eq!(std::fs::metadata(wal).unwrap().len(), 0);
        assert!(!temp.path().join("staging.db-journal").exists());
    }

    #[tokio::test]
    async fn unsupported_profile_only_in_existing_wal_is_read_without_changing_wal() {
        let temp = tempfile::tempdir().unwrap();
        let source = temp.path().join("source.db");
        let _ = raw_database(&source, PROFILE_V2, true);
        let writer = Connection::open(&source).unwrap();
        writer
            .execute_batch(
                "PRAGMA journal_mode=WAL;
                 PRAGMA wal_autocheckpoint=0;
                 PRAGMA wal_checkpoint(TRUNCATE);
                 UPDATE authority SET profile='yinyang-stage-99' WHERE singleton=1;",
            )
            .unwrap();
        assert!(
            std::fs::metadata(source.with_file_name("source.db-wal"))
                .unwrap()
                .len()
                > 0
        );

        let state = temp.path().join("state");
        std::fs::create_dir(&state).unwrap();
        let staging = state.join("staging.db");
        std::fs::copy(&source, &staging).unwrap();
        let source_wal = source.with_file_name("source.db-wal");
        let staging_wal = state.join("staging.db-wal");
        std::fs::copy(&source_wal, &staging_wal).unwrap();
        drop(writer);

        let main_before = std::fs::read(&staging).unwrap();
        let wal_before = std::fs::read(&staging_wal).unwrap();
        assert_eq!(
            Stage::inspect(staging.clone()).await.unwrap_err().kind(),
            ErrorKind::Unsupported
        );
        assert_eq!(std::fs::read(&staging).unwrap(), main_before);
        assert_eq!(std::fs::read(&staging_wal).unwrap(), wal_before);
        assert!(state.join("staging.db-shm").exists());
        assert!(!state.join("staging.db-journal").exists());
    }

    #[tokio::test]
    async fn known_profile_with_partial_schema_is_not_repaired() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("staging.db");
        let fs = raw_database(&path, PROFILE_V2, false);
        assert_eq!(
            Stage::inspect(path.clone()).await.unwrap_err().kind(),
            ErrorKind::Corrupt
        );
        assert_eq!(
            Stage::open(temp.path().to_owned(), fs)
                .await
                .err()
                .unwrap()
                .kind(),
            ErrorKind::Corrupt
        );
        assert!(!temp.path().join("runtime.lock").exists());
        let c = Connection::open(path).unwrap();
        let handles: Vec<String> = {
            let mut statement = c.prepare("PRAGMA table_info(handles)").unwrap();
            statement
                .query_map([], |row| row.get(1))
                .unwrap()
                .collect::<rusqlite::Result<_>>()
                .unwrap()
        };
        assert_eq!(handles, ["id", "record"]);
    }

    #[tokio::test]
    async fn empty_database_is_initialized_after_acquiring_the_lease() {
        let temp = tempfile::tempdir().unwrap();
        std::fs::write(temp.path().join("staging.db"), []).unwrap();
        let fs = NodeId::generate();
        let stage = Stage::open(temp.path().to_owned(), fs).await.unwrap();
        assert!(temp.path().join("runtime.lock").exists());
        assert!(stage.list().await.unwrap().is_empty());
        Stage::validate(temp.path().join("staging.db"), fs)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn release_retains_typed_failure_when_the_ledger_is_unwritable() {
        let temp = tempfile::tempdir().unwrap();
        let backend = crate::support::TestBackend::default();
        let authority = Arc::new(
            yinyang_core::Fs::create(backend.operator(), yinyang_core::BackendProfile::Minio)
                .await
                .unwrap(),
        );
        let runtime = Runtime::open(authority.clone(), temp.path()).await.unwrap();
        runtime.create_file(authority.root(), "file").await.unwrap();
        let mut file = runtime.open_file("file", false).await.unwrap();
        runtime
            .inner
            .stage
            .db
            .lock()
            .await
            .execute_batch("PRAGMA query_only=ON")
            .unwrap();
        assert_eq!(
            file.write(0, b"rejected").await.unwrap_err().kind(),
            ErrorKind::ReadOnly
        );
        let id = file.release().unwrap();
        let status = runtime.status().await.unwrap();
        assert_eq!(status.errors.len(), 1);
        assert!(!status.errors[0].persisted);
        assert_eq!(status.errors[0].error.kind(), ErrorKind::ReadOnly);
        let mut file = runtime.recover(id).await.unwrap();
        assert_eq!(file.close().await.unwrap_err().kind(), ErrorKind::ReadOnly);
        runtime
            .inner
            .stage
            .db
            .lock()
            .await
            .execute_batch("PRAGMA query_only=OFF")
            .unwrap();
        file.acknowledge_error().await.unwrap();
        file.close().await.unwrap();
    }

    #[tokio::test]
    async fn stage_one_upgrade_preserves_pending_data_requests_and_legacy_errors() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("staging.db");
        let fs = NodeId::generate();
        let id = [7_u8; 16];
        // Independently encode stage-1 fields, including an opaque frozen request.
        let bytes = borsh::to_vec(&(
            id,
            [8_u8; 16],
            [9_u8; 24],
            3_u64,
            2_u64,
            1_u64,
            true,
            true,
            Some(vec![1_u8, 2, 3]),
            false,
            Some(String::from("legacy failure")),
        ))
        .unwrap();
        {
            let c = Connection::open(&path).unwrap();
            c.execute_batch("CREATE TABLE authority(singleton INTEGER PRIMARY KEY, profile TEXT, filesystem BLOB);
                CREATE TABLE handles(id BLOB PRIMARY KEY, record BLOB);
                CREATE TABLE chunks(handle BLOB, position INTEGER, data BLOB, PRIMARY KEY(handle,position));
                CREATE TABLE errors(sequence INTEGER PRIMARY KEY AUTOINCREMENT, handle BLOB, message TEXT);").unwrap();
            c.execute(
                "INSERT INTO authority VALUES(1,'yinyang-stage-1',?1)",
                [fs.as_bytes().as_slice()],
            )
            .unwrap();
            c.execute(
                "INSERT INTO handles VALUES(?1,?2)",
                params![id.as_slice(), &bytes],
            )
            .unwrap();
            c.execute(
                "INSERT INTO chunks VALUES(?1,0,?2)",
                params![id.as_slice(), b"abc".as_slice()],
            )
            .unwrap();
            c.execute(
                "INSERT INTO errors(handle,message) VALUES(?1,'legacy failure')",
                [id.as_slice()],
            )
            .unwrap();
        }
        let before = Stage::inspect(path.clone()).await.unwrap();
        assert_eq!(
            before.handles[0].error.as_ref().unwrap().kind(),
            ErrorKind::Unclassified
        );
        assert!(before.handles[0].pending && before.handles[0].frozen);
        let stage = Stage::open(temp.path().to_owned(), fs).await.unwrap();
        let r = stage.load(id).await.unwrap();
        assert_eq!(borsh::to_vec(&r).unwrap(), bytes);
        assert_eq!(stage.read(id, 0, 10).await.unwrap().bytes, b"abc");
        let commit = CommitId::generate();
        stage
            .failure(id, Failure::capture(&Error::Unknown(commit)))
            .await
            .unwrap();
        stage.acknowledge(u64::MAX).await.unwrap();
        drop(stage);
        let stage = Stage::open(temp.path().to_owned(), fs).await.unwrap();
        let status = stage.status().await.unwrap();
        assert!(status.errors.is_empty());
        assert_eq!(
            status.handles[0].error.as_ref().unwrap().commit_id(),
            Some(commit)
        );
        assert_eq!(stage.load(id).await.unwrap().plan, Some(vec![1, 2, 3]));
        assert_eq!(stage.read(id, 0, 3).await.unwrap().bytes, b"abc");
        assert_eq!(
            Stage::inspect(path).await.unwrap().handles[0]
                .error
                .as_ref()
                .unwrap()
                .kind(),
            ErrorKind::Unknown
        );
    }

    #[tokio::test]
    async fn stage_two_chunks_remain_full_materialization_after_upgrade() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("staging.db");
        let fs = raw_database(&path, PROFILE_V2, true);
        let id = [17_u8; 16];
        let record = Record {
            id,
            node: [18_u8; 16],
            base: [19_u8; 24],
            length: 16,
            local: 1,
            remote: 0,
            writable: true,
            ready: true,
            plan: None,
            conflict: false,
            error: None,
            failure: None,
        };
        {
            let c = Connection::open(&path).unwrap();
            c.execute(
                "INSERT INTO handles(id,record,failure) VALUES(?1,?2,NULL)",
                params![id.as_slice(), borsh::to_vec(&record).unwrap()],
            )
            .unwrap();
            c.execute(
                "INSERT INTO chunks VALUES(?1,0,?2)",
                params![id.as_slice(), b"old-materialized".as_slice()],
            )
            .unwrap();
        }

        let stage = Stage::open(temp.path().to_owned(), fs).await.unwrap();
        let read = stage.read(id, 0, 32).await.unwrap();
        assert_eq!(read.bytes, b"old-materialized");
        assert!(read.base.is_empty());
        assert!(stage.base(id).await.unwrap().is_none());
        let profile: String = stage
            .db
            .lock()
            .await
            .query_row(
                "SELECT profile FROM authority WHERE singleton=1",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(profile, PROFILE_V3);
    }

    #[tokio::test]
    async fn sparse_segments_overlay_only_dirty_bytes_and_keep_shrunk_extension_zero() {
        let backend = crate::support::TestBackend::default();
        let authority =
            yinyang_core::Fs::create(backend.operator(), yinyang_core::BackendProfile::Minio)
                .await
                .unwrap();
        let base = b"0123456789abcdef";
        let prepared = authority.prepare(&mut base.as_slice()).await.unwrap();
        let temp = tempfile::tempdir().unwrap();
        let stage = Stage::open(temp.path().to_owned(), authority.filesystem())
            .await
            .unwrap();
        let id = [21_u8; 16];
        let record = Record {
            id,
            node: *yinyang_core::NodeId::generate().as_bytes(),
            base: [7; 24],
            length: base.len() as u64,
            local: 0,
            remote: 0,
            writable: true,
            ready: false,
            plan: None,
            conflict: false,
            error: None,
            failure: None,
        };
        stage.save(record).await.unwrap();
        stage
            .bind_base(id, prepared.descriptor().to_bytes(), base.len() as u64)
            .await
            .unwrap();

        stage.write(id, Some(4), b"WXYZ".to_vec()).await.unwrap();
        let read = stage.read(id, 0, 12).await.unwrap();
        assert_eq!(&read.bytes[..4], &[0; 4]);
        assert_eq!(&read.bytes[4..8], b"WXYZ");
        assert_eq!(&read.bytes[8..], &[0; 4]);
        assert_eq!(read.base, [0..4, 8..12]);
        let segments = stage.segments(id).await.unwrap();
        assert_eq!(segments.len(), 1);
        assert_eq!(segments[0], (4, b"WXYZ".to_vec()));

        stage.truncate(id, 6).await.unwrap();
        stage.truncate(id, 12).await.unwrap();
        let read = stage.read(id, 0, 12).await.unwrap();
        assert_eq!(&read.bytes[..4], &[0; 4]);
        assert_eq!(&read.bytes[4..6], b"WX");
        assert_eq!(&read.bytes[6..], &[0; 6]);
        assert_eq!(read.base.len(), 1);
        assert_eq!(read.base[0], 0..4);
    }

    #[tokio::test]
    async fn sequential_sparse_writes_keep_rows_within_verification_chunks() {
        let backend = crate::support::TestBackend::default();
        let authority =
            yinyang_core::Fs::create(backend.operator(), yinyang_core::BackendProfile::Minio)
                .await
                .unwrap();
        let length = CHUNK * 3;
        let base = vec![0_u8; length as usize];
        let prepared = authority.prepare(&mut base.as_slice()).await.unwrap();
        let temp = tempfile::tempdir().unwrap();
        let stage = Stage::open(temp.path().to_owned(), authority.filesystem())
            .await
            .unwrap();
        let id = [22_u8; 16];
        stage
            .save(Record {
                id,
                node: *NodeId::generate().as_bytes(),
                base: [8; 24],
                length,
                local: 0,
                remote: 0,
                writable: true,
                ready: false,
                plan: None,
                conflict: false,
                error: None,
                failure: None,
            })
            .await
            .unwrap();
        stage
            .bind_base(id, prepared.descriptor().to_bytes(), length)
            .await
            .unwrap();

        for offset in (0..CHUNK * 2).step_by(4096) {
            stage
                .write(id, Some(offset), vec![0x5a; 4096])
                .await
                .unwrap();
        }
        assert_eq!(
            stage.segment_ranges(id).await.unwrap(),
            [(0, CHUNK), (CHUNK, CHUNK)]
        );
        let read = stage.read(id, CHUNK - 8, 16).await.unwrap();
        assert_eq!(read.bytes, [0x5a; 16]);
        assert!(read.base.is_empty());

        stage.truncate(id, CHUNK + 123).await.unwrap();
        stage.truncate(id, length).await.unwrap();
        assert_eq!(
            stage.segment_ranges(id).await.unwrap(),
            [(0, CHUNK), (CHUNK, 123)]
        );
        let read = stage.read(id, CHUNK + 120, 16).await.unwrap();
        assert_eq!(&read.bytes[..3], &[0x5a; 3]);
        assert_eq!(&read.bytes[3..], &[0; 13]);
    }

    #[test]
    fn local_failure_classification_does_not_parse_messages() {
        let full = rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_FULL),
            Some("opaque".into()),
        );
        assert_eq!(local(full).kind(), ErrorKind::NoSpace);
        assert_eq!(
            local(std::io::Error::from(std::io::ErrorKind::PermissionDenied)).kind(),
            ErrorKind::PermissionDenied
        );
        assert_eq!(
            local(rusqlite::Error::QueryReturnedNoRows).kind(),
            ErrorKind::NotFound
        );
    }

    #[test]
    fn cancelled_write_keeps_its_place_before_later_reads() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .max_blocking_threads(1)
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let temp = tempfile::tempdir().unwrap();
            let stage = Stage::open(temp.path().to_owned(), NodeId::generate())
                .await
                .unwrap();
            let (release, blocked) = std::sync::mpsc::channel();
            let (entered, waiting) = tokio::sync::oneshot::channel();
            let blocker = tokio::task::spawn_blocking(move || {
                entered.send(()).unwrap();
                blocked.recv().unwrap();
            });
            waiting.await.unwrap();
            let writer = stage.clone();
            let mut submitted = Box::pin(writer.call(|c| {
                c.execute(
                    "INSERT INTO errors(handle,message) VALUES(?1,'accepted')",
                    params![[0_u8; 16].as_slice()],
                )
                .map_err(local)?;
                Ok(())
            }));
            // The only blocking worker is occupied. Submission must hold the
            // connection before its SQL closure can start. Poll the call itself:
            // try_lock alone can observe a reserved permit before the writer has
            // resumed and submitted its blocking work.
            std::future::poll_fn(|cx| {
                assert!(std::future::Future::poll(submitted.as_mut(), cx).is_pending());
                assert!(stage.db.try_lock().is_err());
                std::task::Poll::Ready(())
            })
            .await;
            drop(submitted);
            let reader = tokio::spawn(async move { stage.status().await.unwrap() });
            release.send(()).unwrap();
            blocker.await.unwrap();
            assert_eq!(reader.await.unwrap().errors[0].error.message(), "accepted");
        });
    }
}
