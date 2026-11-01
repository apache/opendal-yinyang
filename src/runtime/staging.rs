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

//! Local write staging. SQLite commits each accepted write and its generation
//! atomically; explicit barriers make the WAL durable.
use super::*;
use crate::local::store;
use borsh::{BorshDeserialize, BorshSerialize};
use rusqlite::{Connection, OptionalExtension, params};
use std::fs::File;
use std::ops::Range;
use tokio::sync::Mutex;

pub(super) const CHUNK: u64 = 64 * 1024;
const SCHEMA: &[store::TableColumns<'static>] = &[
    ("authority", &["singleton", "profile", "filesystem"]),
    ("bases", &["handle", "descriptor", "prefix"]),
    ("handles", &["id", "record", "failure"]),
    ("chunks", &["handle", "position", "data"]),
    ("errors", &["sequence", "handle", "message", "failure"]),
    ("segments", &["handle", "position", "data"]),
];
const PROFILE: store::Profile = store::Profile {
    name: "yinyang-stage-4",
    table: "authority",
    schema: SCHEMA,
};

const CREATE_SCHEMA: &str = "
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
      failure BLOB NOT NULL
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
            error: self.failure.clone(),
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
    #[cfg(test)]
    barrier_calls: Arc<std::sync::atomic::AtomicUsize>,
}
fn store_error(error: store::Error) -> Error {
    let kind = match error.kind() {
        store::ErrorKind::Invalid => ErrorKind::Invalid,
        store::ErrorKind::Unsupported => ErrorKind::Unsupported,
        store::ErrorKind::Corrupt => ErrorKind::Corrupt,
        store::ErrorKind::Busy => ErrorKind::Busy,
        store::ErrorKind::AlreadyExists => ErrorKind::AlreadyExists,
        store::ErrorKind::NotFound => ErrorKind::NotFound,
        store::ErrorKind::ReadOnly => ErrorKind::ReadOnly,
        store::ErrorKind::PermissionDenied => ErrorKind::PermissionDenied,
        store::ErrorKind::NoSpace => ErrorKind::NoSpace,
        store::ErrorKind::Io => ErrorKind::Io,
    };
    Error::local_kind(kind, error)
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
fn validate_store(c: &Connection, filesystem: Option<NodeId>) -> Result<()> {
    let bindings = filesystem.map(|filesystem| {
        [store::Binding::blob(
            "filesystem",
            filesystem.as_bytes().to_vec(),
        )]
    });
    store::validate(
        c,
        &PROFILE,
        bindings.as_ref().map(|bindings| bindings.as_slice()),
    )
    .map_err(store_error)
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
        "INSERT OR REPLACE INTO chunks(handle,position,data) VALUES(?1,?2,?3)",
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
    let mut query = c
        .prepare("SELECT record,failure FROM handles ORDER BY id")
        .map_err(local)?;
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
    let mut query = c
        .prepare("SELECT sequence,handle,message,failure FROM errors ORDER BY sequence")
        .map_err(local)?;
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
        let (sequence, id, _message, failure) = r.map_err(local)?;
        let bytes = failure.ok_or(Error::State(
            ErrorKind::Corrupt,
            "staging error entry has no typed failure",
        ))?;
        let error =
            borsh::from_slice(&bytes).map_err(|e| Error::local_kind(ErrorKind::Corrupt, e))?;
        Ok(WritebackError {
            sequence: sequence as u64,
            handle: uuid::Uuid::from_slice(&id).map_err(local)?,
            error,
            persisted: true,
        })
    })
    .collect()
}
fn validate_state(c: &Connection) -> Result<()> {
    let _ = records(c)?;
    let _ = errors(c)?;
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
            let c = store::open_read_only(&path).map_err(store_error)?;
            validate_store(&c, Some(fs))?;
            validate_state(&c)?;
            Ok(())
        })
        .await
        .map_err(local)?
    }

    pub async fn inspect(path: PathBuf) -> Result<RuntimeStatus> {
        tokio::task::spawn_blocking(move || {
            let c = store::open_read_only(&path).map_err(store_error)?;
            validate_store(&c, None)?;
            validate_state(&c)?;
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
                let c = store::open_read_only(&database).map_err(store_error)?;
                if !store::is_empty(&c).map_err(store_error)? {
                    validate_store(&c, Some(fs))?;
                    validate_state(&c)?;
                }
            }
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
            let exists = database.try_exists().map_err(local)?;
            let mut c = store::open_read_write(&database, !exists).map_err(store_error)?;
            let empty = store::is_empty(&c).map_err(store_error)?;
            let tx = c.transaction().map_err(local)?;
            if empty {
                store::initialize(
                    &tx,
                    &PROFILE,
                    CREATE_SCHEMA,
                    &[store::Binding::blob("filesystem", fs.as_bytes().to_vec())],
                )
                .map_err(store_error)?;
            } else {
                validate_store(&tx, Some(fs))?;
            }
            validate_state(&tx)?;
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
            store::configure(&c).map_err(store_error)?;
            Ok(Self {
                db: Arc::new(Mutex::new(c)),
                _lock: Arc::new(lock),
                #[cfg(test)]
                barrier_calls: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
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
    pub async fn barrier(&self) -> Result<()> {
        #[cfg(test)]
        let barrier_calls = self.barrier_calls.clone();
        self.call(move |c| {
            store::barrier(c).map_err(store_error)?;
            #[cfg(test)]
            barrier_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        })
        .await
    }
    #[cfg(test)]
    pub fn barrier_counter(&self) -> Arc<std::sync::atomic::AtomicUsize> {
        self.barrier_calls.clone()
    }
    #[cfg(test)]
    pub async fn set_synchronous(&self, synchronous: &'static str) -> Result<()> {
        let pragma = match synchronous {
            "NORMAL" => "PRAGMA synchronous=NORMAL",
            "FULL" => "PRAGMA synchronous=FULL",
            _ => return Err(Error::Invalid("unsupported synchronous mode")),
        };
        self.call(move |connection| {
            connection.execute_batch(pragma).map_err(local)?;
            Ok(())
        })
        .await
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

    fn raw_database(path: &std::path::Path, profile: &str) -> NodeId {
        let fs = NodeId::generate();
        let c = Connection::open(path).unwrap();
        c.execute_batch(CREATE_SCHEMA).unwrap();
        c.execute(
            "INSERT INTO authority(singleton,profile,filesystem) VALUES(1,?1,?2)",
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
    async fn unsupported_profile_is_rejected_without_creating_a_lease_or_changing_sqlite() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("staging.db");
        let fs = raw_database(&path, "yinyang-stage-3");
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
        assert_eq!(profile, "yinyang-stage-3");
    }

    #[tokio::test]
    async fn unsupported_profile_read_only_rejection_preserves_authoritative_bytes() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("staging.db");
        let _ = raw_database(&path, "yinyang-stage-3");
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
        let _ = raw_database(&source, PROFILE.name);
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
        let fs = raw_database(&path, PROFILE.name);
        Connection::open(&path)
            .unwrap()
            .execute_batch("DROP TABLE segments")
            .unwrap();
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
        let segments: bool = c
            .query_row(
                "SELECT EXISTS(
                    SELECT 1 FROM sqlite_master WHERE type='table' AND name='segments'
                 )",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(!segments);
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
                let failure =
                    borsh::to_vec(&Failure::capture(&Error::Invalid("accepted"))).unwrap();
                c.execute(
                    "INSERT INTO errors(handle,message,failure) VALUES(?1,'accepted',?2)",
                    params![[0_u8; 16].as_slice(), failure],
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
