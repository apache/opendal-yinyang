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
//! generation atomic; chunks bound memory independently of the file length.
use super::*;
use borsh::{BorshDeserialize, BorshSerialize};
use rusqlite::{Connection, OptionalExtension, params};
use std::fs::File;
use tokio::sync::Mutex;

pub(super) const CHUNK: u64 = 64 * 1024;
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
        }
    }
}
#[derive(Clone)]
pub(super) struct Stage {
    db: Arc<Mutex<Connection>>,
    _lock: Arc<File>,
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
impl Stage {
    pub async fn inspect(path: PathBuf) -> Result<RuntimeStatus> {
        tokio::task::spawn_blocking(move || {
            let c = Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
                .map_err(local)?;
            let profile: String = c
                .query_row("SELECT profile FROM authority WHERE singleton=1", [], |r| {
                    r.get(0)
                })
                .map_err(local)?;
            if profile != "yinyang-stage-1" && profile != "yinyang-stage-2" {
                return Err(Error::Invalid("unknown staging profile"));
            }
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
            let lock = std::fs::OpenOptions::new().create(true).truncate(false).read(true).write(true).open(path.join("runtime.lock")).map_err(local)?;
            lock.try_lock().map_err(|e| match e {
                std::fs::TryLockError::WouldBlock => Error::State(ErrorKind::Busy, "staging directory is already in use"),
                std::fs::TryLockError::Error(e) => local(e),
            })?;
            let mut c = Connection::open(path.join("staging.db")).map_err(local)?;
            c.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;
                CREATE TABLE IF NOT EXISTS authority(singleton INTEGER PRIMARY KEY CHECK(singleton=1), profile TEXT NOT NULL, filesystem BLOB NOT NULL);
                CREATE TABLE IF NOT EXISTS handles(id BLOB PRIMARY KEY, record BLOB NOT NULL);
                CREATE TABLE IF NOT EXISTS chunks(handle BLOB NOT NULL, position INTEGER NOT NULL, data BLOB NOT NULL, PRIMARY KEY(handle,position));
                CREATE TABLE IF NOT EXISTS errors(sequence INTEGER PRIMARY KEY AUTOINCREMENT, handle BLOB NOT NULL, message TEXT NOT NULL);").map_err(local)?;
            let tx = c.transaction().map_err(local)?;
            tx.execute("INSERT OR IGNORE INTO authority VALUES(1,'yinyang-stage-1',?1)",params![fs.as_bytes().as_slice()]).map_err(local)?;
            let (profile,bound): (String,Vec<u8>) = tx.query_row("SELECT profile,filesystem FROM authority WHERE singleton=1",[],|r|Ok((r.get(0)?,r.get(1)?))).map_err(local)?;
            if (profile!="yinyang-stage-1" && profile!="yinyang-stage-2") || bound!=fs.as_bytes() { return Err(Error::Invalid("staging belongs to a different filesystem or profile")); }
            for table in ["handles", "errors"] {
                if !has_failure(&tx, table)? {
                    tx.execute_batch(&format!("ALTER TABLE {table} ADD COLUMN failure BLOB")).map_err(local)?;
                }
            }
            tx.execute("UPDATE authority SET profile='yinyang-stage-2' WHERE singleton=1", []).map_err(local)?;
            // Incomplete opens have never been exposed or accepted application writes.
            let all = {
                let mut q=tx.prepare("SELECT record FROM handles").map_err(local)?;
                q.query_map([],|r|r.get::<_,Vec<u8>>(0)).map_err(local)?.collect::<std::result::Result<Vec<_>,_>>().map_err(local)?
            };
            for bytes in all {
                let r=decode(&bytes)?;
                if !r.ready {
                    tx.execute("DELETE FROM chunks WHERE handle=?1",params![r.id.as_slice()]).map_err(local)?;
                    tx.execute("DELETE FROM handles WHERE id=?1",params![r.id.as_slice()]).map_err(local)?;
                }
            }
            tx.commit().map_err(local)?;
            Ok(Self{db:Arc::new(Mutex::new(c)),_lock:Arc::new(lock)})
        }).await.map_err(local)?
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
    pub async fn initial_chunk(&self, id: [u8; 16], index: u64, bytes: Vec<u8>) -> Result<()> {
        self.call(move |c| put_chunk(c, id, index, &bytes)).await
    }
    pub async fn read(&self, id: [u8; 16], offset: u64, length: usize) -> Result<Vec<u8>> {
        self.call(move |c| {
            let r = record(c, id)?;
            let end = offset.saturating_add(length as u64).min(r.length);
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
            Ok(result)
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
        self.call(move |c| {
            if length > i64::MAX as u64 {
                return Err(Error::State(ErrorKind::TooLarge, "file length overflows"));
            }
            let tx = c.transaction().map_err(local)?;
            let mut r = record(&tx, id)?;
            mutable(&r)?;
            if length == r.length {
                return Ok(());
            }
            if length < r.length {
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
        assert_eq!(stage.read(id, 0, 10).await.unwrap(), b"abc");
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
        assert_eq!(stage.read(id, 0, 3).await.unwrap(), b"abc");
        assert_eq!(
            Stage::inspect(path).await.unwrap().handles[0]
                .error
                .as_ref()
                .unwrap()
                .kind(),
            ErrorKind::Unknown
        );
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
            let submitted = tokio::spawn(async move {
                writer
                    .call(|c| {
                        c.execute(
                            "INSERT INTO errors(handle,message) VALUES(?1,'accepted')",
                            params![[0_u8; 16].as_slice()],
                        )
                        .map_err(local)?;
                        Ok(())
                    })
                    .await
            });
            // The only blocking worker is occupied. Submission must hold the
            // connection before its SQL closure can start.
            tokio::time::timeout(std::time::Duration::from_secs(5), async {
                while stage.db.try_lock().is_ok() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            submitted.abort();
            let _ = submitted.await;
            let reader = tokio::spawn(async move { stage.status().await.unwrap() });
            release.send(()).unwrap();
            blocker.await.unwrap();
            assert_eq!(reader.await.unwrap().errors[0].error.message(), "accepted");
        });
    }
}
