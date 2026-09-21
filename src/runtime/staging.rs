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
            error: self.error.clone(),
        }
    }
}
#[derive(Clone)]
pub(super) struct Stage {
    db: Arc<Mutex<Connection>>,
    _lock: Arc<File>,
}
fn local(e: impl std::fmt::Display) -> Error {
    Error::Local(e.to_string())
}
fn decode(bytes: &[u8]) -> Result<Record> {
    borsh::from_slice(bytes).map_err(local)
}
fn save(c: &Connection, r: &Record) -> Result<()> {
    c.execute(
        "INSERT OR REPLACE INTO handles VALUES(?1,?2)",
        params![r.id.as_slice(), borsh::to_vec(r).map_err(local)?],
    )
    .map_err(local)?;
    Ok(())
}
fn record(c: &Connection, id: [u8; 16]) -> Result<Record> {
    let bytes: Vec<u8> = c
        .query_row(
            "SELECT record FROM handles WHERE id=?1",
            params![id.as_slice()],
            |r| r.get(0),
        )
        .map_err(local)?;
    decode(&bytes)
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
    let mut query = c
        .prepare("SELECT record FROM handles ORDER BY id")
        .map_err(local)?;
    let bytes = query
        .query_map([], |r| r.get::<_, Vec<u8>>(0))
        .map_err(local)?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(local)?;
    bytes
        .iter()
        .map(|b| decode(b))
        .filter(|r| !matches!(r, Ok(record) if !record.ready))
        .collect()
}
fn errors(c: &Connection) -> Result<Vec<WritebackError>> {
    let mut query = c
        .prepare("SELECT sequence,handle,message FROM errors ORDER BY sequence")
        .map_err(local)?;
    let rows = query
        .query_map([], |r| {
            Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, Vec<u8>>(1)?,
                r.get::<_, String>(2)?,
            ))
        })
        .map_err(local)?;
    rows.map(|r| {
        let (sequence, id, message) = r.map_err(local)?;
        Ok(WritebackError {
            sequence: sequence as u64,
            handle: uuid::Uuid::from_slice(&id).map_err(local)?,
            message,
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
            if profile != "yinyang-stage-1" {
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
            lock.try_lock().map_err(|_|Error::Invalid("staging directory is already in use"))?;
            let mut c = Connection::open(path.join("staging.db")).map_err(local)?;
            c.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;
                CREATE TABLE IF NOT EXISTS authority(singleton INTEGER PRIMARY KEY CHECK(singleton=1), profile TEXT NOT NULL, filesystem BLOB NOT NULL);
                CREATE TABLE IF NOT EXISTS handles(id BLOB PRIMARY KEY, record BLOB NOT NULL);
                CREATE TABLE IF NOT EXISTS chunks(handle BLOB NOT NULL, position INTEGER NOT NULL, data BLOB NOT NULL, PRIMARY KEY(handle,position));
                CREATE TABLE IF NOT EXISTS errors(sequence INTEGER PRIMARY KEY AUTOINCREMENT, handle BLOB NOT NULL, message TEXT NOT NULL);").map_err(local)?;
            let tx = c.transaction().map_err(local)?;
            tx.execute("INSERT OR IGNORE INTO authority VALUES(1,'yinyang-stage-1',?1)",params![fs.as_bytes().as_slice()]).map_err(local)?;
            let (profile,bound): (String,Vec<u8>) = tx.query_row("SELECT profile,filesystem FROM authority WHERE singleton=1",[],|r|Ok((r.get(0)?,r.get(1)?))).map_err(local)?;
            if profile!="yinyang-stage-1" || bound!=fs.as_bytes() { return Err(Error::Invalid("staging belongs to a different filesystem or profile")); }
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
                .ok_or(Error::Invalid("file length overflows"))?;
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
                return Err(Error::Invalid("file length overflows"));
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
    pub async fn failure(&self, id: [u8; 16], message: String) -> Result<()> {
        self.call(move |c| {
            let tx = c.transaction().map_err(local)?;
            let mut r = record(&tx, id)?;
            r.error = Some(message.clone());
            save(&tx, &r)?;
            tx.execute(
                "INSERT INTO errors(handle,message) VALUES(?1,?2)",
                params![id.as_slice(), message],
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
        return Err(Error::Invalid("handle is not writable"));
    }
    if r.plan.is_some() {
        return Err(Error::Invalid(
            "resolve or abort the frozen request before writing",
        ));
    }
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;

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
            assert_eq!(reader.await.unwrap().errors[0].message, "accepted");
        });
    }
}
