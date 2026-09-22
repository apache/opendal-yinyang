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

//! Durable single-authority metadata, independent of object-store head CAS.
//! SQLite files must reside on a local filesystem that honors sync and locking.

use crate::namespace::{Node, NodeKind, decode, encode};
use crate::snapshot::{
    ChangeRecord, Cursor, Outcome, Receipt, RecordReader, Rows, Snapshot, Table,
};
use crate::{
    ContentDescriptor, DataStore, Error, ErrorKind, NodeId, PreparedContent, Result, Revision,
    Transaction,
};
use futures_util::future::BoxFuture;
use opendal::Operator;
use rusqlite::{Connection, OptionalExtension, params};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::AsyncRead;

mod transport;
pub use transport::{ServiceClient, serve};

const PROFILE: &str = "yinyang-sqlite-1";
type Db = Arc<Mutex<Connection>>;

/// One durable filesystem authority. Clones and independent processes coordinate
/// through SQLite; no process-local cache determines the latest revision.
#[derive(Clone, Debug)]
pub struct MetadataService {
    path: PathBuf,
    data: DataStore,
    filesystem: NodeId,
    root: NodeId,
}

fn sql(error: rusqlite::Error) -> Error {
    Error::new(ErrorKind::Storage, "metadata database", error.to_string())
}
async fn db_call<T: Send + 'static>(
    db: &Db,
    call: impl FnOnce(&mut Connection) -> Result<T> + Send + 'static,
) -> Result<T> {
    let db = db.clone();
    tokio::task::spawn_blocking(move || {
        let mut connection = db.lock().map_err(|_| {
            Error::new(
                ErrorKind::Storage,
                "metadata database",
                "connection lock poisoned",
            )
        })?;
        call(&mut connection)
    })
    .await
    .map_err(|e| {
        Error::new(
            ErrorKind::Storage,
            "metadata database worker",
            e.to_string(),
        )
    })?
}
async fn connect(path: &Path, create: bool) -> Result<Db> {
    let path = path.to_owned();
    tokio::task::spawn_blocking(move || {
        let mut flags = rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE;
        if create {
            flags |= rusqlite::OpenFlags::SQLITE_OPEN_CREATE;
        }
        let conn = Connection::open_with_flags(path, flags).map_err(sql)?;
        conn.busy_timeout(Duration::from_millis(250)).map_err(sql)?;
        conn.execute_batch(
            "PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA foreign_keys=ON;",
        )
        .map_err(sql)?;
        Ok(Arc::new(Mutex::new(conn)))
    })
    .await
    .map_err(|e| Error::new(ErrorKind::Storage, "open metadata worker", e.to_string()))?
}
fn manifest(conn: &Connection) -> Result<(NodeId, NodeId, Revision)> {
    let (profile, fs, root, revision): (String, Vec<u8>, Vec<u8>, Vec<u8>) = conn
        .query_row(
            "SELECT profile, filesystem, root, latest FROM authority WHERE singleton=1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        )
        .map_err(sql)?;
    if profile != PROFILE {
        return Err(Error::unsupported(
            "open metadata authority",
            "unknown schema profile",
        ));
    }
    Ok((
        NodeId::from_bytes(fixed(fs)?),
        NodeId::from_bytes(fixed(root)?),
        Revision::from_bytes(fixed(revision)?),
    ))
}
fn fixed<const N: usize>(bytes: Vec<u8>) -> Result<[u8; N]> {
    bytes.try_into().map_err(|_| {
        Error::new(
            ErrorKind::Corrupt,
            "read metadata database",
            "invalid identity length",
        )
    })
}
impl MetadataService {
    /// Initialize a new local database or reopen its exact existing authority.
    pub async fn create(path: impl AsRef<Path>, operator: Operator) -> Result<Self> {
        let path = path.as_ref().to_owned();
        let db = connect(&path, true).await?;
        let (filesystem, root, _) = db_call(&db, |conn| {
            let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate).map_err(sql)?;
            tx.execute_batch("CREATE TABLE IF NOT EXISTS authority(singleton INTEGER PRIMARY KEY CHECK(singleton=1), profile TEXT NOT NULL, filesystem BLOB NOT NULL, root BLOB NOT NULL, latest BLOB NOT NULL);
                CREATE TABLE IF NOT EXISTS revisions(token BLOB PRIMARY KEY, sequence INTEGER UNIQUE NOT NULL);
                CREATE TABLE IF NOT EXISTS records(family INTEGER NOT NULL, key BLOB NOT NULL, start INTEGER NOT NULL, end INTEGER, value BLOB NOT NULL, PRIMARY KEY(family,key,start));
                CREATE INDEX IF NOT EXISTS records_current ON records(family,key) WHERE end IS NULL;
                CREATE TABLE IF NOT EXISTS prepared(descriptor BLOB PRIMARY KEY);").map_err(sql)?;
            if tx.query_row("SELECT count(*) FROM authority", [], |r| r.get::<_,i64>(0)).map_err(sql)? == 0 {
                let fs = NodeId::generate();
                let root = NodeId::generate();
                let revision = Revision::new(0);
                tx.execute("INSERT INTO authority VALUES(1,?1,?2,?3,?4)",params![PROFILE,fs.as_bytes(),root.as_bytes(),revision.to_bytes().as_slice()]).map_err(sql)?;
                tx.execute("INSERT INTO revisions VALUES(?1,0)", params![revision.to_bytes().as_slice()]).map_err(sql)?;
                let node = Node {id:root, generation:1, executable:false, link:None, kind:NodeKind::Directory{membership:1}};
                tx.execute("INSERT INTO records VALUES(0,?1,0,NULL,?2)", params![root.as_bytes(),node.encode()?]).map_err(sql)?;
            }
            let result = manifest(&tx)?;
            tx.commit().map_err(sql)?;
            Ok(result)
        }).await?;
        let fs = Self {
            path,
            data: DataStore::new(operator.clone(), filesystem)?,
            filesystem,
            root,
        };
        let marker = fs.marker()?;
        match operator
            .write_with(".yinyang/head", marker.clone())
            .if_not_exists(true)
            .await
        {
            Ok(_) => {}
            Err(_) => {
                fs.verify_binding(&operator).await?;
            }
        }
        fs.verify_binding(&operator).await?;
        Ok(fs)
    }
    pub async fn open(path: impl AsRef<Path>, operator: Operator) -> Result<Self> {
        let path = path.as_ref().to_owned();
        let db = connect(&path, false).await?;
        let (filesystem, root, _) = db_call(&db, |c| manifest(c)).await?;
        let fs = Self {
            path,
            data: DataStore::new(operator.clone(), filesystem)?,
            filesystem,
            root,
        };
        fs.verify_binding(&operator).await?;
        Ok(fs)
    }
    fn marker(&self) -> Result<Vec<u8>> {
        encode(&(
            *b"YYSERV01",
            *self.filesystem.as_bytes(),
            *self.root.as_bytes(),
        ))
    }
    async fn verify_binding(&self, operator: &Operator) -> Result<()> {
        let marker = operator
            .read(".yinyang/head")
            .await
            .map_err(|e| Error::from_storage("read authority binding", e))?;
        if marker.to_vec() != self.marker()? {
            return Err(Error::invalid(
                "open metadata authority",
                "storage belongs to another filesystem or publication mode",
            ));
        }
        Ok(())
    }
    pub fn data(&self) -> &DataStore {
        &self.data
    }
    pub fn filesystem(&self) -> NodeId {
        self.filesystem
    }
    pub fn root(&self) -> NodeId {
        self.root
    }
    fn snapshot(&self, db: Db, revision: Revision) -> Snapshot {
        Snapshot {
            data: self.data.clone(),
            filesystem: self.filesystem,
            root: self.root,
            revision,
            reader: Arc::new(SqlRecords {
                db,
                sequence: revision.sequence,
            }),
        }
    }
    pub async fn observe_latest(&self) -> Result<Snapshot> {
        let db = connect(&self.path, false).await?;
        let (fs, root, revision) = db_call(&db, |c| manifest(c)).await?;
        if fs != self.filesystem || root != self.root {
            return Err(Error::invalid("observe service", "authority changed"));
        }
        Ok(self.snapshot(db, revision))
    }
    pub async fn observe_revision(&self, revision: Revision) -> Result<Snapshot> {
        if revision.sequence > i64::MAX as u64 {
            return Err(Error::new(
                ErrorKind::NotFound,
                "observe service revision",
                "revision not retained",
            ));
        }
        let db = connect(&self.path, false).await?;
        db_call(&db, move |c| {
            let exists: bool = c
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM revisions WHERE token=?1 AND sequence=?2)",
                    params![revision.to_bytes().as_slice(), revision.sequence as i64],
                    |r| r.get(0),
                )
                .map_err(sql)?;
            if !exists {
                return Err(Error::new(
                    ErrorKind::NotFound,
                    "observe service revision",
                    "revision not retained",
                ));
            }
            Ok(())
        })
        .await?;
        Ok(self.snapshot(db, revision))
    }
    /// In-process preparation already carries this service's trusted evidence.
    pub async fn prepare(&self, source: &mut (impl AsyncRead + Unpin)) -> Result<PreparedContent> {
        let content = self.data.prepare(source).await?;
        self.remember(&content).await?;
        Ok(content)
    }
    pub(crate) async fn remember(&self, content: &PreparedContent) -> Result<()> {
        let descriptor = self.data.accept(content)?.to_bytes();
        let db = connect(&self.path, false).await?;
        db_call(&db, move |c| {
            c.execute(
                "INSERT OR IGNORE INTO prepared VALUES(?1)",
                params![descriptor],
            )
            .map_err(sql)?;
            Ok(())
        })
        .await
    }
    /// Remote descriptors are untrusted: verify once before issuing durable
    /// readiness. Subsequent commit and restart do not download them again.
    pub async fn register(&self, descriptor: &ContentDescriptor) -> Result<()> {
        let content = self.data.import(descriptor).await?;
        self.remember(&content).await
    }
    pub async fn restore_transaction(&self, bytes: &[u8]) -> Result<Transaction> {
        let (revision, files) = Transaction::inspect(bytes)?;
        let snapshot = self.observe_revision(revision).await?;
        let db = connect(&self.path, false).await?;
        let mut prepared = Vec::new();
        for file in files {
            let descriptor = file.to_bytes();
            let present = db_call(&db, move |c| {
                c.query_row(
                    "SELECT EXISTS(SELECT 1 FROM prepared WHERE descriptor=?1)",
                    params![descriptor],
                    |r| r.get::<_, bool>(0),
                )
                .map_err(sql)
            })
            .await?;
            if !present {
                return Err(Error::invalid(
                    "restore service plan",
                    "content has no durable preparation evidence",
                ));
            }
            prepared.push(self.data.published(file)?);
        }
        Transaction::restore(bytes, &snapshot, &prepared).await
    }
    pub async fn commit(&self, request: &Transaction) -> Result<Outcome> {
        Ok(self
            .commit_batch(std::slice::from_ref(request))
            .await?
            .remove(0))
    }
    pub async fn commit_batch(&self, requests: &[Transaction]) -> Result<Vec<Outcome>> {
        if requests.len() > 4096 {
            return Err(Error::unsupported(
                "service batch",
                "more than 4096 requests",
            ));
        }
        if requests.is_empty() {
            return Ok(vec![]);
        }
        // Even an in-process caller must have registered durable readiness.
        let mut restored = Vec::new();
        for request in requests {
            request.validate(self.filesystem)?;
            restored.push(self.restore_transaction(&request.to_bytes()?).await?);
        }
        let result = self.publish(&restored).await;
        match result {
            Err(e) if e.kind() == ErrorKind::Storage => {
                Ok(requests.iter().map(|_| Outcome::Retryable).collect())
            }
            other => other,
        }
    }
    async fn publish(&self, requests: &[Transaction]) -> Result<Vec<Outcome>> {
        let db = connect(&self.path, false).await?;
        db_call(&db, |c| {
            c.execute_batch("BEGIN IMMEDIATE").map_err(sql)?;
            Ok(())
        })
        .await?;
        let (_, _, old) = db_call(&db, |c| manifest(c)).await?;
        let sequence = old
            .sequence
            .checked_add(1)
            .filter(|n| *n <= i64::MAX as u64)
            .ok_or_else(|| Error::unsupported("service commit", "revision exhausted"))?;
        let revision = Revision::new(sequence);
        // This view observes preceding accepted members inside this SQL transaction.
        let candidate = self.snapshot(db.clone(), revision);
        let mut outcomes = Vec::new();
        let mut ordinal = 0;
        for request in requests {
            if let Some(receipt) = candidate.receipt(request.id()).await? {
                if receipt.request_digest != request.digest() {
                    return Err(Error::invalid(
                        "service commit",
                        "identity reused for different request",
                    ));
                }
                outcomes.push(Outcome::Committed(receipt));
                continue;
            }
            let Some(delta) = request.apply(&candidate).await? else {
                outcomes.push(Outcome::Conflict);
                continue;
            };
            let receipt = Receipt {
                commit_id: request.id(),
                request_digest: request.digest(),
                cursor: Cursor { revision, ordinal },
            };
            let encoded_receipt = receipt.encode()?;
            let change = ChangeRecord {
                receipt: receipt.clone(),
                changes: delta.changes,
            }
            .encode()?;
            let id = *request.id().as_bytes();
            let key = receipt.cursor.key();
            db_call(&db, move |c| {
                for (family, rows) in [
                    (0, delta.nodes),
                    (1, delta.entries),
                    (2, vec![(id.to_vec(), Some(encoded_receipt))]),
                    (3, vec![(key, Some(change))]),
                ] {
                    for (key, value) in rows {
                        // Multiple batch members may touch a record in this revision.
                        c.execute(
                            "DELETE FROM records WHERE family=?1 AND key=?2 AND start=?3",
                            params![family, key, sequence as i64],
                        )
                        .map_err(sql)?;
                        c.execute(
                            "UPDATE records SET end=?3 WHERE family=?1 AND key=?2 AND end IS NULL",
                            params![family, key, sequence as i64],
                        )
                        .map_err(sql)?;
                        if let Some(value) = value {
                            c.execute(
                                "INSERT INTO records VALUES(?1,?2,?3,NULL,?4)",
                                params![family, key, sequence as i64, value],
                            )
                            .map_err(sql)?;
                        }
                    }
                }
                Ok(())
            })
            .await?;
            outcomes.push(Outcome::Committed(receipt));
            ordinal += 1;
        }
        if ordinal == 0 {
            db_call(&db, |c| {
                c.execute_batch("ROLLBACK").map_err(sql)?;
                Ok(())
            })
            .await?;
            return Ok(outcomes);
        }
        db_call(&db, move |c| {
            c.execute(
                "INSERT INTO revisions VALUES(?1,?2)",
                params![revision.to_bytes().as_slice(), sequence as i64],
            )
            .map_err(sql)?;
            c.execute(
                "UPDATE authority SET latest=?1 WHERE singleton=1",
                params![revision.to_bytes().as_slice()],
            )
            .map_err(sql)?;
            Ok(())
        })
        .await?;
        // A failed durability acknowledgement is not proof of rollback.
        if db_call(&db, |c| {
            c.execute_batch("COMMIT").map_err(sql)?;
            Ok(())
        })
        .await
        .is_err()
        {
            return Ok(requests.iter().map(|r| Outcome::Unknown(r.id())).collect());
        }
        Ok(outcomes)
    }
}

#[derive(Debug)]
struct SqlRecords {
    db: Db,
    sequence: u64,
}
impl RecordReader for SqlRecords {
    fn get<'a>(&'a self, table: Table, key: &'a [u8]) -> BoxFuture<'a, Result<Option<Vec<u8>>>> {
        Box::pin(async move {
            let key = key.to_vec();
            let seq = self.sequence;
            db_call(&self.db,move |c|c.query_row("SELECT value FROM records WHERE family=?1 AND key=?2 AND start<=?3 AND (end IS NULL OR end>?3) ORDER BY start DESC LIMIT 1",
                params![table as u8,key,seq as i64],|r|r.get(0)).optional().map_err(sql)).await
        })
    }
    fn scan<'a>(
        &'a self,
        table: Table,
        lower: &'a [u8],
        upper: Option<&'a [u8]>,
        after: Option<&'a [u8]>,
        limit: usize,
    ) -> BoxFuture<'a, Result<Rows>> {
        Box::pin(async move {
            let (lower, upper, after) = (
                lower.to_vec(),
                upper.map(<[u8]>::to_vec),
                after.map(<[u8]>::to_vec),
            );
            let seq = self.sequence;
            db_call(&self.db,move |c| {
                let mut stmt=c.prepare("SELECT key,value FROM records WHERE family=?1 AND key>=?2 AND (?3 IS NULL OR key<?3) AND (?4 IS NULL OR key>?4) AND start<=?5 AND (end IS NULL OR end>?5) ORDER BY key LIMIT ?6").map_err(sql)?;
                let rows=stmt.query_map(params![table as u8,lower,upper,after,seq as i64,limit as i64],|r|Ok((r.get(0)?,r.get(1)?))).map_err(sql)?;
                rows.collect::<std::result::Result<Rows,_>>().map_err(sql)
            }).await
        })
    }
}
