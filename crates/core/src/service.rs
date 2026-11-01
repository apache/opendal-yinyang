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

use crate::authority::{RETAINED_REVISIONS_PAGE_MAX, RevisionPage};
use crate::compatibility::{
    SERVICE_BINDING_MAGIC, SQLITE_SCHEMA_PROFILE, SQLITE_SCHEMA_PROFILE_EXPIRY,
};
use crate::expiry::{
    ExpiryConflictKind, ExpiryMarker, ExpiryOutcome, ExpiryRefusal, ExpiryRequest, HistoryEntry,
    HistoryPage, RevisionState, operation_key, revision_key,
};
use crate::namespace::{Node, NodeKind, decode, encode};
use crate::snapshot::{
    ChangeRecord, Cursor, FileVersion, Outcome, Receipt, RecordReader, Rows, Snapshot, Table,
};
use crate::{
    ContentDescriptor, ContentEdit, DataStore, Error, ErrorKind, NodeId, PreparedContent, Result,
    Revision, Transaction,
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

type Db = Arc<Mutex<Connection>>;
const BASE_TABLES: [&str; 4] = ["authority", "prepared", "records", "revisions"];
const EXPIRY_TABLES: [&str; 5] = ["authority", "expiry", "prepared", "records", "revisions"];
const AUTHORITY_COLUMNS: [(&str, &str); 5] = [
    ("singleton", "INTEGER"),
    ("profile", "TEXT"),
    ("filesystem", "BLOB"),
    ("root", "BLOB"),
    ("latest", "BLOB"),
];
const PREPARED_COLUMNS: [(&str, &str); 1] = [("descriptor", "BLOB")];
const RECORDS_COLUMNS: [(&str, &str); 5] = [
    ("family", "INTEGER"),
    ("key", "BLOB"),
    ("start", "INTEGER"),
    ("end", "INTEGER"),
    ("value", "BLOB"),
];
const REVISIONS_COLUMNS: [(&str, &str); 2] = [("token", "BLOB"), ("sequence", "INTEGER")];
const EXPIRY_COLUMNS: [(&str, &str); 2] = [("key", "BLOB"), ("record", "BLOB")];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SchemaProfile {
    Base,
    Expiry,
}

impl SchemaProfile {
    fn parse(profile: &str) -> Result<Self> {
        match profile {
            SQLITE_SCHEMA_PROFILE => Ok(Self::Base),
            SQLITE_SCHEMA_PROFILE_EXPIRY => Ok(Self::Expiry),
            _ => Err(Error::unsupported(
                "open metadata authority",
                "unknown schema profile",
            )),
        }
    }
}

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
fn corrupt_schema(message: impl Into<String>) -> Error {
    Error::new(ErrorKind::Corrupt, "metadata database", message)
}
fn check_columns(conn: &Connection, table: &str, expected: &[(&str, &str)]) -> Result<()> {
    let mut statement = conn
        .prepare(&format!("PRAGMA table_info({table})"))
        .map_err(|error| {
            corrupt_schema(format!("cannot inspect table {table} columns: {error}"))
        })?;
    let columns = statement
        .query_map([], |row| {
            Ok((row.get::<_, String>(1)?, row.get::<_, String>(2)?))
        })
        .and_then(|rows| rows.collect::<std::result::Result<Vec<_>, _>>())
        .map_err(|error| {
            corrupt_schema(format!("cannot inspect table {table} columns: {error}"))
        })?;
    if columns.len() != expected.len()
        || columns
            .iter()
            .zip(expected)
            .any(|((name, kind), (expected_name, expected_kind))| {
                name.as_str() != *expected_name || !kind.eq_ignore_ascii_case(expected_kind)
            })
    {
        return Err(corrupt_schema(format!(
            "schema profile has an incompatible {table} table"
        )));
    }
    Ok(())
}
fn inspect_schema(conn: &Connection) -> Result<Option<SchemaProfile>> {
    let tables = conn
        .prepare(
            "SELECT name FROM sqlite_schema
             WHERE type='table' AND name NOT LIKE 'sqlite_%'
             ORDER BY name",
        )
        .and_then(|mut statement| {
            statement
                .query_map([], |row| row.get::<_, String>(0))?
                .collect::<std::result::Result<Vec<_>, _>>()
        })
        .map_err(|error| corrupt_schema(format!("cannot inspect SQLite schema: {error}")))?;
    if tables.is_empty() {
        return Ok(None);
    }
    if !tables.iter().any(|table| table == "authority") {
        return Err(Error::unsupported(
            "open metadata authority",
            "database is not a YinYang authority",
        ));
    }
    let (profile, _, _, _) = manifest(conn)?;
    let required = match profile {
        SchemaProfile::Base => BASE_TABLES.as_slice(),
        SchemaProfile::Expiry => EXPIRY_TABLES.as_slice(),
    };
    for required in required {
        if !tables.iter().any(|table| table.as_str() == required) {
            return Err(corrupt_schema(format!(
                "schema profile is missing table {required}"
            )));
        }
    }
    check_columns(conn, "authority", &AUTHORITY_COLUMNS)?;
    check_columns(conn, "prepared", &PREPARED_COLUMNS)?;
    check_columns(conn, "records", &RECORDS_COLUMNS)?;
    check_columns(conn, "revisions", &REVISIONS_COLUMNS)?;
    if profile == SchemaProfile::Expiry {
        check_columns(conn, "expiry", &EXPIRY_COLUMNS)?;
    } else if tables.iter().any(|table| table == "expiry") {
        return Err(corrupt_schema(
            "base schema profile unexpectedly contains an expiry table",
        ));
    }
    let (_, _, _, revision) = manifest(conn)?;
    let sequence: Option<i64> = conn
        .query_row(
            "SELECT sequence FROM revisions WHERE token=?1",
            params![revision.to_bytes().as_slice()],
            |row| row.get(0),
        )
        .optional()
        .map_err(|error| corrupt_schema(format!("cannot validate retained revision: {error}")))?;
    if sequence != Some(revision.sequence as i64) {
        return Err(corrupt_schema("latest manifest is not a retained revision"));
    }
    Ok(Some(profile))
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
        let existing = inspect_schema(&conn)?;
        if existing.is_none() && !create {
            return Err(Error::new(
                ErrorKind::NotFound,
                "open metadata authority",
                "database is missing or uninitialized",
            ));
        }
        conn.execute_batch(
            "PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA foreign_keys=ON;",
        )
        .map_err(sql)?;
        Ok(Arc::new(Mutex::new(conn)))
    })
    .await
    .map_err(|e| Error::new(ErrorKind::Storage, "open metadata worker", e.to_string()))?
}

async fn connect_read_only(path: &Path) -> Result<Db> {
    let path = path.to_owned();
    tokio::task::spawn_blocking(move || {
        let conn = Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
            .map_err(sql)?;
        conn.busy_timeout(Duration::from_millis(250)).map_err(sql)?;
        conn.execute_batch("PRAGMA query_only=ON;").map_err(sql)?;
        if inspect_schema(&conn)?.is_none() {
            return Err(Error::new(
                ErrorKind::NotFound,
                "open metadata authority",
                "database is missing or uninitialized",
            ));
        }
        Ok(Arc::new(Mutex::new(conn)))
    })
    .await
    .map_err(|e| Error::new(ErrorKind::Storage, "open metadata worker", e.to_string()))?
}
fn manifest(conn: &Connection) -> Result<(SchemaProfile, NodeId, NodeId, Revision)> {
    let (profile, fs, root, revision): (String, Vec<u8>, Vec<u8>, Vec<u8>) = conn
        .query_row(
            "SELECT profile, filesystem, root, latest FROM authority WHERE singleton=1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        )
        .map_err(|error| corrupt_schema(format!("cannot read authority manifest: {error}")))?;
    let profile = SchemaProfile::parse(&profile)?;
    let revision = Revision::from_bytes(fixed(revision)?);
    if revision.sequence > i64::MAX as u64 {
        return Err(corrupt_schema("latest revision exceeds the SQLite domain"));
    }
    Ok((
        profile,
        NodeId::from_bytes(fixed(fs)?),
        NodeId::from_bytes(fixed(root)?),
        revision,
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
fn parse_service_binding(bytes: &[u8]) -> Result<(NodeId, NodeId)> {
    if bytes.len() < SERVICE_BINDING_MAGIC.len() {
        return Err(corrupt_schema("service binding is truncated"));
    }
    if bytes[..SERVICE_BINDING_MAGIC.len()] != SERVICE_BINDING_MAGIC {
        return Err(Error::unsupported(
            "open metadata authority",
            "unknown service binding profile",
        ));
    }
    let (magic, filesystem, root): ([u8; 8], [u8; 16], [u8; 16]) = decode(bytes)?;
    debug_assert_eq!(magic, SERVICE_BINDING_MAGIC);
    Ok((NodeId::from_bytes(filesystem), NodeId::from_bytes(root)))
}
fn revision_exists(conn: &Connection, revision: Revision) -> Result<bool> {
    let sequence = i64::try_from(revision.sequence).map_err(|_| {
        Error::new(
            ErrorKind::NotFound,
            "observe service revision",
            "revision not retained",
        )
    })?;
    conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM revisions WHERE token=?1 AND sequence=?2)",
        params![revision.to_bytes().as_slice(), sequence],
        |row| row.get(0),
    )
    .map_err(sql)
}
fn load_expiry(conn: &Connection, key: &[u8]) -> Result<Option<ExpiryMarker>> {
    conn.query_row(
        "SELECT record FROM expiry WHERE key=?1",
        params![key],
        |row| row.get::<_, Vec<u8>>(0),
    )
    .optional()
    .map_err(sql)?
    .map(|bytes| ExpiryMarker::decode(&bytes))
    .transpose()
}
fn save_expiry(conn: &Connection, key: &[u8], marker: ExpiryMarker) -> Result<()> {
    conn.execute(
        "INSERT OR REPLACE INTO expiry(key,record) VALUES(?1,?2)",
        params![key, marker.encode()?],
    )
    .map_err(sql)?;
    Ok(())
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
                tx.execute("INSERT INTO authority VALUES(1,?1,?2,?3,?4)",params![SQLITE_SCHEMA_PROFILE,fs.as_bytes(),root.as_bytes(),revision.to_bytes().as_slice()]).map_err(sql)?;
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
        let (_, filesystem, root, _) = db_call(&db, |c| manifest(c)).await?;
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
            SERVICE_BINDING_MAGIC,
            *self.filesystem.as_bytes(),
            *self.root.as_bytes(),
        ))
    }
    async fn verify_binding(&self, operator: &Operator) -> Result<()> {
        let marker = operator
            .read(".yinyang/head")
            .await
            .map_err(|e| Error::from_storage("read authority binding", e))?;
        let (filesystem, root) = parse_service_binding(&marker.to_vec())?;
        if filesystem != self.filesystem || root != self.root {
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
        let (_, fs, root, revision) = db_call(&db, |c| manifest(c)).await?;
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
        let expected_volume = self.filesystem;
        let expected_root = self.root;
        db_call(&db, move |c| {
            let (profile, volume, root, _) = manifest(c)?;
            if volume != expected_volume || root != expected_root {
                return Err(Error::invalid("observe service", "authority changed"));
            }
            if !revision_exists(c, revision)? {
                return Err(Error::new(
                    ErrorKind::NotFound,
                    "observe service revision",
                    "revision not retained",
                ));
            }
            if profile == SchemaProfile::Expiry
                && load_expiry(c, &revision_key(revision))?.is_some()
            {
                return Err(Error::expired(
                    "observe service revision",
                    revision,
                    "revision is logically expired",
                ));
            }
            Ok(())
        })
        .await?;
        Ok(self.snapshot(db, revision))
    }
    /// Read a bounded page from the durable retained revision index without
    /// changing journal mode, taking the writer transaction, or recovering.
    pub async fn retained_revisions_page(
        &self,
        after: Option<Revision>,
        limit: u32,
    ) -> Result<RevisionPage> {
        if !(1..=RETAINED_REVISIONS_PAGE_MAX).contains(&limit) {
            return Err(Error::unsupported(
                "list retained revisions",
                format!("page size must be 1..={RETAINED_REVISIONS_PAGE_MAX}"),
            ));
        }
        let db = connect_read_only(&self.path).await?;
        let after = after.map(Revision::to_bytes);
        let mut revisions = db_call(&db, move |conn| {
            let limit = i64::from(limit) + 1;
            let mut rows = Vec::new();
            if let Some(after) = after.as_ref() {
                let mut statement = conn
                    .prepare(
                        "SELECT token,sequence FROM revisions
                         WHERE token>?1 ORDER BY token LIMIT ?2",
                    )
                    .map_err(sql)?;
                let values = statement
                    .query_map(params![after.as_slice(), limit], |row| {
                        Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, i64>(1)?))
                    })
                    .map_err(sql)?;
                for value in values {
                    rows.push(value.map_err(sql)?);
                }
            } else {
                let mut statement = conn
                    .prepare("SELECT token,sequence FROM revisions ORDER BY token LIMIT ?1")
                    .map_err(sql)?;
                let values = statement
                    .query_map(params![limit], |row| {
                        Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, i64>(1)?))
                    })
                    .map_err(sql)?;
                for value in values {
                    rows.push(value.map_err(sql)?);
                }
            }
            rows.into_iter()
                .map(|(token, sequence)| {
                    let revision = Revision::from_bytes(fixed(token)?);
                    if sequence < 0 || revision.sequence != sequence as u64 {
                        return Err(corrupt_schema(
                            "retained revision token disagrees with its sequence",
                        ));
                    }
                    Ok(revision)
                })
                .collect::<Result<Vec<_>>>()
        })
        .await?;
        let more = revisions.len() > limit as usize;
        revisions.truncate(limit as usize);
        if revisions.windows(2).any(|window| window[0] >= window[1]) {
            return Err(corrupt_schema(
                "retained revisions are not strictly ordered",
            ));
        }
        let next = more.then(|| *revisions.last().expect("a nonempty page has a cursor"));
        Ok(RevisionPage { revisions, next })
    }
    pub async fn history_page(&self, after: Option<Revision>, limit: u32) -> Result<HistoryPage> {
        if !(1..=RETAINED_REVISIONS_PAGE_MAX).contains(&limit) {
            return Err(Error::unsupported(
                "read history",
                format!("page size must be 1..={RETAINED_REVISIONS_PAGE_MAX}"),
            ));
        }
        let db = connect_read_only(&self.path).await?;
        let after = after.map(Revision::to_bytes);
        let expected_volume = self.filesystem;
        let expected_root = self.root;
        db_call(&db, move |conn| {
            let (profile, volume, root, latest) = manifest(conn)?;
            if volume != expected_volume || root != expected_root {
                return Err(Error::invalid("read history", "authority changed"));
            }
            let sql_limit = i64::from(limit) + 1;
            let mut rows = Vec::new();
            if let Some(after) = after.as_ref() {
                let mut statement = conn
                    .prepare(
                        "SELECT token,sequence FROM revisions
                         WHERE token>?1 ORDER BY token LIMIT ?2",
                    )
                    .map_err(sql)?;
                let values = statement
                    .query_map(params![after.as_slice(), sql_limit], |row| {
                        Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, i64>(1)?))
                    })
                    .map_err(sql)?;
                for value in values {
                    rows.push(value.map_err(sql)?);
                }
            } else {
                let mut statement = conn
                    .prepare("SELECT token,sequence FROM revisions ORDER BY token LIMIT ?1")
                    .map_err(sql)?;
                let values = statement
                    .query_map(params![sql_limit], |row| {
                        Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, i64>(1)?))
                    })
                    .map_err(sql)?;
                for value in values {
                    rows.push(value.map_err(sql)?);
                }
            }
            let more = rows.len() > limit as usize;
            rows.truncate(limit as usize);
            let mut entries = Vec::with_capacity(rows.len());
            for (token, sequence) in rows {
                let revision = Revision::from_bytes(fixed(token)?);
                if sequence < 0 || revision.sequence != sequence as u64 {
                    return Err(corrupt_schema(
                        "retained revision token disagrees with its sequence",
                    ));
                }
                let state = if profile == SchemaProfile::Expiry {
                    match load_expiry(conn, &revision_key(revision))? {
                        Some(marker) if marker.after > latest => {
                            return Err(corrupt_schema(
                                "expiry marker was published after the latest revision",
                            ));
                        }
                        Some(marker) => RevisionState::Expired(marker),
                        None => RevisionState::Readable,
                    }
                } else {
                    RevisionState::Readable
                };
                entries.push(HistoryEntry { revision, state });
            }
            let next = more.then(|| entries.last().expect("nonempty history page").revision);
            let page = HistoryPage { entries, next };
            page.validate()?;
            Ok(page)
        })
        .await
    }
    pub async fn expire_revisions(&self, request: &ExpiryRequest) -> Result<ExpiryOutcome> {
        request.validate_volume(self.filesystem)?;
        let request_digest = request.digest()?;
        let request_for_write = request.clone();
        let expected_volume = self.filesystem;
        let expected_root = self.root;
        let db = connect(&self.path, false).await?;
        let result = db_call(&db, move |conn| {
            let tx = conn
                .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
                .map_err(sql)?;
            let (profile, volume, root, latest) = manifest(&tx)?;
            if volume != expected_volume || root != expected_root {
                return Err(Error::invalid("expire revisions", "authority changed"));
            }
            if profile == SchemaProfile::Expiry {
                if let Some(marker) = load_expiry(&tx, &operation_key(request_for_write.id()))? {
                    if marker.request_digest != request_digest {
                        return Err(Error::invalid(
                            "expire revisions",
                            "operation identity was reused with a different request",
                        ));
                    }
                    request_for_write.validate_marker(&marker)?;
                    for candidate in request_for_write.candidates() {
                        let bytes = load_expiry(&tx, &revision_key(*candidate))?.ok_or_else(|| {
                            corrupt_schema(
                                "committed expiry operation is missing a revision marker",
                            )
                        })?;
                        if bytes != marker {
                            return Err(corrupt_schema(
                                "expiry operation and revision markers disagree",
                            ));
                        }
                    }
                    return Ok(ExpiryOutcome::Committed(
                        request_for_write.receipt(marker),
                    ));
                }
            }
            if latest != request_for_write.expected_latest() {
                return Ok(ExpiryOutcome::Conflict);
            }

            let mut refusal = ExpiryRefusal::new(latest);
            for candidate in request_for_write.candidates() {
                if *candidate == latest {
                    refusal.push(*candidate, ExpiryConflictKind::Current);
                } else if !revision_exists(&tx, *candidate)? {
                    refusal.push(*candidate, ExpiryConflictKind::Missing);
                } else if profile == SchemaProfile::Expiry
                    && load_expiry(&tx, &revision_key(*candidate))?.is_some()
                {
                    refusal.push(*candidate, ExpiryConflictKind::AlreadyExpired);
                }
            }
            for protected in request_for_write.protected() {
                if *protected == latest {
                    continue;
                }
                if !revision_exists(&tx, *protected)? {
                    refusal.push(*protected, ExpiryConflictKind::ProtectedMissing);
                } else if profile == SchemaProfile::Expiry
                    && load_expiry(&tx, &revision_key(*protected))?.is_some()
                {
                    refusal.push(*protected, ExpiryConflictKind::ProtectedExpired);
                }
            }
            for candidate in request_for_write.candidates() {
                if request_for_write.protected().binary_search(candidate).is_ok() {
                    refusal.push(*candidate, ExpiryConflictKind::Protected);
                }
            }
            if !refusal.is_empty() {
                return Ok(ExpiryOutcome::Refused(refusal));
            }

            let sequence = latest
                .sequence
                .checked_add(1)
                .filter(|sequence| *sequence <= i64::MAX as u64)
                .ok_or_else(|| {
                    Error::unsupported("expire revisions", "revision space exhausted")
                })?;
            let after = Revision::new(sequence);
            let marker = ExpiryMarker {
                operation_id: request_for_write.id(),
                request_digest,
                evidence_digest: request_for_write.evidence_digest(),
                before: latest,
                after,
            };
            if profile == SchemaProfile::Base {
                tx.execute_batch(&format!(
                    "CREATE TABLE expiry(key BLOB PRIMARY KEY, record BLOB NOT NULL);
                     UPDATE authority SET profile='{SQLITE_SCHEMA_PROFILE_EXPIRY}' WHERE singleton=1;"
                ))
                .map_err(sql)?;
            }
            save_expiry(&tx, &operation_key(request_for_write.id()), marker)?;
            for candidate in request_for_write.candidates() {
                save_expiry(&tx, &revision_key(*candidate), marker)?;
            }
            tx.execute(
                "INSERT INTO revisions VALUES(?1,?2)",
                params![after.to_bytes().as_slice(), sequence as i64],
            )
            .map_err(sql)?;
            tx.execute(
                "UPDATE authority SET latest=?1 WHERE singleton=1",
                params![after.to_bytes().as_slice()],
            )
            .map_err(sql)?;
            tx.commit().map_err(sql)?;
            Ok(ExpiryOutcome::Committed(
                request_for_write.receipt(marker),
            ))
        })
        .await;
        match result {
            Ok(outcome) => Ok(outcome),
            Err(error) if error.kind() == ErrorKind::Storage => {
                self.resolve_expiry_after_error(request, request_digest)
                    .await
            }
            Err(error) => Err(error),
        }
    }
    async fn resolve_expiry_after_error(
        &self,
        request: &ExpiryRequest,
        request_digest: [u8; 32],
    ) -> Result<ExpiryOutcome> {
        let request_id = request.id();
        let db = match connect_read_only(&self.path).await {
            Ok(db) => db,
            Err(_) => return Ok(ExpiryOutcome::Unknown(request_id)),
        };
        let request = request.clone();
        let result = db_call(&db, move |conn| {
            let (profile, _, _, _) = manifest(conn)?;
            if profile != SchemaProfile::Expiry {
                return Ok(None);
            }
            let Some(marker) = load_expiry(conn, &operation_key(request.id()))? else {
                return Ok(None);
            };
            if marker.request_digest != request_digest {
                return Err(Error::invalid(
                    "resolve expiry",
                    "operation identity was reused with a different request",
                ));
            }
            request.validate_marker(&marker)?;
            for candidate in request.candidates() {
                let marker_for_revision = load_expiry(conn, &revision_key(*candidate))?
                    .ok_or_else(|| {
                        corrupt_schema("committed expiry operation is missing a revision marker")
                    })?;
                if marker_for_revision != marker {
                    return Err(corrupt_schema(
                        "expiry operation and revision markers disagree",
                    ));
                }
            }
            Ok(Some(ExpiryOutcome::Committed(request.receipt(marker))))
        })
        .await;
        match result {
            Ok(Some(outcome)) => Ok(outcome),
            Ok(None) => Ok(ExpiryOutcome::Unknown(request_id)),
            Err(error) => Err(error),
        }
    }
    /// In-process preparation already carries this service's trusted evidence.
    pub async fn prepare(&self, source: &mut (impl AsyncRead + Unpin)) -> Result<PreparedContent> {
        let content = self.data.prepare(source).await?;
        self.remember(&content).await?;
        Ok(content)
    }
    /// Prepare a same-length update from a published file version selected by
    /// the caller. Reauthentication prevents a caller-supplied base descriptor
    /// from becoming preparation authority.
    pub async fn prepare_overwrite(
        &self,
        base: &FileVersion,
        edits: &[ContentEdit<'_>],
    ) -> Result<PreparedContent> {
        let snapshot = self.observe_revision(base.revision()).await?;
        let selected = snapshot.open_file(base.node().id()).await?;
        if selected.descriptor() != base.descriptor() {
            return Err(Error::invalid(
                "prepare metadata overwrite",
                "selected base is not in the referenced revision",
            ));
        }
        self.prepare_overwrite_from_version(&selected, edits).await
    }
    pub(crate) async fn prepare_overwrite_version(
        &self,
        revision: Revision,
        node: NodeId,
        edits: &[ContentEdit<'_>],
    ) -> Result<PreparedContent> {
        let snapshot = self.observe_revision(revision).await?;
        let base = snapshot.open_file(node).await?;
        self.prepare_overwrite_from_version(&base, edits).await
    }
    async fn prepare_overwrite_from_version(
        &self,
        base: &FileVersion,
        edits: &[ContentEdit<'_>],
    ) -> Result<PreparedContent> {
        let content = self
            .data
            .overwrite_prepared_ranges(&base.prepared_content()?, edits)
            .await?;
        self.remember(&content).await?;
        Ok(content)
    }
    /// Prepare a length-changing update from a published file version selected
    /// by the caller. Reauthentication prevents a caller-supplied base
    /// descriptor from becoming preparation authority.
    pub async fn prepare_update(
        &self,
        base: &FileVersion,
        preserved_prefix: u64,
        final_length: u64,
        edits: &[ContentEdit<'_>],
    ) -> Result<PreparedContent> {
        let snapshot = self.observe_revision(base.revision()).await?;
        let selected = snapshot.open_file(base.node().id()).await?;
        if selected.descriptor() != base.descriptor() {
            return Err(Error::invalid(
                "prepare metadata update",
                "selected base is not in the referenced revision",
            ));
        }
        self.prepare_update_from_version(&selected, preserved_prefix, final_length, edits)
            .await
    }
    pub(crate) async fn prepare_update_version(
        &self,
        revision: Revision,
        node: NodeId,
        preserved_prefix: u64,
        final_length: u64,
        edits: &[ContentEdit<'_>],
    ) -> Result<PreparedContent> {
        let snapshot = self.observe_revision(revision).await?;
        let base = snapshot.open_file(node).await?;
        self.prepare_update_from_version(&base, preserved_prefix, final_length, edits)
            .await
    }
    async fn prepare_update_from_version(
        &self,
        base: &FileVersion,
        preserved_prefix: u64,
        final_length: u64,
        edits: &[ContentEdit<'_>],
    ) -> Result<PreparedContent> {
        let content = self
            .data
            .update_prepared(
                &base.prepared_content()?,
                preserved_prefix,
                final_length,
                edits,
            )
            .await?;
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
        let (_, _, _, old) = db_call(&db, |c| manifest(c)).await?;
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

#[cfg(test)]
mod admission_tests {
    use super::*;
    use crate::support::TestBackend;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    #[tokio::test]
    async fn unknown_rpc_version_is_rejected_before_command_dispatch() {
        let temp = tempfile::tempdir().unwrap();
        let backend = TestBackend::default();
        let service = MetadataService::create(temp.path().join("metadata.db"), backend.operator())
            .await
            .unwrap();
        let before = service.observe_latest().await.unwrap().revision();
        let token = "metadata-rpc-compatibility-test-token";
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(serve(listener, service.clone(), token.into()));

        let mut stream = TcpStream::connect(addr).await.unwrap();
        let request = borsh::to_vec(&(2_u8, token.to_owned(), 3_u8, vec![0_u8])).unwrap();
        stream.write_u32(request.len() as u32).await.unwrap();
        stream.write_all(&request).await.unwrap();
        let length = stream.read_u32().await.unwrap() as usize;
        let mut response = vec![0; length];
        stream.read_exact(&mut response).await.unwrap();
        let (variant, kind, message): (u8, u8, String) = borsh::from_slice(&response).unwrap();
        assert_eq!(variant, 5, "expected an RPC error response");
        assert_eq!(kind, 4, "expected Unsupported");
        assert!(message.contains("unknown protocol version"), "{message}");
        assert_eq!(service.observe_latest().await.unwrap().revision(), before);
        assert_eq!(backend.state.lock().unwrap().data_reads, 0);
        server.abort();
    }

    #[tokio::test]
    async fn unknown_delta_revision_is_rejected_before_payload_reads() {
        let temp = tempfile::tempdir().unwrap();
        let backend = TestBackend::default();
        let service = MetadataService::create(temp.path().join("metadata.db"), backend.operator())
            .await
            .unwrap();
        let edit = ContentEdit::new(0..1, b"x").unwrap();
        let error = service
            .prepare_overwrite_version(Revision::from_bytes([0; 24]), NodeId::generate(), &[edit])
            .await
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::NotFound);
        assert_eq!(backend.state.lock().unwrap().data_reads, 0);
    }

    #[tokio::test]
    async fn unknown_length_update_revision_is_rejected_before_payload_reads() {
        let temp = tempfile::tempdir().unwrap();
        let backend = TestBackend::default();
        let service = MetadataService::create(temp.path().join("metadata.db"), backend.operator())
            .await
            .unwrap();
        let error = service
            .prepare_update_version(Revision::from_bytes([0; 24]), NodeId::generate(), 0, 1, &[])
            .await
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::NotFound);
        assert_eq!(backend.state.lock().unwrap().data_reads, 0);
    }
}
