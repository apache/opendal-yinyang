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

//! Durable, source-free recovery of prepared directory publications.
use super::{
    DirectoryManifest, install_directory, prepare_directory_request, validate_install_entries,
};
use crate::intent;
use crate::local::store;
use crate::runtime::{Error, ErrorKind, Failure, Result};
use borsh::{BorshDeserialize, BorshSerialize};
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::Mutex;
use yinyang_core::{
    Authority, CommitId, CommitOutcome, NodeId, Receipt, Revision, Snapshot, Transaction,
};

const SCHEMA: &[store::TableColumns<'static>] = &[
    ("binding", &["singleton", "profile", "filesystem", "root"]),
    (intent::TABLE, intent::COLUMNS),
];
const PROFILE: store::Profile = store::Profile {
    name: "yinyang-directory-3",
    table: "binding",
    schema: SCHEMA,
};

#[derive(Clone, Copy)]
enum StateFilter {
    Active,
    Any,
    Local,
    Unknown,
}

/// Stable identity and baseline of one durable directory publication request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DirectoryIntent {
    pub commit_id: CommitId,
    pub baseline: Revision,
    pub request_digest: [u8; 32],
    /// Diagnostic source path captured by preparation. Retry never reads it.
    pub source: PathBuf,
}

/// Durable outcome of one directory publication request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DirectoryState {
    /// Persisted before any dispatch. The original prepared content is ready.
    Prepared,
    /// A dispatch may have succeeded. Receipt absence is not proof of failure.
    Unknown,
    Committed(Receipt),
    /// The original namespace predicates changed; this request is terminal.
    Conflict,
    /// The authority established that this request did not publish.
    Retryable,
    /// Local validation or retained readiness is definitively unusable.
    Failed,
}

impl DirectoryState {
    pub const fn receipt(&self) -> Option<&Receipt> {
        match self {
            Self::Committed(receipt) => Some(receipt),
            _ => None,
        }
    }

    /// The source-free retry operation can make progress or return a receipt.
    pub const fn can_retry(&self) -> bool {
        matches!(self, Self::Prepared | Self::Unknown | Self::Retryable)
    }

    /// The accepted preparation still has a usable frozen manifest.
    pub const fn can_export(&self) -> bool {
        !matches!(self, Self::Failed)
    }
}

/// Reopen-safe status for one directory publication.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DirectoryStatus {
    pub intent: DirectoryIntent,
    pub state: DirectoryState,
    pub failure: Option<Failure>,
}

impl DirectoryStatus {
    pub const fn receipt(&self) -> Option<&Receipt> {
        self.state.receipt()
    }

    pub const fn can_retry(&self) -> bool {
        self.state.can_retry()
    }

    pub const fn can_export(&self) -> bool {
        self.state.can_export()
    }
}

fn state_code(state: &DirectoryState) -> u8 {
    match state {
        DirectoryState::Prepared => intent::State::Prepared.code(),
        DirectoryState::Unknown => intent::State::Unknown.code(),
        DirectoryState::Committed(_) => intent::State::Committed.code(),
        DirectoryState::Conflict => intent::State::Conflict.code(),
        DirectoryState::Retryable => intent::State::Retryable.code(),
        DirectoryState::Failed => intent::State::Failed.code(),
    }
}

fn decode_state(code: u8) -> Result<intent::State> {
    intent::State::from_code(code)
        .ok_or_else(|| Error::local_kind(ErrorKind::Corrupt, "invalid directory intent state"))
}

/// Read-only view of a directory journal without an authority or credentials.
#[derive(Clone, Debug)]
pub struct DirectoryInspection {
    pub filesystem: NodeId,
    pub root: NodeId,
    pub intents: Vec<DirectoryStatus>,
}

impl DirectoryInspection {
    pub fn status(&self, commit_id: CommitId) -> Option<&DirectoryStatus> {
        self.intents
            .iter()
            .find(|status| status.intent.commit_id == commit_id)
    }
}

struct Inner {
    authority: Arc<dyn Authority>,
    journal_directory: PathBuf,
    database: Arc<Mutex<Connection>>,
    gate: Mutex<()>,
}

/// One local journal for exact, crash-safe directory publication replay.
#[derive(Clone)]
pub struct DirectoryJournal {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for DirectoryJournal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DirectoryJournal")
            .field("filesystem", &self.filesystem())
            .field("root", &self.root())
            .finish_non_exhaustive()
    }
}

impl DirectoryJournal {
    /// Open or create a journal bound to one shared publication authority.
    ///
    /// Keep this directory outside every source tree published through it.
    /// [`Self::prepare`] rejects a source that contains the journal directory.
    pub async fn open(authority: Arc<dyn Authority>, directory: impl AsRef<Path>) -> Result<Self> {
        let directory = directory.as_ref().to_owned();
        tokio::fs::create_dir_all(&directory).await.map_err(local)?;
        let directory = tokio::fs::canonicalize(&directory).await.map_err(local)?;
        let path = directory.join("directory.db");
        let filesystem = *authority.filesystem().as_bytes();
        let root = *authority.root().as_bytes();
        let database = tokio::task::spawn_blocking(move || -> Result<_> {
            let exists = path.try_exists().map_err(local)?;
            let mut connection = store::open_read_write(&path, !exists).map_err(store_error)?;
            let empty = store::is_empty(&connection).map_err(store_error)?;
            let tx = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql)?;
            if empty {
                store::initialize(
                    &tx,
                    &PROFILE,
                    "CREATE TABLE binding(
                         singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                         profile TEXT NOT NULL,
                         filesystem BLOB NOT NULL,
                         root BLOB NOT NULL
                     );",
                    &[
                        store::Binding::blob("filesystem", filesystem.to_vec()),
                        store::Binding::blob("root", root.to_vec()),
                    ],
                )
                .map_err(store_error)?;
                tx.execute_batch(intent::CREATE_TABLE).map_err(sql)?;
            }
            validate_binding(&tx, filesystem, root)?;
            tx.commit().map_err(sql)?;
            store::configure(&connection, store::Durability::Journal).map_err(store_error)?;
            Ok(connection)
        })
        .await
        .map_err(local)??;
        Ok(Self {
            inner: Arc::new(Inner {
                authority,
                journal_directory: directory,
                database: Arc::new(Mutex::new(database)),
                gate: Mutex::new(()),
            }),
        })
    }

    pub fn filesystem(&self) -> NodeId {
        self.inner.authority.filesystem()
    }

    pub fn root(&self) -> NodeId {
        self.inner.authority.root()
    }

    /// Build and durably accept one new publication request.
    ///
    /// An identity already present in the journal is rejected before local IO.
    /// Use [`Self::publish`] for source-free exact replay instead. The canonical
    /// journal directory must be outside the canonical input tree.
    pub async fn prepare(
        &self,
        observed: &Snapshot,
        source: &Path,
        commit_id: CommitId,
    ) -> Result<DirectoryStatus> {
        let _gate = self.inner.gate.lock().await;
        if load_async(self, commit_id).await?.is_some() {
            return Err(existing_identity(commit_id));
        }
        let canonical_source = tokio::fs::canonicalize(source).await.map_err(local)?;
        if self.inner.journal_directory == canonical_source
            || self.inner.journal_directory.starts_with(&canonical_source)
        {
            return Err(Error::local_kind(
                ErrorKind::Invalid,
                format!(
                    "directory journal {} is inside source {}; keep journal state outside the published input tree",
                    self.inner.journal_directory.display(),
                    canonical_source.display()
                ),
            ));
        }
        let prepared =
            prepare_directory_request(self.inner.authority.as_ref(), observed, source, commit_id)
                .await?;
        let request = prepared.transaction.to_bytes()?;
        let (_, stored_id, digest) = Transaction::inspect_identity(&request)?;
        if stored_id != commit_id || digest != prepared.transaction.digest() {
            return Err(Error::local_kind(
                ErrorKind::Corrupt,
                "prepared directory transaction identity disagrees",
            ));
        }
        let entries = prepared.manifest.install_entries()?;
        validate_install_entries(self.inner.authority.as_ref(), &entries)?;
        let manifest = borsh::to_vec(&prepared.manifest).map_err(local)?;
        let source = source.to_string_lossy().into_owned();
        let metadata = borsh::to_vec(&DirectoryMetadata { source, manifest }).map_err(local)?;
        let state = state_code(&DirectoryState::Prepared);
        self.call(move |connection| {
            let tx = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql)?;
            if load(&tx, commit_id)?.is_some() {
                return Err(existing_identity(commit_id));
            }
            tx.execute(
                "INSERT INTO intents(
                    id,request_id,request_digest,request,commit_id,state,
                    receipt_revision,receipt_ordinal,failure,metadata
                 ) VALUES(?1,?2,?3,?4,?5,?6,NULL,NULL,NULL,?7)",
                params![
                    commit_id.as_bytes().as_slice(),
                    commit_id.as_bytes().as_slice(),
                    digest.as_slice(),
                    request,
                    commit_id.as_bytes().as_slice(),
                    state,
                    metadata
                ],
            )
            .map_err(sql)?;
            tx.commit().map_err(sql)
        })
        .await?;
        load_async(self, commit_id)
            .await?
            .map(|row| row.status(commit_id))
            .transpose()?
            .ok_or_else(|| Error::local_kind(ErrorKind::Corrupt, "prepared intent disappeared"))
    }

    /// Retry the exact frozen request. This method never scans the source.
    pub async fn publish(&self, commit_id: CommitId) -> Result<Receipt> {
        let _gate = self.inner.gate.lock().await;
        let row = load_async(self, commit_id)
            .await?
            .ok_or_else(|| Error::local_kind(ErrorKind::NotFound, "unknown directory intent"))?;
        let status = row.status(commit_id)?;
        match &status.state {
            DirectoryState::Committed(receipt) => return Ok(receipt.clone()),
            DirectoryState::Conflict | DirectoryState::Failed => {
                return Err(Error::Retained(status.failure.ok_or_else(|| {
                    Error::local_kind(ErrorKind::Corrupt, "terminal intent has no failure")
                })?));
            }
            DirectoryState::Prepared | DirectoryState::Unknown | DirectoryState::Retryable => {}
        }
        if matches!(&status.state, DirectoryState::Unknown) {
            match receipt_for(self.inner.authority.as_ref(), &status.intent).await {
                Ok(Some(receipt)) => {
                    if let Err(error) = save_state_async(
                        self,
                        commit_id,
                        intent::State::Committed.code(),
                        Some(receipt.clone()),
                        None,
                        StateFilter::Any,
                    )
                    .await
                    {
                        return Err(unknown_after_dispatch(
                            self,
                            commit_id,
                            format!(
                                "original receipt was found but local confirmation failed: {error}"
                            ),
                        )
                        .await);
                    }
                    return Ok(receipt);
                }
                Ok(None) => {}
                Err(error) => {
                    return Err(unknown_after_dispatch(self, commit_id, error).await);
                }
            }
        }
        let request = match self.inner.authority.restore_transaction(&row.request).await {
            Ok(request) => request,
            Err(error) => {
                let error = Error::from(error);
                if matches!(&status.state, DirectoryState::Unknown) {
                    return Err(unknown_after_dispatch(self, commit_id, error).await);
                }
                let state = match error.kind() {
                    ErrorKind::Invalid | ErrorKind::Corrupt | ErrorKind::Unsupported => {
                        intent::State::Failed.code()
                    }
                    _ => intent::State::Retryable.code(),
                };
                let failure = Failure::capture(&error);
                save_state_async(
                    self,
                    commit_id,
                    state,
                    None,
                    Some(failure.clone()),
                    StateFilter::Local,
                )
                .await?;
                return Err(Error::Retained(failure));
            }
        };
        if request.id() != commit_id || request.digest() != row.digest() {
            let error = Error::local_kind(
                ErrorKind::Corrupt,
                "restored directory transaction identity disagrees",
            );
            if matches!(&status.state, DirectoryState::Unknown) {
                return Err(unknown_after_dispatch(self, commit_id, error).await);
            }
            let failure = Failure::capture(&error);
            save_state_async(
                self,
                commit_id,
                intent::State::Failed.code(),
                None,
                Some(failure.clone()),
                StateFilter::Local,
            )
            .await?;
            return Err(Error::Retained(failure));
        }
        // From this point a dispatch may occur. Unknown is durable first.
        match claim_dispatch(self, commit_id).await {
            Ok(DispatchClaim::Dispatch) => {}
            Ok(DispatchClaim::Resolved(receipt)) => return Ok(receipt),
            Ok(DispatchClaim::Stopped(status)) => {
                return Err(Error::Retained(status.failure.ok_or_else(|| {
                    Error::local_kind(ErrorKind::Corrupt, "terminal intent has no failure")
                })?));
            }
            Err(error) => {
                if matches!(&status.state, DirectoryState::Unknown) {
                    return Err(unknown_after_dispatch(self, commit_id, error).await);
                }
                return Err(error);
            }
        }
        match self.inner.authority.commit(&request).await {
            Ok(CommitOutcome::Committed(receipt)) => {
                if receipt.commit_id != commit_id || receipt.request_digest != row.digest() {
                    return Err(unknown_after_dispatch(
                        self,
                        commit_id,
                        "authority returned a receipt for another directory request",
                    )
                    .await);
                }
                if let Err(error) = save_state_async(
                    self,
                    commit_id,
                    intent::State::Committed.code(),
                    Some(receipt.clone()),
                    None,
                    StateFilter::Any,
                )
                .await
                {
                    return Err(unknown_after_dispatch(
                        self,
                        commit_id,
                        format!("authority committed but local confirmation failed: {error}"),
                    )
                    .await);
                }
                Ok(receipt)
            }
            Ok(CommitOutcome::Conflict) => {
                let error = Error::State(
                    ErrorKind::Conflict,
                    "directory predicates changed; the original request is retained",
                );
                let failure = Failure::capture(&error);
                if let Err(error) = save_state_async(
                    self,
                    commit_id,
                    intent::State::Conflict.code(),
                    None,
                    Some(failure.clone()),
                    StateFilter::Unknown,
                )
                .await
                {
                    return Err(unknown_after_dispatch(self, commit_id, error).await);
                }
                Err(Error::Retained(failure))
            }
            Ok(CommitOutcome::Retryable) => {
                let failure = Failure::capture(&Error::Retryable);
                if let Err(error) = save_state_async(
                    self,
                    commit_id,
                    intent::State::Retryable.code(),
                    None,
                    Some(failure.clone()),
                    StateFilter::Unknown,
                )
                .await
                {
                    return Err(unknown_after_dispatch(self, commit_id, error).await);
                }
                Err(Error::Retained(failure))
            }
            Ok(CommitOutcome::Unknown(commit)) => {
                Err(unknown_after_dispatch(self, commit_id, Error::Unknown(commit)).await)
            }
            Err(error) => Err(unknown_after_dispatch(self, commit_id, error).await),
        }
    }

    pub async fn status(&self, commit_id: CommitId) -> Result<Option<DirectoryStatus>> {
        let _gate = self.inner.gate.lock().await;
        load_async(self, commit_id)
            .await?
            .map(|row| row.status(commit_id))
            .transpose()
    }

    pub async fn intents(&self) -> Result<Vec<DirectoryStatus>> {
        let _gate = self.inner.gate.lock().await;
        let rows = self
            .call(|connection| {
                let mut statement = connection
                    .prepare(
                        "SELECT id,request_id,request_digest,request,commit_id,state,
                                receipt_revision,receipt_ordinal,failure,metadata
                         FROM intents ORDER BY id",
                    )
                    .map_err(sql)?;
                let rows = statement.query_map([], row_from_sql).map_err(sql)?;
                rows.collect::<rusqlite::Result<Vec<_>>>().map_err(sql)
            })
            .await?;
        rows.into_iter()
            .map(|row| row.status(commit_id(&row)?))
            .collect()
    }

    /// Inspect local durable state without an authority, network or writer lease.
    pub async fn inspect(directory: impl AsRef<Path>) -> Result<DirectoryInspection> {
        let path = directory.as_ref().join("directory.db");
        tokio::task::spawn_blocking(move || -> Result<_> {
            let connection = store::open_read_only(&path).map_err(store_error)?;
            let (filesystem, root): (Vec<u8>, Vec<u8>) = connection
                .query_row(
                    "SELECT filesystem,root FROM binding WHERE singleton=1",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .map_err(sql)?;
            let filesystem: [u8; 16] = filesystem
                .try_into()
                .map_err(|_| Error::State(ErrorKind::Corrupt, "invalid directory filesystem"))?;
            let root: [u8; 16] = root
                .try_into()
                .map_err(|_| Error::State(ErrorKind::Corrupt, "invalid directory root"))?;
            validate_binding(&connection, filesystem, root)?;
            let mut statement = connection
                .prepare(
                    "SELECT id,request_id,request_digest,request,commit_id,state,
                            receipt_revision,receipt_ordinal,failure,metadata
                     FROM intents ORDER BY id",
                )
                .map_err(sql)?;
            let rows = statement.query_map([], row_from_sql).map_err(sql)?;
            let intents = rows
                .map(|row| {
                    let row = row.map_err(sql)?;
                    row.status(commit_id(&row)?)
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(DirectoryInspection {
                filesystem: NodeId::from_bytes(filesystem),
                root: NodeId::from_bytes(root),
                intents,
            })
        })
        .await
        .map_err(local)?
    }

    /// Export the prepared namespace from its frozen transaction and manifest.
    ///
    /// This reads authenticated immutable content from the authority. It never
    /// scans the source. The destination rules match pinned snapshot restoration.
    pub async fn export(&self, commit_id: CommitId, destination: &Path) -> Result<()> {
        let _gate = self.inner.gate.lock().await;
        let row = load_async(self, commit_id)
            .await?
            .ok_or_else(|| Error::local_kind(ErrorKind::NotFound, "unknown directory intent"))?;
        let status = row.status(commit_id)?;
        if !status.can_export() {
            return Err(Error::Retained(status.failure.ok_or_else(|| {
                Error::local_kind(ErrorKind::Corrupt, "failed intent has no diagnostic")
            })?));
        }
        let (baseline, request_id, digest) = Transaction::inspect_identity(&row.request)?;
        if baseline != status.intent.baseline || request_id != commit_id || digest != row.digest() {
            return Err(Error::local_kind(
                ErrorKind::Corrupt,
                "retained directory transaction identity disagrees",
            ));
        }
        let metadata: DirectoryMetadata = borsh::from_slice(&row.metadata).map_err(local)?;
        let manifest: DirectoryManifest = borsh::from_slice(&metadata.manifest).map_err(local)?;
        let entries = manifest.install_entries()?;
        install_directory(self.inner.authority.as_ref(), destination, entries)
            .await
            .map_err(Error::from)
    }

    async fn call<T: Send + 'static>(
        &self,
        function: impl FnOnce(&mut Connection) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let mut connection = self.inner.database.clone().lock_owned().await;
        tokio::task::spawn_blocking(move || function(&mut connection))
            .await
            .map_err(local)?
    }
}

#[derive(Clone)]
struct StoredRow {
    id: Vec<u8>,
    request_id: Vec<u8>,
    digest: Vec<u8>,
    request: Vec<u8>,
    commit_id: Option<Vec<u8>>,
    state: u8,
    receipt_revision: Option<Vec<u8>>,
    receipt_ordinal: Option<i64>,
    failure: Option<Vec<u8>>,
    metadata: Vec<u8>,
}

#[derive(BorshSerialize, BorshDeserialize)]
struct DirectoryMetadata {
    source: String,
    manifest: Vec<u8>,
}

impl StoredRow {
    fn digest(&self) -> [u8; 32] {
        self.digest
            .as_slice()
            .try_into()
            .expect("stored request digest length")
    }

    fn status(&self, id: CommitId) -> Result<DirectoryStatus> {
        let stored_id =
            CommitId::from_bytes(self.id.as_slice().try_into().map_err(|_| {
                Error::local_kind(ErrorKind::Corrupt, "invalid directory intent id")
            })?);
        if stored_id != id {
            return Err(Error::local_kind(
                ErrorKind::Corrupt,
                "directory intent identity disagrees",
            ));
        }
        let digest: [u8; 32] = self.digest.as_slice().try_into().map_err(|_| {
            Error::local_kind(ErrorKind::Corrupt, "invalid directory request digest")
        })?;
        let (request_baseline, request_id, request_digest) =
            Transaction::inspect_identity(&self.request).map_err(|error| {
                Error::local_kind(
                    ErrorKind::Corrupt,
                    format!("invalid retained frozen request: {error}"),
                )
            })?;
        let expected_request_id: [u8; 16] = self
            .request_id
            .as_slice()
            .try_into()
            .map_err(|_| Error::local_kind(ErrorKind::Corrupt, "invalid request id"))?;
        if request_id != id || request_digest != digest || expected_request_id != *id.as_bytes() {
            return Err(Error::local_kind(
                ErrorKind::Corrupt,
                "stored directory request identity disagrees",
            ));
        }
        if self
            .commit_id
            .as_deref()
            .is_some_and(|commit| commit != id.as_bytes())
        {
            return Err(Error::local_kind(
                ErrorKind::Corrupt,
                "stored directory commit id disagrees",
            ));
        }
        let state = match decode_state(self.state)? {
            intent::State::Prepared => DirectoryState::Prepared,
            intent::State::Unknown => DirectoryState::Unknown,
            intent::State::Committed => DirectoryState::Committed(receipt(self, id, digest)?),
            intent::State::Conflict => DirectoryState::Conflict,
            intent::State::Retryable => DirectoryState::Retryable,
            intent::State::Failed => DirectoryState::Failed,
        };
        if !matches!(state, DirectoryState::Committed(_))
            && (self.receipt_revision.is_some() || self.receipt_ordinal.is_some())
        {
            return Err(Error::local_kind(
                ErrorKind::Corrupt,
                "receipt fields exist outside a committed state",
            ));
        }
        let failure = self
            .failure
            .as_deref()
            .map(borsh::from_slice)
            .transpose()
            .map_err(local)?;
        let metadata: DirectoryMetadata = borsh::from_slice(&self.metadata).map_err(local)?;
        Ok(DirectoryStatus {
            intent: DirectoryIntent {
                commit_id: id,
                baseline: request_baseline,
                request_digest: digest,
                source: PathBuf::from(metadata.source),
            },
            state,
            failure,
        })
    }
}

fn row_from_sql(row: &rusqlite::Row<'_>) -> rusqlite::Result<StoredRow> {
    Ok(StoredRow {
        id: row.get(0)?,
        request_id: row.get(1)?,
        digest: row.get(2)?,
        request: row.get(3)?,
        commit_id: row.get(4)?,
        state: row.get(5)?,
        receipt_revision: row.get(6)?,
        receipt_ordinal: row.get(7)?,
        failure: row.get(8)?,
        metadata: row.get(9)?,
    })
}

fn validate_binding(connection: &Connection, filesystem: [u8; 16], root: [u8; 16]) -> Result<()> {
    store::validate(
        connection,
        &PROFILE,
        Some(&[
            store::Binding::blob("filesystem", filesystem.to_vec()),
            store::Binding::blob("root", root.to_vec()),
        ]),
    )
    .map_err(store_error)
}

fn load(connection: &Connection, id: CommitId) -> Result<Option<StoredRow>> {
    connection
        .query_row(
            "SELECT id,request_id,request_digest,request,commit_id,state,
                    receipt_revision,receipt_ordinal,failure,metadata
             FROM intents WHERE id=?1",
            params![id.as_bytes().as_slice()],
            row_from_sql,
        )
        .optional()
        .map_err(sql)
}

async fn load_async(journal: &DirectoryJournal, id: CommitId) -> Result<Option<StoredRow>> {
    journal.call(move |connection| load(connection, id)).await
}

fn save_state(
    connection: &Connection,
    id: CommitId,
    state: u8,
    receipt: Option<&Receipt>,
    failure: Option<&Failure>,
    filter: StateFilter,
) -> Result<()> {
    let receipt_revision = receipt.map(|receipt| receipt.cursor.revision.to_bytes().to_vec());
    let receipt_ordinal = receipt.map(|receipt| i64::from(receipt.cursor.ordinal));
    let failure = failure.map(borsh::to_vec).transpose().map_err(local)?;
    let statement = match filter {
        StateFilter::Active => {
            "UPDATE intents
             SET state=?2,receipt_revision=?3,receipt_ordinal=?4,failure=?5
             WHERE id=?1 AND state IN (0,1,4)"
        }
        StateFilter::Any => {
            "UPDATE intents
             SET state=?2,receipt_revision=?3,receipt_ordinal=?4,failure=?5
             WHERE id=?1"
        }
        StateFilter::Local => {
            "UPDATE intents
             SET state=?2,receipt_revision=?3,receipt_ordinal=?4,failure=?5
             WHERE id=?1 AND state IN (0,4)"
        }
        StateFilter::Unknown => {
            "UPDATE intents
             SET state=?2,receipt_revision=?3,receipt_ordinal=?4,failure=?5
             WHERE id=?1 AND state=1"
        }
    };
    let changed = connection
        .execute(
            statement,
            params![
                id.as_bytes().as_slice(),
                state,
                receipt_revision,
                receipt_ordinal,
                failure
            ],
        )
        .map_err(sql)?;
    if changed != 1 {
        return Err(Error::local_kind(
            ErrorKind::Busy,
            "directory intent state changed concurrently; inspect it before retrying",
        ));
    }
    Ok(())
}

async fn save_state_async(
    journal: &DirectoryJournal,
    id: CommitId,
    state: u8,
    receipt: Option<Receipt>,
    failure: Option<Failure>,
    filter: StateFilter,
) -> Result<()> {
    journal
        .call(move |connection| {
            let tx = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql)?;
            save_state(&tx, id, state, receipt.as_ref(), failure.as_ref(), filter)?;
            tx.commit().map_err(sql)
        })
        .await
}

async fn unknown_after_dispatch(
    journal: &DirectoryJournal,
    id: CommitId,
    cause: impl std::fmt::Display,
) -> Error {
    let cause = cause.to_string();
    let failure = Failure::unknown_with(id, &cause);
    match save_state_async(
        journal,
        id,
        intent::State::Unknown.code(),
        None,
        Some(failure.clone()),
        StateFilter::Unknown,
    )
    .await
    {
        Ok(()) => Error::Retained(failure),
        Err(error) => Error::Retained(Failure::unknown_with(
            id,
            format!("{cause}; failed to persist Unknown diagnostic: {error}"),
        )),
    }
}

enum DispatchClaim {
    Dispatch,
    Resolved(Receipt),
    Stopped(DirectoryStatus),
}

async fn claim_dispatch(journal: &DirectoryJournal, id: CommitId) -> Result<DispatchClaim> {
    for _ in 0..2 {
        match save_state_async(
            journal,
            id,
            intent::State::Unknown.code(),
            None,
            None,
            StateFilter::Active,
        )
        .await
        {
            Ok(()) => return Ok(DispatchClaim::Dispatch),
            Err(error) if error.kind() == ErrorKind::Busy => {
                let row = load_async(journal, id).await?.ok_or_else(|| {
                    Error::local_kind(ErrorKind::NotFound, "directory intent disappeared")
                })?;
                let status = row.status(id)?;
                match &status.state {
                    DirectoryState::Committed(receipt) => {
                        return Ok(DispatchClaim::Resolved(receipt.clone()));
                    }
                    DirectoryState::Conflict | DirectoryState::Failed => {
                        return Ok(DispatchClaim::Stopped(status));
                    }
                    // Another exact dispatcher claimed the request. Replay is
                    // idempotent, so this process may continue with the same plan.
                    DirectoryState::Unknown => return Ok(DispatchClaim::Dispatch),
                    DirectoryState::Prepared | DirectoryState::Retryable => {}
                }
            }
            Err(error) => return Err(error),
        }
    }
    Err(Error::local_kind(
        ErrorKind::Busy,
        "directory intent remained busy while claiming dispatch",
    ))
}

async fn receipt_for(
    authority: &dyn Authority,
    intent: &DirectoryIntent,
) -> Result<Option<Receipt>> {
    intent::receipt_for(authority, intent.commit_id, intent.request_digest)
        .await
        .map_err(Error::from)
}

fn receipt(row: &StoredRow, id: CommitId, digest: [u8; 32]) -> Result<Receipt> {
    let revision = row
        .receipt_revision
        .as_deref()
        .ok_or_else(|| Error::local_kind(ErrorKind::Corrupt, "missing receipt revision"))?;
    let revision: [u8; 24] = revision
        .try_into()
        .map_err(|_| Error::local_kind(ErrorKind::Corrupt, "invalid receipt revision"))?;
    intent::receipt(*id.as_bytes(), digest, Some(revision), row.receipt_ordinal)
        .map_err(|error| Error::local_kind(ErrorKind::Corrupt, error))?
        .ok_or_else(|| Error::local_kind(ErrorKind::Corrupt, "missing receipt fields"))
}

fn commit_id(row: &StoredRow) -> Result<CommitId> {
    let bytes: [u8; 16] = row
        .id
        .as_slice()
        .try_into()
        .map_err(|_| Error::local_kind(ErrorKind::Corrupt, "invalid commit identity"))?;
    Ok(CommitId::from_bytes(bytes))
}

fn existing_identity(id: CommitId) -> Error {
    Error::local_kind(
        ErrorKind::AlreadyExists,
        format!(
            "directory publication {id:?} is already prepared; retry it without a source using this commit identity, and use a new identity for different content"
        ),
    )
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

fn local(error: impl std::error::Error + 'static) -> Error {
    let cause: &dyn std::error::Error = &error;
    if let Some(error) = cause.downcast_ref::<std::io::Error>() {
        return match error.kind() {
            std::io::ErrorKind::PermissionDenied => {
                Error::local_kind(ErrorKind::PermissionDenied, error)
            }
            std::io::ErrorKind::StorageFull => Error::local_kind(ErrorKind::NoSpace, error),
            std::io::ErrorKind::ReadOnlyFilesystem => Error::local_kind(ErrorKind::ReadOnly, error),
            _ => Error::local_kind(ErrorKind::Io, error),
        };
    }
    Error::local_kind(ErrorKind::Io, error)
}

fn sql(error: rusqlite::Error) -> Error {
    let kind = match &error {
        rusqlite::Error::QueryReturnedNoRows => ErrorKind::NotFound,
        rusqlite::Error::SqliteFailure(code, _) => match code.code {
            rusqlite::ErrorCode::ConstraintViolation => ErrorKind::AlreadyExists,
            rusqlite::ErrorCode::DiskFull => ErrorKind::NoSpace,
            rusqlite::ErrorCode::ReadOnly => ErrorKind::ReadOnly,
            rusqlite::ErrorCode::PermissionDenied => ErrorKind::PermissionDenied,
            rusqlite::ErrorCode::DatabaseBusy | rusqlite::ErrorCode::DatabaseLocked => {
                ErrorKind::Busy
            }
            rusqlite::ErrorCode::DatabaseCorrupt | rusqlite::ErrorCode::NotADatabase => {
                ErrorKind::Corrupt
            }
            _ => ErrorKind::Io,
        },
        _ => ErrorKind::Io,
    };
    Error::local_kind(kind, error)
}
