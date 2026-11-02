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

//! Durable direct-from-local-file content coordination for native adapters.

use crate::intent;
use crate::local::store;
use crate::runtime::{Error, ErrorKind, Failure, Result};
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use std::path::Path;
use std::sync::Arc;
use tokio::io::AsyncRead;
use tokio::sync::Mutex;
use yinyang_core::{Authority, CommitId, NodeId, Planner, Receipt, Revision, Transaction};

const SCHEMA: &[store::TableColumns<'static>] = &[
    ("binding", &["singleton", "profile", "filesystem"]),
    (intent::TABLE, intent::COLUMNS),
];
const PROFILE: store::Profile = store::Profile {
    name: "yinyang-sync-4",
    table: "binding",
    schema: SCHEMA,
};
const CREATE_SCHEMA: &str = "
    CREATE TABLE binding(
      singleton INTEGER PRIMARY KEY CHECK(singleton=1),
      profile TEXT NOT NULL,
      filesystem BLOB NOT NULL
    );";

fn store_error(error: store::Error) -> Error {
    Error::local_kind(error.kind().into(), error)
}

fn validate_profile(connection: &Connection, filesystem: NodeId) -> Result<()> {
    store::validate(
        connection,
        &PROFILE,
        Some(&[store::Binding::blob(
            "filesystem",
            filesystem.as_bytes().to_vec(),
        )]),
    )
    .map_err(store_error)
}

pub(crate) async fn preflight_open(
    directory: &Path,
    filesystem: NodeId,
    require_journal: bool,
) -> Result<()> {
    let journal = directory.join("sync.db");
    if !journal.try_exists().map_err(Error::local)? {
        return if require_journal {
            Err(Error::local_kind(
                ErrorKind::Invalid,
                "sync journal is missing",
            ))
        } else {
            Ok(())
        };
    }
    tokio::task::spawn_blocking(move || {
        let connection = store::open_read_only(&journal).map_err(store_error)?;
        validate_profile(&connection, filesystem)
    })
    .await
    .map_err(Error::local)?
}

/// Content-addressed local intent, distinct from the frozen remote commit ID.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct EditId([u8; 32]);

impl EditId {
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IntentState {
    Prepared,
    Unknown,
    Committed,
    Conflict,
    Retryable,
    Failed,
}

impl IntentState {
    const fn from_shared(state: intent::State) -> Self {
        match state {
            intent::State::Prepared => Self::Prepared,
            intent::State::Unknown => Self::Unknown,
            intent::State::Committed => Self::Committed,
            intent::State::Conflict => Self::Conflict,
            intent::State::Retryable => Self::Retryable,
            intent::State::Failed => Self::Failed,
        }
    }
}

#[derive(Clone, Debug)]
pub struct ContentStatus {
    pub edit: EditId,
    pub commit_id: CommitId,
    pub baseline: Revision,
    pub state: IntentState,
    pub pending: bool,
    pub frozen: bool,
    pub conflict: bool,
    pub remote_revision: Revision,
    pub length: u64,
    pub error: Option<Failure>,
}

#[derive(Clone, Debug)]
pub struct SyncInspect {
    pub intents: Vec<ContentStatus>,
}

#[derive(Clone)]
struct IntentRow {
    id: Vec<u8>,
    request_digest: Vec<u8>,
    request: Vec<u8>,
    commit_id: Option<Vec<u8>>,
    state: u8,
    receipt_revision: Option<Vec<u8>>,
    receipt_ordinal: Option<i64>,
    failure: Option<Vec<u8>>,
}

impl IntentRow {
    fn edit(&self) -> Result<EditId> {
        Ok(EditId(self.id.as_slice().try_into().map_err(|_| {
            Error::local_kind(ErrorKind::Corrupt, "invalid content intent id")
        })?))
    }

    fn commit_id(&self) -> Result<CommitId> {
        let commit = self.commit_id.as_deref().ok_or_else(|| {
            Error::local_kind(ErrorKind::Corrupt, "content intent has no commit id")
        })?;
        Ok(CommitId::from_bytes(commit.try_into().map_err(|_| {
            Error::local_kind(ErrorKind::Corrupt, "invalid content commit id")
        })?))
    }

    fn digest(&self) -> Result<[u8; 32]> {
        self.request_digest
            .as_slice()
            .try_into()
            .map_err(|_| Error::local_kind(ErrorKind::Corrupt, "invalid content request digest"))
    }

    fn receipt(&self) -> Result<Option<Receipt>> {
        let Some(commit_id) = self.commit_id.as_deref() else {
            return Ok(None);
        };
        intent::receipt(
            commit_id
                .try_into()
                .map_err(|_| Error::local_kind(ErrorKind::Corrupt, "invalid content commit id"))?,
            self.digest()?,
            self.receipt_revision
                .as_deref()
                .map(|bytes| {
                    bytes.try_into().map_err(|_| {
                        Error::local_kind(ErrorKind::Corrupt, "invalid receipt revision")
                    })
                })
                .transpose()?,
            self.receipt_ordinal,
        )
        .map_err(|error| Error::local_kind(ErrorKind::Corrupt, error))
    }

    fn status(&self) -> Result<ContentStatus> {
        let state = intent::State::from_code(self.state)
            .ok_or_else(|| Error::local_kind(ErrorKind::Corrupt, "invalid content intent state"))?;
        let receipt = self.receipt()?;
        let (baseline, _, _) = Transaction::inspect_identity(&self.request).map_err(|error| {
            Error::local_kind(
                ErrorKind::Corrupt,
                format!("invalid content request: {error}"),
            )
        })?;
        let files = Transaction::inspect(&self.request)
            .map_err(|error| Error::local_kind(ErrorKind::Corrupt, error))?
            .1;
        if files.len() != 1 {
            return Err(Error::local_kind(
                ErrorKind::Corrupt,
                "content intent does not contain exactly one file",
            ));
        }
        let failure = self
            .failure
            .as_deref()
            .map(borsh::from_slice)
            .transpose()
            .map_err(|error| Error::local_kind(ErrorKind::Corrupt, error))?;
        Ok(ContentStatus {
            edit: self.edit()?,
            commit_id: self.commit_id()?,
            baseline,
            state: IntentState::from_shared(state),
            pending: state != intent::State::Committed,
            frozen: matches!(
                state,
                intent::State::Prepared | intent::State::Unknown | intent::State::Retryable
            ),
            conflict: state == intent::State::Conflict,
            remote_revision: receipt
                .as_ref()
                .map(|receipt| receipt.cursor.revision)
                .unwrap_or(baseline),
            length: files[0].content_id().length(),
            error: failure,
        })
    }
}

struct Inner {
    authority: Arc<dyn Authority>,
    journal: Arc<Mutex<Connection>>,
    gate: Mutex<()>,
}

/// Durable content publication from an exact local file.
///
/// This is the final public Sync surface. Namespace reconciliation belongs to
/// `sync_engine::SyncEngine`; this type is retained only for the in-process
/// native content bridge.
#[derive(Clone)]
pub struct Sync {
    inner: Arc<Inner>,
}

impl Sync {
    pub async fn validate_restore(directory: impl AsRef<Path>, filesystem: NodeId) -> Result<()> {
        preflight_open(directory.as_ref(), filesystem, true).await?;
        Self::inspect(directory).await.map(|_| ())
    }

    pub async fn open(authority: Arc<dyn Authority>, directory: impl AsRef<Path>) -> Result<Self> {
        let filesystem = authority.filesystem();
        let directory = directory.as_ref().to_owned();
        tokio::fs::create_dir_all(&directory)
            .await
            .map_err(Error::local)?;
        preflight_open(&directory, filesystem, false).await?;
        let path = directory.join("sync.db");
        let journal = tokio::task::spawn_blocking(move || -> Result<_> {
            let exists = path.try_exists().map_err(Error::local)?;
            let mut connection = store::open_read_write(&path, !exists).map_err(store_error)?;
            let empty = store::is_empty(&connection).map_err(store_error)?;
            let transaction = connection.transaction().map_err(Error::local)?;
            if empty {
                store::initialize(
                    &transaction,
                    &PROFILE,
                    CREATE_SCHEMA,
                    &[store::Binding::blob(
                        "filesystem",
                        filesystem.as_bytes().to_vec(),
                    )],
                )
                .map_err(store_error)?;
                transaction
                    .execute_batch(intent::CREATE_TABLE)
                    .map_err(Error::local)?;
            }
            validate_profile(&transaction, filesystem)?;
            transaction.commit().map_err(Error::local)?;
            store::configure(&connection, store::Durability::Journal).map_err(store_error)?;
            Ok(connection)
        })
        .await
        .map_err(Error::local)??;
        Ok(Self {
            inner: Arc::new(Inner {
                authority,
                journal: Arc::new(Mutex::new(journal)),
                gate: Mutex::new(()),
            }),
        })
    }

    fn authority(&self) -> &dyn Authority {
        self.inner.authority.as_ref()
    }

    async fn call<T: Send + 'static>(
        &self,
        function: impl FnOnce(&mut Connection) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let mut connection = self.inner.journal.clone().lock_owned().await;
        tokio::task::spawn_blocking(move || function(&mut connection))
            .await
            .map_err(Error::local)?
    }

    fn load(connection: &Connection, id: EditId) -> Result<Option<IntentRow>> {
        connection
            .query_row(
                "SELECT id,request_digest,request,commit_id,state,
                        receipt_revision,receipt_ordinal,failure
                 FROM intents WHERE id=?1",
                params![id.as_bytes().as_slice()],
                |row| {
                    Ok(IntentRow {
                        id: row.get(0)?,
                        request_digest: row.get(1)?,
                        request: row.get(2)?,
                        commit_id: row.get(3)?,
                        state: row.get(4)?,
                        receipt_revision: row.get(5)?,
                        receipt_ordinal: row.get(6)?,
                        failure: row.get(7)?,
                    })
                },
            )
            .optional()
            .map_err(Error::local)
    }

    pub async fn stage(
        &self,
        node: NodeId,
        baseline: Revision,
        source: &mut (dyn AsyncRead + Unpin + Send),
    ) -> Result<EditId> {
        let _gate = self.inner.gate.lock().await;
        let snapshot = self.authority().observe_revision(baseline).await?;
        let prepared = self.authority().prepare(source).await?;
        let content = prepared.descriptor().content_id();
        let mut seed = blake3::Hasher::new_derive_key("yinyang-sync-content-edit-2");
        seed.update(self.authority().filesystem().as_bytes());
        seed.update(node.as_bytes());
        seed.update(&baseline.to_bytes());
        seed.update(&content.length().to_le_bytes());
        seed.update(content.digest());
        let edit = EditId(*seed.finalize().as_bytes());
        let existing = self
            .call(move |connection| Self::load(connection, edit))
            .await?;
        if existing.is_some() {
            return Ok(edit);
        }
        let commit = derived_commit(edit);
        let mut planner = Planner::new(&snapshot, commit);
        let _ = planner.node(node).await?;
        planner.set_content(node, prepared).await?;
        let request = planner.finish()?;
        let bytes = request.to_bytes()?;
        let digest = request.digest();
        let state = intent::State::Prepared.code();
        self.call(move |connection| {
            if Self::load(connection, edit)?.is_some() {
                return Ok(());
            }
            let transaction = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(Error::local)?;
            transaction
                .execute(
                    "INSERT INTO intents(
                        id,request_id,request_digest,request,commit_id,state,
                        receipt_revision,receipt_ordinal,failure,metadata
                     ) VALUES(?1,?2,?3,?4,?5,?6,NULL,NULL,NULL,?7)",
                    params![
                        edit.as_bytes().as_slice(),
                        commit.as_bytes().as_slice(),
                        digest.as_slice(),
                        bytes,
                        commit.as_bytes().as_slice(),
                        state,
                        Vec::<u8>::new(),
                    ],
                )
                .map_err(Error::local)?;
            transaction.commit().map_err(Error::local)
        })
        .await?;
        Ok(edit)
    }

    pub async fn status(&self, edit: EditId) -> Result<ContentStatus> {
        let _gate = self.inner.gate.lock().await;
        self.call(move |connection| {
            Self::load(connection, edit)?
                .ok_or(Error::Invalid("unknown local edit"))?
                .status()
        })
        .await
    }

    pub async fn inspect(directory: impl AsRef<Path>) -> Result<SyncInspect> {
        let path = directory.as_ref().join("sync.db");
        tokio::task::spawn_blocking(move || {
            let connection = store::open_read_only(&path).map_err(store_error)?;
            store::validate(&connection, &PROFILE, None).map_err(store_error)?;
            let mut statement = connection
                .prepare(
                    "SELECT id,request_digest,request,commit_id,state,
                            receipt_revision,receipt_ordinal,failure
                     FROM intents ORDER BY id",
                )
                .map_err(Error::local)?;
            let rows = statement
                .query_map([], |row| {
                    Ok(IntentRow {
                        id: row.get(0)?,
                        request_digest: row.get(1)?,
                        request: row.get(2)?,
                        commit_id: row.get(3)?,
                        state: row.get(4)?,
                        receipt_revision: row.get(5)?,
                        receipt_ordinal: row.get(6)?,
                        failure: row.get(7)?,
                    })
                })
                .map_err(Error::local)?;
            let intents = rows
                .map(|row| row.map_err(Error::local).and_then(|row| row.status()))
                .collect::<Result<Vec<_>>>()?;
            Ok(SyncInspect { intents })
        })
        .await
        .map_err(Error::local)?
    }

    pub async fn publish(&self, edit: EditId) -> Result<Revision> {
        let _gate = self.inner.gate.lock().await;
        let row = self
            .call(move |connection| Self::load(connection, edit))
            .await?
            .ok_or(Error::Invalid("unknown local edit"))?;
        let state = intent::State::from_code(row.state)
            .ok_or_else(|| Error::local_kind(ErrorKind::Corrupt, "invalid content intent state"))?;
        match state {
            intent::State::Committed => {
                return Ok(row
                    .receipt()?
                    .ok_or_else(|| {
                        Error::local_kind(
                            ErrorKind::Corrupt,
                            "committed content intent has no receipt",
                        )
                    })?
                    .cursor
                    .revision);
            }
            intent::State::Conflict | intent::State::Failed => {
                let failure: Option<Failure> = row
                    .failure
                    .as_deref()
                    .map(borsh::from_slice)
                    .transpose()
                    .map_err(|error| Error::local_kind(ErrorKind::Corrupt, error))?;
                if let Some(failure) = failure {
                    return Err(Error::Retained(failure));
                }
                return if state == intent::State::Conflict {
                    Err(Error::Conflict)
                } else {
                    Err(Error::State(ErrorKind::Invalid, "content intent failed"))
                };
            }
            intent::State::Prepared | intent::State::Unknown | intent::State::Retryable => {}
        }
        if state == intent::State::Unknown
            && let Some(receipt) =
                intent::receipt_for(self.authority(), row.commit_id()?, row.digest()?).await?
        {
            self.save_state(edit, intent::State::Committed, Some(&receipt), None)
                .await?;
            return Ok(receipt.cursor.revision);
        }
        let request = match self.authority().restore_transaction(&row.request).await {
            Ok(request) => request,
            Err(error) => {
                let error = Error::from(error);
                let failure = Failure::capture(&error);
                let retry_state = if state == intent::State::Unknown {
                    intent::State::Unknown
                } else {
                    intent::State::Retryable
                };
                self.save_state(edit, retry_state, None, Some(&failure))
                    .await?;
                return Err(Error::Retained(failure));
            }
        };
        self.save_state(edit, intent::State::Unknown, None, None)
            .await?;
        match self.authority().commit(&request).await {
            Ok(yinyang_core::CommitOutcome::Committed(receipt)) => {
                self.save_state(edit, intent::State::Committed, Some(&receipt), None)
                    .await?;
                Ok(receipt.cursor.revision)
            }
            Ok(yinyang_core::CommitOutcome::Conflict) => {
                let failure = Failure::capture(&Error::State(
                    ErrorKind::Conflict,
                    "content predicates changed",
                ));
                self.save_state(edit, intent::State::Conflict, None, Some(&failure))
                    .await?;
                Err(Error::Retained(failure))
            }
            Ok(yinyang_core::CommitOutcome::Retryable) => {
                self.save_state(
                    edit,
                    intent::State::Retryable,
                    None,
                    Some(&Failure::capture(&Error::Retryable)),
                )
                .await?;
                Err(Error::Retryable)
            }
            Ok(yinyang_core::CommitOutcome::Unknown(commit)) => {
                let failure = Failure::unknown_with(commit, "content commit outcome is unknown");
                self.save_state(edit, intent::State::Unknown, None, Some(&failure))
                    .await?;
                Err(Error::Retained(failure))
            }
            Err(error) => {
                let failure = Failure::unknown_with(
                    row.commit_id()?,
                    format!("content commit failed after dispatch: {error}"),
                );
                self.save_state(edit, intent::State::Unknown, None, Some(&failure))
                    .await?;
                Err(Error::Retained(failure))
            }
        }
    }

    async fn save_state(
        &self,
        edit: EditId,
        state: intent::State,
        receipt: Option<&Receipt>,
        failure: Option<&Failure>,
    ) -> Result<()> {
        let revision = receipt.map(|receipt| receipt.cursor.revision.to_bytes().to_vec());
        let ordinal = receipt.map(|receipt| i64::from(receipt.cursor.ordinal));
        let failure = failure
            .map(borsh::to_vec)
            .transpose()
            .map_err(Error::local)?;
        self.call(move |connection| {
            let transaction = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(Error::local)?;
            let changed = transaction
                .execute(
                    "UPDATE intents SET state=?2,receipt_revision=?3,receipt_ordinal=?4,failure=?5
                     WHERE id=?1",
                    params![
                        edit.as_bytes().as_slice(),
                        state.code(),
                        revision,
                        ordinal,
                        failure
                    ],
                )
                .map_err(Error::local)?;
            if changed != 1 {
                return Err(Error::Invalid("content intent disappeared"));
            }
            transaction.commit().map_err(Error::local)
        })
        .await
    }
}

fn derived_commit(edit: EditId) -> CommitId {
    let digest = blake3::derive_key("yinyang-sync-content-commit-2", edit.as_bytes());
    CommitId::from_bytes(digest[..16].try_into().expect("fixed commit prefix"))
}
