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

//! Durable namespace intents on top of the shared Authority/Planner contract.
use super::{EditId, Sync};
use crate::runtime::{Error, ErrorKind, Failure, Result};
use borsh::{BorshDeserialize, BorshSerialize};
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use std::ops::Range;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, ReadBuf};
use yinyang_core::{ContentDescriptor, NodeId, Planner, Receipt, Revision, Transaction};

const PREPARED: u8 = 0;
const UNKNOWN: u8 = 1;
const COMMITTED: u8 = 2;
const CONFLICT: u8 = 3;
const RETRYABLE: u8 = 4;
const FAILED: u8 = 5;

/// Creation metadata represented by the current namespace contract.
///
/// The core persists only the executable bit. Adapters must reject ownership,
/// timestamps, ACLs, or richer mode bits before creating a sync intent.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FileMetadata {
    pub executable: bool,
}

impl FileMetadata {
    pub const fn new(executable: bool) -> Self {
        Self { executable }
    }
}

/// Destination behavior for a rename.
#[derive(Clone, Copy, Debug, Eq, PartialEq, BorshSerialize, BorshDeserialize)]
pub enum RenameMode {
    NoReplace,
    Replace,
}

/// The logical request retained by a namespace intent.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NamespaceOperation {
    CreateFile {
        node: NodeId,
        parent: NodeId,
        name: String,
        metadata: FileMetadata,
        content: ContentDescriptor,
    },
    CreateDirectory {
        node: NodeId,
        parent: NodeId,
        name: String,
    },
    Remove {
        node: NodeId,
    },
    Rename {
        node: NodeId,
        new_parent: NodeId,
        new_name: String,
        mode: RenameMode,
        replaced: Option<NodeId>,
    },
    SetExecutable {
        node: NodeId,
        executable: bool,
    },
}

impl NamespaceOperation {
    /// The created or source identity this intent operates on.
    pub const fn node_id(&self) -> NodeId {
        match self {
            Self::CreateFile { node, .. }
            | Self::CreateDirectory { node, .. }
            | Self::Remove { node }
            | Self::Rename { node, .. }
            | Self::SetExecutable { node, .. } => *node,
        }
    }
}

/// Stable identity and baseline-bound request for one namespace operation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NamespaceIntent {
    pub id: EditId,
    pub baseline: Revision,
    pub commit_id: yinyang_core::CommitId,
    pub request_digest: [u8; 32],
    pub operation: NamespaceOperation,
}

/// Durable local outcome of a namespace intent.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NamespaceState {
    /// Accepted locally and persisted before its first dispatch.
    Prepared,
    /// A dispatch may have published and no definitive outcome is stored.
    Unknown,
    Committed(Receipt),
    Conflict,
    Retryable,
    /// Validation, readiness, corruption, or another definitive failure.
    Failed,
}

/// Reopen-safe status for a namespace intent.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NamespaceStatus {
    pub intent: NamespaceIntent,
    pub state: NamespaceState,
    pub failure: Option<Failure>,
}

#[derive(BorshSerialize)]
enum RequestKey {
    CreateFile {
        parent: [u8; 16],
        name: String,
        executable: bool,
        length: u64,
        digest: [u8; 32],
    },
    CreateDirectory {
        parent: [u8; 16],
        name: String,
    },
    Remove {
        node: [u8; 16],
    },
    Rename {
        node: [u8; 16],
        new_parent: [u8; 16],
        new_name: String,
        mode: RenameMode,
    },
    SetExecutable {
        node: [u8; 16],
        executable: bool,
    },
}

#[derive(BorshSerialize, BorshDeserialize)]
enum OperationWire {
    CreateFile {
        node: [u8; 16],
        parent: [u8; 16],
        name: String,
        executable: bool,
        content: Vec<u8>,
    },
    CreateDirectory {
        node: [u8; 16],
        parent: [u8; 16],
        name: String,
    },
    Remove {
        node: [u8; 16],
    },
    Rename {
        node: [u8; 16],
        new_parent: [u8; 16],
        new_name: String,
        mode: RenameMode,
        replaced: Option<[u8; 16]>,
    },
    SetExecutable {
        node: [u8; 16],
        executable: bool,
    },
}

impl OperationWire {
    fn encode(operation: &NamespaceOperation) -> Result<Vec<u8>> {
        let wire = match operation {
            NamespaceOperation::CreateFile {
                node,
                parent,
                name,
                metadata,
                content,
            } => Self::CreateFile {
                node: *node.as_bytes(),
                parent: *parent.as_bytes(),
                name: name.clone(),
                executable: metadata.executable,
                content: content.to_bytes(),
            },
            NamespaceOperation::CreateDirectory { node, parent, name } => Self::CreateDirectory {
                node: *node.as_bytes(),
                parent: *parent.as_bytes(),
                name: name.clone(),
            },
            NamespaceOperation::Remove { node } => Self::Remove {
                node: *node.as_bytes(),
            },
            NamespaceOperation::Rename {
                node,
                new_parent,
                new_name,
                mode,
                replaced,
            } => Self::Rename {
                node: *node.as_bytes(),
                new_parent: *new_parent.as_bytes(),
                new_name: new_name.clone(),
                mode: *mode,
                replaced: replaced.map(|id| *id.as_bytes()),
            },
            NamespaceOperation::SetExecutable { node, executable } => Self::SetExecutable {
                node: *node.as_bytes(),
                executable: *executable,
            },
        };
        borsh::to_vec(&wire).map_err(Error::local)
    }

    fn decode(bytes: &[u8]) -> Result<NamespaceOperation> {
        let wire = borsh::from_slice(bytes)
            .map_err(|error| Error::local_kind(ErrorKind::Corrupt, error))?;
        match wire {
            Self::CreateFile {
                node,
                parent,
                name,
                executable,
                content,
            } => Ok(NamespaceOperation::CreateFile {
                node: NodeId::from_bytes(node),
                parent: NodeId::from_bytes(parent),
                name,
                metadata: FileMetadata { executable },
                content: ContentDescriptor::from_bytes(&content)?,
            }),
            Self::CreateDirectory { node, parent, name } => {
                Ok(NamespaceOperation::CreateDirectory {
                    node: NodeId::from_bytes(node),
                    parent: NodeId::from_bytes(parent),
                    name,
                })
            }
            Self::Remove { node } => Ok(NamespaceOperation::Remove {
                node: NodeId::from_bytes(node),
            }),
            Self::Rename {
                node,
                new_parent,
                new_name,
                mode,
                replaced,
            } => Ok(NamespaceOperation::Rename {
                node: NodeId::from_bytes(node),
                new_parent: NodeId::from_bytes(new_parent),
                new_name,
                mode,
                replaced: replaced.map(NodeId::from_bytes),
            }),
            Self::SetExecutable { node, executable } => Ok(NamespaceOperation::SetExecutable {
                node: NodeId::from_bytes(node),
                executable,
            }),
        }
    }
}

pub(super) fn initialize(connection: &Connection) -> Result<()> {
    connection
        .execute_batch(
            "CREATE TABLE IF NOT EXISTS namespaces(
                id BLOB PRIMARY KEY,
                intent_key BLOB NOT NULL,
                baseline BLOB NOT NULL,
                commit_id BLOB NOT NULL UNIQUE,
                request_digest BLOB NOT NULL,
                operation BLOB NOT NULL,
                request BLOB NOT NULL,
                state INTEGER NOT NULL,
                receipt_revision BLOB,
                receipt_ordinal INTEGER,
                failure BLOB
            );",
        )
        .map_err(Error::local)
}

#[derive(Clone)]
struct IntentRow {
    id: Vec<u8>,
    intent_key: Vec<u8>,
    baseline: Vec<u8>,
    commit_id: Vec<u8>,
    request_digest: Vec<u8>,
    operation: Vec<u8>,
    request: Vec<u8>,
    state: u8,
    receipt_revision: Option<Vec<u8>>,
    receipt_ordinal: Option<i64>,
    failure: Option<Vec<u8>>,
}

impl IntentRow {
    fn status(&self) -> Result<NamespaceStatus> {
        let id: [u8; 32] = self
            .id
            .as_slice()
            .try_into()
            .map_err(|_| Error::local_kind(ErrorKind::Corrupt, "invalid intent identity"))?;
        let baseline: [u8; 24] = self
            .baseline
            .as_slice()
            .try_into()
            .map_err(|_| Error::local_kind(ErrorKind::Corrupt, "invalid intent baseline"))?;
        let commit_id: [u8; 16] = self
            .commit_id
            .as_slice()
            .try_into()
            .map_err(|_| Error::local_kind(ErrorKind::Corrupt, "invalid commit identity"))?;
        let request_digest: [u8; 32] = self
            .request_digest
            .as_slice()
            .try_into()
            .map_err(|_| Error::local_kind(ErrorKind::Corrupt, "invalid request digest"))?;
        let state = match self.state {
            PREPARED => NamespaceState::Prepared,
            UNKNOWN => NamespaceState::Unknown,
            COMMITTED => {
                let revision = self.receipt_revision.as_deref().ok_or_else(|| {
                    Error::local_kind(ErrorKind::Corrupt, "missing receipt revision")
                })?;
                let ordinal = self.receipt_ordinal.ok_or_else(|| {
                    Error::local_kind(ErrorKind::Corrupt, "missing receipt ordinal")
                })?;
                let revision: [u8; 24] = revision.try_into().map_err(|_| {
                    Error::local_kind(ErrorKind::Corrupt, "invalid receipt revision")
                })?;
                let ordinal = u32::try_from(ordinal).map_err(|_| {
                    Error::local_kind(ErrorKind::Corrupt, "invalid receipt ordinal")
                })?;
                NamespaceState::Committed(Receipt {
                    commit_id: yinyang_core::CommitId::from_bytes(commit_id),
                    request_digest,
                    cursor: yinyang_core::Cursor {
                        revision: Revision::from_bytes(revision),
                        ordinal,
                    },
                })
            }
            CONFLICT => NamespaceState::Conflict,
            RETRYABLE => NamespaceState::Retryable,
            FAILED => NamespaceState::Failed,
            _ => {
                return Err(Error::local_kind(
                    ErrorKind::Corrupt,
                    "invalid namespace state",
                ));
            }
        };
        if !matches!(state, NamespaceState::Committed(_))
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
            .map_err(|error| Error::local_kind(ErrorKind::Corrupt, error))?;
        Ok(NamespaceStatus {
            intent: NamespaceIntent {
                id: EditId::from_bytes(id),
                baseline: Revision::from_bytes(baseline),
                commit_id: yinyang_core::CommitId::from_bytes(commit_id),
                request_digest,
                operation: OperationWire::decode(&self.operation)?,
            },
            state,
            failure,
        })
    }
}

fn row_from_sql(row: &rusqlite::Row<'_>) -> rusqlite::Result<IntentRow> {
    Ok(IntentRow {
        id: row.get(0)?,
        intent_key: row.get(1)?,
        baseline: row.get(2)?,
        commit_id: row.get(3)?,
        request_digest: row.get(4)?,
        operation: row.get(5)?,
        request: row.get(6)?,
        state: row.get(7)?,
        receipt_revision: row.get(8)?,
        receipt_ordinal: row.get(9)?,
        failure: row.get(10)?,
    })
}

fn load(connection: &Connection, id: EditId) -> Result<Option<IntentRow>> {
    connection
        .query_row(
            "SELECT id,intent_key,baseline,commit_id,request_digest,operation,request,
                    state,receipt_revision,receipt_ordinal,failure
             FROM namespaces WHERE id=?1",
            params![id.as_bytes().as_slice()],
            row_from_sql,
        )
        .optional()
        .map_err(Error::local)
}

async fn load_async(sync: &Sync, id: EditId) -> Result<Option<IntentRow>> {
    sync.call(move |connection| load(connection, id)).await
}

fn save_state(
    connection: &Connection,
    id: EditId,
    state: u8,
    receipt: Option<&Receipt>,
    failure: Option<&Failure>,
) -> Result<()> {
    let receipt_revision = receipt.map(|receipt| receipt.cursor.revision.to_bytes().to_vec());
    let receipt_ordinal = receipt.map(|receipt| i64::from(receipt.cursor.ordinal));
    let failure = failure
        .map(borsh::to_vec)
        .transpose()
        .map_err(Error::local)?;
    let changed = connection
        .execute(
            "UPDATE namespaces
             SET state=?2,receipt_revision=?3,receipt_ordinal=?4,failure=?5
             WHERE id=?1",
            params![
                id.as_bytes().as_slice(),
                state,
                receipt_revision,
                receipt_ordinal,
                failure
            ],
        )
        .map_err(Error::local)?;
    if changed != 1 {
        return Err(Error::local_kind(
            ErrorKind::NotFound,
            "namespace intent disappeared",
        ));
    }
    Ok(())
}

async fn save_state_async(
    sync: &Sync,
    id: EditId,
    state: u8,
    receipt: Option<Receipt>,
    failure: Option<Failure>,
) -> Result<()> {
    sync.call(move |connection| {
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(Error::local)?;
        save_state(&tx, id, state, receipt.as_ref(), failure.as_ref())?;
        tx.commit().map_err(Error::local)
    })
    .await
}

fn request_key(key: &RequestKey) -> Result<Vec<u8>> {
    borsh::to_vec(key).map_err(Error::local)
}

fn intent_id(baseline: Revision, key: &[u8], filesystem: NodeId) -> EditId {
    let mut hash = blake3::Hasher::new_derive_key("yinyang-sync-namespace-intent-1");
    hash.update(filesystem.as_bytes());
    hash.update(&baseline.to_bytes());
    hash.update(key);
    EditId::from_bytes(*hash.finalize().as_bytes())
}

fn commit_id(id: EditId) -> yinyang_core::CommitId {
    let digest = blake3::derive_key("yinyang-sync-namespace-commit-1", id.as_bytes());
    yinyang_core::CommitId::from_bytes(digest[..16].try_into().expect("fixed commit prefix"))
}

struct ContentDigest {
    hash: blake3::Hasher,
    length: u64,
}

struct HashingReader<'a> {
    source: &'a mut (dyn AsyncRead + Unpin + Send),
    digest: ContentDigest,
}

impl<'a> HashingReader<'a> {
    fn new(source: &'a mut (dyn AsyncRead + Unpin + Send)) -> Self {
        Self {
            source,
            digest: ContentDigest {
                hash: blake3::Hasher::new(),
                length: 0,
            },
        }
    }

    fn finish(self) -> (u64, [u8; 32]) {
        (self.digest.length, *self.digest.hash.finalize().as_bytes())
    }
}

impl AsyncRead for HashingReader<'_> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        let before = buffer.filled().len();
        match Pin::new(&mut *this.source).poll_read(cx, buffer) {
            Poll::Ready(Ok(())) => {
                let bytes = &buffer.filled()[before..];
                this.digest.hash.update(bytes);
                this.digest.length = this
                    .digest
                    .length
                    .checked_add(bytes.len() as u64)
                    .ok_or_else(|| std::io::Error::other("namespace file length overflowed"))?;
                Poll::Ready(Ok(()))
            }
            other => other,
        }
    }
}

async fn insert(
    sync: &Sync,
    id: EditId,
    key: Vec<u8>,
    baseline: Revision,
    commit_id: yinyang_core::CommitId,
    operation: Vec<u8>,
    request: &Transaction,
) -> Result<()> {
    let request_digest = request.digest();
    let request = request.to_bytes()?;
    sync.call(move |connection| {
        let existing = load(connection, id)?;
        if let Some(existing) = existing {
            if existing.intent_key != key {
                return Err(Error::local_kind(
                    ErrorKind::Invalid,
                    "namespace intent identity collision",
                ));
            }
            return Ok(());
        }
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(Error::local)?;
        tx.execute(
            "INSERT INTO namespaces(
                id,intent_key,baseline,commit_id,request_digest,operation,request,state
             ) VALUES(?1,?2,?3,?4,?5,?6,?7,?8)",
            params![
                id.as_bytes().as_slice(),
                key,
                baseline.to_bytes().as_slice(),
                commit_id.as_bytes().as_slice(),
                request_digest.as_slice(),
                operation,
                request,
                PREPARED
            ],
        )
        .map_err(Error::local)?;
        tx.commit().map_err(Error::local)
    })
    .await
}

/// Durably accept a file creation intent and its immutable bytes.
pub async fn stage_create_file(
    sync: &Sync,
    parent: NodeId,
    name: &str,
    baseline: Revision,
    metadata: FileMetadata,
    source: &mut (dyn AsyncRead + Unpin + Send),
) -> Result<EditId> {
    let _gate = sync.inner.gate.lock().await;
    sync.collect_incomplete().await?;
    let snapshot = sync.authority().observe_revision(baseline).await?;
    let mut reader = HashingReader::new(source);
    let prepared = sync.authority().prepare(&mut reader).await?;
    let (length, digest) = reader.finish();
    let key = RequestKey::CreateFile {
        parent: *parent.as_bytes(),
        name: name.to_owned(),
        executable: metadata.executable,
        length,
        digest,
    };
    let key = request_key(&key)?;
    let id = intent_id(baseline, &key, sync.authority().filesystem());
    if let Some(existing) = load_async(sync, id).await? {
        if existing.intent_key != key {
            return Err(Error::local_kind(
                ErrorKind::Invalid,
                "namespace intent identity collision",
            ));
        }
        return Ok(id);
    }
    let mut planner = Planner::new(&snapshot, commit_id(id));
    let node = planner
        .create_file(parent, name, prepared.clone(), metadata.executable)
        .await?;
    let request = planner.finish()?;
    let operation = NamespaceOperation::CreateFile {
        node,
        parent,
        name: name.to_owned(),
        metadata,
        content: prepared.descriptor().clone(),
    };
    insert(
        sync,
        id,
        key,
        baseline,
        commit_id(id),
        OperationWire::encode(&operation)?,
        &request,
    )
    .await?;
    Ok(id)
}

/// Durably accept a directory creation intent.
pub async fn stage_create_directory(
    sync: &Sync,
    parent: NodeId,
    name: &str,
    baseline: Revision,
) -> Result<EditId> {
    let _gate = sync.inner.gate.lock().await;
    sync.collect_incomplete().await?;
    let snapshot = sync.authority().observe_revision(baseline).await?;
    let key = RequestKey::CreateDirectory {
        parent: *parent.as_bytes(),
        name: name.to_owned(),
    };
    let key = request_key(&key)?;
    let id = intent_id(baseline, &key, sync.authority().filesystem());
    if let Some(existing) = load_async(sync, id).await? {
        if existing.intent_key != key {
            return Err(Error::local_kind(
                ErrorKind::Invalid,
                "namespace intent identity collision",
            ));
        }
        return Ok(id);
    }
    let mut planner = Planner::new(&snapshot, commit_id(id));
    let node = planner.create_directory(parent, name).await?;
    let request = planner.finish()?;
    let operation = NamespaceOperation::CreateDirectory {
        node,
        parent,
        name: name.to_owned(),
    };
    insert(
        sync,
        id,
        key,
        baseline,
        commit_id(id),
        OperationWire::encode(&operation)?,
        &request,
    )
    .await?;
    Ok(id)
}

/// Durably accept removal of an observed stable identity.
pub async fn stage_remove(sync: &Sync, node: NodeId, baseline: Revision) -> Result<EditId> {
    let _gate = sync.inner.gate.lock().await;
    sync.collect_incomplete().await?;
    let snapshot = sync.authority().observe_revision(baseline).await?;
    let key = RequestKey::Remove {
        node: *node.as_bytes(),
    };
    let key = request_key(&key)?;
    let id = intent_id(baseline, &key, sync.authority().filesystem());
    if let Some(existing) = load_async(sync, id).await? {
        if existing.intent_key != key {
            return Err(Error::local_kind(
                ErrorKind::Invalid,
                "namespace intent identity collision",
            ));
        }
        return Ok(id);
    }
    let mut planner = Planner::new(&snapshot, commit_id(id));
    // Remove already guards state, link, entry, and emptiness. The logical
    // guard also makes the whole retained node state an explicit dependency.
    let _ = planner.node(node).await?;
    planner.remove(node).await?;
    let request = planner.finish()?;
    let operation = NamespaceOperation::Remove { node };
    insert(
        sync,
        id,
        key,
        baseline,
        commit_id(id),
        OperationWire::encode(&operation)?,
        &request,
    )
    .await?;
    Ok(id)
}

/// Durably accept a rename, optionally replacing one observed destination.
pub async fn stage_rename(
    sync: &Sync,
    node: NodeId,
    baseline: Revision,
    new_parent: NodeId,
    new_name: &str,
    mode: RenameMode,
) -> Result<EditId> {
    let _gate = sync.inner.gate.lock().await;
    sync.collect_incomplete().await?;
    let snapshot = sync.authority().observe_revision(baseline).await?;
    let key = RequestKey::Rename {
        node: *node.as_bytes(),
        new_parent: *new_parent.as_bytes(),
        new_name: new_name.to_owned(),
        mode,
    };
    let key = request_key(&key)?;
    let id = intent_id(baseline, &key, sync.authority().filesystem());
    if let Some(existing) = load_async(sync, id).await? {
        if existing.intent_key != key {
            return Err(Error::local_kind(
                ErrorKind::Invalid,
                "namespace intent identity collision",
            ));
        }
        return Ok(id);
    }
    let mut planner = Planner::new(&snapshot, commit_id(id));
    // Rename's built-in guards preserve reachability. Guarding the complete
    // source state prevents a concurrent content or executable edit from
    // being silently carried across the move.
    let _ = planner.node(node).await?;
    let destination = planner.lookup(new_parent, new_name).await?;
    let replaced = destination
        .as_ref()
        .filter(|entry| entry.node_id != node)
        .map(|entry| entry.node_id);
    if let Some(replaced) = replaced {
        let _ = planner.node(replaced).await?;
    }
    match mode {
        RenameMode::NoReplace => planner.rename(node, new_parent, new_name).await?,
        RenameMode::Replace => planner.rename_replace(node, new_parent, new_name).await?,
    }
    let request = planner.finish()?;
    let operation = NamespaceOperation::Rename {
        node,
        new_parent,
        new_name: new_name.to_owned(),
        mode,
        replaced,
    };
    insert(
        sync,
        id,
        key,
        baseline,
        commit_id(id),
        OperationWire::encode(&operation)?,
        &request,
    )
    .await?;
    Ok(id)
}

/// Durably accept an executable-bit change to one observed file identity.
pub async fn stage_set_executable(
    sync: &Sync,
    node: NodeId,
    baseline: Revision,
    executable: bool,
) -> Result<EditId> {
    let _gate = sync.inner.gate.lock().await;
    sync.collect_incomplete().await?;
    let snapshot = sync.authority().observe_revision(baseline).await?;
    let key = RequestKey::SetExecutable {
        node: *node.as_bytes(),
        executable,
    };
    let key = request_key(&key)?;
    let id = intent_id(baseline, &key, sync.authority().filesystem());
    if let Some(existing) = load_async(sync, id).await? {
        if existing.intent_key != key {
            return Err(Error::local_kind(
                ErrorKind::Invalid,
                "namespace intent identity collision",
            ));
        }
        return Ok(id);
    }
    let mut planner = Planner::new(&snapshot, commit_id(id));
    let _ = planner.node(node).await?;
    planner.set_executable(node, executable).await?;
    let request = planner.finish()?;
    let operation = NamespaceOperation::SetExecutable { node, executable };
    insert(
        sync,
        id,
        key,
        baseline,
        commit_id(id),
        OperationWire::encode(&operation)?,
        &request,
    )
    .await?;
    Ok(id)
}

pub(super) async fn status(sync: &Sync, id: EditId) -> Result<Option<NamespaceStatus>> {
    let _gate = sync.inner.gate.lock().await;
    load_async(sync, id)
        .await?
        .map(|row| row.status())
        .transpose()
}

pub(super) async fn exists(sync: &Sync, id: EditId) -> Result<bool> {
    Ok(load_async(sync, id).await?.is_some())
}

pub(super) async fn statuses(sync: &Sync) -> Result<Vec<NamespaceStatus>> {
    let _gate = sync.inner.gate.lock().await;
    let rows = sync
        .call(|connection| {
            let mut statement = connection
                .prepare(
                    "SELECT id,intent_key,baseline,commit_id,request_digest,operation,request,
                            state,receipt_revision,receipt_ordinal,failure
                     FROM namespaces ORDER BY id",
                )
                .map_err(Error::local)?;
            let rows = statement
                .query_map([], row_from_sql)
                .map_err(Error::local)?;
            rows.collect::<rusqlite::Result<Vec<_>>>()
                .map_err(Error::local)
        })
        .await?;
    rows.iter().map(IntentRow::status).collect()
}

async fn receipt_for(sync: &Sync, intent: &NamespaceIntent) -> Result<Option<Receipt>> {
    let receipt = sync
        .authority()
        .observe_latest()
        .await?
        .receipt(intent.commit_id)
        .await?;
    if let Some(receipt) = &receipt
        && receipt.request_digest != intent.request_digest
    {
        return Err(Error::local_kind(
            ErrorKind::Corrupt,
            "commit identity returned a different request",
        ));
    }
    Ok(receipt)
}

pub(super) async fn publish(sync: &Sync, id: EditId) -> Result<Revision> {
    let row = load_async(sync, id)
        .await?
        .ok_or_else(|| Error::local_kind(ErrorKind::NotFound, "unknown namespace intent"))?;
    let status = row.status()?;
    let state = status.state.clone();
    match &state {
        NamespaceState::Committed(receipt) => return Ok(receipt.cursor.revision),
        NamespaceState::Conflict | NamespaceState::Failed => {
            return Err(Error::Retained(status.failure.ok_or_else(|| {
                Error::local_kind(ErrorKind::Corrupt, "terminal intent has no failure")
            })?));
        }
        NamespaceState::Prepared | NamespaceState::Unknown | NamespaceState::Retryable => {}
    }
    if matches!(&state, NamespaceState::Unknown) {
        match receipt_for(sync, &status.intent).await {
            Ok(Some(receipt)) => {
                let revision = receipt.cursor.revision;
                save_state_async(sync, id, COMMITTED, Some(receipt), None).await?;
                return Ok(revision);
            }
            Ok(None) => {}
            Err(error) => {
                let failure = Failure::capture(&error);
                save_state_async(sync, id, UNKNOWN, None, Some(failure.clone())).await?;
                return Err(Error::Retained(failure));
            }
        }
    }
    let request = match sync
        .authority()
        .restore_transaction(&row.request)
        .await
        .map_err(Error::from)
    {
        Ok(request) => request,
        Err(error) => {
            let state = match state {
                NamespaceState::Unknown => UNKNOWN,
                NamespaceState::Prepared | NamespaceState::Retryable => match error.kind() {
                    ErrorKind::Invalid | ErrorKind::Corrupt | ErrorKind::Unsupported => FAILED,
                    _ => RETRYABLE,
                },
                _ => unreachable!("terminal states returned above"),
            };
            let failure = Failure::capture(&error);
            save_state_async(sync, id, state, None, Some(failure.clone())).await?;
            return Err(Error::Retained(failure));
        }
    };
    // The original request is durable. Recording the possibly-dispatched state
    // before dispatch means a process crash cannot turn this into a safe abort.
    save_state_async(sync, id, UNKNOWN, None, None).await?;
    match sync.authority().commit(&request).await.map_err(Error::from) {
        Ok(yinyang_core::CommitOutcome::Committed(receipt)) => {
            save_state_async(sync, id, COMMITTED, Some(receipt.clone()), None).await?;
            Ok(receipt.cursor.revision)
        }
        Ok(yinyang_core::CommitOutcome::Conflict) => {
            let failure = Failure::capture(&Error::State(
                ErrorKind::Conflict,
                "namespace predicates changed; the original intent is retained",
            ));
            save_state_async(sync, id, CONFLICT, None, Some(failure.clone())).await?;
            Err(Error::Retained(failure))
        }
        Ok(yinyang_core::CommitOutcome::Retryable) => {
            let failure = Failure::capture(&Error::Retryable);
            save_state_async(sync, id, RETRYABLE, None, Some(failure.clone())).await?;
            Err(Error::Retained(failure))
        }
        Ok(yinyang_core::CommitOutcome::Unknown(commit)) => {
            let failure = Failure::capture(&Error::Unknown(commit));
            save_state_async(sync, id, UNKNOWN, None, Some(failure.clone())).await?;
            Err(Error::Retained(failure))
        }
        Err(error) => {
            let (state, failure) = match error.kind() {
                ErrorKind::Retryable => (RETRYABLE, Error::Retryable),
                ErrorKind::Unknown => (UNKNOWN, error),
                ErrorKind::Conflict => (CONFLICT, Error::Conflict),
                _ => (FAILED, error),
            };
            let failure = Failure::capture(&failure);
            save_state_async(sync, id, state, None, Some(failure.clone())).await?;
            Err(Error::Retained(failure))
        }
    }
}

pub(super) async fn resolve(sync: &Sync, id: EditId) -> Result<Option<Receipt>> {
    let _gate = sync.inner.gate.lock().await;
    let row = load_async(sync, id)
        .await?
        .ok_or_else(|| Error::local_kind(ErrorKind::NotFound, "unknown namespace intent"))?;
    let status = row.status()?;
    if let NamespaceState::Committed(receipt) = status.state {
        return Ok(Some(receipt));
    }
    let receipt = receipt_for(sync, &status.intent).await?;
    if let Some(receipt) = &receipt {
        save_state_async(sync, id, COMMITTED, Some(receipt.clone()), None).await?;
    }
    Ok(receipt)
}

pub(super) async fn read(
    sync: &Sync,
    id: EditId,
    offset: u64,
    length: usize,
) -> Result<Option<Vec<u8>>> {
    let Some(row) = load_async(sync, id).await? else {
        return Ok(None);
    };
    let status = row.status()?;
    let NamespaceOperation::CreateFile { content, .. } = status.intent.operation else {
        return Err(Error::State(
            ErrorKind::Unsupported,
            "namespace intent does not retain file bytes",
        ));
    };
    let size = content.content_id().length();
    let start = offset.min(size);
    let end = offset.saturating_add(length as u64).min(size);
    let mut bytes = Vec::new();
    sync.authority()
        .data()
        .read_range(&content, Range { start, end }, &mut bytes)
        .await?;
    Ok(Some(bytes))
}
