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

//! Managed file handles with durable local writes and conditional remote fsync.
//! No OS mount, background synchronization, or full POSIX behavior is implied.
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use tokio::io::AsyncWriteExt;
use yinyang_core::{
    Authority, CommitId, CommitOutcome, NodeId, Planner, Receipt, Revision, Snapshot,
};

mod staging;
use staging::{CHUNK, Record, Stage};

mod error;
pub use error::{Error, ErrorKind, Failure, Result};

/// Local and remote positions are distinct; a local write is not a remote receipt.
#[derive(Clone, Debug)]
pub struct HandleStatus {
    pub id: uuid::Uuid,
    pub node: NodeId,
    pub length: u64,
    pub local_generation: u64,
    pub remote_generation: u64,
    pub remote_revision: Revision,
    pub pending: bool,
    pub frozen: bool,
    pub conflict: bool,
    pub error: Option<Failure>,
}
#[derive(Clone, Debug)]
pub struct WritebackError {
    pub sequence: u64,
    pub handle: uuid::Uuid,
    pub error: Failure,
    pub persisted: bool,
}
#[derive(Clone, Debug)]
pub struct RuntimeStatus {
    pub handles: Vec<HandleStatus>,
    pub errors: Vec<WritebackError>,
}
struct Inner {
    read_only: bool,
    authority: Arc<dyn Authority>,
    stage: Stage,
    active: Mutex<BTreeSet<[u8; 16]>>,
    volatile_errors: Mutex<BTreeMap<[u8; 16], Failure>>,
}
/// One staging directory per active runtime. Different clients may have separate
/// staging directories against the same authority; generation checks arbitrate.
#[derive(Clone)]
pub struct Runtime {
    inner: Arc<Inner>,
}
impl Runtime {
    pub async fn open(authority: Arc<dyn Authority>, staging: impl AsRef<Path>) -> Result<Self> {
        Self::open_mode(authority, staging, false).await
    }
    pub async fn open_read_only(
        authority: Arc<dyn Authority>,
        staging: impl AsRef<Path>,
    ) -> Result<Self> {
        Self::open_mode(authority, staging, true).await
    }
    async fn open_mode(
        authority: Arc<dyn Authority>,
        staging: impl AsRef<Path>,
        read_only: bool,
    ) -> Result<Self> {
        let stage = Stage::open(staging.as_ref().to_owned(), authority.filesystem()).await?;
        Ok(Self {
            inner: Arc::new(Inner {
                read_only,
                authority,
                stage,
                active: Mutex::new(BTreeSet::new()),
                volatile_errors: Mutex::new(BTreeMap::new()),
            }),
        })
    }
    pub fn authority(&self) -> &dyn Authority {
        self.inner.authority.as_ref()
    }
    fn require_write(&self) -> Result<()> {
        if self.inner.read_only {
            Err(Error::State(ErrorKind::ReadOnly, "volume is read-only"))
        } else {
            Ok(())
        }
    }
    /// Read persisted staging status without taking the runtime lease or needing the remote service.
    pub async fn inspect(staging: impl AsRef<Path>) -> Result<RuntimeStatus> {
        Stage::inspect(staging.as_ref().join("staging.db")).await
    }
    pub async fn status(&self) -> Result<RuntimeStatus> {
        let mut status = self.inner.stage.status().await?;
        if let Ok(errors) = self.inner.volatile_errors.lock() {
            for (id, message) in errors.iter() {
                status.errors.push(WritebackError {
                    sequence: 0,
                    handle: uuid::Uuid::from_bytes(*id),
                    error: message.clone(),
                    persisted: false,
                });
                if let Some(handle) = status.handles.iter_mut().find(|h| h.id.as_bytes() == id) {
                    handle.error = Some(message.clone());
                }
            }
        }
        Ok(status)
    }
    /// Errors remain in the volume ledger after recovery until explicitly acknowledged.
    pub async fn acknowledge_errors(&self, through: u64) -> Result<()> {
        self.inner.stage.acknowledge(through).await
    }
    pub async fn recoverable(&self) -> Result<Vec<HandleStatus>> {
        Ok(self
            .inner
            .stage
            .list()
            .await?
            .iter()
            .map(Record::status)
            .collect())
    }
    pub async fn recover(&self, id: uuid::Uuid) -> Result<FileHandle> {
        let r = self.inner.stage.load(*id.as_bytes()).await?;
        if !r.ready {
            return Err(Error::Invalid("incomplete open is not recoverable"));
        }
        self.claim(r.id)
    }
    fn claim(&self, id: [u8; 16]) -> Result<FileHandle> {
        if !self.inner.active.lock().map_err(Error::local)?.insert(id) {
            return Err(Error::State(ErrorKind::Busy, "handle already active"));
        }
        Ok(FileHandle {
            runtime: self.clone(),
            id,
            closed: false,
        })
    }
    /// Resolves a path and opens its node in the same observation. Subsequent
    /// opens observe latest; existing handles are never rebased.
    pub async fn open_file(&self, path: &str, writable: bool) -> Result<FileHandle> {
        if writable {
            self.require_write()?;
        }
        let snapshot = self.authority().observe_latest().await?;
        let node = snapshot
            .resolve(path)
            .await?
            .ok_or(Error::State(ErrorKind::NotFound, "file does not exist"))?;
        self.open_snapshot_node(snapshot, node.id(), writable).await
    }
    /// Opens a stable node identity at latest, independently of its current name.
    /// Use the authority's Snapshot for identity queries, lookup and enumeration.
    pub async fn open_node(&self, node: NodeId, writable: bool) -> Result<FileHandle> {
        if writable {
            self.require_write()?;
        }
        let snapshot = self.authority().observe_latest().await?;
        self.open_snapshot_node(snapshot, node, writable).await
    }
    /// Opens a node in a retained revision of this runtime's authority. A write
    /// from an old revision still checks its original predicates at publication.
    pub async fn open_node_at(
        &self,
        revision: Revision,
        node: NodeId,
        writable: bool,
    ) -> Result<FileHandle> {
        if writable {
            self.require_write()?;
        }
        let snapshot = self.authority().observe_revision(revision).await?;
        self.open_snapshot_node(snapshot, node, writable).await
    }
    async fn open_snapshot_node(
        &self,
        snapshot: Snapshot,
        node: NodeId,
        writable: bool,
    ) -> Result<FileHandle> {
        let file = snapshot.open_file(node).await?;
        if file.descriptor().content_id().length() > i64::MAX as u64 {
            return Err(Error::State(
                ErrorKind::TooLarge,
                "file exceeds staging length limit",
            ));
        }
        let mut r = Record {
            id: *uuid::Uuid::new_v4().as_bytes(),
            node: *node.as_bytes(),
            base: snapshot.revision().to_bytes(),
            length: file.descriptor().content_id().length(),
            local: 0,
            remote: 0,
            writable,
            ready: false,
            plan: None,
            conflict: false,
            error: None,
            failure: None,
        };
        self.inner.stage.save(r.clone()).await?;
        let mut offset = 0;
        while offset < r.length {
            let end = (offset + CHUNK).min(r.length);
            let mut bytes = Vec::new();
            file.read_range(offset..end, &mut bytes).await?;
            self.inner
                .stage
                .initial_chunk(r.id, offset / CHUNK, bytes)
                .await?;
            offset = end;
        }
        r.ready = true;
        self.inner.stage.save(r.clone()).await?;
        self.claim(r.id)
    }
    /// Namespace operations publish explicit transactions. Unknown outcomes carry
    /// an identity and must be resolved through the authority's receipt API.
    pub async fn create_file(&self, parent: NodeId, name: &str) -> Result<Receipt> {
        self.require_write()?;
        let snapshot = self.authority().observe_latest().await?;
        let empty = self.authority().prepare(&mut tokio::io::empty()).await?;
        let mut plan = Planner::new(&snapshot, CommitId::generate());
        plan.create_file(parent, name, empty, false).await?;
        outcome(self.authority().commit(&plan.finish()?).await?)
    }
    pub async fn create_directory(&self, parent: NodeId, name: &str) -> Result<Receipt> {
        self.require_write()?;
        let snapshot = self.authority().observe_latest().await?;
        let mut plan = Planner::new(&snapshot, CommitId::generate());
        plan.create_directory(parent, name).await?;
        outcome(self.authority().commit(&plan.finish()?).await?)
    }
    pub async fn rename(&self, node: NodeId, parent: NodeId, name: &str) -> Result<Receipt> {
        self.require_write()?;
        let snapshot = self.authority().observe_latest().await?;
        let mut plan = Planner::new(&snapshot, CommitId::generate());
        plan.rename(node, parent, name).await?;
        outcome(self.authority().commit(&plan.finish()?).await?)
    }
    pub async fn unlink(&self, node: NodeId) -> Result<Receipt> {
        self.require_write()?;
        let snapshot = self.authority().observe_latest().await?;
        let mut plan = Planner::new(&snapshot, CommitId::generate());
        plan.remove(node).await?;
        outcome(self.authority().commit(&plan.finish()?).await?)
    }
}
fn outcome(outcome: CommitOutcome) -> Result<Receipt> {
    match outcome {
        CommitOutcome::Committed(r) => Ok(r),
        CommitOutcome::Conflict => Err(Error::Conflict),
        CommitOutcome::Retryable => Err(Error::Retryable),
        CommitOutcome::Unknown(id) => Err(Error::Unknown(id)),
    }
}

/// Not cloneable: one caller owns each handle's mutation/publication lifecycle.
/// Dropping a handle releases only its in-process lease, never publishes or
/// discards staged writes. Recover by its stable UUID.
pub struct FileHandle {
    runtime: Runtime,
    id: [u8; 16],
    closed: bool,
}
impl Drop for FileHandle {
    fn drop(&mut self) {
        if let Ok(mut active) = self.runtime.inner.active.lock() {
            active.remove(&self.id);
        }
    }
}
impl FileHandle {
    pub fn id(&self) -> uuid::Uuid {
        uuid::Uuid::from_bytes(self.id)
    }
    fn live(&self) -> Result<()> {
        if self.closed {
            Err(Error::State(ErrorKind::Closed, "handle is closed"))
        } else {
            Ok(())
        }
    }
    pub async fn status(&self) -> Result<HandleStatus> {
        self.live()?;
        let mut status = self.runtime.inner.stage.load(self.id).await?.status();
        if let Ok(errors) = self.runtime.inner.volatile_errors.lock()
            && let Some(error) = errors.get(&self.id)
        {
            status.error = Some(error.clone());
        }
        Ok(status)
    }
    pub async fn read(&self, offset: u64, length: usize) -> Result<Vec<u8>> {
        self.live()?;
        let result = self.runtime.inner.stage.read(self.id, offset, length).await;
        self.report(result).await
    }
    pub async fn write(&mut self, offset: u64, bytes: &[u8]) -> Result<()> {
        self.live()?;
        let result = async {
            self.runtime.require_write()?;
            self.runtime
                .inner
                .stage
                .write(self.id, Some(offset), bytes.to_vec())
                .await
                .map(|_| ())
        }
        .await;
        self.report(result).await
    }
    /// Appends to this handle's private staged version; concurrent handles
    /// conflict at publication rather than silently merging their appends.
    pub async fn append(&mut self, bytes: &[u8]) -> Result<u64> {
        self.live()?;
        let result = async {
            self.runtime.require_write()?;
            self.runtime
                .inner
                .stage
                .write(self.id, None, bytes.to_vec())
                .await
        }
        .await;
        self.report(result).await
    }
    pub async fn truncate(&mut self, length: u64) -> Result<()> {
        self.live()?;
        let result = async {
            self.runtime.require_write()?;
            self.runtime.inner.stage.truncate(self.id, length).await
        }
        .await;
        self.report(result).await
    }
    async fn report<T>(&self, result: Result<T>) -> Result<T> {
        if let Err(error) = &result {
            // Keep the original failure even if the local disk cannot record it.
            if self
                .runtime
                .inner
                .stage
                .failure(self.id, Failure::capture(error))
                .await
                .is_err()
                && let Ok(mut errors) = self.runtime.inner.volatile_errors.lock()
            {
                errors.insert(self.id, Failure::capture(error));
            }
        }
        result
    }
    /// Every successful write already commits a synchronous local SQLite
    /// transaction. This reports its position without claiming remote durability.
    pub async fn sync_local(&self) -> Result<HandleStatus> {
        self.status().await
    }
    /// Explicitly acknowledge the handle's reported error, without changing its
    /// bytes, conflict, or frozen request. The volume ledger is acknowledged separately.
    pub async fn acknowledge_error(&mut self) -> Result<()> {
        self.live()?;
        let mut record = self.runtime.inner.stage.load(self.id).await?;
        record.error = None;
        record.failure = None;
        self.runtime.inner.stage.save(record).await?;
        if let Ok(mut errors) = self.runtime.inner.volatile_errors.lock() {
            errors.remove(&self.id);
        }
        Ok(())
    }
    pub async fn flush(&mut self) -> Result<Option<Receipt>> {
        self.fsync().await
    }
    pub async fn commit(&mut self) -> Result<Option<Receipt>> {
        self.fsync().await
    }
    pub async fn fsync(&mut self) -> Result<Option<Receipt>> {
        self.live()?;
        let result = self.publish().await;
        self.report(result).await
    }
    async fn publish(&mut self) -> Result<Option<Receipt>> {
        let stage = &self.runtime.inner.stage;
        let mut r = stage.load(self.id).await?;
        if r.local == r.remote {
            if let Some(error) = self.status().await?.error {
                return Err(Error::Retained(error));
            }
            return Ok(None);
        }
        self.runtime.require_write()?;
        if r.conflict {
            return Err(Error::Conflict);
        }
        let request = if let Some(bytes) = &r.plan {
            self.runtime.authority().restore_transaction(bytes).await?
        } else {
            let (mut source, mut sink) = tokio::io::duplex(CHUNK as usize);
            let staged = stage.clone();
            let (id, length) = (self.id, r.length);
            let producer = tokio::spawn(async move {
                let mut offset = 0;
                while offset < length {
                    let bytes = staged.read(id, offset, CHUNK as usize).await?;
                    if bytes.is_empty() {
                        return Err(Error::Invalid("staged data ended early"));
                    }
                    sink.write_all(&bytes).await.map_err(Error::local)?;
                    offset += bytes.len() as u64;
                }
                Ok::<_, Error>(())
            });
            let prepared = self.runtime.authority().prepare(&mut source).await;
            drop(source);
            let produced = producer.await.map_err(Error::local)?;
            let prepared = prepared?;
            produced?;
            let snapshot = self
                .runtime
                .authority()
                .observe_revision(Revision::from_bytes(r.base))
                .await?;
            let mut plan = Planner::new(&snapshot, CommitId::generate());
            plan.set_content(NodeId::from_bytes(r.node), prepared)
                .await?;
            let request = plan.finish()?;
            r.plan = Some(request.to_bytes()?);
            // Persist before dispatch: cancellation can never lose request identity.
            stage.save(r.clone()).await?;
            request
        };
        match self.runtime.authority().commit(&request).await? {
            CommitOutcome::Committed(receipt) => {
                r.base = receipt.cursor.revision.to_bytes();
                r.remote = r.local;
                r.plan = None;
                r.error = None;
                r.failure = None;
                r.conflict = false;
                stage.save(r).await?;
                if let Ok(mut errors) = self.runtime.inner.volatile_errors.lock() {
                    errors.remove(&self.id);
                }
                Ok(Some(receipt))
            }
            CommitOutcome::Conflict => {
                r.conflict = true;
                stage.save(r).await?;
                Err(Error::Conflict)
            }
            CommitOutcome::Retryable => Err(Error::Retryable),
            CommitOutcome::Unknown(id) => Err(Error::Unknown(id)),
        }
    }
    /// Abort only a request that was never dispatched or is definitively
    /// conflicted. Receipt absence cannot authorize abandoning a frozen request.
    pub async fn abort(&mut self) -> Result<()> {
        self.live()?;
        let r = self.runtime.inner.stage.load(self.id).await?;
        if r.plan.is_some() && !r.conflict {
            return Err(Error::State(
                ErrorKind::Frozen,
                "resolve the frozen request before aborting",
            ));
        }
        let result = self.runtime.inner.stage.remove(self.id).await;
        self.report(result).await?;
        self.closed = true;
        Ok(())
    }
    /// Release this caller's lease without publishing or deleting staging.
    /// Returns the identity for recovery, including pending, frozen or failed
    /// handles. Errors and data remain inspectable after the reference is gone.
    pub fn release(self) -> Result<uuid::Uuid> {
        self.live()?;
        Ok(self.id())
    }
    /// Publish before deleting staging. Failure leaves the handle open and
    /// recoverable; release or drop never retries or discards the failed data.
    pub async fn close(&mut self) -> Result<()> {
        self.live()?;
        self.fsync().await?;
        let result = self.runtime.inner.stage.remove(self.id).await;
        self.report(result).await?;
        self.closed = true;
        Ok(())
    }
}
