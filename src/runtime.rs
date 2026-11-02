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

//! Managed file handles with explicit local barriers and conditional remote fsync.
//! No OS mount, background synchronization, or full POSIX behavior is implied.
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex as AsyncMutex;
use yinyang_core::{
    Authority, CommitId, CommitOutcome, ContentDescriptor, ContentEdit, FileVersion, NodeId,
    Planner, PreparedContent, Receipt, Revision, Snapshot,
};

mod staging;
use staging::{CHUNK, Record, Stage, StagedRead};

mod error;
pub use error::{Error, ErrorKind, Failure, Result};

// Leave room for the authenticated service request around the encoded edits.
const OVERWRITE_ENCODED_MAX: u64 = 16 * 1024 * 1024 - 2048;
const OVERWRITE_EDITS_MAX: usize = 4096;
const EDIT_WIRE_OVERHEAD: u64 = 24;

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
    /// Commit identity of the frozen request, available without network or
    /// recovery. Definitive Conflict/Retryable keeps its existing error; any
    /// other frozen record is an uncertain dispatch candidate.
    pub frozen_commit_id: Option<CommitId>,
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
struct HandleBase {
    expected: Option<Vec<u8>>,
    file: Option<FileVersion>,
}
/// One staging directory per active runtime. Different clients may have separate
/// staging directories against the same authority; generation checks arbitrate.
#[derive(Clone)]
pub struct Runtime {
    inner: Arc<Inner>,
}
impl Runtime {
    pub(crate) async fn validate_staging(
        staging: impl AsRef<Path>,
        filesystem: NodeId,
    ) -> Result<()> {
        Stage::validate(staging.as_ref().join("staging.db"), filesystem).await
    }

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
        let base = HandleBase {
            expected: self
                .inner
                .stage
                .base(r.id)
                .await?
                .map(|base| base.descriptor),
            file: None,
        };
        self.claim(r.id, base)
    }
    pub(crate) async fn discard_clean(&self, id: uuid::Uuid) -> Result<()> {
        let r = self.inner.stage.load(*id.as_bytes()).await?;
        if !r.ready || r.local != r.remote || r.plan.is_some() || r.conflict || r.error.is_some() {
            return Err(Error::State(
                ErrorKind::Busy,
                "handle is not a disposable clean observation",
            ));
        }
        self.inner.stage.remove(r.id).await
    }
    fn claim(&self, id: [u8; 16], base: HandleBase) -> Result<FileHandle> {
        if !self.inner.active.lock().map_err(Error::local)?.insert(id) {
            return Err(Error::State(ErrorKind::Busy, "handle already active"));
        }
        Ok(FileHandle {
            runtime: self.clone(),
            id,
            base: AsyncMutex::new(base),
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
        let r = Record {
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
        let descriptor = file.descriptor().to_bytes();
        self.inner
            .stage
            .bind_base(r.id, descriptor.clone(), r.length)
            .await?;
        self.claim(
            r.id,
            HandleBase {
                expected: Some(descriptor),
                file: Some(file),
            },
        )
    }
    /// Namespace operations publish explicit transactions. Unknown outcomes carry
    /// an identity and must be resolved through the authority's receipt API.
    pub async fn create_file(&self, parent: NodeId, name: &str) -> Result<Receipt> {
        self.create_file_with_node(parent, name, false)
            .await
            .map(|(_, receipt)| receipt)
    }
    /// Create a file and return the NodeId produced by the same committed plan.
    ///
    /// Adapters must use this identity rather than resolving the path again:
    /// a concurrent rename or replacement could bind that lookup to another node.
    /// The executable flag is the only persisted mode bit supported by the core.
    pub async fn create_file_with_node(
        &self,
        parent: NodeId,
        name: &str,
        executable: bool,
    ) -> Result<(NodeId, Receipt)> {
        self.require_write()?;
        let snapshot = self.authority().observe_latest().await?;
        let empty = self.authority().prepare(&mut tokio::io::empty()).await?;
        let mut plan = Planner::new(&snapshot, CommitId::generate());
        let node = plan.create_file(parent, name, empty, executable).await?;
        let receipt = outcome(self.authority().commit(&plan.finish()?).await?)?;
        Ok((node, receipt))
    }
    pub async fn create_directory(&self, parent: NodeId, name: &str) -> Result<Receipt> {
        self.create_directory_with_node(parent, name)
            .await
            .map(|(_, receipt)| receipt)
    }
    /// Create a directory and return the NodeId produced by the committed plan.
    pub async fn create_directory_with_node(
        &self,
        parent: NodeId,
        name: &str,
    ) -> Result<(NodeId, Receipt)> {
        self.require_write()?;
        let snapshot = self.authority().observe_latest().await?;
        let mut plan = Planner::new(&snapshot, CommitId::generate());
        let node = plan.create_directory(parent, name).await?;
        let receipt = outcome(self.authority().commit(&plan.finish()?).await?)?;
        Ok((node, receipt))
    }
    pub async fn rename(&self, node: NodeId, parent: NodeId, name: &str) -> Result<Receipt> {
        self.require_write()?;
        let snapshot = self.authority().observe_latest().await?;
        let mut plan = Planner::new(&snapshot, CommitId::generate());
        plan.rename(node, parent, name).await?;
        outcome(self.authority().commit(&plan.finish()?).await?)
    }
    /// Rename the entry at `(parent, name)`, resolving and guarding that entry
    /// in the same snapshot used for publication. Destination replacement is
    /// not allowed.
    pub async fn rename_path(
        &self,
        parent: NodeId,
        name: &str,
        new_parent: NodeId,
        new_name: &str,
    ) -> Result<(NodeId, Receipt)> {
        self.rename_path_with(parent, name, new_parent, new_name, false)
            .await
    }
    /// Atomically replace the destination of the entry resolved by `(parent, name)`.
    pub async fn rename_path_replace(
        &self,
        parent: NodeId,
        name: &str,
        new_parent: NodeId,
        new_name: &str,
    ) -> Result<(NodeId, Receipt)> {
        self.rename_path_with(parent, name, new_parent, new_name, true)
            .await
    }
    async fn rename_path_with(
        &self,
        parent: NodeId,
        name: &str,
        new_parent: NodeId,
        new_name: &str,
        replace: bool,
    ) -> Result<(NodeId, Receipt)> {
        self.require_write()?;
        let snapshot = self.authority().observe_latest().await?;
        let mut plan = Planner::new(&snapshot, CommitId::generate());
        let source = plan
            .lookup(parent, name)
            .await?
            .ok_or(Error::State(ErrorKind::NotFound, "source does not exist"))?
            .node_id;
        if replace {
            plan.rename_replace(source, new_parent, new_name).await?;
        } else {
            plan.rename(source, new_parent, new_name).await?;
        }
        let receipt = outcome(self.authority().commit(&plan.finish()?).await?)?;
        Ok((source, receipt))
    }
    pub async fn unlink(&self, node: NodeId) -> Result<Receipt> {
        self.require_write()?;
        let snapshot = self.authority().observe_latest().await?;
        let mut plan = Planner::new(&snapshot, CommitId::generate());
        plan.remove(node).await?;
        outcome(self.authority().commit(&plan.finish()?).await?)
    }
    /// Remove the file entry at `(parent, name)` using one guarded snapshot.
    pub async fn unlink_path(&self, parent: NodeId, name: &str) -> Result<(NodeId, Receipt)> {
        self.remove_path(parent, name, false).await
    }
    /// Remove the empty directory entry at `(parent, name)` using one guarded snapshot.
    pub async fn rmdir_path(&self, parent: NodeId, name: &str) -> Result<(NodeId, Receipt)> {
        self.remove_path(parent, name, true).await
    }
    async fn remove_path(
        &self,
        parent: NodeId,
        name: &str,
        directory: bool,
    ) -> Result<(NodeId, Receipt)> {
        self.require_write()?;
        let snapshot = self.authority().observe_latest().await?;
        let mut plan = Planner::new(&snapshot, CommitId::generate());
        let node = plan
            .lookup(parent, name)
            .await?
            .ok_or(Error::State(ErrorKind::NotFound, "entry does not exist"))?
            .node_id;
        let entry = plan
            .node(node)
            .await?
            .ok_or(Error::State(ErrorKind::NotFound, "entry does not exist"))?;
        match (directory, entry.is_directory()) {
            (false, true) => {
                return Err(Error::State(
                    ErrorKind::IsDirectory,
                    "unlink requires a file",
                ));
            }
            (true, false) => {
                return Err(Error::State(
                    ErrorKind::NotDirectory,
                    "rmdir requires a directory",
                ));
            }
            _ => {}
        }
        plan.remove(node).await?;
        let receipt = outcome(self.authority().commit(&plan.finish()?).await?)?;
        Ok((node, receipt))
    }
    /// Replace the observed destination in one conditional transaction. The
    /// replaced identity remains readable through existing pinned versions.
    pub async fn rename_replace(
        &self,
        node: NodeId,
        parent: NodeId,
        name: &str,
    ) -> Result<Receipt> {
        self.require_write()?;
        let snapshot = self.authority().observe_latest().await?;
        let mut plan = Planner::new(&snapshot, CommitId::generate());
        plan.rename_replace(node, parent, name).await?;
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
async fn read_staged(
    stage: &Stage,
    base: Option<&FileVersion>,
    id: [u8; 16],
    offset: u64,
    length: usize,
) -> Result<Vec<u8>> {
    let staged = stage.read(id, offset, length).await?;
    merge_staged_base(staged, base, offset).await
}
async fn merge_staged_base(
    staged: StagedRead,
    base: Option<&FileVersion>,
    offset: u64,
) -> Result<Vec<u8>> {
    let mut bytes = staged.bytes;
    if staged.base.is_empty() {
        return Ok(bytes);
    }
    let base = base.ok_or(Error::State(
        ErrorKind::Corrupt,
        "sparse staging has no retained base observation",
    ))?;
    for range in staged.base {
        let mut remote = Vec::new();
        base.read_range(range.clone(), &mut remote).await?;
        if remote.len() as u64 != range.end - range.start {
            return Err(Error::State(
                ErrorKind::Corrupt,
                "retained base returned an incomplete staged range",
            ));
        }
        let start = (range.start - offset) as usize;
        let end = (range.end - offset) as usize;
        bytes[start..end].copy_from_slice(&remote);
    }
    Ok(bytes)
}

/// Not cloneable: one caller owns each handle's mutation/publication lifecycle.
/// Dropping a handle releases only its in-process lease, never publishes or
/// discards staged writes. Recover by its stable UUID.
pub struct FileHandle {
    runtime: Runtime,
    id: [u8; 16],
    base: AsyncMutex<HandleBase>,
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
    async fn base_file(&self) -> Result<Option<FileVersion>> {
        let mut base = self.base.lock().await;
        if let Some(file) = &base.file {
            return Ok(Some(file.clone()));
        }
        let Some(expected) = base.expected.clone() else {
            return Ok(None);
        };
        let record = self.runtime.inner.stage.load(self.id).await?;
        let stored = self
            .runtime
            .inner
            .stage
            .base(self.id)
            .await?
            .ok_or(Error::State(
                ErrorKind::Corrupt,
                "sparse staging has no base descriptor",
            ))?;
        if stored.descriptor != expected {
            return Err(Error::State(
                ErrorKind::Corrupt,
                "staging base descriptor changed during recovery",
            ));
        }
        let file = self
            .runtime
            .authority()
            .observe_revision(Revision::from_bytes(record.base))
            .await?
            .open_file(NodeId::from_bytes(record.node))
            .await?;
        if file.descriptor().to_bytes() != expected {
            return Err(Error::State(
                ErrorKind::Corrupt,
                "staging base does not match the retained published version",
            ));
        }
        base.file = Some(file.clone());
        Ok(Some(file))
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
        let result = async {
            let staged = self
                .runtime
                .inner
                .stage
                .read(self.id, offset, length)
                .await?;
            if staged.base.is_empty() {
                return Ok(staged.bytes);
            }
            let base = self.base_file().await?;
            merge_staged_base(staged, base.as_ref(), offset).await
        }
        .await;
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
    /// A Sync replacement is a conditional edit even when both versions are empty.
    pub(crate) async fn reset_content(&mut self) -> Result<()> {
        self.live()?;
        self.runtime.require_write()?;
        let result = self.runtime.inner.stage.reset_content(self.id).await;
        self.report(result).await
    }
    /// Publish dirty content, commit the executable flag, and advance this clean
    /// handle to the new metadata revision without restaging the unchanged bytes.
    pub(crate) async fn set_executable(&mut self, executable: bool) -> Result<Receipt> {
        self.live()?;
        let result = async {
            self.runtime.require_write()?;
            self.publish().await?;
            let record = self.runtime.inner.stage.load(self.id).await?;
            let node = NodeId::from_bytes(record.node);
            let snapshot = self
                .runtime
                .authority()
                .observe_revision(Revision::from_bytes(record.base))
                .await?;
            let mut plan = Planner::new(&snapshot, CommitId::generate());
            plan.set_executable(node, executable).await?;
            let receipt = outcome(self.runtime.authority().commit(&plan.finish()?).await?)?;
            self.advance_clean_base(receipt.cursor.revision).await?;
            Ok(receipt)
        }
        .await;
        self.report(result).await
    }
    async fn advance_clean_base(&mut self, revision: Revision) -> Result<()> {
        self.live()?;
        let stage = &self.runtime.inner.stage;
        let mut record = stage.load(self.id).await?;
        if record.local != record.remote
            || record.plan.is_some()
            || record.conflict
            || record.error.is_some()
        {
            return Err(Error::State(
                ErrorKind::Busy,
                "cannot advance a dirty or failed handle base",
            ));
        }
        record.base = revision.to_bytes();
        stage.save_durable(record).await
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
    /// Make every staged byte and local metadata change durable without
    /// publishing to the authority.
    pub async fn sync_local(&self) -> Result<HandleStatus> {
        self.runtime.inner.stage.barrier().await?;
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
        self.publish_and_report().await
    }
    pub async fn commit(&mut self) -> Result<Option<Receipt>> {
        self.publish_and_report().await
    }
    pub async fn fsync(&mut self) -> Result<Option<Receipt>> {
        self.publish_and_report().await
    }
    async fn publish_and_report(&mut self) -> Result<Option<Receipt>> {
        self.live()?;
        let result = self.publish().await;
        self.report(result).await
    }
    /// Choose the bounded update path from durable local state before asking
    /// the authority to prepare anything. An error from either trusted update
    /// preparation is returned to the caller and never retried as a different
    /// streaming request.
    async fn prepare_publication(&self, r: &Record) -> Result<PreparedContent> {
        self.prepare_publication_uncached(r).await
    }

    async fn prepare_publication_uncached(&self, r: &Record) -> Result<PreparedContent> {
        let stage = &self.runtime.inner.stage;
        let base = self.base_file().await?;
        if let Some(base) = &base
            && let Some(stored) = stage.base(self.id).await?
        {
            let base_length = base.descriptor().content_id().length();
            if stored.prefix > base_length || stored.prefix > r.length {
                return Err(Error::State(
                    ErrorKind::Corrupt,
                    "staged base prefix exceeds file or base length",
                ));
            }
            let ranges = stage.segment_ranges(self.id).await?;
            let mut ends = Vec::with_capacity(ranges.len());
            let mut encoded = 0_u64;
            for (position, length) in &ranges {
                let end = position.checked_add(*length).ok_or(Error::State(
                    ErrorKind::Corrupt,
                    "staged segment length overflows",
                ))?;
                if *length == 0 || end > r.length {
                    return Err(Error::State(
                        ErrorKind::Corrupt,
                        "staged segment exceeds file length",
                    ));
                }
                ends.push(end);
                encoded = encoded
                    .checked_add(*length + EDIT_WIRE_OVERHEAD)
                    .ok_or(Error::State(
                        ErrorKind::TooLarge,
                        "staged edit size overflows",
                    ))?;
            }
            if ranges.len() <= OVERWRITE_EDITS_MAX && encoded <= OVERWRITE_ENCODED_MAX {
                let runs = stage.segments(self.id).await?;
                if runs.len() != ranges.len()
                    || runs.iter().zip(&ranges).any(
                        |((position, bytes), (expected_position, expected_length))| {
                            position != expected_position || bytes.len() as u64 != *expected_length
                        },
                    )
                {
                    return Err(Error::State(
                        ErrorKind::Corrupt,
                        "staged segments changed while preparing publication",
                    ));
                }
                let edits = runs
                    .iter()
                    .zip(&ends)
                    .map(|((position, bytes), end)| ContentEdit::new(*position..*end, bytes))
                    .collect::<yinyang_core::Result<Vec<_>>>()?;
                if r.length == base_length && stored.prefix == r.length {
                    if !edits.is_empty() {
                        return self
                            .runtime
                            .authority()
                            .prepare_overwrite(base, &edits)
                            .await
                            .map_err(Error::from);
                    }
                } else if yinyang_core::data::update_is_bounded(stored.prefix, r.length, &edits) {
                    return self
                        .runtime
                        .authority()
                        .prepare_update(base, stored.prefix, r.length, &edits)
                        .await
                        .map_err(Error::from);
                }
            }
        }

        let (mut source, mut sink) = tokio::io::duplex(CHUNK as usize);
        let staged = stage.clone();
        let (id, length) = (self.id, r.length);
        let producer = tokio::spawn(async move {
            let mut offset = 0;
            while offset < length {
                let bytes = read_staged(&staged, base.as_ref(), id, offset, CHUNK as usize).await?;
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
        Ok(prepared)
    }

    async fn publish(&mut self) -> Result<Option<Receipt>> {
        let stage = &self.runtime.inner.stage;
        stage.barrier().await?;
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
            self.runtime
                .inner
                .authority
                .restore_transaction(bytes)
                .await
                .map_err(Error::from)?
        } else {
            let prepared = self.prepare_publication(&r).await?;
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
            stage.save_durable(r.clone()).await?;
            request
        };
        match self.runtime.authority().commit(&request).await {
            Err(error) => {
                // The plan is already frozen and dispatch may still publish.
                // A generic response or transport error cannot be a definitive
                // failure; identity and digest stay available on the handle.
                Err(Error::Retained(Failure::unknown_with(request.id(), error)))
            }
            Ok(outcome) => match outcome {
                CommitOutcome::Committed(receipt) => {
                    let node = NodeId::from_bytes(r.node);
                    let base = self
                        .runtime
                        .authority()
                        .observe_revision(receipt.cursor.revision)
                        .await?
                        .open_file(node)
                        .await?;
                    if base.descriptor().content_id().length() != r.length {
                        return Err(Error::State(
                            ErrorKind::Corrupt,
                            "published base length does not match committed content",
                        ));
                    }
                    r.base = receipt.cursor.revision.to_bytes();
                    r.remote = r.local;
                    r.plan = None;
                    r.error = None;
                    r.failure = None;
                    r.conflict = false;
                    let descriptor = base.descriptor().to_bytes();
                    stage.complete_base(r, descriptor.clone()).await?;
                    *self.base.lock().await = HandleBase {
                        expected: Some(descriptor),
                        file: Some(base),
                    };
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
            },
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

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::future::BoxFuture;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    #[tokio::test]
    async fn write_is_committed_but_only_sync_local_barriers_it() {
        let backend = crate::support::TestBackend::default();
        let authority = Arc::new(yinyang_core::Fs::create(backend.operator()).await.unwrap());
        let temp = tempfile::tempdir().unwrap();
        let staging = temp.path().join("staging");
        let runtime = Runtime::open(authority.clone(), &staging).await.unwrap();
        runtime.create_file(authority.root(), "file").await.unwrap();
        let mut handle = runtime.open_file("file", true).await.unwrap();
        handle.write(0, b"durable-at-barrier").await.unwrap();
        assert_eq!(
            runtime.inner.stage.barrier_counter().load(Ordering::SeqCst),
            0
        );

        let connection = crate::local::store::open_read_only(&staging.join("staging.db")).unwrap();
        let data: Vec<u8> = connection
            .query_row(
                "SELECT data FROM segments WHERE handle=?1 AND position=0",
                rusqlite::params![handle.id().as_bytes()],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(data, b"durable-at-barrier");

        handle.sync_local().await.unwrap();
        assert_eq!(
            runtime.inner.stage.barrier_counter().load(Ordering::SeqCst),
            1
        );
    }

    #[derive(Debug)]
    struct CommitProbe {
        inner: Arc<dyn Authority>,
        staging: PathBuf,
        barrier: Mutex<Arc<AtomicUsize>>,
        full_sync: Mutex<Arc<AtomicUsize>>,
        observed_barrier: AtomicUsize,
        observed_full_sync: AtomicUsize,
        observed_plan: AtomicBool,
    }

    impl Authority for CommitProbe {
        fn data(&self) -> &yinyang_core::DataStore {
            self.inner.data()
        }

        fn filesystem(&self) -> NodeId {
            self.inner.filesystem()
        }

        fn root(&self) -> NodeId {
            self.inner.root()
        }

        fn observe_latest(&self) -> BoxFuture<'_, yinyang_core::Result<Snapshot>> {
            self.inner.observe_latest()
        }

        fn observe_revision(
            &self,
            revision: Revision,
        ) -> BoxFuture<'_, yinyang_core::Result<Snapshot>> {
            self.inner.observe_revision(revision)
        }

        fn commit_batch<'a>(
            &'a self,
            requests: &'a [yinyang_core::Transaction],
        ) -> BoxFuture<'a, yinyang_core::Result<Vec<yinyang_core::object::Outcome>>> {
            self.inner.commit_batch(requests)
        }

        fn commit<'a>(
            &'a self,
            _request: &'a yinyang_core::Transaction,
        ) -> BoxFuture<'a, yinyang_core::Result<CommitOutcome>> {
            Box::pin(async move {
                let barrier = self.barrier.lock().unwrap().clone();
                self.observed_barrier
                    .store(barrier.load(Ordering::SeqCst), Ordering::SeqCst);
                let full_sync = self.full_sync.lock().unwrap().clone();
                self.observed_full_sync
                    .store(full_sync.load(Ordering::SeqCst), Ordering::SeqCst);
                let connection = crate::local::store::open_read_only(
                    &self.staging.join("staging.db"),
                )
                .map_err(|error| {
                    yinyang_core::Error::new(
                        yinyang_core::ErrorKind::Storage,
                        "commit probe",
                        error.to_string(),
                    )
                })?;
                let record: Vec<u8> = connection
                    .query_row("SELECT record FROM handles LIMIT 1", [], |row| row.get(0))
                    .map_err(|error| {
                        yinyang_core::Error::new(
                            yinyang_core::ErrorKind::Storage,
                            "commit probe",
                            error.to_string(),
                        )
                    })?;
                let record: Record = borsh::from_slice(&record).map_err(|error| {
                    yinyang_core::Error::new(
                        yinyang_core::ErrorKind::Corrupt,
                        "commit probe",
                        error.to_string(),
                    )
                })?;
                self.observed_plan
                    .store(record.plan.is_some(), Ordering::SeqCst);
                Err(yinyang_core::Error::new(
                    yinyang_core::ErrorKind::Unsupported,
                    "commit probe",
                    "stop before dispatch",
                ))
            })
        }
    }

    #[tokio::test]
    async fn frozen_plan_is_durable_before_commit_dispatch() {
        let backend = crate::support::TestBackend::default();
        let inner = Arc::new(yinyang_core::Fs::create(backend.operator()).await.unwrap());
        let temp = tempfile::tempdir().unwrap();
        let staging = temp.path().join("staging");
        {
            let bootstrap = Runtime::open(inner.clone(), &staging).await.unwrap();
            bootstrap.create_file(inner.root(), "file").await.unwrap();
        }

        let probe = Arc::new(CommitProbe {
            inner: inner.clone(),
            staging: staging.clone(),
            barrier: Mutex::new(Arc::new(AtomicUsize::new(0))),
            full_sync: Mutex::new(Arc::new(AtomicUsize::new(0))),
            observed_barrier: AtomicUsize::new(0),
            observed_full_sync: AtomicUsize::new(0),
            observed_plan: AtomicBool::new(false),
        });
        let runtime = Runtime::open(probe.clone(), &staging).await.unwrap();
        *probe.barrier.lock().unwrap() = runtime.inner.stage.barrier_counter();
        *probe.full_sync.lock().unwrap() = runtime.inner.stage.full_sync_counter();
        let mut handle = runtime.open_file("file", true).await.unwrap();
        handle.write(0, b"frozen").await.unwrap();
        assert!(handle.fsync().await.is_err());
        assert_eq!(probe.observed_barrier.load(Ordering::SeqCst), 1);
        assert_eq!(probe.observed_full_sync.load(Ordering::SeqCst), 1);
        assert!(probe.observed_plan.load(Ordering::SeqCst));
        assert!(handle.status().await.unwrap().frozen);
    }
}
