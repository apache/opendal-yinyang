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

#[path = "../crates/core/tests/support/mod.rs"]
mod support;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::{future::Future, pin::Pin};
use support::TestBackend;
use tokio::sync::Semaphore;
use yinyang::core::service::{MetadataService, ServiceClient, serve};
use yinyang::core::{
    Authority, BackendProfile, CommitId, CommitOutcome, ContentEdit, DataStore, FileVersion, Fs,
    NodeId, Planner, PreparedContent, Revision, Snapshot, Transaction,
};
use yinyang::runtime::{Error, ErrorKind, Runtime};

#[derive(Default)]
struct PrepareCalls {
    stream: AtomicUsize,
    overwrite: AtomicUsize,
    update: AtomicUsize,
    latest: AtomicUsize,
    revision: AtomicUsize,
}

impl PrepareCalls {
    fn reset(&self) {
        self.stream.store(0, Ordering::SeqCst);
        self.overwrite.store(0, Ordering::SeqCst);
        self.update.store(0, Ordering::SeqCst);
        self.latest.store(0, Ordering::SeqCst);
        self.revision.store(0, Ordering::SeqCst);
    }
}

struct RecordingAuthority {
    inner: Arc<dyn Authority>,
    calls: Arc<PrepareCalls>,
}

impl RecordingAuthority {
    fn new(inner: Arc<dyn Authority>, calls: Arc<PrepareCalls>) -> Self {
        Self { inner, calls }
    }
}

impl std::fmt::Debug for RecordingAuthority {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RecordingAuthority").finish_non_exhaustive()
    }
}

impl Authority for RecordingAuthority {
    fn data(&self) -> &DataStore {
        self.inner.data()
    }

    fn filesystem(&self) -> NodeId {
        self.inner.filesystem()
    }

    fn root(&self) -> NodeId {
        self.inner.root()
    }

    fn observe_latest(
        &self,
    ) -> Pin<Box<dyn Future<Output = yinyang::core::Result<Snapshot>> + Send + '_>> {
        self.calls.latest.fetch_add(1, Ordering::SeqCst);
        self.inner.observe_latest()
    }

    fn observe_revision(
        &self,
        revision: Revision,
    ) -> Pin<Box<dyn Future<Output = yinyang::core::Result<Snapshot>> + Send + '_>> {
        self.calls.revision.fetch_add(1, Ordering::SeqCst);
        self.inner.observe_revision(revision)
    }

    fn retained_revisions(
        &self,
    ) -> Pin<Box<dyn Future<Output = yinyang::core::Result<Vec<Revision>>> + Send + '_>> {
        self.inner.retained_revisions()
    }

    fn commit_batch<'a>(
        &'a self,
        requests: &'a [Transaction],
    ) -> Pin<Box<dyn Future<Output = yinyang::core::Result<Vec<CommitOutcome>>> + Send + 'a>> {
        self.inner.commit_batch(requests)
    }

    fn prepare<'a>(
        &'a self,
        source: &'a mut (dyn tokio::io::AsyncRead + Unpin + Send),
    ) -> Pin<Box<dyn Future<Output = yinyang::core::Result<PreparedContent>> + Send + 'a>> {
        self.calls.stream.fetch_add(1, Ordering::SeqCst);
        self.inner.prepare(source)
    }

    fn prepare_overwrite<'a>(
        &'a self,
        base: &'a FileVersion,
        edits: &'a [ContentEdit<'a>],
    ) -> Pin<Box<dyn Future<Output = yinyang::core::Result<PreparedContent>> + Send + 'a>> {
        self.calls.overwrite.fetch_add(1, Ordering::SeqCst);
        self.inner.prepare_overwrite(base, edits)
    }

    fn prepare_update<'a>(
        &'a self,
        base: &'a FileVersion,
        preserved_prefix: u64,
        final_length: u64,
        edits: &'a [ContentEdit<'a>],
    ) -> Pin<Box<dyn Future<Output = yinyang::core::Result<PreparedContent>> + Send + 'a>> {
        self.calls.update.fetch_add(1, Ordering::SeqCst);
        self.inner
            .prepare_update(base, preserved_prefix, final_length, edits)
    }

    fn restore_transaction<'a>(
        &'a self,
        bytes: &'a [u8],
    ) -> Pin<Box<dyn Future<Output = yinyang::core::Result<Transaction>> + Send + 'a>> {
        self.inner.restore_transaction(bytes)
    }
}

fn reset_backend_counters(backend: &TestBackend) {
    let mut state = backend.state.lock().unwrap();
    state.data_read_bytes = 0;
    state.data_reads = 0;
    state.written_bytes = 0;
}

fn backend_counters(backend: &TestBackend) -> (u64, u64) {
    let state = backend.state.lock().unwrap();
    (state.data_read_bytes, state.written_bytes)
}

struct PausingAuthority {
    inner: Arc<dyn Authority>,
    entered: Semaphore,
    release: Semaphore,
    pause_next_commit: AtomicBool,
}
impl PausingAuthority {
    fn new(inner: Arc<dyn Authority>) -> Self {
        Self {
            inner,
            entered: Semaphore::new(0),
            release: Semaphore::new(0),
            pause_next_commit: AtomicBool::new(false),
        }
    }
    fn pause_next_commit(&self) {
        self.pause_next_commit.store(true, Ordering::SeqCst);
    }
}
impl std::fmt::Debug for PausingAuthority {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PausingAuthority").finish_non_exhaustive()
    }
}
impl Authority for PausingAuthority {
    fn data(&self) -> &DataStore {
        self.inner.data()
    }
    fn filesystem(&self) -> NodeId {
        self.inner.filesystem()
    }
    fn root(&self) -> NodeId {
        self.inner.root()
    }
    fn observe_latest(
        &self,
    ) -> Pin<Box<dyn Future<Output = yinyang::core::Result<Snapshot>> + Send + '_>> {
        self.inner.observe_latest()
    }
    fn observe_revision(
        &self,
        revision: Revision,
    ) -> Pin<Box<dyn Future<Output = yinyang::core::Result<Snapshot>> + Send + '_>> {
        self.inner.observe_revision(revision)
    }
    fn commit_batch<'a>(
        &'a self,
        requests: &'a [Transaction],
    ) -> Pin<Box<dyn Future<Output = yinyang::core::Result<Vec<CommitOutcome>>> + Send + 'a>> {
        Box::pin(async move {
            if self.pause_next_commit.swap(false, Ordering::SeqCst) {
                self.entered.add_permits(1);
                self.release.acquire().await.unwrap().forget();
            }
            self.inner.commit_batch(requests).await
        })
    }
}

struct FailingRevisionAuthority {
    inner: Arc<dyn Authority>,
    fail_revision: AtomicBool,
}
impl FailingRevisionAuthority {
    fn new(inner: Arc<dyn Authority>) -> Self {
        Self {
            inner,
            fail_revision: AtomicBool::new(false),
        }
    }
}
impl std::fmt::Debug for FailingRevisionAuthority {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FailingRevisionAuthority")
            .finish_non_exhaustive()
    }
}
impl Authority for FailingRevisionAuthority {
    fn data(&self) -> &DataStore {
        self.inner.data()
    }
    fn filesystem(&self) -> NodeId {
        self.inner.filesystem()
    }
    fn root(&self) -> NodeId {
        self.inner.root()
    }
    fn observe_latest(
        &self,
    ) -> Pin<Box<dyn Future<Output = yinyang::core::Result<Snapshot>> + Send + '_>> {
        self.inner.observe_latest()
    }
    fn observe_revision(
        &self,
        revision: Revision,
    ) -> Pin<Box<dyn Future<Output = yinyang::core::Result<Snapshot>> + Send + '_>> {
        if self.fail_revision.load(Ordering::SeqCst) {
            Box::pin(async {
                Err(yinyang::core::Error::new(
                    yinyang::core::ErrorKind::Storage,
                    "observe revision",
                    "injected lazy base authentication failure",
                ))
            })
        } else {
            self.inner.observe_revision(revision)
        }
    }
    fn commit_batch<'a>(
        &'a self,
        requests: &'a [Transaction],
    ) -> Pin<Box<dyn Future<Output = yinyang::core::Result<Vec<CommitOutcome>>> + Send + 'a>> {
        self.inner.commit_batch(requests)
    }
}

async fn exercise(
    authority: Arc<dyn Authority>,
    path: &std::path::Path,
    backend: &TestBackend,
    object_head: bool,
) {
    let runtime = Runtime::open(authority.clone(), path).await.unwrap();
    runtime.create_file(authority.root(), "file").await.unwrap();
    let mut first = runtime.open_file("file", true).await.unwrap();
    let mut stale = runtime.open_file("file", true).await.unwrap();
    first.write(3, b"abc").await.unwrap();
    assert_eq!(first.append(b"def").await.unwrap(), 6);
    assert_eq!(first.read(0, 20).await.unwrap(), b"\0\0\0abcdef");
    first.truncate(5).await.unwrap();
    first.truncate(8).await.unwrap();
    assert_eq!(first.read(0, 20).await.unwrap(), b"\0\0\0ab\0\0\0");
    let before = first.sync_local().await.unwrap();
    assert!(before.pending);
    assert_eq!(before.remote_generation, 0);
    let receipt = first.fsync().await.unwrap().unwrap();
    assert_eq!(
        first.status().await.unwrap().remote_revision,
        receipt.cursor.revision
    );
    assert!(!first.status().await.unwrap().pending);
    assert!(stale.read(0, 100).await.unwrap().is_empty());
    stale.write(0, b"stale").await.unwrap();
    assert!(matches!(stale.fsync().await, Err(Error::Conflict)));
    assert!(matches!(stale.close().await, Err(Error::Conflict)));
    assert_eq!(stale.read(0, 20).await.unwrap(), b"stale");
    stale.abort().await.unwrap();

    let node = first.status().await.unwrap().node;
    runtime
        .rename(node, authority.root(), "renamed")
        .await
        .unwrap();
    first.write(0, b"new").await.unwrap();
    first.flush().await.unwrap();
    let mut reopened = runtime.open_file("renamed", false).await.unwrap();
    assert_eq!(reopened.read(0, 3).await.unwrap(), b"new");
    runtime.unlink(node).await.unwrap();
    assert_eq!(reopened.read(0, 3).await.unwrap(), b"new");
    reopened.close().await.unwrap();
    first.append(b"after unlink").await.unwrap();
    assert!(matches!(first.commit().await, Err(Error::Conflict)));
    assert!(
        authority
            .observe_latest()
            .await
            .unwrap()
            .resolve("renamed")
            .await
            .unwrap()
            .is_none()
    );
    first.abort().await.unwrap();
    assert!(!runtime.status().await.unwrap().errors.is_empty());
    let sequence = runtime
        .status()
        .await
        .unwrap()
        .errors
        .last()
        .unwrap()
        .sequence;
    runtime.acknowledge_errors(sequence).await.unwrap();
    assert!(runtime.status().await.unwrap().errors.is_empty());
    identity_access(&runtime).await;
    completion_and_errors(&runtime, backend).await;
    version_reads(&runtime, backend).await;
    replace_rename(&runtime).await;
    sparse_runtime(&runtime, backend, path, object_head).await;
}

async fn replace_rename(runtime: &Runtime) {
    let authority = runtime.authority();
    let root = authority.root();
    runtime.create_file(root, "safe-save").await.unwrap();
    runtime.create_file(root, "save-temp").await.unwrap();
    let mut old = runtime.open_file("safe-save", true).await.unwrap();
    old.write(0, b"old").await.unwrap();
    old.fsync().await.unwrap();
    let mut new = runtime.open_file("save-temp", true).await.unwrap();
    new.write(0, b"new").await.unwrap();
    new.fsync().await.unwrap();
    let source = new.status().await.unwrap().node;
    let victim = old.status().await.unwrap().node;
    assert_eq!(
        runtime
            .rename(source, root, "safe-save")
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::AlreadyExists
    );
    let snapshot = authority.observe_latest().await.unwrap();
    let pinned = snapshot.open_file(victim).await.unwrap();
    let mut plan = Planner::new(&snapshot, CommitId::generate());
    plan.rename_replace(source, root, "safe-save")
        .await
        .unwrap();
    let request = plan.finish().unwrap();
    let wire = request.to_bytes().unwrap();
    let restored = authority.restore_transaction(&wire).await.unwrap();
    assert_eq!(restored.digest(), request.digest());
    let outcome = authority.commit(&restored).await.unwrap();
    assert!(matches!(outcome, CommitOutcome::Committed(_)));
    assert_eq!(authority.commit(&restored).await.unwrap(), outcome);
    let latest = authority.observe_latest().await.unwrap();
    assert_eq!(
        latest.resolve("safe-save").await.unwrap().unwrap().id(),
        source
    );
    assert!(latest.resolve("save-temp").await.unwrap().is_none());
    assert!(latest.node(victim).await.unwrap().is_none());
    assert_eq!(pinned.read(0, 10).await.unwrap(), b"old");
    assert_eq!(old.read(0, 10).await.unwrap(), b"old");
    old.write(0, b"stale").await.unwrap();
    assert!(matches!(old.fsync().await, Err(Error::Conflict)));
    old.abort().await.unwrap();
    new.close().await.unwrap();
    // The convenience entry also supports case-only moves and absent targets.
    runtime
        .rename_replace(source, root, "SAFE-SAVE")
        .await
        .unwrap();
    runtime.rename_replace(source, root, "saved").await.unwrap();

    runtime.create_file(root, "next-temp").await.unwrap();
    let snapshot = authority.observe_latest().await.unwrap();
    let next = snapshot.resolve("next-temp").await.unwrap().unwrap().id();
    let mut replace = Planner::new(&snapshot, CommitId::generate());
    replace.rename_replace(next, root, "saved").await.unwrap();
    let mut concurrent = runtime.open_node(source, true).await.unwrap();
    concurrent.write(0, b"remote edit").await.unwrap();
    concurrent.close().await.unwrap();
    assert_eq!(
        authority.commit(&replace.finish().unwrap()).await.unwrap(),
        CommitOutcome::Conflict
    );
    assert_eq!(
        authority
            .observe_latest()
            .await
            .unwrap()
            .open_file(source)
            .await
            .unwrap()
            .read(0, 20)
            .await
            .unwrap(),
        b"remote edit"
    );

    runtime.create_directory(root, "replace-dir").await.unwrap();
    runtime.create_directory(root, "empty-dir").await.unwrap();
    let snapshot = authority.observe_latest().await.unwrap();
    let dir = snapshot.resolve("replace-dir").await.unwrap().unwrap().id();
    let empty = snapshot.resolve("empty-dir").await.unwrap().unwrap().id();
    assert_eq!(
        runtime
            .rename_replace(next, root, "empty-dir")
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::IsDirectory
    );
    assert_eq!(
        runtime
            .rename_replace(dir, root, "next-temp")
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::NotDirectory
    );
    let mut replace = Planner::new(&snapshot, CommitId::generate());
    replace
        .rename_replace(dir, root, "empty-dir")
        .await
        .unwrap();
    runtime.create_file(empty, "child").await.unwrap();
    assert_eq!(
        authority.commit(&replace.finish().unwrap()).await.unwrap(),
        CommitOutcome::Conflict
    );
    assert_eq!(
        runtime
            .rename_replace(dir, root, "empty-dir")
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::NotEmpty
    );
    let child = authority
        .observe_latest()
        .await
        .unwrap()
        .resolve("empty-dir/child")
        .await
        .unwrap()
        .unwrap()
        .id();
    runtime.unlink(child).await.unwrap();
    runtime
        .rename_replace(dir, root, "empty-dir")
        .await
        .unwrap();
    assert!(
        authority
            .observe_latest()
            .await
            .unwrap()
            .node(empty)
            .await
            .unwrap()
            .is_none()
    );

    // Cycle validation fails after removal in the tentative view. The caller
    // may still finish this planner: no accidental deletion may survive.
    runtime.create_directory(dir, "descendant").await.unwrap();
    let snapshot = authority.observe_latest().await.unwrap();
    let descendant = snapshot
        .resolve("empty-dir/descendant")
        .await
        .unwrap()
        .unwrap()
        .id();
    let mut plan = Planner::new(&snapshot, CommitId::generate());
    assert!(plan.rename_replace(dir, dir, "descendant").await.is_err());
    assert!(matches!(
        authority.commit(&plan.finish().unwrap()).await.unwrap(),
        CommitOutcome::Committed(_)
    ));
    let latest = authority.observe_latest().await.unwrap();
    assert!(latest.node(dir).await.unwrap().is_some());
    assert!(latest.node(descendant).await.unwrap().is_some());
}

async fn completion_and_errors(runtime: &Runtime, backend: &TestBackend) {
    let root = runtime.authority().root();
    assert_eq!(
        runtime
            .open_file("missing", false)
            .await
            .err()
            .unwrap()
            .kind(),
        ErrorKind::NotFound
    );
    assert_eq!(
        runtime.open_node(root, false).await.err().unwrap().kind(),
        ErrorKind::IsDirectory
    );
    assert_eq!(
        runtime.create_file(root, "con").await.unwrap_err().kind(),
        ErrorKind::InvalidName
    );
    runtime.create_file(root, "completion").await.unwrap();
    assert_eq!(
        runtime
            .create_file(root, "completion")
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::AlreadyExists
    );
    let mut file = runtime.open_file("completion", true).await.unwrap();
    let node = file.status().await.unwrap().node;
    assert_eq!(
        runtime.create_file(node, "child").await.unwrap_err().kind(),
        ErrorKind::NotDirectory
    );
    let snapshot = runtime.authority().observe_latest().await.unwrap();
    let dir = snapshot.resolve("dir").await.unwrap().unwrap().id();
    assert_eq!(
        runtime.unlink(dir).await.unwrap_err().kind(),
        ErrorKind::NotEmpty
    );
    assert_eq!(
        runtime.recover(file.id()).await.err().unwrap().kind(),
        ErrorKind::Busy
    );
    file.write(0, b"local only").await.unwrap();
    let local = file.sync_local().await.unwrap();
    assert!(local.pending);
    assert_eq!(local.remote_generation, 0);
    let writes = backend.state.lock().unwrap().written_bytes;
    let id = file.release().unwrap();
    assert_eq!(backend.state.lock().unwrap().written_bytes, writes);
    assert!(
        runtime
            .authority()
            .observe_latest()
            .await
            .unwrap()
            .open_file(node)
            .await
            .unwrap()
            .read(0, 20)
            .await
            .unwrap()
            .is_empty()
    );
    let mut file = runtime.recover(id).await.unwrap();
    assert_eq!(file.read(0, 20).await.unwrap(), b"local only");
    backend.state.lock().unwrap().fail_data_close = true;
    assert_eq!(file.close().await.unwrap_err().kind(), ErrorKind::Storage);
    let id = file.release().unwrap();
    let status = runtime.status().await.unwrap();
    let state = status.handles.iter().find(|s| s.id == id).unwrap();
    assert!(state.pending);
    assert_eq!(state.error.as_ref().unwrap().kind(), ErrorKind::Storage);
    assert!(
        status
            .errors
            .iter()
            .any(|e| e.handle == id && e.error.kind() == ErrorKind::Storage)
    );
    backend.state.lock().unwrap().fail_data_close = false;
    let mut file = runtime.recover(id).await.unwrap();
    file.fsync().await.unwrap();
    assert!(file.status().await.unwrap().error.is_none());
    file.close().await.unwrap();
    assert_eq!(file.read(0, 1).await.unwrap_err().kind(), ErrorKind::Closed);
    let mut read_only = runtime.open_node(node, false).await.unwrap();
    assert_eq!(
        read_only.write(0, b"no").await.unwrap_err().kind(),
        ErrorKind::ReadOnly
    );
    let id = read_only.release().unwrap();
    let mut read_only = runtime.recover(id).await.unwrap();
    assert_eq!(
        read_only.close().await.unwrap_err().kind(),
        ErrorKind::ReadOnly
    );
    read_only.acknowledge_error().await.unwrap();
    read_only.close().await.unwrap();
}

async fn version_reads(runtime: &Runtime, backend: &TestBackend) {
    let root = runtime.authority().root();
    let bytes: Vec<u8> = (0..16 * 65536).map(|i| (i % 251) as u8).collect();
    let snapshot = runtime.authority().observe_latest().await.unwrap();
    let prepared = runtime
        .authority()
        .prepare(&mut bytes.as_slice())
        .await
        .unwrap();
    let mut plan = Planner::new(&snapshot, CommitId::generate());
    let id = plan
        .create_file(root, "large", prepared, false)
        .await
        .unwrap();
    runtime
        .authority()
        .commit(&plan.finish().unwrap())
        .await
        .unwrap();
    let snapshot = runtime.authority().observe_latest().await.unwrap();
    let before = backend.state.lock().unwrap().data_read_bytes;
    let file = snapshot.open_file(id).await.unwrap();
    assert_eq!(file.node().id(), id);
    assert_eq!(file.revision(), snapshot.revision());
    // Metadata shares pack storage with content; opening may read metadata,
    // but must not read the 1 MiB payload or create a stage record.
    assert!(backend.state.lock().unwrap().data_read_bytes - before < 65536);
    assert!(runtime.recoverable().await.unwrap().is_empty());
    let before = backend.state.lock().unwrap().data_read_bytes;
    assert_eq!(file.read(31, 10).await.unwrap(), bytes[31..41]);
    let read_bytes = backend.state.lock().unwrap().data_read_bytes - before;
    assert!((65536..2 * 65536).contains(&read_bytes));
    let before = backend.state.lock().unwrap().data_read_bytes;
    assert!(file.read(u64::MAX, 100).await.unwrap().is_empty());
    assert!(file.read(31, 0).await.unwrap().is_empty());
    assert!(
        file.read_range(0..bytes.len() as u64 + 1, &mut Vec::new())
            .await
            .is_err()
    );
    assert_eq!(backend.state.lock().unwrap().data_read_bytes, before);
    let clone = file.clone();
    let (a, b) = tokio::join!(
        file.read(65530, 20),
        clone.read(bytes.len() as u64 - 4, 100)
    );
    assert_eq!(a.unwrap(), bytes[65530..65550]);
    assert_eq!(b.unwrap(), bytes[bytes.len() - 4..]);
    let mut writer = runtime.open_node(id, true).await.unwrap();
    writer.write(0, b"changed").await.unwrap();
    writer.close().await.unwrap();
    runtime.rename(id, root, "large-moved").await.unwrap();
    let latest = runtime
        .authority()
        .observe_latest()
        .await
        .unwrap()
        .open_file(id)
        .await
        .unwrap();
    assert_eq!(latest.read(0, 7).await.unwrap(), b"changed");
    runtime.unlink(id).await.unwrap();
    assert_eq!(file.read(0, 10).await.unwrap(), bytes[..10]);
    let retained = runtime
        .authority()
        .observe_revision(file.revision())
        .await
        .unwrap()
        .open_file(id)
        .await
        .unwrap();
    assert_eq!(retained.read(0, 10).await.unwrap(), bytes[..10]);
    // Corrupt the exact packed bytes read by this version. A warm authenticated
    // hit may return the original bytes, but an uncached read must reject the
    // mutated storage without releasing anything.
    {
        let mut state = backend.state.lock().unwrap();
        let object = state
            .objects
            .values_mut()
            .find(|v| v.bytes.starts_with(&bytes[..65536]))
            .unwrap();
        object.bytes[0] ^= 1;
    }
    let mut warm = Vec::new();
    file.read_range(0..10, &mut warm).await.unwrap();
    assert_eq!(warm, bytes[..10]);
    assert_eq!(file.read(0, 10).await.unwrap(), bytes[..10]);

    let mut cold = Vec::new();
    let err = runtime
        .authority()
        .data()
        .read_range(file.descriptor(), 0..10, &mut cold)
        .await
        .unwrap_err();
    assert_eq!(err.kind(), yinyang::core::ErrorKind::Corrupt);
    assert!(cold.is_empty());
}

async fn identity_access(runtime: &Runtime) {
    let root = runtime.authority().root();
    runtime.create_directory(root, "dir").await.unwrap();
    let snapshot = runtime.authority().observe_latest().await.unwrap();
    let dir = snapshot.lookup(root, "dir").await.unwrap().unwrap().node_id;
    runtime.create_file(dir, "a").await.unwrap();
    runtime.create_file(dir, "b").await.unwrap();
    let mut file = runtime.open_file("dir/a", true).await.unwrap();
    file.write(0, b"original").await.unwrap();
    file.close().await.unwrap();
    let pinned = runtime.authority().observe_latest().await.unwrap();
    let first = pinned.scan(dir, None, 1).await.unwrap();
    let node = first.entries[0].node_id;
    assert_eq!(first.entries[0].name, "a");
    assert_eq!(pinned.node(node).await.unwrap().unwrap().id(), node);
    runtime.rename(node, root, "moved").await.unwrap();
    runtime.create_file(dir, "a").await.unwrap();
    let latest = runtime.authority().observe_latest().await.unwrap();
    assert!(latest.scan(dir, first.next.as_ref(), 1).await.is_err());
    let second = pinned.scan(dir, first.next.as_ref(), 1).await.unwrap();
    assert_eq!(second.entries[0].name, "b");
    assert!(second.next.is_none());
    let mut by_id = runtime.open_node(node, true).await.unwrap();
    assert_eq!(by_id.read(0, 20).await.unwrap(), b"original");
    let mut replacement = runtime.open_file("dir/a", false).await.unwrap();
    assert_ne!(replacement.status().await.unwrap().node, node);
    assert!(replacement.read(0, 20).await.unwrap().is_empty());
    replacement.close().await.unwrap();
    by_id.write(0, b"modified").await.unwrap();
    by_id.close().await.unwrap();
    let mut historical = runtime
        .open_node_at(pinned.revision(), node, true)
        .await
        .unwrap();
    assert_eq!(historical.read(0, 20).await.unwrap(), b"original");
    historical.write(0, b"obsolete").await.unwrap();
    assert!(matches!(historical.fsync().await, Err(Error::Conflict)));
    historical.abort().await.unwrap();
    runtime.unlink(node).await.unwrap();
    assert!(runtime.open_node(node, false).await.is_err());
    let mut retained = runtime
        .open_node_at(pinned.revision(), node, false)
        .await
        .unwrap();
    assert_eq!(retained.read(0, 20).await.unwrap(), b"original");
    retained.close().await.unwrap();
    assert!(runtime.open_node(root, false).await.is_err());
    let foreign = Fs::create(TestBackend::default().operator(), BackendProfile::Minio)
        .await
        .unwrap();
    let revision = foreign.observe_latest().await.unwrap().revision();
    assert!(runtime.open_node_at(revision, node, false).await.is_err());
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn file_operations_share_object_and_service_contract() {
    let temp = tempfile::tempdir().unwrap();
    let backend = TestBackend::default();
    let object = Fs::create(backend.operator(), BackendProfile::Minio)
        .await
        .unwrap();
    exercise(
        Arc::new(object),
        &temp.path().join("object"),
        &backend,
        true,
    )
    .await;
    let backend = TestBackend::default();
    let service = MetadataService::create(temp.path().join("metadata.db"), backend.operator())
        .await
        .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let token = "test-only-service-token-at-least-32-bytes";
    let server = tokio::spawn(serve(listener, service, token.into()));
    let client = ServiceClient::connect(addr, token.into(), backend.operator())
        .await
        .unwrap();
    exercise(
        Arc::new(client),
        &temp.path().join("service"),
        &backend,
        false,
    )
    .await;
    server.abort();
}

#[tokio::test]
async fn publication_reuses_only_its_matching_authenticated_base_snapshot() {
    let backend = TestBackend::default();
    let calls = Arc::new(PrepareCalls::default());
    let inner: Arc<dyn Authority> = Arc::new(
        Fs::create(backend.operator(), BackendProfile::Minio)
            .await
            .unwrap(),
    );
    let authority: Arc<dyn Authority> = Arc::new(RecordingAuthority::new(inner, calls.clone()));
    let temp = tempfile::tempdir().unwrap();
    let runtime = Runtime::open(authority.clone(), temp.path()).await.unwrap();
    runtime
        .create_file(authority.root(), "reuse")
        .await
        .unwrap();
    let mut first = runtime.open_file("reuse", true).await.unwrap();
    let mut stale = runtime.open_file("reuse", true).await.unwrap();

    calls.reset();
    first.write(0, b"one").await.unwrap();
    first.fsync().await.unwrap().unwrap();
    assert_eq!(calls.latest.load(Ordering::SeqCst), 0);
    assert_eq!(calls.revision.load(Ordering::SeqCst), 1);

    calls.reset();
    first.write(0, b"two").await.unwrap();
    first.fsync().await.unwrap().unwrap();
    assert_eq!(calls.latest.load(Ordering::SeqCst), 0);
    assert_eq!(calls.revision.load(Ordering::SeqCst), 1);

    // The stale handle reuses its original authenticated Snapshot for planning,
    // but that snapshot still carries the original conditions and cannot rebase.
    calls.reset();
    stale.write(0, b"stale").await.unwrap();
    assert!(matches!(stale.fsync().await, Err(Error::Conflict)));
    assert_eq!(calls.latest.load(Ordering::SeqCst), 0);
    assert_eq!(calls.revision.load(Ordering::SeqCst), 0);
    assert_eq!(stale.read(0, 10).await.unwrap(), b"stale");
    stale.abort().await.unwrap();

    let node = first.status().await.unwrap().node;
    first.close().await.unwrap();
    assert_eq!(
        authority
            .observe_latest()
            .await
            .unwrap()
            .open_file(node)
            .await
            .unwrap()
            .read(0, 10)
            .await
            .unwrap(),
        b"two"
    );
}

#[tokio::test]
async fn recovered_sparse_reads_authenticate_the_base_lazily() {
    const BLOCK: usize = 64 * 1024;

    let backend = TestBackend::default();
    let inner: Arc<dyn Authority> = Arc::new(
        Fs::create(backend.operator(), BackendProfile::Minio)
            .await
            .unwrap(),
    );
    let fault = Arc::new(FailingRevisionAuthority::new(inner));
    let authority: Arc<dyn Authority> = fault.clone();
    let temp = tempfile::tempdir().unwrap();
    let runtime = Runtime::open(authority.clone(), temp.path()).await.unwrap();
    let base = vec![0x11; BLOCK * 2];
    let prepared = authority.prepare(&mut base.as_slice()).await.unwrap();
    let snapshot = authority.observe_latest().await.unwrap();
    let mut plan = Planner::new(&snapshot, CommitId::generate());
    let node = plan
        .create_file(authority.root(), "lazy", prepared, false)
        .await
        .unwrap();
    assert!(matches!(
        authority.commit(&plan.finish().unwrap()).await.unwrap(),
        CommitOutcome::Committed(_)
    ));

    let mut file = runtime.open_node(node, true).await.unwrap();
    file.write(100, b"LOCAL").await.unwrap();
    let handle = file.release().unwrap();
    fault.fail_revision.store(true, Ordering::SeqCst);
    let mut recovered = runtime.recover(handle).await.unwrap();
    assert_eq!(recovered.read(100, 5).await.unwrap(), b"LOCAL");
    assert!(
        recovered
            .read((BLOCK * 2) as u64 + 1, 8)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        recovered.read(0, 1).await.unwrap_err().kind(),
        ErrorKind::Storage
    );
    assert!(recovered.status().await.unwrap().error.is_some());
    recovered.acknowledge_error().await.unwrap();

    fault.fail_revision.store(false, Ordering::SeqCst);
    recovered.fsync().await.unwrap().unwrap();
    assert_eq!(
        authority
            .observe_latest()
            .await
            .unwrap()
            .open_file(node)
            .await
            .unwrap()
            .read(100, 5)
            .await
            .unwrap(),
        b"LOCAL"
    );
    recovered.close().await.unwrap();
}

#[tokio::test]
async fn durable_staging_reopens_and_preserves_concurrent_generation_conflict() {
    let temp = tempfile::tempdir().unwrap();
    let backend = TestBackend::default();
    let authority: Arc<dyn Authority> = Arc::new(
        Fs::create(backend.operator(), BackendProfile::Minio)
            .await
            .unwrap(),
    );
    let runtime = Runtime::open(authority.clone(), temp.path()).await.unwrap();
    assert!(Runtime::open(authority.clone(), temp.path()).await.is_err());
    runtime.create_file(authority.root(), "file").await.unwrap();
    let mut file = runtime.open_file("file", true).await.unwrap();
    // Cross chunk boundaries, append and truncate without resurrecting truncated bytes.
    file.write(65534, b"abcdefgh").await.unwrap();
    file.truncate(65536).await.unwrap();
    file.truncate(65544).await.unwrap();
    let id = file.id();
    assert!(runtime.recover(id).await.is_err());
    drop(file);
    drop(runtime);
    let runtime = Runtime::open(authority.clone(), temp.path()).await.unwrap();
    assert_eq!(runtime.recoverable().await.unwrap().len(), 1);
    let mut file = runtime.recover(id).await.unwrap();
    assert_eq!(file.read(65534, 20).await.unwrap(), b"ab\0\0\0\0\0\0\0\0");
    file.fsync().await.unwrap();
    file.append(b"retained").await.unwrap();
    let snapshot = authority.observe_latest().await.unwrap();
    let mut plan = Planner::new(&snapshot, CommitId::generate());
    let content = authority
        .prepare(&mut b"other writer".as_slice())
        .await
        .unwrap();
    plan.set_content(file.status().await.unwrap().node, content)
        .await
        .unwrap();
    assert!(matches!(
        authority.commit(&plan.finish().unwrap()).await.unwrap(),
        CommitOutcome::Committed(_)
    ));
    assert!(matches!(file.fsync().await, Err(Error::Conflict)));
    drop(file);
    drop(runtime);
    let runtime = Runtime::open(authority.clone(), temp.path()).await.unwrap();
    let mut file = runtime.recover(id).await.unwrap();
    assert!(file.status().await.unwrap().conflict);
    assert!(file.status().await.unwrap().error.is_some());
    assert!(matches!(file.fsync().await, Err(Error::Conflict)));
    assert_eq!(file.read(65544, 8).await.unwrap(), b"retained");
    file.abort().await.unwrap();
}

async fn sparse_runtime(
    runtime: &Runtime,
    backend: &TestBackend,
    staging: &std::path::Path,
    object_head: bool,
) {
    const BASE_BYTES: usize = 1024 * 1024;
    let bytes: Vec<u8> = (0..BASE_BYTES).map(|index| (index % 251) as u8).collect();
    let prepared = runtime
        .authority()
        .prepare(&mut bytes.as_slice())
        .await
        .unwrap();
    let snapshot = runtime.authority().observe_latest().await.unwrap();
    let mut plan = Planner::new(&snapshot, CommitId::generate());
    let node = plan
        .create_file(runtime.authority().root(), "sparse", prepared, false)
        .await
        .unwrap();
    assert!(matches!(
        runtime
            .authority()
            .commit(&plan.finish().unwrap())
            .await
            .unwrap(),
        CommitOutcome::Committed(_)
    ));

    reset_backend_counters(backend);
    let mut file = runtime.open_node(node, true).await.unwrap();
    assert!(
        backend.state.lock().unwrap().data_read_bytes < 65536,
        "opening a large Managed file downloaded its payload"
    );
    let database = rusqlite::Connection::open_with_flags(
        staging.join("staging.db"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .unwrap();
    let id = *file.id().as_bytes();
    let counts = |table: &str| -> i64 {
        database
            .query_row(
                &format!("SELECT COUNT(*) FROM {table} WHERE handle=?1"),
                [id.as_slice()],
                |row| row.get(0),
            )
            .unwrap()
    };
    assert_eq!(counts("bases"), 1);
    assert_eq!(counts("chunks"), 0);
    assert_eq!(counts("segments"), 0);
    assert!(file.read(BASE_BYTES as u64, 64).await.unwrap().is_empty());
    assert!(
        file.read(BASE_BYTES as u64 + 1, 64)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(file.read(0, 0).await.unwrap().is_empty());

    file.write(1000, b"WXYZ").await.unwrap();
    let segments: Vec<(i64, i64)> = {
        let mut query = database
            .prepare("SELECT position,length(data) FROM segments WHERE handle=?1")
            .unwrap();
        query
            .query_map([id.as_slice()], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap()
    };
    assert_eq!(segments, [(1000, 4)]);
    let mut expected = bytes.clone();
    expected[1000..1004].copy_from_slice(b"WXYZ");
    assert_eq!(file.read(996, 16).await.unwrap(), expected[996..1012]);

    reset_backend_counters(backend);
    file.fsync().await.unwrap();
    let (read_bytes, written_bytes) = backend_counters(backend);
    assert!(
        read_bytes < BASE_BYTES as u64 / 2,
        "same-length sparse fsync read the whole base: {} bytes",
        read_bytes
    );
    assert!(
        written_bytes < BASE_BYTES as u64 / 2,
        "same-length sparse fsync uploaded the whole file: {} bytes",
        written_bytes
    );
    assert_eq!(
        runtime
            .authority()
            .observe_latest()
            .await
            .unwrap()
            .open_file(node)
            .await
            .unwrap()
            .read(996, 16)
            .await
            .unwrap(),
        expected[996..1012]
    );

    // Appending must extend the published version without streaming the base.
    reset_backend_counters(backend);
    assert_eq!(file.append(b"TAIL").await.unwrap(), BASE_BYTES as u64);
    file.fsync().await.unwrap();
    let (read_bytes, written_bytes) = backend_counters(backend);
    assert!(
        read_bytes < BASE_BYTES as u64 / 8,
        "bounded append read the base: {read_bytes} bytes"
    );
    assert!(
        written_bytes < BASE_BYTES as u64 / 2,
        "bounded append uploaded the whole file: {written_bytes} bytes"
    );
    expected.extend_from_slice(b"TAIL");
    assert_eq!(
        runtime
            .authority()
            .observe_latest()
            .await
            .unwrap()
            .open_file(node)
            .await
            .unwrap()
            .read(BASE_BYTES as u64 - 8, 12)
            .await
            .unwrap(),
        expected[BASE_BYTES - 8..]
    );

    let mut newer = runtime.open_node(node, true).await.unwrap();
    file.write(2000, b"FROZ").await.unwrap();
    newer.write(3000, b"NEW!").await.unwrap();
    if object_head {
        backend.state.lock().unwrap().delay_next_head = true;
        let commit = match file.fsync().await {
            Err(Error::Unknown(commit)) => commit,
            outcome => panic!(
                "expected sparse publication outcome to be unknown, got {outcome:?}; status: {:?}",
                file.status().await
            ),
        };
        assert_eq!(file.status().await.unwrap().frozen_commit_id, Some(commit));
        assert!(file.write(0, b"blocked").await.is_err());
        assert_eq!(file.fsync().await.unwrap().unwrap().commit_id, commit);
    } else {
        file.fsync().await.unwrap().unwrap();
    }
    assert!(matches!(newer.fsync().await, Err(Error::Conflict)));
    let mut newer_expected = expected.clone();
    newer_expected[3000..3004].copy_from_slice(b"NEW!");
    assert_eq!(
        newer.read(2996, 8).await.unwrap(),
        newer_expected[2996..3004]
    );
    newer.abort().await.unwrap();

    file.truncate(700).await.unwrap();
    file.truncate(BASE_BYTES as u64).await.unwrap();
    let handle = file.release().unwrap();
    let mut file = runtime.recover(handle).await.unwrap();
    assert_eq!(file.read(700, 12).await.unwrap(), &[0; 12]);
    file.close().await.unwrap();
    let mut zero_extended = expected[..700].to_vec();
    zero_extended.resize(BASE_BYTES, 0);
    assert_eq!(
        runtime
            .authority()
            .observe_latest()
            .await
            .unwrap()
            .open_file(node)
            .await
            .unwrap()
            .read(0, BASE_BYTES)
            .await
            .unwrap(),
        zero_extended
    );

    // Extending a published version by a few bytes only materializes the
    // extension units rather than streaming the complete file.
    let mut extended = runtime.open_node(node, true).await.unwrap();
    reset_backend_counters(backend);
    extended.truncate(BASE_BYTES as u64 + 64).await.unwrap();
    extended.fsync().await.unwrap();
    let (read_bytes, written_bytes) = backend_counters(backend);
    assert!(
        read_bytes < BASE_BYTES as u64 / 8,
        "bounded zero extension read the base: {read_bytes} bytes"
    );
    assert!(
        written_bytes < BASE_BYTES as u64 / 2,
        "bounded zero extension uploaded the whole file: {written_bytes} bytes"
    );
    let latest = runtime
        .authority()
        .observe_latest()
        .await
        .unwrap()
        .open_file(node)
        .await
        .unwrap();
    assert_eq!(latest.read(BASE_BYTES as u64, 64).await.unwrap(), [0; 64]);
    drop(latest);
    extended.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sparse_length_changes_use_bounded_update_and_replay_frozen_requests() {
    const BASE_BYTES: usize = 4 * 1024 * 1024;
    let temp = tempfile::tempdir().unwrap();
    let backend = TestBackend::default();
    let inner: Arc<dyn Authority> = Arc::new(
        Fs::create(backend.operator(), BackendProfile::Minio)
            .await
            .unwrap(),
    );
    let calls = Arc::new(PrepareCalls::default());
    let authority: Arc<dyn Authority> = Arc::new(RecordingAuthority::new(inner, calls.clone()));
    let runtime = Runtime::open(authority.clone(), temp.path()).await.unwrap();
    let mut bytes: Vec<u8> = (0..BASE_BYTES).map(|index| (index % 251) as u8).collect();
    let prepared = authority.prepare(&mut bytes.as_slice()).await.unwrap();
    let snapshot = authority.observe_latest().await.unwrap();
    let mut plan = Planner::new(&snapshot, CommitId::generate());
    let node = plan
        .create_file(authority.root(), "file", prepared, false)
        .await
        .unwrap();
    assert!(matches!(
        authority.commit(&plan.finish().unwrap()).await.unwrap(),
        CommitOutcome::Committed(_)
    ));
    calls.reset();

    let mut file = runtime.open_node(node, true).await.unwrap();
    reset_backend_counters(&backend);
    file.write(1000, b"WXYZ").await.unwrap();
    file.fsync().await.unwrap();
    assert_eq!(calls.overwrite.load(Ordering::SeqCst), 1);
    assert_eq!(calls.update.load(Ordering::SeqCst), 0);
    assert_eq!(calls.stream.load(Ordering::SeqCst), 0);
    let (read_bytes, written_bytes) = backend_counters(&backend);
    assert!(
        read_bytes < BASE_BYTES as u64 / 8,
        "same-length overwrite read the base: {read_bytes} bytes"
    );
    assert!(
        written_bytes < BASE_BYTES as u64 / 2,
        "same-length overwrite uploaded the whole file: {written_bytes} bytes"
    );
    bytes[1000..1004].copy_from_slice(b"WXYZ");

    let mut stale = runtime.open_node(node, true).await.unwrap();
    calls.reset();
    reset_backend_counters(&backend);
    assert_eq!(file.append(b"TAIL").await.unwrap(), BASE_BYTES as u64);
    file.fsync().await.unwrap();
    assert_eq!(calls.update.load(Ordering::SeqCst), 1);
    assert_eq!(calls.overwrite.load(Ordering::SeqCst), 0);
    assert_eq!(calls.stream.load(Ordering::SeqCst), 0);
    let (read_bytes, written_bytes) = backend_counters(&backend);
    assert!(
        read_bytes < BASE_BYTES as u64 / 8,
        "bounded append read the base: {read_bytes} bytes"
    );
    assert!(
        written_bytes < BASE_BYTES as u64 / 2,
        "bounded append uploaded the whole file: {written_bytes} bytes"
    );
    bytes.extend_from_slice(b"TAIL");
    assert_eq!(
        authority
            .observe_latest()
            .await
            .unwrap()
            .open_file(node)
            .await
            .unwrap()
            .read(BASE_BYTES as u64 - 8, 16)
            .await
            .unwrap(),
        bytes[BASE_BYTES - 8..]
    );

    stale.write(0, b"STALE").await.unwrap();
    assert!(matches!(stale.fsync().await, Err(Error::Conflict)));
    assert_eq!(stale.read(0, 5).await.unwrap(), b"STALE");
    stale.abort().await.unwrap();
    drop(stale);

    file.truncate(700).await.unwrap();
    file.truncate(1024 * 1024).await.unwrap();
    let id = file.release().unwrap();
    drop(runtime);
    let runtime = Runtime::open(authority.clone(), temp.path()).await.unwrap();
    let mut file = runtime.recover(id).await.unwrap();
    assert_eq!(file.read(700, 8).await.unwrap(), [0; 8]);
    calls.reset();
    reset_backend_counters(&backend);
    file.fsync().await.unwrap();
    assert_eq!(calls.update.load(Ordering::SeqCst), 1);
    assert_eq!(calls.stream.load(Ordering::SeqCst), 0);
    let mut expected = bytes[..700].to_vec();
    expected.resize(1024 * 1024, 0);
    assert_eq!(
        authority
            .observe_latest()
            .await
            .unwrap()
            .open_file(node)
            .await
            .unwrap()
            .read(0, expected.len())
            .await
            .unwrap(),
        expected
    );

    let mut extended = runtime.open_node(node, true).await.unwrap();
    calls.reset();
    reset_backend_counters(&backend);
    extended.truncate(1024 * 1024 + 4).await.unwrap();
    extended.fsync().await.unwrap();
    assert_eq!(calls.update.load(Ordering::SeqCst), 1);
    assert_eq!(calls.stream.load(Ordering::SeqCst), 0);
    let (read_bytes, written_bytes) = backend_counters(&backend);
    assert!(
        read_bytes < BASE_BYTES as u64 / 8,
        "bounded zero extension read the base: {read_bytes} bytes"
    );
    assert!(
        written_bytes < BASE_BYTES as u64 / 2,
        "bounded zero extension uploaded the whole file: {written_bytes} bytes"
    );
    assert_eq!(
        authority
            .observe_latest()
            .await
            .unwrap()
            .open_file(node)
            .await
            .unwrap()
            .read(1024 * 1024, 4)
            .await
            .unwrap(),
        [0; 4]
    );

    calls.reset();
    extended.append(b"LOST").await.unwrap();
    backend.state.lock().unwrap().delay_next_head = true;
    let Err(Error::Unknown(commit)) = extended.fsync().await else {
        panic!("expected a frozen append publication");
    };
    assert_eq!(calls.update.load(Ordering::SeqCst), 1);
    assert_eq!(
        extended.status().await.unwrap().frozen_commit_id,
        Some(commit)
    );
    assert!(extended.write(0, b"blocked").await.is_err());
    let receipt = extended.fsync().await.unwrap().unwrap();
    assert_eq!(receipt.commit_id, commit);
    assert_eq!(calls.update.load(Ordering::SeqCst), 1);
    assert_eq!(calls.stream.load(Ordering::SeqCst), 0);
    assert_eq!(
        authority
            .observe_latest()
            .await
            .unwrap()
            .open_file(node)
            .await
            .unwrap()
            .read(1024 * 1024, 8)
            .await
            .unwrap(),
        b"\0\0\0\0LOST"
    );
    extended.close().await.unwrap();

    // A single write whose encoded edits exceed the existing wire budget must
    // keep the explicit streaming fallback.
    calls.reset();
    let base = vec![0x11_u8; 1024];
    let prepared = authority.prepare(&mut base.as_slice()).await.unwrap();
    let snapshot = authority.observe_latest().await.unwrap();
    let mut plan = Planner::new(&snapshot, CommitId::generate());
    let large = plan
        .create_file(authority.root(), "large-edit", prepared, false)
        .await
        .unwrap();
    authority.commit(&plan.finish().unwrap()).await.unwrap();
    calls.reset();
    let mut file = runtime.open_node(large, true).await.unwrap();
    let data = vec![0xAB_u8; 16 * 1024 * 1024];
    file.write(0, &data).await.unwrap();
    file.fsync().await.unwrap();
    assert_eq!(calls.stream.load(Ordering::SeqCst), 1);
    assert_eq!(calls.update.load(Ordering::SeqCst), 0);
    assert_eq!(calls.overwrite.load(Ordering::SeqCst), 0);
    let latest = authority
        .observe_latest()
        .await
        .unwrap()
        .open_file(large)
        .await
        .unwrap();
    assert_eq!(latest.read(0, 4).await.unwrap(), [0xAB; 4]);
    assert_eq!(
        latest.read(data.len() as u64 - 4, 4).await.unwrap(),
        [0xAB; 4]
    );
    drop(latest);
    file.close().await.unwrap();
}

#[tokio::test]
async fn upload_failure_and_unknown_publication_keep_frozen_identity() {
    let temp = tempfile::tempdir().unwrap();
    let backend = TestBackend::default();
    let authority: Arc<dyn Authority> = Arc::new(
        Fs::create(backend.operator(), BackendProfile::Minio)
            .await
            .unwrap(),
    );
    let runtime = Runtime::open(authority.clone(), temp.path()).await.unwrap();
    runtime.create_file(authority.root(), "file").await.unwrap();
    let mut file = runtime.open_file("file", true).await.unwrap();
    file.append(b"recover me").await.unwrap();
    backend.state.lock().unwrap().fail_data_close = true;
    assert!(file.fsync().await.is_err());
    assert!(file.status().await.unwrap().pending);
    assert!(file.status().await.unwrap().error.is_some());
    assert!(file.close().await.is_err());
    backend.state.lock().unwrap().fail_data_close = false;
    backend.state.lock().unwrap().delay_next_head = true;
    let Err(Error::Unknown(id)) = file.fsync().await else {
        panic!("expected unknown");
    };
    let unknown = file.status().await.unwrap().error.unwrap();
    assert_eq!(unknown.kind(), ErrorKind::Unknown);
    assert_eq!(unknown.commit_id(), Some(id));
    assert!(file.status().await.unwrap().frozen);
    assert!(file.abort().await.is_err());
    assert!(file.write(0, b"different").await.is_err());
    let handle = file.release().unwrap();
    drop(runtime);
    let inspected = Runtime::inspect(temp.path()).await.unwrap();
    assert!(
        inspected
            .errors
            .iter()
            .any(|e| e.error.kind() == ErrorKind::Unknown && e.error.commit_id() == Some(id))
    );
    // A later retry fences the delayed object CAS without changing the original request.
    let runtime = Runtime::open(authority.clone(), temp.path()).await.unwrap();
    let mut file = runtime.recover(handle).await.unwrap();
    let receipt = file.fsync().await.unwrap().unwrap();
    assert_eq!(receipt.commit_id, id);
    assert!(!file.status().await.unwrap().pending);
    file.close().await.unwrap();
    assert!(!runtime.status().await.unwrap().errors.is_empty());
    let mut reopened = runtime.open_file("file", false).await.unwrap();
    assert_eq!(reopened.read(0, 20).await.unwrap(), b"recover me");
    reopened.close().await.unwrap();
}

#[tokio::test]
async fn retryable_publication_replays_the_same_frozen_request() {
    let temp = tempfile::tempdir().unwrap();
    let backend = TestBackend::default();
    let authority: Arc<dyn Authority> = Arc::new(
        Fs::create(backend.operator(), BackendProfile::Minio)
            .await
            .unwrap(),
    );
    let runtime = Runtime::open(authority.clone(), temp.path()).await.unwrap();
    runtime.create_file(authority.root(), "file").await.unwrap();
    let mut file = runtime.open_file("file", true).await.unwrap();
    file.write(0, b"retry exact").await.unwrap();

    backend.state.lock().unwrap().delay_next_head = true;
    let Err(Error::Unknown(commit)) = file.fsync().await else {
        panic!("expected unknown publication");
    };
    assert_eq!(file.status().await.unwrap().frozen_commit_id, Some(commit));

    // Allow replay's observe, then fail the commit's observe as a storage error.
    backend.state.lock().unwrap().fail_head_read_after = Some(1);
    assert!(matches!(file.fsync().await, Err(Error::Retryable)));
    let retryable = file.status().await.unwrap();
    assert!(retryable.pending && retryable.frozen);
    assert_eq!(
        retryable.error.as_ref().unwrap().kind(),
        ErrorKind::Retryable
    );
    assert_eq!(retryable.frozen_commit_id, Some(commit));
    assert!(file.write(0, b"different").await.is_err());

    let receipt = file.fsync().await.unwrap().unwrap();
    assert_eq!(receipt.commit_id, commit);
    assert_eq!(
        authority
            .observe_latest()
            .await
            .unwrap()
            .open_file(file.status().await.unwrap().node)
            .await
            .unwrap()
            .read(0, 20)
            .await
            .unwrap(),
        b"retry exact"
    );
    file.close().await.unwrap();
}

#[tokio::test]
async fn clean_handle_reports_unacknowledged_errors_on_close() {
    let temp = tempfile::tempdir().unwrap();
    let backend = TestBackend::default();
    let authority: Arc<dyn Authority> = Arc::new(
        Fs::create(backend.operator(), BackendProfile::Minio)
            .await
            .unwrap(),
    );
    let runtime = Runtime::open(authority.clone(), temp.path()).await.unwrap();
    runtime.create_file(authority.root(), "file").await.unwrap();
    let mut file = runtime.open_file("file", false).await.unwrap();
    assert!(file.write(0, b"forbidden").await.is_err());
    assert!(file.close().await.is_err());
    file.acknowledge_error().await.unwrap();
    file.close().await.unwrap();
    assert!(!runtime.status().await.unwrap().errors.is_empty());
}

#[tokio::test]
async fn create_results_keep_the_committed_node_identity() {
    let temp = tempfile::tempdir().unwrap();
    let backend = TestBackend::default();
    let authority: Arc<dyn Authority> = Arc::new(
        Fs::create(backend.operator(), BackendProfile::Minio)
            .await
            .unwrap(),
    );
    let runtime = Runtime::open(authority.clone(), temp.path()).await.unwrap();
    let root = authority.root();

    let (first, receipt) = runtime
        .create_file_with_node(root, "file", true)
        .await
        .unwrap();
    let snapshot = authority.observe_latest().await.unwrap();
    assert_eq!(snapshot.revision(), receipt.cursor.revision);
    assert_eq!(snapshot.resolve("file").await.unwrap().unwrap().id(), first);
    assert!(snapshot.node(first).await.unwrap().unwrap().executable());

    // A path lookup after the commit could have selected this replacement.
    runtime.rename(first, root, "moved").await.unwrap();
    let (second, _) = runtime
        .create_file_with_node(root, "file", false)
        .await
        .unwrap();
    assert_ne!(first, second);
    assert_eq!(
        authority
            .observe_latest()
            .await
            .unwrap()
            .resolve("file")
            .await
            .unwrap()
            .unwrap()
            .id(),
        second
    );
    let mut original = runtime.open_node(first, false).await.unwrap();
    assert_eq!(original.status().await.unwrap().node, first);
    original.close().await.unwrap();

    let (directory, _) = runtime
        .create_directory_with_node(root, "directory")
        .await
        .unwrap();
    let (child, _) = runtime
        .create_file_with_node(directory, "child", false)
        .await
        .unwrap();
    let snapshot = authority.observe_latest().await.unwrap();
    assert_eq!(
        snapshot
            .lookup(root, "directory")
            .await
            .unwrap()
            .unwrap()
            .node_id,
        directory
    );
    assert_eq!(
        snapshot
            .lookup(directory, "child")
            .await
            .unwrap()
            .unwrap()
            .node_id,
        child
    );
}

#[tokio::test]
async fn path_namespace_operations_use_snapshot_identity_and_kind() {
    let temp = tempfile::tempdir().unwrap();
    let backend = TestBackend::default();
    let authority: Arc<dyn Authority> = Arc::new(
        Fs::create(backend.operator(), BackendProfile::Minio)
            .await
            .unwrap(),
    );
    let runtime = Runtime::open(authority.clone(), temp.path()).await.unwrap();
    let root = authority.root();
    let (file, _) = runtime
        .create_file_with_node(root, "file", false)
        .await
        .unwrap();
    let (directory, _) = runtime
        .create_directory_with_node(root, "directory")
        .await
        .unwrap();
    assert_eq!(
        runtime
            .unlink_path(root, "directory")
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::IsDirectory
    );
    assert_eq!(
        runtime.rmdir_path(root, "file").await.unwrap_err().kind(),
        ErrorKind::NotDirectory
    );

    let (removed, _) = runtime.unlink_path(root, "file").await.unwrap();
    assert_eq!(removed, file);
    assert!(
        authority
            .observe_latest()
            .await
            .unwrap()
            .resolve("file")
            .await
            .unwrap()
            .is_none()
    );

    let (source, _) = runtime
        .create_file_with_node(root, "source", false)
        .await
        .unwrap();
    let (renamed, _) = runtime
        .rename_path(root, "source", root, "renamed")
        .await
        .unwrap();
    assert_eq!(renamed, source);
    assert_eq!(
        authority
            .observe_latest()
            .await
            .unwrap()
            .resolve("renamed")
            .await
            .unwrap()
            .unwrap()
            .id(),
        source
    );
    let (replacement, _) = runtime
        .create_file_with_node(root, "replacement", false)
        .await
        .unwrap();
    assert_eq!(
        runtime
            .rename_path(root, "replacement", root, "renamed")
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::AlreadyExists
    );
    let (replaced, _) = runtime
        .rename_path_replace(root, "replacement", root, "renamed")
        .await
        .unwrap();
    assert_eq!(replaced, replacement);
    let snapshot = authority.observe_latest().await.unwrap();
    assert_eq!(
        snapshot.resolve("renamed").await.unwrap().unwrap().id(),
        replacement
    );
    assert!(snapshot.node(source).await.unwrap().is_none());

    let (removed, _) = runtime.rmdir_path(root, "directory").await.unwrap();
    assert_eq!(removed, directory);
}

#[tokio::test]
async fn path_namespace_mutations_conflict_after_concurrent_move_and_replace() {
    let temp = tempfile::tempdir().unwrap();
    let backend = TestBackend::default();
    let inner: Arc<dyn Authority> = Arc::new(
        Fs::create(backend.operator(), BackendProfile::Minio)
            .await
            .unwrap(),
    );
    let setup = Runtime::open(inner.clone(), temp.path().join("setup"))
        .await
        .unwrap();
    let (source, _) = setup
        .create_file_with_node(inner.root(), "a", false)
        .await
        .unwrap();
    drop(setup);

    let pausing = Arc::new(PausingAuthority::new(inner.clone()));
    let authority: Arc<dyn Authority> = pausing.clone();
    let runtime = Runtime::open(authority, temp.path().join("mount"))
        .await
        .unwrap();
    let other = Runtime::open(inner.clone(), temp.path().join("other"))
        .await
        .unwrap();
    let root = inner.root();

    pausing.pause_next_commit();
    let task = {
        let runtime = runtime.clone();
        tokio::spawn(async move { runtime.unlink_path(root, "a").await })
    };
    pausing.entered.acquire().await.unwrap().forget();
    other.rename(source, root, "b").await.unwrap();
    let (unlink_replacement, _) = other.create_file_with_node(root, "a", false).await.unwrap();
    pausing.release.add_permits(1);
    assert_eq!(task.await.unwrap().unwrap_err().kind(), ErrorKind::Conflict);
    let snapshot = inner.observe_latest().await.unwrap();
    assert_eq!(snapshot.resolve("b").await.unwrap().unwrap().id(), source);
    assert_eq!(
        snapshot.resolve("a").await.unwrap().unwrap().id(),
        unlink_replacement
    );

    pausing.pause_next_commit();
    let task = {
        let runtime = runtime.clone();
        tokio::spawn(async move { runtime.rename_path(root, "b", root, "target").await })
    };
    pausing.entered.acquire().await.unwrap().forget();
    other.rename(source, root, "c").await.unwrap();
    let (rename_replacement, _) = other.create_file_with_node(root, "b", false).await.unwrap();
    pausing.release.add_permits(1);
    assert_eq!(task.await.unwrap().unwrap_err().kind(), ErrorKind::Conflict);
    let snapshot = inner.observe_latest().await.unwrap();
    assert_eq!(snapshot.resolve("c").await.unwrap().unwrap().id(), source);
    assert_eq!(
        snapshot.resolve("b").await.unwrap().unwrap().id(),
        rename_replacement
    );
    assert!(snapshot.resolve("target").await.unwrap().is_none());
}
