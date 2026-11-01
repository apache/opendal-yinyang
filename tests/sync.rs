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
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use support::TestBackend;
use yinyang::core::service::{MetadataService, ServiceClient, serve};
use yinyang::core::{Authority, CommitOutcome, Fs, NodeId, Revision, Transaction};
use yinyang::runtime::{ErrorKind, Runtime};
use yinyang::sync::{
    EditStatus, FileMetadata, NamespaceOperation, NamespaceState, RenameMode, Sync,
};

async fn exercise(authority: Arc<dyn Authority>) {
    let temp = tempfile::tempdir().unwrap();
    let runtime = Runtime::open(authority.clone(), temp.path().join("editor"))
        .await
        .unwrap();
    runtime.create_file(authority.root(), "file").await.unwrap();
    let baseline = authority.observe_latest().await.unwrap();
    let node = baseline.resolve("file").await.unwrap().unwrap().id();
    let sync = Sync::open(authority.clone(), temp.path().join("sync"))
        .await
        .unwrap();
    let edit = sync
        .stage(node, baseline.revision(), &mut &b"local"[..])
        .await
        .unwrap();
    assert!(sync.status(edit).await.unwrap().pending);
    assert_eq!(
        sync.stage(node, baseline.revision(), &mut &b"local"[..])
            .await
            .unwrap(),
        edit
    );
    assert_eq!(sync.edits().await.unwrap(), vec![edit]);
    let published = sync.publish(edit).await.unwrap();
    assert_eq!(sync.publish(edit).await.unwrap(), published);
    drop(sync);
    let sync = Sync::open(authority.clone(), temp.path().join("sync"))
        .await
        .unwrap();
    assert_eq!(sync.publish(edit).await.unwrap(), published);
    assert_eq!(
        sync.stage(node, baseline.revision(), &mut &b"local"[..])
            .await
            .unwrap(),
        edit
    );
    let pinned = baseline.open_file(node).await.unwrap();
    assert!(pinned.read(0, 10).await.unwrap().is_empty());
    assert_eq!(
        authority
            .observe_revision(published)
            .await
            .unwrap()
            .open_file(node)
            .await
            .unwrap()
            .read(0, 10)
            .await
            .unwrap(),
        b"local"
    );
    let stale = sync
        .stage(node, baseline.revision(), &mut &b"offline"[..])
        .await
        .unwrap();
    assert_eq!(
        sync.publish(stale).await.unwrap_err().kind(),
        ErrorKind::Conflict
    );
    assert_eq!(sync.read(stale, 0, 10).await.unwrap(), b"offline");
    // Empty-to-empty intent must still protect its original baseline.
    let empty = sync
        .stage(node, baseline.revision(), &mut &b""[..])
        .await
        .unwrap();
    assert_eq!(
        sync.publish(empty).await.unwrap_err().kind(),
        ErrorKind::Conflict
    );
    assert!(sync.read(empty, 0, 10).await.unwrap().is_empty());
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sync_is_independent_and_conditional_in_both_modes() {
    let backend = TestBackend::default();
    exercise(Arc::new(Fs::create(backend.operator()).await.unwrap())).await;
    let backend = TestBackend::default();
    let temp = tempfile::tempdir().unwrap();
    let service = MetadataService::create(temp.path().join("metadata.db"), backend.operator())
        .await
        .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let token = "sync-test-only-service-token-at-least-32-bytes";
    let server = tokio::spawn(serve(listener, service, token.into()));
    let client = ServiceClient::connect(address, token.into(), backend.operator())
        .await
        .unwrap();
    exercise(Arc::new(client)).await;
    server.abort();
}
#[tokio::test]
async fn original_upload_survives_unknown_and_restart() {
    let backend = TestBackend::default();
    let authority: Arc<dyn Authority> = Arc::new(Fs::create(backend.operator()).await.unwrap());
    let temp = tempfile::tempdir().unwrap();
    let runtime = Runtime::open(authority.clone(), temp.path().join("editor"))
        .await
        .unwrap();
    runtime.create_file(authority.root(), "file").await.unwrap();
    let base = authority.observe_latest().await.unwrap();
    let node = base.resolve("file").await.unwrap().unwrap().id();
    let sync = Sync::open(authority.clone(), temp.path().join("sync"))
        .await
        .unwrap();
    let edit = sync
        .stage(node, base.revision(), &mut &b"recover"[..])
        .await
        .unwrap();
    backend.state.lock().unwrap().delay_next_head = true;
    let error = sync.publish(edit).await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Unknown);
    let commit = error.commit_id().unwrap();
    drop(sync);
    let sync = Sync::open(authority.clone(), temp.path().join("sync"))
        .await
        .unwrap();
    assert_eq!(
        sync.status(edit).await.unwrap().error.unwrap().commit_id(),
        Some(commit)
    );
    let revision = sync.publish(edit).await.unwrap();
    let receipt = authority
        .observe_latest()
        .await
        .unwrap()
        .receipt(commit)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(revision, receipt.cursor.revision);
}

async fn create_file(
    authority: &Arc<dyn Authority>,
    runtime: &Runtime,
    parent: NodeId,
    name: &str,
) -> (Revision, NodeId) {
    let (node, _) = runtime
        .create_file_with_node(parent, name, false)
        .await
        .unwrap();
    let baseline = authority.observe_latest().await.unwrap();
    (baseline.revision(), node)
}

async fn create_directory(
    authority: &Arc<dyn Authority>,
    runtime: &Runtime,
    parent: NodeId,
    name: &str,
) -> (Revision, NodeId) {
    let (node, _) = runtime
        .create_directory_with_node(parent, name)
        .await
        .unwrap();
    let baseline = authority.observe_latest().await.unwrap();
    (baseline.revision(), node)
}

fn assert_conflict(result: yinyang::runtime::Result<Revision>) {
    assert_eq!(result.unwrap_err().kind(), ErrorKind::Conflict);
}

/// Delegating authority that substitutes a non-Unknown error after the inner
/// authority already published, modeling a lost or malformed response.
#[derive(Debug)]
struct FaultAuthority {
    inner: Arc<dyn Authority>,
    fail_next: Arc<AtomicBool>,
}

impl FaultAuthority {
    fn new(inner: Arc<dyn Authority>) -> Self {
        Self {
            inner,
            fail_next: Arc::new(AtomicBool::new(false)),
        }
    }

    fn fail_next_response(&self) {
        self.fail_next.store(true, Ordering::SeqCst);
    }
}

impl Authority for FaultAuthority {
    fn data(&self) -> &yinyang::core::DataStore {
        self.inner.data()
    }
    fn filesystem(&self) -> NodeId {
        self.inner.filesystem()
    }
    fn root(&self) -> NodeId {
        self.inner.root()
    }
    fn observe_latest<'a>(
        &'a self,
    ) -> Pin<Box<dyn Future<Output = yinyang::core::Result<yinyang::core::Snapshot>> + Send + 'a>>
    {
        self.inner.observe_latest()
    }
    fn observe_revision<'a>(
        &'a self,
        revision: Revision,
    ) -> Pin<Box<dyn Future<Output = yinyang::core::Result<yinyang::core::Snapshot>> + Send + 'a>>
    {
        self.inner.observe_revision(revision)
    }
    fn commit_batch<'a>(
        &'a self,
        requests: &'a [Transaction],
    ) -> Pin<Box<dyn Future<Output = yinyang::core::Result<Vec<CommitOutcome>>> + Send + 'a>> {
        Box::pin(async move {
            let outcomes = self.inner.commit_batch(requests).await?;
            if self.fail_next.swap(false, Ordering::SeqCst) {
                return Err(
                    yinyang::core::ContentDescriptor::from_bytes(b"malformed response")
                        .unwrap_err(),
                );
            }
            Ok(outcomes)
        })
    }
}

async fn exercise_namespace(authority: Arc<dyn Authority>) {
    let temp = tempfile::tempdir().unwrap();
    let runtime = Runtime::open(authority.clone(), temp.path().join("editor"))
        .await
        .unwrap();
    let sync = Sync::open(authority.clone(), temp.path().join("sync"))
        .await
        .unwrap();
    let root = authority.root();

    // Create with bytes and executable metadata. The durable identity survives
    // restart and the exact intent resolves to the same commit.
    let baseline = authority.observe_latest().await.unwrap();
    let created = sync
        .stage_create_file(
            root,
            "created",
            baseline.revision(),
            FileMetadata::new(true),
            &mut &b"created bytes"[..],
        )
        .await
        .unwrap();
    assert_eq!(
        sync.stage_create_file(
            root,
            "created",
            baseline.revision(),
            FileMetadata::new(true),
            &mut &b"created bytes"[..],
        )
        .await
        .unwrap(),
        created
    );
    let status = sync.namespace_status(created).await.unwrap();
    assert_eq!(status.state, NamespaceState::Prepared);
    assert_eq!(sync.read(created, 0, 64).await.unwrap(), b"created bytes");
    let NamespaceOperation::CreateFile {
        node: created_node,
        metadata,
        ..
    } = status.intent.operation
    else {
        panic!("expected file creation intent");
    };
    assert!(metadata.executable);
    assert!(matches!(
        sync.edit_status(created).await.unwrap(),
        EditStatus::Namespace(_)
    ));
    assert!(
        sync.namespace_intents()
            .await
            .unwrap()
            .iter()
            .any(|intent| intent.intent.id == created)
    );
    let inspect = Sync::inspect(temp.path().join("sync")).await.unwrap();
    assert!(
        inspect
            .namespace
            .iter()
            .any(|intent| intent.intent.id == created)
    );
    let revision = sync.publish(created).await.unwrap();
    let committed = authority.observe_revision(revision).await.unwrap();
    let file = committed.open_file(created_node).await.unwrap();
    assert!(file.node().executable());
    assert_eq!(file.read(0, 64).await.unwrap(), b"created bytes");
    let status = sync.namespace_status(created).await.unwrap();
    assert!(matches!(status.state, NamespaceState::Committed(_)));
    drop(sync);
    let sync = Sync::open(authority.clone(), temp.path().join("sync"))
        .await
        .unwrap();
    assert!(matches!(
        sync.namespace_status(created).await.unwrap().state,
        NamespaceState::Committed(_)
    ));
    assert_eq!(sync.publish(created).await.unwrap(), revision);

    // Directory creation and a stable-identity rename.
    let baseline = authority.observe_latest().await.unwrap();
    let directory = sync
        .stage_create_directory(root, "dir", baseline.revision())
        .await
        .unwrap();
    let revision = sync.publish(directory).await.unwrap();
    let directory_node = match sync
        .namespace_status(directory)
        .await
        .unwrap()
        .intent
        .operation
    {
        NamespaceOperation::CreateDirectory { node, .. } => node,
        operation => panic!("expected directory creation, got {operation:?}"),
    };
    assert!(
        authority
            .observe_revision(revision)
            .await
            .unwrap()
            .node(directory_node)
            .await
            .unwrap()
            .unwrap()
            .is_directory()
    );
    let baseline = authority.observe_latest().await.unwrap();
    let rename = sync
        .stage_rename(
            created_node,
            baseline.revision(),
            directory_node,
            "moved",
            RenameMode::NoReplace,
        )
        .await
        .unwrap();
    let revision = sync.publish(rename).await.unwrap();
    let observed = authority.observe_revision(revision).await.unwrap();
    assert_eq!(
        observed.resolve("dir/moved").await.unwrap().unwrap().id(),
        created_node
    );
    assert!(observed.resolve("created").await.unwrap().is_none());

    // Removing a live file cannot make its pinned baseline disappear.
    let baseline = authority.observe_latest().await.unwrap();
    let remove = sync
        .stage_remove(created_node, baseline.revision())
        .await
        .unwrap();
    assert_eq!(
        sync.read(remove, 0, 1).await.unwrap_err().kind(),
        ErrorKind::Unsupported
    );
    let revision = sync.publish(remove).await.unwrap();
    assert!(
        authority
            .observe_revision(revision)
            .await
            .unwrap()
            .resolve("dir/moved")
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        baseline
            .open_file(created_node)
            .await
            .unwrap()
            .read(0, 64)
            .await
            .unwrap(),
        b"created bytes"
    );

    // No-replace rejects an occupied destination while replace removes exactly
    // the observed destination and keeps its old version readable.
    let (_, source) = create_file(&authority, &runtime, root, "source").await;
    let (baseline, target) = create_file(&authority, &runtime, root, "target").await;
    let baseline = authority.observe_revision(baseline).await.unwrap();
    assert_eq!(
        sync.stage_rename(
            source,
            baseline.revision(),
            root,
            "target",
            RenameMode::NoReplace,
        )
        .await
        .unwrap_err()
        .kind(),
        ErrorKind::AlreadyExists
    );
    let replace = sync
        .stage_rename(
            source,
            baseline.revision(),
            root,
            "target",
            RenameMode::Replace,
        )
        .await
        .unwrap();
    assert!(matches!(
        sync.namespace_status(replace).await.unwrap().intent.operation,
        NamespaceOperation::Rename {
            replaced: Some(id),
            ..
        } if id == target
    ));
    let revision = sync.publish(replace).await.unwrap();
    let observed = authority.observe_revision(revision).await.unwrap();
    assert_eq!(
        observed.resolve("target").await.unwrap().unwrap().id(),
        source
    );
    baseline.open_file(target).await.unwrap();

    // Executable changes use a baseline-bound metadata transaction.
    let (baseline, executable) = create_file(&authority, &runtime, root, "executable").await;
    let set_executable = sync
        .stage_set_executable(executable, baseline, true)
        .await
        .unwrap();
    let revision = sync.publish(set_executable).await.unwrap();
    assert!(
        authority
            .observe_revision(revision)
            .await
            .unwrap()
            .node(executable)
            .await
            .unwrap()
            .unwrap()
            .executable()
    );

    // Create/create: the second intent keeps its conflict.
    let baseline = authority.observe_latest().await.unwrap();
    let first = sync
        .stage_create_file(
            root,
            "same",
            baseline.revision(),
            FileMetadata::new(false),
            &mut &b"first"[..],
        )
        .await
        .unwrap();
    let second = sync
        .stage_create_file(
            root,
            "same",
            baseline.revision(),
            FileMetadata::new(false),
            &mut &b"second"[..],
        )
        .await
        .unwrap();
    sync.publish(first).await.unwrap();
    assert_conflict(sync.publish(second).await);
    assert_eq!(
        sync.namespace_status(second).await.unwrap().state,
        NamespaceState::Conflict
    );
    assert_conflict(sync.publish(second).await);

    // Content/edit -> rename conflict. The rename guards the source's complete
    // logical state rather than only its old parent/name topology.
    let (baseline, renamed) = create_file(&authority, &runtime, root, "renamed").await;
    let content = sync
        .stage(renamed, baseline, &mut &b"new content"[..])
        .await
        .unwrap();
    let rename = sync
        .stage_rename(
            renamed,
            baseline,
            root,
            "renamed-new",
            RenameMode::NoReplace,
        )
        .await
        .unwrap();
    sync.publish(content).await.unwrap();
    assert_conflict(sync.publish(rename).await);
    assert_eq!(
        sync.namespace_status(rename).await.unwrap().state,
        NamespaceState::Conflict
    );

    // Rename-first is safe: the same baseline content edit still commits under
    // the moved identity because its content predicates were unchanged.
    let (baseline, always_safe) =
        create_file(&authority, &runtime, root, "moved-then-edited").await;
    let content = sync
        .stage(always_safe, baseline, &mut &b"moved edit"[..])
        .await
        .unwrap();
    let rename = sync
        .stage_rename(
            always_safe,
            baseline,
            root,
            "moved-then-edited-2",
            RenameMode::NoReplace,
        )
        .await
        .unwrap();
    sync.publish(rename).await.unwrap();
    let revision = sync.publish(content).await.unwrap();
    let observed = authority.observe_revision(revision).await.unwrap();
    assert_eq!(
        observed
            .resolve("moved-then-edited-2")
            .await
            .unwrap()
            .unwrap()
            .id(),
        always_safe
    );
    assert_eq!(
        observed
            .open_file(always_safe)
            .await
            .unwrap()
            .read(0, 64)
            .await
            .unwrap(),
        b"moved edit"
    );

    // Delete/edit conflicts in both orders. Neither the local replacement nor
    // the pinned pre-delete version can be silently discarded.
    let (baseline, deleted) = create_file(&authority, &runtime, root, "deleted-first").await;
    let content = sync
        .stage(deleted, baseline, &mut &b"edit before remove"[..])
        .await
        .unwrap();
    let remove = sync.stage_remove(deleted, baseline).await.unwrap();
    sync.publish(content).await.unwrap();
    assert_conflict(sync.publish(remove).await);

    let (baseline, deleted) = create_file(&authority, &runtime, root, "removed-first").await;
    let content = sync
        .stage(deleted, baseline, &mut &b"edit after remove"[..])
        .await
        .unwrap();
    let remove = sync.stage_remove(deleted, baseline).await.unwrap();
    sync.publish(remove).await.unwrap();
    assert_conflict(sync.publish(content).await);
    assert_eq!(
        sync.read(content, 0, 64).await.unwrap(),
        b"edit after remove"
    );

    // Rename/rename: only one move of the common baseline may publish.
    let (baseline, moved) = create_file(&authority, &runtime, root, "moved-twice").await;
    let left = sync
        .stage_rename(moved, baseline, root, "left", RenameMode::NoReplace)
        .await
        .unwrap();
    let right = sync
        .stage_rename(moved, baseline, root, "right", RenameMode::NoReplace)
        .await
        .unwrap();
    sync.publish(left).await.unwrap();
    assert_conflict(sync.publish(right).await);

    // Parent removal versus child creation, in both publication orders.
    let (baseline, parent) = create_directory(&authority, &runtime, root, "parent-a").await;
    let remove = sync.stage_remove(parent, baseline).await.unwrap();
    let child = sync
        .stage_create_file(
            parent,
            "child",
            baseline,
            FileMetadata::new(false),
            &mut &b""[..],
        )
        .await
        .unwrap();
    sync.publish(child).await.unwrap();
    assert_conflict(sync.publish(remove).await);

    let (baseline, parent) = create_directory(&authority, &runtime, root, "parent-b").await;
    let remove = sync.stage_remove(parent, baseline).await.unwrap();
    let child = sync
        .stage_create_file(
            parent,
            "child",
            baseline,
            FileMetadata::new(false),
            &mut &b""[..],
        )
        .await
        .unwrap();
    sync.publish(remove).await.unwrap();
    assert_conflict(sync.publish(child).await);

    // A content edit conflicts with an executable change against the same
    // baseline; the executable intent remains inspectable.
    let (baseline, metadata) = create_file(&authority, &runtime, root, "metadata-race").await;
    let content = sync
        .stage(metadata, baseline, &mut &b"edited"[..])
        .await
        .unwrap();
    let mode = sync
        .stage_set_executable(metadata, baseline, true)
        .await
        .unwrap();
    sync.publish(content).await.unwrap();
    assert_conflict(sync.publish(mode).await);
    assert_eq!(
        sync.namespace_status(mode).await.unwrap().state,
        NamespaceState::Conflict
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn namespace_intents_are_durable_and_conflict_safe_in_both_modes() {
    let backend = TestBackend::default();
    exercise_namespace(Arc::new(Fs::create(backend.operator()).await.unwrap())).await;

    let backend = TestBackend::default();
    let temp = tempfile::tempdir().unwrap();
    let service = MetadataService::create(temp.path().join("metadata.db"), backend.operator())
        .await
        .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let token = "sync-test-only-service-token-at-least-32-bytes";
    let server = tokio::spawn(serve(listener, service, token.into()));
    let client = ServiceClient::connect(address, token.into(), backend.operator())
        .await
        .unwrap();
    exercise_namespace(Arc::new(client)).await;
    server.abort();
}

#[tokio::test]
async fn namespace_unknown_retries_the_original_request_after_restart() {
    let backend = TestBackend::default();
    let authority: Arc<dyn Authority> = Arc::new(Fs::create(backend.operator()).await.unwrap());
    let temp = tempfile::tempdir().unwrap();
    let sync = Sync::open(authority.clone(), temp.path().join("sync"))
        .await
        .unwrap();
    let baseline = authority.observe_latest().await.unwrap();
    let intent = sync
        .stage_create_directory(authority.root(), "uncertain", baseline.revision())
        .await
        .unwrap();
    let node = sync
        .namespace_status(intent)
        .await
        .unwrap()
        .intent
        .operation
        .node_id();
    backend.state.lock().unwrap().delay_next_head = true;
    let error = sync.publish(intent).await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Unknown);
    let status = sync.namespace_status(intent).await.unwrap();
    assert_eq!(status.state, NamespaceState::Unknown);
    assert_eq!(status.failure.unwrap().commit_id(), error.commit_id());
    drop(sync);

    let sync = Sync::open(authority.clone(), temp.path().join("sync"))
        .await
        .unwrap();
    assert_eq!(
        sync.namespace_status(intent).await.unwrap().state,
        NamespaceState::Unknown
    );
    assert!(sync.resolve_namespace(intent).await.unwrap().is_none());
    let revision = sync.publish(intent).await.unwrap();
    let receipt = sync.resolve_namespace(intent).await.unwrap().unwrap();
    assert_eq!(receipt.cursor.revision, revision);
    assert_eq!(
        authority
            .observe_revision(revision)
            .await
            .unwrap()
            .node(node)
            .await
            .unwrap()
            .unwrap()
            .id(),
        node
    );
    assert!(matches!(
        sync.namespace_status(intent).await.unwrap().state,
        NamespaceState::Committed(_)
    ));
}

#[tokio::test]
async fn namespace_unknown_survives_unavailable_recovery_and_resolves_once() {
    let backend = TestBackend::default();
    let authority: Arc<dyn Authority> = Arc::new(Fs::create(backend.operator()).await.unwrap());
    let temp = tempfile::tempdir().unwrap();
    let sync = Sync::open(authority.clone(), temp.path().join("sync"))
        .await
        .unwrap();
    let baseline = authority.observe_latest().await.unwrap();
    let intent = sync
        .stage_create_directory(authority.root(), "published-then-lost", baseline.revision())
        .await
        .unwrap();
    backend.lose_next_head_response_and_go_offline();
    let error = sync.publish(intent).await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Unknown);
    assert_eq!(
        sync.namespace_status(intent).await.unwrap().state,
        NamespaceState::Unknown
    );
    drop(sync);

    let sync = Sync::open(authority.clone(), temp.path().join("sync"))
        .await
        .unwrap();
    let before = sync.namespace_status(intent).await.unwrap();
    assert_eq!(before.state, NamespaceState::Unknown);
    assert!(sync.publish(intent).await.is_err());
    let after = sync.namespace_status(intent).await.unwrap();
    assert_eq!(after.state, NamespaceState::Unknown);
    assert_eq!(after.intent.commit_id, before.intent.commit_id);
    assert_eq!(after.intent.request_digest, before.intent.request_digest);

    backend.state.lock().unwrap().offline = false;
    let revision = sync.publish(intent).await.unwrap();
    let receipt = sync.resolve_namespace(intent).await.unwrap().unwrap();
    assert_eq!(revision, receipt.cursor.revision);
    assert!(matches!(
        sync.namespace_status(intent).await.unwrap().state,
        NamespaceState::Committed(_)
    ));
    assert_eq!(
        authority.observe_latest().await.unwrap().revision(),
        receipt.cursor.revision
    );
    assert_eq!(sync.publish(intent).await.unwrap(), receipt.cursor.revision);
}

#[tokio::test]
async fn namespace_restore_failure_keeps_a_prepared_intent_retryable() {
    let backend = TestBackend::default();
    let authority: Arc<dyn Authority> = Arc::new(Fs::create(backend.operator()).await.unwrap());
    let temp = tempfile::tempdir().unwrap();
    let sync = Sync::open(authority.clone(), temp.path().join("sync"))
        .await
        .unwrap();
    let baseline = authority.observe_latest().await.unwrap();
    let intent = sync
        .stage_create_directory(authority.root(), "retry-restore", baseline.revision())
        .await
        .unwrap();
    backend.state.lock().unwrap().offline = true;
    assert!(sync.publish(intent).await.is_err());
    assert_eq!(
        sync.namespace_status(intent).await.unwrap().state,
        NamespaceState::Retryable
    );
    backend.state.lock().unwrap().offline = false;
    sync.publish(intent).await.unwrap();
    assert!(matches!(
        sync.namespace_status(intent).await.unwrap().state,
        NamespaceState::Committed(_)
    ));
}

#[tokio::test]
async fn namespace_post_dispatch_error_stays_unknown_and_resolves() {
    let backend = TestBackend::default();
    let authority: Arc<dyn Authority> = Arc::new(Fs::create(backend.operator()).await.unwrap());
    let fault = Arc::new(FaultAuthority::new(authority.clone()));
    let temp = tempfile::tempdir().unwrap();
    let sync = Sync::open(fault.clone(), temp.path().join("sync"))
        .await
        .unwrap();
    let baseline = fault.observe_latest().await.unwrap();
    let intent = sync
        .stage_create_directory(fault.root(), "lost-malformed", baseline.revision())
        .await
        .unwrap();
    fault.fail_next_response();
    let error = sync.publish(intent).await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Unknown);
    let status = sync.namespace_status(intent).await.unwrap();
    assert_eq!(status.state, NamespaceState::Unknown);
    let failure = status.failure.clone().unwrap();
    assert_eq!(failure.kind(), ErrorKind::Unknown);
    assert_eq!(failure.commit_id(), Some(status.intent.commit_id));
    let receipt = sync.resolve_namespace(intent).await.unwrap().unwrap();
    assert_eq!(
        authority.observe_latest().await.unwrap().revision(),
        receipt.cursor.revision
    );
}

#[tokio::test]
async fn content_post_dispatch_error_exposes_frozen_identity_without_lease() {
    let backend = TestBackend::default();
    let authority: Arc<dyn Authority> = Arc::new(Fs::create(backend.operator()).await.unwrap());
    let temp = tempfile::tempdir().unwrap();
    let editor = Runtime::open(authority.clone(), temp.path().join("editor"))
        .await
        .unwrap();
    editor.create_file(authority.root(), "file").await.unwrap();
    let baseline = authority.observe_latest().await.unwrap();
    let node = baseline.resolve("file").await.unwrap().unwrap().id();

    let fault = Arc::new(FaultAuthority::new(authority.clone()));
    let sync = Sync::open(fault.clone(), temp.path().join("sync"))
        .await
        .unwrap();
    let edit = sync
        .stage(node, baseline.revision(), &mut &b"content"[..])
        .await
        .unwrap();
    fault.fail_next_response();
    let error = sync.publish(edit).await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Unknown);
    let commit = error.commit_id().unwrap();
    let status = sync.status(edit).await.unwrap();
    assert!(status.frozen);
    assert_eq!(status.frozen_commit_id, Some(commit));
    assert_eq!(status.error.unwrap().commit_id(), Some(commit));

    // Read-only inspection succeeds while the writer still owns the lease.
    let inspect = Sync::inspect(temp.path().join("sync")).await.unwrap();
    let inspected = inspect
        .content
        .iter()
        .find(|inspected| inspected.id == edit)
        .unwrap_or_else(|| panic!("missing inspected edit"));
    assert_eq!(inspected.handle.frozen_commit_id, Some(commit));
    drop(sync);

    backend.state.lock().unwrap().offline = true;
    let sync = Sync::open(fault.clone(), temp.path().join("sync"))
        .await
        .unwrap();
    let status = sync.status(edit).await.unwrap();
    assert_eq!(status.frozen_commit_id, Some(commit));
    assert_eq!(status.error.unwrap().commit_id(), Some(commit));
    backend.state.lock().unwrap().offline = false;
    sync.publish(edit).await.unwrap();
}

#[tokio::test]
async fn sync_one_upgrades_after_exact_legacy_schema_validation() {
    let backend = TestBackend::default();
    let authority: Arc<dyn Authority> = Arc::new(Fs::create(backend.operator()).await.unwrap());
    let temp = tempfile::tempdir().unwrap();
    let state = temp.path().join("sync");
    drop(Sync::open(authority.clone(), &state).await.unwrap());
    {
        let connection = rusqlite::Connection::open(state.join("sync.db")).unwrap();
        connection
            .execute_batch(
                "DROP TABLE namespaces;
                 UPDATE binding SET profile='yinyang-sync-1';",
            )
            .unwrap();
    }

    drop(Sync::open(authority, &state).await.unwrap());
    let connection = rusqlite::Connection::open(state.join("sync.db")).unwrap();
    let profile: String = connection
        .query_row("SELECT profile FROM binding", [], |row| row.get(0))
        .unwrap();
    assert_eq!(profile, "yinyang-sync-2");
    let namespaces: bool = connection
        .query_row(
            "SELECT EXISTS(
                SELECT 1 FROM sqlite_master
                WHERE type='table' AND name='namespaces'
             )",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(namespaces);
}

#[tokio::test]
async fn unsupported_and_partial_sync_schemas_are_not_repaired() {
    let backend = TestBackend::default();
    let authority: Arc<dyn Authority> = Arc::new(Fs::create(backend.operator()).await.unwrap());
    let temp = tempfile::tempdir().unwrap();
    let state = temp.path().join("sync");
    drop(Sync::open(authority.clone(), &state).await.unwrap());
    {
        let connection = rusqlite::Connection::open(state.join("sync.db")).unwrap();
        connection
            .execute_batch(
                "DROP TABLE namespaces;
                 UPDATE binding SET profile='yinyang-sync-99';",
            )
            .unwrap();
    }
    assert_eq!(
        Sync::open(authority.clone(), &state)
            .await
            .err()
            .unwrap()
            .kind(),
        ErrorKind::Unsupported
    );
    {
        let connection = rusqlite::Connection::open(state.join("sync.db")).unwrap();
        let profile: String = connection
            .query_row("SELECT profile FROM binding", [], |row| row.get(0))
            .unwrap();
        assert_eq!(profile, "yinyang-sync-99");
        let namespaces: bool = connection
            .query_row(
                "SELECT EXISTS(
                    SELECT 1 FROM sqlite_master
                    WHERE type='table' AND name='namespaces'
                 )",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(!namespaces);
        connection
            .execute("UPDATE binding SET profile='yinyang-sync-2'", [])
            .unwrap();
    }
    assert_eq!(
        Sync::open(authority.clone(), &state)
            .await
            .err()
            .unwrap()
            .kind(),
        ErrorKind::Corrupt
    );
    assert_eq!(
        Sync::inspect(&state).await.unwrap_err().kind(),
        ErrorKind::Corrupt
    );
}

#[tokio::test]
async fn legacy_staging_and_sync_upgrade_preserves_unknown_identity_and_newer_edits() {
    let backend = TestBackend::default();
    let authority: Arc<dyn Authority> = Arc::new(Fs::create(backend.operator()).await.unwrap());
    let temp = tempfile::tempdir().unwrap();
    let editor = Runtime::open(authority.clone(), temp.path().join("editor"))
        .await
        .unwrap();
    editor.create_file(authority.root(), "file").await.unwrap();
    let baseline = authority.observe_latest().await.unwrap();
    let node = baseline.resolve("file").await.unwrap().unwrap().id();
    let state = temp.path().join("sync");
    let sync = Sync::open(authority.clone(), &state).await.unwrap();
    let original = b"original unknown";
    let edit = sync
        .stage(node, baseline.revision(), &mut &original[..])
        .await
        .unwrap();
    backend.state.lock().unwrap().delay_next_head = true;
    let error = sync.publish(edit).await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Unknown);
    let commit = error.commit_id().unwrap();
    drop(sync);

    {
        let journal = rusqlite::Connection::open(state.join("sync.db")).unwrap();
        let handle: Vec<u8> = journal
            .query_row(
                "SELECT handle FROM edits WHERE id=?1",
                [edit.as_bytes().as_slice()],
                |row| row.get(0),
            )
            .unwrap();
        let staging = rusqlite::Connection::open(state.join("content/staging.db")).unwrap();
        // Convert the sparse test handle to the full-chunks representation the
        // legacy binary produced before simulating a stage-1 database.
        staging
            .execute(
                "INSERT INTO chunks(handle,position,data) VALUES(?1,0,?2)",
                rusqlite::params![handle, original.as_slice()],
            )
            .unwrap();
        staging
            .execute_batch(
                "ALTER TABLE handles DROP COLUMN failure;
                 ALTER TABLE errors DROP COLUMN failure;
                 DROP TABLE bases;
                 DROP TABLE segments;
                 UPDATE authority SET profile='yinyang-stage-1' WHERE singleton=1;",
            )
            .unwrap();
        journal
            .execute_batch(
                "DROP TABLE namespaces;
                 UPDATE binding SET profile='yinyang-sync-1';",
            )
            .unwrap();
    }

    let sync = Sync::open(authority.clone(), &state).await.unwrap();
    let status = sync.status(edit).await.unwrap();
    assert!(status.pending);
    assert!(status.frozen);
    assert_eq!(status.frozen_commit_id, Some(commit));
    assert_eq!(sync.read(edit, 0, original.len()).await.unwrap(), original);
    let revision = sync.publish(edit).await.unwrap();
    let receipt = authority
        .observe_latest()
        .await
        .unwrap()
        .receipt(commit)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(receipt.cursor.revision, revision);
    assert_eq!(
        authority
            .observe_revision(baseline.revision())
            .await
            .unwrap()
            .open_file(node)
            .await
            .unwrap()
            .read(0, 16)
            .await
            .unwrap(),
        b""
    );

    let current = authority.observe_revision(revision).await.unwrap();
    let newer = sync
        .stage(node, current.revision(), &mut &b"newer local edit"[..])
        .await
        .unwrap();
    sync.publish(newer).await.unwrap();
    assert_eq!(
        authority
            .observe_latest()
            .await
            .unwrap()
            .open_file(node)
            .await
            .unwrap()
            .read(0, 32)
            .await
            .unwrap(),
        b"newer local edit"
    );
}
