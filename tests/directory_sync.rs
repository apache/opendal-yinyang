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

#![cfg(target_os = "linux")]

#[path = "../crates/core/tests/support/mod.rs"]
mod support;

use std::collections::BTreeMap;
use std::future::Future;
use std::os::linux::fs::MetadataExt;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use support::TestBackend;
use yinyang::core::service::{MetadataService, ServiceClient, serve};
use yinyang::core::{
    Authority, BackendProfile, CommitOutcome, Fs, NodeId, Revision, Snapshot, Transaction,
};
use yinyang::replica::{DirectorySync, Resolution};
use yinyang::sync::{FileMetadata, Sync};

/// An independent snapshot of a real tree on one side of a sync.
#[derive(Clone, Debug, Eq, PartialEq)]
enum TreeEntry {
    Directory,
    File { executable: bool, bytes: Vec<u8> },
}

fn local_tree(root: &Path) -> BTreeMap<String, TreeEntry> {
    fn walk(path: &Path, rel: &str, tree: &mut BTreeMap<String, TreeEntry>) {
        let metadata = path.symlink_metadata().expect("tree metadata");
        if metadata.is_dir() {
            tree.insert(rel.to_owned(), TreeEntry::Directory);
            for entry in std::fs::read_dir(path).expect("tree directory") {
                let entry = entry.expect("tree entry");
                let name = entry.file_name().to_string_lossy().into_owned();
                let child = if rel.is_empty() {
                    name
                } else {
                    format!("{rel}/{name}")
                };
                walk(&entry.path(), &child, tree);
            }
        } else {
            tree.insert(
                rel.to_owned(),
                TreeEntry::File {
                    executable: metadata.st_mode() & 0o111 != 0,
                    bytes: std::fs::read(path).expect("tree file bytes"),
                },
            );
        }
    }
    let mut tree = BTreeMap::new();
    walk(root, "", &mut tree);
    tree
}

async fn remote_tree(authority: &Arc<dyn Authority>) -> BTreeMap<String, TreeEntry> {
    let snapshot = authority.observe_latest().await.unwrap();
    let mut tree = BTreeMap::new();
    let mut pending = vec![(String::new(), snapshot.root())];
    while let Some((rel, id)) = pending.pop() {
        let node = snapshot.node(id).await.unwrap().expect("remote node");
        if node.is_directory() {
            tree.insert(rel.clone(), TreeEntry::Directory);
            let mut token = None;
            loop {
                let page = snapshot.scan(id, token.as_ref(), 4096).await.unwrap();
                for entry in page.entries {
                    let child = if rel.is_empty() {
                        entry.name.clone()
                    } else {
                        format!("{rel}/{}", entry.name)
                    };
                    pending.push((child, entry.node_id));
                }
                match page.next {
                    Some(next) => token = Some(next),
                    None => break,
                }
            }
        } else {
            let version = snapshot.open_file(id).await.unwrap();
            let length = version.descriptor().content_id().length();
            tree.insert(
                rel,
                TreeEntry::File {
                    executable: node.executable(),
                    bytes: version.read(0, length as usize).await.unwrap(),
                },
            );
        }
    }
    tree
}

/// Seed a nested remote tree through the same public Sync contract that
/// DirectorySync uses for publication.
async fn seed_remote_tree(authority: &Arc<dyn Authority>) {
    let temp = tempfile::tempdir().unwrap();
    let sync = Sync::open(authority.clone(), temp.path().join("seed"))
        .await
        .unwrap();
    let baseline = authority.observe_latest().await.unwrap();
    let directory = sync
        .stage_create_directory(authority.root(), "dir", baseline.revision())
        .await
        .unwrap();
    let revision = sync.publish(directory).await.unwrap();
    let dir_node = sync
        .namespace_status(directory)
        .await
        .unwrap()
        .intent
        .operation
        .node_id();
    let baseline = authority.observe_revision(revision).await.unwrap();
    let sub = sync
        .stage_create_directory(dir_node, "sub", baseline.revision())
        .await
        .unwrap();
    let revision = sync.publish(sub).await.unwrap();
    let sub_node = sync
        .namespace_status(sub)
        .await
        .unwrap()
        .intent
        .operation
        .node_id();
    let baseline = authority.observe_revision(revision).await.unwrap();
    let file = sync
        .stage_create_file(
            sub_node,
            "file",
            baseline.revision(),
            FileMetadata::new(true),
            &mut &b"remote executable"[..],
        )
        .await
        .unwrap();
    let revision = sync.publish(file).await.unwrap();
    let baseline = authority.observe_revision(revision).await.unwrap();
    let empty = sync
        .stage_create_file(
            sub_node,
            "empty",
            baseline.revision(),
            FileMetadata::new(false),
            &mut &b""[..],
        )
        .await
        .unwrap();
    sync.publish(empty).await.unwrap();
}

async fn exercise_initial_pull(authority: Arc<dyn Authority>) {
    let temp = tempfile::tempdir().unwrap();
    seed_remote_tree(&authority).await;
    let root = temp.path().join("payload");
    let state = temp.path().join("state");
    std::fs::create_dir_all(&root).unwrap();

    let mut sync = DirectorySync::init(authority.clone(), &state, &root)
        .await
        .unwrap();
    let first = sync.run().await.unwrap();
    assert_eq!(first.published_local, 0);
    assert_eq!(first.materialized_remote, 4);
    assert!(
        first.conflicts.is_empty(),
        "initial pull conflicts: {:?}",
        first.conflicts
    );
    assert!(first.in_sync);
    let local = local_tree(&root);
    assert_eq!(
        local["dir/sub/file"],
        TreeEntry::File {
            executable: true,
            bytes: b"remote executable".to_vec(),
        }
    );
    assert_eq!(
        local["dir/sub/empty"],
        TreeEntry::File {
            executable: false,
            bytes: Vec::new(),
        }
    );
    assert_eq!(
        local,
        remote_tree(&authority).await,
        "initial pull must converge to the identical real tree"
    );

    let second = sync.run().await.unwrap();
    assert_eq!(second.published_local, 0, "unchanged pull must not publish");
    assert_eq!(
        second.materialized_remote, 0,
        "unchanged pull must not materialize"
    );
    assert!(second.conflicts.is_empty());
    assert!(
        second.in_sync,
        "a converged unchanged pull must report in sync"
    );
    assert_eq!(local_tree(&root), remote_tree(&authority).await);

    drop(sync);
    let mut reopened = DirectorySync::open(authority.clone(), &state)
        .await
        .unwrap();
    let third = reopened.run().await.unwrap();
    assert_eq!(third.published_local, 0);
    assert_eq!(third.materialized_remote, 0);
    assert!(third.conflicts.is_empty());
    assert!(
        third.in_sync,
        "a reopened converged sync must report in sync"
    );
    assert_eq!(
        local_tree(&root),
        remote_tree(&authority).await,
        "a reopened sync must stay converged"
    );
}

async fn exercise_initial_push(authority: Arc<dyn Authority>) {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("payload");
    let state = temp.path().join("state");
    let sub = root.join("dir/sub");
    std::fs::create_dir_all(&sub).unwrap();
    let file = sub.join("file");
    std::fs::write(&file, b"local executable").unwrap();
    std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o755)).unwrap();
    std::fs::write(sub.join("empty"), b"").unwrap();

    let mut sync = DirectorySync::init(authority.clone(), &state, &root)
        .await
        .unwrap();
    let first = sync.run().await.unwrap();
    assert_eq!(first.published_local, 4);
    assert_eq!(first.materialized_remote, 0);
    assert!(
        first.conflicts.is_empty(),
        "initial push conflicts: {:?}",
        first.conflicts
    );
    assert!(first.in_sync);
    assert_eq!(
        local_tree(&root),
        remote_tree(&authority).await,
        "initial push must converge to the identical real tree"
    );

    let second = sync.run().await.unwrap();
    assert_eq!(second.published_local, 0, "unchanged push must not publish");
    assert_eq!(
        second.materialized_remote, 0,
        "unchanged push must not materialize"
    );
    assert!(second.conflicts.is_empty());
    assert!(
        second.in_sync,
        "a converged unchanged push must report in sync"
    );
    assert_eq!(local_tree(&root), remote_tree(&authority).await);
}

async fn exercise_rename_with_edit(authority: Arc<dyn Authority>) {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("payload");
    let state = temp.path().join("state");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::write(root.join("old"), b"version one").unwrap();

    let mut sync = DirectorySync::init(authority.clone(), &state, &root)
        .await
        .unwrap();
    let first = sync.run().await.unwrap();
    assert_eq!(first.published_local, 1);
    assert!(first.conflicts.is_empty());
    assert_eq!(
        local_tree(&root),
        remote_tree(&authority).await,
        "the file must converge at the old name before it is renamed"
    );

    // Rename and rewrite before a single run. The rename receipt must not
    // acknowledge v2 bytes that the namespace intent never published.
    std::fs::rename(root.join("old"), root.join("renamed")).unwrap();
    std::fs::write(root.join("renamed"), b"version two").unwrap();
    for _ in 0..8 {
        let outcome = sync.run().await.unwrap();
        assert!(
            outcome.conflicts.is_empty(),
            "rename/edit run conflicts: {:?}",
            outcome.conflicts
        );
        if local_tree(&root) == remote_tree(&authority).await {
            break;
        }
    }
    let local = local_tree(&root);
    let remote = remote_tree(&authority).await;
    assert_eq!(
        local, remote,
        "the renamed and re-edited local file must converge to the new name and bytes"
    );
    assert_eq!(
        local["renamed"],
        TreeEntry::File {
            executable: false,
            bytes: b"version two".to_vec(),
        }
    );
    assert_eq!(local.get("old"), None);
    assert_eq!(remote.get("old"), None);

    let stable = sync.run().await.unwrap();
    assert_eq!(stable.published_local, 0, "converged run must not publish");
    assert_eq!(
        stable.materialized_remote, 0,
        "converged run must not materialize"
    );
    assert!(stable.conflicts.is_empty());
    assert!(stable.in_sync, "a converged stable run must report in sync");
    assert_eq!(local_tree(&root), remote_tree(&authority).await);
}

async fn service_authority() -> (
    Arc<dyn Authority>,
    tempfile::TempDir,
    tokio::task::JoinHandle<yinyang::core::Result<()>>,
) {
    let backend = TestBackend::default();
    let temp = tempfile::tempdir().unwrap();
    let service = MetadataService::create(temp.path().join("metadata.db"), backend.operator())
        .await
        .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let token = "directory-sync-service-auth-token-0123456789abcdef".to_owned();
    let server = tokio::spawn(serve(listener, service, token.clone()));
    let client = ServiceClient::connect(address, token, backend.operator())
        .await
        .unwrap();
    (Arc::new(client), temp, server)
}

async fn initial_nested_sync_converges_in_object_and_service_modes() {
    exercise_initial_pull(Arc::new(
        Fs::create(TestBackend::default().operator(), BackendProfile::Minio)
            .await
            .unwrap(),
    ))
    .await;
    exercise_initial_push(Arc::new(
        Fs::create(TestBackend::default().operator(), BackendProfile::Minio)
            .await
            .unwrap(),
    ))
    .await;

    let (client, temp, server) = service_authority().await;
    exercise_initial_pull(client).await;
    server.abort();
    drop(temp);

    let (client, temp, server) = service_authority().await;
    exercise_initial_push(client).await;
    server.abort();
    drop(temp);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn renamed_and_re_edited_local_file_converges() {
    exercise_rename_with_edit(Arc::new(
        Fs::create(TestBackend::default().operator(), BackendProfile::Minio)
            .await
            .unwrap(),
    ))
    .await;
}

/// Delegating authority with two explicit hold points. The after-commit mode
/// completes the real commit and then delays the successful response until the
/// test releases it; the before-commit mode parks the request before the real
/// commit so an external edit can land on the authority in between. The
/// original commit outcomes are returned unchanged; lost or Unknown responses
/// are a separate acceptance concern.
#[derive(Debug)]
struct HeldAuthority {
    inner: Arc<dyn Authority>,
    committed: tokio::sync::mpsc::UnboundedSender<()>,
    release: Arc<tokio::sync::Notify>,
    hold_next: Arc<AtomicBool>,
    hold_before_next: Arc<AtomicBool>,
}

impl HeldAuthority {
    fn new(
        inner: Arc<dyn Authority>,
        committed: tokio::sync::mpsc::UnboundedSender<()>,
        release: Arc<tokio::sync::Notify>,
    ) -> Self {
        Self {
            inner,
            committed,
            release,
            hold_next: Arc::new(AtomicBool::new(true)),
            hold_before_next: Arc::new(AtomicBool::new(false)),
        }
    }

    fn new_before_commit(
        inner: Arc<dyn Authority>,
        committed: tokio::sync::mpsc::UnboundedSender<()>,
        release: Arc<tokio::sync::Notify>,
    ) -> Self {
        Self {
            inner,
            committed,
            release,
            hold_next: Arc::new(AtomicBool::new(false)),
            hold_before_next: Arc::new(AtomicBool::new(true)),
        }
    }
}

impl Authority for HeldAuthority {
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
    ) -> Pin<Box<dyn Future<Output = yinyang::core::Result<Snapshot>> + Send + 'a>> {
        self.inner.observe_latest()
    }
    fn observe_revision<'a>(
        &'a self,
        revision: Revision,
    ) -> Pin<Box<dyn Future<Output = yinyang::core::Result<Snapshot>> + Send + 'a>> {
        self.inner.observe_revision(revision)
    }
    fn commit_batch<'a>(
        &'a self,
        requests: &'a [Transaction],
    ) -> Pin<Box<dyn Future<Output = yinyang::core::Result<Vec<CommitOutcome>>> + Send + 'a>> {
        Box::pin(async move {
            if self.hold_before_next.swap(false, Ordering::SeqCst) {
                self.committed.send(()).ok();
                self.release.notified().await;
            }
            let outcomes = self.inner.commit_batch(requests).await?;
            if self.hold_next.swap(false, Ordering::SeqCst) {
                self.committed.send(()).ok();
                self.release.notified().await;
            }
            Ok(outcomes)
        })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn local_edit_during_delayed_publication_receipt_stays_pending() {
    let backend = TestBackend::default();
    let authority: Arc<dyn Authority> = Arc::new(
        Fs::create(backend.operator(), BackendProfile::Minio)
            .await
            .unwrap(),
    );
    let (committed_tx, mut committed_rx) = tokio::sync::mpsc::unbounded_channel();
    let release = Arc::new(tokio::sync::Notify::new());
    let held = Arc::new(HeldAuthority::new(
        authority.clone(),
        committed_tx,
        release.clone(),
    ));

    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("payload");
    let state = temp.path().join("state");
    std::fs::create_dir_all(&root).unwrap();
    let file = root.join("file");
    std::fs::write(&file, b"version one").unwrap();

    let mut sync = DirectorySync::init(held.clone(), &state, &root)
        .await
        .unwrap();
    let run1 = sync.run();
    let writer = async {
        committed_rx
            .recv()
            .await
            .expect("v1 commit must complete before the local edit");
        std::fs::write(&file, b"version two").expect("local v2 write");
        release.notify_one();
    };
    let outcome1 = tokio::time::timeout(std::time::Duration::from_secs(60), async {
        tokio::join!(run1, writer)
    })
    .await
    .expect("v1 publication must commit and release its receipt in time")
    .0
    .unwrap();

    assert_eq!(outcome1.published_local, 1);
    assert!(
        outcome1.conflicts.is_empty(),
        "v1 run conflicts: {:?}",
        outcome1.conflicts
    );
    assert!(
        !outcome1.in_sync,
        "the local v2 edit must not be marked in sync behind a delayed receipt"
    );
    assert!(
        outcome1.pending > 0,
        "the local v2 edit must remain pending until it is published"
    );
    assert_eq!(
        local_tree(&root)["file"],
        TreeEntry::File {
            executable: false,
            bytes: b"version two".to_vec(),
        }
    );
    let remote = remote_tree(&authority).await;
    assert_eq!(
        remote["file"],
        TreeEntry::File {
            executable: false,
            bytes: b"version one".to_vec(),
        },
        "the remote must contain only the version that was actually uploaded"
    );
    drop(sync);

    // The local edit must survive the delayed receipt: a fresh sync publishes v2.
    let mut reopened = DirectorySync::open(held.clone(), &state).await.unwrap();
    let outcome2 = reopened.run().await.unwrap();
    assert!(
        outcome2.published_local >= 1,
        "the second run must publish the pending local version"
    );
    assert!(
        outcome2.conflicts.is_empty(),
        "v2 publish conflicts: {:?}",
        outcome2.conflicts
    );
    assert!(outcome2.in_sync);
    let remote = remote_tree(&authority).await;
    assert_eq!(
        remote["file"],
        TreeEntry::File {
            executable: false,
            bytes: b"version two".to_vec(),
        }
    );

    let outcome3 = reopened.run().await.unwrap();
    assert_eq!(outcome3.published_local, 0);
    assert_eq!(outcome3.materialized_remote, 0);
    assert!(outcome3.conflicts.is_empty());
    assert!(outcome3.in_sync);
    assert_eq!(local_tree(&root), remote);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn interleaved_remote_edit_is_not_overwritten_by_local_run() {
    let backend = TestBackend::default();
    let authority: Arc<dyn Authority> = Arc::new(
        Fs::create(backend.operator(), BackendProfile::Minio)
            .await
            .unwrap(),
    );
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("payload");
    let state = temp.path().join("state");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::write(root.join("a"), b"local-a").unwrap();
    std::fs::write(root.join("b"), b"local-b").unwrap();

    // A common baseline first, without any held commit.
    let mut sync = DirectorySync::init(authority.clone(), &state, &root)
        .await
        .unwrap();
    let first = sync.run().await.unwrap();
    assert_eq!(first.published_local, 2);
    assert!(first.conflicts.is_empty());
    assert_eq!(local_tree(&root), remote_tree(&authority).await);
    drop(sync);

    // The next run stages local edits while the remote is observed unchanged,
    // then parks a's commit before it reaches the real authority.
    std::fs::write(root.join("a"), b"local-a2").unwrap();
    std::fs::write(root.join("b"), b"local-b2").unwrap();
    let (committed_tx, mut committed_rx) = tokio::sync::mpsc::unbounded_channel();
    let release = Arc::new(tokio::sync::Notify::new());
    let held = Arc::new(HeldAuthority::new_before_commit(
        authority.clone(),
        committed_tx,
        release.clone(),
    ));
    let mut sync = DirectorySync::open(held.clone(), &state).await.unwrap();
    let run = sync.run();
    let writer = async {
        committed_rx
            .recv()
            .await
            .expect("a commit must reach the pre-commit barrier");
        // The external edit goes through a public Sync journal on the
        // independent raw authority while a's commit is parked.
        let external = tempfile::tempdir().unwrap();
        let seed = Sync::open(authority.clone(), external.path().join("seed"))
            .await
            .unwrap();
        let baseline = authority.observe_latest().await.unwrap();
        let b = baseline.resolve("b").await.unwrap().unwrap().id();
        assert_eq!(
            baseline
                .open_file(b)
                .await
                .unwrap()
                .read(0, 64)
                .await
                .unwrap(),
            b"local-b",
            "the run must observe the remote unchanged before the interleave"
        );
        let edit = seed
            .stage(b, baseline.revision(), &mut &b"external-b"[..])
            .await
            .unwrap();
        seed.publish(edit).await.unwrap();
        release.notify_one();
    };
    let run_result = tokio::time::timeout(std::time::Duration::from_secs(60), async {
        tokio::join!(run, writer)
    })
    .await
    .expect("the interleaved run must finish within the timeout")
    .0;
    drop(sync);
    match run_result {
        Ok(outcome) => assert!(
            !outcome.in_sync,
            "a run observing a competing remote edit must not report in sync"
        ),
        Err(_) => {}
    }

    // Data safety: a's own edit landed, external-b survived, local b is kept.
    let local = local_tree(&root);
    let remote = remote_tree(&authority).await;
    assert_eq!(
        remote["a"],
        TreeEntry::File {
            executable: false,
            bytes: b"local-a2".to_vec(),
        },
        "the local a edit must be published"
    );
    assert_eq!(
        remote["b"],
        TreeEntry::File {
            executable: false,
            bytes: b"external-b".to_vec(),
        },
        "the interleaved external edit must not be overwritten"
    );
    assert_eq!(
        local["a"],
        TreeEntry::File {
            executable: false,
            bytes: b"local-a2".to_vec(),
        }
    );
    assert_eq!(
        local["b"],
        TreeEntry::File {
            executable: false,
            bytes: b"local-b2".to_vec(),
        },
        "the local b edit must survive the interleaved run"
    );

    // Read-only inspection must expose a resolvable conflict, never in-sync.
    let inspected = DirectorySync::inspect(authority.clone(), &state)
        .await
        .unwrap();
    assert!(
        !inspected.conflicts.is_empty(),
        "the competing external edit must surface as a resolvable conflict"
    );
    assert!(
        inspected
            .conflicts
            .iter()
            .all(|conflict| !conflict.id.is_empty())
    );

    // The conflict is resolvable: keeping the remote side adopts the external
    // bytes locally, preserves them remotely, and returns to in_sync.
    let conflict = inspected.conflicts[0].clone();
    let mut resolver = DirectorySync::open(authority.clone(), &state)
        .await
        .unwrap();
    resolver
        .resolve(&conflict.id, Resolution::KeepRemote)
        .await
        .unwrap();
    let local = local_tree(&root);
    let remote = remote_tree(&authority).await;
    assert_eq!(
        local["b"],
        TreeEntry::File {
            executable: false,
            bytes: b"external-b".to_vec(),
        },
        "keep-remote must adopt the external bytes locally"
    );
    assert_eq!(
        remote["b"],
        TreeEntry::File {
            executable: false,
            bytes: b"external-b".to_vec(),
        },
        "the external remote edit must survive resolution"
    );
    drop(resolver);

    let mut reopened = DirectorySync::open(authority.clone(), &state)
        .await
        .unwrap();
    let stable = reopened.run().await.unwrap();
    assert!(
        stable.in_sync,
        "a resolved interleaving must reach in_sync on the next run"
    );
    let final_status = DirectorySync::inspect(authority.clone(), &state)
        .await
        .unwrap();
    assert!(final_status.conflicts.is_empty());
    assert!(final_status.summary.in_sync);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn local_deletion_during_delayed_publication_receipt_stays_pending() {
    let backend = TestBackend::default();
    let authority: Arc<dyn Authority> = Arc::new(
        Fs::create(backend.operator(), BackendProfile::Minio)
            .await
            .unwrap(),
    );
    let (committed_tx, mut committed_rx) = tokio::sync::mpsc::unbounded_channel();
    let release = Arc::new(tokio::sync::Notify::new());
    let held = Arc::new(HeldAuthority::new(
        authority.clone(),
        committed_tx,
        release.clone(),
    ));

    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("payload");
    let state = temp.path().join("state");
    std::fs::create_dir_all(&root).unwrap();
    let file = root.join("file");
    std::fs::write(&file, b"version one").unwrap();

    let mut sync = DirectorySync::init(held.clone(), &state, &root)
        .await
        .unwrap();
    let run1 = sync.run();
    let writer = async {
        committed_rx
            .recv()
            .await
            .expect("v1 commit must complete before the local delete");
        std::fs::remove_file(&file).expect("local delete");
        release.notify_one();
    };
    let outcome1 = tokio::time::timeout(std::time::Duration::from_secs(60), async {
        tokio::join!(run1, writer)
    })
    .await
    .expect("v1 publication must commit and release its receipt in time")
    .0
    .unwrap();

    assert_eq!(outcome1.published_local, 1);
    assert!(
        outcome1.conflicts.is_empty(),
        "v1 run conflicts: {:?}",
        outcome1.conflicts
    );
    assert!(
        !outcome1.in_sync,
        "the local deletion must not be marked in sync behind a delayed receipt"
    );
    assert!(
        outcome1.pending > 0,
        "the local deletion must remain pending until it is published"
    );
    assert!(
        !file.exists(),
        "the delayed receipt must not resurrect the deleted local file"
    );
    let remote = remote_tree(&authority).await;
    assert_eq!(
        remote["file"],
        TreeEntry::File {
            executable: false,
            bytes: b"version one".to_vec(),
        },
        "the remote must still hold only the uploaded v1"
    );
    drop(sync);

    // The accepted v1 baseline must survive the delayed receipt so the next
    // run publishes the local deletion instead of materializing v1 back.
    let mut reopened = DirectorySync::open(held.clone(), &state).await.unwrap();
    let outcome2 = reopened.run().await.unwrap();
    assert!(
        outcome2.published_local >= 1,
        "the next run must publish the local deletion"
    );
    assert!(
        outcome2.conflicts.is_empty(),
        "deletion publish conflicts: {:?}",
        outcome2.conflicts
    );
    let remote = remote_tree(&authority).await;
    assert_eq!(
        remote.get("file"),
        None,
        "the published deletion must remove the remote file"
    );
    assert!(!file.exists());

    let outcome3 = reopened.run().await.unwrap();
    assert_eq!(outcome3.published_local, 0);
    assert_eq!(outcome3.materialized_remote, 0);
    assert!(outcome3.conflicts.is_empty());
    assert!(
        outcome3.in_sync,
        "a converged run after the published deletion must report in sync"
    );
    assert_eq!(local_tree(&root), remote);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn namespace_only_rename_receipt_keeps_interleaved_content() {
    let backend = TestBackend::default();
    let authority: Arc<dyn Authority> = Arc::new(
        Fs::create(backend.operator(), BackendProfile::Minio)
            .await
            .unwrap(),
    );
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("payload");
    let state = temp.path().join("state");
    let dir = root.join("dir");
    let file = dir.join("file");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(&file, b"v1").unwrap();

    // A common baseline for the directory subtree first.
    let mut sync = DirectorySync::init(authority.clone(), &state, &root)
        .await
        .unwrap();
    let first = sync.run().await.unwrap();
    assert_eq!(first.published_local, 2);
    assert!(first.conflicts.is_empty());
    assert_eq!(local_tree(&root), remote_tree(&authority).await);
    drop(sync);

    // The next run renames the directory locally while the remote is observed
    // unchanged, then parks the rename commit before it executes.
    std::fs::rename(&dir, root.join("moved")).unwrap();
    let (committed_tx, mut committed_rx) = tokio::sync::mpsc::unbounded_channel();
    let release = Arc::new(tokio::sync::Notify::new());
    let held = Arc::new(HeldAuthority::new_before_commit(
        authority.clone(),
        committed_tx,
        release.clone(),
    ));
    let mut sync = DirectorySync::open(held.clone(), &state).await.unwrap();
    let run = sync.run();
    let writer = async {
        committed_rx
            .recv()
            .await
            .expect("the rename commit must reach the pre-commit barrier");
        // The interleaved content edit goes through the independent raw
        // authority while the namespace-only rename is parked. The rename
        // predicates guard links and entries, never the inner file content,
        // so this exact schedule commits and later materializes v2.
        let external = tempfile::tempdir().unwrap();
        let seed = Sync::open(authority.clone(), external.path().join("seed"))
            .await
            .unwrap();
        let baseline = authority.observe_latest().await.unwrap();
        let file = baseline.resolve("dir/file").await.unwrap().unwrap().id();
        assert_eq!(
            baseline
                .open_file(file)
                .await
                .unwrap()
                .read(0, 64)
                .await
                .unwrap(),
            b"v1",
            "the run must observe the remote unchanged before the interleave"
        );
        let edit = seed
            .stage(
                file,
                baseline.revision(),
                &mut &b"v2-was-never-seen-locally"[..],
            )
            .await
            .unwrap();
        seed.publish(edit).await.unwrap();
        release.notify_one();
    };
    let outcome1 = tokio::time::timeout(std::time::Duration::from_secs(60), async {
        tokio::join!(run, writer)
    })
    .await
    .expect("the interleaved rename run must finish within the timeout")
    .0
    .unwrap_or_else(|error| panic!("the audited rename interleave must commit: {error}"));

    assert!(
        outcome1.conflicts.is_empty(),
        "the audited rename interleave must not conflict: {:?}",
        outcome1.conflicts
    );
    let local1 = local_tree(&root);
    let remote1 = remote_tree(&authority).await;
    assert!(
        remote1.get("dir").is_none(),
        "the accepted rename must move the directory out of the old name"
    );
    assert_eq!(
        remote1["moved/file"],
        TreeEntry::File {
            executable: false,
            bytes: b"v2-was-never-seen-locally".to_vec(),
        },
        "the namespace rename receipt must not swallow the interleaved external content"
    );
    assert_eq!(
        local1["moved/file"],
        TreeEntry::File {
            executable: false,
            bytes: b"v1".to_vec(),
        },
        "the local v1 must stay untouched until the next run materializes v2"
    );
    assert!(
        !outcome1.in_sync || local1 == remote1,
        "in_sync must imply identical real trees"
    );
    assert!(
        !outcome1.in_sync,
        "the unobserved remote v2 must stay pending behind the rename receipt"
    );

    // A reopened sync materializes the retained v2 and then runs stable.
    drop(sync);
    let mut reopened = DirectorySync::open(held.clone(), &state).await.unwrap();
    let outcome2 = reopened.run().await.unwrap();
    assert!(outcome2.conflicts.is_empty());
    let local2 = local_tree(&root);
    let remote2 = remote_tree(&authority).await;
    assert_eq!(
        local2, remote2,
        "the next run must converge the moved subtree to the interleaved v2"
    );
    assert_eq!(
        local2["moved/file"],
        TreeEntry::File {
            executable: false,
            bytes: b"v2-was-never-seen-locally".to_vec(),
        }
    );
    assert!(local2.get("dir").is_none());

    let outcome3 = reopened.run().await.unwrap();
    assert_eq!(outcome3.published_local, 0);
    assert_eq!(outcome3.materialized_remote, 0);
    assert!(outcome3.conflicts.is_empty());
    assert!(
        outcome3.in_sync,
        "the converged renamed subtree must report in sync"
    );
    assert_eq!(local_tree(&root), remote_tree(&authority).await);
}
