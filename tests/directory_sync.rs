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
use yinyang::replica::DirectorySync;
use yinyang::sync::{FileMetadata, NamespaceOperation, Sync};

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
                    executable: metadata.mode() & 0o111 != 0,
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
    assert_eq!(local_tree(&root), remote_tree(&authority).await);

    drop(sync);
    let mut reopened = DirectorySync::open(authority.clone(), &state)
        .await
        .unwrap();
    let third = reopened.run().await.unwrap();
    assert_eq!(third.published_local, 0);
    assert_eq!(third.materialized_remote, 0);
    assert!(third.conflicts.is_empty());
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
    let token = "directory-sync-service-token".to_owned();
    let server = tokio::spawn(serve(listener, service, token.clone()));
    let client = ServiceClient::connect(address, token, backend.operator())
        .await
        .unwrap();
    (Arc::new(client), temp, server)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
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

/// Delegating authority that completes the real commit and then parks the
/// response until the test releases it, modeling a lost publication receipt.
#[derive(Debug)]
struct HeldAuthority {
    inner: Arc<dyn Authority>,
    committed: tokio::sync::mpsc::UnboundedSender<()>,
    release: Arc<tokio::sync::Notify>,
    hold_next: Arc<AtomicBool>,
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
async fn local_edit_during_lost_publication_receipt_stays_pending() {
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
    let (outcome1, ()) = tokio::join!(run1, writer);
    let outcome1 = outcome1.unwrap();

    assert_eq!(outcome1.published_local, 1);
    assert!(
        outcome1.conflicts.is_empty(),
        "v1 run conflicts: {:?}",
        outcome1.conflicts
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

    // The local edit must survive the parked receipt: a fresh sync publishes v2.
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
    assert_eq!(local_tree(&root), remote);
}
