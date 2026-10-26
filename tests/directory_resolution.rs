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

//! Public-API regressions for explicit Managed Sync conflict resolution:
//! deletion, directory-subtree and rename decisions against the recorded
//! remote side, with full two-sided tree and final no-op assertions.

#![cfg(target_os = "linux")]

#[path = "../crates/core/tests/support/mod.rs"]
mod support;

use std::collections::BTreeMap;
use std::os::linux::fs::MetadataExt;
use std::path::Path;
use std::sync::Arc;
use support::TestBackend;
use yinyang::core::{Authority, BackendProfile, Fs};
use yinyang::replica::{Conflict, DirectorySync, Resolution, RunOutcome};
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

/// A short-lived public Sync journal used for an independent remote edit.
async fn remote_client(authority: &Arc<dyn Authority>) -> (Sync, tempfile::TempDir) {
    let temp = tempfile::tempdir().unwrap();
    let sync = Sync::open(authority.clone(), temp.path().join("seed"))
        .await
        .unwrap();
    (sync, temp)
}

async fn remote_write(authority: &Arc<dyn Authority>, path: &str, bytes: &[u8]) {
    let (sync, _temp) = remote_client(authority).await;
    let baseline = authority.observe_latest().await.unwrap();
    let node = baseline.resolve(path).await.unwrap().unwrap().id();
    let edit = sync
        .stage(node, baseline.revision(), &mut &bytes[..])
        .await
        .unwrap();
    sync.publish(edit).await.unwrap();
}

async fn remote_create_file(authority: &Arc<dyn Authority>, path: &str, bytes: &[u8]) {
    let (sync, _temp) = remote_client(authority).await;
    let (parent, name) = path.rsplit_once('/').unwrap_or(("", path));
    let baseline = authority.observe_latest().await.unwrap();
    let parent_node = baseline.resolve(parent).await.unwrap().unwrap().id();
    let edit = sync
        .stage_create_file(
            parent_node,
            name,
            baseline.revision(),
            FileMetadata::new(false),
            &mut &bytes[..],
        )
        .await
        .unwrap();
    sync.publish(edit).await.unwrap();
}

async fn remote_remove(authority: &Arc<dyn Authority>, path: &str) {
    let (sync, _temp) = remote_client(authority).await;
    let baseline = authority.observe_latest().await.unwrap();
    let node = baseline.resolve(path).await.unwrap().unwrap().id();
    let edit = sync.stage_remove(node, baseline.revision()).await.unwrap();
    sync.publish(edit).await.unwrap();
}

async fn remote_create_directory(authority: &Arc<dyn Authority>, path: &str) {
    let (sync, _temp) = remote_client(authority).await;
    let (parent, name) = path.rsplit_once('/').unwrap_or(("", path));
    let baseline = authority.observe_latest().await.unwrap();
    let parent_node = baseline.resolve(parent).await.unwrap().unwrap().id();
    let edit = sync
        .stage_create_directory(parent_node, name, baseline.revision())
        .await
        .unwrap();
    sync.publish(edit).await.unwrap();
}

fn conflict_at<'a>(outcome: &'a RunOutcome, path: &str) -> &'a Conflict {
    outcome
        .conflicts
        .iter()
        .find(|conflict| conflict.path == path)
        .unwrap_or_else(|| panic!("missing conflict for {path:?}: {:?}", outcome.conflicts))
}

async fn binding(authority: &Arc<dyn Authority>, root: &Path, state: &Path) -> DirectorySync {
    std::fs::create_dir_all(root).unwrap();
    DirectorySync::init(authority.clone(), state, root)
        .await
        .unwrap()
}

async fn assert_noop_in_sync(sync: &mut DirectorySync) {
    let outcome = sync.run().await.unwrap();
    assert_eq!(outcome.published_local, 0, "settled run must not publish");
    assert_eq!(
        outcome.materialized_remote, 0,
        "settled run must not materialize"
    );
    assert!(
        outcome.conflicts.is_empty(),
        "settled run conflicts: {:?}",
        outcome.conflicts
    );
    assert!(outcome.in_sync, "settled run must report in sync");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn local_deletion_vs_remote_edit_resolves_both_ways() {
    for keep in [true, false] {
        let backend = TestBackend::default();
        let authority: Arc<dyn Authority> = Arc::new(
            Fs::create(backend.operator(), BackendProfile::Minio)
                .await
                .unwrap(),
        );
        let temp = tempfile::tempdir().unwrap();
        let root = temp
            .path()
            .join(if keep { "keep-local" } else { "keep-remote" });
        let state = temp.path().join(if keep {
            "keep-local-state"
        } else {
            "keep-remote-state"
        });
        remote_create_file(&authority, "file.txt", b"base").await;

        let mut sync = binding(&authority, &root, &state).await;
        let pulled = sync.run().await.unwrap();
        assert!(pulled.conflicts.is_empty(), "{pulled:?}");
        assert_eq!(local_tree(&root), remote_tree(&authority).await);

        std::fs::remove_file(root.join("file.txt")).unwrap();
        remote_write(&authority, "file.txt", b"remote-v2").await;

        let conflicted = sync.run().await.unwrap();
        let conflict = conflict_at(&conflicted, "file.txt");
        assert_eq!(conflict.original_path.as_deref(), Some("file.txt"));
        assert_eq!(conflict.desired_path.as_deref(), Some("file.txt"));
        assert_eq!(conflict.local, "deleted");
        assert_eq!(conflict.remote, "current_file");

        let resolved = sync
            .resolve(
                &conflict.id,
                if keep {
                    Resolution::KeepLocal
                } else {
                    Resolution::KeepRemote
                },
            )
            .await
            .unwrap();
        assert!(
            resolved.conflicts.is_empty(),
            "resolution constraints: {:?}",
            resolved.conflicts
        );
        assert_noop_in_sync(&mut sync).await;

        if keep {
            assert!(
                !root.join("file.txt").exists(),
                "keep-local must keep the local deletion"
            );
            assert!(
                remote_tree(&authority).await.get("file.txt").is_none(),
                "keep-local must publish the deletion to the remote side"
            );
        } else {
            assert_eq!(
                std::fs::read(root.join("file.txt")).unwrap(),
                b"remote-v2",
                "keep-remote must materialize the recorded remote version"
            );
            assert_eq!(
                remote_tree(&authority).await["file.txt"],
                TreeEntry::File {
                    executable: false,
                    bytes: b"remote-v2".to_vec(),
                }
            );
        }
        assert_eq!(local_tree(&root), remote_tree(&authority).await);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn local_directory_tree_kept_over_remote_deletion() {
    let backend = TestBackend::default();
    let authority: Arc<dyn Authority> = Arc::new(
        Fs::create(backend.operator(), BackendProfile::Minio)
            .await
            .unwrap(),
    );
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("payload");
    let state = temp.path().join("state");
    remote_create_directory(&authority, "dir").await;
    remote_create_directory(&authority, "dir/sub").await;
    remote_create_file(&authority, "dir/sub/file", b"old").await;
    remote_create_file(&authority, "dir/empty", b"").await;

    let mut sync = binding(&authority, &root, &state).await;
    let pulled = sync.run().await.unwrap();
    assert!(pulled.conflicts.is_empty(), "{pulled:?}");
    assert_eq!(local_tree(&root), remote_tree(&authority).await);

    std::fs::write(root.join("dir/sub/file"), b"new").unwrap();
    for path in ["dir/empty", "dir/sub/file", "dir/sub", "dir"] {
        remote_remove(&authority, path).await;
    }

    let conflicted = sync.run().await.unwrap();
    assert!(
        !conflicted.conflicts.is_empty(),
        "a diverging local tree under a remote-deleted directory must conflict: {conflicted:?}"
    );
    let dir = conflict_at(&conflicted, "dir");
    assert_eq!(dir.local, "current_directory");
    assert_eq!(dir.remote, "deleted");

    let resolved = sync.resolve(&dir.id, Resolution::KeepLocal).await.unwrap();
    assert!(
        resolved.conflicts.is_empty(),
        "keep-local subtree must resolve the whole directory: {:?}",
        resolved.conflicts
    );
    assert_noop_in_sync(&mut sync).await;

    let local = local_tree(&root);
    let remote = remote_tree(&authority).await;
    assert_eq!(
        local["dir/sub/file"],
        TreeEntry::File {
            executable: false,
            bytes: b"new".to_vec(),
        }
    );
    assert_eq!(
        local["dir/empty"],
        TreeEntry::File {
            executable: false,
            bytes: Vec::new(),
        }
    );
    assert_eq!(
        local, remote,
        "keep-local must republish the entire local directory tree"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn local_directory_deletion_keeps_remote_children() {
    let backend = TestBackend::default();
    let authority: Arc<dyn Authority> = Arc::new(
        Fs::create(backend.operator(), BackendProfile::Minio)
            .await
            .unwrap(),
    );
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("payload");
    let state = temp.path().join("state");
    remote_create_directory(&authority, "dir").await;
    remote_create_file(&authority, "dir/x", b"old").await;

    let mut sync = binding(&authority, &root, &state).await;
    let pulled = sync.run().await.unwrap();
    assert!(pulled.conflicts.is_empty(), "{pulled:?}");

    std::fs::remove_file(root.join("dir/x")).unwrap();
    std::fs::remove_dir(root.join("dir")).unwrap();
    // The remote side replaces the directory with a new tree.
    remote_remove(&authority, "dir/x").await;
    remote_remove(&authority, "dir").await;
    remote_create_directory(&authority, "dir").await;
    remote_create_file(&authority, "dir/a", b"new-a").await;

    let conflicted = sync.run().await.unwrap();
    let dir = conflict_at(&conflicted, "dir");
    assert_eq!(dir.local, "deleted");
    assert_eq!(dir.remote, "current_directory");

    // A fresh local input after the conflict must survive keep-remote.
    std::fs::create_dir_all(root.join("dir")).unwrap();
    std::fs::write(root.join("dir/new-local.txt"), b"keep").unwrap();

    let resolved = sync.resolve(&dir.id, Resolution::KeepRemote).await.unwrap();
    assert!(
        resolved.conflicts.is_empty(),
        "keep-remote subtree must resolve the directory: {:?}",
        resolved.conflicts
    );

    let local = local_tree(&root);
    let remote = remote_tree(&authority).await;
    assert_eq!(
        local["dir/a"],
        TreeEntry::File {
            executable: false,
            bytes: b"new-a".to_vec(),
        },
        "keep-remote must materialize the recorded remote children"
    );
    assert_eq!(
        local["dir/new-local.txt"],
        TreeEntry::File {
            executable: false,
            bytes: b"keep".to_vec(),
        },
        "a new local input must not be dropped by keep-remote"
    );
    assert_eq!(local.get("dir/x"), None);
    assert_eq!(
        local, remote,
        "the fresh local input must converge alongside the kept remote tree"
    );
    assert_noop_in_sync(&mut sync).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn renamed_and_rename_rename_conflicts_keep_local() {
    // Rename elsewhere while the local file is edited: keep-local republishes
    // the local bytes under the recorded name.
    let backend = TestBackend::default();
    let authority: Arc<dyn Authority> = Arc::new(
        Fs::create(backend.operator(), BackendProfile::Minio)
            .await
            .unwrap(),
    );
    let temp = tempfile::tempdir().unwrap();
    remote_create_file(&authority, "source.txt", b"base").await;

    let left_root = temp.path().join("rename-edit-left");
    let right_root = temp.path().join("rename-edit-right");
    let mut left = binding(&authority, &left_root, &temp.path().join("left-state")).await;
    let mut right = binding(&authority, &right_root, &temp.path().join("right-state")).await;
    left.run().await.unwrap();
    right.run().await.unwrap();

    std::fs::rename(left_root.join("source.txt"), left_root.join("moved.txt")).unwrap();
    std::fs::write(right_root.join("source.txt"), b"edited").unwrap();
    left.run().await.unwrap();
    let conflicted = right.run().await.unwrap();
    let conflict = conflict_at(&conflicted, "source.txt");
    assert_eq!(conflict.original_path.as_deref(), Some("source.txt"));
    assert_eq!(conflict.desired_path.as_deref(), Some("source.txt"));
    assert_eq!(conflict.local, "current_file");
    assert_eq!(conflict.remote, "deleted");

    right
        .resolve(&conflict.id, Resolution::KeepLocal)
        .await
        .unwrap();
    assert_noop_in_sync(&mut right).await;
    let remote = remote_tree(&authority).await;
    assert_eq!(
        remote["source.txt"],
        TreeEntry::File {
            executable: false,
            bytes: b"edited".to_vec(),
        }
    );
    assert_eq!(
        remote["moved.txt"],
        TreeEntry::File {
            executable: false,
            bytes: b"base".to_vec(),
        }
    );
    assert_eq!(local_tree(&right_root), remote);

    // Rename against rename: keep-local must confirm the recorded local name
    // or republish it without ever failing against a stale predicate.
    let backend = TestBackend::default();
    let authority: Arc<dyn Authority> = Arc::new(
        Fs::create(backend.operator(), BackendProfile::Minio)
            .await
            .unwrap(),
    );
    let temp = tempfile::tempdir().unwrap();
    remote_create_file(&authority, "source.txt", b"base").await;

    let left_root = temp.path().join("rename-left");
    let right_root = temp.path().join("rename-right");
    let mut left = binding(&authority, &left_root, &temp.path().join("left-state")).await;
    let mut right = binding(&authority, &right_root, &temp.path().join("right-state")).await;
    left.run().await.unwrap();
    right.run().await.unwrap();

    std::fs::rename(
        left_root.join("source.txt"),
        left_root.join("left-name.txt"),
    )
    .unwrap();
    std::fs::rename(
        right_root.join("source.txt"),
        right_root.join("right-name.txt"),
    )
    .unwrap();
    left.run().await.unwrap();
    let conflicted = right.run().await.unwrap();
    let conflict = conflict_at(&conflicted, "source.txt");
    assert_eq!(conflict.original_path.as_deref(), Some("source.txt"));
    assert_eq!(conflict.desired_path.as_deref(), Some("right-name.txt"));
    assert_eq!(conflict.local, "current_file");
    assert_eq!(conflict.remote, "deleted");

    right
        .resolve(&conflict.id, Resolution::KeepLocal)
        .await
        .unwrap();
    assert_noop_in_sync(&mut right).await;
    let remote = remote_tree(&authority).await;
    assert_eq!(
        remote["right-name.txt"],
        TreeEntry::File {
            executable: false,
            bytes: b"base".to_vec(),
        }
    );
    assert_eq!(
        remote["left-name.txt"],
        TreeEntry::File {
            executable: false,
            bytes: b"base".to_vec(),
        }
    );
    assert_eq!(local_tree(&right_root), remote);
}
