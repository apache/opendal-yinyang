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

//! Managed Sync conflict-resolution regressions on the shared engine.

#![cfg(target_os = "linux")]

#[path = "../crates/core/tests/support/mod.rs"]
mod support;

use std::path::Path;
use std::sync::Arc;
use support::TestBackend;
use yinyang::core::{Authority, CommitId, CommitOutcome, Fs, Planner};
use yinyang::sync_engine::{
    ConflictSideStatus, ManagedAdapter, Resolution, StatusDocument, SyncEngine, SyncModel,
};

struct Harness {
    engine: SyncEngine<ManagedAdapter>,
    adapter: ManagedAdapter,
    state: std::path::PathBuf,
}

impl Harness {
    async fn init(authority: Arc<dyn Authority>, state: &Path, root: &Path) -> Self {
        std::fs::create_dir_all(root).unwrap();
        let adapter = ManagedAdapter::new(authority);
        let engine = SyncEngine::init(adapter.clone(), state, root)
            .await
            .unwrap();
        Self {
            engine,
            adapter,
            state: state.to_owned(),
        }
    }

    async fn inspect(&self) -> StatusDocument {
        SyncEngine::<ManagedAdapter>::inspect(
            Some(&self.adapter),
            &self.state,
            SyncModel::Managed,
            None,
        )
        .await
    }
}

async fn remote_create_file(authority: &Arc<dyn Authority>, path: &str, bytes: &[u8]) {
    let baseline = authority.observe_latest().await.unwrap();
    let (parent, name) = path.rsplit_once('/').unwrap_or(("", path));
    let parent = baseline.resolve(parent).await.unwrap().unwrap().id();
    let prepared = authority.data().prepare(&mut &bytes[..]).await.unwrap();
    let mut planner = Planner::new(&baseline, CommitId::generate());
    planner
        .create_file(parent, name, prepared, false)
        .await
        .unwrap();
    assert!(matches!(
        authority.commit(&planner.finish().unwrap()).await.unwrap(),
        CommitOutcome::Committed(_)
    ));
}

async fn remote_write(authority: &Arc<dyn Authority>, path: &str, bytes: &[u8]) {
    let baseline = authority.observe_latest().await.unwrap();
    let node = baseline.resolve(path).await.unwrap().unwrap().id();
    let prepared = authority.data().prepare(&mut &bytes[..]).await.unwrap();
    let mut planner = Planner::new(&baseline, CommitId::generate());
    let _ = planner.node(node).await.unwrap();
    planner.set_content(node, prepared).await.unwrap();
    assert!(matches!(
        authority.commit(&planner.finish().unwrap()).await.unwrap(),
        CommitOutcome::Committed(_)
    ));
}

async fn remote_bytes(authority: &Arc<dyn Authority>, path: &str) -> Option<Vec<u8>> {
    let snapshot = authority.observe_latest().await.unwrap();
    let node = snapshot.resolve(path).await.unwrap()?;
    let file = snapshot.open_file(node.id()).await.unwrap();
    Some(
        file.read(0, file.descriptor().content_id().length() as usize)
            .await
            .unwrap(),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn local_deletion_vs_remote_edit_resolves_both_ways() {
    for keep_local in [true, false] {
        let backend = TestBackend::default();
        let authority: Arc<dyn Authority> = Arc::new(Fs::create(backend.operator()).await.unwrap());
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join(if keep_local {
            "keep-local"
        } else {
            "keep-remote"
        });
        let state = temp.path().join(if keep_local {
            "keep-local-state"
        } else {
            "keep-remote-state"
        });
        remote_create_file(&authority, "file.txt", b"base").await;
        let mut sync = Harness::init(authority.clone(), &state, &root).await;
        assert!(sync.engine.run().await.unwrap().in_sync);

        std::fs::remove_file(root.join("file.txt")).unwrap();
        remote_write(&authority, "file.txt", b"remote-v2").await;
        let outcome = sync.engine.run().await.unwrap();
        assert!(!outcome.in_sync);
        let status = sync.inspect().await;
        let conflict = status
            .conflicts
            .iter()
            .find(|conflict| conflict.path == "file.txt")
            .expect("content conflict");
        assert_eq!(conflict.local, ConflictSideStatus::Deleted);
        assert_eq!(conflict.remote, ConflictSideStatus::CurrentFile);
        assert_eq!(conflict.original_path.as_deref(), Some("file.txt"));
        assert_eq!(conflict.desired_path.as_deref(), Some("file.txt"));

        sync.engine
            .resolve(
                &conflict.id,
                if keep_local {
                    Resolution::KeepLocal
                } else {
                    Resolution::KeepRemote
                },
            )
            .await
            .unwrap();
        let settled = sync.engine.run().await.unwrap();
        assert!(settled.in_sync, "{settled:?}");
        assert!(sync.inspect().await.conflicts.is_empty());
        if keep_local {
            assert!(!root.join("file.txt").exists());
            assert_eq!(remote_bytes(&authority, "file.txt").await, None);
        } else {
            assert_eq!(std::fs::read(root.join("file.txt")).unwrap(), b"remote-v2");
            assert_eq!(
                remote_bytes(&authority, "file.txt").await.unwrap(),
                b"remote-v2"
            );
        }
    }
}
