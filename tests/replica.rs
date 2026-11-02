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

use std::collections::BTreeMap;
use std::os::fd::BorrowedFd;
use std::path::{Path, PathBuf};
use yinyang::sync_engine::{
    EntryKind, FrozenRemoteRequest, PreparedRemoteRequest, RemoteAdapter, RemoteBaseline,
    RemoteCapabilities, RemoteCursor, RemoteEntry, RemoteObservation, RemoteOutcome, RemotePin,
    RemoteRequest, SyncEngine, SyncModel, inspect_state,
};

#[derive(Clone)]
struct NoopAdapter {
    namespace: String,
}

impl NoopAdapter {
    fn new(namespace: &str) -> Self {
        Self {
            namespace: namespace.to_owned(),
        }
    }

    fn root_entry() -> RemoteEntry {
        RemoteEntry {
            path: String::new(),
            kind: EntryKind::Directory,
            length: 0,
            digest: None,
            executable: false,
            pin: RemotePin::Direct {
                observation: [0; 16],
                version: Some("empty".to_owned()),
                etag: Some("empty".to_owned()),
                size: 0,
                modified: None,
            },
        }
    }
}

impl RemoteAdapter for NoopAdapter {
    fn model(&self) -> SyncModel {
        SyncModel::Direct
    }

    fn namespace_id(&self) -> String {
        self.namespace.clone()
    }

    fn capabilities(&self) -> RemoteCapabilities {
        RemoteCapabilities::for_model(SyncModel::Direct)
    }

    async fn preflight(&self, _state: &Path) -> yinyang::sync_engine::Result<()> {
        Ok(())
    }

    async fn bind(&self, _state: &Path) -> yinyang::sync_engine::Result<()> {
        Ok(())
    }

    async fn observe(
        &self,
        _baseline: Option<&RemoteBaseline>,
    ) -> yinyang::sync_engine::Result<RemoteObservation> {
        Ok(RemoteObservation {
            cursor: RemoteCursor { bytes: vec![0; 27] },
            revision: None,
            entries: BTreeMap::from([(String::new(), Self::root_entry())]),
        })
    }

    async fn read_file(
        &self,
        _entry: &RemoteEntry,
        _output: BorrowedFd<'_>,
    ) -> yinyang::sync_engine::Result<()> {
        unreachable!("no remote file exists")
    }

    async fn freeze(
        &self,
        _request: RemoteRequest,
    ) -> yinyang::sync_engine::Result<FrozenRemoteRequest> {
        unreachable!("invalid local input must fail before request preparation")
    }

    async fn receipt(
        &self,
        _request: &FrozenRemoteRequest,
    ) -> yinyang::sync_engine::Result<Option<RemoteOutcome>> {
        Ok(None)
    }

    async fn publish_batch(
        &self,
        _requests: &[PreparedRemoteRequest],
    ) -> yinyang::sync_engine::Result<Vec<RemoteOutcome>> {
        unreachable!("invalid local input must fail before publication")
    }
}

async fn initialize(
    temp: &tempfile::TempDir,
    namespace: &str,
) -> (SyncEngine<NoopAdapter>, PathBuf, PathBuf) {
    let root = temp.path().join("root");
    let state = temp.path().join("state");
    std::fs::create_dir_all(&root).unwrap();
    let engine = SyncEngine::init(NoopAdapter::new(namespace), &state, &root)
        .await
        .unwrap();
    (engine, root, state)
}

#[tokio::test]
async fn binding_rejects_namespace_substitution() {
    let temp = tempfile::tempdir().unwrap();
    let (engine, _, state) = initialize(&temp, "first").await;
    drop(engine);
    let error = SyncEngine::open(NoopAdapter::new("second"), &state)
        .await
        .err()
        .unwrap();
    assert_eq!(error.kind(), yinyang::sync_engine::ErrorKind::Invalid);
}

#[tokio::test]
async fn state_lease_rejects_concurrent_engines() {
    let temp = tempfile::tempdir().unwrap();
    let (_engine, _, state) = initialize(&temp, "namespace").await;
    let error = SyncEngine::open(NoopAdapter::new("namespace"), &state)
        .await
        .err()
        .unwrap();
    assert_eq!(error.kind(), yinyang::sync_engine::ErrorKind::Busy);
}

#[tokio::test]
async fn read_only_inspection_rejects_a_replaced_root() {
    let temp = tempfile::tempdir().unwrap();
    let (_engine, root, state) = initialize(&temp, "namespace").await;
    std::fs::rename(&root, temp.path().join("root-old")).unwrap();
    std::fs::create_dir(&root).unwrap();
    let error = inspect_state(&state).unwrap_err();
    assert_eq!(error.kind(), yinyang::sync_engine::ErrorKind::Invalid);
}

#[tokio::test]
async fn local_scan_rejects_symlinks_and_hard_links() {
    for hard_link in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let (mut engine, root, _) = initialize(&temp, "namespace").await;
        if hard_link {
            std::fs::write(root.join("file"), b"bytes").unwrap();
            std::fs::hard_link(root.join("file"), root.join("other")).unwrap();
        } else {
            #[cfg(target_os = "linux")]
            std::os::unix::fs::symlink("missing", root.join("link")).unwrap();
        }
        let error = engine.run().await.unwrap_err();
        assert_eq!(error.kind(), yinyang::sync_engine::ErrorKind::Unsupported);
    }
}
