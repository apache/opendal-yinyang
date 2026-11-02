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
use std::io::Write as _;
use std::os::fd::BorrowedFd;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use yinyang::sync_engine::{
    EntryKind, FrozenRemoteRequest, PreparedRemoteRequest, RemoteAdapter, RemoteBaseline,
    RemoteCapabilities, RemoteCursor, RemoteEntry, RemoteFailure, RemoteObservation,
    RemoteOperation, RemoteOutcome, RemotePin, RemoteRequest, SyncEngine, SyncModel,
};

#[derive(Clone)]
struct FakeAdapter {
    state: Arc<Mutex<FakeState>>,
    capabilities: RemoteCapabilities,
    model: SyncModel,
}

struct FakeState {
    entries: BTreeMap<String, RemoteEntry>,
    payloads: BTreeMap<String, Vec<u8>>,
    sequence: u64,
    publish_calls: Vec<Vec<RemoteOperation>>,
    receipt_calls: usize,
    observe_with_baseline: Vec<bool>,
    unknown_once: bool,
}

impl FakeAdapter {
    fn new(capabilities: RemoteCapabilities, model: SyncModel) -> Self {
        let mut entries = BTreeMap::new();
        entries.insert(
            String::new(),
            fake_entry("", EntryKind::Directory, 0, false),
        );
        Self {
            state: Arc::new(Mutex::new(FakeState {
                entries,
                payloads: BTreeMap::new(),
                sequence: 0,
                publish_calls: Vec::new(),
                receipt_calls: 0,
                observe_with_baseline: Vec::new(),
                unknown_once: false,
            })),
            capabilities,
            model,
        }
    }

    fn managed_like() -> Self {
        Self::new(
            RemoteCapabilities {
                directories: true,
                executable: true,
                batch: true,
                rename: true,
                delta: true,
                receipt_lookup: true,
                max_batch: 4096,
            },
            SyncModel::Managed,
        )
    }

    fn direct_like() -> Self {
        Self::new(
            RemoteCapabilities {
                directories: false,
                executable: false,
                batch: false,
                rename: false,
                delta: false,
                receipt_lookup: false,
                max_batch: 1,
            },
            SyncModel::Direct,
        )
    }

    fn set_capability(&mut self, update: impl FnOnce(&mut RemoteCapabilities)) {
        update(&mut self.capabilities);
    }

    fn put(&self, path: &str, bytes: &[u8]) {
        let mut state = self.state.lock().unwrap();
        state.payloads.insert(path.to_owned(), bytes.to_vec());
        state.entries.insert(
            path.to_owned(),
            fake_entry(path, EntryKind::File, bytes.len() as u64, false),
        );
    }

    fn reset_calls(&self) {
        let mut state = self.state.lock().unwrap();
        state.publish_calls.clear();
        state.receipt_calls = 0;
        state.observe_with_baseline.clear();
    }

    fn publish_calls(&self) -> Vec<Vec<RemoteOperation>> {
        self.state.lock().unwrap().publish_calls.clone()
    }

    fn receipt_calls(&self) -> usize {
        self.state.lock().unwrap().receipt_calls
    }

    fn observed_with_baseline(&self) -> Vec<bool> {
        self.state.lock().unwrap().observe_with_baseline.clone()
    }

    fn operations(&self) -> Vec<RemoteOperation> {
        self.state
            .lock()
            .unwrap()
            .publish_calls
            .iter()
            .flatten()
            .copied()
            .collect()
    }

    fn fail_next_unknown(&self) {
        self.state.lock().unwrap().unknown_once = true;
    }
}

impl RemoteAdapter for FakeAdapter {
    fn model(&self) -> SyncModel {
        self.model
    }

    fn namespace_id(&self) -> String {
        "fake-namespace".to_owned()
    }

    fn capabilities(&self) -> RemoteCapabilities {
        self.capabilities
    }

    async fn preflight(&self, _state: &Path) -> yinyang::sync_engine::Result<()> {
        Ok(())
    }

    async fn bind(&self, _state: &Path) -> yinyang::sync_engine::Result<()> {
        Ok(())
    }

    async fn observe(
        &self,
        baseline: Option<&RemoteBaseline>,
    ) -> yinyang::sync_engine::Result<RemoteObservation> {
        let mut state = self.state.lock().unwrap();
        state.observe_with_baseline.push(baseline.is_some());
        state.sequence += 1;
        let mut cursor = vec![0_u8; 27];
        cursor[..8].copy_from_slice(&state.sequence.to_be_bytes());
        Ok(RemoteObservation {
            cursor: RemoteCursor { bytes: cursor },
            revision: None,
            entries: state.entries.clone(),
        })
    }

    async fn read_file(
        &self,
        entry: &RemoteEntry,
        output: BorrowedFd<'_>,
    ) -> yinyang::sync_engine::Result<()> {
        let bytes = self
            .state
            .lock()
            .unwrap()
            .payloads
            .get(&entry.path)
            .cloned()
            .ok_or_else(|| {
                yinyang::sync_engine::Error::corrupt("fake remote payload disappeared")
            })?;
        let owned = output.try_clone_to_owned().map_err(|error| {
            yinyang::sync_engine::Error::io("duplicate fake download", Path::new("."), error)
        })?;
        let mut file = std::fs::File::from(owned);
        file.write_all(&bytes).map_err(|error| {
            yinyang::sync_engine::Error::io("write fake download", Path::new("."), error)
        })?;
        file.sync_all().map_err(|error| {
            yinyang::sync_engine::Error::io("sync fake download", Path::new("."), error)
        })
    }

    async fn freeze(
        &self,
        request: RemoteRequest,
    ) -> yinyang::sync_engine::Result<FrozenRemoteRequest> {
        let mut state = self.state.lock().unwrap();
        state.sequence += 1;
        let operation = request.operation();
        let path = request.path().to_owned();
        let original_path = request.original_path().map(str::to_owned);
        let desired_path = request.desired_path().map(str::to_owned);
        let bytes = format!("{}:{}:{}", operation.as_str(), path, state.sequence).into_bytes();
        let digest = *blake3::hash(&bytes).as_bytes();
        let mut id = [0_u8; 16];
        id.copy_from_slice(&digest[..16]);
        Ok(FrozenRemoteRequest {
            id,
            digest,
            bytes,
            operation,
            path,
            original_path,
            desired_path,
        })
    }

    async fn receipt(
        &self,
        request: &FrozenRemoteRequest,
    ) -> yinyang::sync_engine::Result<Option<RemoteOutcome>> {
        let mut state = self.state.lock().unwrap();
        state.receipt_calls += 1;
        let Some(entry) = state.entries.get(&request.path).cloned() else {
            return Ok(None);
        };
        let mut accepted = BTreeMap::new();
        accepted.insert(request.path.clone(), Some(entry));
        Ok(Some(RemoteOutcome::Applied {
            accepted,
            receipt: None,
        }))
    }

    async fn publish_batch(
        &self,
        requests: &[PreparedRemoteRequest],
    ) -> yinyang::sync_engine::Result<Vec<RemoteOutcome>> {
        let mut state = self.state.lock().unwrap();
        state.publish_calls.push(
            requests
                .iter()
                .map(|request| request.request.operation)
                .collect(),
        );
        let unknown = state.unknown_once;
        state.unknown_once = false;
        let mut outcomes = Vec::with_capacity(requests.len());
        for request in requests {
            let accepted = apply_request(&mut state, request)?;
            outcomes.push(if unknown {
                RemoteOutcome::Unknown {
                    failure: RemoteFailure {
                        kind: yinyang::runtime::ErrorKind::Unknown,
                        message: "fake response was lost".to_owned(),
                        commit: None,
                    },
                }
            } else {
                let mut accepted_entries = BTreeMap::new();
                accepted_entries.insert(request.request.path.clone(), accepted);
                RemoteOutcome::Applied {
                    accepted: accepted_entries,
                    receipt: None,
                }
            });
        }
        Ok(outcomes)
    }
}

fn apply_request(
    state: &mut FakeState,
    request: &PreparedRemoteRequest,
) -> yinyang::sync_engine::Result<Option<RemoteEntry>> {
    match request.request.operation {
        RemoteOperation::CreateFile | RemoteOperation::Content => {
            let source = request
                .source
                .as_deref()
                .ok_or_else(|| yinyang::sync_engine::Error::corrupt("fake upload has no source"))?;
            let bytes = std::fs::read(source).map_err(|error| {
                yinyang::sync_engine::Error::io("read fake upload", source, error)
            })?;
            let executable = state
                .entries
                .get(&request.request.path)
                .is_some_and(|entry| entry.executable);
            state
                .payloads
                .insert(request.request.path.clone(), bytes.clone());
            let entry = fake_entry(
                &request.request.path,
                EntryKind::File,
                bytes.len() as u64,
                executable,
            );
            state
                .entries
                .insert(request.request.path.clone(), entry.clone());
            Ok(Some(entry))
        }
        RemoteOperation::CreateDirectory => {
            let entry = fake_entry(&request.request.path, EntryKind::Directory, 0, false);
            state
                .entries
                .insert(request.request.path.clone(), entry.clone());
            Ok(Some(entry))
        }
        RemoteOperation::Remove => {
            let prefix = format!("{}/", request.request.path);
            state
                .entries
                .retain(|path, _| path != &request.request.path && !path.starts_with(&prefix));
            state
                .payloads
                .retain(|path, _| path != &request.request.path && !path.starts_with(&prefix));
            Ok(None)
        }
        RemoteOperation::Rename => {
            let from =
                request.request.original_path.as_deref().ok_or_else(|| {
                    yinyang::sync_engine::Error::corrupt("fake rename has no source")
                })?;
            let entry = state.entries.remove(from).ok_or_else(|| {
                yinyang::sync_engine::Error::conflict("fake rename source disappeared")
            })?;
            if let Some(bytes) = state.payloads.remove(from) {
                state.payloads.insert(request.request.path.clone(), bytes);
            }
            let entry = fake_entry(
                &request.request.path,
                entry.kind,
                entry.length,
                entry.executable,
            );
            state
                .entries
                .insert(request.request.path.clone(), entry.clone());
            Ok(Some(entry))
        }
        RemoteOperation::SetExecutable => {
            let entry = state
                .entries
                .get_mut(&request.request.path)
                .ok_or_else(|| {
                    yinyang::sync_engine::Error::conflict("fake metadata target disappeared")
                })?;
            entry.executable = !entry.executable;
            Ok(Some(entry.clone()))
        }
    }
}

fn fake_entry(path: &str, kind: EntryKind, length: u64, executable: bool) -> RemoteEntry {
    let mut hasher = blake3::Hasher::new();
    hasher.update(path.as_bytes());
    hasher.update(&length.to_be_bytes());
    if let Some(bytes) = path.as_bytes().first() {
        hasher.update(&[*bytes]);
    }
    let digest = *hasher.finalize().as_bytes();
    let mut observation = [0_u8; 16];
    observation.copy_from_slice(&digest[..16]);
    RemoteEntry {
        path: path.to_owned(),
        kind,
        length,
        digest: (kind == EntryKind::File).then_some(digest),
        executable,
        pin: RemotePin::Direct {
            observation,
            version: Some(hex(&digest)),
            etag: Some(hex(&digest)),
            size: length,
            modified: Some(length),
        },
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

async fn initialize(
    adapter: &FakeAdapter,
    temp: &tempfile::TempDir,
) -> (SyncEngine<FakeAdapter>, PathBuf, PathBuf) {
    let root = temp.path().join("root");
    let state_path = temp.path().join("state");
    std::fs::create_dir_all(&root).unwrap();
    let engine = SyncEngine::init(adapter.clone(), &state_path, &root)
        .await
        .unwrap();
    (engine, root, state_path)
}

#[tokio::test]
async fn batch_and_max_batch_capabilities_select_batch_boundaries() {
    for (batch, max_batch, expected) in [(true, 2, vec![2, 1]), (false, 1, vec![1, 1, 1])] {
        let mut adapter = FakeAdapter::managed_like();
        adapter.set_capability(|capabilities| {
            capabilities.batch = batch;
            capabilities.max_batch = max_batch;
        });
        let temp = tempfile::tempdir().unwrap();
        let (mut engine, root, _) = initialize(&adapter, &temp).await;
        for index in 0..3 {
            std::fs::write(root.join(format!("file-{index}")), [index as u8]).unwrap();
        }
        assert!(engine.run().await.unwrap().in_sync);
        assert_eq!(
            adapter
                .publish_calls()
                .into_iter()
                .map(|batch| batch.len())
                .collect::<Vec<_>>(),
            expected
        );
    }
}

#[tokio::test]
async fn delta_capability_controls_the_observe_baseline() {
    for delta in [true, false] {
        let mut adapter = FakeAdapter::managed_like();
        adapter.set_capability(|capabilities| capabilities.delta = delta);
        let temp = tempfile::tempdir().unwrap();
        let (mut engine, _, _) = initialize(&adapter, &temp).await;
        adapter.reset_calls();
        assert!(engine.run().await.unwrap().in_sync);
        let observed = adapter.observed_with_baseline();
        assert_eq!(observed.iter().any(|used| *used), delta, "{observed:?}");
    }
}

#[tokio::test]
async fn pull_and_settled_noop_observe_the_remote_once() {
    for adapter in [FakeAdapter::managed_like(), FakeAdapter::direct_like()] {
        adapter.put("file", b"bytes");
        let temp = tempfile::tempdir().unwrap();
        let (mut engine, _, _) = initialize(&adapter, &temp).await;
        adapter.reset_calls();
        assert!(engine.run().await.unwrap().in_sync);
        assert_eq!(adapter.observed_with_baseline().len(), 1);

        adapter.reset_calls();
        assert!(engine.run().await.unwrap().in_sync);
        assert_eq!(adapter.observed_with_baseline().len(), 1);
    }
}

#[tokio::test]
async fn rename_capability_controls_file_move_operations() {
    for rename in [true, false] {
        let mut adapter = FakeAdapter::managed_like();
        adapter.set_capability(|capabilities| capabilities.rename = rename);
        let temp = tempfile::tempdir().unwrap();
        let (mut engine, root, _) = initialize(&adapter, &temp).await;
        std::fs::write(root.join("before"), b"bytes").unwrap();
        assert!(engine.run().await.unwrap().in_sync);
        std::fs::rename(root.join("before"), root.join("after")).unwrap();
        adapter.reset_calls();
        assert!(engine.run().await.unwrap().in_sync);
        let operations = adapter.operations();
        assert_eq!(
            operations.contains(&RemoteOperation::Rename),
            rename,
            "{operations:?}"
        );
        if !rename {
            assert!(operations.contains(&RemoteOperation::Remove));
            assert!(operations.contains(&RemoteOperation::CreateFile));
        }
    }
}

#[tokio::test]
async fn directories_capability_controls_empty_directory_support() {
    for directories in [true, false] {
        let mut adapter = FakeAdapter::managed_like();
        adapter.set_capability(|capabilities| capabilities.directories = directories);
        let temp = tempfile::tempdir().unwrap();
        let (mut engine, root, _) = initialize(&adapter, &temp).await;
        std::fs::create_dir(root.join("empty")).unwrap();
        let result = engine.run().await;
        if directories {
            assert!(result.unwrap().in_sync);
            assert!(
                adapter
                    .operations()
                    .contains(&RemoteOperation::CreateDirectory)
            );
        } else {
            assert_eq!(
                result.unwrap_err().kind(),
                yinyang::runtime::ErrorKind::Unsupported
            );
            assert!(
                !adapter
                    .operations()
                    .contains(&RemoteOperation::CreateDirectory)
            );
        }
    }
}

#[tokio::test]
async fn executable_capability_controls_metadata_publication() {
    for executable in [true, false] {
        let mut adapter = FakeAdapter::managed_like();
        adapter.set_capability(|capabilities| capabilities.executable = executable);
        let temp = tempfile::tempdir().unwrap();
        let (mut engine, root, _) = initialize(&adapter, &temp).await;
        let path = root.join("tool");
        std::fs::write(&path, b"#!/bin/sh\n").unwrap();
        assert!(engine.run().await.unwrap().in_sync);
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        adapter.reset_calls();
        let outcome = engine.run().await.unwrap();
        assert!(outcome.in_sync, "executable={executable}: {outcome:?}");
        assert_eq!(
            adapter
                .operations()
                .contains(&RemoteOperation::SetExecutable),
            executable
        );
    }
}

#[tokio::test]
async fn receipt_lookup_capability_controls_unknown_replay() {
    for receipt_lookup in [true, false] {
        let mut adapter = FakeAdapter::managed_like();
        adapter.set_capability(|capabilities| capabilities.receipt_lookup = receipt_lookup);
        adapter.fail_next_unknown();
        let temp = tempfile::tempdir().unwrap();
        let (mut engine, root, _) = initialize(&adapter, &temp).await;
        std::fs::write(root.join("unknown"), b"value").unwrap();
        assert_eq!(engine.run().await.unwrap().unknown, 1);
        adapter.reset_calls();
        assert!(engine.run().await.unwrap().in_sync);
        assert_eq!(adapter.receipt_calls() > 0, receipt_lookup);
        if !receipt_lookup {
            assert!(!adapter.publish_calls().is_empty());
        }
    }
}

#[tokio::test]
async fn tampered_unknown_input_fails_without_redispatch() {
    let adapter = FakeAdapter::managed_like();
    adapter.fail_next_unknown();
    let temp = tempfile::tempdir().unwrap();
    let (mut engine, root, state) = initialize(&adapter, &temp).await;
    std::fs::write(root.join("unknown"), b"original").unwrap();
    assert_eq!(engine.run().await.unwrap().unknown, 1);
    drop(engine);

    let artifact = std::fs::read_dir(state.join("artifacts"))
        .unwrap()
        .find_map(|entry| {
            let path = entry.unwrap().path();
            path.is_file().then_some(path)
        })
        .expect("Unknown work retains its source artifact");
    std::fs::write(&artifact, b"tampered").unwrap();

    adapter.reset_calls();
    let mut reopened = SyncEngine::open(adapter.clone(), &state).await.unwrap();
    let error = reopened.run().await.unwrap_err();
    assert_eq!(error.kind(), yinyang::runtime::ErrorKind::Corrupt);
    assert!(adapter.publish_calls().is_empty());
}

#[tokio::test]
async fn mixed_create_and_content_edits_share_one_managed_publication() {
    for (profile, adapter) in [
        ("managed", FakeAdapter::managed_like()),
        ("direct", FakeAdapter::direct_like()),
    ] {
        adapter.put("existing", b"baseline");
        let temp = tempfile::tempdir().unwrap();
        let (mut engine, root, _) = initialize(&adapter, &temp).await;
        assert!(engine.run().await.unwrap().in_sync);

        std::fs::write(root.join("existing"), b"replacement").unwrap();
        std::fs::write(root.join("created"), b"new file").unwrap();
        adapter.reset_calls();
        assert!(engine.run().await.unwrap().in_sync);

        let calls = adapter.publish_calls();
        if profile == "managed" {
            assert_eq!(calls.len(), 1, "managed mixed edit used {calls:?}");
            assert_eq!(calls[0].len(), 2, "managed mixed batch: {calls:?}");
        } else {
            assert_eq!(calls.len(), 2, "direct mixed edit used {calls:?}");
        }
        let operations = adapter.operations();
        assert!(operations.contains(&RemoteOperation::Content));
        assert!(operations.contains(&RemoteOperation::CreateFile));
    }
}

#[tokio::test]
async fn w2_costs_hold_for_managed_and_direct_capability_profiles() {
    const FILES: usize = 16;
    const SIZE: usize = 256 * 1024;

    for (profile, adapter) in [
        ("managed", FakeAdapter::managed_like()),
        ("direct", FakeAdapter::direct_like()),
    ] {
        for index in 0..FILES {
            let mut bytes = vec![(index % 251) as u8; SIZE];
            bytes[..8].copy_from_slice(&(index as u64).to_le_bytes());
            adapter.put(&format!("file-{index:02}"), &bytes);
        }
        let temp = tempfile::tempdir().unwrap();
        let (mut engine, root, state_path) = initialize(&adapter, &temp).await;
        let tree = (FILES * SIZE) as u64;
        assert!(engine.run().await.unwrap().in_sync);
        let pull_hashes = engine.hash_bytes_read();
        assert!(
            pull_hashes <= tree * 2,
            "{profile} pull hashed {pull_hashes} bytes for {tree} tree bytes"
        );
        let status =
            SyncEngine::<FakeAdapter>::inspect(Some(&adapter), &state_path, adapter.model(), None)
                .await;
        assert_eq!(status.summary.retained_bytes, 0);
        let engine_state = yinyang::sync_engine::inspect_state(&state_path).unwrap();
        assert_eq!(engine_state.model, adapter.model());
        if profile == "direct" {
            assert!(engine_state.baseline_revisions.is_empty());
            let report = yinyang::space::inspect_sync_space(&state_path).await;
            assert_eq!(report.schema, "yinyang.sync.space.v1");
            assert_eq!(
                report.staging.engine.state,
                yinyang::space::StagingState::Available
            );
        }
        tokio::time::sleep(Duration::from_millis(1100)).await;
        assert!(engine.run().await.unwrap().in_sync);
        assert!(engine.run().await.unwrap().in_sync);
        assert_eq!(
            engine.hash_bytes_read(),
            0,
            "{profile} settled no-op rehashed local files"
        );
        let status =
            SyncEngine::<FakeAdapter>::inspect(Some(&adapter), &state_path, adapter.model(), None)
                .await;
        assert_eq!(status.summary.retained_bytes, 0);
        println!("W5_ENGINE profile={profile} files={FILES} tree_bytes={tree}");
        println!("W5_ENGINE profile={profile} pull_hashed_bytes={pull_hashes}");
        println!("W5_ENGINE profile={profile} settled_noop_hashed_bytes=0");
        println!("W5_ENGINE profile={profile} retained_bytes=0");

        let create_temp = tempfile::tempdir().unwrap();
        let (mut create_engine, create_root, _) = initialize(&adapter, &create_temp).await;
        for index in 0..FILES {
            std::fs::write(create_root.join(format!("new-{index:02}")), [index as u8]).unwrap();
        }
        adapter.reset_calls();
        assert!(create_engine.run().await.unwrap().in_sync);
        let calls = adapter.publish_calls().len();
        if profile == "managed" {
            assert_eq!(calls, 1, "managed creates used {calls} remote batches");
        } else {
            assert_eq!(calls, FILES, "direct creates used {calls} remote writes");
        }
        println!("W5_ENGINE profile={profile} create_remote_calls={calls}");

        drop((engine, create_engine));
        drop((root, create_root));
        let _ = tree;
    }
}
