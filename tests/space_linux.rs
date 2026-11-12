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

use yinyang_test_support as support;

use std::collections::BTreeSet;
use std::sync::Arc;
use support::TestBackend;
use yinyang::core::{Authority, CommitId, CommitOutcome, Fs, NodeId, Planner, Revision};
use yinyang::runtime::Runtime;
use yinyang::space::{
    BackupPackageState, StagingBindingState, inspect_bound_local_staging, plan_managed_retention,
};
use yinyang::sync::Sync;
use yinyang::sync_engine::{
    ErrorKind as EngineErrorKind, FrozenRemoteRequest, ManagedAdapter, PreparedRemoteRequest,
    RemoteAdapter, RemoteOperation, SyncEngine, inspect_backup_package, inspect_state,
};

fn identity_hex(identity: NodeId) -> String {
    identity
        .as_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn revision_hex(revision: Revision) -> String {
    revision
        .to_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn parse_revision(value: &str) -> Revision {
    let mut bytes = [0_u8; 24];
    for (index, byte) in bytes.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16).unwrap();
    }
    Revision::from_bytes(bytes)
}

#[tokio::test]
async fn managed_adapter_rejects_wrong_frozen_wrapper_before_receipt_and_dispatch() {
    let backend = TestBackend::default();
    let fs = Fs::create(backend.operator()).await.unwrap();
    let authority: Arc<dyn Authority> = Arc::new(fs);
    let snapshot = authority.observe_latest().await.unwrap();
    let mut planner = Planner::new(&snapshot, CommitId::generate());
    planner
        .create_directory(authority.root(), "guarded")
        .await
        .unwrap();
    let transaction = planner.finish().unwrap();
    let id = *transaction.id().as_bytes();
    let digest = transaction.digest();
    let bytes = transaction.to_bytes().unwrap();
    let before = snapshot.revision();

    let mut wrong_id = id;
    wrong_id[0] ^= 1;
    let mut wrong_digest = digest;
    wrong_digest[0] ^= 1;
    for (request_id, request_digest) in [(wrong_id, digest), (id, wrong_digest)] {
        let request = PreparedRemoteRequest {
            request: FrozenRemoteRequest {
                id: request_id,
                digest: request_digest,
                bytes: bytes.clone(),
                operation: RemoteOperation::CreateDirectory,
                path: "guarded".to_owned(),
                original_path: None,
                desired_path: None,
            },
            source: None,
        };
        let error = ManagedAdapter::new(authority.clone())
            .publish_batch(&[request])
            .await
            .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::Corrupt);
        assert_eq!(authority.observe_latest().await.unwrap().revision(), before);
    }
    assert!(
        authority
            .observe_latest()
            .await
            .unwrap()
            .resolve("guarded")
            .await
            .unwrap()
            .is_none()
    );

    assert!(matches!(
        authority.commit(&transaction).await.unwrap(),
        CommitOutcome::Committed(_)
    ));
    let committed = authority.observe_latest().await.unwrap();
    assert!(committed.resolve("guarded").await.unwrap().is_some());
    let adapter = ManagedAdapter::new(authority.clone());
    for (request_id, request_digest) in [(wrong_id, digest), (id, wrong_digest)] {
        let frozen = FrozenRemoteRequest {
            id: request_id,
            digest: request_digest,
            bytes: bytes.clone(),
            operation: RemoteOperation::CreateDirectory,
            path: "guarded".to_owned(),
            original_path: None,
            desired_path: None,
        };
        let error = adapter.receipt(&frozen).await.unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::Corrupt);

        let error = adapter
            .publish_batch(&[PreparedRemoteRequest {
                request: frozen,
                source: None,
            }])
            .await
            .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::Corrupt);
        assert_eq!(
            authority.observe_latest().await.unwrap().revision(),
            committed.revision()
        );
    }
}

#[tokio::test]
async fn explicit_backup_protects_readable_package_dependencies_without_original_root() {
    let backend = TestBackend::default();
    let fs = Fs::create(backend.operator()).await.unwrap();
    let authority: Arc<dyn Authority> = Arc::new(fs);
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("root");
    let state = temp.path().join("state");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::write(root.join("seed.txt"), b"seed").unwrap();

    let adapter = ManagedAdapter::new(authority.clone());
    let mut directory = SyncEngine::init(adapter.clone(), &state, &root)
        .await
        .unwrap();
    let first = directory.run().await.unwrap();
    assert!(first.published_local >= 1);
    drop(directory);
    drop(adapter);

    let inspection = inspect_state(&state).unwrap();
    let baseline = parse_revision(
        inspection
            .baseline_revisions
            .first()
            .expect("initial run records a baseline"),
    );
    assert!(
        authority
            .retained_revisions()
            .await
            .unwrap()
            .contains(&baseline)
    );
    let live = inspect_bound_local_staging(&state, authority.filesystem()).await;
    assert_eq!(live.binding.state, StagingBindingState::Verified);
    assert_eq!(live.engine.state, yinyang::space::StagingState::Available);
    assert!(live.engine.baseline.is_some());
    assert!(live.protected_revisions.iter().any(|revision| {
        revision.revision == revision_hex(baseline)
            && revision.reasons.contains(&"sync_engine_baseline")
    }));

    let sync = Sync::open(authority.clone(), &state).await.unwrap();
    let baseline_snapshot = authority.observe_latest().await.unwrap();
    let seeded = baseline_snapshot
        .resolve("seed.txt")
        .await
        .unwrap()
        .expect("seeded file");
    sync.stage(
        seeded.id(),
        baseline_snapshot.revision(),
        &mut &b"replacement seed"[..],
    )
    .await
    .unwrap();
    sync.stage(
        seeded.id(),
        baseline_snapshot.revision(),
        &mut &b"second replacement"[..],
    )
    .await
    .unwrap();
    drop(sync);

    let runtime = Runtime::open(authority.clone(), &state).await.unwrap();
    let file = runtime.open_node(seeded.id(), true).await.unwrap();
    file.release().unwrap();
    drop(runtime);

    let backup = temp.path().join("backup");
    let mut directory = SyncEngine::open(ManagedAdapter::new(authority.clone()), &state)
        .await
        .unwrap();
    directory.backup(&backup).await.unwrap();
    drop(directory);

    std::fs::rename(&root, temp.path().join("root-moved")).unwrap();
    let package = inspect_backup_package(&backup).unwrap();
    assert_eq!(package.model, yinyang::sync_engine::SyncModel::Managed);
    assert!(
        package
            .state
            .baseline_revisions
            .contains(&revision_hex(baseline))
    );

    let absent_staging = temp.path().join("absent-staging");
    let plan = plan_managed_retention(authority.as_ref(), &absent_staging, Some(1), &[], &[backup])
        .await
        .unwrap();
    assert_eq!(plan.protection.backup_evidence, "supplied_validated");
    assert_eq!(plan.protection.backups.supplied, 1);
    assert_eq!(plan.protection.backups.validated, 1);
    let package = &plan.protection.backups.packages[0];
    assert_eq!(package.state, BackupPackageState::Validated);
    let id = identity_hex(authority.filesystem());
    assert_eq!(package.filesystem.as_deref(), Some(id.as_str()));
    let staging = package.staging.as_ref().expect("package staging evidence");
    assert_eq!(staging.binding.state, StagingBindingState::Verified);
    assert!(staging.sync.prepared >= 2);
    assert!(staging.engine.baseline.is_some());

    let reasons = plan
        .revisions
        .iter()
        .flat_map(|revision| revision.reasons.iter().copied())
        .collect::<BTreeSet<_>>();
    assert!(reasons.contains("backup_sync_intent_prepared"));
    assert!(reasons.contains("backup_staging_base"));
    assert!(reasons.contains("backup_sync_engine_baseline"));
    assert!(plan.revisions.iter().any(|revision| {
        revision.revision == revision_hex(baseline)
            && revision.reasons.contains(&"backup_sync_engine_baseline")
    }));
}
