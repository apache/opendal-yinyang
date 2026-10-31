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

use std::collections::BTreeSet;
use std::sync::Arc;
use support::TestBackend;
use yinyang::core::{Authority, BackendProfile, Fs, NodeId, Revision};
use yinyang::replica::{DirectorySync, Replica};
use yinyang::space::{BackupPackageState, StagingBindingState, plan_managed_retention};
use yinyang::sync::{FileMetadata, Sync};

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

#[tokio::test]
async fn explicit_backup_protects_readable_package_dependencies_without_original_root() {
    let backend = TestBackend::default();
    let fs = Fs::create(backend.operator(), BackendProfile::AmazonS3)
        .await
        .unwrap();
    let authority: Arc<dyn Authority> = Arc::new(fs);
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("root");
    let state = temp.path().join("state");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::write(root.join("seed.txt"), b"seed").unwrap();

    let mut directory = DirectorySync::init(authority.clone(), &state, &root)
        .await
        .unwrap();
    let first = directory.run().await.unwrap();
    assert!(first.published_local >= 1);
    drop(directory);

    let status = Replica::inspect(&state).unwrap();
    let baseline = status.baseline.expect("initial run records a baseline");
    assert!(
        authority
            .retained_revisions()
            .await
            .unwrap()
            .contains(&baseline.revision)
    );

    // Stage a pending content edit and namespace intent without publishing it.
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
    sync.stage_create_file(
        baseline_snapshot.root(),
        "pending.txt",
        baseline_snapshot.revision(),
        FileMetadata::new(false),
        &mut &b"pending bytes"[..],
    )
    .await
    .unwrap();
    drop(sync);

    let backup = temp.path().join("backup");
    let directory = DirectorySync::open(authority.clone(), &state)
        .await
        .unwrap();
    directory.backup(&backup).await.unwrap();
    drop(directory);

    // The package keeps its own root payload; the original root may disappear.
    std::fs::rename(&root, temp.path().join("root-moved")).unwrap();
    let protection = Replica::inspect_protection(&backup.join("state")).unwrap();
    assert_eq!(protection.filesystem, authority.filesystem());
    assert!(protection.baseline.is_some());

    let absent_staging = temp.path().join("absent-staging");
    let plan = plan_managed_retention(authority.as_ref(), &absent_staging, Some(1), &[], &[backup])
        .await
        .unwrap();
    assert_eq!(plan.protection.backup_evidence, "supplied_validated");
    assert_eq!(plan.protection.backups.supplied, 1);
    assert_eq!(plan.protection.backups.validated, 1);
    let package = &plan.protection.backups.packages[0];
    assert_eq!(package.state, BackupPackageState::Validated);
    let filesystem = identity_hex(authority.filesystem());
    assert_eq!(package.filesystem.as_deref(), Some(filesystem.as_str()));
    let staging = package.staging.as_ref().expect("package staging evidence");
    assert_eq!(staging.binding.state, StagingBindingState::Verified);
    assert!(staging.pending >= 1);
    assert!(staging.sync.namespace_prepared >= 1);

    let reasons = plan
        .revisions
        .iter()
        .flat_map(|revision| revision.reasons.iter().copied())
        .collect::<BTreeSet<_>>();
    assert!(reasons.contains("backup_staging_pending"));
    assert!(reasons.contains("backup_sync_namespace_pending"));
    assert!(reasons.contains("backup_replica_baseline"));
    assert!(plan.revisions.iter().any(|revision| {
        revision.revision == revision_hex(baseline.revision)
            && revision.reasons.contains(&"backup_replica_baseline")
    }));
}
