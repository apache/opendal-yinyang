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

use yinyang_test_support as support;

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use support::TestBackend;
use yinyang::core::transaction::Planner;
use yinyang::core::{
    Authority, CommitId, CommitOutcome, DataStore, ErrorKind as CoreErrorKind, Fs, NodeId,
    Revision, Snapshot, Transaction,
};
use yinyang::runtime::{ErrorKind, Runtime};
use yinyang::space::{
    MeasurementState, StagingBindingState, StagingState, inspect_bound_local_staging,
    inspect_managed_space, plan_managed_retention,
};
use yinyang::sync::Sync;

#[derive(Debug)]
struct LostResponseAuthority {
    inner: Arc<dyn Authority>,
    lose_next: AtomicBool,
}

impl LostResponseAuthority {
    fn new(inner: Arc<dyn Authority>) -> Self {
        Self {
            inner,
            lose_next: AtomicBool::new(false),
        }
    }

    fn lose_next_response(&self) {
        self.lose_next.store(true, Ordering::SeqCst);
    }
}

impl Authority for LostResponseAuthority {
    fn data(&self) -> &DataStore {
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
            if self.lose_next.swap(false, Ordering::SeqCst) {
                return Err(yinyang::core::Error::new(
                    CoreErrorKind::Invalid,
                    "test sync publication",
                    "lost response after publication",
                ));
            }
            Ok(outcomes)
        })
    }
}

#[tokio::test]
async fn content_unknown_protects_baseline_without_a_staged_handle() {
    let backend = TestBackend::default();
    let fs = Fs::create(backend.operator()).await.unwrap();
    let inner: Arc<dyn Authority> = Arc::new(fs);
    let fault = Arc::new(LostResponseAuthority::new(inner));
    let authority: Arc<dyn Authority> = fault.clone();
    let content = authority
        .data()
        .prepare(&mut b"baseline content".as_slice())
        .await
        .unwrap();
    let mut plan = Planner::new(
        &authority.observe_latest().await.unwrap(),
        CommitId::generate(),
    );
    plan.create_file(authority.root(), "edited", content, false)
        .await
        .unwrap();
    assert!(matches!(
        authority.commit(&plan.finish().unwrap()).await.unwrap(),
        CommitOutcome::Committed(_)
    ));
    let temp = tempfile::tempdir().unwrap();
    let state = temp.path().join("sync");
    let sync = Sync::open(authority.clone(), &state).await.unwrap();
    let baseline = authority.observe_latest().await.unwrap();
    let node = baseline.resolve("edited").await.unwrap().unwrap();
    let intent = sync
        .stage(
            node.id(),
            baseline.revision(),
            &mut b"replacement content".as_slice(),
        )
        .await
        .unwrap();

    fault.lose_next_response();
    let error = sync.publish(intent).await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Unknown);
    drop(sync);

    let sync_database = state.join("sync.db");
    let sync_before = std::fs::read(&sync_database).unwrap();
    let inspection = inspect_bound_local_staging(&state, authority.filesystem()).await;

    assert_eq!(inspection.state, StagingState::Available);
    assert_eq!(inspection.binding.state, StagingBindingState::Verified);
    assert!(inspection.errors.is_empty());
    assert_eq!(inspection.sync.state, StagingState::Available);
    assert_eq!(inspection.sync.intents, 1);
    assert_eq!(inspection.sync.unknown, 1);
    assert_eq!(inspection.handles.total, 0);
    let baseline_hex = baseline
        .revision()
        .to_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let protected = inspection
        .protected_revisions
        .iter()
        .find(|revision| revision.revision == baseline_hex)
        .expect("intent baseline must be protected");
    assert!(protected.reasons.contains(&"sync_intent_unknown"));
    assert_eq!(std::fs::read(&sync_database).unwrap(), sync_before);
}

#[tokio::test]
async fn foreign_staging_is_unavailable_and_not_attributed() {
    let source_backend = TestBackend::default();
    let source = Fs::create(source_backend.operator()).await.unwrap();
    let source: Arc<dyn Authority> = Arc::new(source);
    let temp = tempfile::tempdir().unwrap();
    let state = temp.path().join("sync");
    let sync = Sync::open(source.clone(), &state).await.unwrap();
    drop(sync);

    let target_backend = TestBackend::default();
    let target = Fs::create(target_backend.operator()).await.unwrap();
    assert_ne!(source.filesystem(), target.filesystem());
    let sync_database = state.join("sync.db");
    let before = std::fs::read(&sync_database).unwrap();
    let inspection = inspect_bound_local_staging(&state, target.filesystem()).await;

    assert_eq!(inspection.state, StagingState::Unavailable);
    assert_eq!(inspection.binding.state, StagingBindingState::Unavailable);
    assert!(inspection.binding.error.is_some());
    assert_eq!(inspection.handles.total, 0);
    assert_eq!(inspection.sync.intents, 0);
    assert!(inspection.protected_revisions.is_empty());
    assert_eq!(std::fs::read(&sync_database).unwrap(), before);
}

#[tokio::test]
async fn keep_last_and_requested_pin_select_only_declared_revisions() {
    let backend = TestBackend::default();
    let fs = Fs::create(backend.operator()).await.unwrap();
    let authority: Arc<dyn Authority> = Arc::new(fs);
    let temp = tempfile::tempdir().unwrap();
    let runtime = Runtime::open(authority.clone(), temp.path().join("editor"))
        .await
        .unwrap();
    runtime
        .create_directory(authority.root(), "one")
        .await
        .unwrap();
    runtime
        .create_directory(authority.root(), "two")
        .await
        .unwrap();
    let revisions = authority.retained_revisions().await.unwrap();
    assert_eq!(revisions.len(), 3);

    let staging = temp.path().join("absent-staging");
    let plan = plan_managed_retention(authority.as_ref(), &staging, Some(1), &[revisions[0]], &[])
        .await
        .unwrap();

    assert_eq!(plan.policy.mode, "bounded");
    assert_eq!(plan.policy.published_history, "retained_by_default");
    assert_eq!(plan.retained_revision_count, Some(3));
    assert!(!plan.deletion_authorized);
    assert_eq!(plan.reclaimable_bytes, None);
    assert_eq!(plan.protection.staging.state, StagingState::Missing);
    assert_eq!(
        plan.protection.staging.binding.state,
        StagingBindingState::Verified
    );
    assert!(!staging.exists());
    let selected = plan
        .revisions
        .iter()
        .filter(|revision| revision.selected)
        .collect::<Vec<_>>();
    assert_eq!(selected.len(), 2);
    assert!(selected.iter().any(|revision| {
        revision.revision == hex(revisions[0]) && revision.reasons.contains(&"requested_pin")
    }));
    assert!(selected.iter().any(|revision| {
        revision.revision == hex(revisions[2])
            && revision.reasons.contains(&"current")
            && revision.reasons.contains(&"keep_last")
    }));
    assert!(plan.revisions.iter().any(|revision| {
        revision.revision == hex(revisions[1])
            && !revision.selected
            && revision.reasons == ["outside_bounded_policy"]
    }));
}

#[tokio::test]
async fn space_v2_traverses_metadata_and_content_without_payload_claims() {
    let backend = TestBackend::default();
    let fs = Fs::create(backend.operator()).await.unwrap();
    let authority: Arc<dyn Authority> = Arc::new(fs);
    let content = authority
        .data()
        .prepare(&mut b"reachable content".as_slice())
        .await
        .unwrap();
    let mut plan = Planner::new(
        &authority.observe_latest().await.unwrap(),
        CommitId::generate(),
    );
    plan.create_file(authority.root(), "file", content, false)
        .await
        .unwrap();
    assert!(matches!(
        authority.commit(&plan.finish().unwrap()).await.unwrap(),
        CommitOutcome::Committed(_)
    ));

    let temp = tempfile::tempdir().unwrap();
    let staging = temp.path().join("absent");
    let report = inspect_managed_space(authority.as_ref(), &backend.operator(), &staging)
        .await
        .unwrap();
    assert_eq!(report.schema, "yinyang.managed-space.report.v4");
    assert!(!report.deletion_authorized);
    assert_eq!(report.reclaimable_bytes, None);
    assert_eq!(report.physical.state, MeasurementState::Partial);
    assert!(report.physical.reachability.snapshot_descriptors >= 2);
    assert!(report.physical.reachability.metadata_index_references > 0);
    assert!(report.physical.reachability.content_extent_references > 0);
    assert_eq!(
        report.physical.coverage.payload_verification,
        "content_index_authenticated_payload_not_read"
    );
    assert_eq!(
        report.physical.root_external.state,
        MeasurementState::Partial
    );
    assert_eq!(report.physical.root_external.packs, None);
}

fn hex(revision: Revision) -> String {
    revision
        .to_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}
