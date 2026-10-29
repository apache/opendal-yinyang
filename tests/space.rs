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
use yinyang::core::{
    Authority, BackendProfile, CommitOutcome, DataStore, ErrorKind as CoreErrorKind, Fs, NodeId,
    Revision, Snapshot, Transaction,
};
use yinyang::runtime::{ErrorKind, Runtime};
use yinyang::space::{StagingState, inspect_local_staging, plan_managed_retention};
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
async fn namespace_only_unknown_protects_baseline_without_content_handle() {
    let backend = TestBackend::default();
    let fs = Fs::create(backend.operator(), BackendProfile::AmazonS3)
        .await
        .unwrap();
    let inner: Arc<dyn Authority> = Arc::new(fs);
    let fault = Arc::new(LostResponseAuthority::new(inner));
    let authority: Arc<dyn Authority> = fault.clone();
    let temp = tempfile::tempdir().unwrap();
    let state = temp.path().join("sync");
    let sync = Sync::open(authority.clone(), &state).await.unwrap();
    let baseline = authority.observe_latest().await.unwrap();
    let intent = sync
        .stage_create_directory(authority.root(), "namespace-only", baseline.revision())
        .await
        .unwrap();

    fault.lose_next_response();
    let error = sync.publish(intent).await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Unknown);
    drop(sync);

    let sync_database = state.join("sync.db");
    let content_database = state.join("content").join("staging.db");
    let sync_before = std::fs::read(&sync_database).unwrap();
    let content_before = std::fs::read(&content_database).unwrap();
    let inspection = inspect_local_staging(&state).await;

    assert_eq!(inspection.state, StagingState::Available);
    assert!(inspection.errors.is_empty());
    assert_eq!(inspection.sync.state, StagingState::Available);
    assert_eq!(inspection.sync.namespace_intents, 1);
    assert_eq!(inspection.sync.namespace_unknown, 1);
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
        .expect("namespace baseline must be protected");
    assert!(protected.reasons.contains(&"sync_namespace_unknown"));
    assert_eq!(std::fs::read(&sync_database).unwrap(), sync_before);
    assert_eq!(std::fs::read(&content_database).unwrap(), content_before);
}

#[tokio::test]
async fn keep_last_and_requested_pin_select_only_declared_revisions() {
    let backend = TestBackend::default();
    let fs = Fs::create(backend.operator(), BackendProfile::AmazonS3)
        .await
        .unwrap();
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
    let plan = plan_managed_retention(authority.as_ref(), &staging, Some(1), &[revisions[0]])
        .await
        .unwrap();

    assert_eq!(plan.policy.mode, "bounded");
    assert_eq!(plan.retained_revision_count, Some(3));
    assert!(!plan.deletion_authorized);
    assert_eq!(plan.reclaimable_bytes, None);
    assert_eq!(plan.protection.staging.state, StagingState::Missing);
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

fn hex(revision: Revision) -> String {
    revision
        .to_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}
