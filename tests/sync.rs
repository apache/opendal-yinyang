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

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use support::TestBackend;
use yinyang::core::{
    Authority, CommitId, CommitOutcome, DataStore, ErrorKind as CoreErrorKind, Fs, NodeId, Planner,
    Revision, Snapshot, Transaction,
};
use yinyang::runtime::ErrorKind;
use yinyang::sync::{IntentState, Sync};
use yinyang_test_support as support;

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
                    "test content publication",
                    "lost response after publication",
                ));
            }
            Ok(outcomes)
        })
    }
}

async fn create_file(authority: &Arc<dyn Authority>, name: &str, bytes: &[u8]) -> NodeId {
    let prepared = authority.data().prepare(&mut &bytes[..]).await.unwrap();
    let mut planner = Planner::new(
        &authority.observe_latest().await.unwrap(),
        CommitId::generate(),
    );
    planner
        .create_file(authority.root(), name, prepared, false)
        .await
        .unwrap();
    let outcome = authority.commit(&planner.finish().unwrap()).await.unwrap();
    let CommitOutcome::Committed(receipt) = outcome else {
        panic!("create was not committed");
    };
    authority
        .observe_revision(receipt.cursor.revision)
        .await
        .unwrap()
        .resolve(name)
        .await
        .unwrap()
        .unwrap()
        .id()
}

async fn replace_file(authority: &Arc<dyn Authority>, node: NodeId, bytes: &[u8]) {
    let prepared = authority.data().prepare(&mut &bytes[..]).await.unwrap();
    let mut planner = Planner::new(
        &authority.observe_latest().await.unwrap(),
        CommitId::generate(),
    );
    let _ = planner.node(node).await.unwrap();
    planner.set_content(node, prepared).await.unwrap();
    assert!(matches!(
        authority.commit(&planner.finish().unwrap()).await.unwrap(),
        CommitOutcome::Committed(_)
    ));
}

async fn remote_bytes(authority: &Arc<dyn Authority>, node: NodeId) -> Vec<u8> {
    let snapshot = authority.observe_latest().await.unwrap();
    let file = snapshot.open_file(node).await.unwrap();
    let mut bytes = Vec::new();
    file.read_range(0..file.descriptor().content_id().length(), &mut bytes)
        .await
        .unwrap();
    bytes
}

#[tokio::test]
async fn content_edits_are_conditional_and_conflicts_are_terminal() {
    let backend = TestBackend::default();
    let fs = Fs::create(backend.operator()).await.unwrap();
    let authority: Arc<dyn Authority> = Arc::new(fs);
    let node = create_file(&authority, "edited", b"baseline").await;
    let temp = tempfile::tempdir().unwrap();
    let sync = Sync::open(authority.clone(), temp.path().join("sync"))
        .await
        .unwrap();
    let baseline = authority.observe_latest().await.unwrap();
    let edit = sync
        .stage(node, baseline.revision(), &mut &b"local replacement"[..])
        .await
        .unwrap();

    replace_file(&authority, node, b"remote replacement").await;
    let error = sync.publish(edit).await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Conflict);
    assert_eq!(
        sync.status(edit).await.unwrap().state,
        IntentState::Conflict
    );
    assert_eq!(remote_bytes(&authority, node).await, b"remote replacement");
    let inspected = Sync::inspect(temp.path().join("sync")).await.unwrap();
    assert_eq!(inspected.intents.len(), 1);
    assert_eq!(inspected.intents[0].baseline, baseline.revision());
}

#[tokio::test]
async fn unknown_content_receipt_is_resolved_after_restart_without_recommit() {
    let backend = TestBackend::default();
    let fs = Fs::create(backend.operator()).await.unwrap();
    let inner: Arc<dyn Authority> = Arc::new(fs);
    let node = create_file(&inner, "edited", b"baseline").await;
    let fault = Arc::new(LostResponseAuthority::new(inner));
    let authority: Arc<dyn Authority> = fault.clone();
    let temp = tempfile::tempdir().unwrap();
    let state = temp.path().join("sync");
    let sync = Sync::open(authority.clone(), &state).await.unwrap();
    let baseline = authority.observe_latest().await.unwrap();
    let edit = sync
        .stage(node, baseline.revision(), &mut &b"replacement"[..])
        .await
        .unwrap();
    fault.lose_next_response();
    let error = sync.publish(edit).await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Unknown);
    let published_revision = authority.observe_latest().await.unwrap().revision();
    let status = sync.status(edit).await.unwrap();
    assert_eq!(status.state, IntentState::Unknown);
    assert_eq!(
        status.error.as_ref().unwrap().commit_id(),
        Some(status.commit_id)
    );
    assert_eq!(remote_bytes(&authority, node).await, b"replacement");
    drop(sync);

    let reopened = Sync::open(authority.clone(), &state).await.unwrap();
    let receipt = reopened.publish(edit).await.unwrap();
    assert_eq!(receipt, published_revision);
    assert_eq!(
        reopened.status(edit).await.unwrap().state,
        IntentState::Committed
    );
    assert_eq!(
        authority.observe_latest().await.unwrap().revision(),
        published_revision
    );
    assert_eq!(remote_bytes(&authority, node).await, b"replacement");
}

#[tokio::test]
async fn unsupported_sync_schema_is_not_repaired() {
    let backend = TestBackend::default();
    let fs = Fs::create(backend.operator()).await.unwrap();
    let authority: Arc<dyn Authority> = Arc::new(fs);
    let temp = tempfile::tempdir().unwrap();
    let state = temp.path().join("sync");
    std::fs::create_dir_all(&state).unwrap();
    let connection = rusqlite::Connection::open(state.join("sync.db")).unwrap();
    connection
        .execute_batch(
            "CREATE TABLE binding(
                singleton INTEGER PRIMARY KEY,
                profile TEXT NOT NULL,
                filesystem BLOB NOT NULL
             );
             INSERT INTO binding VALUES(1, 'yinyang-sync-999', x'00000000000000000000000000000000');",
        )
        .unwrap();
    drop(connection);

    let error = match Sync::open(authority, &state).await {
        Ok(_) => panic!("unsupported Sync schema was opened"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), ErrorKind::Unsupported);
    let bytes = std::fs::read(state.join("sync.db")).unwrap();
    assert!(!bytes.is_empty());
}
