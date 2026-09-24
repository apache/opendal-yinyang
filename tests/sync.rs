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
use std::sync::Arc;
use support::TestBackend;
use yinyang::core::service::{MetadataService, ServiceClient, serve};
use yinyang::core::{Authority, BackendProfile, Fs};
use yinyang::runtime::{ErrorKind, Runtime};
use yinyang::sync::Sync;

async fn exercise(authority: Arc<dyn Authority>) {
    let temp = tempfile::tempdir().unwrap();
    let runtime = Runtime::open(authority.clone(), temp.path().join("editor"))
        .await
        .unwrap();
    runtime.create_file(authority.root(), "file").await.unwrap();
    let baseline = authority.observe_latest().await.unwrap();
    let node = baseline.resolve("file").await.unwrap().unwrap().id();
    let sync = Sync::open(authority.clone(), temp.path().join("sync"))
        .await
        .unwrap();
    let edit = sync
        .stage(node, baseline.revision(), &mut &b"local"[..])
        .await
        .unwrap();
    assert!(sync.status(edit).await.unwrap().pending);
    assert_eq!(
        sync.stage(node, baseline.revision(), &mut &b"local"[..])
            .await
            .unwrap(),
        edit
    );
    assert_eq!(sync.edits().await.unwrap(), vec![edit]);
    let published = sync.publish(edit).await.unwrap();
    assert_eq!(sync.publish(edit).await.unwrap(), published);
    drop(sync);
    let sync = Sync::open(authority.clone(), temp.path().join("sync"))
        .await
        .unwrap();
    assert_eq!(sync.publish(edit).await.unwrap(), published);
    assert_eq!(
        sync.stage(node, baseline.revision(), &mut &b"local"[..])
            .await
            .unwrap(),
        edit
    );
    let pinned = baseline.open_file(node).await.unwrap();
    assert!(pinned.read(0, 10).await.unwrap().is_empty());
    assert_eq!(
        authority
            .observe_revision(published)
            .await
            .unwrap()
            .open_file(node)
            .await
            .unwrap()
            .read(0, 10)
            .await
            .unwrap(),
        b"local"
    );
    let stale = sync
        .stage(node, baseline.revision(), &mut &b"offline"[..])
        .await
        .unwrap();
    assert_eq!(
        sync.publish(stale).await.unwrap_err().kind(),
        ErrorKind::Conflict
    );
    assert_eq!(sync.read(stale, 0, 10).await.unwrap(), b"offline");
    // Empty-to-empty intent must still protect its original baseline.
    let empty = sync
        .stage(node, baseline.revision(), &mut &b""[..])
        .await
        .unwrap();
    assert_eq!(
        sync.publish(empty).await.unwrap_err().kind(),
        ErrorKind::Conflict
    );
    assert!(sync.read(empty, 0, 10).await.unwrap().is_empty());
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sync_is_independent_and_conditional_in_both_modes() {
    let backend = TestBackend::default();
    exercise(Arc::new(
        Fs::create(backend.operator(), BackendProfile::Minio)
            .await
            .unwrap(),
    ))
    .await;
    let backend = TestBackend::default();
    let temp = tempfile::tempdir().unwrap();
    let service = MetadataService::create(temp.path().join("metadata.db"), backend.operator())
        .await
        .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let token = "sync-test-only-service-token-at-least-32-bytes";
    let server = tokio::spawn(serve(listener, service, token.into()));
    let client = ServiceClient::connect(address, token.into(), backend.operator())
        .await
        .unwrap();
    exercise(Arc::new(client)).await;
    server.abort();
}
#[tokio::test]
async fn original_upload_survives_unknown_and_restart() {
    let backend = TestBackend::default();
    let authority: Arc<dyn Authority> = Arc::new(
        Fs::create(backend.operator(), BackendProfile::Minio)
            .await
            .unwrap(),
    );
    let temp = tempfile::tempdir().unwrap();
    let runtime = Runtime::open(authority.clone(), temp.path().join("editor"))
        .await
        .unwrap();
    runtime.create_file(authority.root(), "file").await.unwrap();
    let base = authority.observe_latest().await.unwrap();
    let node = base.resolve("file").await.unwrap().unwrap().id();
    let sync = Sync::open(authority.clone(), temp.path().join("sync"))
        .await
        .unwrap();
    let edit = sync
        .stage(node, base.revision(), &mut &b"recover"[..])
        .await
        .unwrap();
    backend.state.lock().unwrap().delay_next_head = true;
    let error = sync.publish(edit).await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Unknown);
    let commit = error.commit_id().unwrap();
    drop(sync);
    let sync = Sync::open(authority.clone(), temp.path().join("sync"))
        .await
        .unwrap();
    assert_eq!(
        sync.status(edit).await.unwrap().error.unwrap().commit_id(),
        Some(commit)
    );
    let revision = sync.publish(edit).await.unwrap();
    let receipt = authority
        .observe_latest()
        .await
        .unwrap()
        .receipt(commit)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(revision, receipt.cursor.revision);
}
