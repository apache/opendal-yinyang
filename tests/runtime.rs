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
use yinyang::core::{Authority, BackendProfile, CommitId, CommitOutcome, Fs, Planner};
use yinyang::runtime::{Error, Runtime};

async fn exercise(authority: Arc<dyn Authority>, path: &std::path::Path) {
    let runtime = Runtime::open(authority.clone(), path).await.unwrap();
    runtime.create_file(authority.root(), "file").await.unwrap();
    let mut first = runtime.open_file("file", true).await.unwrap();
    let mut stale = runtime.open_file("file", true).await.unwrap();
    first.write(3, b"abc").await.unwrap();
    assert_eq!(first.append(b"def").await.unwrap(), 6);
    assert_eq!(first.read(0, 20).await.unwrap(), b"\0\0\0abcdef");
    first.truncate(5).await.unwrap();
    first.truncate(8).await.unwrap();
    assert_eq!(first.read(0, 20).await.unwrap(), b"\0\0\0ab\0\0\0");
    let before = first.sync_local().await.unwrap();
    assert!(before.pending);
    assert_eq!(before.remote_generation, 0);
    let receipt = first.fsync().await.unwrap().unwrap();
    assert_eq!(
        first.status().await.unwrap().remote_revision,
        receipt.cursor.revision
    );
    assert!(!first.status().await.unwrap().pending);
    assert!(stale.read(0, 100).await.unwrap().is_empty());
    stale.write(0, b"stale").await.unwrap();
    assert!(matches!(stale.fsync().await, Err(Error::Conflict)));
    assert!(matches!(stale.close().await, Err(Error::Conflict)));
    assert_eq!(stale.read(0, 20).await.unwrap(), b"stale");
    stale.abort().await.unwrap();

    let node = first.status().await.unwrap().node;
    runtime
        .rename(node, authority.root(), "renamed")
        .await
        .unwrap();
    first.write(0, b"new").await.unwrap();
    first.flush().await.unwrap();
    let mut reopened = runtime.open_file("renamed", false).await.unwrap();
    assert_eq!(reopened.read(0, 3).await.unwrap(), b"new");
    runtime.unlink(node).await.unwrap();
    assert_eq!(reopened.read(0, 3).await.unwrap(), b"new");
    reopened.close().await.unwrap();
    first.append(b"after unlink").await.unwrap();
    assert!(matches!(first.commit().await, Err(Error::Conflict)));
    assert!(
        authority
            .observe_latest()
            .await
            .unwrap()
            .resolve("renamed")
            .await
            .unwrap()
            .is_none()
    );
    first.abort().await.unwrap();
    assert!(!runtime.status().await.unwrap().errors.is_empty());
    let sequence = runtime
        .status()
        .await
        .unwrap()
        .errors
        .last()
        .unwrap()
        .sequence;
    runtime.acknowledge_errors(sequence).await.unwrap();
    assert!(runtime.status().await.unwrap().errors.is_empty());
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn file_operations_share_object_and_service_contract() {
    let temp = tempfile::tempdir().unwrap();
    let backend = TestBackend::default();
    let object = Fs::create(backend.operator(), BackendProfile::Minio)
        .await
        .unwrap();
    exercise(Arc::new(object), &temp.path().join("object")).await;
    let backend = TestBackend::default();
    let service = MetadataService::create(temp.path().join("metadata.db"), backend.operator())
        .await
        .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let token = "test-only-service-token-at-least-32-bytes";
    let server = tokio::spawn(serve(listener, service, token.into()));
    let client = ServiceClient::connect(addr, token.into(), backend.operator())
        .await
        .unwrap();
    exercise(Arc::new(client), &temp.path().join("service")).await;
    server.abort();
}

#[tokio::test]
async fn durable_staging_reopens_and_preserves_concurrent_generation_conflict() {
    let temp = tempfile::tempdir().unwrap();
    let backend = TestBackend::default();
    let authority: Arc<dyn Authority> = Arc::new(
        Fs::create(backend.operator(), BackendProfile::Minio)
            .await
            .unwrap(),
    );
    let runtime = Runtime::open(authority.clone(), temp.path()).await.unwrap();
    assert!(Runtime::open(authority.clone(), temp.path()).await.is_err());
    runtime.create_file(authority.root(), "file").await.unwrap();
    let mut file = runtime.open_file("file", true).await.unwrap();
    // Cross chunk boundaries, append and truncate without resurrecting truncated bytes.
    file.write(65534, b"abcdefgh").await.unwrap();
    file.truncate(65536).await.unwrap();
    file.truncate(65544).await.unwrap();
    let id = file.id();
    assert!(runtime.recover(id).await.is_err());
    drop(file);
    drop(runtime);
    let runtime = Runtime::open(authority.clone(), temp.path()).await.unwrap();
    assert_eq!(runtime.recoverable().await.unwrap().len(), 1);
    let mut file = runtime.recover(id).await.unwrap();
    assert_eq!(file.read(65534, 20).await.unwrap(), b"ab\0\0\0\0\0\0\0\0");
    file.fsync().await.unwrap();
    file.append(b"retained").await.unwrap();
    let snapshot = authority.observe_latest().await.unwrap();
    let mut plan = Planner::new(&snapshot, CommitId::generate());
    let content = authority
        .prepare(&mut b"other writer".as_slice())
        .await
        .unwrap();
    plan.set_content(file.status().await.unwrap().node, content)
        .await
        .unwrap();
    assert!(matches!(
        authority.commit(&plan.finish().unwrap()).await.unwrap(),
        CommitOutcome::Committed(_)
    ));
    assert!(matches!(file.fsync().await, Err(Error::Conflict)));
    drop(file);
    drop(runtime);
    let runtime = Runtime::open(authority.clone(), temp.path()).await.unwrap();
    let mut file = runtime.recover(id).await.unwrap();
    assert!(file.status().await.unwrap().conflict);
    assert!(file.status().await.unwrap().error.is_some());
    assert!(matches!(file.fsync().await, Err(Error::Conflict)));
    assert_eq!(file.read(65544, 8).await.unwrap(), b"retained");
    file.abort().await.unwrap();
}

#[tokio::test]
async fn upload_failure_and_unknown_publication_keep_frozen_identity() {
    let temp = tempfile::tempdir().unwrap();
    let backend = TestBackend::default();
    let authority: Arc<dyn Authority> = Arc::new(
        Fs::create(backend.operator(), BackendProfile::Minio)
            .await
            .unwrap(),
    );
    let runtime = Runtime::open(authority.clone(), temp.path()).await.unwrap();
    runtime.create_file(authority.root(), "file").await.unwrap();
    let mut file = runtime.open_file("file", true).await.unwrap();
    file.append(b"recover me").await.unwrap();
    backend.state.lock().unwrap().fail_data_close = true;
    assert!(file.fsync().await.is_err());
    assert!(file.status().await.unwrap().pending);
    assert!(file.status().await.unwrap().error.is_some());
    assert!(file.close().await.is_err());
    backend.state.lock().unwrap().fail_data_close = false;
    backend.state.lock().unwrap().delay_next_head = true;
    let Err(Error::Unknown(id)) = file.fsync().await else {
        panic!("expected unknown");
    };
    assert!(file.status().await.unwrap().frozen);
    assert!(file.abort().await.is_err());
    assert!(file.write(0, b"different").await.is_err());
    let handle = file.id();
    drop(file);
    drop(runtime);
    // A later retry fences the delayed object CAS without changing the original request.
    let runtime = Runtime::open(authority.clone(), temp.path()).await.unwrap();
    let mut file = runtime.recover(handle).await.unwrap();
    let receipt = file.fsync().await.unwrap().unwrap();
    assert_eq!(receipt.commit_id, id);
    assert!(!file.status().await.unwrap().pending);
    file.close().await.unwrap();
    assert!(!runtime.status().await.unwrap().errors.is_empty());
    let mut reopened = runtime.open_file("file", false).await.unwrap();
    assert_eq!(reopened.read(0, 20).await.unwrap(), b"recover me");
    reopened.close().await.unwrap();
}
#[tokio::test]
async fn clean_handle_reports_unacknowledged_errors_on_close() {
    let temp = tempfile::tempdir().unwrap();
    let backend = TestBackend::default();
    let authority: Arc<dyn Authority> = Arc::new(
        Fs::create(backend.operator(), BackendProfile::Minio)
            .await
            .unwrap(),
    );
    let runtime = Runtime::open(authority.clone(), temp.path()).await.unwrap();
    runtime.create_file(authority.root(), "file").await.unwrap();
    let mut file = runtime.open_file("file", false).await.unwrap();
    assert!(file.write(0, b"forbidden").await.is_err());
    assert!(file.close().await.is_err());
    file.acknowledge_error().await.unwrap();
    file.close().await.unwrap();
    assert!(!runtime.status().await.unwrap().errors.is_empty());
}
