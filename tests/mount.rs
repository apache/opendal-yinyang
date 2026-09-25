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
use yinyang::mount::Mount;
use yinyang::runtime::{ErrorKind, Runtime};

async fn exercise(authority: Arc<dyn Authority>) {
    let temp = tempfile::tempdir().unwrap();
    let mount = Mount::open(authority.clone(), temp.path(), false)
        .await
        .unwrap();
    mount
        .namespace()
        .create_file(authority.root(), "file")
        .await
        .unwrap();
    let id = authority
        .observe_latest()
        .await
        .unwrap()
        .resolve("file")
        .await
        .unwrap()
        .unwrap()
        .id();
    let a = mount.open_node(id, true).await.unwrap();
    let b = mount.open_node(id, false).await.unwrap();
    assert_eq!(a.status().await.unwrap().id, b.status().await.unwrap().id);
    a.write(0, b"first").await.unwrap();
    assert_eq!(b.read(0, 5).await.unwrap(), b"first");
    assert_eq!(
        b.write(0, b"bad").await.unwrap_err().kind(),
        ErrorKind::ReadOnly
    );
    assert_eq!(mount.refresh(id).await.unwrap_err().kind(), ErrorKind::Busy);
    drop(b);
    a.write(0, b"after").await.unwrap();
    a.fsync().await.unwrap();
    assert_eq!(
        authority
            .observe_latest()
            .await
            .unwrap()
            .open_file(id)
            .await
            .unwrap()
            .read(0, 5)
            .await
            .unwrap(),
        b"after"
    );
    let b = mount.open_node(id, true).await.unwrap();
    let (left, right) = tokio::join!(a.append(b"A"), b.append(b"B"));
    assert_ne!(left.unwrap(), right.unwrap());
    assert_eq!(a.status().await.unwrap().length, 7);
    mount.fsync().await.unwrap();
    assert_eq!(mount.reclaim(id).await.unwrap_err().kind(), ErrorKind::Busy);
    let remote = tempfile::tempdir().unwrap();
    let runtime = Runtime::open(authority.clone(), remote.path())
        .await
        .unwrap();
    let mut editor = runtime.open_node(id, true).await.unwrap();
    editor.write(0, b"newer").await.unwrap();
    editor.close().await.unwrap();
    assert_eq!(a.read(0, 5).await.unwrap(), b"after");
    mount.refresh(id).await.unwrap();
    assert_eq!(a.read(0, 5).await.unwrap(), b"newer");
    assert_eq!(b.read(0, 5).await.unwrap(), b"newer");
    a.write(0, b"local").await.unwrap();
    let mut editor = runtime.open_node(id, true).await.unwrap();
    editor.write(0, b"other").await.unwrap();
    editor.close().await.unwrap();
    assert_eq!(a.fsync().await.unwrap_err().kind(), ErrorKind::Conflict);
    drop(a);
    drop(b);
    assert_eq!(mount.reclaim(id).await.unwrap_err().kind(), ErrorKind::Busy);
    drop(mount);
    let mount = Mount::open(authority, temp.path(), false).await.unwrap();
    let a = mount.open_node(id, true).await.unwrap();
    assert_eq!(a.read(0, 5).await.unwrap(), b"local");
    assert!(a.status().await.unwrap().conflict);
    assert_eq!(a.fsync().await.unwrap_err().kind(), ErrorKind::Conflict);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shared_nodes_use_both_authorities() {
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
    let token = "mount-test-only-service-token-at-least-32-bytes";
    let task = tokio::spawn(serve(listener, service, token.into()));
    let client = ServiceClient::connect(address, token.into(), backend.operator())
        .await
        .unwrap();
    exercise(Arc::new(client)).await;
    task.abort();
}

#[tokio::test]
async fn frozen_mount_write_recovers_original_request() {
    let backend = TestBackend::default();
    let authority: Arc<dyn Authority> = Arc::new(
        Fs::create(backend.operator(), BackendProfile::Minio)
            .await
            .unwrap(),
    );
    let temp = tempfile::tempdir().unwrap();
    let mount = Mount::open(authority.clone(), temp.path(), false)
        .await
        .unwrap();
    mount
        .namespace()
        .create_file(authority.root(), "file")
        .await
        .unwrap();
    let id = authority
        .observe_latest()
        .await
        .unwrap()
        .resolve("file")
        .await
        .unwrap()
        .unwrap()
        .id();
    let file = mount.open_node(id, true).await.unwrap();
    file.write(0, b"recover").await.unwrap();
    backend.state.lock().unwrap().delay_next_head = true;
    let error = file.fsync().await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Unknown);
    let commit = error.commit_id().unwrap();
    drop(file);
    drop(mount);
    let mount = Mount::open(authority, temp.path(), false).await.unwrap();
    let file = mount.open_node(id, true).await.unwrap();
    assert_eq!(file.fsync().await.unwrap().unwrap().commit_id, commit);
    drop(file);
    mount.reclaim(id).await.unwrap();
    assert!(mount.status().await.unwrap().handles.is_empty());
}
