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

async fn exercise(authority: Arc<dyn Authority>, path: &std::path::Path, backend: &TestBackend) {
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
    identity_access(&runtime).await;
    version_reads(&runtime, backend).await;
}

async fn version_reads(runtime: &Runtime, backend: &TestBackend) {
    let root = runtime.authority().root();
    let bytes: Vec<u8> = (0..16 * 65536).map(|i| (i % 251) as u8).collect();
    let snapshot = runtime.authority().observe_latest().await.unwrap();
    let prepared = runtime
        .authority()
        .prepare(&mut bytes.as_slice())
        .await
        .unwrap();
    let mut plan = Planner::new(&snapshot, CommitId::generate());
    let id = plan
        .create_file(root, "large", prepared, false)
        .await
        .unwrap();
    runtime
        .authority()
        .commit(&plan.finish().unwrap())
        .await
        .unwrap();
    let snapshot = runtime.authority().observe_latest().await.unwrap();
    let before = backend.state.lock().unwrap().data_read_bytes;
    let file = snapshot.open_file(id).await.unwrap();
    assert_eq!(file.node().id(), id);
    assert_eq!(file.revision(), snapshot.revision());
    // Metadata shares pack storage with content; opening may read metadata,
    // but must not read the 1 MiB payload or create a stage record.
    assert!(backend.state.lock().unwrap().data_read_bytes - before < 65536);
    assert!(runtime.recoverable().await.unwrap().is_empty());
    let before = backend.state.lock().unwrap().data_read_bytes;
    assert_eq!(file.read(31, 10).await.unwrap(), bytes[31..41]);
    let read_bytes = backend.state.lock().unwrap().data_read_bytes - before;
    assert!((65536..2 * 65536).contains(&read_bytes));
    let before = backend.state.lock().unwrap().data_read_bytes;
    assert!(file.read(u64::MAX, 100).await.unwrap().is_empty());
    assert!(file.read(31, 0).await.unwrap().is_empty());
    assert!(
        file.read_range(0..bytes.len() as u64 + 1, &mut Vec::new())
            .await
            .is_err()
    );
    assert_eq!(backend.state.lock().unwrap().data_read_bytes, before);
    let clone = file.clone();
    let (a, b) = tokio::join!(
        file.read(65530, 20),
        clone.read(bytes.len() as u64 - 4, 100)
    );
    assert_eq!(a.unwrap(), bytes[65530..65550]);
    assert_eq!(b.unwrap(), bytes[bytes.len() - 4..]);
    let mut writer = runtime.open_node(id, true).await.unwrap();
    writer.write(0, b"changed").await.unwrap();
    writer.close().await.unwrap();
    runtime.rename(id, root, "large-moved").await.unwrap();
    let latest = runtime
        .authority()
        .observe_latest()
        .await
        .unwrap()
        .open_file(id)
        .await
        .unwrap();
    assert_eq!(latest.read(0, 7).await.unwrap(), b"changed");
    runtime.unlink(id).await.unwrap();
    assert_eq!(file.read(0, 10).await.unwrap(), bytes[..10]);
    let retained = runtime
        .authority()
        .observe_revision(file.revision())
        .await
        .unwrap()
        .open_file(id)
        .await
        .unwrap();
    assert_eq!(retained.read(0, 10).await.unwrap(), bytes[..10]);
    // Corrupt the exact packed bytes read by this version; verified data must
    // not escape merely because a previous read of the same version succeeded.
    {
        let mut state = backend.state.lock().unwrap();
        let object = state
            .objects
            .values_mut()
            .find(|v| v.bytes.starts_with(&bytes[..65536]))
            .unwrap();
        object.bytes[0] ^= 1;
    }
    let mut destination = Vec::new();
    let err = file.read_range(0..10, &mut destination).await.unwrap_err();
    assert_eq!(err.kind(), yinyang::core::ErrorKind::Corrupt);
    assert!(destination.is_empty());
    assert!(file.read(0, 10).await.is_err());
}

async fn identity_access(runtime: &Runtime) {
    let root = runtime.authority().root();
    runtime.create_directory(root, "dir").await.unwrap();
    let snapshot = runtime.authority().observe_latest().await.unwrap();
    let dir = snapshot.lookup(root, "dir").await.unwrap().unwrap().node_id;
    runtime.create_file(dir, "a").await.unwrap();
    runtime.create_file(dir, "b").await.unwrap();
    let mut file = runtime.open_file("dir/a", true).await.unwrap();
    file.write(0, b"original").await.unwrap();
    file.close().await.unwrap();
    let pinned = runtime.authority().observe_latest().await.unwrap();
    let first = pinned.scan(dir, None, 1).await.unwrap();
    let node = first.entries[0].node_id;
    assert_eq!(first.entries[0].name, "a");
    assert_eq!(pinned.node(node).await.unwrap().unwrap().id(), node);
    runtime.rename(node, root, "moved").await.unwrap();
    runtime.create_file(dir, "a").await.unwrap();
    let latest = runtime.authority().observe_latest().await.unwrap();
    assert!(latest.scan(dir, first.next.as_ref(), 1).await.is_err());
    let second = pinned.scan(dir, first.next.as_ref(), 1).await.unwrap();
    assert_eq!(second.entries[0].name, "b");
    assert!(second.next.is_none());
    let mut by_id = runtime.open_node(node, true).await.unwrap();
    assert_eq!(by_id.read(0, 20).await.unwrap(), b"original");
    let mut replacement = runtime.open_file("dir/a", false).await.unwrap();
    assert_ne!(replacement.status().await.unwrap().node, node);
    assert!(replacement.read(0, 20).await.unwrap().is_empty());
    replacement.close().await.unwrap();
    by_id.write(0, b"modified").await.unwrap();
    by_id.close().await.unwrap();
    let mut historical = runtime
        .open_node_at(pinned.revision(), node, true)
        .await
        .unwrap();
    assert_eq!(historical.read(0, 20).await.unwrap(), b"original");
    historical.write(0, b"obsolete").await.unwrap();
    assert!(matches!(historical.fsync().await, Err(Error::Conflict)));
    historical.abort().await.unwrap();
    runtime.unlink(node).await.unwrap();
    assert!(runtime.open_node(node, false).await.is_err());
    let mut retained = runtime
        .open_node_at(pinned.revision(), node, false)
        .await
        .unwrap();
    assert_eq!(retained.read(0, 20).await.unwrap(), b"original");
    retained.close().await.unwrap();
    assert!(runtime.open_node(root, false).await.is_err());
    let foreign = Fs::create(TestBackend::default().operator(), BackendProfile::Minio)
        .await
        .unwrap();
    let revision = foreign.observe_latest().await.unwrap().revision();
    assert!(runtime.open_node_at(revision, node, false).await.is_err());
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn file_operations_share_object_and_service_contract() {
    let temp = tempfile::tempdir().unwrap();
    let backend = TestBackend::default();
    let object = Fs::create(backend.operator(), BackendProfile::Minio)
        .await
        .unwrap();
    exercise(Arc::new(object), &temp.path().join("object"), &backend).await;
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
    exercise(Arc::new(client), &temp.path().join("service"), &backend).await;
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
