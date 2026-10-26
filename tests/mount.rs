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
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use std::{future::Future, pin::Pin};
use support::TestBackend;
use tokio::sync::Semaphore;
use yinyang::core::service::{MetadataService, ServiceClient, serve};
use yinyang::core::{
    Authority, BackendProfile, CommitOutcome, DataStore, Fs, NodeId, Revision, Snapshot,
    Transaction,
};
use yinyang::mount::Mount;
use yinyang::runtime::{ErrorKind, Runtime};

struct PausingObserveAuthority {
    inner: Arc<dyn Authority>,
    entered: Semaphore,
    release: Semaphore,
    pause_next_observe: AtomicBool,
    fail_next_observe: AtomicBool,
}
impl PausingObserveAuthority {
    fn new(inner: Arc<dyn Authority>) -> Self {
        Self {
            inner,
            entered: Semaphore::new(0),
            release: Semaphore::new(0),
            pause_next_observe: AtomicBool::new(false),
            fail_next_observe: AtomicBool::new(false),
        }
    }
    fn pause_next_observe(&self) {
        self.pause_next_observe.store(true, Ordering::SeqCst);
    }
    fn fail_next_observe(&self) {
        self.fail_next_observe.store(true, Ordering::SeqCst);
    }
}
impl std::fmt::Debug for PausingObserveAuthority {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PausingObserveAuthority")
            .finish_non_exhaustive()
    }
}
impl Authority for PausingObserveAuthority {
    fn data(&self) -> &DataStore {
        self.inner.data()
    }
    fn filesystem(&self) -> NodeId {
        self.inner.filesystem()
    }
    fn root(&self) -> NodeId {
        self.inner.root()
    }
    fn observe_latest(
        &self,
    ) -> Pin<Box<dyn Future<Output = yinyang::core::Result<Snapshot>> + Send + '_>> {
        Box::pin(async move {
            if self.pause_next_observe.swap(false, Ordering::SeqCst) {
                self.entered.add_permits(1);
                self.release.acquire().await.unwrap().forget();
            }
            if self.fail_next_observe.swap(false, Ordering::SeqCst) {
                return Err(yinyang::core::Error::new(
                    yinyang::core::ErrorKind::Storage,
                    "test observe",
                    "injected first-open failure",
                ));
            }
            self.inner.observe_latest().await
        })
    }
    fn observe_revision(
        &self,
        revision: Revision,
    ) -> Pin<Box<dyn Future<Output = yinyang::core::Result<Snapshot>> + Send + '_>> {
        self.inner.observe_revision(revision)
    }
    fn commit_batch<'a>(
        &'a self,
        requests: &'a [Transaction],
    ) -> Pin<Box<dyn Future<Output = yinyang::core::Result<Vec<CommitOutcome>>> + Send + 'a>> {
        self.inner.commit_batch(requests)
    }
}

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

#[tokio::test]
async fn flush_publishes_but_reference_release_does_not() {
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
    let (pending, _) = mount
        .namespace()
        .create_file_with_node(authority.root(), "pending", false)
        .await
        .unwrap();
    assert!(mount.node_status(pending).await.unwrap().is_none());

    let file = mount.open_node(pending, true).await.unwrap();
    file.write(0, b"pending").await.unwrap();
    let state = mount.node_status(pending).await.unwrap().unwrap();
    assert_eq!(state.length, 7);
    assert!(state.pending);
    assert!(
        authority
            .observe_latest()
            .await
            .unwrap()
            .open_file(pending)
            .await
            .unwrap()
            .read(0, 7)
            .await
            .unwrap()
            .is_empty()
    );

    // Releasing the shared reference performs no remote publication.
    drop(file);
    assert!(mount.node_status(pending).await.unwrap().unwrap().pending);
    assert_eq!(
        mount.reclaim(pending).await.unwrap_err().kind(),
        ErrorKind::Busy
    );

    let file = mount.open_node(pending, true).await.unwrap();
    assert_eq!(file.read(0, 7).await.unwrap(), b"pending");
    file.flush().await.unwrap().unwrap();
    assert!(!mount.node_status(pending).await.unwrap().unwrap().pending);
    assert_eq!(
        authority
            .observe_latest()
            .await
            .unwrap()
            .open_file(pending)
            .await
            .unwrap()
            .read(0, 7)
            .await
            .unwrap(),
        b"pending"
    );

    drop(file);
    assert!(mount.node_status(pending).await.unwrap().is_some());
    mount.reclaim(pending).await.unwrap();
    assert!(mount.node_status(pending).await.unwrap().is_none());
}

async fn pausing_mount() -> (
    Arc<PausingObserveAuthority>,
    Mount,
    yinyang::core::NodeId,
    yinyang::core::NodeId,
    tempfile::TempDir,
    tempfile::TempDir,
) {
    let backend = TestBackend::default();
    let inner: Arc<dyn Authority> = Arc::new(
        Fs::create(backend.operator(), BackendProfile::Minio)
            .await
            .unwrap(),
    );
    let setup_temp = tempfile::tempdir().unwrap();
    let setup = Runtime::open(inner.clone(), setup_temp.path())
        .await
        .unwrap();
    let (a, _) = setup
        .create_file_with_node(inner.root(), "a", false)
        .await
        .unwrap();
    let (b, _) = setup
        .create_file_with_node(inner.root(), "b", false)
        .await
        .unwrap();
    drop(setup);

    let pausing = Arc::new(PausingObserveAuthority::new(inner));
    let authority: Arc<dyn Authority> = pausing.clone();
    let mount_temp = tempfile::tempdir().unwrap();
    let mount = Mount::open(authority, mount_temp.path(), false)
        .await
        .unwrap();
    (pausing, mount, a, b, setup_temp, mount_temp)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn node_materialization_is_independent_and_broadcasts_completion() {
    let (pausing, mount, a, b, _setup_temp, _mount_temp) = pausing_mount().await;
    pausing.pause_next_observe();
    let owner = {
        let mount = mount.clone();
        tokio::spawn(async move { mount.open_node(a, false).await })
    };
    pausing.entered.acquire().await.unwrap().forget();
    let b_file = tokio::time::timeout(Duration::from_secs(5), async {
        let mount = mount.clone();
        mount.open_node(b, false).await
    })
    .await
    .expect("opening an independent node waited for another node's remote observation")
    .unwrap();
    drop(b_file);
    let waiters: Vec<_> = (0..2)
        .map(|_| {
            let mount = mount.clone();
            tokio::spawn(async move { mount.open_node(a, false).await })
        })
        .collect();
    tokio::time::sleep(Duration::from_millis(250)).await;

    pausing.release.add_permits(1);
    let a_file = tokio::time::timeout(Duration::from_secs(5), owner)
        .await
        .expect("first open did not complete after the observation was released")
        .unwrap()
        .unwrap();
    let first_id = a_file.status().await.unwrap().id;
    for waiter in waiters {
        let file = tokio::time::timeout(Duration::from_secs(5), waiter)
            .await
            .expect("a same-node waiter was not broadcast awake")
            .unwrap()
            .unwrap();
        assert_eq!(file.status().await.unwrap().id, first_id);
    }
    drop(a_file);
    assert_eq!(mount.status().await.unwrap().handles.len(), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failed_first_open_broadcasts_retry_to_all_waiters() {
    let (pausing, mount, a, _b, _setup_temp, _mount_temp) = pausing_mount().await;
    pausing.pause_next_observe();
    pausing.fail_next_observe();
    let owner = {
        let mount = mount.clone();
        tokio::spawn(async move { mount.open_node(a, false).await })
    };
    pausing.entered.acquire().await.unwrap().forget();
    let waiters: Vec<_> = (0..2)
        .map(|_| {
            let mount = mount.clone();
            tokio::spawn(async move { mount.open_node(a, false).await })
        })
        .collect();
    tokio::time::sleep(Duration::from_millis(250)).await;
    pausing.release.add_permits(1);
    let error = tokio::time::timeout(Duration::from_secs(5), owner)
        .await
        .expect("failed owner did not finish")
        .unwrap()
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Storage);
    let mut waiters = waiters.into_iter();
    let first = tokio::time::timeout(Duration::from_secs(5), waiters.next().unwrap())
        .await
        .expect("first waiter did not retry after owner failure")
        .unwrap()
        .unwrap();
    let first_id = first.status().await.unwrap().id;
    let second = tokio::time::timeout(Duration::from_secs(5), waiters.next().unwrap())
        .await
        .expect("second waiter did not retry after owner failure")
        .unwrap()
        .unwrap();
    assert_eq!(second.status().await.unwrap().id, first_id);
    assert_eq!(mount.status().await.unwrap().handles.len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelled_first_open_broadcasts_retry_to_all_waiters() {
    let (pausing, mount, a, _b, _setup_temp, _mount_temp) = pausing_mount().await;
    pausing.pause_next_observe();
    let cancelled = {
        let mount = mount.clone();
        tokio::spawn(async move { mount.open_node(a, false).await })
    };
    pausing.entered.acquire().await.unwrap().forget();
    let waiters: Vec<_> = (0..2)
        .map(|_| {
            let mount = mount.clone();
            tokio::spawn(async move { mount.open_node(a, false).await })
        })
        .collect();
    tokio::time::sleep(Duration::from_millis(250)).await;
    cancelled.abort();
    assert!(cancelled.await.unwrap_err().is_cancelled());

    let mut waiters = waiters.into_iter();
    let first = tokio::time::timeout(Duration::from_secs(5), waiters.next().unwrap())
        .await
        .expect("first waiter did not retry after cancellation")
        .unwrap()
        .unwrap();
    let first_id = first.status().await.unwrap().id;
    let second = tokio::time::timeout(Duration::from_secs(5), waiters.next().unwrap())
        .await
        .expect("second waiter did not retry after cancellation")
        .unwrap()
        .unwrap();
    assert_eq!(second.status().await.unwrap().id, first_id);
    assert_eq!(mount.status().await.unwrap().handles.len(), 1);
}
