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

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;
use std::{future::Future, pin::Pin};
use support::TestBackend;
use tokio::sync::Semaphore;
use yinyang::core::service::{MetadataService, ServiceClient, serve};
use yinyang::core::{
    Authority, CommitOutcome, DataStore, Fs, NodeId, Revision, Snapshot, Transaction,
};
use yinyang::mount::Mount;
use yinyang::runtime::{ErrorKind, Runtime};
use yinyang_test_support as support;

struct PausingObserveAuthority {
    inner: Arc<dyn Authority>,
    entered: Semaphore,
    release: Semaphore,
    pause_next_observe: AtomicBool,
    fail_next_observe: AtomicBool,
    commit_entered: Semaphore,
    commit_release: Semaphore,
    pause_after_commits: AtomicUsize,
}
impl PausingObserveAuthority {
    fn new(inner: Arc<dyn Authority>) -> Self {
        Self {
            inner,
            entered: Semaphore::new(0),
            release: Semaphore::new(0),
            pause_next_observe: AtomicBool::new(false),
            fail_next_observe: AtomicBool::new(false),
            commit_entered: Semaphore::new(0),
            commit_release: Semaphore::new(0),
            pause_after_commits: AtomicUsize::new(0),
        }
    }
    fn pause_next_observe(&self) {
        self.pause_next_observe.store(true, Ordering::SeqCst);
    }
    fn fail_next_observe(&self) {
        self.fail_next_observe.store(true, Ordering::SeqCst);
    }
    fn pause_after_commits(&self, commits: usize) {
        self.pause_after_commits.store(commits, Ordering::SeqCst);
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
        Box::pin(async move {
            let mut remaining = self.pause_after_commits.load(Ordering::SeqCst);
            let mut pause = None;
            while remaining > 0 {
                match self.pause_after_commits.compare_exchange_weak(
                    remaining,
                    remaining - 1,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                ) {
                    Ok(previous) => {
                        pause = Some(previous);
                        break;
                    }
                    Err(actual) => remaining = actual,
                }
            }
            if pause == Some(1) {
                self.commit_entered.add_permits(1);
                self.commit_release.acquire().await.unwrap().forget();
            }
            self.inner.commit_batch(requests).await
        })
    }
}

fn assert_pending<F: Future + Unpin>(future: &mut F) {
    let waker = std::task::Waker::noop();
    let mut context = std::task::Context::from_waker(waker);
    assert!(std::pin::Pin::new(future).poll(&mut context).is_pending());
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
    exercise(Arc::new(Fs::create(backend.operator()).await.unwrap())).await;
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
    let authority: Arc<dyn Authority> = Arc::new(Fs::create(backend.operator()).await.unwrap());
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
    let authority: Arc<dyn Authority> = Arc::new(Fs::create(backend.operator()).await.unwrap());
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

#[tokio::test]
async fn set_executable_serializes_with_dirty_mount_handle() {
    let backend = TestBackend::default();
    let authority: Arc<dyn Authority> = Arc::new(Fs::create(backend.operator()).await.unwrap());
    let temp = tempfile::tempdir().unwrap();
    let mount = Mount::open(authority.clone(), temp.path(), false)
        .await
        .unwrap();
    let (node, _) = mount
        .namespace()
        .create_file_with_node(authority.root(), "file", false)
        .await
        .unwrap();
    let file = mount.open_node(node, true).await.unwrap();
    file.write(0, b"dirty").await.unwrap();
    file.set_executable(true).await.unwrap();
    assert!(
        authority
            .observe_latest()
            .await
            .unwrap()
            .node(node)
            .await
            .unwrap()
            .unwrap()
            .executable()
    );
    assert_eq!(
        authority
            .observe_latest()
            .await
            .unwrap()
            .open_file(node)
            .await
            .unwrap()
            .read(0, 5)
            .await
            .unwrap(),
        b"dirty"
    );
    file.write(5, b"more").await.unwrap();
    file.fsync().await.unwrap();
    assert_eq!(
        authority
            .observe_latest()
            .await
            .unwrap()
            .open_file(node)
            .await
            .unwrap()
            .read(0, 9)
            .await
            .unwrap(),
        b"dirtymore"
    );
    let read_only = mount.open_node(node, false).await.unwrap();
    assert_eq!(
        read_only.set_executable(false).await.unwrap_err().kind(),
        ErrorKind::ReadOnly
    );

    let (retry, _) = mount
        .namespace()
        .create_file_with_node(authority.root(), "retry", false)
        .await
        .unwrap();
    let retry_file = mount.open_node(retry, true).await.unwrap();
    retry_file.write(0, b"staged").await.unwrap();
    backend.state.lock().unwrap().fail_data_close = true;
    assert_eq!(
        retry_file.set_executable(true).await.unwrap_err().kind(),
        ErrorKind::Storage
    );
    assert!(retry_file.status().await.unwrap().pending);
    assert!(retry_file.status().await.unwrap().error.is_some());
    backend.state.lock().unwrap().fail_data_close = false;
    retry_file.set_executable(true).await.unwrap();
    assert!(!retry_file.status().await.unwrap().pending);
    assert_eq!(
        authority
            .observe_latest()
            .await
            .unwrap()
            .open_file(retry)
            .await
            .unwrap()
            .read(0, 6)
            .await
            .unwrap(),
        b"staged"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn set_executable_conflicts_if_content_changes_after_publish() {
    let (pausing, mount, node, _b, _setup_temp, _mount_temp) = pausing_mount().await;
    let file = mount.open_node(node, true).await.unwrap();
    file.write(0, b"staged").await.unwrap();
    pausing.pause_after_commits(2);
    let mut task = Box::pin(file.set_executable(true));
    tokio::select! {
        _ = pausing.commit_entered.acquire() => {}
        _ = &mut task => panic!("mode commit completed before the gate paused"),
    }

    let other_temp = tempfile::tempdir().unwrap();
    let other = Runtime::open(pausing.inner.clone(), other_temp.path())
        .await
        .unwrap();
    let mut editor = other.open_node(node, true).await.unwrap();
    editor.write(0, b"external").await.unwrap();
    editor.close().await.unwrap();
    pausing.commit_release.add_permits(1);

    let error = tokio::time::timeout(Duration::from_secs(5), &mut task)
        .await
        .expect("mode commit did not finish")
        .expect_err("mode commit unexpectedly succeeded");
    assert_eq!(error.kind(), ErrorKind::Conflict);
    drop(task);
    assert!(file.status().await.unwrap().error.is_some());
    assert_eq!(
        pausing
            .inner
            .observe_latest()
            .await
            .unwrap()
            .open_file(node)
            .await
            .unwrap()
            .read(0, 8)
            .await
            .unwrap(),
        b"external"
    );
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
    let inner: Arc<dyn Authority> = Arc::new(Fs::create(backend.operator()).await.unwrap());
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
    let mut owner = Box::pin(mount.open_node(a, false));
    assert_pending(&mut owner);
    pausing.entered.acquire().await.unwrap().forget();
    let mut independent = Box::pin(mount.open_node(b, false));
    let b_file = tokio::time::timeout(Duration::from_secs(5), &mut independent)
        .await
        .expect("opening an independent node waited for another node's remote observation")
        .expect("independent open failed");
    drop(b_file);

    let mut first = Box::pin(mount.open_node(a, false));
    let mut second = Box::pin(mount.open_node(a, false));
    assert_pending(&mut first);
    assert_pending(&mut second);

    pausing.release.add_permits(1);
    let a_file = tokio::time::timeout(Duration::from_secs(5), &mut owner)
        .await
        .expect("first open did not complete after the observation was released")
        .expect("first open failed");
    let shared = a_file.status().await.unwrap().id;
    let first_file = tokio::time::timeout(Duration::from_secs(5), &mut first)
        .await
        .expect("a same-node waiter was not broadcast awake")
        .expect("same-node waiter open failed");
    let second_file = tokio::time::timeout(Duration::from_secs(5), &mut second)
        .await
        .expect("a same-node waiter was not broadcast awake")
        .expect("same-node waiter open failed");
    assert_eq!(first_file.status().await.unwrap().id, shared);
    assert_eq!(second_file.status().await.unwrap().id, shared);
    drop(a_file);
    assert_eq!(mount.status().await.unwrap().handles.len(), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failed_first_open_broadcasts_retry_to_all_waiters() {
    let (pausing, mount, a, _b, _setup_temp, _mount_temp) = pausing_mount().await;
    pausing.pause_next_observe();
    pausing.fail_next_observe();
    let mut owner = Box::pin(mount.open_node(a, false));
    assert_pending(&mut owner);
    pausing.entered.acquire().await.unwrap().forget();
    let mut first = Box::pin(mount.open_node(a, false));
    let mut second = Box::pin(mount.open_node(a, false));
    assert_pending(&mut first);
    assert_pending(&mut second);
    pausing.release.add_permits(1);
    let error = tokio::time::timeout(Duration::from_secs(5), &mut owner)
        .await
        .expect("failed owner did not finish")
        .err()
        .expect("failed owner returned a file");
    assert_eq!(error.kind(), ErrorKind::Storage);
    let first_file = tokio::time::timeout(Duration::from_secs(5), &mut first)
        .await
        .expect("first waiter did not retry after owner failure")
        .expect("first waiter open failed");
    let shared = first_file.status().await.unwrap().id;
    let second_file = tokio::time::timeout(Duration::from_secs(5), &mut second)
        .await
        .expect("second waiter did not retry after owner failure")
        .expect("second waiter open failed");
    assert_eq!(second_file.status().await.unwrap().id, shared);
    assert_eq!(mount.status().await.unwrap().handles.len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelled_first_open_broadcasts_retry_to_all_waiters() {
    let (pausing, mount, a, _b, _setup_temp, _mount_temp) = pausing_mount().await;
    pausing.pause_next_observe();
    let mut owner = Box::pin(mount.open_node(a, false));
    assert_pending(&mut owner);
    pausing.entered.acquire().await.unwrap().forget();
    let mut first = Box::pin(mount.open_node(a, false));
    let mut second = Box::pin(mount.open_node(a, false));
    assert_pending(&mut first);
    assert_pending(&mut second);
    drop(owner);

    let first_file = tokio::time::timeout(Duration::from_secs(5), &mut first)
        .await
        .expect("first waiter did not retry after cancellation")
        .expect("first waiter open failed");
    let shared = first_file.status().await.unwrap().id;
    let second_file = tokio::time::timeout(Duration::from_secs(5), &mut second)
        .await
        .expect("second waiter did not retry after cancellation")
        .expect("second waiter open failed");
    assert_eq!(second_file.status().await.unwrap().id, shared);
    assert_eq!(mount.status().await.unwrap().handles.len(), 1);
}

#[tokio::test]
async fn read_only_range_observations_pin_reads_without_staging() {
    const BLOCK: usize = 64 * 1024;

    let backend = TestBackend::default();
    let authority: Arc<dyn Authority> = Arc::new(Fs::create(backend.operator()).await.unwrap());
    let setup = tempfile::tempdir().unwrap();
    let writer = Runtime::open(authority.clone(), setup.path())
        .await
        .unwrap();
    let (id, _) = writer
        .create_file_with_node(authority.root(), "large", false)
        .await
        .unwrap();
    let contents: Vec<u8> = (0..BLOCK * 8).map(|index| (index % 251) as u8).collect();
    let mut file = writer.open_node(id, true).await.unwrap();
    file.write(0, &contents).await.unwrap();
    file.close().await.unwrap();
    drop(writer);
    backend.state.lock().unwrap().data_read_bytes = 0;

    let mount_temp = tempfile::tempdir().unwrap();
    let mount = Mount::open(authority.clone(), mount_temp.path(), true)
        .await
        .unwrap();
    let first = mount.open_read_only_node(id).await.unwrap();
    let second = mount.open_read_only_node(id).await.unwrap();
    assert_eq!(first.version().revision(), second.version().revision());
    let old_revision = first.version().revision();
    assert!(mount.status().await.unwrap().handles.is_empty());

    let offset = (BLOCK * 5 - 2048) as u64;
    let expected = &contents[offset as usize..offset as usize + 4096];
    assert_eq!(first.read(offset, 4096).await.unwrap(), expected);
    assert_eq!(second.read(offset, 4096).await.unwrap(), expected);
    let bytes_read = backend.state.lock().unwrap().data_read_bytes;
    assert!(
        bytes_read < contents.len() as u64,
        "a 4 KiB range read fetched the whole {}-byte file",
        contents.len()
    );

    let update_temp = tempfile::tempdir().unwrap();
    let updater = Runtime::open(authority.clone(), update_temp.path())
        .await
        .unwrap();
    let mut fresh = updater.open_node(id, true).await.unwrap();
    fresh.write(0, b"new version").await.unwrap();
    fresh.close().await.unwrap();

    assert_eq!(
        first.read(0, 11).await.unwrap(),
        &contents[..11],
        "an existing range handle was rebased onto a newer remote version"
    );
    drop(first);
    drop(second);

    let current = mount.open_read_only_node(id).await.unwrap();
    assert_eq!(current.read(0, 11).await.unwrap(), b"new version");
    assert_ne!(current.version().revision(), old_revision);
}

#[tokio::test]
async fn writable_mount_rejects_unshared_range_observations() {
    let backend = TestBackend::default();
    let authority: Arc<dyn Authority> = Arc::new(Fs::create(backend.operator()).await.unwrap());
    let temp = tempfile::tempdir().unwrap();
    let mount = Mount::open(authority, temp.path(), false).await.unwrap();
    assert_eq!(
        mount
            .open_read_only_node(yinyang::core::NodeId::generate())
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Unsupported
    );
}

async fn pausing_read_only_mount() -> (
    Arc<PausingObserveAuthority>,
    Mount,
    yinyang::core::NodeId,
    tempfile::TempDir,
    tempfile::TempDir,
) {
    let backend = TestBackend::default();
    let authority: Arc<dyn Authority> = Arc::new(Fs::create(backend.operator()).await.unwrap());
    let setup_temp = tempfile::tempdir().unwrap();
    let setup = Runtime::open(authority.clone(), setup_temp.path())
        .await
        .unwrap();
    let (node, _) = setup
        .create_file_with_node(authority.root(), "file", false)
        .await
        .unwrap();
    drop(setup);

    let pausing = Arc::new(PausingObserveAuthority::new(authority));
    let mount_temp = tempfile::tempdir().unwrap();
    let mount = Mount::open(pausing.clone(), mount_temp.path(), true)
        .await
        .unwrap();
    (pausing, mount, node, setup_temp, mount_temp)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failed_first_range_open_broadcasts_retry_to_all_waiters() {
    let (pausing, mount, node, _setup_temp, _mount_temp) = pausing_read_only_mount().await;
    pausing.pause_next_observe();
    pausing.fail_next_observe();
    let mut owner = Box::pin(mount.open_read_only_node(node));
    assert_pending(&mut owner);
    pausing.entered.acquire().await.unwrap().forget();
    let mut first = Box::pin(mount.open_read_only_node(node));
    let mut second = Box::pin(mount.open_read_only_node(node));
    assert_pending(&mut first);
    assert_pending(&mut second);
    pausing.release.add_permits(1);

    let error = tokio::time::timeout(Duration::from_secs(5), &mut owner)
        .await
        .expect("failed range owner did not finish")
        .expect_err("failed range owner returned a file");
    assert_eq!(error.kind(), ErrorKind::Storage);
    let first = tokio::time::timeout(Duration::from_secs(5), &mut first)
        .await
        .expect("first range waiter did not retry")
        .expect("first range waiter failed");
    let second = tokio::time::timeout(Duration::from_secs(5), &mut second)
        .await
        .expect("second range waiter did not retry")
        .expect("second range waiter failed");
    assert_eq!(
        first.version().revision(),
        second.version().revision(),
        "retried range opens did not share one observation"
    );
    assert!(mount.status().await.unwrap().handles.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelled_first_range_open_broadcasts_retry_to_all_waiters() {
    let (pausing, mount, node, _setup_temp, _mount_temp) = pausing_read_only_mount().await;
    pausing.pause_next_observe();
    let mut owner = Box::pin(mount.open_read_only_node(node));
    assert_pending(&mut owner);
    pausing.entered.acquire().await.unwrap().forget();
    let mut first = Box::pin(mount.open_read_only_node(node));
    let mut second = Box::pin(mount.open_read_only_node(node));
    assert_pending(&mut first);
    assert_pending(&mut second);
    drop(owner);

    let first = tokio::time::timeout(Duration::from_secs(5), &mut first)
        .await
        .expect("first range waiter did not retry")
        .expect("first range waiter failed");
    let second = tokio::time::timeout(Duration::from_secs(5), &mut second)
        .await
        .expect("second range waiter did not retry")
        .expect("second range waiter failed");
    assert_eq!(first.version().revision(), second.version().revision());
    assert!(mount.status().await.unwrap().handles.is_empty());
}
