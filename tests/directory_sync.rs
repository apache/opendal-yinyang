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

//! Deterministic Managed Sync engine regression and scale tests.

#![cfg(target_os = "linux")]

#[path = "../crates/core/tests/support/mod.rs"]
mod support;

use std::collections::BTreeMap;
use std::future::Future;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use support::TestBackend;
use yinyang::core::service::{MetadataService, ServiceClient, serve};
use yinyang::core::{
    Authority, CommitId, CommitOutcome, Fs, NodeId, Planner, Revision, Snapshot, Transaction,
};
use yinyang::sync_engine::{
    ManagedAdapter, Resolution, RunOutcome, StatusDocument, SyncEngine, SyncModel,
};

#[derive(Clone, Debug, Eq, PartialEq)]
enum TreeEntry {
    Directory,
    File { executable: bool, bytes: Vec<u8> },
}

fn local_tree(root: &Path) -> BTreeMap<String, TreeEntry> {
    fn walk(path: &Path, rel: &str, tree: &mut BTreeMap<String, TreeEntry>) {
        let metadata = path.symlink_metadata().expect("tree metadata");
        if metadata.is_dir() {
            tree.insert(rel.to_owned(), TreeEntry::Directory);
            for entry in std::fs::read_dir(path).expect("tree directory") {
                let entry = entry.expect("tree entry");
                let name = entry.file_name().to_string_lossy().into_owned();
                let child = if rel.is_empty() {
                    name
                } else {
                    format!("{rel}/{name}")
                };
                walk(&entry.path(), &child, tree);
            }
        } else {
            tree.insert(
                rel.to_owned(),
                TreeEntry::File {
                    executable: metadata.permissions().mode() & 0o111 != 0,
                    bytes: std::fs::read(path).expect("tree file"),
                },
            );
        }
    }
    let mut tree = BTreeMap::new();
    walk(root, "", &mut tree);
    tree
}

fn tree_bytes(path: &Path) -> u64 {
    if !path.exists() {
        return 0;
    }
    let mut bytes = 0;
    let mut pending = vec![path.to_owned()];
    while let Some(directory) = pending.pop() {
        for entry in std::fs::read_dir(directory).expect("tree bytes directory") {
            let entry = entry.expect("tree bytes entry");
            let metadata = entry.metadata().expect("tree bytes metadata");
            if metadata.is_dir() {
                pending.push(entry.path());
            } else {
                bytes += metadata.len();
            }
        }
    }
    bytes
}

async fn remote_tree(authority: &Arc<dyn Authority>) -> BTreeMap<String, TreeEntry> {
    let snapshot = authority.observe_latest().await.unwrap();
    let mut tree = BTreeMap::new();
    let mut pending = vec![(String::new(), snapshot.root())];
    while let Some((rel, id)) = pending.pop() {
        let node = snapshot.node(id).await.unwrap().expect("remote node");
        if node.is_directory() {
            tree.insert(rel.clone(), TreeEntry::Directory);
            let mut token = None;
            loop {
                let page = snapshot.scan(id, token.as_ref(), 4096).await.unwrap();
                for entry in page.entries {
                    let child = if rel.is_empty() {
                        entry.name.clone()
                    } else {
                        format!("{rel}/{}", entry.name)
                    };
                    pending.push((child, entry.node_id));
                }
                match page.next {
                    Some(next) => token = Some(next),
                    None => break,
                }
            }
        } else {
            let version = snapshot.open_file(id).await.unwrap();
            let length = version.descriptor().content_id().length();
            tree.insert(
                rel,
                TreeEntry::File {
                    executable: node.executable(),
                    bytes: version.read(0, length as usize).await.unwrap(),
                },
            );
        }
    }
    tree
}

struct ManagedHarness {
    engine: SyncEngine<ManagedAdapter>,
    adapter: ManagedAdapter,
    state: PathBuf,
}

impl ManagedHarness {
    async fn init(authority: Arc<dyn Authority>, state: &Path, root: &Path) -> Self {
        std::fs::create_dir_all(root).unwrap();
        let adapter = ManagedAdapter::new(authority);
        let engine = SyncEngine::init(adapter.clone(), state, root)
            .await
            .unwrap();
        Self {
            engine,
            adapter,
            state: state.to_owned(),
        }
    }

    async fn open(authority: Arc<dyn Authority>, state: &Path) -> Self {
        let adapter = ManagedAdapter::new(authority);
        let engine = SyncEngine::open(adapter.clone(), state).await.unwrap();
        Self {
            engine,
            adapter,
            state: state.to_owned(),
        }
    }

    async fn run(&mut self) -> RunOutcome {
        self.engine.run().await.unwrap()
    }

    async fn resolve(&mut self, id: &str, resolution: Resolution) -> RunOutcome {
        self.engine.resolve(id, resolution).await.unwrap()
    }

    async fn inspect(&self) -> StatusDocument {
        SyncEngine::<ManagedAdapter>::inspect(
            Some(&self.adapter),
            &self.state,
            SyncModel::Managed,
            None,
        )
        .await
    }

    fn hash_bytes_read(&self) -> u64 {
        self.engine.hash_bytes_read()
    }
}

#[tokio::test]
async fn managed_sync_publishes_two_hundred_file_creates_in_one_commit() {
    const FILES: usize = 200;
    let backend = TestBackend::default();
    let authority: Arc<dyn Authority> = Arc::new(Fs::create(backend.operator()).await.unwrap());
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("root");
    let state = temp.path().join("state");
    std::fs::create_dir_all(&root).unwrap();
    for index in 0..FILES {
        std::fs::write(
            root.join(format!("file-{index:03}")),
            [u8::try_from(index).unwrap()],
        )
        .unwrap();
    }
    let mut sync = ManagedHarness::init(authority, &state, &root).await;
    let before = backend.state.lock().unwrap().head_conditional_writes;
    assert!(sync.run().await.in_sync);
    let commits = backend.state.lock().unwrap().head_conditional_writes - before;
    assert_eq!(
        commits, 1,
        "{FILES} file creates used {commits} head commits"
    );
}

#[tokio::test]
async fn managed_sync_mixed_creates_and_content_edits_use_one_head_commit() {
    let backend = TestBackend::default();
    let authority: Arc<dyn Authority> = Arc::new(Fs::create(backend.operator()).await.unwrap());
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("root");
    let state = temp.path().join("state");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::write(root.join("edited"), b"baseline").unwrap();
    std::fs::write(root.join("other"), b"unchanged").unwrap();
    let mut sync = ManagedHarness::init(authority, &state, &root).await;
    assert!(sync.run().await.in_sync);

    std::fs::write(root.join("edited"), b"replacement").unwrap();
    std::fs::write(root.join("created"), b"new bytes").unwrap();
    let before = backend.state.lock().unwrap().head_conditional_writes;
    assert!(sync.run().await.in_sync);
    let commits = backend.state.lock().unwrap().head_conditional_writes - before;
    assert_eq!(
        commits, 1,
        "mixed file creates and edits used {commits} head commits"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn managed_sync_releases_artifacts_and_has_a_constant_noop_cost() {
    const SMALL: usize = 512 * 1024;
    const LARGE: usize = 1024 * 1024;
    const FILES: usize = 64;
    const EDITS: usize = 40;

    let backend = TestBackend::default();
    let authority: Arc<dyn Authority> = Arc::new(Fs::create(backend.operator()).await.unwrap());
    let temp = tempfile::tempdir().unwrap();
    let writer_root = temp.path().join("writer");
    let reader_root = temp.path().join("reader");
    let writer_state = temp.path().join("writer-state");
    let reader_state = temp.path().join("reader-state");
    std::fs::create_dir_all(&writer_root).unwrap();
    std::fs::create_dir_all(&reader_root).unwrap();
    for index in 0..FILES {
        std::fs::write(
            writer_root.join(format!("small-{index:03}")),
            vec![index as u8; SMALL],
        )
        .unwrap();
    }
    let large = writer_root.join("large");
    std::fs::write(&large, vec![1_u8; LARGE]).unwrap();

    let mut writer = ManagedHarness::init(authority.clone(), &writer_state, &writer_root).await;
    let before = backend.state.lock().unwrap().head_conditional_writes;
    assert!(writer.run().await.in_sync);
    let commits = backend.state.lock().unwrap().head_conditional_writes - before;
    assert!(
        commits <= 16,
        "one new-file pass used {commits} head commits"
    );
    assert_eq!(writer.inspect().await.summary.retained_bytes, 0);
    assert_eq!(tree_bytes(&writer_state.join("artifacts")), 0);

    let mut reader = ManagedHarness::init(authority.clone(), &reader_state, &reader_root).await;
    assert!(reader.run().await.in_sync);
    assert_eq!(reader.inspect().await.summary.retained_bytes, 0);
    assert_eq!(tree_bytes(&reader_state.join("artifacts")), 0);

    for edit in 0..EDITS {
        let mut bytes = vec![(edit % 250 + 2) as u8; LARGE];
        bytes[..8].copy_from_slice(&(edit as u64).to_le_bytes());
        std::fs::write(&large, bytes).unwrap();
        assert!(writer.run().await.in_sync);
        assert_eq!(writer.inspect().await.summary.retained_bytes, 0);
        assert!(reader.run().await.in_sync);
        assert!(
            reader.inspect().await.summary.retained_bytes <= LARGE as u64,
            "reader retained more than one displaced version after edit {edit}"
        );
    }

    drop(reader);
    let mut reader = ManagedHarness::open(authority.clone(), &reader_state).await;
    assert!(reader.run().await.in_sync);
    assert_eq!(reader.inspect().await.summary.retained_bytes, 0);
    assert_eq!(tree_bytes(&reader_state.join("artifacts")), 0);
    tokio::time::sleep(Duration::from_millis(1100)).await;
    assert!(reader.run().await.in_sync);
    assert!(
        reader.hash_bytes_read() <= LARGE as u64,
        "the first post-window run may refresh one racy evidence record"
    );
    assert!(reader.run().await.in_sync);
    assert_eq!(
        reader.hash_bytes_read(),
        0,
        "a settled no-op must use cached stat identity"
    );

    tokio::time::sleep(Duration::from_millis(1100)).await;
    let mut changed = vec![7_u8; LARGE];
    changed[..8].copy_from_slice(&u64::MAX.to_le_bytes());
    std::fs::write(reader_root.join("large"), &changed).unwrap();
    assert!(reader.run().await.in_sync);
    assert!(
        reader.hash_bytes_read() >= LARGE as u64,
        "an in-place edit must invalidate persistent stat evidence"
    );
    assert!(writer.run().await.in_sync);
    assert_eq!(std::fs::read(&large).unwrap(), changed);
}

#[tokio::test]
async fn managed_sync_pull_hashing_is_linear() {
    const SIZE: usize = 256 * 1024;
    for files in [32_usize, 128] {
        let backend = TestBackend::default();
        let authority: Arc<dyn Authority> = Arc::new(Fs::create(backend.operator()).await.unwrap());
        let temp = tempfile::tempdir().unwrap();
        let push_root = temp.path().join("push");
        std::fs::create_dir_all(&push_root).unwrap();
        for index in 0..files {
            let mut bytes = vec![(index % 251) as u8; SIZE];
            bytes[..8].copy_from_slice(&(index as u64).to_le_bytes());
            std::fs::write(push_root.join(format!("file-{index:03}")), bytes).unwrap();
        }
        let mut push = ManagedHarness::init(
            authority.clone(),
            &temp.path().join("push-state"),
            &push_root,
        )
        .await;
        assert!(push.run().await.in_sync);

        let pull_root = temp.path().join("pull");
        std::fs::create_dir_all(&pull_root).unwrap();
        let mut pull = ManagedHarness::init(
            authority.clone(),
            &temp.path().join("pull-state"),
            &pull_root,
        )
        .await;
        let before = pull.hash_bytes_read();
        assert!(pull.run().await.in_sync);
        let tree = (files * SIZE) as u64;
        assert!(
            pull.hash_bytes_read() - before <= tree * 2,
            "{files}-file pull rehashed more than twice the tree"
        );
    }
}

#[tokio::test]
#[ignore = "release-mode scale benchmark; run explicitly with --ignored --nocapture"]
async fn managed_sync_scale_benchmark() {
    const SIZE: usize = 512 * 1024;
    for files in [32_usize, 64, 128, 256] {
        let backend = TestBackend::default();
        let authority: Arc<dyn Authority> = Arc::new(Fs::create(backend.operator()).await.unwrap());
        let temp = tempfile::tempdir().unwrap();
        let push_root = temp.path().join("push");
        for index in 0..files {
            let directory = push_root.join(format!("d{}", index % 4));
            std::fs::create_dir_all(&directory).unwrap();
            let mut bytes = vec![(index % 251) as u8; SIZE];
            bytes[..8].copy_from_slice(&(index as u64).to_le_bytes());
            std::fs::write(directory.join(format!("f{index}")), bytes).unwrap();
        }
        let mut push = ManagedHarness::init(
            authority.clone(),
            &temp.path().join("push-state"),
            &push_root,
        )
        .await;
        let before = Instant::now();
        assert!(push.run().await.in_sync);
        let push_seconds = before.elapsed().as_secs_f64();

        let pull_root = temp.path().join("pull");
        std::fs::create_dir_all(&pull_root).unwrap();
        let mut pull = ManagedHarness::init(
            authority.clone(),
            &temp.path().join("pull-state"),
            &pull_root,
        )
        .await;
        let before = Instant::now();
        assert!(pull.run().await.in_sync);
        let pull_seconds = before.elapsed().as_secs_f64();
        let pull_hashes = pull.hash_bytes_read();
        tokio::time::sleep(Duration::from_millis(1100)).await;
        assert!(pull.run().await.in_sync);
        let before = Instant::now();
        assert!(pull.run().await.in_sync);
        let noop_seconds = before.elapsed().as_secs_f64();
        let noop_hashes = pull.hash_bytes_read();
        assert_eq!(noop_hashes, 0);
        println!(
            "W2_SCALE files={files} size={SIZE} push_s={push_seconds:.3} pull_s={pull_seconds:.3} noop_s={noop_seconds:.3} pull_hashed_bytes={pull_hashes} noop_hashed_bytes={noop_hashes} tree_bytes={}",
            files * SIZE
        );
    }
}

#[tokio::test]
#[ignore = "release-mode edit benchmark; run explicitly with --ignored --nocapture"]
async fn managed_sync_edit_benchmark() {
    const SIZE: usize = 1024 * 1024;
    const EDITS: usize = 40;
    let backend = TestBackend::default();
    let authority: Arc<dyn Authority> = Arc::new(Fs::create(backend.operator()).await.unwrap());
    let temp = tempfile::tempdir().unwrap();
    let (writer_root, reader_root) = (temp.path().join("writer"), temp.path().join("reader"));
    let (writer_state, reader_state) = (temp.path().join("ws"), temp.path().join("rs"));
    std::fs::create_dir_all(&writer_root).unwrap();
    std::fs::create_dir_all(&reader_root).unwrap();
    let file = writer_root.join("file");
    std::fs::write(&file, vec![1_u8; SIZE]).unwrap();
    let mut writer = ManagedHarness::init(authority.clone(), &writer_state, &writer_root).await;
    assert!(writer.run().await.in_sync);
    let mut reader = ManagedHarness::init(authority.clone(), &reader_state, &reader_root).await;
    assert!(reader.run().await.in_sync);
    for edit in 0..EDITS {
        let mut bytes = vec![(edit % 250 + 2) as u8; SIZE];
        bytes[..8].copy_from_slice(&(edit as u64).to_le_bytes());
        std::fs::write(&file, bytes).unwrap();
        assert!(writer.run().await.in_sync);
        assert!(reader.run().await.in_sync);
    }
    let writer_retained = writer.inspect().await.summary.retained_bytes;
    let reader_retained = reader.inspect().await.summary.retained_bytes;
    tokio::time::sleep(Duration::from_millis(1100)).await;
    assert!(reader.run().await.in_sync);
    let started = Instant::now();
    assert!(reader.run().await.in_sync);
    println!(
        "W2_EDITS edits={EDITS} writer_retained_bytes={writer_retained} reader_retained_bytes={reader_retained} settled_retained_bytes={} noop_hashed_bytes={} noop_s={:.3} artifact_bytes={}",
        reader.inspect().await.summary.retained_bytes,
        reader.hash_bytes_read(),
        started.elapsed().as_secs_f64(),
        tree_bytes(&reader_state.join("artifacts"))
    );
}

#[derive(Debug)]
struct HeldAuthority {
    inner: Arc<dyn Authority>,
    committed: tokio::sync::mpsc::UnboundedSender<()>,
    release: Arc<tokio::sync::Notify>,
    hold_next: AtomicBool,
    hold_before_next: AtomicBool,
}

impl HeldAuthority {
    fn new(
        inner: Arc<dyn Authority>,
        committed: tokio::sync::mpsc::UnboundedSender<()>,
        release: Arc<tokio::sync::Notify>,
        before: bool,
    ) -> Self {
        Self {
            inner,
            committed,
            release,
            hold_next: AtomicBool::new(!before),
            hold_before_next: AtomicBool::new(before),
        }
    }
}

impl Authority for HeldAuthority {
    fn data(&self) -> &yinyang::core::DataStore {
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
            if self.hold_before_next.swap(false, Ordering::SeqCst) {
                self.committed.send(()).ok();
                self.release.notified().await;
            }
            let outcomes = self.inner.commit_batch(requests).await?;
            if self.hold_next.swap(false, Ordering::SeqCst) {
                self.committed.send(()).ok();
                self.release.notified().await;
            }
            Ok(outcomes)
        })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn local_edit_during_delayed_publication_receipt_stays_pending() {
    let backend = TestBackend::default();
    let authority: Arc<dyn Authority> = Arc::new(Fs::create(backend.operator()).await.unwrap());
    let (committed_tx, mut committed_rx) = tokio::sync::mpsc::unbounded_channel();
    let release = Arc::new(tokio::sync::Notify::new());
    let held = Arc::new(HeldAuthority::new(
        authority.clone(),
        committed_tx,
        release.clone(),
        false,
    ));
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("payload");
    let state = temp.path().join("state");
    std::fs::create_dir_all(&root).unwrap();
    let file = root.join("file");
    std::fs::write(&file, b"version one").unwrap();
    let mut sync = ManagedHarness::init(held.clone(), &state, &root).await;
    let run = sync.run();
    let writer = async {
        committed_rx.recv().await.expect("v1 commit");
        std::fs::write(&file, b"version two").unwrap();
        release.notify_one();
    };
    let outcome =
        tokio::time::timeout(Duration::from_secs(60), async { tokio::join!(run, writer) })
            .await
            .unwrap()
            .0;
    assert_eq!(outcome.published_local, 1);
    assert_eq!(outcome.conflicts, 0);
    assert!(!outcome.in_sync);
    assert!(outcome.pending > 0);
    assert_eq!(
        remote_tree(&authority).await["file"],
        TreeEntry::File {
            executable: false,
            bytes: b"version one".to_vec(),
        }
    );
    drop(sync);

    let mut reopened = ManagedHarness::open(held.clone(), &state).await;
    let second = reopened.run().await;
    assert!(second.published_local >= 1);
    assert_eq!(second.conflicts, 0);
    assert!(second.in_sync);
    assert_eq!(
        remote_tree(&authority).await["file"],
        TreeEntry::File {
            executable: false,
            bytes: b"version two".to_vec(),
        }
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn interleaved_remote_edit_is_not_overwritten_by_local_run() {
    let backend = TestBackend::default();
    let authority: Arc<dyn Authority> = Arc::new(Fs::create(backend.operator()).await.unwrap());
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("payload");
    let state = temp.path().join("state");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::write(root.join("a"), b"local-a").unwrap();
    std::fs::write(root.join("b"), b"local-b").unwrap();
    let mut sync = ManagedHarness::init(authority.clone(), &state, &root).await;
    let first = sync.run().await;
    assert_eq!(first.published_local, 2);
    assert_eq!(first.conflicts, 0);
    drop(sync);

    std::fs::write(root.join("a"), b"local-a2").unwrap();
    std::fs::write(root.join("b"), b"local-b2").unwrap();
    let (committed_tx, mut committed_rx) = tokio::sync::mpsc::unbounded_channel();
    let release = Arc::new(tokio::sync::Notify::new());
    let held = Arc::new(HeldAuthority::new(
        authority.clone(),
        committed_tx,
        release.clone(),
        true,
    ));
    let mut sync = ManagedHarness::open(held.clone(), &state).await;
    let run = sync.run();
    let writer = async {
        committed_rx.recv().await.expect("pre-commit barrier");
        let baseline = authority.observe_latest().await.unwrap();
        let b = baseline.resolve("b").await.unwrap().unwrap().id();
        let prepared = authority
            .data()
            .prepare(&mut &b"external-b"[..])
            .await
            .unwrap();
        let mut planner = Planner::new(&baseline, CommitId::generate());
        let _ = planner.node(b).await.unwrap();
        planner.set_content(b, prepared).await.unwrap();
        assert!(matches!(
            authority.commit(&planner.finish().unwrap()).await.unwrap(),
            CommitOutcome::Committed(_)
        ));
        release.notify_one();
    };
    let result = tokio::time::timeout(Duration::from_secs(60), async { tokio::join!(run, writer) })
        .await
        .unwrap()
        .0;
    drop(sync);
    assert!(!result.in_sync);

    let local = local_tree(&root);
    let remote = remote_tree(&authority).await;
    assert_eq!(
        remote["b"],
        TreeEntry::File {
            executable: false,
            bytes: b"external-b".to_vec(),
        },
        "external edit must survive"
    );
    assert_eq!(
        local["b"],
        TreeEntry::File {
            executable: false,
            bytes: b"local-b2".to_vec(),
        },
        "local edit must survive"
    );
    let inspected = ManagedHarness::open(authority.clone(), &state).await;
    let status = inspected.inspect().await;
    assert!(!status.conflicts.is_empty());
    let conflict = status.conflicts[0].id.clone();
    let mut resolver = inspected;
    resolver.resolve(&conflict, Resolution::KeepRemote).await;
    let stable = resolver.run().await;
    assert!(stable.in_sync, "{stable:?}");
    assert!(resolver.inspect().await.conflicts.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn managed_sync_publication_does_not_reimport_prepared_content() {
    const SIZE: usize = 4 * 1024 * 1024;

    async fn measure(authority: Arc<dyn Authority>, backend: &TestBackend) -> u64 {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("root");
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(root.join("file"), vec![9_u8; SIZE]).unwrap();
        let adapter = ManagedAdapter::new(authority);
        let state = temp.path().join("state");
        let mut sync = SyncEngine::init(adapter, &state, &root).await.unwrap();
        let reads = || backend.state.lock().unwrap().data_read_bytes;
        let before = reads();
        assert!(sync.run().await.unwrap().in_sync);
        reads() - before
    }

    let object_backend = TestBackend::default();
    let object: Arc<dyn Authority> = Arc::new(Fs::create(object_backend.operator()).await.unwrap());
    let reads = measure(object, &object_backend).await;
    assert!(
        reads <= SIZE as u64 + 64 * 1024,
        "object run read {reads} bytes for a {SIZE}-byte file"
    );

    let service_backend = TestBackend::default();
    let service_temp = tempfile::tempdir().unwrap();
    let service = MetadataService::create(
        service_temp.path().join("metadata.db"),
        service_backend.operator(),
    )
    .await
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let token = "managed-sync-publication-read-test".to_owned();
    let server = tokio::spawn(serve(listener, service, token.clone()));
    let client: Arc<dyn Authority> = Arc::new(
        ServiceClient::connect(address, token, service_backend.operator())
            .await
            .unwrap(),
    );
    let reads = measure(client, &service_backend).await;
    assert!(
        reads <= SIZE as u64 + 128 * 1024,
        "service run read {reads} bytes for a {SIZE}-byte file"
    );
    server.abort();
}
