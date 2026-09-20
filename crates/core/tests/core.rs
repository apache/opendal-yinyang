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

mod support;
use std::sync::Arc;
use support::*;
use yinyang_core::{CommitId, ErrorKind, NodeId};

use yinyang_core::namespace::NodeKind as IndexedKind;
use yinyang_core::object::{BackendProfile, ObjectFs, Outcome};
use yinyang_core::transaction::Planner;

#[tokio::test]
async fn indexed_disjoint_files_and_fenced_delayed_attempts() {
    let backend = TestBackend::default();
    let fs = object_fs(&backend).await;
    let mut seed = Planner::new(&fs.observe_latest().await.unwrap(), CommitId::generate());
    let empty = fs.data().prepare(&mut b"".as_slice()).await.unwrap();
    let a = seed
        .create_file(fs.root(), "a", empty.clone(), false)
        .await
        .unwrap();
    let b = seed
        .create_file(fs.root(), "b", empty, false)
        .await
        .unwrap();
    let parent = seed.create_directory(fs.root(), "parent").await.unwrap();
    committed(fs.commit(&seed.finish().unwrap()).await.unwrap());
    let old = fs.observe_latest().await.unwrap();
    let mut left = Planner::new(&old, CommitId::generate());
    left.set_executable(a, true).await.unwrap();
    let mut right = Planner::new(&old, CommitId::generate());
    right.set_executable(b, true).await.unwrap();
    let left = left.finish().unwrap();
    let right = right.finish().unwrap();
    backend.state.lock().unwrap().head_barrier = Some((Arc::new(tokio::sync::Barrier::new(2)), 2));
    let (left, right) = tokio::join!(fs.commit(&left), fs.commit(&right));
    committed(left.unwrap());
    committed(right.unwrap());
    let old = fs.observe_latest().await.unwrap();
    let mut remove = Planner::new(&old, CommitId::generate());
    remove.remove(parent).await.unwrap();
    let remove = remove.finish().unwrap();
    backend.state.lock().unwrap().delay_next_head = true;
    assert_eq!(
        fs.commit(&remove).await.unwrap(),
        Outcome::Unknown(remove.id())
    );
    let mut create = Planner::new(&old, CommitId::generate());
    create.create_directory(parent, "child").await.unwrap();
    committed(fs.commit(&create.finish().unwrap()).await.unwrap());
    assert_eq!(fs.commit(&remove).await.unwrap(), Outcome::Conflict);
    let mut state = backend.state.lock().unwrap();
    let (_, pending_condition) = state.pending_head.take().unwrap();
    assert_ne!(
        pending_condition,
        Some(state.objects[".yinyang/head"].etag.clone())
    );
}

#[tokio::test]
async fn indexed_creation_recovers_lost_ack_and_digest_ignores_packing() {
    let backend = TestBackend::default();
    backend.fail_next_head_write_after_success();
    let fs = object_fs(&backend).await;
    let snapshot = fs.observe_latest().await.unwrap();
    let id = CommitId::generate();
    let mut left = Planner::new(&snapshot, id);
    left.create_file(
        fs.root(),
        "file",
        fs.data().prepare(&mut b"same".as_slice()).await.unwrap(),
        false,
    )
    .await
    .unwrap();
    let mut right = Planner::new(&snapshot, id);
    right
        .create_file(
            fs.root(),
            "file",
            fs.data().prepare(&mut b"same".as_slice()).await.unwrap(),
            false,
        )
        .await
        .unwrap();
    let left = left.finish().unwrap();
    let right = right.finish().unwrap();
    assert_eq!(left.digest(), right.digest());
    let first = committed(fs.commit(&left).await.unwrap());
    assert_eq!(committed(fs.commit(&right).await.unwrap()), first);
}

#[tokio::test]
async fn indexed_costs_follow_changed_paths_not_namespace_or_history_size() {
    async fn measure(nodes: usize, history: usize) -> (u64, u64, usize) {
        let backend = TestBackend::default();
        let fs = object_fs(&backend).await;
        let mut plan = Planner::new(&fs.observe_latest().await.unwrap(), CommitId::generate());
        let anchor = plan.create_directory(fs.root(), "anchor").await.unwrap();
        for i in 0..nodes {
            plan.create_directory(anchor, &format!("child-{i:04}"))
                .await
                .unwrap();
        }
        committed(fs.commit(&plan.finish().unwrap()).await.unwrap());
        for _ in 0..history {
            let request = Planner::new(&fs.observe_latest().await.unwrap(), CommitId::generate())
                .finish()
                .unwrap();
            committed(fs.commit(&request).await.unwrap());
        }
        let mut plan = Planner::new(&fs.observe_latest().await.unwrap(), CommitId::generate());
        plan.rename(anchor, fs.root(), "renamed").await.unwrap();
        let request = plan.finish().unwrap();
        {
            let mut state = backend.state.lock().unwrap();
            state.written_bytes = 0;
            state.data_read_bytes = 0;
            state.read_paths.clear();
        }
        committed(fs.commit(&request).await.unwrap());
        let state = backend.state.lock().unwrap();
        (
            state.written_bytes,
            state.data_read_bytes,
            state.read_paths.len(),
        )
    }
    let small = measure(32, 16).await;
    let large = measure(1024, 128).await;
    println!(
        "fixed directory rename: small={small:?}, large={large:?} (written bytes, read bytes, reads)"
    );
    assert!(large.0 < small.0 * 4);
    assert!(large.1 < small.1 * 4);
    assert!(large.2 < small.2 * 3);
}

#[tokio::test]
async fn indexed_deep_paths_names_and_invalid_profiles() {
    use yinyang_core::namespace::name_key;
    for name in [
        "",
        "..",
        "a/b",
        "a\\b",
        "NUL.txt",
        "LPT².log",
        "bad.",
        "e\u{0301}",
    ] {
        assert!(name_key(name).is_err(), "{name:?}");
    }
    assert_eq!(name_key("Straße").unwrap(), name_key("STRASSE").unwrap());
    let backend = TestBackend::default();
    let fs = object_fs(&backend).await;
    let mut plan = Planner::new(&fs.observe_latest().await.unwrap(), CommitId::generate());
    let mut parent = fs.root();
    let mut path = Vec::new();
    for i in 0..24 {
        let name = format!("{i:02}{}", "a".repeat(200));
        parent = plan.create_directory(parent, &name).await.unwrap();
        path.push(name);
    }
    committed(fs.commit(&plan.finish().unwrap()).await.unwrap());
    assert!(path.join("/").len() > 4096);
    assert_eq!(
        fs.observe_latest()
            .await
            .unwrap()
            .resolve(&path.join("/"))
            .await
            .unwrap()
            .unwrap()
            .id(),
        parent
    );
    assert_eq!(
        ObjectFs::open(
            opendal::Operator::new(opendal::services::Memory::default()).unwrap(),
            BackendProfile::Minio
        )
        .await
        .unwrap_err()
        .kind(),
        ErrorKind::Unsupported
    );
    let legacy = TestBackend::default();
    legacy.state.lock().unwrap().objects.insert(
        ".yinyang/head".into(),
        StoredObject {
            bytes: b"YYHEAD01legacy".to_vec(),
            etag: "legacy".into(),
        },
    );
    assert_eq!(
        ObjectFs::create(legacy.operator(), BackendProfile::Minio)
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Unsupported
    );
    assert_eq!(legacy.state.lock().unwrap().objects.len(), 1);
}

#[tokio::test]
#[ignore = "requires an isolated S3 bucket configured with YINYANG_S3_* variables"]
async fn s3_indexed_publication_and_reopen() {
    opendal::install_default();
    let config = std::env::vars()
        .filter_map(|(key, value)| {
            key.strip_prefix("YINYANG_S3_")
                .filter(|k| *k != "PROFILE")
                .map(|key| (key.to_ascii_lowercase(), value))
        })
        .collect::<Vec<_>>();
    let operator = opendal::Operator::via_iter("s3", config).unwrap();
    let fs = ObjectFs::create(operator.clone(), BackendProfile::Minio)
        .await
        .unwrap();
    let old = fs.observe_latest().await.unwrap();
    let bytes = vec![37; 12 * 1024 * 1024 + 17];
    let prepared = fs.data().prepare(&mut bytes.as_slice()).await.unwrap();
    let changed = fs
        .data()
        .overwrite_prepared(&prepared, 65534..65538, b"test")
        .await
        .unwrap();
    let mut plan = Planner::new(&old, CommitId::generate());
    let name = format!("file-{}", uuid::Uuid::new_v4().simple());
    let id = plan
        .create_file(fs.root(), &name, changed, false)
        .await
        .unwrap();
    let request = plan.finish().unwrap();
    let receipt = committed(fs.commit(&request).await.unwrap());
    let reopened = ObjectFs::open(operator, BackendProfile::Minio)
        .await
        .unwrap();
    assert_eq!(committed(reopened.commit(&request).await.unwrap()), receipt);
    let file = reopened
        .observe_latest()
        .await
        .unwrap()
        .content(id)
        .await
        .unwrap();
    let mut output = Vec::new();
    reopened
        .data()
        .read_range(file.descriptor(), 65533..65539, &mut output)
        .await
        .unwrap();
    assert_eq!(output, b"%test%");
    assert!(
        reopened
            .observe_revision(old.revision())
            .await
            .unwrap()
            .node(id)
            .await
            .unwrap()
            .is_none()
    );
}

async fn object_fs(backend: &TestBackend) -> ObjectFs {
    ObjectFs::create(backend.operator(), BackendProfile::Minio)
        .await
        .unwrap()
}
fn committed(outcome: Outcome) -> yinyang_core::object::Receipt {
    match outcome {
        Outcome::Committed(receipt) => receipt,
        other => panic!("expected committed, got {other:?}"),
    }
}

#[tokio::test]
async fn indexed_disjoint_creates_survive_cas_race_and_preserve_snapshots() {
    let backend = TestBackend::default();
    let fs = object_fs(&backend).await;
    let old = fs.observe_latest().await.unwrap();
    let mut left = Planner::new(&old, CommitId::generate());
    let a = left.create_directory(fs.root(), "a").await.unwrap();
    let left = left.finish().unwrap();
    let mut right = Planner::new(&old, CommitId::generate());
    let b = right.create_directory(fs.root(), "b").await.unwrap();
    let right = right.finish().unwrap();
    backend.state.lock().unwrap().head_barrier = Some((Arc::new(tokio::sync::Barrier::new(2)), 2));
    let (l, r) = tokio::join!(fs.commit(&left), fs.commit(&right));
    let l = committed(l.unwrap());
    let r = committed(r.unwrap());
    assert_ne!(l.cursor, r.cursor);
    let latest = fs.observe_latest().await.unwrap();
    assert_eq!(
        latest
            .lookup(fs.root(), "a")
            .await
            .unwrap()
            .unwrap()
            .node_id,
        a
    );
    assert_eq!(
        latest
            .lookup(fs.root(), "b")
            .await
            .unwrap()
            .unwrap()
            .node_id,
        b
    );
    assert!(old.lookup(fs.root(), "a").await.unwrap().is_none());
    assert_eq!(
        fs.observe_revision(old.revision())
            .await
            .unwrap()
            .scan(fs.root(), None, 10)
            .await
            .unwrap()
            .entries
            .len(),
        0
    );
    assert_eq!(committed(fs.commit(&left).await.unwrap()), l);
    let mut different = Planner::new(&old, left.id());
    different
        .create_directory(fs.root(), "different")
        .await
        .unwrap();
    assert_eq!(
        fs.commit(&different.finish().unwrap())
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Invalid
    );
    assert_eq!(backend.state.lock().unwrap().head_stat_calls, 0);
}

#[tokio::test]
async fn indexed_conflicts_protect_content_emptiness_ancestry_and_scans() {
    let backend = TestBackend::default();
    let fs = object_fs(&backend).await;
    let old = fs.observe_latest().await.unwrap();
    let mut seed = Planner::new(&old, CommitId::generate());
    let a = seed.create_directory(fs.root(), "a").await.unwrap();
    let b = seed.create_directory(fs.root(), "b").await.unwrap();
    let bytes = fs.data().prepare(&mut b"first".as_slice()).await.unwrap();
    let file = seed
        .create_file(fs.root(), "file", bytes, false)
        .await
        .unwrap();
    committed(fs.commit(&seed.finish().unwrap()).await.unwrap());
    let old = fs.observe_latest().await.unwrap();
    let mut remove = Planner::new(&old, CommitId::generate());
    remove.remove(a).await.unwrap();
    let remove = remove.finish().unwrap();
    let mut insert = Planner::new(&old, CommitId::generate());
    insert.create_directory(a, "child").await.unwrap();
    committed(fs.commit(&insert.finish().unwrap()).await.unwrap());
    assert_eq!(fs.commit(&remove).await.unwrap(), Outcome::Conflict);
    let old = fs.observe_latest().await.unwrap();
    let mut move_a = Planner::new(&old, CommitId::generate());
    move_a.rename(a, b, "a").await.unwrap();
    let mut move_b = Planner::new(&old, CommitId::generate());
    move_b.rename(b, a, "b").await.unwrap();
    committed(fs.commit(&move_a.finish().unwrap()).await.unwrap());
    assert_eq!(
        fs.commit(&move_b.finish().unwrap()).await.unwrap(),
        Outcome::Conflict
    );
    let old = fs.observe_latest().await.unwrap();
    let mut l = Planner::new(&old, CommitId::generate());
    l.set_content(
        file,
        fs.data().prepare(&mut b"left".as_slice()).await.unwrap(),
    )
    .await
    .unwrap();
    let mut r = Planner::new(&old, CommitId::generate());
    r.set_content(
        file,
        fs.data().prepare(&mut b"right".as_slice()).await.unwrap(),
    )
    .await
    .unwrap();
    committed(fs.commit(&l.finish().unwrap()).await.unwrap());
    assert_eq!(
        fs.commit(&r.finish().unwrap()).await.unwrap(),
        Outcome::Conflict
    );
    let old = fs.observe_latest().await.unwrap();
    let mut scan = Planner::new(&old, CommitId::generate());
    scan.scan(fs.root()).await.unwrap();
    let mut create = Planner::new(&old, CommitId::generate());
    create.create_directory(fs.root(), "phantom").await.unwrap();
    committed(fs.commit(&create.finish().unwrap()).await.unwrap());
    assert_eq!(
        fs.commit(&scan.finish().unwrap()).await.unwrap(),
        Outcome::Conflict
    );
}

#[tokio::test]
async fn indexed_batch_receipts_generations_and_physical_repacking() {
    let backend = TestBackend::default();
    let fs = object_fs(&backend).await;
    let old = fs.observe_latest().await.unwrap();
    let mut seed = Planner::new(&old, CommitId::generate());
    let file = seed
        .create_file(
            fs.root(),
            "file",
            fs.data().prepare(&mut b"hello".as_slice()).await.unwrap(),
            false,
        )
        .await
        .unwrap();
    committed(fs.commit(&seed.finish().unwrap()).await.unwrap());
    let old = fs.observe_latest().await.unwrap();
    let mut repack = Planner::new(&old, CommitId::generate());
    repack
        .set_content(
            file,
            fs.data().prepare(&mut b"hello".as_slice()).await.unwrap(),
        )
        .await
        .unwrap();
    let mut edit = Planner::new(&old, CommitId::generate());
    edit.set_content(
        file,
        fs.data().prepare(&mut b"world".as_slice()).await.unwrap(),
    )
    .await
    .unwrap();
    let mut disjoint = Planner::new(&old, CommitId::generate());
    disjoint.create_directory(fs.root(), "dir").await.unwrap();
    let requests = vec![
        repack.finish().unwrap(),
        edit.finish().unwrap(),
        disjoint.finish().unwrap(),
    ];
    let results = fs.commit_batch(&requests).await.unwrap();
    let receipts = results.into_iter().map(committed).collect::<Vec<_>>();
    assert!(
        receipts
            .windows(2)
            .all(|p| p[0].cursor.revision == p[1].cursor.revision
                && p[0].cursor.ordinal + 1 == p[1].cursor.ordinal)
    );
    let latest = fs.observe_latest().await.unwrap();
    assert_eq!(latest.node(file).await.unwrap().unwrap().generation(), 2);
    assert_eq!(old.node(file).await.unwrap().unwrap().generation(), 1);
    let mut noop = Planner::new(&latest, CommitId::generate());
    noop.set_executable(file, true).await.unwrap();
    noop.set_executable(file, false).await.unwrap();
    committed(fs.commit(&noop.finish().unwrap()).await.unwrap());
    assert_eq!(
        fs.observe_latest()
            .await
            .unwrap()
            .node(file)
            .await
            .unwrap()
            .unwrap()
            .generation(),
        2
    );
    let changes = latest.changes(None, 20).await.unwrap();
    assert_eq!(changes.len(), 4);
    assert_eq!(changes[2].receipt, receipts[1]);
}

#[tokio::test]
async fn indexed_prepared_commit_does_not_read_data_and_reopen_retains_history() {
    let backend = TestBackend::default();
    let fs = object_fs(&backend).await;
    let before = backend
        .state
        .lock()
        .unwrap()
        .objects
        .keys()
        .cloned()
        .collect::<std::collections::BTreeSet<_>>();
    let prepared = fs
        .data()
        .prepare(&mut vec![11; 2 * 1024 * 1024].as_slice())
        .await
        .unwrap();
    let data_keys = backend
        .state
        .lock()
        .unwrap()
        .objects
        .keys()
        .filter(|k| !before.contains(*k))
        .cloned()
        .collect::<std::collections::BTreeSet<_>>();
    let old = fs.observe_latest().await.unwrap();
    let mut plan = Planner::new(&old, CommitId::generate());
    let id = plan
        .create_file(fs.root(), "file", prepared, false)
        .await
        .unwrap();
    backend.state.lock().unwrap().read_paths.clear();
    let request = plan.finish().unwrap();
    let receipt = committed(fs.commit(&request).await.unwrap());
    assert!(
        !backend
            .state
            .lock()
            .unwrap()
            .read_paths
            .iter()
            .any(|k| data_keys.contains(k))
    );
    let reopened = ObjectFs::open(backend.operator(), BackendProfile::Minio)
        .await
        .unwrap();
    assert_eq!(committed(reopened.commit(&request).await.unwrap()), receipt);
    let current = reopened.observe_latest().await.unwrap();
    assert!(matches!(
        current.node(id).await.unwrap().unwrap().kind(),
        IndexedKind::File(_)
    ));
    assert!(
        reopened
            .observe_revision(old.revision())
            .await
            .unwrap()
            .node(id)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn indexed_delayed_and_lost_publication_results_remain_resolvable() {
    let backend = TestBackend::default();
    let fs = object_fs(&backend).await;
    let old = fs.observe_latest().await.unwrap();
    let mut plan = Planner::new(&old, CommitId::generate());
    plan.create_directory(fs.root(), "delayed").await.unwrap();
    let request = plan.finish().unwrap();
    backend.state.lock().unwrap().delay_next_head = true;
    assert_eq!(
        fs.commit(&request).await.unwrap(),
        Outcome::Unknown(request.id())
    );
    assert!(
        fs.observe_latest()
            .await
            .unwrap()
            .receipt(request.id())
            .await
            .unwrap()
            .is_none()
    );
    {
        let mut state = backend.state.lock().unwrap();
        let (bytes, condition) = state.pending_head.take().unwrap();
        assert_eq!(condition, Some(state.objects[".yinyang/head"].etag.clone()));
        state.next_revision += 1;
        let etag = format!("delayed-{}", state.next_revision);
        state
            .objects
            .insert(".yinyang/head".into(), StoredObject { bytes, etag });
    }
    let receipt = committed(fs.commit(&request).await.unwrap());
    assert_eq!(committed(fs.commit(&request).await.unwrap()), receipt);
    let old = fs.observe_latest().await.unwrap();
    let mut plan = Planner::new(&old, CommitId::generate());
    plan.create_directory(fs.root(), "lost").await.unwrap();
    let request = plan.finish().unwrap();
    backend.fail_next_head_write_after_success();
    let receipt = committed(fs.commit(&request).await.unwrap());
    assert_eq!(committed(fs.commit(&request).await.unwrap()), receipt);
}

#[tokio::test]
async fn indexed_pagination_binds_snapshot_and_ordering() {
    let backend = TestBackend::default();
    let fs = object_fs(&backend).await;
    let old = fs.observe_latest().await.unwrap();
    let mut plan = Planner::new(&old, CommitId::generate());
    for i in 0..70 {
        plan.create_directory(fs.root(), &format!("entry-{i:03}"))
            .await
            .unwrap();
    }
    committed(fs.commit(&plan.finish().unwrap()).await.unwrap());
    let old = fs.observe_latest().await.unwrap();
    let page = old.scan(fs.root(), None, 17).await.unwrap();
    assert_eq!(page.entries.len(), 17);
    let token = page.next.unwrap();
    let mut plan = Planner::new(&old, CommitId::generate());
    plan.create_directory(fs.root(), "another").await.unwrap();
    committed(fs.commit(&plan.finish().unwrap()).await.unwrap());
    let current = fs.observe_latest().await.unwrap();
    assert_eq!(
        current
            .scan(fs.root(), Some(&token), 17)
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Invalid
    );
    assert_eq!(
        old.scan(fs.root(), Some(&token), 17).await.unwrap().entries[0].name,
        "entry-017"
    );
    let mut plan = Planner::new(&current, CommitId::generate());
    for entry in plan.scan(fs.root()).await.unwrap() {
        plan.remove(entry.node_id).await.unwrap();
    }
    committed(fs.commit(&plan.finish().unwrap()).await.unwrap());
    assert!(
        fs.observe_latest()
            .await
            .unwrap()
            .scan(fs.root(), None, 17)
            .await
            .unwrap()
            .entries
            .is_empty()
    );
    assert_eq!(
        old.scan(fs.root(), None, 100).await.unwrap().entries.len(),
        70
    );
}

#[tokio::test]
async fn content_preparation_requires_verified_storage_acknowledgement() {
    use yinyang_core::data::DataStore;
    let backend = TestBackend::default();
    let store = DataStore::new(backend.operator(), NodeId::generate()).unwrap();
    backend.state.lock().unwrap().fail_data_close = true;
    assert_eq!(
        store
            .prepare(&mut b"abc".as_slice())
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Storage
    );
    assert_eq!(backend.state.lock().unwrap().data_aborts, 1);
    backend.state.lock().unwrap().fail_data_close = false;
    backend.state.lock().unwrap().corrupt_pack_on_close = true;
    assert_eq!(
        store
            .prepare(&mut b"abc".as_slice())
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Corrupt
    );
}

#[tokio::test]
async fn canonical_content_range_reads_and_incremental_overwrites() {
    use yinyang_core::data::{BLOCK_BYTES, DataStore};
    let backend = TestBackend::default();
    let store = DataStore::new(backend.operator(), NodeId::generate()).unwrap();
    let bytes = (0..BLOCK_BYTES * 9 + 17)
        .map(|n| (n % 251) as u8)
        .collect::<Vec<_>>();
    let file = store.prepare(&mut bytes.as_slice()).await.unwrap();
    let second = store.prepare(&mut bytes.as_slice()).await.unwrap();
    assert_eq!(file.content_id(), second.content_id());
    assert_ne!(file.descriptor(), second.descriptor());
    backend.state.lock().unwrap().data_read_bytes = 0;
    let range = (BLOCK_BYTES as u64 - 3)..(BLOCK_BYTES as u64 + 4);
    let mut output = Vec::new();
    store
        .read_range(file.descriptor(), range.clone(), &mut output)
        .await
        .unwrap();
    assert_eq!(output, bytes[range.start as usize..range.end as usize]);
    assert!(backend.state.lock().unwrap().data_read_bytes < (BLOCK_BYTES * 2 + 10_000) as u64);
    backend.state.lock().unwrap().data_read_bytes = 0;
    let patched = store
        .overwrite_prepared(&file, range.clone(), b"changed")
        .await
        .unwrap();
    assert!(backend.state.lock().unwrap().data_read_bytes < (BLOCK_BYTES * 5) as u64);
    let mut expected = bytes.clone();
    expected[range.start as usize..range.end as usize].copy_from_slice(b"changed");
    let complete = store.prepare(&mut expected.as_slice()).await.unwrap();
    assert_eq!(patched.content_id(), complete.content_id());
    output.clear();
    store
        .read_range(patched.descriptor(), 0..expected.len() as u64, &mut output)
        .await
        .unwrap();
    assert_eq!(output, expected);
    output.clear();
    store
        .read_range(file.descriptor(), 0..bytes.len() as u64, &mut output)
        .await
        .unwrap();
    assert_eq!(output, bytes);
}

#[tokio::test]
async fn content_corruption_is_rejected_before_releasing_its_unit() {
    use yinyang_core::data::{BLOCK_BYTES, DataStore};
    let backend = TestBackend::default();
    let store = DataStore::new(backend.operator(), NodeId::generate()).unwrap();
    let bytes = vec![7; BLOCK_BYTES * 3];
    let file = store.prepare(&mut bytes.as_slice()).await.unwrap();
    let key = backend
        .state
        .lock()
        .unwrap()
        .objects
        .keys()
        .find(|key| key.contains("/packs/"))
        .unwrap()
        .clone();
    backend
        .state
        .lock()
        .unwrap()
        .objects
        .get_mut(&key)
        .unwrap()
        .bytes[0] ^= 1;
    let mut output = Vec::new();
    assert_eq!(
        store
            .read_range(file.descriptor(), 0..1, &mut output)
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Corrupt
    );
    assert!(output.is_empty());
    store
        .read_range(
            file.descriptor(),
            BLOCK_BYTES as u64..BLOCK_BYTES as u64 + 1,
            &mut output,
        )
        .await
        .unwrap();
    assert_eq!(output, vec![7]);
    assert_eq!(
        store.import(file.descriptor()).await.unwrap_err().kind(),
        ErrorKind::Corrupt
    );
}

#[tokio::test]
async fn preparation_evidence_cannot_be_recreated_from_a_descriptor() {
    use yinyang_core::data::{ContentDescriptor, DataStore};
    let backend = TestBackend::default();
    let id = NodeId::generate();
    let store = DataStore::new(backend.operator(), id).unwrap();
    let impostor = DataStore::new(backend.operator(), id).unwrap();
    let file = store.prepare(&mut &b"trusted"[..]).await.unwrap();
    assert_eq!(
        impostor.accept(&file).unwrap_err().kind(),
        ErrorKind::Invalid
    );
    let decoded = ContentDescriptor::from_bytes(&file.descriptor().to_bytes()).unwrap();
    let imported = impostor.import(&decoded).await.unwrap();
    assert_eq!(impostor.accept(&imported).unwrap(), decoded);
    let before = backend.state.lock().unwrap().data_reads;
    store.accept(&file).unwrap();
    assert_eq!(backend.state.lock().unwrap().data_reads, before);
    let another = DataStore::new(backend.operator(), NodeId::generate()).unwrap();
    assert_eq!(
        another.import(&decoded).await.unwrap_err().kind(),
        ErrorKind::Invalid
    );
}

#[tokio::test]
async fn content_profile_is_independent_of_input_chunking() {
    use yinyang_core::data::{BLOCK_BYTES, DataStore};
    struct ShortReads<'a>(&'a [u8]);
    impl tokio::io::AsyncRead for ShortReads<'_> {
        fn poll_read(
            mut self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
            output: &mut tokio::io::ReadBuf<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            let count = self.0.len().min(output.remaining()).min(17);
            output.put_slice(&self.0[..count]);
            self.0 = &self.0[count..];
            std::task::Poll::Ready(Ok(()))
        }
    }
    let store = DataStore::new(TestBackend::default().operator(), NodeId::generate()).unwrap();
    for length in [
        0,
        1,
        BLOCK_BYTES - 1,
        BLOCK_BYTES,
        BLOCK_BYTES + 1,
        5 * BLOCK_BYTES + 9,
    ] {
        let bytes = vec![42; length];
        let first = store.prepare(&mut bytes.as_slice()).await.unwrap();
        let second = store.prepare(&mut ShortReads(&bytes)).await.unwrap();
        assert_eq!(first.content_id(), second.content_id());
        let mut read = Vec::new();
        store
            .read_range(first.descriptor(), 0..length as u64, &mut read)
            .await
            .unwrap();
        assert_eq!(bytes, read);
    }
}
