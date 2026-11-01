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
use support::TestBackend;
use yinyang_core::object::{BackendProfile, ObjectFs};
use yinyang_core::service::{MetadataService, ServiceClient, serve};
use yinyang_core::{
    Authority, CommitId, CommitOutcome as Outcome, ContentEdit, ErrorKind, ExpiryConflictKind,
    ExpiryId, ExpiryOutcome, ExpiryRequest, Planner, Revision, RevisionState,
};

fn committed(outcome: Outcome) -> yinyang_core::Receipt {
    match outcome {
        Outcome::Committed(r) => r,
        other => panic!("expected committed: {other:?}"),
    }
}
async fn suite(fs: &dyn Authority) {
    let first = fs.observe_latest().await.unwrap();
    let file = fs.prepare(&mut b"original".as_slice()).await.unwrap();
    let mut create = Planner::new(&first, CommitId::generate());
    let a = create
        .create_file(fs.root(), "a", file.clone(), false)
        .await
        .unwrap();
    let b = create
        .create_file(fs.root(), "b", file.clone(), false)
        .await
        .unwrap();
    let p = create.create_directory(fs.root(), "p").await.unwrap();
    let q = create.create_directory(fs.root(), "q").await.unwrap();
    let transaction = create.finish().unwrap();
    let receipt = committed(fs.commit(&transaction).await.unwrap());
    assert_eq!(committed(fs.commit(&transaction).await.unwrap()), receipt);
    assert!(first.resolve("a").await.unwrap().is_none());
    let base = fs.observe_latest().await.unwrap();
    let serialized = transaction.to_bytes().unwrap();
    let restored = fs.restore_transaction(&serialized).await.unwrap();
    assert_eq!(committed(fs.commit(&restored).await.unwrap()), receipt);

    // Independent same-parent name slots and distinct file states do not conflict.
    let mut left = Planner::new(&base, CommitId::generate());
    left.create_directory(p, "left").await.unwrap();
    left.set_executable(a, true).await.unwrap();
    let mut right = Planner::new(&base, CommitId::generate());
    right.create_directory(p, "right").await.unwrap();
    right.set_executable(b, true).await.unwrap();
    let (left, right) = (left.finish().unwrap(), right.finish().unwrap());
    let (l, r) = tokio::join!(fs.commit(&left), fs.commit(&right));
    // The service's bounded writer-lock wait may expire on a slow runner.
    // Once both attempts finish, replay exactly the same request, never replan.
    for (result, request) in [(l, &left), (r, &right)] {
        let result = result.unwrap();
        committed(if result == Outcome::Retryable {
            fs.commit(request).await.unwrap()
        } else {
            result
        });
    }
    let current = fs.observe_latest().await.unwrap();
    assert!(current.node(a).await.unwrap().unwrap().executable());
    assert!(current.node(b).await.unwrap().unwrap().executable());
    assert!(!base.node(a).await.unwrap().unwrap().executable());

    // Exact same observed file has at most one successful replacement.
    let newer = fs.prepare(&mut b"new bytes".as_slice()).await.unwrap();
    let mut l = Planner::new(&current, CommitId::generate());
    let mut r = Planner::new(&current, CommitId::generate());
    l.set_content(a, newer.clone()).await.unwrap();
    r.set_content(a, file.clone()).await.unwrap();
    committed(fs.commit(&l.finish().unwrap()).await.unwrap());
    assert_eq!(
        fs.commit(&r.finish().unwrap()).await.unwrap(),
        Outcome::Conflict
    );

    // Empty removal is protected against insertions; scans include phantoms.
    let current = fs.observe_latest().await.unwrap();
    let mut remove = Planner::new(&current, CommitId::generate());
    remove.remove(q).await.unwrap();
    let mut scan = Planner::new(&current, CommitId::generate());
    scan.scan(q).await.unwrap();
    let scan = scan.finish().unwrap();
    let mut add = Planner::new(&current, CommitId::generate());
    add.create_directory(q, "child").await.unwrap();
    committed(fs.commit(&add.finish().unwrap()).await.unwrap());
    assert_eq!(
        fs.commit(&remove.finish().unwrap()).await.unwrap(),
        Outcome::Conflict
    );
    assert_eq!(fs.commit(&scan).await.unwrap(), Outcome::Conflict);

    // Competing ancestry changes cannot introduce cycles.
    let current = fs.observe_latest().await.unwrap();
    let mut l = Planner::new(&current, CommitId::generate());
    let mut r = Planner::new(&current, CommitId::generate());
    l.rename(p, q, "p").await.unwrap();
    r.rename(q, p, "q").await.unwrap();
    committed(fs.commit(&l.finish().unwrap()).await.unwrap());
    assert_eq!(
        fs.commit(&r.finish().unwrap()).await.unwrap(),
        Outcome::Conflict
    );

    // Batched results share a revision but retain transaction order.
    let current = fs.observe_latest().await.unwrap();
    let mut l = Planner::new(&current, CommitId::generate());
    let mut r = Planner::new(&current, CommitId::generate());
    l.set_content(a, file).await.unwrap();
    r.set_content(b, newer).await.unwrap();
    let batch = fs
        .commit_batch(&[l.finish().unwrap(), r.finish().unwrap()])
        .await
        .unwrap();
    let l = committed(batch[0].clone());
    let r = committed(batch[1].clone());
    assert_eq!(l.cursor.revision, r.cursor.revision);
    assert!(l.cursor.ordinal < r.cursor.ordinal);
    let pinned = fs.observe_revision(receipt.cursor.revision).await.unwrap();
    let page = pinned.scan(fs.root(), None, 1).await.unwrap();
    assert!(page.next.is_some());
    let latest = fs.observe_latest().await.unwrap();
    assert_eq!(
        latest
            .scan(fs.root(), page.next.as_ref(), 1)
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Invalid
    );
    assert_eq!(
        pinned.receipt(transaction.id()).await.unwrap(),
        Some(receipt)
    );
    assert!(latest.changes(None, 4096).await.unwrap().len() >= 6);

    // A same-length update reuses the exact selected published version.
    let selected = latest.open_file(a).await.unwrap();
    let update = [ContentEdit::new(0..4, b"1234").unwrap()];
    let changed = fs.prepare_overwrite(&selected, &update).await.unwrap();
    let mut rewrite = Planner::new(&latest, CommitId::generate());
    rewrite.set_content(a, changed.clone()).await.unwrap();
    committed(fs.commit(&rewrite.finish().unwrap()).await.unwrap());
    let mut bytes = Vec::new();
    fs.data()
        .read_range(changed.descriptor(), 0..8, &mut bytes)
        .await
        .unwrap();
    assert_eq!(bytes, b"1234inal");

    // A length-changing update preserves only the explicit base prefix.
    let latest = fs.observe_latest().await.unwrap();
    let selected = latest.open_file(a).await.unwrap();
    let length_edits = [
        ContentEdit::new(2..6, b"CDEF").unwrap(),
        ContentEdit::new(9..13, b"tail").unwrap(),
    ];
    let extended = Authority::prepare_update(fs, &selected, 4, 13, &length_edits)
        .await
        .unwrap();
    let expected = b"12CDEF\0\0\0tail";
    let canonical = fs.data().prepare(&mut expected.as_slice()).await.unwrap();
    assert_eq!(extended.content_id(), canonical.content_id());
    let mut bytes = Vec::new();
    fs.data()
        .read_range(extended.descriptor(), 0..13, &mut bytes)
        .await
        .unwrap();
    assert_eq!(bytes, expected);
}

async fn expiry_contract(fs: &dyn Authority, label: &str) {
    let initial = fs.observe_latest().await.unwrap();
    let held_content = fs.prepare(&mut b"held".as_slice()).await.unwrap();
    let mut first = Planner::new(&initial, CommitId::generate());
    first
        .create_directory(fs.root(), "expired-name")
        .await
        .unwrap();
    let held_file = first
        .create_file(fs.root(), "held-file", held_content, false)
        .await
        .unwrap();
    let first_plan = first.finish().unwrap();
    let first_receipt = committed(fs.commit(&first_plan).await.unwrap());
    let first_revision = initial.revision();
    let held = fs.observe_latest().await.unwrap();
    let held_revision = held.revision();
    let held_version = held.open_file(held_file).await.unwrap();

    let mut second = Planner::new(&fs.observe_latest().await.unwrap(), CommitId::generate());
    second
        .create_directory(fs.root(), "current-name")
        .await
        .unwrap();
    committed(fs.commit(&second.finish().unwrap()).await.unwrap());
    let latest = fs.observe_latest().await.unwrap().revision();

    let request = ExpiryRequest::new(
        fs.filesystem(),
        ExpiryId::generate(),
        latest,
        vec![first_revision, held_revision],
        Vec::new(),
        [7; 32],
    )
    .unwrap();
    let ExpiryOutcome::Committed(receipt) =
        Authority::expire_revisions(fs, &request).await.unwrap()
    else {
        panic!("{label}: expected committed expiry");
    };
    assert_eq!(receipt.expired, vec![first_revision, held_revision]);
    assert_eq!(receipt.before, latest);
    assert!(receipt.after > latest);

    let expired = fs.observe_revision(first_revision).await.unwrap_err();
    assert_eq!(expired.kind(), ErrorKind::Expired);
    assert_eq!(expired.expired_revision(), Some(first_revision));
    let held_expired = fs.observe_revision(held_revision).await.unwrap_err();
    assert_eq!(held_expired.kind(), ErrorKind::Expired);
    assert_eq!(held_expired.expired_revision(), Some(held_revision));
    assert!(held.resolve("held-file").await.unwrap().is_some());
    assert_eq!(held_version.read(0, 4).await.unwrap(), b"held");
    let absent = fs
        .observe_revision(Revision::from_bytes([0xaa; 24]))
        .await
        .unwrap_err();
    assert_eq!(absent.kind(), ErrorKind::NotFound);

    let history = Authority::history_page(fs, None, 256)
        .await
        .unwrap_or_else(|error| panic!("{label}: read history: {error}"));
    assert!(history.entries.iter().any(|entry| {
        entry.revision == first_revision && matches!(entry.state, RevisionState::Expired(_))
    }));
    assert!(history.entries.iter().any(|entry| {
        entry.revision == held_revision && matches!(entry.state, RevisionState::Expired(_))
    }));
    assert!(history.entries.iter().any(|entry| {
        entry.revision == receipt.after && matches!(entry.state, RevisionState::Readable)
    }));

    let replay = Authority::expire_revisions(fs, &request).await.unwrap();
    assert_eq!(replay, ExpiryOutcome::Committed(receipt.clone()));
    let resolved = Authority::resolve_receipt(fs, &first_plan.to_bytes().unwrap())
        .await
        .unwrap()
        .expect("original receipt remains resolvable");
    assert_eq!(resolved, first_receipt);
    assert_eq!(
        committed(fs.commit(&first_plan).await.unwrap()),
        first_receipt
    );
    let mut next = Planner::new(&fs.observe_latest().await.unwrap(), CommitId::generate());
    next.create_directory(fs.root(), "after-expiry")
        .await
        .unwrap();
    let next = next.finish().unwrap();
    let batch = fs.commit_batch(&[first_plan.clone(), next]).await.unwrap();
    assert_eq!(batch.len(), 2);
    assert_eq!(batch[0], Outcome::Committed(first_receipt.clone()));
    assert!(matches!(batch[1], Outcome::Committed(_)));
    assert_eq!(
        fs.observe_revision(first_revision)
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Expired
    );

    type Wire = (
        [u8; 8],
        [u8; 16],
        [u8; 16],
        [u8; 24],
        [u8; 32],
        Vec<(Vec<u8>, Option<Vec<u8>>)>,
        Vec<Vec<u8>>,
    );
    let mut tampered: Wire = borsh::from_slice(&first_plan.to_bytes().unwrap()).unwrap();
    tampered.4 = [0; 32];
    assert_eq!(
        Authority::resolve_receipt(fs, &borsh::to_vec(&tampered).unwrap())
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Invalid
    );

    let after = fs.observe_latest().await.unwrap().revision();
    let protected = ExpiryRequest::new(
        fs.filesystem(),
        ExpiryId::generate(),
        after,
        vec![first_revision],
        vec![first_revision],
        [8; 32],
    )
    .unwrap();
    let ExpiryOutcome::Refused(refusal) =
        Authority::expire_revisions(fs, &protected).await.unwrap()
    else {
        panic!("{label}: expected protected refusal");
    };
    assert!(refusal.conflicts.iter().any(|conflict| {
        conflict.revision == first_revision
            && matches!(
                conflict.kind,
                ExpiryConflictKind::Protected | ExpiryConflictKind::AlreadyExpired
            )
    }));

    let missing = ExpiryRequest::new(
        fs.filesystem(),
        ExpiryId::generate(),
        after,
        vec![Revision::from_bytes([0xbb; 24])],
        Vec::new(),
        [9; 32],
    )
    .unwrap();
    let ExpiryOutcome::Refused(refusal) = Authority::expire_revisions(fs, &missing).await.unwrap()
    else {
        panic!("{label}: expected missing refusal");
    };
    assert!(
        refusal
            .conflicts
            .iter()
            .any(|conflict| conflict.kind == ExpiryConflictKind::Missing)
    );

    let stale = ExpiryRequest::new(
        fs.filesystem(),
        ExpiryId::generate(),
        latest,
        vec![first_revision],
        Vec::new(),
        [10; 32],
    )
    .unwrap();
    assert_eq!(
        Authority::expire_revisions(fs, &stale).await.unwrap(),
        ExpiryOutcome::Conflict
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shared_contract_object_and_metadata_service() {
    let object_backend = TestBackend::default();
    let object = ObjectFs::create(object_backend.operator(), BackendProfile::Minio)
        .await
        .unwrap();
    suite(&object).await;
    let dir = tempfile::tempdir().unwrap();
    let backend = TestBackend::default();
    let service = MetadataService::create(dir.path().join("metadata.db"), backend.operator())
        .await
        .unwrap();
    suite(&service).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn logical_expiry_contract_object_service_and_client() {
    let object_backend = TestBackend::default();
    let object = ObjectFs::create(object_backend.operator(), BackendProfile::Minio)
        .await
        .unwrap();
    expiry_contract(&object, "object").await;

    let dir = tempfile::tempdir().unwrap();
    let backend = TestBackend::default();
    let service = MetadataService::create(dir.path().join("metadata.db"), backend.operator())
        .await
        .unwrap();
    expiry_contract(&service, "embedded-service").await;

    let dir = tempfile::tempdir().unwrap();
    let backend = TestBackend::default();
    let service = MetadataService::create(dir.path().join("metadata.db"), backend.operator())
        .await
        .unwrap();
    let token = "test-only-service-token-32-bytes-long";
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(serve(listener, service.clone(), token.into()));
    let client = ServiceClient::connect(addr, token.into(), backend.operator())
        .await
        .unwrap();
    expiry_contract(&client, "service-client").await;
    server.abort();
    let _ = server.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn authenticated_service_protocol_and_restart() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("metadata.db");
    let backend = TestBackend::default();
    let service = MetadataService::create(&path, backend.operator())
        .await
        .unwrap();
    assert_eq!(
        ObjectFs::open(backend.operator(), BackendProfile::Minio)
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Unsupported
    );
    let token = "test-only-service-token-32-bytes-long";
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(serve(listener, service.clone(), token.into()));
    assert_eq!(
        ServiceClient::connect(
            addr,
            "wrong-token-but-at-least-32-bytes-long".into(),
            backend.operator()
        )
        .await
        .unwrap_err()
        .kind(),
        ErrorKind::Invalid
    );
    let client = ServiceClient::connect(addr, token.into(), backend.operator())
        .await
        .unwrap();
    assert!(!format!("{client:?}").contains(token));
    suite(&client).await;
    let snapshot = client.observe_latest().await.unwrap();
    let content = client.prepare(&mut b"persisted".as_slice()).await.unwrap();
    let mut plan = Planner::new(&snapshot, CommitId::generate());
    plan.create_file(client.root(), "persisted", content, false)
        .await
        .unwrap();
    let plan = plan.finish().unwrap();
    let receipt = committed(client.commit(&plan).await.unwrap());
    server.abort();
    let _ = server.await;
    drop(service);
    let reopened = MetadataService::open(&path, backend.operator())
        .await
        .unwrap();
    let restored = reopened
        .restore_transaction(&plan.to_bytes().unwrap())
        .await
        .unwrap();
    // Even with object-head writes failing, service publication and lookup work.
    backend.state.lock().unwrap().fail_after_head_write = true;
    assert_eq!(
        committed(reopened.commit(&restored).await.unwrap()),
        receipt
    );
    assert!(
        reopened
            .observe_revision(snapshot.revision())
            .await
            .unwrap()
            .resolve("persisted")
            .await
            .unwrap()
            .is_none()
    );
    let mut plan = Planner::new(
        &reopened.observe_latest().await.unwrap(),
        CommitId::generate(),
    );
    plan.create_directory(reopened.root(), "after-restart")
        .await
        .unwrap();
    committed(reopened.commit(&plan.finish().unwrap()).await.unwrap());
    assert!(backend.state.lock().unwrap().fail_after_head_write);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retained_revision_history_is_paginated_through_the_service_protocol() {
    let dir = tempfile::tempdir().unwrap();
    let backend = TestBackend::default();
    let service = MetadataService::create(dir.path().join("metadata.db"), backend.operator())
        .await
        .unwrap();
    for name in ["one", "two", "three"] {
        let mut plan = Planner::new(
            &service.observe_latest().await.unwrap(),
            CommitId::generate(),
        );
        plan.create_directory(service.root(), name).await.unwrap();
        committed(service.commit(&plan.finish().unwrap()).await.unwrap());
    }
    let expected = service.retained_revisions().await.unwrap();
    assert_eq!(expected.len(), 4);

    let token = "test-only-service-token-32-bytes-long";
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(serve(listener, service.clone(), token.into()));
    let client = ServiceClient::connect(addr, token.into(), backend.operator())
        .await
        .unwrap();

    let first = client.retained_revisions_page(None, 2).await.unwrap();
    assert_eq!(first.revisions, expected[..2]);
    let cursor = first
        .next
        .expect("two of four revisions have a continuation");
    let second = client
        .retained_revisions_page(Some(cursor), 2)
        .await
        .unwrap();
    assert_eq!(second.revisions, expected[2..]);
    assert_eq!(second.next, None);
    assert_eq!(
        client
            .retained_revisions_page(None, 0)
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Unsupported
    );
    server.abort();
    let _ = server.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn authenticated_delta_preparation_reuses_the_exact_published_base() {
    use yinyang_core::data::{BLOCK_BYTES, MAX_UPDATE_UNITS};

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("metadata.db");
    let backend = TestBackend::default();
    let service = MetadataService::create(&path, backend.operator())
        .await
        .unwrap();
    let token = "test-only-service-token-32-bytes-long";
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(serve(listener, service.clone(), token.into()));
    let client = ServiceClient::connect(addr, token.into(), backend.operator())
        .await
        .unwrap();

    let snapshot = client.observe_latest().await.unwrap();
    let bytes = (0..8 * 1024 * 1024 + 17)
        .map(|index| (index % 251) as u8)
        .collect::<Vec<_>>();
    let prepared = client.prepare(&mut bytes.as_slice()).await.unwrap();
    let mut create = Planner::new(&snapshot, CommitId::generate());
    let node = create
        .create_file(client.root(), "large", prepared, false)
        .await
        .unwrap();
    let created = committed(client.commit(&create.finish().unwrap()).await.unwrap());

    let base = client
        .observe_revision(created.cursor.revision)
        .await
        .unwrap()
        .open_file(node)
        .await
        .unwrap();
    let full_bytes = base.descriptor().content_id().length();
    backend.state.lock().unwrap().data_reads = 0;
    backend.state.lock().unwrap().written_bytes = 0;
    assert_eq!(
        client
            .prepare_update(&base, 0, (MAX_UPDATE_UNITS + 1) * BLOCK_BYTES as u64, &[],)
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Unsupported
    );
    {
        let state = backend.state.lock().unwrap();
        assert_eq!(state.data_reads, 0);
        assert_eq!(state.written_bytes, 0);
    }

    let mut later = Planner::new(
        &client.observe_latest().await.unwrap(),
        CommitId::generate(),
    );
    later
        .create_directory(client.root(), "later")
        .await
        .unwrap();
    committed(client.commit(&later.finish().unwrap()).await.unwrap());

    backend.state.lock().unwrap().data_read_bytes = 0;
    backend.state.lock().unwrap().data_reads = 0;
    let range = 4 * 1024 * 1024 - 7..4 * 1024 * 1024 + 9;
    let change = [ContentEdit::new(range, b"delta-service-16").unwrap()];
    let changed = Authority::prepare_overwrite(&client, &base, &change)
        .await
        .unwrap();
    let (data_read_bytes, data_reads) = {
        let state = backend.state.lock().unwrap();
        (state.data_read_bytes, state.data_reads)
    };
    assert!(
        data_read_bytes < full_bytes / 8,
        "service delta preparation read {data_read_bytes} bytes in {data_reads} reads"
    );
    assert!(
        data_reads <= 64,
        "service delta preparation made {data_reads} reads"
    );

    let mut update = Planner::new(
        &client.observe_latest().await.unwrap(),
        CommitId::generate(),
    );
    update.set_content(node, changed).await.unwrap();
    let request = update.finish().unwrap();
    let receipt = committed(client.commit(&request).await.unwrap());

    server.abort();
    let _ = server.await;
    drop(service);
    drop(client);
    let reopened = MetadataService::open(&path, backend.operator())
        .await
        .unwrap();
    let restored = reopened
        .restore_transaction(&request.to_bytes().unwrap())
        .await
        .unwrap();
    assert_eq!(
        committed(reopened.commit(&restored).await.unwrap()),
        receipt
    );
}

#[tokio::test]
async fn service_rejects_unregistered_content_and_missing_guards() {
    let dir = tempfile::tempdir().unwrap();
    let backend = TestBackend::default();
    let service = Arc::new(
        MetadataService::create(dir.path().join("metadata.db"), backend.operator())
            .await
            .unwrap(),
    );
    let snapshot = service.observe_latest().await.unwrap();
    assert_eq!(
        service.retained_revisions().await.unwrap(),
        vec![snapshot.revision()]
    );
    let content = service
        .data()
        .prepare(&mut b"unregistered".as_slice())
        .await
        .unwrap();
    let mut plan = Planner::new(&snapshot, CommitId::generate());
    plan.create_file(service.root(), "file", content.clone(), false)
        .await
        .unwrap();
    let plan = plan.finish().unwrap();
    assert_eq!(
        service.commit(&plan).await.unwrap_err().kind(),
        ErrorKind::Invalid
    );
    service.register(content.descriptor()).await.unwrap();
    let reads = backend.state.lock().unwrap().data_reads;
    let bytes = plan.to_bytes().unwrap();
    type Wire = (
        [u8; 8],
        [u8; 16],
        [u8; 16],
        [u8; 24],
        [u8; 32],
        Vec<(Vec<u8>, Option<Vec<u8>>)>,
        Vec<Vec<u8>>,
    );
    let mut wire: Wire = borsh::from_slice(&bytes).unwrap();
    wire.5.clear();
    let error = service
        .restore_transaction(&borsh::to_vec(&wire).unwrap())
        .await
        .unwrap_err();
    assert!(
        error.message().contains("missing mandatory predicate"),
        "{error}"
    );
    committed(service.commit(&plan).await.unwrap());
    assert_eq!(backend.state.lock().unwrap().data_reads, reads);
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lost_and_delayed_rpc_responses_resolve_the_original_request() {
    use std::sync::atomic::{AtomicU8, Ordering};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};
    let dir = tempfile::tempdir().unwrap();
    let backend = TestBackend::default();
    let service = MetadataService::create(dir.path().join("db"), backend.operator())
        .await
        .unwrap();
    let token = "test-only-service-token-32-bytes-long";
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let upstream = listener.local_addr().unwrap();
    let server = tokio::spawn(serve(listener, service.clone(), token.into()));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let mode = Arc::new(AtomicU8::new(0));
    let reached = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let proxy = {
        let (mode, reached, release) = (mode.clone(), reached.clone(), release.clone());
        tokio::spawn(async move {
            loop {
                let (mut downstream, _) = listener.accept().await.unwrap();
                let (mode, reached, release) = (mode.clone(), reached.clone(), release.clone());
                tokio::spawn(async move {
                    let mut upstream = TcpStream::connect(upstream).await.unwrap();
                    let length = downstream.read_u32().await.unwrap();
                    let mut request = vec![0; length as usize];
                    downstream.read_exact(&mut request).await.unwrap();
                    upstream.write_u32(length).await.unwrap();
                    upstream.write_all(&request).await.unwrap();
                    let length = upstream.read_u32().await.unwrap();
                    let mut response = vec![0; length as usize];
                    upstream.read_exact(&mut response).await.unwrap();
                    match mode.swap(0, Ordering::SeqCst) {
                        1 => return, // Commit is durable, but acknowledgement is lost.
                        2 => {
                            reached.notify_one();
                            release.notified().await;
                        }
                        _ => {}
                    }
                    let _ = downstream.write_u32(length).await;
                    let _ = downstream.write_all(&response).await;
                });
            }
        })
    };
    let client = ServiceClient::connect(addr, token.into(), backend.operator())
        .await
        .unwrap();
    let first = service.observe_latest().await.unwrap();
    let mut plan = Planner::new(&first, CommitId::generate());
    plan.create_directory(service.root(), "lost").await.unwrap();
    let plan = plan.finish().unwrap();
    mode.store(1, Ordering::SeqCst);
    assert_eq!(
        client.commit(&plan).await.unwrap(),
        Outcome::Unknown(plan.id())
    );
    let receipt = service
        .observe_latest()
        .await
        .unwrap()
        .receipt(plan.id())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(committed(client.commit(&plan).await.unwrap()), receipt);

    let mut delayed = Planner::new(
        &service.observe_latest().await.unwrap(),
        CommitId::generate(),
    );
    delayed
        .create_directory(service.root(), "delayed")
        .await
        .unwrap();
    let delayed = delayed.finish().unwrap();
    let id = delayed.id();
    mode.store(2, Ordering::SeqCst);
    let pending = tokio::spawn(async move { client.commit(&delayed).await });
    reached.notified().await;
    let intermediate = service.observe_latest().await.unwrap();
    let old = intermediate.receipt(id).await.unwrap().unwrap();
    let mut next = Planner::new(&intermediate, CommitId::generate());
    next.create_directory(service.root(), "later")
        .await
        .unwrap();
    committed(service.commit(&next.finish().unwrap()).await.unwrap());
    release.notify_one();
    assert_eq!(committed(pending.await.unwrap().unwrap()), old);
    let expiry_request = ExpiryRequest::new(
        service.filesystem(),
        ExpiryId::generate(),
        service.observe_latest().await.unwrap().revision(),
        vec![first.revision()],
        Vec::new(),
        [0x44; 32],
    )
    .unwrap();
    mode.store(1, Ordering::SeqCst);
    assert_eq!(
        client.expire_revisions(&expiry_request).await.unwrap(),
        ExpiryOutcome::Unknown(expiry_request.id())
    );
    let expiry_receipt = match client.expire_revisions(&expiry_request).await.unwrap() {
        ExpiryOutcome::Committed(receipt) => receipt,
        other => panic!("expected committed expiry replay: {other:?}"),
    };
    assert_eq!(expiry_receipt.id, expiry_request.id());
    assert_eq!(expiry_receipt.before, expiry_request.expected_latest());
    assert_eq!(
        expiry_receipt.expired.as_slice(),
        expiry_request.candidates()
    );
    assert!(first.resolve("lost").await.unwrap().is_none());
    proxy.abort();
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn independent_authorities_busy_retry_and_identity_misuse() {
    let dir = tempfile::tempdir().unwrap();
    let backend = TestBackend::default();
    let path = dir.path().join("db");
    let service = MetadataService::create(&path, backend.operator())
        .await
        .unwrap();
    let second = MetadataService::open(&path, backend.operator())
        .await
        .unwrap();
    let base = service.observe_latest().await.unwrap();
    let mut plan = Planner::new(&base, CommitId::generate());
    plan.create_directory(service.root(), "winner")
        .await
        .unwrap();
    let plan = plan.finish().unwrap();
    let lock = rusqlite::Connection::open(&path).unwrap();
    lock.execute_batch("BEGIN IMMEDIATE").unwrap();
    assert_eq!(second.commit(&plan).await.unwrap(), Outcome::Retryable);
    assert!(base.receipt(plan.id()).await.unwrap().is_none());
    lock.execute_batch("ROLLBACK").unwrap();
    let receipt = committed(second.commit(&plan).await.unwrap());
    assert_eq!(committed(service.commit(&plan).await.unwrap()), receipt);
    let mut misuse = Planner::new(&base, plan.id());
    misuse
        .create_directory(service.root(), "different")
        .await
        .unwrap();
    assert_eq!(
        service
            .commit(&misuse.finish().unwrap())
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Invalid
    );
}
