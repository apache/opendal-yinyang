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

use crate::data::{ContentDescriptor, DataStore};
use crate::namespace::name_key;
use crate::object::{BackendProfile, ObjectFs};
use crate::service::{MetadataService, ServiceClient, serve};
use crate::support::{StoredObject, TestBackend};
use crate::{CommitId, ErrorKind, NodeId, Transaction};

const FILESYSTEM: [u8; 16] = [0x11; 16];
const OBJECT: [u8; 16] = [0x22; 16];
const COMMIT: [u8; 16] = [0x33; 16];
const ROOT: [u8; 16] = [0x55; 16];
const CONTENT_PACK: &[u8] = include_bytes!("../tests/fixtures/compatibility-v2/content.pack");
const CONTENT_DESCRIPTOR: &[u8] =
    include_bytes!("../tests/fixtures/compatibility-v2/content.descriptor");
const CONTENT_ID: &str = include_str!("../tests/fixtures/compatibility-v2/content.id");
const CONTENT_UNKNOWN_DESCRIPTOR: &[u8] =
    include_bytes!("../tests/fixtures/compatibility-v2/content-unknown.descriptor");
const CONTENT_MALFORMED_DESCRIPTOR: &[u8] =
    include_bytes!("../tests/fixtures/compatibility-v2/content-malformed.descriptor");
const CONTENT_UNKNOWN_INDEX_DESCRIPTOR: &[u8] =
    include_bytes!("../tests/fixtures/compatibility-v2/content-unknown-index.descriptor");
const CONTENT_UNKNOWN_INDEX_PACK: &[u8] =
    include_bytes!("../tests/fixtures/compatibility-v2/content-unknown-index.pack");
const CONTENT_MALFORMED_INDEX_DESCRIPTOR: &[u8] =
    include_bytes!("../tests/fixtures/compatibility-v2/content-malformed-index.descriptor");
const CONTENT_MALFORMED_INDEX_PACK: &[u8] =
    include_bytes!("../tests/fixtures/compatibility-v2/content-malformed-index.pack");
const NAME_KEYS: &str = include_str!("../tests/fixtures/compatibility-v2/name-keys.tsv");
const RECEIPT: &[u8] = include_bytes!("../tests/fixtures/compatibility-v2/receipt.bin");
const RECEIPT_MALFORMED: &[u8] =
    include_bytes!("../tests/fixtures/compatibility-v2/receipt-malformed.bin");
const TRANSACTION_PLAN: &[u8] =
    include_bytes!("../tests/fixtures/compatibility-v2/transaction.plan");
const TRANSACTION_UNKNOWN: &[u8] =
    include_bytes!("../tests/fixtures/compatibility-v2/transaction-unknown.plan");
const OBJECT_CURRENT_HEAD: &[u8] =
    include_bytes!("../tests/fixtures/compatibility-v2/object-current.head");
const OBJECT_CURRENT_PACK: &[u8] =
    include_bytes!("../tests/fixtures/compatibility-v2/object-current.pack");
const OBJECT_UNKNOWN_HEAD: &[u8] =
    include_bytes!("../tests/fixtures/compatibility-v2/object-unknown-head.bin");
const OBJECT_UNKNOWN_SNAPSHOT_HEAD: &[u8] =
    include_bytes!("../tests/fixtures/compatibility-v2/object-unknown-snapshot.head");
const OBJECT_UNKNOWN_SNAPSHOT_PACK: &[u8] =
    include_bytes!("../tests/fixtures/compatibility-v2/object-unknown-snapshot.pack");
const OBJECT_UNKNOWN_INDEX_HEAD: &[u8] =
    include_bytes!("../tests/fixtures/compatibility-v2/object-unknown-index.head");
const OBJECT_UNKNOWN_INDEX_PACK: &[u8] =
    include_bytes!("../tests/fixtures/compatibility-v2/object-unknown-index.pack");
const SERVICE_CURRENT: &[u8] =
    include_bytes!("../tests/fixtures/compatibility-v2/service-current.db");
const SERVICE_UNKNOWN: &[u8] =
    include_bytes!("../tests/fixtures/compatibility-v2/service-unknown.db");
const SERVICE_BINDING_CURRENT: &[u8] =
    include_bytes!("../tests/fixtures/compatibility-v2/service-binding-current.bin");
const SERVICE_BINDING_UNKNOWN: &[u8] =
    include_bytes!("../tests/fixtures/compatibility-v2/service-binding-unknown.bin");
const SERVICE_BINDING_MALFORMED: &[u8] =
    include_bytes!("../tests/fixtures/compatibility-v2/service-binding-malformed.bin");
const SERVICE_BINDING_OTHER: &[u8] =
    include_bytes!("../tests/fixtures/compatibility-v2/service-binding-other.bin");

fn pack_key() -> String {
    format!(
        ".yinyang/v2/{}/packs/{}",
        uuid::Uuid::from_bytes(FILESYSTEM).simple(),
        uuid::Uuid::from_bytes(OBJECT).simple()
    )
}

fn insert_object(backend: &TestBackend, path: &str, bytes: &[u8]) {
    backend.state.lock().unwrap().objects.insert(
        path.to_owned(),
        StoredObject {
            bytes: bytes.to_vec(),
            etag: format!("fixture-{path}"),
        },
    );
}

fn seed_pack(backend: &TestBackend, bytes: &[u8]) {
    insert_object(backend, &pack_key(), bytes);
}

fn data_store(backend: &TestBackend) -> DataStore {
    DataStore::new(backend.operator(), NodeId::from_bytes(FILESYSTEM)).unwrap()
}

#[tokio::test]
async fn independent_content_vectors_read_and_reject_before_output() {
    let backend = TestBackend::default();
    seed_pack(&backend, CONTENT_PACK);
    let store = data_store(&backend);
    let descriptor = ContentDescriptor::from_bytes(CONTENT_DESCRIPTOR).unwrap();
    assert_eq!(descriptor.to_bytes().as_slice(), CONTENT_DESCRIPTOR);
    assert_eq!(
        blake3::Hash::from(*descriptor.content_id().digest())
            .to_hex()
            .as_str(),
        CONTENT_ID.trim()
    );

    let mut output = Vec::new();
    store
        .read_range(&descriptor, 65_535..65_537, &mut output)
        .await
        .unwrap();
    assert_eq!(output, [7, b'x']);
    output.clear();
    store
        .read_range(&descriptor, 65_536..65_537, &mut output)
        .await
        .unwrap();
    assert_eq!(output, b"x");

    assert_eq!(
        ContentDescriptor::from_bytes(CONTENT_UNKNOWN_DESCRIPTOR)
            .unwrap_err()
            .kind(),
        ErrorKind::Unsupported
    );
    assert_eq!(
        ContentDescriptor::from_bytes(CONTENT_MALFORMED_DESCRIPTOR)
            .unwrap_err()
            .kind(),
        ErrorKind::Corrupt
    );
}

#[tokio::test]
async fn unknown_or_malformed_content_index_profiles_are_explicit() {
    let backend = TestBackend::default();
    seed_pack(&backend, CONTENT_UNKNOWN_INDEX_PACK);
    let store = data_store(&backend);
    let descriptor = ContentDescriptor::from_bytes(CONTENT_UNKNOWN_INDEX_DESCRIPTOR).unwrap();
    let mut output = Vec::new();
    assert_eq!(
        store
            .read_range(&descriptor, 0..1, &mut output)
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Unsupported
    );
    assert!(output.is_empty());

    seed_pack(&backend, CONTENT_MALFORMED_INDEX_PACK);
    let descriptor = ContentDescriptor::from_bytes(CONTENT_MALFORMED_INDEX_DESCRIPTOR).unwrap();
    assert_eq!(
        store
            .read_range(&descriptor, 0..1, &mut output)
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Corrupt
    );
    assert!(output.is_empty());
}

#[test]
fn independent_name_keys_and_byte_order_are_stable() {
    let mut ordered = Vec::new();
    for line in NAME_KEYS.lines().filter(|line| !line.starts_with('#')) {
        let mut fields = line.split('\t');
        let input = fields.next().unwrap();
        let folded = fields.next().unwrap();
        let order = fields.next().unwrap().parse::<usize>().unwrap();
        assert_eq!(name_key(input).unwrap(), folded);
        ordered.push((order, folded.to_owned()));
    }
    ordered.sort_by_key(|item| item.0);
    let keys = ordered.into_iter().map(|item| item.1).collect::<Vec<_>>();
    let mut sorted = keys.clone();
    sorted.sort_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
    assert_eq!(keys, sorted);
}

#[test]
fn independent_receipt_vector_round_trips_and_rejects_trailing_bytes() {
    let receipt = crate::Receipt::decode(RECEIPT).unwrap();
    assert_eq!(receipt.commit_id, CommitId::from_bytes(COMMIT));
    assert_eq!(receipt.request_digest, [0x77; 32]);
    assert_eq!(receipt.cursor.revision.sequence, 7);
    assert_eq!(receipt.cursor.ordinal, 2);
    assert_eq!(receipt.encode().unwrap().as_slice(), RECEIPT);
    assert_eq!(
        crate::Receipt::decode(RECEIPT_MALFORMED)
            .unwrap_err()
            .kind(),
        ErrorKind::Corrupt
    );
}

#[test]
fn frozen_transaction_profiles_report_identity_before_restore() {
    let (revision, files) = Transaction::inspect(TRANSACTION_PLAN).unwrap();
    assert_eq!(revision.sequence, 7);
    assert_eq!(files.len(), 1);
    assert_eq!(files[0].to_bytes(), CONTENT_DESCRIPTOR);
    let (revision, id, digest) = Transaction::inspect_identity(TRANSACTION_PLAN).unwrap();
    assert_eq!(revision.sequence, 7);
    assert_eq!(id, CommitId::from_bytes(COMMIT));
    assert_eq!(digest.len(), 32);
    assert_eq!(
        Transaction::inspect_identity(TRANSACTION_UNKNOWN)
            .unwrap_err()
            .kind(),
        ErrorKind::Unsupported
    );
}

async fn assert_object_profile_open(head: &[u8], pack: &[u8], expected: Option<ErrorKind>) {
    let backend = TestBackend::default();
    insert_object(&backend, ".yinyang/head", head);
    seed_pack(&backend, pack);
    let before = backend.state.lock().unwrap().objects.clone();
    let result = ObjectFs::open(backend.operator(), BackendProfile::Minio).await;
    match expected {
        Some(kind) => assert_eq!(result.unwrap_err().kind(), kind),
        None => {
            let fs = result.unwrap();
            assert_eq!(fs.filesystem(), NodeId::from_bytes(FILESYSTEM));
            assert_eq!(fs.root(), NodeId::from_bytes(ROOT));
            let node = fs
                .observe_latest()
                .await
                .unwrap()
                .node(fs.root())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(node.generation(), 1);
        }
    }
    assert_eq!(backend.state.lock().unwrap().objects, before);
}

#[tokio::test]
async fn independent_object_fixtures_admit_current_and_reject_unknown_profiles() {
    assert_object_profile_open(OBJECT_CURRENT_HEAD, OBJECT_CURRENT_PACK, None).await;
    assert_object_profile_open(OBJECT_UNKNOWN_HEAD, b"", Some(ErrorKind::Unsupported)).await;
    assert_object_profile_open(
        OBJECT_UNKNOWN_SNAPSHOT_HEAD,
        OBJECT_UNKNOWN_SNAPSHOT_PACK,
        Some(ErrorKind::Unsupported),
    )
    .await;
    assert_object_profile_open(
        OBJECT_UNKNOWN_INDEX_HEAD,
        OBJECT_UNKNOWN_INDEX_PACK,
        Some(ErrorKind::Unsupported),
    )
    .await;
}

fn service_binding() -> Vec<u8> {
    SERVICE_BINDING_CURRENT.to_vec()
}

#[tokio::test]
async fn independent_sqlite_fixture_reads_manifest_and_receipt() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("metadata.db");
    std::fs::write(&path, SERVICE_CURRENT).unwrap();
    let backend = TestBackend::default();
    insert_object(&backend, ".yinyang/head", &service_binding());
    let service = MetadataService::open(&path, backend.operator())
        .await
        .unwrap();
    assert_eq!(service.filesystem(), NodeId::from_bytes(FILESYSTEM));
    assert_eq!(service.root(), NodeId::from_bytes(ROOT));
    let snapshot = service.observe_latest().await.unwrap();
    assert_eq!(
        snapshot
            .receipt(CommitId::from_bytes(COMMIT))
            .await
            .unwrap(),
        Some(crate::Receipt {
            commit_id: CommitId::from_bytes(COMMIT),
            request_digest: [0x77; 32],
            cursor: crate::Cursor {
                revision: snapshot.revision(),
                ordinal: 2,
            },
        })
    );
}

#[tokio::test]
async fn unknown_sqlite_profile_is_rejected_without_changing_the_database() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("metadata.db");
    std::fs::write(&path, SERVICE_UNKNOWN).unwrap();
    let before = std::fs::read(&path).unwrap();
    let operator = opendal::Operator::new(opendal::services::Memory::default()).unwrap();
    let error = MetadataService::open(&path, operator).await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Unsupported);
    assert!(
        error.message().contains("unknown schema profile"),
        "{error}"
    );
    assert_eq!(std::fs::read(&path).unwrap(), before);
    assert!(!path.with_extension("db-wal").exists());
    assert!(!path.with_extension("db-shm").exists());

    let connection = rusqlite::Connection::open(&path).unwrap();
    let mode: String = connection
        .query_row("PRAGMA journal_mode", [], |row| row.get(0))
        .unwrap();
    assert_eq!(mode, "delete");
    let sentinel: String = connection
        .query_row("SELECT value FROM sentinel", [], |row| row.get(0))
        .unwrap();
    assert_eq!(sentinel, "unchanged");
}

fn malformed_authority_schema(path: &std::path::Path, authority: &str, insert: &str) {
    let connection = rusqlite::Connection::open(path).unwrap();
    connection
        .execute_batch(&format!(
            "PRAGMA journal_mode=DELETE;
             {authority}
             CREATE TABLE revisions(token BLOB PRIMARY KEY, sequence INTEGER UNIQUE NOT NULL);
             CREATE TABLE records(family INTEGER NOT NULL, key BLOB NOT NULL, start INTEGER NOT NULL, end INTEGER, value BLOB NOT NULL, PRIMARY KEY(family,key,start));
             CREATE TABLE prepared(descriptor BLOB PRIMARY KEY);
             {insert}"
        ))
        .unwrap();
}

#[tokio::test]
async fn known_profile_schema_shape_is_rejected_before_wal_or_ddl() {
    let cases = [
        (
            "CREATE TABLE authority(singleton INTEGER PRIMARY KEY, profile TEXT NOT NULL, filesystem BLOB NOT NULL, root BLOB NOT NULL);",
            "INSERT INTO authority(singleton,profile,filesystem,root) VALUES(1,'yinyang-sqlite-1',zeroblob(16),zeroblob(16));",
        ),
        (
            "CREATE TABLE authority(singleton INTEGER PRIMARY KEY, filesystem BLOB NOT NULL, profile TEXT NOT NULL, root BLOB NOT NULL, latest BLOB NOT NULL);",
            "INSERT INTO authority VALUES(1,zeroblob(16),'yinyang-sqlite-1',zeroblob(16),zeroblob(24));",
        ),
    ];
    for (index, (authority, insert)) in cases.into_iter().enumerate() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join(format!("metadata-{index}.db"));
        malformed_authority_schema(&path, authority, insert);
        let before = std::fs::read(&path).unwrap();
        let operator = opendal::Operator::new(opendal::services::Memory::default()).unwrap();
        let error = MetadataService::open(&path, operator).await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Corrupt);
        assert!(
            error.message().contains("incompatible authority table"),
            "{error}"
        );
        assert_eq!(std::fs::read(&path).unwrap(), before);
        assert!(!path.with_extension("db-wal").exists());
        assert!(!path.with_extension("db-shm").exists());
        let connection = rusqlite::Connection::open(&path).unwrap();
        let mode: String = connection
            .query_row("PRAGMA journal_mode", [], |row| row.get(0))
            .unwrap();
        assert_eq!(mode, "delete");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn service_binding_fixtures_distinguish_unknown_malformed_and_mismatch() {
    let temp = tempfile::tempdir().unwrap();
    let backend = TestBackend::default();
    insert_object(&backend, ".yinyang/head", SERVICE_BINDING_CURRENT);
    let path = temp.path().join("metadata.db");
    std::fs::write(&path, SERVICE_CURRENT).unwrap();
    let service = MetadataService::open(&path, backend.operator())
        .await
        .unwrap();
    let token = "metadata-rpc-compatibility-test-token";
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(serve(listener, service, token.into()));

    for (marker, kind, message) in [
        (SERVICE_BINDING_CURRENT, None, ""),
        (
            SERVICE_BINDING_UNKNOWN,
            Some(ErrorKind::Unsupported),
            "unknown service binding profile",
        ),
        (
            SERVICE_BINDING_MALFORMED,
            Some(ErrorKind::Corrupt),
            "read namespace",
        ),
        (
            SERVICE_BINDING_OTHER,
            Some(ErrorKind::Invalid),
            "data binding disagrees",
        ),
    ] {
        insert_object(&backend, ".yinyang/head", marker);
        let result = ServiceClient::connect(addr, token.into(), backend.operator()).await;
        match kind {
            Some(kind) => {
                let error = result.err().expect("binding must be rejected");
                assert_eq!(error.kind(), kind);
                assert!(error.to_string().contains(message), "{error}");
            }
            None => {
                result.expect("current binding must connect");
            }
        }
    }
    server.abort();
}
