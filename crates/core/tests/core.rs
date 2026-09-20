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

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use opendal::raw::*;
use opendal::services::Memory;
use opendal::{Buffer, BytesRange, EntryMode, Metadata, OperationContext, Operator};
use yinyang_core::{
    BlobRef, CommitId, CommitOutcome, ContentId, ErrorKind, File, FilePart, Fs, Generation, Node,
    NodeBody, NodeId, Path, Tree,
};

#[derive(Clone, Debug, Default)]
struct TestBackend {
    state: Arc<Mutex<TestState>>,
}

#[derive(Debug, Default)]
struct TestState {
    objects: BTreeMap<String, StoredObject>,
    next_revision: u64,
    fail_after_head_write: bool,
    stat_calls: u64,
    version_write_calls: u64,
    fail_data_close: bool,
    data_aborts: u64,
    data_read_bytes: u64,
    data_reads: u64,
    corrupt_pack_on_close: bool,
}

#[derive(Clone, Debug)]
struct StoredObject {
    bytes: Vec<u8>,
    etag: String,
}

impl TestBackend {
    fn operator(&self) -> Operator {
        self.operator_with_streaming_write(true)
    }

    fn operator_without_streaming_write(&self) -> Operator {
        self.operator_with_streaming_write(false)
    }

    fn operator_with_streaming_write(&self, write_can_multi: bool) -> Operator {
        let service: Servicer = Arc::new(TestService {
            state: self.state.clone(),
            write_can_multi,
        });
        Operator::from_parts(OperationContext::default(), service)
    }

    fn fail_next_head_write_after_success(&self) {
        self.state.lock().unwrap().fail_after_head_write = true;
    }

    fn stat_calls(&self) -> u64 {
        self.state.lock().unwrap().stat_calls
    }

    fn reset_version_write_calls(&self) {
        self.state.lock().unwrap().version_write_calls = 0;
    }

    fn version_write_calls(&self) -> u64 {
        self.state.lock().unwrap().version_write_calls
    }

    fn version_objects(&self) -> Vec<(String, Vec<u8>)> {
        self.state
            .lock()
            .unwrap()
            .objects
            .iter()
            .filter(|(path, _)| path.starts_with(".yinyang/versions/"))
            .map(|(path, object)| (path.clone(), object.bytes.clone()))
            .collect()
    }

    fn remove_current_version(&self) {
        self.state
            .lock()
            .unwrap()
            .objects
            .retain(|path, _| !path.starts_with(".yinyang/versions/"));
    }

    fn corrupt_current_version(&self) {
        let mut state = self.state.lock().unwrap();
        let object = state
            .objects
            .iter_mut()
            .find_map(|(path, object)| path.starts_with(".yinyang/versions/").then_some(object))
            .expect("the test filesystem has a version object");
        object.bytes[0] ^= 1;
    }

    fn corrupt_head(&self) {
        let mut state = self.state.lock().unwrap();
        let object = state
            .objects
            .get_mut(".yinyang/head")
            .expect("the test filesystem has a head");
        let checksum = object
            .bytes
            .last_mut()
            .expect("the head contains a checksum");
        *checksum ^= 1;
    }
}

#[derive(Debug)]
struct TestService {
    state: Arc<Mutex<TestState>>,
    write_can_multi: bool,
}

impl Service for TestService {
    type Reader = TestReader;
    type Writer = TestWriter;
    type Lister = ();
    type Deleter = ();
    type Copier = ();

    fn info(&self) -> ServiceInfo {
        ServiceInfo::with_scheme("yinyang-test")
    }

    fn capability(&self) -> opendal::Capability {
        opendal::Capability {
            stat: true,
            read: true,
            write: true,
            write_can_multi: self.write_can_multi,
            write_can_empty: true,
            write_with_if_match: true,
            write_with_if_not_exists: true,
            shared: true,
            ..Default::default()
        }
    }

    async fn create_dir(
        &self,
        _: &OperationContext,
        _: &str,
        _: OpCreateDir,
    ) -> opendal::Result<RpCreateDir> {
        Err(unsupported())
    }

    async fn stat(&self, _: &OperationContext, path: &str, _: OpStat) -> opendal::Result<RpStat> {
        let mut state = self.state.lock().unwrap();
        state.stat_calls += 1;
        let object = state.objects.get(path).ok_or_else(not_found)?;
        Ok(RpStat::new(metadata(object)))
    }

    fn read(&self, _: &OperationContext, path: &str, _: OpRead) -> opendal::Result<Self::Reader> {
        let state = self.state.lock().unwrap();
        let object = state.objects.get(path).cloned().ok_or_else(not_found)?;
        Ok(TestReader {
            object,
            state: self.state.clone(),
            data: path.contains("/packs/"),
        })
    }

    fn write(
        &self,
        _: &OperationContext,
        path: &str,
        args: OpWrite,
    ) -> opendal::Result<Self::Writer> {
        Ok(TestWriter {
            state: self.state.clone(),
            path: path.to_owned(),
            if_match: args.if_match().map(str::to_owned),
            if_not_exists: args.if_not_exists(),
            bytes: Vec::new(),
        })
    }

    fn delete(&self, _: &OperationContext) -> opendal::Result<Self::Deleter> {
        Err(unsupported())
    }

    fn list(&self, _: &OperationContext, _: &str, _: OpList) -> opendal::Result<Self::Lister> {
        Err(unsupported())
    }

    fn copy(
        &self,
        _: &OperationContext,
        _: &str,
        _: &str,
        _: OpCopy,
        _: OpCopier,
    ) -> opendal::Result<Self::Copier> {
        Err(unsupported())
    }

    async fn rename(
        &self,
        _: &OperationContext,
        _: &str,
        _: &str,
        _: OpRename,
    ) -> opendal::Result<RpRename> {
        Err(unsupported())
    }

    async fn presign(
        &self,
        _: &OperationContext,
        _: &str,
        _: OpPresign,
    ) -> opendal::Result<RpPresign> {
        Err(unsupported())
    }
}

#[derive(Debug)]
struct TestReader {
    object: StoredObject,
    state: Arc<Mutex<TestState>>,
    data: bool,
}

impl oio::Read for TestReader {
    async fn open(
        &self,
        range: BytesRange,
    ) -> opendal::Result<(RpRead, Box<dyn oio::ReadStreamDyn>)> {
        let (response, buffer) = self.read(range).await?;
        Ok((response, Box::new(buffer)))
    }

    async fn read(&self, range: BytesRange) -> opendal::Result<(RpRead, Buffer)> {
        let range = range.to_content_range(self.object.bytes.len())?;
        if self.data {
            let mut state = self.state.lock().unwrap();
            state.data_read_bytes += range.len() as u64;
            state.data_reads += 1;
        }
        Ok((
            RpRead::new(metadata(&self.object)),
            Buffer::from(self.object.bytes[range].to_vec()),
        ))
    }
}

#[derive(Debug)]
struct TestWriter {
    state: Arc<Mutex<TestState>>,
    path: String,
    if_match: Option<String>,
    if_not_exists: bool,
    bytes: Vec<u8>,
}

impl oio::Write for TestWriter {
    async fn write(&mut self, buffer: Buffer) -> opendal::Result<()> {
        if self.path.starts_with(".yinyang/versions/") {
            self.state.lock().unwrap().version_write_calls += 1;
        }
        for chunk in buffer {
            self.bytes.extend_from_slice(&chunk);
        }
        Ok(())
    }

    async fn close(&mut self) -> opendal::Result<Metadata> {
        let mut state = self.state.lock().unwrap();
        if (self.path.starts_with(".yinyang/data/") || self.path.contains("/packs/"))
            && state.fail_data_close
        {
            return Err(opendal::Error::new(
                opendal::ErrorKind::Unexpected,
                "data close failed",
            ));
        }
        let current = state.objects.get(&self.path);
        if self.if_not_exists && current.is_some() {
            return Err(opendal::Error::new(
                opendal::ErrorKind::ConditionNotMatch,
                "object already exists",
            ));
        }
        if self
            .if_match
            .as_ref()
            .is_some_and(|etag| current.is_none_or(|object| object.etag != *etag))
        {
            return Err(opendal::Error::new(
                opendal::ErrorKind::ConditionNotMatch,
                "ETag does not match",
            ));
        }

        state.next_revision += 1;
        let mut object = StoredObject {
            bytes: self.bytes.clone(),
            etag: format!("\"{}\"", state.next_revision),
        };
        if self.path.contains("/packs/") && state.corrupt_pack_on_close && !object.bytes.is_empty()
        {
            object.bytes[0] ^= 1;
        }
        let metadata = metadata(&object);
        state.objects.insert(self.path.clone(), object);
        if self.path == ".yinyang/head" && state.fail_after_head_write {
            state.fail_after_head_write = false;
            return Err(opendal::Error::new(
                opendal::ErrorKind::Unexpected,
                "publication response was lost",
            ));
        }
        Ok(metadata)
    }

    async fn abort(&mut self) -> opendal::Result<()> {
        if self.path.starts_with(".yinyang/data/") || self.path.contains("/packs/") {
            self.state.lock().unwrap().data_aborts += 1;
        }
        self.bytes.clear();
        Ok(())
    }
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

fn metadata(object: &StoredObject) -> Metadata {
    Metadata::new(EntryMode::FILE)
        .with_content_length(object.bytes.len() as u64)
        .with_etag(object.etag.clone())
}

fn not_found() -> opendal::Error {
    opendal::Error::new(opendal::ErrorKind::NotFound, "object is missing")
}

fn unsupported() -> opendal::Error {
    opendal::Error::new(
        opendal::ErrorKind::Unsupported,
        "operation is not supported by the test backend",
    )
}

fn add_directory(tree: &Tree, name: &str, id: NodeId) -> Tree {
    let mut successor = tree.clone();
    advance_root_membership(&mut successor);
    successor.insert(
        Path::new(name).unwrap(),
        Node::dir(id, Generation::FIRST, false, Generation::FIRST),
    );
    successor
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

fn advance_root_membership(tree: &mut Tree) {
    let root_path = Path::root();
    let root = tree
        .get(&root_path)
        .expect("a valid tree has a root")
        .clone();
    let NodeBody::Dir { entries_generation } = root.body() else {
        panic!("the root is a directory");
    };
    tree.insert(
        root_path,
        Node::dir(
            root.id(),
            root.generation(),
            root.executable(),
            entries_generation.next().unwrap(),
        ),
    );
}

fn add_file(tree: &Tree, name: &str, file: File) -> Tree {
    let mut tree = tree.clone();
    advance_root_membership(&mut tree);
    tree.insert(
        Path::new(name).unwrap(),
        Node::file(NodeId::generate(), Generation::FIRST, false, file),
    );
    tree
}

#[tokio::test]
async fn persists_file_bytes_across_reopen_and_old_versions() {
    let backend = TestBackend::default();
    let fs = Fs::create(backend.operator()).await.unwrap();
    let bytes = (0..900_000).map(|i| (i % 251) as u8).collect::<Vec<_>>();
    let file = fs.write_file(&mut bytes.as_slice()).await.unwrap();
    let initial = fs.observe().await.unwrap();
    let id = CommitId::generate();
    let tree = add_file(initial.tree(), "large", file.clone());
    backend.fail_next_head_write_after_success();
    assert_eq!(
        fs.commit(&initial, id, tree.clone()).await.unwrap(),
        CommitOutcome::Committed { version: 1 }
    );
    assert_eq!(
        fs.commit(&initial, id, tree).await.unwrap(),
        CommitOutcome::Committed { version: 1 }
    );
    let reopened = Fs::open(backend.operator()).await.unwrap();
    let observed = reopened.observe().await.unwrap();
    let NodeBody::File(persisted) = observed
        .tree()
        .get(&Path::new("large").unwrap())
        .unwrap()
        .body()
    else {
        panic!()
    };
    let mut output = Vec::new();
    reopened.read_file(persisted, &mut output).await.unwrap();
    assert_eq!(output, bytes);
    let replacement = fs.write_file(&mut &b"new"[..]).await.unwrap();
    let old_node = observed.tree().get(&Path::new("large").unwrap()).unwrap();
    let mut next = observed.tree().clone();
    next.insert(
        Path::new("large").unwrap(),
        Node::file(
            old_node.id(),
            old_node.generation().next().unwrap(),
            false,
            replacement,
        ),
    );
    fs.commit(&observed, CommitId::generate(), next)
        .await
        .unwrap();
    output.clear();
    reopened.read_file(&file, &mut output).await.unwrap();
    assert_eq!(output, bytes);
}

#[tokio::test]
async fn reads_parts_offsets_and_checks_the_logical_digest() {
    let fs = Fs::create(TestBackend::default().operator()).await.unwrap();
    let source = fs.write_file(&mut &b"0123456789"[..]).await.unwrap();
    let blob = source.parts()[0].blob().clone();
    let parts = vec![
        FilePart::new(0..3, 6, blob.clone()).unwrap(),
        FilePart::new(3..5, 1, blob).unwrap(),
    ];
    let file = File::new(
        ContentId::new(blake3::hash(b"67812").into(), 5),
        parts.clone(),
    )
    .unwrap();
    let mut output = Vec::new();
    fs.read_file(&file, &mut output).await.unwrap();
    assert_eq!(output, b"67812");
    let wrong = File::new(ContentId::new([0; 32], 5), parts).unwrap();
    assert_eq!(
        fs.read_file(&wrong, &mut tokio::io::sink())
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Corrupt
    );
}

#[tokio::test]
async fn empty_files_need_no_data_objects() {
    let backend = TestBackend::default();
    let fs = Fs::create(backend.operator()).await.unwrap();
    let empty = fs.write_file(&mut &b""[..]).await.unwrap();
    assert!(empty.parts().is_empty());
    fs.read_file(&empty, &mut tokio::io::sink()).await.unwrap();
    assert!(
        !backend
            .state
            .lock()
            .unwrap()
            .objects
            .keys()
            .any(|path| path.starts_with(".yinyang/data/"))
    );
}

#[tokio::test]
async fn missing_or_corrupt_data_cannot_be_published() {
    for corruption in ["missing", "digest", "short", "long"] {
        let backend = TestBackend::default();
        let fs = Fs::create(backend.operator()).await.unwrap();
        let file = fs.write_file(&mut &b"content"[..]).await.unwrap();
        let path = std::str::from_utf8(file.parts()[0].blob().as_bytes()).unwrap();
        {
            let mut state = backend.state.lock().unwrap();
            if corruption == "missing" {
                state.objects.remove(path);
            } else {
                let bytes = &mut state.objects.get_mut(path).unwrap().bytes;
                match corruption {
                    "digest" => bytes[0] ^= 1,
                    "short" => {
                        bytes.pop();
                    }
                    _ => bytes.push(0),
                }
            }
        }
        let observed = fs.observe().await.unwrap();
        let error = fs
            .commit(
                &observed,
                CommitId::generate(),
                add_file(observed.tree(), "bad", file),
            )
            .await
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Corrupt, "{corruption}: {error}");
        assert_eq!(fs.observe().await.unwrap(), observed);
    }
}

#[tokio::test]
async fn failed_upload_aborts_and_never_changes_head() {
    let backend = TestBackend::default();
    let fs = Fs::create(backend.operator()).await.unwrap();
    let observed = fs.observe().await.unwrap();
    backend.state.lock().unwrap().fail_data_close = true;
    assert_eq!(
        fs.write_file(&mut &b"content"[..])
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Storage
    );
    assert_eq!(backend.state.lock().unwrap().data_aborts, 1);
    assert_eq!(fs.observe().await.unwrap(), observed);
}

#[tokio::test]
async fn invalid_data_locations_are_rejected() {
    let fs = Fs::create(TestBackend::default().operator()).await.unwrap();
    for path in [".yinyang/head", ".yinyang/data/../head", "external"] {
        let content = ContentId::new(blake3::hash(b"x").into(), 1);
        let file = File::new(
            content,
            vec![FilePart::new(0..1, 0, BlobRef::new(path, content)).unwrap()],
        )
        .unwrap();
        assert_eq!(
            fs.read_file(&file, &mut tokio::io::sink())
                .await
                .unwrap_err()
                .kind(),
            ErrorKind::Corrupt
        );
    }
}

#[tokio::test]
async fn verifies_unselected_blob_bytes_and_reports_destination_errors() {
    let backend = TestBackend::default();
    let fs = Fs::create(backend.operator()).await.unwrap();
    let uploaded = fs.write_file(&mut &b"abcdef"[..]).await.unwrap();
    let blob = uploaded.parts()[0].blob().clone();
    let file = File::new(
        ContentId::new(blake3::hash(b"bc").into(), 2),
        vec![FilePart::new(0..2, 1, blob.clone()).unwrap()],
    )
    .unwrap();
    let (mut sink, peer) = tokio::io::duplex(1);
    drop(peer);
    assert_eq!(
        fs.read_file(&file, &mut sink).await.unwrap_err().kind(),
        ErrorKind::Io
    );
    backend
        .state
        .lock()
        .unwrap()
        .objects
        .get_mut(std::str::from_utf8(blob.as_bytes()).unwrap())
        .unwrap()
        .bytes[5] ^= 1;
    assert_eq!(
        fs.read_file(&file, &mut tokio::io::sink())
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Corrupt
    );
}

#[tokio::test]
async fn source_failure_after_upload_starts_aborts_the_writer() {
    struct FailingSource(bool);
    impl tokio::io::AsyncRead for FailingSource {
        fn poll_read(
            mut self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
            buffer: &mut tokio::io::ReadBuf<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            if self.0 {
                return std::task::Poll::Ready(Err(std::io::Error::other("source failed")));
            }
            self.0 = true;
            buffer.put_slice(b"started");
            std::task::Poll::Ready(Ok(()))
        }
    }
    let backend = TestBackend::default();
    let fs = Fs::create(backend.operator()).await.unwrap();
    assert_eq!(
        fs.write_file(&mut FailingSource(false))
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Io
    );
    assert_eq!(backend.state.lock().unwrap().data_aborts, 1);
    assert_eq!(fs.observe().await.unwrap().version().number(), 0);
}

#[tokio::test]
#[ignore = "requires an isolated S3 bucket configured with YINYANG_S3_* variables"]
async fn s3_file_publication_and_reopen() {
    opendal::install_default();
    let config = std::env::vars()
        .filter_map(|(key, value)| {
            key.strip_prefix("YINYANG_S3_")
                .map(|key| (key.to_ascii_lowercase(), value))
        })
        .collect::<Vec<_>>();
    let operator = Operator::via_iter("s3", config).unwrap();
    let content = yinyang_core::data::DataStore::new(operator.clone(), NodeId::generate()).unwrap();
    let original = vec![37; 12 * 1024 * 1024 + 17];
    let prepared = content.prepare(&mut original.as_slice()).await.unwrap();
    let changed = content
        .overwrite_prepared(&prepared, 65534..65538, b"test")
        .await
        .unwrap();
    let mut range = Vec::new();
    content
        .read_range(changed.descriptor(), 65533..65539, &mut range)
        .await
        .unwrap();
    assert_eq!(range, b"%test%");
    let mut entire = Vec::new();
    content
        .read_range(prepared.descriptor(), 0..original.len() as u64, &mut entire)
        .await
        .unwrap();
    assert_eq!(entire, original);
    let fs = Fs::create(operator.clone()).await.unwrap();
    let bytes = vec![37; 12 * 1024 * 1024 + 17];
    let file = fs.write_file(&mut bytes.as_slice()).await.unwrap();
    let observed = fs.observe().await.unwrap();
    let name = format!("file-{}", uuid::Uuid::new_v4().simple());
    let tree = add_file(observed.tree(), &name, file);
    let id = CommitId::generate();
    assert!(matches!(
        fs.commit(&observed, id, tree.clone()).await.unwrap(),
        CommitOutcome::Committed { .. }
    ));
    assert!(matches!(
        fs.commit(&observed, id, tree).await.unwrap(),
        CommitOutcome::Committed { .. }
    ));
    assert!(matches!(
        fs.commit(
            &observed,
            CommitId::generate(),
            add_directory(observed.tree(), "loser", NodeId::generate())
        )
        .await
        .unwrap(),
        CommitOutcome::Conflict { .. }
    ));
    let reopened = Fs::open(operator).await.unwrap();
    let current = reopened.observe().await.unwrap();
    let NodeBody::File(file) = current
        .tree()
        .get(&Path::new(name).unwrap())
        .unwrap()
        .body()
    else {
        panic!()
    };
    let mut output = Vec::new();
    reopened.read_file(file, &mut output).await.unwrap();
    assert_eq!(output, bytes);
}

#[tokio::test]
async fn creates_and_reopens_one_filesystem() {
    let backend = TestBackend::default();
    let (left, right) = tokio::join!(
        Fs::create(backend.operator()),
        Fs::create(backend.operator())
    );
    let left = left.unwrap();
    let right = right.unwrap();

    assert_eq!(left.root(), right.root());
    let observed = Fs::open(backend.operator())
        .await
        .unwrap()
        .observe()
        .await
        .unwrap();
    assert_eq!(observed.version().number(), 0);
    assert_eq!(
        observed.tree().get(&Path::root()).unwrap().id(),
        left.root()
    );
}

#[tokio::test]
async fn streams_versions_to_opaque_object_keys() {
    let backend = TestBackend::default();
    let filesystem = Fs::create(backend.operator()).await.unwrap();
    let observed = filesystem.observe().await.unwrap();
    let mut successor = observed.tree().clone();
    advance_root_membership(&mut successor);
    for index in 0..10_000 {
        successor.insert(
            Path::new(format!("dir-{index:05}")).unwrap(),
            Node::dir(
                NodeId::generate(),
                Generation::FIRST,
                false,
                Generation::FIRST,
            ),
        );
    }
    backend.reset_version_write_calls();

    filesystem
        .commit(&observed, CommitId::generate(), successor)
        .await
        .unwrap();

    assert!(backend.version_write_calls() > 1);
    for (path, bytes) in backend.version_objects() {
        let key = path.strip_prefix(".yinyang/versions/").unwrap();
        assert_eq!(key.len(), 32);
        assert!(
            key.bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        );
        assert_ne!(key, blake3::hash(&bytes).to_hex().as_str());
    }
}

#[tokio::test]
async fn resolves_a_lost_head_creation_response() {
    let backend = TestBackend::default();
    backend.fail_next_head_write_after_success();

    let filesystem = Fs::create(backend.operator()).await.unwrap();

    assert_eq!(filesystem.observe().await.unwrap().version().number(), 0);
}

#[tokio::test]
async fn rejects_backends_without_conditional_head_replacement() {
    let operator = Operator::new(Memory::default()).unwrap();

    let error = Fs::create(operator).await.unwrap_err();

    assert_eq!(error.kind(), ErrorKind::Unsupported);
}

#[tokio::test]
async fn rejects_backends_without_streaming_write() {
    let backend = TestBackend::default();

    let error = Fs::create(backend.operator_without_streaming_write())
        .await
        .unwrap_err();

    assert_eq!(error.kind(), ErrorKind::Unsupported);
}

#[tokio::test]
async fn publishes_and_retries_one_commit() {
    let backend = TestBackend::default();
    let filesystem = Fs::create(backend.operator()).await.unwrap();
    let observed = filesystem.observe().await.unwrap();
    let commit = CommitId::generate();
    let successor = add_directory(observed.tree(), "dir", NodeId::generate());

    assert_eq!(
        filesystem
            .commit(&observed, commit, successor.clone())
            .await
            .unwrap(),
        CommitOutcome::Committed { version: 1 }
    );
    assert_eq!(
        filesystem
            .commit(&observed, commit, successor)
            .await
            .unwrap(),
        CommitOutcome::Committed { version: 1 }
    );

    let reopened = Fs::open(backend.operator()).await.unwrap();
    let current = reopened.observe().await.unwrap();
    assert!(current.tree().get(&Path::new("dir").unwrap()).is_some());
    assert_eq!(current.version().commits().len(), 1);
    assert_eq!(current.version().commits()[0], commit);
}

#[tokio::test]
async fn uses_the_etag_from_the_same_head_read() {
    let backend = TestBackend::default();
    let filesystem = Fs::create(backend.operator()).await.unwrap();
    let observed = filesystem.observe().await.unwrap();
    let successor = add_directory(observed.tree(), "dir", NodeId::generate());

    assert_eq!(
        filesystem
            .commit(&observed, CommitId::generate(), successor)
            .await
            .unwrap(),
        CommitOutcome::Committed { version: 1 }
    );
    assert_eq!(backend.stat_calls(), 0);
}

#[tokio::test]
async fn reports_conflict_for_competing_observations() {
    let filesystem = Fs::create(TestBackend::default().operator()).await.unwrap();
    let first = filesystem.observe().await.unwrap();
    let second = filesystem.observe().await.unwrap();

    let first_tree = add_directory(first.tree(), "first", NodeId::generate());
    let second_tree = add_directory(second.tree(), "second", NodeId::generate());
    assert!(matches!(
        filesystem
            .commit(&first, CommitId::generate(), first_tree)
            .await
            .unwrap(),
        CommitOutcome::Committed { .. }
    ));
    assert_eq!(
        filesystem
            .commit(&second, CommitId::generate(), second_tree)
            .await
            .unwrap(),
        CommitOutcome::Conflict { current: 1 }
    );
}

#[tokio::test]
async fn resolves_a_lost_publication_response() {
    let backend = TestBackend::default();
    let filesystem = Fs::create(backend.operator()).await.unwrap();
    let observed = filesystem.observe().await.unwrap();
    let successor = add_directory(observed.tree(), "durable", NodeId::generate());
    backend.fail_next_head_write_after_success();

    assert_eq!(
        filesystem
            .commit(&observed, CommitId::generate(), successor)
            .await
            .unwrap(),
        CommitOutcome::Committed { version: 1 }
    );
}

#[tokio::test]
async fn requires_directory_generation_for_membership_changes() {
    let filesystem = Fs::create(TestBackend::default().operator()).await.unwrap();
    let observed = filesystem.observe().await.unwrap();
    let mut invalid = observed.tree().clone();
    invalid.insert(
        Path::new("child").unwrap(),
        Node::dir(
            NodeId::generate(),
            Generation::FIRST,
            false,
            Generation::FIRST,
        ),
    );

    let error = filesystem
        .commit(&observed, CommitId::generate(), invalid)
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Invalid);
}

#[tokio::test]
async fn rejects_duplicate_nodes_and_missing_parents() {
    let filesystem = Fs::create(TestBackend::default().operator()).await.unwrap();
    let observed = filesystem.observe().await.unwrap();
    let node = NodeId::generate();
    let mut duplicate = add_directory(observed.tree(), "first", node);
    duplicate.insert(
        Path::new("second").unwrap(),
        Node::dir(node, Generation::FIRST, false, Generation::FIRST),
    );
    assert_eq!(
        filesystem
            .commit(&observed, CommitId::generate(), duplicate)
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Invalid
    );

    let mut missing_parent = observed.tree().clone();
    missing_parent.insert(
        Path::new("missing/child").unwrap(),
        Node::dir(
            NodeId::generate(),
            Generation::FIRST,
            false,
            Generation::FIRST,
        ),
    );
    assert_eq!(
        filesystem
            .commit(&observed, CommitId::generate(), missing_parent)
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Invalid
    );
}

#[tokio::test]
async fn rejects_case_folding_collisions_within_a_directory() {
    let filesystem = Fs::create(TestBackend::default().operator()).await.unwrap();
    let observed = filesystem.observe().await.unwrap();
    let mut successor = observed.tree().clone();
    advance_root_membership(&mut successor);
    successor.insert(
        Path::new("Readme").unwrap(),
        Node::dir(
            NodeId::generate(),
            Generation::FIRST,
            false,
            Generation::FIRST,
        ),
    );
    successor.insert(
        Path::new("README").unwrap(),
        Node::dir(
            NodeId::generate(),
            Generation::FIRST,
            false,
            Generation::FIRST,
        ),
    );

    let error = filesystem
        .commit(&observed, CommitId::generate(), successor)
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Invalid);
}

#[tokio::test]
async fn requires_node_generation_for_file_changes() {
    let filesystem = Fs::create(TestBackend::default().operator()).await.unwrap();
    let genesis = filesystem.observe().await.unwrap();
    let node = NodeId::generate();
    let mut first = genesis.tree().clone();
    advance_root_membership(&mut first);
    first.insert(
        Path::new("file").unwrap(),
        Node::file(
            node,
            Generation::FIRST,
            false,
            filesystem.write_file(&mut &b"original"[..]).await.unwrap(),
        ),
    );
    filesystem
        .commit(&genesis, CommitId::generate(), first)
        .await
        .unwrap();

    let observed = filesystem.observe().await.unwrap();
    let mut changed = observed.tree().clone();
    changed.insert(
        Path::new("file").unwrap(),
        Node::file(
            node,
            Generation::FIRST,
            false,
            File::new(ContentId::new([2; 32], 0), Vec::new()).unwrap(),
        ),
    );
    assert_eq!(
        filesystem
            .commit(&observed, CommitId::generate(), changed)
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Invalid
    );
}

#[tokio::test]
async fn preserves_node_generation_across_rename() {
    let filesystem = Fs::create(TestBackend::default().operator()).await.unwrap();
    let genesis = filesystem.observe().await.unwrap();
    let node_id = NodeId::generate();
    filesystem
        .commit(
            &genesis,
            CommitId::generate(),
            add_directory(genesis.tree(), "before", node_id),
        )
        .await
        .unwrap();
    let observed = filesystem.observe().await.unwrap();
    let mut renamed = observed.tree().clone();
    let node = renamed
        .remove(&Path::new("before").unwrap())
        .expect("the committed directory exists");
    renamed.insert(Path::new("after").unwrap(), node.clone());
    let root = renamed.get(&Path::root()).unwrap().clone();
    let NodeBody::Dir { entries_generation } = root.body() else {
        panic!("the root is a directory");
    };
    renamed.insert(
        Path::root(),
        Node::dir(
            root.id(),
            root.generation(),
            root.executable(),
            entries_generation.next().unwrap(),
        ),
    );

    filesystem
        .commit(&observed, CommitId::generate(), renamed)
        .await
        .unwrap();
    let current = filesystem.observe().await.unwrap();
    assert_eq!(
        current
            .tree()
            .get(&Path::new("after").unwrap())
            .unwrap()
            .generation(),
        Generation::FIRST
    );
}

#[test]
fn validates_file_part_coverage_and_blobs() {
    let blob = BlobRef::new(b"blob".to_vec(), ContentId::new([1; 32], 8));
    let first = FilePart::new(0..4, 0, blob.clone()).unwrap();
    let second = FilePart::new(5..8, 4, blob.clone()).unwrap();
    let content = ContentId::new([2; 32], 8);

    assert_eq!(
        File::new(content, vec![first, second]).unwrap_err().kind(),
        ErrorKind::Invalid
    );
    assert_eq!(
        FilePart::new(0..5, 4, blob).unwrap_err().kind(),
        ErrorKind::Invalid
    );
}

#[test]
fn accepts_only_canonical_portable_paths() {
    assert_eq!(Path::new("").unwrap(), Path::root());
    assert!(Path::new("dir/file").is_ok());
    for invalid in ["/rooted", "trailing/", "double//slash", ".", "CON", "bad?"] {
        assert_eq!(Path::new(invalid).unwrap_err().kind(), ErrorKind::Invalid);
    }
}

#[tokio::test]
async fn rejects_a_missing_referenced_version() {
    let backend = TestBackend::default();
    let filesystem = Fs::create(backend.operator()).await.unwrap();
    backend.remove_current_version();

    let error = filesystem.observe().await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Corrupt);
}

#[tokio::test]
async fn rejects_a_version_that_does_not_match_its_reference() {
    let backend = TestBackend::default();
    let filesystem = Fs::create(backend.operator()).await.unwrap();
    backend.corrupt_current_version();

    let error = filesystem.observe().await.unwrap_err();

    assert_eq!(error.kind(), ErrorKind::Corrupt);
}

#[tokio::test]
async fn rejects_a_corrupt_head() {
    let backend = TestBackend::default();
    Fs::create(backend.operator()).await.unwrap();
    backend.corrupt_head();

    let error = Fs::open(backend.operator()).await.unwrap_err();

    assert_eq!(error.kind(), ErrorKind::Corrupt);
}
