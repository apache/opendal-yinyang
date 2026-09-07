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
        Ok(TestReader { object })
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
        let object = StoredObject {
            bytes: self.bytes.clone(),
            etag: format!("\"{}\"", state.next_revision),
        };
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
        self.bytes.clear();
        Ok(())
    }
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
            File::new(ContentId::new([1; 32], 0), Vec::new()).unwrap(),
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
