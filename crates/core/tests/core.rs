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

use opendal::Operator;
use opendal::services::Memory;
use yinyang_core::{
    BlobRef, CommitId, CommitOutcome, ContentId, ErrorKind, File, FilePart, Fs, Generation, Node,
    NodeBody, NodeId, Path, Tree,
};

mod support;
use support::TestBackend;

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
async fn publishes_checked_edits_and_detects_stale_batches() {
    let backend = TestBackend::default();
    let fs = Fs::create(backend.operator()).await.unwrap();
    let observed = fs.observe().await.unwrap();
    let mut edit = observed.edit();
    edit.create_dir(Path::new("dir").unwrap(), false).unwrap();
    let file = fs.write_file(&mut &b"first"[..]).await.unwrap();
    let id = edit
        .create_file(Path::new("dir/file").unwrap(), file.clone(), false)
        .unwrap();
    let tree = edit.finish().unwrap();
    let commit = CommitId::generate();
    fs.commit(&observed, commit, tree.clone()).await.unwrap();
    assert_eq!(
        fs.commit(&observed, commit, tree).await.unwrap(),
        CommitOutcome::Committed { version: 1 }
    );
    let mut stale = observed.edit();
    stale
        .create_dir(Path::new("stale").unwrap(), false)
        .unwrap();
    assert_eq!(
        fs.commit(&observed, CommitId::generate(), stale.finish().unwrap())
            .await
            .unwrap(),
        CommitOutcome::Conflict { current: 1 }
    );
    let current = fs.observe().await.unwrap();
    let mut edit = current.edit();
    edit.rename(&Path::new("dir/file").unwrap(), Path::new("moved").unwrap())
        .unwrap();
    edit.remove(&Path::new("dir").unwrap()).unwrap();
    edit.replace_file(
        &Path::new("moved").unwrap(),
        fs.write_file(&mut &b"second"[..]).await.unwrap(),
    )
    .unwrap();
    edit.set_executable(&Path::new("moved").unwrap(), true)
        .unwrap();
    fs.commit(&current, CommitId::generate(), edit.finish().unwrap())
        .await
        .unwrap();
    let current = Fs::open(backend.operator())
        .await
        .unwrap()
        .observe()
        .await
        .unwrap();
    let node = current.tree().get(&Path::new("moved").unwrap()).unwrap();
    assert_eq!(node.id(), id);
    assert_eq!(node.generation().value(), 2);
    assert!(node.executable());
    let mut old = Vec::new();
    fs.read_file(&file, &mut old).await.unwrap();
    assert_eq!(old, b"first");
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
