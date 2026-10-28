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
use std::path::Path;
use support::TestBackend;
use yinyang::core::{BackendProfile, CommitId, CommitOutcome, ErrorKind, Fs, Planner};
use yinyang::runtime::ErrorKind as RuntimeErrorKind;
use yinyang::{DirectoryJournal, DirectoryState};

async fn filesystem(backend: &TestBackend) -> Fs {
    Fs::create(backend.operator(), BackendProfile::Minio)
        .await
        .unwrap()
}
fn committed(outcome: CommitOutcome) -> yinyang::core::Receipt {
    match outcome {
        CommitOutcome::Committed(r) => r,
        v => panic!("{v:?}"),
    }
}
async fn source(root: &Path) {
    tokio::fs::create_dir(root).await.unwrap();
    tokio::fs::create_dir(root.join("empty-dir")).await.unwrap();
    tokio::fs::create_dir(root.join("nested")).await.unwrap();
    tokio::fs::write(root.join("empty"), []).await.unwrap();
    tokio::fs::write(root.join("nested/hello"), b"hello")
        .await
        .unwrap();
    tokio::fs::write(root.join("large"), vec![91; 700_000])
        .await
        .unwrap();
}

#[tokio::test]
async fn round_trip_replacement_retry_and_pinned_restore() {
    let temp = tempfile::tempdir().unwrap();
    let input = temp.path().join("source");
    source(&input).await;
    let backend = TestBackend::default();
    let fs = filesystem(&backend).await;
    let first = fs.observe_latest().await.unwrap();
    let request = yinyang::prepare_directory(&fs, &first, &input, CommitId::generate())
        .await
        .unwrap();
    let receipt = committed(fs.commit(&request).await.unwrap());
    let pinned = fs.observe_latest().await.unwrap();
    let first_node = pinned.resolve("nested/hello").await.unwrap().unwrap();
    let before = backend.state.lock().unwrap().objects.len();
    let same = yinyang::prepare_directory(&fs, &pinned, &input, CommitId::generate())
        .await
        .unwrap();
    assert_eq!(
        backend.state.lock().unwrap().objects.len(),
        before,
        "unchanged content must not upload"
    );
    committed(fs.commit(&same).await.unwrap());
    assert_eq!(
        yinyang::directory_nodes(&fs.observe_latest().await.unwrap())
            .await
            .unwrap(),
        yinyang::directory_nodes(&pinned).await.unwrap()
    );
    tokio::fs::write(input.join("nested/hello"), b"updated")
        .await
        .unwrap();
    tokio::fs::remove_file(input.join("empty")).await.unwrap();
    tokio::fs::remove_dir(input.join("empty-dir"))
        .await
        .unwrap();
    tokio::fs::create_dir(input.join("empty")).await.unwrap();
    tokio::fs::write(input.join("empty/new"), b"new")
        .await
        .unwrap();
    committed(
        yinyang::publish_directory(
            &fs,
            &fs.observe_latest().await.unwrap(),
            &input,
            CommitId::generate(),
        )
        .await
        .unwrap(),
    );
    let latest = fs.observe_latest().await.unwrap();
    let next_node = latest.resolve("nested/hello").await.unwrap().unwrap();
    assert_eq!(next_node.id(), first_node.id());
    assert_eq!(next_node.generation(), first_node.generation() + 1);
    assert!(latest.resolve("empty-dir").await.unwrap().is_none());
    assert_eq!(
        committed(fs.commit(&request).await.unwrap()),
        receipt,
        "retry original plan after local mutation"
    );
    let reopened = Fs::open(backend.operator(), BackendProfile::Minio)
        .await
        .unwrap();
    let old_output = temp.path().join("old");
    yinyang::restore_directory_revision(&reopened, pinned.revision(), &old_output)
        .await
        .unwrap();
    assert_eq!(
        tokio::fs::read(old_output.join("nested/hello"))
            .await
            .unwrap(),
        b"hello"
    );
    let output = temp.path().join("restored");
    yinyang::restore_directory(&reopened, &latest, &output)
        .await
        .unwrap();
    assert_eq!(
        tokio::fs::read(output.join("nested/hello")).await.unwrap(),
        b"updated"
    );
    assert_eq!(
        tokio::fs::read(output.join("large")).await.unwrap(),
        vec![91; 700_000]
    );
    assert_eq!(
        tokio::fs::read(output.join("empty/new")).await.unwrap(),
        b"new"
    );
}

#[tokio::test]
async fn failed_upload_and_stale_scope_preserve_remote_state() {
    let temp = tempfile::tempdir().unwrap();
    let input = temp.path().join("source");
    source(&input).await;
    let backend = TestBackend::default();
    let fs = filesystem(&backend).await;
    let first = fs.observe_latest().await.unwrap();
    backend.state.lock().unwrap().fail_data_close = true;
    assert_eq!(
        yinyang::prepare_directory(&fs, &first, &input, CommitId::generate())
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Storage
    );
    assert_eq!(
        fs.observe_latest().await.unwrap().revision(),
        first.revision()
    );
    backend.state.lock().unwrap().fail_data_close = false;
    let request = yinyang::prepare_directory(&fs, &first, &input, CommitId::generate())
        .await
        .unwrap();
    let mut winner = Planner::new(&first, CommitId::generate());
    winner.create_directory(fs.root(), "winner").await.unwrap();
    committed(fs.commit(&winner.finish().unwrap()).await.unwrap());
    assert_eq!(fs.commit(&request).await.unwrap(), CommitOutcome::Conflict);
    assert!(
        fs.observe_latest()
            .await
            .unwrap()
            .resolve("winner")
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn source_mutation_during_upload_prevents_publication() {
    let temp = tempfile::tempdir().unwrap();
    let source = temp.path().join("file");
    std::fs::write(&source, b"original").unwrap();
    let backend = TestBackend::default();
    let fs = filesystem(&backend).await;
    let observed = fs.observe_latest().await.unwrap();
    backend.state.lock().unwrap().file_to_change_on_data_close = Some(source);
    let error = yinyang::publish_directory(&fs, &observed, temp.path(), CommitId::generate())
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Invalid);
    assert!(error.message().contains("source changed"));
    assert_eq!(
        fs.observe_latest().await.unwrap().revision(),
        observed.revision()
    );
}

#[tokio::test]
async fn rejects_foreign_observations_before_local_io() {
    let first = filesystem(&TestBackend::default()).await;
    let second = filesystem(&TestBackend::default()).await;
    let observed = first.observe_latest().await.unwrap();
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("absent");
    assert_eq!(
        yinyang::restore_directory(&second, &observed, &path)
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Invalid
    );
    assert!(!path.exists());
    assert_eq!(
        yinyang::publish_directory(&second, &observed, &path, CommitId::generate())
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Invalid
    );
}

#[tokio::test]
async fn restore_never_overwrites_or_installs_corrupt_content() {
    let temp = tempfile::tempdir().unwrap();
    let input = temp.path().join("source");
    tokio::fs::create_dir(&input).await.unwrap();
    tokio::fs::write(input.join("file"), b"original")
        .await
        .unwrap();
    let backend = TestBackend::default();
    let fs = filesystem(&backend).await;
    committed(
        yinyang::publish_directory(
            &fs,
            &fs.observe_latest().await.unwrap(),
            &input,
            CommitId::generate(),
        )
        .await
        .unwrap(),
    );
    let observed = fs.observe_latest().await.unwrap();
    assert_eq!(
        yinyang::restore_directory(&fs, &observed, &input)
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::AlreadyExists
    );
    assert_eq!(
        tokio::fs::read(input.join("file")).await.unwrap(),
        b"original"
    );
    {
        let mut state = backend.state.lock().unwrap();
        let object = state
            .objects
            .values_mut()
            .find(|v| v.bytes.starts_with(b"original"))
            .unwrap();
        object.bytes[0] ^= 1;
    }
    let output = temp.path().join("failed");
    assert_eq!(
        yinyang::restore_directory(&fs, &observed, &output)
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Corrupt
    );
    assert!(!output.join("file").exists());
    assert_eq!(std::fs::read_dir(output).unwrap().count(), 0);
}

#[cfg(unix)]
#[tokio::test]
async fn preserves_executable_files_and_rejects_symlinks() {
    use std::os::unix::fs::{PermissionsExt as _, symlink};
    let temp = tempfile::tempdir().unwrap();
    let input = temp.path().join("source");
    source(&input).await;
    std::fs::set_permissions(
        input.join("nested/hello"),
        std::fs::Permissions::from_mode(0o755),
    )
    .unwrap();
    let fs = filesystem(&TestBackend::default()).await;
    committed(
        yinyang::publish_directory(
            &fs,
            &fs.observe_latest().await.unwrap(),
            &input,
            CommitId::generate(),
        )
        .await
        .unwrap(),
    );
    let observed = fs.observe_latest().await.unwrap();
    let output = temp.path().join("restored");
    yinyang::restore_directory(&fs, &observed, &output)
        .await
        .unwrap();
    assert_ne!(
        std::fs::metadata(output.join("nested/hello"))
            .unwrap()
            .permissions()
            .mode()
            & 0o111,
        0
    );
    symlink(&input, temp.path().join("linked")).unwrap();
    assert_eq!(
        yinyang::publish_directory(
            &fs,
            &observed,
            &temp.path().join("linked"),
            CommitId::generate()
        )
        .await
        .unwrap_err()
        .kind(),
        ErrorKind::Unsupported
    );
    symlink(input.join("large"), input.join("link")).unwrap();
    assert_eq!(
        yinyang::publish_directory(&fs, &observed, &input, CommitId::generate())
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Unsupported
    );
    assert_eq!(
        fs.observe_latest().await.unwrap().revision(),
        observed.revision()
    );
}

#[cfg(unix)]
#[tokio::test]
async fn invalid_names_fail_before_upload() {
    let temp = tempfile::tempdir().unwrap();
    std::fs::write(temp.path().join("CON"), b"not portable").unwrap();
    let backend = TestBackend::default();
    let fs = filesystem(&backend).await;
    let observed = fs.observe_latest().await.unwrap();
    let count = backend.state.lock().unwrap().objects.len();
    assert_eq!(
        yinyang::prepare_directory(&fs, &observed, temp.path(), CommitId::generate())
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidName
    );
    assert_eq!(backend.state.lock().unwrap().objects.len(), count);
}

#[cfg(unix)]
#[tokio::test]
async fn journal_reopens_exact_intent_and_exports_without_the_source() {
    use std::os::unix::fs::PermissionsExt as _;

    let temp = tempfile::tempdir().unwrap();
    let input = temp.path().join("source");
    source(&input).await;
    tokio::fs::set_permissions(
        input.join("nested/hello"),
        std::fs::Permissions::from_mode(0o755),
    )
    .await
    .unwrap();
    let backend = TestBackend::default();
    let fs = filesystem(&backend).await;
    let state = temp.path().join("journal");
    let journal = DirectoryJournal::open(&fs, &state).await.unwrap();
    let foreign = filesystem(&TestBackend::default()).await;
    assert_eq!(
        DirectoryJournal::open(&foreign, &state)
            .await
            .unwrap_err()
            .kind(),
        RuntimeErrorKind::Invalid
    );
    let id = CommitId::generate();
    let observed = fs.observe_latest().await.unwrap();
    let prepared = journal.prepare(&observed, &input, id).await.unwrap();
    assert_eq!(prepared.state, DirectoryState::Prepared);
    assert!(prepared.can_retry());
    assert!(prepared.can_export());

    let moved = temp.path().join("moved");
    tokio::fs::rename(&input, &moved).await.unwrap();
    assert_eq!(
        journal
            .prepare(&observed, &input, id)
            .await
            .unwrap_err()
            .kind(),
        RuntimeErrorKind::AlreadyExists,
        "an existing identity must be rejected before scanning the missing source"
    );
    backend.state.lock().unwrap().offline = true;
    let inspection = DirectoryJournal::inspect(&state).await.unwrap();
    let inspected = inspection.status(id).unwrap();
    assert_eq!(inspected.state, DirectoryState::Prepared);
    assert_eq!(
        inspected.intent.request_digest,
        prepared.intent.request_digest
    );
    backend.state.lock().unwrap().offline = false;

    drop(journal);
    drop(fs);
    let reopened = Fs::open(backend.operator(), BackendProfile::Minio)
        .await
        .unwrap();
    let journal = DirectoryJournal::open(&reopened, &state).await.unwrap();
    let receipt = journal.publish(id).await.unwrap();
    assert_eq!(receipt.request_digest, prepared.intent.request_digest);
    let exported = temp.path().join("exported");
    journal.export(id, &exported).await.unwrap();
    assert_eq!(
        tokio::fs::read(exported.join("nested/hello"))
            .await
            .unwrap(),
        b"hello"
    );
    assert_eq!(tokio::fs::read(exported.join("empty")).await.unwrap(), b"");
    assert!(exported.join("empty-dir").is_dir());
    assert_ne!(
        std::fs::metadata(exported.join("nested/hello"))
            .unwrap()
            .permissions()
            .mode()
            & 0o111,
        0
    );
    assert_eq!(
        journal.export(id, &exported).await.unwrap_err().kind(),
        RuntimeErrorKind::AlreadyExists
    );
    assert_eq!(
        tokio::fs::read(exported.join("nested/hello"))
            .await
            .unwrap(),
        b"hello"
    );

    let second = CommitId::generate();
    let observed = reopened.observe_latest().await.unwrap();
    let prepared = journal.prepare(&observed, &moved, second).await.unwrap();
    tokio::fs::write(moved.join("nested/hello"), b"changed after second prepare")
        .await
        .unwrap();
    tokio::fs::remove_file(moved.join("empty")).await.unwrap();
    tokio::fs::remove_dir(moved.join("empty-dir"))
        .await
        .unwrap();
    let receipt = journal.publish(second).await.unwrap();
    assert_eq!(receipt.request_digest, prepared.intent.request_digest);
    let second_export = temp.path().join("second-export");
    journal.export(second, &second_export).await.unwrap();
    assert_eq!(
        tokio::fs::read(second_export.join("nested/hello"))
            .await
            .unwrap(),
        b"hello",
        "unchanged content and the accepted manifest must survive source mutation"
    );
    assert_eq!(
        tokio::fs::read(second_export.join("large")).await.unwrap(),
        vec![91; 700_000]
    );
    assert!(second_export.join("empty-dir").is_dir());
    assert_eq!(
        tokio::fs::read(second_export.join("empty")).await.unwrap(),
        b""
    );
}

#[tokio::test]
async fn journal_unknown_survives_offline_retry_and_resolves_the_original_receipt() {
    let temp = tempfile::tempdir().unwrap();
    let input = temp.path().join("source");
    source(&input).await;
    let backend = TestBackend::default();
    let fs = filesystem(&backend).await;
    let state = temp.path().join("journal");
    let journal = DirectoryJournal::open(&fs, &state).await.unwrap();
    let id = CommitId::generate();
    let observed = fs.observe_latest().await.unwrap();
    let prepared = journal.prepare(&observed, &input, id).await.unwrap();
    tokio::fs::write(input.join("nested/hello"), b"changed after prepare")
        .await
        .unwrap();

    backend.lose_next_head_response_and_go_offline();
    let error = journal.publish(id).await.unwrap_err();
    assert_eq!(error.kind(), RuntimeErrorKind::Unknown);
    assert_eq!(error.commit_id(), Some(id));
    assert_eq!(
        journal.status(id).await.unwrap().unwrap().state,
        DirectoryState::Unknown
    );

    let error = journal.publish(id).await.unwrap_err();
    assert_eq!(error.kind(), RuntimeErrorKind::Unknown);
    let inspection = DirectoryJournal::inspect(&state).await.unwrap();
    assert_eq!(
        inspection.status(id).unwrap().state,
        DirectoryState::Unknown
    );

    backend.state.lock().unwrap().offline = false;
    drop(journal);
    drop(fs);
    let reopened = Fs::open(backend.operator(), BackendProfile::Minio)
        .await
        .unwrap();
    let journal = DirectoryJournal::open(&reopened, &state).await.unwrap();
    let receipt = journal.publish(id).await.unwrap();
    assert_eq!(receipt.commit_id, id);
    assert_eq!(receipt.request_digest, prepared.intent.request_digest);
    let status = journal.status(id).await.unwrap().unwrap();
    assert_eq!(status.state, DirectoryState::Committed(receipt.clone()));
    assert!(status.failure.is_none());
    assert!(
        !status.can_retry(),
        "committed work is resolved rather than retryable"
    );
    assert!(
        reopened
            .observe_revision(prepared.intent.baseline)
            .await
            .is_ok()
    );
    let restored = temp.path().join("restored");
    yinyang::restore_directory_revision(&reopened, prepared.intent.baseline, &restored)
        .await
        .unwrap();
    assert!(restored.is_dir());
    let output = temp.path().join("published");
    yinyang::restore_directory_revision(&reopened, receipt.cursor.revision, &output)
        .await
        .unwrap();
    assert_eq!(
        tokio::fs::read(output.join("nested/hello")).await.unwrap(),
        b"hello",
        "retry must publish the original prepared bytes, not the changed source"
    );
}

#[tokio::test]
async fn journal_local_confirmation_failure_remains_typed_unknown() {
    let temp = tempfile::tempdir().unwrap();
    let input = temp.path().join("source");
    source(&input).await;
    let backend = TestBackend::default();
    let fs = filesystem(&backend).await;
    let state = temp.path().join("journal");
    let journal = DirectoryJournal::open(&fs, &state).await.unwrap();
    let id = CommitId::generate();
    let observed = fs.observe_latest().await.unwrap();
    journal.prepare(&observed, &input, id).await.unwrap();

    let database = rusqlite::Connection::open(state.join("directory.db")).unwrap();
    database
        .execute_batch(
            "CREATE TRIGGER fail_local_confirmation
             BEFORE UPDATE OF state ON publications
             WHEN NEW.state=2
             BEGIN
                 SELECT RAISE(ABORT, 'confirmation unavailable');
             END;",
        )
        .unwrap();
    let error = journal.publish(id).await.unwrap_err();
    assert_eq!(error.kind(), RuntimeErrorKind::Unknown);
    assert_eq!(error.commit_id(), Some(id));
    assert_eq!(
        journal.status(id).await.unwrap().unwrap().state,
        DirectoryState::Unknown
    );

    database
        .execute_batch("DROP TRIGGER fail_local_confirmation;")
        .unwrap();
    let receipt = journal.publish(id).await.unwrap();
    assert_eq!(receipt.commit_id, id);
    assert_eq!(
        journal.status(id).await.unwrap().unwrap().state,
        DirectoryState::Committed(receipt)
    );
}

#[tokio::test]
async fn separately_opened_journals_replay_one_exact_request_safely() {
    let temp = tempfile::tempdir().unwrap();
    let input = temp.path().join("source");
    source(&input).await;
    let backend = TestBackend::default();
    let fs = filesystem(&backend).await;
    let state = temp.path().join("journal");
    let first = DirectoryJournal::open(&fs, &state).await.unwrap();
    let second = DirectoryJournal::open(&fs, &state).await.unwrap();
    let id = CommitId::generate();
    let observed = fs.observe_latest().await.unwrap();
    first.prepare(&observed, &input, id).await.unwrap();

    let (left, right) = tokio::join!(first.publish(id), second.publish(id));
    let left = left.unwrap();
    let right = right.unwrap();
    assert_eq!(left, right);
    assert_eq!(
        first.status(id).await.unwrap().unwrap().state,
        DirectoryState::Committed(left.clone())
    );
    let output = temp.path().join("published");
    yinyang::restore_directory_revision(&fs, left.cursor.revision, &output)
        .await
        .unwrap();
    assert_eq!(
        tokio::fs::read(output.join("nested/hello")).await.unwrap(),
        b"hello"
    );
}

#[tokio::test]
async fn journal_rejects_a_future_profile_before_changing_its_schema() {
    let temp = tempfile::tempdir().unwrap();
    let state = temp.path().join("journal");
    tokio::fs::create_dir(&state).await.unwrap();
    {
        let database = rusqlite::Connection::open(state.join("directory.db")).unwrap();
        database
            .execute_batch(
                "CREATE TABLE binding(
                     singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                     profile TEXT NOT NULL,
                     filesystem BLOB NOT NULL,
                     root BLOB NOT NULL
                 );
                 INSERT INTO binding VALUES(1,'yinyang-directory-99',zeroblob(16),zeroblob(16));",
            )
            .unwrap();
    }
    let fs = filesystem(&TestBackend::default()).await;
    assert_eq!(
        DirectoryJournal::open(&fs, &state)
            .await
            .unwrap_err()
            .kind(),
        RuntimeErrorKind::Unsupported
    );
    assert_eq!(
        DirectoryJournal::inspect(&state).await.unwrap_err().kind(),
        RuntimeErrorKind::Unsupported
    );
    let database = rusqlite::Connection::open(state.join("directory.db")).unwrap();
    let mode: String = database
        .query_row("PRAGMA journal_mode", [], |row| row.get(0))
        .unwrap();
    assert_eq!(mode, "delete");
    let publications: bool = database
        .query_row(
            "SELECT EXISTS(
                SELECT 1 FROM sqlite_master WHERE type='table' AND name='publications'
             )",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(
        !publications,
        "rejecting a future profile must not initialize the v1 schema"
    );
}

#[tokio::test]
async fn journal_rejects_a_partial_known_schema_without_repairing_it() {
    let temp = tempfile::tempdir().unwrap();
    let state = temp.path().join("journal");
    tokio::fs::create_dir(&state).await.unwrap();
    {
        let database = rusqlite::Connection::open(state.join("directory.db")).unwrap();
        database
            .execute_batch(
                "CREATE TABLE binding(
                     singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                     profile TEXT NOT NULL,
                     filesystem BLOB NOT NULL,
                     root BLOB NOT NULL
                 );
                 INSERT INTO binding VALUES(1,'yinyang-directory-1',zeroblob(16),zeroblob(16));
                 CREATE TABLE publications(
                     commit_id BLOB PRIMARY KEY,
                     baseline BLOB NOT NULL,
                     request_digest BLOB NOT NULL,
                     source TEXT NOT NULL,
                     request BLOB NOT NULL,
                     manifest BLOB NOT NULL,
                     state INTEGER NOT NULL,
                     receipt_revision BLOB,
                     receipt_ordinal INTEGER
                 );",
            )
            .unwrap();
    }
    let fs = filesystem(&TestBackend::default()).await;
    assert_eq!(
        DirectoryJournal::open(&fs, &state)
            .await
            .unwrap_err()
            .kind(),
        RuntimeErrorKind::Corrupt
    );
    assert_eq!(
        DirectoryJournal::inspect(&state).await.unwrap_err().kind(),
        RuntimeErrorKind::Corrupt
    );
    let database = rusqlite::Connection::open(state.join("directory.db")).unwrap();
    let columns: Vec<String> = {
        let mut statement = database.prepare("PRAGMA table_info(publications)").unwrap();
        statement
            .query_map([], |row| row.get(1))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap()
    };
    assert!(!columns.iter().any(|column| column == "failure"));
}

#[tokio::test]
async fn journal_inspection_rejects_a_malformed_frozen_identity() {
    let temp = tempfile::tempdir().unwrap();
    let input = temp.path().join("source");
    source(&input).await;
    let fs = filesystem(&TestBackend::default()).await;
    let state = temp.path().join("journal");
    let journal = DirectoryJournal::open(&fs, &state).await.unwrap();
    let id = CommitId::generate();
    let observed = fs.observe_latest().await.unwrap();
    journal.prepare(&observed, &input, id).await.unwrap();
    rusqlite::Connection::open(state.join("directory.db"))
        .unwrap()
        .execute(
            "UPDATE publications SET request=?2 WHERE commit_id=?1",
            rusqlite::params![id.as_bytes().as_slice(), b"malformed"],
        )
        .unwrap();
    assert_eq!(
        journal.status(id).await.unwrap_err().kind(),
        RuntimeErrorKind::Corrupt
    );
    assert_eq!(
        DirectoryJournal::inspect(&state).await.unwrap_err().kind(),
        RuntimeErrorKind::Corrupt
    );
}

#[cfg(unix)]
#[tokio::test]
async fn journal_rejects_canonical_source_containing_state_before_publication() {
    use std::os::unix::fs::symlink;

    for (layout, equal) in [("inside", false), ("equal", true)] {
        let temp = tempfile::tempdir().unwrap();
        let input = temp.path().join("source");
        source(&input).await;
        let alias = temp.path().join("source-alias");
        symlink(&input, &alias).unwrap();
        let state = if equal {
            input.clone()
        } else {
            input.join(".state")
        };
        let backend = TestBackend::default();
        let fs = filesystem(&backend).await;
        let observed = fs.observe_latest().await.unwrap();
        let journal = DirectoryJournal::open(&fs, &state).await.unwrap();
        for (source_kind, source_path) in [("direct", &input), ("alias", &alias)] {
            let before = backend.state.lock().unwrap().objects.len();
            let id = CommitId::generate();
            let error = journal
                .prepare(&observed, source_path, id)
                .await
                .unwrap_err();
            assert_eq!(
                error.kind(),
                RuntimeErrorKind::Invalid,
                "{layout} {source_kind}: {error}"
            );
            assert!(
                error.to_string().contains("is inside source"),
                "{layout} {source_kind} did not fail the containment check: {error}"
            );
            assert!(journal.status(id).await.unwrap().is_none());
            assert_eq!(
                backend.state.lock().unwrap().objects.len(),
                before,
                "{layout} {source_kind} uploaded or published directory content"
            );
        }

        // Preserve the scanner's existing symlink-root rejection when the
        // journal itself does not trigger the containment guard.
        let outside = temp.path().join("outside-state");
        let outside_journal = DirectoryJournal::open(&fs, &outside).await.unwrap();
        let before = backend.state.lock().unwrap().objects.len();
        let id = CommitId::generate();
        let error = outside_journal
            .prepare(&observed, &alias, id)
            .await
            .unwrap_err();
        assert_eq!(error.kind(), RuntimeErrorKind::Unsupported);
        assert!(outside_journal.status(id).await.unwrap().is_none());
        assert_eq!(backend.state.lock().unwrap().objects.len(), before);
    }
}

#[tokio::test]
async fn journal_allows_a_source_below_a_larger_state_directory() {
    let temp = tempfile::tempdir().unwrap();
    let state = temp.path().join("state");
    let input = state.join("input");
    tokio::fs::create_dir_all(&input).await.unwrap();
    tokio::fs::write(input.join("file"), b"content")
        .await
        .unwrap();
    let fs = filesystem(&TestBackend::default()).await;
    let observed = fs.observe_latest().await.unwrap();
    let journal = DirectoryJournal::open(&fs, &state).await.unwrap();
    let id = CommitId::generate();
    let status = journal.prepare(&observed, &input, id).await.unwrap();
    assert_eq!(status.state, DirectoryState::Prepared);
}

#[test]
fn cli_help_lists_commands() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_yy"))
        .arg("--help")
        .output()
        .unwrap();
    assert!(output.status.success());
    let text = String::from_utf8(output.stdout).unwrap();
    for name in [
        "create",
        "publish",
        "restore",
        "status",
        "receipt",
        "publication",
    ] {
        assert!(text.contains(name));
    }
    let restore = std::process::Command::new(env!("CARGO_BIN_EXE_yy"))
        .args(["restore", "--help"])
        .output()
        .unwrap();
    assert!(restore.status.success());
    assert!(
        String::from_utf8(restore.stdout)
            .unwrap()
            .contains("--revision <REVISION>")
    );
}

#[test]
fn revision_syntax_is_rejected_before_backend_admission() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_yy"))
        .args(["status", "--revision", "00"])
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("revision must be exactly 48 hexadecimal characters"),
        "{stderr}"
    );
}

#[test]
fn volume_status_rejects_historical_revision_selection() {
    let temp = tempfile::tempdir().unwrap();
    let config = temp.path().join("volume.json");
    std::fs::write(
        &config,
        serde_json::to_vec(&serde_json::json!({
            "name": "revision-admission",
            "model": "managed",
            "access": "mount",
            "frontend": "library",
            "publication": "object",
            "storage_profile": "minio",
            "staging": temp.path().join("state"),
            "read_only": false,
            "require": [],
        }))
        .unwrap(),
    )
    .unwrap();
    let revision = "01".repeat(24);
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_yy"))
        .args([
            "--volume",
            config.to_str().unwrap(),
            "status",
            "--revision",
            &revision,
        ])
        .output()
        .unwrap();
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("--revision is only supported by object-storage `yy status`"),
        "{stderr}"
    );
}

#[test]
#[ignore = "requires an isolated S3 bucket configured with YINYANG_S3_* variables"]
fn s3_cli_directory_round_trip() {
    let temp = tempfile::tempdir().unwrap();
    let input = temp.path().join("source");
    std::fs::create_dir_all(input.join("nested/empty-dir")).unwrap();
    std::fs::write(input.join("nested/hello"), b"hello").unwrap();
    std::fs::write(input.join("empty"), []).unwrap();
    let bytes = vec![43; 12 * 1024 * 1024 + 17];
    std::fs::write(input.join("large"), &bytes).unwrap();
    let root = format!("cli-{}", uuid::Uuid::new_v4().simple());
    let run = |args: &[&str]| {
        std::process::Command::new(env!("CARGO_BIN_EXE_yy"))
            .args(args)
            .env("YINYANG_S3_ROOT", &root)
            .env("YINYANG_STORAGE_PROFILE", "minio")
            .output()
            .unwrap()
    };
    let success = |args: &[&str]| {
        let output = run(args);
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).unwrap()
    };
    success(&["create"]);
    let id = uuid::Uuid::new_v4().to_string();
    success(&["publish", input.to_str().unwrap(), "--commit-id", &id]);
    // Querying the original receipt does not rescan or republish local files.
    assert!(success(&["receipt", &id]).contains(&id));
    assert!(
        !run(&["publish", input.to_str().unwrap(), "--commit-id", &id])
            .status
            .success()
    );
    let first_status = success(&["status"]);
    assert!(first_status.contains("files: 3"));
    let first_revision = first_status
        .lines()
        .find_map(|line| line.strip_prefix("revision: "))
        .unwrap();
    assert_eq!(first_revision.len(), 48);
    assert!(!run(&["publish", input.to_str().unwrap()]).status.success());
    let output = temp.path().join("restored");
    success(&["restore", output.to_str().unwrap()]);
    assert_eq!(std::fs::read(output.join("large")).unwrap(), bytes);
    assert_eq!(
        std::fs::read(output.join("nested/hello")).unwrap(),
        b"hello"
    );
    assert!(output.join("nested/empty-dir").is_dir());
    assert_eq!(std::fs::read(output.join("empty")).unwrap(), b"");
    assert!(!run(&["restore", output.to_str().unwrap()]).status.success());
    std::fs::write(input.join("nested/hello"), b"updated").unwrap();
    std::fs::remove_file(input.join("empty")).unwrap();
    success(&["publish", input.to_str().unwrap(), "--replace"]);
    let second = temp.path().join("second");
    success(&["restore", second.to_str().unwrap()]);
    assert_eq!(
        std::fs::read(second.join("nested/hello")).unwrap(),
        b"updated"
    );
    assert!(!second.join("empty").exists());
    assert!(success(&["status"]).contains("files: 2"));
    let historical = temp.path().join("historical");
    success(&[
        "restore",
        historical.to_str().unwrap(),
        "--revision",
        first_revision,
    ]);
    assert_eq!(
        std::fs::read(historical.join("nested/hello")).unwrap(),
        b"hello"
    );
    assert_eq!(std::fs::read(historical.join("large")).unwrap(), bytes);
    assert!(historical.join("nested/empty-dir").is_dir());
    assert_eq!(std::fs::read(historical.join("empty")).unwrap(), b"");
    let invalid = temp.path().join("invalid-revision");
    assert!(
        !run(&["restore", invalid.to_str().unwrap(), "--revision", "00",])
            .status
            .success()
    );
    assert!(!invalid.exists());
}
