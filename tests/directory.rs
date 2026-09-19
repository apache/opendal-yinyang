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
use yinyang::core::{CommitId, CommitOutcome, ErrorKind, Fs, NodeBody, Path as FsPath};

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
    let fs = Fs::create(backend.operator()).await.unwrap();
    let first = fs.observe().await.unwrap();
    let id = CommitId::generate();
    assert_eq!(
        yinyang::publish_directory(&fs, &first, &input, id)
            .await
            .unwrap(),
        CommitOutcome::Committed { version: 1 }
    );
    let pinned = fs.observe().await.unwrap();
    let first_node = pinned
        .tree()
        .get(&FsPath::new("nested/hello").unwrap())
        .unwrap();
    let same_id = CommitId::generate();
    yinyang::publish_directory(&fs, &pinned, &input, same_id)
        .await
        .unwrap();
    assert_eq!(fs.observe().await.unwrap().tree(), pinned.tree());
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
    let before = fs.observe().await.unwrap();
    let commit = CommitId::generate();
    yinyang::publish_directory(&fs, &before, &input, commit)
        .await
        .unwrap();
    let latest = fs.observe().await.unwrap();
    let next_node = latest
        .tree()
        .get(&FsPath::new("nested/hello").unwrap())
        .unwrap();
    assert_eq!(next_node.id(), first_node.id());
    assert_eq!(
        next_node.generation().value(),
        first_node.generation().value() + 1
    );
    assert!(
        latest
            .tree()
            .get(&FsPath::new("empty-dir").unwrap())
            .is_none()
    );
    assert_eq!(
        yinyang::publish_directory(&fs, &latest, &temp.path().join("missing"), commit)
            .await
            .unwrap(),
        CommitOutcome::Committed { version: 3 }
    );
    let reopened = Fs::open(backend.operator()).await.unwrap();
    let old_output = temp.path().join("old");
    yinyang::restore_directory(&reopened, &pinned, &old_output)
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
async fn failed_upload_and_stale_publication_preserve_remote_state() {
    let temp = tempfile::tempdir().unwrap();
    let input = temp.path().join("source");
    source(&input).await;
    let backend = TestBackend::default();
    let fs = Fs::create(backend.operator()).await.unwrap();
    let first = fs.observe().await.unwrap();
    backend.state.lock().unwrap().fail_data_close = true;
    assert_eq!(
        yinyang::publish_directory(&fs, &first, &input, CommitId::generate())
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Storage
    );
    assert_eq!(fs.observe().await.unwrap(), first);
    backend.state.lock().unwrap().fail_data_close = false;
    let mut winner = first.edit();
    winner
        .create_dir(FsPath::new("winner").unwrap(), false)
        .unwrap();
    fs.commit(&first, CommitId::generate(), winner.finish().unwrap())
        .await
        .unwrap();
    assert_eq!(
        yinyang::publish_directory(&fs, &first, &input, CommitId::generate())
            .await
            .unwrap(),
        CommitOutcome::Conflict { current: 1 }
    );
    assert!(
        fs.observe()
            .await
            .unwrap()
            .tree()
            .get(&FsPath::new("winner").unwrap())
            .is_some()
    );
}

#[tokio::test]
async fn source_mutation_during_upload_prevents_publication() {
    let temp = tempfile::tempdir().unwrap();
    let source = temp.path().join("file");
    std::fs::write(&source, b"original").unwrap();
    let backend = TestBackend::default();
    let fs = Fs::create(backend.operator()).await.unwrap();
    let observed = fs.observe().await.unwrap();
    backend.state.lock().unwrap().file_to_change_on_data_close = Some(source);
    let error = yinyang::publish_directory(&fs, &observed, temp.path(), CommitId::generate())
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Invalid);
    assert_eq!(error.operation(), "publish directory");
    assert!(error.message().contains("source changed"));
    assert_eq!(fs.observe().await.unwrap(), observed);
}

#[tokio::test]
async fn rejects_observations_from_another_filesystem_before_local_io() {
    let first = Fs::create(TestBackend::default().operator()).await.unwrap();
    let second = Fs::create(TestBackend::default().operator()).await.unwrap();
    let observed = first.observe().await.unwrap();
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
async fn restore_never_overwrites_and_does_not_install_corrupt_bytes() {
    let temp = tempfile::tempdir().unwrap();
    let input = temp.path().join("source");
    tokio::fs::create_dir(&input).await.unwrap();
    tokio::fs::write(input.join("file"), b"original")
        .await
        .unwrap();
    let backend = TestBackend::default();
    let fs = Fs::create(backend.operator()).await.unwrap();
    yinyang::publish_directory(
        &fs,
        &fs.observe().await.unwrap(),
        &input,
        CommitId::generate(),
    )
    .await
    .unwrap();
    let observed = fs.observe().await.unwrap();
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
    let NodeBody::File(file) = observed
        .tree()
        .get(&FsPath::new("file").unwrap())
        .unwrap()
        .body()
    else {
        panic!()
    };
    let key = std::str::from_utf8(file.parts()[0].blob().as_bytes()).unwrap();
    backend
        .state
        .lock()
        .unwrap()
        .objects
        .get_mut(key)
        .unwrap()
        .bytes[0] ^= 1;
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
    let fs = Fs::create(TestBackend::default().operator()).await.unwrap();
    yinyang::publish_directory(
        &fs,
        &fs.observe().await.unwrap(),
        &input,
        CommitId::generate(),
    )
    .await
    .unwrap();
    let observed = fs.observe().await.unwrap();
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
    assert_eq!(fs.observe().await.unwrap(), observed);
}

#[cfg(unix)]
#[tokio::test]
async fn invalid_names_fail_before_upload() {
    let temp = tempfile::tempdir().unwrap();
    std::fs::write(temp.path().join("CON"), b"not portable").unwrap();
    let backend = TestBackend::default();
    let fs = Fs::create(backend.operator()).await.unwrap();
    let observed = fs.observe().await.unwrap();
    assert_eq!(
        yinyang::publish_directory(&fs, &observed, temp.path(), CommitId::generate())
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Invalid
    );
    assert!(
        !backend
            .state
            .lock()
            .unwrap()
            .objects
            .keys()
            .any(|key| key.starts_with(".yinyang/data/"))
    );
}

#[test]
fn cli_help_lists_the_transfer_commands() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_yy"))
        .arg("--help")
        .output()
        .unwrap();
    assert!(output.status.success());
    let text = String::from_utf8(output.stdout).unwrap();
    for command in ["create", "publish", "restore", "status"] {
        assert!(text.contains(command));
    }
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
    // Retrying the same operation does not require replacement permission.
    success(&["publish", input.to_str().unwrap(), "--commit-id", &id]);
    assert!(success(&["status"]).contains("version: 1"));
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
    assert!(success(&["status"]).contains("version: 2"));
}
