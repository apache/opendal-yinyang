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

//! Cross-version recovery from requests dispatched by the fd1338c producer.

use super::runtime::{Materialized, current_binary, legacy_binary, tree_files, write_config};
use crate::process::run_timeout;
use crate::s3_gate::{CommitAction, S3Gate};
use borsh::BorshDeserialize;
use serde_json::Value;
use std::collections::BTreeMap;
use std::fs;
use std::io::Write;
use std::path::Path;
use std::process::{Child, Command, Output, Stdio};
use std::thread;
use std::time::Duration;

const PROCESS_TIMEOUT: Duration = Duration::from_secs(120);
const ORIGINAL_PAYLOAD: &[u8] = b"accepted by fd1338c before source mutation\n";
const NEWER_PAYLOAD: &[u8] = b"newer local work for a separate identity\n";
const FILE_FROZEN_PAYLOAD: &[u8] = b"fd1338c frozen file bytes\n";
const FILE_NEWER_PAYLOAD: &[u8] = b"newer pending local bytes\n";
const SYNC_V1: &[u8] = b"fd1338c sync unknown version one\n";
const SYNC_V2: &[u8] = b"newer local version two after dispatch\n";

struct Running {
    child: Option<Child>,
}

impl Running {
    fn spawn(mut command: Command) -> Self {
        command
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        Self {
            child: Some(command.spawn().expect("spawn gated yy process")),
        }
    }

    fn kill(mut self) -> Output {
        let mut child = self.child.take().expect("running child");
        let _ = child.kill();
        child
            .wait_with_output()
            .expect("wait for killed yy process")
    }
}

impl Drop for Running {
    fn drop(&mut self) {
        if let Some(child) = &mut self.child {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

#[derive(BorshDeserialize)]
#[allow(dead_code)]
struct StageRecord {
    id: [u8; 16],
    node: [u8; 16],
    base: [u8; 24],
    length: u64,
    local: u64,
    remote: u64,
    writable: bool,
    ready: bool,
    plan: Option<Vec<u8>>,
    conflict: bool,
    error: Option<String>,
}

#[test]
#[ignore = "requires isolated S3 and YY_LEGACY_BIN; run with cargo x test-compatibility"]
fn compatibility_fd1338c_precommit_drop_recovers_original_directory_request() {
    let storage = Materialized::component("object");
    let current = current_binary();
    let legacy = legacy_binary();
    let source = storage.path("precommit-source");
    write_source(&source, ORIGINAL_PAYLOAD);
    let accepted = tree_files(&source);
    let state = storage.path("precommit-journal");
    let commit = uuid::Uuid::new_v4().to_string();
    let original_head = storage
        .raw_get(".yinyang/head")
        .expect("fd1338c object head");

    let gate = S3Gate::start(CommitAction::DropBeforeCommit);
    let mut command = storage.command(
        &legacy,
        &[
            "publish",
            source.to_str().unwrap(),
            "--replace",
            "--commit-id",
            &commit,
            "--state",
            state.to_str().unwrap(),
        ],
    );
    command.env("YINYANG_S3_ENDPOINT", gate.endpoint());
    let running = Running::spawn(command);
    gate.wait_intercepted();

    assert_eq!(
        storage.raw_get(".yinyang/head"),
        Some(original_head.clone()),
        "the gated request must not reach the object commit"
    );
    let in_flight = inspect_journal(&storage, &current, &state);
    let in_flight_intent = only_intent(&in_flight);
    assert_eq!(in_flight_intent["state"], "unknown");
    assert_eq!(in_flight_intent["commit_id"], commit.as_str());
    assert_eq!(in_flight_intent["can_retry"], true);
    assert_eq!(in_flight_intent["can_export"], true);
    let frozen_digest = in_flight_intent["request_digest"]
        .as_str()
        .expect("frozen request digest")
        .to_owned();

    mutate_source(&source);
    assert_ne!(
        tree_files(&source),
        accepted,
        "source mutation must make newer local work observable"
    );

    let _ = running.kill();
    gate.release();
    gate.wait_original_dropped();
    drop(gate);

    assert_eq!(
        storage.raw_get(".yinyang/head"),
        Some(original_head),
        "dropping the request before commit must leave the authority unchanged"
    );
    let retained = inspect_journal(&storage, &current, &state);
    assert_eq!(only_intent(&retained)["state"], "unknown");

    let exported = storage.path("precommit-export");
    run_ok(
        &storage,
        &current,
        &[
            "publication",
            "export",
            &commit,
            "--state",
            state.to_str().unwrap(),
            "--destination",
            exported.to_str().unwrap(),
        ],
        "current export of the fd1338c frozen directory request",
    );
    assert_eq!(
        tree_files(&exported),
        accepted,
        "source mutation changed the accepted frozen bytes"
    );
    assert!(
        exported.join("nested/empty-dir").is_dir(),
        "directory export lost an accepted empty directory"
    );

    let retried = run_ok(
        &storage,
        &current,
        &[
            "publication",
            "retry",
            &commit,
            "--state",
            state.to_str().unwrap(),
        ],
        "current retry of the fd1338c precommit-dropped request",
    );
    let receipt = parse_receipt(&retried.stdout);
    assert_eq!(field(&receipt, "commit"), commit);
    assert_eq!(field(&receipt, "request digest"), frozen_digest);
    let revision = field(&receipt, "revision").to_owned();
    assert_fd1338c_receipt(&storage, &current, &commit, &frozen_digest);
    let committed = inspect_journal(&storage, &current, &state);
    let committed_intent = only_intent(&committed);
    assert_eq!(committed_intent["state"], "committed");
    assert_eq!(committed_intent["request_digest"], frozen_digest.as_str());

    let restored = storage.path("precommit-restored-original");
    run_ok(
        &storage,
        &current,
        &[
            "restore",
            restored.to_str().unwrap(),
            "--revision",
            &revision,
        ],
        "restore the original fd1338c request revision",
    );
    assert_eq!(tree_files(&restored), accepted);

    let newer_commit = uuid::Uuid::new_v4().to_string();
    run_ok(
        &storage,
        &current,
        &[
            "publish",
            source.to_str().unwrap(),
            "--replace",
            "--commit-id",
            &newer_commit,
        ],
        "publish newer local source as a new identity",
    );
    let latest = storage.path("precommit-restored-newer");
    run_ok(
        &storage,
        &current,
        &["restore", latest.to_str().unwrap()],
        "restore the newer local identity",
    );
    assert_eq!(tree_files(&latest), tree_files(&source));
}

#[test]
#[ignore = "requires isolated S3 and YY_LEGACY_BIN; run with cargo x test-compatibility"]
fn compatibility_fd1338c_lost_commit_response_resolves_directory_receipt() {
    let storage = Materialized::component("object");
    let current = current_binary();
    let legacy = legacy_binary();
    let source = storage.path("response-loss-source");
    write_source(&source, ORIGINAL_PAYLOAD);
    let accepted = tree_files(&source);
    let state = storage.path("response-loss-journal");
    let commit = uuid::Uuid::new_v4().to_string();
    let original_head = storage
        .raw_get(".yinyang/head")
        .expect("fd1338c object head");

    let gate = S3Gate::start(CommitAction::DropResponse);
    let mut command = storage.command(
        &legacy,
        &[
            "publish",
            source.to_str().unwrap(),
            "--replace",
            "--commit-id",
            &commit,
            "--state",
            state.to_str().unwrap(),
        ],
    );
    command.env("YINYANG_S3_ENDPOINT", gate.endpoint());
    let lost = run_timeout(command, PROCESS_TIMEOUT);
    assert!(
        !lost.status.success(),
        "the fd1338c producer must observe response loss as failure:\n{}",
        lost.text()
    );
    gate.wait_intercepted();
    drop(gate);

    assert_ne!(
        storage.raw_get(".yinyang/head"),
        Some(original_head),
        "the response-loss gate must first commit upstream"
    );
    let unknown = inspect_journal(&storage, &current, &state);
    let intent = only_intent(&unknown);
    assert_eq!(intent["state"], "unknown");
    assert_eq!(intent["commit_id"], commit.as_str());
    let frozen_digest = intent["request_digest"]
        .as_str()
        .expect("frozen request digest")
        .to_owned();

    assert_fd1338c_receipt(&storage, &current, &commit, &frozen_digest);
    mutate_source(&source);
    let committed_head = storage
        .raw_get(".yinyang/head")
        .expect("committed object head");
    let retried = run_ok(
        &storage,
        &current,
        &[
            "publication",
            "retry",
            &commit,
            "--state",
            state.to_str().unwrap(),
        ],
        "current receipt resolution of the fd1338c lost response",
    );
    let receipt = parse_receipt(&retried.stdout);
    assert_eq!(field(&receipt, "commit"), commit);
    assert_eq!(field(&receipt, "request digest"), frozen_digest);
    let revision = field(&receipt, "revision").to_owned();
    assert_eq!(
        storage.raw_get(".yinyang/head"),
        Some(committed_head),
        "resolving the receipt must not issue a second directory commit"
    );
    let committed = inspect_journal(&storage, &current, &state);
    assert_eq!(only_intent(&committed)["state"], "committed");

    let restored = storage.path("response-loss-restored");
    run_ok(
        &storage,
        &current,
        &[
            "restore",
            restored.to_str().unwrap(),
            "--revision",
            &revision,
        ],
        "restore the receipted fd1338c bytes",
    );
    assert_eq!(tree_files(&restored), accepted);
}

#[test]
#[ignore = "requires isolated S3 and YY_LEGACY_BIN; run with cargo x test-compatibility"]
fn compatibility_fd1338c_file_staging_recovers_frozen_and_pending_from_backup() {
    let storage = Materialized::component("object");
    let current = current_binary();
    let legacy = legacy_binary();
    let staging = storage.path("file-stage");
    let config = storage.path("file-stage.json");
    write_config(
        &config,
        "compat-file-recovery",
        "mount",
        &staging,
        "object",
        None,
    );

    let frozen_handle = open_handle(&storage, &legacy, &config, "hello.txt", true);
    write_handle(
        &storage,
        &legacy,
        &config,
        &frozen_handle,
        FILE_FROZEN_PAYLOAD,
    );
    let newer_handle = open_handle(&storage, &legacy, &config, "literal%.txt", true);
    write_handle(
        &storage,
        &legacy,
        &config,
        &newer_handle,
        FILE_NEWER_PAYLOAD,
    );

    let gate = S3Gate::start(CommitAction::DropBeforeCommit);
    let mut command = storage.command(
        &legacy,
        &[
            "--volume",
            config.to_str().unwrap(),
            "file",
            "fsync",
            &frozen_handle,
        ],
    );
    command.env("YINYANG_S3_ENDPOINT", gate.endpoint());
    let running = Running::spawn(command);
    gate.wait_intercepted();

    let live_status = file_status(&storage, &current, &config);
    assert!(handle_line(&live_status, &frozen_handle).contains("frozen: true"));
    assert!(handle_line(&live_status, &newer_handle).contains("pending: true"));

    let _ = running.kill();
    gate.release();
    gate.wait_original_dropped();
    drop(gate);

    let frozen_status = file_status(&storage, &current, &config);
    assert!(handle_line(&frozen_status, &frozen_handle).contains("frozen: true"));
    assert!(handle_line(&frozen_status, &newer_handle).contains("pending: true"));
    let (frozen_commit, frozen_digest) =
        stage_identity(&staging.join("staging.db"), &frozen_handle);

    let restored_staging = storage.path("file-stage-backup");
    fs::create_dir(&restored_staging).unwrap();
    sqlite_backup(
        &staging.join("staging.db"),
        &restored_staging.join("staging.db"),
    );
    let restored_config = storage.path("file-stage-backup.json");
    write_config(
        &restored_config,
        "compat-file-restored",
        "mount",
        &restored_staging,
        "object",
        None,
    );
    let restored_status = file_status(&storage, &current, &restored_config);
    assert!(handle_line(&restored_status, &frozen_handle).contains("frozen: true"));
    assert!(handle_line(&restored_status, &newer_handle).contains("pending: true"));

    let frozen_bytes = read_handle(
        &storage,
        &current,
        &restored_config,
        &frozen_handle,
        "read backed-up frozen bytes with current yy",
    );
    assert_eq!(frozen_bytes.stdout, FILE_FROZEN_PAYLOAD);
    let newer_bytes = read_handle(
        &storage,
        &current,
        &restored_config,
        &newer_handle,
        "read backed-up newer pending bytes with current yy",
    );
    assert_eq!(newer_bytes.stdout, FILE_NEWER_PAYLOAD);

    let published = run_ok(
        &storage,
        &current,
        &[
            "--volume",
            restored_config.to_str().unwrap(),
            "file",
            "fsync",
            &frozen_handle,
        ],
        "current publish of the fd1338c frozen file request",
    );
    let published_commit = String::from_utf8(published.stdout)
        .expect("file fsync output is UTF-8")
        .trim()
        .strip_prefix("committed: ")
        .and_then(|line| line.split(" revision: ").next())
        .expect("file fsync commit identity")
        .to_owned();
    assert_eq!(published_commit, frozen_commit);
    assert_fd1338c_receipt(&storage, &current, &frozen_commit, &frozen_digest);

    let after_frozen = read_handle(
        &storage,
        &current,
        &restored_config,
        &newer_handle,
        "newer local bytes survive frozen recovery",
    );
    assert_eq!(after_frozen.stdout, FILE_NEWER_PAYLOAD);

    let newer_published = run_ok(
        &storage,
        &current,
        &[
            "--volume",
            restored_config.to_str().unwrap(),
            "file",
            "fsync",
            &newer_handle,
        ],
        "current publish of newer pending local bytes",
    );
    let newer_commit = parse_file_commit(&newer_published.stdout);
    assert_ne!(newer_commit, frozen_commit);
    let newer_receipt = receipt(&storage, &current, &newer_commit);
    assert_eq!(field(&newer_receipt, "commit"), newer_commit);

    for (path, expected) in [
        ("hello.txt", FILE_FROZEN_PAYLOAD),
        ("literal%.txt", FILE_NEWER_PAYLOAD),
    ] {
        let handle = open_handle(&storage, &current, &restored_config, path, false);
        let bytes = read_handle(
            &storage,
            &current,
            &restored_config,
            &handle,
            "read remotely accepted file bytes",
        );
        assert_eq!(bytes.stdout, expected, "remote bytes for {path}");
    }

    for handle in [frozen_handle, newer_handle] {
        run_ok(
            &storage,
            &current,
            &[
                "--volume",
                restored_config.to_str().unwrap(),
                "file",
                "close",
                &handle,
            ],
            "close recovered file handle",
        );
    }
}

#[test]
#[ignore = "requires isolated S3 and YY_LEGACY_BIN; run with cargo x test-compatibility"]
fn compatibility_fd1338c_sync_unknown_retry_preserves_newer_local_edit() {
    let storage = Materialized::component("object");
    let current = current_binary();
    let legacy = legacy_binary();
    let state = storage.path("sync-state");
    let directory = storage.path("sync-directory");
    let config = storage.path("sync-recovery.json");
    write_config(
        &config,
        "compat-sync-recovery",
        "sync",
        &state,
        "object",
        None,
    );

    run_ok(
        &storage,
        &legacy,
        &[
            "--volume",
            config.to_str().unwrap(),
            "sync",
            "init",
            "--directory",
            directory.to_str().unwrap(),
        ],
        "fd1338c bind the compatibility Sync root",
    );
    run_ok(
        &storage,
        &legacy,
        &["--volume", config.to_str().unwrap(), "sync", "run"],
        "fd1338c initial compatibility Sync",
    );
    assert_eq!(
        sync_status(&storage, &current, &config)["summary"]["in_sync"],
        true
    );

    write_sync_bytes(&directory.join("hello.txt"), SYNC_V1);
    let original_head = storage
        .raw_get(".yinyang/head")
        .expect("fd1338c object head");
    let gate = S3Gate::start(CommitAction::DropBeforeCommit);
    let mut command = storage.command(
        &legacy,
        &["--volume", config.to_str().unwrap(), "sync", "run"],
    );
    command.env("YINYANG_S3_ENDPOINT", gate.endpoint());
    let worker = thread::spawn(move || run_timeout(command, PROCESS_TIMEOUT));
    gate.wait_intercepted();
    gate.release();
    gate.wait_original_dropped();
    drop(gate);
    let lost = worker.join().expect("join fd1338c Sync process");
    assert!(
        !lost.status.success(),
        "fd1338c Sync must observe the dropped request as failure:\n{}",
        lost.text()
    );
    assert_eq!(
        storage.raw_get(".yinyang/head"),
        Some(original_head),
        "the dropped Sync request must not commit upstream"
    );

    write_sync_bytes(&directory.join("hello.txt"), SYNC_V2);
    let retained = sync_status(&storage, &current, &config);
    assert_eq!(retained["summary"]["unknown"], 1, "{retained:#}");
    let entry = sync_entry(&retained, "hello.txt");
    assert_eq!(entry["state"], "unknown");
    assert_eq!(entry["outcome"]["state"], "unknown");
    assert_eq!(entry["local"]["length"], SYNC_V2.len() as u64);
    assert_eq!(entry["local"]["digest"], blake3_hex(SYNC_V2));
    let frozen_commit = entry["intent"]["commit_id"]
        .as_str()
        .expect("old Sync intent commit identity")
        .to_owned();
    let (stored_commit, frozen_digest) =
        staging_unknown_identity(&state.join("content/staging.db"));
    assert_eq!(stored_commit, frozen_commit);
    let frozen_uuid = uuid::Uuid::parse_str(&frozen_commit)
        .expect("fd1338c Sync commit is a UUID")
        .to_string();
    assert_eq!(
        sync_entry(&retained, "hello.txt")["intent"]["commit_id"],
        frozen_commit.as_str()
    );
    assert_eq!(
        fs::read(directory.join("hello.txt")).unwrap(),
        SYNC_V2,
        "the newer local edit must survive process loss"
    );

    run_ok(
        &storage,
        &current,
        &["--volume", config.to_str().unwrap(), "sync", "run"],
        "current resolution of the fd1338c Sync Unknown outcome",
    );
    let after_retry = sync_status(&storage, &current, &config);
    assert_eq!(after_retry["summary"]["unknown"], 0, "{after_retry:#}");
    assert_eq!(
        fs::read(directory.join("hello.txt")).unwrap(),
        SYNC_V2,
        "resolving v1 must not acknowledge the newer local v2"
    );

    let v1_receipt = receipt(&storage, &current, &frozen_uuid);
    assert_eq!(field(&v1_receipt, "commit"), frozen_uuid);
    assert_eq!(field(&v1_receipt, "request digest"), frozen_digest);
    let revision = field(&v1_receipt, "revision").to_owned();
    let restored_v1 = storage.path("sync-restored-v1");
    run_ok(
        &storage,
        &current,
        &[
            "restore",
            restored_v1.to_str().unwrap(),
            "--revision",
            &revision,
        ],
        "restore the fd1338c Sync v1 receipt",
    );
    assert_eq!(fs::read(restored_v1.join("hello.txt")).unwrap(), SYNC_V1);

    let mut converged = after_retry;
    for _ in 0..3 {
        if converged["summary"]["in_sync"] == true {
            break;
        }
        run_ok(
            &storage,
            &current,
            &["--volume", config.to_str().unwrap(), "sync", "run"],
            "current publish of the newer local Sync edit",
        );
        converged = sync_status(&storage, &current, &config);
    }
    assert_eq!(converged["summary"]["in_sync"], true, "{converged:#}");
    assert_eq!(converged["summary"]["unknown"], 0);
    assert_eq!(fs::read(directory.join("hello.txt")).unwrap(), SYNC_V2);
    let retained_receipt = receipt(&storage, &current, &frozen_uuid);
    assert_eq!(field(&retained_receipt, "revision"), revision);
    assert_eq!(field(&retained_receipt, "request digest"), frozen_digest);
    let restored_v2 = storage.path("sync-restored-v2");
    run_ok(
        &storage,
        &current,
        &["restore", restored_v2.to_str().unwrap()],
        "restore the converged newer Sync edit",
    );
    assert_eq!(fs::read(restored_v2.join("hello.txt")).unwrap(), SYNC_V2);
}

fn write_source(root: &Path, payload: &[u8]) {
    fs::create_dir_all(root.join("nested/empty-dir")).unwrap();
    fs::write(root.join("empty"), []).unwrap();
    fs::write(root.join("nested/accepted.txt"), payload).unwrap();
}

fn mutate_source(root: &Path) {
    fs::write(root.join("nested/accepted.txt"), NEWER_PAYLOAD).unwrap();
    fs::remove_file(root.join("empty")).unwrap();
    fs::write(root.join("newer.txt"), b"newer-only source path").unwrap();
}

fn inspect_journal(storage: &Materialized, current: &Path, state: &Path) -> Value {
    let output = run_ok(
        storage,
        current,
        &[
            "publication",
            "inspect",
            "--state",
            state.to_str().unwrap(),
            "--json",
        ],
        "inspect fd1338c directory journal",
    );
    serde_json::from_slice(&output.stdout)
        .unwrap_or_else(|error| panic!("parse journal inspection: {error}\n{}", output.text()))
}

fn only_intent(inspection: &Value) -> &Value {
    let intents = inspection["intents"]
        .as_array()
        .expect("journal intents array");
    assert_eq!(
        intents.len(),
        1,
        "expected one journal intent: {inspection:#}"
    );
    &intents[0]
}

fn parse_receipt(output: &[u8]) -> BTreeMap<String, String> {
    String::from_utf8_lossy(output)
        .lines()
        .filter_map(|line| line.split_once(": "))
        .map(|(name, value)| (name.to_owned(), value.to_owned()))
        .collect()
}

fn field<'a>(fields: &'a BTreeMap<String, String>, name: &str) -> &'a str {
    fields
        .get(name)
        .unwrap_or_else(|| panic!("missing {name:?} in receipt fields {fields:?}"))
}

fn receipt(storage: &Materialized, current: &Path, commit: &str) -> BTreeMap<String, String> {
    let output = run_ok(
        storage,
        current,
        &["receipt", commit],
        "query exact fd1338c receipt",
    );
    parse_receipt(&output.stdout)
}

fn assert_fd1338c_receipt(storage: &Materialized, current: &Path, commit: &str, digest: &str) {
    let receipt = receipt(storage, current, commit);
    assert_eq!(field(&receipt, "commit"), commit);
    assert_eq!(field(&receipt, "request digest"), digest);
}

fn run_ok(
    storage: &Materialized,
    binary: &Path,
    args: &[&str],
    context: &str,
) -> crate::process::Captured {
    let output = storage.run(binary, args);
    output.success_or(context);
    output
}

fn open_handle(
    storage: &Materialized,
    binary: &Path,
    config: &Path,
    path: &str,
    write: bool,
) -> String {
    let mut args = vec!["--volume", config.to_str().unwrap(), "file", "open", path];
    if write {
        args.push("--write");
    }
    let output = run_ok(storage, binary, &args, "open compatibility file handle");
    String::from_utf8(output.stdout)
        .expect("handle output is UTF-8")
        .trim()
        .to_owned()
}

fn write_handle(
    storage: &Materialized,
    binary: &Path,
    config: &Path,
    handle: &str,
    payload: &[u8],
) {
    let source = storage.path(&format!("{handle}-payload"));
    fs::write(&source, payload).unwrap();
    run_ok(
        storage,
        binary,
        &[
            "--volume",
            config.to_str().unwrap(),
            "file",
            "write",
            handle,
            source.to_str().unwrap(),
        ],
        "write compatibility file handle",
    );
    run_ok(
        storage,
        binary,
        &[
            "--volume",
            config.to_str().unwrap(),
            "file",
            "truncate",
            handle,
            &payload.len().to_string(),
        ],
        "truncate compatibility file handle to accepted length",
    );
}

fn read_handle(
    storage: &Materialized,
    binary: &Path,
    config: &Path,
    handle: &str,
    context: &str,
) -> crate::process::Captured {
    run_ok(
        storage,
        binary,
        &["--volume", config.to_str().unwrap(), "file", "read", handle],
        context,
    )
}

fn file_status(storage: &Materialized, binary: &Path, config: &Path) -> String {
    let output = run_ok(
        storage,
        binary,
        &["--volume", config.to_str().unwrap(), "status"],
        "inspect compatibility file staging",
    );
    String::from_utf8(output.stdout).expect("file status output is UTF-8")
}

fn handle_line<'a>(status: &'a str, handle: &str) -> &'a str {
    status
        .lines()
        .find(|line| line.starts_with(&format!("handle: {handle} ")))
        .unwrap_or_else(|| panic!("handle {handle} missing from status:\n{status}"))
}

fn stage_identity(database: &Path, handle: &str) -> (String, String) {
    let id = uuid::Uuid::parse_str(handle).unwrap();
    let connection =
        rusqlite::Connection::open_with_flags(database, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
            .expect("open fd1338c staging read-only");
    let bytes: Vec<u8> = connection
        .query_row(
            "SELECT record FROM handles WHERE id=?1",
            [id.as_bytes().as_slice()],
            |row| row.get(0),
        )
        .expect("read fd1338c frozen handle record");
    let record: StageRecord = borsh::from_slice(&bytes).expect("decode fd1338c staging record");
    let plan = record.plan.expect("handle must retain a frozen request");
    let (_, commit, digest) = yinyang_core::Transaction::inspect_identity(&plan)
        .expect("inspect frozen request identity");
    (
        uuid::Uuid::from_bytes(*commit.as_bytes()).to_string(),
        digest.iter().map(|byte| format!("{byte:02x}")).collect(),
    )
}

fn parse_file_commit(output: &[u8]) -> String {
    String::from_utf8(output.to_vec())
        .expect("file fsync output is UTF-8")
        .trim()
        .strip_prefix("committed: ")
        .and_then(|line| line.split(" revision: ").next())
        .expect("file fsync commit identity")
        .to_owned()
}

fn sqlite_backup(source: &Path, destination: &Path) {
    let script = format!(
        ".backup '{}'",
        destination.display().to_string().replace('\'', "''")
    );
    let backup = run_timeout(
        {
            let mut command = Command::new("sqlite3");
            command.arg(source).arg(script);
            command
        },
        Duration::from_secs(60),
    );
    backup.success_or("sqlite3 .backup of fd1338c staging");
    let check = run_timeout(
        {
            let mut command = Command::new("sqlite3");
            command.arg(destination).arg("PRAGMA integrity_check;");
            command
        },
        Duration::from_secs(60),
    );
    check.success_or("sqlite3 integrity_check of backed-up staging");
    assert_eq!(check.stdout.trim_ascii(), b"ok");
}

fn write_sync_bytes(path: &Path, bytes: &[u8]) {
    let mut file = fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(path)
        .expect("open Sync local file");
    file.write_all(bytes).expect("write Sync local file");
    file.sync_all().expect("sync Sync local file");
}

fn sync_status(storage: &Materialized, binary: &Path, config: &Path) -> Value {
    let output = run_ok(
        storage,
        binary,
        &[
            "--volume",
            config.to_str().unwrap(),
            "sync",
            "status",
            "--json",
        ],
        "inspect compatibility Sync state",
    );
    serde_json::from_slice(&output.stdout)
        .unwrap_or_else(|error| panic!("parse Sync status: {error}\n{}", output.text()))
}

fn sync_entry<'a>(status: &'a Value, path: &str) -> &'a Value {
    status["entries"]
        .as_array()
        .expect("Sync entries array")
        .iter()
        .find(|entry| entry["path"] == path)
        .unwrap_or_else(|| panic!("Sync entry {path:?} missing:\n{status:#}"))
}

fn staging_unknown_identity(database: &Path) -> (String, String) {
    let connection =
        rusqlite::Connection::open_with_flags(database, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
            .expect("open fd1338c content staging read-only");
    let mut statement = connection
        .prepare("SELECT record FROM handles")
        .expect("prepare fd1338c frozen handle query");
    let rows = statement
        .query_map([], |row| row.get::<_, Vec<u8>>(0))
        .expect("query fd1338c content handles");
    let rows = rows
        .collect::<rusqlite::Result<Vec<_>>>()
        .expect("read fd1338c content handles");
    let mut frozen = Vec::new();
    for bytes in rows {
        let record: StageRecord = borsh::from_slice(&bytes).expect("decode content stage record");
        if let Some(plan) = record.plan {
            let (_, commit, digest) =
                yinyang_core::Transaction::inspect_identity(&plan).expect("inspect frozen content");
            frozen.push((
                commit
                    .as_bytes()
                    .iter()
                    .map(|byte| format!("{byte:02x}"))
                    .collect(),
                digest.iter().map(|byte| format!("{byte:02x}")).collect(),
            ));
        }
    }
    assert_eq!(
        frozen.len(),
        1,
        "expected exactly one frozen Sync content handle"
    );
    frozen.pop().unwrap()
}

fn blake3_hex(bytes: &[u8]) -> String {
    blake3::hash(bytes).to_hex().to_string()
}
