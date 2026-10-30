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

//! Installed-prefix transition from the fd1338c producer to a selected target.

#![cfg(target_os = "linux")]

#[path = "support/raw_s3.rs"]
#[allow(dead_code)]
mod raw_s3;
#[path = "support/s3_gate.rs"]
#[allow(dead_code)]
mod s3_gate;
#[path = "support/upgrade.rs"]
mod upgrade;

use borsh::BorshDeserialize;
use s3_gate::{CommitAction, S3Gate};
use serde_json::{Value, json};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Instant;
use upgrade::{
    RemoteCleanup, ServiceProcess, Storage, assert_no_installed_writer, assert_receipt,
    assert_sqlite_integrity, assert_storage_inventory_complete_when_empty, current_yy_command,
    field, handle_line, install_release, open_handle, parse_file_commit, path_text, read_handle,
    receipt, require_environment, run_file_ok, run_ok, sha256_file, switch_current, target_source,
    text, transaction_identity, volume_command, volume_file_command, wait_gate_or_child_exit,
    write_config, write_optional_evidence,
};

const LEGACY_SOURCE: &str = "fd1338c2f924a90e9da752b9158e748d1d3acaf4";
const BASE_BYTES: &[u8] = b"fd1338c installed base bytes\n";
const FROZEN_BYTES: &[u8] = b"fd1338c frozen upgrade bytes\n";
const PENDING_BYTES: &[u8] = b"fd1338c pending upgrade bytes\n";
const SYNC_BASE: &[u8] = b"fd1338c installed Sync base\n";
const SYNC_V1: &[u8] = b"fd1338c sync upgrade version one\n";
const SYNC_V2: &[u8] = b"target-local sync version two\n";
const SERVICE_BYTES: &[u8] = b"fd1338c service WAL bytes\n";

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
#[ignore = "requires isolated S3, YY_LEGACY_BIN, and a selected YY_UPGRADE_BIN"]
fn installed_prefix_upgrade_preserves_retained_work_and_service_wal() {
    require_environment(&["YY_LEGACY_BIN"], "installed-prefix upgrade");
    let started = Instant::now();
    let temp = tempfile::tempdir().expect("create installed-upgrade directory");
    let legacy_source =
        PathBuf::from(std::env::var_os("YY_LEGACY_BIN").expect("YY_LEGACY_BIN is required"));
    let candidate_source = std::env::var_os("YY_UPGRADE_BIN")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_BIN_EXE_yy")));
    assert!(legacy_source.is_file(), "YY_LEGACY_BIN is not a file");
    assert!(candidate_source.is_file(), "YY_UPGRADE_BIN is not a file");

    let legacy_sha256 = sha256_file(&legacy_source);
    let candidate_sha256 = sha256_file(&candidate_source);
    let target_source = target_source();
    let fixture_manifest: Value = serde_json::from_slice(
        &fs::read(
            Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("tests/fixtures/compatibility/fd1338c/manifest.json"),
        )
        .expect("read fd1338c fixture manifest"),
    )
    .expect("parse fd1338c fixture manifest");
    assert_eq!(fixture_manifest["producer"]["commit"], LEGACY_SOURCE);
    let fixture_legacy_sha256 = fixture_manifest["producer"]["binary_sha256"]
        .as_str()
        .expect("fd1338c fixture binary SHA-256")
        .to_owned();
    let legacy_identity = if legacy_sha256 == fixture_legacy_sha256 {
        "fixture-binary"
    } else {
        assert_eq!(
            std::env::var("YINYANG_UPGRADE_LEGACY_SOURCE").as_deref(),
            Ok(LEGACY_SOURCE),
            "a source-pinned fd1338c rebuild requires YINYANG_UPGRADE_LEGACY_SOURCE"
        );
        assert_eq!(
            std::env::var("YINYANG_UPGRADE_LEGACY_REBUILD").as_deref(),
            Ok("1"),
            "a source-pinned fd1338c rebuild requires YINYANG_UPGRADE_LEGACY_REBUILD=1"
        );
        "source-pinned-rebuild"
    };

    let prefix = temp.path().join(".local/lib/yinyang");
    let legacy_release = std::env::var("YY_LEGACY_RELEASE")
        .unwrap_or_else(|_| format!("{}+fd1338c2f924", env!("CARGO_PKG_VERSION")));
    let candidate_release = std::env::var("YY_UPGRADE_RELEASE").unwrap_or_else(|_| {
        format!(
            "{}+target-{}",
            env!("CARGO_PKG_VERSION"),
            &candidate_sha256[..8]
        )
    });
    assert_ne!(
        legacy_release, candidate_release,
        "immutable release directories must differ"
    );
    install_release(&prefix, &legacy_release, &legacy_source);
    install_release(&prefix, &candidate_release, &candidate_source);
    switch_current(&prefix, &legacy_release);
    assert_eq!(
        fs::read_link(prefix.join("current")).expect("read current symlink"),
        Path::new(&legacy_release)
    );

    let storage = Storage::new("installed-prefix upgrade");
    let object_root = storage.root("upgrade-object");
    let sync_root = storage.root("upgrade-sync");
    let service_root = storage.root("upgrade-service");
    let mut cleanup = RemoteCleanup::new(vec![
        object_root.clone(),
        sync_root.clone(),
        service_root.clone(),
    ]);
    let current_yy = prefix.join("current/bin/yy");

    run_ok(
        current_yy_command(&current_yy, &storage, &object_root).args(["create"]),
        "fd1338c create installed object authority",
    );
    let object_config = temp.path().join("object.json");
    write_config(
        &object_config,
        "upgrade-object",
        "mount",
        &temp.path().join("object-state"),
        "object",
        None,
    );
    run_file_ok(
        &current_yy,
        &storage,
        &object_root,
        &object_config,
        &["create", "hello.txt"],
    );
    let base_handle = open_handle(
        &current_yy,
        &storage,
        &object_root,
        &object_config,
        "hello.txt",
        true,
    );
    write_handle(
        &current_yy,
        &storage,
        &object_root,
        &object_config,
        &base_handle,
        BASE_BYTES,
    );
    run_file_ok(
        &current_yy,
        &storage,
        &object_root,
        &object_config,
        &["fsync", &base_handle],
    );
    run_file_ok(
        &current_yy,
        &storage,
        &object_root,
        &object_config,
        &["close", &base_handle],
    );

    run_file_ok(
        &current_yy,
        &storage,
        &object_root,
        &object_config,
        &["create", "pending.txt"],
    );
    let pending_handle = open_handle(
        &current_yy,
        &storage,
        &object_root,
        &object_config,
        "pending.txt",
        true,
    );
    write_handle(
        &current_yy,
        &storage,
        &object_root,
        &object_config,
        &pending_handle,
        PENDING_BYTES,
    );

    let frozen_handle = open_handle(
        &current_yy,
        &storage,
        &object_root,
        &object_config,
        "hello.txt",
        true,
    );
    write_handle(
        &current_yy,
        &storage,
        &object_root,
        &object_config,
        &frozen_handle,
        FROZEN_BYTES,
    );
    let object_head_before_drop = storage
        .raw
        .get(&format!("{object_root}/.yinyang/head"))
        .expect("object head before frozen request");
    let object_gate = S3Gate::start(CommitAction::DropBeforeCommit);
    let mut frozen_command =
        volume_file_command(&current_yy, &storage, &object_root, &object_config);
    frozen_command
        .args(["fsync", &frozen_handle])
        .env("YINYANG_S3_ENDPOINT", object_gate.endpoint())
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut frozen_process = frozen_command
        .spawn()
        .expect("spawn fd1338c frozen file fsync");
    wait_gate_or_child_exit(
        &object_gate,
        &mut frozen_process,
        "fd1338c frozen file fsync",
    );
    frozen_process.kill().expect("kill fd1338c frozen fsync");
    let _ = frozen_process.wait();
    object_gate.release();
    object_gate.wait_original_dropped();
    drop(object_gate);
    assert_eq!(
        storage.raw.get(&format!("{object_root}/.yinyang/head")),
        Some(object_head_before_drop),
        "dropping the file request before commit must not change the object head"
    );

    let legacy_status = run_ok(
        volume_command(&current_yy, &storage, &object_root, &object_config, None).args(["status"]),
        "fd1338c inspect retained object staging",
    );
    let legacy_status = text(&legacy_status);
    assert!(
        handle_line(&legacy_status, &frozen_handle).contains("frozen: true"),
        "fd1338c did not retain the frozen handle:\n{legacy_status}"
    );
    assert!(
        handle_line(&legacy_status, &pending_handle).contains("pending: true"),
        "fd1338c did not retain the pending handle:\n{legacy_status}"
    );
    let (frozen_commit, frozen_digest) =
        stage_identity(&temp.path().join("object-state/staging.db"), &frozen_handle);

    run_ok(
        current_yy_command(&current_yy, &storage, &sync_root).args(["create"]),
        "fd1338c create installed Sync authority",
    );
    let sync_state = temp.path().join("sync-state");
    let sync_directory = temp.path().join("sync-directory");
    let sync_config = temp.path().join("sync.json");
    write_config(
        &sync_config,
        "upgrade-sync",
        "sync",
        &sync_state,
        "object",
        None,
    );
    fs::create_dir_all(&sync_directory).expect("create installed Sync directory");
    run_ok(
        volume_command(&current_yy, &storage, &sync_root, &sync_config, None).args([
            "sync",
            "init",
            "--directory",
            path_text(&sync_directory),
        ]),
        "fd1338c bind installed Sync directory",
    );
    fs::write(sync_directory.join("hello.txt"), SYNC_BASE).expect("write Sync base");
    run_ok(
        volume_command(&current_yy, &storage, &sync_root, &sync_config, None).args(["sync", "run"]),
        "fd1338c initial installed Sync",
    );
    fs::write(sync_directory.join("hello.txt"), SYNC_V1).expect("write Sync v1");
    let sync_head_before_drop = storage
        .raw
        .get(&format!("{sync_root}/.yinyang/head"))
        .expect("Sync object head before dropped request");
    let sync_gate = S3Gate::start(CommitAction::DropBeforeCommit);
    let mut sync_command = volume_command(&current_yy, &storage, &sync_root, &sync_config, None);
    sync_command
        .args(["sync", "run"])
        .env("YINYANG_S3_ENDPOINT", sync_gate.endpoint())
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let sync_process = sync_command.spawn().expect("spawn fd1338c gated Sync");
    let mut sync_process = sync_process;
    wait_gate_or_child_exit(&sync_gate, &mut sync_process, "fd1338c gated Sync");
    sync_gate.release();
    sync_gate.wait_original_dropped();
    drop(sync_gate);
    let sync_output = sync_process
        .wait_with_output()
        .expect("wait for fd1338c dropped Sync");
    assert!(
        !sync_output.status.success(),
        "fd1338c Sync must observe the dropped commit as failure:\n{}",
        output_text(&sync_output)
    );
    assert_eq!(
        storage.raw.get(&format!("{sync_root}/.yinyang/head")),
        Some(sync_head_before_drop),
        "dropped Sync request must not change the object head"
    );
    fs::write(sync_directory.join("hello.txt"), SYNC_V2).expect("write Sync v2");
    let (sync_commit, sync_digest) =
        staging_unknown_identity(&sync_state.join("content/staging.db"));
    assert_eq!(
        fs::read(sync_directory.join("hello.txt")).expect("read newer local Sync bytes"),
        SYNC_V2
    );

    let service_database = temp.path().join("metadata/metadata.db");
    fs::create_dir_all(service_database.parent().expect("metadata parent"))
        .expect("create metadata directory");
    let service = ServiceProcess::start(&current_yy, &storage, &service_root, &service_database);
    let service_address = service.address;
    let (service_wal_reader, service_journal_mode) = sqlite_wal_reader(&service_database);
    let service_config = temp.path().join("service.json");
    write_config(
        &service_config,
        "upgrade-service",
        "mount",
        &temp.path().join("service-state"),
        "service",
        Some(&service.address.to_string()),
    );
    run_file_ok(
        &current_yy,
        &storage,
        &service_root,
        &service_config,
        &["create", "service.txt"],
    );
    let service_handle = open_handle(
        &current_yy,
        &storage,
        &service_root,
        &service_config,
        "service.txt",
        true,
    );
    write_handle(
        &current_yy,
        &storage,
        &service_root,
        &service_config,
        &service_handle,
        SERVICE_BYTES,
    );
    let service_commit_output = run_file_ok(
        &current_yy,
        &storage,
        &service_root,
        &service_config,
        &["fsync", &service_handle],
    );
    let service_commit = parse_file_commit(&service_commit_output.stdout);
    run_file_ok(
        &current_yy,
        &storage,
        &service_root,
        &service_config,
        &["close", &service_handle],
    );
    service.kill_without_cleanup();
    let wal = service_database.with_extension("db-wal");
    let wal_metadata = wal
        .metadata()
        .unwrap_or_else(|error| panic!("fd1338c service WAL is missing: {error}"));
    assert!(
        wal_metadata.len() > 0,
        "fd1338c service WAL must remain non-empty until the target reopens it"
    );
    let wal_bytes = wal_metadata.len();
    let wal_sha256 = sha256_file(&wal);

    assert_no_installed_writer(&prefix, temp.path());
    let previous_target =
        fs::read_link(prefix.join("current")).expect("read previous current target");
    switch_current(&prefix, &candidate_release);
    assert_eq!(
        fs::read_link(prefix.join("current")).expect("read target current symlink"),
        Path::new(&candidate_release)
    );
    assert_eq!(previous_target, Path::new(&legacy_release));
    assert_eq!(sha256_file(&current_yy), candidate_sha256);

    let target_status = run_ok(
        volume_command(&current_yy, &storage, &object_root, &object_config, None).args(["status"]),
        "target inspect retained object staging",
    );
    let target_status = text(&target_status);
    assert!(handle_line(&target_status, &frozen_handle).contains("frozen: true"));
    assert!(handle_line(&target_status, &pending_handle).contains("pending: true"));
    assert_eq!(
        read_handle(
            &current_yy,
            &storage,
            &object_root,
            &object_config,
            &frozen_handle,
        )
        .stdout,
        FROZEN_BYTES
    );
    assert_eq!(
        read_handle(
            &current_yy,
            &storage,
            &object_root,
            &object_config,
            &pending_handle,
        )
        .stdout,
        PENDING_BYTES
    );

    let frozen_publish = run_file_ok(
        &current_yy,
        &storage,
        &object_root,
        &object_config,
        &["fsync", &frozen_handle],
    );
    assert_eq!(
        parse_file_commit(&frozen_publish.stdout),
        frozen_commit,
        "target changed the original frozen file commit"
    );
    assert_receipt(
        &current_yy,
        &storage,
        &object_root,
        &frozen_commit,
        &frozen_digest,
    );
    let pending_publish = run_file_ok(
        &current_yy,
        &storage,
        &object_root,
        &object_config,
        &["fsync", &pending_handle],
    );
    let pending_commit = parse_file_commit(&pending_publish.stdout);
    assert_ne!(frozen_commit, pending_commit);
    let pending_receipt = receipt(&current_yy, &storage, &object_root, &pending_commit);
    let pending_digest = field(&pending_receipt, "request digest").to_owned();
    assert_receipt(
        &current_yy,
        &storage,
        &object_root,
        &pending_commit,
        &pending_digest,
    );
    for handle in [&frozen_handle, &pending_handle] {
        run_file_ok(
            &current_yy,
            &storage,
            &object_root,
            &object_config,
            &["close", handle],
        );
    }
    for (path, expected) in [("hello.txt", FROZEN_BYTES), ("pending.txt", PENDING_BYTES)] {
        let handle = open_handle(
            &current_yy,
            &storage,
            &object_root,
            &object_config,
            path,
            false,
        );
        assert_eq!(
            read_handle(&current_yy, &storage, &object_root, &object_config, &handle,).stdout,
            expected,
            "target remote bytes for {path}"
        );
        run_file_ok(
            &current_yy,
            &storage,
            &object_root,
            &object_config,
            &["close", &handle],
        );
    }

    let mut target_sync = sync_status(&current_yy, &storage, &sync_root, &sync_config);
    assert_eq!(target_sync["summary"]["unknown"], 1, "{target_sync:#}");
    let entry = sync_entry(&target_sync, "hello.txt");
    assert_eq!(entry["state"], "unknown");
    assert_eq!(
        entry["intent"]["commit_id"],
        sync_commit.replace('-', ""),
        "Sync status and frozen transaction must name the same CommitId"
    );
    assert_eq!(
        fs::read(sync_directory.join("hello.txt")).expect("read target local Sync bytes"),
        SYNC_V2
    );
    for _ in 0..4 {
        if target_sync["summary"]["in_sync"] == true {
            break;
        }
        run_ok(
            volume_command(&current_yy, &storage, &sync_root, &sync_config, None)
                .args(["sync", "run"]),
            "target recover installed Sync",
        );
        target_sync = sync_status(&current_yy, &storage, &sync_root, &sync_config);
    }
    let converged = sync_status(&current_yy, &storage, &sync_root, &sync_config);
    assert_eq!(converged["summary"]["in_sync"], true, "{converged:#}");
    assert_eq!(converged["summary"]["unknown"], 0);
    assert_receipt(
        &current_yy,
        &storage,
        &sync_root,
        &sync_commit,
        &sync_digest,
    );
    let sync_receipt = receipt(&current_yy, &storage, &sync_root, &sync_commit);
    let sync_revision = field(&sync_receipt, "revision").to_owned();
    let restored_v1 = temp.path().join("restored-sync-v1");
    run_ok(
        current_yy_command(&current_yy, &storage, &sync_root).args([
            "restore",
            path_text(&restored_v1),
            "--revision",
            &sync_revision,
        ]),
        "target restore original installed Sync revision",
    );
    assert_eq!(
        fs::read(restored_v1.join("hello.txt")).expect("read restored Sync v1"),
        SYNC_V1
    );
    assert_eq!(
        fs::read(sync_directory.join("hello.txt")).expect("read converged Sync v2"),
        SYNC_V2
    );

    let restarted_service = ServiceProcess::start_at(
        &current_yy,
        &storage,
        &service_root,
        &service_database,
        service_address,
    );
    let service_read_handle = open_handle(
        &current_yy,
        &storage,
        &service_root,
        &service_config,
        "service.txt",
        false,
    );
    assert_eq!(
        read_handle(
            &current_yy,
            &storage,
            &service_root,
            &service_config,
            &service_read_handle,
        )
        .stdout,
        SERVICE_BYTES,
        "target did not recover service bytes from the retained authority and WAL"
    );
    run_file_ok(
        &current_yy,
        &storage,
        &service_root,
        &service_config,
        &["close", &service_read_handle],
    );
    restarted_service.stop();
    drop(service_wal_reader);
    assert_sqlite_integrity(&service_database);

    let doctor = run_ok(
        volume_command(&current_yy, &storage, &object_root, &object_config, None)
            .args(["doctor", "--json"]),
        "target installed object doctor after upgrade",
    );
    let doctor: Value = serde_json::from_slice(&doctor.stdout).expect("parse target doctor");
    assert_eq!(doctor["schema"], "yinyang.operational-health.v1");
    assert_eq!(doctor["status"], "pass", "{doctor:#}");

    cleanup.cleanup();
    for root in [&object_root, &sync_root, &service_root] {
        assert_storage_inventory_complete_when_empty(&storage, root);
    }

    let evidence = json!({
        "schema": "yinyang.linux-prefix-upgrade.v1",
        "status": "pass",
        "elapsed_seconds": started.elapsed().as_secs_f64(),
        "legacy": {
            "source": LEGACY_SOURCE,
            "release": legacy_release,
            "binary_sha256": legacy_sha256,
            "binary_identity": legacy_identity,
            "fixture_binary_sha256": fixture_legacy_sha256,
        },
        "target": {
            "source": target_source,
            "release": candidate_release,
            "binary_sha256": candidate_sha256,
        },
        "backend": {
            "profile": "minio",
            "endpoint": std::env::var("YINYANG_S3_ENDPOINT").expect("isolated endpoint"),
            "region": std::env::var("YINYANG_S3_REGION").expect("isolated region"),
            "bucket": std::env::var("YINYANG_S3_BUCKET").expect("isolated bucket"),
        },
        "retained": {
            "frozen_commit": frozen_commit,
            "frozen_request_digest": frozen_digest,
            "pending_commit": pending_commit,
            "pending_request_digest": pending_digest,
            "sync_unknown_commit": sync_commit,
            "sync_unknown_request_digest": sync_digest,
            "service_commit": service_commit,
            "service_journal_mode": service_journal_mode,
            "service_wal_bytes": wal_bytes,
            "service_wal_sha256": wal_sha256,
        },
        "target_convergence": {
            "file_bytes": true,
            "sync_in_sync": true,
            "sync_original_revision_restored": true,
            "service_wal_reopened": true,
            "doctor": doctor["status"],
        },
    });
    write_optional_evidence("YINYANG_UPGRADE_EVIDENCE", &evidence);
    println!(
        "linux-prefix-upgrade: {}",
        serde_json::to_string(&evidence).unwrap()
    );
}

fn write_handle(
    yy: &Path,
    storage: &Storage,
    root: &str,
    config: &Path,
    handle: &str,
    bytes: &[u8],
) {
    let payload = tempfile::NamedTempFile::new().expect("create payload file");
    fs::write(payload.path(), bytes).expect("write installed-upgrade payload");
    run_file_ok(
        yy,
        storage,
        root,
        config,
        &["write", handle, path_text(payload.path())],
    );
    run_file_ok(
        yy,
        storage,
        root,
        config,
        &["truncate", handle, &bytes.len().to_string()],
    );
}

fn stage_identity(database: &Path, handle: &str) -> (String, String) {
    let id = uuid::Uuid::parse_str(handle).expect("file handle UUID");
    let connection =
        rusqlite::Connection::open_with_flags(database, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
            .expect("open staging database read-only");
    let bytes: Vec<u8> = connection
        .query_row(
            "SELECT record FROM handles WHERE id=?1",
            [id.as_bytes().as_slice()],
            |row| row.get(0),
        )
        .expect("read frozen handle record");
    let record: StageRecord = borsh::from_slice(&bytes).expect("decode frozen handle record");
    let plan = record.plan.expect("handle must retain a frozen request");
    transaction_identity(&plan)
}

fn staging_unknown_identity(database: &Path) -> (String, String) {
    let connection =
        rusqlite::Connection::open_with_flags(database, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
            .expect("open Sync content staging read-only");
    let mut statement = connection
        .prepare("SELECT record FROM handles")
        .expect("prepare frozen Sync handle query");
    let rows = statement
        .query_map([], |row| row.get::<_, Vec<u8>>(0))
        .expect("query Sync handles");
    let mut plans = Vec::new();
    let rows = rows
        .collect::<rusqlite::Result<Vec<_>>>()
        .expect("read Sync handles");
    for bytes in rows {
        let record: StageRecord = borsh::from_slice(&bytes).expect("decode Sync stage record");
        if let Some(plan) = record.plan {
            plans.push(transaction_identity(&plan));
        }
    }
    assert_eq!(plans.len(), 1, "expected exactly one frozen Sync request");
    plans.pop().expect("frozen Sync request")
}

fn sync_status(yy: &Path, storage: &Storage, root: &str, config: &Path) -> Value {
    let output = run_ok(
        volume_command(yy, storage, root, config, None).args(["sync", "status", "--json"]),
        "query retained Sync status",
    );
    serde_json::from_slice(&output.stdout).expect("parse retained Sync status")
}

fn sync_entry<'a>(status: &'a Value, path: &str) -> &'a Value {
    status["entries"]
        .as_array()
        .expect("Sync entries array")
        .iter()
        .find(|entry| entry["path"] == path)
        .unwrap_or_else(|| panic!("Sync entry {path:?} is missing:\n{status:#}"))
}

fn sqlite_wal_reader(database: &Path) -> (rusqlite::Connection, String) {
    let connection =
        rusqlite::Connection::open_with_flags(database, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
            .expect("open service database read-only");
    let journal_mode = connection
        .query_row("PRAGMA journal_mode", [], |row| row.get(0))
        .expect("read service SQLite journal mode");
    assert_eq!(
        journal_mode, "wal",
        "fd1338c service authority did not use WAL mode"
    );
    connection
        .execute_batch("BEGIN")
        .expect("begin service WAL read snapshot");
    let _: i64 = connection
        .query_row("SELECT COUNT(*) FROM sqlite_master", [], |row| row.get(0))
        .expect("establish service WAL read snapshot");
    (connection, journal_mode)
}
