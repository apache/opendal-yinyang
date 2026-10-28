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

use borsh::BorshDeserialize;
use raw_s3::RawS3;
use s3_gate::{CommitAction, S3Gate};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs;
use std::io::{BufRead, BufReader, Read};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

const LEGACY_SOURCE: &str = "fd1338c2f924a90e9da752b9158e748d1d3acaf4";
const SERVICE_TOKEN: &str = "installed-upgrade-service-token-with-at-least-32-bytes";
const PROCESS_TIMEOUT: Duration = Duration::from_secs(180);
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
    require_environment();
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
    let fixture_manifest: Value = serde_json::from_slice(
        &fs::read(
            Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("tests/fixtures/compatibility/fd1338c/manifest.json"),
        )
        .expect("read fd1338c fixture manifest"),
    )
    .expect("parse fd1338c fixture manifest");
    assert_eq!(fixture_manifest["producer"]["commit"], LEGACY_SOURCE);
    assert_eq!(
        fixture_manifest["producer"]["binary_sha256"], legacy_sha256,
        "YY_LEGACY_BIN does not match the immutable fd1338c producer identity"
    );

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

    let storage = Storage::new();
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
    assert_eq!(
        sqlite_journal_mode(&service_database),
        "wal",
        "fd1338c service authority did not use WAL mode"
    );
    assert_sqlite_integrity(&service_database);
    let wal_bytes = wal.metadata().map(|metadata| metadata.len()).unwrap_or(0);

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

    assert_sqlite_integrity(&service_database);
    let restarted_service =
        ServiceProcess::start(&current_yy, &storage, &service_root, &service_database);
    rewrite_service_address(&service_config, &restarted_service.address.to_string());
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
        },
        "target": {
            "release": candidate_release,
            "binary_sha256": candidate_sha256,
        },
        "retained": {
            "frozen_commit": frozen_commit,
            "frozen_request_digest": frozen_digest,
            "pending_commit": pending_commit,
            "pending_request_digest": pending_digest,
            "sync_unknown_commit": sync_commit,
            "sync_unknown_request_digest": sync_digest,
            "service_commit": service_commit,
            "service_wal_bytes": wal_bytes,
        },
        "target_convergence": {
            "file_bytes": true,
            "sync_in_sync": true,
            "sync_original_revision_restored": true,
            "service_wal_reopened": true,
            "doctor": doctor["status"],
        },
    });
    write_optional_evidence(&evidence);
    println!(
        "linux-prefix-upgrade: {}",
        serde_json::to_string(&evidence).unwrap()
    );
}

fn require_environment() {
    for name in [
        "YY_LEGACY_BIN",
        "YINYANG_S3_BUCKET",
        "YINYANG_S3_ENDPOINT",
        "YINYANG_S3_REGION",
        "YINYANG_S3_ACCESS_KEY_ID",
        "YINYANG_S3_SECRET_ACCESS_KEY",
    ] {
        assert!(
            std::env::var_os(name).is_some(),
            "{name} is unset; installed-prefix upgrade fails hard instead of skipping"
        );
    }
    assert_eq!(
        std::env::var("YINYANG_STORAGE_PROFILE").as_deref(),
        Ok("minio"),
        "installed-prefix upgrade uses the isolated Silo qualification storage"
    );
    assert!(
        std::env::var("YINYANG_S3_ENDPOINT")
            .expect("isolated S3 endpoint")
            .starts_with("http://"),
        "installed-prefix fault injection requires the plain-HTTP Silo endpoint"
    );
}

struct Storage {
    raw: RawS3,
    variables: Vec<(&'static str, String)>,
}

impl Storage {
    fn new() -> Self {
        let mut variables = Vec::new();
        for name in [
            "YINYANG_S3_BUCKET",
            "YINYANG_S3_ENDPOINT",
            "YINYANG_S3_REGION",
            "YINYANG_S3_ACCESS_KEY_ID",
            "YINYANG_S3_SECRET_ACCESS_KEY",
        ] {
            variables.push((
                name,
                std::env::var(name)
                    .unwrap_or_else(|_| panic!("{name} is required for upgrade acceptance")),
            ));
        }
        Self {
            raw: RawS3::from_env(),
            variables,
        }
    }

    fn root(&self, label: &str) -> String {
        format!(
            "linux-prefix-upgrade/{label}-{}",
            uuid::Uuid::new_v4().simple()
        )
    }

    fn apply(&self, command: &mut Command, root: &str, token: Option<&str>) {
        for (name, value) in std::env::vars_os() {
            if !name.to_string_lossy().starts_with("YINYANG_") {
                command.env(name, value);
            }
        }
        for (name, value) in &self.variables {
            command.env(name, value);
        }
        command
            .env("YINYANG_STORAGE_PROFILE", "minio")
            .env("YINYANG_S3_ROOT", root)
            .env("YINYANG_SERVICE_TOKEN", token.unwrap_or(SERVICE_TOKEN));
    }
}

struct RemoteCleanup {
    roots: Vec<String>,
}

impl RemoteCleanup {
    fn new(roots: Vec<String>) -> Self {
        Self { roots }
    }

    fn cleanup(&mut self) {
        let raw = RawS3::from_env();
        for root in self.roots.drain(..) {
            for _ in 0..3 {
                let keys = raw
                    .inventory(&format!("{root}/"))
                    .into_keys()
                    .collect::<Vec<_>>();
                if keys.is_empty() {
                    break;
                }
                for key in keys {
                    raw.delete(&key);
                }
            }
        }
    }
}

impl Drop for RemoteCleanup {
    fn drop(&mut self) {
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| self.cleanup()));
    }
}

struct ServiceProcess {
    child: Option<Child>,
    stderr: Option<thread::JoinHandle<Vec<u8>>>,
    address: std::net::SocketAddr,
}

impl ServiceProcess {
    fn start(yy: &Path, storage: &Storage, root: &str, database: &Path) -> Self {
        let listener =
            std::net::TcpListener::bind("127.0.0.1:0").expect("reserve service test port");
        let address = listener.local_addr().expect("reserve service address");
        drop(listener);
        let mut command = current_yy_command(yy, storage, root);
        command
            .args([
                "serve",
                "--database",
                path_text(database),
                "--listen",
                &address.to_string(),
            ])
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut child = command.spawn().expect("spawn installed metadata service");
        let stdout = child.stdout.take().expect("service stdout");
        let stderr = child.stderr.take().expect("service stderr");
        let (sender, receiver) = mpsc::channel();
        thread::spawn(move || {
            let mut reader = BufReader::new(stdout);
            let mut line = String::new();
            let result = reader.read_line(&mut line);
            let _ = sender.send((result, line));
            let mut rest = Vec::new();
            let _ = reader.read_to_end(&mut rest);
        });
        let stderr = thread::spawn(move || {
            let mut bytes = Vec::new();
            let _ = BufReader::new(stderr).read_to_end(&mut bytes);
            bytes
        });
        let (read, line) = receiver
            .recv_timeout(Duration::from_secs(60))
            .expect("metadata service readiness timeout");
        read.expect("read metadata service readiness");
        let address = match line
            .trim()
            .strip_prefix("metadata service listening on ")
            .and_then(|value| value.parse().ok())
        {
            Some(address) => address,
            None => {
                let _ = child.kill();
                let status = child.wait().expect("wait for failed metadata service");
                let stderr = stderr.join().unwrap_or_default();
                panic!(
                    "unexpected metadata service readiness {line:?} with {status}:\n{}",
                    String::from_utf8_lossy(&stderr)
                );
            }
        };
        Self {
            child: Some(child),
            stderr: Some(stderr),
            address,
        }
    }

    fn kill_without_cleanup(mut self) {
        if let Some(mut child) = self.child.take() {
            child.kill().expect("kill metadata service");
            let _ = child.wait();
        }
        if let Some(stderr) = self.stderr.take() {
            let _ = stderr.join();
        }
    }

    fn stop(mut self) {
        if let Some(mut child) = self.child.take() {
            child.kill().expect("stop metadata service");
            let status = child.wait().expect("wait for metadata service");
            if !matches!(
                status.code(),
                Some(code) if code == 0
            ) {
                if let Some(stderr) = self.stderr.take() {
                    let bytes = stderr.join().unwrap_or_default();
                    if !bytes.is_empty() {
                        eprintln!("{}", String::from_utf8_lossy(&bytes));
                    }
                }
            }
        }
    }
}

impl Drop for ServiceProcess {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
        if let Some(stderr) = self.stderr.take() {
            let _ = stderr.join();
        }
    }
}

fn current_yy_command(yy: &Path, storage: &Storage, root: &str) -> Command {
    let mut command = Command::new(yy);
    storage.apply(&mut command, root, None);
    command
}

fn volume_command(
    yy: &Path,
    storage: &Storage,
    root: &str,
    config: &Path,
    token: Option<&str>,
) -> Command {
    let mut command = Command::new(yy);
    storage.apply(&mut command, root, token);
    command.args(["--volume", path_text(config)]);
    command
}

fn volume_file_command(yy: &Path, storage: &Storage, root: &str, config: &Path) -> Command {
    let mut command = volume_command(yy, storage, root, config, None);
    command.arg("file");
    command
}

fn run_file_ok(yy: &Path, storage: &Storage, root: &str, config: &Path, args: &[&str]) -> Output {
    run_ok(
        volume_file_command(yy, storage, root, config).args(args),
        &format!("installed yy file {args:?}"),
    )
}

fn run_ok(command: &mut Command, context: &str) -> Output {
    command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = command.spawn().expect("spawn installed yy");
    let deadline = Instant::now() + PROCESS_TIMEOUT;
    loop {
        if child.try_wait().expect("poll installed yy").is_some() {
            let output = child
                .wait_with_output()
                .expect("collect installed yy output");
            assert!(
                output.status.success(),
                "{context} failed with {}:\n{}",
                output.status,
                output_text(&output)
            );
            return output;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let output = child
                .wait_with_output()
                .expect("collect timed-out installed yy output");
            panic!(
                "{context} timed out after {PROCESS_TIMEOUT:?}:\n{}",
                output_text(&output)
            );
        }
        thread::sleep(Duration::from_millis(25));
    }
}

fn wait_gate_or_child_exit(gate: &S3Gate, child: &mut Child, context: &str) {
    let deadline = Instant::now() + Duration::from_secs(90);
    loop {
        if gate.wait_intercepted_timeout(Duration::from_millis(100)) {
            return;
        }
        if let Some(status) = child.try_wait().expect("poll gated child") {
            let mut stdout = Vec::new();
            if let Some(mut pipe) = child.stdout.take() {
                pipe.read_to_end(&mut stdout)
                    .expect("read gated child stdout");
            }
            let mut stderr = Vec::new();
            if let Some(mut pipe) = child.stderr.take() {
                pipe.read_to_end(&mut stderr)
                    .expect("read gated child stderr");
            }
            let output = Output {
                status,
                stdout,
                stderr,
            };
            panic!(
                "{context} exited before the S3 gate intercepted its commit:\n{}",
                output_text(&output)
            );
        }
        assert!(
            Instant::now() < deadline,
            "{context} did not reach the S3 gate within 90s"
        );
    }
}

fn write_config(
    path: &Path,
    name: &str,
    access: &str,
    staging: &Path,
    publication: &str,
    service: Option<&str>,
) {
    let mut document = json!({
        "name": name,
        "model": "managed",
        "access": access,
        "frontend": "library",
        "publication": publication,
        "storage_profile": "minio",
        "staging": staging,
        "read_only": false,
        "require": [],
    });
    if let Some(service) = service {
        document["service_address"] = Value::String(service.to_owned());
    }
    fs::write(
        path,
        serde_json::to_vec_pretty(&document).expect("serialize volume"),
    )
    .expect("write installed-upgrade volume");
}

fn rewrite_service_address(config: &Path, address: &str) {
    let mut document: Value =
        serde_json::from_slice(&fs::read(config).expect("read service volume"))
            .expect("parse service volume");
    document["service_address"] = Value::String(address.to_owned());
    fs::write(
        config,
        serde_json::to_vec_pretty(&document).expect("serialize service volume"),
    )
    .expect("rewrite service volume address");
}

fn open_handle(
    yy: &Path,
    storage: &Storage,
    root: &str,
    config: &Path,
    path: &str,
    write: bool,
) -> String {
    let mut command = volume_file_command(yy, storage, root, config);
    command.args(["open", path]);
    if write {
        command.arg("--write");
    }
    let output = run_ok(&mut command, &format!("open installed file {path}"));
    parse_handle(&output.stdout)
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

fn read_handle(yy: &Path, storage: &Storage, root: &str, config: &Path, handle: &str) -> Output {
    run_file_ok(yy, storage, root, config, &["read", handle])
}

fn parse_handle(stdout: &[u8]) -> String {
    String::from_utf8(stdout.to_vec())
        .expect("file handle is UTF-8")
        .trim()
        .to_owned()
}

fn handle_line<'a>(status: &'a str, handle: &str) -> &'a str {
    status
        .lines()
        .find(|line| line.starts_with(&format!("handle: {handle} ")))
        .unwrap_or_else(|| panic!("handle {handle} is missing from status:\n{status}"))
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

fn transaction_identity(plan: &[u8]) -> (String, String) {
    let (_, commit, digest) =
        yinyang_core::Transaction::inspect_identity(plan).expect("inspect frozen transaction");
    (
        uuid::Uuid::from_bytes(*commit.as_bytes()).to_string(),
        digest.iter().map(|byte| format!("{byte:02x}")).collect(),
    )
}

fn parse_file_commit(stdout: &[u8]) -> String {
    String::from_utf8(stdout.to_vec())
        .expect("file fsync output is UTF-8")
        .trim()
        .strip_prefix("committed: ")
        .and_then(|line| line.split(" revision: ").next())
        .expect("file fsync commit")
        .to_owned()
}

fn receipt(yy: &Path, storage: &Storage, root: &str, commit: &str) -> BTreeMap<String, String> {
    let output = run_ok(
        current_yy_command(yy, storage, root).args(["receipt", commit]),
        "query retained receipt",
    );
    String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter_map(|line| line.split_once(": "))
        .map(|(name, value)| (name.to_owned(), value.to_owned()))
        .collect()
}

fn assert_receipt(yy: &Path, storage: &Storage, root: &str, commit: &str, digest: &str) {
    let receipt = receipt(yy, storage, root, commit);
    assert_eq!(field(&receipt, "commit"), commit);
    assert_eq!(field(&receipt, "request digest"), digest);
}

fn field<'a>(values: &'a BTreeMap<String, String>, name: &str) -> &'a str {
    values
        .get(name)
        .unwrap_or_else(|| panic!("missing {name:?} in receipt fields {values:?}"))
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

fn assert_sqlite_integrity(database: &Path) {
    let connection =
        rusqlite::Connection::open_with_flags(database, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
            .expect("open service database read-only");
    let result: String = connection
        .query_row("PRAGMA integrity_check", [], |row| row.get(0))
        .expect("run service SQLite integrity_check");
    assert_eq!(result, "ok", "service database integrity failure");
}

fn sqlite_journal_mode(database: &Path) -> String {
    let connection =
        rusqlite::Connection::open_with_flags(database, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
            .expect("open service database read-only");
    connection
        .query_row("PRAGMA journal_mode", [], |row| row.get(0))
        .expect("read service SQLite journal mode")
}

fn assert_no_installed_writer(prefix: &Path, work: &Path) {
    let output = Command::new("pgrep")
        .args(["-f", &path_text(prefix)])
        .output()
        .expect("run pgrep for installed prefix");
    if output.status.success() {
        let processes = String::from_utf8_lossy(&output.stdout);
        let allowed = processes
            .lines()
            .all(|pid| !Path::new(&format!("/proc/{pid}/cwd")).exists());
        assert!(
            allowed,
            "an installed yy writer remains before the current switch:\n{processes}"
        );
    }
    assert!(
        work.exists(),
        "upgrade work directory disappeared before the current switch"
    );
}

fn assert_storage_inventory_complete_when_empty(storage: &Storage, root: &str) {
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut inventory = storage.raw.inventory(&format!("{root}/"));
    while !inventory.is_empty() && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(200));
        inventory = storage.raw.inventory(&format!("{root}/"));
    }
    assert!(
        inventory.is_empty(),
        "remote cleanup did not remove every key below {root}: {:?}",
        inventory.keys().collect::<Vec<_>>()
    );
}

fn install_release(prefix: &Path, release: &str, source: &Path) -> PathBuf {
    assert!(
        !release.contains('/') && !release.contains(".."),
        "release directory must be one safe path component"
    );
    let directory = prefix.join(release);
    let bin = directory.join("bin");
    fs::create_dir_all(&bin).expect("create versioned release directory");
    let destination = bin.join("yy");
    fs::copy(source, &destination).expect("install release binary");
    let mut permissions = fs::metadata(&destination)
        .expect("release binary metadata")
        .permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(&destination, permissions).expect("set release binary mode");
    destination
}

fn switch_current(prefix: &Path, release: &str) {
    let temporary = prefix.join(".current.new");
    let current = prefix.join("current");
    if temporary.exists() {
        fs::remove_file(&temporary).expect("remove abandoned current staging link");
    }
    std::os::unix::fs::symlink(release, &temporary).expect("create current staging link");
    fs::rename(&temporary, &current).expect("atomically replace current symlink");
}

fn sha256_file(path: &Path) -> String {
    Sha256::digest(fs::read(path).expect("read binary for SHA-256"))
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn text(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned()
}

fn output_text(output: &Output) -> String {
    format!(
        "status: {}\nstdout:\n{}\nstderr:\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

fn path_text(path: &Path) -> &str {
    path.to_str().expect("acceptance path is UTF-8")
}

fn write_optional_evidence(evidence: &Value) {
    let Some(path) = std::env::var_os("YINYANG_UPGRADE_EVIDENCE").map(PathBuf::from) else {
        return;
    };
    let parent = path.parent().expect("evidence path has a parent");
    fs::create_dir_all(parent).expect("create upgrade evidence directory");
    let temporary = parent.join(format!(
        ".{}.{}.tmp",
        path.file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("upgrade-evidence"),
        std::process::id()
    ));
    fs::write(
        &temporary,
        serde_json::to_vec_pretty(evidence).expect("serialize upgrade evidence"),
    )
    .expect("write temporary upgrade evidence");
    fs::rename(temporary, path).expect("atomically publish upgrade evidence");
}
