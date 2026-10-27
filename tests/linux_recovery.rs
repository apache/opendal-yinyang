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

#![cfg(target_os = "linux")]

//! Real-process Linux backup and isolated-restore acceptance.

#[path = "linux_sync/harness.rs"]
mod fixture;

use fixture::{Captured, Fixture, Publication};
use std::fs;
use std::io::{BufRead, BufReader, Read};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};
use yinyang::core::service::ServiceClient;
use yinyang::core::{Authority, BackendProfile, CommitId, Fs, NodeId, Receipt, Revision};
use yinyang::runtime::{Runtime, RuntimeStatus};

const SERVICE_TOKEN: &str = "linux-recovery-service-token-0123456789";
const PROCESS_TIMEOUT: Duration = Duration::from_secs(180);

#[test]
#[ignore = "requires isolated YINYANG_S3_* storage and sqlite3"]
fn service_and_mount_staging_restore_preserve_pending_and_unknown_work() {
    let mut fixture = Fixture::new(Publication::Service);
    let base = fixture.remote_create_file("pending.txt", b"base", false);
    fixture.remote_create_file("uncertain.txt", b"base", false);
    let source_staging = fixture.path("mount-source");
    let source_config = fixture.path("mount-source.json");
    write_volume_config(
        &source_config,
        "mount-source",
        &source_staging,
        false,
        Some(fixture.service_address()),
    );

    let pending = parse_handle(&fixture.yy_ok(&[
        "--volume",
        text(&source_config),
        "file",
        "open",
        "pending.txt",
        "--write",
    ]));
    let pending_bytes = fixture.path("pending-input");
    fs::write(&pending_bytes, b"pending-accepted").unwrap();
    fixture.yy_ok(&[
        "--volume",
        text(&source_config),
        "file",
        "write",
        &pending.to_string(),
        text(&pending_bytes),
    ]);

    let uncertain = parse_handle(&fixture.yy_ok(&[
        "--volume",
        text(&source_config),
        "file",
        "open",
        "uncertain.txt",
        "--write",
    ]));
    let uncertain_bytes = fixture.path("uncertain-input");
    fs::write(&uncertain_bytes, b"uncertain-accepted").unwrap();
    fixture.yy_ok(&[
        "--volume",
        text(&source_config),
        "file",
        "write",
        &uncertain.to_string(),
        text(&uncertain_bytes),
    ]);

    let gate = fixture.service_gate(true);
    let gate_config = fixture.path("mount-gate.json");
    write_volume_config(
        &gate_config,
        "mount-gate",
        &source_staging,
        false,
        Some(gate.address()),
    );
    let lost = fixture.yy(&[
        "--volume",
        text(&gate_config),
        "file",
        "fsync",
        &uncertain.to_string(),
    ]);
    assert!(!lost.status.success(), "lost response must fail the CLI");
    gate.wait_for_commit_response(Duration::from_secs(30))
        .expect("service gate did not observe the commit response");
    drop(gate);

    let source_status = inspect(&source_staging);
    let frozen = source_status
        .handles
        .iter()
        .find(|handle| handle.id == uncertain)
        .expect("uncertain handle disappeared");
    let uncertain_commit = frozen
        .frozen_commit_id
        .expect("lost response must retain the exact frozen commit");
    assert!(frozen.frozen, "{source_status:#?}");
    let pending_status = source_status
        .handles
        .iter()
        .find(|handle| handle.id == pending)
        .expect("pending handle disappeared");
    assert!(pending_status.pending && !pending_status.frozen);

    let original_receipt = fixture.yy_ok(&[
        "--volume",
        text(&source_config),
        "receipt",
        &uncertain_commit.to_string(),
    ]);
    assert!(String::from_utf8_lossy(&original_receipt.stdout).contains("revision:"));
    fixture.stop_service();

    let destination_root = remote_root("mount-restore");
    copy_prefix(fixture.operator(), operator_for_root(&destination_root));
    let metadata_backup = fixture.path("metadata-backup.db");
    sqlite_backup(&fixture.path("metadata.db"), &metadata_backup);
    let restored_staging = fixture.path("mount-restored");
    fs::create_dir(&restored_staging).unwrap();
    sqlite_backup(
        &source_staging.join("staging.db"),
        &restored_staging.join("staging.db"),
    );
    assert!(
        !restored_staging.join("runtime.lock").exists(),
        "process lease files are not backup data"
    );

    let mut service = spawn_service(
        &fixture,
        &metadata_backup,
        &destination_root,
        SERVICE_TOKEN,
        fixture.service_address(),
    );
    let restored_config = fixture.path("mount-restored.json");
    write_volume_config(
        &restored_config,
        "mount-restored",
        &restored_staging,
        false,
        Some(fixture.service_address()),
    );
    let restored_status = inspect(&restored_staging);
    assert_eq!(
        restored_status
            .handles
            .iter()
            .find(|handle| handle.id == uncertain)
            .and_then(|handle| handle.frozen_commit_id),
        Some(uncertain_commit)
    );
    assert_eq!(
        restored_status
            .handles
            .iter()
            .find(|handle| handle.id == pending)
            .map(|handle| handle.length),
        Some(b"pending-accepted".len() as u64)
    );

    let credential_failure = fixture.yy_with_env(
        &[
            "--volume",
            text(&restored_config),
            "file",
            "fsync",
            &pending.to_string(),
        ],
        &[
            ("YINYANG_S3_ROOT", &destination_root),
            ("YINYANG_SERVICE_TOKEN", SERVICE_TOKEN),
            ("YINYANG_S3_ACCESS_KEY_ID", "invalid"),
            ("YINYANG_S3_SECRET_ACCESS_KEY", "invalid"),
        ],
    );
    assert!(
        !credential_failure.status.success(),
        "credential failure must reach the CLI"
    );
    let after_credential_failure = inspect(&restored_staging);
    assert!(
        after_credential_failure
            .handles
            .iter()
            .any(|handle| handle.id == pending && handle.pending),
        "credential failure must not discard accepted staging"
    );

    let pending_fsync = fixture.yy_with_env(
        &[
            "--volume",
            text(&restored_config),
            "file",
            "fsync",
            &pending.to_string(),
        ],
        &[
            ("YINYANG_S3_ROOT", &destination_root),
            ("YINYANG_SERVICE_TOKEN", SERVICE_TOKEN),
        ],
    );
    pending_fsync.success_or("publish restored pending bytes");
    let uncertain_fsync = fixture.yy_with_env(
        &[
            "--volume",
            text(&restored_config),
            "file",
            "fsync",
            &uncertain.to_string(),
        ],
        &[
            ("YINYANG_S3_ROOT", &destination_root),
            ("YINYANG_SERVICE_TOKEN", SERVICE_TOKEN),
        ],
    );
    uncertain_fsync.success_or("retry restored uncertain commit");
    assert!(
        String::from_utf8_lossy(&uncertain_fsync.stdout).contains(&uncertain_commit.to_string()),
        "retry must retain the exact commit identity: {}",
        String::from_utf8_lossy(&uncertain_fsync.stdout)
    );

    let destination_operator = operator_for_root(&destination_root);
    let authorization = service_authority(
        fixture.service_address(),
        SERVICE_TOKEN,
        destination_operator.clone(),
    );
    let receipt = runtime()
        .block_on(async {
            authorization
                .observe_latest()
                .await?
                .receipt(uncertain_commit)
                .await
        })
        .expect("query restored receipt");
    assert!(receipt.is_some(), "restored authority lost the receipt");
    let historical = runtime()
        .block_on(async {
            let snapshot = authorization.observe_revision(base.cursor.revision).await?;
            let node = snapshot
                .resolve("pending.txt")
                .await?
                .ok_or_else(|| "historical pending.txt disappeared".to_string())?;
            let file = snapshot.open_file(node.id()).await?;
            let mut bytes = Vec::new();
            file.read_range(0..file.descriptor().content_id().length(), &mut bytes)
                .await?;
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(bytes)
        })
        .expect("read restored historical revision");
    assert_eq!(historical, b"base");

    fixture
        .yy_with_env(
            &[
                "--volume",
                text(&restored_config),
                "file",
                "create",
                "outage.txt",
            ],
            &[
                ("YINYANG_S3_ROOT", &destination_root),
                ("YINYANG_SERVICE_TOKEN", SERVICE_TOKEN),
            ],
        )
        .success_or("create outage fixture");
    let outage = parse_handle(&fixture.yy_with_env(
        &[
            "--volume",
            text(&restored_config),
            "file",
            "open",
            "outage.txt",
            "--write",
        ],
        &[
            ("YINYANG_S3_ROOT", &destination_root),
            ("YINYANG_SERVICE_TOKEN", SERVICE_TOKEN),
        ],
    ));
    let outage_bytes = fixture.path("outage-input");
    fs::write(&outage_bytes, b"offline-accepted").unwrap();
    fixture
        .yy_with_env(
            &[
                "--volume",
                text(&restored_config),
                "file",
                "write",
                &outage.to_string(),
                text(&outage_bytes),
            ],
            &[
                ("YINYANG_S3_ROOT", &destination_root),
                ("YINYANG_SERVICE_TOKEN", SERVICE_TOKEN),
            ],
        )
        .success_or("stage outage bytes");
    service.stop();
    let outage_failure = fixture.yy_with_env(
        &[
            "--volume",
            text(&restored_config),
            "file",
            "fsync",
            &outage.to_string(),
        ],
        &[
            ("YINYANG_S3_ROOT", &destination_root),
            ("YINYANG_SERVICE_TOKEN", SERVICE_TOKEN),
        ],
    );
    assert!(!outage_failure.status.success());
    let staged = fixture.yy_with_env(
        &[
            "--volume",
            text(&restored_config),
            "file",
            "read",
            &outage.to_string(),
        ],
        &[
            ("YINYANG_S3_ROOT", &destination_root),
            ("YINYANG_SERVICE_TOKEN", SERVICE_TOKEN),
        ],
    );
    staged.success_or("read staged bytes during backend outage");
    assert_eq!(staged.stdout, b"offline-accepted");

    service = spawn_service(
        &fixture,
        &metadata_backup,
        &destination_root,
        SERVICE_TOKEN,
        fixture.service_address(),
    );
    fixture
        .yy_with_env(
            &[
                "--volume",
                text(&restored_config),
                "file",
                "fsync",
                &outage.to_string(),
            ],
            &[
                ("YINYANG_S3_ROOT", &destination_root),
                ("YINYANG_SERVICE_TOKEN", SERVICE_TOKEN),
            ],
        )
        .success_or("publish staged bytes after backend recovery");
    service.stop();
}

fn inspect(staging: &Path) -> RuntimeStatus {
    runtime()
        .block_on(Runtime::inspect(staging))
        .expect("inspect staging")
}

fn parse_handle(output: &Captured) -> uuid::Uuid {
    output
        .stdout
        .trim_ascii()
        .split(|byte| *byte == b'\n')
        .next()
        .and_then(|line| std::str::from_utf8(line).ok())
        .and_then(|line| line.parse().ok())
        .unwrap_or_else(|| {
            panic!(
                "yy did not print a handle:\n{}",
                String::from_utf8_lossy(&output.stdout)
            )
        })
}

fn write_volume_config(
    path: &Path,
    name: &str,
    staging: &Path,
    object: bool,
    service: Option<SocketAddr>,
) {
    let mut value = serde_json::json!({
        "name": name,
        "model": "managed",
        "access": "mount",
        "frontend": "library",
        "publication": if object { "object" } else { "service" },
        "storage_profile": "minio",
        "staging": staging,
        "read_only": false,
        "require": [],
    });
    if let Some(service) = service {
        value["service_address"] = serde_json::Value::String(service.to_string());
    }
    fs::write(path, serde_json::to_vec_pretty(&value).unwrap()).unwrap();
}

fn remote_root(label: &str) -> String {
    format!(
        "linux-recovery-{label}-{}-{}",
        std::process::id(),
        uuid::Uuid::new_v4().simple()
    )
}

fn operator_for_root(root: &str) -> opendal::Operator {
    opendal::install_default();
    let mut config = std::env::vars()
        .filter_map(|(key, value)| {
            key.strip_prefix("YINYANG_S3_")
                .map(|key| (key.to_ascii_lowercase(), value))
        })
        .collect::<Vec<_>>();
    config.retain(|(key, _)| key != "root");
    config.push(("root".to_owned(), root.to_owned()));
    opendal::Operator::via_iter("s3", config).expect("build isolated operator")
}

fn copy_prefix(source: opendal::Operator, destination: opendal::Operator) {
    runtime().block_on(async move {
        let entries = source
            .list_with("")
            .recursive(true)
            .await
            .expect("list source prefix");
        for entry in entries {
            if !entry.metadata().is_file() {
                continue;
            }
            let path = entry.path().to_owned();
            let bytes = source
                .read(&path)
                .await
                .expect("read source object")
                .to_vec();
            destination
                .write(&path, bytes)
                .await
                .expect("write restored object");
        }
    });
}

fn service_authority(
    address: SocketAddr,
    token: &str,
    operator: opendal::Operator,
) -> std::sync::Arc<dyn Authority> {
    runtime()
        .block_on(ServiceClient::connect(address, token.to_owned(), operator))
        .expect("connect restored service authority")
}

fn sqlite_backup(source: &Path, destination: &Path) {
    if let Some(parent) = destination.parent() {
        fs::create_dir_all(parent).unwrap();
    }
    let script = format!(
        ".backup '{}'",
        destination.display().to_string().replace('\'', "''")
    );
    let output = run_external(
        "sqlite3",
        &[
            &source.display().to_string(),
            &script,
            // The argument above is the complete SQLite command for a
            // consistent database snapshot.
        ],
        Duration::from_secs(60),
    );
    output.success_or("sqlite3 .backup");
    let check = run_external(
        "sqlite3",
        &[
            &destination.display().to_string(),
            "PRAGMA integrity_check;",
        ],
        Duration::from_secs(60),
    );
    check.success_or("sqlite3 integrity_check");
    assert_eq!(check.stdout.trim_ascii(), b"ok");
}

fn spawn_service(
    fixture: &Fixture,
    database: &Path,
    root: &str,
    token: &str,
    address: SocketAddr,
) -> ServiceProcess {
    let mut command = fixture.yy_command(&[
        "serve",
        "--database",
        text(database),
        "--listen",
        &address.to_string(),
    ]);
    command
        .env("YINYANG_S3_ROOT", root)
        .env("YINYANG_SERVICE_TOKEN", token)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = command.spawn().expect("spawn restored yy serve");
    let stdout = child.stdout.take().expect("service stdout is piped");
    let stderr = child.stderr.take().expect("service stderr is piped");
    let (sender, receiver) = mpsc::channel();
    let reader = thread::spawn(move || {
        let mut reader = BufReader::new(stdout);
        let mut line = String::new();
        let _ = reader.read_line(&mut line);
        let _ = sender.send(line);
    });
    let stderr_thread = thread::spawn(move || read_all(stderr));
    let line = receiver
        .recv_timeout(Duration::from_secs(60))
        .expect("restored service did not report readiness");
    assert!(
        line.starts_with("metadata service listening on "),
        "unexpected service readiness line: {line:?}"
    );
    let _ = reader.join();
    ServiceProcess {
        child,
        stderr: Some(stderr_thread),
    }
}

struct ServiceProcess {
    child: Child,
    stderr: Option<JoinHandle<Vec<u8>>>,
}

impl ServiceProcess {
    fn stop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        if let Some(thread) = self.stderr.take() {
            let stderr = thread.join().unwrap_or_default();
            assert!(
                String::from_utf8_lossy(&stderr).is_empty() || self.child.try_wait().is_ok(),
                "restored service stderr: {}",
                String::from_utf8_lossy(&stderr)
            );
        }
    }
}

impl Drop for ServiceProcess {
    fn drop(&mut self) {
        self.stop();
    }
}

fn read_all(mut reader: impl Read) -> Vec<u8> {
    let mut bytes = Vec::new();
    let _ = reader.read_to_end(&mut bytes);
    bytes
}

fn run_external(program: &str, args: &[&str], timeout: Duration) -> Captured {
    let mut command = Command::new(program);
    command
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    run_command(command, timeout)
}

fn run_command(mut command: Command, timeout: Duration) -> Captured {
    let mut child = command.spawn().expect("spawn command");
    let stdout = child.stdout.take().expect("stdout is piped");
    let stderr = child.stderr.take().expect("stderr is piped");
    let stdout_thread = thread::spawn(move || read_all(stdout));
    let stderr_thread = thread::spawn(move || read_all(stderr));
    let deadline = Instant::now() + timeout;
    let status = loop {
        match child.try_wait().expect("wait for command") {
            Some(status) => break status,
            None if Instant::now() < deadline => thread::sleep(Duration::from_millis(25)),
            None => {
                let _ = child.kill();
                let _ = child.wait();
                panic!("command timed out after {timeout:?}");
            }
        }
    };
    Captured {
        status,
        stdout: stdout_thread.join().unwrap_or_default(),
        stderr: stderr_thread.join().unwrap_or_default(),
    }
}

fn text(path: &Path) -> &str {
    path.to_str().expect("UTF-8 path")
}
