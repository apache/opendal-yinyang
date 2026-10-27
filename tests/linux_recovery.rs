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

use fixture::{Captured, EntryState, Fixture, Publication, RemoteEntry, SyncStatus, runtime};
use nix::unistd::{Gid, Uid};
use std::fs;
use std::io::{BufRead, BufReader, Read};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};
use yinyang::core::service::ServiceClient;
use yinyang::core::{Authority, CommitId, NodeId};
use yinyang::runtime::{Runtime, RuntimeStatus};

const SERVICE_TOKEN: &str = "linux-recovery-service-token-0123456789";

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
        &commit_text(uncertain_commit),
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
        String::from_utf8_lossy(&uncertain_fsync.stdout).contains(&commit_text(uncertain_commit)),
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

#[test]
#[ignore = "requires isolated YINYANG_S3_* storage and passwordless sudo for a bounded tmpfs"]
fn mount_staging_enospc_preserves_acknowledged_prefix_across_process_restart() {
    let fixture = Fixture::new(Publication::Service);
    fixture.remote_create_file("full.txt", b"base", false);
    let mountpoint = fixture.path("bounded-tmpfs");
    fs::create_dir(&mountpoint).unwrap();
    let tmpfs = Tmpfs::mount(&mountpoint, "8m");
    let staging = mountpoint.join("staging");
    let config = fixture.path("full.json");
    write_volume_config(
        &config,
        "full",
        &staging,
        false,
        Some(fixture.service_address()),
    );

    let handle = parse_handle(&fixture.yy_ok(&[
        "--volume",
        text(&config),
        "file",
        "open",
        "full.txt",
        "--write",
    ]));
    let acknowledged = fixture.path("acknowledged-input");
    fs::write(&acknowledged, b"acknowledged").unwrap();
    fixture.yy_ok(&[
        "--volume",
        text(&config),
        "file",
        "write",
        &handle.to_string(),
        text(&acknowledged),
    ]);
    let input = fixture.path("large-input");
    fs::write(&input, vec![b'x'; 12 * 1024 * 1024]).unwrap();
    let failed = fixture.yy(&[
        "--volume",
        text(&config),
        "file",
        "write",
        &handle.to_string(),
        text(&input),
        "--append",
    ]);
    let diagnostic = String::from_utf8_lossy(&failed.stderr).to_ascii_lowercase();
    assert!(
        !failed.status.success(),
        "12 MiB into an 8 MiB state filesystem must fail"
    );
    assert!(
        diagnostic.contains("full") || diagnostic.contains("space"),
        "the failure must surface the storage exhaustion: {diagnostic}"
    );

    let status = inspect(&staging);
    let length = status
        .handles
        .iter()
        .find(|candidate| candidate.id == handle)
        .map(|candidate| candidate.length)
        .expect("failed write discarded its staged handle");
    assert!(
        length > b"acknowledged".len() as u64,
        "no append prefix remained beyond the acknowledged write: {status:#?}"
    );
    assert!(
        length < 12 * 1024 * 1024 + b"acknowledged".len() as u64,
        "the simulated ENOSPC accepted the complete input"
    );
    assert_eq!(
        remote_entry_bytes(&fixture, "full.txt").as_deref(),
        Some(b"base".as_slice()),
        "failed append must not publish a partial remote version"
    );
    let read = fixture.yy_ok(&[
        "--volume",
        text(&config),
        "file",
        "read",
        &handle.to_string(),
        "--length",
        &length.to_string(),
    ]);
    assert_eq!(read.stdout.len(), usize::try_from(length).unwrap());
    assert!(read.stdout.starts_with(b"acknowledged"));
    assert!(
        read.stdout[b"acknowledged".len()..]
            .iter()
            .all(|byte| *byte == b'x')
    );
    tmpfs.resize("32m");
    fixture.yy_ok(&[
        "--volume",
        text(&config),
        "file",
        "fsync",
        &handle.to_string(),
    ]);
    assert_eq!(
        remote_entry_bytes(&fixture, "full.txt").as_deref(),
        Some(read.stdout.as_slice()),
        "explicit recovery fsync must publish the retained local prefix"
    );
    drop(tmpfs);
}

#[test]
#[ignore = "requires isolated YINYANG_S3_* storage and sqlite3"]
fn sync_backup_restore_preserves_rename_unknown_and_changed_artifacts() {
    let mut fixture = Fixture::new(Publication::Service);
    fixture.remote_create_file("rename.txt", b"rename-base", false);
    fixture.remote_create_file("conflict.txt", b"remote-base", false);
    let source = fixture.sync_client("sync-source");
    fixture.sync_run_ok(&source);

    fixture.remote_write_file("conflict.txt", b"remote-change");
    fs::write(source.directory.join("conflict.txt"), b"local-change").unwrap();
    fixture.sync_run_ok(&source);
    let conflict_status = fixture.sync_status(&source);
    let conflict = conflict_status.conflict("conflict.txt").clone();
    let artifact_id = conflict
        .local_artifact
        .clone()
        .expect("conflict must retain local bytes");
    let artifact_path = conflict_status
        .artifacts
        .iter()
        .find(|artifact| artifact.id == artifact_id)
        .expect("conflict artifact disappeared")
        .path
        .clone();
    fs::write(&artifact_path, b"changed-retained").unwrap();
    fs::write(
        source.directory.join("conflict.txt"),
        b"newer-local-conflict",
    )
    .unwrap();

    fixture.remote_create_file("unknown.txt", b"unknown-base", false);
    fixture.sync_run_ok(&source);
    fs::write(source.directory.join("unknown.txt"), b"uncertain").unwrap();
    let gate = fixture.service_gate(true);
    let gate_paths = fixture.sync_paths_at("sync-source", Some(gate.address()));
    let unknown_run = fixture.sync_run(&gate_paths);
    assert!(
        !unknown_run.status.success(),
        "an unknown Sync outcome must fail the run"
    );
    gate.wait_for_commit_response(Duration::from_secs(30))
        .expect("service gate did not observe the Sync commit response");
    drop(gate);
    let source = fixture.sync_paths_at("sync-source", None);
    let unknown_status = fixture.sync_status(&source);
    let unknown = unknown_status.entry("unknown.txt");
    assert_eq!(unknown.state, EntryState::Unknown);
    let unknown_commit = unknown
        .intent
        .commit_id
        .clone()
        .expect("unknown Sync work lost its commit identity");
    fs::write(source.directory.join("unknown.txt"), b"newer-unknown").unwrap();

    let rename_node = fixture.remote_file_version("rename.txt").0;
    fs::rename(
        source.directory.join("rename.txt"),
        source.directory.join("renamed.txt"),
    )
    .unwrap();
    let before_backup = fixture.sync_status(&source);
    assert_eq!(
        before_backup.entry("renamed.txt").state,
        EntryState::LocalPending
    );
    assert_eq!(before_backup.summary.unknown, 1);
    assert_eq!(
        before_backup.conflict("conflict.txt").local_artifact,
        Some(artifact_id.clone())
    );

    let backup = fixture.path("sync-backup");
    fixture.yy_ok(&[
        "--volume",
        text(&source.config),
        "sync",
        "backup",
        "--output",
        text(&backup),
    ]);
    fixture.stop_service();
    let destination_root_name = remote_root("sync-restore");
    copy_prefix(
        fixture.operator(),
        operator_for_root(&destination_root_name),
    );
    let metadata_backup = fixture.path("sync-metadata.db");
    sqlite_backup(&fixture.path("metadata.db"), &metadata_backup);

    let wrong = Fixture::new(Publication::Object);
    let wrong_state = wrong.path("wrong-state");
    let wrong_root = wrong.path("wrong-root");
    let wrong_config = wrong.path("wrong.json");
    write_sync_config(
        &wrong_config,
        "wrong",
        &wrong_root,
        &wrong_state,
        true,
        None,
    );
    let wrong_restore = wrong.yy(&[
        "--volume",
        text(&wrong_config),
        "sync",
        "restore",
        "--backup",
        text(&backup),
        "--directory",
        text(&wrong_root),
    ]);
    assert!(
        !wrong_restore.status.success(),
        "restore into another filesystem must fail"
    );
    assert!(
        !wrong_root.exists() && !wrong_state.exists(),
        "wrong-filesystem preflight must not create restore destinations"
    );

    let restored_state = fixture.path("sync-restored-state");
    let restored_root = fixture.path("sync-restored-root");
    let restored_config = fixture.path("sync-restored.json");
    write_sync_config(
        &restored_config,
        "sync-restored",
        &restored_root,
        &restored_state,
        false,
        Some(fixture.service_address()),
    );
    let mut service = spawn_service(
        &fixture,
        &metadata_backup,
        &destination_root_name,
        SERVICE_TOKEN,
        fixture.service_address(),
    );
    fixture
        .yy_with_env(
            &[
                "--volume",
                text(&restored_config),
                "sync",
                "restore",
                "--backup",
                text(&backup),
                "--directory",
                text(&restored_root),
            ],
            &[
                ("YINYANG_S3_ROOT", &destination_root_name),
                ("YINYANG_SERVICE_TOKEN", SERVICE_TOKEN),
            ],
        )
        .success_or("restore isolated Sync backup");
    let restored = sync_status_with_env(
        &fixture,
        &restored_config,
        &destination_root_name,
        SERVICE_TOKEN,
    );
    assert_eq!(restored.entry("unknown.txt").state, EntryState::Unknown);
    assert_eq!(
        restored.entry("unknown.txt").intent.commit_id.as_deref(),
        Some(unknown_commit.as_str())
    );
    assert_eq!(
        fs::read(restored_root.join("unknown.txt")).unwrap(),
        b"newer-unknown"
    );
    assert_eq!(
        fs::read(restored_root.join("conflict.txt")).unwrap(),
        b"newer-local-conflict"
    );
    assert_eq!(
        restored.conflict("conflict.txt").local_artifact.as_deref(),
        Some(artifact_id.as_str())
    );
    let artifact = restored
        .artifacts
        .iter()
        .find(|artifact| artifact.id == artifact_id)
        .expect("restored backup lost the changed artifact");
    assert_eq!(artifact.phase, fixture::ArtifactPhase::Conflict);
    let exported = fixture.path("restored-artifact");
    fixture
        .yy_with_env(
            &[
                "--volume",
                text(&restored_config),
                "sync",
                "export",
                "--artifact",
                &artifact_id,
                "--output",
                text(&exported),
            ],
            &[
                ("YINYANG_S3_ROOT", &destination_root_name),
                ("YINYANG_SERVICE_TOKEN", SERVICE_TOKEN),
            ],
        )
        .success_or("export changed retained artifact");
    assert_eq!(fs::read(&exported).unwrap(), b"changed-retained");

    service.stop();
    let outage = fixture.yy_with_env(
        &["--volume", text(&restored_config), "sync", "run"],
        &[
            ("YINYANG_S3_ROOT", &destination_root_name),
            ("YINYANG_SERVICE_TOKEN", SERVICE_TOKEN),
        ],
    );
    assert!(
        !outage.status.success(),
        "backend outage must fail Sync rather than report convergence"
    );
    let offline = sync_status_with_env(
        &fixture,
        &restored_config,
        &destination_root_name,
        SERVICE_TOKEN,
    );
    assert_eq!(
        offline.remote.availability,
        fixture::RemoteAvailability::Unavailable
    );
    assert_eq!(offline.summary.unknown, 1);
    assert_eq!(
        offline.entry("unknown.txt").intent.commit_id.as_deref(),
        Some(unknown_commit.as_str())
    );

    service = spawn_service(
        &fixture,
        &metadata_backup,
        &destination_root_name,
        SERVICE_TOKEN,
        fixture.service_address(),
    );
    let credential_failure = fixture.yy_with_env(
        &["--volume", text(&restored_config), "sync", "run"],
        &[
            ("YINYANG_S3_ROOT", &destination_root_name),
            ("YINYANG_SERVICE_TOKEN", SERVICE_TOKEN),
            ("YINYANG_S3_ACCESS_KEY_ID", "invalid"),
            ("YINYANG_S3_SECRET_ACCESS_KEY", "invalid"),
        ],
    );
    assert!(
        !credential_failure.status.success(),
        "credential failure must fail Sync"
    );
    let after_credentials = sync_status_with_env(
        &fixture,
        &restored_config,
        &destination_root_name,
        SERVICE_TOKEN,
    );
    assert_eq!(after_credentials.summary.unknown, 1);

    fixture
        .yy_with_env(
            &["--volume", text(&restored_config), "sync", "run"],
            &[
                ("YINYANG_S3_ROOT", &destination_root_name),
                ("YINYANG_SERVICE_TOKEN", SERVICE_TOKEN),
            ],
        )
        .success_or("retry restored Sync state");
    let converged = sync_status_with_env(
        &fixture,
        &restored_config,
        &destination_root_name,
        SERVICE_TOKEN,
    );
    assert_eq!(converged.summary.unknown, 0, "{converged:#?}");
    let authority = service_authority(
        fixture.service_address(),
        SERVICE_TOKEN,
        operator_for_root(&destination_root_name),
    );
    let receipt = runtime()
        .block_on(async {
            authority
                .observe_latest()
                .await?
                .receipt(CommitId::from_bytes(
                    *uuid::Uuid::parse_str(&unknown_commit).unwrap().as_bytes(),
                ))
                .await
        })
        .expect("query original unknown Sync receipt");
    assert!(receipt.is_some(), "restored Sync lost the original commit");
    assert_eq!(
        remote_node(&authority, "renamed.txt"),
        Some(rename_node),
        "restored identity mapping must preserve the unscanned rename"
    );
    assert!(remote_node(&authority, "rename.txt").is_none());
    assert_eq!(
        remote_bytes(&authority, "unknown.txt").as_deref(),
        Some(b"newer-unknown".as_slice())
    );
    assert_eq!(
        remote_bytes(&authority, "conflict.txt").as_deref(),
        Some(b"remote-change".as_slice())
    );
    service.stop();
}

fn inspect(staging: &Path) -> RuntimeStatus {
    runtime()
        .block_on(Runtime::inspect(staging))
        .expect("inspect staging")
}

fn write_sync_config(
    path: &Path,
    name: &str,
    root: &Path,
    state: &Path,
    object: bool,
    service: Option<SocketAddr>,
) {
    let mut value = serde_json::json!({
        "name": name,
        "model": "managed",
        "access": "sync",
        "frontend": "library",
        "publication": if object { "object" } else { "service" },
        "storage_profile": "minio",
        "staging": state,
        "read_only": false,
        "require": [],
    });
    if let Some(service) = service {
        value["service_address"] = serde_json::Value::String(service.to_string());
    }
    let _ = root;
    fs::write(path, serde_json::to_vec_pretty(&value).unwrap()).unwrap();
}

fn sync_status_with_env(fixture: &Fixture, config: &Path, root: &str, token: &str) -> SyncStatus {
    let output = fixture.yy_with_env(
        &["--volume", text(config), "sync", "status", "--json"],
        &[("YINYANG_S3_ROOT", root), ("YINYANG_SERVICE_TOKEN", token)],
    );
    output.success_or("restored sync status");
    let status: SyncStatus = serde_json::from_slice(&output.stdout).unwrap_or_else(|error| {
        panic!(
            "parse restored sync status: {error}\n{}",
            String::from_utf8_lossy(&output.stdout)
        )
    });
    status.validate();
    status
}

fn remote_node(authority: &std::sync::Arc<dyn Authority>, path: &str) -> Option<NodeId> {
    runtime()
        .block_on(async {
            let snapshot = authority.observe_latest().await?;
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(
                snapshot.resolve(path).await?.map(|node| node.id()),
            )
        })
        .expect("resolve restored remote node")
}

fn remote_bytes(authority: &std::sync::Arc<dyn Authority>, path: &str) -> Option<Vec<u8>> {
    runtime()
        .block_on(async {
            let snapshot = authority.observe_latest().await?;
            let Some(node) = snapshot.resolve(path).await? else {
                return Ok(None);
            };
            let file = snapshot.open_file(node.id()).await?;
            let mut bytes = Vec::new();
            file.read_range(0..file.descriptor().content_id().length(), &mut bytes)
                .await?;
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(Some(bytes))
        })
        .expect("read restored remote file")
}

fn remote_entry_bytes(fixture: &Fixture, path: &str) -> Option<Vec<u8>> {
    match fixture.remote_tree().ok()?.remove(path)? {
        RemoteEntry::File { bytes, .. } => Some(bytes),
        RemoteEntry::Directory { .. } => None,
    }
}

struct Tmpfs {
    path: PathBuf,
}

impl Tmpfs {
    fn mount(path: &Path, size: &str) -> Self {
        let option = format!("size={size}");
        run_external(
            "sudo",
            &[
                "-n",
                "mount",
                "-t",
                "tmpfs",
                "-o",
                &option,
                "tmpfs",
                text(path),
            ],
            Duration::from_secs(30),
        )
        .success_or("mount bounded tmpfs");
        let owner = format!("{}:{}", Uid::current().as_raw(), Gid::current().as_raw());
        run_external(
            "sudo",
            &["-n", "chown", &owner, text(path)],
            Duration::from_secs(30),
        )
        .success_or("own bounded tmpfs");
        Self {
            path: path.to_owned(),
        }
    }

    fn resize(&self, size: &str) {
        let option = format!("remount,size={size}");
        run_external(
            "sudo",
            &["-n", "mount", "-o", &option, text(&self.path)],
            Duration::from_secs(30),
        )
        .success_or("resize bounded tmpfs");
    }
}

impl Drop for Tmpfs {
    fn drop(&mut self) {
        let unmounted = run_external(
            "sudo",
            &["-n", "umount", text(&self.path)],
            Duration::from_secs(30),
        );
        if !unmounted.status.success() {
            let _ = run_external(
                "sudo",
                &["-n", "umount", "-l", text(&self.path)],
                Duration::from_secs(30),
            );
        }
    }
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

fn commit_text(commit: CommitId) -> String {
    uuid::Uuid::from_bytes(*commit.as_bytes()).to_string()
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
        .map(|client| std::sync::Arc::new(client) as std::sync::Arc<dyn Authority>)
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
