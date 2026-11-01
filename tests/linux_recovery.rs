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

use fixture::{
    ArtifactKind, Captured, EntryState, Fixture, Publication, RemoteEntry, SyncStatus, local_tree,
    runtime,
};
use nix::unistd::{Gid, Uid};
use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, mpsc};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};
use yinyang::core::service::ServiceClient;
use yinyang::core::{
    Authority, BackendProfile, CommitId, ErrorKind as CoreErrorKind, Fs, NodeId, Revision,
    RevisionState,
};
use yinyang::runtime::{Runtime, RuntimeStatus};

const SERVICE_TOKEN: &str = "linux-recovery-service-token-0123456789";
const MAX_HTTP_HEAD: usize = 64 * 1024;
const MAX_HTTP_BODY: usize = 16 * 1024 * 1024;

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
    let outage_status = inspect(&restored_staging);
    assert_eq!(
        outage_status
            .handles
            .iter()
            .find(|handle| handle.id == outage)
            .map(|handle| handle.length),
        Some(b"offline-accepted".len() as u64),
        "backend outage must retain the locally accepted bytes"
    );

    service = spawn_service(
        &fixture,
        &metadata_backup,
        &destination_root,
        SERVICE_TOKEN,
        fixture.service_address(),
    );
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
    staged.success_or("read staged bytes after backend recovery");
    assert_eq!(staged.stdout, b"offline-accepted");
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
    fixture.remote_create_file("replaced.txt", b"old-retained", false);
    let source = fixture.sync_client("sync-source");
    fixture.sync_run_ok(&source);

    fixture.remote_write_file("conflict.txt", b"remote-change");
    fixture.remote_write_file("replaced.txt", b"new-remote");
    fs::write(source.directory.join("conflict.txt"), b"local-change").unwrap();
    fixture.sync_run_ok(&source);
    let conflict_status = fixture.sync_status(&source);
    let conflict = conflict_status.conflict("conflict.txt").clone();
    let conflict_artifact = conflict
        .local_artifact
        .clone()
        .expect("conflict must retain local bytes");
    let replaced_artifact = conflict_status
        .artifacts
        .iter()
        .find(|artifact| {
            artifact.kind == ArtifactKind::ReplacedFile && artifact.origin == "replaced.txt"
        })
        .expect("remote replacement must retain the displaced inode")
        .clone();
    let artifact_id = replaced_artifact.id;
    let artifact_path = conflict_status
        .artifacts
        .iter()
        .find(|artifact| artifact.id == artifact_id)
        .expect("retained replacement artifact disappeared")
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
        Some(conflict_artifact.clone())
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
    assert_private_directory(&backup);
    assert_private_directory(&backup.join("root"));
    assert_private_directory(&backup.join("state"));
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
    write_sync_config(&wrong_config, "wrong", &wrong_state, true, None);
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
    assert_private_directory(&restored_root);
    assert_private_directory(&restored_state);
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
        Some(conflict_artifact.as_str())
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

#[test]
#[ignore = "requires isolated YINYANG_S3_* storage and sqlite3"]
fn retention_plan_protects_live_and_packaged_sync_baselines_read_only() {
    let fixture = Fixture::new(Publication::Object);
    let source = fixture.sync_client("history-source");
    fs::write(source.directory.join("first.txt"), b"first baseline").unwrap();
    fixture.sync_run_ok(&source);

    clear_ephemeral_locks(&source.state);
    let source_state_before = authoritative_tree(&source.state);
    let remote_before = remote_identities(&fixture.operator());
    let live = retention_plan(&fixture, &source.config, 1, &[], None);
    assert_plan_boundaries(&live);
    let live_baseline = live["protection"]["staging"]["replica"]["baseline"]["revision"]
        .as_str()
        .expect("live replica baseline revision")
        .to_owned();
    assert_eq!(live_baseline.len(), 48);
    assert_decision_reason(&live, &live_baseline, "replica_baseline");
    assert_eq!(
        authoritative_tree(&source.state),
        source_state_before,
        "live retention planning changed authoritative local state"
    );
    assert_no_ephemeral_locks(&source.state);
    assert_eq!(remote_identities(&fixture.operator()), remote_before);

    let backup = fixture.path("history-backup");
    fixture.yy_ok(&[
        "--volume",
        text(&source.config),
        "sync",
        "backup",
        "--output",
        text(&backup),
    ]);
    let backup_before = authoritative_tree(&backup);

    fs::write(source.directory.join("second.txt"), b"second baseline").unwrap();
    fixture.sync_run_ok(&source);
    let second = fixture.sync_status(&source);
    let second_revision = second
        .remote
        .revision
        .as_deref()
        .expect("second sync remote revision");
    assert_ne!(second_revision, live_baseline);

    clear_ephemeral_locks(&source.state);
    let source_state_before_backup_plans = authoritative_tree(&source.state);
    let remote_before_backup_plans = remote_identities(&fixture.operator());
    let observer = fixture.sync_paths("history-observer");
    assert!(!observer.directory.exists());
    assert!(!observer.state.exists());

    let bounded = retention_plan(&fixture, &observer.config, 1, &[], None);
    assert_plan_boundaries(&bounded);
    let prior = plan_decision(&bounded, &live_baseline);
    assert_eq!(prior["selected"], false);
    assert_reason_contains(prior, "outside_bounded_policy");

    // The package carries its own root payload. Planning must not reopen or
    // require the original live root once the package has been written.
    let moved_root = fixture.path("history-source-root-moved");
    fs::rename(&source.directory, &moved_root).unwrap();

    let corrupt = fixture.path("history-backup-corrupt");
    copy_directory(&backup, &corrupt);
    let manifest = corrupt.join("manifest.bin");
    let mut bytes = fs::read(&manifest).unwrap();
    assert!(!bytes.is_empty());
    bytes[0] ^= 0xff;
    fs::write(&manifest, bytes).unwrap();
    let corrupt_before = authoritative_tree(&corrupt);

    let protected = retention_plan(&fixture, &observer.config, 1, &[&backup, &corrupt], None);
    assert_plan_boundaries(&protected);
    assert_eq!(protected["state"], "partial");
    assert_eq!(
        protected["protection"]["backup_evidence"],
        "supplied_incomplete"
    );
    assert_eq!(protected["protection"]["backups"]["supplied"], 2);
    assert_eq!(protected["protection"]["backups"]["validated"], 1);
    let packages = protected["protection"]["backups"]["packages"]
        .as_array()
        .expect("backup package evidence");
    assert_eq!(packages.len(), 2);
    let valid = package_by_path(packages, &backup);
    assert_eq!(valid["state"], "validated");
    assert_eq!(valid["staging"]["binding"]["state"], "verified");
    assert_eq!(
        valid["staging"]["replica"]["baseline"]["revision"],
        live_baseline
    );
    let corrupt_package = package_by_path(packages, &corrupt);
    assert_eq!(corrupt_package["state"], "unavailable");
    assert!(corrupt_package["staging"].is_null());
    assert!(
        corrupt_package["error"]
            .as_str()
            .is_some_and(|error| !error.is_empty())
    );
    assert_decision_reason(&protected, &live_baseline, "backup_replica_baseline");
    assert!(
        protected["unresolved"]
            .as_array()
            .is_some_and(|unresolved| {
                unresolved.iter().any(|entry| {
                    entry["subject"] == "backup-package" && entry["state"] == "unavailable"
                })
            })
    );

    assert!(!source.directory.exists());
    assert!(moved_root.exists());
    assert!(!observer.state.exists());
    assert_no_ephemeral_locks(&source.state);
    assert_eq!(
        authoritative_tree(&source.state),
        source_state_before_backup_plans,
        "backup retention planning changed the unrelated live replica state"
    );
    assert_eq!(authoritative_tree(&backup), backup_before);
    assert_eq!(authoritative_tree(&corrupt), corrupt_before);
    assert_eq!(
        remote_identities(&fixture.operator()),
        remote_before_backup_plans
    );

    let foreign_root = remote_root("history-backup-foreign");
    fixture
        .yy_with_env(&["create"], &[("YINYANG_S3_ROOT", &foreign_root)])
        .success_or("create foreign Managed filesystem");
    let foreign_operator = operator_for_root(&foreign_root);
    let foreign_before = remote_identities(&foreign_operator);
    let foreign = fixture.sync_paths("history-foreign-observer");
    let foreign_plan = retention_plan(
        &fixture,
        &foreign.config,
        1,
        &[&backup],
        Some(&foreign_root),
    );
    assert_plan_boundaries(&foreign_plan);
    assert_eq!(foreign_plan["state"], "partial");
    assert_eq!(
        foreign_plan["protection"]["backup_evidence"],
        "supplied_incomplete"
    );
    assert_eq!(foreign_plan["protection"]["backups"]["supplied"], 1);
    assert_eq!(foreign_plan["protection"]["backups"]["validated"], 0);
    let package = &foreign_plan["protection"]["backups"]["packages"][0];
    assert_eq!(package["state"], "foreign");
    assert!(package["staging"].is_null());
    assert!(
        package["error"]
            .as_str()
            .is_some_and(|error| error.contains("does not match volume filesystem"))
    );
    assert_eq!(remote_identities(&foreign_operator), foreign_before);
    remove_prefix(&foreign_operator);
}

#[test]
#[ignore = "requires isolated YINYANG_S3_* storage and sqlite3"]
fn explicit_expiry_preserves_staged_and_backup_work_on_object_and_service() {
    for publication in [Publication::Object, Publication::Service] {
        explicit_expiry_case(publication);
    }
}

fn explicit_expiry_case(publication: Publication) {
    let legacy = legacy_yy_binary();
    let mut fixture = Fixture::new(publication);
    let zero = fixture.remote_create_file("protected.txt", b"version-zero", false);
    let backup_source = fixture.sync_client("expiry-backup-source");
    // Admit several remote-only revisions so the package records baseline
    // entries that are not also local Sync receipts.
    fixture.remote_create_file("expiry-baseline-only.txt", b"external-baseline", false);
    fixture.remote_create_file("expiry-latest-only.txt", b"external-latest", false);
    fixture.sync_run_ok(&backup_source);
    let backup = fixture.path("expiry-backup");
    fixture.yy_ok(&[
        "--volume",
        text(&backup_source.config),
        "sync",
        "backup",
        "--output",
        text(&backup),
    ]);
    let restored = fixture.sync_paths("expiry-backup-restore");
    let moved_source_root = fixture.path("expiry-backup-source-root-moved");
    fs::rename(&backup_source.directory, &moved_source_root).unwrap();
    fixture.yy_ok(&[
        "--volume",
        text(&restored.config),
        "sync",
        "restore",
        "--backup",
        text(&backup),
        "--directory",
        text(&restored.directory),
    ]);
    let restored_plan = retention_plan(&fixture, &restored.config, 1, &[], None);
    let baseline_target_text =
        restored_plan["protection"]["staging"]["replica"]["baseline"]["revision"]
            .as_str()
            .unwrap_or_else(|| panic!("restored replica baseline revision: {restored_plan}"))
            .to_owned();

    fixture.remote_create_file("expiry-handle-base.txt", b"handle-base", false);
    let handle_base_text = revision_text(
        runtime()
            .block_on(fixture_authority(&fixture).observe_latest())
            .unwrap()
            .revision(),
    );
    assert_ne!(
        baseline_target_text, handle_base_text,
        "backup baseline must be distinct from the live file-handle base"
    );

    let staging = fixture.path("expiry-staging");
    let config = fixture.path("expiry.json");
    write_volume_config(
        &config,
        "expiry",
        &staging,
        publication == Publication::Object,
        (publication == Publication::Service).then(|| fixture.service_address()),
    );
    let handle = parse_handle(&fixture.yy_ok(&[
        "--volume",
        text(&config),
        "file",
        "open",
        "protected.txt",
        "--write",
    ]));
    let published = fixture.path("expiry-published");
    fs::write(&published, b"version-one!").unwrap();
    fixture.yy_ok(&[
        "--volume",
        text(&config),
        "file",
        "write",
        &handle.to_string(),
        text(&published),
    ]);
    fixture.yy_ok(&[
        "--volume",
        text(&config),
        "file",
        "fsync",
        &handle.to_string(),
    ]);
    let pending = fixture.path("expiry-pending");
    fs::write(&pending, b"PEND").unwrap();
    fixture.yy_ok(&[
        "--volume",
        text(&config),
        "file",
        "write",
        &handle.to_string(),
        text(&pending),
        "--offset",
        "4",
    ]);

    let zero_text = revision_text(zero.cursor.revision);
    assert_ne!(
        baseline_target_text, zero_text,
        "baseline refusal target must differ from the later expiry target"
    );
    let refusal_cases = [
        (&config, Some(backup.as_path()), "backup_replica_baseline"),
        (&restored.config, None, "replica_baseline"),
    ];
    for (config, backup, reason) in refusal_cases {
        let before = runtime()
            .block_on(fixture_authority(&fixture).observe_latest())
            .unwrap()
            .revision();
        assert_ne!(
            baseline_target_text,
            revision_text(before),
            "baseline refusal target must not be the current revision"
        );
        let mut args = vec![
            "--volume",
            text(config),
            "space",
            "retention",
            "expire",
            "--revision",
            &baseline_target_text,
        ];
        if let Some(backup) = backup {
            args.extend(["--backup", text(backup)]);
        }
        args.push("--json");
        let refusal = fixture.yy(&args);
        assert_eq!(
            refusal.status.code(),
            Some(2),
            "{reason} refusal failed:\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&refusal.stdout),
            String::from_utf8_lossy(&refusal.stderr)
        );
        let refusal: serde_json::Value =
            serde_json::from_slice(&refusal.stdout).expect("parse baseline refusal JSON");
        assert_eq!(refusal["state"], "refused", "{reason}");
        assert_eq!(refusal["revisions"][0]["state"], "protected", "{reason}");
        assert_eq!(
            refusal["revisions"][0]["reasons"],
            serde_json::json!([reason]),
            "{reason}"
        );
        let after = runtime()
            .block_on(fixture_authority(&fixture).observe_latest())
            .unwrap()
            .revision();
        assert_eq!(after, before, "{reason} refusal advanced the head");
    }
    assert!(
        runtime()
            .block_on(fixture_authority(&fixture).observe_revision(zero.cursor.revision),)
            .is_ok(),
        "backup-only refusal changed the unprotected target revision"
    );

    if let Some(binary) = &legacy {
        let latest = fixture.remote_file_version("protected.txt").2;
        legacy_pre_expiry(
            binary,
            &mut fixture,
            publication,
            zero.cursor.revision,
            zero.commit_id,
            latest,
        );
    }

    let immutable_before = immutable_remote_identities(&fixture.operator());
    let expiry = fixture.yy_ok(&[
        "--volume",
        text(&config),
        "space",
        "retention",
        "expire",
        "--revision",
        &zero_text,
        "--backup",
        text(&backup),
        "--json",
    ]);
    let report: serde_json::Value =
        serde_json::from_slice(&expiry.stdout).expect("parse expiry JSON");
    assert_eq!(report["schema"], "yinyang.managed-retention.expiry.v1");
    assert_eq!(report["state"], "committed");
    assert_eq!(report["revisions"][0]["state"], "expired");
    assert_eq!(
        report["evidence_digest"]
            .as_str()
            .expect("expiry evidence digest")
            .len(),
        64
    );
    assert_eq!(report["deletion_authorized"], false);
    assert!(report["reclaimable_bytes"].is_null());
    assert_eq!(report["physical_cleanup"], "not_performed");
    let space = fixture.yy_ok(&["--volume", text(&config), "space", "report", "--json"]);
    let space: serde_json::Value =
        serde_json::from_slice(&space.stdout).expect("parse post-expiry space report");
    assert_eq!(space["logical"]["retained"]["state"], "available");
    assert_eq!(space["logical"]["retained"]["expired_revision_count"], 1);
    assert!(
        space["logical"]["retained"]["expired_revisions"]
            .as_array()
            .is_some_and(|revisions| {
                revisions
                    .iter()
                    .any(|revision| revision == zero_text.as_str())
            })
    );
    let plan = fixture.yy_ok(&[
        "--volume",
        text(&config),
        "space",
        "retention",
        "plan",
        "--json",
    ]);
    let plan: serde_json::Value =
        serde_json::from_slice(&plan.stdout).expect("parse post-expiry retention plan");
    assert_eq!(plan["state"], "available");
    assert_eq!(plan["policy"]["published_history"], "readable_by_default");
    assert_eq!(plan["expired_revision_count"], 1);
    let expired = plan["revisions"]
        .as_array()
        .expect("post-expiry plan revisions")
        .iter()
        .find(|revision| revision["revision"] == zero_text.as_str())
        .expect("expired revision in plan");
    assert_eq!(expired["state"], "expired");
    assert_eq!(expired["selected"], false);
    assert_eq!(expired["reasons"], serde_json::json!(["expired"]));

    let authority = fixture_authority(&fixture);
    assert_eq!(
        runtime()
            .block_on(authority.observe_revision(zero.cursor.revision))
            .unwrap_err()
            .kind(),
        CoreErrorKind::Expired
    );
    assert_eq!(
        runtime()
            .block_on(authority.observe_revision(Revision::from_bytes([0x7f; 24])))
            .unwrap_err()
            .kind(),
        CoreErrorKind::NotFound
    );
    let history = runtime()
        .block_on(authority.history_page(None, yinyang::core::RETAINED_REVISIONS_PAGE_MAX))
        .unwrap();
    assert!(history.entries.iter().any(|entry| {
        entry.revision == zero.cursor.revision && matches!(entry.state, RevisionState::Expired(_))
    }));

    if let Some(binary) = &legacy {
        legacy_post_expiry(binary, &mut fixture, publication, &zero_text);
    }

    let mut expected = b"version-one!".to_vec();
    expected[4..8].copy_from_slice(b"PEND");
    let read = fixture.yy_ok(&[
        "--volume",
        text(&config),
        "file",
        "read",
        &handle.to_string(),
        "--length",
        &expected.len().to_string(),
    ]);
    assert_eq!(read.stdout, expected);
    fixture.yy_ok(&[
        "--volume",
        text(&config),
        "file",
        "fsync",
        &handle.to_string(),
    ]);
    assert_eq!(
        remote_entry_bytes(&fixture, "protected.txt").as_deref(),
        Some(expected.as_slice()),
        "{publication:?} did not publish recovered protected work"
    );

    let authority = fixture_authority(&fixture);
    assert_eq!(
        runtime()
            .block_on(authority.observe_revision(zero.cursor.revision))
            .unwrap_err()
            .kind(),
        CoreErrorKind::Expired,
        "{publication:?} lost the expiry marker after a normal commit"
    );
    let immutable_after = immutable_remote_identities(&fixture.operator());
    for identity in &immutable_before {
        assert!(
            immutable_after.contains(identity),
            "{publication:?} deleted or rewrote pre-existing immutable content: {identity:?}"
        );
    }

    let before_repeat = runtime()
        .block_on(authority.observe_latest())
        .unwrap()
        .revision();
    let repeated = fixture.yy_ok(&[
        "--volume",
        text(&config),
        "space",
        "retention",
        "expire",
        "--revision",
        &zero_text,
        "--backup",
        text(&backup),
        "--json",
    ]);
    let repeated: serde_json::Value =
        serde_json::from_slice(&repeated.stdout).expect("parse repeated expiry JSON");
    assert_eq!(repeated["state"], "already_expired");
    assert_eq!(
        runtime()
            .block_on(authority.observe_latest())
            .unwrap()
            .revision(),
        before_repeat,
        "idempotent expiry advanced the head"
    );
}

#[test]
#[ignore = "requires isolated YINYANG_S3_* storage and sqlite3"]
fn object_publication_journal_restore_preserves_history_and_unknown() {
    let fixture = Fixture::new(Publication::Object);
    let history = fixture.path("history-source");
    fs::create_dir(&history).unwrap();
    fs::write(history.join("version.txt"), b"history-v1").unwrap();
    let first_tree = local_tree(&history).unwrap();
    fixture.yy_ok(&["publish", text(&history)]);
    let first_status = fixture.yy_ok(&["status"]);
    let first_revision = String::from_utf8_lossy(&first_status.stdout)
        .lines()
        .find_map(|line| line.strip_prefix("revision: ").map(str::to_owned))
        .expect("first publish did not report a revision");
    fs::write(history.join("version.txt"), b"history-v2").unwrap();
    fixture.yy_ok(&["publish", text(&history), "--replace"]);

    let source = fixture.path("journal-source");
    fs::create_dir_all(source.join("nested")).unwrap();
    fs::write(source.join("accepted.txt"), b"accepted-before-backup").unwrap();
    fs::write(source.join("nested/payload"), b"nested").unwrap();
    let accepted_tree = local_tree(&source).unwrap();
    let journal = fixture.path("journal-source-state");
    let commit = uuid::Uuid::new_v4().to_string();
    let gate = S3DropGate::start();
    let mut command = fixture.yy_command(&[
        "publish",
        text(&source),
        "--replace",
        "--state",
        text(&journal),
        "--commit-id",
        &commit,
    ]);
    command.env("YINYANG_S3_ENDPOINT", gate.endpoint());
    let lost = run_command(command, Duration::from_secs(180));
    assert_eq!(
        lost.status.code(),
        Some(3),
        "lost commit response must remain Uncertain\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&lost.stdout),
        String::from_utf8_lossy(&lost.stderr)
    );
    gate.wait_intercepted();
    drop(gate);
    fs::write(source.join("accepted.txt"), b"newer-local-source").unwrap();

    let restored_state = fixture.path("journal-restored-state");
    fs::create_dir(&restored_state).unwrap();
    sqlite_backup(
        &journal.join("directory.db"),
        &restored_state.join("directory.db"),
    );
    let destination_root = remote_root("journal-restore");
    copy_prefix(fixture.operator(), operator_for_root(&destination_root));
    let inspection = fixture.yy_ok(&[
        "publication",
        "inspect",
        &commit,
        "--state",
        text(&restored_state),
        "--json",
    ]);
    let inspection: serde_json::Value = serde_json::from_slice(&inspection.stdout).unwrap();
    assert_eq!(inspection["intents"][0]["state"], "unknown");
    assert_eq!(inspection["intents"][0]["commit_id"], commit);

    let exported = fixture.path("journal-exported");
    fixture
        .yy_with_env(
            &[
                "publication",
                "export",
                &commit,
                "--state",
                text(&restored_state),
                "--destination",
                text(&exported),
            ],
            &[("YINYANG_S3_ROOT", &destination_root)],
        )
        .success_or("export accepted publication bytes after restore");
    assert_eq!(local_tree(&exported).unwrap(), accepted_tree);

    let retried = fixture.yy_with_env(
        &[
            "publication",
            "retry",
            &commit,
            "--state",
            text(&restored_state),
        ],
        &[("YINYANG_S3_ROOT", &destination_root)],
    );
    retried.success_or("retry restored publication journal");
    assert!(
        String::from_utf8_lossy(&retried.stdout).contains(&commit),
        "restored retry must preserve the original commit: {}",
        String::from_utf8_lossy(&retried.stdout)
    );
    fixture
        .yy_with_env(
            &["receipt", &commit],
            &[("YINYANG_S3_ROOT", &destination_root)],
        )
        .success_or("query restored publication receipt");
    let restored_history = fixture.path("journal-history");
    fixture
        .yy_with_env(
            &[
                "restore",
                text(&restored_history),
                "--revision",
                &first_revision,
            ],
            &[("YINYANG_S3_ROOT", &destination_root)],
        )
        .success_or("restore historical revision from isolated object copy");
    assert_eq!(local_tree(&restored_history).unwrap(), first_tree);
}

fn inspect(staging: &Path) -> RuntimeStatus {
    runtime()
        .block_on(Runtime::inspect(staging))
        .expect("inspect staging")
}

fn assert_private_directory(path: &Path) {
    let mode = fs::metadata(path)
        .unwrap_or_else(|error| panic!("inspect {}: {error}", path.display()))
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(
        mode,
        0o700,
        "recovery output {} must not expose pending payloads",
        path.display()
    );
}

#[derive(Debug, Eq, Ord, PartialEq, PartialOrd)]
struct RemoteIdentity {
    path: String,
    length: u64,
    etag: String,
}

fn retention_plan(
    fixture: &Fixture,
    config: &Path,
    keep_last: u64,
    backups: &[&Path],
    root: Option<&str>,
) -> serde_json::Value {
    let mut args = vec![
        "--volume".to_owned(),
        text(config).to_owned(),
        "space".to_owned(),
        "retention".to_owned(),
        "plan".to_owned(),
        "--keep-last".to_owned(),
        keep_last.to_string(),
    ];
    for backup in backups {
        args.push("--backup".to_owned());
        args.push(text(backup).to_owned());
    }
    args.push("--json".to_owned());
    let args = args.iter().map(String::as_str).collect::<Vec<_>>();
    let output = match root {
        Some(root) => fixture.yy_with_env(&args, &[("YINYANG_S3_ROOT", root)]),
        None => fixture.yy(&args),
    };
    output.success_or("space retention plan with history protection");
    serde_json::from_slice(&output.stdout).unwrap_or_else(|error| {
        panic!(
            "parse retention plan JSON: {error}\n{}",
            String::from_utf8_lossy(&output.stdout)
        )
    })
}

fn assert_plan_boundaries(plan: &serde_json::Value) {
    assert_eq!(plan["schema"], "yinyang.managed-retention.plan.v3");
    assert_eq!(plan["deletion_authorized"], false);
    assert!(plan["reclaimable_bytes"].is_null());
    assert_eq!(plan["policy"]["published_history"], "readable_by_default");
}

fn plan_decision<'a>(plan: &'a serde_json::Value, revision: &str) -> &'a serde_json::Value {
    plan["revisions"]
        .as_array()
        .expect("retention plan revisions")
        .iter()
        .find(|entry| entry["revision"] == revision)
        .unwrap_or_else(|| panic!("retention plan has no revision {revision}: {plan}"))
}

fn assert_decision_reason(plan: &serde_json::Value, revision: &str, reason: &str) {
    let decision = plan_decision(plan, revision);
    assert_eq!(
        decision["selected"], true,
        "revision {revision} was not selected for {reason}: {plan}"
    );
    assert_reason_contains(decision, reason);
}

fn assert_reason_contains(decision: &serde_json::Value, reason: &str) {
    assert!(
        decision["reasons"]
            .as_array()
            .is_some_and(|reasons| reasons.iter().any(|value| value == reason)),
        "revision decision does not contain {reason}: {decision}"
    );
}

fn package_by_path<'a>(packages: &'a [serde_json::Value], path: &Path) -> &'a serde_json::Value {
    packages
        .iter()
        .find(|package| package["path"] == text(path))
        .unwrap_or_else(|| panic!("backup package {} is absent", path.display()))
}

fn clear_ephemeral_locks(staging: &Path) {
    for relative in ["replica.lock", "runtime.lock", "content/runtime.lock"] {
        match fs::remove_file(staging.join(relative)) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => panic!("remove ephemeral lease {relative}: {error}"),
        }
    }
}

fn assert_no_ephemeral_locks(staging: &Path) {
    for relative in ["replica.lock", "runtime.lock", "content/runtime.lock"] {
        assert!(
            !staging.join(relative).exists(),
            "read-only planning created writer lease {relative} under {}",
            staging.display()
        );
    }
}

fn authoritative_tree(root: &Path) -> fixture::Tree {
    let mut tree = local_tree(root).expect("read local state identity");
    tree.retain(|path, entry| {
        if path.ends_with("-shm") {
            return false;
        }
        if path.ends_with("-wal")
            && let fixture::RemoteEntry::File { bytes, .. } = entry
        {
            return !bytes.is_empty();
        }
        true
    });
    tree
}

fn copy_directory(source: &Path, destination: &Path) {
    let metadata = fs::symlink_metadata(source).expect("inspect source directory");
    assert!(
        metadata.is_dir(),
        "{} must be a directory",
        source.display()
    );
    fs::create_dir(destination).expect("create copied directory");
    fs::set_permissions(destination, metadata.permissions()).expect("copy directory mode");
    for entry in fs::read_dir(source).expect("read source directory") {
        let entry = entry.expect("source directory entry");
        let source = entry.path();
        let destination = destination.join(entry.file_name());
        let metadata = fs::symlink_metadata(&source).expect("inspect source entry");
        if metadata.is_dir() {
            copy_directory(&source, &destination);
        } else if metadata.is_file() {
            fs::copy(&source, &destination).expect("copy backup file");
            fs::set_permissions(&destination, metadata.permissions()).expect("copy file mode");
        } else {
            panic!("backup contains unsupported entry {}", source.display());
        }
    }
}

fn remote_identities(operator: &opendal::Operator) -> Vec<RemoteIdentity> {
    let mut identities = runtime().block_on(async {
        operator
            .list_with("")
            .recursive(true)
            .await
            .expect("list remote object identities")
            .into_iter()
            .filter(|entry| entry.metadata().is_file())
            .map(|entry| RemoteIdentity {
                path: entry.path().to_owned(),
                length: entry.metadata().content_length(),
                etag: entry
                    .metadata()
                    .etag()
                    .expect("isolated acceptance LIST must expose an ETag")
                    .to_owned(),
            })
            .collect::<Vec<_>>()
    });
    identities.sort();
    identities
}

fn immutable_remote_identities(operator: &opendal::Operator) -> Vec<RemoteIdentity> {
    remote_identities(operator)
        .into_iter()
        .filter(|identity| identity.path != ".yinyang/head")
        .collect()
}

fn remove_prefix(operator: &opendal::Operator) {
    runtime().block_on(async {
        let entries = operator
            .list_with("")
            .recursive(true)
            .await
            .expect("list temporary prefix");
        for entry in entries {
            if entry.metadata().is_file() {
                operator
                    .delete(entry.path())
                    .await
                    .expect("delete temporary prefix object");
            }
        }
    });
}

fn write_sync_config(
    path: &Path,
    name: &str,
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

struct S3DropGate {
    address: SocketAddr,
    intercepted: mpsc::Receiver<Result<(), String>>,
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

impl S3DropGate {
    fn start() -> Self {
        let upstream = upstream_endpoint();
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind S3 response-loss gate");
        let address = listener
            .local_addr()
            .expect("S3 response-loss gate address");
        let intercepted = Arc::new(AtomicBool::new(false));
        let poisoned = Arc::new(AtomicBool::new(false));
        let stop = Arc::new(AtomicBool::new(false));
        let (sender, receiver) = mpsc::channel();
        let thread_stop = stop.clone();
        let thread = thread::spawn(move || {
            for incoming in listener.incoming() {
                if thread_stop.load(Ordering::SeqCst) {
                    break;
                }
                let Ok(client) = incoming else {
                    break;
                };
                let intercepted = intercepted.clone();
                let poisoned = poisoned.clone();
                let sender = sender.clone();
                thread::spawn(move || {
                    handle_dropped_s3_response(client, upstream, intercepted, poisoned, sender);
                });
            }
        });
        Self {
            address,
            intercepted: receiver,
            stop,
            thread: Some(thread),
        }
    }

    fn endpoint(&self) -> String {
        format!("http://{}", self.address)
    }

    fn wait_intercepted(&self) {
        self.intercepted
            .recv_timeout(Duration::from_secs(90))
            .expect("S3 gate did not intercept a commit")
            .unwrap_or_else(|error| panic!("S3 gate commit failed: {error}"));
    }
}

impl Drop for S3DropGate {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        let _ = TcpStream::connect(self.address);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

fn handle_dropped_s3_response(
    mut client: TcpStream,
    upstream: SocketAddr,
    intercepted: Arc<AtomicBool>,
    poisoned: Arc<AtomicBool>,
    sender: mpsc::Sender<Result<(), String>>,
) {
    if poisoned.load(Ordering::SeqCst) {
        return;
    }
    let Ok((head, request_line, headers)) = read_http_head(&mut client) else {
        return;
    };
    let is_commit = is_head_commit(&request_line) && !intercepted.swap(true, Ordering::SeqCst);
    if has_expect_continue(&headers) {
        let _ = client.write_all(b"HTTP/1.1 100 Continue\r\n\r\n");
    }
    let Ok(body) = read_http_body(&mut client, &headers) else {
        return;
    };
    let Ok(mut server) = TcpStream::connect(upstream) else {
        return;
    };
    if server.write_all(&head).is_err() || server.write_all(&body).is_err() {
        return;
    }
    let head_only = request_line
        .split_whitespace()
        .next()
        .is_some_and(|method| method == "HEAD");
    let Ok(response) = read_http_response(&mut server, head_only) else {
        return;
    };
    if is_commit {
        if response.status == 200 {
            poisoned.store(true, Ordering::SeqCst);
            let _ = sender.send(Ok(()));
        } else {
            let _ = sender.send(Err(format!(
                "conditional head PUT returned HTTP {}, expected 200",
                response.status
            )));
        }
        let _ = client.shutdown(Shutdown::Both);
        return;
    }
    let response = with_connection_close(&response.bytes);
    let _ = client.write_all(&response);
    let _ = client.shutdown(Shutdown::Both);
}

fn read_http_head(
    stream: &mut TcpStream,
) -> std::io::Result<(Vec<u8>, String, Vec<(String, String)>)> {
    let mut bytes = Vec::new();
    while !bytes.ends_with(b"\r\n\r\n") {
        if bytes.len() == MAX_HTTP_HEAD {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "HTTP request head exceeds 64 KiB",
            ));
        }
        let mut byte = [0_u8; 1];
        stream.read_exact(&mut byte)?;
        bytes.push(byte[0]);
    }
    let text = std::str::from_utf8(&bytes)
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidData, "non-UTF-8 HTTP head"))?;
    let mut lines = text.split("\r\n");
    let request_line = lines.next().unwrap_or_default().to_owned();
    let headers = lines
        .filter(|line| !line.is_empty())
        .filter_map(|line| line.split_once(':'))
        .map(|(name, value)| (name.trim().to_ascii_lowercase(), value.trim().to_owned()))
        .collect();
    Ok((bytes, request_line, headers))
}

struct HttpResponse {
    status: u16,
    bytes: Vec<u8>,
}

fn read_http_response(stream: &mut TcpStream, head_only: bool) -> std::io::Result<HttpResponse> {
    loop {
        let (head, status_line, headers) = read_http_head(stream)?;
        let status = status_line
            .split_whitespace()
            .nth(1)
            .and_then(|value| value.parse::<u16>().ok())
            .ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, "missing HTTP status")
            })?;
        let body = if head_only || matches!(status, 204 | 304) {
            Vec::new()
        } else {
            read_http_body(stream, &headers)?
        };
        if !(100..200).contains(&status) {
            let mut response = head;
            response.extend(body);
            return Ok(HttpResponse {
                status,
                bytes: response,
            });
        }
    }
}

fn read_http_body(
    stream: &mut TcpStream,
    headers: &[(String, String)],
) -> std::io::Result<Vec<u8>> {
    if header(headers, "transfer-encoding").is_some_and(|value| value.contains("chunked")) {
        return read_chunked(stream);
    }
    let length = header(headers, "content-length")
        .map(|value| {
            value.parse::<usize>().map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid content length")
            })
        })
        .transpose()?
        .unwrap_or(0);
    if length > MAX_HTTP_BODY {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "HTTP body exceeds 16 MiB gate bound",
        ));
    }
    let mut body = vec![0; length];
    stream.read_exact(&mut body)?;
    Ok(body)
}

fn read_chunked(stream: &mut TcpStream) -> std::io::Result<Vec<u8>> {
    let mut body = Vec::new();
    loop {
        let line = read_line(stream)?;
        let size = line
            .split(';')
            .next()
            .and_then(|value| usize::from_str_radix(value, 16).ok())
            .ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid chunk size")
            })?;
        body.extend_from_slice(line.as_bytes());
        body.extend_from_slice(b"\r\n");
        if size == 0 {
            loop {
                let trailer = read_line(stream)?;
                body.extend_from_slice(trailer.as_bytes());
                body.extend_from_slice(b"\r\n");
                if trailer.is_empty() {
                    return Ok(body);
                }
            }
        }
        if body.len().saturating_add(size) > MAX_HTTP_BODY {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "chunked HTTP body exceeds 16 MiB gate bound",
            ));
        }
        let mut chunk = vec![0; size + 2];
        stream.read_exact(&mut chunk)?;
        body.extend_from_slice(&chunk);
    }
}

fn read_line(stream: &mut TcpStream) -> std::io::Result<String> {
    let mut line = Vec::new();
    loop {
        let mut byte = [0_u8; 1];
        stream.read_exact(&mut byte)?;
        line.push(byte[0]);
        if line.ends_with(b"\r\n") {
            line.truncate(line.len() - 2);
            return String::from_utf8(line).map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, "non-UTF-8 HTTP line")
            });
        }
        if line.len() > MAX_HTTP_HEAD {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "HTTP line exceeds 64 KiB",
            ));
        }
    }
}

fn with_connection_close(response: &[u8]) -> Vec<u8> {
    let Some(boundary) = response.windows(4).position(|window| window == b"\r\n\r\n") else {
        return response.to_vec();
    };
    let Ok(head) = std::str::from_utf8(&response[..boundary]) else {
        return response.to_vec();
    };
    let mut output = Vec::with_capacity(response.len() + 19);
    for line in head.split("\r\n") {
        if !line
            .split_once(':')
            .is_some_and(|(name, _)| name.eq_ignore_ascii_case("connection"))
        {
            output.extend_from_slice(line.as_bytes());
            output.extend_from_slice(b"\r\n");
        }
    }
    output.extend_from_slice(b"Connection: close\r\n\r\n");
    output.extend_from_slice(&response[boundary + 4..]);
    output
}

fn header<'a>(headers: &'a [(String, String)], name: &str) -> Option<&'a str> {
    headers
        .iter()
        .find(|(key, _)| key == name)
        .map(|(_, value)| value.as_str())
}

fn has_expect_continue(headers: &[(String, String)]) -> bool {
    header(headers, "expect").is_some_and(|value| value.eq_ignore_ascii_case("100-continue"))
}

fn is_head_commit(request_line: &str) -> bool {
    let mut parts = request_line.split_whitespace();
    parts.next() == Some("PUT")
        && parts.next().is_some_and(|target| {
            target
                .split('?')
                .next()
                .is_some_and(|path| path.ends_with("/.yinyang/head"))
        })
}

fn upstream_endpoint() -> SocketAddr {
    let endpoint = std::env::var("YINYANG_S3_ENDPOINT")
        .expect("YINYANG_S3_ENDPOINT is required by recovery acceptance");
    endpoint
        .strip_prefix("http://")
        .expect("the S3 gate supports the isolated plain-HTTP fixture")
        .split('/')
        .next()
        .expect("S3 endpoint authority")
        .parse()
        .expect("S3 endpoint must be an IP socket address")
}

fn commit_text(commit: CommitId) -> String {
    uuid::Uuid::from_bytes(*commit.as_bytes()).to_string()
}

fn revision_text(revision: Revision) -> String {
    revision
        .to_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn legacy_yy_binary() -> Option<PathBuf> {
    let Some(path) = std::env::var_os("YY_LEGACY_EXPIRY_BIN") else {
        eprintln!("legacy expiry qualification: not run (YY_LEGACY_EXPIRY_BIN is unset)");
        return None;
    };
    let path = PathBuf::from(path);
    assert!(
        path.is_file(),
        "YY_LEGACY_EXPIRY_BIN must name the recorded legacy yy binary: {}",
        path.display()
    );
    let hash = run_external("sha256sum", &[text(&path)], Duration::from_secs(30));
    let hash = std::str::from_utf8(&hash.stdout)
        .unwrap()
        .split_ascii_whitespace()
        .next()
        .expect("sha256sum output");
    eprintln!(
        "legacy expiry qualification: running {} sha256={} source={}",
        path.display(),
        hash,
        std::env::var("YY_LEGACY_EXPIRY_SOURCE").unwrap_or_else(|_| "unspecified".to_owned())
    );
    Some(path)
}

fn legacy_yy(binary: &Path, fixture: &Fixture, args: &[&str]) -> Captured {
    run_command(
        legacy_yy_command(binary, fixture, args),
        Duration::from_secs(60),
    )
}

fn legacy_pre_expiry(
    binary: &Path,
    fixture: &mut Fixture,
    publication: Publication,
    zero: Revision,
    zero_commit: CommitId,
    latest: Revision,
) {
    let head_before = runtime()
        .block_on(fixture_authority(fixture).observe_latest())
        .unwrap()
        .revision();
    let zero_text = revision_text(zero);
    match publication {
        Publication::Object => {
            legacy_yy(binary, fixture, &["status", "--revision", &zero_text])
                .success_or("legacy object status before expiry");
            let read_path = fixture.path("legacy-pre-expiry-read");
            legacy_yy(
                binary,
                fixture,
                &["restore", text(&read_path), "--revision", &zero_text],
            )
            .success_or("legacy object read before expiry");
            assert_eq!(
                fs::read(read_path.join("protected.txt")).unwrap(),
                b"version-zero"
            );
        }
        Publication::Service => {
            let original_address = fixture.service_address();
            fixture.stop_service();
            let metadata = fixture.path("metadata.db");
            let service = legacy_service(binary, fixture, &metadata);
            let config = fixture.path("legacy-service.json");
            write_read_only_service_config(&config, &fixture.path("legacy-state"), service.address);
            let receipt = legacy_yy(
                binary,
                fixture,
                &[
                    "--volume",
                    text(&config),
                    "receipt",
                    &commit_text(zero_commit),
                ],
            );
            receipt.success_or("legacy service receipt before expiry");
            assert!(
                String::from_utf8_lossy(&receipt.stdout).contains("revision:"),
                "legacy service did not read the known receipt: {}",
                String::from_utf8_lossy(&receipt.stdout)
            );
            let status = legacy_yy(binary, fixture, &["--volume", text(&config), "status"]);
            status.success_or("legacy service status before expiry");
            assert!(
                String::from_utf8_lossy(&status.stdout)
                    .contains(&format!("remote latest: {}", revision_text(latest))),
                "legacy service did not read the known snapshot: {}",
                String::from_utf8_lossy(&status.stdout)
            );
            service.stop();
            fixture.restart_service();
            assert_eq!(fixture.service_address(), original_address);
        }
    }
    assert_eq!(
        runtime()
            .block_on(fixture_authority(fixture).observe_latest())
            .unwrap()
            .revision(),
        head_before,
        "legacy pre-expiry qualification mutated the head"
    );
}

fn legacy_post_expiry(
    binary: &Path,
    fixture: &mut Fixture,
    publication: Publication,
    expired: &str,
) {
    let head_before = runtime()
        .block_on(fixture_authority(fixture).observe_latest())
        .unwrap()
        .revision();
    match publication {
        Publication::Object => {
            let status = legacy_yy(binary, fixture, &["status", "--revision", expired]);
            assert!(
                !status.status.success(),
                "legacy object binary accepted the activated format"
            );
            let diagnostic = String::from_utf8_lossy(&status.stderr).to_ascii_lowercase();
            assert_eq!(
                diagnostic.trim(),
                "yy: open volume: unknown profile or publication mode",
                "legacy object rejection lacked the exact format diagnostic: {diagnostic}"
            );
            let read_path = fixture.path("legacy-post-expiry-read");
            let read = legacy_yy(
                binary,
                fixture,
                &["restore", text(&read_path), "--revision", expired],
            );
            assert!(!read.status.success());
            assert!(!read_path.exists());
        }
        Publication::Service => {
            fixture.stop_service();
            let metadata = fixture.path("metadata.db");
            let serve = legacy_yy(
                binary,
                fixture,
                &[
                    "serve",
                    "--database",
                    text(&metadata),
                    "--listen",
                    "127.0.0.1:0",
                ],
            );
            assert!(
                !serve.status.success(),
                "legacy service daemon accepted the activated profile"
            );
            let diagnostic = String::from_utf8_lossy(&serve.stderr).to_ascii_lowercase();
            assert!(
                diagnostic.contains("profile")
                    && (diagnostic.contains("unsupported") || diagnostic.contains("unknown")),
                "legacy service rejection lacked the exact profile diagnostic: {diagnostic}"
            );
            fixture.restart_service();
        }
    }
    assert_eq!(
        runtime()
            .block_on(fixture_authority(fixture).observe_latest())
            .unwrap()
            .revision(),
        head_before,
        "legacy post-activation qualification mutated the head"
    );
}

fn write_read_only_service_config(path: &Path, state: &Path, address: SocketAddr) {
    fs::write(
        path,
        serde_json::to_vec_pretty(&serde_json::json!({
            "name": "legacy-service",
            "model": "managed",
            "access": "mount",
            "frontend": "library",
            "publication": "service",
            "service_address": address.to_string(),
            "storage_profile": "minio",
            "staging": state,
            "read_only": true,
            "require": [],
        }))
        .unwrap(),
    )
    .unwrap();
}

struct LegacyService {
    child: Child,
    address: SocketAddr,
    stdout: Option<JoinHandle<Vec<u8>>>,
    stderr: Option<JoinHandle<Vec<u8>>>,
}

impl LegacyService {
    fn stop(mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        if let Some(thread) = self.stdout.take() {
            let _ = thread.join();
        }
        if let Some(thread) = self.stderr.take() {
            let _ = thread.join();
        }
    }
}

fn legacy_service(binary: &Path, fixture: &Fixture, metadata: &Path) -> LegacyService {
    let mut command = legacy_yy_command(
        binary,
        fixture,
        &[
            "serve",
            "--database",
            text(metadata),
            "--listen",
            "127.0.0.1:0",
        ],
    );
    command.stdout(Stdio::piped()).stderr(Stdio::piped());
    let mut child = command.spawn().expect("spawn legacy yy serve");
    let stdout = child.stdout.take().expect("legacy service stdout");
    let stderr = child.stderr.take().expect("legacy service stderr");
    let (sender, receiver) = mpsc::channel();
    let stdout_thread = thread::spawn(move || {
        let mut reader = BufReader::new(stdout);
        let mut line = String::new();
        let sent = reader.read_line(&mut line).is_ok();
        let _ = sender.send((sent, line));
        let mut rest = Vec::new();
        let _ = reader.read_to_end(&mut rest);
        rest
    });
    let stderr_thread = thread::spawn(move || read_all(stderr));
    let (sent, line) = receiver
        .recv_timeout(Duration::from_secs(60))
        .unwrap_or_else(|error| {
            let _ = child.kill();
            let _ = child.wait();
            panic!("legacy yy serve did not report readiness: {error:?}");
        });
    assert!(sent, "legacy yy serve closed stdout before readiness");
    let address = line
        .trim()
        .strip_prefix("metadata service listening on ")
        .and_then(|value| value.parse().ok())
        .unwrap_or_else(|| panic!("unexpected legacy service readiness line {line:?}"));
    LegacyService {
        child,
        address,
        stdout: Some(stdout_thread),
        stderr: Some(stderr_thread),
    }
}

fn legacy_yy_command(binary: &Path, fixture: &Fixture, args: &[&str]) -> Command {
    let source = fixture.yy_command(&[]);
    let envs = source
        .get_envs()
        .map(|(key, value)| (key.to_owned(), value.map(|value| value.to_owned())))
        .collect::<Vec<_>>();
    let mut command = Command::new(binary);
    for (key, value) in envs {
        if let Some(value) = value {
            command.env(key, value);
        }
    }
    command.args(args);
    command
}

fn fixture_authority(fixture: &Fixture) -> Arc<dyn Authority> {
    match fixture.publication {
        Publication::Object => Arc::new(
            runtime()
                .block_on(Fs::open(fixture.operator(), BackendProfile::Minio))
                .expect("open object authority"),
        ),
        Publication::Service => {
            let command = fixture.yy_command(&[]);
            let token = command
                .get_envs()
                .find(|(key, _)| *key == "YINYANG_SERVICE_TOKEN")
                .and_then(|(_, value)| value)
                .expect("fixture service token")
                .to_string_lossy()
                .into_owned();
            service_authority(fixture.service_address(), &token, fixture.operator())
        }
    }
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
    let child = command.spawn().expect("spawn restored yy serve");
    let mut service = ServiceProcess {
        child: Some(child),
        stderr: None,
    };
    let stdout = service
        .child
        .as_mut()
        .expect("service process is live")
        .stdout
        .take()
        .expect("service stdout is piped");
    let stderr = service
        .child
        .as_mut()
        .expect("service process is live")
        .stderr
        .take()
        .expect("service stderr is piped");
    let (sender, receiver) = mpsc::channel();
    let reader = thread::spawn(move || {
        let mut reader = BufReader::new(stdout);
        let mut line = String::new();
        let _ = reader.read_line(&mut line);
        let _ = sender.send(line);
    });
    service.stderr = Some(thread::spawn(move || read_all(stderr)));
    let line = receiver
        .recv_timeout(Duration::from_secs(60))
        .expect("restored service did not report readiness");
    assert!(
        line.starts_with("metadata service listening on "),
        "unexpected service readiness line: {line:?}"
    );
    let _ = reader.join();
    service
}

struct ServiceProcess {
    child: Option<Child>,
    stderr: Option<JoinHandle<Vec<u8>>>,
}

impl ServiceProcess {
    fn stop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
        if let Some(thread) = self.stderr.take() {
            let stderr = thread.join().unwrap_or_default();
            let _ = stderr;
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
    command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let child = command.spawn().expect("spawn command");
    RunningCommand::new(child).finish(timeout)
}

struct RunningCommand {
    child: Option<Child>,
    stdout: Option<JoinHandle<Vec<u8>>>,
    stderr: Option<JoinHandle<Vec<u8>>>,
}

impl RunningCommand {
    fn new(mut child: Child) -> Self {
        let stdout = child
            .stdout
            .take()
            .map(|stdout| thread::spawn(move || read_all(stdout)));
        let stderr = child
            .stderr
            .take()
            .map(|stderr| thread::spawn(move || read_all(stderr)));
        Self {
            child: Some(child),
            stdout,
            stderr,
        }
    }

    fn finish(mut self, timeout: Duration) -> Captured {
        let child = self.child.as_mut().expect("command process is live");
        let status = {
            let deadline = Instant::now() + timeout;
            loop {
                match child.try_wait().expect("wait for command") {
                    Some(status) => break status,
                    None if Instant::now() < deadline => thread::sleep(Duration::from_millis(25)),
                    None => panic!("command timed out after {timeout:?}"),
                }
            }
        };
        self.child.take();
        Captured {
            status,
            stdout: self
                .stdout
                .take()
                .and_then(|thread| thread.join().ok())
                .unwrap_or_default(),
            stderr: self
                .stderr
                .take()
                .and_then(|thread| thread.join().ok())
                .unwrap_or_default(),
        }
    }
}

impl Drop for RunningCommand {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
        if let Some(thread) = self.stdout.take() {
            let _ = thread.join();
        }
        if let Some(thread) = self.stderr.take() {
            let _ = thread.join();
        }
    }
}

fn text(path: &Path) -> &str {
    path.to_str().expect("UTF-8 path")
}
