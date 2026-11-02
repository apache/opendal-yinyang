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

//! Real storage and CLI acceptance for finite Direct Sync.

use crate::harness::{
    Fixture, RunningCommand, direct_test_storage_profile, path_text, payload, run_timeout,
};
use crate::s3_gate::{CommitAction, S3Gate};
use serde_json::{Value, json};
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::time::Duration;

fn sync_config(fixture: &Fixture, name: &str, staging: &Path) -> PathBuf {
    fixture.config_document(
        name,
        json!({
            "name": name,
            "model": "direct",
            "access": "sync",
            "frontend": "library",
            "publication": "object",
            "storage_profile": direct_test_storage_profile(),
            "read_only": false,
            "staging": staging,
        }),
    )
}

fn sync_run(fixture: &Fixture, config: &Path) {
    sync_run_at(fixture, config, None).success_or("Direct Sync run");
}

fn sync_status(fixture: &Fixture, config: &Path) -> Value {
    sync_status_at(fixture, config, None)
}

fn sync_run_at(
    fixture: &Fixture,
    config: &Path,
    endpoint: Option<&str>,
) -> crate::harness::Captured {
    run_sync_command(fixture, config, endpoint, &["run"])
}

fn sync_status_at(fixture: &Fixture, config: &Path, endpoint: Option<&str>) -> Value {
    let output = run_sync_command(fixture, config, endpoint, &["status", "--json"]);
    output.success_or("Direct Sync status");
    let status: Value = serde_json::from_slice(&output.stdout).expect("Direct Sync status JSON");
    assert_eq!(status["schema"], "yinyang.sync.status.v1");
    status
}

fn doctor_check<'a>(document: &'a Value, name: &str) -> &'a Value {
    document["checks"]
        .as_array()
        .expect("doctor checks")
        .iter()
        .find(|check| check["name"] == name)
        .unwrap_or_else(|| panic!("doctor omitted {name:?}: {document:#}"))
}

fn run_sync_command(
    fixture: &Fixture,
    config: &Path,
    endpoint: Option<&str>,
    command: &[&str],
) -> crate::harness::Captured {
    let mut args = vec!["--volume", path_text(config), "sync"];
    args.extend_from_slice(command);
    let mut process = fixture.yy_command(&args);
    if let Some(endpoint) = endpoint {
        process.env("YINYANG_S3_ENDPOINT", endpoint);
    }
    run_timeout(process, Duration::from_secs(120))
}

fn entry<'a>(status: &'a Value, path: &str) -> &'a Value {
    status["entries"]
        .as_array()
        .expect("Direct Sync entries")
        .iter()
        .find(|entry| entry["path"] == path)
        .unwrap_or_else(|| panic!("Direct Sync status omitted {path:?}: {status:#}"))
}

fn assert_in_sync(status: &Value) {
    assert_eq!(status["summary"]["in_sync"], true, "{status:#}");
    assert_eq!(status["summary"]["pending"], 0, "{status:#}");
    assert_eq!(status["summary"]["unknown"], 0, "{status:#}");
    assert_eq!(status["summary"]["conflicts"], 0, "{status:#}");
    assert_eq!(status["summary"]["failed"], 0, "{status:#}");
    assert_eq!(status["remote"]["availability"], "available", "{status:#}");
    assert!(status["remote"]["revision"].is_null(), "{status:#}");
}

#[test]
#[ignore = "requires isolated S3; run with cargo x test-linux-direct"]
fn direct_sync_finite_reconciliation_tracks_both_authorities() {
    let fixture = Fixture::new();
    let state = fixture.path("direct-sync-basic-state");
    let config = sync_config(&fixture, "direct-sync-basic", &state);
    let local = fixture.path("direct-sync-basic-local");

    fixture
        .yy_ok(&[
            "--volume",
            path_text(&config),
            "sync",
            "init",
            "--directory",
            path_text(&local),
        ])
        .success_or("Direct Sync init");
    assert_in_sync(&sync_status(&fixture, &config));

    fs::write(local.join("hello.txt"), b"local hello").expect("write Direct Sync local file");
    fs::create_dir(local.join("nested")).expect("create Direct Sync local directory");
    fs::write(local.join("nested/file.txt"), b"nested local")
        .expect("write nested Direct Sync local file");
    sync_run(&fixture, &config);
    assert_eq!(
        fixture.raw_get("hello.txt").as_deref(),
        Some(b"local hello".as_slice())
    );
    assert_eq!(
        fixture.raw_get("nested/file.txt").as_deref(),
        Some(b"nested local".as_slice())
    );
    let status = sync_status(&fixture, &config);
    assert_in_sync(&status);
    assert_eq!(entry(&status, "hello.txt")["state"], "in_sync");

    fixture.write_raw("hello.txt", b"remote hello");
    fixture.write_raw("remote-only.txt", b"remote only");
    sync_run(&fixture, &config);
    assert_eq!(
        fs::read(local.join("hello.txt")).expect("read remote Direct Sync replacement"),
        b"remote hello"
    );
    assert_eq!(
        fs::read(local.join("remote-only.txt")).expect("read remote-only Direct Sync file"),
        b"remote only"
    );

    fs::remove_file(local.join("hello.txt")).expect("delete local Direct Sync file");
    sync_run(&fixture, &config);
    assert_eq!(fixture.raw_get("hello.txt"), None);
    fixture.delete_raw("remote-only.txt");
    sync_run(&fixture, &config);
    assert!(!local.join("remote-only.txt").exists());
    assert_in_sync(&sync_status(&fixture, &config));
    assert!(
        fixture
            .inventory()
            .keys()
            .all(|key| !key.starts_with(".yinyang/")),
        "Direct Sync created Managed metadata"
    );
}

#[test]
#[ignore = "requires isolated S3; run with cargo x test-linux-direct"]
fn direct_sync_backup_restore_round_trip() {
    let fixture = Fixture::new();
    let state = fixture.path("direct-sync-backup-source-state");
    let config = sync_config(&fixture, "direct-sync-backup-source", &state);
    let local = fixture.path("direct-sync-backup-source-local");
    fs::create_dir(&local).expect("create Direct Sync backup source root");
    fs::write(local.join("before.txt"), b"before").expect("write Direct Sync backup source");
    fixture
        .yy_ok(&[
            "--volume",
            path_text(&config),
            "sync",
            "init",
            "--directory",
            path_text(&local),
        ])
        .success_or("Direct Sync backup source init");
    sync_run(&fixture, &config);
    assert_in_sync(&sync_status(&fixture, &config));

    let backup = fixture.path("direct-sync-backup-package");
    fixture
        .yy_ok(&[
            "--volume",
            path_text(&config),
            "sync",
            "backup",
            "--output",
            path_text(&backup),
        ])
        .success_or("Direct Sync backup");

    fixture.write_raw("after.txt", b"after");
    let restored_state = fixture.path("direct-sync-backup-restored-state");
    let restored_config = sync_config(&fixture, "direct-sync-backup-restored", &restored_state);
    let restored = fixture.path("direct-sync-backup-restored-local");
    fixture
        .yy_ok(&[
            "--volume",
            path_text(&restored_config),
            "sync",
            "restore",
            "--backup",
            path_text(&backup),
            "--directory",
            path_text(&restored),
        ])
        .success_or("Direct Sync restore");
    assert_eq!(
        fs::read(restored.join("before.txt")).expect("read restored Direct Sync file"),
        b"before"
    );
    assert!(!restored.join("after.txt").exists());

    sync_run(&fixture, &restored_config);
    assert_eq!(
        fs::read(restored.join("after.txt")).expect("read reconciled restored Direct Sync file"),
        b"after"
    );
    assert_in_sync(&sync_status(&fixture, &restored_config));
}

#[test]
#[ignore = "requires isolated S3; run with cargo x test-linux-direct"]
fn direct_sync_conflicts_export_and_explicit_resolution() {
    let fixture = Fixture::new();
    let state = fixture.path("direct-sync-conflict-state");
    let config = sync_config(&fixture, "direct-sync-conflict", &state);
    let local = fixture.path("direct-sync-conflict-local");
    fs::create_dir(&local).expect("create Direct Sync conflict local root");
    fs::write(local.join("shared.txt"), b"baseline").expect("write Direct Sync baseline");
    fixture
        .yy_ok(&[
            "--volume",
            path_text(&config),
            "sync",
            "init",
            "--directory",
            path_text(&local),
        ])
        .success_or("Direct Sync conflict init");
    sync_run(&fixture, &config);
    assert_eq!(
        fixture.raw_get("shared.txt").as_deref(),
        Some(b"baseline".as_slice())
    );

    fs::write(local.join("shared.txt"), b"local side").expect("write local conflict side");
    fixture.write_raw("shared.txt", b"remote side");
    sync_run(&fixture, &config);
    let status = sync_status(&fixture, &config);
    assert_eq!(status["summary"]["conflicts"], 1, "{status:#}");
    let conflict = &status["conflicts"][0];
    let id = conflict["id"].as_str().expect("Direct Sync conflict id");
    assert_eq!(conflict["path"], "shared.txt");
    assert_eq!(entry(&status, "shared.txt")["state"], "conflict");
    assert!(conflict["local_artifact"].as_str().is_some(), "{status:#}");
    assert!(conflict["remote_artifact"].as_str().is_some(), "{status:#}");

    let local_export = fixture.path("direct-sync-local-conflict.bin");
    fixture
        .yy_ok(&[
            "--volume",
            path_text(&config),
            "sync",
            "export",
            "--conflict",
            id,
            "--side",
            "local",
            "--output",
            path_text(&local_export),
        ])
        .success_or("export Direct Sync local conflict");
    assert_eq!(
        fs::read(&local_export).expect("read local conflict export"),
        b"local side"
    );
    let remote_export = fixture.path("direct-sync-remote-conflict.bin");
    fixture
        .yy_ok(&[
            "--volume",
            path_text(&config),
            "sync",
            "export",
            "--conflict",
            id,
            "--side",
            "remote",
            "--output",
            path_text(&remote_export),
        ])
        .success_or("export Direct Sync remote conflict");
    assert_eq!(
        fs::read(&remote_export).expect("read remote conflict export"),
        b"remote side"
    );

    fixture
        .yy_ok(&[
            "--volume",
            path_text(&config),
            "sync",
            "resolve",
            id,
            "--keep",
            "local",
        ])
        .success_or("resolve Direct Sync conflict to local");
    assert_eq!(
        fixture.raw_get("shared.txt").as_deref(),
        Some(b"local side".as_slice())
    );
    assert_in_sync(&sync_status(&fixture, &config));

    fs::write(local.join("shared.txt"), b"second local side")
        .expect("write second local conflict side");
    fixture.write_raw("shared.txt", b"second remote side");
    sync_run(&fixture, &config);
    let status = sync_status(&fixture, &config);
    let conflict = &status["conflicts"][0];
    let id = conflict["id"]
        .as_str()
        .expect("second Direct Sync conflict id");
    fixture
        .yy_ok(&[
            "--volume",
            path_text(&config),
            "sync",
            "resolve",
            id,
            "--keep",
            "remote",
        ])
        .success_or("resolve Direct Sync conflict to remote");
    assert_eq!(
        fs::read(local.join("shared.txt")).expect("read remote-resolved Direct Sync file"),
        b"second remote side"
    );
    assert_eq!(
        fixture.raw_get("shared.txt").as_deref(),
        Some(b"second remote side".as_slice())
    );
    assert_in_sync(&sync_status(&fixture, &config));
}

#[test]
#[ignore = "requires isolated S3; run with cargo x test-linux-direct"]
fn direct_sync_doctor_reports_persisted_state_without_mount_assumptions() {
    let fixture = Fixture::new();
    let state = fixture.path("direct-sync-doctor-state");
    let config = sync_config(&fixture, "direct-sync-doctor", &state);
    let local = fixture.path("direct-sync-doctor-local");
    fs::create_dir(&local).expect("create Direct Sync doctor local root");
    fs::write(local.join("shared.txt"), b"baseline").expect("write Direct Sync doctor baseline");
    fixture
        .yy_ok(&[
            "--volume",
            path_text(&config),
            "sync",
            "init",
            "--directory",
            path_text(&local),
        ])
        .success_or("Direct Sync doctor init");
    sync_run(&fixture, &config);

    fs::write(local.join("shared.txt"), b"local side").expect("write local doctor conflict");
    fixture.write_raw("shared.txt", b"remote side");
    sync_run(&fixture, &config);
    let status = sync_status(&fixture, &config);
    assert_eq!(status["summary"]["conflicts"], 1, "{status:#}");
    let before_inventory = fixture.inventory();
    let before_local = fs::read(local.join("shared.txt")).expect("read local doctor conflict");

    let doctor = fixture.yy(&["--volume", path_text(&config), "doctor", "--json"]);
    assert!(doctor.status.success(), "{}", doctor.text());
    let value: Value = serde_json::from_slice(&doctor.stdout).expect("Direct Sync doctor JSON");
    assert_eq!(value["schema"], "yinyang.operational-health.v1");
    assert_eq!(value["status"], "pass", "{value:#}");
    let sync_state = doctor_check(&value, "sync-state");
    assert_eq!(sync_state["status"], "info", "{value:#}");
    let detail = sync_state["detail"].as_str().expect("sync-state detail");
    assert!(detail.contains("conflicts=1"), "{value:#}");
    assert!(detail.contains("unknown=0"), "{value:#}");
    assert!(
        !value["checks"]
            .as_array()
            .expect("doctor checks")
            .iter()
            .any(|check| check["name"] == "mount-state"),
        "Direct Sync doctor used a Mount-runtime state check: {value:#}"
    );
    assert_eq!(fixture.inventory(), before_inventory);
    assert_eq!(
        fs::read(local.join("shared.txt")).expect("read local doctor conflict after doctor"),
        before_local
    );
    assert_eq!(
        sync_status(&fixture, &config)["summary"]["conflicts"],
        1,
        "doctor changed Direct Sync conflict state"
    );
}

#[test]
#[ignore = "requires isolated S3; run with cargo x test-linux-direct"]
fn direct_sync_doctor_rejects_unrepresentable_state_without_effects() {
    let private_fixture = Fixture::new();
    let private_state = private_fixture.path("direct-sync-doctor-private-state");
    fs::create_dir(&private_state).expect("create private Direct Sync doctor state");
    fs::set_permissions(&private_state, fs::Permissions::from_mode(0o755))
        .expect("make Direct Sync doctor state non-private");
    let private_config = sync_config(
        &private_fixture,
        "direct-sync-doctor-private",
        &private_state,
    );
    let private_local = private_fixture.path("direct-sync-doctor-private-local");
    fs::create_dir(&private_local).expect("create private Direct Sync doctor local root");
    let private_report =
        private_fixture.yy(&["--volume", path_text(&private_config), "doctor", "--json"]);
    let private_value: Value =
        serde_json::from_slice(&private_report.stdout).expect("private Direct Sync doctor JSON");
    assert_ne!(private_value["status"], "pass", "{private_value:#}");
    let private_state_check = doctor_check(&private_value, "sync-state");
    assert_eq!(
        private_state_check["status"], "action_required",
        "{private_value:#}"
    );
    assert!(
        private_state_check["detail"]
            .as_str()
            .is_some_and(|detail| detail.contains("group or other")),
        "{private_value:#}"
    );
    assert!(!private_state.join("sync-engine.db").exists());

    let marker_fixture = Fixture::new();
    let marker_state = marker_fixture.path("direct-sync-doctor-marker-state");
    let marker_config = sync_config(&marker_fixture, "direct-sync-doctor-marker", &marker_state);
    let marker_local = marker_fixture.path("direct-sync-doctor-marker-local");
    fs::create_dir(&marker_local).expect("create marker Direct Sync doctor local root");
    marker_fixture.write_raw("empty/", b"");
    let marker_report =
        marker_fixture.yy(&["--volume", path_text(&marker_config), "doctor", "--json"]);
    let marker_value: Value =
        serde_json::from_slice(&marker_report.stdout).expect("marker Direct Sync doctor JSON");
    assert_ne!(marker_value["status"], "pass", "{marker_value:#}");
    let marker_namespace = doctor_check(&marker_value, "namespace");
    assert_eq!(
        marker_namespace["status"], "action_required",
        "{marker_value:#}"
    );
    assert!(
        marker_namespace["detail"]
            .as_str()
            .is_some_and(|detail| detail.contains("empty directory markers")),
        "{marker_value:#}"
    );
    assert!(!marker_state.join("sync-engine.db").exists());
    assert_eq!(
        marker_fixture.raw_get("empty/").as_deref(),
        Some(b"".as_slice())
    );
}

fn assert_explicit_directory_boundary(status: &Value, path: &str, context: &str) {
    assert_ne!(
        status["summary"]["in_sync"], true,
        "{context} was silently reported in sync: {status:#}"
    );
    let pending = status["summary"]["pending"].as_u64().unwrap_or_default();
    let failed = status["summary"]["failed"].as_u64().unwrap_or_default();
    let conflicts = status["summary"]["conflicts"].as_u64().unwrap_or_default();
    assert!(
        pending + failed + conflicts > 0,
        "{context} had no explicit pending, failed or conflict evidence: {status:#}"
    );
    if let Some(entry) = status["entries"]
        .as_array()
        .and_then(|entries| entries.iter().find(|entry| entry["path"] == path))
    {
        assert!(
            [
                "local_pending",
                "remote_pending",
                "conflict",
                "failed",
                "unknown"
            ]
            .contains(&entry["state"].as_str().unwrap_or_default()),
            "{context} used a silent entry state: {status:#}"
        );
    }
}

#[test]
#[ignore = "requires isolated S3; run with cargo x test-linux-direct"]
fn direct_sync_empty_directories_and_kind_changes_are_never_silently_ignored() {
    let fixture = Fixture::new();
    let state = fixture.path("direct-sync-directory-state");
    let config = sync_config(&fixture, "direct-sync-directory", &state);
    let local = fixture.path("direct-sync-directory-local");
    fs::create_dir(&local).expect("create Direct Sync directory root");
    fs::create_dir(local.join("local-empty")).expect("create local-only empty directory");
    let init = fixture.yy(&[
        "--volume",
        path_text(&config),
        "sync",
        "init",
        "--directory",
        path_text(&local),
    ]);
    if !init.status.success() {
        assert!(
            init.text()
                .to_ascii_lowercase()
                .contains("empty directory markers"),
            "Direct Sync empty-directory rejection was not explicit:\n{}",
            init.text()
        );
        assert!(!state.join("sync-engine.db").exists());
        assert_eq!(fixture.raw_get("local-empty/"), None);
        assert!(local.join("local-empty").is_dir());
    } else {
        sync_run(&fixture, &config);
        let status = sync_status(&fixture, &config);
        if status["summary"]["in_sync"] == true {
            assert_eq!(
                fixture.raw_get("local-empty/").as_deref(),
                Some(b"".as_slice()),
                "Direct Sync ignored an empty local directory: {status:#}"
            );
            assert!(local.join("local-empty").is_dir());
        } else {
            assert_explicit_directory_boundary(&status, "local-empty", "local empty directory");
        }
    }

    let marker_fixture = Fixture::new();
    let marker_state = marker_fixture.path("direct-sync-marker-state");
    let marker_config = sync_config(&marker_fixture, "direct-sync-marker", &marker_state);
    let marker_local = marker_fixture.path("direct-sync-marker-local");
    fs::create_dir(&marker_local).expect("create marker Direct Sync root");
    marker_fixture.write_raw("remote-empty/", b"");
    let marker_init = marker_fixture.yy(&[
        "--volume",
        path_text(&marker_config),
        "sync",
        "init",
        "--directory",
        path_text(&marker_local),
    ]);
    if !marker_init.status.success() {
        assert!(
            marker_init
                .text()
                .to_ascii_lowercase()
                .contains("empty directory markers"),
            "Direct Sync remote-marker rejection was not explicit:\n{}",
            marker_init.text()
        );
        assert!(!marker_state.join("sync-engine.db").exists());
        assert!(!marker_local.join("remote-empty").exists());
    } else {
        sync_run(&marker_fixture, &marker_config);
        let marker_status = sync_status(&marker_fixture, &marker_config);
        if marker_status["summary"]["in_sync"] == true {
            assert!(
                marker_local.join("remote-empty").is_dir(),
                "Direct Sync ignored a remote empty-directory marker: {marker_status:#}"
            );
        } else {
            assert_explicit_directory_boundary(
                &marker_status,
                "remote-empty",
                "remote empty-directory marker",
            );
        }
    }
    assert_eq!(
        marker_fixture.raw_get("remote-empty/").as_deref(),
        Some(b"".as_slice()),
        "Direct Sync changed the remote directory marker without support"
    );

    let kind_fixture = Fixture::new();
    let kind_state = kind_fixture.path("direct-sync-kind-state");
    let kind_config = sync_config(&kind_fixture, "direct-sync-kind", &kind_state);
    let kind_local = kind_fixture.path("direct-sync-kind-local");
    fs::create_dir(&kind_local).expect("create kind Direct Sync root");
    fs::write(kind_local.join("kind"), b"baseline file").expect("write local kind baseline");
    kind_fixture
        .yy_ok(&[
            "--volume",
            path_text(&kind_config),
            "sync",
            "init",
            "--directory",
            path_text(&kind_local),
        ])
        .success_or("Direct Sync kind init");
    sync_run(&kind_fixture, &kind_config);
    assert_eq!(
        kind_fixture.raw_get("kind").as_deref(),
        Some(b"baseline file".as_slice())
    );
    fs::remove_file(kind_local.join("kind")).expect("remove local kind baseline file");
    fs::create_dir(kind_local.join("kind")).expect("replace local file with directory");
    fs::write(kind_local.join("kind/child.txt"), b"local child").expect("write local kind child");
    sync_run(&kind_fixture, &kind_config);
    let kind_status = sync_status(&kind_fixture, &kind_config);
    assert_explicit_directory_boundary(&kind_status, "kind", "local file to directory transition");
    assert_eq!(
        kind_fixture.raw_get("kind").as_deref(),
        Some(b"baseline file".as_slice()),
        "unsupported local kind change overwrote the remote baseline file"
    );
    assert_eq!(
        fs::read(kind_local.join("kind/child.txt")).expect("read retained local kind child"),
        b"local child"
    );

    let remote_kind_fixture = Fixture::new();
    let remote_kind_state = remote_kind_fixture.path("direct-sync-remote-kind-state");
    let remote_kind_config = sync_config(
        &remote_kind_fixture,
        "direct-sync-remote-kind",
        &remote_kind_state,
    );
    let remote_kind_local = remote_kind_fixture.path("direct-sync-remote-kind-local");
    fs::create_dir(&remote_kind_local).expect("create remote kind Direct Sync root");
    remote_kind_fixture.write_raw("remote-kind/child.txt", b"remote child");
    remote_kind_fixture
        .yy_ok(&[
            "--volume",
            path_text(&remote_kind_config),
            "sync",
            "init",
            "--directory",
            path_text(&remote_kind_local),
        ])
        .success_or("Direct Sync remote-kind init");
    sync_run(&remote_kind_fixture, &remote_kind_config);
    assert!(remote_kind_local.join("remote-kind").is_dir());
    fs::remove_dir_all(remote_kind_local.join("remote-kind"))
        .expect("remove materialized remote kind directory");
    fs::write(remote_kind_local.join("remote-kind"), b"local file")
        .expect("write local file over remote directory");
    sync_run(&remote_kind_fixture, &remote_kind_config);
    let remote_kind_status = sync_status(&remote_kind_fixture, &remote_kind_config);
    assert_explicit_directory_boundary(
        &remote_kind_status,
        "remote-kind",
        "remote directory to local file transition",
    );
    assert_eq!(
        fs::read(remote_kind_local.join("remote-kind"))
            .expect("read retained local remote-kind file"),
        b"local file"
    );
    assert_eq!(
        remote_kind_fixture
            .raw_get("remote-kind/child.txt")
            .as_deref(),
        Some(b"remote child".as_slice()),
        "unsupported remote directory transition changed its descendant object"
    );
}

fn exercise_uncertain_sync(action: CommitAction, delivered: bool) {
    let fixture = Fixture::new();
    let state = fixture.path("direct-sync-uncertain-state");
    let config = sync_config(&fixture, "direct-sync-uncertain", &state);
    let local = fixture.path("direct-sync-uncertain-local");
    fs::create_dir(&local).expect("create Direct Sync uncertainty root");
    let original = payload(24 * 1024 + 1, 0x17);
    let first = payload(24 * 1024 + 5, 0x39);
    let newer = payload(24 * 1024 + 3, 0x5b);
    fixture.write_raw("uncertain.txt", &original);

    let suffix = format!("/{}/uncertain.txt", fixture.root());
    let gate = S3Gate::start_for_path(action, "PUT", suffix);
    let endpoint = gate.endpoint();
    run_sync_command(
        &fixture,
        &config,
        Some(&endpoint),
        &["init", "--directory", path_text(&local)],
    )
    .success_or("Direct Sync uncertainty init");
    sync_run_at(&fixture, &config, Some(&endpoint)).success_or("Direct Sync uncertainty baseline");
    assert_eq!(
        fs::read(local.join("uncertain.txt")).expect("read Direct Sync uncertainty baseline"),
        original
    );
    fs::write(local.join("uncertain.txt"), &first).expect("write first Direct Sync local edit");

    let command = {
        let mut command = fixture.yy_command(&["--volume", path_text(&config), "sync", "run"]);
        command.env("YINYANG_S3_ENDPOINT", &endpoint);
        command
    };
    let mut running = RunningCommand::spawn(command);
    let intercepted = gate.wait_intercepted_for(Duration::from_secs(60));
    assert_eq!(intercepted.method, "PUT");
    assert!(
        intercepted.header("if-match").is_some(),
        "Direct Sync replacement omitted its conditional ETag: {:?}",
        intercepted.headers
    );
    if delivered {
        fixture.wait_raw_bytes("uncertain.txt", &first, std::time::Duration::from_secs(30));
    } else {
        assert_eq!(
            fixture.raw_get("uncertain.txt").as_deref(),
            Some(original.as_slice()),
            "dropped Direct Sync request reached the backend"
        );
        running.kill();
    }
    gate.release();
    if !delivered {
        gate.wait_original_dropped();
    }
    let output = running.wait(std::time::Duration::from_secs(120));
    if !delivered {
        assert!(
            !output.status.success(),
            "killed Direct Sync producer reported success:\n{}",
            output.text()
        );
    }
    gate.resume();

    fs::write(local.join("uncertain.txt"), &newer).expect("write newer Direct Sync local edit");
    let status = sync_status_at(&fixture, &config, Some(&endpoint));
    assert_eq!(status["summary"]["unknown"], 1, "{status:#}");
    let unknown = entry(&status, "uncertain.txt");
    assert_eq!(unknown["state"], "unknown", "{status:#}");
    let work = unknown["intent"]["id"]
        .as_str()
        .expect("Direct Sync Unknown work id");
    assert_eq!(
        fs::read(local.join("uncertain.txt")).expect("read newer Direct Sync local edit"),
        newer
    );
    let backend_before_retry = if delivered {
        first.as_slice()
    } else {
        original.as_slice()
    };
    assert_eq!(
        fixture.raw_get("uncertain.txt").as_deref(),
        Some(backend_before_retry),
        "status changed an uncertain backend outcome"
    );

    let artifacts = status["artifacts"]
        .as_array()
        .expect("Direct Sync artifacts")
        .iter()
        .filter(|artifact| {
            artifact["kind"] == "upload-retained" && artifact["origin"] == "uncertain.txt"
        })
        .collect::<Vec<_>>();
    assert!(
        !artifacts.is_empty(),
        "Direct Sync omitted retained upload artifacts: {status:#}"
    );
    let mut exported_first = false;
    for (index, artifact) in artifacts.into_iter().enumerate() {
        let artifact_id = artifact["id"].as_str().expect("Direct Sync artifact id");
        let export = fixture.path(&format!("direct-sync-uncertain-artifact-{index}.bin"));
        fixture
            .yy_ok(&[
                "--volume",
                path_text(&config),
                "sync",
                "export",
                "--artifact",
                artifact_id,
                "--output",
                path_text(&export),
            ])
            .success_or("export uncertain Direct Sync artifact");
        if fs::read(&export).expect("read Direct Sync uncertain artifact") == first {
            exported_first = true;
        }
    }
    assert!(
        exported_first,
        "Direct Sync did not retain the original uncertain upload bytes"
    );

    let _retry = sync_run_at(&fixture, &config, Some(&endpoint));
    assert_eq!(
        fixture.raw_get("uncertain.txt").as_deref(),
        Some(backend_before_retry),
        "a later run automatically overwrote from an older uncertain request"
    );
    let status = sync_status_at(&fixture, &config, Some(&endpoint));
    assert_eq!(status["summary"]["unknown"], 1, "{status:#}");

    run_sync_command(
        &fixture,
        &config,
        Some(&endpoint),
        &["resolve", work, "--keep", "local"],
    )
    .success_or("resolve Direct Sync Unknown work to current local state");
    assert_eq!(
        fixture.raw_get("uncertain.txt").as_deref(),
        Some(newer.as_slice()),
        "explicit local resolution did not publish the observed desired state"
    );
    assert_in_sync(&sync_status_at(&fixture, &config, Some(&endpoint)));
    drop(gate);
}

#[test]
#[ignore = "requires isolated S3; run with cargo x test-linux-direct"]
fn direct_sync_dropped_request_requires_explicit_current_state_resolution() {
    exercise_uncertain_sync(CommitAction::DropBeforeCommit, false);
}

#[test]
#[ignore = "requires isolated S3; run with cargo x test-linux-direct"]
fn direct_sync_lost_response_requires_explicit_current_state_resolution() {
    exercise_uncertain_sync(CommitAction::DropResponse, true);
}
