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

use crate::harness::{Fixture, RunningCommand, path_text, payload};
use crate::s3_gate::{CommitAction, S3Gate};
use serde_json::{Value, json};
use std::fs;
use std::path::{Path, PathBuf};

fn sync_config(fixture: &Fixture, name: &str, staging: &Path) -> PathBuf {
    fixture.config_document(
        name,
        json!({
            "name": name,
            "model": "direct",
            "access": "sync",
            "frontend": "library",
            "publication": "object",
            "storage_profile": "minio",
            "read_only": false,
            "staging": staging,
        }),
    )
}

fn sync_run(fixture: &Fixture, config: &Path) {
    fixture
        .yy_ok(&["--volume", path_text(config), "sync", "run"])
        .success_or("Direct Sync run");
}

fn sync_status(fixture: &Fixture, config: &Path) -> Value {
    let output = fixture.yy_ok(&["--volume", path_text(config), "sync", "status", "--json"]);
    let status: Value = serde_json::from_slice(&output.stdout).expect("Direct Sync status JSON");
    assert_eq!(status["schema"], "yinyang.direct-sync.status.v1");
    status
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
    assert_eq!(
        conflict["local_artifact"].as_str().is_some(),
        true,
        "{status:#}"
    );
    assert_eq!(
        conflict["remote_artifact"].as_str().is_some(),
        true,
        "{status:#}"
    );

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
    fixture
        .yy_ok(&[
            "--volume",
            path_text(&config),
            "sync",
            "init",
            "--directory",
            path_text(&local),
        ])
        .success_or("Direct Sync empty-directory init");
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

    let marker_fixture = Fixture::new();
    let marker_state = marker_fixture.path("direct-sync-marker-state");
    let marker_config = sync_config(&marker_fixture, "direct-sync-marker", &marker_state);
    let marker_local = marker_fixture.path("direct-sync-marker-local");
    fs::create_dir(&marker_local).expect("create marker Direct Sync root");
    marker_fixture.write_raw("remote-empty/", b"");
    marker_fixture
        .yy_ok(&[
            "--volume",
            path_text(&marker_config),
            "sync",
            "init",
            "--directory",
            path_text(&marker_local),
        ])
        .success_or("Direct Sync remote-marker init");
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
    assert_eq!(
        marker_fixture.raw_get("remote-empty/").as_deref(),
        Some(b"".as_slice()),
        "Direct Sync changed the remote directory marker without support"
    );

    let transition = marker_local.join("remote-empty");
    if transition.is_dir() {
        fs::remove_dir(&transition).expect("remove materialized empty directory");
    }
    fs::write(&transition, b"local file").expect("write file over remote directory");
    sync_run(&marker_fixture, &marker_config);
    let transition_status = sync_status(&marker_fixture, &marker_config);
    assert_explicit_directory_boundary(
        &transition_status,
        "remote-empty",
        "remote directory to local file transition",
    );
    assert_eq!(
        fs::read(&transition).expect("read retained local transition file"),
        b"local file"
    );
    assert_eq!(
        marker_fixture.raw_get("remote-empty/").as_deref(),
        Some(b"".as_slice()),
        "unsupported directory transition deleted the remote marker"
    );

    fs::write(local.join("kind"), b"baseline file").expect("write local kind baseline");
    sync_run(&fixture, &config);
    assert_eq!(
        fixture.raw_get("kind").as_deref(),
        Some(b"baseline file".as_slice())
    );
    fs::remove_file(local.join("kind")).expect("remove local kind baseline file");
    fs::create_dir(local.join("kind")).expect("replace local file with directory");
    fs::write(local.join("kind/child.txt"), b"local child").expect("write local kind child");
    sync_run(&fixture, &config);
    let kind_status = sync_status(&fixture, &config);
    assert_explicit_directory_boundary(&kind_status, "kind", "local file to directory transition");
    assert_eq!(
        fixture.raw_get("kind").as_deref(),
        Some(b"baseline file".as_slice()),
        "unsupported local kind change overwrote the remote baseline file"
    );
    assert_eq!(
        fs::read(local.join("kind/child.txt")).expect("read retained local kind child"),
        b"local child"
    );
}

fn baseline_sync(fixture: &Fixture, config: &Path, local: &Path, original: &[u8]) {
    fixture
        .yy_ok(&[
            "--volume",
            path_text(config),
            "sync",
            "init",
            "--directory",
            path_text(local),
        ])
        .success_or("Direct Sync uncertainty init");
    sync_run(fixture, config);
    assert_eq!(fixture.raw_get("uncertain.txt").as_deref(), Some(original));
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
    fs::write(local.join("uncertain.txt"), &original)
        .expect("write Direct Sync uncertainty baseline");
    baseline_sync(&fixture, &config, &local, &original);
    fs::write(local.join("uncertain.txt"), &first).expect("write first Direct Sync local edit");

    let suffix = format!("/{}/uncertain.txt", fixture.root());
    let gate = S3Gate::start_for_path(action, "PUT", suffix);
    let mut command = fixture.yy_command(&["--volume", path_text(&config), "sync", "run"]);
    command.env("YINYANG_S3_ENDPOINT", gate.endpoint());
    let mut running = RunningCommand::spawn(command);
    let intercepted = gate.wait_intercepted();
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
    drop(gate);

    fs::write(local.join("uncertain.txt"), &newer).expect("write newer Direct Sync local edit");
    let status = sync_status(&fixture, &config);
    assert_eq!(status["summary"]["unknown"], 1, "{status:#}");
    let unknown = entry(&status, "uncertain.txt");
    assert_eq!(unknown["state"], "unknown", "{status:#}");
    let work = unknown["work"]["id"]
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
        .filter(|artifact| artifact["kind"] == "upload" && artifact["origin"] == "uncertain.txt")
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

    let _retry = fixture.yy(&["--volume", path_text(&config), "sync", "run"]);
    assert_eq!(
        fixture.raw_get("uncertain.txt").as_deref(),
        Some(backend_before_retry),
        "a later run automatically overwrote from an older uncertain request"
    );
    let status = sync_status(&fixture, &config);
    assert_eq!(status["summary"]["unknown"], 1, "{status:#}");

    fixture
        .yy_ok(&[
            "--volume",
            path_text(&config),
            "sync",
            "resolve",
            work,
            "--keep",
            "local",
        ])
        .success_or("resolve Direct Sync Unknown work to current local state");
    assert_eq!(
        fixture.raw_get("uncertain.txt").as_deref(),
        Some(newer.as_slice()),
        "explicit local resolution did not publish the observed desired state"
    );
    assert_in_sync(&sync_status(&fixture, &config));
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
