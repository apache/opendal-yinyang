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

//! Acceptance scenarios are added against the final directory CLI contract.

use crate::linux_sync::harness::{
    BindingState, Captured, ConflictSide, EntryKind, EntryState, Fixture, LocalScanState,
    OutcomeState, Presence, Publication, RemoteAvailability, RemoteEntry, SyncStatus, Tree,
    local_tree,
};
use std::fs;
use std::io::{Seek, SeekFrom, Write};
use std::os::unix::fs::PermissionsExt as _;
use std::os::unix::fs::symlink;
use std::os::unix::net::UnixListener;
use std::path::Path;
use std::time::Duration;

fn for_each_authority(mut run: impl FnMut(Publication)) {
    for publication in [Publication::Object, Publication::Service] {
        run(publication);
    }
}

fn assert_success(output: &Captured, context: &str) {
    output.success_or(context);
}

fn assert_failure(output: &Captured, context: &str) {
    assert!(
        !output.status.success(),
        "{context} unexpectedly succeeded:\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn assert_tree(root: &Path, expected: &Tree, context: &str) {
    let actual = local_tree(root).unwrap_or_else(|error| {
        panic!("scan local tree {} for {context}: {error}", root.display())
    });
    assert_eq!(&actual, expected, "{context}: local tree mismatch");
}

fn assert_in_sync(fixture: &Fixture, paths: &crate::linux_sync::harness::SyncPaths) -> SyncStatus {
    let status = fixture.sync_status(paths);
    assert_eq!(status.binding.state, BindingState::Bound, "{status:#?}");
    assert_eq!(
        status.binding.root.as_deref(),
        Some(
            fs::canonicalize(&paths.directory)
                .unwrap()
                .to_str()
                .unwrap()
        ),
        "{status:#?}"
    );
    assert_eq!(
        status.binding.state_directory,
        fs::canonicalize(&paths.state).unwrap().to_str().unwrap(),
        "{status:#?}"
    );
    assert_eq!(status.binding.same_filesystem, Some(true), "{status:#?}");
    assert_eq!(status.remote.availability, RemoteAvailability::Available);
    assert_eq!(status.local.scan, LocalScanState::Clean, "{status:#?}");
    assert!(
        status.summary.in_sync,
        "expected in_sync status: {status:#?}"
    );
    assert_eq!(status.summary.pending, 0);
    assert_eq!(status.summary.unknown, 0);
    assert_eq!(status.summary.conflicts, 0);
    assert_eq!(status.summary.failed, 0);
    status
}

fn assert_entries_match_tree(status: &SyncStatus, tree: &Tree) {
    assert_eq!(
        status.entries.len(),
        tree.len(),
        "entry count differs from independently observed tree: {status:#?}"
    );
    for (path, entry) in tree {
        let reported = status.entry(path);
        assert_eq!(reported.state, EntryState::InSync, "{status:#?}");
        assert_eq!(reported.local.presence, Presence::Present, "{status:#?}");
        assert_eq!(reported.remote.presence, Presence::Present, "{status:#?}");
        let kind = match entry {
            RemoteEntry::File { .. } => EntryKind::File,
            RemoteEntry::Directory { .. } => EntryKind::Directory,
        };
        assert_eq!(reported.local.kind, Some(kind), "{status:#?}");
    }
}

fn remote_file_bytes(fixture: &Fixture, path: &str) -> Vec<u8> {
    match fixture.remote_tree().unwrap().remove(path) {
        Some(RemoteEntry::File { bytes, .. }) => bytes,
        Some(RemoteEntry::Directory { .. }) => panic!("remote path {path} is a directory"),
        None => panic!("remote path {path} is absent"),
    }
}

fn write_local(path: &Path, bytes: &[u8]) {
    fs::write(path, bytes)
        .unwrap_or_else(|error| panic!("write local file {}: {error}", path.display()));
}

#[test]
#[ignore = "requires isolated S3 and the Managed Sync CLI; run with cargo x test-managed-sync"]
fn managed_sync_initial_pull_and_push_both_authorities() {
    for_each_authority(|publication| {
        let pull = Fixture::new(publication);
        pull.remote_create_directory("nested");
        pull.remote_create_directory("empty-dir");
        pull.remote_create_file("nested/hello", b"hello", false);
        pull.remote_create_file("empty", b"", false);
        pull.remote_create_file("tool", b"#!/bin/sh\nexit 0\n", true);
        let expected = pull.remote_tree().unwrap();
        let client = pull.sync_client("pull");
        let before = pull.sync_status(&client);
        assert_eq!(before.binding.state, BindingState::Bound);
        assert_eq!(before.local.scan, LocalScanState::Clean);
        assert!(!before.summary.in_sync);
        pull.sync_run_ok(&client);
        assert_tree(&client.directory, &expected, "initial remote pull");
        let status = assert_in_sync(&pull, &client);
        assert_entries_match_tree(&status, &expected);
        pull.sync_run_ok(&client);
        assert_tree(&client.directory, &expected, "repeated remote pull");
        assert_in_sync(&pull, &client);

        let push = Fixture::new(publication);
        let client = push.sync_paths("push");
        fs::create_dir(&client.directory).unwrap();
        fs::create_dir(client.directory.join("nested")).unwrap();
        fs::create_dir(client.directory.join("empty-dir")).unwrap();
        write_local(&client.directory.join("nested/hello"), b"local hello");
        write_local(&client.directory.join("empty"), b"");
        write_local(&client.directory.join("tool"), b"#!/bin/sh\nexit 0\n");
        fs::set_permissions(
            client.directory.join("tool"),
            fs::Permissions::from_mode(0o755),
        )
        .unwrap();
        let expected = local_tree(&client.directory).unwrap();
        let (code, status) = push.sync_status_any_exit(&client);
        assert_eq!(code, 3, "{status:#?}");
        assert_eq!(status.binding.state, BindingState::Unbound);
        assert!(!status.summary.in_sync);
        push.sync_init(&client);
        push.sync_run_ok(&client);
        push.wait_remote_tree(&expected, Duration::from_secs(60));
        let status = assert_in_sync(&push, &client);
        assert_entries_match_tree(&status, &expected);
        push.sync_run_ok(&client);
        assert_tree(&client.directory, &expected, "repeated local push");
        assert_in_sync(&push, &client);
    });
}

#[test]
#[ignore = "requires isolated S3 and the Managed Sync CLI; run with cargo x test-managed-sync"]
fn managed_sync_local_and_remote_namespace_both_authorities() {
    for_each_authority(|publication| {
        let fixture = Fixture::new(publication);
        fixture.remote_create_directory("docs");
        fixture.remote_create_directory("empty");
        fixture.remote_create_directory("local-tree/sub");
        fixture.remote_create_directory("remote-delete/sub");
        fixture.remote_create_file("docs/base", b"base", false);
        fixture.remote_create_file("local-tree/sub/local-data", b"local-data", false);
        fixture.remote_create_file("remote-delete/sub/remote-data", b"remote-data", false);
        fixture.remote_create_file("keep", b"keep", false);
        fixture.remote_create_file("remove", b"remove", false);
        let client = fixture.sync_client("namespace");
        fixture.sync_run_ok(&client);

        fs::create_dir(client.directory.join("newdir")).unwrap();
        write_local(&client.directory.join("newdir/local"), b"local");
        fs::rename(
            client.directory.join("keep"),
            client.directory.join("moved"),
        )
        .unwrap();
        fs::rename(
            client.directory.join("docs"),
            client.directory.join("renamed-docs"),
        )
        .unwrap();
        fs::remove_file(client.directory.join("remove")).unwrap();
        fs::remove_dir(client.directory.join("empty")).unwrap();
        fs::remove_dir_all(client.directory.join("local-tree")).unwrap();
        let expected = local_tree(&client.directory).unwrap();
        fixture.sync_run_ok(&client);
        fixture.wait_remote_tree(&expected, Duration::from_secs(60));
        assert_in_sync(&fixture, &client);

        fixture.remote_create_directory("remote-dir");
        fixture.remote_create_file("remote-dir/remote", b"remote", false);
        fixture.remote_rename("moved", "remote-moved");
        fixture.remote_rename("renamed-docs", "remote-docs");
        fixture.remote_remove("remote-moved");
        fixture.remote_remove_tree("newdir");
        fixture.remote_remove_tree("remote-delete");
        let expected = fixture.remote_tree().unwrap();
        fixture.sync_run_ok(&client);
        assert_tree(
            &client.directory,
            &expected,
            "remote namespace materialization",
        );
        assert_in_sync(&fixture, &client);
        fixture.sync_run_ok(&client);
        assert_in_sync(&fixture, &client);
    });
}

#[test]
#[ignore = "requires isolated S3 and the Managed Sync CLI; run with cargo x test-managed-sync"]
fn managed_sync_content_conflicts_export_and_explicit_resolution_both_authorities() {
    for_each_authority(|publication| {
        let fixture = Fixture::new(publication);
        fixture.remote_create_file("shared.txt", b"base", false);
        let left = fixture.sync_client("left");
        let right = fixture.sync_client("right");
        fixture.sync_run_ok(&left);
        fixture.sync_run_ok(&right);

        write_local(&left.directory.join("shared.txt"), b"left");
        write_local(&right.directory.join("shared.txt"), b"right");
        fixture.sync_run_ok(&left);
        let _ = fixture.sync_run(&right);
        let status = fixture.sync_status(&right);
        let conflict = status.conflict("shared.txt").clone();
        assert_eq!(conflict.original_path.as_deref(), Some("shared.txt"));
        assert_eq!(conflict.desired_path.as_deref(), Some("shared.txt"));
        assert_eq!(
            status.entry("shared.txt").original_path.as_deref(),
            Some("shared.txt")
        );
        assert_eq!(
            status.entry("shared.txt").desired_path.as_deref(),
            Some("shared.txt")
        );
        assert_eq!(status.entry("shared.txt").state, EntryState::Conflict);
        assert_eq!(
            status.entry("shared.txt").outcome.state,
            OutcomeState::Conflict
        );
        assert_eq!(conflict.local, ConflictSide::CurrentFile);
        assert_eq!(conflict.remote, ConflictSide::CurrentFile);
        assert!(conflict.remote_revision.is_some());

        let local_export = fixture.path("content-local");
        let remote_export = fixture.path("content-remote");
        fixture.sync_export_conflict(&right, &conflict.id, "local", &local_export);
        fixture.sync_export_conflict(&right, &conflict.id, "remote", &remote_export);
        assert_eq!(fs::read(&local_export).unwrap(), b"right");
        assert_eq!(fs::read(&remote_export).unwrap(), b"left");

        fixture.sync_resolve_keep(&right, &conflict.id, "local");
        fixture.sync_run_ok(&right);
        assert_eq!(remote_file_bytes(&fixture, "shared.txt"), b"right");
        assert_in_sync(&fixture, &right);
        fixture.sync_run_ok(&left);
        assert_eq!(
            fs::read(left.directory.join("shared.txt")).unwrap(),
            b"right"
        );
        assert_in_sync(&fixture, &left);

        write_local(&left.directory.join("shared.txt"), b"left-v2");
        write_local(&right.directory.join("shared.txt"), b"right-v2");
        fixture.sync_run_ok(&left);
        let _ = fixture.sync_run(&right);
        let status = fixture.sync_status(&right);
        let conflict = status.conflict("shared.txt").clone();
        assert_eq!(conflict.local, ConflictSide::CurrentFile);
        assert_eq!(conflict.remote, ConflictSide::CurrentFile);

        let chosen = fixture.path("content-chosen");
        write_local(&chosen, b"chosen");
        fixture.sync_resolve_content(&right, &conflict.id, &chosen);
        fixture.sync_run_ok(&right);
        assert_eq!(remote_file_bytes(&fixture, "shared.txt"), b"chosen");
        assert_in_sync(&fixture, &right);
    });
}

#[test]
#[ignore = "requires isolated S3 and the Managed Sync CLI; run with cargo x test-managed-sync"]
fn managed_sync_offline_edits_survive_process_and_authority_restart() {
    {
        let fixture = Fixture::new(Publication::Object);
        fixture.remote_create_file("offline.txt", b"remote-base", false);
        let client = fixture.sync_client("object-offline");
        fixture.sync_run_ok(&client);
        write_local(&client.directory.join("offline.txt"), b"object-local");
        let failed =
            fixture.sync_run_with_env(&client, &[("YINYANG_S3_ENDPOINT", "http://127.0.0.1:1")]);
        assert_failure(&failed, "object sync while authority is unavailable");
        assert_eq!(
            fs::read(client.directory.join("offline.txt")).unwrap(),
            b"object-local"
        );
        assert_eq!(remote_file_bytes(&fixture, "offline.txt"), b"remote-base");
        let status = fixture.sync_status(&client);
        assert_eq!(status.remote.availability, RemoteAvailability::Available);
        assert_eq!(status.local.scan, LocalScanState::Changed);
        assert!(!status.summary.in_sync);

        fixture.sync_run_ok(&client);
        assert_eq!(remote_file_bytes(&fixture, "offline.txt"), b"object-local");
        assert_in_sync(&fixture, &client);
        fixture.sync_run_ok(&client);
        assert_in_sync(&fixture, &client);
    }

    {
        let mut fixture = Fixture::new(Publication::Service);
        fixture.remote_create_file("offline.txt", b"remote-base", false);
        let client = fixture.sync_client("service-offline");
        fixture.sync_run_ok(&client);
        fixture.stop_service();
        write_local(&client.directory.join("offline.txt"), b"service-local");
        let failed = fixture.sync_run(&client);
        assert_failure(&failed, "service sync while authority is unavailable");
        assert_eq!(
            fs::read(client.directory.join("offline.txt")).unwrap(),
            b"service-local"
        );
        let status = fixture.sync_status(&client);
        assert_eq!(status.remote.availability, RemoteAvailability::Unavailable);
        assert_eq!(status.local.scan, LocalScanState::Changed);
        assert!(!status.summary.in_sync);

        fixture.restart_service();
        assert_eq!(remote_file_bytes(&fixture, "offline.txt"), b"remote-base");
        fixture.sync_run_ok(&client);
        assert_eq!(remote_file_bytes(&fixture, "offline.txt"), b"service-local");
        assert_in_sync(&fixture, &client);
        fixture.sync_run_ok(&client);
        assert_in_sync(&fixture, &client);
    }
}

#[test]
#[ignore = "requires isolated S3 and the Managed Sync CLI; run with cargo x test-managed-sync"]
fn managed_sync_completion_race_and_unknown_retry_use_real_service_processes() {
    {
        let fixture = Fixture::new(Publication::Service);
        fixture.remote_create_file("race.txt", b"remote-base", false);
        let gate = fixture.service_gate(false);
        let client = fixture.sync_paths_at("completion-race", Some(gate.address()));
        fs::create_dir(&client.directory).unwrap();
        fixture.sync_init(&client);
        fixture.sync_run_ok(&client);
        let base_version = fixture.remote_file_version("race.txt");

        write_local(&client.directory.join("race.txt"), b"local-v1");
        let running = fixture.spawn_sync_run(&client);
        gate.wait_for_commit_response(Duration::from_secs(120));
        let v1_version = fixture.remote_file_version("race.txt");
        assert_eq!(v1_version.0, base_version.0);
        assert!(v1_version.1 > base_version.1, "{v1_version:?}");
        assert_eq!(remote_file_bytes(&fixture, "race.txt"), b"local-v1");
        let during = fixture.sync_status(&client);
        assert_eq!(during.binding.state, BindingState::Bound, "{during:#?}");
        assert_eq!(during.remote.availability, RemoteAvailability::Available);
        assert!(!during.summary.in_sync, "{during:#?}");
        assert!(
            matches!(
                during.entry("race.txt").outcome.state,
                OutcomeState::Pending | OutcomeState::Unknown
            ),
            "{during:#?}"
        );
        assert!(during.summary.pending + during.summary.unknown >= 1);
        write_local(&client.directory.join("race.txt"), b"local-v2");
        gate.release();
        let output = running.wait(Duration::from_secs(120));
        assert_success(&output, "v1 sync completion after local v2 edit");

        assert_eq!(remote_file_bytes(&fixture, "race.txt"), b"local-v1");
        assert_eq!(
            fs::read(client.directory.join("race.txt")).unwrap(),
            b"local-v2"
        );
        let status = fixture.sync_status(&client);
        let entry = status.entry("race.txt");
        assert_eq!(entry.outcome.state, OutcomeState::RemoteCompleted);
        assert_eq!(entry.state, EntryState::LocalPending);
        assert!(status.summary.pending >= 1, "{status:#?}");
        assert_eq!(status.summary.unknown, 0);
        assert_eq!(status.summary.conflicts, 0);
        assert!(!status.summary.in_sync);

        fixture.sync_run_ok(&client);
        assert_eq!(remote_file_bytes(&fixture, "race.txt"), b"local-v2");
        let v2_version = fixture.remote_file_version("race.txt");
        assert_eq!(v2_version.0, v1_version.0);
        assert!(v2_version.1 > v1_version.1, "{v2_version:?}");
        assert_in_sync(&fixture, &client);
    }

    {
        let fixture = Fixture::new(Publication::Service);
        fixture.remote_create_file("unknown.txt", b"remote-base", false);
        let gate = fixture.service_gate(true);
        let client = fixture.sync_paths_at("unknown-retry", Some(gate.address()));
        fs::create_dir(&client.directory).unwrap();
        fixture.sync_init(&client);
        fixture.sync_run_ok(&client);
        let base_version = fixture.remote_file_version("unknown.txt");

        write_local(&client.directory.join("unknown.txt"), b"exactly-once");
        let running = fixture.spawn_sync_run(&client);
        gate.wait_for_commit_response(Duration::from_secs(120));
        let output = running.wait(Duration::from_secs(120));
        assert_failure(&output, "sync with a lost commit response");

        assert_eq!(remote_file_bytes(&fixture, "unknown.txt"), b"exactly-once");
        let committed = fixture.remote_file_version("unknown.txt");
        assert_eq!(committed.0, base_version.0);
        assert!(committed.1 > base_version.1, "{committed:?}");
        assert_eq!(
            fs::read(client.directory.join("unknown.txt")).unwrap(),
            b"exactly-once"
        );
        let status = fixture.sync_status(&client);
        let entry = status.entry("unknown.txt");
        let commit_id = entry
            .intent
            .commit_id
            .clone()
            .expect("unknown outcome retains its original commit ID");
        assert_eq!(entry.outcome.state, OutcomeState::Unknown);
        assert_eq!(
            entry.intent.state,
            Some(crate::linux_sync::harness::IntentState::Unknown)
        );
        assert_eq!(status.summary.unknown, 1, "{status:#?}");
        assert_eq!(status.summary.failed, 0);
        assert_eq!(status.summary.conflicts, 0);
        assert!(!status.summary.in_sync);

        fixture.sync_run_ok(&client);
        let status = assert_in_sync(&fixture, &client);
        assert_eq!(
            status.entry("unknown.txt").intent.commit_id.as_deref(),
            Some(commit_id.as_str())
        );
        assert_eq!(remote_file_bytes(&fixture, "unknown.txt"), b"exactly-once");
        assert_eq!(
            fixture.remote_file_version("unknown.txt"),
            committed,
            "retry of the exact unknown request must not publish a second commit"
        );
    }
}

#[test]
#[ignore = "requires isolated S3 and the Managed Sync CLI; run with cargo x test-managed-sync"]
fn managed_sync_rejects_unsafe_local_inputs_without_remote_mutation_both_authorities() {
    for_each_authority(|publication| {
        let fixture = Fixture::new(publication);
        fixture.remote_create_file("safe.txt", b"remote", false);
        let client = fixture.sync_client("unsafe");
        fixture.sync_run_ok(&client);
        let expected = fixture.remote_tree().unwrap();

        let link = client.directory.join("link");
        symlink("../outside", &link).unwrap();
        assert_failure(&fixture.sync_run(&client), "sync with a local symlink");
        assert!(
            fs::symlink_metadata(&link)
                .unwrap()
                .file_type()
                .is_symlink()
        );
        assert_eq!(fixture.remote_tree().unwrap(), expected);
        let status = fixture.sync_status(&client);
        assert_eq!(status.local.scan, LocalScanState::Failed, "{status:#?}");
        assert!(status.local.error.is_some(), "{status:#?}");
        fs::remove_file(&link).unwrap();
        fixture.sync_run_ok(&client);
        assert_in_sync(&fixture, &client);

        let socket = client.directory.join("socket");
        let listener = UnixListener::bind(&socket).unwrap();
        assert_failure(&fixture.sync_run(&client), "sync with a Unix socket");
        assert_eq!(fixture.remote_tree().unwrap(), expected);
        drop(listener);
        fs::remove_file(&socket).unwrap();
        fixture.sync_run_ok(&client);
        assert_in_sync(&fixture, &client);

        let missing = fixture.sync_client("missing-root");
        fs::remove_dir(&missing.directory).unwrap();
        let failed = fixture.sync_run(&missing);
        assert_failure(&failed, "sync after the bound root disappeared");
        let diagnostics = format!(
            "{}\n{}",
            String::from_utf8_lossy(&failed.stdout),
            String::from_utf8_lossy(&failed.stderr)
        );
        assert!(
            diagnostics.contains(&missing.directory.display().to_string()),
            "missing-root diagnostic does not name the bound root: {diagnostics}"
        );
        let (code, status) = fixture.sync_status_any_exit(&missing);
        assert_eq!(code, 4, "{status:#?}");
        assert_eq!(status.binding.state, BindingState::Error, "{status:#?}");
        assert!(!status.summary.in_sync);
    });
}

#[test]
#[ignore = "requires isolated S3 and the Managed Sync CLI; run with cargo x test-managed-sync"]
fn managed_sync_create_create_and_delete_edit_conflicts_both_authorities() {
    for_each_authority(|publication| {
        let fixture = Fixture::new(publication);
        let left = fixture.sync_client("create-left");
        let right = fixture.sync_client("create-right");
        fixture.sync_run_ok(&left);
        fixture.sync_run_ok(&right);

        write_local(&left.directory.join("same.txt"), b"left");
        write_local(&right.directory.join("same.txt"), b"right");
        fixture.sync_run_ok(&left);
        let _ = fixture.sync_run(&right);
        let status = fixture.sync_status(&right);
        let conflict = status.conflict("same.txt").clone();
        assert_eq!(conflict.original_path, None);
        assert_eq!(conflict.desired_path.as_deref(), Some("same.txt"));
        assert_eq!(status.entry("same.txt").original_path, None);
        assert_eq!(
            status.entry("same.txt").desired_path.as_deref(),
            Some("same.txt")
        );
        assert_eq!(conflict.local, ConflictSide::CurrentFile);
        assert_eq!(conflict.remote, ConflictSide::CurrentFile);
        let local = fixture.path("create-local");
        let remote = fixture.path("create-remote");
        fixture.sync_export_conflict(&right, &conflict.id, "local", &local);
        fixture.sync_export_conflict(&right, &conflict.id, "remote", &remote);
        assert_eq!(fs::read(local).unwrap(), b"right");
        assert_eq!(fs::read(remote).unwrap(), b"left");
        fixture.sync_resolve_keep(&right, &conflict.id, "local");
        fixture.sync_run_ok(&right);
        assert_eq!(remote_file_bytes(&fixture, "same.txt"), b"right");
        assert_in_sync(&fixture, &right);

        let fixture = Fixture::new(publication);
        fixture.remote_create_file("victim.txt", b"base", false);
        let left = fixture.sync_client("delete-left");
        let right = fixture.sync_client("delete-right");
        fixture.sync_run_ok(&left);
        fixture.sync_run_ok(&right);
        fs::remove_file(left.directory.join("victim.txt")).unwrap();
        write_local(&right.directory.join("victim.txt"), b"right-edit");
        fixture.sync_run_ok(&left);
        let _ = fixture.sync_run(&right);
        let status = fixture.sync_status(&right);
        let conflict = status.conflict("victim.txt").clone();
        assert_eq!(conflict.original_path.as_deref(), Some("victim.txt"));
        assert_eq!(conflict.desired_path.as_deref(), Some("victim.txt"));
        assert_eq!(
            status.entry("victim.txt").original_path.as_deref(),
            Some("victim.txt")
        );
        assert_eq!(
            status.entry("victim.txt").desired_path.as_deref(),
            Some("victim.txt")
        );
        assert_eq!(conflict.local, ConflictSide::CurrentFile);
        assert_eq!(conflict.remote, ConflictSide::Deleted);
        assert_eq!(
            fs::read(right.directory.join("victim.txt")).unwrap(),
            b"right-edit"
        );
        fixture.sync_resolve_keep(&right, &conflict.id, "local");
        fixture.sync_run_ok(&right);
        assert_eq!(remote_file_bytes(&fixture, "victim.txt"), b"right-edit");
        assert_in_sync(&fixture, &right);
        fixture.sync_run_ok(&left);
        assert_eq!(
            fs::read(left.directory.join("victim.txt")).unwrap(),
            b"right-edit"
        );
        assert_in_sync(&fixture, &left);
    });
}

#[test]
#[ignore = "requires isolated S3 and the Managed Sync CLI; run with cargo x test-managed-sync"]
fn managed_sync_retained_artifacts_export_post_install_writes_both_authorities() {
    for_each_authority(|publication| {
        let fixture = Fixture::new(publication);
        fixture.remote_create_file("watched.txt", b"original", false);
        fixture.remote_create_directory("gone/sub");
        fixture.remote_create_file("gone/sub/data", b"quarantined", false);
        let client = fixture.sync_client("artifacts");
        fixture.sync_run_ok(&client);

        let mut old_writer = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(client.directory.join("watched.txt"))
            .unwrap();
        fixture.remote_write_file("watched.txt", b"replacement");
        fixture.remote_remove_tree("gone");
        fixture.sync_run_ok(&client);

        assert_eq!(
            fs::read(client.directory.join("watched.txt")).unwrap(),
            b"replacement"
        );
        assert!(!client.directory.join("gone").exists());
        let (replaced_id, directory_id) = {
            let status = assert_in_sync(&fixture, &client);
            let replaced = status
                .artifacts
                .iter()
                .find(|artifact| artifact.origin == "watched.txt")
                .unwrap_or_else(|| panic!("missing replaced-file artifact: {status:#?}"));
            assert_eq!(
                replaced.kind,
                crate::linux_sync::harness::ArtifactKind::ReplacedFile
            );
            assert_eq!(
                replaced.phase,
                crate::linux_sync::harness::ArtifactPhase::Retained
            );
            assert!(replaced.exportable, "{replaced:#?}");
            assert!(Path::new(&replaced.path).is_absolute(), "{replaced:#?}");
            assert!(Path::new(&replaced.path).exists(), "{replaced:#?}");
            let directory = status
                .artifacts
                .iter()
                .find(|artifact| artifact.origin == "gone")
                .unwrap_or_else(|| panic!("missing quarantined-directory artifact: {status:#?}"));
            assert_eq!(
                directory.kind,
                crate::linux_sync::harness::ArtifactKind::QuarantinedDirectory
            );
            assert!(!directory.exportable, "{directory:#?}");
            (replaced.id.clone(), directory.id.clone())
        };

        old_writer.seek(SeekFrom::Start(0)).unwrap();
        old_writer.set_len(0).unwrap();
        old_writer.write_all(b"written-after-install").unwrap();
        old_writer.sync_all().unwrap();
        drop(old_writer);

        let status = fixture.sync_status(&client);
        let conflict = status.conflict("watched.txt").clone();
        assert_eq!(conflict.local, ConflictSide::RetainedArtifact);
        assert_eq!(conflict.remote, ConflictSide::CurrentFile);
        assert_eq!(
            conflict.local_artifact.as_deref(),
            Some(replaced_id.as_str())
        );
        assert!(!status.summary.in_sync, "{status:#?}");

        let exported = fixture.path("artifact-export");
        fixture.sync_export_artifact(&client, &replaced_id, &exported);
        assert_eq!(fs::read(&exported).unwrap(), b"written-after-install");

        let invalid = fixture.path("directory-artifact-export");
        let failed = fixture.sync_export_artifact_output(&client, &directory_id, &invalid);
        assert_failure(&failed, "export of a directory artifact as a byte stream");
        assert!(
            !invalid.exists(),
            "directory artifact export created a pseudo file"
        );

        fixture.sync_resolve_keep(&client, &conflict.id, "local");
        fixture.sync_run_ok(&client);
        assert_eq!(
            remote_file_bytes(&fixture, "watched.txt"),
            b"written-after-install"
        );
        assert_in_sync(&fixture, &client);
    });
}

#[test]
#[ignore = "requires isolated S3 and the Managed Sync CLI; run with cargo x test-managed-sync"]
fn managed_sync_rename_edit_and_rename_rename_conflicts_both_authorities() {
    for_each_authority(|publication| {
        let fixture = Fixture::new(publication);
        fixture.remote_create_file("source.txt", b"base", false);
        let left = fixture.sync_client("rename-edit-left");
        let right = fixture.sync_client("rename-edit-right");
        fixture.sync_run_ok(&left);
        fixture.sync_run_ok(&right);

        fs::rename(
            left.directory.join("source.txt"),
            left.directory.join("moved.txt"),
        )
        .unwrap();
        write_local(&right.directory.join("source.txt"), b"edited");
        fixture.sync_run_ok(&left);
        let _ = fixture.sync_run(&right);
        let status = fixture.sync_status(&right);
        let conflict = status.conflict("source.txt").clone();
        assert_eq!(conflict.original_path.as_deref(), Some("source.txt"));
        assert_eq!(conflict.desired_path.as_deref(), Some("source.txt"));
        assert_eq!(conflict.local, ConflictSide::CurrentFile);
        assert_eq!(conflict.remote, ConflictSide::Deleted);
        assert_eq!(
            fs::read(right.directory.join("source.txt")).unwrap(),
            b"edited"
        );
        fixture.sync_resolve_keep(&right, &conflict.id, "local");
        fixture.sync_run_ok(&right);
        assert_eq!(remote_file_bytes(&fixture, "source.txt"), b"edited");
        assert_eq!(remote_file_bytes(&fixture, "moved.txt"), b"base");
        assert_in_sync(&fixture, &right);

        let fixture = Fixture::new(publication);
        fixture.remote_create_file("source.txt", b"base", false);
        let left = fixture.sync_client("rename-left");
        let right = fixture.sync_client("rename-right");
        fixture.sync_run_ok(&left);
        fixture.sync_run_ok(&right);
        fs::rename(
            left.directory.join("source.txt"),
            left.directory.join("left-name.txt"),
        )
        .unwrap();
        fs::rename(
            right.directory.join("source.txt"),
            right.directory.join("right-name.txt"),
        )
        .unwrap();
        fixture.sync_run_ok(&left);
        let _ = fixture.sync_run(&right);
        let status = fixture.sync_status(&right);
        let conflict = status.conflict("source.txt").clone();
        assert_eq!(conflict.original_path.as_deref(), Some("source.txt"));
        assert_eq!(conflict.desired_path.as_deref(), Some("right-name.txt"));
        assert_eq!(conflict.local, ConflictSide::CurrentFile);
        assert_eq!(conflict.remote, ConflictSide::Deleted);
        assert_eq!(
            fs::read(right.directory.join("right-name.txt")).unwrap(),
            b"base"
        );
        assert_eq!(remote_file_bytes(&fixture, "left-name.txt"), b"base");
        let entry = status.entry("right-name.txt");
        assert_eq!(entry.original_path.as_deref(), Some("source.txt"));
        assert_eq!(entry.desired_path.as_deref(), Some("right-name.txt"));
        fixture.sync_resolve_keep(&right, &conflict.id, "local");
        fixture.sync_run_ok(&right);
        assert_eq!(remote_file_bytes(&fixture, "right-name.txt"), b"base");
        assert_in_sync(&fixture, &right);
    });
}

#[test]
#[ignore = "requires isolated S3 and the Managed Sync CLI; run with cargo x test-managed-sync"]
fn managed_sync_parent_removal_retains_child_conflicts_both_authorities() {
    for_each_authority(|publication| {
        let fixture = Fixture::new(publication);
        fixture.remote_create_directory("parent");
        fixture.remote_create_file("parent/old-child.txt", b"base", false);
        let left = fixture.sync_client("parent-left");
        let right = fixture.sync_client("parent-right");
        fixture.sync_run_ok(&left);
        fixture.sync_run_ok(&right);

        fs::remove_file(left.directory.join("parent/old-child.txt")).unwrap();
        fs::remove_dir(left.directory.join("parent")).unwrap();
        fixture.sync_run_ok(&left);
        write_local(&right.directory.join("parent/new-child.txt"), b"new");
        let _ = fixture.sync_run(&right);

        assert_eq!(
            fs::read(right.directory.join("parent/old-child.txt")).unwrap(),
            b"base"
        );
        assert_eq!(
            fs::read(right.directory.join("parent/new-child.txt")).unwrap(),
            b"new"
        );
        let status = fixture.sync_status(&right);
        let old = status.conflict("parent/old-child.txt");
        assert_eq!(old.original_path.as_deref(), Some("parent/old-child.txt"));
        assert_eq!(old.desired_path.as_deref(), Some("parent/old-child.txt"));
        assert_eq!(old.local, ConflictSide::CurrentFile);
        assert_eq!(old.remote, ConflictSide::Deleted);
        assert_eq!(
            fs::read(right.directory.join("parent/old-child.txt")).unwrap(),
            b"base"
        );
        assert_eq!(
            fs::read(right.directory.join("parent/new-child.txt")).unwrap(),
            b"new"
        );
        assert!(!status.summary.in_sync);
        assert_eq!(fixture.remote_tree().unwrap().get("parent"), None);
    });
}
