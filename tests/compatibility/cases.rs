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

//! Cross-version object, service, staging, Sync and journal transitions.

use super::runtime::{
    Materialized, assert_tree_matches, copy_tree, current_binary, expected, legacy_binary,
    rewrite_staging, tree_files, write_config,
};
use crate::process::run_timeout;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::fs;
use std::path::Path;
use std::process::Command;
use std::time::Duration;

fn sha256(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .as_slice()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn parse_handle(output: &[u8]) -> String {
    String::from_utf8(output.to_vec())
        .expect("handle output is UTF-8")
        .trim()
        .split('\n')
        .next()
        .expect("handle output")
        .to_owned()
}

fn status_revision(output: &[u8]) -> String {
    String::from_utf8_lossy(output)
        .lines()
        .find_map(|line| line.strip_prefix("revision: ").map(str::to_owned))
        .expect("status revision")
}

#[test]
#[ignore = "requires isolated S3 and YY_LEGACY_BIN; run with cargo x test-compatibility"]
fn compatibility_fd1338c_object_stage_and_journal_cross_both_binaries() {
    let storage = Materialized::component("object");
    let fixture = expected("object");
    let expected_head = fixture["object"]["head"]["revision"]
        .as_str()
        .expect("object head revision");
    let current = current_binary();
    let legacy = legacy_binary();

    let current_status = storage.run(&current, &["status"]);
    current_status.success_or("current reader on fd1338c object fixture");
    assert_eq!(status_revision(&current_status.stdout), expected_head);
    let legacy_status = storage.run(&legacy, &["status"]);
    legacy_status.success_or("fd1338c reader on its object fixture");
    assert_eq!(status_revision(&legacy_status.stdout), expected_head);

    let restored_v1 = storage.path("restored-v1");
    storage
        .run(
            &current,
            &[
                "restore",
                restored_v1.to_str().unwrap(),
                "--revision",
                fixture["object"]["revision_v1"].as_str().unwrap(),
            ],
        )
        .success_or("current historical restore");
    assert_tree_matches(&restored_v1, &fixture["source_v1"]);

    let stage = storage.path("stage-pending");
    storage.copy_fixture("object", "stage-pending", &stage);
    let stage_config = storage.path("stage.json");
    rewrite_staging(
        &super::fixtures::fixture_root()
            .join("object")
            .join("object-volume.json"),
        &stage_config,
        &stage,
    );
    let handle = fixture["stage_pending"]["handle"]
        .as_str()
        .expect("pending handle");
    let current_read = storage.run(
        &current,
        &[
            "--volume",
            stage_config.to_str().unwrap(),
            "file",
            "read",
            handle,
            "--length",
            "4096",
        ],
    );
    current_read.success_or("current reader on fd1338c pending staging");
    assert_eq!(
        sha256(&current_read.stdout),
        fixture["stage_pending"]["bytes_sha256"].as_str().unwrap()
    );
    let pending_status = storage.run(
        &current,
        &["--volume", stage_config.to_str().unwrap(), "status"],
    );
    pending_status.success_or("current status on pending staging");
    assert!(
        String::from_utf8_lossy(&pending_status.stdout).contains("pending: true"),
        "fd1338c pending state was not retained:\n{}",
        pending_status.text()
    );
    let legacy_read = storage.run(
        &legacy,
        &[
            "--volume",
            stage_config.to_str().unwrap(),
            "file",
            "read",
            handle,
            "--length",
            "4096",
        ],
    );
    legacy_read.success_or("fd1338c reader on current pending staging");
    assert_eq!(legacy_read.stdout, current_read.stdout);
    storage
        .run(
            &current,
            &[
                "--volume",
                stage_config.to_str().unwrap(),
                "file",
                "fsync",
                handle,
            ],
        )
        .success_or("current publication of fd1338c pending bytes");
    let after_fsync = storage.run(
        &legacy,
        &[
            "--volume",
            stage_config.to_str().unwrap(),
            "file",
            "read",
            handle,
            "--length",
            "4096",
        ],
    );
    after_fsync.success_or("fd1338c read after current fsync");
    assert_eq!(after_fsync.stdout, current_read.stdout);
    storage
        .run(
            &current,
            &[
                "--volume",
                stage_config.to_str().unwrap(),
                "file",
                "close",
                handle,
            ],
        )
        .success_or("current close of recovered pending handle");

    let journal = storage.path("journal");
    storage.copy_fixture("object", "journal", &journal);
    let inspection = storage.run(
        &current,
        &[
            "publication",
            "inspect",
            "--state",
            journal.to_str().unwrap(),
            "--json",
        ],
    );
    inspection.success_or("current directory journal inspection");
    let inspection: Value = serde_json::from_slice(&inspection.stdout).unwrap();
    let intent = inspection["intents"]
        .as_array()
        .and_then(|intents| intents.first())
        .expect("fixture journal intent");
    assert_eq!(
        intent["commit_id"],
        fixture["journal"]["commit"].as_str().unwrap()
    );
    assert_eq!(intent["state"], "committed");
    let exported = storage.path("journal-export");
    storage
        .run(
            &current,
            &[
                "publication",
                "export",
                fixture["journal"]["commit"].as_str().unwrap(),
                "--state",
                journal.to_str().unwrap(),
                "--destination",
                exported.to_str().unwrap(),
            ],
        )
        .success_or("current source-free journal export");
    assert_tree_matches(&exported, &fixture["journal"]["source"]);
    let legacy_inspection = storage.run(
        &legacy,
        &[
            "publication",
            "inspect",
            "--state",
            journal.to_str().unwrap(),
            "--json",
        ],
    );
    legacy_inspection.success_or("fd1338c directory journal inspection");
    let legacy_inspection: Value = serde_json::from_slice(&legacy_inspection.stdout).unwrap();
    assert_eq!(legacy_inspection, inspection);

    let writer_source = storage.path("current-source");
    fs::create_dir_all(writer_source.join("nested")).unwrap();
    fs::write(writer_source.join("cross-version.txt"), b"current writer").unwrap();
    fs::write(writer_source.join("nested/empty"), b"").unwrap();
    storage
        .run(
            &current,
            &[
                "publish",
                writer_source.to_str().unwrap(),
                "--replace",
                "--commit-id",
                "00000000-0000-4000-8000-000000000104",
            ],
        )
        .success_or("current writer on fd1338c object storage");
    let legacy_restore = storage.path("legacy-current-restore");
    storage
        .run(&legacy, &["restore", legacy_restore.to_str().unwrap()])
        .success_or("fd1338c restore after current writer");
    assert_eq!(
        tree_files(&legacy_restore),
        tree_files(&writer_source),
        "fd1338c reader must observe exact current-writer bytes"
    );

    let broken = Materialized::component("object");
    let mut head = broken
        .raw_get(".yinyang/head")
        .expect("fixture object head");
    head[..8].copy_from_slice(b"YYHEAD99");
    let checksum = blake3::hash(&head[..head.len() - 32]);
    let checksum_offset = head.len() - 32;
    head[checksum_offset..].copy_from_slice(checksum.as_bytes());
    broken.raw_put(".yinyang/head", &head);
    let before = broken.inventory();
    let rejected = broken.run(&current, &["status"]);
    assert!(
        !rejected.status.success(),
        "future object head was accepted"
    );
    assert!(
        rejected
            .text()
            .to_ascii_lowercase()
            .contains(&"yyhead".to_ascii_lowercase())
            || rejected.text().to_ascii_lowercase().contains("profile"),
        "future object head did not reach the profile boundary:\n{}",
        rejected.text()
    );
    assert_eq!(
        broken.inventory(),
        before,
        "future object profile rejection mutated storage"
    );
}

#[test]
#[ignore = "requires isolated S3 and YY_LEGACY_BIN; run with cargo x test-compatibility"]
fn compatibility_fd1338c_service_cross_both_binaries() {
    let storage = Materialized::component("service");
    let fixture = expected("service");
    let database = storage.path("metadata.db");
    storage.copy_fixture("service", "metadata.db", &database);
    let current = current_binary();
    let legacy = legacy_binary();
    let staging = storage.path("service-staging");

    let service = super::runtime::Service::start(&storage, &current, &database);
    let config = storage.path("service-current.json");
    write_config(
        &config,
        "compat-service",
        "mount",
        &staging,
        "service",
        Some(service.address),
    );
    let opened = storage.run(
        &current,
        &[
            "--volume",
            config.to_str().unwrap(),
            "file",
            "open",
            "service.txt",
        ],
    );
    opened.success_or("current service open");
    let handle = parse_handle(&opened.stdout);
    let bytes = storage.run(
        &current,
        &[
            "--volume",
            config.to_str().unwrap(),
            "file",
            "read",
            &handle,
        ],
    );
    bytes.success_or("current service read");
    assert_eq!(
        sha256(&bytes.stdout),
        fixture["bytes_sha256"].as_str().unwrap()
    );

    storage
        .run(
            &current,
            &[
                "--volume",
                config.to_str().unwrap(),
                "file",
                "create",
                "current-service.txt",
            ],
        )
        .success_or("current service create");
    let opened = storage.run(
        &current,
        &[
            "--volume",
            config.to_str().unwrap(),
            "file",
            "open",
            "current-service.txt",
            "--write",
        ],
    );
    opened.success_or("current service writable open");
    let write_handle = parse_handle(&opened.stdout);
    let payload = storage.path("current-service-input");
    fs::write(&payload, b"current service writer").unwrap();
    storage
        .run(
            &current,
            &[
                "--volume",
                config.to_str().unwrap(),
                "file",
                "write",
                &write_handle,
                payload.to_str().unwrap(),
            ],
        )
        .success_or("current service write");
    storage
        .run(
            &current,
            &[
                "--volume",
                config.to_str().unwrap(),
                "file",
                "fsync",
                &write_handle,
            ],
        )
        .success_or("current service fsync");
    storage
        .run(
            &current,
            &[
                "--volume",
                config.to_str().unwrap(),
                "file",
                "close",
                &write_handle,
            ],
        )
        .success_or("current service close");
    service.stop();

    let service = super::runtime::Service::start(&storage, &legacy, &database);
    let legacy_config = storage.path("service-legacy.json");
    write_config(
        &legacy_config,
        "compat-service",
        "mount",
        &staging,
        "service",
        Some(service.address),
    );
    let current_service_hash = sha256(b"current service writer");
    let fixture_service_hash = fixture["bytes_sha256"].as_str().unwrap();
    for (path, expected) in [
        ("service.txt", fixture_service_hash),
        ("current-service.txt", current_service_hash.as_str()),
    ] {
        let opened = storage.run(
            &legacy,
            &[
                "--volume",
                legacy_config.to_str().unwrap(),
                "file",
                "open",
                path,
            ],
        );
        opened.success_or("fd1338c service open");
        let handle = parse_handle(&opened.stdout);
        let bytes = storage.run(
            &legacy,
            &[
                "--volume",
                legacy_config.to_str().unwrap(),
                "file",
                "read",
                &handle,
            ],
        );
        bytes.success_or("fd1338c service read");
        assert_eq!(sha256(&bytes.stdout), expected, "service bytes for {path}");
    }
    service.stop();
}

#[test]
#[ignore = "requires isolated S3 and YY_LEGACY_BIN; run with cargo x test-compatibility"]
fn compatibility_fd1338c_service_unknown_profile_rejects_without_mutation() {
    let storage = Materialized::component("service");
    let database = storage.path("metadata.db");
    storage.copy_fixture("service", "metadata.db", &database);
    sqlite_execute(
        &database,
        "UPDATE authority SET profile='yinyang-sqlite-99'",
    );
    let before = tree_hashes(storage.temp.path());
    let rejected = run_timeout(
        storage.command(
            &current_binary(),
            &[
                "serve",
                "--database",
                database.to_str().unwrap(),
                "--listen",
                "127.0.0.1:0",
            ],
        ),
        Duration::from_secs(30),
    );
    assert!(
        !rejected.status.success(),
        "unknown service profile was accepted"
    );
    assert!(
        rejected.text().to_ascii_lowercase().contains("profile")
            || rejected.text().to_ascii_lowercase().contains("unsupported"),
        "service rejection did not reach the profile boundary:\n{}",
        rejected.text()
    );
    assert_eq!(
        tree_hashes(storage.temp.path()),
        before,
        "unknown service profile rejection mutated the SQLite authority"
    );
}

#[test]
#[ignore = "requires isolated S3 and YY_LEGACY_BIN; run with cargo x test-compatibility"]
fn compatibility_fd1338c_sync_replica_and_backup_cross_both_binaries() {
    let storage = Materialized::component("sync");
    let state = storage.path("state");
    let local = storage.path("local");
    let backup = storage.path("backup");
    storage.copy_fixture("sync", "backup", &backup);
    let config = storage.path("sync.json");
    write_config(&config, "compat-sync", "sync", &state, "object", None);
    let current = current_binary();
    let legacy = legacy_binary();
    storage
        .run(
            &legacy,
            &[
                "--volume",
                config.to_str().unwrap(),
                "sync",
                "restore",
                "--backup",
                backup.to_str().unwrap(),
                "--directory",
                local.to_str().unwrap(),
            ],
        )
        .success_or("fd1338c restore of its own Sync backup");
    assert_eq!(
        tree_files(&local),
        tree_files(&backup.join("root")),
        "fd1338c restore produced different initial bytes"
    );
    fs::copy(
        super::fixtures::fixture_root()
            .join("sync")
            .join("local")
            .join("sync.txt"),
        local.join("sync.txt"),
    )
    .expect("apply fd1338c pending local edit after isolated restore");

    let before = storage.run(
        &current,
        &[
            "--volume",
            config.to_str().unwrap(),
            "sync",
            "status",
            "--json",
        ],
    );
    before.success_or("current Sync status on fd1338c state");
    let before: Value = serde_json::from_slice(&before.stdout).unwrap();
    assert_eq!(before["schema"], "yinyang.sync.status.v1");
    assert_eq!(before["summary"]["in_sync"], false);

    storage
        .run(
            &current,
            &["--volume", config.to_str().unwrap(), "sync", "run"],
        )
        .success_or("current Sync run on fd1338c state");
    let after = storage.run(
        &current,
        &[
            "--volume",
            config.to_str().unwrap(),
            "sync",
            "status",
            "--json",
        ],
    );
    after.success_or("current Sync status after transition");
    let after: Value = serde_json::from_slice(&after.stdout).unwrap();
    assert_eq!(after["summary"]["in_sync"], true);

    let legacy_status = storage.run(
        &legacy,
        &[
            "--volume",
            config.to_str().unwrap(),
            "sync",
            "status",
            "--json",
        ],
    );
    legacy_status.success_or("fd1338c Sync reader on current state");
    let legacy_status: Value = serde_json::from_slice(&legacy_status.stdout).unwrap();
    assert_eq!(legacy_status["summary"]["in_sync"], true);

    let restored_state = storage.path("restored-state");
    let restored_local = storage.path("restored-local");
    let restore_config = storage.path("restore.json");
    write_config(
        &restore_config,
        "compat-sync-restored",
        "sync",
        &restored_state,
        "object",
        None,
    );
    storage
        .run(
            &current,
            &[
                "--volume",
                restore_config.to_str().unwrap(),
                "sync",
                "restore",
                "--backup",
                backup.to_str().unwrap(),
                "--directory",
                restored_local.to_str().unwrap(),
            ],
        )
        .success_or("current restore of fd1338c backup package");
    assert_eq!(
        tree_files(&restored_local),
        tree_files(&backup.join("root")),
        "restored backup bytes differ from the fd1338c package"
    );
}

#[test]
fn compatibility_unknown_profiles_reject_without_local_mutation() {
    let temp = tempfile::tempdir().expect("create compatibility rejection directory");

    let stage = temp.path().join("stage");
    copy_tree(
        &super::fixtures::fixture_root()
            .join("object")
            .join("stage-pending"),
        &stage,
    );
    sqlite_execute(
        &stage.join("staging.db"),
        "UPDATE authority SET profile='yinyang-stage-99'",
    );
    let stage_config = temp.path().join("stage.json");
    write_config(
        &stage_config,
        "unknown-stage",
        "mount",
        &stage,
        "object",
        None,
    );
    assert_profile_rejection(
        temp.path(),
        &["--volume", stage_config.to_str().unwrap(), "status"],
        Some(&stage),
    );

    let journal = temp.path().join("journal");
    copy_tree(
        &super::fixtures::fixture_root()
            .join("object")
            .join("journal"),
        &journal,
    );
    sqlite_execute(
        &journal.join("directory.db"),
        "UPDATE binding SET profile='yinyang-directory-99'",
    );
    assert_profile_rejection(
        temp.path(),
        &[
            "publication",
            "inspect",
            "--state",
            journal.to_str().unwrap(),
            "--json",
        ],
        Some(&journal),
    );

    let sync = temp.path().join("sync");
    copy_tree(
        &super::fixtures::fixture_root().join("sync").join("state"),
        &sync,
    );
    sqlite_execute(
        &sync.join("sync.db"),
        "UPDATE binding SET profile='yinyang-sync-99'",
    );
    let sync_config = temp.path().join("sync.json");
    write_config(&sync_config, "unknown-sync", "sync", &sync, "object", None);
    assert_profile_rejection(
        temp.path(),
        &[
            "--volume",
            sync_config.to_str().unwrap(),
            "sync",
            "status",
            "--json",
        ],
        Some(&sync),
    );

    let extra = temp.path().join("extra");
    copy_tree(
        &super::fixtures::fixture_root()
            .join("object")
            .join("journal"),
        &extra,
    );
    sqlite_execute(
        &extra.join("directory.db"),
        "ALTER TABLE publications ADD COLUMN future_value TEXT",
    );
    assert_profile_rejection(
        temp.path(),
        &[
            "publication",
            "inspect",
            "--state",
            extra.to_str().unwrap(),
            "--json",
        ],
        Some(&extra),
    );
}

fn sqlite_execute(database: &Path, sql: &str) {
    rusqlite::Connection::open(database)
        .unwrap()
        .execute_batch(sql)
        .unwrap();
}

fn assert_profile_rejection(temp: &Path, args: &[&str], local: Option<&Path>) {
    let before = local.map(tree_hashes);
    let mut command = Command::new(current_binary());
    command.args(args);
    command.current_dir(temp);
    for name in [
        "YINYANG_S3_BUCKET",
        "YINYANG_S3_ENDPOINT",
        "YINYANG_S3_REGION",
        "YINYANG_S3_ACCESS_KEY_ID",
        "YINYANG_S3_SECRET_ACCESS_KEY",
        "YINYANG_SERVICE_TOKEN",
        "YINYANG_STORAGE_PROFILE",
    ] {
        command.env_remove(name);
    }
    command
        .env("YINYANG_S3_BUCKET", "compatibility-rejection")
        .env("YINYANG_S3_ENDPOINT", "http://127.0.0.1:9")
        .env("YINYANG_S3_REGION", "us-east-1")
        .env("YINYANG_S3_ACCESS_KEY_ID", "compatibility-test")
        .env("YINYANG_S3_SECRET_ACCESS_KEY", "compatibility-test")
        .env("YINYANG_STORAGE_PROFILE", "minio");
    let rejected = run_timeout(command, Duration::from_secs(30));
    assert!(
        !rejected.status.success(),
        "unknown profile was accepted for {args:?}"
    );
    let diagnostic = rejected.text().to_ascii_lowercase();
    assert!(
        diagnostic.contains("profile")
            || diagnostic.contains("unsupported")
            || diagnostic.contains("corrupt")
            || diagnostic.contains("schema"),
        "rejection did not reach a profile/schema boundary for {args:?}:\n{}",
        rejected.text()
    );
    if let Some(local) = local {
        assert_eq!(
            tree_hashes(local),
            before.unwrap(),
            "unknown profile rejection mutated local state for {args:?}"
        );
    }
}

fn tree_hashes(root: &Path) -> Vec<(String, String)> {
    tree_files(root)
        .into_iter()
        .map(|(path, bytes)| (path, sha256(&bytes)))
        .collect()
}
