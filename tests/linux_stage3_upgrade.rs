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

//! Genuine `yinyang-stage-3` fixture production and installed-target upgrade.

#![cfg(target_os = "linux")]

#[path = "support/process.rs"]
mod process;
#[path = "support/raw_s3.rs"]
mod raw_s3;
#[path = "support/s3_gate.rs"]
mod s3_gate;
#[path = "support/upgrade.rs"]
mod upgrade;

use borsh::BorshDeserialize;
use process::run_timeout;
use raw_s3::RawS3;
use s3_gate::{CommitAction, S3Gate};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Instant;
use upgrade::{
    PROCESS_TIMEOUT, RemoteCleanup, ServiceProcess, Storage, assert_receipt,
    assert_sqlite_integrity, field, handle_line, open_handle, parse_file_commit, path_text,
    read_handle, receipt, require_environment, run_file_ok, run_ok, sha256_file,
    sqlite_journal_mode, target_source, transaction_identity, volume_command, volume_file_command,
    wait_gate_or_child_exit, wal_path, write_config, write_optional_evidence,
};

const PRODUCER_SOURCE: &str = "546f419f9158e156f9a561822f3cb3f307222584";
const PRODUCER_TREE: &str = "ba7c7f2c3bbebd16c01891bc2361d9d36217d503";
const PRODUCER_RELEASE: &str = "0.0.24+546f419f9158";
const PRODUCER_ARCHIVE: &str = "yinyang-0.0.24-ubuntu-24.04-x86_64-546f419f9158.tar.gz";
const PRODUCER_ARCHIVE_SHA256: &str =
    "771c94a5d5063ab2d61ac7a9896abfd2430d55baeb478bc9b3f97ae187790453";
const PRODUCER_ARCHIVE_BINARY_SHA256: &str =
    "dce3c6e1b8e4d5682f7e90cc6376686dac6c0d9be005ad67388279519a8696e6";
const FIXTURE_SCHEMA: &str = "yinyang.compatibility.stage3-fixture.v1";
const EXPECTED_SCHEMA: &str = "yinyang.compatibility.stage3-expected.v1";
const UPGRADE_SCHEMA: &str = "yinyang.linux-stage3-upgrade.v1";
const BASE_LENGTH: usize = 200_000;
const PENDING_LENGTH: usize = 150_008;
const SERVICE_LENGTH: usize = 131_072;

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

struct Authority {
    label: &'static str,
    root: String,
    config: PathBuf,
    staging: PathBuf,
    base_bytes: Vec<u8>,
    base_revision: String,
}

#[test]
fn stage3_fixture_manifest_is_immutable() {
    let root = fixture_root();
    let manifest = verify_fixture(&root).expect("verify stage-3 compatibility fixture");
    assert_eq!(manifest["schema"], FIXTURE_SCHEMA);
    assert_eq!(manifest["producer"]["commit"], PRODUCER_SOURCE);
    assert_eq!(
        producer_binary_sha(&manifest),
        PRODUCER_ARCHIVE_BINARY_SHA256
    );
}

#[test]
#[ignore = "requires YY_STAGE3_PRODUCER_BIN, isolated S3, and a new fixture output path"]
fn generate_stage3_fixture_from_546f419() {
    let producer = PathBuf::from(
        std::env::var_os("YY_STAGE3_PRODUCER_BIN")
            .expect("YY_STAGE3_PRODUCER_BIN is required to generate the fixture"),
    );
    assert!(producer.is_file(), "YY_STAGE3_PRODUCER_BIN is not a file");
    let output = PathBuf::from(
        std::env::var_os("YINYANG_STAGE3_FIXTURE_OUTPUT")
            .expect("YINYANG_STAGE3_FIXTURE_OUTPUT is required"),
    );
    assert!(
        !output.exists(),
        "fixture output already exists and must not be replaced: {}",
        output.display()
    );
    require_environment(&[], "stage-3 compatibility");

    let producer_sha = sha256_file(&producer);
    let binary_identity = if producer_sha == PRODUCER_ARCHIVE_BINARY_SHA256 {
        "trial-archive"
    } else {
        assert_eq!(
            std::env::var("YINYANG_STAGE3_PRODUCER_SOURCE").as_deref(),
            Ok(PRODUCER_SOURCE),
            "a rebuilt producer requires YINYANG_STAGE3_PRODUCER_SOURCE={PRODUCER_SOURCE}"
        );
        assert_eq!(
            std::env::var("YINYANG_STAGE3_PRODUCER_REBUILD").as_deref(),
            Ok("1"),
            "a rebuilt producer requires YINYANG_STAGE3_PRODUCER_REBUILD=1"
        );
        "source-pinned-rebuild"
    };

    let output_parent = output.parent().expect("fixture output parent");
    fs::create_dir_all(output_parent).expect("create fixture output parent");
    let staging_output = tempfile::Builder::new()
        .prefix(".stage3-fixture-")
        .tempdir_in(output_parent)
        .expect("create staged fixture output");
    let work = tempfile::tempdir().expect("create stage-3 fixture work directory");
    let storage = Storage::new("stage-3 compatibility");
    let mut cleanup = RemoteCleanup::new(Vec::new());

    let object = create_authority(&storage, &producer, work.path(), &mut cleanup, "object", 11);
    let clean_handle = open_handle(
        &producer,
        &storage,
        &object.root,
        &object.config,
        "base.txt",
        true,
    );
    let sparse_handle = open_handle(
        &producer,
        &storage,
        &object.root,
        &object.config,
        "base.txt",
        true,
    );
    write_offset(
        &producer,
        &storage,
        &object.root,
        &object.config,
        &sparse_handle,
        1000,
        b"removed-by-shrink",
    );
    write_offset(
        &producer,
        &storage,
        &object.root,
        &object.config,
        &sparse_handle,
        65_530,
        b"removed-cross",
    );
    truncate_handle(
        &producer,
        &storage,
        &object.root,
        &object.config,
        &sparse_handle,
        700,
    );
    truncate_handle(
        &producer,
        &storage,
        &object.root,
        &object.config,
        &sparse_handle,
        150_000,
    );
    write_offset(
        &producer,
        &storage,
        &object.root,
        &object.config,
        &sparse_handle,
        1000,
        b"stage3-1000",
    );
    write_offset(
        &producer,
        &storage,
        &object.root,
        &object.config,
        &sparse_handle,
        65_530,
        b"cross-sixty-four-kib",
    );
    write_offset(
        &producer,
        &storage,
        &object.root,
        &object.config,
        &sparse_handle,
        149_990,
        b"zero-extended-tail",
    );

    let mut pending_bytes = object.base_bytes.clone();
    pending_bytes.truncate(700);
    pending_bytes.resize(150_000, 0);
    splice(&mut pending_bytes, 1000, b"stage3-1000");
    splice(&mut pending_bytes, 65_530, b"cross-sixty-four-kib");
    pending_bytes.resize(PENDING_LENGTH, 0);
    splice(&mut pending_bytes, 149_990, b"zero-extended-tail");
    assert_eq!(pending_bytes.len(), PENDING_LENGTH);

    let frozen = create_authority(&storage, &producer, work.path(), &mut cleanup, "frozen", 23);
    let frozen_handle = open_handle(
        &producer,
        &storage,
        &frozen.root,
        &frozen.config,
        "base.txt",
        true,
    );
    write_offset(
        &producer,
        &storage,
        &frozen.root,
        &frozen.config,
        &frozen_handle,
        4096,
        b"frozen-request-before-commit",
    );
    freeze_before_commit(
        &producer,
        &storage,
        &frozen.root,
        &frozen.config,
        &frozen_handle,
    );
    let mut frozen_bytes = frozen.base_bytes.clone();
    splice(&mut frozen_bytes, 4096, b"frozen-request-before-commit");

    let unknown = create_authority(
        &storage,
        &producer,
        work.path(),
        &mut cleanup,
        "unknown",
        37,
    );
    let unknown_handle = open_handle(
        &producer,
        &storage,
        &unknown.root,
        &unknown.config,
        "base.txt",
        true,
    );
    write_offset(
        &producer,
        &storage,
        &unknown.root,
        &unknown.config,
        &unknown_handle,
        8192,
        b"unknown-response-after-commit",
    );
    lose_commit_response(
        &producer,
        &storage,
        &unknown.root,
        &unknown.config,
        &unknown_handle,
    );
    let mut unknown_bytes = unknown.base_bytes.clone();
    splice(&mut unknown_bytes, 8192, b"unknown-response-after-commit");

    let object_status = status(&producer, &storage, &object.root, &object.config);
    assert!(handle_line(&object_status, &clean_handle).contains("pending: false"));
    assert!(handle_line(&object_status, &clean_handle).contains("frozen: false"));
    assert!(handle_line(&object_status, &sparse_handle).contains("pending: true"));
    let frozen_status = status(&producer, &storage, &frozen.root, &frozen.config);
    assert!(handle_line(&frozen_status, &frozen_handle).contains("frozen: true"));
    let unknown_status = status(&producer, &storage, &unknown.root, &unknown.config);
    assert!(handle_line(&unknown_status, &unknown_handle).contains("frozen: true"));
    assert!(handle_line(&unknown_status, &unknown_handle).contains("handle error:"));

    let (frozen_commit, frozen_digest) =
        stage_identity(&frozen.staging.join("staging.db"), &frozen_handle);
    let (unknown_commit, unknown_digest) =
        stage_identity(&unknown.staging.join("staging.db"), &unknown_handle);
    let segment_ranges = stage_segment_ranges(&object.staging.join("staging.db"), &sparse_handle);
    assert!(segment_ranges.len() >= 4, "{segment_ranges:?}");

    let service_root = storage.root("service");
    run_ok(
        &mut storage.command(&producer, &["create"], &service_root, None),
        "create stage-3 service authority",
    );
    let service_database = work.path().join("service/metadata.db");
    fs::create_dir_all(
        service_database
            .parent()
            .expect("service metadata database parent"),
    )
    .expect("create service metadata directory");
    let service = ServiceProcess::start(&producer, &storage, &service_root, &service_database);
    let service_config = work.path().join("service-volume.json");
    write_config(
        &service_config,
        "stage3-service",
        "mount",
        &work.path().join("service-client"),
        "service",
        Some(&service.address.to_string()),
    );
    run_file_ok(
        &producer,
        &storage,
        &service_root,
        &service_config,
        &["create", "service.txt"],
    );
    let service_bytes = pattern(SERVICE_LENGTH, 53);
    let service_handle = open_handle(
        &producer,
        &storage,
        &service_root,
        &service_config,
        "service.txt",
        true,
    );
    write_bytes(
        &producer,
        &storage,
        &service_root,
        &service_config,
        &service_handle,
        &service_bytes,
    );
    run_file_ok(
        &producer,
        &storage,
        &service_root,
        &service_config,
        &["fsync", &service_handle],
    );
    run_file_ok(
        &producer,
        &storage,
        &service_root,
        &service_config,
        &["close", &service_handle],
    );
    let service_wal = wal_path(&service_database);
    let wal_bytes = fs::metadata(&service_wal)
        .unwrap_or_else(|error| panic!("fd1338c service WAL is missing: {error}"))
        .len();
    assert!(
        wal_bytes > 0,
        "service WAL must be non-empty before the kill"
    );
    let service_journal_mode = sqlite_journal_mode(&service_database);
    service.kill_without_cleanup();
    assert!(service_wal.metadata().expect("service WAL metadata").len() > 0);

    let fixture = staging_output.path().join("fixture");
    finalize_authority(&storage, &object, &fixture).expect("finalize stage-3 object fixture");
    finalize_authority(&storage, &frozen, &fixture).expect("finalize stage-3 frozen fixture");
    finalize_authority(&storage, &unknown, &fixture).expect("finalize stage-3 unknown fixture");
    copy_s3_prefix(
        &storage.raw,
        &service_root,
        &fixture.join("service/storage"),
    );
    copy_sqlite_with_wal(
        &service_database,
        &fixture.join("service/metadata/metadata.db"),
    );
    write_service_config(
        &fixture.join("service/volume.json"),
        &service_config,
        &fixture,
    );

    let expected = json!({
        "schema": EXPECTED_SCHEMA,
        "object": {
            "base_revision": object.base_revision,
            "base_sha256": sha256(&object.base_bytes),
            "clean": {
                "handle": clean_handle,
                "length": object.base_bytes.len(),
                "sha256": sha256(&object.base_bytes),
            },
            "sparse": {
                "handle": sparse_handle,
                "length": pending_bytes.len(),
                "sha256": sha256(&pending_bytes),
                "segment_ranges": segment_ranges,
            },
        },
        "frozen": {
            "base_revision": frozen.base_revision,
            "base_sha256": sha256(&frozen.base_bytes),
            "handle": frozen_handle,
            "commit": frozen_commit,
            "request_digest": frozen_digest,
            "length": frozen_bytes.len(),
            "sha256": sha256(&frozen_bytes),
        },
        "unknown": {
            "base_revision": unknown.base_revision,
            "base_sha256": sha256(&unknown.base_bytes),
            "handle": unknown_handle,
            "commit": unknown_commit,
            "request_digest": unknown_digest,
            "length": unknown_bytes.len(),
            "sha256": sha256(&unknown_bytes),
        },
        "service": {
            "path": "service.txt",
            "length": service_bytes.len(),
            "sha256": sha256(&service_bytes),
            "database": "metadata/metadata.db",
            "wal": "metadata/metadata.db-wal",
            "journal_mode": service_journal_mode,
            "wal_bytes": wal_bytes,
            "wal_sha256": sha256_file(&service_wal),
        },
    });
    write_json(&fixture.join("expected.json"), &expected);
    fs::write(fixture.join("README.md"), fixture_readme()).expect("write fixture README");

    let producer_metadata = json!({
        "commit": PRODUCER_SOURCE,
        "tree": PRODUCER_TREE,
        "release": PRODUCER_RELEASE,
        "binary_sha256": producer_sha,
        "binary_identity": binary_identity,
        "trial_archive": {
            "name": PRODUCER_ARCHIVE,
            "sha256": PRODUCER_ARCHIVE_SHA256,
            "binary_sha256": PRODUCER_ARCHIVE_BINARY_SHA256,
        },
    });
    let manifest = json!({
        "schema": FIXTURE_SCHEMA,
        "producer": producer_metadata,
        "fixture": {
            "object": {
                "base": "base.txt",
                "clean_handle": clean_handle,
                "sparse_handle": sparse_handle,
            },
            "frozen": {
                "commit": frozen_commit,
                "request_digest": frozen_digest,
            },
            "unknown": {
                "commit": unknown_commit,
                "request_digest": unknown_digest,
            },
            "service": {
                "path": "service.txt",
                "database": "metadata/metadata.db",
                "wal": "metadata/metadata.db-wal",
            },
        },
        "files": manifest_files(&fixture),
    });
    write_json(&fixture.join("manifest.json"), &manifest);
    verify_fixture(&fixture).expect("verify newly generated stage-3 fixture");

    fs::rename(&fixture, &output).unwrap_or_else(|error| {
        panic!(
            "move generated fixture from {} to {}: {error}",
            fixture.display(),
            output.display()
        )
    });
    cleanup.cleanup();
}

#[test]
#[ignore = "requires isolated S3 and YY_UPGRADE_BIN naming the installed target archive binary"]
fn installed_stage3_upgrade_preserves_sparse_segments_and_service_wal() {
    require_environment(
        &["YY_UPGRADE_BIN", "YY_UPGRADE_RELEASE"],
        "installed stage-3 upgrade",
    );
    let target = PathBuf::from(
        std::env::var_os("YY_UPGRADE_BIN")
            .expect("YY_UPGRADE_BIN must name the installed target binary"),
    );
    assert!(target.is_file(), "YY_UPGRADE_BIN does not name a file");
    let target_release = std::env::var("YY_UPGRADE_RELEASE")
        .expect("YY_UPGRADE_RELEASE must identify the installed target release");
    let target_source = target_source();
    let target_sha256 = sha256_file(&target);
    let started = Instant::now();
    let root = fixture_root();
    let fixture_manifest_path = root.join("manifest.json");
    let fixture_manifest_sha256 = sha256_file(&fixture_manifest_path);
    let fixture = verify_fixture(&root).expect("verify stage-3 compatibility fixture");
    let expected: Value = read_json(&root.join("expected.json"));
    let temp = tempfile::tempdir().expect("create installed stage-3 upgrade directory");
    let storage = Storage::new("installed stage-3 upgrade");
    let mut cleanup = RemoteCleanup::new(Vec::new());

    let object = materialize_component(
        &storage,
        &mut cleanup,
        &root,
        &expected,
        "object",
        temp.path(),
    );
    let frozen = materialize_component(
        &storage,
        &mut cleanup,
        &root,
        &expected,
        "frozen",
        temp.path(),
    );
    let unknown = materialize_component(
        &storage,
        &mut cleanup,
        &root,
        &expected,
        "unknown",
        temp.path(),
    );

    let object_status = status(&target, &storage, &object.root, &object.config);
    assert_handle_state(&object_status, &expected["object"]["clean"], false, false);
    assert_handle_state(&object_status, &expected["object"]["sparse"], true, false);
    assert_handle_state(
        &frozen_status(&target, &storage, &frozen),
        &expected["frozen"],
        true,
        true,
    );
    assert_handle_state(
        &unknown_status(&target, &storage, &unknown),
        &expected["unknown"],
        true,
        true,
    );

    assert_handle_bytes(
        &target,
        &storage,
        &object.root,
        &object.config,
        &expected["object"]["clean"],
        "target read clean staged base",
    );
    assert_handle_bytes(
        &target,
        &storage,
        &object.root,
        &object.config,
        &expected["object"]["sparse"],
        "target read sparse staged bytes",
    );
    assert_handle_bytes(
        &target,
        &storage,
        &frozen.root,
        &frozen.config,
        &expected["frozen"],
        "target read frozen staged bytes",
    );
    assert_handle_bytes(
        &target,
        &storage,
        &unknown.root,
        &unknown.config,
        &expected["unknown"],
        "target read unknown staged bytes",
    );

    let unknown_head_before = storage
        .raw
        .get(&format!("{}/.yinyang/head", unknown.root))
        .expect("unknown object head before receipt resolution");
    let unknown_commit = expected["unknown"]["commit"]
        .as_str()
        .expect("unknown fixture commit");
    let unknown_digest = expected["unknown"]["request_digest"]
        .as_str()
        .expect("unknown fixture digest");
    let unknown_retry = run_file_ok(
        &target,
        &storage,
        &unknown.root,
        &unknown.config,
        &[
            "fsync",
            expected["unknown"]["handle"]
                .as_str()
                .expect("unknown fixture handle"),
        ],
    );
    assert_eq!(parse_file_commit(&unknown_retry.stdout), unknown_commit);
    assert_eq!(
        storage.raw.get(&format!("{}/.yinyang/head", unknown.root)),
        Some(unknown_head_before),
        "resolving the unknown receipt issued a second object commit"
    );
    assert_receipt(
        &target,
        &storage,
        &unknown.root,
        unknown_commit,
        unknown_digest,
    );

    let frozen_head_before = storage
        .raw
        .get(&format!("{}/.yinyang/head", frozen.root))
        .expect("frozen object head before retry");
    let frozen_commit = expected["frozen"]["commit"]
        .as_str()
        .expect("frozen fixture commit");
    let frozen_digest = expected["frozen"]["request_digest"]
        .as_str()
        .expect("frozen fixture digest");
    let frozen_retry = run_file_ok(
        &target,
        &storage,
        &frozen.root,
        &frozen.config,
        &[
            "fsync",
            expected["frozen"]["handle"]
                .as_str()
                .expect("frozen fixture handle"),
        ],
    );
    assert_eq!(parse_file_commit(&frozen_retry.stdout), frozen_commit);
    assert_ne!(
        storage.raw.get(&format!("{}/.yinyang/head", frozen.root)),
        Some(frozen_head_before),
        "retrying the frozen request did not publish it"
    );
    assert_receipt(
        &target,
        &storage,
        &frozen.root,
        frozen_commit,
        frozen_digest,
    );

    let pending_handle = expected["object"]["sparse"]["handle"]
        .as_str()
        .expect("sparse fixture handle");
    let pending_publish = run_file_ok(
        &target,
        &storage,
        &object.root,
        &object.config,
        &["fsync", pending_handle],
    );
    let pending_commit = parse_file_commit(&pending_publish.stdout);
    let pending_receipt = receipt(&target, &storage, &object.root, &pending_commit);
    assert_eq!(field(&pending_receipt, "commit"), pending_commit);

    run_file_ok(
        &target,
        &storage,
        &object.root,
        &object.config,
        &["create", "target-new.txt"],
    );
    let target_handle = open_handle(
        &target,
        &storage,
        &object.root,
        &object.config,
        "target-new.txt",
        true,
    );
    let target_bytes = b"target installed local write\n";
    write_bytes(
        &target,
        &storage,
        &object.root,
        &object.config,
        &target_handle,
        target_bytes,
    );
    run_file_ok(
        &target,
        &storage,
        &object.root,
        &object.config,
        &["fsync", &target_handle],
    );
    run_file_ok(
        &target,
        &storage,
        &object.root,
        &object.config,
        &["close", &target_handle],
    );
    let target_read = open_handle(
        &target,
        &storage,
        &object.root,
        &object.config,
        "target-new.txt",
        false,
    );
    assert_eq!(
        read_handle(
            &target,
            &storage,
            &object.root,
            &object.config,
            &target_read,
        )
        .stdout,
        target_bytes
    );
    run_file_ok(
        &target,
        &storage,
        &object.root,
        &object.config,
        &["close", &target_read],
    );

    let service = materialize_service(&storage, &mut cleanup, &root, &expected, temp.path());
    let service_process = ServiceProcess::start_at(
        &target,
        &storage,
        &service.root,
        &service.database,
        service.address,
    );
    let service_handle = open_handle(
        &target,
        &storage,
        &service.root,
        &service.config,
        expected["service"]["path"].as_str().expect("service path"),
        false,
    );
    assert_eq!(
        read_handle(
            &target,
            &storage,
            &service.root,
            &service.config,
            &service_handle,
        )
        .stdout
        .len() as u64,
        expected["service"]["length"]
            .as_u64()
            .expect("service fixture length")
    );
    run_file_ok(
        &target,
        &storage,
        &service.root,
        &service.config,
        &["close", &service_handle],
    );
    service_process.stop();
    assert_sqlite_integrity(&service.database);

    for handle in [
        expected["object"]["clean"]["handle"]
            .as_str()
            .expect("clean fixture handle"),
        pending_handle,
    ] {
        run_file_ok(
            &target,
            &storage,
            &object.root,
            &object.config,
            &["close", handle],
        );
    }
    for (component, key) in [(&frozen, "frozen"), (&unknown, "unknown")] {
        run_file_ok(
            &target,
            &storage,
            &component.root,
            &component.config,
            &[
                "close",
                expected[key]["handle"].as_str().expect("fixture handle"),
            ],
        );
    }
    let doctor = run_ok(
        volume_command(&target, &storage, &object.root, &object.config, None)
            .args(["doctor", "--json"]),
        "target installed stage-3 object doctor",
    );
    let doctor: Value = serde_json::from_slice(&doctor.stdout).expect("parse target doctor");
    assert_eq!(doctor["schema"], "yinyang.operational-health.v1");
    assert_eq!(doctor["status"], "pass", "{doctor:#}");

    let evidence = json!({
        "schema": UPGRADE_SCHEMA,
        "status": "pass",
        "elapsed_seconds": started.elapsed().as_secs_f64(),
        "fixture": {
            "manifest_sha256": fixture_manifest_sha256,
            "schema": fixture["schema"],
            "producer": fixture["producer"],
        },
        "target": {
            "source": target_source,
            "release": target_release,
            "binary_path": target,
            "binary_sha256": target_sha256,
        },
        "retained": {
            "clean_handle": expected["object"]["clean"]["handle"],
            "sparse_handle": pending_handle,
            "sparse_sha256": expected["object"]["sparse"]["sha256"],
            "sparse_segment_ranges": expected["object"]["sparse"]["segment_ranges"],
            "frozen_commit": frozen_commit,
            "frozen_request_digest": frozen_digest,
            "unknown_commit": unknown_commit,
            "unknown_request_digest": unknown_digest,
            "pending_commit": pending_commit,
            "pending_request_digest": field(&pending_receipt, "request digest"),
            "service_wal_bytes": expected["service"]["wal_bytes"],
            "service_wal_sha256": expected["service"]["wal_sha256"],
            "service_journal_mode": expected["service"]["journal_mode"],
        },
        "target_convergence": {
            "sparse_bytes": true,
            "unknown_receipt_same_identity": true,
            "frozen_retry_same_identity": true,
            "pending_published": true,
            "new_local_write": true,
            "service_wal_reopened": true,
            "sqlite_integrity": "ok",
            "doctor": doctor["status"],
        },
    });
    write_optional_evidence("YINYANG_STAGE3_EVIDENCE", &evidence);
    println!(
        "linux-stage3-upgrade: {}",
        serde_json::to_string(&evidence).expect("serialize stage-3 evidence")
    );
    cleanup.cleanup();
}

fn create_authority(
    storage: &Storage,
    producer: &Path,
    work: &Path,
    cleanup: &mut RemoteCleanup,
    label: &'static str,
    seed: u8,
) -> Authority {
    let root = storage.root(label);
    cleanup.add(root.clone());
    run_ok(
        &mut storage.command(producer, &["create"], &root, None),
        &format!("create {label} stage-3 authority"),
    );
    let config = work.join(format!("{label}.json"));
    let staging = work.join(format!("{label}-staging"));
    write_config(
        &config,
        &format!("stage3-{label}"),
        "mount",
        &staging,
        "object",
        None,
    );
    run_file_ok(producer, storage, &root, &config, &["create", "base.txt"]);
    let base_bytes = pattern(BASE_LENGTH, seed);
    let base_handle = open_handle(producer, storage, &root, &config, "base.txt", true);
    write_bytes(producer, storage, &root, &config, &base_handle, &base_bytes);
    run_file_ok(producer, storage, &root, &config, &["fsync", &base_handle]);
    run_file_ok(producer, storage, &root, &config, &["close", &base_handle]);
    let base_revision = status(producer, storage, &root, &config)
        .lines()
        .find_map(|line| line.strip_prefix("remote latest: ").map(str::to_owned))
        .expect("base authority remote revision");
    Authority {
        label,
        root,
        config,
        staging,
        base_bytes,
        base_revision,
    }
}

fn finalize_authority(
    storage: &Storage,
    authority: &Authority,
    fixture: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let destination = fixture.join(authority.label);
    copy_s3_prefix(&storage.raw, &authority.root, &destination.join("storage"));
    copy_staging(&authority.staging, &destination.join("staging"))?;
    let mut config: Value = read_json(&authority.config);
    config["staging"] = Value::String("staging".to_owned());
    write_json(&destination.join("volume.json"), &config);
    Ok(())
}

fn materialize_component(
    storage: &Storage,
    cleanup: &mut RemoteCleanup,
    fixture_root: &Path,
    expected: &Value,
    component: &'static str,
    work: &Path,
) -> MaterializedComponent {
    let root = storage.root(component);
    cleanup.add(root.clone());
    copy_tree_to_s3(
        &storage.raw,
        &fixture_root.join(component).join("storage"),
        &root,
    );
    let staging = work.join(format!("{component}-staging"));
    copy_tree(&fixture_root.join(component).join("staging"), &staging);
    let config = work.join(format!("{component}.json"));
    let mut document: Value = read_json(&fixture_root.join(component).join("volume.json"));
    document["staging"] = Value::String(staging.display().to_string());
    write_json(&config, &document);
    if component == "object" {
        assert!(expected["object"]["sparse"]["segment_ranges"].is_array());
    }
    MaterializedComponent { root, config }
}

struct MaterializedComponent {
    root: String,
    config: PathBuf,
}

struct MaterializedService {
    root: String,
    config: PathBuf,
    database: PathBuf,
    address: SocketAddr,
}

fn materialize_service(
    storage: &Storage,
    cleanup: &mut RemoteCleanup,
    fixture_root: &Path,
    expected: &Value,
    work: &Path,
) -> MaterializedService {
    let root = storage.root("service");
    cleanup.add(root.clone());
    copy_tree_to_s3(
        &storage.raw,
        &fixture_root.join("service").join("storage"),
        &root,
    );
    let metadata = work.join("service-metadata");
    copy_tree(&fixture_root.join("service").join("metadata"), &metadata);
    let database = metadata.join("metadata.db");
    assert!(database.is_file(), "service fixture database is missing");
    let wal = wal_path(&database);
    assert_eq!(
        fs::metadata(&wal).expect("service fixture WAL").len(),
        expected["service"]["wal_bytes"]
            .as_u64()
            .expect("service fixture WAL length")
    );
    let listener = std::net::TcpListener::bind("127.0.0.1:0")
        .expect("reserve stage-3 service acceptance port");
    let address = listener.local_addr().expect("stage-3 service address");
    drop(listener);
    let config = work.join("service.json");
    write_config(
        &config,
        "stage3-service-target",
        "mount",
        &work.join("service-client-target"),
        "service",
        Some(&address.to_string()),
    );
    MaterializedService {
        root,
        config,
        database,
        address,
    }
}

fn frozen_status(binary: &Path, storage: &Storage, component: &MaterializedComponent) -> String {
    status(binary, storage, &component.root, &component.config)
}

fn unknown_status(binary: &Path, storage: &Storage, component: &MaterializedComponent) -> String {
    status(binary, storage, &component.root, &component.config)
}

fn assert_handle_state(status: &str, expected: &Value, pending: bool, frozen: bool) {
    let handle = expected["handle"].as_str().expect("fixture handle");
    let line = handle_line(status, handle);
    assert_eq!(
        line.contains("pending: true"),
        pending,
        "unexpected pending state for {handle}: {line}"
    );
    assert_eq!(
        line.contains("frozen: true"),
        frozen,
        "unexpected frozen state for {handle}: {line}"
    );
}

fn assert_handle_bytes(
    binary: &Path,
    storage: &Storage,
    root: &str,
    config: &Path,
    expected: &Value,
    context: &str,
) {
    let handle = expected["handle"].as_str().expect("fixture handle");
    let output = run_ok(
        volume_file_command(binary, storage, root, config).args(["read", handle]),
        context,
    );
    assert_eq!(
        output.stdout.len() as u64,
        expected["length"].as_u64().expect("fixture length"),
        "{context} length"
    );
    assert_eq!(
        sha256(&output.stdout),
        expected["sha256"].as_str().expect("fixture SHA-256"),
        "{context} bytes"
    );
}

fn freeze_before_commit(binary: &Path, storage: &Storage, root: &str, config: &Path, handle: &str) {
    let gate = S3Gate::start(CommitAction::DropBeforeCommit);
    let mut command = volume_file_command(binary, storage, root, config);
    command
        .args(["fsync", handle])
        .env("YINYANG_S3_ENDPOINT", gate.endpoint())
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = command.spawn().expect("spawn stage-3 frozen fsync");
    wait_gate_or_child_exit(&gate, &mut child, "stage-3 frozen fsync");
    child.kill().expect("kill frozen stage-3 fsync");
    let _ = child.wait();
    gate.release();
    gate.wait_original_dropped();
    drop(gate);
}

fn lose_commit_response(binary: &Path, storage: &Storage, root: &str, config: &Path, handle: &str) {
    let gate = S3Gate::start(CommitAction::DropResponse);
    let mut command = volume_file_command(binary, storage, root, config);
    command
        .args(["fsync", handle])
        .env("YINYANG_S3_ENDPOINT", gate.endpoint());
    let lost = run_timeout(command, PROCESS_TIMEOUT);
    assert!(
        !lost.status.success(),
        "stage-3 producer must observe a lost response as failure:\n{}",
        lost.text()
    );
    gate.wait_intercepted();
    drop(gate);
}

fn write_bytes(
    binary: &Path,
    storage: &Storage,
    root: &str,
    config: &Path,
    handle: &str,
    bytes: &[u8],
) {
    let payload = tempfile::NamedTempFile::new().expect("create stage-3 payload file");
    fs::write(payload.path(), bytes).expect("write stage-3 payload");
    run_file_ok(
        binary,
        storage,
        root,
        config,
        &["write", handle, path_text(payload.path())],
    );
}

fn write_offset(
    binary: &Path,
    storage: &Storage,
    root: &str,
    config: &Path,
    handle: &str,
    offset: u64,
    bytes: &[u8],
) {
    let payload = tempfile::NamedTempFile::new().expect("create stage-3 offset payload");
    fs::write(payload.path(), bytes).expect("write stage-3 offset payload");
    run_file_ok(
        binary,
        storage,
        root,
        config,
        &[
            "write",
            handle,
            path_text(payload.path()),
            "--offset",
            &offset.to_string(),
        ],
    );
}

fn truncate_handle(
    binary: &Path,
    storage: &Storage,
    root: &str,
    config: &Path,
    handle: &str,
    length: u64,
) {
    run_file_ok(
        binary,
        storage,
        root,
        config,
        &["truncate", handle, &length.to_string()],
    );
}

fn status(binary: &Path, storage: &Storage, root: &str, config: &Path) -> String {
    let output = run_ok(
        volume_command(binary, storage, root, config, None).args(["status"]),
        "inspect stage-3 volume",
    );
    String::from_utf8(output.stdout).expect("stage-3 status is UTF-8")
}

fn stage_identity(database: &Path, handle: &str) -> (String, String) {
    let id = uuid::Uuid::parse_str(handle).expect("stage-3 handle UUID");
    let connection =
        rusqlite::Connection::open_with_flags(database, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
            .expect("open stage-3 database read-only");
    let bytes: Vec<u8> = connection
        .query_row(
            "SELECT record FROM handles WHERE id=?1",
            [id.as_bytes().as_slice()],
            |row| row.get(0),
        )
        .expect("read frozen stage-3 handle record");
    let record: StageRecord = borsh::from_slice(&bytes).expect("decode stage-3 handle record");
    let plan = record.plan.expect("handle must retain a frozen request");
    transaction_identity(&plan)
}

fn stage_segment_ranges(database: &Path, handle: &str) -> Vec<(u64, u64)> {
    let id = uuid::Uuid::parse_str(handle).expect("sparse stage-3 handle UUID");
    let connection =
        rusqlite::Connection::open_with_flags(database, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
            .expect("open sparse stage-3 database read-only");
    let mut query = connection
        .prepare(
            "SELECT position,length(data) FROM segments
             WHERE handle=?1 ORDER BY position",
        )
        .expect("prepare stage-3 segment query");
    query
        .query_map([id.as_bytes().as_slice()], |row| {
            Ok((row.get::<_, i64>(0)? as u64, row.get::<_, i64>(1)? as u64))
        })
        .expect("query stage-3 segments")
        .collect::<rusqlite::Result<Vec<_>>>()
        .expect("read stage-3 segments")
}

fn write_service_config(path: &Path, source: &Path, fixture: &Path) {
    let mut config: Value = read_json(source);
    config["staging"] = Value::String(
        fixture
            .join("service/client-placeholder")
            .display()
            .to_string(),
    );
    config["service_address"] = Value::String("127.0.0.1:1".to_owned());
    write_json(path, &config);
}

fn manifest_files(root: &Path) -> Vec<Value> {
    let mut files = Vec::new();
    collect_files(root, root, &mut files);
    files.sort_by(|left, right| {
        left.as_str()
            .expect("manifest path")
            .cmp(right.as_str().expect("manifest path"))
    });
    files
}

fn collect_files(root: &Path, path: &Path, output: &mut Vec<Value>) {
    for entry in
        fs::read_dir(path).unwrap_or_else(|error| panic!("read {}: {error}", path.display()))
    {
        let entry = entry.expect("read stage-3 fixture entry");
        let child = entry.path();
        if entry
            .file_type()
            .expect("stage-3 fixture entry type")
            .is_dir()
        {
            collect_files(root, &child, output);
            continue;
        }
        let relative = child
            .strip_prefix(root)
            .expect("stage-3 fixture path is relative")
            .to_str()
            .expect("stage-3 fixture path is UTF-8")
            .replace('\\', "/");
        if matches!(relative.as_str(), "manifest.json" | "README.md")
            || relative.ends_with("-shm")
            || relative.ends_with("runtime.lock")
        {
            continue;
        }
        let bytes = fs::read(&child).unwrap_or_else(|error| {
            panic!("read stage-3 fixture file {}: {error}", child.display())
        });
        output.push(json!({
            "path": relative,
            "length": bytes.len(),
            "sha256": sha256(&bytes),
        }));
    }
}

fn verify_fixture(root: &Path) -> Result<Value, String> {
    let manifest_path = root.join("manifest.json");
    let manifest: Value = serde_json::from_slice(
        &fs::read(&manifest_path)
            .map_err(|error| format!("read {}: {error}", manifest_path.display()))?,
    )
    .map_err(|error| format!("parse {}: {error}", manifest_path.display()))?;
    if manifest["schema"] != FIXTURE_SCHEMA {
        return Err(format!(
            "unexpected stage-3 fixture schema: {}",
            manifest["schema"]
        ));
    }
    if manifest["producer"]["commit"] != PRODUCER_SOURCE {
        return Err(format!(
            "unexpected stage-3 fixture producer: {}",
            manifest["producer"]["commit"]
        ));
    }
    let binary_sha = producer_binary_sha(&manifest);
    if binary_sha.len() != 64 {
        return Err(format!("invalid stage-3 producer SHA-256: {binary_sha}"));
    }
    let mut listed = BTreeMap::new();
    for entry in manifest["files"]
        .as_array()
        .ok_or("stage-3 fixture files must be an array")?
    {
        let path = entry["path"]
            .as_str()
            .ok_or("stage-3 fixture path must be a string")?;
        if path.starts_with('/')
            || path
                .split('/')
                .any(|component| component.is_empty() || component == "..")
        {
            return Err(format!("unsafe stage-3 fixture path: {path:?}"));
        }
        let length = entry["length"]
            .as_u64()
            .ok_or("stage-3 fixture length must be an integer")?;
        let hash = entry["sha256"]
            .as_str()
            .ok_or("stage-3 fixture SHA-256 must be a string")?;
        if listed
            .insert(path.to_owned(), (length, hash.to_owned()))
            .is_some()
        {
            return Err(format!("duplicate stage-3 fixture path: {path}"));
        }
    }
    let mut actual = BTreeSet::new();
    collect_paths(root, root, &mut actual)?;
    actual.remove("manifest.json");
    actual.remove("README.md");
    if listed.keys().cloned().collect::<BTreeSet<_>>() != actual {
        return Err("stage-3 fixture directory and manifest file list disagree".to_owned());
    }
    for (relative, (expected_length, expected_hash)) in listed {
        let bytes = fs::read(root.join(&relative))
            .map_err(|error| format!("read stage-3 fixture {relative}: {error}"))?;
        if bytes.len() as u64 != expected_length {
            return Err(format!("stage-3 fixture length changed for {relative}"));
        }
        if sha256(&bytes) != expected_hash {
            return Err(format!("stage-3 fixture bytes changed for {relative}"));
        }
    }
    Ok(manifest)
}

fn producer_binary_sha(manifest: &Value) -> &str {
    manifest["producer"]["binary_sha256"]
        .as_str()
        .expect("stage-3 fixture producer binary SHA-256")
}

fn collect_paths(root: &Path, path: &Path, output: &mut BTreeSet<String>) -> Result<(), String> {
    for entry in fs::read_dir(path)
        .map_err(|error| format!("read stage-3 fixture {}: {error}", path.display()))?
    {
        let entry = entry.map_err(|error| format!("read stage-3 fixture entry: {error}"))?;
        let child = entry.path();
        if entry
            .file_type()
            .map_err(|error| format!("read stage-3 fixture entry type: {error}"))?
            .is_dir()
        {
            collect_paths(root, &child, output)?;
        } else {
            output.insert(
                child
                    .strip_prefix(root)
                    .map_err(|error| format!("stage-3 fixture path: {error}"))?
                    .to_str()
                    .ok_or("stage-3 fixture path is not UTF-8")?
                    .replace('\\', "/"),
            );
        }
    }
    Ok(())
}

fn copy_s3_prefix(raw: &RawS3, source: &str, destination: &Path) {
    let prefix = format!("{source}/");
    for (key, object) in raw.inventory(&prefix) {
        let relative = key
            .strip_prefix(&prefix)
            .unwrap_or_else(|| panic!("S3 key {key:?} is outside prefix {prefix:?}"));
        let target = destination.join(relative);
        if let Some(parent) = target.parent() {
            fs::create_dir_all(parent).expect("create stage-3 S3 fixture parent");
        }
        fs::write(&target, object.bytes).unwrap_or_else(|error| {
            panic!("write stage-3 S3 fixture {}: {error}", target.display())
        });
    }
}

fn copy_tree_to_s3(raw: &RawS3, source: &Path, root: &str) {
    let mut files = Vec::new();
    local_files(source, source, &mut files);
    for relative in files {
        let bytes = fs::read(source.join(&relative))
            .unwrap_or_else(|error| panic!("read fixture source {relative}: {error}"));
        raw.put(&format!("{root}/{relative}"), &bytes);
    }
}

fn local_files(root: &Path, path: &Path, output: &mut Vec<String>) {
    for entry in
        fs::read_dir(path).unwrap_or_else(|error| panic!("read {}: {error}", path.display()))
    {
        let entry = entry.expect("read fixture source entry");
        let child = entry.path();
        if entry
            .file_type()
            .expect("fixture source entry type")
            .is_dir()
        {
            local_files(root, &child, output);
        } else {
            output.push(
                child
                    .strip_prefix(root)
                    .expect("fixture source path is relative")
                    .to_str()
                    .expect("fixture source path is UTF-8")
                    .replace('\\', "/"),
            );
        }
    }
}

fn copy_staging(source: &Path, destination: &Path) -> Result<(), Box<dyn std::error::Error>> {
    fs::create_dir_all(destination)?;
    for name in ["staging.db", "staging.db-wal"] {
        let path = source.join(name);
        if !path.is_file() {
            continue;
        }
        if name.ends_with("-wal") && path.metadata()?.len() == 0 {
            continue;
        }
        fs::copy(&path, destination.join(name))?;
    }
    Ok(())
}

fn copy_sqlite_with_wal(source: &Path, destination: &Path) {
    if let Some(parent) = destination.parent() {
        fs::create_dir_all(parent).expect("create service fixture database parent");
    }
    fs::copy(source, destination).expect("copy service fixture database");
    let source_wal = wal_path(source);
    let destination_wal = wal_path(destination);
    assert!(
        source_wal.metadata().expect("service fixture WAL").len() > 0,
        "service fixture WAL must be non-empty"
    );
    fs::copy(&source_wal, &destination_wal).expect("copy service fixture WAL");
}

fn copy_tree(source: &Path, destination: &Path) {
    if source.is_dir() {
        fs::create_dir_all(destination)
            .unwrap_or_else(|error| panic!("create {}: {error}", destination.display()));
        for entry in fs::read_dir(source).unwrap() {
            let entry = entry.unwrap();
            copy_tree(&entry.path(), &destination.join(entry.file_name()));
        }
    } else {
        if let Some(parent) = destination.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        fs::copy(source, destination).unwrap_or_else(|error| {
            panic!(
                "copy {} to {}: {error}",
                source.display(),
                destination.display()
            )
        });
    }
}

fn pattern(length: usize, seed: u8) -> Vec<u8> {
    (0..length)
        .map(|index| seed.wrapping_add((index % 251) as u8))
        .collect()
}

fn splice(bytes: &mut [u8], offset: usize, replacement: &[u8]) {
    bytes[offset..offset + replacement.len()].copy_from_slice(replacement);
}

fn sha256(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .as_slice()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn read_json(path: &Path) -> Value {
    serde_json::from_slice(
        &fs::read(path).unwrap_or_else(|error| panic!("read {}: {error}", path.display())),
    )
    .unwrap_or_else(|error| panic!("parse {}: {error}", path.display()))
}

fn write_json(path: &Path, value: &Value) {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).expect("create stage-3 JSON parent");
    }
    let mut bytes = serde_json::to_vec_pretty(value).expect("serialize stage-3 JSON");
    bytes.push(b'\n');
    fs::write(path, bytes).unwrap_or_else(|error| panic!("write {}: {error}", path.display()));
}

fn fixture_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/compatibility/546f419")
}

fn fixture_readme() -> &'static str {
    "# 546f419 stage-3 compatibility fixture\n\
     \n\
     This directory is immutable cross-version evidence. Do not regenerate or\n\
     edit these files with the current checkout.\n\
     \n\
     The payloads were created by the release `yy` binary from\n\
     `546f419f9158e156f9a561822f3cb3f307222584`. The producer archive SHA-256 and\n\
     packaged binary SHA-256 are recorded in `manifest.json`. The producer tree\n\
     equals `ba7c7f2c3bbebd16c01891bc2361d9d36217d503`.\n\
     \n\
     The fixture contains stage-3 sparse bases and segments, a pending handle, a\n\
     request dropped before commit, a successful commit whose response was lost,\n\
     and a service metadata database copied together with its non-empty WAL.\n\
     `expected.json` records exact handles, CommitIds, request digests, segment\n\
     ranges, payload hashes and the service WAL identity.\n\
     \n\
     `manifest.json` hashes every other file with SHA-256. `.gitattributes` keeps\n\
     the fixture bytes exact on every checkout platform.\n"
}
