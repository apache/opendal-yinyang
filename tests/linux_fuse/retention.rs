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

//! Observation-only space and retention acceptance during dirty FUSE work.

use crate::harness::{
    Fixture, Frontend, Publication, RemoteObjectIdentity, ServiceGate, mount_fs_type,
};
use nix::fcntl::{FcntlArg, FdFlag, fcntl};
use serde_json::Value;
use std::fs::{self, File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::os::unix::fs::{FileExt, MetadataExt};
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::thread;
use std::time::Duration;
use yinyang::runtime::Runtime as YinYangRuntime;
use yinyang::space::{inspect_managed_space, plan_managed_retention};

const UNIT_BYTES: usize = 64 * 1024;
const RANGE_BYTES: usize = 4096;
const FILE_BYTES: usize = 2 * UNIT_BYTES + 2 * RANGE_BYTES + 19;

#[test]
#[ignore = "requires /dev/fuse and isolated S3; run with cargo x test-linux-fuse"]
fn fuse_space_and_retention_are_observational_during_dirty_work_both_publications() {
    for publication in [Publication::Object, Publication::Service] {
        space_and_retention_are_observational(&Fixture::new(publication));
    }
}

#[test]
#[ignore = "requires /dev/fuse and isolated S3; run with cargo x test-linux-fuse"]
fn fuse_space_cli_emits_versioned_documents_both_publications() {
    for publication in [Publication::Object, Publication::Service] {
        let fixture = Fixture::new(publication);
        let suffix = fixture.publication().as_str();
        let library =
            fixture.volume_config(&format!("lib-space-cli-{suffix}"), Frontend::Library, false);
        fixture.library_seed(&library, "cli.txt", b"cli");
        let config =
            fixture.volume_config(&format!("stage-space-cli-{suffix}"), Frontend::Fuse, false);
        let mountpoint = fixture.temp.path().join(format!("mnt-space-cli-{suffix}"));
        fs::create_dir(&mountpoint).expect("create space CLI mountpoint");
        let mut mounted = fixture.mount(&config, &mountpoint);

        let report = fixture.yy(&[
            "--volume",
            config.to_str().expect("UTF-8 space CLI config"),
            "space",
            "report",
            "--json",
        ]);
        report.success_or("space report CLI");
        let report: Value = serde_json::from_slice(&report.stdout).expect("space report CLI JSON");
        assert_eq!(report["schema"], "yinyang.managed-space.report.v1");
        assert_eq!(report["deletion_authorized"], false);
        assert!(report["reclaimable_bytes"].is_null());

        let plan = fixture.yy(&[
            "--volume",
            config.to_str().expect("UTF-8 space CLI config"),
            "space",
            "retention",
            "plan",
            "--keep-last",
            "1",
            "--json",
        ]);
        plan.success_or("space retention plan CLI");
        let plan: Value = serde_json::from_slice(&plan.stdout).expect("retention plan CLI JSON");
        assert_eq!(plan["schema"], "yinyang.managed-retention.plan.v1");
        assert_eq!(plan["deletion_authorized"], false);
        assert!(plan["reclaimable_bytes"].is_null());
        mounted.unmount();
    }
}

#[test]
#[ignore = "requires /dev/fuse and isolated S3; run with cargo x test-linux-fuse"]
fn fuse_space_and_retention_preserve_frozen_service_work() {
    let fixture = Fixture::new(Publication::Service);
    let library = fixture.volume_config("lib-space-frozen", Frontend::Library, false);
    let original = payload(FILE_BYTES, 0x37);
    fixture.library_seed(&library, "space.bin", &original);
    let staging = fixture.temp.path().join("stage-space-frozen");
    let gate = ServiceGate::new(staging.clone(), fixture.service_address());
    let config = fixture.volume_config_with_service(
        "stage-space-frozen",
        Frontend::Fuse,
        false,
        Some(gate.address()),
    );
    let mountpoint = fixture.temp.path().join("mnt-space-frozen");
    fs::create_dir(&mountpoint).expect("create frozen space mountpoint");
    let mut mounted = fixture.mount(&config, &mountpoint);

    let mut dirty = OpenOptions::new()
        .read(true)
        .write(true)
        .open(mounted.mountpoint().join("space.bin"))
        .expect("open frozen space descriptor");
    let flags = FdFlag::from_bits_truncate(
        fcntl(&dirty, FcntlArg::F_GETFD).expect("read frozen descriptor flags"),
    );
    assert!(
        flags.contains(FdFlag::FD_CLOEXEC),
        "the frozen dirty descriptor must be close-on-exec before observation commands"
    );
    let mut dirty_bytes = original.clone();
    let offset = UNIT_BYTES - RANGE_BYTES / 2;
    let update = payload(RANGE_BYTES, 0x71);
    dirty_bytes[offset..offset + update.len()].copy_from_slice(&update);
    dirty
        .seek(SeekFrom::Start(0))
        .expect("rewind frozen descriptor");
    dirty
        .write_all(&dirty_bytes)
        .expect("write frozen descriptor without fsync");

    let in_flight = dirty.try_clone().expect("duplicate frozen descriptor");
    let (sender, receiver) = mpsc::channel();
    let caller = thread::spawn(move || {
        let _ = sender.send(in_flight.sync_all());
    });
    gate.wait_ready(Duration::from_secs(30));
    let (commit, frozen) = fixture.wait_for_frozen(&config);
    println!("space observation reached saved frozen commit {commit:?}:\n{frozen}");
    assert_pending_staging(&staging, true);
    assert_eq!(
        fixture.remote_read(&library, "space.bin", original.len()),
        original,
        "a frozen publication must not expose dirty bytes while observation runs"
    );

    let config_before = fs::read(&config).expect("read frozen volume config before observation");
    let remote_before = fixture.remote_object_identities();
    let staging_before = tree_identity(&staging);
    let report = in_process_space_report(&fixture, &staging);
    assert_space_report(&report, &staging);
    assert_frozen_report(&report);
    assert_no_effects(
        &fixture,
        &library,
        &config,
        &staging,
        &original,
        &config_before,
        &remote_before,
        &staging_before,
        &mut mounted,
    );

    let plan = in_process_retention_plan(&fixture, &staging, 1);
    assert_retention_plan(&plan);
    assert_frozen_plan(&plan);
    assert_no_effects(
        &fixture,
        &library,
        &config,
        &staging,
        &original,
        &config_before,
        &remote_before,
        &staging_before,
        &mut mounted,
    );

    gate.release();
    receiver
        .recv_timeout(Duration::from_secs(30))
        .expect("frozen fsync did not return after gate release")
        .expect("frozen fsync failed after gate release");
    caller.join().expect("frozen fsync caller panicked");
    assert_eq!(
        fixture.remote_read(&library, "space.bin", dirty_bytes.len()),
        dirty_bytes,
        "observation changed or discarded the frozen request"
    );
    drop(dirty);
    mounted.unmount();
}

fn space_and_retention_are_observational(fixture: &Fixture) {
    let suffix = fixture.publication().as_str();
    let library = fixture.volume_config(
        &format!("lib-space-observation-{suffix}"),
        Frontend::Library,
        false,
    );
    let original = payload(FILE_BYTES, 0x49);
    fixture.library_seed(&library, "space.bin", &original);
    let config = fixture.volume_config(
        &format!("stage-space-observation-{suffix}"),
        Frontend::Fuse,
        false,
    );
    let staging = fixture
        .temp
        .path()
        .join(format!("stage-space-observation-{suffix}"));
    let mountpoint = fixture
        .temp
        .path()
        .join(format!("mnt-space-observation-{suffix}"));
    fs::create_dir(&mountpoint).expect("create space observation mountpoint");
    let mut mounted = fixture.mount(&config, &mountpoint);

    let mut dirty = OpenOptions::new()
        .read(true)
        .write(true)
        .open(mounted.mountpoint().join("space.bin"))
        .expect("open dirty FUSE descriptor");
    let flags = FdFlag::from_bits_truncate(
        fcntl(&dirty, FcntlArg::F_GETFD).expect("read descriptor close-on-exec flag"),
    );
    assert!(
        flags.contains(FdFlag::FD_CLOEXEC),
        "the dirty descriptor must be close-on-exec before spawning observation commands"
    );
    let mut dirty_bytes = original.clone();
    let write_offset = UNIT_BYTES - RANGE_BYTES / 2;
    let update = payload(RANGE_BYTES, 0x5d);
    dirty_bytes[write_offset..write_offset + update.len()].copy_from_slice(&update);
    dirty
        .seek(SeekFrom::Start(0))
        .expect("rewind dirty descriptor");
    dirty
        .write_all(&dirty_bytes)
        .expect("write a dirty full descriptor view without fsync");
    assert_pending_staging(&staging, false);
    assert_eq!(
        fixture.remote_read(&library, "space.bin", original.len()),
        original,
        "an un-fsynced descriptor write must not reach the authority"
    );

    let config_before = fs::read(&config).expect("read volume config before observation");
    let remote_before = fixture.remote_object_identities();
    let staging_before = tree_identity(&staging);
    assert_no_effects(
        fixture,
        &library,
        &config,
        &staging,
        &original,
        &config_before,
        &remote_before,
        &staging_before,
        &mut mounted,
    );

    let report = in_process_space_report(fixture, &staging);
    assert_pending_staging(&staging, false);
    assert_space_report(&report, &staging);
    assert_pending_report(&report);
    assert_no_effects(
        fixture,
        &library,
        &config,
        &staging,
        &original,
        &config_before,
        &remote_before,
        &staging_before,
        &mut mounted,
    );

    let plan = in_process_retention_plan(fixture, &staging, 1);
    assert_retention_plan(&plan);
    assert_pending_plan(&plan);
    assert_no_effects(
        fixture,
        &library,
        &config,
        &staging,
        &original,
        &config_before,
        &remote_before,
        &staging_before,
        &mut mounted,
    );

    let mut staged = vec![0_u8; dirty_bytes.len()];
    read_exact_at(&dirty, &mut staged, 0);
    assert_eq!(
        staged, dirty_bytes,
        "space observation changed the active dirty descriptor's staged bytes"
    );
    dirty
        .sync_all()
        .expect("the dirty descriptor still publishes after observation");
    assert_eq!(
        fixture.remote_read(&library, "space.bin", dirty_bytes.len()),
        dirty_bytes,
        "post-observation fsync did not publish the retained dirty bytes"
    );
    drop(dirty);
    mounted.unmount();
}

#[allow(clippy::too_many_arguments)]
fn assert_no_effects(
    fixture: &Fixture,
    library: &Path,
    config: &Path,
    staging: &Path,
    remote_original: &[u8],
    config_before: &[u8],
    remote_before: &[RemoteObjectIdentity],
    staging_before: &TreeIdentity,
    mounted: &mut crate::harness::Mounted,
) {
    assert_eq!(
        fs::read(config).expect("read volume config after observation"),
        config_before,
        "space observation rewrote the volume configuration"
    );
    assert_eq!(
        fixture.remote_object_identities(),
        remote_before,
        "space observation mutated remote keys, lengths or etags"
    );
    assert_eq!(
        fixture.remote_read(library, "space.bin", remote_original.len()),
        remote_original,
        "space observation published dirty content"
    );
    assert_eq!(
        tree_identity(staging),
        *staging_before,
        "space observation changed staging files or authoritative bytes"
    );
    assert_eq!(
        mount_fs_type(mounted.mountpoint()).as_deref(),
        Some("fuse.yinyang"),
        "space observation disturbed the live FUSE mount"
    );
}

fn assert_space_report(document: &Value, staging: &Path) {
    assert_eq!(
        document.get("schema").and_then(Value::as_str),
        Some("yinyang.managed-space.report.v1"),
        "unexpected space JSON schema: {document}"
    );
    assert_eq!(
        document["staging"]["path"].as_str(),
        staging.to_str(),
        "space report inspected a different staging directory: {document}"
    );
    assert_eq!(
        document["staging"]["binding"]["state"], "verified",
        "space report did not bind staging to the observed filesystem: {document}"
    );
    assert_eq!(
        document["deletion_authorized"],
        Value::Bool(false),
        "space report must not authorize deletion"
    );
    assert!(
        document["reclaimable_bytes"].is_null(),
        "space report must not estimate reclaimable bytes"
    );
    assert_eq!(
        document["measurement"]["non_atomic_inventory"],
        Value::Bool(true),
        "space report must disclose that recursive listing is not atomic"
    );
    assert_eq!(
        document["measurement"]["noncurrent_versions"],
        Value::String("unknown".to_owned()),
        "space report must not infer unknown backend versions"
    );
    assert_eq!(
        document["measurement"]["external_pins"],
        Value::String("unknown".to_owned()),
        "space report must not infer external readers or untracked pins"
    );
    assert_eq!(
        document["physical"]["noncurrent_versions"],
        Value::String("unknown".to_owned()),
        "physical accounting must preserve the noncurrent-version boundary"
    );
    assert!(
        document["physical"]["accounting"]
            .as_str()
            .is_some_and(|value| !value.is_empty()),
        "space report must state the physical accounting boundary"
    );
    assert_limit_mentions(document, "share", "space report");
    assert_dirty_handle_count(&document["staging"]["handles"], "space report");
}

fn assert_retention_plan(document: &Value) {
    assert_eq!(
        document.get("schema").and_then(Value::as_str),
        Some("yinyang.managed-retention.plan.v1"),
        "unexpected retention JSON schema: {document}"
    );
    assert_eq!(
        document["deletion_authorized"],
        Value::Bool(false),
        "retention planning must not authorize deletion"
    );
    assert!(
        document["reclaimable_bytes"].is_null(),
        "retention planning must not estimate reclaimable bytes"
    );
    assert_eq!(
        document["protection"]["staging"]["binding"]["state"], "verified",
        "retention plan did not bind staging to the observed filesystem: {document}"
    );
    assert_limit_mentions(document, "shared pack", "retention plan");
    assert_eq!(document["policy"]["mode"], "bounded");
    assert_eq!(document["policy"]["keep_last"], 1);
    assert_eq!(
        document["protection"]["external_readers"], "unknown",
        "retention plan must preserve the external-reader boundary"
    );
    assert_eq!(
        document["protection"]["untracked_pins"], "unknown",
        "retention plan must preserve the untracked-pin boundary"
    );
    assert_dirty_handle_count(&document["protection"]["staging"], "retention plan");
    let current = document["protection"]["current_revision"]
        .as_str()
        .expect("retention plan current revision");
    let current_decision = document["revisions"]
        .as_array()
        .expect("retention plan revisions")
        .iter()
        .find(|revision| revision["revision"] == current)
        .expect("current revision in retention decisions");
    assert_eq!(current_decision["selected"], true);
    let reasons = current_decision["reasons"]
        .as_array()
        .expect("current revision reasons");
    assert!(
        reasons.contains(&Value::String("current".to_owned()))
            && reasons.iter().any(|reason| {
                reason
                    .as_str()
                    .is_some_and(|reason| reason.starts_with("staging_"))
            }),
        "retention plan did not protect both the current revision and dirty staging base: {document}"
    );
}

fn assert_pending_report(document: &Value) {
    assert!(
        document["staging"]["handles"]["pending"]
            .as_u64()
            .is_some_and(|pending| pending >= 1),
        "space report did not count the pending handle: {document}"
    );
}

fn assert_pending_plan(document: &Value) {
    assert!(
        document["protection"]["staging"]["pending"]
            .as_u64()
            .is_some_and(|pending| pending >= 1),
        "retention plan did not protect the pending handle: {document}"
    );
}

fn assert_dirty_handle_count(staging: &Value, context: &str) {
    let count = ["pending", "frozen", "conflict", "unknown", "failed"]
        .into_iter()
        .filter_map(|state| staging[state].as_u64())
        .fold(0_u64, u64::saturating_add);
    assert!(
        count >= 1,
        "{context} did not observe an active dirty handle: {staging}"
    );
}

fn assert_limit_mentions(document: &Value, needle: &str, context: &str) {
    assert!(
        document["limits"]
            .as_array()
            .is_some_and(|limits| limits.iter().any(|limit| {
                limit
                    .as_str()
                    .is_some_and(|limit| limit.to_ascii_lowercase().contains(needle))
            })),
        "{context} did not disclose the shared physical-pack boundary: {document}"
    );
}

fn assert_frozen_report(document: &Value) {
    assert!(
        document["staging"]["handles"]["frozen"]
            .as_u64()
            .is_some_and(|frozen| frozen >= 1),
        "space report did not count the frozen handle: {document}"
    );
}

fn assert_frozen_plan(document: &Value) {
    assert!(
        document["protection"]["staging"]["frozen"]
            .as_u64()
            .is_some_and(|frozen| frozen >= 1),
        "retention plan did not protect the frozen handle: {document}"
    );
    let current = document["protection"]["current_revision"]
        .as_str()
        .expect("frozen retention current revision");
    let current_decision = document["revisions"]
        .as_array()
        .expect("frozen retention revisions")
        .iter()
        .find(|revision| revision["revision"] == current)
        .expect("frozen current revision decision");
    assert!(
        current_decision["reasons"]
            .as_array()
            .is_some_and(|reasons| reasons.contains(&Value::String("staging_frozen".to_owned()))),
        "retention plan did not name the frozen base: {document}"
    );
}

fn assert_pending_staging(staging: &Path, frozen: bool) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build staging inspection runtime");
    let status = runtime
        .block_on(YinYangRuntime::inspect(staging))
        .expect("inspect live staging without taking its writer lease");
    let handle = status
        .handles
        .iter()
        .find(|handle| handle.pending)
        .unwrap_or_else(|| panic!("staging has no pending handle: {status:#?}"));
    assert_eq!(
        handle.frozen, frozen,
        "unexpected frozen state: {status:#?}"
    );
}

fn in_process_space_report(fixture: &Fixture, staging: &Path) -> Value {
    let authority = fixture.in_process_authority();
    let operator = fixture.operator();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build space observation runtime");
    let report = runtime
        .block_on(inspect_managed_space(
            authority.as_ref(),
            &operator,
            staging,
        ))
        .expect("inspect managed space in process");
    serde_json::to_value(report).expect("serialize managed space report")
}

fn in_process_retention_plan(fixture: &Fixture, staging: &Path, keep_last: u64) -> Value {
    let authority = fixture.in_process_authority();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build retention planning runtime");
    let plan = runtime
        .block_on(plan_managed_retention(
            authority.as_ref(),
            staging,
            Some(keep_last),
            &[],
        ))
        .expect("plan managed retention in process");
    serde_json::to_value(plan).expect("serialize retention plan")
}

#[derive(Debug, Eq, PartialEq)]
struct TreeIdentity {
    entries: Vec<TreeEntry>,
}

#[derive(Debug, Eq, PartialEq)]
enum TreeEntry {
    Directory {
        path: PathBuf,
        mode: u32,
    },
    File {
        path: PathBuf,
        mode: u32,
        length: u64,
        digest: String,
    },
}

fn tree_identity(root: &Path) -> TreeIdentity {
    fn walk(root: &Path, path: &Path, entries: &mut Vec<TreeEntry>) {
        let metadata = fs::symlink_metadata(path)
            .unwrap_or_else(|error| panic!("stat {}: {error}", path.display()));
        let relative = path
            .strip_prefix(root)
            .expect("tree entry below root")
            .to_path_buf();
        let mode = metadata.mode() & 0o7777;
        if metadata.is_dir() {
            entries.push(TreeEntry::Directory {
                path: relative,
                mode,
            });
            let mut children = fs::read_dir(path)
                .unwrap_or_else(|error| panic!("read {}: {error}", path.display()))
                .map(|entry| entry.expect("read directory entry").path())
                .collect::<Vec<_>>();
            children.sort();
            for child in children {
                walk(root, &child, entries);
            }
        } else if metadata.is_file() {
            if path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.ends_with("-shm"))
            {
                // SQLite may update its derived shared-memory file during a
                // read-only inspection. The database and WAL remain the
                // authoritative byte comparisons.
                return;
            }
            let bytes =
                fs::read(path).unwrap_or_else(|error| panic!("read {}: {error}", path.display()));
            entries.push(TreeEntry::File {
                path: relative,
                mode,
                length: metadata.len(),
                digest: blake3::hash(&bytes).to_hex().to_string(),
            });
        }
    }

    let mut entries = Vec::new();
    if root.exists() {
        walk(root, root, &mut entries);
    }
    entries.sort_by(|left, right| entry_path(left).cmp(entry_path(right)));
    TreeIdentity { entries }
}

fn entry_path(entry: &TreeEntry) -> &Path {
    match entry {
        TreeEntry::Directory { path, .. } | TreeEntry::File { path, .. } => path,
    }
}

fn read_exact_at(file: &File, mut buffer: &mut [u8], mut offset: u64) {
    while !buffer.is_empty() {
        let count = file
            .read_at(buffer, offset)
            .expect("pread dirty descriptor");
        assert_ne!(count, 0, "unexpected EOF at offset {offset}");
        offset += count as u64;
        buffer = &mut buffer[count..];
    }
}

fn payload(length: usize, seed: u8) -> Vec<u8> {
    let mut value = Vec::with_capacity(length);
    let mut state = u64::from(seed) | 1;
    while value.len() < length {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        value.extend_from_slice(&state.to_le_bytes());
    }
    value.truncate(length);
    value
}
