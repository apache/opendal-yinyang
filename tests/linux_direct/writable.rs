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

//! Accepted writable Direct admission and native mutation scenarios.

use crate::harness::{
    Fixture, Mounted, RequestCounter, assert_errno, effective_capabilities, local_paths, path_text,
    payload, run_timeout,
};
use crate::s3_gate::{CommitAction, S3Gate};
use nix::fcntl::{FcntlArg, OFlag, fcntl};
use nix::unistd::{chown, getgid, getuid};
use rustix::fs::{RenameFlags, renameat_with};
use std::collections::BTreeSet;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::process::Command;
use std::thread;
use std::time::Duration;

const REQUIRED_WRITABLE_CAPABILITIES: [&str; 8] = [
    "read",
    "list",
    "write",
    "append",
    "truncate",
    "pinned-reads",
    "remote-fsync",
    "recoverable-staging",
];

const FORBIDDEN_DIRECT_CAPABILITIES: [&str; 3] =
    ["stable-identity", "atomic-rename", "retained-unlink-read"];

fn assert_writable_capabilities(capabilities: &BTreeSet<String>) {
    for required in REQUIRED_WRITABLE_CAPABILITIES {
        assert!(
            capabilities.contains(required),
            "writable Direct capabilities omitted {required:?}: {capabilities:?}"
        );
    }
    for forbidden in FORBIDDEN_DIRECT_CAPABILITIES {
        assert!(
            !capabilities.contains(forbidden),
            "writable Direct capabilities unexpectedly claimed {forbidden:?}: {capabilities:?}"
        );
    }
}

#[test]
#[ignore = "requires isolated S3; run with cargo x test-linux-direct"]
fn direct_write_admission_requires_staging_and_publishes_limited_capabilities() {
    let fixture = Fixture::new();
    fixture.write_raw("seed.txt", b"seed");
    let initial_inventory = fixture.inventory();
    let staging = fixture.path("direct-write-state");
    let config = fixture.writable_config("direct-write", "fuse", &staging);
    let local_before = local_paths(fixture.temp.path());

    let output = fixture.yy_ok(&["--volume", path_text(&config), "capabilities"]);
    let capabilities = effective_capabilities(&output.stdout);
    assert_writable_capabilities(&capabilities);

    assert!(
        !staging.exists(),
        "writable Direct capabilities created the configured staging path"
    );
    assert_eq!(
        local_paths(fixture.temp.path()),
        local_before,
        "writable Direct capabilities created local state"
    );
    assert_eq!(
        fixture.inventory(),
        initial_inventory,
        "writable Direct capabilities mutated raw storage"
    );
    assert!(
        fixture
            .inventory()
            .keys()
            .all(|key| !key.starts_with(".yinyang/")),
        "writable Direct admission created Managed metadata"
    );

    let missing_staging = fixture.config("direct-write-missing-staging", "fuse", Some(false), None);
    let requests = RequestCounter::start();
    let mut command =
        fixture.yy_command(&["--volume", path_text(&missing_staging), "capabilities"]);
    command.env("YINYANG_S3_ENDPOINT", requests.endpoint());
    let rejected = run_timeout(command, Duration::from_secs(30));
    assert!(
        !rejected.status.success(),
        "writable Direct accepted a configuration without staging"
    );
    assert!(
        rejected.text().to_ascii_lowercase().contains("staging"),
        "writable Direct staging rejection did not identify the field:\n{}",
        rejected.text()
    );
    requests.assert_no_requests("writable Direct missing staging");
    assert!(
        !staging.exists(),
        "missing-staging admission created another Direct state path"
    );
    assert_eq!(
        fixture.inventory(),
        initial_inventory,
        "missing-staging admission mutated raw storage"
    );
}

#[test]
#[ignore = "requires isolated S3; run with cargo x test-linux-direct"]
fn direct_write_doctor_rejects_invalid_staging_without_state_or_storage_effects() {
    let fixture = Fixture::new();
    fixture.write_raw("doctor.txt", b"doctor");
    let inventory = fixture.inventory();
    let staging = fixture.path("direct-write-doctor-state");
    fs::write(&staging, b"not a directory").expect("write invalid Direct staging file");
    let config = fixture.writable_config("direct-write-doctor", "fuse", &staging);

    let doctor = fixture.yy(&["--volume", path_text(&config), "doctor", "--json"]);
    let value: serde_json::Value = serde_json::from_slice(&doctor.stdout).unwrap_or_else(|error| {
        panic!(
            "Direct doctor returned invalid JSON: {error}\n{}",
            doctor.text()
        )
    });
    assert_ne!(
        value["status"], "pass",
        "writable Direct doctor reported ready with a non-directory staging path: {value:#}"
    );
    let local_state = value["checks"]
        .as_array()
        .expect("doctor checks")
        .iter()
        .find(|check| check["name"] == "local-state")
        .expect("doctor local-state check");
    assert_ne!(local_state["status"], "pass", "{value:#}");
    assert!(
        local_state["detail"].as_str().is_some_and(
            |detail| detail.contains("staging") || detail.contains(path_text(&staging))
        ),
        "doctor did not identify the invalid Direct staging path: {value:#}"
    );
    assert_eq!(
        fs::read(&staging).expect("read invalid Direct staging file"),
        b"not a directory"
    );
    assert_eq!(
        fixture.inventory(),
        inventory,
        "invalid Direct staging doctor mutated raw storage"
    );
    assert!(
        fixture
            .inventory()
            .keys()
            .all(|key| !key.starts_with(".yinyang/")),
        "invalid Direct staging doctor created Managed metadata"
    );
}

#[test]
#[ignore = "requires /dev/fuse and isolated S3; run with cargo x test-linux-direct"]
fn direct_write_mount_publishes_whole_objects_and_persists_remount() {
    let fixture = Fixture::new();
    let original = payload(16 * 1024, 0x31);
    fixture.write_raw("basic.bin", &original);
    let staging = fixture.path("direct-write-basic-state");
    let config = fixture.writable_config("direct-write-basic", "fuse", &staging);
    let mountpoint = fixture.path("mnt-direct-write-basic");
    fs::create_dir(&mountpoint).expect("create writable Direct mountpoint");
    let mounted = fixture.spawn_writable_mount(&config, &mountpoint);
    let path = mounted.mountpoint().join("basic.bin");

    let patch = payload(1024, 0x47);
    let tail = payload(17, 0x63);
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .expect("open writable Direct file");
    file.seek(SeekFrom::Start(4096))
        .expect("seek writable Direct file");
    file.write_all(&patch).expect("write Direct file range");
    file.set_len(8192 + tail.len() as u64)
        .expect("extend Direct file");
    file.seek(SeekFrom::Start(8192))
        .expect("seek extended Direct file");
    file.write_all(&tail).expect("write Direct file tail");
    file.seek(SeekFrom::Start(0))
        .expect("rewind writable Direct file");
    let mut staged = Vec::new();
    file.read_to_end(&mut staged)
        .expect("read staged Direct file");
    assert_eq!(
        staged,
        {
            let mut expected = original.clone();
            expected[4096..4096 + patch.len()].copy_from_slice(&patch);
            expected.resize(8192 + tail.len(), 0);
            expected[8192..].copy_from_slice(&tail);
            expected
        },
        "read must observe the complete staged object"
    );
    file.sync_all()
        .expect("publish writable Direct whole object");
    drop(file);

    assert_eq!(
        fixture.raw_get("basic.bin").as_deref(),
        Some(staged.as_slice()),
        "successful Direct fsync must publish the exact staged bytes"
    );
    assert_eq!(
        fixture.raw_object("basic.bin").map(|object| object.bytes),
        Some(staged.clone())
    );
    assert_eq!(
        fixture.raw_get("basic.bin").unwrap().len(),
        8192 + tail.len()
    );
    mounted.unmount();

    let remounted = fixture.spawn_writable_mount(&config, &mountpoint);
    assert_eq!(
        fs::read(remounted.mountpoint().join("basic.bin"))
            .expect("read remounted writable Direct file"),
        staged,
        "remount must recover the accepted whole-object bytes"
    );
    remounted.unmount();
    assert!(
        fixture
            .inventory()
            .keys()
            .all(|key| !key.starts_with(".yinyang/")),
        "writable Direct created Managed metadata"
    );
}

#[test]
#[ignore = "requires /dev/fuse and isolated S3; run with cargo x test-linux-direct"]
fn direct_write_read_only_flush_does_not_publish_shared_dirty_bytes() {
    let fixture = Fixture::new();
    let original = payload(8192, 0x19);
    let dirty = payload(8192, 0x71);
    fixture.write_raw("flush.txt", &original);
    let staging = fixture.path("direct-write-flush-state");
    let config = fixture.writable_config("direct-write-flush", "fuse", &staging);
    let mountpoint = fixture.path("mnt-direct-write-flush");
    fs::create_dir(&mountpoint).expect("create writable Direct flush mountpoint");
    let mounted = fixture.spawn_writable_mount(&config, &mountpoint);
    let path = mounted.mountpoint().join("flush.txt");

    let mut writer = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .expect("open writable Direct writer");
    writer
        .seek(SeekFrom::Start(0))
        .expect("rewind dirty writer");
    writer.write_all(&dirty).expect("stage Direct dirty bytes");
    let mut reader = File::open(&path).expect("open read-only Direct descriptor");
    let mut staged = Vec::new();
    reader
        .read_to_end(&mut staged)
        .expect("read shared staged Direct bytes");
    assert_eq!(staged, dirty);
    drop(reader);

    assert_eq!(
        fixture.raw_get("flush.txt").as_deref(),
        Some(original.as_slice()),
        "closing a read-only descriptor published another writer's dirty bytes"
    );
    writer
        .sync_all()
        .expect("publish dirty bytes through writable descriptor");
    drop(writer);
    assert_eq!(
        fixture.raw_get("flush.txt").as_deref(),
        Some(dirty.as_slice())
    );
    mounted.unmount();
}

#[test]
#[ignore = "requires /dev/fuse and isolated S3; run with cargo x test-linux-direct"]
fn direct_write_append_flags_and_shared_fd_visibility() {
    let fixture = Fixture::new();
    fixture.write_raw("append.txt", b"0123456789");
    let staging = fixture.path("direct-write-append-state");
    let config = fixture.writable_config("direct-write-append", "fuse", &staging);
    let mountpoint = fixture.path("mnt-direct-write-append");
    fs::create_dir(&mountpoint).expect("create writable Direct append mountpoint");
    let mounted = fixture.spawn_writable_mount(&config, &mountpoint);
    let path = mounted.mountpoint().join("append.txt");

    let mut file = OpenOptions::new()
        .read(true)
        .append(true)
        .open(&path)
        .expect("open Direct file with O_APPEND");
    let flags = fcntl(&file, FcntlArg::F_GETFL).expect("read Direct open flags");
    let flags = OFlag::from_bits_truncate(flags);
    assert!(flags.contains(OFlag::O_APPEND));
    fcntl(&file, FcntlArg::F_SETFL(flags.difference(OFlag::O_APPEND)))
        .expect("clear Direct O_APPEND");
    file.seek(SeekFrom::Start(0))
        .expect("seek after clearing Direct O_APPEND");
    file.write_all(b"AB").expect("write Direct file offset");
    file.sync_all()
        .expect("publish Direct overwrite without O_APPEND");
    assert_eq!(
        fixture.raw_get("append.txt").as_deref(),
        Some(b"AB23456789".as_slice())
    );

    let mut reader = File::open(&path).expect("open shared Direct reader");
    fcntl(&file, FcntlArg::F_SETFL(flags)).expect("restore Direct O_APPEND");
    file.seek(SeekFrom::Start(0))
        .expect("O_APPEND must ignore this seek");
    file.write_all(b"CD").expect("append Direct bytes");
    reader
        .seek(SeekFrom::Start(0))
        .expect("rewind shared Direct reader");
    let mut shared = String::new();
    reader
        .read_to_string(&mut shared)
        .expect("read shared dirty bytes");
    assert_eq!(shared, "AB23456789CD");
    drop(reader);
    assert_eq!(
        fixture.raw_get("append.txt").as_deref(),
        Some(b"AB23456789".as_slice()),
        "shared reader close published appended bytes before the writable fsync"
    );
    file.sync_all().expect("publish Direct appended bytes");
    drop(file);
    assert_eq!(
        fixture.raw_get("append.txt").as_deref(),
        Some(b"AB23456789CD".as_slice())
    );
    mounted.unmount();
}

#[test]
#[ignore = "requires /dev/fuse and isolated S3; run with cargo x test-linux-direct"]
fn direct_write_create_is_conditional_and_never_clobbers_external_appearance() {
    let fixture = Fixture::new();
    let staging = fixture.path("direct-write-create-state");
    let config = fixture.writable_config("direct-write-create", "fuse", &staging);
    let mountpoint = fixture.path("mnt-direct-write-create");
    fs::create_dir(&mountpoint).expect("create writable Direct create mountpoint");
    let mounted = fixture.spawn_writable_mount(&config, &mountpoint);

    let created_bytes = payload(12 * 1024 + 5, 0x27);
    let mut created =
        File::create(mounted.mountpoint().join("created.txt")).expect("create Direct file");
    created
        .write_all(&created_bytes)
        .expect("write new Direct file");
    created
        .sync_all()
        .expect("publish conditional Direct creation");
    drop(created);
    assert_eq!(
        fixture.raw_get("created.txt").as_deref(),
        Some(created_bytes.as_slice())
    );

    let local = payload(20 * 1024 + 17, 0x51);
    let external = payload(20 * 1024 + 3, 0x73);
    let mut raced =
        File::create(mounted.mountpoint().join("raced.txt")).expect("create raced Direct file");
    raced
        .write_all(&local)
        .expect("write raced Direct local bytes");
    fixture.write_raw("raced.txt", &external);
    let error = raced
        .sync_all()
        .expect_err("external object appearance must reject Direct creation");
    assert_errno(
        &error,
        &[libc::EIO, libc::ESTALE],
        "conditional Direct creation",
    );
    assert_eq!(
        fixture.raw_get("raced.txt").as_deref(),
        Some(external.as_slice()),
        "conditional Direct creation overwrote an externally created object"
    );
    raced.seek(SeekFrom::Start(0)).expect("rewind raced file");
    let mut retained = Vec::new();
    raced
        .read_to_end(&mut retained)
        .expect("read raced Direct local bytes");
    assert_eq!(retained, local);
    drop(raced);
    mounted.unmount();
}

#[test]
#[ignore = "requires /dev/fuse and isolated S3; run with cargo x test-linux-direct"]
fn direct_write_unlink_is_conditional_and_refreshes_before_retry() {
    let fixture = Fixture::new();
    let original = payload(12 * 1024 + 3, 0x23);
    let external = payload(12 * 1024 + 7, 0x49);
    fixture.write_raw("unlink.txt", &original);
    let staging = fixture.path("direct-write-unlink-state");
    let config = fixture.writable_config("direct-write-unlink", "fuse", &staging);
    let mountpoint = fixture.path("mnt-direct-write-unlink");
    fs::create_dir(&mountpoint).expect("create writable Direct unlink mountpoint");
    let mounted = fixture.spawn_writable_mount(&config, &mountpoint);
    let path = mounted.mountpoint().join("unlink.txt");

    fixture.write_raw("unlink.txt", &external);
    let error =
        fs::remove_file(&path).expect_err("Direct unlink must reject an externally replaced ETag");
    assert_errno(
        &error,
        &[libc::ESTALE, libc::EIO],
        "conditional Direct unlink",
    );
    assert_eq!(
        fixture.raw_get("unlink.txt").as_deref(),
        Some(external.as_slice()),
        "conditional Direct unlink removed the external replacement"
    );

    mounted.refresh_and_wait(Duration::from_secs(30));
    fs::remove_file(&path).expect("retry Direct unlink after refresh");
    assert_eq!(
        fixture.raw_get("unlink.txt"),
        None,
        "refreshed conditional unlink did not remove the accepted object"
    );
    mounted.unmount();
}

fn set_xattr(path: &Path) -> std::io::Result<()> {
    let mut command = Command::new("python3");
    command.args([
        "-c",
        "import os, sys; os.setxattr(sys.argv[1], b'user.yinyang', b'unsupported')",
    ]);
    command.arg(path);
    let output = run_timeout(command, Duration::from_secs(30));
    if output.status.success() {
        Ok(())
    } else {
        Err(std::io::Error::other(output.text()))
    }
}

fn renameat2(
    directory: &File,
    source: &str,
    destination: &str,
    flags: RenameFlags,
) -> std::io::Result<()> {
    renameat_with(directory, source, directory, destination, flags)
        .map_err(|error| std::io::Error::from_raw_os_error(error.raw_os_error()))
}

#[test]
#[ignore = "requires /dev/fuse and isolated S3; run with cargo x test-linux-direct"]
fn direct_write_rejects_unsupported_posix_operations_without_storage_effect() {
    let fixture = Fixture::new();
    fixture.write_raw("base.txt", b"base");
    fixture.write_raw("other.txt", b"other");
    fixture.write_raw("directory/child.txt", b"child");
    let inventory = fixture.inventory();
    let staging = fixture.path("direct-write-reject-state");
    let config = fixture.writable_config("direct-write-reject", "fuse", &staging);
    let mountpoint = fixture.path("mnt-direct-write-reject");
    fs::create_dir(&mountpoint).expect("create writable Direct rejection mountpoint");
    let mounted = fixture.spawn_writable_mount(&config, &mountpoint);
    let root = mounted.mountpoint();
    let base = root.join("base.txt");

    let error = fs::set_permissions(&base, fs::Permissions::from_mode(0o600))
        .expect_err("Direct must reject POSIX mode changes");
    assert_errno(
        &error,
        &[libc::EINVAL, libc::EOPNOTSUPP, libc::ENOSYS],
        "Direct chmod",
    );

    let error = chown(&base, Some(getuid()), Some(getgid()))
        .expect_err("Direct must reject ownership changes");
    assert!(
        [libc::EINVAL, libc::EOPNOTSUPP, libc::ENOSYS].contains(&(error as i32)),
        "Direct chown returned unexpected errno {error}"
    );

    let error = set_xattr(&base).expect_err("Direct must reject extended attributes");
    let diagnostic = error.to_string().to_ascii_lowercase();
    assert!(
        diagnostic.contains("operation not supported")
            || diagnostic.contains("errno 95")
            || diagnostic.contains("not implemented"),
        "Direct xattr rejection did not identify unsupported semantics: {diagnostic}"
    );

    let error = std::os::unix::fs::symlink("base.txt", root.join("symlink"))
        .expect_err("Direct must reject symlinks");
    assert_errno(
        &error,
        &[libc::EOPNOTSUPP, libc::ENOSYS, libc::EPERM],
        "Direct symlink",
    );
    let error =
        fs::hard_link(&base, root.join("hardlink")).expect_err("Direct must reject hard links");
    assert_errno(
        &error,
        &[libc::EOPNOTSUPP, libc::ENOSYS, libc::EPERM],
        "Direct hard link",
    );

    let error = fs::rename(root.join("directory"), root.join("renamed-directory"))
        .expect_err("Direct must reject directory rename");
    assert_errno(
        &error,
        &[libc::EOPNOTSUPP, libc::ENOSYS, libc::EINVAL],
        "Direct directory rename",
    );
    let root_directory = File::open(root).expect("open writable Direct root");
    let error = renameat2(
        &root_directory,
        "base.txt",
        "other.txt",
        RenameFlags::EXCHANGE,
    )
    .expect_err("Direct must reject RENAME_EXCHANGE");
    assert_errno(
        &error,
        &[libc::EOPNOTSUPP, libc::ENOSYS, libc::EINVAL],
        "Direct RENAME_EXCHANGE",
    );
    let error = renameat2(
        &root_directory,
        "base.txt",
        "whiteout",
        RenameFlags::WHITEOUT,
    )
    .expect_err("Direct must reject RENAME_WHITEOUT");
    assert_errno(
        &error,
        &[libc::EOPNOTSUPP, libc::ENOSYS, libc::EINVAL],
        "Direct RENAME_WHITEOUT",
    );

    mounted.unmount();
    assert_eq!(
        fixture.inventory(),
        inventory,
        "unsupported Direct operations changed backend objects"
    );
}

#[test]
#[ignore = "requires /dev/fuse and isolated S3; run with cargo x test-linux-direct"]
fn direct_write_safe_save_retries_source_cleanup_without_atomic_rename_claim() {
    let fixture = Fixture::new();
    fixture.write_raw("safe.txt", b"old destination");
    let replacement = payload(96 * 1024 + 13, 0x5a);
    let staging = fixture.path("direct-write-safe-save-state");
    let config = fixture.writable_config("direct-write-safe-save", "fuse", &staging);
    let mountpoint = fixture.path("mnt-direct-write-safe-save");
    fs::create_dir(&mountpoint).expect("create writable Direct safe-save mountpoint");

    let suffix = format!("/{}/safe.txt.tmp", fixture.root());
    let gate = S3Gate::start_for_path(CommitAction::DropBeforeCommit, "DELETE", suffix);
    let mut command = fixture.mount_command(&config, &mountpoint);
    command.env("YINYANG_S3_ENDPOINT", gate.endpoint());
    let mounted = Mounted::spawn_writable(command, &mountpoint);
    let source = mounted.mountpoint().join("safe.txt.tmp");
    let destination = mounted.mountpoint().join("safe.txt");
    let mut temporary = File::create(&source).expect("create safe-save temporary file");
    temporary
        .write_all(&replacement)
        .expect("write safe-save temporary bytes");
    temporary
        .sync_all()
        .expect("publish safe-save temporary object");
    drop(temporary);
    assert_eq!(
        fixture.raw_get("safe.txt.tmp").as_deref(),
        Some(replacement.as_slice())
    );

    let source_for_thread = source.clone();
    let destination_for_thread = destination.clone();
    let rename = thread::spawn(move || fs::rename(source_for_thread, destination_for_thread));
    let intercepted = gate.wait_intercepted();
    assert_eq!(intercepted.method, "DELETE");
    assert!(
        intercepted.target.contains("/safe.txt.tmp"),
        "safe-save cleanup did not target the temporary source: {}",
        intercepted.target
    );
    assert!(
        intercepted.header("if-match").is_some(),
        "safe-save source cleanup omitted its conditional ETag: {:?}",
        intercepted.headers
    );
    fixture.wait_raw_bytes("safe.txt", &replacement, Duration::from_secs(30));
    assert_eq!(
        fixture.raw_get("safe.txt.tmp").as_deref(),
        Some(replacement.as_slice()),
        "held source cleanup must leave the published temporary object available for retry"
    );

    gate.release();
    gate.wait_original_dropped();
    let renamed = rename.join().expect("safe-save rename thread panicked");
    drop(gate);
    assert_eq!(
        fixture.raw_get("safe.txt").as_deref(),
        Some(replacement.as_slice())
    );
    match renamed {
        Ok(()) => assert_eq!(
            fixture.raw_get("safe.txt.tmp"),
            None,
            "safe-save reported success before source cleanup completed"
        ),
        Err(error) => {
            assert_errno(
                &error,
                &[libc::EIO, libc::ESTALE, libc::EBUSY],
                "safe-save source cleanup failure",
            );
            assert_eq!(
                fixture.raw_get("safe.txt.tmp").as_deref(),
                Some(replacement.as_slice()),
                "safe-save failure must retain the source needed for cleanup retry"
            );
            fs::rename(&source, &destination).expect("retry safe-save source cleanup");
        }
    }
    assert_eq!(
        fixture.raw_get("safe.txt").as_deref(),
        Some(replacement.as_slice())
    );
    assert_eq!(
        fixture.raw_get("safe.txt.tmp"),
        None,
        "safe-save retry did not remove the conditional source object"
    );
    mounted.unmount();
}

#[test]
#[ignore = "requires /dev/fuse and isolated S3; run with cargo x test-linux-direct"]
fn direct_write_external_replacement_conflict_retains_local_bytes_after_restart() {
    let fixture = Fixture::new();
    let original = payload(48 * 1024 + 3, 0x11);
    let local = payload(48 * 1024 + 5, 0x29);
    let external = payload(48 * 1024 + 7, 0x43);
    fixture.write_raw("conflict.txt", &original);
    let staging = fixture.path("direct-write-conflict-state");
    let config = fixture.writable_config("direct-write-conflict", "fuse", &staging);
    let mountpoint = fixture.path("mnt-direct-write-conflict");
    fs::create_dir(&mountpoint).expect("create writable Direct conflict mountpoint");
    let mounted = fixture.spawn_writable_mount(&config, &mountpoint);
    let path = mounted.mountpoint().join("conflict.txt");

    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .expect("open conflict Direct writer");
    file.write_all(&local).expect("stage local conflict bytes");
    file.set_len(local.len() as u64)
        .expect("set local conflict length");
    fixture.write_raw("conflict.txt", &external);
    let refresh = mounted.refresh_and_wait(Duration::from_secs(30));
    assert!(
        refresh.contains("inode=") && refresh.contains("entries="),
        "writable Direct refresh omitted its invalidation counts: {refresh}"
    );
    let error = file
        .sync_all()
        .expect_err("external replacement must make Direct publication conflict");
    assert_errno(
        &error,
        &[libc::EIO, libc::ESTALE],
        "Direct external replacement conflict",
    );
    assert_eq!(
        fixture.raw_get("conflict.txt").as_deref(),
        Some(external.as_slice()),
        "conflicting Direct publication overwrote the external object"
    );
    assert_eq!(
        file.metadata().expect("stat conflicted Direct fd").len(),
        local.len() as u64
    );
    file.seek(SeekFrom::Start(0))
        .expect("rewind conflicted Direct fd");
    let mut retained = Vec::new();
    file.read_to_end(&mut retained)
        .expect("read retained conflict bytes");
    assert_eq!(retained, local);
    drop(file);
    mounted.unmount();

    let remounted = fixture.spawn_writable_mount(&config, &mountpoint);
    assert_eq!(
        fs::read(remounted.mountpoint().join("conflict.txt"))
            .expect("read recovered Direct conflict bytes"),
        local,
        "restart discarded the locally retained conflict bytes"
    );
    assert_eq!(
        fixture.raw_get("conflict.txt").as_deref(),
        Some(external.as_slice()),
        "restart automatically retried and overwrote the external conflict"
    );
    remounted.unmount();
}

#[test]
#[ignore = "requires /dev/fuse and isolated S3; run with cargo x test-linux-direct"]
fn direct_write_fresh_open_after_external_replace_uses_new_snapshot_identity() {
    let fixture = Fixture::new();
    let old = payload(64 * 1024 + 7, 0x13);
    let new = payload(16 * 1024 + 5, 0x6b);
    fixture.write_raw("replace.txt", &old);
    let staging = fixture.path("direct-write-replace-state");
    let config = fixture.writable_config("direct-write-replace", "fuse", &staging);
    let mountpoint = fixture.path("mnt-direct-write-replace");
    fs::create_dir(&mountpoint).expect("create writable Direct replace mountpoint");
    let mounted = fixture.spawn_writable_mount(&config, &mountpoint);
    let path = mounted.mountpoint().join("replace.txt");

    let mut old_fd = File::open(&path).expect("open old Direct version");
    let mut old_bytes = Vec::new();
    old_fd
        .read_to_end(&mut old_bytes)
        .expect("read old Direct version");
    assert_eq!(old_bytes, old);
    fixture.write_raw("replace.txt", &new);
    mounted.refresh_and_wait(Duration::from_secs(30));

    let mut fresh = File::open(&path).expect("fresh Direct open after replacement");
    assert_eq!(
        fresh.metadata().expect("stat fresh Direct version").len(),
        new.len() as u64,
        "fresh Direct open reused cached old attributes"
    );
    let mut fresh_bytes = Vec::new();
    fresh
        .read_to_end(&mut fresh_bytes)
        .expect("read fresh Direct version");
    assert_eq!(
        fresh_bytes, new,
        "fresh Direct open reused cached old bytes"
    );

    old_fd
        .seek(SeekFrom::Start(0))
        .expect("rewind retained Direct fd");
    match old_fd.read_to_end(&mut old_bytes) {
        Ok(_) => assert_eq!(
            old_bytes, old,
            "retained Direct fd was rebased to replacement bytes"
        ),
        Err(error) => assert_errno(
            &error,
            &[libc::EIO, libc::ESTALE],
            "retained Direct fd after replacement",
        ),
    }
    drop(fresh);
    drop(old_fd);
    mounted.unmount();
}

#[test]
#[ignore = "requires /dev/fuse and isolated S3; run with cargo x test-linux-direct"]
fn direct_write_unflushed_create_and_append_survive_crash() {
    let fixture = Fixture::new();
    let staging = fixture.path("direct-write-crash-state");
    let config = fixture.writable_config("direct-write-crash", "fuse", &staging);
    let mountpoint = fixture.path("mnt-direct-write-crash");
    fs::create_dir(&mountpoint).expect("create writable Direct crash mountpoint");
    let first_mount = fixture.spawn_writable_mount(&config, &mountpoint);

    let created_bytes = b"fresh";
    let mut created = File::create(first_mount.mountpoint().join("created-crash.txt"))
        .expect("create unflushed Direct file");
    created
        .write_all(created_bytes)
        .expect("write unflushed Direct create bytes");
    assert_eq!(
        created
            .metadata()
            .expect("stat unflushed Direct file")
            .len(),
        created_bytes.len() as u64
    );
    first_mount.crash();
    drop(created);
    assert_eq!(
        fixture.raw_get("created-crash.txt"),
        None,
        "unflushed Direct create reached remote storage before crash"
    );

    let second_mount = fixture.spawn_writable_mount(&config, &mountpoint);
    assert_eq!(
        fs::read(second_mount.mountpoint().join("created-crash.txt"))
            .expect("read recovered unflushed Direct create"),
        created_bytes
    );
    let base = payload(16 * 1024 + 1, 0x2d);
    fixture.write_raw("append-crash.txt", &base);
    let mounted_again = second_mount;
    mounted_again.refresh_and_wait(Duration::from_secs(30));
    let mut append = OpenOptions::new()
        .read(true)
        .write(true)
        .open(mounted_again.mountpoint().join("append-crash.txt"))
        .expect("open existing Direct append file");
    append
        .seek(SeekFrom::End(0))
        .expect("seek Direct append EOF");
    let extension = b"-unflushed-extension";
    append
        .write_all(extension)
        .expect("append unflushed Direct bytes");
    assert_eq!(
        append
            .metadata()
            .expect("stat unflushed Direct append")
            .len(),
        base.len() as u64 + extension.len() as u64
    );
    mounted_again.crash();
    drop(append);
    assert_eq!(
        fixture.raw_get("append-crash.txt").as_deref(),
        Some(base.as_slice()),
        "unflushed Direct append reached remote storage before crash"
    );

    let final_mount = fixture.spawn_writable_mount(&config, &mountpoint);
    let mut expected = base;
    expected.extend_from_slice(extension);
    assert_eq!(
        fs::read(final_mount.mountpoint().join("append-crash.txt"))
            .expect("read recovered unflushed Direct append"),
        expected
    );
    final_mount.unmount();
}

fn exercise_uncertain_publication(action: CommitAction, delivered: bool) {
    let fixture = Fixture::new();
    let original = payload(32 * 1024 + 1, 0x15);
    let local = payload(32 * 1024 + 9, 0x37);
    let newer = payload(32 * 1024 + 3, 0x59);
    fixture.write_raw("uncertain.txt", &original);
    let staging = fixture.path("direct-write-uncertain-state");
    let config = fixture.writable_config("direct-write-uncertain", "fuse", &staging);
    let mountpoint = fixture.path("mnt-direct-write-uncertain");
    fs::create_dir(&mountpoint).expect("create writable Direct uncertainty mountpoint");

    let suffix = format!("/{}/uncertain.txt", fixture.root());
    let gate = S3Gate::start_for_path(action, "PUT", suffix);
    let mut command = fixture.mount_command(&config, &mountpoint);
    command.env("YINYANG_S3_ENDPOINT", gate.endpoint());
    let mounted = Mounted::spawn_writable(command, &mountpoint);
    let path = mounted.mountpoint().join("uncertain.txt");
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .expect("open uncertain Direct writer");
    file.write_all(&local)
        .expect("stage uncertain Direct publication");
    file.set_len(local.len() as u64)
        .expect("set uncertain Direct length");

    let publication = thread::spawn(move || {
        let result = file.sync_all();
        (file, result)
    });
    let intercepted = gate.wait_intercepted();
    assert_eq!(intercepted.method, "PUT");
    assert!(
        intercepted.target.contains("/uncertain.txt"),
        "uncertainty gate intercepted the wrong object: {}",
        intercepted.target
    );
    assert!(
        intercepted.header("if-match").is_some(),
        "Direct replacement omitted its conditional ETag: {:?}",
        intercepted.headers
    );

    if delivered {
        fixture.wait_raw_bytes("uncertain.txt", &local, Duration::from_secs(30));
    } else {
        assert_eq!(
            fixture.raw_get("uncertain.txt").as_deref(),
            Some(original.as_slice()),
            "dropped Direct request reached upstream storage"
        );
    }
    mounted.crash();
    gate.release();
    if !delivered {
        gate.wait_original_dropped();
    }
    drop(gate);

    let (file, error) = publication
        .join()
        .expect("uncertain Direct publication panicked");
    let error = error.expect_err("uncertain Direct publication reported success");
    assert_errno(
        &error,
        &[libc::EIO, libc::ESTALE, libc::ENOTCONN],
        "uncertain Direct publication",
    );
    drop(file);

    let remounted = fixture.spawn_writable_mount(&config, &mountpoint);
    assert_eq!(
        fs::read(remounted.mountpoint().join("uncertain.txt"))
            .expect("read retained uncertain Direct bytes"),
        local,
        "restart discarded uncertain local bytes"
    );
    let expected_remote = if delivered {
        local.as_slice()
    } else {
        original.as_slice()
    };
    assert_eq!(
        fixture.raw_get("uncertain.txt").as_deref(),
        Some(expected_remote),
        "restart incorrectly changed the uncertain backend outcome"
    );

    let mut writer = OpenOptions::new()
        .read(true)
        .write(true)
        .open(remounted.mountpoint().join("uncertain.txt"))
        .expect("open newer Direct edit");
    writer
        .write_all(&newer)
        .expect("stage newer Direct local edit");
    writer
        .set_len(newer.len() as u64)
        .expect("set newer Direct length");
    remounted.crash();
    drop(writer);

    let final_mount = fixture.spawn_writable_mount(&config, &mountpoint);
    assert_eq!(
        fs::read(final_mount.mountpoint().join("uncertain.txt"))
            .expect("read newer local Direct bytes after restart"),
        newer,
        "restart lost a newer local edit made after the uncertain outcome"
    );
    assert_eq!(
        fixture.raw_get("uncertain.txt").as_deref(),
        Some(expected_remote),
        "restart auto-resolved the uncertain outcome with a newer local edit"
    );
    final_mount.unmount();
}

#[test]
#[ignore = "requires /dev/fuse and isolated S3; run with cargo x test-linux-direct"]
fn direct_write_dropped_request_remains_unknown() {
    exercise_uncertain_publication(CommitAction::DropBeforeCommit, false);
}

#[test]
#[ignore = "requires /dev/fuse and isolated S3; run with cargo x test-linux-direct"]
fn direct_write_lost_response_remains_unknown() {
    exercise_uncertain_publication(CommitAction::DropResponse, true);
}
