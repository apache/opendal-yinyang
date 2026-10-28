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
use std::collections::BTreeSet;
use std::ffi::CString;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
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

fn set_xattr(path: &Path, name: &str, value: &[u8]) -> std::io::Result<()> {
    let path = CString::new(path.as_os_str().as_bytes()).expect("test path has no NUL");
    let name = CString::new(name).expect("xattr name has no NUL");
    let result = unsafe {
        libc::setxattr(
            path.as_ptr(),
            name.as_ptr(),
            value.as_ptr().cast(),
            value.len(),
            0,
        )
    };
    if result == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

fn renameat2(source: &Path, destination: &Path, flags: u32) -> std::io::Result<()> {
    let source = CString::new(source.as_os_str().as_bytes()).expect("test source has no NUL");
    let destination =
        CString::new(destination.as_os_str().as_bytes()).expect("test destination has no NUL");
    let result = unsafe {
        libc::syscall(
            libc::SYS_renameat2,
            libc::AT_FDCWD,
            source.as_ptr(),
            libc::AT_FDCWD,
            destination.as_ptr(),
            flags,
        )
    };
    if result == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
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
        [libc::EINVAL, libc::EOPNOTSUPP, libc::ENOSYS].contains(&error as i32),
        "Direct chown returned unexpected errno {error}"
    );

    let error = set_xattr(&base, "user.yinyang", b"unsupported")
        .expect_err("Direct must reject extended attributes");
    assert_errno(&error, &[libc::EOPNOTSUPP, libc::ENOSYS], "Direct xattr");

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
    let error = renameat2(&base, &root.join("other.txt"), libc::RENAME_EXCHANGE)
        .expect_err("Direct must reject RENAME_EXCHANGE");
    assert_errno(
        &error,
        &[libc::EOPNOTSUPP, libc::ENOSYS, libc::EINVAL],
        "Direct RENAME_EXCHANGE",
    );
    let error = renameat2(&base, &root.join("whiteout"), libc::RENAME_WHITEOUT)
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
    let error = rename
        .join()
        .expect("safe-save rename thread panicked")
        .expect_err("dropped source cleanup must not report a completed rename");
    assert_errno(
        &error,
        &[libc::EIO, libc::ESTALE],
        "safe-save source cleanup failure",
    );
    drop(gate);
    assert_eq!(
        fixture.raw_get("safe.txt").as_deref(),
        Some(replacement.as_slice())
    );
    assert_eq!(
        fixture.raw_get("safe.txt.tmp").as_deref(),
        Some(replacement.as_slice()),
        "safe-save failure must retain the source needed for cleanup retry"
    );

    fs::rename(&source, &destination).expect("retry safe-save source cleanup");
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
