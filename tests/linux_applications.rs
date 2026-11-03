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

//! Real Linux application acceptance through the YinYang FUSE mount.

#![cfg(target_os = "linux")]

mod support;

use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt, symlink};
use std::path::Path;
use std::process::Command;
use std::time::Duration;
use support::linux_fuse::{Fixture, Frontend, Publication, run_timeout};
use yinyang_test_support::process::Captured;

const APPLICATION_TIMEOUT: Duration = Duration::from_secs(120);

fn run_application(command: Command) -> Captured {
    run_timeout(command, APPLICATION_TIMEOUT)
}

fn git_command() -> Command {
    let mut command = Command::new("git");
    command
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("LC_ALL", "C");
    command
}

fn payload(seed: u8, length: usize) -> Vec<u8> {
    (0..length)
        .map(|index| seed.wrapping_add((index % 251) as u8))
        .collect()
}

fn mode(path: &Path) -> u32 {
    fs::metadata(path)
        .unwrap_or_else(|error| panic!("stat {}: {error}", path.display()))
        .permissions()
        .mode()
        & 0o777
}

fn assert_file(path: &Path, expected: &[u8], expected_mode: u32) {
    let actual = fs::read(path).unwrap_or_else(|error| panic!("read {}: {error}", path.display()));
    assert_eq!(actual, expected, "bytes from {}", path.display());
    assert_eq!(mode(path), expected_mode, "mode for {}", path.display());
}

fn assert_not_found(path: &Path, context: &str) {
    match fs::symlink_metadata(path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => panic!(
            "{context} at {}: expected NotFound, got {error}",
            path.display()
        ),
        Ok(_) => panic!("{context} unexpectedly exists at {}", path.display()),
    }
}

fn write_source_tree(source: &Path) -> Vec<u8> {
    fs::create_dir_all(source.join("assets")).expect("create source assets");
    fs::create_dir_all(source.join("bin")).expect("create source bin");
    fs::create_dir_all(source.join("notes")).expect("create source notes");
    fs::create_dir_all(source.join("empty")).expect("create empty source directory");
    fs::create_dir_all(source.join("private-dir")).expect("create private source directory");

    let bytes = payload(17, 384 * 1024 + 37);
    fs::write(source.join("assets/payload.bin"), &bytes).expect("write source payload");
    fs::write(
        source.join("notes/readme.txt"),
        b"YinYang application trial\n",
    )
    .expect("write source note");
    fs::write(
        source.join("notes/remove.txt"),
        b"this file is removed by the second sync\n",
    )
    .expect("write removable source note");
    fs::write(source.join("notes/private.txt"), b"private rsync mode\n")
        .expect("write private source note");
    fs::set_permissions(
        source.join("notes/private.txt"),
        fs::Permissions::from_mode(0o600),
    )
    .expect("mark source note private");
    fs::set_permissions(
        source.join("private-dir"),
        fs::Permissions::from_mode(0o700),
    )
    .expect("mark source directory private");
    fs::write(
        source.join("bin/run.sh"),
        b"#!/bin/sh\nprintf 'archive trial\\n'\n",
    )
    .expect("write source script");
    fs::set_permissions(source.join("bin/run.sh"), fs::Permissions::from_mode(0o755))
        .expect("mark source script executable");
    bytes
}

fn rsync(source: &Path, destination: &Path, delete: bool) -> Captured {
    let mut command = Command::new("rsync");
    command.env("LC_ALL", "C");
    command.args([
        "-r",
        "--checksum",
        "--no-times",
        "--no-owner",
        "--no-group",
        "--omit-dir-times",
    ]);
    if delete {
        command.arg("--delete");
    }
    command
        .arg(format!("{}/", source.display()))
        .arg(format!("{}/", destination.display()));
    run_application(command)
}

fn assert_rsync_tree(target: &Path, expected_payload: &[u8]) {
    assert_file(&target.join("assets/payload.bin"), expected_payload, 0o644);
    assert_file(
        &target.join("notes/readme.txt"),
        b"YinYang application trial\n",
        0o644,
    );
    assert_file(
        &target.join("bin/run.sh"),
        b"#!/bin/sh\nprintf 'archive trial\\n'\n",
        0o755,
    );
    assert_file(
        &target.join("notes/private.txt"),
        b"private rsync mode\n",
        0o600,
    );
    assert!(target.join("empty").is_dir());
    assert_eq!(mode(&target.join("empty")), 0o755);
    assert_eq!(mode(&target.join("private-dir")), 0o700);
}

#[test]
#[ignore = "requires /dev/fuse, rsync, and isolated S3; run with cargo x test-linux-applications"]
fn application_rsync_creates_and_replaces_without_seeded_destination_both_publications() {
    for publication in [Publication::Object, Publication::Service] {
        let fixture = Fixture::new(publication);
        let suffix = fixture.publication().as_str();
        let config = fixture.volume_config(
            &format!("application-rsync-{suffix}"),
            Frontend::Fuse,
            false,
        );
        let mountpoint = fixture
            .temp
            .path()
            .join(format!("mnt-application-rsync-{suffix}"));
        fs::create_dir(&mountpoint).expect("create rsync mountpoint");
        let source = fixture.temp.path().join("rsync-source");
        let target = mountpoint.join("rsync-target");
        let initial_payload = write_source_tree(&source);

        let mut mounted = fixture.mount(&config, &mountpoint);
        let mount_root = mounted.mountpoint().to_path_buf();
        rsync(&source, &target, false).success_or("initial rsync");
        rsync(&source, &target, false).success_or("repeated rsync");
        assert_rsync_tree(&target, &initial_payload);
        assert_eq!(
            fixture.remote_read(
                &config,
                "rsync-target/assets/payload.bin",
                initial_payload.len()
            ),
            initial_payload,
            "the independent authority must observe rsync payload bytes"
        );

        let updated_payload = payload(93, 448 * 1024 + 11);
        fs::write(source.join("assets/payload.bin"), &updated_payload)
            .expect("update source payload");
        fs::remove_file(source.join("notes/remove.txt")).expect("remove source note");
        fs::write(source.join("notes/new.txt"), b"added by rsync\n").expect("add source note");
        rsync(&source, &target, true).success_or("updating rsync with deletion");
        assert_file(&target.join("assets/payload.bin"), &updated_payload, 0o644);
        assert!(
            !target.join("notes/remove.txt").exists(),
            "rsync --delete must remove the stale mounted file"
        );
        assert_file(&target.join("notes/new.txt"), b"added by rsync\n", 0o644);
        assert_eq!(
            fixture.remote_read(&config, "rsync-target/notes/new.txt", 32),
            b"added by rsync\n"
        );
        mounted.unmount();
        drop(mounted);
        let mut remounted = fixture.mount(&config, &mountpoint);
        assert_eq!(remounted.mountpoint(), mount_root);
        assert_rsync_tree(
            remounted.mountpoint().join("rsync-target").as_path(),
            &updated_payload,
        );
        assert!(
            !remounted
                .mountpoint()
                .join("rsync-target/notes/remove.txt")
                .exists()
        );
        assert_file(
            &remounted.mountpoint().join("rsync-target/notes/new.txt"),
            b"added by rsync\n",
            0o644,
        );
        assert_file(
            &remounted
                .mountpoint()
                .join("rsync-target/notes/private.txt"),
            b"private rsync mode\n",
            0o600,
        );
        assert_eq!(
            fixture.remote_read(
                &config,
                "rsync-target/assets/payload.bin",
                updated_payload.len()
            ),
            updated_payload,
            "rsync bytes must remain authoritative after remount"
        );
        remounted.unmount();
    }
}

#[test]
#[ignore = "requires /dev/fuse and isolated S3; run with cargo x test-linux-applications"]
fn application_safe_save_0600_replacement_preserves_old_fd_and_remounts_both_publications() {
    for publication in [Publication::Object, Publication::Service] {
        let fixture = Fixture::new(publication);
        let suffix = fixture.publication().as_str();
        let config = fixture.volume_config(
            &format!("application-safe-save-{suffix}"),
            Frontend::Fuse,
            false,
        );
        let mountpoint = fixture
            .temp
            .path()
            .join(format!("mnt-application-safe-save-{suffix}"));
        fs::create_dir(&mountpoint).expect("create safe-save mountpoint");
        let mut mounted = fixture.mount(&config, &mountpoint);
        let root = mounted.mountpoint();
        let umask_file = root.join("umask-file.txt");
        let umask_directory = root.join("umask-directory");
        let umask_created = Command::new("sh")
            .env("LC_ALL", "C")
            .args(["-c", "umask 022; : > \"$1\"; mkdir \"$2\"", "sh"])
            .arg(&umask_file)
            .arg(&umask_directory)
            .output()
            .expect("run umask create probe");
        assert!(
            umask_created.status.success(),
            "umask create probe failed:\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&umask_created.stdout),
            String::from_utf8_lossy(&umask_created.stderr)
        );
        assert_file(&umask_file, b"", 0o644);
        assert_eq!(mode(&umask_directory), 0o755, "directory umask mode");

        let target = root.join("safe-save.bin");
        let old_bytes = payload(31, 192 * 1024 + 7);
        let new_bytes = payload(197, 256 * 1024 + 13);

        fs::write(&target, &old_bytes).expect("write old safe-save target");
        fs::set_permissions(&target, fs::Permissions::from_mode(0o644))
            .expect("set old safe-save target mode");
        let mut old_fd = File::open(&target).expect("open old safe-save descriptor");

        let temporary = root.join(".safe-save.tmp");
        let mut replacement = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&temporary)
            .expect("create private safe-save temporary");
        assert_eq!(mode(&temporary), 0o600, "safe-save temporary mode");
        replacement
            .write_all(&new_bytes)
            .expect("write safe-save temporary");
        replacement.sync_all().expect("fsync safe-save temporary");
        replacement
            .set_permissions(fs::Permissions::from_mode(0o644))
            .expect("change safe-save temporary mode");
        assert_eq!(mode(&temporary), 0o644, "safe-save final mode");
        drop(replacement);

        fs::rename(&temporary, &target).expect("safe-save rename replacement");
        assert_file(&target, &new_bytes, 0o644);

        let mut fresh_fd = File::open(&target).expect("open fresh safe-save descriptor");
        let mut fresh_bytes = Vec::new();
        fresh_fd
            .read_to_end(&mut fresh_bytes)
            .expect("read fresh safe-save bytes");
        assert_eq!(fresh_bytes, new_bytes, "fresh fd bytes after replacement");

        old_fd
            .seek(SeekFrom::Start(0))
            .expect("rewind old safe-save descriptor");
        let mut retained_old_bytes = Vec::new();
        old_fd
            .read_to_end(&mut retained_old_bytes)
            .expect("read old safe-save bytes");
        assert_eq!(
            retained_old_bytes, old_bytes,
            "old fd bytes must remain pinned across rename replacement"
        );
        assert_eq!(
            fixture.remote_read(&config, "safe-save.bin", new_bytes.len()),
            new_bytes,
            "independent authority must observe replacement bytes"
        );
        drop(fresh_fd);
        drop(old_fd);

        mounted.unmount();
        drop(mounted);
        let mut remounted = fixture.mount(&config, &mountpoint);
        assert_file(
            &remounted.mountpoint().join("safe-save.bin"),
            &new_bytes,
            0o644,
        );
        assert_eq!(
            fixture.remote_read(&config, "safe-save.bin", new_bytes.len()),
            new_bytes,
            "safe-save bytes must remain authoritative after remount"
        );
        remounted.unmount();
    }
}

fn tar_create(archive: &Path, source: &Path) -> Captured {
    let mut command = Command::new("tar");
    command
        .env("LC_ALL", "C")
        .arg("-cf")
        .arg(archive)
        .arg("-C")
        .arg(source)
        .arg(".");
    run_application(command)
}

fn tar_extract(archive: &Path, destination: &Path) -> Captured {
    let mut command = Command::new("tar");
    command
        .env("LC_ALL", "C")
        .arg("-xf")
        .arg(archive)
        .arg("-C")
        .arg(destination)
        .args(["--no-same-owner", "--touch", "--no-overwrite-dir"]);
    run_application(command)
}

fn assert_tar_round_trip(root: &Path, expected_payload: &[u8], read_only: bool, root_mode: u32) {
    let file_mode = if read_only { 0o444 } else { 0o644 };
    let executable_mode = if read_only { 0o555 } else { 0o755 };
    let directory_mode = if read_only { 0o555 } else { 0o755 };
    let private_file_mode = if read_only { 0o400 } else { 0o600 };
    let private_directory_mode = if read_only { 0o500 } else { 0o700 };
    assert_eq!(mode(root), root_mode, "extraction root mode");
    assert_file(
        &root.join("assets/payload.bin"),
        expected_payload,
        file_mode,
    );
    assert_file(
        &root.join("notes/readme.txt"),
        b"YinYang application trial\n",
        file_mode,
    );
    assert_file(
        &root.join("notes/remove.txt"),
        b"this file is removed by the second sync\n",
        file_mode,
    );
    assert_file(
        &root.join("notes/private.txt"),
        b"private rsync mode\n",
        private_file_mode,
    );
    assert_file(
        &root.join("bin/run.sh"),
        b"#!/bin/sh\nprintf 'archive trial\\n'\n",
        executable_mode,
    );
    for directory in ["assets", "bin", "notes"] {
        let path = root.join(directory);
        assert_eq!(
            mode(&path),
            directory_mode,
            "directory mode for {}",
            path.display()
        );
    }
    assert!(root.join("empty").is_dir());
    assert_eq!(mode(&root.join("empty")), directory_mode);
    assert_eq!(mode(&root.join("private-dir")), private_directory_mode);
}

#[test]
#[ignore = "requires /dev/fuse, GNU tar, and isolated S3; run with cargo x test-linux-applications"]
fn application_gnu_tar_extracts_reads_and_rejects_symlink_both_publications() {
    for publication in [Publication::Object, Publication::Service] {
        let fixture = Fixture::new(publication);
        let suffix = fixture.publication().as_str();
        let source = fixture.temp.path().join("tar-source");
        let archive = fixture.temp.path().join("tar-source.tar");
        let roundtrip = fixture.temp.path().join("tar-roundtrip");
        let expected_payload = write_source_tree(&source);
        tar_create(&archive, &source).success_or("create source tar archive");

        let config_name = format!("application-tar-{suffix}");
        let config = fixture.volume_config(&config_name, Frontend::Fuse, false);
        let mountpoint = fixture
            .temp
            .path()
            .join(format!("mnt-application-tar-{suffix}"));
        fs::create_dir(&mountpoint).expect("create tar mountpoint");
        let extracted = mountpoint.join("tar-tree");

        let mut mounted = fixture.mount(&config, &mountpoint);
        fs::create_dir(&extracted).expect("create mounted extraction root");
        tar_extract(&archive, &extracted).success_or("extract tar through FUSE");
        // --no-overwrite-dir leaves the pre-created writable root at 0755.
        assert_tar_round_trip(&extracted, &expected_payload, false, 0o755);
        assert_eq!(
            fixture.remote_read(&config, "tar-tree/notes/readme.txt", 64),
            b"YinYang application trial\n"
        );

        let symlink_source = fixture.temp.path().join("tar-symlink-source");
        let symlink_archive = fixture.temp.path().join("tar-symlink.tar");
        let symlink_destination = mountpoint.join("unsupported-symlink");
        fs::create_dir(&symlink_source).expect("create symlink archive source");
        symlink("target.txt", symlink_source.join("link")).expect("create source symlink");
        tar_create(&symlink_archive, &symlink_source).success_or("create symlink archive");
        fs::create_dir(&symlink_destination).expect("create symlink extraction root");
        let rejected = tar_extract(&symlink_archive, &symlink_destination);
        assert!(
            !rejected.status.success(),
            "GNU tar unexpectedly created a symlink through the mount:\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&rejected.stdout),
            String::from_utf8_lossy(&rejected.stderr)
        );
        let stderr = String::from_utf8_lossy(&rejected.stderr).to_ascii_lowercase();
        assert!(
            stderr.contains("cannot create symlink") && stderr.contains("operation not supported"),
            "tar failed before reaching the symlink syscall with the expected diagnostic:\n{stderr}"
        );
        assert_not_found(
            &symlink_destination.join("link"),
            "rejected symlink destination",
        );

        mounted.unmount();
        drop(mounted);
        let config = fixture.volume_config(&config_name, Frontend::Fuse, true);
        let mut remounted = fixture.mount(&config, &mountpoint);
        assert_tar_round_trip(
            remounted.mountpoint().join("tar-tree").as_path(),
            &expected_payload,
            true,
            0o555,
        );
        let mounted_archive = fixture.temp.path().join("mounted-tree.tar");
        assert!(
            !mounted_archive.starts_with(remounted.mountpoint()),
            "the backup archive must be written outside the mounted tree"
        );
        tar_create(&mounted_archive, &remounted.mountpoint().join("tar-tree"))
            .success_or("archive the read-only mounted tar tree");
        fs::create_dir(&roundtrip).expect("create local tar round-trip root");
        tar_extract(&mounted_archive, &roundtrip)
            .success_or("extract mounted-tree archive locally");
        // The local no-overwrite-dir target also keeps its pre-created 0755 root.
        assert_tar_round_trip(&roundtrip, &expected_payload, true, 0o755);
        assert_eq!(
            fixture.remote_read(&config, "tar-tree/bin/run.sh", 64),
            b"#!/bin/sh\nprintf 'archive trial\\n'\n"
        );
        remounted.unmount();
    }
}

fn copy_representable_tree(source: &Path, destination: &Path) {
    fs::create_dir_all(destination).expect("create representable tree destination");
    fs::set_permissions(destination, fs::Permissions::from_mode(0o755))
        .expect("set representable directory mode");
    for entry in fs::read_dir(source).expect("read representable tree source") {
        let entry = entry.expect("read representable tree entry");
        let source_path = entry.path();
        let destination_path = destination.join(entry.file_name());
        let metadata = fs::symlink_metadata(&source_path)
            .unwrap_or_else(|error| panic!("stat {}: {error}", source_path.display()));
        if metadata.file_type().is_symlink() {
            panic!(
                "test fixture unexpectedly contains a symlink: {}",
                source_path.display()
            );
        }
        if metadata.is_dir() {
            copy_representable_tree(&source_path, &destination_path);
        } else if metadata.is_file() {
            fs::copy(&source_path, &destination_path)
                .unwrap_or_else(|error| panic!("copy {}: {error}", source_path.display()));
            let file_mode = if metadata.permissions().mode() & 0o111 != 0 {
                0o755
            } else {
                0o644
            };
            fs::set_permissions(&destination_path, fs::Permissions::from_mode(file_mode))
                .expect("set representable file mode");
        } else {
            panic!(
                "test fixture contains an unsupported file type: {}",
                source_path.display()
            );
        }
    }
}

#[test]
#[ignore = "requires /dev/fuse, git, and isolated S3; run with cargo x test-linux-applications"]
fn application_git_loose_object_private_mode_probe() {
    let fixture = Fixture::new(Publication::Object);
    let config = fixture.volume_config("application-git", Frontend::Fuse, false);
    let mountpoint = fixture.temp.path().join("mnt-application-git");
    let local_bare = fixture.temp.path().join("local-repository.git");
    let blob = fixture.temp.path().join("git-blob.txt");
    fs::create_dir(&mountpoint).expect("create Git mountpoint");

    let mut init = git_command();
    init.args(["init", "--bare"]).arg(&local_bare);
    run_application(init).success_or("initialize local bare Git fixture");
    fs::write(&blob, b"Git loose object mode probe\n").expect("write Git blob source");

    let mut mounted = fixture.mount(&config, &mountpoint);
    let mounted_bare = mounted.mountpoint().join("repository.git");
    copy_representable_tree(&local_bare, &mounted_bare);

    let mut hash = git_command();
    hash.args(["hash-object"]).arg(&blob);
    let hash = run_application(hash);
    hash.success_or("hash Git probe blob");
    let object_id = String::from_utf8(hash.stdout)
        .expect("Git object ID is UTF-8")
        .trim()
        .to_owned();
    assert_eq!(object_id.len(), 40, "Git object ID length");

    let mut write = git_command();
    write
        .arg("--git-dir")
        .arg(&mounted_bare)
        .args(["hash-object", "-w"])
        .arg(&blob);
    run_application(write).success_or("write Git loose object through the mode boundary");
    let object_path = mounted_bare
        .join("objects")
        .join(&object_id[..2])
        .join(&object_id[2..]);
    assert!(object_path.is_file(), "Git loose object was not created");

    let mut read = git_command();
    read.arg("--git-dir")
        .arg(&mounted_bare)
        .args(["cat-file", "-p", &object_id]);
    let read = run_application(read);
    read.success_or("read Git loose object through the mount");
    assert_eq!(
        read.stdout, b"Git loose object mode probe\n",
        "loose-object bytes through the mount"
    );

    mounted.unmount();
}
