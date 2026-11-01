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

#[allow(dead_code)]
#[path = "linux_fuse/harness.rs"]
mod harness;

use harness::{Fixture, Frontend, Publication, run_timeout};
use std::fs;
use std::os::unix::fs::{PermissionsExt, symlink};
use std::path::Path;
use std::process::{Command, ExitStatus};
use std::time::Duration;

const APPLICATION_TIMEOUT: Duration = Duration::from_secs(120);

#[derive(Debug)]
struct AppOutput {
    status: ExitStatus,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
}

impl AppOutput {
    fn success_or(&self, context: &str) {
        assert!(
            self.status.success(),
            "{context} failed with {}:\nstdout:\n{}\nstderr:\n{}",
            self.status,
            String::from_utf8_lossy(&self.stdout),
            String::from_utf8_lossy(&self.stderr)
        );
    }
}

fn run_application(command: Command) -> AppOutput {
    let output = run_timeout(command, APPLICATION_TIMEOUT);
    AppOutput {
        status: output.status,
        stdout: output.stdout,
        stderr: output.stderr,
    }
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

fn write_source_tree(source: &Path) -> Vec<u8> {
    fs::create_dir_all(source.join("assets")).expect("create source assets");
    fs::create_dir_all(source.join("bin")).expect("create source bin");
    fs::create_dir_all(source.join("notes")).expect("create source notes");
    fs::create_dir_all(source.join("empty")).expect("create empty source directory");

    let bytes = payload(17, 96 * 1024 + 37);
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
    fs::write(
        source.join("bin/run.sh"),
        b"#!/bin/sh\nprintf 'archive trial\\n'\n",
    )
    .expect("write source script");
    fs::set_permissions(source.join("bin/run.sh"), fs::Permissions::from_mode(0o755))
        .expect("mark source script executable");
    bytes
}

fn rsync(source: &Path, destination: &Path, delete: bool, inplace: bool) -> AppOutput {
    let mut command = Command::new("rsync");
    command.args([
        "-r",
        "--checksum",
        "--no-times",
        "--no-owner",
        "--no-group",
        "--omit-dir-times",
    ]);
    if inplace {
        command.arg("--inplace");
    }
    if delete {
        command.arg("--delete");
    }
    command
        .arg(format!("{}/", source.display()))
        .arg(format!("{}/", destination.display()));
    run_application(command)
}

fn seed_rsync_destination(target: &Path) {
    // rsync creates new files and default safe-save temporaries with mode 0600.
    // This trial therefore exercises explicit in-place updates of entries whose
    // modes the Managed mount can represent instead of claiming general rsync.
    for directory in ["assets", "bin", "notes", "empty"] {
        fs::create_dir_all(target.join(directory))
            .unwrap_or_else(|error| panic!("create rsync target {directory}: {error}"));
    }
    for path in [
        "assets/payload.bin",
        "notes/readme.txt",
        "notes/remove.txt",
        "bin/run.sh",
    ] {
        fs::write(target.join(path), b"")
            .unwrap_or_else(|error| panic!("seed rsync destination {path}: {error}"));
    }
    fs::set_permissions(target.join("bin/run.sh"), fs::Permissions::from_mode(0o755))
        .expect("set seeded script mode");
}

fn assert_default_rsync_creation_rejected(source: &Path, target: &Path) {
    fs::create_dir_all(source).expect("create rejected rsync source");
    fs::create_dir(target).expect("create rejected rsync target");
    fs::write(
        source.join("created.txt"),
        b"rsync cannot create this file\n",
    )
    .expect("write rejected rsync source");

    let rejected = rsync(source, target, false, false);
    assert_eq!(
        rejected.status.code(),
        Some(23),
        "default rsync should report a partial transfer:\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&rejected.stdout),
        String::from_utf8_lossy(&rejected.stderr)
    );
    let stderr = String::from_utf8_lossy(&rejected.stderr).to_ascii_lowercase();
    assert!(
        stderr.contains("mkstemp") && stderr.contains("operation not supported"),
        "default rsync failed before reaching its private temporary-file boundary:\n{stderr}"
    );
    assert!(
        fs::read_dir(target)
            .expect("list rejected rsync target")
            .next()
            .is_none(),
        "a rejected rsync temporary must not leave another destination entry"
    );
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
    assert!(target.join("empty").is_dir());
    assert_eq!(mode(&target.join("empty")), 0o755);
}

#[test]
#[ignore = "requires /dev/fuse, rsync, and isolated S3; run with cargo x test-linux-applications"]
fn application_rsync_mirror_survives_remount_both_publications() {
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
        seed_rsync_destination(&target);
        rsync(&source, &target, false, true).success_or("initial rsync");
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

        let updated_payload = payload(93, 112 * 1024 + 11);
        fs::write(source.join("assets/payload.bin"), &updated_payload)
            .expect("update source payload");
        fs::remove_file(source.join("notes/remove.txt")).expect("remove source note");
        fs::write(source.join("notes/new.txt"), b"added by rsync\n").expect("add source note");
        fs::write(target.join("notes/new.txt"), b"").expect("seed added rsync destination");
        rsync(&source, &target, true, true).success_or("updating rsync with deletion");
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
        assert_default_rsync_creation_rejected(
            &fixture.temp.path().join("rsync-rejected-source"),
            &mountpoint.join("rsync-rejected-target"),
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

fn tar_create(archive: &Path, source: &Path) -> AppOutput {
    let mut command = Command::new("tar");
    command
        .arg("-cf")
        .arg(archive)
        .arg("-C")
        .arg(source)
        .arg(".");
    run_application(command)
}

fn tar_extract(archive: &Path, destination: &Path) -> AppOutput {
    let mut command = Command::new("tar");
    command
        .arg("-xf")
        .arg(archive)
        .arg("-C")
        .arg(destination)
        .args(["--no-same-owner", "--touch", "--no-overwrite-dir"]);
    run_application(command)
}

fn assert_tar_round_trip(root: &Path, expected_payload: &[u8], read_only: bool) {
    let file_mode = if read_only { 0o444 } else { 0o644 };
    let writable_mode = if read_only { 0o555 } else { 0o755 };
    let directory_mode = if read_only { 0o555 } else { 0o755 };
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
        &root.join("bin/run.sh"),
        b"#!/bin/sh\nprintf 'archive trial\\n'\n",
        writable_mode,
    );
    for directory in ["", "assets", "bin", "notes"] {
        let path = if directory.is_empty() {
            root.to_path_buf()
        } else {
            root.join(directory)
        };
        assert_eq!(
            mode(&path),
            directory_mode,
            "directory mode for {}",
            path.display()
        );
    }
    assert!(root.join("empty").is_dir());
    assert_eq!(mode(&root.join("empty")), directory_mode);
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
        assert_tar_round_trip(&extracted, &expected_payload, false);
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
            stderr.contains("symlink"),
            "tar failed before reaching the symlink boundary:\n{stderr}"
        );
        assert!(
            fs::symlink_metadata(symlink_destination.join("link")).is_err(),
            "a rejected symlink must not appear as another file type"
        );

        mounted.unmount();
        drop(mounted);
        let config = fixture.volume_config(&config_name, Frontend::Fuse, true);
        let mut remounted = fixture.mount(&config, &mountpoint);
        assert_tar_round_trip(
            &remounted.mountpoint().join("tar-tree").as_path(),
            &expected_payload,
            true,
        );
        let mounted_archive = fixture.temp.path().join("mounted-tree.tar");
        tar_create(&mounted_archive, &remounted.mountpoint().join("tar-tree"))
            .success_or("archive the read-only mounted tar tree");
        fs::create_dir(&roundtrip).expect("create local tar round-trip root");
        tar_extract(&mounted_archive, &roundtrip)
            .success_or("extract mounted-tree archive locally");
        assert_tar_round_trip(&roundtrip, &expected_payload, true);
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
fn application_git_loose_object_mode_is_rejected() {
    let fixture = Fixture::new(Publication::Object);
    let config = fixture.volume_config("application-git", Frontend::Fuse, false);
    let mountpoint = fixture.temp.path().join("mnt-application-git");
    let local_bare = fixture.temp.path().join("local-repository.git");
    let blob = fixture.temp.path().join("git-blob.txt");
    fs::create_dir(&mountpoint).expect("create Git mountpoint");

    let mut init = Command::new("git");
    init.args(["init", "--bare"])
        .arg(&local_bare)
        .env("GIT_CONFIG_NOSYSTEM", "1");
    run_application(init).success_or("initialize local bare Git fixture");
    fs::write(&blob, b"Git loose object mode probe\n").expect("write Git blob source");

    let mut mounted = fixture.mount(&config, &mountpoint);
    let mounted_bare = mounted.mountpoint().join("repository.git");
    copy_representable_tree(&local_bare, &mounted_bare);

    let mut hash = Command::new("git");
    hash.args(["hash-object"]).arg(&blob);
    let hash = run_application(hash);
    hash.success_or("hash Git probe blob");
    let object_id = String::from_utf8(hash.stdout)
        .expect("Git object ID is UTF-8")
        .trim()
        .to_owned();
    assert_eq!(object_id.len(), 40, "Git object ID length");

    let mut write = Command::new("git");
    write
        .arg("--git-dir")
        .arg(&mounted_bare)
        .args(["hash-object", "-w"])
        .arg(&blob)
        .env("HOME", fixture.temp.path().join("git-home"))
        .env("GIT_CONFIG_NOSYSTEM", "1");
    let rejected = run_application(write);
    assert_eq!(
        rejected.status.code(),
        Some(128),
        "Git loose-object creation should fail explicitly:\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&rejected.stdout),
        String::from_utf8_lossy(&rejected.stderr)
    );
    let stderr = String::from_utf8_lossy(&rejected.stderr).to_ascii_lowercase();
    assert!(
        stderr.contains("unable to create temporary file")
            && stderr.contains("operation not supported"),
        "Git failed before reaching its loose-object temporary file:\n{stderr}"
    );
    let object_path = mounted_bare
        .join("objects")
        .join(&object_id[..2])
        .join(&object_id[2..]);
    assert!(
        fs::symlink_metadata(&object_path).is_err(),
        "a rejected Git object must not exist at {}",
        object_path.display()
    );

    mounted.unmount();
}
