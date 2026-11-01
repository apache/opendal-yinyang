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

fn rsync(source: &Path, destination: &Path, delete: bool) -> AppOutput {
    let mut command = Command::new("rsync");
    command.args([
        "-r",
        "--checksum",
        "--no-times",
        "--no-owner",
        "--no-group",
        "--omit-dir-times",
        "--inplace",
    ]);
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
        rsync(&source, &target, false).success_or("initial rsync");
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

fn assert_tar_round_trip(root: &Path, expected_payload: &[u8]) {
    assert_file(&root.join("assets/payload.bin"), expected_payload, 0o644);
    let script = root.join("bin/run.sh");
    assert_eq!(
        fs::read(&script).expect("read extracted script"),
        b"#!/bin/sh\nprintf 'archive trial\\n'\n"
    );
    assert_eq!(mode(&script), 0o755, "tar must restore executable mode");
    assert_eq!(
        fs::read(root.join("notes/readme.txt")).expect("read extracted note"),
        b"YinYang application trial\n"
    );
    assert!(root.join("empty").is_dir());
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

        let config =
            fixture.volume_config(&format!("application-tar-{suffix}"), Frontend::Fuse, false);
        let mountpoint = fixture
            .temp
            .path()
            .join(format!("mnt-application-tar-{suffix}"));
        fs::create_dir(&mountpoint).expect("create tar mountpoint");
        let extracted = mountpoint.join("tar-tree");

        let mut mounted = fixture.mount(&config, &mountpoint);
        fs::create_dir(&extracted).expect("create mounted extraction root");
        tar_extract(&archive, &extracted).success_or("extract tar through FUSE");
        assert_tar_round_trip(&extracted, &expected_payload);
        assert_eq!(
            fixture.remote_read(&config, "tar-tree/notes/readme.txt", 64),
            b"YinYang application trial\n"
        );

        let mounted_archive = fixture.temp.path().join("mounted-tree.tar");
        let archived = tar_create(&mounted_archive, &extracted);
        if archived.status.code() == Some(1) {
            let stderr = String::from_utf8_lossy(&archived.stderr);
            assert!(
                stderr.contains("file changed as we read it"),
                "GNU tar exited 1 without its documented metadata-change diagnostic:\n{stderr}"
            );
        } else {
            archived.success_or("archive the mounted tar tree");
        }
        fs::create_dir(&roundtrip).expect("create local tar round-trip root");
        tar_extract(&mounted_archive, &roundtrip)
            .success_or("extract mounted-tree archive locally");
        assert_tar_round_trip(&roundtrip, &expected_payload);

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
        let mut remounted = fixture.mount(&config, &mountpoint);
        assert_tar_round_trip(
            &remounted.mountpoint().join("tar-tree").as_path(),
            &expected_payload,
        );
        assert_eq!(
            fixture.remote_read(&config, "tar-tree/bin/run.sh", 64),
            b"#!/bin/sh\nprintf 'archive trial\\n'\n"
        );
        remounted.unmount();
    }
}

#[test]
#[ignore = "one-off fresh-main Git capability probe"]
fn probe_git_loose_object_creation() {
    let fixture = Fixture::new(Publication::Object);
    let config = fixture.volume_config("probe-git", Frontend::Fuse, false);
    let mountpoint = fixture.temp.path().join("mnt-probe-git");
    fs::create_dir(&mountpoint).expect("create Git probe mountpoint");
    let mut mounted = fixture.mount(&config, &mountpoint);
    let home = fixture.temp.path().join("git-home");
    fs::create_dir(&home).expect("create isolated Git home");

    let mut init = Command::new("git");
    init.args(["init", "--template=", "repository"])
        .current_dir(mounted.mountpoint());
    let init = run_application(init);
    eprintln!(
        "git init: {}\nstdout:\n{}\nstderr:\n{}",
        init.status,
        String::from_utf8_lossy(&init.stdout),
        String::from_utf8_lossy(&init.stderr)
    );
    if !init.status.success() {
        mounted.unmount();
        return;
    }

    let repository = mounted.mountpoint().join("repository");
    fs::write(repository.join("tracked.txt"), b"probe\n").expect("write tracked Git file");
    let mut add = Command::new("git");
    add.args(["add", "tracked.txt"])
        .current_dir(&repository)
        .env("HOME", &home)
        .env("GIT_CONFIG_NOSYSTEM", "1");
    let add = run_application(add);
    eprintln!(
        "git add: {}\nstdout:\n{}\nstderr:\n{}",
        add.status,
        String::from_utf8_lossy(&add.stdout),
        String::from_utf8_lossy(&add.stderr)
    );

    let objects = repository.join(".git/objects");
    eprintln!("git objects directory: {}", objects.display());
    mounted.unmount();
}
