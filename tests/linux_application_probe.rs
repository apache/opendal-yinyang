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

//! Temporary real-FUSE rsync safe-save probe.

#![cfg(target_os = "linux")]

mod support;

use std::fs;
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::Path;
use std::process::Command;
use std::time::Duration;
use support::linux_fuse::{Fixture, Frontend, Publication, run_timeout};

fn write_source_tree(source: &Path) {
    fs::create_dir_all(source.join("nested")).expect("create nested source directory");
    fs::write(
        source.join("nested/regular.txt"),
        b"regular rsync payload\n",
    )
    .expect("write regular source file");
    fs::set_permissions(
        source.join("nested/regular.txt"),
        fs::Permissions::from_mode(0o644),
    )
    .expect("set regular source mode");
    fs::write(source.join("nested/run.sh"), b"#!/bin/sh\nexit 0\n").expect("write script source");
    fs::set_permissions(
        source.join("nested/run.sh"),
        fs::Permissions::from_mode(0o755),
    )
    .expect("set script source mode");
}

fn rsync(source: &Path, destination: &Path) -> support::linux_fuse::Captured {
    let mut command = Command::new("rsync");
    command
        .env("LC_ALL", "C")
        .args(["-r", "--checksum"])
        .args(["--no-times", "--no-owner", "--no-group", "--omit-dir-times"])
        .arg(format!("{}/", source.display()))
        .arg(format!("{}/", destination.display()));
    run_timeout(command, Duration::from_secs(120))
}

#[test]
#[ignore = "temporary Linux FUSE rsync probe; run with cargo test --test linux_application_probe"]
fn probe_rsync_default_creation_fuse_mode() {
    let fixture = Fixture::new(Publication::Object);
    let source = fixture.temp.path().join("probe-source");
    write_source_tree(&source);

    let config = fixture.volume_config("application-probe", Frontend::Fuse, false);
    let mountpoint = fixture.temp.path().join("mnt-application-probe");
    fs::create_dir(&mountpoint).expect("create probe mountpoint");
    let mut mounted = fixture.mount(&config, &mountpoint);
    let target = mounted.mountpoint().join("target");
    fs::create_dir(&target).expect("create empty mounted target");

    let mounted_rsync = rsync(&source, &target);
    eprintln!(
        "mounted rsync status={:?}\nstdout:\n{}\nstderr:\n{}",
        mounted_rsync.status.code(),
        String::from_utf8_lossy(&mounted_rsync.stdout),
        String::from_utf8_lossy(&mounted_rsync.stderr)
    );

    let ordinary = mounted.mountpoint().join("ordinary-create.txt");
    fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o666)
        .open(&ordinary)
        .expect("create an ordinary 0666 file through FUSE");

    eprintln!("yy output:\n{}", mounted.output_text());
    assert_eq!(
        mounted_rsync.status.code(),
        Some(23),
        "default rsync must fail at the current safe-save boundary"
    );
    mounted.unmount();
}
