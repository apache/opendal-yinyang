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

//! Temporary rsync mode and rename probe.

#![cfg(target_os = "linux")]

mod support;

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::process::Command;
use std::time::Duration;
use support::linux_fuse::{Fixture, Frontend, Publication, run_timeout};

const TRACE_FILTER: &str = "trace=execve,mkdir,mkdirat,open,openat,creat,rename,renameat,renameat2,chmod,fchmod,fchmodat,fsync,fdatasync,close";

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

fn traced_rsync(source: &Path, destination: &Path, trace: &Path) -> support::linux_fuse::Captured {
    let mut command = Command::new("strace");
    command
        .env("LC_ALL", "C")
        .args(["-f", "-qq", "-s", "200", "-o"])
        .arg(trace)
        .args(["-e", TRACE_FILTER, "rsync", "-r", "--checksum"])
        .args(["--no-times", "--no-owner", "--no-group", "--omit-dir-times"])
        .arg(format!("{}/", source.display()))
        .arg(format!("{}/", destination.display()));
    run_timeout(command, Duration::from_secs(120))
}

fn print_trace(path: &Path) {
    let trace = fs::read_to_string(path).expect("read strace output");
    eprintln!("=== trace {} ===\n{trace}", path.display());
}

#[test]
#[ignore = "temporary Linux rsync probe; run with cargo test --test linux_application_probe"]
fn probe_rsync_modes_and_safe_save() {
    let fixture = Fixture::new(Publication::Object);
    let source = fixture.temp.path().join("probe-source");
    write_source_tree(&source);

    let local_target = fixture.temp.path().join("probe-local-target");
    fs::create_dir(&local_target).expect("create local control target");
    let local_trace = fixture.temp.path().join("probe-local.trace");
    let local = traced_rsync(&source, &local_target, &local_trace);
    eprintln!(
        "local rsync status={:?}\nstdout:\n{}\nstderr:\n{}",
        local.status.code(),
        String::from_utf8_lossy(&local.stdout),
        String::from_utf8_lossy(&local.stderr)
    );
    print_trace(&local_trace);
    local.success_or("local rsync control");

    let config = fixture.volume_config("application-probe", Frontend::Fuse, false);
    let mountpoint = fixture.temp.path().join("mnt-application-probe");
    fs::create_dir(&mountpoint).expect("create probe mountpoint");
    let mut mounted = fixture.mount(&config, &mountpoint);
    let target = mounted.mountpoint().join("target");
    fs::create_dir(&target).expect("create empty mounted target");

    let mounted_trace = fixture.temp.path().join("probe-mounted.trace");
    let mounted_rsync = traced_rsync(&source, &target, &mounted_trace);
    eprintln!(
        "mounted rsync status={:?}\nstdout:\n{}\nstderr:\n{}",
        mounted_rsync.status.code(),
        String::from_utf8_lossy(&mounted_rsync.stdout),
        String::from_utf8_lossy(&mounted_rsync.stderr)
    );
    print_trace(&mounted_trace);

    let create = Command::new("sh")
        .env("LC_ALL", "C")
        .args(["-c", "umask 022; : > \"$1\"; umask 077; : > \"$2\"", "sh"])
        .arg(mounted.mountpoint().join("ordinary-create.txt"))
        .arg(mounted.mountpoint().join("private-create.txt"))
        .output()
        .expect("run controlled create probe");
    eprintln!(
        "controlled create status={:?}\nstdout:\n{}\nstderr:\n{}",
        create.status.code(),
        String::from_utf8_lossy(&create.stdout),
        String::from_utf8_lossy(&create.stderr)
    );
    eprintln!("yy output:\n{}", mounted.output_text());
    assert!(!mounted_rsync.status.success());
    mounted.unmount();
}
