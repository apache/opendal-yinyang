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

//! Read-only Linux FUSE acceptance for pinned range hydration.

use crate::harness::{
    ContentCorruption, Fixture, Frontend, Publication, ServiceGate, assert_errno, directory_bytes,
};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::fs::{FileExt, MetadataExt};
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

const BLOCK_BYTES: usize = 64 * 1024;
const RANGE_BYTES: usize = 4096;
const EIO: i32 = 5;

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

fn read_exact_at(file: &File, mut buffer: &mut [u8], mut offset: u64) {
    while !buffer.is_empty() {
        let count = file
            .read_at(buffer, offset)
            .unwrap_or_else(|error| panic!("pread at offset {offset}: {error}"));
        assert_ne!(count, 0, "unexpected EOF at offset {offset}");
        offset += count as u64;
        buffer = &mut buffer[count..];
    }
}

#[test]
#[ignore = "requires /dev/fuse and isolated S3; run with cargo x test-linux-fuse"]
fn fuse_read_only_range_hydration_and_staging_bound_both_publications() {
    for publication in [Publication::Object, Publication::Service] {
        let fixture = Fixture::new(publication);
        let suffix = fixture.publication().as_str();
        let size = 8 * 1024 * 1024 + 123;
        let expected = payload(size, 0x51);
        fixture.remote_create_file("range.bin", &expected, false);
        let config =
            fixture.volume_config(&format!("stage-read-range-{suffix}"), Frontend::Fuse, true);
        let staging = fixture
            .temp
            .path()
            .join(format!("stage-read-range-{suffix}"));
        let mountpoint = fixture.temp.path().join(format!("mnt-read-range-{suffix}"));
        fs::create_dir(&mountpoint).expect("create range mountpoint");
        let mut mounted = fixture.mount(&config, &mountpoint);
        let path = mounted.mountpoint().join("range.bin");
        let file = File::open(&path).expect("open read-only range file");

        let midpoint = (size / 2) as u64;
        let mut cold = vec![0_u8; RANGE_BYTES];
        read_exact_at(&file, &mut cold, midpoint);
        assert_eq!(
            cold,
            expected[midpoint as usize..midpoint as usize + RANGE_BYTES]
        );

        let boundary = (BLOCK_BYTES - RANGE_BYTES / 2) as u64;
        let mut crossing = vec![0_u8; RANGE_BYTES * 2];
        read_exact_at(&file, &mut crossing, boundary);
        assert_eq!(
            crossing,
            expected[boundary as usize..boundary as usize + crossing.len()],
            "a range spanning a verification-unit boundary must preserve every byte"
        );

        let tail = (size - BLOCK_BYTES) as u64;
        let mut partial_unit = vec![0_u8; BLOCK_BYTES];
        read_exact_at(&file, &mut partial_unit, tail);
        assert_eq!(
            partial_unit,
            expected[tail as usize..],
            "the partial final verification unit must be authenticated and complete"
        );

        assert_eq!(
            file.metadata().expect("fstat range file").len(),
            size as u64
        );
        assert_eq!(
            fs::metadata(&path).expect("stat range path").len(),
            size as u64
        );
        let allocated = directory_bytes(&staging);
        assert!(
            allocated <= 2 * 1024 * 1024,
            "read-only range hydration materialized {allocated} staged bytes for a {size}-byte file; \
             staging must remain bounded by requested units and metadata"
        );

        drop(file);
        mounted.unmount();
    }
}

#[test]
#[ignore = "requires /dev/fuse and isolated S3; run with cargo x test-linux-fuse"]
fn fuse_read_only_range_replacement_preserves_old_fd_and_fresh_open_both_publications() {
    for publication in [Publication::Object, Publication::Service] {
        let fixture = Fixture::new(publication);
        let suffix = fixture.publication().as_str();
        let old_bytes = payload(BLOCK_BYTES * 3 + 11, 0x11);
        let new_bytes = payload(BLOCK_BYTES * 4 + 77, 0x77);
        fixture.remote_create_file("replace.bin", &old_bytes, false);
        let config = fixture.volume_config(
            &format!("stage-read-replace-{suffix}"),
            Frontend::Fuse,
            true,
        );
        let mountpoint = fixture
            .temp
            .path()
            .join(format!("mnt-read-replace-{suffix}"));
        fs::create_dir(&mountpoint).expect("create replacement mountpoint");
        let mut mounted = fixture.mount(&config, &mountpoint);
        let root = mounted.mountpoint().to_path_buf();
        let path = root.join("replace.bin");
        let mut old = File::open(&path).expect("open old replacement descriptor");
        let old_inode = old.metadata().expect("fstat old descriptor").ino();
        let mut prefix = vec![0_u8; RANGE_BYTES];
        read_exact_at(&old, &mut prefix, BLOCK_BYTES as u64 - 2048);
        assert_eq!(
            &prefix,
            &old_bytes[BLOCK_BYTES - 2048..BLOCK_BYTES - 2048 + RANGE_BYTES]
        );

        fixture.remote_replace_file("replace.bin", &new_bytes);
        fixture.remote_create_file("added.txt", b"added after replacement", false);
        mounted.refresh_sighup();
        let refresh =
            mounted.wait_for_output_line("yy: fuse refresh: done (", Duration::from_secs(30));
        let completion = refresh
            .lines()
            .find(|line| line.contains("yy: fuse refresh: done ("))
            .expect("refresh completion line");
        assert!(
            !completion.contains("inode=0 ") && !completion.contains("entries=0 "),
            "replacement refresh must invalidate the changed inode and dentry: {completion}"
        );
        assert!(
            completion.contains("errors=0"),
            "replacement refresh reported notification errors: {completion}"
        );

        let mut fresh = File::open(&path).expect("fresh open of remotely replaced file");
        let fresh_inode = fresh.metadata().expect("fstat fresh descriptor").ino();
        assert_ne!(
            fresh_inode, old_inode,
            "remote replacement must expose a fresh node identity"
        );
        assert_eq!(
            fresh.metadata().expect("fstat fresh descriptor").len(),
            new_bytes.len() as u64
        );
        let mut fresh_bytes = Vec::new();
        fresh
            .read_to_end(&mut fresh_bytes)
            .expect("read remotely replaced file");
        assert_eq!(fresh_bytes, new_bytes);
        assert_eq!(
            fs::read(root.join("added.txt")).expect("read file added before SIGHUP"),
            b"added after replacement"
        );

        assert_eq!(
            old.metadata().expect("fstat retained old descriptor").ino(),
            old_inode
        );
        assert_eq!(
            old.metadata().expect("fstat retained old descriptor").len(),
            old_bytes.len() as u64,
            "remote replacement must not resize an open descriptor"
        );
        let mut retained = Vec::new();
        old.read_to_end(&mut retained)
            .expect("read retained old descriptor");
        assert_eq!(retained, old_bytes);
        drop(fresh);
        drop(old);
        mounted.unmount();
    }
}

#[test]
#[ignore = "requires /dev/fuse and isolated S3; run with cargo x test-linux-fuse"]
fn fuse_read_only_range_corrupt_proof_and_data_report_eio_both_publications() {
    for publication in [Publication::Object, Publication::Service] {
        let fixture = Fixture::new(publication);
        let suffix = fixture.publication().as_str();
        let bytes = payload(BLOCK_BYTES * 2 + 31, 0x42);
        fixture.remote_create_file("proof.bin", &bytes, false);
        fixture.remote_create_file("data.bin", &bytes, false);
        let config = fixture.volume_config(
            &format!("stage-read-corrupt-{suffix}"),
            Frontend::Fuse,
            true,
        );
        let mountpoint = fixture
            .temp
            .path()
            .join(format!("mnt-read-corrupt-{suffix}"));
        fs::create_dir(&mountpoint).expect("create corruption mountpoint");
        let mut mounted = fixture.mount(&config, &mountpoint);

        fixture.corrupt_content("proof.bin", ContentCorruption::Proof);
        fixture.corrupt_content("data.bin", ContentCorruption::Data);

        for name in ["proof.bin", "data.bin"] {
            let error = match File::open(mounted.mountpoint().join(name)) {
                Ok(file) => {
                    let mut value = [0_u8; 1];
                    file.read_at(&mut value, 0)
                        .expect_err("corrupt content must not release bytes")
                }
                Err(error) => error,
            };
            assert_errno(error, &[EIO], name);
        }
        mounted.unmount();
    }
}

#[test]
#[ignore = "requires /dev/fuse and isolated S3; run with cargo x test-linux-fuse"]
fn fuse_read_only_range_admission_rejects_dirty_staging_both_publications() {
    for publication in [Publication::Object, Publication::Service] {
        let fixture = Fixture::new(publication);
        let suffix = fixture.publication().as_str();
        let library = fixture.volume_config(
            &format!("lib-read-dirty-{suffix}"),
            Frontend::Library,
            false,
        );
        fixture.library_seed(&library, "dirty.txt", b"seed");
        let name = format!("stage-read-dirty-{suffix}");
        let writable = fixture.volume_config(&name, Frontend::Fuse, false);
        let first_mountpoint = fixture
            .temp
            .path()
            .join(format!("mnt-read-dirty-writable-{suffix}"));
        fs::create_dir(&first_mountpoint).expect("create writable admission mountpoint");
        let mut mounted = fixture.mount(&writable, &first_mountpoint);
        let mut dirty = OpenOptions::new()
            .write(true)
            .open(mounted.mountpoint().join("dirty.txt"))
            .expect("open dirty descriptor");
        dirty.write_all(b"unpublished").expect("stage dirty bytes");
        mounted.kill_daemon_only();
        drop(dirty);
        mounted.cleanup_after_kill();

        let read_only = fixture.volume_config(&name, Frontend::Fuse, true);
        let rejected_mountpoint = fixture
            .temp
            .path()
            .join(format!("mnt-read-dirty-rejected-{suffix}"));
        fs::create_dir(&rejected_mountpoint).expect("create rejected admission mountpoint");
        let rejected =
            fixture.reject_mount(&read_only, &rejected_mountpoint, Duration::from_secs(30));
        assert!(
            !rejected.status.success(),
            "read-only mount unexpectedly admitted dirty staging"
        );
        let stderr = String::from_utf8_lossy(&rejected.stderr).to_ascii_lowercase();
        assert!(
            stderr.contains("staging contains handle") && stderr.contains("unpublished"),
            "dirty admission failure did not identify unpublished staging:\n{stderr}"
        );
    }
}

#[test]
#[ignore = "requires /dev/fuse and isolated S3; run with cargo x test-linux-fuse"]
fn fuse_read_only_range_admission_rejects_frozen_service_staging() {
    let fixture = Fixture::new(Publication::Service);
    let library = fixture.volume_config("lib-read-frozen", Frontend::Library, false);
    fixture.library_seed(&library, "frozen.txt", b"seed");
    let staging = fixture.temp.path().join("stage-read-frozen");
    let gate = ServiceGate::new(staging, fixture.service_address());
    let config = fixture.volume_config_with_service(
        "stage-read-frozen",
        Frontend::Fuse,
        false,
        Some(gate.address()),
    );
    let mountpoint = fixture.temp.path().join("mnt-read-frozen-writable");
    fs::create_dir(&mountpoint).expect("create frozen admission mountpoint");
    let mut mounted = fixture.mount(&config, &mountpoint);

    let mut dirty = OpenOptions::new()
        .read(true)
        .write(true)
        .open(mounted.mountpoint().join("frozen.txt"))
        .expect("open frozen descriptor");
    dirty
        .write_all(b"frozen-local")
        .expect("stage frozen publication bytes");
    let in_flight = dirty.try_clone().expect("duplicate frozen descriptor");
    let (sender, receiver) = mpsc::channel();
    let caller = thread::spawn(move || {
        let _ = sender.send(in_flight.sync_all());
    });
    gate.wait_ready(Duration::from_secs(30));
    let (commit, frozen) = fixture.wait_for_frozen(&config);
    println!("read-only admission observed saved frozen commit {commit:?}:\n{frozen}");
    mounted.kill_daemon_only();
    gate.release();
    let outcome = receiver
        .recv_timeout(Duration::from_secs(20))
        .expect("frozen fsync did not return after daemon exit");
    caller.join().expect("frozen fsync caller panicked");
    assert!(
        outcome.is_err(),
        "interrupted frozen fsync must not report success"
    );
    drop(dirty);
    mounted.cleanup_after_kill();

    let config = fixture.volume_config_with_service(
        "stage-read-frozen",
        Frontend::Fuse,
        true,
        Some(gate.address()),
    );
    let mountpoint = fixture.temp.path().join("mnt-read-frozen-rejected");
    fs::create_dir(&mountpoint).expect("create frozen rejected mountpoint");
    let rejected = fixture.reject_mount(&config, &mountpoint, Duration::from_secs(30));
    assert!(
        !rejected.status.success(),
        "read-only mount unexpectedly admitted frozen staging"
    );
    let stderr = String::from_utf8_lossy(&rejected.stderr).to_ascii_lowercase();
    assert!(
        stderr.contains("staging contains handle") && stderr.contains("frozen"),
        "frozen admission failure did not identify frozen staging:\n{stderr}"
    );
}
