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

//! Incremental write acceptance through real Linux FUSE descriptors.

use crate::harness::{Fixture, Frontend, Publication, ServiceGate, directory_bytes};
use nix::unistd::{getegid, geteuid, truncate};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::os::unix::fs::{FileExt, MetadataExt, PermissionsExt};
use std::path::Path;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

const UNIT_BYTES: usize = 64 * 1024;
const RANGE_BYTES: usize = 4096;
const INITIAL_BYTES: usize = 8 * UNIT_BYTES + 3 * RANGE_BYTES + 73;
const LARGE_INITIAL_BYTES: usize = 32 * 1024 * 1024;
const LARGE_STAGING_LIMIT: u64 = 2 * 1024 * 1024;

#[test]
#[ignore = "requires /dev/fuse and isolated S3; run with cargo x test-linux-fuse"]
fn fuse_incremental_sparse_units_eof_and_descriptor_contract_both_publications() {
    for publication in [Publication::Object, Publication::Service] {
        sparse_units_eof_and_descriptor_contract(&Fixture::new(publication));
    }
}

fn sparse_units_eof_and_descriptor_contract(fixture: &Fixture) {
    let suffix = fixture.publication().as_str();
    let library = fixture.volume_config(
        &format!("lib-incremental-contract-{suffix}"),
        Frontend::Library,
        false,
    );
    let mut expected = payload(INITIAL_BYTES, 0x28);
    fixture.library_seed(&library, "incremental.bin", &expected);
    let config = fixture.volume_config(
        &format!("stage-incremental-contract-{suffix}"),
        Frontend::Fuse,
        false,
    );
    let mountpoint = fixture
        .temp
        .path()
        .join(format!("mnt-incremental-contract-{suffix}"));
    let staging = fixture
        .temp
        .path()
        .join(format!("stage-incremental-contract-{suffix}"));
    fs::create_dir(&mountpoint).expect("create incremental contract mountpoint");
    let mut mounted = fixture.mount(&config, &mountpoint);
    let path = mounted.mountpoint().join("incremental.bin");

    assert_attributes(&path, expected.len() as u64);
    let early_reader = File::open(&path).expect("open descriptor before sparse writes");
    assert_attributes_from(&early_reader, expected.len() as u64);
    let first_writer = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .expect("open first sparse writer");
    let second_writer = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .expect("open second sparse writer before the first publication");
    assert_staging_bounded(&staging, "open");

    let bulk_offset = UNIT_BYTES;
    let bulk = payload(3 * UNIT_BYTES + 123, 0x43);
    overwrite_expected(&mut expected, bulk_offset, &bulk);
    write_all_at(&first_writer, &bulk, bulk_offset as u64);
    first_writer
        .sync_all()
        .expect("fsync a large write split across multiple FUSE requests");
    assert_remote(fixture, &library, &expected, "large multi-request fsync");
    assert_range(&early_reader, bulk_offset, &bulk);
    assert_staging_bounded(&staging, "large write");

    let first_offset = UNIT_BYTES - RANGE_BYTES / 2;
    let first = payload(RANGE_BYTES, 0x51);
    overwrite_expected(&mut expected, first_offset, &first);
    write_all_at(&first_writer, &first, first_offset as u64);
    first_writer
        .sync_all()
        .expect("fsync a range crossing a verification-unit boundary");
    assert_remote(fixture, &library, &expected, "unit-boundary fsync");
    assert_range(&early_reader, first_offset, &first);
    assert_attributes(&path, expected.len() as u64);

    let second_offset = 2 * UNIT_BYTES + 5;
    let second = payload(RANGE_BYTES + 11, 0x63);
    overwrite_expected(&mut expected, second_offset, &second);
    write_all_at(&second_writer, &second, second_offset as u64);
    second_writer
        .sync_all()
        .expect("fsync a same-length update through a second descriptor");
    assert_remote(fixture, &library, &expected, "multi-fd fsync");
    assert_range(&early_reader, second_offset, &second);

    let mut append = OpenOptions::new()
        .append(true)
        .open(&path)
        .expect("open append descriptor");
    let appended = payload(37, 0x75);
    append.write_all(&appended).expect("append through FUSE");
    append.sync_all().expect("fsync append");
    expected.extend_from_slice(&appended);
    assert_remote(fixture, &library, &expected, "append fsync");
    assert_range(&early_reader, expected.len() - appended.len(), &appended);
    assert_attributes_from(&early_reader, expected.len() as u64);

    let shrunk = 6 * UNIT_BYTES + 23;
    first_writer
        .set_len(shrunk as u64)
        .expect("shrink through an active descriptor");
    first_writer.sync_all().expect("fsync shrink");
    expected.truncate(shrunk);
    assert_remote(fixture, &library, &expected, "descriptor shrink fsync");
    assert_attributes(&path, expected.len() as u64);
    assert_attributes_from(&early_reader, expected.len() as u64);

    let extended = 6 * UNIT_BYTES + 2 * RANGE_BYTES + 9;
    first_writer
        .set_len(extended as u64)
        .expect("extend with a sparse zero gap");
    first_writer.sync_all().expect("fsync zero extension");
    expected.resize(extended, 0);
    assert_remote(fixture, &library, &expected, "descriptor extension fsync");
    assert_attributes_from(&early_reader, expected.len() as u64);

    let eof_offset = extended - 4;
    let eof_write = payload(17, 0x87);
    overwrite_expected(&mut expected, eof_offset, &eof_write);
    write_all_at(&first_writer, &eof_write, eof_offset as u64);
    first_writer
        .sync_all()
        .expect("fsync an overwrite that crosses the staged EOF");
    assert_remote(fixture, &library, &expected, "EOF-growing fsync");
    assert_range(&early_reader, eof_offset, &eof_write);
    assert_attributes_from(&early_reader, expected.len() as u64);
    assert_eof_reads(&first_writer, &expected);
    assert_eof_reads(&early_reader, &expected);

    let path_shrunk = 5 * UNIT_BYTES + 31;
    truncate(
        path.as_path(),
        i64::try_from(path_shrunk).expect("path shrink fits off_t"),
    )
    .expect("path truncate shrink");
    expected.truncate(path_shrunk);
    assert_remote(fixture, &library, &expected, "path shrink");
    assert_attributes(&path, expected.len() as u64);

    let path_extended = path_shrunk + RANGE_BYTES;
    truncate(
        path.as_path(),
        i64::try_from(path_extended).expect("path extension fits off_t"),
    )
    .expect("path truncate zero extension");
    expected.resize(path_extended, 0);
    assert_remote(fixture, &library, &expected, "path zero extension");
    assert_attributes(&path, expected.len() as u64);

    drop(append);
    drop(second_writer);
    drop(first_writer);
    drop(early_reader);
    mounted.unmount();

    let restarted_mountpoint = fixture
        .temp
        .path()
        .join(format!("mnt-incremental-contract-restart-{suffix}"));
    fs::create_dir(&restarted_mountpoint).expect("create restarted incremental mountpoint");
    let mut restarted = fixture.mount(&config, &restarted_mountpoint);
    let restarted_path = restarted.mountpoint().join("incremental.bin");
    assert_attributes(&restarted_path, expected.len() as u64);
    assert_eq!(
        fs::read(&restarted_path).expect("read restarted incremental file"),
        expected,
        "restart changed sparse bytes or zero-extended attributes"
    );
    restarted.unmount();
}

#[test]
#[ignore = "requires /dev/fuse and isolated S3; run with cargo x test-linux-fuse"]
fn fuse_length_changing_shared_descriptors_and_remount_both_publications() {
    for publication in [Publication::Object, Publication::Service] {
        length_changing_shared_descriptors_and_remount(&Fixture::new(publication));
    }
}

fn length_changing_shared_descriptors_and_remount(fixture: &Fixture) {
    let suffix = fixture.publication().as_str();
    let library = fixture.volume_config(
        &format!("lib-length-changing-{suffix}"),
        Frontend::Library,
        false,
    );
    let mut expected = payload(LARGE_INITIAL_BYTES, 0x37);
    fixture.library_seed(&library, "length-changing.bin", &expected);
    let config = fixture.volume_config(
        &format!("stage-length-changing-{suffix}"),
        Frontend::Fuse,
        false,
    );
    let staging = fixture
        .temp
        .path()
        .join(format!("stage-length-changing-{suffix}"));
    let mountpoint = fixture
        .temp
        .path()
        .join(format!("mnt-length-changing-{suffix}"));
    fs::create_dir(&mountpoint).expect("create length-changing mountpoint");
    let mut mounted = fixture.mount(&config, &mountpoint);
    let path = mounted.mountpoint().join("length-changing.bin");

    let shared_reader = File::open(&path).expect("open shared read-only descriptor");
    let mut writer = OpenOptions::new()
        .read(true)
        .write(true)
        .append(true)
        .open(&path)
        .expect("open append descriptor");
    assert_staging_below(&staging, LARGE_STAGING_LIMIT, "length-changing open");
    assert_length_contract(
        fixture,
        &library,
        "length-changing.bin",
        &path,
        &writer,
        &shared_reader,
        &expected,
        "length-changing open",
    );

    for sample in 0..2 {
        let value = payload(RANGE_BYTES, 0x49 + sample as u8);
        let offset = expected.len();
        writer.write_all(&value).expect("small append through FUSE");
        writer.sync_all().expect("fsync small append");
        expected.extend_from_slice(&value);
        assert_range(&shared_reader, offset, &value);
        assert_length_contract(
            fixture,
            &library,
            "length-changing.bin",
            &path,
            &writer,
            &shared_reader,
            &expected,
            "small append",
        );
    }
    assert_staging_below(&staging, LARGE_STAGING_LIMIT, "small append");

    for sample in 0..2 {
        let value = payload(UNIT_BYTES + RANGE_BYTES, 0x59 + sample as u8);
        let offset = expected.len();
        writer
            .write_all(&value)
            .expect("append across a verification-unit boundary");
        writer.sync_all().expect("fsync boundary-crossing append");
        expected.extend_from_slice(&value);
        assert_range(&shared_reader, offset, &value);
        assert_length_contract(
            fixture,
            &library,
            "length-changing.bin",
            &path,
            &writer,
            &shared_reader,
            &expected,
            "boundary append",
        );
    }
    assert_staging_below(&staging, LARGE_STAGING_LIMIT, "boundary append");

    let shrunk = expected.len() - RANGE_BYTES;
    writer
        .set_len(shrunk as u64)
        .expect("shrink through the active descriptor");
    writer.sync_all().expect("fsync descriptor shrink");
    expected.truncate(shrunk);
    assert_length_contract(
        fixture,
        &library,
        "length-changing.bin",
        &path,
        &writer,
        &shared_reader,
        &expected,
        "descriptor shrink",
    );

    let zero_extended = expected.len() + 2 * RANGE_BYTES;
    writer
        .set_len(zero_extended as u64)
        .expect("zero-extend through the active descriptor");
    writer.sync_all().expect("fsync zero extension");
    expected.resize(zero_extended, 0);
    assert_range(&shared_reader, shrunk, &vec![0; 2 * RANGE_BYTES]);
    assert_length_contract(
        fixture,
        &library,
        "length-changing.bin",
        &path,
        &writer,
        &shared_reader,
        &expected,
        "descriptor zero extension",
    );

    let shrink_extend_shrunk = expected.len() - RANGE_BYTES;
    let shrink_extend_extended = expected.len() + RANGE_BYTES;
    writer
        .set_len(shrink_extend_shrunk as u64)
        .expect("shrink before extension");
    writer
        .set_len(shrink_extend_extended as u64)
        .expect("zero-extend after shrink");
    writer.sync_all().expect("fsync shrink-then-extend");
    expected.truncate(shrink_extend_shrunk);
    expected.resize(shrink_extend_extended, 0);
    assert_range(
        &shared_reader,
        shrink_extend_shrunk,
        &vec![0; 2 * RANGE_BYTES],
    );
    assert_length_contract(
        fixture,
        &library,
        "length-changing.bin",
        &path,
        &writer,
        &shared_reader,
        &expected,
        "descriptor shrink-then-extend",
    );
    assert_staging_below(&staging, LARGE_STAGING_LIMIT, "length-changing final");

    drop(writer);
    drop(shared_reader);
    mounted.unmount();

    let restarted_mountpoint = fixture
        .temp
        .path()
        .join(format!("mnt-length-changing-restart-{suffix}"));
    fs::create_dir(&restarted_mountpoint).expect("create length-changing restart mountpoint");
    let mut restarted = fixture.mount(&config, &restarted_mountpoint);
    let restarted_path = restarted.mountpoint().join("length-changing.bin");
    assert_eq!(
        fs::read(&restarted_path).expect("read final length-changing file after remount"),
        expected,
        "remount did not preserve the final length-changing bytes and zeros"
    );
    restarted.unmount();
}

#[test]
#[ignore = "requires /dev/fuse and isolated S3; run with cargo x test-linux-fuse"]
fn fuse_length_changing_dirty_recovery_both_publications() {
    for publication in [Publication::Object, Publication::Service] {
        length_changing_dirty_recovery(&Fixture::new(publication));
    }
}

fn length_changing_dirty_recovery(fixture: &Fixture) {
    let suffix = fixture.publication().as_str();
    let library = fixture.volume_config(
        &format!("lib-length-recovery-{suffix}"),
        Frontend::Library,
        false,
    );
    let mut expected = payload(UNIT_BYTES + 123, 0x6b);
    fixture.library_seed(&library, "length-recovery.bin", &expected);
    let config = fixture.volume_config(
        &format!("stage-length-recovery-{suffix}"),
        Frontend::Fuse,
        false,
    );
    let staging = fixture
        .temp
        .path()
        .join(format!("stage-length-recovery-{suffix}"));
    let mountpoint = fixture
        .temp
        .path()
        .join(format!("mnt-length-recovery-{suffix}"));
    fs::create_dir(&mountpoint).expect("create length recovery mountpoint");
    let mut mounted = fixture.mount(&config, &mountpoint);
    let path = mounted.mountpoint().join("length-recovery.bin");
    let reader = File::open(&path).expect("open recovery reader");
    let writer = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .expect("open recovery writer");

    let extended = 2 * UNIT_BYTES + RANGE_BYTES;
    writer
        .set_len(extended as u64)
        .expect("stage dirty zero extension");
    expected.resize(extended, 0);
    let tail_offset = extended - 37;
    let tail = payload(37, 0x7d);
    write_all_at(&writer, &tail, tail_offset as u64);
    expected[tail_offset..].copy_from_slice(&tail);
    assert_shared_lengths(
        &path,
        &writer,
        &reader,
        &expected,
        "dirty length-changing recovery staging",
    );
    assert!(
        fixture.remote_read(&library, "length-recovery.bin", expected.len()) != expected,
        "dirty staged length changes must not be visible through the independent authority before publish"
    );
    assert_staging_below(&staging, LARGE_STAGING_LIMIT, "dirty recovery staging");

    mounted.kill_daemon_only();
    drop(writer);
    drop(reader);
    mounted.cleanup_after_kill();

    let reopened_mountpoint = fixture
        .temp
        .path()
        .join(format!("mnt-length-recovery-reopen-{suffix}"));
    fs::create_dir(&reopened_mountpoint).expect("create reopened length recovery mountpoint");
    let mut reopened = fixture.mount(&config, &reopened_mountpoint);
    let reopened_path = reopened.mountpoint().join("length-recovery.bin");
    assert_eq!(
        fs::read(&reopened_path).expect("read recovered length-changing staging"),
        expected,
        "remount did not recover the exact dirty length-changing bytes"
    );
    let publish = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&reopened_path)
        .expect("open recovered length-changing handle for publication");
    publish
        .sync_all()
        .expect("recovered length-changing handle publishes");
    drop(publish);
    assert_remote_named(
        fixture,
        &library,
        "length-recovery.bin",
        &expected,
        "recovered length-changing publish",
    );
    reopened.unmount();
}

#[test]
#[ignore = "requires /dev/fuse and isolated S3; run with cargo x test-linux-fuse"]
fn fuse_length_changing_frozen_request_preserves_commit_service() {
    let fixture = Fixture::new(Publication::Service);
    let library = fixture.volume_config("lib-length-frozen", Frontend::Library, false);
    let mut expected = payload(UNIT_BYTES + 123, 0x8f);
    let initial = expected.clone();
    fixture.library_seed(&library, "length-frozen.bin", &expected);
    let staging = fixture.temp.path().join("stage-length-frozen");
    let gate = ServiceGate::new(staging.clone(), fixture.service_address());
    let config = fixture.volume_config_with_service(
        "stage-length-frozen",
        Frontend::Fuse,
        false,
        Some(gate.address()),
    );
    let mountpoint = fixture.temp.path().join("mnt-length-frozen");
    fs::create_dir(&mountpoint).expect("create frozen length mountpoint");
    let mut mounted = fixture.mount(&config, &mountpoint);
    let path = mounted.mountpoint().join("length-frozen.bin");
    let writer = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .expect("open frozen length-changing writer");

    let extended = 2 * UNIT_BYTES + RANGE_BYTES;
    writer
        .set_len(extended as u64)
        .expect("stage length change before frozen publication");
    expected.resize(extended, 0);
    let tail_offset = extended - 61;
    let tail = payload(61, 0xa1);
    write_all_at(&writer, &tail, tail_offset as u64);
    expected[tail_offset..].copy_from_slice(&tail);

    let in_flight = writer.try_clone().expect("duplicate frozen writer");
    let (sender, receiver) = mpsc::channel();
    let caller = thread::spawn(move || {
        let _ = sender.send(in_flight.sync_all());
    });
    let gate_commit = gate.wait_ready(Duration::from_secs(30));
    let (saved_commit, frozen) = fixture.wait_for_frozen(&config);
    assert_eq!(
        gate_commit, saved_commit,
        "the service gate blocked a different commit than the locally saved frozen request"
    );
    println!("length-changing fsync saved frozen commit {saved_commit:?}:\n{frozen}");

    mounted.kill_daemon_only();
    gate.release();
    let outcome = receiver
        .recv_timeout(Duration::from_secs(20))
        .unwrap_or_else(|error| {
            panic!("frozen length-changing fsync did not return after daemon exit: {error}")
        });
    caller
        .join()
        .expect("frozen length-changing caller panicked");
    assert!(
        outcome.is_err(),
        "interrupting the mount daemon must not report remote durability"
    );
    drop(writer);
    mounted.cleanup_after_kill();

    let post_release = fixture.remote_read(&library, "length-frozen.bin", extended);
    assert!(
        post_release == initial || post_release == expected,
        "gate release left a partial remote file: {} bytes",
        post_release.len()
    );

    let (frozen_after_kill, status) = fixture.wait_for_frozen(&config);
    assert_eq!(
        frozen_after_kill, saved_commit,
        "staging recovery changed the saved frozen identity"
    );
    println!("recovered frozen length-changing commit {saved_commit:?}:\n{status}");

    let reopened_mountpoint = fixture.temp.path().join("mnt-length-frozen-reopen");
    fs::create_dir(&reopened_mountpoint).expect("create frozen length reopen mountpoint");
    let mut reopened = fixture.mount(&config, &reopened_mountpoint);
    assert_eq!(
        fs::read(reopened.mountpoint().join("length-frozen.bin"))
            .expect("read recovered frozen length file"),
        expected,
        "recovery did not expose the exact staged length-changing file"
    );
    let publish = OpenOptions::new()
        .read(true)
        .write(true)
        .open(reopened.mountpoint().join("length-frozen.bin"))
        .expect("open recovered frozen length handle");
    publish
        .sync_all()
        .expect("recovered frozen length request converges");
    drop(publish);
    assert_remote_named(
        &fixture,
        &library,
        "length-frozen.bin",
        &expected,
        "recovered frozen length request",
    );
    assert_eq!(
        fixture.frozen_commit(&config),
        None,
        "successful convergence left a frozen handle in staging"
    );
    reopened.unmount();
}

fn assert_length_contract(
    fixture: &Fixture,
    library: &Path,
    remote_name: &str,
    path: &Path,
    writer: &File,
    reader: &File,
    expected: &[u8],
    phase: &str,
) {
    assert_shared_lengths(path, writer, reader, expected, phase);
    assert_remote_named(fixture, library, remote_name, expected, phase);
}

fn assert_shared_lengths(path: &Path, writer: &File, reader: &File, expected: &[u8], phase: &str) {
    let length = expected.len() as u64;
    assert_eq!(
        writer.metadata().expect("fstat length writer").len(),
        length,
        "{phase}: writer length"
    );
    assert_eq!(
        reader.metadata().expect("fstat length reader").len(),
        length,
        "{phase}: shared reader length"
    );
    assert_attributes(path, length);
}

fn assert_attributes(path: &Path, length: u64) {
    let metadata =
        fs::metadata(path).unwrap_or_else(|error| panic!("stat {}: {error}", path.display()));
    assert!(
        metadata.is_file(),
        "{} must remain a regular file",
        path.display()
    );
    assert_eq!(metadata.len(), length, "path length");
    assert_eq!(metadata.permissions().mode() & 0o7777, 0o644, "path mode");
    assert_eq!(metadata.uid(), geteuid().as_raw(), "path uid");
    assert_eq!(metadata.gid(), getegid().as_raw(), "path gid");
}

fn assert_attributes_from(file: &File, length: u64) {
    let metadata = file.metadata().expect("fstat active descriptor");
    assert_eq!(metadata.len(), length, "descriptor length");
    assert_eq!(
        metadata.permissions().mode() & 0o7777,
        0o644,
        "descriptor mode"
    );
    assert_eq!(metadata.uid(), geteuid().as_raw(), "descriptor uid");
    assert_eq!(metadata.gid(), getegid().as_raw(), "descriptor gid");
}

fn assert_remote(fixture: &Fixture, library: &Path, expected: &[u8], phase: &str) {
    assert_remote_named(fixture, library, "incremental.bin", expected, phase);
}

fn assert_remote_named(
    fixture: &Fixture,
    library: &Path,
    name: &str,
    expected: &[u8],
    phase: &str,
) {
    assert_eq!(
        fixture.remote_read(library, name, expected.len()),
        expected,
        "{phase}: independent authority bytes"
    );
}

fn assert_range(file: &File, offset: usize, expected: &[u8]) {
    let mut actual = vec![0_u8; expected.len()];
    read_exact_at(file, &mut actual, offset as u64);
    assert_eq!(actual, expected, "active descriptor range at {offset}");
}

fn assert_eof_reads(file: &File, expected: &[u8]) {
    let eof = expected.len() as u64;
    let mut buffer = [0xcc_u8; 32];
    assert_eq!(
        file.read_at(&mut buffer, eof).expect("pread at exact EOF"),
        0,
        "a read at exact EOF must return zero bytes"
    );
    assert_eq!(
        file.read_at(&mut buffer, eof + RANGE_BYTES as u64)
            .expect("pread beyond EOF"),
        0,
        "a read beyond EOF must return zero bytes"
    );
    assert_eq!(
        file.read_at(&mut [], 0).expect("zero-length pread"),
        0,
        "a zero-length read must return zero bytes"
    );
    assert_eq!(
        file.read_at(&mut [], eof + RANGE_BYTES as u64)
            .expect("zero-length pread beyond EOF"),
        0,
        "a zero-length read beyond EOF must return zero bytes"
    );
    let mut crossing = [0xcc_u8; 8];
    let count = file
        .read_at(&mut crossing, eof - 4)
        .expect("pread crossing EOF");
    assert_eq!(count, 4, "a read crossing EOF must stop at EOF");
    assert_eq!(
        &crossing[..count],
        &expected[expected.len() - 4..],
        "a read crossing EOF returned incorrect final bytes"
    );
}

fn assert_staging_bounded(staging: &Path, phase: &str) {
    assert_staging_below(staging, INITIAL_BYTES as u64, phase);
}

fn assert_staging_below(staging: &Path, limit: u64, phase: &str) {
    let bytes = directory_bytes(staging);
    assert!(
        bytes < limit,
        "{phase}: staging allocated {bytes} bytes, at or above the {limit}-byte materialization bound"
    );
}

fn overwrite_expected(expected: &mut Vec<u8>, offset: usize, value: &[u8]) {
    let end = offset + value.len();
    if expected.len() < end {
        expected.resize(end, 0);
    }
    expected[offset..end].copy_from_slice(value);
}

fn read_exact_at(file: &File, mut buffer: &mut [u8], mut offset: u64) {
    while !buffer.is_empty() {
        let count = file.read_at(buffer, offset).expect("pread FUSE file");
        assert_ne!(count, 0, "unexpected EOF at offset {offset}");
        offset += count as u64;
        buffer = &mut buffer[count..];
    }
}

fn write_all_at(file: &File, mut buffer: &[u8], mut offset: u64) {
    while !buffer.is_empty() {
        let count = file.write_at(buffer, offset).expect("pwrite FUSE file");
        assert_ne!(count, 0, "unexpected zero-byte write at offset {offset}");
        offset += count as u64;
        buffer = &buffer[count..];
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
