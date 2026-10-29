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

use crate::harness::{Fixture, Frontend, Publication};
use nix::unistd::{getegid, geteuid, truncate};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::fs::{FileExt, MetadataExt, PermissionsExt};
use std::path::Path;

const UNIT_BYTES: usize = 64 * 1024;
const RANGE_BYTES: usize = 4096;
const INITIAL_BYTES: usize = 4 * UNIT_BYTES + 3 * RANGE_BYTES + 73;

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
    fs::create_dir(&mountpoint).expect("create incremental contract mountpoint");
    let mut mounted = fixture.mount(&config, &mountpoint);
    let path = mounted.mountpoint().join("incremental.bin");

    assert_attributes(&path, expected.len() as u64);
    let mut early_reader = File::open(&path).expect("open descriptor before sparse writes");
    assert_attributes_from(&early_reader, expected.len() as u64);
    let mut first_writer = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .expect("open first sparse writer");

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

    let mut second_writer = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .expect("open independent sparse writer");
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

    let shrunk = 3 * UNIT_BYTES + 23;
    first_writer
        .set_len(shrunk as u64)
        .expect("shrink through an active descriptor");
    first_writer.sync_all().expect("fsync shrink");
    expected.truncate(shrunk);
    assert_remote(fixture, &library, &expected, "descriptor shrink fsync");
    assert_attributes(&path, expected.len() as u64);

    let extended = 3 * UNIT_BYTES + 2 * RANGE_BYTES + 9;
    first_writer
        .set_len(extended as u64)
        .expect("extend with a sparse zero gap");
    first_writer.sync_all().expect("fsync zero extension");
    expected.resize(extended, 0);
    assert_remote(fixture, &library, &expected, "descriptor extension fsync");

    let eof_offset = extended - 4;
    let eof_write = payload(17, 0x87);
    overwrite_expected(&mut expected, eof_offset, &eof_write);
    write_all_at(&first_writer, &eof_write, eof_offset as u64);
    first_writer
        .sync_all()
        .expect("fsync an overwrite that crosses the staged EOF");
    assert_remote(fixture, &library, &expected, "EOF-growing fsync");
    assert_range(&early_reader, eof_offset, &eof_write);

    let path_shrunk = 2 * UNIT_BYTES + 31;
    truncate(path.as_path(), path_shrunk as u64).expect("path truncate shrink");
    expected.truncate(path_shrunk);
    assert_remote(fixture, &library, &expected, "path shrink");
    assert_attributes(&path, expected.len() as u64);

    let path_extended = path_shrunk + RANGE_BYTES;
    truncate(path.as_path(), path_extended as u64).expect("path truncate zero extension");
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
    assert_eq!(
        fixture.remote_read(library, "incremental.bin", expected.len()),
        expected,
        "{phase}: independent authority bytes"
    );
}

fn assert_range(file: &File, offset: usize, expected: &[u8]) {
    let mut actual = vec![0_u8; expected.len()];
    read_exact_at(file, &mut actual, offset as u64);
    assert_eq!(actual, expected, "active descriptor range at {offset}");
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
