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

//! Observation-only space and retention acceptance during dirty FUSE work.

use crate::harness::{Fixture, Frontend, Publication, RemoteObjectIdentity, mount_fs_type};
use nix::fcntl::{FcntlArg, FdFlag, fcntl};
use serde_json::Value;
use std::fs::{self, File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::os::unix::fs::{FileExt, MetadataExt};
use std::path::{Path, PathBuf};

const UNIT_BYTES: usize = 64 * 1024;
const RANGE_BYTES: usize = 4096;
const FILE_BYTES: usize = 2 * UNIT_BYTES + 2 * RANGE_BYTES + 19;

#[test]
#[ignore = "requires /dev/fuse and isolated S3; run with cargo x test-linux-fuse"]
fn fuse_space_and_retention_are_observational_during_dirty_work_both_publications() {
    for publication in [Publication::Object, Publication::Service] {
        space_and_retention_are_observational(&Fixture::new(publication));
    }
}

fn space_and_retention_are_observational(fixture: &Fixture) {
    let suffix = fixture.publication().as_str();
    let library = fixture.volume_config(
        &format!("lib-space-observation-{suffix}"),
        Frontend::Library,
        false,
    );
    let original = payload(FILE_BYTES, 0x49);
    fixture.library_seed(&library, "space.bin", &original);
    let config = fixture.volume_config(
        &format!("stage-space-observation-{suffix}"),
        Frontend::Fuse,
        false,
    );
    let staging = fixture
        .temp
        .path()
        .join(format!("stage-space-observation-{suffix}"));
    let mountpoint = fixture
        .temp
        .path()
        .join(format!("mnt-space-observation-{suffix}"));
    fs::create_dir(&mountpoint).expect("create space observation mountpoint");
    let mut mounted = fixture.mount(&config, &mountpoint);

    let mut dirty = OpenOptions::new()
        .read(true)
        .write(true)
        .open(mounted.mountpoint().join("space.bin"))
        .expect("open dirty FUSE descriptor");
    let flags = fcntl(&dirty, FcntlArg::F_GETFD).expect("read descriptor close-on-exec flag");
    assert!(
        flags.contains(FdFlag::FD_CLOEXEC),
        "the dirty descriptor must be close-on-exec before spawning observation commands"
    );
    let mut dirty_bytes = original.clone();
    let write_offset = UNIT_BYTES - RANGE_BYTES / 2;
    let update = payload(RANGE_BYTES, 0x5d);
    dirty_bytes[write_offset..write_offset + update.len()].copy_from_slice(&update);
    dirty
        .seek(SeekFrom::Start(0))
        .expect("rewind dirty descriptor");
    dirty
        .write_all(&dirty_bytes)
        .expect("write a dirty full descriptor view without fsync");
    assert_eq!(
        fixture.remote_read(&library, "space.bin", original.len()),
        original,
        "an un-fsynced descriptor write must not reach the authority"
    );

    let config_before = fs::read(&config).expect("read volume config before observation");
    let remote_before = fixture.remote_object_identities();
    let staging_before = tree_identity(&staging);
    assert_no_effects(
        fixture,
        &library,
        &config,
        &staging,
        &original,
        &config_before,
        &remote_before,
        &staging_before,
        &mut mounted,
    );

    let report = fixture.yy(&[
        "--volume",
        config.to_str().expect("UTF-8 volume config"),
        "space",
        "report",
        "--json",
    ]);
    report.success_or("space report during dirty FUSE work");
    assert_schema(&report.stdout, "yinyang.managed-space.report.v1");
    assert_no_effects(
        fixture,
        &library,
        &config,
        &staging,
        &original,
        &config_before,
        &remote_before,
        &staging_before,
        &mut mounted,
    );

    let plan = fixture.yy(&[
        "--volume",
        config.to_str().expect("UTF-8 volume config"),
        "space",
        "retention",
        "plan",
        "--keep-last",
        "1",
        "--json",
    ]);
    plan.success_or("retention plan during dirty FUSE work");
    assert_schema(&plan.stdout, "yinyang.managed-retention.plan.v1");
    assert_no_effects(
        fixture,
        &library,
        &config,
        &staging,
        &original,
        &config_before,
        &remote_before,
        &staging_before,
        &mut mounted,
    );

    let mut staged = vec![0_u8; dirty_bytes.len()];
    read_exact_at(&dirty, &mut staged, 0);
    assert_eq!(
        staged, dirty_bytes,
        "space observation changed the active dirty descriptor's staged bytes"
    );
    dirty
        .sync_all()
        .expect("the dirty descriptor still publishes after observation");
    assert_eq!(
        fixture.remote_read(&library, "space.bin", dirty_bytes.len()),
        dirty_bytes,
        "post-observation fsync did not publish the retained dirty bytes"
    );
    drop(dirty);
    mounted.unmount();
}

#[allow(clippy::too_many_arguments)]
fn assert_no_effects(
    fixture: &Fixture,
    library: &Path,
    config: &Path,
    staging: &Path,
    remote_original: &[u8],
    config_before: &[u8],
    remote_before: &[RemoteObjectIdentity],
    staging_before: &TreeIdentity,
    mounted: &mut crate::harness::Mounted,
) {
    assert_eq!(
        fs::read(config).expect("read volume config after observation"),
        config_before,
        "space observation rewrote the volume configuration"
    );
    assert_eq!(
        fixture.remote_object_identities(),
        remote_before,
        "space observation mutated remote keys, lengths or etags"
    );
    assert_eq!(
        fixture.remote_read(library, "space.bin", remote_original.len()),
        remote_original,
        "space observation published dirty content"
    );
    assert_eq!(
        tree_identity(staging),
        *staging_before,
        "space observation changed staging files or authoritative bytes"
    );
    assert_eq!(
        mount_fs_type(mounted.mountpoint()).as_deref(),
        Some("fuse.yinyang"),
        "space observation disturbed the live FUSE mount"
    );
}

fn assert_schema(output: &[u8], expected: &str) {
    let document: Value =
        serde_json::from_slice(output).expect("space command returned valid JSON");
    assert_eq!(
        document.get("schema").and_then(Value::as_str),
        Some(expected),
        "unexpected space JSON schema: {document}"
    );
}

#[derive(Debug, Eq, PartialEq)]
struct TreeIdentity {
    entries: Vec<TreeEntry>,
}

#[derive(Debug, Eq, PartialEq)]
enum TreeEntry {
    Directory {
        path: PathBuf,
        mode: u32,
    },
    File {
        path: PathBuf,
        mode: u32,
        length: u64,
        digest: String,
    },
}

fn tree_identity(root: &Path) -> TreeIdentity {
    fn walk(root: &Path, path: &Path, entries: &mut Vec<TreeEntry>) {
        let metadata = fs::symlink_metadata(path)
            .unwrap_or_else(|error| panic!("stat {}: {error}", path.display()));
        let relative = path
            .strip_prefix(root)
            .expect("tree entry below root")
            .to_path_buf();
        let mode = metadata.mode() & 0o7777;
        if metadata.is_dir() {
            entries.push(TreeEntry::Directory {
                path: relative,
                mode,
            });
            let mut children = fs::read_dir(path)
                .unwrap_or_else(|error| panic!("read {}: {error}", path.display()))
                .map(|entry| entry.expect("read directory entry").path())
                .collect::<Vec<_>>();
            children.sort();
            for child in children {
                walk(root, &child, entries);
            }
        } else if metadata.is_file() {
            if path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.ends_with("-shm"))
            {
                // SQLite may update its derived shared-memory file during a
                // read-only inspection. The database and WAL remain the
                // authoritative byte comparisons.
                return;
            }
            let bytes =
                fs::read(path).unwrap_or_else(|error| panic!("read {}: {error}", path.display()));
            entries.push(TreeEntry::File {
                path: relative,
                mode,
                length: metadata.len(),
                digest: blake3::hash(&bytes).to_hex().to_string(),
            });
        }
    }

    let mut entries = Vec::new();
    if root.exists() {
        walk(root, root, &mut entries);
    }
    entries.sort_by(|left, right| entry_path(left).cmp(entry_path(right)));
    TreeIdentity { entries }
}

fn entry_path(entry: &TreeEntry) -> &Path {
    match entry {
        TreeEntry::Directory { path, .. } | TreeEntry::File { path, .. } => path,
    }
}

fn read_exact_at(file: &File, mut buffer: &mut [u8], mut offset: u64) {
    while !buffer.is_empty() {
        let count = file
            .read_at(buffer, offset)
            .expect("pread dirty descriptor");
        assert_ne!(count, 0, "unexpected EOF at offset {offset}");
        offset += count as u64;
        buffer = &mut buffer[count..];
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
