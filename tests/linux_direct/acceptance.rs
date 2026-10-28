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

//! Real S3/FUSE acceptance for read-only Direct volumes.

use crate::harness::{Fixture, RequestCounter, assert_errno, path_text, run_timeout};
use serde_json::Value;
use std::collections::BTreeSet;
use std::fs::{self, File, OpenOptions};
use std::io::Read;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::time::Duration;

const EACCES: i32 = 13;
const EPERM: i32 = 1;
const EROFS: i32 = 30;

#[test]
#[ignore = "requires isolated S3; run with cargo x test-linux-direct"]
fn direct_raw_s3_harness_preserves_requested_keys() {
    use crate::raw_s3::RawS3;

    let raw = RawS3::from_env();
    let root = format!(
        "direct-raw-{}-{}",
        std::process::id(),
        uuid::Uuid::new_v4().simple()
    );
    let keys = [
        "literal%.txt",
        "../escape",
        "./dot",
        "control\u{7f}",
        "a//b",
        "/leading",
        "trailing/",
        "nul\0byte",
    ];
    for (index, suffix) in keys.iter().enumerate() {
        let key = format!("{root}/{suffix}");
        let bytes = format!("raw-key-{index}").into_bytes();
        raw.put(&key, &bytes);
        assert_eq!(
            raw.get(&key).as_deref(),
            Some(bytes.as_slice()),
            "raw S3 did not preserve key {suffix:?}"
        );
    }
    let inventory = raw.inventory(&format!("{root}/"));
    let mut expected = keys
        .iter()
        .map(|suffix| format!("{root}/{suffix}"))
        .collect::<Vec<_>>();
    expected.sort();
    assert_eq!(
        inventory.keys().cloned().collect::<Vec<_>>(),
        expected,
        "raw S3 inventory must report the exact requested keys"
    );
    for (index, suffix) in keys.iter().enumerate() {
        let key = format!("{root}/{suffix}");
        assert_eq!(
            inventory[&key].bytes,
            format!("raw-key-{index}").into_bytes()
        );
        raw.delete(&key);
    }
    assert!(
        raw.inventory(&format!("{root}/")).is_empty(),
        "raw S3 harness cleanup left objects behind"
    );
}

fn effective_capabilities(document: &[u8]) -> BTreeSet<String> {
    let value: Value = serde_json::from_slice(document).expect("capabilities JSON");
    value["effective"]
        .as_array()
        .expect("effective capability array")
        .iter()
        .map(|value| value.as_str().expect("capability name").to_owned())
        .collect()
}

fn assert_direct_read_only_capabilities(capabilities: &BTreeSet<String>) {
    for required in ["read", "list"] {
        assert!(
            capabilities.contains(required),
            "Direct read-only capabilities omitted {required:?}: {capabilities:?}"
        );
    }
    for forbidden in [
        "write",
        "append",
        "truncate",
        "atomic-rename",
        "remote-fsync",
        "recoverable-staging",
    ] {
        assert!(
            !capabilities.contains(forbidden),
            "Direct read-only capabilities unexpectedly claimed {forbidden:?}: {capabilities:?}"
        );
    }
}

fn local_paths(root: &Path) -> Vec<String> {
    let mut paths = Vec::new();
    let mut pending = vec![root.to_owned()];
    while let Some(path) = pending.pop() {
        for entry in fs::read_dir(&path)
            .unwrap_or_else(|error| panic!("read test directory {}: {error}", path.display()))
        {
            let entry = entry.expect("read test directory entry");
            let path = entry.path();
            paths.push(
                path.strip_prefix(root)
                    .expect("test path is below root")
                    .display()
                    .to_string(),
            );
            if entry.file_type().expect("test path type").is_dir() {
                pending.push(path);
            }
        }
    }
    paths.sort();
    paths
}

#[test]
#[ignore = "requires /dev/fuse and isolated S3; run with cargo x test-linux-direct"]
fn direct_read_only_default_admission_doctor_capabilities_and_fuse() {
    let fixture = Fixture::new();
    fixture.write_raw("hello.txt", b"hello Direct");
    fixture.write_raw("nested/file.txt", b"nested Direct");
    let initial_inventory = fixture.inventory();
    let config = fixture.config("direct-default", "fuse", None, None);
    let local_before = local_paths(fixture.temp.path());

    let capabilities = fixture.yy_ok(&["--volume", path_text(&config), "capabilities"]);
    assert_direct_read_only_capabilities(&effective_capabilities(&capabilities.stdout));

    let doctor = fixture.yy_ok(&["--volume", path_text(&config), "doctor", "--json"]);
    let doctor: Value = serde_json::from_slice(&doctor.stdout).expect("doctor JSON");
    assert_eq!(doctor["schema"], "yinyang.operational-health.v1");
    assert_eq!(doctor["status"], "pass", "{doctor:#}");
    assert_eq!(doctor["volume"]["access"], "mount", "{doctor:#}");
    assert_eq!(doctor["volume"]["frontend"], "fuse", "{doctor:#}");
    assert_eq!(
        local_paths(fixture.temp.path()),
        local_before,
        "Direct capabilities and doctor must not create staging or authority state"
    );
    assert_eq!(
        fixture.inventory(),
        initial_inventory,
        "Direct capabilities and doctor must not mutate raw storage"
    );

    let mountpoint = fixture.path("mnt");
    fs::create_dir(&mountpoint).expect("create Direct mountpoint");
    let mounted = fixture.spawn_mount(&config, &mountpoint, "fuse");
    assert_eq!(
        fs::read(mounted.mountpoint().join("hello.txt")).expect("read raw Direct file"),
        b"hello Direct"
    );
    assert_eq!(
        fs::read(mounted.mountpoint().join("nested/file.txt"))
            .expect("read nested raw Direct file"),
        b"nested Direct"
    );

    let write_existing = OpenOptions::new()
        .write(true)
        .open(mounted.mountpoint().join("hello.txt"));
    let error = write_existing.expect_err("Direct default must reject a writable open");
    assert_errno(&error, &[EROFS, EACCES, EPERM], "Direct writable open");
    let error = fs::write(mounted.mountpoint().join("created.txt"), b"must not exist")
        .expect_err("Direct default must reject create");
    assert_errno(&error, &[EROFS, EACCES, EPERM], "Direct create");
    let error = fs::create_dir(mounted.mountpoint().join("created-dir"))
        .expect_err("Direct default must reject mkdir");
    assert_errno(&error, &[EROFS, EACCES, EPERM], "Direct mkdir");
    let error = fs::rename(
        mounted.mountpoint().join("hello.txt"),
        mounted.mountpoint().join("moved.txt"),
    )
    .expect_err("Direct default must reject rename");
    assert_errno(&error, &[EROFS, EACCES, EPERM], "Direct rename");

    mounted.unmount();
    assert_eq!(
        fixture.inventory(),
        initial_inventory,
        "Direct list/read and rejected mutations must leave every backend object unchanged"
    );
    assert!(
        initial_inventory
            .keys()
            .all(|key| !key.starts_with(".yinyang/")),
        "Direct acceptance must start without authoritative YinYang metadata"
    );
    assert!(
        fixture
            .inventory()
            .keys()
            .all(|key| !key.starts_with(".yinyang/")),
        "Direct reads must never create authoritative YinYang metadata"
    );

    let explicit_true = fixture.config("direct-true", "fuse", Some(true), None);
    let capabilities = fixture.yy_ok(&["--volume", path_text(&explicit_true), "capabilities"]);
    assert_direct_read_only_capabilities(&effective_capabilities(&capabilities.stdout));

    let explicit_false = fixture.config("direct-false", "fuse", Some(false), None);
    let requests = RequestCounter::start();
    let rejected_mountpoint = fixture.path("mnt-false");
    fs::create_dir(&rejected_mountpoint).expect("create rejected Direct mountpoint");
    let mut rejected_command = fixture.yy_command(&[
        "--volume",
        path_text(&explicit_false),
        "mount",
        path_text(&rejected_mountpoint),
    ]);
    rejected_command.env("YINYANG_S3_ENDPOINT", requests.endpoint());
    let rejected = run_timeout(rejected_command, Duration::from_secs(30));
    assert!(
        !rejected.status.success(),
        "Direct writable admission must fail before mounting"
    );
    let diagnostic = rejected.text().to_ascii_lowercase();
    assert!(
        diagnostic.contains("read-only")
            || diagnostic.contains("read_only")
            || diagnostic.contains("unimplemented"),
        "Direct writable rejection did not identify its admission boundary:\n{}",
        rejected.text()
    );
    requests.assert_no_requests("Direct writable mount");
    assert_eq!(
        fixture.inventory(),
        initial_inventory,
        "rejected Direct writable admission must not mutate raw storage"
    );
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

fn read_fully_at(file: &File, offset: u64, length: usize) -> std::io::Result<Vec<u8>> {
    let mut output = vec![0_u8; length];
    let mut read = 0;
    while read < output.len() {
        let count = file.read_at(&mut output[read..], offset + read as u64)?;
        if count == 0 {
            output.truncate(read);
            break;
        }
        read += count;
    }
    Ok(output)
}

#[test]
#[ignore = "requires /dev/fuse and isolated S3; run with cargo x test-linux-direct"]
fn direct_external_replacement_and_delete_do_not_mix_open_versions() {
    let fixture = Fixture::new();
    let old = payload(192 * 1024 + 31, 0x31);
    let shorter = payload(64 * 1024 + 7, 0x71);
    fixture.write_raw("replace.bin", &old);
    let config = fixture.config("direct-replace", "fuse", Some(true), None);
    let mountpoint = fixture.path("mnt-replace");
    fs::create_dir(&mountpoint).expect("create replacement mountpoint");
    let mounted = fixture.spawn_mount(&config, &mountpoint, "fuse");
    let path = mounted.mountpoint().join("replace.bin");

    let mut old_fd = File::open(&path).expect("open old Direct version");
    assert!(
        old_fd
            .metadata()
            .expect("fstat old Direct version")
            .is_file(),
        "old Direct fd must expose a regular file identity"
    );
    assert_eq!(
        old_fd.metadata().expect("fstat old Direct version").len(),
        old.len() as u64
    );
    assert_eq!(
        read_fully_at(&old_fd, 0, 4096).expect("read old prefix"),
        old[..4096]
    );

    fixture.write_raw("replace.bin", &shorter);
    mounted.refresh();
    mounted.wait_for_log("refresh", Duration::from_secs(30));
    let mut fresh = File::open(&path).expect("fresh Direct open after SIGHUP");
    assert!(
        fresh
            .metadata()
            .expect("fstat fresh Direct version")
            .is_file(),
        "fresh Direct fd must expose a regular file identity"
    );
    assert_eq!(
        fresh.metadata().expect("fstat fresh Direct version").len(),
        shorter.len() as u64
    );
    let mut fresh_bytes = Vec::new();
    fresh
        .read_to_end(&mut fresh_bytes)
        .expect("read fresh Direct version");
    assert_eq!(fresh_bytes, shorter);

    assert_eq!(
        old_fd
            .metadata()
            .expect("fstat retained old Direct fd")
            .len(),
        old.len() as u64,
        "a replacement must not rebase an existing Direct fd's attributes"
    );
    let tail_offset = (old.len() - 4096) as u64;
    match read_fully_at(&old_fd, tail_offset, 4096) {
        Ok(bytes) => assert_eq!(
            bytes,
            old[old.len() - 4096..],
            "a retained Direct fd may read its pinned version but must never mix replacement bytes"
        ),
        Err(error) => assert_errno(&error, &[5, 116], "old Direct fd after replacement"),
    }
    drop(fresh);
    drop(old_fd);

    fixture.delete_raw("replace.bin");
    mounted.refresh();
    mounted.wait_for_log("refresh", Duration::from_secs(30));
    assert!(
        fs::metadata(&path).is_err(),
        "a fresh lookup after external delete must not serve the removed raw key"
    );
    mounted.unmount();
}

#[test]
#[ignore = "requires /dev/fuse, an isolated true-versioning bucket, and cargo x test-linux-direct"]
fn direct_true_versioning_replacement_keeps_old_fd_bytes_and_attributes() {
    let fixture = Fixture::versioned();
    let old = payload(256 * 1024 + 41, 0x41);
    let shorter = payload(32 * 1024 + 13, 0x13);
    fixture.write_raw("versioned.bin", &old);
    let seeded = fixture.inventory();
    let version = seeded["versioned.bin"]
        .version
        .as_deref()
        .filter(|version| *version != "null")
        .unwrap_or_else(|| {
            panic!(
                "true-versioning Direct acceptance requires versionId, got {:?}",
                seeded["versioned.bin"].version
            )
        })
        .to_owned();

    let config = fixture.config("direct-versioned", "fuse", None, None);
    let mountpoint = fixture.path("mnt-versioned");
    fs::create_dir(&mountpoint).expect("create versioned Direct mountpoint");
    let mounted = fixture.spawn_mount(&config, &mountpoint, "fuse");
    let path = mounted.mountpoint().join("versioned.bin");
    let mut old_fd = File::open(&path).expect("open versioned old Direct fd");
    let old_metadata = old_fd.metadata().expect("fstat versioned old Direct fd");
    assert!(old_metadata.is_file());
    assert_eq!(old_metadata.len(), old.len() as u64);
    assert_eq!(
        read_fully_at(&old_fd, 0, 4096).expect("read versioned old prefix"),
        old[..4096]
    );

    fixture.write_raw("versioned.bin", &shorter);
    let replaced = fixture.inventory();
    assert_ne!(
        replaced["versioned.bin"].version.as_deref(),
        Some(version.as_str()),
        "a real overwrite in a versioned bucket must allocate a new version identity"
    );
    assert_eq!(
        read_fully_at(&old_fd, (old.len() - 4096) as u64, 4096)
            .expect("versioned old range read must retain the pinned version"),
        old[old.len() - 4096..],
        "true object versioning must let an open Direct fd keep reading its exact version"
    );
    let retained = old_fd.metadata().expect("fstat retained versioned fd");
    assert!(retained.is_file());
    assert_eq!(retained.len(), old.len() as u64);

    mounted.refresh();
    mounted.wait_for_log("refresh", Duration::from_secs(30));
    let mut fresh = File::open(&path).expect("fresh versioned Direct open after SIGHUP");
    let fresh_metadata = fresh.metadata().expect("fstat fresh versioned Direct fd");
    assert!(fresh_metadata.is_file());
    assert_eq!(fresh_metadata.len(), shorter.len() as u64);
    let mut fresh_bytes = Vec::new();
    fresh
        .read_to_end(&mut fresh_bytes)
        .expect("read fresh versioned Direct file");
    assert_eq!(fresh_bytes, shorter);
    assert_ne!(
        fresh_metadata.len(),
        old.len() as u64,
        "fresh versioned open must not inherit the old immutable length"
    );

    drop(fresh);
    drop(old_fd);
    mounted.unmount();
    let _ = version;
}

#[test]
#[ignore = "requires /dev/fuse and isolated S3; run with cargo x test-linux-direct"]
fn direct_percent_mapping_preserves_case_unicode_and_reserved_components() {
    let fixture = Fixture::new();
    fixture.write_raw("plain.txt", b"plain");
    fixture.write_raw("literal%.txt", b"percent");
    fixture.write_raw("../escape", b"dotdot");
    fixture.write_raw("./dot", b"dot");
    fixture.write_raw("control\u{7f}", b"control");
    fixture.write_raw("Case.txt", b"uppercase");
    fixture.write_raw("case.txt", b"lowercase");
    fixture.write_raw("caf\u{e9}", b"nfc");
    fixture.write_raw("cafe\u{301}", b"nfd");
    let inventory = fixture.inventory();
    let config = fixture.config("direct-mapping", "fuse", None, None);
    let mountpoint = fixture.path("mnt-mapping");
    fs::create_dir(&mountpoint).expect("create mapping mountpoint");
    let mounted = fixture.spawn_mount(&config, &mountpoint, "fuse");
    let root = mounted.mountpoint();

    assert_eq!(fs::read(root.join("plain.txt")).unwrap(), b"plain");
    assert_eq!(
        fs::read(root.join(find_encoded(root, "literal%.txt"))).unwrap(),
        b"percent"
    );
    assert_eq!(
        fs::read(root.join(find_encoded(root, "..").join("escape"))).unwrap(),
        b"dotdot"
    );
    assert_eq!(
        fs::read(root.join(find_encoded(root, ".").join("dot")).unwrap()),
        b"dot"
    );
    assert_eq!(
        fs::read(root.join(find_encoded(root, "control\u{7f}"))).unwrap(),
        b"control"
    );
    assert_eq!(fs::read(root.join("Case.txt")).unwrap(), b"uppercase");
    assert_eq!(fs::read(root.join("case.txt")).unwrap(), b"lowercase");
    assert_eq!(fs::read(root.join("caf\u{e9}")).unwrap(), b"nfc");
    assert_eq!(fs::read(root.join("cafe\u{301}")).unwrap(), b"nfd");

    mounted.unmount();
    assert_eq!(
        fixture.inventory(),
        inventory,
        "Direct name mapping and reads must not rewrite raw keys"
    );
}

fn find_encoded(directory: &Path, raw_component: &str) -> PathBuf {
    let matches = fs::read_dir(directory)
        .unwrap_or_else(|error| panic!("read encoded Direct directory: {error}"))
        .filter_map(Result::ok)
        .filter_map(|entry| entry.file_name().into_string().ok())
        .filter(|name| percent_decode(name) == raw_component)
        .collect::<Vec<_>>();
    assert_eq!(
        matches.len(),
        1,
        "raw component {raw_component:?} must map to exactly one visible name, got {matches:?}"
    );
    PathBuf::from(matches.into_iter().next().unwrap())
}

fn percent_decode(component: &str) -> String {
    let bytes = component.as_bytes();
    let mut output = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            assert!(
                index + 2 < bytes.len(),
                "Direct name contains a truncated percent escape: {component:?}"
            );
            let high = hex(bytes[index + 1]);
            let low = hex(bytes[index + 2]);
            output.push((high << 4) | low);
            index += 3;
        } else {
            output.push(bytes[index]);
            index += 1;
        }
    }
    String::from_utf8(output).expect("Direct encoded name decodes to UTF-8")
}

fn hex(value: u8) -> u8 {
    match value {
        b'0'..=b'9' => value - b'0',
        b'a'..=b'f' => value - b'a' + 10,
        b'A'..=b'F' => value - b'A' + 10,
        _ => panic!("invalid percent escape"),
    }
}
