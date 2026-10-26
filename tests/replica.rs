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

#![cfg(target_os = "linux")]

use rustix::fs::{CWD, RenameFlags, renameat_with};
use std::fs;
use std::io::Write as _;
use std::os::unix::fs::FileExt as _;
use yinyang::core::NodeId;
use yinyang::replica::{ErrorKind, Replica};

fn filesystem(byte: u8) -> NodeId {
    NodeId::from_bytes([byte; 16])
}

fn setup() -> (tempfile::TempDir, std::path::PathBuf, Replica) {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("payload");
    let state = temp.path().join("state");
    let replica = Replica::init(&state, &root, filesystem(7)).unwrap();
    (temp, root, replica)
}

#[test]
fn binding_rejects_root_and_filesystem_substitution() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("payload");
    let state = temp.path().join("state");
    let replica = Replica::init(&state, &root, filesystem(1)).unwrap();
    drop(replica);

    let error = Replica::open(&state, filesystem(2)).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Invalid);

    let nested = root.join(".sync-state");
    let error = Replica::init(&nested, &root, filesystem(1)).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Invalid);
}

#[test]
fn state_lease_rejects_concurrent_processes_in_one_process_test() {
    let (_temp, _root, replica) = setup();
    let state = replica.state().to_owned();
    let error = Replica::open(&state, filesystem(7)).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Busy);
    assert_eq!(
        Replica::inspect(&state).unwrap().root,
        replica.root().to_path_buf()
    );
}

#[test]
fn scan_hashes_content_and_rejects_symlinks() {
    let (_temp, root, replica) = setup();
    fs::create_dir(root.join("directory")).unwrap();
    fs::write(root.join("directory/file"), b"content").unwrap();

    let snapshot = replica.scan().unwrap();
    assert_eq!(snapshot.len(), 3);
    let file = snapshot.get("directory/file").unwrap();
    assert!(matches!(
        file.kind,
        yinyang::replica::LocalKind::File { length: 7, .. }
    ));

    std::os::unix::fs::symlink("directory", root.join("link")).unwrap();
    let error = replica.scan().unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Unsupported);
}

#[test]
fn scan_rejects_hard_links_with_the_existing_validation_path() {
    let (_temp, root, replica) = setup();
    fs::write(root.join("file"), b"content").unwrap();
    fs::hard_link(root.join("file"), root.join("linked")).unwrap();
    let error = replica.scan().unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Unsupported);
    assert!(error.to_string().contains("hard-linked"));
}

#[test]
fn exchange_retains_old_inode_for_open_writer() {
    let (temp, root, mut replica) = setup();
    fs::write(root.join("file"), b"old").unwrap();
    let snapshot = replica.scan().unwrap();
    let expected = snapshot.get("file").unwrap();
    let mut writer = fs::OpenOptions::new()
        .write(true)
        .open(root.join("file"))
        .unwrap();

    let outcome = replica
        .install_bytes("file", expected, false, None, b"remote")
        .unwrap();
    writer.write_at(b"-late", 3).unwrap();
    writer.sync_all().unwrap();
    drop(writer);

    assert_eq!(fs::read(root.join("file")).unwrap(), b"remote");
    let status = replica.status().unwrap();
    let artifact = status
        .artifacts
        .iter()
        .find(|artifact| artifact.id == outcome.artifact.id)
        .unwrap();
    assert_eq!(artifact.phase, "conflict");
    let mut retained = Vec::new();
    replica
        .export_artifact(&outcome.artifact.id, &mut retained)
        .unwrap();
    assert_eq!(retained, b"old-late");

    // The temporary name never remains in the synchronized payload.
    assert_eq!(fs::read_dir(&root).unwrap().count(), 1);
    assert!(temp.path().join("state/quarantine").exists());
}

#[test]
fn concurrent_in_place_edit_is_restored_atomically() {
    let (_temp, root, mut replica) = setup();
    fs::write(root.join("file"), b"old").unwrap();
    let snapshot = replica.scan().unwrap();
    let expected = snapshot.get("file").unwrap();
    let mut install = replica
        .prepare_install("file", expected, false, None)
        .unwrap();
    install.write_all(b"remote").unwrap();
    let mut writer = fs::OpenOptions::new()
        .write(true)
        .open(root.join("file"))
        .unwrap();
    writer.write_at(b"busy", 0).unwrap();

    let error = install.finish().unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Conflict);
    assert_eq!(fs::read(root.join("file")).unwrap(), b"busy");
    writer.write_at(b"-later", 4).unwrap();
    writer.sync_all().unwrap();
    drop(writer);
    assert_eq!(fs::read(root.join("file")).unwrap(), b"busy-later");
}

#[test]
fn restart_classifies_exchange_written_before_phase_update() {
    let (_temp, root, mut replica) = setup();
    fs::write(root.join("file"), b"old").unwrap();
    let snapshot = replica.scan().unwrap();
    let expected = snapshot.get("file").unwrap();
    let mut install = replica
        .prepare_install("file", expected, false, None)
        .unwrap();
    install.write_all(b"remote").unwrap();
    drop(install);

    let temporary = fs::read_dir(&root)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| {
            path.file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with(".yy-install-")
        })
        .unwrap();
    let target = root.join("file");
    renameat_with(
        CWD,
        temporary.as_path(),
        CWD,
        target.as_path(),
        RenameFlags::EXCHANGE,
    )
    .unwrap();
    let state = replica.state().to_owned();
    drop(replica);

    let mut replica = Replica::open(&state, filesystem(7)).unwrap();
    assert_eq!(fs::read(&target).unwrap(), b"remote");
    let artifact = replica
        .status()
        .unwrap()
        .artifacts
        .into_iter()
        .find(|artifact| artifact.origin == "file")
        .unwrap();
    let mut retained = Vec::new();
    replica
        .export_artifact(&artifact.id, &mut retained)
        .unwrap();
    assert_eq!(retained, b"old");
}

#[test]
fn parent_symlink_substitution_is_not_followed() {
    let (temp, root, mut replica) = setup();
    let parent = root.join("parent");
    let moved = root.join("moved");
    let outside = temp.path().join("outside");
    fs::create_dir(&parent).unwrap();
    fs::create_dir(&outside).unwrap();
    fs::write(parent.join("file"), b"old").unwrap();
    let snapshot = replica.scan().unwrap();
    let expected = snapshot.get("parent/file").unwrap();
    let mut install = replica
        .prepare_install("parent/file", expected, false, None)
        .unwrap();
    install.write_all(b"remote").unwrap();

    fs::rename(&parent, &moved).unwrap();
    std::os::unix::fs::symlink(&outside, &parent).unwrap();

    let error = install.finish().unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Conflict);
    assert_eq!(fs::read(moved.join("file")).unwrap(), b"old");
    assert!(fs::read_dir(&outside).unwrap().next().is_none());
}

#[test]
fn remote_deletion_quarantines_late_children_without_recursive_delete() {
    let (_temp, root, mut replica) = setup();
    let directory = root.join("directory");
    fs::create_dir(&directory).unwrap();
    fs::write(directory.join("known"), b"known").unwrap();
    let snapshot = replica.scan().unwrap();

    fs::write(directory.join("late"), b"late bytes").unwrap();
    let artifact = replica
        .quarantine("directory", &snapshot, "remote directory deletion", None)
        .unwrap();

    assert_eq!(artifact.phase, "conflict");
    assert!(!directory.exists());
    let retained = replica.artifact_path(&artifact.id).unwrap();
    assert_eq!(fs::read(retained.join("known")).unwrap(), b"known");
    assert_eq!(fs::read(retained.join("late")).unwrap(), b"late bytes");
}
