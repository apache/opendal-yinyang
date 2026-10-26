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

//! Real /dev/fuse acceptance scenarios for the YinYang mount adapter.

use crate::harness::{Fixture, Frontend, Publication, assert_errno, sync_all_with_deadline};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::unix::fs::FileExt;
use std::sync::{Arc, Barrier, mpsc};
use std::thread;
use std::time::Duration;

const EACCES: i32 = 13;
const ENOSYS: i32 = 38;
const EPERM: i32 = 1;
const EOPNOTSUPP: i32 = 95;
const EROFS: i32 = 30;

#[test]
#[ignore = "requires /dev/fuse and isolated S3; run with cargo x test-linux-fuse"]
fn fuse_read_only_both_publications() {
    for publication in [Publication::Object, Publication::Service] {
        read_only(&Fixture::new(publication));
    }
}

fn read_only(fixture: &Fixture) {
    let library = fixture.volume_config(
        &format!("lib-readonly-{}", fixture.publication().as_str()),
        Frontend::Library,
        false,
    );
    fixture.library_seed(&library, "readonly.txt", b"read-only bytes");
    let config = fixture.volume_config(
        &format!("stage-readonly-{}", fixture.publication().as_str()),
        Frontend::Fuse,
        true,
    );
    let mountpoint = fixture
        .temp
        .path()
        .join(format!("mnt-readonly-{}", fixture.publication().as_str()));
    fs::create_dir(&mountpoint).expect("create mountpoint");
    let mut mounted = fixture.mount(&config, &mountpoint);

    assert_eq!(
        fs::read(mounted.mountpoint().join("readonly.txt")).expect("read mounted file"),
        b"read-only bytes"
    );
    let write_existing = OpenOptions::new()
        .write(true)
        .open(mounted.mountpoint().join("readonly.txt"));
    assert_errno(
        write_existing.expect_err("read-only volume must reject a writable open"),
        &[EROFS, EACCES, EPERM],
        "read-only writable open",
    );
    let create_new = fs::write(mounted.mountpoint().join("new.txt"), b"x");
    assert_errno(
        create_new.expect_err("read-only volume must reject create"),
        &[EROFS, EACCES, EPERM],
        "read-only create",
    );
    let create_dir = fs::create_dir(mounted.mountpoint().join("new-dir"));
    assert_errno(
        create_dir.expect_err("read-only volume must reject mkdir"),
        &[EROFS, EACCES, EPERM],
        "read-only mkdir",
    );
    let rename = fs::rename(
        mounted.mountpoint().join("readonly.txt"),
        mounted.mountpoint().join("moved.txt"),
    );
    assert_errno(
        rename.expect_err("read-only volume must reject rename"),
        &[EROFS, EACCES, EPERM],
        "read-only rename",
    );
    mounted.unmount();
}

#[test]
#[ignore = "requires /dev/fuse and isolated S3; run with cargo x test-linux-fuse"]
fn fuse_basic_io_both_publications() {
    for publication in [Publication::Object, Publication::Service] {
        basic_io(&Fixture::new(publication));
    }
}

fn basic_io(fixture: &Fixture) {
    let suffix = fixture.publication().as_str();
    let library = fixture.volume_config(&format!("lib-basic-{suffix}"), Frontend::Library, false);
    fixture.library_seed(&library, "basic.txt", b"initial");
    let config = fixture.volume_config(&format!("stage-basic-{suffix}"), Frontend::Fuse, false);
    let mountpoint = fixture.temp.path().join(format!("mnt-basic-{suffix}"));
    fs::create_dir(&mountpoint).expect("create mountpoint");
    let mut mounted = fixture.mount(&config, &mountpoint);
    let root = mounted.mountpoint();

    let mut file = OpenOptions::new()
        .write(true)
        .open(root.join("basic.txt"))
        .expect("open mounted file for write");
    file.write_all(b"hello").expect("write mounted file");
    file.sync_all().expect("remote fsync through FUSE");
    drop(file);
    assert_eq!(
        fixture.remote_read(&library, "basic.txt", 8),
        b"hello",
        "independent authority must observe a successful FUSE fsync"
    );

    let mut append = OpenOptions::new()
        .append(true)
        .open(root.join("basic.txt"))
        .expect("open mounted file for append");
    append.write_all(b"!").expect("append through FUSE");
    append.sync_all().expect("remote fsync after append");
    drop(append);
    assert_eq!(fixture.remote_read(&library, "basic.txt", 8), b"hello!");

    let mut truncate = OpenOptions::new()
        .write(true)
        .open(root.join("basic.txt"))
        .expect("open mounted file for truncate");
    truncate.set_len(3).expect("truncate through FUSE");
    truncate.sync_all().expect("remote fsync after truncate");
    drop(truncate);
    assert_eq!(
        fixture.remote_read(&library, "basic.txt", 8),
        vec![b'h', b'e', b'l']
    );

    let mut created = File::create(root.join("created.txt")).expect("create through FUSE");
    created.write_all(b"mount-created").expect("write new file");
    created.sync_all().expect("remote fsync for new file");
    drop(created);
    assert_eq!(
        fixture.remote_read(&library, "created.txt", 32),
        b"mount-created"
    );

    let mut first = File::create(root.join("multi-fd.txt")).expect("create multi-fd file");
    first.write_all(b"first").expect("write multi-fd file");
    first.sync_all().expect("remote fsync multi-fd file");
    drop(first);
    let mut reader = File::open(root.join("multi-fd.txt")).expect("open multi-fd reader");
    let mut writer = OpenOptions::new()
        .write(true)
        .open(root.join("multi-fd.txt"))
        .expect("open multi-fd writer");
    let mut initial = [0u8; 5];
    reader.read_exact(&mut initial).expect("read initial");
    assert_eq!(&initial, b"first");
    writer
        .seek(SeekFrom::Start(0))
        .expect("seek multi-fd writer");
    writer.write_all(b"X").expect("write through second fd");
    drop(writer);
    reader
        .seek(SeekFrom::Start(0))
        .expect("rewind multi-fd reader");
    let mut updated = String::new();
    reader
        .read_to_string(&mut updated)
        .expect("read updated bytes");
    assert_eq!(
        updated, "Xirst",
        "an open reader must observe accepted writes"
    );
    mounted.unmount();
}

#[test]
#[ignore = "requires /dev/fuse and isolated S3; run with cargo x test-linux-fuse"]
fn fuse_rename_replace_and_retained_unlink_both_publications() {
    for publication in [Publication::Object, Publication::Service] {
        rename_and_unlink(&Fixture::new(publication));
    }
}

fn rename_and_unlink(fixture: &Fixture) {
    let suffix = fixture.publication().as_str();
    let library = fixture.volume_config(&format!("lib-rename-{suffix}"), Frontend::Library, false);
    let config = fixture.volume_config(&format!("stage-rename-{suffix}"), Frontend::Fuse, false);
    let mountpoint = fixture.temp.path().join(format!("mnt-rename-{suffix}"));
    fs::create_dir(&mountpoint).expect("create mountpoint");
    let mut mounted = fixture.mount(&config, &mountpoint);
    let root = mounted.mountpoint();

    let mut target = File::create(root.join("saved.txt")).expect("create rename target");
    target.write_all(b"old").expect("write old target");
    target.sync_all().expect("fsync rename target");
    drop(target);
    let mut temp = File::create(root.join("saved.txt.new")).expect("create rename source");
    temp.write_all(b"new").expect("write rename source");
    temp.sync_all().expect("fsync rename source");
    drop(temp);
    fs::rename(root.join("saved.txt.new"), root.join("saved.txt"))
        .expect("safe-save rename must atomically replace the target");
    assert!(!root.join("saved.txt.new").exists());
    assert_eq!(
        fs::read(root.join("saved.txt")).expect("read replaced target"),
        b"new"
    );
    assert_eq!(
        fixture.remote_read(&library, "saved.txt", 8),
        b"new",
        "independent authority must observe the rename replacement"
    );

    let mut kept = File::create(root.join("retained.txt")).expect("create retained file");
    kept.write_all(b"keep").expect("write retained file");
    kept.sync_all().expect("fsync retained file");
    drop(kept);
    let mut open = File::open(root.join("retained.txt")).expect("open file before unlink");
    fs::remove_file(root.join("retained.txt")).expect("unlink through FUSE");
    let mut bytes = Vec::new();
    open.read_to_end(&mut bytes)
        .expect("read through an unlinked descriptor");
    assert_eq!(bytes, b"keep", "retained unlink reads must keep bytes");
    drop(open);
    assert!(!root.join("retained.txt").exists());
    mounted.unmount();
}

#[test]
#[ignore = "requires /dev/fuse and isolated S3; run with cargo x test-linux-fuse"]
fn fuse_unsupported_operations_both_publications() {
    for publication in [Publication::Object, Publication::Service] {
        unsupported_operations(&Fixture::new(publication));
    }
}

fn unsupported_operations(fixture: &Fixture) {
    let suffix = fixture.publication().as_str();
    let config = fixture.volume_config(
        &format!("stage-unsupported-{suffix}"),
        Frontend::Fuse,
        false,
    );
    let mountpoint = fixture
        .temp
        .path()
        .join(format!("mnt-unsupported-{suffix}"));
    fs::create_dir(&mountpoint).expect("create mountpoint");
    let mut mounted = fixture.mount(&config, &mountpoint);
    File::create(mounted.mountpoint().join("base.txt"))
        .expect("create base file")
        .sync_all()
        .expect("fsync base file");

    let symlink = std::os::unix::fs::symlink("base.txt", mounted.mountpoint().join("link"));
    assert_errno(
        symlink.expect_err("symlinks are not part of the YinYang contract and must be explicit"),
        &[ENOSYS, EOPNOTSUPP, EPERM, EACCES],
        "symlink",
    );
    let hardlink = fs::hard_link(
        mounted.mountpoint().join("base.txt"),
        mounted.mountpoint().join("hard"),
    );
    assert_errno(
        hardlink.expect_err("hard links are not part of the YinYang contract and must be explicit"),
        &[ENOSYS, EOPNOTSUPP, EPERM, EACCES],
        "hard link",
    );
    mounted.unmount();
}

#[test]
#[ignore = "requires /dev/fuse and isolated S3; run with cargo x test-linux-fuse"]
fn fuse_external_changes_and_dirty_refresh_both_publications() {
    for publication in [Publication::Object, Publication::Service] {
        external_changes(&Fixture::new(publication));
    }
}

fn external_changes(fixture: &Fixture) {
    let suffix = fixture.publication().as_str();
    let library =
        fixture.volume_config(&format!("lib-external-{suffix}"), Frontend::Library, false);
    fixture.library_seed(&library, "external.txt", b"AAAA");
    fixture.library_seed(&library, "shrink.txt", b"0123456789");
    fixture.library_seed(&library, "dirty.txt", b"BBBBBBBB");
    let config = fixture.volume_config(&format!("stage-external-{suffix}"), Frontend::Fuse, false);
    let mountpoint = fixture.temp.path().join(format!("mnt-external-{suffix}"));
    fs::create_dir(&mountpoint).expect("create mountpoint");
    let mut mounted = fixture.mount(&config, &mountpoint);

    let mut old = File::open(mounted.mountpoint().join("external.txt"))
        .expect("open descriptor before external growth");
    let mut initial = Vec::new();
    old.read_to_end(&mut initial).expect("read original bytes");
    assert_eq!(initial, b"AAAA");
    assert_eq!(old.metadata().expect("stat original").len(), 4);
    fixture.remote_write(&library, "external.txt", b"BBBBBBBB");
    assert_eq!(
        fixture.remote_read(&library, "external.txt", 16),
        b"BBBBBBBB",
        "independent external growth must be authoritative"
    );
    assert_eq!(
        old.metadata().expect("stat old descriptor").len(),
        4,
        "external growth must not resize a descriptor that was opened before the change"
    );
    let mut beyond_old_eof = [0u8; 4];
    let count = old
        .read_at(&mut beyond_old_eof, 4)
        .expect("pread beyond the old EOF");
    assert_eq!(
        count, 0,
        "pread past the old EOF must not leak bytes from the externally grown file"
    );
    drop(old);

    let dirty_path = mounted.mountpoint().join("dirty.txt");
    let mut dirty = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&dirty_path)
        .expect("open file for dirty write");
    dirty.write_all(b"LOCAL").expect("stage dirty write");
    fixture.remote_write(&library, "dirty.txt", b"ZZZZZZZZZZ");
    dirty.seek(SeekFrom::Start(0)).expect("rewind dirty file");
    let mut local = Vec::new();
    dirty
        .read_to_end(&mut local)
        .expect("read dirty staged bytes");
    assert_eq!(
        local, b"LOCALBBB",
        "an external change must not silently rebase a dirty descriptor"
    );
    let conflict = dirty.sync_all();
    assert!(
        conflict.is_err(),
        "fsync of a stale dirty base must report failure instead of claiming remote durability"
    );
    drop(dirty);
    mounted.unmount();

    let config = fixture.volume_config(&format!("stage-external-{suffix}"), Frontend::Fuse, false);
    let mountpoint = fixture
        .temp
        .path()
        .join(format!("mnt-external-reopen-{suffix}"));
    fs::create_dir(&mountpoint).expect("create reopen mountpoint");
    let mut reopen = fixture.mount(&config, &mountpoint);
    let mut old_shrink = File::open(reopen.mountpoint().join("shrink.txt"))
        .expect("open descriptor before external shrink");
    let mut before = Vec::new();
    old_shrink
        .read_to_end(&mut before)
        .expect("read original shrink file");
    assert_eq!(before, b"0123456789");
    assert_eq!(old_shrink.metadata().expect("stat original").len(), 10);
    fixture.remote_truncate(&library, "shrink.txt", 4);
    assert_eq!(
        old_shrink.metadata().expect("stat old descriptor").len(),
        10,
        "external shrink must not resize a descriptor that was opened before the change"
    );
    let mut tail = [0u8; 2];
    let count = old_shrink
        .read_at(&mut tail, 8)
        .expect("pread within the old EOF");
    assert_eq!(
        count,
        tail.len(),
        "pread at the old EOF must still return pinned bytes"
    );
    assert_eq!(&tail, b"89");
    drop(old_shrink);
    reopen.unmount();

    let mountpoint = fixture
        .temp
        .path()
        .join(format!("mnt-external-final-{suffix}"));
    fs::create_dir(&mountpoint).expect("create final mountpoint");
    let mut final_mount = fixture.mount(&config, &mountpoint);
    assert_eq!(
        final_mount
            .mountpoint()
            .join("shrink.txt")
            .metadata()
            .expect("stat externally shrunk file")
            .len(),
        4
    );
    assert_eq!(
        fs::read(final_mount.mountpoint().join("shrink.txt")).expect("read externally shrunk file"),
        b"0123"
    );
    final_mount.unmount();
}

#[test]
#[ignore = "requires /dev/fuse and isolated S3; run with cargo x test-linux-fuse"]
fn fuse_concurrent_mounts_object() {
    let fixture = Fixture::new(Publication::Object);
    let library = fixture.volume_config("lib-concurrent", Frontend::Library, false);
    let config_a = fixture.volume_config("stage-concurrent-a", Frontend::Fuse, false);
    let config_b = fixture.volume_config("stage-concurrent-b", Frontend::Fuse, false);
    let mount_a = fixture.temp.path().join("mnt-concurrent-a");
    let mount_b = fixture.temp.path().join("mnt-concurrent-b");
    fs::create_dir(&mount_a).expect("create mount A");
    fs::create_dir(&mount_b).expect("create mount B");
    let mut a = fixture.mount(&config_a, &mount_a);
    let mut b = fixture.mount(&config_b, &mount_b);

    let start = Arc::new(Barrier::new(3));
    let publish = Arc::new(Barrier::new(3));
    let path_a = a.mountpoint().join("from-a.txt");
    let path_b = b.mountpoint().join("from-b.txt");
    let start_a = start.clone();
    let start_b = start.clone();
    let publish_a = publish.clone();
    let publish_b = publish.clone();
    let writer_a = thread::spawn(move || {
        start_a.wait();
        let mut file = File::create(&path_a).expect("create on mount A");
        file.write_all(b"a").expect("write via mount A");
        publish_a.wait();
        file.sync_all().expect("fsync via mount A");
    });
    let writer_b = thread::spawn(move || {
        start_b.wait();
        let mut file = File::create(&path_b).expect("create on mount B");
        file.write_all(b"b").expect("write via mount B");
        publish_b.wait();
        file.sync_all().expect("fsync via mount B");
    });
    start.wait();
    publish.wait();
    writer_a.join().expect("mount A writer panicked");
    writer_b.join().expect("mount B writer panicked");

    assert_eq!(
        fs::read(a.mountpoint().join("from-b.txt")).expect("cross read"),
        b"b"
    );
    assert_eq!(
        fs::read(b.mountpoint().join("from-a.txt")).expect("cross read"),
        b"a"
    );
    assert_eq!(fixture.remote_read(&library, "from-a.txt", 8), b"a");
    assert_eq!(fixture.remote_read(&library, "from-b.txt", 8), b"b");
    a.unmount();
    b.unmount();
}

#[test]
#[ignore = "requires /dev/fuse and isolated S3; run with cargo x test-linux-fuse"]
fn fuse_process_kill_recovery_object() {
    let fixture = Fixture::new(Publication::Object);
    let library = fixture.volume_config("lib-kill", Frontend::Library, false);
    fixture.library_seed(&library, "recover.txt", b"seed");
    let config = fixture.volume_config("stage-kill", Frontend::Fuse, false);
    let mountpoint = fixture.temp.path().join("mnt-kill");
    fs::create_dir(&mountpoint).expect("create mountpoint");
    let mut mounted = fixture.mount(&config, &mountpoint);

    let mut dirty = OpenOptions::new()
        .write(true)
        .open(mounted.mountpoint().join("recover.txt"))
        .expect("open file for staged write");
    dirty.write_all(b"dirty-local").expect("stage dirty write");
    assert_eq!(
        fixture.remote_read(&library, "recover.txt", 64),
        b"seed",
        "an un-fsynced mounted write must not publish before the daemon is interrupted"
    );
    mounted.kill();
    drop(dirty);

    let mountpoint = fixture.temp.path().join("mnt-kill-reopen");
    fs::create_dir(&mountpoint).expect("create reopen mountpoint");
    let mut reopened = fixture.mount(&config, &mountpoint);
    assert_eq!(
        fs::read(reopened.mountpoint().join("recover.txt")).expect("read recovered staging"),
        b"dirty-local",
        "a killed mount process must recover staged bytes from persistent staging"
    );
    let mut publish = OpenOptions::new()
        .write(true)
        .open(reopened.mountpoint().join("recover.txt"))
        .expect("reopen recovered file");
    publish.sync_all().expect("recovered handle publishes");
    drop(publish);
    assert_eq!(
        fixture.remote_read(&library, "recover.txt", 64),
        b"dirty-local",
        "independent authority must observe the recovered publication"
    );
    reopened.unmount();
}

#[test]
#[ignore = "requires /dev/fuse and isolated S3; run with cargo x test-linux-fuse"]
fn fuse_fsync_failure_retention_service() {
    let mut fixture = Fixture::new(Publication::Service);
    let library = fixture.volume_config("lib-network", Frontend::Library, false);
    fixture.library_seed(&library, "network.txt", b"seed");
    let config = fixture.volume_config("stage-network", Frontend::Fuse, false);
    let mountpoint = fixture.temp.path().join("mnt-network");
    fs::create_dir(&mountpoint).expect("create mountpoint");
    let mut mounted = fixture.mount(&config, &mountpoint);

    let mut dirty = OpenOptions::new()
        .read(true)
        .write(true)
        .open(mounted.mountpoint().join("network.txt"))
        .expect("open file for staged write");
    dirty.write_all(b"survives").expect("stage dirty write");
    fixture.stop_service();
    let failure = sync_all_with_deadline(&dirty, Duration::from_secs(20));
    assert!(
        failure.is_err(),
        "fsync must fail while the metadata service is unavailable"
    );
    dirty.seek(SeekFrom::Start(0)).expect("rewind dirty file");
    let mut retained = Vec::new();
    dirty
        .read_to_end(&mut retained)
        .expect("read retained dirty bytes");
    assert_eq!(
        retained, b"survives",
        "a failed fsync must not discard staged bytes"
    );
    drop(dirty);
    mounted.kill();

    fixture.restart_service();
    let mountpoint = fixture.temp.path().join("mnt-network-reopen");
    fs::create_dir(&mountpoint).expect("create reopen mountpoint");
    let mut reopened = fixture.mount(&config, &mountpoint);
    assert_eq!(
        fs::read(reopened.mountpoint().join("network.txt")).expect("read staged recovery"),
        b"survives"
    );
    let mut publish = OpenOptions::new()
        .write(true)
        .open(reopened.mountpoint().join("network.txt"))
        .expect("reopen recovered file");
    publish.sync_all().expect("recovered handle publishes");
    drop(publish);
    assert_eq!(
        fixture.remote_read(&library, "network.txt", 64),
        b"survives"
    );
    reopened.unmount();
}

#[test]
#[ignore = "requires /dev/fuse and isolated S3; run with cargo x test-linux-fuse"]
fn fuse_interrupted_call_retention_service() {
    let fixture = Fixture::new(Publication::Service);
    let library = fixture.volume_config("lib-interrupted", Frontend::Library, false);
    fixture.library_seed(&library, "interrupted.txt", b"seed");
    let config = fixture.volume_config("stage-interrupted", Frontend::Fuse, false);
    let mountpoint = fixture.temp.path().join("mnt-interrupted");
    fs::create_dir(&mountpoint).expect("create mountpoint");
    let mut mounted = fixture.mount(&config, &mountpoint);

    let mut dirty = OpenOptions::new()
        .read(true)
        .write(true)
        .open(mounted.mountpoint().join("interrupted.txt"))
        .expect("open file for staged write");
    dirty
        .write_all(b"interrupted-local")
        .expect("stage dirty write");
    let in_flight = dirty.try_clone().expect("duplicate descriptor");
    fixture.pause_service();
    let (sender, receiver) = mpsc::channel();
    let caller = thread::spawn(move || {
        let _ = sender.send(in_flight.sync_all());
    });
    thread::sleep(Duration::from_millis(200));
    mounted.kill();
    let outcome = receiver
        .recv_timeout(Duration::from_secs(20))
        .unwrap_or_else(|error| {
            panic!("fsync did not return after the mount daemon was killed: {error}")
        });
    caller.join().expect("fsync caller panicked");
    assert!(
        outcome.is_err(),
        "an fsync interrupted by killing the mount daemon must fail, not report remote durability"
    );
    drop(dirty);

    fixture.resume_service();
    assert_eq!(
        fixture.remote_read(&library, "interrupted.txt", 64),
        b"seed",
        "the interrupted publication must not have reached the authority"
    );
    let mountpoint = fixture.temp.path().join("mnt-interrupted-reopen");
    fs::create_dir(&mountpoint).expect("create reopen mountpoint");
    let mut reopened = fixture.mount(&config, &mountpoint);
    assert_eq!(
        fs::read(reopened.mountpoint().join("interrupted.txt")).expect("read staged recovery"),
        b"interrupted-local"
    );
    let mut publish = OpenOptions::new()
        .write(true)
        .open(reopened.mountpoint().join("interrupted.txt"))
        .expect("reopen recovered file");
    publish.sync_all().expect("recovered handle publishes");
    drop(publish);
    assert_eq!(
        fixture.remote_read(&library, "interrupted.txt", 64),
        b"interrupted-local"
    );
    reopened.unmount();
}

#[test]
#[ignore = "requires /dev/fuse and isolated S3; run with cargo x test-linux-fuse"]
fn fuse_busy_unmount_object() {
    let fixture = Fixture::new(Publication::Object);
    let config = fixture.volume_config("stage-busy", Frontend::Fuse, false);
    let mountpoint = fixture.temp.path().join("mnt-busy");
    fs::create_dir(&mountpoint).expect("create mountpoint");
    let mut mounted = fixture.mount(&config, &mountpoint);

    let file = File::create(mounted.mountpoint().join("held.txt")).expect("create held file");
    let output = crate::harness::run_timeout(
        std::process::Command::new("fusermount3").args([
            "-u",
            mounted.mountpoint().to_str().expect("UTF-8 mountpoint"),
        ]),
        Duration::from_secs(30),
    );
    assert!(
        !output.status.success(),
        "busy unmount unexpectedly succeeded while a descriptor was open: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    drop(file);
    mounted.unmount();
}

#[test]
#[ignore = "requires /dev/fuse and isolated S3; run with cargo x test-linux-fuse"]
fn fuse_sigterm_lifecycle_object() {
    let fixture = Fixture::new(Publication::Object);
    let config = fixture.volume_config("stage-sigterm", Frontend::Fuse, false);
    let mountpoint = fixture.temp.path().join("mnt-sigterm");
    fs::create_dir(&mountpoint).expect("create mountpoint");
    let mut mounted = fixture.mount(&config, &mountpoint);
    mounted.terminate();
}
