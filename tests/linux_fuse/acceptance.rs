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

use crate::harness::{
    Fixture, Frontend, Mounted, Publication, ServiceGate, assert_errno, run_external,
    sync_all_with_deadline,
};
use nix::fcntl::{FcntlArg, OFlag, fcntl};
use nix::unistd::truncate;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::os::unix::fs::{FileExt, MetadataExt, PermissionsExt};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

const EACCES: i32 = 13;
const ENOSYS: i32 = 38;
const EPERM: i32 = 1;
const EOPNOTSUPP: i32 = 95;
const EROFS: i32 = 30;
const EINVAL: i32 = 22;
const ENOTCONN: i32 = 107;
const EIO: i32 = 5;
const ENOENT: i32 = 2;
const ATTR_TTL_WAIT: Duration = Duration::from_millis(1500);

#[derive(Clone, Copy)]
enum Interrupt {
    Kill,
    Terminate,
}

#[test]
#[ignore = "requires /dev/fuse and isolated S3; run with cargo x test-linux-fuse"]
fn fuse_read_only_both_publications() {
    for publication in [Publication::Object, Publication::Service] {
        read_only(&Fixture::new(publication));
    }
}

fn names(path: &Path) -> std::collections::BTreeSet<String> {
    fs::read_dir(path)
        .unwrap_or_else(|error| panic!("read directory {}: {error}", path.display()))
        .map(|entry| {
            entry
                .expect("read directory entry")
                .file_name()
                .into_string()
                .expect("UTF-8 directory entry")
        })
        .collect()
}

fn inode(path: &Path) -> u64 {
    fs::metadata(path)
        .unwrap_or_else(|error| panic!("stat {}: {error}", path.display()))
        .ino()
}

fn mode(path: &Path) -> u32 {
    fs::metadata(path)
        .unwrap_or_else(|error| panic!("stat mode {}: {error}", path.display()))
        .permissions()
        .mode()
        & 0o7777
}

fn assert_staged_size(path: &Path, file: &File, expected: u64, phase: &str) {
    let fd = file
        .metadata()
        .unwrap_or_else(|error| panic!("fstat {phase}: {error}"));
    let by_path = fs::metadata(path).unwrap_or_else(|error| panic!("stat {phase}: {error}"));
    assert_eq!(
        by_path.ino(),
        fd.ino(),
        "{phase}: path lookup resolved a different inode"
    );
    assert_eq!(fd.len(), expected, "{phase}: fstat staged length");
    assert_eq!(
        by_path.len(),
        expected,
        "{phase}: path stat must use the same staged length"
    );
}

fn wait_child(child: &mut Child, timeout: Duration) -> std::process::ExitStatus {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(status) = child
            .try_wait()
            .unwrap_or_else(|error| panic!("wait for helper client: {error}"))
        {
            return status;
        }
        if Instant::now() >= deadline {
            panic!(
                "helper client did not exit within {timeout:?}; waiting while the service is paused is not allowed"
            );
        }
        thread::sleep(Duration::from_millis(50));
    }
}

fn wait_remote_bytes(
    fixture: &Fixture,
    library: &Path,
    path: &str,
    expected: &[u8],
    timeout: Duration,
) {
    let deadline = Instant::now() + timeout;
    loop {
        if fixture.remote_read(library, path, 128) == expected {
            return;
        }
        if Instant::now() >= deadline {
            panic!("remote bytes did not reach the delayed completion within {timeout:?}");
        }
        thread::sleep(Duration::from_millis(200));
    }
}

fn refresh_counts(line: &str) -> (u64, u64, u64) {
    let fields = line
        .split_once("done (")
        .and_then(|(_, rest)| rest.trim().strip_suffix(')'))
        .expect("refresh completion line has counts");
    let mut inode = None;
    let mut entries = None;
    let mut errors = None;
    for field in fields.split_whitespace() {
        let (key, value) = field.split_once('=').expect("refresh count field");
        let value = value.parse::<u64>().expect("numeric refresh count");
        match key {
            "inode" => inode = Some(value),
            "entries" => entries = Some(value),
            "errors" => errors = Some(value),
            other => panic!("unexpected refresh count field {other}"),
        }
    }
    (
        inode.expect("inode count"),
        entries.expect("entries count"),
        errors.expect("errors count"),
    )
}

#[test]
#[ignore = "requires /dev/fuse and isolated S3; run with cargo x test-linux-fuse"]
fn fuse_read_only_cached_readdir_and_nested_inodes_both_publications() {
    for publication in [Publication::Object, Publication::Service] {
        let fixture = Fixture::new(publication);
        fixture.seed_read_only_tree();
        let config = fixture.volume_config(
            &format!("stage-readdir-{}", fixture.publication().as_str()),
            Frontend::Fuse,
            true,
        );
        let mountpoint = fixture
            .temp
            .path()
            .join(format!("mnt-readdir-{}", fixture.publication().as_str()));
        fs::create_dir(&mountpoint).expect("create mountpoint");
        let mut mounted = fixture.mount(&config, &mountpoint);
        let root = mounted.mountpoint().to_path_buf();

        let nested = root.join("nested");
        let inner = nested.join("inner");
        let file = inner.join("hello.txt");
        assert_eq!(
            fs::read(&file).expect("read nested file"),
            b"hello from nested"
        );
        let nested_ino = inode(&nested);
        let inner_ino = inode(&inner);
        let file_ino = inode(&file);
        assert_eq!(
            inode(&nested),
            nested_ino,
            "nested inode changed across stat"
        );
        assert_eq!(
            inode(&inner),
            inner_ino,
            "inner directory inode changed across stat"
        );
        assert_eq!(inode(&file), file_ino, "file inode changed across stat");

        let root_first = names(root);
        let root_second = names(root);
        assert_eq!(root_first, root_second, "cached root readdir changed");
        assert!(root_first.contains("nested"));
        assert!(root_first.contains("bulk"));
        assert!(root_first.contains("readonly.txt"));
        let nested_first = names(&nested);
        assert_eq!(
            names(&nested),
            nested_first,
            "cached nested readdir changed"
        );
        let inner_first = names(&inner);
        assert_eq!(inner_first.len(), 1);
        assert!(inner_first.contains("hello.txt"));
        assert_eq!(names(&inner), inner_first, "cached inner readdir changed");

        let bulk = root.join("bulk");
        assert_eq!(fs::read(bulk.join("entry-0000")).unwrap(), b"");
        assert_eq!(
            inode(&bulk.join("entry-0000")),
            inode(&bulk.join("entry-0000"))
        );
        let bulk_first = names(&bulk);
        let bulk_second = names(&bulk);
        assert_eq!(bulk_first.len(), 520, "bulk directory entry count");
        assert_eq!(bulk_first, bulk_second, "multi-page cached readdir changed");
        for index in 0..520 {
            assert!(
                bulk_first.contains(&format!("entry-{index:04}")),
                "missing bulk entry {index}"
            );
        }
        mounted.unmount();
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
    file.write_all(b"hello!!").expect("write mounted file");
    file.sync_all().expect("remote fsync through FUSE");
    drop(file);
    assert_eq!(
        fixture.remote_read(&library, "basic.txt", 8),
        b"hello!!",
        "independent authority must observe a successful FUSE fsync"
    );

    let mut append = OpenOptions::new()
        .append(true)
        .open(root.join("basic.txt"))
        .expect("open mounted file for append");
    append.write_all(b"!").expect("append through FUSE");
    append.sync_all().expect("remote fsync after append");
    drop(append);
    assert_eq!(fixture.remote_read(&library, "basic.txt", 8), b"hello!!!");

    let truncate = OpenOptions::new()
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
    drop(reader);
    mounted.unmount();
}

#[test]
#[ignore = "requires /dev/fuse and isolated S3; run with cargo x test-linux-fuse"]
fn fuse_dirty_fd_readonly_close_keeps_remote_unchanged_both_publications() {
    for publication in [Publication::Object, Publication::Service] {
        let fixture = Fixture::new(publication);
        let suffix = fixture.publication().as_str();
        let library =
            fixture.volume_config(&format!("lib-flush-{suffix}"), Frontend::Library, false);
        fixture.library_seed(&library, "flush.txt", b"original");
        let config = fixture.volume_config(&format!("stage-flush-{suffix}"), Frontend::Fuse, false);
        let mountpoint = fixture.temp.path().join(format!("mnt-flush-{suffix}"));
        fs::create_dir(&mountpoint).expect("create mountpoint");
        let mut mounted = fixture.mount(&config, &mountpoint);

        let mut dirty = OpenOptions::new()
            .read(true)
            .write(true)
            .open(mounted.mountpoint().join("flush.txt"))
            .expect("open write descriptor");
        dirty.write_all(b"dirty!!!").expect("stage dirty bytes");
        let mut reader =
            File::open(mounted.mountpoint().join("flush.txt")).expect("open read-only descriptor");
        let mut staged = Vec::new();
        reader
            .read_to_end(&mut staged)
            .expect("read through the read-only descriptor");
        assert_eq!(staged, b"dirty!!!");
        drop(reader);
        assert_eq!(
            fixture.remote_read(&library, "flush.txt", 16),
            b"original",
            "closing a read-only descriptor must not publish another descriptor's dirty bytes"
        );

        let path = mounted.mountpoint().join("flush.txt");
        assert_staged_size(&path, &dirty, 8, "same-length dirty write");
        dirty.set_len(12).expect("grow the staged file");
        thread::sleep(ATTR_TTL_WAIT);
        assert_staged_size(&path, &dirty, 12, "dirty grow after attr TTL");
        dirty.seek(SeekFrom::Start(0)).expect("rewind grown file");
        let mut grown = Vec::new();
        dirty
            .read_to_end(&mut grown)
            .expect("read grown staged bytes");
        assert_eq!(grown, b"dirty!!!\0\0\0\0");
        dirty.set_len(4).expect("shrink the staged file");
        thread::sleep(ATTR_TTL_WAIT);
        assert_staged_size(&path, &dirty, 4, "dirty shrink after attr TTL");
        dirty.seek(SeekFrom::Start(0)).expect("rewind shrunk file");
        let mut shrunk = Vec::new();
        dirty
            .read_to_end(&mut shrunk)
            .expect("read shrunk staged bytes");
        assert_eq!(shrunk, b"dirt");
        dirty.sync_all().expect("explicit dirty fsync publishes");
        drop(dirty);
        assert_eq!(fixture.remote_read(&library, "flush.txt", 16), b"dirt");
        mounted.unmount();
    }
}

#[test]
#[ignore = "requires /dev/fuse and isolated S3; run with cargo x test-linux-fuse"]
fn fuse_cloexec_child_close_flushes_dirty_fd_both_publications() {
    for publication in [Publication::Object, Publication::Service] {
        let fixture = Fixture::new(publication);
        let suffix = fixture.publication().as_str();
        let library =
            fixture.volume_config(&format!("lib-cloexec-{suffix}"), Frontend::Library, false);
        fixture.library_seed(&library, "cloexec.txt", b"original");
        let config =
            fixture.volume_config(&format!("stage-cloexec-{suffix}"), Frontend::Fuse, false);
        let mountpoint = fixture.temp.path().join(format!("mnt-cloexec-{suffix}"));
        fs::create_dir(&mountpoint).expect("create mountpoint");
        let mut mounted = fixture.mount(&config, &mountpoint);

        let mut dirty = OpenOptions::new()
            .read(true)
            .write(true)
            .open(mounted.mountpoint().join("cloexec.txt"))
            .expect("open dirty descriptor");
        dirty.write_all(b"dirty!!!").expect("stage dirty bytes");
        assert_eq!(
            fixture.remote_read(&library, "cloexec.txt", 16),
            b"original",
            "an in-process independent read must not close the dirty FUSE descriptor"
        );
        Command::new("/bin/true")
            .status()
            .expect("spawn a child that exec-closes the inherited dirty descriptor");
        wait_remote_bytes(
            &fixture,
            &library,
            "cloexec.txt",
            b"dirty!!!",
            Duration::from_secs(15),
        );
        drop(dirty);
        mounted.unmount();
    }
}

#[test]
#[ignore = "requires /dev/fuse and isolated S3; run with cargo x test-linux-fuse"]
fn fuse_append_flag_toggle_both_publications() {
    for publication in [Publication::Object, Publication::Service] {
        let fixture = Fixture::new(publication);
        let suffix = fixture.publication().as_str();
        let library =
            fixture.volume_config(&format!("lib-append-{suffix}"), Frontend::Library, false);
        fixture.library_seed(&library, "append-toggle.txt", b"0123456789");
        let config =
            fixture.volume_config(&format!("stage-append-{suffix}"), Frontend::Fuse, false);
        let mountpoint = fixture.temp.path().join(format!("mnt-append-{suffix}"));
        fs::create_dir(&mountpoint).expect("create mountpoint");
        let mut mounted = fixture.mount(&config, &mountpoint);
        let path = mounted.mountpoint().join("append-toggle.txt");

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .append(true)
            .open(&path)
            .expect("open with O_APPEND");
        let flags = fcntl(&file, FcntlArg::F_GETFL).expect("read open flags");
        let flags = OFlag::from_bits_truncate(flags);
        assert!(flags.contains(OFlag::O_APPEND));
        fcntl(&file, FcntlArg::F_SETFL(flags.difference(OFlag::O_APPEND)))
            .expect("clear O_APPEND with fcntl");
        file.seek(SeekFrom::Start(0))
            .expect("seek after clearing O_APPEND");
        file.write_all(b"AB")
            .expect("write after clearing O_APPEND");
        file.sync_all().expect("fsync overwrite without O_APPEND");
        assert_eq!(
            fixture.remote_read(&library, "append-toggle.txt", 16),
            b"AB23456789",
            "cleared O_APPEND must honor the explicit offset"
        );

        fcntl(&file, FcntlArg::F_SETFL(flags)).expect("restore O_APPEND with fcntl");
        file.seek(SeekFrom::Start(0))
            .expect("seek is ignored when O_APPEND is restored");
        file.write_all(b"CD")
            .expect("write after restoring O_APPEND");
        file.sync_all().expect("fsync appended write");
        assert_eq!(
            fixture.remote_read(&library, "append-toggle.txt", 16),
            b"AB23456789CD",
            "restored O_APPEND must append at the current EOF"
        );
        drop(file);
        mounted.unmount();
    }
}

#[test]
#[ignore = "requires /dev/fuse and isolated S3; run with cargo x test-linux-fuse"]
fn fuse_path_truncate_commits_both_publications() {
    for publication in [Publication::Object, Publication::Service] {
        let fixture = Fixture::new(publication);
        let suffix = fixture.publication().as_str();
        let library =
            fixture.volume_config(&format!("lib-truncate-{suffix}"), Frontend::Library, false);
        fixture.library_seed(&library, "truncate.txt", b"0123456789");
        let config =
            fixture.volume_config(&format!("stage-truncate-{suffix}"), Frontend::Fuse, false);
        let mountpoint = fixture.temp.path().join(format!("mnt-truncate-{suffix}"));
        fs::create_dir(&mountpoint).expect("create mountpoint");
        let mut mounted = fixture.mount(&config, &mountpoint);
        let path = mounted.mountpoint().join("truncate.txt");

        truncate(&path, 3).expect("truncate(2) shrink without an open descriptor");
        assert_eq!(fs::metadata(&path).expect("stat truncated path").len(), 3);
        assert_eq!(
            fixture.remote_read(&library, "truncate.txt", 16),
            b"012",
            "a successful path truncate must have an explicit remote commit boundary"
        );
        truncate(&path, 6).expect("truncate(2) extend without an open descriptor");
        assert_eq!(
            fixture.remote_read(&library, "truncate.txt", 16),
            vec![b'0', b'1', b'2', 0u8, 0u8, 0u8],
            "path extension must also reach the independent authority"
        );
        mounted.unmount();
    }
}

#[test]
#[ignore = "requires /dev/fuse and isolated S3; run with cargo x test-linux-fuse"]
fn fuse_chmod_limits_to_executable_bit_both_publications() {
    for publication in [Publication::Object, Publication::Service] {
        let fixture = Fixture::new(publication);
        let suffix = fixture.publication().as_str();
        let library =
            fixture.volume_config(&format!("lib-chmod-{suffix}"), Frontend::Library, false);
        fixture.library_seed(&library, "chmod.txt", b"mode");
        let config = fixture.volume_config(&format!("stage-chmod-{suffix}"), Frontend::Fuse, false);
        let mountpoint = fixture.temp.path().join(format!("mnt-chmod-{suffix}"));
        fs::create_dir(&mountpoint).expect("create mountpoint");
        let mut mounted = fixture.mount(&config, &mountpoint);
        let path = mounted.mountpoint().join("chmod.txt");

        fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).expect("baseline chmod 0644");
        assert_eq!(mode(&path), 0o644);
        fs::set_permissions(&path, fs::Permissions::from_mode(0o755))
            .expect("chmod 0755 must be representable");
        assert_eq!(mode(&path), 0o755);
        fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).expect("chmod back to 0644");
        assert_eq!(mode(&path), 0o644);

        for unsupported_mode in [0o600, 0o000] {
            let error = fs::set_permissions(&path, fs::Permissions::from_mode(unsupported_mode))
                .expect_err("non-representable mode must fail explicitly");
            assert_errno(
                error,
                &[ENOSYS, EOPNOTSUPP, EINVAL],
                "chmod non-representable mode",
            );
            assert_eq!(
                mode(&path),
                0o644,
                "failed chmod {unsupported_mode:o} must not change the previous attributes"
            );
        }

        let directory = mounted.mountpoint().join("fixed-directory");
        fs::create_dir(&directory).expect("create directory through the writable mount");
        assert_eq!(mode(&directory), 0o755, "directories are fixed at 0755");
        let error = fs::set_permissions(&directory, fs::Permissions::from_mode(0o644))
            .expect_err("directory chmod 0644 must not report success");
        assert_errno(error, &[ENOSYS, EOPNOTSUPP, EINVAL], "chmod directory 0644");
        assert_eq!(
            mode(&directory),
            0o755,
            "a rejected directory chmod must leave 0755 unchanged"
        );
        mounted.unmount();
    }
}

#[test]
#[ignore = "requires /dev/fuse and isolated S3; run with cargo x test-linux-fuse"]
fn fuse_dirty_fd_and_executable_chmod_do_not_self_conflict_both_publications() {
    for publication in [Publication::Object, Publication::Service] {
        let fixture = Fixture::new(publication);
        let suffix = fixture.publication().as_str();
        let library = fixture.volume_config(
            &format!("lib-chmod-dirty-{suffix}"),
            Frontend::Library,
            false,
        );
        fixture.library_seed(&library, "chmod-dirty.txt", b"base");
        let config = fixture.volume_config(
            &format!("stage-chmod-dirty-{suffix}"),
            Frontend::Fuse,
            false,
        );
        let mountpoint = fixture
            .temp
            .path()
            .join(format!("mnt-chmod-dirty-{suffix}"));
        fs::create_dir(&mountpoint).expect("create mountpoint");
        let mut mounted = fixture.mount(&config, &mountpoint);
        let path = mounted.mountpoint().join("chmod-dirty.txt");

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .expect("open descriptor before chmod");
        file.write_all(b"first").expect("stage first dirty write");
        fs::set_permissions(&path, fs::Permissions::from_mode(0o755))
            .expect("path chmod executable while the descriptor is dirty");
        file.write_all(b"-tail")
            .expect("continue writing through the same descriptor");
        file.sync_all()
            .expect("the mount's own metadata generation change must not self-conflict");
        drop(file);
        assert_eq!(mode(&path), 0o755);
        assert_eq!(
            fixture.remote_read(&library, "chmod-dirty.txt", 32),
            b"first-tail",
            "continued writes and executable metadata must publish together"
        );
        mounted.unmount();
    }
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
    let source_inode = inode(&root.join("saved.txt.new"));
    fs::rename(root.join("saved.txt.new"), root.join("renamed.txt"))
        .expect("plain rename must preserve inode identity");
    assert_eq!(
        inode(&root.join("renamed.txt")),
        source_inode,
        "rename must not change the inode identity of the moved node"
    );
    fs::rename(root.join("renamed.txt"), root.join("saved.txt.new"))
        .expect("rename back before safe-save");
    let target_inode = inode(&root.join("saved.txt"));
    fs::rename(root.join("saved.txt.new"), root.join("saved.txt"))
        .expect("safe-save rename must atomically replace the target");
    assert_eq!(
        inode(&root.join("saved.txt")),
        source_inode,
        "safe-save replacement must keep the source identity"
    );
    assert_ne!(
        target_inode, source_inode,
        "safe-save replacement must replace the observed target node"
    );
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
    thread::sleep(ATTR_TTL_WAIT);
    assert_staged_size(
        &mounted.mountpoint().join("external.txt"),
        &old,
        4,
        "external growth with a retained descriptor",
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
    let mut fresh = File::open(mounted.mountpoint().join("external.txt"))
        .expect("reopen externally grown file on the live mount");
    let mut fresh_bytes = Vec::new();
    fresh
        .read_to_end(&mut fresh_bytes)
        .expect("read externally grown file on the live mount");
    assert_eq!(fresh_bytes, b"BBBBBBBB");
    assert_eq!(
        fresh.metadata().expect("fstat fresh grow").len(),
        8,
        "fresh open must materialize the external grow length"
    );
    drop(fresh);

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
    thread::sleep(ATTR_TTL_WAIT);
    assert_staged_size(
        &reopen.mountpoint().join("shrink.txt"),
        &old_shrink,
        10,
        "external shrink with a retained descriptor",
    );
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
    let mut fresh_shrink = File::open(reopen.mountpoint().join("shrink.txt"))
        .expect("reopen externally shrunk file on the live mount");
    let mut fresh_shrink_bytes = Vec::new();
    fresh_shrink
        .read_to_end(&mut fresh_shrink_bytes)
        .expect("read externally shrunk file on the live mount");
    assert_eq!(fresh_shrink_bytes, b"0123");
    assert_eq!(
        fresh_shrink.metadata().expect("fstat fresh shrink").len(),
        4,
        "fresh open must materialize the external shrink length"
    );
    drop(fresh_shrink);
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
fn fuse_sighup_refreshes_namespace_without_rebasing_old_fd_both_publications() {
    for publication in [Publication::Object, Publication::Service] {
        let fixture = Fixture::new(publication);
        let suffix = fixture.publication().as_str();
        let library =
            fixture.volume_config(&format!("lib-sighup-{suffix}"), Frontend::Library, false);
        fixture.library_seed(&library, "a.txt", b"old-content");
        let config =
            fixture.volume_config(&format!("stage-sighup-{suffix}"), Frontend::Fuse, false);
        let mountpoint = fixture.temp.path().join(format!("mnt-sighup-{suffix}"));
        fs::create_dir(&mountpoint).expect("create mountpoint");
        let mut mounted = fixture.mount(&config, &mountpoint);
        let root = mounted.mountpoint();

        let old_path = root.join("a.txt");
        let mut old = File::open(&old_path).expect("open old descriptor before refresh");
        let old_ino = inode(&old_path);
        let old_len = fs::metadata(&old_path).expect("warm old dentry").len();
        let mut old_bytes = Vec::new();
        old.read_to_end(&mut old_bytes).expect("read old bytes");
        assert_eq!(old_bytes, b"old-content");

        let new_payload = b"new-content-longer";
        fixture.remote_rename(&library, "a.txt", "b.txt");
        fixture.remote_write(&library, "b.txt", new_payload);
        fixture.remote_create(&library, "new.txt");
        fixture.remote_write(&library, "new.txt", b"added");

        mounted.refresh_sighup();
        let refresh_log =
            mounted.wait_for_output_line("yy: fuse refresh: done (", Duration::from_secs(30));
        let completion = refresh_log
            .lines()
            .find(|line| line.contains("yy: fuse refresh: done ("))
            .expect("refresh completion line");
        let (inode_count, entry_count, error_count) = refresh_counts(completion);
        assert!(
            inode_count > 0 && entry_count > 0 && error_count == 0,
            "refresh must acknowledge native notifications (inode={inode_count} \
             entries={entry_count} errors={error_count}):\n{refresh_log}"
        );

        assert_errno(
            fs::metadata(&old_path).expect_err("renamed source must disappear after SIGHUP"),
            &[ENOENT],
            "stat renamed source after SIGHUP",
        );
        let shared = fs::metadata(root.join("b.txt")).expect("stat renamed target after SIGHUP");
        assert_eq!(shared.ino(), old_ino);
        assert_eq!(shared.len(), old_len);
        assert_eq!(
            fs::read(root.join("b.txt")).expect("read renamed target after SIGHUP"),
            b"old-content",
            "the live mount keeps the shared pinned view while the old descriptor is active"
        );
        assert_eq!(
            fixture.remote_read(&library, "b.txt", 32),
            new_payload,
            "the independent authority sees the externally updated target"
        );
        assert_eq!(
            fs::read(root.join("new.txt")).expect("read externally added file after SIGHUP"),
            b"added"
        );
        assert_eq!(old.metadata().expect("fstat old descriptor").ino(), old_ino);
        assert_eq!(old.metadata().expect("fstat old descriptor").len(), old_len);
        old.seek(SeekFrom::Start(0)).expect("rewind old descriptor");
        let mut pinned = Vec::new();
        old.read_to_end(&mut pinned)
            .expect("read old descriptor after SIGHUP");
        assert_eq!(pinned, b"old-content");
        drop(old);

        let mut fresh = File::open(root.join("b.txt")).expect("fresh open after old close");
        let mut fresh_bytes = Vec::new();
        fresh
            .read_to_end(&mut fresh_bytes)
            .expect("fresh read after old close");
        assert_eq!(fresh_bytes, new_payload);
        assert_eq!(
            fresh.metadata().expect("fresh fstat after old close").len(),
            new_payload.len() as u64
        );
        drop(fresh);
        mounted.unmount();
    }
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

    let path_a = a.mountpoint().join("from-a.txt");
    let path_b = b.mountpoint().join("from-b.txt");
    let (ready_tx, ready_rx) = mpsc::channel::<Result<(), String>>();
    let (go_a_tx, go_a_rx) = mpsc::channel();
    let (go_b_tx, go_b_rx) = mpsc::channel();
    let ready_a = ready_tx.clone();
    let writer_a = thread::spawn(move || {
        let prepared = (|| -> std::io::Result<File> {
            let mut file = File::create(&path_a)?;
            file.write_all(b"a")?;
            Ok(file)
        })();
        match prepared {
            Ok(file) => {
                ready_a
                    .send(Ok(()))
                    .expect("send mount A preparation barrier");
                let _ = go_a_rx.recv();
                file.sync_all()
            }
            Err(error) => {
                let _ = ready_a.send(Err(format!("mount A prepare failed: {error}")));
                Err(error)
            }
        }
    });
    let ready_b = ready_tx;
    let writer_b = thread::spawn(move || {
        let prepared = (|| -> std::io::Result<File> {
            let mut file = File::create(&path_b)?;
            file.write_all(b"b")?;
            Ok(file)
        })();
        match prepared {
            Ok(file) => {
                ready_b
                    .send(Ok(()))
                    .expect("send mount B preparation barrier");
                let _ = go_b_rx.recv();
                file.sync_all()
            }
            Err(error) => {
                let _ = ready_b.send(Err(format!("mount B prepare failed: {error}")));
                Err(error)
            }
        }
    });
    let first = ready_rx
        .recv_timeout(Duration::from_secs(60))
        .expect("mount A did not reach its preparation barrier");
    let second = ready_rx
        .recv_timeout(Duration::from_secs(60))
        .expect("mount B did not reach its preparation barrier");
    let _ = go_a_tx.send(());
    let _ = go_b_tx.send(());
    let result_a = writer_a.join().expect("mount A writer panicked");
    let result_b = writer_b.join().expect("mount B writer panicked");
    assert!(first.is_ok(), "{first:?}");
    assert!(second.is_ok(), "{second:?}");
    result_a.expect("fsync via mount A");
    result_b.expect("fsync via mount B");

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
    mounted.kill_daemon_only();
    drop(dirty);
    mounted.cleanup_after_kill();

    let mountpoint = fixture.temp.path().join("mnt-kill-reopen");
    fs::create_dir(&mountpoint).expect("create reopen mountpoint");
    let mut reopened = fixture.mount(&config, &mountpoint);
    assert_eq!(
        fs::read(reopened.mountpoint().join("recover.txt")).expect("read recovered staging"),
        b"dirty-local",
        "a killed mount process must recover staged bytes from persistent staging"
    );
    let publish = OpenOptions::new()
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
    mounted.kill_daemon_only();
    drop(dirty);
    mounted.cleanup_after_kill();

    fixture.restart_service();
    let mountpoint = fixture.temp.path().join("mnt-network-reopen");
    fs::create_dir(&mountpoint).expect("create reopen mountpoint");
    let mut reopened = fixture.mount(&config, &mountpoint);
    assert_eq!(
        fs::read(reopened.mountpoint().join("network.txt")).expect("read staged recovery"),
        b"survives"
    );
    let publish = OpenOptions::new()
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
fn fuse_daemon_crash_interrupts_pending_fsync_service() {
    let fixture = Fixture::new(Publication::Service);
    let library = fixture.volume_config("lib-interrupted", Frontend::Library, false);
    fixture.library_seed(&library, "interrupted.txt", b"seed");
    let staging = fixture.temp.path().join("stage-interrupted");
    let gate = ServiceGate::new(staging, fixture.service_address());
    let config = fixture.volume_config_with_service(
        "stage-interrupted",
        Frontend::Fuse,
        false,
        Some(gate.address()),
    );
    let mountpoint = fixture.temp.path().join("mnt-interrupted");
    fs::create_dir(&mountpoint).expect("create mountpoint");
    let mounted = fixture.mount(&config, &mountpoint);
    interrupt_pending_fsync(&fixture, &library, &config, mounted, &gate, Interrupt::Kill);
}

#[test]
#[ignore = "requires /dev/fuse and isolated S3; run with cargo x test-linux-fuse"]
fn fuse_sigterm_pending_fsync_service_exits_after_grace() {
    let fixture = Fixture::new(Publication::Service);
    let library = fixture.volume_config("lib-sigterm-fsync", Frontend::Library, false);
    fixture.library_seed(&library, "interrupted.txt", b"seed");
    let staging = fixture.temp.path().join("stage-sigterm-fsync");
    let gate = ServiceGate::new(staging, fixture.service_address());
    let config = fixture.volume_config_with_service(
        "stage-sigterm-fsync",
        Frontend::Fuse,
        false,
        Some(gate.address()),
    );
    let mountpoint = fixture.temp.path().join("mnt-sigterm-fsync");
    fs::create_dir(&mountpoint).expect("create mountpoint");
    let mounted = fixture.mount(&config, &mountpoint);
    interrupt_pending_fsync(
        &fixture,
        &library,
        &config,
        mounted,
        &gate,
        Interrupt::Terminate,
    );
}

fn interrupt_pending_fsync(
    fixture: &Fixture,
    library: &Path,
    config: &Path,
    mut mounted: Mounted,
    gate: &ServiceGate,
    interrupt: Interrupt,
) {
    let mut dirty = OpenOptions::new()
        .read(true)
        .write(true)
        .open(mounted.mountpoint().join("interrupted.txt"))
        .expect("open file for staged write");
    dirty
        .write_all(b"interrupted-local")
        .expect("stage dirty write");
    let in_flight = dirty.try_clone().expect("duplicate descriptor");
    let (sender, receiver) = mpsc::channel();
    let caller = thread::spawn(move || {
        let _ = sender.send(in_flight.sync_all());
    });
    gate.wait_ready(Duration::from_secs(30));
    let frozen = fixture.wait_for_frozen(&config);
    println!("fsync reached the frozen publication boundary:\n{frozen}");
    match interrupt {
        Interrupt::Kill => mounted.kill_daemon_only(),
        Interrupt::Terminate => {
            let status = mounted.terminate_busy();
            assert!(
                !status.success(),
                "SIGTERM with a pending fsync must force a non-zero exit after grace"
            );
        }
    }
    assert_eq!(
        fixture.remote_read(&library, "interrupted.txt", 64),
        b"seed",
        "the authority must remain unchanged while the frozen commit connection is gated"
    );
    gate.release();
    let outcome = receiver
        .recv_timeout(Duration::from_secs(20))
        .unwrap_or_else(|error| {
            panic!("fsync did not return after the mount daemon stopped: {error}")
        });
    caller.join().expect("fsync caller panicked");
    assert!(
        outcome.is_err(),
        "an fsync interrupted by stopping the mount daemon must fail, not report remote durability"
    );
    drop(dirty);
    mounted.cleanup_after_kill();

    let mountpoint = fixture.temp.path().join(format!(
        "mnt-interrupted-reopen-{}",
        fixture.publication().as_str()
    ));
    fs::create_dir(&mountpoint).expect("create reopen mountpoint");
    let mut reopened = fixture.mount(&config, &mountpoint);
    assert_eq!(
        fs::read(reopened.mountpoint().join("interrupted.txt")).expect("read staged recovery"),
        b"interrupted-local"
    );
    let publish = OpenOptions::new()
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
fn fuse_delayed_completion_survives_client_interruption_service() {
    let fixture = Fixture::new(Publication::Service);
    let library = fixture.volume_config("lib-delayed", Frontend::Library, false);
    fixture.library_seed(&library, "delayed.txt", b"seed");
    let staging = fixture.temp.path().join("stage-delayed");
    let gate = ServiceGate::new(staging, fixture.service_address());
    let config = fixture.volume_config_with_service(
        "stage-delayed",
        Frontend::Fuse,
        false,
        Some(gate.address()),
    );
    let mountpoint = fixture.temp.path().join("mnt-delayed");
    fs::create_dir(&mountpoint).expect("create mountpoint");
    let mut mounted = fixture.mount(&config, &mountpoint);
    let path = mounted.mountpoint().join("delayed.txt");

    let script = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/linux_fuse/delayed_client.py");
    let mut client = Command::new("python3")
        .arg(script)
        .arg(&path)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn delayed completion client");
    let stdout = client.stdout.take().expect("client stdout is piped");
    let (sender, receiver) = mpsc::channel();
    let reader = thread::spawn(move || {
        let mut line = String::new();
        if BufReader::new(stdout).read_line(&mut line).is_ok() {
            let _ = sender.send(line);
        }
    });
    let line = receiver
        .recv_timeout(Duration::from_secs(30))
        .unwrap_or_else(|error| panic!("client did not reach its write barrier: {error}"));
    assert_eq!(line.trim(), "READY", "unexpected client barrier: {line:?}");

    client
        .stdin
        .as_mut()
        .expect("client stdin is piped")
        .write_all(b"FSYNC\n")
        .expect("send fsync command");
    gate.wait_ready(Duration::from_secs(30));
    let frozen = fixture.wait_for_frozen(&config);
    println!("delayed fsync reached the frozen publication boundary:\n{frozen}");
    client.kill().expect("SIGKILL the blocked client");
    gate.release();
    let _ = wait_child(&mut client, Duration::from_secs(30));
    let _ = reader.join();

    wait_remote_bytes(
        &fixture,
        &library,
        "delayed.txt",
        b"delayed-payload",
        Duration::from_secs(60),
    );
    assert!(
        names(mounted.mountpoint()).contains("delayed.txt"),
        "the daemon must remain available after delayed completion"
    );
    mounted.unmount();
}

#[test]
#[ignore = "requires /dev/fuse and isolated S3; run with cargo x test-linux-fuse"]
fn fuse_sigterm_with_open_fd_exits_and_releases_lease_both_publications() {
    for publication in [Publication::Object, Publication::Service] {
        let fixture = Fixture::new(publication);
        let suffix = fixture.publication().as_str();
        let library = fixture.volume_config(
            &format!("lib-sigterm-open-{suffix}"),
            Frontend::Library,
            false,
        );
        fixture.library_seed(&library, "sigterm-open.txt", b"seed");
        let config = fixture.volume_config(
            &format!("stage-sigterm-open-{suffix}"),
            Frontend::Fuse,
            false,
        );
        let mountpoint = fixture
            .temp
            .path()
            .join(format!("mnt-sigterm-open-{suffix}"));
        fs::create_dir(&mountpoint).expect("create mountpoint");
        let mut mounted = fixture.mount(&config, &mountpoint);
        let path = mounted.mountpoint().join("sigterm-open.txt");
        let mut held = File::open(&path).expect("hold a descriptor across SIGTERM");
        let status = mounted.terminate_busy();
        assert!(
            !status.success(),
            "SIGTERM with an open descriptor must exit through the forced non-zero path"
        );
        let mut after = Vec::new();
        let error = held
            .read_to_end(&mut after)
            .expect_err("a held descriptor must report an I/O error after daemon exit");
        assert_errno(error, &[ENOTCONN, EIO], "read after busy SIGTERM");
        drop(held);
        mounted.cleanup_after_kill();

        let mountpoint = fixture
            .temp
            .path()
            .join(format!("mnt-sigterm-open-reopen-{suffix}"));
        fs::create_dir(&mountpoint).expect("create reopen mountpoint");
        let mut reopened = fixture.mount(&config, &mountpoint);
        assert_eq!(
            fs::read(reopened.mountpoint().join("sigterm-open.txt"))
                .expect("read after SIGTERM remount"),
            b"seed"
        );
        reopened.unmount();
    }
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
    let mountpoint = mounted.mountpoint().to_str().expect("UTF-8 mountpoint");
    let output = run_external("fusermount3", &["-u", mountpoint], Duration::from_secs(30));
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
