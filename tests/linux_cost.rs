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

//! Reproducible Linux product-path cost baseline for issue #78.
//!
//! Every workload enters through the `yy` executable. FUSE scenarios use an
//! ordinary kernel mount and POSIX descriptors, Sync scenarios use the managed
//! directory CLI, and one-shot transfer scenarios use `yy publish`/`yy restore`.
//! A loopback counting proxy in front of Silo supplies request and byte counts
//! without instrumenting production code.

use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::os::linux::fs::MetadataExt;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use serde::Serialize;

#[allow(dead_code)]
#[path = "linux_sync/harness.rs"]
mod fixture;

use fixture::{Fixture, Publication, SyncPaths};

const RESULT_SCHEMA: &str = "yinyang.linux-cost.v1";
const CHUNK: usize = 64 * 1024;
const RANGE_BYTES: usize = 4096;
const PROCESS_IO_NOTE: &str = "sampled from /proc/PID/io while alive; terminal increments can be missed, so values are lower bounds and character I/O includes sockets";

#[derive(Clone, Copy)]
enum Profile {
    Early,
    Full,
}

#[derive(Clone, Copy)]
enum Scale {
    Small,
    Medium,
}

impl Scale {
    fn as_str(self) -> &'static str {
        match self {
            Self::Small => "small",
            Self::Medium => "medium",
        }
    }

    fn files(self) -> usize {
        match self {
            Self::Small => 32,
            Self::Medium => 128,
        }
    }

    fn directories(self) -> usize {
        match self {
            Self::Small => 4,
            Self::Medium => 8,
        }
    }

    fn file_size(self) -> usize {
        match self {
            Self::Small => 8 * 1024 * 1024,
            Self::Medium => 32 * 1024 * 1024,
        }
    }

    fn writers(self) -> usize {
        match self {
            Self::Small => 2,
            Self::Medium => 4,
        }
    }
}

struct RunContext {
    profile: Profile,
    source_sha: String,
    source_dirty: bool,
    binary_path: String,
    binary_blake3: String,
    host: String,
    rustc: String,
}

#[test]
#[ignore = "requires /dev/fuse, isolated S3, and the Linux benchmark entry point"]
fn linux_cost_benchmark() {
    require_benchmark_environment();
    let profile = match std::env::var("YINYANG_COST_PROFILE").as_deref() {
        Ok("early") => Profile::Early,
        Ok("full") => Profile::Full,
        value => panic!("YINYANG_COST_PROFILE must be early or full, got {value:?}"),
    };
    let output = PathBuf::from(
        std::env::var_os("YINYANG_COST_OUTPUT")
            .expect("set YINYANG_COST_OUTPUT to the benchmark JSONL path"),
    );
    let summary = std::env::var_os("YINYANG_COST_SUMMARY")
        .map(PathBuf::from)
        .unwrap_or_else(|| output.with_extension("md"));
    let source_sha = std::env::var("YINYANG_COST_SOURCE_SHA")
        .expect("set YINYANG_COST_SOURCE_SHA to the exact tested source commit");
    let source_dirty = std::env::var("YINYANG_COST_SOURCE_DIRTY").as_deref() == Ok("1");
    let binary_path = PathBuf::from(env!("CARGO_BIN_EXE_yy"));
    let context = RunContext {
        profile,
        source_sha,
        source_dirty,
        binary_blake3: blake3_file(&binary_path),
        binary_path: binary_path.display().to_string(),
        host: host_identity(),
        rustc: command_line("rustc", &["--version"]),
    };

    let upstream = s3_upstream();
    let proxy = Arc::new(CountingProxy::start(upstream));
    println!(
        "linux-cost: source={} dirty={} binary={} binary_blake3={} profile={:?} proxy={} silo={upstream}",
        context.source_sha,
        context.source_dirty,
        context.binary_path,
        context.binary_blake3,
        match context.profile {
            Profile::Early => "early",
            Profile::Full => "full",
        },
        proxy.endpoint()
    );

    let publications: &[Publication] = match profile {
        Profile::Early => &[Publication::Object],
        Profile::Full => &[Publication::Object, Publication::Service],
    };
    let scales: &[Scale] = match profile {
        Profile::Early => &[Scale::Small],
        Profile::Full => &[Scale::Small, Scale::Medium],
    };
    let mut recorder = Recorder::new(&context, &output, &summary);

    for &publication in publications {
        for &scale in scales {
            run_sync_matrix(&context, &proxy, publication, scale, &mut recorder);
            run_fuse_matrix(&context, &proxy, publication, scale, &mut recorder);
            run_concurrent_matrix(&context, &proxy, publication, scale, &mut recorder);
        }
    }
    for &scale in scales {
        run_publish_restore_matrix(&context, &proxy, scale, &mut recorder);
        if matches!(profile, Profile::Full) {
            recorder.unsupported(
                &context,
                "publish_restore",
                Publication::Service,
                scale,
                "one-shot transfer has no service-authority CLI path; it remains object-only",
            );
        }
    }
    recorder.finish();
}

fn require_benchmark_environment() {
    if !Path::new("/dev/fuse").exists() {
        panic!("Linux cost benchmark requires /dev/fuse");
    }
    if command_line("fusermount3", &["--version"])
        .trim()
        .is_empty()
    {
        panic!("Linux cost benchmark requires fusermount3");
    }
    for name in [
        "YINYANG_S3_BUCKET",
        "YINYANG_S3_ENDPOINT",
        "YINYANG_S3_REGION",
        "YINYANG_S3_ACCESS_KEY_ID",
        "YINYANG_S3_SECRET_ACCESS_KEY",
    ] {
        assert!(
            std::env::var_os(name).is_some(),
            "{name} is unset; the benchmark fails hard instead of skipping"
        );
    }
}

fn run_sync_matrix(
    context: &RunContext,
    proxy: &Arc<CountingProxy>,
    publication: Publication,
    scale: Scale,
    recorder: &mut Recorder,
) {
    let fixture = Fixture::new_with_storage_endpoint(publication, Some(proxy.endpoint()));
    let paths = fixture.sync_paths(&format!("sync-{}-{}", publication.as_str(), scale.as_str()));
    fs::create_dir(&paths.directory).expect("create benchmark sync root");
    populate_small_tree(&paths.directory, scale);
    fixture.sync_init(&paths);
    let dataset = tree_dataset(&paths.directory);
    let completion = "latency is one finite yy sync run; in_sync status was read separately after the measured run";

    let initial = run_yy_operation(
        proxy,
        fixture.yy_command(&["--volume", path_str(&paths.config), "sync", "run"]),
        "initial Sync push",
    );
    assert_sync_in_sync(&fixture, &paths, "after initial Sync push");
    recorder.record(
        context,
        "sync_initial",
        publication,
        scale,
        dataset.clone(),
        completion,
        vec![initial.latency_ms],
        initial.io,
        initial.network,
        vec![],
        space_snapshot(&fixture, Some(&paths.directory), Some(&paths.state), None),
        vec!["status.in_sync=true".to_owned()],
    );

    let mut noop = OperationBatch::default();
    for index in 0..5 {
        let measured = run_yy_operation(
            proxy,
            fixture.yy_command(&["--volume", path_str(&paths.config), "sync", "run"]),
            &format!("Sync no-op {}", index + 1),
        );
        noop.push(measured);
    }
    assert_sync_in_sync(&fixture, &paths, "after Sync no-op runs");
    noop.record(
        recorder,
        context,
        "sync_noop",
        publication,
        scale,
        dataset.clone(),
        completion,
        space_snapshot(&fixture, Some(&paths.directory), Some(&paths.state), None),
        vec!["status.in_sync=true".to_owned()],
    );

    let edit = paths.directory.join("dir-0000/file-0000");
    let mut edits = OperationBatch::default();
    for index in 0..3 {
        fs::write(
            &edit,
            payload(1024, 0x40 + u8::try_from(index).expect("three edits")),
        )
        .expect("write one Sync edit");
        let measured = run_yy_operation(
            proxy,
            fixture.yy_command(&["--volume", path_str(&paths.config), "sync", "run"]),
            &format!("Sync single edit {}", index + 1),
        );
        edits.push(measured);
    }
    assert_sync_in_sync(&fixture, &paths, "after Sync edits");
    edits.record(
        recorder,
        context,
        "sync_single_edit",
        publication,
        scale,
        dataset.clone(),
        completion,
        space_snapshot(&fixture, Some(&paths.directory), Some(&paths.state), None),
        vec!["status.in_sync=true".to_owned()],
    );

    let mut source = edit.clone();
    let mut destination = paths.directory.join("dir-0000/file-renamed");
    let mut renames = OperationBatch::default();
    for index in 0..3 {
        fs::rename(&source, &destination).unwrap_or_else(|error| {
            panic!(
                "rename Sync benchmark path {} to {}: {error}",
                source.display(),
                destination.display()
            )
        });
        let measured = run_yy_operation(
            proxy,
            fixture.yy_command(&["--volume", path_str(&paths.config), "sync", "run"]),
            &format!("Sync rename {}", index + 1),
        );
        renames.push(measured);
        std::mem::swap(&mut source, &mut destination);
    }
    assert_sync_in_sync(&fixture, &paths, "after Sync renames");
    renames.record(
        recorder,
        context,
        "sync_rename",
        publication,
        scale,
        dataset,
        completion,
        space_snapshot(&fixture, Some(&paths.directory), Some(&paths.state), None),
        vec!["status.in_sync=true".to_owned()],
    );
}

fn run_fuse_matrix(
    context: &RunContext,
    proxy: &Arc<CountingProxy>,
    publication: Publication,
    scale: Scale,
    recorder: &mut Recorder,
) {
    let fixture = Fixture::new_with_storage_endpoint(publication, Some(proxy.endpoint()));
    let size = scale.file_size();
    let bytes = payload(size, 0x5a);
    fixture.remote_create_file("large.bin", &bytes, false);
    drop(bytes);
    let (config, staging, mountpoint) = mount_paths(
        &fixture,
        &format!("fuse-{}-{}", publication.as_str(), scale.as_str()),
    );
    fs::create_dir(&mountpoint).expect("create benchmark mountpoint");
    let mounted_at = Instant::now();
    let mut mounted = FuseMount::start(&fixture, &config, &mountpoint);
    let mount_ready_ms = mounted_at.elapsed().as_secs_f64() * 1000.0;
    let dataset = Dataset {
        kind: "single_large_file".to_owned(),
        files: 1,
        directories: 0,
        logical_bytes: size as u64,
        file_size_bytes: Some(size as u64),
        range_bytes: Some(RANGE_BYTES as u64),
        writers: None,
    };
    recorder.record(
        context,
        "fuse_mount_ready",
        publication,
        scale,
        dataset.clone(),
        "yy mount process reported the fuse.yinyang mount ready",
        vec![mount_ready_ms],
        IoSnapshot::default(),
        NetworkSnapshot::default(),
        vec![],
        space_snapshot(&fixture, None, None, Some(&staging)),
        vec!["mount type=fuse.yinyang".to_owned()],
    );

    let large = mountpoint.join("large.bin");
    let cold = measure_mount_operation(proxy, mounted.pid(), || {
        let file = File::open(&large).expect("cold open large FUSE file");
        let mut value = vec![0_u8; RANGE_BYTES];
        read_exact_at(&file, &mut value, (size / 2) as u64);
        drop(file);
        value
    });
    let cold_bytes = cold.value;
    assert_eq!(cold_bytes, payload(RANGE_BYTES, 0x5a));
    recorder.record(
        context,
        "fuse_cold_open_range_read",
        publication,
        scale,
        dataset.clone(),
        "open materialized the whole file before the 4 KiB range read returned",
        vec![cold.latency_ms],
        cold.io,
        cold.network,
        vec![],
        space_snapshot(&fixture, None, None, Some(&staging)),
        vec!["range bytes verified".to_owned()],
    );

    let warm_file = File::open(&large).expect("warm open large FUSE file");
    let mut warm = OperationBatch::default();
    for index in 0..5 {
        let offset = ((index * RANGE_BYTES) as u64).min((size - RANGE_BYTES) as u64);
        let measured = measure_mount_operation(proxy, mounted.pid(), || {
            let mut value = vec![0_u8; RANGE_BYTES];
            read_exact_at(&warm_file, &mut value, offset);
            assert_eq!(value, payload(RANGE_BYTES, 0x5a));
        });
        warm.push(measured);
    }
    drop(warm_file);
    warm.record(
        recorder,
        context,
        "fuse_warm_range_read",
        publication,
        scale,
        dataset.clone(),
        "five 4 KiB reads on one already-open descriptor",
        space_snapshot(&fixture, None, None, Some(&staging)),
        vec!["range bytes verified".to_owned()],
    );

    let mut expected_large = payload(size, 0x5a);
    let mut sparse = OperationBatch::default();
    let mut spaces = Vec::new();
    for index in 0..4_u64 {
        let offset = (size as u64 - RANGE_BYTES as u64) / 2 + index * RANGE_BYTES as u64;
        let value = payload(
            RANGE_BYTES,
            0x80 + u8::try_from(index).expect("four updates"),
        );
        expected_large[offset as usize..offset as usize + RANGE_BYTES].copy_from_slice(&value);
        let measured = measure_mount_operation(proxy, mounted.pid(), || {
            let file = OpenOptions::new()
                .write(true)
                .open(&large)
                .expect("open large FUSE file for sparse update");
            write_all_at(&file, &value, offset);
            file.sync_all().expect("remote fsync after sparse update");
            drop(file);
        });
        sparse.push(measured);
        spaces.push(space_snapshot(&fixture, None, None, Some(&staging)));
    }
    assert_eq!(
        remote_file_bytes(&fixture, "large.bin"),
        expected_large,
        "remote content did not reflect every sparse update"
    );
    sparse.record_with_spaces(
        recorder,
        context,
        "fuse_sparse_update",
        publication,
        scale,
        dataset.clone(),
        "each 4 KiB write was followed by fsync and descriptor close, which acknowledged remote publication",
        spaces,
        vec![
            "all four sparse writes fsync returned success".to_owned(),
            "space series records retained remote object growth".to_owned(),
        ],
    );

    mounted.unmount();
    let mut restarted = FuseMount::start(&fixture, &config, &mountpoint);
    let restart = measure_mount_operation(proxy, restarted.pid(), || {
        let file = File::open(&large).expect("open after FUSE restart");
        let verify_offset = (size as u64 - RANGE_BYTES as u64) / 2 - RANGE_BYTES as u64;
        let mut value = vec![0_u8; 6 * RANGE_BYTES];
        read_exact_at(&file, &mut value, verify_offset);
        drop(file);
        (verify_offset, value)
    });
    let (verify_offset, restart_bytes) = restart.value;
    assert_eq!(
        restart_bytes,
        expected_large[verify_offset as usize..verify_offset as usize + 6 * RANGE_BYTES],
        "restarted mount did not preserve sparse bytes and adjacent untouched bytes"
    );
    recorder.record(
        context,
        "fuse_restart_cold_open_range_read",
        publication,
        scale,
        dataset,
        "new yy mount process, then cold open and 4 KiB range read",
        vec![restart.latency_ms],
        restart.io,
        restart.network,
        vec![],
        space_snapshot(&fixture, None, None, Some(&staging)),
        vec!["restarted mount served the original range bytes".to_owned()],
    );
    restarted.unmount();
}

fn run_concurrent_matrix(
    context: &RunContext,
    proxy: &Arc<CountingProxy>,
    publication: Publication,
    scale: Scale,
    recorder: &mut Recorder,
) {
    let fixture = Fixture::new_with_storage_endpoint(publication, Some(proxy.endpoint()));
    fixture.remote_create_directory("concurrent");
    let (config, staging, mountpoint) = mount_paths(
        &fixture,
        &format!("concurrent-{}-{}", publication.as_str(), scale.as_str()),
    );
    fs::create_dir(&mountpoint).expect("create concurrent mountpoint");
    let mut mounted = FuseMount::start(&fixture, &config, &mountpoint);
    let writers = scale.writers();
    let writer_bytes = 256 * 1024;
    let dataset = Dataset {
        kind: "concurrent_distinct_files".to_owned(),
        files: writers,
        directories: 1,
        logical_bytes: (writers * writer_bytes) as u64,
        file_size_bytes: Some(writer_bytes as u64),
        range_bytes: None,
        writers: Some(writers),
    };
    let expected = (0..writers)
        .map(|index| {
            payload(
                writer_bytes,
                0x20 + u8::try_from(index).expect("small writer set"),
            )
        })
        .collect::<Vec<_>>();
    let start = Arc::new((Mutex::new(false), Condvar::new()));
    let mut threads = Vec::with_capacity(writers);
    for index in 0..writers {
        let start = start.clone();
        let path = mountpoint.join(format!("concurrent/writer-{index:02}.bin"));
        let value = expected[index].clone();
        threads.push(thread::spawn(move || {
            let (started, ready) = &*start;
            let mut started = started
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            while !*started {
                started = ready
                    .wait(started)
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
            }
            drop(started);
            let started = Instant::now();
            let mut file = File::create(path).expect("create concurrent FUSE file");
            file.write_all(&value).expect("write concurrent FUSE file");
            file.sync_all().expect("fsync concurrent FUSE file");
            drop(file);
            started.elapsed().as_secs_f64() * 1000.0
        }));
    }
    let net_before = proxy.snapshot();
    let io_before = read_proc_io(mounted.pid());
    let started = Instant::now();
    {
        let (release, ready) = &*start;
        *release
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = true;
        ready.notify_all();
    }
    let mut samples = Vec::with_capacity(writers);
    for thread in threads {
        samples.push(thread.join().expect("concurrent FUSE writer panicked"));
    }
    let wall_ms = started.elapsed().as_secs_f64() * 1000.0;
    let network = proxy.snapshot().since(net_before);
    let io = read_proc_io(mounted.pid()).since(io_before);
    let remote = fixture.remote_tree().expect("read concurrent remote tree");
    for (index, expected) in expected.iter().enumerate() {
        match remote.get(&format!("concurrent/writer-{index:02}.bin")) {
            Some(fixture::RemoteEntry::File { bytes, .. }) => {
                assert_eq!(bytes, expected, "concurrent writer {index} remote bytes")
            }
            value => panic!("concurrent writer {index} has unexpected remote state: {value:?}"),
        }
    }
    recorder.record(
        context,
        "fuse_concurrent_writers",
        publication,
        scale,
        dataset,
        "all writer fsync calls returned; remote bytes were verified after the measured interval",
        samples,
        io,
        network,
        vec![],
        space_snapshot(&fixture, None, None, Some(&staging)),
        vec![
            format!("wall_ms={wall_ms:.3}"),
            "all writer files matched their complete expected remote bytes".to_owned(),
        ],
    );
    mounted.unmount();
}

fn run_publish_restore_matrix(
    context: &RunContext,
    proxy: &Arc<CountingProxy>,
    scale: Scale,
    recorder: &mut Recorder,
) {
    let fixture = Fixture::new_with_storage_endpoint(Publication::Object, Some(proxy.endpoint()));
    let source = fixture.path(format!("publish-source-{}", scale.as_str()));
    populate_small_tree(&source, scale);
    let dataset = tree_dataset(&source);
    let publish = run_yy_operation(
        proxy,
        fixture.yy_command(&["publish", path_str(&source), "--replace"]),
        "one-shot publish",
    );
    recorder.record(
        context,
        "publish_directory",
        Publication::Object,
        scale,
        dataset.clone(),
        "yy publish returned after commit acknowledgement",
        vec![publish.latency_ms],
        publish.io,
        publish.network,
        vec![],
        space_snapshot(&fixture, Some(&source), None, None),
        vec!["object-authority one-shot transfer".to_owned()],
    );

    let restored = fixture.path(format!("publish-restored-{}", scale.as_str()));
    let restore = run_yy_operation(
        proxy,
        fixture.yy_command(&["restore", path_str(&restored)]),
        "one-shot restore",
    );
    assert_eq!(
        fixture::local_tree(&source).expect("read publish source"),
        fixture::local_tree(&restored).expect("read restored tree")
    );
    recorder.record(
        context,
        "restore_directory",
        Publication::Object,
        scale,
        dataset,
        "yy restore returned after writing and verifying the complete local tree",
        vec![restore.latency_ms],
        restore.io,
        restore.network,
        vec![],
        space_snapshot(&fixture, Some(&restored), None, None),
        vec!["restored tree matched source bytes and metadata".to_owned()],
    );
}

fn populate_small_tree(root: &Path, scale: Scale) {
    fs::create_dir_all(root).expect("create Sync benchmark root");
    for directory in 0..scale.directories() {
        fs::create_dir_all(root.join(format!("dir-{directory:04}")))
            .expect("create Sync benchmark directory");
    }
    for index in 0..scale.files() {
        let directory = index % scale.directories();
        let path = root.join(format!("dir-{directory:04}/file-{index:04}"));
        fs::write(
            path,
            payload(1024, 0x10 + u8::try_from(index % 16).expect("file pattern")),
        )
        .expect("write Sync benchmark file");
    }
}

fn tree_dataset(root: &Path) -> Dataset {
    let (files, directories, bytes) = tree_counts(root);
    Dataset {
        kind: "small_file_tree".to_owned(),
        files,
        directories,
        logical_bytes: bytes,
        file_size_bytes: Some(1024),
        range_bytes: None,
        writers: None,
    }
}

fn tree_counts(root: &Path) -> (usize, usize, u64) {
    fn walk(path: &Path, files: &mut usize, directories: &mut usize, bytes: &mut u64) {
        let metadata = fs::symlink_metadata(path).expect("read benchmark tree metadata");
        if metadata.is_dir() {
            *directories += 1;
            for entry in fs::read_dir(path).expect("read benchmark directory") {
                walk(
                    &entry.expect("read benchmark entry").path(),
                    files,
                    directories,
                    bytes,
                );
            }
        } else if metadata.is_file() {
            *files += 1;
            *bytes = bytes.saturating_add(metadata.len());
        }
    }
    let mut files = 0;
    let mut directories = 0;
    let mut bytes = 0;
    walk(root, &mut files, &mut directories, &mut bytes);
    (files, directories, bytes)
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

fn assert_sync_in_sync(fixture: &Fixture, paths: &SyncPaths, context: &str) {
    let status = fixture.sync_status(paths);
    assert!(
        status.summary.in_sync,
        "{context}: Sync status was not in_sync: {status:#?}"
    );
}

fn remote_file_bytes(fixture: &Fixture, path: &str) -> Vec<u8> {
    match fixture
        .remote_tree()
        .expect("read remote tree for content verification")
        .remove(path)
    {
        Some(fixture::RemoteEntry::File { bytes, .. }) => bytes,
        Some(fixture::RemoteEntry::Directory { .. }) => panic!("{path} is a remote directory"),
        None => panic!("{path} is absent from the remote tree"),
    }
}

fn mount_paths(fixture: &Fixture, name: &str) -> (PathBuf, PathBuf, PathBuf) {
    let staging = fixture.path(format!("{name}-state"));
    let mountpoint = fixture.path(format!("{name}-mount"));
    let config = fixture.path(format!("{name}.json"));
    let mut value = serde_json::json!({
        "name": name,
        "model": "managed",
        "access": "mount",
        "frontend": "fuse",
        "publication": fixture.publication.as_str(),
        "storage_profile": "minio",
        "staging": staging,
        "read_only": false,
        "require": [],
    });
    if fixture.publication == Publication::Service {
        value["service_address"] = serde_json::Value::String(fixture.service_address().to_string());
    }
    fs::write(
        &config,
        serde_json::to_vec_pretty(&value).expect("serialize benchmark mount config"),
    )
    .expect("write benchmark mount config");
    (config, staging, mountpoint)
}

struct FuseMount {
    child: Child,
    stdout: Option<JoinHandle<Vec<u8>>>,
    stderr: Option<JoinHandle<Vec<u8>>>,
    mountpoint: PathBuf,
    finished: bool,
}

impl FuseMount {
    fn start(fixture: &Fixture, config: &Path, mountpoint: &Path) -> Self {
        let mut command =
            fixture.yy_command(&["--volume", path_str(config), "mount", path_str(mountpoint)]);
        command.stdout(Stdio::piped()).stderr(Stdio::piped());
        let mut child = command.spawn().expect("spawn benchmark yy mount");
        let stdout = child.stdout.take().expect("mount stdout is piped");
        let stderr = child.stderr.take().expect("mount stderr is piped");
        let mut mounted = Self {
            child,
            stdout: Some(thread::spawn(move || read_all(stdout))),
            stderr: Some(thread::spawn(move || read_all(stderr))),
            mountpoint: mountpoint.to_path_buf(),
            finished: false,
        };
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            if mount_type(mountpoint).as_deref() == Some("fuse.yinyang") {
                return mounted;
            }
            if let Some(status) = mounted.child.try_wait().expect("wait for yy mount") {
                let stderr = mounted.join_stderr();
                panic!(
                    "yy mount exited before reaching fuse.yinyang with {status}:\n{}",
                    String::from_utf8_lossy(&stderr)
                );
            }
            if Instant::now() >= deadline {
                let stderr = mounted.join_stderr();
                panic!(
                    "yy mount did not reach fuse.yinyang within 60s:\n{}",
                    String::from_utf8_lossy(&stderr)
                );
            }
            thread::sleep(Duration::from_millis(25));
        }
    }

    fn pid(&self) -> u32 {
        self.child.id()
    }

    fn unmount(&mut self) {
        if self.finished {
            return;
        }
        let output = Command::new("fusermount3")
            .args(["-u", path_str(&self.mountpoint)])
            .output()
            .expect("run fusermount3");
        assert!(
            output.status.success(),
            "fusermount3 failed: {}\n{}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        );
        let status = wait_child(&mut self.child, Duration::from_secs(30));
        let stderr = self.join_stderr();
        assert!(
            status.success(),
            "yy mount exited with {status} after unmount:\n{}",
            String::from_utf8_lossy(&stderr)
        );
        self.join_stdout();
        self.finished = true;
    }

    fn join_stderr(&mut self) -> Vec<u8> {
        self.stderr
            .take()
            .and_then(|thread| thread.join().ok())
            .unwrap_or_default()
    }

    fn join_stdout(&mut self) {
        let _ = self.stdout.take().map(JoinHandle::join);
    }
}

impl Drop for FuseMount {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = Command::new("fusermount3")
            .args(["-u", "-z", path_str(&self.mountpoint)])
            .output();
        self.join_stdout();
        self.join_stderr();
    }
}

fn mount_type(mountpoint: &Path) -> Option<String> {
    let mounts = fs::read_to_string("/proc/mounts").expect("read /proc/mounts");
    mounts.lines().find_map(|line| {
        let mut fields = line.split_whitespace();
        let _source = fields.next()?;
        let point = fields.next()?;
        let kind = fields.next()?;
        (Path::new(point) == mountpoint).then(|| kind.to_owned())
    })
}

fn wait_child(child: &mut Child, timeout: Duration) -> ExitStatus {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(status) = child.try_wait().expect("wait for benchmark child") {
            return status;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("benchmark child did not exit within {timeout:?}");
        }
        thread::sleep(Duration::from_millis(25));
    }
}

fn read_exact_at(file: &File, mut buffer: &mut [u8], mut offset: u64) {
    while !buffer.is_empty() {
        let count = file.read_at(buffer, offset).expect("pread benchmark file");
        assert_ne!(count, 0, "unexpected EOF at offset {offset}");
        offset += count as u64;
        buffer = &mut buffer[count..];
    }
}

fn write_all_at(file: &File, mut buffer: &[u8], mut offset: u64) {
    while !buffer.is_empty() {
        let count = file
            .write_at(buffer, offset)
            .expect("pwrite benchmark file");
        assert_ne!(count, 0, "unexpected zero-byte write at offset {offset}");
        offset += count as u64;
        buffer = &buffer[count..];
    }
}

struct Measurement<T> {
    value: T,
    latency_ms: f64,
    io: IoSnapshot,
    network: NetworkSnapshot,
}

fn measure_mount_operation<T>(
    proxy: &Arc<CountingProxy>,
    pid: u32,
    operation: impl FnOnce() -> T,
) -> Measurement<T> {
    let net_before = proxy.snapshot();
    let io_before = read_proc_io(pid);
    let started = Instant::now();
    let value = operation();
    let latency_ms = started.elapsed().as_secs_f64() * 1000.0;
    Measurement {
        value,
        latency_ms,
        io: read_proc_io(pid).since(io_before),
        network: proxy.snapshot().since(net_before),
    }
}

fn run_yy_operation(
    proxy: &Arc<CountingProxy>,
    command: Command,
    context: &str,
) -> Measurement<()> {
    let net_before = proxy.snapshot();
    let started = Instant::now();
    let output = run_observed(command, Duration::from_secs(300));
    let latency_ms = started.elapsed().as_secs_f64() * 1000.0;
    assert!(
        output.status.success(),
        "{context} failed with {}:\nstdout:\n{}\nstderr:\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    Measurement {
        value: (),
        latency_ms,
        io: output.io,
        network: proxy.snapshot().since(net_before),
    }
}

struct ObservedOutput {
    status: ExitStatus,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
    io: IoSnapshot,
}

fn run_observed(mut command: Command, timeout: Duration) -> ObservedOutput {
    command.stdout(Stdio::piped()).stderr(Stdio::piped());
    let mut child = command.spawn().expect("spawn benchmark yy command");
    let stdout = child.stdout.take().expect("stdout is piped");
    let stderr = child.stderr.take().expect("stderr is piped");
    let stdout_thread = thread::spawn(move || read_all(stdout));
    let stderr_thread = thread::spawn(move || read_all(stderr));
    let deadline = Instant::now() + timeout;
    let mut io = IoSnapshot::default();
    let status = loop {
        let current = read_proc_io(child.id());
        if current.rchar != 0 || current.wchar != 0 {
            io = current;
        }
        if let Some(status) = child.try_wait().expect("wait for benchmark yy command") {
            break status;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("benchmark yy command did not exit within {timeout:?}");
        }
        thread::sleep(Duration::from_millis(5));
    };
    ObservedOutput {
        status,
        stdout: stdout_thread.join().expect("stdout reader panicked"),
        stderr: stderr_thread.join().expect("stderr reader panicked"),
        io,
    }
}

fn read_all(mut reader: impl Read) -> Vec<u8> {
    let mut bytes = Vec::new();
    let _ = reader.read_to_end(&mut bytes);
    bytes
}

#[derive(Clone, Copy, Debug, Default, Serialize)]
struct IoSnapshot {
    rchar: u64,
    wchar: u64,
    syscr: u64,
    syscw: u64,
    read_bytes: u64,
    write_bytes: u64,
}

impl IoSnapshot {
    fn since(self, before: Self) -> Self {
        Self {
            rchar: self.rchar.saturating_sub(before.rchar),
            wchar: self.wchar.saturating_sub(before.wchar),
            syscr: self.syscr.saturating_sub(before.syscr),
            syscw: self.syscw.saturating_sub(before.syscw),
            read_bytes: self.read_bytes.saturating_sub(before.read_bytes),
            write_bytes: self.write_bytes.saturating_sub(before.write_bytes),
        }
    }

    fn add(&mut self, other: Self) {
        self.rchar = self.rchar.saturating_add(other.rchar);
        self.wchar = self.wchar.saturating_add(other.wchar);
        self.syscr = self.syscr.saturating_add(other.syscr);
        self.syscw = self.syscw.saturating_add(other.syscw);
        self.read_bytes = self.read_bytes.saturating_add(other.read_bytes);
        self.write_bytes = self.write_bytes.saturating_add(other.write_bytes);
    }
}

fn read_proc_io(pid: u32) -> IoSnapshot {
    let Ok(text) = fs::read_to_string(format!("/proc/{pid}/io")) else {
        return IoSnapshot::default();
    };
    let mut value = IoSnapshot::default();
    for line in text.lines() {
        let Some((key, raw)) = line.split_once(':') else {
            continue;
        };
        let number = raw.trim().parse::<u64>().unwrap_or_default();
        match key {
            "rchar" => value.rchar = number,
            "wchar" => value.wchar = number,
            "syscr" => value.syscr = number,
            "syscw" => value.syscw = number,
            "read_bytes" => value.read_bytes = number,
            "write_bytes" => value.write_bytes = number,
            _ => {}
        }
    }
    value
}

#[derive(Default)]
struct OperationBatch {
    latencies: Vec<f64>,
    io: IoSnapshot,
    network: NetworkSnapshot,
}

impl OperationBatch {
    fn push(&mut self, value: Measurement<()>) {
        self.latencies.push(value.latency_ms);
        self.io.add(value.io);
        self.network.add(value.network);
    }

    fn record(
        self,
        recorder: &mut Recorder,
        context: &RunContext,
        scenario: &str,
        publication: Publication,
        scale: Scale,
        dataset: Dataset,
        completion: &str,
        space: SpaceSnapshot,
        invariants: Vec<String>,
    ) {
        self.record_with_spaces(
            recorder,
            context,
            scenario,
            publication,
            scale,
            dataset,
            completion,
            vec![space],
            invariants,
        );
    }

    fn record_with_spaces(
        self,
        recorder: &mut Recorder,
        context: &RunContext,
        scenario: &str,
        publication: Publication,
        scale: Scale,
        dataset: Dataset,
        completion: &str,
        spaces: Vec<SpaceSnapshot>,
        invariants: Vec<String>,
    ) {
        let space = spaces.last().cloned().unwrap_or_default();
        recorder.record(
            context,
            scenario,
            publication,
            scale,
            dataset,
            completion,
            self.latencies,
            self.io,
            self.network,
            spaces,
            space,
            invariants,
        );
    }
}

#[derive(Clone, Default, Serialize)]
struct NetworkSnapshot {
    connections: u64,
    requests: u64,
    request_bytes: u64,
    response_bytes: u64,
    get: u64,
    head: u64,
    put: u64,
    post: u64,
    delete: u64,
    other: u64,
}

impl NetworkSnapshot {
    fn since(self, before: Self) -> Self {
        Self {
            connections: self.connections.saturating_sub(before.connections),
            requests: self.requests.saturating_sub(before.requests),
            request_bytes: self.request_bytes.saturating_sub(before.request_bytes),
            response_bytes: self.response_bytes.saturating_sub(before.response_bytes),
            get: self.get.saturating_sub(before.get),
            head: self.head.saturating_sub(before.head),
            put: self.put.saturating_sub(before.put),
            post: self.post.saturating_sub(before.post),
            delete: self.delete.saturating_sub(before.delete),
            other: self.other.saturating_sub(before.other),
        }
    }

    fn add(&mut self, other: Self) {
        self.connections = self.connections.saturating_add(other.connections);
        self.requests = self.requests.saturating_add(other.requests);
        self.request_bytes = self.request_bytes.saturating_add(other.request_bytes);
        self.response_bytes = self.response_bytes.saturating_add(other.response_bytes);
        self.get = self.get.saturating_add(other.get);
        self.head = self.head.saturating_add(other.head);
        self.put = self.put.saturating_add(other.put);
        self.post = self.post.saturating_add(other.post);
        self.delete = self.delete.saturating_add(other.delete);
        self.other = self.other.saturating_add(other.other);
    }
}

#[derive(Default)]
struct ProxyCounters {
    connections: AtomicU64,
    requests: AtomicU64,
    request_bytes: AtomicU64,
    response_bytes: AtomicU64,
    get: AtomicU64,
    head: AtomicU64,
    put: AtomicU64,
    post: AtomicU64,
    delete: AtomicU64,
    other: AtomicU64,
}

struct CountingProxy {
    address: SocketAddr,
    counters: Arc<ProxyCounters>,
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

impl CountingProxy {
    fn start(upstream: SocketAddr) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind counting S3 proxy");
        listener
            .set_nonblocking(true)
            .expect("make counting proxy listener nonblocking");
        let address = listener.local_addr().expect("counting proxy address");
        let counters = Arc::new(ProxyCounters::default());
        let stop = Arc::new(AtomicBool::new(false));
        let thread_counters = counters.clone();
        let thread_stop = stop.clone();
        let thread = thread::spawn(move || {
            while !thread_stop.load(Ordering::SeqCst) {
                match listener.accept() {
                    Ok((client, _)) => {
                        thread_counters.connections.fetch_add(1, Ordering::Relaxed);
                        let counters = thread_counters.clone();
                        thread::spawn(move || proxy_connection(client, upstream, counters));
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(5));
                    }
                    Err(_) => break,
                }
            }
        });
        Self {
            address,
            counters,
            stop,
            thread: Some(thread),
        }
    }

    fn endpoint(&self) -> String {
        format!("http://{}", self.address)
    }

    fn snapshot(&self) -> NetworkSnapshot {
        NetworkSnapshot {
            connections: self.counters.connections.load(Ordering::Relaxed),
            requests: self.counters.requests.load(Ordering::Relaxed),
            request_bytes: self.counters.request_bytes.load(Ordering::Relaxed),
            response_bytes: self.counters.response_bytes.load(Ordering::Relaxed),
            get: self.counters.get.load(Ordering::Relaxed),
            head: self.counters.head.load(Ordering::Relaxed),
            put: self.counters.put.load(Ordering::Relaxed),
            post: self.counters.post.load(Ordering::Relaxed),
            delete: self.counters.delete.load(Ordering::Relaxed),
            other: self.counters.other.load(Ordering::Relaxed),
        }
    }
}

impl Drop for CountingProxy {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        let _ = TcpStream::connect(self.address);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

fn proxy_connection(client: TcpStream, upstream: SocketAddr, counters: Arc<ProxyCounters>) {
    let Ok(server) = TcpStream::connect(upstream) else {
        return;
    };
    let (Ok(mut client_read), Ok(mut server_write)) = (client.try_clone(), server.try_clone())
    else {
        return;
    };
    let (Ok(mut server_read), Ok(mut client_write)) = (server.try_clone(), client.try_clone())
    else {
        return;
    };
    let request_counters = counters.clone();
    let request =
        thread::spawn(move || pump(&mut client_read, &mut server_write, &request_counters, true));
    let response =
        thread::spawn(move || pump(&mut server_read, &mut client_write, &counters, false));
    let _ = request.join();
    let _ = response.join();
    let _ = client.shutdown(Shutdown::Both);
}

fn pump(reader: &mut TcpStream, writer: &mut TcpStream, counters: &ProxyCounters, direction: bool) {
    let mut buffer = vec![0_u8; CHUNK];
    let mut scanner = if direction {
        Some(RequestScanner::default())
    } else {
        None
    };
    loop {
        let count = match reader.read(&mut buffer) {
            Ok(0) => break,
            Ok(count) => count,
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(_) => break,
        };
        if direction {
            counters
                .request_bytes
                .fetch_add(count as u64, Ordering::Relaxed);
            scanner
                .as_mut()
                .expect("request scanner")
                .scan(&buffer[..count], counters);
        } else {
            counters
                .response_bytes
                .fetch_add(count as u64, Ordering::Relaxed);
        }
        if writer.write_all(&buffer[..count]).is_err() {
            break;
        }
    }
    let _ = writer.shutdown(Shutdown::Write);
}

#[derive(Clone, Copy, Default)]
enum ScanState {
    #[default]
    RequestLine,
    Headers,
    Body(u64),
    ChunkSize,
    ChunkData(u64),
    ChunkCrlf(u8),
    Trailers,
}

#[derive(Default)]
struct RequestScanner {
    state: ScanState,
    line: Vec<u8>,
    content_length: Option<u64>,
    chunked: bool,
}

impl RequestScanner {
    fn scan(&mut self, bytes: &[u8], counters: &ProxyCounters) {
        let mut offset = 0;
        while offset < bytes.len() {
            match self.state {
                ScanState::Body(remaining) | ScanState::ChunkData(remaining) => {
                    let count = remaining.min((bytes.len() - offset) as u64) as usize;
                    offset += count;
                    if count as u64 == remaining {
                        self.state = if matches!(self.state, ScanState::Body(_)) {
                            ScanState::RequestLine
                        } else {
                            ScanState::ChunkCrlf(2)
                        };
                    } else {
                        self.state = match self.state {
                            ScanState::Body(_) => ScanState::Body(remaining - count as u64),
                            ScanState::ChunkData(_) => {
                                ScanState::ChunkData(remaining - count as u64)
                            }
                            _ => unreachable!(),
                        };
                    }
                }
                ScanState::ChunkCrlf(remaining) => {
                    let count = usize::from(remaining).min(bytes.len() - offset);
                    offset += count;
                    let remaining = remaining - count as u8;
                    self.state = if remaining == 0 {
                        ScanState::ChunkSize
                    } else {
                        ScanState::ChunkCrlf(remaining)
                    };
                }
                ScanState::RequestLine
                | ScanState::Headers
                | ScanState::ChunkSize
                | ScanState::Trailers => {
                    let end = bytes[offset..]
                        .iter()
                        .position(|byte| *byte == b'\n')
                        .map(|position| offset + position);
                    if let Some(end) = end {
                        self.line.extend_from_slice(&bytes[offset..end]);
                        let line = std::mem::take(&mut self.line);
                        let line = line.strip_suffix(b"\r").unwrap_or(&line);
                        self.process_line(line, counters);
                        offset = end + 1;
                    } else {
                        self.line.extend_from_slice(&bytes[offset..]);
                        offset = bytes.len();
                        if self.line.len() > 16 * 1024 {
                            self.line.clear();
                            self.state = ScanState::RequestLine;
                        }
                    }
                }
            }
        }
    }

    fn process_line(&mut self, line: &[u8], counters: &ProxyCounters) {
        match self.state {
            ScanState::RequestLine => {
                count_request_line(line, counters);
                self.content_length = None;
                self.chunked = false;
                self.state = ScanState::Headers;
            }
            ScanState::Headers if line.is_empty() => {
                self.state = if self.chunked {
                    ScanState::ChunkSize
                } else if let Some(length) = self.content_length.filter(|length| *length > 0) {
                    ScanState::Body(length)
                } else {
                    ScanState::RequestLine
                };
            }
            ScanState::Headers => {
                let header = String::from_utf8_lossy(line).to_ascii_lowercase();
                if let Some(value) = header.strip_prefix("content-length:") {
                    self.content_length = value.trim().parse().ok();
                } else if header.starts_with("transfer-encoding:") && header.contains("chunked") {
                    self.chunked = true;
                }
            }
            ScanState::ChunkSize => {
                let size = std::str::from_utf8(line)
                    .ok()
                    .and_then(|line| line.split(';').next())
                    .and_then(|size| u64::from_str_radix(size.trim(), 16).ok())
                    .unwrap_or_else(|| panic!("invalid HTTP chunk size in benchmark request"));
                self.state = if size == 0 {
                    ScanState::Trailers
                } else {
                    ScanState::ChunkData(size)
                };
            }
            ScanState::Trailers if line.is_empty() => {
                self.state = ScanState::RequestLine;
            }
            ScanState::Trailers => {}
            ScanState::Body(_) | ScanState::ChunkData(_) | ScanState::ChunkCrlf(_) => {
                unreachable!("fixed-width HTTP states do not process lines")
            }
        }
    }
}

fn count_request_line(line: &[u8], counters: &ProxyCounters) {
    if !line.windows(8).any(|window| window == b" HTTP/1.") {
        return;
    }
    let counter = if line.starts_with(b"GET ") {
        &counters.get
    } else if line.starts_with(b"HEAD ") {
        &counters.head
    } else if line.starts_with(b"PUT ") {
        &counters.put
    } else if line.starts_with(b"POST ") {
        &counters.post
    } else if line.starts_with(b"DELETE ") {
        &counters.delete
    } else if [
        b"OPTIONS ".as_slice(),
        b"PATCH ".as_slice(),
        b"CONNECT ".as_slice(),
        b"TRACE ".as_slice(),
    ]
    .iter()
    .any(|method| line.starts_with(method))
    {
        &counters.other
    } else {
        return;
    };
    counter.fetch_add(1, Ordering::Relaxed);
    counters.requests.fetch_add(1, Ordering::Relaxed);
}

#[derive(Clone, Default, Serialize)]
struct SpaceSnapshot {
    local_payload_bytes: u64,
    sync_state_bytes: u64,
    mount_staging_bytes: u64,
    metadata_database_bytes: u64,
    remote_stored_bytes: u64,
    remote_objects: u64,
    tracked_total_bytes: u64,
}

fn space_snapshot(
    fixture: &Fixture,
    local: Option<&Path>,
    sync_state: Option<&Path>,
    mount_staging: Option<&Path>,
) -> SpaceSnapshot {
    let local_payload_bytes = local.map(tree_bytes).unwrap_or_default();
    let sync_state_bytes = sync_state.map(tree_bytes).unwrap_or_default();
    let mount_staging_bytes = mount_staging.map(tree_bytes).unwrap_or_default();
    let metadata_database_bytes = ["metadata.db", "metadata.db-wal", "metadata.db-shm"]
        .into_iter()
        .filter_map(|name| fixture.temp.path().join(name).metadata().ok())
        .map(|metadata| metadata.len())
        .fold(0_u64, u64::saturating_add);
    let (remote_stored_bytes, remote_objects) = remote_storage(fixture);
    SpaceSnapshot {
        local_payload_bytes,
        sync_state_bytes,
        mount_staging_bytes,
        metadata_database_bytes,
        remote_stored_bytes,
        remote_objects,
        tracked_total_bytes: local_payload_bytes
            .saturating_add(sync_state_bytes)
            .saturating_add(mount_staging_bytes)
            .saturating_add(metadata_database_bytes)
            .saturating_add(remote_stored_bytes),
    }
}

fn tree_bytes(root: &Path) -> u64 {
    fn walk(path: &Path, total: &mut u64) {
        let Ok(metadata) = fs::symlink_metadata(path) else {
            return;
        };
        if metadata.is_dir() {
            if let Ok(entries) = fs::read_dir(path) {
                for entry in entries.flatten() {
                    walk(&entry.path(), total);
                }
            }
        } else if metadata.is_file() {
            *total = total.saturating_add(metadata.st_blocks().saturating_mul(512));
        }
    }
    let mut total = 0;
    walk(root, &mut total);
    total
}

fn remote_storage(fixture: &Fixture) -> (u64, u64) {
    let operator = fixture.operator();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build benchmark storage listing runtime");
    runtime.block_on(async move {
        let mut entries = operator
            .lister_with("")
            .recursive(true)
            .await
            .expect("list persistent benchmark storage");
        let mut bytes = 0_u64;
        let mut objects = 0_u64;
        while let Some(entry) = entries
            .try_next()
            .await
            .expect("read persistent benchmark object")
        {
            let metadata = entry.metadata();
            if metadata.is_file() {
                objects += 1;
                bytes = bytes.saturating_add(metadata.content_length());
            }
        }
        (bytes, objects)
    })
}

#[derive(Clone, Serialize)]
struct Dataset {
    kind: String,
    files: usize,
    directories: usize,
    logical_bytes: u64,
    file_size_bytes: Option<u64>,
    range_bytes: Option<u64>,
    writers: Option<usize>,
}

#[derive(Serialize)]
struct LatencyStats {
    p50_ms: f64,
    p95_ms: f64,
    min_ms: f64,
    max_ms: f64,
}

impl LatencyStats {
    fn new(samples: &[f64]) -> Self {
        assert!(!samples.is_empty(), "benchmark latency samples are empty");
        let mut sorted = samples.to_vec();
        sorted.sort_by(f64::total_cmp);
        Self {
            p50_ms: percentile(&sorted, 0.50),
            p95_ms: percentile(&sorted, 0.95),
            min_ms: sorted[0],
            max_ms: *sorted.last().expect("non-empty samples"),
        }
    }
}

fn percentile(sorted: &[f64], value: f64) -> f64 {
    let index = ((sorted.len() - 1) as f64 * value).ceil() as usize;
    sorted[index]
}

#[derive(Serialize)]
struct CostRecord {
    schema: &'static str,
    scenario: String,
    profile: String,
    publication: String,
    scale: String,
    supported: bool,
    limitation: Option<String>,
    source_sha: String,
    source_dirty: bool,
    binary_path: String,
    binary_blake3: String,
    host: String,
    rustc: String,
    dataset: Dataset,
    completion: String,
    iterations: usize,
    wall_ms: f64,
    latency: LatencyStats,
    local_process_io: IoSnapshot,
    local_process_io_note: String,
    network: NetworkSnapshot,
    space_series: Vec<SpaceSnapshot>,
    space: SpaceSnapshot,
    failures: u64,
    invariants: Vec<String>,
}

struct Recorder {
    output_path: PathBuf,
    output: File,
    summary_path: PathBuf,
    context: RunContext,
    records: Vec<CostRecord>,
}

impl Recorder {
    fn new(context: &RunContext, output: &Path, summary: &Path) -> Self {
        if let Some(parent) = output.parent() {
            fs::create_dir_all(parent).expect("create benchmark output directory");
        }
        if let Some(parent) = summary.parent() {
            fs::create_dir_all(parent).expect("create benchmark summary directory");
        }
        Self {
            output_path: output.to_path_buf(),
            output: File::create(output).expect("create benchmark JSONL output"),
            summary_path: summary.to_path_buf(),
            context: RunContext {
                profile: context.profile,
                source_sha: context.source_sha.clone(),
                source_dirty: context.source_dirty,
                binary_path: context.binary_path.clone(),
                binary_blake3: context.binary_blake3.clone(),
                host: context.host.clone(),
                rustc: context.rustc.clone(),
            },
            records: Vec::new(),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn record(
        &mut self,
        context: &RunContext,
        scenario: &str,
        publication: Publication,
        scale: Scale,
        dataset: Dataset,
        completion: &str,
        latencies: Vec<f64>,
        io: IoSnapshot,
        network: NetworkSnapshot,
        space_series: Vec<SpaceSnapshot>,
        space: SpaceSnapshot,
        invariants: Vec<String>,
    ) {
        assert!(!latencies.is_empty(), "{scenario} has no latency samples");
        let latency = LatencyStats::new(&latencies);
        let record = CostRecord {
            schema: RESULT_SCHEMA,
            scenario: scenario.to_owned(),
            profile: match context.profile {
                Profile::Early => "early",
                Profile::Full => "full",
            }
            .to_owned(),
            publication: publication.as_str().to_owned(),
            scale: scale.as_str().to_owned(),
            supported: true,
            limitation: None,
            source_sha: context.source_sha.clone(),
            source_dirty: context.source_dirty,
            binary_path: context.binary_path.clone(),
            binary_blake3: context.binary_blake3.clone(),
            host: context.host.clone(),
            rustc: context.rustc.clone(),
            dataset,
            completion: completion.to_owned(),
            iterations: latencies.len(),
            wall_ms: latencies.iter().sum(),
            latency,
            local_process_io: io,
            local_process_io_note: PROCESS_IO_NOTE.to_owned(),
            network,
            space_series,
            space,
            failures: 0,
            invariants,
        };
        serde_json::to_writer(&mut self.output, &record).expect("write benchmark JSONL record");
        self.output
            .write_all(b"\n")
            .expect("terminate benchmark JSONL record");
        self.output.flush().expect("flush benchmark JSONL output");
        println!(
            "linux-cost: {} publication={} scale={} p50={:.3}ms requests={} request_bytes={} response_bytes={}",
            record.scenario,
            record.publication,
            record.scale,
            record.latency.p50_ms,
            record.network.requests,
            record.network.request_bytes,
            record.network.response_bytes
        );
        self.records.push(record);
    }

    fn unsupported(
        &mut self,
        context: &RunContext,
        scenario: &str,
        publication: Publication,
        scale: Scale,
        limitation: &str,
    ) {
        let record = CostRecord {
            schema: RESULT_SCHEMA,
            scenario: scenario.to_owned(),
            profile: match context.profile {
                Profile::Early => "early",
                Profile::Full => "full",
            }
            .to_owned(),
            publication: publication.as_str().to_owned(),
            scale: scale.as_str().to_owned(),
            supported: false,
            limitation: Some(limitation.to_owned()),
            source_sha: context.source_sha.clone(),
            source_dirty: context.source_dirty,
            binary_path: context.binary_path.clone(),
            binary_blake3: context.binary_blake3.clone(),
            host: context.host.clone(),
            rustc: context.rustc.clone(),
            dataset: Dataset {
                kind: "not_run".to_owned(),
                files: 0,
                directories: 0,
                logical_bytes: 0,
                file_size_bytes: None,
                range_bytes: None,
                writers: None,
            },
            completion: "not applicable".to_owned(),
            iterations: 0,
            wall_ms: 0.0,
            latency: LatencyStats {
                p50_ms: 0.0,
                p95_ms: 0.0,
                min_ms: 0.0,
                max_ms: 0.0,
            },
            local_process_io: IoSnapshot::default(),
            local_process_io_note: PROCESS_IO_NOTE.to_owned(),
            network: NetworkSnapshot::default(),
            space_series: Vec::new(),
            space: SpaceSnapshot::default(),
            failures: 0,
            invariants: vec![limitation.to_owned()],
        };
        serde_json::to_writer(&mut self.output, &record).expect("write unsupported JSONL record");
        self.output
            .write_all(b"\n")
            .expect("terminate unsupported JSONL record");
        self.records.push(record);
    }

    fn finish(mut self) {
        self.output.flush().expect("flush benchmark JSONL output");
        let mut markdown = String::new();
        markdown.push_str("# Linux cost baseline\n\n");
        markdown.push_str(&format!(
            "- Source: `{}` (dirty: `{}`)\n- Binary: `{}`\n- Binary BLAKE3: `{}`\n- Host: {}\n- Rust: {}\n- Profile: {}\n\n",
            self.context.source_sha,
            self.context.source_dirty,
            self.context.binary_path,
            self.context.binary_blake3,
            self.context.host,
            self.context.rustc,
            match self.context.profile {
                Profile::Early => "early",
                Profile::Full => "full",
            }
        ));
        markdown.push_str(
            "| Scenario | Publication | Scale | Iters | p50 ms | p95 ms | Requests | Request MiB | Response MiB | Remote MiB | Staging MiB |\n",
        );
        markdown.push_str(
            "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n",
        );
        for record in &self.records {
            markdown.push_str(&format!(
                "| {} | {} | {} | {} | {:.3} | {:.3} | {} | {:.3} | {:.3} | {:.3} | {:.3} |\n",
                record.scenario,
                record.publication,
                record.scale,
                record.iterations,
                record.latency.p50_ms,
                record.latency.p95_ms,
                record.network.requests,
                record.network.request_bytes as f64 / 1024.0 / 1024.0,
                record.network.response_bytes as f64 / 1024.0 / 1024.0,
                record.space.remote_stored_bytes as f64 / 1024.0 / 1024.0,
                record.space.mount_staging_bytes as f64 / 1024.0 / 1024.0,
            ));
        }
        markdown.push_str(
            "\nRequest and byte values cover storage HTTP traffic through the counting proxy. Process I/O values are sampled lower bounds, not exact local scan or hash costs. Space series and completion semantics are preserved in the JSONL evidence.\n",
        );
        fs::write(&self.summary_path, markdown).expect("write benchmark Markdown summary");
        println!(
            "linux-cost: wrote {} records to {} and {}",
            self.records.len(),
            self.output_path.display(),
            self.summary_path.display()
        );
    }
}

fn s3_upstream() -> SocketAddr {
    let endpoint = std::env::var("YINYANG_S3_ENDPOINT").expect("YINYANG_S3_ENDPOINT is required");
    let raw = endpoint.strip_prefix("http://").unwrap_or_else(|| {
        panic!("benchmark counting proxy requires an http endpoint, got {endpoint}")
    });
    raw.trim_end_matches('/')
        .parse()
        .unwrap_or_else(|error| panic!("parse S3 endpoint {endpoint}: {error}"))
}

fn host_identity() -> String {
    let uname = command_line("uname", &["-a"]);
    let cpus = std::thread::available_parallelism()
        .map(|value| value.get())
        .unwrap_or_default();
    let memory = fs::read_to_string("/proc/meminfo")
        .ok()
        .and_then(|text| {
            text.lines()
                .find(|line| line.starts_with("MemTotal:"))
                .and_then(|line| line.split_whitespace().nth(1))
                .map(str::to_owned)
        })
        .unwrap_or_else(|| "unknown".to_owned());
    format!("{uname}; cpus={cpus}; mem_kib={memory}")
}

fn command_line(program: &str, args: &[&str]) -> String {
    let output = Command::new(program)
        .args(args)
        .output()
        .unwrap_or_else(|error| panic!("run {program} {args:?}: {error}"));
    assert!(
        output.status.success(),
        "{program} {args:?} failed: {}",
        output.status
    );
    String::from_utf8_lossy(&output.stdout).trim().to_owned()
}

fn blake3_file(path: &Path) -> String {
    let bytes = fs::read(path).expect("read benchmark binary");
    blake3::hash(&bytes).to_hex().to_string()
}

fn path_str(path: &Path) -> &str {
    path.to_str().expect("benchmark path is UTF-8")
}

#[cfg(test)]
mod tests {
    use super::{ProxyCounters, RequestScanner};
    use std::sync::atomic::Ordering;

    #[test]
    fn request_scanner_skips_content_length_and_chunked_bodies() {
        let counters = ProxyCounters::default();
        let body = b"PUT /not-a-request HTTP/1.1\r\n\r\n";
        let mut wire = format!(
            "PUT /object HTTP/1.1\r\nContent-Length: {}\r\n\r\n",
            body.len()
        )
        .into_bytes();
        wire.extend_from_slice(body);
        wire.extend_from_slice(
            b"POST /chunked HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n\
              4\r\nGET \r\n0\r\n\r\n\
              HEAD /next HTTP/1.1\r\n\r\n",
        );
        let mut scanner = RequestScanner::default();
        for byte in wire {
            scanner.scan(&[byte], &counters);
        }
        assert_eq!(counters.requests.load(Ordering::Relaxed), 3);
        assert_eq!(counters.put.load(Ordering::Relaxed), 1);
        assert_eq!(counters.post.load(Ordering::Relaxed), 1);
        assert_eq!(counters.head.load(Ordering::Relaxed), 1);
        assert_eq!(counters.get.load(Ordering::Relaxed), 0);
    }
}
