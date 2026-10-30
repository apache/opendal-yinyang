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
//! Loopback counting proxies in front of Silo and the metadata service supply
//! distinct request, connection, and byte counts without production hooks.

use std::collections::VecDeque;
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

use serde::{Deserialize, Serialize};

#[path = "linux_sync/harness.rs"]
mod fixture;

use fixture::{Fixture, Publication, SyncPaths};
use nix::sys::signal::{Signal, kill};
use nix::unistd::Pid;

const RESULT_SCHEMA: &str = "yinyang.linux-cost.v6";
const CHUNK: usize = 64 * 1024;
const BLOCK_BYTES: usize = 64 * 1024;
const RANGE_BYTES: usize = 4096;
const PROCESS_IO_NOTE: &str = "sampled from /proc/PID/io and /proc/PID/status while alive; terminal increments can be missed, so values are lower bounds, character I/O includes sockets, and RSS/HWM are process-wide rather than cache-only";
const REPEATED_RANDOM_SAMPLES: usize = 32;
const RANDOM_SEED: u64 = 0x79_59_4d_32_37_37_32;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Profile {
    Early,
    Full,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Workload {
    Full,
    ReadOnlyRange,
    RepeatedRandom,
    Incremental,
    LengthChanging,
}

impl Workload {
    fn as_str(self) -> &'static str {
        match self {
            Self::Full => "full",
            Self::ReadOnlyRange => "read-only-range",
            Self::RepeatedRandom => "repeated-random",
            Self::Incremental => "incremental",
            Self::LengthChanging => "length-changing",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum IncrementalExpectation {
    Baseline,
    Required,
}

impl IncrementalExpectation {
    fn as_str(self) -> &'static str {
        match self {
            Self::Baseline => "baseline",
            Self::Required => "required",
        }
    }
}

#[derive(Clone, Copy)]
enum PublicationFilter {
    All,
    Object,
    Service,
}

impl PublicationFilter {
    fn as_str(self) -> &'static str {
        match self {
            Self::All => "all",
            Self::Object => "object",
            Self::Service => "service",
        }
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
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
    workload: Workload,
    publication_filter: PublicationFilter,
    storage_backend: String,
    metadata_transport: String,
    network_source: String,
    range_cache_policy: String,
    expect_range_reuse: bool,
    incremental_expectation: String,
    source_sha: String,
    source_dirty: bool,
    binary_path: String,
    binary_blake3: String,
    host: String,
    rustc: String,
}

struct BenchFixture {
    fixture: Fixture,
    service_proxy: Option<CountingServiceProxy>,
}

impl BenchFixture {
    fn new(publication: Publication, storage_endpoint: String) -> Self {
        let mut fixture = Fixture::new_with_storage_endpoint(publication, Some(storage_endpoint));
        let service_proxy = if publication == Publication::Service {
            let proxy = CountingServiceProxy::start(fixture.service_address());
            fixture.set_service_client_address(proxy.address());
            Some(proxy)
        } else {
            None
        };
        Self {
            fixture,
            service_proxy,
        }
    }

    fn network<'a>(&'a self, storage: &'a CountingProxy) -> NetworkMeter<'a> {
        NetworkMeter {
            storage: NetworkSource::Proxy(storage),
            service: self.service_proxy.as_ref(),
        }
    }
}

#[derive(Clone, Copy)]
enum NetworkSource<'a> {
    Proxy(&'a CountingProxy),
    Telemetry(&'a TelemetryMeter),
}

#[derive(Clone, Copy)]
struct NetworkMeter<'a> {
    storage: NetworkSource<'a>,
    service: Option<&'a CountingServiceProxy>,
}

impl NetworkMeter<'_> {
    fn snapshot(&self) -> NetworkSnapshot {
        let mut snapshot = match self.storage {
            NetworkSource::Proxy(proxy) => proxy.snapshot(),
            NetworkSource::Telemetry(telemetry) => telemetry.snapshot(),
        };
        if let Some(service) = self.service {
            let service = service.snapshot();
            snapshot.service_connections = service.connections;
            snapshot.service_request_bytes = service.request_bytes;
            snapshot.service_response_bytes = service.response_bytes;
        }
        snapshot
    }

    fn settle(&self) {
        if matches!(self.storage, NetworkSource::Telemetry(_)) {
            thread::sleep(Duration::from_millis(150));
        }
    }

    fn telemetry_path(&self) -> Option<&Path> {
        match self.storage {
            NetworkSource::Proxy(_) => None,
            NetworkSource::Telemetry(telemetry) => Some(&telemetry.path),
        }
    }
}

struct TelemetryMeter {
    path: PathBuf,
}

impl TelemetryMeter {
    fn new(path: PathBuf) -> Self {
        let meter = Self { path };
        meter.reset();
        meter
    }

    fn reset(&self) {
        let snapshot = TelemetrySnapshot {
            schema: "yinyang.benchmark-http.v1".to_owned(),
            requests: 0,
            request_bytes: 0,
            response_bytes: 0,
            read_requests: 0,
        };
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent).expect("create benchmark telemetry directory");
        }
        fs::write(
            &self.path,
            serde_json::to_vec(&snapshot).expect("serialize zero benchmark telemetry"),
        )
        .expect("reset benchmark telemetry");
    }

    fn snapshot(&self) -> NetworkSnapshot {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if let Ok(bytes) = fs::read(&self.path)
                && let Ok(snapshot) = serde_json::from_slice::<TelemetrySnapshot>(&bytes)
            {
                assert_eq!(
                    snapshot.schema,
                    "yinyang.benchmark-http.v1",
                    "unexpected benchmark telemetry schema in {}",
                    self.path.display()
                );
                return NetworkSnapshot {
                    requests: snapshot.requests,
                    request_bytes: snapshot.request_bytes,
                    response_bytes: snapshot.response_bytes,
                    get: snapshot.read_requests,
                    other: snapshot.requests.saturating_sub(snapshot.read_requests),
                    ..NetworkSnapshot::default()
                };
            }
            assert!(
                Instant::now() < deadline,
                "benchmark telemetry {} did not become readable",
                self.path.display()
            );
            thread::sleep(Duration::from_millis(10));
        }
    }
}

#[derive(Deserialize, Serialize)]
struct TelemetrySnapshot {
    schema: String,
    requests: u64,
    request_bytes: u64,
    response_bytes: u64,
    read_requests: u64,
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
    let workload = match std::env::var("YINYANG_COST_WORKLOAD").as_deref() {
        Ok("full") | Err(_) => Workload::Full,
        Ok("read-only-range") => Workload::ReadOnlyRange,
        Ok("repeated-random") => Workload::RepeatedRandom,
        Ok("incremental") => Workload::Incremental,
        Ok("length-changing") => Workload::LengthChanging,
        value => panic!(
            "YINYANG_COST_WORKLOAD must be full, read-only-range, repeated-random, incremental, or length-changing, got {value:?}"
        ),
    };
    let incremental_expectation = match std::env::var("YINYANG_COST_INCREMENTAL_EXPECT").as_deref()
    {
        Ok("required") => IncrementalExpectation::Required,
        Ok("baseline") | Err(_) => IncrementalExpectation::Baseline,
        value => {
            panic!("YINYANG_COST_INCREMENTAL_EXPECT must be baseline or required, got {value:?}")
        }
    };
    let publication_filter = match std::env::var("YINYANG_COST_PUBLICATION").as_deref() {
        Ok("all") | Err(_) => PublicationFilter::All,
        Ok("object") => PublicationFilter::Object,
        Ok("service") => PublicationFilter::Service,
        value => panic!("YINYANG_COST_PUBLICATION must be all, object, or service, got {value:?}"),
    };
    if matches!(
        workload,
        Workload::ReadOnlyRange
            | Workload::RepeatedRandom
            | Workload::Incremental
            | Workload::LengthChanging
    ) && profile != Profile::Full
    {
        panic!("range and incremental workloads require YINYANG_COST_PROFILE=full");
    }
    if matches!(workload, Workload::Full)
        && matches!(profile, Profile::Early)
        && matches!(publication_filter, PublicationFilter::Service)
    {
        panic!("service publication selection requires the full profile");
    }
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
    let compiled_binary = PathBuf::from(env!("CARGO_BIN_EXE_yy"));
    let binary_path = std::env::var_os("YY_UPGRADE_BIN")
        .map(PathBuf::from)
        .unwrap_or_else(|| compiled_binary.clone());
    let binary_blake3 = blake3_file(&binary_path);
    if binary_path != compiled_binary {
        assert_eq!(
            binary_blake3,
            blake3_file(&compiled_binary),
            "YY_UPGRADE_BIN selects different bytes from the compiled benchmark binary"
        );
    }
    let storage_backend = std::env::var("YINYANG_COST_STORAGE_LABEL").unwrap_or_else(|_| {
        if std::env::var("YINYANG_S3_ENDPOINT")
            .is_ok_and(|endpoint| endpoint.starts_with("https://"))
        {
            "non-loopback S3-compatible storage over HTTPS".to_owned()
        } else {
            "loopback Silo, MinIO-compatible S3 over HTTP".to_owned()
        }
    });
    let network_source = if workload == Workload::RepeatedRandom {
        "OpenDAL released MetricsLayer in the measured yy process; HTTP request count and body bytes only"
            .to_owned()
    } else {
        "loopback HTTP/1 parser; total request/response bytes include headers, and GET/PUT body bytes are tracked separately"
            .to_owned()
    };
    let metadata_transport = if workload == Workload::RepeatedRandom {
        "object authority directly over HTTPS; no metadata service".to_owned()
    } else {
        "authenticated loopback TCP through a byte-counting proxy; transport opens one connection per RPC"
            .to_owned()
    };
    let range_cache_policy = std::env::var("YINYANG_COST_RANGE_POLICY")
        .unwrap_or_else(|_| "unspecified; select the exact measured binary policy".to_owned());
    let expect_range_reuse = std::env::var("YINYANG_COST_EXPECT_RANGE_REUSE").as_deref() == Ok("1");
    let context = RunContext {
        profile,
        workload,
        publication_filter,
        storage_backend,
        metadata_transport,
        network_source,
        range_cache_policy,
        expect_range_reuse,
        incremental_expectation: incremental_expectation.as_str().to_owned(),
        source_sha,
        source_dirty,
        binary_blake3,
        binary_path: binary_path.display().to_string(),
        host: host_identity(),
        rustc: command_line("rustc", &["--version"]),
    };
    let mut recorder = Recorder::new(&context, &output, &summary);

    if workload == Workload::RepeatedRandom {
        assert!(
            benchmark_telemetry_enabled(),
            "repeated-random measurement requires --features benchmark-telemetry"
        );
        let publications = &[Publication::Object];
        for &publication in publications {
            for &scale in &[Scale::Small, Scale::Medium] {
                run_repeated_random_matrix(
                    &context,
                    publication,
                    scale,
                    &output.with_extension(format!(
                        "telemetry-{}-{}",
                        publication.as_str(),
                        scale.as_str()
                    )),
                    &mut recorder,
                );
            }
        }
        recorder.finish();
        return;
    }

    let upstream = s3_upstream();
    let proxy = Arc::new(CountingProxy::start(upstream));
    println!(
        "linux-cost: source={} dirty={} binary={} binary_blake3={} workload={} profile={:?} proxy={} silo={upstream}",
        context.source_sha,
        context.source_dirty,
        context.binary_path,
        context.binary_blake3,
        context.workload.as_str(),
        match context.profile {
            Profile::Early => "early",
            Profile::Full => "full",
        },
        proxy.endpoint()
    );

    let publications: &[Publication] = match (profile, publication_filter) {
        (Profile::Early, PublicationFilter::All | PublicationFilter::Object) => {
            &[Publication::Object]
        }
        (Profile::Early, PublicationFilter::Service) => unreachable!(),
        (Profile::Full, PublicationFilter::All) => &[Publication::Object, Publication::Service],
        (Profile::Full, PublicationFilter::Object) => &[Publication::Object],
        (Profile::Full, PublicationFilter::Service) => &[Publication::Service],
    };
    if matches!(workload, Workload::Incremental) {
        for &publication in publications {
            run_incremental_matrix(
                &context,
                &proxy,
                publication,
                Scale::Medium,
                incremental_expectation,
                &mut recorder,
            );
        }
        recorder.finish();
        return;
    }
    if matches!(workload, Workload::LengthChanging) {
        for &publication in publications {
            run_length_changing_matrix(
                &context,
                &proxy,
                publication,
                Scale::Medium,
                incremental_expectation,
                &mut recorder,
            );
        }
        recorder.finish();
        return;
    }
    let scales: &[Scale] = match (workload, profile) {
        (Workload::ReadOnlyRange, _) | (Workload::Full, Profile::Full) => {
            &[Scale::Small, Scale::Medium]
        }
        (Workload::Full, Profile::Early) => &[Scale::Small],
        (Workload::RepeatedRandom | Workload::Incremental | Workload::LengthChanging, _) => {
            unreachable!("specialized workloads return above")
        }
    };
    if matches!(workload, Workload::ReadOnlyRange) {
        for &publication in publications {
            for &scale in scales {
                run_read_only_range_matrix(&context, &proxy, publication, scale, &mut recorder);
            }
        }
        recorder.finish();
        return;
    }

    for &publication in publications {
        for &scale in scales {
            run_sync_matrix(&context, &proxy, publication, scale, &mut recorder);
            run_fuse_matrix(&context, &proxy, publication, scale, &mut recorder);
            run_concurrent_matrix(&context, &proxy, publication, scale, &mut recorder);
        }
    }
    for &scale in scales {
        if !matches!(publication_filter, PublicationFilter::Service) {
            run_publish_restore_matrix(&context, &proxy, scale, &mut recorder);
        }
        if matches!(profile, Profile::Full)
            && !matches!(publication_filter, PublicationFilter::Object)
        {
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

fn benchmark_telemetry_enabled() -> bool {
    cfg!(feature = "benchmark-telemetry")
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
        "YINYANG_S3_REGION",
        "YINYANG_S3_ACCESS_KEY_ID",
        "YINYANG_S3_SECRET_ACCESS_KEY",
    ] {
        assert!(
            std::env::var_os(name).is_some(),
            "{name} is unset; the benchmark fails hard instead of skipping"
        );
    }
    if std::env::var("YINYANG_STORAGE_PROFILE").as_deref() == Ok("minio") {
        assert!(
            std::env::var_os("YINYANG_S3_ENDPOINT").is_some(),
            "YINYANG_S3_ENDPOINT is unset for the MinIO benchmark profile"
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
    let bench = BenchFixture::new(publication, proxy.endpoint());
    let fixture = &bench.fixture;
    let meter = bench.network(proxy);
    let paths = fixture.sync_paths(&format!("sync-{}-{}", publication.as_str(), scale.as_str()));
    fs::create_dir(&paths.directory).expect("create benchmark sync root");
    populate_small_tree(&paths.directory, scale);
    fixture.sync_init(&paths);
    let dataset = tree_dataset(&paths.directory);
    let completion = "latency is one finite yy sync run; in_sync status was read separately after the measured run";

    let initial = run_yy_operation(
        &meter,
        fixture.yy_command(&["--volume", path_str(&paths.config), "sync", "run"]),
        "initial Sync push",
    );
    assert_sync_in_sync(fixture, &paths, "after initial Sync push");
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
        space_snapshot(fixture, Some(&paths.directory), Some(&paths.state), None),
        vec!["status.in_sync=true".to_owned()],
    );

    let mut noop = OperationBatch::default();
    for index in 0..5 {
        let measured = run_yy_operation(
            &meter,
            fixture.yy_command(&["--volume", path_str(&paths.config), "sync", "run"]),
            &format!("Sync no-op {}", index + 1),
        );
        noop.push(measured);
    }
    assert_sync_in_sync(fixture, &paths, "after Sync no-op runs");
    noop.record(
        recorder,
        context,
        "sync_noop",
        publication,
        scale,
        dataset.clone(),
        completion,
        space_snapshot(fixture, Some(&paths.directory), Some(&paths.state), None),
        vec!["status.in_sync=true".to_owned()],
    );

    let edit = paths.directory.join("dir-0000/file-0000");
    let mut edits = OperationBatch::default();
    let mut edit_spaces = Vec::new();
    for index in 0..3 {
        fs::write(
            &edit,
            payload(1024, 0x40 + u8::try_from(index).expect("three edits")),
        )
        .expect("write one Sync edit");
        let measured = run_yy_operation(
            &meter,
            fixture.yy_command(&["--volume", path_str(&paths.config), "sync", "run"]),
            &format!("Sync single edit {}", index + 1),
        );
        edits.push(measured);
        edit_spaces.push(space_snapshot(
            fixture,
            Some(&paths.directory),
            Some(&paths.state),
            None,
        ));
    }
    assert_sync_in_sync(fixture, &paths, "after Sync edits");
    edits.record_with_spaces(
        recorder,
        context,
        "sync_single_edit",
        publication,
        scale,
        dataset.clone(),
        completion,
        edit_spaces,
        vec![
            "status.in_sync=true".to_owned(),
            "space series covers each completed edit publish".to_owned(),
        ],
    );

    let mut source = edit.clone();
    let mut destination = paths.directory.join("dir-0000/file-renamed");
    let mut renames = OperationBatch::default();
    let mut rename_spaces = Vec::new();
    for index in 0..3 {
        fs::rename(&source, &destination).unwrap_or_else(|error| {
            panic!(
                "rename Sync benchmark path {} to {}: {error}",
                source.display(),
                destination.display()
            )
        });
        let measured = run_yy_operation(
            &meter,
            fixture.yy_command(&["--volume", path_str(&paths.config), "sync", "run"]),
            &format!("Sync rename {}", index + 1),
        );
        renames.push(measured);
        rename_spaces.push(space_snapshot(
            fixture,
            Some(&paths.directory),
            Some(&paths.state),
            None,
        ));
        std::mem::swap(&mut source, &mut destination);
    }
    assert_sync_in_sync(fixture, &paths, "after Sync renames");
    renames.record_with_spaces(
        recorder,
        context,
        "sync_rename",
        publication,
        scale,
        dataset,
        completion,
        rename_spaces,
        vec![
            "status.in_sync=true".to_owned(),
            "space series covers each completed rename publish".to_owned(),
        ],
    );
}

fn run_fuse_matrix(
    context: &RunContext,
    proxy: &Arc<CountingProxy>,
    publication: Publication,
    scale: Scale,
    recorder: &mut Recorder,
) {
    let bench = BenchFixture::new(publication, proxy.endpoint());
    let fixture = &bench.fixture;
    let meter = bench.network(proxy);
    let size = scale.file_size();
    let mut expected_large = payload(size, 0x5a);
    fixture.remote_create_file("large.bin", &expected_large, false);
    let (config, staging, mountpoint) = mount_paths(
        fixture,
        &format!("fuse-{}-{}", publication.as_str(), scale.as_str()),
        false,
    );
    fs::create_dir(&mountpoint).expect("create benchmark mountpoint");
    let (mut mounted, mount_ready) = FuseMount::start(fixture, &config, &mountpoint, &meter);
    let dataset = Dataset {
        kind: "single_large_file".to_owned(),
        files: 1,
        directories: 0,
        logical_bytes: size as u64,
        file_size_bytes: Some(size as u64),
        range_bytes: Some(RANGE_BYTES as u64),
        writers: None,
        read_pattern: None,
        working_set_bytes: None,
        random_seed: None,
    };
    recorder.record(
        context,
        "fuse_mount_ready",
        publication,
        scale,
        dataset.clone(),
        "interval begins before yy mount spawn and ends when fuse.yinyang appears in /proc/mounts",
        vec![mount_ready.latency_ms],
        mount_ready.io,
        mount_ready.network,
        vec![],
        space_snapshot(fixture, None, None, Some(&staging)),
        vec!["mount type=fuse.yinyang".to_owned()],
    );

    let large = mountpoint.join("large.bin");
    let cold = measure_mount_operation(&meter, mounted.pid(), || {
        let file = File::open(&large).expect("cold open large FUSE file");
        let mut value = vec![0_u8; RANGE_BYTES];
        read_exact_at(&file, &mut value, (size / 2) as u64);
        drop(file);
        value
    });
    let cold_bytes = cold.value;
    assert_eq!(cold_bytes, expected_large[size / 2..size / 2 + RANGE_BYTES]);
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
        space_snapshot(fixture, None, None, Some(&staging)),
        vec!["range bytes verified".to_owned()],
    );

    let warm_file = File::open(&large).expect("warm open large FUSE file");
    let mut warm = OperationBatch::default();
    for index in 0..5 {
        let offset = ((index * RANGE_BYTES) as u64).min((size - RANGE_BYTES) as u64);
        let measured = measure_mount_operation(&meter, mounted.pid(), || {
            let mut value = vec![0_u8; RANGE_BYTES];
            read_exact_at(&warm_file, &mut value, offset);
            assert_eq!(
                value,
                expected_large[offset as usize..offset as usize + RANGE_BYTES]
            );
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
        space_snapshot(fixture, None, None, Some(&staging)),
        vec!["range bytes verified".to_owned()],
    );

    let mut sparse = OperationBatch::default();
    let mut spaces = Vec::new();
    for index in 0..4_u64 {
        let offset = (size as u64 - RANGE_BYTES as u64) / 2 + index * RANGE_BYTES as u64;
        let value = payload(
            RANGE_BYTES,
            0x80 + u8::try_from(index).expect("four updates"),
        );
        expected_large[offset as usize..offset as usize + RANGE_BYTES].copy_from_slice(&value);
        let measured = measure_mount_operation(&meter, mounted.pid(), || {
            let file = OpenOptions::new()
                .write(true)
                .open(&large)
                .expect("open large FUSE file for sparse update");
            write_all_at(&file, &value, offset);
            file.sync_all().expect("remote fsync after sparse update");
            drop(file);
        });
        sparse.push(measured);
        spaces.push(space_snapshot(fixture, None, None, Some(&staging)));
    }
    assert_eq!(
        remote_file_bytes(fixture, "large.bin"),
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
    let (mut restarted, restart_ready) = FuseMount::start(fixture, &config, &mountpoint, &meter);
    recorder.record(
        context,
        "fuse_restart_mount_ready",
        publication,
        scale,
        dataset.clone(),
        "interval begins before the replacement yy mount spawn and ends when fuse.yinyang appears in /proc/mounts",
        vec![restart_ready.latency_ms],
        restart_ready.io,
        restart_ready.network,
        vec![],
        space_snapshot(fixture, None, None, Some(&staging)),
        vec!["replacement mount type=fuse.yinyang".to_owned()],
    );
    let restart = measure_mount_operation(&meter, restarted.pid(), || {
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
        "interval starts after the replacement mount is ready, then cold-opens and reads 4 KiB",
        vec![restart.latency_ms],
        restart.io,
        restart.network,
        vec![],
        space_snapshot(fixture, None, None, Some(&staging)),
        vec!["restarted mount served the original range bytes".to_owned()],
    );
    restarted.unmount();
}

fn run_incremental_matrix(
    context: &RunContext,
    proxy: &Arc<CountingProxy>,
    publication: Publication,
    scale: Scale,
    expectation: IncrementalExpectation,
    recorder: &mut Recorder,
) {
    let bench = BenchFixture::new(publication, proxy.endpoint());
    let fixture = &bench.fixture;
    let meter = bench.network(proxy);
    let size = scale.file_size();
    let mut expected = payload(size, 0x3c);
    fixture.remote_create_file("incremental.bin", &expected, false);
    let (config, staging, mountpoint) = mount_paths(
        fixture,
        &format!("incremental-{}-{}", publication.as_str(), scale.as_str()),
        false,
    );
    fs::create_dir(&mountpoint).expect("create incremental benchmark mountpoint");
    let (mut mounted, mount_ready) = FuseMount::start(fixture, &config, &mountpoint, &meter);
    let base_dataset = Dataset {
        kind: "single_large_file_incremental".to_owned(),
        files: 1,
        directories: 0,
        logical_bytes: size as u64,
        file_size_bytes: Some(size as u64),
        range_bytes: Some(RANGE_BYTES as u64),
        writers: Some(1),
        read_pattern: None,
        working_set_bytes: None,
        random_seed: None,
    };
    recorder.record(
        context,
        "fuse_incremental_mount_ready",
        publication,
        scale,
        base_dataset.clone(),
        "interval begins before yy mount spawn and ends when fuse.yinyang appears in /proc/mounts",
        vec![mount_ready.latency_ms],
        mount_ready.io,
        mount_ready.network,
        vec![],
        space_snapshot(fixture, None, None, Some(&staging)),
        vec!["writable mount type=fuse.yinyang".to_owned()],
    );

    let path = mountpoint.join("incremental.bin");
    let open = measure_mount_operation(&meter, mounted.pid(), || {
        OpenOptions::new()
            .write(true)
            .open(&path)
            .expect("open incremental benchmark file")
    });
    let Measurement {
        value: writer,
        latency_ms: open_latency_ms,
        io: open_io,
        network: open_network,
    } = open;
    let open_get_body_bytes = open_network.get_response_body_bytes;
    if expectation == IncrementalExpectation::Required {
        assert!(
            open_get_body_bytes < size as u64 / 2,
            "open materialized {} bytes of a {size}-byte file; this is consistent with a full-file fallback",
            open_get_body_bytes
        );
    }
    recorder.record(
        context,
        "fuse_incremental_open_materialization",
        publication,
        scale,
        base_dataset.clone(),
        "open completed before a write or fsync; GET body bytes are HTTP entity bytes, excluding headers",
        vec![open_latency_ms],
        open_io,
        open_network,
        vec![],
        space_snapshot(fixture, None, None, Some(&staging)),
        vec![
            "writable descriptor opened successfully".to_owned(),
            format!("get_response_body_bytes={open_get_body_bytes}"),
            format!("full_file_bytes={size}"),
        ],
    );

    let unit_center =
        (size as u64 / 2 / BLOCK_BYTES as u64) * BLOCK_BYTES as u64 + BLOCK_BYTES as u64 / 4;
    let boundary_offset =
        size as u64 - 2 * BLOCK_BYTES as u64 + BLOCK_BYTES as u64 - RANGE_BYTES as u64 / 2;
    let updates = [
        ("unit_center", unit_center, RANGE_BYTES, 0x91_u8),
        ("unit_boundary", boundary_offset, RANGE_BYTES * 2, 0xa7_u8),
        (
            "eof_tail",
            size as u64 - RANGE_BYTES as u64,
            RANGE_BYTES,
            0xb9_u8,
        ),
    ];
    for (label, offset, length, seed) in updates {
        let value = payload(length, seed);
        expected[offset as usize..offset as usize + length].copy_from_slice(&value);
        let measured = measure_mount_operation(&meter, mounted.pid(), || {
            write_all_at(&writer, &value, offset);
            writer
                .sync_all()
                .expect("fsync incremental benchmark overwrite");
        });
        assert_eq!(
            remote_file_bytes(fixture, "incremental.bin"),
            expected,
            "{label}: remote bytes after incremental fsync"
        );
        assert!(
            measured.network.put > 0,
            "{label}: a successful fsync must publish at least one PUT"
        );
        let put_body_bytes = measured.network.put_request_body_bytes;
        let get_body_bytes = measured.network.get_response_body_bytes;
        if expectation == IncrementalExpectation::Required {
            assert!(
                put_body_bytes < size as u64 / 2,
                "{label}: PUT request bodies were {put_body_bytes} bytes for a {length}-byte overwrite in a {size}-byte file; this is consistent with full-file fallback"
            );
            assert!(
                get_body_bytes < size as u64 / 2,
                "{label}: fsync preparation read {get_body_bytes} bytes for a {length}-byte overwrite in a {size}-byte file; service traffic did not hide a full-file fallback"
            );
        }
        recorder.record(
            context,
            &format!("fuse_incremental_{label}"),
            publication,
            scale,
            Dataset {
                range_bytes: Some(length as u64),
                read_pattern: Some(format!(
                    "one {length}-byte overwrite at offset {offset}, followed by fsync"
                )),
                working_set_bytes: Some(length as u64),
                ..base_dataset.clone()
            },
            "pwrite was accepted and fsync returned before the remote byte check; PUT body bytes are HTTP entity bytes, excluding headers",
            vec![measured.latency_ms],
            measured.io,
            measured.network,
            vec![],
            space_snapshot(fixture, None, None, Some(&staging)),
            vec![
                "complete remote bytes matched the expected file".to_owned(),
                format!("put_request_body_bytes={put_body_bytes}"),
                format!("get_response_body_bytes={get_body_bytes}"),
                format!("full_file_bytes={size}"),
            ],
        );
    }

    let unchanged_fsync = measure_mount_operation(&meter, mounted.pid(), || {
        writer
            .sync_all()
            .expect("fsync an unchanged incremental descriptor");
    });
    assert_eq!(
        unchanged_fsync.network.put, 0,
        "an unchanged fsync must not upload another file version"
    );
    recorder.record(
        context,
        "fuse_incremental_unchanged_fsync",
        publication,
        scale,
        base_dataset.clone(),
        "the descriptor was opened before measurement; only an unchanged fsync is timed",
        vec![unchanged_fsync.latency_ms],
        unchanged_fsync.io,
        unchanged_fsync.network,
        vec![],
        space_snapshot(fixture, None, None, Some(&staging)),
        vec!["unchanged fsync emitted no backend PUT".to_owned()],
    );
    drop(writer);
    mounted.unmount();

    let (mut restarted, restart_ready) = FuseMount::start(fixture, &config, &mountpoint, &meter);
    recorder.record(
        context,
        "fuse_incremental_restart_mount_ready",
        publication,
        scale,
        base_dataset.clone(),
        "interval begins before the replacement yy mount spawn and ends when fuse.yinyang appears in /proc/mounts",
        vec![restart_ready.latency_ms],
        restart_ready.io,
        restart_ready.network,
        vec![],
        space_snapshot(fixture, None, None, Some(&staging)),
        vec!["replacement writable mount type=fuse.yinyang".to_owned()],
    );
    let verify_offset = size as u64 - 2 * BLOCK_BYTES as u64;
    let verify_length = 2 * BLOCK_BYTES;
    let restart = measure_mount_operation(&meter, restarted.pid(), || {
        let file = File::open(&path).expect("open incremental file after mount restart");
        let mut value = vec![0_u8; verify_length];
        read_exact_at(&file, &mut value, verify_offset);
        drop(file);
        value
    });
    assert_eq!(
        restart.value,
        expected[verify_offset as usize..verify_offset as usize + verify_length],
        "restart did not preserve the boundary and EOF sparse overwrites"
    );
    recorder.record(
        context,
        "fuse_incremental_restart_boundary_eof_read",
        publication,
        scale,
        Dataset {
            range_bytes: Some(verify_length as u64),
            read_pattern: Some(
                "one read covering the boundary-crossing update and EOF update after restart"
                    .to_owned(),
            ),
            working_set_bytes: Some(verify_length as u64),
            ..base_dataset
        },
        "open and read complete after restart; bytes cover the boundary-crossing and EOF updates",
        vec![restart.latency_ms],
        restart.io,
        restart.network,
        vec![],
        space_snapshot(fixture, None, None, Some(&staging)),
        vec!["restarted mount returned the exact incremental bytes".to_owned()],
    );
    restarted.unmount();
}

fn run_length_changing_matrix(
    context: &RunContext,
    proxy: &Arc<CountingProxy>,
    publication: Publication,
    scale: Scale,
    expectation: IncrementalExpectation,
    recorder: &mut Recorder,
) {
    let bench = BenchFixture::new(publication, proxy.endpoint());
    let fixture = &bench.fixture;
    let meter = bench.network(proxy);
    let size = scale.file_size();
    let mut expected = payload(size, 0x4d);
    fixture.remote_create_file("length-changing.bin", &expected, false);
    let (config, staging, mountpoint) = mount_paths(
        fixture,
        &format!(
            "length-changing-{}-{}",
            publication.as_str(),
            scale.as_str()
        ),
        false,
    );
    fs::create_dir(&mountpoint).expect("create length-changing benchmark mountpoint");
    let (mut mounted, mount_ready) = FuseMount::start(fixture, &config, &mountpoint, &meter);
    let base_dataset = Dataset {
        kind: "single_large_file_length_changing".to_owned(),
        files: 1,
        directories: 0,
        logical_bytes: size as u64,
        file_size_bytes: Some(size as u64),
        range_bytes: None,
        writers: Some(1),
        read_pattern: None,
        working_set_bytes: None,
        random_seed: None,
    };
    recorder.record(
        context,
        "fuse_length_changing_mount_ready",
        publication,
        scale,
        Dataset {
            working_set_bytes: Some(0),
            ..base_dataset.clone()
        },
        "interval begins before yy mount spawn and ends when fuse.yinyang appears in /proc/mounts",
        vec![mount_ready.latency_ms],
        mount_ready.io,
        mount_ready.network,
        vec![],
        space_snapshot(fixture, None, None, Some(&staging)),
        vec!["writable mount type=fuse.yinyang".to_owned()],
    );

    let path = mountpoint.join("length-changing.bin");
    let mut open_batch = OperationBatch::default();
    let mut shared_reader = None;
    let mut writer = None;
    for sample in 0..2 {
        let measured = measure_mount_operation(&meter, mounted.pid(), || {
            if sample == 0 {
                File::open(&path).expect("open shared length-changing reader")
            } else {
                OpenOptions::new()
                    .read(true)
                    .write(true)
                    .append(true)
                    .open(&path)
                    .expect("open length-changing writer")
            }
        });
        if sample == 0 {
            shared_reader = Some(measured.value);
        } else {
            writer = Some(measured.value);
        }
        open_batch.push(Measurement {
            value: (),
            latency_ms: measured.latency_ms,
            io: measured.io,
            network: measured.network,
        });
    }
    let mut writer = writer.expect("writer opened during the measured batch");
    let shared_reader = shared_reader.expect("shared reader opened during the measured batch");
    if expectation == IncrementalExpectation::Required {
        let samples = open_batch.latencies.len() as u64;
        assert!(
            open_batch.network.get_response_body_bytes < size as u64 / 2 * samples,
            "writable open and shared reader read {} bytes for a {size}-byte file; this is consistent with full-file materialization",
            open_batch.network.get_response_body_bytes
        );
    }
    assert_required_staging(context, &staging, size, "writable open");
    recorder.record(
        context,
        "fuse_length_changing_open_shared_descriptor",
        publication,
        scale,
        Dataset {
            read_pattern: Some(
                "one read-only descriptor followed by one writable append descriptor".to_owned(),
            ),
            working_set_bytes: Some(0),
            ..base_dataset.clone()
        },
        "both descriptors opened before any write; GET body bytes are HTTP entity bytes",
        open_batch.latencies,
        open_batch.io,
        open_batch.network,
        vec![],
        space_snapshot(fixture, None, None, Some(&staging)),
        vec![
            "read-only and writable descriptors share the same node generation".to_owned(),
            format!("logical_bytes={}", expected.len()),
        ],
    );

    let mut append_batch = OperationBatch::default();
    for sample in 0..2 {
        let value = payload(RANGE_BYTES, 0x51 + sample as u8);
        let measured = measure_mount_operation(&meter, mounted.pid(), || {
            writer.write_all(&value).expect("append through FUSE");
            writer.sync_all().expect("fsync FUSE append");
        });
        append_batch.push(measured);
        expected.extend_from_slice(&value);
        assert_shared_length_and_remote(
            fixture,
            &path,
            &writer,
            &shared_reader,
            &expected,
            "small append",
        );
    }
    assert_length_batch(context, &append_batch, size, RANGE_BYTES, "small append");
    assert_required_staging(context, &staging, size, "small append");
    recorder.record(
        context,
        "fuse_length_changing_small_append",
        publication,
        scale,
        Dataset {
            logical_bytes: expected.len() as u64,
            range_bytes: Some(RANGE_BYTES as u64),
            read_pattern: Some("two 4 KiB O_APPEND writes, each followed by fsync".to_owned()),
            ..base_dataset.clone()
        },
        "each pwrite and fsync returned before complete remote bytes were verified outside the measured interval",
        append_batch.latencies,
        append_batch.io,
        append_batch.network,
        vec![],
        space_snapshot(fixture, None, None, Some(&staging)),
        vec![
            "both active descriptors observed each staged length".to_owned(),
            "complete remote bytes matched after every sample".to_owned(),
        ],
    );

    let mut boundary_batch = OperationBatch::default();
    for sample in 0..2 {
        let value = payload(BLOCK_BYTES + RANGE_BYTES, 0x61 + sample as u8);
        let measured = measure_mount_operation(&meter, mounted.pid(), || {
            writer
                .write_all(&value)
                .expect("append across a verification-unit boundary");
            writer
                .sync_all()
                .expect("fsync verification-unit boundary append");
        });
        boundary_batch.push(measured);
        expected.extend_from_slice(&value);
        assert_shared_length_and_remote(
            fixture,
            &path,
            &writer,
            &shared_reader,
            &expected,
            "boundary append",
        );
    }
    assert_length_batch(
        context,
        &boundary_batch,
        size,
        BLOCK_BYTES + RANGE_BYTES,
        "boundary append",
    );
    assert_required_staging(context, &staging, size, "boundary append");
    recorder.record(
        context,
        "fuse_length_changing_boundary_append",
        publication,
        scale,
        Dataset {
            kind: "single_large_file_tree_shape_change".to_owned(),
            logical_bytes: expected.len() as u64,
            range_bytes: Some((BLOCK_BYTES + RANGE_BYTES) as u64),
            read_pattern: Some(
                "two 68 KiB appends, each crossing an existing 64 KiB verification unit; append starts change the content tree shape"
                    .to_owned(),
            ),
            ..base_dataset.clone()
        },
        "each append and fsync returned before complete remote bytes were verified outside the measured interval",
        boundary_batch.latencies,
        boundary_batch.io,
        boundary_batch.network,
        vec![],
        space_snapshot(fixture, None, None, Some(&staging)),
        vec![
            "each sample appended past a 64 KiB unit boundary".to_owned(),
            "the initial 32 MiB power-of-two tree gained a partial and then a complete leaf".to_owned(),
            "complete remote bytes matched after every sample".to_owned(),
        ],
    );

    let mut truncate_batch = OperationBatch::default();
    for _ in 0..2 {
        let length = expected.len() - RANGE_BYTES;
        let measured = measure_mount_operation(&meter, mounted.pid(), || {
            writer
                .set_len(length as u64)
                .expect("shrink through an active FUSE descriptor");
            writer.sync_all().expect("fsync FUSE shrink");
        });
        truncate_batch.push(measured);
        expected.truncate(length);
        assert_shared_length_and_remote(
            fixture,
            &path,
            &writer,
            &shared_reader,
            &expected,
            "descriptor shrink",
        );
    }
    assert_length_batch(
        context,
        &truncate_batch,
        size,
        RANGE_BYTES,
        "descriptor shrink",
    );
    assert_required_staging(context, &staging, size, "descriptor shrink");
    recorder.record(
        context,
        "fuse_length_changing_truncate",
        publication,
        scale,
        Dataset {
            logical_bytes: expected.len() as u64,
            range_bytes: Some(RANGE_BYTES as u64),
            read_pattern: Some("two 4 KiB descriptor truncates, each followed by fsync".to_owned()),
            ..base_dataset.clone()
        },
        "each ftruncate and fsync returned before complete remote bytes were verified outside the measured interval",
        truncate_batch.latencies,
        truncate_batch.io,
        truncate_batch.network,
        vec![],
        space_snapshot(fixture, None, None, Some(&staging)),
        vec![
            "both active descriptors observed each shortened length".to_owned(),
            "complete remote bytes matched after every sample".to_owned(),
        ],
    );

    let mut extend_batch = OperationBatch::default();
    for _ in 0..2 {
        let length = expected.len() + RANGE_BYTES;
        let measured = measure_mount_operation(&meter, mounted.pid(), || {
            writer
                .set_len(length as u64)
                .expect("zero-extend through an active FUSE descriptor");
            writer.sync_all().expect("fsync FUSE zero extension");
        });
        extend_batch.push(measured);
        expected.resize(length, 0);
        assert_shared_length_and_remote(
            fixture,
            &path,
            &writer,
            &shared_reader,
            &expected,
            "descriptor zero extension",
        );
    }
    assert_length_batch(
        context,
        &extend_batch,
        size,
        RANGE_BYTES,
        "descriptor zero extension",
    );
    assert_required_staging(context, &staging, size, "descriptor zero extension");
    recorder.record(
        context,
        "fuse_length_changing_zero_extension",
        publication,
        scale,
        Dataset {
            logical_bytes: expected.len() as u64,
            range_bytes: Some(RANGE_BYTES as u64),
            read_pattern: Some(
                "two 4 KiB descriptor zero extensions, each followed by fsync".to_owned(),
            ),
            ..base_dataset.clone()
        },
        "each ftruncate and fsync returned before complete remote zero bytes were verified outside the measured interval",
        extend_batch.latencies,
        extend_batch.io,
        extend_batch.network,
        vec![],
        space_snapshot(fixture, None, None, Some(&staging)),
        vec![
            "both active descriptors observed each extended length".to_owned(),
            "complete remote bytes, including inserted zeros, matched after every sample".to_owned(),
        ],
    );

    let mut shrink_extend_batch = OperationBatch::default();
    for _ in 0..2 {
        let current = expected.len();
        let shrunk = current - RANGE_BYTES;
        let extended = current + RANGE_BYTES;
        let measured = measure_mount_operation(&meter, mounted.pid(), || {
            writer
                .set_len(shrunk as u64)
                .expect("shrink before zero extension");
            writer
                .set_len(extended as u64)
                .expect("extend after shrink");
            writer.sync_all().expect("fsync shrink-then-extend");
        });
        shrink_extend_batch.push(measured);
        expected.truncate(shrunk);
        expected.resize(extended, 0);
        let mut removed = vec![0xff_u8; 2 * RANGE_BYTES];
        read_exact_at(&shared_reader, &mut removed, shrunk as u64);
        assert!(
            removed.iter().all(|byte| *byte == 0),
            "shrink-then-extend resurrected bytes below the shortened EOF"
        );
        assert_shared_length_and_remote(
            fixture,
            &path,
            &writer,
            &shared_reader,
            &expected,
            "descriptor shrink-then-extend",
        );
    }
    assert_length_batch(
        context,
        &shrink_extend_batch,
        size,
        2 * RANGE_BYTES,
        "descriptor shrink-then-extend",
    );
    assert_required_staging(context, &staging, size, "descriptor shrink-then-extend");
    recorder.record(
        context,
        "fuse_length_changing_shrink_then_extend",
        publication,
        scale,
        Dataset {
            logical_bytes: expected.len() as u64,
            range_bytes: Some((2 * RANGE_BYTES) as u64),
            read_pattern: Some(
                "two rounds of a 4 KiB shrink followed by an 8 KiB zero extension and fsync"
                    .to_owned(),
            ),
            ..base_dataset.clone()
        },
        "the shared reader and remote authority were checked after each publish; removed bytes read as zero",
        shrink_extend_batch.latencies,
        shrink_extend_batch.io,
        shrink_extend_batch.network,
        vec![],
        space_snapshot(fixture, None, None, Some(&staging)),
        vec![
            "shrink-then-extend never resurrected removed base bytes".to_owned(),
            "complete remote bytes matched after every sample".to_owned(),
        ],
    );

    let mut unchanged_batch = OperationBatch::default();
    for _ in 0..2 {
        unchanged_batch.push(measure_mount_operation(&meter, mounted.pid(), || {
            writer
                .sync_all()
                .expect("fsync unchanged length-changing descriptor");
        }));
    }
    assert_eq!(
        unchanged_batch.network.put, 0,
        "an unchanged length-changing fsync emitted an unexpected backend PUT"
    );
    assert_eq!(
        unchanged_batch.network.put_request_body_bytes, 0,
        "an unchanged length-changing fsync uploaded bytes"
    );
    recorder.record(
        context,
        "fuse_length_changing_unchanged_fsync",
        publication,
        scale,
        Dataset {
            logical_bytes: expected.len() as u64,
            read_pattern: Some("two unchanged fsync calls on the active descriptor".to_owned()),
            working_set_bytes: Some(0),
            ..base_dataset.clone()
        },
        "the descriptor was fully published before measurement; only unchanged fsync calls are timed",
        unchanged_batch.latencies,
        unchanged_batch.io,
        unchanged_batch.network,
        vec![],
        space_snapshot(fixture, None, None, Some(&staging)),
        vec!["unchanged fsync emitted zero backend PUT and zero PUT body bytes".to_owned()],
    );

    drop(writer);
    drop(shared_reader);
    mounted.unmount();

    let (mut restarted, restart_ready) = FuseMount::start(fixture, &config, &mountpoint, &meter);
    recorder.record(
        context,
        "fuse_length_changing_restart_mount_ready",
        publication,
        scale,
        Dataset {
            logical_bytes: expected.len() as u64,
            working_set_bytes: Some(0),
            ..base_dataset.clone()
        },
        "interval begins before the replacement yy mount spawn and ends when fuse.yinyang appears in /proc/mounts",
        vec![restart_ready.latency_ms],
        restart_ready.io,
        restart_ready.network,
        vec![],
        space_snapshot(fixture, None, None, Some(&staging)),
        vec!["replacement writable mount type=fuse.yinyang".to_owned()],
    );
    let tail_offset = expected.len() - RANGE_BYTES;
    let mut restart_batch = OperationBatch::default();
    for _ in 0..2 {
        let measured = measure_mount_operation(&meter, restarted.pid(), || {
            let file = File::open(&path).expect("open length-changing file after restart");
            let mut value = vec![0_u8; RANGE_BYTES];
            read_exact_at(&file, &mut value, tail_offset as u64);
            drop(file);
            value
        });
        assert_eq!(
            measured.value,
            expected[tail_offset..tail_offset + RANGE_BYTES],
            "restart did not return the final length-changing tail"
        );
        restart_batch.push(Measurement {
            value: (),
            latency_ms: measured.latency_ms,
            io: measured.io,
            network: measured.network,
        });
    }
    assert_eq!(
        remote_file_bytes(fixture, "length-changing.bin"),
        expected,
        "restart complete remote byte check"
    );
    recorder.record(
        context,
        "fuse_length_changing_restart_tail_read",
        publication,
        scale,
        Dataset {
            logical_bytes: expected.len() as u64,
            range_bytes: Some(RANGE_BYTES as u64),
            read_pattern: Some(
                "two fresh opens and 4 KiB tail reads after remount".to_owned(),
            ),
            ..base_dataset
        },
        "open and read completed after remount; complete remote bytes were checked outside the measured interval",
        restart_batch.latencies,
        restart_batch.io,
        restart_batch.network,
        vec![],
        space_snapshot(fixture, None, None, Some(&staging)),
        vec![
            "both restarted reads returned the final tail".to_owned(),
            "complete remote bytes matched the final expected file".to_owned(),
        ],
    );
    restarted.unmount();
}

fn assert_shared_length_and_remote(
    fixture: &Fixture,
    path: &Path,
    writer: &File,
    reader: &File,
    expected: &[u8],
    phase: &str,
) {
    let length = expected.len() as u64;
    assert_eq!(
        writer.metadata().expect("fstat active writer").len(),
        length,
        "{phase}: writer length"
    );
    assert_eq!(
        reader.metadata().expect("fstat shared reader").len(),
        length,
        "{phase}: shared reader length"
    );
    assert_eq!(
        fs::metadata(path).expect("stat length-changing path").len(),
        length,
        "{phase}: path length"
    );
    assert_eq!(
        remote_file_bytes(fixture, "length-changing.bin"),
        expected,
        "{phase}: independent remote bytes"
    );
}

fn assert_length_batch(
    context: &RunContext,
    batch: &OperationBatch,
    file_size: usize,
    useful_bytes: usize,
    phase: &str,
) {
    assert!(
        batch.network.put > 0,
        "{phase}: a successful fsync did not publish backend bytes"
    );
    if context.incremental_expectation != "required" {
        return;
    }
    let samples = batch.latencies.len() as u64;
    let payload_limit = file_size as u64 / 2 * samples;
    assert!(
        batch.network.put_request_body_bytes < payload_limit,
        "{phase}: PUT bodies were {} bytes for {samples} samples changing {useful_bytes} useful bytes in a {file_size}-byte file; this is consistent with full-file fallback",
        batch.network.put_request_body_bytes
    );
    assert!(
        batch.network.get_response_body_bytes < payload_limit,
        "{phase}: GET bodies were {} bytes for {samples} samples changing {useful_bytes} useful bytes in a {file_size}-byte file; preparation imported the complete base",
        batch.network.get_response_body_bytes
    );
}

fn assert_required_staging(context: &RunContext, staging: &Path, file_size: usize, phase: &str) {
    if context.incremental_expectation != "required" {
        return;
    }
    let bytes = tree_bytes(staging);
    assert!(
        bytes < file_size as u64 / 2,
        "{phase}: allocated staging was {bytes} bytes for a {file_size}-byte file; this is consistent with full local materialization"
    );
}

fn run_read_only_range_matrix(
    context: &RunContext,
    proxy: &Arc<CountingProxy>,
    publication: Publication,
    scale: Scale,
    recorder: &mut Recorder,
) {
    let bench = BenchFixture::new(publication, proxy.endpoint());
    let fixture = &bench.fixture;
    let meter = bench.network(proxy);
    let size = scale.file_size();
    let expected = payload(size, 0x6d);
    fixture.remote_create_file("large.bin", &expected, false);
    let (config, staging, mountpoint) = mount_paths(
        fixture,
        &format!(
            "read-only-range-{}-{}",
            publication.as_str(),
            scale.as_str()
        ),
        true,
    );
    fs::create_dir(&mountpoint).expect("create read-only benchmark mountpoint");
    let (mut mounted, mount_ready) = FuseMount::start(fixture, &config, &mountpoint, &meter);
    let dataset = Dataset {
        kind: "single_large_read_only_file".to_owned(),
        files: 1,
        directories: 0,
        logical_bytes: size as u64,
        file_size_bytes: Some(size as u64),
        range_bytes: Some(RANGE_BYTES as u64),
        writers: None,
        read_pattern: None,
        working_set_bytes: None,
        random_seed: None,
    };
    recorder.record(
        context,
        "fuse_read_only_range_mount_ready",
        publication,
        scale,
        dataset.clone(),
        "interval begins before yy mount spawn and ends when fuse.yinyang appears in /proc/mounts",
        vec![mount_ready.latency_ms],
        mount_ready.io,
        mount_ready.network,
        vec![],
        space_snapshot(fixture, None, None, Some(&staging)),
        vec!["read-only mount type=fuse.yinyang".to_owned()],
    );

    let large = mountpoint.join("large.bin");
    let midpoint = (size / 2) as u64;
    let cold_remote_space = space_snapshot(fixture, None, None, None);
    let cold = measure_mount_operation(&meter, mounted.pid(), || {
        let file = File::open(&large).expect("cold open read-only FUSE file");
        let mut value = vec![0_u8; RANGE_BYTES];
        read_exact_at(&file, &mut value, midpoint);
        let staging_bytes = tree_bytes(&staging);
        (file, value, staging_bytes)
    });
    let Measurement {
        value: (cold_file, cold_bytes, cold_staging_bytes),
        latency_ms,
        io,
        network,
    } = cold;
    let cold_space = SpaceSnapshot {
        mount_staging_bytes: cold_staging_bytes,
        ..cold_remote_space
    };
    assert_eq!(
        cold_bytes,
        expected[midpoint as usize..midpoint as usize + RANGE_BYTES]
    );
    let cold_after = space_snapshot(fixture, None, None, Some(&staging));
    recorder.record(
        context,
        "fuse_read_only_range_cold_4k",
        publication,
        scale,
        dataset.clone(),
        "cold open plus one 4 KiB pread while the descriptor remains open",
        vec![latency_ms],
        io,
        network,
        vec![cold_space, cold_after.clone()],
        cold_after,
        vec!["middle 4 KiB range verified".to_owned()],
    );

    let mut warm = OperationBatch::default();
    for index in 0..5 {
        let offset = midpoint + (index * RANGE_BYTES) as u64;
        let measured = measure_mount_operation(&meter, mounted.pid(), || {
            let mut value = vec![0_u8; RANGE_BYTES];
            read_exact_at(&cold_file, &mut value, offset);
            assert_eq!(
                value,
                expected[offset as usize..offset as usize + RANGE_BYTES]
            );
        });
        warm.push(measured);
    }
    warm.record(
        recorder,
        context,
        "fuse_read_only_range_warm_4k",
        publication,
        scale,
        dataset.clone(),
        "five 4 KiB reads on the already-open read-only descriptor",
        space_snapshot(fixture, None, None, Some(&staging)),
        vec!["all warm range bytes verified".to_owned()],
    );
    drop(cold_file);
    mounted.unmount();

    let (mut restarted, restart_ready) = FuseMount::start(fixture, &config, &mountpoint, &meter);
    recorder.record(
        context,
        "fuse_read_only_range_restart_mount_ready",
        publication,
        scale,
        dataset.clone(),
        "interval begins before the replacement read-only mount spawn and ends when fuse.yinyang appears in /proc/mounts",
        vec![restart_ready.latency_ms],
        restart_ready.io,
        restart_ready.network,
        vec![],
        space_snapshot(fixture, None, None, Some(&staging)),
        vec!["replacement mount type=fuse.yinyang and read-only policy".to_owned()],
    );

    let boundary = (RANGE_BYTES / 2) as u64;
    let boundary_offset = BLOCK_BYTES as u64 - boundary;
    let boundary_length = RANGE_BYTES * 2;
    let boundary_remote_space = space_snapshot(fixture, None, None, None);
    let crossing = measure_mount_operation(&meter, restarted.pid(), || {
        let file = File::open(&large).expect("open read-only boundary file after restart");
        let mut value = vec![0_u8; boundary_length];
        read_exact_at(&file, &mut value, boundary_offset);
        let staging_bytes = tree_bytes(&staging);
        (file, value, staging_bytes)
    });
    let Measurement {
        value: (boundary_file, boundary_bytes, boundary_staging_bytes),
        latency_ms,
        io,
        network,
    } = crossing;
    let boundary_space = SpaceSnapshot {
        mount_staging_bytes: boundary_staging_bytes,
        ..boundary_remote_space
    };
    assert_eq!(
        boundary_bytes,
        expected[boundary_offset as usize..boundary_offset as usize + boundary_length],
        "restarted read-only mount did not preserve bytes across a verification-unit boundary"
    );
    drop(boundary_file);
    let boundary_dataset = Dataset {
        range_bytes: Some(boundary_length as u64),
        ..dataset
    };
    let boundary_after = space_snapshot(fixture, None, None, Some(&staging));
    recorder.record(
        context,
        "fuse_read_only_range_restart_boundary_crossing",
        publication,
        scale,
        boundary_dataset,
        "cold open after restart plus one 8 KiB pread spanning a 64 KiB verification-unit boundary",
        vec![latency_ms],
        io,
        network,
        vec![boundary_space, boundary_after.clone()],
        boundary_after,
        vec!["boundary-crossing range bytes verified".to_owned()],
    );
    restarted.unmount();
}

fn run_repeated_random_matrix(
    context: &RunContext,
    publication: Publication,
    scale: Scale,
    telemetry_base: &Path,
    recorder: &mut Recorder,
) {
    assert_eq!(
        publication,
        Publication::Object,
        "repeated-random measurement uses the object authority"
    );
    let fixture = Fixture::new_with_storage_endpoint(publication, None);
    let size = scale.file_size();
    let expected = payload(size, 0x6d);
    fixture.remote_create_file("large.bin", &expected, false);
    let (config, staging, mountpoint) = mount_paths(
        &fixture,
        &format!(
            "repeated-random-{}-{}",
            publication.as_str(),
            scale.as_str()
        ),
        true,
    );
    fs::create_dir(&mountpoint).expect("create repeated-random benchmark mountpoint");
    let telemetry_path = PathBuf::from(format!(
        "{}-{}-{}.json",
        telemetry_base.display(),
        publication.as_str(),
        scale.as_str()
    ));
    let telemetry = TelemetryMeter::new(telemetry_path);
    let meter = NetworkMeter {
        storage: NetworkSource::Telemetry(&telemetry),
        service: None,
    };
    let (mut mounted, mount_ready) = FuseMount::start(&fixture, &config, &mountpoint, &meter);
    let large = mountpoint.join("large.bin");
    let base_dataset = Dataset {
        kind: "single_large_read_only_file_repeated_random".to_owned(),
        files: 1,
        directories: 0,
        logical_bytes: size as u64,
        file_size_bytes: Some(size as u64),
        range_bytes: Some(RANGE_BYTES as u64),
        writers: None,
        read_pattern: Some("cold and repeated same-offset reads".to_owned()),
        working_set_bytes: Some(size as u64),
        random_seed: Some(RANDOM_SEED),
    };
    recorder.record(
        context,
        "fuse_repeated_random_mount_ready",
        publication,
        scale,
        base_dataset.clone(),
        "interval begins before yy mount spawn and ends when fuse.yinyang appears in /proc/mounts; telemetry and RSS settle after latency stops",
        vec![mount_ready.latency_ms],
        mount_ready.io,
        mount_ready.network,
        vec![],
        space_snapshot(&fixture, None, None, Some(&staging)),
        vec!["read-only mount type=fuse.yinyang".to_owned()],
    );

    let midpoint = (size / 2) as u64;
    let (cold_file, cold) = measure_telemetry_single(&meter, mounted.pid(), || {
        let file = File::open(&large).expect("cold open repeated-random FUSE file");
        let mut value = vec![0_u8; RANGE_BYTES];
        read_exact_at(&file, &mut value, midpoint);
        assert_eq!(
            value,
            expected[midpoint as usize..midpoint as usize + RANGE_BYTES]
        );
        file
    });
    assert!(
        cold.network.get > 0,
        "cold read must not be satisfied by an application cache"
    );
    cold.record(
        recorder,
        context,
        "fuse_repeated_random_same_range_cold_4k",
        publication,
        scale,
        base_dataset.clone(),
        "cold open plus one 4 KiB pread at the midpoint",
        space_snapshot(&fixture, None, None, Some(&staging)),
        vec!["cold midpoint range verified".to_owned()],
    );

    let repeated = measure_telemetry_batch(&meter, mounted.pid(), REPEATED_RANDOM_SAMPLES, |_| {
        let mut value = vec![0_u8; RANGE_BYTES];
        read_exact_at(&cold_file, &mut value, midpoint);
        assert_eq!(
            value,
            expected[midpoint as usize..midpoint as usize + RANGE_BYTES]
        );
    });
    if context.expect_range_reuse {
        assert_eq!(
            repeated.network.requests, 0,
            "same-range warm reads must reuse the exact immutable version"
        );
    }
    repeated.record(
        recorder,
        context,
        "fuse_repeated_random_same_range_warm_4k",
        publication,
        scale,
        Dataset {
            read_pattern: Some(format!(
                "{REPEATED_RANDOM_SAMPLES} repeated 4 KiB reads at one offset"
            )),
            ..base_dataset.clone()
        },
        "all reads use one open descriptor and the same pinned version",
        space_snapshot(&fixture, None, None, Some(&staging)),
        vec!["all repeated range bytes verified".to_owned()],
    );
    let boundary_offset = BLOCK_BYTES as u64 - RANGE_BYTES as u64 / 2;
    let boundary_length = RANGE_BYTES * 2;
    let (boundary_file, boundary_cold) = measure_telemetry_single(&meter, mounted.pid(), || {
        let file = File::open(&large).expect("open repeated-random boundary-crossing FUSE file");
        let mut value = vec![0_u8; boundary_length];
        read_exact_at(&file, &mut value, boundary_offset);
        assert_eq!(
            value,
            expected[boundary_offset as usize..boundary_offset as usize + boundary_length]
        );
        file
    });
    assert!(
        boundary_cold.network.get > 0,
        "cold boundary-crossing read must reach storage"
    );
    let boundary_dataset = Dataset {
        range_bytes: Some(boundary_length as u64),
        read_pattern: Some("one cold 8 KiB read spanning a 64 KiB unit boundary".to_owned()),
        ..base_dataset.clone()
    };
    boundary_cold.record(
        recorder,
        context,
        "fuse_repeated_random_boundary_cold_8k",
        publication,
        scale,
        boundary_dataset.clone(),
        "cold open plus one 8 KiB pread spanning a verification-unit boundary",
        space_snapshot(&fixture, None, None, Some(&staging)),
        vec!["boundary-crossing bytes verified".to_owned()],
    );
    let boundary_warm =
        measure_telemetry_batch(&meter, mounted.pid(), REPEATED_RANDOM_SAMPLES, |_| {
            let mut value = vec![0_u8; boundary_length];
            read_exact_at(&boundary_file, &mut value, boundary_offset);
            assert_eq!(
                value,
                expected[boundary_offset as usize..boundary_offset as usize + boundary_length]
            );
        });
    if context.expect_range_reuse {
        assert_eq!(
            boundary_warm.network.requests, 0,
            "warm boundary reads must reuse both immutable units and their proofs"
        );
    }
    boundary_warm.record(
        recorder,
        context,
        "fuse_repeated_random_boundary_warm_8k",
        publication,
        scale,
        boundary_dataset,
        "all reads use one open descriptor and the same pinned version",
        space_snapshot(&fixture, None, None, Some(&staging)),
        vec!["all boundary-crossing range bytes verified".to_owned()],
    );
    drop(boundary_file);

    let working_units = 8 * 1024 * 1024 / BLOCK_BYTES;
    let fill_units = shuffled_units(working_units, RANDOM_SEED);
    let fill = measure_telemetry_batch(&meter, mounted.pid(), fill_units.len(), |sample| {
        let offset = unit_offset(fill_units[sample]);
        let mut value = vec![0_u8; RANGE_BYTES];
        read_exact_at(&cold_file, &mut value, offset);
        assert_eq!(
            value,
            expected[offset as usize..offset as usize + RANGE_BYTES]
        );
    });
    let hot_dataset = Dataset {
        read_pattern: Some(format!(
            "deterministic random 4 KiB reads across a {} byte hot working set",
            8 * 1024 * 1024
        )),
        working_set_bytes: Some(8 * 1024 * 1024),
        ..base_dataset.clone()
    };
    fill.record(
        recorder,
        context,
        "fuse_repeated_random_hot_set_fill_4k",
        publication,
        scale,
        hot_dataset.clone(),
        "one read in each distinct hot-set verification unit, in deterministic random order",
        space_snapshot(&fixture, None, None, Some(&staging)),
        vec!["all hot-set fill bytes verified".to_owned()],
    );
    let hot_units = shuffled_units(working_units, RANDOM_SEED ^ 0xa5a5_a5a5);
    let hot_warm =
        measure_telemetry_batch(&meter, mounted.pid(), REPEATED_RANDOM_SAMPLES, |sample| {
            let offset = unit_offset(hot_units[sample]);
            let mut value = vec![0_u8; RANGE_BYTES];
            read_exact_at(&cold_file, &mut value, offset);
            assert_eq!(
                value,
                expected[offset as usize..offset as usize + RANGE_BYTES]
            );
        });
    if context.expect_range_reuse {
        assert_eq!(
            hot_warm.network.requests, 0,
            "the filled hot working set must remain within the configured reuse bound"
        );
    }
    hot_warm.record(
        recorder,
        context,
        "fuse_repeated_random_hot_set_warm_4k",
        publication,
        scale,
        hot_dataset,
        "deterministic random reads drawn from the filled hot working set",
        space_snapshot(&fixture, None, None, Some(&staging)),
        vec!["all warm hot-set range bytes verified".to_owned()],
    );

    if scale == Scale::Medium {
        let units = size / BLOCK_BYTES;
        let full_units = shuffled_units(units, RANDOM_SEED ^ 0x5a5a_5a5a);
        let full_random = measure_telemetry_batch(&meter, mounted.pid(), 288, |sample| {
            let offset = unit_offset(full_units[sample]);
            let mut value = vec![0_u8; RANGE_BYTES];
            read_exact_at(&cold_file, &mut value, offset);
            assert_eq!(
                value,
                expected[offset as usize..offset as usize + RANGE_BYTES]
            );
        });
        full_random.record(
            recorder,
            context,
            "fuse_repeated_random_over_capacity_4k",
            publication,
            scale,
            Dataset {
                read_pattern: Some(
                    "288 deterministic random 4 KiB reads across the complete 32 MiB file"
                        .to_owned(),
                ),
                working_set_bytes: Some(size as u64),
                ..base_dataset.clone()
            },
            "random offsets exceed any intended bounded content-reuse working set",
            space_snapshot(&fixture, None, None, Some(&staging)),
            vec!["all over-capacity range bytes verified".to_owned()],
        );
    }
    drop(cold_file);
    mounted.unmount();

    telemetry.reset();
    let (mut restarted, restart_ready) = FuseMount::start(&fixture, &config, &mountpoint, &meter);
    recorder.record(
        context,
        "fuse_repeated_random_restart_mount_ready",
        publication,
        scale,
        base_dataset.clone(),
        "interval begins before the replacement mount spawn and ends when fuse.yinyang appears; telemetry and RSS settle after latency stops",
        vec![restart_ready.latency_ms],
        restart_ready.io,
        restart_ready.network,
        vec![],
        space_snapshot(&fixture, None, None, Some(&staging)),
        vec!["replacement read-only mount type=fuse.yinyang".to_owned()],
    );
    let (restart_file, restart_cold) = measure_telemetry_single(&meter, restarted.pid(), || {
        let file = File::open(&large).expect("open after repeated-random restart");
        let mut value = vec![0_u8; RANGE_BYTES];
        read_exact_at(&file, &mut value, midpoint);
        assert_eq!(
            value,
            expected[midpoint as usize..midpoint as usize + RANGE_BYTES]
        );
        file
    });
    assert!(
        restart_cold.network.get > 0,
        "restart cold read must not reuse a previous process's content"
    );
    restart_cold.record(
        recorder,
        context,
        "fuse_repeated_random_restart_cold_4k",
        publication,
        scale,
        Dataset {
            read_pattern: Some("cold read in a replacement mount process".to_owned()),
            ..base_dataset.clone()
        },
        "replacement mount observes the same immutable version from storage",
        space_snapshot(&fixture, None, None, Some(&staging)),
        vec!["restart cold range bytes verified".to_owned()],
    );

    if scale == Scale::Medium {
        let sentinel = unit_offset(0);
        let sentinel_file = File::open(&large).expect("open eviction sentinel file");
        let before = measure_telemetry_batch(&meter, restarted.pid(), 8, |_| {
            let mut value = vec![0_u8; RANGE_BYTES];
            read_exact_at(&sentinel_file, &mut value, sentinel);
            assert_eq!(
                value,
                expected[sentinel as usize..sentinel as usize + RANGE_BYTES]
            );
        });
        before.record(
            recorder,
            context,
            "fuse_repeated_random_eviction_sentinel_warm_4k",
            publication,
            scale,
            Dataset {
                read_pattern: Some("repeated sentinel reads before eviction pressure".to_owned()),
                working_set_bytes: Some(BLOCK_BYTES as u64),
                ..base_dataset.clone()
            },
            "sentinel is read before loading more unique units than the bounded reuse capacity",
            space_snapshot(&fixture, None, None, Some(&staging)),
            vec!["sentinel bytes verified before eviction pressure".to_owned()],
        );

        let pressure = measure_telemetry_batch(&meter, restarted.pid(), 272, |sample| {
            let offset = unit_offset(sample + 1);
            let mut value = vec![0_u8; RANGE_BYTES];
            read_exact_at(&sentinel_file, &mut value, offset);
            assert_eq!(
                value,
                expected[offset as usize..offset as usize + RANGE_BYTES]
            );
        });
        pressure.record(
            recorder,
            context,
            "fuse_repeated_random_eviction_pressure_4k",
            publication,
            scale,
            Dataset {
                read_pattern: Some(
                    "272 distinct verification units loaded after the sentinel".to_owned(),
                ),
                working_set_bytes: Some(272 * BLOCK_BYTES as u64),
                ..base_dataset.clone()
            },
            "the loaded units exceed a 16 MiB intended content bound before the sentinel is reread",
            space_snapshot(&fixture, None, None, Some(&staging)),
            vec!["all eviction-pressure range bytes verified".to_owned()],
        );
        let after = measure_telemetry_batch(&meter, restarted.pid(), 8, |_| {
            let mut value = vec![0_u8; RANGE_BYTES];
            read_exact_at(&sentinel_file, &mut value, sentinel);
            assert_eq!(
                value,
                expected[sentinel as usize..sentinel as usize + RANGE_BYTES]
            );
        });
        after.record(
            recorder,
            context,
            "fuse_repeated_random_eviction_sentinel_reread_4k",
            publication,
            scale,
            Dataset {
                read_pattern: Some("repeated sentinel reads after eviction pressure".to_owned()),
                working_set_bytes: Some(BLOCK_BYTES as u64),
                ..base_dataset.clone()
            },
            "the first sample must observe the evicted sentinel again; later samples may reuse it",
            space_snapshot(&fixture, None, None, Some(&staging)),
            vec!["sentinel bytes verified after eviction pressure".to_owned()],
        );
        drop(sentinel_file);
    }
    drop(restart_file);

    let old_bytes = payload(256 * 1024, 0x31);
    let new_bytes = payload(256 * 1024 + 17, 0x71);
    fixture.remote_create_file("replace.bin", &old_bytes, false);
    let old = File::open(mountpoint.join("replace.bin")).expect("open old replacement descriptor");
    let old_offset = 3 * RANGE_BYTES as u64;
    let old_read = measure_telemetry_batch(&meter, restarted.pid(), 2, |_| {
        let mut value = vec![0_u8; RANGE_BYTES];
        read_exact_at(&old, &mut value, old_offset);
        assert_eq!(
            value,
            old_bytes[old_offset as usize..old_offset as usize + RANGE_BYTES]
        );
    });
    old_read.record(
        recorder,
        context,
        "fuse_repeated_random_old_descriptor_warm_4k",
        publication,
        scale,
        Dataset {
            kind: "single_replaced_read_only_file".to_owned(),
            logical_bytes: old_bytes.len() as u64,
            file_size_bytes: Some(old_bytes.len() as u64),
            read_pattern: Some("repeated old-version reads".to_owned()),
            working_set_bytes: Some(BLOCK_BYTES as u64),
            ..base_dataset.clone()
        },
        "the old descriptor remains pinned before the external replacement and refresh",
        space_snapshot(&fixture, None, None, Some(&staging)),
        vec!["old descriptor bytes verified".to_owned()],
    );
    fixture.remote_remove("replace.bin");
    fixture.remote_create_file("replace.bin", &new_bytes, false);
    restarted.refresh_sighup();
    restarted.wait_for_output_line("yy: fuse refresh: done (", Duration::from_secs(30));
    let fresh = measure_telemetry_batch(&meter, restarted.pid(), 1, |_| {
        let file = File::open(mountpoint.join("replace.bin")).expect("fresh replacement open");
        let mut value = vec![0_u8; RANGE_BYTES];
        read_exact_at(&file, &mut value, old_offset);
        assert_eq!(
            value,
            new_bytes[old_offset as usize..old_offset as usize + RANGE_BYTES]
        );
    });
    assert!(
        fresh.network.get > 0,
        "fresh replacement open must not reuse the old logical version"
    );
    fresh.record(
        recorder,
        context,
        "fuse_repeated_random_fresh_descriptor_cold_4k",
        publication,
        scale,
        Dataset {
            kind: "single_replaced_read_only_file".to_owned(),
            logical_bytes: new_bytes.len() as u64,
            file_size_bytes: Some(new_bytes.len() as u64),
            read_pattern: Some("fresh open after replacement and SIGHUP".to_owned()),
            working_set_bytes: Some(BLOCK_BYTES as u64),
            ..base_dataset.clone()
        },
        "fresh path open observes the replacement version after explicit refresh",
        space_snapshot(&fixture, None, None, Some(&staging)),
        vec!["fresh replacement bytes verified".to_owned()],
    );
    let retained = measure_telemetry_batch(&meter, restarted.pid(), 1, |_| {
        let mut value = vec![0_u8; RANGE_BYTES];
        read_exact_at(&old, &mut value, old_offset);
        assert_eq!(
            value,
            old_bytes[old_offset as usize..old_offset as usize + RANGE_BYTES]
        );
    });
    retained.record(
        recorder,
        context,
        "fuse_repeated_random_old_descriptor_after_refresh_4k",
        publication,
        scale,
        Dataset {
            kind: "single_replaced_read_only_file".to_owned(),
            logical_bytes: old_bytes.len() as u64,
            file_size_bytes: Some(old_bytes.len() as u64),
            read_pattern: Some("old descriptor read after replacement and SIGHUP".to_owned()),
            working_set_bytes: Some(BLOCK_BYTES as u64),
            ..base_dataset
        },
        "the pre-replacement descriptor retains its exact immutable bytes",
        space_snapshot(&fixture, None, None, Some(&staging)),
        vec!["old descriptor bytes remained pinned after refresh".to_owned()],
    );
    drop(old);
    restarted.unmount();
}

fn shuffled_units(count: usize, seed: u64) -> Vec<usize> {
    let mut units = (0..count).collect::<Vec<_>>();
    let mut state = seed | 1;
    for index in (1..units.len()).rev() {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        let selected = (state as usize) % (index + 1);
        units.swap(index, selected);
    }
    units
}

fn unit_offset(unit: usize) -> u64 {
    unit as u64 * BLOCK_BYTES as u64 + 3 * RANGE_BYTES as u64
}

fn run_concurrent_matrix(
    context: &RunContext,
    proxy: &Arc<CountingProxy>,
    publication: Publication,
    scale: Scale,
    recorder: &mut Recorder,
) {
    let bench = BenchFixture::new(publication, proxy.endpoint());
    let fixture = &bench.fixture;
    let meter = bench.network(proxy);
    fixture.remote_create_directory("concurrent");
    let (config, staging, mountpoint) = mount_paths(
        fixture,
        &format!("concurrent-{}-{}", publication.as_str(), scale.as_str()),
        false,
    );
    fs::create_dir(&mountpoint).expect("create concurrent mountpoint");
    let (mut mounted, _) = FuseMount::start(fixture, &config, &mountpoint, &meter);
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
        read_pattern: None,
        working_set_bytes: None,
        random_seed: None,
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
    for (index, value) in expected.iter().cloned().enumerate() {
        let start = start.clone();
        let path = mountpoint.join(format!("concurrent/writer-{index:02}.bin"));
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
    let net_before = meter.snapshot();
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
    let network = meter.snapshot().since(net_before);
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
        space_snapshot(fixture, None, None, Some(&staging)),
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
    let meter = NetworkMeter {
        storage: NetworkSource::Proxy(proxy),
        service: None,
    };
    let source = fixture.path(format!("publish-source-{}", scale.as_str()));
    populate_small_tree(&source, scale);
    let dataset = tree_dataset(&source);
    let publish = run_yy_operation(
        &meter,
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
        &meter,
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
        read_pattern: None,
        working_set_bytes: None,
        random_seed: None,
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

fn mount_paths(fixture: &Fixture, name: &str, read_only: bool) -> (PathBuf, PathBuf, PathBuf) {
    let staging = fixture.path(format!("{name}-state"));
    let mountpoint = fixture.path(format!("{name}-mount"));
    let config = fixture.path(format!("{name}.json"));
    let storage_profile = match std::env::var("YINYANG_STORAGE_PROFILE").as_deref() {
        Ok("minio") => "minio",
        Ok("amazon-s3") => "amazon-s3",
        value => panic!("YINYANG_STORAGE_PROFILE must be minio or amazon-s3, got {value:?}"),
    };
    let mut value = serde_json::json!({
        "name": name,
        "model": "managed",
        "access": "mount",
        "frontend": "fuse",
        "publication": fixture.publication.as_str(),
        "storage_profile": storage_profile,
        "staging": staging,
        "read_only": read_only,
        "require": [],
    });
    if fixture.publication == Publication::Service {
        value["service_address"] =
            serde_json::Value::String(fixture.service_client_address().to_string());
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
    stderr_seen: Arc<Mutex<Vec<u8>>>,
    mountpoint: PathBuf,
    finished: bool,
}

impl FuseMount {
    fn start(
        fixture: &Fixture,
        config: &Path,
        mountpoint: &Path,
        network: &NetworkMeter<'_>,
    ) -> (Self, Measurement<()>) {
        let network_before = network.snapshot();
        let started = Instant::now();
        let mut command =
            fixture.yy_command(&["--volume", path_str(config), "mount", path_str(mountpoint)]);
        if let Some(path) = network.telemetry_path() {
            command.env("YINYANG_BENCH_METRICS_FILE", path);
        }
        command.stdout(Stdio::piped()).stderr(Stdio::piped());
        let mut child = command.spawn().expect("spawn benchmark yy mount");
        let stdout = child.stdout.take().expect("mount stdout is piped");
        let stderr = child.stderr.take().expect("mount stderr is piped");
        let stderr_seen = Arc::new(Mutex::new(Vec::new()));
        let stderr_copy = stderr_seen.clone();
        let mut mounted = Self {
            child,
            stdout: Some(thread::spawn(move || read_all(stdout))),
            stderr: Some(thread::spawn(move || {
                read_all_observed(stderr, stderr_copy)
            })),
            stderr_seen,
            mountpoint: mountpoint.to_path_buf(),
            finished: false,
        };
        let mut io = read_proc_io(mounted.pid());
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            if mount_type(mountpoint).as_deref() == Some("fuse.yinyang") {
                let current = read_proc_io(mounted.pid());
                if current != IoSnapshot::default() {
                    io = current;
                }
                let latency_ms = started.elapsed().as_secs_f64() * 1000.0;
                network.settle();
                let measurement = Measurement {
                    value: (),
                    latency_ms,
                    io,
                    network: network.snapshot().since(network_before),
                };
                return (mounted, measurement);
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
            let current = read_proc_io(mounted.pid());
            if current != IoSnapshot::default() {
                io = current;
            }
            thread::sleep(Duration::from_millis(25));
        }
    }

    fn pid(&self) -> u32 {
        self.child.id()
    }

    fn refresh_sighup(&self) {
        kill(Pid::from_raw(self.pid() as i32), Signal::SIGHUP)
            .expect("send SIGHUP to benchmark mount");
    }

    fn wait_for_output_line(&self, needle: &str, timeout: Duration) -> String {
        let deadline = Instant::now() + timeout;
        loop {
            let output = String::from_utf8_lossy(
                &self
                    .stderr_seen
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner()),
            )
            .into_owned();
            if output
                .lines()
                .any(|line| line.contains(needle) && line.trim_end().ends_with(')'))
            {
                return output;
            }
            assert!(
                Instant::now() < deadline,
                "benchmark mount did not print {needle:?} within {timeout:?}:\n{output}"
            );
            thread::sleep(Duration::from_millis(25));
        }
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
        let _ = self
            .stderr
            .take()
            .and_then(|thread| thread.join().ok())
            .unwrap_or_default();
        self.stderr_seen
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
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
    network: &NetworkMeter<'_>,
    pid: u32,
    operation: impl FnOnce() -> T,
) -> Measurement<T> {
    let net_before = network.snapshot();
    let io_before = read_proc_io(pid);
    let started = Instant::now();
    let value = operation();
    let latency_ms = started.elapsed().as_secs_f64() * 1000.0;
    Measurement {
        value,
        latency_ms,
        io: read_proc_io(pid).since(io_before),
        network: network.snapshot().since(net_before),
    }
}

fn measure_telemetry_batch(
    network: &NetworkMeter<'_>,
    pid: u32,
    samples: usize,
    mut operation: impl FnMut(usize),
) -> OperationBatch {
    assert!(
        samples > 0,
        "telemetry batch must contain at least one sample"
    );
    network.settle();
    let net_before = network.snapshot();
    let io_before = read_proc_io(pid);
    let mut latencies = Vec::with_capacity(samples);
    for sample in 0..samples {
        let started = Instant::now();
        operation(sample);
        latencies.push(started.elapsed().as_secs_f64() * 1000.0);
    }
    let io = read_proc_io(pid).since(io_before);
    network.settle();
    let network = network.snapshot().since(net_before);
    OperationBatch {
        latencies,
        io,
        network,
    }
}

fn measure_telemetry_single<T>(
    network: &NetworkMeter<'_>,
    pid: u32,
    operation: impl FnOnce() -> T,
) -> (T, OperationBatch) {
    network.settle();
    let net_before = network.snapshot();
    let io_before = read_proc_io(pid);
    let started = Instant::now();
    let value = operation();
    let latency_ms = started.elapsed().as_secs_f64() * 1000.0;
    let io = read_proc_io(pid).since(io_before);
    network.settle();
    let network = network.snapshot().since(net_before);
    (
        value,
        OperationBatch {
            latencies: vec![latency_ms],
            io,
            network,
        },
    )
}

fn run_yy_operation(
    network: &NetworkMeter<'_>,
    command: Command,
    context: &str,
) -> Measurement<()> {
    let net_before = network.snapshot();
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
        network: network.snapshot().since(net_before),
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

fn read_all_observed(mut reader: impl Read, observed: Arc<Mutex<Vec<u8>>>) -> Vec<u8> {
    let mut bytes = Vec::new();
    let mut buffer = [0_u8; 4096];
    loop {
        match reader.read(&mut buffer) {
            Ok(0) => break,
            Ok(count) => {
                bytes.extend_from_slice(&buffer[..count]);
                observed
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .extend_from_slice(&buffer[..count]);
            }
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
            Err(_) => break,
        }
    }
    bytes
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize)]
struct IoSnapshot {
    rchar: u64,
    wchar: u64,
    syscr: u64,
    syscw: u64,
    read_bytes: u64,
    write_bytes: u64,
    rss_kib: u64,
    peak_rss_kib: u64,
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
            rss_kib: self.rss_kib,
            peak_rss_kib: self.peak_rss_kib,
        }
    }

    fn add(&mut self, other: Self) {
        self.rchar = self.rchar.saturating_add(other.rchar);
        self.wchar = self.wchar.saturating_add(other.wchar);
        self.syscr = self.syscr.saturating_add(other.syscr);
        self.syscw = self.syscw.saturating_add(other.syscw);
        self.read_bytes = self.read_bytes.saturating_add(other.read_bytes);
        self.write_bytes = self.write_bytes.saturating_add(other.write_bytes);
        self.rss_kib = self.rss_kib.max(other.rss_kib);
        self.peak_rss_kib = self.peak_rss_kib.max(other.peak_rss_kib);
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
    if let Ok(text) = fs::read_to_string(format!("/proc/{pid}/status")) {
        for line in text.lines() {
            let Some((key, raw)) = line.split_once(':') else {
                continue;
            };
            let number = raw
                .split_whitespace()
                .next()
                .and_then(|value| value.parse::<u64>().ok())
                .unwrap_or_default();
            match key {
                "VmRSS" => value.rss_kib = number,
                "VmHWM" => value.peak_rss_kib = number,
                _ => {}
            }
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
    get_response_body_bytes: u64,
    put_request_body_bytes: u64,
    service_connections: u64,
    service_request_bytes: u64,
    service_response_bytes: u64,
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
            get_response_body_bytes: self
                .get_response_body_bytes
                .saturating_sub(before.get_response_body_bytes),
            put_request_body_bytes: self
                .put_request_body_bytes
                .saturating_sub(before.put_request_body_bytes),
            service_connections: self
                .service_connections
                .saturating_sub(before.service_connections),
            service_request_bytes: self
                .service_request_bytes
                .saturating_sub(before.service_request_bytes),
            service_response_bytes: self
                .service_response_bytes
                .saturating_sub(before.service_response_bytes),
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
        self.get_response_body_bytes = self
            .get_response_body_bytes
            .saturating_add(other.get_response_body_bytes);
        self.put_request_body_bytes = self
            .put_request_body_bytes
            .saturating_add(other.put_request_body_bytes);
        self.service_connections = self
            .service_connections
            .saturating_add(other.service_connections);
        self.service_request_bytes = self
            .service_request_bytes
            .saturating_add(other.service_request_bytes);
        self.service_response_bytes = self
            .service_response_bytes
            .saturating_add(other.service_response_bytes);
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
    get_response_body_bytes: AtomicU64,
    put_request_body_bytes: AtomicU64,
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
        let address = listener.local_addr().expect("counting proxy address");
        let counters = Arc::new(ProxyCounters::default());
        let stop = Arc::new(AtomicBool::new(false));
        let thread_counters = counters.clone();
        let thread_stop = stop.clone();
        let thread = thread::spawn(move || {
            loop {
                let Ok((client, _)) = listener.accept() else {
                    break;
                };
                if thread_stop.load(Ordering::SeqCst) {
                    break;
                }
                client
                    .set_nodelay(true)
                    .expect("disable Nagle on counting S3 client");
                thread_counters.connections.fetch_add(1, Ordering::Relaxed);
                let counters = thread_counters.clone();
                thread::spawn(move || proxy_connection(client, upstream, counters));
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
            get_response_body_bytes: self
                .counters
                .get_response_body_bytes
                .load(Ordering::Relaxed),
            put_request_body_bytes: self.counters.put_request_body_bytes.load(Ordering::Relaxed),
            service_connections: 0,
            service_request_bytes: 0,
            service_response_bytes: 0,
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

#[derive(Clone, Copy, Debug, Default, Serialize)]
struct ServiceSnapshot {
    connections: u64,
    request_bytes: u64,
    response_bytes: u64,
}

#[derive(Default)]
struct ServiceProxyCounters {
    connections: AtomicU64,
    request_bytes: AtomicU64,
    response_bytes: AtomicU64,
}

struct CountingServiceProxy {
    address: SocketAddr,
    counters: Arc<ServiceProxyCounters>,
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

impl CountingServiceProxy {
    fn start(upstream: SocketAddr) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind counting service proxy");
        let address = listener
            .local_addr()
            .expect("counting service proxy address");
        let counters = Arc::new(ServiceProxyCounters::default());
        let stop = Arc::new(AtomicBool::new(false));
        let thread_counters = counters.clone();
        let thread_stop = stop.clone();
        let thread = thread::spawn(move || {
            loop {
                let Ok((client, _)) = listener.accept() else {
                    break;
                };
                if thread_stop.load(Ordering::SeqCst) {
                    break;
                }
                client
                    .set_nodelay(true)
                    .expect("disable Nagle on counting service client");
                thread_counters.connections.fetch_add(1, Ordering::Relaxed);
                let counters = thread_counters.clone();
                thread::spawn(move || service_proxy_connection(client, upstream, counters));
            }
        });
        Self {
            address,
            counters,
            stop,
            thread: Some(thread),
        }
    }

    fn address(&self) -> SocketAddr {
        self.address
    }

    fn snapshot(&self) -> ServiceSnapshot {
        ServiceSnapshot {
            connections: self.counters.connections.load(Ordering::Relaxed),
            request_bytes: self.counters.request_bytes.load(Ordering::Relaxed),
            response_bytes: self.counters.response_bytes.load(Ordering::Relaxed),
        }
    }
}

impl Drop for CountingServiceProxy {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        let _ = TcpStream::connect(self.address);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

fn service_proxy_connection(
    client: TcpStream,
    upstream: SocketAddr,
    counters: Arc<ServiceProxyCounters>,
) {
    let Ok(server) = TcpStream::connect(upstream) else {
        return;
    };
    server
        .set_nodelay(true)
        .expect("disable Nagle on counting service upstream");
    let (Ok(mut client_read), Ok(mut server_write)) = (client.try_clone(), server.try_clone())
    else {
        return;
    };
    let (Ok(mut server_read), Ok(mut client_write)) = (server.try_clone(), client.try_clone())
    else {
        return;
    };
    let request_counters = counters.clone();
    let request = thread::spawn(move || {
        pump_service(
            &mut client_read,
            &mut server_write,
            &request_counters.request_bytes,
        )
    });
    let response = thread::spawn(move || {
        pump_service(
            &mut server_read,
            &mut client_write,
            &counters.response_bytes,
        )
    });
    let _ = request.join();
    let _ = response.join();
    let _ = client.shutdown(Shutdown::Both);
}

fn pump_service(reader: &mut TcpStream, writer: &mut TcpStream, counter: &AtomicU64) {
    let mut buffer = vec![0_u8; CHUNK];
    loop {
        let count = match reader.read(&mut buffer) {
            Ok(0) => break,
            Ok(count) => count,
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(_) => break,
        };
        counter.fetch_add(count as u64, Ordering::Relaxed);
        if writer.write_all(&buffer[..count]).is_err() {
            break;
        }
    }
    let _ = writer.shutdown(Shutdown::Write);
}

fn proxy_connection(client: TcpStream, upstream: SocketAddr, counters: Arc<ProxyCounters>) {
    let Ok(server) = TcpStream::connect(upstream) else {
        return;
    };
    server
        .set_nodelay(true)
        .expect("disable Nagle on counting S3 upstream");
    let (Ok(mut client_read), Ok(mut server_write)) = (client.try_clone(), server.try_clone())
    else {
        return;
    };
    let (Ok(mut server_read), Ok(mut client_write)) = (server.try_clone(), client.try_clone())
    else {
        return;
    };
    let methods = Arc::new(Mutex::new(VecDeque::new()));
    let request_counters = counters.clone();
    let request_methods = methods.clone();
    let request = thread::spawn(move || {
        pump_request(
            &mut client_read,
            &mut server_write,
            &request_counters,
            &request_methods,
        )
    });
    let response = thread::spawn(move || {
        pump_response(&mut server_read, &mut client_write, &counters, &methods)
    });
    let _ = request.join();
    let _ = response.join();
    let _ = client.shutdown(Shutdown::Both);
}

fn pump_request(
    reader: &mut TcpStream,
    writer: &mut TcpStream,
    counters: &ProxyCounters,
    methods: &Arc<Mutex<VecDeque<HttpMethod>>>,
) {
    let mut buffer = vec![0_u8; CHUNK];
    let mut scanner = RequestScanner::default();
    loop {
        let count = match reader.read(&mut buffer) {
            Ok(0) => break,
            Ok(count) => count,
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(_) => break,
        };
        counters
            .request_bytes
            .fetch_add(count as u64, Ordering::Relaxed);
        scanner.scan(&buffer[..count], counters, methods);
        if writer.write_all(&buffer[..count]).is_err() {
            break;
        }
    }
    let _ = writer.shutdown(Shutdown::Write);
}

fn pump_response(
    reader: &mut TcpStream,
    writer: &mut TcpStream,
    counters: &ProxyCounters,
    methods: &Arc<Mutex<VecDeque<HttpMethod>>>,
) {
    let mut buffer = vec![0_u8; CHUNK];
    let mut scanner = ResponseScanner::new(methods.clone());
    loop {
        let count = match reader.read(&mut buffer) {
            Ok(0) => break,
            Ok(count) => count,
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(_) => break,
        };
        counters
            .response_bytes
            .fetch_add(count as u64, Ordering::Relaxed);
        scanner.scan(&buffer[..count], counters);
        if writer.write_all(&buffer[..count]).is_err() {
            break;
        }
    }
    let _ = writer.shutdown(Shutdown::Write);
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum HttpMethod {
    Get,
    Head,
    Put,
    Post,
    Delete,
    Other,
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
    method: Option<HttpMethod>,
}

impl RequestScanner {
    fn scan(
        &mut self,
        bytes: &[u8],
        counters: &ProxyCounters,
        methods: &Arc<Mutex<VecDeque<HttpMethod>>>,
    ) {
        let mut offset = 0;
        while offset < bytes.len() {
            match self.state {
                ScanState::Body(remaining) | ScanState::ChunkData(remaining) => {
                    let count = remaining.min((bytes.len() - offset) as u64) as usize;
                    offset += count;
                    if self.method == Some(HttpMethod::Put) {
                        counters
                            .put_request_body_bytes
                            .fetch_add(count as u64, Ordering::Relaxed);
                    }
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
                        self.process_line(line, counters, methods);
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

    fn process_line(
        &mut self,
        line: &[u8],
        counters: &ProxyCounters,
        methods: &Arc<Mutex<VecDeque<HttpMethod>>>,
    ) {
        match self.state {
            ScanState::RequestLine => {
                if let Some(method) = count_request_line(line, counters) {
                    self.method = Some(method);
                    methods
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner())
                        .push_back(method);
                } else {
                    self.method = None;
                }
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

fn count_request_line(line: &[u8], counters: &ProxyCounters) -> Option<HttpMethod> {
    if !line.windows(8).any(|window| window == b" HTTP/1.") {
        return None;
    }
    let (method, counter) = if line.starts_with(b"GET ") {
        (HttpMethod::Get, &counters.get)
    } else if line.starts_with(b"HEAD ") {
        (HttpMethod::Head, &counters.head)
    } else if line.starts_with(b"PUT ") {
        (HttpMethod::Put, &counters.put)
    } else if line.starts_with(b"POST ") {
        (HttpMethod::Post, &counters.post)
    } else if line.starts_with(b"DELETE ") {
        (HttpMethod::Delete, &counters.delete)
    } else if [
        b"OPTIONS ".as_slice(),
        b"PATCH ".as_slice(),
        b"CONNECT ".as_slice(),
        b"TRACE ".as_slice(),
    ]
    .iter()
    .any(|method| line.starts_with(method))
    {
        (HttpMethod::Other, &counters.other)
    } else {
        return None;
    };
    counter.fetch_add(1, Ordering::Relaxed);
    counters.requests.fetch_add(1, Ordering::Relaxed);
    Some(method)
}

#[derive(Clone, Copy, Default)]
enum ResponseState {
    #[default]
    StatusLine,
    Headers,
    Body(u64),
    ChunkSize,
    ChunkData(u64),
    ChunkCrlf(u8),
    Trailers,
    UntilClose,
}

struct ResponseScanner {
    state: ResponseState,
    line: Vec<u8>,
    content_length: Option<u64>,
    chunked: bool,
    status: Option<u16>,
    method: Option<HttpMethod>,
    methods: Arc<Mutex<VecDeque<HttpMethod>>>,
}

impl ResponseScanner {
    fn new(methods: Arc<Mutex<VecDeque<HttpMethod>>>) -> Self {
        Self {
            state: ResponseState::StatusLine,
            line: Vec::new(),
            content_length: None,
            chunked: false,
            status: None,
            method: None,
            methods,
        }
    }

    fn scan(&mut self, bytes: &[u8], counters: &ProxyCounters) {
        let mut offset = 0;
        while offset < bytes.len() {
            match self.state {
                ResponseState::Body(remaining) | ResponseState::ChunkData(remaining) => {
                    let count = remaining.min((bytes.len() - offset) as u64) as usize;
                    self.count_body(count, counters);
                    offset += count;
                    if count as u64 == remaining {
                        self.state = if matches!(self.state, ResponseState::Body(_)) {
                            self.reset_response();
                            ResponseState::StatusLine
                        } else {
                            ResponseState::ChunkCrlf(2)
                        };
                    } else {
                        self.state = match self.state {
                            ResponseState::Body(_) => ResponseState::Body(remaining - count as u64),
                            ResponseState::ChunkData(_) => {
                                ResponseState::ChunkData(remaining - count as u64)
                            }
                            _ => unreachable!(),
                        };
                    }
                }
                ResponseState::ChunkCrlf(remaining) => {
                    let count = usize::from(remaining).min(bytes.len() - offset);
                    offset += count;
                    let remaining = remaining - count as u8;
                    self.state = if remaining == 0 {
                        ResponseState::ChunkSize
                    } else {
                        ResponseState::ChunkCrlf(remaining)
                    };
                }
                ResponseState::UntilClose => {
                    let count = bytes.len() - offset;
                    self.count_body(count, counters);
                    offset = bytes.len();
                }
                ResponseState::StatusLine
                | ResponseState::Headers
                | ResponseState::ChunkSize
                | ResponseState::Trailers => {
                    let end = bytes[offset..]
                        .iter()
                        .position(|byte| *byte == b'\n')
                        .map(|position| offset + position);
                    if let Some(end) = end {
                        self.line.extend_from_slice(&bytes[offset..end]);
                        let line = std::mem::take(&mut self.line);
                        let line = line.strip_suffix(b"\r").unwrap_or(&line);
                        self.process_line(line);
                        offset = end + 1;
                    } else {
                        self.line.extend_from_slice(&bytes[offset..]);
                        offset = bytes.len();
                        if self.line.len() > 16 * 1024 {
                            self.line.clear();
                            self.state = ResponseState::StatusLine;
                        }
                    }
                }
            }
        }
    }

    fn process_line(&mut self, line: &[u8]) {
        match self.state {
            ResponseState::StatusLine => {
                if let Some(status) = parse_response_status(line)
                    && status >= 200
                {
                    self.status = Some(status);
                    self.method = self
                        .methods
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner())
                        .pop_front();
                }
                self.content_length = None;
                self.chunked = false;
                self.state = ResponseState::Headers;
            }
            ResponseState::Headers if line.is_empty() => {
                let status = self.status.unwrap_or_default();
                let body_forbidden = status < 200
                    || status == 204
                    || status == 304
                    || self.method == Some(HttpMethod::Head);
                self.state = if body_forbidden {
                    self.reset_response();
                    ResponseState::StatusLine
                } else if self.chunked {
                    ResponseState::ChunkSize
                } else if let Some(length) = self.content_length {
                    if length == 0 {
                        self.reset_response();
                        ResponseState::StatusLine
                    } else {
                        ResponseState::Body(length)
                    }
                } else {
                    ResponseState::UntilClose
                };
            }
            ResponseState::Headers => {
                let header = String::from_utf8_lossy(line).to_ascii_lowercase();
                if let Some(value) = header.strip_prefix("content-length:") {
                    self.content_length = value.trim().parse().ok();
                } else if header.starts_with("transfer-encoding:") && header.contains("chunked") {
                    self.chunked = true;
                }
            }
            ResponseState::ChunkSize => {
                let size = std::str::from_utf8(line)
                    .ok()
                    .and_then(|line| line.split(';').next())
                    .and_then(|size| u64::from_str_radix(size.trim(), 16).ok())
                    .unwrap_or_else(|| panic!("invalid HTTP chunk size in benchmark response"));
                self.state = if size == 0 {
                    ResponseState::Trailers
                } else {
                    ResponseState::ChunkData(size)
                };
            }
            ResponseState::Trailers if line.is_empty() => {
                self.reset_response();
                self.state = ResponseState::StatusLine;
            }
            ResponseState::Trailers => {}
            ResponseState::Body(_)
            | ResponseState::ChunkData(_)
            | ResponseState::ChunkCrlf(_)
            | ResponseState::UntilClose => {
                unreachable!("fixed-width response states do not process lines")
            }
        }
    }

    fn count_body(&self, count: usize, counters: &ProxyCounters) {
        if self.method == Some(HttpMethod::Get) {
            counters
                .get_response_body_bytes
                .fetch_add(count as u64, Ordering::Relaxed);
        }
    }

    fn reset_response(&mut self) {
        self.status = None;
        self.method = None;
        self.content_length = None;
        self.chunked = false;
    }
}

fn parse_response_status(line: &[u8]) -> Option<u16> {
    let line = std::str::from_utf8(line).ok()?;
    let status = if let Some(status) = line.strip_prefix("HTTP/1.1 ") {
        status
    } else {
        line.strip_prefix("HTTP/1.0 ")?
    };
    status.get(..3)?.parse().ok()
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
        .map(|metadata| metadata.st_blocks().saturating_mul(512))
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
        let entries = operator
            .list_with("")
            .recursive(true)
            .await
            .expect("list persistent benchmark storage");
        let mut bytes = 0_u64;
        let mut objects = 0_u64;
        for entry in entries {
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
    read_pattern: Option<String>,
    working_set_bytes: Option<u64>,
    random_seed: Option<u64>,
}

impl Dataset {
    fn useful_bytes(&self) -> u64 {
        self.working_set_bytes
            .or(self.range_bytes)
            .unwrap_or(self.logical_bytes)
    }
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
struct NetworkPerSample {
    s3_requests: f64,
    s3_request_bytes: f64,
    s3_response_bytes: f64,
    get_response_body_bytes: f64,
    put_request_body_bytes: f64,
    service_connections: f64,
    service_request_bytes: f64,
    service_response_bytes: f64,
    note: &'static str,
}

impl NetworkPerSample {
    fn new(network: &NetworkSnapshot, samples: usize) -> Self {
        let samples = samples.max(1) as f64;
        Self {
            s3_requests: network.requests as f64 / samples,
            s3_request_bytes: network.request_bytes as f64 / samples,
            s3_response_bytes: network.response_bytes as f64 / samples,
            get_response_body_bytes: network.get_response_body_bytes as f64 / samples,
            put_request_body_bytes: network.put_request_body_bytes as f64 / samples,
            service_connections: network.service_connections as f64 / samples,
            service_request_bytes: network.service_request_bytes as f64 / samples,
            service_response_bytes: network.service_response_bytes as f64 / samples,
            note: "per latency sample; concurrent samples overlap and share the batch network total",
        }
    }
}

fn cache_policy(context: &RunContext, scenario: &str) -> String {
    if context.workload == Workload::RepeatedRandom && scenario.starts_with("fuse_") {
        return context.range_cache_policy.clone();
    }
    if scenario.starts_with("fuse_") {
        "FUSE FOPEN_DIRECT_IO; no kernel page cache for file pages; clean nodes are reclaimed on last release".to_owned()
    } else if scenario.starts_with("sync_") {
        "one CLI process per finite run; no OS page-cache drop and no cross-run content cache state"
            .to_owned()
    } else {
        "one CLI process; no OS page-cache drop and no retained application cache".to_owned()
    }
}

fn volume_mode(scenario: &str) -> &'static str {
    if scenario.starts_with("fuse_read_only_") {
        "read_only"
    } else {
        "read_write"
    }
}

#[derive(Serialize)]
struct CostRecord {
    schema: &'static str,
    scenario: String,
    workload: String,
    volume_mode: &'static str,
    profile: String,
    publication_filter: String,
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
    storage_backend: String,
    metadata_transport: String,
    network_source: String,
    cache_policy: String,
    incremental_expectation: String,
    dataset: Dataset,
    useful_bytes: u64,
    completion: String,
    iterations: usize,
    wall_ms: f64,
    latency: LatencyStats,
    latency_samples_ms: Vec<f64>,
    local_process_io: IoSnapshot,
    local_process_io_note: String,
    network: NetworkSnapshot,
    network_per_sample: NetworkPerSample,
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
                workload: context.workload,
                publication_filter: context.publication_filter,
                storage_backend: context.storage_backend.clone(),
                metadata_transport: context.metadata_transport.clone(),
                network_source: context.network_source.clone(),
                range_cache_policy: context.range_cache_policy.clone(),
                expect_range_reuse: context.expect_range_reuse,
                incremental_expectation: context.incremental_expectation.clone(),
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
        let latency_samples_ms = latencies.clone();
        let network_per_sample = NetworkPerSample::new(&network, latencies.len());
        let useful_bytes = dataset.useful_bytes();
        let record = CostRecord {
            schema: RESULT_SCHEMA,
            scenario: scenario.to_owned(),
            workload: context.workload.as_str().to_owned(),
            volume_mode: volume_mode(scenario),
            profile: match context.profile {
                Profile::Early => "early",
                Profile::Full => "full",
            }
            .to_owned(),
            publication_filter: context.publication_filter.as_str().to_owned(),
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
            storage_backend: context.storage_backend.clone(),
            metadata_transport: context.metadata_transport.clone(),
            network_source: context.network_source.clone(),
            cache_policy: cache_policy(context, scenario),
            incremental_expectation: context.incremental_expectation.clone(),
            dataset,
            useful_bytes,
            completion: completion.to_owned(),
            iterations: latencies.len(),
            wall_ms: latencies.iter().sum(),
            latency,
            latency_samples_ms,
            local_process_io: io,
            local_process_io_note: PROCESS_IO_NOTE.to_owned(),
            network,
            network_per_sample,
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
            "linux-cost: {} publication={} scale={} samples={} useful_bytes={} p50={:.3}ms s3_requests={} s3_request_bytes={} s3_response_bytes={} get_response_body_bytes={} put_request_body_bytes={} service_connections={} service_request_bytes={} service_response_bytes={}",
            record.scenario,
            record.publication,
            record.scale,
            record.iterations,
            record.useful_bytes,
            record.latency.p50_ms,
            record.network.requests,
            record.network.request_bytes,
            record.network.response_bytes,
            record.network.get_response_body_bytes,
            record.network.put_request_body_bytes,
            record.network.service_connections,
            record.network.service_request_bytes,
            record.network.service_response_bytes,
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
            workload: context.workload.as_str().to_owned(),
            volume_mode: volume_mode(scenario),
            profile: match context.profile {
                Profile::Early => "early",
                Profile::Full => "full",
            }
            .to_owned(),
            publication_filter: context.publication_filter.as_str().to_owned(),
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
            storage_backend: context.storage_backend.clone(),
            metadata_transport: context.metadata_transport.clone(),
            network_source: context.network_source.clone(),
            cache_policy: cache_policy(context, scenario),
            incremental_expectation: context.incremental_expectation.clone(),
            dataset: Dataset {
                kind: "not_run".to_owned(),
                files: 0,
                directories: 0,
                logical_bytes: 0,
                file_size_bytes: None,
                range_bytes: None,
                writers: None,
                read_pattern: None,
                working_set_bytes: None,
                random_seed: None,
            },
            useful_bytes: 0,
            completion: "not applicable".to_owned(),
            iterations: 0,
            wall_ms: 0.0,
            latency: LatencyStats {
                p50_ms: 0.0,
                p95_ms: 0.0,
                min_ms: 0.0,
                max_ms: 0.0,
            },
            latency_samples_ms: Vec::new(),
            local_process_io: IoSnapshot::default(),
            local_process_io_note: PROCESS_IO_NOTE.to_owned(),
            network: NetworkSnapshot::default(),
            network_per_sample: NetworkPerSample::new(&NetworkSnapshot::default(), 1),
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
        let measured = self
            .records
            .iter()
            .filter(|record| record.supported)
            .count();
        let unsupported = self
            .records
            .iter()
            .filter(|record| !record.supported)
            .count();
        let mut markdown = String::new();
        markdown.push_str("# Linux cost baseline\n\n");
        markdown.push_str(&format!(
            "- Source: `{}` (dirty: `{}`)\n- Binary: `{}`\n- Binary BLAKE3: `{}`\n- Host: {}\n- Rust: {}\n- Workload: `{}`\n- Incremental expectation: `{}`\n- Profile: {}\n- Publication filter: `{}`\n- Records: {} measured, {} unsupported\n\n",
            self.context.source_sha,
            self.context.source_dirty,
            self.context.binary_path,
            self.context.binary_blake3,
            self.context.host,
            self.context.rustc,
            self.context.workload.as_str(),
            self.context.incremental_expectation,
            match self.context.profile {
                Profile::Early => "early",
                Profile::Full => "full",
            },
            self.context.publication_filter.as_str(),
            measured,
            unsupported,
        ));
        markdown.push_str(
            "| Scenario | Workload | Volume | Publication | Scale | Status | Samples | Useful bytes | p50 ms | p95 ms | S3 requests total/sample | S3 request MiB total/sample | S3 response MiB total/sample | GET response body MiB total/sample | PUT request body MiB total/sample | Service conns total/sample | Service request KiB total/sample | Service response KiB total/sample | Remote MiB | Staging MiB |\n",
        );
        markdown.push_str(
            "| --- | --- | --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n",
        );
        for record in &self.records {
            if !record.supported {
                markdown.push_str(&format!(
                    "| {} | {} | {} | {} | {} | unsupported: {} | - | - | - | - | - | - | - | - | - | - | - | - | - | - |\n",
                    record.scenario,
                    record.workload,
                    record.volume_mode,
                    record.publication,
                    record.scale,
                    record
                        .limitation
                        .as_deref()
                        .expect("unsupported record has a limitation"),
                ));
                continue;
            }
            let network = &record.network_per_sample;
            markdown.push_str(&format!(
                "| {} | {} | {} | {} | {} | measured | {} | {} | {:.3} | {:.3} | {} / {:.1} | {:.3} / {:.3} | {:.3} / {:.3} | {:.3} / {:.3} | {:.3} / {:.3} | {} / {:.1} | {:.3} / {:.3} | {:.3} / {:.3} | {:.3} | {:.3} |\n",
                record.scenario,
                record.workload,
                record.volume_mode,
                record.publication,
                record.scale,
                record.iterations,
                record.useful_bytes,
                record.latency.p50_ms,
                record.latency.p95_ms,
                record.network.requests,
                network.s3_requests,
                record.network.request_bytes as f64 / 1024.0 / 1024.0,
                network.s3_request_bytes / 1024.0 / 1024.0,
                record.network.response_bytes as f64 / 1024.0 / 1024.0,
                network.s3_response_bytes / 1024.0 / 1024.0,
                record.network.get_response_body_bytes as f64 / 1024.0 / 1024.0,
                network.get_response_body_bytes / 1024.0 / 1024.0,
                record.network.put_request_body_bytes as f64 / 1024.0 / 1024.0,
                network.put_request_body_bytes / 1024.0 / 1024.0,
                record.network.service_connections,
                network.service_connections,
                record.network.service_request_bytes as f64 / 1024.0,
                network.service_request_bytes / 1024.0,
                record.network.service_response_bytes as f64 / 1024.0,
                network.service_response_bytes / 1024.0,
                record.space.remote_stored_bytes as f64 / 1024.0 / 1024.0,
                record.space.mount_staging_bytes as f64 / 1024.0 / 1024.0,
            ));
        }
        markdown.push_str(
            "\nRows marked unsupported were not executed; dashes distinguish them from measured zero-cost operations. The `total/sample` columns divide batch counters by the actual number of latency samples so a five-run batch is not presented as one run. For concurrent writers, samples overlap and share the same batch totals. S3 and metadata-service counters are separate: S3 columns come from parsed HTTP requests to Silo, while service columns count authenticated TCP connections and raw framed bytes. Process I/O values are sampled lower bounds, not exact local scan or hash costs. Space series, raw latency samples, cache policy, and completion semantics are preserved in the JSONL evidence.\n",
        );
        fs::write(&self.summary_path, markdown).expect("write benchmark Markdown summary");
        println!(
            "linux-cost: workload={} filter={} wrote {} records ({} measured, {} unsupported) to {} and {}",
            self.context.workload.as_str(),
            self.context.publication_filter.as_str(),
            self.records.len(),
            measured,
            unsupported,
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
    use super::{HttpMethod, ProxyCounters, RequestScanner, ResponseScanner};
    use std::collections::VecDeque;
    use std::sync::atomic::Ordering;
    use std::sync::{Arc, Mutex};

    #[test]
    fn request_scanner_skips_content_length_and_chunked_bodies() {
        let counters = ProxyCounters::default();
        let methods = Arc::new(Mutex::new(VecDeque::new()));
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
            scanner.scan(&[byte], &counters, &methods);
        }
        assert_eq!(counters.requests.load(Ordering::Relaxed), 3);
        assert_eq!(counters.put.load(Ordering::Relaxed), 1);
        assert_eq!(counters.post.load(Ordering::Relaxed), 1);
        assert_eq!(counters.head.load(Ordering::Relaxed), 1);
        assert_eq!(counters.get.load(Ordering::Relaxed), 0);
        assert_eq!(
            counters.put_request_body_bytes.load(Ordering::Relaxed),
            body.len() as u64
        );
    }

    #[test]
    fn response_scanner_counts_get_body_bytes() {
        let counters = ProxyCounters::default();
        let methods = Arc::new(Mutex::new(VecDeque::from([
            HttpMethod::Get,
            HttpMethod::Put,
            HttpMethod::Head,
        ])));
        let mut scanner = ResponseScanner::new(methods);
        let wire = b"HTTP/1.1 206 Partial Content\r\nContent-Length: 4\r\n\r\nbody\
              HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\nput\
              HTTP/1.1 200 OK\r\nContent-Length: 999\r\n\r\n";
        for byte in wire {
            scanner.scan(&[*byte], &counters);
        }
        assert_eq!(counters.get_response_body_bytes.load(Ordering::Relaxed), 4);
        assert_eq!(counters.put.load(Ordering::Relaxed), 0);
    }
}
