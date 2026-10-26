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

//! Harness for real Linux FUSE acceptance against the yy CLI.

use nix::sys::signal::{Signal, kill};
use nix::unistd::Pid;
use std::fs;
use std::io::{BufRead, BufReader, Read};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::RecvTimeoutError;
use std::sync::{Arc, Condvar, Mutex, mpsc};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};
use yinyang::core::service::ServiceClient;
use yinyang::core::{Authority, BackendProfile, CommitId, CommitOutcome, Fs, Planner};
use yinyang::runtime::Runtime as YinYangRuntime;

/// The kernel filesystem type advertised by the YinYang FUSE adapter.
pub const MOUNT_TYPE: &str = "fuse.yinyang";

pub fn require_fuse() {
    if !Path::new("/dev/fuse").exists() {
        panic!(
            "/dev/fuse is not available; this is a hard Linux FUSE acceptance failure, not a skip"
        );
    }
    for tool in ["fusermount3", "timeout", "python3"] {
        if Command::new(tool).arg("--version").output().is_err() {
            panic!("required tool {tool} is not available for Linux FUSE acceptance");
        }
    }
}

#[derive(Debug)]
pub struct Captured {
    pub status: ExitStatus,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
}

impl Captured {
    pub fn text(&self) -> String {
        String::from_utf8_lossy(&self.stdout).trim().to_owned()
    }

    pub fn success_or(&self, context: &str) {
        assert!(
            self.status.success(),
            "{context} failed with {}:\nstdout:\n{}\nstderr:\n{}",
            self.status,
            String::from_utf8_lossy(&self.stdout),
            String::from_utf8_lossy(&self.stderr)
        );
    }
}

pub fn run_timeout(mut cmd: Command, timeout: Duration) -> Captured {
    cmd.stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = cmd
        .spawn()
        .unwrap_or_else(|error| panic!("failed to spawn command (format: {cmd:?}): {error}"));
    let stdout = child.stdout.take().expect("stdout is piped");
    let stderr = child.stderr.take().expect("stderr is piped");
    let stdout_thread = thread::spawn(move || read_all(stdout));
    let stderr_thread = thread::spawn(move || read_all(stderr));
    let deadline = Instant::now() + timeout;
    let status = loop {
        match child.try_wait() {
            Ok(Some(status)) => break status,
            Ok(None) => {
                if Instant::now() >= deadline {
                    let _ = child.kill();
                    let status = child.wait().expect("wait after kill");
                    let stdout = stdout_thread.join().unwrap_or_default();
                    let stderr = stderr_thread.join().unwrap_or_default();
                    panic!(
                        "command timed out after {timeout:?} with {status} (format: {cmd:?}):\nstdout:\n{}\nstderr:\n{}",
                        String::from_utf8_lossy(&stdout),
                        String::from_utf8_lossy(&stderr)
                    );
                }
                thread::sleep(Duration::from_millis(25));
            }
            Err(error) => {
                let _ = child.kill();
                let _ = child.wait();
                panic!("failed to wait for command (format: {cmd:?}): {error}");
            }
        }
    };
    let stdout = stdout_thread.join().unwrap_or_default();
    let stderr = stderr_thread.join().unwrap_or_default();
    Captured {
        status,
        stdout,
        stderr,
    }
}

pub fn run_external(program: &str, args: &[&str], timeout: Duration) -> Captured {
    let mut cmd = Command::new(program);
    cmd.args(args);
    run_timeout(cmd, timeout)
}

fn read_all(mut reader: impl Read) -> Vec<u8> {
    let mut bytes = Vec::new();
    let _ = reader.read_to_end(&mut bytes);
    bytes
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Publication {
    Object,
    Service,
}

impl Publication {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Object => "object",
            Self::Service => "service",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Frontend {
    Library,
    Fuse,
}

impl Frontend {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Library => "library",
            Self::Fuse => "fuse",
        }
    }
}

pub struct Fixture {
    pub temp: tempfile::TempDir,
    root: String,
    token: String,
    service: Option<ServiceGuard>,
    last_service_address: Option<SocketAddr>,
}

impl Fixture {
    pub fn new(publication: Publication) -> Self {
        require_fuse();
        let temp = tempfile::tempdir().expect("create temporary test directory");
        let root = format!(
            "fuse-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let token = "0123456789abcdef0123456789abcdef".to_owned();
        let mut fixture = Fixture {
            temp,
            root,
            token,
            service: None,
            last_service_address: None,
        };
        match publication {
            Publication::Object => {
                fixture.yy_ok(&["create"]);
            }
            Publication::Service => fixture.spawn_service(),
        }
        fixture
    }

    pub fn publication(&self) -> Publication {
        if self.service.is_some() {
            Publication::Service
        } else {
            Publication::Object
        }
    }

    fn storage_env(&self) -> Vec<(&'static str, String)> {
        let required = [
            "YINYANG_S3_BUCKET",
            "YINYANG_S3_ENDPOINT",
            "YINYANG_S3_REGION",
            "YINYANG_S3_ACCESS_KEY_ID",
            "YINYANG_S3_SECRET_ACCESS_KEY",
        ];
        let mut env = Vec::new();
        for name in required {
            let value = std::env::var(name).unwrap_or_else(|_| {
                panic!("set {name} before running Linux FUSE acceptance; missing configuration is a hard failure")
            });
            env.push((name, value));
        }
        env.push(("YINYANG_STORAGE_PROFILE", "minio".to_owned()));
        env.push(("YINYANG_S3_ROOT", self.root.clone()));
        env.push(("YINYANG_SERVICE_TOKEN", self.token.clone()));
        env
    }

    pub fn yy_command(&self, args: &[&str]) -> Command {
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_yy"));
        cmd.args(args);
        for (name, value) in self.storage_env() {
            cmd.env(name, value);
        }
        cmd
    }

    pub fn yy(&self, args: &[&str]) -> Captured {
        run_timeout(self.yy_command(args), Duration::from_secs(120))
    }

    pub fn yy_ok(&self, args: &[&str]) -> Captured {
        let output = self.yy(args);
        output.success_or(&format!("yy {args:?}"));
        output
    }

    fn spawn_service(&mut self) {
        self.spawn_service_on("127.0.0.1:0".parse().expect("loopback wildcard"));
    }

    fn spawn_service_on(&mut self, listen: SocketAddr) {
        let database = self.temp.path().join("metadata.db");
        let database = database.to_str().expect("UTF-8 database path");
        let listen = listen.to_string();
        let mut cmd = self.yy_command(&["serve", "--database", database, "--listen", &listen]);
        cmd.stdout(Stdio::piped()).stderr(Stdio::piped());
        let mut child = cmd
            .spawn()
            .unwrap_or_else(|error| panic!("failed to spawn yy serve: {error}"));
        let stdout = child.stdout.take().expect("stdout is piped");
        let stderr = child.stderr.take().expect("stderr is piped");
        let (sender, receiver) = mpsc::channel();
        let reader = thread::spawn(move || {
            let mut line = String::new();
            if BufReader::new(stdout).read_line(&mut line).is_ok() {
                let _ = sender.send(line);
            }
        });
        let stderr_thread = thread::spawn(move || read_all(stderr));
        let line = match receiver.recv_timeout(Duration::from_secs(60)) {
            Ok(line) => line,
            Err(error @ (RecvTimeoutError::Timeout | RecvTimeoutError::Disconnected)) => {
                let _ = child.kill();
                let _ = child.wait();
                let _ = reader.join();
                let stderr = stderr_thread.join().unwrap_or_default();
                panic!(
                    "yy serve did not report a listening address ({error:?}):\n{}",
                    String::from_utf8_lossy(&stderr)
                );
            }
        };
        let address: SocketAddr = line
            .trim()
            .strip_prefix("metadata service listening on ")
            .unwrap_or_else(|| {
                let _ = child.kill();
                let _ = child.wait();
                panic!("unexpected yy serve startup line: {line:?}");
            })
            .parse()
            .unwrap_or_else(|error| {
                let _ = child.kill();
                let _ = child.wait();
                panic!("invalid yy serve listening address {line:?}: {error}");
            });
        let _ = reader.join();
        self.last_service_address = Some(address);
        self.service = Some(ServiceGuard {
            child,
            address,
            stderr: Some(stderr_thread),
        });
    }

    pub fn stop_service(&mut self) {
        if let Some(mut service) = self.service.take() {
            self.last_service_address = Some(service.address);
            service.stop();
        }
    }

    pub fn restart_service(&mut self) {
        let address = self.service_address();
        self.stop_service();
        self.spawn_service_on(address);
    }

    pub fn service_address(&self) -> SocketAddr {
        self.service
            .as_ref()
            .map(|service| service.address)
            .or(self.last_service_address)
            .expect("service publication requires a running metadata service")
    }

    pub fn pause_service(&self) {
        let pid = self
            .service
            .as_ref()
            .expect("service publication requires a running metadata service")
            .child
            .id();
        kill(Pid::from_raw(pid as i32), Signal::SIGSTOP)
            .expect("pause metadata service without exec");
    }

    pub fn resume_service(&self) {
        let pid = self
            .service
            .as_ref()
            .expect("service publication requires a running metadata service")
            .child
            .id();
        kill(Pid::from_raw(pid as i32), Signal::SIGCONT)
            .expect("resume metadata service without exec");
    }

    /// Polls the existing no-lease status entry until a handle reports the
    /// frozen publication boundary. A hang is a hard failure, not a skip.
    pub fn wait_for_frozen(&self, config: &Path) -> String {
        let deadline = Instant::now() + Duration::from_secs(30);
        let staging = self
            .temp
            .path()
            .join(config.file_stem().expect("volume config has a file stem"));
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build status runtime");
        loop {
            let status = runtime
                .block_on(YinYangRuntime::inspect(&staging))
                .expect("inspect staging without the runtime lease");
            let text = format!("{status:#?}");
            if status.handles.iter().any(|handle| handle.frozen) {
                return text;
            }
            if Instant::now() >= deadline {
                panic!(
                    "fsync never reached the frozen publication boundary within 30s; last yy status:\n{text}"
                );
            }
            thread::sleep(Duration::from_millis(100));
        }
    }

    /// Writes a strict volume config and returns its path. Rewriting the same
    /// path is intentional: a restarted service keeps its original loopback
    /// address, so an unchanged config keeps working.
    pub fn volume_config(&self, name: &str, frontend: Frontend, read_only: bool) -> PathBuf {
        self.volume_config_with_service(name, frontend, read_only, None)
    }

    pub fn volume_config_with_service(
        &self,
        name: &str,
        frontend: Frontend,
        read_only: bool,
        service_address: Option<SocketAddr>,
    ) -> PathBuf {
        let staging = self.temp.path().join(name);
        let mut json = format!(
            "{{\n  \"name\": {},\n  \"model\": \"managed\",\n  \"access\": \"mount\",\n  \"frontend\": \"{}\",\n  \"publication\": \"{}\",\n  \"storage_profile\": \"minio\",\n  \"staging\": {},\n  \"read_only\": {},\n  \"require\": []",
            json_string(name),
            frontend.as_str(),
            self.publication().as_str(),
            json_string(staging.to_str().expect("UTF-8 staging path")),
            read_only
        );
        if self.publication() == Publication::Service {
            json.push_str(&format!(
                ",\n  \"service_address\": \"{}\"",
                service_address.unwrap_or_else(|| self.service_address())
            ));
        }
        json.push_str("\n}");
        let path = self.temp.path().join(format!("{name}.json"));
        fs::write(&path, json).expect("write volume config");
        path
    }

    /// Seed a directory tree in one core transaction. The 520 entries in
    /// `bulk` cross the adapter's 512-entry readdir page without launching one
    /// CLI process per entry.
    pub fn seed_read_only_tree(&self) {
        let operator = self.operator();
        let publication = self.publication();
        let address = self.service.as_ref().map(|service| service.address);
        let token = self.token.clone();
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("build seed runtime");
        runtime.block_on(async move {
            let authority: Arc<dyn Authority> = match publication {
                Publication::Object => Arc::new(
                    Fs::open(operator, BackendProfile::Minio)
                        .await
                        .expect("open object authority for batch seed"),
                ),
                Publication::Service => Arc::new(
                    ServiceClient::connect(address.expect("service address"), token, operator)
                        .await
                        .expect("connect service authority for batch seed"),
                ),
            };
            let snapshot = authority.observe_latest().await.expect("observe seed base");
            let mut empty_source: &[u8] = b"";
            let empty = authority
                .prepare(&mut empty_source)
                .await
                .expect("prepare empty file content");
            let mut hello_source: &[u8] = b"hello from nested";
            let hello = authority
                .prepare(&mut hello_source)
                .await
                .expect("prepare nested file content");
            let mut root_source: &[u8] = b"read-only bytes";
            let root_file = authority
                .prepare(&mut root_source)
                .await
                .expect("prepare root file content");
            let mut plan = Planner::new(&snapshot, CommitId::generate());
            let nested = plan
                .create_directory(authority.root(), "nested")
                .await
                .expect("plan nested directory");
            let inner = plan
                .create_directory(nested, "inner")
                .await
                .expect("plan inner directory");
            plan.create_file(inner, "hello.txt", hello, false)
                .await
                .expect("plan nested file");
            plan.create_file(authority.root(), "readonly.txt", root_file, false)
                .await
                .expect("plan root file");
            let bulk = plan
                .create_directory(authority.root(), "bulk")
                .await
                .expect("plan bulk directory");
            for index in 0..520 {
                plan.create_file(bulk, &format!("entry-{index:04}"), empty.clone(), false)
                    .await
                    .expect("plan bulk entry");
            }
            let request = plan.finish().expect("finish batch seed");
            let outcome = authority.commit(&request).await.expect("commit batch seed");
            assert!(
                matches!(&outcome, CommitOutcome::Committed(_)),
                "batch seed did not commit: {outcome:?}"
            );
        });
    }

    fn operator(&self) -> opendal::Operator {
        opendal::install_default();
        let config = self
            .storage_env()
            .into_iter()
            .filter_map(|(key, value)| {
                key.strip_prefix("YINYANG_S3_")
                    .map(|key| (key.to_ascii_lowercase(), value))
            })
            .collect::<Vec<_>>();
        opendal::Operator::via_iter("s3", config).expect("construct S3 operator")
    }

    pub fn library_seed(&self, config: &Path, path: &str, bytes: &[u8]) {
        self.yy_ok(&[
            "--volume",
            config.to_str().expect("UTF-8 volume config"),
            "file",
            "create",
            path,
        ]);
        self.remote_write(config, path, bytes);
    }

    pub fn remote_write(&self, _config: &Path, path: &str, bytes: &[u8]) {
        let authority = self.in_process_authority();
        let staging = self.in_process_staging("write");
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build in-process write runtime");
        runtime.block_on(async move {
            let runtime = YinYangRuntime::open(authority, staging)
                .await
                .expect("open in-process write runtime");
            let mut file = runtime
                .open_file(path, true)
                .await
                .expect("open in-process write handle");
            file.write(0, bytes)
                .await
                .expect("write through in-process authority");
            file.fsync()
                .await
                .expect("fsync in-process authority write");
            file.close().await.expect("close in-process write handle");
        });
    }

    pub fn remote_truncate(&self, _config: &Path, path: &str, length: u64) {
        let authority = self.in_process_authority();
        let staging = self.in_process_staging("truncate");
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build in-process truncate runtime");
        runtime.block_on(async move {
            let runtime = YinYangRuntime::open(authority, staging)
                .await
                .expect("open in-process truncate runtime");
            let mut file = runtime
                .open_file(path, true)
                .await
                .expect("open in-process truncate handle");
            file.truncate(length)
                .await
                .expect("truncate through in-process authority");
            file.fsync()
                .await
                .expect("fsync in-process authority truncate");
            file.close()
                .await
                .expect("close in-process truncate handle");
        });
    }

    pub fn remote_create(&self, _config: &Path, path: &str) {
        let authority = self.in_process_authority();
        let staging = self.in_process_staging("create");
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build in-process create runtime");
        runtime.block_on(async move {
            let runtime = YinYangRuntime::open(authority, staging)
                .await
                .expect("open in-process create runtime");
            let (parent, name) = path.rsplit_once('/').unwrap_or(("", path));
            let snapshot = runtime
                .authority()
                .observe_latest()
                .await
                .expect("observe in-process create parent");
            let parent = snapshot
                .resolve(parent)
                .await
                .expect("resolve in-process create parent")
                .expect("in-process create parent exists");
            runtime
                .create_file(parent.id(), name)
                .await
                .expect("create through in-process authority");
        });
    }

    pub fn remote_rename(&self, _config: &Path, source: &str, destination: &str) {
        let authority = self.in_process_authority();
        let staging = self.in_process_staging("rename");
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build in-process rename runtime");
        runtime.block_on(async move {
            let runtime = YinYangRuntime::open(authority, staging)
                .await
                .expect("open in-process rename runtime");
            let snapshot = runtime
                .authority()
                .observe_latest()
                .await
                .expect("observe in-process rename source");
            let source = snapshot
                .resolve(source)
                .await
                .expect("resolve in-process rename source")
                .expect("in-process rename source exists");
            let (parent, name) = destination.rsplit_once('/').unwrap_or(("", destination));
            let parent = snapshot
                .resolve(parent)
                .await
                .expect("resolve in-process rename parent")
                .expect("in-process rename parent exists");
            runtime
                .rename(source.id(), parent.id(), name)
                .await
                .expect("rename through in-process authority");
        });
    }

    pub fn remote_read(&self, _config: &Path, path: &str, length: usize) -> Vec<u8> {
        let authority = self.in_process_authority();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build in-process read runtime");
        runtime.block_on(async move {
            let snapshot = authority
                .observe_latest()
                .await
                .expect("observe in-process read");
            let node = snapshot
                .resolve(path)
                .await
                .expect("resolve in-process read")
                .expect("in-process read path exists");
            snapshot
                .open_file(node.id())
                .await
                .expect("open in-process read version")
                .read(0, length)
                .await
                .expect("read through in-process authority")
        })
    }

    fn in_process_authority(&self) -> Arc<dyn Authority> {
        let operator = self.operator();
        let publication = self.publication();
        let address = self.last_service_address;
        let token = self.token.clone();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build in-process authority runtime");
        runtime.block_on(async move {
            match publication {
                Publication::Object => Arc::new(
                    Fs::open(operator, BackendProfile::Minio)
                        .await
                        .expect("open in-process object authority"),
                ),
                Publication::Service => Arc::new(
                    ServiceClient::connect(
                        address.expect("in-process service address"),
                        token,
                        operator,
                    )
                    .await
                    .expect("connect in-process service authority"),
                ),
            }
        })
    }

    fn in_process_staging(&self, operation: &str) -> PathBuf {
        self.temp.path().join(format!(
            "inproc-{operation}-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system time")
                .as_nanos()
        ))
    }

    pub fn mount(&self, config: &Path, mountpoint: &Path) -> Mounted {
        let staging = self
            .temp
            .path()
            .join(config.file_stem().expect("volume config has a file stem"));
        let mountpoint = mountpoint.to_str().expect("UTF-8 mountpoint");
        let config = config.to_str().expect("UTF-8 config path");
        let mut cmd = self.yy_command(&["--volume", config, "mount", mountpoint]);
        cmd.stdout(Stdio::piped()).stderr(Stdio::piped());
        let mut child = cmd.spawn().unwrap_or_else(|error| {
            panic!("failed to spawn `yy --volume {config} mount {mountpoint}`: {error}")
        });
        let stdout = child.stdout.take().expect("stdout is piped");
        let stderr = child.stderr.take().expect("stderr is piped");
        let stdout_log = Arc::new(Mutex::new(Vec::new()));
        let stderr_log = Arc::new(Mutex::new(Vec::new()));
        let log = stdout_log.clone();
        let stdout_thread = thread::spawn(move || read_all_logged(stdout, log));
        let log = stderr_log.clone();
        let stderr_thread = thread::spawn(move || read_all_logged(stderr, log));
        let mut mounted = Mounted {
            child,
            stdout: Some(stdout_thread),
            stderr: Some(stderr_thread),
            mountpoint: PathBuf::from(mountpoint),
            staging,
            stdout_log,
            stderr_log,
        };
        mounted.wait_mounted();
        mounted
    }
}

pub struct ServiceGuard {
    child: Child,
    address: SocketAddr,
    stderr: Option<JoinHandle<Vec<u8>>>,
}

impl ServiceGuard {
    fn stop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        if let Some(thread) = self.stderr.take() {
            let _ = thread.join();
        }
    }
}

impl Drop for ServiceGuard {
    fn drop(&mut self) {
        self.stop();
    }
}

pub struct Mounted {
    child: Child,
    stdout: Option<JoinHandle<Vec<u8>>>,
    stderr: Option<JoinHandle<Vec<u8>>>,
    mountpoint: PathBuf,
    staging: PathBuf,
    stdout_log: Arc<Mutex<Vec<u8>>>,
    stderr_log: Arc<Mutex<Vec<u8>>>,
}

impl Mounted {
    pub fn mountpoint(&self) -> &Path {
        &self.mountpoint
    }

    fn wait_mounted(&mut self) {
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            if let Some(kind) = mount_fs_type(&self.mountpoint) {
                assert_eq!(
                    kind,
                    MOUNT_TYPE,
                    "{} is mounted as {kind}, not the YinYang FUSE type {MOUNT_TYPE}; a foreign or sample mount is not acceptance",
                    self.mountpoint.display()
                );
                return;
            }
            if let Some(status) = self
                .child
                .try_wait()
                .unwrap_or_else(|error| panic!("failed to wait for yy mount: {error}"))
            {
                let stderr = self.join_stderr();
                panic!(
                    "yy mount exited before mounting {} with {status}:\n{}",
                    self.mountpoint.display(),
                    String::from_utf8_lossy(&stderr)
                );
            }
            if Instant::now() >= deadline {
                let _ = self.child.kill();
                let _ = self.child.wait();
                let stderr = self.join_stderr();
                panic!(
                    "yy mount did not install {} as {MOUNT_TYPE} within 60s:\n{}",
                    self.mountpoint.display(),
                    String::from_utf8_lossy(&stderr)
                );
            }
            thread::sleep(Duration::from_millis(50));
        }
    }

    pub fn kill(&mut self) {
        let _ = self.child.kill();
        let _ = self.wait_exit(Duration::from_secs(30));
        self.unmount_force();
        self.assert_staging_lease_released();
    }

    /// Stop the daemon without spawning a cleanup helper while a dirty FUSE
    /// descriptor is still owned by the test process.
    pub fn kill_daemon_only(&mut self) {
        let _ = self.child.kill();
        let _ = self.wait_exit(Duration::from_secs(30));
    }

    pub fn cleanup_after_kill(&mut self) {
        self.unmount_force();
        self.assert_staging_lease_released();
    }

    pub fn terminate(&mut self) {
        let pid = Pid::from_raw(self.child.id() as i32);
        kill(pid, Signal::SIGTERM).expect("send SIGTERM without exec");
        let status = self.wait_exit(Duration::from_secs(30));
        assert!(
            status.success(),
            "yy mount did not exit cleanly after SIGTERM: {status}"
        );
        assert!(
            mount_fs_type(&self.mountpoint).is_none(),
            "yy did not remove {} after a graceful SIGTERM",
            self.mountpoint.display()
        );
        self.assert_staging_lease_released();
    }

    /// SIGTERM with open descriptors or a pending fsync must exit under the
    /// forced non-zero grace path and release the staging lease. No success
    /// assertion is made here; the caller requires the busy exit contract.
    pub fn terminate_busy(&mut self) -> ExitStatus {
        let pid = Pid::from_raw(self.child.id() as i32);
        kill(pid, Signal::SIGTERM).expect("send SIGTERM without exec");
        self.wait_exit(Duration::from_secs(10))
    }

    /// Ask the live daemon for the explicit SIGHUP refresh path.
    pub fn refresh_sighup(&mut self) {
        let pid = Pid::from_raw(self.child.id() as i32);
        kill(pid, Signal::SIGHUP).expect("send SIGHUP without exec");
    }

    pub fn wait_for_output(&self, needle: &str, timeout: Duration) -> String {
        let deadline = Instant::now() + timeout;
        loop {
            let output = self.output_text();
            if output.contains(needle) {
                return output;
            }
            if Instant::now() >= deadline {
                panic!(
                    "yy mount did not emit expected output {needle:?} within {timeout:?}:\n{output}"
                );
            }
            thread::sleep(Duration::from_millis(50));
        }
    }

    pub fn output_text(&self) -> String {
        let stdout = self
            .stdout_log
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let stderr = self
            .stderr_log
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        format!(
            "stdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&stdout),
            String::from_utf8_lossy(&stderr)
        )
    }

    pub fn unmount(&mut self) {
        let mountpoint = self.mountpoint.to_str().expect("UTF-8 mountpoint");
        let output = run_external("fusermount3", &["-u", mountpoint], Duration::from_secs(30));
        output.success_or(&format!("fusermount3 -u {mountpoint}"));
        let status = self.wait_exit(Duration::from_secs(30));
        assert!(
            status.success(),
            "yy mount did not exit cleanly after external unmount: {status}"
        );
        assert!(
            mount_fs_type(&self.mountpoint).is_none(),
            "external unmount left {} in /proc/mounts",
            self.mountpoint.display()
        );
        self.assert_staging_lease_released();
    }

    fn unmount_force(&mut self) {
        if mount_fs_type(&self.mountpoint).is_none() {
            return;
        }
        let mountpoint = self.mountpoint.to_str().expect("UTF-8 mountpoint");
        let _ = run_external(
            "fusermount3",
            &["-u", "-z", mountpoint],
            Duration::from_secs(30),
        );
    }

    fn wait_exit(&mut self, timeout: Duration) -> ExitStatus {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(status) = self.child.try_wait().expect("wait for yy mount") {
                let _ = self.stdout.take().map(JoinHandle::join);
                let _ = self.stderr.take().map(JoinHandle::join);
                return status;
            }
            if Instant::now() >= deadline {
                let _ = self.child.kill();
                let _ = self.child.wait();
                let stderr = self.join_stderr();
                panic!(
                    "yy mount did not exit after unmount {}:\n{}",
                    self.mountpoint.display(),
                    String::from_utf8_lossy(&stderr)
                );
            }
            thread::sleep(Duration::from_millis(25));
        }
    }

    fn join_stderr(&mut self) -> Vec<u8> {
        self.stderr
            .take()
            .and_then(|thread| thread.join().ok())
            .unwrap_or_default()
    }

    fn assert_staging_lease_released(&self) {
        let path = self.staging.join("runtime.lock");
        let lock = fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&path)
            .unwrap_or_else(|error| panic!("open staging lock {}: {error}", path.display()));
        lock.try_lock().unwrap_or_else(|error| {
            panic!(
                "yy mount still holds the staging lease {} after process exit: {error}",
                path.display()
            )
        });
    }
}

fn read_all_logged(mut reader: impl Read, log: Arc<Mutex<Vec<u8>>>) -> Vec<u8> {
    let mut bytes = Vec::new();
    let _ = reader.read_to_end(&mut bytes);
    if let Ok(mut output) = log.lock() {
        output.extend_from_slice(&bytes);
    }
    bytes
}

/// A loopback TCP gate used only by the mounting volume. It watches the
/// no-lease staging status and blocks the first connection observed after a
/// frozen publication is persisted. Service clients used by the test itself
/// connect directly to the real service.
pub struct ServiceGate {
    address: SocketAddr,
    release: Arc<(Mutex<bool>, Condvar)>,
    ready: mpsc::Receiver<()>,
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

impl ServiceGate {
    pub fn new(staging: PathBuf, upstream: SocketAddr) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind service gate");
        let address = listener.local_addr().expect("service gate address");
        let release = Arc::new((Mutex::new(false), Condvar::new()));
        let stop = Arc::new(AtomicBool::new(false));
        let ready_sent = Arc::new(AtomicBool::new(false));
        let (sender, ready) = mpsc::channel();
        let thread_release = release.clone();
        let thread_stop = stop.clone();
        let thread = thread::spawn(move || {
            while !thread_stop.load(Ordering::SeqCst) {
                let Ok((client, _)) = listener.accept() else {
                    break;
                };
                if thread_stop.load(Ordering::SeqCst) {
                    break;
                }
                let staging = staging.clone();
                let release = thread_release.clone();
                let ready_sent = ready_sent.clone();
                let sender = sender.clone();
                thread::spawn(move || {
                    handle_gate_connection(client, upstream, staging, release, ready_sent, sender);
                });
            }
        });
        Self {
            address,
            release,
            ready,
            stop,
            thread: Some(thread),
        }
    }

    pub fn address(&self) -> SocketAddr {
        self.address
    }

    pub fn wait_ready(&self, timeout: Duration) {
        self.ready
            .recv_timeout(timeout)
            .expect("service gate did not observe a frozen commit connection");
    }

    pub fn release(&self) {
        let (released, condvar) = &*self.release;
        *released
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = true;
        condvar.notify_all();
    }
}

impl Drop for ServiceGate {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        self.release();
        let _ = TcpStream::connect(self.address);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

fn handle_gate_connection(
    client: TcpStream,
    upstream: SocketAddr,
    staging: PathBuf,
    release: Arc<(Mutex<bool>, Condvar)>,
    ready_sent: Arc<AtomicBool>,
    sender: mpsc::Sender<()>,
) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build service gate runtime");
    let frozen = runtime
        .block_on(YinYangRuntime::inspect(&staging))
        .expect("inspect service gate staging")
        .handles
        .iter()
        .any(|handle| handle.frozen);
    if frozen {
        if !ready_sent.swap(true, Ordering::SeqCst) {
            let _ = sender.send(());
        }
        let (released, condvar) = &*release;
        let mut released = released
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        while !*released {
            released = condvar
                .wait(released)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
    }
    let Ok(server) = TcpStream::connect(upstream) else {
        return;
    };
    let (Ok(mut client_read), Ok(mut client_write)) = (client.try_clone(), client.try_clone())
    else {
        return;
    };
    let (Ok(mut server_read), Ok(mut server_write)) = (server.try_clone(), server.try_clone())
    else {
        return;
    };
    let client_to_server = thread::spawn(move || {
        let _ = std::io::copy(&mut client_read, &mut server_write);
        let _ = server_write.shutdown(std::net::Shutdown::Write);
    });
    let server_to_client = thread::spawn(move || {
        let _ = std::io::copy(&mut server_read, &mut client_write);
        let _ = client_write.shutdown(std::net::Shutdown::Write);
    });
    let _ = client_to_server.join();
    let _ = server_to_client.join();
}

impl Drop for Mounted {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        if let Some(thread) = self.stdout.take() {
            let _ = thread.join();
        }
        if let Some(thread) = self.stderr.take() {
            let _ = thread.join();
        }
        self.unmount_force();
    }
}

pub fn mount_fs_type(mountpoint: &Path) -> Option<String> {
    let mounts = fs::read_to_string("/proc/mounts").expect("read /proc/mounts");
    for line in mounts.lines() {
        let mut fields = line.split_whitespace();
        let _source = fields.next()?;
        let point = fields.next()?;
        let kind = fields.next()?;
        if Path::new(point) == mountpoint {
            return Some(kind.to_owned());
        }
    }
    None
}

pub fn json_string(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len() + 2);
    escaped.push('"');
    for ch in value.chars() {
        match ch {
            '"' => escaped.push_str("\\\""),
            '\\' => escaped.push_str("\\\\"),
            '\n' => escaped.push_str("\\n"),
            _ => escaped.push(ch),
        }
    }
    escaped.push('"');
    escaped
}

pub fn assert_errno(error: std::io::Error, allowed: &[i32], operation: &str) {
    let code = error.raw_os_error().unwrap_or_else(|| {
        panic!("{operation} failed without an explicit OS error: {error}");
    });
    assert!(
        allowed.contains(&code),
        "{operation} returned unexpected errno {code} ({error}); expected one of {allowed:?}"
    );
}

pub fn sync_all_with_deadline(file: &fs::File, timeout: Duration) -> std::io::Result<()> {
    let clone = file.try_clone()?;
    let (sender, receiver) = mpsc::channel();
    let worker = thread::spawn(move || {
        let _ = sender.send(clone.sync_all());
    });
    let result = receiver.recv_timeout(timeout).unwrap_or_else(|error| {
        panic!("fsync did not complete within {timeout:?}; a hang is not acceptance: {error}")
    });
    worker.join().expect("fsync worker panicked");
    result
}
