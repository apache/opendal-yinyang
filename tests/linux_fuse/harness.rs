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

use std::fs;
use std::io::{BufRead, BufReader, Read};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::mpsc::{self, RecvTimeoutError};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

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
        self.service = Some(ServiceGuard {
            child,
            address,
            stderr: Some(stderr_thread),
        });
    }

    pub fn stop_service(&mut self) {
        if let Some(mut service) = self.service.take() {
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
            .expect("service publication requires a running metadata service")
            .address
    }

    pub fn pause_service(&self) {
        let pid = self
            .service
            .as_ref()
            .expect("service publication requires a running metadata service")
            .child
            .id();
        let output = run_external(
            "kill",
            &["-STOP", &pid.to_string()],
            Duration::from_secs(30),
        );
        output.success_or("pause metadata service");
    }

    pub fn resume_service(&self) {
        let pid = self
            .service
            .as_ref()
            .expect("service publication requires a running metadata service")
            .child
            .id();
        let output = run_external(
            "kill",
            &["-CONT", &pid.to_string()],
            Duration::from_secs(30),
        );
        output.success_or("resume metadata service");
    }

    /// Polls the existing no-lease status entry until a handle reports the
    /// frozen publication boundary. A hang is a hard failure, not a skip.
    pub fn wait_for_frozen(&self, config: &Path) -> String {
        let deadline = Instant::now() + Duration::from_secs(30);
        let config = config.to_str().expect("UTF-8 volume config");
        loop {
            let output = self.yy(&["--volume", config, "status"]);
            output.success_or("yy status");
            let text = String::from_utf8_lossy(&output.stdout).into_owned();
            if text
                .lines()
                .any(|line| line.starts_with("handle: ") && line.contains("frozen: true"))
            {
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
                self.service_address()
            ));
        }
        json.push_str("\n}");
        let path = self.temp.path().join(format!("{name}.json"));
        fs::write(&path, json).expect("write volume config");
        path
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

    pub fn remote_write(&self, config: &Path, path: &str, bytes: &[u8]) {
        let handle = self
            .yy_ok(&[
                "--volume",
                config.to_str().expect("UTF-8 volume config"),
                "file",
                "open",
                path,
                "--write",
            ])
            .text();
        let source = self.temp.path().join("remote-write-source");
        fs::write(&source, bytes).expect("write remote source");
        self.yy_ok(&[
            "--volume",
            config.to_str().expect("UTF-8 volume config"),
            "file",
            "write",
            &handle,
            source.to_str().expect("UTF-8 source path"),
        ]);
        self.yy_ok(&[
            "--volume",
            config.to_str().expect("UTF-8 volume config"),
            "file",
            "fsync",
            &handle,
        ]);
        self.yy_ok(&[
            "--volume",
            config.to_str().expect("UTF-8 volume config"),
            "file",
            "close",
            &handle,
        ]);
    }

    pub fn remote_truncate(&self, config: &Path, path: &str, length: u64) {
        let handle = self
            .yy_ok(&[
                "--volume",
                config.to_str().expect("UTF-8 volume config"),
                "file",
                "open",
                path,
                "--write",
            ])
            .text();
        self.yy_ok(&[
            "--volume",
            config.to_str().expect("UTF-8 volume config"),
            "file",
            "truncate",
            &handle,
            &length.to_string(),
        ]);
        self.yy_ok(&[
            "--volume",
            config.to_str().expect("UTF-8 volume config"),
            "file",
            "fsync",
            &handle,
        ]);
        self.yy_ok(&[
            "--volume",
            config.to_str().expect("UTF-8 volume config"),
            "file",
            "close",
            &handle,
        ]);
    }

    pub fn remote_read(&self, config: &Path, path: &str, length: usize) -> Vec<u8> {
        let handle = self
            .yy_ok(&[
                "--volume",
                config.to_str().expect("UTF-8 volume config"),
                "file",
                "open",
                path,
            ])
            .text();
        let output = self.yy_ok(&[
            "--volume",
            config.to_str().expect("UTF-8 volume config"),
            "file",
            "read",
            &handle,
            "--length",
            &length.to_string(),
        ]);
        self.yy_ok(&[
            "--volume",
            config.to_str().expect("UTF-8 volume config"),
            "file",
            "close",
            &handle,
        ]);
        output.stdout
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
        let stdout_thread = thread::spawn(move || read_all(stdout));
        let stderr_thread = thread::spawn(move || read_all(stderr));
        let mut mounted = Mounted {
            child,
            stdout: Some(stdout_thread),
            stderr: Some(stderr_thread),
            mountpoint: PathBuf::from(mountpoint),
            staging,
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

    pub fn terminate(&mut self) {
        let pid = self.child.id().to_string();
        let output = run_external("kill", &["-TERM", &pid], Duration::from_secs(30));
        output.success_or("send SIGTERM to yy mount");
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
