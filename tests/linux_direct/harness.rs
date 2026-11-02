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

//! Real raw-S3 and Linux FUSE harness for read-only Direct acceptance.

pub use crate::process::{Captured, run_timeout};
pub use crate::raw_s3::{RawInventory as Inventory, RawS3};
use crate::real_s3::AwsS3;
use nix::sys::signal::{Signal, kill};
use nix::unistd::Pid;
use serde_json::{Value, json};
use std::fs;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

const STORAGE_ENV_WITH_SESSION: [&str; 6] = [
    "YINYANG_S3_BUCKET",
    "YINYANG_S3_ENDPOINT",
    "YINYANG_S3_REGION",
    "YINYANG_S3_ACCESS_KEY_ID",
    "YINYANG_S3_SECRET_ACCESS_KEY",
    "YINYANG_S3_SESSION_TOKEN",
];

pub fn direct_test_storage_profile() -> &'static str {
    match std::env::var("YINYANG_DIRECT_TEST_STORAGE_PROFILE").as_deref() {
        Ok("amazon-s3") => "amazon-s3",
        Ok("minio") | Err(_) => "minio",
        Ok(profile) => panic!("unsupported Direct acceptance storage profile {profile:?}"),
    }
}

pub fn require_direct_environment() {
    if !Path::new("/dev/fuse").exists() {
        panic!("/dev/fuse is unavailable; Direct acceptance must fail rather than skip");
    }
    for tool in ["fusermount3", "timeout", "python3"] {
        if Command::new(tool).arg("--version").output().is_err() {
            panic!("{tool} is unavailable; Direct acceptance must fail rather than skip");
        }
    }
    let amazon_s3 = direct_test_storage_profile() == "amazon-s3";
    for name in STORAGE_ENV_WITH_SESSION {
        if amazon_s3 && name == "YINYANG_S3_ENDPOINT" {
            assert!(
                std::env::var_os(name).is_none(),
                "{name} must be unset for the Amazon S3 default endpoint"
            );
        } else if !amazon_s3 && name == "YINYANG_S3_SESSION_TOKEN" {
            continue;
        } else if std::env::var_os(name).is_none() {
            panic!("{name} is unset; Direct acceptance must fail rather than skip");
        }
    }
}

enum FixtureRaw {
    Http(RawS3),
    Amazon(AwsS3),
}

pub struct Fixture {
    pub temp: tempfile::TempDir,
    pub root: String,
    bucket: String,
    raw: FixtureRaw,
}

pub struct RunningCommand {
    child: Option<Child>,
    stdout: Option<JoinHandle<Vec<u8>>>,
    stderr: Option<JoinHandle<Vec<u8>>>,
    description: String,
}

impl RunningCommand {
    pub fn spawn(mut command: Command) -> Self {
        let description = describe_command(&command);
        command
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut child = command
            .spawn()
            .unwrap_or_else(|error| panic!("spawn command ({description}): {error}"));
        let stdout = child.stdout.take().expect("stdout is piped");
        let stderr = child.stderr.take().expect("stderr is piped");
        Self {
            child: Some(child),
            stdout: Some(thread::spawn(move || read_captured(stdout))),
            stderr: Some(thread::spawn(move || read_captured(stderr))),
            description,
        }
    }

    pub fn kill(&mut self) {
        self.child
            .as_mut()
            .expect("running command is live")
            .kill()
            .expect("kill running command");
    }

    pub fn wait(mut self, timeout: Duration) -> Captured {
        let child = self.child.as_mut().expect("running command is live");
        let deadline = Instant::now() + timeout;
        let status = loop {
            match child.try_wait().expect("wait for running command") {
                Some(status) => break status,
                None if Instant::now() < deadline => thread::sleep(Duration::from_millis(25)),
                None => {
                    let _ = child.kill();
                    let status = child.wait().expect("wait after killing timed-out command");
                    let stdout = self.stdout.take().unwrap().join().unwrap_or_default();
                    let stderr = self.stderr.take().unwrap().join().unwrap_or_default();
                    panic!(
                        "command timed out after {timeout:?} with {status} ({}):\nstdout:\n{}\nstderr:\n{}",
                        self.description,
                        String::from_utf8_lossy(&stdout),
                        String::from_utf8_lossy(&stderr)
                    );
                }
            }
        };
        self.child.take();
        Captured {
            status,
            stdout: self.stdout.take().unwrap().join().unwrap_or_default(),
            stderr: self.stderr.take().unwrap().join().unwrap_or_default(),
        }
    }
}

impl Drop for RunningCommand {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
        if let Some(thread) = self.stdout.take() {
            let _ = thread.join();
        }
        if let Some(thread) = self.stderr.take() {
            let _ = thread.join();
        }
    }
}

impl Fixture {
    pub fn new() -> Self {
        require_direct_environment();
        Self::with_bucket(std::env::var("YINYANG_S3_BUCKET").expect("S3 bucket is required"))
    }

    pub fn versioned() -> Self {
        require_direct_environment();
        let bucket = std::env::var("YINYANG_DIRECT_VERSIONED_BUCKET").unwrap_or_else(|_| {
            panic!(
                "YINYANG_DIRECT_VERSIONED_BUCKET is required for the true-versioning Direct path"
            )
        });
        Self::with_bucket(bucket)
    }

    fn with_bucket(bucket: String) -> Self {
        let temp = tempfile::tempdir().expect("create Direct test directory");
        let root = format!(
            "direct-{}-{}",
            std::process::id(),
            uuid::Uuid::new_v4().simple()
        );
        let raw = if direct_test_storage_profile() == "amazon-s3" {
            FixtureRaw::Amazon(AwsS3::new(bucket.clone()))
        } else {
            let raw_base = RawS3::from_env();
            FixtureRaw::Http(raw_base.with_bucket(bucket.clone()))
        };
        Self {
            temp,
            root,
            bucket,
            raw,
        }
    }

    pub fn path(&self, name: &str) -> PathBuf {
        self.temp.path().join(name)
    }

    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    pub fn root(&self) -> &str {
        &self.root
    }

    pub fn write_raw(&self, key: &str, bytes: &[u8]) {
        let key = self.full_key(key);
        match &self.raw {
            FixtureRaw::Http(raw) => raw.put(&key, bytes),
            FixtureRaw::Amazon(raw) => raw.raw_put(&key, bytes),
        }
        assert_eq!(
            self.raw_get_storage_key(&key).as_deref(),
            Some(bytes),
            "raw S3 did not preserve the requested key {key:?}"
        );
    }

    pub fn delete_raw(&self, key: &str) {
        let key = self.full_key(key);
        match &self.raw {
            FixtureRaw::Http(raw) => raw.delete(&key),
            FixtureRaw::Amazon(raw) => raw.raw_delete(&key),
        }
    }

    pub fn raw_get(&self, key: &str) -> Option<Vec<u8>> {
        self.raw_get_storage_key(&self.full_key(key))
    }

    pub fn wait_raw_bytes(&self, key: &str, expected: &[u8], timeout: Duration) -> Option<Vec<u8>> {
        let mut observed = None;
        wait_until(timeout, &format!("raw Direct bytes for {key:?}"), || {
            observed = self.raw_get(key);
            observed.as_deref() == Some(expected)
        });
        observed
    }

    pub fn inventory(&self) -> Inventory {
        let prefix = format!("{}/", self.root);
        let inventory = match &self.raw {
            FixtureRaw::Http(raw) => raw.inventory(&prefix),
            FixtureRaw::Amazon(raw) => raw.raw_inventory(&prefix),
        };
        inventory
            .into_iter()
            .map(|(key, object)| {
                let key = key
                    .strip_prefix(&prefix)
                    .unwrap_or_else(|| panic!("raw inventory key {key:?} escaped Direct root"))
                    .to_owned();
                (key, object)
            })
            .collect()
    }

    pub fn config(
        &self,
        name: &str,
        frontend: &str,
        read_only: Option<bool>,
        staging: Option<&Path>,
    ) -> PathBuf {
        let mut config = json!({
            "name": name,
            "model": "direct",
            "access": "mount",
            "frontend": frontend,
            "publication": "object",
            "storage_profile": direct_test_storage_profile(),
        });
        if let Some(read_only) = read_only {
            config["read_only"] = Value::Bool(read_only);
        }
        if let Some(staging) = staging {
            config["staging"] = Value::String(
                staging
                    .to_str()
                    .expect("Direct staging path is UTF-8")
                    .to_owned(),
            );
        }
        self.config_document(name, config)
    }

    pub fn writable_config(&self, name: &str, frontend: &str, staging: &Path) -> PathBuf {
        self.config_document(
            name,
            json!({
                "name": name,
                "model": "direct",
                "access": "mount",
                "frontend": frontend,
                "publication": "object",
                "storage_profile": direct_test_storage_profile(),
                "read_only": false,
                "staging": staging,
            }),
        )
    }

    pub fn config_document(&self, name: &str, config: Value) -> PathBuf {
        let path = self.path(&format!("{name}.json"));
        fs::write(
            &path,
            serde_json::to_vec_pretty(&config).expect("encode Direct config"),
        )
        .expect("write Direct config");
        path
    }

    pub fn yy_command(&self, args: &[&str]) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_yy"));
        command.args(args);
        let amazon_s3 = direct_test_storage_profile() == "amazon-s3";
        for name in STORAGE_ENV_WITH_SESSION {
            let value = if name == "YINYANG_S3_BUCKET" {
                self.bucket.clone()
            } else if amazon_s3 && name == "YINYANG_S3_ENDPOINT" {
                command.env_remove(name);
                continue;
            } else if !amazon_s3 && name == "YINYANG_S3_SESSION_TOKEN" {
                continue;
            } else {
                std::env::var(name)
                    .unwrap_or_else(|_| panic!("{name} is required for Direct acceptance"))
            };
            command.env(name, value);
        }
        command.env("YINYANG_S3_ROOT", &self.root);
        command.env("YINYANG_STORAGE_PROFILE", direct_test_storage_profile());
        command
    }

    pub fn yy(&self, args: &[&str]) -> Captured {
        run_timeout(self.yy_command(args), Duration::from_secs(120))
    }

    pub fn yy_ok(&self, args: &[&str]) -> Captured {
        let output = self.yy(args);
        output.success_or(&format!("yy {args:?}"));
        output
    }

    pub fn spawn_mount(&self, config: &Path, mountpoint: &Path, _frontend: &str) -> Mounted {
        let command = self.mount_command(config, mountpoint);
        Mounted::spawn(command, mountpoint)
    }

    pub fn spawn_writable_mount(&self, config: &Path, mountpoint: &Path) -> Mounted {
        let command = self.mount_command(config, mountpoint);
        Mounted::spawn_writable(command, mountpoint)
    }

    pub fn mount_command(&self, config: &Path, mountpoint: &Path) -> Command {
        self.yy_command(&[
            "--volume",
            config.to_str().expect("Direct config path is UTF-8"),
            "mount",
            mountpoint
                .to_str()
                .expect("Direct mountpoint path is UTF-8"),
        ])
    }

    pub fn raw_object(&self, key: &str) -> Option<crate::raw_s3::RawObject> {
        let full_key = self.full_key(key);
        match &self.raw {
            FixtureRaw::Http(raw) => raw.inventory(&full_key).remove(&full_key),
            FixtureRaw::Amazon(raw) => raw.raw_object(&full_key),
        }
    }

    pub fn full_key(&self, key: &str) -> String {
        format!("{}/{key}", self.root)
    }

    fn raw_get_storage_key(&self, key: &str) -> Option<Vec<u8>> {
        match &self.raw {
            FixtureRaw::Http(raw) => raw.get(key),
            FixtureRaw::Amazon(raw) => raw.raw_get(key),
        }
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        if let FixtureRaw::Amazon(raw) = &self.raw {
            raw.raw_cleanup(&self.root);
        }
    }
}

pub struct Mounted {
    child: Option<Child>,
    mountpoint: PathBuf,
    logs: Arc<Mutex<Vec<u8>>>,
    readers: Vec<JoinHandle<()>>,
}

impl Mounted {
    pub fn spawn(command: Command, mountpoint: &Path) -> Self {
        Self::spawn_with_policy(command, mountpoint, true)
    }

    pub fn spawn_writable(command: Command, mountpoint: &Path) -> Self {
        Self::spawn_with_policy(command, mountpoint, false)
    }

    fn spawn_with_policy(mut command: Command, mountpoint: &Path, read_only: bool) -> Self {
        command.stdout(Stdio::piped()).stderr(Stdio::piped());
        let mut child = command
            .spawn()
            .unwrap_or_else(|error| panic!("spawn Direct FUSE mount: {error}"));
        let stdout = child.stdout.take().expect("mount stdout is piped");
        let stderr = child.stderr.take().expect("mount stderr is piped");
        let logs = Arc::new(Mutex::new(Vec::new()));
        let stdout = spawn_log_reader(stdout, logs.clone());
        let stderr = spawn_log_reader(stderr, logs.clone());
        let mut mounted = Mounted {
            child: Some(child),
            mountpoint: mountpoint.to_owned(),
            logs,
            readers: vec![stdout, stderr],
        };
        mounted.wait_visible(Duration::from_secs(30), read_only);
        mounted
    }
    pub fn mountpoint(&self) -> &Path {
        &self.mountpoint
    }

    pub fn pid(&self) -> Pid {
        Pid::from_raw(self.child.as_ref().expect("mount process is live").id() as i32)
    }

    pub fn logs(&self) -> String {
        String::from_utf8_lossy(&self.logs.lock().expect("mount log lock")).into_owned()
    }

    fn wait_visible(&mut self, timeout: Duration, read_only: bool) {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some((filesystem, options)) = mount_info(&self.mountpoint) {
                assert_eq!(
                    filesystem, "fuse.yinyang",
                    "Direct mount must expose the YinYang FUSE filesystem type"
                );
                let kernel_ro = options.split(',').any(|option| option == "ro");
                assert_eq!(
                    kernel_ro, read_only,
                    "Direct mount policy does not match the kernel mount options: {options}"
                );
                return;
            }
            if let Some(status) = self
                .child
                .as_mut()
                .expect("mount process is live")
                .try_wait()
                .expect("poll Direct mount")
            {
                panic!(
                    "Direct mount exited before becoming visible with {status}:\n{}",
                    self.logs()
                );
            }
            if Instant::now() >= deadline {
                panic!(
                    "Direct mount {} did not become visible within {timeout:?}:\n{}",
                    self.mountpoint.display(),
                    self.logs()
                );
            }
            thread::sleep(Duration::from_millis(50));
        }
    }

    pub fn refresh(&self) {
        kill(self.pid(), Signal::SIGHUP).expect("send SIGHUP to Direct mount");
    }

    pub fn refresh_and_wait(&self, timeout: Duration) -> String {
        const COMPLETION: &str = "yy: fuse refresh: done (";
        let before = self.logs().matches(COMPLETION).count();
        self.refresh();
        wait_until(timeout, "a completed Direct SIGHUP completion line", || {
            self.logs().matches(COMPLETION).count() > before
        });
        let logs = self.logs();
        let completion = logs
            .lines()
            .filter(|line| line.contains(COMPLETION))
            .nth(before)
            .unwrap_or_else(|| panic!("missing Direct refresh completion line:\n{logs}"))
            .to_owned();
        assert!(
            completion.contains("errors=0"),
            "Direct refresh did not invalidate every known entry: {completion}"
        );
        completion
    }

    pub fn wait_for_log(&self, needle: &str, timeout: Duration) {
        wait_until(timeout, &format!("mount log containing {needle:?}"), || {
            self.logs().contains(needle)
        });
    }

    pub fn unmount(mut self) {
        let output = run_timeout(
            command("fusermount3", &["-u", path_text(&self.mountpoint)]),
            Duration::from_secs(30),
        );
        if !output.status.success() {
            panic!("fusermount3 failed:\n{}", output.text());
        }
        self.wait(Duration::from_secs(30));
        self.join_readers();
    }

    pub fn terminate(mut self) {
        kill(self.pid(), Signal::SIGTERM).expect("send SIGTERM to Direct mount");
        self.wait(Duration::from_secs(30));
        self.join_readers();
    }

    pub fn crash(mut self) {
        kill(self.pid(), Signal::SIGKILL).expect("send SIGKILL to Direct mount");
        let child = self.child.as_mut().expect("mount process is live");
        let status = child.wait().expect("wait for crashed Direct mount");
        self.child.take();
        assert!(
            !status.success(),
            "SIGKILLed Direct mount unexpectedly exited successfully"
        );
        self.join_readers();
    }

    fn wait(&mut self, timeout: Duration) {
        let child = self.child.as_mut().expect("mount process is live");
        let deadline = Instant::now() + timeout;
        loop {
            match child.try_wait().expect("wait for Direct mount") {
                Some(status) => {
                    assert!(
                        status.success(),
                        "Direct mount exited with {status}:\n{}",
                        self.logs()
                    );
                    self.child.take();
                    return;
                }
                None if Instant::now() < deadline => {
                    thread::sleep(Duration::from_millis(50));
                }
                None => {
                    let _ = child.kill();
                    let status = child.wait().expect("wait after killing Direct mount");
                    panic!(
                        "Direct mount did not exit within {timeout:?}; killed with {status}:\n{}",
                        self.logs()
                    );
                }
            }
        }
    }

    fn join_readers(&mut self) {
        for reader in self.readers.drain(..) {
            reader.join().expect("join Direct mount log reader");
        }
    }
}

impl Drop for Mounted {
    fn drop(&mut self) {
        let _ = run_timeout(
            command("fusermount3", &["-u", "-z", path_text(&self.mountpoint)]),
            Duration::from_secs(5),
        );
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
        for reader in self.readers.drain(..) {
            let _ = reader.join();
        }
    }
}

pub fn assert_errno(error: &std::io::Error, expected: &[i32], context: &str) {
    let actual = error.raw_os_error().unwrap_or_default();
    assert!(
        expected.contains(&actual),
        "{context}: expected errno {expected:?}, got {actual} ({error})"
    );
}

pub struct RequestCounter {
    address: std::net::SocketAddr,
    requests: Arc<AtomicUsize>,
    request_lines: Arc<Mutex<Vec<String>>>,
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

impl RequestCounter {
    pub fn start() -> Self {
        Self::start_inner(None)
    }

    pub fn start_with_response(response: Vec<u8>) -> Self {
        Self::start_inner(Some(response))
    }

    fn start_inner(response: Option<Vec<u8>>) -> Self {
        let listener =
            std::net::TcpListener::bind("127.0.0.1:0").expect("bind Direct request counter");
        listener
            .set_nonblocking(true)
            .expect("configure Direct request counter");
        let address = listener
            .local_addr()
            .expect("Direct request counter address");
        let requests = Arc::new(AtomicUsize::new(0));
        let request_lines = Arc::new(Mutex::new(Vec::new()));
        let stop = Arc::new(AtomicBool::new(false));
        let thread_requests = requests.clone();
        let thread_request_lines = request_lines.clone();
        let thread_stop = stop.clone();
        let thread = thread::spawn(move || {
            while !thread_stop.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        thread_requests.fetch_add(1, Ordering::Relaxed);
                        let request_line = read_request_line(&mut stream);
                        thread_request_lines
                            .lock()
                            .expect("Direct request line lock")
                            .push(request_line);
                        let response = response.as_deref().unwrap_or(
                            b"HTTP/1.1 500 Internal Server Error\r\n\
                              Content-Length: 0\r\n\
                              Connection: close\r\n\r\n",
                        );
                        let _ = stream.write_all(response);
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(10));
                    }
                    Err(error) => panic!("accept Direct request counter connection: {error}"),
                }
            }
        });
        Self {
            address,
            requests,
            request_lines,
            stop,
            thread: Some(thread),
        }
    }

    pub fn endpoint(&self) -> String {
        format!("http://{}", self.address)
    }

    pub fn count(&self) -> usize {
        self.requests.load(Ordering::Relaxed)
    }

    pub fn request_lines(&self) -> Vec<String> {
        self.request_lines
            .lock()
            .expect("Direct request line lock")
            .clone()
    }

    pub fn assert_no_requests(&self, context: &str) {
        thread::sleep(Duration::from_millis(250));
        assert_eq!(
            self.count(),
            0,
            "{context} reached the S3 endpoint before admission rejected it"
        );
    }
}

fn read_request_line(stream: &mut std::net::TcpStream) -> String {
    const MAX_HEADER: usize = 64 * 1024;

    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .expect("set Direct request counter read timeout");
    let mut request = Vec::new();
    let mut buffer = [0_u8; 4096];
    while request.len() < MAX_HEADER {
        match stream.read(&mut buffer) {
            Ok(0) | Err(_) => break,
            Ok(count) => {
                request.extend_from_slice(&buffer[..count]);
                if request.windows(4).any(|window| window == b"\r\n\r\n") {
                    break;
                }
            }
        }
    }
    request
        .split(|byte| *byte == b'\r' || *byte == b'\n')
        .next()
        .map(String::from_utf8_lossy)
        .unwrap_or_default()
        .into_owned()
}

fn describe_command(command: &Command) -> String {
    std::iter::once(command.get_program())
        .chain(command.get_args())
        .map(|value| format!("{value:?}"))
        .collect::<Vec<_>>()
        .join(" ")
}

fn read_captured(mut reader: impl Read) -> Vec<u8> {
    let mut bytes = Vec::new();
    let _ = reader.read_to_end(&mut bytes);
    bytes
}

impl Drop for RequestCounter {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(thread) = self.thread.take() {
            thread.join().expect("join Direct request counter");
        }
    }
}

pub fn wait_until(timeout: Duration, context: &str, mut condition: impl FnMut() -> bool) {
    let deadline = Instant::now() + timeout;
    while !condition() {
        if Instant::now() >= deadline {
            panic!("timed out after {timeout:?} waiting for {context}");
        }
        thread::sleep(Duration::from_millis(50));
    }
}

pub fn local_paths(root: &Path) -> Vec<String> {
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

pub fn admitted_capabilities(document: &[u8]) -> std::collections::BTreeSet<String> {
    let value: Value = serde_json::from_slice(document).expect("capabilities JSON");
    value["admitted"]
        .as_array()
        .expect("admitted capability array")
        .iter()
        .map(|value| value.as_str().expect("capability name").to_owned())
        .collect()
}

pub fn payload(length: usize, seed: u8) -> Vec<u8> {
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

pub fn read_fully_at(file: &fs::File, offset: u64, length: usize) -> std::io::Result<Vec<u8>> {
    use std::os::unix::fs::FileExt;

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

pub fn path_text(path: &Path) -> &str {
    path.to_str().expect("test path is UTF-8")
}

fn mount_info(mountpoint: &Path) -> Option<(String, String)> {
    let mountpoint = path_text(mountpoint);
    let mountinfo = fs::read_to_string("/proc/self/mountinfo").ok()?;
    for line in mountinfo.lines() {
        let (before, after) = line.split_once(" - ")?;
        let fields = before.split_whitespace().collect::<Vec<_>>();
        if fields.get(4).copied() != Some(mountpoint) {
            continue;
        }
        let mut trailing = after.split_whitespace();
        let filesystem = trailing.next()?.to_owned();
        let options = fields.get(5).copied().unwrap_or_default().to_owned();
        return Some((filesystem, options));
    }
    None
}

fn command(program: &str, args: &[&str]) -> Command {
    let mut command = Command::new(program);
    command.args(args);
    command
}

fn spawn_log_reader(
    mut reader: impl Read + Send + 'static,
    logs: Arc<Mutex<Vec<u8>>>,
) -> JoinHandle<()> {
    thread::spawn(move || {
        let mut buffer = [0_u8; 4096];
        loop {
            match reader.read(&mut buffer) {
                Ok(0) | Err(_) => return,
                Ok(count) => {
                    let mut output = logs.lock().expect("mount log lock");
                    output.extend_from_slice(&buffer[..count]);
                }
            }
        }
    })
}
