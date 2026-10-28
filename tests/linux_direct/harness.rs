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

const STORAGE_ENV: [&str; 5] = [
    "YINYANG_S3_BUCKET",
    "YINYANG_S3_ENDPOINT",
    "YINYANG_S3_REGION",
    "YINYANG_S3_ACCESS_KEY_ID",
    "YINYANG_S3_SECRET_ACCESS_KEY",
];

pub fn require_direct_environment() {
    if !Path::new("/dev/fuse").exists() {
        panic!("/dev/fuse is unavailable; Direct acceptance must fail rather than skip");
    }
    for tool in ["fusermount3", "timeout"] {
        if Command::new(tool).arg("--version").output().is_err() {
            panic!("{tool} is unavailable; Direct acceptance must fail rather than skip");
        }
    }
    for name in STORAGE_ENV {
        if std::env::var_os(name).is_none() {
            panic!("{name} is unset; Direct acceptance must fail rather than skip");
        }
    }
}

pub struct Fixture {
    pub temp: tempfile::TempDir,
    pub root: String,
    bucket: String,
    raw: RawS3,
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
        let raw_base = RawS3::from_env();
        let raw = raw_base.with_bucket(bucket.clone());
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

    pub fn write_raw(&self, key: &str, bytes: &[u8]) -> u16 {
        let key = self.full_key(key);
        let status = self.raw.put(&key, bytes);
        assert_eq!(
            self.raw.get(&key).as_deref(),
            Some(bytes),
            "raw S3 did not preserve the requested key {key:?}"
        );
        status
    }

    pub fn delete_raw(&self, key: &str) {
        self.raw.delete(&self.full_key(key));
    }

    pub fn read_raw(&self, key: &str) -> Option<Vec<u8>> {
        self.raw.get(&self.full_key(key))
    }

    pub fn raw_list_xml(&self) -> String {
        self.raw.list_xml(&format!("{}/", self.root))
    }

    pub fn inventory(&self) -> Inventory {
        let prefix = format!("{}/", self.root);
        self.raw
            .inventory(&prefix)
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
            "storage_profile": "minio",
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
        for name in STORAGE_ENV {
            let value = if name == "YINYANG_S3_BUCKET" {
                self.bucket.clone()
            } else {
                std::env::var(name)
                    .unwrap_or_else(|_| panic!("{name} is required for Direct acceptance"))
            };
            command.env(name, value);
        }
        command.env("YINYANG_S3_ROOT", &self.root);
        command.env("YINYANG_STORAGE_PROFILE", "minio");
        command
    }

    pub fn operator(&self) -> opendal::Operator {
        self.operator_with(&[])
    }

    pub fn operator_with(&self, extra: &[(&str, &str)]) -> opendal::Operator {
        self.operator_at_root(&self.root, extra)
    }

    pub fn operator_at_root(&self, root: &str, extra: &[(&str, &str)]) -> opendal::Operator {
        opendal::install_default();
        let mut config = std::env::vars()
            .filter_map(|(key, value)| {
                key.strip_prefix("YINYANG_S3_")
                    .map(|key| (key.to_ascii_lowercase(), value))
            })
            .filter(|(key, _)| key != "root")
            .collect::<Vec<_>>();
        if !root.is_empty() {
            config.push(("root".to_owned(), root.to_owned()));
        }
        config.extend(
            extra
                .iter()
                .map(|(key, value)| ((*key).to_owned(), (*value).to_owned())),
        );
        opendal::Operator::via_iter("s3", config).expect("construct Direct test operator")
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
        let mut command = self.yy_command(&[
            "--volume",
            config.to_str().expect("Direct config path is UTF-8"),
            "mount",
            mountpoint
                .to_str()
                .expect("Direct mountpoint path is UTF-8"),
        ]);
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
        mounted.wait_visible(Duration::from_secs(30));
        mounted
    }

    fn full_key(&self, key: &str) -> String {
        format!("{}/{key}", self.root)
    }
}

pub struct Mounted {
    child: Option<Child>,
    mountpoint: PathBuf,
    logs: Arc<Mutex<Vec<u8>>>,
    readers: Vec<JoinHandle<()>>,
}

impl Mounted {
    pub fn mountpoint(&self) -> &Path {
        &self.mountpoint
    }

    pub fn pid(&self) -> Pid {
        Pid::from_raw(self.child.as_ref().expect("mount process is live").id() as i32)
    }

    pub fn logs(&self) -> String {
        String::from_utf8_lossy(&self.logs.lock().expect("mount log lock")).into_owned()
    }

    fn wait_visible(&mut self, timeout: Duration) {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some((filesystem, options)) = mount_info(&self.mountpoint) {
                assert_eq!(
                    filesystem, "fuse.yinyang",
                    "Direct mount must expose the YinYang FUSE filesystem type"
                );
                assert!(
                    options.split(',').any(|option| option == "ro"),
                    "default Direct mount must include the kernel ro option: {options}"
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

    pub fn refresh_and_wait(&self) {
        let before = self.logs().matches("yy: fuse refresh: done (").count();
        self.refresh();
        wait_until(
            Duration::from_secs(30),
            "a new Direct refresh completion",
            || self.logs().matches("yy: fuse refresh: done (").count() > before,
        );
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
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

impl RequestCounter {
    pub fn start() -> Self {
        let listener =
            std::net::TcpListener::bind("127.0.0.1:0").expect("bind Direct request counter");
        listener
            .set_nonblocking(true)
            .expect("configure Direct request counter");
        let address = listener
            .local_addr()
            .expect("Direct request counter address");
        let requests = Arc::new(AtomicUsize::new(0));
        let stop = Arc::new(AtomicBool::new(false));
        let thread_requests = requests.clone();
        let thread_stop = stop.clone();
        let thread = thread::spawn(move || {
            while !thread_stop.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        thread_requests.fetch_add(1, Ordering::Relaxed);
                        let _ = stream.write_all(
                            b"HTTP/1.1 500 Internal Server Error\r\n\
                              Content-Length: 0\r\n\
                              Connection: close\r\n\r\n",
                        );
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

    pub fn assert_no_requests(&self, context: &str) {
        thread::sleep(Duration::from_millis(250));
        assert_eq!(
            self.count(),
            0,
            "{context} reached the S3 endpoint before admission rejected it"
        );
    }
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
