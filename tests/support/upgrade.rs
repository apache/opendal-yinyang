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

//! Shared installed-prefix, real-process and isolated-S3 acceptance helpers.

use super::raw_s3::RawS3;
use super::s3_gate::S3Gate;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs;
use std::io::{BufRead, BufReader, Read};
use std::net::SocketAddr;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

pub const PROCESS_TIMEOUT: Duration = Duration::from_secs(180);
const SERVICE_TOKEN: &str = "installed-upgrade-service-token-with-at-least-32-bytes";

pub fn require_environment(extra: &[&str], context: &str) {
    for name in extra.iter().copied().chain([
        "YINYANG_S3_BUCKET",
        "YINYANG_S3_ENDPOINT",
        "YINYANG_S3_REGION",
        "YINYANG_S3_ACCESS_KEY_ID",
        "YINYANG_S3_SECRET_ACCESS_KEY",
    ]) {
        assert!(
            std::env::var_os(name).is_some(),
            "{name} is unset; {context} fails hard instead of skipping"
        );
    }
    assert_eq!(
        std::env::var("YINYANG_STORAGE_PROFILE").as_deref(),
        Ok("minio"),
        "{context} uses the isolated Silo qualification storage"
    );
    assert!(
        std::env::var("YINYANG_S3_ENDPOINT")
            .expect("isolated S3 endpoint")
            .starts_with("http://"),
        "{context} fault injection requires the plain-HTTP Silo endpoint"
    );
}

pub fn target_source() -> String {
    if let Ok(source) = std::env::var("YINYANG_UPGRADE_SOURCE") {
        return source;
    }
    let output = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .expect("run git rev-parse for upgrade target");
    assert!(
        output.status.success(),
        "git rev-parse HEAD failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout)
        .expect("target source is UTF-8")
        .trim()
        .to_owned()
}

pub struct Storage {
    pub raw: RawS3,
    variables: Vec<(&'static str, String)>,
}

impl Storage {
    pub fn new(context: &str) -> Self {
        let mut variables = Vec::new();
        for name in [
            "YINYANG_S3_BUCKET",
            "YINYANG_S3_ENDPOINT",
            "YINYANG_S3_REGION",
            "YINYANG_S3_ACCESS_KEY_ID",
            "YINYANG_S3_SECRET_ACCESS_KEY",
        ] {
            variables.push((
                name,
                std::env::var(name).unwrap_or_else(|_| panic!("{name} is required for {context}")),
            ));
        }
        Self {
            raw: RawS3::from_env(),
            variables,
        }
    }

    pub fn root(&self, label: &str) -> String {
        format!("linux-upgrade/{label}-{}", uuid::Uuid::new_v4().simple())
    }

    pub fn command(
        &self,
        binary: &Path,
        args: &[&str],
        root: &str,
        token: Option<&str>,
    ) -> Command {
        let mut command = Command::new(binary);
        self.apply(&mut command, root, token);
        command.args(args);
        command
    }

    pub fn apply(&self, command: &mut Command, root: &str, token: Option<&str>) {
        for (name, value) in std::env::vars_os() {
            if !name.to_string_lossy().starts_with("YINYANG_") {
                command.env(name, value);
            }
        }
        for (name, value) in &self.variables {
            command.env(name, value);
        }
        command
            .env("YINYANG_STORAGE_PROFILE", "minio")
            .env("YINYANG_S3_ROOT", root)
            .env("YINYANG_SERVICE_TOKEN", token.unwrap_or(SERVICE_TOKEN));
    }
}

pub struct RemoteCleanup {
    roots: Vec<String>,
}

impl RemoteCleanup {
    pub fn new(roots: Vec<String>) -> Self {
        Self { roots }
    }

    pub fn add(&mut self, root: String) {
        self.roots.push(root);
    }

    pub fn cleanup(&mut self) {
        let raw = RawS3::from_env();
        for root in self.roots.drain(..) {
            for _ in 0..3 {
                let keys = raw
                    .inventory(&format!("{root}/"))
                    .into_keys()
                    .collect::<Vec<_>>();
                if keys.is_empty() {
                    break;
                }
                for key in keys {
                    raw.delete(&key);
                }
            }
        }
    }
}

impl Drop for RemoteCleanup {
    fn drop(&mut self) {
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| self.cleanup()));
    }
}

pub struct ServiceProcess {
    child: Option<Child>,
    stderr: Option<thread::JoinHandle<Vec<u8>>>,
    pub address: SocketAddr,
}

impl ServiceProcess {
    pub fn start(binary: &Path, storage: &Storage, root: &str, database: &Path) -> Self {
        let listener =
            std::net::TcpListener::bind("127.0.0.1:0").expect("reserve metadata service port");
        let address = listener.local_addr().expect("reserve service address");
        drop(listener);
        Self::start_at(binary, storage, root, database, address)
    }

    pub fn start_at(
        binary: &Path,
        storage: &Storage,
        root: &str,
        database: &Path,
        address: SocketAddr,
    ) -> Self {
        let mut command = storage.command(
            binary,
            &[
                "serve",
                "--database",
                path_text(database),
                "--listen",
                &address.to_string(),
            ],
            root,
            None,
        );
        command
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut child = command.spawn().expect("spawn metadata service");
        let stdout = child.stdout.take().expect("service stdout");
        let stderr = child.stderr.take().expect("service stderr");
        let (sender, receiver) = mpsc::channel();
        thread::spawn(move || {
            let mut reader = BufReader::new(stdout);
            let mut line = String::new();
            let result = reader.read_line(&mut line);
            let _ = sender.send((result, line));
            let mut rest = Vec::new();
            let _ = reader.read_to_end(&mut rest);
        });
        let stderr = thread::spawn(move || {
            let mut bytes = Vec::new();
            let _ = BufReader::new(stderr).read_to_end(&mut bytes);
            bytes
        });
        let (read, line) = receiver
            .recv_timeout(Duration::from_secs(60))
            .expect("metadata service readiness timeout");
        read.expect("read metadata service readiness");
        let address = match line
            .trim()
            .strip_prefix("metadata service listening on ")
            .and_then(|value| value.parse().ok())
        {
            Some(address) => address,
            None => {
                let _ = child.kill();
                let status = child.wait().expect("wait for failed metadata service");
                let stderr = stderr.join().unwrap_or_default();
                panic!(
                    "unexpected metadata service readiness {line:?} with {status}:\n{}",
                    String::from_utf8_lossy(&stderr)
                );
            }
        };
        Self {
            child: Some(child),
            stderr: Some(stderr),
            address,
        }
    }

    pub fn kill_without_cleanup(mut self) {
        if let Some(mut child) = self.child.take() {
            child.kill().expect("kill metadata service");
            let _ = child.wait();
        }
        if let Some(stderr) = self.stderr.take() {
            let _ = stderr.join();
        }
    }

    pub fn stop(mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
        if let Some(stderr) = self.stderr.take() {
            let _ = stderr.join();
        }
    }
}

impl Drop for ServiceProcess {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
        if let Some(stderr) = self.stderr.take() {
            let _ = stderr.join();
        }
    }
}

pub fn current_yy_command(binary: &Path, storage: &Storage, root: &str) -> Command {
    storage.command(binary, &[], root, None)
}

pub fn volume_command(
    binary: &Path,
    storage: &Storage,
    root: &str,
    config: &Path,
    token: Option<&str>,
) -> Command {
    let mut command = storage.command(binary, &[], root, token);
    command.args(["--volume", path_text(config)]);
    command
}

pub fn volume_file_command(binary: &Path, storage: &Storage, root: &str, config: &Path) -> Command {
    let mut command = volume_command(binary, storage, root, config, None);
    command.arg("file");
    command
}

pub fn run_file_ok(
    binary: &Path,
    storage: &Storage,
    root: &str,
    config: &Path,
    args: &[&str],
) -> Output {
    run_ok(
        volume_file_command(binary, storage, root, config).args(args),
        &format!("installed yy file {args:?}"),
    )
}

pub fn run_ok(command: &mut Command, context: &str) -> Output {
    command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = command
        .spawn()
        .unwrap_or_else(|error| panic!("{context}: spawn: {error}"));
    let stdout = child.stdout.take().expect("command stdout");
    let stderr = child.stderr.take().expect("command stderr");
    let stdout = thread::spawn(move || read_all(stdout));
    let stderr = thread::spawn(move || read_all(stderr));
    let deadline = Instant::now() + PROCESS_TIMEOUT;
    let status = loop {
        match child.try_wait() {
            Ok(Some(status)) => break status,
            Ok(None) if Instant::now() < deadline => thread::sleep(Duration::from_millis(25)),
            Ok(None) => {
                let _ = child.kill();
                let status = child.wait().expect("wait after command timeout");
                let stdout = stdout.join().unwrap_or_default();
                let stderr = stderr.join().unwrap_or_default();
                panic!(
                    "{context} timed out after {PROCESS_TIMEOUT:?}: {status}\n\
                     stdout:\n{}\nstderr:\n{}",
                    String::from_utf8_lossy(&stdout),
                    String::from_utf8_lossy(&stderr)
                );
            }
            Err(error) => {
                let _ = child.kill();
                let _ = child.wait();
                panic!("{context}: wait: {error}");
            }
        }
    };
    let output = Output {
        status,
        stdout: stdout.join().unwrap_or_default(),
        stderr: stderr.join().unwrap_or_default(),
    };
    assert!(
        output.status.success(),
        "{context} failed with {}:\n{}",
        output.status,
        output_text(&output)
    );
    output
}

pub fn wait_gate_or_child_exit(gate: &S3Gate, child: &mut Child, context: &str) {
    let deadline = Instant::now() + Duration::from_secs(90);
    loop {
        if gate.wait_intercepted_timeout(Duration::from_millis(100)) {
            return;
        }
        if let Some(status) = child.try_wait().expect("poll gated child") {
            let mut stdout = Vec::new();
            if let Some(mut pipe) = child.stdout.take() {
                pipe.read_to_end(&mut stdout)
                    .expect("read gated child stdout");
            }
            let mut stderr = Vec::new();
            if let Some(mut pipe) = child.stderr.take() {
                pipe.read_to_end(&mut stderr)
                    .expect("read gated child stderr");
            }
            let output = Output {
                status,
                stdout,
                stderr,
            };
            panic!(
                "{context} exited before the S3 gate intercepted its commit:\n{}",
                output_text(&output)
            );
        }
        assert!(
            Instant::now() < deadline,
            "{context} did not reach the S3 gate within 90s"
        );
    }
}

pub fn write_config(
    path: &Path,
    name: &str,
    access: &str,
    staging: &Path,
    publication: &str,
    service: Option<&str>,
) {
    let mut document = json!({
        "name": name,
        "model": "managed",
        "access": access,
        "frontend": "library",
        "publication": publication,
        "storage_profile": "minio",
        "staging": staging,
        "read_only": false,
        "require": [],
    });
    if let Some(service) = service {
        document["service_address"] = Value::String(service.to_owned());
    }
    fs::write(
        path,
        serde_json::to_vec_pretty(&document).expect("serialize volume"),
    )
    .expect("write upgrade volume");
}

pub fn open_handle(
    binary: &Path,
    storage: &Storage,
    root: &str,
    config: &Path,
    path: &str,
    write: bool,
) -> String {
    let mut command = volume_file_command(binary, storage, root, config);
    command.args(["open", path]);
    if write {
        command.arg("--write");
    }
    let output = run_ok(&mut command, &format!("open installed file {path}"));
    parse_handle(&output.stdout)
}

pub fn read_handle(
    binary: &Path,
    storage: &Storage,
    root: &str,
    config: &Path,
    handle: &str,
) -> Output {
    run_file_ok(binary, storage, root, config, &["read", handle])
}

pub fn parse_handle(stdout: &[u8]) -> String {
    String::from_utf8(stdout.to_vec())
        .expect("file handle is UTF-8")
        .trim()
        .to_owned()
}

pub fn handle_line<'a>(status: &'a str, handle: &str) -> &'a str {
    status
        .lines()
        .find(|line| line.starts_with(&format!("handle: {handle} ")))
        .unwrap_or_else(|| panic!("handle {handle} is missing from status:\n{status}"))
}

pub fn parse_file_commit(stdout: &[u8]) -> String {
    String::from_utf8(stdout.to_vec())
        .expect("file fsync output is UTF-8")
        .trim()
        .strip_prefix("committed: ")
        .and_then(|line| line.split(" revision: ").next())
        .expect("file fsync commit")
        .to_owned()
}

pub fn transaction_identity(plan: &[u8]) -> (String, String) {
    let (_, commit, digest) =
        yinyang_core::Transaction::inspect_identity(plan).expect("inspect frozen transaction");
    (
        uuid::Uuid::from_bytes(*commit.as_bytes()).to_string(),
        digest.iter().map(|byte| format!("{byte:02x}")).collect(),
    )
}

pub fn receipt(
    binary: &Path,
    storage: &Storage,
    root: &str,
    commit: &str,
) -> BTreeMap<String, String> {
    let output = run_ok(
        current_yy_command(binary, storage, root).args(["receipt", commit]),
        "query retained receipt",
    );
    String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter_map(|line| line.split_once(": "))
        .map(|(name, value)| (name.to_owned(), value.to_owned()))
        .collect()
}

pub fn assert_receipt(binary: &Path, storage: &Storage, root: &str, commit: &str, digest: &str) {
    let receipt = receipt(binary, storage, root, commit);
    assert_eq!(field(&receipt, "commit"), commit);
    assert_eq!(field(&receipt, "request digest"), digest);
}

pub fn field<'a>(values: &'a BTreeMap<String, String>, name: &str) -> &'a str {
    values
        .get(name)
        .unwrap_or_else(|| panic!("missing {name:?} in receipt fields {values:?}"))
}

pub fn path_text(path: &Path) -> &str {
    path.to_str().expect("acceptance path is UTF-8")
}

pub fn sha256_file(path: &Path) -> String {
    Sha256::digest(fs::read(path).expect("read binary for SHA-256"))
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

pub fn install_release(prefix: &Path, release: &str, source: &Path) -> PathBuf {
    assert!(
        !release.contains('/') && !release.contains(".."),
        "release directory must be one safe path component"
    );
    let directory = prefix.join(release);
    let bin = directory.join("bin");
    fs::create_dir_all(&bin).expect("create versioned release directory");
    let destination = bin.join("yy");
    fs::copy(source, &destination).expect("install release binary");
    let mut permissions = fs::metadata(&destination)
        .expect("release binary metadata")
        .permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(&destination, permissions).expect("set release binary mode");
    destination
}

pub fn switch_current(prefix: &Path, release: &str) {
    let temporary = prefix.join(".current.new");
    let current = prefix.join("current");
    if temporary.exists() {
        fs::remove_file(&temporary).expect("remove abandoned current staging link");
    }
    std::os::unix::fs::symlink(release, &temporary).expect("create current staging link");
    fs::rename(&temporary, &current).expect("atomically replace current symlink");
}

pub fn assert_no_installed_writer(prefix: &Path, work: &Path) {
    let output = Command::new("pgrep")
        .args(["-f", path_text(prefix)])
        .output()
        .expect("run pgrep for installed prefix");
    if output.status.success() {
        let processes = String::from_utf8_lossy(&output.stdout);
        let allowed = processes
            .lines()
            .all(|pid| !Path::new(&format!("/proc/{pid}/cwd")).exists());
        assert!(
            allowed,
            "an installed yy writer remains before the current switch:\n{processes}"
        );
    }
    assert!(
        work.exists(),
        "upgrade work directory disappeared before the current switch"
    );
}

pub fn assert_storage_inventory_complete_when_empty(storage: &Storage, root: &str) {
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut inventory = storage.raw.inventory(&format!("{root}/"));
    while !inventory.is_empty() && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(200));
        inventory = storage.raw.inventory(&format!("{root}/"));
    }
    assert!(
        inventory.is_empty(),
        "remote cleanup did not remove every key below {root}: {:?}",
        inventory.keys().collect::<Vec<_>>()
    );
}

pub fn wal_path(database: &Path) -> PathBuf {
    PathBuf::from(format!("{}-wal", database.display()))
}

pub fn sqlite_journal_mode(database: &Path) -> String {
    let connection =
        rusqlite::Connection::open_with_flags(database, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
            .expect("open SQLite database read-only");
    connection
        .query_row("PRAGMA journal_mode", [], |row| row.get(0))
        .expect("read SQLite journal mode")
}

pub fn assert_sqlite_integrity(database: &Path) {
    let connection =
        rusqlite::Connection::open_with_flags(database, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
            .expect("open SQLite database read-only");
    let result: String = connection
        .query_row("PRAGMA integrity_check", [], |row| row.get(0))
        .expect("run SQLite integrity_check");
    assert_eq!(result, "ok", "SQLite integrity failure");
}

pub fn write_optional_evidence(variable: &str, evidence: &Value) {
    let Some(path) = std::env::var_os(variable).map(PathBuf::from) else {
        return;
    };
    let parent = path.parent().expect("evidence path has a parent");
    fs::create_dir_all(parent).expect("create evidence directory");
    let temporary = parent.join(format!(
        ".{}.{}.tmp",
        path.file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("upgrade-evidence"),
        std::process::id()
    ));
    let mut bytes = serde_json::to_vec_pretty(evidence).expect("serialize upgrade evidence");
    bytes.push(b'\n');
    fs::write(&temporary, bytes).expect("write temporary upgrade evidence");
    fs::rename(temporary, path).expect("atomically publish upgrade evidence");
}

pub fn text(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned()
}

fn output_text(output: &Output) -> String {
    format!(
        "status: {}\nstdout:\n{}\nstderr:\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

fn read_all(mut reader: impl Read) -> Vec<u8> {
    let mut bytes = Vec::new();
    let _ = reader.read_to_end(&mut bytes);
    bytes
}
