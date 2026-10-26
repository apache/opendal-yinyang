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

//! Isolated real-process fixture for Managed Sync acceptance.
//!
//! The fixture deliberately knows nothing about the eventual Sync CLI. It owns
//! storage/service lifetime and observes remote state through a fresh core
//! authority on every check, never through the adapter under test.

#![allow(dead_code)]

use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io::{BufRead, BufReader, Read};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::Arc;
use std::sync::mpsc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};
use yinyang::core::service::ServiceClient;
use yinyang::core::{
    Authority, BackendProfile, CommitOutcome, Error, ErrorKind, Fs, NodeKind, Planner, Receipt,
    Result as CoreResult, Snapshot,
};

const STORAGE_VARIABLES: [&str; 5] = [
    "YINYANG_S3_BUCKET",
    "YINYANG_S3_ENDPOINT",
    "YINYANG_S3_REGION",
    "YINYANG_S3_ACCESS_KEY_ID",
    "YINYANG_S3_SECRET_ACCESS_KEY",
];

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

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RemoteEntry {
    Directory { executable: bool },
    File { executable: bool, bytes: Vec<u8> },
}

pub type Tree = BTreeMap<String, RemoteEntry>;

#[derive(Debug)]
pub struct Captured {
    pub status: ExitStatus,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
}

impl Captured {
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

pub fn require_acceptance() {
    for name in STORAGE_VARIABLES {
        if std::env::var_os(name).is_none() {
            panic!("{name} is unset; Managed Sync acceptance fails instead of skipping");
        }
    }
}

pub struct Fixture {
    pub temp: tempfile::TempDir,
    pub publication: Publication,
    root: String,
    token: String,
    service: Option<ServiceProcess>,
    last_service_address: Option<SocketAddr>,
}

#[derive(Clone, Debug)]
pub struct SyncPaths {
    pub config: PathBuf,
    pub directory: PathBuf,
    pub state: PathBuf,
}

impl Fixture {
    pub fn new(publication: Publication) -> Self {
        require_acceptance();
        let temp = tempfile::tempdir().expect("create isolated acceptance directory");
        let root = format!(
            "managed-sync-{}-{}",
            std::process::id(),
            uuid::Uuid::new_v4().simple()
        );
        let mut fixture = Self {
            temp,
            publication,
            root,
            token: "managed-sync-acceptance-token-000000000000".to_owned(),
            service: None,
            last_service_address: None,
        };
        match publication {
            Publication::Object => {
                fixture.yy_ok(&["create"]);
            }
            Publication::Service => fixture.spawn_service(),
        };
        fixture
    }

    pub fn path(&self, name: impl AsRef<Path>) -> PathBuf {
        self.temp.path().join(name)
    }

    fn storage_env(&self) -> Vec<(&'static str, String)> {
        let mut env = Vec::with_capacity(STORAGE_VARIABLES.len() + 3);
        for name in STORAGE_VARIABLES {
            let value = std::env::var(name).unwrap_or_else(|_| {
                panic!("{name} disappeared after acceptance preflight; refusing to skip")
            });
            env.push((name, value));
        }
        env.push(("YINYANG_STORAGE_PROFILE", "minio".to_owned()));
        env.push(("YINYANG_S3_ROOT", self.root.clone()));
        env.push(("YINYANG_SERVICE_TOKEN", self.token.clone()));
        env
    }

    pub fn yy_command(&self, args: &[&str]) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_yy"));
        command.args(args);
        for (name, value) in self.storage_env() {
            command.env(name, value);
        }
        command
    }

    pub fn yy(&self, args: &[&str]) -> Captured {
        run_timeout(self.yy_command(args), Duration::from_secs(180))
    }

    pub fn yy_ok(&self, args: &[&str]) -> Captured {
        let output = self.yy(args);
        output.success_or(&format!("yy {args:?}"));
        output
    }

    pub fn sync_paths(&self, name: &str) -> SyncPaths {
        let directory = self.path(format!("{name}-root"));
        let state = self.path(format!("{name}-state"));
        let config = self.path(format!("{name}.json"));
        let mut value = serde_json::json!({
            "name": name,
            "model": "managed",
            "access": "sync",
            "frontend": "library",
            "publication": self.publication.as_str(),
            "storage_profile": "minio",
            "staging": state,
            "read_only": false,
            "require": [],
        });
        if self.publication == Publication::Service {
            value["service_address"] =
                serde_json::Value::String(self.service_address().to_string());
        }
        fs::write(
            &config,
            serde_json::to_vec_pretty(&value).expect("serialize sync volume config"),
        )
        .unwrap_or_else(|error| panic!("write sync config {}: {error}", config.display()));
        SyncPaths {
            config,
            directory,
            state,
        }
    }

    pub fn sync_client(&self, name: &str) -> SyncPaths {
        let paths = self.sync_paths(name);
        fs::create_dir(&paths.directory).unwrap_or_else(|error| {
            panic!("create local root {}: {error}", paths.directory.display())
        });
        self.sync_init(&paths);
        paths
    }

    pub fn sync_init(&self, paths: &SyncPaths) {
        self.yy_ok(&[
            "--volume",
            text(&paths.config),
            "sync",
            "init",
            "--directory",
            text(&paths.directory),
        ]);
    }

    pub fn sync_run(&self, paths: &SyncPaths) -> Captured {
        self.yy(&["--volume", text(&paths.config), "sync", "run"])
    }

    pub fn sync_run_ok(&self, paths: &SyncPaths) -> Captured {
        let output = self.sync_run(paths);
        output.success_or(&format!("yy sync run for {}", paths.directory.display()));
        output
    }

    pub fn sync_status(&self, paths: &SyncPaths) -> serde_json::Value {
        let output = self.yy_ok(&["--volume", text(&paths.config), "sync", "status", "--json"]);
        serde_json::from_slice(&output.stdout).unwrap_or_else(|error| {
            panic!(
                "parse sync status JSON for {}: {error}\nstdout:\n{}",
                paths.directory.display(),
                String::from_utf8_lossy(&output.stdout)
            )
        })
    }

    pub fn sync_export(&self, paths: &SyncPaths, conflict: &str, side: &str, output: &Path) {
        self.yy_ok(&[
            "--volume",
            text(&paths.config),
            "sync",
            "export",
            conflict,
            "--side",
            side,
            "--output",
            text(output),
        ]);
    }

    pub fn sync_resolve_keep(&self, paths: &SyncPaths, conflict: &str, side: &str) {
        self.yy_ok(&[
            "--volume",
            text(&paths.config),
            "sync",
            "resolve",
            conflict,
            "--keep",
            side,
        ]);
    }

    pub fn sync_resolve_content(&self, paths: &SyncPaths, conflict: &str, content: &Path) {
        self.yy_ok(&[
            "--volume",
            text(&paths.config),
            "sync",
            "resolve",
            conflict,
            "--content",
            text(content),
        ]);
    }

    fn spawn_service(&mut self) {
        let address = self
            .last_service_address
            .unwrap_or_else(|| "127.0.0.1:0".parse().expect("loopback wildcard"));
        let database = self.path(format!("metadata-{}.db", uuid::Uuid::new_v4().simple()));
        let mut command = self.yy_command(&[
            "serve",
            "--database",
            database.to_str().expect("UTF-8 metadata database"),
            "--listen",
            &address.to_string(),
        ]);
        command.stdout(Stdio::piped()).stderr(Stdio::piped());
        let mut child = command
            .spawn()
            .unwrap_or_else(|error| panic!("spawn yy serve: {error}"));
        let stdout = child.stdout.take().expect("service stdout is piped");
        let stderr = child.stderr.take().expect("service stderr is piped");
        let (sender, receiver) = mpsc::channel();
        let stdout_thread = thread::spawn(move || {
            let mut reader = BufReader::new(stdout);
            let mut line = String::new();
            let read = reader.read_line(&mut line);
            let sent = read.is_ok();
            let _ = sender.send((sent, line));
            let mut rest = Vec::new();
            let _ = reader.read_to_end(&mut rest);
            rest
        });
        let stderr_thread = thread::spawn(move || read_all(stderr));
        let (sent, line) = match receiver.recv_timeout(Duration::from_secs(60)) {
            Ok(value) => value,
            Err(error) => {
                let _ = child.kill();
                let _ = child.wait();
                let _ = stdout_thread.join();
                let stderr = stderr_thread.join().unwrap_or_default();
                panic!(
                    "yy serve did not report readiness ({error:?}):\n{}",
                    String::from_utf8_lossy(&stderr)
                );
            }
        };
        if !sent {
            let _ = child.kill();
            let _ = child.wait();
            let _ = stdout_thread.join();
            let stderr = stderr_thread.join().unwrap_or_default();
            panic!(
                "yy serve closed stdout before reporting readiness:\n{}",
                String::from_utf8_lossy(&stderr)
            );
        }
        let address = match line
            .trim()
            .strip_prefix("metadata service listening on ")
            .and_then(|value| value.parse().ok())
        {
            Some(address) => address,
            None => {
                let _ = child.kill();
                let _ = child.wait();
                let _ = stdout_thread.join();
                let stderr = stderr_thread.join().unwrap_or_default();
                panic!(
                    "unexpected yy serve startup line {line:?}:\n{}",
                    String::from_utf8_lossy(&stderr)
                );
            }
        };
        self.last_service_address = Some(address);
        self.service = Some(ServiceProcess {
            child,
            address,
            stdout: Some(stdout_thread),
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
        assert_eq!(self.publication, Publication::Service);
        self.stop_service();
        self.spawn_service();
    }

    pub fn service_address(&self) -> SocketAddr {
        self.service
            .as_ref()
            .map(|service| service.address)
            .or(self.last_service_address)
            .expect("service publication needs a loopback metadata address")
    }

    pub fn remote_tree(&self) -> CoreResult<Tree> {
        let authority = self.remote_authority();
        runtime().block_on(async {
            let snapshot = authority.observe_latest().await?;
            tree(&snapshot).await
        })
    }

    pub fn wait_remote_tree(&self, expected: &Tree, timeout: Duration) -> Tree {
        let deadline = Instant::now() + timeout;
        loop {
            match self.remote_tree() {
                Ok(actual) if &actual == expected => return actual,
                Ok(actual) if Instant::now() < deadline => {
                    let _ = actual;
                    thread::sleep(Duration::from_millis(100));
                }
                Ok(actual) => {
                    panic!(
                        "remote state did not converge within {timeout:?}\nexpected:\n{expected:#?}\nactual:\n{actual:#?}"
                    );
                }
                Err(error) if Instant::now() < deadline => {
                    let _ = error;
                    thread::sleep(Duration::from_millis(100));
                }
                Err(error) => panic!("observe remote state within {timeout:?}: {error}"),
            }
        }
    }

    pub fn remote_create_directory(&self, path: &str) -> Receipt {
        let authority = self.remote_authority();
        runtime()
            .block_on(async {
                let snapshot = authority.observe_latest().await?;
                let (parent, name) = destination(&snapshot, path).await?;
                let mut plan = Planner::new(&snapshot, yinyang::core::CommitId::generate());
                plan.create_directory(parent, &name).await?;
                commit(&authority, plan).await
            })
            .unwrap_or_else(|error| panic!("create remote directory {path}: {error}"))
    }

    pub fn remote_create_file(&self, path: &str, mut bytes: &[u8], executable: bool) -> Receipt {
        let authority = self.remote_authority();
        runtime()
            .block_on(async {
                let snapshot = authority.observe_latest().await?;
                let (parent, name) = destination(&snapshot, path).await?;
                let prepared = authority.prepare(&mut bytes).await?;
                let mut plan = Planner::new(&snapshot, yinyang::core::CommitId::generate());
                plan.create_file(parent, &name, prepared, executable)
                    .await?;
                commit(&authority, plan).await
            })
            .unwrap_or_else(|error| panic!("create remote file {path}: {error}"))
    }

    pub fn remote_write_file(&self, path: &str, mut bytes: &[u8]) -> Receipt {
        let authority = self.remote_authority();
        runtime()
            .block_on(async {
                let snapshot = authority.observe_latest().await?;
                let node = snapshot
                    .resolve(path)
                    .await?
                    .ok_or_else(|| invalid(format!("remote file {path} is missing")))?;
                if !matches!(node.kind(), NodeKind::File(_)) {
                    return Err(invalid(format!("remote path {path} is not a file")));
                }
                let prepared = authority.prepare(&mut bytes).await?;
                let mut plan = Planner::new(&snapshot, yinyang::core::CommitId::generate());
                plan.set_content(node.id(), prepared).await?;
                commit(&authority, plan).await
            })
            .unwrap_or_else(|error| panic!("write remote file {path}: {error}"))
    }

    pub fn remote_rename(&self, source: &str, target: &str) -> Receipt {
        let authority = self.remote_authority();
        runtime()
            .block_on(async {
                let snapshot = authority.observe_latest().await?;
                let node = snapshot
                    .resolve(source)
                    .await?
                    .ok_or_else(|| invalid(format!("remote source {source} is missing")))?;
                let (parent, name) = destination(&snapshot, target).await?;
                let mut plan = Planner::new(&snapshot, yinyang::core::CommitId::generate());
                plan.rename(node.id(), parent, &name).await?;
                commit(&authority, plan).await
            })
            .unwrap_or_else(|error| panic!("rename remote {source} to {target}: {error}"))
    }

    pub fn remote_remove(&self, path: &str) -> Receipt {
        let authority = self.remote_authority();
        runtime()
            .block_on(async {
                let snapshot = authority.observe_latest().await?;
                let node = snapshot
                    .resolve(path)
                    .await?
                    .ok_or_else(|| invalid(format!("remote path {path} is missing")))?;
                let mut plan = Planner::new(&snapshot, yinyang::core::CommitId::generate());
                plan.remove(node.id()).await?;
                commit(&authority, plan).await
            })
            .unwrap_or_else(|error| panic!("remove remote path {path}: {error}"))
    }

    fn remote_authority(&self) -> Arc<dyn Authority> {
        let operator = self.operator();
        runtime().block_on(async {
            match self.publication {
                Publication::Object => Arc::new(
                    Fs::open(operator, BackendProfile::Minio)
                        .await
                        .expect("open independent object authority"),
                ) as Arc<dyn Authority>,
                Publication::Service => Arc::new(
                    ServiceClient::connect(self.service_address(), self.token.clone(), operator)
                        .await
                        .expect("connect independent service authority"),
                ) as Arc<dyn Authority>,
            }
        })
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
        opendal::Operator::via_iter("s3", config).expect("construct independent S3 operator")
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.stop_service();
    }
}

struct ServiceProcess {
    child: Child,
    address: SocketAddr,
    stdout: Option<JoinHandle<Vec<u8>>>,
    stderr: Option<JoinHandle<Vec<u8>>>,
}

impl ServiceProcess {
    fn stop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        if let Some(thread) = self.stdout.take() {
            let _ = thread.join();
        }
        if let Some(thread) = self.stderr.take() {
            let _ = thread.join();
        }
    }
}

impl Drop for ServiceProcess {
    fn drop(&mut self) {
        self.stop();
    }
}

pub fn local_tree(root: &Path) -> std::io::Result<Tree> {
    let root_metadata = fs::symlink_metadata(root)?;
    if !root_metadata.is_dir() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::NotADirectory,
            format!("{} is not a directory", root.display()),
        ));
    }
    let mut tree = Tree::new();
    let mut pending = vec![(root.to_path_buf(), String::new())];
    while let Some((directory, relative)) = pending.pop() {
        for entry in fs::read_dir(&directory)? {
            let entry = entry?;
            let name = entry.file_name().into_string().map_err(|_| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("non-UTF-8 name under {}", directory.display()),
                )
            })?;
            let path = join(&relative, &name);
            let source = entry.path();
            let metadata = fs::symlink_metadata(&source)?;
            let executable = metadata.is_file() && {
                use std::os::unix::fs::PermissionsExt as _;
                metadata.permissions().mode() & 0o111 != 0
            };
            if metadata.is_dir() {
                tree.insert(path.clone(), RemoteEntry::Directory { executable });
                pending.push((source, path));
            } else if metadata.is_file() {
                tree.insert(
                    path,
                    RemoteEntry::File {
                        executable,
                        bytes: fs::read(source)?,
                    },
                );
            } else {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("{} is a symlink or special file", source.display()),
                ));
            }
        }
    }
    Ok(tree)
}

pub fn assert_lock_released(path: &Path) {
    let lock = OpenOptions::new()
        .create(false)
        .read(true)
        .write(true)
        .open(path)
        .unwrap_or_else(|error| panic!("open process lease {}: {error}", path.display()));
    lock.try_lock().unwrap_or_else(|error| {
        panic!(
            "process lease {} is still held after command exit: {error}",
            path.display()
        )
    });
}

async fn tree(snapshot: &Snapshot) -> CoreResult<Tree> {
    let mut tree = Tree::new();
    let mut pending = vec![(String::new(), snapshot.root())];
    while let Some((path, id)) = pending.pop() {
        let node = snapshot
            .node(id)
            .await?
            .ok_or_else(|| invalid("remote node disappeared during inspection"))?;
        match node.kind() {
            NodeKind::Directory { .. } => {
                tree.insert(
                    path.clone(),
                    RemoteEntry::Directory {
                        executable: node.executable(),
                    },
                );
                let mut token = None;
                loop {
                    let page = snapshot.scan(id, token.as_ref(), 4096).await?;
                    for entry in page.entries {
                        pending.push((join(&path, &entry.name), entry.node_id));
                    }
                    token = page.next;
                    if token.is_none() {
                        break;
                    }
                }
            }
            NodeKind::File(file) => {
                let mut bytes = Vec::new();
                snapshot
                    .open_file(id)
                    .await?
                    .read_range(0..file.content_id().length(), &mut bytes)
                    .await?;
                if bytes.len() as u64 != file.content_id().length() {
                    return Err(invalid("remote file length changed during inspection"));
                }
                tree.insert(
                    path,
                    RemoteEntry::File {
                        executable: node.executable(),
                        bytes,
                    },
                );
            }
        }
    }
    tree.remove("");
    Ok(tree)
}

async fn destination(
    snapshot: &Snapshot,
    path: &str,
) -> CoreResult<(yinyang::core::NodeId, String)> {
    let (parent, name) = path.rsplit_once('/').unwrap_or(("", path));
    let parent = snapshot
        .resolve(parent)
        .await?
        .ok_or_else(|| invalid(format!("remote parent {parent:?} is missing")))?;
    if !parent.is_directory() {
        return Err(invalid(format!(
            "remote parent {parent:?} is not a directory"
        )));
    }
    if name.is_empty() {
        return Err(invalid("remote destination name is empty"));
    }
    Ok((parent.id(), name.to_owned()))
}

async fn commit(authority: &Arc<dyn Authority>, plan: Planner) -> CoreResult<Receipt> {
    match authority.commit(&plan.finish()?).await? {
        CommitOutcome::Committed(receipt) => Ok(receipt),
        CommitOutcome::Conflict => Err(invalid("independent remote mutation conflicted")),
        CommitOutcome::Retryable => Err(invalid("independent remote mutation was retryable")),
        CommitOutcome::Unknown(_) => Err(invalid(
            "independent remote mutation is unknown and requires receipt resolution",
        )),
    }
}

fn invalid(message: impl Into<String>) -> Error {
    Error::new(ErrorKind::Invalid, "Managed Sync acceptance", message)
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build acceptance runtime")
}

fn join(parent: &str, name: &str) -> String {
    if parent.is_empty() {
        name.to_owned()
    } else {
        format!("{parent}/{name}")
    }
}

fn text(path: &Path) -> &str {
    path.to_str()
        .unwrap_or_else(|| panic!("acceptance path is not UTF-8: {}", path.display()))
}

fn run_timeout(mut command: Command, timeout: Duration) -> Captured {
    command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = command
        .spawn()
        .unwrap_or_else(|error| panic!("spawn process (format: {command:?}): {error}"));
    let stdout = child.stdout.take().expect("stdout is piped");
    let stderr = child.stderr.take().expect("stderr is piped");
    let stdout_thread = thread::spawn(move || read_all(stdout));
    let stderr_thread = thread::spawn(move || read_all(stderr));
    let deadline = Instant::now() + timeout;
    let status = loop {
        match child.try_wait() {
            Ok(Some(status)) => break status,
            Ok(None) if Instant::now() < deadline => thread::sleep(Duration::from_millis(25)),
            Ok(None) => {
                let _ = child.kill();
                let status = child.wait().expect("wait after timeout kill");
                let stdout = stdout_thread.join().unwrap_or_default();
                let stderr = stderr_thread.join().unwrap_or_default();
                panic!(
                    "command timed out after {timeout:?} with {status} (format: {command:?}):\nstdout:\n{}\nstderr:\n{}",
                    String::from_utf8_lossy(&stdout),
                    String::from_utf8_lossy(&stderr)
                );
            }
            Err(error) => {
                let _ = child.kill();
                let _ = child.wait();
                panic!("wait for process (format: {command:?}): {error}");
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

fn read_all(mut reader: impl Read) -> Vec<u8> {
    let mut bytes = Vec::new();
    let _ = reader.read_to_end(&mut bytes);
    bytes
}
