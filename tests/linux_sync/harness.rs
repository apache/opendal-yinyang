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

use serde::Deserialize;
use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};
use yinyang::core::service::ServiceClient;
use yinyang::core::{
    Authority, CommitOutcome, Error, ErrorKind, Fs, NodeKind, Planner, Receipt,
    Result as CoreResult, Snapshot,
};

#[path = "../support/yy.rs"]
mod yy;

const STORAGE_VARIABLES: [&str; 5] = [
    "YINYANG_S3_BUCKET",
    "YINYANG_S3_ENDPOINT",
    "YINYANG_S3_REGION",
    "YINYANG_S3_ACCESS_KEY_ID",
    "YINYANG_S3_SECRET_ACCESS_KEY",
];
const MAX_FRAME: usize = 16 * 1024 * 1024;

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

pub struct RunningSync {
    child: Child,
    stdout: Option<JoinHandle<Vec<u8>>>,
    stderr: Option<JoinHandle<Vec<u8>>>,
}

impl RunningSync {
    pub fn wait(mut self, timeout: Duration) -> Captured {
        let deadline = Instant::now() + timeout;
        let status = loop {
            match self.child.try_wait() {
                Ok(Some(status)) => break status,
                Ok(None) if Instant::now() < deadline => {
                    thread::sleep(Duration::from_millis(25));
                }
                Ok(None) => {
                    let _ = self.child.kill();
                    let _ = self.child.wait();
                    panic!("yy sync run did not exit within {timeout:?}");
                }
                Err(error) => {
                    let _ = self.child.kill();
                    let _ = self.child.wait();
                    panic!("wait for yy sync run: {error}");
                }
            }
        };
        Captured {
            status,
            stdout: self.stdout.take().unwrap().join().unwrap_or_default(),
            stderr: self.stderr.take().unwrap().join().unwrap_or_default(),
        }
    }
}

impl Drop for RunningSync {
    fn drop(&mut self) {
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

pub fn require_acceptance() {
    let amazon_s3 = std::env::var("YINYANG_STORAGE_PROFILE").as_deref() == Ok("amazon-s3");
    for name in STORAGE_VARIABLES {
        if amazon_s3 && name == "YINYANG_S3_ENDPOINT" {
            continue;
        }
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
    storage_endpoint: Option<String>,
    service: Option<ServiceProcess>,
    last_service_address: Option<SocketAddr>,
    service_client_address: Option<SocketAddr>,
    service_database: Option<PathBuf>,
}

#[derive(Clone, Debug)]
pub struct SyncPaths {
    pub config: PathBuf,
    pub directory: PathBuf,
    pub state: PathBuf,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct SyncStatus {
    pub schema: String,
    pub binding: BindingStatus,
    pub remote: RemoteStatus,
    pub local: LocalStatus,
    pub summary: StatusSummary,
    pub entries: Vec<SyncEntry>,
    pub conflicts: Vec<ConflictRecord>,
    pub artifacts: Vec<ArtifactRecord>,
}

impl SyncStatus {
    pub fn validate(&self) {
        assert_eq!(self.schema, "yinyang.sync.status.v1");
        assert!(
            self.entries
                .windows(2)
                .all(|pair| pair[0].path < pair[1].path),
            "sync status entries must be strictly sorted by path"
        );
        assert_eq!(
            self.summary.conflicts,
            self.conflicts.len(),
            "summary.conflicts must equal the top-level conflict count"
        );
        assert_eq!(
            self.summary.retained_artifacts,
            self.artifacts.len() as u64,
            "summary.retained_artifacts must equal the top-level artifact count"
        );
        if self.summary.in_sync {
            assert_eq!(
                self.remote.availability,
                RemoteAvailability::Available,
                "in_sync requires an observed remote"
            );
            assert_eq!(self.summary.pending, 0);
            assert_eq!(self.summary.unknown, 0);
            assert_eq!(self.summary.conflicts, 0);
            assert_eq!(self.summary.failed, 0);
        }
        for conflict in &self.conflicts {
            assert_eq!(conflict.id.len(), 32, "conflict id must be 16-byte hex");
            if let Some(revision) = &conflict.remote_revision {
                assert_eq!(
                    revision.len(),
                    48,
                    "conflict remote revision must be 24-byte hex"
                );
            }
            let matches = self
                .entries
                .iter()
                .filter(|entry| entry.conflict.as_deref() == Some(conflict.id.as_str()))
                .collect::<Vec<_>>();
            assert_eq!(
                matches.len(),
                1,
                "conflict {} must reference exactly one entry",
                conflict.id
            );
            let entry = matches[0];
            assert_eq!(
                entry.state,
                EntryState::Conflict,
                "conflict {} entry must have state conflict",
                conflict.id
            );
            assert_eq!(entry.original_path, conflict.original_path);
            assert_eq!(entry.desired_path, conflict.desired_path);
            match &conflict.original_path {
                Some(original) => assert_eq!(conflict.path, *original),
                None => assert_eq!(
                    conflict.desired_path.as_deref(),
                    Some(conflict.path.as_str())
                ),
            }
        }
    }

    pub fn conflict(&self, path: &str) -> &ConflictRecord {
        self.conflicts
            .iter()
            .find(|conflict| conflict.path == path)
            .unwrap_or_else(|| panic!("sync status has no conflict for {path}: {self:#?}"))
    }

    pub fn entry(&self, path: &str) -> &SyncEntry {
        self.entries
            .iter()
            .find(|entry| entry.path == path)
            .unwrap_or_else(|| panic!("sync status has no entry for {path}: {self:#?}"))
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct BindingStatus {
    pub state: BindingState,
    pub state_directory: String,
    pub root: Option<String>,
    pub namespace: Option<String>,
    pub filesystem: Option<String>,
    pub same_filesystem: Option<bool>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum BindingState {
    Unbound,
    Bound,
    Error,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct RemoteStatus {
    pub availability: RemoteAvailability,
    pub cursor: Option<String>,
    pub revision: Option<String>,
    pub error: Option<String>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum RemoteAvailability {
    NotChecked,
    Available,
    Unavailable,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct LocalStatus {
    pub scan: LocalScanState,
    pub error: Option<String>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum LocalScanState {
    NotScanned,
    Clean,
    Changed,
    Failed,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct StatusSummary {
    pub in_sync: bool,
    pub pending: u64,
    pub unknown: u64,
    pub conflicts: usize,
    pub failed: u64,
    pub retained_artifacts: u64,
    pub retained_bytes: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct SyncEntry {
    pub path: String,
    pub original_path: Option<String>,
    pub desired_path: Option<String>,
    pub state: EntryState,
    pub local: LocalEvidence,
    pub remote: RemoteEvidence,
    pub baseline: BaselineEvidence,
    pub intent: IntentStatus,
    pub outcome: OutcomeStatus,
    pub conflict: Option<String>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum EntryState {
    InSync,
    LocalPending,
    RemotePending,
    Conflict,
    Unknown,
    Failed,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct LocalEvidence {
    pub kind: Option<EntryKind>,
    pub length: Option<u64>,
    pub digest: Option<String>,
    pub executable: Option<bool>,
    pub presence: Presence,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct RemoteEvidence {
    pub kind: Option<EntryKind>,
    pub length: Option<u64>,
    pub digest: Option<String>,
    pub executable: Option<bool>,
    pub presence: Presence,
    pub identity: Option<String>,
    pub pin: Option<String>,
    pub node: Option<String>,
    pub revision: Option<String>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum EntryKind {
    File,
    Directory,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum Presence {
    Present,
    Absent,
    Unknown,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct BaselineEvidence {
    pub local_digest: Option<String>,
    pub local_length: Option<u64>,
    pub remote_identity: Option<String>,
    pub remote_pin: Option<String>,
    pub node: Option<String>,
    pub revision: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct IntentStatus {
    pub id: Option<String>,
    pub operation: Option<IntentOperation>,
    pub state: Option<IntentState>,
    pub commit_id: Option<String>,
    pub receipt_revision: Option<String>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
pub enum IntentOperation {
    #[serde(rename = "content")]
    Content,
    #[serde(rename = "create-file")]
    CreateFile,
    #[serde(rename = "create-directory")]
    CreateDirectory,
    #[serde(rename = "remove")]
    Remove,
    #[serde(rename = "rename")]
    Rename,
    #[serde(rename = "set-executable")]
    SetExecutable,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum IntentState {
    Prepared,
    Unknown,
    Committed,
    Conflict,
    Retryable,
    Failed,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct OutcomeStatus {
    pub state: OutcomeState,
    pub error: Option<String>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum OutcomeState {
    Pending,
    RemoteCompleted,
    Conflict,
    Unknown,
    Retryable,
    Failed,
    None,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct ConflictRecord {
    pub id: String,
    pub path: String,
    pub original_path: Option<String>,
    pub desired_path: Option<String>,
    pub local: ConflictSide,
    pub remote: ConflictSide,
    pub local_artifact: Option<String>,
    pub remote_artifact: Option<String>,
    pub remote_revision: Option<String>,
    pub error: String,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ConflictSide {
    CurrentFile,
    CurrentDirectory,
    RetainedArtifact,
    Deleted,
    Unknown,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct ArtifactRecord {
    pub id: String,
    pub origin: String,
    pub kind: ArtifactKind,
    pub phase: ArtifactPhase,
    pub path: String,
    pub exportable: bool,
    pub error: Option<String>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
pub enum ArtifactKind {
    #[serde(rename = "replaced-file")]
    ReplacedFile,
    #[serde(rename = "rollback-displaced")]
    RollbackDisplaced,
    #[serde(rename = "quarantined-file")]
    QuarantinedFile,
    #[serde(rename = "quarantined-directory")]
    QuarantinedDirectory,
    #[serde(rename = "upload-retained")]
    UploadRetained,
    #[serde(rename = "conflict-local")]
    ConflictLocal,
    #[serde(rename = "conflict-remote")]
    ConflictRemote,
    #[serde(rename = "resolution")]
    Resolution,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactPhase {
    Preparing,
    Prepared,
    Moved,
    Retained,
    Conflict,
    Failed,
}

impl Fixture {
    pub fn new(publication: Publication) -> Self {
        Self::new_with_storage_endpoint(publication, None)
    }

    /// Create a fixture whose storage traffic goes through an explicit
    /// endpoint. Benchmarks use this to measure the product path through a
    /// counting proxy without changing the application.
    pub fn new_with_storage_endpoint(
        publication: Publication,
        storage_endpoint: Option<String>,
    ) -> Self {
        require_acceptance();
        let temp = tempfile::tempdir().expect("create isolated acceptance directory");
        let isolated = format!(
            "managed-sync-{}-{}",
            std::process::id(),
            uuid::Uuid::new_v4().simple()
        );
        let root = match std::env::var("YINYANG_S3_ROOT") {
            Ok(prefix) if !prefix.trim_matches('/').is_empty() => {
                format!("{}/{}", prefix.trim_matches('/'), isolated)
            }
            _ => isolated,
        };
        let mut fixture = Self {
            temp,
            publication,
            root,
            token: "managed-sync-acceptance-token-000000000000".to_owned(),
            storage_endpoint,
            service: None,
            last_service_address: None,
            service_client_address: None,
            service_database: None,
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
        let mut env = Vec::with_capacity(STORAGE_VARIABLES.len() + 4);
        let profile =
            std::env::var("YINYANG_STORAGE_PROFILE").unwrap_or_else(|_| "minio".to_owned());
        for name in STORAGE_VARIABLES {
            if profile == "amazon-s3"
                && name == "YINYANG_S3_ENDPOINT"
                && self.storage_endpoint.is_none()
                && std::env::var_os(name).is_none()
            {
                continue;
            }
            let value = if name == "YINYANG_S3_ENDPOINT" {
                self.storage_endpoint
                    .clone()
                    .or_else(|| std::env::var(name).ok())
            } else {
                std::env::var(name).ok()
            }
            .unwrap_or_else(|| {
                panic!("{name} disappeared after acceptance preflight; refusing to skip")
            });
            env.push((name, value));
        }
        env.push(("YINYANG_STORAGE_PROFILE", profile));
        env.push(("YINYANG_S3_ROOT", self.root.clone()));
        env.push(("YINYANG_SERVICE_TOKEN", self.token.clone()));
        if let Ok(token) = std::env::var("YINYANG_S3_SESSION_TOKEN") {
            env.push(("YINYANG_S3_SESSION_TOKEN", token));
        }
        env
    }

    pub fn yy_command(&self, args: &[&str]) -> Command {
        let mut command = Command::new(yy::binary());
        command.args(args);
        for (name, value) in self.storage_env() {
            command.env(name, value);
        }
        command
    }

    pub fn yy(&self, args: &[&str]) -> Captured {
        run_timeout(self.yy_command(args), Duration::from_secs(180))
    }

    pub fn yy_with_env(&self, args: &[&str], overrides: &[(&str, &str)]) -> Captured {
        let mut command = self.yy_command(args);
        for (name, value) in overrides {
            command.env(name, value);
        }
        run_timeout(command, Duration::from_secs(180))
    }

    pub fn yy_ok(&self, args: &[&str]) -> Captured {
        let output = self.yy(args);
        output.success_or(&format!("yy {args:?}"));
        output
    }

    pub fn sync_paths(&self, name: &str) -> SyncPaths {
        self.sync_paths_at(name, None)
    }

    pub fn sync_paths_at(&self, name: &str, service_address: Option<SocketAddr>) -> SyncPaths {
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
        });
        if self.publication == Publication::Service {
            value["service_address"] = serde_json::Value::String(
                service_address
                    .unwrap_or_else(|| self.service_client_address())
                    .to_string(),
            );
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

    pub fn sync_run_with_env(&self, paths: &SyncPaths, overrides: &[(&str, &str)]) -> Captured {
        self.yy_with_env(&["--volume", text(&paths.config), "sync", "run"], overrides)
    }

    pub fn spawn_yy(&self, args: &[&str]) -> RunningSync {
        let mut command = self.yy_command(args);
        command
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut child = command
            .spawn()
            .unwrap_or_else(|error| panic!("spawn yy sync run: {error}"));
        let stdout = child.stdout.take().expect("sync stdout is piped");
        let stderr = child.stderr.take().expect("sync stderr is piped");
        RunningSync {
            child,
            stdout: Some(thread::spawn(move || read_all(stdout))),
            stderr: Some(thread::spawn(move || read_all(stderr))),
        }
    }

    pub fn spawn_sync_run(&self, paths: &SyncPaths) -> RunningSync {
        self.spawn_yy(&["--volume", text(&paths.config), "sync", "run"])
    }

    pub fn sync_run_ok(&self, paths: &SyncPaths) -> Captured {
        let output = self.sync_run(paths);
        output.success_or(&format!("yy sync run for {}", paths.directory.display()));
        output
    }

    pub fn sync_status(&self, paths: &SyncPaths) -> SyncStatus {
        let output = self.yy_ok(&["--volume", text(&paths.config), "sync", "status", "--json"]);
        parse_status(paths, &output.stdout)
    }

    pub fn sync_status_any_exit(&self, paths: &SyncPaths) -> (i32, SyncStatus) {
        let output = self.yy(&["--volume", text(&paths.config), "sync", "status", "--json"]);
        let status = parse_status(paths, &output.stdout);
        (
            output
                .status
                .code()
                .expect("sync status terminated by signal"),
            status,
        )
    }

    pub fn sync_export_conflict(
        &self,
        paths: &SyncPaths,
        conflict: &str,
        side: &str,
        output: &Path,
    ) {
        self.yy_ok(&[
            "--volume",
            text(&paths.config),
            "sync",
            "export",
            "--conflict",
            conflict,
            "--side",
            side,
            "--output",
            text(output),
        ]);
    }

    pub fn sync_export_conflict_output(
        &self,
        paths: &SyncPaths,
        conflict: &str,
        side: &str,
        output: &Path,
    ) -> Captured {
        self.yy(&[
            "--volume",
            text(&paths.config),
            "sync",
            "export",
            "--conflict",
            conflict,
            "--side",
            side,
            "--output",
            text(output),
        ])
    }

    pub fn sync_export_artifact(&self, paths: &SyncPaths, artifact: &str, output: &Path) {
        self.yy_ok(&[
            "--volume",
            text(&paths.config),
            "sync",
            "export",
            "--artifact",
            artifact,
            "--output",
            text(output),
        ]);
    }

    pub fn sync_export_artifact_output(
        &self,
        paths: &SyncPaths,
        artifact: &str,
        output: &Path,
    ) -> Captured {
        self.yy(&[
            "--volume",
            text(&paths.config),
            "sync",
            "export",
            "--artifact",
            artifact,
            "--output",
            text(output),
        ])
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

    pub fn spawn_sync_resolve_content(
        &self,
        paths: &SyncPaths,
        conflict: &str,
        content: &Path,
    ) -> RunningSync {
        self.spawn_yy(&[
            "--volume",
            text(&paths.config),
            "sync",
            "resolve",
            conflict,
            "--content",
            text(content),
        ])
    }

    fn spawn_service(&mut self) {
        let address = self
            .last_service_address
            .unwrap_or_else(|| "127.0.0.1:0".parse().expect("loopback wildcard"));
        if self.service_database.is_none() {
            self.service_database = Some(self.path("metadata.db"));
        }
        let database = self
            .service_database
            .clone()
            .expect("service database path initialized");
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

    pub fn set_service_client_address(&mut self, address: SocketAddr) {
        assert_eq!(self.publication, Publication::Service);
        self.service_client_address = Some(address);
    }

    pub fn service_client_address(&self) -> SocketAddr {
        self.service_client_address
            .unwrap_or_else(|| self.service_address())
    }

    pub fn service_gate(&self, drop_commit_response: bool) -> ServiceGate {
        assert_eq!(
            self.publication,
            Publication::Service,
            "service races require service publication"
        );
        ServiceGate::new(
            self.service_address(),
            self.token.clone(),
            drop_commit_response,
        )
    }

    pub fn service_gate_drop_after_release(&self) -> ServiceGate {
        assert_eq!(
            self.publication,
            Publication::Service,
            "service races require service publication"
        );
        ServiceGate::new_drop_after_release(self.service_address(), self.token.clone())
    }

    pub fn remote_tree(&self) -> CoreResult<Tree> {
        let authority = self.remote_authority();
        runtime().block_on(async {
            let snapshot = authority.observe_latest().await?;
            tree(&snapshot).await
        })
    }

    pub fn remote_file_version(
        &self,
        path: &str,
    ) -> (yinyang::core::NodeId, u64, yinyang::core::Revision) {
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
                Ok((node.id(), node.generation(), snapshot.revision()))
            })
            .unwrap_or_else(|error| panic!("observe remote version {path}: {error}"))
    }

    pub fn wait_remote_tree(&self, expected: &Tree, timeout: Duration) -> Tree {
        let deadline = Instant::now() + timeout;
        loop {
            match self.remote_tree() {
                Ok(actual) if &actual == expected => return actual,
                Ok(_) if Instant::now() < deadline => {
                    thread::sleep(Duration::from_millis(100));
                }
                Ok(actual) => {
                    panic!(
                        "remote state did not converge within {timeout:?}\nexpected:\n{expected:#?}\nactual:\n{actual:#?}"
                    );
                }
                Err(_) if Instant::now() < deadline => {
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

    pub fn remote_remove_tree(&self, path: &str) -> Receipt {
        let authority = self.remote_authority();
        runtime()
            .block_on(async {
                let snapshot = authority.observe_latest().await?;
                let root = snapshot
                    .resolve(path)
                    .await?
                    .ok_or_else(|| invalid(format!("remote path {path} is missing")))?;
                let mut pending = vec![root.id()];
                let mut nodes = Vec::new();
                while let Some(id) = pending.pop() {
                    let node = snapshot
                        .node(id)
                        .await?
                        .ok_or_else(|| invalid("remote subtree node disappeared"))?;
                    if node.is_directory() {
                        let mut token = None;
                        loop {
                            let page = snapshot.scan(id, token.as_ref(), 4096).await?;
                            for entry in page.entries {
                                pending.push(entry.node_id);
                            }
                            token = page.next;
                            if token.is_none() {
                                break;
                            }
                        }
                    }
                    nodes.push(id);
                }
                let mut plan = Planner::new(&snapshot, yinyang::core::CommitId::generate());
                for id in nodes.into_iter().rev() {
                    plan.remove(id).await?;
                }
                commit(&authority, plan).await
            })
            .unwrap_or_else(|error| panic!("remove remote subtree {path}: {error}"))
    }

    fn remote_authority(&self) -> Arc<dyn Authority> {
        let operator = self.operator();
        runtime().block_on(async {
            match self.publication {
                Publication::Object => Arc::new(
                    Fs::open(operator)
                        .await
                        .expect("open independent object authority"),
                ) as Arc<dyn Authority>,
                Publication::Service => Arc::new(
                    ServiceClient::connect(
                        self.service_client_address(),
                        self.token.clone(),
                        operator,
                    )
                    .await
                    .expect("connect independent service authority"),
                ) as Arc<dyn Authority>,
            }
        })
    }

    pub fn operator(&self) -> opendal::Operator {
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

fn parse_status(paths: &SyncPaths, stdout: &[u8]) -> SyncStatus {
    let status: SyncStatus = serde_json::from_slice(stdout).unwrap_or_else(|error| {
        panic!(
            "parse sync status JSON for {}: {error}\nstdout:\n{}",
            paths.directory.display(),
            String::from_utf8_lossy(stdout)
        )
    });
    status.validate();
    status
}

pub struct ServiceGate {
    address: SocketAddr,
    release: Arc<(Mutex<bool>, Condvar)>,
    ready: mpsc::Receiver<()>,
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

#[derive(Clone, Copy)]
enum CommitAction {
    ForwardAfterRelease,
    DropImmediately,
    DropAfterRelease,
}

impl ServiceGate {
    pub fn new(upstream: SocketAddr, token: String, drop_commit_response: bool) -> Self {
        Self::new_with_action(
            upstream,
            token,
            if drop_commit_response {
                CommitAction::DropImmediately
            } else {
                CommitAction::ForwardAfterRelease
            },
        )
    }

    pub fn new_drop_after_release(upstream: SocketAddr, token: String) -> Self {
        Self::new_with_action(upstream, token, CommitAction::DropAfterRelease)
    }

    fn new_with_action(upstream: SocketAddr, token: String, action: CommitAction) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind Managed Sync service gate");
        let address = listener
            .local_addr()
            .expect("Managed Sync service gate address");
        let release = Arc::new((Mutex::new(false), Condvar::new()));
        let ready = Arc::new(AtomicBool::new(false));
        let intercepted = Arc::new(AtomicBool::new(false));
        let stop = Arc::new(AtomicBool::new(false));
        let (sender, receiver) = mpsc::channel();
        let thread_release = release.clone();
        let thread_ready = ready.clone();
        let thread_intercepted = intercepted.clone();
        let thread_stop = stop.clone();
        let thread = thread::spawn(move || {
            for incoming in listener.incoming() {
                if thread_stop.load(Ordering::SeqCst) {
                    break;
                }
                let Ok(client) = incoming else {
                    break;
                };
                let release = thread_release.clone();
                let ready = thread_ready.clone();
                let intercepted = thread_intercepted.clone();
                let sender = sender.clone();
                let token = token.clone();
                thread::spawn(move || {
                    handle_service_connection(
                        client,
                        upstream,
                        &token,
                        action,
                        release,
                        ready,
                        intercepted,
                        sender,
                    );
                });
            }
        });
        Self {
            address,
            release,
            ready: receiver,
            stop,
            thread: Some(thread),
        }
    }

    pub fn address(&self) -> SocketAddr {
        self.address
    }

    pub fn wait_for_commit_response(
        &self,
        timeout: Duration,
    ) -> Result<(), mpsc::RecvTimeoutError> {
        self.ready.recv_timeout(timeout)
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

fn handle_service_connection(
    mut client: TcpStream,
    upstream: SocketAddr,
    token: &str,
    action: CommitAction,
    release: Arc<(Mutex<bool>, Condvar)>,
    ready: Arc<AtomicBool>,
    intercepted: Arc<AtomicBool>,
    sender: mpsc::Sender<()>,
) {
    let Ok(request) = read_frame(&mut client) else {
        return;
    };
    let Ok(mut server) = TcpStream::connect(upstream) else {
        return;
    };
    if write_frame(&mut server, &request).is_err() {
        return;
    }
    let Ok(response) = read_frame(&mut server) else {
        return;
    };
    if !is_commit(&request, token) || intercepted.swap(true, Ordering::SeqCst) {
        let _ = write_frame(&mut client, &response);
        let _ = client.shutdown(Shutdown::Write);
        return;
    }
    if !ready.swap(true, Ordering::SeqCst) {
        let _ = sender.send(());
    }
    if matches!(action, CommitAction::DropImmediately) {
        return;
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
    drop(released);
    if matches!(action, CommitAction::DropAfterRelease) {
        return;
    }
    let _ = write_frame(&mut client, &response);
    let _ = client.shutdown(Shutdown::Write);
}

fn read_frame(stream: &mut TcpStream) -> std::io::Result<Vec<u8>> {
    let mut length = [0_u8; 4];
    stream.read_exact(&mut length)?;
    let length = u32::from_be_bytes(length) as usize;
    if length > MAX_FRAME {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "frame exceeds 16 MiB",
        ));
    }
    let mut frame = vec![0_u8; length];
    stream.read_exact(&mut frame)?;
    Ok(frame)
}

fn write_frame(stream: &mut TcpStream, frame: &[u8]) -> std::io::Result<()> {
    if frame.len() > MAX_FRAME {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "frame exceeds 16 MiB",
        ));
    }
    let length = u32::try_from(frame.len())
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidData, "frame too large"))?;
    stream.write_all(&length.to_be_bytes())?;
    stream.write_all(frame)
}

fn is_commit(request: &[u8], token: &str) -> bool {
    const COMMIT: u8 = 4;
    if request.first() != Some(&1) || request.len() < 5 {
        return false;
    }
    let length = u32::from_le_bytes(request[1..5].try_into().expect("four-byte token length"));
    let command = 5_usize.saturating_add(length as usize);
    request.get(5..command) == Some(token.as_bytes()) && request.get(command) == Some(&COMMIT)
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

pub fn runtime() -> &'static tokio::runtime::Runtime {
    // OpenDAL's reqwest transport is process-wide. Recreating a runtime between
    // opening the authority and using it can leave pooled connections attached
    // to a dead executor, which surfaces as intermittent reqwest::send errors
    // when acceptance tests run concurrently.
    static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("build acceptance runtime")
    })
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
    let command_description = describe_command(&command);
    command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = command
        .spawn()
        .unwrap_or_else(|error| panic!("spawn process ({command_description}): {error}"));
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
                    "command timed out after {timeout:?} with {status} ({command_description}):\nstdout:\n{}\nstderr:\n{}",
                    String::from_utf8_lossy(&stdout),
                    String::from_utf8_lossy(&stderr)
                );
            }
            Err(error) => {
                let _ = child.kill();
                let _ = child.wait();
                panic!("wait for process ({command_description}): {error}");
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

fn describe_command(command: &Command) -> String {
    std::iter::once(command.get_program())
        .chain(command.get_args())
        .map(|value| format!("{value:?}"))
        .collect::<Vec<_>>()
        .join(" ")
}

fn read_all(mut reader: impl Read) -> Vec<u8> {
    let mut bytes = Vec::new();
    let _ = reader.read_to_end(&mut bytes);
    bytes
}
