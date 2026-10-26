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

//! CLI adapter for Linux ordinary-directory Managed Sync.
//!
//! This module owns argument parsing, volume admission, exit-code mapping and
//! the `yinyang.sync.status.v1` projection. The reconciliation itself, the
//! durable binding and the local installation live in the library (`replica`
//! and `sync`); this adapter binds a volume configuration to that contract and
//! formats its results.

use clap::{Subcommand, ValueEnum};
use serde::Serialize;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use yinyang::volume::{Access, Model, VolumeConfig};

/// Managed Sync subcommands. Every subcommand requires `--volume CONFIG`.
#[derive(Subcommand)]
pub enum SyncCommand {
    /// Bind a local directory to the volume's Managed remote root.
    Init {
        #[arg(long)]
        directory: PathBuf,
    },
    /// Reconcile the bound local directory with the Managed remote root.
    Run,
    /// Report Managed Sync status without mutating any state.
    Status {
        /// Emit the versioned `yinyang.sync.status.v1` document instead of prose.
        #[arg(long)]
        json: bool,
    },
    /// Export retained bytes for a conflict side or a retained artifact.
    Export {
        /// Retained artifact identity reported by `sync status`.
        #[arg(long, conflicts_with = "conflict")]
        artifact: Option<String>,
        /// Conflict identity reported by `sync status`.
        #[arg(long, requires = "side")]
        conflict: Option<String>,
        /// Conflict side to export; required with `--conflict`.
        #[arg(long)]
        side: Option<SyncSide>,
        /// Destination path; must not already exist.
        #[arg(long)]
        output: PathBuf,
    },
    /// Resolve one conflict by keeping a side or adopting explicit content.
    Resolve {
        /// Conflict identity reported by `sync status`.
        conflict: String,
        /// Keep the local or remote observation.
        #[arg(long, conflicts_with = "content")]
        keep: Option<SyncSide>,
        /// Adopt explicit bytes from a local file.
        #[arg(long)]
        content: Option<PathBuf>,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum SyncSide {
    Local,
    Remote,
}

impl SyncSide {
    fn as_str(self) -> &'static str {
        match self {
            Self::Local => "local",
            Self::Remote => "remote",
        }
    }
}

/// Exit codes are part of the Managed Sync CLI contract: `0` valid, `3`
/// unbound, `4` invalid binding, `1` argument or unexpected error.
const EXIT_UNBOUND: u8 = 3;

/// The versioned status document consumed by operators and acceptance tests.
///
/// Every field is always present; unknown values are reported as `null` rather
/// than omitted, so a consumer can distinguish "unknown" from "not applicable".
#[derive(Clone, Debug, Serialize, Eq, PartialEq)]
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
    pub const SCHEMA: &'static str = "yinyang.sync.status.v1";

    /// A schema-valid document for a state directory that is not yet bound.
    fn unbound(state_directory: &Path) -> Self {
        Self {
            schema: Self::SCHEMA.to_owned(),
            binding: BindingStatus {
                state: BindingState::Unbound,
                state_directory: state_directory.display().to_string(),
                root: None,
                filesystem: None,
                same_filesystem: None,
            },
            remote: RemoteStatus {
                availability: RemoteAvailability::NotChecked,
                revision: None,
                error: None,
            },
            local: LocalStatus {
                scan: LocalScanState::NotScanned,
                error: None,
            },
            summary: StatusSummary::default(),
            entries: Vec::new(),
            conflicts: Vec::new(),
            artifacts: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Eq, PartialEq)]
pub struct BindingStatus {
    pub state: BindingState,
    pub state_directory: String,
    pub root: Option<String>,
    pub filesystem: Option<String>,
    pub same_filesystem: Option<bool>,
}

#[derive(Clone, Copy, Debug, Serialize, Eq, PartialEq)]
#[allow(dead_code)]
#[serde(rename_all = "snake_case")]
pub enum BindingState {
    Unbound,
    Bound,
    Error,
}

#[derive(Clone, Debug, Serialize, Eq, PartialEq)]
pub struct RemoteStatus {
    pub availability: RemoteAvailability,
    pub revision: Option<String>,
    pub error: Option<String>,
}

#[derive(Clone, Copy, Debug, Serialize, Eq, PartialEq)]
#[allow(dead_code)]
#[serde(rename_all = "snake_case")]
pub enum RemoteAvailability {
    NotChecked,
    Available,
    Unavailable,
}

#[derive(Clone, Debug, Serialize, Eq, PartialEq)]
pub struct LocalStatus {
    pub scan: LocalScanState,
    pub error: Option<String>,
}

#[derive(Clone, Copy, Debug, Serialize, Eq, PartialEq)]
#[allow(dead_code)]
#[serde(rename_all = "snake_case")]
pub enum LocalScanState {
    NotScanned,
    Clean,
    Changed,
    Failed,
}

#[derive(Clone, Debug, Default, Serialize, Eq, PartialEq)]
pub struct StatusSummary {
    pub in_sync: bool,
    pub pending: u64,
    pub unknown: u64,
    pub conflicts: usize,
    pub failed: u64,
    pub retained_artifacts: u64,
}

#[derive(Clone, Debug, Serialize, Eq, PartialEq)]
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
    pub conflict: Option<EntryConflict>,
}

#[derive(Clone, Copy, Debug, Serialize, Eq, PartialEq)]
#[allow(dead_code)]
#[serde(rename_all = "snake_case")]
pub enum EntryState {
    InSync,
    LocalPending,
    RemotePending,
    Conflict,
    Unknown,
    Failed,
}

#[derive(Clone, Debug, Serialize, Eq, PartialEq)]
pub struct LocalEvidence {
    pub kind: Option<EntryKind>,
    pub length: Option<u64>,
    pub digest: Option<String>,
    pub executable: Option<bool>,
    pub presence: Presence,
}

#[derive(Clone, Debug, Serialize, Eq, PartialEq)]
pub struct RemoteEvidence {
    pub kind: Option<EntryKind>,
    pub length: Option<u64>,
    pub digest: Option<String>,
    pub executable: Option<bool>,
    pub presence: Presence,
    pub node: Option<String>,
    pub revision: Option<String>,
}

#[derive(Clone, Copy, Debug, Serialize, Eq, PartialEq)]
#[allow(dead_code)]
#[serde(rename_all = "snake_case")]
pub enum EntryKind {
    File,
    Directory,
}

#[derive(Clone, Copy, Debug, Serialize, Eq, PartialEq)]
#[allow(dead_code)]
#[serde(rename_all = "snake_case")]
pub enum Presence {
    Present,
    Absent,
    Unknown,
}

#[derive(Clone, Debug, Serialize, Eq, PartialEq)]
pub struct BaselineEvidence {
    pub node: Option<String>,
    pub revision: Option<String>,
}

#[derive(Clone, Debug, Serialize, Eq, PartialEq)]
pub struct IntentStatus {
    pub id: Option<String>,
    pub operation: Option<IntentOperation>,
    pub state: Option<IntentState>,
    pub commit_id: Option<String>,
    pub receipt_revision: Option<String>,
}

#[derive(Clone, Copy, Debug, Serialize, Eq, PartialEq)]
#[allow(dead_code)]
#[serde(rename_all = "snake_case")]
pub enum IntentOperation {
    Content,
    CreateFile,
    CreateDirectory,
    Remove,
    Rename,
    SetExecutable,
}

#[derive(Clone, Copy, Debug, Serialize, Eq, PartialEq)]
#[allow(dead_code)]
#[serde(rename_all = "snake_case")]
pub enum IntentState {
    Prepared,
    Unknown,
    Committed,
    Conflict,
    Retryable,
    Failed,
}

#[derive(Clone, Debug, Serialize, Eq, PartialEq)]
pub struct OutcomeStatus {
    pub state: OutcomeState,
    pub error: Option<String>,
}

#[derive(Clone, Copy, Debug, Serialize, Eq, PartialEq)]
#[allow(dead_code)]
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

#[derive(Clone, Debug, Serialize, Eq, PartialEq)]
pub struct EntryConflict {
    pub id: String,
    pub local: ConflictSide,
    pub remote: ConflictSide,
    pub local_artifact: Option<String>,
    pub remote_artifact: Option<String>,
    pub remote_revision: Option<String>,
}

#[derive(Clone, Debug, Serialize, Eq, PartialEq)]
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

#[derive(Clone, Copy, Debug, Serialize, Eq, PartialEq)]
#[allow(dead_code)]
#[serde(rename_all = "snake_case")]
pub enum ConflictSide {
    CurrentFile,
    CurrentDirectory,
    RetainedArtifact,
    Deleted,
    Unknown,
}

#[derive(Clone, Debug, Serialize, Eq, PartialEq)]
pub struct ArtifactRecord {
    pub id: String,
    pub origin: String,
    pub kind: ArtifactKind,
    pub phase: ArtifactPhase,
    pub path: String,
    pub exportable: bool,
    pub error: Option<String>,
}

#[derive(Clone, Copy, Debug, Serialize, Eq, PartialEq)]
#[allow(dead_code)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactKind {
    ReplacedFile,
    RollbackDisplaced,
    QuarantinedFile,
    QuarantinedDirectory,
    UploadRetained,
}

#[derive(Clone, Copy, Debug, Serialize, Eq, PartialEq)]
#[allow(dead_code)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactPhase {
    Preparing,
    Prepared,
    Moved,
    Retained,
    Conflict,
    Failed,
}

pub async fn run(
    config_path: &Path,
    command: SyncCommand,
) -> Result<ExitCode, Box<dyn std::error::Error>> {
    let mut config = load_volume(config_path).await?;
    if config.staging.is_relative() {
        config.staging = config_path
            .parent()
            .unwrap_or(Path::new("."))
            .join(&config.staging);
    }
    admit(&config)?;
    match command {
        SyncCommand::Status { json } => status(&config, json).await,
        SyncCommand::Init { directory } => init(&config, &directory).await,
        SyncCommand::Run => reconcile(&config).await,
        SyncCommand::Export {
            artifact,
            conflict,
            side,
            output,
        } => export(&config, artifact, conflict, side, &output).await,
        SyncCommand::Resolve {
            conflict,
            keep,
            content,
        } => resolve(&config, &conflict, keep, content.as_deref()).await,
    }
}

async fn load_volume(path: &Path) -> Result<VolumeConfig, Box<dyn std::error::Error>> {
    let bytes = tokio::fs::read(path)
        .await
        .map_err(|error| format!("read volume configuration {}: {error}", path.display()))?;
    Ok(VolumeConfig::from_json(&bytes)?)
}

/// Builds the OpenDAL operator from `YINYANG_S3_*` without contacting storage.
fn operator() -> Result<opendal::Operator, opendal::Error> {
    let config = std::env::vars()
        .filter_map(|(key, value)| {
            key.strip_prefix("YINYANG_S3_")
                .map(|key| (key.to_ascii_lowercase(), value))
        })
        .collect::<Vec<_>>();
    opendal::Operator::via_iter("s3", config)
}

/// Applies the same four-layer admission as every other volume command.
fn admit(config: &VolumeConfig) -> Result<(), Box<dyn std::error::Error>> {
    // `VolumeConfig::validate` admits every supported mode; the sync frontend
    // must additionally pin the exact Managed + Sync combination it implements.
    if config.model != Model::Managed || config.access != Access::Sync {
        return Err(format!(
            "managed sync requires a Managed volume with access=sync, found model={:?} access={:?}",
            config.model, config.access
        )
        .into());
    }
    let operator = operator().map_err(|error| format!("storage configuration: {error}"))?;
    config.validate(&operator)?;
    Ok(())
}

async fn status(config: &VolumeConfig, json: bool) -> Result<ExitCode, Box<dyn std::error::Error>> {
    // Unbound is observable from the local state directory alone, so status
    // stays offline and never needs the authority.
    if !config.staging.is_dir() {
        let document = SyncStatus::unbound(&config.staging);
        emit(&document, json)?;
        return Ok(ExitCode::from(EXIT_UNBOUND));
    }
    let inspection = manager::inspect(config).await?;
    emit(&inspection.status, json)?;
    Ok(ExitCode::from(inspection.exit))
}

async fn init(
    config: &VolumeConfig,
    directory: &Path,
) -> Result<ExitCode, Box<dyn std::error::Error>> {
    manager::init(config, directory).await?;
    Ok(ExitCode::SUCCESS)
}

async fn reconcile(config: &VolumeConfig) -> Result<ExitCode, Box<dyn std::error::Error>> {
    manager::reconcile(config).await?;
    Ok(ExitCode::SUCCESS)
}

async fn export(
    config: &VolumeConfig,
    artifact: Option<String>,
    conflict: Option<String>,
    side: Option<SyncSide>,
    output: &Path,
) -> Result<ExitCode, Box<dyn std::error::Error>> {
    let selector = match (artifact, conflict) {
        (Some(artifact), None) => manager::Export::artifact(artifact),
        (None, Some(conflict)) => {
            let side = side.ok_or("--conflict requires --side local|remote")?;
            manager::Export::conflict(conflict, side.as_str())
        }
        (Some(_), Some(_)) => {
            return Err("--artifact and --conflict are mutually exclusive".into());
        }
        (None, None) => {
            return Err("export requires exactly one of --artifact or --conflict".into());
        }
    };
    manager::export(config, selector, output).await?;
    Ok(ExitCode::SUCCESS)
}

async fn resolve(
    config: &VolumeConfig,
    conflict: &str,
    keep: Option<SyncSide>,
    content: Option<&Path>,
) -> Result<ExitCode, Box<dyn std::error::Error>> {
    let decision = match (keep, content) {
        (Some(side), None) => manager::Resolution::keep(side.as_str()),
        (None, Some(path)) => manager::Resolution::content(path),
        (Some(_), Some(_)) => {
            return Err("--keep and --content are mutually exclusive".into());
        }
        (None, None) => {
            return Err("resolve requires exactly one of --keep or --content".into());
        }
    };
    manager::resolve(config, conflict, decision).await?;
    Ok(ExitCode::SUCCESS)
}

/// Writes the status projection, propagating serialization or stdout failure
/// instead of reporting success without a document.
fn emit(document: &SyncStatus, json: bool) -> Result<(), Box<dyn std::error::Error>> {
    use std::io::Write as _;
    let mut stdout = std::io::stdout().lock();
    if json {
        let text = serde_json::to_string_pretty(document)?;
        writeln!(stdout, "{text}")?;
    } else {
        writeln!(
            stdout,
            "sync: {:?} remote: {:?} local: {:?} in-sync: {} pending: {} unknown: {} conflicts: {} failed: {} artifacts: {}",
            document.binding.state,
            document.remote.availability,
            document.local.scan,
            document.summary.in_sync,
            document.summary.pending,
            document.summary.unknown,
            document.summary.conflicts,
            document.summary.failed,
            document.summary.retained_artifacts,
        )?;
    }
    Ok(())
}

/// Boundary between the CLI adapter and the library's reconciliation contract.
///
/// The replica and sync modules own the durable binding, local installation and
/// baseline-bound reconciliation. This adapter contributes argument shapes,
/// admission, exit codes and the status projection; the functions below are the
/// single place that has to be wired to that public API.
mod manager {
    use super::SyncStatus;
    use std::path::Path;
    use yinyang::volume::VolumeConfig;

    pub struct Inspection {
        pub status: SyncStatus,
        /// `0` for a valid bound state, `4` for an invalid binding.
        pub exit: u8,
    }

    #[allow(dead_code)]
    pub enum Export {
        Artifact(String),
        Conflict(String, String),
    }

    impl Export {
        pub fn artifact(id: String) -> Self {
            Self::Artifact(id)
        }

        pub fn conflict(id: String, side: &str) -> Self {
            Self::Conflict(id, side.to_owned())
        }
    }

    #[allow(dead_code)]
    pub enum Resolution {
        Keep(String),
        Content(std::path::PathBuf),
    }

    impl Resolution {
        pub fn keep(side: &str) -> Self {
            Self::Keep(side.to_owned())
        }

        pub fn content(path: &Path) -> Self {
            Self::Content(path.to_owned())
        }
    }

    fn unwired<T>() -> Result<T, String> {
        Err(
            "managed sync reconciliation is not wired to the library yet; the replica/sync \
             modules must expose a read-only binding probe plus baseline-bound init, run, export \
             and resolve entry points returning yinyang.sync.status.v1"
                .to_owned(),
        )
    }

    pub async fn inspect(_config: &VolumeConfig) -> Result<Inspection, String> {
        unwired()
    }

    pub async fn init(_config: &VolumeConfig, _directory: &Path) -> Result<(), String> {
        unwired()
    }

    pub async fn reconcile(_config: &VolumeConfig) -> Result<(), String> {
        unwired()
    }

    pub async fn export(
        _config: &VolumeConfig,
        _selector: Export,
        _output: &Path,
    ) -> Result<(), String> {
        unwired()
    }

    pub async fn resolve(
        _config: &VolumeConfig,
        _conflict: &str,
        _decision: Resolution,
    ) -> Result<(), String> {
        unwired()
    }
}
