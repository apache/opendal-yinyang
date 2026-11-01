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
//! the fixed `yinyang.sync.status.v1` projection. Reconciliation, durable
//! binding and local installation live in `yinyang::replica::DirectorySync`;
//! this adapter only binds a volume configuration to that responsibility and
//! projects its domain evidence into the CLI contract.

use std::path::{Path, PathBuf};
use std::process::ExitCode;

use crate::{CliError, CliResult, CliStage};
use clap::{Subcommand, ValueEnum};
use serde::Serialize;
use yinyang::core::Authority;
use yinyang::replica as domain;
use yinyang::replica::{DirectorySync, Replica};
use yinyang::volume::{Access, Model, VolumeContext};

/// Managed Sync subcommands. Every subcommand requires `--volume CONFIG`.
#[derive(Subcommand)]
pub enum SyncCommand {
    /// Bind a local directory to the volume's Managed remote root.
    Init {
        #[arg(long)]
        directory: PathBuf,
    },
    /// Write a quiesced local Sync backup package.
    Backup {
        #[arg(long)]
        output: PathBuf,
    },
    /// Complete an isolated restore of copied sync state and local directory.
    Restore {
        /// Backup package created by `sync backup`.
        #[arg(long)]
        backup: PathBuf,
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
        /// Destination path for the exported bytes.
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
    fn domain_keep(self) -> domain::Resolution {
        match self {
            Self::Local => domain::Resolution::KeepLocal,
            Self::Remote => domain::Resolution::KeepRemote,
        }
    }
}

/// Exit codes are part of the Managed Sync CLI contract: `0` valid, `3`
/// unbound, `4` invalid binding, `1` argument or unexpected error.
const EXIT_UNBOUND: u8 = 3;
const EXIT_INVALID_BINDING: u8 = 4;

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
    const SCHEMA: &'static str = "yinyang.sync.status.v1";
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
#[serde(rename_all = "snake_case")]
pub enum LocalScanState {
    NotScanned,
    Clean,
    Changed,
    Failed,
}

#[derive(Clone, Debug, Serialize, Eq, PartialEq)]
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
#[serde(rename_all = "snake_case")]
pub enum EntryKind {
    File,
    Directory,
}

#[derive(Clone, Copy, Debug, Serialize, Eq, PartialEq)]
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
#[serde(rename_all = "snake_case")]
pub enum ArtifactKind {
    ReplacedFile,
    RollbackDisplaced,
    QuarantinedFile,
    QuarantinedDirectory,
    UploadRetained,
}

#[derive(Clone, Copy, Debug, Serialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactPhase {
    Preparing,
    Prepared,
    Moved,
    Retained,
    Conflict,
    Failed,
}

pub async fn run(context: VolumeContext, command: SyncCommand) -> CliResult<ExitCode> {
    if context.config.model != Model::Managed || context.config.access != Access::Sync {
        return Err(CliError::invalid(
            CliStage::Argument,
            format!(
                "managed sync requires a Managed volume with access=sync, found model={:?} access={:?}",
                context.config.model, context.config.access
            ),
        ));
    }
    match command {
        SyncCommand::Status { json } => status(&context, json).await,
        SyncCommand::Init { directory } => init(&context, &directory).await,
        SyncCommand::Backup { output } => backup(&context, &output).await,
        SyncCommand::Restore { backup, directory } => restore(&context, &backup, &directory).await,
        SyncCommand::Run => reconcile(&context).await,
        SyncCommand::Export {
            artifact,
            conflict,
            side,
            output,
        } => export(&context, artifact, conflict, side, &output).await,
        SyncCommand::Resolve {
            conflict,
            keep,
            content,
        } => resolve(&context, &conflict, keep, content.as_deref()).await,
    }
}

/// Connects the configured publication authority, if it is reachable.
async fn connect(context: &VolumeContext) -> CliResult<std::sync::Arc<dyn Authority>> {
    context
        .connect_authority()
        .await
        .map_err(|error| CliError::from_runtime(CliStage::Connect, error))
}

async fn status(context: &VolumeContext, json: bool) -> CliResult<ExitCode> {
    let config = &context.config;
    // Classify the local binding without touching the authority, so an unbound
    // or invalid binding stays offline and never blocks on remote I/O.
    let local = DirectorySync::inspect_local(&config.staging, None)
        .await
        .map_err(|error| CliError::from_boxed(CliStage::SyncState, error))?;
    let inspection = match local.binding.state {
        domain::BindingState::Bound => match connect(context).await {
            Ok(authority) => DirectorySync::inspect(authority, &config.staging)
                .await
                .map_err(|error| CliError::from_boxed(CliStage::Observe, error))?,
            Err(error) => DirectorySync::inspect_local(&config.staging, Some(error.to_string()))
                .await
                .map_err(|error| CliError::from_boxed(CliStage::SyncState, error))?,
        },
        _ => local,
    };
    let code = match inspection.binding.state {
        domain::BindingState::Unbound => EXIT_UNBOUND,
        domain::BindingState::Bound => 0,
        domain::BindingState::Error => EXIT_INVALID_BINDING,
    };
    emit(&project(&inspection)?, json)?;
    Ok(ExitCode::from(code))
}

async fn init(context: &VolumeContext, directory: &Path) -> CliResult<ExitCode> {
    let authority = connect(context).await?;
    DirectorySync::init(authority, &context.config.staging, directory)
        .await
        .map_err(|error| CliError::from_boxed(CliStage::LocalState, error))?;
    println!(
        "sync bound {} to state {}",
        directory.display(),
        context.config.staging.display()
    );
    Ok(ExitCode::SUCCESS)
}

async fn backup(context: &VolumeContext, output: &Path) -> CliResult<ExitCode> {
    let authority = connect(context).await?;
    let directory = DirectorySync::open(authority, &context.config.staging)
        .await
        .map_err(|error| CliError::from_boxed(CliStage::SyncState, error))?;
    directory
        .backup(output)
        .await
        .map_err(|error| CliError::from_boxed(CliStage::LocalState, error))?;
    println!("sync backup written to {}", output.display());
    Ok(ExitCode::SUCCESS)
}

async fn restore(context: &VolumeContext, backup: &Path, directory: &Path) -> CliResult<ExitCode> {
    let authority = connect(context).await?;
    DirectorySync::restore(authority, backup, &context.config.staging, directory)
        .await
        .map_err(|error| CliError::from_boxed(CliStage::LocalState, error))?;
    println!(
        "sync restored {} with state {}",
        directory.display(),
        context.config.staging.display()
    );
    Ok(ExitCode::SUCCESS)
}

async fn reconcile(context: &VolumeContext) -> CliResult<ExitCode> {
    let authority = connect(context).await?;
    let mut directory = DirectorySync::open(authority, &context.config.staging)
        .await
        .map_err(|error| CliError::from_boxed(CliStage::SyncState, error))?;
    let outcome = directory
        .run()
        .await
        .map_err(|error| CliError::from_boxed(CliStage::SyncState, error))?;
    report(&outcome)?;
    Ok(ExitCode::SUCCESS)
}

async fn export(
    context: &VolumeContext,
    artifact: Option<String>,
    conflict: Option<String>,
    side: Option<SyncSide>,
    output: &Path,
) -> CliResult<ExitCode> {
    let side = match (&artifact, &conflict) {
        (Some(_), None) => None,
        (None, Some(_)) => Some(side.ok_or_else(|| {
            CliError::invalid(
                CliStage::Argument,
                "--conflict requires --side local|remote",
            )
        })?),
        (Some(_), Some(_)) => {
            return Err(CliError::invalid(
                CliStage::Argument,
                "--artifact and --conflict are mutually exclusive",
            ));
        }
        (None, None) => {
            return Err(CliError::invalid(
                CliStage::Argument,
                "export requires exactly one of --artifact or --conflict",
            ));
        }
    };
    // `create_new` refuses to clobber an existing path, so a failed export can
    // only ever remove the output this command created.
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(output)
        .map_err(|error| {
            CliError::new(
                CliStage::Output,
                crate::runtime_kind_from_io(error.kind()),
                format!(
                    "create export {} (refusing to overwrite an existing path): {error}",
                    output.display()
                ),
            )
        })?;
    let outcome: CliResult<()> = if let Some(id) = &artifact {
        // Retained bytes are local; export must not require remote connectivity.
        export_artifact_local(context, id, &mut file).await
    } else {
        let id = conflict.as_deref().expect("conflict selector present");
        match side.expect("conflict side present") {
            // The local side is durable local bytes; no authority is needed.
            SyncSide::Local => export_conflict_local(context, id, &mut file),
            SyncSide::Remote => export_conflict_remote(context, id, &mut file).await,
        }
    };
    drop(file);
    if let Err(error) = outcome {
        // A failed export must not leave a pseudo artifact behind.
        let _ = std::fs::remove_file(output);
        return Err(error);
    }
    Ok(ExitCode::SUCCESS)
}

/// Exports a retained artifact through the local replica, without connecting to
/// the publication authority.
async fn export_artifact_local(
    context: &VolumeContext,
    id: &str,
    output: &mut dyn std::io::Write,
) -> CliResult<()> {
    let inspection = DirectorySync::inspect_local(&context.config.staging, None)
        .await
        .map_err(|error| CliError::from_boxed(CliStage::SyncState, error))?;
    let volume = inspection.binding.volume.ok_or_else(|| {
        CliError::invalid(
            CliStage::SyncState,
            "sync state is not bound; run sync init",
        )
    })?;
    let replica = Replica::open(&context.config.staging, volume).map_err(|error| {
        CliError::new(
            CliStage::LocalState,
            runtime_kind_from_replica(error.kind()),
            error.to_string(),
        )
    })?;
    replica.export_artifact(id, output).map_err(|error| {
        CliError::new(
            CliStage::LocalState,
            runtime_kind_from_replica(error.kind()),
            error.to_string(),
        )
    })?;
    Ok(())
}

/// Exports a conflict's local side through the authority-free local reader.
fn export_conflict_local(
    context: &VolumeContext,
    id: &str,
    output: &mut dyn std::io::Write,
) -> CliResult<()> {
    yinyang::replica::export_conflict_local(&context.config.staging, id, output)
        .map_err(|error| CliError::from_boxed(CliStage::LocalState, error))?;
    Ok(())
}

/// Exports a conflict's remote side. It reads a retained revision, so this path
/// needs the publication authority.
async fn export_conflict_remote(
    context: &VolumeContext,
    id: &str,
    output: &mut dyn std::io::Write,
) -> CliResult<()> {
    let authority = connect(context).await?;
    let directory = DirectorySync::open(authority, &context.config.staging)
        .await
        .map_err(|error| CliError::from_boxed(CliStage::SyncState, error))?;
    directory
        .export_conflict_side(id, "remote", output)
        .await
        .map_err(|error| CliError::from_boxed(CliStage::Observe, error))?;
    Ok(())
}

async fn resolve(
    context: &VolumeContext,
    conflict: &str,
    keep: Option<SyncSide>,
    content: Option<&Path>,
) -> CliResult<ExitCode> {
    let decision = match (keep, content) {
        (Some(side), None) => side.domain_keep(),
        (None, Some(path)) => {
            let bytes = tokio::fs::read(path).await.map_err(|error| {
                CliError::new(
                    CliStage::LocalState,
                    crate::runtime_kind_from_io(error.kind()),
                    format!("read resolution content {}: {error}", path.display()),
                )
            })?;
            domain::Resolution::Content(bytes)
        }
        (Some(_), Some(_)) => {
            return Err(CliError::invalid(
                CliStage::Argument,
                "--keep and --content are mutually exclusive",
            ));
        }
        (None, None) => {
            return Err(CliError::invalid(
                CliStage::Argument,
                "resolve requires exactly one of --keep or --content",
            ));
        }
    };
    let authority = connect(context).await?;
    let mut directory = DirectorySync::open(authority, &context.config.staging)
        .await
        .map_err(|error| CliError::from_boxed(CliStage::SyncState, error))?;
    let outcome = directory
        .resolve(conflict, decision)
        .await
        .map_err(|error| CliError::from_boxed(CliStage::SyncState, error))?;
    report(&outcome)?;
    Ok(ExitCode::SUCCESS)
}

/// A finite reconciliation may legitimately end with work still pending (for
/// example, an observed v1 completion with a newer local v2). Only an
/// unresolved `Unknown` outcome — a possibly published request with no receipt
/// — must fail so the operator retries instead of treating it as lost.
fn report(outcome: &domain::RunOutcome) -> CliResult<()> {
    if outcome.unknown > 0 {
        return Err(CliError::new(
            CliStage::SyncState,
            yinyang::runtime::ErrorKind::Unknown,
            format!(
                "{} sync outcome(s) are unknown; a request may still publish — inspect `sync status` and retry",
                outcome.unknown
            ),
        ));
    }
    println!(
        "sync: published {} materialized {} pending {} conflicts {} in-sync {}",
        outcome.published_local,
        outcome.materialized_remote,
        outcome.pending,
        outcome.conflicts.len(),
        outcome.in_sync,
    );
    Ok(())
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn runtime_kind_from_replica(kind: domain::ErrorKind) -> yinyang::runtime::ErrorKind {
    use domain::ErrorKind as Kind;
    use yinyang::runtime::ErrorKind as RuntimeKind;
    match kind {
        Kind::Invalid => RuntimeKind::Invalid,
        Kind::Unsupported => RuntimeKind::Unsupported,
        Kind::Conflict => RuntimeKind::Conflict,
        Kind::Corrupt => RuntimeKind::Corrupt,
        Kind::Busy => RuntimeKind::Busy,
        Kind::Io => RuntimeKind::Io,
    }
}

fn project(inspection: &domain::Inspection) -> CliResult<SyncStatus> {
    let entries = inspection
        .entries
        .iter()
        .map(|entry| project_entry(entry, &inspection.conflicts))
        .collect::<Result<Vec<_>, _>>()?;
    let conflicts = inspection
        .conflicts
        .iter()
        .map(project_conflict)
        .collect::<Vec<_>>();
    let artifacts = inspection
        .artifacts
        .iter()
        .map(project_artifact)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(SyncStatus {
        schema: SyncStatus::SCHEMA.to_owned(),
        binding: project_binding(&inspection.binding),
        remote: project_remote(&inspection.remote),
        local: project_local(&inspection.local),
        summary: StatusSummary {
            in_sync: inspection.summary.in_sync,
            pending: inspection.summary.pending,
            unknown: inspection.summary.unknown,
            conflicts: inspection.summary.conflicts,
            failed: inspection.summary.failed,
            retained_artifacts: inspection.summary.retained_artifacts,
        },
        entries,
        conflicts,
        artifacts,
    })
}

fn project_binding(binding: &domain::BindingEvidence) -> BindingStatus {
    BindingStatus {
        state: match binding.state {
            domain::BindingState::Unbound => BindingState::Unbound,
            domain::BindingState::Bound => BindingState::Bound,
            domain::BindingState::Error => BindingState::Error,
        },
        state_directory: binding.state_directory.display().to_string(),
        root: binding.root.as_ref().map(|root| root.display().to_string()),
        filesystem: binding.volume.map(|volume| hex(volume.as_bytes())),
        same_filesystem: binding.same_volume,
    }
}

fn project_remote(remote: &domain::RemoteStatus) -> RemoteStatus {
    RemoteStatus {
        availability: match remote.availability {
            domain::RemoteAvailability::NotChecked => RemoteAvailability::NotChecked,
            domain::RemoteAvailability::Available => RemoteAvailability::Available,
            domain::RemoteAvailability::Unavailable => RemoteAvailability::Unavailable,
        },
        revision: remote.revision.map(|revision| hex(&revision.to_bytes())),
        error: remote.error.clone(),
    }
}

fn project_local(local: &domain::LocalStatus) -> LocalStatus {
    LocalStatus {
        scan: match local.scan {
            domain::LocalScanState::NotScanned => LocalScanState::NotScanned,
            domain::LocalScanState::Clean => LocalScanState::Clean,
            domain::LocalScanState::Changed => LocalScanState::Changed,
            domain::LocalScanState::Failed => LocalScanState::Failed,
        },
        error: local.error.clone(),
    }
}

fn project_entry(
    entry: &domain::EntryEvidence,
    conflicts: &[domain::Conflict],
) -> Result<SyncEntry, String> {
    let conflict = entry
        .conflict
        .as_deref()
        .and_then(|id| conflicts.iter().find(|conflict| conflict.id == id))
        .map(project_entry_conflict);
    Ok(SyncEntry {
        path: entry.path.clone(),
        original_path: entry.original_path.clone(),
        desired_path: entry.desired_path.clone(),
        state: match entry.state {
            domain::EntryState::InSync => EntryState::InSync,
            domain::EntryState::LocalPending => EntryState::LocalPending,
            domain::EntryState::RemotePending => EntryState::RemotePending,
            domain::EntryState::Conflict => EntryState::Conflict,
            domain::EntryState::Unknown => EntryState::Unknown,
            domain::EntryState::Failed => EntryState::Failed,
        },
        local: project_local_evidence(&entry.local),
        remote: project_remote_evidence(&entry.remote),
        baseline: BaselineEvidence {
            node: entry.baseline.node.map(|node| hex(node.as_bytes())),
            revision: entry
                .baseline
                .revision
                .map(|revision| hex(&revision.to_bytes())),
        },
        intent: IntentStatus {
            id: entry.intent.id.map(|id| hex(&id)),
            operation: entry.intent.operation.map(|operation| match operation {
                domain::IntentOperation::Content => IntentOperation::Content,
                domain::IntentOperation::CreateFile => IntentOperation::CreateFile,
                domain::IntentOperation::CreateDirectory => IntentOperation::CreateDirectory,
                domain::IntentOperation::Remove => IntentOperation::Remove,
                domain::IntentOperation::Rename => IntentOperation::Rename,
                domain::IntentOperation::SetExecutable => IntentOperation::SetExecutable,
            }),
            state: entry.intent.state.map(|state| match state {
                domain::IntentState::Prepared => IntentState::Prepared,
                domain::IntentState::Unknown => IntentState::Unknown,
                domain::IntentState::Committed => IntentState::Committed,
                domain::IntentState::Conflict => IntentState::Conflict,
                domain::IntentState::Retryable => IntentState::Retryable,
                domain::IntentState::Failed => IntentState::Failed,
            }),
            commit_id: entry.intent.commit_id.map(|commit| hex(commit.as_bytes())),
            receipt_revision: entry
                .intent
                .receipt_revision
                .map(|revision| hex(&revision.to_bytes())),
        },
        outcome: OutcomeStatus {
            state: match entry.outcome.state {
                domain::OutcomeState::Pending => OutcomeState::Pending,
                domain::OutcomeState::RemoteCompleted => OutcomeState::RemoteCompleted,
                domain::OutcomeState::Conflict => OutcomeState::Conflict,
                domain::OutcomeState::Unknown => OutcomeState::Unknown,
                domain::OutcomeState::Retryable => OutcomeState::Retryable,
                domain::OutcomeState::Failed => OutcomeState::Failed,
                domain::OutcomeState::None => OutcomeState::None,
            },
            error: entry.outcome.error.clone(),
        },
        conflict,
    })
}

fn project_local_evidence(local: &domain::LocalEvidence) -> LocalEvidence {
    LocalEvidence {
        kind: local.kind.map(|kind| match kind {
            domain::EntryKind::File => EntryKind::File,
            domain::EntryKind::Directory => EntryKind::Directory,
        }),
        length: local.length,
        digest: local.digest.map(|digest| hex(&digest)),
        executable: local.executable,
        presence: match local.presence {
            domain::Presence::Present => Presence::Present,
            domain::Presence::Absent => Presence::Absent,
            domain::Presence::Unknown => Presence::Unknown,
        },
    }
}

fn project_remote_evidence(remote: &domain::RemoteEvidence) -> RemoteEvidence {
    RemoteEvidence {
        kind: remote.kind.map(|kind| match kind {
            domain::EntryKind::File => EntryKind::File,
            domain::EntryKind::Directory => EntryKind::Directory,
        }),
        length: remote.length,
        digest: remote.digest.map(|digest| hex(&digest)),
        executable: remote.executable,
        presence: match remote.presence {
            domain::Presence::Present => Presence::Present,
            domain::Presence::Absent => Presence::Absent,
            domain::Presence::Unknown => Presence::Unknown,
        },
        node: remote.node.map(|node| hex(node.as_bytes())),
        revision: remote.revision.map(|revision| hex(&revision.to_bytes())),
    }
}

fn project_entry_conflict(conflict: &domain::Conflict) -> EntryConflict {
    EntryConflict {
        id: conflict.id.clone(),
        local: conflict_side(&conflict.local),
        remote: conflict_side(&conflict.remote),
        local_artifact: conflict.local_artifact.clone(),
        // The domain record exposes the remote node identity, not a retained
        // remote artifact; the schema keeps the field present and null.
        remote_artifact: None,
        remote_revision: conflict.remote_revision.map(|revision| hex(&revision)),
    }
}

fn project_conflict(conflict: &domain::Conflict) -> ConflictRecord {
    ConflictRecord {
        id: conflict.id.clone(),
        path: conflict.path.clone(),
        original_path: conflict.original_path.clone(),
        desired_path: conflict.desired_path.clone(),
        local: conflict_side(&conflict.local),
        remote: conflict_side(&conflict.remote),
        local_artifact: conflict.local_artifact.clone(),
        remote_artifact: None,
        remote_revision: conflict.remote_revision.map(|revision| hex(&revision)),
        error: conflict.error.clone(),
    }
}

fn conflict_side(side: &str) -> ConflictSide {
    match side {
        "current_file" => ConflictSide::CurrentFile,
        "current_directory" => ConflictSide::CurrentDirectory,
        "retained_artifact" => ConflictSide::RetainedArtifact,
        "deleted" => ConflictSide::Deleted,
        _ => ConflictSide::Unknown,
    }
}

fn project_artifact(artifact: &domain::Artifact) -> Result<ArtifactRecord, String> {
    let kind = match artifact.kind.as_str() {
        "replaced-file" => ArtifactKind::ReplacedFile,
        "rollback-displaced" => ArtifactKind::RollbackDisplaced,
        "quarantined-file" => ArtifactKind::QuarantinedFile,
        "quarantined-directory" => ArtifactKind::QuarantinedDirectory,
        "upload-retained" => ArtifactKind::UploadRetained,
        other => return Err(format!("unknown retained artifact kind {other:?}")),
    };
    let phase = match artifact.phase.as_str() {
        "preparing" => ArtifactPhase::Preparing,
        "prepared" => ArtifactPhase::Prepared,
        "moved" => ArtifactPhase::Moved,
        "retained" => ArtifactPhase::Retained,
        "conflict" => ArtifactPhase::Conflict,
        "failed" => ArtifactPhase::Failed,
        other => return Err(format!("unknown retained artifact phase {other:?}")),
    };
    Ok(ArtifactRecord {
        id: artifact.id.clone(),
        origin: artifact.origin.clone(),
        kind,
        phase,
        path: artifact.path.display().to_string(),
        exportable: artifact.exportable,
        error: artifact.error.clone(),
    })
}

/// Writes the status projection, propagating serialization or stdout failure
/// instead of reporting success without a document.
fn emit(document: &SyncStatus, json: bool) -> CliResult<()> {
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
