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

//! The single CLI frontend for Managed and Direct Sync.

use crate::{CliError, CliResult, CliStage};
use clap::{Subcommand, ValueEnum};
use std::os::fd::BorrowedFd;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use yinyang::sync_engine::{
    BindingState, DirectAdapter, FrozenRemoteRequest, ManagedAdapter, PreparedRemoteRequest,
    RemoteAdapter, RemoteBaseline, RemoteCapabilities, RemoteEntry, RemoteObservation,
    RemoteOutcome, RemoteRequest, Resolution, RunOutcome, StatusDocument, SyncEngine, SyncModel,
};
use yinyang::volume::{Access, Model, VolumeContext};

#[derive(Subcommand)]
pub enum SyncCommand {
    /// Bind a local directory to the configured remote namespace.
    Init {
        #[arg(long)]
        directory: PathBuf,
    },
    /// Write a quiesced local Sync backup package.
    Backup {
        #[arg(long)]
        output: PathBuf,
    },
    /// Restore a backup into absent same-filesystem root and state directories.
    Restore {
        #[arg(long)]
        backup: PathBuf,
        #[arg(long)]
        directory: PathBuf,
    },
    /// Reconcile the bound local directory with the configured remote namespace.
    Run,
    /// Report Sync status without mutating local or remote state.
    Status {
        /// Emit the versioned `yinyang.sync.status.v1` document.
        #[arg(long)]
        json: bool,
    },
    /// Export retained bytes for a conflict side or retained artifact.
    Export {
        #[arg(long, conflicts_with = "conflict")]
        artifact: Option<String>,
        #[arg(long, requires = "side")]
        conflict: Option<String>,
        #[arg(long)]
        side: Option<SyncSide>,
        #[arg(long)]
        output: PathBuf,
    },
    /// Resolve one conflict or uncertain work item.
    Resolve {
        id: String,
        #[arg(long, conflicts_with = "content")]
        keep: Option<SyncSide>,
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

    fn resolution(self) -> Resolution {
        match self {
            Self::Local => Resolution::KeepLocal,
            Self::Remote => Resolution::KeepRemote,
        }
    }
}

const EXIT_UNBOUND: u8 = 3;
const EXIT_INVALID_BINDING: u8 = 4;

#[derive(Clone)]
enum CliRemote {
    Managed(Box<ManagedAdapter>),
    Direct(Box<DirectAdapter>),
}

impl RemoteAdapter for CliRemote {
    fn model(&self) -> SyncModel {
        match self {
            Self::Managed(remote) => remote.model(),
            Self::Direct(remote) => remote.model(),
        }
    }

    fn namespace_id(&self) -> String {
        match self {
            Self::Managed(remote) => remote.namespace_id(),
            Self::Direct(remote) => remote.namespace_id(),
        }
    }

    fn capabilities(&self) -> RemoteCapabilities {
        match self {
            Self::Managed(remote) => remote.capabilities(),
            Self::Direct(remote) => remote.capabilities(),
        }
    }

    async fn preflight(&self, state: &Path) -> yinyang::sync_engine::Result<()> {
        match self {
            Self::Managed(remote) => remote.preflight(state).await,
            Self::Direct(remote) => remote.preflight(state).await,
        }
    }

    async fn bind(&self, state: &Path) -> yinyang::sync_engine::Result<()> {
        match self {
            Self::Managed(remote) => remote.bind(state).await,
            Self::Direct(remote) => remote.bind(state).await,
        }
    }

    async fn observe(
        &self,
        baseline: Option<&RemoteBaseline>,
    ) -> yinyang::sync_engine::Result<RemoteObservation> {
        match self {
            Self::Managed(remote) => remote.observe(baseline).await,
            Self::Direct(remote) => remote.observe(baseline).await,
        }
    }

    async fn read_file(
        &self,
        entry: &RemoteEntry,
        output: BorrowedFd<'_>,
    ) -> yinyang::sync_engine::Result<()> {
        match self {
            Self::Managed(remote) => remote.read_file(entry, output).await,
            Self::Direct(remote) => remote.read_file(entry, output).await,
        }
    }

    async fn freeze(
        &self,
        request: RemoteRequest,
    ) -> yinyang::sync_engine::Result<FrozenRemoteRequest> {
        match self {
            Self::Managed(remote) => remote.freeze(request).await,
            Self::Direct(remote) => remote.freeze(request).await,
        }
    }

    async fn receipt(
        &self,
        request: &FrozenRemoteRequest,
    ) -> yinyang::sync_engine::Result<Option<RemoteOutcome>> {
        match self {
            Self::Managed(remote) => remote.receipt(request).await,
            Self::Direct(remote) => remote.receipt(request).await,
        }
    }

    async fn publish_batch(
        &self,
        requests: &[PreparedRemoteRequest],
    ) -> yinyang::sync_engine::Result<Vec<RemoteOutcome>> {
        match self {
            Self::Managed(remote) => remote.publish_batch(requests).await,
            Self::Direct(remote) => remote.publish_batch(requests).await,
        }
    }
}

pub async fn run(context: VolumeContext, command: SyncCommand) -> CliResult<ExitCode> {
    if context.config.access != Access::Sync {
        return Err(CliError::invalid(
            CliStage::Argument,
            format!(
                "sync requires access=sync, found {:?}",
                context.config.access
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
        SyncCommand::Resolve { id, keep, content } => {
            resolve(&context, &id, keep, content.as_deref()).await
        }
    }
}

async fn remote(context: &VolumeContext) -> CliResult<CliRemote> {
    context.storage_capabilities()?;
    match context.config.model {
        Model::Managed => {
            let authority = context
                .connect_authority()
                .await
                .map_err(|error| CliError::from_runtime(CliStage::Connect, error))?;
            Ok(CliRemote::Managed(Box::new(ManagedAdapter::new(authority))))
        }
        Model::Direct => {
            let binding = context
                .direct_storage_binding()
                .map_err(|error| CliError::from_runtime(CliStage::Admission, error))?;
            let namespace = context
                .direct_namespace()
                .map_err(|error| CliError::from_runtime(CliStage::Admission, error))?;
            let remote = DirectAdapter::open(
                namespace,
                yinyang::sync_engine::hex(&binding.namespace_id()),
            )
            .map_err(|error| engine_error(CliStage::Admission, &error))?;
            Ok(CliRemote::Direct(Box::new(remote)))
        }
    }
}

async fn status(context: &VolumeContext, json: bool) -> CliResult<ExitCode> {
    let document = inspect_document(context, None).await;
    let code = match document.binding.state {
        BindingState::Unbound => EXIT_UNBOUND,
        BindingState::Bound => 0,
        BindingState::Error => EXIT_INVALID_BINDING,
    };
    emit(&document, json)?;
    Ok(ExitCode::from(code))
}

pub(crate) async fn inspect_document(
    context: &VolumeContext,
    previous_remote_error: Option<String>,
) -> StatusDocument {
    let model = match context.config.model {
        Model::Managed => SyncModel::Managed,
        Model::Direct => SyncModel::Direct,
    };
    let (remote, remote_error) = match remote(context).await {
        Ok(remote) => (Some(remote), None),
        Err(error) => {
            let detail = match previous_remote_error {
                Some(previous) => format!("{previous}; {error}"),
                None => error.to_string(),
            };
            (None, Some(detail))
        }
    };
    SyncEngine::<CliRemote>::inspect(
        remote.as_ref(),
        &context.config.staging,
        model,
        remote_error,
    )
    .await
}

async fn init(context: &VolumeContext, directory: &Path) -> CliResult<ExitCode> {
    let remote = remote(context).await?;
    SyncEngine::init(remote, &context.config.staging, directory)
        .await
        .map_err(|error| engine_error(CliStage::LocalState, &error))?;
    println!(
        "sync bound {} to state {}",
        directory.display(),
        context.config.staging.display()
    );
    Ok(ExitCode::SUCCESS)
}

async fn backup(context: &VolumeContext, output: &Path) -> CliResult<ExitCode> {
    let remote = remote(context).await?;
    let mut engine = SyncEngine::open(remote, &context.config.staging)
        .await
        .map_err(|error| engine_error(CliStage::SyncState, &error))?;
    engine
        .backup(output)
        .await
        .map_err(|error| engine_error(CliStage::LocalState, &error))?;
    println!("sync backup written to {}", output.display());
    Ok(ExitCode::SUCCESS)
}

async fn restore(context: &VolumeContext, backup: &Path, directory: &Path) -> CliResult<ExitCode> {
    let remote = remote(context).await?;
    SyncEngine::restore(remote, backup, directory, &context.config.staging)
        .await
        .map_err(|error| engine_error(CliStage::LocalState, &error))?;
    println!(
        "sync restored {} with state {}",
        directory.display(),
        context.config.staging.display()
    );
    Ok(ExitCode::SUCCESS)
}

async fn reconcile(context: &VolumeContext) -> CliResult<ExitCode> {
    let remote = remote(context).await?;
    let mut engine = SyncEngine::open(remote, &context.config.staging)
        .await
        .map_err(|error| engine_error(CliStage::SyncState, &error))?;
    let outcome = engine
        .run()
        .await
        .map_err(|error| engine_error(CliStage::SyncState, &error))?;
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
    let result = if let Some(id) = artifact {
        SyncEngine::<CliRemote>::export_artifact_from_state(&context.config.staging, &id, &mut file)
    } else {
        let id = conflict.as_deref().expect("conflict selector present");
        let side = side.expect("conflict side present");
        if side == SyncSide::Local {
            SyncEngine::<CliRemote>::export_conflict_local_from_state(
                &context.config.staging,
                id,
                &mut file,
            )
        } else {
            let remote = remote(context).await?;
            let engine = SyncEngine::open(remote, &context.config.staging)
                .await
                .map_err(|error| engine_error(CliStage::SyncState, &error))?;
            engine.export_conflict_side(id, side.as_str(), &mut file)
        }
    };
    drop(file);
    if let Err(error) = result {
        let _ = std::fs::remove_file(output);
        return Err(engine_error(CliStage::LocalState, &error));
    }
    Ok(ExitCode::SUCCESS)
}

async fn resolve(
    context: &VolumeContext,
    id: &str,
    keep: Option<SyncSide>,
    content: Option<&Path>,
) -> CliResult<ExitCode> {
    let resolution = match (keep, content) {
        (Some(side), None) => side.resolution(),
        (None, Some(path)) => {
            Resolution::Content(tokio::fs::read(path).await.map_err(|error| {
                CliError::new(
                    CliStage::LocalState,
                    crate::runtime_kind_from_io(error.kind()),
                    format!("read resolution content {}: {error}", path.display()),
                )
            })?)
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
    let remote = remote(context).await?;
    let mut engine = SyncEngine::open(remote, &context.config.staging)
        .await
        .map_err(|error| engine_error(CliStage::SyncState, &error))?;
    let outcome = engine
        .resolve(id, resolution)
        .await
        .map_err(|error| engine_error(CliStage::SyncState, &error))?;
    report(&outcome)?;
    Ok(ExitCode::SUCCESS)
}

fn report(outcome: &RunOutcome) -> CliResult<()> {
    if outcome.unknown > 0 {
        return Err(CliError::new(
            CliStage::SyncState,
            yinyang::runtime::ErrorKind::Unknown,
            format!(
                "{} sync outcome(s) are unknown; inspect `sync status` and retry",
                outcome.unknown
            ),
        ));
    }
    println!(
        "sync: published {} materialized {} deleted-remote {} deleted-local {} pending {} conflicts {} in-sync {}",
        outcome.published_local,
        outcome.materialized_remote,
        outcome.deleted_remote,
        outcome.deleted_local,
        outcome.pending,
        outcome.conflicts,
        outcome.in_sync,
    );
    Ok(())
}

fn emit(document: &StatusDocument, json: bool) -> CliResult<()> {
    use std::io::Write as _;
    let mut stdout = std::io::stdout().lock();
    if json {
        writeln!(stdout, "{}", serde_json::to_string_pretty(document)?)?;
    } else {
        writeln!(
            stdout,
            "sync: {:?} remote: {:?} local: {:?} in-sync: {} pending: {} unknown: {} conflicts: {} failed: {} artifacts: {} retained-bytes: {}",
            document.binding.state,
            document.remote.availability,
            document.local.scan,
            document.summary.in_sync,
            document.summary.pending,
            document.summary.unknown,
            document.summary.conflicts,
            document.summary.failed,
            document.summary.retained_artifacts,
            document.summary.retained_bytes,
        )?;
    }
    Ok(())
}

fn engine_error(stage: CliStage, error: &yinyang::sync_engine::Error) -> CliError {
    CliError::new(stage, error.kind(), error.to_string())
}
