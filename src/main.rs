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

use clap::{Parser, Subcommand};
use std::error::Error as StdError;
use std::path::PathBuf;
use std::process::ExitCode;
use std::str::FromStr;
use std::sync::Arc;
use yinyang::core::{CommitId, Fs, Revision};
use yinyang::runtime::ErrorKind as RuntimeErrorKind;
#[cfg(target_os = "linux")]
use yinyang::volume::Model;
use yinyang::volume::{VolumeContext, VolumeLoadError, VolumeLoadStage};
#[cfg(target_os = "linux")]
mod direct_sync_cli;
mod directory_cli;
mod file_cli;
mod operations_cli;
mod space_cli;
#[cfg(target_os = "linux")]
mod sync_cli;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CliStage {
    Argument,
    Configuration,
    Operator,
    Admission,
    Connect,
    Observe,
    #[cfg(target_os = "linux")]
    LocalState,
    #[cfg(target_os = "linux")]
    SyncState,
    Publication,
    Output,
    Operation,
}

#[derive(Debug)]
pub(crate) struct CliError {
    stage: CliStage,
    kind: RuntimeErrorKind,
    message: String,
}

impl CliError {
    pub(crate) fn new(stage: CliStage, kind: RuntimeErrorKind, message: impl Into<String>) -> Self {
        Self {
            stage,
            kind,
            message: message.into(),
        }
    }

    pub(crate) fn invalid(stage: CliStage, message: impl Into<String>) -> Self {
        Self::new(stage, RuntimeErrorKind::Invalid, message)
    }

    pub(crate) fn from_runtime(stage: CliStage, error: yinyang::runtime::Error) -> Self {
        Self::new(stage, error.kind(), error.to_string())
    }

    pub(crate) fn from_boxed(stage: CliStage, error: Box<dyn StdError + Send + Sync>) -> Self {
        let kind = classify_boxed_error(error.as_ref());
        Self::new(stage, kind, error.to_string())
    }

    pub(crate) const fn stage(&self) -> CliStage {
        self.stage
    }

    pub(crate) const fn kind(&self) -> RuntimeErrorKind {
        self.kind
    }

    pub(crate) fn message(&self) -> &str {
        &self.message
    }

    fn exit_code(&self) -> u8 {
        exit_code_for_kind(self.kind)
    }
}

impl std::fmt::Display for CliError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl StdError for CliError {}

pub(crate) fn exit_code_for_kind(kind: RuntimeErrorKind) -> u8 {
    match kind {
        RuntimeErrorKind::Unknown => 3,
        RuntimeErrorKind::Conflict | RuntimeErrorKind::Retryable => 4,
        _ => 1,
    }
}

fn classify_boxed_error(error: &(dyn StdError + 'static)) -> RuntimeErrorKind {
    if let Some(error) = error.downcast_ref::<yinyang::runtime::Error>() {
        return error.kind();
    }
    if let Some(error) = error.downcast_ref::<yinyang::core::Error>() {
        return runtime_kind_from_core(error.kind());
    }
    if let Some(error) = error.downcast_ref::<yinyang::direct::Error>() {
        return runtime_kind_from_direct(error.kind());
    }
    #[cfg(target_os = "linux")]
    if let Some(error) = error.downcast_ref::<yinyang::replica::Error>() {
        return runtime_kind_from_replica(error.kind());
    }
    if let Some(error) = error.downcast_ref::<opendal::Error>() {
        return runtime_kind_from_opendal(error.kind());
    }
    if let Some(error) = error.downcast_ref::<std::io::Error>() {
        return runtime_kind_from_io(error.kind());
    }
    RuntimeErrorKind::Io
}

pub(crate) fn runtime_kind_from_io(kind: std::io::ErrorKind) -> RuntimeErrorKind {
    match kind {
        std::io::ErrorKind::NotFound => RuntimeErrorKind::NotFound,
        std::io::ErrorKind::AlreadyExists => RuntimeErrorKind::AlreadyExists,
        std::io::ErrorKind::PermissionDenied => RuntimeErrorKind::PermissionDenied,
        _ => RuntimeErrorKind::Io,
    }
}

pub(crate) fn runtime_kind_from_core(kind: yinyang::core::ErrorKind) -> RuntimeErrorKind {
    use yinyang::core::ErrorKind as Kind;
    match kind {
        Kind::Invalid => RuntimeErrorKind::Invalid,
        Kind::Corrupt => RuntimeErrorKind::Corrupt,
        Kind::NotFound => RuntimeErrorKind::NotFound,
        Kind::AlreadyExists => RuntimeErrorKind::AlreadyExists,
        Kind::Unsupported => RuntimeErrorKind::Unsupported,
        Kind::Storage => RuntimeErrorKind::Storage,
        Kind::Io => RuntimeErrorKind::Io,
        Kind::InvalidName => RuntimeErrorKind::InvalidName,
        Kind::NotDirectory => RuntimeErrorKind::NotDirectory,
        Kind::IsDirectory => RuntimeErrorKind::IsDirectory,
        Kind::NotEmpty => RuntimeErrorKind::NotEmpty,
        Kind::PermissionDenied => RuntimeErrorKind::PermissionDenied,
    }
}

fn runtime_kind_from_opendal(kind: opendal::ErrorKind) -> RuntimeErrorKind {
    match kind {
        opendal::ErrorKind::Unsupported => RuntimeErrorKind::Unsupported,
        opendal::ErrorKind::ConfigInvalid => RuntimeErrorKind::Invalid,
        opendal::ErrorKind::NotFound => RuntimeErrorKind::NotFound,
        opendal::ErrorKind::PermissionDenied => RuntimeErrorKind::PermissionDenied,
        opendal::ErrorKind::IsADirectory => RuntimeErrorKind::IsDirectory,
        opendal::ErrorKind::NotADirectory => RuntimeErrorKind::NotDirectory,
        opendal::ErrorKind::AlreadyExists => RuntimeErrorKind::AlreadyExists,
        opendal::ErrorKind::Conflict | opendal::ErrorKind::ConditionNotMatch => {
            RuntimeErrorKind::Conflict
        }
        _ => RuntimeErrorKind::Storage,
    }
}

pub(crate) fn runtime_kind_from_direct(kind: yinyang::direct::ErrorKind) -> RuntimeErrorKind {
    use yinyang::direct::ErrorKind as Kind;
    match kind {
        Kind::Invalid => RuntimeErrorKind::Invalid,
        Kind::InvalidName => RuntimeErrorKind::InvalidName,
        Kind::NotFound => RuntimeErrorKind::NotFound,
        Kind::NotDirectory => RuntimeErrorKind::NotDirectory,
        Kind::IsDirectory => RuntimeErrorKind::IsDirectory,
        Kind::Stale => RuntimeErrorKind::Conflict,
        Kind::Unsupported => RuntimeErrorKind::Unsupported,
        Kind::Corrupt => RuntimeErrorKind::Corrupt,
        Kind::Storage => RuntimeErrorKind::Storage,
    }
}

#[cfg(target_os = "linux")]
fn runtime_kind_from_replica(kind: yinyang::replica::ErrorKind) -> RuntimeErrorKind {
    use yinyang::replica::ErrorKind as Kind;
    match kind {
        Kind::Invalid => RuntimeErrorKind::Invalid,
        Kind::Unsupported => RuntimeErrorKind::Unsupported,
        Kind::Conflict => RuntimeErrorKind::Conflict,
        Kind::Corrupt => RuntimeErrorKind::Corrupt,
        Kind::Busy => RuntimeErrorKind::Busy,
        Kind::Io => RuntimeErrorKind::Io,
    }
}

impl From<VolumeLoadError> for CliError {
    fn from(error: VolumeLoadError) -> Self {
        let stage = match error.stage() {
            VolumeLoadStage::Configuration => CliStage::Configuration,
            VolumeLoadStage::Operator => CliStage::Operator,
            VolumeLoadStage::Admission => CliStage::Admission,
        };
        Self::new(stage, error.kind(), error.message())
    }
}

impl From<yinyang::runtime::Error> for CliError {
    fn from(error: yinyang::runtime::Error) -> Self {
        Self::from_runtime(CliStage::Operation, error)
    }
}

impl From<yinyang::core::Error> for CliError {
    fn from(error: yinyang::core::Error) -> Self {
        Self::from_runtime(CliStage::Operation, yinyang::runtime::Error::from(error))
    }
}

impl From<yinyang::direct::Error> for CliError {
    fn from(error: yinyang::direct::Error) -> Self {
        Self::new(
            CliStage::Operation,
            runtime_kind_from_direct(error.kind()),
            error.to_string(),
        )
    }
}

#[cfg(target_os = "linux")]
impl From<yinyang::replica::Error> for CliError {
    fn from(error: yinyang::replica::Error) -> Self {
        Self::new(
            CliStage::Operation,
            runtime_kind_from_replica(error.kind()),
            error.to_string(),
        )
    }
}

impl From<opendal::Error> for CliError {
    fn from(error: opendal::Error) -> Self {
        Self::new(
            CliStage::Operation,
            runtime_kind_from_opendal(error.kind()),
            error.to_string(),
        )
    }
}

impl From<std::io::Error> for CliError {
    fn from(error: std::io::Error) -> Self {
        Self::new(
            CliStage::Operation,
            runtime_kind_from_io(error.kind()),
            error.to_string(),
        )
    }
}

impl From<serde_json::Error> for CliError {
    fn from(error: serde_json::Error) -> Self {
        Self::invalid(CliStage::Output, error.to_string())
    }
}

impl From<String> for CliError {
    fn from(message: String) -> Self {
        Self::invalid(CliStage::Operation, message)
    }
}

impl From<&str> for CliError {
    fn from(message: &str) -> Self {
        Self::invalid(CliStage::Operation, message)
    }
}

impl From<Box<dyn StdError + Send + Sync>> for CliError {
    fn from(error: Box<dyn StdError + Send + Sync>) -> Self {
        Self::from_boxed(CliStage::Operation, error)
    }
}

impl From<Box<dyn StdError>> for CliError {
    fn from(error: Box<dyn StdError>) -> Self {
        Self::new(
            CliStage::Operation,
            classify_boxed_error(error.as_ref()),
            error.to_string(),
        )
    }
}

pub(crate) type CliResult<T> = std::result::Result<T, CliError>;

#[derive(Parser)]
#[command(
    name = "yy",
    about = "Publish and restore transactional YinYang snapshots"
)]
struct Cli {
    /// Named volume configuration for capability checks and recoverable file operations.
    #[arg(long, global = true)]
    volume: Option<PathBuf>,
    #[command(subcommand)]
    command: Command,
}
#[derive(Subcommand)]
enum Command {
    /// Inspect the admitted and storage-reported volume capabilities.
    Capabilities,
    /// Diagnose platform, configuration, storage, service and local-state readiness.
    Doctor {
        /// Emit the versioned `yinyang.operational-health.v1` document.
        #[arg(long)]
        json: bool,
    },
    /// Mount a Managed or Direct volume through the Linux FUSE frontend.
    Mount { mountpoint: PathBuf },
    /// Operate recoverable file handles (requires --volume).
    File {
        #[command(subcommand)]
        command: file_cli::FileCommand,
    },
    /// Reconcile a bound ordinary directory with a Managed or Direct volume (requires --volume).
    #[cfg(target_os = "linux")]
    Sync {
        #[command(subcommand)]
        command: sync_cli::SyncCommand,
    },
    /// Inspect Managed space and plan declarative retention (requires --volume).
    Space {
        #[command(subcommand)]
        command: space_cli::SpaceCommand,
    },
    /// Run the authenticated loopback metadata authority with a local SQLite database.
    Serve {
        #[arg(long)]
        database: PathBuf,
        #[arg(long, default_value = "127.0.0.1:7447")]
        listen: std::net::SocketAddr,
    },
    /// Create a filesystem, or validate an existing one.
    Create,
    /// Publish a complete directory, removing remote-only paths.
    Publish {
        source: PathBuf,
        /// Allow replacement of a non-empty remote namespace.
        #[arg(long)]
        replace: bool,
        /// Unique UUID for a new request, not a request to replan an old attempt.
        #[arg(long)]
        commit_id: Option<uuid::Uuid>,
        /// Explicit state directory for durable, source-free recovery.
        #[arg(long, requires = "commit_id")]
        state: Option<PathBuf>,
    },
    /// Query an existing request without rescanning or republishing local files.
    Receipt { commit_id: uuid::Uuid },
    /// Inspect, retry, or export durable one-shot directory publications.
    Publication {
        #[command(subcommand)]
        command: directory_cli::PublicationCommand,
    },
    /// Restore a pinned snapshot into a directory that does not exist.
    Restore {
        destination: PathBuf,
        /// Retained revision to restore; defaults to the latest revision.
        #[arg(long)]
        revision: Option<RevisionArg>,
    },
    /// Enumerate a pinned namespace and report its size.
    Status {
        /// Retained revision to inspect; defaults to the latest revision.
        #[arg(long)]
        revision: Option<RevisionArg>,
    },
}

#[derive(Clone, Copy, Debug)]
struct RevisionArg(Revision);

impl FromStr for RevisionArg {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        parse_revision(value).map(Self)
    }
}
#[tokio::main]
async fn main() -> ExitCode {
    #[cfg(feature = "benchmark-telemetry")]
    let telemetry = yinyang::benchmark::Telemetry::install();
    let result = match run(Cli::parse()).await {
        Ok(code) => code,
        Err(error) => {
            eprintln!("yy: {error}");
            ExitCode::from(error.exit_code())
        }
    };
    #[cfg(feature = "benchmark-telemetry")]
    drop(telemetry);
    result
}
async fn run(cli: Cli) -> CliResult<ExitCode> {
    opendal::install_default();
    if let Command::Publication {
        command:
            directory_cli::PublicationCommand::Inspect {
                commit_id,
                state,
                json,
            },
    } = &cli.command
    {
        if cli.volume.is_some() {
            return Err(CliError::invalid(
                CliStage::Argument,
                "publication commands do not use --volume; configure object storage with YINYANG_S3_* instead",
            ));
        }
        return directory_cli::inspect(*commit_id, state, *json).await;
    }
    if cli.volume.is_some() && matches!(cli.command, Command::Publication { .. }) {
        return Err(CliError::invalid(
            CliStage::Argument,
            "publication commands do not use --volume; configure object storage with YINYANG_S3_* instead",
        ));
    }
    if let Some(path) = &cli.volume {
        if let Command::Doctor { json } = &cli.command {
            return Ok(operations_cli::run(path, *json).await);
        }
        let context = VolumeContext::load(path).await?;
        return match cli.command {
            Command::Capabilities => {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&context.storage_capabilities()?)?
                );
                Ok(ExitCode::SUCCESS)
            }
            #[cfg(target_os = "linux")]
            Command::Sync { command } => match context.config.model {
                Model::Managed => sync_cli::run(context, command).await,
                Model::Direct => direct_sync_cli::run(context, command).await,
            },
            Command::Space { command } => space_cli::run(context, command).await,
            command => file_cli::run(context, command).await,
        };
    }
    match cli.command {
        Command::Mount { .. } => {
            return Err(CliError::invalid(
                CliStage::Argument,
                "mount requires --volume CONFIG.json with frontend=fuse",
            ));
        }
        Command::File { .. } | Command::Capabilities | Command::Doctor { .. } => {
            return Err(CliError::invalid(
                CliStage::Argument,
                "this command requires --volume CONFIG.json",
            ));
        }
        #[cfg(target_os = "linux")]
        Command::Sync { .. } => {
            return Err(CliError::invalid(
                CliStage::Argument,
                "this command requires --volume CONFIG.json",
            ));
        }
        Command::Space { .. } => {
            return Err(CliError::invalid(
                CliStage::Argument,
                "space inspection requires --volume CONFIG.json",
            ));
        }
        _ => {}
    }
    let connection = yinyang::volume::storage_connection()
        .map_err(|error| CliError::from_runtime(CliStage::Operator, error))?;
    yinyang::volume::storage_profile_from_env()
        .map_err(|error| CliError::from_runtime(CliStage::Operator, error))?;
    if let Command::Serve { database, listen } = &cli.command {
        let token = connection
            .service_token()
            .ok_or_else(|| {
                CliError::new(
                    CliStage::Operator,
                    RuntimeErrorKind::PermissionDenied,
                    "set YINYANG_SERVICE_TOKEN to a random authentication token of at least 32 bytes",
                )
            })?
            .to_owned();
        if !listen.ip().is_loopback() || !(32..=1024).contains(&token.len()) {
            return Err(CliError::invalid(
                CliStage::Configuration,
                "metadata service requires loopback and a 32..=1024 byte token",
            ));
        }
        let (operator, _) = connection.into_parts();
        let service = yinyang::core::service::MetadataService::create(database, operator).await?;
        let listener = tokio::net::TcpListener::bind(listen).await?;
        println!("metadata service listening on {}", listener.local_addr()?);
        yinyang::core::service::serve(listener, service, token).await?;
        return Ok(ExitCode::SUCCESS);
    }
    let (operator, _) = connection.into_parts();
    if matches!(cli.command, Command::Create) {
        let fs = Fs::create(operator).await?;
        println!(
            "filesystem ready at revision {}",
            revision(fs.observe_latest().await?.revision())
        );
        return Ok(ExitCode::SUCCESS);
    }
    let fs = Fs::open(operator).await?;
    match cli.command {
        Command::Create
        | Command::Serve { .. }
        | Command::File { .. }
        | Command::Capabilities
        | Command::Doctor { .. }
        | Command::Mount { .. } => unreachable!(),
        #[cfg(target_os = "linux")]
        Command::Sync { .. } => unreachable!(),
        Command::Space { .. } => unreachable!(),
        Command::Publish {
            source,
            replace,
            commit_id,
            state,
        } => {
            let uuid = commit_id.unwrap_or_else(uuid::Uuid::new_v4);
            let id = CommitId::from_bytes(*uuid.as_bytes());
            let journal = if let Some(state) = &state {
                let journal = yinyang::DirectoryJournal::open(Arc::new(fs.clone()), state).await?;
                if journal.status(id).await?.is_some() {
                    return Err(format!(
                        "publication {uuid} is already recorded in {}; replay it without SOURCE using `yy publication retry {uuid} --state {}`, or select a new --commit-id",
                        state.display(),
                        state.display()
                    )
                    .into());
                }
                Some(journal)
            } else {
                None
            };
            let observed = fs.observe_latest().await?;
            if observed.receipt(id).await?.is_some() {
                return Err(format!("commit identity is already used; query its original result with yy receipt {uuid}; use a new identity for a new snapshot").into());
            }
            if !replace && !observed.scan(fs.root(), None, 1).await?.entries.is_empty() {
                return Err(
                    "remote namespace is not empty; use --replace for a complete replacement"
                        .into(),
                );
            }
            eprintln!("publication ID: {uuid}");
            if let (Some(journal), Some(state)) = (journal, state) {
                let status = journal.prepare(&observed, &source, id).await?;
                eprintln!(
                    "request digest: {}\njournal state: {}",
                    blake3::Hash::from(status.intent.request_digest).to_hex(),
                    state.display()
                );
                return directory_cli::publish(&journal, id).await;
            }
            let request = yinyang::prepare_directory(&fs, &observed, &source, id).await?;
            eprintln!(
                "request digest: {}",
                blake3::Hash::from(request.digest()).to_hex()
            );
            return directory_cli::classify_outcome(fs.commit(&request).await?, uuid);
        }
        Command::Receipt { commit_id } => {
            let id = CommitId::from_bytes(*commit_id.as_bytes());
            let observed = fs.observe_latest().await?;
            let Some(receipt) = observed.receipt(id).await? else {
                eprintln!(
                    "yy: receipt not found; a delayed attempt may still publish, so absence is not proof of failure"
                );
                return Ok(ExitCode::from(3));
            };
            println!(
                "commit: {commit_id}\nrevision: {}\nordinal: {}\nrequest digest: {}",
                revision(receipt.cursor.revision),
                receipt.cursor.ordinal,
                blake3::Hash::from(receipt.request_digest).to_hex()
            );
        }
        Command::Publication { command } => {
            return directory_cli::run(&fs, command).await;
        }
        Command::Restore {
            destination,
            revision: selected,
        } => {
            let snapshot = match selected {
                Some(selected) => fs.observe_revision(selected.0).await?,
                None => fs.observe_latest().await?,
            };
            yinyang::restore_directory(&fs, &snapshot, &destination).await?;
            println!(
                "restored revision {} to {}",
                revision(snapshot.revision()),
                destination.display()
            );
        }
        Command::Status { revision: selected } => {
            let snapshot = match selected {
                Some(selected) => fs.observe_revision(selected.0).await?,
                None => fs.observe_latest().await?,
            };
            let nodes = yinyang::directory_nodes(&snapshot).await?;
            let directories = nodes.values().filter(|n| n.is_directory()).count();
            println!(
                "revision: {}\nfiles: {}\ndirectories: {directories}",
                revision(snapshot.revision()),
                nodes.len() - directories
            );
        }
    }
    Ok(ExitCode::SUCCESS)
}
fn revision(value: Revision) -> String {
    value
        .to_bytes()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

fn parse_revision(value: &str) -> Result<Revision, String> {
    let value = value.as_bytes();
    if value.len() != 48 {
        return Err("revision must be exactly 48 hexadecimal characters".to_owned());
    }
    let mut bytes = [0_u8; 24];
    for (index, byte) in bytes.iter_mut().enumerate() {
        let offset = index * 2;
        *byte = (hex_nibble(value[offset])? << 4) | hex_nibble(value[offset + 1])?;
    }
    Ok(Revision::from_bytes(bytes))
}

fn hex_nibble(value: u8) -> Result<u8, String> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        b'A'..=b'F' => Ok(value - b'A' + 10),
        _ => Err("revision contains a non-hexadecimal character".to_owned()),
    }
}

#[cfg(test)]
mod tests {
    use super::{parse_revision, runtime_kind_from_direct};
    use yinyang::runtime::ErrorKind;

    #[test]
    fn revision_requires_exactly_24_hex_bytes() {
        assert!(parse_revision("00").is_err());
        assert!(parse_revision(&"é".repeat(24)).is_err());
        assert!(parse_revision(&"g".repeat(48)).is_err());
        assert_eq!(
            parse_revision(&"01".repeat(24))
                .unwrap()
                .to_bytes()
                .as_slice(),
            &[1; 24]
        );
    }

    #[test]
    fn stale_direct_pin_is_a_conflict() {
        assert_eq!(
            runtime_kind_from_direct(yinyang::direct::ErrorKind::Stale),
            ErrorKind::Conflict
        );
    }
}
