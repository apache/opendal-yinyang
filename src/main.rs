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
use std::path::PathBuf;
use std::process::ExitCode;
use std::str::FromStr;
use yinyang::core::{BackendProfile, CommitId, Fs, Revision};
#[cfg(target_os = "linux")]
mod direct_sync_cli;
mod directory_cli;
mod file_cli;
mod operations_cli;
mod space_cli;
#[cfg(target_os = "linux")]
mod sync_cli;

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
    /// Inspect each capability layer and its effective intersection.
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
    /// Inspect Managed space and plan or explicitly expire retained history (requires --volume).
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
            ExitCode::FAILURE
        }
    };
    #[cfg(feature = "benchmark-telemetry")]
    drop(telemetry);
    result
}
async fn run(cli: Cli) -> Result<ExitCode, Box<dyn std::error::Error>> {
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
            return Err("publication commands do not use --volume; configure object storage with YINYANG_S3_* instead".into());
        }
        return directory_cli::inspect(*commit_id, state, *json).await;
    }
    if cli.volume.is_some() && matches!(cli.command, Command::Publication { .. }) {
        return Err("publication commands do not use --volume; configure object storage with YINYANG_S3_* instead".into());
    }
    if let Some(path) = &cli.volume {
        return match cli.command {
            Command::Doctor { json } => {
                return Ok(operations_cli::run(path, json).await);
            }
            #[cfg(target_os = "linux")]
            Command::Sync { command } => direct_sync_cli::run(path, command).await,
            Command::Space { command } => space_cli::run(path, command).await,
            command => file_cli::run(path, command).await,
        };
    }
    match cli.command {
        Command::Mount { .. } => {
            return Err("mount requires --volume CONFIG.json with frontend=fuse".into());
        }
        Command::File { .. } | Command::Capabilities | Command::Doctor { .. } => {
            return Err("this command requires --volume CONFIG.json".into());
        }
        #[cfg(target_os = "linux")]
        Command::Sync { .. } => {
            return Err("this command requires --volume CONFIG.json".into());
        }
        Command::Space { .. } => {
            return Err("space commands require --volume CONFIG.json".into());
        }
        _ => {}
    }
    let profile = match std::env::var("YINYANG_STORAGE_PROFILE").as_deref() {
        Ok("amazon-s3") => BackendProfile::AmazonS3,
        Ok("minio") => BackendProfile::Minio,
        Err(_) if std::env::var_os("YINYANG_S3_ENDPOINT").is_none() => BackendProfile::AmazonS3,
        _ => return Err("set YINYANG_STORAGE_PROFILE to amazon-s3 or minio for the configured endpoint; other S3-compatible deployments are unsupported".into()),
    };
    let config = std::env::vars()
        .filter_map(|(key, value)| {
            key.strip_prefix("YINYANG_S3_")
                .map(|key| (key.to_ascii_lowercase(), value))
        })
        .collect::<Vec<_>>();
    let operator = opendal::Operator::via_iter("s3", config)?;
    #[cfg(feature = "benchmark-telemetry")]
    let operator = yinyang::benchmark::instrument(operator);
    if let Command::Serve { database, listen } = &cli.command {
        let token = std::env::var("YINYANG_SERVICE_TOKEN").map_err(
            |_| "set YINYANG_SERVICE_TOKEN to a random authentication token of at least 32 bytes",
        )?;
        if !listen.ip().is_loopback() || !(32..=1024).contains(&token.len()) {
            return Err("metadata service requires loopback and a 32..=1024 byte token".into());
        }
        let service = yinyang::core::service::MetadataService::create(database, operator).await?;
        let listener = tokio::net::TcpListener::bind(listen).await?;
        println!("metadata service listening on {}", listener.local_addr()?);
        yinyang::core::service::serve(listener, service, token).await?;
        return Ok(ExitCode::SUCCESS);
    }
    if matches!(cli.command, Command::Create) {
        let fs = Fs::create(operator, profile).await?;
        println!(
            "filesystem ready at revision {}",
            revision(fs.observe_latest().await?.revision())
        );
        return Ok(ExitCode::SUCCESS);
    }
    let fs = Fs::open(operator, profile).await?;
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
                let journal = yinyang::DirectoryJournal::open(&fs, state).await?;
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
    use super::parse_revision;

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
}
