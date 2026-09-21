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
use yinyang::core::{BackendProfile, CommitId, CommitOutcome, Fs, Revision};
mod file_cli;

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
    /// Operate recoverable file handles (requires --volume).
    File {
        #[command(subcommand)]
        command: file_cli::FileCommand,
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
    },
    /// Query an existing request without rescanning or republishing local files.
    Receipt { commit_id: uuid::Uuid },
    /// Restore the current pinned snapshot into a directory that does not exist.
    Restore { destination: PathBuf },
    /// Enumerate the pinned namespace and report its size.
    Status,
}
#[tokio::main]
async fn main() -> ExitCode {
    match run(Cli::parse()).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("yy: {error}");
            ExitCode::FAILURE
        }
    }
}
async fn run(cli: Cli) -> Result<(), Box<dyn std::error::Error>> {
    opendal::install_default();
    if let Some(path) = &cli.volume {
        return file_cli::run(path, cli.command).await;
    }
    if matches!(cli.command, Command::File { .. } | Command::Capabilities) {
        return Err("this command requires --volume CONFIG.json".into());
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
        return Ok(());
    }
    if matches!(cli.command, Command::Create) {
        let fs = Fs::create(operator, profile).await?;
        println!(
            "filesystem ready at revision {}",
            revision(fs.observe_latest().await?.revision())
        );
        return Ok(());
    }
    let fs = Fs::open(operator, profile).await?;
    let observed = fs.observe_latest().await?;
    match cli.command {
        Command::Create | Command::Serve { .. } | Command::File { .. } | Command::Capabilities => {
            unreachable!()
        }
        Command::Publish {
            source,
            replace,
            commit_id,
        } => {
            let uuid = commit_id.unwrap_or_else(uuid::Uuid::new_v4);
            let id = CommitId::from_bytes(*uuid.as_bytes());
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
            let request = yinyang::prepare_directory(&fs, &observed, &source, id).await?;
            eprintln!(
                "request digest: {}",
                blake3::Hash::from(request.digest()).to_hex()
            );
            match fs.commit(&request).await? {
                CommitOutcome::Committed(receipt) => println!("published revision {} (commit {uuid})",revision(receipt.cursor.revision)),
                CommitOutcome::Conflict => return Err("original namespace predicates changed; inspect the remote state before planning a new request".into()),
                CommitOutcome::Retryable => return Err("publication opportunity was exhausted without an unresolved write; submit a new directory request or retry the frozen Transaction through the library".into()),
                CommitOutcome::Unknown(_) => return Err(format!("publication may still complete; query yy receipt {uuid}; an absent receipt does not prove failure").into()),
            }
        }
        Command::Receipt { commit_id } => {
            let id = CommitId::from_bytes(*commit_id.as_bytes());
            let receipt = observed.receipt(id).await?.ok_or("receipt not found; a delayed attempt may still publish, so absence is not proof of failure")?;
            println!(
                "commit: {commit_id}\nrevision: {}\nordinal: {}\nrequest digest: {}",
                revision(receipt.cursor.revision),
                receipt.cursor.ordinal,
                blake3::Hash::from(receipt.request_digest).to_hex()
            );
        }
        Command::Restore { destination } => {
            yinyang::restore_directory(&fs, &observed, &destination).await?;
            println!(
                "restored revision {} to {}",
                revision(observed.revision()),
                destination.display()
            );
        }
        Command::Status => {
            let nodes = yinyang::directory_nodes(&observed).await?;
            let directories = nodes.values().filter(|n| n.is_directory()).count();
            println!(
                "revision: {}\nfiles: {}\ndirectories: {directories}",
                revision(observed.revision()),
                nodes.len() - directories
            );
        }
    }
    Ok(())
}
fn revision(value: Revision) -> String {
    value
        .to_bytes()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}
