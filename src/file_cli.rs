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

//! CLI adapter for volume admission and durable file-handle operations.
use crate::{CliError, CliResult, CliStage, Command, revision};
use clap::Subcommand;
use std::path::PathBuf;
use std::process::ExitCode;
use tokio::io::AsyncReadExt;
use yinyang::direct::{DirectRuntime, DirectRuntimeInspectionState};
use yinyang::runtime::{Runtime, RuntimeStatus};
use yinyang::volume::{Access, Model, VolumeContext};

#[derive(Subcommand)]
pub enum FileCommand {
    Open {
        path: String,
        #[arg(long)]
        write: bool,
    },
    Read {
        handle: uuid::Uuid,
        #[arg(long, default_value_t = 0)]
        offset: u64,
        #[arg(long, default_value_t = 65536)]
        length: usize,
    },
    Write {
        handle: uuid::Uuid,
        source: PathBuf,
        #[arg(long, default_value_t = 0, conflicts_with = "append")]
        offset: u64,
        #[arg(long)]
        append: bool,
    },
    Truncate {
        handle: uuid::Uuid,
        length: u64,
    },
    Fsync {
        handle: uuid::Uuid,
    },
    Close {
        handle: uuid::Uuid,
    },
    Abort {
        handle: uuid::Uuid,
    },
    AcknowledgeError {
        handle: uuid::Uuid,
    },
    AcknowledgeErrors {
        through: u64,
    },
    Create {
        path: String,
    },
    Mkdir {
        path: String,
    },
    Rename {
        source: String,
        destination: String,
    },
    Unlink {
        path: String,
    },
}
fn status(value: RuntimeStatus) {
    println!("local handles: {}", value.handles.len());
    for h in value.handles {
        println!(
            "handle: {} local: {} remote: {} revision: {} pending: {} frozen: {} conflict: {}",
            h.id,
            h.local_generation,
            h.remote_generation,
            revision(h.remote_revision),
            h.pending,
            h.frozen,
            h.conflict
        );
        if let Some(error) = h.error {
            println!("handle error: {error}");
        }
    }
    for e in value.errors {
        println!(
            "error {} handle {} persisted {}: {}",
            e.sequence, e.handle, e.persisted, e.error
        );
    }
}
async fn destination(runtime: &Runtime, path: &str) -> CliResult<(yinyang::core::NodeId, String)> {
    let (parent, name) = path.rsplit_once('/').unwrap_or(("", path));
    let snapshot = runtime.authority().observe_latest().await?;
    let parent = snapshot
        .resolve(parent)
        .await?
        .ok_or_else(|| CliError::invalid(CliStage::Operation, "destination parent missing"))?;
    Ok((parent.id(), name.to_owned()))
}
pub async fn run(context: VolumeContext, command: Command) -> CliResult<ExitCode> {
    let config = &context.config;
    if let Command::Status { revision: Some(_) } = &command {
        return Err(CliError::invalid(
            CliStage::Argument,
            "--revision is only supported by object-storage `yy status`, not `yy --volume CONFIG status`",
        ));
    }
    if matches!(command, Command::Status { .. }) {
        println!(
            "volume: {} model: {:?} access: {:?} frontend: {:?} read-only: {}",
            config.name, config.model, config.access, config.frontend, config.read_only
        );
        if config.model == Model::Direct {
            let mut local_error = None;
            if config.read_only {
                println!("local staging: not used by read-only Direct");
            } else if config.access == Access::Sync {
                println!(
                    "direct sync state: inspect with `yy --volume {} sync status --json`",
                    context.path.display()
                );
            } else {
                match DirectRuntime::inspect(&config.staging, None) {
                    Ok(inspection) => match inspection.state {
                        DirectRuntimeInspectionState::Missing => {
                            println!("direct local state: missing; nothing has been initialized")
                        }
                        DirectRuntimeInspectionState::Uninitialized => println!(
                            "direct local state: uninitialized; staging contains no runtime database"
                        ),
                        DirectRuntimeInspectionState::Ready => println!(
                            "direct local state: profile={} objects(clean={} staged={} pending={} conflict={} unknown={} failed={} discarded={}) mutations(staged={} dispatching={} applied={} conflict={} unknown={} destination-applied={} source-cleanup={} failed={} source-dispatching={})",
                            inspection.profile.as_deref().unwrap_or("unknown"),
                            inspection.objects.clean,
                            inspection.objects.staged,
                            inspection.objects.pending,
                            inspection.objects.conflict,
                            inspection.objects.unknown,
                            inspection.objects.failed,
                            inspection.objects.discarded,
                            inspection.mutations.staged,
                            inspection.mutations.dispatching,
                            inspection.mutations.applied,
                            inspection.mutations.conflict,
                            inspection.mutations.unknown,
                            inspection.mutations.destination_applied,
                            inspection.mutations.pending_source_cleanup,
                            inspection.mutations.failed,
                            inspection.mutations.source_dispatching
                        ),
                    },
                    Err(error) => {
                        println!("direct local state: unavailable ({error})");
                        local_error = Some(error);
                    }
                }
            }
            match context.direct_namespace() {
                Ok(namespace) => match namespace.observe().await {
                    Ok(snapshot) => {
                        let root = snapshot.read_dir(snapshot.root()).unwrap_or_default();
                        let directories = root.iter().filter(|node| node.is_directory()).count();
                        println!(
                            "direct namespace: {} ({} root entries, {directories} directories)",
                            snapshot.revision().to_hex(),
                            root.len()
                        );
                    }
                    Err(error) => println!("direct namespace: unavailable ({error})"),
                },
                Err(error) => println!("direct namespace: unavailable ({error})"),
            }
            return match local_error {
                Some(error) => Err(error.into()),
                None => Ok(ExitCode::SUCCESS),
            };
        }
        if config.staging.join("staging.db").exists() {
            status(Runtime::inspect(&config.staging).await?);
        } else {
            println!("local staging: not initialized");
        }
        // Always report persisted errors even when credentials or the authority are unavailable.
        match context.connect_authority().await {
            Ok(authority) => match authority.observe_latest().await {
                Ok(snapshot) => println!("remote latest: {}", revision(snapshot.revision())),
                Err(error) => println!("remote: unavailable ({error})"),
            },
            Err(error) => println!("remote: unavailable ({error})"),
        }
        return Ok(ExitCode::SUCCESS);
    }
    if let Command::Receipt { commit_id } = command {
        if config.model == Model::Direct {
            return Err(CliError::invalid(
                CliStage::Argument,
                "Direct volumes have no Managed publication receipts",
            ));
        }
        let authority = context
            .connect_authority()
            .await
            .map_err(|error| CliError::from_runtime(CliStage::Connect, error))?;
        let Some(receipt) = authority
            .observe_latest()
            .await?
            .receipt(yinyang::core::CommitId::from_bytes(*commit_id.as_bytes()))
            .await?
        else {
            eprintln!(
                "yy: receipt absent; a delayed request may still publish, so absence is not proof of failure"
            );
            return Ok(ExitCode::from(3));
        };
        println!(
            "commit: {commit_id}\nrevision: {}",
            revision(receipt.cursor.revision)
        );
        return Ok(ExitCode::SUCCESS);
    }
    if let Command::Mount { mountpoint } = command {
        #[cfg(target_os = "linux")]
        {
            let operator = context.operator()?;
            let service_token = context.service_token()?;
            return match yinyang::fuse::run(
                context.config.clone(),
                operator,
                service_token,
                mountpoint,
            )
            .await?
            {
                yinyang::fuse::Outcome::Unmounted => Ok(ExitCode::SUCCESS),
                yinyang::fuse::Outcome::TeardownExpired { grace } => {
                    eprintln!(
                        "yy: FUSE mount did not finish teardown within {}s (the session or the \
                         refresh task is still running, for example clients holding references or a \
                         refresh blocked on remote I/O); exiting non-zero to abort the connection \
                         (equivalent to crash recovery)",
                        grace.as_secs()
                    );
                    std::process::exit(1);
                }
            };
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = mountpoint;
            return Err(CliError::invalid(
                CliStage::Admission,
                "the fuse frontend is implemented only on Linux",
            ));
        }
    }
    let Command::File { command } = command else {
        return Err(CliError::invalid(
            CliStage::Argument,
            "with --volume use mount, file, sync, space, capabilities, receipt, or status; initialize object storage with yy create or service storage with yy serve",
        ));
    };
    context.storage_capabilities()?;
    let volume = context.open_file_volume().await?;
    let runtime = volume.runtime();
    match command {
        FileCommand::Open { path, write } => {
            println!("{}", runtime.open_file(&path, write).await?.id())
        }
        FileCommand::Read {
            handle,
            offset,
            length,
        } => {
            use std::io::Write;
            let file = runtime.recover(handle).await?;
            let mut position = offset;
            let mut remaining = length;
            while remaining > 0 {
                let bytes = file.read(position, remaining.min(65536)).await?;
                if bytes.is_empty() {
                    break;
                }
                std::io::stdout().write_all(&bytes)?;
                remaining -= bytes.len();
                position += bytes.len() as u64;
            }
        }
        FileCommand::Write {
            handle,
            source,
            offset,
            append,
        } => {
            let mut file = runtime.recover(handle).await?;
            let mut source = tokio::fs::File::open(source).await?;
            let mut buffer = vec![0; 65536];
            let mut position = offset;
            loop {
                let count = source.read(&mut buffer).await?;
                if count == 0 {
                    break;
                }
                if append {
                    file.append(&buffer[..count]).await?;
                } else {
                    file.write(position, &buffer[..count]).await?;
                    position = position
                        .checked_add(count as u64)
                        .ok_or_else(|| CliError::invalid(CliStage::Argument, "offset overflow"))?;
                }
            }
            let status = file.sync_local().await?;
            println!(
                "handle: {} local: {} remote: {} pending: {}",
                file.id(),
                status.local_generation,
                status.remote_generation,
                status.pending
            );
        }
        FileCommand::Truncate { handle, length } => {
            runtime.recover(handle).await?.truncate(length).await?
        }
        FileCommand::Fsync { handle } => {
            if let Some(receipt) = runtime.recover(handle).await?.fsync().await? {
                println!(
                    "committed: {} revision: {}",
                    uuid::Uuid::from_bytes(*receipt.commit_id.as_bytes()),
                    revision(receipt.cursor.revision)
                );
            } else {
                println!("already remotely published");
            }
        }
        FileCommand::Close { handle } => runtime.recover(handle).await?.close().await?,
        FileCommand::Abort { handle } => runtime.recover(handle).await?.abort().await?,
        FileCommand::AcknowledgeError { handle } => {
            runtime.recover(handle).await?.acknowledge_error().await?
        }
        FileCommand::AcknowledgeErrors { through } => runtime.acknowledge_errors(through).await?,
        FileCommand::Create { path } => {
            let (parent, name) = destination(runtime, &path).await?;
            runtime.create_file(parent, &name).await?;
        }
        FileCommand::Mkdir { path } => {
            let (parent, name) = destination(runtime, &path).await?;
            runtime.create_directory(parent, &name).await?;
        }
        FileCommand::Rename {
            source,
            destination: target,
        } => {
            let node = runtime
                .authority()
                .observe_latest()
                .await?
                .resolve(&source)
                .await?
                .ok_or_else(|| CliError::invalid(CliStage::Operation, "source missing"))?;
            let (parent, name) = destination(runtime, &target).await?;
            runtime.rename(node.id(), parent, &name).await?;
        }
        FileCommand::Unlink { path } => {
            let node = runtime
                .authority()
                .observe_latest()
                .await?
                .resolve(&path)
                .await?
                .ok_or_else(|| CliError::invalid(CliStage::Operation, "path missing"))?;
            runtime.unlink(node.id()).await?;
        }
    }
    Ok(ExitCode::SUCCESS)
}
