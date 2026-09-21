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
use crate::{Command, revision};
use clap::Subcommand;
use std::path::{Path, PathBuf};
use tokio::io::AsyncReadExt;
use yinyang::runtime::{Runtime, RuntimeStatus};
use yinyang::volume::{Volume, VolumeConfig};

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
fn operator() -> Result<opendal::Operator, opendal::Error> {
    let config = std::env::vars()
        .filter_map(|(k, v)| {
            k.strip_prefix("YINYANG_S3_")
                .map(|k| (k.to_ascii_lowercase(), v))
        })
        .collect::<Vec<_>>();
    opendal::Operator::via_iter("s3", config)
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
            e.sequence, e.handle, e.persisted, e.message
        );
    }
}
async fn destination(
    runtime: &Runtime,
    path: &str,
) -> Result<(yinyang::core::NodeId, String), Box<dyn std::error::Error>> {
    let (parent, name) = path.rsplit_once('/').unwrap_or(("", path));
    let snapshot = runtime.authority().observe_latest().await?;
    let parent = snapshot
        .resolve(parent)
        .await?
        .ok_or("destination parent missing")?;
    Ok((parent.id(), name.to_owned()))
}
pub async fn run(path: &Path, command: Command) -> Result<(), Box<dyn std::error::Error>> {
    let mut config = VolumeConfig::from_json(&tokio::fs::read(path).await?)?;
    if config.staging.is_relative() {
        config.staging = path
            .parent()
            .unwrap_or(Path::new("."))
            .join(&config.staging);
    }
    let token = std::env::var("YINYANG_SERVICE_TOKEN").ok();
    if matches!(command, Command::Status) {
        println!(
            "volume: {} access: {:?} frontend: {:?} read-only: {}",
            config.name, config.access, config.frontend, config.read_only
        );
        if config.staging.join("staging.db").exists() {
            status(Runtime::inspect(&config.staging).await?);
        } else {
            println!("local staging: not initialized");
        }
        // Always report persisted errors even when credentials or the authority are unavailable.
        match operator() {
            Ok(operator) => match Volume::connect(&config, operator, token).await {
                Ok(authority) => match authority.observe_latest().await {
                    Ok(snapshot) => println!("remote latest: {}", revision(snapshot.revision())),
                    Err(error) => println!("remote: unavailable ({error})"),
                },
                Err(error) => println!("remote: unavailable ({error})"),
            },
            Err(error) => println!("remote: unavailable ({error})"),
        }
        return Ok(());
    }
    let operator = operator()?;
    if matches!(command, Command::Capabilities) {
        println!(
            "{}",
            serde_json::to_string_pretty(&config.capabilities(&operator))?
        );
        config.validate(&operator)?;
        return Ok(());
    }
    if let Command::Receipt { commit_id } = command {
        let authority = Volume::connect(&config, operator, token).await?;
        let receipt = authority
            .observe_latest()
            .await?
            .receipt(yinyang::core::CommitId::from_bytes(*commit_id.as_bytes()))
            .await?
            .ok_or("receipt absent; a delayed request may still publish")?;
        println!(
            "commit: {commit_id}\nrevision: {}",
            revision(receipt.cursor.revision)
        );
        return Ok(());
    }
    let Command::File { command } = command else {
        return Err("with --volume use file, capabilities, receipt, or status; initialize object storage with yy create or service storage with yy serve".into());
    };
    let volume = Volume::open(config, operator, token).await?;
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
                        .ok_or("offset overflow")?;
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
                .ok_or("source missing")?;
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
                .ok_or("path missing")?;
            runtime.unlink(node.id()).await?;
        }
    }
    Ok(())
}
