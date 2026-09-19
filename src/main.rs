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

use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};
use yinyang::core::{CommitId, CommitOutcome, Fs, NodeBody};

#[derive(Parser)]
#[command(
    name = "yy",
    about = "Publish and restore directories on a Managed YinYang filesystem"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Create a filesystem, or validate an existing one.
    Create,
    /// Publish a complete directory snapshot, removing remote-only paths.
    Publish {
        source: PathBuf,
        /// Allow replacement of a non-empty remote namespace.
        #[arg(long)]
        replace: bool,
        /// Reuse this UUID when retrying an uncertain publication.
        #[arg(long)]
        commit_id: Option<uuid::Uuid>,
    },
    /// Restore the current snapshot into a directory that does not exist.
    Restore { destination: PathBuf },
    /// Show the current version and namespace size.
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
    let config = std::env::vars()
        .filter_map(|(key, value)| {
            key.strip_prefix("YINYANG_S3_")
                .map(|key| (key.to_ascii_lowercase(), value))
        })
        .collect::<Vec<_>>();
    let operator = opendal::Operator::via_iter("s3", config)?;
    if matches!(cli.command, Command::Create) {
        let fs = Fs::create(operator).await?;
        println!(
            "filesystem ready at version {}",
            fs.observe().await?.version().number()
        );
        return Ok(());
    }
    let fs = Fs::open(operator).await?;
    let observed = fs.observe().await?;
    match cli.command {
        Command::Create => unreachable!(),
        Command::Publish {
            source,
            replace,
            commit_id,
        } => {
            let uuid = commit_id.unwrap_or_else(uuid::Uuid::new_v4);
            let id = CommitId::from_bytes(*uuid.as_bytes());
            if !replace
                && observed.tree().iter().count() > 1
                && !observed.version().commits().contains(&id)
            {
                return Err("remote namespace is not empty; use --replace to publish a complete replacement".into());
            }
            eprintln!("publication ID: {uuid}; reuse --commit-id {uuid} for an uncertain retry");
            match yinyang::publish_directory(&fs, &observed, &source, id).await? {
                CommitOutcome::Committed { version } => println!("published version {version} (commit {uuid})"),
                CommitOutcome::Conflict { current } => return Err(format!("publication conflict: remote is at version {current}; inspect it before retrying").into()),
            }
        }
        Command::Restore { destination } => {
            yinyang::restore_directory(&fs, &observed, &destination).await?;
            println!(
                "restored version {} to {}",
                observed.version().number(),
                destination.display()
            );
        }
        Command::Status => {
            let files = observed
                .tree()
                .iter()
                .filter(|(_, node)| matches!(node.body(), NodeBody::File(_)))
                .count();
            let directories = observed.tree().iter().count() - files;
            println!(
                "version: {}\nfiles: {files}\ndirectories: {directories}",
                observed.version().number()
            );
        }
    }
    Ok(())
}
