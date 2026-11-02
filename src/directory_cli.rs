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

//! CLI adapter for durable one-shot directory publication recovery.

use crate::{CliError, CliResult, CliStage, exit_code_for_kind};
use clap::Subcommand;
use serde::Serialize;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use uuid::Uuid;
use yinyang::core::{CommitId, Fs, Receipt};
use yinyang::runtime::{Error, ErrorKind};
use yinyang::{DirectoryInspection, DirectoryJournal, DirectoryState, DirectoryStatus};

const SCHEMA: &str = "yinyang.publication.inspect.v1";

#[derive(Subcommand)]
pub enum PublicationCommand {
    /// Inspect durable publication requests without storage credentials.
    Inspect {
        /// Publication identity; omit to inspect every retained intent.
        commit_id: Option<Uuid>,
        /// Directory containing the durable publication journal.
        #[arg(long)]
        state: PathBuf,
        /// Emit a machine-readable document.
        #[arg(long)]
        json: bool,
    },
    /// Replay one original frozen publication without reading its source.
    Retry {
        commit_id: Uuid,
        /// Directory containing the durable publication journal.
        #[arg(long)]
        state: PathBuf,
    },
    /// Export the original prepared directory from immutable storage objects.
    Export {
        commit_id: Uuid,
        /// Directory containing the durable publication journal.
        #[arg(long)]
        state: PathBuf,
        /// New destination directory.
        #[arg(long)]
        destination: PathBuf,
    },
}

#[derive(Serialize)]
struct InspectionDocument {
    schema: &'static str,
    filesystem: String,
    root: String,
    intents: Vec<IntentDocument>,
}

#[derive(Serialize)]
struct IntentDocument {
    commit_id: String,
    baseline: String,
    request_digest: String,
    source: String,
    state: &'static str,
    receipt: Option<ReceiptDocument>,
    failure: Option<FailureDocument>,
    can_retry: bool,
    can_export: bool,
}

#[derive(Serialize)]
struct ReceiptDocument {
    revision: String,
    ordinal: u32,
}

#[derive(Serialize)]
struct FailureDocument {
    kind: &'static str,
    message: String,
    commit_id: Option<String>,
}

pub async fn inspect(commit_id: Option<Uuid>, state: &Path, json: bool) -> CliResult<ExitCode> {
    let inspection = DirectoryJournal::inspect(state).await?;
    let document = project_inspection(&inspection, commit_id)?;
    if json {
        println!("{}", serde_json::to_string_pretty(&document)?);
    } else {
        print_inspection(&document);
    }
    Ok(ExitCode::SUCCESS)
}

pub async fn run(fs: &Fs, command: PublicationCommand) -> CliResult<ExitCode> {
    match command {
        PublicationCommand::Inspect { .. } => {
            unreachable!("inspection is handled without a backend")
        }
        PublicationCommand::Retry { commit_id, state } => retry(fs, &state, commit_id).await,
        PublicationCommand::Export {
            commit_id,
            state,
            destination,
        } => export(fs, &state, commit_id, &destination).await,
    }
}

pub async fn publish(journal: &DirectoryJournal, commit_id: CommitId) -> CliResult<ExitCode> {
    match journal.publish(commit_id).await {
        Ok(receipt) => {
            print_receipt(&receipt);
            Ok(ExitCode::SUCCESS)
        }
        Err(error) => classify_journal_error(journal, commit_id, error).await,
    }
}

pub fn classify_outcome(
    outcome: yinyang::core::CommitOutcome,
    commit_id: Uuid,
) -> CliResult<ExitCode> {
    match outcome {
        yinyang::core::CommitOutcome::Committed(receipt) => {
            println!(
                "published revision {} (commit {commit_id})",
                revision(receipt.cursor.revision.to_bytes())
            );
            Ok(ExitCode::SUCCESS)
        }
        yinyang::core::CommitOutcome::Conflict => {
            eprintln!(
                "yy: original namespace predicates changed; publication conflicts are not retryable"
            );
            Ok(ExitCode::from(exit_code_for_kind(ErrorKind::Conflict)))
        }
        yinyang::core::CommitOutcome::Retryable => {
            eprintln!(
                "yy: publication did not commit; retry the frozen request with a durable publication journal"
            );
            Ok(ExitCode::from(exit_code_for_kind(ErrorKind::Retryable)))
        }
        yinyang::core::CommitOutcome::Unknown(_) => {
            eprintln!(
                "yy: publication outcome is unknown; query `yy receipt {commit_id}`; an absent receipt does not prove failure"
            );
            Ok(ExitCode::from(exit_code_for_kind(ErrorKind::Unknown)))
        }
    }
}

async fn retry(fs: &Fs, state: &Path, commit_id: Uuid) -> CliResult<ExitCode> {
    let journal = DirectoryJournal::open(fs, state).await?;
    publish(&journal, CommitId::from_bytes(*commit_id.as_bytes())).await
}

async fn export(fs: &Fs, state: &Path, commit_id: Uuid, destination: &Path) -> CliResult<ExitCode> {
    let journal = DirectoryJournal::open(fs, state).await?;
    let id = CommitId::from_bytes(*commit_id.as_bytes());
    let status = journal.status(id).await?.ok_or_else(|| {
        CliError::invalid(
            CliStage::Publication,
            format!(
                "publication intent {commit_id} is not present in {}",
                state.display()
            ),
        )
    })?;
    if !status.can_export() {
        return Err(CliError::invalid(
            CliStage::Publication,
            format!(
                "publication intent {commit_id} cannot be exported from state {}",
                state.display()
            ),
        ));
    }
    journal.export(id, destination).await?;
    println!(
        "exported original publication {commit_id} to {}",
        destination.display()
    );
    Ok(ExitCode::SUCCESS)
}

async fn classify_journal_error(
    journal: &DirectoryJournal,
    commit_id: CommitId,
    error: Error,
) -> CliResult<ExitCode> {
    let status = journal.status(commit_id).await.ok().flatten();
    if let Some(status) = status {
        match &status.state {
            DirectoryState::Committed(receipt) => {
                print_receipt(receipt);
                return Ok(ExitCode::SUCCESS);
            }
            DirectoryState::Unknown => {
                eprintln!(
                    "yy: {error}; publication outcome is unknown, query `yy receipt {}`; an absent receipt does not prove failure",
                    commit_uuid(commit_id)
                );
                return Ok(ExitCode::from(exit_code_for_kind(ErrorKind::Unknown)));
            }
            DirectoryState::Conflict | DirectoryState::Retryable => {
                eprintln!("yy: {error}");
                return Ok(ExitCode::from(exit_code_for_kind(ErrorKind::Conflict)));
            }
            DirectoryState::Prepared | DirectoryState::Failed => {}
        }
    }
    match error.kind() {
        ErrorKind::Unknown => {
            eprintln!(
                "yy: {error}; query `yy receipt {}`; an absent receipt does not prove failure",
                commit_uuid(commit_id)
            );
            Ok(ExitCode::from(exit_code_for_kind(ErrorKind::Unknown)))
        }
        ErrorKind::Conflict | ErrorKind::Retryable => {
            eprintln!("yy: {error}");
            Ok(ExitCode::from(exit_code_for_kind(error.kind())))
        }
        _ => Err(error.into()),
    }
}

fn project_inspection(
    inspection: &DirectoryInspection,
    selected: Option<Uuid>,
) -> CliResult<InspectionDocument> {
    let mut intents = inspection
        .intents
        .iter()
        .filter(|status| {
            selected
                .is_none_or(|id| status.intent.commit_id == CommitId::from_bytes(*id.as_bytes()))
        })
        .map(project_status)
        .collect::<Vec<_>>();
    if intents.is_empty()
        && let Some(selected) = selected
    {
        return Err(CliError::invalid(
            CliStage::Publication,
            format!(
                "publication intent {} is not present in the journal",
                selected
            ),
        ));
    }
    intents.sort_by(|left, right| left.commit_id.cmp(&right.commit_id));
    Ok(InspectionDocument {
        schema: SCHEMA,
        filesystem: hex(inspection.filesystem.as_bytes()),
        root: hex(inspection.root.as_bytes()),
        intents,
    })
}

fn project_status(status: &DirectoryStatus) -> IntentDocument {
    let (state, receipt) = match &status.state {
        DirectoryState::Prepared => ("prepared", None),
        DirectoryState::Unknown => ("unknown", None),
        DirectoryState::Committed(receipt) => ("committed", Some(receipt)),
        DirectoryState::Conflict => ("conflict", None),
        DirectoryState::Retryable => ("retryable", None),
        DirectoryState::Failed => ("failed", None),
    };
    IntentDocument {
        commit_id: commit_uuid(status.intent.commit_id),
        baseline: revision(status.intent.baseline.to_bytes()),
        request_digest: hex(&status.intent.request_digest),
        source: status.intent.source.display().to_string(),
        state,
        receipt: receipt.map(|receipt| ReceiptDocument {
            revision: revision(receipt.cursor.revision.to_bytes()),
            ordinal: receipt.cursor.ordinal,
        }),
        failure: status.failure.as_ref().map(|failure| FailureDocument {
            kind: error_kind(failure.kind()),
            message: failure.message().to_owned(),
            commit_id: failure.commit_id().map(commit_uuid),
        }),
        can_retry: status.can_retry(),
        can_export: status.can_export(),
    }
}

fn print_inspection(document: &InspectionDocument) {
    println!(
        "filesystem: {}\nroot: {}\nintents: {}",
        document.filesystem,
        document.root,
        document.intents.len()
    );
    for intent in &document.intents {
        println!(
            "publication: {}\n  state: {}\n  source: {}\n  baseline: {}\n  request digest: {}\n  retry: {}\n  export: {}",
            intent.commit_id,
            intent.state,
            intent.source,
            intent.baseline,
            intent.request_digest,
            intent.can_retry,
            intent.can_export
        );
        if let Some(receipt) = &intent.receipt {
            println!(
                "  receipt revision: {}\n  receipt ordinal: {}",
                receipt.revision, receipt.ordinal
            );
        }
        if let Some(failure) = &intent.failure {
            println!("  failure [{}]: {}", failure.kind, failure.message);
        }
    }
}

fn print_receipt(receipt: &Receipt) {
    println!(
        "commit: {}\nrevision: {}\nordinal: {}\nrequest digest: {}",
        commit_uuid(receipt.commit_id),
        revision(receipt.cursor.revision.to_bytes()),
        receipt.cursor.ordinal,
        hex(&receipt.request_digest)
    );
}

fn commit_uuid(id: CommitId) -> String {
    Uuid::from_bytes(*id.as_bytes()).to_string()
}

fn revision(bytes: [u8; 24]) -> String {
    hex(&bytes)
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn error_kind(kind: ErrorKind) -> &'static str {
    match kind {
        ErrorKind::Invalid => "invalid",
        ErrorKind::InvalidName => "invalid_name",
        ErrorKind::NotFound => "not_found",
        ErrorKind::AlreadyExists => "already_exists",
        ErrorKind::NotDirectory => "not_directory",
        ErrorKind::IsDirectory => "is_directory",
        ErrorKind::NotEmpty => "not_empty",
        ErrorKind::ReadOnly => "read_only",
        ErrorKind::PermissionDenied => "permission_denied",
        ErrorKind::Busy => "busy",
        ErrorKind::Closed => "closed",
        ErrorKind::Frozen => "frozen",
        ErrorKind::TooLarge => "too_large",
        ErrorKind::Conflict => "conflict",
        ErrorKind::Retryable => "retryable",
        ErrorKind::Unknown => "unknown",
        ErrorKind::Corrupt => "corrupt",
        ErrorKind::Unsupported => "unsupported",
        ErrorKind::Storage => "storage",
        ErrorKind::Io => "io",
        ErrorKind::NoSpace => "no_space",
        ErrorKind::Unclassified => "unclassified",
    }
}
