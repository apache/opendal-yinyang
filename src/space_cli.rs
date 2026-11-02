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

//! Read-only Managed space and retention CLI.

use crate::RevisionArg;
use crate::{CliError, CliResult, CliStage};
use clap::Subcommand;
use std::path::PathBuf;
use std::process::ExitCode;
use yinyang::core::Revision;
use yinyang::space::{
    ListingConsistency, ManagedRetentionPlan, ManagedSpaceReport, MeasurementState, StagingState,
};
use yinyang::volume::{Model, VolumeConfig, VolumeContext};

#[derive(Subcommand)]
pub enum SpaceCommand {
    /// Report logical, physical-object, and local-staging measurements.
    Report {
        /// Emit the versioned `yinyang.managed-space.report.v4` document.
        #[arg(long)]
        json: bool,
    },
    /// Build an explicit declarative retention selection without deleting data.
    Retention {
        #[command(subcommand)]
        command: RetentionCommand,
    },
}

#[derive(Subcommand)]
pub enum RetentionCommand {
    /// Plan protected revisions and bounded history selection.
    Plan {
        /// Keep this many newest retained revisions in the bounded selection.
        #[arg(long)]
        keep_last: Option<u64>,
        /// Protect one retained revision; repeat for additional pins.
        #[arg(long = "keep")]
        keep: Vec<RevisionArg>,
        /// Protect revisions referenced by an explicit validated backup package.
        #[arg(long = "backup")]
        backup: Vec<PathBuf>,
        /// Emit the versioned `yinyang.managed-retention.plan.v4` document.
        #[arg(long)]
        json: bool,
    },
}

pub async fn run(context: VolumeContext, command: SpaceCommand) -> CliResult<ExitCode> {
    let config = &context.config;
    if config.model == Model::Direct {
        return run_sync_space(config, command).await;
    }
    let operator = context.operator()?;
    let authority = context
        .connect_authority()
        .await
        .map_err(|error| CliError::from_runtime(CliStage::Connect, error))?;
    match command {
        SpaceCommand::Report { json } => {
            let report = yinyang::space::inspect_managed_space(
                authority.as_ref(),
                &operator,
                &config.staging,
            )
            .await?;
            if json {
                println!("{}", serde_json::to_string_pretty(&report)?);
            } else {
                print_report(&report);
            }
        }
        SpaceCommand::Retention {
            command:
                RetentionCommand::Plan {
                    keep_last,
                    keep,
                    backup,
                    json,
                },
        } => {
            let pins = keep
                .into_iter()
                .map(|revision| revision.0)
                .collect::<Vec<Revision>>();
            let plan = yinyang::space::plan_managed_retention(
                authority.as_ref(),
                &config.staging,
                keep_last,
                &pins,
                &backup,
            )
            .await?;
            if json {
                println!("{}", serde_json::to_string_pretty(&plan)?);
            } else {
                print_plan(&plan);
            }
        }
    }
    Ok(ExitCode::SUCCESS)
}

async fn run_sync_space(config: &VolumeConfig, command: SpaceCommand) -> CliResult<ExitCode> {
    match command {
        SpaceCommand::Report { json } => {
            let report = yinyang::space::inspect_sync_space(&config.staging).await;
            if json {
                println!("{}", serde_json::to_string_pretty(&report)?);
            } else {
                print_sync_space(&report.staging);
            }
        }
        SpaceCommand::Retention {
            command: RetentionCommand::Plan { json, .. },
        } => {
            let plan = yinyang::space::plan_sync_retention(&config.staging).await;
            if json {
                println!("{}", serde_json::to_string_pretty(&plan)?);
            } else {
                print_sync_retention(&plan);
            }
        }
    }
    Ok(ExitCode::SUCCESS)
}

fn print_sync_space(staging: &yinyang::space::StagingSpace) {
    println!(
        "sync space: {} state {:?} files {} apparent bytes {}",
        staging.path,
        staging.state,
        staging
            .directory
            .as_ref()
            .map(|directory| directory.regular_files)
            .unwrap_or(0),
        staging
            .directory
            .as_ref()
            .map(|directory| directory.apparent_bytes)
            .unwrap_or(0),
    );
    println!(
        "sync engine: {:?} pending-installs {} artifacts {} conflicts {}",
        staging.engine.state,
        staging.engine.pending_installs.unwrap_or_default(),
        staging.engine.artifacts.unwrap_or_default(),
        staging.engine.conflicts.unwrap_or_default(),
    );
    println!("protected revisions: {}", staging.protected_revisions.len());
}

fn print_sync_retention(plan: &yinyang::space::SyncRetentionPlan) {
    println!(
        "sync retention: state {:?}, {} protected revisions, deletion authorized: no",
        plan.staging.state,
        plan.staging.revisions.len()
    );
    println!("reclaimable bytes: unknown; no physical cleanup claim is made");
}

fn print_report(report: &ManagedSpaceReport) {
    println!(
        "managed space: filesystem {} revision {}",
        report.filesystem, report.observed_revision
    );
    println!(
        "logical live: files {} directories {} bytes {} unique-content {} objects / {} bytes",
        report.logical.live.files,
        report.logical.live.directories,
        report.logical.live.file_bytes,
        report.logical.live.unique_content_count,
        report.logical.live.unique_content_bytes
    );
    match report.logical.retained.state {
        MeasurementState::Available | MeasurementState::Partial => println!(
            "logical retained: {} revisions observed, {} bytes across snapshots, {} unique-content bytes",
            report.logical.retained.observed_revisions.len(),
            report.logical.retained.observed_file_bytes,
            report.logical.retained.observed_unique_content_bytes
        ),
        MeasurementState::Unavailable => println!(
            "logical retained: unavailable ({})",
            report
                .logical
                .retained
                .error
                .as_deref()
                .unwrap_or("authority does not expose retained revisions")
        ),
    }
    match (&report.physical.packs, &report.physical.head) {
        (Some(packs), Some(head)) => println!(
            "physical current objects: {} packs / {} bytes plus head {} bytes; {} referenced coverage bytes; noncurrent versions unknown",
            packs.objects,
            packs.bytes,
            head.bytes,
            report.physical.coverage.observed_referenced_bytes
        ),
        _ => println!(
            "physical current objects: {}; noncurrent versions unknown",
            report.physical.error.as_deref().unwrap_or("unavailable")
        ),
    }
    if report.physical.listing_consistency != ListingConsistency::StableHead {
        println!(
            "physical listing consistency: {:?}",
            report.physical.listing_consistency
        );
    }
    println!(
        "physical reachability: {:?}; {} retained revisions, {} snapshots, {} metadata pages, {} metadata values, {} content indexes, {} content extents, {} shared packs",
        report.physical.reachability.state,
        report
            .physical
            .reachability
            .retained_revisions
            .map(|value| value.to_string())
            .unwrap_or_else(|| "unknown".to_owned()),
        report.physical.reachability.snapshot_descriptors,
        report.physical.reachability.metadata_index_references,
        report.physical.reachability.metadata_value_references,
        report.physical.reachability.content_index_references,
        report.physical.reachability.content_extent_references,
        report.physical.reachability.shared_packs
    );
    match (
        report.physical.coverage.visible_referenced_bytes,
        report.physical.coverage.visible_uncovered_bytes,
    ) {
        (Some(referenced), Some(uncovered)) => println!(
            "physical visible coverage: {} referenced bytes; {} uncovered bytes (not reclaimable)",
            referenced, uncovered
        ),
        _ => println!("physical visible coverage: unavailable"),
    }
    match (
        report.physical.root_external.state,
        report.physical.root_external.packs,
        report.physical.root_external.bytes,
    ) {
        (MeasurementState::Available, Some(packs), Some(bytes)) => println!(
            "observed-root-external visible packs: {} / {} bytes; relative to observed roots only",
            packs, bytes
        ),
        _ => println!(
            "observed-root-external visible packs: partial or unavailable; no orphan claim"
        ),
    }
    println!(
        "local staging: {:?} binding {:?} at {} ({} handles, {} staged bytes)",
        report.staging.state,
        report.staging.binding.state,
        report.staging.path,
        report.staging.handles.total,
        report.staging.handles.handle_bytes
    );
    if let Some(directory) = &report.staging.directory {
        println!(
            "local staging files: {} regular files, {} apparent bytes, {} symlinks skipped",
            directory.regular_files, directory.apparent_bytes, directory.symlinks_skipped
        );
    }
    println!(
        "managed sync: {:?} ({} content intents: prepared {} unknown {} committed {} conflict {} retryable {} failed {})",
        report.staging.sync.state,
        report.staging.sync.intents,
        report.staging.sync.prepared,
        report.staging.sync.unknown,
        report.staging.sync.committed,
        report.staging.sync.conflict,
        report.staging.sync.retryable,
        report.staging.sync.failed
    );
    println!(
        "sync engine: {:?} pending-installs {} artifacts {} conflicts {}; baseline {}",
        report.staging.engine.state,
        report
            .staging
            .engine
            .pending_installs
            .map(|value| value.to_string())
            .unwrap_or_else(|| "unknown".to_owned()),
        report
            .staging
            .engine
            .artifacts
            .map(|value| value.to_string())
            .unwrap_or_else(|| "unknown".to_owned()),
        report
            .staging
            .engine
            .conflicts
            .map(|value| value.to_string())
            .unwrap_or_else(|| "unknown".to_owned()),
        match &report.staging.engine.baseline {
            Some(baseline) => format!(
                "{} baseline revisions (+{} receipt revisions)",
                baseline.revisions.len(),
                baseline.receipt_revisions.len()
            ),
            None => report.staging.engine.baseline_coverage.to_owned(),
        }
    );
    for unresolved in &report.unresolved {
        println!(
            "unresolved {} ({}): {}",
            unresolved.subject, unresolved.state, unresolved.detail
        );
    }
    println!("deletion authorized: no");
    println!("reclaimable bytes: unknown; no physical cleanup claim is made");
}

fn print_plan(plan: &ManagedRetentionPlan) {
    println!(
        "retention plan: filesystem {} revision {}",
        plan.filesystem, plan.observed_revision
    );
    println!(
        "policy: {} keep-last {} requested pins {}",
        plan.policy.mode,
        plan.policy
            .keep_last
            .map(|value| value.to_string())
            .unwrap_or_else(|| "not-set".to_owned()),
        plan.policy.requested_pins.len()
    );
    println!("published history: {}", plan.policy.published_history);
    let selected = plan
        .revisions
        .iter()
        .filter(|revision| revision.selected)
        .count();
    println!(
        "selection: {selected} selected of {} observed revisions ({:?})",
        plan.revisions.len(),
        plan.state
    );
    if plan.retained_revision_count.is_none() {
        println!(
            "retained revision index: unavailable; the plan does not infer that observed history is empty"
        );
    }
    for revision in &plan.revisions {
        println!(
            "revision {}: {} [{}]",
            revision.revision,
            if revision.selected {
                "selected"
            } else {
                "outside-policy"
            },
            revision.reasons.join(",")
        );
    }
    println!(
        "protected staging: binding {:?} pending {} frozen {} conflict {} unknown {} failed {} ledger-errors {}",
        plan.protection.staging.binding.state,
        plan.protection.staging.pending,
        plan.protection.staging.frozen,
        plan.protection.staging.conflict,
        plan.protection.staging.unknown,
        plan.protection.staging.failed,
        plan.protection.staging.ledger_errors
    );
    println!(
        "protected sync evidence: content intents {} (prepared {} unknown {} committed {} conflict {} retryable {} failed {})",
        plan.protection.staging.sync.intents,
        plan.protection.staging.sync.prepared,
        plan.protection.staging.sync.unknown,
        plan.protection.staging.sync.committed,
        plan.protection.staging.sync.conflict,
        plan.protection.staging.sync.retryable,
        plan.protection.staging.sync.failed
    );
    println!(
        "sync engine evidence: pending-installs {} artifacts {} conflicts {}; baseline {}",
        plan.protection
            .staging
            .engine
            .pending_installs
            .map(|value| value.to_string())
            .unwrap_or_else(|| "unknown".to_owned()),
        plan.protection
            .staging
            .engine
            .artifacts
            .map(|value| value.to_string())
            .unwrap_or_else(|| "unknown".to_owned()),
        plan.protection
            .staging
            .engine
            .conflicts
            .map(|value| value.to_string())
            .unwrap_or_else(|| "unknown".to_owned()),
        match &plan.protection.staging.engine.baseline {
            Some(baseline) => format!(
                "{} baseline revisions (+{} receipt revisions)",
                baseline.revisions.len(),
                baseline.receipt_revisions.len()
            ),
            None => plan.protection.staging.engine.baseline_coverage.to_owned(),
        }
    );
    println!(
        "backup packages: {} supplied, {} validated ({})",
        plan.protection.backups.supplied,
        plan.protection.backups.validated,
        plan.protection.backup_evidence
    );
    for package in &plan.protection.backups.packages {
        println!(
            "backup {}: {:?} filesystem {} protected-revisions {}",
            package.path,
            package.state,
            package.filesystem.as_deref().unwrap_or("unknown"),
            package
                .staging
                .as_ref()
                .map(|staging| staging.revisions.len().to_string())
                .unwrap_or_else(|| "0".to_owned())
        );
    }
    println!(
        "receipt evidence: {:?} ({} records / {} revisions)",
        plan.protection.receipts.state,
        plan.protection
            .receipts
            .records
            .map(|value| value.to_string())
            .unwrap_or_else(|| "unknown".to_owned()),
        plan.protection
            .receipts
            .revisions
            .map(|value| value.to_string())
            .unwrap_or_else(|| "unknown".to_owned())
    );
    println!(
        "external readers: {}; untracked pins: {}; backup evidence: {}; quiescence: {}",
        plan.protection.external_readers,
        plan.protection.untracked_pins,
        plan.protection.backup_evidence,
        plan.protection.quiescence
    );
    for unresolved in &plan.unresolved {
        println!(
            "unresolved {} ({}): {}",
            unresolved.subject, unresolved.state, unresolved.detail
        );
    }
    println!("deletion authorized: no");
    println!("reclaimable bytes: unknown; selection is not a cleanup estimate");
    println!(
        "outside-policy revisions remain readable; --keep is not a persistent pin; no expiry command exists"
    );
    if plan.protection.staging.state == StagingState::Unavailable {
        println!("local staging evidence is unavailable; do not infer that no pending work exists");
    }
}
