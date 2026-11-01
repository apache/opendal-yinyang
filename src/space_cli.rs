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
use clap::Subcommand;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use yinyang::core::{ExpiryId, Revision};
use yinyang::space::{
    ExpiryReportState, ListingConsistency, ManagedExpiryReport, ManagedRetentionPlan,
    ManagedSpaceReport, MeasurementState, StagingState,
};
use yinyang::volume::{Model, Volume, VolumeConfig};

#[derive(Subcommand)]
pub enum SpaceCommand {
    /// Report logical, physical-object, and local-staging measurements.
    Report {
        /// Emit the versioned `yinyang.managed-space.report.v3` document.
        #[arg(long)]
        json: bool,
    },
    /// Plan or explicitly expire retained history without physical deletion.
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
        /// Emit the versioned `yinyang.managed-retention.plan.v3` document.
        #[arg(long)]
        json: bool,
    },
    /// Explicitly expire selected historical revisions after protection checks.
    Expire {
        /// Historical revision to expire; repeat for an atomic batch.
        #[arg(long = "revision", required = true)]
        revision: Vec<RevisionArg>,
        /// Protect revisions referenced by an explicit validated backup package.
        #[arg(long = "backup")]
        backup: Vec<PathBuf>,
        /// Reuse an operation identity only when retrying the exact same command.
        #[arg(long)]
        operation_id: Option<uuid::Uuid>,
        /// Emit the versioned `yinyang.managed-retention.expiry.v1` document.
        #[arg(long)]
        json: bool,
    },
}

pub async fn run(
    path: &Path,
    command: SpaceCommand,
) -> Result<ExitCode, Box<dyn std::error::Error>> {
    let bytes = tokio::fs::read(path).await?;
    let mut config = VolumeConfig::from_json(&bytes)?;
    if config.staging.is_relative() && !config.staging.as_os_str().is_empty() {
        config.staging = path
            .parent()
            .unwrap_or(Path::new("."))
            .join(&config.staging);
    }
    if config.model != Model::Managed {
        return Err(
            "space inspection supports Managed volumes only; Direct namespace objects are excluded"
                .into(),
        );
    }
    let operator = operator()?;
    config.validate(&operator)?;
    let token = std::env::var("YINYANG_SERVICE_TOKEN").ok();
    let authority = Volume::connect(&config, operator.clone(), token).await?;
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
        SpaceCommand::Retention {
            command:
                RetentionCommand::Expire {
                    revision,
                    backup,
                    operation_id,
                    json,
                },
        } => {
            let revisions = revision
                .into_iter()
                .map(|revision| revision.0)
                .collect::<Vec<_>>();
            let operation_id = operation_id
                .map(|id| ExpiryId::from_bytes(*id.as_bytes()))
                .unwrap_or_else(ExpiryId::generate);
            let report = yinyang::space::expire_managed_retention(
                authority.as_ref(),
                &config.staging,
                &revisions,
                &backup,
                operation_id,
            )
            .await?;
            if json {
                println!("{}", serde_json::to_string_pretty(&report)?);
            } else {
                print_expiry(&report);
            }
            let code = match report.state {
                ExpiryReportState::Committed | ExpiryReportState::AlreadyExpired => 0,
                ExpiryReportState::Refused | ExpiryReportState::Conflict => 2,
                ExpiryReportState::Retryable | ExpiryReportState::Unknown => 3,
            };
            return Ok(ExitCode::from(code));
        }
    }
    Ok(ExitCode::SUCCESS)
}

fn operator() -> Result<opendal::Operator, opendal::Error> {
    let config = std::env::vars()
        .filter_map(|(key, value)| {
            key.strip_prefix("YINYANG_S3_")
                .map(|key| (key.to_ascii_lowercase(), value))
        })
        .collect::<Vec<_>>();
    let operator = opendal::Operator::via_iter("s3", config)?;
    #[cfg(feature = "benchmark-telemetry")]
    let operator = yinyang::benchmark::instrument(operator);
    Ok(operator)
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
        "managed sync: {:?} ({} content edits, {} namespace intents: prepared {} unknown {} committed {} conflict {} retryable {} failed {})",
        report.staging.sync.state,
        report.staging.sync.content_edits,
        report.staging.sync.namespace_intents,
        report.staging.sync.namespace_prepared,
        report.staging.sync.namespace_unknown,
        report.staging.sync.namespace_committed,
        report.staging.sync.namespace_conflict,
        report.staging.sync.namespace_retryable,
        report.staging.sync.namespace_failed
    );
    println!(
        "replica: {:?} pending-installs {} artifacts {} conflicts {}; baseline {}",
        report.staging.replica.state,
        report
            .staging
            .replica
            .pending_installs
            .map(|value| value.to_string())
            .unwrap_or_else(|| "unknown".to_owned()),
        report
            .staging
            .replica
            .artifacts
            .map(|value| value.to_string())
            .unwrap_or_else(|| "unknown".to_owned()),
        report
            .staging
            .replica
            .conflicts
            .map(|value| value.to_string())
            .unwrap_or_else(|| "unknown".to_owned()),
        match &report.staging.replica.baseline {
            Some(baseline) => format!(
                "{} (+{} entry revisions)",
                baseline.revision,
                baseline.entry_revisions.len()
            ),
            None => report.staging.replica.baseline_coverage.to_owned(),
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
        "protected sync evidence: namespace intents {} (prepared {} unknown {} committed {} conflict {} retryable {} failed {})",
        plan.protection.staging.sync.namespace_intents,
        plan.protection.staging.sync.namespace_prepared,
        plan.protection.staging.sync.namespace_unknown,
        plan.protection.staging.sync.namespace_committed,
        plan.protection.staging.sync.namespace_conflict,
        plan.protection.staging.sync.namespace_retryable,
        plan.protection.staging.sync.namespace_failed
    );
    println!(
        "replica evidence: pending-installs {} artifacts {} conflicts {}; baseline {}",
        plan.protection
            .staging
            .replica
            .pending_installs
            .map(|value| value.to_string())
            .unwrap_or_else(|| "unknown".to_owned()),
        plan.protection
            .staging
            .replica
            .artifacts
            .map(|value| value.to_string())
            .unwrap_or_else(|| "unknown".to_owned()),
        plan.protection
            .staging
            .replica
            .conflicts
            .map(|value| value.to_string())
            .unwrap_or_else(|| "unknown".to_owned()),
        match &plan.protection.staging.replica.baseline {
            Some(baseline) => format!(
                "{} (+{} entry revisions)",
                baseline.revision,
                baseline.entry_revisions.len()
            ),
            None => plan.protection.staging.replica.baseline_coverage.to_owned(),
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
        "outside-policy revisions remain readable; --keep is not a persistent pin; planning never expires history"
    );
    if plan.protection.staging.state == StagingState::Unavailable {
        println!("local staging evidence is unavailable; do not infer that no pending work exists");
    }
}

fn print_expiry(report: &ManagedExpiryReport) {
    println!(
        "retention expiry: filesystem {} revision {} operation {}",
        report.filesystem, report.observed_revision, report.operation_id
    );
    println!(
        "state: {}{}",
        expiry_state_name(report.state),
        report
            .after_revision
            .as_deref()
            .map(|revision| format!("; after revision {revision}"))
            .unwrap_or_default()
    );
    for revision in &report.revisions {
        println!(
            "revision {}: {} [{}]",
            revision.revision,
            expiry_decision_name(revision.state),
            revision.reasons.join(",")
        );
    }
    println!(
        "protected staging: binding {:?} pending {} frozen {} conflict {} unknown {} failed {}",
        report.staging.binding.state,
        report.staging.pending,
        report.staging.frozen,
        report.staging.conflict,
        report.staging.unknown,
        report.staging.failed
    );
    println!(
        "backup packages: {} supplied, {} validated",
        report.backups.supplied, report.backups.validated
    );
    for unresolved in &report.unresolved {
        println!(
            "unresolved {} ({}): {}",
            unresolved.subject, unresolved.state, unresolved.detail
        );
    }
    println!(
        "external readers: {}; untracked pins: {}; quiescence: {}",
        report.external_readers, report.untracked_pins, report.quiescence
    );
    println!("evidence digest: {}", report.evidence_digest);
    println!(
        "physical cleanup: {}; reclaimable bytes: unknown; deletion authorized: no",
        report.physical_cleanup
    );
    match report.state {
        ExpiryReportState::Committed => {}
        ExpiryReportState::AlreadyExpired => {
            println!("all requested revisions were already expired; no new mutation was issued");
        }
        ExpiryReportState::Refused | ExpiryReportState::Conflict => {
            println!("no new revision was expired");
        }
        ExpiryReportState::Retryable => {
            println!("expiry is retryable; no physical cleanup is implied");
        }
        ExpiryReportState::Unknown => {
            println!(
                "expiry outcome is unknown; retry the exact command with --operation-id {}",
                report.operation_id
            );
        }
    }
}

fn expiry_state_name(state: ExpiryReportState) -> &'static str {
    match state {
        ExpiryReportState::Committed => "committed",
        ExpiryReportState::AlreadyExpired => "already_expired",
        ExpiryReportState::Refused => "refused",
        ExpiryReportState::Conflict => "conflict",
        ExpiryReportState::Retryable => "retryable",
        ExpiryReportState::Unknown => "unknown",
    }
}

fn expiry_decision_name(state: yinyang::space::ExpiryDecisionState) -> &'static str {
    use yinyang::space::ExpiryDecisionState;
    match state {
        ExpiryDecisionState::Eligible => "eligible",
        ExpiryDecisionState::Expired => "expired",
        ExpiryDecisionState::AlreadyExpired => "already_expired",
        ExpiryDecisionState::Current => "current",
        ExpiryDecisionState::Missing => "missing",
        ExpiryDecisionState::Protected => "protected",
        ExpiryDecisionState::ProtectedMissing => "protected_missing",
        ExpiryDecisionState::ProtectedExpired => "protected_expired",
    }
}
