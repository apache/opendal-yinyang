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

//! Read-only Managed space accounting and declarative retention planning.
//!
//! This module intentionally has no deletion path. It observes authority
//! metadata, current backend objects, and existing local staging state without
//! opening a writer, acquiring a runtime lease, or changing local databases.

#[cfg(target_os = "linux")]
use crate::runtime::StagingLease;
use crate::runtime::{ErrorKind as RuntimeErrorKind, Runtime};
#[cfg(target_os = "linux")]
use crate::sync::StateLease as SyncStateLease;
use crate::sync::{ContentStatus, IntentState, Sync};
#[cfg(target_os = "linux")]
use crate::sync_engine::{
    EngineInspection, StateLease as EngineStateLease, inspect_backup_package, inspect_protection,
    inspect_state,
};
use opendal::{EntryMode, Operator};
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use yinyang_core::reachability::PhysicalReferenceKind;
use yinyang_core::{
    Authority, CommitId, Error, ErrorKind, NodeId, NodeKind, Result, Revision, RevisionRecord,
    RevisionStatus, Snapshot,
};
#[cfg(target_os = "linux")]
use yinyang_core::{CommitOutcome, ExpiryRequest};

pub const SPACE_REPORT_SCHEMA: &str = "yinyang.managed-space.report.v5";
pub const RETENTION_PLAN_SCHEMA: &str = "yinyang.managed-retention.plan.v5";
pub const RETENTION_EXPIRY_SCHEMA: &str = "yinyang.managed-retention.expiry.v1";

const HEAD_KEY: &str = ".yinyang/head";
const PACK_ROOT: &str = ".yinyang/v2";
const SCAN_LIMIT: usize = 4096;

#[cfg(target_os = "linux")]
struct ExpiryLeases {
    _engine: EngineStateLease,
    _sync: SyncStateLease,
    _runtime: StagingLease,
}

#[cfg(target_os = "linux")]
impl ExpiryLeases {
    async fn acquire(staging: &Path) -> Result<Self> {
        let engine = EngineStateLease::acquire(staging)
            .map_err(|error| expiry_lease_error("Sync engine", error))?;
        let sync = SyncStateLease::acquire(staging)
            .await
            .map_err(|error| expiry_lease_error("Sync journal", error))?;
        let runtime = StagingLease::acquire(staging.to_owned())
            .await
            .map_err(|error| expiry_lease_error("runtime", error))?;
        Ok(Self {
            _engine: engine,
            _sync: sync,
            _runtime: runtime,
        })
    }
}

#[cfg(target_os = "linux")]
fn expiry_lease_error(subject: &'static str, error: impl std::fmt::Display) -> Error {
    Error::new(
        ErrorKind::Storage,
        "acquire expiry maintenance leases",
        format!("{subject} local-state lease is unavailable: {error}"),
    )
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MeasurementState {
    Available,
    Partial,
    Unavailable,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct Unresolved {
    pub subject: String,
    pub state: String,
    pub detail: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ManagedSpaceReport {
    pub schema: &'static str,
    pub filesystem: String,
    pub observed_revision: String,
    pub measurement: SpaceMeasurement,
    pub logical: LogicalSpace,
    pub physical: PhysicalSpace,
    pub staging: StagingSpace,
    pub unresolved: Vec<Unresolved>,
    pub deletion_authorized: bool,
    pub reclaimable_bytes: Option<u64>,
    pub limits: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct SyncSpaceReport {
    pub schema: &'static str,
    pub staging: StagingSpace,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct SyncRetentionPlan {
    pub schema: &'static str,
    pub staging: StagingProtection,
    pub unresolved: Vec<Unresolved>,
    pub deletion_authorized: bool,
    pub reclaimable_bytes: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct SpaceMeasurement {
    pub logical_live: &'static str,
    pub logical_retained: &'static str,
    pub physical_objects: &'static str,
    pub referenced_extents: &'static str,
    pub local_staging: &'static str,
    pub noncurrent_versions: &'static str,
    pub external_pins: &'static str,
    pub non_atomic_inventory: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct LogicalSpace {
    pub live: LogicalRevision,
    pub retained: RetainedLogicalSpace,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct LogicalRevision {
    pub revision: String,
    pub files: u64,
    pub directories: u64,
    pub file_bytes: u64,
    pub unique_content_count: u64,
    pub unique_content_bytes: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct RetainedLogicalSpace {
    pub state: MeasurementState,
    pub revision_count: Option<u64>,
    pub readable_revision_count: Option<u64>,
    pub expired_revision_count: Option<u64>,
    pub observed_revisions: Vec<LogicalRevision>,
    pub observed_file_bytes: u64,
    pub observed_unique_content_bytes: u64,
    pub error: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct PhysicalSpace {
    pub state: MeasurementState,
    pub head: Option<PhysicalObject>,
    pub packs: Option<PhysicalObject>,
    pub visible_objects: Option<u64>,
    pub visible_bytes: Option<u64>,
    pub before_revision: String,
    pub after_revision: Option<String>,
    pub listing_consistency: ListingConsistency,
    pub noncurrent_versions: &'static str,
    pub accounting: &'static str,
    pub reachability: PhysicalReachability,
    pub coverage: PhysicalCoverage,
    pub root_external: ObservedRootExternal,
    pub issues: Vec<PhysicalIssueReport>,
    pub error: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct PhysicalObject {
    pub objects: u64,
    pub bytes: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct PhysicalReachability {
    pub state: MeasurementState,
    pub retained_revisions: Option<u64>,
    pub snapshot_descriptors: u64,
    pub metadata_index_references: u64,
    pub metadata_value_references: u64,
    pub content_descriptors: u64,
    pub content_index_references: u64,
    pub content_extent_references: u64,
    pub packs_with_references: u64,
    pub shared_packs: u64,
    pub issues: u64,
    pub issues_omitted: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct PhysicalCoverage {
    pub observed_referenced_bytes: u64,
    pub visible_referenced_bytes: Option<u64>,
    pub visible_uncovered_bytes: Option<u64>,
    pub fully_referenced_visible_packs: Option<u64>,
    pub partially_referenced_visible_packs: Option<u64>,
    pub missing_reference_packs: u64,
    pub truncated_reference_packs: u64,
    pub payload_verification: &'static str,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ObservedRootExternal {
    pub state: MeasurementState,
    pub packs: Option<u64>,
    pub bytes: Option<u64>,
    pub treatment: &'static str,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct PhysicalIssueReport {
    pub subject: String,
    pub state: &'static str,
    pub detail: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ListingConsistency {
    StableHead,
    ChangedDuringInventory,
    Unverified,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct StagingSpace {
    pub path: String,
    pub state: StagingState,
    pub binding: StagingBinding,
    pub directory: Option<DirectoryUsage>,
    pub databases: Vec<StagingDatabase>,
    pub handles: StagingHandles,
    pub sync: SyncEvidence,
    pub engine: EngineEvidence,
    pub protected_revisions: Vec<ProtectedRevision>,
    pub frozen_commits: Vec<String>,
    pub errors: Vec<String>,
    #[serde(skip)]
    protected: BTreeMap<Revision, BTreeSet<&'static str>>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct StagingBinding {
    pub state: StagingBindingState,
    pub expected_filesystem: Option<String>,
    pub error: Option<String>,
}

impl StagingBinding {
    fn unbound() -> Self {
        Self {
            state: StagingBindingState::Unbound,
            expected_filesystem: None,
            error: None,
        }
    }

    fn verified(filesystem: NodeId) -> Self {
        Self {
            state: StagingBindingState::Verified,
            expected_filesystem: Some(identity_hex(filesystem)),
            error: None,
        }
    }

    fn unavailable(filesystem: NodeId, error: String) -> Self {
        Self {
            state: StagingBindingState::Unavailable,
            expected_filesystem: Some(identity_hex(filesystem)),
            error: Some(error),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StagingBindingState {
    Unbound,
    Verified,
    Unavailable,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StagingState {
    Missing,
    NotInitialized,
    Available,
    Partial,
    Unavailable,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct DirectoryUsage {
    pub regular_files: u64,
    pub apparent_bytes: u64,
    pub symlinks_skipped: u64,
    pub other_entries_skipped: u64,
    pub non_atomic: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct StagingDatabase {
    pub kind: &'static str,
    pub path: String,
    pub state: MeasurementState,
    pub error: Option<String>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
pub struct StagingHandles {
    pub total: u64,
    pub handle_bytes: u64,
    pub pending: u64,
    pub frozen: u64,
    pub conflict: u64,
    pub unknown: u64,
    pub failed: u64,
    pub ledger_errors: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct SyncEvidence {
    pub state: StagingState,
    pub intents: u64,
    pub prepared: u64,
    pub unknown: u64,
    pub committed: u64,
    pub conflict: u64,
    pub retryable: u64,
    pub failed: u64,
    pub commit_ids: Vec<String>,
    pub error: Option<String>,
}

impl SyncEvidence {
    fn missing() -> Self {
        Self {
            state: StagingState::Missing,
            intents: 0,
            prepared: 0,
            unknown: 0,
            committed: 0,
            conflict: 0,
            retryable: 0,
            failed: 0,
            commit_ids: Vec::new(),
            error: None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct EngineEvidence {
    pub state: StagingState,
    pub pending_installs: Option<u64>,
    pub artifacts: Option<u64>,
    pub exportable_artifacts: Option<u64>,
    pub conflicts: Option<u64>,
    pub baseline_coverage: &'static str,
    pub baseline: Option<EngineBaselineEvidence>,
    pub error: Option<String>,
}

impl EngineEvidence {
    fn missing() -> Self {
        Self {
            state: StagingState::Missing,
            pending_installs: None,
            artifacts: None,
            exportable_artifacts: None,
            conflicts: None,
            baseline_coverage: "not_observed",
            baseline: None,
            error: None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct EngineBaselineEvidence {
    pub revisions: Vec<String>,
    pub receipt_revisions: Vec<String>,
}

impl EngineBaselineEvidence {
    #[cfg(target_os = "linux")]
    fn from_inspection(inspection: &EngineInspection) -> Self {
        Self {
            revisions: inspection.baseline_revisions.clone(),
            receipt_revisions: inspection.intent_receipt_revisions.clone(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ProtectedRevision {
    pub revision: String,
    pub reasons: Vec<&'static str>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ManagedRetentionPlan {
    pub schema: &'static str,
    pub filesystem: String,
    pub observed_revision: String,
    pub state: MeasurementState,
    pub policy: RetentionPolicyReport,
    pub retained_revision_count: Option<u64>,
    pub readable_revision_count: Option<u64>,
    pub expired_revision_count: Option<u64>,
    pub revisions: Vec<RetentionRevision>,
    pub protection: RetentionProtection,
    pub unresolved: Vec<Unresolved>,
    pub deletion_authorized: bool,
    pub reclaimable_bytes: Option<u64>,
    pub limits: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct RetentionPolicyReport {
    pub mode: &'static str,
    pub keep_last: Option<u64>,
    pub requested_pins: Vec<RequestedPin>,
    pub published_history: &'static str,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct RequestedPin {
    pub revision: String,
    pub retained: Option<bool>,
    pub state: RequestedPinState,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RequestedPinState {
    Readable,
    Expired,
    NotRetained,
    Unknown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RetentionRevisionState {
    Readable,
    Expired,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct RetentionRevision {
    pub revision: String,
    pub state: RetentionRevisionState,
    pub selected: bool,
    pub reasons: Vec<&'static str>,
    pub files: Option<u64>,
    pub directories: Option<u64>,
    pub file_bytes: Option<u64>,
    pub unique_content_bytes: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct RetentionProtection {
    pub current_revision: String,
    pub staging: StagingProtection,
    pub backups: BackupProtection,
    pub receipts: ReceiptEvidence,
    pub backup_evidence: &'static str,
    pub external_readers: &'static str,
    pub untracked_pins: &'static str,
    pub quiescence: &'static str,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct BackupProtection {
    pub supplied: u64,
    pub validated: u64,
    pub packages: Vec<BackupPackageEvidence>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BackupPackageState {
    Validated,
    Partial,
    Foreign,
    Unavailable,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct BackupPackageEvidence {
    pub path: String,
    pub state: BackupPackageState,
    pub filesystem: Option<String>,
    pub staging: Option<StagingProtection>,
    pub error: Option<String>,
    #[serde(skip)]
    protected: BTreeMap<Revision, BTreeSet<&'static str>>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct StagingProtection {
    pub state: StagingState,
    pub binding: StagingBinding,
    pub pending: u64,
    pub frozen: u64,
    pub conflict: u64,
    pub unknown: u64,
    pub failed: u64,
    pub ledger_errors: u64,
    pub sync: SyncEvidence,
    pub engine: EngineEvidence,
    pub revisions: Vec<ProtectedRevision>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ReceiptEvidence {
    pub state: MeasurementState,
    pub records: Option<u64>,
    pub revisions: Option<u64>,
    pub treatment: &'static str,
    pub error: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ManagedExpiryReport {
    pub schema: &'static str,
    pub filesystem: String,
    pub operation_id: String,
    pub observed_revision: String,
    pub after_revision: Option<String>,
    pub state: ExpiryReportState,
    pub evidence_digest: String,
    pub staging: StagingProtection,
    pub backups: BackupProtection,
    pub revisions: Vec<ExpiryRevisionDecision>,
    pub unresolved: Vec<Unresolved>,
    pub external_readers: &'static str,
    pub untracked_pins: &'static str,
    pub quiescence: &'static str,
    pub deletion_authorized: bool,
    pub reclaimable_bytes: Option<u64>,
    pub physical_cleanup: &'static str,
    pub limits: Vec<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ExpiryReportState {
    Committed,
    AlreadyExpired,
    Refused,
    Conflict,
    Retryable,
    Unknown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ExpiryDecisionState {
    Eligible,
    Expired,
    AlreadyExpired,
    Current,
    Missing,
    Unavailable,
    Protected,
    ProtectedMissing,
    ProtectedExpired,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ExpiryRevisionDecision {
    pub revision: String,
    pub state: ExpiryDecisionState,
    pub reasons: Vec<&'static str>,
}

#[derive(Default)]
struct LogicalAccumulator {
    files: u64,
    directories: u64,
    file_bytes: u64,
    contents: BTreeSet<ContentKey>,
    unique_content_bytes: u64,
}

#[derive(Clone, Copy, Eq, Ord, PartialEq, PartialOrd)]
struct ContentKey {
    length: u64,
    digest: [u8; 32],
}

struct MeasuredSnapshot {
    logical: LogicalRevision,
    contents: BTreeSet<ContentKey>,
}

impl LogicalAccumulator {
    fn directory(&mut self) -> Result<()> {
        self.directories = checked_add(self.directories, 1, "directory count")?;
        Ok(())
    }

    fn content(&mut self, key: ContentKey) -> Result<()> {
        self.files = checked_add(self.files, 1, "file count")?;
        self.file_bytes = checked_add(self.file_bytes, key.length, "logical file bytes")?;
        if self.contents.insert(key) {
            self.unique_content_bytes = checked_add(
                self.unique_content_bytes,
                key.length,
                "unique content bytes",
            )?;
        }
        Ok(())
    }

    fn finish(self, revision: Revision) -> MeasuredSnapshot {
        MeasuredSnapshot {
            logical: LogicalRevision {
                revision: revision_hex(revision),
                files: self.files,
                directories: self.directories,
                file_bytes: self.file_bytes,
                unique_content_count: self.contents.len() as u64,
                unique_content_bytes: self.unique_content_bytes,
            },
            contents: self.contents,
        }
    }
}

/// Measure one authoritative latest revision, current visible objects, and the
/// configured local staging path. Missing or unsupported optional evidence is
/// reported in the document rather than replaced with zero.
pub async fn inspect_managed_space(
    authority: &dyn Authority,
    operator: &Operator,
    staging: &Path,
) -> Result<ManagedSpaceReport> {
    let snapshot = authority.observe_latest().await?;
    let live = measure_snapshot(&snapshot).await?;
    let mut unresolved = Vec::new();
    let retained = inspect_retained(authority, &snapshot, &mut unresolved).await;
    let physical = inspect_physical(authority, operator, &snapshot).await;
    for issue in &physical.issues {
        unresolved.push(Unresolved {
            subject: issue.subject.clone(),
            state: issue.state.to_owned(),
            detail: issue.detail.clone(),
        });
    }
    let staging = inspect_bound_local_staging(staging, authority.filesystem()).await;
    for error in &staging.errors {
        unresolved.push(Unresolved {
            subject: "local-staging".to_owned(),
            state: "unavailable".to_owned(),
            detail: error.clone(),
        });
    }
    if physical.listing_consistency == ListingConsistency::ChangedDuringInventory
        || physical.listing_consistency == ListingConsistency::Unverified
    {
        let state = match physical.listing_consistency {
            ListingConsistency::StableHead => unreachable!(),
            ListingConsistency::ChangedDuringInventory => "changed_during_inventory",
            ListingConsistency::Unverified => "unverified",
        };
        unresolved.push(Unresolved {
            subject: "physical-inventory".to_owned(),
            state: state.to_owned(),
            detail: "the recursive current-object listing is not an atomic snapshot".to_owned(),
        });
    }
    Ok(ManagedSpaceReport {
        schema: SPACE_REPORT_SCHEMA,
        filesystem: identity_hex(authority.filesystem()),
        observed_revision: revision_hex(snapshot.revision()),
        measurement: SpaceMeasurement {
            logical_live: "latest_authority_snapshot",
            logical_retained: "authority_revision_history_metadata",
            physical_objects: "recursive_current_object_listing",
            referenced_extents: "authenticated_index_references_without_payload_reads",
            local_staging: "existing_database_read_only_plus_directory_metadata",
            noncurrent_versions: "unknown",
            external_pins: "unknown",
            non_atomic_inventory: true,
        },
        logical: LogicalSpace {
            live: live.logical,
            retained,
        },
        physical,
        staging,
        unresolved,
        deletion_authorized: false,
        reclaimable_bytes: None,
        limits: space_limits(),
    })
}

async fn inspect_backup_packages(
    backup_packages: &[PathBuf],
    filesystem: NodeId,
) -> BackupProtection {
    let mut packages = Vec::with_capacity(backup_packages.len());
    for backup in backup_packages {
        packages.push(inspect_backup_package_evidence(backup, filesystem).await);
    }
    let supplied = u64::try_from(packages.len()).unwrap_or(u64::MAX);
    let validated = u64::try_from(
        packages
            .iter()
            .filter(|package| package.state == BackupPackageState::Validated)
            .count(),
    )
    .unwrap_or(u64::MAX);
    BackupProtection {
        supplied,
        validated,
        packages,
    }
}

async fn inspect_backup_package_evidence(
    backup: &Path,
    filesystem: NodeId,
) -> BackupPackageEvidence {
    let path = display_path(backup);
    #[cfg(not(target_os = "linux"))]
    {
        let _ = filesystem;
        unavailable_backup(
            &path,
            None,
            "backup package inspection is available only on Linux".to_owned(),
        )
    }
    #[cfg(target_os = "linux")]
    {
        let inspection = {
            let backup = backup.to_owned();
            tokio::task::spawn_blocking(move || inspect_backup_package(&backup)).await
        };
        let inspection = match inspection {
            Ok(Ok(inspection)) => inspection,
            Ok(Err(error)) => return unavailable_backup(&path, None, error.to_string()),
            Err(error) => {
                return unavailable_backup(&path, None, format!("inspect backup package: {error}"));
            }
        };
        let package_filesystem = if inspection.model == crate::sync_engine::SyncModel::Managed {
            inspection
                .namespace
                .split_once(':')
                .and_then(|(authority, _)| parse_node_id_hex(authority))
        } else {
            None
        };
        let filesystem_hex = package_filesystem.map(identity_hex);
        if package_filesystem != Some(filesystem) {
            let error = format!(
                "backup filesystem {:?} does not match volume filesystem {}",
                filesystem_hex,
                identity_hex(filesystem)
            );
            return BackupPackageEvidence {
                path,
                state: BackupPackageState::Foreign,
                filesystem: filesystem_hex,
                staging: None,
                error: Some(error),
                protected: BTreeMap::new(),
            };
        }
        let state_directory = backup.join("state");
        let mut staging = inspect_package_staging(&state_directory).await;
        staging.binding = StagingBinding::verified(filesystem);
        let state = match staging.state {
            StagingState::Available => BackupPackageState::Validated,
            StagingState::Partial => BackupPackageState::Partial,
            StagingState::Missing | StagingState::NotInitialized | StagingState::Unavailable => {
                BackupPackageState::Unavailable
            }
        };
        let error = (!staging.errors.is_empty()).then(|| staging.errors.join("; "));
        BackupPackageEvidence {
            path,
            state,
            filesystem: filesystem_hex,
            staging: Some(staging_protection(&staging)),
            error,
            protected: staging.protected,
        }
    }
}

fn unavailable_backup(
    path: &str,
    filesystem: Option<String>,
    error: String,
) -> BackupPackageEvidence {
    BackupPackageEvidence {
        path: path.to_owned(),
        state: BackupPackageState::Unavailable,
        filesystem,
        staging: None,
        error: Some(error),
        protected: BTreeMap::new(),
    }
}

fn staging_protection(staging: &StagingSpace) -> StagingProtection {
    StagingProtection {
        state: staging.state,
        binding: staging.binding.clone(),
        pending: staging.handles.pending,
        frozen: staging.handles.frozen,
        conflict: staging.handles.conflict,
        unknown: staging.handles.unknown,
        failed: staging.handles.failed,
        ledger_errors: staging.handles.ledger_errors,
        sync: staging.sync.clone(),
        engine: staging.engine.clone(),
        revisions: staging.protected_revisions.clone(),
    }
}

fn backup_reason(reason: &'static str) -> &'static str {
    match reason {
        "staging_base" => "backup_staging_base",
        "staging_pending" => "backup_staging_pending",
        "staging_frozen" => "backup_staging_frozen",
        "staging_unknown" => "backup_staging_unknown",
        "staging_conflict" => "backup_staging_conflict",
        "staging_failed" => "backup_staging_failed",
        "sync_intent_prepared" => "backup_sync_intent_prepared",
        "sync_intent_unknown" => "backup_sync_intent_unknown",
        "sync_intent_receipt" => "backup_sync_intent_receipt",
        "sync_intent_conflict" => "backup_sync_intent_conflict",
        "sync_intent_retryable" => "backup_sync_intent_retryable",
        "sync_intent_failed" => "backup_sync_intent_failed",
        "sync_receipt" => "backup_sync_receipt",
        "sync_engine_conflict" => "backup_sync_engine_conflict",
        "sync_engine_baseline" => "backup_sync_engine_baseline",
        "sync_engine_receipt" => "backup_sync_engine_receipt",
        _ => "backup_unclassified",
    }
}

fn backup_evidence_state(backups: &BackupProtection) -> &'static str {
    if backups.supplied == 0 {
        "not_supplied"
    } else if backups.validated == backups.supplied {
        "supplied_validated"
    } else {
        "supplied_incomplete"
    }
}

/// Build a bounded declarative retention selection over authoritative retained
/// revisions. The result never authorizes deletion and never estimates bytes
/// that physical cleanup could reclaim.
pub async fn plan_managed_retention(
    authority: &dyn Authority,
    staging: &Path,
    keep_last: Option<u64>,
    requested_pins: &[Revision],
    backup_packages: &[PathBuf],
) -> Result<ManagedRetentionPlan> {
    if keep_last == Some(0) {
        return Err(Error::new(
            ErrorKind::Invalid,
            "plan managed retention",
            "keep-last must be at least one",
        ));
    }
    let latest = authority.observe_latest().await?;
    let staging = inspect_bound_local_staging(staging, authority.filesystem()).await;
    let backups = inspect_backup_packages(backup_packages, authority.filesystem()).await;
    let mut unresolved = staging
        .errors
        .iter()
        .map(|detail| Unresolved {
            subject: "local-staging".to_owned(),
            state: "unavailable".to_owned(),
            detail: detail.clone(),
        })
        .collect::<Vec<_>>();
    for package in &backups.packages {
        if package.state == BackupPackageState::Validated {
            continue;
        }
        let state = match package.state {
            BackupPackageState::Validated => unreachable!(),
            BackupPackageState::Partial => "partial",
            BackupPackageState::Foreign => "foreign",
            BackupPackageState::Unavailable => "unavailable",
        };
        unresolved.push(Unresolved {
            subject: "backup-package".to_owned(),
            state: state.to_owned(),
            detail: format!(
                "{}: {}",
                package.path,
                package
                    .error
                    .as_deref()
                    .unwrap_or("package evidence is partial")
            ),
        });
    }
    let (history, retained_revision_count, readable_revision_count, expired_revision_count, state) =
        match authority.revision_history().await {
            Ok(mut records) => {
                records.sort_by_key(|record| record.revision);
                let mut complete = true;
                if !records
                    .iter()
                    .any(|record| record.revision == latest.revision())
                {
                    complete = false;
                    unresolved.push(Unresolved {
                        subject: "retained-revisions".to_owned(),
                        state: "latest-not-listed".to_owned(),
                        detail: revision_hex(latest.revision()),
                    });
                    records.push(RevisionRecord {
                        revision: latest.revision(),
                        status: RevisionStatus::Readable,
                    });
                    records.sort_by_key(|record| record.revision);
                }
                let history = records
                    .into_iter()
                    .map(|record| (record.revision, record.status))
                    .collect::<BTreeMap<_, _>>();
                let total = history.len() as u64;
                let readable = history
                    .values()
                    .filter(|status| **status == RevisionStatus::Readable)
                    .count() as u64;
                let expired = total.saturating_sub(readable);
                (
                    history,
                    Some(total),
                    Some(readable),
                    Some(expired),
                    if complete {
                        MeasurementState::Available
                    } else {
                        MeasurementState::Partial
                    },
                )
            }
            Err(error) => {
                unresolved.push(Unresolved {
                    subject: "retained-revisions".to_owned(),
                    state: "unavailable".to_owned(),
                    detail: error.to_string(),
                });
                (
                    BTreeMap::from([(latest.revision(), RevisionStatus::Readable)]),
                    None,
                    None,
                    None,
                    MeasurementState::Unavailable,
                )
            }
        };

    let revisions = history.keys().copied().collect::<Vec<_>>();
    let readable_revisions = history
        .iter()
        .filter(|(_, status)| **status == RevisionStatus::Readable)
        .map(|(revision, _)| *revision)
        .collect::<Vec<_>>();
    let pins = requested_pins.iter().copied().collect::<BTreeSet<_>>();
    let requested = requested_pins
        .iter()
        .map(|revision| RequestedPin {
            revision: revision_hex(*revision),
            retained: retained_revision_count.map(|_| history.contains_key(revision)),
            state: if retained_revision_count.is_none() {
                RequestedPinState::Unknown
            } else {
                match history.get(revision) {
                    Some(RevisionStatus::Readable) => RequestedPinState::Readable,
                    Some(RevisionStatus::Expired) => RequestedPinState::Expired,
                    None => RequestedPinState::NotRetained,
                }
            },
        })
        .collect::<Vec<_>>();
    if retained_revision_count.is_some() {
        for revision in requested_pins {
            match history.get(revision) {
                Some(RevisionStatus::Readable) => {}
                Some(RevisionStatus::Expired) => unresolved.push(Unresolved {
                    subject: "requested-pin".to_owned(),
                    state: "expired".to_owned(),
                    detail: revision_hex(*revision),
                }),
                None => unresolved.push(Unresolved {
                    subject: "requested-pin".to_owned(),
                    state: "not_retained".to_owned(),
                    detail: revision_hex(*revision),
                }),
            }
        }
        for revision in staging.protected.keys() {
            match history.get(revision) {
                Some(RevisionStatus::Readable) => {}
                Some(RevisionStatus::Expired) => unresolved.push(Unresolved {
                    subject: "local-staging-pin".to_owned(),
                    state: "protected-expired".to_owned(),
                    detail: revision_hex(*revision),
                }),
                None => unresolved.push(Unresolved {
                    subject: "local-staging-pin".to_owned(),
                    state: "not_retained".to_owned(),
                    detail: revision_hex(*revision),
                }),
            }
        }
    }

    let mut backup_protected = BTreeMap::<Revision, BTreeSet<&'static str>>::new();
    for package in &backups.packages {
        for (revision, reasons) in &package.protected {
            let entry = backup_protected.entry(*revision).or_default();
            for reason in reasons {
                entry.insert(backup_reason(reason));
            }
        }
    }
    if retained_revision_count.is_some() {
        for revision in backup_protected.keys() {
            match history.get(revision) {
                Some(RevisionStatus::Readable) => {}
                Some(RevisionStatus::Expired) => unresolved.push(Unresolved {
                    subject: "backup-pin".to_owned(),
                    state: "protected-expired".to_owned(),
                    detail: revision_hex(*revision),
                }),
                None => unresolved.push(Unresolved {
                    subject: "backup-pin".to_owned(),
                    state: "not_retained".to_owned(),
                    detail: revision_hex(*revision),
                }),
            }
        }
    }

    let bounded = keep_last.is_some() || !requested_pins.is_empty();
    let keep_last_set = keep_last
        .map(|count| {
            let count = usize::try_from(count).unwrap_or(usize::MAX);
            readable_revisions
                .iter()
                .copied()
                .skip(readable_revisions.len().saturating_sub(count))
                .collect::<BTreeSet<_>>()
        })
        .unwrap_or_default();
    let mut protected = staging.protected.clone();
    merge_protected(&mut protected, &backup_protected);
    let protected = &protected;
    let mut decisions = Vec::with_capacity(revisions.len());
    for revision in &revisions {
        if history.get(revision) == Some(&RevisionStatus::Expired) {
            decisions.push(RetentionRevision {
                revision: revision_hex(*revision),
                state: RetentionRevisionState::Expired,
                selected: false,
                reasons: vec!["expired"],
                files: None,
                directories: None,
                file_bytes: None,
                unique_content_bytes: None,
            });
            continue;
        }
        let reasons = selection_reasons(
            *revision,
            latest.revision(),
            bounded,
            &keep_last_set,
            &pins,
            protected,
        );
        let selected = reasons
            .iter()
            .any(|reason| *reason != "outside_bounded_policy");
        let measured = if *revision == latest.revision() {
            Some(measure_snapshot(&latest).await?)
        } else {
            match authority.observe_revision(*revision).await {
                Ok(snapshot) => match measure_snapshot(&snapshot).await {
                    Ok(measured) => Some(measured),
                    Err(error) => {
                        unresolved.push(Unresolved {
                            subject: "retained-logical".to_owned(),
                            state: "unavailable".to_owned(),
                            detail: format!("{}: {error}", revision_hex(*revision)),
                        });
                        None
                    }
                },
                Err(error) if error.kind() == ErrorKind::Expired => {
                    decisions.push(RetentionRevision {
                        revision: revision_hex(*revision),
                        state: RetentionRevisionState::Expired,
                        selected: false,
                        reasons: vec!["expired"],
                        files: None,
                        directories: None,
                        file_bytes: None,
                        unique_content_bytes: None,
                    });
                    continue;
                }
                Err(error) => {
                    unresolved.push(Unresolved {
                        subject: "retained-snapshot".to_owned(),
                        state: "unavailable".to_owned(),
                        detail: format!("{}: {error}", revision_hex(*revision)),
                    });
                    None
                }
            }
        };
        decisions.push(RetentionRevision {
            revision: revision_hex(*revision),
            state: RetentionRevisionState::Readable,
            selected,
            reasons,
            files: measured.as_ref().map(|value| value.logical.files),
            directories: measured.as_ref().map(|value| value.logical.directories),
            file_bytes: measured.as_ref().map(|value| value.logical.file_bytes),
            unique_content_bytes: measured
                .as_ref()
                .map(|value| value.logical.unique_content_bytes),
        });
    }

    let receipts = inspect_receipts(&latest, &mut unresolved).await;
    match authority.observe_latest().await {
        Ok(after) if after.revision() != latest.revision() => {
            unresolved.push(Unresolved {
                subject: "retention-observation".to_owned(),
                state: "changed_during_planning".to_owned(),
                detail: format!(
                    "{} -> {}",
                    revision_hex(latest.revision()),
                    revision_hex(after.revision())
                ),
            });
        }
        Ok(_) => {}
        Err(error) => unresolved.push(Unresolved {
            subject: "retention-observation".to_owned(),
            state: "unverified".to_owned(),
            detail: error.to_string(),
        }),
    }
    let state = if state == MeasurementState::Available && !unresolved.is_empty() {
        MeasurementState::Partial
    } else {
        state
    };
    let backup_evidence = backup_evidence_state(&backups);
    Ok(ManagedRetentionPlan {
        schema: RETENTION_PLAN_SCHEMA,
        filesystem: identity_hex(authority.filesystem()),
        observed_revision: revision_hex(latest.revision()),
        state,
        policy: RetentionPolicyReport {
            mode: if bounded { "bounded" } else { "keep_all" },
            keep_last,
            requested_pins: requested,
            published_history: "retained_by_default",
        },
        retained_revision_count,
        readable_revision_count,
        expired_revision_count,
        revisions: decisions,
        protection: RetentionProtection {
            current_revision: revision_hex(latest.revision()),
            staging: staging_protection(&staging),
            backups,
            receipts,
            backup_evidence,
            external_readers: "unknown",
            untracked_pins: "unknown",
            quiescence: "not_observed",
        },
        unresolved,
        deletion_authorized: false,
        reclaimable_bytes: None,
        limits: retention_limits(),
    })
}

/// Explicitly expire historical revisions after validating declared local and
/// supplied backup dependencies under their existing writer leases.
#[cfg(not(target_os = "linux"))]
pub async fn expire_managed_retention(
    _authority: &dyn Authority,
    _staging: &Path,
    _candidates: &[Revision],
    _backup_packages: &[PathBuf],
    _operation_id: CommitId,
) -> Result<ManagedExpiryReport> {
    Err(Error::new(
        ErrorKind::Unsupported,
        "expire managed retention",
        "logical history expiry requires the Linux local-state lease protocol",
    ))
}

/// Explicitly expire historical revisions after validating declared local and
/// supplied backup dependencies under their existing writer leases.
#[cfg(target_os = "linux")]
pub async fn expire_managed_retention(
    authority: &dyn Authority,
    staging_path: &Path,
    candidates: &[Revision],
    backup_packages: &[PathBuf],
    operation_id: CommitId,
) -> Result<ManagedExpiryReport> {
    let candidates = candidates.iter().copied().collect::<BTreeSet<_>>();
    if candidates.is_empty() {
        return Err(Error::new(
            ErrorKind::Invalid,
            "expire managed retention",
            "at least one revision is required",
        ));
    }
    if candidates.len() > yinyang_core::EXPIRY_CANDIDATES_MAX {
        return Err(Error::new(
            ErrorKind::Unsupported,
            "expire managed retention",
            format!(
                "more than {} unique candidates",
                yinyang_core::EXPIRY_CANDIDATES_MAX
            ),
        ));
    }
    let backup_packages = backup_packages
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();

    let leases = ExpiryLeases::acquire(staging_path).await?;
    // Freeze the guard before collecting evidence. The authority mutation must
    // either apply to exactly this head or return Conflict.
    let observed = authority.observe_latest().await?;
    let expected_latest = observed.revision();
    let staging = inspect_bound_local_staging(staging_path, authority.filesystem()).await;
    let backups = inspect_backup_packages(&backup_packages, authority.filesystem()).await;

    let mut unresolved = staging
        .errors
        .iter()
        .map(|detail| Unresolved {
            subject: "local-staging".to_owned(),
            state: "unavailable".to_owned(),
            detail: detail.clone(),
        })
        .collect::<Vec<_>>();
    let mut evidence_complete = match staging.state {
        StagingState::Missing | StagingState::NotInitialized => true,
        StagingState::Available => staging.binding.state == StagingBindingState::Verified,
        StagingState::Partial | StagingState::Unavailable => false,
    };
    if !evidence_complete && unresolved.is_empty() {
        unresolved.push(Unresolved {
            subject: "local-staging".to_owned(),
            state: "partial".to_owned(),
            detail: format!(
                "staging state {:?} with binding {:?}",
                staging.state, staging.binding.state
            ),
        });
    }
    for package in &backups.packages {
        if package.state == BackupPackageState::Validated {
            continue;
        }
        evidence_complete = false;
        let state = match package.state {
            BackupPackageState::Validated => unreachable!(),
            BackupPackageState::Partial => "partial",
            BackupPackageState::Foreign => "foreign",
            BackupPackageState::Unavailable => "unavailable",
        };
        unresolved.push(Unresolved {
            subject: "backup-package".to_owned(),
            state: state.to_owned(),
            detail: format!(
                "{}: {}",
                package.path,
                package
                    .error
                    .as_deref()
                    .unwrap_or("package evidence is incomplete")
            ),
        });
    }

    let mut backup_protected = BTreeMap::<Revision, BTreeSet<&'static str>>::new();
    for package in &backups.packages {
        for (revision, reasons) in &package.protected {
            let entry = backup_protected.entry(*revision).or_default();
            for reason in reasons {
                entry.insert(backup_reason(reason));
            }
        }
    }
    let mut protected = staging.protected.clone();
    merge_protected(&mut protected, &backup_protected);
    let mut targets = candidates.clone();
    targets.extend(protected.keys().copied());
    targets.insert(expected_latest);
    let mut lookups = BTreeMap::new();
    for revision in targets {
        if revision == expected_latest {
            lookups.insert(revision, ExpiryRevisionLookup::Readable);
            continue;
        }
        let state = match authority.observe_revision(revision).await {
            Ok(_) => ExpiryRevisionLookup::Readable,
            Err(error) if error.kind() == ErrorKind::Expired => ExpiryRevisionLookup::Expired,
            Err(error) if error.kind() == ErrorKind::NotFound => ExpiryRevisionLookup::Missing,
            Err(error) => {
                evidence_complete = false;
                unresolved.push(Unresolved {
                    subject: "revision-state".to_owned(),
                    state: "unavailable".to_owned(),
                    detail: format!("{}: {error}", revision_hex(revision)),
                });
                ExpiryRevisionLookup::Unavailable
            }
        };
        lookups.insert(revision, state);
    }
    for (revision, reasons) in &protected {
        if *revision == expected_latest {
            continue;
        }
        let state = match lookups.get(revision) {
            Some(ExpiryRevisionLookup::Readable) => continue,
            Some(ExpiryRevisionLookup::Expired) => "protected-expired",
            Some(ExpiryRevisionLookup::Missing) => "protected-missing",
            Some(ExpiryRevisionLookup::Unavailable) => "protected-unavailable",
            None => "protected-unavailable",
        };
        evidence_complete = false;
        unresolved.push(Unresolved {
            subject: "protected-revision".to_owned(),
            state: state.to_owned(),
            detail: format!(
                "{} is protected by {}",
                revision_hex(*revision),
                reasons.iter().copied().collect::<Vec<_>>().join(",")
            ),
        });
    }

    let evidence_digest = expiry_evidence_digest(
        authority,
        expected_latest,
        &candidates,
        &staging,
        &backups,
        &protected,
        &lookups,
        &unresolved,
    )?;
    let mut decisions = candidates
        .iter()
        .map(|revision| classify_expiry_candidate(*revision, expected_latest, &lookups, &protected))
        .collect::<Vec<_>>();
    let has_refusal = !evidence_complete
        || decisions.iter().any(|decision| {
            !matches!(
                decision.state,
                ExpiryDecisionState::Eligible | ExpiryDecisionState::AlreadyExpired
            )
        });
    if has_refusal {
        drop(leases);
        return Ok(expiry_report(
            authority,
            operation_id,
            expected_latest,
            None,
            ExpiryReportState::Refused,
            evidence_digest,
            &staging,
            &backups,
            decisions,
            unresolved,
            protected.len(),
        ));
    }

    let expire = decisions
        .iter()
        .filter(|decision| decision.state == ExpiryDecisionState::Eligible)
        .filter_map(|decision| parse_revision_hex(&decision.revision))
        .collect::<Vec<_>>();
    if expire.is_empty() {
        drop(leases);
        return Ok(expiry_report(
            authority,
            operation_id,
            expected_latest,
            None,
            ExpiryReportState::AlreadyExpired,
            evidence_digest,
            &staging,
            &backups,
            decisions,
            unresolved,
            protected.len(),
        ));
    }

    let request = ExpiryRequest::new(
        authority.filesystem(),
        operation_id,
        expected_latest,
        expire,
    )?;
    let (state, after_revision) = match authority.expire(&request).await? {
        CommitOutcome::Committed(receipt) => {
            if receipt.commit_id != operation_id || receipt.request_digest != request.digest() {
                return Err(Error::new(
                    ErrorKind::Corrupt,
                    "expire managed retention",
                    "authority returned a receipt for another request",
                ));
            }
            for decision in &mut decisions {
                if decision.state == ExpiryDecisionState::Eligible {
                    decision.state = ExpiryDecisionState::Expired;
                }
            }
            (ExpiryReportState::Committed, Some(receipt.cursor.revision))
        }
        CommitOutcome::Conflict => (ExpiryReportState::Conflict, None),
        CommitOutcome::Retryable => (ExpiryReportState::Retryable, None),
        CommitOutcome::Unknown(commit) => {
            if commit != operation_id {
                return Err(Error::new(
                    ErrorKind::Corrupt,
                    "expire managed retention",
                    "authority returned an unknown outcome for another request",
                ));
            }
            (ExpiryReportState::Unknown, None)
        }
    };
    drop(leases);
    Ok(expiry_report(
        authority,
        operation_id,
        expected_latest,
        after_revision,
        state,
        evidence_digest,
        &staging,
        &backups,
        decisions,
        unresolved,
        protected.len(),
    ))
}

#[cfg(target_os = "linux")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ExpiryRevisionLookup {
    Readable,
    Expired,
    Missing,
    Unavailable,
}

#[cfg(target_os = "linux")]
fn classify_expiry_candidate(
    revision: Revision,
    latest: Revision,
    lookups: &BTreeMap<Revision, ExpiryRevisionLookup>,
    protected: &BTreeMap<Revision, BTreeSet<&'static str>>,
) -> ExpiryRevisionDecision {
    let reasons = protected
        .get(&revision)
        .map(|reasons| reasons.iter().copied().collect())
        .unwrap_or_default();
    let state = if revision == latest {
        ExpiryDecisionState::Current
    } else {
        match lookups.get(&revision) {
            Some(ExpiryRevisionLookup::Readable) if protected.contains_key(&revision) => {
                ExpiryDecisionState::Protected
            }
            Some(ExpiryRevisionLookup::Readable) => ExpiryDecisionState::Eligible,
            Some(ExpiryRevisionLookup::Expired) if protected.contains_key(&revision) => {
                ExpiryDecisionState::ProtectedExpired
            }
            Some(ExpiryRevisionLookup::Expired) => ExpiryDecisionState::AlreadyExpired,
            Some(ExpiryRevisionLookup::Missing) if protected.contains_key(&revision) => {
                ExpiryDecisionState::ProtectedMissing
            }
            Some(ExpiryRevisionLookup::Missing) => ExpiryDecisionState::Missing,
            Some(ExpiryRevisionLookup::Unavailable) | None => ExpiryDecisionState::Unavailable,
        }
    };
    ExpiryRevisionDecision {
        revision: revision_hex(revision),
        state,
        reasons,
    }
}

#[cfg(target_os = "linux")]
#[allow(clippy::too_many_arguments)]
fn expiry_report(
    authority: &dyn Authority,
    operation_id: CommitId,
    observed_revision: Revision,
    after_revision: Option<Revision>,
    state: ExpiryReportState,
    evidence_digest: [u8; 32],
    staging: &StagingSpace,
    backups: &BackupProtection,
    revisions: Vec<ExpiryRevisionDecision>,
    unresolved: Vec<Unresolved>,
    protected_revisions: usize,
) -> ManagedExpiryReport {
    let requested = revisions.len();
    ManagedExpiryReport {
        schema: RETENTION_EXPIRY_SCHEMA,
        filesystem: identity_hex(authority.filesystem()),
        operation_id: uuid::Uuid::from_bytes(*operation_id.as_bytes()).to_string(),
        observed_revision: revision_hex(observed_revision),
        after_revision: after_revision.map(revision_hex),
        state,
        evidence_digest: bytes_hex(&evidence_digest),
        staging: staging_protection(staging),
        backups: backups.clone(),
        revisions,
        unresolved,
        external_readers: "unknown",
        untracked_pins: "unknown",
        quiescence: "not_observed",
        deletion_authorized: false,
        reclaimable_bytes: None,
        physical_cleanup: "not_performed",
        limits: expiry_limits(protected_revisions, requested),
    }
}

#[cfg(target_os = "linux")]
#[allow(clippy::too_many_arguments)]
fn expiry_evidence_digest(
    authority: &dyn Authority,
    expected_latest: Revision,
    candidates: &BTreeSet<Revision>,
    staging: &StagingSpace,
    backups: &BackupProtection,
    protected: &BTreeMap<Revision, BTreeSet<&'static str>>,
    lookups: &BTreeMap<Revision, ExpiryRevisionLookup>,
    unresolved: &[Unresolved],
) -> Result<[u8; 32]> {
    let protected = protected
        .iter()
        .map(|(revision, reasons)| {
            (
                revision.to_bytes(),
                reasons.iter().copied().collect::<Vec<_>>(),
            )
        })
        .collect::<Vec<_>>();
    let lookups = lookups
        .iter()
        .map(|(revision, state)| (revision.to_bytes(), expiry_lookup_tag(*state)))
        .collect::<Vec<_>>();
    let backups = backups
        .packages
        .iter()
        .map(|package| {
            (
                backup_state_tag(package.state),
                package.filesystem.clone(),
                *blake3::hash(package.path.as_bytes()).as_bytes(),
                package
                    .protected
                    .iter()
                    .map(|(revision, reasons)| {
                        (
                            revision.to_bytes(),
                            reasons.iter().copied().collect::<Vec<_>>(),
                        )
                    })
                    .collect::<Vec<_>>(),
            )
        })
        .collect::<Vec<_>>();
    let unresolved = unresolved
        .iter()
        .map(|item| {
            (
                item.subject.as_str(),
                item.state.as_str(),
                item.detail.as_str(),
            )
        })
        .collect::<Vec<_>>();
    let bytes = borsh::to_vec(&(
        *b"YYMEXP01",
        authority.filesystem().as_bytes(),
        expected_latest.to_bytes(),
        candidates
            .iter()
            .map(|revision| revision.to_bytes())
            .collect::<Vec<_>>(),
        staging_state_tag(staging.state),
        staging_binding_tag(staging.binding.state),
        *blake3::hash(staging.path.as_bytes()).as_bytes(),
        staging.handles.total,
        staging.handles.pending,
        staging.handles.frozen,
        staging.handles.conflict,
        staging.handles.unknown,
        staging.handles.failed,
        protected,
        lookups,
        backups,
        unresolved,
    ))
    .map_err(|error| {
        Error::new(
            ErrorKind::Invalid,
            "encode expiry evidence",
            error.to_string(),
        )
    })?;
    Ok(blake3::derive_key(
        "yinyang managed-retention expiry evidence v1",
        &bytes,
    ))
}

#[cfg(target_os = "linux")]
fn expiry_lookup_tag(state: ExpiryRevisionLookup) -> u8 {
    match state {
        ExpiryRevisionLookup::Readable => 0,
        ExpiryRevisionLookup::Expired => 1,
        ExpiryRevisionLookup::Missing => 2,
        ExpiryRevisionLookup::Unavailable => 3,
    }
}

#[cfg(target_os = "linux")]
fn staging_state_tag(state: StagingState) -> u8 {
    match state {
        StagingState::Missing => 0,
        StagingState::NotInitialized => 1,
        StagingState::Available => 2,
        StagingState::Partial => 3,
        StagingState::Unavailable => 4,
    }
}

#[cfg(target_os = "linux")]
fn staging_binding_tag(state: StagingBindingState) -> u8 {
    match state {
        StagingBindingState::Unbound => 0,
        StagingBindingState::Verified => 1,
        StagingBindingState::Unavailable => 2,
    }
}

#[cfg(target_os = "linux")]
fn backup_state_tag(state: BackupPackageState) -> u8 {
    match state {
        BackupPackageState::Validated => 0,
        BackupPackageState::Partial => 1,
        BackupPackageState::Foreign => 2,
        BackupPackageState::Unavailable => 3,
    }
}

#[cfg(target_os = "linux")]
fn parse_revision_hex(value: &str) -> Option<Revision> {
    if value.len() != 48 {
        return None;
    }
    let mut bytes = [0_u8; 24];
    for (index, byte) in bytes.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16).ok()?;
    }
    Some(Revision::from_bytes(bytes))
}

#[cfg(target_os = "linux")]
fn expiry_limits(protected: usize, requested: usize) -> Vec<String> {
    vec![
        format!(
            "Observed {protected} protected revisions and requested {requested} explicit expiry candidates."
        ),
        "Logical expiry never authorizes physical deletion or estimates reclaimable bytes."
            .to_owned(),
        "External readers, untracked pins, other staging directories, and undisclosed backups remain unknown."
            .to_owned(),
        "Quiescence is not observed; every declared local and backup activity must remain stopped until the desired candidate state is confirmed."
            .to_owned(),
        "Configured writer entry points are leased through the Core mutation; optional absent component databases are not treated as unreadable evidence."
            .to_owned(),
        "Mutable authority metadata may advance while immutable physical content and packs are not deleted."
            .to_owned(),
    ]
}

/// Inspect existing Mount/file-runtime staging, the content Sync journal and
/// shared Sync engine evidence without creating a directory or database,
/// acquiring a runtime lease, changing state or recovering records.
pub async fn inspect_local_staging(staging: &Path) -> StagingSpace {
    let runtime = inspect_local_runtime_staging(staging).await;
    let sync = inspect_sync_evidence(staging).await;
    let engine = inspect_engine_evidence(staging).await;
    assemble_staging_space(staging, runtime, sync, engine)
}

pub async fn inspect_sync_space(staging: &Path) -> SyncSpaceReport {
    SyncSpaceReport {
        schema: "yinyang.sync.space.v1",
        staging: inspect_local_staging(staging).await,
    }
}

pub async fn plan_sync_retention(staging: &Path) -> SyncRetentionPlan {
    let staging = inspect_local_staging(staging).await;
    let unresolved = staging
        .errors
        .iter()
        .map(|detail| Unresolved {
            subject: "local-staging".to_owned(),
            state: "unavailable".to_owned(),
            detail: detail.clone(),
        })
        .collect();
    SyncRetentionPlan {
        schema: "yinyang.sync.retention.v1",
        staging: staging_protection(&staging),
        unresolved,
        deletion_authorized: false,
        reclaimable_bytes: None,
    }
}

/// Inspect a validated backup package state directory. The package validator
/// already verified database hashes, the manifest, and the root payload, so
/// this path reads protection facts without opening the original local root.
#[cfg(target_os = "linux")]
async fn inspect_package_staging(staging: &Path) -> StagingSpace {
    let runtime = inspect_local_runtime_staging(staging).await;
    let sync = inspect_sync_evidence(staging).await;
    let engine = inspect_engine_protection_evidence(staging).await;
    assemble_staging_space(staging, runtime, sync, engine)
}

fn assemble_staging_space(
    staging: &Path,
    mut runtime: StagingSpace,
    sync: SyncInspection,
    engine: EngineInspectionRecord,
) -> StagingSpace {
    merge_protected(&mut runtime.protected, &sync.protected);
    merge_protected(&mut runtime.protected, &engine.protected);
    runtime
        .frozen_commits
        .extend(sync.frozen_commits.iter().cloned());
    runtime.frozen_commits.sort();
    runtime.frozen_commits.dedup();
    push_unique(&mut runtime.errors, sync.evidence.error.clone());
    push_unique(&mut runtime.errors, engine.evidence.error.clone());
    if sync.evidence.state != StagingState::Missing {
        runtime.databases.push(StagingDatabase {
            kind: "sync-journal",
            path: display_path(&staging.join("sync.db")),
            state: measurement_state(sync.evidence.state),
            error: sync.evidence.error.clone(),
        });
    }
    if engine.evidence.state != StagingState::Missing {
        runtime.databases.push(StagingDatabase {
            kind: "sync-engine",
            path: display_path(&staging.join("sync-engine.db")),
            state: measurement_state(engine.evidence.state),
            error: engine.evidence.error.clone(),
        });
    }
    runtime.protected_revisions = protected_revisions(&runtime.protected);
    runtime.sync = sync.evidence;
    runtime.engine = engine.evidence;
    runtime.state = staging_state(&runtime.databases, &runtime.errors, runtime.state);
    runtime
}

/// Inspect local evidence only after existing read-only validators prove every
/// present component belongs to the expected filesystem. Foreign or
/// unverifiable state is reported as unavailable with no handles, namespace
/// intents, or protected revisions attributed to the current volume.
pub async fn inspect_bound_local_staging(staging: &Path, filesystem: NodeId) -> StagingSpace {
    match validate_local_staging_binding(staging, filesystem).await {
        Ok(()) => {
            let mut inspection = inspect_local_staging(staging).await;
            inspection.binding = StagingBinding::verified(filesystem);
            inspection
        }
        Err(error) => unavailable_staging(staging, filesystem, error).await,
    }
}

async fn validate_local_staging_binding(
    staging: &Path,
    filesystem: NodeId,
) -> std::result::Result<(), String> {
    let mount = staging.join("staging.db");
    if path_exists(&mount)? {
        Runtime::validate_staging(staging, filesystem)
            .await
            .map_err(|error| {
                format!(
                    "validate mount staging binding for filesystem {}: {error}",
                    identity_hex(filesystem)
                )
            })?;
    }

    let sync = staging.join("sync.db");
    if path_exists(&sync)? {
        Sync::validate_restore(staging, filesystem)
            .await
            .map_err(|error| {
                format!(
                    "validate Sync binding for filesystem {}: {error}",
                    identity_hex(filesystem)
                )
            })?;
    }

    let engine = staging.join("sync-engine.db");
    if path_exists(&engine)? {
        #[cfg(target_os = "linux")]
        {
            let inspection = inspect_state(staging).map_err(|error| {
                format!(
                    "validate Sync engine binding for filesystem {}: {error}",
                    identity_hex(filesystem)
                )
            })?;
            let prefix = format!("{}:", identity_hex(filesystem));
            if inspection.model != crate::sync_engine::SyncModel::Managed
                || !inspection.namespace.starts_with(&prefix)
            {
                return Err(format!(
                    "sync engine namespace {} does not match volume filesystem {}",
                    inspection.namespace,
                    identity_hex(filesystem)
                ));
            }
        }
        #[cfg(not(target_os = "linux"))]
        {
            return Err(
                "Sync engine binding cannot be verified on this platform; foreign state is not attributed"
                    .to_owned(),
            );
        }
    }
    Ok(())
}

fn path_exists(path: &Path) -> std::result::Result<bool, String> {
    path.try_exists()
        .map_err(|error| format!("inspect local state {}: {error}", path.display()))
}

async fn unavailable_staging(
    staging: &Path,
    filesystem: NodeId,
    binding_error: String,
) -> StagingSpace {
    let directory = match measure_directory(staging).await {
        Ok(usage) => Some(usage),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(directory_error) => {
            let error = format!(
                "{binding_error}; measure {}: {directory_error}",
                staging.display()
            );
            return StagingSpace {
                path: display_path(staging),
                state: StagingState::Unavailable,
                binding: StagingBinding::unavailable(filesystem, error.clone()),
                directory: None,
                databases: Vec::new(),
                handles: StagingHandles::default(),
                sync: SyncEvidence::missing(),
                engine: EngineEvidence::missing(),
                protected_revisions: Vec::new(),
                frozen_commits: Vec::new(),
                errors: vec![error],
                protected: BTreeMap::new(),
            };
        }
    };
    StagingSpace {
        path: display_path(staging),
        state: StagingState::Unavailable,
        binding: StagingBinding::unavailable(filesystem, binding_error.clone()),
        directory,
        databases: Vec::new(),
        handles: StagingHandles::default(),
        sync: SyncEvidence::missing(),
        engine: EngineEvidence::missing(),
        protected_revisions: Vec::new(),
        frozen_commits: Vec::new(),
        errors: vec![binding_error],
        protected: BTreeMap::new(),
    }
}

struct SyncInspection {
    evidence: SyncEvidence,
    protected: BTreeMap<Revision, BTreeSet<&'static str>>,
    frozen_commits: BTreeSet<String>,
}

struct EngineInspectionRecord {
    evidence: EngineEvidence,
    protected: BTreeMap<Revision, BTreeSet<&'static str>>,
}

async fn inspect_sync_evidence(staging: &Path) -> SyncInspection {
    let database = staging.join("sync.db");
    let mut inspection = SyncInspection {
        evidence: SyncEvidence::missing(),
        protected: BTreeMap::new(),
        frozen_commits: BTreeSet::new(),
    };
    match database.try_exists() {
        Ok(false) => return inspection,
        Err(error) => {
            inspection.evidence.state = StagingState::Unavailable;
            inspection.evidence.error = Some(format!(
                "inspect sync journal {}: {error}",
                database.display()
            ));
            return inspection;
        }
        Ok(true) => {}
    }
    match Sync::inspect(staging).await {
        Ok(sync) => {
            inspection.evidence.state = StagingState::Available;
            let mut commit_ids = BTreeSet::new();
            for status in sync.intents {
                absorb_intent(
                    status,
                    &mut inspection.evidence,
                    &mut inspection.protected,
                    &mut commit_ids,
                    &mut inspection.frozen_commits,
                );
            }
            inspection.evidence.commit_ids = commit_ids.into_iter().collect();
        }
        Err(error) => {
            inspection.evidence.state = StagingState::Unavailable;
            inspection.evidence.error = Some(error.to_string());
        }
    }
    inspection
}

async fn inspect_engine_evidence(staging: &Path) -> EngineInspectionRecord {
    inspect_engine_database(staging, false).await
}

async fn inspect_engine_database(staging: &Path, protection_only: bool) -> EngineInspectionRecord {
    #[cfg(not(target_os = "linux"))]
    let _ = protection_only;
    let database = staging.join("sync-engine.db");
    let mut inspection = EngineInspectionRecord {
        evidence: EngineEvidence::missing(),
        protected: BTreeMap::new(),
    };
    match database.try_exists() {
        Ok(false) => return inspection,
        Err(error) => {
            inspection.evidence.state = StagingState::Unavailable;
            inspection.evidence.baseline_coverage = "unavailable";
            inspection.evidence.error = Some(format!(
                "inspect Sync engine state {}: {error}",
                database.display()
            ));
            return inspection;
        }
        Ok(true) => {}
    }
    #[cfg(target_os = "linux")]
    {
        let state = staging.to_owned();
        let result = tokio::task::spawn_blocking(move || {
            if protection_only {
                inspect_protection(&state)
            } else {
                inspect_state(&state)
            }
        })
        .await;
        let engine = match result {
            Ok(Ok(engine)) => engine,
            Ok(Err(error)) => {
                inspection.evidence.state = StagingState::Unavailable;
                inspection.evidence.baseline_coverage = "unavailable";
                inspection.evidence.error = Some(error.to_string());
                return inspection;
            }
            Err(error) => {
                inspection.evidence.state = StagingState::Unavailable;
                inspection.evidence.baseline_coverage = "unavailable";
                inspection.evidence.error = Some(format!("inspect Sync engine state: {error}"));
                return inspection;
            }
        };
        let pending = engine.pending_installs;
        inspection.evidence = EngineEvidence {
            state: if pending > 0 {
                StagingState::Partial
            } else {
                StagingState::Available
            },
            pending_installs: Some(pending),
            artifacts: Some(engine.artifacts.len() as u64),
            exportable_artifacts: Some(
                engine
                    .artifacts
                    .iter()
                    .filter(|artifact| artifact.exportable)
                    .count() as u64,
            ),
            conflicts: Some(engine.conflicts.len() as u64),
            baseline_coverage: "observed_from_read_only_baseline",
            baseline: Some(EngineBaselineEvidence::from_inspection(&engine)),
            error: (pending > 0).then(|| {
                format!(
                    "Sync engine has {pending} pending installs; retained artifacts, conflicts, and baseline revisions are observable"
                )
            }),
        };
        for revision in &engine.baseline_revisions {
            if let Some(revision) = parse_revision_hex(revision) {
                inspection
                    .protected
                    .entry(revision)
                    .or_default()
                    .insert("sync_engine_baseline");
            }
        }
        for revision in &engine.intent_receipt_revisions {
            if let Some(revision) = parse_revision_hex(revision) {
                inspection
                    .protected
                    .entry(revision)
                    .or_default()
                    .insert("sync_engine_receipt");
            }
        }
        for conflict in &engine.conflicts {
            if let Some(revision) = conflict
                .remote_revision
                .as_deref()
                .and_then(parse_revision_hex)
            {
                inspection
                    .protected
                    .entry(revision)
                    .or_default()
                    .insert("sync_engine_conflict");
            }
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        inspection.evidence.state = StagingState::Unavailable;
        inspection.evidence.baseline_coverage = "unavailable";
        inspection.evidence.error = Some(
            "Sync engine read-only inspection is available only on Linux; baseline and retained artifact coverage is unobserved"
                .to_owned(),
        );
    }
    inspection
}

/// Read Sync engine revision-protection facts from validated package state
/// without opening the recorded original root.
#[cfg(target_os = "linux")]
async fn inspect_engine_protection_evidence(staging: &Path) -> EngineInspectionRecord {
    inspect_engine_database(staging, true).await
}

fn absorb_intent(
    status: ContentStatus,
    evidence: &mut SyncEvidence,
    protected: &mut BTreeMap<Revision, BTreeSet<&'static str>>,
    commit_ids: &mut BTreeSet<String>,
    frozen_commits: &mut BTreeSet<String>,
) {
    evidence.intents = evidence.intents.saturating_add(1);
    commit_ids.insert(commit_hex(status.commit_id));
    let reason = match status.state {
        IntentState::Prepared => {
            evidence.prepared = evidence.prepared.saturating_add(1);
            frozen_commits.insert(commit_hex(status.commit_id));
            "sync_intent_prepared"
        }
        IntentState::Unknown => {
            evidence.unknown = evidence.unknown.saturating_add(1);
            frozen_commits.insert(commit_hex(status.commit_id));
            "sync_intent_unknown"
        }
        IntentState::Committed => {
            evidence.committed = evidence.committed.saturating_add(1);
            protected
                .entry(status.remote_revision)
                .or_default()
                .insert("sync_receipt");
            "sync_intent_receipt"
        }
        IntentState::Conflict => {
            evidence.conflict = evidence.conflict.saturating_add(1);
            frozen_commits.insert(commit_hex(status.commit_id));
            "sync_intent_conflict"
        }
        IntentState::Retryable => {
            evidence.retryable = evidence.retryable.saturating_add(1);
            frozen_commits.insert(commit_hex(status.commit_id));
            "sync_intent_retryable"
        }
        IntentState::Failed => {
            evidence.failed = evidence.failed.saturating_add(1);
            frozen_commits.insert(commit_hex(status.commit_id));
            "sync_intent_failed"
        }
    };
    protected.entry(status.baseline).or_default().insert(reason);
}

fn merge_protected(
    destination: &mut BTreeMap<Revision, BTreeSet<&'static str>>,
    source: &BTreeMap<Revision, BTreeSet<&'static str>>,
) {
    for (revision, reasons) in source {
        destination
            .entry(*revision)
            .or_default()
            .extend(reasons.iter().copied());
    }
}

fn protected_revisions(
    protected: &BTreeMap<Revision, BTreeSet<&'static str>>,
) -> Vec<ProtectedRevision> {
    protected
        .iter()
        .map(|(revision, reasons)| ProtectedRevision {
            revision: revision_hex(*revision),
            reasons: reasons.iter().copied().collect(),
        })
        .collect()
}

fn push_unique(errors: &mut Vec<String>, error: Option<String>) {
    if let Some(error) = error
        && !errors.contains(&error)
    {
        errors.push(error);
    }
}

fn measurement_state(state: StagingState) -> MeasurementState {
    match state {
        StagingState::Missing | StagingState::Unavailable => MeasurementState::Unavailable,
        StagingState::NotInitialized | StagingState::Partial => MeasurementState::Partial,
        StagingState::Available => MeasurementState::Available,
    }
}

fn staging_state(
    databases: &[StagingDatabase],
    errors: &[String],
    fallback: StagingState,
) -> StagingState {
    if databases.is_empty() {
        return fallback;
    }
    let available = databases
        .iter()
        .any(|database| database.state == MeasurementState::Available);
    let partial = databases
        .iter()
        .any(|database| database.state == MeasurementState::Partial);
    let unavailable = databases
        .iter()
        .any(|database| database.state == MeasurementState::Unavailable);
    if !available && !partial {
        StagingState::Unavailable
    } else if unavailable || partial || !errors.is_empty() {
        StagingState::Partial
    } else {
        StagingState::Available
    }
}

/// Inspect the existing Mount/file-runtime staging database without creating a
/// directory or database, acquiring a runtime lease, changing state, or
/// recovering records.
async fn inspect_local_runtime_staging(staging: &Path) -> StagingSpace {
    let mut errors = Vec::new();
    let directory = match measure_directory(staging).await {
        Ok(usage) => Some(usage),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => {
            errors.push(format!("measure {}: {error}", staging.display()));
            None
        }
    };
    let database_path = staging.join("staging.db");
    let exists = match database_path.try_exists() {
        Ok(exists) => exists,
        Err(error) => {
            errors.push(format!(
                "inspect staging database {}: {error}",
                database_path.display()
            ));
            false
        }
    };
    if !exists {
        return StagingSpace {
            path: display_path(staging),
            state: if !errors.is_empty() {
                StagingState::Unavailable
            } else if staging.try_exists().unwrap_or(false) {
                StagingState::NotInitialized
            } else {
                StagingState::Missing
            },
            binding: StagingBinding::unbound(),
            directory,
            databases: Vec::new(),
            handles: StagingHandles::default(),
            sync: SyncEvidence::missing(),
            engine: EngineEvidence::missing(),
            protected_revisions: Vec::new(),
            frozen_commits: Vec::new(),
            errors,
            protected: BTreeMap::new(),
        };
    }

    let mut handles = StagingHandles::default();
    let mut protected = BTreeMap::<Revision, BTreeSet<&'static str>>::new();
    let mut frozen_commits = BTreeSet::new();
    let mut handle_revisions = BTreeMap::new();
    let mut databases = Vec::new();
    let state = match Runtime::inspect(staging).await {
        Ok(status) => {
            databases.push(StagingDatabase {
                kind: "mount",
                path: display_path(&database_path),
                state: MeasurementState::Available,
                error: None,
            });
            for handle in status.handles {
                handle_revisions.insert(handle.id, handle.remote_revision);
                handles.total = handles.total.saturating_add(1);
                handles.handle_bytes = handles.handle_bytes.saturating_add(handle.length);
                // Every ready handle remains recoverable by UUID and may
                // read untouched bytes or publish from this exact base.
                let mut reasons = BTreeSet::from(["staging_base"]);
                if handle.pending {
                    handles.pending = handles.pending.saturating_add(1);
                    reasons.insert("staging_pending");
                }
                if handle.frozen {
                    handles.frozen = handles.frozen.saturating_add(1);
                    reasons.insert("staging_frozen");
                }
                if let Some(commit) = handle.frozen_commit_id {
                    frozen_commits.insert(commit_hex(commit));
                }
                let mut conflict = handle.conflict;
                let mut unknown = false;
                let mut failed = false;
                if let Some(error) = &handle.error {
                    match error.kind() {
                        RuntimeErrorKind::Unknown => {
                            unknown = true;
                            reasons.insert("staging_unknown");
                        }
                        RuntimeErrorKind::Conflict => {
                            conflict = true;
                        }
                        _ => {
                            failed = true;
                            reasons.insert("staging_failed");
                        }
                    }
                }
                if conflict {
                    handles.conflict = handles.conflict.saturating_add(1);
                    reasons.insert("staging_conflict");
                }
                if unknown {
                    handles.unknown = handles.unknown.saturating_add(1);
                }
                if failed {
                    handles.failed = handles.failed.saturating_add(1);
                }
                protected
                    .entry(handle.remote_revision)
                    .or_default()
                    .extend(reasons);
            }
            handles.ledger_errors = handles
                .ledger_errors
                .saturating_add(status.errors.len() as u64);
            for error in status.errors {
                if let Some(commit) = error.error.commit_id() {
                    frozen_commits.insert(commit_hex(commit));
                }
                if let Some(revision) = handle_revisions.get(&error.handle) {
                    let reason = match error.error.kind() {
                        RuntimeErrorKind::Unknown => "staging_unknown",
                        RuntimeErrorKind::Conflict => "staging_conflict",
                        _ => "staging_failed",
                    };
                    protected.entry(*revision).or_default().insert(reason);
                } else {
                    errors.push(format!(
                        "ledger error {} references absent handle {}",
                        error.sequence, error.handle
                    ));
                }
            }
            StagingState::Available
        }
        Err(error) => {
            databases.push(StagingDatabase {
                kind: "mount",
                path: display_path(&database_path),
                state: MeasurementState::Unavailable,
                error: Some(error.to_string()),
            });
            errors.push(format!("{}: {error}", database_path.display()));
            StagingState::Unavailable
        }
    };
    let protected_revisions = protected
        .iter()
        .map(|(revision, reasons)| ProtectedRevision {
            revision: revision_hex(*revision),
            reasons: reasons.iter().copied().collect(),
        })
        .collect::<Vec<_>>();
    StagingSpace {
        path: display_path(staging),
        state,
        binding: StagingBinding::unbound(),
        directory,
        databases,
        handles,
        sync: SyncEvidence::missing(),
        engine: EngineEvidence::missing(),
        protected_revisions,
        frozen_commits: frozen_commits.into_iter().collect(),
        errors,
        protected,
    }
}

async fn measure_snapshot(snapshot: &Snapshot) -> Result<MeasuredSnapshot> {
    let mut measured = LogicalAccumulator::default();
    let mut pending = vec![snapshot.root()];
    let mut seen = BTreeSet::new();
    while let Some(directory) = pending.pop() {
        if !seen.insert(directory) {
            return Err(Error::new(
                ErrorKind::Invalid,
                "measure managed space",
                "namespace contains a duplicate or cyclic node identity",
            ));
        }
        measured.directory()?;
        let mut token = None;
        loop {
            let page = snapshot.scan(directory, token.as_ref(), SCAN_LIMIT).await?;
            for entry in page.entries {
                let node = snapshot.node(entry.node_id).await?.ok_or_else(|| {
                    Error::new(
                        ErrorKind::Invalid,
                        "measure managed space",
                        "directory item disappeared from the pinned snapshot",
                    )
                })?;
                match node.kind() {
                    NodeKind::Directory { .. } => pending.push(node.id()),
                    NodeKind::File(content) => {
                        let content_id = content.content_id();
                        measured.content(ContentKey {
                            length: content_id.length(),
                            digest: *content_id.digest(),
                        })?;
                    }
                }
            }
            token = page.next;
            if token.is_none() {
                break;
            }
        }
    }
    Ok(measured.finish(snapshot.revision()))
}

async fn inspect_retained(
    authority: &dyn Authority,
    latest: &Snapshot,
    unresolved: &mut Vec<Unresolved>,
) -> RetainedLogicalSpace {
    match authority.revision_history().await {
        Ok(mut revisions) => {
            revisions.sort_by_key(|record| record.revision);
            let mut complete = true;
            if !revisions
                .iter()
                .any(|record| record.revision == latest.revision())
            {
                complete = false;
                unresolved.push(Unresolved {
                    subject: "retained-revisions".to_owned(),
                    state: "latest-not-listed".to_owned(),
                    detail: revision_hex(latest.revision()),
                });
                revisions.push(RevisionRecord {
                    revision: latest.revision(),
                    status: RevisionStatus::Readable,
                });
                revisions.sort_by_key(|record| record.revision);
            }
            let revision_count = revisions.len() as u64;
            let readable_revision_count = revisions
                .iter()
                .filter(|record| record.status == RevisionStatus::Readable)
                .count() as u64;
            let expired_revision_count = revision_count.saturating_sub(readable_revision_count);
            let mut observed = Vec::with_capacity(readable_revision_count as usize);
            let mut observed_file_bytes = 0_u64;
            let mut unique = BTreeSet::new();
            let mut observed_unique_content_bytes = 0_u64;
            for record in revisions {
                if record.status == RevisionStatus::Expired {
                    continue;
                }
                let revision = record.revision;
                let snapshot = if revision == latest.revision() {
                    latest.clone()
                } else {
                    match authority.observe_revision(revision).await {
                        Ok(snapshot) => snapshot,
                        Err(error) => {
                            complete = false;
                            unresolved.push(Unresolved {
                                subject: "retained-snapshot".to_owned(),
                                state: "unavailable".to_owned(),
                                detail: format!("{}: {error}", revision_hex(revision)),
                            });
                            continue;
                        }
                    }
                };
                match measure_snapshot(&snapshot).await {
                    Ok(measured) => {
                        observed_file_bytes =
                            match observed_file_bytes.checked_add(measured.logical.file_bytes) {
                                Some(value) => value,
                                None => {
                                    complete = false;
                                    unresolved.push(Unresolved {
                                        subject: "retained-logical".to_owned(),
                                        state: "unavailable".to_owned(),
                                        detail: "retained logical file bytes overflow".to_owned(),
                                    });
                                    continue;
                                }
                            };
                        for content in measured.contents {
                            if unique.insert(content) {
                                match observed_unique_content_bytes.checked_add(content.length) {
                                    Some(value) => observed_unique_content_bytes = value,
                                    None => {
                                        complete = false;
                                        unresolved.push(Unresolved {
                                            subject: "retained-logical".to_owned(),
                                            state: "unavailable".to_owned(),
                                            detail: "retained unique content bytes overflow"
                                                .to_owned(),
                                        });
                                        break;
                                    }
                                }
                            }
                        }
                        observed.push(measured.logical);
                    }
                    Err(error) => {
                        complete = false;
                        unresolved.push(Unresolved {
                            subject: "retained-logical".to_owned(),
                            state: "unavailable".to_owned(),
                            detail: format!("{}: {error}", revision_hex(revision)),
                        });
                    }
                }
            }
            RetainedLogicalSpace {
                state: if complete {
                    MeasurementState::Available
                } else {
                    MeasurementState::Partial
                },
                revision_count: Some(revision_count),
                readable_revision_count: Some(readable_revision_count),
                expired_revision_count: Some(expired_revision_count),
                observed_revisions: observed,
                observed_file_bytes,
                observed_unique_content_bytes,
                error: None,
            }
        }
        Err(error) => {
            unresolved.push(Unresolved {
                subject: "retained-revisions".to_owned(),
                state: "unavailable".to_owned(),
                detail: error.to_string(),
            });
            RetainedLogicalSpace {
                state: MeasurementState::Unavailable,
                revision_count: None,
                readable_revision_count: None,
                expired_revision_count: None,
                observed_revisions: Vec::new(),
                observed_file_bytes: 0,
                observed_unique_content_bytes: 0,
                error: Some(error.to_string()),
            }
        }
    }
}

async fn inspect_physical(
    authority: &dyn Authority,
    operator: &Operator,
    latest: &Snapshot,
) -> PhysicalSpace {
    let prefix = format!(
        "{PACK_ROOT}/{}/packs/",
        uuid::Uuid::from_bytes(*authority.filesystem().as_bytes()).simple()
    );
    let reachability = authority.inspect_physical(latest).await;
    let pack_usage = reachability.packs();
    let mut issues = reachability
        .issues()
        .iter()
        .map(|issue| PhysicalIssueReport {
            subject: issue.subject.clone(),
            state: physical_issue_state(issue.kind),
            detail: issue.detail.clone(),
        })
        .collect::<Vec<_>>();
    let mut errors = Vec::new();
    let head = match operator.stat(HEAD_KEY).await {
        Ok(metadata) if !metadata.is_dir() => Some(PhysicalObject {
            objects: 1,
            bytes: metadata.content_length(),
        }),
        Ok(_) => {
            errors.push("head path is not a file".to_owned());
            None
        }
        Err(error) => {
            errors.push(format!("stat head object: {error}"));
            None
        }
    };
    let mut pack_sizes = BTreeMap::<[u8; 16], u64>::new();
    let mut unrecognized_visible_objects = 0_u64;
    let packs = match operator.list_with(prefix.as_str()).recursive(true).await {
        Ok(entries) => {
            let mut objects = 0_u64;
            let mut bytes = 0_u64;
            let mut seen = BTreeSet::new();
            for entry in entries {
                if !entry.path().starts_with(&prefix) || entry.path().ends_with('/') {
                    continue;
                }
                let metadata = entry.metadata();
                if metadata.mode() == EntryMode::DIR {
                    continue;
                }
                if !seen.insert(entry.path().to_owned()) {
                    continue;
                }
                objects = objects.saturating_add(1);
                bytes = bytes.saturating_add(metadata.content_length());
                if let Some(object) = pack_object_id(entry.path(), &prefix) {
                    pack_sizes.insert(object, metadata.content_length());
                } else {
                    unrecognized_visible_objects = unrecognized_visible_objects.saturating_add(1);
                    push_physical_issue(
                        &mut issues,
                        format!("visible-pack:{}", entry.path()),
                        "visible pack key is not a UUID object below the Managed pack root"
                            .to_owned(),
                    );
                }
            }
            Some(PhysicalObject { objects, bytes })
        }
        Err(error) => {
            errors.push(format!("list current pack objects: {error}"));
            None
        }
    };
    let after = authority.observe_latest().await;
    let (after_revision, consistency) = match after {
        Ok(snapshot) if snapshot.revision() == latest.revision() => (
            Some(revision_hex(snapshot.revision())),
            ListingConsistency::StableHead,
        ),
        Ok(snapshot) => (
            Some(revision_hex(snapshot.revision())),
            ListingConsistency::ChangedDuringInventory,
        ),
        Err(error) => {
            errors.push(format!("reobserve latest revision: {error}"));
            (None, ListingConsistency::Unverified)
        }
    };
    let (visible_objects, visible_bytes) = match (&head, &packs) {
        (Some(head), Some(packs)) => (
            Some(head.objects.saturating_add(packs.objects)),
            Some(head.bytes.saturating_add(packs.bytes)),
        ),
        _ => (None, None),
    };
    let mut observed_referenced_bytes = 0_u64;
    let mut visible_referenced_bytes = 0_u64;
    let mut visible_uncovered_bytes = 0_u64;
    let mut fully_referenced = 0_u64;
    let mut partially_referenced = 0_u64;
    let mut missing_reference_packs = 0_u64;
    let mut truncated_reference_packs = 0_u64;
    for usage in &pack_usage {
        observed_referenced_bytes = observed_referenced_bytes.saturating_add(usage.covered_bytes);
        if packs.is_none() {
            continue;
        }
        let Some(size) = pack_sizes.get(&usage.object).copied() else {
            missing_reference_packs = missing_reference_packs.saturating_add(1);
            push_physical_issue(
                &mut issues,
                format!(
                    "referenced-pack:{}",
                    uuid::Uuid::from_bytes(usage.object).simple()
                ),
                "authenticated references name a pack absent from the current-key listing"
                    .to_owned(),
            );
            continue;
        };
        let mut covered = 0_u64;
        let mut truncated = false;
        for (start, end) in &usage.intervals {
            if *start >= size {
                truncated = true;
                continue;
            }
            if *end > size {
                truncated = true;
            }
            let visible_start = *start;
            let visible_end = (*end).min(size);
            covered = covered.saturating_add(visible_end.saturating_sub(visible_start));
        }
        if truncated {
            truncated_reference_packs = truncated_reference_packs.saturating_add(1);
            push_physical_issue(
                &mut issues,
                format!(
                    "referenced-pack:{}",
                    uuid::Uuid::from_bytes(usage.object).simple()
                ),
                "an authenticated extent exceeds the visible pack length".to_owned(),
            );
        }
        visible_referenced_bytes = visible_referenced_bytes.saturating_add(covered);
        if covered == size {
            fully_referenced = fully_referenced.saturating_add(1);
        } else if covered > 0 {
            partially_referenced = partially_referenced.saturating_add(1);
        }
    }
    if packs.is_some() {
        let visible_bytes = pack_sizes
            .values()
            .fold(0_u64, |total, bytes| total.saturating_add(*bytes));
        visible_uncovered_bytes = visible_bytes.saturating_sub(visible_referenced_bytes);
    }
    let listing_complete = head.is_some()
        && packs.is_some()
        && after_revision.is_some()
        && consistency == ListingConsistency::StableHead;
    let root_relative_complete = listing_complete
        && reachability.is_complete()
        && missing_reference_packs == 0
        && truncated_reference_packs == 0
        && unrecognized_visible_objects == 0;
    let referenced_objects = pack_usage
        .iter()
        .map(|usage| usage.object)
        .collect::<BTreeSet<_>>();
    let unreferenced = if root_relative_complete {
        pack_sizes
            .keys()
            .filter(|object| !referenced_objects.contains(*object))
            .copied()
            .collect::<Vec<_>>()
    } else {
        Vec::new()
    };
    let unreferenced_bytes = unreferenced
        .iter()
        .filter_map(|object| pack_sizes.get(object))
        .fold(0_u64, |total, bytes| total.saturating_add(*bytes));
    let coverage_complete = listing_complete
        && missing_reference_packs == 0
        && truncated_reference_packs == 0
        && unrecognized_visible_objects == 0;
    let state = if reachability.is_complete() && coverage_complete {
        MeasurementState::Available
    } else if head.is_some() || packs.is_some() || !reachability.references().is_empty() {
        MeasurementState::Partial
    } else {
        MeasurementState::Unavailable
    };
    let coverage_available = packs.is_some();
    PhysicalSpace {
        state,
        head,
        packs,
        visible_objects,
        visible_bytes,
        before_revision: revision_hex(latest.revision()),
        after_revision,
        listing_consistency: consistency,
        noncurrent_versions: "unknown",
        accounting: "authenticated_reference_union_against_current_object_keys",
        reachability: PhysicalReachability {
            state: if reachability.is_complete() && listing_complete {
                MeasurementState::Available
            } else {
                MeasurementState::Partial
            },
            retained_revisions: reachability.retained_revisions(),
            snapshot_descriptors: reachability
                .reference_count(PhysicalReferenceKind::SnapshotDescriptor),
            metadata_index_references: reachability
                .reference_count(PhysicalReferenceKind::MetadataIndex),
            metadata_value_references: reachability
                .reference_count(PhysicalReferenceKind::MetadataValue),
            content_descriptors: reachability.content_descriptor_count(),
            content_index_references: reachability
                .reference_count(PhysicalReferenceKind::ContentIndex),
            content_extent_references: reachability
                .reference_count(PhysicalReferenceKind::ContentExtent),
            packs_with_references: pack_usage.len().try_into().unwrap_or(u64::MAX),
            shared_packs: pack_usage
                .iter()
                .filter(|usage| usage.references > 1)
                .count()
                .try_into()
                .unwrap_or(u64::MAX),
            issues: issues.len().try_into().unwrap_or(u64::MAX),
            issues_omitted: reachability.issues_omitted(),
        },
        coverage: PhysicalCoverage {
            observed_referenced_bytes,
            visible_referenced_bytes: coverage_available.then_some(visible_referenced_bytes),
            visible_uncovered_bytes: coverage_available.then_some(visible_uncovered_bytes),
            fully_referenced_visible_packs: coverage_available.then_some(fully_referenced),
            partially_referenced_visible_packs: coverage_available.then_some(partially_referenced),
            missing_reference_packs,
            truncated_reference_packs,
            payload_verification: "content_index_authenticated_payload_not_read",
        },
        root_external: ObservedRootExternal {
            state: if root_relative_complete {
                MeasurementState::Available
            } else {
                MeasurementState::Partial
            },
            packs: root_relative_complete
                .then_some(unreferenced.len().try_into().unwrap_or(u64::MAX)),
            bytes: root_relative_complete.then_some(unreferenced_bytes),
            treatment: "observed-root-relative only; not an orphan or deletion-eligibility claim",
        },
        issues,
        error: (!errors.is_empty()).then(|| errors.join("; ")),
    }
}

fn pack_object_id(path: &str, prefix: &str) -> Option<[u8; 16]> {
    let name = path.strip_prefix(prefix)?;
    if name.contains('/') {
        return None;
    }
    let uuid = uuid::Uuid::parse_str(name).ok()?;
    if name != uuid.simple().to_string() {
        return None;
    }
    Some(*uuid.as_bytes())
}

fn physical_issue_state(kind: ErrorKind) -> &'static str {
    match kind {
        ErrorKind::NotFound => "missing",
        ErrorKind::Corrupt => "corrupt",
        ErrorKind::Invalid | ErrorKind::InvalidName => "invalid",
        ErrorKind::Unsupported => "unsupported",
        ErrorKind::Storage | ErrorKind::Io | ErrorKind::PermissionDenied | ErrorKind::Expired => {
            "unavailable"
        }
        ErrorKind::AlreadyExists
        | ErrorKind::NotDirectory
        | ErrorKind::IsDirectory
        | ErrorKind::NotEmpty => "invalid",
    }
}

fn push_physical_issue(issues: &mut Vec<PhysicalIssueReport>, subject: String, detail: String) {
    if issues.len() >= 256
        || issues
            .iter()
            .any(|issue| issue.subject == subject && issue.detail == detail)
    {
        return;
    }
    issues.push(PhysicalIssueReport {
        subject,
        state: "unavailable",
        detail,
    });
}

async fn inspect_receipts(
    snapshot: &Snapshot,
    unresolved: &mut Vec<Unresolved>,
) -> ReceiptEvidence {
    let mut after = None;
    let mut records = 0_u64;
    let mut revisions = BTreeSet::new();
    loop {
        match snapshot.changes(after, SCAN_LIMIT).await {
            Ok(changes) => {
                if changes.is_empty() {
                    break;
                }
                records = records.saturating_add(changes.len() as u64);
                for change in &changes {
                    revisions.insert(change.receipt.cursor.revision);
                }
                after = changes.last().map(|change| change.receipt.cursor);
                if changes.len() < SCAN_LIMIT {
                    break;
                }
            }
            Err(error) => {
                unresolved.push(Unresolved {
                    subject: "receipt-evidence".to_owned(),
                    state: "unavailable".to_owned(),
                    detail: error.to_string(),
                });
                return ReceiptEvidence {
                    state: MeasurementState::Unavailable,
                    records: None,
                    revisions: None,
                    treatment: "receipt records remain revision evidence; they are not a deletion authorization",
                    error: Some(error.to_string()),
                };
            }
        }
    }
    ReceiptEvidence {
        state: MeasurementState::Available,
        records: Some(records),
        revisions: Some(revisions.len() as u64),
        treatment: "receipt records remain revision evidence; they are not a deletion authorization",
        error: None,
    }
}

fn selection_reasons(
    revision: Revision,
    latest: Revision,
    bounded: bool,
    keep_last: &BTreeSet<Revision>,
    requested_pins: &BTreeSet<Revision>,
    protected: &BTreeMap<Revision, BTreeSet<&'static str>>,
) -> Vec<&'static str> {
    let mut reasons = Vec::new();
    if revision == latest {
        reasons.push("current");
    }
    if !bounded {
        reasons.push("keep_all");
    }
    if keep_last.contains(&revision) {
        reasons.push("keep_last");
    }
    if requested_pins.contains(&revision) {
        reasons.push("requested_pin");
    }
    if let Some(protected) = protected.get(&revision) {
        reasons.extend(protected.iter().copied());
    }
    if reasons.is_empty() {
        reasons.push("outside_bounded_policy");
    }
    reasons
}

async fn measure_directory(path: &Path) -> std::io::Result<DirectoryUsage> {
    let path = path.to_owned();
    tokio::task::spawn_blocking(move || measure_directory_sync(&path))
        .await
        .map_err(std::io::Error::other)?
}

fn measure_directory_sync(path: &Path) -> std::io::Result<DirectoryUsage> {
    let metadata = std::fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "staging path is not a directory",
        ));
    }
    let mut usage = DirectoryUsage {
        regular_files: 0,
        apparent_bytes: 0,
        symlinks_skipped: 0,
        other_entries_skipped: 0,
        non_atomic: true,
    };
    let mut pending = vec![path.to_owned()];
    while let Some(directory) = pending.pop() {
        for entry in std::fs::read_dir(directory)? {
            let entry = entry?;
            let metadata = std::fs::symlink_metadata(entry.path())?;
            if metadata.file_type().is_symlink() {
                usage.symlinks_skipped = usage.symlinks_skipped.saturating_add(1);
            } else if metadata.is_dir() {
                pending.push(entry.path());
            } else if metadata.is_file() {
                usage.regular_files = usage.regular_files.saturating_add(1);
                usage.apparent_bytes = usage.apparent_bytes.saturating_add(metadata.len());
            } else {
                usage.other_entries_skipped = usage.other_entries_skipped.saturating_add(1);
            }
        }
    }
    Ok(usage)
}

fn checked_add(value: u64, increment: u64, subject: &'static str) -> Result<u64> {
    value.checked_add(increment).ok_or_else(|| {
        Error::new(
            ErrorKind::Invalid,
            "measure managed space",
            format!("{subject} overflows"),
        )
    })
}

fn revision_hex(revision: Revision) -> String {
    revision
        .to_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn identity_hex(identity: NodeId) -> String {
    identity
        .as_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[cfg(target_os = "linux")]
fn parse_node_id_hex(value: &str) -> Option<NodeId> {
    if value.len() != 32 {
        return None;
    }
    let mut bytes = [0_u8; 16];
    for (index, byte) in bytes.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16).ok()?;
    }
    Some(NodeId::from_bytes(bytes))
}

fn commit_hex(commit: yinyang_core::CommitId) -> String {
    bytes_hex(commit.as_bytes())
}

fn bytes_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn display_path(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

fn space_limits() -> Vec<String> {
    [
        "Logical file bytes are not physical allocated bytes; index pages, metadata, orphan packs, backend overhead, and local files are separate measurements.",
        "Referenced extents are authenticated index references, not payload reads; a root-relative unreferenced pack is not an orphan or a deletion-eligibility claim.",
        "Recursive listing returns current object keys only and is not atomic; noncurrent backend versions are unknown.",
        "Referenced byte coverage is a union over authenticated extents per visible pack; uncovered bytes are not reclaimed and may belong to unobserved/unknown readers.",
        "External readers, untracked revision pins, other staging directories, backup packages, and quiescence are not discoverable by this inspection.",
        "Known expired revisions remain history identities but are not measured as readable snapshots.",
        "Standalone inspect_local_staging is unbound; volume-bound report and retention planning require verified local filesystem attribution.",
        "Direct namespace objects are excluded; this module inspects only Managed authority metadata and Managed local staging.",
        "SQLite read-only inspection may create or update derived shared-memory or empty WAL files; authoritative database and existing WAL bytes are not migrated or modified by this code.",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect()
}

fn retention_limits() -> Vec<String> {
    [
        "A selected revision is a declarative protection request, not evidence that any physical object is unreachable or safe to delete.",
        "External readers, untracked revision pins, other staging directories, and undisclosed backup packages must be supplied as explicit requested pins; supplied backup packages are validated read-only.",
        "Noncurrent backend versions and shared pack extents remain unknown; no reclaimable-byte estimate is produced.",
        "Receipt records are revision evidence, not a physical cleanup authorization.",
        "Quiescence is not observed; planning does not stop writers, readers, mounts, Sync processes, or backup activity.",
        "Expiry is disabled by default; keep-last changes only the declarative selection, and outside_bounded_policy revisions remain readable.",
        "Keep and keep-last apply only to this invocation; they are not persistent pins.",
        "Only the explicit expire command changes fresh revision readability; planning never expires history and expiry never performs physical deletion.",
        "Direct namespace objects are excluded.",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;
    use tempfile::tempdir;

    fn revision(byte: u8) -> Revision {
        Revision::from_bytes([byte; 24])
    }

    #[test]
    fn unique_content_accounting_does_not_double_count_shared_bytes() {
        let mut logical = LogicalAccumulator::default();
        let content = ContentKey {
            length: 4096,
            digest: [7; 32],
        };
        logical.content(content).unwrap();
        logical.content(content).unwrap();
        let measured = logical.finish(revision(1));
        assert_eq!(measured.logical.files, 2);
        assert_eq!(measured.logical.file_bytes, 8192);
        assert_eq!(measured.logical.unique_content_count, 1);
        assert_eq!(measured.logical.unique_content_bytes, 4096);
    }

    #[test]
    fn bounded_selection_keeps_current_requested_and_staging_protected_revisions() {
        let revisions = [revision(1), revision(2), revision(3), revision(4)];
        let keep_last = BTreeSet::from([revision(3), revision(4)]);
        let requested = BTreeSet::from([revision(1)]);
        let protected = BTreeMap::from([(
            revision(2),
            BTreeSet::from(["staging_pending", "staging_frozen"]),
        )]);
        let reasons = revisions
            .into_iter()
            .map(|value| {
                selection_reasons(value, revision(4), true, &keep_last, &requested, &protected)
            })
            .collect::<Vec<_>>();
        assert_eq!(
            reasons,
            vec![
                vec!["requested_pin"],
                vec!["staging_frozen", "staging_pending"],
                vec!["keep_last"],
                vec!["current", "keep_last"],
            ]
        );
    }

    #[test]
    fn pack_identity_requires_the_canonical_simple_uuid_key() {
        let prefix = ".yinyang/v2/fs/packs/";
        let canonical = "0123456789abcdef0123456789abcdef";
        assert!(pack_object_id(&format!("{prefix}{canonical}"), prefix).is_some());
        assert!(pack_object_id(&format!("{prefix}{}", canonical.to_uppercase()), prefix).is_none());
        assert!(
            pack_object_id(
                &format!("{prefix}01234567-89ab-cdef-0123-456789abcdef"),
                prefix
            )
            .is_none()
        );
    }

    #[tokio::test]
    async fn missing_staging_is_reported_without_creating_it() {
        let root = tempdir().unwrap();
        let staging = root.path().join("absent");
        let inspection = inspect_local_staging(&staging).await;
        assert_eq!(inspection.state, StagingState::Missing);
        assert!(inspection.errors.is_empty());
        assert!(!staging.exists());
    }

    #[tokio::test]
    async fn existing_empty_staging_is_not_initialized() {
        let root = tempdir().unwrap();
        let staging = root.path().join("staging");
        std::fs::create_dir(&staging).unwrap();
        let inspection = inspect_local_staging(&staging).await;
        assert_eq!(inspection.state, StagingState::NotInitialized);
        assert!(inspection.databases.is_empty());
        assert!(!staging.join("staging.db").exists());
        assert!(!staging.join("runtime.lock").exists());
    }

    #[tokio::test]
    async fn malformed_staging_is_reported_without_creating_a_lease() {
        let root = tempdir().unwrap();
        let staging = root.path().join("staging");
        std::fs::create_dir(&staging).unwrap();
        let database = staging.join("staging.db");
        std::fs::write(&database, b"not a sqlite database").unwrap();
        let before = std::fs::read(&database).unwrap();
        let inspection = inspect_local_staging(&staging).await;
        assert_eq!(inspection.state, StagingState::Unavailable);
        assert!(!inspection.errors.is_empty());
        assert!(!staging.join("runtime.lock").exists());
        assert_eq!(std::fs::read(&database).unwrap(), before);
    }

    #[tokio::test]
    async fn unknown_staging_profile_is_rejected_without_rewriting_the_database() {
        let root = tempdir().unwrap();
        let staging = root.path().join("staging");
        std::fs::create_dir(&staging).unwrap();
        let database = staging.join("staging.db");
        let connection = Connection::open(&database).unwrap();
        connection
            .execute_batch(
                "CREATE TABLE authority(
                   singleton INTEGER PRIMARY KEY,
                   profile TEXT NOT NULL,
                   filesystem BLOB NOT NULL
                 );
                 INSERT INTO authority VALUES(1,'future-stage',X'00');",
            )
            .unwrap();
        drop(connection);
        let before = std::fs::read(&database).unwrap();
        let inspection = inspect_local_staging(&staging).await;
        assert_eq!(inspection.state, StagingState::Unavailable);
        assert!(inspection.errors.iter().any(|error| {
            error.contains("unknown staging profile")
                || error.contains("unsupported profile")
                || error.contains("is not supported")
        }));
        assert!(!staging.join("runtime.lock").exists());
        assert_eq!(std::fs::read(&database).unwrap(), before);
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn expiry_admits_empty_declared_state_and_reports_already_expired() {
        use crate::support::TestBackend;
        use std::sync::Arc;
        use yinyang_core::{Fs, Planner};

        let backend = TestBackend::default();
        let fs = Fs::create(backend.operator()).await.unwrap();
        let authority: Arc<dyn Authority> = Arc::new(fs);
        let candidate = authority.observe_latest().await.unwrap().revision();
        for name in ["one", "two"] {
            let snapshot = authority.observe_latest().await.unwrap();
            let mut planner = Planner::new(&snapshot, CommitId::generate());
            planner
                .create_directory(authority.root(), name)
                .await
                .unwrap();
            assert!(matches!(
                authority.commit(&planner.finish().unwrap()).await.unwrap(),
                CommitOutcome::Committed(_)
            ));
        }

        let temp = tempdir().unwrap();
        let staging = temp.path().join("state");
        let report = expire_managed_retention(
            authority.as_ref(),
            &staging,
            &[candidate],
            &[],
            CommitId::generate(),
        )
        .await
        .unwrap();
        assert_eq!(report.state, ExpiryReportState::Committed);
        assert!(!staging.join("staging.db").exists());
        assert!(matches!(
            authority
                .observe_revision(candidate)
                .await
                .unwrap_err()
                .kind(),
            ErrorKind::Expired
        ));

        let latest = authority.observe_latest().await.unwrap().revision();
        let report = expire_managed_retention(
            authority.as_ref(),
            &staging,
            &[candidate],
            &[],
            CommitId::generate(),
        )
        .await
        .unwrap();
        assert_eq!(report.state, ExpiryReportState::AlreadyExpired);
        assert_eq!(authority.observe_latest().await.unwrap().revision(), latest);
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn expiry_refuses_a_clean_recoverable_handle_base() {
        use crate::support::TestBackend;
        use std::sync::Arc;
        use yinyang_core::Fs;

        let backend = TestBackend::default();
        let fs = Fs::create(backend.operator()).await.unwrap();
        let authority: Arc<dyn Authority> = Arc::new(fs);
        let temp = tempdir().unwrap();
        let staging = temp.path().join("state");
        let runtime = Runtime::open(authority.clone(), &staging).await.unwrap();
        let (node, receipt) = runtime
            .create_file_with_node(authority.root(), "file", false)
            .await
            .unwrap();
        let handle = runtime
            .open_node(node, true)
            .await
            .unwrap()
            .release()
            .unwrap();
        runtime
            .create_directory(authority.root(), "later")
            .await
            .unwrap();
        drop(runtime);

        let protected = receipt.cursor.revision;
        let report = expire_managed_retention(
            authority.as_ref(),
            &staging,
            &[protected],
            &[],
            CommitId::generate(),
        )
        .await
        .unwrap();
        assert_eq!(report.state, ExpiryReportState::Refused);
        assert!(
            report.revisions.iter().any(|decision| {
                decision.revision == revision_hex(protected)
                    && decision.state == ExpiryDecisionState::Protected
                    && decision.reasons.contains(&"staging_base")
            }),
            "{:#?}",
            report.revisions
        );
        assert!(authority.observe_revision(protected).await.is_ok());

        let runtime = Runtime::open(authority.clone(), &staging).await.unwrap();
        let mut recovered = runtime.recover(handle).await.unwrap();
        recovered.close().await.unwrap();
        drop(recovered);
        drop(runtime);
        let report = expire_managed_retention(
            authority.as_ref(),
            &staging,
            &[protected],
            &[],
            CommitId::generate(),
        )
        .await
        .unwrap();
        assert_eq!(report.state, ExpiryReportState::Committed);
    }
}
