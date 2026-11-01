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
//! opening a writer, acquiring a runtime lease, or migrating local databases.

#[cfg(target_os = "linux")]
use crate::replica::{Replica, ReplicaBaseline, ReplicaStateLease, inspect_backup_package};
#[cfg(target_os = "linux")]
use crate::runtime::StagingLease;
use crate::runtime::{ErrorKind as RuntimeErrorKind, Runtime};
use crate::sync::{NamespaceState, NamespaceStatus, Sync};
use opendal::{EntryMode, Operator};
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use yinyang_core::reachability::PhysicalReferenceKind;
use yinyang_core::{
    Authority, Error, ErrorKind, ExpiryId, NodeId, NodeKind, Result, Revision, Snapshot,
};
#[cfg(target_os = "linux")]
use yinyang_core::{ExpiryConflictKind, ExpiryOutcome, HistoryEntry, RevisionState};

pub const SPACE_REPORT_SCHEMA: &str = "yinyang.managed-space.report.v3";
pub const RETENTION_PLAN_SCHEMA: &str = "yinyang.managed-retention.plan.v3";
pub const RETENTION_EXPIRY_SCHEMA: &str = "yinyang.managed-retention.expiry.v1";

const HEAD_KEY: &str = ".yinyang/head";
const PACK_ROOT: &str = ".yinyang/v2";
const SCAN_LIMIT: usize = 4096;

/// Local writer leases held by an explicit expiry operation.
///
/// Acquisition follows the existing Managed Sync startup order: replica state,
/// Sync content runtime, then any Mount/file runtime. The guards fence all
/// known configured writer entry points through the Core mutation.
#[cfg(target_os = "linux")]
pub(crate) struct ExpiryLeases {
    _replica: ReplicaStateLease,
    _sync_content: StagingLease,
    _runtime: StagingLease,
}

#[cfg(target_os = "linux")]
impl ExpiryLeases {
    pub(crate) async fn acquire(staging: &Path) -> std::result::Result<Self, ExpiryLeaseError> {
        let replica =
            ReplicaStateLease::acquire(staging).map_err(ExpiryLeaseError::from_replica)?;
        let sync_content = StagingLease::acquire(staging.join("content"))
            .await
            .map_err(|error| ExpiryLeaseError::from_runtime("sync-content", error))?;
        let runtime = StagingLease::acquire(staging.to_owned())
            .await
            .map_err(|error| ExpiryLeaseError::from_runtime("runtime", error))?;
        Ok(Self {
            _replica: replica,
            _sync_content: sync_content,
            _runtime: runtime,
        })
    }
}

#[cfg(target_os = "linux")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ExpiryLeaseErrorKind {
    Busy,
    Unavailable,
}

#[cfg(target_os = "linux")]
#[derive(Debug)]
pub(crate) struct ExpiryLeaseError {
    kind: ExpiryLeaseErrorKind,
    subject: &'static str,
    detail: String,
}

#[cfg(target_os = "linux")]
impl ExpiryLeaseError {
    fn from_replica(error: crate::replica::Error) -> Self {
        let kind = if error.kind() == crate::replica::ErrorKind::Busy {
            ExpiryLeaseErrorKind::Busy
        } else {
            ExpiryLeaseErrorKind::Unavailable
        };
        Self {
            kind,
            subject: "replica",
            detail: error.to_string(),
        }
    }

    fn from_runtime(subject: &'static str, error: crate::runtime::Error) -> Self {
        let kind = if error.kind() == RuntimeErrorKind::Busy {
            ExpiryLeaseErrorKind::Busy
        } else {
            ExpiryLeaseErrorKind::Unavailable
        };
        Self {
            kind,
            subject,
            detail: error.to_string(),
        }
    }

    pub(crate) fn is_busy(&self) -> bool {
        self.kind == ExpiryLeaseErrorKind::Busy
    }
}

#[cfg(target_os = "linux")]
impl std::fmt::Display for ExpiryLeaseError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "{} local-state lease: {} ({})",
            self.subject,
            self.detail,
            if self.is_busy() {
                "busy"
            } else {
                "unavailable"
            }
        )
    }
}

#[cfg(target_os = "linux")]
impl std::error::Error for ExpiryLeaseError {}

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
    pub replica: ReplicaEvidence,
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
    pub namespace_intents: u64,
    pub namespace_prepared: u64,
    pub namespace_unknown: u64,
    pub namespace_committed: u64,
    pub namespace_conflict: u64,
    pub namespace_retryable: u64,
    pub namespace_failed: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct SyncEvidence {
    pub state: StagingState,
    pub content_edits: u64,
    pub namespace_intents: u64,
    pub namespace_prepared: u64,
    pub namespace_unknown: u64,
    pub namespace_committed: u64,
    pub namespace_conflict: u64,
    pub namespace_retryable: u64,
    pub namespace_failed: u64,
    pub commit_ids: Vec<String>,
    pub error: Option<String>,
}

impl SyncEvidence {
    fn missing() -> Self {
        Self {
            state: StagingState::Missing,
            content_edits: 0,
            namespace_intents: 0,
            namespace_prepared: 0,
            namespace_unknown: 0,
            namespace_committed: 0,
            namespace_conflict: 0,
            namespace_retryable: 0,
            namespace_failed: 0,
            commit_ids: Vec::new(),
            error: None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ReplicaEvidence {
    pub state: StagingState,
    pub pending_installs: Option<u64>,
    pub artifacts: Option<u64>,
    pub exportable_artifacts: Option<u64>,
    pub conflicts: Option<u64>,
    pub baseline_coverage: &'static str,
    pub baseline: Option<ReplicaBaselineEvidence>,
    pub error: Option<String>,
}

impl ReplicaEvidence {
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
pub struct ReplicaBaselineEvidence {
    pub revision: String,
    pub entry_revisions: Vec<String>,
}

impl ReplicaBaselineEvidence {
    #[cfg(target_os = "linux")]
    fn from_baseline(baseline: ReplicaBaseline) -> Self {
        Self {
            revision: revision_hex(baseline.revision),
            entry_revisions: baseline.entries.into_iter().map(revision_hex).collect(),
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
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct RetentionRevision {
    pub revision: String,
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
    pub replica: ReplicaEvidence,
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
            logical_retained: "authority_retained_revision_metadata",
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
        return unavailable_backup(
            &path,
            None,
            "backup package inspection is available only on Linux".to_owned(),
        );
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
        let package_filesystem = inspection.filesystem;
        let filesystem_hex = identity_hex(package_filesystem);
        if package_filesystem != filesystem {
            let error = format!(
                "backup filesystem {filesystem_hex} does not match volume filesystem {}",
                identity_hex(filesystem)
            );
            return BackupPackageEvidence {
                path,
                state: BackupPackageState::Foreign,
                filesystem: Some(filesystem_hex),
                staging: None,
                error: Some(error),
                protected: BTreeMap::new(),
            };
        }
        let state_directory = backup.join("state");
        if let Err(error) = Sync::validate_restore(&state_directory, filesystem).await {
            return unavailable_backup(&path, Some(filesystem_hex), error.to_string());
        }
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
            filesystem: Some(filesystem_hex),
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
        replica: staging.replica.clone(),
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
        "sync_namespace_pending" => "backup_sync_namespace_pending",
        "sync_namespace_unknown" => "backup_sync_namespace_unknown",
        "sync_namespace_receipt" => "backup_sync_namespace_receipt",
        "sync_namespace_conflict" => "backup_sync_namespace_conflict",
        "sync_namespace_retryable" => "backup_sync_namespace_retryable",
        "sync_namespace_failed" => "backup_sync_namespace_failed",
        "sync_receipt" => "backup_sync_receipt",
        "replica_conflict" => "backup_replica_conflict",
        "replica_baseline" => "backup_replica_baseline",
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
    let (revisions, retained_revision_count, state) = match authority.retained_revisions().await {
        Ok(revisions) => {
            let mut revisions = revisions;
            revisions.sort();
            revisions.dedup();
            if revisions.binary_search(&latest.revision()).is_err() {
                revisions.push(latest.revision());
                revisions.sort();
            }
            let count = revisions.len() as u64;
            (revisions, Some(count), MeasurementState::Available)
        }
        Err(error) => {
            unresolved.push(Unresolved {
                subject: "retained-revisions".to_owned(),
                state: "unavailable".to_owned(),
                detail: error.to_string(),
            });
            (vec![latest.revision()], None, MeasurementState::Unavailable)
        }
    };

    let retained_set = revisions.iter().copied().collect::<BTreeSet<_>>();
    let pins = requested_pins.iter().copied().collect::<BTreeSet<_>>();
    let requested = requested_pins
        .iter()
        .map(|revision| RequestedPin {
            revision: revision_hex(*revision),
            retained: retained_revision_count.map(|_| retained_set.contains(revision)),
        })
        .collect::<Vec<_>>();
    if retained_revision_count.is_some() {
        for revision in requested_pins {
            if retained_set.contains(revision) {
                continue;
            }
            unresolved.push(Unresolved {
                subject: "requested-pin".to_owned(),
                state: "not_retained".to_owned(),
                detail: revision_hex(*revision),
            });
        }
        for revision in staging.protected.keys() {
            if !retained_set.contains(revision) {
                unresolved.push(Unresolved {
                    subject: "local-staging-pin".to_owned(),
                    state: "not_retained".to_owned(),
                    detail: revision_hex(*revision),
                });
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
            if !retained_set.contains(revision) {
                unresolved.push(Unresolved {
                    subject: "backup-pin".to_owned(),
                    state: "not_retained".to_owned(),
                    detail: revision_hex(*revision),
                });
            }
        }
    }

    let bounded = keep_last.is_some() || !requested_pins.is_empty();
    let keep_last_set = keep_last
        .map(|count| {
            let count = usize::try_from(count).unwrap_or(usize::MAX);
            revisions
                .iter()
                .copied()
                .skip(revisions.len().saturating_sub(count))
                .collect::<BTreeSet<_>>()
        })
        .unwrap_or_default();
    let mut protected = staging.protected.clone();
    merge_protected(&mut protected, &backup_protected);
    let protected = &protected;
    let mut decisions = Vec::with_capacity(revisions.len());
    for revision in &revisions {
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

/// Explicitly expire the requested historical revisions after validating all
/// configured local protection evidence under the existing writer leases.
#[cfg(not(target_os = "linux"))]
pub async fn expire_managed_retention(
    _authority: &dyn Authority,
    _staging: &Path,
    _candidates: &[Revision],
    _backup_packages: &[PathBuf],
    _operation_id: ExpiryId,
) -> Result<ManagedExpiryReport> {
    Err(Error::new(
        ErrorKind::Unsupported,
        "expire managed retention",
        "logical history expiry requires the Linux local-state lease protocol",
    ))
}

/// Explicitly expire the requested historical revisions after validating all
/// configured local protection evidence under the existing writer leases.
#[cfg(target_os = "linux")]
pub async fn expire_managed_retention(
    authority: &dyn Authority,
    staging_path: &Path,
    candidates: &[Revision],
    backup_packages: &[PathBuf],
    operation_id: ExpiryId,
) -> Result<ManagedExpiryReport> {
    let candidates = candidates.iter().copied().collect::<BTreeSet<_>>();
    if candidates.is_empty() {
        return Err(Error::new(
            ErrorKind::Invalid,
            "expire managed retention",
            "at least one revision is required",
        ));
    }

    let leases = ExpiryLeases::acquire(staging_path).await.map_err(|error| {
        let kind = if error.is_busy() {
            ErrorKind::Invalid
        } else {
            ErrorKind::Io
        };
        Error::new(kind, "expire managed retention", error.to_string())
    })?;
    let observed = authority.observe_latest().await?;
    let expected_latest = observed.revision();
    let staging = inspect_bound_local_staging(staging_path, authority.filesystem()).await;
    let backups = inspect_backup_packages(backup_packages, authority.filesystem()).await;
    let history = collect_expiry_history(authority, expected_latest).await?;
    let mut unresolved = staging
        .errors
        .iter()
        .map(|detail| Unresolved {
            subject: "local-staging".to_owned(),
            state: "unavailable".to_owned(),
            detail: detail.clone(),
        })
        .collect::<Vec<_>>();
    let mut evidence_complete = matches!(
        staging.state,
        StagingState::Missing | StagingState::NotInitialized | StagingState::Available
    ) && staging.binding.state == StagingBindingState::Verified;
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

    let mut protected = staging.protected.clone();
    let mut backup_protected = BTreeMap::<Revision, BTreeSet<&'static str>>::new();
    for package in &backups.packages {
        for (revision, reasons) in &package.protected {
            let entry = backup_protected.entry(*revision).or_default();
            for reason in reasons {
                entry.insert(backup_reason(reason));
            }
        }
    }
    merge_protected(&mut protected, &backup_protected);

    for (revision, reasons) in &protected {
        if *revision == expected_latest {
            continue;
        }
        let state = history.get(revision);
        let detail = match state {
            Some(RevisionState::Readable) => continue,
            Some(RevisionState::Expired(_)) => "protected-expired",
            None => "protected-missing",
        };
        evidence_complete = false;
        unresolved.push(Unresolved {
            subject: "protected-revision".to_owned(),
            state: detail.to_owned(),
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
        &staging,
        &backups,
        &protected,
        &unresolved,
    )?;
    let mut decisions = candidates
        .iter()
        .map(|revision| classify_expiry_candidate(*revision, expected_latest, &history, &protected))
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
            &protected,
            decisions,
            unresolved,
        ));
    }

    let expire = decisions
        .iter()
        .filter(|decision| decision.state == ExpiryDecisionState::Eligible)
        .map(|decision| Revision::from_bytes(parse_revision_hex(&decision.revision)))
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
            &protected,
            decisions,
            unresolved,
        ));
    }

    let protected_for_request = protected
        .keys()
        .copied()
        .filter(|revision| *revision != expected_latest)
        .collect::<Vec<_>>();
    let request = ExpiryRequest::new(
        authority.filesystem(),
        operation_id,
        expected_latest,
        expire,
        protected_for_request,
        evidence_digest,
    )?;
    let outcome = authority.expire_revisions(&request).await?;
    let (state, after_revision) = match &outcome {
        ExpiryOutcome::Committed(receipt) => {
            for decision in &mut decisions {
                if receipt
                    .expired
                    .iter()
                    .any(|revision| revision_hex(*revision) == decision.revision)
                {
                    decision.state = ExpiryDecisionState::Expired;
                }
            }
            (ExpiryReportState::Committed, Some(receipt.after))
        }
        ExpiryOutcome::Refused(refusal) => {
            let mut only_already = true;
            for conflict in &refusal.conflicts {
                if conflict.kind != ExpiryConflictKind::AlreadyExpired {
                    only_already = false;
                }
                if let Some(decision) = decisions
                    .iter_mut()
                    .find(|decision| decision.revision == revision_hex(conflict.revision))
                {
                    decision.state = expiry_conflict_state(conflict.kind);
                    decision.reasons = protected
                        .get(&conflict.revision)
                        .map(|reasons| reasons.iter().copied().collect())
                        .unwrap_or_default();
                }
            }
            let state = if only_already {
                ExpiryReportState::AlreadyExpired
            } else {
                ExpiryReportState::Refused
            };
            (state, None)
        }
        ExpiryOutcome::Conflict => (ExpiryReportState::Conflict, None),
        ExpiryOutcome::Retryable => (ExpiryReportState::Retryable, None),
        ExpiryOutcome::Unknown(_) => (ExpiryReportState::Unknown, None),
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
        &protected,
        decisions,
        unresolved,
    ))
}

#[cfg(target_os = "linux")]
async fn collect_expiry_history(
    authority: &dyn Authority,
    latest: Revision,
) -> Result<BTreeMap<Revision, RevisionState>> {
    let mut after = None;
    let mut history = BTreeMap::new();
    loop {
        let page = authority
            .history_page(after, yinyang_core::RETAINED_REVISIONS_PAGE_MAX)
            .await?;
        let page_len = page.entries.len();
        for HistoryEntry { revision, state } in page.entries {
            if history.insert(revision, state).is_some() {
                return Err(Error::new(
                    ErrorKind::Corrupt,
                    "read expiry history",
                    "history page repeated a revision",
                ));
            }
        }
        match page.next {
            Some(next)
                if page_len > 0
                    && after.is_none_or(|cursor| next > cursor)
                    && history.last_key_value().map(|(revision, _)| *revision) == Some(next) =>
            {
                after = Some(next);
            }
            Some(_) => {
                return Err(Error::new(
                    ErrorKind::Corrupt,
                    "read expiry history",
                    "history continuation does not advance",
                ));
            }
            None => break,
        }
    }
    if !history.contains_key(&latest) {
        return Err(Error::new(
            ErrorKind::Corrupt,
            "read expiry history",
            "latest revision is absent from the history index",
        ));
    }
    Ok(history)
}

#[cfg(target_os = "linux")]
fn classify_expiry_candidate(
    revision: Revision,
    latest: Revision,
    history: &BTreeMap<Revision, RevisionState>,
    protected: &BTreeMap<Revision, BTreeSet<&'static str>>,
) -> ExpiryRevisionDecision {
    let protected_reasons = protected
        .get(&revision)
        .map(|reasons| reasons.iter().copied().collect())
        .unwrap_or_default();
    let state = if revision == latest {
        ExpiryDecisionState::Current
    } else {
        match history.get(&revision) {
            Some(RevisionState::Readable) if protected.contains_key(&revision) => {
                ExpiryDecisionState::Protected
            }
            Some(RevisionState::Readable) => ExpiryDecisionState::Eligible,
            Some(RevisionState::Expired(_)) if protected.contains_key(&revision) => {
                ExpiryDecisionState::ProtectedExpired
            }
            Some(RevisionState::Expired(_)) => ExpiryDecisionState::AlreadyExpired,
            None if protected.contains_key(&revision) => ExpiryDecisionState::ProtectedMissing,
            None => ExpiryDecisionState::Missing,
        }
    };
    ExpiryRevisionDecision {
        revision: revision_hex(revision),
        state,
        reasons: protected_reasons,
    }
}

#[cfg(target_os = "linux")]
fn expiry_conflict_state(kind: ExpiryConflictKind) -> ExpiryDecisionState {
    match kind {
        ExpiryConflictKind::Current => ExpiryDecisionState::Current,
        ExpiryConflictKind::Missing => ExpiryDecisionState::Missing,
        ExpiryConflictKind::AlreadyExpired => ExpiryDecisionState::AlreadyExpired,
        ExpiryConflictKind::Protected => ExpiryDecisionState::Protected,
        ExpiryConflictKind::ProtectedMissing => ExpiryDecisionState::ProtectedMissing,
        ExpiryConflictKind::ProtectedExpired => ExpiryDecisionState::ProtectedExpired,
    }
}

#[cfg(target_os = "linux")]
fn expiry_report(
    authority: &dyn Authority,
    operation_id: ExpiryId,
    observed: Revision,
    after: Option<Revision>,
    state: ExpiryReportState,
    evidence_digest: [u8; 32],
    staging: &StagingSpace,
    backups: &BackupProtection,
    protected: &BTreeMap<Revision, BTreeSet<&'static str>>,
    revisions: Vec<ExpiryRevisionDecision>,
    unresolved: Vec<Unresolved>,
) -> ManagedExpiryReport {
    ManagedExpiryReport {
        schema: RETENTION_EXPIRY_SCHEMA,
        filesystem: identity_hex(authority.filesystem()),
        operation_id: expiry_id_string(operation_id),
        observed_revision: revision_hex(observed),
        after_revision: after.map(revision_hex),
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
        limits: expiry_limits(protected.len() as u64, revisions.len() as u64),
    }
}

#[cfg(target_os = "linux")]
fn expiry_evidence_digest(
    authority: &dyn Authority,
    expected_latest: Revision,
    staging: &StagingSpace,
    backups: &BackupProtection,
    protected: &BTreeMap<Revision, BTreeSet<&'static str>>,
    unresolved: &[Unresolved],
) -> Result<[u8; 32]> {
    let protected = protected
        .iter()
        .map(|(revision, reasons)| {
            (
                revision.to_bytes(),
                reasons
                    .iter()
                    .map(|reason| (*reason).to_owned())
                    .collect::<Vec<_>>(),
            )
        })
        .collect::<Vec<_>>();
    let mut backup_rows = backups
        .packages
        .iter()
        .map(|package| {
            let protected = package
                .protected
                .iter()
                .map(|(revision, reasons)| {
                    (
                        revision.to_bytes(),
                        reasons
                            .iter()
                            .map(|reason| (*reason).to_owned())
                            .collect::<Vec<_>>(),
                    )
                })
                .collect::<Vec<_>>();
            (
                backup_state_tag(package.state),
                package.filesystem.map(|volume| parse_identity_hex(&volume)),
                *blake3::hash(package.path.as_bytes()).as_bytes(),
                protected,
            )
        })
        .collect::<Vec<_>>();
    backup_rows.sort();
    let unresolved = unresolved
        .iter()
        .map(|item| {
            (
                item.subject.clone(),
                item.state.clone(),
                item.detail.clone(),
            )
        })
        .collect::<Vec<_>>();
    let bytes = borsh::to_vec(&(
        *b"YYMEXP01",
        authority.filesystem().as_bytes(),
        expected_latest.to_bytes(),
        staging_state_tag(staging.state),
        staging_binding_tag(staging.binding.state),
        *blake3::hash(staging.path.as_bytes()).as_bytes(),
        staging.handles.total,
        staging.handles.pending,
        staging.handles.frozen,
        staging.handles.conflict,
        staging.handles.unknown,
        staging.handles.failed,
        staging.sync.namespace_prepared,
        staging.sync.namespace_unknown,
        staging.sync.namespace_committed,
        staging.sync.namespace_conflict,
        staging.sync.namespace_retryable,
        staging.sync.namespace_failed,
        protected,
        backup_rows,
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
fn expiry_id_string(id: ExpiryId) -> String {
    uuid::Uuid::from_bytes(*id.as_bytes()).to_string()
}

#[cfg(target_os = "linux")]
fn parse_revision_hex(value: &str) -> [u8; 24] {
    let mut bytes = [0_u8; 24];
    for (index, byte) in bytes.iter_mut().enumerate() {
        let start = index * 2;
        *byte = u8::from_str_radix(&value[start..start + 2], 16)
            .expect("expiry decisions always contain canonical revisions");
    }
    bytes
}

#[cfg(target_os = "linux")]
fn parse_identity_hex(value: &str) -> [u8; 16] {
    let mut bytes = [0_u8; 16];
    for (index, byte) in bytes.iter_mut().enumerate() {
        let start = index * 2;
        *byte = u8::from_str_radix(&value[start..start + 2], 16)
            .expect("backup package identities are always canonical");
    }
    bytes
}

#[cfg(target_os = "linux")]
fn expiry_limits(protected: u64, requested: u64) -> Vec<String> {
    vec![
        format!(
            "Observed {protected} locally protected revisions and requested {requested} explicit expiry candidates."
        ),
        "Logical expiry never authorizes physical deletion or estimates reclaimable bytes.".to_owned(),
        "External readers, untracked pins, other staging directories, and undisclosed backups remain unknown.".to_owned(),
        "Quiescence is not observed; the operator must stop every writer, reader, mount, Sync process, and backup activity.".to_owned(),
        "Configured local-state writer entry points are held under their existing protocol leases through the Core mutation.".to_owned(),
        "Mutable authority metadata may advance; pre-existing immutable content and packs are not rewritten or deleted.".to_owned(),
        "Default keep-all remains unchanged and does not resurrect an explicitly expired revision.".to_owned(),
    ]
}

/// Inspect existing Managed content, Sync, and replica evidence without creating
/// a directory or database, acquiring a runtime lease, migrating state, or
/// recovering records. Known SQLite read-only behavior may create or update
/// derived WAL/SHM files.
pub async fn inspect_local_staging(staging: &Path) -> StagingSpace {
    let content = inspect_local_content_staging(staging).await;
    let sync = inspect_sync_evidence(staging).await;
    let replica = inspect_replica_evidence(staging).await;
    assemble_staging_space(staging, content, sync, replica)
}

/// Inspect a validated backup package state directory. The package validator
/// already verified database hashes, the manifest, and the root payload, so
/// this path reads protection facts without opening the original local root.
#[cfg(target_os = "linux")]
async fn inspect_package_staging(staging: &Path) -> StagingSpace {
    let content = inspect_local_content_staging(staging).await;
    let sync = inspect_sync_evidence(staging).await;
    let replica = inspect_replica_protection_evidence(staging).await;
    assemble_staging_space(staging, content, sync, replica)
}

fn assemble_staging_space(
    staging: &Path,
    mut content: StagingSpace,
    sync: SyncInspection,
    replica: ReplicaInspection,
) -> StagingSpace {
    merge_protected(&mut content.protected, &sync.protected);
    merge_protected(&mut content.protected, &replica.protected);
    content
        .frozen_commits
        .extend(sync.frozen_commits.iter().cloned());
    content.frozen_commits.sort();
    content.frozen_commits.dedup();
    content.handles.namespace_intents = sync.evidence.namespace_intents;
    content.handles.namespace_prepared = sync.evidence.namespace_prepared;
    content.handles.namespace_unknown = sync.evidence.namespace_unknown;
    content.handles.namespace_committed = sync.evidence.namespace_committed;
    content.handles.namespace_conflict = sync.evidence.namespace_conflict;
    content.handles.namespace_retryable = sync.evidence.namespace_retryable;
    content.handles.namespace_failed = sync.evidence.namespace_failed;
    push_unique(&mut content.errors, sync.evidence.error.clone());
    push_unique(&mut content.errors, replica.evidence.error.clone());
    if sync.evidence.state != StagingState::Missing {
        content.databases.push(StagingDatabase {
            kind: "sync-journal",
            path: display_path(&staging.join("sync.db")),
            state: measurement_state(sync.evidence.state),
            error: sync.evidence.error.clone(),
        });
    }
    if replica.evidence.state != StagingState::Missing {
        content.databases.push(StagingDatabase {
            kind: "replica",
            path: display_path(&staging.join("replica.db")),
            state: measurement_state(replica.evidence.state),
            error: replica.evidence.error.clone(),
        });
    }
    content.protected_revisions = protected_revisions(&content.protected);
    content.sync = sync.evidence;
    content.replica = replica.evidence;
    content.state = staging_state(&content.databases, &content.errors, content.state);
    content
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
    let content = staging.join("content");
    if path_exists(&sync)? {
        Sync::validate_restore(staging, filesystem)
            .await
            .map_err(|error| {
                format!(
                    "validate Sync binding for filesystem {}: {error}",
                    identity_hex(filesystem)
                )
            })?;
    } else if path_exists(&content.join("staging.db"))? {
        Runtime::validate_staging(&content, filesystem)
            .await
            .map_err(|error| {
                format!(
                    "validate Sync content staging binding for filesystem {}: {error}",
                    identity_hex(filesystem)
                )
            })?;
    }

    let replica = staging.join("replica.db");
    if path_exists(&replica)? {
        #[cfg(target_os = "linux")]
        {
            let status = Replica::inspect(staging).map_err(|error| {
                format!(
                    "validate replica binding for filesystem {}: {error}",
                    identity_hex(filesystem)
                )
            })?;
            if status.filesystem != filesystem {
                return Err(format!(
                    "replica filesystem {} does not match volume filesystem {}",
                    identity_hex(status.filesystem),
                    identity_hex(filesystem)
                ));
            }
        }
        #[cfg(not(target_os = "linux"))]
        {
            return Err(
                "replica binding cannot be verified on this platform; foreign state is not attributed"
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
                replica: ReplicaEvidence::missing(),
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
        replica: ReplicaEvidence::missing(),
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

struct ReplicaInspection {
    evidence: ReplicaEvidence,
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
            inspection.evidence.content_edits = sync.content.len() as u64;
            let mut commit_ids = BTreeSet::new();
            for status in sync.namespace {
                absorb_namespace(
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

async fn inspect_replica_evidence(staging: &Path) -> ReplicaInspection {
    let database = staging.join("replica.db");
    let mut inspection = ReplicaInspection {
        evidence: ReplicaEvidence::missing(),
        protected: BTreeMap::new(),
    };
    match database.try_exists() {
        Ok(false) => return inspection,
        Err(error) => {
            inspection.evidence.state = StagingState::Unavailable;
            inspection.evidence.baseline_coverage = "unavailable";
            inspection.evidence.error = Some(format!(
                "inspect replica state {}: {error}",
                database.display()
            ));
            return inspection;
        }
        Ok(true) => {}
    }
    #[cfg(target_os = "linux")]
    {
        match Replica::inspect(staging) {
            Ok(status) => {
                let pending = u64::try_from(status.pending_installs).unwrap_or(u64::MAX);
                let baseline = status.baseline.clone();
                inspection.evidence = ReplicaEvidence {
                    state: if pending > 0 {
                        StagingState::Partial
                    } else {
                        StagingState::Available
                    },
                    pending_installs: Some(pending),
                    artifacts: Some(status.artifacts.len() as u64),
                    exportable_artifacts: Some(
                        status
                            .artifacts
                            .iter()
                            .filter(|artifact| artifact.exportable)
                            .count() as u64,
                    ),
                    conflicts: Some(status.conflicts.len() as u64),
                    baseline_coverage: "observed_from_read_only_baseline",
                    baseline: baseline
                        .as_ref()
                        .map(|value| ReplicaBaselineEvidence::from_baseline(value.clone())),
                    error: None,
                };
                if pending > 0 {
                    inspection.evidence.error = Some(format!(
                        "replica has {pending} pending installs; retained artifacts, conflicts, and baseline revisions are observable"
                    ));
                }
                if let Some(baseline) = baseline {
                    inspection
                        .protected
                        .entry(baseline.revision)
                        .or_default()
                        .insert("replica_baseline");
                    for revision in baseline.entries {
                        inspection
                            .protected
                            .entry(revision)
                            .or_default()
                            .insert("replica_baseline");
                    }
                }
                for conflict in status.conflicts {
                    if let Some(revision) = conflict.remote_revision {
                        inspection
                            .protected
                            .entry(Revision::from_bytes(revision))
                            .or_default()
                            .insert("replica_conflict");
                    }
                }
            }
            Err(error) => {
                inspection.evidence.state = StagingState::Unavailable;
                inspection.evidence.baseline_coverage = "unavailable";
                inspection.evidence.error = Some(error.to_string());
            }
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        inspection.evidence.state = StagingState::Unavailable;
        inspection.evidence.baseline_coverage = "unavailable";
        inspection.evidence.error = Some(
            "replica read-only inspection is available only on Linux; baseline and retained artifact coverage is unobserved"
                .to_owned(),
        );
    }
    inspection
}

/// Read replica revision-protection facts from validated package state without
/// opening the recorded original root or revalidating retained artifact bytes.
#[cfg(target_os = "linux")]
async fn inspect_replica_protection_evidence(staging: &Path) -> ReplicaInspection {
    let database = staging.join("replica.db");
    let mut inspection = ReplicaInspection {
        evidence: ReplicaEvidence::missing(),
        protected: BTreeMap::new(),
    };
    match database.try_exists() {
        Ok(false) => return inspection,
        Err(error) => {
            inspection.evidence.state = StagingState::Unavailable;
            inspection.evidence.baseline_coverage = "unavailable";
            inspection.evidence.error = Some(format!(
                "inspect replica state {}: {error}",
                database.display()
            ));
            return inspection;
        }
        Ok(true) => {}
    }
    #[cfg(target_os = "linux")]
    {
        match Replica::inspect_protection(staging) {
            Ok(status) => {
                let pending = status.pending_installs;
                inspection.evidence = ReplicaEvidence {
                    state: if pending > 0 {
                        StagingState::Partial
                    } else {
                        StagingState::Available
                    },
                    pending_installs: Some(status.pending_installs),
                    artifacts: Some(status.artifacts),
                    exportable_artifacts: Some(status.exportable_artifacts),
                    conflicts: Some(status.conflicts.len() as u64),
                    baseline_coverage: "observed_from_read_only_baseline",
                    baseline: status
                        .baseline
                        .as_ref()
                        .map(|value| ReplicaBaselineEvidence::from_baseline(value.clone())),
                    error: (pending > 0).then(|| {
                        format!(
                            "package replica state has {pending} incomplete installs; protection evidence remains readable"
                        )
                    }),
                };
                if let Some(baseline) = status.baseline {
                    inspection
                        .protected
                        .entry(baseline.revision)
                        .or_default()
                        .insert("replica_baseline");
                    for revision in baseline.entries {
                        inspection
                            .protected
                            .entry(revision)
                            .or_default()
                            .insert("replica_baseline");
                    }
                }
                for revision in status.conflicts {
                    inspection
                        .protected
                        .entry(revision)
                        .or_default()
                        .insert("replica_conflict");
                }
            }
            Err(error) => {
                inspection.evidence.state = StagingState::Unavailable;
                inspection.evidence.baseline_coverage = "unavailable";
                inspection.evidence.error = Some(error.to_string());
            }
        }
    }
    inspection
}

fn absorb_namespace(
    status: NamespaceStatus,
    evidence: &mut SyncEvidence,
    protected: &mut BTreeMap<Revision, BTreeSet<&'static str>>,
    commit_ids: &mut BTreeSet<String>,
    frozen_commits: &mut BTreeSet<String>,
) {
    evidence.namespace_intents = evidence.namespace_intents.saturating_add(1);
    commit_ids.insert(commit_hex(status.intent.commit_id));
    let reason = match &status.state {
        NamespaceState::Prepared => {
            evidence.namespace_prepared = evidence.namespace_prepared.saturating_add(1);
            frozen_commits.insert(commit_hex(status.intent.commit_id));
            "sync_namespace_pending"
        }
        NamespaceState::Unknown => {
            evidence.namespace_unknown = evidence.namespace_unknown.saturating_add(1);
            frozen_commits.insert(commit_hex(status.intent.commit_id));
            "sync_namespace_unknown"
        }
        NamespaceState::Committed(receipt) => {
            evidence.namespace_committed = evidence.namespace_committed.saturating_add(1);
            protected
                .entry(receipt.cursor.revision)
                .or_default()
                .insert("sync_receipt");
            "sync_namespace_receipt"
        }
        NamespaceState::Conflict => {
            evidence.namespace_conflict = evidence.namespace_conflict.saturating_add(1);
            frozen_commits.insert(commit_hex(status.intent.commit_id));
            "sync_namespace_conflict"
        }
        NamespaceState::Retryable => {
            evidence.namespace_retryable = evidence.namespace_retryable.saturating_add(1);
            frozen_commits.insert(commit_hex(status.intent.commit_id));
            "sync_namespace_retryable"
        }
        NamespaceState::Failed => {
            evidence.namespace_failed = evidence.namespace_failed.saturating_add(1);
            frozen_commits.insert(commit_hex(status.intent.commit_id));
            "sync_namespace_failed"
        }
    };
    protected
        .entry(status.intent.baseline)
        .or_default()
        .insert(reason);
    if let Some(commit) = status.failure.and_then(|failure| failure.commit_id()) {
        commit_ids.insert(commit_hex(commit));
        frozen_commits.insert(commit_hex(commit));
    }
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

/// Inspect existing Managed content staging databases without creating a
/// directory or database, acquiring a runtime lease, migrating state, or
/// recovering records.
async fn inspect_local_content_staging(staging: &Path) -> StagingSpace {
    let mut errors = Vec::new();
    let directory = match measure_directory(staging).await {
        Ok(usage) => Some(usage),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => {
            errors.push(format!("measure {}: {error}", staging.display()));
            None
        }
    };
    let candidates = [
        ("mount", staging.join("staging.db")),
        ("sync-content", staging.join("content").join("staging.db")),
    ];
    let mut existing = Vec::new();
    let mut detection_failed = false;
    for (kind, path) in candidates {
        match path.try_exists() {
            Ok(true) => existing.push((kind, path)),
            Ok(false) => {}
            Err(error) => {
                detection_failed = true;
                errors.push(format!(
                    "inspect staging database {}: {error}",
                    path.display()
                ));
            }
        }
    }
    if existing.is_empty() {
        return StagingSpace {
            path: display_path(staging),
            state: if detection_failed {
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
            replica: ReplicaEvidence::missing(),
            protected_revisions: Vec::new(),
            frozen_commits: Vec::new(),
            errors,
            protected: BTreeMap::new(),
        };
    }

    let mut databases = Vec::new();
    let mut handles = StagingHandles::default();
    let mut protected = BTreeMap::<Revision, BTreeSet<&'static str>>::new();
    let mut frozen_commits = BTreeSet::new();
    let mut handle_revisions = BTreeMap::new();
    let mut valid = 0_u64;
    for (kind, database_path) in existing {
        let parent = database_path
            .parent()
            .expect("staging database always has a parent");
        match Runtime::inspect(parent).await {
            Ok(status) => {
                valid += 1;
                databases.push(StagingDatabase {
                    kind,
                    path: display_path(&database_path),
                    state: MeasurementState::Available,
                    error: None,
                });
                for handle in status.handles {
                    handle_revisions.insert(handle.id, handle.remote_revision);
                    handles.total = handles.total.saturating_add(1);
                    handles.handle_bytes = handles.handle_bytes.saturating_add(handle.length);
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
            }
            Err(error) => {
                databases.push(StagingDatabase {
                    kind,
                    path: display_path(&database_path),
                    state: MeasurementState::Unavailable,
                    error: Some(error.to_string()),
                });
                errors.push(format!("{}: {error}", database_path.display()));
            }
        }
    }
    let protected_revisions = protected
        .iter()
        .map(|(revision, reasons)| ProtectedRevision {
            revision: revision_hex(*revision),
            reasons: reasons.iter().copied().collect(),
        })
        .collect::<Vec<_>>();
    let state = if valid == databases.len() as u64 {
        StagingState::Available
    } else if valid == 0 {
        StagingState::Unavailable
    } else {
        StagingState::Partial
    };
    StagingSpace {
        path: display_path(staging),
        state,
        binding: StagingBinding::unbound(),
        directory,
        databases,
        handles,
        sync: SyncEvidence::missing(),
        replica: ReplicaEvidence::missing(),
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
    match authority.retained_revisions().await {
        Ok(mut revisions) => {
            revisions.sort();
            revisions.dedup();
            if revisions.binary_search(&latest.revision()).is_err() {
                revisions.push(latest.revision());
                revisions.sort();
            }
            let revision_count = revisions.len() as u64;
            let mut observed = Vec::with_capacity(revisions.len());
            let mut observed_file_bytes = 0_u64;
            let mut unique = BTreeSet::new();
            let mut observed_unique_content_bytes = 0_u64;
            let mut complete = true;
            for revision in revisions {
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
        ErrorKind::Expired => "expired",
        ErrorKind::NotFound => "missing",
        ErrorKind::Corrupt => "corrupt",
        ErrorKind::Invalid | ErrorKind::InvalidName => "invalid",
        ErrorKind::Unsupported => "unsupported",
        ErrorKind::Storage | ErrorKind::Io | ErrorKind::PermissionDenied => "unavailable",
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
                    treatment: "receipt records remain namespace evidence; they are not a deletion authorization",
                    error: Some(error.to_string()),
                };
            }
        }
    }
    ReceiptEvidence {
        state: MeasurementState::Available,
        records: Some(records),
        revisions: Some(revisions.len() as u64),
        treatment: "receipt records remain namespace evidence; they are not a deletion authorization",
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
        "Receipt records are namespace evidence, not a physical cleanup authorization.",
        "Quiescence is not observed; planning does not stop writers, readers, mounts, Sync processes, or backup activity.",
        "Published history and receipts are retained by default; keep-last changes only the declarative selection, never removes or hides retained revisions, and outside_bounded_policy revisions remain readable.",
        "Keep and keep-last apply only to this invocation; they are not persistent pins.",
        "No expiry command exists in this contract; expiry and physical deletion are separate future designs.",
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
            error.contains("unknown staging profile") || error.contains("unsupported profile")
        }));
        assert!(!staging.join("runtime.lock").exists());
        assert_eq!(std::fs::read(&database).unwrap(), before);
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn expiry_leases_create_only_derived_state_and_fence_writer_entry_points() {
        use crate::core::{BackendProfile, Fs};
        use crate::support::TestBackend;
        use std::sync::Arc;

        let root = tempdir().unwrap();
        let staging = root.path().join("staging");
        let backend = TestBackend::default();
        let authority: Arc<dyn Authority> = Arc::new(
            Fs::create(backend.operator(), BackendProfile::AmazonS3)
                .await
                .unwrap(),
        );

        let leases = ExpiryLeases::acquire(&staging).await.unwrap();
        assert!(staging.join("replica.lock").is_file());
        assert!(staging.join("content/runtime.lock").is_file());
        assert!(staging.join("runtime.lock").is_file());
        for database in ["replica.db", "sync.db", "content/staging.db", "staging.db"] {
            assert!(
                !staging.join(database).exists(),
                "derived lease unexpectedly created {database}"
            );
        }

        let Err(error) = Runtime::open(authority.clone(), &staging).await else {
            panic!("runtime open must be fenced by the expiry lease");
        };
        assert_eq!(error.kind(), RuntimeErrorKind::Busy);

        let Err(error) = Sync::open(authority.clone(), &staging).await else {
            panic!("Sync open must be fenced by the expiry lease");
        };
        assert_eq!(error.kind(), RuntimeErrorKind::Busy);

        let replica_root = root.path().join("replica-root");
        let error = Replica::init(&staging, &replica_root, authority.filesystem()).unwrap_err();
        assert_eq!(error.kind(), crate::replica::ErrorKind::Busy);

        match ExpiryLeases::acquire(&staging).await {
            Err(error) => assert!(error.is_busy()),
            Ok(_) => panic!("second expiry lease acquisition must be busy"),
        }
        for database in ["replica.db", "sync.db", "content/staging.db", "staging.db"] {
            assert!(
                !staging.join(database).exists(),
                "failed writer open unexpectedly created {database}"
            );
        }
        drop(leases);
    }
}
