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
use crate::replica::Replica;
use crate::runtime::{ErrorKind as RuntimeErrorKind, Runtime};
use crate::sync::{NamespaceState, NamespaceStatus, Sync};
use opendal::{EntryMode, Operator};
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use yinyang_core::{Authority, Error, ErrorKind, NodeId, NodeKind, Result, Revision, Snapshot};

pub const SPACE_REPORT_SCHEMA: &str = "yinyang.managed-space.report.v1";
pub const RETENTION_PLAN_SCHEMA: &str = "yinyang.managed-retention.plan.v1";

const HEAD_KEY: &str = ".yinyang/head";
const PACK_ROOT: &str = ".yinyang/v2";
const SCAN_LIMIT: usize = 4096;

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
    pub error: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct PhysicalObject {
    pub objects: u64,
    pub bytes: u64,
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
            error: None,
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
    pub receipts: ReceiptEvidence,
    pub backup_evidence: &'static str,
    pub external_readers: &'static str,
    pub untracked_pins: &'static str,
    pub quiescence: &'static str,
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
            referenced_extents: "unavailable",
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

/// Build a bounded declarative retention selection over authoritative retained
/// revisions. The result never authorizes deletion and never estimates bytes
/// that physical cleanup could reclaim.
pub async fn plan_managed_retention(
    authority: &dyn Authority,
    staging: &Path,
    keep_last: Option<u64>,
    requested_pins: &[Revision],
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
    let mut unresolved = staging
        .errors
        .iter()
        .map(|detail| Unresolved {
            subject: "local-staging".to_owned(),
            state: "unavailable".to_owned(),
            detail: detail.clone(),
        })
        .collect::<Vec<_>>();
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
    let protected = &staging.protected;
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
    Ok(ManagedRetentionPlan {
        schema: RETENTION_PLAN_SCHEMA,
        filesystem: identity_hex(authority.filesystem()),
        observed_revision: revision_hex(latest.revision()),
        state,
        policy: RetentionPolicyReport {
            mode: if bounded { "bounded" } else { "keep_all" },
            keep_last,
            requested_pins: requested,
        },
        retained_revision_count,
        revisions: decisions,
        protection: RetentionProtection {
            current_revision: revision_hex(latest.revision()),
            staging: StagingProtection {
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
            },
            receipts,
            backup_evidence: "not_observed",
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

/// Inspect existing Managed content, Sync, and replica evidence without creating
/// a directory or database, acquiring a runtime lease, migrating state, or
/// recovering records. Known SQLite read-only behavior may create or update
/// derived WAL/SHM files.
pub async fn inspect_local_staging(staging: &Path) -> StagingSpace {
    let mut content = inspect_local_content_staging(staging).await;
    let sync = inspect_sync_evidence(staging).await;
    let replica = inspect_replica_evidence(staging).await;

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
        Err(error) => {
            return StagingSpace {
                path: display_path(staging),
                state: StagingState::Unavailable,
                binding: StagingBinding::unavailable(filesystem, binding_error.clone()),
                directory: None,
                databases: Vec::new(),
                handles: StagingHandles::default(),
                sync: SyncEvidence::missing(),
                replica: ReplicaEvidence::missing(),
                protected_revisions: Vec::new(),
                frozen_commits: Vec::new(),
                errors: vec![binding_error],
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
                inspection.evidence = ReplicaEvidence {
                    state: StagingState::Partial,
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
                    baseline_coverage: "unobserved_by_public_read_only_inspection",
                    error: None,
                };
                if pending > 0 {
                    inspection.evidence.error = Some(format!(
                        "replica has {pending} pending installs; retained artifacts and conflicts are observable, but replica baseline coverage is unobserved"
                    ));
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
                    let mut reasons = BTreeSet::new();
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
                    if !reasons.is_empty() {
                        protected
                            .entry(handle.remote_revision)
                            .or_default()
                            .extend(reasons);
                    }
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
    PhysicalSpace {
        state: if head.is_some() && packs.is_some() && after_revision.is_some() {
            if consistency == ListingConsistency::StableHead {
                MeasurementState::Available
            } else {
                MeasurementState::Partial
            }
        } else if head.is_some() || packs.is_some() {
            MeasurementState::Partial
        } else {
            MeasurementState::Unavailable
        },
        head,
        packs,
        visible_objects,
        visible_bytes,
        before_revision: revision_hex(latest.revision()),
        after_revision,
        listing_consistency: consistency,
        noncurrent_versions: "unknown",
        accounting: "unique_current_object_key",
        error: (!errors.is_empty()).then(|| errors.join("; ")),
    }
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
        "Referenced physical extents are not enumerated, so the report does not attribute object bytes to individual files or revisions.",
        "Recursive listing returns current object keys only and is not atomic; noncurrent backend versions are unknown.",
        "Each visible pack object key is counted once even when multiple files or revisions share its extents.",
        "External readers, untracked revision pins, other staging directories, backup packages, and quiescence are not discoverable by this inspection.",
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
        "External readers, untracked revision pins, other staging directories, and backup packages must be supplied as explicit requested pins.",
        "Noncurrent backend versions and shared pack extents remain unknown; no reclaimable-byte estimate is produced.",
        "Receipt records are namespace evidence, not a physical cleanup authorization.",
        "Quiescence is not observed; planning does not stop writers, readers, mounts, Sync processes, or backup activity.",
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
}
