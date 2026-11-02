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

use super::adapter::{SYNC_SCHEMA, SyncModel};
use serde::Serialize;

#[derive(Clone, Debug, Serialize)]
pub struct StatusDocument {
    pub schema: &'static str,
    pub model: SyncModel,
    pub binding: BindingStatus,
    pub remote: RemoteStatus,
    pub local: LocalStatus,
    pub summary: StatusSummary,
    pub entries: Vec<EntryStatus>,
    pub conflicts: Vec<ConflictStatus>,
    pub artifacts: Vec<ArtifactStatus>,
    /// Artifact bytes read by this status projection. Not serialized.
    #[doc(hidden)]
    #[serde(skip)]
    pub artifact_bytes_hashed: u64,
}

impl StatusDocument {
    pub fn empty(model: SyncModel, state_directory: String) -> Self {
        Self {
            schema: SYNC_SCHEMA,
            model,
            binding: BindingStatus {
                state: BindingState::Unbound,
                state_directory,
                root: None,
                namespace: None,
                same_filesystem: None,
                error: None,
            },
            remote: RemoteStatus {
                availability: RemoteAvailability::NotChecked,
                cursor: None,
                revision: None,
                error: None,
            },
            local: LocalStatus {
                scan: LocalScanState::NotScanned,
                error: None,
            },
            summary: StatusSummary::default(),
            entries: Vec::new(),
            conflicts: Vec::new(),
            artifacts: Vec::new(),
            artifact_bytes_hashed: 0,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct EngineInspection {
    pub model: SyncModel,
    pub namespace: String,
    pub root: String,
    pub cursor: Option<String>,
    pub baseline_revisions: Vec<String>,
    pub intent_receipt_revisions: Vec<String>,
    pub pending_installs: u64,
    pub artifacts: Vec<EngineArtifact>,
    pub conflicts: Vec<EngineConflict>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct EngineArtifact {
    pub id: String,
    pub kind: String,
    pub phase: String,
    pub release_event: String,
    pub length: u64,
    pub exportable: bool,
    pub error: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct EngineConflict {
    pub id: String,
    pub path: String,
    pub remote_revision: Option<String>,
    pub error: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct BackupInspection {
    pub model: SyncModel,
    pub namespace: String,
    pub root_entries: u64,
    pub artifact_entries: u64,
    pub databases: Vec<String>,
    pub support: Vec<String>,
    pub state: EngineInspection,
}

#[derive(Clone, Debug, Serialize)]
pub struct BindingStatus {
    pub state: BindingState,
    pub state_directory: String,
    pub root: Option<String>,
    pub namespace: Option<String>,
    pub same_filesystem: Option<bool>,
    pub error: Option<String>,
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BindingState {
    Unbound,
    Bound,
    Error,
}

#[derive(Clone, Debug, Serialize)]
pub struct RemoteStatus {
    pub availability: RemoteAvailability,
    pub cursor: Option<String>,
    pub revision: Option<String>,
    pub error: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RemoteAvailability {
    NotChecked,
    Available,
    Unavailable,
}

#[derive(Clone, Debug, Serialize)]
pub struct LocalStatus {
    pub scan: LocalScanState,
    pub error: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LocalScanState {
    NotScanned,
    Clean,
    Changed,
    Failed,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct StatusSummary {
    pub in_sync: bool,
    pub pending: u64,
    pub unknown: u64,
    pub conflicts: usize,
    pub failed: u64,
    pub retained_artifacts: u64,
    pub retained_bytes: u64,
}

#[derive(Clone, Debug, Serialize)]
pub struct EntryStatus {
    pub path: String,
    pub original_path: Option<String>,
    pub desired_path: Option<String>,
    pub state: EntryState,
    pub local: LocalEvidence,
    pub remote: RemoteEvidence,
    pub baseline: BaselineEvidence,
    pub intent: IntentStatus,
    pub outcome: OutcomeStatus,
    pub conflict: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EntryState {
    InSync,
    LocalPending,
    RemotePending,
    Conflict,
    Unknown,
    Failed,
}

#[derive(Clone, Debug, Serialize)]
pub struct LocalEvidence {
    pub kind: Option<EvidenceKind>,
    pub length: Option<u64>,
    pub digest: Option<String>,
    pub executable: Option<bool>,
    pub presence: Presence,
}

#[derive(Clone, Debug, Serialize)]
pub struct RemoteEvidence {
    pub kind: Option<EvidenceKind>,
    pub length: Option<u64>,
    pub digest: Option<String>,
    pub executable: Option<bool>,
    pub presence: Presence,
    pub identity: Option<String>,
    pub pin: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct BaselineEvidence {
    pub local_digest: Option<String>,
    pub local_length: Option<u64>,
    pub remote_identity: Option<String>,
    pub remote_pin: Option<String>,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct IntentStatus {
    pub id: Option<String>,
    pub operation: Option<String>,
    pub state: Option<IntentState>,
    pub request_id: Option<String>,
    pub commit_id: Option<String>,
    pub receipt_revision: Option<String>,
    pub error: Option<String>,
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum IntentState {
    Prepared,
    Unknown,
    Committed,
    Conflict,
    Retryable,
    Failed,
}

#[derive(Clone, Debug, Serialize)]
pub struct OutcomeStatus {
    pub state: OutcomeState,
    pub error: Option<String>,
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OutcomeState {
    Pending,
    RemoteCompleted,
    Conflict,
    Unknown,
    Retryable,
    Failed,
    None,
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceKind {
    File,
    Directory,
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Presence {
    Present,
    Absent,
    Unknown,
}

#[derive(Clone, Debug, Serialize)]
pub struct ConflictStatus {
    pub id: String,
    pub path: String,
    pub original_path: Option<String>,
    pub desired_path: Option<String>,
    pub local: ConflictSideStatus,
    pub remote: ConflictSideStatus,
    pub local_artifact: Option<String>,
    pub remote_artifact: Option<String>,
    pub remote_revision: Option<String>,
    pub error: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ConflictSideStatus {
    CurrentFile,
    CurrentDirectory,
    RetainedArtifact,
    Deleted,
    Unknown,
}

#[derive(Clone, Debug, Serialize)]
pub struct ArtifactStatus {
    pub id: String,
    pub origin: String,
    pub kind: String,
    pub phase: String,
    pub release_event: String,
    pub path: String,
    pub exportable: bool,
    pub length: u64,
    pub error: Option<String>,
}
