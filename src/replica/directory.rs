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

//! Finite directory reconciliation on top of the existing Sync intent journal.
//!
//! `Sync` owns durable, conditional publication of local intents; this module
//! owns the common baseline that records the last agreed state of both sides,
//! drives local installation through `Replica`, and reports truthful,
//! read-only domain evidence for the adapter to project.

mod resolution;

use super::backup::BackupManifest;
use crate::replica::state::{BaselineNode, BaselineRecord, ConflictRecord, WorkRecord};
use crate::replica::{Artifact, Conflict, Identity, LocalKind, LocalNode, LocalSnapshot, Replica};
use crate::sync::{EditId, FileMetadata, NamespaceState, RenameMode, Sync};
use std::collections::{BTreeMap, BTreeSet};
use std::io::{Seek as _, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use yinyang_core::{Authority, CommitId, Node, NodeId, NodeKind, Revision, Snapshot};

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + std::marker::Sync>>;

/// The observable result of one finite reconciliation.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RunOutcome {
    pub revision: Option<Revision>,
    pub published_local: u64,
    pub materialized_remote: u64,
    pub pending: u64,
    pub unknown: u64,
    pub conflicts: Vec<Conflict>,
    pub in_sync: bool,
}

/// A durable decision for one active conflict.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Resolution {
    KeepLocal,
    KeepRemote,
    Content(Vec<u8>),
}

// ---- Read-only domain evidence -------------------------------------------------

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Presence {
    Present,
    Absent,
    Unknown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EntryKind {
    File,
    Directory,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EntryState {
    InSync,
    LocalPending,
    RemotePending,
    Conflict,
    Unknown,
    Failed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IntentOperation {
    Content,
    CreateFile,
    CreateDirectory,
    Remove,
    Rename,
    SetExecutable,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IntentState {
    Prepared,
    Unknown,
    Committed,
    Conflict,
    Retryable,
    Failed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OutcomeState {
    Pending,
    RemoteCompleted,
    Conflict,
    Unknown,
    Retryable,
    Failed,
    None,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BindingState {
    Unbound,
    Bound,
    Error,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RemoteAvailability {
    NotChecked,
    Available,
    Unavailable,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LocalScanState {
    NotScanned,
    Clean,
    Changed,
    Failed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BindingEvidence {
    pub state: BindingState,
    pub state_directory: PathBuf,
    pub root: Option<PathBuf>,
    pub volume: Option<NodeId>,
    pub same_volume: Option<bool>,
    pub error: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RemoteStatus {
    pub availability: RemoteAvailability,
    pub revision: Option<Revision>,
    pub error: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LocalStatus {
    pub scan: LocalScanState,
    pub error: Option<String>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct StatusSummary {
    pub in_sync: bool,
    pub pending: u64,
    pub unknown: u64,
    pub conflicts: usize,
    pub failed: u64,
    pub retained_artifacts: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LocalEvidence {
    pub kind: Option<EntryKind>,
    pub length: Option<u64>,
    pub digest: Option<[u8; 32]>,
    pub executable: Option<bool>,
    pub presence: Presence,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RemoteEvidence {
    pub kind: Option<EntryKind>,
    pub length: Option<u64>,
    pub digest: Option<[u8; 32]>,
    pub executable: Option<bool>,
    pub presence: Presence,
    pub node: Option<NodeId>,
    pub revision: Option<Revision>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BaselineEvidence {
    pub node: Option<NodeId>,
    pub revision: Option<Revision>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct IntentEvidence {
    pub id: Option<[u8; 32]>,
    pub operation: Option<IntentOperation>,
    pub state: Option<IntentState>,
    pub commit_id: Option<CommitId>,
    pub receipt_revision: Option<Revision>,
    pub error: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OutcomeEvidence {
    pub state: OutcomeState,
    pub error: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EntryEvidence {
    pub path: String,
    pub original_path: Option<String>,
    pub desired_path: Option<String>,
    pub state: EntryState,
    pub local: LocalEvidence,
    pub remote: RemoteEvidence,
    pub baseline: BaselineEvidence,
    pub intent: IntentEvidence,
    pub outcome: OutcomeEvidence,
    pub conflict: Option<String>,
}

/// Truthful, read-only inspection of one Managed Sync binding. It never
/// acquires the writer lease, recovers, or writes; it remains usable while a
/// writer runs and while the remote is unreachable.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Inspection {
    pub binding: BindingEvidence,
    pub remote: RemoteStatus,
    pub local: LocalStatus,
    pub summary: StatusSummary,
    pub entries: Vec<EntryEvidence>,
    pub conflicts: Vec<Conflict>,
    pub artifacts: Vec<Artifact>,
}

pub struct DirectorySync {
    sync: Sync,
    replica: Replica,
}

impl DirectorySync {
    /// Write one quiesced local Sync backup package. The caller must not run a
    /// second writer; this method holds the existing replica and runtime leases
    /// while it captures the root and SQLite state.
    pub async fn backup(&self, output: &Path) -> Result<()> {
        Sync::validate_restore(self.replica.state(), self.sync.authority().filesystem())
            .await
            .map_err(Box::new)?;
        BackupManifest::create(&self.replica, output).map_err(Box::new)?;
        Ok(())
    }

    /// Complete an isolated restore after both the state directory and bound
    /// local root have been copied from one backup package.
    pub async fn restore(
        authority: Arc<dyn Authority>,
        backup: &Path,
        state: &Path,
        root: &Path,
    ) -> Result<Self> {
        let manifest = BackupManifest::load(backup).map_err(Box::new)?;
        if manifest.filesystem() != authority.filesystem() {
            return Err(format!(
                "backup belongs to filesystem {}, not {}",
                hex(manifest.filesystem().as_bytes()),
                hex(authority.filesystem().as_bytes())
            )
            .into());
        }
        Sync::validate_restore(backup.join("state"), authority.filesystem())
            .await
            .map_err(Box::new)?;
        manifest.copy_to(backup, root, state).map_err(Box::new)?;
        Sync::validate_restore(state, authority.filesystem()).await?;
        let replica =
            Replica::restore(state, root, authority.filesystem(), &manifest).map_err(Box::new)?;
        let sync = Sync::open(authority, state).await?;
        Ok(Self { sync, replica })
    }

    pub async fn open(authority: Arc<dyn Authority>, state: &Path) -> Result<Self> {
        let replica = Replica::open(state, authority.filesystem())?;
        let sync = Sync::open(authority, state).await?;
        Ok(Self { sync, replica })
    }

    pub async fn init(authority: Arc<dyn Authority>, state: &Path, root: &Path) -> Result<Self> {
        let replica = Replica::init(state, root, authority.filesystem())?;
        let sync = Sync::open(authority, state).await?;
        Ok(Self { sync, replica })
    }

    /// Truthful, read-only inspection for the adapter to project. It never
    /// acquires the writer lease, recovers, or writes, and it works while the
    /// remote is unreachable.
    pub async fn inspect(authority: Arc<dyn Authority>, state: &Path) -> Result<Inspection> {
        let (remote_status, remote_map) = match authority.observe_latest().await {
            Ok(observed) => match crate::directory_nodes(&observed).await {
                Ok(map) => (
                    RemoteStatus {
                        availability: RemoteAvailability::Available,
                        revision: Some(observed.revision()),
                        error: None,
                    },
                    Some(map),
                ),
                Err(error) => (
                    RemoteStatus {
                        availability: RemoteAvailability::Unavailable,
                        revision: None,
                        error: Some(error.to_string()),
                    },
                    None,
                ),
            },
            Err(error) => (
                RemoteStatus {
                    availability: RemoteAvailability::Unavailable,
                    revision: None,
                    error: Some(error.to_string()),
                },
                None,
            ),
        };
        Self::inspect_with(
            state,
            Some(authority.filesystem()),
            remote_status,
            remote_map.as_ref(),
        )
        .await
    }

    /// Local-only inspection for when an `Authority` cannot be constructed,
    /// such as an unreachable service. Remote evidence is reported as
    /// unavailable with the real connection error; nothing is fabricated.
    pub async fn inspect_local(state: &Path, remote_error: Option<String>) -> Result<Inspection> {
        Self::inspect_with(
            state,
            None,
            RemoteStatus {
                availability: RemoteAvailability::Unavailable,
                revision: None,
                error: remote_error,
            },
            None,
        )
        .await
    }

    async fn inspect_with(
        state: &Path,
        volume_hint: Option<NodeId>,
        remote_status: RemoteStatus,
        remote_map: Option<&BTreeMap<String, Node>>,
    ) -> Result<Inspection> {
        let state_directory = std::fs::canonicalize(state).unwrap_or_else(|_| state.to_owned());
        let mut unbound = Inspection {
            binding: BindingEvidence {
                state: BindingState::Unbound,
                state_directory: state_directory.clone(),
                root: None,
                volume: None,
                same_volume: None,
                error: None,
            },
            remote: RemoteStatus {
                availability: RemoteAvailability::NotChecked,
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
        };
        unbound.remote = RemoteStatus {
            availability: remote_status.availability,
            revision: None,
            error: remote_status.error.clone(),
        };
        if !state.join("replica.db").exists() {
            return Ok(unbound);
        }
        let status = match Replica::inspect(state) {
            Ok(status) => status,
            Err(error) => {
                return Ok(Inspection {
                    binding: BindingEvidence {
                        state: BindingState::Error,
                        state_directory,
                        root: None,
                        volume: volume_hint,
                        same_volume: None,
                        error: Some(error.to_string()),
                    },
                    ..unbound
                });
            }
        };
        let binding = BindingEvidence {
            state: BindingState::Bound,
            state_directory,
            root: Some(status.root.clone()),
            volume: Some(status.filesystem),
            same_volume: Some(true),
            error: None,
        };

        let (snapshot, scan_error) = match Replica::scan_read_only(state) {
            Ok(snapshot) => (Some(snapshot), None),
            Err(error) => (None, Some(error.to_string())),
        };

        let store = crate::replica::state::Store::open_read_only(&state.join("replica.db"))?;
        let baseline = store.load_baseline()?;
        let work = store.work()?;
        let sync_inspect = Sync::inspect(state).await.ok();

        // Local scan state is classified against the stored common baseline so
        // it stays truthful even while the remote is unavailable.
        let local_status = match (&snapshot, &scan_error) {
            (Some(snapshot), _) => {
                let changed = match &baseline {
                    Some(baseline) => {
                        let mut probe: BTreeSet<String> = BTreeSet::new();
                        probe.extend(baseline.local.nodes().keys().cloned());
                        probe.extend(snapshot.nodes().keys().cloned());
                        probe.iter().any(|path| {
                            !path.is_empty()
                                && local_action(baseline.local.get(path), snapshot.get(path))
                                    != Action::None
                        })
                    }
                    None => snapshot.len() > 1,
                };
                LocalStatus {
                    scan: if changed {
                        LocalScanState::Changed
                    } else {
                        LocalScanState::Clean
                    },
                    error: None,
                }
            }
            (None, error) => LocalStatus {
                scan: LocalScanState::Failed,
                error: error.clone(),
            },
        };

        let mut paths: BTreeSet<String> = BTreeSet::new();
        if let Some(baseline) = &baseline {
            paths.extend(baseline.remote.keys().cloned());
            paths.extend(baseline.local.nodes().keys().cloned());
        }
        if let Some(snapshot) = &snapshot {
            paths.extend(snapshot.nodes().keys().cloned());
        }
        if let Some(remote) = &remote_map {
            paths.extend(remote.keys().cloned());
        }
        paths.remove("");

        let mut conflicts: Vec<Conflict> = status.conflicts.clone();
        // A retained install inode that gained late writer bytes is an
        // unresolved conflict even though nothing durable was written before.
        for artifact in &status.artifacts {
            let install_kind =
                artifact.kind == "replaced-file" || artifact.kind == "rollback-displaced";
            if artifact.phase != "conflict"
                || !install_kind
                || artifact.origin.is_empty()
                || conflicts.iter().any(|conflict| {
                    conflict.local_artifact.as_deref() == Some(artifact.id.as_str())
                })
            {
                continue;
            }
            let path = artifact.origin.clone();
            let remote = match remote_map.and_then(|map| map.get(&path)) {
                Some(node) if node.is_directory() => "current_directory",
                Some(_) => "current_file",
                None => "deleted",
            };
            conflicts.push(Conflict {
                id: artifact_conflict_identity(&artifact.id),
                path: path.clone(),
                original_path: Some(path.clone()),
                desired_path: Some(path),
                local: "retained_artifact".to_owned(),
                remote: remote.to_owned(),
                local_artifact: Some(artifact.id.clone()),
                remote_node: remote_map
                    .and_then(|map| map.get(&artifact.origin))
                    .map(Node::id),
                remote_revision: remote_status.revision.map(Revision::to_bytes),
                error: artifact
                    .error
                    .clone()
                    .unwrap_or_else(|| "retained inode changed after installation".to_owned()),
            });
        }
        let mut entries = Vec::with_capacity(paths.len());
        let mut pending = 0_u64;
        for path in &paths {
            let local_node = snapshot.as_ref().and_then(|snapshot| snapshot.get(path));
            let remote_node = remote_map.as_ref().and_then(|remote| remote.get(path));
            let baseline_node = baseline
                .as_ref()
                .and_then(|baseline| baseline.remote.get(path));
            let baseline_local = baseline
                .as_ref()
                .and_then(|baseline| baseline.local.get(path));
            let conflict = conflicts.iter().find(|conflict| {
                conflict
                    .desired_path
                    .as_deref()
                    .unwrap_or(conflict.path.as_str())
                    == path
            });
            let work_record = select_work(&work, path, sync_inspect.as_ref());
            let intent = intent_evidence(work_record, sync_inspect.as_ref());
            let local = local_evidence(local_node);
            let remote = if remote_map.is_some() {
                remote_evidence(remote_node, remote_status.revision)
            } else {
                remote_evidence(None, None)
            };
            let baseline_ev = BaselineEvidence {
                node: baseline_node.map(|node| node.node),
                revision: baseline_node.map(|node| node.revision),
            };
            let local_action = local_action(baseline_local, local_node);
            let remote_action = if remote_map.is_some() {
                remote_action(baseline_node, remote_node)
            } else {
                Action::None
            };
            let (state, outcome) = if conflict.is_some() {
                (
                    EntryState::Conflict,
                    OutcomeEvidence {
                        state: OutcomeState::Conflict,
                        error: None,
                    },
                )
            } else if intent.state == Some(IntentState::Unknown) {
                (
                    EntryState::Unknown,
                    OutcomeEvidence {
                        state: OutcomeState::Unknown,
                        error: intent.error.clone(),
                    },
                )
            } else if intent.state == Some(IntentState::Failed) {
                (
                    EntryState::Failed,
                    OutcomeEvidence {
                        state: OutcomeState::Failed,
                        error: intent.error.clone(),
                    },
                )
            } else if local_action != Action::None {
                pending += 1;
                // A local change is pending, but a previous request may already
                // have completed: report that receipt separately from the new
                // local edit so a completed v1 is not shown as merely Pending.
                let previous_completed = baseline_node.is_some() && remote_action == Action::None;
                (
                    EntryState::LocalPending,
                    OutcomeEvidence {
                        state: if previous_completed {
                            OutcomeState::RemoteCompleted
                        } else {
                            OutcomeState::Pending
                        },
                        error: None,
                    },
                )
            } else if remote_action != Action::None {
                pending += 1;
                (
                    EntryState::RemotePending,
                    OutcomeEvidence {
                        state: OutcomeState::Pending,
                        error: None,
                    },
                )
            } else if baseline_node.is_some() {
                (
                    EntryState::InSync,
                    OutcomeEvidence {
                        state: OutcomeState::RemoteCompleted,
                        error: None,
                    },
                )
            } else {
                (
                    EntryState::InSync,
                    OutcomeEvidence {
                        state: OutcomeState::None,
                        error: None,
                    },
                )
            };
            entries.push(EntryEvidence {
                path: path.clone(),
                original_path: conflict.and_then(|c| c.original_path.clone()),
                desired_path: conflict.and_then(|c| c.desired_path.clone()),
                state,
                local,
                remote,
                baseline: baseline_ev,
                intent,
                outcome,
                conflict: conflict.map(|c| c.id.clone()),
            });
        }
        pending += status.pending_installs as u64;
        let (unknown, failed) = associated_counts(&work, sync_inspect.as_ref());
        let in_sync = pending == 0
            && unknown == 0
            && failed == 0
            && conflicts.is_empty()
            && baseline.is_some()
            && remote_status.availability == RemoteAvailability::Available
            && local_status.scan != LocalScanState::Failed;
        let summary = StatusSummary {
            in_sync,
            pending,
            unknown,
            conflicts: conflicts.len(),
            failed,
            retained_artifacts: status.artifacts.len() as u64,
        };
        Ok(Inspection {
            binding,
            remote: remote_status,
            local: local_status,
            summary,
            entries,
            conflicts,
            artifacts: status.artifacts,
        })
    }

    pub fn root(&self) -> &Path {
        self.replica.root()
    }

    pub fn state(&self) -> &Path {
        self.replica.state()
    }

    /// Retained-artifact export and conflict-side export share one path-based
    /// reader so late writers into a retained inode stay observable.
    pub fn export_artifact(&self, id: &str, output: &mut dyn Write) -> Result<()> {
        Ok(self.replica.export_artifact(id, output)?)
    }

    pub fn artifact_path(&self, id: &str) -> Result<PathBuf> {
        Ok(self.replica.artifact_path(id)?)
    }

    /// Export one side of a conflict. `side` is `"local"` or `"remote"`.
    pub async fn export_conflict_side(
        &self,
        conflict_id: &str,
        side: &str,
        output: &mut dyn Write,
    ) -> Result<()> {
        let record = self.load_conflict_record(conflict_id)?;
        match side {
            "local" => self.export_conflict_local(conflict_id, output),
            "remote" => {
                let node = record
                    .remote_node
                    .ok_or_else(|| "this conflict has no remote side to export".to_string())?;
                let snapshot = match record.remote_revision {
                    Some(revision) => {
                        self.sync
                            .authority()
                            .observe_revision(Revision::from_bytes(revision))
                            .await?
                    }
                    None => self.sync.authority().observe_latest().await?,
                };
                let version = snapshot.open_file(NodeId::from_bytes(node)).await?;
                let length = version.descriptor().content_id().length();
                let mut position = 0;
                while position < length {
                    let end = (position + 64 * 1024).min(length);
                    let mut chunk = Vec::new();
                    version.read_range(position..end, &mut chunk).await?;
                    output
                        .write_all(&chunk)
                        .map_err(|error| error.to_string())?;
                    position = end;
                }
                Ok(())
            }
            other => Err(format!("unknown conflict side {other:?}").into()),
        }
    }

    /// Export only the local side of a conflict. The bytes are local (a
    /// retained artifact or the live local file), so no Authority is needed;
    /// it keeps working offline.
    pub fn export_conflict_local(&self, conflict_id: &str, output: &mut dyn Write) -> Result<()> {
        let record = self.load_conflict_record(conflict_id)?;
        export_local_side(&self.replica, &record, output)
    }

    fn load_conflict_record(&self, conflict_id: &str) -> Result<ConflictRecord> {
        match self
            .replica
            .store()
            .conflicts()?
            .into_iter()
            .find(|record| record.id == conflict_id)
        {
            Some(record) => Ok(record),
            None => self.synthetic_conflict(conflict_id),
        }
    }

    pub async fn run(&mut self) -> Result<RunOutcome> {
        let mut outcome = RunOutcome::default();
        self.retry_associated(&mut outcome).await?;

        let observed = self.sync.authority().observe_latest().await?;
        let mut remote = crate::directory_nodes(&observed).await?;
        let current = self.replica.scan()?;

        if self.replica.store().load_baseline()?.is_none() {
            self.admit(&mut outcome, &current, &mut remote, &observed)
                .await?;
        }

        // Paths owned by unresolved or terminal work are excluded from this
        // pass so the exact original request is never silently superseded by a
        // freshly staged edit.
        let blocked: BTreeSet<String> = self
            .replica
            .store()
            .work()?
            .into_iter()
            .filter_map(|record| match record.state.as_str() {
                "prepared" | "unknown" | "retryable" | "conflict" | "failed" => Some(record.path),
                _ => None,
            })
            .collect();

        if let Some(baseline) = self.replica.store().load_baseline()? {
            let mut ctx = Context {
                sync: &mut self.sync,
                replica: &mut self.replica,
                outcome: &mut outcome,
                baseline,
                current: &current,
                remote: &mut remote,
                revision: observed.revision(),
                nodes: BTreeMap::new(),
                revisions: BTreeMap::new(),
                dirty: BTreeSet::new(),
                blocked,
            };
            ctx.reconcile().await?;
        }

        self.finalize(&mut outcome, observed.revision()).await?;
        Ok(outcome)
    }

    /// Apply one durable decision to an active conflict, then reconcile again.
    /// The conflict stays active until its derived request actually commits.
    pub async fn resolve(&mut self, conflict_id: &str, decision: Resolution) -> Result<RunOutcome> {
        let record = self.load_conflict_record(conflict_id)?;
        let observed = self.sync.authority().observe_latest().await?;
        let remote = crate::directory_nodes(&observed).await?;
        let baseline = self.replica.store().load_baseline()?;
        let _applied = match decision {
            Resolution::KeepLocal | Resolution::Content(_) => {
                self.resolve_keep_local(&record, &remote, baseline.as_ref(), decision)
                    .await?
            }
            Resolution::KeepRemote => {
                self.resolve_keep_remote(&record, &observed, &remote)
                    .await?
            }
        };
        self.run().await
    }

    /// Stop reporting a resolved conflict: drop the durable record, or
    /// acknowledge the retained inode if the conflict was synthesized from one.
    fn clear_resolved_conflict(&mut self, conflict_id: &str) -> Result<()> {
        let artifact = self
            .replica
            .store()
            .artifacts()?
            .into_iter()
            .find(|artifact| artifact_conflict_identity(&artifact.id) == conflict_id);
        match artifact {
            Some(artifact) => self.replica.acknowledge_retained_artifact(&artifact.id)?,
            None => {
                self.replica
                    .store_mut()
                    .supersede_conflict_work(conflict_id)?;
                self.replica.store_mut().remove_conflict(conflict_id)?;
            }
        }
        Ok(())
    }

    /// Build the conflict record for a retained install inode that gained late
    /// writer bytes. This conflict is reported by inspection but is not durable
    /// until an explicit decision is applied.
    fn synthetic_conflict(&self, conflict_id: &str) -> Result<ConflictRecord> {
        let artifact = self
            .replica
            .store()
            .artifacts()?
            .into_iter()
            .find(|artifact| artifact_conflict_identity(&artifact.id) == conflict_id)
            .ok_or_else(|| crate::replica::Error::invalid("unknown conflict").to_string())?;
        if artifact.origin.is_empty() || !self.replica.retained_artifact_changed(&artifact.id)? {
            return Err(crate::replica::Error::invalid("unknown conflict")
                .to_string()
                .into());
        }
        Ok(ConflictRecord {
            id: conflict_id.to_owned(),
            path: artifact.origin.clone(),
            original_path: Some(artifact.origin.clone()),
            desired_path: Some(artifact.origin.clone()),
            local: "retained_artifact".to_owned(),
            remote: "current_file".to_owned(),
            local_artifact: Some(artifact.id),
            remote_node: None,
            remote_revision: None,
            error: artifact
                .error
                .unwrap_or_else(|| "retained inode changed after installation".to_owned()),
        })
    }

    async fn admit(
        &mut self,
        outcome: &mut RunOutcome,
        current: &LocalSnapshot,
        remote: &mut BTreeMap<String, Node>,
        observed: &Snapshot,
    ) -> Result<()> {
        let local_empty = current.len() == 1;
        let remote_empty = remote.len() == 1;
        if !local_empty && !remote_empty {
            let id = conflict_identity(
                "",
                None,
                Some(""),
                "current_directory",
                "current_directory",
                None,
                None,
            );
            record_conflict_side(
                outcome,
                &mut self.replica,
                &id,
                "",
                None,
                Some(""),
                "current_directory",
                "current_directory",
                None,
                None,
                None,
                "initial sync requires one empty side; two non-empty sides are an explicit merge",
            )?;
            return Ok(());
        }
        let root_node = observed
            .resolve("")
            .await?
            .ok_or_else(|| "remote root is unresolved".to_string())?;
        let root_local = current
            .get("")
            .cloned()
            .ok_or_else(|| "local root is unresolved".to_string())?;
        self.replica.store_mut().save_baseline_path(
            "",
            &root_node,
            Some(&root_local),
            observed.revision(),
        )?;
        self.replica
            .store_mut()
            .set_baseline_revision(observed.revision())?;
        let _ = remote;
        Ok(())
    }

    async fn finalize(&mut self, outcome: &mut RunOutcome, revision: Revision) -> Result<()> {
        outcome.revision = Some(revision);
        let status = self.replica.status()?;
        outcome.conflicts.extend(status.conflicts.clone());
        outcome
            .conflicts
            .sort_by_key(|a| (a.path.clone(), a.id.clone()));
        outcome.conflicts.dedup_by(|a, b| a.id == b.id);

        // Recompute honest pending/unknown from the persisted baseline.
        let baseline = self.replica.store().load_baseline()?;
        let current = self.replica.scan()?;
        let observed = self.sync.authority().observe_latest().await?;
        let remote = crate::directory_nodes(&observed).await?;
        let mut pending = 0_u64;
        if let Some(baseline) = &baseline {
            let mut paths: BTreeSet<String> = BTreeSet::new();
            paths.extend(baseline.remote.keys().cloned());
            paths.extend(baseline.local.nodes().keys().cloned());
            paths.extend(current.nodes().keys().cloned());
            paths.extend(remote.keys().cloned());
            for path in &paths {
                if path.is_empty() {
                    continue;
                }
                if local_action(baseline.local.get(path), current.get(path)) != Action::None {
                    pending += 1;
                }
                if remote_action(baseline.remote.get(path), remote.get(path)) != Action::None {
                    pending += 1;
                }
            }
        }
        let work = self.replica.store().work()?;
        let sync_inspect = Sync::inspect(self.replica.state()).await.ok();
        let (unknown, failed) = associated_counts(&work, sync_inspect.as_ref());
        outcome.pending = outcome.pending.max(pending);
        outcome.unknown = outcome.unknown.max(unknown);
        outcome.in_sync = outcome.pending == 0
            && outcome.unknown == 0
            && failed == 0
            && outcome.conflicts.is_empty();
        Ok(())
    }

    async fn retry_associated(&mut self, outcome: &mut RunOutcome) -> Result<()> {
        let work = self.replica.store().work()?;
        for record in work {
            if matches!(
                record.state.as_str(),
                "committed" | "superseded" | "conflict" | "failed"
            ) {
                continue;
            }
            let edit = EditId::from_bytes(record.id);
            // Capture the original commit identity before a replay clears it.
            if let Ok(crate::sync::EditStatus::Content(status)) = self.sync.edit_status(edit).await
                && let Some(commit) = status.frozen_commit_id
            {
                self.replica
                    .store_mut()
                    .set_work_commit_id(&record.id, *commit.as_bytes())?;
            }
            match self.sync.publish(edit).await {
                Ok(revision) => {
                    outcome.published_local += 1;
                    outcome.revision = Some(revision);
                    // Apply baseline and clear the linked conflict before
                    // marking the work complete. Publication is idempotent, so
                    // a crash here is recovered by re-running this retry.
                    let applied = self.apply_retry_receipt(&record, revision).await?;
                    if applied {
                        if let Some(conflict) = &record.conflict {
                            self.clear_resolved_conflict(conflict)?;
                        }
                        self.replica.store_mut().update_work(
                            &record.id,
                            "committed",
                            Some(revision),
                            None,
                        )?;
                    } else {
                        // The remote receipt committed but the live local
                        // version moved on. Keep the association unresolved so
                        // the next run retries the idempotent decision.
                        self.replica.store_mut().update_work(
                            &record.id,
                            "prepared",
                            Some(revision),
                            None,
                        )?;
                    }
                }
                Err(error) if error.kind() == crate::runtime::ErrorKind::Unknown => {
                    outcome.unknown += 1;
                    if let Some(commit) = error.commit_id() {
                        self.replica
                            .store_mut()
                            .set_work_commit_id(&record.id, *commit.as_bytes())?;
                    }
                    self.replica.store_mut().update_work(
                        &record.id,
                        "unknown",
                        None,
                        Some(&error.to_string()),
                    )?;
                }
                Err(error) => {
                    let state = work_state_for(&error);
                    if state == "unknown" {
                        outcome.unknown += 1;
                    }
                    self.replica.store_mut().update_work(
                        &record.id,
                        state,
                        None,
                        Some(&error.to_string()),
                    )?;
                }
            }
        }
        Ok(())
    }

    async fn accepted_node_at(
        &mut self,
        record: &WorkRecord,
        revision: Revision,
    ) -> Result<Option<Node>> {
        let observed = self.sync.authority().observe_revision(revision).await?;
        Ok(observed.resolve(&record.path).await?)
    }

    /// Retry-path counterpart of the reconciliation parent refresh: advance the
    /// parent's accepted remote node/revision while preserving the previously
    /// accepted local parent fingerprint.
    async fn refresh_parent_baseline(&mut self, path: &str, revision: Revision) -> Result<()> {
        let (parent, _) = split(path);
        let snapshot = self.sync.authority().observe_revision(revision).await?;
        let Some(node) = snapshot.resolve(parent).await? else {
            return Ok(());
        };
        let accepted = self
            .replica
            .store()
            .load_baseline()?
            .and_then(|baseline| baseline.local.get(parent).cloned());
        self.replica
            .store_mut()
            .save_baseline_path(parent, &node, accepted.as_ref(), revision)?;
        Ok(())
    }

    /// Apply an already-committed retry receipt to the common baseline by
    /// operation kind: a removal drops the subtree, a rename relocates the
    /// subtree preserving accepted fingerprints, and other edits save the
    /// accepted version at the path.
    async fn apply_retry_receipt(
        &mut self,
        record: &WorkRecord,
        revision: Revision,
    ) -> Result<bool> {
        if record.source_artifact.is_some() {
            // Explicit resolution receipt (content or create-file): the
            // selected bytes live in the artifact, not in the work's local
            // fingerprint. Install conditionally against the pre-dispatch
            // version and acknowledge the selected fingerprint as the common
            // baseline; a newer live version stays pending for the next run.
            let Some(artifact) = &record.source_artifact else {
                return Ok(true);
            };
            let mut file = self.replica.open_artifact(artifact)?;
            let mut bytes = Vec::new();
            std::io::copy(&mut file, &mut bytes).map_err(|error| error.to_string())?;
            let expected = accepted_local(record)?;
            let chosen = self.install_chosen_bytes(
                &record.path,
                &bytes,
                record.local_executable.unwrap_or(false),
                expected.as_ref(),
            )?;
            if let Some(node) = self.accepted_node_at(record, revision).await? {
                self.replica.store_mut().save_baseline_path(
                    &record.path,
                    &node,
                    chosen.as_ref(),
                    revision,
                )?;
            }
            if record.operation == "create-file" {
                self.refresh_parent_baseline(&record.path, revision).await?;
            }
            return Ok(true);
        }
        match record.operation.as_str() {
            "remove" => {
                self.replica
                    .store_mut()
                    .remove_baseline_subtree(&record.path)?;
                self.refresh_parent_baseline(&record.path, revision).await?;
            }
            "rename" => {
                if let Some(from) = record.original_path.clone() {
                    self.migrate_baseline_from_work(&from, &record.path, revision)?;
                    self.refresh_parent_baseline(&from, revision).await?;
                    self.refresh_parent_baseline(&record.path, revision).await?;
                }
            }
            _ => {
                if let Some(node) = self.accepted_node_at(record, revision).await? {
                    let accepted = accepted_local(record)?;
                    self.replica.store_mut().save_baseline_path(
                        &record.path,
                        &node,
                        accepted.as_ref(),
                        revision,
                    )?;
                }
                if matches!(
                    record.operation.as_str(),
                    "create-file" | "create-directory"
                ) {
                    self.refresh_parent_baseline(&record.path, revision).await?;
                }
            }
        }
        Ok(true)
    }

    /// Relocate a whole subtree recorded by a committed rename receipt without
    /// re-reading content: the receipt proves the namespace move only.
    fn migrate_baseline_from_work(
        &mut self,
        from: &str,
        to: &str,
        revision: Revision,
    ) -> Result<()> {
        let Some(baseline) = self.replica.store().load_baseline()? else {
            return Ok(());
        };
        let prefix = format!("{from}/");
        let moves: Vec<(
            String,
            String,
            crate::replica::state::BaselineNode,
            Option<LocalNode>,
        )> = baseline
            .remote
            .iter()
            .filter(|(path, _)| *path == from || path.starts_with(&prefix))
            .map(|(path, entry)| {
                let destination = if path == from {
                    to.to_owned()
                } else {
                    format!("{to}{}", &path[from.len()..])
                };
                (
                    path.clone(),
                    destination,
                    entry.clone(),
                    baseline.local.get(path).cloned(),
                )
            })
            .collect();
        for (source, destination, entry, previous) in moves {
            let accepted = accepted_local_before(&self.replica, &destination, previous.as_ref())?;
            let length = accepted
                .as_ref()
                .and_then(|node| match node.kind {
                    LocalKind::File { length, .. } => Some(length),
                    LocalKind::Directory => None,
                })
                .unwrap_or(0);
            // The moved root advances its namespace revision; an unchanged
            // descendant keeps its own accepted revision and content.
            let accepted_revision = if source == from {
                revision
            } else {
                entry.revision
            };
            self.replica.store_mut().save_baseline_entry(
                &destination,
                entry.node,
                entry.directory,
                entry.executable,
                entry.digest,
                length,
                accepted.as_ref(),
                accepted_revision,
            )?;
            self.replica.store_mut().remove_baseline_path(&source)?;
        }
        Ok(())
    }
}

/// Export only the local side of a conflict without constructing an Authority.
/// `conflict_id` is the identity reported by inspection, including a conflict
/// synthesized from a retained install inode that gained late writer bytes.
pub fn export_conflict_local(
    state: &Path,
    conflict_id: &str,
    output: &mut dyn Write,
) -> Result<()> {
    let replica = Replica::open_read_only(state)?;
    let record = match replica
        .store()
        .conflicts()?
        .into_iter()
        .find(|record| record.id == conflict_id)
    {
        Some(record) => record,
        None => {
            let artifact = replica
                .store()
                .artifacts()?
                .into_iter()
                .find(|artifact| {
                    artifact_conflict_identity(&artifact.id) == conflict_id
                        && replica
                            .retained_artifact_changed(&artifact.id)
                            .unwrap_or(false)
                })
                .ok_or_else(|| crate::replica::Error::invalid("unknown conflict").to_string())?;
            ConflictRecord {
                id: conflict_id.to_owned(),
                path: artifact.origin.clone(),
                original_path: Some(artifact.origin.clone()),
                desired_path: Some(artifact.origin.clone()),
                local: "retained_artifact".to_owned(),
                remote: "current_file".to_owned(),
                local_artifact: Some(artifact.id),
                remote_node: None,
                remote_revision: None,
                error: artifact
                    .error
                    .unwrap_or_else(|| "retained inode changed after installation".to_owned()),
            }
        }
    };
    export_local_side(&replica, &record, output)
}

fn export_local_side(
    replica: &Replica,
    record: &ConflictRecord,
    output: &mut dyn Write,
) -> Result<()> {
    if let Some(artifact) = &record.local_artifact {
        return Ok(replica.export_artifact(artifact, output)?);
    }
    let path = record
        .desired_path
        .as_deref()
        .unwrap_or(record.path.as_str());
    let mut file = replica.open_local_file(path)?;
    std::io::copy(&mut file, output).map_err(|error| error.to_string())?;
    Ok(())
}

fn accepted_local_before(
    replica: &Replica,
    to: &str,
    previous: Option<&LocalNode>,
) -> Result<Option<LocalNode>> {
    let Some(previous) = previous else {
        return Ok(None);
    };
    let fresh = replica.scan()?;
    let Some(current) = fresh.get(to) else {
        return Ok(None);
    };
    Ok(Some(LocalNode {
        path: to.to_owned(),
        kind: previous.kind,
        executable: previous.executable,
        identity: current.identity,
        parent: current.parent,
    }))
}

fn accepted_local(record: &WorkRecord) -> Result<Option<LocalNode>> {
    let Some(kind) = record.local_kind.as_deref() else {
        return Ok(None);
    };
    let kind = match kind {
        "directory" => LocalKind::Directory,
        "file" => LocalKind::File {
            length: record.local_length.unwrap_or(0),
            digest: crate::replica::ContentHash::from_bytes(
                record
                    .local_digest
                    .ok_or_else(|| "work association lacks a local digest".to_string())?,
            ),
        },
        other => return Err(format!("unknown work local kind {other:?}").into()),
    };
    let (Some(dev), Some(ino)) = (record.local_dev, record.local_ino) else {
        return Ok(None);
    };
    let identity = Identity { dev, ino };
    let parent = match (record.local_parent_dev, record.local_parent_ino) {
        (Some(dev), Some(ino)) => Some(Identity { dev, ino }),
        _ => None,
    };
    Ok(Some(LocalNode {
        path: record.path.clone(),
        kind,
        executable: record.local_executable.unwrap_or(false),
        identity,
        parent,
    }))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Action {
    None,
    Create,
    Modify,
    Delete,
}

struct Context<'a> {
    sync: &'a mut Sync,
    replica: &'a mut Replica,
    outcome: &'a mut RunOutcome,
    baseline: BaselineRecord,
    current: &'a LocalSnapshot,
    remote: &'a mut BTreeMap<String, Node>,
    revision: Revision,
    nodes: BTreeMap<String, NodeId>,
    revisions: BTreeMap<String, Revision>,
    /// Paths whose remote namespace we changed earlier in this run; their
    /// staging predicate must reflect those receipts.
    dirty: BTreeSet<String>,
    /// Paths owned by unresolved or terminal work; never re-planned here.
    blocked: BTreeSet<String>,
}

impl<'a> Context<'a> {
    fn seed(&mut self) {
        for (path, node) in self.remote.iter() {
            self.nodes.insert(path.clone(), node.id());
        }
        for path in self.remote.keys() {
            let revision = self
                .baseline
                .remote
                .get(path)
                .map(|entry| entry.revision)
                .unwrap_or(self.revision);
            self.revisions.insert(path.clone(), revision);
        }
    }

    async fn reconcile(&mut self) -> Result<()> {
        self.seed();
        let (remote_moves, local_moves) = self.detect_moves();
        let mut handled: BTreeSet<String> = BTreeSet::new();
        self.apply_moves(&remote_moves, &local_moves, &mut handled)
            .await?;
        let retained = self.retained_remote_deleted_dirs();
        self.handle_retained(&retained, &mut handled).await?;

        let mut paths: BTreeSet<String> = BTreeSet::new();
        paths.extend(self.baseline.remote.keys().cloned());
        paths.extend(self.current.nodes().keys().cloned());
        paths.extend(self.remote.keys().cloned());

        // Quarantine a remote-deleted directory as a single unit before
        // per-entry handling, so its descendants are never removed one by one.
        let mut dirs: Vec<String> = paths.iter().cloned().collect();
        dirs.sort_by_key(|path| (depth(path), path.clone()));
        for path in &dirs {
            if path.is_empty()
                || handled.contains(path)
                || self.blocked.contains(path)
                || under_any_inclusive(&retained, path)
            {
                continue;
            }
            let is_remote_deleted_dir = self
                .baseline
                .remote
                .get(path)
                .is_some_and(|entry| entry.directory)
                && !self.remote.contains_key(path);
            if !is_remote_deleted_dir {
                continue;
            }
            if local_action(self.baseline.local.get(path), self.current.get(path)) != Action::None {
                continue;
            }
            self.materialize_delete(path).await?;
            self.replica.store_mut().remove_baseline_subtree(path)?;
            self.mark_subtree(&mut handled, path);
            handled.insert(path.clone());
        }

        let mut ascending: Vec<String> = paths.iter().cloned().collect();
        ascending.sort_by_key(|path| (depth(path), path.clone()));
        for path in &ascending {
            if path.is_empty() || handled.contains(path) || self.blocked.contains(path) {
                continue;
            }
            if under_any(&retained, path) {
                continue;
            }
            self.entry(path).await?;
        }

        let mut descending = ascending.clone();
        descending.sort_by_key(|path| (depth(path), path.clone()));
        descending.reverse();
        for path in &descending {
            if handled.contains(path) || self.blocked.contains(path) || under_any(&retained, path) {
                continue;
            }
            let local = local_action(self.baseline.local.get(path), self.current.get(path));
            let remote = remote_action(self.baseline.remote.get(path), self.remote.get(path));
            if local == Action::Delete && remote == Action::None {
                self.publish_remove(path).await?;
            }
        }
        Ok(())
    }

    fn detect_moves(&self) -> (BTreeMap<String, String>, BTreeMap<String, String>) {
        let mut remote_moves = BTreeMap::new();
        let mut by_node = BTreeMap::new();
        for (path, node) in &self.baseline.remote {
            by_node.insert((node.node, node.directory), path.clone());
        }
        for (path, node) in self.remote.iter() {
            if let Some(old) = by_node
                .get(&(node.id(), node.is_directory()))
                .filter(|old| *old != path)
            {
                remote_moves.insert(old.clone(), path.clone());
            }
        }
        let mut local_moves = BTreeMap::new();
        let mut local_by_identity = BTreeMap::new();
        for (path, node) in self.baseline.local.nodes() {
            local_by_identity.insert((node.identity, node.is_directory()), path.clone());
        }
        for (path, node) in self.current.nodes() {
            if let Some(old) = local_by_identity
                .get(&(node.identity, node.is_directory()))
                .filter(|old| *old != path)
            {
                local_moves.insert(old.clone(), path.clone());
            }
        }
        (remote_moves, local_moves)
    }

    async fn apply_moves(
        &mut self,
        remote_moves: &BTreeMap<String, String>,
        local_moves: &BTreeMap<String, String>,
        handled: &mut BTreeSet<String>,
    ) -> Result<()> {
        let mut sources: BTreeSet<String> = BTreeSet::new();
        sources.extend(remote_moves.keys().cloned());
        sources.extend(local_moves.keys().cloned());
        // Only the top-most moved path is renamed; descendants move with it.
        let roots: Vec<String> = sources
            .iter()
            .filter(|source| {
                !sources.iter().any(|other| {
                    *other != **source
                        && source
                            .strip_prefix(other)
                            .is_some_and(|suffix| suffix.starts_with('/'))
                })
            })
            .cloned()
            .collect();
        for source in roots {
            if self.blocked.contains(&source) {
                continue;
            }
            let rm = remote_moves.get(&source);
            let lm = local_moves.get(&source);
            match (rm, lm) {
                (Some(destination), Some(local_destination))
                    if destination == local_destination =>
                {
                    self.nodes.remove(&source);
                    if let Some(node) = self.remote.get(destination) {
                        self.nodes.insert(destination.clone(), node.id());
                    }
                    self.migrate_subtree(handled, &source, destination, self.revision)
                        .await?;
                }
                (Some(_), Some(local_destination)) => {
                    let desired = local_destination.clone();
                    self.mark_subtree_pair(handled, &source, &desired);
                    self.conflict(&source, Some(&source), Some(&desired))
                        .await?;
                }
                (Some(destination), None) => {
                    let local_unchanged =
                        local_action(self.baseline.local.get(&source), self.current.get(&source))
                            == Action::None;
                    if local_unchanged && self.current.get(destination).is_none() {
                        if self.remote_rename(&source, destination).await? {
                            self.migrate_subtree(handled, &source, destination, self.revision)
                                .await?;
                        } else {
                            self.mark_subtree_pair(handled, &source, destination);
                        }
                    } else {
                        self.mark_subtree_pair(handled, &source, destination);
                        self.conflict(&source, Some(&source), Some(&source)).await?;
                    }
                }
                (None, Some(destination)) => {
                    let remote_unchanged =
                        remote_action(self.baseline.remote.get(&source), self.remote.get(&source))
                            == Action::None;
                    if remote_unchanged && !self.remote.contains_key(destination) {
                        if let Some(revision) = self.local_rename(&source, destination).await? {
                            self.migrate_subtree(handled, &source, destination, revision)
                                .await?;
                        } else {
                            self.mark_subtree_pair(handled, &source, destination);
                        }
                    } else {
                        self.mark_subtree_pair(handled, &source, destination);
                        self.conflict(&source, Some(&source), Some(&source)).await?;
                    }
                }
                (None, None) => {}
            }
        }
        Ok(())
    }

    /// Mark every local/baseline path under one renamed directory as handled so
    /// the single per-root rename is the only namespace request issued.
    fn mark_subtree(&self, handled: &mut BTreeSet<String>, from: &str) {
        let prefix = format!("{from}/");
        let targets: Vec<String> = self
            .current
            .nodes()
            .keys()
            .chain(self.baseline.local.nodes().keys())
            .chain(self.baseline.remote.keys())
            .filter(|path| path.starts_with(&prefix))
            .cloned()
            .collect();
        handled.extend(targets);
    }

    /// Block both the source and destination subtrees when a rename cannot be
    /// applied, so the same run never falls back to creating the destination.
    fn mark_subtree_pair(&self, handled: &mut BTreeSet<String>, from: &str, to: &str) {
        self.mark_subtree(handled, from);
        handled.insert(from.to_owned());
        let prefix = format!("{to}/");
        let targets: Vec<String> = self
            .current
            .nodes()
            .keys()
            .chain(self.baseline.local.nodes().keys())
            .chain(self.baseline.remote.keys())
            .chain(self.remote.keys())
            .filter(|path| *path == to || path.starts_with(&prefix))
            .cloned()
            .collect();
        handled.extend(targets);
        handled.insert(to.to_owned());
    }

    /// Relocate every baseline entry under a renamed directory to its new path,
    /// preserving each entry's previously accepted content fingerprint so a
    /// concurrent content edit stays pending instead of being acknowledged by
    /// the rename receipt.
    async fn migrate_subtree(
        &mut self,
        handled: &mut BTreeSet<String>,
        from: &str,
        to: &str,
        revision: Revision,
    ) -> Result<()> {
        handled.insert(from.to_owned());
        handled.insert(to.to_owned());
        let prefix = format!("{from}/");
        let descendants: Vec<String> = self
            .baseline
            .remote
            .keys()
            .filter(|path| path.starts_with(&prefix))
            .cloned()
            .collect();
        for descendant in descendants {
            let destination = format!("{to}{}", &descendant[from.len()..]);
            // Descendants change their path only; their own accepted content,
            // executable bit and revision stay anchored, so a namespace-only
            // parent receipt can never fold unobserved child content or revision
            // into the common baseline.
            self.migrate_baseline_path_child(&descendant, &destination)
                .await?;
            handled.insert(descendant);
            handled.insert(destination);
        }
        let _ = revision;
        Ok(())
    }

    async fn migrate_baseline_path_child(&mut self, from: &str, to: &str) -> Result<()> {
        let Some(entry) = self.baseline.remote.get(from).cloned() else {
            return Ok(());
        };
        let previous = self.baseline.local.get(from).cloned();
        let accepted = self.accepted_local_at(to, previous.as_ref())?;
        let length = accepted
            .as_ref()
            .and_then(|node| match node.kind {
                LocalKind::File { length, .. } => Some(length),
                LocalKind::Directory => None,
            })
            .unwrap_or(0);
        self.replica.store_mut().save_baseline_entry(
            to,
            entry.node,
            entry.directory,
            entry.executable,
            entry.digest,
            length,
            accepted.as_ref(),
            entry.revision,
        )?;
        self.replica.store_mut().remove_baseline_path(from)?;
        Ok(())
    }

    async fn migrate_baseline_path(
        &mut self,
        from: &str,
        to: &str,
        revision: Revision,
    ) -> Result<()> {
        let Some(entry) = self.baseline.remote.get(from).cloned() else {
            return Ok(());
        };
        let previous = self.baseline.local.get(from).cloned();
        let accepted = self.accepted_local_at(to, previous.as_ref())?;
        let length = accepted
            .as_ref()
            .and_then(|node| match node.kind {
                LocalKind::File { length, .. } => Some(length),
                LocalKind::Directory => None,
            })
            .unwrap_or(0);
        // The rename receipt proves the namespace move, not descendant content.
        // Keep the previously accepted remote digest and executable bit.
        self.replica.store_mut().save_baseline_entry(
            to,
            entry.node,
            entry.directory,
            entry.executable,
            entry.digest,
            length,
            accepted.as_ref(),
            revision,
        )?;
        self.replica.store_mut().remove_baseline_path(from)?;
        self.nodes.remove(from);
        self.nodes.insert(to.to_owned(), entry.node);
        self.revisions.remove(from);
        self.revisions.insert(to.to_owned(), revision);
        Ok(())
    }

    /// Build the accepted local fingerprint for a path, preserving the previous
    /// content kind/executable but taking the current inode identity so later
    /// move detection stays accurate.
    fn accepted_local_at(
        &self,
        to: &str,
        previous: Option<&LocalNode>,
    ) -> Result<Option<LocalNode>> {
        let Some(previous) = previous else {
            return Ok(None);
        };
        let fresh = self.replica.scan()?;
        let Some(current) = fresh.get(to) else {
            return Ok(None);
        };
        Ok(Some(LocalNode {
            path: to.to_owned(),
            kind: previous.kind,
            executable: previous.executable,
            identity: current.identity,
            parent: current.parent,
        }))
    }

    fn retained_remote_deleted_dirs(&self) -> BTreeSet<String> {
        let mut retained = BTreeSet::new();
        for (path, entry) in &self.baseline.remote {
            if path.is_empty() || !entry.directory || self.remote.contains_key(path) {
                continue;
            }
            if self.subtree_local_diverges(path) {
                retained.insert(path.clone());
            }
        }
        retained
    }

    fn subtree_local_diverges(&self, directory: &str) -> bool {
        let prefix = format!("{directory}/");
        let mut paths: BTreeSet<&str> = BTreeSet::new();
        paths.extend(self.baseline.local.nodes().keys().map(String::as_str));
        paths.extend(self.current.nodes().keys().map(String::as_str));
        for path in paths {
            if !path.starts_with(&prefix) {
                continue;
            }
            if local_action(self.baseline.local.get(path), self.current.get(path)) != Action::None {
                return true;
            }
        }
        false
    }

    async fn handle_retained(
        &mut self,
        retained: &BTreeSet<String>,
        handled: &mut BTreeSet<String>,
    ) -> Result<()> {
        if retained.is_empty() {
            return Ok(());
        }
        let paths: Vec<String> = self
            .baseline
            .remote
            .keys()
            .cloned()
            .chain(self.current.nodes().keys().cloned())
            .collect();
        for path in paths {
            if path.is_empty() || !under_any_inclusive(retained, &path) {
                continue;
            }
            handled.insert(path.clone());
            let local = local_action(self.baseline.local.get(&path), self.current.get(&path));
            let remote = remote_action(self.baseline.remote.get(&path), self.remote.get(&path));
            if remote == Action::Delete && local == Action::None {
                let kind = self
                    .current
                    .get(&path)
                    .map(|node| {
                        if node.is_directory() {
                            "current_directory"
                        } else {
                            "current_file"
                        }
                    })
                    .unwrap_or("unknown");
                let baseline_node = self
                    .baseline
                    .remote
                    .get(&path)
                    .map(|entry| *entry.node.as_bytes());
                let id = conflict_identity(
                    &path,
                    Some(&path),
                    Some(&path),
                    kind,
                    "deleted",
                    baseline_node,
                    None,
                );
                record_conflict_side(
                    self.outcome,
                    self.replica,
                    &id,
                    &path,
                    Some(&path),
                    Some(&path),
                    kind,
                    "deleted",
                    None,
                    None,
                    Some(self.revision),
                    "a remote-deleted directory still holds local entries; nothing was removed",
                )?;
            }
        }
        Ok(())
    }

    async fn entry(&mut self, path: &str) -> Result<()> {
        let local = local_action(self.baseline.local.get(path), self.current.get(path));
        let remote = remote_action(self.baseline.remote.get(path), self.remote.get(path));
        match (local, remote) {
            (Action::None, Action::None) => Ok(()),
            (Action::Create, Action::None) => {
                let node = self
                    .current
                    .get(path)
                    .cloned()
                    .ok_or_else(|| "local create disappeared".to_string())?;
                if node.is_directory() {
                    self.publish_directory(path).await
                } else {
                    self.publish_file(path, &node).await
                }
            }
            (Action::Modify, Action::None) => {
                let node = self
                    .current
                    .get(path)
                    .cloned()
                    .ok_or_else(|| "local modify disappeared".to_string())?;
                self.publish_modify(path, &node).await
            }
            (Action::None, Action::Create) => {
                let node = self
                    .remote
                    .get(path)
                    .cloned()
                    .ok_or_else(|| "remote create disappeared".to_string())?;
                self.materialize_create(path, &node).await
            }
            (Action::None, Action::Modify) => {
                let node = self
                    .remote
                    .get(path)
                    .cloned()
                    .ok_or_else(|| "remote modify disappeared".to_string())?;
                self.materialize_modify(path, &node).await
            }
            (Action::Delete, Action::Delete) => {
                self.replica.store_mut().remove_baseline_path(path)?;
                Ok(())
            }
            (Action::None, Action::Delete) => self.materialize_delete(path).await,
            (Action::Delete, Action::None) => Ok(()),
            (Action::Create, Action::Create) => {
                let remote_node = self.remote.get(path).cloned().unwrap();
                if canonical_matches(self.replica, self.current.get(path).unwrap(), &remote_node)
                    .await?
                {
                    self.accept_remote(path, &remote_node).await
                } else {
                    self.conflict(path, None, Some(path)).await
                }
            }
            (Action::Modify, Action::Modify) => {
                let remote_node = self.remote.get(path).cloned().unwrap();
                if canonical_matches(self.replica, self.current.get(path).unwrap(), &remote_node)
                    .await?
                {
                    self.accept_remote(path, &remote_node).await
                } else {
                    self.conflict(path, Some(path), Some(path)).await
                }
            }
            _ => {
                let original = self.baseline.local.get(path).is_some()
                    || self.baseline.remote.contains_key(path);
                let original_path = original.then_some(path);
                self.conflict(path, original_path, Some(path)).await
            }
        }
    }

    async fn publish_directory(&mut self, path: &str) -> Result<()> {
        let (parent, name) = split(path);
        let parent_node = self
            .nodes
            .get(parent)
            .copied()
            .ok_or_else(|| format!("directory parent {parent:?} is missing"))?;
        let parent_revision = self.entry_revision(parent);
        let edit = self
            .sync
            .stage_create_directory(parent_node, name, parent_revision)
            .await?;
        self.associate(
            edit,
            path,
            None,
            Some(path),
            "create-directory",
            self.current.get(path),
            Some(parent_node),
            parent_revision,
        )?;
        let Some(revision) = self.publish_edit(edit, path).await? else {
            return Ok(());
        };
        self.revision = revision;
        let node_id = self.namespace_node(edit).await?;
        self.nodes.insert(path.to_owned(), node_id);
        self.revisions.insert(path.to_owned(), revision);
        self.mark_parent_dirty(path);
        if let Some(local) = self.current.get(path) {
            self.accept_local(path, local, node_id, revision).await?;
        }
        self.refresh_parent_baseline(path, revision).await?;
        self.replica
            .store_mut()
            .update_work(edit.as_bytes(), "committed", Some(revision), None)?;
        self.outcome.published_local += 1;
        self.outcome.revision = Some(revision);
        Ok(())
    }

    async fn publish_file(&mut self, path: &str, node: &LocalNode) -> Result<()> {
        let (parent, name) = split(path);
        let parent_node = self
            .nodes
            .get(parent)
            .copied()
            .ok_or_else(|| format!("file parent {parent:?} is missing"))?;
        let parent_revision = self.entry_revision(parent);
        let captured = self.replica.capture_file(path, node)?;
        let mut source =
            tokio::fs::File::from_std(self.replica.open_artifact(&captured.artifact.id)?);
        let edit = self
            .sync
            .stage_create_file(
                parent_node,
                name,
                parent_revision,
                FileMetadata::new(node.executable),
                &mut source,
            )
            .await?;
        self.associate(
            edit,
            path,
            None,
            Some(path),
            "create-file",
            Some(node),
            Some(parent_node),
            parent_revision,
        )?;
        let Some(revision) = self.publish_edit(edit, path).await? else {
            return Ok(());
        };
        self.revision = revision;
        let node_id = self.namespace_node(edit).await?;
        self.nodes.insert(path.to_owned(), node_id);
        self.revisions.insert(path.to_owned(), revision);
        self.mark_parent_dirty(path);
        self.accept_local(path, node, node_id, revision).await?;
        self.refresh_parent_baseline(path, revision).await?;
        self.replica
            .store_mut()
            .update_work(edit.as_bytes(), "committed", Some(revision), None)?;
        self.outcome.published_local += 1;
        self.outcome.revision = Some(revision);
        Ok(())
    }

    async fn publish_modify(&mut self, path: &str, node: &LocalNode) -> Result<()> {
        let remote_node = self
            .nodes
            .get(path)
            .copied()
            .ok_or_else(|| format!("modify source {path:?} is missing"))?;
        let predicate = self.entry_revision(path);
        let captured = self.replica.capture_file(path, node)?;
        let mut source =
            tokio::fs::File::from_std(self.replica.open_artifact(&captured.artifact.id)?);
        let edit = self.sync.stage(remote_node, predicate, &mut source).await?;
        self.associate(
            edit,
            path,
            Some(path),
            Some(path),
            "content",
            Some(node),
            Some(remote_node),
            predicate,
        )?;
        let Some(revision) = self.publish_edit(edit, path).await? else {
            return Ok(());
        };
        self.revision = revision;
        self.revisions.insert(path.to_owned(), revision);
        self.accept_local(path, node, remote_node, revision).await?;
        self.replica
            .store_mut()
            .update_work(edit.as_bytes(), "committed", Some(revision), None)?;
        self.outcome.published_local += 1;
        self.outcome.revision = Some(revision);
        Ok(())
    }

    async fn publish_remove(&mut self, path: &str) -> Result<()> {
        let node = self
            .nodes
            .get(path)
            .copied()
            .ok_or_else(|| format!("remove source {path:?} is missing"))?;
        let predicate = self.entry_revision(path);
        let edit = self.sync.stage_remove(node, predicate).await?;
        self.associate(
            edit,
            path,
            Some(path),
            Some(path),
            "remove",
            None,
            Some(node),
            predicate,
        )?;
        let Some(revision) = self.publish_edit(edit, path).await? else {
            return Ok(());
        };
        self.revision = revision;
        self.nodes.remove(path);
        self.revisions.remove(path);
        self.mark_parent_dirty(path);
        self.replica.store_mut().remove_baseline_path(path)?;
        self.refresh_parent_baseline(path, revision).await?;
        self.replica
            .store_mut()
            .update_work(edit.as_bytes(), "committed", Some(revision), None)?;
        self.outcome.published_local += 1;
        self.outcome.revision = Some(revision);
        Ok(())
    }

    async fn local_rename(&mut self, from: &str, to: &str) -> Result<Option<Revision>> {
        let node = self
            .nodes
            .get(from)
            .copied()
            .ok_or_else(|| format!("rename source {from:?} is missing"))?;
        let predicate = self.entry_revision(from);
        let (parent, name) = split(to);
        let parent_node = self
            .nodes
            .get(parent)
            .copied()
            .ok_or_else(|| format!("rename parent {parent:?} is missing"))?;
        let edit = self
            .sync
            .stage_rename(node, predicate, parent_node, name, RenameMode::NoReplace)
            .await?;
        self.associate(
            edit,
            to,
            Some(from),
            Some(to),
            "rename",
            None,
            Some(node),
            predicate,
        )?;
        let Some(revision) = self.publish_edit(edit, from).await? else {
            return Ok(None);
        };
        self.revision = revision;
        // The rename receipt proves the namespace move, never a concurrent
        // content edit. Preserve the previously accepted fingerprint for `to`.
        self.mark_parent_dirty(from);
        self.mark_parent_dirty(to);
        self.migrate_baseline_path(from, to, revision).await?;
        self.refresh_parent_baseline(from, revision).await?;
        self.refresh_parent_baseline(to, revision).await?;
        self.replica
            .store_mut()
            .update_work(edit.as_bytes(), "committed", Some(revision), None)?;
        self.outcome.published_local += 1;
        self.outcome.revision = Some(revision);
        Ok(Some(revision))
    }

    async fn materialize_create(&mut self, path: &str, node: &Node) -> Result<()> {
        match node.kind() {
            NodeKind::Directory { .. } => match self.replica.create_directory_local(path) {
                Ok(()) => {
                    self.nodes.insert(path.to_owned(), node.id());
                    self.revisions.insert(path.to_owned(), self.revision);
                    self.outcome.materialized_remote += 1;
                    self.accept_remote(path, node).await?;
                    Ok(())
                }
                Err(_) if self.current.get(path).is_some_and(LocalNode::is_directory) => {
                    self.nodes.insert(path.to_owned(), node.id());
                    self.revisions.insert(path.to_owned(), self.revision);
                    self.accept_remote(path, node).await?;
                    Ok(())
                }
                Err(_) => self.conflict(path, None, Some(path)).await,
            },
            NodeKind::File(_) => {
                if self.current.get(path).is_some() {
                    return self.conflict(path, None, Some(path)).await;
                }
                let mut staged = self.stage_remote_file(node).await?;
                match self
                    .replica
                    .create_file_local(path, node.executable(), &mut staged)
                {
                    Ok(()) => {
                        self.nodes.insert(path.to_owned(), node.id());
                        self.revisions.insert(path.to_owned(), self.revision);
                        self.outcome.materialized_remote += 1;
                        self.accept_remote(path, node).await?;
                        Ok(())
                    }
                    Err(_) => self.conflict(path, None, Some(path)).await,
                }
            }
        }
    }

    async fn materialize_modify(&mut self, path: &str, node: &Node) -> Result<()> {
        let expected = self
            .current
            .get(path)
            .cloned()
            .ok_or_else(|| format!("remote modify path {path:?} is local-absent"))?;
        if expected.is_directory() || node.is_directory() {
            return self.conflict(path, Some(path), Some(path)).await;
        }
        let snapshot = self
            .sync
            .authority()
            .observe_revision(self.revision)
            .await?;
        let version = snapshot.open_file(node.id()).await?;
        let length = version.descriptor().content_id().length();
        let mut install = self.replica.prepare_install(
            path,
            &expected,
            node.executable(),
            Some(self.revision.to_bytes()),
        )?;
        let mut position = 0;
        while position < length {
            let end = (position + 64 * 1024).min(length);
            let mut chunk = Vec::new();
            version.read_range(position..end, &mut chunk).await?;
            install.write_all(&chunk)?;
            position = end;
        }
        match install.finish() {
            Ok(_) => {
                self.nodes.insert(path.to_owned(), node.id());
                self.revisions.insert(path.to_owned(), self.revision);
                self.outcome.materialized_remote += 1;
                self.accept_remote(path, node).await?;
                Ok(())
            }
            Err(_) => self.conflict(path, Some(path), Some(path)).await,
        }
    }

    async fn materialize_delete(&mut self, path: &str) -> Result<()> {
        if path.is_empty() {
            return Ok(());
        }
        let expected = self.baseline.local.clone();
        match self.replica.quarantine(
            path,
            &expected,
            "remote deletion",
            Some(self.revision.to_bytes()),
        ) {
            Ok(_) => {
                self.nodes.remove(path);
                self.revisions.remove(path);
                self.replica.store_mut().remove_baseline_path(path)?;
                self.outcome.materialized_remote += 1;
                Ok(())
            }
            Err(_) => self.conflict(path, Some(path), Some(path)).await,
        }
    }

    async fn remote_rename(&mut self, from: &str, to: &str) -> Result<bool> {
        match self.replica.rename_local(from, to) {
            Ok(()) => {
                self.migrate_baseline_path(from, to, self.revision).await?;
                self.outcome.materialized_remote += 1;
                Ok(true)
            }
            Err(_) => {
                self.conflict(from, Some(from), Some(to)).await?;
                Ok(false)
            }
        }
    }

    /// Publish an associated edit and translate a predicate Conflict into a
    /// durable directory conflict instead of aborting the run.
    async fn publish_edit(&mut self, edit: EditId, path: &str) -> Result<Option<Revision>> {
        match self.sync.publish(edit).await {
            Ok(revision) => Ok(Some(revision)),
            Err(error) if error.kind() == crate::runtime::ErrorKind::Conflict => {
                self.replica.store_mut().update_work(
                    edit.as_bytes(),
                    "conflict",
                    None,
                    Some(&error.to_string()),
                )?;
                self.record_publish_conflict(path).await?;
                Ok(None)
            }
            Err(error) if error.kind() == crate::runtime::ErrorKind::Retryable => {
                self.replica.store_mut().update_work(
                    edit.as_bytes(),
                    "retryable",
                    None,
                    Some(&error.to_string()),
                )?;
                Ok(None)
            }
            Err(error) => {
                if let Some(commit) = error.commit_id() {
                    self.replica
                        .store_mut()
                        .set_work_commit_id(edit.as_bytes(), *commit.as_bytes())?;
                }
                self.replica.store_mut().update_work(
                    edit.as_bytes(),
                    work_state_for(&error),
                    None,
                    Some(&error.to_string()),
                )?;
                Err(error.into())
            }
        }
    }

    /// Record the current remote evidence for a path whose publication
    /// predicate no longer held.
    async fn record_publish_conflict(&mut self, path: &str) -> Result<()> {
        let observed = self.sync.authority().observe_latest().await?;
        let remote_node = observed.resolve(path).await?;
        let local = self.local_side(path);
        let remote = match &remote_node {
            Some(node) if node.is_directory() => "current_directory",
            Some(_) => "current_file",
            None => "deleted",
        };
        let baseline_node = self.baseline.remote.get(path).map(|entry| entry.node);
        let remote_id = remote_node.as_ref().map(Node::id);
        let id = conflict_identity(
            path,
            Some(path),
            Some(path),
            local,
            remote,
            baseline_node.map(|node| *node.as_bytes()),
            remote_id,
        );
        let existing_artifact = self
            .replica
            .store()
            .conflicts()?
            .into_iter()
            .find(|conflict| conflict.id == id)
            .and_then(|conflict| conflict.local_artifact);
        let local_artifact = match existing_artifact {
            Some(artifact) => Some(artifact),
            None => self
                .current
                .get(path)
                .filter(|node| matches!(node.kind, LocalKind::File { .. }))
                .and_then(|node| self.replica.capture_file(path, node).ok())
                .map(|captured| captured.artifact.id),
        };
        record_conflict_side(
            self.outcome,
            self.replica,
            &id,
            path,
            Some(path),
            Some(path),
            local,
            remote,
            local_artifact.as_deref(),
            remote_id,
            Some(observed.revision()),
            "a concurrent remote edit changed the publication predicate",
        )
    }

    fn entry_revision(&self, path: &str) -> Revision {
        // Preserve each path's own accepted revision, except where our earlier
        // namespace receipts in this run changed that path; then stage against
        // the running revision. This keeps unrelated paths from being rebased
        // onto a newer global revision (which could hide an external edit).
        if self.dirty.contains(path) {
            return self.revision;
        }
        self.revisions
            .get(path)
            .copied()
            .or_else(|| self.baseline.remote.get(path).map(|entry| entry.revision))
            .unwrap_or(self.revision)
    }

    /// A path whose parent directory entry list we changed this run.
    fn mark_parent_dirty(&mut self, path: &str) {
        let (parent, _) = split(path);
        self.dirty.insert(parent.to_owned());
    }

    /// A child creation/removal changes the parent directory's membership, so
    /// the parent's accepted revision must advance to the receipt that reflects
    /// it. Without this a later namespace predicate on the parent is planned
    /// against a stale membership and conflicts spuriously.
    async fn refresh_parent_baseline(&mut self, path: &str, revision: Revision) -> Result<()> {
        let (parent, _) = split(path);
        let snapshot = self.sync.authority().observe_revision(revision).await?;
        let Some(node) = snapshot.resolve(parent).await? else {
            return Ok(());
        };
        // Only the remote parent node/revision advances here. Preserve the
        // previously accepted local parent fingerprint so a pending local
        // deletion or rename of the parent is not acknowledged by a child's
        // receipt.
        let accepted = self
            .replica
            .store()
            .load_baseline()?
            .and_then(|baseline| baseline.local.get(parent).cloned());
        self.replica
            .store_mut()
            .save_baseline_path(parent, &node, accepted.as_ref(), revision)?;
        self.revisions.insert(parent.to_owned(), revision);
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn associate(
        &mut self,
        edit: EditId,
        path: &str,
        original_path: Option<&str>,
        desired_path: Option<&str>,
        operation: &str,
        local: Option<&LocalNode>,
        baseline_node: Option<NodeId>,
        baseline_revision: Revision,
    ) -> Result<()> {
        let (local_kind, local_digest, local_length, local_executable) = match local {
            Some(node) => (
                Some(
                    if node.is_directory() {
                        "directory"
                    } else {
                        "file"
                    }
                    .to_owned(),
                ),
                match node.kind {
                    LocalKind::File { digest, .. } => Some(*digest.as_bytes()),
                    LocalKind::Directory => None,
                },
                match node.kind {
                    LocalKind::File { length, .. } => Some(length),
                    LocalKind::Directory => None,
                },
                Some(node.executable),
            ),
            None => (None, None, None, None),
        };
        self.replica.store_mut().insert_work(&WorkRecord {
            seq: 0,
            id: *edit.as_bytes(),
            path: path.to_owned(),
            original_path: original_path.map(str::to_owned),
            desired_path: desired_path.map(str::to_owned),
            operation: operation.to_owned(),
            local_kind,
            local_digest,
            local_length,
            local_executable,
            local_dev: local.map(|node| node.identity.dev),
            local_ino: local.map(|node| node.identity.ino),
            local_parent_dev: local.and_then(|node| node.parent.map(|parent| parent.dev)),
            local_parent_ino: local.and_then(|node| node.parent.map(|parent| parent.ino)),
            baseline_node: baseline_node.map(|node| *node.as_bytes()),
            baseline_revision: baseline_revision.to_bytes(),
            remote_revision: None,
            commit_id: None,
            conflict: None,
            source_artifact: None,
            state: "prepared".to_owned(),
            error: None,
        })?;
        Ok(())
    }

    async fn conflict(
        &mut self,
        path: &str,
        original_path: Option<&str>,
        desired_path: Option<&str>,
    ) -> Result<()> {
        // The observable local side is classified by presence. A preserved
        // backup is reported separately as `local_artifact` and does not turn
        // an ordinary content divergence into a retained-artifact side.
        let local = self.local_side(path);
        let remote = self.remote_side(path);
        let baseline_node = self.baseline.remote.get(path).map(|entry| entry.node);
        let remote_node = self.remote.get(path).map(Node::id);
        let id = conflict_identity(
            path,
            original_path,
            desired_path,
            local,
            remote,
            baseline_node.map(|node| *node.as_bytes()),
            remote_node,
        );
        let existing_artifact = self
            .replica
            .store()
            .conflicts()?
            .into_iter()
            .find(|conflict| conflict.id == id)
            .and_then(|conflict| conflict.local_artifact);
        let local_artifact = match existing_artifact {
            Some(artifact) => Some(artifact),
            None => self
                .current
                .get(path)
                .filter(|node| matches!(node.kind, LocalKind::File { .. }))
                .and_then(|node| self.replica.capture_file(path, node).ok())
                .map(|captured| captured.artifact.id),
        };
        record_conflict_side(
            self.outcome,
            self.replica,
            &id,
            path,
            original_path,
            desired_path,
            local,
            remote,
            local_artifact.as_deref(),
            remote_node,
            Some(self.revision),
            "local and remote changed from the same baseline",
        )
    }

    /// The local side of a conflict by observable content, following a local
    /// move of the same inode so a rename is reported as retained content.
    fn local_side(&self, path: &str) -> &'static str {
        if let Some(node) = self.current.get(path) {
            return local_kind_side(node);
        }
        if let Some(baseline) = self.baseline.local.get(path)
            && self
                .current
                .nodes()
                .values()
                .any(|node| node.identity == baseline.identity)
        {
            return local_kind_side(baseline);
        }
        "deleted"
    }

    fn remote_side(&self, path: &str) -> &'static str {
        match self.remote.get(path) {
            Some(node) if node.is_directory() => "current_directory",
            Some(_) => "current_file",
            None => "deleted",
        }
    }

    async fn namespace_node(&mut self, edit: EditId) -> Result<NodeId> {
        let status = self.sync.namespace_status(edit).await?;
        Ok(status.intent.operation.node_id())
    }

    async fn accept_local(
        &mut self,
        path: &str,
        accepted: &LocalNode,
        node_id: NodeId,
        revision: Revision,
    ) -> Result<()> {
        let snapshot = self.sync.authority().observe_revision(revision).await?;
        let Some(node) = snapshot.node(node_id).await? else {
            return Ok(());
        };
        self.replica
            .store_mut()
            .save_baseline_path(path, &node, Some(accepted), revision)?;
        self.nodes.insert(path.to_owned(), node_id);
        self.revisions.insert(path.to_owned(), revision);
        Ok(())
    }

    async fn accept_remote(&mut self, path: &str, node: &Node) -> Result<()> {
        let fresh = self.replica.scan()?;
        let Some(current) = fresh.get(path).cloned() else {
            return Ok(());
        };
        if canonical_matches(self.replica, &current, node).await? {
            self.replica.store_mut().save_baseline_path(
                path,
                node,
                Some(&current),
                self.revision,
            )?;
        }
        Ok(())
    }

    async fn stage_remote_file(&self, node: &Node) -> Result<std::fs::File> {
        let snapshot = self
            .sync
            .authority()
            .observe_revision(self.revision)
            .await?;
        let version = snapshot.open_file(node.id()).await?;
        let length = version.descriptor().content_id().length();
        let mut output = tokio::fs::File::from_std(tempfile::tempfile()?);
        let mut position = 0;
        while position < length {
            let end = (position + 64 * 1024).min(length);
            version.read_range(position..end, &mut output).await?;
            position = end;
        }
        let mut spool = output.into_std().await;
        spool.seek(SeekFrom::Start(0))?;
        Ok(spool)
    }
}

fn local_action(baseline: Option<&LocalNode>, current: Option<&LocalNode>) -> Action {
    match (baseline, current) {
        (None, None) => Action::None,
        (None, Some(_)) => Action::Create,
        (Some(_), None) => Action::Delete,
        (Some(baseline), Some(current)) => {
            if local_equal(baseline, current) {
                Action::None
            } else {
                Action::Modify
            }
        }
    }
}

fn local_kind_side(node: &LocalNode) -> &'static str {
    if node.is_directory() {
        "current_directory"
    } else {
        "current_file"
    }
}

fn work_state_for(error: &crate::runtime::Error) -> &'static str {
    match error.kind() {
        crate::runtime::ErrorKind::Unknown => "unknown",
        crate::runtime::ErrorKind::Retryable => "retryable",
        crate::runtime::ErrorKind::Conflict => "conflict",
        _ => "failed",
    }
}

/// The durable state of one associated work record, preferring the Sync
/// journal's own outcome when it is present.
fn effective_intent_state(
    record: &WorkRecord,
    inspect: Option<&crate::sync::SyncInspect>,
) -> Option<IntentState> {
    if let Some(inspect) = inspect {
        let edit = EditId::from_bytes(record.id);
        if let Some(status) = inspect
            .namespace
            .iter()
            .find(|status| status.intent.id == edit)
        {
            return Some(namespace_intent_state(&status.state));
        }
        if let Some(content) = inspect.content.iter().find(|content| content.id == edit) {
            return Some(intent_state_from_handle(&content.handle));
        }
    }
    Some(intent_state_from_work(&record.state))
}

/// Select the current work record for a path: an unsatisfied request takes
/// priority over an older committed receipt so a newer pending or Unknown edit
/// is never hidden by history.
fn select_work<'a>(
    work: &'a [WorkRecord],
    path: &str,
    _inspect: Option<&crate::sync::SyncInspect>,
) -> Option<&'a WorkRecord> {
    // The newest non-retired request is the current one. History remains
    // stored but never hides a later request for the same path.
    work.iter()
        .filter(|record| record.path == path && record.state != "superseded")
        .max_by_key(|record| record.seq)
        .or_else(|| {
            work.iter()
                .filter(|record| record.path == path)
                .max_by_key(|record| record.seq)
        })
}

/// Count each associated logical request once by its effective durable state.
fn associated_counts(
    work: &[WorkRecord],
    inspect: Option<&crate::sync::SyncInspect>,
) -> (u64, u64) {
    let mut unknown = 0_u64;
    let mut failed = 0_u64;
    for record in work {
        match effective_intent_state(record, inspect) {
            Some(IntentState::Unknown) => unknown += 1,
            Some(IntentState::Failed) => failed += 1,
            _ => {}
        }
    }
    (unknown, failed)
}

fn remote_action(baseline: Option<&BaselineNode>, current: Option<&Node>) -> Action {
    match (baseline, current) {
        (None, None) => Action::None,
        (None, Some(_)) => Action::Create,
        (Some(_), None) => Action::Delete,
        (Some(baseline), Some(current)) => {
            if remote_equal(baseline, current) {
                Action::None
            } else {
                Action::Modify
            }
        }
    }
}

fn local_equal(left: &LocalNode, right: &LocalNode) -> bool {
    left.kind == right.kind && left.executable == right.executable
}

async fn canonical_matches(
    replica: &mut Replica,
    local: &LocalNode,
    remote: &Node,
) -> Result<bool> {
    match (local.kind, remote.kind()) {
        (LocalKind::Directory, NodeKind::Directory { .. }) => Ok(true),
        (LocalKind::File { .. }, NodeKind::File(file)) => {
            let captured = replica.capture_file(&local.path, local)?;
            let mut source =
                tokio::fs::File::from_std(replica.open_artifact(&captured.artifact.id)?);
            let content = yinyang_core::ContentId::calculate(&mut source).await?;
            Ok(local.executable == remote.executable() && content == file.content_id())
        }
        _ => Ok(false),
    }
}

fn remote_equal(baseline: &BaselineNode, current: &Node) -> bool {
    let digest = match current.kind() {
        NodeKind::File(file) => Some(*file.content_id().digest()),
        NodeKind::Directory { .. } => None,
    };
    baseline.node == current.id()
        && baseline.directory == current.is_directory()
        && baseline.executable == current.executable()
        && baseline.digest == digest
}

fn revision_bytes(revision: Option<[u8; 24]>) -> Option<Revision> {
    revision.map(Revision::from_bytes)
}

#[allow(clippy::too_many_arguments)]
fn conflict_identity(
    path: &str,
    original_path: Option<&str>,
    desired_path: Option<&str>,
    local: &str,
    remote: &str,
    baseline_node: Option<[u8; 16]>,
    remote_node: Option<NodeId>,
) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(path.as_bytes());
    hasher.update(original_path.unwrap_or("").as_bytes());
    hasher.update(desired_path.unwrap_or("").as_bytes());
    hasher.update(local.as_bytes());
    hasher.update(remote.as_bytes());
    let baseline = baseline_node.unwrap_or([0; 16]);
    hasher.update(&baseline);
    let remote_id = remote_node.map(|node| *node.as_bytes()).unwrap_or([0; 16]);
    hasher.update(&remote_id);
    hex16(&hasher.finalize())
}

/// A stable 32-hex identity derived from an artifact, used for the read-only
/// conflict reported when a retained inode gains late writer bytes.
fn artifact_conflict_identity(artifact_id: &str) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"yinyang-retained-conflict-1");
    hasher.update(artifact_id.as_bytes());
    hex16(&hasher.finalize())
}

fn hex16(digest: &blake3::Hash) -> String {
    digest.as_bytes()[..16]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// Record one active conflict. Detection is idempotent: the same evidence
/// keeps one record and one stable identity until an explicit resolution, and a
/// retained backup is reused instead of duplicated on every run.
#[allow(clippy::too_many_arguments)]
fn record_conflict_side(
    outcome: &mut RunOutcome,
    replica: &mut Replica,
    id: &str,
    path: &str,
    original_path: Option<&str>,
    desired_path: Option<&str>,
    local: &str,
    remote: &str,
    local_artifact: Option<&str>,
    remote_node: Option<NodeId>,
    remote_revision: Option<Revision>,
    error: &str,
) -> Result<()> {
    let existing = replica
        .store()
        .conflicts()?
        .into_iter()
        .find(|conflict| conflict.id == id);
    let local_artifact = local_artifact
        .map(str::to_owned)
        .or_else(|| existing.as_ref().and_then(|c| c.local_artifact.clone()));
    let record = ConflictRecord {
        id: id.to_owned(),
        path: path.to_owned(),
        original_path: original_path.map(str::to_owned),
        desired_path: desired_path.map(str::to_owned),
        local: local.to_owned(),
        remote: remote.to_owned(),
        local_artifact,
        remote_node: remote_node.map(|value| *value.as_bytes()),
        remote_revision: remote_revision.map(Revision::to_bytes),
        error: error.to_owned(),
    };
    if existing.is_some() {
        replica.store_mut().refresh_conflict(&record)?;
    } else {
        replica.store_mut().insert_conflict(&record)?;
    }
    outcome.conflicts.push(Conflict {
        id: record.id,
        path: record.path,
        original_path: record.original_path,
        desired_path: record.desired_path,
        local: record.local,
        remote: record.remote,
        local_artifact: record.local_artifact,
        remote_node,
        remote_revision: record.remote_revision,
        error: record.error,
    });
    Ok(())
}

fn split(path: &str) -> (&str, &str) {
    path.rsplit_once('/').unwrap_or(("", path))
}

fn depth(path: &str) -> usize {
    path.matches('/').count()
}

fn under_any(dirs: &BTreeSet<String>, path: &str) -> bool {
    dirs.iter().any(|dir| {
        path != dir
            && path
                .strip_prefix(dir)
                .is_some_and(|suffix| suffix.starts_with('/'))
    })
}

fn under_any_inclusive(dirs: &BTreeSet<String>, path: &str) -> bool {
    dirs.iter().any(|dir| {
        path == dir
            || path
                .strip_prefix(dir)
                .is_some_and(|suffix| suffix.starts_with('/'))
    })
}

fn local_evidence(node: Option<&LocalNode>) -> LocalEvidence {
    match node {
        None => LocalEvidence {
            kind: None,
            length: None,
            digest: None,
            executable: None,
            presence: Presence::Absent,
        },
        Some(node) => match node.kind {
            LocalKind::Directory => LocalEvidence {
                kind: Some(EntryKind::Directory),
                length: None,
                digest: None,
                executable: Some(node.executable),
                presence: Presence::Present,
            },
            LocalKind::File { length, digest } => LocalEvidence {
                kind: Some(EntryKind::File),
                length: Some(length),
                digest: Some(*digest.as_bytes()),
                executable: Some(node.executable),
                presence: Presence::Present,
            },
        },
    }
}

fn remote_evidence(node: Option<&Node>, revision: Option<Revision>) -> RemoteEvidence {
    match node {
        None => RemoteEvidence {
            kind: None,
            length: None,
            digest: None,
            executable: None,
            presence: Presence::Absent,
            node: None,
            revision: None,
        },
        Some(node) => {
            let (kind, length, digest) = match node.kind() {
                NodeKind::Directory { .. } => (EntryKind::Directory, None, None),
                NodeKind::File(file) => (
                    EntryKind::File,
                    Some(file.content_id().length()),
                    Some(*file.content_id().digest()),
                ),
            };
            RemoteEvidence {
                kind: Some(kind),
                length,
                digest,
                executable: Some(node.executable()),
                presence: Presence::Present,
                node: Some(node.id()),
                revision,
            }
        }
    }
}

fn intent_evidence(
    work: Option<&WorkRecord>,
    inspect: Option<&crate::sync::SyncInspect>,
) -> IntentEvidence {
    let Some(work) = work else {
        return IntentEvidence::default();
    };
    let operation = match work.operation.as_str() {
        "content" => IntentOperation::Content,
        "create-file" => IntentOperation::CreateFile,
        "create-directory" => IntentOperation::CreateDirectory,
        "remove" => IntentOperation::Remove,
        "rename" => IntentOperation::Rename,
        "set-executable" => IntentOperation::SetExecutable,
        _ => return IntentEvidence::default(),
    };
    let mut evidence = IntentEvidence {
        id: Some(work.id),
        operation: Some(operation),
        state: Some(intent_state_from_work(&work.state)),
        commit_id: work.commit_id.map(CommitId::from_bytes),
        receipt_revision: work.remote_revision.map(Revision::from_bytes),
        error: work.error.clone(),
    };
    if let Some(inspect) = inspect {
        let edit = EditId::from_bytes(work.id);
        if let Some(status) = inspect
            .namespace
            .iter()
            .find(|status| status.intent.id == edit)
        {
            evidence.commit_id = Some(status.intent.commit_id);
            evidence.state = Some(namespace_intent_state(&status.state));
            if let NamespaceState::Committed(receipt) = &status.state {
                evidence.receipt_revision = Some(receipt.cursor.revision);
            }
            evidence.error = status.failure.as_ref().map(|failure| failure.to_string());
        } else if let Some(content) = inspect.content.iter().find(|content| content.id == edit) {
            evidence.state = Some(intent_state_from_handle(&content.handle));
            evidence.commit_id = content
                .handle
                .frozen_commit_id
                .or(work.commit_id.map(CommitId::from_bytes));
            evidence.error = content
                .handle
                .error
                .as_ref()
                .map(|failure| failure.to_string());
        }
    }
    evidence
}

fn intent_state_from_work(state: &str) -> IntentState {
    match state {
        "prepared" => IntentState::Prepared,
        "unknown" => IntentState::Unknown,
        "committed" => IntentState::Committed,
        "superseded" => IntentState::Committed,
        "conflict" => IntentState::Conflict,
        "retryable" => IntentState::Retryable,
        _ => IntentState::Failed,
    }
}

fn namespace_intent_state(state: &NamespaceState) -> IntentState {
    match state {
        NamespaceState::Prepared => IntentState::Prepared,
        NamespaceState::Unknown => IntentState::Unknown,
        NamespaceState::Committed(_) => IntentState::Committed,
        NamespaceState::Conflict => IntentState::Conflict,
        NamespaceState::Retryable => IntentState::Retryable,
        NamespaceState::Failed => IntentState::Failed,
    }
}

fn intent_state_from_handle(handle: &crate::runtime::HandleStatus) -> IntentState {
    if handle.conflict {
        return IntentState::Conflict;
    }
    match handle.error.as_ref().map(|failure| failure.kind()) {
        Some(crate::runtime::ErrorKind::Unknown) => IntentState::Unknown,
        Some(crate::runtime::ErrorKind::Retryable) => IntentState::Retryable,
        Some(crate::runtime::ErrorKind::Conflict) => IntentState::Conflict,
        Some(_) => IntentState::Failed,
        None if handle.pending || handle.frozen => IntentState::Prepared,
        None => IntentState::Committed,
    }
}
