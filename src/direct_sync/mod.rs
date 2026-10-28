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

//! Explicit finite reconciliation between one ordinary directory and one
//! storage-native Direct object namespace.

mod adapter;
mod local;
mod state;

use crate::replica::{Error, ErrorKind, LocalKind, LocalNode, LocalSnapshot, Result};
pub use adapter::DirectRemoteAdapter;
use local::LocalTree;
use serde::{Deserialize, Serialize};
pub use state::{
    ArtifactKind, ArtifactPhase, ArtifactRecord, ConflictPresence, ConflictRecord, ConflictSide,
    EvidenceKind, InstallState, RemoteOperation, RemotePin, WorkState,
};
use state::{BaselineEntry, InstallOperation, InstallRecord, LocalFingerprint, Store, WorkRecord};
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::io::Write;
use std::path::{Path, PathBuf};

pub const STATUS_SCHEMA: &str = "yinyang.direct-sync.status.v1";

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RemoteKind {
    File,
    Directory,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RemoteEntry {
    pub path: String,
    pub kind: RemoteKind,
    pub size: u64,
    pub pin: Option<RemotePin>,
    /// Opaque observation identity used by the adapter to reopen exactly this
    /// accepted object version. It is not a Managed NodeId.
    pub observation: [u8; 16],
}

impl RemoteEntry {
    pub fn is_file(&self) -> bool {
        self.kind == RemoteKind::File
    }
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct RemoteSnapshot {
    pub revision: Option<String>,
    pub entries: BTreeMap<String, RemoteEntry>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RemotePrecondition {
    Absent,
    Etag(String),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MutationOutcome {
    Applied { accepted: Option<RemotePin> },
    Conflict,
    Unknown { error: String },
    Unsupported { message: String },
}

/// Storage-native operations used by Direct Sync.
///
/// The adapter never turns a later observation into proof that an earlier
/// request committed. Only [`MutationOutcome::Applied`] may advance the common
/// baseline for a published edit.
pub trait DirectRemote {
    fn namespace_id(&self) -> String;

    fn observe(&self) -> impl Future<Output = Result<RemoteSnapshot>> + Send;

    fn read_to_path<'a>(
        &'a self,
        entry: &'a RemoteEntry,
        output: &'a Path,
    ) -> impl Future<Output = Result<()>> + Send + 'a;

    fn publish_file<'a>(
        &'a self,
        path: &'a str,
        source: &'a Path,
        precondition: &'a RemotePrecondition,
    ) -> impl Future<Output = Result<MutationOutcome>> + Send + 'a;

    fn delete<'a>(
        &'a self,
        path: &'a str,
        precondition: &'a RemotePrecondition,
    ) -> impl Future<Output = Result<MutationOutcome>> + Send + 'a;
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EntryState {
    InSync,
    LocalPending,
    RemotePending,
    Conflict,
    Unknown,
    Failed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BindingEvidence {
    pub state: BindingState,
    pub state_directory: PathBuf,
    pub root: Option<PathBuf>,
    pub namespace: Option<String>,
    pub same_filesystem: Option<bool>,
    pub error: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RemoteStatus {
    pub availability: RemoteAvailability,
    pub revision: Option<String>,
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
    pub presence: ConflictPresence,
    pub kind: Option<EvidenceKind>,
    pub length: Option<u64>,
    pub digest: Option<[u8; 32]>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RemoteEvidence {
    pub presence: ConflictPresence,
    pub kind: Option<EvidenceKind>,
    pub length: Option<u64>,
    pub pin: Option<RemotePin>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BaselineEvidence {
    pub local_digest: Option<[u8; 32]>,
    pub local_length: Option<u64>,
    pub remote: Option<RemotePin>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkEvidence {
    pub id: Option<String>,
    pub operation: Option<RemoteOperation>,
    pub state: Option<WorkState>,
    pub install_state: Option<InstallState>,
    pub error: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EntryEvidence {
    pub path: String,
    pub state: EntryState,
    pub local: LocalEvidence,
    pub remote: RemoteEvidence,
    pub baseline: BaselineEvidence,
    pub work: WorkEvidence,
    pub conflict: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Inspection {
    pub binding: BindingEvidence,
    pub remote: RemoteStatus,
    pub local: LocalStatus,
    pub summary: StatusSummary,
    pub entries: Vec<EntryEvidence>,
    pub conflicts: Vec<ConflictRecord>,
    pub artifacts: Vec<ArtifactRecord>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RunOutcome {
    pub published_local: u64,
    pub materialized_remote: u64,
    pub deleted_remote: u64,
    pub deleted_local: u64,
    pub pending: u64,
    pub unknown: u64,
    pub conflicts: usize,
    pub in_sync: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Resolution {
    KeepLocal,
    KeepRemote,
}

pub struct DirectSync<R> {
    remote: R,
    local: LocalTree,
    store: Store,
    namespace: String,
}

impl<R: DirectRemote> DirectSync<R> {
    /// Bind a new state directory. An unbound namespace admits at most one
    /// non-empty file side; two non-empty sides require explicit resolution.
    pub async fn init(remote: R, state: &Path, root: &Path) -> Result<Self> {
        let database = state.join("direct-sync.db");
        match std::fs::symlink_metadata(&database) {
            Ok(_) => {
                return Err(Error::invalid(format!(
                    "Direct Sync state is already initialized at {}",
                    state.display()
                )));
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(Error::io(
                    "inspect Direct Sync state before initialization",
                    &database,
                    error,
                ));
            }
        }
        let local = LocalTree::create(root, state)?;
        let namespace = remote.namespace_id();
        let local_scan = local.scan()?;
        let remote_snapshot = remote.observe().await?;
        reject_unsupported_directories(&local_scan, &remote_snapshot, &BTreeSet::new())?;
        let local_files = local_files(&local_scan);
        let remote_files = remote_files(&remote_snapshot);
        if !local_files.is_empty() && !remote_files.is_empty() {
            return Err(Error::conflict(
                "Direct Sync initialization requires one empty file side; both local and remote contain files",
            ));
        }
        let binding = state::Binding {
            root: local.root().to_owned(),
            root_identity: local.root_identity(),
            state_identity: local.state_identity(),
            namespace: namespace.clone(),
        };
        let store = Store::create(&state.join("direct-sync.db"), &binding)?;
        Ok(Self {
            remote,
            local,
            store,
            namespace,
        })
    }

    pub async fn open(remote: R, state: &Path) -> Result<Self> {
        let database = state.join("direct-sync.db");
        if !database.exists() {
            return Err(Error::invalid(format!(
                "Direct Sync state is not initialized at {}",
                state.display()
            )));
        }
        let binding = {
            let probe = Store::open_read_only(&database)?;
            probe.binding()?
        };
        let local = LocalTree::open(&binding.root, state)?;
        if local.root_identity() != binding.root_identity
            || local.state_identity() != binding.state_identity
        {
            return Err(Error::invalid(
                "Direct Sync root or state identity changed since initialization",
            ));
        }
        let namespace = remote.namespace_id();
        if namespace != binding.namespace {
            return Err(Error::invalid(format!(
                "Direct Sync state is bound to namespace {:?}, not {:?}",
                binding.namespace, namespace
            )));
        }
        let mut store = Store::open(&database)?;
        local.recover_installs(&mut store)?;
        Ok(Self {
            remote,
            local,
            store,
            namespace,
        })
    }

    pub fn root(&self) -> &Path {
        self.local.root()
    }

    pub fn state(&self) -> &Path {
        self.local.state()
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// Perform one finite reconciliation pass. Conflicts and safe partial
    /// outcomes are reported in the returned summary rather than retried
    /// indefinitely.
    pub async fn run(&mut self) -> Result<RunOutcome> {
        self.local.recover_installs(&mut self.store)?;

        let observed_remote = self.remote.observe().await?;
        let observed_local = self.local.scan()?;
        let pending_paths = self
            .store
            .work()?
            .into_iter()
            .map(|record| record.path)
            .chain(self.store.installs()?.into_iter().map(|record| record.path))
            .chain(self.store.baseline()?.into_keys())
            .collect::<BTreeSet<_>>();
        reject_unsupported_directories(&observed_local, &observed_remote, &pending_paths)?;
        self.backfill_conflicts(&observed_local, &observed_remote)
            .await?;

        for work in self.store.work()? {
            if matches!(work.state, WorkState::Prepared | WorkState::Unknown)
                && work_matches_local_input(&work, &observed_local)
            {
                let _ = self.dispatch_existing_work(work).await?;
            }
        }

        let remote_snapshot = self.remote.observe().await?;
        let local_snapshot = self.local.scan()?;
        reject_unsupported_directories(&local_snapshot, &remote_snapshot, &pending_paths)?;
        let baseline = self.store.baseline()?;
        let blocked = self.blocked_paths()?;
        let actions = plan_actions(&baseline, &local_snapshot, &remote_snapshot, &blocked)?;

        let mut outcome = RunOutcome::default();
        for action in actions {
            match action {
                Action::Publish {
                    path,
                    local,
                    baseline,
                } => match self.publish_local(&path, &local, baseline).await {
                    Ok(true) => outcome.published_local += 1,
                    Ok(false) => {}
                    Err(error) if error.kind() == ErrorKind::Conflict => {}
                    Err(error) => return Err(error),
                },
                Action::DeleteRemote { baseline } => match self.delete_remote(baseline).await {
                    Ok(true) => outcome.deleted_remote += 1,
                    Ok(false) => {}
                    Err(error) if error.kind() == ErrorKind::Conflict => {}
                    Err(error) => return Err(error),
                },
                Action::InstallFile {
                    remote,
                    expected_local,
                } => match self.install_remote(&remote, expected_local).await {
                    Ok(()) => outcome.materialized_remote += 1,
                    Err(error) if error.kind() == ErrorKind::Conflict => {}
                    Err(error) => return Err(error),
                },
                Action::DeleteLocal { baseline } => match self.delete_local(baseline).await {
                    Ok(()) => outcome.deleted_local += 1,
                    Err(error) if error.kind() == ErrorKind::Conflict => {}
                    Err(error) => return Err(error),
                },
                Action::Conflict {
                    path,
                    local,
                    remote,
                    message,
                } => {
                    self.record_conflict(
                        &path,
                        local.as_ref(),
                        remote.as_ref(),
                        None,
                        None,
                        &message,
                    )
                    .await?;
                }
                Action::RemoveBaseline { path } => {
                    self.store.remove_baseline(&path)?;
                    self.local.prune_parents_of(&path)?;
                }
            }
        }

        let final_local = self
            .local
            .scan()
            .map_err(|error| Error::invalid(format!("final Direct Sync scan failed: {error}")))?;
        let final_remote = self.remote.observe().await;
        let inspection = self.inspect_observed(final_remote, Ok(final_local))?;
        outcome.pending = inspection.summary.pending;
        outcome.unknown = inspection.summary.unknown;
        outcome.conflicts = inspection.summary.conflicts;
        outcome.in_sync = inspection.summary.in_sync;
        Ok(outcome)
    }

    /// Inspect durable state and the live remote without acquiring the writer
    /// lease. Construction errors are represented in the returned document.
    pub async fn inspect(
        remote: Option<&R>,
        state: &Path,
        remote_error: Option<String>,
    ) -> Inspection {
        let state_directory = state.to_owned();
        let database = state.join("direct-sync.db");
        let unbound = Inspection {
            binding: BindingEvidence {
                state: BindingState::Unbound,
                state_directory: state_directory.clone(),
                root: None,
                namespace: None,
                same_filesystem: None,
                error: None,
            },
            remote: RemoteStatus {
                availability: RemoteAvailability::NotChecked,
                revision: None,
                error: remote_error.clone(),
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
        if !database.exists() {
            return unbound;
        }
        if let Err(error) = local::validate_private_state(state) {
            return Inspection {
                binding: BindingEvidence {
                    state: BindingState::Error,
                    state_directory,
                    root: None,
                    namespace: None,
                    same_filesystem: None,
                    error: Some(error.to_string()),
                },
                ..unbound
            };
        }
        let store = match Store::open_read_only(&database) {
            Ok(store) => store,
            Err(error) => {
                return Inspection {
                    binding: BindingEvidence {
                        state: BindingState::Error,
                        state_directory,
                        root: None,
                        namespace: None,
                        same_filesystem: None,
                        error: Some(error.to_string()),
                    },
                    ..unbound
                };
            }
        };
        let binding = match store.binding() {
            Ok(binding) => binding,
            Err(error) => {
                return Inspection {
                    binding: BindingEvidence {
                        state: BindingState::Error,
                        state_directory,
                        root: None,
                        namespace: None,
                        same_filesystem: None,
                        error: Some(error.to_string()),
                    },
                    ..unbound
                };
            }
        };
        if let Some(remote) = remote
            && remote.namespace_id() != binding.namespace
        {
            return Inspection {
                binding: BindingEvidence {
                    state: BindingState::Error,
                    state_directory,
                    root: Some(binding.root),
                    namespace: Some(binding.namespace.clone()),
                    same_filesystem: Some(binding.root_identity.dev == binding.state_identity.dev),
                    error: Some(format!(
                        "configured storage namespace {} does not match the Direct Sync binding {}",
                        remote.namespace_id(),
                        binding.namespace
                    )),
                },
                ..unbound
            };
        }
        let local_scan = LocalTree::scan_read_only(&binding.root);
        let remote_observation = match remote {
            Some(remote) => Some(remote.observe().await),
            None => None,
        };
        Self::inspect_with_store(
            store,
            binding,
            state_directory,
            local_scan,
            remote_observation,
            remote_error,
        )
    }

    fn inspect_observed(
        &self,
        remote: Result<RemoteSnapshot>,
        local: Result<LocalSnapshot>,
    ) -> Result<Inspection> {
        let binding = state::Binding {
            root: self.local.root().to_owned(),
            root_identity: self.local.root_identity(),
            state_identity: self.local.state_identity(),
            namespace: self.namespace.clone(),
        };
        let inspection = Self::inspect_with_store_read(
            &self.store,
            binding,
            self.local.state().to_owned(),
            local,
            Some(remote),
            None,
        )?;
        Ok(inspection)
    }

    fn inspect_with_store(
        store: Store,
        binding: state::Binding,
        state_directory: PathBuf,
        local_scan: Result<LocalSnapshot>,
        remote_observation: Option<Result<RemoteSnapshot>>,
        remote_error: Option<String>,
    ) -> Inspection {
        Self::inspect_with_store_read(
            &store,
            binding,
            state_directory,
            local_scan,
            remote_observation,
            remote_error,
        )
        .unwrap_or_else(|error| Inspection {
            binding: BindingEvidence {
                state: BindingState::Error,
                state_directory: PathBuf::new(),
                root: None,
                namespace: None,
                same_filesystem: None,
                error: Some(error.to_string()),
            },
            remote: RemoteStatus {
                availability: RemoteAvailability::Unavailable,
                revision: None,
                error: Some(error.to_string()),
            },
            local: LocalStatus {
                scan: LocalScanState::Failed,
                error: Some(error.to_string()),
            },
            summary: StatusSummary::default(),
            entries: Vec::new(),
            conflicts: Vec::new(),
            artifacts: Vec::new(),
        })
    }

    fn inspect_with_store_read(
        store: &Store,
        binding: state::Binding,
        state_directory: PathBuf,
        local_scan: Result<LocalSnapshot>,
        remote_observation: Option<Result<RemoteSnapshot>>,
        remote_error: Option<String>,
    ) -> Result<Inspection> {
        let baseline = store.baseline()?;
        let work = store.work()?;
        let installs = store.installs()?;
        let conflicts = store.conflicts()?;
        let artifacts = store.artifacts()?;
        let same_filesystem = binding.root_identity.dev == binding.state_identity.dev;
        let binding_evidence = BindingEvidence {
            state: BindingState::Bound,
            state_directory,
            root: Some(binding.root.clone()),
            namespace: Some(binding.namespace.clone()),
            same_filesystem: Some(same_filesystem),
            error: None,
        };
        let (local_snapshot, local_status) = match local_scan {
            Ok(snapshot) => {
                let changed = local_changed(&baseline, &snapshot);
                (
                    Some(snapshot),
                    LocalStatus {
                        scan: if changed {
                            LocalScanState::Changed
                        } else {
                            LocalScanState::Clean
                        },
                        error: None,
                    },
                )
            }
            Err(error) => (
                None,
                LocalStatus {
                    scan: LocalScanState::Failed,
                    error: Some(error.to_string()),
                },
            ),
        };
        let (remote_snapshot, remote_status) = match remote_observation {
            Some(Ok(snapshot)) => (
                Some(snapshot.clone()),
                RemoteStatus {
                    availability: RemoteAvailability::Available,
                    revision: snapshot.revision,
                    error: None,
                },
            ),
            Some(Err(error)) => (
                None,
                RemoteStatus {
                    availability: RemoteAvailability::Unavailable,
                    revision: None,
                    error: Some(error.to_string()),
                },
            ),
            None => (
                None,
                RemoteStatus {
                    availability: RemoteAvailability::NotChecked,
                    revision: None,
                    error: remote_error,
                },
            ),
        };

        let local_map = local_snapshot.as_ref().map(all_local_entries);
        let remote_map = remote_snapshot.as_ref().map(all_remote_entries);
        let pending_paths = work
            .iter()
            .map(|record| record.path.clone())
            .chain(installs.iter().map(|record| record.path.clone()))
            .chain(baseline.keys().cloned())
            .collect::<BTreeSet<_>>();
        let mut unsupported_paths = BTreeSet::new();
        if let Some(local) = &local_snapshot {
            let files = local_files(local);
            unsupported_paths.extend(
                local
                    .nodes()
                    .values()
                    .filter(|node| {
                        node.is_directory()
                            && !node.path.is_empty()
                            && !has_descendant_file(&files, &node.path)
                            && !is_pending_parent(&node.path, &pending_paths)
                    })
                    .map(|node| node.path.clone()),
            );
        }
        if let Some(remote) = &remote_snapshot {
            let files = remote_files(remote);
            unsupported_paths.extend(
                remote
                    .entries
                    .values()
                    .filter(|entry| {
                        !entry.is_file()
                            && !entry.path.is_empty()
                            && !has_descendant_file(&files, &entry.path)
                            && !is_pending_parent(&entry.path, &pending_paths)
                    })
                    .map(|entry| entry.path.clone()),
            );
        }
        let mut paths = BTreeSet::new();
        paths.extend(baseline.keys().cloned());
        paths.extend(work.iter().map(|record| record.path.clone()));
        paths.extend(installs.iter().map(|record| record.path.clone()));
        paths.extend(conflicts.iter().map(|record| record.path.clone()));
        if let Some(map) = &local_map {
            paths.extend(
                map.values()
                    .filter(|node| matches!(node.kind, LocalKind::File { .. }))
                    .map(|node| node.path.clone()),
            );
        }
        if let Some(map) = &remote_map {
            paths.extend(
                map.values()
                    .filter(|entry| entry.is_file())
                    .map(|entry| entry.path.clone()),
            );
        }
        paths.extend(unsupported_paths.iter().cloned());
        paths.remove("");

        let conflict_by_path: BTreeMap<&str, &ConflictRecord> = conflicts
            .iter()
            .map(|conflict| (conflict.path.as_str(), conflict))
            .collect();
        let work_by_path: BTreeMap<&str, &WorkRecord> = work
            .iter()
            .map(|record| (record.path.as_str(), record))
            .collect();
        let install_by_path: BTreeMap<&str, &InstallRecord> = installs
            .iter()
            .map(|record| (record.path.as_str(), record))
            .collect();

        let mut entries = Vec::with_capacity(paths.len());
        let mut pending = 0;
        for path in paths {
            let local_node = local_map.as_ref().and_then(|map| map.get(&path));
            let remote_entry = remote_map.as_ref().and_then(|map| map.get(&path));
            let baseline_entry = baseline.get(&path);
            let local_change = local_path_changed(baseline_entry, local_node);
            let remote_change = remote_map
                .as_ref()
                .is_some_and(|_| remote_path_changed(baseline_entry, remote_entry));
            let conflict = conflict_by_path.get(path.as_str()).copied();
            let work = work_by_path.get(path.as_str()).copied();
            let install = install_by_path.get(path.as_str()).copied();
            let unsupported = unsupported_paths.contains(&path);
            let state = if unsupported {
                EntryState::Failed
            } else if conflict.is_some() {
                EntryState::Conflict
            } else if work.is_some_and(|record| record.state == WorkState::Unknown) {
                EntryState::Unknown
            } else if work.is_some_and(|record| record.state == WorkState::Failed)
                || install.is_some_and(|record| record.state == InstallState::Failed)
            {
                EntryState::Failed
            } else if install.is_some_and(|record| record.state != InstallState::Conflict) {
                pending += 1;
                EntryState::RemotePending
            } else if work.is_some_and(|record| record.state == WorkState::Prepared)
                || install.is_some_and(|record| record.state == InstallState::Conflict)
            {
                pending += 1;
                EntryState::LocalPending
            } else if local_change && remote_change {
                EntryState::Conflict
            } else if local_change {
                pending += 1;
                EntryState::LocalPending
            } else if remote_change {
                pending += 1;
                EntryState::RemotePending
            } else {
                EntryState::InSync
            };
            entries.push(EntryEvidence {
                path: path.clone(),
                state,
                local: local_evidence(local_node),
                remote: remote_evidence(remote_entry),
                baseline: BaselineEvidence {
                    local_digest: baseline_entry.map(|entry| entry.local.digest),
                    local_length: baseline_entry.map(|entry| entry.local.length),
                    remote: baseline_entry.map(|entry| entry.remote.clone()),
                },
                work: WorkEvidence {
                    id: work.map(|record| hex(&record.id)),
                    operation: work.map(|record| record.operation),
                    state: work.map(|record| record.state),
                    install_state: install.map(|record| record.state),
                    error: if unsupported {
                        Some(
                            "empty directories are not representable in the Direct file namespace"
                                .to_owned(),
                        )
                    } else {
                        work.and_then(|record| record.error.clone())
                            .or_else(|| install.and_then(|record| record.error.clone()))
                    },
                },
                conflict: conflict.map(|record| record.id.clone()),
            });
        }

        let unknown = work
            .iter()
            .filter(|record| record.state == WorkState::Unknown)
            .count() as u64;
        let failed = work
            .iter()
            .filter(|record| record.state == WorkState::Failed)
            .count() as u64
            + installs
                .iter()
                .filter(|record| record.state == InstallState::Failed)
                .count() as u64;
        let failed = failed + unsupported_paths.len() as u64;
        let in_sync = remote_status.availability == RemoteAvailability::Available
            && local_status.scan == LocalScanState::Clean
            && pending == 0
            && unknown == 0
            && failed == 0
            && conflicts.is_empty()
            && entries
                .iter()
                .all(|entry| entry.state == EntryState::InSync);
        Ok(Inspection {
            binding: binding_evidence,
            remote: remote_status,
            local: local_status,
            summary: StatusSummary {
                in_sync,
                pending,
                unknown,
                conflicts: conflicts.len(),
                failed,
                retained_artifacts: artifacts.len() as u64,
            },
            entries,
            conflicts,
            artifacts,
        })
    }

    pub fn export_artifact(&self, id: &str, output: &mut dyn Write) -> Result<()> {
        let artifact = self
            .store
            .artifacts()?
            .into_iter()
            .find(|artifact| artifact.id == id)
            .ok_or_else(|| Error::invalid("unknown Direct Sync artifact"))?;
        let mut file = self.local.open_artifact(&artifact.name)?;
        std::io::copy(&mut file, output)
            .map(|_| ())
            .map_err(|error| Error::io("export Direct Sync artifact", &self.local.state(), error))
    }

    /// Export one retained artifact without constructing an operator, acquiring
    /// the writer lease or contacting storage.
    pub fn export_artifact_local(state: &Path, id: &str, output: &mut dyn Write) -> Result<()> {
        local::validate_private_state(state)?;
        let store = Store::open_read_only(&state.join("direct-sync.db"))?;
        let artifact = store
            .artifacts()?
            .into_iter()
            .find(|artifact| artifact.id == id)
            .ok_or_else(|| Error::invalid("unknown Direct Sync artifact"))?;
        let path = state.join("artifacts").join(&artifact.name);
        let fd = rustix::fs::open(
            &path,
            rustix::fs::OFlags::RDONLY | rustix::fs::OFlags::NOFOLLOW | rustix::fs::OFlags::CLOEXEC,
            rustix::fs::Mode::empty(),
        )
        .map_err(|error| Error::errno("open Direct Sync artifact", &path, error))?;
        let mut file = std::fs::File::from(fd);
        std::io::copy(&mut file, output)
            .map(|_| ())
            .map_err(|error| Error::io("export Direct Sync artifact", &path, error))
    }

    pub fn export_conflict_side_local(
        state: &Path,
        conflict_id: &str,
        side: &str,
        output: &mut dyn Write,
    ) -> Result<()> {
        let store = Store::open_read_only(&state.join("direct-sync.db"))?;
        let conflict = store
            .conflict(conflict_id)?
            .ok_or_else(|| Error::invalid("unknown Direct Sync conflict"))?;
        let artifact = match side {
            "local" => conflict.local_artifact,
            "remote" => conflict.remote_artifact,
            other => {
                return Err(Error::invalid(format!(
                    "unknown Direct Sync conflict side {other:?}"
                )));
            }
        }
        .ok_or_else(|| Error::invalid(format!("Direct Sync conflict has no {side} artifact")))?;
        Self::export_artifact_local(state, &artifact, output)
    }

    pub fn export_conflict_side(
        &self,
        conflict_id: &str,
        side: &str,
        output: &mut dyn Write,
    ) -> Result<()> {
        let conflict = self
            .store
            .conflict(conflict_id)?
            .ok_or_else(|| Error::invalid("unknown Direct Sync conflict"))?;
        match side {
            "local" => {
                if let Some(id) = conflict.local_artifact {
                    return self.export_artifact(&id, output);
                }
                match conflict.local.presence {
                    ConflictPresence::Present => {
                        let mut file = self.local.open_live_file(&conflict.path)?;
                        std::io::copy(&mut file, output)
                            .map(|_| ())
                            .map_err(|error| {
                                Error::io(
                                    "export live Direct Sync local conflict",
                                    &self.local.root().join(&conflict.path),
                                    error,
                                )
                            })
                    }
                    ConflictPresence::Absent => Err(Error::invalid(
                        "this Direct Sync conflict has no local bytes",
                    )),
                }
            }
            "remote" => {
                let id = conflict.remote_artifact.ok_or_else(|| {
                    Error::invalid("this Direct Sync conflict has no retained remote bytes")
                })?;
                self.export_artifact(&id, output)
            }
            other => Err(Error::invalid(format!(
                "unknown Direct Sync conflict side {other:?}"
            ))),
        }
    }

    pub async fn resolve(&mut self, id: &str, resolution: Resolution) -> Result<RunOutcome> {
        let remote_snapshot = self.remote.observe().await?;
        if let Some(conflict) = self.store.conflict(id)? {
            self.resolve_conflict(conflict, &remote_snapshot, resolution)
                .await?;
            return self
                .inspect_current_outcome(self.remote.observe().await?)
                .await;
        }
        if let Some(work) = self
            .store
            .work()?
            .into_iter()
            .find(|record| hex(&record.id) == id)
        {
            self.resolve_work(work, &remote_snapshot, resolution)
                .await?;
            return self
                .inspect_current_outcome(self.remote.observe().await?)
                .await;
        }
        Err(Error::invalid(format!(
            "Direct Sync status has no conflict or work identity {id:?}"
        )))
    }

    async fn inspect_current_outcome(&self, remote_snapshot: RemoteSnapshot) -> Result<RunOutcome> {
        let local = self.local.scan()?;
        let inspection = Self::inspect_with_store_read(
            &self.store,
            state::Binding {
                root: self.local.root().to_owned(),
                root_identity: self.local.root_identity(),
                state_identity: self.local.state_identity(),
                namespace: self.namespace.clone(),
            },
            self.local.state().to_owned(),
            Ok(local),
            Some(Ok(remote_snapshot)),
            None,
        )?;
        Ok(RunOutcome {
            pending: inspection.summary.pending,
            unknown: inspection.summary.unknown,
            conflicts: inspection.summary.conflicts,
            in_sync: inspection.summary.in_sync,
            ..RunOutcome::default()
        })
    }

    fn blocked_paths(&self) -> Result<BTreeSet<String>> {
        let mut blocked = BTreeSet::new();
        blocked.extend(self.store.work()?.into_iter().map(|record| record.path));
        blocked.extend(self.store.installs()?.into_iter().map(|record| record.path));
        blocked.extend(
            self.store
                .conflicts()?
                .into_iter()
                .map(|record| record.path),
        );
        Ok(blocked)
    }

    async fn backfill_conflicts(
        &mut self,
        local: &LocalSnapshot,
        remote: &RemoteSnapshot,
    ) -> Result<()> {
        let existing = self
            .store
            .conflicts()?
            .into_iter()
            .map(|conflict| conflict.path)
            .collect::<BTreeSet<_>>();
        let mut missing = BTreeMap::new();
        for work in self
            .store
            .work()?
            .into_iter()
            .filter(|work| work.state == WorkState::Conflict)
        {
            missing.entry(work.path).or_insert_with(|| {
                work.error
                    .unwrap_or_else(|| "retained Direct Sync work is in conflict".to_owned())
            });
        }
        for install in self
            .store
            .installs()?
            .into_iter()
            .filter(|install| install.state == InstallState::Conflict)
        {
            missing.entry(install.path).or_insert_with(|| {
                install
                    .error
                    .unwrap_or_else(|| "retained Direct Sync install is in conflict".to_owned())
            });
        }
        for (path, error) in missing {
            if existing.contains(&path) {
                continue;
            }
            let local_node = local.get(&path);
            let remote_entry = remote.entries.get(&path);
            self.record_conflict(&path, local_node, remote_entry, None, None, &error)
                .await?;
        }
        Ok(())
    }

    async fn dispatch_existing_work(&mut self, work: WorkRecord) -> Result<bool> {
        let was_unknown = work.state == WorkState::Unknown;
        self.store
            .update_work_state(work.id, WorkState::Unknown, work.error.as_deref())?;
        let precondition = match work.operation {
            RemoteOperation::Publish if work.baseline_remote.is_none() => {
                RemotePrecondition::Absent
            }
            RemoteOperation::Publish | RemoteOperation::Delete => {
                let pin = work
                    .baseline_remote
                    .as_ref()
                    .ok_or_else(|| Error::corrupt("Direct Sync delete has no remote baseline"))?;
                RemotePrecondition::Etag(
                    pin.cas_etag()
                        .ok_or_else(|| {
                            Error::unsupported(format!(
                                "object {:?} has no ETag for conditional mutation",
                                work.path
                            ))
                        })?
                        .to_owned(),
                )
            }
        };
        let outcome = match work.operation {
            RemoteOperation::Publish => {
                let artifact_id = work.desired_artifact.as_deref().ok_or_else(|| {
                    Error::corrupt("Direct Sync publish work has no retained input")
                })?;
                let artifact = self
                    .store
                    .artifacts()?
                    .into_iter()
                    .find(|artifact| artifact.id == artifact_id)
                    .ok_or_else(|| Error::corrupt("Direct Sync upload artifact disappeared"))?;
                let path = self.local.artifact_path(&artifact);
                self.remote
                    .publish_file(&work.path, &path, &precondition)
                    .await?
            }
            RemoteOperation::Delete => self.remote.delete(&work.path, &precondition).await?,
        };
        self.apply_remote_outcome(work, was_unknown, outcome).await
    }

    async fn apply_remote_outcome(
        &mut self,
        work: WorkRecord,
        was_unknown: bool,
        outcome: MutationOutcome,
    ) -> Result<bool> {
        let mut applied = false;
        match outcome {
            MutationOutcome::Applied { accepted } => match work.operation {
                RemoteOperation::Publish => {
                    let accepted = accepted.ok_or_else(|| {
                        Error::unsupported(
                            "conditional write returned no accepted version or ETag; Direct Sync cannot baseline it",
                        )
                    })?;
                    let local = work.desired_local.ok_or_else(|| {
                        Error::corrupt("Direct Sync publish work has no accepted local fingerprint")
                    })?;
                    self.store.complete_work(
                        work.id,
                        Some(&BaselineEntry {
                            path: work.path,
                            remote: accepted,
                            local,
                        }),
                    )?;
                    applied = true;
                }
                RemoteOperation::Delete => {
                    self.store.complete_work(work.id, None)?;
                    applied = true;
                }
            },
            MutationOutcome::Conflict => {
                if was_unknown {
                    self.store.update_work_state(
                        work.id,
                        WorkState::Unknown,
                        Some("the original request outcome remains unknown; its accepted precondition no longer matches"),
                    )?;
                } else {
                    let remote_snapshot = self.remote.observe().await?;
                    let local_snapshot = self.local.scan()?;
                    let local = local_file(&local_snapshot, &work.path).cloned();
                    let remote = remote_files(&remote_snapshot).get(&work.path).cloned();
                    self.record_conflict(
                        &work.path,
                        local.as_ref(),
                        remote.as_ref(),
                        None,
                        None,
                        "remote replacement or deletion conflicted with the accepted baseline",
                    )
                    .await?;
                    self.store.update_work_state(
                        work.id,
                        WorkState::Conflict,
                        Some("the accepted remote precondition no longer matches"),
                    )?;
                }
            }
            MutationOutcome::Unknown { error } => {
                self.store
                    .update_work_state(work.id, WorkState::Unknown, Some(&error))?;
            }
            MutationOutcome::Unsupported { message } => {
                self.store
                    .update_work_state(work.id, WorkState::Failed, Some(&message))?;
            }
        }
        Ok(applied)
    }

    async fn publish_local(
        &mut self,
        path: &str,
        node: &LocalNode,
        baseline_remote: Option<RemotePin>,
    ) -> Result<bool> {
        let artifact = self.local.capture_local(node, ArtifactKind::Upload)?;
        self.store.insert_artifact(&artifact)?;
        let local = LocalFingerprint::from_node(node)?;
        let operation = if baseline_remote.is_some() {
            RemoteOperation::Publish
        } else {
            RemoteOperation::Publish
        };
        let id = work_identity(
            operation,
            path,
            baseline_remote.as_ref(),
            Some(local.digest),
        );
        let work = WorkRecord {
            id,
            path: path.to_owned(),
            operation,
            state: WorkState::Prepared,
            baseline_remote,
            baseline_local: Some(local),
            desired_local: Some(local),
            desired_artifact: Some(artifact.id),
            error: None,
        };
        self.store.insert_work(&work)?;
        self.store
            .update_work_state(work.id, WorkState::Unknown, None)?;
        self.dispatch_existing_work(work).await
    }

    async fn delete_remote(&mut self, baseline: BaselineEntry) -> Result<bool> {
        let id = work_identity(
            RemoteOperation::Delete,
            &baseline.path,
            Some(&baseline.remote),
            None,
        );
        let work = WorkRecord {
            id,
            path: baseline.path.clone(),
            operation: RemoteOperation::Delete,
            state: WorkState::Prepared,
            baseline_remote: Some(baseline.remote),
            baseline_local: Some(baseline.local),
            desired_local: None,
            desired_artifact: None,
            error: None,
        };
        self.store.insert_work(&work)?;
        self.store
            .update_work_state(work.id, WorkState::Unknown, None)?;
        self.dispatch_existing_work(work).await
    }

    async fn install_remote(
        &mut self,
        entry: &RemoteEntry,
        expected_local: Option<LocalFingerprint>,
    ) -> Result<()> {
        let pin = entry
            .pin
            .clone()
            .ok_or_else(|| Error::unsupported("Direct Sync remote file has no pin"))?;
        let (temporary, path, file) = self.local.create_install_file("download")?;
        drop(file);
        let mut install = InstallRecord {
            id: uuid::Uuid::new_v4().to_string(),
            path: entry.path.clone(),
            operation: if expected_local.is_some() {
                InstallOperation::ReplaceFile
            } else {
                InstallOperation::CreateFile
            },
            state: InstallState::Prepared,
            expected_local,
            desired_local: None,
            desired_remote: Some(pin.clone()),
            temporary,
            artifact: None,
            error: None,
        };
        self.store.insert_install(&install)?;
        if let Err(error) = self.remote.read_to_path(entry, &path).await {
            let _ = std::fs::remove_file(&path);
            install.state = InstallState::Conflict;
            install.error = Some(error.to_string());
            self.store.update_install(&install)?;
            return Err(error);
        }
        let desired = self.local.install_desired_file(&install.temporary)?;
        install.desired_local = Some(desired);
        self.store.update_install(&install)?;
        if let Err(error) = self.local.apply_install(&mut self.store, &mut install) {
            if error.kind() == ErrorKind::Conflict {
                let remote_artifact = self
                    .retain_temporary_conflict(&install.temporary, &install.path)
                    .ok();
                if let Some(artifact) = &remote_artifact {
                    self.store.insert_artifact(artifact)?;
                }
                let local_snapshot = self.local.scan()?;
                let local = local_file(&local_snapshot, &install.path).cloned();
                self.record_conflict(
                    &install.path,
                    local.as_ref(),
                    Some(entry),
                    None,
                    remote_artifact.map(|artifact| artifact.id),
                    &error.to_string(),
                )
                .await?;
            }
            return Err(error);
        }
        Ok(())
    }

    async fn delete_local(&mut self, baseline: BaselineEntry) -> Result<()> {
        let pin = baseline.remote.clone();
        let mut install = InstallRecord {
            id: uuid::Uuid::new_v4().to_string(),
            path: baseline.path.clone(),
            operation: InstallOperation::DeleteFile,
            state: InstallState::Prepared,
            expected_local: Some(baseline.local),
            desired_local: None,
            desired_remote: Some(pin),
            temporary: format!("delete-{}", uuid::Uuid::new_v4().simple()),
            artifact: None,
            error: None,
        };
        self.store.insert_install(&install)?;
        if let Err(error) = self.local.apply_install(&mut self.store, &mut install) {
            if error.kind() == ErrorKind::Conflict {
                let local_snapshot = self.local.scan()?;
                let local = local_file(&local_snapshot, &install.path).cloned();
                self.record_conflict(
                    &install.path,
                    local.as_ref(),
                    None,
                    None,
                    None,
                    &error.to_string(),
                )
                .await?;
            }
            return Err(error);
        }
        Ok(())
    }

    async fn record_conflict(
        &mut self,
        path: &str,
        local: Option<&LocalNode>,
        remote: Option<&RemoteEntry>,
        local_artifact: Option<String>,
        remote_artifact: Option<String>,
        error: &str,
    ) -> Result<()> {
        let local_side = conflict_local_side(local);
        let remote_side = conflict_remote_side(remote);
        let id = conflict_identity(path, &local_side, &remote_side);
        let local_artifact = match local_artifact {
            Some(id) => Some(id),
            None => match local.filter(|node| matches!(node.kind, LocalKind::File { .. })) {
                Some(node) => match self.local.capture_local(node, ArtifactKind::LocalConflict) {
                    Ok(artifact) => {
                        self.store.insert_artifact(&artifact)?;
                        Some(artifact.id)
                    }
                    Err(_) => None,
                },
                None => None,
            },
        };
        let remote_artifact = match remote_artifact {
            Some(id) => Some(id),
            None => match remote.filter(|entry| entry.is_file()) {
                Some(entry) => {
                    match Self::capture_remote_artifact_with(&self.local, &self.remote, entry).await
                    {
                        Ok(artifact) => {
                            self.store.insert_artifact(&artifact)?;
                            Some(artifact.id)
                        }
                        Err(_) => None,
                    }
                }
                None => None,
            },
        };
        self.store.upsert_conflict(&ConflictRecord {
            id,
            path: path.to_owned(),
            local: local_side,
            remote: remote_side,
            local_artifact,
            remote_artifact,
            error: error.to_owned(),
        })
    }

    async fn capture_remote_artifact_with(
        local: &LocalTree,
        remote: &R,
        entry: &RemoteEntry,
    ) -> Result<ArtifactRecord> {
        let (temporary, path, file) = local.create_install_file("conflict")?;
        drop(file);
        match remote.read_to_path(entry, &path).await {
            Ok(()) => local.retain_install_temporary(
                &temporary,
                &entry.path,
                ArtifactKind::RemoteConflict,
            ),
            Err(error) => {
                let _ = std::fs::remove_file(&path);
                Err(error)
            }
        }
    }

    fn retain_temporary_conflict(&self, temporary: &str, path: &str) -> Result<ArtifactRecord> {
        self.local
            .retain_install_temporary(temporary, path, ArtifactKind::RemoteConflict)
    }

    async fn resolve_conflict(
        &mut self,
        conflict: ConflictRecord,
        snapshot: &RemoteSnapshot,
        resolution: Resolution,
    ) -> Result<()> {
        match resolution {
            Resolution::KeepRemote => {
                let remote = snapshot.entries.get(&conflict.path).cloned();
                let current = self.local.scan()?;
                if current
                    .get(&conflict.path)
                    .is_some_and(|node| matches!(node.kind, LocalKind::Directory))
                {
                    return Err(Error::conflict(
                        "local path is a directory; keep-remote cannot replace it with a file",
                    ));
                }
                let current_local = local_file(&current, &conflict.path).cloned();
                if let Some(entry) = remote.as_ref().filter(|entry| entry.is_file()) {
                    let expected_local = current_local
                        .as_ref()
                        .map(LocalFingerprint::from_node)
                        .transpose()?;
                    self.install_remote(entry, expected_local).await?;
                } else if remote.is_none() {
                    if let Some(node) = current_local {
                        let local = LocalFingerprint::from_node(&node)?;
                        let mut install = InstallRecord {
                            id: uuid::Uuid::new_v4().to_string(),
                            path: conflict.path.clone(),
                            operation: InstallOperation::DeleteFile,
                            state: InstallState::Prepared,
                            expected_local: Some(local),
                            desired_local: None,
                            desired_remote: None,
                            temporary: format!("delete-{}", uuid::Uuid::new_v4().simple()),
                            artifact: None,
                            error: None,
                        };
                        self.store.insert_install(&install)?;
                        self.local.apply_install(&mut self.store, &mut install)?;
                    } else {
                        self.store.remove_baseline(&conflict.path)?;
                    }
                } else {
                    return Err(Error::conflict(
                        "remote path is now a directory; keep-remote cannot materialize it",
                    ));
                }
                self.store.remove_conflict(&conflict.id)?;
            }
            Resolution::KeepLocal => {
                let local_node = self.local_fingerprint_node(&conflict.path)?;
                let remote = snapshot.entries.get(&conflict.path).cloned();
                let baseline_remote = remote.as_ref().and_then(|entry| entry.pin.clone());
                let mut work = match local_node {
                    Some(node) => {
                        if remote.as_ref().is_some_and(|entry| !entry.is_file()) {
                            return Err(Error::conflict(
                                "remote path is a directory; keep-local cannot replace it with a file",
                            ));
                        }
                        let artifact = self.local.capture_local(&node, ArtifactKind::Resolution)?;
                        self.store.insert_artifact(&artifact)?;
                        let local = LocalFingerprint::from_node(&node)?;
                        WorkRecord {
                            id: work_identity(
                                RemoteOperation::Publish,
                                &conflict.path,
                                baseline_remote.as_ref(),
                                Some(local.digest),
                            ),
                            path: conflict.path.clone(),
                            operation: RemoteOperation::Publish,
                            state: WorkState::Prepared,
                            baseline_remote,
                            baseline_local: None,
                            desired_local: Some(local),
                            desired_artifact: Some(artifact.id),
                            error: None,
                        }
                    }
                    None if remote.as_ref().is_some_and(RemoteEntry::is_file) => WorkRecord {
                        id: work_identity(
                            RemoteOperation::Delete,
                            &conflict.path,
                            baseline_remote.as_ref(),
                            None,
                        ),
                        path: conflict.path.clone(),
                        operation: RemoteOperation::Delete,
                        state: WorkState::Prepared,
                        baseline_remote,
                        baseline_local: None,
                        desired_local: None,
                        desired_artifact: None,
                        error: None,
                    },
                    None if remote.is_some() => {
                        return Err(Error::conflict(
                            "remote path is a directory; keep-local cannot delete it",
                        ));
                    }
                    None => {
                        self.store.remove_baseline(&conflict.path)?;
                        self.store.remove_conflict(&conflict.id)?;
                        return Ok(());
                    }
                };
                if work.operation == RemoteOperation::Publish && work.desired_artifact.is_none() {
                    return Err(Error::conflict(
                        "current local file could not be retained for keep-local resolution",
                    ));
                }
                self.store.insert_work(&work)?;
                self.store
                    .update_work_state(work.id, WorkState::Unknown, None)?;
                work.state = WorkState::Unknown;
                let _ = self.dispatch_existing_work(work).await?;
                if !self
                    .store
                    .work()?
                    .iter()
                    .any(|record| record.path == conflict.path)
                {
                    self.store.remove_conflict(&conflict.id)?;
                }
            }
        }
        Ok(())
    }

    async fn resolve_work(
        &mut self,
        work: WorkRecord,
        snapshot: &RemoteSnapshot,
        resolution: Resolution,
    ) -> Result<()> {
        match resolution {
            Resolution::KeepLocal => {
                let current = self.local.scan()?;
                if current
                    .get(&work.path)
                    .is_some_and(|node| matches!(node.kind, LocalKind::Directory))
                {
                    return Err(Error::conflict(
                        "local path is a directory; keep-local cannot publish it as a file",
                    ));
                }
                let current_local = local_file(&current, &work.path).cloned();
                let remote = snapshot
                    .entries
                    .get(&work.path)
                    .and_then(|entry| entry.pin.clone());
                let mut replacement = match current_local {
                    Some(node) => {
                        let artifact = self.local.capture_local(&node, ArtifactKind::Resolution)?;
                        self.store.insert_artifact(&artifact)?;
                        let local = LocalFingerprint::from_node(&node)?;
                        WorkRecord {
                            id: work_identity(
                                RemoteOperation::Publish,
                                &work.path,
                                remote.as_ref(),
                                Some(local.digest),
                            ),
                            path: work.path.clone(),
                            operation: RemoteOperation::Publish,
                            state: WorkState::Prepared,
                            baseline_remote: remote,
                            baseline_local: None,
                            desired_local: Some(local),
                            desired_artifact: Some(artifact.id),
                            error: None,
                        }
                    }
                    None if remote.is_some() => WorkRecord {
                        id: work_identity(
                            RemoteOperation::Delete,
                            &work.path,
                            remote.as_ref(),
                            None,
                        ),
                        path: work.path.clone(),
                        operation: RemoteOperation::Delete,
                        state: WorkState::Prepared,
                        baseline_remote: remote,
                        baseline_local: None,
                        desired_local: None,
                        desired_artifact: None,
                        error: None,
                    },
                    None => {
                        self.store.remove_work(work.id)?;
                        self.store.remove_baseline(&work.path)?;
                        return Ok(());
                    }
                };
                replacement.state = WorkState::Prepared;
                replacement.error = None;
                self.store.remove_work(work.id)?;
                self.store.insert_work(&replacement)?;
                self.store
                    .update_work_state(replacement.id, WorkState::Unknown, None)?;
                let _ = self.dispatch_existing_work(replacement).await?;
            }
            Resolution::KeepRemote => {
                let remote = snapshot.entries.get(&work.path).cloned();
                let current = self.local.scan()?;
                if current
                    .get(&work.path)
                    .is_some_and(|node| matches!(node.kind, LocalKind::Directory))
                {
                    return Err(Error::conflict(
                        "local path is a directory; keep-remote cannot replace it with a file",
                    ));
                }
                let local = local_file(&current, &work.path).cloned();
                if let Some(entry) = remote.as_ref().filter(|entry| entry.is_file()) {
                    let expected_local = local
                        .as_ref()
                        .map(LocalFingerprint::from_node)
                        .transpose()?;
                    self.install_remote(entry, expected_local).await?;
                } else if remote.is_none() {
                    if let Some(node) = local {
                        let local = LocalFingerprint::from_node(&node)?;
                        let mut install = InstallRecord {
                            id: uuid::Uuid::new_v4().to_string(),
                            path: work.path.clone(),
                            operation: InstallOperation::DeleteFile,
                            state: InstallState::Prepared,
                            expected_local: Some(local),
                            desired_local: None,
                            desired_remote: None,
                            temporary: format!("delete-{}", uuid::Uuid::new_v4().simple()),
                            artifact: None,
                            error: None,
                        };
                        self.store.insert_install(&install)?;
                        self.local.apply_install(&mut self.store, &mut install)?;
                    } else {
                        self.store.remove_baseline(&work.path)?;
                    }
                } else {
                    return Err(Error::conflict(
                        "remote path is now a directory; keep-remote cannot materialize it",
                    ));
                }
                self.store.remove_work(work.id)?;
            }
        }
        Ok(())
    }

    fn local_fingerprint_node(&self, path: &str) -> Result<Option<LocalNode>> {
        let snapshot = self.local.scan()?;
        Ok(local_file(&snapshot, path).cloned())
    }
}

enum Action {
    Publish {
        path: String,
        local: LocalNode,
        baseline: Option<RemotePin>,
    },
    DeleteRemote {
        baseline: BaselineEntry,
    },
    InstallFile {
        remote: RemoteEntry,
        expected_local: Option<LocalFingerprint>,
    },
    DeleteLocal {
        baseline: BaselineEntry,
    },
    Conflict {
        path: String,
        local: Option<LocalNode>,
        remote: Option<RemoteEntry>,
        message: String,
    },
    RemoveBaseline {
        path: String,
    },
}

fn plan_actions(
    baseline: &BTreeMap<String, BaselineEntry>,
    local: &LocalSnapshot,
    remote: &RemoteSnapshot,
    blocked: &BTreeSet<String>,
) -> Result<Vec<Action>> {
    let local_entries = all_local_entries(local);
    let remote_entries = all_remote_entries(remote);
    let mut blocked_subtrees = blocked.clone();
    let mut paths = BTreeSet::new();
    paths.extend(baseline.keys().cloned());
    paths.extend(
        local_entries
            .values()
            .filter(|node| matches!(node.kind, LocalKind::File { .. }))
            .map(|node| node.path.clone()),
    );
    paths.extend(
        remote_entries
            .values()
            .filter(|entry| entry.is_file())
            .map(|entry| entry.path.clone()),
    );
    paths.remove("");

    let mut publish = Vec::new();
    let mut install = Vec::new();
    let mut delete_remote = Vec::new();
    let mut delete_local = Vec::new();
    let mut remove_baseline = Vec::new();
    let mut conflicts = Vec::new();
    for path in paths {
        if has_path_or_parent(&path, &blocked_subtrees) {
            continue;
        }
        let baseline_entry = baseline.get(&path);
        let local_node = local_entries.get(&path);
        let remote_entry = remote_entries.get(&path);
        let local_directory =
            local_node.is_some_and(|node| matches!(node.kind, LocalKind::Directory));
        let local_file = local_node.is_some_and(|node| matches!(node.kind, LocalKind::File { .. }));
        let remote_directory = remote_entry.is_some_and(|entry| !entry.is_file());
        let remote_file = remote_entry.is_some_and(RemoteEntry::is_file);
        if (local_directory && remote_file)
            || (local_file && remote_directory)
            || (baseline_entry.is_some() && (local_directory || remote_directory))
        {
            conflicts.push(Action::Conflict {
                path: path.clone(),
                local: local_node.cloned(),
                remote: remote_entry.cloned(),
                message: "file and directory kinds cannot be reconciled automatically".to_owned(),
            });
            blocked_subtrees.insert(path);
            continue;
        }
        let local_change = local_path_changed(baseline_entry, local_node);
        let remote_change = remote_path_changed(baseline_entry, remote_entry);

        if baseline_entry.is_some() && local_node.is_none() && remote_entry.is_none() {
            remove_baseline.push(Action::RemoveBaseline { path });
            continue;
        }

        if !local_change && !remote_change {
            continue;
        }
        if local_change && remote_change {
            conflicts.push(Action::Conflict {
                path: path.clone(),
                local: local_node.cloned(),
                remote: remote_entry.cloned(),
                message: "local and remote changes cannot be ordered safely".to_owned(),
            });
            continue;
        }
        if local_change {
            match local_node {
                Some(node) if matches!(node.kind, LocalKind::File { .. }) => {
                    publish.push(Action::Publish {
                        path: path.clone(),
                        local: node.clone(),
                        baseline: baseline_entry.map(|entry| entry.remote.clone()),
                    });
                }
                None if baseline_entry.is_some() => {
                    delete_remote.push(Action::DeleteRemote {
                        baseline: baseline_entry.expect("baseline present").clone(),
                    });
                }
                Some(_) | None => {}
            }
            continue;
        }
        match remote_entry {
            Some(entry) if entry.is_file() => install.push(Action::InstallFile {
                remote: entry.clone(),
                expected_local: baseline_entry.map(|entry| entry.local),
            }),
            None if baseline_entry.is_some() => delete_local.push(Action::DeleteLocal {
                baseline: baseline_entry.expect("baseline present").clone(),
            }),
            Some(_) | None => {}
        }
    }
    let mut actions = Vec::new();
    actions.extend(publish);
    actions.extend(install);
    actions.extend(delete_remote);
    actions.extend(delete_local);
    actions.extend(remove_baseline);
    actions.extend(conflicts);
    Ok(actions)
}

fn local_changed(baseline: &BTreeMap<String, BaselineEntry>, local: &LocalSnapshot) -> bool {
    let local = all_local_entries(local);
    let mut paths = BTreeSet::new();
    paths.extend(baseline.keys().cloned());
    paths.extend(
        local
            .values()
            .filter(|node| matches!(node.kind, LocalKind::File { .. }))
            .map(|node| node.path.clone()),
    );
    paths
        .into_iter()
        .filter(|path| !path.is_empty())
        .any(|path| local_path_changed(baseline.get(&path), local.get(&path)))
}

fn local_path_changed(baseline: Option<&BaselineEntry>, local: Option<&LocalNode>) -> bool {
    match (baseline, local) {
        (Some(baseline), Some(node)) => match node.kind {
            LocalKind::File { length, digest } => {
                !baseline.local.same_content(length, *digest.as_bytes())
            }
            LocalKind::Directory => true,
        },
        (Some(_), None) => true,
        (None, Some(node)) => matches!(node.kind, LocalKind::File { .. }),
        (None, None) => false,
    }
}

fn work_matches_local_input(work: &WorkRecord, local: &LocalSnapshot) -> bool {
    match work.operation {
        RemoteOperation::Publish => {
            let Some(desired) = work.desired_local else {
                return false;
            };
            let Some(node) = local_file(local, &work.path) else {
                return false;
            };
            match node.kind {
                LocalKind::File { length, digest } => {
                    desired.length == length && desired.digest == *digest.as_bytes()
                }
                LocalKind::Directory => false,
            }
        }
        RemoteOperation::Delete => local.get(&work.path).is_none(),
    }
}

fn remote_path_changed(baseline: Option<&BaselineEntry>, remote: Option<&RemoteEntry>) -> bool {
    match (baseline, remote) {
        (Some(baseline), Some(remote)) if remote.is_file() => remote
            .pin
            .as_ref()
            .is_none_or(|pin| !baseline.remote.matches(pin)),
        (Some(_), None) => true,
        (Some(_), Some(_)) => true,
        (None, Some(remote)) => remote.is_file(),
        (None, None) => false,
    }
}

fn all_local_entries(snapshot: &LocalSnapshot) -> BTreeMap<String, LocalNode> {
    snapshot.nodes().clone()
}

fn all_remote_entries(snapshot: &RemoteSnapshot) -> BTreeMap<String, RemoteEntry> {
    snapshot.entries.clone()
}

fn local_files(snapshot: &LocalSnapshot) -> BTreeMap<String, LocalNode> {
    snapshot
        .nodes()
        .iter()
        .filter(|(_, node)| matches!(node.kind, LocalKind::File { .. }))
        .map(|(path, node)| (path.clone(), node.clone()))
        .collect()
}

fn reject_unsupported_directories(
    local: &LocalSnapshot,
    remote: &RemoteSnapshot,
    pending_paths: &BTreeSet<String>,
) -> Result<()> {
    let local_files = local_files(local);
    let remote_files = remote_files(remote);
    let mut unsupported = Vec::new();
    for node in local.nodes().values() {
        if node.is_directory()
            && !node.path.is_empty()
            && !has_descendant_file(&local_files, &node.path)
            && !is_pending_parent(&node.path, pending_paths)
        {
            unsupported.push(format!("local directory {:?}", node.path));
        }
    }
    for entry in remote.entries.values() {
        if !entry.is_file()
            && !entry.path.is_empty()
            && !has_descendant_file(&remote_files, &entry.path)
            && !is_pending_parent(&entry.path, pending_paths)
        {
            unsupported.push(format!("remote directory {:?}", entry.path));
        }
    }
    if unsupported.is_empty() {
        Ok(())
    } else {
        Err(Error::unsupported(format!(
            "Direct Sync does not represent empty directory markers; remove or populate: {}",
            unsupported.join(", ")
        )))
    }
}

fn has_descendant_file<T>(files: &BTreeMap<String, T>, directory: &str) -> bool {
    let prefix = format!("{directory}/");
    files.keys().any(|path| path.starts_with(&prefix))
}

fn is_pending_parent(directory: &str, pending_paths: &BTreeSet<String>) -> bool {
    let prefix = format!("{directory}/");
    pending_paths.iter().any(|path| path.starts_with(&prefix))
}

fn has_path_or_parent(path: &str, blocked: &BTreeSet<String>) -> bool {
    blocked.iter().any(|blocked| {
        path == blocked
            || path
                .strip_prefix(blocked)
                .is_some_and(|suffix| suffix.starts_with('/'))
    })
}

fn remote_files(snapshot: &RemoteSnapshot) -> BTreeMap<String, RemoteEntry> {
    snapshot
        .entries
        .iter()
        .filter(|(_, entry)| entry.is_file())
        .map(|(path, entry)| (path.clone(), entry.clone()))
        .collect()
}

fn local_file<'a>(snapshot: &'a LocalSnapshot, path: &str) -> Option<&'a LocalNode> {
    snapshot
        .get(path)
        .filter(|node| matches!(node.kind, LocalKind::File { .. }))
}

fn local_evidence(node: Option<&LocalNode>) -> LocalEvidence {
    match node {
        Some(node) => match node.kind {
            LocalKind::File { length, digest } => LocalEvidence {
                presence: ConflictPresence::Present,
                kind: Some(EvidenceKind::File),
                length: Some(length),
                digest: Some(*digest.as_bytes()),
            },
            LocalKind::Directory => LocalEvidence {
                presence: ConflictPresence::Present,
                kind: Some(EvidenceKind::Directory),
                length: None,
                digest: None,
            },
        },
        None => LocalEvidence {
            presence: ConflictPresence::Absent,
            kind: None,
            length: None,
            digest: None,
        },
    }
}

fn remote_evidence(entry: Option<&RemoteEntry>) -> RemoteEvidence {
    match entry {
        Some(entry) => RemoteEvidence {
            presence: ConflictPresence::Present,
            kind: Some(match entry.kind {
                RemoteKind::File => EvidenceKind::File,
                RemoteKind::Directory => EvidenceKind::Directory,
            }),
            length: Some(entry.size),
            pin: entry.pin.clone(),
        },
        None => RemoteEvidence {
            presence: ConflictPresence::Absent,
            kind: None,
            length: None,
            pin: None,
        },
    }
}

fn conflict_local_side(node: Option<&LocalNode>) -> ConflictSide {
    match node {
        Some(node) => match node.kind {
            LocalKind::File { length, digest } => ConflictSide {
                presence: ConflictPresence::Present,
                kind: Some(EvidenceKind::File),
                length: Some(length),
                digest: Some(*digest.as_bytes()),
                pin: None,
            },
            LocalKind::Directory => ConflictSide {
                presence: ConflictPresence::Present,
                kind: Some(EvidenceKind::Directory),
                length: None,
                digest: None,
                pin: None,
            },
        },
        None => ConflictSide::absent(),
    }
}

fn conflict_remote_side(entry: Option<&RemoteEntry>) -> ConflictSide {
    match entry {
        Some(entry) => ConflictSide {
            presence: ConflictPresence::Present,
            kind: Some(match entry.kind {
                RemoteKind::File => EvidenceKind::File,
                RemoteKind::Directory => EvidenceKind::Directory,
            }),
            length: Some(entry.size),
            digest: None,
            pin: entry.pin.clone(),
        },
        None => ConflictSide::absent(),
    }
}

fn work_identity(
    operation: RemoteOperation,
    path: &str,
    baseline: Option<&RemotePin>,
    desired_digest: Option<[u8; 32]>,
) -> [u8; 16] {
    let mut hasher = blake3::Hasher::new_derive_key("yinyang-direct-sync-work-1");
    hasher.update(&[match operation {
        RemoteOperation::Publish => 0,
        RemoteOperation::Delete => 1,
    }]);
    hasher.update(&(path.len() as u64).to_be_bytes());
    hasher.update(path.as_bytes());
    if let Some(pin) = baseline {
        hash_pin(&mut hasher, pin);
    }
    if let Some(digest) = desired_digest {
        hasher.update(&digest);
    }
    let mut id = [0; 16];
    id.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
    id
}

fn conflict_identity(path: &str, local: &ConflictSide, remote: &ConflictSide) -> String {
    let mut hasher = blake3::Hasher::new_derive_key("yinyang-direct-sync-conflict-1");
    hasher.update(&(path.len() as u64).to_be_bytes());
    hasher.update(path.as_bytes());
    for bytes in [
        serde_json::to_vec(local).unwrap_or_default(),
        serde_json::to_vec(remote).unwrap_or_default(),
    ] {
        hasher.update(&(bytes.len() as u64).to_be_bytes());
        hasher.update(&bytes);
    }
    let digest = hasher.finalize();
    digest.as_bytes()[..16]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn hash_pin(hasher: &mut blake3::Hasher, pin: &RemotePin) {
    for value in [&pin.version, &pin.etag] {
        match value {
            Some(value) => {
                hasher.update(&[1]);
                hasher.update(&(value.len() as u64).to_be_bytes());
                hasher.update(value.as_bytes());
            }
            None => {
                hasher.update(&[0]);
            }
        }
    }
    hasher.update(&pin.size.to_be_bytes());
    match pin.modified {
        Some(modified) => {
            hasher.update(&[1]);
            hasher.update(&modified.to_be_bytes());
        }
        None => {
            hasher.update(&[0]);
        }
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}
