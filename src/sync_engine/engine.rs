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

//! One reconciliation engine over one local tree and one remote adapter.

use super::adapter::{
    EntryKind, FrozenRemoteRequest, PreparedRemoteRequest, RemoteAdapter, RemoteBaseline,
    RemoteCapabilities, RemoteEntry, RemoteFailure, RemoteObservation, RemoteOperation,
    RemoteOutcome, RemoteRequest, SyncModel, hex,
};
use super::evidence::{
    ArtifactStatus, BaselineEvidence, BindingState, BindingStatus, ConflictSideStatus,
    ConflictStatus, EntryState, EntryStatus, EvidenceKind, IntentState as StatusIntentState,
    IntentStatus, LocalEvidence, LocalScanState, LocalStatus, OutcomeState, OutcomeStatus,
    Presence, RemoteAvailability, RemoteEvidence, RemoteStatus, StatusDocument, StatusSummary,
};
use super::local::{LocalTree, check_root_state};
use super::store::{
    ArtifactPhase, BaselineEntry, BaselineRecord, ConflictPresence, ConflictRecord, ConflictSide,
    EvidenceKind as StoreEvidenceKind, InstallOperation, InstallRecord, InstallState, IntentRecord,
    IntentState, LocalFingerprint, Store, WorkRecord,
};
use super::types::{LocalKind, LocalNode, LocalSnapshot};
use super::{Error, ErrorKind, Result};
use std::collections::{BTreeMap, BTreeSet};
use std::io::{Read, Write};
use std::os::fd::{AsFd, OwnedFd};
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};

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
    Content(Vec<u8>),
}

pub struct SyncEngine<R> {
    pub(super) remote: R,
    pub(super) local: LocalTree,
    pub(super) model: SyncModel,
}

impl<R: RemoteAdapter> SyncEngine<R> {
    pub async fn init(remote: R, state: &Path, root: &Path) -> Result<Self> {
        remote.preflight(state).await?;
        prepare_directory(root)?;
        check_root_state(root, state)?;
        let empty_hints = BTreeMap::new();
        let local_scan = LocalTree::scan_read_only(root, &empty_hints)?;
        let observed_remote = remote.observe(None).await?;
        validate_unsupported_directories(
            remote.capabilities().directories,
            &local_scan,
            &observed_remote,
            &BTreeSet::new(),
        )?;
        let local_nonempty = local_scan.nodes().keys().any(|path| !path.is_empty());
        let remote_nonempty = observed_remote.entries.keys().any(|path| !path.is_empty());
        if local_nonempty && remote_nonempty {
            return Err(Error::conflict(
                "sync initialization requires one empty side; both local and remote contain entries",
            ));
        }
        let model = remote.model();
        let namespace = remote.namespace_id();
        let mut local = LocalTree::create(root, state, model, namespace)?;
        remote.bind(state).await?;
        let root_entry = observed_remote.entries.get("").cloned().ok_or_else(|| {
            Error::corrupt("remote observation did not include the synchronized root")
        })?;
        local
            .store_mut()
            .save_baseline_entry("", &root_entry, None)?;
        local
            .store_mut()
            .set_cursor(&super::adapter::RemoteCursor::default())?;
        Ok(Self {
            remote,
            local,
            model,
        })
    }

    pub async fn open(remote: R, state: &Path) -> Result<Self> {
        remote.preflight(state).await?;
        let model = remote.model();
        let namespace = remote.namespace_id();
        let mut local = LocalTree::open(state, model, namespace)?;
        remote.bind(state).await?;
        local.recover_installs()?;
        Ok(Self {
            remote,
            local,
            model,
        })
    }

    pub fn root(&self) -> &Path {
        self.local.root()
    }

    pub fn state(&self) -> &Path {
        self.local.state()
    }

    pub fn namespace(&self) -> String {
        self.remote.namespace_id()
    }

    pub fn model(&self) -> SyncModel {
        self.model
    }

    pub fn hash_bytes_read(&self) -> u64 {
        self.local.hash_bytes_read()
    }

    pub fn artifact_path(&self, id: &str) -> Result<PathBuf> {
        let artifact = self
            .local
            .store()
            .artifacts()?
            .into_iter()
            .find(|artifact| artifact.id == id)
            .ok_or_else(|| Error::invalid("unknown retained artifact"))?;
        Ok(self.local.artifact_path(&artifact))
    }

    pub fn export_artifact(&self, id: &str, output: &mut dyn Write) -> Result<()> {
        let artifact = self
            .local
            .store()
            .artifacts()?
            .into_iter()
            .find(|artifact| artifact.id == id)
            .ok_or_else(|| Error::invalid("unknown retained artifact"))?;
        let mut file = self.local.open_artifact(&artifact.name)?;
        std::io::copy(&mut file, output)
            .map(|_| ())
            .map_err(|error| Error::io("export retained artifact", self.local.state(), error))
    }

    pub fn export_artifact_from_state(
        state: &Path,
        id: &str,
        output: &mut dyn Write,
    ) -> Result<()> {
        let store = Store::open_read_only(&state.join("sync-engine.db"))?;
        let artifact = store
            .artifacts()?
            .into_iter()
            .find(|artifact| artifact.id == id)
            .ok_or_else(|| Error::invalid("unknown retained artifact"))?;
        let path = state.join("artifacts").join(&artifact.name);
        let mut file = std::fs::File::open(&path)
            .map_err(|error| Error::io("open retained artifact", &path, error))?;
        std::io::copy(&mut file, output)
            .map(|_| ())
            .map_err(|error| Error::io("export retained artifact", &path, error))
    }

    pub fn export_conflict_local_from_state(
        state: &Path,
        conflict_id: &str,
        output: &mut dyn Write,
    ) -> Result<()> {
        let store = Store::open_read_only(&state.join("sync-engine.db"))?;
        let conflict = store
            .conflict(conflict_id)?
            .ok_or_else(|| Error::invalid("unknown conflict"))?;
        if let Some(id) = conflict.local_artifact {
            return Self::export_artifact_from_state(state, &id, output);
        }
        let binding = store.binding()?;
        let path = conflict.desired_path.as_deref().unwrap_or(&conflict.path);
        super::types::validate_path(path)?;
        let path = binding.root.join(path);
        let mut file = std::fs::File::open(&path)
            .map_err(|error| Error::io("open live local conflict", &path, error))?;
        std::io::copy(&mut file, output)
            .map(|_| ())
            .map_err(|error| Error::io("export live local conflict", &path, error))
    }

    pub async fn backup(&mut self, output: &Path) -> Result<()> {
        self.local.recover_installs()?;
        super::backup::create(self, output)
    }

    pub async fn restore(remote: R, backup: &Path, root: &Path, state: &Path) -> Result<Self> {
        super::backup::restore(remote, backup, root, state).await
    }

    pub async fn run(&mut self) -> Result<RunOutcome> {
        self.local.settle_fresh_artifacts()?;
        self.local.recover_installs()?;
        self.retry_work().await?;

        let baseline = self
            .local
            .store()
            .baseline()?
            .ok_or_else(|| Error::invalid("sync state has no common baseline"))?;
        let observation = self.observe(&baseline).await?;
        let mut current = self.local.scan()?;
        let blocked = self.blocked_paths()?;
        validate_unsupported_directories(
            self.remote.capabilities().directories,
            &current,
            &observation,
            &blocked,
        )?;

        let mut remote = observation.entries.clone();
        let decisions = plan(
            &baseline,
            &current,
            &remote,
            &blocked,
            self.remote.capabilities(),
        )?;
        let mut outcome = RunOutcome::default();
        let capabilities = self.remote.capabilities();
        let mut pending = Vec::new();
        for decision in decisions {
            if capabilities.batch
                && let Decision::PublishFile {
                    path,
                    local,
                    baseline,
                } = &decision
            {
                let prepared = self
                    .prepare_file_publication(path, local, baseline.clone())
                    .await?;
                pending.push((path.clone(), local.clone(), prepared));
                if pending.len() >= capabilities.max_batch.max(1) {
                    self.flush_file_publications(
                        &mut pending,
                        &mut current,
                        &mut remote,
                        &mut outcome,
                    )
                    .await?;
                }
                continue;
            }
            self.flush_file_publications(&mut pending, &mut current, &mut remote, &mut outcome)
                .await?;
            self.apply_decision(decision, &mut current, &mut remote, &mut outcome)
                .await?;
        }
        self.flush_file_publications(&mut pending, &mut current, &mut remote, &mut outcome)
            .await?;

        let final_observation = if remote_entries_match(&observation.entries, &remote) {
            observation
        } else {
            let final_baseline = self.remote.capabilities().delta.then(|| RemoteBaseline {
                cursor: Some(baseline.cursor.clone()),
                entries: remote.clone(),
            });
            self.remote.observe(final_baseline.as_ref()).await?
        };
        let cursor = if remote_entries_match(&final_observation.entries, &remote) {
            final_observation.cursor.clone()
        } else {
            baseline.cursor.clone()
        };
        if cursor != baseline.cursor {
            self.local.store_mut().set_cursor(&cursor)?;
        }
        let final_local = if outcome.materialized_remote == 0 && outcome.deleted_local == 0 {
            current
        } else {
            self.local.scan()?
        };
        if !self.remote.capabilities().directories {
            for (path, entry) in &final_observation.entries {
                if entry.kind == EntryKind::Directory
                    && final_local.get(path).is_some_and(LocalNode::is_directory)
                    && !baseline.remote.contains_key(path)
                {
                    self.local.store().save_baseline_entry(path, entry, None)?;
                }
            }
        }
        let baseline = self
            .local
            .store()
            .baseline()?
            .ok_or_else(|| Error::invalid("sync baseline disappeared"))?;
        let inspection = self.inspect_observed(
            Some(&final_observation),
            Ok(final_local),
            Some(baseline),
            None,
        )?;
        outcome.pending = inspection.summary.pending;
        outcome.unknown = inspection.summary.unknown;
        outcome.conflicts = inspection.summary.conflicts;
        outcome.in_sync = inspection.summary.in_sync;
        self.local.prune_artifacts()?;
        Ok(outcome)
    }

    async fn prepare_file_publication(
        &mut self,
        path: &str,
        local: &LocalNode,
        baseline: Option<RemoteEntry>,
    ) -> Result<PreparedRemoteRequest> {
        let request = match baseline {
            Some(baseline) => RemoteRequest::ReplaceFile {
                path: path.to_owned(),
                source: PathBuf::new(),
                baseline,
            },
            None => {
                let parent = parent_path(path);
                let parent_entry = self
                    .local
                    .store()
                    .baseline()?
                    .and_then(|baseline| baseline.remote.get(parent).cloned());
                RemoteRequest::CreateFile {
                    path: path.to_owned(),
                    source: PathBuf::new(),
                    executable: local.executable,
                    parent: parent_entry,
                }
            }
        };
        let accepted = LocalFingerprint::from_node(local)?;
        self.prepare_publication(request, Some(local), Some(accepted), None)
            .await
    }

    async fn flush_file_publications(
        &mut self,
        pending: &mut Vec<(String, LocalNode, PreparedRemoteRequest)>,
        current: &mut LocalSnapshot,
        remote: &mut BTreeMap<String, RemoteEntry>,
        outcome: &mut RunOutcome,
    ) -> Result<()> {
        if pending.is_empty() {
            return Ok(());
        }
        let prepared = std::mem::take(pending);
        let outcomes = self
            .publish_prepared(
                prepared
                    .iter()
                    .map(|(_, _, request)| request.clone())
                    .collect(),
                false,
            )
            .await?;
        self.update_ledger(&outcomes, remote);
        for ((path, _, _), accepted) in prepared.into_iter().zip(outcomes) {
            if matches!(accepted, RemoteOutcome::Applied { .. }) {
                refresh_local_path(&self.local, &path, current)?;
                outcome.published_local += 1;
            }
        }
        Ok(())
    }

    async fn observe(&self, baseline: &BaselineRecord) -> Result<RemoteObservation> {
        let remote_baseline = if self.remote.capabilities().delta {
            Some(RemoteBaseline {
                cursor: Some(baseline.cursor.clone()),
                entries: baseline.remote.clone(),
            })
        } else {
            None
        };
        self.remote.observe(remote_baseline.as_ref()).await
    }

    fn blocked_paths(&self) -> Result<BTreeSet<String>> {
        let mut blocked = BTreeSet::new();
        let committed = self
            .local
            .store()
            .intents()?
            .into_iter()
            .filter(|intent| intent.state == IntentState::Committed)
            .map(|intent| intent.id)
            .collect::<BTreeSet<_>>();
        blocked.extend(
            self.local
                .store()
                .work()?
                .into_iter()
                .filter(|work| !committed.contains(&work.id))
                .map(|work| work.path),
        );
        blocked.extend(
            self.local
                .store()
                .installs()?
                .into_iter()
                .map(|install| install.path),
        );
        blocked.extend(
            self.local
                .store()
                .conflicts()?
                .into_iter()
                .map(|conflict| conflict.path),
        );
        Ok(blocked)
    }

    async fn retry_work(&mut self) -> Result<()> {
        let work = self.local.store().work()?;
        let intents = self
            .local
            .store()
            .intents()?
            .into_iter()
            .map(|intent| (intent.id.clone(), intent))
            .collect::<BTreeMap<_, _>>();
        let mut retry = Vec::new();
        for work in work {
            let Some(intent) = intents.get(&work.id).cloned() else {
                continue;
            };
            if !matches!(
                intent.state,
                IntentState::Prepared | IntentState::Unknown | IntentState::Retryable
            ) {
                continue;
            }
            if !self.remote.capabilities().receipt_lookup
                && !self.work_matches_local_input(&work)?
            {
                continue;
            }
            let source = self.work_source(&work)?;
            let frozen = FrozenRemoteRequest {
                id: intent.request_id,
                digest: intent.request_digest,
                bytes: intent.request.clone(),
                operation: work.operation,
                path: work.path.clone(),
                original_path: work.original_path.clone(),
                desired_path: work.desired_path.clone(),
            };
            let was_unknown = intent.state == IntentState::Unknown;
            if self.remote.capabilities().receipt_lookup
                && let Some(outcome) = self.remote.receipt(&frozen).await?
            {
                self.apply_outcome(work, intent, was_unknown, outcome, &BTreeMap::new())
                    .await?;
                continue;
            }
            let mut intent = intent;
            intent.state = IntentState::Unknown;
            self.local.store().update_intent(&intent)?;
            retry.push((
                PreparedRemoteRequest {
                    request: frozen,
                    source,
                },
                work,
                intent,
                was_unknown,
            ));
        }
        let max_batch = self.remote.capabilities().max_batch.max(1);
        for chunk in retry.chunks(max_batch) {
            let prepared = chunk
                .iter()
                .map(|(prepared, _, _, _)| prepared.clone())
                .collect::<Vec<_>>();
            let outcomes = self.remote.publish_batch(&prepared).await?;
            if outcomes.len() != chunk.len() {
                return Err(Error::corrupt(
                    "remote adapter returned the wrong retry outcome count",
                ));
            }
            for ((_, work, intent, was_unknown), outcome) in chunk.iter().cloned().zip(outcomes) {
                self.apply_outcome(work, intent, was_unknown, outcome, &BTreeMap::new())
                    .await?;
            }
        }
        Ok(())
    }

    fn work_matches_local_input(&self, work: &WorkRecord) -> Result<bool> {
        let current = self.local.observe_path(&work.path)?;
        match work.operation {
            RemoteOperation::Remove => Ok(current.is_none()),
            RemoteOperation::Content
            | RemoteOperation::CreateFile
            | RemoteOperation::SetExecutable => {
                let Some(accepted) = work.accepted_local else {
                    return Ok(false);
                };
                let Some(current) = current.filter(|node| !node.is_directory()) else {
                    return Ok(false);
                };
                Ok(accepted.same_content(
                    match current.kind {
                        LocalKind::File { length, .. } => length,
                        LocalKind::Directory => 0,
                    },
                    local_digest(&current),
                    current.executable,
                ))
            }
            RemoteOperation::CreateDirectory | RemoteOperation::Rename => Ok(true),
        }
    }

    fn work_source(&self, work: &WorkRecord) -> Result<Option<PathBuf>> {
        let Some(id) = work.source_artifact.as_deref() else {
            return Ok(None);
        };
        let artifact = self
            .local
            .store()
            .artifacts()?
            .into_iter()
            .find(|artifact| artifact.id == id)
            .ok_or_else(|| Error::corrupt("work source artifact disappeared"))?;
        self.local.verify_artifact(&artifact)?;
        Ok(Some(self.local.artifact_path(&artifact)))
    }

    async fn prepare_publication(
        &mut self,
        mut request: RemoteRequest,
        source: Option<&LocalNode>,
        accepted_local: Option<LocalFingerprint>,
        conflict: Option<String>,
    ) -> Result<PreparedRemoteRequest> {
        let source_artifact = match source {
            Some(node) => {
                let artifact = self
                    .local
                    .capture_local(node, super::store::ArtifactKind::UploadRetained)?;
                self.local.store().insert_artifact(&artifact)?;
                Some(artifact)
            }
            None => None,
        };
        if let Some(artifact) = &source_artifact {
            request.set_source(self.local.artifact_path(artifact));
        }
        let operation = request.operation();
        let path = request.path().to_owned();
        let original_path = request.original_path().map(str::to_owned);
        let desired_path = request.desired_path().map(str::to_owned);
        let frozen = match self.remote.freeze(request).await {
            Ok(frozen) => frozen,
            Err(error) => {
                if let Some(artifact) = &source_artifact {
                    let _ = self.local.release_artifact(artifact);
                }
                return Err(error);
            }
        };
        let id = hex(&frozen.id);
        let intent = IntentRecord {
            id: id.clone(),
            request_id: frozen.id,
            request_digest: frozen.digest,
            request: frozen.bytes.clone(),
            commit_id: None,
            state: IntentState::Prepared,
            receipt_revision: None,
            receipt_ordinal: None,
            failure: None,
        };
        let work = WorkRecord {
            seq: 0,
            id: id.clone(),
            path: path.clone(),
            original_path: original_path.clone(),
            desired_path: desired_path.clone(),
            operation,
            accepted_local,
            baseline: None,
            request_id: frozen.id,
            source_artifact: source_artifact.as_ref().map(|artifact| artifact.id.clone()),
            conflict,
        };
        self.local.store().insert_intent(&intent)?;
        self.local.store_mut().insert_work(&work)?;
        Ok(PreparedRemoteRequest {
            request: frozen,
            source: source_artifact
                .as_ref()
                .map(|artifact| self.local.artifact_path(artifact)),
        })
    }

    async fn publish_prepared(
        &mut self,
        prepared: Vec<PreparedRemoteRequest>,
        was_unknown: bool,
    ) -> Result<Vec<RemoteOutcome>> {
        if prepared.is_empty() {
            return Ok(Vec::new());
        }
        let ids = prepared
            .iter()
            .map(|prepared| hex(&prepared.request.id))
            .collect::<Vec<_>>();
        for id in &ids {
            if let Some(mut intent) = self.local.store().intent(id)? {
                intent.state = IntentState::Unknown;
                self.local.store().update_intent(&intent)?;
            }
        }
        let outcomes = self.remote.publish_batch(&prepared).await?;
        if outcomes.len() != prepared.len() {
            return Err(Error::corrupt(
                "remote adapter returned the wrong publication outcome count",
            ));
        }
        let mut applied = Vec::with_capacity(outcomes.len());
        for (prepared, outcome) in prepared.into_iter().zip(outcomes) {
            let id = hex(&prepared.request.id);
            let work = self
                .local
                .store()
                .work()?
                .into_iter()
                .find(|work| work.id == id)
                .ok_or_else(|| Error::corrupt("publication work association disappeared"))?;
            let intent = self
                .local
                .store()
                .intent(&id)?
                .ok_or_else(|| Error::corrupt("publication intent disappeared"))?;
            self.apply_outcome(work, intent, was_unknown, outcome.clone(), &BTreeMap::new())
                .await?;
            applied.push(outcome);
        }
        Ok(applied)
    }

    async fn apply_outcome(
        &mut self,
        work: WorkRecord,
        mut intent: IntentRecord,
        was_unknown: bool,
        outcome: RemoteOutcome,
        _remote: &BTreeMap<String, RemoteEntry>,
    ) -> Result<()> {
        match outcome {
            RemoteOutcome::Applied { accepted, receipt } => {
                let mut updates = Vec::new();
                let mut removed = Vec::new();
                if work.operation == RemoteOperation::Rename
                    && let Some(from) = work.original_path.as_deref()
                {
                    self.local
                        .store_mut()
                        .rename_baseline_subtree(from, &work.path)?;
                }
                for (path, entry) in accepted {
                    match entry {
                        Some(entry) => {
                            let local = if path == work.path {
                                work.accepted_local
                            } else {
                                None
                            };
                            updates.push((path, entry, local));
                        }
                        None => removed.push(path),
                    }
                }
                let update_refs = updates
                    .iter()
                    .map(|(path, entry, local)| (path.as_str(), entry.clone(), *local))
                    .collect::<Vec<_>>();
                let removed_refs = removed.iter().map(String::as_str).collect::<Vec<_>>();
                let commit_id = receipt
                    .as_ref()
                    .and_then(|receipt| receipt.commit)
                    .or(intent.commit_id)
                    .or_else(|| intent.failure.as_ref().and_then(|failure| failure.commit));
                self.local.store_mut().complete_publication(
                    &work,
                    &update_refs,
                    &removed_refs,
                    receipt.as_ref(),
                    commit_id,
                )?;
                if let Some(artifact_id) = work.source_artifact.as_deref()
                    && let Some(artifact) = self
                        .local
                        .store()
                        .artifacts()?
                        .into_iter()
                        .find(|artifact| artifact.id == artifact_id)
                    && self.local.release_artifact(&artifact)?
                {
                    self.local.store().remove_artifact(&artifact.id)?;
                }
            }
            RemoteOutcome::Conflict { observed } => {
                if was_unknown {
                    intent.state = IntentState::Unknown;
                    intent.failure = Some(RemoteFailure {
                        kind: ErrorKind::Unknown,
                        message: "the original request remains unknown; its accepted precondition no longer matches".to_owned(),
                        commit: None,
                    });
                    self.local.store().update_intent(&intent)?;
                } else {
                    intent.state = IntentState::Conflict;
                    intent.failure = Some(RemoteFailure {
                        kind: ErrorKind::Conflict,
                        message: "remote precondition no longer matches".to_owned(),
                        commit: None,
                    });
                    self.local.store().update_intent(&intent)?;
                    let local = self.local.observe_path(&work.path)?;
                    self.record_conflict(
                        &work.path,
                        work.original_path.as_deref(),
                        work.desired_path.as_deref(),
                        local.as_ref(),
                        observed.as_ref(),
                        "remote precondition no longer matches the accepted baseline",
                    )
                    .await?;
                }
            }
            RemoteOutcome::Unknown { failure } => {
                intent.commit_id = failure.commit;
                intent.state = IntentState::Unknown;
                intent.failure = Some(failure);
                self.local.store().update_intent(&intent)?;
            }
            RemoteOutcome::Retryable { failure } => {
                intent.commit_id = failure.commit;
                intent.state = IntentState::Retryable;
                intent.failure = Some(failure);
                self.local.store().update_intent(&intent)?;
            }
            RemoteOutcome::Unsupported { failure } => {
                intent.commit_id = failure.commit;
                intent.state = IntentState::Failed;
                intent.failure = Some(failure);
                self.local.store().update_intent(&intent)?;
            }
        }
        Ok(())
    }

    async fn apply_decision(
        &mut self,
        decision: Decision,
        current: &mut LocalSnapshot,
        remote: &mut BTreeMap<String, RemoteEntry>,
        outcome: &mut RunOutcome,
    ) -> Result<()> {
        match decision {
            Decision::PublishFile {
                path,
                local,
                baseline,
            } => {
                let parent = parent_path(&path);
                let parent_entry = remote.get(parent).cloned();
                if parent_entry.is_none() && self.remote.capabilities().directories {
                    return Err(Error::conflict(format!(
                        "remote parent {parent:?} is missing"
                    )));
                }
                let request = match baseline {
                    Some(baseline) => RemoteRequest::ReplaceFile {
                        path: path.clone(),
                        source: PathBuf::new(),
                        baseline,
                    },
                    None => RemoteRequest::CreateFile {
                        path: path.clone(),
                        source: PathBuf::new(),
                        executable: local.executable,
                        parent: parent_entry,
                    },
                };
                let accepted = LocalFingerprint::from_node(&local)?;
                let outcomes = self
                    .publish_request(request, Some(&local), Some(accepted), None)
                    .await?;
                self.update_ledger(&outcomes, remote);
                if outcomes
                    .iter()
                    .any(|outcome| matches!(outcome, RemoteOutcome::Applied { .. }))
                {
                    refresh_local_path(&self.local, &path, current)?;
                    outcome.published_local += 1;
                }
            }
            Decision::PublishDirectory { path } => {
                let parent = parent_path(&path);
                let parent_entry = remote.get(parent).cloned().ok_or_else(|| {
                    Error::conflict(format!("remote parent {parent:?} is missing"))
                })?;
                let request = RemoteRequest::CreateDirectory {
                    path: path.clone(),
                    parent: parent_entry,
                };
                let outcomes = self.publish_request(request, None, None, None).await?;
                self.update_ledger(&outcomes, remote);
                if outcomes
                    .iter()
                    .any(|outcome| matches!(outcome, RemoteOutcome::Applied { .. }))
                {
                    refresh_local_path(&self.local, &path, current)?;
                    outcome.published_local += 1;
                }
            }
            Decision::PublishDelete { path } => {
                let baseline = remote.get(&path).cloned().ok_or_else(|| {
                    Error::conflict(format!("remote delete target {path:?} disappeared"))
                })?;
                let request = RemoteRequest::Remove {
                    path: path.clone(),
                    baseline,
                };
                let descendants = remote
                    .keys()
                    .filter(|candidate| {
                        candidate
                            .strip_prefix(&path)
                            .is_some_and(|suffix| suffix.starts_with('/'))
                    })
                    .cloned()
                    .collect::<Vec<_>>();
                let outcomes = self
                    .publish_request(request, None, None, None)
                    .await
                    .map_err(|error| {
                        Error::new(
                            error.kind(),
                            format!(
                                "delete {path} with accepted remote descendants {descendants:?}: {error}"
                            ),
                        )
                    })?;
                self.update_ledger(&outcomes, remote);
                if outcomes
                    .iter()
                    .any(|outcome| matches!(outcome, RemoteOutcome::Applied { .. }))
                {
                    current.remove_subtree(&path);
                    outcome.deleted_remote += 1;
                }
            }
            Decision::PublishExecutable {
                path,
                baseline,
                executable,
            } => {
                let accepted = self
                    .local
                    .observe_path(&path)?
                    .filter(|node| !node.is_directory())
                    .map(|node| LocalFingerprint::from_node(&node))
                    .transpose()?;
                let request = RemoteRequest::SetExecutable {
                    path: path.clone(),
                    baseline,
                    executable,
                };
                let outcomes = self.publish_request(request, None, accepted, None).await?;
                self.update_ledger(&outcomes, remote);
                if outcomes
                    .iter()
                    .any(|outcome| matches!(outcome, RemoteOutcome::Applied { .. }))
                {
                    outcome.published_local += 1;
                }
            }
            Decision::PublishRename { from, to, baseline } => {
                let new_parent = remote.get(parent_path(&to)).cloned().ok_or_else(|| {
                    Error::conflict(format!(
                        "remote rename parent {:?} is missing",
                        parent_path(&to)
                    ))
                })?;
                let request = RemoteRequest::Rename {
                    from: from.clone(),
                    to: to.clone(),
                    baseline,
                    new_parent,
                    replace: false,
                };
                let outcomes = self.publish_request(request, None, None, None).await?;
                self.update_ledger(&outcomes, remote);
                if outcomes
                    .iter()
                    .any(|outcome| matches!(outcome, RemoteOutcome::Applied { .. }))
                {
                    if let (Some(remote_entry), Some(node)) =
                        (remote.get(&to).cloned(), self.local.observe_path(&to)?)
                    {
                        let local = (!node.is_directory())
                            .then(|| LocalFingerprint::from_node(&node))
                            .transpose()?;
                        self.local
                            .store()
                            .save_baseline_entry(&to, &remote_entry, local)?;
                    }
                    outcome.published_local += 1;
                }
            }
            Decision::MaterializeFile { entry, expected } => {
                self.materialize_file(&entry, expected).await?;
                let path = entry.path.clone();
                remote.insert(path.clone(), entry);
                refresh_local_path(&self.local, &path, current)?;
                outcome.materialized_remote += 1;
            }
            Decision::MaterializeDirectory { entry } => {
                let path = entry.path.clone();
                match self.local.create_directory_local(&path) {
                    Ok(()) => {}
                    Err(error)
                        if error.kind() == ErrorKind::Conflict
                            && current.get(&path).is_some_and(|node| node.is_directory()) => {}
                    Err(error) => return Err(error),
                }
                self.local
                    .store()
                    .save_baseline_entry(&path, &entry, None)?;
                remote.insert(path.clone(), entry);
                refresh_local_path(&self.local, &path, current)?;
                outcome.materialized_remote += 1;
            }
            Decision::DeleteLocalFile { entry, expected } => {
                self.delete_local_file(&entry.path, expected).await?;
                let path = entry.path.clone();
                remote.insert(path.clone(), entry);
                refresh_local_path(&self.local, &path, current)?;
                outcome.deleted_local += 1;
            }
            Decision::DeleteLocalDirectory { path, expected } => {
                self.local
                    .quarantine(&path, &expected, "remote directory deletion", None)?;
                self.local.store().remove_baseline_subtree(&path)?;
                current.remove_subtree(&path);
                remote.remove(&path);
                outcome.deleted_local += 1;
            }
            Decision::RemoveBaseline { path } => {
                self.local.store().remove_baseline_path(&path)?;
            }
            Decision::AcceptRemote { path, entry, local } => {
                let fingerprint = local
                    .as_ref()
                    .filter(|node| !node.is_directory())
                    .map(LocalFingerprint::from_node)
                    .transpose()?;
                self.local
                    .store()
                    .save_baseline_entry(&path, &entry, fingerprint)?;
                remote.insert(path.clone(), entry);
                if let Some(local) = local {
                    current.insert_node(local);
                }
            }
            Decision::ApplyLocalRename { from, to, entry } => {
                self.local.rename_local(&from, &to)?;
                self.local.store_mut().rename_baseline_subtree(&from, &to)?;
                if let Some(observed) = self.local.observe_path(&to)? {
                    current.move_subtree(&from, &to, &observed);
                }
                let to_parent = parent_path(&to);
                if let Some(parent) = remote.get(to_parent).cloned() {
                    self.local
                        .store()
                        .save_baseline_entry(to_parent, &parent, None)?;
                }
                remote.remove(&from);
                remote.insert(to.clone(), entry.clone());
                self.local.store().save_baseline_entry(&to, &entry, None)?;
                outcome.materialized_remote += 1;
            }
            Decision::AcceptRename { from, to, entry } => {
                self.local.store_mut().rename_baseline_subtree(&from, &to)?;
                if let Some(observed) = self.local.observe_path(&to)? {
                    current.move_subtree(&from, &to, &observed);
                }
                remote.remove(&from);
                remote.insert(to.clone(), entry.clone());
                let local = self
                    .local
                    .observe_path(&to)?
                    .filter(|node| !node.is_directory())
                    .map(|node| LocalFingerprint::from_node(&node))
                    .transpose()?;
                self.local.store().save_baseline_entry(&to, &entry, local)?;
            }
            Decision::Conflict {
                path,
                original_path,
                desired_path,
                local,
                remote: remote_entry,
                message,
            } => {
                self.record_conflict(
                    &path,
                    original_path.as_deref(),
                    desired_path.as_deref(),
                    local.as_ref(),
                    remote_entry.as_ref(),
                    &message,
                )
                .await?;
            }
        }
        Ok(())
    }

    async fn publish_request(
        &mut self,
        request: RemoteRequest,
        source: Option<&LocalNode>,
        accepted_local: Option<LocalFingerprint>,
        conflict: Option<String>,
    ) -> Result<Vec<RemoteOutcome>> {
        let prepared = self
            .prepare_publication(request, source, accepted_local, conflict)
            .await?;
        self.publish_prepared(vec![prepared], false).await
    }

    fn update_ledger(
        &self,
        outcomes: &[RemoteOutcome],
        remote: &mut BTreeMap<String, RemoteEntry>,
    ) {
        for outcome in outcomes {
            let RemoteOutcome::Applied { accepted, .. } = outcome else {
                continue;
            };
            for (path, entry) in accepted {
                match entry {
                    Some(entry) => {
                        remote.insert(path.clone(), entry.clone());
                    }
                    None => {
                        remote.remove(path);
                    }
                }
            }
        }
    }

    async fn materialize_file(
        &mut self,
        entry: &RemoteEntry,
        expected: Option<LocalFingerprint>,
    ) -> Result<()> {
        let (temporary, path, file) = self.local.create_install_file("download")?;
        let owned: OwnedFd = file.into();
        let mut install = InstallRecord {
            id: uuid::Uuid::new_v4().to_string(),
            path: entry.path.clone(),
            operation: if expected.is_some() {
                InstallOperation::Replace
            } else {
                InstallOperation::Create
            },
            state: InstallState::Prepared,
            expected_local: expected,
            desired_local: None,
            desired_remote: Some(entry.clone()),
            temporary: temporary.clone(),
            artifact: None,
            error: None,
        };
        self.local.store().insert_install(&install)?;
        if let Err(error) = self.remote.read_file(entry, owned.as_fd()).await {
            let _ = std::fs::remove_file(&path);
            let _ = self.local.store().remove_install(&install.id);
            return Err(error);
        }
        drop(owned);
        std::fs::set_permissions(
            &path,
            std::fs::Permissions::from_mode(if entry.executable { 0o700 } else { 0o600 }),
        )
        .map_err(|error| Error::io("set downloaded sync file mode", &path, error))?;
        let desired = match self.local.install_desired_file(&temporary) {
            Ok(desired) => desired,
            Err(error) => {
                let _ = std::fs::remove_file(&path);
                let _ = self.local.store().remove_install(&install.id);
                return Err(error);
            }
        };
        install.desired_local = Some(desired);
        self.local.store().update_install(&install)?;
        self.local.apply_install(&mut install)
    }

    async fn delete_local_file(&mut self, path: &str, expected: LocalFingerprint) -> Result<()> {
        let mut install = InstallRecord {
            id: uuid::Uuid::new_v4().to_string(),
            path: path.to_owned(),
            operation: InstallOperation::Delete,
            state: InstallState::Prepared,
            expected_local: Some(expected),
            desired_local: None,
            desired_remote: None,
            temporary: format!("delete-{}", uuid::Uuid::new_v4().simple()),
            artifact: None,
            error: None,
        };
        self.local.store().insert_install(&install)?;
        self.local.apply_install(&mut install)
    }

    async fn record_conflict(
        &mut self,
        path: &str,
        original_path: Option<&str>,
        desired_path: Option<&str>,
        local: Option<&LocalNode>,
        remote: Option<&RemoteEntry>,
        message: &str,
    ) -> Result<()> {
        let local_side = conflict_local_side(local);
        let remote_side = conflict_remote_side(remote);
        let id = conflict_identity(path, original_path, desired_path, &local_side, &remote_side);
        if self.local.store().conflict(&id)?.is_some() {
            return Ok(());
        }
        let local_artifact = match local.filter(|node| !node.is_directory()) {
            Some(node) => {
                let artifact = self
                    .local
                    .capture_local(node, super::store::ArtifactKind::ConflictLocal)?;
                self.local.store().insert_artifact(&artifact)?;
                Some(artifact.id)
            }
            None => None,
        };
        let remote_artifact = match remote.filter(|entry| entry.kind == EntryKind::File) {
            Some(entry) => match self.capture_remote_artifact(entry).await {
                Ok(artifact) => {
                    self.local.store().insert_artifact(&artifact)?;
                    Some(artifact.id)
                }
                Err(_) => None,
            },
            None => None,
        };
        let record = ConflictRecord {
            id,
            path: path.to_owned(),
            original_path: original_path.map(str::to_owned),
            desired_path: desired_path.map(str::to_owned),
            local: local_side,
            remote: remote_side,
            local_artifact,
            remote_artifact,
            remote_revision: remote.and_then(|entry| entry.pin.revision()),
            error: message.to_owned(),
        };
        self.local.store().upsert_conflict(&record)
    }

    async fn capture_remote_artifact(
        &self,
        entry: &RemoteEntry,
    ) -> Result<super::store::ArtifactRecord> {
        let (temporary, path, file) = self.local.create_install_file("conflict")?;
        let owned: OwnedFd = file.into();
        if let Err(error) = self.remote.read_file(entry, owned.as_fd()).await {
            let _ = std::fs::remove_file(&path);
            return Err(error);
        }
        drop(owned);
        let mut artifact = self.local.retain_install_temporary(
            &temporary,
            &entry.path,
            super::store::ArtifactKind::ConflictRemote,
        )?;
        artifact.phase = ArtifactPhase::Conflict;
        artifact.settle_pending = false;
        self.local.store().update_artifact(&artifact)?;
        Ok(artifact)
    }

    pub async fn inspect(
        remote: Option<&R>,
        state: &Path,
        model: SyncModel,
        remote_error: Option<String>,
    ) -> StatusDocument {
        let state_text = std::fs::canonicalize(state)
            .unwrap_or_else(|_| state.to_owned())
            .display()
            .to_string();
        if !state.join("sync-engine.db").exists() {
            let mut document = StatusDocument::empty(model, state_text);
            if let Some(remote) = remote
                && let Err(error) = remote.preflight(state).await
            {
                document.binding.state = BindingState::Error;
                document.binding.error = Some(error.to_string());
            }
            return document;
        }
        let (store, binding, current) = match LocalTree::inspect(state) {
            Ok(value) => value,
            Err(error) => {
                let mut document = StatusDocument::empty(model, state_text.clone());
                document.binding.state = BindingState::Error;
                document.binding.error = Some(error.to_string());
                return document;
            }
        };
        if let Some(remote) = remote
            && remote.namespace_id() != binding.namespace
        {
            let mut document = StatusDocument::empty(model, state_text);
            document.binding.state = BindingState::Error;
            document.binding.root = Some(binding.root.display().to_string());
            document.binding.namespace = Some(binding.namespace);
            document.binding.error =
                Some("configured storage namespace differs from binding".to_owned());
            return document;
        }
        let baseline = store.baseline().ok().flatten();
        let hints = baseline
            .as_ref()
            .map(|baseline| baseline.local.clone())
            .unwrap_or_default();
        let local_scan = LocalTree::scan_read_only(&current.root, &hints);
        let remote_observation = match remote {
            Some(remote) => {
                let baseline = baseline.as_ref().map(|baseline| RemoteBaseline {
                    cursor: Some(baseline.cursor.clone()),
                    entries: baseline.remote.clone(),
                });
                Some(remote.observe(baseline.as_ref()).await)
            }
            None => None,
        };
        match Self::build_status(
            &store,
            &binding,
            state_text,
            local_scan,
            remote_observation,
            remote_error,
            baseline,
            remote
                .as_ref()
                .map(|remote| remote.capabilities().executable)
                .unwrap_or(RemoteCapabilities::for_model(binding.model).executable),
        ) {
            Ok(document) => document,
            Err(error) => {
                let mut document = StatusDocument::empty(model, binding.root.display().to_string());
                document.binding.state = BindingState::Error;
                document.binding.error = Some(error.to_string());
                document
            }
        }
    }

    fn inspect_observed(
        &self,
        remote: Option<&RemoteObservation>,
        local: Result<LocalSnapshot>,
        baseline: Option<BaselineRecord>,
        remote_error: Option<String>,
    ) -> Result<StatusDocument> {
        let binding = self.local.binding()?;
        Self::build_status(
            self.local.store(),
            &binding,
            self.local.state().display().to_string(),
            local,
            remote.map(|remote| Ok(remote.clone())),
            remote_error,
            baseline,
            self.remote.capabilities().executable,
        )
    }

    fn build_status(
        store: &Store,
        binding: &super::store::Binding,
        state_directory: String,
        local: Result<LocalSnapshot>,
        remote: Option<Result<RemoteObservation>>,
        remote_error: Option<String>,
        baseline: Option<BaselineRecord>,
        executable: bool,
    ) -> Result<StatusDocument> {
        let artifacts = store.artifacts()?;
        let conflicts = store.conflicts()?;
        let work = store.work()?;
        let intents = store
            .intents()?
            .into_iter()
            .map(|intent| (intent.id.clone(), intent))
            .collect::<BTreeMap<_, _>>();
        let installs = store.installs()?;
        let (local_snapshot, local_status) = match local {
            Ok(local) => {
                let changed = baseline.as_ref().is_some_and(|baseline| {
                    let local_paths = local.nodes().keys().cloned().collect::<BTreeSet<_>>();
                    let baseline_paths = baseline.local.keys().cloned().collect::<BTreeSet<_>>();
                    let mut paths = local_paths;
                    paths.extend(baseline_paths);
                    paths
                        .into_iter()
                        .filter(|path| !path.is_empty())
                        .any(|path| {
                            local_changed(
                                baseline.remote.get(&path).map(|entry| entry.kind),
                                baseline.local.get(&path),
                                local.get(&path),
                                executable,
                            )
                        })
                });
                (
                    Some(local),
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
        let (remote_entries, remote_status) = match remote {
            Some(Ok(observation)) => (
                Some(observation.entries),
                RemoteStatus {
                    availability: RemoteAvailability::Available,
                    cursor: Some(hex(&observation.cursor.bytes)),
                    error: None,
                },
            ),
            Some(Err(error)) => (
                None,
                RemoteStatus {
                    availability: RemoteAvailability::Unavailable,
                    cursor: None,
                    error: Some(error.to_string()),
                },
            ),
            None => (
                None,
                RemoteStatus {
                    availability: if remote_error.is_some() {
                        RemoteAvailability::Unavailable
                    } else {
                        RemoteAvailability::NotChecked
                    },
                    cursor: None,
                    error: remote_error,
                },
            ),
        };
        let state_path = Path::new(&state_directory);
        let mut virtual_conflicts = Vec::new();
        let mut virtual_conflicts_by_path = BTreeMap::new();
        let mut mutated_artifacts = BTreeSet::new();
        if let Some(remote_entries) = &remote_entries {
            for artifact in &artifacts {
                if !artifact_changed(state_path, artifact) {
                    continue;
                }
                let id = artifact_conflict_id(&artifact.id);
                let remote = remote_entries.get(&artifact.origin);
                virtual_conflicts_by_path.insert(artifact.origin.clone(), id.clone());
                mutated_artifacts.insert(artifact.id.clone());
                virtual_conflicts.push(ConflictStatus {
                    id,
                    path: artifact.origin.clone(),
                    original_path: Some(artifact.origin.clone()),
                    desired_path: Some(artifact.origin.clone()),
                    local: ConflictSideStatus::RetainedArtifact,
                    remote: conflict_side_status(&conflict_remote_side(remote)),
                    local_artifact: Some(artifact.id.clone()),
                    remote_artifact: None,
                    remote_revision: remote
                        .and_then(|remote| remote.pin.revision())
                        .map(|revision| hex(&revision)),
                    error: "retained artifact changed after reconciliation".to_owned(),
                });
            }
        }

        let mut paths = BTreeSet::new();
        if let Some(baseline) = &baseline {
            paths.extend(baseline.remote.keys().cloned());
            paths.extend(baseline.local.keys().cloned());
        }
        if let Some(local) = &local_snapshot {
            paths.extend(local.nodes().keys().cloned());
        }
        if let Some(remote) = &remote_entries {
            paths.extend(remote.keys().cloned());
        }
        paths.extend(work.iter().map(|work| work.path.clone()));
        paths.extend(conflicts.iter().map(|conflict| conflict.path.clone()));
        paths.extend(
            conflicts
                .iter()
                .filter_map(|conflict| conflict.original_path.clone()),
        );
        paths.extend(
            conflicts
                .iter()
                .filter_map(|conflict| conflict.desired_path.clone()),
        );
        paths.extend(virtual_conflicts_by_path.keys().cloned());
        paths.remove("");

        let mut conflict_by_path = BTreeMap::new();
        for conflict in &conflicts {
            if let Some(desired) = conflict.desired_path.as_deref() {
                conflict_by_path.insert(desired, conflict);
            } else {
                conflict_by_path.insert(conflict.path.as_str(), conflict);
            }
        }
        let work_by_path = work
            .iter()
            .map(|work| (work.path.as_str(), work))
            .collect::<BTreeMap<_, _>>();
        let install_by_path = installs
            .iter()
            .map(|install| (install.path.as_str(), install))
            .collect::<BTreeMap<_, _>>();

        let mut entries = Vec::with_capacity(paths.len());
        let mut pending = 0_u64;
        let mut unknown = 0_u64;
        let mut failed = 0_u64;
        for path in paths {
            let local_node = local_snapshot.as_ref().and_then(|local| local.get(&path));
            let remote_entry = remote_entries.as_ref().and_then(|remote| remote.get(&path));
            let baseline_entry = baseline.as_ref().and_then(|baseline| {
                baseline.remote.get(&path).map(|remote| BaselineEntry {
                    remote: remote.clone(),
                    local: baseline.local.get(&path).copied(),
                })
            });
            let local_diff = local_changed(
                baseline_entry.as_ref().map(|entry| entry.remote.kind),
                baseline_entry
                    .as_ref()
                    .and_then(|entry| entry.local.as_ref()),
                local_node,
                executable,
            );
            let remote_diff = remote_changed(
                baseline_entry.as_ref().map(|entry| &entry.remote),
                remote_entry,
            );
            if let Some(local) = baseline_entry
                .as_ref()
                .and_then(|entry| entry.local.as_ref())
                && let Some(remote) = baseline_entry.as_ref().map(|entry| &entry.remote)
                && remote.kind == EntryKind::File
                && local.length != remote.length
            {
                return Err(Error::corrupt(format!(
                    "sync baseline pairs local length {} with remote length {} at {path:?}",
                    local.length, remote.length
                )));
            }
            if let Some(local) = baseline_entry
                .as_ref()
                .and_then(|entry| entry.local.as_ref())
                && let Some(remote) = baseline_entry.as_ref().map(|entry| &entry.remote)
                && remote.kind == EntryKind::File
                && let Some(remote_digest) = remote.digest
                && local.remote_digest != Some(remote_digest)
            {
                return Err(Error::corrupt(format!(
                    "sync baseline pairs local remote digest {:?} with remote digest {} at {path:?}",
                    local.remote_digest.map(|digest| hex(&digest)),
                    hex(&remote_digest)
                )));
            }
            let conflict = conflict_by_path.get(path.as_str()).copied();
            let virtual_conflict = virtual_conflicts_by_path.get(&path);
            let work = work_by_path.get(path.as_str()).copied();
            let intent = work.and_then(|work| intents.get(&work.id));
            let install = install_by_path.get(path.as_str()).copied();
            let open_intent = intent.is_some_and(|intent| intent.state != IntentState::Committed);
            let remote_completed = !open_intent && install.is_none() && !remote_diff;
            let state = if conflict.is_some() || virtual_conflict.is_some() {
                EntryState::Conflict
            } else if intent.is_some_and(|intent| intent.state == IntentState::Unknown) {
                unknown += 1;
                EntryState::Unknown
            } else if intent.is_some_and(|intent| intent.state == IntentState::Failed)
                || install.is_some_and(|install| install.state == InstallState::Failed)
            {
                failed += 1;
                EntryState::Failed
            } else if intent.is_some_and(|intent| {
                matches!(intent.state, IntentState::Prepared | IntentState::Retryable)
            }) || install.is_some_and(|install| {
                matches!(
                    install.state,
                    InstallState::Prepared | InstallState::Dispatched
                )
            }) {
                pending += 1;
                EntryState::LocalPending
            } else if local_diff && remote_diff {
                EntryState::Conflict
            } else if local_diff {
                pending += 1;
                if baseline_entry
                    .as_ref()
                    .and_then(|entry| entry.local.as_ref())
                    .zip(local_node)
                    .is_some_and(|(accepted, current)| local_is_superseded(accepted, current))
                {
                    EntryState::RemotePending
                } else {
                    EntryState::LocalPending
                }
            } else if remote_diff {
                pending += 1;
                EntryState::RemotePending
            } else {
                EntryState::InSync
            };
            entries.push(EntryStatus {
                path: path.clone(),
                original_path: conflict
                    .and_then(|conflict| conflict.original_path.clone())
                    .or_else(|| virtual_conflict.map(|_| path.clone())),
                desired_path: conflict
                    .and_then(|conflict| conflict.desired_path.clone())
                    .or_else(|| virtual_conflict.map(|_| path.clone())),
                state,
                local: local_evidence(local_node),
                remote: remote_evidence(remote_entry),
                baseline: BaselineEvidence {
                    local_digest: baseline_entry
                        .as_ref()
                        .and_then(|entry| entry.local.as_ref())
                        .map(|local| hex(&local.digest)),
                    local_length: baseline_entry
                        .as_ref()
                        .and_then(|entry| entry.local.as_ref())
                        .map(|local| local.length),
                    remote_identity: baseline_entry
                        .as_ref()
                        .map(|entry| hex(&entry.remote.pin.stable_id())),
                    remote_pin: baseline_entry
                        .as_ref()
                        .map(|entry| remote_pin_text(&entry.remote)),
                },
                intent: intent_status(work, intent),
                outcome: OutcomeStatus {
                    state: match state {
                        EntryState::InSync => OutcomeState::RemoteCompleted,
                        EntryState::LocalPending if remote_completed => {
                            OutcomeState::RemoteCompleted
                        }
                        EntryState::LocalPending | EntryState::RemotePending => {
                            OutcomeState::Pending
                        }
                        EntryState::Conflict => OutcomeState::Conflict,
                        EntryState::Unknown => OutcomeState::Unknown,
                        EntryState::Failed => OutcomeState::Failed,
                    },
                    error: intent.and_then(|intent| {
                        intent
                            .failure
                            .as_ref()
                            .map(|failure| failure.message.clone())
                    }),
                },
                conflict: conflict
                    .map(|conflict| conflict.id.clone())
                    .or_else(|| virtual_conflict.cloned()),
            });
        }
        let mut conflict_status = conflicts.iter().map(conflict_status).collect::<Vec<_>>();
        conflict_status.extend(virtual_conflicts);
        conflict_status.sort_by_key(|conflict| (conflict.path.clone(), conflict.id.clone()));
        let mut artifact_status = artifacts
            .iter()
            .map(|artifact| ArtifactStatus {
                id: artifact.id.clone(),
                origin: artifact.origin.clone(),
                kind: artifact.kind.as_str().to_owned(),
                phase: artifact.phase.as_str().to_owned(),
                release_event: artifact.release_event.as_str().to_owned(),
                path: Path::new(&state_directory)
                    .join("artifacts")
                    .join(&artifact.name)
                    .display()
                    .to_string(),
                exportable: artifact.kind != super::store::ArtifactKind::QuarantinedDirectory,
                length: artifact.length,
                error: if mutated_artifacts.contains(&artifact.id) {
                    Some("retained artifact changed after reconciliation".to_owned())
                } else {
                    artifact.error.clone()
                },
            })
            .collect::<Vec<_>>();
        artifact_status.sort_by_key(|artifact| artifact.id.clone());
        let retained_bytes = artifacts.iter().map(|artifact| artifact.length).sum();
        let in_sync = remote_status.availability == RemoteAvailability::Available
            && local_status.scan == LocalScanState::Clean
            && pending == 0
            && unknown == 0
            && failed == 0
            && conflict_status.is_empty()
            && entries
                .iter()
                .all(|entry| entry.state == EntryState::InSync);
        Ok(StatusDocument {
            schema: super::adapter::SYNC_SCHEMA,
            model: binding.model,
            binding: BindingStatus {
                state: BindingState::Bound,
                state_directory: state_directory.clone(),
                root: Some(binding.root.display().to_string()),
                namespace: Some(binding.namespace.clone()),
                same_filesystem: Some(binding.root_identity.dev == binding.state_identity.dev),
                error: None,
            },
            remote: remote_status,
            local: local_status,
            summary: StatusSummary {
                in_sync,
                pending,
                unknown,
                conflicts: conflict_status.len(),
                failed,
                retained_artifacts: artifacts.len() as u64,
                retained_bytes,
            },
            entries,
            conflicts: conflict_status,
            artifacts: artifact_status,
        })
    }

    pub fn export_conflict_side(
        &self,
        conflict_id: &str,
        side: &str,
        output: &mut dyn Write,
    ) -> Result<()> {
        let conflict = self
            .local
            .store()
            .conflict(conflict_id)?
            .ok_or_else(|| Error::invalid("unknown conflict"))?;
        match side {
            "local" => {
                if let Some(id) = conflict.local_artifact {
                    return self.export_artifact(&id, output);
                }
                let path = conflict.desired_path.as_deref().unwrap_or(&conflict.path);
                let mut file = self.local.open_local_file(path)?;
                std::io::copy(&mut file, output)
                    .map(|_| ())
                    .map_err(|error| {
                        Error::io(
                            "export live local conflict",
                            &self.local.root().join(path),
                            error,
                        )
                    })
            }
            "remote" => {
                let id = conflict
                    .remote_artifact
                    .ok_or_else(|| Error::invalid("conflict has no retained remote bytes"))?;
                self.export_artifact(&id, output)
            }
            other => Err(Error::invalid(format!("unknown conflict side {other:?}"))),
        }
    }

    pub async fn resolve(&mut self, id: &str, resolution: Resolution) -> Result<RunOutcome> {
        self.local
            .store()
            .baseline()?
            .ok_or_else(|| Error::invalid("sync state has no common baseline"))?;
        let observation = self.remote.observe(None).await?;
        let local = self.local.scan()?;
        let mut remote = observation.entries.clone();
        let mut outcome = RunOutcome::default();
        if let Some(conflict) = self.local.store().conflict(id)? {
            let path = conflict
                .desired_path
                .clone()
                .unwrap_or_else(|| conflict.path.clone());
            match resolution {
                Resolution::KeepRemote => {
                    self.resolve_keep_remote(&conflict, &path, &mut remote, &local, &mut outcome)
                        .await?;
                }
                Resolution::KeepLocal => {
                    self.resolve_keep_local(&conflict, &path, &mut remote, &local, &mut outcome)
                        .await?;
                }
                Resolution::Content(bytes) => {
                    self.resolve_content(&path, bytes, &mut remote, &local, &mut outcome)
                        .await?;
                }
            }
            if self.local.store().conflict(id)?.is_some() {
                self.local.store().remove_conflict(id)?;
                for artifact_id in [
                    conflict.local_artifact.as_deref(),
                    conflict.remote_artifact.as_deref(),
                ]
                .into_iter()
                .flatten()
                {
                    if let Some(artifact) = self
                        .local
                        .store()
                        .artifacts()?
                        .into_iter()
                        .find(|artifact| artifact.id == artifact_id)
                        && self.local.release_artifact(&artifact)?
                    {
                        self.local.store().remove_artifact(&artifact.id)?;
                    }
                }
            }
        } else if let Some(work) = self
            .local
            .store()
            .work()?
            .into_iter()
            .find(|work| work.id == id)
        {
            self.discard_work(&work)?;
            match resolution {
                Resolution::KeepRemote => {
                    self.resolve_work_keep_remote(&work, &mut remote, &mut outcome)
                        .await?;
                }
                Resolution::KeepLocal => {
                    self.resolve_work_keep_local(&work, &mut remote, &local, &mut outcome)
                        .await?;
                }
                Resolution::Content(bytes) => {
                    self.resolve_content(&work.path, bytes, &mut remote, &local, &mut outcome)
                        .await?;
                }
            }
        } else if let Some(artifact) =
            self.local
                .store()
                .artifacts()?
                .into_iter()
                .find(|artifact| {
                    artifact_conflict_id(&artifact.id) == id
                        && artifact_changed(self.local.state(), artifact)
                })
        {
            let path = artifact.origin.clone();
            match resolution {
                Resolution::KeepLocal => {
                    let artifact_path = self.local.artifact_path(&artifact);
                    let mut file = self.local.open_artifact(&artifact.name)?;
                    let mut bytes = Vec::new();
                    file.read_to_end(&mut bytes).map_err(|error| {
                        Error::io("read retained conflict artifact", &artifact_path, error)
                    })?;
                    self.resolve_content(&path, bytes, &mut remote, &local, &mut outcome)
                        .await?;
                }
                Resolution::KeepRemote => {
                    if let Some(entry) = remote.get(&path).cloned() {
                        let expected = local
                            .get(&path)
                            .filter(|node| !node.is_directory())
                            .map(LocalFingerprint::from_node)
                            .transpose()?;
                        if entry.kind == EntryKind::File {
                            self.materialize_file(&entry, expected).await?;
                        } else {
                            self.materialize_remote_subtree(
                                &path,
                                &mut remote,
                                &local,
                                &mut outcome,
                            )
                            .await?;
                        }
                        remote.insert(path.clone(), entry);
                        outcome.materialized_remote += 1;
                    }
                }
                Resolution::Content(bytes) => {
                    self.resolve_content(&path, bytes, &mut remote, &local, &mut outcome)
                        .await?;
                }
            }
            if self.local.release_artifact(&artifact)? {
                self.local.store().remove_artifact(&artifact.id)?;
            }
        } else {
            return Err(Error::invalid(format!(
                "sync status has no conflict or work identity {id:?}"
            )));
        }
        let mut final_outcome = self.run().await?;
        final_outcome.published_local += outcome.published_local;
        final_outcome.materialized_remote += outcome.materialized_remote;
        Ok(final_outcome)
    }

    fn discard_work(&self, work: &WorkRecord) -> Result<()> {
        self.local.store().remove_work(&work.id)?;
        self.local.store().remove_intent(&work.id)?;
        if let Some(id) = work.source_artifact.as_deref()
            && let Some(artifact) = self
                .local
                .store()
                .artifacts()?
                .into_iter()
                .find(|artifact| artifact.id == id)
            && self.local.release_artifact(&artifact)?
        {
            self.local.store().remove_artifact(&artifact.id)?;
        }
        Ok(())
    }

    async fn resolve_keep_remote(
        &mut self,
        conflict: &ConflictRecord,
        path: &str,
        remote: &mut BTreeMap<String, RemoteEntry>,
        local: &LocalSnapshot,
        outcome: &mut RunOutcome,
    ) -> Result<()> {
        let entry = conflict
            .remote
            .remote
            .clone()
            .or_else(|| remote.get(path).cloned());
        match entry {
            Some(entry) if entry.kind == EntryKind::File => {
                let expected = local
                    .get(path)
                    .filter(|node| !node.is_directory())
                    .map(LocalFingerprint::from_node)
                    .transpose()?;
                self.materialize_file(&entry, expected).await?;
                remote.insert(path.to_owned(), entry);
                outcome.materialized_remote += 1;
            }
            Some(entry) => {
                self.materialize_remote_subtree(path, remote, local, outcome)
                    .await?;
                let _ = entry;
            }
            None => {
                if let Some(node) = local.get(path) {
                    if node.is_directory() {
                        self.local.quarantine(
                            path,
                            local,
                            "conflict resolution kept the remote deletion",
                            conflict.remote_revision,
                        )?;
                    } else {
                        let expected = LocalFingerprint::from_node(node)?;
                        self.delete_local_file(path, expected).await?;
                    }
                }
                self.local.store().remove_baseline_subtree(path)?;
                remote.remove(path);
                outcome.deleted_local += 1;
            }
        }
        Ok(())
    }

    async fn materialize_remote_subtree(
        &mut self,
        path: &str,
        remote: &mut BTreeMap<String, RemoteEntry>,
        local: &LocalSnapshot,
        outcome: &mut RunOutcome,
    ) -> Result<()> {
        let prefix = format!("{path}/");
        let mut entries = remote
            .iter()
            .filter(|(entry_path, _)| {
                entry_path.as_str() == path || entry_path.starts_with(&prefix)
            })
            .map(|(path, entry)| (path.clone(), entry.clone()))
            .collect::<Vec<_>>();
        entries.sort_by_key(|(path, _)| (path.matches('/').count(), path.clone()));
        let mut current = local.clone();
        for (entry_path, entry) in entries {
            match entry.kind {
                EntryKind::Directory => {
                    if current.get(&entry_path).is_none() {
                        self.local.create_directory_local(&entry_path)?;
                        refresh_local_path(&self.local, &entry_path, &mut current)?;
                    }
                    self.local
                        .store()
                        .save_baseline_entry(&entry_path, &entry, None)?;
                    outcome.materialized_remote += 1;
                }
                EntryKind::File => {
                    let expected = current
                        .get(&entry_path)
                        .filter(|node| !node.is_directory())
                        .map(LocalFingerprint::from_node)
                        .transpose()?;
                    self.materialize_file(&entry, expected).await?;
                    refresh_local_path(&self.local, &entry_path, &mut current)?;
                    remote.insert(entry_path, entry);
                    outcome.materialized_remote += 1;
                }
            }
        }
        Ok(())
    }

    async fn resolve_keep_local(
        &mut self,
        conflict: &ConflictRecord,
        path: &str,
        remote: &mut BTreeMap<String, RemoteEntry>,
        local: &LocalSnapshot,
        outcome: &mut RunOutcome,
    ) -> Result<()> {
        match local.get(path) {
            None => {
                if let Some(baseline) = remote.get(path).cloned() {
                    let outcomes = self
                        .publish_request(
                            RemoteRequest::Remove {
                                path: path.to_owned(),
                                baseline,
                            },
                            None,
                            None,
                            Some(conflict.id.clone()),
                        )
                        .await?;
                    self.update_ledger(&outcomes, remote);
                    outcome.published_local += 1;
                }
            }
            Some(node) if node.is_directory() => {
                self.publish_local_subtree(path, local, remote, &conflict.id, outcome)
                    .await?;
            }
            Some(node) => {
                let accepted = LocalFingerprint::from_node(node)?;
                let request = match remote.get(path).cloned() {
                    Some(baseline) => RemoteRequest::ReplaceFile {
                        path: path.to_owned(),
                        source: PathBuf::new(),
                        baseline,
                    },
                    None => {
                        let parent = remote.get(parent_path(path)).cloned();
                        if parent.is_none() && self.remote.capabilities().directories {
                            return Err(Error::conflict(format!(
                                "remote resolution parent {:?} is missing",
                                parent_path(path)
                            )));
                        }
                        RemoteRequest::CreateFile {
                            path: path.to_owned(),
                            source: PathBuf::new(),
                            executable: node.executable,
                            parent,
                        }
                    }
                };
                let outcomes = self
                    .publish_request(
                        request,
                        Some(node),
                        Some(accepted),
                        Some(conflict.id.clone()),
                    )
                    .await?;
                self.update_ledger(&outcomes, remote);
                if outcomes
                    .iter()
                    .any(|outcome| matches!(outcome, RemoteOutcome::Applied { .. }))
                    && let Some(original) = conflict.original_path.as_deref()
                    && original != path
                {
                    self.local.store().remove_baseline_path(original)?;
                }
                outcome.published_local += 1;
            }
        }
        Ok(())
    }

    async fn publish_local_subtree(
        &mut self,
        path: &str,
        local: &LocalSnapshot,
        remote: &mut BTreeMap<String, RemoteEntry>,
        conflict: &str,
        outcome: &mut RunOutcome,
    ) -> Result<()> {
        let prefix = format!("{path}/");
        let mut entries = local
            .nodes()
            .iter()
            .filter(|(entry_path, _)| {
                entry_path.as_str() == path || entry_path.starts_with(&prefix)
            })
            .map(|(path, node)| (path.clone(), node.clone()))
            .collect::<Vec<_>>();
        entries.sort_by_key(|(path, _)| (path.matches('/').count(), path.clone()));
        for (entry_path, node) in entries {
            if remote.get(&entry_path).is_some() {
                continue;
            }
            if node.is_directory() {
                let parent = remote
                    .get(parent_path(&entry_path))
                    .cloned()
                    .ok_or_else(|| Error::conflict("remote directory parent is missing"))?;
                let outcomes = self
                    .publish_request(
                        RemoteRequest::CreateDirectory {
                            path: entry_path,
                            parent,
                        },
                        None,
                        None,
                        Some(conflict.to_owned()),
                    )
                    .await?;
                self.update_ledger(&outcomes, remote);
            } else {
                let parent = remote.get(parent_path(&entry_path)).cloned();
                if parent.is_none() && self.remote.capabilities().directories {
                    return Err(Error::conflict("remote file parent is missing"));
                }
                let accepted = LocalFingerprint::from_node(&node)?;
                let outcomes = self
                    .publish_request(
                        RemoteRequest::CreateFile {
                            path: entry_path,
                            source: PathBuf::new(),
                            executable: node.executable,
                            parent,
                        },
                        Some(&node),
                        Some(accepted),
                        Some(conflict.to_owned()),
                    )
                    .await?;
                self.update_ledger(&outcomes, remote);
            }
            outcome.published_local += 1;
        }
        Ok(())
    }

    async fn resolve_content(
        &mut self,
        path: &str,
        bytes: Vec<u8>,
        remote: &mut BTreeMap<String, RemoteEntry>,
        local: &LocalSnapshot,
        outcome: &mut RunOutcome,
    ) -> Result<()> {
        if let Some(node) = local.get(path).filter(|node| !node.is_directory()) {
            let artifact = self.local.retain_bytes(path, &bytes)?;
            let mut accepted = LocalFingerprint::from_node(node)?;
            accepted.length = bytes.len() as u64;
            accepted.digest = *blake3::hash(&bytes).as_bytes();
            let mut request = match remote.get(path).cloned() {
                Some(baseline) => RemoteRequest::ReplaceFile {
                    path: path.to_owned(),
                    source: self.local.artifact_path(&artifact),
                    baseline,
                },
                None => {
                    let parent = remote.get(parent_path(path)).cloned();
                    if parent.is_none() && self.remote.capabilities().directories {
                        return Err(Error::conflict("remote resolution parent is missing"));
                    }
                    RemoteRequest::CreateFile {
                        path: path.to_owned(),
                        source: self.local.artifact_path(&artifact),
                        executable: node.executable,
                        parent,
                    }
                }
            };
            request.set_source(self.local.artifact_path(&artifact));
            let frozen = self.remote.freeze(request).await?;
            let id = hex(&frozen.id);
            let intent = IntentRecord {
                id: id.clone(),
                request_id: frozen.id,
                request_digest: frozen.digest,
                request: frozen.bytes.clone(),
                commit_id: None,
                state: IntentState::Prepared,
                receipt_revision: None,
                receipt_ordinal: None,
                failure: None,
            };
            self.local.store().insert_intent(&intent)?;
            self.local.store_mut().insert_work(&WorkRecord {
                seq: 0,
                id: id.clone(),
                path: path.to_owned(),
                original_path: None,
                desired_path: Some(path.to_owned()),
                operation: frozen.operation,
                accepted_local: Some(accepted),
                baseline: None,
                request_id: frozen.id,
                source_artifact: Some(artifact.id.clone()),
                conflict: None,
            })?;
            let outcomes = self
                .publish_prepared(
                    vec![PreparedRemoteRequest {
                        request: frozen,
                        source: Some(self.local.artifact_path(&artifact)),
                    }],
                    false,
                )
                .await?;
            self.update_ledger(&outcomes, remote);
            outcome.published_local += 1;
        } else {
            return Err(Error::unsupported(
                "content resolution currently requires an existing local file",
            ));
        }
        Ok(())
    }

    async fn resolve_work_keep_remote(
        &mut self,
        work: &WorkRecord,
        remote: &mut BTreeMap<String, RemoteEntry>,
        outcome: &mut RunOutcome,
    ) -> Result<()> {
        if let Some(entry) = remote.get(&work.path).cloned() {
            let local = self.local.scan()?;
            let expected = local
                .get(&work.path)
                .filter(|node| !node.is_directory())
                .map(LocalFingerprint::from_node)
                .transpose()?;
            if entry.kind == EntryKind::File {
                self.materialize_file(&entry, expected).await?;
            } else {
                self.materialize_remote_subtree(&work.path, remote, &local, outcome)
                    .await?;
            }
            remote.insert(work.path.clone(), entry);
            outcome.materialized_remote += 1;
        } else if let Some(local) = self.local.observe_path(&work.path)? {
            if local.is_directory() {
                let snapshot = self.local.scan()?;
                self.local.quarantine(
                    &work.path,
                    &snapshot,
                    "explicit resolution kept remote deletion",
                    None,
                )?;
            } else {
                let expected = LocalFingerprint::from_node(&local)?;
                self.delete_local_file(&work.path, expected).await?;
            }
        }
        Ok(())
    }

    async fn resolve_work_keep_local(
        &mut self,
        work: &WorkRecord,
        remote: &mut BTreeMap<String, RemoteEntry>,
        local: &LocalSnapshot,
        outcome: &mut RunOutcome,
    ) -> Result<()> {
        if let Some(node) = local.get(&work.path).filter(|node| !node.is_directory()) {
            let accepted = LocalFingerprint::from_node(node)?;
            let request = match remote.get(&work.path).cloned() {
                Some(baseline) => RemoteRequest::ReplaceFile {
                    path: work.path.clone(),
                    source: PathBuf::new(),
                    baseline,
                },
                None => {
                    let parent = remote.get(parent_path(&work.path)).cloned();
                    if parent.is_none() && self.remote.capabilities().directories {
                        return Err(Error::conflict("remote resolution parent is missing"));
                    }
                    RemoteRequest::CreateFile {
                        path: work.path.clone(),
                        source: PathBuf::new(),
                        executable: node.executable,
                        parent,
                    }
                }
            };
            let outcomes = self
                .publish_request(request, Some(node), Some(accepted), None)
                .await?;
            self.update_ledger(&outcomes, remote);
            outcome.published_local += 1;
        } else if work.path.is_empty() {
            return Err(Error::invalid("the synchronized root cannot be published"));
        } else if let Some(baseline) = remote.get(&work.path).cloned() {
            let outcomes = self
                .publish_request(
                    RemoteRequest::Remove {
                        path: work.path.clone(),
                        baseline,
                    },
                    None,
                    None,
                    None,
                )
                .await?;
            self.update_ledger(&outcomes, remote);
            outcome.published_local += 1;
        }
        Ok(())
    }
}

enum Decision {
    PublishFile {
        path: String,
        local: LocalNode,
        baseline: Option<RemoteEntry>,
    },
    PublishDirectory {
        path: String,
    },
    PublishDelete {
        path: String,
    },
    PublishExecutable {
        path: String,
        baseline: RemoteEntry,
        executable: bool,
    },
    PublishRename {
        from: String,
        to: String,
        baseline: RemoteEntry,
    },
    MaterializeFile {
        entry: RemoteEntry,
        expected: Option<LocalFingerprint>,
    },
    MaterializeDirectory {
        entry: RemoteEntry,
    },
    DeleteLocalFile {
        entry: RemoteEntry,
        expected: LocalFingerprint,
    },
    DeleteLocalDirectory {
        path: String,
        expected: LocalSnapshot,
    },
    RemoveBaseline {
        path: String,
    },
    AcceptRemote {
        path: String,
        entry: RemoteEntry,
        local: Option<LocalNode>,
    },
    ApplyLocalRename {
        from: String,
        to: String,
        entry: RemoteEntry,
    },
    AcceptRename {
        from: String,
        to: String,
        entry: RemoteEntry,
    },
    Conflict {
        path: String,
        original_path: Option<String>,
        desired_path: Option<String>,
        local: Option<LocalNode>,
        remote: Option<RemoteEntry>,
        message: String,
    },
}

fn plan(
    baseline: &BaselineRecord,
    local: &LocalSnapshot,
    remote: &BTreeMap<String, RemoteEntry>,
    blocked: &BTreeSet<String>,
    capabilities: super::adapter::RemoteCapabilities,
) -> Result<Vec<Decision>> {
    let local_snapshot = local;
    let mut handled = BTreeSet::new();
    let mut decisions = Vec::new();
    let mut conflict_roots = BTreeSet::new();
    let mut deleted_local_roots = BTreeSet::new();
    let mut remote_deleted_dirs = BTreeSet::new();
    if capabilities.rename {
        plan_file_moves(
            baseline,
            local,
            remote,
            blocked,
            &mut handled,
            &mut decisions,
        )?;
    }

    if capabilities.directories {
        let mut directories = baseline
            .remote
            .iter()
            .filter(|(path, entry)| {
                !path.is_empty()
                    && entry.kind == EntryKind::Directory
                    && !remote.contains_key(*path)
            })
            .map(|(path, _)| path.clone())
            .collect::<Vec<_>>();
        directories.sort_by_key(|path| (path.matches('/').count(), path.clone()));
        for path in directories {
            remote_deleted_dirs.insert(path.clone());
            if deleted_local_roots
                .iter()
                .any(|root| path.starts_with(&format!("{root}/")))
            {
                continue;
            }
            if remote
                .keys()
                .any(|candidate| candidate.starts_with(&format!("{path}/")))
                || !local.get(&path).is_some_and(LocalNode::is_directory)
                || !local_subtree_matches_baseline(baseline, local, &path)
            {
                continue;
            }
            decisions.push(Decision::DeleteLocalDirectory {
                path: path.clone(),
                expected: local_snapshot.clone(),
            });
            deleted_local_roots.insert(path);
        }
    }

    let mut paths = BTreeSet::new();
    paths.extend(baseline.remote.keys().cloned());
    paths.extend(baseline.local.keys().cloned());
    paths.extend(local.nodes().keys().cloned());
    paths.extend(remote.keys().cloned());
    paths.remove("");
    paths.retain(|path| !handled.contains(path) && !is_blocked(path, blocked));

    let mut deletes = Vec::new();
    let mut ascending = paths.into_iter().collect::<Vec<_>>();
    ascending.sort_by_key(|path| (path.matches('/').count(), path.clone()));
    for path in ascending {
        if conflict_roots.iter().any(|root| {
            path == *root
                || path
                    .strip_prefix(root)
                    .is_some_and(|suffix| suffix.starts_with('/'))
        }) {
            continue;
        }
        if deleted_local_roots
            .iter()
            .any(|root| path == *root || path.starts_with(&format!("{root}/")))
        {
            continue;
        }
        let local_node = local.get(&path);
        if let Some(deleted) = remote_deleted_dirs
            .iter()
            .find(|deleted| path.starts_with(&format!("{deleted}/")))
        {
            if let Some(local) = local_node {
                decisions.push(Decision::Conflict {
                    path: path.clone(),
                    original_path: Some(path.clone()),
                    desired_path: Some(path.clone()),
                    local: Some(local.clone()),
                    remote: None,
                    message: format!("remote parent directory {deleted:?} was deleted"),
                });
            }
            continue;
        }
        if remote_deleted_dirs.contains(&path) {
            continue;
        }
        let remote_entry = remote.get(&path);
        let baseline_remote = baseline.remote.get(&path);
        let baseline_local = baseline.local.get(&path);
        let local_directory = local_node.is_some_and(LocalNode::is_directory);
        let remote_directory = remote_entry.is_some_and(|entry| entry.kind == EntryKind::Directory);
        let baseline_directory =
            baseline_remote.is_some_and(|entry| entry.kind == EntryKind::Directory);
        if local_directory && remote_entry.is_some_and(|entry| entry.kind == EntryKind::File)
            || local_node.is_some_and(|node| !node.is_directory()) && remote_directory
            || baseline_remote.is_some()
                && (local_directory || remote_directory) != baseline_directory
        {
            if local_directory || remote_directory || baseline_directory {
                conflict_roots.insert(path.clone());
            }
            decisions.push(Decision::Conflict {
                path: path.clone(),
                original_path: Some(path.clone()),
                desired_path: Some(path.clone()),
                local: local_node.cloned(),
                remote: remote_entry.cloned(),
                message: "file and directory kinds cannot be reconciled automatically".to_owned(),
            });
            continue;
        }
        let local_change = local_changed(
            baseline_remote.map(|entry| entry.kind),
            baseline_local,
            local_node,
            capabilities.executable,
        );
        let remote_change = remote_changed(baseline_remote, remote_entry);
        if !local_change && !remote_change {
            continue;
        }
        if !capabilities.directories && (local_directory || remote_directory) {
            continue;
        }
        match (
            local_node,
            remote_entry.cloned(),
            baseline_remote.cloned(),
            local_change,
            remote_change,
        ) {
            (None, None, Some(_), _, _) => deletes.push(Decision::RemoveBaseline { path }),
            (None, Some(_), Some(_), _, false) => deletes.push(Decision::PublishDelete { path }),
            (None, Some(remote), Some(_), _, true) => decisions.push(Decision::Conflict {
                path: path.clone(),
                original_path: Some(path.clone()),
                desired_path: Some(path.clone()),
                local: None,
                remote: Some(remote),
                message: "local deletion races a remote edit".to_owned(),
            }),
            (Some(local), None, Some(_), false, true) => {
                if local.is_directory() {
                    deletes.push(Decision::DeleteLocalDirectory {
                        path: path.clone(),
                        expected: local_snapshot.clone(),
                    });
                } else if let Some(expected) = baseline_local {
                    deletes.push(Decision::DeleteLocalFile {
                        entry: baseline_remote
                            .cloned()
                            .expect("remote deletion requires a baseline"),
                        expected: *expected,
                    });
                }
            }
            (Some(local), None, Some(_), true, true) => decisions.push(Decision::Conflict {
                path: path.clone(),
                original_path: Some(path.clone()),
                desired_path: Some(path.clone()),
                local: Some(local.clone()),
                remote: None,
                message: "local edit races a remote deletion".to_owned(),
            }),
            (Some(local), Some(remote), None, true, true) => {
                if local.is_directory() && remote.kind == EntryKind::Directory {
                    decisions.push(Decision::AcceptRemote {
                        path,
                        entry: remote.clone(),
                        local: Some(local.clone()),
                    });
                } else {
                    decisions.push(Decision::Conflict {
                        path: path.clone(),
                        original_path: None,
                        desired_path: Some(path.clone()),
                        local: Some(local.clone()),
                        remote: Some(remote.clone()),
                        message: "local and remote both added different entries".to_owned(),
                    });
                }
            }
            (Some(local), Some(remote), Some(_), true, true) => {
                if local.is_directory() && remote.kind == EntryKind::Directory {
                    decisions.push(Decision::AcceptRemote {
                        path,
                        entry: remote.clone(),
                        local: Some(local.clone()),
                    });
                } else {
                    decisions.push(Decision::Conflict {
                        path: path.clone(),
                        original_path: Some(path.clone()),
                        desired_path: Some(path.clone()),
                        local: Some(local.clone()),
                        remote: Some(remote.clone()),
                        message: "local and remote changed from the same baseline".to_owned(),
                    });
                }
            }
            (Some(local), None, None, true, _) => {
                if local.is_directory() {
                    decisions.push(Decision::PublishDirectory { path });
                } else {
                    decisions.push(Decision::PublishFile {
                        path,
                        local: local.clone(),
                        baseline: baseline_remote.cloned(),
                    });
                }
            }
            (Some(local), Some(remote), Some(baseline_remote), true, false) => {
                if local.is_directory() || remote.kind == EntryKind::Directory {
                    decisions.push(Decision::AcceptRemote {
                        path,
                        entry: remote.clone(),
                        local: Some(local.clone()),
                    });
                } else if baseline_local
                    .is_some_and(|accepted| local_is_superseded(accepted, local))
                {
                    decisions.push(Decision::MaterializeFile {
                        entry: remote.clone(),
                        expected: Some(LocalFingerprint::from_node(local)?),
                    });
                } else if baseline_local.is_some_and(|baseline_local| {
                    baseline_local.length == local_length(local)
                        && baseline_local.digest == local_digest(local)
                        && baseline_local.executable != local.executable
                }) {
                    decisions.push(Decision::PublishExecutable {
                        path,
                        baseline: baseline_remote,
                        executable: local.executable,
                    });
                } else {
                    decisions.push(Decision::PublishFile {
                        path,
                        local: local.clone(),
                        baseline: Some(baseline_remote),
                    });
                }
            }
            (Some(local), Some(remote), Some(_), false, true) => {
                decisions.push(Decision::MaterializeFile {
                    entry: remote.clone(),
                    expected: baseline_local.copied(),
                });
                let _ = local;
            }
            (None, Some(remote), None, _, true) => {
                if remote.kind == EntryKind::Directory {
                    decisions.push(Decision::MaterializeDirectory {
                        entry: remote.clone(),
                    });
                } else {
                    decisions.push(Decision::MaterializeFile {
                        entry: remote.clone(),
                        expected: None,
                    });
                }
            }
            _ => {}
        }
    }
    deletes.sort_by_key(|decision| match decision {
        Decision::PublishDelete { path } => {
            (std::cmp::Reverse(path.matches('/').count()), path.clone())
        }
        Decision::DeleteLocalFile { entry, .. } => (
            std::cmp::Reverse(entry.path.matches('/').count()),
            entry.path.clone(),
        ),
        Decision::DeleteLocalDirectory { path, .. } => {
            (std::cmp::Reverse(path.matches('/').count()), path.clone())
        }
        Decision::RemoveBaseline { path } => {
            (std::cmp::Reverse(path.matches('/').count()), path.clone())
        }
        _ => (std::cmp::Reverse(0), String::new()),
    });
    decisions.extend(deletes);
    Ok(decisions)
}

fn plan_file_moves(
    baseline: &BaselineRecord,
    local: &LocalSnapshot,
    remote: &BTreeMap<String, RemoteEntry>,
    blocked: &BTreeSet<String>,
    handled: &mut BTreeSet<String>,
    decisions: &mut Vec<Decision>,
) -> Result<()> {
    let mut local_moves = BTreeMap::new();
    let mut local_by_identity = BTreeMap::new();
    for (path, fingerprint) in &baseline.local {
        local_by_identity.insert(fingerprint.identity(), path.clone());
    }
    for (path, node) in local.nodes() {
        if node.is_directory() {
            continue;
        }
        if let Some(old) = local_by_identity
            .get(&node.identity)
            .filter(|old| *old != path)
        {
            local_moves.insert(old.clone(), path.clone());
        }
    }
    let mut remote_by_identity = BTreeMap::new();
    for (path, entry) in &baseline.remote {
        if entry.kind == EntryKind::File {
            remote_by_identity.insert(entry.pin.stable_id(), path.clone());
        }
    }
    let mut remote_moves = BTreeMap::new();
    for (path, entry) in remote {
        if entry.kind != EntryKind::File {
            continue;
        }
        if let Some(old) = remote_by_identity
            .get(&entry.pin.stable_id())
            .filter(|old| *old != path)
        {
            remote_moves.insert(old.clone(), path.clone());
        }
    }
    let mut sources = BTreeSet::new();
    sources.extend(local_moves.keys().cloned());
    sources.extend(remote_moves.keys().cloned());
    for source in sources {
        if blocked.contains(&source) {
            continue;
        }
        let local_destination = local_moves.get(&source);
        let remote_destination = remote_moves.get(&source);
        match (local_destination, remote_destination) {
            (Some(local_to), Some(remote_to)) if local_to == remote_to => {
                if let Some(entry) = remote.get(remote_to).cloned() {
                    decisions.push(Decision::AcceptRename {
                        from: source.clone(),
                        to: remote_to.clone(),
                        entry,
                    });
                    handled.insert(source.clone());
                    handled.insert(remote_to.clone());
                }
            }
            (Some(local_to), clone @ (Some(_) | None)) => {
                let destination_parent = parent_path(local_to);
                if !destination_parent.is_empty() && !remote.contains_key(destination_parent) {
                    let _ = clone;
                    continue;
                }
                let remote_unchanged = baseline
                    .remote
                    .get(&source)
                    .zip(remote.get(&source))
                    .is_some_and(|(baseline, observed)| baseline.same_state(observed));
                if remote_unchanged && remote.get(local_to).is_none() {
                    if let Some(baseline) = baseline.remote.get(&source).cloned() {
                        decisions.push(Decision::PublishRename {
                            from: source.clone(),
                            to: local_to.clone(),
                            baseline,
                        });
                        handled.insert(source.clone());
                        handled.insert(local_to.clone());
                    }
                } else {
                    decisions.push(Decision::Conflict {
                        path: source.clone(),
                        original_path: Some(source.clone()),
                        desired_path: Some(local_to.clone()),
                        local: local.get(local_to).cloned(),
                        remote: remote.get(&source).cloned(),
                        message: "local rename races a remote namespace change".to_owned(),
                    });
                    handled.insert(source.clone());
                    handled.insert(local_to.clone());
                }
                let _ = clone;
            }
            (None, Some(remote_to)) => {
                let destination_parent = parent_path(remote_to);
                if !destination_parent.is_empty() && local.get(destination_parent).is_none() {
                    continue;
                }
                let local_unchanged = baseline
                    .local
                    .get(&source)
                    .zip(local.get(&source))
                    .is_some_and(|(baseline, observed)| {
                        baseline.same_content(
                            local_length(observed),
                            local_digest(observed),
                            observed.executable,
                        )
                    });
                if local_unchanged && local.get(remote_to).is_none() {
                    if let Some(entry) = remote.get(remote_to).cloned() {
                        decisions.push(Decision::ApplyLocalRename {
                            from: source.clone(),
                            to: remote_to.clone(),
                            entry,
                        });
                        handled.insert(source.clone());
                        handled.insert(remote_to.clone());
                    }
                } else {
                    decisions.push(Decision::Conflict {
                        path: source.clone(),
                        original_path: Some(source.clone()),
                        desired_path: Some(source.clone()),
                        local: local.get(&source).cloned(),
                        remote: None,
                        message: "remote rename races a local namespace change".to_owned(),
                    });
                    handled.insert(source.clone());
                }
            }
            (None, None) => {}
        }
    }
    Ok(())
}

fn validate_unsupported_directories(
    directories: bool,
    local: &LocalSnapshot,
    remote: &RemoteObservation,
    pending_paths: &BTreeSet<String>,
) -> Result<()> {
    if directories {
        return Ok(());
    }
    let local_files = local
        .nodes()
        .iter()
        .filter(|(_, node)| !node.is_directory())
        .map(|(path, _)| path.clone())
        .collect::<BTreeSet<_>>();
    let remote_files = remote
        .entries
        .iter()
        .filter(|(_, entry)| entry.kind == EntryKind::File)
        .map(|(path, _)| path.clone())
        .collect::<BTreeSet<_>>();
    let unsupported_local = local
        .nodes()
        .values()
        .filter(|node| {
            node.is_directory()
                && !node.path.is_empty()
                && !has_descendant(&local_files, &node.path)
                && !has_descendant(pending_paths, &node.path)
        })
        .map(|node| format!("local directory {:?}", node.path))
        .collect::<Vec<_>>();
    let unsupported_remote = remote
        .entries
        .values()
        .filter(|entry| {
            entry.kind == EntryKind::Directory
                && !entry.path.is_empty()
                && !has_descendant(&remote_files, &entry.path)
                && !has_descendant(pending_paths, &entry.path)
        })
        .map(|entry| format!("remote directory {:?}", entry.path))
        .collect::<Vec<_>>();
    let mut unsupported = unsupported_local;
    unsupported.extend(unsupported_remote);
    if unsupported.is_empty() {
        Ok(())
    } else {
        Err(Error::unsupported(format!(
            "Direct Sync does not represent empty directory markers; remove or populate: {}",
            unsupported.join(", ")
        )))
    }
}

fn has_descendant(paths: &BTreeSet<String>, directory: &str) -> bool {
    let prefix = format!("{directory}/");
    paths
        .range(prefix.clone()..)
        .next()
        .is_some_and(|path| path.starts_with(&prefix))
}

fn is_blocked(path: &str, blocked: &BTreeSet<String>) -> bool {
    blocked.iter().any(|blocked| {
        path == blocked
            || path
                .strip_prefix(blocked)
                .is_some_and(|suffix| suffix.starts_with('/'))
    })
}

fn local_changed(
    baseline_kind: Option<EntryKind>,
    baseline_local: Option<&LocalFingerprint>,
    local: Option<&LocalNode>,
    executable: bool,
) -> bool {
    match (baseline_kind, baseline_local, local) {
        (None, _, None) => false,
        (None, _, Some(_)) => true,
        (Some(EntryKind::Directory), _, Some(local)) => !local.is_directory(),
        (Some(EntryKind::Directory), _, None) => true,
        (Some(EntryKind::File), _, None) => true,
        (Some(EntryKind::File), Some(baseline), Some(local)) if !local.is_directory() => !baseline
            .same_content(
                local_length(local),
                local_digest(local),
                if executable {
                    local.executable
                } else {
                    baseline.executable
                },
            ),
        _ => true,
    }
}

fn local_is_superseded(accepted: &LocalFingerprint, current: &LocalNode) -> bool {
    if current.is_directory()
        || accepted.length == local_length(current) && accepted.digest == local_digest(current)
    {
        return false;
    }
    let Some(current_stat) = current.stat else {
        return false;
    };
    let Some(accepted_stat) = accepted.stat else {
        return false;
    };
    accepted.dev == current.identity.dev
        && accepted.ino == current.identity.ino
        && accepted.parent_dev == current.parent.map_or(0, |parent| parent.dev)
        && accepted.parent_ino == current.parent.map_or(0, |parent| parent.ino)
        && accepted_stat.dev == current_stat.dev
        && accepted_stat.ino == current_stat.ino
        && accepted_stat.mtime == current_stat.mtime
        && accepted_stat.mtime_nsec == current_stat.mtime_nsec
        && accepted_stat.ctime == current_stat.ctime
        && accepted_stat.ctime_nsec == current_stat.ctime_nsec
}

fn remote_changed(baseline: Option<&RemoteEntry>, observed: Option<&RemoteEntry>) -> bool {
    match (baseline, observed) {
        (None, None) => false,
        (None, Some(_)) | (Some(_), None) => true,
        (Some(baseline), Some(observed)) => !baseline.same_state(observed),
    }
}

fn remote_entries_match(
    accepted: &BTreeMap<String, RemoteEntry>,
    observed: &BTreeMap<String, RemoteEntry>,
) -> bool {
    accepted.len() == observed.len()
        && accepted.iter().all(|(path, entry)| {
            observed
                .get(path)
                .is_some_and(|observed| entry.same_state(observed))
        })
}

fn local_subtree_matches_baseline(
    baseline: &BaselineRecord,
    local: &LocalSnapshot,
    root: &str,
) -> bool {
    let prefix = format!("{root}/");
    local
        .nodes()
        .iter()
        .filter(|(path, _)| path.as_str() == root || path.starts_with(&prefix))
        .all(|(path, node)| {
            if node.is_directory() {
                baseline
                    .remote
                    .get(path)
                    .is_some_and(|entry| entry.kind == EntryKind::Directory)
            } else {
                baseline.local.get(path).is_some_and(|accepted| {
                    accepted.same_content(local_length(node), local_digest(node), node.executable)
                })
            }
        })
}

fn local_length(node: &LocalNode) -> u64 {
    match node.kind {
        LocalKind::File { length, .. } => length,
        LocalKind::Directory => 0,
    }
}

fn local_digest(node: &LocalNode) -> [u8; 32] {
    match node.kind {
        LocalKind::File { digest, .. } => *digest.as_bytes(),
        LocalKind::Directory => [0; 32],
    }
}

fn parent_path(path: &str) -> &str {
    path.rsplit_once('/').map_or("", |(parent, _)| parent)
}

fn refresh_local_path(local: &LocalTree, path: &str, snapshot: &mut LocalSnapshot) -> Result<()> {
    match local.observe_path(path)? {
        Some(node) => {
            snapshot.insert_node(node);
        }
        None => snapshot.remove_subtree(path),
    }
    Ok(())
}

fn conflict_local_side(local: Option<&LocalNode>) -> ConflictSide {
    match local {
        None => ConflictSide::absent(),
        Some(node) if node.is_directory() => ConflictSide {
            presence: ConflictPresence::Present,
            kind: Some(StoreEvidenceKind::Directory),
            length: None,
            digest: None,
            executable: Some(false),
            remote: None,
        },
        Some(node) => ConflictSide {
            presence: ConflictPresence::Present,
            kind: Some(StoreEvidenceKind::File),
            length: Some(local_length(node)),
            digest: Some(local_digest(node)),
            executable: Some(node.executable),
            remote: None,
        },
    }
}

fn conflict_remote_side(remote: Option<&RemoteEntry>) -> ConflictSide {
    match remote {
        None => ConflictSide::absent(),
        Some(entry) => ConflictSide {
            presence: ConflictPresence::Present,
            kind: Some(match entry.kind {
                EntryKind::File => StoreEvidenceKind::File,
                EntryKind::Directory => StoreEvidenceKind::Directory,
            }),
            length: Some(entry.length),
            digest: entry.digest,
            executable: Some(entry.executable),
            remote: Some(entry.clone()),
        },
    }
}

fn conflict_identity(
    path: &str,
    original_path: Option<&str>,
    desired_path: Option<&str>,
    local: &ConflictSide,
    remote: &ConflictSide,
) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(path.as_bytes());
    hasher.update(original_path.unwrap_or("").as_bytes());
    hasher.update(desired_path.unwrap_or("").as_bytes());
    for bytes in [
        serde_json::to_vec(local).unwrap_or_default(),
        serde_json::to_vec(remote).unwrap_or_default(),
    ] {
        hasher.update(&(bytes.len() as u64).to_be_bytes());
        hasher.update(&bytes);
    }
    hex(&hasher.finalize().as_bytes()[..16])
}

fn artifact_conflict_id(id: &str) -> String {
    let mut hasher = blake3::Hasher::new_derive_key("yinyang sync artifact conflict v1");
    hasher.update(id.as_bytes());
    hex(&hasher.finalize().as_bytes()[..16])
}

fn artifact_changed(state: &Path, artifact: &super::store::ArtifactRecord) -> bool {
    if artifact.kind == super::store::ArtifactKind::QuarantinedDirectory {
        return false;
    }
    let path = state.join("artifacts").join(&artifact.name);
    let metadata = match std::fs::symlink_metadata(&path) {
        Ok(metadata) => metadata,
        Err(_) => return artifact.stat.is_some(),
    };
    let Some(recorded) = artifact.stat else {
        return false;
    };
    metadata.dev() != recorded.dev
        || metadata.ino() != recorded.ino
        || metadata.mtime() != recorded.mtime
        || metadata.mtime_nsec() != recorded.mtime_nsec
        || metadata.ctime() != recorded.ctime
        || metadata.ctime_nsec() != recorded.ctime_nsec
        || metadata.size() != artifact.length
}

fn conflict_status(conflict: &ConflictRecord) -> ConflictStatus {
    ConflictStatus {
        id: conflict.id.clone(),
        path: conflict.path.clone(),
        original_path: conflict.original_path.clone(),
        desired_path: conflict.desired_path.clone(),
        local: conflict_side_status(&conflict.local),
        remote: conflict_side_status(&conflict.remote),
        local_artifact: conflict.local_artifact.clone(),
        remote_artifact: conflict.remote_artifact.clone(),
        remote_revision: conflict.remote_revision.map(|revision| hex(&revision)),
        error: conflict.error.clone(),
    }
}

fn conflict_side_status(side: &ConflictSide) -> ConflictSideStatus {
    match (side.presence, side.kind) {
        (ConflictPresence::Absent, _) => ConflictSideStatus::Deleted,
        (ConflictPresence::Present, Some(StoreEvidenceKind::File)) => {
            ConflictSideStatus::CurrentFile
        }
        (ConflictPresence::Present, Some(StoreEvidenceKind::Directory)) => {
            ConflictSideStatus::CurrentDirectory
        }
        _ => ConflictSideStatus::Unknown,
    }
}

fn local_evidence(local: Option<&LocalNode>) -> LocalEvidence {
    match local {
        None => LocalEvidence {
            kind: None,
            length: None,
            digest: None,
            executable: None,
            presence: Presence::Absent,
        },
        Some(node) if node.is_directory() => LocalEvidence {
            kind: Some(EvidenceKind::Directory),
            length: None,
            digest: None,
            executable: Some(false),
            presence: Presence::Present,
        },
        Some(node) => LocalEvidence {
            kind: Some(EvidenceKind::File),
            length: Some(local_length(node)),
            digest: Some(hex(&local_digest(node))),
            executable: Some(node.executable),
            presence: Presence::Present,
        },
    }
}

fn remote_evidence(remote: Option<&RemoteEntry>) -> RemoteEvidence {
    match remote {
        None => RemoteEvidence {
            kind: None,
            length: None,
            digest: None,
            executable: None,
            presence: Presence::Absent,
            identity: None,
            pin: None,
        },
        Some(entry) => RemoteEvidence {
            kind: Some(match entry.kind {
                EntryKind::File => EvidenceKind::File,
                EntryKind::Directory => EvidenceKind::Directory,
            }),
            length: Some(entry.length),
            digest: entry.digest.map(|digest| hex(&digest)),
            executable: Some(entry.executable),
            presence: Presence::Present,
            identity: Some(hex(&entry.pin.stable_id())),
            pin: Some(remote_pin_text(entry)),
        },
    }
}

fn remote_pin_text(entry: &RemoteEntry) -> String {
    serde_json::to_string(&entry.pin).unwrap_or_default()
}

fn intent_status(work: Option<&WorkRecord>, intent: Option<&IntentRecord>) -> IntentStatus {
    IntentStatus {
        id: work.map(|work| work.id.clone()),
        operation: work.map(|work| work.operation.as_str().to_owned()),
        state: intent.map(|intent| match intent.state {
            IntentState::Prepared => StatusIntentState::Prepared,
            IntentState::Unknown => StatusIntentState::Unknown,
            IntentState::Committed => StatusIntentState::Committed,
            IntentState::Conflict => StatusIntentState::Conflict,
            IntentState::Retryable => StatusIntentState::Retryable,
            IntentState::Failed => StatusIntentState::Failed,
        }),
        request_id: intent.map(|intent| hex(&intent.request_id)),
        commit_id: intent
            .and_then(|intent| intent.commit_id)
            .map(|commit| hex(&commit)),
        receipt_revision: intent
            .and_then(|intent| intent.receipt_revision)
            .map(|revision| hex(&revision)),
        error: intent.and_then(|intent| {
            intent
                .failure
                .as_ref()
                .map(|failure| failure.message.clone())
        }),
    }
}

fn prepare_directory(path: &Path) -> Result<()> {
    std::fs::create_dir_all(path)
        .map_err(|error| Error::io("create synchronized root", path, error))?;
    let metadata = std::fs::symlink_metadata(path)
        .map_err(|error| Error::io("inspect sync root", path, error))?;
    if !metadata.file_type().is_dir() || metadata.file_type().is_symlink() {
        return Err(Error::unsupported(format!(
            "{} must be an ordinary directory",
            path.display()
        )));
    }
    Ok(())
}
