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

use crate::replica::state::{BaselineRecord, ConflictRecord};
use crate::replica::{Conflict, LocalKind, LocalNode, LocalSnapshot, Replica, ReplicaStatus};
use crate::sync::{EditId, FileMetadata, NamespaceState, RenameMode, Sync};
use std::collections::{BTreeMap, BTreeSet};
use std::io::{Seek as _, SeekFrom, Write};
use std::path::Path;
use std::sync::Arc;
use yinyang_core::{Authority, ContentId, Node, NodeId, NodeKind, Revision, Snapshot};

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + std::marker::Sync>>;

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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Resolution {
    KeepLocal,
    KeepRemote,
}

pub struct DirectorySync {
    sync: Sync,
    replica: Replica,
}

impl DirectorySync {
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

    pub fn inspect(state: &Path) -> Result<ReplicaStatus> {
        Ok(Replica::inspect(state)?)
    }

    pub fn export_artifact(&self, id: &str, output: &mut dyn Write) -> Result<()> {
        Ok(self.replica.export_artifact(id, output)?)
    }

    pub async fn run(&mut self) -> Result<RunOutcome> {
        let mut outcome = self.retry_pending().await?;
        let mut current = self.replica.scan()?;
        let observed = self.sync.authority().observe_latest().await?;
        let mut remote = crate::directory_nodes(&observed).await?;
        let baseline = self.replica.store().load_baseline()?;
        match baseline {
            Some(baseline) => {
                let mut ctx = Context::new(
                    &mut self.sync,
                    &mut self.replica,
                    &mut outcome,
                    baseline,
                    &current,
                    &mut remote,
                    observed.revision(),
                );
                ctx.reconcile().await?;
            }
            None => {
                let local_empty = current.len() == 1;
                let remote_empty = remote.len() == 1;
                if let Some(root) = current.get("") {
                    if let Some(root_node) = observed.resolve("").await? {
                        self.replica
                            .store_mut()
                            .save_baseline_path("", &root_node, Some(root))?;
                        self.replica
                            .store_mut()
                            .set_baseline_revision(observed.revision())?;
                    }
                }
                if local_empty && remote_empty {
                    outcome.revision = Some(observed.revision());
                } else if local_empty {
                    self.materialize_all(&mut outcome, &observed, &remote)
                        .await?;
                } else if remote_empty {
                    self.publish_all(&mut outcome, &observed, &mut current)
                        .await?;
                } else {
                    record_conflict(
                        &mut outcome,
                        &mut self.replica,
                        "",
                        None,
                        Some(""),
                        "local-present",
                        "remote-present",
                        None,
                        None,
                        None,
                        "initial sync requires one empty side; other non-empty states are explicit merges",
                    )?;
                }
            }
        }
        let status = self.replica.status()?;
        outcome.pending = status.pending_installs as u64 + outcome.pending;
        outcome.conflicts.extend(status.conflicts);
        outcome.conflicts.sort_by(|a, b| a.path.cmp(&b.path));
        outcome.in_sync = outcome.revision.is_some()
            && outcome.pending == 0
            && outcome.unknown == 0
            && outcome.conflicts.is_empty();
        if outcome.revision.is_some() {
            let latest = self.sync.authority().observe_latest().await?;
            self.replica
                .store_mut()
                .set_baseline_revision(latest.revision())?;
        }
        Ok(outcome)
    }

    pub async fn resolve(&mut self, conflict_id: &str, pick: Resolution) -> Result<RunOutcome> {
        let record = self
            .replica
            .store()
            .conflicts()?
            .into_iter()
            .find(|record| record.id == conflict_id)
            .ok_or_else(|| crate::replica::Error::invalid("unknown conflict").to_string())?;
        let message = match pick {
            Resolution::KeepLocal => {
                if let Some(artifact) = record.local_artifact {
                    let observed = self.sync.authority().observe_latest().await?;
                    let remote = crate::directory_nodes(&observed).await?;
                    if let Some(node) = remote.get(&record.path) {
                        let source = self.replica.open_artifact(&artifact)?;
                        let mut source =
                            tokio::io::BufReader::new(tokio::fs::File::from_std(source));
                        self.sync
                            .stage(node.id(), observed.revision(), &mut source)
                            .await?;
                    }
                }
                "resolved to local; run publishes the retained intent"
            }
            Resolution::KeepRemote => "resolved to remote; run materializes the retained revision",
        }
        .to_owned();
        self.replica
            .store_mut()
            .update_conflict_error(&conflict_id, &message)?;
        self.run().await
    }

    async fn retry_pending(&mut self) -> Result<RunOutcome> {
        let mut outcome = RunOutcome::default();
        for status in self.sync.namespace_intents().await? {
            match status.state {
                NamespaceState::Prepared | NamespaceState::Unknown | NamespaceState::Retryable => {
                    match self.sync.publish(status.intent.id).await {
                        Ok(revision) => {
                            outcome.revision = Some(revision);
                            outcome.published_local += 1;
                        }
                        Err(error) if error.kind() == crate::runtime::ErrorKind::Unknown => {
                            outcome.unknown += 1;
                        }
                        Err(error) => return Err(error.into()),
                    }
                }
                NamespaceState::Committed(_) => {}
                NamespaceState::Conflict | NamespaceState::Failed => {
                    outcome.conflicts.push(Conflict {
                        id: format!("intent-{:?}", status.intent.id),
                        path: String::new(),
                        original_path: None,
                        desired_path: None,
                        local: "local-intent".to_owned(),
                        remote: "conflict".to_owned(),
                        local_artifact: None,
                        remote_node: None,
                        remote_revision: None,
                        error: status
                            .failure
                            .map(|failure| failure.to_string())
                            .unwrap_or_else(|| "namespace intent is terminal".to_owned()),
                    });
                }
            }
        }
        for id in self.sync.edits().await? {
            if let crate::sync::EditStatus::Content(status) = self.sync.edit_status(id).await?
                && (status.pending || status.frozen)
            {
                match self.sync.publish(id).await {
                    Ok(revision) => {
                        outcome.revision = Some(revision);
                        outcome.published_local += 1;
                    }
                    Err(error) if error.kind() == crate::runtime::ErrorKind::Unknown => {
                        outcome.unknown += 1;
                    }
                    Err(error) => return Err(error.into()),
                }
            }
        }
        Ok(outcome)
    }

    async fn materialize_all(
        &mut self,
        outcome: &mut RunOutcome,
        observed: &Snapshot,
        remote: &BTreeMap<String, Node>,
    ) -> Result<()> {
        for (path, node) in remote {
            if path.is_empty() {
                continue;
            }
            match node.kind() {
                NodeKind::Directory { .. } => {
                    self.replica.create_directory_local(path).map_err(|error| {
                        crate::replica::Error::conflict(format!(
                            "{path} exists during initial materialization: {error}"
                        ))
                        .to_string()
                    })?;
                    outcome.materialized_remote += 1;
                    let fresh = self.replica.scan()?;
                    if let Some(local) = fresh.get(path) {
                        self.replica
                            .store_mut()
                            .save_baseline_path(path, node, Some(local))?;
                    }
                }
                NodeKind::File(_) => {
                    let mut staged = self.stage_remote_file(observed, node).await?;
                    self.replica
                        .create_file_local(path, node.executable(), &mut staged)?;
                    outcome.materialized_remote += 1;
                    let fresh = self.replica.scan()?;
                    if let Some(local) = fresh.get(path) {
                        if canonical_matches(&mut self.replica, local, node).await? {
                            self.replica
                                .store_mut()
                                .save_baseline_path(path, node, Some(local))?;
                        }
                    }
                }
            }
        }
        outcome.revision = Some(observed.revision());
        Ok(())
    }

    async fn publish_all(
        &mut self,
        outcome: &mut RunOutcome,
        observed: &Snapshot,
        current: &LocalSnapshot,
    ) -> Result<()> {
        let mut nodes: BTreeMap<String, NodeId> =
            BTreeMap::from([(String::new(), observed.root())]);
        let mut stage_revision = observed.revision();
        for (path, entry) in current.nodes() {
            if path.is_empty() {
                continue;
            }
            let (parent, name) = split(path);
            let parent_node = nodes.get(parent).copied().ok_or_else(|| {
                crate::replica::Error::invalid("local parent missing").to_string()
            })?;
            let edit = match entry.kind {
                LocalKind::Directory => {
                    self.sync
                        .stage_create_directory(parent_node, name, stage_revision)
                        .await?
                }
                LocalKind::File { .. } => {
                    let captured = self.replica.capture_file(path, entry)?;
                    let mut source = tokio::fs::File::from_std(
                        self.replica.open_artifact(&captured.artifact.id)?,
                    );
                    self.sync
                        .stage_create_file(
                            parent_node,
                            name,
                            stage_revision,
                            FileMetadata::new(entry.executable),
                            &mut source,
                        )
                        .await?
                }
            };
            let revision = self.sync.publish(edit).await?;
            stage_revision = revision;
            let node = self.namespace_node(edit).await?;
            nodes.insert(path.clone(), node);
            let snapshot = self.sync.authority().observe_revision(revision).await?;
            if let Some(remote_node) = snapshot.resolve(path).await? {
                let fresh = self.replica.scan()?;
                if let Some(local) = fresh.get(path) {
                    if local_equal(entry, local) {
                        self.replica.store_mut().save_baseline_path(
                            path,
                            &remote_node,
                            Some(local),
                        )?;
                    }
                }
            }
            outcome.revision = Some(revision);
            outcome.published_local += 1;
        }
        Ok(())
    }

    async fn namespace_node(&mut self, edit: EditId) -> Result<NodeId> {
        let status = self.sync.namespace_status(edit).await?;
        Ok(status.intent.operation.node_id())
    }

    async fn stage_remote_file(&self, observed: &Snapshot, node: &Node) -> Result<std::fs::File> {
        let version = observed.open_file(node.id()).await?;
        let length = version.descriptor().content_id().length();
        let spool = tempfile::tempfile()?;
        let mut output = tokio::fs::File::from_std(spool);
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

struct Context<'a> {
    sync: &'a mut Sync,
    replica: &'a mut Replica,
    outcome: &'a mut RunOutcome,
    baseline: BaselineRecord,
    current: &'a LocalSnapshot,
    remote: &'a mut BTreeMap<String, Node>,
    revision: Revision,
    nodes: BTreeMap<String, NodeId>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Action {
    None,
    Create,
    Modify,
    Delete,
}

impl<'a> Context<'a> {
    fn new(
        sync: &'a mut Sync,
        replica: &'a mut Replica,
        outcome: &'a mut RunOutcome,
        baseline: BaselineRecord,
        current: &'a LocalSnapshot,
        remote: &'a mut BTreeMap<String, Node>,
        revision: Revision,
    ) -> Self {
        let nodes = remote
            .iter()
            .map(|(path, node)| (path.clone(), node.id()))
            .collect();
        Self {
            sync,
            replica,
            outcome,
            baseline,
            current,
            remote,
            revision,
            nodes,
        }
    }

    async fn reconcile(&mut self) -> Result<()> {
        let (remote_moves, local_moves) = self.detect_moves();
        let mut paths: BTreeSet<String> = BTreeSet::new();
        paths.extend(self.baseline.remote.keys().cloned());
        paths.extend(self.current.nodes().keys().cloned());
        paths.extend(self.remote.keys().cloned());
        let order: Vec<String> = paths.iter().cloned().collect();
        let mut ascending = order.clone();
        ascending.sort_by_key(|path| (depth(path), path.clone()));
        let mut descending = order;
        descending.sort_by_key(|path| (depth(path), path.clone()));
        descending.reverse();
        for path in &ascending {
            if let Some(destination) = remote_moves.get(path) {
                self.remote_rename(path, destination).await?;
                continue;
            }
            if remote_moves.values().any(|from| from == path) {
                continue;
            }
            if let Some(destination) = local_moves.get(path) {
                self.local_rename(path, destination).await?;
                continue;
            }
            if local_moves.values().any(|from| from == path) {
                continue;
            }
            if is_moved_child(&remote_moves, path) || is_moved_child(&local_moves, path) {
                continue;
            }
            self.entry(path, &remote_moves, &local_moves).await?;
        }
        for path in &descending {
            let local = local_action(self.baseline.local.get(path), self.current.get(path));
            let remote = remote_action(self.baseline.remote.get(path), self.remote.get(path));
            if local == Action::Delete && remote == Action::None {
                if !is_moved_child(&remote_moves, path) && !is_moved_child(&local_moves, path) {
                    self.publish_remove(path).await?;
                }
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

    async fn entry(
        &mut self,
        path: &str,
        _remote_moves: &BTreeMap<String, String>,
        _local_moves: &BTreeMap<String, String>,
    ) -> Result<()> {
        let local = local_action(self.baseline.local.get(path), self.current.get(path));
        let remote = remote_action(self.baseline.remote.get(path), self.remote.get(path));
        match (local, remote) {
            (Action::None, Action::None) => Ok(()),
            (Action::Create, Action::None) => {
                let node = self.current.get(path).ok_or_else(|| {
                    crate::replica::Error::invalid("local create disappeared").to_string()
                })?;
                if node.is_directory() {
                    self.publish_directory(path).await
                } else {
                    self.publish_file(path, node).await
                }
            }
            (Action::Modify, Action::None) => {
                let node = self.current.get(path).ok_or_else(|| {
                    crate::replica::Error::invalid("local modify disappeared").to_string()
                })?;
                self.publish_modify(path, node).await
            }
            (Action::None, Action::Create) => {
                let node = self.remote.get(path).cloned().ok_or_else(|| {
                    crate::replica::Error::invalid("remote create disappeared").to_string()
                })?;
                self.materialize_create(path, &node).await
            }
            (Action::None, Action::Modify) => {
                let node = self.remote.get(path).cloned().ok_or_else(|| {
                    crate::replica::Error::invalid("remote modify disappeared").to_string()
                })?;
                self.materialize_modify(path, &node).await
            }
            (Action::Delete, Action::Delete) => Ok(()),
            (Action::None, Action::Delete) => self.materialize_delete(path).await,
            (Action::Delete, Action::None) => Ok(()), // applied in descending pass
            (Action::Create, Action::Create) => {
                let local = self.current.get(path);
                let remote = self.remote.get(path);
                if canonical_matches(self.replica, local.unwrap(), remote.unwrap()).await? {
                    Ok(())
                } else {
                    self.conflict(path, None, Some(path), "create", "create")
                        .await
                }
            }
            (Action::Modify, Action::Modify) => {
                let local = self.current.get(path);
                let remote = self.remote.get(path);
                if canonical_matches(self.replica, local.unwrap(), remote.unwrap()).await? {
                    Ok(())
                } else {
                    self.conflict(path, Some(path), None, "modify", "modify")
                        .await
                }
            }
            (Action::Delete, Action::Modify)
            | (Action::Delete, Action::Create)
            | (Action::Modify, Action::Delete)
            | (Action::Modify, Action::Create)
            | (Action::Create, Action::Modify)
            | (Action::Create, Action::Delete) => {
                self.conflict(path, Some(path), None, "local", "remote")
                    .await
            }
        }
    }

    async fn publish_directory(&mut self, path: &str) -> Result<()> {
        let (parent, name) = split(path);
        let parent_node = self.nodes.get(parent).copied().ok_or_else(|| {
            crate::replica::Error::invalid("directory parent missing").to_string()
        })?;
        let edit = self
            .sync
            .stage_create_directory(parent_node, name, self.revision)
            .await?;
        let revision = self.sync.publish(edit).await?;
        let node_id = self.namespace_node(&mut edit.clone()).await?;
        self.nodes.insert(path.to_owned(), node_id);
        if let Some(local) = self.current.get(path) {
            self.accept_local(path, local, revision).await?;
        }
        self.outcome.published_local += 1;
        Ok(())
    }

    async fn publish_file(&mut self, path: &str, node: &LocalNode) -> Result<()> {
        let (parent, name) = split(path);
        let parent_node = self
            .nodes
            .get(parent)
            .copied()
            .ok_or_else(|| crate::replica::Error::invalid("file parent missing").to_string())?;
        let captured = self.replica.capture_file(path, node)?;
        let mut source =
            tokio::fs::File::from_std(self.replica.open_artifact(&captured.artifact.id)?);
        let edit = self
            .sync
            .stage_create_file(
                parent_node,
                name,
                self.revision,
                FileMetadata::new(node.executable),
                &mut source,
            )
            .await?;
        let revision = self.sync.publish(edit).await?;
        let node_id = self.namespace_node(&mut edit.clone()).await?;
        self.nodes.insert(path.to_owned(), node_id);
        self.accept_local(path, node, revision).await?;
        self.outcome.published_local += 1;
        Ok(())
    }

    async fn publish_modify(&mut self, path: &str, node: &LocalNode) -> Result<()> {
        let remote_node =
            self.nodes.get(path).copied().ok_or_else(|| {
                crate::replica::Error::invalid("modify source missing").to_string()
            })?;
        let captured = self.replica.capture_file(path, node)?;
        let mut source =
            tokio::fs::File::from_std(self.replica.open_artifact(&captured.artifact.id)?);
        let edit = self
            .sync
            .stage(remote_node, self.revision, &mut source)
            .await?;
        let revision = self.sync.publish(edit).await?;
        self.accept_local(path, node, revision).await?;
        self.outcome.published_local += 1;
        Ok(())
    }

    async fn publish_remove(&mut self, path: &str) -> Result<()> {
        let node =
            self.nodes.get(path).copied().ok_or_else(|| {
                crate::replica::Error::invalid("remove source missing").to_string()
            })?;
        let edit = self.sync.stage_remove(node, self.revision).await?;
        self.sync.publish(edit).await?;
        self.nodes.remove(path);
        self.replica.store_mut().remove_baseline_path(path)?;
        self.outcome.published_local += 1;
        Ok(())
    }

    async fn local_rename(&mut self, from: &str, to: &str) -> Result<()> {
        let node =
            self.nodes.get(from).copied().ok_or_else(|| {
                crate::replica::Error::invalid("rename source missing").to_string()
            })?;
        let (parent, name) = split(to);
        let parent_node =
            self.nodes.get(parent).copied().ok_or_else(|| {
                crate::replica::Error::invalid("rename parent missing").to_string()
            })?;
        let edit = self
            .sync
            .stage_rename(
                node,
                self.revision,
                parent_node,
                name,
                RenameMode::NoReplace,
            )
            .await?;
        let revision = self.sync.publish(edit).await?;
        self.nodes.remove(from);
        self.nodes.insert(to.to_owned(), node);
        self.replica.store_mut().remove_baseline_path(from)?;
        let fresh = self.replica.scan()?;
        if let Some(local) = fresh.get(to) {
            self.accept_local(to, local, revision).await?;
        }
        self.outcome.published_local += 1;
        Ok(())
    }

    async fn materialize_create(&mut self, path: &str, node: &Node) -> Result<()> {
        match node.kind() {
            NodeKind::Directory { .. } => match self.replica.create_directory_local(path) {
                Ok(()) => {
                    self.nodes.insert(path.to_owned(), node.id());
                    self.outcome.materialized_remote += 1;
                    self.accept_remote(path, node).await?;
                    Ok(())
                }
                Err(_)
                    if self
                        .current
                        .get(path)
                        .is_some_and(|entry| entry.is_directory()) =>
                {
                    self.nodes.insert(path.to_owned(), node.id());
                    self.accept_remote(path, node).await?;
                    Ok(())
                }
                Err(_) => {
                    self.conflict(path, None, Some(path), "present", "remote-create")
                        .await
                }
            },
            NodeKind::File(_) => {
                if self.current.get(path).is_some() {
                    return self
                        .conflict(path, None, Some(path), "present", "remote-create")
                        .await;
                }
                let mut staged = self.stage_remote_file(node).await?;
                match self
                    .replica
                    .create_file_local(path, node.executable(), &mut staged)
                {
                    Ok(()) => {
                        self.nodes.insert(path.to_owned(), node.id());
                        self.outcome.materialized_remote += 1;
                        self.accept_remote(path, node).await?;
                        Ok(())
                    }
                    Err(_) => {
                        self.conflict(path, None, Some(path), "present", "remote-create")
                            .await
                    }
                }
            }
        }
    }

    async fn materialize_modify(&mut self, path: &str, node: &Node) -> Result<()> {
        let expected = self.current.get(path).cloned().ok_or_else(|| {
            crate::replica::Error::invalid("remote modify path is local-absent").to_string()
        })?;
        if expected.is_directory() || node.is_directory() {
            return self
                .conflict(path, Some(path), None, "local", "remote-kind")
                .await;
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
                self.outcome.materialized_remote += 1;
                self.accept_remote(path, node).await?;
                Ok(())
            }
            Err(_) => {
                self.conflict(
                    path,
                    Some(path),
                    None,
                    "changed-during-install",
                    "remote-modify",
                )
                .await
            }
        }
    }

    async fn materialize_delete(&mut self, path: &str) -> Result<()> {
        // The common baseline, not the fresh scan, is the deletion predicate.
        let expected = self.baseline.local.clone();
        match self.replica.quarantine(
            path,
            &expected,
            "remote deletion",
            Some(self.revision.to_bytes()),
        ) {
            Ok(_) => {
                self.nodes.remove(path);
                self.replica.store_mut().remove_baseline_path(path)?;
                self.outcome.materialized_remote += 1;
                Ok(())
            }
            Err(_) => {
                self.conflict(path, Some(path), None, "local-changed", "remote-delete")
                    .await
            }
        }
    }

    async fn remote_rename(&mut self, from: &str, to: &str) -> Result<()> {
        if local_action(self.baseline.local.get(from), self.current.get(from)) != Action::None {
            return self
                .conflict(from, Some(from), Some(to), "local-changed", "remote-rename")
                .await;
        }
        if self.current.get(to).is_some() {
            return self
                .conflict(to, Some(from), Some(to), "present", "remote-rename")
                .await;
        }
        match self.replica.rename_local(from, to) {
            Ok(()) => {
                let node = self.nodes.remove(from);
                if let Some(node) = node {
                    self.nodes.insert(to.to_owned(), node);
                }
                self.replica.store_mut().remove_baseline_path(from)?;
                if let Some(remote_node) = self.remote.get(to).cloned() {
                    self.accept_remote(to, &remote_node).await?;
                }
                self.outcome.materialized_remote += 1;
                Ok(())
            }
            Err(_) => {
                self.conflict(from, Some(from), Some(to), "local-changed", "remote-rename")
                    .await
            }
        }
    }

    async fn conflict(
        &mut self,
        path: &str,
        original_path: Option<&str>,
        desired_path: Option<&str>,
        local: &str,
        remote: &str,
    ) -> Result<()> {
        let captured = self
            .current
            .get(path)
            .filter(|node| matches!(node.kind, LocalKind::File { .. }))
            .and_then(|node| self.replica.capture_file(path, node).ok());
        let remote_node = self.remote.get(path).map(Node::id);
        record_conflict(
            self.outcome,
            self.replica,
            path,
            original_path,
            desired_path,
            local,
            remote,
            captured.as_ref().map(|value| value.artifact.id.as_str()),
            remote_node,
            Some(self.revision),
            "local and remote changed from the same baseline",
        )
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

    async fn namespace_node(&mut self, edit: &mut EditId) -> Result<NodeId> {
        let status = self.sync.namespace_status(*edit).await?;
        Ok(status.intent.operation.node_id())
    }

    async fn accept_local(
        &mut self,
        path: &str,
        expected: &LocalNode,
        revision: Revision,
    ) -> Result<()> {
        let snapshot = self.sync.authority().observe_revision(revision).await?;
        let Some(node) = snapshot.resolve(path).await? else {
            return Ok(());
        };
        let fresh = self.replica.scan()?;
        let Some(current) = fresh.get(path) else {
            return Ok(());
        };
        if local_equal(expected, current) {
            self.replica
                .store_mut()
                .save_baseline_path(path, &node, Some(current))?;
        }
        Ok(())
    }

    async fn accept_remote(&mut self, path: &str, node: &Node) -> Result<()> {
        let fresh = self.replica.scan()?;
        let Some(current) = fresh.get(path) else {
            return Ok(());
        };
        if canonical_matches(self.replica, current, node).await? {
            self.replica
                .store_mut()
                .save_baseline_path(path, node, Some(current))?;
        }
        Ok(())
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

fn remote_action(
    baseline: Option<&crate::replica::state::BaselineNode>,
    current: Option<&Node>,
) -> Action {
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
            let content = ContentId::calculate(&mut source).await?;
            Ok(local.executable == remote.executable() && content == file.content_id())
        }
        _ => Ok(false),
    }
}

fn remote_equal(baseline: &crate::replica::state::BaselineNode, current: &Node) -> bool {
    let digest = match current.kind() {
        NodeKind::File(file) => Some(*file.content_id().digest()),
        NodeKind::Directory { .. } => None,
    };
    baseline.node == current.id()
        && baseline.directory == current.is_directory()
        && baseline.executable == current.executable()
        && baseline.digest == digest
}

fn record_conflict(
    outcome: &mut RunOutcome,
    replica: &mut Replica,
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
    let id = uuid::Uuid::new_v4().to_string();
    outcome.conflicts.push(Conflict {
        id: id.clone(),
        path: path.to_owned(),
        original_path: original_path.map(str::to_owned),
        desired_path: desired_path.map(str::to_owned),
        local: local.to_owned(),
        remote: remote.to_owned(),
        local_artifact: local_artifact.map(str::to_owned),
        remote_node,
        remote_revision: remote_revision.map(Revision::to_bytes),
        error: error.to_owned(),
    });
    replica.store_mut().insert_conflict(&ConflictRecord {
        id,
        path: path.to_owned(),
        original_path: original_path.map(str::to_owned),
        desired_path: desired_path.map(str::to_owned),
        local: local.to_owned(),
        remote: remote.to_owned(),
        local_artifact: local_artifact.map(str::to_owned),
        remote_node: remote_node.map(|value| *value.as_bytes()),
        remote_revision: remote_revision.map(Revision::to_bytes),
        error: error.to_owned(),
    })?;
    Ok(())
}

fn split(path: &str) -> (&str, &str) {
    path.rsplit_once('/').unwrap_or(("", path))
}

fn depth(path: &str) -> usize {
    path.matches('/').count()
}

fn is_moved_child(moves: &BTreeMap<String, String>, path: &str) -> bool {
    moves.iter().any(|(from, to)| {
        (path != *from
            && path
                .strip_prefix(from)
                .is_some_and(|suffix| !suffix.is_empty() && suffix.starts_with('/')))
            || (path != *to
                && path
                    .strip_prefix(to)
                    .is_some_and(|suffix| !suffix.is_empty() && suffix.starts_with('/')))
    })
}
