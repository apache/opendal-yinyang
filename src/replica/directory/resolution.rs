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

//! Explicit conflict resolution for one Managed Sync binding.
//!
//! Detection lives in the reconciliation context. This module applies a
//! durable decision for the exact conflict record it was given. Every
//! mutation is staged against the remote evidence the conflict recorded and
//! the predicate that pinned it; a newer head is never adopted silently.
//! Directory and deletion decisions publish or materialize the corresponding
//! tree through the same persistent `Sync`/`Replica` capabilities, keep the
//! recoverable work association, and confirm the common baseline only for the
//! version that was actually selected and made durable on both sides.

use super::{DirectorySync, Resolution, Result, revision_bytes, split, work_state_for};
use crate::replica::state::{BaselineRecord, ConflictRecord, WorkRecord};
use crate::replica::{ContentHash, Identity, LocalKind, LocalNode};
use crate::sync::{EditId, FileMetadata};
use std::collections::BTreeMap;
use yinyang_core::{ContentId, Node, NodeId, NodeKind, Revision, Snapshot};

impl DirectorySync {
    /// Publish the chosen local bytes through the same durable work/receipt
    /// path as normal publication, linked to the conflict. The conflict is
    /// only cleared once the receipt is applied.
    pub(super) async fn resolve_keep_local(
        &mut self,
        record: &ConflictRecord,
        _remote: &BTreeMap<String, Node>,
        baseline: Option<&BaselineRecord>,
        decision: Resolution,
    ) -> Result<bool> {
        let path = record
            .desired_path
            .clone()
            .unwrap_or_else(|| record.path.clone());
        let bytes = match &decision {
            Resolution::Content(bytes) => Some(bytes.clone()),
            _ => None,
        };
        let current = self.replica.observe_path(&path)?;
        match (bytes, current.as_ref()) {
            (Some(bytes), _) => {
                self.publish_keep_local_file(record, baseline, &path, bytes)
                    .await
            }
            (None, None) => {
                // The user keeps the local deletion: publish the removal of the
                // recorded remote node against its recorded predicate.
                self.publish_keep_local_delete(record, baseline, &path)
                    .await
            }
            (None, Some(node)) if node.is_directory() => {
                self.publish_keep_local_subtree(record, &path).await
            }
            (None, Some(_)) => {
                let bytes = self.local_side_bytes(record)?;
                self.publish_keep_local_file(record, baseline, &path, bytes)
                    .await
            }
        }
    }

    /// Apply the recorded remote side: materialize the recorded remote node, or
    /// remove the local path when the recorded remote side is a deletion.
    pub(super) async fn resolve_keep_remote(
        &mut self,
        record: &ConflictRecord,
        observed: &Snapshot,
        _remote: &BTreeMap<String, Node>,
    ) -> Result<bool> {
        let path = record
            .desired_path
            .clone()
            .unwrap_or_else(|| record.path.clone());
        if record.remote == "deleted" {
            // The selected remote side is an explicit deletion. Apply the local
            // removal durably; the quarantine keeps any new local bytes
            // exportable.
            return self.apply_keep_remote_deletion(record, &path).await;
        }
        // A recorded node is materialized exactly as recorded. A synthetic
        // retained-inode conflict reports the current file without a recorded
        // node, so the current observation is its evidence.
        let (snapshot, node_id) = match (record.remote_node, revision_bytes(record.remote_revision))
        {
            (Some(node), Some(revision)) => (
                self.sync.authority().observe_revision(revision).await?,
                NodeId::from_bytes(node),
            ),
            _ => {
                let snapshot = observed.clone();
                let Some(node) = snapshot.resolve(&path).await? else {
                    // The reported remote file is no longer present.
                    return self.apply_keep_remote_deletion(record, &path).await;
                };
                (snapshot, node.id())
            }
        };
        let Some(node) = snapshot.node(node_id).await? else {
            return Ok(false);
        };
        let revision = snapshot.revision();
        if node.is_directory() {
            self.materialize_remote_subtree(&path, &snapshot, revision)
                .await
        } else {
            self.materialize_remote_file(record, &path, &snapshot, revision, node)
                .await
        }
    }

    /// Record the accepted common state for a resolved path and drop the
    /// conflict's original path so a rename no longer re-conflicts. The local
    /// fingerprint and inode identity come from the chosen side, never from a
    /// fresh post-receipt scan. Returns false when the pinned revision does
    /// not resolve to the selected node, in which case the caller must not
    /// clear the conflict.
    async fn apply_resolved_baseline(
        &mut self,
        record: &ConflictRecord,
        path: &str,
        revision: Revision,
        expected: Option<NodeId>,
        accepted: Option<&LocalNode>,
    ) -> Result<bool> {
        let snapshot = self.sync.authority().observe_revision(revision).await?;
        let Some(node) = snapshot.resolve(path).await? else {
            return Ok(false);
        };
        if let Some(expected) = expected
            && node.id() != expected
        {
            return Ok(false);
        }
        if record.path != path {
            self.replica
                .store_mut()
                .remove_baseline_subtree(&record.path)?;
        }
        if let Some(accepted) = accepted {
            self.replica
                .store_mut()
                .save_baseline_path(path, &node, Some(accepted), revision)?;
        }
        Ok(true)
    }

    /// Install the chosen bytes only when the live local file is still the
    /// version this decision was dispatched against. A newer local write stays
    /// live and pending; it is never overwritten by the chosen version.
    /// Returns the local fingerprint the baseline may accept for this path.
    pub(super) fn install_chosen_bytes(
        &mut self,
        path: &str,
        bytes: &[u8],
        executable: bool,
        pre_dispatch: Option<&LocalNode>,
    ) -> Result<Option<LocalNode>> {
        let live = self.replica.observe_path(path)?;
        let chosen = chosen_file_local(path, bytes, executable, pre_dispatch);
        // A crash after the selected bytes were already installed but before
        // the baseline was saved leaves them on a new inode. Accept the actual
        // live node so later move detection keeps the real identity.
        if let Some(live) = &live
            && matches!(live.kind, LocalKind::File { .. })
            && local_file_matches(live, bytes)?
            && live.executable == executable
        {
            return Ok(Some(live.clone()));
        }
        match (live, pre_dispatch) {
            (Some(live), Some(expected))
                if matches!(live.kind, LocalKind::File { .. })
                    && live.identity == expected.identity
                    && live.kind == expected.kind
                    && live.executable == expected.executable =>
            {
                // Same inode as dispatch time: replace its bytes with the
                // explicit chosen version, then verify the installed result.
                self.replica
                    .install_bytes(path, &live, executable, None, bytes)?;
                self.verified_local_file(path, bytes)
            }
            (Some(_live), _) => {
                // A newer local write or a different inode appeared while the
                // receipt was pending: leave it live and pending.
                Ok(Some(chosen))
            }
            (None, Some(_)) => {
                // The local file was deleted while the receipt was pending:
                // never resurrect it; the deletion stays pending.
                Ok(Some(chosen))
            }
            (None, None) => {
                // No local file existed before dispatch: install the chosen
                // bytes and acknowledge only the verified result.
                self.replica
                    .create_file_local(path, executable, &mut &bytes[..])?;
                self.verified_local_file(path, bytes)
            }
        }
    }

    async fn download_remote_bytes(&self, snapshot: &Snapshot, node: &Node) -> Result<Vec<u8>> {
        let version = snapshot.open_file(node.id()).await?;
        let length = version.descriptor().content_id().length();
        let mut bytes = Vec::new();
        let mut position = 0;
        while position < length {
            let end = (position + 64 * 1024).min(length);
            let mut chunk = Vec::new();
            version.read_range(position..end, &mut chunk).await?;
            bytes.extend_from_slice(&chunk);
            position = end;
        }
        Ok(bytes)
    }

    fn local_side_bytes(&self, record: &ConflictRecord) -> Result<Vec<u8>> {
        if let Some(artifact) = &record.local_artifact {
            let mut file = self.replica.open_artifact(artifact)?;
            let mut bytes = Vec::new();
            std::io::copy(&mut file, &mut bytes).map_err(|e| e.to_string())?;
            return Ok(bytes);
        }
        let path = record
            .desired_path
            .as_deref()
            .unwrap_or(record.path.as_str());
        let mut file = self.replica.open_local_file(path)?;
        let mut bytes = Vec::new();
        std::io::copy(&mut file, &mut bytes).map_err(|e| e.to_string())?;
        Ok(bytes)
    }

    fn resolve_remote_node(&self, remote: &BTreeMap<String, Node>, path: &str) -> Result<NodeId> {
        remote
            .get(path)
            .map(Node::id)
            .ok_or_else(|| format!("remote parent {path:?} is missing").into())
    }

    /// Keep a local deletion: publish the removal of the recorded remote node
    /// against its recorded predicate, or confirm the already-converged
    /// absence. The conflict is cleared only after the remove receipt is
    /// durable; a removal is never retargeted onto a newer node.
    async fn publish_keep_local_delete(
        &mut self,
        record: &ConflictRecord,
        baseline: Option<&BaselineRecord>,
        path: &str,
    ) -> Result<bool> {
        let Some(node_bytes) = record.remote_node else {
            self.replica.store_mut().remove_baseline_subtree(path)?;
            if record.path != path {
                self.replica
                    .store_mut()
                    .remove_baseline_subtree(&record.path)?;
            }
            self.clear_resolved_conflict(&record.id)?;
            return Ok(true);
        };
        let Some(predicate) = revision_bytes(record.remote_revision).or_else(|| {
            baseline
                .and_then(|b| b.remote.get(path))
                .map(|entry| entry.revision)
        }) else {
            // Without the recorded predicate the removal cannot be expressed
            // conditionally; leave the conflict for a refreshed run.
            return Ok(false);
        };
        let node = NodeId::from_bytes(node_bytes);
        let edit = self.sync.stage_remove(node, predicate).await?;
        self.associate_resolution(
            record,
            edit,
            path,
            "remove",
            None,
            Some(node),
            predicate,
            None,
        )?;
        match self.sync.publish(edit).await {
            Ok(revision) => {
                self.replica.store_mut().remove_baseline_subtree(path)?;
                if record.path != path {
                    self.replica
                        .store_mut()
                        .remove_baseline_subtree(&record.path)?;
                }
                self.refresh_parent_baseline(path, revision).await?;
                self.clear_resolved_conflict(&record.id)?;
                self.replica.store_mut().update_work(
                    edit.as_bytes(),
                    "committed",
                    Some(revision),
                    None,
                )?;
                self.replica.store_mut().remove_work(edit.as_bytes())?;
                Ok(true)
            }
            Err(error) => {
                self.replica.store_mut().update_work(
                    edit.as_bytes(),
                    work_state_for(&error),
                    None,
                    Some(&error.to_string()),
                )?;
                Ok(false)
            }
        }
    }

    /// Keep the local file version. A recorded remote file is edited only at
    /// its recorded predicate; a recorded remote deletion confines the
    /// decision to a conditional create (or equality confirmation). A remote
    /// path that now holds a different version is never overwritten: the
    /// conflict stays active so the next run refreshes its evidence.
    async fn publish_keep_local_file(
        &mut self,
        record: &ConflictRecord,
        baseline: Option<&BaselineRecord>,
        path: &str,
        bytes: Vec<u8>,
    ) -> Result<bool> {
        let expected = self.replica.observe_path(path)?;
        let executable = expected
            .as_ref()
            .filter(|node| matches!(node.kind, LocalKind::File { .. }))
            .is_some_and(|node| node.executable);
        if record.remote == "deleted" {
            return self
                .publish_keep_local_over_recorded_deletion(
                    record, baseline, path, bytes, executable, expected,
                )
                .await;
        }
        // The exact file evidence this decision acts on: the recorded node at
        // its recorded revision, or (for a synthetic retained-inode conflict)
        // the current observation that reported it.
        let (snapshot, node) = match (record.remote_node, revision_bytes(record.remote_revision)) {
            (Some(node), Some(revision)) => {
                let snapshot = self.sync.authority().observe_revision(revision).await?;
                let Some(node) = snapshot.node(NodeId::from_bytes(node)).await? else {
                    return Ok(false);
                };
                (snapshot, node)
            }
            _ => {
                let snapshot = self.sync.authority().observe_latest().await?;
                let Some(node) = snapshot.resolve(path).await? else {
                    return Ok(false);
                };
                (snapshot, node)
            }
        };
        if node.is_directory() {
            // A file decision cannot replace a remote directory; the next run
            // keeps reporting the kind conflict.
            return Ok(false);
        }
        let artifact = self.replica.retain_bytes(path, &bytes)?;
        let predicate = snapshot.revision();
        let mut source = bytes.as_slice();
        let edit = self.sync.stage(node.id(), predicate, &mut source).await?;
        self.finish_keep_local_edit(
            record,
            path,
            node.id(),
            edit,
            Some(artifact),
            &bytes,
            predicate,
            executable,
            expected.as_ref(),
        )
        .await
    }

    /// Keep the local file version where the recorded remote side is a
    /// deletion. Identical current content is confirmed without mutation;
    /// otherwise the create is staged against the recorded predicate so a
    /// newer foreign version can never be replaced.
    async fn publish_keep_local_over_recorded_deletion(
        &mut self,
        record: &ConflictRecord,
        baseline: Option<&BaselineRecord>,
        path: &str,
        bytes: Vec<u8>,
        executable: bool,
        expected: Option<LocalNode>,
    ) -> Result<bool> {
        if let Some((snapshot, node)) = matching_remote_file(self, path, &bytes, executable).await?
        {
            // The head already holds exactly the chosen version: confirming it
            // overwrites nothing and is the selected version on both sides.
            let accepted = expected
                .as_ref()
                .map(|node| chosen_file_local(path, &bytes, executable, Some(node)));
            if self
                .apply_resolved_baseline(
                    record,
                    path,
                    snapshot.revision(),
                    Some(node.id()),
                    accepted.as_ref(),
                )
                .await?
            {
                self.clear_resolved_conflict(&record.id)?;
            }
            return Ok(true);
        }
        let Some(predicate) = revision_bytes(record.remote_revision).or_else(|| {
            baseline
                .and_then(|b| b.remote.get(path))
                .map(|entry| entry.revision)
        }) else {
            return Ok(true);
        };
        self.publish_keep_local_create(
            record,
            predicate,
            path,
            executable,
            &bytes,
            expected.as_ref(),
        )
        .await
    }

    /// Keep the local file version at a path the recorded remote side lacks:
    /// create it under the remote parent, creating any missing ancestor
    /// directories first, all against the recorded predicate.
    async fn publish_keep_local_create(
        &mut self,
        record: &ConflictRecord,
        predicate: Revision,
        path: &str,
        executable: bool,
        bytes: &[u8],
        local: Option<&LocalNode>,
    ) -> Result<bool> {
        let recorded_remote = {
            let recorded = self.sync.authority().observe_revision(predicate).await?;
            crate::directory_nodes(&recorded).await?
        };
        let (parent, name) = split(path);
        let (parent_node, predicate) = if parent.is_empty() || recorded_remote.contains_key(parent)
        {
            (
                self.resolve_remote_node(&recorded_remote, parent)?,
                predicate,
            )
        } else {
            let Some((node, predicate)) = self
                .ensure_remote_ancestors(record, &recorded_remote, predicate, parent)
                .await?
            else {
                return Ok(false);
            };
            (node, predicate)
        };
        let artifact = self.replica.retain_bytes(path, bytes)?;
        let artifact_id = Some(artifact.clone());
        let mut source = bytes;
        let edit = self
            .sync
            .stage_create_file(
                parent_node,
                name,
                predicate,
                FileMetadata::new(executable),
                &mut source,
            )
            .await?;
        let node_id = self.namespace_created_node(edit).await?;
        self.associate_resolution(
            record,
            edit,
            path,
            "create-file",
            local,
            Some(parent_node),
            predicate,
            Some(artifact),
        )?;
        match self.sync.publish(edit).await {
            Ok(published) => {
                let accepted = self.install_chosen_bytes(path, bytes, executable, local)?;
                self.refresh_parent_baseline(path, published).await?;
                let resolved = self
                    .apply_resolved_baseline(
                        record,
                        path,
                        published,
                        Some(node_id),
                        accepted.as_ref(),
                    )
                    .await?;
                if resolved {
                    self.clear_resolved_conflict(&record.id)?;
                }
                self.replica.store_mut().update_work(
                    edit.as_bytes(),
                    "committed",
                    Some(published),
                    None,
                )?;
                if resolved {
                    self.replica.store_mut().remove_work(edit.as_bytes())?;
                    if let Some(artifact) = artifact_id.as_deref() {
                        self.replica.release_artifact(artifact)?;
                    }
                }
                Ok(true)
            }
            Err(error) => {
                self.replica.store_mut().update_work(
                    edit.as_bytes(),
                    work_state_for(&error),
                    None,
                    Some(&error.to_string()),
                )?;
                Ok(false)
            }
        }
    }

    /// Finish one conditional content edit chosen by keep-local.
    #[allow(clippy::too_many_arguments)]
    async fn finish_keep_local_edit(
        &mut self,
        record: &ConflictRecord,
        path: &str,
        node_id: NodeId,
        edit: EditId,
        artifact: Option<String>,
        bytes: &[u8],
        predicate: Revision,
        executable: bool,
        local: Option<&LocalNode>,
    ) -> Result<bool> {
        let artifact_id = artifact.clone();
        self.associate_resolution(
            record,
            edit,
            path,
            "content",
            local,
            Some(node_id),
            predicate,
            artifact,
        )?;
        match self.sync.publish(edit).await {
            Ok(revision) => {
                let accepted = self.install_chosen_bytes(path, bytes, executable, local)?;
                self.refresh_parent_baseline(path, revision).await?;
                let resolved = self
                    .apply_resolved_baseline(
                        record,
                        path,
                        revision,
                        Some(node_id),
                        accepted.as_ref(),
                    )
                    .await?;
                if resolved {
                    self.clear_resolved_conflict(&record.id)?;
                }
                self.replica.store_mut().update_work(
                    edit.as_bytes(),
                    "committed",
                    Some(revision),
                    None,
                )?;
                if resolved {
                    self.replica.store_mut().remove_work(edit.as_bytes())?;
                    if let Some(artifact) = artifact_id.as_deref() {
                        self.replica.release_artifact(artifact)?;
                    }
                }
                Ok(true)
            }
            Err(error) if error.kind() == crate::runtime::ErrorKind::Unknown => {
                self.replica.store_mut().update_work(
                    edit.as_bytes(),
                    "unknown",
                    None,
                    Some(&error.to_string()),
                )?;
                Ok(false)
            }
            Err(error) => {
                self.replica.store_mut().update_work(
                    edit.as_bytes(),
                    work_state_for(&error),
                    None,
                    Some(&error.to_string()),
                )?;
                Ok(false)
            }
        }
    }

    /// Keep the local directory subtree: create every local entry that the
    /// recorded remote side lacks (creating ancestors first) and confirm each
    /// published entry's baseline. Entries that already exist on the recorded
    /// remote side are left for normal reconciliation, never overwritten here.
    async fn publish_keep_local_subtree(
        &mut self,
        record: &ConflictRecord,
        path: &str,
    ) -> Result<bool> {
        let current = self.replica.scan()?;
        let root = current
            .get(path)
            .cloned()
            .ok_or_else(|| "keep-local subtree disappeared".to_string())?;
        if !root.is_directory() {
            return Ok(false);
        }
        let Some(predicate) = revision_bytes(record.remote_revision) else {
            return Ok(false);
        };
        let recorded_remote = {
            let recorded = self.sync.authority().observe_revision(predicate).await?;
            crate::directory_nodes(&recorded).await?
        };
        let prefix = format!("{path}/");
        let mut entries: Vec<String> = current
            .nodes()
            .keys()
            .filter(|entry| *entry == path || entry.starts_with(&prefix))
            .cloned()
            .collect();
        entries.sort_by_key(|entry| (entry.matches('/').count(), entry.clone()));
        let mut created: BTreeMap<String, NodeId> = BTreeMap::new();
        let mut running = predicate;
        for entry in &entries {
            let local = current.get(entry).cloned().unwrap();
            if recorded_remote.contains_key(entry) {
                // The recorded side still has this entry; its other version is
                // not the subject of this directory decision.
                continue;
            }
            let (parent, name) = split(entry);
            let parent_id = if let Some(id) = created.get(parent) {
                *id
            } else if parent.is_empty() {
                self.resolve_remote_node(&recorded_remote, "")?
            } else {
                self.resolve_remote_node(&recorded_remote, parent)?
            };
            if local.is_directory() {
                let edit = self
                    .sync
                    .stage_create_directory(parent_id, name, running)
                    .await?;
                self.associate_resolution(
                    record,
                    edit,
                    entry,
                    "create-directory",
                    Some(&local),
                    Some(parent_id),
                    running,
                    None,
                )?;
                match self.sync.publish(edit).await {
                    Ok(published) => {
                        running = published;
                        let node_id = self.namespace_created_node(edit).await?;
                        created.insert(entry.clone(), node_id);
                        self.confirm_baseline(entry, &local, node_id, published)
                            .await?;
                        self.refresh_parent_baseline(entry, published).await?;
                        self.replica.store_mut().update_work(
                            edit.as_bytes(),
                            "committed",
                            Some(published),
                            None,
                        )?;
                        self.replica.store_mut().remove_work(edit.as_bytes())?;
                    }
                    Err(error) => {
                        self.replica.store_mut().update_work(
                            edit.as_bytes(),
                            work_state_for(&error),
                            None,
                            Some(&error.to_string()),
                        )?;
                        return Ok(false);
                    }
                }
            } else {
                let captured = self.replica.capture_file(entry, &local)?;
                let captured_id = captured.artifact.id.clone();
                let mut source =
                    tokio::fs::File::from_std(self.replica.open_artifact(&captured.artifact.id)?);
                let edit = self
                    .sync
                    .stage_create_file(
                        parent_id,
                        name,
                        running,
                        FileMetadata::new(local.executable),
                        &mut source,
                    )
                    .await?;
                let node_id = self.namespace_created_node(edit).await?;
                self.associate_resolution(
                    record,
                    edit,
                    entry,
                    "create-file",
                    Some(&local),
                    Some(parent_id),
                    running,
                    Some(captured_id.clone()),
                )?;
                match self.sync.publish(edit).await {
                    Ok(published) => {
                        running = published;
                        self.confirm_baseline(entry, &local, node_id, published)
                            .await?;
                        self.refresh_parent_baseline(entry, published).await?;
                        self.replica.store_mut().update_work(
                            edit.as_bytes(),
                            "committed",
                            Some(published),
                            None,
                        )?;
                        self.replica.store_mut().remove_work(edit.as_bytes())?;
                        self.replica.release_artifact(&captured_id)?;
                    }
                    Err(error) => {
                        self.replica.store_mut().update_work(
                            edit.as_bytes(),
                            work_state_for(&error),
                            None,
                            Some(&error.to_string()),
                        )?;
                        return Ok(false);
                    }
                }
            }
        }
        self.clear_conflicts_under(path).await?;
        Ok(true)
    }

    /// Create every missing recorded-remote ancestor of `parent`, returning
    /// the remote parent node and the running revision. Returns None when
    /// publication of an ancestor stalled; the recoverable work record stays
    /// linked.
    async fn ensure_remote_ancestors(
        &mut self,
        record: &ConflictRecord,
        recorded_remote: &BTreeMap<String, Node>,
        revision: Revision,
        parent: &str,
    ) -> Result<Option<(NodeId, Revision)>> {
        let mut missing: Vec<&str> = Vec::new();
        let mut cursor = Some(parent);
        while let Some(path) = cursor {
            if path.is_empty() || recorded_remote.contains_key(path) {
                break;
            }
            missing.push(path);
            cursor = split(path).0.into();
        }
        missing.sort_by_key(|path| path.matches('/').count());
        let mut running = revision;
        let mut created: BTreeMap<String, NodeId> = BTreeMap::new();
        for path in &missing {
            let (parent_path, name) = split(path);
            let parent_id = if let Some(id) = created.get(parent_path) {
                *id
            } else {
                self.resolve_remote_node(recorded_remote, parent_path)?
            };
            let edit = self
                .sync
                .stage_create_directory(parent_id, name, running)
                .await?;
            let local = self.replica.observe_path(path)?;
            self.associate_resolution(
                record,
                edit,
                path,
                "create-directory",
                local.as_ref(),
                Some(parent_id),
                running,
                None,
            )?;
            match self.sync.publish(edit).await {
                Ok(published) => {
                    running = published;
                    let node_id = self.namespace_created_node(edit).await?;
                    created.insert(path.to_string(), node_id);
                    self.confirm_baseline_for(path, local.as_ref(), node_id, published)
                        .await?;
                    self.refresh_parent_baseline(path, published).await?;
                    self.replica.store_mut().update_work(
                        edit.as_bytes(),
                        "committed",
                        Some(published),
                        None,
                    )?;
                    self.replica.store_mut().remove_work(edit.as_bytes())?;
                }
                Err(error) => {
                    self.replica.store_mut().update_work(
                        edit.as_bytes(),
                        work_state_for(&error),
                        None,
                        Some(&error.to_string()),
                    )?;
                    return Ok(None);
                }
            }
        }
        Ok(created.get(parent).copied().map(|node| (node, running)))
    }

    /// Keep the recorded remote deletion: quarantine any current local path
    /// (preserving its bytes as an artifact), drop the accepted baseline
    /// subtree and clear the conflict once the local side is durably gone.
    async fn apply_keep_remote_deletion(
        &mut self,
        record: &ConflictRecord,
        path: &str,
    ) -> Result<bool> {
        let quarantined = if self.replica.observe_path(path)?.is_some() {
            let current = self.replica.scan_subtree(path)?;
            Some(self.replica.quarantine(
                path,
                &current,
                "conflict resolution kept the remote deletion",
                record.remote_revision,
            )?)
        } else {
            None
        };
        self.replica.store_mut().remove_baseline_subtree(path)?;
        if record.path != path {
            self.replica
                .store_mut()
                .remove_baseline_subtree(&record.path)?;
        }
        self.clear_resolved_conflict(&record.id)?;
        if let Some(artifact) = quarantined {
            self.replica.release_artifact(&artifact.id)?;
        }
        Ok(true)
    }

    /// Keep the recorded remote file: install the chosen bytes locally,
    /// retaining any divergent local bytes as an artifact first, then confirm
    /// the baseline for the exact selected node.
    async fn materialize_remote_file(
        &mut self,
        record: &ConflictRecord,
        path: &str,
        snapshot: &Snapshot,
        revision: Revision,
        node: Node,
    ) -> Result<bool> {
        let bytes = self.download_remote_bytes(snapshot, &node).await?;
        let expected = self.replica.observe_path(path)?;
        match expected {
            Some(expected) if !expected.is_directory() => {
                self.replica.install_bytes(
                    path,
                    &expected,
                    node.executable(),
                    Some(revision.to_bytes()),
                    &bytes,
                )?;
            }
            _ => {
                self.replica
                    .create_file_local(path, node.executable(), &mut &bytes[..])?;
            }
        }
        let Some(accepted) = self.verified_local_file(path, &bytes)? else {
            return Ok(false);
        };
        if self
            .apply_resolved_baseline(record, path, revision, Some(node.id()), Some(&accepted))
            .await?
        {
            self.clear_resolved_conflict(&record.id)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Keep the recorded remote directory: create every recorded entry that is
    /// missing locally (never deleting existing local entries, whose changes
    /// stay pending or conflicting), then confirm baselines for exactly the
    /// recorded remote tree.
    async fn materialize_remote_subtree(
        &mut self,
        path: &str,
        snapshot: &Snapshot,
        revision: Revision,
    ) -> Result<bool> {
        let current = self.replica.scan()?;
        if current.get(path).is_none() {
            self.replica.create_directory_local(path)?;
        }
        let tree = self.recorded_subtree(snapshot, path).await?;
        let mut pending: Vec<(String, Node)> = tree
            .iter()
            .map(|(entry, node)| (entry.clone(), node.clone()))
            .collect();
        pending.sort_by_key(|(entry, _)| (entry.matches('/').count(), entry.clone()));
        let mut materialized: BTreeMap<String, Vec<u8>> = BTreeMap::new();
        for (entry, node) in &pending {
            if self.replica.observe_path(entry)?.is_some() {
                continue;
            }
            if node.is_directory() {
                self.replica.create_directory_local(entry)?;
            } else {
                let bytes = self.download_remote_bytes(snapshot, node).await?;
                self.replica
                    .create_file_local(entry, node.executable(), &mut &bytes[..])?;
                materialized.insert(entry.clone(), bytes);
            }
        }
        for (entry, node) in &pending {
            let accepted = if node.is_directory() {
                match self.verified_local_directory(entry)? {
                    Some(accepted) => accepted,
                    None => return Ok(false),
                }
            } else {
                let live = match self.replica.observe_path(entry)? {
                    Some(live) => live,
                    None => return Ok(false),
                };
                match materialized.get(entry) {
                    // Freshly materialized bytes verify against themselves.
                    Some(bytes) => match local_file_matches(&live, bytes)? {
                        true => live,
                        false => return Ok(false),
                    },
                    // An existing local entry was intentionally left alone; the
                    // baseline can still accept the recorded remote version.
                    None => live,
                }
            };
            self.confirm_baseline_for(entry, Some(&accepted), node.id(), revision)
                .await?;
        }
        // Drop baselines for recorded-remote-deleted descendants that no
        // longer exist on either side; local-only entries keep no baseline and
        // stay pending.
        let prefix = format!("{path}/");
        let candidates = self
            .replica
            .store()
            .load_baseline()?
            .map(|baseline| {
                baseline
                    .remote
                    .keys()
                    .filter(|entry| entry.starts_with(&prefix) && !tree.contains_key(*entry))
                    .cloned()
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let mut stale = Vec::new();
        for entry in candidates {
            if self.replica.observe_path(&entry)?.is_none() {
                stale.push(entry);
            }
        }
        for entry in stale {
            self.replica.store_mut().remove_baseline_path(&entry)?;
        }
        self.clear_conflicts_under(path).await?;
        Ok(true)
    }

    /// Read every node under `path` from one pinned snapshot, including `path`
    /// itself.
    async fn recorded_subtree(
        &self,
        snapshot: &Snapshot,
        path: &str,
    ) -> Result<BTreeMap<String, Node>> {
        let mut map = BTreeMap::new();
        let Some(root) = snapshot.resolve(path).await? else {
            return Ok(map);
        };
        let mut pending = vec![(String::new(), root.id())];
        while let Some((rel, id)) = pending.pop() {
            let Some(node) = snapshot.node(id).await? else {
                continue;
            };
            if node.is_directory() {
                let mut token = None;
                loop {
                    let page = snapshot.scan(id, token.as_ref(), 4096).await?;
                    for entry in page.entries {
                        let child = if rel.is_empty() {
                            entry.name.clone()
                        } else {
                            format!("{rel}/{}", entry.name)
                        };
                        pending.push((child, entry.node_id));
                    }
                    match page.next {
                        Some(next) => token = Some(next),
                        None => break,
                    }
                }
            }
            let full = if rel.is_empty() {
                path.to_owned()
            } else {
                format!("{path}/{rel}")
            };
            map.insert(full, node);
        }
        Ok(map)
    }

    /// Save the baseline for one path this resolution published, using the
    /// pre-dispatch local fingerprint and inode identity.
    async fn confirm_baseline(
        &mut self,
        path: &str,
        local: &LocalNode,
        node_id: NodeId,
        revision: Revision,
    ) -> Result<()> {
        self.confirm_baseline_for(path, Some(local), node_id, revision)
            .await
    }

    async fn confirm_baseline_for(
        &mut self,
        path: &str,
        local: Option<&LocalNode>,
        node_id: NodeId,
        revision: Revision,
    ) -> Result<()> {
        let snapshot = self.sync.authority().observe_revision(revision).await?;
        let Some(node) = snapshot.node(node_id).await? else {
            return Ok(());
        };
        self.replica
            .store_mut()
            .save_baseline_path(path, &node, local, revision)?;
        Ok(())
    }

    /// Drop every active conflict that belongs to a resolved directory
    /// subtree: the same local or remote version decision covers each of them.
    async fn clear_conflicts_under(&mut self, path: &str) -> Result<()> {
        let prefix = format!("{path}/");
        let ids: Vec<String> = self
            .replica
            .store()
            .conflicts()?
            .into_iter()
            .filter(|conflict| conflict.path == path || conflict.path.starts_with(&prefix))
            .map(|conflict| conflict.id)
            .collect();
        for id in ids {
            self.clear_resolved_conflict(&id)?;
        }
        Ok(())
    }

    /// Resolve the node identity created by one staged namespace intent.
    async fn namespace_created_node(&self, edit: EditId) -> Result<NodeId> {
        let status = self.sync.namespace_status(edit).await?;
        Ok(status.intent.operation.node_id())
    }

    fn verified_local_file(&self, path: &str, bytes: &[u8]) -> Result<Option<LocalNode>> {
        let Some(node) = self.replica.observe_path(path)? else {
            return Ok(None);
        };
        if matches!(node.kind, LocalKind::File { .. }) && local_file_matches(&node, bytes)? {
            Ok(Some(node))
        } else {
            Ok(None)
        }
    }

    fn verified_local_directory(&self, path: &str) -> Result<Option<LocalNode>> {
        match self.replica.observe_path(path)? {
            Some(node) if matches!(node.kind, LocalKind::Directory) => Ok(Some(node)),
            _ => Ok(None),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn associate_resolution(
        &mut self,
        record: &ConflictRecord,
        edit: EditId,
        path: &str,
        operation: &str,
        local: Option<&LocalNode>,
        baseline_node: Option<NodeId>,
        baseline_revision: Revision,
        source_artifact: Option<String>,
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
            original_path: record.original_path.clone(),
            desired_path: Some(path.to_owned()),
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
            conflict: Some(record.id.clone()),
            source_artifact,
            upload_artifact: None,
            state: "prepared".to_owned(),
            error: None,
        })?;
        Ok(())
    }
}

/// The chosen local side of a keep-local file: chosen bytes plus the
/// pre-dispatch inode identity, never a post-receipt scan.
fn chosen_file_local(
    path: &str,
    bytes: &[u8],
    executable: bool,
    identity: Option<&LocalNode>,
) -> LocalNode {
    LocalNode {
        path: path.to_owned(),
        kind: LocalKind::File {
            length: bytes.len() as u64,
            digest: ContentHash::from_bytes(*blake3::hash(bytes).as_bytes()),
        },
        executable,
        identity: identity
            .map(|node| node.identity)
            .unwrap_or(Identity { dev: 0, ino: 0 }),
        parent: identity.and_then(|node| node.parent),
        stat: identity.and_then(|node| node.stat),
    }
}

/// Compare one local file with raw chosen bytes. Local observations keep the
/// raw content hash, while remote nodes keep the canonical content identity.
fn local_file_matches(node: &LocalNode, bytes: &[u8]) -> Result<bool> {
    match node.kind {
        LocalKind::File { length, digest } => {
            Ok(length == bytes.len() as u64 && digest.as_bytes() == blake3::hash(bytes).as_bytes())
        }
        LocalKind::Directory => Ok(false),
    }
}

/// Find the current remote file that holds exactly the chosen bytes, returning
/// the single observation used both to compare and to acknowledge the
/// baseline. Nothing is mutated here.
async fn matching_remote_file(
    directory: &DirectorySync,
    path: &str,
    bytes: &[u8],
    executable: bool,
) -> Result<Option<(Snapshot, Node)>> {
    let snapshot = directory.sync.authority().observe_latest().await?;
    let Some(node) = snapshot.resolve(path).await? else {
        return Ok(None);
    };
    if node.is_directory() {
        return Ok(None);
    }
    let NodeKind::File(file) = node.kind() else {
        return Ok(None);
    };
    if node.executable() != executable {
        return Ok(None);
    }
    let content = ContentId::calculate(&mut &bytes[..]).await?;
    if file.content_id() == content {
        Ok(Some((snapshot, node)))
    } else {
        Ok(None)
    }
}
