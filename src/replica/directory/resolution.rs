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
//! mutation is staged against the conflict's recorded remote side and
//! predicate; a newer head is never adopted silently. Directory and deletion
//! decisions publish or materialize the corresponding tree through the same
//! persistent `Sync`/`Replica` capabilities, keep the recoverable work
//! association, and confirm the common baseline only for the version that was
//! actually selected and made durable on both sides.

use super::{DirectorySync, Resolution, Result, revision_bytes, split, work_state_for};
use crate::replica::state::{BaselineRecord, ConflictRecord, WorkRecord};
use crate::replica::{ContentHash, Identity, LocalKind, LocalNode};
use crate::sync::{EditId, FileMetadata};
use std::collections::BTreeMap;
use std::io::{Seek as _, SeekFrom};
use yinyang_core::{Node, NodeId, NodeKind, Revision, Snapshot};

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
        // The recorded remote side pins every predicate below. The fresh head
        // is observed only to confirm equality with the chosen version, never
        // to overwrite a changed remote side.
        let recorded = match revision_bytes(record.remote_revision) {
            Some(revision) => self.sync.authority().observe_revision(revision).await?,
            None => self.sync.authority().observe_latest().await?,
        };
        let recorded_remote = crate::directory_nodes(&recorded).await?;
        let bytes = match &decision {
            Resolution::Content(bytes) => Some(bytes.clone()),
            _ => None,
        };
        let current = self.replica.scan()?;
        match (bytes, current.get(&path)) {
            (Some(bytes), _) => {
                self.publish_keep_local_file(record, &recorded, &recorded_remote, baseline, bytes)
                    .await
            }
            (None, None) => {
                // The user keeps the local deletion: publish the removal of the
                // recorded remote node against its recorded predicate.
                self.publish_keep_local_delete(record, baseline, recorded.revision())
                    .await
            }
            (None, Some(node)) if node.is_directory() => {
                self.publish_keep_local_subtree(
                    record,
                    &recorded_remote,
                    recorded.revision(),
                    &path,
                )
                .await
            }
            (None, Some(_)) => {
                let bytes = self.local_side_bytes(record)?;
                self.publish_keep_local_file(record, &recorded, &recorded_remote, baseline, bytes)
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
        let Some(node_id) = record.remote_node else {
            // The selected remote side is a deletion. Apply the local removal
            // durably; the quarantine keeps any new local bytes exportable.
            return self.apply_keep_remote_deletion(record, &path).await;
        };
        // Keep the version recorded by the conflict, exactly the version the
        // adapter could have exported; never a newer head.
        let snapshot = match revision_bytes(record.remote_revision) {
            Some(revision) => self.sync.authority().observe_revision(revision).await?,
            None => observed.clone(),
        };
        let revision = snapshot.revision();
        let Some(node) = snapshot.node(NodeId::from_bytes(node_id)).await? else {
            return Ok(false);
        };
        if node.is_directory() {
            self.materialize_remote_subtree(record, &path, &snapshot, revision)
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
                .remove_baseline_path(&record.path)?;
        }
        if let Some(accepted) = accepted {
            self.replica
                .store_mut()
                .save_baseline_path(path, &node, Some(accepted), revision)?;
        }
        Ok(true)
    }

    /// Write the exactly chosen bytes into the local payload so it matches the
    /// acknowledged remote version.
    fn install_local_bytes(&mut self, path: &str, bytes: &[u8]) -> Result<()> {
        let expected = self.replica.scan()?.get(path).cloned();
        match expected {
            Some(expected) if matches!(expected.kind, LocalKind::File { .. }) => {
                self.replica
                    .install_bytes(path, &expected, expected.executable, None, bytes)?;
            }
            Some(_) => {}
            None => {
                let mut source = bytes;
                self.replica.create_file_local(path, false, &mut source)?;
            }
        }
        Ok(())
    }

    async fn download_remote(&self, snapshot: &Snapshot, node: &Node) -> Result<std::fs::File> {
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
        fallback: Revision,
    ) -> Result<bool> {
        let path = record
            .desired_path
            .clone()
            .unwrap_or_else(|| record.path.clone());
        let Some(node_bytes) = record.remote_node else {
            self.replica.store_mut().remove_baseline_path(&path)?;
            self.replica
                .store_mut()
                .remove_baseline_path(&record.path)?;
            self.clear_resolved_conflict(&record.id)?;
            return Ok(true);
        };
        let predicate = revision_bytes(record.remote_revision)
            .or_else(|| {
                baseline
                    .and_then(|b| b.remote.get(&path))
                    .map(|entry| entry.revision)
            })
            .unwrap_or(fallback);
        let node = NodeId::from_bytes(node_bytes);
        let edit = self.sync.stage_remove(node, predicate).await?;
        self.associate_resolution(
            record,
            edit,
            &path,
            "remove",
            None,
            Some(node),
            predicate,
            None,
        )?;
        match self.sync.publish(edit).await {
            Ok(revision) => {
                self.replica.store_mut().remove_baseline_path(&path)?;
                self.replica
                    .store_mut()
                    .remove_baseline_path(&record.path)?;
                self.clear_resolved_conflict(&record.id)?;
                self.replica.store_mut().update_work(
                    edit.as_bytes(),
                    "committed",
                    Some(revision),
                    None,
                )?;
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

    /// Keep the local file version. An existing recorded remote node is edited
    /// only at the recorded predicate; a recorded remote absence publishes a
    /// conditional create. A remote path that now holds a different version is
    /// never overwritten: the conflict stays active so the next run refreshes
    /// its evidence.
    async fn publish_keep_local_file(
        &mut self,
        record: &ConflictRecord,
        recorded: &Snapshot,
        recorded_remote: &BTreeMap<String, Node>,
        baseline: Option<&BaselineRecord>,
        bytes: Vec<u8>,
    ) -> Result<bool> {
        let path = record
            .desired_path
            .clone()
            .unwrap_or_else(|| record.path.clone());
        let expected = self.replica.scan()?.get(&path).cloned();
        let executable = expected
            .as_ref()
            .filter(|node| matches!(node.kind, LocalKind::File { .. }))
            .map_or(false, |node| node.executable);
        let predicate = revision_bytes(record.remote_revision)
            .or_else(|| {
                baseline
                    .and_then(|b| b.remote.get(&path))
                    .map(|entry| entry.revision)
            })
            .unwrap_or_else(|| recorded.revision());
        let Some(node) = recorded_remote.get(&path).cloned() else {
            // Recorded remote absence: the desired path only exists at a newer
            // head. Confirm it when it is identical to the chosen bytes
            // (nothing would be overwritten); otherwise publish the create at
            // the recorded predicate, whose absence guard keeps a newer
            // foreign version from being replaced.
            if remote_file_matches_observed(self, &path, &bytes, executable).await? {
                let accepted = chosen_file_local(&path, &bytes, executable, expected.as_ref());
                let observed = self.sync.authority().observe_latest().await?;
                if self
                    .apply_resolved_baseline(
                        record,
                        &path,
                        observed.revision(),
                        observed.resolve(&path).await?.map(|node| node.id()),
                        Some(&accepted),
                    )
                    .await?
                {
                    self.clear_resolved_conflict(&record.id)?;
                }
                return Ok(true);
            }
            return self
                .publish_keep_local_create(
                    record,
                    recorded_remote,
                    &path,
                    executable,
                    &bytes,
                    expected.as_ref(),
                    predicate,
                )
                .await;
        };
        if node.is_directory() {
            // A file decision cannot replace a remote directory; the next run
            // keeps reporting the kind conflict.
            return Ok(false);
        }
        if *node.id().as_bytes() != record.remote_node.unwrap_or([0; 16]) {
            // The remote side changed since the conflict was recorded. Confirm
            // identical content
            // or leave the conflict for a refreshed run; never edit a newer
            // node against a stale predicate.
            if remote_file_matches_observed(self, &path, &bytes, executable).await? {
                let accepted = chosen_file_local(&path, &bytes, executable, expected.as_ref());
                let observed = self.sync.authority().observe_latest().await?;
                if self
                    .apply_resolved_baseline(
                        record,
                        &path,
                        observed.revision(),
                        observed.resolve(&path).await?.map(|node| node.id()),
                        Some(&accepted),
                    )
                    .await?
                {
                    self.clear_resolved_conflict(&record.id)?;
                }
                return Ok(true);
            }
            return Ok(false);
        }
        // The recorded remote node: stage the chosen bytes against its recorded
        // predicate, and keep the exact chosen input durable.
        let artifact = self.replica.retain_bytes(&path, &bytes)?;
        let mut source = &bytes[..];
        let edit = self.sync.stage(node.id(), predicate, &mut source).await?;
        self.finish_keep_local_edit(
            record,
            &path,
            node.id(),
            edit,
            Some(artifact),
            &bytes,
            predicate,
            expected.as_ref(),
        )
        .await
    }

    /// Keep the local file version at a path the recorded remote side lacks:
    /// create it under the recorded remote parent, creating any missing
    /// ancestor directories first, all against the recorded predicate.
    #[allow(clippy::too_many_arguments)]
    async fn publish_keep_local_create(
        &mut self,
        record: &ConflictRecord,
        recorded_remote: &BTreeMap<String, Node>,
        path: &str,
        executable: bool,
        bytes: &[u8],
        local: Option<&LocalNode>,
        predicate: Revision,
    ) -> Result<bool> {
        let (parent, name) = split(path);
        let (parent_node, predicate) = if parent.is_empty() || recorded_remote.contains_key(parent)
        {
            (
                self.resolve_remote_node(recorded_remote, parent)?,
                predicate,
            )
        } else {
            let Some((node, predicate)) = self
                .ensure_remote_ancestors(record, recorded_remote, predicate, parent)
                .await?
            else {
                return Ok(false);
            };
            (node, predicate)
        };
        let artifact = self.replica.retain_bytes(path, bytes)?;
        let mut source = &bytes[..];
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
                self.install_local_bytes(path, bytes)?;
                let accepted = self.accepted_local_file(path, bytes, executable, local)?;
                if self
                    .apply_resolved_baseline(
                        record,
                        path,
                        published,
                        Some(node_id),
                        accepted.as_ref(),
                    )
                    .await?
                {
                    self.clear_resolved_conflict(&record.id)?;
                }
                self.replica.store_mut().update_work(
                    edit.as_bytes(),
                    "committed",
                    Some(published),
                    None,
                )?;
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
    async fn finish_keep_local_edit(
        &mut self,
        record: &ConflictRecord,
        path: &str,
        node_id: NodeId,
        edit: EditId,
        artifact: Option<String>,
        bytes: &[u8],
        predicate: Revision,
        local: Option<&LocalNode>,
    ) -> Result<bool> {
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
                self.install_local_bytes(path, bytes)?;
                let accepted = self.accepted_local_file(
                    path,
                    bytes,
                    local.map_or(false, |n| n.executable),
                    local,
                )?;
                if self
                    .apply_resolved_baseline(
                        record,
                        path,
                        revision,
                        Some(node_id),
                        accepted.as_ref(),
                    )
                    .await?
                {
                    self.clear_resolved_conflict(&record.id)?;
                }
                self.replica.store_mut().update_work(
                    edit.as_bytes(),
                    "committed",
                    Some(revision),
                    None,
                )?;
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
    /// remote side with another version are left for normal reconciliation,
    /// never overwritten here.
    async fn publish_keep_local_subtree(
        &mut self,
        record: &ConflictRecord,
        recorded_remote: &BTreeMap<String, Node>,
        revision: Revision,
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
        let prefix = format!("{path}/");
        let mut entries: Vec<String> = current
            .nodes()
            .keys()
            .filter(|entry| *entry == path || entry.starts_with(&prefix))
            .cloned()
            .collect();
        entries.sort_by_key(|entry| (entry.matches('/').count(), entry.clone()));
        let mut created: BTreeMap<String, NodeId> = BTreeMap::new();
        let mut running = revision;
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
                self.resolve_remote_node(recorded_remote, "")?
            } else {
                self.resolve_remote_node(recorded_remote, parent)?
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
                        self.replica.store_mut().update_work(
                            edit.as_bytes(),
                            "committed",
                            Some(published),
                            None,
                        )?;
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
                    Some(captured.artifact.id),
                )?;
                match self.sync.publish(edit).await {
                    Ok(published) => {
                        running = published;
                        self.confirm_baseline(entry, &local, node_id, published)
                            .await?;
                        self.replica.store_mut().update_work(
                            edit.as_bytes(),
                            "committed",
                            Some(published),
                            None,
                        )?;
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
        self.clear_resolved_conflict(&record.id)?;
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
            let local = self.replica.scan()?.get(path).cloned();
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
                    self.replica.store_mut().update_work(
                        edit.as_bytes(),
                        "committed",
                        Some(published),
                        None,
                    )?;
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
    /// (preserving its bytes as an artifact), drop the stale baselines and
    /// clear the conflict once the local side is durably gone.
    async fn apply_keep_remote_deletion(
        &mut self,
        record: &ConflictRecord,
        path: &str,
    ) -> Result<bool> {
        let current = self.replica.scan()?;
        if current.get(path).is_some() {
            self.replica.quarantine(
                path,
                &current,
                "conflict resolution kept the remote deletion",
                record.remote_revision,
            )?;
        }
        let baseline = self.replica.store().load_baseline()?;
        if let Some(baseline) = baseline.as_ref() {
            let prefix = format!("{path}/");
            let stale: Vec<String> = baseline
                .remote
                .keys()
                .filter(|entry| *entry == path || entry.starts_with(&prefix))
                .cloned()
                .collect();
            for entry in stale {
                self.replica.store_mut().remove_baseline_path(&entry)?;
            }
        }
        self.replica
            .store_mut()
            .remove_baseline_path(&record.path)?;
        self.clear_resolved_conflict(&record.id)?;
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
        let mut staged = self.download_remote(snapshot, &node).await?;
        let expected = self.replica.scan()?.get(path).cloned();
        match expected {
            Some(expected) if !expected.is_directory() => {
                let mut bytes = Vec::new();
                std::io::copy(&mut staged, &mut bytes).map_err(|e| e.to_string())?;
                let current_digest = match expected.kind {
                    LocalKind::File { digest, .. } => Some(*digest.as_bytes()),
                    LocalKind::Directory => None,
                };
                let remote_digest = match node.kind() {
                    NodeKind::File(file) => Some(*file.content_id().digest()),
                    _ => None,
                };
                if current_digest != remote_digest {
                    // Preserve the replaced local bytes as a retained artifact.
                    self.replica.capture_file(path, &expected)?;
                }
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
                    .create_file_local(path, node.executable(), &mut staged)?;
            }
        }
        let Some((length, digest)) = node_file_content(&node) else {
            return Ok(false);
        };
        let Some(accepted) = self.verified_local_file(path, &digest, length)? else {
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
        record: &ConflictRecord,
        path: &str,
        snapshot: &Snapshot,
        revision: Revision,
    ) -> Result<bool> {
        let current = self.replica.scan()?;
        if current.get(path).is_none() {
            self.replica.create_directory_local(path)?;
        }
        let tree = self.recorded_subtree(snapshot, path).await?;
        let mut pending: Vec<(String, Node)> = tree.into_iter().collect();
        pending.sort_by_key(|(entry, _)| (entry.matches('/').count(), entry.clone()));
        for (entry, node) in &pending {
            if self.replica.scan()?.get(entry).is_some() {
                continue;
            }
            if node.is_directory() {
                self.replica.create_directory_local(entry)?;
            } else {
                let mut staged = self.download_remote(snapshot, node).await?;
                self.replica
                    .create_file_local(entry, node.executable(), &mut staged)?;
            }
        }
        for (entry, node) in &pending {
            let accepted = if node.is_directory() {
                match self.verified_local_directory(entry)? {
                    Some(accepted) => accepted,
                    None => return Ok(false),
                }
            } else {
                let Some((length, digest)) = node_file_content(node) else {
                    return Ok(false);
                };
                match self.verified_local_file(entry, &digest, length)? {
                    Some(accepted) => accepted,
                    None => return Ok(false),
                }
            };
            self.confirm_baseline_for(entry, Some(&accepted), node.id(), revision)
                .await?;
        }
        // Drop baselines for recorded-remote-deleted descendants that no
        // longer exist on either side; local-only entries keep no baseline and
        // stay pending.
        let stale: Vec<String> = {
            let baseline = self.replica.store().load_baseline()?;
            let fresh = self.replica.scan()?;
            let prefix = format!("{path}/");
            baseline
                .as_ref()
                .map(|baseline| {
                    baseline
                        .remote
                        .keys()
                        .filter(|entry| {
                            entry.starts_with(&prefix)
                                && !tree.contains_key(*entry)
                                && fresh.get(*entry).is_none()
                        })
                        .cloned()
                        .collect()
                })
                .unwrap_or_default()
        };
        for entry in stale {
            self.replica.store_mut().remove_baseline_path(&entry)?;
        }
        self.clear_resolved_conflict(&record.id)?;
        Ok(true)
    }

    /// Read every node under `path` from one pinned snapshot, in remote
    /// order, including `path` itself.
    async fn recorded_subtree(
        &self,
        snapshot: &Snapshot,
        path: &str,
    ) -> Result<BTreeMap<String, Node>> {
        let mut map = BTreeMap::new();
        let Some(root_id) = snapshot.resolve(path).await? else {
            return Ok(map);
        };
        let mut pending = vec![(String::new(), root_id)];
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

    /// Resolve the node identity created by one staged namespace intent.
    async fn namespace_created_node(&self, edit: EditId) -> Result<NodeId> {
        let status = self.sync.namespace_status(edit).await?;
        Ok(status.intent.operation.node_id())
    }

    /// Build the accepted local fingerprint for a keep-local file: the chosen
    /// bytes plus the pre-dispatch inode identity. When no local file existed
    /// before dispatch, the just-installed file is verified against the chosen
    /// bytes instead.
    fn accepted_local_file(
        &mut self,
        path: &str,
        bytes: &[u8],
        executable: bool,
        pre_dispatch: Option<&LocalNode>,
    ) -> Result<Option<LocalNode>> {
        match pre_dispatch {
            Some(node) => Ok(Some(chosen_file_local(path, bytes, executable, Some(node)))),
            None => {
                self.verified_local_file(path, blake3::hash(bytes).as_bytes(), bytes.len() as u64)
            }
        }
    }

    fn verified_local_file(
        &self,
        path: &str,
        digest: &[u8; 32],
        length: u64,
    ) -> Result<Option<LocalNode>> {
        match self.replica.scan()?.get(path).cloned() {
            Some(node) => match node.kind {
                LocalKind::File {
                    digest: current,
                    length: current_length,
                } if current.as_bytes() == digest && current_length == length => Ok(Some(node)),
                _ => Ok(None),
            },
            None => Ok(None),
        }
    }

    fn verified_local_directory(&self, path: &str) -> Result<Option<LocalNode>> {
        match self.replica.scan()?.get(path).cloned() {
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
    }
}

/// Confirm that the fresh head holds the chosen bytes at `path` without
/// mutating it: resolving a keep-local file against identical remote content
/// overwrites nothing and is the actual selected version on both sides.
async fn remote_file_matches_observed(
    directory: &DirectorySync,
    path: &str,
    bytes: &[u8],
    executable: bool,
) -> Result<bool> {
    let observed = directory.sync.authority().observe_latest().await?;
    let Some(node) = observed.resolve(path).await? else {
        return Ok(false);
    };
    if node.is_directory() {
        return Ok(false);
    }
    Ok(remote_file_matches(&node, bytes, executable))
}

fn node_file_content(node: &Node) -> Option<(u64, [u8; 32])> {
    match node.kind() {
        NodeKind::File(file) => Some((file.content_id().length(), *file.content_id().digest())),
        _ => None,
    }
}

fn remote_file_matches(node: &Node, bytes: &[u8], executable: bool) -> bool {
    match node.kind() {
        NodeKind::File(file) => {
            node.executable() == executable
                && file.content_id().digest() == blake3::hash(bytes).as_bytes()
        }
        _ => false,
    }
}
