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
//! durable decision for the exact conflict record it was given: it publishes
//! or materializes only the selected version through `Sync` and `Replica`,
//! keeps the recoverable work association, and confirms the common baseline
//! only after that version is durable on both sides.

use super::{DirectorySync, Resolution, Result, revision_bytes, split, work_state_for};
use crate::replica::LocalKind;
use crate::replica::state::{BaselineRecord, ConflictRecord, WorkRecord};
use crate::sync::{FileMetadata, RenameMode};
use std::collections::BTreeMap;
use std::io::{Seek as _, SeekFrom};
use yinyang_core::{Node, NodeId, Revision, Snapshot};

impl DirectorySync {
    /// Publish the chosen local bytes through the same durable work/receipt
    /// path as normal publication, linked to the conflict. The conflict is
    /// only cleared once the receipt is applied.
    async fn resolve_keep_local(
        &mut self,
        record: &ConflictRecord,
        remote: &BTreeMap<String, Node>,
        baseline: Option<&BaselineRecord>,
        decision: Resolution,
    ) -> Result<bool> {
        let path = record
            .desired_path
            .clone()
            .unwrap_or_else(|| record.path.clone());
        let (parent, name) = split(&path);
        let bytes = match &decision {
            Resolution::Content(bytes) => bytes.clone(),
            _ => self.local_side_bytes(record)?,
        };
        let predicate = revision_bytes(record.remote_revision)
            .or_else(|| {
                baseline
                    .and_then(|b| b.remote.get(&path))
                    .map(|n| n.revision)
            })
            .ok_or_else(|| "conflict has no baseline revision to rebase".to_string())?;
        // Retain the exact chosen input so export and baseline reconstruction
        // survive a crash or a later path change.
        let artifact = self.replica.retain_bytes(&path, &bytes)?;
        let edit = match remote.get(&path) {
            Some(node) if !node.is_directory() => {
                let mut source = &bytes[..];
                self.sync.stage(node.id(), predicate, &mut source).await?
            }
            _ => {
                let parent_node = self.resolve_remote_node(remote, parent)?;
                let mut source = &bytes[..];
                self.sync
                    .stage_create_file(
                        parent_node,
                        name,
                        predicate,
                        FileMetadata::new(false),
                        &mut source,
                    )
                    .await?
            }
        };
        let expected_identity = self.replica.scan()?.get(&path).cloned();
        self.replica.store_mut().insert_work(&WorkRecord {
            id: *edit.as_bytes(),
            path: path.clone(),
            original_path: record.original_path.clone(),
            desired_path: Some(path.clone()),
            operation: "content".to_owned(),
            local_kind: Some("file".to_owned()),
            local_digest: Some(*blake3::hash(&bytes).as_bytes()),
            local_length: Some(bytes.len() as u64),
            local_executable: Some(false),
            local_dev: expected_identity.as_ref().map(|node| node.identity.dev),
            local_ino: expected_identity.as_ref().map(|node| node.identity.ino),
            local_parent_dev: expected_identity
                .as_ref()
                .and_then(|node| node.parent.map(|parent| parent.dev)),
            local_parent_ino: expected_identity
                .as_ref()
                .and_then(|node| node.parent.map(|parent| parent.ino)),
            baseline_node: remote.get(&path).map(|node| *node.id().as_bytes()),
            baseline_revision: predicate.to_bytes(),
            remote_revision: None,
            commit_id: None,
            conflict: Some(record.id.clone()),
            source_artifact: Some(artifact),
            state: "prepared".to_owned(),
            error: None,
        })?;
        match self.sync.publish(edit).await {
            Ok(revision) => {
                self.install_local_bytes(&path, &bytes)?;
                self.apply_resolved_baseline(record, &path, revision)
                    .await?;
                self.clear_resolved_conflict(&record.id)?;
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
                let state = work_state_for(&error);
                self.replica.store_mut().update_work(
                    edit.as_bytes(),
                    state,
                    None,
                    Some(&error.to_string()),
                )?;
                Ok(false)
            }
        }
    }

    /// Apply the recorded remote side: materialize the observed remote node, or
    /// remove the local path when the remote side is a deletion.
    async fn resolve_keep_remote(
        &mut self,
        record: &ConflictRecord,
        observed: &Snapshot,
        remote: &BTreeMap<String, Node>,
    ) -> Result<bool> {
        let path = record
            .desired_path
            .clone()
            .unwrap_or_else(|| record.path.clone());
        let Some(node_id) = record.remote_node else {
            let expected = self.replica.scan()?;
            if expected.get(&path).is_some() {
                self.replica.quarantine(
                    &path,
                    &expected,
                    "conflict resolution kept the remote deletion",
                    None,
                )?;
            }
            self.replica.store_mut().remove_baseline_path(&path)?;
            self.replica
                .store_mut()
                .remove_baseline_path(&record.path)?;
            self.clear_resolved_conflict(&record.id)?;
            return Ok(true);
        };
        if remote.get(&path).is_none() {
            // The remote side already converged; keep the local path removed.
            return Ok(true);
        }
        let snapshot = match record.remote_revision {
            Some(revision) => {
                self.sync
                    .authority()
                    .observe_revision(Revision::from_bytes(revision))
                    .await?
            }
            None => observed.clone(),
        };
        let Some(node) = snapshot.node(NodeId::from_bytes(node_id)).await? else {
            return Ok(false);
        };
        if node.is_directory() {
            let fresh = self.replica.scan()?;
            if fresh.get(&path).is_none() {
                self.replica.create_directory_local(&path)?;
            }
        } else {
            let mut staged = self.download_remote(&snapshot, &node).await?;
            let expected = self.replica.scan()?.get(&path).cloned();
            match expected {
                Some(expected) if !expected.is_directory() => {
                    let mut bytes = Vec::new();
                    std::io::copy(&mut staged, &mut bytes).map_err(|e| e.to_string())?;
                    self.replica.install_bytes(
                        &path,
                        &expected,
                        node.executable(),
                        record.remote_revision,
                        &bytes,
                    )?;
                }
                _ => {
                    self.replica
                        .create_file_local(&path, node.executable(), &mut staged)?;
                }
            }
        }
        self.apply_resolved_baseline(record, &path, snapshot.revision())
            .await?;
        self.clear_resolved_conflict(&record.id)?;
        Ok(true)
    }

    /// Record the accepted common state for a resolved path and drop the
    /// conflict's original path so a rename no longer re-conflicts.
    async fn apply_resolved_baseline(
        &mut self,
        record: &ConflictRecord,
        path: &str,
        revision: Revision,
    ) -> Result<()> {
        let snapshot = self.sync.authority().observe_revision(revision).await?;
        if let Some(node) = snapshot.resolve(path).await? {
            let fresh = self.replica.scan()?;
            if let Some(current) = fresh.get(path).cloned() {
                if record.path != path {
                    self.replica
                        .store_mut()
                        .remove_baseline_path(&record.path)?;
                }
                self.replica.store_mut().save_baseline_path(
                    path,
                    &node,
                    Some(&current),
                    revision,
                )?;
            }
        }
        Ok(())
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
}
