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

//! Managed authority and durable-intent adapter for the shared sync engine.

use super::adapter::{
    EntryKind, FrozenRemoteRequest, PreparedRemoteRequest, RemoteAdapter, RemoteBaseline,
    RemoteCapabilities, RemoteCursor, RemoteEntry, RemoteFailure, RemoteObservation, RemoteOutcome,
    RemotePin, RemoteRequest, SyncModel,
};
use super::{Error, ErrorKind, Result};
use std::collections::BTreeMap;
use std::os::fd::AsRawFd;
use std::os::fd::BorrowedFd;
use std::path::Path;
use std::sync::Arc;
use tokio::io::AsyncWriteExt as _;
use yinyang_core::{
    Authority, CommitId, CommitOutcome, ContentId, Cursor, Node, NodeId, NodeKind, Revision,
    Snapshot,
};

#[derive(Clone)]
pub struct ManagedAdapter {
    authority: Arc<dyn Authority>,
}

impl ManagedAdapter {
    pub fn new(authority: Arc<dyn Authority>) -> Self {
        Self { authority }
    }

    fn parent(path: &str) -> &str {
        path.rsplit_once('/').map_or("", |(parent, _)| parent)
    }
}

impl RemoteAdapter for ManagedAdapter {
    fn model(&self) -> SyncModel {
        SyncModel::Managed
    }

    fn namespace_id(&self) -> String {
        format!(
            "{}:{}",
            super::adapter::hex(self.authority.filesystem().as_bytes()),
            super::adapter::hex(self.authority.root().as_bytes())
        )
    }

    fn capabilities(&self) -> RemoteCapabilities {
        RemoteCapabilities::for_model(SyncModel::Managed)
    }

    async fn preflight(&self, _state: &Path) -> Result<()> {
        Ok(())
    }

    async fn bind(&self, _state: &Path) -> Result<()> {
        Ok(())
    }

    async fn observe(&self, baseline: Option<&RemoteBaseline>) -> Result<RemoteObservation> {
        let snapshot = self.authority.observe_latest().await.map_err(Error::from)?;
        if let Some(baseline) = baseline
            && let Some(cursor) = &baseline.cursor
            && let Ok(after) = decode_cursor(&cursor.bytes)
            && after.revision <= snapshot.revision()
            && let Ok(Some(mut entries)) =
                apply_changes(&snapshot, after, baseline.entries.clone()).await
        {
            stamp_revision(&mut entries, snapshot.revision());
            return Ok(RemoteObservation {
                cursor: current_cursor(snapshot.revision()),
                revision: Some(snapshot.revision().to_bytes()),
                entries,
            });
        }
        let mut entries = walk_remote(&snapshot, snapshot.revision()).await?;
        stamp_revision(&mut entries, snapshot.revision());
        Ok(RemoteObservation {
            cursor: current_cursor(snapshot.revision()),
            revision: Some(snapshot.revision().to_bytes()),
            entries,
        })
    }

    async fn read_file(&self, entry: &RemoteEntry, output: BorrowedFd<'_>) -> Result<()> {
        let RemotePin::Managed { node, revision, .. } = &entry.pin else {
            return Err(Error::corrupt(
                "Managed adapter received a Direct remote pin",
            ));
        };
        let snapshot = self
            .authority
            .observe_revision(Revision::from_bytes(*revision))
            .await
            .map_err(Error::from)?;
        let version = snapshot
            .open_file(NodeId::from_bytes(*node))
            .await
            .map_err(Error::from)?;
        if version.node().id() != NodeId::from_bytes(*node) {
            return Err(Error::conflict(
                "Managed remote node identity changed after observation",
            ));
        }
        let owned = output
            .try_clone_to_owned()
            .map_err(|error| Error::io("duplicate Managed download fd", Path::new("."), error))?;
        let mut target = tokio::fs::File::from_std(std::fs::File::from(owned));
        let length = version.descriptor().content_id().length();
        let mut offset = 0_u64;
        while offset < length {
            let end = offset.saturating_add(64 * 1024).min(length);
            let mut bytes = Vec::new();
            version
                .read_range(offset..end, &mut bytes)
                .await
                .map_err(Error::from)?;
            target
                .write_all(&bytes)
                .await
                .map_err(|error| Error::io("write Managed download", Path::new("."), error))?;
            offset = end;
        }
        target
            .flush()
            .await
            .map_err(|error| Error::io("flush Managed download", Path::new("."), error))?;
        let verify_path = format!("/proc/self/fd/{}", output.as_raw_fd());
        let mut verification = tokio::fs::File::open(&verify_path)
            .await
            .map_err(|error| Error::io("open Managed verification fd", Path::new("."), error))?;
        if ContentId::calculate(&mut verification)
            .await
            .map_err(Error::from)?
            != version.descriptor().content_id()
        {
            return Err(Error::conflict(
                "Managed downloaded content does not match its accepted digest",
            ));
        }
        target
            .sync_all()
            .await
            .map_err(|error| Error::io("sync Managed download", Path::new("."), error))
    }

    async fn freeze(&self, request: RemoteRequest) -> Result<FrozenRemoteRequest> {
        super::managed_intent::freeze(&self.authority, request).await
    }

    async fn receipt(&self, request: &FrozenRemoteRequest) -> Result<Option<RemoteOutcome>> {
        let snapshot = self.authority.observe_latest().await.map_err(Error::from)?;
        let Some(receipt) = snapshot
            .receipt(CommitId::from_bytes(request.id))
            .await
            .map_err(Error::from)?
        else {
            return Ok(None);
        };
        if receipt.request_digest != request.digest {
            return Err(Error::corrupt(
                "Managed receipt identity is paired with a different request",
            ));
        }
        let revision = receipt.cursor.revision;
        let mut snapshots = BTreeMap::new();
        let accepted =
            accepted_entries(self.authority.as_ref(), request, revision, &mut snapshots).await?;
        Ok(Some(RemoteOutcome::Applied {
            accepted,
            receipt: Some(super::adapter::RemoteReceipt {
                commit: Some(*receipt.commit_id.as_bytes()),
                revision: Some(revision.to_bytes()),
                ordinal: Some(receipt.cursor.ordinal),
            }),
        }))
    }

    async fn publish_batch(
        &self,
        requests: &[PreparedRemoteRequest],
    ) -> Result<Vec<RemoteOutcome>> {
        let mut transactions = Vec::with_capacity(requests.len());
        for request in requests {
            transactions
                .push(super::managed_intent::prepare(&self.authority, &request.request).await?);
        }
        let results = self
            .authority
            .commit_batch(&transactions)
            .await
            .map_err(Error::from)?;
        if results.len() != requests.len() {
            return Err(Error::corrupt(
                "Managed commit outcome count disagrees with the prepared batch",
            ));
        }
        let mut snapshots = BTreeMap::<[u8; 24], Snapshot>::new();
        let mut outcomes = Vec::with_capacity(requests.len());
        for (request, result) in requests.iter().zip(results) {
            match result {
                CommitOutcome::Committed(receipt) => {
                    if receipt.request_digest != request.request.digest {
                        return Err(Error::corrupt(
                            "Managed commit receipt is paired with a different request",
                        ));
                    }
                    let revision = receipt.cursor.revision;
                    let entries = accepted_entries(
                        self.authority.as_ref(),
                        &request.request,
                        revision,
                        &mut snapshots,
                    )
                    .await?;
                    outcomes.push(RemoteOutcome::Applied {
                        accepted: entries,
                        receipt: Some(super::adapter::RemoteReceipt {
                            commit: Some(*receipt.commit_id.as_bytes()),
                            revision: Some(revision.to_bytes()),
                            ordinal: Some(receipt.cursor.ordinal),
                        }),
                    });
                }
                CommitOutcome::Conflict => {
                    outcomes.push(RemoteOutcome::Conflict { observed: None });
                }
                CommitOutcome::Retryable => outcomes.push(RemoteOutcome::Retryable {
                    failure: RemoteFailure::from_error(&Error::new(
                        ErrorKind::Retryable,
                        "Managed commit was not accepted",
                    )),
                }),
                CommitOutcome::Unknown(commit) => outcomes.push(RemoteOutcome::Unknown {
                    failure: RemoteFailure::from_error(&Error::unknown(
                        Some(commit),
                        "Managed commit outcome is unknown",
                    )),
                }),
            }
        }
        Ok(outcomes)
    }
}

fn stamp_revision(entries: &mut BTreeMap<String, RemoteEntry>, revision: Revision) {
    for entry in entries.values_mut() {
        if let RemotePin::Managed {
            revision: accepted, ..
        } = &mut entry.pin
        {
            *accepted = revision.to_bytes();
        }
    }
}

async fn walk_remote(
    snapshot: &Snapshot,
    revision: Revision,
) -> Result<BTreeMap<String, RemoteEntry>> {
    let nodes = crate::directory_nodes(snapshot)
        .await
        .map_err(Error::from)?;
    nodes
        .into_iter()
        .map(|(path, node)| Ok((path.clone(), remote_entry(&node, path, revision)?)))
        .collect()
}

async fn walk_subtree(
    snapshot: &Snapshot,
    root: &str,
    revision: Revision,
) -> Result<BTreeMap<String, RemoteEntry>> {
    let Some(node) = snapshot.resolve(root).await.map_err(Error::from)? else {
        return Ok(BTreeMap::new());
    };
    let mut entries = BTreeMap::new();
    let mut pending = vec![(root.to_owned(), node)];
    while let Some((path, node)) = pending.pop() {
        if node.is_directory() {
            let mut token = None;
            loop {
                let page = snapshot
                    .scan(node.id(), token.as_ref(), 4096)
                    .await
                    .map_err(Error::from)?;
                for entry in page.entries {
                    let child_path = if path.is_empty() {
                        entry.name.clone()
                    } else {
                        format!("{path}/{}", entry.name)
                    };
                    let child = snapshot
                        .node(entry.node_id)
                        .await
                        .map_err(Error::from)?
                        .ok_or_else(|| {
                            Error::corrupt("Managed change references a missing child")
                        })?;
                    pending.push((child_path, child));
                }
                match page.next {
                    Some(next) => token = Some(next),
                    None => break,
                }
            }
        }
        entries.insert(path.clone(), remote_entry(&node, path, revision)?);
    }
    Ok(entries)
}

async fn apply_changes(
    snapshot: &Snapshot,
    after: Cursor,
    mut entries: BTreeMap<String, RemoteEntry>,
) -> Result<Option<BTreeMap<String, RemoteEntry>>> {
    let mut cursor = after;
    let mut records = Vec::new();
    loop {
        let page = match snapshot.changes(Some(cursor), 4096).await {
            Ok(page) => page,
            Err(_) => return Ok(None),
        };
        if page.is_empty() {
            break;
        }
        cursor = page.last().expect("non-empty page").receipt.cursor;
        records.extend(page);
    }
    for record in records {
        for change in record.changes {
            let before_path = change.before.as_ref().and_then(|node| {
                entries
                    .iter()
                    .find(|(_, entry)| {
                        matches!(
                            entry.pin,
                            RemotePin::Managed { node: id, .. } if id == *node.id().as_bytes()
                        )
                    })
                    .map(|(path, _)| path.clone())
            });
            match (change.before, change.after) {
                (Some(before), Some(after)) => {
                    let Some(before_path) = before_path else {
                        return Ok(None);
                    };
                    let after_path = node_path(snapshot, &after).await?;
                    if before.is_directory() && before_path != after_path {
                        remove_subtree(&mut entries, &before_path);
                        entries.extend(
                            walk_subtree(snapshot, &after_path, snapshot.revision()).await?,
                        );
                    } else if before_path != after_path {
                        entries.remove(&before_path);
                        entries.insert(
                            after_path.clone(),
                            remote_entry(&after, after_path, snapshot.revision())?,
                        );
                    } else {
                        entries.insert(
                            before_path.clone(),
                            remote_entry(&after, before_path, snapshot.revision())?,
                        );
                    }
                }
                (Some(before), None) => {
                    let Some(before_path) = before_path else {
                        return Ok(None);
                    };
                    if before.is_directory() {
                        remove_subtree(&mut entries, &before_path);
                    } else {
                        entries.remove(&before_path);
                    }
                }
                (None, Some(after)) => {
                    let after_path = node_path(snapshot, &after).await?;
                    if after.is_directory() {
                        entries.extend(
                            walk_subtree(snapshot, &after_path, snapshot.revision()).await?,
                        );
                    } else {
                        entries.insert(
                            after_path.clone(),
                            remote_entry(&after, after_path, snapshot.revision())?,
                        );
                    }
                }
                (None, None) => return Err(Error::corrupt("Managed change has no side")),
            }
        }
    }
    Ok(Some(entries))
}

async fn node_path(snapshot: &Snapshot, node: &Node) -> Result<String> {
    let mut components = Vec::new();
    let mut current = node.clone();
    while let Some(link) = current.link() {
        components.push(link.name.clone());
        current = snapshot
            .node(link.parent)
            .await
            .map_err(Error::from)?
            .ok_or_else(|| Error::corrupt("Managed change has a missing parent"))?;
    }
    components.reverse();
    Ok(components.join("/"))
}

fn remove_subtree(entries: &mut BTreeMap<String, RemoteEntry>, root: &str) {
    let prefix = format!("{root}/");
    entries.retain(|path, _| path != root && !path.starts_with(&prefix));
}

fn remote_entry(node: &Node, path: String, revision: Revision) -> Result<RemoteEntry> {
    let (kind, length, digest) = match node.kind() {
        NodeKind::Directory { .. } => (EntryKind::Directory, 0, None),
        NodeKind::File(file) => (
            EntryKind::File,
            file.content_id().length(),
            Some(*file.content_id().digest()),
        ),
    };
    Ok(RemoteEntry {
        path,
        kind,
        length,
        digest,
        executable: node.executable(),
        pin: RemotePin::Managed {
            node: *node.id().as_bytes(),
            revision: revision.to_bytes(),
            digest,
            executable: node.executable(),
            directory: node.is_directory(),
        },
    })
}

async fn accepted_entries(
    authority: &dyn Authority,
    request: &FrozenRemoteRequest,
    revision: Revision,
    snapshots: &mut BTreeMap<[u8; 24], Snapshot>,
) -> Result<BTreeMap<String, Option<RemoteEntry>>> {
    let snapshot = match snapshots.get(&revision.to_bytes()) {
        Some(snapshot) => snapshot.clone(),
        None => {
            let snapshot = authority
                .observe_revision(revision)
                .await
                .map_err(Error::from)?;
            snapshots.insert(revision.to_bytes(), snapshot.clone());
            snapshot
        }
    };
    let mut entries = BTreeMap::new();
    match request.operation {
        super::adapter::RemoteOperation::Remove => {
            entries.insert(request.path.clone(), None);
        }
        super::adapter::RemoteOperation::Rename => {
            let from = request
                .original_path
                .as_deref()
                .ok_or_else(|| Error::corrupt("Managed rename has no original path"))?;
            entries.insert(from.to_owned(), None);
            insert_current(&snapshot, &request.path, revision, &mut entries).await?;
        }
        _ => {
            insert_current(&snapshot, &request.path, revision, &mut entries).await?;
        }
    }
    for parent in [
        ManagedAdapter::parent(request.original_path.as_deref().unwrap_or(&request.path)),
        ManagedAdapter::parent(&request.path),
    ] {
        if !entries.contains_key(parent) {
            insert_current(&snapshot, parent, revision, &mut entries).await?;
        }
    }
    Ok(entries)
}

async fn insert_current(
    snapshot: &Snapshot,
    path: &str,
    revision: Revision,
    entries: &mut BTreeMap<String, Option<RemoteEntry>>,
) -> Result<()> {
    let value = snapshot.resolve(path).await.map_err(Error::from)?;
    entries.insert(
        path.to_owned(),
        value
            .map(|node| remote_entry(&node, path.to_owned(), revision))
            .transpose()?,
    );
    Ok(())
}

fn current_cursor(revision: Revision) -> RemoteCursor {
    let mut bytes = revision.to_bytes().to_vec();
    bytes.extend_from_slice(&u32::MAX.to_be_bytes());
    RemoteCursor { bytes }
}

fn decode_cursor(bytes: &[u8]) -> Result<Cursor> {
    if bytes.len() != 28 {
        return Err(Error::corrupt("invalid Managed Sync cursor length"));
    }
    let revision: [u8; 24] = bytes[..24]
        .try_into()
        .map_err(|_| Error::corrupt("invalid Managed Sync revision"))?;
    let ordinal: [u8; 4] = bytes[24..]
        .try_into()
        .map_err(|_| Error::corrupt("invalid Managed Sync ordinal"))?;
    Ok(Cursor {
        revision: Revision::from_bytes(revision),
        ordinal: u32::from_be_bytes(ordinal),
    })
}
