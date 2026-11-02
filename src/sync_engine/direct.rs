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

//! Direct namespace adapter for the shared sync engine.

use super::adapter::{
    EntryKind, FrozenRemoteRequest, PreparedRemoteRequest, RemoteAdapter, RemoteBaseline,
    RemoteCapabilities, RemoteCursor, RemoteEntry, RemoteFailure, RemoteObservation, RemoteOutcome,
    RemotePin, RemoteRequest, SyncModel, digest,
};
use super::local::validate_private_state;
use super::{Error, Result};
use crate::direct::{
    DirectKey, DirectKind, DirectMutation, DirectMutationOutcome, DirectNamespace, DirectNode,
    DirectObjectPin, DirectPrecondition, DirectSnapshot,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::io::Write;
use std::os::fd::BorrowedFd;
use std::path::Path;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Clone, Debug, Deserialize, Serialize)]
enum FrozenDirectMutation {
    CreateFile { path: String },
    ReplaceFile { path: String, etag: String },
    Delete { path: String, etag: String },
}

#[derive(Clone)]
pub struct DirectAdapter {
    namespace: DirectNamespace,
    mutation: DirectMutation,
    binding: String,
    snapshot: std::sync::Arc<Mutex<Option<DirectSnapshot>>>,
}

impl DirectAdapter {
    pub fn open(namespace: DirectNamespace, binding: impl Into<String>) -> Result<Self> {
        let mutation = DirectMutation::open(namespace.clone()).map_err(Error::from)?;
        Ok(Self {
            namespace,
            mutation,
            binding: binding.into(),
            snapshot: std::sync::Arc::new(Mutex::new(None)),
        })
    }

    fn cached_snapshot(&self) -> Result<DirectSnapshot> {
        self.snapshot
            .lock()
            .map_err(|_| Error::corrupt("Direct Sync snapshot cache was poisoned"))?
            .clone()
            .ok_or_else(|| Error::conflict("no accepted Direct observation exists"))
    }

    fn direct_pin(entry: &RemoteEntry) -> Result<(&str, [u8; 16])> {
        let RemotePin::Direct {
            observation, etag, ..
        } = &entry.pin
        else {
            return Err(Error::corrupt(
                "Direct adapter received a Managed remote pin",
            ));
        };
        let etag = etag
            .as_deref()
            .ok_or_else(|| Error::unsupported("Direct object exposes no ETag"))?;
        Ok((etag, *observation))
    }
}

impl RemoteAdapter for DirectAdapter {
    fn model(&self) -> SyncModel {
        SyncModel::Direct
    }

    fn namespace_id(&self) -> String {
        self.binding.clone()
    }

    fn capabilities(&self) -> RemoteCapabilities {
        RemoteCapabilities::for_model(SyncModel::Direct)
    }

    async fn preflight(&self, state: &Path) -> Result<()> {
        if state.exists() {
            validate_private_state(state)?;
        }
        Ok(())
    }

    async fn bind(&self, _state: &Path) -> Result<()> {
        Ok(())
    }

    async fn observe(&self, _baseline: Option<&RemoteBaseline>) -> Result<RemoteObservation> {
        let snapshot = self.namespace.observe().await.map_err(Error::from)?;
        let mut entries = BTreeMap::new();
        entries.insert(String::new(), remote_entry(snapshot.root_node())?);
        let mut pending = vec![snapshot.root()];
        while let Some(directory) = pending.pop() {
            for node in snapshot.read_dir(directory).map_err(Error::from)? {
                if node.kind() == DirectKind::Directory {
                    pending.push(node.id());
                }
                entries.insert(node.path().to_owned(), remote_entry(node)?);
            }
        }
        let cursor = RemoteCursor {
            bytes: snapshot.revision().to_bytes().to_vec(),
        };
        *self
            .snapshot
            .lock()
            .map_err(|_| Error::corrupt("Direct Sync snapshot cache was poisoned"))? =
            Some(snapshot);
        Ok(RemoteObservation { cursor, entries })
    }

    async fn read_file(&self, entry: &RemoteEntry, output: BorrowedFd<'_>) -> Result<()> {
        let snapshot = self.cached_snapshot()?;
        let node = snapshot
            .resolve(&entry.path)
            .ok_or_else(|| Error::conflict("Direct object disappeared after observation"))?;
        let (_, observation) = Self::direct_pin(entry)?;
        if node.id().as_bytes() != &observation {
            return Err(Error::conflict(
                "Direct object changed after its accepted observation",
            ));
        }
        let file = snapshot.open_file(node.id()).await.map_err(Error::from)?;
        let duplicate = output
            .try_clone_to_owned()
            .map_err(|error| Error::io("duplicate Direct download fd", Path::new("."), error))?;
        let mut target = std::fs::File::from(duplicate);
        let mut offset = 0_u64;
        while offset < file.length() {
            let end = offset.saturating_add(64 * 1024).min(file.length());
            let bytes = file
                .read(offset, (end - offset) as usize)
                .await
                .map_err(Error::from)?;
            target
                .write_all(&bytes)
                .map_err(|error| Error::io("write Direct download", Path::new("."), error))?;
            offset = end;
        }
        if offset != entry.length {
            return Err(Error::conflict(
                "Direct download length changed after observation",
            ));
        }
        target
            .sync_all()
            .map_err(|error| Error::io("persist Direct download", Path::new("."), error))
    }

    async fn freeze(&self, request: RemoteRequest) -> Result<FrozenRemoteRequest> {
        let operation_name = request.operation();
        let path = request.path().to_owned();
        let original_path = request.original_path().map(str::to_owned);
        let desired_path = request.desired_path().map(str::to_owned);
        let operation = match request {
            RemoteRequest::CreateFile { path, .. } => FrozenDirectMutation::CreateFile { path },
            RemoteRequest::ReplaceFile { path, baseline, .. } => {
                let (etag, _) = Self::direct_pin(&baseline)?;
                FrozenDirectMutation::ReplaceFile {
                    path,
                    etag: etag.to_owned(),
                }
            }
            RemoteRequest::Remove { path, baseline } => {
                let (etag, _) = Self::direct_pin(&baseline)?;
                FrozenDirectMutation::Delete {
                    path,
                    etag: etag.to_owned(),
                }
            }
            RemoteRequest::CreateDirectory { .. } => {
                return Err(Error::unsupported(
                    "Direct object storage has no portable empty-directory operation",
                ));
            }
            RemoteRequest::Rename { .. } => {
                return Err(Error::unsupported("Direct Sync has no atomic rename"));
            }
            RemoteRequest::SetExecutable { .. } => {
                return Err(Error::unsupported(
                    "Direct object storage has no executable metadata",
                ));
            }
        };
        let bytes = serde_json::to_vec(&operation)
            .map_err(|error| Error::corrupt(format!("encode Direct request: {error}")))?;
        let request_digest = digest(&bytes);
        let mut id = [0_u8; 16];
        id.copy_from_slice(&request_digest[..16]);
        Ok(FrozenRemoteRequest {
            id,
            digest: request_digest,
            bytes,
            operation: operation_name,
            path,
            original_path,
            desired_path,
        })
    }

    async fn receipt(&self, _request: &FrozenRemoteRequest) -> Result<Option<RemoteOutcome>> {
        Ok(None)
    }

    async fn publish_batch(
        &self,
        requests: &[PreparedRemoteRequest],
    ) -> Result<Vec<RemoteOutcome>> {
        let mut outcomes = Vec::with_capacity(requests.len());
        for request in requests {
            let operation: FrozenDirectMutation = serde_json::from_slice(&request.request.bytes)
                .map_err(|error| Error::corrupt(format!("decode Direct request: {error}")))?;
            let outcome = match operation.clone() {
                FrozenDirectMutation::CreateFile { path } => {
                    let source = request
                        .source
                        .as_deref()
                        .ok_or_else(|| Error::corrupt("Direct create has no retained input"))?;
                    let key = DirectKey::from_path(&path).map_err(Error::from)?;
                    self.mutation
                        .publish_file(&key, source, DirectPrecondition::Absent)
                        .await
                        .map_err(Error::from)?
                }
                FrozenDirectMutation::ReplaceFile { path, etag } => {
                    let source = request
                        .source
                        .as_deref()
                        .ok_or_else(|| Error::corrupt("Direct replace has no retained input"))?;
                    let key = DirectKey::from_path(&path).map_err(Error::from)?;
                    self.mutation
                        .publish_file(&key, source, DirectPrecondition::Etag(etag))
                        .await
                        .map_err(Error::from)?
                }
                FrozenDirectMutation::Delete { path, etag } => {
                    let key = DirectKey::from_path(&path).map_err(Error::from)?;
                    self.mutation
                        .delete(&key, DirectPrecondition::Etag(etag))
                        .await
                        .map_err(Error::from)?
                }
            };
            outcomes.push(direct_outcome(operation, outcome));
        }
        Ok(outcomes)
    }
}

pub fn validate_namespace_entries(entries: &BTreeMap<String, RemoteEntry>) -> Result<()> {
    for (path, entry) in entries {
        if path.is_empty() || entry.kind != EntryKind::Directory {
            continue;
        }
        let prefix = format!("{path}/");
        if !entries
            .keys()
            .any(|candidate| candidate.starts_with(&prefix))
        {
            return Err(Error::unsupported(format!(
                "Direct Sync cannot represent empty directory markers at {path:?}"
            )));
        }
    }
    Ok(())
}

fn direct_outcome(
    operation: FrozenDirectMutation,
    outcome: DirectMutationOutcome,
) -> RemoteOutcome {
    match outcome {
        DirectMutationOutcome::Applied { accepted } => {
            let path = match &operation {
                FrozenDirectMutation::CreateFile { path }
                | FrozenDirectMutation::ReplaceFile { path, .. }
                | FrozenDirectMutation::Delete { path, .. } => path.clone(),
            };
            let mut accepted_entries = BTreeMap::new();
            accepted_entries.insert(
                path.clone(),
                accepted.map(|pin| RemoteEntry {
                    path,
                    kind: EntryKind::File,
                    length: pin.size(),
                    digest: None,
                    executable: false,
                    pin: RemotePin::Direct {
                        observation: direct_observation(&pin),
                        version: pin.version().map(str::to_owned),
                        etag: pin.etag().map(str::to_owned),
                        size: pin.size(),
                        modified: pin.modified().map(system_time_seconds),
                    },
                }),
            );
            RemoteOutcome::Applied {
                accepted: accepted_entries,
                receipt: None,
            }
        }
        DirectMutationOutcome::Conflict { observed } => RemoteOutcome::Conflict {
            observed: observed.present().map(|pin| {
                remote_entry_from_pin(
                    match &operation {
                        FrozenDirectMutation::CreateFile { path }
                        | FrozenDirectMutation::ReplaceFile { path, .. }
                        | FrozenDirectMutation::Delete { path, .. } => path.clone(),
                    },
                    pin,
                )
            }),
        },
        DirectMutationOutcome::Unknown { message, .. } => RemoteOutcome::Unknown {
            failure: RemoteFailure {
                kind: super::ErrorKind::Unknown,
                message,
                commit: None,
            },
        },
        DirectMutationOutcome::Unsupported { message } => RemoteOutcome::Unsupported {
            failure: RemoteFailure {
                kind: super::ErrorKind::Unsupported,
                message,
                commit: None,
            },
        },
    }
}

fn remote_entry(node: &DirectNode) -> Result<RemoteEntry> {
    let kind = match node.kind() {
        DirectKind::File => EntryKind::File,
        DirectKind::Directory => EntryKind::Directory,
    };
    let pin = if node.is_file() {
        let version = node.version().map(str::to_owned);
        let etag = node.etag().map(str::to_owned);
        if version.is_none() && etag.is_none() {
            return Err(Error::unsupported(format!(
                "Direct object {:?} exposes neither a version nor an ETag",
                node.path()
            )));
        }
        RemotePin::Direct {
            observation: *node.id().as_bytes(),
            version,
            etag,
            size: node.size(),
            modified: node.modified().map(system_time_seconds),
        }
    } else {
        RemotePin::Direct {
            observation: *node.id().as_bytes(),
            version: None,
            etag: None,
            size: 0,
            modified: None,
        }
    };
    Ok(RemoteEntry {
        path: node.path().to_owned(),
        kind,
        length: node.size(),
        digest: None,
        executable: false,
        pin,
    })
}

fn remote_entry_from_pin(path: String, pin: &DirectObjectPin) -> RemoteEntry {
    RemoteEntry {
        path,
        kind: EntryKind::File,
        length: pin.size(),
        digest: None,
        executable: false,
        pin: RemotePin::Direct {
            observation: direct_observation(pin),
            version: pin.version().map(str::to_owned),
            etag: pin.etag().map(str::to_owned),
            size: pin.size(),
            modified: pin.modified().map(system_time_seconds),
        },
    }
}

fn direct_observation(pin: &DirectObjectPin) -> [u8; 16] {
    let mut hasher = blake3::Hasher::new();
    for value in [pin.version(), pin.etag()] {
        match value {
            Some(value) => {
                hasher.update(&[1]);
                hasher.update(value.as_bytes());
            }
            None => {
                hasher.update(&[0]);
            }
        }
    }
    hasher.update(&pin.size().to_be_bytes());
    if let Some(modified) = pin.modified().map(system_time_seconds) {
        hasher.update(&[1]);
        hasher.update(&modified.to_be_bytes());
    } else {
        hasher.update(&[0]);
    }
    let mut observation = [0_u8; 16];
    observation.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
    observation
}

fn system_time_seconds(time: SystemTime) -> u64 {
    time.duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or_default()
}
