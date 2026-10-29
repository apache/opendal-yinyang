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

//! Adapter from the shared Direct namespace/mutation seam to Direct Sync.

use super::{
    DirectRemote, MutationOutcome, RemoteEntry, RemoteKind, RemotePin, RemotePrecondition,
    RemoteSnapshot,
};
use crate::direct::{
    DirectKey, DirectKind, DirectMutation, DirectMutationOutcome, DirectNamespace, DirectNode,
    DirectObjectPin, DirectPrecondition,
};
use crate::replica::{Error, ErrorKind, Result};
use std::io::Write;
use std::path::Path;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

pub struct DirectRemoteAdapter {
    namespace: DirectNamespace,
    mutation: DirectMutation,
    binding: String,
    snapshot: Mutex<Option<crate::direct::DirectSnapshot>>,
}

impl DirectRemoteAdapter {
    /// Open stateless Direct operations. `binding` must be a canonical,
    /// non-secret identity of scheme, bucket, root and selected storage profile.
    pub fn open(namespace: DirectNamespace, binding: impl Into<String>) -> Result<Self> {
        let mutation = DirectMutation::open(namespace.clone()).map_err(direct_error)?;
        Ok(Self {
            namespace,
            mutation,
            binding: binding.into(),
            snapshot: Mutex::new(None),
        })
    }
}

impl DirectRemote for DirectRemoteAdapter {
    fn namespace_id(&self) -> String {
        self.binding.clone()
    }

    async fn observe(&self) -> Result<RemoteSnapshot> {
        let snapshot = self.namespace.observe().await.map_err(direct_error)?;
        let mut entries = std::collections::BTreeMap::new();
        let mut pending = vec![snapshot.root()];
        while let Some(directory) = pending.pop() {
            for node in snapshot.read_dir(directory).map_err(direct_error)? {
                match node.kind() {
                    DirectKind::Directory => {
                        pending.push(node.id());
                        entries.insert(node.path().to_owned(), remote_entry(node)?);
                    }
                    DirectKind::File => {
                        entries.insert(node.path().to_owned(), remote_entry(node)?);
                    }
                }
            }
        }
        let revision = snapshot.revision().to_hex();
        *self
            .snapshot
            .lock()
            .map_err(|_| Error::corrupt("Direct Sync snapshot cache was poisoned"))? =
            Some(snapshot);
        Ok(RemoteSnapshot {
            revision: Some(revision),
            entries,
        })
    }

    async fn read_to_path(&self, entry: &RemoteEntry, output: &Path) -> Result<()> {
        let output_path = output;
        let snapshot = self
            .snapshot
            .lock()
            .map_err(|_| Error::corrupt("Direct Sync snapshot cache was poisoned"))?
            .clone()
            .ok_or_else(|| {
                Error::conflict(format!(
                    "no accepted Direct observation exists for {:?}",
                    entry.path
                ))
            })?;
        let node = snapshot
            .resolve(&entry.path)
            .filter(|node| node.id().as_bytes() == &entry.observation)
            .ok_or_else(|| {
                Error::conflict(format!(
                    "Direct object {:?} changed after its accepted observation",
                    entry.path
                ))
            })?;
        let file = snapshot.open_file(node.id()).await.map_err(direct_error)?;
        let fd = rustix::fs::open(
            output_path,
            rustix::fs::OFlags::WRONLY
                | rustix::fs::OFlags::TRUNC
                | rustix::fs::OFlags::NOFOLLOW
                | rustix::fs::OFlags::CLOEXEC,
            rustix::fs::Mode::RUSR | rustix::fs::Mode::WUSR,
        )
        .map_err(|error| Error::errno("create Direct Sync download", output_path, error))?;
        let mut output = std::fs::File::from(fd);
        let mut offset = 0_u64;
        while offset < file.length() {
            let end = offset.saturating_add(64 * 1024).min(file.length());
            let bytes = file
                .read(offset, (end - offset) as usize)
                .await
                .map_err(direct_error)?;
            output
                .write_all(&bytes)
                .map_err(|error| Error::io("write Direct Sync download", output_path, error))?;
            offset = end;
        }
        output
            .sync_all()
            .map_err(|error| Error::io("persist Direct Sync download", output_path, error))
    }

    async fn publish_file(
        &self,
        path: &str,
        source: &Path,
        precondition: &RemotePrecondition,
    ) -> Result<MutationOutcome> {
        let key = DirectKey::from_path(path).map_err(direct_error)?;
        let precondition = match precondition {
            RemotePrecondition::Absent => DirectPrecondition::Absent,
            RemotePrecondition::Etag(etag) => DirectPrecondition::Etag(etag.clone()),
        };
        let outcome = self
            .mutation
            .publish_file(&key, source, precondition)
            .await
            .map_err(direct_error)?;
        Ok(mutation_outcome(outcome))
    }

    async fn delete(
        &self,
        path: &str,
        precondition: &RemotePrecondition,
    ) -> Result<MutationOutcome> {
        let key = DirectKey::from_path(path).map_err(direct_error)?;
        let precondition = match precondition {
            RemotePrecondition::Absent => {
                return Ok(MutationOutcome::Unsupported {
                    message: "conditional delete requires an observed ETag".to_owned(),
                });
            }
            RemotePrecondition::Etag(etag) => DirectPrecondition::Etag(etag.clone()),
        };
        let outcome = self
            .mutation
            .delete(&key, precondition)
            .await
            .map_err(direct_error)?;
        Ok(mutation_outcome(outcome))
    }
}

fn remote_entry(node: &DirectNode) -> Result<RemoteEntry> {
    let kind = match node.kind() {
        DirectKind::File => RemoteKind::File,
        DirectKind::Directory => RemoteKind::Directory,
    };
    let pin = if node.is_file() {
        Some(RemotePin {
            version: node.version().map(str::to_owned),
            etag: node.etag().map(str::to_owned),
            size: node.size(),
            modified: node.modified().map(system_time_seconds),
        })
    } else {
        None
    };
    if node.is_file()
        && pin
            .as_ref()
            .is_none_or(|pin| pin.version.is_none() && pin.etag.is_none())
    {
        return Err(Error::unsupported(format!(
            "Direct object {:?} exposes neither a version nor an ETag",
            node.path()
        )));
    }
    let mut observation = [0_u8; 16];
    observation.copy_from_slice(node.id().as_bytes());
    Ok(RemoteEntry {
        path: node.path().to_owned(),
        kind,
        size: node.size(),
        pin,
        observation,
    })
}

fn mutation_outcome(outcome: DirectMutationOutcome) -> MutationOutcome {
    match outcome {
        DirectMutationOutcome::Applied { accepted } => MutationOutcome::Applied {
            accepted: accepted.map(object_pin),
        },
        DirectMutationOutcome::Conflict { .. } => MutationOutcome::Conflict,
        DirectMutationOutcome::Unknown { message, .. } => {
            MutationOutcome::Unknown { error: message }
        }
        DirectMutationOutcome::Unsupported { message } => MutationOutcome::Unsupported { message },
    }
}

fn object_pin(pin: DirectObjectPin) -> RemotePin {
    RemotePin {
        version: pin.version().map(str::to_owned),
        etag: pin.etag().map(str::to_owned),
        size: pin.size(),
        modified: pin.modified().map(system_time_seconds),
    }
}

fn system_time_seconds(time: SystemTime) -> u64 {
    time.duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or_default()
}

fn direct_error(error: crate::direct::Error) -> Error {
    let kind = match error.kind() {
        crate::direct::ErrorKind::Unsupported => ErrorKind::Unsupported,
        crate::direct::ErrorKind::Stale | crate::direct::ErrorKind::NotFound => ErrorKind::Conflict,
        _ => ErrorKind::Io,
    };
    Error::new(kind, error.to_string())
}
