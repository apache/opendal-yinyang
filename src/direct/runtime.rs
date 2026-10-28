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

//! Durable whole-object staging for writable Direct Mount.

use super::journal::{
    EndpointRecord, MUTATION_APPLIED, MUTATION_CONFLICT, MUTATION_DESTINATION_APPLIED,
    MUTATION_DISPATCHING, MUTATION_FAILED, MUTATION_KIND_COPY, MUTATION_KIND_CREATE,
    MUTATION_KIND_DELETE, MUTATION_KIND_PUBLISH, MUTATION_KIND_RENAME,
    MUTATION_PENDING_SOURCE_CLEANUP, MUTATION_SOURCE_DISPATCHING, MUTATION_STAGED,
    MUTATION_UNKNOWN, MutationRecord, OBJECT_CONFLICT, OBJECT_FAILED, OBJECT_PENDING,
    OBJECT_STAGED, OBJECT_UNKNOWN, ObjectRecord, ObservationRecord, PinRecord, PreconditionRecord,
    Store,
};
use super::mutation::{
    DirectMutation, DirectMutationOutcome, DirectObjectPin, DirectObservation, DirectPrecondition,
};
use super::{
    DirectFile, DirectId, DirectKey, DirectNamespace, DirectNode, DirectSnapshot, Error, ErrorKind,
    Result, derive_id,
};
use opendal::EntryMode;
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::SystemTime;
use tokio::sync::Mutex as AsyncMutex;

const IO_CHUNK: usize = 64 * 1024;
const EMPTY_DIGEST: [u8; 32] = [
    0xaf, 0x13, 0x49, 0xb9, 0xf5, 0xf9, 0xa1, 0xa6, 0xa0, 0x40, 0x4d, 0xea, 0x36, 0xdc, 0xc9, 0x49,
    0x9b, 0xcb, 0x25, 0xc9, 0xad, 0xc1, 0x12, 0xb7, 0xcc, 0x9a, 0x93, 0xca, 0xe4, 0x1f, 0x32, 0x62,
];

/// Stable identity of one durable Direct mutation.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct DirectMutationId([u8; 16]);

impl DirectMutationId {
    pub fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }

    pub fn to_hex(self) -> String {
        self.0.iter().map(|byte| format!("{byte:02x}")).collect()
    }

    fn new() -> Self {
        Self(*uuid::Uuid::new_v4().as_bytes())
    }

    fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }
}

/// The durable local state of one Direct object generation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DirectObjectState {
    Clean,
    Staged,
    Pending,
    Conflict,
    Unknown,
    Failed,
}

/// The operation represented by a durable Direct mutation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DirectMutationKind {
    Publish,
    Create,
    Delete,
    Copy,
    Rename,
}

/// The reopen-safe state of a Direct mutation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DirectMutationState {
    Staged,
    Dispatching,
    Applied,
    Conflict,
    Unknown,
    DestinationApplied,
    PendingSourceCleanup,
    Failed,
}

/// One object endpoint of a durable namespace mutation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DirectMutationEndpoint {
    pub id: DirectId,
    pub key: DirectKey,
    pub path: String,
    pub parent: DirectId,
    pub name: String,
    pub precondition: DirectPrecondition,
}

/// Durable status of one Direct mutation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DirectMutationStatus {
    pub id: DirectMutationId,
    pub kind: DirectMutationKind,
    pub state: DirectMutationState,
    pub generation: u64,
    pub source: Option<DirectMutationEndpoint>,
    pub destination: Option<DirectMutationEndpoint>,
    pub input_length: u64,
    pub input_digest: Option<[u8; 32]>,
    pub accepted: Option<DirectObjectPin>,
    pub observed: DirectObservation,
    pub error: Option<String>,
}

/// Reopen-safe status of a Direct object and its retained local bytes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DirectObjectStatus {
    pub id: DirectId,
    pub key: DirectKey,
    pub path: String,
    pub parent: DirectId,
    pub name: String,
    pub length: u64,
    pub state: DirectObjectState,
    pub existed_remote: bool,
    pub generation: u64,
    pub precondition: Option<DirectPrecondition>,
    pub accepted: Option<DirectObjectPin>,
    pub observed: DirectObservation,
    pub pending_mutation: Option<DirectMutationId>,
    pub error: Option<String>,
}

/// The result of flushing one shared Direct object generation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DirectFlushOutcome {
    Unchanged,
    Applied {
        mutation: DirectMutationId,
        accepted: Option<DirectObjectPin>,
    },
    Conflict {
        mutation: DirectMutationId,
        observed: DirectObservation,
    },
    Unknown {
        mutation: DirectMutationId,
        observed: DirectObservation,
        message: String,
    },
    Unsupported {
        message: String,
    },
}

#[derive(Clone)]
struct ObjectIdentity {
    id: DirectId,
    key: DirectKey,
    path: String,
    parent: DirectId,
    name: String,
    existed_remote: bool,
}

struct ObjectEntry {
    identity: ObjectIdentity,
    inner: AsyncMutex<ObjectInner>,
}

struct ObjectInner {
    remote: Option<DirectFile>,
    file: Option<File>,
    data_path: Option<PathBuf>,
    state: DirectObjectState,
    length: u64,
    generation: u64,
    durable_generation: u64,
    digest: Option<[u8; 32]>,
    precondition: Option<DirectPrecondition>,
    accepted: Option<DirectObjectPin>,
    observed: DirectObservation,
    pending_mutation: Option<DirectMutationId>,
    error: Option<String>,
}

impl ObjectEntry {
    fn new(identity: ObjectIdentity, inner: ObjectInner) -> Self {
        Self {
            identity,
            inner: AsyncMutex::new(inner),
        }
    }
}

/// A writable Direct runtime layered over the read-only namespace.
#[derive(Clone)]
pub struct DirectRuntime {
    namespace: DirectNamespace,
    mutation: DirectMutation,
    read_only: bool,
    store: Option<Arc<Mutex<Store>>>,
    objects: Arc<AsyncMutex<BTreeMap<DirectId, Arc<ObjectEntry>>>>,
}

impl DirectRuntime {
    /// Open a runtime.
    ///
    /// Read-only runtimes do not inspect or create the optional staging path.
    /// Writable runtimes require a dedicated staging directory and the explicit
    /// non-secret storage binding used to reject reused state.
    pub async fn open(
        namespace: DirectNamespace,
        staging: Option<PathBuf>,
        read_only: bool,
        binding: super::DirectStorageBinding,
    ) -> Result<Self> {
        let mutation = DirectMutation::open(namespace.clone())?;
        let store = if read_only {
            None
        } else {
            let path = staging.ok_or_else(|| {
                Error::new(
                    ErrorKind::Invalid,
                    "writable Direct runtime requires a staging directory",
                )
            })?;
            Some(Arc::new(Mutex::new(Store::open(&path, &binding)?)))
        };
        Ok(Self {
            namespace,
            mutation,
            read_only,
            store,
            objects: Arc::new(AsyncMutex::new(BTreeMap::new())),
        })
    }

    pub fn is_read_only(&self) -> bool {
        self.read_only
    }

    /// Observe the current remote namespace. This never overlays local bytes.
    pub async fn observe_remote(&self) -> Result<DirectSnapshot> {
        self.namespace.observe().await
    }

    /// Return durable or active local object generations for a mount overlay.
    pub async fn pending_objects(&self) -> Result<Vec<DirectObjectStatus>> {
        let mut statuses = BTreeMap::new();
        if let Some(store) = &self.store {
            for record in guard(store).objects()? {
                if record.state != 0 {
                    let status = status_from_record(&record)?;
                    statuses.insert(status.id, status);
                }
            }
        }
        for entry in self.objects.lock().await.values() {
            let status = status_from_entry(entry).await;
            if status.state != DirectObjectState::Clean || !status.existed_remote {
                statuses.insert(status.id, status);
            }
        }
        Ok(statuses.into_values().collect())
    }

    /// Return all durable mutation intents.
    pub fn pending_mutations(&self) -> Result<Vec<DirectMutationStatus>> {
        let Some(store) = &self.store else {
            return Ok(Vec::new());
        };
        guard(store)
            .mutations()?
            .into_iter()
            .map(|record| status_from_mutation(&record))
            .collect()
    }

    /// Return one durable mutation status.
    pub fn mutation_status(&self, id: DirectMutationId) -> Result<DirectMutationStatus> {
        let Some(store) = &self.store else {
            return Err(Error::new(
                ErrorKind::NotFound,
                "read-only Direct runtime has no mutation state",
            ));
        };
        let record = guard(store).mutation(id.0)?.ok_or_else(|| {
            Error::new(
                ErrorKind::NotFound,
                format!("unknown Direct mutation {}", id.to_hex()),
            )
        })?;
        status_from_mutation(&record)
    }

    /// Open one observed file, sharing any retained local generation for its key.
    pub async fn open_file(
        &self,
        snapshot: &DirectSnapshot,
        id: DirectId,
        writable: bool,
    ) -> Result<DirectFileHandle> {
        let node = snapshot
            .node(id)
            .ok_or_else(|| Error::new(ErrorKind::NotFound, "file is not in this snapshot"))?;
        if !node.is_file() {
            return Err(Error::new(
                ErrorKind::IsDirectory,
                format!("{:?} is a directory", node.path()),
            ));
        }
        let entry = self.entry_for_node(snapshot, node, writable).await?;
        if writable {
            if self.read_only {
                return Err(Error::new(
                    ErrorKind::Unsupported,
                    "Direct runtime is read-only",
                ));
            }
            self.materialize(&entry).await?;
        }
        Ok(DirectFileHandle {
            runtime: self.clone(),
            entry,
            writable,
        })
    }

    /// Open a durable pending object by its local identity.
    pub async fn open_pending(&self, id: DirectId, writable: bool) -> Result<DirectFileHandle> {
        if writable && self.read_only {
            return Err(Error::new(
                ErrorKind::Unsupported,
                "Direct runtime is read-only",
            ));
        }
        let entry = if let Some(entry) = self.objects.lock().await.get(&id).cloned() {
            entry
        } else {
            let Some(store) = &self.store else {
                return Err(Error::new(
                    ErrorKind::NotFound,
                    "read-only Direct runtime has no pending objects",
                ));
            };
            let record = guard(store).object_by_id(*id.as_bytes())?.ok_or_else(|| {
                Error::new(ErrorKind::NotFound, "pending Direct object is not retained")
            })?;
            let entry = entry_from_record(store, record)?;
            self.objects
                .lock()
                .await
                .entry(id)
                .or_insert_with(|| entry.clone())
                .clone()
        };
        if writable {
            self.materialize(&entry).await?;
        }
        Ok(DirectFileHandle {
            runtime: self.clone(),
            entry,
            writable,
        })
    }

    /// Create an empty local-only file. Storage is not changed until flush.
    pub async fn create_file(
        &self,
        snapshot: &DirectSnapshot,
        parent: DirectId,
        name: &str,
    ) -> Result<DirectFileHandle> {
        if self.read_only {
            return Err(Error::new(
                ErrorKind::Unsupported,
                "Direct runtime is read-only",
            ));
        }
        let parent_node = snapshot
            .node(parent)
            .ok_or_else(|| Error::new(ErrorKind::NotFound, "parent is not in this snapshot"))?;
        if !parent_node.is_directory() {
            return Err(Error::new(
                ErrorKind::NotDirectory,
                format!("{:?} is not a directory", parent_node.path()),
            ));
        }
        if snapshot.lookup(parent, name)?.is_some() {
            return Err(Error::new(
                ErrorKind::Invalid,
                format!(
                    "Direct path {:?} already exists remotely",
                    parent_node.path()
                ),
            ));
        }
        let path = if parent_node.path().is_empty() {
            name.to_owned()
        } else {
            format!("{}/{name}", parent_node.path())
        };
        let key = DirectKey::from_path(&path)?;
        let store = self.store.as_ref().expect("writable runtime has a store");
        if guard(store).object(key.as_str())?.is_some()
            || self
                .objects
                .lock()
                .await
                .values()
                .any(|entry| entry.identity.key == key)
        {
            return Err(Error::new(
                ErrorKind::Invalid,
                format!("Direct local path {path:?} already has pending state"),
            ));
        }
        let id = derive_id(&path, super::DirectKind::File, None);
        let data_path = guard(store).data_path(*id.as_bytes());
        create_private_file(&data_path)?;
        let record = ObjectRecord {
            id: *id.as_bytes(),
            key: key.as_str().to_owned(),
            path: path.clone(),
            parent: *parent.as_bytes(),
            name: name.to_owned(),
            existed_remote: false,
            precondition: PreconditionRecord::Absent,
            state: OBJECT_STAGED,
            generation: 1,
            length: 0,
            digest: Some(EMPTY_DIGEST),
            accepted: None,
            observed: ObservationRecord::Unavailable("not observed".to_owned()),
            pending_mutation: None,
            error: None,
        };
        guard(store).put_object(&record)?;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&data_path)
            .map_err(|error| local_io("open staged Direct object", error))?;
        let identity = ObjectIdentity {
            id,
            key,
            path,
            parent,
            name: name.to_owned(),
            existed_remote: false,
        };
        let inner = ObjectInner {
            remote: None,
            file: Some(file),
            data_path: Some(data_path),
            state: DirectObjectState::Staged,
            length: 0,
            generation: 1,
            durable_generation: 1,
            digest: Some(EMPTY_DIGEST),
            precondition: Some(DirectPrecondition::Absent),
            accepted: None,
            observed: DirectObservation::Unavailable {
                message: "not observed".to_owned(),
            },
            pending_mutation: None,
            error: None,
        };
        let entry = Arc::new(ObjectEntry::new(identity, inner));
        self.objects.lock().await.insert(id, entry.clone());
        Ok(DirectFileHandle {
            runtime: self.clone(),
            entry,
            writable: true,
        })
    }

    async fn entry_for_node(
        &self,
        snapshot: &DirectSnapshot,
        node: &DirectNode,
        _writable: bool,
    ) -> Result<Arc<ObjectEntry>> {
        let key = DirectKey::from_storage_key(node.key().expect("file node has key"))?;
        if let Some(entry) = self
            .objects
            .lock()
            .await
            .values()
            .find(|entry| entry.identity.key == key)
            .cloned()
        {
            return Ok(entry);
        }
        if let Some(store) = &self.store {
            if let Some(record) = guard(store).object(key.as_str())? {
                if record.state != 0 {
                    let entry = entry_from_record(store, record)?;
                    return Ok(self
                        .objects
                        .lock()
                        .await
                        .entry(entry.identity.id)
                        .or_insert_with(|| entry.clone())
                        .clone());
                }
            }
        }

        let file = snapshot.open_file(node.id()).await?;
        let precondition = file
            .etag()
            .map(|etag| DirectPrecondition::Etag(etag.to_owned()));
        let identity = ObjectIdentity {
            id: node.id(),
            key: key.clone(),
            path: node.path().to_owned(),
            parent: node.parent().ok_or_else(|| {
                Error::new(ErrorKind::Invalid, "file node does not have a parent")
            })?,
            name: node.name().to_owned(),
            existed_remote: true,
        };
        let inner = ObjectInner {
            remote: Some(file),
            file: None,
            data_path: None,
            state: DirectObjectState::Clean,
            length: node.size(),
            generation: 0,
            durable_generation: 0,
            digest: None,
            precondition,
            accepted: None,
            observed: DirectObservation::Unavailable {
                message: "not observed".to_owned(),
            },
            pending_mutation: None,
            error: None,
        };
        let entry = Arc::new(ObjectEntry::new(identity, inner));
        Ok(self
            .objects
            .lock()
            .await
            .entry(node.id())
            .or_insert_with(|| entry.clone())
            .clone())
    }

    async fn materialize(&self, entry: &Arc<ObjectEntry>) -> Result<()> {
        let mut inner = entry.inner.lock().await;
        if inner.file.is_some() {
            return Ok(());
        }
        if self.read_only {
            return Err(Error::new(
                ErrorKind::Unsupported,
                "Direct runtime is read-only",
            ));
        }
        let store = self.store.as_ref().expect("writable runtime has a store");
        let precondition = inner.precondition.clone().ok_or_else(|| {
            Error::new(
                ErrorKind::Unsupported,
                "object exposes no ETag for conditional mutation",
            )
        })?;
        let remote = inner.remote.clone().ok_or_else(|| {
            Error::new(
                ErrorKind::NotFound,
                "Direct object has neither local bytes nor a remote descriptor",
            )
        })?;
        self.verify_remote(&entry.identity.key, &precondition)
            .await?;

        let id = *entry.identity.id.as_bytes();
        let target = guard(store).data_path(id);
        let temporary = guard(store).temporary_path("objects", id);
        remove_if_exists(&temporary)?;
        let mut output = create_private_file(&temporary)?;
        let mut offset = 0_u64;
        let mut hasher = blake3::Hasher::new();
        while offset < remote.length() {
            let read = remote
                .read(offset, IO_CHUNK.min((remote.length() - offset) as usize))
                .await?;
            if read.is_empty() {
                return Err(Error::new(
                    ErrorKind::Stale,
                    "pinned Direct object ended while materializing",
                ));
            }
            output
                .write_all(&read)
                .map_err(|error| local_io("write staged Direct object", error))?;
            hasher.update(&read);
            offset += read.len() as u64;
        }
        output
            .sync_all()
            .map_err(|error| local_io("sync staged Direct object", error))?;
        std::fs::rename(&temporary, &target)
            .map_err(|error| local_io("publish staged Direct object", error))?;
        sync_parent(&target)?;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&target)
            .map_err(|error| local_io("open staged Direct object", error))?;
        let digest = *hasher.finalize().as_bytes();
        inner.file = Some(file);
        inner.data_path = Some(target);
        inner.length = remote.length();
        inner.generation = 0;
        inner.durable_generation = 0;
        inner.digest = Some(digest);
        inner.state = DirectObjectState::Clean;
        inner.accepted = None;
        inner.observed = DirectObservation::Unavailable {
            message: "not observed".to_owned(),
        };
        inner.error = None;
        let record = record_from_entry(entry, &inner);
        guard(store).put_object(&record)?;
        Ok(())
    }

    async fn verify_remote(
        &self,
        key: &DirectKey,
        precondition: &DirectPrecondition,
    ) -> Result<()> {
        let metadata =
            self.namespace
                .operator
                .stat(key.as_str())
                .await
                .map_err(|error| match error.kind() {
                    opendal::ErrorKind::NotFound => {
                        Error::new(ErrorKind::Stale, "Direct object no longer exists")
                    }
                    _ => Error::storage(error),
                })?;
        if metadata.mode() == EntryMode::DIR {
            return Err(Error::new(
                ErrorKind::IsDirectory,
                "Direct object is now a directory",
            ));
        }
        match precondition {
            DirectPrecondition::Absent => Err(Error::new(
                ErrorKind::Stale,
                "Direct object appeared after local creation",
            )),
            DirectPrecondition::Etag(etag) if metadata.etag() == Some(etag.as_str()) => Ok(()),
            DirectPrecondition::Etag(_) => Err(Error::new(
                ErrorKind::Stale,
                "Direct object ETag changed after observation",
            )),
        }
    }

    async fn flush_entry(&self, entry: &Arc<ObjectEntry>) -> Result<DirectFlushOutcome> {
        let store = self.store.as_ref().expect("writable runtime has a store");
        let mut inner = entry.inner.lock().await;
        if inner.state == DirectObjectState::Clean && inner.generation == inner.durable_generation {
            return Ok(DirectFlushOutcome::Unchanged);
        }
        if matches!(
            inner.state,
            DirectObjectState::Conflict
                | DirectObjectState::Unknown
                | DirectObjectState::Pending
                | DirectObjectState::Failed
        ) {
            return Ok(flush_outcome_from_entry(entry, &inner));
        }
        if inner.file.is_none() {
            return Err(Error::new(
                ErrorKind::Invalid,
                "Direct object has no staged bytes to flush",
            ));
        }
        let precondition = inner.precondition.clone().ok_or_else(|| {
            Error::new(
                ErrorKind::Unsupported,
                "object exposes no ETag for conditional mutation",
            )
        })?;
        let mutation_id = DirectMutationId::new();
        let input_path = guard(store).input_path(mutation_id.0);
        let temporary = guard(store).temporary_path("inputs", mutation_id.0);
        remove_if_exists(&temporary)?;
        let (input_length, input_digest) = freeze_object(
            inner.file.as_mut().expect("checked staged file"),
            &temporary,
            &input_path,
        )?;
        if input_length != inner.length {
            return Err(Error::new(
                ErrorKind::Storage,
                format!(
                    "staged Direct object is {input_length} bytes, expected {}",
                    inner.length
                ),
            ));
        }
        inner.digest = Some(input_digest);

        let destination = endpoint_from_identity(&entry.identity, precondition.clone());
        let id = mutation_id.0;
        let kind = if entry.identity.existed_remote {
            MUTATION_KIND_PUBLISH
        } else {
            MUTATION_KIND_CREATE
        };
        let mut mutation = MutationRecord {
            id,
            kind,
            state: MUTATION_STAGED,
            generation: inner.generation,
            source: None,
            destination: Some(destination),
            input_length,
            input_digest: Some(input_digest),
            accepted: None,
            observed: ObservationRecord::Unavailable("not observed".to_owned()),
            error: None,
        };
        inner.state = DirectObjectState::Staged;
        inner.pending_mutation = Some(mutation_id);
        inner.error = None;
        let mut object = record_from_entry(entry, &inner);
        mutation.state = MUTATION_DISPATCHING;
        object.state = OBJECT_PENDING;
        guard(store).put_records(&[object], &[mutation.clone()])?;
        inner.state = DirectObjectState::Pending;

        let outcome = self
            .mutation
            .publish_file(
                &entry.identity.key,
                &input_path,
                mutation
                    .destination
                    .as_ref()
                    .map(|endpoint| precondition_from_record(&endpoint.precondition))
                    .expect("publication has a destination"),
            )
            .await;
        match outcome {
            Ok(DirectMutationOutcome::Applied { accepted }) => {
                mutation.state = MUTATION_APPLIED;
                mutation.accepted = accepted.as_ref().map(pin_to_record);
                mutation.error = None;
                inner.accepted = accepted.clone();
                inner.observed = DirectObservation::Unavailable {
                    message: "not observed".to_owned(),
                };
                inner.pending_mutation = Some(mutation_id);
                inner.durable_generation = inner.generation;
                if let Some(etag) = accepted.as_ref().and_then(DirectObjectPin::etag) {
                    inner.precondition = Some(DirectPrecondition::Etag(etag.to_owned()));
                    inner.state = DirectObjectState::Clean;
                    inner.error = None;
                } else {
                    inner.state = DirectObjectState::Failed;
                    inner.error =
                        Some("successful Direct write response omitted an ETag".to_owned());
                }
                let object = record_from_entry(entry, &inner);
                guard(store).put_records(&[object], &[mutation])?;
                Ok(flush_outcome_from_entry(entry, &inner))
            }
            Ok(DirectMutationOutcome::Conflict { observed }) => {
                mutation.state = MUTATION_CONFLICT;
                mutation.observed = observation_to_record(&observed);
                mutation.error =
                    Some("storage precondition changed; local bytes are retained".to_owned());
                inner.state = DirectObjectState::Conflict;
                inner.observed = observed;
                inner.error = mutation.error.clone();
                let object = record_from_entry(entry, &inner);
                guard(store).put_records(&[object], &[mutation])?;
                Ok(flush_outcome_from_entry(entry, &inner))
            }
            Ok(DirectMutationOutcome::Unknown { observed, message }) => {
                mutation.state = MUTATION_UNKNOWN;
                mutation.observed = observation_to_record(&observed);
                mutation.error = Some(message.clone());
                inner.state = DirectObjectState::Unknown;
                inner.observed = observed;
                inner.error = Some(message);
                let object = record_from_entry(entry, &inner);
                guard(store).put_records(&[object], &[mutation])?;
                Ok(flush_outcome_from_entry(entry, &inner))
            }
            Ok(DirectMutationOutcome::Unsupported { message }) => {
                mutation.state = MUTATION_FAILED;
                mutation.error = Some(message.clone());
                inner.state = DirectObjectState::Failed;
                inner.error = Some(message);
                let object = record_from_entry(entry, &inner);
                guard(store).put_records(&[object], &[mutation])?;
                Ok(flush_outcome_from_entry(entry, &inner))
            }
            Err(error) => {
                // Dispatch was durable before the call. A process-level or
                // local error cannot prove that completion did not race with it.
                mutation.state = MUTATION_UNKNOWN;
                mutation.error = Some(error.to_string());
                inner.state = DirectObjectState::Unknown;
                inner.error = Some(error.to_string());
                let object = record_from_entry(entry, &inner);
                guard(store).put_records(&[object], &[mutation])?;
                Ok(flush_outcome_from_entry(entry, &inner))
            }
        }
    }
}

/// One Direct file descriptor view sharing local generations by object key.
#[derive(Clone)]
pub struct DirectFileHandle {
    runtime: DirectRuntime,
    entry: Arc<ObjectEntry>,
    writable: bool,
}

impl DirectFileHandle {
    pub fn id(&self) -> DirectId {
        self.entry.identity.id
    }

    pub fn key(&self) -> &DirectKey {
        &self.entry.identity.key
    }

    pub fn path(&self) -> &str {
        &self.entry.identity.path
    }

    pub fn parent(&self) -> DirectId {
        self.entry.identity.parent
    }

    pub fn name(&self) -> &str {
        &self.entry.identity.name
    }

    pub fn existed_remote(&self) -> bool {
        self.entry.identity.existed_remote
    }

    pub fn is_writable(&self) -> bool {
        self.writable
    }

    pub async fn len(&self) -> Result<u64> {
        Ok(self.entry.inner.lock().await.length)
    }

    pub async fn status(&self) -> Result<DirectObjectStatus> {
        Ok(status_from_entry(&self.entry).await)
    }

    pub async fn read(&self, offset: u64, length: usize) -> Result<Vec<u8>> {
        let mut inner = self.entry.inner.lock().await;
        if offset >= inner.length || length == 0 {
            return Ok(Vec::new());
        }
        let end = offset.saturating_add(length as u64).min(inner.length);
        let size = (end - offset) as usize;
        if let Some(file) = inner.file.as_mut() {
            let mut bytes = vec![0_u8; size];
            file.seek(SeekFrom::Start(offset))
                .map_err(|error| local_io("seek staged Direct object", error))?;
            file.read_exact(&mut bytes)
                .map_err(|error| local_io("read staged Direct object", error))?;
            return Ok(bytes);
        }
        let remote = inner.remote.as_ref().ok_or_else(|| {
            Error::new(
                ErrorKind::NotFound,
                "Direct object has neither local nor remote bytes",
            )
        })?;
        remote.read(offset, size).await
    }

    pub async fn write(&self, offset: u64, bytes: &[u8]) -> Result<()> {
        self.require_writable()?;
        let mut inner = self.entry.inner.lock().await;
        let file = inner.file.as_mut().ok_or_else(|| {
            Error::new(
                ErrorKind::Invalid,
                "writable Direct handle is not materialized",
            )
        })?;
        file.seek(SeekFrom::Start(offset))
            .map_err(|error| local_io("seek staged Direct object", error))?;
        file.write_all(bytes)
            .map_err(|error| local_io("write staged Direct object", error))?;
        inner.length = inner.length.max(offset.saturating_add(bytes.len() as u64));
        bump_generation(&mut inner);
        Ok(())
    }

    pub async fn append(&self, bytes: &[u8]) -> Result<u64> {
        self.require_writable()?;
        let mut inner = self.entry.inner.lock().await;
        let offset = inner.length;
        let file = inner.file.as_mut().ok_or_else(|| {
            Error::new(
                ErrorKind::Invalid,
                "writable Direct handle is not materialized",
            )
        })?;
        file.seek(SeekFrom::Start(offset))
            .map_err(|error| local_io("seek staged Direct object", error))?;
        file.write_all(bytes)
            .map_err(|error| local_io("append staged Direct object", error))?;
        inner.length = inner.length.saturating_add(bytes.len() as u64);
        bump_generation(&mut inner);
        Ok(offset)
    }

    pub async fn truncate(&self, length: u64) -> Result<()> {
        self.require_writable()?;
        let mut inner = self.entry.inner.lock().await;
        let file = inner.file.as_mut().ok_or_else(|| {
            Error::new(
                ErrorKind::Invalid,
                "writable Direct handle is not materialized",
            )
        })?;
        file.set_len(length)
            .map_err(|error| local_io("truncate staged Direct object", error))?;
        if inner.length != length {
            inner.length = length;
            bump_generation(&mut inner);
        }
        Ok(())
    }

    pub async fn flush(&self) -> Result<DirectFlushOutcome> {
        if !self.writable {
            return Ok(DirectFlushOutcome::Unchanged);
        }
        self.runtime.flush_entry(&self.entry).await
    }

    pub async fn export(&self, destination: &Path) -> Result<()> {
        let mut output = File::create(destination)
            .map_err(|error| local_io("create Direct object export", error))?;
        let length = self.len().await?;
        let mut offset = 0_u64;
        while offset < length {
            let bytes = self
                .read(offset, IO_CHUNK.min((length - offset) as usize))
                .await?;
            if bytes.is_empty() {
                return Err(Error::new(
                    ErrorKind::Stale,
                    "Direct object ended during export",
                ));
            }
            output
                .write_all(&bytes)
                .map_err(|error| local_io("write Direct object export", error))?;
            offset += bytes.len() as u64;
        }
        output
            .sync_all()
            .map_err(|error| local_io("sync Direct object export", error))
    }

    fn require_writable(&self) -> Result<()> {
        if !self.writable {
            return Err(Error::new(
                ErrorKind::Unsupported,
                "Direct file handle is read-only",
            ));
        }
        Ok(())
    }
}

fn bump_generation(inner: &mut ObjectInner) {
    inner.generation = inner.generation.saturating_add(1);
    if inner.state == DirectObjectState::Clean {
        inner.state = DirectObjectState::Staged;
    }
    inner.error = None;
}

fn entry_from_record(store: &Arc<Mutex<Store>>, record: ObjectRecord) -> Result<Arc<ObjectEntry>> {
    let data_path = guard(store).data_path(record.id);
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&data_path)
        .map_err(|error| local_io("open retained Direct object", error))?;
    let file_length = file
        .metadata()
        .map_err(|error| local_io("inspect retained Direct object", error))?
        .len();
    if file_length < record.length {
        return Err(Error::new(
            ErrorKind::Storage,
            format!(
                "retained Direct object {} is {file_length} bytes, expected {}",
                record.key, record.length
            ),
        ));
    }
    if file_length > record.length {
        file.set_len(record.length)
            .map_err(|error| local_io("discard unacknowledged Direct object tail", error))?;
    }
    let identity = ObjectIdentity {
        id: record.direct_id(),
        key: DirectKey::from_storage_key(&record.key)?,
        path: record.path,
        parent: DirectId::from_bytes(record.parent),
        name: record.name,
        existed_remote: record.existed_remote,
    };
    let inner = ObjectInner {
        remote: None,
        file: Some(file),
        data_path: Some(data_path),
        state: object_state(record.state)?,
        length: record.length,
        generation: record.generation,
        durable_generation: record.generation,
        digest: record.digest,
        precondition: Some(precondition_from_record(&record.precondition)),
        accepted: record.accepted.map(pin_from_record),
        observed: observation_from_record(record.observed),
        pending_mutation: record.pending_mutation.map(DirectMutationId::from_bytes),
        error: record.error,
    };
    Ok(Arc::new(ObjectEntry::new(identity, inner)))
}

fn record_from_entry(entry: &ObjectEntry, inner: &ObjectInner) -> ObjectRecord {
    ObjectRecord {
        id: *entry.identity.id.as_bytes(),
        key: entry.identity.key.as_str().to_owned(),
        path: entry.identity.path.clone(),
        parent: *entry.identity.parent.as_bytes(),
        name: entry.identity.name.clone(),
        existed_remote: entry.identity.existed_remote,
        precondition: precondition_to_record(
            inner
                .precondition
                .as_ref()
                .expect("durable Direct objects have a precondition"),
        ),
        state: object_state_code(inner.state),
        generation: inner.generation,
        length: inner.length,
        digest: inner.digest,
        accepted: inner.accepted.as_ref().map(pin_to_record),
        observed: observation_to_record(&inner.observed),
        pending_mutation: inner.pending_mutation.map(|id| *id.as_bytes()),
        error: inner.error.clone(),
    }
}

async fn status_from_entry(entry: &ObjectEntry) -> DirectObjectStatus {
    let inner = entry.inner.lock().await;
    DirectObjectStatus {
        id: entry.identity.id,
        key: entry.identity.key.clone(),
        path: entry.identity.path.clone(),
        parent: entry.identity.parent,
        name: entry.identity.name.clone(),
        length: inner.length,
        state: inner.state,
        existed_remote: entry.identity.existed_remote,
        generation: inner.generation,
        precondition: inner.precondition.clone(),
        accepted: inner.accepted.clone(),
        observed: inner.observed.clone(),
        pending_mutation: inner.pending_mutation,
        error: inner.error.clone(),
    }
}

fn status_from_record(record: &ObjectRecord) -> Result<DirectObjectStatus> {
    Ok(DirectObjectStatus {
        id: record.direct_id(),
        key: DirectKey::from_storage_key(&record.key)?,
        path: record.path.clone(),
        parent: DirectId::from_bytes(record.parent),
        name: record.name.clone(),
        length: record.length,
        state: object_state(record.state)?,
        existed_remote: record.existed_remote,
        generation: record.generation,
        precondition: Some(precondition_from_record(&record.precondition)),
        accepted: record.accepted.clone().map(pin_from_record),
        observed: observation_from_record(record.observed.clone()),
        pending_mutation: record.pending_mutation.map(DirectMutationId::from_bytes),
        error: record.error.clone(),
    })
}

fn status_from_mutation(record: &MutationRecord) -> Result<DirectMutationStatus> {
    Ok(DirectMutationStatus {
        id: DirectMutationId::from_bytes(record.id),
        kind: mutation_kind(record.kind)?,
        state: mutation_state(record.state)?,
        generation: record.generation,
        source: record
            .source
            .as_ref()
            .map(endpoint_from_record)
            .transpose()?,
        destination: record
            .destination
            .as_ref()
            .map(endpoint_from_record)
            .transpose()?,
        input_length: record.input_length,
        input_digest: record.input_digest,
        accepted: record.accepted.clone().map(pin_from_record),
        observed: observation_from_record(record.observed.clone()),
        error: record.error.clone(),
    })
}

fn endpoint_from_identity(
    identity: &ObjectIdentity,
    precondition: DirectPrecondition,
) -> EndpointRecord {
    EndpointRecord {
        id: *identity.id.as_bytes(),
        key: identity.key.as_str().to_owned(),
        path: identity.path.clone(),
        parent: *identity.parent.as_bytes(),
        name: identity.name.clone(),
        precondition: precondition_to_record(&precondition),
    }
}

fn endpoint_from_record(record: &EndpointRecord) -> Result<DirectMutationEndpoint> {
    Ok(DirectMutationEndpoint {
        id: record.direct_id(),
        key: DirectKey::from_storage_key(&record.key)?,
        path: record.path.clone(),
        parent: DirectId::from_bytes(record.parent),
        name: record.name.clone(),
        precondition: precondition_from_record(&record.precondition),
    })
}

fn flush_outcome_from_entry(entry: &ObjectEntry, inner: &ObjectInner) -> DirectFlushOutcome {
    let mutation = inner.pending_mutation.unwrap_or_else(DirectMutationId::new);
    match inner.state {
        DirectObjectState::Clean if inner.generation == inner.durable_generation => {
            if inner.accepted.is_some() {
                DirectFlushOutcome::Applied {
                    mutation,
                    accepted: inner.accepted.clone(),
                }
            } else {
                DirectFlushOutcome::Unchanged
            }
        }
        DirectObjectState::Conflict => DirectFlushOutcome::Conflict {
            mutation,
            observed: inner.observed.clone(),
        },
        DirectObjectState::Unknown => DirectFlushOutcome::Unknown {
            mutation,
            observed: inner.observed.clone(),
            message: inner
                .error
                .clone()
                .unwrap_or_else(|| "Direct publication outcome is unknown".to_owned()),
        },
        DirectObjectState::Failed => DirectFlushOutcome::Unsupported {
            message: inner
                .error
                .clone()
                .unwrap_or_else(|| format!("Direct mutation for {:?} failed", entry.identity.path)),
        },
        DirectObjectState::Pending | DirectObjectState::Staged => DirectFlushOutcome::Unknown {
            mutation,
            observed: inner.observed.clone(),
            message: "Direct mutation did not reach a terminal outcome".to_owned(),
        },
    }
}

fn object_state(value: u8) -> Result<DirectObjectState> {
    match value {
        0 => Ok(DirectObjectState::Clean),
        OBJECT_STAGED => Ok(DirectObjectState::Staged),
        OBJECT_PENDING => Ok(DirectObjectState::Pending),
        OBJECT_CONFLICT => Ok(DirectObjectState::Conflict),
        OBJECT_UNKNOWN => Ok(DirectObjectState::Unknown),
        OBJECT_FAILED => Ok(DirectObjectState::Failed),
        _ => Err(Error::new(
            ErrorKind::Storage,
            format!("unknown Direct object state {value}"),
        )),
    }
}

fn object_state_code(value: DirectObjectState) -> u8 {
    match value {
        DirectObjectState::Clean => 0,
        DirectObjectState::Staged => OBJECT_STAGED,
        DirectObjectState::Pending => OBJECT_PENDING,
        DirectObjectState::Conflict => OBJECT_CONFLICT,
        DirectObjectState::Unknown => OBJECT_UNKNOWN,
        DirectObjectState::Failed => OBJECT_FAILED,
    }
}

fn mutation_kind(value: u8) -> Result<DirectMutationKind> {
    match value {
        MUTATION_KIND_PUBLISH => Ok(DirectMutationKind::Publish),
        MUTATION_KIND_CREATE => Ok(DirectMutationKind::Create),
        MUTATION_KIND_DELETE => Ok(DirectMutationKind::Delete),
        MUTATION_KIND_COPY => Ok(DirectMutationKind::Copy),
        MUTATION_KIND_RENAME => Ok(DirectMutationKind::Rename),
        _ => Err(Error::new(
            ErrorKind::Storage,
            format!("unknown Direct mutation kind {value}"),
        )),
    }
}

fn mutation_state(value: u8) -> Result<DirectMutationState> {
    match value {
        MUTATION_STAGED => Ok(DirectMutationState::Staged),
        MUTATION_DISPATCHING => Ok(DirectMutationState::Dispatching),
        MUTATION_APPLIED => Ok(DirectMutationState::Applied),
        MUTATION_CONFLICT => Ok(DirectMutationState::Conflict),
        MUTATION_UNKNOWN => Ok(DirectMutationState::Unknown),
        MUTATION_DESTINATION_APPLIED => Ok(DirectMutationState::DestinationApplied),
        MUTATION_PENDING_SOURCE_CLEANUP => Ok(DirectMutationState::PendingSourceCleanup),
        MUTATION_FAILED => Ok(DirectMutationState::Failed),
        MUTATION_SOURCE_DISPATCHING => Ok(DirectMutationState::PendingSourceCleanup),
        _ => Err(Error::new(
            ErrorKind::Storage,
            format!("unknown Direct mutation state {value}"),
        )),
    }
}

fn precondition_to_record(value: &DirectPrecondition) -> PreconditionRecord {
    match value {
        DirectPrecondition::Absent => PreconditionRecord::Absent,
        DirectPrecondition::Etag(etag) => PreconditionRecord::Etag(etag.clone()),
    }
}

fn precondition_from_record(value: &PreconditionRecord) -> DirectPrecondition {
    match value {
        PreconditionRecord::Absent => DirectPrecondition::Absent,
        PreconditionRecord::Etag(etag) => DirectPrecondition::Etag(etag.clone()),
    }
}

fn pin_to_record(value: &DirectObjectPin) -> PinRecord {
    PinRecord {
        version: value.version().map(str::to_owned),
        etag: value.etag().map(str::to_owned),
        size: value.size(),
        modified: value
            .modified()
            .and_then(|time| time.duration_since(SystemTime::UNIX_EPOCH).ok())
            .map(|duration| duration.as_secs()),
    }
}

fn pin_from_record(value: PinRecord) -> DirectObjectPin {
    DirectObjectPin::from_parts(value.version, value.etag, value.size, value.modified_time())
}

fn observation_to_record(value: &DirectObservation) -> ObservationRecord {
    match value {
        DirectObservation::Present(pin) => ObservationRecord::Present(pin_to_record(pin)),
        DirectObservation::Missing => ObservationRecord::Missing,
        DirectObservation::Unavailable { message } => {
            ObservationRecord::Unavailable(message.clone())
        }
    }
}

fn observation_from_record(value: ObservationRecord) -> DirectObservation {
    match value {
        ObservationRecord::Present(pin) => DirectObservation::Present(pin_from_record(pin)),
        ObservationRecord::Missing => DirectObservation::Missing,
        ObservationRecord::Unavailable(message) => DirectObservation::Unavailable { message },
    }
}

fn freeze_object(file: &mut File, temporary: &Path, target: &Path) -> Result<(u64, [u8; 32])> {
    file.seek(SeekFrom::Start(0))
        .map_err(|error| local_io("seek Direct mutation input", error))?;
    let mut output = create_private_file(temporary)?;
    let mut hasher = blake3::Hasher::new();
    let mut buffer = vec![0_u8; IO_CHUNK];
    let mut length = 0_u64;
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|error| local_io("read Direct mutation input", error))?;
        if read == 0 {
            break;
        }
        output
            .write_all(&buffer[..read])
            .map_err(|error| local_io("freeze Direct mutation input", error))?;
        hasher.update(&buffer[..read]);
        length += read as u64;
    }
    output
        .sync_all()
        .map_err(|error| local_io("sync Direct mutation input", error))?;
    std::fs::rename(temporary, target)
        .map_err(|error| local_io("publish Direct mutation input", error))?;
    sync_parent(target)?;
    Ok((length, *hasher.finalize().as_bytes()))
}

fn create_private_file(path: &Path) -> Result<File> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|error| local_io("create Direct runtime file", error))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        file.set_permissions(std::fs::Permissions::from_mode(0o600))
            .map_err(|error| local_io("protect Direct runtime file", error))?;
    }
    Ok(file)
}

fn remove_if_exists(path: &Path) -> Result<()> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(local_io("remove stale Direct temporary file", error)),
    }
}

fn sync_parent(path: &Path) -> Result<()> {
    let parent = path.parent().ok_or_else(|| {
        Error::new(
            ErrorKind::Storage,
            "Direct runtime path has no parent directory",
        )
    })?;
    let directory =
        File::open(parent).map_err(|error| local_io("open Direct runtime directory", error))?;
    directory
        .sync_all()
        .map_err(|error| local_io("sync Direct runtime directory", error))
}

fn local_io(context: &str, error: std::io::Error) -> Error {
    let kind = ErrorKind::Storage;
    Error::new(kind, format!("{context}: {error}"))
}

fn guard<T>(lock: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}
