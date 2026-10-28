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
    MUTATION_UNKNOWN, MutationRecord, OBJECT_CONFLICT, OBJECT_DISCARDED, OBJECT_FAILED,
    OBJECT_PENDING, OBJECT_STAGED, OBJECT_UNKNOWN, ObjectRecord, ObservationRecord, PinRecord,
    PreconditionRecord, Store,
};
use super::mutation::{
    DirectMutation, DirectMutationOutcome, DirectObjectPin, DirectObservation, DirectPrecondition,
};
use super::{
    DirectFile, DirectId, DirectKey, DirectNamespace, DirectNode, DirectSnapshot, Error, ErrorKind,
    Result, derive_id, pin_value,
};
use opendal::EntryMode;
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
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

/// Process-local identity for one shared Direct object entry.
///
/// The value survives clean-entry rebinding across Direct snapshot pins and is
/// intentionally not persisted across runtime restarts.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct DirectObjectId([u8; 16]);

impl DirectObjectId {
    pub fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }

    pub fn to_hex(self) -> String {
        self.0.iter().map(|byte| format!("{byte:02x}")).collect()
    }

    fn new() -> Self {
        Self(*uuid::Uuid::new_v4().as_bytes())
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
    Discarded,
}

/// Explicit decision for one retained Conflict or Unknown object generation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DirectConflictResolution {
    /// Rebase local bytes onto the currently observed ETag/absence and stage a
    /// new conditional generation.
    KeepLocal,
    /// Drop local bytes without a storage request and expose the current remote
    /// namespace after refresh.
    KeepRemote,
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

/// Whether durable Direct state exists without changing it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DirectRuntimeInspectionState {
    Missing,
    Uninitialized,
    Ready,
}

/// Read-only object-generation counts.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct DirectObjectCounts {
    pub clean: u64,
    pub staged: u64,
    pub pending: u64,
    pub conflict: u64,
    pub unknown: u64,
    pub failed: u64,
    pub discarded: u64,
}

/// Read-only mutation-intent counts.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct DirectMutationCounts {
    pub staged: u64,
    pub dispatching: u64,
    pub applied: u64,
    pub conflict: u64,
    pub unknown: u64,
    pub destination_applied: u64,
    pub pending_source_cleanup: u64,
    pub failed: u64,
    pub source_dispatching: u64,
}

/// Non-mutating inspection result for writable Direct doctor admission.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DirectRuntimeInspection {
    pub state: DirectRuntimeInspectionState,
    pub profile: Option<String>,
    pub binding_id: Option<[u8; 32]>,
    pub binding_description: Option<String>,
    pub objects: DirectObjectCounts,
    pub mutations: DirectMutationCounts,
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
    pub source_observed: DirectObservation,
    pub destination_observed: DirectObservation,
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

struct ObjectIdentity {
    id: RwLock<DirectId>,
    key: DirectKey,
    path: String,
    parent: DirectId,
    name: String,
    existed_remote: AtomicBool,
}

impl ObjectIdentity {
    fn id(&self) -> DirectId {
        *self
            .id
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn rebind_id(&self, id: DirectId) {
        *self
            .id
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = id;
    }
}

struct ObjectEntry {
    object_id: DirectObjectId,
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
            object_id: DirectObjectId::new(),
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
    /// Inspect durable state without creating directories, acquiring the writer
    /// lease, changing journal mode, running recovery, or contacting storage.
    pub fn inspect(
        path: &Path,
        binding: Option<&super::DirectStorageBinding>,
    ) -> Result<DirectRuntimeInspection> {
        if !path
            .try_exists()
            .map_err(|error| local_io("inspect Direct runtime path", error))?
        {
            return Ok(DirectRuntimeInspection {
                state: DirectRuntimeInspectionState::Missing,
                profile: None,
                binding_id: None,
                binding_description: None,
                objects: DirectObjectCounts::default(),
                mutations: DirectMutationCounts::default(),
            });
        }
        if !std::fs::metadata(path)
            .map_err(|error| local_io("inspect Direct runtime path", error))?
            .is_dir()
        {
            return Err(Error::new(
                ErrorKind::Invalid,
                "Direct runtime staging path is not a directory",
            ));
        }
        let database = path.join("runtime.db");
        if !database
            .try_exists()
            .map_err(|error| local_io("inspect Direct runtime database", error))?
            || std::fs::metadata(&database)
                .map_err(|error| local_io("inspect Direct runtime database", error))?
                .len()
                == 0
        {
            return Ok(DirectRuntimeInspection {
                state: DirectRuntimeInspectionState::Uninitialized,
                profile: None,
                binding_id: None,
                binding_description: None,
                objects: DirectObjectCounts::default(),
                mutations: DirectMutationCounts::default(),
            });
        }
        let inspected = super::journal::inspect(&database, binding)?;
        Ok(DirectRuntimeInspection {
            state: DirectRuntimeInspectionState::Ready,
            profile: Some(inspected.profile),
            binding_id: Some(inspected.binding_id),
            binding_description: Some(inspected.binding_description),
            objects: DirectObjectCounts {
                clean: inspected.objects[0],
                staged: inspected.objects[1],
                pending: inspected.objects[2],
                conflict: inspected.objects[3],
                unknown: inspected.objects[4],
                failed: inspected.objects[5],
                discarded: inspected.objects[6],
            },
            mutations: DirectMutationCounts {
                staged: inspected.mutations[0],
                dispatching: inspected.mutations[1],
                applied: inspected.mutations[2],
                conflict: inspected.mutations[3],
                unknown: inspected.mutations[4],
                destination_applied: inspected.mutations[5],
                pending_source_cleanup: inspected.mutations[6],
                failed: inspected.mutations[7],
                source_dispatching: inspected.mutations[8],
            },
        })
    }

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
        let entries = self
            .objects
            .lock()
            .await
            .values()
            .cloned()
            .collect::<Vec<_>>();
        for entry in entries {
            let status = status_from_entry(&entry).await;
            if status.state != DirectObjectState::Discarded
                && (status.state != DirectObjectState::Clean || !status.existed_remote)
            {
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
        if !self.mutation.capability().create {
            return Err(Error::new(
                ErrorKind::Unsupported,
                "storage does not advertise conditional creation",
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
        let retained = { guard(store).object(key.as_str())?.is_some() };
        if retained
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
            durable_generation: 0,
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
            id: RwLock::new(id),
            key,
            path,
            parent,
            name: name.to_owned(),
            existed_remote: AtomicBool::new(false),
        };
        let inner = ObjectInner {
            remote: None,
            file: Some(file),
            data_path: Some(data_path),
            state: DirectObjectState::Staged,
            length: 0,
            generation: 1,
            durable_generation: 0,
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

    /// Discard one never-flushed local-only file.
    ///
    /// This removes the local overlay without issuing a storage request. It is
    /// intentionally unavailable once a mutation may have reached storage.
    pub async fn discard_local(&self, id: DirectId) -> Result<()> {
        if self.read_only {
            return Err(Error::new(
                ErrorKind::Unsupported,
                "Direct runtime is read-only",
            ));
        }
        let entry = if let Some(entry) = self.objects.lock().await.get(&id).cloned() {
            entry
        } else {
            let store = self.store.as_ref().expect("writable runtime has a store");
            let record = guard(store).object_by_id(*id.as_bytes())?.ok_or_else(|| {
                Error::new(ErrorKind::NotFound, "Direct local object is not retained")
            })?;
            entry_from_record(store, record)?
        };
        let mut inner = entry.inner.lock().await;
        if entry.identity.existed_remote.load(Ordering::SeqCst) {
            return Err(Error::new(
                ErrorKind::Unsupported,
                "Direct object existed remotely; use conditional unlink",
            ));
        }
        if inner.state == DirectObjectState::Discarded {
            return Ok(());
        }
        if inner.pending_mutation.is_some()
            || !matches!(
                inner.state,
                DirectObjectState::Staged | DirectObjectState::Failed
            )
        {
            return Err(Error::new(
                ErrorKind::Unsupported,
                "Direct local object has a dispatched or unresolved mutation",
            ));
        }
        let store = self.store.as_ref().expect("writable runtime has a store");
        guard(store).remove_object(*id.as_bytes())?;
        inner.state = DirectObjectState::Discarded;
        inner.pending_mutation = None;
        inner.accepted = None;
        inner.observed = DirectObservation::Missing;
        inner.error = Some("Direct local name was discarded".to_owned());
        if let Some(path) = inner.data_path.take() {
            match std::fs::remove_file(&path) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => {
                    return Err(local_io("remove discarded Direct object", error));
                }
            }
        }
        self.objects.lock().await.remove(&id);
        Ok(())
    }

    /// Resolve one retained Conflict/Unknown object generation explicitly.
    ///
    /// `KeepLocal` rebases the retained bytes onto the currently observed
    /// ETag/absence; the next flush still uses a conditional write. `KeepRemote`
    /// removes only the local overlay and never mutates storage.
    pub async fn resolve_object(
        &self,
        id: DirectId,
        resolution: DirectConflictResolution,
    ) -> Result<DirectObjectStatus> {
        if self.read_only {
            return Err(Error::new(
                ErrorKind::Unsupported,
                "Direct runtime is read-only",
            ));
        }
        let entry = if let Some(entry) = self.objects.lock().await.get(&id).cloned() {
            entry
        } else {
            let store = self.store.as_ref().expect("writable runtime has a store");
            let record = guard(store).object_by_id(*id.as_bytes())?.ok_or_else(|| {
                Error::new(ErrorKind::NotFound, "Direct local object is not retained")
            })?;
            entry_from_record(store, record)?
        };
        let mut inner = entry.inner.lock().await;
        if !matches!(
            inner.state,
            DirectObjectState::Conflict | DirectObjectState::Unknown | DirectObjectState::Failed
        ) {
            return Err(Error::new(
                ErrorKind::Unsupported,
                "Direct object has no resolvable Conflict or Unknown state",
            ));
        }
        let observation = self.observe_key(&entry.identity.key).await;
        if matches!(observation, DirectObservation::Unavailable { .. }) {
            return Err(Error::new(
                ErrorKind::Unsupported,
                "cannot resolve Direct conflict without an exact observation",
            ));
        }
        match resolution {
            DirectConflictResolution::KeepLocal => {
                match &observation {
                    DirectObservation::Present(pin) => {
                        let etag = pin.etag().ok_or_else(|| {
                            Error::new(
                                ErrorKind::Unsupported,
                                "observed Direct object exposes no ETag",
                            )
                        })?;
                        inner.precondition = Some(DirectPrecondition::Etag(etag.to_owned()));
                        entry.identity.existed_remote.store(true, Ordering::SeqCst);
                    }
                    DirectObservation::Missing => {
                        inner.precondition = Some(DirectPrecondition::Absent);
                        entry.identity.existed_remote.store(false, Ordering::SeqCst);
                    }
                    DirectObservation::Unavailable { .. } => unreachable!("checked above"),
                }
                inner.generation = inner.generation.saturating_add(1);
                inner.digest = None;
                inner.state = DirectObjectState::Staged;
                inner.accepted = None;
                inner.observed = observation;
                inner.pending_mutation = None;
                inner.error = None;
                let store = self.store.as_ref().expect("writable runtime has a store");
                guard(store).put_object(&record_from_entry(&entry, &inner))?;
                Ok(status_from_entry_locked(&entry, &inner))
            }
            DirectConflictResolution::KeepRemote => {
                let store = self.store.as_ref().expect("writable runtime has a store");
                let data = inner.data_path.take();
                guard(store).remove_object(*entry.identity.id().as_bytes())?;
                if let Some(data) = data {
                    remove_if_exists(&data)?;
                }
                inner.state = DirectObjectState::Discarded;
                inner.observed = observation;
                inner.pending_mutation = None;
                inner.error = Some("Direct local bytes were discarded explicitly".to_owned());
                let status = status_from_entry_locked(&entry, &inner);
                let id = entry.identity.id();
                drop(inner);
                self.objects.lock().await.remove(&id);
                Ok(status)
            }
        }
    }

    /// Conditionally delete one observed file.
    ///
    /// If a dirty local handle exists, its bytes remain readable while the name
    /// is deleted. A later flush of that handle reports Conflict rather than
    /// recreating the removed name.
    pub async fn unlink_file(
        &self,
        snapshot: &DirectSnapshot,
        id: DirectId,
    ) -> Result<DirectMutationStatus> {
        if self.read_only {
            return Err(Error::new(
                ErrorKind::Unsupported,
                "Direct runtime is read-only",
            ));
        }
        if !self.mutation.capability().delete {
            return Err(Error::new(
                ErrorKind::Unsupported,
                "storage does not advertise conditional delete",
            ));
        }
        let node = snapshot
            .node(id)
            .ok_or_else(|| Error::new(ErrorKind::NotFound, "file is not in this snapshot"))?;
        if !node.is_file() {
            return Err(Error::new(
                ErrorKind::IsDirectory,
                format!("{:?} is a directory", node.path()),
            ));
        }
        let key = DirectKey::from_storage_key(node.key().expect("file node has key"))?;
        let existing = self
            .objects
            .lock()
            .await
            .values()
            .find(|entry| entry.identity.key == key)
            .cloned();
        let (source, generation, dirty) = if let Some(entry) = &existing {
            let inner = entry.inner.lock().await;
            if !entry.identity.existed_remote.load(Ordering::SeqCst) {
                return Err(Error::new(
                    ErrorKind::Unsupported,
                    format!(
                        "Direct path {:?} is a local-only create; use discard_local",
                        entry.identity.path
                    ),
                ));
            }
            if inner.pending_mutation.is_some()
                || matches!(
                    inner.state,
                    DirectObjectState::Pending
                        | DirectObjectState::Conflict
                        | DirectObjectState::Unknown
                )
            {
                return Err(Error::new(
                    ErrorKind::Unsupported,
                    "Direct object has an unresolved mutation",
                ));
            }
            let precondition = inner.precondition.clone().ok_or_else(|| {
                Error::new(
                    ErrorKind::Unsupported,
                    "Direct object exposes no ETag for conditional delete",
                )
            })?;
            (
                endpoint_from_identity(&entry.identity, precondition),
                inner.generation,
                inner.generation != inner.durable_generation,
            )
        } else {
            let etag = node.etag().ok_or_else(|| {
                Error::new(
                    ErrorKind::Unsupported,
                    "Direct object exposes no ETag for conditional delete",
                )
            })?;
            let identity = ObjectIdentity {
                id: RwLock::new(node.id()),
                key: key.clone(),
                path: node.path().to_owned(),
                parent: node.parent().ok_or_else(|| {
                    Error::new(ErrorKind::Invalid, "file node does not have a parent")
                })?,
                name: node.name().to_owned(),
                existed_remote: AtomicBool::new(true),
            };
            (
                endpoint_from_identity(&identity, DirectPrecondition::Etag(etag.to_owned())),
                0,
                false,
            )
        };
        let mut mutation = MutationRecord {
            id: *DirectMutationId::new().as_bytes(),
            kind: MUTATION_KIND_DELETE,
            state: MUTATION_DISPATCHING,
            generation,
            source: Some(source),
            destination: None,
            input_length: 0,
            input_digest: None,
            accepted: None,
            source_observed: ObservationRecord::Unavailable("not observed".to_owned()),
            destination_observed: ObservationRecord::Unavailable("not observed".to_owned()),
            error: None,
        };
        let store = self.store.as_ref().expect("writable runtime has a store");
        guard(store).put_mutation(&mutation)?;
        let outcome = self
            .mutation
            .delete(
                &key,
                precondition_from_record(&mutation.source.as_ref().unwrap().precondition),
            )
            .await;
        match outcome {
            Ok(DirectMutationOutcome::Applied { .. }) => {
                mutation.state = MUTATION_APPLIED;
                mutation.source_observed = ObservationRecord::Missing;
            }
            Ok(DirectMutationOutcome::Conflict { observed }) => {
                mutation.state = MUTATION_CONFLICT;
                mutation.source_observed = observation_to_record(&observed);
                mutation.error = Some(
                    "storage precondition changed; the remote name was not deleted".to_owned(),
                );
            }
            Ok(DirectMutationOutcome::Unknown { observed, message }) => {
                mutation.state = MUTATION_UNKNOWN;
                mutation.source_observed = observation_to_record(&observed);
                mutation.error = Some(message);
            }
            Ok(DirectMutationOutcome::Unsupported { message }) => {
                mutation.state = MUTATION_FAILED;
                mutation.error = Some(message);
            }
            Err(error) => {
                mutation.state = MUTATION_UNKNOWN;
                mutation.error = Some(error.to_string());
            }
        }
        let mut object = None;
        if let Some(entry) = &existing {
            let mut inner = entry.inner.lock().await;
            match mutation.state {
                MUTATION_APPLIED if !dirty => {
                    guard(store).remove_object(*entry.identity.id().as_bytes())?;
                    if let Some(path) = inner.data_path.take() {
                        remove_if_exists(&path)?;
                    }
                    self.objects.lock().await.remove(&entry.identity.id());
                }
                MUTATION_APPLIED => {
                    inner.state = DirectObjectState::Conflict;
                    inner.observed = DirectObservation::Missing;
                    inner.pending_mutation = Some(DirectMutationId::from_bytes(mutation.id));
                    inner.error =
                        Some("Direct name was unlinked while local bytes remain".to_owned());
                    object = Some(record_from_entry(entry, &inner));
                }
                MUTATION_CONFLICT => {
                    inner.state = DirectObjectState::Conflict;
                    inner.observed = observation_from_record(mutation.source_observed.clone());
                    inner.error = mutation.error.clone();
                    object = Some(record_from_entry(entry, &inner));
                }
                MUTATION_UNKNOWN => {
                    inner.state = DirectObjectState::Unknown;
                    inner.observed = observation_from_record(mutation.source_observed.clone());
                    inner.error = mutation.error.clone();
                    object = Some(record_from_entry(entry, &inner));
                }
                _ => {}
            }
        }
        match object {
            Some(object) => guard(store).put_records(
                std::slice::from_ref(&object),
                std::slice::from_ref(&mutation),
            )?,
            None => guard(store).put_mutation(&mutation)?,
        }
        status_from_mutation(&mutation)
    }

    /// Publish destination bytes from one pinned source file without deleting
    /// the source.
    pub async fn copy_file(
        &self,
        snapshot: &DirectSnapshot,
        source: DirectId,
        destination_parent: DirectId,
        destination_name: &str,
        replace: bool,
    ) -> Result<DirectMutationStatus> {
        let (mutation, input) = self
            .prepare_namespace_mutation(
                snapshot,
                source,
                destination_parent,
                destination_name,
                replace,
                MUTATION_KIND_COPY,
            )
            .await?;
        self.persist_staged_mutation(&mutation)?;
        let mutation = self.dispatch_destination(mutation, &input).await?;
        status_from_mutation(&mutation)
    }

    /// Safe-save a file: conditionally publish the destination from pinned
    /// source bytes, then conditionally delete the source.
    ///
    /// This is not a POSIX atomic rename. A destination-applied/source-pending
    /// mutation is retained for explicit recovery.
    pub async fn rename_file(
        &self,
        snapshot: &DirectSnapshot,
        source: DirectId,
        destination_parent: DirectId,
        destination_name: &str,
        replace: bool,
    ) -> Result<DirectMutationStatus> {
        let (mutation, input) = self
            .prepare_namespace_mutation(
                snapshot,
                source,
                destination_parent,
                destination_name,
                replace,
                MUTATION_KIND_RENAME,
            )
            .await?;
        self.persist_staged_mutation(&mutation)?;
        let mutation = self.dispatch_destination(mutation, &input).await?;
        let mutation = if mutation.state == MUTATION_DESTINATION_APPLIED {
            self.cleanup_rename_source(mutation).await?
        } else {
            mutation
        };
        status_from_mutation(&mutation)
    }

    /// Observe the current endpoints of one retained mutation without dispatch.
    pub async fn observe_mutation(&self, id: DirectMutationId) -> Result<DirectMutationStatus> {
        let store = self.store.as_ref().expect("writable runtime has a store");
        let mut mutation = guard(store).mutation(id.0)?.ok_or_else(|| {
            Error::new(
                ErrorKind::NotFound,
                format!("unknown Direct mutation {}", id.to_hex()),
            )
        })?;
        if let Some(source) = &mutation.source {
            let key = DirectKey::from_storage_key(&source.key)?;
            mutation.source_observed = observation_to_record(&self.observe_key(&key).await);
        }
        if let Some(destination) = &mutation.destination {
            let key = DirectKey::from_storage_key(&destination.key)?;
            mutation.destination_observed = observation_to_record(&self.observe_key(&key).await);
        }
        guard(store).put_mutation(&mutation)?;
        status_from_mutation(&mutation)
    }

    /// Retry one mutation only when retained evidence proves it is safe.
    ///
    /// Content equality is never used as commit proof. A write/copy retry runs
    /// only while the original destination precondition still holds. A rename
    /// source cleanup runs only after the accepted destination pin is observed
    /// unchanged.
    pub async fn retry_mutation(&self, id: DirectMutationId) -> Result<DirectMutationStatus> {
        let store = self.store.as_ref().expect("writable runtime has a store");
        let mutation = guard(store).mutation(id.0)?.ok_or_else(|| {
            Error::new(
                ErrorKind::NotFound,
                format!("unknown Direct mutation {}", id.to_hex()),
            )
        })?;
        let mutation = match (mutation.kind, mutation_state(mutation.state)?) {
            (MUTATION_KIND_DELETE, DirectMutationState::Staged) => {
                self.dispatch_delete(mutation).await?
            }
            (MUTATION_KIND_DELETE, DirectMutationState::Unknown) => {
                self.retry_delete(mutation).await?
            }
            (
                MUTATION_KIND_PUBLISH | MUTATION_KIND_CREATE | MUTATION_KIND_COPY,
                DirectMutationState::Staged,
            ) => {
                let mutation = self.verify_mutation_input_or_fail(mutation)?;
                let input = guard(store).input_path(mutation.id);
                self.dispatch_destination(mutation, &input).await?
            }
            (
                MUTATION_KIND_PUBLISH | MUTATION_KIND_CREATE | MUTATION_KIND_COPY,
                DirectMutationState::Unknown,
            ) => self.retry_destination(mutation).await?,
            (
                MUTATION_KIND_RENAME,
                DirectMutationState::DestinationApplied | DirectMutationState::PendingSourceCleanup,
            ) => self.cleanup_rename_source(mutation).await?,
            (MUTATION_KIND_RENAME, DirectMutationState::Staged) => {
                let mutation = self.verify_mutation_input_or_fail(mutation)?;
                let input = guard(store).input_path(mutation.id);
                let mutation = self.dispatch_destination(mutation, &input).await?;
                if mutation.state == MUTATION_DESTINATION_APPLIED {
                    self.cleanup_rename_source(mutation).await?
                } else {
                    mutation
                }
            }
            (MUTATION_KIND_RENAME, DirectMutationState::Unknown) => {
                let mutation = self.retry_destination(mutation).await?;
                if mutation.state == MUTATION_DESTINATION_APPLIED {
                    self.cleanup_rename_source(mutation).await?
                } else {
                    mutation
                }
            }
            (_, state) => {
                return Err(Error::new(
                    ErrorKind::Unsupported,
                    format!("Direct mutation in state {state:?} cannot be retried"),
                ));
            }
        };
        self.apply_mutation_to_object(&mutation).await?;
        status_from_mutation(&mutation)
    }

    async fn retry_destination(&self, mut mutation: MutationRecord) -> Result<MutationRecord> {
        let store = self.store.as_ref().expect("writable runtime has a store");
        let destination = mutation
            .destination
            .as_ref()
            .expect("write mutation has a destination");
        let destination_key = DirectKey::from_storage_key(&destination.key)?;
        let observation = self.observe_key(&destination_key).await;
        mutation.destination_observed = observation_to_record(&observation);
        let precondition = precondition_from_record(&destination.precondition);
        if !observation_matches_precondition(&observation, &precondition) {
            guard(store).put_mutation(&mutation)?;
            return Ok(mutation);
        }
        let mutation = self.verify_mutation_input_or_fail(mutation)?;
        let input = guard(store).input_path(mutation.id);
        self.dispatch_destination(mutation, &input).await
    }

    async fn retry_delete(&self, mut mutation: MutationRecord) -> Result<MutationRecord> {
        let store = self.store.as_ref().expect("writable runtime has a store");
        let source = mutation
            .source
            .as_ref()
            .expect("delete mutation has a source");
        let source_key = DirectKey::from_storage_key(&source.key)?;
        let observation = self.observe_key(&source_key).await;
        mutation.source_observed = observation_to_record(&observation);
        if observation.is_missing() {
            mutation.state = MUTATION_APPLIED;
            mutation.error = None;
            guard(store).put_mutation(&mutation)?;
            return Ok(mutation);
        }
        let precondition = precondition_from_record(&source.precondition);
        if !observation_matches_precondition(&observation, &precondition) {
            mutation.state = MUTATION_CONFLICT;
            mutation.error = Some("Direct delete source changed; no delete was issued".to_owned());
            guard(store).put_mutation(&mutation)?;
            return Ok(mutation);
        }
        self.dispatch_delete(mutation).await
    }

    async fn dispatch_delete(&self, mut mutation: MutationRecord) -> Result<MutationRecord> {
        let store = self.store.as_ref().expect("writable runtime has a store");
        mutation.state = MUTATION_DISPATCHING;
        mutation.error = None;
        guard(store).put_mutation(&mutation)?;
        let source = mutation
            .source
            .as_ref()
            .expect("delete mutation has a source");
        let source_key = DirectKey::from_storage_key(&source.key)?;
        let outcome = self
            .mutation
            .delete(&source_key, precondition_from_record(&source.precondition))
            .await;
        match outcome {
            Ok(DirectMutationOutcome::Applied { .. }) => {
                mutation.state = MUTATION_APPLIED;
                mutation.source_observed = ObservationRecord::Missing;
            }
            Ok(DirectMutationOutcome::Conflict { observed }) => {
                mutation.state = MUTATION_CONFLICT;
                mutation.source_observed = observation_to_record(&observed);
                mutation.error = Some("Direct delete precondition changed".to_owned());
            }
            Ok(DirectMutationOutcome::Unknown { observed, message }) => {
                mutation.state = MUTATION_UNKNOWN;
                mutation.source_observed = observation_to_record(&observed);
                mutation.error = Some(message);
            }
            Ok(DirectMutationOutcome::Unsupported { message }) => {
                mutation.state = MUTATION_FAILED;
                mutation.error = Some(message);
            }
            Err(error) => {
                mutation.state = MUTATION_UNKNOWN;
                mutation.error = Some(error.to_string());
            }
        }
        guard(store).put_mutation(&mutation)?;
        Ok(mutation)
    }

    fn verify_mutation_input(&self, mutation: &MutationRecord) -> Result<()> {
        let Some(expected_digest) = mutation.input_digest else {
            return Ok(());
        };
        let store = self.store.as_ref().expect("writable runtime has a store");
        let path = guard(store).input_path(mutation.id);
        let mut file = File::open(&path)
            .map_err(|error| local_io("open retained Direct mutation input", error))?;
        let length = file
            .metadata()
            .map_err(|error| local_io("inspect retained Direct mutation input", error))?
            .len();
        let mut hasher = blake3::Hasher::new();
        let mut buffer = vec![0_u8; IO_CHUNK];
        loop {
            let read = file
                .read(&mut buffer)
                .map_err(|error| local_io("read retained Direct mutation input", error))?;
            if read == 0 {
                break;
            }
            hasher.update(&buffer[..read]);
        }
        if length != mutation.input_length || *hasher.finalize().as_bytes() != expected_digest {
            return Err(Error::new(
                ErrorKind::Storage,
                format!(
                    "retained Direct mutation input {} is missing or corrupt",
                    mutation
                        .id
                        .iter()
                        .map(|byte| format!("{byte:02x}"))
                        .collect::<String>()
                ),
            ));
        }
        Ok(())
    }

    fn verify_mutation_input_or_fail(
        &self,
        mut mutation: MutationRecord,
    ) -> Result<MutationRecord> {
        match self.verify_mutation_input(&mutation) {
            Ok(()) => Ok(mutation),
            Err(error) => {
                mutation.state = MUTATION_FAILED;
                mutation.error = Some(error.to_string());
                let store = self.store.as_ref().expect("writable runtime has a store");
                guard(store).put_mutation(&mutation)?;
                Err(error)
            }
        }
    }

    async fn apply_mutation_to_object(&self, mutation: &MutationRecord) -> Result<()> {
        let store = self.store.as_ref().expect("writable runtime has a store");
        let endpoint = match mutation.kind {
            MUTATION_KIND_PUBLISH | MUTATION_KIND_CREATE | MUTATION_KIND_COPY => {
                mutation.destination.as_ref()
            }
            MUTATION_KIND_DELETE => mutation.source.as_ref(),
            MUTATION_KIND_RENAME if mutation.state == MUTATION_APPLIED => mutation.source.as_ref(),
            _ => None,
        };
        let Some(endpoint) = endpoint else {
            return Ok(());
        };
        let entry = self
            .objects
            .lock()
            .await
            .values()
            .find(|entry| entry.identity.key.as_str() == endpoint.key)
            .cloned();
        let Some(entry) = entry else {
            return Ok(());
        };
        let mut inner = entry.inner.lock().await;
        let mutation_id = DirectMutationId::from_bytes(mutation.id);
        if inner.pending_mutation != Some(mutation_id) {
            return Ok(());
        }
        match mutation_state(mutation.state)? {
            DirectMutationState::Applied => {
                if mutation.kind == MUTATION_KIND_DELETE {
                    if inner.generation == inner.durable_generation {
                        let data = inner.data_path.take();
                        let id = entry.identity.id();
                        drop(inner);
                        guard(store).remove_object(*id.as_bytes())?;
                        if let Some(data) = data {
                            remove_if_exists(&data)?;
                        }
                        self.objects.lock().await.remove(&id);
                        return Ok(());
                    }
                    inner.state = DirectObjectState::Conflict;
                    inner.observed = observation_from_record(mutation.source_observed.clone());
                    inner.error =
                        Some("Direct name was unlinked while local bytes remain".to_owned());
                } else {
                    inner.accepted = mutation.accepted.clone().map(pin_from_record);
                    inner.observed = observation_from_record(mutation.destination_observed.clone());
                    inner.durable_generation = mutation.generation;
                    if let Some(etag) = inner
                        .accepted
                        .as_ref()
                        .and_then(DirectObjectPin::etag)
                        .map(str::to_owned)
                    {
                        inner.precondition = Some(DirectPrecondition::Etag(etag));
                        inner.state = DirectObjectState::Clean;
                        inner.pending_mutation = None;
                        inner.error = None;
                    }
                }
            }
            DirectMutationState::Conflict => {
                inner.state = DirectObjectState::Conflict;
                inner.observed =
                    observation_from_record(if mutation.kind == MUTATION_KIND_DELETE {
                        mutation.source_observed.clone()
                    } else {
                        mutation.destination_observed.clone()
                    });
                inner.error = mutation.error.clone();
            }
            DirectMutationState::Unknown => {
                inner.state = DirectObjectState::Unknown;
                inner.observed =
                    observation_from_record(if mutation.kind == MUTATION_KIND_DELETE {
                        mutation.source_observed.clone()
                    } else {
                        mutation.destination_observed.clone()
                    });
                inner.error = mutation.error.clone();
            }
            _ => {}
        }
        let object = record_from_entry(&entry, &inner);
        guard(store).put_object(&object)
    }

    async fn prepare_namespace_mutation(
        &self,
        snapshot: &DirectSnapshot,
        source: DirectId,
        destination_parent: DirectId,
        destination_name: &str,
        replace: bool,
        kind: u8,
    ) -> Result<(MutationRecord, PathBuf)> {
        if self.read_only {
            return Err(Error::new(
                ErrorKind::Unsupported,
                "Direct runtime is read-only",
            ));
        }
        let source_node = snapshot
            .node(source)
            .ok_or_else(|| Error::new(ErrorKind::NotFound, "source is not in this snapshot"))?;
        if !source_node.is_file() {
            return Err(Error::new(
                ErrorKind::IsDirectory,
                format!("{:?} is a directory", source_node.path()),
            ));
        }
        let destination_parent_node = snapshot.node(destination_parent).ok_or_else(|| {
            Error::new(
                ErrorKind::NotFound,
                "destination parent is not in this snapshot",
            )
        })?;
        if !destination_parent_node.is_directory() {
            return Err(Error::new(
                ErrorKind::NotDirectory,
                format!("{:?} is not a directory", destination_parent_node.path()),
            ));
        }
        let destination_path = if destination_parent_node.path().is_empty() {
            destination_name.to_owned()
        } else {
            format!("{}/{destination_name}", destination_parent_node.path())
        };
        if destination_path == source_node.path() {
            return Err(Error::new(
                ErrorKind::Invalid,
                "Direct source and destination are the same path",
            ));
        }
        let destination_key = DirectKey::from_path(&destination_path)?;
        let source_key =
            DirectKey::from_storage_key(source_node.key().expect("file node has key"))?;
        let existing_destination = snapshot.lookup(destination_parent, destination_name)?;
        if existing_destination.is_some_and(DirectNode::is_directory) {
            return Err(Error::new(
                ErrorKind::IsDirectory,
                "Direct file rename/copy destination is a directory",
            ));
        }
        if existing_destination.is_some() && !replace {
            return Err(Error::new(
                ErrorKind::Invalid,
                format!("Direct destination {destination_path:?} already exists"),
            ));
        }
        let store = self.store.as_ref().expect("writable runtime has a store");
        if let Some(record) = { guard(store).object(source_key.as_str())? }
            && (record.state != 0 || record.generation != record.durable_generation)
        {
            return Err(Error::new(
                ErrorKind::Unsupported,
                "Direct source has local edits; flush or discard them before rename/copy",
            ));
        }
        if { guard(store).object(destination_key.as_str())? }.is_some() {
            return Err(Error::new(
                ErrorKind::Unsupported,
                "Direct destination has retained local state",
            ));
        }

        let source_file = snapshot.open_file(source).await?;
        let source_etag = source_file.etag().ok_or_else(|| {
            Error::new(
                ErrorKind::Unsupported,
                "Direct source exposes no ETag for conditional read/delete",
            )
        })?;
        let destination_precondition = if let Some(node) = existing_destination {
            let destination_file = snapshot.open_file(node.id()).await?;
            DirectPrecondition::Etag(
                destination_file
                    .etag()
                    .ok_or_else(|| {
                        Error::new(
                            ErrorKind::Unsupported,
                            "Direct replacement destination exposes no ETag",
                        )
                    })?
                    .to_owned(),
            )
        } else {
            DirectPrecondition::Absent
        };
        let capability = self.mutation.capability();
        match destination_precondition {
            DirectPrecondition::Absent if !capability.create => {
                return Err(Error::new(
                    ErrorKind::Unsupported,
                    "storage does not advertise conditional creation",
                ));
            }
            DirectPrecondition::Etag(_) if !capability.replace => {
                return Err(Error::new(
                    ErrorKind::Unsupported,
                    "storage does not advertise conditional replacement",
                ));
            }
            _ => {}
        }
        if kind == MUTATION_KIND_RENAME && !capability.delete {
            return Err(Error::new(
                ErrorKind::Unsupported,
                "storage does not advertise conditional delete for safe-save rename",
            ));
        }
        let destination_id = existing_destination
            .map(DirectNode::id)
            .unwrap_or_else(|| derive_id(&destination_path, super::DirectKind::File, None));
        let mutation_id = DirectMutationId::new();
        let input_path = guard(store).input_path(mutation_id.0);
        let temporary = guard(store).temporary_path("inputs", mutation_id.0);
        remove_if_exists(&temporary)?;
        let (input_length, input_digest) =
            freeze_remote_file(&source_file, &temporary, &input_path).await?;
        if input_length != source_node.size() {
            return Err(Error::new(
                ErrorKind::Stale,
                format!(
                    "Direct source read {input_length} bytes, expected {}",
                    source_node.size()
                ),
            ));
        }
        let source_endpoint = EndpointRecord {
            id: *source_node.id().as_bytes(),
            key: source_key.as_str().to_owned(),
            path: source_node.path().to_owned(),
            parent: *source_node.parent().expect("file has a parent").as_bytes(),
            name: source_node.name().to_owned(),
            precondition: PreconditionRecord::Etag(source_etag.to_owned()),
        };
        let destination_endpoint = EndpointRecord {
            id: *destination_id.as_bytes(),
            key: destination_key.as_str().to_owned(),
            path: destination_path,
            parent: *destination_parent.as_bytes(),
            name: destination_name.to_owned(),
            precondition: precondition_to_record(&destination_precondition),
        };
        Ok((
            MutationRecord {
                id: mutation_id.0,
                kind,
                state: MUTATION_STAGED,
                generation: 0,
                source: Some(source_endpoint),
                destination: Some(destination_endpoint),
                input_length,
                input_digest: Some(input_digest),
                accepted: None,
                source_observed: ObservationRecord::Unavailable("not observed".to_owned()),
                destination_observed: ObservationRecord::Unavailable("not observed".to_owned()),
                error: None,
            },
            input_path,
        ))
    }

    fn persist_staged_mutation(&self, mutation: &MutationRecord) -> Result<()> {
        let store = self.store.as_ref().expect("writable runtime has a store");
        guard(store).put_mutation(mutation)
    }

    async fn dispatch_destination(
        &self,
        mut mutation: MutationRecord,
        input: &Path,
    ) -> Result<MutationRecord> {
        let store = self.store.as_ref().expect("writable runtime has a store");
        mutation.state = MUTATION_DISPATCHING;
        mutation.error = None;
        guard(store).put_mutation(&mutation)?;
        let destination = mutation
            .destination
            .as_ref()
            .expect("destination mutation has a destination");
        let destination_key = DirectKey::from_storage_key(&destination.key)?;
        let precondition = precondition_from_record(&destination.precondition);
        let outcome = self
            .mutation
            .publish_file(&destination_key, input, precondition)
            .await;
        match outcome {
            Ok(DirectMutationOutcome::Applied { accepted }) => {
                mutation.accepted = accepted.as_ref().map(pin_to_record);
                mutation.destination_observed = accepted
                    .as_ref()
                    .map(|pin| ObservationRecord::Present(pin_to_record(pin)))
                    .unwrap_or_else(|| {
                        ObservationRecord::Unavailable(
                            "successful response omitted metadata".to_owned(),
                        )
                    });
                mutation.state = if mutation.kind == MUTATION_KIND_RENAME {
                    MUTATION_DESTINATION_APPLIED
                } else {
                    MUTATION_APPLIED
                };
                if accepted.is_none()
                    || (accepted
                        .as_ref()
                        .is_some_and(|pin| pin.etag().is_none() && pin.version().is_none()))
                {
                    mutation.state = if mutation.kind == MUTATION_KIND_RENAME {
                        MUTATION_DESTINATION_APPLIED
                    } else {
                        MUTATION_APPLIED
                    };
                    mutation.error = Some(
                        "successful Direct write response omitted a usable identity".to_owned(),
                    );
                }
            }
            Ok(DirectMutationOutcome::Conflict { observed }) => {
                mutation.state = MUTATION_CONFLICT;
                mutation.destination_observed = observation_to_record(&observed);
                mutation.error = Some(
                    "storage destination precondition changed; source bytes are retained"
                        .to_owned(),
                );
            }
            Ok(DirectMutationOutcome::Unknown { observed, message }) => {
                mutation.state = MUTATION_UNKNOWN;
                mutation.destination_observed = observation_to_record(&observed);
                mutation.error = Some(message);
            }
            Ok(DirectMutationOutcome::Unsupported { message }) => {
                mutation.state = MUTATION_FAILED;
                mutation.error = Some(message);
            }
            Err(error) => {
                mutation.state = MUTATION_UNKNOWN;
                mutation.error = Some(error.to_string());
            }
        }
        guard(store).put_mutation(&mutation)?;
        Ok(mutation)
    }

    async fn cleanup_rename_source(&self, mut mutation: MutationRecord) -> Result<MutationRecord> {
        let store = self.store.as_ref().expect("writable runtime has a store");
        let accepted = match mutation.accepted.clone().map(pin_from_record) {
            Some(accepted) => accepted,
            None => {
                mutation.state = MUTATION_DESTINATION_APPLIED;
                mutation.error =
                    Some("Direct rename destination has no verifiable accepted pin".to_owned());
                guard(store).put_mutation(&mutation)?;
                return Ok(mutation);
            }
        };
        let destination = mutation
            .destination
            .as_ref()
            .expect("rename has a destination");
        let destination_key = DirectKey::from_storage_key(&destination.key)?;
        let destination_observation = self.observe_key(&destination_key).await;
        mutation.destination_observed = observation_to_record(&destination_observation);
        if !observation_matches_pin(&destination_observation, &accepted) {
            mutation.state = MUTATION_CONFLICT;
            mutation.error = Some(
                "Direct rename destination changed after publication; source was not deleted"
                    .to_owned(),
            );
            guard(store).put_mutation(&mutation)?;
            return Ok(mutation);
        }

        let source = mutation.source.as_ref().expect("rename has a source");
        let source_key = DirectKey::from_storage_key(&source.key)?;
        let source_observation = self.observe_key(&source_key).await;
        mutation.source_observed = observation_to_record(&source_observation);
        if source_observation.is_missing() {
            mutation.state = MUTATION_APPLIED;
            mutation.error = None;
            guard(store).put_mutation(&mutation)?;
            return Ok(mutation);
        }
        let source_precondition = precondition_from_record(&source.precondition);
        if !observation_matches_precondition(&source_observation, &source_precondition) {
            mutation.state = MUTATION_CONFLICT;
            mutation.error =
                Some("Direct rename source changed after destination publication".to_owned());
            guard(store).put_mutation(&mutation)?;
            return Ok(mutation);
        }

        mutation.state = MUTATION_SOURCE_DISPATCHING;
        guard(store).put_mutation(&mutation)?;
        let outcome = self.mutation.delete(&source_key, source_precondition).await;
        match outcome {
            Ok(DirectMutationOutcome::Applied { .. }) => {
                mutation.state = MUTATION_APPLIED;
                mutation.source_observed = ObservationRecord::Missing;
                mutation.error = None;
            }
            Ok(DirectMutationOutcome::Conflict { observed }) => {
                mutation.state = MUTATION_CONFLICT;
                mutation.source_observed = observation_to_record(&observed);
                mutation.error = Some(
                    "Direct rename source precondition changed; destination remains applied"
                        .to_owned(),
                );
            }
            Ok(DirectMutationOutcome::Unknown { observed, message }) => {
                mutation.state = MUTATION_PENDING_SOURCE_CLEANUP;
                mutation.source_observed = observation_to_record(&observed);
                mutation.error = Some(message);
            }
            Ok(DirectMutationOutcome::Unsupported { message }) => {
                mutation.state = MUTATION_FAILED;
                mutation.error = Some(message);
            }
            Err(error) => {
                mutation.state = MUTATION_PENDING_SOURCE_CLEANUP;
                mutation.error = Some(error.to_string());
            }
        }
        guard(store).put_mutation(&mutation)?;
        Ok(mutation)
    }

    async fn observe_key(&self, key: &DirectKey) -> DirectObservation {
        match self.namespace.operator.stat(key.as_str()).await {
            Ok(metadata) if metadata.mode() != EntryMode::DIR => {
                DirectObservation::Present(DirectObjectPin::from_parts(
                    pin_value(metadata.version()).map(str::to_owned),
                    pin_value(metadata.etag()).map(str::to_owned),
                    metadata.content_length(),
                    metadata
                        .last_modified()
                        .map(|time| super::normalize_time(SystemTime::from(time))),
                ))
            }
            Err(error) if error.kind() == opendal::ErrorKind::NotFound => {
                DirectObservation::Missing
            }
            Ok(_) => DirectObservation::Unavailable {
                message: "Direct key is a directory".to_owned(),
            },
            Err(error) => DirectObservation::Unavailable {
                message: error.to_string(),
            },
        }
    }

    async fn entry_for_node(
        &self,
        snapshot: &DirectSnapshot,
        node: &DirectNode,
        _writable: bool,
    ) -> Result<Arc<ObjectEntry>> {
        let key = DirectKey::from_storage_key(node.key().expect("file node has key"))?;
        if let Some(entry) = self.objects.lock().await.get(&node.id()).cloned() {
            return Ok(entry);
        }
        let candidates = self
            .objects
            .lock()
            .await
            .values()
            .filter(|entry| entry.identity.key == key)
            .cloned()
            .collect::<Vec<_>>();
        let mut clean = Vec::new();
        for entry in &candidates {
            let inner = entry.inner.lock().await;
            let state = inner.state;
            if state != DirectObjectState::Clean {
                return Ok(entry.clone());
            }
            if inner.file.is_some() && inner.accepted.is_some() {
                clean.push(entry.clone());
            }
        }
        drop(candidates);
        if let Some(store) = &self.store
            && let Some(record) = { guard(store).object(key.as_str())? }
            && record.state != 0
        {
            let entry = entry_from_record(store, record)?;
            return Ok(self
                .objects
                .lock()
                .await
                .entry(entry.identity.id())
                .or_insert_with(|| entry.clone())
                .clone());
        }

        let file = snapshot.open_file(node.id()).await?;
        let observed = DirectObservation::Present(pin_from_direct_file(&file));
        for entry in &clean {
            let mut inner = entry.inner.lock().await;
            let matches = inner
                .accepted
                .as_ref()
                .is_some_and(|accepted| observation_matches_pin(&observed, accepted));
            if !matches {
                continue;
            }
            let old_id = entry.identity.id();
            inner.remote = Some(file.clone());
            entry.identity.rebind_id(node.id());
            entry.identity.existed_remote.store(true, Ordering::SeqCst);
            if let Some(store) = &self.store {
                let record = record_from_entry(entry, &inner);
                guard(store).put_object(&record)?;
            }
            let mut objects = self.objects.lock().await;
            if old_id != node.id() {
                objects.remove(&old_id);
                objects.insert(node.id(), entry.clone());
            }
            return Ok(entry.clone());
        }
        {
            let mut objects = self.objects.lock().await;
            for entry in &clean {
                if Arc::strong_count(entry) == 2 {
                    objects.remove(&entry.identity.id());
                }
            }
        }
        let precondition = file
            .etag()
            .map(|etag| DirectPrecondition::Etag(etag.to_owned()));
        let identity = ObjectIdentity {
            id: RwLock::new(node.id()),
            key: key.clone(),
            path: node.path().to_owned(),
            parent: node.parent().ok_or_else(|| {
                Error::new(ErrorKind::Invalid, "file node does not have a parent")
            })?,
            name: node.name().to_owned(),
            existed_remote: AtomicBool::new(true),
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
        if !self.mutation.capability().replace {
            return Err(Error::new(
                ErrorKind::Unsupported,
                "storage does not advertise conditional replacement",
            ));
        }
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

        let id = *entry.identity.id().as_bytes();
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
        if inner.state == DirectObjectState::Discarded {
            return Ok(DirectFlushOutcome::Unsupported {
                message: "Direct local name was discarded".to_owned(),
            });
        }
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
            let mutation = inner.pending_mutation.unwrap_or_else(DirectMutationId::new);
            return Ok(flush_outcome_from_entry(entry, &inner, mutation));
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
        match precondition {
            DirectPrecondition::Absent if !self.mutation.capability().create => {
                return Ok(DirectFlushOutcome::Unsupported {
                    message: "storage does not advertise conditional creation".to_owned(),
                });
            }
            DirectPrecondition::Etag(_) if !self.mutation.capability().replace => {
                return Ok(DirectFlushOutcome::Unsupported {
                    message: "storage does not advertise conditional replacement".to_owned(),
                });
            }
            _ => {}
        }
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
        let kind = if entry.identity.existed_remote.load(Ordering::SeqCst) {
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
            source_observed: ObservationRecord::Unavailable("not observed".to_owned()),
            destination_observed: ObservationRecord::Unavailable("not observed".to_owned()),
            error: None,
        };
        inner.state = DirectObjectState::Staged;
        inner.pending_mutation = Some(mutation_id);
        inner.error = None;
        let mut object = record_from_entry(entry, &inner);
        mutation.state = MUTATION_DISPATCHING;
        object.state = OBJECT_PENDING;
        guard(store).put_records(
            std::slice::from_ref(&object),
            std::slice::from_ref(&mutation),
        )?;
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
                entry.identity.existed_remote.store(true, Ordering::SeqCst);
                inner.accepted = accepted.clone();
                inner.observed = DirectObservation::Unavailable {
                    message: "not observed".to_owned(),
                };
                inner.durable_generation = inner.generation;
                if let Some(etag) = accepted.as_ref().and_then(DirectObjectPin::etag) {
                    inner.precondition = Some(DirectPrecondition::Etag(etag.to_owned()));
                    inner.state = DirectObjectState::Clean;
                    inner.pending_mutation = None;
                    inner.error = None;
                } else {
                    inner.state = DirectObjectState::Failed;
                    inner.pending_mutation = Some(mutation_id);
                    inner.error =
                        Some("successful Direct write response omitted an ETag".to_owned());
                }
                let object = record_from_entry(entry, &inner);
                guard(store).put_records(&[object], &[mutation])?;
                Ok(flush_outcome_from_entry(entry, &inner, mutation_id))
            }
            Ok(DirectMutationOutcome::Conflict { observed }) => {
                mutation.state = MUTATION_CONFLICT;
                mutation.destination_observed = observation_to_record(&observed);
                mutation.error =
                    Some("storage precondition changed; local bytes are retained".to_owned());
                inner.state = DirectObjectState::Conflict;
                inner.observed = observed;
                inner.error = mutation.error.clone();
                let object = record_from_entry(entry, &inner);
                guard(store).put_records(&[object], &[mutation])?;
                Ok(flush_outcome_from_entry(entry, &inner, mutation_id))
            }
            Ok(DirectMutationOutcome::Unknown { observed, message }) => {
                mutation.state = MUTATION_UNKNOWN;
                mutation.destination_observed = observation_to_record(&observed);
                mutation.error = Some(message.clone());
                inner.state = DirectObjectState::Unknown;
                inner.observed = observed;
                inner.error = Some(message);
                let object = record_from_entry(entry, &inner);
                guard(store).put_records(&[object], &[mutation])?;
                Ok(flush_outcome_from_entry(entry, &inner, mutation_id))
            }
            Ok(DirectMutationOutcome::Unsupported { message }) => {
                mutation.state = MUTATION_FAILED;
                mutation.error = Some(message.clone());
                inner.state = DirectObjectState::Failed;
                inner.error = Some(message);
                let object = record_from_entry(entry, &inner);
                guard(store).put_records(&[object], &[mutation])?;
                Ok(flush_outcome_from_entry(entry, &inner, mutation_id))
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
                Ok(flush_outcome_from_entry(entry, &inner, mutation_id))
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
        self.entry.identity.id()
    }

    pub fn object_id(&self) -> DirectObjectId {
        self.entry.object_id
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
        self.entry.identity.existed_remote.load(Ordering::SeqCst)
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
        if bytes.is_empty() {
            return Ok(());
        }
        let mut inner = self.entry.inner.lock().await;
        if inner.state == DirectObjectState::Discarded {
            return Err(Error::new(
                ErrorKind::Unsupported,
                "Direct local name was discarded",
            ));
        }
        {
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
            file.sync_data()
                .map_err(|error| local_io("sync staged Direct object", error))?;
        }
        inner.length = inner.length.max(offset.saturating_add(bytes.len() as u64));
        bump_generation(&mut inner);
        self.persist_object(&mut inner)?;
        Ok(())
    }

    pub async fn append(&self, bytes: &[u8]) -> Result<u64> {
        self.require_writable()?;
        if bytes.is_empty() {
            return self.len().await;
        }
        let mut inner = self.entry.inner.lock().await;
        if inner.state == DirectObjectState::Discarded {
            return Err(Error::new(
                ErrorKind::Unsupported,
                "Direct local name was discarded",
            ));
        }
        let offset = inner.length;
        {
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
            file.sync_data()
                .map_err(|error| local_io("sync staged Direct object", error))?;
        }
        inner.length = inner.length.saturating_add(bytes.len() as u64);
        bump_generation(&mut inner);
        self.persist_object(&mut inner)?;
        Ok(offset)
    }

    pub async fn truncate(&self, length: u64) -> Result<()> {
        self.require_writable()?;
        let mut inner = self.entry.inner.lock().await;
        if inner.state == DirectObjectState::Discarded {
            return Err(Error::new(
                ErrorKind::Unsupported,
                "Direct local name was discarded",
            ));
        }
        if inner.length != length {
            let file = inner.file.as_mut().ok_or_else(|| {
                Error::new(
                    ErrorKind::Invalid,
                    "writable Direct handle is not materialized",
                )
            })?;
            file.set_len(length)
                .map_err(|error| local_io("truncate staged Direct object", error))?;
            file.sync_data()
                .map_err(|error| local_io("sync staged Direct object", error))?;
            inner.length = length;
            bump_generation(&mut inner);
            self.persist_object(&mut inner)?;
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

    fn persist_object(&self, inner: &mut ObjectInner) -> Result<()> {
        let store = self
            .runtime
            .store
            .as_ref()
            .expect("writable runtime has a store");
        let record = record_from_entry(&self.entry, inner);
        if let Err(error) = guard(store).put_object(&record) {
            inner.error = Some(error.to_string());
            return Err(error);
        }
        Ok(())
    }
}

fn bump_generation(inner: &mut ObjectInner) {
    inner.generation = inner.generation.saturating_add(1);
    inner.digest = None;
    if inner.state == DirectObjectState::Clean {
        inner.state = DirectObjectState::Staged;
    }
    inner.error = None;
}

fn entry_from_record(store: &Arc<Mutex<Store>>, record: ObjectRecord) -> Result<Arc<ObjectEntry>> {
    let data_path = guard(store).data_path(record.id);
    let file = OpenOptions::new()
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
        id: RwLock::new(record.direct_id()),
        key: DirectKey::from_storage_key(&record.key)?,
        path: record.path,
        parent: DirectId::from_bytes(record.parent),
        name: record.name,
        existed_remote: atomic_bool(record.existed_remote),
    };
    let inner = ObjectInner {
        remote: None,
        file: Some(file),
        data_path: Some(data_path),
        state: object_state(record.state)?,
        length: record.length,
        generation: record.generation,
        durable_generation: record.durable_generation,
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
        id: *entry.identity.id().as_bytes(),
        key: entry.identity.key.as_str().to_owned(),
        path: entry.identity.path.clone(),
        parent: *entry.identity.parent.as_bytes(),
        name: entry.identity.name.clone(),
        existed_remote: entry.identity.existed_remote.load(Ordering::SeqCst),
        precondition: precondition_to_record(
            inner
                .precondition
                .as_ref()
                .expect("durable Direct objects have a precondition"),
        ),
        state: object_state_code(inner.state),
        generation: inner.generation,
        durable_generation: inner.durable_generation,
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
    status_from_entry_locked(entry, &inner)
}

fn status_from_entry_locked(entry: &ObjectEntry, inner: &ObjectInner) -> DirectObjectStatus {
    DirectObjectStatus {
        id: entry.identity.id(),
        key: entry.identity.key.clone(),
        path: entry.identity.path.clone(),
        parent: entry.identity.parent,
        name: entry.identity.name.clone(),
        length: inner.length,
        state: inner.state,
        existed_remote: entry.identity.existed_remote.load(Ordering::SeqCst),
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
        source_observed: observation_from_record(record.source_observed.clone()),
        destination_observed: observation_from_record(record.destination_observed.clone()),
        error: record.error.clone(),
    })
}

fn endpoint_from_identity(
    identity: &ObjectIdentity,
    precondition: DirectPrecondition,
) -> EndpointRecord {
    EndpointRecord {
        id: *identity.id().as_bytes(),
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

fn flush_outcome_from_entry(
    entry: &ObjectEntry,
    inner: &ObjectInner,
    mutation: DirectMutationId,
) -> DirectFlushOutcome {
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
        DirectObjectState::Clean => DirectFlushOutcome::Unchanged,
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
        DirectObjectState::Discarded => DirectFlushOutcome::Unsupported {
            message: "Direct local name was discarded".to_owned(),
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
        OBJECT_DISCARDED => Ok(DirectObjectState::Discarded),
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
        DirectObjectState::Discarded => OBJECT_DISCARDED,
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
    let modified = value.modified_time();
    DirectObjectPin::from_parts(value.version, value.etag, value.size, modified)
}

fn pin_from_direct_file(file: &DirectFile) -> DirectObjectPin {
    DirectObjectPin::from_parts(
        file.version().map(str::to_owned),
        file.etag().map(str::to_owned),
        file.length(),
        file.modified(),
    )
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

fn atomic_bool(value: bool) -> AtomicBool {
    AtomicBool::new(value)
}

async fn freeze_remote_file(
    source: &DirectFile,
    temporary: &Path,
    target: &Path,
) -> Result<(u64, [u8; 32])> {
    let mut output = create_private_file(temporary)?;
    let mut hasher = blake3::Hasher::new();
    let mut offset = 0_u64;
    while offset < source.length() {
        let bytes = source
            .read(offset, IO_CHUNK.min((source.length() - offset) as usize))
            .await?;
        if bytes.is_empty() {
            return Err(Error::new(
                ErrorKind::Stale,
                "pinned Direct source ended during mutation preparation",
            ));
        }
        output
            .write_all(&bytes)
            .map_err(|error| local_io("freeze Direct mutation input", error))?;
        hasher.update(&bytes);
        offset += bytes.len() as u64;
    }
    output
        .sync_all()
        .map_err(|error| local_io("sync Direct mutation input", error))?;
    std::fs::rename(temporary, target)
        .map_err(|error| local_io("publish Direct mutation input", error))?;
    sync_parent(target)?;
    Ok((offset, *hasher.finalize().as_bytes()))
}

fn observation_matches_precondition(
    observation: &DirectObservation,
    precondition: &DirectPrecondition,
) -> bool {
    match (observation, precondition) {
        (DirectObservation::Missing, DirectPrecondition::Absent) => true,
        (DirectObservation::Present(observed), DirectPrecondition::Etag(expected)) => {
            observed.etag() == Some(expected.as_str())
        }
        _ => false,
    }
}

fn observation_matches_pin(observation: &DirectObservation, accepted: &DirectObjectPin) -> bool {
    let DirectObservation::Present(observed) = observation else {
        return false;
    };
    if let Some(version) = accepted.version() {
        return observed.version() == Some(version)
            && accepted
                .etag()
                .is_none_or(|etag| observed.etag() == Some(etag))
            && accepted.size() == observed.size();
    }
    accepted
        .etag()
        .is_some_and(|etag| observed.etag() == Some(etag))
        && accepted.size() == observed.size()
        && match (accepted.modified(), observed.modified()) {
            (Some(left), Some(right)) => left == right,
            _ => true,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::support::TestBackend;

    fn binding(bucket: &str) -> super::super::DirectStorageBinding {
        super::super::DirectStorageBinding::new(
            "s3",
            "minio",
            bucket,
            "/direct",
            "http://127.0.0.1:9000",
        )
        .unwrap()
    }

    fn namespace(operator: &opendal::Operator) -> DirectNamespace {
        DirectNamespace::unchecked(operator.clone())
    }

    fn snapshot(operator: &opendal::Operator, backend: &TestBackend) -> DirectSnapshot {
        let records = backend
            .state
            .lock()
            .unwrap()
            .objects
            .iter()
            .map(|(key, object)| super::super::ObjectRecord {
                key: key.clone(),
                size: object.bytes.len() as u64,
                etag: Some(object.etag.clone()),
                version: None,
                directory: false,
                modified: None,
            })
            .collect();
        DirectSnapshot::build(namespace(operator), records).unwrap()
    }

    fn empty_snapshot(operator: &opendal::Operator) -> DirectSnapshot {
        DirectSnapshot::build(namespace(operator), Vec::new()).unwrap()
    }

    #[tokio::test]
    async fn staged_write_survives_restart_without_publication() {
        let backend = TestBackend::default();
        let operator = backend.operator();
        let state = tempfile::tempdir().unwrap();
        let first = DirectRuntime::open(
            namespace(&operator),
            Some(state.path().to_owned()),
            false,
            binding("bucket"),
        )
        .await
        .unwrap();
        let snapshot = empty_snapshot(&operator);
        let file = first
            .create_file(&snapshot, snapshot.root(), "local")
            .await
            .unwrap();
        file.write(0, b"local").await.unwrap();
        let id = file.id();
        drop(file);
        drop(first);

        let reopened = DirectRuntime::open(
            namespace(&operator),
            Some(state.path().to_owned()),
            false,
            binding("bucket"),
        )
        .await
        .unwrap();
        let pending = reopened.pending_objects().await.unwrap();
        let status = pending
            .iter()
            .find(|status| status.id == id)
            .expect("local generation was not retained");
        assert_eq!(status.length, 5);
        assert_eq!(status.state, DirectObjectState::Staged);
        assert!(!status.existed_remote);
        let file = reopened.open_pending(id, false).await.unwrap();
        assert_eq!(file.read(0, 5).await.unwrap(), b"local");
        assert!(operator.stat("local").await.is_err());
    }

    #[tokio::test]
    async fn create_flush_uses_absent_condition_and_returns_response_pin() {
        let backend = TestBackend::default();
        let operator = backend.operator();
        let state = tempfile::tempdir().unwrap();
        let runtime = DirectRuntime::open(
            namespace(&operator),
            Some(state.path().to_owned()),
            false,
            binding("bucket"),
        )
        .await
        .unwrap();
        let snapshot = empty_snapshot(&operator);
        let file = runtime
            .create_file(&snapshot, snapshot.root(), "created")
            .await
            .unwrap();
        file.write(0, b"bytes").await.unwrap();
        let DirectFlushOutcome::Applied {
            accepted: Some(accepted),
            ..
        } = file.flush().await.unwrap()
        else {
            panic!("conditional create did not apply");
        };
        assert!(accepted.etag().is_some());
        assert_eq!(operator.read("created").await.unwrap().to_vec(), b"bytes");
        let status = file.status().await.unwrap();
        assert_eq!(status.state, DirectObjectState::Clean);
        assert!(status.existed_remote);
        assert!(status.pending_mutation.is_none());
    }

    #[tokio::test]
    async fn flush_conflict_retains_local_bytes_after_restart() {
        let backend = TestBackend::default();
        let operator = backend.operator();
        operator.write("file", b"remote".to_vec()).await.unwrap();
        let snapshot = snapshot(&operator, &backend);
        let file_id = snapshot.resolve("file").unwrap().id();
        let state = tempfile::tempdir().unwrap();
        let runtime = DirectRuntime::open(
            namespace(&operator),
            Some(state.path().to_owned()),
            false,
            binding("bucket"),
        )
        .await
        .unwrap();
        let file = runtime.open_file(&snapshot, file_id, true).await.unwrap();
        file.truncate(0).await.unwrap();
        file.write(0, b"local").await.unwrap();
        operator.write("file", b"external".to_vec()).await.unwrap();
        let outcome = file.flush().await.unwrap();
        assert!(matches!(
            outcome,
            DirectFlushOutcome::Conflict {
                observed: DirectObservation::Present(_),
                ..
            }
        ));
        assert_eq!(file.read(0, 5).await.unwrap(), b"local");
        let id = file.id();
        drop(file);
        drop(runtime);

        let reopened = DirectRuntime::open(
            namespace(&operator),
            Some(state.path().to_owned()),
            false,
            binding("bucket"),
        )
        .await
        .unwrap();
        let pending = reopened.pending_objects().await.unwrap();
        let status = pending
            .iter()
            .find(|status| status.id == id)
            .expect("conflicted generation was not retained");
        assert_eq!(status.state, DirectObjectState::Conflict);
        assert_eq!(
            reopened
                .open_pending(id, false)
                .await
                .unwrap()
                .read(0, 5)
                .await
                .unwrap(),
            b"local"
        );
        assert_eq!(operator.read("file").await.unwrap().to_vec(), b"external");
    }

    #[tokio::test]
    async fn keep_local_resolution_rebases_to_the_observed_etag() {
        let backend = TestBackend::default();
        let operator = backend.operator();
        operator.write("file", b"remote".to_vec()).await.unwrap();
        let snapshot = snapshot(&operator, &backend);
        let file_id = snapshot.resolve("file").unwrap().id();
        let state = tempfile::tempdir().unwrap();
        let runtime = DirectRuntime::open(
            namespace(&operator),
            Some(state.path().to_owned()),
            false,
            binding("bucket"),
        )
        .await
        .unwrap();
        let file = runtime.open_file(&snapshot, file_id, true).await.unwrap();
        file.truncate(0).await.unwrap();
        file.write(0, b"local").await.unwrap();
        operator.write("file", b"external".to_vec()).await.unwrap();
        assert!(matches!(
            file.flush().await.unwrap(),
            DirectFlushOutcome::Conflict { .. }
        ));

        let status = runtime
            .resolve_object(file.id(), DirectConflictResolution::KeepLocal)
            .await
            .unwrap();
        assert_eq!(status.state, DirectObjectState::Staged);
        assert!(status.pending_mutation.is_none());
        assert!(matches!(
            file.flush().await.unwrap(),
            DirectFlushOutcome::Applied { .. }
        ));
        assert_eq!(operator.read("file").await.unwrap().to_vec(), b"local");
    }

    #[tokio::test]
    async fn lost_response_is_unknown_across_restart() {
        let backend = TestBackend::default();
        let operator = backend.operator();
        operator.write("file", b"remote".to_vec()).await.unwrap();
        let snapshot = snapshot(&operator, &backend);
        let file_id = snapshot.resolve("file").unwrap().id();
        let state = tempfile::tempdir().unwrap();
        let runtime = DirectRuntime::open(
            namespace(&operator),
            Some(state.path().to_owned()),
            false,
            binding("bucket"),
        )
        .await
        .unwrap();
        let file = runtime.open_file(&snapshot, file_id, true).await.unwrap();
        file.write(0, b"local").await.unwrap();
        backend.lose_next_write_response();
        let mutation = match file.flush().await.unwrap() {
            DirectFlushOutcome::Unknown { mutation, .. } => mutation,
            outcome => panic!("lost response was not Unknown: {outcome:?}"),
        };
        drop(file);
        drop(runtime);

        let reopened = DirectRuntime::open(
            namespace(&operator),
            Some(state.path().to_owned()),
            false,
            binding("bucket"),
        )
        .await
        .unwrap();
        assert_eq!(
            reopened.mutation_status(mutation).unwrap().state,
            DirectMutationState::Unknown
        );
        let pending = reopened.pending_objects().await.unwrap();
        assert!(
            pending
                .iter()
                .any(|status| status.state == DirectObjectState::Unknown),
            "restart did not preserve the Unknown local generation"
        );
    }

    #[tokio::test]
    async fn safe_retry_dispatches_only_after_precondition_is_reobserved() {
        let backend = TestBackend::default();
        let operator = backend.operator();
        operator.write("file", b"remote".to_vec()).await.unwrap();
        let snapshot = snapshot(&operator, &backend);
        let file_id = snapshot.resolve("file").unwrap().id();
        let state = tempfile::tempdir().unwrap();
        let runtime = DirectRuntime::open(
            namespace(&operator),
            Some(state.path().to_owned()),
            false,
            binding("bucket"),
        )
        .await
        .unwrap();
        let file = runtime.open_file(&snapshot, file_id, true).await.unwrap();
        file.truncate(0).await.unwrap();
        file.write(0, b"local").await.unwrap();
        backend.fail_next_write_before_effect();
        let mutation = match file.flush().await.unwrap() {
            DirectFlushOutcome::Unknown { mutation, .. } => mutation,
            outcome => panic!("failed request was not Unknown: {outcome:?}"),
        };
        let status = runtime.retry_mutation(mutation).await.unwrap();
        assert_eq!(status.state, DirectMutationState::Applied);
        assert_eq!(operator.read("file").await.unwrap().to_vec(), b"local");
    }

    #[tokio::test]
    async fn fresh_snapshot_does_not_reuse_clean_old_pin() {
        let backend = TestBackend::default();
        let operator = backend.operator();
        operator.write("file", b"v1".to_vec()).await.unwrap();
        let first_snapshot = snapshot(&operator, &backend);
        let first_id = first_snapshot.resolve("file").unwrap().id();
        let runtime = DirectRuntime::open(namespace(&operator), None, true, binding("bucket"))
            .await
            .unwrap();
        let old = runtime
            .open_file(&first_snapshot, first_id, false)
            .await
            .unwrap();
        operator.write("file", b"v2".to_vec()).await.unwrap();
        let second_snapshot = snapshot(&operator, &backend);
        let second_id = second_snapshot.resolve("file").unwrap().id();
        assert_ne!(first_id, second_id);
        let new = runtime
            .open_file(&second_snapshot, second_id, false)
            .await
            .unwrap();
        assert_ne!(old.object_id(), new.object_id());
        assert_eq!(new.read(0, 2).await.unwrap(), b"v2");
        assert_eq!(old.read(0, 2).await.unwrap_err().kind(), ErrorKind::Stale);
    }

    #[tokio::test]
    async fn shared_reader_tracks_the_current_local_generation() {
        let backend = TestBackend::default();
        let operator = backend.operator();
        operator.write("file", b"remote".to_vec()).await.unwrap();
        let snapshot = snapshot(&operator, &backend);
        let file_id = snapshot.resolve("file").unwrap().id();
        let state = tempfile::tempdir().unwrap();
        let runtime = DirectRuntime::open(
            namespace(&operator),
            Some(state.path().to_owned()),
            false,
            binding("bucket"),
        )
        .await
        .unwrap();
        let reader = runtime.open_file(&snapshot, file_id, false).await.unwrap();
        let writer = runtime.open_file(&snapshot, file_id, true).await.unwrap();
        assert_eq!(reader.object_id(), writer.object_id());
        writer.truncate(0).await.unwrap();
        writer.write(0, b"local").await.unwrap();
        assert_eq!(reader.read(0, 5).await.unwrap(), b"local");
        assert!(matches!(
            writer.flush().await.unwrap(),
            DirectFlushOutcome::Applied { .. }
        ));
        assert_eq!(reader.read(0, 5).await.unwrap(), b"local");
    }

    #[tokio::test]
    async fn accepted_flush_rebinds_clean_entry_to_new_snapshot_pin() {
        let backend = TestBackend::default();
        let operator = backend.operator();
        operator.write("file", b"remote".to_vec()).await.unwrap();
        let first_snapshot = snapshot(&operator, &backend);
        let first_id = first_snapshot.resolve("file").unwrap().id();
        let state = tempfile::tempdir().unwrap();
        let runtime = DirectRuntime::open(
            namespace(&operator),
            Some(state.path().to_owned()),
            false,
            binding("bucket"),
        )
        .await
        .unwrap();
        let writer = runtime
            .open_file(&first_snapshot, first_id, true)
            .await
            .unwrap();
        writer.truncate(0).await.unwrap();
        writer.write(0, b"one").await.unwrap();
        assert!(matches!(
            writer.flush().await.unwrap(),
            DirectFlushOutcome::Applied { .. }
        ));

        let second_snapshot = snapshot(&operator, &backend);
        let second_id = second_snapshot.resolve("file").unwrap().id();
        assert_ne!(first_id, second_id);
        let reader = runtime
            .open_file(&second_snapshot, second_id, false)
            .await
            .unwrap();
        assert_eq!(reader.id(), second_id);
        assert_eq!(reader.object_id(), writer.object_id());
        writer.append(b"!").await.unwrap();
        assert_eq!(reader.read(0, 4).await.unwrap(), b"one!");
    }

    #[tokio::test]
    async fn storage_binding_mismatch_fails_before_runtime_state_creation() {
        let backend = TestBackend::default();
        let operator = backend.operator();
        let state = tempfile::tempdir().unwrap();
        let runtime = DirectRuntime::open(
            namespace(&operator),
            Some(state.path().to_owned()),
            false,
            binding("bucket"),
        )
        .await
        .unwrap();
        drop(runtime);
        for wrong in [
            binding("other-bucket"),
            super::super::DirectStorageBinding::new(
                "s3",
                "amazon-s3",
                "bucket",
                "/direct",
                "http://127.0.0.1:9000",
            )
            .unwrap(),
            super::super::DirectStorageBinding::new(
                "s3",
                "minio",
                "bucket",
                "/other",
                "http://127.0.0.1:9000",
            )
            .unwrap(),
            super::super::DirectStorageBinding::new(
                "s3",
                "minio",
                "bucket",
                "/direct",
                "http://127.0.0.1:9001",
            )
            .unwrap(),
        ] {
            let error = match DirectRuntime::open(
                namespace(&operator),
                Some(state.path().to_owned()),
                false,
                wrong,
            )
            .await
            {
                Ok(_) => panic!("wrong Direct storage binding was accepted"),
                Err(error) => error,
            };
            assert_eq!(error.kind(), ErrorKind::Invalid);
        }
    }

    #[tokio::test]
    async fn partial_existing_database_is_rejected_without_runtime_files() {
        let backend = TestBackend::default();
        let operator = backend.operator();
        let state = tempfile::tempdir().unwrap();
        let database = state.path().join("runtime.db");
        let connection = rusqlite::Connection::open(&database).unwrap();
        connection
            .execute_batch("CREATE TABLE foreign_state(value INTEGER);")
            .unwrap();
        drop(connection);

        let error = match DirectRuntime::open(
            namespace(&operator),
            Some(state.path().to_owned()),
            false,
            binding("bucket"),
        )
        .await
        {
            Ok(_) => panic!("partial Direct runtime database was accepted"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), ErrorKind::Storage);
        assert!(!state.path().join("runtime.lock").exists());
        assert!(!state.path().join("objects").exists());
        assert!(!state.path().join("inputs").exists());
    }

    #[tokio::test]
    async fn safe_save_rename_never_deletes_source_after_destination_changes() {
        let backend = TestBackend::default();
        let operator = backend.operator();
        operator.write("source", b"source".to_vec()).await.unwrap();
        let first_snapshot = snapshot(&operator, &backend);
        let source_id = first_snapshot.resolve("source").unwrap().id();
        let state = tempfile::tempdir().unwrap();
        let runtime = DirectRuntime::open(
            namespace(&operator),
            Some(state.path().to_owned()),
            false,
            binding("bucket"),
        )
        .await
        .unwrap();
        backend.fail_next_delete_before_effect();
        let status = runtime
            .rename_file(
                &first_snapshot,
                source_id,
                first_snapshot.root(),
                "destination",
                false,
            )
            .await
            .unwrap();
        assert_eq!(status.state, DirectMutationState::PendingSourceCleanup);
        assert_eq!(operator.read("source").await.unwrap().to_vec(), b"source");
        assert_eq!(
            operator.read("destination").await.unwrap().to_vec(),
            b"source"
        );

        operator
            .write("destination", b"external".to_vec())
            .await
            .unwrap();
        let status = runtime.retry_mutation(status.id).await.unwrap();
        assert_eq!(status.state, DirectMutationState::Conflict);
        assert_eq!(
            operator.read("destination").await.unwrap().to_vec(),
            b"external"
        );
        assert_eq!(operator.read("source").await.unwrap().to_vec(), b"source");
    }

    #[tokio::test]
    async fn unlink_with_dirty_handle_retains_bytes_and_prevents_resurrection() {
        let backend = TestBackend::default();
        let operator = backend.operator();
        operator.write("file", b"remote".to_vec()).await.unwrap();
        let snapshot = snapshot(&operator, &backend);
        let file_id = snapshot.resolve("file").unwrap().id();
        let state = tempfile::tempdir().unwrap();
        let runtime = DirectRuntime::open(
            namespace(&operator),
            Some(state.path().to_owned()),
            false,
            binding("bucket"),
        )
        .await
        .unwrap();
        let file = runtime.open_file(&snapshot, file_id, true).await.unwrap();
        file.write(0, b"local").await.unwrap();
        let status = runtime.unlink_file(&snapshot, file_id).await.unwrap();
        assert_eq!(status.state, DirectMutationState::Applied);
        assert!(operator.stat("file").await.is_err());
        assert!(matches!(
            file.flush().await.unwrap(),
            DirectFlushOutcome::Conflict {
                observed: DirectObservation::Missing,
                ..
            }
        ));
        assert!(operator.stat("file").await.is_err());
        assert_eq!(file.read(0, 5).await.unwrap(), b"local");
    }

    #[tokio::test]
    async fn conditional_create_conflict_keeps_same_handle_readable() {
        let backend = TestBackend::default();
        let operator = backend.operator();
        let state = tempfile::tempdir().unwrap();
        let runtime = DirectRuntime::open(
            namespace(&operator),
            Some(state.path().to_owned()),
            false,
            binding("bucket"),
        )
        .await
        .unwrap();
        let snapshot = empty_snapshot(&operator);
        let file = runtime
            .create_file(&snapshot, snapshot.root(), "created")
            .await
            .unwrap();
        file.write(0, b"local").await.unwrap();
        operator
            .write("created", b"external".to_vec())
            .await
            .unwrap();
        assert!(matches!(
            file.flush().await.unwrap(),
            DirectFlushOutcome::Conflict {
                observed: DirectObservation::Present(_),
                ..
            }
        ));
        assert_eq!(file.read(0, 5).await.unwrap(), b"local");
        assert_eq!(
            operator.read("created").await.unwrap().to_vec(),
            b"external"
        );
    }

    #[tokio::test]
    async fn local_only_create_has_explicit_discard() {
        let backend = TestBackend::default();
        let operator = backend.operator();
        let state = tempfile::tempdir().unwrap();
        let runtime = DirectRuntime::open(
            namespace(&operator),
            Some(state.path().to_owned()),
            false,
            binding("bucket"),
        )
        .await
        .unwrap();
        let snapshot = empty_snapshot(&operator);
        let file = runtime
            .create_file(&snapshot, snapshot.root(), "local")
            .await
            .unwrap();
        let id = file.id();
        file.write(0, b"bytes").await.unwrap();
        runtime.discard_local(id).await.unwrap();
        assert!(operator.stat("local").await.is_err());
        assert!(matches!(
            file.flush().await.unwrap(),
            DirectFlushOutcome::Unsupported { .. }
        ));
        assert!(
            runtime
                .pending_objects()
                .await
                .unwrap()
                .iter()
                .all(|status| status.id != id)
        );
    }

    #[tokio::test]
    async fn inspection_is_read_only_and_checks_binding() {
        let backend = TestBackend::default();
        let operator = backend.operator();
        let root = tempfile::tempdir().unwrap();
        let missing = root.path().join("missing");
        let inspection = DirectRuntime::inspect(&missing, None).unwrap();
        assert_eq!(inspection.state, DirectRuntimeInspectionState::Missing);
        assert!(!missing.exists());

        let empty = root.path().join("empty");
        std::fs::create_dir(&empty).unwrap();
        let inspection = DirectRuntime::inspect(&empty, None).unwrap();
        assert_eq!(
            inspection.state,
            DirectRuntimeInspectionState::Uninitialized
        );
        assert!(!empty.join("runtime.lock").exists());
        assert!(!empty.join("runtime.db").exists());

        let state = root.path().join("state");
        let runtime = DirectRuntime::open(
            namespace(&operator),
            Some(state.clone()),
            false,
            binding("bucket"),
        )
        .await
        .unwrap();
        let snapshot = empty_snapshot(&operator);
        let file = runtime
            .create_file(&snapshot, snapshot.root(), "local")
            .await
            .unwrap();
        file.write(0, b"x").await.unwrap();
        drop(file);
        drop(runtime);

        let inspection = DirectRuntime::inspect(&state, Some(&binding("bucket"))).unwrap();
        assert_eq!(inspection.state, DirectRuntimeInspectionState::Ready);
        assert_eq!(inspection.objects.staged, 1);
        assert_eq!(inspection.mutations.staged, 0);
        assert_eq!(
            inspection.profile.as_deref(),
            Some("yinyang-direct-runtime-1")
        );
        let error = DirectRuntime::inspect(&state, Some(&binding("other"))).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Invalid);
    }
}
