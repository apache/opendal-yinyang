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

//! Durable conditional uploads for OS-managed or ordinary local replicas.
use crate::local::store;
use crate::runtime::{Error, ErrorKind, Failure, HandleStatus, Result, Runtime};
use rusqlite::{Connection, OptionalExtension, params};
use std::collections::BTreeSet;
use std::path::Path;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::sync::Mutex;
use yinyang_core::{Authority, NodeId, Receipt, Revision, Transaction};

mod namespace;
pub use namespace::{
    FileMetadata, NamespaceIntent, NamespaceOperation, NamespaceState, NamespaceStatus, RenameMode,
};

const SCHEMA: &[store::TableColumns<'static>] = &[
    ("binding", &["singleton", "profile", "filesystem"]),
    ("edits", &["id", "handle"]),
    (namespace::TABLE, namespace::COLUMNS),
];
const PROFILE: store::Profile = store::Profile {
    name: "yinyang-sync-3",
    table: "binding",
    schema: SCHEMA,
};
const CREATE_SCHEMA: &str = "
    CREATE TABLE binding(
      singleton INTEGER PRIMARY KEY CHECK(singleton=1),
      profile TEXT NOT NULL,
      filesystem BLOB NOT NULL
    );
    CREATE TABLE edits(id BLOB PRIMARY KEY, handle BLOB NOT NULL UNIQUE);
    CREATE TABLE namespaces(
      id BLOB PRIMARY KEY,
      intent_key BLOB NOT NULL,
      baseline BLOB NOT NULL,
      commit_id BLOB NOT NULL UNIQUE,
      request_digest BLOB NOT NULL,
      operation BLOB NOT NULL,
      request BLOB NOT NULL,
      state INTEGER NOT NULL,
      receipt_revision BLOB,
      receipt_ordinal INTEGER,
      failure BLOB
    );";

fn store_error(error: store::Error) -> Error {
    let kind = match error.kind() {
        store::ErrorKind::Invalid => ErrorKind::Invalid,
        store::ErrorKind::Unsupported => ErrorKind::Unsupported,
        store::ErrorKind::Corrupt => ErrorKind::Corrupt,
        store::ErrorKind::Busy => ErrorKind::Busy,
        store::ErrorKind::AlreadyExists => ErrorKind::AlreadyExists,
        store::ErrorKind::NotFound => ErrorKind::NotFound,
        store::ErrorKind::ReadOnly => ErrorKind::ReadOnly,
        store::ErrorKind::PermissionDenied => ErrorKind::PermissionDenied,
        store::ErrorKind::NoSpace => ErrorKind::NoSpace,
        store::ErrorKind::Io => ErrorKind::Io,
    };
    Error::local_kind(kind, error)
}

fn validate_profile(connection: &Connection, filesystem: NodeId) -> Result<()> {
    store::validate(
        connection,
        &PROFILE,
        Some(&[store::Binding::blob(
            "filesystem",
            filesystem.as_bytes().to_vec(),
        )]),
    )
    .map_err(store_error)
}

async fn validate_database(
    path: std::path::PathBuf,
    filesystem: NodeId,
    allow_empty: bool,
) -> Result<()> {
    tokio::task::spawn_blocking(move || {
        let connection = store::open_read_only(&path).map_err(store_error)?;
        if allow_empty && store::is_empty(&connection).map_err(store_error)? {
            return Ok(());
        }
        validate_profile(&connection, filesystem)?;
        Ok(())
    })
    .await
    .map_err(Error::local)?
}

pub(crate) async fn preflight_open(
    directory: &Path,
    filesystem: NodeId,
    require_journal: bool,
) -> Result<()> {
    let journal = directory.join("sync.db");
    let staging = directory.join("content/staging.db");
    let journal_exists = journal.try_exists().map_err(Error::local)?;
    if journal_exists {
        validate_database(journal, filesystem, !require_journal).await?;
    } else if require_journal {
        return Err(Error::local_kind(
            ErrorKind::Invalid,
            "sync journal is missing",
        ));
    }
    let staging_exists = staging.try_exists().map_err(Error::local)?;
    if staging_exists {
        Runtime::validate_staging(directory.join("content"), filesystem).await?;
    } else if require_journal {
        return Err(Error::local_kind(
            ErrorKind::Invalid,
            "sync content staging is missing",
        ));
    }
    Ok(())
}

/// Content-addressed local intent, distinct from the frozen remote commit ID.
/// It binds the filesystem, node, observed revision and exact replacement bytes.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct EditId([u8; 32]);
impl EditId {
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}
struct Inner {
    runtime: Runtime,
    journal: Arc<Mutex<Connection>>,
    gate: Mutex<()>,
}
/// Sync is independent of Mount. The caller supplies the common remote baseline,
/// not a newly observed revision after an offline edit. Publication does not mark
/// an OS file in-sync: adapters must first match the completed local edit.
#[derive(Clone)]
pub struct Sync {
    inner: Arc<Inner>,
}
impl Sync {
    /// Validate copied Sync and content-staging databases without acquiring a
    /// writer lease, changing schemas, or recovering records.
    pub async fn validate_restore(directory: impl AsRef<Path>, filesystem: NodeId) -> Result<()> {
        let directory = directory.as_ref().to_owned();
        preflight_open(&directory, filesystem, true).await?;
        let _ = Self::inspect(&directory).await?;
        tokio::task::spawn_blocking(move || {
            let path = directory.join("sync.db");
            let connection = store::open_read_only(&path).map_err(store_error)?;
            validate_profile(&connection, filesystem)?;
            let mut statement = connection
                .prepare("SELECT handle FROM edits")
                .map_err(Error::local)?;
            let handles = statement
                .query_map([], |row| row.get::<_, Vec<u8>>(0))
                .map_err(Error::local)?
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(Error::local)?;
            if handles
                .iter()
                .any(|handle| uuid::Uuid::from_slice(handle).is_err())
            {
                return Err(Error::local_kind(
                    ErrorKind::Corrupt,
                    "sync journal contains an invalid staging handle",
                ));
            }
            Ok(())
        })
        .await
        .map_err(Error::local)?
    }

    pub async fn open(authority: Arc<dyn Authority>, directory: impl AsRef<Path>) -> Result<Self> {
        let filesystem = *authority.filesystem().as_bytes();
        let filesystem = NodeId::from_bytes(filesystem);
        let directory = directory.as_ref().to_owned();
        // Reject every unsupported existing component before Runtime::open
        // validates staging or removes incomplete handles.
        preflight_open(&directory, filesystem, false).await?;
        let runtime = Runtime::open(authority.clone(), directory.join("content")).await?;
        let path = directory.join("sync.db");
        let exists = path.try_exists().map_err(Error::local)?;
        let journal = tokio::task::spawn_blocking(move || -> Result<_> {
            let mut c = store::open_read_write(&path, !exists).map_err(store_error)?;
            let empty = store::is_empty(&c).map_err(store_error)?;
            let tx = c.transaction().map_err(Error::local)?;
            if empty {
                store::initialize(
                    &tx,
                    &PROFILE,
                    CREATE_SCHEMA,
                    &[store::Binding::blob(
                        "filesystem",
                        filesystem.as_bytes().to_vec(),
                    )],
                )
                .map_err(store_error)?;
            }
            validate_profile(&tx, filesystem)?;
            tx.commit().map_err(Error::local)?;
            store::configure(&c, store::Durability::Journal).map_err(store_error)?;
            Ok(c)
        })
        .await
        .map_err(Error::local)??;
        let sync = Self {
            inner: Arc::new(Inner {
                runtime,
                journal: Arc::new(Mutex::new(journal)),
                gate: Mutex::new(()),
            }),
        };
        sync.collect_incomplete().await?;
        Ok(sync)
    }
    pub fn authority(&self) -> &dyn Authority {
        self.inner.runtime.authority()
    }
    async fn call<T: Send + 'static>(
        &self,
        f: impl FnOnce(&mut Connection) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let mut c = self.inner.journal.clone().lock_owned().await;
        tokio::task::spawn_blocking(move || f(&mut c))
            .await
            .map_err(Error::local)?
    }
    async fn handle_id(&self, edit: EditId) -> Result<uuid::Uuid> {
        self.call(move |c| {
            let bytes: Option<Vec<u8>> = c
                .query_row(
                    "SELECT handle FROM edits WHERE id=?1",
                    params![edit.0.as_slice()],
                    |r| r.get(0),
                )
                .optional()
                .map_err(Error::local)?;
            uuid::Uuid::from_slice(&bytes.ok_or(Error::Invalid("unknown local edit"))?)
                .map_err(Error::local)
        })
        .await
    }
    async fn collect_incomplete(&self) -> Result<()> {
        let referenced = self
            .call(|c| {
                let mut q = c
                    .prepare("SELECT handle FROM edits")
                    .map_err(Error::local)?;
                let rows = q
                    .query_map([], |r| r.get::<_, Vec<u8>>(0))
                    .map_err(Error::local)?;
                rows.map(|r| {
                    uuid::Uuid::from_slice(&r.map_err(Error::local)?).map_err(Error::local)
                })
                .collect::<Result<BTreeSet<_>>>()
            })
            .await?;
        let states = self.inner.runtime.recoverable().await?;
        let existing: BTreeSet<_> = states.iter().map(|s| s.id).collect();
        if !referenced.is_subset(&existing) {
            return Err(Error::Invalid(
                "sync journal references missing staged content",
            ));
        }
        for state in states {
            if !referenced.contains(&state.id) {
                // No journal identity was exposed; no publication was permitted.
                // abort still refuses any unexpectedly frozen request.
                self.inner.runtime.recover(state.id).await?.abort().await?;
            }
        }
        Ok(())
    }
    /// Durably accept replacement bytes against a pinned common baseline.
    /// Repeating the exact intent resolves to the same edit and frozen request,
    /// including after extension restart or a lost publication response.
    pub async fn stage(
        &self,
        node: NodeId,
        baseline: Revision,
        source: &mut (dyn AsyncRead + Unpin + Send),
    ) -> Result<EditId> {
        let _gate = self.inner.gate.lock().await;
        self.collect_incomplete().await?;
        let mut handle = self
            .inner
            .runtime
            .open_node_at(baseline, node, true)
            .await?;
        handle.reset_content().await?;
        let mut hash = blake3::Hasher::new_derive_key("yinyang-sync-content-edit-1");
        hash.update(self.authority().filesystem().as_bytes());
        hash.update(node.as_bytes());
        hash.update(&baseline.to_bytes());
        let mut bytes = vec![0; 64 * 1024];
        let mut offset = 0;
        loop {
            let count = source.read(&mut bytes).await.map_err(Error::local)?;
            if count == 0 {
                break;
            }
            hash.update(&bytes[..count]);
            handle.write(offset, &bytes[..count]).await?;
            offset += count as u64;
        }
        let edit = EditId(*hash.finalize().as_bytes());
        let handle_id = handle.id();
        handle.sync_local().await?;
        let inserted = self
            .call(move |c| {
                c.execute(
                    "INSERT OR IGNORE INTO edits(id,handle) VALUES(?1,?2)",
                    params![edit.0.as_slice(), handle_id.as_bytes().as_slice()],
                )
                .map_err(Error::local)
            })
            .await?;
        if inserted == 0 {
            handle.abort().await?;
        }
        Ok(edit)
    }
    pub async fn status(&self, edit: EditId) -> Result<HandleStatus> {
        let _gate = self.inner.gate.lock().await;
        self.inner
            .runtime
            .recover(self.handle_id(edit).await?)
            .await?
            .status()
            .await
    }
    /// Inspect either a content edit or a namespace intent.
    pub async fn edit_status(&self, edit: EditId) -> Result<EditStatus> {
        if let Some(status) = namespace::status(self, edit).await? {
            return Ok(EditStatus::Namespace(Box::new(status)));
        }
        Ok(EditStatus::Content(self.status(edit).await?))
    }
    /// Inspect durable Sync state without opening a writer or acquiring the
    /// runtime lease. The journal and staging database are opened read-only.
    pub async fn inspect(directory: impl AsRef<Path>) -> Result<SyncInspect> {
        let directory = directory.as_ref().to_owned();
        // Journal mappings are committed only after their staging records are
        // durable. Read mappings first, then staging, so a concurrent stage
        // cannot make this read-only join observe a mapping without its handle.
        let journal_directory = directory.clone();
        let (edits, namespace) = tokio::task::spawn_blocking(move || -> Result<_> {
            let path = journal_directory.join("sync.db");
            let connection = store::open_read_only(&path).map_err(store_error)?;
            store::validate(&connection, &PROFILE, None).map_err(store_error)?;
            let namespaces = namespace::inspect(&connection)?;
            let mut statement = connection
                .prepare("SELECT id,handle FROM edits ORDER BY id")
                .map_err(Error::local)?;
            let rows = statement
                .query_map([], |row| {
                    Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, Vec<u8>>(1)?))
                })
                .map_err(Error::local)?;
            let mut edits = Vec::new();
            for row in rows {
                let (id, handle) = row.map_err(Error::local)?;
                let id = id
                    .try_into()
                    .map_err(|_| Error::State(ErrorKind::Corrupt, "invalid edit identity"))
                    .map(EditId::from_bytes)?;
                let handle = uuid::Uuid::from_slice(&handle)
                    .map_err(|_| Error::State(ErrorKind::Corrupt, "invalid handle identity"))?;
                edits.push((id, handle));
            }
            Ok((edits, namespaces))
        })
        .await
        .map_err(Error::local)??;
        let runtime = Runtime::inspect(directory.join("content")).await?;
        let mut content = Vec::with_capacity(edits.len());
        for (id, handle) in edits {
            let status = runtime
                .handles
                .iter()
                .find(|status| status.id == handle)
                .cloned()
                .ok_or(Error::State(
                    ErrorKind::Corrupt,
                    "sync journal references missing staged content",
                ))?;
            content.push(InspectedContent { id, handle: status });
        }
        Ok(SyncInspect { content, namespace })
    }
    pub async fn edits(&self) -> Result<Vec<EditId>> {
        self.call(|c| {
            let mut q = c
                .prepare("SELECT id FROM edits ORDER BY id")
                .map_err(Error::local)?;
            let rows = q
                .query_map([], |r| r.get::<_, Vec<u8>>(0))
                .map_err(Error::local)?;
            rows.map(|r| {
                Ok(EditId(r.map_err(Error::local)?.try_into().map_err(
                    |_| Error::Invalid("invalid local edit identity"),
                )?))
            })
            .collect()
        })
        .await
    }
    /// Durably accept a file creation with its immutable prepared content.
    pub async fn stage_create_file(
        &self,
        parent: NodeId,
        name: &str,
        baseline: Revision,
        metadata: FileMetadata,
        source: &mut (dyn AsyncRead + Unpin + Send),
    ) -> Result<EditId> {
        namespace::stage_create_file(self, parent, name, baseline, metadata, source).await
    }
    /// Durably accept directory creation.
    pub async fn stage_create_directory(
        &self,
        parent: NodeId,
        name: &str,
        baseline: Revision,
    ) -> Result<EditId> {
        namespace::stage_create_directory(self, parent, name, baseline).await
    }
    /// Durably accept removal of an observed stable identity.
    pub async fn stage_remove(&self, node: NodeId, baseline: Revision) -> Result<EditId> {
        namespace::stage_remove(self, node, baseline).await
    }
    /// Durably accept a rename with explicit destination replacement policy.
    pub async fn stage_rename(
        &self,
        node: NodeId,
        baseline: Revision,
        new_parent: NodeId,
        new_name: &str,
        mode: RenameMode,
    ) -> Result<EditId> {
        namespace::stage_rename(self, node, baseline, new_parent, new_name, mode).await
    }
    /// Durably accept an executable-bit change to one observed file identity.
    pub async fn stage_set_executable(
        &self,
        node: NodeId,
        baseline: Revision,
        executable: bool,
    ) -> Result<EditId> {
        namespace::stage_set_executable(self, node, baseline, executable).await
    }
    /// Inspect one durable namespace intent and its exact logical request.
    pub async fn namespace_status(&self, edit: EditId) -> Result<NamespaceStatus> {
        namespace::status(self, edit)
            .await?
            .ok_or(Error::Invalid("unknown namespace intent"))
    }
    /// List all durable namespace intents in stable identity order.
    pub async fn namespace_intents(&self) -> Result<Vec<NamespaceStatus>> {
        namespace::statuses(self).await
    }
    /// Resolve a namespace intent through the original commit receipt.
    ///
    /// Absence is not proof of failure. This method never dispatches or replans.
    pub async fn resolve_namespace(&self, edit: EditId) -> Result<Option<Receipt>> {
        namespace::resolve(self, edit).await
    }
    /// Return the revision that accepted this edit, never a later observation.
    /// Conflicts and Unknown retain all bytes and their original frozen request.
    pub async fn publish(&self, edit: EditId) -> Result<Revision> {
        let _gate = self.inner.gate.lock().await;
        if namespace::exists(self, edit).await? {
            return namespace::publish(self, edit).await;
        }
        let mut handle = self
            .inner
            .runtime
            .recover(self.handle_id(edit).await?)
            .await?;
        handle.fsync().await?;
        Ok(handle.status().await?.remote_revision)
    }

    /// Publish namespace intents through one authority batch while retaining a
    /// separate receipt, Conflict, Retryable or Unknown outcome per member.
    /// Content edits fall back to the existing per-edit path.
    pub async fn publish_batch(&self, edits: &[EditId]) -> Vec<Result<Revision>> {
        let mut results = (0..edits.len()).map(|_| None).collect::<Vec<_>>();
        let mut requests = Vec::<(usize, EditId, Transaction)>::new();
        let mut content = Vec::new();
        {
            let _gate = self.inner.gate.lock().await;
            for (index, edit) in edits.iter().copied().enumerate() {
                match namespace::exists(self, edit).await {
                    Ok(true) => match namespace::prepare_batch(self, edit).await {
                        Ok(namespace::BatchPrepare::Done(result)) => {
                            results[index] = Some(result);
                        }
                        Ok(namespace::BatchPrepare::Request(request)) => {
                            requests.push((index, edit, request));
                        }
                        Err(error) => results[index] = Some(Err(error)),
                    },
                    Ok(false) => content.push((index, edit)),
                    Err(error) => results[index] = Some(Err(error)),
                }
            }
        }
        for (index, edit) in content {
            results[index] = Some(self.publish(edit).await);
        }
        if !requests.is_empty() {
            let batch = requests
                .iter()
                .map(|(_, _, request)| request.clone())
                .collect::<Vec<_>>();
            match self.authority().commit_batch(&batch).await {
                Ok(outcomes) if outcomes.len() == requests.len() => {
                    for ((index, edit, request), outcome) in requests.iter().zip(outcomes) {
                        results[*index] =
                            Some(namespace::complete_batch(self, *edit, request, outcome).await);
                    }
                }
                Ok(_) => {
                    let failure = Failure::capture(&Error::State(
                        ErrorKind::Corrupt,
                        "authority returned the wrong batch outcome count",
                    ));
                    for (index, edit, _) in &requests {
                        let _ = namespace::mark_batch_unknown(self, *edit, failure.clone()).await;
                        results[*index] = Some(Err(Error::Retained(failure.clone())));
                    }
                }
                Err(error) => {
                    let failure = Failure::capture(&Error::from(error));
                    for (index, edit, _) in &requests {
                        let _ = namespace::mark_batch_unknown(self, *edit, failure.clone()).await;
                        results[*index] = Some(Err(Error::Retained(failure.clone())));
                    }
                }
            }
        }
        results
            .into_iter()
            .map(|result| result.unwrap_or(Err(Error::Invalid("missing batch result"))))
            .collect()
    }
    /// Export retained bytes without claiming remote completion.
    ///
    /// Content edits read their local staging. A file-creation namespace intent
    /// reads its immutable prepared descriptor from the authority; other
    /// namespace intents return Unsupported. Adapters that need offline export
    /// of a creation conflict must retain their durable local input separately.
    pub async fn read(&self, edit: EditId, offset: u64, length: usize) -> Result<Vec<u8>> {
        let _gate = self.inner.gate.lock().await;
        if let Some(bytes) = namespace::read(self, edit, offset, length).await? {
            return Ok(bytes);
        }
        self.inner
            .runtime
            .recover(self.handle_id(edit).await?)
            .await?
            .read(offset, length)
            .await
    }
}

/// Status of the two durable edit families supported by Sync.
#[derive(Clone, Debug)]
pub enum EditStatus {
    Content(HandleStatus),
    Namespace(Box<NamespaceStatus>),
}

/// Read-only view of one content edit and its durable handle.
#[derive(Clone, Debug)]
pub struct InspectedContent {
    pub id: EditId,
    pub handle: HandleStatus,
}

/// Read-only Sync state available without acquiring the writer lease.
#[derive(Clone, Debug)]
pub struct SyncInspect {
    pub content: Vec<InspectedContent>,
    pub namespace: Vec<NamespaceStatus>,
}
