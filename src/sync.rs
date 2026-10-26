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
use crate::runtime::{Error, ErrorKind, HandleStatus, Result, Runtime};
use rusqlite::{Connection, OpenFlags, OptionalExtension, params};
use std::collections::BTreeSet;
use std::path::Path;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::sync::Mutex;
use yinyang_core::{Authority, NodeId, Receipt, Revision};

mod namespace;
pub use namespace::{
    FileMetadata, NamespaceIntent, NamespaceOperation, NamespaceState, NamespaceStatus, RenameMode,
};

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
    pub async fn open(authority: Arc<dyn Authority>, directory: impl AsRef<Path>) -> Result<Self> {
        let runtime = Runtime::open(authority.clone(), directory.as_ref().join("content")).await?;
        let path = directory.as_ref().join("sync.db");
        let filesystem = *authority.filesystem().as_bytes();
        let journal = tokio::task::spawn_blocking(move || -> Result<_> {
            let mut c = Connection::open(path).map_err(Error::local)?;
            c.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;
                CREATE TABLE IF NOT EXISTS binding(singleton INTEGER PRIMARY KEY CHECK(singleton=1), profile TEXT NOT NULL, filesystem BLOB NOT NULL);
                CREATE TABLE IF NOT EXISTS edits(id BLOB PRIMARY KEY, handle BLOB NOT NULL UNIQUE);").map_err(Error::local)?;
            namespace::initialize(&c)?;
            let tx = c.transaction().map_err(Error::local)?;
            let binding: Option<(String, Vec<u8>)> = tx
                .query_row("SELECT profile,filesystem FROM binding", [], |r| {
                    Ok((r.get(0)?, r.get(1)?))
                })
                .optional()
                .map_err(Error::local)?;
            match binding {
                None => {
                    tx.execute(
                        "INSERT INTO binding VALUES(1,'yinyang-sync-2',?1)",
                        params![filesystem.as_slice()],
                    )
                    .map_err(Error::local)?;
                }
                Some((_profile, bound)) if bound != filesystem => {
                    return Err(Error::Invalid(
                        "sync journal belongs to another filesystem or profile",
                    ));
                }
                Some((profile, _)) if profile == "yinyang-sync-1" => {
                    tx.execute("UPDATE binding SET profile='yinyang-sync-2'", [])
                        .map_err(Error::local)?;
                }
                Some((profile, _)) => {
                    if profile != "yinyang-sync-2" {
                        return Err(Error::Invalid(
                            "sync journal belongs to another filesystem or profile",
                        ));
                    }
                }
            }
            tx.commit().map_err(Error::local)?;
            Ok(c)
        }).await.map_err(Error::local)??;
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
        let inserted = self
            .call(move |c| {
                c.execute(
                    "INSERT OR IGNORE INTO edits VALUES(?1,?2)",
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
            return Ok(EditStatus::Namespace(status));
        }
        Ok(EditStatus::Content(self.status(edit).await?))
    }
    /// Inspect durable Sync state without opening a writer or acquiring the
    /// runtime lease. The journal and staging database are opened read-only.
    pub async fn inspect(directory: impl AsRef<Path>) -> Result<SyncInspect> {
        let directory = directory.as_ref().to_owned();
        let runtime = Runtime::inspect(directory.join("content")).await?;
        let (edits, namespace) = tokio::task::spawn_blocking(move || -> Result<_> {
            let path = directory.join("sync.db");
            let connection = Connection::open_with_flags(
                &path,
                OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
            )
            .map_err(Error::local)?;
            connection
                .busy_timeout(std::time::Duration::from_secs(1))
                .map_err(Error::local)?;
            let profile: Option<String> = connection
                .query_row("SELECT profile FROM binding", [], |row| row.get(0))
                .optional()
                .map_err(Error::local)?;
            let namespaces = match profile.as_deref() {
                Some("yinyang-sync-1") => Vec::new(),
                Some("yinyang-sync-2") => namespace::inspect(&connection)?,
                _ => {
                    return Err(Error::Invalid(
                        "sync journal belongs to another filesystem or profile",
                    ));
                }
            };
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
