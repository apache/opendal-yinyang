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

//! Managed metadata backend over Mount, Runtime and Authority.

use super::attrs::{ATTR_TTL, METADATA_STALENESS, ObservedAttr};
use super::backend::{
    Backend, BackendError, Created, DirectoryOpen, DirectoryPage, Entry, Opened, Result,
    SetAttrRequest, file_type,
};
use super::flags::{has_open_flag, representable_mode, writable_create, writable_open};
use super::reject_unpublished_staging;
use crate::mount::{Mount, MountFile, MountReadOnlyFile};
use crate::volume::{Volume, VolumeConfig};
use fuser::OpenFlags;
use std::sync::RwLock;
use std::time::{Duration, Instant};
use tokio::sync::Mutex as AsyncMutex;
use yinyang_core::{FileVersion, Node, NodeId, ScanToken, Snapshot};

const READDIR_PAGE: usize = 512;

struct AcceptedSnapshot {
    snapshot: Snapshot,
    observed_at: Instant,
}

pub(super) struct ManagedBackend {
    mount: Mount,
    name: String,
    read_only: bool,
    root: NodeId,
    accepted: RwLock<Option<AcceptedSnapshot>>,
    observe_gate: AsyncMutex<()>,
}

pub(super) struct ManagedDirectory {
    snapshot: Snapshot,
    directory: NodeId,
    token: Option<ScanToken>,
    complete: bool,
}

impl ManagedBackend {
    pub(super) async fn open(
        config: VolumeConfig,
        operator: opendal::Operator,
        token: Option<String>,
    ) -> std::result::Result<Self, Box<dyn std::error::Error>> {
        let read_only = config.read_only;
        let name = config.name.clone();
        let authority = Volume::connect(&config, operator, token).await?;
        let mount = Mount::open(authority, &config.staging, read_only).await?;
        if read_only {
            reject_unpublished_staging(&mount).await?;
        }
        let root = mount.authority().root();
        let snapshot = mount.authority().observe_latest().await?;
        let root_node = snapshot.node(root).await?;
        if !root_node.is_some_and(|node| node.is_directory()) {
            return Err(std::io::Error::other(
                "remote filesystem root is unavailable or not a directory",
            )
            .into());
        }
        Ok(Self {
            mount,
            name,
            read_only,
            root,
            accepted: RwLock::new(Some(AcceptedSnapshot {
                snapshot,
                observed_at: Instant::now(),
            })),
            observe_gate: AsyncMutex::new(()),
        })
    }

    async fn metadata_snapshot(&self) -> yinyang_core::Result<Snapshot> {
        if let Some(snapshot) = self.cached_snapshot() {
            return Ok(snapshot);
        }
        let _guard = self.observe_gate.lock().await;
        if let Some(snapshot) = self.cached_snapshot() {
            return Ok(snapshot);
        }
        self.fetch_snapshot_locked().await
    }

    fn cached_snapshot(&self) -> Option<Snapshot> {
        let accepted = self
            .accepted
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        accepted.as_ref().and_then(|accepted| {
            (accepted.observed_at.elapsed() <= METADATA_STALENESS)
                .then(|| accepted.snapshot.clone())
        })
    }

    async fn fetch_snapshot(&self) -> yinyang_core::Result<Snapshot> {
        let _guard = self.observe_gate.lock().await;
        self.fetch_snapshot_locked().await
    }

    async fn fetch_snapshot_locked(&self) -> yinyang_core::Result<Snapshot> {
        let snapshot = self.mount.authority().observe_latest().await?;
        self.install_snapshot(snapshot.clone());
        Ok(snapshot)
    }

    fn install_snapshot(&self, snapshot: Snapshot) {
        *self
            .accepted
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(AcceptedSnapshot {
            snapshot,
            observed_at: Instant::now(),
        });
    }

    fn invalidate_snapshot(&self) {
        *self
            .accepted
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
    }

    async fn reclaim_clean_state(&self, id: NodeId) -> Result<()> {
        match self.mount.reclaim(id).await {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == crate::runtime::ErrorKind::Busy => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    fn observed_attr(&self, node: &Node, revision: yinyang_core::Revision) -> ObservedAttr {
        let size = match node.kind() {
            yinyang_core::NodeKind::File(content) => content.content_id().length(),
            yinyang_core::NodeKind::Directory { .. } => 0,
        };
        ObservedAttr::from_revision(node.is_directory(), node.executable(), size, revision)
    }

    async fn snapshot_attr(&self, snapshot: &Snapshot, id: NodeId) -> Result<ObservedAttr> {
        let node = snapshot.node(id).await?.ok_or_else(|| {
            yinyang_core::Error::new(
                yinyang_core::ErrorKind::NotFound,
                "read FUSE attributes",
                "node is absent",
            )
        })?;
        Ok(self.observed_attr(&node, snapshot.revision()))
    }

    fn version_attr(&self, version: &FileVersion) -> ObservedAttr {
        self.observed_attr(version.node(), version.revision())
    }

    async fn handle_attr(&self, id: NodeId, file: &ManagedFile) -> Result<ObservedAttr> {
        match file {
            ManagedFile::Staged(_) => self.shared_attr(id).await?.ok_or(BackendError::MutationIo),
            ManagedFile::ReadOnly(file) => Ok(self.version_attr(file.version())),
        }
    }

    async fn pinned_attr(&self, id: NodeId) -> Result<Option<ObservedAttr>> {
        if self.read_only {
            return Ok(self
                .mount
                .read_only_version(id)?
                .as_ref()
                .map(|version| self.version_attr(version)));
        }
        self.shared_attr(id).await
    }

    async fn shared_attr(&self, id: NodeId) -> Result<Option<ObservedAttr>> {
        self.reclaim_clean_state(id).await?;
        let status = match self.mount.node_status(id).await? {
            Some(status) => status,
            None => return Ok(None),
        };
        let snapshot = self
            .mount
            .authority()
            .observe_revision(status.remote_revision)
            .await?;
        let node = snapshot.node(id).await?.ok_or(BackendError::MutationIo)?;
        let mut observed = self.observed_attr(&node, status.remote_revision);
        observed.size = status.length;
        Ok(Some(observed))
    }

    async fn path_attr(&self, snapshot: &Snapshot, id: NodeId) -> Result<ObservedAttr> {
        if let Some(attr) = self.pinned_attr(id).await? {
            return Ok(attr);
        }
        self.snapshot_attr(snapshot, id).await
    }
}

#[cfg(test)]
impl ManagedBackend {
    pub(super) async fn from_mount_for_test(mount: Mount, read_only: bool) -> Self {
        let root = mount.authority().root();
        let snapshot = mount.authority().observe_latest().await.unwrap();
        Self {
            mount,
            name: "managed".to_owned(),
            read_only,
            root,
            accepted: RwLock::new(Some(AcceptedSnapshot {
                snapshot,
                observed_at: Instant::now(),
            })),
            observe_gate: AsyncMutex::new(()),
        }
    }
}

pub(super) enum ManagedFile {
    Staged(MountFile),
    ReadOnly(Box<MountReadOnlyFile>),
}

impl Backend for ManagedBackend {
    type Inode = NodeId;
    type File = ManagedFile;
    type Directory = ManagedDirectory;

    fn name(&self) -> &str {
        &self.name
    }

    fn read_only(&self) -> bool {
        self.read_only
    }

    fn root(&self) -> NodeId {
        self.root
    }

    fn attr_ttl(&self) -> Duration {
        ATTR_TTL
    }

    fn file_matches(&self, _file: &ManagedFile, _id: NodeId) -> bool {
        false
    }

    async fn lookup(&self, parent: NodeId, name: &str) -> Result<Option<Entry<NodeId>>> {
        let snapshot = self.metadata_snapshot().await?;
        let Some(entry) = snapshot.lookup(parent, name).await? else {
            return Ok(None);
        };
        let attr = self.path_attr(&snapshot, entry.node_id).await?;
        Ok(Some(Entry {
            id: entry.node_id,
            attr,
        }))
    }

    async fn getattr(&self, id: NodeId, file: Option<&ManagedFile>) -> Result<ObservedAttr> {
        if let Some(file) = file {
            return self.handle_attr(id, file).await;
        }
        let snapshot = self.metadata_snapshot().await?;
        self.path_attr(&snapshot, id).await
    }

    async fn open(&self, id: NodeId, flags: OpenFlags) -> Result<Opened<ManagedFile>> {
        let writable = writable_open(flags);
        if self.read_only {
            let file = self.mount.open_read_only_node(id).await?;
            return Ok(Opened {
                file: ManagedFile::ReadOnly(Box::new(file)),
                writable: false,
            });
        }
        self.reclaim_clean_state(id).await?;
        let file = self.mount.open_node(id, writable).await?;
        if has_open_flag(flags.0, nix::fcntl::OFlag::O_TRUNC) {
            file.truncate(0).await?;
        }
        Ok(Opened {
            file: ManagedFile::Staged(file),
            writable,
        })
    }

    async fn read(&self, file: &ManagedFile, offset: u64, size: usize) -> Result<Vec<u8>> {
        match file {
            ManagedFile::Staged(file) => Ok(file.read(offset, size).await?),
            ManagedFile::ReadOnly(file) => Ok(file.read(offset, size).await?),
        }
    }

    async fn write(
        &self,
        file: &ManagedFile,
        offset: u64,
        data: &[u8],
        flags: OpenFlags,
    ) -> Result<()> {
        let ManagedFile::Staged(file) = file else {
            return Err(BackendError::MutationIo);
        };
        if has_open_flag(flags.0, nix::fcntl::OFlag::O_APPEND) {
            file.append(data).await?;
        } else {
            file.write(offset, data).await?;
        }
        if has_open_flag(flags.0, nix::fcntl::OFlag::O_SYNC)
            || has_open_flag(flags.0, nix::fcntl::OFlag::O_DSYNC)
        {
            file.flush().await?;
            self.invalidate_snapshot();
        }
        Ok(())
    }

    async fn flush(&self, file: &ManagedFile) -> Result<()> {
        let ManagedFile::Staged(file) = file else {
            return Ok(());
        };
        file.flush().await?;
        self.invalidate_snapshot();
        Ok(())
    }

    async fn fsync(&self, file: &ManagedFile) -> Result<()> {
        let ManagedFile::Staged(file) = file else {
            return Ok(());
        };
        file.fsync().await?;
        self.invalidate_snapshot();
        Ok(())
    }

    async fn release(&self, id: NodeId, _writable: bool) -> Result<()> {
        let result = if self.read_only {
            self.mount.reclaim_read_only(id)
        } else {
            self.mount.reclaim(id).await
        };
        match result {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == crate::runtime::ErrorKind::Busy => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    async fn setattr(
        &self,
        id: NodeId,
        file: Option<&ManagedFile>,
        request: SetAttrRequest,
    ) -> Result<ObservedAttr> {
        let supported_mutation = request.size.is_some() || request.mode.is_some();
        if request.uid.is_some()
            || request.gid.is_some()
            || request.flags.is_some()
            || (!supported_mutation && request.has_timestamp())
            || (request.size.is_some() && request.mode.is_some())
        {
            return Err(BackendError::MutationUnsupported);
        }
        if let Some(length) = request.size {
            let attr = if let Some(file) = file {
                let ManagedFile::Staged(file) = file else {
                    return Err(BackendError::MutationUnsupported);
                };
                file.truncate(length).await?;
                self.shared_attr(id)
                    .await?
                    .ok_or(BackendError::MutationIo)?
            } else {
                self.reclaim_clean_state(id).await?;
                let file = self.mount.open_node(id, true).await?;
                file.truncate(length).await?;
                file.flush().await?;
                let attr = self
                    .shared_attr(id)
                    .await?
                    .ok_or(BackendError::MutationIo)?;
                drop(file);
                if let Err(error) = self.mount.reclaim(id).await
                    && error.kind() != crate::runtime::ErrorKind::Busy
                {
                    eprintln!("yy: fuse truncate cleanup: {error}");
                }
                attr
            };
            self.invalidate_snapshot();
            return Ok(attr);
        }
        if let Some(mode) = request.mode {
            let snapshot = self.fetch_snapshot().await?;
            let current = snapshot.node(id).await?.ok_or_else(|| {
                yinyang_core::Error::new(
                    yinyang_core::ErrorKind::NotFound,
                    "set FUSE attributes",
                    "node is absent",
                )
            })?;
            let executable = mode & 0o111 != 0;
            if !representable_mode(mode, 0, self.read_only, current.is_directory(), executable) {
                return Err(BackendError::MutationUnsupported);
            }
            if !current.is_directory() {
                let result = if let Some(file) = file {
                    let ManagedFile::Staged(file) = file else {
                        return Err(BackendError::MutationUnsupported);
                    };
                    file.set_executable(executable).await
                } else {
                    self.reclaim_clean_state(id).await?;
                    let file = self.mount.open_node(id, true).await?;
                    let result = file.set_executable(executable).await;
                    drop(file);
                    if let Err(error) = self.mount.reclaim(id).await
                        && error.kind() != crate::runtime::ErrorKind::Busy
                    {
                        eprintln!("yy: fuse chmod cleanup: {error}");
                    }
                    result
                };
                result?;
            }
            let snapshot = self.fetch_snapshot().await?;
            return self.snapshot_attr(&snapshot, id).await;
        }
        let snapshot = self.metadata_snapshot().await?;
        self.snapshot_attr(&snapshot, id).await
    }

    async fn create(
        &self,
        parent: NodeId,
        name: &str,
        mode: u32,
        umask: u32,
        flags: i32,
    ) -> Result<Created<NodeId, ManagedFile>> {
        if !representable_mode(mode, umask, self.read_only, false, mode & 0o111 != 0) {
            return Err(BackendError::MutationUnsupported);
        }
        let exclusive = has_open_flag(flags, nix::fcntl::OFlag::O_EXCL);
        let node = match self
            .mount
            .namespace()
            .create_file_with_node(parent, name, mode & 0o111 != 0)
            .await
        {
            Ok((node, _)) => node,
            Err(error)
                if !exclusive && error.kind() == crate::runtime::ErrorKind::AlreadyExists =>
            {
                let snapshot = self.fetch_snapshot().await?;
                match snapshot.lookup(parent, name).await? {
                    Some(entry) => entry.node_id,
                    None => return Err(BackendError::MutationUnsupported),
                }
            }
            Err(error) => return Err(error.into()),
        };
        self.reclaim_clean_state(node).await?;
        let writable = writable_create(flags);
        let file = self.mount.open_node(node, writable).await?;
        if has_open_flag(flags, nix::fcntl::OFlag::O_TRUNC) {
            file.truncate(0).await?;
        }
        let attr = self
            .shared_attr(node)
            .await?
            .ok_or(BackendError::MutationIo)?;
        self.fetch_snapshot().await?;
        Ok(Created {
            id: node,
            attr,
            file: ManagedFile::Staged(file),
            writable,
        })
    }

    async fn mkdir(
        &self,
        parent: NodeId,
        name: &str,
        mode: u32,
        umask: u32,
    ) -> Result<Entry<NodeId>> {
        if !representable_mode(mode, umask, self.read_only, true, false) {
            return Err(BackendError::MutationUnsupported);
        }
        let (node, _) = self
            .mount
            .namespace()
            .create_directory_with_node(parent, name)
            .await?;
        let snapshot = self.fetch_snapshot().await?;
        let attr = self.snapshot_attr(&snapshot, node).await?;
        Ok(Entry { id: node, attr })
    }

    async fn unlink(&self, parent: NodeId, name: &str) -> Result<()> {
        match self.mount.namespace().unlink_path(parent, name).await {
            Ok(_) => {
                self.invalidate_snapshot();
                Ok(())
            }
            Err(error) => Err(BackendError::runtime_namespace_mutation(error)),
        }
    }

    async fn rmdir(&self, parent: NodeId, name: &str) -> Result<()> {
        match self.mount.namespace().rmdir_path(parent, name).await {
            Ok(_) => {
                self.invalidate_snapshot();
                Ok(())
            }
            Err(error) => Err(error.into()),
        }
    }

    async fn rename(
        &self,
        parent: NodeId,
        name: &str,
        new_parent: NodeId,
        new_name: &str,
        replace: bool,
    ) -> Result<()> {
        let result = if replace {
            self.mount
                .namespace()
                .rename_path_replace(parent, name, new_parent, new_name)
                .await
        } else {
            self.mount
                .namespace()
                .rename_path(parent, name, new_parent, new_name)
                .await
        };
        match result {
            Ok(_) => {
                self.invalidate_snapshot();
                Ok(())
            }
            Err(error) => Err(BackendError::runtime_namespace_mutation(error)),
        }
    }

    async fn open_directory(&self, id: NodeId) -> Result<DirectoryOpen<NodeId, ManagedDirectory>> {
        let snapshot = self.metadata_snapshot().await?;
        let node = snapshot.node(id).await?.ok_or_else(|| {
            yinyang_core::Error::new(
                yinyang_core::ErrorKind::NotFound,
                "open FUSE directory",
                "node is absent",
            )
        })?;
        if !node.is_directory() {
            return Err(yinyang_core::Error::new(
                yinyang_core::ErrorKind::NotDirectory,
                "open FUSE directory",
                "node is not a directory",
            )
            .into());
        }
        let parent = node.link().map_or(self.root, |link| link.parent);
        Ok(DirectoryOpen {
            id,
            parent,
            state: ManagedDirectory {
                snapshot,
                directory: id,
                token: None,
                complete: false,
            },
        })
    }

    async fn read_directory(
        &self,
        state: &mut ManagedDirectory,
        restart: bool,
    ) -> Result<DirectoryPage<NodeId>> {
        if restart {
            state.token = None;
            state.complete = false;
        }
        if state.complete {
            return Ok(DirectoryPage {
                entries: Vec::new(),
                complete: true,
            });
        }
        let page = state
            .snapshot
            .scan(state.directory, state.token.as_ref(), READDIR_PAGE)
            .await?;
        state.token = page.next;
        state.complete = state.token.is_none();
        let mut entries = Vec::with_capacity(page.entries.len());
        for entry in page.entries {
            let node = state
                .snapshot
                .node(entry.node_id)
                .await?
                .ok_or(BackendError::MutationIo)?;
            entries.push(super::backend::DirectoryEntry {
                id: entry.node_id,
                kind: file_type(node.is_directory()),
                name: entry.name,
            });
        }
        Ok(DirectoryPage {
            entries,
            complete: state.complete,
        })
    }

    async fn refresh(&self, ids: &[NodeId]) -> Result<()> {
        let snapshot = self.fetch_snapshot().await?;
        let (mut reclaimed, mut active, mut failures) = (0usize, 0usize, 0usize);
        for id in ids {
            let result = if self.read_only {
                self.mount.reclaim_read_only(*id)
            } else {
                self.mount.reclaim(*id).await
            };
            match result {
                Ok(()) => reclaimed += 1,
                Err(error) if error.kind() == crate::runtime::ErrorKind::Busy => active += 1,
                Err(error) => {
                    failures += 1;
                    eprintln!("yy: fuse refresh: reclaim {id:?}: {error}");
                }
            }
        }
        eprintln!(
            "yy: fuse refresh: Managed revision {} reclaimed {reclaimed} clean nodes ({active} active, {failures} failed)",
            revision_hex(snapshot.revision())
        );
        Ok(())
    }
}

fn revision_hex(revision: yinyang_core::Revision) -> String {
    revision
        .to_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::Fs;
    use crate::runtime::Runtime;
    use crate::support::TestBackend;
    use std::sync::Arc;
    use yinyang_core::Authority;

    async fn backend(
        backend: &TestBackend,
        read_only: bool,
    ) -> (ManagedBackend, Arc<dyn Authority>, tempfile::TempDir) {
        let temp = tempfile::tempdir().unwrap();
        let authority: Arc<dyn Authority> = Arc::new(
            Fs::open(backend.operator())
                .await
                .unwrap_or_else(|_| unreachable!("test creates the authority first")),
        );
        let mount = Mount::open(authority.clone(), temp.path().join("staging"), read_only)
            .await
            .unwrap();
        let snapshot = authority.observe_latest().await.unwrap();
        (
            ManagedBackend {
                mount,
                name: "managed".to_owned(),
                read_only,
                root: authority.root(),
                accepted: RwLock::new(Some(AcceptedSnapshot {
                    snapshot,
                    observed_at: Instant::now(),
                })),
                observe_gate: AsyncMutex::new(()),
            },
            authority,
            temp,
        )
    }

    #[tokio::test]
    async fn dirty_shared_node_reports_staged_size_in_path_attrs() {
        let backend_state = TestBackend::default();
        Fs::create(backend_state.operator()).await.unwrap();
        let (backend, authority, _temp) = backend(&backend_state, false).await;
        let (id, _) = backend
            .mount
            .namespace()
            .create_file_with_node(authority.root(), "file", false)
            .await
            .unwrap();
        let file = backend.mount.open_node(id, true).await.unwrap();
        file.write(0, b"dirty").await.unwrap();
        let attr = backend.shared_attr(id).await.unwrap().unwrap();
        assert_eq!(attr.size, 5);
    }

    #[tokio::test]
    async fn repeated_metadata_callbacks_reuse_the_accepted_snapshot() {
        let backend_state = TestBackend::default();
        let authority: Arc<dyn Authority> =
            Arc::new(Fs::create(backend_state.operator()).await.unwrap());
        let writer_temp = tempfile::tempdir().unwrap();
        let writer = Runtime::open(authority.clone(), writer_temp.path())
            .await
            .unwrap();
        writer.create_file(authority.root(), "file").await.unwrap();
        let id = authority
            .observe_latest()
            .await
            .unwrap()
            .resolve("file")
            .await
            .unwrap()
            .unwrap()
            .id();
        let temp = tempfile::tempdir().unwrap();
        let mount = Mount::open(authority.clone(), temp.path(), false)
            .await
            .unwrap();
        let snapshot = authority.observe_latest().await.unwrap();
        let backend = ManagedBackend {
            mount,
            name: "managed".to_owned(),
            read_only: false,
            root: authority.root(),
            accepted: RwLock::new(Some(AcceptedSnapshot {
                snapshot,
                observed_at: Instant::now(),
            })),
            observe_gate: AsyncMutex::new(()),
        };
        let before = backend_state.head_stat_calls();
        backend
            .lookup(authority.root(), "file")
            .await
            .unwrap()
            .unwrap();
        backend.getattr(id, None).await.unwrap();
        let reads_after_warm = backend_state.state.lock().unwrap().data_reads;
        for _ in 0..32 {
            backend
                .lookup(authority.root(), "file")
                .await
                .unwrap()
                .unwrap();
            backend.getattr(id, None).await.unwrap();
        }
        assert_eq!(
            backend_state.head_stat_calls(),
            before,
            "metadata callbacks within the window observed the head again"
        );
        assert_eq!(
            backend_state.state.lock().unwrap().data_reads,
            reads_after_warm,
            "warm metadata callbacks performed a new page read"
        );
    }
}
