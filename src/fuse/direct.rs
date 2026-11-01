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

//! Direct metadata backend for storage-native namespaces.

use super::attrs::{ATTR_TTL, ObservedAttr};
use super::backend::{
    Backend, BackendError, Created, DirectoryEntry, DirectoryOpen, DirectoryPage, Entry, Opened,
    Result, SetAttrRequest, file_type,
};
use super::flags::{has_open_flag, representable_mode, writable_create, writable_open};
use super::inode::InodeIdentity;
use crate::direct::{
    DirectFile, DirectFileHandle, DirectFlushOutcome, DirectId, DirectKind, DirectMutationState,
    DirectMutationStatus, DirectNamespace, DirectNode, DirectObjectId, DirectObjectPin,
    DirectObjectState, DirectObjectStatus, DirectRuntime, DirectSnapshot,
};
use crate::volume::VolumeConfig;
use fuser::OpenFlags;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, SystemTime};

impl InodeIdentity for DirectId {
    const DOMAIN: &'static [u8] = b"yinyang direct fuse inode v1";

    fn identity_bytes(self) -> [u8; 16] {
        *self.as_bytes()
    }
}

#[derive(Clone)]
struct FileView {
    size: u64,
    modified: Option<SystemTime>,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum ViewKey {
    Remote(DirectId),
    Staged(DirectObjectId),
}

fn prune_unreferenced_views(views: &mut BTreeMap<ViewKey, Arc<Mutex<FileView>>>) {
    views.retain(|_, view| Arc::strong_count(view) > 1);
}

#[derive(Clone)]
enum OpenSource {
    Remote(DirectFile),
    Staged(DirectFileHandle),
}

pub(super) struct DirectOpenFile {
    id: DirectId,
    source: OpenSource,
    writable: bool,
    view: Arc<Mutex<FileView>>,
}

impl DirectOpenFile {
    fn current_id(&self) -> DirectId {
        match &self.source {
            OpenSource::Remote(file) => file.id(),
            OpenSource::Staged(file) => file.id(),
        }
    }

    fn matches_id(&self, id: DirectId) -> bool {
        self.id == id || self.current_id() == id
    }
}

pub(super) struct DirectDirectory {
    entries: Vec<DirectoryEntry<DirectId>>,
    served: bool,
}

pub(super) struct DirectBackend {
    namespace: DirectNamespace,
    name: String,
    read_only: bool,
    root: DirectId,
    direct: Option<DirectRuntime>,
    snapshot: RwLock<Arc<DirectSnapshot>>,
    pending: Mutex<BTreeMap<DirectId, DirectObjectStatus>>,
    views: Mutex<BTreeMap<ViewKey, Arc<Mutex<FileView>>>>,
    refresh_error: Mutex<Option<String>>,
}

impl DirectBackend {
    pub(super) async fn open(
        config: VolumeConfig,
        operator: opendal::Operator,
    ) -> std::result::Result<Self, Box<dyn std::error::Error>> {
        let namespace = DirectNamespace::new(operator.clone())?;
        let (direct, pending) = if config.read_only {
            (None, Vec::new())
        } else {
            let binding = crate::volume::direct_storage_binding(&config, &operator)?;
            let direct = DirectRuntime::open(
                namespace.clone(),
                Some(config.staging.clone()),
                false,
                binding,
            )
            .await?;
            let pending = direct.pending_objects().await?;
            (Some(direct), pending)
        };
        let snapshot = namespace.observe().await?;
        if !snapshot.root_node().is_directory() {
            return Err(std::io::Error::other("Direct namespace root is not a directory").into());
        }
        let root = snapshot.root();
        let backend = Self {
            namespace,
            name: config.name,
            read_only: config.read_only,
            root,
            direct,
            snapshot: RwLock::new(Arc::new(snapshot)),
            pending: Mutex::new(
                pending
                    .into_iter()
                    .map(|status| (status.id, status))
                    .collect(),
            ),
            views: Mutex::new(BTreeMap::new()),
            refresh_error: Mutex::new(None),
        };
        Ok(backend)
    }

    fn pending_status(&self, id: DirectId) -> Option<DirectObjectStatus> {
        guard(&self.pending).get(&id).cloned()
    }

    fn pending_child(&self, parent: DirectId, name: &str) -> Option<DirectObjectStatus> {
        guard(&self.pending)
            .values()
            .find(|status| {
                status.parent == parent && status.name == name && !status.observed.is_missing()
            })
            .cloned()
    }

    fn store_pending(&self, status: DirectObjectStatus) {
        let mut pending = guard(&self.pending);
        if matches!(
            status.state,
            DirectObjectState::Clean | DirectObjectState::Discarded
        ) {
            pending.remove(&status.id);
        } else {
            pending.insert(status.id, status);
        }
    }

    async fn refresh_pending(&self) -> Result<()> {
        let Some(direct) = &self.direct else {
            return Ok(());
        };
        let statuses = direct.pending_objects().await?;
        let mut pending = BTreeMap::new();
        for status in statuses {
            if !matches!(
                status.state,
                DirectObjectState::Clean | DirectObjectState::Discarded
            ) {
                pending.insert(status.id, status);
            }
        }
        *guard(&self.pending) = pending;
        Ok(())
    }

    fn current(&self) -> Result<Arc<DirectSnapshot>> {
        if guard(&self.refresh_error).is_some() {
            return Err(BackendError::MutationIo);
        }
        Ok(self
            .snapshot
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone())
    }

    fn view_for(
        &self,
        key: ViewKey,
        size: u64,
        modified: Option<SystemTime>,
    ) -> Arc<Mutex<FileView>> {
        let mut views = guard(&self.views);
        prune_unreferenced_views(&mut views);
        let view = views
            .entry(key)
            .or_insert_with(|| Arc::new(Mutex::new(FileView { size, modified })))
            .clone();
        {
            let mut current = guard(&view);
            current.size = size;
            if modified.is_some() {
                current.modified = modified;
            }
        }
        view
    }

    fn open_attr(&self, open: &DirectOpenFile) -> ObservedAttr {
        let view = guard(&open.view).clone();
        ObservedAttr::from_storage(false, view.size, view.modified)
    }

    fn live_file_attr(&self, id: DirectId) -> Option<ObservedAttr> {
        let views = guard(&self.views);
        let view = views.iter().find_map(|(key, view)| {
            matches!(key, ViewKey::Remote(remote) if *remote == id)
                .then_some(view)
                .filter(|view| Arc::strong_count(view) > 1)
        })?;
        let view = guard(view).clone();
        Some(ObservedAttr::from_storage(false, view.size, view.modified))
    }

    fn status_attr(&self, status: &DirectObjectStatus) -> ObservedAttr {
        let modified = status
            .accepted
            .as_ref()
            .and_then(DirectObjectPin::modified)
            .or_else(|| {
                status
                    .observed
                    .present()
                    .and_then(DirectObjectPin::modified)
            });
        ObservedAttr::from_storage(false, status.length, modified)
    }

    async fn refresh_remote_only(&self) {
        match self.namespace.observe().await {
            Ok(snapshot) => {
                let revision = snapshot.revision().to_hex();
                let root_entries = snapshot
                    .read_dir(snapshot.root())
                    .map(|entries| entries.len())
                    .unwrap_or(0);
                *self
                    .snapshot
                    .write()
                    .unwrap_or_else(|poisoned| poisoned.into_inner()) = Arc::new(snapshot);
                *guard(&self.refresh_error) = None;
                if let Err(error) = self.refresh_pending().await {
                    eprintln!("yy: fuse refresh: cannot load Direct pending state: {error}");
                }
                let pending = guard(&self.pending).len();
                eprintln!(
                    "yy: fuse refresh: Direct observation {revision} with {root_entries} root entries and {pending} pending objects"
                );
            }
            Err(error) => {
                let message = error.to_string();
                eprintln!(
                    "yy: fuse refresh: Direct namespace rejected: {message}; existing open descriptors remain pinned and namespace operations now fail with EIO"
                );
                *guard(&self.refresh_error) = Some(message);
            }
        }
    }

    async fn flush_handle(&self, file: &DirectFileHandle) -> Result<()> {
        match file.flush().await? {
            DirectFlushOutcome::Unchanged => Ok(()),
            DirectFlushOutcome::Applied { .. } => {
                if let Ok(status) = file.status().await {
                    self.store_pending(status);
                }
                self.refresh_remote_only().await;
                Ok(())
            }
            DirectFlushOutcome::Conflict { .. } => {
                if let Err(error) = self.refresh_pending().await {
                    eprintln!("yy: fuse flush: cannot retain Direct conflict state: {error}");
                }
                self.refresh_remote_only().await;
                Err(BackendError::NamespaceConflict)
            }
            DirectFlushOutcome::Unknown { .. } => {
                if let Err(error) = self.refresh_pending().await {
                    eprintln!("yy: fuse flush: cannot retain Direct unknown state: {error}");
                }
                self.refresh_remote_only().await;
                Err(BackendError::MutationIo)
            }
            DirectFlushOutcome::Unsupported { message } => {
                eprintln!("yy: fuse flush: Direct publication is unsupported: {message}");
                Err(BackendError::MutationUnsupported)
            }
        }
    }

    async fn flush_staged(&self, open: &DirectOpenFile) -> Result<()> {
        if !open.writable {
            return Ok(());
        }
        let OpenSource::Staged(file) = &open.source else {
            return Err(BackendError::MutationUnsupported);
        };
        self.flush_handle(file).await
    }

    fn mutation_error(status: &DirectMutationStatus) -> Result<()> {
        match status.state {
            DirectMutationState::Applied => Ok(()),
            DirectMutationState::Conflict => Err(BackendError::NamespaceConflict),
            DirectMutationState::DestinationApplied | DirectMutationState::PendingSourceCleanup => {
                Err(BackendError::MutationIo)
            }
            DirectMutationState::Failed => Err(BackendError::MutationUnsupported),
            DirectMutationState::Staged
            | DirectMutationState::Dispatching
            | DirectMutationState::Unknown => Err(BackendError::MutationIo),
        }
    }
}

impl Backend for DirectBackend {
    type Inode = DirectId;
    type File = DirectOpenFile;
    type Directory = DirectDirectory;

    fn name(&self) -> &str {
        &self.name
    }

    fn read_only(&self) -> bool {
        self.read_only
    }

    fn root(&self) -> DirectId {
        self.root
    }

    fn attr_ttl(&self) -> Duration {
        if self.read_only {
            ATTR_TTL
        } else {
            Duration::ZERO
        }
    }

    fn file_matches(&self, file: &DirectOpenFile, id: DirectId) -> bool {
        file.matches_id(id)
    }

    async fn lookup(&self, parent: DirectId, name: &str) -> Result<Option<Entry<DirectId>>> {
        if let Some(status) = self.pending_child(parent, name) {
            return Ok(Some(Entry {
                id: status.id,
                attr: self.status_attr(&status),
            }));
        }
        let snapshot = self.current()?;
        let node = match snapshot.lookup(parent, name) {
            Ok(Some(node)) => node,
            Ok(None) => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        Ok(Some(Entry {
            id: node.id(),
            attr: ObservedAttr::from_storage(node.is_directory(), node.size(), node.modified()),
        }))
    }

    async fn getattr(&self, id: DirectId, file: Option<&DirectOpenFile>) -> Result<ObservedAttr> {
        if let Some(file) = file {
            return Ok(self.open_attr(file));
        }
        if let Some(status) = self.pending_status(id) {
            return Ok(self.status_attr(&status));
        }
        let snapshot = self.current()?;
        let node = snapshot.node(id).ok_or(BackendError::NotFound)?;
        if let Some(attr) = self.live_file_attr(id) {
            return Ok(attr);
        }
        Ok(ObservedAttr::from_storage(
            node.is_directory(),
            node.size(),
            node.modified(),
        ))
    }

    async fn open(&self, id: DirectId, flags: OpenFlags) -> Result<Opened<DirectOpenFile>> {
        let writable = writable_open(flags);
        if self.read_only {
            let snapshot = self.current()?;
            let node = snapshot.node(id).ok_or(BackendError::MutationIo)?;
            if node.is_directory() {
                return Err(BackendError::IsDirectory);
            }
            let file = snapshot.open_file(id).await?;
            let view = self.view_for(ViewKey::Remote(id), node.size(), node.modified());
            return Ok(Opened {
                file: DirectOpenFile {
                    id,
                    source: OpenSource::Remote(file),
                    writable: false,
                    view,
                },
                writable: false,
            });
        }
        let direct = self.direct.as_ref().ok_or(BackendError::MutationIo)?;
        let (file, modified) = if let Some(status) = self.pending_status(id) {
            if status.observed.is_missing() {
                return Err(BackendError::NotFound);
            }
            let modified = status
                .accepted
                .as_ref()
                .and_then(DirectObjectPin::modified)
                .or_else(|| {
                    status
                        .observed
                        .present()
                        .and_then(DirectObjectPin::modified)
                });
            (direct.open_pending(id, writable).await?, modified)
        } else {
            let snapshot = self.current()?;
            let node = snapshot.node(id).ok_or(BackendError::MutationIo)?;
            if node.is_directory() {
                return Err(BackendError::IsDirectory);
            }
            (
                direct.open_file(&snapshot, id, writable).await?,
                node.modified(),
            )
        };
        if has_open_flag(flags.0, nix::fcntl::OFlag::O_TRUNC) {
            file.truncate(0).await?;
        }
        let length = file.len().await?;
        let view = self.view_for(ViewKey::Staged(file.object_id()), length, modified);
        file.status().await?;
        Ok(Opened {
            file: DirectOpenFile {
                id,
                source: OpenSource::Staged(file),
                writable,
                view,
            },
            writable,
        })
    }

    async fn read(&self, file: &DirectOpenFile, offset: u64, size: usize) -> Result<Vec<u8>> {
        match &file.source {
            OpenSource::Remote(file) => Ok(file.read(offset, size).await?),
            OpenSource::Staged(file) => Ok(file.read(offset, size).await?),
        }
    }

    async fn write(
        &self,
        file: &DirectOpenFile,
        offset: u64,
        data: &[u8],
        flags: OpenFlags,
    ) -> Result<()> {
        let OpenSource::Staged(staged) = &file.source else {
            return Err(BackendError::MutationIo);
        };
        if has_open_flag(flags.0, nix::fcntl::OFlag::O_APPEND) {
            staged.append(data).await?;
        } else {
            staged.write(offset, data).await?;
        }
        if let Ok(length) = staged.len().await {
            let mut view = guard(&file.view);
            view.size = length;
            view.modified = Some(SystemTime::now());
        }
        if let Ok(status) = staged.status().await {
            self.store_pending(status);
        }
        if has_open_flag(flags.0, nix::fcntl::OFlag::O_SYNC)
            || has_open_flag(flags.0, nix::fcntl::OFlag::O_DSYNC)
        {
            self.flush_staged(file).await?;
        }
        Ok(())
    }

    async fn flush(&self, file: &DirectOpenFile) -> Result<()> {
        self.flush_staged(file).await
    }

    async fn fsync(&self, file: &DirectOpenFile) -> Result<()> {
        self.flush_staged(file).await
    }

    async fn release(&self, _id: DirectId, _file: &DirectOpenFile, _writable: bool) -> Result<()> {
        {
            let mut views = guard(&self.views);
            prune_unreferenced_views(&mut views);
        }
        if let Err(error) = self.refresh_pending().await {
            eprintln!("yy: fuse release: cannot refresh Direct pending state: {error}");
        }
        Ok(())
    }

    async fn setattr(
        &self,
        id: DirectId,
        file: Option<&DirectOpenFile>,
        request: SetAttrRequest,
    ) -> Result<ObservedAttr> {
        if request.mode.is_some()
            || request.uid.is_some()
            || request.gid.is_some()
            || request.flags.is_some()
            || (request.size.is_none() && request.has_timestamp())
        {
            return Err(BackendError::MutationUnsupported);
        }
        let Some(length) = request.size else {
            if let Some(status) = self.pending_status(id) {
                return Ok(self.status_attr(&status));
            }
            let snapshot = self.current()?;
            let node = snapshot.node(id).ok_or(BackendError::MutationIo)?;
            if let Some(file) = file {
                return Ok(self.open_attr(file));
            }
            if let Some(attr) = self.live_file_attr(id) {
                return Ok(attr);
            }
            return Ok(ObservedAttr::from_storage(
                node.is_directory(),
                node.size(),
                node.modified(),
            ));
        };
        if self.read_only {
            return Err(BackendError::Runtime(crate::runtime::Error::State(
                crate::runtime::ErrorKind::ReadOnly,
                "Direct mount is read-only",
            )));
        }
        if let Some(open) = file {
            let OpenSource::Staged(file) = &open.source else {
                return Err(BackendError::MutationUnsupported);
            };
            file.truncate(length).await?;
            if let Ok(size) = file.len().await {
                let mut view = guard(&open.view);
                view.size = size;
                view.modified = Some(SystemTime::now());
            }
            if let Ok(status) = file.status().await {
                self.store_pending(status);
            }
            return Ok(self.open_attr(open));
        }
        let direct = self.direct.as_ref().ok_or(BackendError::MutationIo)?;
        let file = if self.pending_status(id).is_some() {
            direct.open_pending(id, true).await?
        } else {
            let snapshot = self.current()?;
            let node = snapshot.node(id).ok_or(BackendError::MutationIo)?;
            if node.is_directory() {
                return Err(BackendError::IsDirectory);
            }
            direct.open_file(&snapshot, id, true).await?
        };
        file.truncate(length).await?;
        self.flush_handle(&file).await?;
        let status = file.status().await?;
        self.store_pending(status.clone());
        Ok(self.status_attr(&status))
    }

    async fn create(
        &self,
        parent: DirectId,
        name: &str,
        mode: u32,
        umask: u32,
        flags: i32,
    ) -> Result<Created<DirectId, DirectOpenFile>> {
        if !representable_mode(mode, umask, false, false, false) {
            return Err(BackendError::MutationUnsupported);
        }
        let snapshot = self.current()?;
        let direct = self.direct.as_ref().ok_or(BackendError::MutationIo)?;
        let file = direct.create_file(&snapshot, parent, name).await?;
        let writable = writable_create(flags);
        if !writable {
            match file.flush().await? {
                DirectFlushOutcome::Unchanged | DirectFlushOutcome::Applied { .. } => {
                    self.refresh_remote_only().await;
                }
                DirectFlushOutcome::Conflict { .. } => {
                    let _ = self.refresh_pending().await;
                    self.refresh_remote_only().await;
                    return Err(BackendError::NamespaceConflict);
                }
                DirectFlushOutcome::Unknown { .. } => {
                    let _ = self.refresh_pending().await;
                    self.refresh_remote_only().await;
                    return Err(BackendError::MutationIo);
                }
                DirectFlushOutcome::Unsupported { message } => {
                    let _ = self.refresh_pending().await;
                    self.refresh_remote_only().await;
                    eprintln!("yy: fuse create: Direct publication is unsupported: {message}");
                    return Err(BackendError::MutationUnsupported);
                }
            }
        }
        let status = file.status().await?;
        self.store_pending(status.clone());
        let id = if writable {
            file.id()
        } else {
            self.current()?
                .lookup(parent, name)
                .ok()
                .flatten()
                .map(DirectNode::id)
                .unwrap_or_else(|| file.id())
        };
        let modified = status
            .accepted
            .as_ref()
            .and_then(DirectObjectPin::modified)
            .or_else(|| {
                status
                    .observed
                    .present()
                    .and_then(DirectObjectPin::modified)
            });
        let view = self.view_for(ViewKey::Staged(file.object_id()), status.length, modified);
        let attr = ObservedAttr::from_storage(false, status.length, modified);
        Ok(Created {
            id,
            attr,
            file: DirectOpenFile {
                id,
                source: OpenSource::Staged(file),
                writable,
                view,
            },
            writable,
        })
    }

    async fn mkdir(
        &self,
        _parent: DirectId,
        _name: &str,
        _mode: u32,
        _umask: u32,
    ) -> Result<Entry<DirectId>> {
        Err(BackendError::MutationUnsupported)
    }

    async fn unlink(&self, parent: DirectId, name: &str) -> Result<()> {
        let direct = self.direct.as_ref().ok_or(BackendError::MutationIo)?;
        if let Some(status) = self.pending_child(parent, name) {
            if !status.existed_remote {
                direct.discard_local(status.id).await?;
                if let Err(error) = self.refresh_pending().await {
                    eprintln!("yy: fuse unlink: cannot refresh local state: {error}");
                }
                return Ok(());
            }
            let snapshot = self.current()?;
            let source = snapshot
                .node(status.id)
                .filter(|node| node.is_file())
                .map(DirectNode::id)
                .or_else(|| {
                    snapshot
                        .resolve(&status.path)
                        .filter(|node| node.is_file())
                        .map(DirectNode::id)
                })
                .ok_or(BackendError::MutationIo)?;
            return self.finish_unlink(direct, &snapshot, source).await;
        }
        let snapshot = self.current()?;
        let source = match snapshot.lookup(parent, name) {
            Ok(Some(node)) if node.is_file() => node.id(),
            Ok(Some(_)) => {
                return Err(BackendError::IsDirectory);
            }
            Ok(None) => {
                return Err(BackendError::NotFound);
            }
            Err(error) => return Err(error.into()),
        };
        self.finish_unlink(direct, &snapshot, source).await
    }

    async fn rmdir(&self, _parent: DirectId, _name: &str) -> Result<()> {
        Err(BackendError::MutationUnsupported)
    }

    async fn rename(
        &self,
        parent: DirectId,
        name: &str,
        new_parent: DirectId,
        new_name: &str,
        replace: bool,
    ) -> Result<()> {
        let direct = self.direct.as_ref().ok_or(BackendError::MutationIo)?;
        let snapshot = self.current()?;
        let source = if let Some(status) = self.pending_child(parent, name) {
            if !status.existed_remote {
                return Err(BackendError::MutationUnsupported);
            }
            snapshot
                .node(status.id)
                .filter(|node| node.is_file())
                .map(DirectNode::id)
                .or_else(|| {
                    snapshot
                        .resolve(&status.path)
                        .filter(|node| node.is_file())
                        .map(DirectNode::id)
                })
        } else {
            match snapshot.lookup(parent, name) {
                Ok(Some(node)) if node.is_file() => Some(node.id()),
                Ok(Some(_)) => return Err(BackendError::MutationUnsupported),
                Ok(None) => {
                    return Err(BackendError::NotFound);
                }
                Err(error) => return Err(error.into()),
            }
        }
        .ok_or(BackendError::MutationIo)?;
        let status = direct
            .rename_file(&snapshot, source, new_parent, new_name, replace)
            .await?;
        self.finish_mutation(status, "rename").await
    }

    async fn open_directory(
        &self,
        id: DirectId,
    ) -> Result<DirectoryOpen<DirectId, DirectDirectory>> {
        let snapshot = self.current()?;
        let directory = snapshot.node(id).ok_or(BackendError::NotFound)?;
        if !directory.is_directory() {
            return Err(BackendError::NotDirectory);
        }
        let parent = directory.parent().unwrap_or(id);
        let mut entries = BTreeMap::new();
        for node in snapshot.read_dir(id)? {
            entries.insert(
                node.name().to_owned(),
                DirectoryEntry {
                    id: node.id(),
                    kind: file_type(matches!(node.kind(), DirectKind::Directory)),
                    name: node.name().to_owned(),
                },
            );
        }
        for status in guard(&self.pending).values().cloned() {
            if status.parent != id || status.observed.is_missing() {
                continue;
            }
            entries.insert(
                status.name.clone(),
                DirectoryEntry {
                    id: status.id,
                    kind: file_type(false),
                    name: status.name,
                },
            );
        }
        Ok(DirectoryOpen {
            id,
            parent,
            state: DirectDirectory {
                entries: entries.into_values().collect(),
                served: false,
            },
        })
    }

    async fn read_directory(
        &self,
        state: &mut DirectDirectory,
        restart: bool,
    ) -> Result<DirectoryPage<DirectId>> {
        if restart {
            state.served = false;
        }
        if state.served {
            return Ok(DirectoryPage {
                entries: Vec::new(),
                complete: true,
            });
        }
        state.served = true;
        Ok(DirectoryPage {
            entries: state.entries.clone(),
            complete: true,
        })
    }

    async fn refresh(&self, _ids: &[DirectId]) -> Result<()> {
        self.refresh_remote_only().await;
        match guard(&self.refresh_error).as_ref() {
            Some(message) => {
                let _ = message;
                Err(BackendError::MutationIo)
            }
            None => Ok(()),
        }
    }
}

impl DirectBackend {
    async fn finish_unlink(
        &self,
        direct: &DirectRuntime,
        snapshot: &DirectSnapshot,
        source: DirectId,
    ) -> Result<()> {
        let status = direct.unlink_file(snapshot, source).await?;
        self.finish_mutation(status, "unlink").await
    }

    async fn finish_mutation(&self, status: DirectMutationStatus, operation: &str) -> Result<()> {
        match DirectBackend::mutation_error(&status) {
            Ok(()) => {
                self.refresh_remote_only().await;
                Ok(())
            }
            Err(error) => {
                if let Err(pending) = self.refresh_pending().await {
                    eprintln!("yy: fuse {operation}: cannot retain mutation state: {pending}");
                }
                if status.state != DirectMutationState::Conflict {
                    self.refresh_remote_only().await;
                }
                eprintln!(
                    "yy: fuse {operation}: Direct mutation {} retained in {:?}: {}",
                    status.id.to_hex(),
                    status.state,
                    status.error.as_deref().unwrap_or("no diagnostic recorded")
                );
                Err(error)
            }
        }
    }
}

fn guard<T>(lock: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn view_pruning_keeps_live_versions_and_reclaims_released_ones() {
        let live_id = DirectId::from_bytes([1; 16]);
        let released_id = DirectId::from_bytes([2; 16]);
        let live = Arc::new(Mutex::new(FileView {
            size: 7,
            modified: None,
        }));
        let released = Arc::new(Mutex::new(FileView {
            size: 11,
            modified: None,
        }));
        let mut views = BTreeMap::from([
            (ViewKey::Remote(live_id), live.clone()),
            (ViewKey::Remote(released_id), released),
        ]);

        prune_unreferenced_views(&mut views);
        assert_eq!(views.len(), 1);
        assert!(views.contains_key(&ViewKey::Remote(live_id)));

        drop(live);
        prune_unreferenced_views(&mut views);
        assert!(views.is_empty());
    }
}
