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

//! Read-only Linux FUSE adapter for storage-native Direct namespaces.

use super::attrs::{ATTR_TTL, DIRECTORY_SIZE};
use super::{Outcome, TEARDOWN_GRACE, blocking_error, shutdown_signal};
use crate::direct::{
    DirectFile, DirectFileHandle, DirectFlushOutcome, DirectId, DirectKind, DirectMutationState,
    DirectMutationStatus, DirectNamespace, DirectNode, DirectObjectPin, DirectObjectState,
    DirectObjectStatus, DirectRuntime, DirectSnapshot, Error as DirectError,
    ErrorKind as DirectErrorKind,
};
use crate::volume::VolumeConfig;
use fuser::{
    AccessFlags, BsdFileFlags, Config, FileAttr, FileHandle, FileType, Filesystem, FopenFlags,
    Generation, INodeNo, InitFlags, KernelConfig, LockOwner, MountOption, Notifier, OpenAccMode,
    OpenFlags, RenameFlags, ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory, ReplyEmpty,
    ReplyEntry, ReplyOpen, ReplyStatfs, ReplyWrite, ReplyXattr, Request, Session, SessionACL,
    TimeOrNow, WriteFlags,
};
use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsStr;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use tokio::runtime::Handle;

const ROOT_INODE: u64 = 1;
const INODE_DOMAIN: &[u8] = b"yinyang direct fuse inode v1";
const FALLBACK_STAMP_SECONDS: u64 = 1_600_000_000;

fn has_open_flag(flags: i32, flag: nix::fcntl::OFlag) -> bool {
    flags & flag.bits() != 0
}

fn writable_open(flags: OpenFlags) -> bool {
    flags.acc_mode() != OpenAccMode::O_RDONLY
}

fn writable_create(flags: i32) -> bool {
    flags & libc::O_ACCMODE != libc::O_RDONLY
}

fn representable_mode(mode: u32, umask: u32, read_only: bool, directory: bool) -> bool {
    const S_IFMT: u32 = 0o170000;
    const S_IFREG: u32 = 0o100000;
    const S_IFDIR: u32 = 0o040000;
    let file_type = mode & S_IFMT;
    let type_ok = file_type == 0
        || (directory && file_type == S_IFDIR)
        || (!directory && file_type == S_IFREG);
    if !type_ok || mode & !(S_IFMT | 0o777) != 0 {
        return false;
    }
    let effective = mode & !umask & 0o777;
    let expected = if directory {
        if read_only { 0o555 } else { 0o755 }
    } else if read_only {
        0o444
    } else {
        0o644
    };
    effective == expected
}

fn rename_replace(flags: RenameFlags) -> Result<bool, fuser::Errno> {
    let known =
        RenameFlags::RENAME_NOREPLACE | RenameFlags::RENAME_EXCHANGE | RenameFlags::RENAME_WHITEOUT;
    if flags.bits() & !known.bits() != 0 {
        return Err(fuser::Errno::EINVAL);
    }
    if flags.contains(RenameFlags::RENAME_EXCHANGE) || flags.contains(RenameFlags::RENAME_WHITEOUT)
    {
        return Err(fuser::Errno::EOPNOTSUPP);
    }
    Ok(!flags.contains(RenameFlags::RENAME_NOREPLACE))
}

struct DirectInodeTable {
    by_inode: BTreeMap<u64, DirectId>,
    by_id: BTreeMap<DirectId, u64>,
    lookups: BTreeMap<u64, u64>,
}

impl DirectInodeTable {
    fn new(root: DirectId) -> Self {
        Self {
            by_inode: [(ROOT_INODE, root)].into_iter().collect(),
            by_id: [(root, ROOT_INODE)].into_iter().collect(),
            lookups: BTreeMap::new(),
        }
    }

    fn inode(&mut self, id: DirectId) -> Result<u64, fuser::Errno> {
        if let Some(inode) = self.by_id.get(&id) {
            return Ok(*inode);
        }
        let mut hasher = blake3::Hasher::new();
        hasher.update(INODE_DOMAIN);
        hasher.update(id.as_bytes());
        let mut bytes = [0_u8; 8];
        bytes.copy_from_slice(&hasher.finalize().as_bytes()[..8]);
        let inode = u64::from_be_bytes(bytes);
        if inode <= ROOT_INODE || self.by_inode.contains_key(&inode) {
            return Err(fuser::Errno::EIO);
        }
        self.by_inode.insert(inode, id);
        self.by_id.insert(id, inode);
        Ok(inode)
    }

    fn node(&self, inode: u64) -> Option<DirectId> {
        self.by_inode.get(&inode).copied()
    }

    fn remember_lookup(&mut self, inode: u64) {
        let count = self.lookups.entry(inode).or_default().saturating_add(1);
        self.lookups.insert(inode, count);
    }

    fn forget(&mut self, inode: u64, count: u64) {
        let Some(remaining) = self.lookups.get_mut(&inode) else {
            return;
        };
        *remaining = remaining.saturating_sub(count);
        if *remaining == 0 {
            self.lookups.remove(&inode);
        }
    }

    fn inodes(&self) -> Vec<u64> {
        self.by_inode.keys().copied().collect()
    }
}

#[derive(Clone)]
struct FileView {
    size: u64,
    modified: Option<std::time::SystemTime>,
}

#[derive(Clone)]
enum OpenSource {
    Remote(DirectFile),
    Staged(DirectFileHandle),
}

#[derive(Clone)]
struct OpenFile {
    id: DirectId,
    key: String,
    source: OpenSource,
    writable: bool,
    view: Arc<Mutex<FileView>>,
}

#[derive(Clone)]
struct DirItem {
    ino: u64,
    kind: FileType,
    name: String,
}

struct DirCursor {
    ino: u64,
    parent_ino: u64,
    items: Vec<DirItem>,
}

pub(super) struct DirectInner {
    namespace: DirectNamespace,
    read_only: bool,
    direct: Option<DirectRuntime>,
    snapshot: RwLock<Arc<DirectSnapshot>>,
    pending: Mutex<BTreeMap<DirectId, DirectObjectStatus>>,
    views: Mutex<BTreeMap<String, Arc<Mutex<FileView>>>>,
    refresh_error: Mutex<Option<String>>,
    inodes: Mutex<DirectInodeTable>,
    files: Mutex<BTreeMap<u64, Arc<OpenFile>>>,
    dirs: Mutex<BTreeMap<u64, Arc<Mutex<DirCursor>>>>,
    dentries: Mutex<BTreeSet<(u64, String)>>,
    notifier: Mutex<Option<Notifier>>,
    next_handle: AtomicU64,
    tokio: Handle,
}

impl DirectInner {
    fn new(
        namespace: DirectNamespace,
        read_only: bool,
        direct: Option<DirectRuntime>,
        pending: Vec<DirectObjectStatus>,
        snapshot: DirectSnapshot,
        tokio: Handle,
    ) -> Self {
        let root = snapshot.root();
        Self {
            namespace,
            read_only,
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
            inodes: Mutex::new(DirectInodeTable::new(root)),
            files: Mutex::new(BTreeMap::new()),
            dirs: Mutex::new(BTreeMap::new()),
            dentries: Mutex::new(BTreeSet::new()),
            notifier: Mutex::new(None),
            next_handle: AtomicU64::new(0),
            tokio,
        }
    }

    fn spawn(&self, future: impl Future<Output = ()> + Send + 'static) {
        self.tokio.spawn(future);
    }

    fn next_handle(&self) -> u64 {
        self.next_handle.fetch_add(1, Ordering::Relaxed) + 1
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

    async fn refresh_pending(&self) -> Result<(), fuser::Errno> {
        let Some(direct) = &self.direct else {
            return Ok(());
        };
        let statuses = direct
            .pending_objects()
            .await
            .map_err(|error| direct_errno(&error))?;
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

    fn view_for(
        &self,
        key: &str,
        size: u64,
        modified: Option<std::time::SystemTime>,
    ) -> Arc<Mutex<FileView>> {
        let mut views = guard(&self.views);
        let view = views
            .entry(key.to_owned())
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

    fn view_attr(
        &self,
        ino: INodeNo,
        key: &str,
        fallback_modified: Option<std::time::SystemTime>,
    ) -> Option<FileAttr> {
        let has_open = guard(&self.files).values().any(|open| open.key == key);
        let has_pending = guard(&self.pending)
            .values()
            .any(|status| status.key.as_str() == key);
        if !has_open && !has_pending {
            return None;
        }
        let view = guard(&self.views).get(key).cloned()?;
        let view = guard(&view).clone();
        Some(attribute_with_size(
            ino,
            false,
            view.size,
            view.modified.or(fallback_modified),
            self.read_only,
        ))
    }

    fn open_attr(&self, ino: INodeNo, open: &OpenFile) -> FileAttr {
        let view = guard(&open.view).clone();
        attribute_with_size(ino, false, view.size, view.modified, self.read_only)
    }

    fn status_attr(&self, ino: INodeNo, status: &DirectObjectStatus) -> FileAttr {
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
        let view = self.view_for(status.key.as_str(), status.length, modified);
        let view = guard(&view).clone();
        attribute_with_size(ino, false, view.size, view.modified, self.read_only)
    }

    fn install_notifier(&self, notifier: Notifier) {
        guard(&self.notifier).replace(notifier);
    }

    fn current(&self) -> Result<Arc<DirectSnapshot>, fuser::Errno> {
        if guard(&self.refresh_error).is_some() {
            return Err(fuser::Errno::EIO);
        }
        let snapshot = self
            .snapshot
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        Ok(snapshot)
    }

    fn attr_ttl(&self) -> std::time::Duration {
        if self.read_only {
            ATTR_TTL
        } else {
            std::time::Duration::ZERO
        }
    }

    fn node(&self, ino: INodeNo) -> Option<DirectId> {
        guard(&self.inodes).node(ino.0)
    }

    fn inode_for(&self, id: DirectId) -> Result<INodeNo, fuser::Errno> {
        guard(&self.inodes).inode(id).map(INodeNo)
    }

    fn remember_lookup(&self, ino: INodeNo) {
        guard(&self.inodes).remember_lookup(ino.0);
    }

    fn forget(&self, ino: INodeNo, count: u64) {
        guard(&self.inodes).forget(ino.0, count);
    }

    fn record_dentry(&self, parent: INodeNo, name: &str) {
        guard(&self.dentries).insert((parent.0, name.to_owned()));
    }

    async fn refresh_once(&self) {
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
                if let Err(errno) = self.refresh_pending().await {
                    eprintln!("yy: fuse refresh: cannot load Direct pending state: {errno:?}");
                }
                let pending = guard(&self.pending).len();
                eprintln!(
                    "yy: fuse refresh: Direct observation {revision} with {root_entries} root entries and {pending} pending objects"
                );
                self.invalidate_known().await;
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

    async fn invalidate_known(&self) {
        let inodes = guard(&self.inodes).inodes();
        let dentries = guard(&self.dentries).iter().cloned().collect::<Vec<_>>();
        let Some(notifier) = guard(&self.notifier).clone() else {
            eprintln!("yy: fuse refresh: notifier not installed; skipping kernel invalidation");
            return;
        };
        let attempts = inodes.len() + dentries.len();
        let result = tokio::task::spawn_blocking(move || {
            let (mut inode_ok, mut entry_ok, mut errors) = (0usize, 0usize, 0usize);
            for inode in inodes {
                match notifier.inval_inode(INodeNo(inode), -1, 0) {
                    Ok(()) => inode_ok += 1,
                    Err(_) => errors += 1,
                }
            }
            for (parent, name) in dentries {
                match notifier.inval_entry(INodeNo(parent), OsStr::new(&name)) {
                    Ok(()) => entry_ok += 1,
                    Err(_) => errors += 1,
                }
            }
            (inode_ok, entry_ok, errors)
        })
        .await
        .unwrap_or((0, 0, attempts));
        eprintln!(
            "yy: fuse refresh: done (inode={} entries={} errors={})",
            result.0, result.1, result.2
        );
    }

    pub(super) async fn serve_hup(
        self: Arc<Self>,
        mut signal: tokio::signal::unix::Signal,
        mut stop: tokio::sync::oneshot::Receiver<()>,
    ) {
        loop {
            tokio::select! {
                biased;
                _ = &mut stop => return,
                event = signal.recv() => match event {
                    Some(()) => self.refresh_once().await,
                    None => return,
                },
            }
        }
    }

    async fn lookup(&self, parent: INodeNo, name: String, reply: ReplyEntry) {
        let Some(parent_id) = self.node(parent) else {
            return reply.error(fuser::Errno::ESTALE);
        };
        if let Some(status) = self.pending_child(parent_id, &name) {
            let ino = match self.inode_for(status.id) {
                Ok(ino) => ino,
                Err(errno) => return reply.error(errno),
            };
            self.record_dentry(parent, &name);
            self.remember_lookup(ino);
            return reply.entry(
                &self.attr_ttl(),
                &self.status_attr(ino, &status),
                Generation(0),
            );
        }
        let snapshot = match self.current() {
            Ok(snapshot) => snapshot,
            Err(errno) => return reply.error(errno),
        };
        let node = match snapshot.lookup(parent_id, &name) {
            Ok(Some(node)) => node,
            Ok(None) => return reply.error(fuser::Errno::ENOENT),
            Err(error) => return reply.error(direct_errno(&error)),
        };
        let ino = match self.inode_for(node.id()) {
            Ok(ino) => ino,
            Err(errno) => return reply.error(errno),
        };
        self.record_dentry(parent, &name);
        self.remember_lookup(ino);
        reply.entry(
            &self.attr_ttl(),
            &attribute(ino, node, node.modified(), self.read_only),
            Generation(0),
        );
    }

    async fn getattr(&self, ino: INodeNo, fh: Option<FileHandle>, reply: ReplyAttr) {
        if let Some(fh) = fh
            && let Some(open) = guard(&self.files).get(&fh.0).cloned()
        {
            return reply.attr(&self.attr_ttl(), &self.open_attr(ino, &open));
        }
        let Some(id) = self.node(ino) else {
            return reply.error(fuser::Errno::ESTALE);
        };
        if let Some(status) = self.pending_status(id) {
            return reply.attr(&self.attr_ttl(), &self.status_attr(ino, &status));
        }
        let snapshot = match self.current() {
            Ok(snapshot) => snapshot,
            Err(errno) => return reply.error(errno),
        };
        let Some(node) = snapshot.node(id) else {
            return reply.error(fuser::Errno::ENOENT);
        };
        let key = node.key().unwrap_or(node.path());
        if let Some(attr) = self.view_attr(ino, key, node.modified()) {
            return reply.attr(&self.attr_ttl(), &attr);
        }
        reply.attr(
            &self.attr_ttl(),
            &attribute(ino, node, node.modified(), self.read_only),
        );
    }

    async fn open(&self, ino: INodeNo, flags: OpenFlags, reply: ReplyOpen) {
        let Some(id) = self.node(ino) else {
            return reply.error(fuser::Errno::ESTALE);
        };
        let writable = writable_open(flags);
        let truncate = has_open_flag(flags.0, nix::fcntl::OFlag::O_TRUNC);
        if truncate && !writable {
            return reply.error(fuser::Errno::EINVAL);
        }
        if self.read_only {
            if writable {
                return reply.error(fuser::Errno::EROFS);
            }
            return self.open_read_only(ino, id, reply).await;
        }
        let Some(direct) = &self.direct else {
            return reply.error(fuser::Errno::EIO);
        };
        let (file, key, modified) = if let Some(status) = self.pending_status(id) {
            if status.observed.is_missing() {
                return reply.error(fuser::Errno::ENOENT);
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
            let file = match direct.open_pending(id, writable).await {
                Ok(file) => file,
                Err(error) => return reply.error(direct_errno(&error)),
            };
            (file, status.key.as_str().to_owned(), modified)
        } else {
            let snapshot = match self.current() {
                Ok(snapshot) => snapshot,
                Err(errno) => return reply.error(errno),
            };
            let Some(node) = snapshot.node(id) else {
                return reply.error(fuser::Errno::ESTALE);
            };
            if node.is_directory() {
                return reply.error(fuser::Errno::EISDIR);
            }
            let file = match direct.open_file(&snapshot, id, writable).await {
                Ok(file) => file,
                Err(error) => return reply.error(direct_errno(&error)),
            };
            (
                file,
                node.key().unwrap_or(node.path()).to_owned(),
                node.modified(),
            )
        };
        if let Err(errno) = self.invalidate_before_open(ino).await {
            return reply.error(errno);
        }
        if truncate && let Err(error) = file.truncate(0).await {
            return reply.error(direct_errno(&error));
        }
        let length = match file.len().await {
            Ok(length) => length,
            Err(error) => return reply.error(direct_errno(&error)),
        };
        let view = self.view_for(&key, length, modified);
        if let Err(error) = file.status().await {
            return reply.error(direct_errno(&error));
        }
        let fh = self.next_handle();
        guard(&self.files).insert(
            fh,
            Arc::new(OpenFile {
                id,
                key,
                source: OpenSource::Staged(file),
                writable,
                view,
            }),
        );
        reply.opened(FileHandle(fh), FopenFlags::FOPEN_DIRECT_IO);
    }

    async fn open_read_only(&self, ino: INodeNo, id: DirectId, reply: ReplyOpen) {
        let snapshot = match self.current() {
            Ok(snapshot) => snapshot,
            Err(errno) => return reply.error(errno),
        };
        let Some(node) = snapshot.node(id) else {
            return reply.error(fuser::Errno::ESTALE);
        };
        if node.is_directory() {
            return reply.error(fuser::Errno::EISDIR);
        }
        let file = match snapshot.open_file(id).await {
            Ok(file) => file,
            Err(error) => return reply.error(direct_errno(&error)),
        };
        if let Err(errno) = self.invalidate_before_open(ino).await {
            return reply.error(errno);
        }
        let key = node.key().unwrap_or(node.path()).to_owned();
        let view = self.view_for(&key, node.size(), node.modified());
        let fh = self.next_handle();
        guard(&self.files).insert(
            fh,
            Arc::new(OpenFile {
                id,
                key,
                source: OpenSource::Remote(file),
                writable: false,
                view,
            }),
        );
        reply.opened(FileHandle(fh), FopenFlags::FOPEN_DIRECT_IO);
    }

    async fn create(
        &self,
        parent: INodeNo,
        name: String,
        mode: u32,
        umask: u32,
        flags: i32,
        reply: ReplyCreate,
    ) {
        if self.read_only {
            return reply.error(fuser::Errno::EROFS);
        }
        if !representable_mode(mode, umask, false, false) {
            return reply.error(fuser::Errno::EOPNOTSUPP);
        }
        let Some(parent_id) = self.node(parent) else {
            return reply.error(fuser::Errno::ESTALE);
        };
        let snapshot = match self.current() {
            Ok(snapshot) => snapshot,
            Err(errno) => return reply.error(errno),
        };
        let Some(direct) = &self.direct else {
            return reply.error(fuser::Errno::EIO);
        };
        let file = match direct.create_file(&snapshot, parent_id, &name).await {
            Ok(file) => file,
            Err(error) => return reply.error(direct_errno(&error)),
        };
        let writable = writable_create(flags);
        if !writable {
            let flush = match file.flush().await {
                Ok(flush) => flush,
                Err(error) => return reply.error(direct_errno(&error)),
            };
            match flush {
                DirectFlushOutcome::Unchanged | DirectFlushOutcome::Applied { .. } => {
                    self.refresh_once().await;
                }
                DirectFlushOutcome::Conflict { .. } => {
                    let _ = self.refresh_pending().await;
                    return reply.error(fuser::Errno::ESTALE);
                }
                DirectFlushOutcome::Unknown { .. } => {
                    let _ = self.refresh_pending().await;
                    return reply.error(fuser::Errno::EIO);
                }
                DirectFlushOutcome::Unsupported { message } => {
                    let _ = self.refresh_pending().await;
                    eprintln!("yy: fuse create: Direct publication is unsupported: {message}");
                    return reply.error(fuser::Errno::EOPNOTSUPP);
                }
            }
        }
        let status = match file.status().await {
            Ok(status) => status,
            Err(error) => return reply.error(direct_errno(&error)),
        };
        self.store_pending(status.clone());
        let id = if writable {
            file.id()
        } else {
            match self.current() {
                Ok(snapshot) => snapshot
                    .lookup(parent_id, &name)
                    .ok()
                    .flatten()
                    .map(DirectNode::id)
                    .unwrap_or_else(|| file.id()),
                Err(_) => file.id(),
            }
        };
        let ino = match self.inode_for(id) {
            Ok(ino) => ino,
            Err(errno) => return reply.error(errno),
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
        let view = self.view_for(status.key.as_str(), status.length, modified);
        let fh = self.next_handle();
        let open = OpenFile {
            id,
            key: status.key.as_str().to_owned(),
            source: OpenSource::Staged(file),
            writable,
            view,
        };
        self.record_dentry(parent, &name);
        self.remember_lookup(ino);
        let attr = self.open_attr(ino, &open);
        guard(&self.files).insert(fh, Arc::new(open));
        reply.created(
            &self.attr_ttl(),
            &attr,
            Generation(0),
            FileHandle(fh),
            FopenFlags::FOPEN_DIRECT_IO,
        );
    }

    async fn unlink(&self, parent: INodeNo, name: String, reply: ReplyEmpty) {
        if self.read_only {
            return reply.error(fuser::Errno::EROFS);
        }
        let Some(parent_id) = self.node(parent) else {
            return reply.error(fuser::Errno::ESTALE);
        };
        let Some(direct) = &self.direct else {
            return reply.error(fuser::Errno::EIO);
        };
        if let Some(status) = self.pending_child(parent_id, &name) {
            if !status.existed_remote {
                if guard(&self.files).values().any(|open| open.id == status.id) {
                    return reply.error(fuser::Errno::EOPNOTSUPP);
                }
                match direct.discard_local(status.id).await {
                    Ok(()) => {
                        if let Err(errno) = self.refresh_pending().await {
                            eprintln!("yy: fuse unlink: cannot refresh local state: {errno:?}");
                        }
                        return reply.ok();
                    }
                    Err(error) => return reply.error(direct_errno(&error)),
                }
            }
            let snapshot = match self.current() {
                Ok(snapshot) => snapshot,
                Err(errno) => return reply.error(errno),
            };
            let source = snapshot
                .node(status.id)
                .filter(|node| node.is_file())
                .map(DirectNode::id)
                .or_else(|| {
                    snapshot
                        .resolve(&status.path)
                        .filter(|node| node.is_file())
                        .map(DirectNode::id)
                });
            let Some(source) = source else {
                return reply.error(fuser::Errno::ESTALE);
            };
            return self.finish_unlink(direct, &snapshot, source, reply).await;
        }
        let snapshot = match self.current() {
            Ok(snapshot) => snapshot,
            Err(errno) => return reply.error(errno),
        };
        let source = match snapshot.lookup(parent_id, &name) {
            Ok(Some(node)) if node.is_file() => node.id(),
            Ok(Some(_)) => return reply.error(fuser::Errno::EISDIR),
            Ok(None) => return reply.error(fuser::Errno::ENOENT),
            Err(error) => return reply.error(direct_errno(&error)),
        };
        self.finish_unlink(direct, &snapshot, source, reply).await;
    }

    async fn finish_unlink(
        &self,
        direct: &DirectRuntime,
        snapshot: &DirectSnapshot,
        source: DirectId,
        reply: ReplyEmpty,
    ) {
        let status = match direct.unlink_file(snapshot, source).await {
            Ok(status) => status,
            Err(error) => return reply.error(direct_errno(&error)),
        };
        match mutation_errno(&status) {
            None => {
                self.refresh_once().await;
                reply.ok();
            }
            Some(errno) => {
                if let Err(pending) = self.refresh_pending().await {
                    eprintln!("yy: fuse unlink: cannot retain mutation state: {pending:?}");
                }
                eprintln!(
                    "yy: fuse unlink: Direct mutation {} retained in {:?}: {}",
                    status.id.to_hex(),
                    status.state,
                    status.error.as_deref().unwrap_or("no diagnostic recorded")
                );
                reply.error(errno);
            }
        }
    }

    async fn rename(
        &self,
        parent: INodeNo,
        name: String,
        new_parent: INodeNo,
        new_name: String,
        flags: RenameFlags,
        reply: ReplyEmpty,
    ) {
        if self.read_only {
            return reply.error(fuser::Errno::EROFS);
        }
        let replace = match rename_replace(flags) {
            Ok(replace) => replace,
            Err(errno) => return reply.error(errno),
        };
        if parent == new_parent && name == new_name {
            return reply.ok();
        }
        let Some(parent_id) = self.node(parent) else {
            return reply.error(fuser::Errno::ESTALE);
        };
        let Some(new_parent_id) = self.node(new_parent) else {
            return reply.error(fuser::Errno::ESTALE);
        };
        let Some(direct) = &self.direct else {
            return reply.error(fuser::Errno::EIO);
        };
        let snapshot = match self.current() {
            Ok(snapshot) => snapshot,
            Err(errno) => return reply.error(errno),
        };
        let source = if let Some(status) = self.pending_child(parent_id, &name) {
            if !status.existed_remote {
                return reply.error(fuser::Errno::EOPNOTSUPP);
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
            match snapshot.lookup(parent_id, &name) {
                Ok(Some(node)) if node.is_file() => Some(node.id()),
                Ok(Some(_)) => return reply.error(fuser::Errno::EOPNOTSUPP),
                Ok(None) => return reply.error(fuser::Errno::ENOENT),
                Err(error) => return reply.error(direct_errno(&error)),
            }
        };
        let Some(source) = source else {
            return reply.error(fuser::Errno::ESTALE);
        };
        let mut status = match direct
            .rename_file(&snapshot, source, new_parent_id, &new_name, replace)
            .await
        {
            Ok(status) => status,
            Err(error) => return reply.error(direct_errno(&error)),
        };
        if matches!(
            status.state,
            DirectMutationState::DestinationApplied | DirectMutationState::PendingSourceCleanup
        ) {
            match direct.retry_mutation(status.id).await {
                Ok(retried) => status = retried,
                Err(error) => {
                    eprintln!(
                        "yy: fuse rename: retry Direct mutation {}: {error}",
                        status.id.to_hex()
                    );
                }
            }
        }
        match mutation_errno(&status) {
            None => {
                self.refresh_once().await;
                reply.ok();
            }
            Some(errno) => {
                if let Err(pending) = self.refresh_pending().await {
                    eprintln!("yy: fuse rename: cannot retain mutation state: {pending:?}");
                }
                eprintln!(
                    "yy: fuse rename: Direct mutation {} retained in {:?}: {}",
                    status.id.to_hex(),
                    status.state,
                    status.error.as_deref().unwrap_or("no diagnostic recorded")
                );
                reply.error(errno);
            }
        }
    }

    async fn invalidate_before_open(&self, ino: INodeNo) -> Result<(), fuser::Errno> {
        let Some(notifier) = guard(&self.notifier).clone() else {
            return Err(fuser::Errno::EIO);
        };
        match tokio::task::spawn_blocking(move || notifier.inval_inode(ino, -1, 0)).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => {
                eprintln!(
                    "yy: fuse direct open: cannot invalidate inode {}: {error}",
                    ino.0
                );
                Err(fuser::Errno::EIO)
            }
            Err(error) => {
                eprintln!(
                    "yy: fuse direct open: inode {} invalidation task failed: {error}",
                    ino.0
                );
                Err(fuser::Errno::EIO)
            }
        }
    }

    async fn read(&self, fh: FileHandle, offset: u64, size: u32, reply: ReplyData) {
        let Some(open) = guard(&self.files).get(&fh.0).cloned() else {
            return reply.error(fuser::Errno::EBADF);
        };
        let result = match &open.source {
            OpenSource::Remote(file) => file
                .read(offset, size as usize)
                .await
                .map_err(|error| direct_errno(&error)),
            OpenSource::Staged(file) => file
                .read(offset, size as usize)
                .await
                .map_err(|error| direct_errno(&error)),
        };
        match result {
            Ok(bytes) => reply.data(&bytes),
            Err(errno) => reply.error(errno),
        }
    }

    async fn write(
        &self,
        fh: FileHandle,
        offset: u64,
        data: Vec<u8>,
        flags: OpenFlags,
        reply: ReplyWrite,
    ) {
        let Some(open) = guard(&self.files).get(&fh.0).cloned() else {
            return reply.error(fuser::Errno::EBADF);
        };
        if !open.writable {
            return reply.error(fuser::Errno::EBADF);
        }
        let OpenSource::Staged(file) = &open.source else {
            return reply.error(fuser::Errno::EROFS);
        };
        let append = has_open_flag(flags.0, nix::fcntl::OFlag::O_APPEND);
        let sync = has_open_flag(flags.0, nix::fcntl::OFlag::O_SYNC)
            || has_open_flag(flags.0, nix::fcntl::OFlag::O_DSYNC);
        let written = match u32::try_from(data.len()) {
            Ok(written) => written,
            Err(_) => return reply.error(fuser::Errno::EINVAL),
        };
        let staged = if append {
            file.append(&data).await.map(|_| ())
        } else {
            file.write(offset, &data).await
        };
        if let Err(error) = staged {
            return reply.error(direct_errno(&error));
        }
        if let Ok(length) = file.len().await {
            let mut view = guard(&open.view);
            view.size = length;
            view.modified = Some(std::time::SystemTime::now());
        }
        if let Ok(status) = file.status().await {
            self.store_pending(status);
        }
        if sync && let Err(errno) = self.flush_staged(&open).await {
            return reply.error(errno);
        }
        reply.written(written);
    }

    async fn flush(&self, fh: FileHandle, reply: ReplyEmpty) {
        let Some(open) = guard(&self.files).get(&fh.0).cloned() else {
            return reply.error(fuser::Errno::EBADF);
        };
        match self.flush_staged(&open).await {
            Ok(()) => reply.ok(),
            Err(errno) => reply.error(errno),
        }
    }

    async fn fsync(&self, fh: FileHandle, reply: ReplyEmpty) {
        self.flush(fh, reply).await;
    }

    #[allow(clippy::too_many_arguments)]
    async fn setattr(
        &self,
        ino: INodeNo,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<TimeOrNow>,
        mtime: Option<TimeOrNow>,
        ctime: Option<std::time::SystemTime>,
        crtime: Option<std::time::SystemTime>,
        chgtime: Option<std::time::SystemTime>,
        bkuptime: Option<std::time::SystemTime>,
        flags: Option<BsdFileFlags>,
        fh: Option<FileHandle>,
        reply: ReplyAttr,
    ) {
        let kernel_timestamps = atime.is_some()
            || mtime.is_some()
            || ctime.is_some()
            || crtime.is_some()
            || chgtime.is_some()
            || bkuptime.is_some();
        if mode.is_some()
            || uid.is_some()
            || gid.is_some()
            || flags.is_some()
            || (size.is_none() && kernel_timestamps)
        {
            return reply.error(fuser::Errno::EOPNOTSUPP);
        }
        let Some(length) = size else {
            let Some(id) = self.node(ino) else {
                return reply.error(fuser::Errno::ESTALE);
            };
            if let Some(status) = self.pending_status(id) {
                return reply.attr(&self.attr_ttl(), &self.status_attr(ino, &status));
            }
            let snapshot = match self.current() {
                Ok(snapshot) => snapshot,
                Err(errno) => return reply.error(errno),
            };
            let Some(node) = snapshot.node(id) else {
                return reply.error(fuser::Errno::ENOENT);
            };
            let key = node.key().unwrap_or(node.path());
            let attr = self
                .view_attr(ino, key, node.modified())
                .unwrap_or_else(|| attribute(ino, node, node.modified(), self.read_only));
            return reply.attr(&self.attr_ttl(), &attr);
        };
        if self.read_only {
            return reply.error(fuser::Errno::EROFS);
        }
        if let Some(fh) = fh {
            let Some(open) = guard(&self.files).get(&fh.0).cloned() else {
                return reply.error(fuser::Errno::EBADF);
            };
            if !open.writable {
                return reply.error(fuser::Errno::EINVAL);
            }
            let OpenSource::Staged(file) = &open.source else {
                return reply.error(fuser::Errno::EROFS);
            };
            if let Err(error) = file.truncate(length).await {
                return reply.error(direct_errno(&error));
            }
            if let Ok(size) = file.len().await {
                let mut view = guard(&open.view);
                view.size = size;
                view.modified = Some(std::time::SystemTime::now());
            }
            if let Ok(status) = file.status().await {
                self.store_pending(status);
            }
            return reply.attr(&self.attr_ttl(), &self.open_attr(ino, &open));
        }

        let Some(id) = self.node(ino) else {
            return reply.error(fuser::Errno::ESTALE);
        };
        let Some(direct) = &self.direct else {
            return reply.error(fuser::Errno::EIO);
        };
        let file = if self.pending_status(id).is_some() {
            match direct.open_pending(id, true).await {
                Ok(file) => file,
                Err(error) => return reply.error(direct_errno(&error)),
            }
        } else {
            let snapshot = match self.current() {
                Ok(snapshot) => snapshot,
                Err(errno) => return reply.error(errno),
            };
            let Some(node) = snapshot.node(id) else {
                return reply.error(fuser::Errno::ENOENT);
            };
            if node.is_directory() {
                return reply.error(fuser::Errno::EISDIR);
            }
            match direct.open_file(&snapshot, id, true).await {
                Ok(file) => file,
                Err(error) => return reply.error(direct_errno(&error)),
            }
        };
        if let Err(error) = file.truncate(length).await {
            return reply.error(direct_errno(&error));
        }
        if let Err(errno) = self.flush_handle(&file).await {
            return reply.error(errno);
        }
        let status = match file.status().await {
            Ok(status) => status,
            Err(error) => return reply.error(direct_errno(&error)),
        };
        self.store_pending(status.clone());
        let current_id = match self.current() {
            Ok(snapshot) => snapshot
                .resolve(&status.path)
                .map(DirectNode::id)
                .unwrap_or(status.id),
            Err(_) => status.id,
        };
        let ino = match self.inode_for(current_id) {
            Ok(ino) => ino,
            Err(errno) => return reply.error(errno),
        };
        reply.attr(&self.attr_ttl(), &self.status_attr(ino, &status));
    }

    async fn flush_staged(&self, open: &OpenFile) -> Result<(), fuser::Errno> {
        if !open.writable {
            return Ok(());
        }
        let OpenSource::Staged(file) = &open.source else {
            return Err(fuser::Errno::EROFS);
        };
        self.flush_handle(file).await
    }

    async fn flush_handle(&self, file: &DirectFileHandle) -> Result<(), fuser::Errno> {
        match file.flush().await.map_err(|error| direct_errno(&error))? {
            DirectFlushOutcome::Unchanged => Ok(()),
            DirectFlushOutcome::Applied { .. } => {
                if let Ok(status) = file.status().await {
                    self.store_pending(status);
                }
                self.refresh_once().await;
                Ok(())
            }
            DirectFlushOutcome::Conflict { .. } => {
                if let Err(errno) = self.refresh_pending().await {
                    eprintln!("yy: fuse flush: cannot retain Direct conflict state: {errno:?}");
                }
                Err(fuser::Errno::ESTALE)
            }
            DirectFlushOutcome::Unknown { .. } => {
                if let Err(errno) = self.refresh_pending().await {
                    eprintln!("yy: fuse flush: cannot retain Direct unknown state: {errno:?}");
                }
                Err(fuser::Errno::EIO)
            }
            DirectFlushOutcome::Unsupported { message } => {
                eprintln!("yy: fuse flush: Direct publication is unsupported: {message}");
                Err(fuser::Errno::EOPNOTSUPP)
            }
        }
    }

    async fn release(&self, fh: FileHandle, reply: ReplyEmpty) {
        guard(&self.files).remove(&fh.0);
        if let Err(errno) = self.refresh_pending().await {
            eprintln!("yy: fuse release: cannot refresh Direct pending state: {errno:?}");
        }
        reply.ok();
    }

    async fn opendir(&self, ino: INodeNo, reply: ReplyOpen) {
        let Some(id) = self.node(ino) else {
            return reply.error(fuser::Errno::ESTALE);
        };
        let snapshot = match self.current() {
            Ok(snapshot) => snapshot,
            Err(errno) => return reply.error(errno),
        };
        let Some(directory) = snapshot.node(id) else {
            return reply.error(fuser::Errno::ENOENT);
        };
        if !directory.is_directory() {
            return reply.error(fuser::Errno::ENOTDIR);
        }
        let parent_ino = match directory.parent() {
            Some(parent) => match self.inode_for(parent) {
                Ok(ino) => ino.0,
                Err(errno) => return reply.error(errno),
            },
            None => ROOT_INODE,
        };
        let children = match snapshot.read_dir(id) {
            Ok(children) => children,
            Err(error) => return reply.error(direct_errno(&error)),
        };
        let mut items = BTreeMap::new();
        for node in children {
            let ino = match self.inode_for(node.id()) {
                Ok(ino) => ino.0,
                Err(errno) => return reply.error(errno),
            };
            items.insert(
                node.name().to_owned(),
                DirItem {
                    ino,
                    kind: match node.kind() {
                        DirectKind::Directory => FileType::Directory,
                        DirectKind::File => FileType::RegularFile,
                    },
                    name: node.name().to_owned(),
                },
            );
        }
        let pending = guard(&self.pending).values().cloned().collect::<Vec<_>>();
        for status in pending {
            if status.parent != id || status.observed.is_missing() {
                continue;
            }
            let ino = match self.inode_for(status.id) {
                Ok(ino) => ino.0,
                Err(errno) => return reply.error(errno),
            };
            items.insert(
                status.name.clone(),
                DirItem {
                    ino,
                    kind: FileType::RegularFile,
                    name: status.name,
                },
            );
        }
        // A pending replacement wins over the last remote observation.
        let items = items.into_values().collect::<Vec<_>>();
        let fh = self.next_handle();
        guard(&self.dirs).insert(
            fh,
            Arc::new(Mutex::new(DirCursor {
                ino: ino.0,
                parent_ino,
                items,
            })),
        );
        reply.opened(FileHandle(fh), FopenFlags::empty());
    }

    async fn readdir(&self, fh: FileHandle, offset: u64, mut reply: ReplyDirectory) {
        let Some(cursor) = guard(&self.dirs).get(&fh.0).cloned() else {
            return reply.error(fuser::Errno::EBADF);
        };
        let cursor = guard(&cursor);
        let mut index = offset as usize;
        loop {
            if index == 0 {
                if reply.add(INodeNo(cursor.ino), 1, FileType::Directory, ".") {
                    break;
                }
                index = 1;
                continue;
            }
            if index == 1 {
                if reply.add(INodeNo(cursor.parent_ino), 2, FileType::Directory, "..") {
                    break;
                }
                index = 2;
                continue;
            }
            let local = index - 2;
            let Some(item) = cursor.items.get(local) else {
                break;
            };
            if reply.add(INodeNo(item.ino), index as u64 + 1, item.kind, &item.name) {
                break;
            }
            index += 1;
        }
        reply.ok();
    }

    async fn releasedir(&self, fh: FileHandle, reply: ReplyEmpty) {
        guard(&self.dirs).remove(&fh.0);
        reply.ok();
    }
}

fn attribute(
    ino: INodeNo,
    node: &DirectNode,
    modified: Option<std::time::SystemTime>,
    read_only: bool,
) -> FileAttr {
    attribute_with_size(ino, node.is_directory(), node.size(), modified, read_only)
}

fn attribute_with_size(
    ino: INodeNo,
    directory: bool,
    node_size: u64,
    modified: Option<std::time::SystemTime>,
    read_only: bool,
) -> FileAttr {
    use nix::unistd::{getegid, geteuid};
    let stamp = modified.unwrap_or_else(|| {
        std::time::SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(FALLBACK_STAMP_SECONDS)
    });
    let size = if directory { DIRECTORY_SIZE } else { node_size };
    FileAttr {
        ino,
        size,
        blocks: size.div_ceil(512),
        atime: stamp,
        mtime: stamp,
        ctime: stamp,
        crtime: stamp,
        kind: if directory {
            FileType::Directory
        } else {
            FileType::RegularFile
        },
        perm: if directory {
            if read_only { 0o555 } else { 0o755 }
        } else if read_only {
            0o444
        } else {
            0o644
        },
        nlink: if directory { 2 } else { 1 },
        uid: geteuid().as_raw(),
        gid: getegid().as_raw(),
        rdev: 0,
        blksize: 4096,
        flags: 0,
    }
}

fn direct_errno(error: &DirectError) -> fuser::Errno {
    match error.kind() {
        DirectErrorKind::Invalid | DirectErrorKind::InvalidName => fuser::Errno::EINVAL,
        DirectErrorKind::NotFound => fuser::Errno::ENOENT,
        DirectErrorKind::NotDirectory => fuser::Errno::ENOTDIR,
        DirectErrorKind::IsDirectory => fuser::Errno::EISDIR,
        DirectErrorKind::Stale => fuser::Errno::ESTALE,
        DirectErrorKind::Unsupported => fuser::Errno::EOPNOTSUPP,
        DirectErrorKind::Storage => fuser::Errno::EIO,
    }
}

fn mutation_errno(status: &DirectMutationStatus) -> Option<fuser::Errno> {
    match status.state {
        DirectMutationState::Applied => None,
        DirectMutationState::Conflict => Some(fuser::Errno::ESTALE),
        DirectMutationState::DestinationApplied | DirectMutationState::PendingSourceCleanup => {
            Some(fuser::Errno::EBUSY)
        }
        DirectMutationState::Failed => Some(fuser::Errno::EOPNOTSUPP),
        DirectMutationState::Staged
        | DirectMutationState::Dispatching
        | DirectMutationState::Unknown => Some(fuser::Errno::EIO),
    }
}

fn guard<T>(lock: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

pub(super) async fn run(
    config: VolumeConfig,
    operator: opendal::Operator,
    mountpoint: std::path::PathBuf,
) -> Result<Outcome, Box<dyn std::error::Error>> {
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
    let inner = Arc::new(DirectInner::new(
        namespace,
        config.read_only,
        direct,
        pending,
        snapshot,
        tokio::runtime::Handle::current(),
    ));
    let filesystem = DirectFuseFrontend {
        inner: inner.clone(),
    };
    let mut options = Config::default();
    options.mount_options = vec![
        MountOption::FSName(format!("yinyang-{}", config.name)),
        MountOption::Subtype("yinyang".into()),
    ];
    if config.read_only {
        options.mount_options.insert(0, MountOption::RO);
    }
    options.acl = SessionACL::Owner;
    let hangup = match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup()) {
        Ok(signal) => Some(signal),
        Err(error) => {
            eprintln!("yy: fuse refresh: cannot handle SIGHUP: {error}");
            None
        }
    };
    let session = Session::new(filesystem, &mountpoint, &options)?;
    inner.install_notifier(session.notifier());
    println!(
        "yy: mounted {} {} Direct at {} (fuse.yinyang)",
        config.name,
        if config.read_only {
            "read-only"
        } else {
            "read-write"
        },
        mountpoint.display()
    );
    let background = session.spawn()?;
    let (stop_refresh, stop_refresh_rx) = tokio::sync::oneshot::channel::<()>();
    let refresh_task = hangup.map(|signal| {
        let refresh = inner.clone();
        tokio::runtime::Handle::current()
            .spawn(async move { refresh.serve_hup(signal, stop_refresh_rx).await })
    });
    let (teardown, teardown_rx) = std::sync::mpsc::channel();
    let join = tokio::task::spawn_blocking(move || {
        loop {
            match teardown_rx.recv_timeout(std::time::Duration::from_millis(100)) {
                Ok(()) | Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                    return background.umount_and_join();
                }
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                    if background.guard.is_finished() {
                        return background.join();
                    }
                }
            }
        }
    });
    tokio::pin!(join);
    let joined = tokio::select! {
        result = &mut join => Some(result),
        _ = shutdown_signal() => {
            let _ = teardown.send(());
            None
        }
    };
    let _ = stop_refresh.send(());
    match tokio::time::timeout(TEARDOWN_GRACE, async {
        let session = match joined {
            Some(result) => result,
            None => join.await,
        };
        let refresh = match refresh_task {
            Some(task) => task.await,
            None => Ok(()),
        };
        session.map_err(blocking_error)??;
        refresh.map_err(blocking_error)?;
        Ok(())
    })
    .await
    {
        Ok(Ok(())) => {
            println!("yy: unmounted {}", mountpoint.display());
            Ok(Outcome::Unmounted)
        }
        Ok(Err(error)) => Err(error),
        Err(_) => Ok(Outcome::TeardownExpired {
            grace: TEARDOWN_GRACE,
        }),
    }
}

struct DirectFuseFrontend {
    inner: Arc<DirectInner>,
}

impl Filesystem for DirectFuseFrontend {
    fn init(&mut self, _req: &Request, config: &mut KernelConfig) -> std::io::Result<()> {
        let _ = config.add_capabilities(InitFlags::FUSE_FILE_OPS | InitFlags::FUSE_PARALLEL_DIROPS);
        let _ = config.set_time_granularity(std::time::Duration::from_secs(1));
        Ok(())
    }

    fn destroy(&mut self) {}

    fn lookup(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEntry) {
        let Some(name) = name.to_str() else {
            return reply.error(fuser::Errno::EINVAL);
        };
        let name = name.to_owned();
        let inner = self.inner.clone();
        self.inner
            .spawn(async move { inner.lookup(parent, name, reply).await });
    }

    fn getattr(&self, _req: &Request, ino: INodeNo, fh: Option<FileHandle>, reply: ReplyAttr) {
        let inner = self.inner.clone();
        self.inner
            .spawn(async move { inner.getattr(ino, fh, reply).await });
    }

    fn open(&self, _req: &Request, ino: INodeNo, flags: OpenFlags, reply: ReplyOpen) {
        let inner = self.inner.clone();
        self.inner
            .spawn(async move { inner.open(ino, flags, reply).await });
    }

    fn read(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        size: u32,
        _flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        reply: ReplyData,
    ) {
        let inner = self.inner.clone();
        self.inner
            .spawn(async move { inner.read(fh, offset, size, reply).await });
    }

    fn release(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        _flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        let inner = self.inner.clone();
        self.inner
            .spawn(async move { inner.release(fh, reply).await });
    }

    fn flush(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        _lock_owner: LockOwner,
        reply: ReplyEmpty,
    ) {
        let inner = self.inner.clone();
        self.inner
            .spawn(async move { inner.flush(fh, reply).await });
    }

    fn fsync(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        _datasync: bool,
        reply: ReplyEmpty,
    ) {
        let inner = self.inner.clone();
        self.inner
            .spawn(async move { inner.fsync(fh, reply).await });
    }

    fn opendir(&self, _req: &Request, ino: INodeNo, _flags: OpenFlags, reply: ReplyOpen) {
        let inner = self.inner.clone();
        self.inner
            .spawn(async move { inner.opendir(ino, reply).await });
    }

    fn readdir(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        reply: ReplyDirectory,
    ) {
        let inner = self.inner.clone();
        self.inner
            .spawn(async move { inner.readdir(fh, offset, reply).await });
    }

    fn releasedir(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        _flags: OpenFlags,
        reply: ReplyEmpty,
    ) {
        let inner = self.inner.clone();
        self.inner
            .spawn(async move { inner.releasedir(fh, reply).await });
    }

    fn fsyncdir(
        &self,
        _req: &Request,
        _ino: INodeNo,
        _fh: FileHandle,
        _datasync: bool,
        reply: ReplyEmpty,
    ) {
        reply.ok();
    }

    fn statfs(&self, _req: &Request, _ino: INodeNo, reply: ReplyStatfs) {
        reply.statfs(0, 0, 0, 0, 0, 4096, 255, 0);
    }

    fn access(&self, _req: &Request, _ino: INodeNo, mask: AccessFlags, reply: ReplyEmpty) {
        if self.inner.read_only && mask.contains(AccessFlags::W_OK) {
            reply.error(fuser::Errno::EROFS);
        } else {
            reply.ok();
        }
    }

    fn forget(&self, _req: &Request, ino: INodeNo, nlookup: u64) {
        self.inner.forget(ino, nlookup);
    }

    fn setattr(
        &self,
        _req: &Request,
        ino: INodeNo,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<TimeOrNow>,
        mtime: Option<TimeOrNow>,
        ctime: Option<std::time::SystemTime>,
        fh: Option<FileHandle>,
        crtime: Option<std::time::SystemTime>,
        chgtime: Option<std::time::SystemTime>,
        bkuptime: Option<std::time::SystemTime>,
        flags: Option<BsdFileFlags>,
        reply: ReplyAttr,
    ) {
        let inner = self.inner.clone();
        self.inner.spawn(async move {
            inner
                .setattr(
                    ino, mode, uid, gid, size, atime, mtime, ctime, crtime, chgtime, bkuptime,
                    flags, fh, reply,
                )
                .await
        });
    }

    fn readlink(&self, _req: &Request, _ino: INodeNo, reply: ReplyData) {
        reply.error(fuser::Errno::EINVAL);
    }

    fn mknod(
        &self,
        _req: &Request,
        _parent: INodeNo,
        _name: &OsStr,
        _mode: u32,
        _umask: u32,
        _rdev: u32,
        reply: ReplyEntry,
    ) {
        reply.error(if self.inner.read_only {
            fuser::Errno::EROFS
        } else {
            fuser::Errno::EOPNOTSUPP
        });
    }

    fn mkdir(
        &self,
        _req: &Request,
        _parent: INodeNo,
        _name: &OsStr,
        _mode: u32,
        _umask: u32,
        reply: ReplyEntry,
    ) {
        reply.error(if self.inner.read_only {
            fuser::Errno::EROFS
        } else {
            fuser::Errno::EOPNOTSUPP
        });
    }

    fn unlink(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        let Some(name) = name.to_str() else {
            return reply.error(fuser::Errno::EINVAL);
        };
        let name = name.to_owned();
        let inner = self.inner.clone();
        self.inner
            .spawn(async move { inner.unlink(parent, name, reply).await });
    }

    fn rmdir(&self, _req: &Request, _parent: INodeNo, _name: &OsStr, reply: ReplyEmpty) {
        reply.error(if self.inner.read_only {
            fuser::Errno::EROFS
        } else {
            fuser::Errno::EOPNOTSUPP
        });
    }

    fn symlink(
        &self,
        _req: &Request,
        _parent: INodeNo,
        _link_name: &OsStr,
        _target: &std::path::Path,
        reply: ReplyEntry,
    ) {
        reply.error(if self.inner.read_only {
            fuser::Errno::EROFS
        } else {
            fuser::Errno::EOPNOTSUPP
        });
    }

    fn rename(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        newparent: INodeNo,
        newname: &OsStr,
        flags: RenameFlags,
        reply: ReplyEmpty,
    ) {
        let (Some(name), Some(newname)) = (name.to_str(), newname.to_str()) else {
            return reply.error(fuser::Errno::EINVAL);
        };
        let (name, newname) = (name.to_owned(), newname.to_owned());
        let inner = self.inner.clone();
        self.inner.spawn(async move {
            inner
                .rename(parent, name, newparent, newname, flags, reply)
                .await
        });
    }

    fn link(
        &self,
        _req: &Request,
        _ino: INodeNo,
        _newparent: INodeNo,
        _newname: &OsStr,
        reply: ReplyEntry,
    ) {
        reply.error(if self.inner.read_only {
            fuser::Errno::EROFS
        } else {
            fuser::Errno::EOPNOTSUPP
        });
    }

    fn write(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        data: &[u8],
        _write_flags: WriteFlags,
        flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        reply: ReplyWrite,
    ) {
        let data = data.to_vec();
        let inner = self.inner.clone();
        self.inner
            .spawn(async move { inner.write(fh, offset, data, flags, reply).await });
    }

    fn setxattr(
        &self,
        _req: &Request,
        _ino: INodeNo,
        _name: &OsStr,
        _value: &[u8],
        _flags: i32,
        _position: u32,
        reply: ReplyEmpty,
    ) {
        reply.error(fuser::Errno::EOPNOTSUPP);
    }

    fn getxattr(
        &self,
        _req: &Request,
        _ino: INodeNo,
        _name: &OsStr,
        _size: u32,
        reply: ReplyXattr,
    ) {
        reply.error(fuser::Errno::EOPNOTSUPP);
    }

    fn listxattr(&self, _req: &Request, _ino: INodeNo, _size: u32, reply: ReplyXattr) {
        reply.error(fuser::Errno::EOPNOTSUPP);
    }

    fn removexattr(&self, _req: &Request, _ino: INodeNo, _name: &OsStr, reply: ReplyEmpty) {
        reply.error(fuser::Errno::EOPNOTSUPP);
    }

    fn create(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        umask: u32,
        flags: i32,
        reply: ReplyCreate,
    ) {
        let Some(name) = name.to_str() else {
            return reply.error(fuser::Errno::EINVAL);
        };
        let name = name.to_owned();
        let inner = self.inner.clone();
        self.inner
            .spawn(async move { inner.create(parent, name, mode, umask, flags, reply).await });
    }

    fn fallocate(
        &self,
        _req: &Request,
        _ino: INodeNo,
        _fh: FileHandle,
        _offset: u64,
        _length: u64,
        _mode: i32,
        reply: ReplyEmpty,
    ) {
        reply.error(if self.inner.read_only {
            fuser::Errno::EROFS
        } else {
            fuser::Errno::EOPNOTSUPP
        });
    }

    fn lseek(
        &self,
        _req: &Request,
        _ino: INodeNo,
        _fh: FileHandle,
        _offset: i64,
        _whence: i32,
        reply: fuser::ReplyLseek,
    ) {
        reply.error(fuser::Errno::ENOSYS);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn attributes_use_storage_time_and_safe_fallback() {
        let modified = std::time::SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(123);
        let file = attribute_with_size(INodeNo(42), false, 7, Some(modified), true);
        assert_eq!(file.ino, INodeNo(42));
        assert_eq!(file.size, 7);
        assert_eq!(file.mtime, modified);
        assert_eq!(file.ctime, modified);

        let directory = attribute_with_size(INodeNo(43), true, 0, None, true);
        assert_eq!(directory.size, DIRECTORY_SIZE);
        assert_eq!(
            directory.mtime,
            std::time::SystemTime::UNIX_EPOCH
                + std::time::Duration::from_secs(FALLBACK_STAMP_SECONDS)
        );
    }

    #[test]
    fn open_file_attributes_are_files_with_object_length() {
        let node = DirectNode::for_test_file(
            DirectId::from_bytes([7; 16]),
            "object",
            19,
            Some(std::time::SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(123)),
        );
        let attr = attribute(INodeNo(88), &node, node.modified(), true);
        assert_eq!(attr.kind, FileType::RegularFile);
        assert_eq!(attr.size, 19);
        assert_eq!(attr.perm, 0o444);
    }

    #[test]
    fn writable_modes_match_the_storage_native_contract() {
        assert_eq!(
            attribute_with_size(INodeNo(44), false, 7, None, false).perm,
            0o644
        );
        assert_eq!(
            attribute_with_size(INodeNo(45), true, 0, None, false).perm,
            0o755
        );
        assert!(representable_mode(0o100644, 0o022, false, false));
        assert!(representable_mode(0o755, 0, false, true));
        assert!(!representable_mode(0o100600, 0o022, false, false));
        assert!(!representable_mode(0o100755, 0, false, false));
        assert!(!representable_mode(0o1004644, 0, false, false));
        assert!(!representable_mode(0o100644, 0, true, false));
    }

    #[test]
    fn rename_flags_name_supported_replace_semantics() {
        assert!(matches!(rename_replace(RenameFlags::empty()), Ok(true)));
        assert!(matches!(
            rename_replace(RenameFlags::RENAME_NOREPLACE),
            Ok(false)
        ));
        assert!(matches!(
            rename_replace(RenameFlags::RENAME_EXCHANGE),
            Err(errno) if format!("{errno:?}") == "EOPNOTSUPP"
        ));
        assert!(matches!(
            rename_replace(RenameFlags::RENAME_WHITEOUT),
            Err(errno) if format!("{errno:?}") == "EOPNOTSUPP"
        ));
        assert!(matches!(
            rename_replace(RenameFlags::from_bits_retain(1 << 20)),
            Err(errno) if format!("{errno:?}") == "EINVAL"
        ));
    }
}
