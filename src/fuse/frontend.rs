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

//! Read-only Linux FUSE frontend over the shared Mount staging contract.
//!
//! `fuser` owns the FUSE device, session loop and reply wire encoding; this
//! module implements only the filesystem callbacks. Every callback that can
//! perform remote I/O dispatches onto the process Tokio runtime and moves its
//! reply into the task, so one reply is sent per request and no adapter-wide
//! lock is held across an authority or staging await. Input buffers borrowed
//! from the request are converted to owned values before the dispatch, and
//! the low-level binding replies EIO if a task drops an unanswered reply.
//!
//! Files pin the node's shared staged version. `fstat` on an open handle uses
//! that staged record's length, and reads return staged bytes, so an old
//! descriptor never mixes a newer remote length with older content. `release`
//! reclaims a clean shared node, which makes the next open observe the
//! current remote revision without rebasing live handles. Directory handles
//! pin one `Snapshot` revision; readdir offsets are stable ordinal positions
//! over pages of that pinned listing.
//!
//! The binding replies ENOSYS to FUSE_INTERRUPT, which the kernel ignores;
//! an interrupted syscall therefore does not cancel in-flight remote work.
//! Each operation still replies exactly once. Request tasks are detached and
//! owned by the process Tokio runtime: ending the FUSE session does not abort
//! them, and a reply written after unmount fails on the closing channel. The
//! CLI returns after session teardown, so the runtime shutdown at process
//! exit is the actual point where remaining tasks are dropped.

use super::attrs::{ATTR_TTL, DIRECTORY_SIZE, attribute};
use super::inode::{CachedInfo, InodeTable};
use crate::mount::{Mount, MountFile};
use fuser::{
    AccessFlags, BsdFileFlags, FileAttr, FileHandle, FileType, Filesystem, FopenFlags, Generation,
    INodeNo, InitFlags, KernelConfig, LockOwner, OpenAccMode, OpenFlags, ReplyAttr, ReplyData,
    ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyOpen, ReplyStatfs, ReplyWrite, ReplyXattr,
    Request, TimeOrNow, WriteFlags,
};
use std::collections::BTreeMap;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::SystemTime;
use tokio::runtime::Handle;
use tokio::sync::Mutex as AsyncMutex;
use yinyang_core::{NodeId, ScanToken, Snapshot};

/// Entries fetched per pinned directory scan.
const READDIR_PAGE: usize = 512;

pub(super) struct Inner {
    mount: Mount,
    inodes: Mutex<InodeTable>,
    files: Mutex<BTreeMap<u64, Arc<OpenFile>>>,
    dirs: Mutex<BTreeMap<u64, Arc<AsyncMutex<DirCursor>>>>,
    next_handle: AtomicU64,
    runtime: Handle,
}

/// One kernel open maps to one reference on the node's shared staged version.
struct OpenFile {
    node: NodeId,
    file: MountFile,
    /// Metadata from the exact observation the staged version was
    /// materialized from, so path stats and fstat agree with read bytes.
    base: CachedInfo,
}

#[derive(Clone)]
struct DirItem {
    ino: INodeNo,
    kind: FileType,
    name: String,
}

/// A pinned directory enumeration. Cursor state is serialized per open handle;
/// no mount-wide lock spans the page scans or per-entry metadata reads.
struct DirCursor {
    snapshot: Snapshot,
    ino: INodeNo,
    directory: NodeId,
    parent_ino: INodeNo,
    entries: Vec<DirItem>,
    token: Option<ScanToken>,
    complete: bool,
}

impl Inner {
    pub(super) fn new(mount: Mount, runtime: Handle) -> Self {
        let root = mount.authority().root();
        Self {
            mount,
            inodes: Mutex::new(InodeTable::new(root)),
            files: Mutex::new(BTreeMap::new()),
            dirs: Mutex::new(BTreeMap::new()),
            next_handle: AtomicU64::new(0),
            runtime,
        }
    }

    fn spawn(&self, future: impl Future<Output = ()> + Send + 'static) {
        self.runtime.spawn(future);
    }

    fn next_handle(&self) -> u64 {
        self.next_handle.fetch_add(1, Ordering::Relaxed) + 1
    }

    fn inode_for(&self, id: NodeId, info: Option<CachedInfo>) -> Result<INodeNo, fuser::Errno> {
        let mut inodes = guard(&self.inodes);
        if let Some(info) = info {
            inodes.cache(id, info);
        }
        inodes.inode(id).map(INodeNo).map_err(|_| fuser::Errno::EIO)
    }

    fn node(&self, ino: INodeNo) -> Option<NodeId> {
        guard(&self.inodes).node(ino.0)
    }

    fn remember_lookup(&self, ino: INodeNo) {
        guard(&self.inodes).remember_lookup(ino.0);
    }

    fn forget(&self, ino: INodeNo, count: u64) {
        guard(&self.inodes).forget(ino.0, count);
    }

    fn open_file(&self, fh: FileHandle) -> Option<Arc<OpenFile>> {
        guard(&self.files).get(&fh.0).cloned()
    }

    /// Any active open for a node. `Mount::open_node` reuses the node's shared
    /// staged version, so all handles for one node report the same base.
    fn open_for_node(&self, id: NodeId) -> Option<Arc<OpenFile>> {
        guard(&self.files)
            .values()
            .find(|open| open.node == id)
            .cloned()
    }

    fn dir_cursor(&self, fh: FileHandle) -> Option<Arc<AsyncMutex<DirCursor>>> {
        guard(&self.dirs).get(&fh.0).cloned()
    }

    async fn observe(&self) -> Result<Snapshot, fuser::Errno> {
        self.mount
            .authority()
            .observe_latest()
            .await
            .map_err(|e| core_errno(&e))
    }

    /// Attributes and size from one pinned observation.
    async fn snapshot_attr(
        &self,
        snapshot: &Snapshot,
        id: NodeId,
    ) -> Result<FileAttr, fuser::Errno> {
        let node = snapshot
            .node(id)
            .await
            .map_err(|e| core_errno(&e))?
            .ok_or(fuser::Errno::ENOENT)?;
        let info = CachedInfo {
            directory: node.is_directory(),
            executable: node.executable(),
            generation: node.generation(),
        };
        let ino = self.inode_for(id, Some(info))?;
        let size = match node.kind() {
            yinyang_core::NodeKind::File(content) => content.content_id().length(),
            yinyang_core::NodeKind::Directory { .. } => DIRECTORY_SIZE,
        };
        Ok(attribute(
            ino,
            info.directory,
            info.executable,
            size,
            snapshot.revision(),
        ))
    }

    /// Attributes for an open handle: the staged length and the base
    /// observation are authoritative, so `fstat`, path stats and read bytes
    /// describe the same pinned version.
    async fn handle_attr(&self, open: &OpenFile) -> Result<FileAttr, fuser::Errno> {
        let status = open.file.status().await.map_err(|e| runtime_errno(&e))?;
        let ino = self.inode_for(open.node, None)?;
        Ok(attribute(
            ino,
            open.base.directory,
            open.base.executable,
            status.length,
            status.remote_revision,
        ))
    }

    /// Metadata for one inode. While any open handle exists, the staged/base
    /// version wins over the latest remote observation so the kernel attr
    /// cache cannot mix an external size with staged bytes for the same inode.
    async fn attr_for(&self, snapshot: &Snapshot, id: NodeId) -> Result<FileAttr, fuser::Errno> {
        if let Some(open) = self.open_for_node(id) {
            self.handle_attr(&open).await
        } else {
            self.snapshot_attr(snapshot, id).await
        }
    }

    async fn lookup(&self, parent: INodeNo, name: String, reply: ReplyEntry) {
        let Some(parent_id) = self.node(parent) else {
            return reply.error(fuser::Errno::ESTALE);
        };
        let snapshot = match self.observe().await {
            Ok(snapshot) => snapshot,
            Err(errno) => return reply.error(errno),
        };
        let entry = match snapshot.lookup(parent_id, &name).await {
            Ok(Some(entry)) => entry,
            Ok(None) => return reply.error(fuser::Errno::ENOENT),
            Err(error) => return reply.error(core_errno(&error)),
        };
        match self.attr_for(&snapshot, entry.node_id).await {
            Ok(attr) => {
                self.remember_lookup(attr.ino);
                reply.entry(&ATTR_TTL, &attr, Generation(0));
            }
            Err(errno) => reply.error(errno),
        }
    }

    async fn getattr(&self, ino: INodeNo, fh: Option<FileHandle>, reply: ReplyAttr) {
        if let Some(fh) = fh
            && let Some(open) = self.open_file(fh)
        {
            return match self.handle_attr(&open).await {
                Ok(attr) => reply.attr(&ATTR_TTL, &attr),
                Err(errno) => reply.error(errno),
            };
        }
        let Some(id) = self.node(ino) else {
            return reply.error(fuser::Errno::ESTALE);
        };
        let snapshot = match self.observe().await {
            Ok(snapshot) => snapshot,
            Err(errno) => return reply.error(errno),
        };
        match self.attr_for(&snapshot, id).await {
            Ok(attr) => reply.attr(&ATTR_TTL, &attr),
            Err(errno) => reply.error(errno),
        }
    }

    async fn open(&self, ino: INodeNo, flags: OpenFlags, reply: ReplyOpen) {
        if flags.acc_mode() != OpenAccMode::O_RDONLY {
            return reply.error(fuser::Errno::EROFS);
        }
        let Some(id) = self.node(ino) else {
            return reply.error(fuser::Errno::ESTALE);
        };
        if guard(&self.inodes)
            .cached(id)
            .is_some_and(|info| info.directory)
        {
            return reply.error(fuser::Errno::EISDIR);
        }
        match self.prepare_open(id).await {
            Ok(open) => {
                let fh = self.next_handle();
                guard(&self.files).insert(fh, Arc::new(open));
                reply.opened(FileHandle(fh), FopenFlags::FOPEN_DIRECT_IO);
            }
            Err(errno) => reply.error(errno),
        }
    }

    /// Materialize the shared staged version and capture its base observation.
    async fn prepare_open(&self, id: NodeId) -> Result<OpenFile, fuser::Errno> {
        let file = self
            .mount
            .open_node(id, false)
            .await
            .map_err(|e| runtime_errno(&e))?;
        let status = match file.status().await {
            Ok(status) => status,
            Err(error) => {
                drop(file);
                let _ = self.mount.reclaim(id).await;
                return Err(runtime_errno(&error));
            }
        };
        let snapshot = match self
            .mount
            .authority()
            .observe_revision(status.remote_revision)
            .await
        {
            Ok(snapshot) => snapshot,
            Err(error) => {
                drop(file);
                let _ = self.mount.reclaim(id).await;
                return Err(core_errno(&error));
            }
        };
        let node = match snapshot.node(id).await {
            Ok(Some(node)) => node,
            Ok(None) => {
                drop(file);
                let _ = self.mount.reclaim(id).await;
                return Err(fuser::Errno::ENOENT);
            }
            Err(error) => {
                drop(file);
                let _ = self.mount.reclaim(id).await;
                return Err(core_errno(&error));
            }
        };
        let base = CachedInfo {
            directory: node.is_directory(),
            executable: node.executable(),
            generation: node.generation(),
        };
        if base.directory {
            drop(file);
            let _ = self.mount.reclaim(id).await;
            return Err(fuser::Errno::EISDIR);
        }
        Ok(OpenFile {
            node: id,
            file,
            base,
        })
    }

    async fn read(&self, fh: FileHandle, offset: u64, size: u32, reply: ReplyData) {
        let Some(open) = self.open_file(fh) else {
            return reply.error(fuser::Errno::EBADF);
        };
        match open.file.read(offset, size as usize).await {
            Ok(bytes) => reply.data(&bytes),
            Err(error) => reply.error(runtime_errno(&error)),
        }
    }

    async fn release(&self, ino: INodeNo, fh: FileHandle, reply: ReplyEmpty) {
        let Some(removed) = guard(&self.files).remove(&fh.0) else {
            return reply.ok();
        };
        let node = removed.node;
        drop(removed);
        // Reclaim only the last clean reference so the next open materializes
        // the current remote revision. A busy result means an interrupted read
        // still shares the node; that pinned version stays coherent and is
        // reclaimed once a later release is the final reference.
        if let Err(error) = self.mount.reclaim(node).await
            && error.kind() != crate::runtime::ErrorKind::Busy
        {
            eprintln!("yy: fuse release cleanup inode {ino}: {error}");
        }
        reply.ok();
    }

    async fn opendir(&self, ino: INodeNo, reply: ReplyOpen) {
        let Some(id) = self.node(ino) else {
            return reply.error(fuser::Errno::ESTALE);
        };
        let snapshot = match self.observe().await {
            Ok(snapshot) => snapshot,
            Err(errno) => return reply.error(errno),
        };
        let node = match snapshot.node(id).await {
            Ok(Some(node)) => node,
            Ok(None) => return reply.error(fuser::Errno::ENOENT),
            Err(error) => return reply.error(core_errno(&error)),
        };
        if !node.is_directory() {
            return reply.error(fuser::Errno::ENOTDIR);
        }
        let parent_ino = match node.link() {
            Some(link) => match self.inode_for(link.parent, None) {
                Ok(ino) => ino,
                Err(errno) => return reply.error(errno),
            },
            // The root's "." and ".." both denote itself.
            None => INodeNo::ROOT,
        };
        guard(&self.inodes).cache(
            id,
            CachedInfo {
                directory: true,
                executable: false,
                generation: node.generation(),
            },
        );
        let fh = self.next_handle();
        let cursor = Arc::new(AsyncMutex::new(DirCursor {
            snapshot,
            ino,
            directory: id,
            parent_ino,
            entries: Vec::new(),
            token: None,
            complete: false,
        }));
        guard(&self.dirs).insert(fh, cursor);
        reply.opened(FileHandle(fh), FopenFlags::empty());
    }

    async fn readdir(&self, fh: FileHandle, offset: u64, mut reply: ReplyDirectory) {
        let Some(handle) = self.dir_cursor(fh) else {
            return reply.error(fuser::Errno::EBADF);
        };
        let mut cursor = handle.lock().await;
        if offset == 0 {
            cursor.entries.clear();
            cursor.token = None;
            cursor.complete = false;
        }
        let mut index = offset as usize;
        let mut added = false;
        loop {
            if index == 0 {
                if reply.add(cursor.ino, 1, FileType::Directory, ".") {
                    break;
                }
                added = true;
                index = 1;
                continue;
            }
            if index == 1 {
                if reply.add(cursor.parent_ino, 2, FileType::Directory, "..") {
                    break;
                }
                added = true;
                index = 2;
                continue;
            }
            let local = index - 2;
            if local >= cursor.entries.len() {
                if cursor.complete {
                    break;
                }
                let before = cursor.entries.len();
                if let Err(errno) = self.load_page(&mut cursor).await {
                    // Verified entries stay visible; a mid-stream failure
                    // resumes from the committed offset on the next call.
                    if added || cursor.entries.len() > before {
                        break;
                    }
                    return reply.error(errno);
                }
                continue;
            }
            let item = cursor.entries[local].clone();
            if reply.add(item.ino, index as u64 + 1, item.kind, &item.name) {
                break;
            }
            added = true;
            index += 1;
        }
        reply.ok();
    }

    async fn releasedir(&self, fh: FileHandle, reply: ReplyEmpty) {
        guard(&self.dirs).remove(&fh.0);
        reply.ok();
    }

    async fn load_page(&self, cursor: &mut DirCursor) -> Result<(), fuser::Errno> {
        let page = cursor
            .snapshot
            .scan(cursor.directory, cursor.token.as_ref(), READDIR_PAGE)
            .await
            .map_err(|e| core_errno(&e))?;
        cursor.token = page.next;
        cursor.complete = cursor.token.is_none();
        for entry in page.entries {
            let (ino, kind) = self.resolve_entry(&cursor.snapshot, entry.node_id).await?;
            cursor.entries.push(DirItem {
                ino,
                kind,
                name: entry.name,
            });
        }
        Ok(())
    }

    async fn resolve_entry(
        &self,
        snapshot: &Snapshot,
        id: NodeId,
    ) -> Result<(INodeNo, FileType), fuser::Errno> {
        // Release the inode-table guard before `inode_for` reacquires it;
        // an `if let` scrutinee would keep the temporary guard alive.
        let cached = { guard(&self.inodes).cached(id) };
        if let Some(info) = cached {
            let ino = self.inode_for(id, None)?;
            return Ok((ino, entry_kind(info.directory)));
        }
        let node = snapshot
            .node(id)
            .await
            .map_err(|e| core_errno(&e))?
            .ok_or(fuser::Errno::EIO)?;
        let info = CachedInfo {
            directory: node.is_directory(),
            executable: node.executable(),
            generation: node.generation(),
        };
        let ino = self.inode_for(id, Some(info))?;
        Ok((ino, entry_kind(info.directory)))
    }
}

fn entry_kind(directory: bool) -> FileType {
    if directory {
        FileType::Directory
    } else {
        FileType::RegularFile
    }
}

fn guard<T>(lock: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn runtime_errno(error: &crate::runtime::Error) -> fuser::Errno {
    use crate::runtime::ErrorKind;
    match error.kind() {
        ErrorKind::NotFound => fuser::Errno::ENOENT,
        ErrorKind::AlreadyExists => fuser::Errno::EEXIST,
        ErrorKind::NotDirectory => fuser::Errno::ENOTDIR,
        ErrorKind::IsDirectory => fuser::Errno::EISDIR,
        ErrorKind::NotEmpty => fuser::Errno::ENOTEMPTY,
        ErrorKind::ReadOnly => fuser::Errno::EROFS,
        ErrorKind::PermissionDenied => fuser::Errno::EACCES,
        ErrorKind::Busy | ErrorKind::Frozen => fuser::Errno::EBUSY,
        ErrorKind::TooLarge => fuser::Errno::EFBIG,
        ErrorKind::NoSpace => fuser::Errno::ENOSPC,
        ErrorKind::Invalid | ErrorKind::InvalidName => fuser::Errno::EINVAL,
        ErrorKind::Unsupported => fuser::Errno::ENOSYS,
        ErrorKind::Closed
        | ErrorKind::Conflict
        | ErrorKind::Retryable
        | ErrorKind::Unknown
        | ErrorKind::Corrupt
        | ErrorKind::Storage
        | ErrorKind::Io
        | ErrorKind::Unclassified => fuser::Errno::EIO,
    }
}

fn core_errno(error: &yinyang_core::Error) -> fuser::Errno {
    use yinyang_core::ErrorKind;
    match error.kind() {
        ErrorKind::NotFound => fuser::Errno::ENOENT,
        ErrorKind::AlreadyExists => fuser::Errno::EEXIST,
        ErrorKind::NotDirectory => fuser::Errno::ENOTDIR,
        ErrorKind::IsDirectory => fuser::Errno::EISDIR,
        ErrorKind::NotEmpty => fuser::Errno::ENOTEMPTY,
        ErrorKind::PermissionDenied => fuser::Errno::EACCES,
        ErrorKind::Invalid | ErrorKind::InvalidName => fuser::Errno::EINVAL,
        ErrorKind::Unsupported => fuser::Errno::ENOSYS,
        ErrorKind::Corrupt | ErrorKind::Storage | ErrorKind::Io => fuser::Errno::EIO,
    }
}

/// The filesystem object handed to the fuser session.
pub(super) struct FuseFrontend {
    pub(super) inner: Arc<Inner>,
}

impl Filesystem for FuseFrontend {
    fn init(&mut self, _req: &Request, config: &mut KernelConfig) -> std::io::Result<()> {
        // FILE_OPS makes the kernel carry the open handle on getattr, which
        // lets fstat report the staged version; PARALLEL_DIROPS admits
        // concurrent lookup/readdir, which the adapter already supports.
        let _ = config.add_capabilities(InitFlags::FUSE_FILE_OPS | InitFlags::FUSE_PARALLEL_DIROPS);
        let _ = config.set_time_granularity(std::time::Duration::from_secs(1));
        Ok(())
    }

    fn destroy(&mut self) {}

    fn lookup(&self, _req: &Request, parent: INodeNo, name: &std::ffi::OsStr, reply: ReplyEntry) {
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

    fn setattr(
        &self,
        _req: &Request,
        _ino: INodeNo,
        _mode: Option<u32>,
        _uid: Option<u32>,
        _gid: Option<u32>,
        _size: Option<u64>,
        _atime: Option<TimeOrNow>,
        _mtime: Option<TimeOrNow>,
        _ctime: Option<SystemTime>,
        _fh: Option<FileHandle>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<BsdFileFlags>,
        reply: ReplyAttr,
    ) {
        reply.error(fuser::Errno::EROFS);
    }

    fn readlink(&self, _req: &Request, _ino: INodeNo, reply: ReplyData) {
        reply.error(fuser::Errno::EINVAL);
    }

    fn mknod(
        &self,
        _req: &Request,
        _parent: INodeNo,
        _name: &std::ffi::OsStr,
        _mode: u32,
        _umask: u32,
        _rdev: u32,
        reply: ReplyEntry,
    ) {
        reply.error(fuser::Errno::EROFS);
    }

    fn mkdir(
        &self,
        _req: &Request,
        _parent: INodeNo,
        _name: &std::ffi::OsStr,
        _mode: u32,
        _umask: u32,
        reply: ReplyEntry,
    ) {
        reply.error(fuser::Errno::EROFS);
    }

    fn unlink(&self, _req: &Request, _parent: INodeNo, _name: &std::ffi::OsStr, reply: ReplyEmpty) {
        reply.error(fuser::Errno::EROFS);
    }

    fn rmdir(&self, _req: &Request, _parent: INodeNo, _name: &std::ffi::OsStr, reply: ReplyEmpty) {
        reply.error(fuser::Errno::EROFS);
    }

    fn symlink(
        &self,
        _req: &Request,
        _parent: INodeNo,
        _link_name: &std::ffi::OsStr,
        _target: &std::path::Path,
        reply: ReplyEntry,
    ) {
        reply.error(fuser::Errno::EROFS);
    }

    fn rename(
        &self,
        _req: &Request,
        _parent: INodeNo,
        _name: &std::ffi::OsStr,
        _newparent: INodeNo,
        _newname: &std::ffi::OsStr,
        _flags: fuser::RenameFlags,
        reply: ReplyEmpty,
    ) {
        reply.error(fuser::Errno::EROFS);
    }

    fn link(
        &self,
        _req: &Request,
        _ino: INodeNo,
        _newparent: INodeNo,
        _newname: &std::ffi::OsStr,
        reply: ReplyEntry,
    ) {
        reply.error(fuser::Errno::EROFS);
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

    fn write(
        &self,
        _req: &Request,
        _ino: INodeNo,
        _fh: FileHandle,
        _offset: u64,
        _data: &[u8],
        _write_flags: WriteFlags,
        _flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        reply: ReplyWrite,
    ) {
        reply.error(fuser::Errno::EROFS);
    }

    fn flush(
        &self,
        _req: &Request,
        _ino: INodeNo,
        _fh: FileHandle,
        _lock_owner: LockOwner,
        reply: ReplyEmpty,
    ) {
        reply.ok();
    }

    fn fsync(
        &self,
        _req: &Request,
        _ino: INodeNo,
        _fh: FileHandle,
        _datasync: bool,
        reply: ReplyEmpty,
    ) {
        reply.ok();
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
        // No local capacity claim is implied by a remote snapshot.
        reply.statfs(0, 0, 0, 0, 0, 4096, 255, 0);
    }

    fn setxattr(
        &self,
        _req: &Request,
        _ino: INodeNo,
        _name: &std::ffi::OsStr,
        _value: &[u8],
        _flags: i32,
        _position: u32,
        reply: ReplyEmpty,
    ) {
        reply.error(fuser::Errno::EROFS);
    }

    fn getxattr(
        &self,
        _req: &Request,
        _ino: INodeNo,
        _name: &std::ffi::OsStr,
        _size: u32,
        reply: ReplyXattr,
    ) {
        reply.error(fuser::Errno::EOPNOTSUPP);
    }

    fn listxattr(&self, _req: &Request, _ino: INodeNo, _size: u32, reply: ReplyXattr) {
        reply.error(fuser::Errno::EOPNOTSUPP);
    }

    fn removexattr(
        &self,
        _req: &Request,
        _ino: INodeNo,
        _name: &std::ffi::OsStr,
        reply: ReplyEmpty,
    ) {
        reply.error(fuser::Errno::EROFS);
    }

    fn access(&self, _req: &Request, _ino: INodeNo, mask: AccessFlags, reply: ReplyEmpty) {
        if mask.contains(AccessFlags::W_OK) {
            reply.error(fuser::Errno::EROFS);
        } else {
            reply.ok();
        }
    }

    fn create(
        &self,
        _req: &Request,
        _parent: INodeNo,
        _name: &std::ffi::OsStr,
        _mode: u32,
        _umask: u32,
        _flags: i32,
        reply: fuser::ReplyCreate,
    ) {
        reply.error(fuser::Errno::EROFS);
    }

    fn forget(&self, _req: &Request, ino: INodeNo, nlookup: u64) {
        self.inner.forget(ino, nlookup);
    }

    fn release(
        &self,
        _req: &Request,
        ino: INodeNo,
        fh: FileHandle,
        _flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        let inner = self.inner.clone();
        self.inner
            .spawn(async move { inner.release(ino, fh, reply).await });
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
        reply.error(fuser::Errno::EROFS);
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
    use super::{Inner, guard};
    use crate::core::{BackendProfile, Fs};
    use crate::mount::Mount;
    use crate::runtime::Runtime;
    use crate::support;
    use std::sync::Arc;
    use yinyang_core::Authority;

    /// An open read-only handle pins the staged version: while the file table
    /// holds a reference, path stats and fstat both report the base length,
    /// and a later external version is only visible after release.
    #[tokio::test]
    async fn active_handle_pins_metadata_until_release() {
        let temp = tempfile::tempdir().unwrap();
        let backend = support::TestBackend::default();
        Fs::create(backend.operator(), BackendProfile::Minio)
            .await
            .unwrap();
        let authority: Arc<dyn Authority> = Arc::new(
            Fs::open(backend.operator(), BackendProfile::Minio)
                .await
                .unwrap(),
        );

        let published = temp.path().join("published");
        let runtime = Runtime::open(authority.clone(), &published).await.unwrap();
        runtime
            .create_file(runtime.authority().root(), "file")
            .await
            .unwrap();
        let id = authority
            .observe_latest()
            .await
            .unwrap()
            .resolve("file")
            .await
            .unwrap()
            .unwrap()
            .id();
        let mut first = runtime.open_file("file", true).await.unwrap();
        first.write(0, b"first version").await.unwrap();
        first.fsync().await.unwrap();
        first.close().await.unwrap();
        drop(runtime);

        let mount = Mount::open(authority.clone(), temp.path().join("ro"), true)
            .await
            .unwrap();
        let inner = Inner::new(mount, tokio::runtime::Handle::current());
        let fh = inner.next_handle();
        let open = inner.prepare_open(id).await.unwrap();
        guard(&inner.files).insert(fh, Arc::new(open));

        let other = temp.path().join("other");
        let writer = Runtime::open(authority.clone(), &other).await.unwrap();
        let mut second = writer.open_file("file", true).await.unwrap();
        second.write(0, b"second version").await.unwrap();
        second.fsync().await.unwrap();

        let latest = authority.observe_latest().await.unwrap();
        let pinned = inner.attr_for(&latest, id).await.unwrap();
        assert_eq!(pinned.size, "first version".len() as u64);
        let open = inner.open_for_node(id).unwrap();
        assert_eq!(inner.handle_attr(&open).await.unwrap().size, pinned.size);
        assert_eq!(inner.handle_attr(&open).await.unwrap().perm, 0o444);

        // Release the only reference: clean staging is reclaimed and the next
        // observation reports the externally published second version.
        drop(second);
        drop(writer);
        drop(open);
        drop(guard(&inner.files).remove(&fh).unwrap());
        inner.mount.reclaim(id).await.unwrap();
        let latest = authority.observe_latest().await.unwrap();
        let refreshed = inner.attr_for(&latest, id).await.unwrap();
        assert_eq!(refreshed.size, "second version".len() as u64);
    }
}
