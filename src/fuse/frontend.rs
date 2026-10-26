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
    INodeNo, InitFlags, KernelConfig, LockOwner, OpenAccMode, OpenFlags, RenameFlags, ReplyAttr,
    ReplyCreate, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyOpen, ReplyStatfs,
    ReplyWrite, ReplyXattr, Request, TimeOrNow, WriteFlags,
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
    read_only: bool,
}

/// One kernel open maps to one reference on the node's shared staged version.
struct OpenFile {
    node: NodeId,
    file: MountFile,
    writable: bool,
}

fn has_open_flag(flags: i32, flag: nix::fcntl::OFlag) -> bool {
    flags & flag.bits() != 0
}

fn writable_open(flags: OpenFlags) -> bool {
    flags.acc_mode() != OpenAccMode::O_RDONLY
}

fn representable_mode(
    mode: u32,
    umask: u32,
    read_only: bool,
    directory: bool,
    executable: bool,
) -> bool {
    const S_IFMT: u32 = 0o170000;
    const S_IFREG: u32 = 0o100000;
    const S_IFDIR: u32 = 0o040000;
    let file_type = mode & S_IFMT;
    let type_ok = file_type == 0
        || (directory && file_type == S_IFDIR)
        || (!directory && file_type == S_IFREG);
    let special_bits = mode & !(S_IFMT | 0o777);
    if !type_ok || special_bits != 0 {
        return false;
    }
    let effective = mode & !umask & 0o777;
    let expected = if directory || executable {
        if read_only { 0o555 } else { 0o755 }
    } else {
        if read_only { 0o444 } else { 0o644 }
    };
    effective == expected
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
    pub(super) fn with_read_only(mount: Mount, runtime: Handle, read_only: bool) -> Self {
        let root = mount.authority().root();
        Self {
            mount,
            inodes: Mutex::new(InodeTable::new(root)),
            files: Mutex::new(BTreeMap::new()),
            dirs: Mutex::new(BTreeMap::new()),
            next_handle: AtomicU64::new(0),
            runtime,
            read_only,
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

    /// Drop a stale clean version before opening. Busy means another reference
    /// still pins the shared version, or the node retains dirty/frozen/error
    /// state; both must be reused rather than reset.
    async fn reclaim_clean_state(&self, id: NodeId) -> Result<(), fuser::Errno> {
        match self.mount.reclaim(id).await {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == crate::runtime::ErrorKind::Busy => Ok(()),
            Err(error) => Err(runtime_errno(&error)),
        }
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
            self.read_only,
        ))
    }

    /// Attributes for an open handle: the staged length is authoritative so
    /// `fstat` and read bytes describe the same pinned version.
    async fn handle_attr(&self, open: &OpenFile) -> Result<FileAttr, fuser::Errno> {
        self.shared_attr(open.node).await?.ok_or(fuser::Errno::EIO)
    }

    /// Attributes for a node with a shared Mount handle. The staged length and
    /// the handle's base revision come from the same record, and kind/executable
    /// are read from that same revision, so path `stat` cannot disagree with
    /// `fstat`/read bytes while the node is pinned or dirty.
    async fn shared_attr(&self, id: NodeId) -> Result<Option<FileAttr>, fuser::Errno> {
        self.reclaim_clean_state(id).await?;
        let status = match self.mount.node_status(id).await {
            Ok(Some(status)) => status,
            Ok(None) => return Ok(None),
            Err(error) => return Err(runtime_errno(&error)),
        };
        let snapshot = self
            .mount
            .authority()
            .observe_revision(status.remote_revision)
            .await
            .map_err(|e| core_errno(&e))?;
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
        Ok(Some(attribute(
            ino,
            info.directory,
            info.executable,
            status.length,
            status.remote_revision,
            self.read_only,
        )))
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
        match self.shared_attr(entry.node_id).await {
            Ok(Some(attr)) => {
                self.remember_lookup(attr.ino);
                reply.entry(&ATTR_TTL, &attr, Generation(0));
            }
            Ok(None) => match self.snapshot_attr(&snapshot, entry.node_id).await {
                Ok(attr) => {
                    self.remember_lookup(attr.ino);
                    reply.entry(&ATTR_TTL, &attr, Generation(0));
                }
                Err(errno) => reply.error(errno),
            },
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
        match self.shared_attr(id).await {
            Ok(Some(attr)) => reply.attr(&ATTR_TTL, &attr),
            Ok(None) => match self.snapshot_attr(&snapshot, id).await {
                Ok(attr) => reply.attr(&ATTR_TTL, &attr),
                Err(errno) => reply.error(errno),
            },
            Err(errno) => reply.error(errno),
        }
    }

    async fn open(&self, ino: INodeNo, flags: OpenFlags, reply: ReplyOpen) {
        let Some(id) = self.node(ino) else {
            return reply.error(fuser::Errno::ESTALE);
        };
        if guard(&self.inodes)
            .cached(id)
            .is_some_and(|info| info.directory)
        {
            return reply.error(fuser::Errno::EISDIR);
        }
        let writable = writable_open(flags);
        let truncate = has_open_flag(flags.0, nix::fcntl::OFlag::O_TRUNC);
        if truncate && !writable {
            return reply.error(fuser::Errno::EINVAL);
        }
        if let Err(errno) = self.reclaim_clean_state(id).await {
            return reply.error(errno);
        }
        match self.mount.open_node(id, writable).await {
            Ok(file) => {
                if truncate && let Err(error) = file.truncate(0).await {
                    return reply.error(runtime_errno(&error));
                }
                let fh = self.next_handle();
                guard(&self.files).insert(
                    fh,
                    Arc::new(OpenFile {
                        node: id,
                        file,
                        writable,
                    }),
                );
                reply.opened(FileHandle(fh), FopenFlags::FOPEN_DIRECT_IO);
            }
            Err(error) => reply.error(runtime_errno(&error)),
        }
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

    async fn write(
        &self,
        fh: FileHandle,
        offset: u64,
        data: Vec<u8>,
        flags: OpenFlags,
        reply: ReplyWrite,
    ) {
        let Some(open) = self.open_file(fh) else {
            return reply.error(fuser::Errno::EBADF);
        };
        if !open.writable {
            return reply.error(fuser::Errno::EBADF);
        }
        // Linux may toggle these with fcntl(F_SETFL), so the flags on this
        // WRITE request, not the ones captured by open, define its semantics.
        let append = has_open_flag(flags.0, nix::fcntl::OFlag::O_APPEND);
        let sync = has_open_flag(flags.0, nix::fcntl::OFlag::O_SYNC)
            || has_open_flag(flags.0, nix::fcntl::OFlag::O_DSYNC);
        let staged = if append {
            open.file.append(&data).await.map(|_| ())
        } else {
            open.file.write(offset, &data).await
        };
        match staged {
            Ok(()) => {
                if sync && let Err(error) = open.file.flush().await {
                    return reply.error(runtime_errno(&error));
                }
                reply.written(data.len() as u32);
            }
            Err(error) => reply.error(runtime_errno(&error)),
        }
    }

    async fn flush(&self, fh: FileHandle, reply: ReplyEmpty) {
        let Some(open) = self.open_file(fh) else {
            return reply.error(fuser::Errno::EBADF);
        };
        if !open.writable {
            // A read-only reference must not publish another writer's staged
            // bytes through its close path.
            return reply.ok();
        }
        match open.file.flush().await {
            Ok(_) => reply.ok(),
            Err(error) => reply.error(runtime_errno(&error)),
        }
    }

    async fn fsync(&self, fh: FileHandle, reply: ReplyEmpty) {
        let Some(open) = self.open_file(fh) else {
            return reply.error(fuser::Errno::EBADF);
        };
        if !open.writable {
            return reply.ok();
        }
        match open.file.fsync().await {
            Ok(_) => reply.ok(),
            Err(error) => reply.error(runtime_errno(&error)),
        }
    }

    async fn status_attr(&self, id: NodeId) -> Result<FileAttr, fuser::Errno> {
        self.shared_attr(id).await?.ok_or(fuser::Errno::ESTALE)
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
        ctime: Option<SystemTime>,
        crtime: Option<SystemTime>,
        chgtime: Option<SystemTime>,
        bkuptime: Option<SystemTime>,
        flags: Option<BsdFileFlags>,
        fh: Option<FileHandle>,
        reply: ReplyAttr,
    ) {
        let supported_mutation = size.is_some() || mode.is_some();
        let kernel_timestamps = atime.is_some()
            || mtime.is_some()
            || ctime.is_some()
            || crtime.is_some()
            || chgtime.is_some()
            || bkuptime.is_some();
        if uid.is_some()
            || gid.is_some()
            || flags.is_some()
            || (!supported_mutation && kernel_timestamps)
            || (size.is_some() && mode.is_some())
        {
            return reply.error(fuser::Errno::EOPNOTSUPP);
        }
        let Some(id) = self.node(ino) else {
            return reply.error(fuser::Errno::ESTALE);
        };
        if let Some(length) = size {
            let attr = if let Some(fh) = fh {
                let Some(open) = self.open_file(fh) else {
                    return reply.error(fuser::Errno::EBADF);
                };
                if let Err(error) = open.file.truncate(length).await {
                    return reply.error(runtime_errno(&error));
                }
                match self.handle_attr(&open).await {
                    Ok(attr) => attr,
                    Err(errno) => return reply.error(errno),
                }
            } else {
                if let Err(errno) = self.reclaim_clean_state(id).await {
                    return reply.error(errno);
                }
                let file = match self.mount.open_node(id, true).await {
                    Ok(file) => file,
                    Err(error) => return reply.error(runtime_errno(&error)),
                };
                if let Err(error) = file.truncate(length).await {
                    return reply.error(runtime_errno(&error));
                }
                if let Err(error) = file.flush().await {
                    return reply.error(runtime_errno(&error));
                }
                let attr = match self.status_attr(id).await {
                    Ok(attr) => attr,
                    Err(errno) => return reply.error(errno),
                };
                drop(file);
                if let Err(error) = self.mount.reclaim(id).await
                    && error.kind() != crate::runtime::ErrorKind::Busy
                {
                    eprintln!("yy: fuse truncate cleanup inode {}: {error}", ino.0);
                }
                attr
            };
            return reply.attr(&ATTR_TTL, &attr);
        }
        if let Some(mode) = mode {
            let snapshot = match self.observe().await {
                Ok(snapshot) => snapshot,
                Err(errno) => return reply.error(errno),
            };
            let current = match snapshot.node(id).await {
                Ok(Some(node)) => node,
                Ok(None) => return reply.error(fuser::Errno::ENOENT),
                Err(error) => return reply.error(core_errno(&error)),
            };
            let executable = mode & 0o111 != 0;
            if !representable_mode(mode, 0, self.read_only, current.is_directory(), executable) {
                return reply.error(fuser::Errno::EOPNOTSUPP);
            }
            if !current.is_directory() {
                let result = if let Some(fh) = fh {
                    let Some(open) = self.open_file(fh) else {
                        return reply.error(fuser::Errno::EBADF);
                    };
                    open.file.set_executable(executable).await
                } else {
                    if let Err(errno) = self.reclaim_clean_state(id).await {
                        return reply.error(errno);
                    }
                    let file = match self.mount.open_node(id, true).await {
                        Ok(file) => file,
                        Err(error) => return reply.error(runtime_errno(&error)),
                    };
                    let result = file.set_executable(executable).await;
                    drop(file);
                    if let Err(error) = self.mount.reclaim(id).await
                        && error.kind() != crate::runtime::ErrorKind::Busy
                    {
                        eprintln!("yy: fuse chmod cleanup inode {}: {error}", ino.0);
                    }
                    result
                };
                if let Err(error) = result {
                    return reply.error(runtime_errno(&error));
                }
            }
            let snapshot = match self.observe().await {
                Ok(snapshot) => snapshot,
                Err(errno) => return reply.error(errno),
            };
            return match self.snapshot_attr(&snapshot, id).await {
                Ok(attr) => reply.attr(&ATTR_TTL, &attr),
                Err(errno) => reply.error(errno),
            };
        }
        // A no-op setattr is valid and reports current attributes.
        let snapshot = match self.observe().await {
            Ok(snapshot) => snapshot,
            Err(errno) => return reply.error(errno),
        };
        match self.snapshot_attr(&snapshot, id).await {
            Ok(attr) => reply.attr(&ATTR_TTL, &attr),
            Err(errno) => reply.error(errno),
        }
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
        let Some(parent_id) = self.node(parent) else {
            return reply.error(fuser::Errno::ESTALE);
        };
        if !representable_mode(mode, umask, self.read_only, false, mode & 0o111 != 0) {
            return reply.error(fuser::Errno::EOPNOTSUPP);
        }
        let exclusive = has_open_flag(flags, nix::fcntl::OFlag::O_EXCL);
        let node = match self
            .mount
            .namespace()
            .create_file_with_node(parent_id, &name, mode & 0o111 != 0)
            .await
        {
            Ok((node, _)) => Some(node),
            Err(error)
                if !exclusive && error.kind() == crate::runtime::ErrorKind::AlreadyExists =>
            {
                let snapshot = match self.observe().await {
                    Ok(snapshot) => snapshot,
                    Err(errno) => return reply.error(errno),
                };
                match snapshot.lookup(parent_id, &name).await {
                    Ok(Some(entry)) => Some(entry.node_id),
                    Ok(None) => return reply.error(fuser::Errno::EEXIST),
                    Err(error) => return reply.error(core_errno(&error)),
                }
            }
            Err(error) => return reply.error(runtime_errno(&error)),
        };
        let node = node.expect("create resolved a node id");
        if let Err(errno) = self.reclaim_clean_state(node).await {
            return reply.error(errno);
        }
        let open = match self
            .mount
            .open_node(node, writable_open(OpenFlags(flags)))
            .await
        {
            Ok(file) => file,
            Err(error) => return reply.error(runtime_errno(&error)),
        };
        if has_open_flag(flags, nix::fcntl::OFlag::O_TRUNC) && !writable_open(OpenFlags(flags)) {
            return reply.error(fuser::Errno::EINVAL);
        }
        if has_open_flag(flags, nix::fcntl::OFlag::O_TRUNC)
            && let Err(error) = open.truncate(0).await
        {
            return reply.error(runtime_errno(&error));
        }
        let snapshot = match self.observe().await {
            Ok(snapshot) => snapshot,
            Err(errno) => return reply.error(errno),
        };
        let attr = match self.snapshot_attr(&snapshot, node).await {
            Ok(attr) => attr,
            Err(errno) => return reply.error(errno),
        };
        self.remember_lookup(attr.ino);
        let fh = self.next_handle();
        guard(&self.files).insert(
            fh,
            Arc::new(OpenFile {
                node,
                file: open,
                writable: writable_open(OpenFlags(flags)),
            }),
        );
        reply.created(
            &ATTR_TTL,
            &attr,
            Generation(0),
            FileHandle(fh),
            FopenFlags::FOPEN_DIRECT_IO,
        );
    }

    async fn mkdir(&self, parent: INodeNo, name: String, mode: u32, umask: u32, reply: ReplyEntry) {
        let Some(parent_id) = self.node(parent) else {
            return reply.error(fuser::Errno::ESTALE);
        };
        if !representable_mode(mode, umask, self.read_only, true, false) {
            return reply.error(fuser::Errno::EOPNOTSUPP);
        }
        let node = match self
            .mount
            .namespace()
            .create_directory_with_node(parent_id, &name)
            .await
        {
            Ok((node, _)) => node,
            Err(error) => return reply.error(runtime_errno(&error)),
        };
        let snapshot = match self.observe().await {
            Ok(snapshot) => snapshot,
            Err(errno) => return reply.error(errno),
        };
        match self.snapshot_attr(&snapshot, node).await {
            Ok(attr) => {
                self.remember_lookup(attr.ino);
                reply.entry(&ATTR_TTL, &attr, Generation(0));
            }
            Err(errno) => reply.error(errno),
        }
    }

    async fn unlink(&self, parent: INodeNo, name: String, reply: ReplyEmpty) {
        let Some(parent_id) = self.node(parent) else {
            return reply.error(fuser::Errno::ESTALE);
        };
        match self.mount.namespace().unlink_path(parent_id, &name).await {
            Ok(_) => reply.ok(),
            Err(error) => reply.error(runtime_errno(&error)),
        }
    }

    async fn rmdir(&self, parent: INodeNo, name: String, reply: ReplyEmpty) {
        let Some(parent_id) = self.node(parent) else {
            return reply.error(fuser::Errno::ESTALE);
        };
        match self.mount.namespace().rmdir_path(parent_id, &name).await {
            Ok(_) => reply.ok(),
            Err(error) => reply.error(runtime_errno(&error)),
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
        let known = RenameFlags::RENAME_NOREPLACE
            | RenameFlags::RENAME_EXCHANGE
            | RenameFlags::RENAME_WHITEOUT;
        if flags.bits() & !known.bits() != 0 {
            return reply.error(fuser::Errno::EINVAL);
        }
        if flags.contains(RenameFlags::RENAME_EXCHANGE)
            || flags.contains(RenameFlags::RENAME_WHITEOUT)
        {
            return reply.error(fuser::Errno::EOPNOTSUPP);
        }
        let Some(parent_id) = self.node(parent) else {
            return reply.error(fuser::Errno::ESTALE);
        };
        let Some(new_parent_id) = self.node(new_parent) else {
            return reply.error(fuser::Errno::ESTALE);
        };
        let result = if flags.contains(RenameFlags::RENAME_NOREPLACE) {
            self.mount
                .namespace()
                .rename_path(parent_id, &name, new_parent_id, &new_name)
                .await
        } else {
            self.mount
                .namespace()
                .rename_path_replace(parent_id, &name, new_parent_id, &new_name)
                .await
        };
        match result {
            Ok(_) => reply.ok(),
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
        ino: INodeNo,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<TimeOrNow>,
        mtime: Option<TimeOrNow>,
        ctime: Option<SystemTime>,
        fh: Option<FileHandle>,
        crtime: Option<SystemTime>,
        chgtime: Option<SystemTime>,
        bkuptime: Option<SystemTime>,
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
        _name: &std::ffi::OsStr,
        _mode: u32,
        _umask: u32,
        _rdev: u32,
        reply: ReplyEntry,
    ) {
        reply.error(fuser::Errno::EOPNOTSUPP);
    }

    fn mkdir(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &std::ffi::OsStr,
        mode: u32,
        umask: u32,
        reply: ReplyEntry,
    ) {
        let Some(name) = name.to_str() else {
            return reply.error(fuser::Errno::EINVAL);
        };
        let name = name.to_owned();
        let inner = self.inner.clone();
        self.inner
            .spawn(async move { inner.mkdir(parent, name, mode, umask, reply).await });
    }

    fn unlink(&self, _req: &Request, parent: INodeNo, name: &std::ffi::OsStr, reply: ReplyEmpty) {
        let Some(name) = name.to_str() else {
            return reply.error(fuser::Errno::EINVAL);
        };
        let name = name.to_owned();
        let inner = self.inner.clone();
        self.inner
            .spawn(async move { inner.unlink(parent, name, reply).await });
    }

    fn rmdir(&self, _req: &Request, parent: INodeNo, name: &std::ffi::OsStr, reply: ReplyEmpty) {
        let Some(name) = name.to_str() else {
            return reply.error(fuser::Errno::EINVAL);
        };
        let name = name.to_owned();
        let inner = self.inner.clone();
        self.inner
            .spawn(async move { inner.rmdir(parent, name, reply).await });
    }

    fn symlink(
        &self,
        _req: &Request,
        _parent: INodeNo,
        _link_name: &std::ffi::OsStr,
        _target: &std::path::Path,
        reply: ReplyEntry,
    ) {
        reply.error(fuser::Errno::EOPNOTSUPP);
    }

    fn rename(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &std::ffi::OsStr,
        newparent: INodeNo,
        newname: &std::ffi::OsStr,
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
        _newname: &std::ffi::OsStr,
        reply: ReplyEntry,
    ) {
        reply.error(fuser::Errno::EOPNOTSUPP);
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
        reply.error(fuser::Errno::EOPNOTSUPP);
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
        reply.error(fuser::Errno::EOPNOTSUPP);
    }

    fn access(&self, _req: &Request, _ino: INodeNo, mask: AccessFlags, reply: ReplyEmpty) {
        if self.inner.read_only && mask.contains(AccessFlags::W_OK) {
            reply.error(fuser::Errno::EROFS);
        } else {
            reply.ok();
        }
    }

    fn create(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &std::ffi::OsStr,
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
        reply.error(fuser::Errno::EOPNOTSUPP);
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
    use crate::core::{BackendProfile, Fs};
    use crate::runtime::Runtime;
    use crate::support::TestBackend;
    use std::sync::Arc;
    use yinyang_core::Authority;

    #[test]
    fn representable_mode_accepts_linux_file_type_bits() {
        assert!(representable_mode(0o100644, 0, false, false, false));
        assert!(representable_mode(0o100755, 0, false, false, true));
        assert!(representable_mode(0o40755, 0, false, true, false));
        assert!(representable_mode(0o100644, 0o022, false, false, false));
        assert!(!representable_mode(0o100600, 0, false, false, false));
        assert!(!representable_mode(
            0o100644 | 0o4000,
            0,
            false,
            false,
            false
        ));
        assert!(!representable_mode(0o020000, 0, false, false, false));
        assert!(!representable_mode(0o40755, 0, false, false, false));
    }

    #[tokio::test]
    async fn dirty_shared_node_reports_staged_size_in_path_attrs() {
        let temp = tempfile::tempdir().unwrap();
        let backend = TestBackend::default();
        let authority: Arc<dyn Authority> = Arc::new(
            Fs::create(backend.operator(), BackendProfile::Minio)
                .await
                .unwrap(),
        );
        let mount = Mount::open(authority.clone(), temp.path(), false)
            .await
            .unwrap();
        let (id, _) = mount
            .namespace()
            .create_file_with_node(authority.root(), "file", false)
            .await
            .unwrap();
        let file = mount.open_node(id, true).await.unwrap();
        file.write(0, b"dirty").await.unwrap();
        let inner = Arc::new(Inner::with_read_only(
            mount,
            tokio::runtime::Handle::current(),
            false,
        ));
        let status = file.status().await.unwrap();
        let attr = inner.shared_attr(id).await.unwrap().unwrap();
        assert_eq!(attr.size, status.length);
        assert_eq!(attr.size, 5);
        assert_eq!(attr.ino, inner.inode_for(id, None).unwrap());
    }

    #[tokio::test]
    async fn stale_clean_path_attr_reclaims_to_latest() {
        let temp = tempfile::tempdir().unwrap();
        let backend = TestBackend::default();
        let authority: Arc<dyn Authority> = Arc::new(
            Fs::create(backend.operator(), BackendProfile::Minio)
                .await
                .unwrap(),
        );
        let writer = Runtime::open(authority.clone(), temp.path().join("writer"))
            .await
            .unwrap();
        writer.create_file(authority.root(), "file").await.unwrap();
        let mut file = writer.open_file("file", true).await.unwrap();
        file.write(0, b"first").await.unwrap();
        file.close().await.unwrap();
        drop(writer);
        let id = authority
            .observe_latest()
            .await
            .unwrap()
            .resolve("file")
            .await
            .unwrap()
            .unwrap()
            .id();

        let mount = Mount::open(authority.clone(), temp.path().join("mount"), false)
            .await
            .unwrap();
        let inner = Inner::with_read_only(mount, tokio::runtime::Handle::current(), false);
        let stale = inner.mount.open_node(id, false).await.unwrap();
        drop(stale);

        let writer = Runtime::open(authority.clone(), temp.path().join("writer2"))
            .await
            .unwrap();
        let mut file = writer.open_node(id, true).await.unwrap();
        file.write(0, b"second").await.unwrap();
        file.close().await.unwrap();

        assert!(inner.shared_attr(id).await.unwrap().is_none());
        let latest = authority.observe_latest().await.unwrap();
        let attr = inner.snapshot_attr(&latest, id).await.unwrap();
        assert_eq!(attr.size, 6);
    }

    #[tokio::test]
    async fn open_reclaims_stale_clean_version_after_transient_busy() {
        let temp = tempfile::tempdir().unwrap();
        let backend = TestBackend::default();
        let authority: Arc<dyn Authority> = Arc::new(
            Fs::create(backend.operator(), BackendProfile::Minio)
                .await
                .unwrap(),
        );
        let writer = Runtime::open(authority.clone(), temp.path().join("writer"))
            .await
            .unwrap();
        writer.create_file(authority.root(), "file").await.unwrap();
        let mut file = writer.open_file("file", true).await.unwrap();
        file.write(0, b"first").await.unwrap();
        file.close().await.unwrap();
        drop(writer);
        let id = authority
            .observe_latest()
            .await
            .unwrap()
            .resolve("file")
            .await
            .unwrap()
            .unwrap()
            .id();

        let mount = Mount::open(authority.clone(), temp.path().join("mount"), false)
            .await
            .unwrap();
        let inner = Inner::with_read_only(mount, tokio::runtime::Handle::current(), false);
        let pinned = inner.mount.open_node(id, false).await.unwrap();
        inner.reclaim_clean_state(id).await.unwrap();
        assert!(inner.mount.node_status(id).await.unwrap().is_some());
        drop(pinned);
        inner.reclaim_clean_state(id).await.unwrap();
        assert!(inner.mount.node_status(id).await.unwrap().is_none());

        let writer = Runtime::open(authority.clone(), temp.path().join("writer2"))
            .await
            .unwrap();
        let mut file = writer.open_node(id, true).await.unwrap();
        file.write(0, b"second").await.unwrap();
        file.close().await.unwrap();
        let fresh = inner.mount.open_node(id, false).await.unwrap();
        assert_eq!(fresh.read(0, 6).await.unwrap(), b"second");
    }
}
