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
    DirectFile, DirectId, DirectKind, DirectNamespace, DirectNode, DirectRevision, DirectSnapshot,
    Error as DirectError, ErrorKind as DirectErrorKind,
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
const STAMP_EPOCH: u64 = 1_600_000_000;

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

struct OpenFile {
    _file: DirectFile,
    attr: FileAttr,
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
    snapshot: RwLock<Arc<DirectSnapshot>>,
    refresh_error: Mutex<Option<String>>,
    inodes: Mutex<DirectInodeTable>,
    files: Mutex<BTreeMap<u64, Arc<OpenFile>>>,
    dirs: Mutex<BTreeMap<u64, Arc<Mutex<DirCursor>>>>,
    dentries: Mutex<BTreeSet<(u64, String)>>,
    notifier: Mutex<Option<Notifier>>,
    next_handle: AtomicU64,
    runtime: Handle,
}

impl DirectInner {
    fn new(namespace: DirectNamespace, snapshot: DirectSnapshot, runtime: Handle) -> Self {
        let root = snapshot.root();
        Self {
            namespace,
            snapshot: RwLock::new(Arc::new(snapshot)),
            refresh_error: Mutex::new(None),
            inodes: Mutex::new(DirectInodeTable::new(root)),
            files: Mutex::new(BTreeMap::new()),
            dirs: Mutex::new(BTreeMap::new()),
            dentries: Mutex::new(BTreeSet::new()),
            notifier: Mutex::new(None),
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
                eprintln!(
                    "yy: fuse refresh: Direct observation {revision} with {root_entries} root entries"
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
            &ATTR_TTL,
            &attribute(ino, node, snapshot.revision()),
            Generation(0),
        );
    }

    async fn getattr(&self, ino: INodeNo, fh: Option<FileHandle>, reply: ReplyAttr) {
        if let Some(fh) = fh
            && let Some(open) = guard(&self.files).get(&fh.0).cloned()
        {
            return reply.attr(&ATTR_TTL, &open.attr);
        }
        let Some(id) = self.node(ino) else {
            return reply.error(fuser::Errno::ESTALE);
        };
        let snapshot = match self.current() {
            Ok(snapshot) => snapshot,
            Err(errno) => return reply.error(errno),
        };
        let Some(node) = snapshot.node(id) else {
            return reply.error(fuser::Errno::ENOENT);
        };
        reply.attr(&ATTR_TTL, &attribute(ino, node, snapshot.revision()));
    }

    async fn open(&self, ino: INodeNo, flags: OpenFlags, reply: ReplyOpen) {
        if flags.acc_mode() != OpenAccMode::O_RDONLY
            || flags.0 & nix::fcntl::OFlag::O_TRUNC.bits() != 0
        {
            return reply.error(fuser::Errno::EROFS);
        }
        let Some(id) = self.node(ino) else {
            return reply.error(fuser::Errno::ESTALE);
        };
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
        let attr = attribute(ino, node, snapshot.revision());
        let fh = self.next_handle();
        guard(&self.files).insert(fh, Arc::new(OpenFile { _file: file, attr }));
        reply.opened(FileHandle(fh), FopenFlags::FOPEN_DIRECT_IO);
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
        match open._file.read(offset, size as usize).await {
            Ok(bytes) => reply.data(&bytes),
            Err(error) => reply.error(direct_errno(&error)),
        }
    }

    async fn release(&self, fh: FileHandle, reply: ReplyEmpty) {
        guard(&self.files).remove(&fh.0);
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
        let mut items = Vec::with_capacity(children.len());
        for node in children {
            let ino = match self.inode_for(node.id()) {
                Ok(ino) => ino.0,
                Err(errno) => return reply.error(errno),
            };
            items.push(DirItem {
                ino,
                kind: match node.kind() {
                    DirectKind::Directory => FileType::Directory,
                    DirectKind::File => FileType::RegularFile,
                },
                name: node.name().to_owned(),
            });
        }
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

fn attribute(ino: INodeNo, node: &DirectNode, revision: DirectRevision) -> FileAttr {
    use nix::unistd::{getegid, geteuid};
    let revision = revision.to_bytes();
    let stamp = std::time::SystemTime::UNIX_EPOCH
        + std::time::Duration::from_secs(
            STAMP_EPOCH
                + u64::from_be_bytes(revision[..8].try_into().expect("revision stamp prefix")),
        );
    let directory = node.is_directory();
    let size = if directory {
        DIRECTORY_SIZE
    } else {
        node.size()
    };
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
        perm: if directory { 0o555 } else { 0o444 },
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

fn guard<T>(lock: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

pub(super) async fn run(
    config: VolumeConfig,
    operator: opendal::Operator,
    mountpoint: std::path::PathBuf,
) -> Result<Outcome, Box<dyn std::error::Error>> {
    let namespace = DirectNamespace::new(operator)?;
    let snapshot = namespace.observe().await?;
    if !snapshot.root_node().is_directory() {
        return Err(std::io::Error::other("Direct namespace root is not a directory").into());
    }
    let inner = Arc::new(DirectInner::new(
        namespace,
        snapshot,
        tokio::runtime::Handle::current(),
    ));
    let filesystem = DirectFuseFrontend {
        inner: inner.clone(),
    };
    let mut options = Config::default();
    options.mount_options = vec![
        MountOption::RO,
        MountOption::FSName(format!("yinyang-{}", config.name)),
        MountOption::Subtype("yinyang".into()),
    ];
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
        "yy: mounted {} read-only Direct at {} (fuse.yinyang)",
        config.name,
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
        reply.statfs(0, 0, 0, 0, 0, 4096, 255, 0);
    }

    fn access(&self, _req: &Request, _ino: INodeNo, mask: AccessFlags, reply: ReplyEmpty) {
        if mask.contains(AccessFlags::W_OK) {
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
        _ino: INodeNo,
        _mode: Option<u32>,
        _uid: Option<u32>,
        _gid: Option<u32>,
        _size: Option<u64>,
        _atime: Option<TimeOrNow>,
        _mtime: Option<TimeOrNow>,
        _ctime: Option<std::time::SystemTime>,
        _fh: Option<FileHandle>,
        _crtime: Option<std::time::SystemTime>,
        _chgtime: Option<std::time::SystemTime>,
        _bkuptime: Option<std::time::SystemTime>,
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
        _name: &OsStr,
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
        _name: &OsStr,
        _mode: u32,
        _umask: u32,
        reply: ReplyEntry,
    ) {
        reply.error(fuser::Errno::EROFS);
    }

    fn unlink(&self, _req: &Request, _parent: INodeNo, _name: &OsStr, reply: ReplyEmpty) {
        reply.error(fuser::Errno::EROFS);
    }

    fn rmdir(&self, _req: &Request, _parent: INodeNo, _name: &OsStr, reply: ReplyEmpty) {
        reply.error(fuser::Errno::EROFS);
    }

    fn symlink(
        &self,
        _req: &Request,
        _parent: INodeNo,
        _link_name: &OsStr,
        _target: &std::path::Path,
        reply: ReplyEntry,
    ) {
        reply.error(fuser::Errno::EROFS);
    }

    fn rename(
        &self,
        _req: &Request,
        _parent: INodeNo,
        _name: &OsStr,
        _newparent: INodeNo,
        _newname: &OsStr,
        _flags: RenameFlags,
        reply: ReplyEmpty,
    ) {
        reply.error(fuser::Errno::EROFS);
    }

    fn link(
        &self,
        _req: &Request,
        _ino: INodeNo,
        _newparent: INodeNo,
        _newname: &OsStr,
        reply: ReplyEntry,
    ) {
        reply.error(fuser::Errno::EROFS);
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
        _parent: INodeNo,
        _name: &OsStr,
        _mode: u32,
        _umask: u32,
        _flags: i32,
        reply: ReplyCreate,
    ) {
        reply.error(fuser::Errno::EROFS);
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
