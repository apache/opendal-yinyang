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

//! The single `fuser::Filesystem` adapter over a metadata backend.

use super::attrs::{ObservedAttr, synthesize};
use super::backend::{Backend, BackendError, Result, SetAttrRequest};
use super::flags::{has_open_flag, rename_replace, writable_open};
use super::inode::{CachedInfo, InodeTable};
use fuser::{
    AccessFlags, BsdFileFlags, FileAttr, FileHandle, FileType, Filesystem, FopenFlags, Generation,
    INodeNo, InitFlags, KernelConfig, LockOwner, Notifier, OpenFlags, RenameFlags, ReplyAttr,
    ReplyCreate, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyOpen, ReplyStatfs,
    ReplyWrite, ReplyXattr, Request, TimeOrNow, WriteFlags,
};
use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsStr;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::SystemTime;
use tokio::runtime::Handle;
use tokio::sync::Mutex as AsyncMutex;

pub(super) struct Adapter<B: Backend> {
    inner: Arc<AdapterInner<B>>,
}

impl<B: Backend> Clone for Adapter<B> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

struct AdapterInner<B: Backend> {
    backend: Arc<B>,
    inodes: Mutex<InodeTable<B::Inode>>,
    files: Mutex<BTreeMap<u64, Arc<OpenFile<B>>>>,
    dirs: Mutex<BTreeMap<u64, Arc<AsyncMutex<DirCursor<B>>>>>,
    dentries: Mutex<BTreeSet<(u64, String)>>,
    notifier: Mutex<Option<Notifier>>,
    next_handle: AtomicU64,
    runtime: Handle,
}

struct OpenFile<B: Backend> {
    id: B::Inode,
    file: Arc<B::File>,
    writable: bool,
}

#[derive(Clone)]
struct DirItem {
    ino: INodeNo,
    kind: FileType,
    name: String,
}

pub(super) struct DirCursor<B: Backend> {
    ino: INodeNo,
    parent_ino: INodeNo,
    state: B::Directory,
    entries: Vec<DirItem>,
    complete: bool,
}

impl<B: Backend> Adapter<B> {
    pub(super) fn new(backend: Arc<B>, runtime: Handle) -> Self {
        Self {
            inner: Arc::new(AdapterInner::new(backend, runtime)),
        }
    }

    pub(super) fn install_notifier(&self, notifier: Notifier) {
        self.inner.install_notifier(notifier);
    }

    pub(super) async fn serve_hup(
        self,
        signal: tokio::signal::unix::Signal,
        stop: tokio::sync::oneshot::Receiver<()>,
    ) {
        Arc::clone(&self.inner).serve_hup(signal, stop).await;
    }

    fn spawn(&self, future: impl Future<Output = ()> + Send + 'static) {
        self.inner.runtime.spawn(future);
    }
}

impl<B: Backend> AdapterInner<B> {
    fn new(backend: Arc<B>, runtime: Handle) -> Self {
        Self {
            inodes: Mutex::new(InodeTable::new(backend.root())),
            backend,
            files: Mutex::new(BTreeMap::new()),
            dirs: Mutex::new(BTreeMap::new()),
            dentries: Mutex::new(BTreeSet::new()),
            notifier: Mutex::new(None),
            next_handle: AtomicU64::new(0),
            runtime,
        }
    }

    fn next_handle(&self) -> u64 {
        self.next_handle.fetch_add(1, Ordering::Relaxed) + 1
    }

    pub(super) fn install_notifier(&self, notifier: Notifier) {
        guard(&self.notifier).replace(notifier);
    }

    fn record_dentry(&self, parent: INodeNo, name: &str) {
        guard(&self.dentries).insert((parent.0, name.to_owned()));
    }

    fn inode_for(&self, id: B::Inode, info: Option<CachedInfo>) -> Result<INodeNo> {
        let mut inodes = guard(&self.inodes);
        if let Some(info) = info {
            inodes.cache(id, info);
        }
        inodes
            .inode(id)
            .map(INodeNo)
            .map_err(|_| BackendError::MutationIo)
    }

    fn node(&self, ino: INodeNo) -> Option<B::Inode> {
        guard(&self.inodes).node(ino.0)
    }

    fn cached_info(&self, id: B::Inode) -> Option<CachedInfo> {
        guard(&self.inodes).cached(id)
    }

    fn remember_lookup(&self, ino: INodeNo) {
        guard(&self.inodes).remember_lookup(ino.0);
    }

    fn forget(&self, ino: INodeNo, count: u64) {
        guard(&self.inodes).forget(ino.0, count);
    }

    fn attribute_for(&self, id: B::Inode, observed: ObservedAttr) -> Result<FileAttr> {
        let info = CachedInfo {
            directory: observed.directory,
            executable: observed.executable,
            generation: 0,
        };
        let ino = self.inode_for(id, Some(info))?;
        Ok(synthesize(ino, observed, self.backend.read_only()))
    }

    async fn invalidate_before_open(&self, ino: INodeNo) -> Result<()> {
        let Some(notifier) = guard(&self.notifier).clone() else {
            eprintln!(
                "yy: fuse open: inode {} invalidation requested before notifier installation",
                ino.0
            );
            return Err(BackendError::MutationIo);
        };
        match tokio::task::spawn_blocking(move || notifier.inval_inode(ino, -1, 0)).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => {
                eprintln!("yy: fuse open: cannot invalidate inode {}: {error}", ino.0);
                Err(BackendError::MutationIo)
            }
            Err(error) => {
                eprintln!(
                    "yy: fuse open: inode {} invalidation task failed: {error}",
                    ino.0
                );
                Err(BackendError::MutationIo)
            }
        }
    }

    fn open_file(&self, fh: FileHandle) -> Option<Arc<OpenFile<B>>> {
        guard(&self.files).get(&fh.0).cloned()
    }

    fn file_for(&self, fh: FileHandle, id: Option<B::Inode>) -> Option<Arc<OpenFile<B>>> {
        let files = guard(&self.files);
        files.get(&fh.0).cloned().or_else(|| {
            let id = id?;
            files
                .values()
                .find(|open| self.backend.file_matches(&open.file, id))
                .cloned()
        })
    }

    fn file_for_id(&self, id: B::Inode) -> Option<Arc<OpenFile<B>>> {
        guard(&self.files)
            .values()
            .find(|open| self.backend.file_matches(&open.file, id))
            .cloned()
    }

    fn detach_file(&self, fh: FileHandle) -> Option<(B::Inode, bool)> {
        let open = guard(&self.files).remove(&fh.0)?;
        let id = open.id;
        let writable = open.writable;
        drop(open);
        Some((id, writable))
    }

    async fn serve_hup(
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

    async fn refresh_once(&self) {
        let ids = guard(&self.inodes).nodes();
        if let Err(error) = self.backend.refresh(&ids).await {
            eprintln!(
                "yy: fuse refresh: {} refresh rejected: {error}",
                self.backend.name()
            );
            return;
        }
        self.invalidate_known().await;
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
            let mut ended = false;
            for inode in inodes {
                match notifier.inval_inode(INodeNo(inode), -1, 0) {
                    Ok(()) => inode_ok += 1,
                    Err(error) if is_connection_ended(&error) => {
                        errors += 1;
                        ended = true;
                        break;
                    }
                    Err(_) => errors += 1,
                }
            }
            if !ended {
                for (parent, name) in dentries {
                    match notifier.inval_entry(INodeNo(parent), OsStr::new(&name)) {
                        Ok(()) => entry_ok += 1,
                        Err(error) if is_connection_ended(&error) => {
                            errors += 1;
                            ended = true;
                            break;
                        }
                        Err(_) => errors += 1,
                    }
                }
            }
            (inode_ok, entry_ok, errors, ended, attempts)
        })
        .await
        .unwrap_or((0, 0, attempts, false, attempts));
        let (inode_ok, entry_ok, errors, ended, _) = result;
        if ended {
            eprintln!(
                "yy: fuse refresh: channel ended; sent {inode_ok} inode and {entry_ok} entry invalidations before {errors} errors"
            );
        } else {
            eprintln!(
                "yy: fuse refresh: done (inode={inode_ok} entries={entry_ok} errors={errors})"
            );
        }
    }

    async fn lookup(&self, parent: INodeNo, name: String, reply: ReplyEntry) {
        let Some(parent_id) = self.node(parent) else {
            return reply.error(fuser::Errno::ESTALE);
        };
        let entry = match self.backend.lookup(parent_id, &name).await {
            Ok(Some(entry)) => entry,
            Ok(None) => return reply.error(fuser::Errno::ENOENT),
            Err(error) => return reply.error(error.errno()),
        };
        let attr = match self.attribute_for(entry.id, entry.attr) {
            Ok(attr) => attr,
            Err(error) => return reply.error(error.errno()),
        };
        self.record_dentry(parent, &name);
        self.remember_lookup(attr.ino);
        reply.entry(&self.backend.attr_ttl(), &attr, Generation(0));
    }

    async fn getattr(&self, ino: INodeNo, fh: Option<FileHandle>, reply: ReplyAttr) {
        let Some(id) = self.node(ino) else {
            return reply.error(fuser::Errno::ESTALE);
        };
        let open = fh
            .and_then(|fh| self.open_file(fh))
            .or_else(|| self.file_for_id(id));
        let result = self
            .backend
            .getattr(id, open.as_deref().map(|open| open.file.as_ref()))
            .await
            .and_then(|observed| self.attribute_for(id, observed));
        match result {
            Ok(attr) => reply.attr(&self.backend.attr_ttl(), &attr),
            Err(error) => reply.error(error.errno()),
        }
    }

    async fn open(&self, ino: INodeNo, flags: OpenFlags, reply: ReplyOpen) {
        let Some(id) = self.node(ino) else {
            return reply.error(fuser::Errno::ESTALE);
        };
        if self.cached_info(id).is_some_and(|info| info.directory) {
            return reply.error(fuser::Errno::EISDIR);
        }
        let writable = writable_open(flags);
        let truncate = has_open_flag(flags.0, nix::fcntl::OFlag::O_TRUNC);
        if truncate && !writable {
            return reply.error(fuser::Errno::EINVAL);
        }
        if self.backend.read_only() && writable {
            return reply.error(fuser::Errno::EROFS);
        }
        let opened = match self.backend.open(id, flags).await {
            Ok(opened) => opened,
            Err(error) => return reply.error(error.errno()),
        };
        if let Err(error) = self.invalidate_before_open(ino).await {
            return reply.error(error.errno());
        }
        let fh = self.next_handle();
        guard(&self.files).insert(
            fh,
            Arc::new(OpenFile {
                id,
                file: Arc::new(opened.file),
                writable: opened.writable,
            }),
        );
        reply.opened(FileHandle(fh), FopenFlags::FOPEN_DIRECT_IO);
    }

    async fn read(&self, ino: INodeNo, fh: FileHandle, offset: u64, size: u32, reply: ReplyData) {
        let Some(open) = self.file_for(fh, self.node(ino)) else {
            return reply.error(fuser::Errno::EBADF);
        };
        match self.backend.read(&open.file, offset, size as usize).await {
            Ok(bytes) => reply.data(&bytes),
            Err(error) => reply.error(error.errno()),
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
        let written = match u32::try_from(data.len()) {
            Ok(written) => written,
            Err(_) => return reply.error(fuser::Errno::EINVAL),
        };
        match self.backend.write(&open.file, offset, &data, flags).await {
            Ok(()) => reply.written(written),
            Err(error) => reply.error(error.errno()),
        }
    }

    async fn sync(&self, fh: FileHandle, reply: ReplyEmpty, fsync: bool) {
        let Some(open) = self.open_file(fh) else {
            return reply.error(fuser::Errno::EBADF);
        };
        if !open.writable {
            return reply.ok();
        }
        let result = if fsync {
            self.backend.fsync(&open.file).await
        } else {
            self.backend.flush(&open.file).await
        };
        match result {
            Ok(()) => reply.ok(),
            Err(error) => reply.error(error.errno()),
        }
    }

    async fn release(&self, id: B::Inode, writable: bool, reply: ReplyEmpty) {
        if let Err(error) = self.backend.release(id, writable).await {
            eprintln!("yy: fuse release: {}: {error}", self.backend.name());
        }
        reply.ok();
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
        let Some(id) = self.node(ino) else {
            return reply.error(fuser::Errno::ESTALE);
        };
        let open = if let Some(fh) = fh {
            let Some(open) = self.open_file(fh) else {
                return reply.error(fuser::Errno::EBADF);
            };
            Some(open)
        } else {
            None
        };
        let request = SetAttrRequest {
            mode,
            uid,
            gid,
            size,
            atime,
            mtime,
            ctime,
            crtime,
            chgtime,
            bkuptime,
            flags,
        };
        match self
            .backend
            .setattr(id, open.as_deref().map(|open| open.file.as_ref()), request)
            .await
            .and_then(|observed| self.attribute_for(id, observed))
        {
            Ok(attr) => reply.attr(&self.backend.attr_ttl(), &attr),
            Err(error) => reply.error(error.errno()),
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
        if self.backend.read_only() {
            return reply.error(fuser::Errno::EROFS);
        }
        let Some(parent_id) = self.node(parent) else {
            return reply.error(fuser::Errno::ESTALE);
        };
        let created = match self
            .backend
            .create(parent_id, &name, mode, umask, flags)
            .await
        {
            Ok(created) => created,
            Err(error) => return reply.error(error.errno()),
        };
        let attr = match self.attribute_for(created.id, created.attr) {
            Ok(attr) => attr,
            Err(error) => return reply.error(error.errno()),
        };
        let fh = self.next_handle();
        guard(&self.files).insert(
            fh,
            Arc::new(OpenFile {
                id: created.id,
                file: Arc::new(created.file),
                writable: created.writable,
            }),
        );
        self.record_dentry(parent, &name);
        self.remember_lookup(attr.ino);
        reply.created(
            &self.backend.attr_ttl(),
            &attr,
            Generation(0),
            FileHandle(fh),
            FopenFlags::FOPEN_DIRECT_IO,
        );
    }

    async fn mkdir(&self, parent: INodeNo, name: String, mode: u32, umask: u32, reply: ReplyEntry) {
        if self.backend.read_only() {
            return reply.error(fuser::Errno::EROFS);
        }
        let Some(parent_id) = self.node(parent) else {
            return reply.error(fuser::Errno::ESTALE);
        };
        let entry = match self.backend.mkdir(parent_id, &name, mode, umask).await {
            Ok(entry) => entry,
            Err(error) => return reply.error(error.errno()),
        };
        let attr = match self.attribute_for(entry.id, entry.attr) {
            Ok(attr) => attr,
            Err(error) => return reply.error(error.errno()),
        };
        self.record_dentry(parent, &name);
        self.remember_lookup(attr.ino);
        reply.entry(&self.backend.attr_ttl(), &attr, Generation(0));
    }

    async fn unlink(&self, parent: INodeNo, name: String, reply: ReplyEmpty) {
        if self.backend.read_only() {
            return reply.error(fuser::Errno::EROFS);
        }
        let Some(parent_id) = self.node(parent) else {
            return reply.error(fuser::Errno::ESTALE);
        };
        match self.backend.unlink(parent_id, &name).await {
            Ok(()) => reply.ok(),
            Err(error) => reply.error(error.errno()),
        }
    }

    async fn rmdir(&self, parent: INodeNo, name: String, reply: ReplyEmpty) {
        if self.backend.read_only() {
            return reply.error(fuser::Errno::EROFS);
        }
        let Some(parent_id) = self.node(parent) else {
            return reply.error(fuser::Errno::ESTALE);
        };
        match self.backend.rmdir(parent_id, &name).await {
            Ok(()) => reply.ok(),
            Err(error) => reply.error(error.errno()),
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
        if self.backend.read_only() {
            return reply.error(fuser::Errno::EROFS);
        }
        let replace = match rename_replace(flags) {
            Ok(replace) => replace,
            Err(rejection) => return reply.error(rejection.errno()),
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
        match self
            .backend
            .rename(parent_id, &name, new_parent_id, &new_name, replace)
            .await
        {
            Ok(()) => reply.ok(),
            Err(error) => reply.error(error.errno()),
        }
    }

    async fn opendir(&self, ino: INodeNo, reply: ReplyOpen) {
        let Some(id) = self.node(ino) else {
            return reply.error(fuser::Errno::ESTALE);
        };
        let opened = match self.backend.open_directory(id).await {
            Ok(opened) => opened,
            Err(error) => return reply.error(error.errno()),
        };
        let (Ok(directory_ino), Ok(parent_ino)) = (
            self.inode_for(
                opened.id,
                Some(CachedInfo {
                    directory: true,
                    executable: false,
                    generation: 0,
                }),
            ),
            self.inode_for(opened.parent, None),
        ) else {
            return reply.error(fuser::Errno::EIO);
        };
        let fh = self.next_handle();
        guard(&self.dirs).insert(
            fh,
            Arc::new(AsyncMutex::new(DirCursor {
                ino: directory_ino,
                parent_ino,
                state: opened.state,
                entries: Vec::new(),
                complete: false,
            })),
        );
        reply.opened(FileHandle(fh), FopenFlags::empty());
    }

    async fn readdir(&self, fh: FileHandle, offset: u64, mut reply: ReplyDirectory) {
        let Some(handle) = guard(&self.dirs).get(&fh.0).cloned() else {
            return reply.error(fuser::Errno::EBADF);
        };
        let mut cursor = handle.lock().await;
        let mut restart = offset == 0;
        if restart {
            cursor.entries.clear();
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
                let page = match self
                    .backend
                    .read_directory(&mut cursor.state, restart)
                    .await
                {
                    Ok(page) => page,
                    Err(error) => {
                        if added || cursor.entries.len() > before {
                            break;
                        }
                        return reply.error(error.errno());
                    }
                };
                restart = false;
                for entry in page.entries {
                    let ino = match self.inode_for(
                        entry.id,
                        Some(CachedInfo {
                            directory: entry.kind == FileType::Directory,
                            executable: false,
                            generation: 0,
                        }),
                    ) {
                        Ok(ino) => ino,
                        Err(error) => {
                            if added || cursor.entries.len() > before {
                                break;
                            }
                            return reply.error(error.errno());
                        }
                    };
                    cursor.entries.push(DirItem {
                        ino,
                        kind: entry.kind,
                        name: entry.name,
                    });
                }
                cursor.complete = page.complete;
                continue;
            }
            let item = &cursor.entries[local];
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
}

impl<B: Backend> Filesystem for Adapter<B> {
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
        let adapter = self.inner.clone();
        self.spawn(async move { adapter.lookup(parent, name, reply).await });
    }

    fn getattr(&self, _req: &Request, ino: INodeNo, fh: Option<FileHandle>, reply: ReplyAttr) {
        let adapter = self.inner.clone();
        self.spawn(async move { adapter.getattr(ino, fh, reply).await });
    }

    fn open(&self, _req: &Request, ino: INodeNo, flags: OpenFlags, reply: ReplyOpen) {
        let adapter = self.inner.clone();
        self.spawn(async move { adapter.open(ino, flags, reply).await });
    }

    fn read(
        &self,
        _req: &Request,
        ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        size: u32,
        _flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        reply: ReplyData,
    ) {
        let adapter = self.inner.clone();
        self.spawn(async move { adapter.read(ino, fh, offset, size, reply).await });
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
        let adapter = self.inner.clone();
        self.spawn(async move { adapter.write(fh, offset, data, flags, reply).await });
    }

    fn flush(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        _lock_owner: LockOwner,
        reply: ReplyEmpty,
    ) {
        let adapter = self.inner.clone();
        self.spawn(async move { adapter.sync(fh, reply, false).await });
    }

    fn fsync(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        _datasync: bool,
        reply: ReplyEmpty,
    ) {
        let adapter = self.inner.clone();
        self.spawn(async move { adapter.sync(fh, reply, true).await });
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
        let adapter = self.inner.clone();
        let Some((id, writable)) = adapter.detach_file(fh) else {
            return reply.ok();
        };
        self.spawn(async move { adapter.release(id, writable, reply).await });
    }

    fn opendir(&self, _req: &Request, ino: INodeNo, _flags: OpenFlags, reply: ReplyOpen) {
        let adapter = self.inner.clone();
        self.spawn(async move { adapter.opendir(ino, reply).await });
    }

    fn readdir(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        reply: ReplyDirectory,
    ) {
        let adapter = self.inner.clone();
        self.spawn(async move { adapter.readdir(fh, offset, reply).await });
    }

    fn releasedir(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        _flags: OpenFlags,
        reply: ReplyEmpty,
    ) {
        let adapter = self.inner.clone();
        self.spawn(async move { adapter.releasedir(fh, reply).await });
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
        if self.inner.backend.read_only() && mask.contains(AccessFlags::W_OK) {
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
        ctime: Option<SystemTime>,
        fh: Option<FileHandle>,
        crtime: Option<SystemTime>,
        chgtime: Option<SystemTime>,
        bkuptime: Option<SystemTime>,
        flags: Option<BsdFileFlags>,
        reply: ReplyAttr,
    ) {
        let adapter = self.inner.clone();
        self.spawn(async move {
            adapter
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
        reply.error(unsupported(self.inner.backend.read_only()));
    }

    fn mkdir(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        umask: u32,
        reply: ReplyEntry,
    ) {
        let Some(name) = name.to_str() else {
            return reply.error(fuser::Errno::EINVAL);
        };
        let name = name.to_owned();
        let adapter = self.inner.clone();
        self.spawn(async move { adapter.mkdir(parent, name, mode, umask, reply).await });
    }

    fn unlink(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        let Some(name) = name.to_str() else {
            return reply.error(fuser::Errno::EINVAL);
        };
        let name = name.to_owned();
        let adapter = self.inner.clone();
        self.spawn(async move { adapter.unlink(parent, name, reply).await });
    }

    fn rmdir(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        let Some(name) = name.to_str() else {
            return reply.error(fuser::Errno::EINVAL);
        };
        let name = name.to_owned();
        let adapter = self.inner.clone();
        self.spawn(async move { adapter.rmdir(parent, name, reply).await });
    }

    fn symlink(
        &self,
        _req: &Request,
        _parent: INodeNo,
        _link_name: &OsStr,
        _target: &std::path::Path,
        reply: ReplyEntry,
    ) {
        reply.error(unsupported(self.inner.backend.read_only()));
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
        let adapter = self.inner.clone();
        self.spawn(async move {
            adapter
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
        reply.error(unsupported(self.inner.backend.read_only()));
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
        let adapter = self.inner.clone();
        self.spawn(async move {
            adapter
                .create(parent, name, mode, umask, flags, reply)
                .await
        });
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
        reply.error(unsupported(self.inner.backend.read_only()));
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

fn unsupported(read_only: bool) -> fuser::Errno {
    if read_only {
        fuser::Errno::EROFS
    } else {
        fuser::Errno::EOPNOTSUPP
    }
}

fn is_connection_ended(error: &std::io::Error) -> bool {
    matches!(
        error.raw_os_error(),
        Some(code)
            if matches!(
                code,
                libc::ENODEV | libc::ENOTCONN | libc::EPIPE | libc::ECONNRESET | libc::ESHUTDOWN
            )
    )
}

fn guard<T>(lock: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{BackendProfile, Fs};
    use crate::fuse::frontend::{ManagedBackend, ManagedFile};
    use crate::mount::Mount;
    use crate::runtime::Runtime;
    use crate::support::TestBackend;
    use yinyang_core::Authority;

    #[tokio::test]
    async fn release_drops_the_last_mount_reference_on_the_dispatch_thread() {
        let backend_state = TestBackend::default();
        let authority: Arc<dyn Authority> = Arc::new(
            Fs::create(backend_state.operator(), BackendProfile::Minio)
                .await
                .unwrap(),
        );
        let writer_temp = tempfile::tempdir().unwrap();
        let writer = Runtime::open(authority.clone(), writer_temp.path())
            .await
            .unwrap();
        let (id, _) = writer
            .create_file_with_node(authority.root(), "file", false)
            .await
            .unwrap();
        let mut file = writer.open_node(id, true).await.unwrap();
        file.write(0, b"old-content").await.unwrap();
        file.close().await.unwrap();
        drop(writer);

        let mount_temp = tempfile::tempdir().unwrap();
        let mount = Mount::open(authority.clone(), mount_temp.path(), true)
            .await
            .unwrap();
        let old = mount.open_read_only_node(id).await.unwrap();
        assert_eq!(mount.reference_count(id), Some(1));
        let backend = Arc::new(ManagedBackend::from_mount_for_test(mount.clone(), true).await);
        let inner = AdapterInner::new(backend, Handle::current());
        let fh = FileHandle(1);
        guard(&inner.files).insert(
            fh.0,
            Arc::new(OpenFile {
                id,
                file: Arc::new(ManagedFile::ReadOnly(Box::new(old))),
                writable: false,
            }),
        );

        let update_temp = tempfile::tempdir().unwrap();
        let updater = Runtime::open(authority, update_temp.path()).await.unwrap();
        let mut file = updater.open_node(id, true).await.unwrap();
        file.write(0, b"new-content").await.unwrap();
        file.close().await.unwrap();
        drop(updater);

        assert_eq!(inner.detach_file(fh), Some((id, false)));
        assert_eq!(mount.reference_count(id), Some(0));
        let current = mount.open_read_only_node(id).await.unwrap();
        assert_eq!(current.read(0, 11).await.unwrap(), b"new-content");
    }
}
