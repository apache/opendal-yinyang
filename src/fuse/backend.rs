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

//! The narrow contract between the one FUSE adapter and a metadata backend.

use super::attrs::ObservedAttr;
use super::inode::InodeIdentity;
use fuser::{BsdFileFlags, FileType, OpenFlags, TimeOrNow};
use std::fmt;
use std::future::Future;
use std::time::{Duration, SystemTime};

pub(super) type Result<T> = std::result::Result<T, BackendError>;

pub(super) fn file_type(directory: bool) -> FileType {
    if directory {
        FileType::Directory
    } else {
        FileType::RegularFile
    }
}

/// Errors whose Linux mapping is shared by every frontend path.
#[derive(Debug)]
pub(super) enum BackendError {
    Runtime(crate::runtime::Error),
    NamespaceConflict,
    Core(yinyang_core::Error),
    Direct(crate::direct::Error),
    NotFound,
    NotDirectory,
    IsDirectory,
    MutationIo,
    MutationUnsupported,
}

impl BackendError {
    pub(super) fn runtime_namespace_mutation(error: crate::runtime::Error) -> Self {
        if error.kind() == crate::runtime::ErrorKind::Conflict {
            Self::NamespaceConflict
        } else {
            Self::Runtime(error)
        }
    }

    /// The one errno table. Operation-specific callers select a semantic
    /// variant such as `NamespaceConflict`; they do not define local tables.
    pub(super) fn errno(&self) -> fuser::Errno {
        match self {
            Self::Runtime(error) => runtime_errno(error.kind()),
            Self::NamespaceConflict => fuser::Errno::EAGAIN,
            Self::Core(error) => core_errno(error.kind()),
            Self::Direct(error) => direct_errno(error.kind()),
            Self::NotFound => fuser::Errno::ENOENT,
            Self::NotDirectory => fuser::Errno::ENOTDIR,
            Self::IsDirectory => fuser::Errno::EISDIR,
            Self::MutationIo => fuser::Errno::EIO,
            Self::MutationUnsupported => fuser::Errno::EOPNOTSUPP,
        }
    }
}

impl fmt::Display for BackendError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Runtime(error) => error.fmt(formatter),
            Self::NamespaceConflict => formatter.write_str("namespace precondition conflict"),
            Self::Core(error) => error.fmt(formatter),
            Self::Direct(error) => error.fmt(formatter),
            Self::NotFound => formatter.write_str("entry is absent"),
            Self::NotDirectory => formatter.write_str("entry is not a directory"),
            Self::IsDirectory => formatter.write_str("entry is a directory"),
            Self::MutationIo => formatter.write_str("namespace mutation did not complete"),
            Self::MutationUnsupported => {
                formatter.write_str("namespace mutation is unsupported by the backend")
            }
        }
    }
}

impl std::error::Error for BackendError {}

impl From<crate::runtime::Error> for BackendError {
    fn from(error: crate::runtime::Error) -> Self {
        Self::Runtime(error)
    }
}

impl From<yinyang_core::Error> for BackendError {
    fn from(error: yinyang_core::Error) -> Self {
        Self::Core(error)
    }
}

impl From<crate::direct::Error> for BackendError {
    fn from(error: crate::direct::Error) -> Self {
        Self::Direct(error)
    }
}

fn runtime_errno(kind: crate::runtime::ErrorKind) -> fuser::Errno {
    use crate::runtime::ErrorKind;
    match kind {
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

fn core_errno(kind: yinyang_core::ErrorKind) -> fuser::Errno {
    use yinyang_core::ErrorKind;
    match kind {
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

fn direct_errno(kind: crate::direct::ErrorKind) -> fuser::Errno {
    use crate::direct::ErrorKind;
    match kind {
        ErrorKind::Invalid | ErrorKind::InvalidName => fuser::Errno::EINVAL,
        ErrorKind::NotFound => fuser::Errno::ENOENT,
        ErrorKind::NotDirectory => fuser::Errno::ENOTDIR,
        ErrorKind::IsDirectory => fuser::Errno::EISDIR,
        ErrorKind::Stale => fuser::Errno::ESTALE,
        ErrorKind::Unsupported => fuser::Errno::EOPNOTSUPP,
        ErrorKind::Storage => fuser::Errno::EIO,
    }
}

#[derive(Clone, Debug)]
pub(super) struct Entry<I: InodeIdentity> {
    pub(super) id: I,
    pub(super) attr: ObservedAttr,
}

#[derive(Clone, Debug)]
pub(super) struct Opened<F> {
    pub(super) file: F,
    pub(super) writable: bool,
}

#[derive(Clone, Debug)]
pub(super) struct Created<I: InodeIdentity, F> {
    pub(super) id: I,
    pub(super) attr: ObservedAttr,
    pub(super) file: F,
    pub(super) writable: bool,
}

#[derive(Clone, Debug)]
pub(super) struct DirectoryOpen<I: InodeIdentity, D> {
    pub(super) id: I,
    pub(super) parent: I,
    pub(super) state: D,
}

#[derive(Clone, Debug)]
pub(super) struct DirectoryEntry<I: InodeIdentity> {
    pub(super) id: I,
    pub(super) kind: FileType,
    pub(super) name: String,
}

#[derive(Clone, Debug)]
pub(super) struct DirectoryPage<I: InodeIdentity> {
    pub(super) entries: Vec<DirectoryEntry<I>>,
    pub(super) complete: bool,
}

#[derive(Clone, Debug)]
pub(super) struct SetAttrRequest {
    pub(super) mode: Option<u32>,
    pub(super) uid: Option<u32>,
    pub(super) gid: Option<u32>,
    pub(super) size: Option<u64>,
    pub(super) atime: Option<TimeOrNow>,
    pub(super) mtime: Option<TimeOrNow>,
    pub(super) ctime: Option<SystemTime>,
    pub(super) crtime: Option<SystemTime>,
    pub(super) chgtime: Option<SystemTime>,
    pub(super) bkuptime: Option<SystemTime>,
    pub(super) flags: Option<BsdFileFlags>,
}

impl SetAttrRequest {
    pub(super) fn has_timestamp(&self) -> bool {
        self.atime.is_some()
            || self.mtime.is_some()
            || self.ctime.is_some()
            || self.crtime.is_some()
            || self.chgtime.is_some()
            || self.bkuptime.is_some()
    }
}

pub(super) trait Backend: Send + Sync + 'static {
    type Inode: InodeIdentity;
    type File: Send + Sync + 'static;
    type Directory: Send + Sync + 'static;

    fn name(&self) -> &str;
    fn read_only(&self) -> bool;
    fn root(&self) -> Self::Inode;
    fn attr_ttl(&self) -> Duration;
    fn file_matches(&self, file: &Self::File, id: Self::Inode) -> bool;

    fn lookup(
        &self,
        parent: Self::Inode,
        name: &str,
    ) -> impl Future<Output = Result<Option<Entry<Self::Inode>>>> + Send;
    fn getattr(
        &self,
        id: Self::Inode,
        file: Option<&Self::File>,
    ) -> impl Future<Output = Result<ObservedAttr>> + Send;
    fn open(
        &self,
        id: Self::Inode,
        flags: OpenFlags,
    ) -> impl Future<Output = Result<Opened<Self::File>>> + Send;
    fn read(
        &self,
        file: &Self::File,
        offset: u64,
        size: usize,
    ) -> impl Future<Output = Result<Vec<u8>>> + Send;
    fn write(
        &self,
        file: &Self::File,
        offset: u64,
        data: &[u8],
        flags: OpenFlags,
    ) -> impl Future<Output = Result<()>> + Send;
    fn flush(&self, file: &Self::File) -> impl Future<Output = Result<()>> + Send;
    fn fsync(&self, file: &Self::File) -> impl Future<Output = Result<()>> + Send;
    fn release(
        &self,
        id: Self::Inode,
        file: &Self::File,
        writable: bool,
    ) -> impl Future<Output = Result<()>> + Send;
    fn setattr(
        &self,
        id: Self::Inode,
        file: Option<&Self::File>,
        request: SetAttrRequest,
    ) -> impl Future<Output = Result<ObservedAttr>> + Send;
    fn create(
        &self,
        parent: Self::Inode,
        name: &str,
        mode: u32,
        umask: u32,
        flags: i32,
    ) -> impl Future<Output = Result<Created<Self::Inode, Self::File>>> + Send;
    fn mkdir(
        &self,
        parent: Self::Inode,
        name: &str,
        mode: u32,
        umask: u32,
    ) -> impl Future<Output = Result<Entry<Self::Inode>>> + Send;
    fn unlink(&self, parent: Self::Inode, name: &str) -> impl Future<Output = Result<()>> + Send;
    fn rmdir(&self, parent: Self::Inode, name: &str) -> impl Future<Output = Result<()>> + Send;
    fn rename(
        &self,
        parent: Self::Inode,
        name: &str,
        new_parent: Self::Inode,
        new_name: &str,
        replace: bool,
    ) -> impl Future<Output = Result<()>> + Send;
    fn open_directory(
        &self,
        id: Self::Inode,
    ) -> impl Future<Output = Result<DirectoryOpen<Self::Inode, Self::Directory>>> + Send;
    fn read_directory(
        &self,
        state: &mut Self::Directory,
        restart: bool,
    ) -> impl Future<Output = Result<DirectoryPage<Self::Inode>>> + Send;
    fn refresh(&self, ids: &[Self::Inode]) -> impl Future<Output = Result<()>> + Send;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn namespace_mutation_conflicts_use_eagain() {
        assert_eq!(
            i32::from(BackendError::NamespaceConflict.errno()),
            libc::EAGAIN
        );
        assert_eq!(i32::from(BackendError::MutationIo.errno()), libc::EIO);
        assert_eq!(
            i32::from(BackendError::MutationUnsupported.errno()),
            libc::EOPNOTSUPP
        );
    }
}
