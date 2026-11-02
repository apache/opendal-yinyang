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

use std::path::Path;

pub use crate::runtime::ErrorKind;
use yinyang_core::CommitId;

#[derive(Debug)]
pub struct Error {
    kind: ErrorKind,
    message: String,
    commit: Option<CommitId>,
}

impl Error {
    pub fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            commit: None,
        }
    }

    pub fn unknown(commit: Option<CommitId>, message: impl Into<String>) -> Self {
        Self {
            kind: ErrorKind::Unknown,
            message: message.into(),
            commit,
        }
    }

    pub fn invalid(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Invalid, message)
    }

    pub fn unsupported(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Unsupported, message)
    }

    pub fn conflict(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Conflict, message)
    }

    pub fn corrupt(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Corrupt, message)
    }

    pub fn busy(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Busy, message)
    }

    pub fn io(operation: &str, path: &Path, error: impl std::fmt::Display) -> Self {
        Self::new(
            ErrorKind::Io,
            format!("{operation} {}: {error}", path.display()),
        )
    }

    pub fn errno(operation: &str, path: &Path, error: rustix::io::Errno) -> Self {
        Self::io(operation, path, error)
    }

    pub const fn kind(&self) -> ErrorKind {
        self.kind
    }

    pub const fn commit_id(&self) -> Option<CommitId> {
        self.commit
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for Error {}

impl From<yinyang_core::Error> for Error {
    fn from(error: yinyang_core::Error) -> Self {
        let kind = match error.kind() {
            yinyang_core::ErrorKind::Invalid => ErrorKind::Invalid,
            yinyang_core::ErrorKind::InvalidName => ErrorKind::InvalidName,
            yinyang_core::ErrorKind::NotFound => ErrorKind::NotFound,
            yinyang_core::ErrorKind::AlreadyExists => ErrorKind::AlreadyExists,
            yinyang_core::ErrorKind::NotDirectory => ErrorKind::NotDirectory,
            yinyang_core::ErrorKind::IsDirectory => ErrorKind::IsDirectory,
            yinyang_core::ErrorKind::NotEmpty => ErrorKind::NotEmpty,
            yinyang_core::ErrorKind::PermissionDenied => ErrorKind::PermissionDenied,
            yinyang_core::ErrorKind::Corrupt => ErrorKind::Corrupt,
            yinyang_core::ErrorKind::Unsupported => ErrorKind::Unsupported,
            yinyang_core::ErrorKind::Storage => ErrorKind::Storage,
            yinyang_core::ErrorKind::Io => ErrorKind::Io,
        };
        Self::new(kind, error.to_string())
    }
}

impl From<crate::runtime::Error> for Error {
    fn from(error: crate::runtime::Error) -> Self {
        Self {
            kind: error.kind(),
            message: error.to_string(),
            commit: error.commit_id(),
        }
    }
}

impl From<crate::direct::Error> for Error {
    fn from(error: crate::direct::Error) -> Self {
        let kind = match error.kind() {
            crate::direct::ErrorKind::Invalid => ErrorKind::Invalid,
            crate::direct::ErrorKind::InvalidName => ErrorKind::InvalidName,
            crate::direct::ErrorKind::NotFound => ErrorKind::NotFound,
            crate::direct::ErrorKind::NotDirectory => ErrorKind::NotDirectory,
            crate::direct::ErrorKind::IsDirectory => ErrorKind::IsDirectory,
            crate::direct::ErrorKind::Stale => ErrorKind::Conflict,
            crate::direct::ErrorKind::Unsupported => ErrorKind::Unsupported,
            crate::direct::ErrorKind::Corrupt => ErrorKind::Corrupt,
            crate::direct::ErrorKind::Storage => ErrorKind::Storage,
        };
        Self::new(kind, error.to_string())
    }
}

pub type Result<T> = std::result::Result<T, Error>;
