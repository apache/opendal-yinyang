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

use crate::local::store;
pub use crate::runtime::ErrorKind;
use crate::runtime::kind_from_errno;
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

    pub fn io(operation: &str, path: &Path, error: impl std::error::Error + 'static) -> Self {
        let kind = store::classify_error(&error).into();
        Self::new(kind, format!("{operation} {}: {error}", path.display()))
    }

    pub fn errno(operation: &str, path: &Path, error: rustix::io::Errno) -> Self {
        let kind = kind_from_errno(error);
        Self::new(kind, format!("{operation} {}: {error}", path.display()))
    }

    pub(crate) fn from_local(error: store::Error) -> Self {
        Self::new(error.kind().into(), error.to_string())
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
        let kind = error.kind().into();
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
        let kind = error.kind().into();
        Self::new(kind, error.to_string())
    }
}

pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn errno_preserves_operational_kinds() {
        for (error, kind) in [
            (rustix::io::Errno::NOENT, ErrorKind::NotFound),
            (rustix::io::Errno::EXIST, ErrorKind::AlreadyExists),
            (rustix::io::Errno::ACCESS, ErrorKind::PermissionDenied),
            (rustix::io::Errno::ROFS, ErrorKind::ReadOnly),
            (rustix::io::Errno::NOSPC, ErrorKind::NoSpace),
            (rustix::io::Errno::WOULDBLOCK, ErrorKind::Busy),
        ] {
            assert_eq!(
                Error::errno("test local operation", Path::new("state"), error).kind(),
                kind
            );
        }
    }
}
