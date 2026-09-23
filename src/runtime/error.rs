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

//! Domain failures shared by runtime calls and durable error reporting.
use borsh::{BorshDeserialize, BorshSerialize};
use yinyang_core::CommitId;

/// Classify errors without parsing diagnostic text. This is not an errno or
/// platform status: adapters still choose their operation-specific mapping.
#[derive(Clone, Copy, Debug, Eq, PartialEq, BorshSerialize, BorshDeserialize)]
pub enum ErrorKind {
    Invalid,
    InvalidName,
    NotFound,
    AlreadyExists,
    NotDirectory,
    IsDirectory,
    NotEmpty,
    ReadOnly,
    PermissionDenied,
    Busy,
    Closed,
    Frozen,
    TooLarge,
    Conflict,
    Retryable,
    Unknown,
    Corrupt,
    Unsupported,
    Storage,
    Io,
    NoSpace,
    /// An older staging record retained only diagnostic text.
    Unclassified,
}

/// A durable failure, including the original uncertain publication identity.
#[derive(Clone, Debug, Eq, PartialEq, BorshSerialize, BorshDeserialize)]
pub struct Failure {
    kind: ErrorKind,
    message: String,
    commit: Option<[u8; 16]>,
}
impl Failure {
    pub const fn kind(&self) -> ErrorKind {
        self.kind
    }
    pub fn message(&self) -> &str {
        &self.message
    }
    pub fn commit_id(&self) -> Option<CommitId> {
        self.commit.map(CommitId::from_bytes)
    }
    pub(super) fn legacy(message: String) -> Self {
        Self {
            kind: ErrorKind::Unclassified,
            message,
            commit: None,
        }
    }
    pub(super) fn capture(error: &Error) -> Self {
        match error {
            Error::Retained(failure) | Error::Local(failure) => failure.clone(),
            _ => Self {
                kind: error.kind(),
                message: error.to_string(),
                commit: error.commit_id().map(|id| *id.as_bytes()),
            },
        }
    }
}
impl std::fmt::Display for Failure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

#[derive(Debug)]
pub enum Error {
    Core(yinyang_core::Error),
    Local(Failure),
    Invalid(&'static str),
    State(ErrorKind, &'static str),
    Retained(Failure),
    Conflict,
    Retryable,
    Unknown(CommitId),
}
impl Error {
    pub fn kind(&self) -> ErrorKind {
        match self {
            Self::Core(e) => match e.kind() {
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
            },
            Self::Local(e) | Self::Retained(e) => e.kind(),
            Self::Invalid(_) => ErrorKind::Invalid,
            Self::State(kind, _) => *kind,
            Self::Conflict => ErrorKind::Conflict,
            Self::Retryable => ErrorKind::Retryable,
            Self::Unknown(_) => ErrorKind::Unknown,
        }
    }
    pub fn commit_id(&self) -> Option<CommitId> {
        match self {
            Self::Unknown(id) => Some(*id),
            Self::Retained(e) | Self::Local(e) => e.commit_id(),
            _ => None,
        }
    }
    pub(crate) fn local(message: impl std::fmt::Display) -> Self {
        Self::local_kind(ErrorKind::Io, message)
    }
    pub(crate) fn local_kind(kind: ErrorKind, message: impl std::fmt::Display) -> Self {
        Self::Local(Failure {
            kind,
            message: message.to_string(),
            commit: None,
        })
    }
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Core(e) => write!(f, "{e}"),
            Self::Local(e) => write!(f, "local staging: {e}"),
            Self::Invalid(e) | Self::State(_, e) => write!(f, "{e}"),
            Self::Retained(e) => write!(f, "{e}"),
            Self::Conflict => write!(f, "observed state changed; any staged bytes are retained"),
            Self::Retryable => write!(
                f,
                "publication is retryable; the original request is retained"
            ),
            Self::Unknown(id) => write!(
                f,
                "publication outcome is unknown for {id:?}; the original request is retained"
            ),
        }
    }
}
impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Core(e) => Some(e),
            _ => None,
        }
    }
}
impl From<yinyang_core::Error> for Error {
    fn from(e: yinyang_core::Error) -> Self {
        Self::Core(e)
    }
}
pub type Result<T> = std::result::Result<T, Error>;
