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

use std::fmt;

/// Broad category of a YinYang Format failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ErrorKind {
    Invalid,
    Corrupt,
    NotFound,
    Unsupported,
    Storage,
}

/// Error returned by the YinYang Format core.
#[derive(Debug)]
pub struct Error {
    kind: ErrorKind,
    operation: &'static str,
    message: String,
}

impl Error {
    pub fn new(kind: ErrorKind, operation: &'static str, message: impl Into<String>) -> Self {
        Self {
            kind,
            operation,
            message: message.into(),
        }
    }

    pub const fn kind(&self) -> ErrorKind {
        self.kind
    }

    pub const fn operation(&self) -> &'static str {
        self.operation
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub(crate) fn invalid(operation: &'static str, message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Invalid, operation, message)
    }

    pub(crate) fn corrupt(operation: &'static str, message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Corrupt, operation, message)
    }

    pub(crate) fn unsupported(operation: &'static str, message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Unsupported, operation, message)
    }

    pub(crate) fn from_storage(operation: &'static str, error: opendal::Error) -> Self {
        Self::new(ErrorKind::Storage, operation, error.to_string())
    }
}

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.operation, self.message)
    }
}

impl std::error::Error for Error {}

pub type Result<T, E = Error> = std::result::Result<T, E>;
