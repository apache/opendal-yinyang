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

//! Process leases for path-addressed shared local state.

use super::store::{Error, ErrorKind, Result};
use std::fs::{File, OpenOptions};
use std::path::Path;

/// One exclusive process lease for a shared local-state directory.
///
/// Acquisition may create the directory and named lock file. It never opens
/// or creates the profile database stored in that directory.
pub(crate) struct StateLease {
    file: File,
}

impl StateLease {
    pub(crate) fn acquire(directory: &Path, name: &'static str) -> Result<Self> {
        std::fs::create_dir_all(directory).map_err(|error| {
            local_error(
                format!("create local state directory {}", directory.display()),
                error,
            )
        })?;
        let path = directory.join(name);
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&path)
            .map_err(|error| {
                local_error(format!("open local state lease {}", path.display()), error)
            })?;
        Self::acquire_file(file, &path)
    }

    /// Acquire one nonblocking exclusive lease for a caller-opened lock file.
    ///
    /// The caller keeps responsibility for fd-relative opening, directory
    /// admission, permissions and any parent fsync. The shared lease owns only
    /// the lock operation, its Busy classification and file lifetime.
    pub(crate) fn acquire_file(file: File, path: &Path) -> Result<Self> {
        file.try_lock().map_err(|error| match error {
            std::fs::TryLockError::WouldBlock => Error::new(
                ErrorKind::Busy,
                format!("local state lease {} is held", path.display()),
            ),
            std::fs::TryLockError::Error(error) => {
                local_error(format!("lock local state lease {}", path.display()), error)
            }
        })?;
        Ok(Self { file })
    }

    pub(crate) fn into_file(self) -> File {
        self.file
    }
}

fn local_error(context: String, error: std::io::Error) -> Error {
    Error::new(
        super::store::classify_io(&error),
        format!("{context}: {error}"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lease_acquisition_creates_only_the_lock_file() {
        let temp = tempfile::tempdir().unwrap();
        let state = temp.path().join("state");
        let lease = StateLease::acquire(&state, "state.lock").unwrap();
        assert!(state.join("state.lock").is_file());
        assert!(!state.join("state.db").exists());

        let error = match StateLease::acquire(&state, "state.lock") {
            Ok(_) => panic!("a second local-state writer acquired the lease"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), ErrorKind::Busy);

        drop(lease);
        StateLease::acquire(&state, "state.lock").unwrap();
    }
}
