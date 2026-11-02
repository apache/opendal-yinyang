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
            Error::new(
                ErrorKind::Io,
                format!(
                    "create local state directory {}: {error}",
                    directory.display()
                ),
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
                Error::new(
                    ErrorKind::Io,
                    format!("open local state lease {}: {error}", path.display()),
                )
            })?;
        file.try_lock().map_err(|error| match error {
            std::fs::TryLockError::WouldBlock => Error::new(
                ErrorKind::Busy,
                format!("local state lease {} is held", path.display()),
            ),
            std::fs::TryLockError::Error(error) => Error::new(
                ErrorKind::Io,
                format!("lock local state lease {}: {error}", path.display()),
            ),
        })?;
        Ok(Self { file })
    }

    pub(crate) fn into_file(self) -> File {
        self.file
    }
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

        let error = StateLease::acquire(&state, "state.lock").unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Busy);

        drop(lease);
        StateLease::acquire(&state, "state.lock").unwrap();
    }
}
