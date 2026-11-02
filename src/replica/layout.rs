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

use super::{Error, Result};
use std::path::{Path, PathBuf};

/// Reject equal or nested root/state paths and require one local filesystem.
pub fn check_root_state(root: &Path, state: &Path) -> Result<()> {
    let root = canonical_target(root, "root")?;
    let state = canonical_target(state, "state")?;
    if root == state {
        return Err(Error::invalid("root and state must be different"));
    }
    if state.starts_with(&root) {
        return Err(Error::invalid(
            "state must not be inside the synchronized root",
        ));
    }
    if root.starts_with(&state) {
        return Err(Error::invalid(
            "the synchronized root must not be inside state",
        ));
    }
    #[cfg(unix)]
    if device_of(&root)? != device_of(&state)? {
        return Err(Error::unsupported(
            "root and state must be on the same filesystem",
        ));
    }
    Ok(())
}

fn canonical_target(path: &Path, label: &str) -> Result<PathBuf> {
    if path.exists() {
        return std::fs::canonicalize(path)
            .map_err(|error| Error::io(&format!("canonicalize {label}"), path, error));
    }
    let mut missing = Vec::new();
    let mut ancestor = path;
    loop {
        match std::fs::metadata(ancestor) {
            Ok(_) => break,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let name = ancestor.file_name().ok_or_else(|| {
                    Error::invalid(format!("{label} path has no existing ancestor"))
                })?;
                missing.push(name.to_owned());
                ancestor = ancestor.parent().ok_or_else(|| {
                    Error::invalid(format!("{label} path has no existing ancestor"))
                })?;
            }
            Err(error) => {
                return Err(Error::io("inspect root/state path", ancestor, error));
            }
        }
    }
    let mut canonical = std::fs::canonicalize(ancestor)
        .map_err(|error| Error::io(&format!("canonicalize {label} ancestor"), ancestor, error))?;
    for component in missing.into_iter().rev() {
        if component == std::ffi::OsStr::new(".") || component == std::ffi::OsStr::new("..") {
            return Err(Error::invalid(format!(
                "{label} path must not contain unresolved . or .. components"
            )));
        }
        canonical.push(component);
    }
    Ok(canonical)
}

#[cfg(unix)]
fn device_of(path: &Path) -> Result<u64> {
    use std::os::unix::fs::MetadataExt as _;
    let mut candidate = path;
    loop {
        match std::fs::metadata(candidate) {
            Ok(metadata) => return Ok(metadata.dev()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                candidate = candidate
                    .parent()
                    .ok_or_else(|| Error::invalid("root/state path has no existing ancestor"))?;
            }
            Err(error) => {
                return Err(Error::io("inspect root/state device", candidate, error));
            }
        }
    }
}
