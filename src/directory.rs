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

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::time::SystemTime;
use tokio::io::{AsyncSeekExt as _, AsyncWriteExt as _};
use yinyang_core::{
    CommitId, CommitOutcome, ContentId, Error, ErrorKind, Fs, Node, NodeKind, Planner, Result,
    Snapshot, Transaction,
};

/// Prepare one complete replacement, including predicates over every observed
/// node and directory. Keep the returned transaction to retry identical intent
/// without scanning local files or uploading prepared content again.
pub async fn prepare_directory(
    fs: &Fs,
    observed: &Snapshot,
    source: &Path,
    id: CommitId,
) -> Result<Transaction> {
    check_filesystem(fs, observed)?;
    let manifest = scan(source).await?;
    // Recover readiness under this process's authority, including after reopen.
    let observed = fs.observe_revision(observed.revision()).await?;
    let mut planner = Planner::new(&observed, id);
    let mut remote = BTreeMap::<String, Node>::new();
    let mut pending = vec![(String::new(), observed.root())];
    while let Some((path, id)) = pending.pop() {
        let node = planner
            .node(id)
            .await?
            .ok_or_else(|| invalid("observed node disappeared"))?;
        if node.is_directory() {
            for entry in planner.scan(id).await? {
                pending.push((join(&path, &entry.name), entry.node_id));
            }
        }
        remote.insert(path, node);
    }
    for (path, node) in remote.iter().rev().filter(|(path, _)| !path.is_empty()) {
        if manifest
            .get(path)
            .is_none_or(|entry| entry.directory != node.is_directory())
        {
            planner.remove(node.id()).await?;
        }
    }
    let empty = fs.data().prepare(&mut b"".as_slice()).await?;
    let mut identities = BTreeMap::from([(String::new(), fs.root())]);
    for (path, entry) in &manifest {
        let id = if path.is_empty() {
            fs.root()
        } else if let Some(node) = remote
            .get(path)
            .filter(|n| n.is_directory() == entry.directory)
        {
            node.id()
        } else {
            let (parent, name) = split(path);
            if entry.directory {
                planner.create_directory(identities[parent], name).await?
            } else {
                planner
                    .create_file(identities[parent], name, empty.clone(), entry.executable)
                    .await?
            }
        };
        identities.insert(path.clone(), id);
        planner.set_executable(id, entry.executable).await?;
    }
    for (path, entry) in &manifest {
        if entry.directory {
            continue;
        }
        let local = source.join(path);
        let mut input = tokio::fs::File::open(&local)
            .await
            .map_err(|e| io_error(&local, e))?;
        if fingerprint(&input.metadata().await.map_err(|e| io_error(&local, e))?)? != *entry {
            return Err(source_changed());
        }
        let old = remote.get(path).and_then(|node| match node.kind() {
            NodeKind::File(f) => Some(f),
            _ => None,
        });
        let identity = ContentId::calculate(&mut input).await?;
        if old.is_none_or(|f| f.content_id() != identity) {
            input.rewind().await.map_err(|e| io_error(&local, e))?;
            let prepared = fs.data().prepare(&mut input).await?;
            if prepared.content_id() != identity {
                return Err(source_changed());
            }
            planner.set_content(identities[path], prepared).await?;
        }
        if identity.length() != entry.length
            || fingerprint(&input.metadata().await.map_err(|e| io_error(&local, e))?)? != *entry
        {
            return Err(source_changed());
        }
    }
    if scan(source).await? != manifest {
        return Err(source_changed());
    }
    planner.finish()
}

/// One-shot convenience. For an uncertain retry, retain prepare_directory's
/// Transaction and call Fs::commit again; do not replan under the same identity.
pub async fn publish_directory(
    fs: &Fs,
    observed: &Snapshot,
    source: &Path,
    id: CommitId,
) -> Result<CommitOutcome> {
    let request = prepare_directory(fs, observed, source, id).await?;
    fs.commit(&request).await
}

/// Materialize names only for a whole-directory operation, not core publication.
pub async fn directory_nodes(observed: &Snapshot) -> Result<BTreeMap<String, Node>> {
    let mut nodes = BTreeMap::new();
    let mut pending = vec![(String::new(), observed.root())];
    while let Some((path, id)) = pending.pop() {
        let node = observed
            .node(id)
            .await?
            .ok_or_else(|| invalid("observed node disappeared"))?;
        if node.is_directory() {
            let mut token = None;
            loop {
                let page = observed.scan(id, token.as_ref(), 4096).await?;
                for entry in page.entries {
                    pending.push((join(&path, &entry.name), entry.node_id));
                }
                token = page.next;
                if token.is_none() {
                    break;
                }
            }
        }
        nodes.insert(path, node);
    }
    Ok(nodes)
}

/// Restore a pinned snapshot into a new destination, installing each verified
/// file without replacement. Whole-directory installation is not atomic.
pub async fn restore_directory(fs: &Fs, observed: &Snapshot, destination: &Path) -> Result<()> {
    check_filesystem(fs, observed)?;
    let nodes = directory_nodes(observed).await?;
    for node in nodes.values() {
        if node.executable() && (cfg!(not(unix)) || node.is_directory()) {
            return Err(Error::new(
                ErrorKind::Unsupported,
                "restore directory",
                "this destination cannot represent the executable attribute",
            ));
        }
    }
    tokio::fs::create_dir(destination)
        .await
        .map_err(|e| io_error(destination, e))?;
    for (path, node) in nodes {
        if path.is_empty() {
            continue;
        }
        let target = destination.join(&path);
        match node.kind() {
            NodeKind::Directory { .. } => tokio::fs::create_dir(&target)
                .await
                .map_err(|e| io_error(&target, e))?,
            NodeKind::File(file) => {
                let staged = tempfile::NamedTempFile::new_in(
                    target.parent().expect("non-root file has a parent"),
                )
                .map_err(|e| io_error(&target, e))?;
                let (output, temporary) = staged.into_parts();
                let mut output = tokio::fs::File::from_std(output);
                let copied = fs
                    .data()
                    .read_range(file, 0..file.content_id().length(), &mut output)
                    .await;
                let flushed = output.flush().await.map_err(|e| io_error(&target, e));
                if let Err(error) = copied.and(flushed) {
                    drop(output);
                    return Err(error);
                }
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt as _;
                    output
                        .set_permissions(std::fs::Permissions::from_mode(if node.executable() {
                            0o700
                        } else {
                            0o600
                        }))
                        .await
                        .map_err(|e| io_error(&target, e))?;
                }
                output.sync_all().await.map_err(|e| io_error(&target, e))?;
                drop(output);
                temporary
                    .persist_noclobber(&target)
                    .map_err(|e| io_error(&target, e.error))?;
            }
        }
    }
    Ok(())
}
fn check_filesystem(fs: &Fs, observed: &Snapshot) -> Result<()> {
    if fs.filesystem() != observed.filesystem() || fs.root() != observed.root() {
        return Err(invalid("observation belongs to another filesystem"));
    }
    Ok(())
}
fn split(path: &str) -> (&str, &str) {
    path.rsplit_once('/').unwrap_or(("", path))
}
fn join(parent: &str, name: &str) -> String {
    if parent.is_empty() {
        name.to_owned()
    } else {
        format!("{parent}/{name}")
    }
}
fn invalid(message: &str) -> Error {
    Error::new(ErrorKind::Invalid, "transfer directory", message)
}

#[derive(Debug, Eq, PartialEq)]
struct LocalEntry {
    directory: bool,
    executable: bool,
    length: u64,
    modified: SystemTime,
}

async fn scan(root: &Path) -> Result<BTreeMap<String, LocalEntry>> {
    let metadata = tokio::fs::symlink_metadata(root)
        .await
        .map_err(|error| io_error(root, error))?;
    let entry = fingerprint(&metadata)?;
    if !entry.directory {
        return Err(Error::new(
            ErrorKind::Invalid,
            "scan directory",
            "source is not a directory",
        ));
    }
    let mut manifest = BTreeMap::from([(String::new(), entry)]);
    let mut pending = vec![(root.to_path_buf(), String::new())];
    let mut slots = BTreeSet::new();
    while let Some((directory, relative)) = pending.pop() {
        let mut entries = tokio::fs::read_dir(&directory)
            .await
            .map_err(|error| io_error(&directory, error))?;
        while let Some(entry) = entries
            .next_entry()
            .await
            .map_err(|error| io_error(&directory, error))?
        {
            let name = entry.file_name().into_string().map_err(|_| {
                Error::new(
                    ErrorKind::Unsupported,
                    "scan directory",
                    "non-UTF-8 filename",
                )
            })?;
            let folded = yinyang_core::namespace::name_key(&name)?;
            if !slots.insert((relative.clone(), folded)) {
                return Err(invalid("case-folded name collision"));
            }
            let path = if relative.is_empty() {
                name
            } else {
                format!("{relative}/{name}")
            };
            let local = entry.path();
            let metadata = tokio::fs::symlink_metadata(&local)
                .await
                .map_err(|error| io_error(&local, error))?;
            let fingerprint = fingerprint(&metadata)?;
            if fingerprint.directory {
                pending.push((local, path.clone()));
            }
            manifest.insert(path, fingerprint);
        }
    }
    Ok(manifest)
}

fn fingerprint(metadata: &std::fs::Metadata) -> Result<LocalEntry> {
    if !metadata.is_dir() && !metadata.is_file() {
        return Err(Error::new(
            ErrorKind::Unsupported,
            "scan directory",
            "symlinks and special files are not supported",
        ));
    }
    #[cfg(unix)]
    let executable = {
        use std::os::unix::fs::PermissionsExt as _;
        metadata.is_file() && metadata.permissions().mode() & 0o111 != 0
    };
    #[cfg(not(unix))]
    let executable = false;
    Ok(LocalEntry {
        directory: metadata.is_dir(),
        executable,
        length: if metadata.is_dir() { 0 } else { metadata.len() },
        modified: metadata
            .modified()
            .map_err(|error| Error::new(ErrorKind::Io, "scan directory", error.to_string()))?,
    })
}

fn source_changed() -> Error {
    Error::new(
        ErrorKind::Invalid,
        "publish directory",
        "source changed during publication; retry from a quiescent directory",
    )
}

fn io_error(path: &Path, error: std::io::Error) -> Error {
    let kind = if error.kind() == std::io::ErrorKind::AlreadyExists {
        ErrorKind::AlreadyExists
    } else {
        ErrorKind::Io
    };
    Error::new(
        kind,
        "access local directory",
        format!("{}: {error}", path.display()),
    )
}
