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

use borsh::{BorshDeserialize, BorshSerialize};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::time::SystemTime;
use tokio::io::{AsyncSeekExt as _, AsyncWriteExt as _};
use yinyang_core::{
    CommitId, CommitOutcome, ContentDescriptor, ContentId, Error, ErrorKind, Fs, Node, NodeKind,
    Planner, Result, Revision, Snapshot, Transaction,
};

mod journal;
pub use journal::{
    DirectoryInspection, DirectoryIntent, DirectoryJournal, DirectoryState, DirectoryStatus,
};

struct PreparedDirectory {
    transaction: Transaction,
    manifest: DirectoryManifest,
}

#[derive(Clone, BorshSerialize, BorshDeserialize)]
struct DirectoryManifest {
    entries: Vec<ManifestEntry>,
}

#[derive(Clone, BorshSerialize, BorshDeserialize)]
struct ManifestEntry {
    path: String,
    kind: ManifestKind,
    executable: bool,
    content: Option<Vec<u8>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, BorshSerialize, BorshDeserialize)]
enum ManifestKind {
    Directory,
    File,
}

/// Prepare one complete replacement, including predicates over every observed
/// node and directory. Keep the returned transaction to retry identical intent
/// without scanning local files or uploading prepared content again.
pub async fn prepare_directory(
    fs: &Fs,
    observed: &Snapshot,
    source: &Path,
    id: CommitId,
) -> Result<Transaction> {
    Ok(prepare_directory_request(fs, observed, source, id)
        .await?
        .transaction)
}

async fn prepare_directory_request(
    fs: &Fs,
    observed: &Snapshot,
    source: &Path,
    id: CommitId,
) -> Result<PreparedDirectory> {
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
    let mut contents = BTreeMap::<String, ContentDescriptor>::new();
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
            NodeKind::File(f) => Some(f.clone()),
            _ => None,
        });
        let identity = ContentId::calculate(&mut input).await?;
        let content = if old
            .as_ref()
            .is_none_or(|file| file.content_id() != identity)
        {
            input.rewind().await.map_err(|e| io_error(&local, e))?;
            let prepared = fs.data().prepare(&mut input).await?;
            if prepared.content_id() != identity {
                return Err(source_changed());
            }
            let content = prepared.descriptor().clone();
            planner.set_content(identities[path], prepared).await?;
            content
        } else {
            old.expect("checked above")
        };
        if identity.length() != entry.length
            || fingerprint(&input.metadata().await.map_err(|e| io_error(&local, e))?)? != *entry
        {
            return Err(source_changed());
        }
        contents.insert(path.clone(), content);
    }
    if scan(source).await? != manifest {
        return Err(source_changed());
    }
    let transaction = planner.finish()?;
    let entries = manifest
        .into_iter()
        .map(|(path, entry)| {
            let kind = if entry.directory {
                ManifestKind::Directory
            } else {
                ManifestKind::File
            };
            let content = if entry.directory {
                None
            } else {
                Some(
                    contents
                        .get(&path)
                        .ok_or_else(|| invalid("prepared file has no content descriptor"))?
                        .to_bytes(),
                )
            };
            Ok(ManifestEntry {
                path,
                kind,
                executable: entry.executable,
                content,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(PreparedDirectory {
        transaction,
        manifest: DirectoryManifest { entries },
    })
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
    let entries = nodes
        .into_iter()
        .map(|(path, node)| {
            let (kind, content) = match node.kind() {
                NodeKind::Directory { .. } => (ManifestKind::Directory, None),
                NodeKind::File(file) => (ManifestKind::File, Some(file.clone())),
            };
            InstallEntry {
                path,
                kind,
                executable: node.executable(),
                content,
            }
        })
        .collect();
    install_directory(fs, destination, entries).await
}

/// Restore a retained historical revision into a new destination.
pub async fn restore_directory_revision(
    fs: &Fs,
    revision: Revision,
    destination: &Path,
) -> Result<()> {
    let observed = fs.observe_revision(revision).await?;
    restore_directory(fs, &observed, destination).await
}

struct InstallEntry {
    path: String,
    kind: ManifestKind,
    executable: bool,
    content: Option<ContentDescriptor>,
}

async fn install_directory(
    fs: &Fs,
    destination: &Path,
    mut entries: Vec<InstallEntry>,
) -> Result<()> {
    validate_install_entries(fs, &entries)?;
    entries.sort_by(|left, right| left.path.cmp(&right.path));
    tokio::fs::create_dir(destination)
        .await
        .map_err(|e| io_error(destination, e))?;
    for entry in entries
        .iter()
        .filter(|entry| matches!(entry.kind, ManifestKind::Directory) && !entry.path.is_empty())
    {
        let target = destination.join(&entry.path);
        tokio::fs::create_dir(&target)
            .await
            .map_err(|e| io_error(&target, e))?;
    }
    for entry in entries {
        if matches!(entry.kind, ManifestKind::Directory) {
            continue;
        }
        let path = entry.path;
        let file = entry
            .content
            .expect("validated file has a content descriptor");
        let target = destination.join(&path);
        let staged =
            tempfile::NamedTempFile::new_in(target.parent().expect("non-root file has a parent"))
                .map_err(|e| io_error(&target, e))?;
        let (output, temporary) = staged.into_parts();
        let mut output = tokio::fs::File::from_std(output);
        let copied = fs
            .data()
            .read_range(&file, 0..file.content_id().length(), &mut output)
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
                .set_permissions(std::fs::Permissions::from_mode(if entry.executable {
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
    Ok(())
}

fn validate_install_entries(fs: &Fs, entries: &[InstallEntry]) -> Result<()> {
    let mut paths = BTreeMap::<&str, &InstallEntry>::new();
    for entry in entries {
        if paths.insert(&entry.path, entry).is_some() {
            return Err(invalid("directory manifest contains a duplicate path"));
        }
        if entry.executable && (cfg!(not(unix)) || matches!(entry.kind, ManifestKind::Directory)) {
            return Err(Error::new(
                ErrorKind::Unsupported,
                "restore directory",
                "this destination cannot represent the executable attribute",
            ));
        }
        match (entry.kind, &entry.content) {
            (ManifestKind::Directory, None) => {}
            (ManifestKind::File, Some(content)) if content.filesystem() == fs.filesystem() => {}
            (ManifestKind::File, Some(_)) => {
                return Err(invalid(
                    "directory manifest content belongs to another filesystem",
                ));
            }
            _ => return Err(invalid("directory manifest kind and content disagree")),
        }
        if entry.path.is_empty() {
            if !matches!(entry.kind, ManifestKind::Directory) {
                return Err(invalid("directory manifest root is not a directory"));
            }
            if entry.executable {
                return Err(invalid("directory manifest root is executable"));
            }
            continue;
        }
        if entry.path.starts_with('/')
            || entry.path.ends_with('/')
            || entry.path.contains("//")
            || entry.path.contains('\\')
        {
            return Err(invalid("directory manifest contains a non-relative path"));
        }
        for name in entry.path.split('/') {
            yinyang_core::namespace::name_key(name)?;
        }
    }
    if !matches!(
        paths.get("").map(|entry| entry.kind),
        Some(ManifestKind::Directory)
    ) {
        return Err(invalid("directory manifest root is missing"));
    }
    for entry in entries.iter().filter(|entry| !entry.path.is_empty()) {
        let (parent, name) = split(&entry.path);
        if name.is_empty() {
            return Err(invalid("directory manifest contains an empty name"));
        }
        let parent_entry = paths
            .get(parent)
            .ok_or_else(|| invalid("directory manifest parent is missing"))?;
        if !matches!(parent_entry.kind, ManifestKind::Directory) {
            return Err(invalid("directory manifest parent is not a directory"));
        }
    }
    let mut slots = BTreeSet::new();
    for entry in entries.iter().filter(|entry| !entry.path.is_empty()) {
        let (parent, name) = split(&entry.path);
        if !slots.insert((parent.to_owned(), yinyang_core::namespace::name_key(name)?)) {
            return Err(invalid("directory manifest has a case-folded collision"));
        }
    }
    Ok(())
}

impl DirectoryManifest {
    fn install_entries(&self) -> Result<Vec<InstallEntry>> {
        self.entries
            .iter()
            .map(|entry| {
                let content = entry
                    .content
                    .as_deref()
                    .map(ContentDescriptor::from_bytes)
                    .transpose()?;
                Ok(InstallEntry {
                    path: entry.path.clone(),
                    kind: entry.kind,
                    executable: entry.executable,
                    content,
                })
            })
            .collect()
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::support::TestBackend;
    use yinyang_core::BackendProfile;

    #[tokio::test]
    async fn install_rejects_noncanonical_paths_before_creating_the_destination() {
        let backend = TestBackend::default();
        let fs = Fs::create(backend.operator(), BackendProfile::Minio)
            .await
            .unwrap();
        let temp = tempfile::tempdir().unwrap();
        for (index, path) in [
            "/outside",
            "../outside",
            "nested//outside",
            "nested/",
            "nested\\outside",
            "C:/outside",
        ]
        .into_iter()
        .enumerate()
        {
            let destination = temp.path().join(format!("destination-{index}"));
            let entries = vec![
                InstallEntry {
                    path: String::new(),
                    kind: ManifestKind::Directory,
                    executable: false,
                    content: None,
                },
                InstallEntry {
                    path: path.to_owned(),
                    kind: ManifestKind::Directory,
                    executable: false,
                    content: None,
                },
            ];
            let error = install_directory(&fs, &destination, entries)
                .await
                .unwrap_err();
            assert!(
                matches!(error.kind(), ErrorKind::Invalid | ErrorKind::InvalidName),
                "{path}: {error}"
            );
            assert!(!destination.exists(), "{path} created the destination");
        }
    }
}
