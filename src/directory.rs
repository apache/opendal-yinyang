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

use std::collections::BTreeMap;
use std::path::Path;
use std::time::SystemTime;

use tokio::io::{AsyncReadExt as _, AsyncSeekExt as _, AsyncWriteExt as _};
use yinyang_core::{
    CommitId, CommitOutcome, ContentId, Error, ErrorKind, File, Fs, NodeBody, Observation,
    Path as FsPath, Result,
};

/// Publish a complete local directory as one remote namespace version.
///
/// The source must remain quiescent until completion. Remote-only entries are
/// removed. Same-path, same-kind nodes keep their identities; rename detection
/// is not attempted. Reuse the observation and commit ID for an uncertain retry.
/// A conflict is returned without rebasing or overwriting the winner.
pub async fn publish_directory(
    fs: &Fs,
    observed: &Observation,
    source: &Path,
    id: CommitId,
) -> Result<CommitOutcome> {
    check_filesystem(fs, observed)?;
    if observed.version().commits().contains(&id) {
        return fs.commit(observed, id, observed.tree().clone()).await;
    }
    let manifest = scan(source).await?;
    let mut edit = observed.edit();
    let old_paths = observed
        .tree()
        .iter()
        .map(|(path, _)| path.clone())
        .collect::<Vec<_>>();
    for path in old_paths
        .into_iter()
        .rev()
        .filter(|path| path != &FsPath::root())
    {
        let old = edit.tree().get(&path).expect("observed entry exists");
        if manifest
            .get(&path)
            .is_none_or(|entry| entry.directory != matches!(old.body(), NodeBody::Dir { .. }))
        {
            edit.remove(&path)?;
        }
    }
    // Validate all portable names and kind changes before uploading any content.
    for (path, entry) in &manifest {
        if path == &FsPath::root() {
            continue;
        }
        if edit.tree().get(path).is_none() {
            if entry.directory {
                edit.create_dir(path.clone(), false)?;
            } else {
                edit.create_file(path.clone(), empty_file(), entry.executable)?;
            }
        }
        edit.set_executable(path, entry.executable)?;
    }
    for (path, entry) in &manifest {
        if entry.directory {
            continue;
        }
        let local = source.join(path.as_str());
        let mut input = tokio::fs::File::open(&local)
            .await
            .map_err(|error| io_error(&local, error))?;
        if fingerprint(
            &input
                .metadata()
                .await
                .map_err(|error| io_error(&local, error))?,
        )? != *entry
        {
            return Err(source_changed());
        }
        let old_file = observed
            .tree()
            .get(path)
            .and_then(|node| match node.body() {
                NodeBody::File(file) => Some(file),
                NodeBody::Dir { .. } => None,
            });
        let content = if let Some(old) = old_file {
            if hash_file(&mut input).await? == old.content() {
                old.clone()
            } else {
                input
                    .rewind()
                    .await
                    .map_err(|error| io_error(&local, error))?;
                fs.write_file(&mut input).await?
            }
        } else {
            fs.write_file(&mut input).await?
        };
        if content.content().length() != entry.length
            || fingerprint(
                &input
                    .metadata()
                    .await
                    .map_err(|error| io_error(&local, error))?,
            )? != *entry
        {
            return Err(source_changed());
        }
        edit.replace_file(path, content)?;
    }
    if scan(source).await? != manifest {
        return Err(source_changed());
    }
    fs.commit(observed, id, edit.finish()?).await
}

/// Restore exactly the supplied immutable observation into a new directory.
///
/// The destination must not exist, and callers must prevent concurrent local
/// mutation. Each file is verified and synced before being installed without
/// replacement. Failure can leave directories and already verified files;
/// incomplete temporary files are removed on ordinary error. The whole tree
/// is not installed atomically, and a crash may leave temporary files.
pub async fn restore_directory(fs: &Fs, observed: &Observation, destination: &Path) -> Result<()> {
    check_filesystem(fs, observed)?;
    for (_, node) in observed.tree().iter() {
        if node.executable() && (cfg!(not(unix)) || matches!(node.body(), NodeBody::Dir { .. })) {
            return Err(Error::new(
                ErrorKind::Unsupported,
                "restore directory",
                "this destination cannot represent the executable attribute",
            ));
        }
    }
    tokio::fs::create_dir(destination)
        .await
        .map_err(|error| io_error(destination, error))?;
    for (path, node) in observed.tree().iter() {
        if path == &FsPath::root() {
            continue;
        }
        let target = destination.join(path.as_str());
        match node.body() {
            NodeBody::Dir { .. } => tokio::fs::create_dir(&target)
                .await
                .map_err(|error| io_error(&target, error))?,
            NodeBody::File(file) => {
                let staged = tempfile::NamedTempFile::new_in(
                    target.parent().expect("non-root file has a parent"),
                )
                .map_err(|error| io_error(&target, error))?;
                let (output, temporary) = staged.into_parts();
                let mut output = tokio::fs::File::from_std(output);
                let copied = fs.read_file(file, &mut output).await;
                let flushed = output
                    .flush()
                    .await
                    .map_err(|error| io_error(&target, error));
                if let Err(error) = copied.and(flushed) {
                    drop(output);
                    return Err(error);
                }
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt as _;
                    let mode = if node.executable() { 0o700 } else { 0o600 };
                    output
                        .set_permissions(std::fs::Permissions::from_mode(mode))
                        .await
                        .map_err(|error| io_error(&target, error))?;
                }
                output
                    .sync_all()
                    .await
                    .map_err(|error| io_error(&target, error))?;
                drop(output);
                temporary
                    .persist_noclobber(&target)
                    .map_err(|error| io_error(&target, error.error))?;
            }
        }
    }
    Ok(())
}

#[derive(Debug, Eq, PartialEq)]
struct LocalEntry {
    directory: bool,
    executable: bool,
    length: u64,
    modified: SystemTime,
}

async fn scan(root: &Path) -> Result<BTreeMap<FsPath, LocalEntry>> {
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
    let mut manifest = BTreeMap::from([(FsPath::root(), entry)]);
    let mut pending = vec![(root.to_path_buf(), String::new())];
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
            let path = FsPath::new(if relative.is_empty() {
                name
            } else {
                format!("{relative}/{name}")
            })?;
            let local = entry.path();
            let metadata = tokio::fs::symlink_metadata(&local)
                .await
                .map_err(|error| io_error(&local, error))?;
            let fingerprint = fingerprint(&metadata)?;
            if fingerprint.directory {
                pending.push((local, path.as_str().to_owned()));
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

async fn hash_file(input: &mut tokio::fs::File) -> Result<ContentId> {
    let mut buffer = vec![0; 256 * 1024];
    let mut hasher = blake3::Hasher::new();
    let mut length = 0_u64;
    loop {
        let count = input
            .read(&mut buffer)
            .await
            .map_err(|error| Error::new(ErrorKind::Io, "hash local file", error.to_string()))?;
        if count == 0 {
            break;
        }
        length = length
            .checked_add(count as u64)
            .ok_or_else(source_changed)?;
        hasher.update(&buffer[..count]);
    }
    Ok(ContentId::new(hasher.finalize().into(), length))
}

fn check_filesystem(fs: &Fs, observed: &Observation) -> Result<()> {
    if observed
        .tree()
        .get(&FsPath::root())
        .is_none_or(|root| root.id() != fs.root())
    {
        return Err(Error::new(
            ErrorKind::Invalid,
            "transfer directory",
            "observation belongs to another filesystem",
        ));
    }
    Ok(())
}

fn empty_file() -> File {
    File::new(ContentId::new(blake3::hash(&[]).into(), 0), Vec::new()).expect("valid empty content")
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
