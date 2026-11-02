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

use super::store::LocalFingerprint;
use super::{Error, Result};
use rustix::fd::OwnedFd;
use rustix::fs::{AtFlags, Dir, FileType, Mode, OFlags, Stat, fstat, open, openat, statat};
use std::collections::{BTreeMap, BTreeSet};
use std::io::Read;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct ContentHash([u8; 32]);

impl ContentHash {
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LocalKind {
    Directory,
    File { length: u64, digest: ContentHash },
}

#[derive(
    Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, serde::Deserialize, serde::Serialize,
)]
pub struct Identity {
    pub dev: u64,
    pub ino: u64,
}

impl Identity {
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let bytes: [u8; 16] = bytes
            .try_into()
            .map_err(|_| Error::corrupt("invalid local identity length"))?;
        Ok(Self {
            dev: u64::from_be_bytes(bytes[..8].try_into().expect("fixed device")),
            ino: u64::from_be_bytes(bytes[8..].try_into().expect("fixed inode")),
        })
    }

    pub fn to_bytes(self) -> [u8; 16] {
        let mut bytes = [0_u8; 16];
        bytes[..8].copy_from_slice(&self.dev.to_be_bytes());
        bytes[8..].copy_from_slice(&self.ino.to_be_bytes());
        bytes
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LocalStat {
    pub dev: u64,
    pub ino: u64,
    pub size: u64,
    pub mtime: i64,
    pub mtime_nsec: i64,
    pub ctime: i64,
    pub ctime_nsec: i64,
}

impl LocalStat {
    pub(crate) fn from_stat(stat: &Stat) -> Self {
        Self {
            dev: stat.st_dev,
            ino: stat.st_ino,
            size: stat.st_size as u64,
            mtime: stat.st_mtime,
            mtime_nsec: stat.st_mtime_nsec as i64,
            ctime: stat.st_ctime,
            ctime_nsec: stat.st_ctime_nsec as i64,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LocalNode {
    pub path: String,
    pub kind: LocalKind,
    pub executable: bool,
    pub identity: Identity,
    pub parent: Option<Identity>,
    pub stat: Option<LocalStat>,
}

impl LocalNode {
    pub fn is_directory(&self) -> bool {
        matches!(self.kind, LocalKind::Directory)
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct LocalSnapshot {
    nodes: BTreeMap<String, LocalNode>,
}

impl LocalSnapshot {
    pub(super) fn from_nodes(nodes: BTreeMap<String, LocalNode>) -> Self {
        Self { nodes }
    }

    pub fn nodes(&self) -> &BTreeMap<String, LocalNode> {
        &self.nodes
    }

    pub fn get(&self, path: &str) -> Option<&LocalNode> {
        self.nodes.get(path)
    }

    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    pub fn subtree(&self, root: &str) -> BTreeMap<String, LocalNode> {
        self.nodes
            .iter()
            .filter_map(|(path, node)| {
                let relative = if root.is_empty() {
                    Some(path.as_str())
                } else if path == root {
                    Some("")
                } else {
                    path.strip_prefix(root)
                        .and_then(|suffix| suffix.strip_prefix('/'))
                }?;
                let mut node = node.clone();
                node.path = relative.to_owned();
                Some((relative.to_owned(), node))
            })
            .collect()
    }

    pub fn insert_node(&mut self, node: LocalNode) -> Option<LocalNode> {
        self.nodes.insert(node.path.clone(), node)
    }

    pub fn remove_path(&mut self, path: &str) -> Option<LocalNode> {
        self.nodes.remove(path)
    }

    pub fn remove_subtree(&mut self, path: &str) {
        let prefix = format!("{path}/");
        self.nodes
            .retain(|candidate, _| candidate != path && !candidate.starts_with(&prefix));
    }

    pub fn move_subtree(&mut self, from: &str, to: &str, observed_root: &LocalNode) {
        let prefix = format!("{from}/");
        let moved = self
            .nodes
            .iter()
            .filter(|(path, _)| *path == from || path.starts_with(&prefix))
            .map(|(path, node)| {
                let destination = if path == from {
                    to.to_owned()
                } else {
                    format!("{to}{}", &path[from.len()..])
                };
                (path.clone(), destination, node.clone())
            })
            .collect::<Vec<_>>();
        for (source, _, _) in &moved {
            self.nodes.remove(source);
        }
        for (source, destination, mut node) in moved {
            node.path = destination.clone();
            if source == from {
                node.identity = observed_root.identity;
                node.parent = observed_root.parent;
            }
            self.nodes.insert(destination, node);
        }
    }
}

pub(super) fn scan_path_with_hints(
    path: &Path,
    hints: &BTreeMap<String, LocalFingerprint>,
) -> Result<LocalSnapshot> {
    scan_path_with_hints_counted(path, hints, None)
}

pub(super) fn scan_path_with_hints_counted(
    path: &Path,
    hints: &BTreeMap<String, LocalFingerprint>,
    hash_counter: Option<&AtomicU64>,
) -> Result<LocalSnapshot> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| Error::unsupported("observation path requires a UTF-8 file name"))?;
    let (parent_fd, root_dev) = open_bound_directory(parent, "open observation parent")?;
    let stat = statat(&parent_fd, name, AtFlags::SYMLINK_NOFOLLOW)
        .map_err(|error| Error::errno("inspect observation path", path, error))?;
    let root_identity = identity(&stat);
    let mut nodes = BTreeMap::new();
    match FileType::from_raw_mode(stat.st_mode) {
        FileType::Directory => {
            let fd = openat(
                &parent_fd,
                name,
                OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                Mode::empty(),
            )
            .map_err(|error| Error::errno("open observation directory", path, error))?;
            let opened = fstat(&fd)
                .map_err(|error| Error::errno("inspect observation directory", path, error))?;
            if opened.st_dev != root_dev.dev || opened.st_dev != root_identity.dev {
                return Err(Error::unsupported(format!(
                    "observation directory {} changed filesystem",
                    path.display()
                )));
            }
            nodes.insert(
                String::new(),
                LocalNode {
                    path: String::new(),
                    kind: LocalKind::Directory,
                    executable: false,
                    identity: root_identity,
                    parent: None,
                    stat: Some(LocalStat::from_stat(&opened)),
                },
            );
            scan_directory(
                &fd,
                "",
                root_identity,
                path,
                root_identity.dev,
                hints,
                hash_counter,
                &mut nodes,
            )?;
        }
        FileType::RegularFile => {
            let mut node = match hints.get("").filter(|hint| hint_matches(&stat, hint)) {
                Some(hint) => node_from_hint("", hint, Some(root_identity)),
                None => {
                    let (length, digest, executable) =
                        hash_counted(&parent_fd, name, path, hash_counter)?;
                    LocalNode {
                        path: String::new(),
                        kind: LocalKind::File { length, digest },
                        executable,
                        identity: root_identity,
                        parent: None,
                        stat: Some(LocalStat::from_stat(&stat)),
                    }
                }
            };
            node.path = String::new();
            node.parent = None;
            node.stat = Some(LocalStat::from_stat(&stat));
            nodes.insert(String::new(), node);
        }
        FileType::Symlink => {
            return Err(Error::unsupported(format!(
                "{} is a symlink",
                path.display()
            )));
        }
        _ => {
            return Err(Error::unsupported(format!(
                "{} is a special file",
                path.display()
            )));
        }
    }
    Ok(LocalSnapshot::from_nodes(nodes))
}

pub fn scan_directory(
    directory: &OwnedFd,
    prefix: &str,
    directory_identity: Identity,
    display_root: &Path,
    filesystem_dev: u64,
    hints: &BTreeMap<String, LocalFingerprint>,
    hash_counter: Option<&AtomicU64>,
    nodes: &mut BTreeMap<String, LocalNode>,
) -> Result<()> {
    let mut reader =
        Dir::read_from(directory).map_err(|e| Error::errno("read directory", display_root, e))?;
    let mut slots = BTreeSet::new();
    while let Some(entry) = reader.read() {
        let entry = entry.map_err(|e| Error::errno("read directory", display_root, e))?;
        let name = entry
            .file_name()
            .to_str()
            .map_err(|_| Error::unsupported("non-UTF-8 local filename"))?;
        if name == "." || name == ".." {
            continue;
        }
        let folded = yinyang_core::namespace::name_key(name)
            .map_err(|e| Error::invalid(format!("invalid local name {name:?}: {e}")))?;
        if !slots.insert(folded) {
            return Err(Error::invalid(format!(
                "case-folded local name collision below {prefix:?}"
            )));
        }
        let path = join(prefix, name);
        let display = display_root.join(&path);
        let stat = statat(directory, name, AtFlags::SYMLINK_NOFOLLOW)
            .map_err(|e| Error::errno("inspect local entry", &display, e))?;
        match FileType::from_raw_mode(stat.st_mode) {
            FileType::Directory => {
                let fd = openat(
                    directory,
                    name,
                    OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                    Mode::empty(),
                )
                .map_err(|e| Error::errno("open local directory", &display, e))?;
                let opened =
                    fstat(&fd).map_err(|e| Error::errno("inspect local directory", &display, e))?;
                if opened.st_dev != filesystem_dev {
                    return Err(Error::unsupported(format!(
                        "local directory {} crosses a filesystem boundary",
                        path
                    )));
                }
                let identity = identity(&opened);
                nodes.insert(
                    path.clone(),
                    LocalNode {
                        path: path.clone(),
                        kind: LocalKind::Directory,
                        executable: false,
                        identity,
                        parent: Some(directory_identity),
                        stat: Some(LocalStat::from_stat(&opened)),
                    },
                );
                scan_directory(
                    &fd,
                    &path,
                    identity,
                    display_root,
                    filesystem_dev,
                    hints,
                    hash_counter,
                    nodes,
                )?;
            }
            FileType::RegularFile => {
                let mut node = match hints.get(&path).filter(|hint| hint_matches(&stat, hint)) {
                    Some(hint) => node_from_hint(&path, hint, Some(directory_identity)),
                    None => {
                        let (length, digest, executable) =
                            hash_counted(directory, name, &display, hash_counter)?;
                        LocalNode {
                            path: path.clone(),
                            kind: LocalKind::File { length, digest },
                            executable,
                            identity: identity(&stat),
                            parent: Some(directory_identity),
                            stat: Some(LocalStat::from_stat(&stat)),
                        }
                    }
                };
                node.path = path.clone();
                node.parent = Some(directory_identity);
                node.stat = Some(LocalStat::from_stat(&stat));
                nodes.insert(path, node);
            }
            FileType::Symlink => {
                return Err(Error::unsupported(format!(
                    "local symlink is not supported: {path}"
                )));
            }
            _ => {
                return Err(Error::unsupported(format!(
                    "local special file is not supported: {path}"
                )));
            }
        }
    }
    Ok(())
}

pub fn hash_file_at(parent: &OwnedFd, name: &str, path: &Path) -> Result<(u64, ContentHash, bool)> {
    let fd = openat(
        parent,
        name,
        OFlags::RDONLY | OFlags::NONBLOCK | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map_err(|e| Error::errno("open local file", path, e))?;
    let before = fstat(&fd).map_err(|e| Error::errno("inspect local file", path, e))?;
    if FileType::from_raw_mode(before.st_mode) != FileType::RegularFile {
        return Err(Error::unsupported(format!(
            "{} is not a regular file",
            path.display()
        )));
    }
    if before.st_nlink > 1 {
        return Err(Error::unsupported(format!(
            "{} is hard-linked; hard links are not part of the sync contract",
            path.display()
        )));
    }
    let mut file = std::fs::File::from(fd);
    let mut hasher = blake3::Hasher::new();
    let mut buffer = vec![0_u8; 64 * 1024];
    loop {
        let count = file
            .read(&mut buffer)
            .map_err(|e| Error::io("read local file", path, e))?;
        if count == 0 {
            break;
        }
        hasher.update(&buffer[..count]);
    }
    let after = fstat(&file).map_err(|e| Error::errno("recheck local file", path, e))?;
    if !same_stable_stat(&before, &after) {
        return Err(Error::conflict(format!(
            "{} changed while it was being scanned",
            path.display()
        )));
    }
    Ok((
        after.st_size as u64,
        ContentHash(*hasher.finalize().as_bytes()),
        Mode::from_raw_mode(after.st_mode).intersects(Mode::XUSR | Mode::XGRP | Mode::XOTH),
    ))
}

fn hash_counted(
    parent: &OwnedFd,
    name: &str,
    path: &Path,
    counter: Option<&AtomicU64>,
) -> Result<(u64, ContentHash, bool)> {
    let result = hash_file_at(parent, name, path)?;
    if let Some(counter) = counter {
        counter.fetch_add(result.0, Ordering::Relaxed);
    }
    Ok(result)
}

pub fn open_bound_directory(path: &Path, operation: &str) -> Result<(OwnedFd, Identity)> {
    let fd = open(
        path,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map_err(|e| Error::errno(operation, path, e))?;
    let stat = fstat(&fd).map_err(|e| Error::errno(operation, path, e))?;
    if FileType::from_raw_mode(stat.st_mode) != FileType::Directory {
        return Err(Error::invalid(format!(
            "{} is not a directory",
            path.display()
        )));
    }
    Ok((fd, identity(&stat)))
}

pub fn identity(stat: &Stat) -> Identity {
    Identity {
        dev: stat.st_dev,
        ino: stat.st_ino,
    }
}

pub fn same_stable_stat(left: &Stat, right: &Stat) -> bool {
    left.st_dev == right.st_dev
        && left.st_ino == right.st_ino
        && left.st_size == right.st_size
        && left.st_mtime == right.st_mtime
        && left.st_mtime_nsec == right.st_mtime_nsec
        && left.st_ctime == right.st_ctime
        && left.st_ctime_nsec == right.st_ctime_nsec
}

fn node_from_hint(path: &str, hint: &LocalFingerprint, parent: Option<Identity>) -> LocalNode {
    LocalNode {
        path: path.to_owned(),
        kind: LocalKind::File {
            length: hint.length,
            digest: ContentHash::from_bytes(hint.digest),
        },
        executable: hint.executable,
        identity: hint.identity(),
        parent,
        stat: hint.stat.map(|stat| LocalStat {
            dev: stat.dev,
            ino: stat.ino,
            size: hint.length,
            mtime: stat.mtime,
            mtime_nsec: stat.mtime_nsec,
            ctime: stat.ctime,
            ctime_nsec: stat.ctime_nsec,
        }),
    }
}

fn hint_matches(current: &Stat, hint: &LocalFingerprint) -> bool {
    let Some(stored) = hint.stat else {
        return false;
    };
    stored.dev == current.st_dev
        && stored.ino == current.st_ino
        && hint.length == current.st_size as u64
        && stored.mtime == current.st_mtime
        && stored.mtime_nsec == current.st_mtime_nsec as i64
        && stored.ctime == current.st_ctime
        && stored.ctime_nsec == current.st_ctime_nsec as i64
        && local_stat_is_racy_clean(
            stored.mtime,
            stored.mtime_nsec,
            stored.ctime,
            stored.ctime_nsec,
            hint.recorded_ns,
        )
}

pub(super) fn local_stat_is_racy_clean(
    mtime: i64,
    mtime_nsec: i64,
    ctime: i64,
    ctime_nsec: i64,
    recorded_ns: i64,
) -> bool {
    let timestamp_ns = |seconds: i64, nanos: i64| -> i128 {
        i128::from(seconds) * 1_000_000_000 + i128::from(nanos)
    };
    let cutoff = i128::from(recorded_ns) - 1_000_000_000;
    timestamp_ns(mtime, mtime_nsec) < cutoff && timestamp_ns(ctime, ctime_nsec) < cutoff
}

fn join(parent: &str, name: &str) -> String {
    if parent.is_empty() {
        name.to_owned()
    } else {
        format!("{parent}/{name}")
    }
}

pub fn validate_path(path: &str) -> Result<()> {
    if path.is_empty() {
        return Ok(());
    }
    if path.starts_with('/') || path.ends_with('/') {
        return Err(Error::invalid(
            "local path must be relative without a trailing slash",
        ));
    }
    for component in path.split('/') {
        if component.is_empty() {
            return Err(Error::invalid("local path contains an empty component"));
        }
        yinyang_core::namespace::name_key(component)
            .map_err(|e| Error::invalid(format!("invalid local path component: {e}")))?;
    }
    Ok(())
}

pub fn split_path(path: &str) -> Result<(&str, &str)> {
    if path.is_empty() {
        return Err(Error::invalid(
            "the synchronized root is not a mutation target",
        ));
    }
    validate_path(path)?;
    Ok(path.rsplit_once('/').unwrap_or(("", path)))
}
