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

//! fd-relative local storage shared by Managed and Direct Sync.

use super::adapter::{RemoteEntry, SyncModel};
use super::store::{
    ArtifactKind, ArtifactPhase, ArtifactRecord, ArtifactRelease, Binding, InstallOperation,
    InstallRecord, InstallState, LocalFingerprint, Store,
};
use super::types::{
    ContentHash, Identity, LocalKind, LocalNode, LocalSnapshot, open_bound_directory,
    same_stable_stat, scan_path_with_hints_counted, split_path, validate_path,
};
use super::{Error, ErrorKind, Result};
use rustix::fd::OwnedFd;
use rustix::fs::{
    AtFlags, FileType, FlockOperation, Mode, OFlags, RenameFlags, Stat, flock, fstat, fsync,
    mkdirat, openat, renameat_with, statat, unlinkat,
};
use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

pub const ARTIFACT_BUDGET_COUNT: usize = 64;
pub const ARTIFACT_BUDGET_BYTES: u64 = 256 * 1024 * 1024;

pub(super) struct LocalTree {
    root: PathBuf,
    state: PathBuf,
    root_fd: OwnedFd,
    root_identity: Identity,
    state_identity: Identity,
    install_fd: OwnedFd,
    artifact_fd: OwnedFd,
    _lease: std::fs::File,
    store: Store,
    hash_bytes_read: AtomicU64,
}

pub(super) struct ExistingBinding {
    pub root: PathBuf,
}

impl LocalTree {
    pub fn create(
        root: &Path,
        state: &Path,
        model: SyncModel,
        namespace: impl Into<String>,
    ) -> Result<Self> {
        prepare_directory(root, "create sync root")?;
        prepare_private_directory(state, "create sync state")?;
        let root = std::fs::canonicalize(root)
            .map_err(|error| Error::io("canonicalize sync root", root, error))?;
        let state = std::fs::canonicalize(state)
            .map_err(|error| Error::io("canonicalize sync state", state, error))?;
        check_root_state(&root, &state)?;
        let (root_fd, root_identity) = open_bound_directory(&root, "open sync root")?;
        let (state_fd, state_identity) = open_bound_directory(&state, "open sync state")?;
        if root_identity.dev != state_identity.dev {
            return Err(Error::unsupported(
                "sync root and state must share a filesystem for atomic local installs",
            ));
        }
        let lease = acquire_lease(&state_fd, &state)?;
        let namespace = namespace.into();
        let binding = Binding {
            model,
            namespace_digest: namespace_digest(&namespace),
            namespace,
            root: root.clone(),
            root_identity,
            state_identity,
        };
        let store = Store::create(&state.join("sync-engine.db"), &binding)?;
        let install_fd = open_private_directory(&state_fd, "install", &state)?;
        let artifact_fd = open_private_directory(&state_fd, "artifacts", &state)?;
        Ok(Self {
            root,
            state,
            root_fd,
            root_identity,
            state_identity,
            install_fd,
            artifact_fd,
            _lease: lease,
            store,
            hash_bytes_read: AtomicU64::new(0),
        })
    }

    pub fn open(state: &Path, model: SyncModel, namespace: impl Into<String>) -> Result<Self> {
        let state = std::fs::canonicalize(state)
            .map_err(|error| Error::io("canonicalize sync state", state, error))?;
        let (state_fd, state_identity) = open_bound_directory(&state, "open sync state")?;
        let _lease = acquire_lease(&state_fd, &state)?;
        let store = Store::open(&state.join("sync-engine.db"))?;
        let binding = store.binding()?;
        let namespace = namespace.into();
        if binding.namespace_digest != namespace_digest(&namespace)
            || binding.model != model
            || binding.namespace != namespace
        {
            return Err(Error::invalid(format!(
                "sync state is bound to {:?} namespace {:?}, not {:?} namespace {:?}",
                binding.model, binding.namespace, model, namespace
            )));
        }
        if binding.state_identity != state_identity {
            return Err(Error::invalid(
                "sync state identity changed since initialization",
            ));
        }
        let root = binding.root.clone();
        check_root_state(&root, &state)?;
        let (root_fd, root_identity) = open_bound_directory(&root, "open bound sync root")?;
        if root_identity != binding.root_identity {
            return Err(Error::invalid(
                "sync root identity changed since initialization",
            ));
        }
        if root_identity.dev != state_identity.dev {
            return Err(Error::unsupported(
                "sync root and state no longer share a filesystem",
            ));
        }
        let install_fd = open_private_directory(&state_fd, "install", &state)?;
        let artifact_fd = open_private_directory(&state_fd, "artifacts", &state)?;
        Ok(Self {
            root,
            state,
            root_fd,
            root_identity,
            state_identity,
            install_fd,
            artifact_fd,
            _lease,
            store,
            hash_bytes_read: AtomicU64::new(0),
        })
    }

    /// Validate an existing root and state without acquiring the writer lease
    /// or creating any state subdirectories.
    pub fn inspect(state: &Path) -> Result<(Store, Binding, ExistingBinding)> {
        let state = std::fs::canonicalize(state)
            .map_err(|error| Error::io("canonicalize sync state", state, error))?;
        let (_state_fd, state_identity) = open_bound_directory(&state, "open sync state")?;
        let store = Store::open_read_only(&state.join("sync-engine.db"))?;
        let binding = store.binding()?;
        if binding.state_identity != state_identity {
            return Err(Error::invalid(
                "sync state identity changed since initialization",
            ));
        }
        let root = binding.root.clone();
        check_root_state(&root, &state)?;
        let (_root_fd, root_identity) = open_bound_directory(&root, "open bound sync root")?;
        if root_identity != binding.root_identity {
            return Err(Error::invalid(
                "sync root identity changed since initialization",
            ));
        }
        Ok((store, binding.clone(), ExistingBinding { root }))
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn state(&self) -> &Path {
        &self.state
    }

    pub fn store(&self) -> &Store {
        &self.store
    }

    pub fn store_mut(&mut self) -> &mut Store {
        &mut self.store
    }

    pub fn binding(&self) -> Result<Binding> {
        self.store.binding()
    }

    /// Scan through the shared fd-relative scanner with persisted stat evidence.
    pub fn scan(&self) -> Result<LocalSnapshot> {
        let hints = self.scan_hints()?;
        self.hash_bytes_read.store(0, Ordering::Relaxed);
        let snapshot =
            scan_path_with_hints_counted(&self.root, &hints, Some(&self.hash_bytes_read))?;
        let root = snapshot
            .get("")
            .ok_or_else(|| Error::corrupt("local scan omitted the synchronized root"))?;
        if root.identity != self.root_identity {
            return Err(Error::invalid(
                "Direct Sync root identity changed since the writer opened it",
            ));
        }
        let updates = snapshot
            .nodes()
            .values()
            .filter(|node| {
                !hints
                    .get(&node.path)
                    .is_some_and(|hint| fingerprint_matches_node(hint, node))
            })
            .collect::<Vec<_>>();
        self.store.save_scan_evidence(&updates)?;
        Ok(snapshot)
    }

    pub fn scan_read_only(
        root: &Path,
        hints: &BTreeMap<String, LocalFingerprint>,
    ) -> Result<LocalSnapshot> {
        scan_path_with_hints_counted(root, hints, None)
    }

    pub fn hash_bytes_read(&self) -> u64 {
        self.hash_bytes_read.load(Ordering::Relaxed)
    }

    pub fn scan_hints(&self) -> Result<BTreeMap<String, LocalFingerprint>> {
        Ok(self
            .store
            .baseline()?
            .map(|baseline| baseline.local)
            .unwrap_or_default())
    }

    pub fn artifact_path(&self, artifact: &ArtifactRecord) -> PathBuf {
        self.state.join("artifacts").join(&artifact.name)
    }

    pub fn install_path(&self, temporary: &str) -> PathBuf {
        self.state.join("install").join(temporary)
    }

    pub fn install_desired_file(&self, temporary: &str) -> Result<LocalFingerprint> {
        let path = self.install_path(temporary);
        let stat = statat(&self.install_fd, temporary, AtFlags::SYMLINK_NOFOLLOW)
            .map_err(|error| Error::errno("inspect sync download", &path, error))?;
        let (length, digest, identity) = hash_at(&self.install_fd, temporary, &path)?;
        let file_stat = fstat(
            &openat(
                &self.install_fd,
                temporary,
                OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                Mode::empty(),
            )
            .map_err(|error| Error::errno("open sync download", &path, error))?,
        )
        .map_err(|error| Error::errno("inspect sync download", &path, error))?;
        let executable =
            Mode::from_raw_mode(file_stat.st_mode).intersects(Mode::XUSR | Mode::XGRP | Mode::XOTH);
        LocalFingerprint::from_node(&LocalNode {
            path: String::new(),
            kind: LocalKind::File { length, digest },
            executable,
            identity,
            parent: Some(self.state_identity),
            stat: Some(super::types::LocalStat::from_stat(&stat)),
        })
    }

    pub fn create_install_file(&self, prefix: &str) -> Result<(String, PathBuf, std::fs::File)> {
        let name = format!("{prefix}-{}", uuid::Uuid::new_v4().simple());
        let fd = openat(
            &self.install_fd,
            name.as_str(),
            OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::RUSR | Mode::WUSR,
        )
        .map_err(|error| {
            Error::errno("create Direct Sync install temporary", &self.state, error)
        })?;
        fsync(&self.install_fd).map_err(|error| {
            Error::errno("persist Direct Sync install directory", &self.state, error)
        })?;
        Ok((
            name.clone(),
            self.state.join("install").join(name),
            std::fs::File::from(fd),
        ))
    }

    pub fn retain_install_temporary(
        &self,
        temporary: &str,
        origin: &str,
        kind: ArtifactKind,
    ) -> Result<ArtifactRecord> {
        let (length, digest, _) =
            hash_at(&self.install_fd, temporary, &self.install_path(temporary))?;
        let id = uuid::Uuid::new_v4().to_string();
        let name = format!("artifact-{id}");
        renameat_with(
            &self.install_fd,
            temporary,
            &self.artifact_fd,
            name.as_str(),
            RenameFlags::NOREPLACE,
        )
        .map_err(|error| {
            Error::errno(
                "retain Direct Sync install temporary",
                &self.install_path(temporary),
                error,
            )
        })?;
        fsync(&self.install_fd).map_err(|error| {
            Error::errno("persist Direct Sync install directory", &self.state, error)
        })?;
        fsync(&self.artifact_fd).map_err(|error| {
            Error::errno("persist Direct Sync artifact directory", &self.state, error)
        })?;
        let stat = optional_stat(
            &self.artifact_fd,
            &name,
            &self.state.join("artifacts").join(&name),
        )?;
        Ok(ArtifactRecord {
            id,
            origin: origin.to_owned(),
            release_event: kind.release_event(),
            kind,
            digest: Some(*digest.as_bytes()),
            length,
            phase: ArtifactPhase::Retained,
            name,
            remote_revision: None,
            stat: stat.map(|stat| super::store::ArtifactStat {
                dev: stat.st_dev,
                ino: stat.st_ino,
                mtime: stat.st_mtime,
                mtime_nsec: stat.st_mtime_nsec as i64,
                ctime: stat.st_ctime,
                ctime_nsec: stat.st_ctime_nsec as i64,
            }),
            settle_pending: false,
            error: None,
        })
    }

    pub fn capture_local(&self, node: &LocalNode, kind: ArtifactKind) -> Result<ArtifactRecord> {
        validate_path(&node.path)?;
        let LocalFingerprint {
            length: expected_length,
            digest: expected_digest,
            dev,
            ino,
            parent_dev,
            parent_ino,
            ..
        } = LocalFingerprint::from_node(node)?;
        let (parent, name) = split_path(&node.path)?;
        let parent_fd = self.open_relative_dir(parent)?;
        let parent_identity = identity(
            &parent_fd,
            &self.root.join(parent),
            "inspect Direct Sync upload parent",
        )?;
        if parent_identity.dev != parent_dev || parent_identity.ino != parent_ino {
            return Err(Error::conflict(
                "local upload parent changed after the scan",
            ));
        }
        let source = openat(
            &parent_fd,
            name,
            OFlags::RDONLY | OFlags::NONBLOCK | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .map_err(|error| {
            Error::errno(
                "open Direct Sync upload",
                &self.root.join(&node.path),
                error,
            )
        })?;
        let before = fstat(&source).map_err(|error| {
            Error::errno(
                "inspect Direct Sync upload",
                &self.root.join(&node.path),
                error,
            )
        })?;
        if FileType::from_raw_mode(before.st_mode) != FileType::RegularFile
            || before.st_dev != dev
            || before.st_ino != ino
        {
            return Err(Error::conflict(
                "local upload identity changed after the scan",
            ));
        }
        let id = uuid::Uuid::new_v4().to_string();
        let artifact_name = format!("artifact-{id}");
        let output = openat(
            &self.artifact_fd,
            artifact_name.as_str(),
            OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::RUSR | Mode::WUSR,
        )
        .map_err(|error| Error::errno("create Direct Sync upload artifact", &self.state, error))?;
        let mut source = std::fs::File::from(source);
        let mut output = std::fs::File::from(output);
        let mut hasher = Hasher::default();
        let mut buffer = vec![0_u8; 64 * 1024];
        loop {
            let count = source.read(&mut buffer).map_err(|error| {
                Error::io(
                    "read Direct Sync upload",
                    &self.root.join(&node.path),
                    error,
                )
            })?;
            if count == 0 {
                break;
            }
            output.write_all(&buffer[..count]).map_err(|error| {
                Error::io("write Direct Sync upload artifact", &self.state, error)
            })?;
            hasher.update(&buffer[..count]);
        }
        let after = fstat(&source).map_err(|error| {
            Error::errno(
                "recheck Direct Sync upload",
                &self.root.join(&node.path),
                error,
            )
        })?;
        if !same_stable_stat(&before, &after) {
            return Err(Error::conflict(
                "local upload changed while its bytes were captured",
            ));
        }
        output.sync_all().map_err(|error| {
            Error::io("persist Direct Sync upload artifact", &self.state, error)
        })?;
        let (length, digest) = hasher.finish();
        let retained = fstat(&output)
            .map_err(|error| Error::errno("inspect sync upload artifact", &self.state, error))?;
        drop(output);
        fsync(&self.artifact_fd).map_err(|error| {
            Error::errno("persist Direct Sync artifact directory", &self.state, error)
        })?;
        if length != expected_length || digest.as_bytes() != &expected_digest {
            return Err(Error::conflict(
                "local upload bytes do not match the accepted scan",
            ));
        }
        Ok(ArtifactRecord {
            id,
            origin: node.path.clone(),
            release_event: kind.release_event(),
            kind,
            digest: Some(*digest.as_bytes()),
            length,
            phase: ArtifactPhase::Retained,
            name: artifact_name,
            remote_revision: None,
            stat: Some(super::store::ArtifactStat {
                dev: retained.st_dev,
                ino: retained.st_ino,
                mtime: retained.st_mtime,
                mtime_nsec: retained.st_mtime_nsec as i64,
                ctime: retained.st_ctime,
                ctime_nsec: retained.st_ctime_nsec as i64,
            }),
            settle_pending: false,
            error: None,
        })
    }

    pub fn retain_bytes(&self, origin: &str, bytes: &[u8]) -> Result<ArtifactRecord> {
        let id = uuid::Uuid::new_v4().to_string();
        let name = format!("resolution-{id}");
        let fd = openat(
            &self.artifact_fd,
            name.as_str(),
            OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::RUSR | Mode::WUSR,
        )
        .map_err(|error| Error::errno("create resolution artifact", &self.state, error))?;
        let mut file = std::fs::File::from(fd);
        file.write_all(bytes)
            .map_err(|error| Error::io("write resolution artifact", &self.state, error))?;
        file.sync_all()
            .map_err(|error| Error::io("persist resolution artifact", &self.state, error))?;
        let stat = fstat(&file)
            .map_err(|error| Error::errno("inspect resolution artifact", &self.state, error))?;
        fsync(&self.artifact_fd)
            .map_err(|error| Error::errno("persist resolution directory", &self.state, error))?;
        let artifact = ArtifactRecord {
            id,
            origin: origin.to_owned(),
            name,
            kind: ArtifactKind::Resolution,
            phase: ArtifactPhase::Retained,
            release_event: ArtifactKind::Resolution.release_event(),
            remote_revision: None,
            digest: Some(*blake3::hash(bytes).as_bytes()),
            length: bytes.len() as u64,
            stat: Some(super::store::ArtifactStat {
                dev: stat.st_dev,
                ino: stat.st_ino,
                mtime: stat.st_mtime,
                mtime_nsec: stat.st_mtime_nsec as i64,
                ctime: stat.st_ctime,
                ctime_nsec: stat.st_ctime_nsec as i64,
            }),
            settle_pending: false,
            error: None,
        };
        self.store.insert_artifact(&artifact)?;
        Ok(artifact)
    }

    pub fn open_artifact(&self, name: &str) -> Result<std::fs::File> {
        let fd = openat(
            &self.artifact_fd,
            name,
            OFlags::RDONLY | OFlags::NONBLOCK | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .map_err(|error| Error::errno("open Direct Sync artifact", &self.state, error))?;
        Ok(std::fs::File::from(fd))
    }

    pub fn verify_artifact(&self, artifact: &ArtifactRecord) -> Result<()> {
        if self.artifact_matches_record(artifact)? {
            return Ok(());
        }
        let (length, digest, _) = hash_at(
            &self.artifact_fd,
            &artifact.name,
            &self.artifact_path(artifact),
        )?;
        if length != artifact.length || artifact.digest != Some(*digest.as_bytes()) {
            return Err(Error::corrupt(format!(
                "retained sync artifact {} no longer matches its recorded bytes",
                artifact.id
            )));
        }
        Ok(())
    }

    pub fn release_artifact(&self, artifact: &ArtifactRecord) -> Result<bool> {
        if self.artifact_referenced(artifact)? {
            return Ok(false);
        }
        let Some(stat) = optional_stat(
            &self.artifact_fd,
            &artifact.name,
            &self.artifact_path(artifact),
        )?
        else {
            return Ok(true);
        };
        match FileType::from_raw_mode(stat.st_mode) {
            FileType::RegularFile => {
                unlinkat(&self.artifact_fd, artifact.name.as_str(), AtFlags::empty()).map_err(
                    |error| {
                        Error::errno(
                            "release sync artifact",
                            &self.artifact_path(artifact),
                            error,
                        )
                    },
                )?;
            }
            FileType::Directory => {
                remove_artifact_directory(
                    &self.artifact_fd,
                    &artifact.name,
                    &self.artifact_path(artifact),
                )?;
                unlinkat(
                    &self.artifact_fd,
                    artifact.name.as_str(),
                    AtFlags::REMOVEDIR,
                )
                .map_err(|error| {
                    Error::errno(
                        "release sync directory artifact",
                        &self.artifact_path(artifact),
                        error,
                    )
                })?;
            }
            _ => return Ok(false),
        }
        fsync(&self.artifact_fd).map_err(|error| {
            Error::errno("persist Direct Sync artifact release", &self.state, error)
        })?;
        Ok(true)
    }

    fn artifact_referenced(&self, artifact: &ArtifactRecord) -> Result<bool> {
        if self.store.conflicts()?.iter().any(|conflict| {
            conflict.local_artifact.as_deref() == Some(artifact.id.as_str())
                || conflict.remote_artifact.as_deref() == Some(artifact.id.as_str())
        }) {
            return Ok(true);
        }
        if self
            .store
            .work()?
            .iter()
            .any(|work| work.source_artifact.as_deref() == Some(artifact.id.as_str()))
        {
            return Ok(true);
        }
        if self
            .store
            .installs()?
            .iter()
            .any(|install| install.artifact.as_deref() == Some(artifact.id.as_str()))
        {
            return Ok(true);
        }
        Ok(false)
    }

    pub fn settle_fresh_artifacts(&mut self) -> Result<()> {
        let pending = self
            .store
            .artifacts()?
            .into_iter()
            .filter(|artifact| artifact.settle_pending)
            .collect::<Vec<_>>();
        for mut artifact in pending {
            if artifact.phase == ArtifactPhase::Conflict {
                continue;
            }
            if self.artifact_matches_record(&artifact)? {
                if self.release_artifact(&artifact)? {
                    self.store.remove_artifact(&artifact.id)?;
                }
            } else {
                artifact.phase = ArtifactPhase::Conflict;
                artifact.settle_pending = false;
                artifact.error =
                    Some("retained artifact changed after its reconciliation".to_owned());
                self.store.update_artifact(&artifact)?;
            }
        }
        self.prune_artifacts()
    }

    pub fn prune_artifacts(&mut self) -> Result<()> {
        let artifacts = self.store.artifacts()?;
        let mut count = artifacts
            .iter()
            .filter(|artifact| artifact.phase != ArtifactPhase::Conflict)
            .count();
        let mut bytes = artifacts
            .iter()
            .filter(|artifact| artifact.phase != ArtifactPhase::Conflict)
            .map(|artifact| artifact.length)
            .sum::<u64>();
        for artifact in artifacts {
            if count <= ARTIFACT_BUDGET_COUNT && bytes <= ARTIFACT_BUDGET_BYTES {
                break;
            }
            if artifact.phase == ArtifactPhase::Conflict || self.artifact_referenced(&artifact)? {
                continue;
            }
            if self.release_artifact(&artifact)? {
                self.store.remove_artifact(&artifact.id)?;
                count = count.saturating_sub(1);
                bytes = bytes.saturating_sub(artifact.length);
            }
        }
        Ok(())
    }

    pub fn open_live_file(&self, path: &str) -> Result<std::fs::File> {
        validate_path(path)?;
        let (parent, name) = split_path(path)?;
        let parent_fd = self.open_relative_dir(parent)?;
        let fd = openat(
            &parent_fd,
            name,
            OFlags::RDONLY | OFlags::NONBLOCK | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .map_err(|error| {
            Error::errno(
                "open Direct Sync local conflict",
                &self.root.join(path),
                error,
            )
        })?;
        Ok(std::fs::File::from(fd))
    }

    pub fn open_local_file(&self, path: &str) -> Result<std::fs::File> {
        self.open_live_file(path)
    }

    pub fn create_directory_local(&self, path: &str) -> Result<()> {
        validate_path(path)?;
        if path.is_empty() {
            return Err(Error::invalid("the synchronized root already exists"));
        }
        let (parent, name) = split_path(path)?;
        let parent_fd = self.open_relative_dir(parent)?;
        match mkdirat(&parent_fd, name, Mode::RWXU) {
            Ok(()) => {}
            Err(rustix::io::Errno::EXIST) => {
                return Err(Error::conflict(format!(
                    "local directory already exists: {path}"
                )));
            }
            Err(error) => {
                return Err(Error::errno(
                    "create local directory",
                    &self.root.join(path),
                    error,
                ));
            }
        }
        fsync(&parent_fd).map_err(|error| {
            Error::errno("persist local directory", &self.root.join(parent), error)
        })
    }

    pub fn rename_local(&self, source: &str, destination: &str) -> Result<()> {
        validate_path(source)?;
        validate_path(destination)?;
        let (source_parent, source_name) = split_path(source)?;
        let (destination_parent, destination_name) = split_path(destination)?;
        let source_fd = self.open_relative_dir(source_parent)?;
        let destination_fd = self.open_relative_dir(destination_parent)?;
        renameat_with(
            &source_fd,
            source_name,
            &destination_fd,
            destination_name,
            RenameFlags::NOREPLACE,
        )
        .map_err(|error| {
            Error::errno(
                "rename local entry without replacement",
                &self.root.join(source),
                error,
            )
        })?;
        fsync(&source_fd).map_err(|error| {
            Error::errno(
                "persist local rename",
                &self.root.join(source_parent),
                error,
            )
        })?;
        fsync(&destination_fd).map_err(|error| {
            Error::errno(
                "persist local rename destination",
                &self.root.join(destination_parent),
                error,
            )
        })
    }

    pub fn quarantine(
        &self,
        path: &str,
        expected: &LocalSnapshot,
        reason: &str,
        remote_revision: Option<[u8; 24]>,
    ) -> Result<ArtifactRecord> {
        if path.is_empty() {
            return Err(Error::invalid(
                "the synchronized root cannot be quarantined",
            ));
        }
        validate_path(path)?;
        let expected_node = expected
            .get(path)
            .ok_or_else(|| Error::invalid("quarantine target is absent from the observation"))?;
        let (parent, name) = split_path(path)?;
        let parent_fd = self.open_relative_dir(parent)?;
        let parent_identity = identity(
            &parent_fd,
            &self.root.join(parent),
            "inspect quarantine parent",
        )?;
        if let Some(expected_parent) = expected_node.parent
            && parent_identity != expected_parent
        {
            return Err(Error::conflict("quarantine parent identity changed"));
        }
        let current = optional_stat(&parent_fd, name, &self.root.join(path))?
            .ok_or_else(|| Error::conflict("quarantine target disappeared"))?;
        let kind = match (FileType::from_raw_mode(current.st_mode), expected_node.kind) {
            (FileType::RegularFile, LocalKind::File { .. }) => ArtifactKind::QuarantinedFile,
            (FileType::Directory, LocalKind::Directory) => ArtifactKind::QuarantinedDirectory,
            (FileType::Symlink, _) => {
                return Err(Error::conflict(
                    "quarantine target became a symlink; it was not followed",
                ));
            }
            _ => return Err(Error::conflict("quarantine target changed kind")),
        };
        if current.st_dev != expected_node.identity.dev
            || current.st_ino != expected_node.identity.ino
        {
            return Err(Error::conflict(
                "quarantine target identity changed after observation",
            ));
        }
        let id = uuid::Uuid::new_v4().to_string();
        let name = format!("quarantine-{id}");
        let mut artifact = ArtifactRecord {
            id,
            origin: path.to_owned(),
            name: name.clone(),
            kind,
            phase: ArtifactPhase::Prepared,
            release_event: kind.release_event(),
            remote_revision,
            digest: match expected_node.kind {
                LocalKind::File { digest, .. } => Some(*digest.as_bytes()),
                LocalKind::Directory => None,
            },
            length: expected
                .subtree(path)
                .values()
                .map(|node| match node.kind {
                    LocalKind::File { length, .. } => length,
                    LocalKind::Directory => 0,
                })
                .sum(),
            stat: None,
            settle_pending: false,
            error: Some(reason.to_owned()),
        };
        self.store.insert_artifact(&artifact)?;
        if let Err(error) = renameat_with(
            &parent_fd,
            path.rsplit_once('/').map_or(path, |(_, name)| name),
            &self.artifact_fd,
            artifact.name.as_str(),
            RenameFlags::NOREPLACE,
        ) {
            self.store.remove_artifact(&artifact.id)?;
            return Err(Error::errno(
                "quarantine local path",
                &self.root.join(path),
                error,
            ));
        }
        fsync(&parent_fd).map_err(|error| {
            Error::errno("persist quarantine source directory", &self.root, error)
        })?;
        fsync(&self.artifact_fd)
            .map_err(|error| Error::errno("persist quarantine directory", &self.state, error))?;
        artifact.phase = ArtifactPhase::Retained;
        if let Some(stat) = optional_stat(
            &self.artifact_fd,
            &artifact.name,
            &self.artifact_path(&artifact),
        )? {
            artifact.stat = Some(super::store::ArtifactStat {
                dev: stat.st_dev,
                ino: stat.st_ino,
                mtime: stat.st_mtime,
                mtime_nsec: stat.st_mtime_nsec as i64,
                ctime: stat.st_ctime,
                ctime_nsec: stat.st_ctime_nsec as i64,
            });
        }
        self.store.update_artifact(&artifact)?;
        Ok(artifact)
    }

    pub fn artifact_matches_record(&self, artifact: &ArtifactRecord) -> Result<bool> {
        let Some(stat) = optional_stat(
            &self.artifact_fd,
            &artifact.name,
            &self.artifact_path(artifact),
        )?
        else {
            return Ok(false);
        };
        if let Some(recorded) = artifact.stat
            && recorded.dev == stat.st_dev
            && recorded.ino == stat.st_ino
            && recorded.mtime == stat.st_mtime
            && recorded.mtime_nsec == stat.st_mtime_nsec as i64
            && recorded.ctime == stat.st_ctime
            && recorded.ctime_nsec == stat.st_ctime_nsec as i64
            && (FileType::from_raw_mode(stat.st_mode) != FileType::RegularFile
                || artifact.length == stat.st_size as u64)
        {
            return Ok(true);
        }
        if FileType::from_raw_mode(stat.st_mode) != FileType::RegularFile {
            return Ok(false);
        }
        let Some(expected) = artifact.digest else {
            return Ok(false);
        };
        let (length, digest, _) = hash_at(
            &self.artifact_fd,
            &artifact.name,
            &self.artifact_path(artifact),
        )?;
        Ok(length == artifact.length && digest.as_bytes() == &expected)
    }

    pub fn recover_installs(&mut self) -> Result<()> {
        for mut install in self.store.installs()? {
            match install.state {
                InstallState::Prepared => {
                    if install.operation != InstallOperation::Delete
                        && install.desired_local.is_none()
                    {
                        if optional_stat(
                            &self.install_fd,
                            &install.temporary,
                            &self.install_path(&install.temporary),
                        )?
                        .is_some()
                        {
                            unlinkat(
                                &self.install_fd,
                                install.temporary.as_str(),
                                AtFlags::empty(),
                            )
                            .map_err(|error| {
                                Error::errno(
                                    "remove incomplete Direct Sync download",
                                    &self.install_path(&install.temporary),
                                    error,
                                )
                            })?;
                            fsync(&self.install_fd).map_err(|error| {
                                Error::errno(
                                    "persist Direct Sync download cleanup",
                                    &self.state,
                                    error,
                                )
                            })?;
                        }
                        self.store.remove_install(&install.id)?;
                        continue;
                    }
                    if install.operation != InstallOperation::Delete {
                        let desired = install.desired_local.ok_or_else(|| {
                            Error::corrupt("prepared file install has no desired content")
                        })?;
                        let current = match self.install_desired_file(&install.temporary) {
                            Ok(current) => current,
                            Err(error) => {
                                install.state = InstallState::Failed;
                                install.error = Some(format!(
                                    "prepared Direct Sync download is unavailable: {error}"
                                ));
                                self.store.update_install(&install)?;
                                continue;
                            }
                        };
                        if current.length != desired.length || current.digest != desired.digest {
                            install.state = InstallState::Failed;
                            install.error = Some(
                                "prepared Direct Sync download no longer matches its accepted bytes"
                                    .to_owned(),
                            );
                            self.store.update_install(&install)?;
                            continue;
                        }
                    }
                    if let Err(error) = self.apply_install(&mut install)
                        && error.kind() != ErrorKind::Conflict
                    {
                        return Err(error);
                    }
                }
                InstallState::Dispatched => {
                    if let Some(fingerprint) = self.infer_dispatched(&install)? {
                        if install.operation == InstallOperation::Delete
                            && let Some(stat) = optional_stat(
                                &self.install_fd,
                                &install.temporary,
                                &self.install_path(&install.temporary),
                            )?
                        {
                            let expected = install.expected_local.ok_or_else(|| {
                                Error::corrupt("local delete recovery has no expected file")
                            })?;
                            if FileType::from_raw_mode(stat.st_mode) != FileType::RegularFile
                                || stat.st_dev != expected.dev
                                || stat.st_ino != expected.ino
                            {
                                install.state = InstallState::Conflict;
                                install.error = Some(
                                    "a different local inode was quarantined during delete recovery"
                                        .to_owned(),
                                );
                                self.store.update_install(&install)?;
                                continue;
                            }
                            unlinkat(
                                &self.install_fd,
                                install.temporary.as_str(),
                                AtFlags::empty(),
                            )
                            .map_err(|error| {
                                Error::errno(
                                    "remove quarantined Direct Sync delete",
                                    &self.install_path(&install.temporary),
                                    error,
                                )
                            })?;
                            fsync(&self.install_fd).map_err(|error| {
                                Error::errno(
                                    "persist Direct Sync delete quarantine",
                                    &self.state,
                                    error,
                                )
                            })?;
                            self.prune_empty_ancestors(&install.path)?;
                        }
                        self.settle_displaced_artifact(&install)?;
                        let baseline = build_baseline(&install, fingerprint)?;
                        let baseline_ref = baseline
                            .as_ref()
                            .map(|(remote, local, keep_local)| (remote, *local, *keep_local));
                        self.store
                            .complete_install(&install.id, baseline_ref, &install.path)?;
                        self.mark_settled_artifact(&install)?;
                    } else if self.local_matches_expected(&install)? {
                        install.state = InstallState::Prepared;
                        self.store.update_install(&install)?;
                    } else {
                        install.state = InstallState::Conflict;
                        install.error = Some(
                            "local install outcome is not safely inferable after restart"
                                .to_owned(),
                        );
                        self.store.update_install(&install)?;
                    }
                }
                InstallState::Applied => {
                    let fingerprint = self.local_fingerprint(&install.path)?;
                    if desired_matches(install.desired_local, Some(fingerprint)) {
                        self.settle_displaced_artifact(&install)?;
                        let baseline = build_baseline(&install, fingerprint)?;
                        let baseline_ref = baseline
                            .as_ref()
                            .map(|(remote, local, keep_local)| (remote, *local, *keep_local));
                        self.store
                            .complete_install(&install.id, baseline_ref, &install.path)?;
                        self.mark_settled_artifact(&install)?;
                    } else {
                        install.state = InstallState::Conflict;
                        install.error = Some(
                            "applied local install no longer matches its accepted bytes".to_owned(),
                        );
                        self.store.update_install(&install)?;
                    }
                }
                InstallState::Conflict | InstallState::Failed => {}
            }
        }
        Ok(())
    }

    pub fn apply_install(&mut self, install: &mut InstallRecord) -> Result<()> {
        match install.operation {
            InstallOperation::Create | InstallOperation::Replace => self.install_file(install),
            InstallOperation::Delete => self.delete_file(install),
        }
    }

    pub fn local_fingerprint(&self, path: &str) -> Result<LocalFingerprint> {
        validate_path(path)?;
        let (parent, name) = split_path(path)?;
        let parent_fd = self.open_relative_dir(parent)?;
        let parent_identity = identity(
            &parent_fd,
            &self.root.join(parent),
            "inspect local file parent",
        )?;
        let target = self.root.join(path);
        let stat = statat(&parent_fd, name, AtFlags::SYMLINK_NOFOLLOW)
            .map_err(|error| Error::errno("inspect sync local file", &target, error))?;
        let (length, digest, identity) = hash_at(&parent_fd, name, &target)?;
        let file = openat(
            &parent_fd,
            name,
            OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .map_err(|error| Error::errno("open sync local file", &target, error))?;
        let opened = fstat(&file)
            .map_err(|error| Error::errno("inspect sync local file", &target, error))?;
        let executable =
            Mode::from_raw_mode(opened.st_mode).intersects(Mode::XUSR | Mode::XGRP | Mode::XOTH);
        LocalFingerprint::from_node(&LocalNode {
            path: path.to_owned(),
            kind: LocalKind::File { length, digest },
            executable,
            identity,
            parent: Some(parent_identity),
            stat: Some(super::types::LocalStat::from_stat(&stat)),
        })
    }

    pub fn observe_path(&self, path: &str) -> Result<Option<LocalNode>> {
        validate_path(path)?;
        if path.is_empty() {
            return Ok(Some(LocalNode {
                path: String::new(),
                kind: LocalKind::Directory,
                executable: false,
                identity: self.root_identity,
                parent: None,
                stat: None,
            }));
        }
        let (parent, name) = split_path(path)?;
        let parent_fd = self.open_relative_dir(parent)?;
        let parent_identity = identity(
            &parent_fd,
            &self.root.join(parent),
            "inspect Direct Sync local parent",
        )?;
        let Some(stat) = optional_stat(&parent_fd, name, &self.root.join(path))? else {
            return Ok(None);
        };
        match FileType::from_raw_mode(stat.st_mode) {
            FileType::Directory => {
                let fd = openat(
                    &parent_fd,
                    name,
                    OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                    Mode::empty(),
                )
                .map_err(|error| {
                    Error::errno(
                        "open Direct Sync local directory",
                        &self.root.join(path),
                        error,
                    )
                })?;
                Ok(Some(LocalNode {
                    path: path.to_owned(),
                    kind: LocalKind::Directory,
                    executable: false,
                    identity: identity(
                        &fd,
                        &self.root.join(path),
                        "inspect Direct Sync local directory",
                    )?,
                    parent: Some(parent_identity),
                    stat: Some(super::types::LocalStat::from_stat(&stat)),
                }))
            }
            FileType::RegularFile => {
                let fingerprint = self.local_fingerprint(path)?;
                Ok(Some(LocalNode {
                    path: path.to_owned(),
                    kind: LocalKind::File {
                        length: fingerprint.length,
                        digest: ContentHash::from_bytes(fingerprint.digest),
                    },
                    executable: Mode::from_raw_mode(stat.st_mode)
                        .intersects(Mode::XUSR | Mode::XGRP | Mode::XOTH),
                    identity: fingerprint.identity(),
                    parent: Some(Identity {
                        dev: fingerprint.parent_dev,
                        ino: fingerprint.parent_ino,
                    }),
                    stat: Some(super::types::LocalStat::from_stat(&stat)),
                }))
            }
            FileType::Symlink => Err(Error::unsupported(format!(
                "Direct Sync local symlink is not supported: {path}"
            ))),
            _ => Err(Error::unsupported(format!(
                "Direct Sync local special file is not supported: {path}"
            ))),
        }
    }

    fn install_file(&mut self, install: &mut InstallRecord) -> Result<()> {
        let desired = install
            .desired_local
            .ok_or_else(|| Error::corrupt("file install has no desired content"))?;
        let (temporary_length, temporary_digest, _) = hash_at(
            &self.install_fd,
            &install.temporary,
            &self.install_path(&install.temporary),
        )?;
        if temporary_length != desired.length || temporary_digest.as_bytes() != &desired.digest {
            return Err(Error::corrupt(
                "downloaded file temporary does not match its accepted content",
            ));
        }

        let (parent, name) = split_path(&install.path)?;
        self.ensure_directory(parent)?;
        let temporary_path = self.install_path(&install.temporary);
        let temporary = openat(
            &self.install_fd,
            install.temporary.as_str(),
            OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .map_err(|error| Error::errno("open sync download", &temporary_path, error))?;
        let mode = if desired.executable {
            Mode::RUSR | Mode::WUSR | Mode::XUSR
        } else {
            Mode::RUSR | Mode::WUSR
        };
        rustix::fs::fchmod(&temporary, mode)
            .map_err(|error| Error::errno("set sync download mode", &temporary_path, error))?;
        let parent_fd = self.open_relative_dir(parent)?;
        let parent_identity = identity(
            &parent_fd,
            &self.root.join(parent),
            "inspect Direct Sync install parent",
        )?;
        let current = optional_stat(&parent_fd, name, &self.root.join(&install.path))?;
        match (install.operation, install.expected_local, current) {
            (InstallOperation::Create, None, None) => {}
            (InstallOperation::Replace, Some(expected), Some(stat)) => {
                if parent_identity.dev != expected.parent_dev
                    || parent_identity.ino != expected.parent_ino
                    || stat.st_dev != expected.dev
                    || stat.st_ino != expected.ino
                {
                    return self.install_conflict(install, "local target changed before install");
                }
                let (length, digest, identity) =
                    hash_at(&parent_fd, name, &self.root.join(&install.path))?;
                if identity.dev != expected.dev
                    || identity.ino != expected.ino
                    || length != expected.length
                    || digest.as_bytes() != &expected.digest
                {
                    return self
                        .install_conflict(install, "local target bytes changed before install");
                }
            }
            (InstallOperation::Create, None, Some(_)) => {
                return self.install_conflict(install, "local create target already exists");
            }
            (InstallOperation::Replace, Some(_), None) => {
                return self.install_conflict(install, "local replacement target disappeared");
            }
            _ => {
                return Err(Error::corrupt(
                    "file install operation and expected local state disagree",
                ));
            }
        }

        if install.operation == InstallOperation::Replace && install.artifact.is_none() {
            let expected = install
                .expected_local
                .ok_or_else(|| Error::corrupt("replacement has no expected local file"))?;
            let artifact = ArtifactRecord {
                id: uuid::Uuid::new_v4().to_string(),
                origin: install.path.clone(),
                kind: ArtifactKind::ReplacedFile,
                release_event: ArtifactRelease::NextReconciliation,
                digest: Some(expected.digest),
                length: expected.length,
                phase: ArtifactPhase::Preparing,
                name: format!("artifact-{}", uuid::Uuid::new_v4()),
                remote_revision: install
                    .desired_remote
                    .as_ref()
                    .and_then(|remote| remote.pin.revision()),
                stat: None,
                settle_pending: false,
                error: None,
            };
            self.store.insert_artifact(&artifact)?;
            install.artifact = Some(artifact.id);
        }

        install.state = InstallState::Dispatched;
        self.store.update_install(install)?;
        let exchange = install.operation == InstallOperation::Replace;
        let flags = if exchange {
            RenameFlags::EXCHANGE
        } else {
            RenameFlags::NOREPLACE
        };
        if let Err(error) = renameat_with(
            &self.install_fd,
            install.temporary.as_str(),
            &parent_fd,
            name,
            flags,
        ) {
            if matches!(
                error,
                rustix::io::Errno::EXIST | rustix::io::Errno::NOTEMPTY
            ) {
                return self.install_conflict(install, "local target appeared before install");
            }
            return Err(Error::errno(
                "install Direct Sync remote file",
                &self.root.join(&install.path),
                error,
            ));
        }
        fsync(&parent_fd).map_err(|error| {
            Error::errno(
                "persist Direct Sync file install",
                &self.root.join(parent),
                error,
            )
        })?;
        fsync(&self.install_fd).map_err(|error| {
            Error::errno("persist Direct Sync install directory", &self.state, error)
        })?;

        if exchange {
            let expected = install
                .expected_local
                .ok_or_else(|| Error::corrupt("replacement lost its expected local file"))?;
            let displaced = hash_at(
                &self.install_fd,
                &install.temporary,
                &self.install_path(&install.temporary),
            );
            let displaced_matches = displaced.as_ref().is_ok_and(|(length, digest, identity)| {
                *length == expected.length
                    && digest.as_bytes() == &expected.digest
                    && *identity == expected.identity()
            });
            if !displaced_matches {
                if renameat_with(
                    &parent_fd,
                    name,
                    &self.install_fd,
                    install.temporary.as_str(),
                    RenameFlags::EXCHANGE,
                )
                .is_ok()
                {
                    fsync(&parent_fd).map_err(|error| {
                        Error::errno(
                            "persist Direct Sync rollback",
                            &self.root.join(&install.path),
                            error,
                        )
                    })?;
                    fsync(&self.install_fd).map_err(|error| {
                        Error::errno("persist Direct Sync rollback temporary", &self.state, error)
                    })?;
                }
                return self.install_conflict(
                    install,
                    "local target changed concurrently; replacement was rolled back",
                );
            }
            let artifact_id = install
                .artifact
                .as_deref()
                .ok_or_else(|| Error::corrupt("replacement artifact disappeared"))?;
            let artifact = self
                .store
                .artifacts()?
                .into_iter()
                .find(|artifact| artifact.id == artifact_id)
                .ok_or_else(|| Error::corrupt("replacement artifact record disappeared"))?;
            renameat_with(
                &self.install_fd,
                install.temporary.as_str(),
                &self.artifact_fd,
                artifact.name.as_str(),
                RenameFlags::NOREPLACE,
            )
            .map_err(|error| {
                Error::errno(
                    "retain displaced Direct Sync local file",
                    &self.install_path(&install.temporary),
                    error,
                )
            })?;
            fsync(&self.install_fd).map_err(|error| {
                Error::errno("persist Direct Sync install directory", &self.state, error)
            })?;
            fsync(&self.artifact_fd).map_err(|error| {
                Error::errno("persist Direct Sync artifact directory", &self.state, error)
            })?;
            let mut artifact = artifact;
            artifact.phase = ArtifactPhase::Retained;
            artifact.release_event = ArtifactRelease::NextReconciliation;
            artifact.settle_pending = true;
            if let Some(stat) = optional_stat(
                &self.artifact_fd,
                &artifact.name,
                &self.artifact_path(&artifact),
            )? {
                artifact.length = stat.st_size as u64;
                artifact.stat = Some(super::store::ArtifactStat {
                    dev: stat.st_dev,
                    ino: stat.st_ino,
                    mtime: stat.st_mtime,
                    mtime_nsec: stat.st_mtime_nsec as i64,
                    ctime: stat.st_ctime,
                    ctime_nsec: stat.st_ctime_nsec as i64,
                });
            }
            self.store.update_artifact(&artifact)?;
        }

        install.state = InstallState::Applied;
        self.store.update_install(install)?;
        let fingerprint = self.local_fingerprint(&install.path)?;
        if !desired_matches(install.desired_local, Some(fingerprint)) {
            return self.install_conflict(
                install,
                "installed local bytes changed before baseline acceptance",
            );
        }
        let baseline = build_baseline(install, fingerprint)?;
        let baseline_ref = baseline
            .as_ref()
            .map(|(remote, local, keep_local)| (remote, *local, *keep_local));
        self.store
            .complete_install(&install.id, baseline_ref, &install.path)?;
        self.mark_settled_artifact(install)?;
        Ok(())
    }

    fn delete_file(&mut self, install: &mut InstallRecord) -> Result<()> {
        let expected = install
            .expected_local
            .ok_or_else(|| Error::corrupt("local delete has no expected file"))?;
        let (parent, name) = split_path(&install.path)?;
        let parent_fd = self.open_relative_dir(parent)?;
        let parent_identity = identity(
            &parent_fd,
            &self.root.join(parent),
            "inspect Direct Sync delete parent",
        )?;
        if parent_identity.dev != expected.parent_dev || parent_identity.ino != expected.parent_ino
        {
            return self.install_conflict(install, "local delete parent changed");
        }
        if optional_stat(&parent_fd, name, &self.root.join(&install.path))?.is_none() {
            return self.install_conflict(install, "local delete target disappeared");
        }
        let (length, digest, identity) = hash_at(&parent_fd, name, &self.root.join(&install.path))?;
        if identity != expected.identity()
            || length != expected.length
            || digest.as_bytes() != &expected.digest
        {
            return self.install_conflict(install, "local delete target changed");
        }
        install.state = InstallState::Dispatched;
        self.store.update_install(install)?;
        if let Err(error) = renameat_with(
            &parent_fd,
            name,
            &self.install_fd,
            install.temporary.as_str(),
            RenameFlags::NOREPLACE,
        ) {
            if matches!(error, rustix::io::Errno::NOENT | rustix::io::Errno::EXIST) {
                return self
                    .install_conflict(install, "local delete target changed before quarantine");
            }
            return Err(Error::errno(
                "quarantine Direct Sync local file",
                &self.root.join(&install.path),
                error,
            ));
        }
        fsync(&parent_fd).map_err(|error| {
            Error::errno(
                "persist Direct Sync local removal",
                &self.root.join(parent),
                error,
            )
        })?;
        let quarantined = hash_at(
            &self.install_fd,
            &install.temporary,
            &self.install_path(&install.temporary),
        )?;
        if quarantined.2 != expected.identity()
            || quarantined.0 != expected.length
            || quarantined.1.as_bytes() != &expected.digest
        {
            let _ = renameat_with(
                &self.install_fd,
                install.temporary.as_str(),
                &parent_fd,
                name,
                RenameFlags::NOREPLACE,
            );
            return self.install_conflict(install, "local delete target changed during quarantine");
        }
        unlinkat(
            &self.install_fd,
            install.temporary.as_str(),
            AtFlags::empty(),
        )
        .map_err(|error| {
            Error::errno(
                "remove quarantined Direct Sync local file",
                &self.install_path(&install.temporary),
                error,
            )
        })?;
        fsync(&self.install_fd).map_err(|error| {
            Error::errno("persist Direct Sync delete quarantine", &self.state, error)
        })?;
        self.prune_empty_ancestors(&install.path)?;
        self.store
            .complete_install(&install.id, None, &install.path)?;
        Ok(())
    }

    fn install_conflict(&mut self, install: &mut InstallRecord, reason: &str) -> Result<()> {
        install.state = InstallState::Conflict;
        install.error = Some(reason.to_owned());
        self.store.update_install(install)?;
        Err(Error::conflict(reason))
    }

    fn settle_displaced_artifact(&mut self, install: &InstallRecord) -> Result<()> {
        if install.operation != InstallOperation::Replace {
            return Ok(());
        }
        let Some(id) = install.artifact.as_deref() else {
            return Ok(());
        };
        let artifact = self
            .store
            .artifacts()?
            .into_iter()
            .find(|artifact| artifact.id == id)
            .ok_or_else(|| Error::corrupt("displaced local artifact record disappeared"))?;
        let artifact_exists = optional_stat(
            &self.artifact_fd,
            &artifact.name,
            &self.state.join("artifacts"),
        )?
        .is_some();
        let temporary_exists = optional_stat(
            &self.install_fd,
            &install.temporary,
            &self.install_path(&install.temporary),
        )?
        .is_some();
        if temporary_exists && !artifact_exists {
            renameat_with(
                &self.install_fd,
                install.temporary.as_str(),
                &self.artifact_fd,
                artifact.name.as_str(),
                RenameFlags::NOREPLACE,
            )
            .map_err(|error| {
                Error::errno(
                    "recover displaced Direct Sync local file",
                    &self.install_path(&install.temporary),
                    error,
                )
            })?;
            fsync(&self.install_fd).map_err(|error| {
                Error::errno("persist Direct Sync install directory", &self.state, error)
            })?;
            fsync(&self.artifact_fd).map_err(|error| {
                Error::errno("persist Direct Sync artifact directory", &self.state, error)
            })?;
            let mut artifact = artifact;
            artifact.phase = ArtifactPhase::Retained;
            artifact.settle_pending = true;
            self.store.update_artifact(&artifact)?;
        }
        Ok(())
    }

    fn mark_settled_artifact(&mut self, install: &InstallRecord) -> Result<()> {
        if install.operation != InstallOperation::Replace {
            return Ok(());
        }
        let Some(id) = install.artifact.as_deref() else {
            return Ok(());
        };
        let Some(mut artifact) = self
            .store
            .artifacts()?
            .into_iter()
            .find(|artifact| artifact.id == id)
        else {
            return Ok(());
        };
        artifact.settle_pending = true;
        self.store.update_artifact(&artifact)?;
        Ok(())
    }

    fn infer_dispatched(&self, install: &InstallRecord) -> Result<Option<LocalFingerprint>> {
        match install.operation {
            InstallOperation::Create => {
                let current = self.optional_local_fingerprint(&install.path)?;
                let temp = self.install_temporary_exists(&install.temporary);
                match (current, temp) {
                    (Some(fingerprint), false)
                        if desired_matches(install.desired_local, Some(fingerprint)) =>
                    {
                        Ok(Some(fingerprint))
                    }
                    _ => Ok(None),
                }
            }
            InstallOperation::Replace => {
                let current = self.optional_local_fingerprint(&install.path)?;
                let temp = self.optional_install_fingerprint(&install.temporary)?;
                match (current, temp, install.expected_local) {
                    (Some(current), Some(temp), Some(expected))
                        if desired_matches(install.desired_local, Some(current))
                            && temp.length == expected.length
                            && temp.digest == expected.digest
                            && temp.dev == expected.dev
                            && temp.ino == expected.ino =>
                    {
                        Ok(Some(current))
                    }
                    _ => Ok(None),
                }
            }
            InstallOperation::Delete => {
                if self.optional_local_fingerprint(&install.path)?.is_some() {
                    return Ok(None);
                }
                if let Some(quarantined) = self.optional_install_fingerprint(&install.temporary)?
                    && let Some(expected) = install.expected_local
                    && (quarantined.dev != expected.dev
                        || quarantined.ino != expected.ino
                        || quarantined.length != expected.length
                        || quarantined.digest != expected.digest)
                {
                    return Ok(None);
                }
                Ok(Some(LocalFingerprint {
                    length: 0,
                    digest: [0; 32],
                    remote_digest: None,
                    executable: false,
                    dev: 0,
                    ino: 0,
                    parent_dev: 0,
                    parent_ino: 0,
                    stat: None,
                    recorded_ns: 0,
                }))
            }
        }
    }

    fn local_matches_expected(&self, install: &InstallRecord) -> Result<bool> {
        match (install.operation, install.expected_local) {
            (InstallOperation::Delete, _) | (InstallOperation::Create, None) => Ok(true),
            (_, Some(expected)) => {
                Ok(self.optional_local_fingerprint(&install.path)? == Some(expected))
            }
            _ => Ok(false),
        }
    }

    fn optional_local_fingerprint(&self, path: &str) -> Result<Option<LocalFingerprint>> {
        validate_path(path)?;
        let (parent, name) = split_path(path)?;
        let parent_fd = match self.open_relative_dir_optional(parent)? {
            Some(fd) => fd,
            None => return Ok(None),
        };
        let Some(_) = optional_stat(&parent_fd, name, &self.root.join(path))? else {
            return Ok(None);
        };
        self.local_fingerprint(path).map(Some)
    }

    fn optional_install_fingerprint(&self, temporary: &str) -> Result<Option<LocalFingerprint>> {
        let Some(stat) = optional_stat(&self.install_fd, temporary, &self.install_path(temporary))?
        else {
            return Ok(None);
        };
        if FileType::from_raw_mode(stat.st_mode) != FileType::RegularFile {
            return Ok(None);
        }
        let (length, digest, identity) =
            hash_at(&self.install_fd, temporary, &self.install_path(temporary))?;
        Ok(Some(LocalFingerprint {
            length,
            digest: *digest.as_bytes(),
            remote_digest: None,
            executable: Mode::from_raw_mode(stat.st_mode)
                .intersects(Mode::XUSR | Mode::XGRP | Mode::XOTH),
            dev: identity.dev,
            ino: identity.ino,
            parent_dev: 0,
            parent_ino: 0,
            stat: Some(super::store::ArtifactStat {
                dev: stat.st_dev,
                ino: stat.st_ino,
                mtime: stat.st_mtime,
                mtime_nsec: stat.st_mtime_nsec as i64,
                ctime: stat.st_ctime,
                ctime_nsec: stat.st_ctime_nsec as i64,
            }),
            recorded_ns: super::store::recorded_now()?,
        }))
    }

    fn install_temporary_exists(&self, temporary: &str) -> bool {
        optional_stat(&self.install_fd, temporary, &self.install_path(temporary))
            .ok()
            .flatten()
            .is_some()
    }

    fn ensure_directory(&self, path: &str) -> Result<()> {
        if path.is_empty() {
            return Ok(());
        }
        validate_path(path)?;
        let mut directory = self
            .root_fd
            .try_clone()
            .map_err(|error| Error::io("clone Direct Sync root descriptor", &self.root, error))?;
        let mut current = String::new();
        for component in path.split('/') {
            if !current.is_empty() {
                current.push('/');
            }
            current.push_str(component);
            match mkdirat(&directory, component, Mode::RWXU | Mode::RWXG | Mode::RWXO) {
                Ok(()) => {
                    fsync(&directory).map_err(|error| {
                        Error::errno(
                            "persist Direct Sync directory creation",
                            &self.root.join(&current),
                            error,
                        )
                    })?;
                }
                Err(rustix::io::Errno::EXIST) => {}
                Err(error) => {
                    return Err(Error::errno(
                        "create Direct Sync directory",
                        &self.root.join(&current),
                        error,
                    ));
                }
            }
            let next = openat(
                &directory,
                component,
                OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                Mode::empty(),
            )
            .map_err(|error| {
                Error::errno(
                    "open Direct Sync directory",
                    &self.root.join(&current),
                    error,
                )
            })?;
            directory = next;
        }
        Ok(())
    }

    fn open_relative_dir(&self, path: &str) -> Result<OwnedFd> {
        self.open_relative_dir_optional(path)?.ok_or_else(|| {
            Error::conflict(format!("Direct Sync parent directory {path:?} is missing"))
        })
    }

    fn open_relative_dir_optional(&self, path: &str) -> Result<Option<OwnedFd>> {
        if path.is_empty() {
            return self.root_fd.try_clone().map(Some).map_err(|error| {
                Error::io("clone Direct Sync root descriptor", &self.root, error)
            });
        }
        validate_path(path)?;
        let mut directory = self
            .root_fd
            .try_clone()
            .map_err(|error| Error::io("clone Direct Sync root descriptor", &self.root, error))?;
        let mut current = String::new();
        for component in path.split('/') {
            if !current.is_empty() {
                current.push('/');
            }
            current.push_str(component);
            match openat(
                &directory,
                component,
                OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                Mode::empty(),
            ) {
                Ok(next) => directory = next,
                Err(rustix::io::Errno::NOENT) => return Ok(None),
                Err(error) => {
                    return Err(Error::errno(
                        "open Direct Sync directory",
                        &self.root.join(&current),
                        error,
                    ));
                }
            }
        }
        Ok(Some(directory))
    }

    fn prune_empty_ancestors(&self, path: &str) -> Result<()> {
        let (parent, _) = split_path(path)?;
        if parent.is_empty() {
            return Ok(());
        }
        let components = parent.split('/').collect::<Vec<_>>();
        for depth in (1..=components.len()).rev() {
            let directory = components[..depth].join("/");
            let parent_path = components[..depth - 1].join("/");
            let Some(parent_fd) = self.open_relative_dir_optional(&parent_path)? else {
                break;
            };
            match unlinkat(&parent_fd, components[depth - 1], AtFlags::REMOVEDIR) {
                Ok(()) => {
                    fsync(&parent_fd).map_err(|error| {
                        Error::errno(
                            "persist Direct Sync directory cleanup",
                            &self.root.join(&parent_path),
                            error,
                        )
                    })?;
                }
                Err(
                    rustix::io::Errno::NOTEMPTY
                    | rustix::io::Errno::NOENT
                    | rustix::io::Errno::NOTDIR,
                ) => break,
                Err(error) => {
                    return Err(Error::errno(
                        "remove empty Direct Sync directory",
                        &self.root.join(&directory),
                        error,
                    ));
                }
            }
        }
        Ok(())
    }
}

fn build_baseline(
    install: &InstallRecord,
    local: LocalFingerprint,
) -> Result<Option<(RemoteEntry, LocalFingerprint, bool)>> {
    let remote = match install.operation {
        InstallOperation::Create | InstallOperation::Replace => install
            .desired_remote
            .clone()
            .ok_or_else(|| Error::corrupt("file install has no remote pin"))?,
        InstallOperation::Delete => return Ok(None),
    };
    Ok(Some((remote, local, true)))
}

fn desired_matches(
    desired: Option<LocalFingerprint>,
    fingerprint: Option<LocalFingerprint>,
) -> bool {
    match (desired, fingerprint) {
        (Some(desired), Some(fingerprint)) => {
            desired.length == fingerprint.length
                && desired.digest == fingerprint.digest
                && desired.executable == fingerprint.executable
        }
        _ => false,
    }
}

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
    let root_device = device_of(&root)?;
    let state_device = device_of(&state)?;
    if root_device != state_device {
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
            Err(error) => return Err(Error::io("inspect root/state device", candidate, error)),
        }
    }
}

pub fn namespace_digest(namespace: &str) -> [u8; 32] {
    *blake3::hash(namespace.as_bytes()).as_bytes()
}

fn prepare_directory(path: &Path, operation: &str) -> Result<()> {
    match std::fs::create_dir_all(path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(error) => return Err(Error::io(operation, path, error)),
    }
    let metadata =
        std::fs::symlink_metadata(path).map_err(|error| Error::io(operation, path, error))?;
    if !metadata.file_type().is_dir() || metadata.file_type().is_symlink() {
        return Err(Error::unsupported(format!(
            "{} must be an ordinary directory",
            path.display()
        )));
    }
    Ok(())
}

fn prepare_private_directory(path: &Path, operation: &str) -> Result<()> {
    use std::os::unix::fs::{DirBuilderExt, PermissionsExt};

    let mut builder = std::fs::DirBuilder::new();
    builder.recursive(true).mode(0o700);
    match builder.create(path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(error) => return Err(Error::io(operation, path, error)),
    }
    let metadata =
        std::fs::symlink_metadata(path).map_err(|error| Error::io(operation, path, error))?;
    if !metadata.file_type().is_dir() || metadata.file_type().is_symlink() {
        return Err(Error::unsupported(format!(
            "{} must be an ordinary directory",
            path.display()
        )));
    }
    if metadata.permissions().mode() & 0o077 != 0 {
        return Err(Error::invalid(format!(
            "Direct Sync state {} must not be accessible by group or other users",
            path.display()
        )));
    }
    Ok(())
}

pub(super) fn validate_private_state(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    let metadata = std::fs::symlink_metadata(path)
        .map_err(|error| Error::io("inspect Direct Sync state", path, error))?;
    if !metadata.file_type().is_dir() || metadata.file_type().is_symlink() {
        return Err(Error::unsupported(format!(
            "{} must be an ordinary directory",
            path.display()
        )));
    }
    if metadata.permissions().mode() & 0o077 != 0 {
        return Err(Error::invalid(format!(
            "Direct Sync state {} must not be accessible by group or other users",
            path.display()
        )));
    }
    Ok(())
}

fn open_private_directory(parent: &OwnedFd, name: &str, state: &Path) -> Result<OwnedFd> {
    match mkdirat(parent, name, Mode::RUSR | Mode::WUSR | Mode::XUSR) {
        Ok(()) | Err(rustix::io::Errno::EXIST) => {}
        Err(error) => {
            return Err(Error::errno(
                "create Direct Sync state directory",
                state.join(name).as_path(),
                error,
            ));
        }
    }
    let fd = openat(
        parent,
        name,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map_err(|error| Error::errno("open Direct Sync state directory", &state.join(name), error))?;
    fsync(parent)
        .map_err(|error| Error::errno("persist Direct Sync state directory", state, error))?;
    Ok(fd)
}

fn acquire_lease(state_fd: &OwnedFd, state: &Path) -> Result<std::fs::File> {
    let fd = openat(
        state_fd,
        "sync-engine.lock",
        OFlags::RDWR | OFlags::CREATE | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::RUSR | Mode::WUSR,
    )
    .map_err(|error| Error::errno("open Direct Sync lease", state, error))?;
    match flock(&fd, FlockOperation::NonBlockingLockExclusive) {
        Ok(()) => {
            fsync(state_fd)
                .map_err(|error| Error::errno("persist Direct Sync lease", state, error))?;
            Ok(std::fs::File::from(fd))
        }
        Err(rustix::io::Errno::WOULDBLOCK) => Err(Error::busy(format!(
            "another process holds the Direct Sync state lease at {}",
            state.display()
        ))),
        Err(error) => Err(Error::errno(
            "acquire Direct Sync state lease",
            state,
            error,
        )),
    }
}

fn optional_stat(parent: &OwnedFd, name: &str, path: &Path) -> Result<Option<Stat>> {
    match statat(parent, name, AtFlags::SYMLINK_NOFOLLOW) {
        Ok(stat) => Ok(Some(stat)),
        Err(rustix::io::Errno::NOENT) => Ok(None),
        Err(error) => Err(Error::errno("inspect Direct Sync local entry", path, error)),
    }
}

fn identity(fd: &OwnedFd, path: &Path, operation: &str) -> Result<Identity> {
    let stat = fstat(fd).map_err(|error| Error::errno(operation, path, error))?;
    Ok(Identity {
        dev: stat.st_dev,
        ino: stat.st_ino,
    })
}

fn remove_artifact_directory(parent: &OwnedFd, name: &str, display: &Path) -> Result<()> {
    let directory = openat(
        parent,
        name,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map_err(|error| Error::errno("open retained directory artifact", display, error))?;
    let mut names = Vec::new();
    let mut reader = rustix::fs::Dir::read_from(&directory)
        .map_err(|error| Error::errno("read retained directory artifact", display, error))?;
    while let Some(entry) = reader.read() {
        let entry = entry
            .map_err(|error| Error::errno("read retained directory artifact", display, error))?;
        let name = entry
            .file_name()
            .to_str()
            .map_err(|_| Error::unsupported("retained artifact contains a non-UTF-8 name"))?;
        if name != "." && name != ".." {
            names.push(name.to_owned());
        }
    }
    for child in names {
        let stat = statat(&directory, child.as_str(), AtFlags::SYMLINK_NOFOLLOW)
            .map_err(|error| Error::errno("inspect retained directory artifact", display, error))?;
        match FileType::from_raw_mode(stat.st_mode) {
            FileType::RegularFile => {
                unlinkat(&directory, child.as_str(), AtFlags::empty()).map_err(|error| {
                    Error::errno("remove retained artifact file", display, error)
                })?;
            }
            FileType::Directory => {
                remove_artifact_directory(&directory, &child, &display.join(&child))?;
                unlinkat(&directory, child.as_str(), AtFlags::REMOVEDIR).map_err(|error| {
                    Error::errno("remove retained artifact directory", display, error)
                })?;
            }
            _ => {
                return Err(Error::unsupported(
                    "retained directory artifact contains a symlink or special file",
                ));
            }
        }
    }
    Ok(())
}

fn hash_at(parent: &OwnedFd, name: &str, path: &Path) -> Result<(u64, ContentHash, Identity)> {
    let fd = openat(
        parent,
        name,
        OFlags::RDONLY | OFlags::NONBLOCK | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map_err(|error| Error::errno("open Direct Sync local file", path, error))?;
    let before =
        fstat(&fd).map_err(|error| Error::errno("inspect Direct Sync local file", path, error))?;
    if FileType::from_raw_mode(before.st_mode) != FileType::RegularFile {
        return Err(Error::unsupported(format!(
            "{} is not a regular file",
            path.display()
        )));
    }
    let mut file = std::fs::File::from(fd);
    let mut hasher = Hasher::default();
    let mut buffer = vec![0_u8; 64 * 1024];
    loop {
        let count = file
            .read(&mut buffer)
            .map_err(|error| Error::io("read Direct Sync local file", path, error))?;
        if count == 0 {
            break;
        }
        hasher.update(&buffer[..count]);
    }
    let after = fstat(&file)
        .map_err(|error| Error::errno("recheck Direct Sync local file", path, error))?;
    if !same_stable_stat(&before, &after) {
        return Err(Error::conflict(format!(
            "local file changed while hashing: {}",
            path.display()
        )));
    }
    let (length, digest) = hasher.finish();
    Ok((
        length,
        digest,
        Identity {
            dev: before.st_dev,
            ino: before.st_ino,
        },
    ))
}

fn fingerprint_matches_node(hint: &LocalFingerprint, node: &LocalNode) -> bool {
    let LocalKind::File { length, digest } = node.kind else {
        return false;
    };
    let Some(stat) = node.stat else {
        return false;
    };
    hint.length == length
        && hint.digest == *digest.as_bytes()
        && hint.executable == node.executable
        && hint.identity() == node.identity
        && hint.stat.is_some_and(|recorded| {
            recorded.dev == stat.dev
                && recorded.ino == stat.ino
                && recorded.mtime == stat.mtime
                && recorded.mtime_nsec == stat.mtime_nsec
                && recorded.ctime == stat.ctime
                && recorded.ctime_nsec == stat.ctime_nsec
                && super::types::local_stat_is_racy_clean(
                    recorded.mtime,
                    recorded.mtime_nsec,
                    recorded.ctime,
                    recorded.ctime_nsec,
                    hint.recorded_ns,
                )
        })
}

#[derive(Default)]
struct Hasher {
    hasher: blake3::Hasher,
    length: u64,
}

impl Hasher {
    fn update(&mut self, bytes: &[u8]) {
        self.hasher.update(bytes);
        self.length = self.length.saturating_add(bytes.len() as u64);
    }

    fn finish(self) -> (u64, ContentHash) {
        let mut digest = [0_u8; 32];
        digest.copy_from_slice(self.hasher.finalize().as_bytes());
        (self.length, ContentHash::from_bytes(digest))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unsafe_paths_are_rejected() {
        for path in ["/absolute", "../escape", "a//b", "a/./b", "a/", "a\0b"] {
            assert!(validate_path(path).is_err(), "{path:?} must fail");
        }
        for path in ["", "a", "a/b", "%2E/%2E%2E", "space name"] {
            validate_path(path).unwrap();
        }
    }

    #[test]
    fn artifact_budget_releases_oldest_unreferenced_entries() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("root");
        let state = temp.path().join("state");
        let mut tree = LocalTree::create(&root, &state, SyncModel::Managed, "test").unwrap();
        for index in 0..ARTIFACT_BUDGET_COUNT + 6 {
            tree.retain_bytes(&format!("artifact-{index}"), &[index as u8; 16])
                .unwrap();
        }
        assert_eq!(
            tree.store().artifacts().unwrap().len(),
            ARTIFACT_BUDGET_COUNT + 6
        );
        tree.prune_artifacts().unwrap();
        let artifacts = tree.store().artifacts().unwrap();
        assert_eq!(artifacts.len(), ARTIFACT_BUDGET_COUNT);
        assert!(
            artifacts
                .iter()
                .all(|artifact| artifact.origin != "artifact-0")
        );
    }
}
