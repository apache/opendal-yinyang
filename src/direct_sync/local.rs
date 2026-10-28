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

//! fd-relative local storage for Direct Sync.

use super::state::{
    ArtifactKind, ArtifactPhase, ArtifactRecord, DesiredFile, InstallOperation, InstallRecord,
    InstallState, LocalFingerprint, Store,
};
use crate::replica::{ContentHash, Error, Identity, LocalNode, LocalSnapshot, Result, scan_path};
use rustix::fd::OwnedFd;
use rustix::fs::{
    AtFlags, FileType, FlockOperation, Mode, OFlags, RenameFlags, Stat, flock, fstat, fsync,
    mkdirat, open, openat, renameat_with, statat, unlinkat,
};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

static TEMPORARY_SEQUENCE: AtomicU64 = AtomicU64::new(0);

pub(super) struct LocalTree {
    root: PathBuf,
    state: PathBuf,
    root_fd: OwnedFd,
    root_identity: Identity,
    state_identity: Identity,
    install_fd: OwnedFd,
    artifact_fd: OwnedFd,
    _lease: std::fs::File,
}

impl LocalTree {
    pub fn create(root: &Path, state: &Path) -> Result<Self> {
        prepare_directory(root, "create Direct Sync root")?;
        prepare_directory(state, "create Direct Sync state")?;
        Self::open(root, state)
    }

    pub fn open(root: &Path, state: &Path) -> Result<Self> {
        let root = std::fs::canonicalize(root)
            .map_err(|error| Error::io("canonicalize Direct Sync root", root, error))?;
        let state = std::fs::canonicalize(state)
            .map_err(|error| Error::io("canonicalize Direct Sync state", state, error))?;
        if root == state || root.starts_with(&state) || state.starts_with(&root) {
            return Err(Error::invalid(format!(
                "Direct Sync root {} and state {} must not contain one another",
                root.display(),
                state.display()
            )));
        }
        let root_fd = open(
            &root,
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .map_err(|error| Error::errno("open Direct Sync root", &root, error))?;
        let state_fd = open(
            &state,
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .map_err(|error| Error::errno("open Direct Sync state", &state, error))?;
        let root_identity = identity(&root_fd, &root, "inspect Direct Sync root")?;
        let state_identity = identity(&state_fd, &state, "inspect Direct Sync state")?;
        if root_identity.dev != state_identity.dev {
            return Err(Error::unsupported(
                "Direct Sync root and state must share a filesystem for atomic local installs",
            ));
        }
        let _lease = acquire_lease(&state_fd, &state)?;
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
        })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn state(&self) -> &Path {
        &self.state
    }

    pub fn root_identity(&self) -> Identity {
        self.root_identity
    }

    pub fn state_identity(&self) -> Identity {
        self.state_identity
    }

    /// Scan through the existing fd-relative ordinary-directory scanner.
    pub fn scan(&self) -> Result<LocalSnapshot> {
        let snapshot = scan_path(&self.root)?;
        let root = snapshot
            .get("")
            .ok_or_else(|| Error::corrupt("local scan omitted the synchronized root"))?;
        if root.identity != self.root_identity {
            return Err(Error::invalid(
                "Direct Sync root identity changed since the writer opened it",
            ));
        }
        Ok(snapshot)
    }

    pub fn scan_read_only(root: &Path) -> Result<LocalSnapshot> {
        scan_path(root)
    }

    pub fn artifact_path(&self, artifact: &ArtifactRecord) -> PathBuf {
        self.state.join("artifacts").join(&artifact.name)
    }

    pub fn install_path(&self, temporary: &str) -> PathBuf {
        self.state.join("install").join(temporary)
    }

    pub fn install_desired_file(&self, temporary: &str) -> Result<DesiredFile> {
        let (length, digest, _) =
            hash_at(&self.install_fd, temporary, &self.install_path(temporary))?;
        Ok(DesiredFile {
            length,
            digest: *digest.as_bytes(),
        })
    }

    pub fn create_install_file(&self, prefix: &str) -> Result<(String, PathBuf, std::fs::File)> {
        let sequence = TEMPORARY_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let name = format!("{prefix}-{}-{sequence}", std::process::id());
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
        Ok(ArtifactRecord {
            id,
            origin: origin.to_owned(),
            kind,
            digest: *digest.as_bytes(),
            length,
            phase: ArtifactPhase::Retained,
            name,
            error: None,
        })
    }

    pub fn retain_file_path(
        &self,
        source: &Path,
        origin: &str,
        kind: ArtifactKind,
    ) -> Result<ArtifactRecord> {
        let mut source = std::fs::File::open(source)
            .map_err(|error| Error::io("open explicit resolution input", source, error))?;
        let id = uuid::Uuid::new_v4().to_string();
        let name = format!("artifact-{id}");
        let fd = openat(
            &self.artifact_fd,
            name.as_str(),
            OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::RUSR | Mode::WUSR,
        )
        .map_err(|error| Error::errno("create Direct Sync artifact", &self.state, error))?;
        let mut output = std::fs::File::from(fd);
        let mut bucket = Hasher::default();
        let mut buffer = vec![0_u8; 64 * 1024];
        loop {
            let count = source
                .read(&mut buffer)
                .map_err(|error| Error::io("read resolution input", &source, error))?;
            if count == 0 {
                break;
            }
            output
                .write_all(&buffer[..count])
                .map_err(|error| Error::io("write Direct Sync artifact", &self.state, error))?;
            bucket.update(&buffer[..count]);
        }
        output
            .sync_all()
            .map_err(|error| Error::io("persist Direct Sync artifact", &self.state, error))?;
        let (length, digest) = bucket.finish();
        drop(output);
        fsync(&self.artifact_fd).map_err(|error| {
            Error::errno("persist Direct Sync artifact directory", &self.state, error)
        })?;
        Ok(ArtifactRecord {
            id,
            origin: origin.to_owned(),
            kind,
            digest: *digest.as_bytes(),
            length,
            phase: ArtifactPhase::Retained,
            name,
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
            kind,
            digest: *digest.as_bytes(),
            length,
            phase: ArtifactPhase::Retained,
            name: artifact_name,
            error: None,
        })
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

    pub fn recover_installs(&self, store: &mut Store) -> Result<()> {
        for mut install in store.installs()? {
            match install.state {
                InstallState::Prepared => {}
                InstallState::Dispatched => {
                    if let Some(fingerprint) = self.infer_dispatched(&install)? {
                        let baseline = build_baseline(&install, fingerprint)?;
                        store.complete_install(&install.id, baseline.as_ref(), &install.path)?;
                    } else if self.local_matches_expected(&install)? {
                        install.state = InstallState::Prepared;
                        store.update_install(&install)?;
                    } else {
                        install.state = InstallState::Conflict;
                        install.error = Some(
                            "local install outcome is not safely inferable after restart"
                                .to_owned(),
                        );
                        store.update_install(&install)?;
                    }
                }
                InstallState::Applied => {
                    let fingerprint = self.local_fingerprint(&install.path)?;
                    if desired_matches(install.desired_local, Some(fingerprint)) {
                        let baseline = build_baseline(&install, fingerprint)?;
                        store.complete_install(&install.id, baseline.as_ref(), &install.path)?;
                    } else {
                        install.state = InstallState::Conflict;
                        install.error = Some(
                            "applied local install no longer matches its accepted bytes".to_owned(),
                        );
                        store.update_install(&install)?;
                    }
                }
                InstallState::Conflict | InstallState::Failed => {}
            }
        }
        Ok(())
    }

    pub fn apply_install(&self, store: &mut Store, install: &mut InstallRecord) -> Result<()> {
        match install.operation {
            InstallOperation::CreateFile | InstallOperation::ReplaceFile => {
                self.install_file(store, install)
            }
            InstallOperation::DeleteFile => self.delete_file(store, install),
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
        let (length, digest, identity) = hash_at(&parent_fd, name, &self.root.join(path))?;
        Ok(LocalFingerprint {
            length,
            digest: *digest.as_bytes(),
            dev: identity.dev,
            ino: identity.ino,
            parent_dev: parent_identity.dev,
            parent_ino: parent_identity.ino,
        })
    }

    fn install_file(&self, store: &mut Store, install: &mut InstallRecord) -> Result<()> {
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
        let parent_fd = self.open_relative_dir(parent)?;
        let parent_identity = identity(
            &parent_fd,
            &self.root.join(parent),
            "inspect Direct Sync install parent",
        )?;
        let current = optional_stat(&parent_fd, name, &self.root.join(&install.path))?;
        match (install.operation, install.expected_local, current) {
            (InstallOperation::CreateFile, None, None) => {}
            (InstallOperation::ReplaceFile, Some(expected), Some(stat)) => {
                if parent_identity.dev != expected.parent_dev
                    || parent_identity.ino != expected.parent_ino
                    || stat.st_dev != expected.dev
                    || stat.st_ino != expected.ino
                {
                    return self.install_conflict(
                        store,
                        install,
                        "local target changed before install",
                    );
                }
                let (length, digest, identity) =
                    hash_at(&parent_fd, name, &self.root.join(&install.path))?;
                if identity.dev != expected.dev
                    || identity.ino != expected.ino
                    || length != expected.length
                    || digest.as_bytes() != &expected.digest
                {
                    return self.install_conflict(
                        store,
                        install,
                        "local target bytes changed before install",
                    );
                }
            }
            (InstallOperation::CreateFile, None, Some(_)) => {
                return self.install_conflict(store, install, "local create target already exists");
            }
            (InstallOperation::ReplaceFile, Some(_), None) => {
                return self.install_conflict(
                    store,
                    install,
                    "local replacement target disappeared",
                );
            }
            _ => {
                return Err(Error::corrupt(
                    "file install operation and expected local state disagree",
                ));
            }
        }

        if install.operation == InstallOperation::ReplaceFile && install.artifact.is_none() {
            let expected = install
                .expected_local
                .ok_or_else(|| Error::corrupt("replacement has no expected local file"))?;
            let artifact = ArtifactRecord {
                id: uuid::Uuid::new_v4().to_string(),
                origin: install.path.clone(),
                kind: ArtifactKind::DisplacedLocal,
                digest: expected.digest,
                length: expected.length,
                phase: ArtifactPhase::Preparing,
                name: format!("artifact-{}", uuid::Uuid::new_v4()),
                error: None,
            };
            store.insert_artifact(&artifact)?;
            install.artifact = Some(artifact.id);
        }

        install.state = InstallState::Dispatched;
        store.update_install(install)?;
        let exchange = install.operation == InstallOperation::ReplaceFile;
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
                return self.install_conflict(
                    store,
                    install,
                    "local target appeared before install",
                );
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
                    store,
                    install,
                    "local target changed concurrently; replacement was rolled back",
                );
            }
            let artifact_id = install
                .artifact
                .as_deref()
                .ok_or_else(|| Error::corrupt("replacement artifact disappeared"))?;
            let artifact = store
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
            store.update_artifact(artifact_id, ArtifactPhase::Retained, None)?;
        }

        install.state = InstallState::Applied;
        store.update_install(install)?;
        let fingerprint = self.local_fingerprint(&install.path)?;
        if !desired_matches(install.desired_local, Some(fingerprint)) {
            return self.install_conflict(
                store,
                install,
                "installed local bytes changed before baseline acceptance",
            );
        }
        let baseline = build_baseline(install, fingerprint)?;
        store.complete_install(&install.id, baseline.as_ref(), &install.path)?;
        Ok(())
    }

    fn delete_file(&self, store: &mut Store, install: &mut InstallRecord) -> Result<()> {
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
            return self.install_conflict(store, install, "local delete parent changed");
        }
        let (length, digest, identity) = hash_at(&parent_fd, name, &self.root.join(&install.path))?;
        if identity != expected.identity()
            || length != expected.length
            || digest.as_bytes() != &expected.digest
        {
            return self.install_conflict(store, install, "local delete target changed");
        }
        install.state = InstallState::Dispatched;
        store.update_install(install)?;
        unlinkat(&parent_fd, name, AtFlags::empty()).map_err(|error| {
            Error::errno(
                "remove Direct Sync local file",
                &self.root.join(&install.path),
                error,
            )
        })?;
        fsync(&parent_fd).map_err(|error| {
            Error::errno(
                "persist Direct Sync local removal",
                &self.root.join(parent),
                error,
            )
        })?;
        store.complete_install(&install.id, None, &install.path)?;
        Ok(())
    }

    fn install_conflict(
        &self,
        store: &mut Store,
        install: &mut InstallRecord,
        reason: &str,
    ) -> Result<()> {
        install.state = InstallState::Conflict;
        install.error = Some(reason.to_owned());
        store.update_install(install)?;
        Err(Error::conflict(reason))
    }

    fn infer_dispatched(&self, install: &InstallRecord) -> Result<Option<LocalFingerprint>> {
        match install.operation {
            InstallOperation::CreateFile => {
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
            InstallOperation::ReplaceFile => {
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
            InstallOperation::DeleteFile => {
                if self.optional_local_fingerprint(&install.path)?.is_none() {
                    Ok(Some(LocalFingerprint {
                        length: 0,
                        digest: [0; 32],
                        dev: 0,
                        ino: 0,
                        parent_dev: 0,
                        parent_ino: 0,
                    }))
                } else {
                    Ok(None)
                }
            }
        }
    }

    fn local_matches_expected(&self, install: &InstallRecord) -> Result<bool> {
        match (install.operation, install.expected_local) {
            (InstallOperation::DeleteFile, _) | (InstallOperation::CreateFile, None) => Ok(true),
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
            dev: identity.dev,
            ino: identity.ino,
            parent_dev: 0,
            parent_ino: 0,
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
}

pub(super) fn validate_path(path: &str) -> Result<()> {
    if path.is_empty() {
        return Ok(());
    }
    if path.starts_with('/') || path.ends_with('/') {
        return Err(Error::invalid(format!(
            "Direct Sync path must be relative without a trailing slash: {path:?}"
        )));
    }
    for component in path.split('/') {
        if component.is_empty() || component == "." || component == ".." || component.contains('\0')
        {
            return Err(Error::invalid(format!(
                "unsafe Direct Sync path component in {path:?}"
            )));
        }
    }
    Ok(())
}

pub(super) fn split_path(path: &str) -> Result<(&str, &str)> {
    validate_path(path)?;
    let (parent, name) = path.rsplit_once('/').unwrap_or(("", path));
    if name.is_empty() {
        return Err(Error::invalid("Direct Sync file path has no file name"));
    }
    Ok((parent, name))
}

fn build_baseline(
    install: &InstallRecord,
    local: LocalFingerprint,
) -> Result<Option<super::state::BaselineEntry>> {
    let remote = match install.operation {
        InstallOperation::CreateFile | InstallOperation::ReplaceFile => install
            .desired_remote
            .clone()
            .ok_or_else(|| Error::corrupt("file install has no remote pin"))?,
        InstallOperation::DeleteFile => return Ok(None),
    };
    Ok(Some(super::state::BaselineEntry {
        path: install.path.clone(),
        remote,
        local,
    }))
}

fn desired_matches(desired: Option<DesiredFile>, fingerprint: Option<LocalFingerprint>) -> bool {
    match (desired, fingerprint) {
        (Some(desired), Some(fingerprint)) => {
            desired.length == fingerprint.length && desired.digest == fingerprint.digest
        }
        _ => false,
    }
}

fn prepare_directory(path: &Path, operation: &str) -> Result<()> {
    match std::fs::create_dir(path) {
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
        "direct-sync.lock",
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

fn identity(fd: &OwnedFd, path: &Path, operation: &str) -> Result<Identity> {
    let stat = fstat(fd).map_err(|error| Error::errno(operation, path, error))?;
    Ok(Identity {
        dev: stat.st_dev,
        ino: stat.st_ino,
    })
}

fn optional_stat(parent: &OwnedFd, name: &str, path: &Path) -> Result<Option<Stat>> {
    match statat(parent, name, AtFlags::SYMLINK_NOFOLLOW) {
        Ok(stat) => Ok(Some(stat)),
        Err(rustix::io::Errno::NOENT) => Ok(None),
        Err(error) => Err(Error::errno("inspect Direct Sync local entry", path, error)),
    }
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

fn same_stable_stat(left: &Stat, right: &Stat) -> bool {
    left.st_dev == right.st_dev
        && left.st_ino == right.st_ino
        && left.st_size == right.st_size
        && left.st_mtime == right.st_mtime
        && left.st_mtime_nsec == right.st_mtime_nsec
        && left.st_ctime == right.st_ctime
        && left.st_ctime_nsec == right.st_ctime_nsec
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
}
