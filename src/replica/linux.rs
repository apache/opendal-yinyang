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

use super::backup::BackupManifest;
use super::state::{
    ArtifactRecord, ArtifactStat, ConflictRecord, InstallPhase, InstallRecord, LocalScanHint, Store,
};
use super::{Error, Result};
use rustix::fd::OwnedFd;
use rustix::fs::{
    AtFlags, Dir, FileType, FlockOperation, Mode, OFlags, RenameFlags, Stat, fchmod, flock, fstat,
    fsync, mkdirat, open, openat, renameat_with, statat, unlinkat,
};
use std::collections::{BTreeMap, BTreeSet};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use yinyang_core::{NodeId, Revision};

/// Maximum number of non-conflict artifacts retained by one replica.
pub const ARTIFACT_BUDGET_COUNT: usize = 64;
/// Maximum bytes retained by non-conflict artifacts in one replica.
pub const ARTIFACT_BUDGET_BYTES: u64 = 256 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct ContentHash([u8; 32]);

impl ContentHash {
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    pub fn to_hex(self) -> String {
        self.0.iter().map(|byte| format!("{byte:02x}")).collect()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LocalKind {
    Directory,
    File { length: u64, digest: ContentHash },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct Identity {
    pub dev: u64,
    pub ino: u64,
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ArtifactKind {
    ReplacedFile,
    RollbackDisplaced,
    QuarantinedFile,
    QuarantinedDirectory,
}

impl ArtifactKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::ReplacedFile => "replaced-file",
            Self::RollbackDisplaced => "rollback-displaced",
            Self::QuarantinedFile => "quarantined-file",
            Self::QuarantinedDirectory => "quarantined-directory",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Artifact {
    pub id: String,
    pub origin: String,
    pub name: String,
    pub kind: String,
    pub reason: String,
    pub phase: String,
    pub error: Option<String>,
    pub path: PathBuf,
    pub exportable: bool,
    pub length: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Conflict {
    pub id: String,
    pub path: String,
    pub original_path: Option<String>,
    pub desired_path: Option<String>,
    pub local: String,
    pub remote: String,
    pub local_artifact: Option<String>,
    pub remote_node: Option<NodeId>,
    pub remote_revision: Option<[u8; 24]>,
    pub error: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InstallOutcome {
    pub artifact: Artifact,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CapturedContent {
    pub artifact: Artifact,
    pub length: u64,
    pub digest: ContentHash,
    pub executable: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicaStatus {
    pub root: PathBuf,
    pub filesystem: NodeId,
    pub pending_installs: usize,
    pub artifacts: Vec<Artifact>,
    pub retained_bytes: u64,
    pub conflicts: Vec<Conflict>,
    pub baseline: Option<ReplicaBaseline>,
}

/// Remote revisions recorded by the local common baseline.
///
/// `revision` is the global baseline revision. `entries` contains the distinct
/// per-entry remote revisions used as future staging predicates, excluding the
/// global revision itself.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicaBaseline {
    pub revision: Revision,
    pub entries: Vec<Revision>,
}

/// Revision-protection facts readable from a replica state database without
/// opening the bound local root or revalidating retained artifact bytes.
///
/// This supports validated backup packages whose original root may no longer
/// exist. Use [`Replica::inspect`] for a live local root.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicaProtection {
    pub filesystem: NodeId,
    pub pending_installs: u64,
    pub artifacts: u64,
    pub exportable_artifacts: u64,
    pub conflicts: Vec<Revision>,
    pub baseline: Option<ReplicaBaseline>,
}

pub struct Replica {
    root: PathBuf,
    state: PathBuf,
    filesystem: NodeId,
    root_identity: Identity,
    state_identity: Identity,
    root_fd: OwnedFd,
    quarantine_fd: OwnedFd,
    _lease: Option<std::fs::File>,
    store: Store,
    hash_bytes: Arc<AtomicU64>,
}

fn baseline_from_store(store: &Store) -> Result<Option<ReplicaBaseline>> {
    let Some(baseline) = store.load_baseline()? else {
        return Ok(None);
    };
    let entries = baseline
        .remote
        .values()
        .map(|node| node.revision)
        .filter(|revision| *revision != baseline.revision)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    Ok(Some(ReplicaBaseline {
        revision: baseline.revision,
        entries,
    }))
}

impl std::fmt::Debug for Replica {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Replica")
            .field("root", &self.root)
            .field("state", &self.state)
            .field("filesystem", &self.filesystem)
            .field("root_identity", &self.root_identity)
            .field("state_identity", &self.state_identity)
            .finish_non_exhaustive()
    }
}

impl Replica {
    /// Create or reopen the durable binding between one local root and one
    /// filesystem. State and payload must share a filesystem so exchange and
    /// quarantine moves never degrade into copy-and-delete.
    pub fn init(state: &Path, root: &Path, filesystem: NodeId) -> Result<Self> {
        std::fs::create_dir_all(root).map_err(|e| Error::io("create root", root, e))?;
        std::fs::create_dir_all(state).map_err(|e| Error::io("create state", state, e))?;
        let root = std::fs::canonicalize(root).map_err(|e| Error::io("canonicalize", root, e))?;
        let state =
            std::fs::canonicalize(state).map_err(|e| Error::io("canonicalize", state, e))?;
        if state.starts_with(&root) {
            return Err(Error::invalid(format!(
                "sync state {} must be outside the synchronized root {}",
                state.display(),
                root.display()
            )));
        }
        let root_text = root
            .to_str()
            .ok_or_else(|| Error::unsupported("local root path is not UTF-8"))?
            .to_owned();
        let (root_fd, root_identity) = open_bound_directory(&root, "open local root")?;
        let (state_fd, state_identity) = open_bound_directory(&state, "open sync state")?;
        if root_identity.dev != state_identity.dev {
            return Err(Error::unsupported(
                "local root and sync state must be on the same filesystem for atomic exchange and quarantine",
            ));
        }
        let lease = acquire_state_lease(&state_fd, &state)?;
        let mut store = Store::open(&state.join("replica.db"))?;
        let requested = super::state::Binding {
            filesystem,
            root: root_text,
            root_dev: root_identity.dev,
            root_ino: root_identity.ino,
            state_dev: state_identity.dev,
            state_ino: state_identity.ino,
        };
        if !store.initialize_binding(&requested)? {
            let stored = store.binding()?.ok_or_else(|| {
                Error::corrupt("replica binding disappeared after initialization")
            })?;
            return Err(Error::invalid(format!(
                "replica state is already bound to filesystem {} at {} (root identity {}/{}, state identity {}/{}); requested filesystem {} at {}",
                hex(stored.filesystem.as_bytes()),
                stored.root,
                stored.root_dev,
                stored.root_ino,
                stored.state_dev,
                stored.state_ino,
                hex(filesystem.as_bytes()),
                root.display(),
            )));
        }
        let quarantine_fd = open_quarantine(&state_fd)?;
        let mut replica = Self {
            root,
            state,
            filesystem,
            root_identity,
            state_identity,
            root_fd,
            quarantine_fd,
            _lease: Some(lease),
            store,
            hash_bytes: Arc::new(AtomicU64::new(0)),
        };
        replica.recover_incomplete()?;
        Ok(replica)
    }

    /// Complete an isolated restore after the state and local root have been
    /// copied. Journaled installs must be terminal; assigning new identities to
    /// an interrupted exchange would invent recovery evidence.
    pub(super) fn restore(
        state: &Path,
        root: &Path,
        filesystem: NodeId,
        manifest: &BackupManifest,
    ) -> Result<Self> {
        let root = std::fs::canonicalize(root).map_err(|e| Error::io("canonicalize", root, e))?;
        let state =
            std::fs::canonicalize(state).map_err(|e| Error::io("canonicalize", state, e))?;
        if state.starts_with(&root) {
            return Err(Error::invalid(format!(
                "sync state {} must be outside the synchronized root {}",
                state.display(),
                root.display()
            )));
        }
        let root_text = root
            .to_str()
            .ok_or_else(|| Error::unsupported("local root path is not UTF-8"))?
            .to_owned();
        let (root_fd, root_identity) = open_bound_directory(&root, "open restored local root")?;
        let (state_fd, state_identity) = open_bound_directory(&state, "open restored sync state")?;
        if root_identity.dev != state_identity.dev {
            return Err(Error::unsupported(
                "restored local root and sync state must be on the same filesystem",
            ));
        }
        let lease = acquire_state_lease(&state_fd, &state)?;
        let store = Store::open(&state.join("replica.db"))?;
        let previous = store
            .binding()?
            .ok_or_else(|| Error::invalid("sync state is not initialized; run sync init"))?;
        if previous.filesystem != filesystem {
            return Err(Error::invalid(format!(
                "replica is bound to filesystem {}, not {}",
                hex(previous.filesystem.as_bytes()),
                hex(filesystem.as_bytes())
            )));
        }
        if !store.incomplete_installs()?.is_empty() {
            return Err(Error::invalid(
                "sync state contains an incomplete local install; finish recovery with the original root and state before taking or restoring the backup",
            ));
        }
        let quarantine_fd = open_quarantine(&state_fd)?;
        let binding = super::state::Binding {
            filesystem,
            root: root_text,
            root_dev: root_identity.dev,
            root_ino: root_identity.ino,
            state_dev: state_identity.dev,
            state_ino: state_identity.ino,
        };
        let mut replica = Self {
            root,
            state,
            filesystem,
            root_identity,
            state_identity,
            root_fd,
            quarantine_fd,
            _lease: Some(lease),
            store,
            hash_bytes: Arc::new(AtomicU64::new(0)),
        };
        manifest.apply(&mut replica, &binding)?;
        replica.verify_retained_artifacts()?;
        Ok(replica)
    }

    /// Validate existing replica state without taking the lease, upgrading the
    /// schema, recovering installs, or observing the local root.
    pub(super) fn validate_existing(state: &Path, filesystem: NodeId) -> Result<()> {
        let path = state.join("replica.db");
        if !path
            .try_exists()
            .map_err(|e| Error::io("inspect", &path, e))?
        {
            return Ok(());
        }
        let store = Store::open_read_only(&path)?;
        if let Some(binding) = store.binding()?
            && binding.filesystem != filesystem
        {
            return Err(Error::invalid(format!(
                "replica is bound to filesystem {}, not {}",
                hex(binding.filesystem.as_bytes()),
                hex(filesystem.as_bytes())
            )));
        }
        Ok(())
    }

    pub fn open(state: &Path, filesystem: NodeId) -> Result<Self> {
        let state =
            std::fs::canonicalize(state).map_err(|e| Error::io("canonicalize", state, e))?;
        let (state_fd, state_identity) = open_bound_directory(&state, "open sync state")?;
        let lease = acquire_state_lease(&state_fd, &state)?;
        let store = Store::open(&state.join("replica.db"))?;
        let binding = store
            .binding()?
            .ok_or_else(|| Error::invalid("sync state is not initialized; run sync init"))?;
        if binding.filesystem != filesystem {
            return Err(Error::invalid(format!(
                "replica is bound to filesystem {}, not {}",
                hex(binding.filesystem.as_bytes()),
                hex(filesystem.as_bytes())
            )));
        }
        if binding.state_dev != state_identity.dev || binding.state_ino != state_identity.ino {
            return Err(Error::invalid(format!(
                "sync state identity changed at {}; expected {}/{}, found {}/{}",
                state.display(),
                binding.state_dev,
                binding.state_ino,
                state_identity.dev,
                state_identity.ino
            )));
        }
        let root = PathBuf::from(&binding.root);
        let (root_fd, root_identity) = open_bound_directory(&root, "open bound local root")?;
        if binding.root_dev != root_identity.dev || binding.root_ino != root_identity.ino {
            return Err(Error::invalid(format!(
                "bound local root identity changed at {}; expected {}/{}, found {}/{}",
                root.display(),
                binding.root_dev,
                binding.root_ino,
                root_identity.dev,
                root_identity.ino
            )));
        }
        if root_identity.dev != state_identity.dev {
            return Err(Error::unsupported(
                "local root and sync state no longer share a filesystem",
            ));
        }
        let quarantine_fd = open_quarantine(&state_fd)?;
        let mut replica = Self {
            root,
            state,
            filesystem,
            root_identity,
            state_identity,
            root_fd,
            quarantine_fd,
            _lease: Some(lease),
            store,
            hash_bytes: Arc::new(AtomicU64::new(0)),
        };
        replica.recover_incomplete()?;
        Ok(replica)
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn state(&self) -> &Path {
        &self.state
    }

    pub fn filesystem(&self) -> NodeId {
        self.filesystem
    }

    pub fn hash_bytes_read(&self) -> u64 {
        self.hash_bytes.load(Ordering::Relaxed)
    }

    pub fn reset_hash_bytes_read(&self) {
        self.hash_bytes.store(0, Ordering::Relaxed);
    }

    pub(super) fn store(&self) -> &Store {
        &self.store
    }

    pub(super) fn store_mut(&mut self) -> &mut Store {
        &mut self.store
    }

    pub fn status(&mut self) -> Result<ReplicaStatus> {
        self.verify_retained_artifacts()?;
        let artifacts = self
            .store
            .artifacts()?
            .into_iter()
            .map(|record| self.artifact_from(record))
            .collect::<Vec<_>>();
        let retained_bytes = artifacts.iter().map(|artifact| artifact.length).sum();
        let conflicts = self
            .store
            .conflicts()?
            .into_iter()
            .map(Conflict::from)
            .collect();
        Ok(ReplicaStatus {
            root: self.root.clone(),
            filesystem: self.filesystem,
            pending_installs: self.store.incomplete_installs()?.len(),
            artifacts,
            retained_bytes,
            conflicts,
            baseline: baseline_from_store(&self.store)?,
        })
    }

    /// Read-only bound status. No exclusive lease, no recovery and no writes,
    /// so progress remains inspectable while another process runs.
    pub fn inspect(state: &Path) -> Result<ReplicaStatus> {
        Self::open_read_only(state)?.inspect_status_readonly()
    }

    /// Read-only local scan without acquiring the writer lease or performing
    /// recovery, so a bound root stays inspectable while a writer runs.
    pub fn scan_read_only(state: &Path) -> Result<LocalSnapshot> {
        Self::open_read_only(state)?.scan()
    }

    /// Read revision-protection facts from a replica state database without
    /// opening the bound local root.
    ///
    /// A backup package records its original root path but keeps the root
    /// payload inside the package. Inspecting protection must therefore not
    /// require that original path to still exist.
    pub fn inspect_protection(state: &Path) -> Result<ReplicaProtection> {
        let store = Store::open_read_only(&state.join("replica.db"))?;
        let binding = store
            .binding()?
            .ok_or_else(|| Error::invalid("sync state is not initialized; run sync init"))?;
        let artifacts = store.artifacts()?;
        let exportable = artifacts
            .iter()
            .filter(|artifact| artifact.kind != "quarantined-directory")
            .count();
        let conflicts = store
            .conflicts()?
            .into_iter()
            .filter_map(|conflict| conflict.remote_revision)
            .map(Revision::from_bytes)
            .collect();
        Ok(ReplicaProtection {
            filesystem: binding.filesystem,
            pending_installs: u64::try_from(store.incomplete_installs()?.len()).unwrap_or(u64::MAX),
            artifacts: u64::try_from(artifacts.len()).unwrap_or(u64::MAX),
            exportable_artifacts: u64::try_from(exportable).unwrap_or(u64::MAX),
            conflicts,
            baseline: baseline_from_store(&store)?,
        })
    }

    pub(crate) fn open_read_only(state: &Path) -> Result<Self> {
        let state =
            std::fs::canonicalize(state).map_err(|e| Error::io("canonicalize", state, e))?;
        let (state_fd, state_identity) = open_bound_directory(&state, "open sync state")?;
        let store = Store::open_read_only(&state.join("replica.db"))?;
        let binding = store
            .binding()?
            .ok_or_else(|| Error::invalid("sync state is not initialized; run sync init"))?;
        if binding.state_dev != state_identity.dev || binding.state_ino != state_identity.ino {
            return Err(Error::invalid("sync state identity changed"));
        }
        let root = PathBuf::from(&binding.root);
        let (root_fd, root_identity) = open_bound_directory(&root, "open bound local root")
            .map_err(|error| Error::invalid(format!("bound local root is unavailable: {error}")))?;
        if root_identity.dev != binding.root_dev || root_identity.ino != binding.root_ino {
            return Err(Error::invalid("bound local root identity changed"));
        }
        if root_identity.dev != state_identity.dev {
            return Err(Error::invalid(
                "bound local root and sync state no longer share a filesystem",
            ));
        }
        let quarantine_fd = openat(
            &state_fd,
            "quarantine",
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .map_err(|e| Error::errno("open retained artifact directory", &state, e))?;
        Ok(Self {
            root,
            state,
            filesystem: binding.filesystem,
            root_identity,
            state_identity,
            root_fd,
            quarantine_fd,
            _lease: None,
            store,
            hash_bytes: Arc::new(AtomicU64::new(0)),
        })
    }

    fn inspect_status_readonly(&self) -> Result<ReplicaStatus> {
        let records = self.store.artifacts()?;
        let installs = self.store.installs()?;
        let mut artifacts: Vec<Artifact> = records
            .iter()
            .cloned()
            .map(|record| self.artifact_from(record))
            .collect();
        for (artifact, record) in artifacts.iter_mut().zip(records) {
            let install = installs
                .iter()
                .find(|install| install.artifact == record.name);
            if let Some(install) = install
                && (install.phase == InstallPhase::Retained
                    || install.phase == InstallPhase::Conflict)
            {
                let matches = self.artifact_matches_expected(install).unwrap_or_default();
                if !matches {
                    artifact.phase = "conflict".to_owned();
                    artifact.error = Some(
                        "retained inode changed after installation; export before cleanup"
                            .to_owned(),
                    );
                }
            } else if record.phase == "retained"
                && !self.artifact_matches_record(&record).unwrap_or_default()
            {
                artifact.phase = "conflict".to_owned();
                artifact.error = Some(
                    "retained artifact changed after creation; export before cleanup".to_owned(),
                );
            }
        }
        let conflicts = self
            .store
            .conflicts()?
            .into_iter()
            .map(Conflict::from)
            .collect();
        let retained_bytes = artifacts.iter().map(|artifact| artifact.length).sum();
        Ok(ReplicaStatus {
            root: self.root.clone(),
            filesystem: self.filesystem,
            pending_installs: self.store.incomplete_installs()?.len(),
            artifacts,
            retained_bytes,
            conflicts,
            baseline: baseline_from_store(&self.store)?,
        })
    }

    fn verify_retained_artifacts(&mut self) -> Result<()> {
        for mut install in self.store.installs()? {
            if install.phase != InstallPhase::Retained {
                continue;
            }
            let matches = match self.artifact_matches_expected(&install) {
                Ok(matches) => matches,
                Err(error) => {
                    install.phase = InstallPhase::Conflict;
                    install.error = Some(format!(
                        "retained artifact could not be revalidated: {error}"
                    ));
                    self.store.update_install(&install)?;
                    self.store.update_artifact(
                        &install.id,
                        "conflict",
                        install.error.as_deref(),
                    )?;
                    continue;
                }
            };
            if !matches {
                install.phase = InstallPhase::Conflict;
                install.error = Some(
                    "retained inode changed after installation; newly written bytes remain in the artifact"
                        .to_owned(),
                );
                self.store.update_install(&install)?;
                self.store
                    .update_artifact(&install.id, "conflict", install.error.as_deref())?;
            } else {
                self.refresh_artifact_stat(&install)?;
            }
        }
        for mut artifact in self.store.artifacts()? {
            if artifact.phase != "retained" {
                continue;
            }
            match self.artifact_matches_record(&artifact) {
                Ok(true) => {
                    if let Some(stat) =
                        optional_stat(&self.quarantine_fd, &artifact.name, &self.state)?
                    {
                        let length =
                            if FileType::from_raw_mode(stat.st_mode) == FileType::RegularFile {
                                stat.st_size as u64
                            } else {
                                artifact.length
                            };
                        if !artifact_stat_matches(&artifact, &stat) {
                            self.store.update_artifact_stat(
                                &artifact.id,
                                length,
                                artifact_stat(&stat),
                            )?;
                            artifact.length = length;
                            artifact.stat = Some(artifact_stat(&stat));
                        }
                    }
                }
                Ok(false) | Err(_) => {
                    artifact.phase = "conflict".to_owned();
                    artifact.error = Some(
                        "retained artifact changed after creation; export before cleanup"
                            .to_owned(),
                    );
                    self.store.update_artifact(
                        &artifact.id,
                        &artifact.phase,
                        artifact.error.as_deref(),
                    )?;
                }
            }
        }
        Ok(())
    }

    /// Scan and hash every supported local entry. A transient I/O failure, a
    /// symlink or a mutating file aborts the scan; callers must not convert an
    /// aborted scan into a delete set.
    pub fn scan(&self) -> Result<LocalSnapshot> {
        let hints = self
            .store
            .load_baseline()?
            .map(|baseline| baseline.local_scan_hints)
            .unwrap_or_default();
        let mut nodes = BTreeMap::new();
        nodes.insert(
            String::new(),
            LocalNode {
                path: String::new(),
                kind: LocalKind::Directory,
                executable: false,
                identity: self.root_identity,
                parent: None,
                stat: None,
            },
        );
        self.scan_directory(
            &self.root_fd,
            "",
            self.root_identity,
            &mut nodes,
            Some(&hints),
        )?;
        if self._lease.is_some() {
            for (path, node) in &nodes {
                if !hints
                    .get(path)
                    .is_some_and(|hint| scan_hint_matches_node(node, hint))
                {
                    self.store.save_local_scan_evidence(node)?;
                }
            }
        }
        Ok(LocalSnapshot { nodes })
    }

    /// Recheck exactly one path. Files are hashed only when their current bytes
    /// are needed; directories use their descriptor identity directly.
    pub fn observe_path(&self, path: &str) -> Result<Option<LocalNode>> {
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
        validate_path(path)?;
        let (parent, name) = split_path(path)?;
        let Some(parent_fd) = self.open_relative_dir_optional(parent)? else {
            return Ok(None);
        };
        let parent_identity = identity(&parent_fd, &self.root, "inspect observed parent")?;
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
                        "open observed local directory",
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
                        "inspect observed local directory",
                    )?,
                    parent: Some(parent_identity),
                    stat: Some(local_stat(&fstat(&fd).map_err(|error| {
                        Error::errno(
                            "inspect observed local directory",
                            &self.root.join(path),
                            error,
                        )
                    })?)),
                }))
            }
            FileType::RegularFile => {
                let hint = self.store.local_scan_hint(path)?;
                if let Some(hint) = hint.filter(|hint| scan_hint_matches(&stat, hint)) {
                    let mut node = hint.node;
                    node.path = path.to_owned();
                    node.parent = Some(parent_identity);
                    node.stat = Some(local_stat(&stat));
                    return Ok(Some(node));
                }
                let (length, digest, identity, executable) = hash_file_at_counted(
                    &parent_fd,
                    name,
                    &self.root.join(path),
                    Some(&self.hash_bytes),
                )?;
                let node = LocalNode {
                    path: path.to_owned(),
                    kind: LocalKind::File { length, digest },
                    executable,
                    identity,
                    parent: Some(parent_identity),
                    stat: Some(local_stat(&stat)),
                };
                if self._lease.is_some() {
                    self.store.save_local_scan_evidence(&node)?;
                }
                Ok(Some(node))
            }
            FileType::Symlink => Err(Error::unsupported(format!(
                "local symlink is not supported: {path}"
            ))),
            _ => Err(Error::unsupported(format!(
                "local special file is not supported: {path}"
            ))),
        }
    }

    /// Scan exactly one path and, for a directory, its descendants.
    pub fn scan_subtree(&self, path: &str) -> Result<LocalSnapshot> {
        if path.is_empty() {
            return self.scan();
        }
        let hints = self
            .store
            .load_baseline()?
            .map(|baseline| baseline.local_scan_hints)
            .unwrap_or_default();
        validate_path(path)?;
        let (parent, name) = split_path(path)?;
        let parent_fd = self.open_relative_dir(parent)?;
        let parent_identity = identity(&parent_fd, &self.root, "inspect subtree parent")?;
        let stat = statat(&parent_fd, name, AtFlags::SYMLINK_NOFOLLOW)
            .map_err(|error| Error::errno("inspect local subtree", &self.root.join(path), error))?;
        let mut nodes = BTreeMap::new();
        match FileType::from_raw_mode(stat.st_mode) {
            FileType::Directory => {
                let fd = openat(
                    &parent_fd,
                    name,
                    OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                    Mode::empty(),
                )
                .map_err(|error| {
                    Error::errno("open local subtree", &self.root.join(path), error)
                })?;
                let root_identity = identity(&fd, &self.root.join(path), "inspect local subtree")?;
                nodes.insert(
                    path.to_owned(),
                    LocalNode {
                        path: path.to_owned(),
                        kind: LocalKind::Directory,
                        executable: false,
                        identity: root_identity,
                        parent: Some(parent_identity),
                        stat: Some(local_stat(&stat)),
                    },
                );
                scan_directory(
                    &fd,
                    path,
                    root_identity,
                    &self.root,
                    self.root_identity.dev,
                    Some(&self.hash_bytes),
                    Some(&hints),
                    &mut nodes,
                )?;
            }
            FileType::RegularFile => {
                let mut node = match hints
                    .get(path)
                    .filter(|hint| scan_hint_matches(&stat, hint))
                {
                    Some(hint) => hint.node.clone(),
                    None => {
                        let (length, digest, identity, executable) = hash_file_at_counted(
                            &parent_fd,
                            name,
                            &self.root.join(path),
                            Some(&self.hash_bytes),
                        )?;
                        LocalNode {
                            path: path.to_owned(),
                            kind: LocalKind::File { length, digest },
                            executable,
                            identity,
                            parent: Some(parent_identity),
                            stat: Some(local_stat(&stat)),
                        }
                    }
                };
                node.path = path.to_owned();
                node.parent = Some(parent_identity);
                node.stat = Some(local_stat(&stat));
                nodes.insert(path.to_owned(), node);
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
        if self._lease.is_some() {
            for (path, node) in &nodes {
                if !hints
                    .get(path)
                    .is_some_and(|hint| scan_hint_matches_node(node, hint))
                {
                    self.store.save_local_scan_evidence(node)?;
                }
            }
        }
        Ok(LocalSnapshot::from_nodes(nodes))
    }

    /// Open the exact scanned inode for a one-pass remote content comparison.
    pub fn open_observed_file(&self, path: &str, expected: &LocalNode) -> Result<std::fs::File> {
        let LocalKind::File { .. } = expected.kind else {
            return Err(Error::invalid("observed content must be a regular file"));
        };
        let (parent, name) = split_path(path)?;
        let parent_fd = self.open_relative_dir(parent)?;
        let fd = openat(
            &parent_fd,
            name,
            OFlags::RDONLY | OFlags::NONBLOCK | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .map_err(|error| Error::errno("open observed local file", &self.root.join(path), error))?;
        let stat = fstat(&fd).map_err(|error| {
            Error::errno("inspect observed local file", &self.root.join(path), error)
        })?;
        if FileType::from_raw_mode(stat.st_mode) != FileType::RegularFile
            || stat.st_dev != expected.identity.dev
            || stat.st_ino != expected.identity.ino
        {
            return Err(Error::conflict(
                "observed local file identity changed before use",
            ));
        }
        Ok(std::fs::File::from(fd))
    }

    pub fn install_bytes(
        &mut self,
        path: &str,
        expected: &LocalNode,
        executable: bool,
        remote_revision: Option<[u8; 24]>,
        bytes: &[u8],
    ) -> Result<InstallOutcome> {
        let mut install = self.prepare_install(path, expected, executable, remote_revision)?;
        install
            .write_all(bytes)
            .map_err(|e| Error::invalid(format!("write replacement: {e}")))?;
        install.finish()
    }

    /// Copy one observed local file into retained state before an upload. The
    /// capture is immutable and remains exportable after the local path changes.
    pub fn capture_file(&mut self, path: &str, expected: &LocalNode) -> Result<CapturedContent> {
        let LocalKind::File {
            length: expected_length,
            digest: expected_digest,
        } = expected.kind
        else {
            return Err(Error::invalid("captured content must be a regular file"));
        };
        if expected.path != path {
            return Err(Error::invalid(
                "captured content path does not match observation",
            ));
        }
        let (parent, name) = split_path(path)?;
        let parent_fd = self.open_relative_dir(parent)?;
        let parent_identity = identity(&parent_fd, &self.root, "inspect upload parent")?;
        if let Some(expected_parent) = expected.parent
            && parent_identity != expected_parent
        {
            return Err(Error::conflict("upload parent identity changed"));
        }
        let id = uuid::Uuid::new_v4().to_string();
        let artifact_name = format!("upload-{id}");
        let mut artifact = ArtifactRecord {
            id: id.clone(),
            origin: path.to_owned(),
            name: artifact_name.clone(),
            kind: "upload-retained".to_owned(),
            reason: "durable local input retained for publication and export".to_owned(),
            remote_revision: None,
            phase: "preparing".to_owned(),
            error: None,
            digest: None,
            length: expected_length,
            stat: None,
            settle_pending: false,
        };
        self.store.insert_artifact(&artifact)?;
        let output = openat(
            &self.quarantine_fd,
            artifact_name.as_str(),
            OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::RUSR | Mode::WUSR,
        )
        .map_err(|e| Error::errno("create retained upload", &self.state, e))?;
        let output_identity = identity(&output, &self.state, "inspect retained upload")?;
        let capture = (|| -> Result<CapturedContent> {
            let source = openat(
                &parent_fd,
                name,
                OFlags::RDONLY | OFlags::NONBLOCK | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                Mode::empty(),
            )
            .map_err(|e| Error::errno("open upload source", &self.root.join(path), e))?;
            let before = fstat(&source)
                .map_err(|e| Error::errno("inspect upload source", &self.root.join(path), e))?;
            if FileType::from_raw_mode(before.st_mode) != FileType::RegularFile
                || before.st_dev != expected.identity.dev
                || before.st_ino != expected.identity.ino
            {
                return Err(Error::conflict(
                    "upload source identity changed before capture",
                ));
            }
            let mut source = std::fs::File::from(source);
            let mut output = std::fs::File::from(output);
            let mut hasher = blake3::Hasher::new();
            let mut length = 0_u64;
            let mut buffer = vec![0_u8; 64 * 1024];
            loop {
                let count = source
                    .read(&mut buffer)
                    .map_err(|e| Error::io("read upload source", &self.root.join(path), e))?;
                if count == 0 {
                    break;
                }
                output.write_all(&buffer[..count]).map_err(|e| {
                    Error::io("write retained upload", &self.state.join(&artifact_name), e)
                })?;
                hasher.update(&buffer[..count]);
                length = length
                    .checked_add(count as u64)
                    .ok_or_else(|| Error::invalid("upload length overflowed"))?;
            }
            let after = fstat(&source)
                .map_err(|e| Error::errno("recheck upload source", &self.root.join(path), e))?;
            if !same_stable_stat(&before, &after) {
                return Err(Error::conflict(
                    "upload source changed during durable capture",
                ));
            }
            output.sync_all().map_err(|e| {
                Error::io(
                    "persist retained upload",
                    &self.state.join(&artifact_name),
                    e,
                )
            })?;
            let retained = fstat(&output)
                .map_err(|e| Error::errno("inspect retained upload", &self.state, e))?;
            let digest = ContentHash(*hasher.finalize().as_bytes());
            if length != expected_length || digest.as_bytes() != expected_digest.as_bytes() {
                return Err(Error::conflict(
                    "durable upload capture does not match the scanned local version",
                ));
            }
            fsync(&self.quarantine_fd)
                .map_err(|e| Error::errno("persist retained upload directory", &self.state, e))?;
            self.store
                .update_artifact_stat(&artifact.id, length, artifact_stat(&retained))?;
            self.store
                .update_artifact_digest(&artifact.id, *digest.as_bytes())?;
            artifact.digest = Some(*digest.as_bytes());
            artifact.length = length;
            artifact.stat = Some(artifact_stat(&retained));
            artifact.phase = "retained".to_owned();
            self.store
                .update_artifact(&artifact.id, &artifact.phase, artifact.error.as_deref())?;
            Ok(CapturedContent {
                artifact: self.artifact_from(artifact.clone()),
                length,
                digest,
                executable: expected.executable,
            })
        })();
        if capture.is_err() {
            let current = optional_stat(&self.quarantine_fd, &artifact_name, &self.state)?;
            if current.as_ref().is_some_and(|stat| {
                stat.st_dev == output_identity.dev && stat.st_ino == output_identity.ino
            }) {
                let _ = unlinkat(
                    &self.quarantine_fd,
                    artifact_name.as_str(),
                    AtFlags::empty(),
                );
            }
            self.store.remove_artifact(&artifact.id)?;
        }
        capture
    }

    pub fn open_artifact(&self, id: &str) -> Result<std::fs::File> {
        let artifact = self
            .store
            .artifacts()?
            .into_iter()
            .find(|artifact| artifact.id == id)
            .ok_or_else(|| Error::invalid("unknown retained artifact"))?;
        let fd = openat(
            &self.quarantine_fd,
            artifact.name.as_str(),
            OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .map_err(|e| Error::errno("open retained artifact", &self.state, e))?;
        Ok(std::fs::File::from(fd))
    }

    /// Open one live local file without following symlinks and without blocking
    /// on a FIFO: used to export the current local side of a conflict.
    pub fn open_local_file(&self, path: &str) -> Result<std::fs::File> {
        validate_path(path)?;
        let (parent, name) = split_path(path)?;
        let parent_fd = self.open_relative_dir(parent)?;
        let fd = openat(
            &parent_fd,
            name,
            OFlags::RDONLY | OFlags::NONBLOCK | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .map_err(|e| Error::errno("open local file", &self.root.join(path), e))?;
        Ok(std::fs::File::from(fd))
    }

    /// Release one terminal artifact after the reconciliation that consumed it
    /// is durable. Active conflicts and unresolved work keep their evidence.
    pub fn release_artifact(&mut self, id: &str) -> Result<bool> {
        let Some(record) = self
            .store
            .artifacts()?
            .into_iter()
            .find(|artifact| artifact.id == id)
        else {
            return Ok(true);
        };
        if record.phase == "conflict" || self.artifact_referenced(&record, true)? {
            return Ok(false);
        }
        if !self.remove_artifact_payload(&record)? {
            return Ok(false);
        }
        self.store.remove_install_by_artifact(&record.name)?;
        self.store.remove_artifact(&record.id)?;
        Ok(true)
    }

    /// Release superseded local versions for one path after its accepted
    /// baseline is durable.
    pub fn release_superseded(&mut self, path: &str) -> Result<()> {
        let ids = self
            .store
            .artifacts()?
            .into_iter()
            .filter(|artifact| {
                artifact.origin == path
                    && matches!(
                        artifact.kind.as_str(),
                        "replaced-file" | "rollback-displaced"
                    )
            })
            .map(|artifact| artifact.id)
            .collect::<Vec<_>>();
        for id in ids {
            self.release_artifact(&id)?;
        }
        Ok(())
    }

    /// Verify artifacts created by the previous reconciliation and release
    /// them if no old writer changed their bytes. This one-pass grace period
    /// keeps late writes through a displaced inode observable.
    pub fn settle_fresh_artifacts(&mut self) -> Result<()> {
        let pending = self
            .store
            .artifacts()?
            .into_iter()
            .filter(|artifact| artifact.settle_pending)
            .collect::<Vec<_>>();
        for artifact in pending {
            if artifact.phase == "conflict" {
                continue;
            }
            if self.artifact_matches_record(&artifact)? {
                self.release_artifact(&artifact.id)?;
            } else {
                self.store.update_artifact(
                    &artifact.id,
                    "conflict",
                    Some("retained artifact changed after its reconciliation"),
                )?;
            }
        }
        self.prune_artifacts()
    }

    /// Apply the documented count and byte budget to release eligible
    /// artifacts oldest-first. Conflict material and referenced work are never
    /// candidates.
    pub fn prune_artifacts(&mut self) -> Result<()> {
        let artifacts = self.store.artifacts()?;
        let mut count = artifacts
            .iter()
            .filter(|artifact| artifact.phase != "conflict")
            .count();
        let mut bytes = artifacts
            .iter()
            .filter(|artifact| artifact.phase != "conflict")
            .map(|artifact| artifact.length)
            .sum::<u64>();
        for artifact in artifacts {
            if count <= ARTIFACT_BUDGET_COUNT && bytes <= ARTIFACT_BUDGET_BYTES {
                break;
            }
            if artifact.phase == "conflict"
                || self.artifact_referenced(&artifact, true)?
                || !self.remove_artifact_payload(&artifact)?
            {
                continue;
            }
            self.store.remove_install_by_artifact(&artifact.name)?;
            self.store.remove_artifact(&artifact.id)?;
            count = count.saturating_sub(1);
            bytes = bytes.saturating_sub(artifact.length);
        }
        Ok(())
    }

    fn artifact_referenced(
        &self,
        artifact: &ArtifactRecord,
        allow_terminal_install: bool,
    ) -> Result<bool> {
        if self
            .store
            .conflicts()?
            .iter()
            .any(|conflict| conflict.local_artifact.as_deref() == Some(artifact.id.as_str()))
        {
            return Ok(true);
        }
        if self.store.work()?.iter().any(|work| {
            work.state != "committed"
                && work.state != "superseded"
                && (work.source_artifact.as_deref() == Some(artifact.id.as_str())
                    || work.upload_artifact.as_deref() == Some(artifact.id.as_str()))
        }) {
            return Ok(true);
        }
        for install in self.store.installs()? {
            if install.artifact != artifact.name {
                continue;
            }
            if !(allow_terminal_install && install.phase == InstallPhase::Retained) {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn remove_artifact_payload(&self, artifact: &ArtifactRecord) -> Result<bool> {
        let Some(stat) = optional_stat(&self.quarantine_fd, &artifact.name, &self.state)? else {
            return Ok(true);
        };
        match FileType::from_raw_mode(stat.st_mode) {
            FileType::RegularFile => {
                unlinkat(
                    &self.quarantine_fd,
                    artifact.name.as_str(),
                    AtFlags::empty(),
                )
                .map_err(|error| {
                    Error::errno(
                        "release retained artifact",
                        &self.state.join("quarantine").join(&artifact.name),
                        error,
                    )
                })?;
            }
            FileType::Directory if artifact.phase == "retained" => {
                remove_artifact_directory(
                    &self.quarantine_fd,
                    &artifact.name,
                    &self.state.join("quarantine"),
                )?;
                unlinkat(
                    &self.quarantine_fd,
                    artifact.name.as_str(),
                    AtFlags::REMOVEDIR,
                )
                .map_err(|error| {
                    Error::errno(
                        "release retained directory artifact",
                        &self.state.join("quarantine").join(&artifact.name),
                        error,
                    )
                })?;
            }
            FileType::Directory => return Ok(false),
            _ => return Ok(false),
        }
        fsync(&self.quarantine_fd)
            .map_err(|error| Error::errno("persist artifact release", &self.state, error))?;
        Ok(true)
    }

    /// Retain an arbitrary byte stream as an immutable artifact so an explicit
    /// conflict resolution input survives a crash, export and a later path
    /// change.
    pub fn retain_bytes(&mut self, origin: &str, bytes: &[u8]) -> Result<String> {
        let id = uuid::Uuid::new_v4().to_string();
        let name = format!("resolve-{id}");
        let mut record = ArtifactRecord {
            id: id.clone(),
            origin: origin.to_owned(),
            name: name.clone(),
            kind: "upload-retained".to_owned(),
            reason: "explicit conflict resolution input retained for publication and export"
                .to_owned(),
            remote_revision: None,
            phase: "preparing".to_owned(),
            error: None,
            digest: Some(*blake3::hash(bytes).as_bytes()),
            length: bytes.len() as u64,
            stat: None,
            settle_pending: false,
        };
        self.store.insert_artifact(&record)?;
        let fd = openat(
            &self.quarantine_fd,
            name.as_str(),
            OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::RUSR | Mode::WUSR,
        )
        .map_err(|e| Error::errno("create retained resolution input", &self.state, e))?;
        let written = (|| -> Result<()> {
            let mut file = std::fs::File::from(fd);
            file.write_all(bytes).map_err(|e| {
                Error::io(
                    "write retained resolution input",
                    &self.state.join(&name),
                    e,
                )
            })?;
            file.sync_all().map_err(|e| {
                Error::io(
                    "persist retained resolution input",
                    &self.state.join(&name),
                    e,
                )
            })?;
            fsync(&self.quarantine_fd).map_err(|e| {
                Error::errno("persist retained resolution directory", &self.state, e)
            })?;
            let retained = fstat(&file)
                .map_err(|e| Error::errno("inspect retained resolution input", &self.state, e))?;
            self.store.update_artifact_stat(
                &record.id,
                bytes.len() as u64,
                artifact_stat(&retained),
            )?;
            record.stat = Some(artifact_stat(&retained));
            Ok(())
        })();
        match written {
            Ok(()) => {
                record.phase = "retained".to_owned();
                self.store
                    .update_artifact(&record.id, &record.phase, None)?;
                Ok(id)
            }
            Err(error) => {
                let _ = self.remove_artifact_payload(&record);
                self.store.remove_artifact(&record.id)?;
                Err(error)
            }
        }
    }

    /// Acknowledge a retained install artifact whose inode gained late writer
    /// bytes: re-baseline its expected digest to the current bytes so a
    /// deliberate keep-local resolution is no longer reported as a conflict.
    pub fn acknowledge_retained_artifact(&mut self, id: &str) -> Result<()> {
        let artifacts = self.store.artifacts()?;
        let artifact = artifacts
            .iter()
            .find(|artifact| artifact.id == id)
            .ok_or_else(|| Error::invalid("unknown retained artifact"))?;
        let installs = self.store.installs()?;
        if let Some(mut install) = installs
            .into_iter()
            .find(|install| install.artifact == artifact.name)
        {
            let (length, digest, _, _) =
                hash_file_at(&self.quarantine_fd, &artifact.name, &self.state)?;
            install.expected_digest = *digest.as_bytes();
            install.expected_length = length;
            install.phase = InstallPhase::Retained;
            install.error = None;
            self.store.retarget_install(&install)?;
        }
        self.store.update_artifact(id, "retained", None)?;
        Ok(())
    }

    /// Create one local directory with a no-replace mkdirat; the entry only
    /// becomes visible after a successful directory entry creation.
    pub(super) fn create_directory_local(&self, path: &str) -> Result<()> {
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
        fsync(&parent_fd)
            .map_err(|e| Error::errno("persist local directory", &self.root.join(parent), e))
    }

    /// Atomically install a complete file at an absent name. The file becomes
    /// visible only after a no-replace rename from retained state; a crash
    /// leaves only a complete hidden copy in state.
    pub(super) fn create_file_local(
        &mut self,
        path: &str,
        executable: bool,
        source: &mut (dyn Read + Send),
    ) -> Result<()> {
        validate_path(path)?;
        let (parent, name) = split_path(path)?;
        let name_check = yinyang_core::namespace::name_key(name)
            .map_err(|e| Error::invalid(format!("invalid local name {name:?}: {e}")))?;
        let parent_fd = self.open_relative_dir(parent)?;
        if optional_stat(&parent_fd, name, &self.root.join(path))?.is_some() {
            return Err(Error::conflict(format!(
                "{} already exists before remote creation could install",
                path
            )));
        }
        let id = uuid::Uuid::new_v4().to_string();
        let temporary = format!("create-{id}");
        let temp_fd = openat(
            &self.quarantine_fd,
            temporary.as_str(),
            OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::RUSR | Mode::WUSR,
        )
        .map_err(|e| Error::errno("create staged remote file", &self.state, e))?;
        let temp_identity = identity(&temp_fd, &self.state, "inspect staged remote file")?;
        let mut temp = std::fs::File::from(temp_fd);
        let copied = std::io::copy(source, &mut temp)
            .map(|_| ())
            .map_err(|e| Error::io("stage remote file", &self.root.join(path), e));
        let synced = copied.and_then(|_| {
            temp.sync_all()
                .map_err(|e| Error::io("persist staged remote file", &self.state, e))
        });
        let mode = if executable {
            Mode::RUSR | Mode::WUSR | Mode::XUSR
        } else {
            Mode::RUSR | Mode::WUSR
        };
        let mode = synced.and_then(|_| {
            fchmod(&temp, mode).map_err(|e| Error::errno("set staged remote mode", &self.state, e))
        });
        mode?;
        drop(temp);
        if let Err(error) = renameat_with(
            &self.quarantine_fd,
            temporary.as_str(),
            &parent_fd,
            name,
            RenameFlags::NOREPLACE,
        ) {
            let _ = unlink_temporary_if_owned(
                &self.quarantine_fd,
                &temporary,
                temp_identity,
                &self.state,
            );
            return Err(Error::errno(
                "install staged remote file",
                &self.root.join(path),
                error,
            ));
        }
        fsync(&parent_fd).map_err(|e| {
            Error::errno("persist remote file creation", &self.root.join(parent), e)
        })?;
        fsync(&self.quarantine_fd)
            .map_err(|e| Error::errno("persist staged file removal", &self.state, e))?;
        let _ = name_check;
        Ok(())
    }

    /// Atomically move one local entry to a new name. The move is no-replace
    /// and uses fds that never follow symlinks.
    pub(super) fn rename_local(&self, source: &str, destination: &str) -> Result<()> {
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
        .map_err(|e| {
            Error::errno(
                "rename local entry without replacement",
                &self.root.join(source),
                e,
            )
        })?;
        fsync(&source_fd)
            .map_err(|e| Error::errno("persist local rename", &self.root.join(source_parent), e))?;
        fsync(&destination_fd).map_err(|e| {
            Error::errno(
                "persist local rename destination",
                &self.root.join(destination_parent),
                e,
            )
        })
    }

    pub fn prepare_install(
        &mut self,
        path: &str,
        expected: &LocalNode,
        executable: bool,
        remote_revision: Option<[u8; 24]>,
    ) -> Result<PreparedInstall<'_>> {
        let LocalKind::File { length, digest } = expected.kind else {
            return Err(Error::invalid("install target must be a regular file"));
        };
        if expected.path != path {
            return Err(Error::invalid(
                "install target path does not match observation",
            ));
        }
        let (parent, name) = split_path(path)?;
        let parent_fd = self.open_relative_dir(parent)?;
        let parent_identity = identity(&parent_fd, &self.root, "inspect install parent")?;
        if let Some(expected_parent) = expected.parent
            && parent_identity != expected_parent
        {
            return Err(Error::conflict("install parent identity changed"));
        }
        let id = uuid::Uuid::new_v4().to_string();
        let temporary = format!(".yy-install-{id}");
        let artifact_name = format!("install-{id}");
        self.store.insert_artifact(&ArtifactRecord {
            id: id.clone(),
            origin: path.to_owned(),
            name: artifact_name.clone(),
            kind: ArtifactKind::ReplacedFile.as_str().to_owned(),
            reason: "superseded local file inode".to_owned(),
            remote_revision,
            phase: "preparing".to_owned(),
            error: None,
            digest: Some(*digest.as_bytes()),
            length,
            stat: None,
            settle_pending: false,
        })?;
        let mut record = InstallRecord {
            id,
            path: path.to_owned(),
            parent: parent.to_owned(),
            name: name.to_owned(),
            temporary: temporary.clone(),
            temporary_dev: None,
            temporary_ino: None,
            expected_digest: *digest.as_bytes(),
            expected_length: length,
            expected_dev: expected.identity.dev,
            expected_ino: expected.identity.ino,
            parent_dev: parent_identity.dev,
            parent_ino: parent_identity.ino,
            replacement_digest: None,
            replacement_length: None,
            remote_revision,
            artifact: artifact_name,
            phase: InstallPhase::Creating,
            error: None,
        };
        self.store.insert_install(&record)?;
        let fd = match openat(
            &parent_fd,
            temporary.as_str(),
            OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::RUSR | Mode::WUSR,
        ) {
            Ok(fd) => fd,
            Err(error) => {
                record.phase = InstallPhase::Failed;
                record.error = Some(error.to_string());
                self.store.update_install(&record)?;
                return Err(Error::errno(
                    "create replacement file",
                    &self.root.join(&record.parent).join(&record.name),
                    error,
                ));
            }
        };
        let temp_stat =
            fstat(&fd).map_err(|e| Error::errno("inspect replacement file", &self.root, e))?;
        record.temporary_dev = Some(temp_stat.st_dev);
        record.temporary_ino = Some(temp_stat.st_ino);
        self.store.update_install(&record)?;
        Ok(PreparedInstall {
            replica: self,
            parent_fd,
            file: Some(std::fs::File::from(fd)),
            hasher: blake3::Hasher::new(),
            record,
            executable,
        })
    }

    /// Atomically move the top-level path into retained state. The subtree is
    /// never recursively deleted. A child created after the scan is therefore
    /// still present in the artifact and turns the deletion into a conflict.
    pub fn quarantine(
        &mut self,
        path: &str,
        expected: &LocalSnapshot,
        reason: &str,
        remote_revision: Option<[u8; 24]>,
    ) -> Result<Artifact> {
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
        let parent_identity = identity(&parent_fd, &self.root, "inspect quarantine parent")?;
        if let Some(expected_parent) = expected_node.parent
            && parent_identity != expected_parent
        {
            return Err(Error::conflict("quarantine parent identity changed"));
        }
        let current = statat(&parent_fd, name, AtFlags::SYMLINK_NOFOLLOW)
            .map_err(|e| Error::errno("inspect quarantine target", &self.root.join(path), e))?;
        let kind = match (FileType::from_raw_mode(current.st_mode), expected_node.kind) {
            (FileType::RegularFile, LocalKind::File { .. }) => ArtifactKind::QuarantinedFile,
            (FileType::Directory, LocalKind::Directory) => ArtifactKind::QuarantinedDirectory,
            (FileType::Symlink, _) => {
                return Err(Error::conflict(
                    "quarantine target became a symlink; it was not followed",
                ));
            }
            (FileType::Directory, LocalKind::File { .. })
            | (FileType::RegularFile, LocalKind::Directory) => {
                return Err(Error::conflict("quarantine target changed kind"));
            }
            _ => {
                return Err(Error::conflict("quarantine target became a special file"));
            }
        };
        if current.st_dev != expected_node.identity.dev
            || current.st_ino != expected_node.identity.ino
        {
            return Err(Error::conflict(
                "quarantine target identity changed after observation",
            ));
        }
        let id = uuid::Uuid::new_v4().to_string();
        let artifact_name = format!("quarantine-{id}");
        let mut record = ArtifactRecord {
            id: id.clone(),
            origin: path.to_owned(),
            name: artifact_name.clone(),
            kind: kind.as_str().to_owned(),
            reason: reason.to_owned(),
            remote_revision,
            phase: "prepared".to_owned(),
            error: None,
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
        };
        self.store.insert_artifact(&record)?;
        if let Err(error) = renameat_with(
            &parent_fd,
            name,
            &self.quarantine_fd,
            artifact_name.as_str(),
            RenameFlags::NOREPLACE,
        ) {
            self.store.remove_artifact(&record.id)?;
            return Err(Error::errno(
                "quarantine local path",
                &self.root.join(path),
                error,
            ));
        }
        fsync(&parent_fd)
            .map_err(|e| Error::errno("persist quarantine source directory", &self.root, e))?;
        fsync(&self.quarantine_fd)
            .map_err(|e| Error::errno("persist quarantine directory", &self.state, e))?;
        record.phase = "moved".to_owned();
        self.store
            .update_artifact(&record.id, &record.phase, record.error.as_deref())?;
        let actual = self.scan_artifact(&artifact_name)?;
        let matches = same_tree(&expected.subtree(path), &actual);
        if let Some(stat) = optional_stat(&self.quarantine_fd, &artifact_name, &self.state)? {
            record.length = actual
                .values()
                .map(|node| match node.kind {
                    LocalKind::File { length, .. } => length,
                    LocalKind::Directory => 0,
                })
                .sum();
            record.stat = Some(artifact_stat(&stat));
            self.store
                .update_artifact_stat(&record.id, record.length, artifact_stat(&stat))?;
        }
        record.phase = if matches { "retained" } else { "conflict" }.to_owned();
        if !matches {
            record.error = Some(
                "local subtree changed after observation; retained quarantine contains all bytes"
                    .to_owned(),
            );
        }
        self.store
            .update_artifact(&record.id, &record.phase, record.error.as_deref())?;
        Ok(self.artifact_from(record))
    }

    pub fn export_artifact(&self, id: &str, output: &mut (impl Write + ?Sized)) -> Result<()> {
        let artifact = self
            .store
            .artifacts()?
            .into_iter()
            .find(|artifact| artifact.id == id)
            .ok_or_else(|| Error::invalid("unknown retained artifact"))?;
        let stat = statat(
            &self.quarantine_fd,
            artifact.name.as_str(),
            AtFlags::SYMLINK_NOFOLLOW,
        )
        .map_err(|e| Error::errno("open retained artifact", &self.state, e))?;
        if !matches!(FileType::from_raw_mode(stat.st_mode), FileType::RegularFile) {
            return Err(Error::unsupported(
                "only file artifacts can be exported as one byte stream; directory artifacts remain at their reported state path",
            ));
        }
        let fd = openat(
            &self.quarantine_fd,
            artifact.name.as_str(),
            OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .map_err(|e| Error::errno("open retained artifact", &self.state, e))?;
        let mut file = std::fs::File::from(fd);
        std::io::copy(&mut file, output)
            .map_err(|e| Error::io("export retained artifact", &self.state, e))?;
        Ok(())
    }

    pub fn artifact_path(&self, id: &str) -> Result<PathBuf> {
        let artifact = self
            .store
            .artifacts()?
            .into_iter()
            .find(|artifact| artifact.id == id)
            .ok_or_else(|| Error::invalid("unknown retained artifact"))?;
        Ok(self.state.join("quarantine").join(artifact.name))
    }

    /// Canonical domain evidence for a retained artifact: physical path and
    /// whether one byte stream can represent it.
    fn artifact_from(&self, record: ArtifactRecord) -> Artifact {
        let path = self.state.join("quarantine").join(&record.name);
        let exportable = !matches!(record.kind.as_str(), "quarantined-directory");
        Artifact {
            id: record.id,
            origin: record.origin,
            name: record.name,
            kind: record.kind,
            reason: record.reason,
            phase: record.phase,
            error: record.error,
            path,
            exportable,
            length: record.length,
        }
    }

    /// Read-only classification of a retained install inode: whether its
    /// current bytes differ from the digest recorded when it was displaced.
    /// This is the same evidence inspection uses, so a synthesized conflict can
    /// be loaded later without a prior mutating status write.
    pub(crate) fn retained_artifact_changed(&self, id: &str) -> Result<bool> {
        let artifacts = self.store.artifacts()?;
        let Some(artifact) = artifacts.iter().find(|artifact| artifact.id == id) else {
            return Ok(false);
        };
        let installs = self.store.installs()?;
        let Some(install) = installs
            .iter()
            .find(|install| install.artifact == artifact.name)
        else {
            return Ok(false);
        };
        Ok(!self.artifact_matches_expected(install)?)
    }

    fn scan_directory(
        &self,
        directory: &OwnedFd,
        prefix: &str,
        directory_identity: Identity,
        nodes: &mut BTreeMap<String, LocalNode>,
        hints: Option<&BTreeMap<String, LocalScanHint>>,
    ) -> Result<()> {
        scan_directory(
            directory,
            prefix,
            directory_identity,
            &self.root,
            self.root_identity.dev,
            Some(&self.hash_bytes),
            hints,
            nodes,
        )
    }

    pub(super) fn scan_artifact(&self, name: &str) -> Result<BTreeMap<String, LocalNode>> {
        let stat = statat(&self.quarantine_fd, name, AtFlags::SYMLINK_NOFOLLOW)
            .map_err(|e| Error::errno("inspect retained artifact", &self.state, e))?;
        let mut nodes = BTreeMap::new();
        match FileType::from_raw_mode(stat.st_mode) {
            FileType::Directory => {
                let fd = openat(
                    &self.quarantine_fd,
                    name,
                    OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                    Mode::empty(),
                )
                .map_err(|e| Error::errno("open retained directory", &self.state, e))?;
                let identity = identity(&fd, &self.state, "inspect retained directory")?;
                nodes.insert(
                    String::new(),
                    LocalNode {
                        path: String::new(),
                        kind: LocalKind::Directory,
                        executable: false,
                        identity,
                        parent: None,
                        stat: Some(local_stat(&stat)),
                    },
                );
                scan_directory(
                    &fd,
                    "",
                    identity,
                    &self.state,
                    self.state_identity.dev,
                    Some(&self.hash_bytes),
                    None,
                    &mut nodes,
                )?;
            }
            FileType::RegularFile => {
                let (length, digest, identity, executable) =
                    hash_file_at(&self.quarantine_fd, name, &self.state.join(name))?;
                nodes.insert(
                    String::new(),
                    LocalNode {
                        path: String::new(),
                        kind: LocalKind::File { length, digest },
                        executable,
                        identity,
                        parent: None,
                        stat: Some(local_stat(&stat)),
                    },
                );
            }
            _ => {
                return Err(Error::unsupported(
                    "retained artifact is a symlink or special file",
                ));
            }
        }
        Ok(nodes)
    }

    fn open_relative_dir(&self, relative: &str) -> Result<OwnedFd> {
        if !relative.is_empty() {
            validate_path(relative)?;
        }
        let mut fd = openat(
            &self.root_fd,
            ".",
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .map_err(|e| Error::errno("duplicate local root", &self.root, e))?;
        if relative.is_empty() {
            return Ok(fd);
        }
        for component in relative.split('/') {
            let next = openat(
                &fd,
                component,
                OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                Mode::empty(),
            )
            .map_err(|e| {
                Error::errno(
                    "open local parent without following symlinks",
                    &self.root.join(relative),
                    e,
                )
            })?;
            let stat = fstat(&next)
                .map_err(|e| Error::errno("inspect local parent", &self.root.join(relative), e))?;
            if FileType::from_raw_mode(stat.st_mode) != FileType::Directory {
                return Err(Error::invalid("local parent is not a directory"));
            }
            if stat.st_dev != self.root_identity.dev {
                return Err(Error::unsupported(
                    "local path crosses a filesystem boundary",
                ));
            }
            fd = next;
        }
        Ok(fd)
    }

    fn open_relative_dir_optional(&self, relative: &str) -> Result<Option<OwnedFd>> {
        if relative.is_empty() {
            return self
                .root_fd
                .try_clone()
                .map(Some)
                .map_err(|error| Error::io("clone local root descriptor", &self.root, error));
        }
        validate_path(relative)?;
        let mut fd = self
            .root_fd
            .try_clone()
            .map_err(|error| Error::io("clone local root descriptor", &self.root, error))?;
        for component in relative.split('/') {
            fd = match openat(
                &fd,
                component,
                OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                Mode::empty(),
            ) {
                Ok(next) => next,
                Err(rustix::io::Errno::NOENT) => return Ok(None),
                Err(error) => {
                    return Err(Error::errno(
                        "open local parent without following symlinks",
                        &self.root.join(relative),
                        error,
                    ));
                }
            };
            let stat = fstat(&fd).map_err(|error| {
                Error::errno("inspect local parent", &self.root.join(relative), error)
            })?;
            if stat.st_dev != self.root_identity.dev {
                return Err(Error::unsupported("local path crosses a volume boundary"));
            }
        }
        Ok(Some(fd))
    }

    fn open_journal_parent(&self, install: &InstallRecord) -> Result<OwnedFd> {
        if let Ok(parent) = self.open_relative_dir(&install.parent)
            && let Ok(current) = identity(&parent, &self.root, "inspect install parent")
            && current.dev == install.parent_dev
            && current.ino == install.parent_ino
        {
            return Ok(parent);
        }
        find_directory(
            &self.root_fd,
            self.root_identity.dev,
            Identity {
                dev: install.parent_dev,
                ino: install.parent_ino,
            },
            &self.root,
        )?
        .ok_or_else(|| {
            Error::conflict(format!(
                "cannot locate the journaled install parent {}/{} without following symlinks",
                install.parent_dev, install.parent_ino
            ))
        })
    }

    fn recover_incomplete(&mut self) -> Result<()> {
        for mut install in self.store.incomplete_installs()? {
            match install.phase {
                InstallPhase::Creating => {
                    self.recover_creating(&mut install)?;
                }
                InstallPhase::Prepared => {
                    self.recover_prepared(&mut install)?;
                }
                InstallPhase::Exchanged => {
                    self.retain_exchanged(&mut install)?;
                }
                InstallPhase::Retained | InstallPhase::Conflict | InstallPhase::Failed => {}
            }
        }
        Ok(())
    }

    fn recover_creating(&mut self, install: &mut InstallRecord) -> Result<()> {
        let parent = self.open_journal_parent(install)?;
        let Some(stat) = optional_stat(&parent, &install.temporary, &self.root)? else {
            install.phase = InstallPhase::Failed;
            install.error = Some("interrupted before replacement bytes were prepared".to_owned());
            self.store.update_install(install)?;
            self.store
                .update_artifact(&install.id, "failed", install.error.as_deref())?;
            return Ok(());
        };
        let owned = install.temporary_dev == Some(stat.st_dev)
            && install.temporary_ino == Some(stat.st_ino)
            && FileType::from_raw_mode(stat.st_mode) == FileType::RegularFile;
        if !owned {
            return self.retain_uncertain_temporary(
                install,
                "interrupted temporary identity cannot be proven; retained instead of deleting",
            );
        }
        unlinkat(&parent, install.temporary.as_str(), AtFlags::empty()).map_err(|e| {
            Error::errno(
                "remove interrupted install temporary",
                &self.root.join(&install.path),
                e,
            )
        })?;
        fsync(&parent)
            .map_err(|e| Error::errno("persist interrupted install cleanup", &self.root, e))?;
        install.phase = InstallPhase::Failed;
        install.error = Some("interrupted before replacement bytes were prepared".to_owned());
        self.store.update_install(install)?;
        self.store
            .update_artifact(&install.id, "failed", install.error.as_deref())?;
        Ok(())
    }

    fn recover_prepared(&mut self, install: &mut InstallRecord) -> Result<()> {
        let parent = self.open_journal_parent(install)?;
        let artifact = optional_stat(&self.quarantine_fd, &install.artifact, &self.state)?;
        let target = optional_stat(&parent, &install.name, &self.root.join(&install.path))?;
        let temporary = optional_stat(&parent, &install.temporary, &self.root.join(&install.path))?;

        if artifact.is_some() {
            let matches = self.artifact_matches_expected(install)?;
            if temporary.is_some() {
                install.phase = InstallPhase::Conflict;
                install.error = Some(
                    "retained artifact and an unexplained temporary both exist after restart"
                        .to_owned(),
                );
                self.store.update_install(install)?;
                self.store
                    .update_artifact(&install.id, "conflict", install.error.as_deref())?;
                return Ok(());
            }
            install.phase = if matches {
                InstallPhase::Retained
            } else {
                InstallPhase::Conflict
            };
            install.error = (!matches)
                .then(|| "retained artifact differs from the scanned local version".to_owned());
            self.store.update_install(install)?;
            self.store.update_artifact(
                &install.id,
                install.phase.as_str(),
                install.error.as_deref(),
            )?;
            return Ok(());
        }

        let Some(temporary) = temporary else {
            install.phase = InstallPhase::Conflict;
            install.error =
                Some("prepared replacement is absent without a retained artifact".to_owned());
            self.store.update_install(install)?;
            self.store
                .update_artifact(&install.id, "conflict", install.error.as_deref())?;
            return Ok(());
        };
        let exchange_completed = target.as_ref().is_some_and(|target| {
            install.temporary_dev == Some(target.st_dev)
                && install.temporary_ino == Some(target.st_ino)
        });
        if exchange_completed {
            self.retain_exchanged(install)?;
            return Ok(());
        }

        let temporary_is_ours = install.temporary_dev == Some(temporary.st_dev)
            && install.temporary_ino == Some(temporary.st_ino)
            && FileType::from_raw_mode(temporary.st_mode) == FileType::RegularFile
            && self.temporary_matches_replacement(install)?;
        if temporary_is_ours {
            let target_unchanged = target.as_ref().is_some_and(|target| {
                target.st_dev == install.expected_dev
                    && target.st_ino == install.expected_ino
                    && FileType::from_raw_mode(target.st_mode) == FileType::RegularFile
            });
            if target_unchanged {
                // EXCHANGE did not execute. Keep the exact prepared replacement
                // for an explicit retry; scanning rejects its reserved name.
                return Ok(());
            }
            unlinkat(&parent, install.temporary.as_str(), AtFlags::empty()).map_err(|e| {
                Error::errno(
                    "remove unexposed replacement after target change",
                    &self.root.join(&install.path),
                    e,
                )
            })?;
            fsync(&parent)
                .map_err(|e| Error::errno("persist replacement cleanup", &self.root, e))?;
            install.phase = InstallPhase::Conflict;
            install.error = Some(
                "install target changed before exchange; owned unexposed replacement was removed"
                    .to_owned(),
            );
            self.store.update_install(install)?;
            self.store
                .update_artifact(&install.id, "conflict", install.error.as_deref())?;
            return Ok(());
        }

        self.retain_uncertain_temporary(
            install,
            "prepared temporary is no longer the recorded unexposed inode; retained",
        )
    }

    fn temporary_matches_replacement(&self, install: &InstallRecord) -> Result<bool> {
        let (Some(expected_digest), Some(expected_length)) =
            (install.replacement_digest, install.replacement_length)
        else {
            return Ok(false);
        };
        let parent = self.open_journal_parent(install)?;
        let (length, digest, _, _) =
            hash_file_at(&parent, &install.temporary, &self.root.join(&install.path))?;
        Ok(length == expected_length && digest.as_bytes() == &expected_digest)
    }

    fn retain_uncertain_temporary(
        &mut self,
        install: &mut InstallRecord,
        reason: &str,
    ) -> Result<()> {
        self.retain_exchanged(install)?;
        if install.phase == InstallPhase::Retained {
            install.phase = InstallPhase::Conflict;
            install.error = Some(reason.to_owned());
            self.store.update_install(install)?;
            self.store
                .update_artifact(&install.id, "conflict", install.error.as_deref())?;
        }
        Ok(())
    }

    fn retain_exchanged(&mut self, install: &mut InstallRecord) -> Result<()> {
        let parent = self.open_journal_parent(install)?;
        let temp_exists = statat(
            &parent,
            install.temporary.as_str(),
            AtFlags::SYMLINK_NOFOLLOW,
        )
        .is_ok();
        let artifact_exists = statat(
            &self.quarantine_fd,
            install.artifact.as_str(),
            AtFlags::SYMLINK_NOFOLLOW,
        )
        .is_ok();
        if temp_exists {
            renameat_with(
                &parent,
                install.temporary.as_str(),
                &self.quarantine_fd,
                install.artifact.as_str(),
                RenameFlags::NOREPLACE,
            )
            .map_err(|e| {
                Error::errno(
                    "retain displaced inode after restart",
                    &self.root.join(&install.path),
                    e,
                )
            })?;
            fsync(&parent)
                .map_err(|e| Error::errno("persist displaced inode move", &self.root, e))?;
            fsync(&self.quarantine_fd)
                .map_err(|e| Error::errno("persist retained inode", &self.state, e))?;
        } else if !artifact_exists {
            install.phase = InstallPhase::Conflict;
            install.error = Some(
                "exchange state cannot be recovered: both temporary and retained artifact are absent"
                    .to_owned(),
            );
            self.store.update_install(install)?;
            self.store
                .update_artifact(&install.id, "conflict", install.error.as_deref())?;
            return Ok(());
        }
        let matches = self.artifact_matches_expected(install)?;
        install.phase = if matches {
            InstallPhase::Retained
        } else {
            InstallPhase::Conflict
        };
        install.error = if matches {
            None
        } else {
            Some(
                "displaced inode changed from its scanned version; retained without deletion"
                    .to_owned(),
            )
        };
        self.store.update_install(install)?;
        self.store.update_artifact(
            &install.id,
            install.phase.as_str(),
            install.error.as_deref(),
        )?;
        Ok(())
    }

    fn artifact_matches_expected(&self, install: &InstallRecord) -> Result<bool> {
        let stat = statat(
            &self.quarantine_fd,
            install.artifact.as_str(),
            AtFlags::SYMLINK_NOFOLLOW,
        )
        .map_err(|e| Error::errno("inspect retained displaced inode", &self.state, e))?;
        if !matches!(FileType::from_raw_mode(stat.st_mode), FileType::RegularFile) {
            return Ok(false);
        }
        if let Some(record) = self
            .store
            .artifacts()?
            .into_iter()
            .find(|record| record.name == install.artifact)
            && artifact_stat_matches(&record, &stat)
        {
            return Ok(true);
        }
        let (length, digest, _, _) =
            hash_file_at(&self.quarantine_fd, &install.artifact, &self.state)?;
        Ok(length == install.expected_length && digest.as_bytes() == &install.expected_digest)
    }

    fn refresh_artifact_stat(&mut self, install: &InstallRecord) -> Result<()> {
        let Some(stat) = optional_stat(&self.quarantine_fd, &install.artifact, &self.state)? else {
            return Ok(());
        };
        let Some(record) = self
            .store
            .artifacts()?
            .into_iter()
            .find(|record| record.name == install.artifact)
        else {
            return Ok(());
        };
        if artifact_stat_matches(&record, &stat) {
            return Ok(());
        }
        let length = if FileType::from_raw_mode(stat.st_mode) == FileType::RegularFile {
            stat.st_size as u64
        } else {
            record.length
        };
        self.store
            .update_artifact_stat(&record.id, length, artifact_stat(&stat))
    }

    fn artifact_matches_record(&self, artifact: &ArtifactRecord) -> Result<bool> {
        let Some(stat) = optional_stat(&self.quarantine_fd, &artifact.name, &self.state)? else {
            return Ok(false);
        };
        if artifact_stat_matches(artifact, &stat) {
            return Ok(true);
        }
        if FileType::from_raw_mode(stat.st_mode) != FileType::RegularFile {
            return Ok(false);
        }
        let Some(expected) = artifact.digest else {
            return Ok(false);
        };
        let (length, digest, _, _) =
            hash_file_at(&self.quarantine_fd, &artifact.name, &self.state)?;
        Ok(length == artifact.length && digest.as_bytes() == &expected)
    }
}

pub struct PreparedInstall<'a> {
    replica: &'a mut Replica,
    parent_fd: OwnedFd,
    file: Option<std::fs::File>,
    hasher: blake3::Hasher,
    record: InstallRecord,
    executable: bool,
}

impl PreparedInstall<'_> {
    pub fn write_all(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        self.hasher.update(bytes);
        self.file
            .as_mut()
            .expect("prepared install is live")
            .write_all(bytes)
    }

    pub fn finish(self) -> Result<InstallOutcome> {
        self.finish_with_hook(|_| {})
    }

    fn finish_with_hook<F>(mut self, after_exchange: F) -> Result<InstallOutcome>
    where
        F: FnOnce(&Self),
    {
        let file = self.file.take().expect("prepared install is live");
        file.sync_all().map_err(|e| {
            Error::io(
                "persist replacement file",
                &self.replica.root.join(&self.record.path),
                e,
            )
        })?;
        let mode = if self.executable {
            Mode::RUSR | Mode::WUSR | Mode::XUSR
        } else {
            Mode::RUSR | Mode::WUSR
        };
        fchmod(&file, mode).map_err(|e| {
            Error::errno(
                "set replacement mode",
                &self.replica.root.join(&self.record.path),
                e,
            )
        })?;
        let temp_stat = fstat(&file)
            .map_err(|e| Error::errno("inspect replacement file", &self.replica.root, e))?;
        if FileType::from_raw_mode(temp_stat.st_mode) != FileType::RegularFile {
            return Err(Error::invalid(
                "replacement temporary is not a regular file",
            ));
        }
        let replacement_digest = *self.hasher.finalize().as_bytes();
        self.record.replacement_digest = Some(replacement_digest);
        self.record.replacement_length = Some(temp_stat.st_size as u64);
        self.record.temporary_dev = Some(temp_stat.st_dev);
        self.record.temporary_ino = Some(temp_stat.st_ino);
        self.record.phase = InstallPhase::Prepared;
        self.replica.store.update_install(&self.record)?;
        fsync(&self.parent_fd).map_err(|e| {
            Error::errno(
                "persist prepared replacement",
                &self.replica.root.join(&self.record.parent),
                e,
            )
        })?;
        drop(file);

        if let Err(error) = self.require_parent_unchanged() {
            self.abort_replacement(&error.to_string(), None)?;
            return Err(error);
        }
        let target = match statat(
            &self.parent_fd,
            self.record.name.as_str(),
            AtFlags::SYMLINK_NOFOLLOW,
        ) {
            Ok(stat) => stat,
            Err(error) => {
                self.abort_replacement("install target disappeared before exchange", Some(error))?;
                return Err(Error::conflict(
                    "install target disappeared before exchange",
                ));
            }
        };
        if FileType::from_raw_mode(target.st_mode) != FileType::RegularFile
            || target.st_dev != self.record.expected_dev
            || target.st_ino != self.record.expected_ino
        {
            self.abort_replacement("install target identity changed before exchange", None)?;
            return Err(Error::conflict(
                "install target changed before exchange; replacement was discarded",
            ));
        }
        renameat_with(
            &self.parent_fd,
            self.record.temporary.as_str(),
            &self.parent_fd,
            self.record.name.as_str(),
            RenameFlags::EXCHANGE,
        )
        .map_err(|error| {
            let message = if matches!(error, rustix::io::Errno::INVAL | rustix::io::Errno::NOSYS) {
                Error::unsupported("atomic RENAME_EXCHANGE is unavailable on the local filesystem")
            } else {
                Error::errno(
                    "atomically exchange replacement",
                    &self.replica.root.join(&self.record.path),
                    error,
                )
            };
            self.record.phase = InstallPhase::Failed;
            self.record.error = Some(message.to_string());
            let _ = self.replica.store.update_install(&self.record);
            message
        })?;
        fsync(&self.parent_fd).map_err(|e| {
            Error::errno(
                "persist file exchange",
                &self.replica.root.join(&self.record.parent),
                e,
            )
        })?;
        self.record.phase = InstallPhase::Exchanged;
        self.replica.store.update_install(&self.record)?;
        after_exchange(&self);

        if let Err(error) = self.require_parent_unchanged() {
            self.retain_displaced(
                "install parent changed after exchange",
                InstallPhase::Conflict,
            )?;
            return Err(error);
        }
        let displaced_matches = self.displaced_matches_expected().unwrap_or_default();
        if !displaced_matches {
            return self.recover_concurrent_change();
        }
        self.retain_displaced(
            "local file inode replaced by remote materialization",
            InstallPhase::Retained,
        )?;
        Ok(InstallOutcome {
            artifact: self.artifact()?,
        })
    }

    fn require_parent_unchanged(&self) -> Result<()> {
        let directory = self
            .replica
            .open_relative_dir(&self.record.parent)
            .map_err(|error| {
                Error::conflict(format!(
                    "install parent is no longer reachable without following symlinks: {error}"
                ))
            })?;
        let current = identity(
            &directory,
            &self.replica.root,
            "inspect install parent after exchange",
        )?;
        if current.dev != self.record.parent_dev || current.ino != self.record.parent_ino {
            return Err(Error::conflict(
                "install parent path no longer identifies the directory used for exchange",
            ));
        }
        Ok(())
    }

    fn displaced_matches_expected(&self) -> Result<bool> {
        let stat = statat(
            &self.parent_fd,
            self.record.temporary.as_str(),
            AtFlags::SYMLINK_NOFOLLOW,
        )
        .map_err(|e| {
            Error::errno(
                "inspect displaced inode",
                &self.replica.root.join(&self.record.path),
                e,
            )
        })?;
        if FileType::from_raw_mode(stat.st_mode) != FileType::RegularFile {
            return Ok(false);
        }
        let path = self.replica.root.join(&self.record.path);
        let (length, digest, identity, _) =
            hash_file_at(&self.parent_fd, &self.record.temporary, &path)?;
        Ok(identity.dev == self.record.expected_dev
            && identity.ino == self.record.expected_ino
            && length == self.record.expected_length
            && digest.as_bytes() == &self.record.expected_digest)
    }

    fn recover_concurrent_change(mut self) -> Result<InstallOutcome> {
        match renameat_with(
            &self.parent_fd,
            self.record.temporary.as_str(),
            &self.parent_fd,
            self.record.name.as_str(),
            RenameFlags::EXCHANGE,
        ) {
            Ok(()) => {
                fsync(&self.parent_fd).map_err(|e| {
                    Error::errno(
                        "persist concurrent-change recovery",
                        &self.replica.root.join(&self.record.parent),
                        e,
                    )
                })?;
                let rollback = self.retain_rollback(
                    "target displaced by the rollback exchange; retained for recovery",
                )?;
                self.record.phase = InstallPhase::Conflict;
                self.record.error = Some(format!(
                    "local target changed concurrently; original local inode was restored and rollback-displaced bytes were retained as artifact {}",
                    rollback.id
                ));
                self.replica.store.update_install(&self.record)?;
                self.replica.store.update_artifact(
                    &self.record.id,
                    "conflict",
                    self.record.error.as_deref(),
                )?;
                Err(Error::conflict(
                    "local target changed concurrently; original local inode was restored",
                ))
            }
            Err(error) => {
                self.record.error = Some(format!(
                    "local target changed and restoring it failed: {error}; displaced inode remains retained"
                ));
                self.record.phase = InstallPhase::Conflict;
                self.replica.store.update_install(&self.record)?;
                self.retain_displaced(
                    "local target changed and the atomic rollback exchange failed",
                    InstallPhase::Conflict,
                )?;
                Err(Error::conflict(self.record.error.clone().unwrap_or_else(
                    || "local target changed concurrently".to_owned(),
                )))
            }
        }
    }

    fn retain_rollback(&mut self, reason: &str) -> Result<Artifact> {
        let id = uuid::Uuid::new_v4().to_string();
        let name = format!("rollback-{id}");
        let mut record = ArtifactRecord {
            id: id.clone(),
            origin: self.record.path.clone(),
            name: name.clone(),
            kind: ArtifactKind::RollbackDisplaced.as_str().to_owned(),
            reason: reason.to_owned(),
            remote_revision: self.record.remote_revision,
            phase: "prepared".to_owned(),
            error: None,
            digest: None,
            length: 0,
            stat: None,
            settle_pending: false,
        };
        self.replica.store.insert_artifact(&record)?;
        renameat_with(
            &self.parent_fd,
            self.record.temporary.as_str(),
            &self.replica.quarantine_fd,
            name.as_str(),
            RenameFlags::NOREPLACE,
        )
        .map_err(|e| {
            Error::errno(
                "retain rollback-displaced inode",
                &self.replica.root.join(&self.record.path),
                e,
            )
        })?;
        fsync(&self.parent_fd).map_err(|e| {
            Error::errno(
                "persist rollback-displaced inode removal from payload",
                &self.replica.root.join(&self.record.parent),
                e,
            )
        })?;
        fsync(&self.replica.quarantine_fd).map_err(|e| {
            Error::errno("persist rollback-displaced inode", &self.replica.state, e)
        })?;
        if let Some(stat) = optional_stat(&self.replica.quarantine_fd, &name, &self.replica.state)?
        {
            record.length = stat.st_size as u64;
            record.stat = Some(artifact_stat(&stat));
            self.replica.store.update_artifact_stat(
                &record.id,
                record.length,
                artifact_stat(&stat),
            )?;
        }
        record.phase = "conflict".to_owned();
        record.error = Some(reason.to_owned());
        self.replica
            .store
            .update_artifact(&record.id, &record.phase, record.error.as_deref())?;
        Ok(self.replica.artifact_from(record))
    }

    fn retain_displaced(&mut self, reason: &str, phase: InstallPhase) -> Result<()> {
        renameat_with(
            &self.parent_fd,
            self.record.temporary.as_str(),
            &self.replica.quarantine_fd,
            self.record.artifact.as_str(),
            RenameFlags::NOREPLACE,
        )
        .map_err(|e| {
            Error::errno(
                "retain displaced inode",
                &self.replica.root.join(&self.record.path),
                e,
            )
        })?;
        fsync(&self.parent_fd).map_err(|e| {
            Error::errno(
                "persist displaced inode removal from payload",
                &self.replica.root.join(&self.record.parent),
                e,
            )
        })?;
        fsync(&self.replica.quarantine_fd).map_err(|e| {
            Error::errno("persist retained displaced inode", &self.replica.state, e)
        })?;
        if let Some(stat) = optional_stat(
            &self.replica.quarantine_fd,
            &self.record.artifact,
            &self.replica.state,
        )? {
            self.replica.store.update_artifact_stat(
                &self.record.id,
                stat.st_size as u64,
                artifact_stat(&stat),
            )?;
        }
        self.record.phase = phase.clone();
        self.record.error = if phase == InstallPhase::Conflict {
            Some(reason.to_owned())
        } else {
            None
        };
        self.replica.store.update_install(&self.record)?;
        self.replica.store.update_artifact(
            &self.record.id,
            if phase == InstallPhase::Conflict {
                "conflict"
            } else {
                "retained"
            },
            self.record.error.as_deref(),
        )?;
        Ok(())
    }

    fn abort_replacement(&mut self, message: &str, error: Option<rustix::io::Errno>) -> Result<()> {
        unlinkat(
            &self.parent_fd,
            self.record.temporary.as_str(),
            AtFlags::empty(),
        )
        .map_err(|e| {
            Error::errno(
                "remove unused replacement",
                &self.replica.root.join(&self.record.path),
                e,
            )
        })?;
        fsync(&self.parent_fd).map_err(|e| {
            Error::errno(
                "persist unused replacement removal",
                &self.replica.root.join(&self.record.parent),
                e,
            )
        })?;
        self.record.phase = InstallPhase::Conflict;
        self.record.error = Some(match error {
            Some(error) => format!("{message}: {error}"),
            None => message.to_owned(),
        });
        self.replica.store.update_install(&self.record)?;
        self.replica.store.update_artifact(
            &self.record.id,
            "conflict",
            self.record.error.as_deref(),
        )?;
        Ok(())
    }

    fn artifact(&self) -> Result<Artifact> {
        self.replica
            .store
            .artifacts()?
            .into_iter()
            .find(|artifact| artifact.id == self.record.id)
            .map(|artifact| self.replica.artifact_from(artifact))
            .ok_or_else(|| Error::corrupt("retained artifact record disappeared"))
    }
}

impl From<ConflictRecord> for Conflict {
    fn from(value: ConflictRecord) -> Self {
        Self {
            id: value.id,
            path: value.path,
            original_path: value.original_path,
            desired_path: value.desired_path,
            local: value.local,
            remote: value.remote,
            local_artifact: value.local_artifact,
            remote_node: value.remote_node.map(NodeId::from_bytes),
            remote_revision: value.remote_revision,
            error: value.error,
        }
    }
}

/// Scan one existing path without following symlinks or crossing a filesystem
/// boundary. The returned paths are relative to `path`, whose snapshot key is
/// the empty string.
pub fn scan_path(path: &Path) -> Result<LocalSnapshot> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| Error::unsupported("observation path requires a UTF-8 file name"))?;
    let (parent_fd, _) = open_bound_directory(parent, "open observation parent")?;
    let stat = statat(&parent_fd, name, AtFlags::SYMLINK_NOFOLLOW)
        .map_err(|error| Error::errno("inspect observation path", path, error))?;
    let root_identity = stat_identity(&stat);
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
            if opened.st_dev != root_identity.dev {
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
                    stat: Some(local_stat(&opened)),
                },
            );
            scan_directory(
                &fd,
                "",
                root_identity,
                path,
                root_identity.dev,
                None,
                None,
                &mut nodes,
            )?;
        }
        FileType::RegularFile => {
            let (length, digest, identity, executable) = hash_file_at(&parent_fd, name, path)?;
            nodes.insert(
                String::new(),
                LocalNode {
                    path: String::new(),
                    kind: LocalKind::File { length, digest },
                    executable,
                    identity,
                    parent: None,
                    stat: Some(local_stat(&stat)),
                },
            );
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

fn scan_directory(
    directory: &OwnedFd,
    prefix: &str,
    directory_identity: Identity,
    display_root: &Path,
    filesystem_dev: u64,
    counter: Option<&AtomicU64>,
    hints: Option<&BTreeMap<String, LocalScanHint>>,
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
        if name.starts_with(".yy-install-") || name.starts_with(".yy-rollback-") {
            return Err(Error::conflict(format!(
                "unrecovered internal installation entry is present: {}",
                join(prefix, name)
            )));
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
                let identity = stat_identity(&opened);
                nodes.insert(
                    path.clone(),
                    LocalNode {
                        path: path.clone(),
                        kind: LocalKind::Directory,
                        executable: false,
                        identity,
                        parent: Some(directory_identity),
                        stat: Some(local_stat(&opened)),
                    },
                );
                scan_directory(
                    &fd,
                    &path,
                    identity,
                    display_root,
                    filesystem_dev,
                    counter,
                    hints,
                    nodes,
                )?;
            }
            FileType::RegularFile => {
                if let Some(hint) = hints
                    .and_then(|hints| hints.get(&path))
                    .filter(|hint| scan_hint_matches(&stat, hint))
                {
                    let mut node = hint.node.clone();
                    node.path = path.clone();
                    node.parent = Some(directory_identity);
                    node.stat = Some(local_stat(&stat));
                    nodes.insert(path, node);
                    continue;
                }
                let (length, digest, identity, executable) =
                    hash_file_at_counted(directory, name, &display, counter)?;
                let node = LocalNode {
                    path: path.clone(),
                    kind: LocalKind::File { length, digest },
                    executable,
                    identity,
                    parent: Some(directory_identity),
                    stat: Some(local_stat(&stat)),
                };
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

fn open_bound_directory(path: &Path, operation: &str) -> Result<(OwnedFd, Identity)> {
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
    Ok((fd, stat_identity(&stat)))
}

fn open_quarantine(state: &OwnedFd) -> Result<OwnedFd> {
    match mkdirat(state, "quarantine", Mode::RWXU) {
        Ok(()) => {}
        Err(rustix::io::Errno::EXIST) => {}
        Err(error) => {
            return Err(Error::errno(
                "create retained artifact directory",
                Path::new("quarantine"),
                error,
            ));
        }
    }
    let fd = openat(
        state,
        "quarantine",
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map_err(|e| {
        Error::errno(
            "open retained artifact directory",
            Path::new("quarantine"),
            e,
        )
    })?;
    fsync(state)
        .map_err(|e| Error::errno("persist retained artifact directory", Path::new("."), e))?;
    Ok(fd)
}

fn acquire_state_lease(state: &OwnedFd, path: &Path) -> Result<std::fs::File> {
    let fd = openat(
        state,
        "replica.lock",
        OFlags::RDWR | OFlags::CREATE | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::RUSR | Mode::WUSR,
    )
    .map_err(|e| Error::errno("open replica lease", path, e))?;
    match flock(&fd, FlockOperation::NonBlockingLockExclusive) {
        Ok(()) => {
            fsync(state).map_err(|e| Error::errno("persist replica lease", path, e))?;
            Ok(std::fs::File::from(fd))
        }
        Err(rustix::io::Errno::WOULDBLOCK) => Err(Error::busy(format!(
            "another process holds the replica state lease at {}",
            path.display()
        ))),
        Err(error) => Err(Error::errno("acquire replica state lease", path, error)),
    }
}

fn identity(fd: &OwnedFd, path: &Path, operation: &str) -> Result<Identity> {
    let stat = fstat(fd).map_err(|e| Error::errno(operation, path, e))?;
    Ok(stat_identity(&stat))
}

fn optional_stat(parent: &OwnedFd, name: &str, path: &Path) -> Result<Option<Stat>> {
    match statat(parent, name, AtFlags::SYMLINK_NOFOLLOW) {
        Ok(stat) => Ok(Some(stat)),
        Err(rustix::io::Errno::NOENT) => Ok(None),
        Err(error) => Err(Error::errno("inspect local entry", path, error)),
    }
}

fn unlink_temporary_if_owned(
    parent: &OwnedFd,
    name: &str,
    expected: Identity,
    path: &Path,
) -> Result<()> {
    let Some(stat) = optional_stat(parent, name, path)? else {
        return Ok(());
    };
    if stat.st_dev != expected.dev || stat.st_ino != expected.ino {
        return Ok(()); // Preserve the uncertain entry; it is outside the payload.
    }
    unlinkat(parent, name, AtFlags::empty())
        .map_err(|e| Error::errno("remove owned staged temporary", path, e))
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
    let mut reader = Dir::read_from(&directory)
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

fn find_directory(
    directory: &OwnedFd,
    filesystem_dev: u64,
    target: Identity,
    display_path: &Path,
) -> Result<Option<OwnedFd>> {
    let stat =
        fstat(directory).map_err(|e| Error::errno("inspect bounded directory", display_path, e))?;
    if stat.st_dev == target.dev && stat.st_ino == target.ino {
        return openat(
            directory,
            ".",
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .map(Some)
        .map_err(|e| Error::errno("duplicate journaled parent", display_path, e));
    }
    let mut reader = Dir::read_from(directory)
        .map_err(|e| Error::errno("read bounded directory", display_path, e))?;
    while let Some(entry) = reader.read() {
        let entry = entry.map_err(|e| Error::errno("read bounded directory", display_path, e))?;
        let name = entry
            .file_name()
            .to_str()
            .map_err(|_| Error::unsupported("non-UTF-8 local filename"))?;
        if name == "." || name == ".." {
            continue;
        }
        let child = statat(directory, name, AtFlags::SYMLINK_NOFOLLOW)
            .map_err(|e| Error::errno("inspect bounded directory", display_path, e))?;
        if FileType::from_raw_mode(child.st_mode) != FileType::Directory
            || child.st_dev != filesystem_dev
        {
            continue;
        }
        let fd = openat(
            directory,
            name,
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .map_err(|e| Error::errno("open bounded directory", display_path, e))?;
        if let Some(found) = find_directory(&fd, filesystem_dev, target, display_path)? {
            return Ok(Some(found));
        }
    }
    Ok(None)
}

fn stat_identity(stat: &Stat) -> Identity {
    Identity {
        dev: stat.st_dev,
        ino: stat.st_ino,
    }
}

fn artifact_stat(stat: &Stat) -> ArtifactStat {
    ArtifactStat {
        dev: stat.st_dev,
        ino: stat.st_ino,
        mtime: stat.st_mtime,
        mtime_nsec: stat.st_mtime_nsec as i64,
        ctime: stat.st_ctime,
        ctime_nsec: stat.st_ctime_nsec as i64,
    }
}

fn local_stat(stat: &Stat) -> LocalStat {
    LocalStat::from_stat(stat)
}

fn artifact_stat_matches(record: &ArtifactRecord, stat: &Stat) -> bool {
    record.stat.is_some_and(|stored| {
        stored.dev == stat.st_dev
            && stored.ino == stat.st_ino
            && stored.mtime == stat.st_mtime
            && stored.mtime_nsec == stat.st_mtime_nsec as i64
            && stored.ctime == stat.st_ctime
            && stored.ctime_nsec == stat.st_ctime_nsec as i64
            && (FileType::from_raw_mode(stat.st_mode) != FileType::RegularFile
                || stat.st_size as u64 == record.length)
    })
}

fn hash_file_at(
    parent: &OwnedFd,
    name: &str,
    path: &Path,
) -> Result<(u64, ContentHash, Identity, bool)> {
    hash_file_at_counted(parent, name, path, None)
}

fn hash_file_at_counted(
    parent: &OwnedFd,
    name: &str,
    path: &Path,
    counter: Option<&AtomicU64>,
) -> Result<(u64, ContentHash, Identity, bool)> {
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
        if let Some(counter) = counter {
            counter.fetch_add(count as u64, Ordering::Relaxed);
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
        stat_identity(&after),
        Mode::from_raw_mode(after.st_mode).intersects(Mode::XUSR | Mode::XGRP | Mode::XOTH),
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

fn scan_hint_matches(current: &Stat, hint: &LocalScanHint) -> bool {
    let Some(stored) = hint.node.stat else {
        return false;
    };
    stored
        == LocalStat {
            dev: current.st_dev,
            ino: current.st_ino,
            size: current.st_size as u64,
            mtime: current.st_mtime,
            mtime_nsec: current.st_mtime_nsec as i64,
            ctime: current.st_ctime,
            ctime_nsec: current.st_ctime_nsec as i64,
        }
        && local_stat_is_racy_clean(&stored, hint.recorded_ns)
}

fn scan_hint_matches_node(node: &LocalNode, hint: &LocalScanHint) -> bool {
    let (Some(stored), Some(current)) = (hint.node.stat, node.stat) else {
        return false;
    };
    stored == current && local_stat_is_racy_clean(&current, hint.recorded_ns)
}

fn local_stat_is_racy_clean(stat: &LocalStat, recorded_ns: i64) -> bool {
    let timestamp_ns = |seconds: i64, nanos: i64| -> i128 {
        i128::from(seconds) * 1_000_000_000 + i128::from(nanos)
    };
    let cutoff = i128::from(recorded_ns) - 1_000_000_000;
    timestamp_ns(stat.mtime, stat.mtime_nsec) < cutoff
        && timestamp_ns(stat.ctime, stat.ctime_nsec) < cutoff
}

fn same_tree(expected: &BTreeMap<String, LocalNode>, actual: &BTreeMap<String, LocalNode>) -> bool {
    expected.len() == actual.len()
        && expected.iter().all(|(path, expected)| {
            actual.get(path).is_some_and(|actual| {
                expected.kind == actual.kind && expected.executable == actual.executable
            })
        })
}

pub(super) fn validate_path(path: &str) -> Result<()> {
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

fn split_path(path: &str) -> Result<(&str, &str)> {
    if path.is_empty() {
        return Err(Error::invalid(
            "the synchronized root is not a mutation target",
        ));
    }
    validate_path(path)?;
    Ok(path.rsplit_once('/').unwrap_or(("", path)))
}

fn join(parent: &str, name: &str) -> String {
    if parent.is_empty() {
        name.to_owned()
    } else {
        format!("{parent}/{name}")
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replica::ErrorKind;
    use std::os::unix::fs::FileExt as _;

    #[test]
    fn rollback_retains_replacement_inode_after_second_exchange() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("payload");
        let state = temp.path().join("state");
        std::fs::create_dir(&root).unwrap();
        std::fs::write(root.join("file"), b"old").unwrap();
        let mut replica = Replica::init(&state, &root, NodeId::from_bytes([9; 16])).unwrap();
        let snapshot = replica.scan().unwrap();
        let expected = snapshot.get("file").unwrap();
        let local_writer = std::fs::OpenOptions::new()
            .write(true)
            .open(root.join("file"))
            .unwrap();
        let mut install = replica
            .prepare_install("file", expected, false, None)
            .unwrap();
        install.write_all(b"remote").unwrap();
        local_writer.write_at(b"busy", 0).unwrap();

        let target = root.join("file");
        let error = install
            .finish_with_hook(|_| {
                let replacement_writer = std::fs::OpenOptions::new()
                    .write(true)
                    .open(&target)
                    .unwrap();
                replacement_writer.write_at(b"remote-late", 0).unwrap();
                replacement_writer.sync_all().unwrap();
            })
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Conflict);
        assert_eq!(std::fs::read(&target).unwrap(), b"busy");

        let rollback = replica
            .status()
            .unwrap()
            .artifacts
            .into_iter()
            .find(|artifact| artifact.kind == "rollback-displaced")
            .unwrap();
        let mut retained = Vec::new();
        replica
            .export_artifact(&rollback.id, &mut retained)
            .unwrap();
        assert_eq!(retained, b"remote-late");
    }
}
