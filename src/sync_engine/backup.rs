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

//! Quiesced backup and isolated restore for the shared sync engine.

use super::adapter::{RemoteAdapter, SyncModel};
use super::local::{LocalTree, check_root_state};
use super::store::{Binding, InstallState, Store};
use super::types::{Identity, LocalKind, LocalNode, validate_path};
use super::{Error, Result};
use crate::local::store;
use borsh::{BorshDeserialize, BorshSerialize};
use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Component, Path, PathBuf};

const MAGIC: [u8; 8] = *b"YYSYNC01";
const VERSION: u8 = 1;
const MANIFEST: &str = "manifest.bin";
const DATABASES: [&str; 3] = ["sync-engine.db", "sync.db", "content/staging.db"];

#[derive(Debug, BorshSerialize, BorshDeserialize)]
struct BackupManifest {
    magic: [u8; 8],
    version: u8,
    model: u8,
    namespace: String,
    namespace_digest: [u8; 32],
    root: Vec<SnapshotNode>,
    artifacts: Vec<ArtifactSnapshot>,
    databases: Vec<FileEvidence>,
    support: Vec<FileEvidence>,
}

#[derive(Clone, Debug, BorshSerialize, BorshDeserialize)]
struct SnapshotNode {
    path: String,
    directory: bool,
    length: Option<u64>,
    digest: Option<[u8; 32]>,
    executable: bool,
    dev: u64,
    ino: u64,
    parent_dev: Option<u64>,
    parent_ino: Option<u64>,
}

impl SnapshotNode {
    fn from_local(node: &LocalNode) -> Self {
        let (directory, length, digest) = match node.kind {
            LocalKind::Directory => (true, None, None),
            LocalKind::File { length, digest } => (false, Some(length), Some(*digest.as_bytes())),
        };
        Self {
            path: node.path.clone(),
            directory,
            length,
            digest,
            executable: node.executable,
            dev: node.identity.dev,
            ino: node.identity.ino,
            parent_dev: node.parent.map(|parent| parent.dev),
            parent_ino: node.parent.map(|parent| parent.ino),
        }
    }

    fn identity(&self) -> Identity {
        Identity {
            dev: self.dev,
            ino: self.ino,
        }
    }

    fn same_content(&self, other: &Self) -> bool {
        self.path == other.path
            && self.directory == other.directory
            && self.length == other.length
            && self.digest == other.digest
            && self.executable == other.executable
    }
}

#[derive(Debug, BorshSerialize, BorshDeserialize)]
struct ArtifactSnapshot {
    id: String,
    name: String,
    present: bool,
    nodes: Vec<SnapshotNode>,
}

#[derive(Debug, BorshSerialize, BorshDeserialize)]
struct FileEvidence {
    path: String,
    length: u64,
    hash: [u8; 32],
}

impl BackupManifest {
    fn capture(engine: &super::SyncEngine<impl RemoteAdapter>) -> Result<Self> {
        if engine.local.store().installs()?.iter().any(|install| {
            matches!(
                install.state,
                InstallState::Prepared | InstallState::Dispatched
            )
        }) {
            return Err(Error::invalid(
                "sync state contains an incomplete local install; complete recovery before taking a backup",
            ));
        }
        let binding = engine.local.binding()?;
        let root = snapshot_path(engine.root())?;
        let mut artifacts = Vec::new();
        for artifact in engine.local.store().artifacts()? {
            let path = engine.local.artifact_path(&artifact);
            let present = path
                .try_exists()
                .map_err(|error| Error::io("inspect retained sync artifact", &path, error))?;
            let nodes = if present {
                snapshot_path(&path)?
                    .nodes()
                    .values()
                    .map(SnapshotNode::from_local)
                    .collect()
            } else {
                Vec::new()
            };
            artifacts.push(ArtifactSnapshot {
                id: artifact.id,
                name: artifact.name,
                present,
                nodes,
            });
        }
        let databases = DATABASES
            .into_iter()
            .filter(|relative| engine.state().join(relative).is_file())
            .map(|relative| {
                let (length, hash) = hash_file(&engine.state().join(relative))?;
                Ok(FileEvidence {
                    path: relative.to_owned(),
                    length,
                    hash,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let support = discover_support(engine.state())?
            .into_iter()
            .map(|path| {
                let (length, hash) = hash_file(&engine.state().join(&path))?;
                Ok(FileEvidence { path, length, hash })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            magic: MAGIC,
            version: VERSION,
            model: match binding.model {
                SyncModel::Managed => 0,
                SyncModel::Direct => 1,
            },
            namespace: binding.namespace,
            namespace_digest: binding.namespace_digest,
            root: root
                .nodes()
                .values()
                .map(SnapshotNode::from_local)
                .collect(),
            artifacts,
            databases,
            support,
        })
    }

    fn model(&self) -> Result<SyncModel> {
        match self.model {
            0 => Ok(SyncModel::Managed),
            1 => Ok(SyncModel::Direct),
            _ => Err(Error::corrupt("backup manifest has an unknown sync model")),
        }
    }

    fn write(
        &mut self,
        engine: &super::SyncEngine<impl RemoteAdapter>,
        output: &Path,
    ) -> Result<()> {
        if output.exists() {
            return Err(Error::invalid(format!(
                "backup destination {} already exists",
                output.display()
            )));
        }
        let root = fs::canonicalize(engine.root())
            .map_err(|error| Error::io("canonicalize backup root", engine.root(), error))?;
        let state = fs::canonicalize(engine.state())
            .map_err(|error| Error::io("canonicalize backup state", engine.state(), error))?;
        let parent = output.parent().unwrap_or_else(|| Path::new("."));
        let parent = fs::canonicalize(parent)
            .map_err(|error| Error::io("canonicalize backup parent", parent, error))?;
        let output = parent.join(
            output
                .file_name()
                .ok_or_else(|| Error::invalid("backup destination requires a directory name"))?,
        );
        if output.starts_with(&root) || output.starts_with(&state) {
            return Err(Error::invalid(
                "backup destination must be outside the synchronized root and state directories",
            ));
        }

        create_private_directory(&output, "create backup destination")?;
        create_private_directory(&output.join("root"), "create backup root")?;
        create_private_directory(&output.join("state"), "create backup state")?;
        sync_directory(&parent)?;
        sync_directory(&output)?;

        let mut seen = BTreeMap::new();
        copy_snapshot(&root, &output.join("root"), &self.root, &mut seen)?;
        sync_snapshot_directories(&output.join("root"), &self.root)?;
        for artifact in &self.artifacts {
            if !artifact.present {
                continue;
            }
            let source = state.join("artifacts").join(&artifact.name);
            let destination = output.join("state/artifacts").join(&artifact.name);
            if let Some(parent) = destination.parent() {
                create_private_directory_all(parent, "create backup artifact parent")?;
            }
            copy_snapshot(&source, &destination, &artifact.nodes, &mut seen)?;
            sync_snapshot_directories(&destination, &artifact.nodes)?;
        }
        for file in &self.support {
            let source = state.join(&file.path);
            let destination = output.join("state").join(&file.path);
            if let Some(parent) = destination.parent() {
                create_private_directory_all(parent, "create backup support parent")?;
            }
            copy_file(&source, &destination)?;
            verify_evidence(&destination, file)?;
        }
        for database in &mut self.databases {
            let destination = output.join("state").join(&database.path);
            if let Some(parent) = destination.parent() {
                create_private_directory_all(parent, "create backup database parent")?;
            }
            vacuum(&state.join(&database.path), &destination)?;
            (database.length, database.hash) = hash_file(&destination)?;
        }
        self.save(&output.join(MANIFEST))?;
        for relative in ["root", "state/artifacts", "state/content", "state"] {
            let path = output.join(relative);
            if path.is_dir() {
                sync_directory(&path)?;
            }
        }
        sync_directory(&output)?;
        sync_directory(&parent)
    }

    fn load(backup: &Path) -> Result<Self> {
        let path = backup.join(MANIFEST);
        let mut bytes =
            fs::read(&path).map_err(|error| Error::io("read backup manifest", &path, error))?;
        if bytes.len() < 32 {
            return Err(Error::corrupt("backup manifest is truncated"));
        }
        let hash = bytes.split_off(bytes.len() - 32);
        if blake3::hash(&bytes).as_bytes().as_slice() != hash {
            return Err(Error::corrupt("backup manifest checksum does not match"));
        }
        let manifest: Self = borsh::from_slice(&bytes)
            .map_err(|_| Error::corrupt("backup manifest is malformed"))?;
        if manifest.magic != MAGIC || manifest.version != VERSION {
            return Err(Error::unsupported("unsupported sync backup profile"));
        }
        manifest.validate_shape()?;
        Ok(manifest)
    }

    fn save(&self, path: &Path) -> Result<()> {
        let mut bytes =
            borsh::to_vec(self).map_err(|_| Error::corrupt("encode backup manifest"))?;
        bytes.extend(blake3::hash(&bytes).as_bytes());
        let temporary = path.with_extension("tmp");
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)
            .map_err(|error| Error::io("create backup manifest", &temporary, error))?;
        file.write_all(&bytes)
            .map_err(|error| Error::io("write backup manifest", &temporary, error))?;
        file.sync_all()
            .map_err(|error| Error::io("persist backup manifest", &temporary, error))?;
        fs::rename(&temporary, path)
            .map_err(|error| Error::io("install backup manifest", path, error))
    }

    fn validate_shape(&self) -> Result<()> {
        let _ = self.model()?;
        if self.namespace_digest != super::local::namespace_digest(&self.namespace) {
            return Err(Error::corrupt(
                "backup manifest namespace digest does not match its namespace",
            ));
        }
        if self.namespace.is_empty() {
            return Err(Error::corrupt("backup manifest has an empty namespace"));
        }
        let mut artifact_ids = BTreeSet::new();
        let mut artifact_names = BTreeSet::new();
        for artifact in &self.artifacts {
            if !valid_artifact_name(&artifact.name) {
                return Err(Error::corrupt(
                    "backup manifest contains an unsafe artifact name",
                ));
            }
            if !artifact_ids.insert(artifact.id.as_str())
                || !artifact_names.insert(artifact.name.as_str())
            {
                return Err(Error::corrupt(
                    "backup manifest contains duplicate artifact identities",
                ));
            }
            if !artifact.present && !artifact.nodes.is_empty() {
                return Err(Error::corrupt(
                    "backup manifest has nodes for an absent artifact",
                ));
            }
            if artifact.present {
                validate_nodes(&artifact.nodes, "backup artifact")?;
            }
        }
        validate_nodes(&self.root, "backup root")?;
        if !self
            .databases
            .iter()
            .any(|database| database.path == "sync-engine.db")
        {
            return Err(Error::corrupt(
                "backup manifest does not contain the sync engine database",
            ));
        }
        for evidence in self.databases.iter().chain(&self.support) {
            if !safe_relative(&evidence.path) {
                return Err(Error::corrupt(
                    "backup manifest contains an unsafe support path",
                ));
            }
        }
        Ok(())
    }

    fn validate_package(&self, backup: &Path) -> Result<()> {
        let store = Store::open_read_only(&backup.join("state/sync-engine.db"))?;
        let binding = store.binding()?;
        if binding.model != self.model()?
            || binding.namespace != self.namespace
            || binding.namespace_digest != self.namespace_digest
        {
            return Err(Error::corrupt(
                "backup sync binding does not match its manifest",
            ));
        }
        if store.installs()?.iter().any(|install| {
            matches!(
                install.state,
                InstallState::Prepared | InstallState::Dispatched
            )
        }) {
            return Err(Error::corrupt(
                "backup sync state contains an incomplete local install",
            ));
        }
        for evidence in self.databases.iter().chain(&self.support) {
            verify_evidence(&backup.join("state").join(&evidence.path), evidence)?;
        }
        let mut identities = BTreeMap::new();
        verify_snapshot(
            &self.root,
            &snapshot_path(&backup.join("root"))?,
            &mut identities,
            "backup root",
        )?;
        for artifact in &self.artifacts {
            let path = backup.join("state/artifacts").join(&artifact.name);
            if !artifact.present {
                if path
                    .try_exists()
                    .map_err(|error| Error::io("inspect absent backup artifact", &path, error))?
                {
                    return Err(Error::corrupt(format!(
                        "absent artifact {} unexpectedly exists in the backup",
                        artifact.id
                    )));
                }
                continue;
            }
            verify_snapshot(
                &artifact.nodes,
                &snapshot_path(&path)?,
                &mut identities,
                &format!("backup artifact {}", artifact.id),
            )?;
        }
        Ok(())
    }

    fn restore(&self, backup: &Path, root: &Path, state: &Path) -> Result<()> {
        self.validate_package(backup)?;
        let backup = fs::canonicalize(backup)
            .map_err(|error| Error::io("canonicalize backup package", backup, error))?;
        let root = canonical_target(root)?;
        let state = canonical_target(state)?;
        if root.starts_with(&backup) || backup.starts_with(&root) {
            return Err(Error::invalid("restore root overlaps the backup package"));
        }
        if state.starts_with(&backup) || backup.starts_with(&state) {
            return Err(Error::invalid("restore state overlaps the backup package"));
        }
        check_root_state(&root, &state)?;
        if root.exists() {
            return Err(Error::invalid(format!(
                "restore root {} already exists",
                root.display()
            )));
        }
        if state.exists() {
            return Err(Error::invalid(format!(
                "restore state {} already exists",
                state.display()
            )));
        }
        create_private_directory(&root, "create restore root")?;
        create_private_directory(&state, "create restore state")?;
        sync_parent_directory(&root)?;
        sync_parent_directory(&state)?;

        let mut seen = BTreeMap::new();
        copy_snapshot(&backup.join("root"), &root, &self.root, &mut seen)?;
        sync_snapshot_directories(&root, &self.root)?;
        for artifact in &self.artifacts {
            if !artifact.present {
                continue;
            }
            let source = backup.join("state/artifacts").join(&artifact.name);
            let destination = state.join("artifacts").join(&artifact.name);
            if let Some(parent) = destination.parent() {
                create_private_directory_all(parent, "create restored artifact parent")?;
            }
            copy_snapshot(&source, &destination, &artifact.nodes, &mut seen)?;
            sync_snapshot_directories(&destination, &artifact.nodes)?;
        }
        for file in &self.support {
            let source = backup.join("state").join(&file.path);
            let destination = state.join(&file.path);
            if let Some(parent) = destination.parent() {
                create_private_directory_all(parent, "create restored support parent")?;
            }
            copy_file(&source, &destination)?;
        }
        for database in &self.databases {
            let source = backup.join("state").join(&database.path);
            let destination = state.join(&database.path);
            if let Some(parent) = destination.parent() {
                create_private_directory_all(parent, "create restored database parent")?;
            }
            copy_file(&source, &destination)?;
        }

        let mut identities = BTreeMap::new();
        verify_snapshot(
            &self.root,
            &snapshot_path(&root)?,
            &mut identities,
            "restored root",
        )?;
        for artifact in &self.artifacts {
            if !artifact.present {
                continue;
            }
            verify_snapshot(
                &artifact.nodes,
                &snapshot_path(&state.join("artifacts").join(&artifact.name))?,
                &mut identities,
                &format!("restored artifact {}", artifact.id),
            )?;
        }
        let old = Store::open_read_only(&state.join("sync-engine.db"))?.binding()?;
        let root_metadata =
            fs::metadata(&root).map_err(|error| Error::io("inspect restore root", &root, error))?;
        let state_metadata = fs::metadata(&state)
            .map_err(|error| Error::io("inspect restore state", &state, error))?;
        if root_metadata.dev() != state_metadata.dev() {
            return Err(Error::unsupported(
                "restore root and state must be on the same filesystem",
            ));
        }
        identities.insert(old.root_identity, identity(&root_metadata));
        identities.insert(old.state_identity, identity(&state_metadata));
        let binding = Binding {
            model: self.model()?,
            namespace: self.namespace.clone(),
            namespace_digest: self.namespace_digest,
            root: root.clone(),
            root_identity: identity(&root_metadata),
            state_identity: identity(&state_metadata),
        };
        let mut store = Store::open(&state.join("sync-engine.db"))?;
        store.rebind(&binding, &identities)?;
        for relative in ["artifacts", "content", "content/staging.db"] {
            let path = state.join(relative);
            if path.is_dir() {
                sync_directory(&path)?;
            } else if path.is_file() {
                File::open(&path)
                    .and_then(|file| file.sync_all())
                    .map_err(|error| Error::io("persist restored state", &path, error))?;
            }
        }
        sync_directory(&state)?;
        sync_directory(&root)?;
        Ok(())
    }
}

pub(super) fn create(engine: &super::SyncEngine<impl RemoteAdapter>, output: &Path) -> Result<()> {
    BackupManifest::capture(engine)?.write(engine, output)
}

pub(super) fn inspect(backup: &Path) -> Result<super::BackupInspection> {
    let manifest = BackupManifest::load(backup)?;
    manifest.validate_package(backup)?;
    Ok(super::BackupInspection {
        model: manifest.model()?,
        namespace: manifest.namespace.clone(),
        root_entries: manifest.root.len() as u64,
        artifact_entries: manifest.artifacts.len() as u64,
        databases: manifest
            .databases
            .iter()
            .map(|evidence| evidence.path.clone())
            .collect(),
        support: manifest
            .support
            .iter()
            .map(|evidence| evidence.path.clone())
            .collect(),
        state: super::inspect_protection(&backup.join("state"))?,
    })
}

pub(super) async fn restore<A: RemoteAdapter>(
    remote: A,
    backup: &Path,
    root: &Path,
    state: &Path,
) -> Result<super::SyncEngine<A>> {
    let manifest = BackupManifest::load(backup)?;
    if remote.model() != manifest.model()? || remote.namespace_id() != manifest.namespace {
        return Err(Error::invalid(
            "backup package belongs to a different sync model or namespace",
        ));
    }
    remote.preflight(&backup.join("state")).await?;
    manifest.restore(backup, root, state)?;
    remote.bind(state).await?;
    let local = LocalTree::open(state, remote.model(), remote.namespace_id())?;
    Ok(super::SyncEngine {
        remote,
        local,
        model: manifest.model()?,
    })
}

fn validate_nodes(nodes: &[SnapshotNode], context: &str) -> Result<()> {
    let mut paths = BTreeSet::new();
    for node in nodes {
        validate_path(&node.path)
            .map_err(|error| Error::corrupt(format!("{context} has an invalid path: {error}")))?;
        if !paths.insert(node.path.as_str()) {
            return Err(Error::corrupt(format!(
                "{context} contains duplicate path {:?}",
                node.path
            )));
        }
        if node.directory {
            if node.length.is_some() || node.digest.is_some() || node.executable {
                return Err(Error::corrupt(format!(
                    "{context} contains invalid directory metadata"
                )));
            }
        } else if node.length.is_none() || node.digest.is_none() {
            return Err(Error::corrupt(format!(
                "{context} contains a file without a length or digest"
            )));
        }
        if node.path.is_empty() == node.parent_dev.is_some()
            || node.parent_dev.is_some() != node.parent_ino.is_some()
        {
            return Err(Error::corrupt(format!(
                "{context} contains inconsistent parent identity"
            )));
        }
    }
    if nodes.iter().filter(|node| node.path.is_empty()).count() != 1 {
        return Err(Error::corrupt(format!(
            "{context} must contain exactly one root"
        )));
    }
    Ok(())
}

fn snapshot_path(path: &Path) -> Result<super::LocalSnapshot> {
    let metadata = fs::symlink_metadata(path)
        .map_err(|error| Error::io("inspect backup snapshot", path, error))?;
    if metadata.file_type().is_dir() {
        let hints = BTreeMap::new();
        super::types::scan_path_with_hints(path, &hints)
    } else if metadata.file_type().is_file() {
        let (length, digest) = hash_file(path)?;
        let executable = fs::Permissions::from_mode(metadata.mode()).mode() & 0o111 != 0;
        let node = LocalNode {
            path: String::new(),
            kind: LocalKind::File {
                length,
                digest: super::types::ContentHash::from_bytes(digest),
            },
            executable,
            identity: identity(&metadata),
            parent: None,
            stat: Some(super::types::LocalStat {
                dev: metadata.dev(),
                ino: metadata.ino(),
                size: metadata.size(),
                mtime: metadata.mtime(),
                mtime_nsec: metadata.mtime_nsec(),
                ctime: metadata.ctime(),
                ctime_nsec: metadata.ctime_nsec(),
            }),
        };
        Ok(super::LocalSnapshot::from_nodes(BTreeMap::from([(
            String::new(),
            node,
        )])))
    } else {
        Err(Error::unsupported(format!(
            "backup snapshot path is not a regular file or directory: {}",
            path.display()
        )))
    }
}

fn copy_snapshot(
    source: &Path,
    destination: &Path,
    nodes: &[SnapshotNode],
    seen: &mut BTreeMap<Identity, PathBuf>,
) -> Result<()> {
    for node in nodes {
        let target = if node.path.is_empty() {
            destination.to_owned()
        } else {
            destination.join(&node.path)
        };
        if node.directory {
            create_private_directory_all(&target, "create backup directory")?;
            continue;
        }
        if let Some(parent) = target.parent() {
            create_private_directory_all(parent, "create backup parent")?;
        }
        let source_path = if node.path.is_empty() {
            source.to_owned()
        } else {
            source.join(&node.path)
        };
        if let Some(existing) = seen.get(&node.identity()) {
            fs::hard_link(existing, &target)
                .map_err(|error| Error::io("preserve backup hard link", &target, error))?;
        } else {
            copy_file(&source_path, &target)?;
            seen.insert(node.identity(), target.clone());
        }
        fs::set_permissions(
            &target,
            fs::Permissions::from_mode(if node.executable { 0o700 } else { 0o600 }),
        )
        .map_err(|error| Error::io("set backup file mode", &target, error))?;
        File::open(&target)
            .and_then(|file| file.sync_all())
            .map_err(|error| Error::io("persist backup file", &target, error))?;
        let (length, hash) = hash_file(&target)?;
        if Some(length) != node.length || Some(hash) != node.digest {
            return Err(Error::corrupt(format!(
                "backup copy {} does not match the quiesced snapshot",
                target.display()
            )));
        }
    }
    Ok(())
}

fn verify_snapshot(
    expected: &[SnapshotNode],
    actual: &super::LocalSnapshot,
    identities: &mut BTreeMap<Identity, Identity>,
    context: &str,
) -> Result<()> {
    let actual = actual
        .nodes()
        .values()
        .map(SnapshotNode::from_local)
        .map(|node| (node.path.clone(), node))
        .collect::<BTreeMap<_, _>>();
    if expected.len() != actual.len() {
        return Err(Error::corrupt(format!(
            "{context} paths do not match the backup manifest"
        )));
    }
    let mut reverse = BTreeMap::new();
    for expected in expected {
        let actual = actual.get(&expected.path).ok_or_else(|| {
            Error::corrupt(format!(
                "{context} is missing path {:?} from the backup manifest",
                expected.path
            ))
        })?;
        if !expected.same_content(actual) {
            return Err(Error::corrupt(format!(
                "{context} path {:?} does not match the backup manifest",
                expected.path
            )));
        }
        let old = expected.identity();
        let new = actual.identity();
        if let Some(previous) = identities.insert(old, new)
            && previous != new
        {
            return Err(Error::corrupt(format!(
                "{context} contains an ambiguous inode mapping"
            )));
        }
        if let Some(previous) = reverse.insert(new, old)
            && previous != old
        {
            return Err(Error::corrupt(format!(
                "{context} does not preserve hard-link identity"
            )));
        }
    }
    Ok(())
}

fn discover_support(state: &Path) -> Result<Vec<String>> {
    let mut files = Vec::new();
    let mut pending = vec![state.to_owned()];
    while let Some(directory) = pending.pop() {
        for entry in fs::read_dir(&directory)
            .map_err(|error| Error::io("read sync state for backup", &directory, error))?
        {
            let entry = entry
                .map_err(|error| Error::io("read sync state for backup", &directory, error))?;
            let path = entry.path();
            let relative = path
                .strip_prefix(state)
                .map_err(|_| Error::corrupt("sync backup support escaped its state directory"))?;
            let relative_text = relative
                .to_str()
                .ok_or_else(|| Error::unsupported("sync state path is not UTF-8"))?;
            if relative_text == "install"
                || relative_text == "artifacts"
                || relative_text.starts_with("install/")
                || relative_text.starts_with("artifacts/")
                || DATABASES.contains(&relative_text)
                || relative_text.ends_with("-wal")
                || relative_text.ends_with("-shm")
            {
                continue;
            }
            let metadata = fs::symlink_metadata(&path)
                .map_err(|error| Error::io("inspect sync state for backup", &path, error))?;
            if metadata.file_type().is_dir() {
                pending.push(path);
            } else if metadata.file_type().is_file() {
                files.push(relative_text.to_owned());
            } else {
                return Err(Error::unsupported(format!(
                    "sync state contains an unsupported file type: {}",
                    path.display()
                )));
            }
        }
    }
    files.sort();
    Ok(files)
}

fn verify_evidence(path: &Path, expected: &FileEvidence) -> Result<()> {
    let (length, hash) = hash_file(path)?;
    if length != expected.length || hash != expected.hash {
        return Err(Error::corrupt(format!(
            "backup file {} does not match its manifest",
            path.display()
        )));
    }
    Ok(())
}

fn copy_file(source: &Path, destination: &Path) -> Result<()> {
    let mut input =
        File::open(source).map_err(|error| Error::io("open backup source", source, error))?;
    let mut output = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(destination)
        .map_err(|error| Error::io("create backup destination", destination, error))?;
    std::io::copy(&mut input, &mut output)
        .map_err(|error| Error::io("copy backup file", destination, error))?;
    output
        .sync_all()
        .map_err(|error| Error::io("persist backup file", destination, error))
}

fn vacuum(source: &Path, destination: &Path) -> Result<()> {
    let connection = store::open_read_write(source, false)
        .map_err(|error| Error::io("open SQLite database for backup", source, error))?;
    let destination_text = destination
        .to_str()
        .ok_or_else(|| Error::unsupported("backup database path is not UTF-8"))?;
    connection
        .execute("VACUUM INTO ?1", [destination_text])
        .map_err(|error| Error::io("backup SQLite database", source, error))?;
    File::open(destination)
        .and_then(|file| file.sync_all())
        .map_err(|error| Error::io("persist SQLite backup", destination, error))
}

fn hash_file(path: &Path) -> Result<(u64, [u8; 32])> {
    let mut file = File::open(path).map_err(|error| Error::io("open for checksum", path, error))?;
    let mut hasher = blake3::Hasher::new();
    let mut length = 0_u64;
    let mut buffer = vec![0_u8; 64 * 1024];
    loop {
        let count = file
            .read(&mut buffer)
            .map_err(|error| Error::io("read for checksum", path, error))?;
        if count == 0 {
            break;
        }
        hasher.update(&buffer[..count]);
        length = length
            .checked_add(count as u64)
            .ok_or_else(|| Error::unsupported("file length overflow"))?;
    }
    Ok((length, *hasher.finalize().as_bytes()))
}

fn valid_artifact_name(name: &str) -> bool {
    !name.is_empty()
        && name != "."
        && name != ".."
        && !name.contains('/')
        && yinyang_core::namespace::name_key(name).is_ok()
}

fn safe_relative(path: &str) -> bool {
    !path.is_empty()
        && Path::new(path)
            .components()
            .all(|component| matches!(component, Component::Normal(_)))
}

fn canonical_target(path: &Path) -> Result<PathBuf> {
    if path.exists() {
        return fs::canonicalize(path)
            .map_err(|error| Error::io("canonicalize restore destination", path, error));
    }
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let parent = fs::canonicalize(parent)
        .map_err(|error| Error::io("canonicalize restore parent", parent, error))?;
    let name = path
        .file_name()
        .ok_or_else(|| Error::invalid("restore destination requires a directory name"))?;
    Ok(parent.join(name))
}

fn create_private_directory(path: &Path, operation: &str) -> Result<()> {
    fs::DirBuilder::new()
        .mode(0o700)
        .create(path)
        .map_err(|error| Error::io(operation, path, error))
}

fn create_private_directory_all(path: &Path, operation: &str) -> Result<()> {
    if path.is_dir() {
        return Ok(());
    }
    if let Some(parent) = path.parent() {
        create_private_directory_all(parent, operation)?;
    }
    create_private_directory(path, operation)?;
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
        .map_err(|error| Error::io("set private state directory mode", path, error))
}

fn sync_directory(path: &Path) -> Result<()> {
    File::open(path)
        .and_then(|file| file.sync_all())
        .map_err(|error| Error::io("persist directory", path, error))
}

fn sync_parent_directory(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        sync_directory(parent)?;
    }
    Ok(())
}

fn sync_snapshot_directories(destination: &Path, nodes: &[SnapshotNode]) -> Result<()> {
    let mut directories = nodes
        .iter()
        .filter(|node| node.directory)
        .map(|node| node.path.as_str())
        .collect::<Vec<_>>();
    directories.sort_by_key(|path| Reverse(Path::new(path).components().count()));
    for path in directories {
        let target = if path.is_empty() {
            destination.to_owned()
        } else {
            destination.join(path)
        };
        sync_directory(&target)?;
    }
    Ok(())
}

fn identity(metadata: &fs::Metadata) -> Identity {
    Identity {
        dev: metadata.dev(),
        ino: metadata.ino(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manifest_path_traversal_is_rejected() {
        let temp = tempfile::tempdir().unwrap();
        let manifest = BackupManifest {
            magic: MAGIC,
            version: VERSION,
            model: 0,
            namespace: "namespace".to_owned(),
            namespace_digest: super::super::local::namespace_digest("namespace"),
            root: vec![SnapshotNode {
                path: String::new(),
                directory: true,
                length: None,
                digest: None,
                executable: false,
                dev: 1,
                ino: 2,
                parent_dev: None,
                parent_ino: None,
            }],
            artifacts: vec![ArtifactSnapshot {
                id: "artifact".to_owned(),
                name: "artifact".to_owned(),
                present: true,
                nodes: vec![SnapshotNode {
                    path: "../outside".to_owned(),
                    directory: false,
                    length: Some(1),
                    digest: Some([0; 32]),
                    executable: false,
                    dev: 1,
                    ino: 3,
                    parent_dev: Some(1),
                    parent_ino: Some(2),
                }],
            }],
            databases: vec![FileEvidence {
                path: "sync-engine.db".to_owned(),
                length: 1,
                hash: [0; 32],
            }],
            support: Vec::new(),
        };
        manifest.save(&temp.path().join(MANIFEST)).unwrap();
        let error = BackupManifest::load(temp.path()).unwrap_err();
        assert!(error.to_string().contains("invalid path"));
    }
}
