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

//! Experimental quiesced backup package for Linux Managed Sync.

use super::linux::{
    Identity, LocalKind, LocalNode, LocalSnapshot, Replica, scan_path, validate_path,
};
use super::state::{Binding, IdentityMap, Store};
use super::{Error, Result};
use borsh::{BorshDeserialize, BorshSerialize};
use rusqlite::Connection;
use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::fs::{DirBuilderExt, MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
use yinyang_core::NodeId;
const MAGIC: [u8; 8] = *b"YYBK0001";
const VERSION: u8 = 1;
const MANIFEST: &str = "manifest.bin";
const DATABASES: [&str; 3] = [
    "state/replica.db",
    "state/sync.db",
    "state/content/staging.db",
];

#[derive(BorshSerialize, BorshDeserialize)]
pub(super) struct BackupManifest {
    magic: [u8; 8],
    version: u8,
    filesystem: [u8; 16],
    binding: BackupBinding,
    root: Vec<SnapshotNode>,
    artifacts: Vec<ArtifactSnapshot>,
    databases: Vec<DatabaseEvidence>,
}

#[derive(BorshSerialize, BorshDeserialize)]
struct BackupBinding {
    root: String,
    root_dev: u64,
    root_ino: u64,
    state_dev: u64,
    state_ino: u64,
}

#[derive(Clone, BorshSerialize, BorshDeserialize)]
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

#[derive(BorshSerialize, BorshDeserialize)]
struct ArtifactSnapshot {
    id: String,
    name: String,
    present: bool,
    nodes: Vec<SnapshotNode>,
}

#[derive(BorshSerialize, BorshDeserialize)]
struct DatabaseEvidence {
    path: String,
    length: u64,
    hash: [u8; 32],
}

fn valid_artifact_name(name: &str) -> bool {
    !name.is_empty()
        && name != "."
        && name != ".."
        && !name.contains('/')
        && yinyang_core::namespace::name_key(name).is_ok()
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
    let roots = nodes.iter().filter(|node| node.path.is_empty()).count();
    if roots != 1 {
        return Err(Error::corrupt(format!(
            "{context} must contain exactly one root"
        )));
    }
    Ok(())
}

impl BackupManifest {
    pub(super) fn filesystem(&self) -> NodeId {
        NodeId::from_bytes(self.filesystem)
    }

    pub(super) fn create(replica: &Replica, output: &Path) -> Result<()> {
        Self::capture(replica)?.write(replica, output)
    }

    fn capture(replica: &Replica) -> Result<Self> {
        if !replica.store().incomplete_installs()?.is_empty() {
            return Err(Error::invalid(
                "sync state contains an incomplete local install; complete recovery before taking a backup",
            ));
        }
        let binding = replica
            .store()
            .binding()?
            .ok_or_else(|| Error::invalid("sync state is not initialized; run sync init"))?;
        let root = snapshot(replica.root())?;
        let mut artifacts = Vec::new();
        for artifact in replica.store().artifacts()? {
            if artifact.name.is_empty()
                || artifact.name == "."
                || artifact.name == ".."
                || artifact.name.contains('/')
            {
                return Err(Error::corrupt(
                    "retained artifact has an unsafe stored name",
                ));
            }
            let artifact_path = replica.artifact_path(&artifact.id)?;
            match fs::symlink_metadata(&artifact_path) {
                Err(error)
                    if error.kind() == std::io::ErrorKind::NotFound
                        && artifact.phase == "failed" =>
                {
                    artifacts.push(ArtifactSnapshot {
                        id: artifact.id,
                        name: artifact.name,
                        present: false,
                        nodes: Vec::new(),
                    })
                }
                Err(error) => {
                    return Err(Error::io(
                        "inspect retained artifact",
                        &artifact_path,
                        error,
                    ));
                }
                Ok(_) => {
                    let nodes = replica.scan_artifact(&artifact.name)?;
                    artifacts.push(ArtifactSnapshot {
                        id: artifact.id,
                        name: artifact.name,
                        present: true,
                        nodes: nodes.values().map(SnapshotNode::from_local).collect(),
                    });
                }
            }
        }
        Ok(Self {
            magic: MAGIC,
            version: VERSION,
            filesystem: *replica.filesystem().as_bytes(),
            binding: BackupBinding {
                root: binding.root,
                root_dev: binding.root_dev,
                root_ino: binding.root_ino,
                state_dev: binding.state_dev,
                state_ino: binding.state_ino,
            },
            root: root
                .nodes()
                .values()
                .map(SnapshotNode::from_local)
                .collect(),
            artifacts,
            databases: Vec::new(),
        })
    }

    fn write(&mut self, replica: &Replica, output: &Path) -> Result<()> {
        if output.exists() {
            return Err(Error::invalid(format!(
                "backup destination {} already exists",
                output.display()
            )));
        }
        let root = fs::canonicalize(replica.root())
            .map_err(|error| Error::io("canonicalize backup root", replica.root(), error))?;
        let state = fs::canonicalize(replica.state())
            .map_err(|error| Error::io("canonicalize backup state", replica.state(), error))?;
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
        let root_output = output.join("root");
        let state_output = output.join("state");
        create_private_directory(&root_output, "create backup root")?;
        create_private_directory(&state_output, "create backup state")?;
        sync_directory(&parent)?;
        sync_directory(&output)?;

        let mut seen = BTreeMap::new();
        copy_snapshot(&root, &root_output, &self.root, &mut seen)?;
        sync_snapshot_directories(&root_output, &self.root)?;
        fs::create_dir_all(state_output.join("content"))
            .map_err(|error| Error::io("create backup content state", &state_output, error))?;
        fs::create_dir(state_output.join("quarantine"))
            .map_err(|error| Error::io("create backup artifacts", &state_output, error))?;
        for artifact in &self.artifacts {
            if !artifact.present {
                continue;
            }
            copy_snapshot(
                &state.join("quarantine").join(&artifact.name),
                &state_output.join("quarantine").join(&artifact.name),
                &artifact.nodes,
                &mut seen,
            )?;
            sync_snapshot_directories(
                &state_output.join("quarantine").join(&artifact.name),
                &artifact.nodes,
            )?;
        }
        self.databases.clear();
        for relative in DATABASES {
            let destination = output.join(relative);
            vacuum(
                &state.join(relative.trim_start_matches("state/")),
                &destination,
            )?;
            let (length, hash) = hash_file(&destination)?;
            self.databases.push(DatabaseEvidence {
                path: relative.to_owned(),
                length,
                hash,
            });
        }
        self.save(&output.join(MANIFEST))?;
        sync_directory(&state_output.join("content"))?;
        sync_directory(&state_output.join("quarantine"))?;
        sync_directory(&root_output)?;
        sync_directory(&state_output)?;
        sync_directory(&output)?;
        sync_directory(&parent)?;
        Ok(())
    }

    pub(super) fn load(backup: &Path) -> Result<Self> {
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
            return Err(Error::unsupported("unsupported backup manifest profile"));
        }
        manifest.validate_shape()?;
        Ok(manifest)
    }

    fn validate_shape(&self) -> Result<()> {
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
        validate_nodes(&self.root, "backup root")
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

    fn validate_package(&self, backup: &Path) -> Result<()> {
        let mut identities = IdentityMap::new();
        self.validate_databases(backup)?;
        self.validate_local_state(backup)?;
        let root = snapshot(&backup.join("root"))?;
        let root = root
            .nodes()
            .values()
            .map(SnapshotNode::from_local)
            .collect::<Vec<_>>();
        verify_snapshot(&self.root, &root, &mut identities, "backup root")?;
        for artifact in &self.artifacts {
            let path = backup.join("state/quarantine").join(&artifact.name);
            if !artifact.present {
                match fs::symlink_metadata(&path) {
                    Ok(_) => {
                        return Err(Error::corrupt(format!(
                            "failed artifact {} unexpectedly exists in the backup",
                            artifact.id
                        )));
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                    Err(error) => {
                        return Err(Error::io("inspect absent backup artifact", &path, error));
                    }
                }
            }
            let actual = snapshot(&path)?;
            let actual = actual
                .nodes()
                .values()
                .map(SnapshotNode::from_local)
                .collect::<Vec<_>>();
            verify_snapshot(
                &artifact.nodes,
                &actual,
                &mut identities,
                &format!("backup artifact {}", artifact.id),
            )?;
        }
        Ok(())
    }

    fn validate_local_state(&self, backup: &Path) -> Result<()> {
        let store = Store::open_read_only(&backup.join("state/replica.db"))?;
        let binding = store
            .binding()?
            .ok_or_else(|| Error::invalid("backup replica state has no binding"))?;
        if binding.filesystem != self.filesystem()
            || binding.root != self.binding.root
            || binding.root_dev != self.binding.root_dev
            || binding.root_ino != self.binding.root_ino
            || binding.state_dev != self.binding.state_dev
            || binding.state_ino != self.binding.state_ino
        {
            return Err(Error::corrupt(
                "backup replica binding does not match its manifest",
            ));
        }
        if !store.incomplete_installs()?.is_empty() {
            return Err(Error::invalid(
                "backup replica state contains an incomplete local install",
            ));
        }
        Ok(())
    }

    fn validate_databases(&self, backup: &Path) -> Result<()> {
        let expected = self
            .databases
            .iter()
            .map(|database| (database.path.as_str(), database))
            .collect::<BTreeMap<_, _>>();
        if expected.keys().copied().collect::<BTreeSet<_>>()
            != DATABASES.into_iter().collect::<BTreeSet<_>>()
        {
            return Err(Error::corrupt(
                "backup manifest does not contain the expected SQLite databases",
            ));
        }
        for relative in DATABASES {
            let evidence = expected[relative];
            let path = backup.join(relative);
            let (length, hash) = hash_file(&path)?;
            if length != evidence.length || hash != evidence.hash {
                return Err(Error::corrupt(format!(
                    "backup SQLite database {relative} does not match its manifest"
                )));
            }
        }
        Ok(())
    }

    pub(super) fn copy_to(&self, backup: &Path, root: &Path, state: &Path) -> Result<()> {
        self.validate_package(backup)?;
        let backup = fs::canonicalize(backup)
            .map_err(|error| Error::io("canonicalize backup package", backup, error))?;
        let root = canonical_target(root)?;
        let state = canonical_target(state)?;
        if root == state {
            return Err(Error::invalid(
                "restore root and state destinations must be different",
            ));
        }
        if root.starts_with(&backup) || backup.starts_with(&root) {
            return Err(Error::invalid("restore root overlaps the backup package"));
        }
        if state.starts_with(&backup) || backup.starts_with(&state) {
            return Err(Error::invalid("restore state overlaps the backup package"));
        }
        if root.starts_with(&state) || state.starts_with(&root) {
            return Err(Error::invalid(
                "restore root and state must not contain each other",
            ));
        }
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
        sync_directory(&root)?;
        sync_directory(&state)?;
        let root_identity =
            fs::metadata(&root).map_err(|error| Error::io("inspect restore root", &root, error))?;
        let state_identity = fs::metadata(&state)
            .map_err(|error| Error::io("inspect restore state", &state, error))?;
        if root_identity.dev() != state_identity.dev() {
            return Err(Error::unsupported(
                "restore root and state must be on the same filesystem",
            ));
        }
        let mut seen = BTreeMap::new();
        copy_snapshot(&backup.join("root"), &root, &self.root, &mut seen)?;
        sync_snapshot_directories(&root, &self.root)?;
        fs::create_dir_all(state.join("content"))
            .map_err(|error| Error::io("create restored content state", &state, error))?;
        fs::create_dir(state.join("quarantine"))
            .map_err(|error| Error::io("create restored artifacts", &state, error))?;
        for artifact in &self.artifacts {
            if !artifact.present {
                continue;
            }
            copy_snapshot(
                &backup.join("state/quarantine").join(&artifact.name),
                &state.join("quarantine").join(&artifact.name),
                &artifact.nodes,
                &mut seen,
            )?;
            sync_snapshot_directories(
                &state.join("quarantine").join(&artifact.name),
                &artifact.nodes,
            )?;
        }
        for relative in DATABASES {
            let source = backup.join(relative);
            let destination = state.join(relative.trim_start_matches("state/"));
            copy_file(&source, &destination)?;
        }
        sync_directory(&state.join("content"))?;
        sync_directory(&state.join("quarantine"))?;
        sync_directory(&root)?;
        sync_directory(&state)?;
        Ok(())
    }

    pub(super) fn apply(&self, replica: &mut Replica, binding: &Binding) -> Result<()> {
        let root = replica.scan()?;
        let actual_root = root
            .nodes()
            .values()
            .map(SnapshotNode::from_local)
            .collect::<Vec<_>>();
        let mut identities = IdentityMap::new();
        verify_snapshot(&self.root, &actual_root, &mut identities, "restored root")?;
        for artifact in &self.artifacts {
            let artifact_path = replica.artifact_path(&artifact.id)?;
            let actual = match fs::symlink_metadata(&artifact_path) {
                Err(error) if !artifact.present && error.kind() == std::io::ErrorKind::NotFound => {
                    Vec::new()
                }
                Err(error) => {
                    return Err(Error::io(
                        "inspect restored retained artifact",
                        &artifact_path,
                        error,
                    ));
                }
                Ok(_) => replica
                    .scan_artifact(&artifact.name)?
                    .values()
                    .map(SnapshotNode::from_local)
                    .collect::<Vec<_>>(),
            };
            if !artifact.present {
                if !actual.is_empty() {
                    return Err(Error::corrupt(format!(
                        "failed artifact {} unexpectedly exists after restore",
                        artifact.id
                    )));
                }
                continue;
            }
            verify_snapshot(
                &artifact.nodes,
                &actual,
                &mut identities,
                &format!("restored artifact {}", artifact.id),
            )?;
        }
        identities.insert(
            (self.binding.state_dev, self.binding.state_ino),
            (binding.state_dev, binding.state_ino),
        );
        replica.store_mut().rebind(binding, &identities)
    }
}

fn snapshot(root: &Path) -> Result<LocalSnapshot> {
    scan_path(root)
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
            fs::create_dir_all(&target)
                .map_err(|error| Error::io("create backup directory", &target, error))?;
            fs::set_permissions(&target, fs::Permissions::from_mode(0o700))
                .map_err(|error| Error::io("set backup directory mode", &target, error))?;
            continue;
        }
        if let Some(parent) = target.parent() {
            fs::create_dir_all(parent)
                .map_err(|error| Error::io("create backup parent", parent, error))?;
        }
        let source_path = if node.path.is_empty() {
            source.to_owned()
        } else {
            source.join(&node.path)
        };
        let key = node.identity();
        if let Some(existing) = seen.get(&key) {
            fs::hard_link(existing, &target)
                .map_err(|error| Error::io("preserve backup hard link", &target, error))?;
        } else {
            copy_file(&source_path, &target)?;
            seen.insert(key, target.clone());
        }
        let mode = if node.executable { 0o700 } else { 0o600 };
        fs::set_permissions(&target, fs::Permissions::from_mode(mode))
            .map_err(|error| Error::io("set backup file mode", &target, error))?;
        File::open(&target)
            .and_then(|file| file.sync_all())
            .map_err(|error| Error::io("persist backup file mode", &target, error))?;
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

fn copy_file(source: &Path, destination: &Path) -> Result<()> {
    let mut input =
        File::open(source).map_err(|error| Error::io("open backup source", source, error))?;
    let mut output = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(destination)
        .map_err(|error| Error::io("create backup destination", destination, error))?;
    std::io::copy(&mut input, &mut output)
        .map_err(|error| Error::io("copy backup file", destination, error))?;
    output
        .sync_all()
        .map_err(|error| Error::io("persist backup file", destination, error))
}

fn create_private_directory(path: &Path, operation: &str) -> Result<()> {
    fs::DirBuilder::new()
        .mode(0o700)
        .create(path)
        .map_err(|error| Error::io(operation, path, error))
}

fn vacuum(source: &Path, destination: &Path) -> Result<()> {
    let connection = Connection::open(source)
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

fn verify_snapshot(
    expected: &[SnapshotNode],
    actual: &[SnapshotNode],
    identities: &mut IdentityMap,
    context: &str,
) -> Result<()> {
    let expected = expected
        .iter()
        .map(|node| (node.path.as_str(), node))
        .collect::<BTreeMap<_, _>>();
    let actual = actual
        .iter()
        .map(|node| (node.path.as_str(), node))
        .collect::<BTreeMap<_, _>>();
    if expected.keys().copied().collect::<BTreeSet<_>>()
        != actual.keys().copied().collect::<BTreeSet<_>>()
    {
        return Err(Error::corrupt(format!(
            "{context} paths do not match the backup manifest"
        )));
    }
    let mut reverse = BTreeMap::new();
    for (path, expected) in expected {
        let actual = actual[path];
        if !expected.same_content(actual) {
            return Err(Error::corrupt(format!(
                "{context} path {path:?} does not match the backup manifest"
            )));
        }
        let old = (expected.dev, expected.ino);
        let new = (actual.dev, actual.ino);
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn restore_rejects_an_unknown_component_profile_before_creating_destinations() {
        let temp = tempfile::tempdir().unwrap();
        let backup = temp.path().join("backup");
        fs::create_dir_all(backup.join("state/content")).unwrap();
        fs::create_dir_all(backup.join("state/quarantine")).unwrap();
        {
            let database = rusqlite::Connection::open(backup.join("state/replica.db")).unwrap();
            database
                .execute_batch(
                    "CREATE TABLE binding(
                         singleton INTEGER PRIMARY KEY,
                         profile TEXT NOT NULL,
                         filesystem BLOB NOT NULL,
                         root TEXT NOT NULL,
                         root_dev BLOB NOT NULL,
                         root_ino BLOB NOT NULL,
                         state_dev BLOB NOT NULL,
                         state_ino BLOB NOT NULL
                     );
                     INSERT INTO binding VALUES(
                         1,'yinyang-directory-replica-99',zeroblob(16),'/old',1,2,1,3
                     );",
                )
                .unwrap();
        }
        fs::write(backup.join("state/sync.db"), b"not reached").unwrap();
        fs::write(backup.join("state/content/staging.db"), b"not reached").unwrap();
        let databases = DATABASES
            .into_iter()
            .map(|path| {
                let (length, hash) = hash_file(&backup.join(path)).unwrap();
                DatabaseEvidence {
                    path: path.to_owned(),
                    length,
                    hash,
                }
            })
            .collect();
        let manifest = BackupManifest {
            magic: MAGIC,
            version: VERSION,
            filesystem: [1; 16],
            binding: BackupBinding {
                root: "/old".to_owned(),
                root_dev: 1,
                root_ino: 2,
                state_dev: 1,
                state_ino: 3,
            },
            root: Vec::new(),
            artifacts: Vec::new(),
            databases,
        };
        let root = temp.path().join("restored-root");
        let state = temp.path().join("restored-state");
        let error = manifest
            .copy_to(&backup, &root, &state)
            .expect_err("unsupported component profile was accepted");
        assert_eq!(error.kind(), crate::replica::ErrorKind::Unsupported);
        assert!(!root.exists());
        assert!(!state.exists());
    }

    #[test]
    fn manifest_path_traversal_is_rejected_before_restore_io() {
        let temp = tempfile::tempdir().unwrap();
        let outside = temp.path().join("outside");
        fs::create_dir(&outside).unwrap();
        fs::write(outside.join("sentinel"), b"keep").unwrap();
        let target = temp.path().join("target");
        let manifest = BackupManifest {
            magic: MAGIC,
            version: VERSION,
            filesystem: [1; 16],
            binding: BackupBinding {
                root: "/original".to_owned(),
                root_dev: 1,
                root_ino: 2,
                state_dev: 1,
                state_ino: 3,
            },
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
                name: "../outside".to_owned(),
                present: true,
                nodes: vec![SnapshotNode {
                    path: String::new(),
                    directory: false,
                    length: Some(1),
                    digest: Some([0; 32]),
                    executable: false,
                    dev: 1,
                    ino: 4,
                    parent_dev: None,
                    parent_ino: None,
                }],
            }],
            databases: Vec::new(),
        };
        manifest.save(&temp.path().join(MANIFEST)).unwrap();
        let error = match BackupManifest::load(temp.path()) {
            Ok(_) => panic!("unsafe manifest was accepted"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("unsafe artifact name"));
        assert!(!target.exists());
        assert_eq!(fs::read(outside.join("sentinel")).unwrap(), b"keep");
    }
}
