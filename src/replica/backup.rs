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

use super::linux::{ContentHash, Identity, LocalKind, LocalNode, LocalSnapshot, Replica};
use super::state::{Binding, IdentityMap};
use super::{Error, Result};
use borsh::{BorshDeserialize, BorshSerialize};
use rusqlite::Connection;
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::fs::{MetadataExt, PermissionsExt};
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
        fs::create_dir(&output)
            .map_err(|error| Error::io("create backup destination", &output, error))?;
        let root_output = output.join("root");
        let state_output = output.join("state");
        fs::create_dir(&root_output)
            .map_err(|error| Error::io("create backup root", &root_output, error))?;
        fs::create_dir(&state_output)
            .map_err(|error| Error::io("create backup state", &state_output, error))?;

        let mut seen = BTreeMap::new();
        copy_snapshot(&root, &root_output, &self.root, &mut seen)?;
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
        sync_directory(&root_output)?;
        sync_directory(&state_output)?;
        sync_directory(&output)?;
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

    fn validate_package(&self, backup: &Path) -> Result<()> {
        let mut identities = IdentityMap::new();
        self.validate_databases(backup)?;
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
        fs::create_dir(&root).map_err(|error| Error::io("create restore root", &root, error))?;
        fs::create_dir(&state).map_err(|error| Error::io("create restore state", &state, error))?;
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
        }
        for relative in DATABASES {
            let source = backup.join(relative);
            let destination = state.join(relative.trim_start_matches("state/"));
            copy_file(&source, &destination)?;
        }
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
    let root_metadata = fs::symlink_metadata(root)
        .map_err(|error| Error::io("inspect backup tree", root, error))?;
    let root_identity = identity(&root_metadata);
    let mut nodes = BTreeMap::new();
    if root_metadata.file_type().is_file() {
        let (length, digest) = hash_file(root)?;
        nodes.insert(
            String::new(),
            LocalNode {
                path: String::new(),
                kind: LocalKind::File {
                    length,
                    digest: ContentHash::from_bytes(digest),
                },
                executable: root_metadata.permissions().mode() & 0o111 != 0,
                identity: root_identity,
                parent: None,
            },
        );
        return Ok(LocalSnapshot::from_nodes(nodes));
    }
    if !root_metadata.file_type().is_dir() {
        return Err(Error::corrupt(format!(
            "backup tree {} is a symlink or special file",
            root.display()
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
        },
    );
    scan_directory(root, "", root_identity, &mut nodes)
        .map_err(|error| Error::io("scan backup tree", root, error))?;
    Ok(LocalSnapshot::from_nodes(nodes))
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

fn scan_directory(
    directory: &Path,
    prefix: &str,
    directory_identity: Identity,
    nodes: &mut BTreeMap<String, LocalNode>,
) -> std::io::Result<()> {
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        let name = entry.file_name().into_string().map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, "non-UTF-8 filename")
        })?;
        let path = if prefix.is_empty() {
            name.clone()
        } else {
            format!("{prefix}/{name}")
        };
        let metadata = fs::symlink_metadata(entry.path())?;
        let file_type = metadata.file_type();
        if file_type.is_dir() {
            let current = identity(&metadata);
            nodes.insert(
                path.clone(),
                LocalNode {
                    path: path.clone(),
                    kind: LocalKind::Directory,
                    executable: false,
                    identity: current,
                    parent: Some(directory_identity),
                },
            );
            scan_directory(&entry.path(), &path, current, nodes)?;
        } else if file_type.is_file() {
            let (length, digest) = hash_file(&entry.path())
                .map_err(|error| std::io::Error::other(error.to_string()))?;
            nodes.insert(
                path.clone(),
                LocalNode {
                    path,
                    kind: LocalKind::File {
                        length,
                        digest: ContentHash::from_bytes(digest),
                    },
                    executable: metadata.permissions().mode() & 0o111 != 0,
                    identity: identity(&metadata),
                    parent: Some(directory_identity),
                },
            );
        } else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("{} is a symlink or special file", entry.path().display()),
            ));
        }
    }
    Ok(())
}

fn identity(metadata: &fs::Metadata) -> Identity {
    Identity {
        dev: metadata.dev(),
        ino: metadata.ino(),
    }
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
