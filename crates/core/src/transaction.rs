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

use crate::data::PreparedContent;
use crate::namespace::{DirectoryEntry, Link, Node, NodeKind, encode, entry_key};
use crate::snapshot::{Change, Snapshot};
use crate::{CommitId, Error, NodeId, Result};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug)]
enum Mutation {
    Create {
        id: NodeId,
        parent: NodeId,
        name: String,
        file: Option<PreparedContent>,
        executable: bool,
    },
    Content(NodeId, PreparedContent),
    Executable(NodeId, bool),
    Rename(NodeId, NodeId, String),
    Remove(NodeId),
}
impl Mutation {
    fn logical(&self) -> Result<Vec<u8>> {
        Ok(match self {
            Self::Create {
                id,
                parent,
                name,
                file,
                executable,
            } => encode(&(
                0_u8,
                *id.as_bytes(),
                *parent.as_bytes(),
                name,
                file.as_ref()
                    .map(|f| (f.content_id().length(), *f.content_id().digest())),
                *executable,
            ))?,
            Self::Content(id, file) => encode(&(
                1_u8,
                *id.as_bytes(),
                file.content_id().length(),
                *file.content_id().digest(),
            ))?,
            Self::Executable(id, value) => encode(&(2_u8, *id.as_bytes(), *value))?,
            Self::Rename(id, parent, name) => {
                encode(&(3_u8, *id.as_bytes(), *parent.as_bytes(), name))?
            }
            Self::Remove(id) => encode(&(4_u8, *id.as_bytes()))?,
        })
    }
}
const KIND: u8 = 0;
const STATE: u8 = 1;
const LINK: u8 = 2;
const MEMBERSHIP: u8 = 3;
const LOGICAL: u8 = 4;
const ENTRY: u8 = 5;

#[derive(Clone, Debug)]
struct Condition {
    key: Vec<u8>,
    expected: Option<Vec<u8>>,
}
impl Condition {
    async fn matches(&self, snapshot: &Snapshot) -> Result<bool> {
        let actual = if self.key[0] == ENTRY {
            snapshot
                .entry(&self.key[1..])
                .await?
                .map(|v| v.encode())
                .transpose()?
        } else {
            let id = NodeId::from_bytes(
                self.key[1..]
                    .try_into()
                    .expect("private condition identity"),
            );
            part(snapshot.node(id).await?.as_ref(), self.key[0])?
        };
        Ok(actual == self.expected)
    }
}
/// Frozen logical intent. Retries preserve conditions, identities and prepared bytes.
#[derive(Clone, Debug)]
pub struct Transaction {
    filesystem: NodeId,
    base: crate::Revision,
    id: CommitId,
    digest: [u8; 32],
    conditions: Vec<Condition>,
    mutations: Vec<Mutation>,
}
impl Transaction {
    pub const fn id(&self) -> CommitId {
        self.id
    }
    pub const fn digest(&self) -> [u8; 32] {
        self.digest
    }
    fn computed_digest(&self) -> Result<[u8; 32]> {
        let bytes = encode(&(
            *b"YYREQ002",
            *self.filesystem.as_bytes(),
            self.conditions
                .iter()
                .map(|c| (&c.key, &c.expected))
                .collect::<Vec<_>>(),
            self.mutations
                .iter()
                .map(Mutation::logical)
                .collect::<Result<Vec<_>>>()?,
        ))?;
        Ok(blake3::derive_key(
            "Apache OpenDAL YinYang request profile 2",
            &bytes,
        ))
    }
    pub(crate) fn validate(&self, filesystem: NodeId) -> Result<()> {
        if self.filesystem != filesystem || self.computed_digest()? != self.digest {
            return Err(Error::invalid(
                "commit request",
                "filesystem or canonical digest mismatch",
            ));
        }
        Ok(())
    }
    pub(crate) async fn apply(&self, snapshot: &Snapshot) -> Result<Option<Delta>> {
        for condition in &self.conditions {
            if !condition.matches(snapshot).await? {
                return Ok(None);
            }
        }
        let mut work = Working::new(snapshot.clone(), false);
        for mutation in &self.mutations {
            work.apply(mutation).await?;
        }
        work.finish().await.map(Some)
    }
}

type RequestWire = (
    [u8; 8],
    [u8; 16],
    [u8; 16],
    [u8; 24],
    [u8; 32],
    Vec<(Vec<u8>, Option<Vec<u8>>)>,
    Vec<Vec<u8>>,
);

impl Transaction {
    /// Persist the exact plan, not permission to trust its content descriptors.
    /// Restore through the selected authority before submitting after a restart.
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        let mutations = self
            .mutations
            .iter()
            .map(|m| {
                Ok(match m {
                    Mutation::Create {
                        id,
                        parent,
                        name,
                        file,
                        executable,
                    } => encode(&(
                        0_u8,
                        *id.as_bytes(),
                        *parent.as_bytes(),
                        name,
                        file.as_ref().map(|f| f.descriptor().to_bytes()),
                        executable,
                    ))?,
                    Mutation::Content(id, file) => {
                        encode(&(1_u8, *id.as_bytes(), file.descriptor().to_bytes()))?
                    }
                    Mutation::Executable(id, value) => encode(&(2_u8, *id.as_bytes(), value))?,
                    Mutation::Rename(id, parent, name) => {
                        encode(&(3_u8, *id.as_bytes(), *parent.as_bytes(), name))?
                    }
                    Mutation::Remove(id) => encode(&(4_u8, *id.as_bytes()))?,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        encode(&(
            *b"YYPLAN01",
            *self.filesystem.as_bytes(),
            *self.id.as_bytes(),
            self.base.to_bytes(),
            self.digest,
            self.conditions
                .iter()
                .map(|c| (&c.key, &c.expected))
                .collect::<Vec<_>>(),
            mutations,
        ))
    }

    pub(crate) fn inspect(
        bytes: &[u8],
    ) -> Result<(crate::Revision, Vec<crate::ContentDescriptor>)> {
        let (_, _, _, revision, _, _, mutations) = Self::wire(bytes)?;
        let mut files = Vec::new();
        for bytes in mutations {
            match bytes.first() {
                Some(0) => {
                    let (_, _, _, _, file, _): (
                        u8,
                        [u8; 16],
                        [u8; 16],
                        String,
                        Option<Vec<u8>>,
                        bool,
                    ) = crate::namespace::decode(&bytes)?;
                    if let Some(file) = file {
                        files.push(crate::ContentDescriptor::from_bytes(&file)?);
                    }
                }
                Some(1) => {
                    let (_, _, file): (u8, [u8; 16], Vec<u8>) = crate::namespace::decode(&bytes)?;
                    files.push(crate::ContentDescriptor::from_bytes(&file)?);
                }
                _ => {}
            }
        }
        Ok((crate::Revision::from_bytes(revision), files))
    }
    fn wire(bytes: &[u8]) -> Result<RequestWire> {
        if bytes.len() > 16 * 1024 * 1024 {
            return Err(Error::unsupported(
                "restore transaction",
                "plan exceeds 16 MiB",
            ));
        }
        let wire: RequestWire = crate::namespace::decode(bytes)?;
        if wire.0 != *b"YYPLAN01" {
            return Err(Error::invalid(
                "restore transaction",
                "unknown plan profile",
            ));
        }
        Ok(wire)
    }
    pub(crate) async fn restore(
        bytes: &[u8],
        snapshot: &Snapshot,
        prepared: &[PreparedContent],
    ) -> Result<Self> {
        let (_, fs, id, revision, digest, conditions, encoded) = Self::wire(bytes)?;
        if fs != *snapshot.filesystem.as_bytes() || revision != snapshot.revision().to_bytes() {
            return Err(Error::invalid(
                "restore transaction",
                "wrong authority or base revision",
            ));
        }
        let mut map = BTreeMap::new();
        for (key, expected) in conditions {
            if key.is_empty()
                || key[0] > ENTRY
                || (key[0] != ENTRY && key.len() != 17)
                || (key[0] == ENTRY && key.len() < 18)
                || map.contains_key(&key)
            {
                return Err(Error::invalid(
                    "restore transaction",
                    "invalid or duplicate predicate",
                ));
            }
            let condition = Condition {
                key: key.clone(),
                expected,
            };
            if !condition.matches(snapshot).await? {
                return Err(Error::invalid(
                    "restore transaction",
                    "predicate disagrees with original observation",
                ));
            }
            map.insert(key, condition);
        }
        let find = |bytes: Vec<u8>| -> Result<PreparedContent> {
            prepared
                .iter()
                .find(|p| p.descriptor().to_bytes() == bytes)
                .cloned()
                .ok_or_else(|| {
                    Error::invalid("restore transaction", "content readiness not established")
                })
        };
        let mut work = Working::new(snapshot.clone(), true);
        let mut mutations = Vec::new();
        for bytes in encoded {
            let mutation = match bytes.first() {
                Some(0) => {
                    let (_, node, parent, name, file, executable): (
                        u8,
                        [u8; 16],
                        [u8; 16],
                        String,
                        Option<Vec<u8>>,
                        bool,
                    ) = crate::namespace::decode(&bytes)?;
                    let seed = encode(&(fs, id, mutations.len() as u64))?;
                    let derived =
                        blake3::derive_key("Apache OpenDAL YinYang node identity profile 2", &seed);
                    if node != derived[..16] {
                        return Err(Error::invalid(
                            "restore transaction",
                            "invalid creation identity",
                        ));
                    }
                    Mutation::Create {
                        id: NodeId::from_bytes(node),
                        parent: NodeId::from_bytes(parent),
                        name,
                        file: file.map(&find).transpose()?,
                        executable,
                    }
                }
                Some(1) => {
                    let (_, node, file): (u8, [u8; 16], Vec<u8>) =
                        crate::namespace::decode(&bytes)?;
                    Mutation::Content(NodeId::from_bytes(node), find(file)?)
                }
                Some(2) => {
                    let (_, node, value): (u8, [u8; 16], bool) = crate::namespace::decode(&bytes)?;
                    Mutation::Executable(NodeId::from_bytes(node), value)
                }
                Some(3) => {
                    let (_, node, parent, name): (u8, [u8; 16], [u8; 16], String) =
                        crate::namespace::decode(&bytes)?;
                    Mutation::Rename(NodeId::from_bytes(node), NodeId::from_bytes(parent), name)
                }
                Some(4) => {
                    let (_, node): (u8, [u8; 16]) = crate::namespace::decode(&bytes)?;
                    Mutation::Remove(NodeId::from_bytes(node))
                }
                _ => return Err(Error::invalid("restore transaction", "unknown mutation")),
            };
            work.apply(&mutation).await?;
            mutations.push(mutation);
        }
        // Lower-level clients may add dependencies, but cannot omit the guards
        // required by the same planner used by trusted in-process callers.
        for required in work.conditions.values() {
            if map
                .get(&required.key)
                .is_none_or(|c| c.expected != required.expected)
            {
                return Err(Error::invalid(
                    "restore transaction",
                    "missing mandatory predicate",
                ));
            }
        }
        let request = Self {
            filesystem: snapshot.filesystem,
            base: snapshot.revision(),
            id: CommitId::from_bytes(id),
            digest,
            conditions: map.into_values().collect(),
            mutations,
        };
        request.validate(snapshot.filesystem)?;
        Ok(request)
    }
}

/// Dependency-capturing operation planner; it never runs caller callbacks on retry.
pub struct Planner {
    id: CommitId,
    work: Working,
    mutations: Vec<Mutation>,
}
impl Planner {
    pub fn new(snapshot: &Snapshot, id: CommitId) -> Self {
        Self {
            id,
            work: Working::new(snapshot.clone(), true),
            mutations: Vec::new(),
        }
    }
    pub async fn node(&mut self, id: NodeId) -> Result<Option<Node>> {
        self.work.guard(id, LOGICAL).await?;
        self.work.node(id).await
    }
    pub async fn lookup(&mut self, parent: NodeId, name: &str) -> Result<Option<DirectoryEntry>> {
        self.work.directory(parent).await?;
        let key = entry_key(parent, name)?;
        self.work.guard_entry(&key).await?;
        self.work.entry(&key).await
    }
    pub async fn resolve(&mut self, path: &str) -> Result<Option<NodeId>> {
        let mut id = self.work.base.root();
        if !path.is_empty() {
            for name in path.split('/') {
                let Some(entry) = self.lookup(id, name).await? else {
                    return Ok(None);
                };
                id = entry.node_id;
            }
        }
        Ok(Some(id))
    }
    /// Reading the complete mapping captures a membership predicate, including phantoms.
    pub async fn scan(&mut self, parent: NodeId) -> Result<Vec<DirectoryEntry>> {
        self.work.directory(parent).await?;
        self.work.guard(parent, MEMBERSHIP).await?;
        self.work.list(parent).await
    }
    pub async fn create_directory(&mut self, parent: NodeId, name: &str) -> Result<NodeId> {
        self.create(parent, name, None, false).await
    }
    pub async fn create_file(
        &mut self,
        parent: NodeId,
        name: &str,
        file: PreparedContent,
        executable: bool,
    ) -> Result<NodeId> {
        self.create(parent, name, Some(file), executable).await
    }
    async fn create(
        &mut self,
        parent: NodeId,
        name: &str,
        file: Option<PreparedContent>,
        executable: bool,
    ) -> Result<NodeId> {
        let seed = encode(&(
            *self.work.base.filesystem.as_bytes(),
            *self.id.as_bytes(),
            self.mutations.len() as u64,
        ))?;
        let digest = blake3::derive_key("Apache OpenDAL YinYang node identity profile 2", &seed);
        let id = NodeId::from_bytes(digest[..16].try_into().unwrap());
        self.push(Mutation::Create {
            id,
            parent,
            name: name.to_owned(),
            file,
            executable,
        })
        .await?;
        Ok(id)
    }
    pub async fn set_content(&mut self, id: NodeId, file: PreparedContent) -> Result<()> {
        self.push(Mutation::Content(id, file)).await
    }
    pub async fn set_executable(&mut self, id: NodeId, value: bool) -> Result<()> {
        self.push(Mutation::Executable(id, value)).await
    }
    /// Destination must be absent, or the same normalized slot of the moved node.
    pub async fn rename(&mut self, id: NodeId, parent: NodeId, name: &str) -> Result<()> {
        self.push(Mutation::Rename(id, parent, name.to_owned()))
            .await
    }
    pub async fn remove(&mut self, id: NodeId) -> Result<()> {
        self.push(Mutation::Remove(id)).await
    }
    async fn push(&mut self, mutation: Mutation) -> Result<()> {
        self.work.apply(&mutation).await?;
        self.mutations.push(mutation);
        Ok(())
    }
    pub fn finish(self) -> Result<Transaction> {
        let mut request = Transaction {
            filesystem: self.work.base.filesystem,
            base: self.work.base.revision(),
            id: self.id,
            digest: [0; 32],
            conditions: self.work.conditions.into_values().collect(),
            mutations: self.mutations,
        };
        request.digest = request.computed_digest()?;
        Ok(request)
    }
}
struct Working {
    base: Snapshot,
    capture: bool,
    conditions: BTreeMap<Vec<u8>, Condition>,
    nodes: BTreeMap<NodeId, Option<Node>>,
    original_nodes: BTreeMap<NodeId, Option<Node>>,
    entries: BTreeMap<Vec<u8>, Option<DirectoryEntry>>,
    original_entries: BTreeMap<Vec<u8>, Option<DirectoryEntry>>,
}
impl Working {
    fn new(base: Snapshot, capture: bool) -> Self {
        Self {
            base,
            capture,
            conditions: BTreeMap::new(),
            nodes: BTreeMap::new(),
            original_nodes: BTreeMap::new(),
            entries: BTreeMap::new(),
            original_entries: BTreeMap::new(),
        }
    }
    async fn original(&mut self, id: NodeId) -> Result<Option<Node>> {
        if !self.original_nodes.contains_key(&id) {
            self.original_nodes.insert(id, self.base.node(id).await?);
        }
        Ok(self.original_nodes[&id].clone())
    }
    async fn node(&mut self, id: NodeId) -> Result<Option<Node>> {
        if let Some(node) = self.nodes.get(&id) {
            return Ok(node.clone());
        }
        self.original(id).await
    }
    async fn required(&mut self, id: NodeId) -> Result<Node> {
        self.node(id).await?.ok_or_else(|| {
            Error::new(
                crate::ErrorKind::NotFound,
                "plan mutation",
                "node is absent",
            )
        })
    }
    async fn directory(&mut self, id: NodeId) -> Result<Node> {
        self.guard(id, KIND).await?;
        let node = self.required(id).await?;
        if !node.is_directory() {
            return Err(Error::new(
                crate::ErrorKind::NotDirectory,
                "plan mutation",
                "parent is not a directory",
            ));
        }
        Ok(node)
    }
    async fn guard(&mut self, id: NodeId, tag: u8) -> Result<()> {
        if !self.capture {
            return Ok(());
        }
        let original = self.original(id).await?;
        // New identities are already protected by their creation absence predicate.
        if original.is_none() && self.nodes.contains_key(&id) {
            return Ok(());
        }
        let mut key = vec![tag];
        key.extend(id.as_bytes());
        let expected = part(original.as_ref(), tag)?;
        self.conditions
            .entry(key.clone())
            .or_insert(Condition { key, expected });
        Ok(())
    }
    async fn entry(&mut self, key: &[u8]) -> Result<Option<DirectoryEntry>> {
        if let Some(entry) = self.entries.get(key) {
            return Ok(entry.clone());
        }
        self.original_entry(key).await
    }
    async fn original_entry(&mut self, key: &[u8]) -> Result<Option<DirectoryEntry>> {
        if !self.original_entries.contains_key(key) {
            self.original_entries
                .insert(key.to_vec(), self.base.entry(key).await?);
        }
        Ok(self.original_entries[key].clone())
    }
    async fn guard_entry(&mut self, key: &[u8]) -> Result<()> {
        let expected = self
            .original_entry(key)
            .await?
            .map(|v| v.encode())
            .transpose()?;
        if self.capture {
            let mut condition_key = vec![ENTRY];
            condition_key.extend(key);
            self.conditions
                .entry(condition_key.clone())
                .or_insert(Condition {
                    key: condition_key,
                    expected,
                });
        }
        Ok(())
    }
    async fn list(&mut self, parent: NodeId) -> Result<Vec<DirectoryEntry>> {
        let mut map = BTreeMap::new();
        if self.original(parent).await?.is_some() {
            let mut token = None;
            loop {
                let page = self.base.scan(parent, token.as_ref(), 4096).await?;
                for entry in page.entries {
                    map.insert(entry_key(parent, &entry.name)?, entry);
                }
                token = page.next;
                if token.is_none() {
                    break;
                }
            }
        }
        for (key, value) in self.entries.range(parent.as_bytes().to_vec()..) {
            if !key.starts_with(parent.as_bytes()) {
                break;
            }
            match value {
                Some(value) => {
                    map.insert(key.clone(), value.clone());
                }
                None => {
                    map.remove(key);
                }
            }
        }
        Ok(map.into_values().collect())
    }
    async fn apply(&mut self, mutation: &Mutation) -> Result<()> {
        match mutation {
            Mutation::Create {
                id,
                parent,
                name,
                file,
                executable,
            } => {
                let key = entry_key(*parent, name)?;
                self.directory(*parent).await?;
                self.guard_entry(&key).await?;
                self.guard(*id, KIND).await?;
                if self.entry(&key).await?.is_some() || self.node(*id).await?.is_some() {
                    return Err(Error::new(
                        crate::ErrorKind::AlreadyExists,
                        "create node",
                        "name or identity exists",
                    ));
                }
                let kind = match file {
                    Some(file) => NodeKind::File(self.base.data.accept(file)?),
                    None => NodeKind::Directory { membership: 1 },
                };
                let node = Node {
                    id: *id,
                    generation: 1,
                    executable: *executable,
                    link: Some(Link {
                        parent: *parent,
                        name: name.clone(),
                    }),
                    kind,
                };
                self.nodes.insert(*id, Some(node));
                self.entries.insert(
                    key,
                    Some(DirectoryEntry {
                        name: name.clone(),
                        node_id: *id,
                    }),
                );
            }
            Mutation::Content(id, file) => {
                self.guard(*id, STATE).await?;
                let mut node = self.required(*id).await?;
                if node.is_directory() {
                    return Err(Error::new(
                        crate::ErrorKind::IsDirectory,
                        "set content",
                        "node is a directory",
                    ));
                }
                node.kind = NodeKind::File(self.base.data.accept(file)?);
                self.nodes.insert(*id, Some(node));
            }
            Mutation::Executable(id, value) => {
                self.guard(*id, STATE).await?;
                let mut node = self.required(*id).await?;
                node.executable = *value;
                self.nodes.insert(*id, Some(node));
            }
            Mutation::Rename(id, parent, name) => {
                let destination = entry_key(*parent, name)?;
                self.guard(*id, LINK).await?;
                let mut node = self.required(*id).await?;
                let source = node
                    .link
                    .clone()
                    .ok_or_else(|| Error::invalid("rename", "root cannot move"))?;
                let source_key = entry_key(source.parent, &source.name)?;
                self.guard_entry(&source_key).await?;
                self.guard_entry(&destination).await?;
                self.directory(*parent).await?;
                if self
                    .entry(&destination)
                    .await?
                    .is_some_and(|v| v.node_id != *id)
                {
                    return Err(Error::new(
                        crate::ErrorKind::AlreadyExists,
                        "rename",
                        "destination exists",
                    ));
                }
                if node.is_directory() {
                    let mut ancestor = *parent;
                    let mut seen = BTreeSet::new();
                    loop {
                        if ancestor == *id || !seen.insert(ancestor) {
                            return Err(Error::invalid("rename", "directory move creates a cycle"));
                        }
                        self.guard(ancestor, LINK).await?;
                        let ancestor_node = self.directory(ancestor).await?;
                        match ancestor_node.link {
                            Some(link) => ancestor = link.parent,
                            None => break,
                        }
                    }
                }
                node.link = Some(Link {
                    parent: *parent,
                    name: name.clone(),
                });
                self.nodes.insert(*id, Some(node));
                self.entries.insert(source_key, None);
                self.entries.insert(
                    destination,
                    Some(DirectoryEntry {
                        name: name.clone(),
                        node_id: *id,
                    }),
                );
            }
            Mutation::Remove(id) => {
                self.guard(*id, STATE).await?;
                self.guard(*id, LINK).await?;
                let node = self.required(*id).await?;
                let link = node
                    .link
                    .ok_or_else(|| Error::invalid("remove", "root cannot be removed"))?;
                let key = entry_key(link.parent, &link.name)?;
                self.guard_entry(&key).await?;
                if matches!(node.kind, NodeKind::Directory { .. }) {
                    self.guard(*id, MEMBERSHIP).await?;
                    if !self.list(*id).await?.is_empty() {
                        return Err(Error::new(
                            crate::ErrorKind::NotEmpty,
                            "remove",
                            "directory is not empty",
                        ));
                    }
                }
                self.nodes.insert(*id, None);
                self.entries.insert(key, None);
            }
        }
        Ok(())
    }
    async fn finish(mut self) -> Result<Delta> {
        let mut changed_parents = BTreeSet::new();
        for (key, entry) in &self.entries {
            if self
                .original_entries
                .get(key)
                .expect("entry write is guarded")
                != entry
            {
                changed_parents.insert(NodeId::from_bytes(key[..16].try_into().unwrap()));
            }
        }
        for parent in changed_parents {
            if let Some(mut node) = self.node(parent).await? {
                if let Some(old) = self.original(parent).await? {
                    let NodeKind::Directory { membership } = old.kind else {
                        unreachable!()
                    };
                    node.kind = NodeKind::Directory {
                        membership: increment(membership)?,
                    };
                }
                self.nodes.insert(parent, Some(node));
            }
        }
        let mut changes = Vec::new();
        let mut nodes = Vec::new();
        let mut entries = Vec::new();
        for (id, mut node) in self.nodes {
            let before = self.original_nodes.get(&id).cloned().flatten();
            if let (Some(old), Some(node)) = (&before, &mut node) {
                let old_content = match &old.kind {
                    NodeKind::File(f) => Some(f.content_id()),
                    _ => None,
                };
                let content = match &node.kind {
                    NodeKind::File(f) => Some(f.content_id()),
                    _ => None,
                };
                if old.executable != node.executable || old_content != content {
                    node.generation = increment(old.generation)?;
                }
            }
            if before != node {
                nodes.push((
                    id.as_bytes().to_vec(),
                    node.as_ref().map(Node::encode).transpose()?,
                ));
                changes.push(Change {
                    before,
                    after: node,
                });
            }
        }
        for (key, entry) in self.entries {
            if self.original_entries.get(&key).unwrap() != &entry {
                entries.push((key, entry.as_ref().map(DirectoryEntry::encode).transpose()?));
            }
        }
        Ok(Delta {
            nodes,
            entries,
            changes,
        })
    }
}
fn part(node: Option<&Node>, tag: u8) -> Result<Option<Vec<u8>>> {
    node.map(|node| match tag {
        KIND => encode(&node.is_directory()),
        STATE => encode(&(
            node.generation,
            node.executable,
            match &node.kind {
                NodeKind::File(f) => Some((f.content_id().length(), *f.content_id().digest())),
                _ => None,
            },
        )),
        LINK => encode(
            &node
                .link
                .as_ref()
                .map(|l| (*l.parent.as_bytes(), l.name.clone())),
        ),
        MEMBERSHIP => encode(&match node.kind {
            NodeKind::Directory { membership } => membership,
            _ => 0,
        }),
        LOGICAL => node.logical(),
        _ => unreachable!(),
    })
    .transpose()
}
fn increment(value: u64) -> Result<u64> {
    value
        .checked_add(1)
        .ok_or_else(|| Error::unsupported("commit", "generation space exhausted"))
}

/// Changed records, independent of an authority's physical index encoding.
pub(crate) struct Delta {
    pub nodes: Vec<(Vec<u8>, Option<Vec<u8>>)>,
    pub entries: Vec<(Vec<u8>, Option<Vec<u8>>)>,
    pub changes: Vec<Change>,
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn canonical_noop_request_vector() {
        let request = Transaction {
            filesystem: NodeId::from_bytes([1; 16]),
            base: crate::Revision::new(0),
            id: CommitId::from_bytes([2; 16]),
            digest: [0; 32],
            conditions: vec![],
            mutations: vec![],
        };
        assert_eq!(
            blake3::Hash::from(request.computed_digest().unwrap())
                .to_hex()
                .as_str(),
            "811032fb290ade714ee0ca3d8716fabd1f7154e478ae5da1a8b822411513eb4d"
        );
        assert_eq!(
            increment(u64::MAX).unwrap_err().kind(),
            crate::ErrorKind::Unsupported
        );
    }
}
