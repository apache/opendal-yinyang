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

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::ops::Range;

use unicode_casefold::UnicodeCaseFold as _;
use unicode_normalization::UnicodeNormalization as _;

use crate::{Error, Generation, NodeId, Result};

mod edit;
pub use edit::TreeEdit;

const MAX_COMPONENT_BYTES: usize = 255;
const MAX_PATH_BYTES: usize = 4096;

/// Canonical relative filesystem path. The empty path is the root.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Path(String);

impl Path {
    pub const fn root() -> Self {
        Self(String::new())
    }

    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        validate_portable_path(&value)?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    fn is_root(&self) -> bool {
        self.0.is_empty()
    }

    pub fn parent(&self) -> Option<Self> {
        if self.is_root() {
            return None;
        }
        Some(match self.0.rsplit_once('/') {
            Some((parent, _)) => Self(parent.to_owned()),
            None => Self::root(),
        })
    }

    pub fn name(&self) -> Option<&str> {
        (!self.is_root()).then(|| {
            self.0
                .rsplit('/')
                .next()
                .expect("a non-root path has a name")
        })
    }
}

impl fmt::Display for Path {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// Stable identity of one logical byte sequence.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ContentId {
    digest: [u8; 32],
    length: u64,
}

impl ContentId {
    pub const fn new(digest: [u8; 32], length: u64) -> Self {
        Self { digest, length }
    }

    pub const fn digest(&self) -> &[u8; 32] {
        &self.digest
    }

    pub const fn length(self) -> u64 {
        self.length
    }
}

/// Opaque persistent reference to one verifiable immutable byte region.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct BlobRef {
    reference: Vec<u8>,
    content: ContentId,
}

impl BlobRef {
    pub fn new(reference: impl Into<Vec<u8>>, content: ContentId) -> Self {
        Self {
            reference: reference.into(),
            content,
        }
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.reference
    }

    pub const fn content(&self) -> ContentId {
        self.content
    }
}

/// Mapping from one logical file range into an immutable blob.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FilePart {
    range: Range<u64>,
    blob_offset: u64,
    blob: BlobRef,
}

impl FilePart {
    pub fn new(range: Range<u64>, blob_offset: u64, blob: BlobRef) -> Result<Self> {
        if range.is_empty() {
            return Err(Error::invalid(
                "construct YinYang file part",
                "file part is empty",
            ));
        }
        let length = range.end - range.start;
        if blob_offset
            .checked_add(length)
            .is_none_or(|end| end > blob.content().length())
        {
            return Err(Error::invalid(
                "construct YinYang file part",
                "file part exceeds its blob content",
            ));
        }
        Ok(Self {
            range,
            blob_offset,
            blob,
        })
    }

    pub const fn range(&self) -> &Range<u64> {
        &self.range
    }

    pub const fn blob_offset(&self) -> u64 {
        self.blob_offset
    }

    pub const fn blob(&self) -> &BlobRef {
        &self.blob
    }
}

/// One published logical file.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct File {
    content: ContentId,
    parts: Vec<FilePart>,
}

impl File {
    pub fn new(content: ContentId, parts: Vec<FilePart>) -> Result<Self> {
        let file = Self { content, parts };
        file.validate()?;
        Ok(file)
    }

    pub const fn content(&self) -> ContentId {
        self.content
    }

    pub fn parts(&self) -> &[FilePart] {
        &self.parts
    }

    fn validate(&self) -> Result<()> {
        if self.content.length() == 0 {
            if self.parts.is_empty() {
                return Ok(());
            }
            return Err(Error::invalid(
                "validate YinYang file",
                "empty file contains parts",
            ));
        }

        let mut expected_offset = 0;
        for part in &self.parts {
            if part.range.start != expected_offset {
                return Err(Error::invalid(
                    "validate YinYang file",
                    "file parts contain a gap or overlap",
                ));
            }
            expected_offset = part.range.end;
        }
        if expected_offset != self.content.length() {
            return Err(Error::invalid(
                "validate YinYang file",
                "file parts do not exactly cover the logical content",
            ));
        }
        Ok(())
    }
}

/// Node-specific filesystem state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NodeBody {
    Dir { entries_generation: Generation },
    File(File),
}

/// One stable filesystem node.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Node {
    id: NodeId,
    generation: Generation,
    executable: bool,
    body: NodeBody,
}

impl Node {
    pub const fn dir(
        id: NodeId,
        generation: Generation,
        executable: bool,
        entries_generation: Generation,
    ) -> Self {
        Self {
            id,
            generation,
            executable,
            body: NodeBody::Dir { entries_generation },
        }
    }

    pub const fn file(id: NodeId, generation: Generation, executable: bool, file: File) -> Self {
        Self {
            id,
            generation,
            executable,
            body: NodeBody::File(file),
        }
    }

    pub const fn id(&self) -> NodeId {
        self.id
    }

    pub const fn generation(&self) -> Generation {
        self.generation
    }

    pub const fn executable(&self) -> bool {
        self.executable
    }

    pub const fn body(&self) -> &NodeBody {
        &self.body
    }

    const fn dir_generation(&self) -> Option<Generation> {
        match self.body {
            NodeBody::Dir { entries_generation } => Some(entries_generation),
            NodeBody::File(_) => None,
        }
    }

    const fn file_body(&self) -> Option<&File> {
        match &self.body {
            NodeBody::Dir { .. } => None,
            NodeBody::File(file) => Some(file),
        }
    }
}

/// Materialized ordered filesystem tree.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Tree {
    entries: BTreeMap<Path, Node>,
}

impl Tree {
    pub(crate) fn genesis(root: NodeId) -> Self {
        Self {
            entries: BTreeMap::from([(
                Path::root(),
                Node::dir(root, Generation::FIRST, false, Generation::FIRST),
            )]),
        }
    }

    pub fn from_entries(entries: impl IntoIterator<Item = (Path, Node)>) -> Result<Self> {
        let mut tree = Self {
            entries: BTreeMap::new(),
        };
        for (path, node) in entries {
            if tree.entries.insert(path, node).is_some() {
                return Err(Error::invalid(
                    "construct YinYang tree",
                    "tree contains a duplicate path",
                ));
            }
        }
        Ok(tree)
    }

    pub fn get(&self, path: &Path) -> Option<&Node> {
        self.entries.get(path)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&Path, &Node)> {
        self.entries.iter()
    }

    pub fn insert(&mut self, path: Path, node: Node) -> Option<Node> {
        self.entries.insert(path, node)
    }

    pub fn remove(&mut self, path: &Path) -> Option<Node> {
        self.entries.remove(path)
    }

    pub(crate) fn root_id(&self) -> Result<NodeId> {
        let root = self
            .entries
            .get(&Path::root())
            .ok_or_else(|| Error::invalid("validate YinYang tree", "tree root is missing"))?;
        if root.dir_generation().is_none() {
            return Err(Error::invalid(
                "validate YinYang tree",
                "tree root is not a directory",
            ));
        }
        Ok(root.id)
    }

    pub(crate) fn validate(&self, root: NodeId) -> Result<()> {
        if self.root_id()? != root {
            return Err(Error::invalid(
                "validate YinYang tree",
                "tree root identity changed",
            ));
        }

        let mut identities = BTreeSet::new();
        for (path, node) in &self.entries {
            if node.generation == Generation::ZERO {
                return Err(Error::invalid(
                    "validate YinYang tree",
                    "node generation is zero",
                ));
            }
            if !identities.insert(node.id) {
                return Err(Error::invalid(
                    "validate YinYang tree",
                    "node identity appears at more than one path",
                ));
            }
            if node.dir_generation() == Some(Generation::ZERO) {
                return Err(Error::invalid(
                    "validate YinYang tree",
                    "directory entries generation is zero",
                ));
            }
            if let Some(file) = node.file_body() {
                file.validate()?;
            }
            if let Some(parent) = path.parent()
                && self
                    .entries
                    .get(&parent)
                    .and_then(Node::dir_generation)
                    .is_none()
            {
                return Err(Error::invalid(
                    "validate YinYang tree",
                    "tree entry parent is missing or is not a directory",
                ));
            }
        }
        directory_membership(self)?;
        Ok(())
    }

    pub(crate) fn validate_successor(&self, successor: &Self, root: NodeId) -> Result<()> {
        if successor.root_id()? != root {
            return Err(Error::invalid(
                "validate YinYang tree successor",
                "tree root identity changed",
            ));
        }

        let previous_by_id = nodes_by_id(self);
        let successor_by_id = nodes_by_id(successor);
        for (id, (_, next)) in &successor_by_id {
            let Some((_, previous)) = previous_by_id.get(id) else {
                if next.generation != Generation::FIRST
                    || next
                        .dir_generation()
                        .is_some_and(|generation| generation != Generation::FIRST)
                {
                    return Err(Error::invalid(
                        "validate YinYang tree successor",
                        "new node does not start at the first generation",
                    ));
                }
                continue;
            };

            if matches!(previous.body, NodeBody::Dir { .. })
                != matches!(next.body, NodeBody::Dir { .. })
            {
                return Err(Error::invalid(
                    "validate YinYang tree successor",
                    "node kind changed without a new identity",
                ));
            }
            let payload_changed = previous.executable != next.executable
                || match (&previous.body, &next.body) {
                    (NodeBody::Dir { .. }, NodeBody::Dir { .. }) => false,
                    (NodeBody::File(previous), NodeBody::File(next)) => previous != next,
                    _ => unreachable!("node kind was checked above"),
                };
            let expected = if payload_changed {
                previous.generation.next()?
            } else {
                previous.generation
            };
            if next.generation != expected {
                return Err(Error::invalid(
                    "validate YinYang tree successor",
                    "node generation does not match its content change",
                ));
            }
        }

        let previous_membership = directory_membership(self)?;
        let successor_membership = directory_membership(successor)?;
        for (id, (_, next)) in successor_by_id {
            let Some(next_generation) = next.dir_generation() else {
                continue;
            };
            let Some((_, previous)) = previous_by_id.get(&id) else {
                continue;
            };
            let previous_generation = previous
                .dir_generation()
                .expect("a stable directory identity remains a directory");
            let changed = previous_membership.get(&id) != successor_membership.get(&id);
            let expected = if changed {
                previous_generation.next()?
            } else {
                previous_generation
            };
            if next_generation != expected {
                return Err(Error::invalid(
                    "validate YinYang tree successor",
                    "directory generation does not match its membership change",
                ));
            }
        }
        Ok(())
    }
}

fn nodes_by_id(tree: &Tree) -> BTreeMap<NodeId, (&Path, &Node)> {
    tree.entries
        .iter()
        .map(|(path, node)| (node.id, (path, node)))
        .collect()
}

fn directory_membership(tree: &Tree) -> Result<BTreeMap<NodeId, BTreeMap<String, NodeId>>> {
    let mut membership = tree
        .entries
        .values()
        .filter_map(|node| node.dir_generation().map(|_| (node.id, BTreeMap::new())))
        .collect::<BTreeMap<_, _>>();
    let mut folded_names = BTreeMap::<NodeId, BTreeSet<String>>::new();
    for (path, node) in &tree.entries {
        let Some(parent) = path.parent() else {
            continue;
        };
        let parent_id = tree
            .entries
            .get(&parent)
            .and_then(|node| node.dir_generation().map(|_| node.id))
            .ok_or_else(|| {
                Error::invalid(
                    "validate YinYang tree",
                    "tree entry parent is missing or is not a directory",
                )
            })?;
        let name = path.name().expect("a non-root path has a name");
        if !folded_names
            .entry(parent_id)
            .or_default()
            .insert(name.case_fold().nfc().collect())
        {
            return Err(Error::invalid(
                "validate YinYang tree",
                "directory contains a case-folding name collision",
            ));
        }
        membership
            .get_mut(&parent_id)
            .expect("every directory has a membership set")
            .insert(name.to_owned(), node.id);
    }
    Ok(membership)
}

fn validate_portable_path(path: &str) -> Result<()> {
    if path.is_empty() {
        return Ok(());
    }
    if path.len() > MAX_PATH_BYTES
        || path.starts_with('/')
        || path.ends_with('/')
        || path.contains("//")
    {
        return Err(Error::invalid(
            "construct YinYang path",
            "path is not canonical and portable",
        ));
    }
    for component in path.split('/') {
        validate_portable_component(component)?;
    }
    Ok(())
}

fn validate_portable_component(component: &str) -> Result<()> {
    if component.len() > MAX_COMPONENT_BYTES
        || component == "."
        || component == ".."
        || component.ends_with([' ', '.'])
        || component.chars().any(|character| {
            character.is_control()
                || matches!(character, '<' | '>' | ':' | '"' | '\\' | '|' | '?' | '*')
        })
        || !component.nfc().eq(component.chars())
    {
        return Err(Error::invalid(
            "construct YinYang path",
            "path component is not canonical and portable",
        ));
    }
    let folded = component.case_fold().nfc().collect::<String>();
    let stem = folded.split('.').next().unwrap_or_default();
    if matches!(stem, "con" | "prn" | "aux" | "nul")
        || stem.len() == 4
            && (stem.starts_with("com") || stem.starts_with("lpt"))
            && matches!(stem.as_bytes()[3], b'1'..=b'9')
        || matches!(stem, "com¹" | "com²" | "com³" | "lpt¹" | "lpt²" | "lpt³")
    {
        return Err(Error::invalid(
            "construct YinYang path",
            "path component is reserved",
        ));
    }
    Ok(())
}
