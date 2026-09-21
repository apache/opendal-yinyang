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

use crate::data::{DataStore, PreparedContent};
use crate::namespace::{
    DirectoryEntry, Node, NodeKind, corrupt, decode, encode, entry_key, prefix_end,
};
use crate::{CommitId, Error, ErrorKind, NodeId, Result};
use futures_util::future::BoxFuture;
use std::sync::Arc;

/// Logical record families shared by publication authorities.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Table {
    Nodes = 0,
    Entries = 1,
    Receipts = 2,
    Changes = 3,
}
pub(crate) type Rows = Vec<(Vec<u8>, Vec<u8>)>;

/// An authority-bound immutable observation, never caller-supplied metadata.
pub(crate) trait RecordReader: std::fmt::Debug + Send + Sync {
    fn get<'a>(&'a self, table: Table, key: &'a [u8]) -> BoxFuture<'a, Result<Option<Vec<u8>>>>;
    fn scan<'a>(
        &'a self,
        table: Table,
        lower: &'a [u8],
        upper: Option<&'a [u8]>,
        after: Option<&'a [u8]>,
        limit: usize,
    ) -> BoxFuture<'a, Result<Rows>>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct Revision {
    pub(crate) sequence: u64,
    nonce: [u8; 16],
}
pub(crate) type RevisionWire = (u64, [u8; 16]);
impl Revision {
    pub(crate) fn new(sequence: u64) -> Self {
        Self {
            sequence,
            nonce: *uuid::Uuid::new_v4().as_bytes(),
        }
    }
    pub fn to_bytes(self) -> [u8; 24] {
        let mut bytes = [0; 24];
        bytes[..8].copy_from_slice(&self.sequence.to_be_bytes());
        bytes[8..].copy_from_slice(&self.nonce);
        bytes
    }
    /// Decode an untrusted locator. observe_revision checks that the token
    /// identifies a retained snapshot in this filesystem's authority lineage.
    pub fn from_bytes(bytes: [u8; 24]) -> Self {
        Self {
            sequence: u64::from_be_bytes(bytes[..8].try_into().expect("fixed revision prefix")),
            nonce: bytes[8..].try_into().expect("fixed revision nonce"),
        }
    }
    pub(crate) fn wire(self) -> RevisionWire {
        (self.sequence, self.nonce)
    }
    pub(crate) fn from_wire((sequence, nonce): RevisionWire) -> Self {
        Self { sequence, nonce }
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct Cursor {
    pub revision: Revision,
    pub ordinal: u32,
}
impl Cursor {
    pub(crate) fn key(self) -> Vec<u8> {
        let mut key = self.revision.to_bytes().to_vec();
        key.extend(self.ordinal.to_be_bytes());
        key
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Receipt {
    pub commit_id: CommitId,
    pub request_digest: [u8; 32],
    pub cursor: Cursor,
}
type ReceiptWire = ([u8; 16], [u8; 32], RevisionWire, u32);
impl Receipt {
    pub(crate) fn encode(&self) -> Result<Vec<u8>> {
        encode(&(
            *self.commit_id.as_bytes(),
            self.request_digest,
            self.cursor.revision.wire(),
            self.cursor.ordinal,
        ))
    }
    pub(crate) fn decode(bytes: &[u8]) -> Result<Self> {
        let (id, request_digest, revision, ordinal): ReceiptWire = decode(bytes)?;
        Ok(Self {
            commit_id: CommitId::from_bytes(id),
            request_digest,
            cursor: Cursor {
                revision: Revision::from_wire(revision),
                ordinal,
            },
        })
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Outcome {
    Committed(Receipt),
    Conflict,
    Retryable,
    Unknown(CommitId),
}
#[derive(Clone, Debug)]
pub struct Change {
    pub before: Option<Node>,
    pub after: Option<Node>,
}
#[derive(Clone, Debug)]
pub struct ChangeRecord {
    pub receipt: Receipt,
    pub changes: Vec<Change>,
}
type ChangeWire = (Vec<u8>, Vec<(Option<Vec<u8>>, Option<Vec<u8>>)>);
impl ChangeRecord {
    pub(crate) fn encode(&self) -> Result<Vec<u8>> {
        encode(&(
            self.receipt.encode()?,
            self.changes
                .iter()
                .map(|v| {
                    Ok((
                        v.before.as_ref().map(Node::encode).transpose()?,
                        v.after.as_ref().map(Node::encode).transpose()?,
                    ))
                })
                .collect::<Result<Vec<_>>>()?,
        ))
    }
    pub(crate) fn decode(bytes: &[u8], fs: NodeId) -> Result<Self> {
        let (receipt, changes): ChangeWire = decode(bytes)?;
        Ok(Self {
            receipt: Receipt::decode(&receipt)?,
            changes: changes
                .into_iter()
                .map(|(before, after)| {
                    Ok(Change {
                        before: before.map(|b| Node::decode(&b, fs)).transpose()?,
                        after: after.map(|b| Node::decode(&b, fs)).transpose()?,
                    })
                })
                .collect::<Result<_>>()?,
        })
    }
}
#[derive(Clone, Debug)]
pub struct ScanToken {
    filesystem: NodeId,
    revision: Revision,
    directory: NodeId,
    after: Vec<u8>,
}
#[derive(Clone, Debug)]
pub struct DirectoryPage {
    pub entries: Vec<DirectoryEntry>,
    pub next: Option<ScanToken>,
}

/// A coherent, lazily read observation pinned to one retained revision.
#[derive(Clone, Debug)]
pub struct Snapshot {
    pub(crate) data: DataStore,
    pub(crate) filesystem: NodeId,
    pub(crate) root: NodeId,
    pub(crate) revision: Revision,
    pub(crate) reader: Arc<dyn RecordReader>,
}
impl Snapshot {
    pub const fn revision(&self) -> Revision {
        self.revision
    }
    pub const fn root(&self) -> NodeId {
        self.root
    }
    pub const fn filesystem(&self) -> NodeId {
        self.filesystem
    }
    pub async fn node(&self, id: NodeId) -> Result<Option<Node>> {
        let node = self
            .reader
            .get(Table::Nodes, id.as_bytes())
            .await?
            .map(|b| Node::decode(&b, self.filesystem))
            .transpose()?;
        if let Some(node) = &node
            && (node.id != id
                || (id == self.root) != node.link.is_none()
                || (id == self.root && !node.is_directory()))
        {
            return Err(corrupt("node identity or root link disagrees"));
        }
        Ok(node)
    }
    pub async fn lookup(&self, parent: NodeId, name: &str) -> Result<Option<DirectoryEntry>> {
        self.require_directory(parent).await?;
        self.entry(&entry_key(parent, name)?).await
    }
    pub(crate) async fn entry(&self, key: &[u8]) -> Result<Option<DirectoryEntry>> {
        let entry = self
            .reader
            .get(Table::Entries, key)
            .await?
            .map(|b| DirectoryEntry::decode(&b))
            .transpose()?;
        if let Some(entry) = &entry {
            let node = self
                .node(entry.node_id)
                .await?
                .ok_or_else(|| corrupt("directory references missing node"))?;
            let link = node
                .link
                .ok_or_else(|| corrupt("directory references root"))?;
            if entry_key(link.parent, &entry.name)? != key || link.name != entry.name {
                return Err(corrupt("parent link disagrees with directory entry"));
            }
        }
        Ok(entry)
    }
    pub async fn resolve(&self, path: &str) -> Result<Option<Node>> {
        let mut id = self.root;
        if !path.is_empty() {
            for name in path.split('/') {
                let Some(entry) = self.lookup(id, name).await? else {
                    return Ok(None);
                };
                id = entry.node_id;
            }
        }
        self.node(id).await
    }
    pub(crate) async fn require_directory(&self, id: NodeId) -> Result<Node> {
        let node = self
            .node(id)
            .await?
            .ok_or_else(|| Error::new(ErrorKind::NotFound, "read directory", "node is absent"))?;
        if !node.is_directory() {
            return Err(Error::invalid("read directory", "node is not a directory"));
        }
        Ok(node)
    }
    pub async fn scan(
        &self,
        directory: NodeId,
        token: Option<&ScanToken>,
        limit: usize,
    ) -> Result<DirectoryPage> {
        if limit == 0 || limit > 4096 {
            return Err(Error::unsupported(
                "scan directory",
                "page size must be 1..=4096",
            ));
        }
        self.require_directory(directory).await?;
        if token.is_some_and(|t| {
            t.filesystem != self.filesystem
                || t.revision != self.revision
                || t.directory != directory
        }) {
            return Err(Error::invalid(
                "scan directory",
                "continuation belongs to another snapshot or directory",
            ));
        }
        let upper = prefix_end(directory.as_bytes());
        let mut rows = self
            .reader
            .scan(
                Table::Entries,
                directory.as_bytes(),
                upper.as_deref(),
                token.map(|t| t.after.as_slice()),
                limit + 1,
            )
            .await?;
        let more = rows.len() > limit;
        rows.truncate(limit);
        let next = if more {
            Some(ScanToken {
                filesystem: self.filesystem,
                revision: self.revision,
                directory,
                after: rows.last().unwrap().0.clone(),
            })
        } else {
            None
        };
        let mut entries = Vec::new();
        for (key, _) in rows {
            entries.push(
                self.entry(&key)
                    .await?
                    .ok_or_else(|| corrupt("scan lost entry"))?,
            );
        }
        Ok(DirectoryPage { entries, next })
    }
    pub async fn content(&self, id: NodeId) -> Result<PreparedContent> {
        match self
            .node(id)
            .await?
            .ok_or_else(|| Error::new(ErrorKind::NotFound, "read content", "node is absent"))?
            .kind
        {
            NodeKind::File(file) => self.data.published(file),
            _ => Err(Error::invalid("read content", "node is a directory")),
        }
    }
    pub async fn receipt(&self, id: CommitId) -> Result<Option<Receipt>> {
        let value = self
            .reader
            .get(Table::Receipts, id.as_bytes())
            .await?
            .map(|b| Receipt::decode(&b))
            .transpose()?;
        if value
            .as_ref()
            .is_some_and(|r| r.commit_id != id || r.cursor.revision > self.revision)
        {
            return Err(corrupt("invalid receipt lineage"));
        }
        Ok(value)
    }
    pub async fn changes(&self, after: Option<Cursor>, limit: usize) -> Result<Vec<ChangeRecord>> {
        if limit == 0 || limit > 4096 {
            return Err(Error::unsupported(
                "read changes",
                "page size must be 1..=4096",
            ));
        }
        let key = after.map(Cursor::key);
        let rows = self
            .reader
            .scan(Table::Changes, &[], None, key.as_deref(), limit)
            .await?;
        let mut records = Vec::new();
        for (key, bytes) in rows {
            let record = ChangeRecord::decode(&bytes, self.filesystem)?;
            if record.receipt.cursor.key() != key || record.receipt.cursor.revision > self.revision
            {
                return Err(corrupt("invalid change cursor"));
            }
            records.push(record);
        }
        Ok(records)
    }
}
