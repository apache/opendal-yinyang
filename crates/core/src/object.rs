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

use crate::data::{DataStore, PackedRef, PreparedContent, RefWire};
use crate::index::Index;
use crate::namespace::{
    DirectoryEntry, Node, NodeKind, corrupt, decode, encode, entry_key, prefix_end,
};
use crate::transaction::Transaction;
use crate::{CommitId, Error, ErrorKind, NodeId, Result};
use futures_util::TryStreamExt as _;
use opendal::Operator;

/// Deployment semantics, not inferred from OpenDAL capability flags.
/// The endpoint must implement the named provider's documented strong reads,
/// durable immutable writes and atomic conditional writes. External mutations
/// under the owned prefix invalidate these guarantees.
#[derive(Clone, Copy, Debug)]
pub enum BackendProfile {
    AmazonS3,
    Minio,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct Revision {
    sequence: u64,
    nonce: [u8; 16],
}
type RevisionWire = (u64, [u8; 16]);
impl Revision {
    fn new(sequence: u64) -> Self {
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
    fn wire(self) -> RevisionWire {
        (self.sequence, self.nonce)
    }
    fn from_wire((sequence, nonce): RevisionWire) -> Self {
        Self { sequence, nonce }
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct Cursor {
    pub revision: Revision,
    pub ordinal: u32,
}
impl Cursor {
    fn key(self) -> Vec<u8> {
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
    fn encode(&self) -> Result<Vec<u8>> {
        encode(&(
            *self.commit_id.as_bytes(),
            self.request_digest,
            self.cursor.revision.wire(),
            self.cursor.ordinal,
        ))
    }
    fn decode(bytes: &[u8]) -> Result<Self> {
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
    fn encode(&self) -> Result<Vec<u8>> {
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
    fn decode(bytes: &[u8], fs: NodeId) -> Result<Self> {
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
    descriptor: PackedRef,
    directory: NodeId,
    after: Vec<u8>,
}
#[derive(Clone, Debug)]
pub struct DirectoryPage {
    pub entries: Vec<DirectoryEntry>,
    pub next: Option<ScanToken>,
}

/// A pinned, lazily read snapshot. Its roots can only come from this authority.
#[derive(Clone, Debug)]
pub struct Snapshot {
    pub(crate) data: DataStore,
    pub(crate) filesystem: NodeId,
    pub(crate) root: NodeId,
    pub(crate) revision: Revision,
    pub(crate) nodes: Index,
    pub(crate) entries: Index,
    receipts: Index,
    changes: Index,
    history: Index,
    reference: PackedRef,
    etag: Option<String>,
}
type SnapshotWire = (
    [u8; 8],
    [u8; 16],
    [u8; 16],
    RevisionWire,
    [Option<RefWire>; 5],
);
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
            .nodes
            .get(&self.data, id.as_bytes())
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
            .entries
            .get(&self.data, key)
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
    async fn require_directory(&self, id: NodeId) -> Result<Node> {
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
        if token.is_some_and(|t| t.descriptor != self.reference || t.directory != directory) {
            return Err(Error::invalid(
                "scan directory",
                "continuation belongs to another snapshot or directory",
            ));
        }
        let upper = prefix_end(directory.as_bytes());
        let mut rows = self
            .entries
            .scan(
                &self.data,
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
                descriptor: self.reference.clone(),
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
            .receipts
            .get(&self.data, id.as_bytes())
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
            .changes
            .scan(&self.data, &[], None, key.as_deref(), limit)
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
    async fn persist(&mut self) -> Result<()> {
        let roots = [
            &self.nodes,
            &self.entries,
            &self.receipts,
            &self.changes,
            &self.history,
        ]
        .map(|i| i.0.as_ref().map(PackedRef::wire));
        self.reference = self
            .data
            .put_metadata(&encode(&(
                *b"YYSNAP02",
                *self.filesystem.as_bytes(),
                *self.root.as_bytes(),
                self.revision.wire(),
                roots,
            ))?)
            .await?;
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct ObjectFs {
    operator: Operator,
    data: DataStore,
    filesystem: NodeId,
    root: NodeId,
}
type HeadWire = ([u8; 8], u8, [u8; 16], [u8; 16], RevisionWire, RefWire);
struct Head {
    filesystem: NodeId,
    root: NodeId,
    revision: Revision,
    reference: PackedRef,
    etag: String,
}
impl ObjectFs {
    pub async fn create(operator: Operator, profile: BackendProfile) -> Result<Self> {
        validate_backend(&operator, profile)?;
        if let Some(head) = read_head(&operator).await? {
            return Self::from_head(operator, head).await;
        }
        let filesystem = NodeId::generate();
        let root = NodeId::generate();
        let data = DataStore::new(operator.clone(), filesystem)?;
        let mut nodes = Index::default();
        nodes
            .set(
                &data,
                root.as_bytes().to_vec(),
                Some(
                    Node {
                        id: root,
                        generation: 1,
                        executable: false,
                        link: None,
                        kind: NodeKind::Directory { membership: 1 },
                    }
                    .encode()?,
                ),
            )
            .await?;
        let placeholder = nodes.0.clone().unwrap();
        let mut snapshot = Snapshot {
            data: data.clone(),
            filesystem,
            root,
            revision: Revision::new(0),
            nodes,
            entries: Index::default(),
            receipts: Index::default(),
            changes: Index::default(),
            history: Index::default(),
            reference: placeholder,
            etag: None,
        };
        snapshot.persist().await?;
        let bytes = head_bytes(&snapshot)?;
        match operator
            .write_with(".yinyang/head", bytes)
            .if_not_exists(true)
            .await
        {
            Ok(_) => Self::open(operator, profile).await,
            Err(error) => match Self::open(operator, profile).await {
                Ok(fs) => Ok(fs),
                Err(_) => Err(Error::from_storage("create filesystem head", error)),
            },
        }
    }
    pub async fn open(operator: Operator, profile: BackendProfile) -> Result<Self> {
        validate_backend(&operator, profile)?;
        let head = read_head(&operator)
            .await?
            .ok_or_else(|| Error::new(ErrorKind::NotFound, "open filesystem", "head is missing"))?;
        Self::from_head(operator, head).await
    }
    async fn from_head(operator: Operator, head: Head) -> Result<Self> {
        let fs = Self {
            data: DataStore::new(operator.clone(), head.filesystem)?,
            operator,
            filesystem: head.filesystem,
            root: head.root,
        };
        let snapshot = fs
            .load(head.reference, head.revision, Some(head.etag))
            .await?;
        snapshot.require_directory(fs.root).await?;
        Ok(fs)
    }
    pub fn data(&self) -> &DataStore {
        &self.data
    }
    pub const fn root(&self) -> NodeId {
        self.root
    }
    pub const fn filesystem(&self) -> NodeId {
        self.filesystem
    }
    pub async fn observe_latest(&self) -> Result<Snapshot> {
        let head = read_head(&self.operator)
            .await?
            .ok_or_else(|| corrupt("published head disappeared"))?;
        if head.filesystem != self.filesystem || head.root != self.root {
            return Err(corrupt("filesystem lineage changed"));
        }
        self.load(head.reference, head.revision, Some(head.etag))
            .await
    }
    async fn load(
        &self,
        reference: PackedRef,
        revision: Revision,
        etag: Option<String>,
    ) -> Result<Snapshot> {
        let bytes = self.data.read_extent(&reference, 4096).await?;
        let (magic, fs, root, rev, roots): SnapshotWire = decode(&bytes)?;
        if magic != *b"YYSNAP02"
            || fs != *self.filesystem.as_bytes()
            || root != *self.root.as_bytes()
            || Revision::from_wire(rev) != revision
        {
            return Err(corrupt("snapshot envelope disagrees with authority"));
        }
        let mut roots = roots
            .into_iter()
            .map(|r| r.map(PackedRef::from_wire).transpose().map(Index));
        Ok(Snapshot {
            data: self.data.clone(),
            filesystem: self.filesystem,
            root: self.root,
            revision,
            nodes: roots.next().unwrap()?,
            entries: roots.next().unwrap()?,
            receipts: roots.next().unwrap()?,
            changes: roots.next().unwrap()?,
            history: roots.next().unwrap()?,
            reference,
            etag,
        })
    }
    pub async fn observe_revision(&self, revision: Revision) -> Result<Snapshot> {
        let latest = self.observe_latest().await?;
        if latest.revision == revision {
            return Ok(latest);
        }
        let bytes = latest
            .history
            .get(&self.data, &revision.to_bytes())
            .await?
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::NotFound,
                    "observe revision",
                    "revision is not retained in this lineage",
                )
            })?;
        self.load(PackedRef::from_wire(decode(&bytes)?)?, revision, None)
            .await
    }
    pub async fn commit(&self, request: &Transaction) -> Result<Outcome> {
        Ok(self
            .commit_batch(std::slice::from_ref(request))
            .await?
            .remove(0))
    }
    /// Deterministic input order; rejected speculative members are not reported
    /// until the preceding accepted members become authoritative.
    pub async fn commit_batch(&self, requests: &[Transaction]) -> Result<Vec<Outcome>> {
        match self.commit_batch_inner(requests).await {
            Err(error) if error.kind() == ErrorKind::Storage => {
                Ok(requests.iter().map(|_| Outcome::Retryable).collect())
            }
            result => result,
        }
    }

    async fn commit_batch_inner(&self, requests: &[Transaction]) -> Result<Vec<Outcome>> {
        if requests.is_empty() {
            return Ok(Vec::new());
        }
        if requests.len() > 4096 {
            return Err(Error::unsupported(
                "commit batch",
                "batch exceeds 4096 requests",
            ));
        }
        for request in requests {
            request.validate(self)?;
        }
        for _ in 0..8 {
            let base = self.observe_latest().await?;
            let mut candidate = base.clone();
            candidate.revision = Revision::new(
                base.revision
                    .sequence
                    .checked_add(1)
                    .ok_or_else(|| Error::unsupported("commit", "revision space exhausted"))?,
            );
            let mut outcomes = Vec::new();
            let mut accepted = 0_u32;
            for request in requests {
                if let Some(receipt) = candidate.receipt(request.id()).await? {
                    if receipt.request_digest != request.digest() {
                        return Err(Error::invalid(
                            "commit",
                            "identity reused for different request",
                        ));
                    }
                    outcomes.push(Outcome::Committed(receipt));
                    continue;
                }
                let Some((nodes, entries, changes)) = request.apply(&candidate).await? else {
                    outcomes.push(Outcome::Conflict);
                    continue;
                };
                candidate.nodes = nodes;
                candidate.entries = entries;
                let receipt = Receipt {
                    commit_id: request.id(),
                    request_digest: request.digest(),
                    cursor: Cursor {
                        revision: candidate.revision,
                        ordinal: accepted,
                    },
                };
                candidate
                    .receipts
                    .set(
                        &self.data,
                        request.id().as_bytes().to_vec(),
                        Some(receipt.encode()?),
                    )
                    .await?;
                candidate
                    .changes
                    .set(
                        &self.data,
                        receipt.cursor.key(),
                        Some(
                            ChangeRecord {
                                receipt: receipt.clone(),
                                changes,
                            }
                            .encode()?,
                        ),
                    )
                    .await?;
                outcomes.push(Outcome::Committed(receipt));
                accepted += 1;
            }
            if accepted == 0 {
                return Ok(outcomes);
            }
            candidate
                .history
                .set(
                    &self.data,
                    base.revision.to_bytes().to_vec(),
                    Some(encode(&base.reference.wire())?),
                )
                .await?;
            candidate.persist().await?;
            let etag = base
                .etag
                .as_ref()
                .expect("latest observation has a conditional token");
            match self
                .operator
                .write_with(".yinyang/head", head_bytes(&candidate)?)
                .if_match(etag)
                .await
            {
                Ok(_) => return Ok(outcomes),
                Err(error) if error.kind() == opendal::ErrorKind::ConditionNotMatch => continue,
                Err(_) => match self.observe_latest().await {
                    Ok(current) if current.etag.as_ref() != Some(etag) => continue,
                    _ => return Ok(requests.iter().map(|r| Outcome::Unknown(r.id())).collect()),
                },
            }
        }
        let current = self.observe_latest().await?;
        let mut outcomes = Vec::new();
        for request in requests {
            match current.receipt(request.id()).await? {
                Some(receipt) if receipt.request_digest == request.digest() => {
                    outcomes.push(Outcome::Committed(receipt))
                }
                Some(_) => {
                    return Err(Error::invalid(
                        "commit",
                        "identity reused for different request",
                    ));
                }
                None => outcomes.push(Outcome::Retryable),
            }
        }
        Ok(outcomes)
    }
}
fn validate_backend(operator: &Operator, _profile: BackendProfile) -> Result<()> {
    let info = operator.info();
    let cap = info.capability();
    if info.scheme() != "s3"
        || !cap.read
        || !cap.write
        || !cap.write_can_multi
        || !cap.write_with_if_match
        || !cap.write_with_if_not_exists
    {
        return Err(Error::unsupported(
            "open object authority",
            "the selected S3 semantic profile and conditional streaming capabilities are required",
        ));
    }
    Ok(())
}
fn head_bytes(snapshot: &Snapshot) -> Result<Vec<u8>> {
    let mut bytes = encode(&(
        *b"YYHEAD02",
        0_u8,
        *snapshot.filesystem.as_bytes(),
        *snapshot.root.as_bytes(),
        snapshot.revision.wire(),
        snapshot.reference.wire(),
    ))?;
    bytes.extend(blake3::hash(&bytes).as_bytes());
    Ok(bytes)
}
async fn read_head(operator: &Operator) -> Result<Option<Head>> {
    let reader = match operator.reader(".yinyang/head").await {
        Ok(reader) => reader,
        Err(e) if e.kind() == opendal::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(Error::from_storage("observe head", e)),
    };
    let mut stream = match reader.into_stream(..).await {
        Ok(stream) => stream,
        Err(e) if e.kind() == opendal::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(Error::from_storage("observe head", e)),
    };
    let metadata = match stream.metadata().await {
        Ok(m) => m,
        Err(e) if e.kind() == opendal::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(Error::from_storage("observe head", e)),
    };
    let etag = metadata
        .etag()
        .filter(|v| !v.is_empty())
        .ok_or_else(|| Error::unsupported("observe head", "exact read has no conditional token"))?
        .to_owned();
    if metadata.content_length() > 4096 {
        return Err(corrupt("oversized head"));
    }
    let mut bytes = Vec::new();
    while let Some(buffer) = stream
        .try_next()
        .await
        .map_err(|e| Error::from_storage("observe head", e))?
    {
        if bytes.len() + buffer.len() > 4096 {
            return Err(corrupt("oversized head"));
        }
        for chunk in buffer {
            bytes.extend_from_slice(&chunk);
        }
    }
    if bytes.starts_with(b"YYHEAD01") {
        return Err(Error::unsupported(
            "open filesystem",
            "legacy format is not supported by the object authority",
        ));
    }
    if bytes.len() < 40
        || blake3::hash(&bytes[..bytes.len() - 32]).as_bytes() != &bytes[bytes.len() - 32..]
    {
        return Err(corrupt("invalid head checksum"));
    }
    let (magic, mode, fs, root, revision, reference): HeadWire =
        decode(&bytes[..bytes.len() - 32])?;
    if magic != *b"YYHEAD02" || mode != 0 {
        return Err(Error::unsupported(
            "open filesystem",
            "unknown profile or publication mode",
        ));
    }
    Ok(Some(Head {
        filesystem: NodeId::from_bytes(fs),
        root: NodeId::from_bytes(root),
        revision: Revision::from_wire(revision),
        reference: PackedRef::from_wire(reference)?,
        etag,
    }))
}
