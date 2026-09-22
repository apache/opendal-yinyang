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

use crate::data::{DataStore, PackedRef, RefWire};
use crate::index::Index;
use crate::namespace::{Node, NodeKind, corrupt, decode, encode};
use crate::transaction::Transaction;
use crate::{Error, ErrorKind, NodeId, Result};
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

// Kept available here for existing object-profile callers.
pub use crate::snapshot::{
    Change, ChangeRecord, Cursor, DirectoryPage, Outcome, Receipt, Revision, ScanToken, Snapshot,
};
use crate::snapshot::{RecordReader, RevisionWire, Rows, Table};
use futures_util::future::BoxFuture;
use std::sync::Arc;

type SnapshotWire = (
    [u8; 8],
    [u8; 16],
    [u8; 16],
    RevisionWire,
    [Option<RefWire>; 5],
);
#[derive(Clone, Debug)]
struct ObjectState {
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
impl ObjectState {
    fn view(&self) -> Snapshot {
        Snapshot {
            data: self.data.clone(),
            filesystem: self.filesystem,
            root: self.root,
            revision: self.revision,
            reader: Arc::new(ObjectRecords {
                data: self.data.clone(),
                indexes: [
                    self.nodes.clone(),
                    self.entries.clone(),
                    self.receipts.clone(),
                    self.changes.clone(),
                ],
            }),
        }
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

#[derive(Debug)]
struct ObjectRecords {
    data: DataStore,
    indexes: [Index; 4],
}
impl RecordReader for ObjectRecords {
    fn get<'a>(&'a self, table: Table, key: &'a [u8]) -> BoxFuture<'a, Result<Option<Vec<u8>>>> {
        Box::pin(self.indexes[table as usize].get(&self.data, key))
    }
    fn scan<'a>(
        &'a self,
        table: Table,
        lower: &'a [u8],
        upper: Option<&'a [u8]>,
        after: Option<&'a [u8]>,
        limit: usize,
    ) -> BoxFuture<'a, Result<Rows>> {
        Box::pin(self.indexes[table as usize].scan(&self.data, lower, upper, after, limit))
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
        let mut snapshot = ObjectState {
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
        snapshot.view().require_directory(fs.root).await?;
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
        Ok(self.observe_state().await?.view())
    }
    async fn observe_state(&self) -> Result<ObjectState> {
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
    ) -> Result<ObjectState> {
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
        Ok(ObjectState {
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
        let latest = self.observe_state().await?;
        if latest.revision == revision {
            return Ok(latest.view());
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
        Ok(self
            .load(PackedRef::from_wire(decode(&bytes)?)?, revision, None)
            .await?
            .view())
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
            request.validate(self.filesystem)?;
        }
        for _ in 0..8 {
            let base = self.observe_state().await?;
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
                if let Some(receipt) = candidate.view().receipt(request.id()).await? {
                    if receipt.request_digest != request.digest() {
                        return Err(Error::invalid(
                            "commit",
                            "identity reused for different request",
                        ));
                    }
                    outcomes.push(Outcome::Committed(receipt));
                    continue;
                }
                let Some(delta) = request.apply(&candidate.view()).await? else {
                    outcomes.push(Outcome::Conflict);
                    continue;
                };
                for (key, value) in delta.nodes {
                    candidate.nodes.set(&self.data, key, value).await?;
                }
                for (key, value) in delta.entries {
                    candidate.entries.set(&self.data, key, value).await?;
                }
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
                                changes: delta.changes,
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
                Err(_) => match self.observe_state().await {
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
fn head_bytes(snapshot: &ObjectState) -> Result<Vec<u8>> {
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
    if bytes.starts_with(b"YYSERV01") {
        return Err(Error::unsupported(
            "open filesystem",
            "metadata-service authority required",
        ));
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
