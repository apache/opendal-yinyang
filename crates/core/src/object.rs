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

use crate::authority::{RETAINED_REVISIONS_PAGE_MAX, RevisionPage};
use crate::compatibility::{
    OBJECT_HEAD_MAGIC, OBJECT_SNAPSHOT_EXPIRY_MAGIC, OBJECT_SNAPSHOT_MAGIC, SERVICE_BINDING_MAGIC,
};
use crate::data::{DataStore, PackedRef, RefWire};
use crate::expiry::{
    ExpiryConflictKind, ExpiryMarker, ExpiryOutcome, ExpiryRefusal, ExpiryRequest, HistoryEntry,
    HistoryPage, RevisionState, operation_key, revision_key,
};
use crate::index::{Index, IndexCache};
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
type SnapshotExpiryWire = (
    [u8; 8],
    [u8; 16],
    [u8; 16],
    RevisionWire,
    [Option<RefWire>; 6],
);
#[derive(Clone, Debug)]
pub(crate) struct ObjectState {
    data: DataStore,
    filesystem: NodeId,
    root: NodeId,
    revision: Revision,
    nodes: Index,
    entries: Index,
    receipts: Index,
    changes: Index,
    history: Index,
    expiry: Index,
    head_mode: u8,
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
                cache: IndexCache::default(),
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
            &self.expiry,
        ]
        .map(|i| i.0.as_ref().map(PackedRef::wire));
        let descriptor = match self.head_mode {
            0 => encode(&(
                OBJECT_SNAPSHOT_MAGIC,
                *self.filesystem.as_bytes(),
                *self.root.as_bytes(),
                self.revision.wire(),
                [roots[0], roots[1], roots[2], roots[3], roots[4]],
            ))?,
            1 => encode(&(
                OBJECT_SNAPSHOT_EXPIRY_MAGIC,
                *self.filesystem.as_bytes(),
                *self.root.as_bytes(),
                self.revision.wire(),
                roots,
            ))?,
            _ => {
                return Err(Error::unsupported(
                    "persist snapshot",
                    "unknown object head mode",
                ));
            }
        };
        self.reference = self.data.put_metadata(&descriptor).await?;
        Ok(())
    }

    pub(crate) fn data(&self) -> &DataStore {
        &self.data
    }

    pub(crate) const fn filesystem(&self) -> NodeId {
        self.filesystem
    }

    pub(crate) const fn revision(&self) -> Revision {
        self.revision
    }

    pub(crate) const fn nodes(&self) -> &Index {
        &self.nodes
    }

    pub(crate) const fn entries(&self) -> &Index {
        &self.entries
    }

    pub(crate) const fn receipts(&self) -> &Index {
        &self.receipts
    }

    pub(crate) const fn changes(&self) -> &Index {
        &self.changes
    }

    pub(crate) const fn history(&self) -> &Index {
        &self.history
    }

    pub(crate) const fn expiry(&self) -> &Index {
        &self.expiry
    }

    pub(crate) const fn reference(&self) -> &PackedRef {
        &self.reference
    }

    pub(crate) async fn load_retained(
        &self,
        revision: Revision,
        reference: &PackedRef,
    ) -> Result<Self> {
        let roots = read_snapshot_roots(
            &self.data,
            reference,
            revision,
            self.filesystem,
            self.root,
            None,
        )
        .await?;
        Ok(Self {
            data: self.data.clone(),
            filesystem: self.filesystem,
            root: self.root,
            revision,
            nodes: roots.nodes,
            entries: roots.entries,
            receipts: roots.receipts,
            changes: roots.changes,
            history: roots.history,
            expiry: roots.expiry,
            head_mode: roots.head_mode,
            reference: reference.clone(),
            etag: None,
        })
    }
}

#[derive(Debug)]
struct ObjectRecords {
    data: DataStore,
    cache: IndexCache,
    indexes: [Index; 4],
}
impl RecordReader for ObjectRecords {
    fn get<'a>(&'a self, table: Table, key: &'a [u8]) -> BoxFuture<'a, Result<Option<Vec<u8>>>> {
        Box::pin(self.indexes[table as usize].get(&self.data, Some(&self.cache), key))
    }
    fn scan<'a>(
        &'a self,
        table: Table,
        lower: &'a [u8],
        upper: Option<&'a [u8]>,
        after: Option<&'a [u8]>,
        limit: usize,
    ) -> BoxFuture<'a, Result<Rows>> {
        Box::pin(self.indexes[table as usize].scan(
            &self.data,
            Some(&self.cache),
            lower,
            upper,
            after,
            limit,
        ))
    }
}
#[derive(Clone, Debug)]
pub struct ObjectFs {
    operator: Operator,
    data: DataStore,
    filesystem: NodeId,
    root: NodeId,
    // Clones share a fair local publication gate; separate authorities retain
    // CAS contention with one another.
    commit_gate: Arc<tokio::sync::Mutex<()>>,
}
type HeadWire = ([u8; 8], u8, [u8; 16], [u8; 16], RevisionWire, RefWire);
struct Head {
    filesystem: NodeId,
    root: NodeId,
    mode: u8,
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
            expiry: Index::default(),
            head_mode: 0,
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
            commit_gate: Arc::new(tokio::sync::Mutex::new(())),
        };
        let snapshot = fs
            .load(
                head.reference,
                head.revision,
                Some(head.etag),
                Some(head.mode),
            )
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
    pub(crate) async fn observe_state(&self) -> Result<ObjectState> {
        let head = read_head(&self.operator)
            .await?
            .ok_or_else(|| corrupt("published head disappeared"))?;
        if head.filesystem != self.filesystem || head.root != self.root {
            return Err(corrupt("filesystem lineage changed"));
        }
        self.load(
            head.reference,
            head.revision,
            Some(head.etag),
            Some(head.mode),
        )
        .await
    }
    async fn load(
        &self,
        reference: PackedRef,
        revision: Revision,
        etag: Option<String>,
        expected_mode: Option<u8>,
    ) -> Result<ObjectState> {
        let roots = read_snapshot_roots(
            &self.data,
            &reference,
            revision,
            self.filesystem,
            self.root,
            expected_mode,
        )
        .await?;
        Ok(ObjectState {
            data: self.data.clone(),
            filesystem: self.filesystem,
            root: self.root,
            revision,
            nodes: roots.nodes,
            entries: roots.entries,
            receipts: roots.receipts,
            changes: roots.changes,
            history: roots.history,
            expiry: roots.expiry,
            head_mode: roots.head_mode,
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
            .get(&self.data, None, &revision.to_bytes())
            .await?;
        let expired = if latest.head_mode == 1 {
            latest
                .expiry
                .get(&self.data, None, &revision_key(revision))
                .await?
                .map(|bytes| ExpiryMarker::decode(&bytes))
                .transpose()?
        } else {
            None
        };
        if bytes.is_none() && expired.is_some() {
            return Err(Error::corrupt(
                "observe revision",
                "expiry marker has no retained snapshot reference",
            ));
        }
        if expired.is_some() {
            return Err(Error::expired(
                "observe revision",
                revision,
                "revision is logically expired",
            ));
        }
        let bytes = bytes.ok_or_else(|| {
            Error::new(
                ErrorKind::NotFound,
                "observe revision",
                "revision is not retained in this lineage",
            )
        })?;
        Ok(self
            .load(PackedRef::from_wire(decode(&bytes)?)?, revision, None, None)
            .await?
            .view())
    }
    pub(crate) async fn retained_revisions_page(
        &self,
        after: Option<Revision>,
        limit: u32,
    ) -> Result<RevisionPage> {
        if !(1..=RETAINED_REVISIONS_PAGE_MAX).contains(&limit) {
            return Err(Error::unsupported(
                "list retained revisions",
                format!("page size must be 1..={RETAINED_REVISIONS_PAGE_MAX}"),
            ));
        }
        let latest = self.observe_state().await?;
        let after_key = after.map(|revision| revision.to_bytes().to_vec());
        let rows = latest
            .history
            .scan(
                &self.data,
                None,
                &[],
                None,
                after_key.as_deref(),
                limit as usize + 1,
            )
            .await?;
        let mut revisions = Vec::new();
        for (key, value) in rows {
            let key = key
                .try_into()
                .map_err(|_| corrupt("invalid retained revision key"))?;
            let _: PackedRef = PackedRef::from_wire(decode(&value)?)?;
            revisions.push(Revision::from_bytes(key));
        }
        if revisions.len() > limit as usize {
            revisions.truncate(limit as usize);
            let next = revisions.last().copied();
            return Ok(RevisionPage { revisions, next });
        }
        if after < Some(latest.revision) {
            revisions.push(latest.revision);
        }
        if revisions.len() > limit as usize {
            revisions.truncate(limit as usize);
            let next = revisions.last().copied();
            return Ok(RevisionPage { revisions, next });
        }
        if revisions.windows(2).any(|window| window[0] >= window[1]) {
            return Err(corrupt("retained revisions are not strictly ordered"));
        }
        Ok(RevisionPage {
            revisions,
            next: None,
        })
    }
    pub(crate) async fn history_page(
        &self,
        after: Option<Revision>,
        limit: u32,
    ) -> Result<HistoryPage> {
        if !(1..=RETAINED_REVISIONS_PAGE_MAX).contains(&limit) {
            return Err(Error::unsupported(
                "read history",
                format!("page size must be 1..={RETAINED_REVISIONS_PAGE_MAX}"),
            ));
        }
        let latest = self.observe_state().await?;
        let after_key = after.map(|revision| revision.to_bytes().to_vec());
        let rows = latest
            .history
            .scan(
                &self.data,
                None,
                &[],
                None,
                after_key.as_deref(),
                limit as usize + 1,
            )
            .await?;
        let mut revisions = Vec::new();
        for (key, value) in rows {
            let key = key
                .try_into()
                .map_err(|_| corrupt("invalid retained revision key"))?;
            let _: PackedRef = PackedRef::from_wire(decode(&value)?)?;
            revisions.push(Revision::from_bytes(key));
        }
        let mut next = None;
        if revisions.len() > limit as usize {
            revisions.truncate(limit as usize);
            next = revisions.last().copied();
        } else if after < Some(latest.revision) {
            revisions.push(latest.revision);
            if revisions.len() > limit as usize {
                revisions.truncate(limit as usize);
                next = revisions.last().copied();
            }
        }
        if revisions.windows(2).any(|window| window[0] >= window[1]) {
            return Err(corrupt("retained revisions are not strictly ordered"));
        }
        let mut entries = Vec::with_capacity(revisions.len());
        for revision in revisions {
            let state = if latest.head_mode == 1 {
                match self.revision_marker(&latest, revision).await? {
                    Some(marker) => {
                        if marker.after > latest.revision {
                            return Err(Error::corrupt(
                                "read history",
                                "expiry marker was published after the observed head",
                            ));
                        }
                        RevisionState::Expired(marker)
                    }
                    None => RevisionState::Readable,
                }
            } else {
                RevisionState::Readable
            };
            entries.push(HistoryEntry { revision, state });
        }
        let page = HistoryPage { entries, next };
        page.validate()?;
        Ok(page)
    }
    async fn revision_marker(
        &self,
        state: &ObjectState,
        revision: Revision,
    ) -> Result<Option<ExpiryMarker>> {
        if state.head_mode == 0 {
            return Ok(None);
        }
        state
            .expiry
            .get(&self.data, None, &revision_key(revision))
            .await?
            .map(|bytes| ExpiryMarker::decode(&bytes))
            .transpose()
    }
    async fn committed_expiry(
        &self,
        state: &ObjectState,
        request: &ExpiryRequest,
        request_digest: [u8; 32],
    ) -> Result<Option<ExpiryOutcome>> {
        if state.head_mode == 0 {
            return Ok(None);
        }
        let Some(bytes) = state
            .expiry
            .get(&self.data, None, &operation_key(request.id()))
            .await?
        else {
            return Ok(None);
        };
        let marker = ExpiryMarker::decode(&bytes)?;
        if marker.request_digest != request_digest {
            return Err(Error::invalid(
                "expire revisions",
                "operation identity was reused with a different request",
            ));
        }
        request.validate_marker(&marker)?;
        for candidate in request.candidates() {
            let bytes = state
                .expiry
                .get(&self.data, None, &revision_key(*candidate))
                .await?
                .ok_or_else(|| {
                    Error::corrupt(
                        "expire revisions",
                        "committed operation is missing a revision marker",
                    )
                })?;
            if ExpiryMarker::decode(&bytes)? != marker {
                return Err(Error::corrupt(
                    "expire revisions",
                    "operation and revision markers disagree",
                ));
            }
        }
        Ok(Some(ExpiryOutcome::Committed(request.receipt(marker))))
    }
    pub async fn expire_revisions(&self, request: &ExpiryRequest) -> Result<ExpiryOutcome> {
        request.validate_volume(self.filesystem)?;
        let request_digest = request.digest()?;
        let _guard = self.commit_gate.lock().await;
        let base = self.observe_state().await?;
        if let Some(outcome) = self
            .committed_expiry(&base, request, request_digest)
            .await?
        {
            return Ok(outcome);
        }
        if base.revision != request.expected_latest() {
            return Ok(ExpiryOutcome::Conflict);
        }

        let mut refusal = ExpiryRefusal::new(base.revision);
        for candidate in request.candidates() {
            if *candidate == base.revision {
                refusal.push(*candidate, ExpiryConflictKind::Current);
                continue;
            }
            let Some(value) = base
                .history
                .get(&self.data, None, &candidate.to_bytes())
                .await?
            else {
                refusal.push(*candidate, ExpiryConflictKind::Missing);
                continue;
            };
            let _: PackedRef = PackedRef::from_wire(decode(&value)?)?;
            if self.revision_marker(&base, *candidate).await?.is_some() {
                refusal.push(*candidate, ExpiryConflictKind::AlreadyExpired);
            }
        }
        for protected in request.protected() {
            if *protected == base.revision {
                continue;
            }
            let Some(value) = base
                .history
                .get(&self.data, None, &protected.to_bytes())
                .await?
            else {
                refusal.push(*protected, ExpiryConflictKind::ProtectedMissing);
                continue;
            };
            let _: PackedRef = PackedRef::from_wire(decode(&value)?)?;
            if self.revision_marker(&base, *protected).await?.is_some() {
                refusal.push(*protected, ExpiryConflictKind::ProtectedExpired);
            }
        }
        for candidate in request.candidates() {
            if request.protected().binary_search(candidate).is_ok() {
                refusal.push(*candidate, ExpiryConflictKind::Protected);
            }
        }
        if !refusal.is_empty() {
            return Ok(ExpiryOutcome::Refused(refusal));
        }

        let sequence =
            base.revision.sequence.checked_add(1).ok_or_else(|| {
                Error::unsupported("expire revisions", "revision space exhausted")
            })?;
        let after = Revision::new(sequence);
        let marker = ExpiryMarker {
            operation_id: request.id(),
            request_digest,
            evidence_digest: request.evidence_digest(),
            before: base.revision,
            after,
        };
        let mut candidate = base.clone();
        candidate.revision = after;
        candidate.head_mode = 1;
        candidate
            .history
            .set(
                &self.data,
                base.revision.to_bytes().to_vec(),
                Some(encode(&base.reference.wire())?),
            )
            .await?;
        candidate
            .expiry
            .set(
                &self.data,
                operation_key(request.id()),
                Some(marker.encode()?),
            )
            .await?;
        for revision in request.candidates() {
            candidate
                .expiry
                .set(&self.data, revision_key(*revision), Some(marker.encode()?))
                .await?;
        }
        if let Err(error) = candidate.persist().await {
            return if error.kind() == ErrorKind::Storage {
                Ok(ExpiryOutcome::Retryable)
            } else {
                Err(error)
            };
        }
        let etag = base
            .etag
            .as_ref()
            .ok_or_else(|| Error::unsupported("expire revisions", "head has no CAS token"))?;
        match self
            .operator
            .write_with(".yinyang/head", head_bytes(&candidate)?)
            .if_match(etag)
            .await
        {
            Ok(_) => Ok(ExpiryOutcome::Committed(request.receipt(marker))),
            Err(error) if error.kind() == opendal::ErrorKind::ConditionNotMatch => {
                Ok(ExpiryOutcome::Conflict)
            }
            Err(_) => match self.observe_state().await {
                Ok(current) => match self
                    .committed_expiry(&current, request, request_digest)
                    .await
                {
                    Ok(Some(outcome)) => Ok(outcome),
                    Ok(None) => Ok(ExpiryOutcome::Unknown(request.id())),
                    Err(error) => Err(error),
                },
                Err(_) => Ok(ExpiryOutcome::Unknown(request.id())),
            },
        }
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
        let _guard = self.commit_gate.lock().await;
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

struct SnapshotRoots {
    head_mode: u8,
    nodes: Index,
    entries: Index,
    receipts: Index,
    changes: Index,
    history: Index,
    expiry: Index,
}

async fn read_snapshot_roots(
    data: &DataStore,
    reference: &PackedRef,
    revision: Revision,
    filesystem: NodeId,
    root: NodeId,
    expected_mode: Option<u8>,
) -> Result<SnapshotRoots> {
    let bytes = data.read_extent(reference, 4096).await?;
    if bytes.len() < OBJECT_SNAPSHOT_MAGIC.len() {
        return Err(corrupt("truncated snapshot descriptor"));
    }
    if expected_mode == Some(0) && bytes[..OBJECT_SNAPSHOT_MAGIC.len()] != OBJECT_SNAPSHOT_MAGIC {
        return Err(Error::unsupported(
            "open snapshot",
            "mode-0 head has an unknown snapshot profile",
        ));
    }
    if expected_mode == Some(1)
        && bytes[..OBJECT_SNAPSHOT_EXPIRY_MAGIC.len()] != OBJECT_SNAPSHOT_EXPIRY_MAGIC
    {
        return Err(Error::unsupported(
            "open snapshot",
            "mode-1 head has an unknown snapshot profile",
        ));
    }
    let (head_mode, roots) = if bytes[..OBJECT_SNAPSHOT_MAGIC.len()] == OBJECT_SNAPSHOT_MAGIC {
        let (magic, fs, encoded_root, rev, roots): SnapshotWire = decode(&bytes)?;
        validate_snapshot_envelope(
            magic == OBJECT_SNAPSHOT_MAGIC,
            fs,
            encoded_root,
            rev,
            revision,
            filesystem,
            root,
        )?;
        let mut roots = roots
            .into_iter()
            .map(|entry| entry.map(PackedRef::from_wire).transpose().map(Index));
        (
            0,
            [
                roots.next().expect("five snapshot roots")?,
                roots.next().expect("five snapshot roots")?,
                roots.next().expect("five snapshot roots")?,
                roots.next().expect("five snapshot roots")?,
                roots.next().expect("five snapshot roots")?,
                Index::default(),
            ],
        )
    } else if bytes[..OBJECT_SNAPSHOT_EXPIRY_MAGIC.len()] == OBJECT_SNAPSHOT_EXPIRY_MAGIC {
        let (magic, fs, encoded_root, rev, roots): SnapshotExpiryWire = decode(&bytes)?;
        validate_snapshot_envelope(
            magic == OBJECT_SNAPSHOT_EXPIRY_MAGIC,
            fs,
            encoded_root,
            rev,
            revision,
            filesystem,
            root,
        )?;
        let mut roots = roots
            .into_iter()
            .map(|entry| entry.map(PackedRef::from_wire).transpose().map(Index));
        (
            1,
            [
                roots.next().expect("six snapshot roots")?,
                roots.next().expect("six snapshot roots")?,
                roots.next().expect("six snapshot roots")?,
                roots.next().expect("six snapshot roots")?,
                roots.next().expect("six snapshot roots")?,
                roots.next().expect("six snapshot roots")?,
            ],
        )
    } else {
        return Err(Error::unsupported(
            "open snapshot",
            "unknown snapshot profile",
        ));
    };
    if expected_mode.is_some_and(|mode| mode != head_mode) {
        return Err(Error::corrupt(
            "open snapshot",
            "head mode disagrees with snapshot profile",
        ));
    }
    let [nodes, entries, receipts, changes, history, expiry] = roots;
    Ok(SnapshotRoots {
        head_mode,
        nodes,
        entries,
        receipts,
        changes,
        history,
        expiry,
    })
}

fn validate_snapshot_envelope(
    recognized: bool,
    fs: [u8; 16],
    encoded_root: [u8; 16],
    revision: RevisionWire,
    expected_revision: Revision,
    filesystem: NodeId,
    root: NodeId,
) -> Result<()> {
    if !recognized
        || fs != *filesystem.as_bytes()
        || encoded_root != *root.as_bytes()
        || Revision::from_wire(revision) != expected_revision
    {
        return Err(corrupt("snapshot envelope disagrees with authority"));
    }
    Ok(())
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
        OBJECT_HEAD_MAGIC,
        snapshot.head_mode,
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
    if bytes.starts_with(&SERVICE_BINDING_MAGIC) {
        return Err(Error::unsupported(
            "open filesystem",
            "metadata-service authority required",
        ));
    }
    if bytes.starts_with(b"YYSERV") {
        return Err(Error::unsupported(
            "open filesystem",
            "unknown service binding profile",
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
    if magic != OBJECT_HEAD_MAGIC || !matches!(mode, 0 | 1) {
        return Err(Error::unsupported(
            "open filesystem",
            "unknown profile or publication mode",
        ));
    }
    Ok(Some(Head {
        filesystem: NodeId::from_bytes(fs),
        root: NodeId::from_bytes(root),
        mode,
        revision: Revision::from_wire(revision),
        reference: PackedRef::from_wire(reference)?,
        etag,
    }))
}
