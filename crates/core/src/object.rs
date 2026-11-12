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

use crate::authority::{
    RETAINED_REVISIONS_PAGE_MAX, RevisionHistoryPage, RevisionRecord, RevisionStatus,
};
use crate::compatibility::{OBJECT_HEAD_MAGIC, OBJECT_SNAPSHOT_MAGIC, SERVICE_BINDING_MAGIC};
use crate::data::{DataStore, PackedRef, RefWire};
use crate::index::{Index, IndexCache};
use crate::namespace::{Node, NodeKind, corrupt, decode, encode};
use crate::transaction::Transaction;
use crate::{Error, ErrorKind, ExpiryRequest, NodeId, Result};
use futures_util::TryStreamExt as _;
use opendal::Operator;

// Kept available here for existing object-profile callers.
pub use crate::snapshot::{
    Change, ChangeRecord, Cursor, DirectoryPage, Outcome, Receipt, Revision, ScanToken, Snapshot,
};
use crate::snapshot::{RecordReader, RevisionWire, Rows, Table};
use futures_util::future::BoxFuture;
use std::sync::Arc;

const HISTORY_READABLE: u8 = 0;
const HISTORY_EXPIRED: u8 = 1;

fn history_key(status: RevisionStatus, revision: Revision) -> Vec<u8> {
    let mut key = Vec::with_capacity(25);
    key.push(match status {
        RevisionStatus::Readable => HISTORY_READABLE,
        RevisionStatus::Expired => HISTORY_EXPIRED,
    });
    key.extend(revision.to_bytes());
    key
}

fn history_revision(key: &[u8], status: RevisionStatus) -> Result<Revision> {
    let expected = match status {
        RevisionStatus::Readable => HISTORY_READABLE,
        RevisionStatus::Expired => HISTORY_EXPIRED,
    };
    if key.len() != 25 || key[0] != expected {
        return Err(corrupt("invalid retained revision key"));
    }
    Ok(Revision::from_bytes(
        key[1..]
            .try_into()
            .expect("the retained revision key was length checked"),
    ))
}

type SnapshotWire = (
    [u8; 8],
    [u8; 16],
    [u8; 16],
    RevisionWire,
    [Option<RefWire>; 5],
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
    reference: PackedRef,
    etag: Option<String>,
}
impl ObjectState {
    fn view(&self) -> Snapshot {
        self.view_cached(IndexCache::default())
    }

    fn view_cached(&self, cache: IndexCache) -> Snapshot {
        Snapshot {
            data: self.data.clone(),
            filesystem: self.filesystem,
            root: self.root,
            revision: self.revision,
            reader: Arc::new(ObjectRecords {
                data: self.data.clone(),
                cache,
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
                OBJECT_SNAPSHOT_MAGIC,
                *self.filesystem.as_bytes(),
                *self.root.as_bytes(),
                self.revision.wire(),
                roots,
            ))?)
            .await?;
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

    pub(crate) const fn reference(&self) -> &PackedRef {
        &self.reference
    }

    pub(crate) async fn load_retained(
        &self,
        revision: Revision,
        reference: &PackedRef,
    ) -> Result<Self> {
        let [nodes, entries, receipts, changes, history] =
            read_snapshot_roots(&self.data, reference, revision, self.filesystem, self.root)
                .await?;
        Ok(Self {
            data: self.data.clone(),
            filesystem: self.filesystem,
            root: self.root,
            revision,
            nodes,
            entries,
            receipts,
            changes,
            history,
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
    cache: IndexCache,
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
    revision: Revision,
    reference: PackedRef,
    etag: String,
}
impl ObjectFs {
    pub async fn create(operator: Operator) -> Result<Self> {
        validate_backend(&operator)?;
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
            Ok(_) => Self::open(operator).await,
            Err(error) => match Self::open(operator).await {
                Ok(fs) => Ok(fs),
                Err(_) => Err(Error::from_storage("create filesystem head", error)),
            },
        }
    }
    pub async fn open(operator: Operator) -> Result<Self> {
        validate_backend(&operator)?;
        let head = read_head(&operator)
            .await?
            .ok_or_else(|| Error::new(ErrorKind::NotFound, "open filesystem", "head is missing"))?;
        Self::from_head(operator, head).await
    }
    async fn from_head(operator: Operator, head: Head) -> Result<Self> {
        let fs = Self {
            data: DataStore::new(operator.clone(), head.filesystem)?,
            operator,
            cache: IndexCache::default(),
            filesystem: head.filesystem,
            root: head.root,
            commit_gate: Arc::new(tokio::sync::Mutex::new(())),
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
        Ok(self.observe_state().await?.view_cached(self.cache.clone()))
    }
    pub(crate) async fn observe_state(&self) -> Result<ObjectState> {
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
        let [nodes, entries, receipts, changes, history] =
            read_snapshot_roots(&self.data, &reference, revision, self.filesystem, self.root)
                .await?;
        Ok(ObjectState {
            data: self.data.clone(),
            filesystem: self.filesystem,
            root: self.root,
            revision,
            nodes,
            entries,
            receipts,
            changes,
            history,
            reference,
            etag,
        })
    }
    pub async fn observe_revision(&self, revision: Revision) -> Result<Snapshot> {
        let latest = self.observe_state().await?;
        if latest.revision == revision {
            return Ok(latest.view_cached(self.cache.clone()));
        }
        match self
            .revision_reference(&latest, revision, Some(&self.cache))
            .await?
        {
            Some((RevisionStatus::Readable, bytes)) => Ok(self
                .load(PackedRef::from_wire(decode(&bytes)?)?, revision, None)
                .await?
                .view_cached(self.cache.clone())),
            Some((RevisionStatus::Expired, _)) => Err(Error::new(
                ErrorKind::Expired,
                "observe revision",
                format!("revision {revision:?} is logically expired"),
            )),
            None => Err(Error::new(
                ErrorKind::NotFound,
                "observe revision",
                "revision is not retained in this lineage",
            )),
        }
    }
    async fn observe_known_revision(&self, revision: Revision) -> Result<Snapshot> {
        let latest = self.observe_state().await?;
        if latest.revision == revision {
            return Ok(latest.view_cached(self.cache.clone()));
        }
        let (_, bytes) = self
            .revision_reference(&latest, revision, None)
            .await?
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::NotFound,
                    "observe known revision",
                    "revision is not known in this lineage",
                )
            })?;
        Ok(self
            .load(PackedRef::from_wire(decode(&bytes)?)?, revision, None)
            .await?
            .view_cached(self.cache.clone()))
    }
    async fn revision_reference(
        &self,
        latest: &ObjectState,
        revision: Revision,
        cache: Option<&IndexCache>,
    ) -> Result<Option<(RevisionStatus, Vec<u8>)>> {
        let readable = latest
            .history
            .get(
                &self.data,
                cache,
                &history_key(RevisionStatus::Readable, revision),
            )
            .await?;
        let expired = latest
            .history
            .get(
                &self.data,
                cache,
                &history_key(RevisionStatus::Expired, revision),
            )
            .await?;
        match (readable, expired) {
            (Some(_), Some(_)) => Err(corrupt(
                "revision has both readable and expired history records",
            )),
            (Some(bytes), None) => Ok(Some((RevisionStatus::Readable, bytes))),
            (None, Some(bytes)) => Ok(Some((RevisionStatus::Expired, bytes))),
            (None, None) => Ok(None),
        }
    }
    pub(crate) async fn revision_history_page(
        &self,
        after: Option<Revision>,
        limit: u32,
    ) -> Result<RevisionHistoryPage> {
        if !(1..=RETAINED_REVISIONS_PAGE_MAX).contains(&limit) {
            return Err(Error::unsupported(
                "list revision history",
                format!("page size must be 1..={RETAINED_REVISIONS_PAGE_MAX}"),
            ));
        }
        let latest = self.observe_state().await?;
        let mut records = Vec::new();
        for status in [RevisionStatus::Readable, RevisionStatus::Expired] {
            let after_key = after.map(|revision| history_key(status, revision));
            let upper = match status {
                RevisionStatus::Readable => [HISTORY_EXPIRED],
                RevisionStatus::Expired => [HISTORY_EXPIRED + 1],
            };
            let rows = latest
                .history
                .scan(
                    &self.data,
                    Some(&self.cache),
                    &[match status {
                        RevisionStatus::Readable => HISTORY_READABLE,
                        RevisionStatus::Expired => HISTORY_EXPIRED,
                    }],
                    Some(&upper),
                    after_key.as_deref(),
                    limit as usize + 1,
                )
                .await?;
            for (key, value) in rows {
                let revision = history_revision(&key, status)?;
                let _: PackedRef = PackedRef::from_wire(decode(&value)?)?;
                records.push(RevisionRecord { revision, status });
            }
        }
        if after < Some(latest.revision) {
            records.push(RevisionRecord {
                revision: latest.revision,
                status: RevisionStatus::Readable,
            });
        }
        records.sort_by_key(|record| record.revision);
        if records
            .windows(2)
            .any(|window| window[0].revision == window[1].revision)
        {
            return Err(corrupt(
                "revision has duplicate history records in one page",
            ));
        }
        let more = records.len() > limit as usize;
        records.truncate(limit as usize);
        let next = more.then(|| records.last().expect("nonempty history page").revision);
        Ok(RevisionHistoryPage {
            revisions: records,
            next,
        })
    }
    pub async fn expire(&self, request: &ExpiryRequest) -> Result<Outcome> {
        request.validate(self.filesystem)?;
        let _guard = self.commit_gate.lock().await;
        match self.expire_inner(request).await {
            Err(error) if error.kind() == ErrorKind::Storage => Ok(Outcome::Retryable),
            result => result,
        }
    }
    async fn expire_inner(&self, request: &ExpiryRequest) -> Result<Outcome> {
        for _ in 0..8 {
            let base = self.observe_state().await?;
            if let Some(receipt) = base.view().receipt(request.id()).await? {
                if receipt.request_digest != request.digest() {
                    return Err(Error::invalid(
                        "expire revisions",
                        "identity reused for a different request",
                    ));
                }
                return Ok(Outcome::Committed(receipt));
            }
            if base.revision != request.expected_latest() {
                return Ok(Outcome::Conflict);
            }
            let mut references = Vec::with_capacity(request.candidates().len());
            for revision in request.candidates() {
                if *revision == base.revision {
                    return Err(Error::invalid(
                        "expire revisions",
                        "the current revision cannot expire",
                    ));
                }
                let (status, bytes) = self
                    .revision_reference(&base, *revision, None)
                    .await?
                    .ok_or_else(|| {
                        Error::new(
                            ErrorKind::NotFound,
                            "expire revisions",
                            format!("revision {revision:?} is unknown"),
                        )
                    })?;
                if status == RevisionStatus::Expired {
                    return Err(Error::new(
                        ErrorKind::Expired,
                        "expire revisions",
                        format!("revision {revision:?} is already expired"),
                    ));
                }
                let reference = PackedRef::from_wire(decode(&bytes)?)?;
                self.load(reference, *revision, None).await?;
                references.push((*revision, bytes));
            }

            let mut candidate = base.clone();
            candidate.revision = Revision::new(
                base.revision
                    .sequence
                    .checked_add(1)
                    .ok_or_else(|| Error::unsupported("expire revisions", "revision exhausted"))?,
            );
            candidate
                .history
                .set(
                    &self.data,
                    history_key(RevisionStatus::Readable, base.revision),
                    Some(encode(&base.reference.wire())?),
                )
                .await?;
            for (revision, bytes) in references {
                candidate
                    .history
                    .set(
                        &self.data,
                        history_key(RevisionStatus::Readable, revision),
                        None,
                    )
                    .await?;
                candidate
                    .history
                    .set(
                        &self.data,
                        history_key(RevisionStatus::Expired, revision),
                        Some(bytes),
                    )
                    .await?;
            }
            let receipt = Receipt {
                commit_id: request.id(),
                request_digest: request.digest(),
                cursor: Cursor {
                    revision: candidate.revision,
                    ordinal: 0,
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
                            changes: Vec::new(),
                        }
                        .encode()?,
                    ),
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
                Ok(_) => return Ok(Outcome::Committed(receipt)),
                Err(error) if error.kind() == opendal::ErrorKind::ConditionNotMatch => continue,
                Err(_) => match self.observe_state().await {
                    Ok(current) if current.etag.as_ref() != Some(etag) => {
                        match current.view().receipt(request.id()).await {
                            Ok(Some(current)) => {
                                if current.request_digest != request.digest() {
                                    return Err(Error::invalid(
                                        "expire revisions",
                                        "identity reused for a different request",
                                    ));
                                }
                                return Ok(Outcome::Committed(current));
                            }
                            // The fresh observation fences the failed
                            // conditional write. The fixed expected-latest guard
                            // is now known to have lost, so do not retry candidates.
                            Ok(None) => return Ok(Outcome::Conflict),
                            Err(_) => return Ok(Outcome::Unknown(request.id())),
                        }
                    }
                    _ => return Ok(Outcome::Unknown(request.id())),
                },
            }
        }
        Ok(Outcome::Retryable)
    }
    pub(crate) async fn restore_transaction(&self, bytes: &[u8]) -> Result<Transaction> {
        let (revision, _, _) = Transaction::verify_identity(bytes, self.filesystem)?;
        let snapshot = self.observe_known_revision(revision).await?;
        self.restore_transaction_from_snapshot(bytes, snapshot)
            .await
    }
    async fn restore_transaction_from_latest(
        &self,
        bytes: &[u8],
        latest: &ObjectState,
    ) -> Result<Transaction> {
        let (revision, _, _) = Transaction::verify_identity(bytes, self.filesystem)?;
        let snapshot = if revision == latest.revision {
            latest.view_cached(self.cache.clone())
        } else {
            let (_, reference) = self
                .revision_reference(latest, revision, None)
                .await?
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::NotFound,
                        "observe known revision",
                        "revision is not known in this lineage",
                    )
                })?;
            self.load(PackedRef::from_wire(decode(&reference)?)?, revision, None)
                .await?
                .view_cached(self.cache.clone())
        };
        self.restore_transaction_from_snapshot(bytes, snapshot)
            .await
    }
    async fn restore_transaction_from_snapshot(
        &self,
        bytes: &[u8],
        snapshot: Snapshot,
    ) -> Result<Transaction> {
        let (_, files) = Transaction::inspect(bytes)?;
        let mut prepared = Vec::new();
        for file in files {
            prepared.push(self.data.import(&file).await?);
        }
        Transaction::restore(bytes, &snapshot, &prepared).await
    }
    pub(crate) async fn commit_frozen_batch(&self, requests: &[Vec<u8>]) -> Result<Vec<Outcome>> {
        if requests.len() > 4096 {
            return Err(Error::unsupported(
                "commit frozen batch",
                "batch exceeds 4096 requests",
            ));
        }
        if requests.is_empty() {
            return Ok(Vec::new());
        }
        let latest = self.observe_state().await?;
        let latest_view = latest.view_cached(self.cache.clone());
        let mut outcomes = vec![None; requests.len()];
        let mut pending = Vec::new();
        for (index, bytes) in requests.iter().enumerate() {
            let (_, id, digest) = Transaction::verify_identity(bytes, self.filesystem)?;
            if let Some(receipt) = latest_view.receipt(id).await? {
                if receipt.request_digest != digest {
                    return Err(Error::invalid(
                        "commit frozen batch",
                        "identity reused for different request",
                    ));
                }
                outcomes[index] = Some(Outcome::Committed(receipt));
            } else {
                pending.push((index, bytes));
            }
        }
        if !pending.is_empty() {
            let mut restored = Vec::with_capacity(pending.len());
            for (_, bytes) in &pending {
                restored.push(self.restore_transaction_from_latest(bytes, &latest).await?);
            }
            let committed = self.commit_batch(&restored).await?;
            for ((index, _), outcome) in pending.into_iter().zip(committed) {
                outcomes[index] = Some(outcome);
            }
        }
        Ok(outcomes
            .into_iter()
            .map(|outcome| outcome.expect("every frozen request is resolved"))
            .collect())
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
                    history_key(RevisionStatus::Readable, base.revision),
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

async fn read_snapshot_roots(
    data: &DataStore,
    reference: &PackedRef,
    revision: Revision,
    filesystem: NodeId,
    root: NodeId,
) -> Result<[Index; 5]> {
    let bytes = data.read_extent(reference, 4096).await?;
    if bytes.len() < OBJECT_SNAPSHOT_MAGIC.len() {
        return Err(corrupt("truncated snapshot descriptor"));
    }
    if bytes[..OBJECT_SNAPSHOT_MAGIC.len()] != OBJECT_SNAPSHOT_MAGIC {
        return Err(Error::unsupported(
            "open snapshot",
            "unknown snapshot profile",
        ));
    }
    let (magic, fs, encoded_root, rev, roots): SnapshotWire = decode(&bytes)?;
    if magic != OBJECT_SNAPSHOT_MAGIC
        || fs != *filesystem.as_bytes()
        || encoded_root != *root.as_bytes()
        || Revision::from_wire(rev) != revision
    {
        return Err(corrupt("snapshot envelope disagrees with authority"));
    }
    let mut roots = roots
        .into_iter()
        .map(|entry| entry.map(PackedRef::from_wire).transpose().map(Index));
    Ok([
        roots.next().unwrap()?,
        roots.next().unwrap()?,
        roots.next().unwrap()?,
        roots.next().unwrap()?,
        roots.next().unwrap()?,
    ])
}
fn validate_backend(operator: &Operator) -> Result<()> {
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
            "the S3 transport and conditional streaming capabilities are required",
        ));
    }
    Ok(())
}
fn head_bytes(snapshot: &ObjectState) -> Result<Vec<u8>> {
    let mut bytes = encode(&(
        OBJECT_HEAD_MAGIC,
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
    if bytes.starts_with(b"YYHEAD01") || bytes.starts_with(b"YYHEAD02") {
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
    if magic != OBJECT_HEAD_MAGIC || mode != 0 {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::support::TestBackend;
    use crate::{Authority, CommitId, Planner};

    #[tokio::test]
    async fn snapshots_share_the_content_addressed_index_cache() {
        let backend = TestBackend::default();
        let fs = ObjectFs::create(backend.operator()).await.unwrap();
        let mut empty_source: &[u8] = b"";
        let empty = fs.prepare(&mut empty_source).await.unwrap();
        let base = fs.observe_latest().await.unwrap();
        let mut plan = Planner::new(&base, CommitId::generate());
        let bulk = plan.create_directory(fs.root(), "bulk").await.unwrap();
        for index in 0..1000 {
            plan.create_file(bulk, &format!("entry-{index:04}"), empty.clone(), false)
                .await
                .unwrap();
        }
        let request = plan.finish().unwrap();
        fs.commit(&request).await.unwrap();

        let first = fs.observe_latest().await.unwrap();
        let first_entry = first.lookup(bulk, "entry-0500").await.unwrap().unwrap();
        first.node(first_entry.node_id).await.unwrap().unwrap();

        let second = fs.observe_latest().await.unwrap();
        backend.state.lock().unwrap().data_reads = 0;
        let second_entry = second.lookup(bulk, "entry-0500").await.unwrap().unwrap();
        second.node(second_entry.node_id).await.unwrap().unwrap();
        assert_eq!(
            backend.state.lock().unwrap().data_reads,
            0,
            "a warm page cache was not shared across snapshots"
        );
    }

    #[tokio::test]
    async fn read_only_history_reuses_the_authority_index_cache() {
        let backend = TestBackend::default();
        let fs = ObjectFs::create(backend.operator()).await.unwrap();
        let oldest = fs.observe_latest().await.unwrap().revision();
        for name in ["one", "two", "three"] {
            let mut plan = Planner::new(&fs.observe_latest().await.unwrap(), CommitId::generate());
            plan.create_directory(fs.root(), name).await.unwrap();
            fs.commit(&plan.finish().unwrap()).await.unwrap();
        }

        fs.revision_history_page(None, RETAINED_REVISIONS_PAGE_MAX)
            .await
            .unwrap();
        backend.state.lock().unwrap().data_reads = 0;
        let history = fs
            .revision_history_page(None, RETAINED_REVISIONS_PAGE_MAX)
            .await
            .unwrap();
        assert_eq!(history.revisions.len(), 4);
        assert_eq!(
            backend.state.lock().unwrap().data_reads,
            1,
            "only the fresh snapshot descriptor should be read"
        );

        backend.state.lock().unwrap().data_reads = 0;
        fs.observe_revision(oldest).await.unwrap();
        assert_eq!(
            backend.state.lock().unwrap().data_reads,
            2,
            "only the fresh latest and selected snapshot descriptors should be read"
        );
    }

    #[tokio::test]
    async fn write_and_recovery_history_reads_bypass_the_authority_index_cache() {
        let backend = TestBackend::default();
        let fs = ObjectFs::create(backend.operator()).await.unwrap();
        let mut older_plan =
            Planner::new(&fs.observe_latest().await.unwrap(), CommitId::generate());
        older_plan
            .create_directory(fs.root(), "older")
            .await
            .unwrap();
        let older_request = older_plan.finish().unwrap();
        let older_revision = match fs.commit(&older_request).await.unwrap() {
            Outcome::Committed(receipt) => receipt.cursor.revision,
            other => panic!("expected committed, got {other:?}"),
        };

        let older_snapshot = fs.observe_revision(older_revision).await.unwrap();
        let mut pending = Planner::new(&older_snapshot, CommitId::generate());
        pending
            .create_directory(fs.root(), "pending")
            .await
            .unwrap();
        let pending_bytes = pending.finish().unwrap().to_bytes().unwrap();

        let mut advance = Planner::new(&fs.observe_latest().await.unwrap(), CommitId::generate());
        advance
            .create_directory(fs.root(), "advance")
            .await
            .unwrap();
        fs.commit(&advance.finish().unwrap()).await.unwrap();
        let latest = fs.observe_latest().await.unwrap();

        fs.revision_history_page(None, RETAINED_REVISIONS_PAGE_MAX)
            .await
            .unwrap();
        let history = fs
            .observe_state()
            .await
            .unwrap()
            .history()
            .0
            .clone()
            .expect("history index is present");
        let history_object = uuid::Uuid::from_bytes(history.object).simple().to_string();
        {
            let mut state = backend.state.lock().unwrap();
            let key = state
                .objects
                .keys()
                .find(|key| key.ends_with(&history_object))
                .cloned()
                .expect("history index page is stored");
            state.objects.get_mut(&key).unwrap().bytes[history.offset as usize] ^= 1;
        }

        let expiry = ExpiryRequest::new(
            fs.filesystem(),
            CommitId::generate(),
            latest.revision(),
            vec![older_revision],
        )
        .unwrap();
        assert_eq!(
            fs.expire(&expiry).await.unwrap_err().kind(),
            ErrorKind::Corrupt
        );
        assert_eq!(
            fs.restore_transaction(&older_request.to_bytes().unwrap())
                .await
                .unwrap_err()
                .kind(),
            ErrorKind::Corrupt
        );
        assert_eq!(
            fs.commit_frozen_batch(&[pending_bytes])
                .await
                .unwrap_err()
                .kind(),
            ErrorKind::Corrupt
        );
    }
}
