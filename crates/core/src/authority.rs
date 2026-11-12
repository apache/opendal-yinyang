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

//! Shared publication interface; access runtimes do not select physical metadata.
use crate::object::{ObjectFs, Outcome};
use crate::reachability::PhysicalInspection;
use crate::{
    ContentEdit, DataStore, ExpiryRequest, FileVersion, NodeId, PreparedContent, Result, Revision,
    Snapshot, Transaction,
};
use futures_util::future::BoxFuture;
use tokio::io::AsyncRead;

/// Maximum number of retained revisions returned by one authority request.
pub const RETAINED_REVISIONS_PAGE_MAX: u32 = 256;

/// Whether a known revision may still be opened by a fresh observation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RevisionStatus {
    Readable,
    Expired,
}

/// One known revision and its current fresh-lookup state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RevisionRecord {
    pub revision: Revision,
    pub status: RevisionStatus,
}

/// One strictly ordered page of known revision history.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RevisionHistoryPage {
    pub revisions: Vec<RevisionRecord>,
    pub next: Option<Revision>,
}

pub trait Authority: std::fmt::Debug + Send + Sync {
    fn data(&self) -> &DataStore;
    fn filesystem(&self) -> NodeId;
    fn root(&self) -> NodeId;
    fn observe_latest(&self) -> BoxFuture<'_, Result<Snapshot>>;
    fn observe_revision(&self, revision: Revision) -> BoxFuture<'_, Result<Snapshot>>;
    /// List every currently readable revision using bounded typed history pages.
    fn retained_revisions(&self) -> BoxFuture<'_, Result<Vec<Revision>>> {
        Box::pin(async move {
            Ok(self
                .revision_history()
                .await?
                .into_iter()
                .filter(|record| record.status == RevisionStatus::Readable)
                .map(|record| record.revision)
                .collect())
        })
    }
    /// List known readable and expired revisions using bounded authority pages.
    fn revision_history_page(
        &self,
        _after: Option<Revision>,
        _limit: u32,
    ) -> BoxFuture<'_, Result<RevisionHistoryPage>> {
        Box::pin(async {
            Err(crate::Error::unsupported(
                "list revision history",
                "authority does not expose revision history",
            ))
        })
    }
    /// List all known revisions, including their expiry state.
    fn revision_history(&self) -> BoxFuture<'_, Result<Vec<RevisionRecord>>> {
        Box::pin(async move {
            let mut revisions = Vec::new();
            let mut previous = None;
            loop {
                let page = self
                    .revision_history_page(previous, RETAINED_REVISIONS_PAGE_MAX)
                    .await?;
                if page.revisions.is_empty() {
                    if page.next.is_some() {
                        return Err(crate::Error::new(
                            crate::ErrorKind::Corrupt,
                            "list revision history",
                            "authority returned an empty non-terminal page",
                        ));
                    }
                    break;
                }
                for record in page.revisions {
                    if previous.is_some_and(|previous| record.revision <= previous) {
                        return Err(crate::Error::new(
                            crate::ErrorKind::Corrupt,
                            "list revision history",
                            "authority page is not strictly ordered",
                        ));
                    }
                    previous = Some(record.revision);
                    revisions.push(record);
                }
                match page.next {
                    Some(next) if previous == Some(next) => {}
                    Some(_) => {
                        return Err(crate::Error::new(
                            crate::ErrorKind::Corrupt,
                            "list revision history",
                            "authority continuation does not advance",
                        ));
                    }
                    None => break,
                }
            }
            Ok(revisions)
        })
    }
    /// Traverse authenticated physical references reachable from this
    /// authority's observed metadata roots without reading leaf payloads.
    fn inspect_physical<'a>(&'a self, snapshot: &'a Snapshot) -> BoxFuture<'a, PhysicalInspection> {
        Box::pin(crate::reachability::inspect_authority(self, snapshot))
    }
    fn commit_batch<'a>(
        &'a self,
        requests: &'a [Transaction],
    ) -> BoxFuture<'a, Result<Vec<Outcome>>>;
    /// Mark exact known revisions logically expired.
    ///
    /// Implementations must check an existing request receipt before consulting
    /// expected-latest or candidate state so an exact retry can reuse its
    /// original receipt after later publications.
    fn expire<'a>(&'a self, _request: &'a ExpiryRequest) -> BoxFuture<'a, Result<Outcome>> {
        Box::pin(async {
            Err(crate::Error::unsupported(
                "expire revisions",
                "authority does not support explicit expiry",
            ))
        })
    }
    /// Replay exact frozen-plan bytes.
    ///
    /// The default preserves correctness for simple authorities. Authorities
    /// that can resolve receipts without restoring content should override it.
    fn commit_frozen_batch<'a>(
        &'a self,
        requests: &'a [Vec<u8>],
    ) -> BoxFuture<'a, Result<Vec<Outcome>>> {
        Box::pin(async move {
            let mut restored = Vec::with_capacity(requests.len());
            for request in requests {
                restored.push(self.restore_transaction(request).await?);
            }
            self.commit_batch(&restored).await
        })
    }
    fn register<'a>(&'a self, content: &'a PreparedContent) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            self.data().accept(content)?;
            Ok(())
        })
    }
    fn prepare<'a>(
        &'a self,
        mut source: &'a mut (dyn AsyncRead + Unpin + Send),
    ) -> BoxFuture<'a, Result<PreparedContent>> {
        Box::pin(async move {
            let content = self.data().prepare(&mut source).await?;
            self.register(&content).await?;
            Ok(content)
        })
    }
    /// Prepare a same-length update from one exact published file version.
    ///
    /// The base is the metadata-authenticated version selected by the caller,
    /// not a newly observed latest revision. Length-changing writes use
    /// `prepare_update` when their generated work stays within the bounded
    /// update budget.
    fn prepare_overwrite<'a>(
        &'a self,
        base: &'a FileVersion,
        edits: &'a [ContentEdit<'a>],
    ) -> BoxFuture<'a, Result<PreparedContent>> {
        Box::pin(async move {
            let base = base.prepared_content()?;
            let content = self.data().overwrite_prepared_ranges(&base, edits).await?;
            self.register(&content).await?;
            Ok(content)
        })
    }
    /// Prepare a length-changing update from one exact published file version.
    ///
    /// The output retains base bytes below `preserved_prefix`, zero-fills to
    /// `final_length`, then overlays the bounded edits. Base bytes beyond the
    /// preserved prefix are never reused. Length changes that exceed caller
    /// resource bounds must use the explicit streaming `prepare` fallback.
    fn prepare_update<'a>(
        &'a self,
        base: &'a FileVersion,
        preserved_prefix: u64,
        final_length: u64,
        edits: &'a [ContentEdit<'a>],
    ) -> BoxFuture<'a, Result<PreparedContent>> {
        Box::pin(async move {
            let base = base.prepared_content()?;
            let content = self
                .data()
                .update_prepared(&base, preserved_prefix, final_length, edits)
                .await?;
            self.register(&content).await?;
            Ok(content)
        })
    }
    fn commit<'a>(&'a self, request: &'a Transaction) -> BoxFuture<'a, Result<Outcome>> {
        Box::pin(async move {
            Ok(self
                .commit_batch(std::slice::from_ref(request))
                .await?
                .remove(0))
        })
    }
    fn restore_transaction<'a>(&'a self, bytes: &'a [u8]) -> BoxFuture<'a, Result<Transaction>> {
        Box::pin(async move {
            let (revision, files) = Transaction::inspect(bytes)?;
            let snapshot = self.observe_revision(revision).await?;
            let mut prepared = Vec::new();
            for file in files {
                prepared.push(self.data().import(&file).await?);
            }
            Transaction::restore(bytes, &snapshot, &prepared).await
        })
    }
}

macro_rules! impl_authority_forwarding {
    ($authority:ty, { $($additional:item)* }) => {
        impl Authority for $authority {
            fn data(&self) -> &DataStore {
                self.data()
            }
            fn filesystem(&self) -> NodeId {
                self.filesystem()
            }
            fn root(&self) -> NodeId {
                self.root()
            }
            fn observe_latest(&self) -> BoxFuture<'_, Result<Snapshot>> {
                Box::pin(self.observe_latest())
            }
            fn observe_revision(&self, revision: Revision) -> BoxFuture<'_, Result<Snapshot>> {
                Box::pin(self.observe_revision(revision))
            }
            fn revision_history_page(
                &self,
                after: Option<Revision>,
                limit: u32,
            ) -> BoxFuture<'_, Result<RevisionHistoryPage>> {
                Box::pin(self.revision_history_page(after, limit))
            }
            fn commit_batch<'a>(
                &'a self,
                requests: &'a [Transaction],
            ) -> BoxFuture<'a, Result<Vec<Outcome>>> {
                Box::pin(self.commit_batch(requests))
            }
            fn expire<'a>(
                &'a self,
                request: &'a ExpiryRequest,
            ) -> BoxFuture<'a, Result<Outcome>> {
                Box::pin(self.expire(request))
            }
            fn commit_frozen_batch<'a>(
                &'a self,
                requests: &'a [Vec<u8>],
            ) -> BoxFuture<'a, Result<Vec<Outcome>>> {
                Box::pin(self.commit_frozen_batch(requests))
            }
            $($additional)*
        }
    };
}

impl_authority_forwarding!(ObjectFs, {
    fn inspect_physical<'a>(&'a self, snapshot: &'a Snapshot) -> BoxFuture<'a, PhysicalInspection> {
        Box::pin(async move {
            match self.observe_state().await {
                Ok(state) => {
                    crate::reachability::inspect_object_state(snapshot.revision(), state).await
                }
                Err(error) => {
                    let mut inspection = PhysicalInspection::default();
                    inspection.issue(&error, "object-history");
                    inspection
                }
            }
        })
    }
    fn restore_transaction<'a>(&'a self, bytes: &'a [u8]) -> BoxFuture<'a, Result<Transaction>> {
        Box::pin(self.restore_transaction(bytes))
    }
});

impl_authority_forwarding!(crate::service::MetadataService, {
    fn register<'a>(&'a self, content: &'a PreparedContent) -> BoxFuture<'a, Result<()>> {
        Box::pin(self.remember(content))
    }
    fn prepare_overwrite<'a>(
        &'a self,
        base: &'a FileVersion,
        edits: &'a [ContentEdit<'a>],
    ) -> BoxFuture<'a, Result<PreparedContent>> {
        Box::pin(self.prepare_overwrite(base, edits))
    }
    fn prepare_update<'a>(
        &'a self,
        base: &'a FileVersion,
        preserved_prefix: u64,
        final_length: u64,
        edits: &'a [ContentEdit<'a>],
    ) -> BoxFuture<'a, Result<PreparedContent>> {
        Box::pin(self.prepare_update(base, preserved_prefix, final_length, edits))
    }
    fn restore_transaction<'a>(&'a self, bytes: &'a [u8]) -> BoxFuture<'a, Result<Transaction>> {
        Box::pin(self.restore_transaction(bytes))
    }
});

impl_authority_forwarding!(crate::service::ServiceClient, {
    fn register<'a>(&'a self, content: &'a PreparedContent) -> BoxFuture<'a, Result<()>> {
        Box::pin(self.register(content))
    }
    fn prepare<'a>(
        &'a self,
        source: &'a mut (dyn AsyncRead + Unpin + Send),
    ) -> BoxFuture<'a, Result<PreparedContent>> {
        Box::pin(crate::service::ServiceClient::prepare(self, source))
    }
    fn prepare_overwrite<'a>(
        &'a self,
        base: &'a FileVersion,
        edits: &'a [ContentEdit<'a>],
    ) -> BoxFuture<'a, Result<PreparedContent>> {
        Box::pin(self.prepare_overwrite(base, edits))
    }
    fn prepare_update<'a>(
        &'a self,
        base: &'a FileVersion,
        preserved_prefix: u64,
        final_length: u64,
        edits: &'a [ContentEdit<'a>],
    ) -> BoxFuture<'a, Result<PreparedContent>> {
        Box::pin(self.prepare_update(base, preserved_prefix, final_length, edits))
    }
    fn restore_transaction<'a>(&'a self, bytes: &'a [u8]) -> BoxFuture<'a, Result<Transaction>> {
        Box::pin(self.restore_transaction(bytes))
    }
});
