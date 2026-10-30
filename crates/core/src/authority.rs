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
    ContentEdit, DataStore, FileVersion, NodeId, PreparedContent, Result, Revision, Snapshot,
    Transaction,
};
use futures_util::future::BoxFuture;
use std::collections::BTreeSet;
use tokio::io::AsyncRead;

/// Maximum number of retained revisions returned by one authority request.
pub const RETAINED_REVISIONS_PAGE_MAX: u32 = 256;

/// One strictly ordered page from an authority's retained revision index.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RevisionPage {
    /// Revisions returned by this request.
    pub revisions: Vec<Revision>,
    /// Exclusive continuation for the next page.
    pub next: Option<Revision>,
}

pub trait Authority: std::fmt::Debug + Send + Sync {
    fn data(&self) -> &DataStore;
    fn filesystem(&self) -> NodeId;
    fn root(&self) -> NodeId;
    fn observe_latest(&self) -> BoxFuture<'_, Result<Snapshot>>;
    fn observe_revision(&self, revision: Revision) -> BoxFuture<'_, Result<Snapshot>>;
    /// List metadata-authenticated revisions retained by this authority.
    ///
    /// The default keeps backends without a revision index explicit rather
    /// than inferring history from content or local staging.
    fn retained_revisions_page(
        &self,
        _after: Option<Revision>,
        _limit: u32,
    ) -> BoxFuture<'_, Result<RevisionPage>> {
        Box::pin(async {
            Err(crate::Error::unsupported(
                "list retained revisions",
                "authority does not expose a revision index",
            ))
        })
    }
    /// List every retained revision using bounded authority pages.
    fn retained_revisions(&self) -> BoxFuture<'_, Result<Vec<Revision>>> {
        Box::pin(async move {
            let mut after = None;
            let mut revisions = Vec::new();
            let mut seen = BTreeSet::new();
            loop {
                let page = self
                    .retained_revisions_page(after, RETAINED_REVISIONS_PAGE_MAX)
                    .await?;
                if page.revisions.is_empty() {
                    if page.next.is_some() {
                        return Err(crate::Error::new(
                            crate::ErrorKind::Corrupt,
                            "list retained revisions",
                            "authority returned an empty non-terminal page",
                        ));
                    }
                    break;
                }
                for revision in page.revisions {
                    if after.is_some_and(|cursor| revision <= cursor) || !seen.insert(revision) {
                        return Err(crate::Error::new(
                            crate::ErrorKind::Corrupt,
                            "list retained revisions",
                            "authority page is not strictly ordered",
                        ));
                    }
                    revisions.push(revision);
                }
                let last = revisions.last().copied();
                match page.next {
                    Some(next)
                        if after.is_none_or(|cursor| next > cursor) && last == Some(next) =>
                    {
                        after = Some(next);
                    }
                    Some(_) => {
                        return Err(crate::Error::new(
                            crate::ErrorKind::Corrupt,
                            "list retained revisions",
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
    fn inspect_physical(&self, snapshot: &Snapshot) -> BoxFuture<'_, PhysicalInspection> {
        Box::pin(crate::reachability::inspect_authority(self, snapshot))
    }
    fn commit_batch<'a>(
        &'a self,
        requests: &'a [Transaction],
    ) -> BoxFuture<'a, Result<Vec<Outcome>>>;
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
    /// not a newly observed latest revision. Length-changing writes must use
    /// `prepare` instead.
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
impl Authority for ObjectFs {
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
    fn observe_revision(&self, r: Revision) -> BoxFuture<'_, Result<Snapshot>> {
        Box::pin(self.observe_revision(r))
    }
    fn retained_revisions_page(
        &self,
        after: Option<Revision>,
        limit: u32,
    ) -> BoxFuture<'_, Result<RevisionPage>> {
        Box::pin(self.retained_revisions_page(after, limit))
    }
    fn inspect_physical(&self, snapshot: &Snapshot) -> BoxFuture<'_, PhysicalInspection> {
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
    fn commit_batch<'a>(
        &'a self,
        requests: &'a [Transaction],
    ) -> BoxFuture<'a, Result<Vec<Outcome>>> {
        Box::pin(self.commit_batch(requests))
    }
}
impl Authority for crate::service::MetadataService {
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
    fn observe_revision(&self, r: Revision) -> BoxFuture<'_, Result<Snapshot>> {
        Box::pin(self.observe_revision(r))
    }
    fn retained_revisions_page(
        &self,
        after: Option<Revision>,
        limit: u32,
    ) -> BoxFuture<'_, Result<RevisionPage>> {
        Box::pin(self.retained_revisions_page(after, limit))
    }
    fn commit_batch<'a>(
        &'a self,
        requests: &'a [Transaction],
    ) -> BoxFuture<'a, Result<Vec<Outcome>>> {
        Box::pin(self.commit_batch(requests))
    }
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
    fn restore_transaction<'a>(&'a self, bytes: &'a [u8]) -> BoxFuture<'a, Result<Transaction>> {
        Box::pin(self.restore_transaction(bytes))
    }
}
impl Authority for crate::service::ServiceClient {
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
    fn observe_revision(&self, r: Revision) -> BoxFuture<'_, Result<Snapshot>> {
        Box::pin(self.observe_revision(r))
    }
    fn commit_batch<'a>(
        &'a self,
        requests: &'a [Transaction],
    ) -> BoxFuture<'a, Result<Vec<Outcome>>> {
        Box::pin(self.commit_batch(requests))
    }
    fn register<'a>(&'a self, content: &'a PreparedContent) -> BoxFuture<'a, Result<()>> {
        Box::pin(self.register(content))
    }
    fn prepare_overwrite<'a>(
        &'a self,
        base: &'a FileVersion,
        edits: &'a [ContentEdit<'a>],
    ) -> BoxFuture<'a, Result<PreparedContent>> {
        Box::pin(self.prepare_overwrite(base, edits))
    }
    fn restore_transaction<'a>(&'a self, bytes: &'a [u8]) -> BoxFuture<'a, Result<Transaction>> {
        Box::pin(self.restore_transaction(bytes))
    }
}
