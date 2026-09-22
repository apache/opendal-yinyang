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
use crate::{DataStore, NodeId, PreparedContent, Result, Revision, Snapshot, Transaction};
use futures_util::future::BoxFuture;
use tokio::io::AsyncRead;

pub trait Authority: std::fmt::Debug + Send + Sync {
    fn data(&self) -> &DataStore;
    fn filesystem(&self) -> NodeId;
    fn root(&self) -> NodeId;
    fn observe_latest(&self) -> BoxFuture<'_, Result<Snapshot>>;
    fn observe_revision(&self, revision: Revision) -> BoxFuture<'_, Result<Snapshot>>;
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
    fn commit_batch<'a>(
        &'a self,
        requests: &'a [Transaction],
    ) -> BoxFuture<'a, Result<Vec<Outcome>>> {
        Box::pin(self.commit_batch(requests))
    }
    fn register<'a>(&'a self, content: &'a PreparedContent) -> BoxFuture<'a, Result<()>> {
        Box::pin(self.remember(content))
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
    fn restore_transaction<'a>(&'a self, bytes: &'a [u8]) -> BoxFuture<'a, Result<Transaction>> {
        Box::pin(self.restore_transaction(bytes))
    }
}
