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

//! Instance-local, identity-based state for online mount adapters.
use crate::runtime::{Error, ErrorKind, FileHandle, HandleStatus, Result, Runtime, RuntimeStatus};
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::Mutex;
use yinyang_core::{Authority, NodeId, Receipt};

struct Node {
    handle: Mutex<FileHandle>,
    references: AtomicUsize,
}
struct Inner {
    runtime: Runtime,
    read_only: bool,
    nodes: Mutex<BTreeMap<NodeId, Arc<Node>>>,
}
/// A mount instance owns one recoverable staged version per node, not per open.
/// It does not install a mount or manage kernel caches.
#[derive(Clone)]
pub struct Mount {
    inner: Arc<Inner>,
}
impl Mount {
    pub async fn open(
        authority: Arc<dyn Authority>,
        staging: impl AsRef<Path>,
        read_only: bool,
    ) -> Result<Self> {
        let runtime = if read_only {
            Runtime::open_read_only(authority, staging).await?
        } else {
            Runtime::open(authority, staging).await?
        };
        let mut nodes = BTreeMap::new();
        for state in runtime.recoverable().await? {
            if !state.pending && !state.frozen && state.error.is_none() {
                // Clean observations are disposable. This also collects a
                // replacement materialized before an interrupted refresh.
                runtime.recover(state.id).await?.abort().await?;
                continue;
            }
            if nodes.contains_key(&state.node) {
                return Err(Error::Invalid(
                    "multiple staged versions for one mount node",
                ));
            }
            nodes.insert(
                state.node,
                Arc::new(Node {
                    handle: Mutex::new(runtime.recover(state.id).await?),
                    references: AtomicUsize::new(0),
                }),
            );
        }
        Ok(Self {
            inner: Arc::new(Inner {
                runtime,
                read_only,
                nodes: Mutex::new(nodes),
            }),
        })
    }
    pub fn authority(&self) -> &dyn Authority {
        self.inner.runtime.authority()
    }
    pub async fn status(&self) -> Result<RuntimeStatus> {
        self.inner.runtime.status().await
    }
    /// Namespace operations use the same conditional runtime contract. Do not
    /// open private FileHandles through this runtime alongside Mount references.
    pub fn namespace(&self) -> &Runtime {
        &self.inner.runtime
    }
    pub async fn open_node(&self, id: NodeId, writable: bool) -> Result<MountFile> {
        if writable && self.inner.read_only {
            return Err(Error::State(ErrorKind::ReadOnly, "mount is read-only"));
        }
        // Serialize first materialization with opens and reclaim. File I/O and
        // publication use per-node locks, never this namespace lock.
        let mut nodes = self.inner.nodes.lock().await;
        let node = match nodes.get(&id) {
            Some(node) => node.clone(),
            None => {
                let handle = self
                    .inner
                    .runtime
                    .open_node(id, !self.inner.read_only)
                    .await?;
                let node = Arc::new(Node {
                    handle: Mutex::new(handle),
                    references: AtomicUsize::new(0),
                });
                nodes.insert(id, node.clone());
                node
            }
        };
        node.references.fetch_add(1, Ordering::Relaxed);
        Ok(MountFile { node, writable })
    }
    /// Replace a clean local version with a fresh remote observation. Pending,
    /// frozen or failed versions are never rebased. Adapters must coordinate any
    /// OS cache invalidation outside this call; no OS callbacks run under locks.
    pub async fn refresh(&self, id: NodeId) -> Result<()> {
        let nodes = self.inner.nodes.lock().await;
        let Some(node) = nodes.get(&id) else {
            return Ok(());
        };
        let mut handle = node.handle.lock().await;
        let state = handle.status().await?;
        if state.pending || state.frozen || state.error.is_some() {
            return Err(Error::State(
                ErrorKind::Busy,
                "cannot refresh pending or failed node",
            ));
        }
        // Open first: a failed/cancelled fetch must leave the existing version
        // readable. Neither handle is exposed until the replacement is complete.
        let fresh = self
            .inner
            .runtime
            .open_node(id, !self.inner.read_only)
            .await?;
        let mut old = std::mem::replace(&mut *handle, fresh);
        old.close().await
    }
    /// Persist every currently staged node remotely. Continue after one failure
    /// so independent nodes can finish; the original failures remain in status.
    pub async fn fsync(&self) -> Result<()> {
        let nodes: Vec<_> = self.inner.nodes.lock().await.values().cloned().collect();
        let mut failure = None;
        for node in nodes {
            if let Err(error) = node.handle.lock().await.fsync().await {
                failure.get_or_insert(error);
            }
        }
        failure.map_or(Ok(()), Err)
    }
    /// Forget only an unreferenced, clean node. Failed and pending nodes remain
    /// recoverable even after the OS releases all references.
    pub async fn reclaim(&self, id: NodeId) -> Result<()> {
        let mut nodes = self.inner.nodes.lock().await;
        let Some(node) = nodes.get(&id) else {
            return Ok(());
        };
        if node.references.load(Ordering::Relaxed) != 0 {
            return Err(Error::State(
                ErrorKind::Busy,
                "node still has open references",
            ));
        }
        let handle = node.handle.lock().await;
        let state = handle.status().await?;
        if state.pending || state.frozen || state.error.is_some() {
            return Err(Error::State(
                ErrorKind::Busy,
                "node has retained writes or failures",
            ));
        }
        drop(handle);
        let node = nodes
            .remove(&id)
            .expect("node exists under the namespace lock");
        node.handle.lock().await.close().await
    }
}
/// An access-checked reference to a shared node. Dropping it performs no I/O,
/// publication or deletion. An adapter with an unreportable final close must
/// retain errors through Mount status instead of claiming successful fsync.
pub struct MountFile {
    node: Arc<Node>,
    writable: bool,
}
impl Drop for MountFile {
    fn drop(&mut self) {
        self.node.references.fetch_sub(1, Ordering::Relaxed);
    }
}
impl MountFile {
    fn require_write(&self) -> Result<()> {
        if self.writable {
            Ok(())
        } else {
            Err(Error::State(ErrorKind::ReadOnly, "reference is read-only"))
        }
    }
    pub async fn status(&self) -> Result<HandleStatus> {
        self.node.handle.lock().await.status().await
    }
    pub async fn read(&self, offset: u64, length: usize) -> Result<Vec<u8>> {
        self.node.handle.lock().await.read(offset, length).await
    }
    pub async fn write(&self, offset: u64, bytes: &[u8]) -> Result<()> {
        self.require_write()?;
        self.node.handle.lock().await.write(offset, bytes).await
    }
    pub async fn append(&self, bytes: &[u8]) -> Result<u64> {
        self.require_write()?;
        self.node.handle.lock().await.append(bytes).await
    }
    pub async fn truncate(&self, length: u64) -> Result<()> {
        self.require_write()?;
        self.node.handle.lock().await.truncate(length).await
    }
    pub async fn fsync(&self) -> Result<Option<Receipt>> {
        self.node.handle.lock().await.fsync().await
    }
}
