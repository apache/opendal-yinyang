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
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use tokio::sync::{Mutex, Semaphore};
use yinyang_core::{Authority, NodeId, Receipt};

struct Node {
    handle: Mutex<FileHandle>,
    references: AtomicUsize,
}
struct OpenGate {
    inner: Arc<Inner>,
    id: NodeId,
    ready: Arc<Semaphore>,
}
impl Drop for OpenGate {
    fn drop(&mut self) {
        if let Ok(mut opening) = self.inner.opening.lock() {
            opening.remove(&self.id);
        }
        // Close wakes every waiter; a single permit would strand later waiters.
        self.ready.close();
    }
}
struct NodeGuard {
    node: Arc<Node>,
}
impl NodeGuard {
    fn new(node: Arc<Node>) -> Self {
        node.references.fetch_add(1, Ordering::Relaxed);
        Self { node }
    }
}
impl Drop for NodeGuard {
    fn drop(&mut self) {
        self.node.references.fetch_sub(1, Ordering::Relaxed);
    }
}
struct Inner {
    runtime: Runtime,
    read_only: bool,
    nodes: StdMutex<BTreeMap<NodeId, Arc<Node>>>,
    opening: StdMutex<BTreeMap<NodeId, Arc<Semaphore>>>,
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
                nodes: StdMutex::new(nodes),
                opening: StdMutex::new(BTreeMap::new()),
            }),
        })
    }
    pub fn authority(&self) -> &dyn Authority {
        self.inner.runtime.authority()
    }
    pub async fn status(&self) -> Result<RuntimeStatus> {
        self.inner.runtime.status().await
    }
    /// Observe staged metadata for one shared node without opening a reference.
    /// `None` means this mount has no shared handle for the node.
    pub async fn node_status(&self, id: NodeId) -> Result<Option<HandleStatus>> {
        let guard = {
            let nodes = self.inner.nodes.lock().map_err(Error::local)?;
            nodes.get(&id).cloned().map(NodeGuard::new)
        };
        let Some(guard) = guard else {
            return Ok(None);
        };
        guard.node.handle.lock().await.status().await.map(Some)
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
        loop {
            // Cache lookup and gate registration must be one critical section.
            // Always take nodes then opening, and never hold either across await.
            let (existing, gate) = {
                let nodes = self.inner.nodes.lock().map_err(Error::local)?;
                if let Some(node) = nodes.get(&id) {
                    node.references.fetch_add(1, Ordering::Relaxed);
                    (Some(node.clone()), None)
                } else {
                    let mut opening = self.inner.opening.lock().map_err(Error::local)?;
                    match opening.get(&id) {
                        Some(ready) => (None, Some((ready.clone(), true))),
                        None => {
                            let ready = Arc::new(Semaphore::new(0));
                            opening.insert(id, ready.clone());
                            (None, Some((ready, false)))
                        }
                    }
                }
            };
            if let Some(node) = existing {
                return Ok(MountFile { node, writable });
            }
            let (ready, waiting) = gate.expect("a waiter or owner always carries the node gate");
            if waiting {
                // Closing the gate is a broadcast completion signal, not an error.
                let _ = ready.acquire().await;
                continue;
            }

            // Only one task materializes a NodeId. The namespace map is not
            // held across the runtime's remote observation and staging read.
            let gate = OpenGate {
                inner: self.inner.clone(),
                id,
                ready,
            };
            let handle = self
                .inner
                .runtime
                .open_node(id, !self.inner.read_only)
                .await?;
            let node = Arc::new(Node {
                handle: Mutex::new(handle),
                references: AtomicUsize::new(0),
            });
            {
                let mut nodes = self.inner.nodes.lock().map_err(Error::local)?;
                nodes.insert(id, node.clone());
                node.references.fetch_add(1, Ordering::Relaxed);
            }
            drop(gate);
            return Ok(MountFile { node, writable });
        }
    }
    /// Replace a clean local version with a fresh remote observation. Pending,
    /// frozen or failed versions are never rebased. Adapters must coordinate any
    /// OS cache invalidation outside this call; no OS callbacks run under locks.
    pub async fn refresh(&self, id: NodeId) -> Result<()> {
        let guard = {
            let nodes = self.inner.nodes.lock().map_err(Error::local)?;
            nodes.get(&id).cloned().map(NodeGuard::new)
        };
        let Some(guard) = guard else {
            return Ok(());
        };
        let mut handle = guard.node.handle.lock().await;
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
        let nodes: Vec<_> = {
            let nodes = self.inner.nodes.lock().map_err(Error::local)?;
            nodes.values().cloned().map(NodeGuard::new).collect()
        };
        let mut failure = None;
        for node in &nodes {
            if let Err(error) = node.node.handle.lock().await.fsync().await {
                failure.get_or_insert(error);
            }
        }
        failure.map_or(Ok(()), Err)
    }
    /// Forget only an unreferenced, clean node. Failed and pending nodes remain
    /// recoverable even after the OS releases all references.
    pub async fn reclaim(&self, id: NodeId) -> Result<()> {
        let node = {
            let nodes = self.inner.nodes.lock().map_err(Error::local)?;
            let Some(node) = nodes.get(&id) else {
                return Ok(());
            };
            if node.references.load(Ordering::Relaxed) != 0 {
                return Err(Error::State(
                    ErrorKind::Busy,
                    "node still has open references",
                ));
            }
            node.clone()
        };
        {
            let handle = node.handle.lock().await;
            let state = handle.status().await?;
            if state.pending || state.frozen || state.error.is_some() {
                return Err(Error::State(
                    ErrorKind::Busy,
                    "node has retained writes or failures",
                ));
            }
        }
        {
            let mut nodes = self.inner.nodes.lock().map_err(Error::local)?;
            let current = nodes.get(&id).cloned();
            let Some(current) = current else {
                return Ok(());
            };
            if current.references.load(Ordering::Relaxed) != 0 || !Arc::ptr_eq(&current, &node) {
                return Err(Error::State(
                    ErrorKind::Busy,
                    "node state changed during reclaim",
                ));
            }
            let _ = nodes.remove(&id);
        }
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
    /// Publish staged bytes for a FUSE flush or close that can report errors.
    /// Dropping or releasing the reference never performs this operation.
    pub async fn flush(&self) -> Result<Option<Receipt>> {
        self.fsync().await
    }
    /// Publish this node's dirty content first, then update the executable
    /// flag and advance the shared clean handle to the metadata revision.
    pub async fn set_executable(&self, executable: bool) -> Result<Receipt> {
        self.require_write()?;
        self.node
            .handle
            .lock()
            .await
            .set_executable(executable)
            .await
    }
}
