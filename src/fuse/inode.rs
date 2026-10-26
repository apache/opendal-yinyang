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

//! Volume-lifetime numeric inode mapping for the Linux FUSE boundary.
//!
//! Inode numbers are deterministic functions of the core [`NodeId`], so they
//! are stable across mounts and restarts. The root is always 1. Identities
//! with a colliding derivation are rejected instead of aliased; collisions
//! are astronomically unlikely for 128-bit node identities.
//!
//! The map and the cached kind/executable observations live for the whole
//! mount, mirroring the macOS prototype's identity-map lifetime. Lookup
//! counters still track kernel references so a later layer can evict entries
//! without changing the deterministic inode numbers. This module holds no
//! lock while touching the authority or runtime.

use std::collections::BTreeMap;
use yinyang_core::NodeId;

/// The FUSE inode reserved for the volume root.
pub const ROOT_INODE: u64 = 1;

/// Domain separation keeps the derivation independent of content hashes.
const INODE_DOMAIN: &[u8] = b"yinyang fuse inode v1";

/// Kind-adjacent metadata observed from an authority snapshot.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CachedInfo {
    pub directory: bool,
    pub executable: bool,
    pub generation: u64,
}

/// Stable NodeId <-> inode mapping with kernel lookup counters.
pub struct InodeTable {
    by_inode: BTreeMap<u64, NodeId>,
    by_node: BTreeMap<NodeId, u64>,
    lookups: BTreeMap<u64, u64>,
    info: BTreeMap<NodeId, CachedInfo>,
}

impl InodeTable {
    pub fn new(root: NodeId) -> Self {
        Self {
            by_inode: [(ROOT_INODE, root)].into_iter().collect(),
            by_node: [(root, ROOT_INODE)].into_iter().collect(),
            lookups: BTreeMap::new(),
            info: BTreeMap::new(),
        }
    }

    /// Resolve or allocate the deterministic inode for a node identity.
    pub fn inode(&mut self, id: NodeId) -> Result<u64, &'static str> {
        if let Some(inode) = self.by_node.get(&id) {
            return Ok(*inode);
        }
        let inode = derive(id)?;
        if let Some(existing) = self.by_inode.get(&inode) {
            return Err(if *existing == id {
                "inode already mapped to another identity"
            } else {
                "inode collision"
            });
        }
        self.by_inode.insert(inode, id);
        self.by_node.insert(id, inode);
        Ok(inode)
    }

    /// Resolve an inode number to a node identity.
    pub fn node(&self, inode: u64) -> Option<NodeId> {
        self.by_inode.get(&inode).copied()
    }

    /// Remember one successful kernel lookup for an inode.
    pub fn remember_lookup(&mut self, inode: u64) {
        let count = self.lookups.entry(inode).or_default().saturating_add(1);
        self.lookups.insert(inode, count);
    }

    /// Forget `count` lookups. Mappings are retained for the mount lifetime.
    pub fn forget(&mut self, inode: u64, count: u64) {
        let Some(remaining) = self.lookups.get_mut(&inode) else {
            return;
        };
        *remaining = remaining.saturating_sub(count);
        if *remaining == 0 {
            self.lookups.remove(&inode);
        }
    }

    #[cfg(test)]
    pub fn lookup_count(&self, inode: u64) -> u64 {
        self.lookups.get(&inode).copied().unwrap_or(0)
    }

    /// Cache kind-adjacent metadata observed from a snapshot.
    pub fn cache(&mut self, id: NodeId, info: CachedInfo) {
        self.info.insert(id, info);
    }

    pub fn cached(&self, id: NodeId) -> Option<CachedInfo> {
        self.info.get(&id).copied()
    }

    /// Inode numbers mapped so far; used by the explicit refresh to invalidate
    /// only the attributes the kernel may cache.
    pub fn inodes(&self) -> Vec<u64> {
        self.by_inode.keys().copied().collect()
    }
}

fn derive(id: NodeId) -> Result<u64, &'static str> {
    let mut hasher = blake3::Hasher::new();
    hasher.update(INODE_DOMAIN);
    hasher.update(id.as_bytes());
    let hash = hasher.finalize();
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&hash.as_bytes()[..8]);
    let inode = u64::from_be_bytes(bytes);
    if inode <= ROOT_INODE {
        return Err("derived inode collides with the root mapping");
    }
    Ok(inode)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn root_is_always_inode_one() {
        let root = NodeId::generate();
        let mut table = InodeTable::new(root);
        assert_eq!(table.inode(root).unwrap(), ROOT_INODE);
        assert_eq!(table.node(ROOT_INODE), Some(root));
    }

    #[test]
    fn derivation_is_stable_and_unique() {
        let root = NodeId::generate();
        let mut table = InodeTable::new(root);
        let ids: Vec<NodeId> = (0..512).map(|_| NodeId::generate()).collect();
        let first: Vec<u64> = ids.iter().map(|id| table.inode(*id).unwrap()).collect();
        let mut second = InodeTable::new(root);
        let repeated: Vec<u64> = ids.iter().map(|id| second.inode(*id).unwrap()).collect();
        assert_eq!(first, repeated);
        for (index, inode) in first.iter().enumerate() {
            assert_ne!(*inode, ROOT_INODE);
            assert_eq!(second.node(*inode), Some(ids[index]));
            assert!(!first[..index].contains(inode));
        }
    }

    #[test]
    fn lookup_counters_are_saturating() {
        let root = NodeId::generate();
        let mut table = InodeTable::new(root);
        let id = NodeId::generate();
        let inode = table.inode(id).unwrap();
        table.remember_lookup(inode);
        table.remember_lookup(inode);
        assert_eq!(table.lookup_count(inode), 2);
        table.forget(inode, 1);
        assert_eq!(table.lookup_count(inode), 1);
        table.forget(inode, 100);
        assert_eq!(table.lookup_count(inode), 0);
        assert_eq!(table.node(inode), Some(id));
    }

    #[test]
    fn cached_kind_tracks_observations() {
        let root = NodeId::generate();
        let mut table = InodeTable::new(root);
        let id = NodeId::generate();
        assert_eq!(table.cached(id), None);
        let info = CachedInfo {
            directory: false,
            executable: true,
            generation: 3,
        };
        table.cache(id, info);
        assert_eq!(table.cached(id), Some(info));
    }
}
