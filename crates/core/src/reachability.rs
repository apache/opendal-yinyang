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

//! Read-only physical-reference reachability for published authority roots.

use crate::authority::{Authority, RETAINED_REVISIONS_PAGE_MAX};
use crate::data::{ContentDescriptor, ContentReferenceKind, DataStore, PackedRef, RefWire};
use crate::index::{Index, IndexCache, MetadataVisitor};
use crate::namespace::{DirectoryEntry, Node, NodeKind, decode};
use crate::object::ObjectState;
use crate::snapshot::{ChangeRecord, Receipt, Revision, Snapshot};
use crate::{Error, ErrorKind, NodeId, Result};
use std::collections::{BTreeMap, BTreeSet, HashSet, VecDeque};
use std::sync::Arc;

const SCAN_PAGE: usize = 4096;
const MAX_ISSUES: usize = 256;
const MAX_WALK_NODES: u64 = 1_000_000;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum ObjectTable {
    Nodes,
    Entries,
    Receipts,
    Changes,
    History,
}

impl ObjectTable {
    const fn tag(self) -> u8 {
        self as u8
    }
}

/// One authenticated physical extent referenced by an observed authority root.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct PhysicalReference {
    /// Physical role of the referenced extent.
    pub kind: PhysicalReferenceKind,
    /// Immutable pack identity.
    pub object: [u8; 16],
    /// Byte offset within the pack.
    pub offset: u64,
    /// Referenced byte length.
    pub length: u32,
    /// Authentication digest carried by the parent reference.
    pub digest: [u8; 32],
}

/// Physical roles kept separate because not all references are payload reads.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum PhysicalReferenceKind {
    /// A snapshot descriptor referenced by history.
    SnapshotDescriptor,
    /// An authenticated metadata B+ tree page or branch child.
    MetadataIndex,
    /// An external metadata value referenced by a leaf record.
    MetadataValue,
    /// An authenticated content hash-index node.
    ContentIndex,
    /// A content leaf extent whose payload was not downloaded by reachability.
    ContentExtent,
}

/// A non-fatal traversal failure. Any issue keeps the physical result partial.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhysicalIssue {
    /// Core error category from the failed traversal.
    pub kind: ErrorKind,
    /// Logical subject or physical object being inspected.
    pub subject: String,
    /// Bounded diagnostic detail.
    pub detail: String,
}

/// Deduplicated coverage for one immutable pack.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhysicalPackUsage {
    /// Immutable pack identity.
    pub object: [u8; 16],
    /// Number of distinct authenticated references into this pack.
    pub references: u64,
    /// Non-overlapping half-open byte intervals covered by those references.
    pub intervals: Vec<(u64, u64)>,
    /// Sum of the non-overlapping interval lengths.
    pub covered_bytes: u64,
}

/// Result of inspecting all physical roots known to one authority.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PhysicalInspection {
    references: BTreeSet<PhysicalReference>,
    content_descriptors: HashSet<ContentDescriptor>,
    retained_revisions: Option<u64>,
    issues: Vec<PhysicalIssue>,
    issues_omitted: u64,
}

impl PhysicalInspection {
    /// Return all distinct physical references.
    pub fn references(&self) -> &BTreeSet<PhysicalReference> {
        &self.references
    }

    /// Count distinct references of one physical role.
    pub fn reference_count(&self, kind: PhysicalReferenceKind) -> u64 {
        self.references
            .iter()
            .filter(|reference| reference.kind == kind)
            .count()
            .try_into()
            .unwrap_or(u64::MAX)
    }

    /// Number of deduplicated content descriptors reached by observed metadata.
    pub fn content_descriptor_count(&self) -> u64 {
        self.content_descriptors
            .len()
            .try_into()
            .unwrap_or(u64::MAX)
    }

    /// Number of metadata-authenticated retained revisions, when enumerable.
    pub const fn retained_revisions(&self) -> Option<u64> {
        self.retained_revisions
    }

    /// Return the non-fatal traversal failures retained by this inspection.
    pub fn issues(&self) -> &[PhysicalIssue] {
        &self.issues
    }

    /// Number of failures omitted after the bounded issue list filled.
    pub const fn issues_omitted(&self) -> u64 {
        self.issues_omitted
    }

    /// A complete result has an enumerated retained-revision set and no issue.
    pub fn is_complete(&self) -> bool {
        self.retained_revisions.is_some() && self.issues.is_empty()
    }

    /// Merge coverage by pack identity and merge byte intervals.
    pub fn packs(&self) -> Vec<PhysicalPackUsage> {
        let mut packs = BTreeMap::<[u8; 16], (u64, BTreeMap<u64, u64>)>::new();
        for reference in &self.references {
            let (references, intervals) = packs.entry(reference.object).or_default();
            *references = references.saturating_add(1);
            insert_interval(
                intervals,
                reference.offset,
                reference.offset.saturating_add(u64::from(reference.length)),
            );
        }
        packs
            .into_iter()
            .map(|(object, (references, intervals))| {
                let intervals = intervals.into_iter().collect::<Vec<_>>();
                let covered_bytes = intervals
                    .iter()
                    .map(|(start, end)| end - start)
                    .fold(0_u64, u64::saturating_add);
                PhysicalPackUsage {
                    object,
                    references,
                    intervals,
                    covered_bytes,
                }
            })
            .collect()
    }

    fn add_reference(&mut self, kind: PhysicalReferenceKind, reference: &PackedRef) -> Result<()> {
        self.references.insert(PhysicalReference {
            kind,
            object: reference.object,
            offset: reference.offset,
            length: reference.length,
            digest: reference.digest,
        });
        Ok(())
    }

    fn add_descriptor(&mut self, descriptor: ContentDescriptor) {
        self.content_descriptors.insert(descriptor);
    }

    pub(crate) fn issue(&mut self, error: &Error, subject: impl Into<String>) {
        let issue = PhysicalIssue {
            kind: error.kind(),
            subject: subject.into(),
            detail: error.to_string(),
        };
        if self.issues.contains(&issue) {
            return;
        }
        if self.issues.len() < MAX_ISSUES {
            self.issues.push(issue);
        } else {
            self.issues_omitted = self.issues_omitted.saturating_add(1);
        }
    }
}

/// Inspect references from the supplied latest snapshot and the authority's
/// retained revision set. Authorities with opaque metadata override this hook
/// when their physical representation needs additional roots.
pub(crate) async fn inspect_authority(
    authority: &dyn Authority,
    snapshot: &Snapshot,
) -> PhysicalInspection {
    let mut inspection = inspect_snapshot(snapshot).await;
    match collect_retained_revision_count(authority, snapshot.revision()).await {
        Ok(count) => inspection.retained_revisions = Some(count),
        Err(error) => inspection.issue(&error, "retained-revisions"),
    }
    inspection
}

async fn collect_retained_revision_count(
    authority: &dyn Authority,
    latest: Revision,
) -> Result<u64> {
    let mut after = None;
    let mut revisions = BTreeSet::new();
    loop {
        let page = authority
            .retained_revisions_page(after, RETAINED_REVISIONS_PAGE_MAX)
            .await?;
        if page.revisions.is_empty() {
            if page.next.is_some() {
                return Err(Error::new(
                    ErrorKind::Corrupt,
                    "list retained revisions",
                    "authority returned an empty non-terminal page",
                ));
            }
            break;
        }
        for revision in page.revisions {
            if after.is_some_and(|cursor| revision <= cursor) || !revisions.insert(revision) {
                return Err(Error::new(
                    ErrorKind::Corrupt,
                    "list retained revisions",
                    "authority page is not strictly ordered",
                ));
            }
        }
        let last = revisions.iter().next_back().copied();
        match page.next {
            Some(next) if after.is_none_or(|cursor| next > cursor) && last == Some(next) => {
                after = Some(next)
            }
            Some(_) => {
                return Err(Error::new(
                    ErrorKind::Corrupt,
                    "list retained revisions",
                    "authority continuation does not advance",
                ));
            }
            None => break,
        }
    }
    if !revisions.contains(&latest) {
        return Err(Error::corrupt(
            "list retained revisions",
            "latest observation is absent from the retained revision index",
        ));
    }
    revisions.len().try_into().map_err(|_| {
        Error::new(
            ErrorKind::Invalid,
            "list retained revisions",
            "count overflow",
        )
    })
}

async fn inspect_snapshot(snapshot: &Snapshot) -> PhysicalInspection {
    let mut inspection = PhysicalInspection::default();
    let mut descriptors = HashSet::new();
    inspect_namespace(snapshot, &mut inspection, &mut descriptors).await;
    inspect_changes(snapshot, &mut inspection, &mut descriptors).await;
    inspect_content_descriptors(snapshot.data(), descriptors, &mut inspection).await;
    inspection
}

async fn inspect_namespace(
    snapshot: &Snapshot,
    inspection: &mut PhysicalInspection,
    descriptors: &mut HashSet<ContentDescriptor>,
) {
    let mut pending = vec![snapshot.root()];
    let mut visited = BTreeSet::new();
    let mut visited_nodes = 0_u64;
    while let Some(directory) = pending.pop() {
        if !visited.insert(directory) {
            inspection.issue(
                &Error::corrupt("inspect physical roots", "duplicate directory identity"),
                format!("namespace-directory:{directory:?}"),
            );
            continue;
        }
        visited_nodes = visited_nodes.saturating_add(1);
        if visited_nodes > MAX_WALK_NODES {
            inspection.issue(
                &Error::unsupported(
                    "inspect physical roots",
                    "namespace traversal exceeds one million directories",
                ),
                "namespace",
            );
            break;
        }
        let mut token = None;
        loop {
            let page = match snapshot.scan(directory, token.as_ref(), SCAN_PAGE).await {
                Ok(page) => page,
                Err(error) => {
                    inspection.issue(&error, format!("namespace-directory:{directory:?}"));
                    break;
                }
            };
            for entry in page.entries {
                match snapshot.node(entry.node_id).await {
                    Ok(Some(Node {
                        kind: NodeKind::Directory { .. },
                        ..
                    })) => pending.push(entry.node_id),
                    Ok(Some(Node {
                        kind: NodeKind::File(content),
                        ..
                    })) => {
                        descriptors.insert(content);
                    }
                    Ok(None) => inspection.issue(
                        &Error::corrupt(
                            "inspect physical roots",
                            "directory entry references a missing node",
                        ),
                        format!("namespace-entry:{:?}", entry.node_id),
                    ),
                    Err(error) => {
                        inspection.issue(&error, format!("namespace-entry:{:?}", entry.node_id))
                    }
                }
            }
            token = page.next;
            if token.is_none() {
                break;
            }
        }
    }
}

async fn inspect_changes(
    snapshot: &Snapshot,
    inspection: &mut PhysicalInspection,
    descriptors: &mut HashSet<ContentDescriptor>,
) {
    let mut after = None;
    loop {
        let changes = match snapshot.changes(after, SCAN_PAGE).await {
            Ok(changes) => changes,
            Err(error) => {
                inspection.issue(&error, "retained-change-records");
                break;
            }
        };
        if changes.is_empty() {
            break;
        }
        let next = changes.last().map(|change| change.receipt.cursor);
        for change in &changes {
            for node in change
                .changes
                .iter()
                .flat_map(|item| item.before.iter().chain(item.after.iter()))
            {
                if let NodeKind::File(content) = node.kind() {
                    descriptors.insert(content.clone());
                }
            }
        }
        if changes.len() < SCAN_PAGE || next.is_none() || next == after {
            break;
        }
        after = next;
    }
}

async fn inspect_content_descriptors(
    store: &DataStore,
    descriptors: HashSet<ContentDescriptor>,
    inspection: &mut PhysicalInspection,
) {
    let mut visited = HashSet::new();
    for descriptor in descriptors {
        let subject = format!(
            "content:{}:{}",
            descriptor.content_id().length(),
            hex(descriptor.content_id().digest())
        );
        inspection.add_descriptor(descriptor.clone());
        let result = store
            .inspect_content_references(&descriptor, &mut visited, &mut |kind, reference| {
                let kind = match kind {
                    ContentReferenceKind::Index => PhysicalReferenceKind::ContentIndex,
                    ContentReferenceKind::Payload => PhysicalReferenceKind::ContentExtent,
                };
                inspection.add_reference(kind, reference)
            })
            .await;
        if let Err(error) = result {
            inspection.issue(&error, subject);
        }
    }
}

pub(crate) async fn inspect_object_state(
    snapshot: Revision,
    state: ObjectState,
) -> PhysicalInspection {
    let mut inspection = PhysicalInspection::default();
    if state.revision() != snapshot {
        inspection.issue(
            &Error::new(
                ErrorKind::Invalid,
                "inspect physical roots",
                "authority head changed after the source snapshot",
            ),
            "object-history",
        );
        return inspection;
    }
    let mut descriptors = HashSet::new();
    let mut pending = VecDeque::new();
    let mut seen_descriptors = BTreeSet::new();
    let mut seen_roots = BTreeSet::new();
    let mut seen_pages = HashSet::new();
    let mut cache = IndexCache::default();
    let mut observed_snapshots = 0_u64;
    seen_descriptors.insert(state.reference().clone());
    inspection
        .add_reference(PhysicalReferenceKind::SnapshotDescriptor, state.reference())
        .expect("record a valid snapshot reference");
    observed_snapshots = observed_snapshots.saturating_add(1);
    inspect_object_roots(
        &state,
        [
            (ObjectTable::Nodes, state.nodes()),
            (ObjectTable::Entries, state.entries()),
            (ObjectTable::Receipts, state.receipts()),
            (ObjectTable::Changes, state.changes()),
            (ObjectTable::History, state.history()),
        ],
        &mut inspection,
        &mut descriptors,
        &mut pending,
        &mut seen_roots,
        &mut seen_pages,
        &cache,
    )
    .await;

    while let Some((revision, reference)) = pending.pop_front() {
        if !seen_descriptors.insert(reference.clone()) {
            continue;
        }
        match state.load_retained(revision, &reference).await {
            Ok(retained) => {
                inspection
                    .add_reference(PhysicalReferenceKind::SnapshotDescriptor, &reference)
                    .expect("record a valid snapshot reference");
                observed_snapshots = observed_snapshots.saturating_add(1);
                inspect_object_roots(
                    &retained,
                    [
                        (ObjectTable::Nodes, retained.nodes()),
                        (ObjectTable::Entries, retained.entries()),
                        (ObjectTable::Receipts, retained.receipts()),
                        (ObjectTable::Changes, retained.changes()),
                        (ObjectTable::History, retained.history()),
                    ],
                    &mut inspection,
                    &mut descriptors,
                    &mut pending,
                    &mut seen_roots,
                    &mut seen_pages,
                    &cache,
                )
                .await;
            }
            Err(error) => inspection.issue(
                &error,
                format!("snapshot-descriptor:{}", hex(&revision.to_bytes())),
            ),
        }
    }
    inspection.retained_revisions = Some(observed_snapshots);
    inspect_content_descriptors(state.data(), descriptors, &mut inspection).await;
    inspection
}

async fn inspect_object_roots(
    state: &ObjectState,
    roots: [(ObjectTable, &Index); 5],
    inspection: &mut PhysicalInspection,
    descriptors: &mut HashSet<ContentDescriptor>,
    pending: &mut VecDeque<(Revision, PackedRef)>,
    seen_roots: &mut BTreeSet<(ObjectTable, PackedRef)>,
    seen_pages: &mut HashSet<PackedRef>,
    cache: &IndexCache,
) {
    for (table, index) in roots {
        let Some(root) = &index.0 else {
            continue;
        };
        if !seen_roots.insert((table, root.clone())) {
            continue;
        }
        let mut visitor = ObjectMetadataVisitor {
            table,
            revision: state.revision(),
            filesystem: state.filesystem(),
            inspection,
            descriptors,
            descriptors_pending: pending,
        };
        if let Err(error) = index
            .inspect(state.data(), Some(cache), seen_pages, &mut visitor)
            .await
        {
            visitor.inspection.issue(
                &error,
                format!("metadata-root:{}:{:?}", table.tag(), root.object),
            );
        }
    }
}

struct ObjectMetadataVisitor<'a> {
    table: ObjectTable,
    revision: Revision,
    filesystem: NodeId,
    inspection: &'a mut PhysicalInspection,
    descriptors: &'a mut HashSet<ContentDescriptor>,
    descriptors_pending: &'a mut VecDeque<(Revision, PackedRef)>,
}

impl MetadataVisitor for ObjectMetadataVisitor<'_> {
    fn page(&mut self, reference: &PackedRef) {
        self.inspection
            .add_reference(PhysicalReferenceKind::MetadataIndex, reference)
            .expect("record a valid metadata page reference");
    }

    fn value(&mut self, reference: Option<&PackedRef>, key: &[u8], bytes: &[u8]) {
        if let Some(reference) = reference {
            self.inspection
                .add_reference(PhysicalReferenceKind::MetadataValue, reference)
                .expect("record a valid metadata value reference");
        }
        let result = match self.table {
            ObjectTable::Nodes => decode::<Node>(bytes, self.filesystem)
                .map(|node| {
                    if node.id().as_bytes().as_slice() != key {
                        return Err(Error::corrupt(
                            "inspect physical roots",
                            "node key disagrees with its encoded identity",
                        ));
                    }
                    if let NodeKind::File(content) = node.kind() {
                        self.descriptors.insert(content.clone());
                    }
                    Ok(())
                })
                .and_then(|result| result),
            ObjectTable::Entries => DirectoryEntry::decode(bytes).map(|_| ()),
            ObjectTable::Receipts => Receipt::decode(bytes).and_then(|receipt| {
                if receipt.commit_id.as_bytes().as_slice() != key
                    || receipt.cursor.revision > self.revision
                {
                    return Err(Error::corrupt(
                        "inspect physical roots",
                        "receipt key or revision disagrees with the retained root",
                    ));
                }
                Ok(())
            }),
            ObjectTable::Changes => {
                ChangeRecord::decode(bytes, self.filesystem).and_then(|record| {
                    if record.receipt.cursor.key() != key
                        || record.receipt.cursor.revision > self.revision
                    {
                        return Err(Error::corrupt(
                            "inspect physical roots",
                            "change key or revision disagrees with the retained root",
                        ));
                    }
                    for node in record
                        .changes
                        .iter()
                        .flat_map(|change| change.before.iter().chain(change.after.iter()))
                    {
                        if let NodeKind::File(content) = node.kind() {
                            self.descriptors.insert(content.clone());
                        }
                    }
                    Ok(())
                })
            }
            ObjectTable::History => {
                let revision: [u8; 24] = key.try_into().map_err(|_| {
                    Error::corrupt("inspect physical roots", "invalid retained revision key")
                })?;
                decode::<RefWire>(bytes)
                    .and_then(PackedRef::from_wire)
                    .map(|reference| {
                        self.descriptors_pending
                            .push_back((Revision::from_bytes(revision), reference));
                    })
            }
        };
        if let Err(error) = result {
            self.inspection
                .issue(&error, metadata_subject(self.table, key));
        }
    }
}

fn metadata_subject(table: Table, key: &[u8]) -> String {
    format!("metadata:{}:{}", table as u8, hex(key))
}

fn insert_interval(intervals: &mut BTreeMap<u64, u64>, start: u64, end: u64) {
    if start >= end {
        return;
    }
    let mut start = start;
    let mut end = end;
    if let Some((&previous_start, &previous_end)) = intervals.range(..=start).next_back()
        && previous_end >= start
    {
        intervals.remove(&previous_start);
        start = previous_start;
        end = end.max(previous_end);
    }
    while let Some((&next_start, &next_end)) = intervals.range(start..).next() {
        if next_start > end {
            break;
        }
        intervals.remove(&next_start);
        end = end.max(next_end);
    }
    intervals.insert(start, end);
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn overlapping_coverage_is_counted_once_per_pack() {
        let mut inspection = PhysicalInspection::default();
        for (kind, offset, length) in [
            (PhysicalReferenceKind::ContentExtent, 0, 64),
            (PhysicalReferenceKind::ContentExtent, 32, 64),
            (PhysicalReferenceKind::MetadataIndex, 128, 32),
        ] {
            inspection
                .add_reference(
                    kind,
                    &PackedRef {
                        object: [1; 16],
                        offset,
                        length,
                        digest: [2; 32],
                    },
                )
                .unwrap();
        }
        let packs = inspection.packs();
        assert_eq!(packs.len(), 1);
        assert_eq!(packs[0].references, 3);
        assert_eq!(packs[0].intervals, vec![(0, 96), (128, 160)]);
        assert_eq!(packs[0].covered_bytes, 128);
    }
}
