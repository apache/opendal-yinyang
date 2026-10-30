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

use crate::compatibility::OBJECT_INDEX_MAGIC;
use crate::data::{DataStore, PackedRef, RefWire};
use crate::{Error, Result};
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

const MAX: usize = 32;
const MIN: usize = MAX / 2;
const PAGE_BYTES: u32 = 64 * 1024;
const VALUE_BYTES: u32 = 16 * 1024 * 1024;
const INLINE: usize = 512;
const KEY_BYTES: usize = 1024;
const CACHED_PAGES: usize = 256;
const CACHED_VALUES: usize = 4096;
const CACHED_VALUE_BYTES: usize = 32 * 1024 * 1024;
type Wire = ([u8; 8], u8, Vec<(Vec<u8>, Vec<u8>, Option<RefWire>)>);

/// An immutable, authenticated B+ tree. Branch separators are inclusive maxima.
#[derive(Clone, Debug, Default)]
pub(crate) struct Index(pub(crate) Option<PackedRef>);

pub(crate) trait MetadataVisitor: Send {
    fn page(&mut self, reference: &PackedRef);
    fn value(&mut self, reference: Option<&PackedRef>, key: &[u8], bytes: &[u8]);
}

/// Operation-local memoization for one immutable snapshot view.
///
/// Index pages and external values are content-addressed, so reuse cannot
/// change what the pinned snapshot observes. Writers never use this cache.
#[derive(Clone, Debug, Default)]
pub(crate) struct IndexCache {
    state: Arc<Mutex<IndexCacheState>>,
}

#[derive(Debug, Default)]
struct IndexCacheState {
    pages: HashMap<PackedRef, Vec<u8>>,
    values: HashMap<PackedRef, Vec<u8>>,
    value_bytes: usize,
}

impl IndexCache {
    fn page(&self, reference: &PackedRef) -> Option<Vec<u8>> {
        let state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.pages.get(reference).cloned()
    }

    fn insert_page(&self, reference: PackedRef, bytes: Vec<u8>) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if state.pages.len() < CACHED_PAGES {
            state.pages.entry(reference).or_insert(bytes);
        }
    }

    fn value(&self, reference: &PackedRef) -> Option<Vec<u8>> {
        let state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.values.get(reference).cloned()
    }

    fn insert_value(&self, reference: PackedRef, bytes: Vec<u8>) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if state.values.len() >= CACHED_VALUES {
            return;
        }
        let state = &mut *state;
        if let Entry::Vacant(entry) = state.values.entry(reference) {
            let next = state.value_bytes.saturating_add(bytes.len());
            if next <= CACHED_VALUE_BYTES {
                state.value_bytes = next;
                entry.insert(bytes);
            }
        }
    }
}

#[derive(Clone, Debug)]
enum Value {
    Inline(Vec<u8>),
    External(PackedRef),
}
#[derive(Clone, Debug)]
enum Page {
    Leaf(Vec<(Vec<u8>, Value)>),
    Branch(u8, Vec<(Vec<u8>, PackedRef)>),
}
impl Page {
    fn level(&self) -> u8 {
        match self {
            Self::Leaf(_) => 0,
            Self::Branch(level, _) => *level,
        }
    }
    fn len(&self) -> usize {
        match self {
            Self::Leaf(v) => v.len(),
            Self::Branch(_, v) => v.len(),
        }
    }
    fn last(&self) -> Vec<u8> {
        match self {
            Self::Leaf(v) => v.last().unwrap().0.clone(),
            Self::Branch(_, v) => v.last().unwrap().0.clone(),
        }
    }
    fn split(&mut self) -> Self {
        let mid = self.len() / 2;
        match self {
            Self::Leaf(v) => Self::Leaf(v.split_off(mid)),
            Self::Branch(level, v) => Self::Branch(*level, v.split_off(mid)),
        }
    }
    fn join(&mut self, other: Self) -> Result<()> {
        match (self, other) {
            (Self::Leaf(left), Self::Leaf(right)) => left.extend(right),
            (Self::Branch(l, left), Self::Branch(r, right)) if *l == r => left.extend(right),
            _ => return Err(corrupt("index levels disagree")),
        }
        Ok(())
    }
    async fn write(&self, store: &DataStore) -> Result<PackedRef> {
        let records: Vec<_> = match self {
            Self::Leaf(v) => v
                .iter()
                .map(|(k, v)| match v {
                    Value::Inline(bytes) => (k.clone(), bytes.clone(), None),
                    Value::External(r) => (k.clone(), Vec::new(), Some(r.wire())),
                })
                .collect(),
            Self::Branch(_, v) => v
                .iter()
                .map(|(k, r)| (k.clone(), Vec::new(), Some(r.wire())))
                .collect(),
        };
        let bytes = borsh::to_vec(&(OBJECT_INDEX_MAGIC, self.level(), records))
            .map_err(|e| corrupt(e.to_string()))?;
        if bytes.len() > PAGE_BYTES as usize {
            return Err(corrupt("index page exceeds its bound"));
        }
        store.put_metadata(&bytes).await
    }
}
impl Index {
    async fn page(
        store: &DataStore,
        cache: Option<&IndexCache>,
        reference: &PackedRef,
        root: bool,
    ) -> Result<Page> {
        let bytes = match cache.and_then(|cache| cache.page(reference)) {
            Some(bytes) => bytes,
            None => {
                let bytes = store.read_extent(reference, PAGE_BYTES).await?;
                if let Some(cache) = cache {
                    cache.insert_page(reference.clone(), bytes.clone());
                }
                bytes
            }
        };
        Self::decode_page(&bytes, root)
    }
    fn decode_page(bytes: &[u8], root: bool) -> Result<Page> {
        if bytes.len() < OBJECT_INDEX_MAGIC.len() {
            return Err(corrupt("truncated index page"));
        }
        if bytes[..OBJECT_INDEX_MAGIC.len()] != OBJECT_INDEX_MAGIC {
            return Err(Error::unsupported(
                "read metadata index",
                "unknown index profile",
            ));
        }
        let (magic, level, records): Wire =
            borsh::from_slice(bytes).map_err(|e| corrupt(e.to_string()))?;
        if magic != OBJECT_INDEX_MAGIC
            || level > 32
            || records.is_empty()
            || records.len() > MAX
            || (!root && records.len() < MIN)
            || (level > 0 && root && records.len() < 2)
            || records
                .iter()
                .any(|(k, _, _)| k.is_empty() || k.len() > KEY_BYTES)
            || records.windows(2).any(|p| p[0].0 >= p[1].0)
        {
            return Err(corrupt("invalid index shape, occupancy, or ordering"));
        }
        if level == 0 {
            let mut entries = Vec::new();
            for (key, inline, external) in records {
                let value = match external {
                    Some(r) if inline.is_empty() => Value::External(PackedRef::from_wire(r)?),
                    None if inline.len() <= INLINE => Value::Inline(inline),
                    _ => return Err(corrupt("invalid leaf value")),
                };
                entries.push((key, value));
            }
            Ok(Page::Leaf(entries))
        } else {
            let mut children = Vec::new();
            for (key, inline, external) in records {
                if !inline.is_empty() {
                    return Err(corrupt("inline branch value"));
                }
                children.push((
                    key,
                    PackedRef::from_wire(external.ok_or_else(|| corrupt("missing child"))?)?,
                ));
            }
            Ok(Page::Branch(level, children))
        }
    }
    async fn child(
        store: &DataStore,
        cache: Option<&IndexCache>,
        level: u8,
        key: &[u8],
        reference: &PackedRef,
    ) -> Result<Page> {
        let page = Self::page(store, cache, reference, false).await?;
        if page.level() + 1 != level || page.last() != key {
            return Err(corrupt("child separator or level disagrees"));
        }
        Ok(page)
    }
    async fn value(
        store: &DataStore,
        cache: Option<&IndexCache>,
        value: &Value,
    ) -> Result<Vec<u8>> {
        match value {
            Value::Inline(v) => Ok(v.clone()),
            Value::External(reference) => {
                if let Some(bytes) = cache.and_then(|cache| cache.value(reference)) {
                    return Ok(bytes);
                }
                let bytes = store.read_extent(reference, VALUE_BYTES).await?;
                if let Some(cache) = cache {
                    cache.insert_value(reference.clone(), bytes.clone());
                }
                Ok(bytes)
            }
        }
    }
    pub(crate) async fn get(
        &self,
        store: &DataStore,
        cache: Option<&IndexCache>,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>> {
        let Some(root) = &self.0 else {
            return Ok(None);
        };
        let mut page = Self::page(store, cache, root, true).await?;
        loop {
            match page {
                Page::Leaf(entries) => {
                    return match entries.binary_search_by(|(k, _)| k.as_slice().cmp(key)) {
                        Ok(i) => Self::value(store, cache, &entries[i].1).await.map(Some),
                        Err(_) => Ok(None),
                    };
                }
                Page::Branch(level, children) => {
                    let i = children.partition_point(|(k, _)| k.as_slice() < key);
                    if i == children.len() {
                        return Ok(None);
                    }
                    page = Self::child(store, cache, level, &children[i].0, &children[i].1).await?;
                }
            }
        }
    }

    /// Walk every authenticated page and external value in this immutable tree.
    pub(crate) async fn inspect(
        &self,
        store: &DataStore,
        cache: Option<&IndexCache>,
        visited: &mut HashSet<PackedRef>,
        visitor: &mut dyn MetadataVisitor,
    ) -> Result<()> {
        let Some(root) = &self.0 else {
            return Ok(());
        };
        let page = Self::page(store, cache, root, true).await?;
        if !visited.insert(root.clone()) {
            return Ok(());
        }
        Self::inspect_page(store, cache, visited, page, root, visitor).await
    }

    fn inspect_page<'a>(
        store: &'a DataStore,
        cache: Option<&'a IndexCache>,
        visited: &'a mut HashSet<PackedRef>,
        page: Page,
        reference: &'a PackedRef,
        visitor: &'a mut dyn MetadataVisitor,
    ) -> futures_util::future::BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            visitor.page(reference);
            match page {
                Page::Leaf(entries) => {
                    for (key, value) in entries {
                        match value {
                            Value::Inline(bytes) => visitor.value(None, &key, &bytes),
                            Value::External(reference) => {
                                let bytes =
                                    Self::value(store, cache, &Value::External(reference.clone()))
                                        .await?;
                                visitor.value(Some(&reference), &key, &bytes);
                            }
                        }
                    }
                    Ok(())
                }
                Page::Branch(level, children) => {
                    for (key, child) in children {
                        let page = Self::child(store, cache, level, &key, &child).await?;
                        if !visited.insert(child.clone()) {
                            continue;
                        }
                        Self::inspect_page(store, cache, visited, page, &child, visitor).await?;
                    }
                    Ok(())
                }
            }
        })
    }
    pub(crate) async fn set(
        &mut self,
        store: &DataStore,
        key: Vec<u8>,
        bytes: Option<Vec<u8>>,
    ) -> Result<()> {
        if key.is_empty() || key.len() > KEY_BYTES {
            return Err(Error::invalid("edit index", "invalid key length"));
        }
        let value = match bytes {
            Some(v) if v.len() > VALUE_BYTES as usize => {
                return Err(Error::unsupported("edit index", "value exceeds 16 MiB"));
            }
            Some(v) if v.len() > INLINE => Some(Value::External(store.put_metadata(&v).await?)),
            Some(v) => Some(Value::Inline(v)),
            None => None,
        };
        let page = match &self.0 {
            Some(r) => Self::page(store, None, r, true).await?,
            None => Page::Leaf(Vec::new()),
        };
        let mut page = Self::edit(store, page, &key, value).await?;
        if page.len() == 0 {
            self.0 = None;
            return Ok(());
        }
        if let Page::Branch(_, children) = &page
            && children.len() == 1
        {
            self.0 = Some(children[0].1.clone());
            return Ok(());
        }
        if page.len() > MAX {
            let right = page.split();
            let level = page
                .level()
                .checked_add(1)
                .filter(|v| *v <= 32)
                .ok_or_else(|| Error::unsupported("edit index", "maximum tree depth"))?;
            page = Page::Branch(
                level,
                vec![
                    (page.last(), page.write(store).await?),
                    (right.last(), right.write(store).await?),
                ],
            );
        }
        self.0 = Some(page.write(store).await?);
        Ok(())
    }
    async fn edit(
        store: &DataStore,
        mut page: Page,
        key: &[u8],
        value: Option<Value>,
    ) -> Result<Page> {
        match &mut page {
            Page::Leaf(entries) => match (
                entries.binary_search_by(|(k, _)| k.as_slice().cmp(key)),
                value,
            ) {
                (Ok(i), Some(v)) => entries[i].1 = v,
                (Ok(i), None) => {
                    entries.remove(i);
                }
                (Err(i), Some(v)) => entries.insert(i, (key.to_vec(), v)),
                (Err(_), None) => {}
            },
            Page::Branch(level, children) => {
                let i = children
                    .partition_point(|(k, _)| k.as_slice() < key)
                    .min(children.len() - 1);
                let child =
                    Self::child(store, None, *level, &children[i].0, &children[i].1).await?;
                let mut child = Box::pin(Self::edit(store, child, key, value)).await?;
                if child.len() < MIN && children.len() > 1 {
                    let sibling_index = if i > 0 { i - 1 } else { i + 1 };
                    let sibling = Self::child(
                        store,
                        None,
                        *level,
                        &children[sibling_index].0,
                        &children[sibling_index].1,
                    )
                    .await?;
                    let start = i.min(sibling_index);
                    if sibling_index < i {
                        let right = child;
                        child = sibling;
                        child.join(right)?;
                    } else {
                        child.join(sibling)?;
                    }
                    children.drain(start..start + 2);
                    if child.len() > MAX {
                        let right = child.split();
                        children.insert(start, (right.last(), right.write(store).await?));
                    }
                    children.insert(start, (child.last(), child.write(store).await?));
                } else {
                    children.remove(i);
                    if child.len() > MAX {
                        let right = child.split();
                        children.insert(i, (right.last(), right.write(store).await?));
                    }
                    if child.len() > 0 {
                        children.insert(i, (child.last(), child.write(store).await?));
                    }
                }
            }
        }
        Ok(page)
    }
    /// Inclusive lower / exclusive upper bound, with an exclusive continuation key.
    pub(crate) async fn scan(
        &self,
        store: &DataStore,
        cache: Option<&IndexCache>,
        lower: &[u8],
        upper: Option<&[u8]>,
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut output = Vec::new();
        if limit == 0 {
            return Ok(output);
        }
        if let Some(root) = &self.0 {
            let page = Self::page(store, cache, root, true).await?;
            Self::scan_page(store, cache, page, lower, upper, after, limit, &mut output).await?;
        }
        Ok(output)
    }
    async fn scan_page(
        store: &DataStore,
        cache: Option<&IndexCache>,
        page: Page,
        lower: &[u8],
        upper: Option<&[u8]>,
        after: Option<&[u8]>,
        limit: usize,
        output: &mut Vec<(Vec<u8>, Vec<u8>)>,
    ) -> Result<()> {
        match page {
            Page::Leaf(entries) => {
                for (key, value) in entries {
                    if key.as_slice() < lower || after.is_some_and(|a| key.as_slice() <= a) {
                        continue;
                    }
                    if upper.is_some_and(|u| key.as_slice() >= u) || output.len() == limit {
                        break;
                    }
                    output.push((key, Self::value(store, cache, &value).await?));
                }
            }
            Page::Branch(level, children) => {
                let mut previous: Option<Vec<u8>> = None;
                for (key, reference) in children {
                    if output.len() == limit
                        || previous
                            .as_ref()
                            .is_some_and(|p| upper.is_some_and(|u| p.as_slice() >= u))
                    {
                        break;
                    }
                    if key.as_slice() >= lower && !after.is_some_and(|a| key.as_slice() <= a) {
                        let child = Self::child(store, cache, level, &key, &reference).await?;
                        Box::pin(Self::scan_page(
                            store, cache, child, lower, upper, after, limit, output,
                        ))
                        .await?;
                    }
                    previous = Some(key);
                }
            }
        }
        Ok(())
    }
}
fn corrupt(message: impl Into<String>) -> Error {
    Error::corrupt("read metadata index", message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ErrorKind, NodeId, support::TestBackend};

    #[test]
    fn cache_charges_duplicate_values_once_and_respects_its_bounds() {
        let cache = IndexCache::default();
        let reference = PackedRef {
            object: [1; 16],
            offset: 0,
            length: 4,
            digest: [2; 32],
        };
        cache.insert_value(reference.clone(), vec![1, 2, 3, 4]);
        cache.insert_value(reference, vec![1, 2, 3, 4]);
        {
            let state = cache.state.lock().unwrap();
            assert_eq!(state.values.len(), 1);
            assert_eq!(state.value_bytes, 4);
        }

        for i in 0..CACHED_VALUES + 1 {
            let reference = PackedRef {
                object: (i as u128).to_be_bytes(),
                offset: i as u64,
                length: 1,
                digest: [3; 32],
            };
            cache.insert_value(reference, vec![0]);
        }
        assert_eq!(cache.state.lock().unwrap().values.len(), CACHED_VALUES);
    }

    #[tokio::test]
    async fn splits_merges_external_values_and_bounded_lookup() {
        let backend = TestBackend::default();
        let store = DataStore::new(backend.operator(), NodeId::generate()).unwrap();
        let mut index = Index::default();
        let mut expected = std::collections::BTreeMap::new();
        for i in 0_u64..1100 {
            let n = (i * 607) % 1100;
            let key = n.to_be_bytes().to_vec();
            let value = vec![(n % 251) as u8; if n % 17 == 0 { 1000 } else { 8 }];
            index
                .set(&store, key.clone(), Some(value.clone()))
                .await
                .unwrap();
            expected.insert(key, value);
        }
        let pinned = index.clone();
        for (key, value) in &expected {
            assert_eq!(
                index.get(&store, None, key).await.unwrap().as_ref(),
                Some(value)
            );
        }
        let rows = index
            .scan(&store, None, &[], None, None, 2000)
            .await
            .unwrap();
        assert_eq!(rows, expected.clone().into_iter().collect::<Vec<_>>());
        backend.state.lock().unwrap().data_reads = 0;
        index
            .get(&store, None, &500_u64.to_be_bytes())
            .await
            .unwrap();
        assert!(backend.state.lock().unwrap().data_reads <= 4);
        let root = Index::page(&store, None, index.0.as_ref().unwrap(), true)
            .await
            .unwrap();
        assert_eq!(root.level(), 2);
        for i in 0_u64..1100 {
            let n = (i * 607) % 1100;
            let key = n.to_be_bytes().to_vec();
            index.set(&store, key.clone(), None).await.unwrap();
            expected.remove(&key);
            if i % 79 == 0 {
                assert_eq!(
                    index
                        .scan(&store, None, &[], None, None, 2000)
                        .await
                        .unwrap(),
                    expected.clone().into_iter().collect::<Vec<_>>()
                );
            }
        }
        assert!(index.0.is_none());
        assert_eq!(
            pinned
                .scan(&store, None, &[], None, None, 2000)
                .await
                .unwrap()
                .len(),
            1100
        );
    }

    #[tokio::test]
    async fn cached_page_bytes_are_revalidated_for_root_and_child_contexts() {
        let backend = TestBackend::default();
        let store = DataStore::new(backend.operator(), NodeId::generate()).unwrap();
        let mut index = Index::default();
        for i in 0..MAX + 1 {
            index
                .set(&store, (i as u64).to_be_bytes().to_vec(), Some(vec![1]))
                .await
                .unwrap();
        }
        let cache = IndexCache::default();
        let root = index.0.as_ref().unwrap();
        let page = Index::page(&store, Some(&cache), root, true).await.unwrap();
        assert!(matches!(&page, Page::Branch(1, children) if children.len() == 2));

        backend.state.lock().unwrap().data_reads = 0;
        let error = Index::page(&store, Some(&cache), root, false)
            .await
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Corrupt);
        assert_eq!(backend.state.lock().unwrap().data_reads, 0);
    }

    #[tokio::test]
    async fn duplicate_child_reference_is_revalidated_for_each_separator() {
        let backend = TestBackend::default();
        let store = DataStore::new(backend.operator(), NodeId::generate()).unwrap();
        let child = Page::Leaf(vec![(b"a".to_vec(), Value::Inline(vec![1]))])
            .write(&store)
            .await
            .unwrap();
        let root = Page::Branch(
            1,
            vec![(b"a".to_vec(), child.clone()), (b"b".to_vec(), child)],
        )
        .write(&store)
        .await
        .unwrap();

        struct Noop;
        impl MetadataVisitor for Noop {
            fn page(&mut self, _: &PackedRef) {}
            fn value(&mut self, _: Option<&PackedRef>, _: &[u8], _: &[u8]) {}
        }

        let error = Index(Some(root))
            .inspect(&store, None, &mut HashSet::new(), &mut Noop)
            .await
            .unwrap_err();
        assert_eq!(error.kind(), crate::ErrorKind::Corrupt);
        assert!(error.to_string().contains("separator"));
    }

    #[tokio::test]
    async fn malformed_index_pages_are_rejected() {
        let backend = TestBackend::default();
        let store = DataStore::new(backend.operator(), NodeId::generate()).unwrap();
        let invalid: Wire = (
            OBJECT_INDEX_MAGIC,
            0,
            vec![(b"b".to_vec(), vec![], None), (b"a".to_vec(), vec![], None)],
        );
        let root = store
            .put_metadata(&borsh::to_vec(&invalid).unwrap())
            .await
            .unwrap();
        assert_eq!(
            Index(Some(root))
                .get(&store, None, b"a")
                .await
                .unwrap_err()
                .kind(),
            crate::ErrorKind::Corrupt
        );
    }
}
