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

use crate::data::{DataStore, PackedRef, RefWire};
use crate::{Error, Result};

const MAX: usize = 32;
const MIN: usize = MAX / 2;
const PAGE_BYTES: u32 = 64 * 1024;
const VALUE_BYTES: u32 = 16 * 1024 * 1024;
const INLINE: usize = 512;
const KEY_BYTES: usize = 1024;
type Wire = ([u8; 8], u8, Vec<(Vec<u8>, Vec<u8>, Option<RefWire>)>);

/// An immutable, authenticated B+ tree. Branch separators are inclusive maxima.
#[derive(Clone, Debug, Default)]
pub(crate) struct Index(pub(crate) Option<PackedRef>);

#[derive(Clone)]
enum Value {
    Inline(Vec<u8>),
    External(PackedRef),
}
#[derive(Clone)]
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
        let bytes = borsh::to_vec(&(*b"YYINDEX2", self.level(), records))
            .map_err(|e| corrupt(e.to_string()))?;
        if bytes.len() > PAGE_BYTES as usize {
            return Err(corrupt("index page exceeds its bound"));
        }
        store.put_metadata(&bytes).await
    }
}
impl Index {
    async fn page(store: &DataStore, reference: &PackedRef, root: bool) -> Result<Page> {
        let bytes = store.read_extent(reference, PAGE_BYTES).await?;
        let (magic, level, records): Wire =
            borsh::from_slice(&bytes).map_err(|e| corrupt(e.to_string()))?;
        if magic != *b"YYINDEX2"
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
        level: u8,
        key: &[u8],
        reference: &PackedRef,
    ) -> Result<Page> {
        let page = Self::page(store, reference, false).await?;
        if page.level() + 1 != level || page.last() != key {
            return Err(corrupt("child separator or level disagrees"));
        }
        Ok(page)
    }
    async fn value(store: &DataStore, value: &Value) -> Result<Vec<u8>> {
        match value {
            Value::Inline(v) => Ok(v.clone()),
            Value::External(r) => store.read_extent(r, VALUE_BYTES).await,
        }
    }
    pub(crate) async fn get(&self, store: &DataStore, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let Some(root) = &self.0 else {
            return Ok(None);
        };
        let mut page = Self::page(store, root, true).await?;
        loop {
            match page {
                Page::Leaf(entries) => {
                    return match entries.binary_search_by(|(k, _)| k.as_slice().cmp(key)) {
                        Ok(i) => Self::value(store, &entries[i].1).await.map(Some),
                        Err(_) => Ok(None),
                    };
                }
                Page::Branch(level, children) => {
                    let i = children.partition_point(|(k, _)| k.as_slice() < key);
                    if i == children.len() {
                        return Ok(None);
                    }
                    page = Self::child(store, level, &children[i].0, &children[i].1).await?;
                }
            }
        }
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
            Some(r) => Self::page(store, r, true).await?,
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
                let child = Self::child(store, *level, &children[i].0, &children[i].1).await?;
                let mut child = Box::pin(Self::edit(store, child, key, value)).await?;
                if child.len() < MIN && children.len() > 1 {
                    let sibling_index = if i > 0 { i - 1 } else { i + 1 };
                    let sibling = Self::child(
                        store,
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
            let page = Self::page(store, root, true).await?;
            Self::scan_page(store, page, lower, upper, after, limit, &mut output).await?;
        }
        Ok(output)
    }
    async fn scan_page(
        store: &DataStore,
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
                    output.push((key, Self::value(store, &value).await?));
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
                        let child = Self::child(store, level, &key, &reference).await?;
                        Box::pin(Self::scan_page(
                            store, child, lower, upper, after, limit, output,
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
    use crate::{NodeId, support::TestBackend};

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
            assert_eq!(index.get(&store, key).await.unwrap().as_ref(), Some(value));
        }
        let rows = index.scan(&store, &[], None, None, 2000).await.unwrap();
        assert_eq!(rows, expected.clone().into_iter().collect::<Vec<_>>());
        backend.state.lock().unwrap().data_reads = 0;
        index.get(&store, &500_u64.to_be_bytes()).await.unwrap();
        assert!(backend.state.lock().unwrap().data_reads <= 4);
        let root = Index::page(&store, index.0.as_ref().unwrap(), true)
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
                    index.scan(&store, &[], None, None, 2000).await.unwrap(),
                    expected.clone().into_iter().collect::<Vec<_>>()
                );
            }
        }
        assert!(index.0.is_none());
        assert_eq!(
            pinned
                .scan(&store, &[], None, None, 2000)
                .await
                .unwrap()
                .len(),
            1100
        );
    }
    #[tokio::test]
    async fn malformed_index_pages_are_rejected() {
        let backend = TestBackend::default();
        let store = DataStore::new(backend.operator(), NodeId::generate()).unwrap();
        let invalid: Wire = (
            *b"YYINDEX2",
            0,
            vec![(b"b".to_vec(), vec![], None), (b"a".to_vec(), vec![], None)],
        );
        let root = store
            .put_metadata(&borsh::to_vec(&invalid).unwrap())
            .await
            .unwrap();
        assert_eq!(
            Index(Some(root))
                .get(&store, b"a")
                .await
                .unwrap_err()
                .kind(),
            crate::ErrorKind::Corrupt
        );
    }
}
