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

//! Canonical, range-verifiable content and process-local preparation evidence.

use std::ops::Range;
use std::sync::Arc;

use futures_util::TryStreamExt as _;
use opendal::Operator;
use tokio::io::{AsyncRead, AsyncReadExt as _, AsyncWrite, AsyncWriteExt as _};

use crate::{Error, NodeId, Result};

/// Fixed logical verification unit of the experimental content profile.
pub const BLOCK_BYTES: usize = 64 * 1024;
const FILE_MAGIC: [u8; 8] = *b"YYFILE02";
const NODE_MAGIC: [u8; 8] = *b"YYHASH02";
const MAX_NODE_BYTES: u32 = 512;

pub(crate) type RefWire = ([u8; 16], u64, u32, [u8; 32]);
type ChildWire = ([u8; 32], RefWire);
type FileWire = ([u8; 8], [u8; 16], u64, Option<ChildWire>);

/// Identity of logical bytes in the fixed YYFILE02 content profile.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ContentId {
    length: u64,
    digest: [u8; 32],
}

impl ContentId {
    pub const fn length(self) -> u64 {
        self.length
    }
    pub const fn digest(&self) -> &[u8; 32] {
        &self.digest
    }
}

/// Untrusted serializable reference, not evidence that its bytes are ready.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ContentDescriptor {
    filesystem: NodeId,
    length: u64,
    root: Option<Child>,
}

impl ContentDescriptor {
    pub fn content_id(&self) -> ContentId {
        let mut hash = domain(b"file");
        hash.update(&self.length.to_le_bytes());
        hash.update(&self.root.as_ref().map_or(empty_hash(), |root| root.hash));
        ContentId {
            length: self.length,
            digest: hash.finalize().into(),
        }
    }

    pub const fn filesystem(&self) -> NodeId {
        self.filesystem
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        encode(&(
            FILE_MAGIC,
            *self.filesystem.as_bytes(),
            self.length,
            self.root.as_ref().map(Child::wire),
        ))
        .expect("content descriptor contains only bounded values")
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() > MAX_NODE_BYTES as usize {
            return Err(corrupt("descriptor is too large"));
        }
        let (magic, filesystem, length, root): FileWire = decode(bytes)?;
        if magic != FILE_MAGIC {
            return Err(Error::unsupported(
                "decode content",
                "unknown content profile",
            ));
        }
        if (length == 0) != root.is_none() {
            return Err(corrupt("empty content has an invalid root"));
        }
        let root = root.map(Child::from_wire).transpose()?;
        Ok(Self {
            filesystem: NodeId::from_bytes(filesystem),
            length,
            root,
        })
    }
}

/// Evidence issued by one trusted data binding after immutable preparation.
///
/// The handle has no public constructor or deserializer. Serialization of its
/// descriptor loses the evidence; import must independently verify it again.
#[derive(Clone, Debug)]
pub struct PreparedContent {
    descriptor: ContentDescriptor,
    issuer: Arc<()>,
}

impl PreparedContent {
    pub fn descriptor(&self) -> &ContentDescriptor {
        &self.descriptor
    }
    pub fn content_id(&self) -> ContentId {
        self.descriptor.content_id()
    }
}

/// Immutable data binding for one filesystem and one storage operator.
///
/// Clones share preparation authority. A separately constructed binding cannot
/// fabricate evidence accepted by this instance, even with the same filesystem ID.
#[derive(Clone, Debug)]
pub struct DataStore {
    operator: Operator,
    filesystem: NodeId,
    issuer: Arc<()>,
}

impl DataStore {
    pub fn new(operator: Operator, filesystem: NodeId) -> Result<Self> {
        let capability = operator.info().capability();
        if !capability.read
            || !capability.write
            || !capability.write_can_multi
            || !capability.write_with_if_not_exists
        {
            return Err(Error::unsupported(
                "open content binding",
                "read, streaming immutable writes, and range reads are required",
            ));
        }
        Ok(Self {
            operator,
            filesystem,
            issuer: Arc::new(()),
        })
    }

    /// Validate preparation authority without downloading data.
    pub fn accept(&self, prepared: &PreparedContent) -> Result<ContentDescriptor> {
        self.check(&prepared.descriptor)?;
        if !Arc::ptr_eq(&self.issuer, &prepared.issuer) {
            return Err(Error::invalid(
                "accept prepared content",
                "preparation authority does not match",
            ));
        }
        Ok(prepared.descriptor.clone())
    }

    /// Prepare content with bounded buffers and a logarithmic hash frontier.
    ///
    /// This conservative binding verifies the acknowledged pack once during
    /// preparation: an OpenDAL writer-close acknowledgement alone does not
    /// establish end-to-end integrity. Commit never repeats this read.
    pub async fn prepare(&self, source: &mut (impl AsyncRead + Unpin)) -> Result<PreparedContent> {
        let mut pack = Pack::new(self).await?;
        let result = self.prepare_into(source, &mut pack).await;
        match result {
            Ok(descriptor) => {
                pack.finish().await?;
                Ok(self.seal(descriptor))
            }
            Err(error) => {
                pack.abort().await;
                Err(error)
            }
        }
    }

    async fn prepare_into(
        &self,
        source: &mut (impl AsyncRead + Unpin),
        pack: &mut Pack,
    ) -> Result<ContentDescriptor> {
        let mut frontier = Vec::<(u64, u64, Child)>::new();
        let mut length = 0_u64;
        loop {
            let mut bytes = vec![0; BLOCK_BYTES];
            let mut used = 0;
            while used < bytes.len() {
                let count = source
                    .read(&mut bytes[used..])
                    .await
                    .map_err(|error| Error::from_io("read content source", error))?;
                if count == 0 {
                    break;
                }
                used += count;
            }
            if used == 0 {
                break;
            }
            bytes.truncate(used);
            let start = length / BLOCK_BYTES as u64;
            length = length
                .checked_add(used as u64)
                .ok_or_else(|| Error::invalid("prepare content", "length overflows"))?;
            let child = pack.leaf(start, &bytes).await?;
            frontier.push((start, 1, child));
            while frontier.len() >= 2
                && frontier[frontier.len() - 1].1 == frontier[frontier.len() - 2].1
            {
                let (_, right_count, right) = frontier.pop().expect("right frontier");
                let (start, left_count, left) = frontier.pop().expect("left frontier");
                let count = left_count + right_count;
                let child = pack.branch(start, count, left, right).await?;
                frontier.push((start, count, child));
            }
        }
        let root = if let Some((mut start, mut count, mut child)) = frontier.pop() {
            while let Some((left_start, left_count, left)) = frontier.pop() {
                start = left_start;
                count += left_count;
                child = pack.branch(start, count, left, child).await?;
            }
            debug_assert_eq!(start, 0);
            Some(child)
        } else {
            None
        };
        Ok(ContentDescriptor {
            filesystem: self.filesystem,
            length,
            root,
        })
    }

    /// Import untrusted references by verifying every logical unit.
    pub async fn import(&self, descriptor: &ContentDescriptor) -> Result<PreparedContent> {
        self.read_range(descriptor, 0..descriptor.length, &mut tokio::io::sink())
            .await?;
        Ok(self.seal(descriptor.clone()))
    }

    /// Read only the intersecting units and their proof paths.
    ///
    /// Each unit is authenticated before any of its bytes reach the destination.
    /// A later failure can leave a verified prefix, never an unverified unit.
    pub async fn read_range(
        &self,
        descriptor: &ContentDescriptor,
        range: Range<u64>,
        destination: &mut (impl AsyncWrite + Unpin),
    ) -> Result<()> {
        self.check(descriptor)?;
        if range.start > range.end || range.end > descriptor.length {
            return Err(Error::invalid(
                "read content range",
                "range exceeds logical length",
            ));
        }
        if range.is_empty() {
            return Ok(());
        }
        let root = descriptor
            .root
            .as_ref()
            .ok_or_else(|| corrupt("missing nonempty root"))?;
        self.read_subtree(
            root,
            0,
            leaves(descriptor.length),
            descriptor.length,
            &range,
            destination,
        )
        .await
    }

    async fn read_subtree(
        &self,
        child: &Child,
        start: u64,
        count: u64,
        length: u64,
        range: &Range<u64>,
        destination: &mut (impl AsyncWrite + Unpin),
    ) -> Result<()> {
        match self.node(child, start, count).await? {
            HashNode::Leaf(reference) => {
                let bytes = self.unit(&reference, child.hash, start, length).await?;
                let offset = start * BLOCK_BYTES as u64;
                let begin = range.start.saturating_sub(offset) as usize;
                let end = (range.end - offset).min(bytes.len() as u64) as usize;
                destination
                    .write_all(&bytes[begin..end])
                    .await
                    .map_err(|error| Error::from_io("write verified content", error))?;
            }
            HashNode::Branch(left, right) => {
                let left_count = split(count);
                let boundary = (start + left_count) * BLOCK_BYTES as u64;
                if range.start < boundary {
                    Box::pin(self.read_subtree(
                        &left,
                        start,
                        left_count,
                        length,
                        range,
                        destination,
                    ))
                    .await?;
                }
                if range.end > boundary {
                    Box::pin(self.read_subtree(
                        &right,
                        start + left_count,
                        count - left_count,
                        length,
                        range,
                        destination,
                    ))
                    .await?;
                }
            }
        }
        Ok(())
    }

    /// Fixed-offset update of trusted content. Work follows affected units and hash paths.
    pub async fn overwrite_prepared(
        &self,
        file: &PreparedContent,
        range: Range<u64>,
        bytes: &[u8],
    ) -> Result<PreparedContent> {
        let descriptor = self.accept(file)?;
        if range.start > range.end
            || range.end > descriptor.length
            || range.end - range.start != bytes.len() as u64
        {
            return Err(Error::invalid(
                "overwrite prepared content",
                "invalid replacement range",
            ));
        }
        if range.is_empty() {
            return Ok(file.clone());
        }
        let mut pack = Pack::new(self).await?;
        let result = self
            .patch(
                descriptor.root.as_ref().expect("nonempty content"),
                0,
                leaves(descriptor.length),
                descriptor.length,
                &range,
                bytes,
                &mut pack,
            )
            .await;
        match result {
            Ok(root) => {
                pack.finish().await?;
                Ok(self.seal(ContentDescriptor {
                    root: Some(root),
                    ..descriptor
                }))
            }
            Err(error) => {
                pack.abort().await;
                Err(error)
            }
        }
    }

    async fn patch(
        &self,
        child: &Child,
        start: u64,
        count: u64,
        length: u64,
        range: &Range<u64>,
        replacement: &[u8],
        pack: &mut Pack,
    ) -> Result<Child> {
        let offset = start * BLOCK_BYTES as u64;
        let subtree_end = start
            .checked_add(count)
            .and_then(|n| n.checked_mul(BLOCK_BYTES as u64))
            .unwrap_or(u64::MAX)
            .min(length);
        if range.end <= offset || range.start >= subtree_end {
            return Ok(child.clone());
        }
        match self.node(child, start, count).await? {
            HashNode::Leaf(reference) => {
                let mut bytes = self.unit(&reference, child.hash, start, length).await?;
                let begin = range.start.max(offset);
                let end = range.end.min(subtree_end);
                bytes[(begin - offset) as usize..(end - offset) as usize].copy_from_slice(
                    &replacement[(begin - range.start) as usize..(end - range.start) as usize],
                );
                pack.leaf(start, &bytes).await
            }
            HashNode::Branch(left, right) => {
                let left_count = split(count);
                let left = Box::pin(self.patch(
                    &left,
                    start,
                    left_count,
                    length,
                    range,
                    replacement,
                    pack,
                ))
                .await?;
                let right = Box::pin(self.patch(
                    &right,
                    start + left_count,
                    count - left_count,
                    length,
                    range,
                    replacement,
                    pack,
                ))
                .await?;
                pack.branch(start, count, left, right).await
            }
        }
    }

    async fn node(&self, child: &Child, start: u64, count: u64) -> Result<HashNode> {
        let bytes = self.read_extent(&child.reference, MAX_NODE_BYTES).await?;
        let node = HashNode::decode(&bytes)?;
        match &node {
            HashNode::Leaf(_) if count == 1 => {}
            HashNode::Branch(left, right)
                if count > 1 && branch_hash(start, count, left.hash, right.hash) == child.hash => {}
            _ => return Err(corrupt("invalid hash tree shape or proof")),
        }
        Ok(node)
    }

    async fn unit(
        &self,
        reference: &PackedRef,
        hash: [u8; 32],
        start: u64,
        length: u64,
    ) -> Result<Vec<u8>> {
        let bytes = self.read_extent(reference, BLOCK_BYTES as u32).await?;
        let expected = (length - start * BLOCK_BYTES as u64).min(BLOCK_BYTES as u64);
        if bytes.len() as u64 != expected || leaf_hash(start, &bytes) != hash {
            return Err(corrupt(
                "verification unit does not match the pinned logical root",
            ));
        }
        Ok(bytes)
    }

    fn check(&self, descriptor: &ContentDescriptor) -> Result<()> {
        if descriptor.filesystem != self.filesystem {
            return Err(Error::invalid("use content", "filesystem does not match"));
        }
        Ok(())
    }

    fn seal(&self, descriptor: ContentDescriptor) -> PreparedContent {
        PreparedContent {
            descriptor,
            issuer: self.issuer.clone(),
        }
    }

    pub(crate) fn published(&self, descriptor: ContentDescriptor) -> Result<PreparedContent> {
        self.check(&descriptor)?;
        Ok(self.seal(descriptor))
    }

    pub(crate) async fn put_metadata(&self, bytes: &[u8]) -> Result<PackedRef> {
        let mut pack = Pack::new(self).await?;
        let reference = match pack.append(bytes.to_vec()).await {
            Ok(reference) => reference,
            Err(error) => {
                pack.abort().await;
                return Err(error);
            }
        };
        pack.finish().await?;
        Ok(reference)
    }

    pub(crate) async fn read_extent(&self, reference: &PackedRef, maximum: u32) -> Result<Vec<u8>> {
        let end = reference
            .offset
            .checked_add(reference.length as u64)
            .ok_or_else(|| corrupt("extent overflows"))?;
        if reference.length == 0 || reference.length > maximum {
            return Err(corrupt("extent length exceeds its profile bound"));
        }
        let bytes = self
            .operator
            .read_with(&self.key(reference.object))
            .range(reference.offset..end)
            .await
            .map_err(storage_error)?
            .to_bytes()
            .to_vec();
        if bytes.len() != reference.length as usize
            || blake3::hash(&bytes).as_bytes() != &reference.digest
        {
            return Err(corrupt("extent checksum or length does not match"));
        }
        Ok(bytes)
    }

    fn key(&self, object: [u8; 16]) -> String {
        format!(
            ".yinyang/v2/{}/packs/{}",
            uuid::Uuid::from_bytes(*self.filesystem.as_bytes()).simple(),
            uuid::Uuid::from_bytes(object).simple()
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PackedRef {
    pub(crate) object: [u8; 16],
    pub(crate) offset: u64,
    pub(crate) length: u32,
    pub(crate) digest: [u8; 32],
}

impl PackedRef {
    pub(crate) fn wire(&self) -> RefWire {
        (self.object, self.offset, self.length, self.digest)
    }
    pub(crate) fn from_wire((object, offset, length, digest): RefWire) -> Result<Self> {
        if length == 0 || offset.checked_add(length as u64).is_none() {
            return Err(corrupt("invalid extent"));
        }
        Ok(Self {
            object,
            offset,
            length,
            digest,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Child {
    hash: [u8; 32],
    reference: PackedRef,
}

impl Child {
    fn wire(&self) -> ChildWire {
        (self.hash, self.reference.wire())
    }
    fn from_wire((hash, reference): ChildWire) -> Result<Self> {
        Ok(Self {
            hash,
            reference: PackedRef::from_wire(reference)?,
        })
    }
}

enum HashNode {
    Leaf(PackedRef),
    Branch(Child, Child),
}

impl HashNode {
    fn bytes(&self) -> Result<Vec<u8>> {
        match self {
            Self::Leaf(reference) => encode(&(NODE_MAGIC, 0_u8, reference.wire())),
            Self::Branch(left, right) => encode(&(NODE_MAGIC, 1_u8, left.wire(), right.wire())),
        }
    }

    fn decode(bytes: &[u8]) -> Result<Self> {
        if !bytes.starts_with(&NODE_MAGIC) {
            return Err(corrupt("invalid hash index profile"));
        }
        match bytes.get(8) {
            Some(0) => Ok(Self::Leaf(PackedRef::from_wire(decode(&bytes[9..])?)?)),
            Some(1) => {
                let (left, right): (ChildWire, ChildWire) = decode(&bytes[9..])?;
                Ok(Self::Branch(
                    Child::from_wire(left)?,
                    Child::from_wire(right)?,
                ))
            }
            _ => Err(corrupt("invalid hash index kind")),
        }
    }
}

struct Pack {
    store: DataStore,
    object: [u8; 16],
    writer: opendal::Writer,
    length: u64,
    hasher: blake3::Hasher,
}

impl Pack {
    async fn new(store: &DataStore) -> Result<Self> {
        let object = *uuid::Uuid::new_v4().as_bytes();
        let writer = store
            .operator
            .writer_with(&store.key(object))
            .if_not_exists(true)
            .chunk(5 * 1024 * 1024)
            .await
            .map_err(|error| Error::from_storage("prepare immutable pack", error))?;
        Ok(Self {
            store: store.clone(),
            object,
            writer,
            length: 0,
            hasher: blake3::Hasher::new(),
        })
    }

    async fn append(&mut self, bytes: Vec<u8>) -> Result<PackedRef> {
        let length = u32::try_from(bytes.len()).map_err(|_| corrupt("extent is too large"))?;
        let reference = PackedRef {
            object: self.object,
            offset: self.length,
            length,
            digest: blake3::hash(&bytes).into(),
        };
        self.length = self
            .length
            .checked_add(length as u64)
            .ok_or_else(|| corrupt("pack length overflows"))?;
        self.hasher.update(&bytes);
        self.writer
            .write(bytes)
            .await
            .map_err(|error| Error::from_storage("write immutable pack", error))?;
        Ok(reference)
    }

    async fn leaf(&mut self, position: u64, bytes: &[u8]) -> Result<Child> {
        let hash = leaf_hash(position, bytes);
        let data = self.append(bytes.to_vec()).await?;
        let reference = self.append(HashNode::Leaf(data).bytes()?).await?;
        Ok(Child { hash, reference })
    }

    async fn branch(&mut self, start: u64, count: u64, left: Child, right: Child) -> Result<Child> {
        let hash = branch_hash(start, count, left.hash, right.hash);
        let reference = self.append(HashNode::Branch(left, right).bytes()?).await?;
        Ok(Child { hash, reference })
    }

    async fn finish(mut self) -> Result<()> {
        if self.length == 0 {
            self.abort().await;
            return Ok(());
        }
        if let Err(error) = self.writer.close().await {
            self.abort().await;
            return Err(Error::from_storage("finish immutable pack", error));
        }
        let reader = self
            .store
            .operator
            .reader_with(&self.store.key(self.object))
            .chunk(BLOCK_BYTES)
            .await
            .map_err(storage_error)?;
        let mut stream = reader.into_stream(..).await.map_err(storage_error)?;
        let mut length = 0_u64;
        let mut hash = blake3::Hasher::new();
        while let Some(buffer) = stream.try_next().await.map_err(storage_error)? {
            for bytes in buffer {
                length = length
                    .checked_add(bytes.len() as u64)
                    .filter(|length| *length <= self.length)
                    .ok_or_else(|| corrupt("prepared pack is longer than expected"))?;
                hash.update(&bytes);
            }
        }
        if length != self.length || hash.finalize() != self.hasher.finalize() {
            return Err(corrupt("prepared pack failed readback verification"));
        }
        Ok(())
    }

    async fn abort(&mut self) {
        let _ = self.writer.abort().await;
    }
}

fn leaves(length: u64) -> u64 {
    length.div_ceil(BLOCK_BYTES as u64)
}
fn split(count: u64) -> u64 {
    1_u64 << (63 - (count - 1).leading_zeros())
}

fn domain(tag: &[u8]) -> blake3::Hasher {
    let mut hash = blake3::Hasher::new_derive_key("Apache OpenDAL YinYang content profile 2");
    hash.update(tag);
    hash
}
fn empty_hash() -> [u8; 32] {
    domain(b"empty").finalize().into()
}
fn leaf_hash(position: u64, bytes: &[u8]) -> [u8; 32] {
    let mut hash = domain(b"leaf");
    hash.update(&position.to_le_bytes());
    hash.update(&(bytes.len() as u64).to_le_bytes());
    hash.update(bytes);
    hash.finalize().into()
}
fn branch_hash(start: u64, count: u64, left: [u8; 32], right: [u8; 32]) -> [u8; 32] {
    let mut hash = domain(b"branch");
    hash.update(&start.to_le_bytes());
    hash.update(&count.to_le_bytes());
    hash.update(&left);
    hash.update(&right);
    hash.finalize().into()
}
fn encode(value: &impl borsh::BorshSerialize) -> Result<Vec<u8>> {
    borsh::to_vec(value).map_err(|error| Error::invalid("encode content", error.to_string()))
}
fn decode<T: borsh::BorshDeserialize>(bytes: &[u8]) -> Result<T> {
    borsh::from_slice(bytes).map_err(|error| corrupt(error.to_string()))
}
fn corrupt(message: impl Into<String>) -> Error {
    Error::corrupt("verify content", message)
}
fn storage_error(error: opendal::Error) -> Error {
    if matches!(
        error.kind(),
        opendal::ErrorKind::NotFound | opendal::ErrorKind::RangeNotSatisfied
    ) {
        corrupt("referenced extent is missing")
    } else {
        Error::from_storage("read immutable content", error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn content_vectors() {
        let empty = ContentDescriptor {
            filesystem: NodeId::from_bytes([0; 16]),
            length: 0,
            root: None,
        };
        assert_eq!(
            blake3::Hash::from(*empty.content_id().digest())
                .to_hex()
                .as_str(),
            "8b6e8e7afc13ad19dbda880a74217bc132f02679c34b4338cd318b6c264c7714"
        );
        assert_eq!(
            blake3::Hash::from(leaf_hash(0, b"abc")).to_hex().as_str(),
            "47134cf0c0612369f04c52aff76036d23a91db22a5cc8f15a4589cec1f7f9cb5"
        );
        assert_eq!(
            blake3::Hash::from(leaf_hash(1, b"x")).to_hex().as_str(),
            "4569163fa8678830f8c5b4814cb162d80d2e6044c323b593fd6b8231905b6e9f"
        );
        assert_eq!(
            blake3::Hash::from(branch_hash(
                0,
                2,
                leaf_hash(0, &vec![7; BLOCK_BYTES]),
                leaf_hash(1, b"x")
            ))
            .to_hex()
            .as_str(),
            "4a9449de9b3ed735f25145e7ec1dbf209b8b698092e29a7f476215dcd1c8ced3"
        );
    }
}
