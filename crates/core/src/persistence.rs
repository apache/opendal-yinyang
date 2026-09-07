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

use futures_util::TryStreamExt as _;
use opendal::{ErrorKind as StorageErrorKind, Operator, Writer};

use crate::{
    BlobRef, CommitId, ContentId, Error, File, FilePart, FsVersion, Generation, Node, NodeBody,
    NodeId, Path, Result, Tree,
};

const HEAD_PATH: &str = ".yinyang/head";
const VERSION_PREFIX: &str = ".yinyang/versions/";
const HEAD_MAGIC: &[u8; 8] = b"YYHEAD01";
const VERSION_MAGIC: &[u8; 8] = b"YYVER001";
const MAX_HEAD_BYTES: usize = 4 * 1024;
const MAX_MATERIALIZED_VERSION_BYTES: usize = 64 * 1024 * 1024;
const VERSION_WRITE_BUFFER_BYTES: usize = 256 * 1024;
const CHECKSUM_BYTES: usize = 32;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct HeadObservation {
    version: BlobRef,
    etag: String,
}

impl HeadObservation {
    pub(crate) const fn version(&self) -> &BlobRef {
        &self.version
    }
}

pub(crate) fn validate_operator(operator: &Operator) -> Result<()> {
    let capability = operator.info().capability();
    if !capability.read
        || !capability.write
        || !capability.write_can_multi
        || !capability.write_with_if_match
        || !capability.write_with_if_not_exists
    {
        return Err(Error::unsupported(
            "use YinYang filesystem",
            "OpenDAL backend must support read, streaming write, create-if-absent, and ETag if-match",
        ));
    }
    Ok(())
}

pub(crate) async fn write_version(operator: &Operator, version: &FsVersion) -> Result<BlobRef> {
    let path = new_version_path();
    let writer = operator
        .writer_with(&path)
        .if_not_exists(true)
        .await
        .map_err(|error| Error::from_storage("write YinYang version", error))?;
    let mut encoder = VersionEncoder::new(writer);
    if let Err(error) = encoder.encode(version).await {
        encoder.abort().await;
        return Err(error);
    }
    let content = match encoder.finish().await {
        Ok(content) => content,
        Err(error) => {
            encoder.abort().await;
            return Err(error);
        }
    };
    Ok(BlobRef::new(path, content))
}

pub(crate) async fn read_version(operator: &Operator, reference: &BlobRef) -> Result<FsVersion> {
    let path = version_reference_path(reference)
        .ok_or_else(|| Error::corrupt("read YinYang version", "version reference is invalid"))?;
    if reference.content().length() > MAX_MATERIALIZED_VERSION_BYTES as u64 {
        return Err(Error::unsupported(
            "read YinYang version",
            "version exceeds the current materialized implementation limit",
        ));
    }
    if reference.content().length() < VERSION_MAGIC.len() as u64 {
        return Err(Error::corrupt(
            "read YinYang version",
            "version reference is invalid",
        ));
    }
    let object = read_bounded(
        operator,
        path,
        MAX_MATERIALIZED_VERSION_BYTES,
        "read YinYang version",
    )
    .await?
    .ok_or_else(|| Error::corrupt("read YinYang version", "referenced version is missing"))?;
    if content_id(&object.bytes) != reference.content() {
        return Err(Error::corrupt(
            "read YinYang version",
            "version does not match its reference",
        ));
    }
    decode_version(&object.bytes)
}

pub(crate) async fn create_head(operator: &Operator, version: &BlobRef) -> Result<()> {
    let bytes = encode_head(version)?;
    match operator
        .write_with(HEAD_PATH, bytes)
        .if_not_exists(true)
        .await
    {
        Ok(_) => Ok(()),
        Err(error)
            if matches!(
                error.kind(),
                StorageErrorKind::AlreadyExists | StorageErrorKind::ConditionNotMatch
            ) =>
        {
            Ok(())
        }
        Err(error) => Err(Error::from_storage("create YinYang head", error)),
    }
}

pub(crate) async fn observe_head(operator: &Operator) -> Result<Option<HeadObservation>> {
    let Some(object) =
        read_bounded(operator, HEAD_PATH, MAX_HEAD_BYTES, "observe YinYang head").await?
    else {
        return Ok(None);
    };
    let etag = object.etag.ok_or_else(|| {
        Error::unsupported(
            "observe YinYang head",
            "OpenDAL backend did not return an ETag with the head read",
        )
    })?;
    Ok(Some(HeadObservation {
        version: decode_head(&object.bytes)?,
        etag,
    }))
}

pub(crate) async fn replace_head(
    operator: &Operator,
    observed: &HeadObservation,
    next: &BlobRef,
) -> Result<bool> {
    let bytes = encode_head(next)?;
    match operator
        .write_with(HEAD_PATH, bytes)
        .if_match(&observed.etag)
        .await
    {
        Ok(_) => Ok(true),
        Err(error)
            if matches!(
                error.kind(),
                StorageErrorKind::ConditionNotMatch | StorageErrorKind::NotFound
            ) =>
        {
            Ok(false)
        }
        Err(error) => Err(Error::from_storage("replace YinYang head", error)),
    }
}

struct StoredObject {
    bytes: Vec<u8>,
    etag: Option<String>,
}

async fn read_bounded(
    operator: &Operator,
    path: &str,
    maximum_bytes: usize,
    operation: &'static str,
) -> Result<Option<StoredObject>> {
    let reader = match operator.reader(path).await {
        Ok(reader) => reader,
        Err(error) if error.kind() == StorageErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(Error::from_storage(operation, error)),
    };
    let mut stream = match reader.into_stream(..).await {
        Ok(stream) => stream,
        Err(error) if error.kind() == StorageErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(Error::from_storage(operation, error)),
    };
    let (capacity, etag) = match stream.metadata().await {
        Ok(metadata) => {
            let length = usize::try_from(metadata.content_length())
                .ok()
                .filter(|length| *length <= maximum_bytes)
                .ok_or_else(|| Error::corrupt(operation, "object exceeds its size limit"))?;
            (length, metadata.etag().map(str::to_owned))
        }
        Err(error) if error.kind() == StorageErrorKind::NotFound => return Ok(None),
        Err(error) if error.kind() == StorageErrorKind::Unsupported => (0, None),
        Err(error) => return Err(Error::from_storage(operation, error)),
    };

    let mut bytes = Vec::with_capacity(capacity);
    while let Some(buffer) = stream
        .try_next()
        .await
        .map_err(|error| Error::from_storage(operation, error))?
    {
        if buffer.len() > maximum_bytes.saturating_sub(bytes.len()) {
            return Err(Error::corrupt(operation, "object exceeds its size limit"));
        }
        for chunk in buffer {
            bytes.extend_from_slice(&chunk);
        }
    }
    Ok(Some(StoredObject { bytes, etag }))
}

fn decode_version(bytes: &[u8]) -> Result<FsVersion> {
    if bytes.len() > MAX_MATERIALIZED_VERSION_BYTES {
        return Err(Error::unsupported(
            "decode YinYang version",
            "version exceeds the current materialized implementation limit",
        ));
    }
    if !bytes.starts_with(VERSION_MAGIC) {
        return Err(Error::corrupt(
            "decode YinYang version",
            "version envelope is invalid",
        ));
    }

    let mut decoder = Decoder::new(&bytes[VERSION_MAGIC.len()..], "decode YinYang version");
    let entry_count: u32 = decoder.value()?;
    let mut entries = Vec::new();
    for _ in 0..entry_count {
        let path = Path::new(decoder.value::<String>()?).map_err(corrupt_version_value)?;
        entries.push((path, decode_node(&mut decoder)?));
    }
    let commit_count: u32 = decoder.value()?;
    let mut commits = Vec::new();
    for _ in 0..commit_count {
        commits.push(CommitId::from_bytes(decoder.value()?));
    }
    decoder.finish()?;

    let tree = Tree::from_entries(entries).map_err(corrupt_version_value)?;
    FsVersion::new(tree, commits).map_err(corrupt_version_value)
}

fn encode_head(reference: &BlobRef) -> Result<Vec<u8>> {
    let mut encoder = Encoder::new(HEAD_MAGIC, MAX_HEAD_BYTES, "encode YinYang head");
    encode_blob_ref(&mut encoder, reference)?;
    let checksum = blake3::hash(encoder.as_bytes());
    encoder.raw_bytes(checksum.as_bytes())?;
    Ok(encoder.finish())
}

fn decode_head(bytes: &[u8]) -> Result<BlobRef> {
    if bytes.len() < HEAD_MAGIC.len() + CHECKSUM_BYTES
        || bytes.len() > MAX_HEAD_BYTES
        || !bytes.starts_with(HEAD_MAGIC)
    {
        return Err(Error::corrupt(
            "decode YinYang head",
            "head envelope is invalid",
        ));
    }
    let checksum_offset = bytes.len() - CHECKSUM_BYTES;
    if blake3::hash(&bytes[..checksum_offset]).as_bytes() != &bytes[checksum_offset..] {
        return Err(Error::corrupt(
            "decode YinYang head",
            "head checksum is invalid",
        ));
    }
    let mut decoder = Decoder::new(
        &bytes[HEAD_MAGIC.len()..checksum_offset],
        "decode YinYang head",
    );
    let reference = decode_blob_ref(&mut decoder)?;
    decoder.finish()?;
    Ok(reference)
}

fn content_id(bytes: &[u8]) -> ContentId {
    ContentId::new(blake3::hash(bytes).into(), bytes.len() as u64)
}

fn new_version_path() -> String {
    format!("{VERSION_PREFIX}{}", uuid::Uuid::new_v4().simple())
}

fn version_reference_path(reference: &BlobRef) -> Option<&str> {
    let path = std::str::from_utf8(reference.as_bytes()).ok()?;
    let key = path.strip_prefix(VERSION_PREFIX)?;
    (key.len() == 32
        && key
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)))
    .then_some(path)
}

fn encode_node(encoder: &mut Encoder, node: &Node) -> Result<()> {
    encoder.value(node.id().as_bytes())?;
    encoder.value(&node.generation().value())?;
    encoder.value(&node.executable())?;
    match node.body() {
        NodeBody::Dir { entries_generation } => {
            encoder.value(&0_u8)?;
            encoder.value(&entries_generation.value())
        }
        NodeBody::File(file) => {
            encoder.value(&1_u8)?;
            encode_file(encoder, file)
        }
    }
}

fn encode_file(encoder: &mut Encoder, file: &File) -> Result<()> {
    encode_content_id(encoder, file.content())?;
    encoder.sequence_len(file.parts().len())?;
    for part in file.parts() {
        encoder.value(&part.range().start)?;
        encoder.value(&part.range().end)?;
        encoder.value(&part.blob_offset())?;
        encode_blob_ref(encoder, part.blob())?;
    }
    Ok(())
}

fn encode_blob_ref(encoder: &mut Encoder, reference: &BlobRef) -> Result<()> {
    encoder.value(reference.as_bytes())?;
    encode_content_id(encoder, reference.content())
}

fn encode_content_id(encoder: &mut Encoder, content: ContentId) -> Result<()> {
    encoder.value(content.digest())?;
    encoder.value(&content.length())
}

fn decode_node(decoder: &mut Decoder<'_>) -> Result<Node> {
    let id = NodeId::from_bytes(decoder.value()?);
    let generation = Generation::from_value(decoder.value()?);
    let executable = decoder.value()?;
    match decoder.value::<u8>()? {
        0 => Ok(Node::dir(
            id,
            generation,
            executable,
            Generation::from_value(decoder.value()?),
        )),
        1 => Ok(Node::file(
            id,
            generation,
            executable,
            decode_file(decoder)?,
        )),
        _ => Err(decoder.corrupt("node kind is invalid")),
    }
}

fn decode_file(decoder: &mut Decoder<'_>) -> Result<File> {
    let content = decode_content_id(decoder)?;
    let part_count: u32 = decoder.value()?;
    let mut parts = Vec::new();
    for _ in 0..part_count {
        let start = decoder.value()?;
        let end = decoder.value()?;
        let blob_offset = decoder.value()?;
        let blob = decode_blob_ref(decoder)?;
        parts.push(FilePart::new(start..end, blob_offset, blob).map_err(corrupt_version_value)?);
    }
    File::new(content, parts).map_err(corrupt_version_value)
}

fn decode_blob_ref(decoder: &mut Decoder<'_>) -> Result<BlobRef> {
    let reference = decoder.value::<Vec<u8>>()?;
    Ok(BlobRef::new(reference, decode_content_id(decoder)?))
}

fn decode_content_id(decoder: &mut Decoder<'_>) -> Result<ContentId> {
    Ok(ContentId::new(decoder.value()?, decoder.value()?))
}

fn corrupt_version_value(error: Error) -> Error {
    Error::corrupt("decode YinYang version", error.message())
}

struct VersionEncoder {
    writer: Writer,
    encoder: Encoder,
    hasher: blake3::Hasher,
}

impl VersionEncoder {
    fn new(writer: Writer) -> Self {
        Self {
            writer,
            encoder: Encoder::new_with_implementation_limit(
                VERSION_MAGIC,
                MAX_MATERIALIZED_VERSION_BYTES,
                "encode YinYang version",
            ),
            hasher: blake3::Hasher::new(),
        }
    }

    async fn encode(&mut self, version: &FsVersion) -> Result<()> {
        self.encoder.sequence_len(version.tree().iter().count())?;
        for (path, node) in version.tree().iter() {
            self.encoder.value(path.as_str())?;
            encode_node(&mut self.encoder, node)?;
            self.flush_if_full().await?;
        }
        self.encoder.sequence_len(version.commits().len())?;
        for commit in version.commits() {
            self.encoder.value(commit.as_bytes())?;
            self.flush_if_full().await?;
        }
        Ok(())
    }

    async fn flush_if_full(&mut self) -> Result<()> {
        if self.encoder.buffered_len() >= VERSION_WRITE_BUFFER_BYTES {
            self.flush().await?;
        }
        Ok(())
    }

    async fn flush(&mut self) -> Result<()> {
        let bytes = self.encoder.take_bytes();
        if bytes.is_empty() {
            return Ok(());
        }
        self.hasher.update(&bytes);
        self.writer
            .write(bytes)
            .await
            .map_err(|error| Error::from_storage("write YinYang version", error))
    }

    async fn finish(&mut self) -> Result<ContentId> {
        self.flush().await?;
        self.writer
            .close()
            .await
            .map_err(|error| Error::from_storage("write YinYang version", error))?;
        Ok(ContentId::new(
            self.hasher.finalize().into(),
            self.encoder.encoded_len() as u64,
        ))
    }

    async fn abort(&mut self) {
        let _ = self.writer.abort().await;
    }
}

struct Encoder {
    bytes: Vec<u8>,
    flushed_bytes: usize,
    maximum_bytes: usize,
    operation: &'static str,
    implementation_limit: bool,
    limit_exceeded: bool,
}

impl Encoder {
    fn new(magic: &[u8], maximum_bytes: usize, operation: &'static str) -> Self {
        Self {
            bytes: magic.to_vec(),
            flushed_bytes: 0,
            maximum_bytes,
            operation,
            implementation_limit: false,
            limit_exceeded: false,
        }
    }

    fn new_with_implementation_limit(
        magic: &[u8],
        maximum_bytes: usize,
        operation: &'static str,
    ) -> Self {
        Self {
            bytes: magic.to_vec(),
            flushed_bytes: 0,
            maximum_bytes,
            operation,
            implementation_limit: true,
            limit_exceeded: false,
        }
    }

    fn sequence_len(&mut self, value: usize) -> Result<()> {
        let value = u32::try_from(value)
            .map_err(|_| Error::invalid(self.operation, "length cannot be encoded"))?;
        self.value(&value)
    }

    fn value<T: borsh::BorshSerialize + ?Sized>(&mut self, value: &T) -> Result<()> {
        value
            .serialize(self)
            .map_err(|error| self.encode_error(error))
    }

    fn raw_bytes(&mut self, value: &[u8]) -> Result<()> {
        borsh::io::Write::write_all(self, value).map_err(|error| self.encode_error(error))
    }

    fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    fn buffered_len(&self) -> usize {
        self.bytes.len()
    }

    fn encoded_len(&self) -> usize {
        self.flushed_bytes + self.bytes.len()
    }

    fn take_bytes(&mut self) -> Vec<u8> {
        self.flushed_bytes += self.bytes.len();
        std::mem::replace(
            &mut self.bytes,
            Vec::with_capacity(VERSION_WRITE_BUFFER_BYTES),
        )
    }

    fn encode_error(&self, error: borsh::io::Error) -> Error {
        if self.implementation_limit && self.limit_exceeded {
            Error::unsupported(
                self.operation,
                "version exceeds the current materialized implementation limit",
            )
        } else {
            Error::invalid(self.operation, error.to_string())
        }
    }

    fn finish(self) -> Vec<u8> {
        debug_assert_eq!(self.flushed_bytes, 0);
        self.bytes
    }
}

impl borsh::io::Write for Encoder {
    fn write(&mut self, value: &[u8]) -> borsh::io::Result<usize> {
        if value.len() > self.maximum_bytes.saturating_sub(self.encoded_len()) {
            self.limit_exceeded = true;
            return Err(borsh::io::Error::new(
                borsh::io::ErrorKind::InvalidData,
                "encoded object exceeds its size limit",
            ));
        }
        self.bytes.extend_from_slice(value);
        Ok(value.len())
    }

    fn flush(&mut self) -> borsh::io::Result<()> {
        Ok(())
    }
}

struct Decoder<'a> {
    bytes: &'a [u8],
    operation: &'static str,
}

impl<'a> Decoder<'a> {
    const fn new(bytes: &'a [u8], operation: &'static str) -> Self {
        Self { bytes, operation }
    }

    fn value<T: borsh::BorshDeserialize>(&mut self) -> Result<T> {
        T::deserialize(&mut self.bytes).map_err(|error| self.corrupt(error.to_string()))
    }

    fn finish(self) -> Result<()> {
        if !self.bytes.is_empty() {
            return Err(self.corrupt("encoded object contains trailing bytes"));
        }
        Ok(())
    }

    fn corrupt(&self, message: impl Into<String>) -> Error {
        Error::corrupt(self.operation, message)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opendal::services::Memory;

    async fn persisted_version_bytes(version: &FsVersion) -> Vec<u8> {
        let operator = Operator::new(Memory::default()).unwrap();
        let reference = write_version(&operator, version).await.unwrap();
        let path = version_reference_path(&reference).unwrap();
        let bytes = operator.read(path).await.unwrap().to_bytes().to_vec();
        assert_eq!(content_id(&bytes), reference.content());
        bytes
    }

    #[tokio::test]
    async fn version_wire_contract_is_stable() {
        let root = NodeId::from_bytes([1; 16]);
        let version = FsVersion::new(Tree::genesis(root), Vec::new()).unwrap();

        let actual = persisted_version_bytes(&version).await;
        let mut expected = VERSION_MAGIC.to_vec();
        expected.extend_from_slice(&1_u32.to_le_bytes());
        expected.extend_from_slice(&0_u32.to_le_bytes());
        expected.extend_from_slice(&[1; 16]);
        expected.extend_from_slice(&1_u64.to_le_bytes());
        expected.push(0);
        expected.push(0);
        expected.extend_from_slice(&1_u64.to_le_bytes());
        expected.extend_from_slice(&0_u32.to_le_bytes());

        assert_eq!(actual, expected);
        assert_eq!(decode_version(&actual).unwrap(), version);
    }

    #[tokio::test]
    async fn file_version_wire_contract_is_stable() {
        let root = NodeId::from_bytes([1; 16]);
        let mut tree = Tree::genesis(root);
        tree.insert(
            Path::root(),
            Node::dir(root, Generation::FIRST, false, Generation::from_value(2)),
        );
        let blob = BlobRef::new(b"blob".to_vec(), ContentId::new([4; 32], 8));
        let file = File::new(
            ContentId::new([3; 32], 4),
            vec![FilePart::new(0..4, 1, blob).unwrap()],
        )
        .unwrap();
        tree.insert(
            Path::new("file").unwrap(),
            Node::file(
                NodeId::from_bytes([2; 16]),
                Generation::from_value(3),
                true,
                file,
            ),
        );
        let version = FsVersion::new(tree, vec![CommitId::from_bytes([5; 16])]).unwrap();

        let actual = persisted_version_bytes(&version).await;
        let mut expected = VERSION_MAGIC.to_vec();
        expected.extend_from_slice(&2_u32.to_le_bytes());
        expected.extend_from_slice(&0_u32.to_le_bytes());
        expected.extend_from_slice(&[1; 16]);
        expected.extend_from_slice(&1_u64.to_le_bytes());
        expected.push(0);
        expected.push(0);
        expected.extend_from_slice(&2_u64.to_le_bytes());
        expected.extend_from_slice(&4_u32.to_le_bytes());
        expected.extend_from_slice(b"file");
        expected.extend_from_slice(&[2; 16]);
        expected.extend_from_slice(&3_u64.to_le_bytes());
        expected.push(1);
        expected.push(1);
        expected.extend_from_slice(&[3; 32]);
        expected.extend_from_slice(&4_u64.to_le_bytes());
        expected.extend_from_slice(&1_u32.to_le_bytes());
        expected.extend_from_slice(&0_u64.to_le_bytes());
        expected.extend_from_slice(&4_u64.to_le_bytes());
        expected.extend_from_slice(&1_u64.to_le_bytes());
        expected.extend_from_slice(&4_u32.to_le_bytes());
        expected.extend_from_slice(b"blob");
        expected.extend_from_slice(&[4; 32]);
        expected.extend_from_slice(&8_u64.to_le_bytes());
        expected.extend_from_slice(&1_u32.to_le_bytes());
        expected.extend_from_slice(&[5; 16]);

        assert_eq!(actual, expected);
        assert_eq!(decode_version(&actual).unwrap(), version);
    }

    #[tokio::test]
    async fn version_decoder_rejects_invalid_borsh() {
        let root = NodeId::from_bytes([1; 16]);
        let version = FsVersion::new(Tree::genesis(root), Vec::new()).unwrap();
        let encoded = persisted_version_bytes(&version).await;

        let mut invalid_boolean = encoded.clone();
        invalid_boolean[40] = 2;
        assert_eq!(
            decode_version(&invalid_boolean).unwrap_err().kind(),
            crate::ErrorKind::Corrupt
        );

        let mut invalid_node_kind = encoded.clone();
        invalid_node_kind[41] = 2;
        assert_eq!(
            decode_version(&invalid_node_kind).unwrap_err().kind(),
            crate::ErrorKind::Corrupt
        );

        assert_eq!(
            decode_version(&encoded[..encoded.len() - 1])
                .unwrap_err()
                .kind(),
            crate::ErrorKind::Corrupt
        );

        let mut trailing = encoded;
        trailing.push(0);
        assert_eq!(
            decode_version(&trailing).unwrap_err().kind(),
            crate::ErrorKind::Corrupt
        );
    }

    #[test]
    fn head_wire_contract_is_stable() {
        let reference = BlobRef::new(b"v".to_vec(), ContentId::new([2; 32], 3));

        let actual = encode_head(&reference).unwrap();
        let mut expected = HEAD_MAGIC.to_vec();
        expected.extend_from_slice(&1_u32.to_le_bytes());
        expected.push(b'v');
        expected.extend_from_slice(&[2; 32]);
        expected.extend_from_slice(&3_u64.to_le_bytes());
        let checksum = blake3::hash(&expected);
        expected.extend_from_slice(checksum.as_bytes());

        assert_eq!(actual, expected);
        assert_eq!(decode_head(&actual).unwrap(), reference);
    }

    #[test]
    fn oversized_materialized_version_is_unsupported() {
        let mut encoder = Encoder::new_with_implementation_limit(
            VERSION_MAGIC,
            VERSION_MAGIC.len(),
            "encode YinYang version",
        );

        let error = encoder.value(&0_u8).unwrap_err();

        assert_eq!(error.kind(), crate::ErrorKind::Unsupported);
    }

    #[tokio::test]
    async fn rejects_invalid_version_object_keys() {
        let operator = Operator::new(Memory::default()).unwrap();
        let reference = BlobRef::new(
            b".yinyang/versions/not-a-version-id".to_vec(),
            ContentId::new([0; 32], VERSION_MAGIC.len() as u64),
        );

        let error = read_version(&operator, &reference).await.unwrap_err();

        assert_eq!(error.kind(), crate::ErrorKind::Corrupt);
    }

    #[tokio::test]
    async fn oversized_version_reference_is_unsupported() {
        let operator = Operator::new(Memory::default()).unwrap();
        let reference = BlobRef::new(
            new_version_path(),
            ContentId::new([0; 32], MAX_MATERIALIZED_VERSION_BYTES as u64 + 1),
        );

        let error = read_version(&operator, &reference).await.unwrap_err();

        assert_eq!(error.kind(), crate::ErrorKind::Unsupported);
    }
}
