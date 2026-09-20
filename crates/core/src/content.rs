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

use std::collections::BTreeMap;

use futures_util::TryStreamExt as _;
use tokio::io::{AsyncRead, AsyncReadExt as _, AsyncWrite, AsyncWriteExt as _};

use crate::{BlobRef, ContentId, Error, File, FilePart, Fs, NodeBody, Result, Tree};

const DATA_PREFIX: &str = ".yinyang/data/";
const BUFFER_BYTES: usize = 256 * 1024;

impl Fs {
    /// Upload a complete file without publishing a namespace change.
    ///
    /// Returns only after the backend acknowledges the immutable object.
    /// Failed or cancelled uploads can leave unreachable data. Empty files do
    /// not allocate an object.
    pub async fn write_file(&self, source: &mut (impl AsyncRead + Unpin)) -> Result<File> {
        let mut bytes = vec![0; BUFFER_BYTES];
        let first = source
            .read(&mut bytes)
            .await
            .map_err(|error| Error::from_io("read YinYang file source", error))?;
        if first == 0 {
            return File::new(ContentId::new(blake3::hash(&[]).into(), 0), Vec::new());
        }
        let path = format!("{DATA_PREFIX}{}", uuid::Uuid::new_v4().simple());
        let mut writer = self
            .operator
            .writer_with(&path)
            .if_not_exists(true)
            .await
            .map_err(|error| Error::from_storage("write YinYang file", error))?;
        let result = async {
            let mut hasher = blake3::Hasher::new();
            let mut length = 0_u64;
            let mut count = first;
            loop {
                length = length
                    .checked_add(count as u64)
                    .ok_or_else(|| Error::invalid("write YinYang file", "file length overflows"))?;
                hasher.update(&bytes[..count]);
                writer
                    .write(bytes[..count].to_vec())
                    .await
                    .map_err(|error| Error::from_storage("write YinYang file", error))?;
                count = source
                    .read(&mut bytes)
                    .await
                    .map_err(|error| Error::from_io("read YinYang file source", error))?;
                if count == 0 {
                    break;
                }
            }
            writer
                .close()
                .await
                .map_err(|error| Error::from_storage("close YinYang file", error))?;
            let content = ContentId::new(hasher.finalize().into(), length);
            File::new(
                content,
                vec![FilePart::new(0..length, 0, BlobRef::new(path, content))?],
            )
        }
        .await;
        if result.is_err() {
            let _ = writer.abort().await;
        }
        result
    }

    /// Copy and verify a complete file with bounded transfer buffers.
    ///
    /// Every referenced blob is read in full, including bytes outside a part,
    /// because its digest covers the complete blob. The assembled logical
    /// file is checked separately. Bytes delivered before this method returns
    /// are provisional: on any error the caller must discard the destination.
    /// This method does not flush, close, or sync the caller's destination.
    pub async fn read_file(
        &self,
        file: &File,
        destination: &mut (impl AsyncWrite + Unpin),
    ) -> Result<()> {
        let mut logical_hasher = blake3::Hasher::new();
        for part in file.parts() {
            let reference = part.blob();
            let path = data_path(reference)?;
            let reader = self
                .operator
                .reader_with(path)
                .chunk(BUFFER_BYTES)
                .await
                .map_err(read_error)?;
            let mut stream = reader.into_stream(..).await.map_err(read_error)?;
            let mut hasher = blake3::Hasher::new();
            let mut offset = 0_u64;
            let start = part.blob_offset();
            let end = start + (part.range().end - part.range().start);
            while let Some(buffer) = stream.try_next().await.map_err(read_error)? {
                for bytes in buffer {
                    let next = offset
                        .checked_add(bytes.len() as u64)
                        .filter(|next| *next <= reference.content().length())
                        .ok_or_else(|| {
                            Error::corrupt("read YinYang file", "blob exceeds its declared length")
                        })?;
                    hasher.update(&bytes);
                    let copy_start = start.max(offset);
                    let copy_end = end.min(next);
                    if copy_start < copy_end {
                        let selected =
                            &bytes[(copy_start - offset) as usize..(copy_end - offset) as usize];
                        logical_hasher.update(selected);
                        destination.write_all(selected).await.map_err(|error| {
                            Error::from_io("write YinYang file destination", error)
                        })?;
                    }
                    offset = next;
                }
            }
            if ContentId::new(hasher.finalize().into(), offset) != reference.content() {
                return Err(Error::corrupt(
                    "read YinYang file",
                    "blob does not match its reference",
                ));
            }
        }
        if logical_hasher.finalize().as_bytes() != file.content().digest() {
            return Err(Error::corrupt(
                "read YinYang file",
                "logical content digest does not match",
            ));
        }
        Ok(())
    }

    pub(crate) async fn verify_new_files(&self, previous: &Tree, next: &Tree) -> Result<()> {
        let previous = previous
            .iter()
            .map(|(_, node)| (node.id(), node))
            .collect::<BTreeMap<_, _>>();
        for (_, node) in next.iter() {
            if let NodeBody::File(file) = node.body() {
                if previous
                    .get(&node.id())
                    .is_some_and(|old| old.body() == node.body())
                {
                    continue;
                }
                self.read_file(file, &mut tokio::io::sink()).await?;
            }
        }
        Ok(())
    }
}

fn data_path(reference: &BlobRef) -> Result<&str> {
    let path = std::str::from_utf8(reference.as_bytes())
        .map_err(|_| Error::corrupt("read YinYang file", "invalid data reference"))?;
    let valid = path.strip_prefix(DATA_PREFIX).is_some_and(|id| {
        id.len() == 32
            && id
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    });
    if !valid {
        return Err(Error::corrupt(
            "read YinYang file",
            "invalid data reference",
        ));
    }
    Ok(path)
}

fn read_error(error: opendal::Error) -> Error {
    if error.kind() == opendal::ErrorKind::NotFound {
        Error::corrupt("read YinYang file", "referenced data is missing")
    } else {
        Error::from_storage("read YinYang file", error)
    }
}
