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

//! Stateless storage-native conditional mutation primitives.
//!
//! This module deliberately owns no local database or retry state. Direct Sync
//! and the durable Mount runtime persist the precondition and immutable input
//! themselves, then use these operations to make one backend request.

use super::{DirectKey, DirectNamespace, Error, ErrorKind, Result, normalize_time, pin_value};
use opendal::{Buffer, Metadata, Operator};
use std::path::Path;
use std::time::SystemTime;
use tokio::io::AsyncReadExt;

const COPY_CHUNK: usize = 64 * 1024;

/// One exact storage observation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DirectObjectPin {
    version: Option<String>,
    etag: Option<String>,
    size: u64,
    modified: Option<SystemTime>,
}

impl DirectObjectPin {
    fn from_metadata(metadata: &Metadata) -> Self {
        Self {
            version: pin_value(metadata.version()).map(str::to_owned),
            etag: pin_value(metadata.etag()).map(str::to_owned),
            size: metadata.content_length(),
            modified: metadata
                .last_modified()
                .map(|time| normalize_time(SystemTime::from(time))),
        }
    }

    /// The storage version, when the backend exposes one.
    pub fn version(&self) -> Option<&str> {
        self.version.as_deref()
    }

    /// The exact ETag returned by storage.
    pub fn etag(&self) -> Option<&str> {
        self.etag.as_deref()
    }

    /// The complete object length.
    pub fn size(&self) -> u64 {
        self.size
    }

    /// The storage modification time normalized to whole seconds.
    pub fn modified(&self) -> Option<SystemTime> {
        self.modified
    }
}

/// The storage condition used for one replacement or delete.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DirectPrecondition {
    /// The destination must not exist.
    Absent,
    /// The destination must still have this exact ETag.
    Etag(String),
}

impl DirectPrecondition {
    fn validate(&self) -> Result<()> {
        match self {
            Self::Absent => Ok(()),
            Self::Etag(etag) if etag.is_empty() || etag == "*" || etag == "null" => {
                Err(Error::new(
                    ErrorKind::Invalid,
                    "Direct requires a concrete ETag precondition",
                ))
            }
            Self::Etag(_) => Ok(()),
        }
    }
}

/// Mutation primitives available from the selected operator.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DirectMutationCapability {
    pub create: bool,
    pub replace: bool,
    pub delete: bool,
}

/// The observed result of one conditional backend request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DirectMutationOutcome {
    /// The backend accepted the request. `accepted` is response metadata for a
    /// write and `None` for a delete.
    Applied { accepted: Option<DirectObjectPin> },
    /// The supplied condition did not hold.
    Conflict { observed: Option<DirectObjectPin> },
    /// A request may have reached storage, but no definitive outcome is known.
    Unknown {
        observed: Option<DirectObjectPin>,
        message: String,
    },
    /// The selected backend does not provide the required native primitive.
    Unsupported { message: String },
}

/// Stateless conditional operations over one Direct namespace.
#[derive(Clone, Debug)]
pub struct DirectMutation {
    operator: Operator,
}

impl DirectMutation {
    /// Open the stateless mutation seam. This creates no local state.
    pub fn open(namespace: DirectNamespace) -> Result<Self> {
        Ok(Self {
            operator: namespace.operator.clone(),
        })
    }

    /// Report the conditional primitives advertised by the selected backend.
    pub fn capability(&self) -> DirectMutationCapability {
        let capability = self.operator.info().capability();
        DirectMutationCapability {
            create: capability.write && capability.write_with_if_not_exists,
            replace: capability.write && capability.write_with_if_match,
            delete: capability.delete && capability.delete_with_if_match,
        }
    }

    /// Stream a complete replacement from one immutable local source.
    ///
    /// The source is read once in bounded chunks. A source read error aborts the
    /// multipart request before completion and is returned as a local error. Any
    /// non-definitive completion error is represented as `Unknown`.
    pub async fn publish_file(
        &self,
        key: &DirectKey,
        source: &Path,
        precondition: DirectPrecondition,
    ) -> Result<DirectMutationOutcome> {
        precondition.validate()?;
        let capability = self.capability();
        match &precondition {
            DirectPrecondition::Absent if !capability.create => {
                return Ok(DirectMutationOutcome::Unsupported {
                    message: "storage does not advertise conditional creation".to_owned(),
                });
            }
            DirectPrecondition::Etag(_) if !capability.replace => {
                return Ok(DirectMutationOutcome::Unsupported {
                    message: "storage does not advertise conditional replacement".to_owned(),
                });
            }
            _ => {}
        }

        let mut input = tokio::fs::File::open(source).await.map_err(|error| {
            Error::new(
                ErrorKind::Storage,
                format!("open Direct mutation source {}: {error}", source.display()),
            )
        })?;
        let metadata = input.metadata().await.map_err(|error| {
            Error::new(
                ErrorKind::Storage,
                format!(
                    "inspect Direct mutation source {}: {error}",
                    source.display()
                ),
            )
        })?;
        if metadata.is_dir() {
            return Err(Error::new(
                ErrorKind::Invalid,
                format!("Direct mutation source {} is a directory", source.display()),
            ));
        }
        let source_length = metadata.len();

        let writer = self.operator.writer_with(key.as_str());
        let mut writer = match &precondition {
            DirectPrecondition::Absent => writer.if_not_exists(true).await,
            DirectPrecondition::Etag(etag) => writer.if_match(etag).await,
        }
        .map_err(Error::storage)?;

        let mut read_length = 0_u64;
        let mut buffer = vec![0_u8; COPY_CHUNK];
        loop {
            let read = input.read(&mut buffer).await.map_err(|error| {
                Error::new(
                    ErrorKind::Storage,
                    format!("read Direct mutation source {}: {error}", source.display()),
                )
            });
            let read = match read {
                Ok(read) => read,
                Err(error) => {
                    let _ = writer.abort().await;
                    return Err(error);
                }
            };
            if read == 0 {
                break;
            }
            read_length = read_length.saturating_add(read as u64);
            if let Err(error) = writer.write(Buffer::from(buffer[..read].to_vec())).await {
                let _ = writer.abort().await;
                return Err(Error::storage(error));
            }
        }
        if read_length != source_length {
            let _ = writer.abort().await;
            return Err(Error::new(
                ErrorKind::Storage,
                format!(
                    "Direct mutation source {} changed while being read: expected {source_length} bytes, read {read_length}",
                    source.display()
                ),
            ));
        }

        match writer.close().await {
            Ok(metadata) => Ok(DirectMutationOutcome::Applied {
                accepted: Some(DirectObjectPin::from_metadata(&metadata)),
            }),
            Err(error) => self.classify(key, error).await,
        }
    }

    /// Conditionally delete one whole object.
    pub async fn delete(
        &self,
        key: &DirectKey,
        precondition: DirectPrecondition,
    ) -> Result<DirectMutationOutcome> {
        precondition.validate()?;
        let DirectPrecondition::Etag(etag) = precondition else {
            return Err(Error::new(
                ErrorKind::Invalid,
                "Direct delete requires the observed object ETag",
            ));
        };
        if !self.capability().delete {
            return Ok(DirectMutationOutcome::Unsupported {
                message: "storage does not advertise conditional delete".to_owned(),
            });
        }
        match self
            .operator
            .delete_with(key.as_str())
            .if_match(&etag)
            .await
        {
            Ok(()) => Ok(DirectMutationOutcome::Applied { accepted: None }),
            Err(error) => self.classify(key, error).await,
        }
    }

    async fn classify(
        &self,
        key: &DirectKey,
        error: opendal::Error,
    ) -> Result<DirectMutationOutcome> {
        let observed = self.observe(key).await;
        match error.kind() {
            opendal::ErrorKind::ConditionNotMatch
            | opendal::ErrorKind::Conflict
            | opendal::ErrorKind::NotFound => Ok(DirectMutationOutcome::Conflict { observed }),
            opendal::ErrorKind::Unsupported => Ok(DirectMutationOutcome::Unsupported {
                message: error.to_string(),
            }),
            opendal::ErrorKind::ConfigInvalid
            | opendal::ErrorKind::PermissionDenied
            | opendal::ErrorKind::IsADirectory
            | opendal::ErrorKind::NotADirectory
            | opendal::ErrorKind::AlreadyExists
            | opendal::ErrorKind::IsSameFile => Err(Error::storage(error)),
            _ => Ok(DirectMutationOutcome::Unknown {
                observed,
                message: error.to_string(),
            }),
        }
    }

    async fn observe(&self, key: &DirectKey) -> Option<DirectObjectPin> {
        match self.operator.stat(key.as_str()).await {
            Ok(metadata) if metadata.mode() != opendal::EntryMode::DIR => {
                Some(DirectObjectPin::from_metadata(&metadata))
            }
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::direct::DirectNamespace;
    use crate::support::TestBackend;
    use opendal::Operator;
    use opendal::services::Memory;
    use std::io::Write;

    fn mutation(operator: Operator) -> DirectMutation {
        DirectMutation::open(DirectNamespace::unchecked(operator)).unwrap()
    }

    fn source(bytes: &[u8]) -> tempfile::NamedTempFile {
        let mut source = tempfile::NamedTempFile::new().unwrap();
        source.write_all(bytes).unwrap();
        source.flush().unwrap();
        source
    }

    async fn seed(operator: &Operator, key: &str, bytes: &[u8]) -> String {
        operator.write(key, bytes.to_vec()).await.unwrap();
        operator.stat(key).await.unwrap().etag().unwrap().to_owned()
    }

    #[tokio::test]
    async fn conditional_create_records_the_response_pin() {
        let backend = TestBackend::default();
        let operator = backend.operator();
        let mutation = mutation(operator.clone());
        let input = source(b"created");
        let key = DirectKey::from_storage_key("objects/new").unwrap();

        let outcome = mutation
            .publish_file(&key, input.path(), DirectPrecondition::Absent)
            .await
            .unwrap();
        let DirectMutationOutcome::Applied {
            accepted: Some(accepted),
        } = outcome
        else {
            panic!("conditional create did not apply: {outcome:?}");
        };
        assert_eq!(accepted.size(), 7);
        assert!(accepted.etag().is_some());
        assert_eq!(
            operator.read("objects/new").await.unwrap().to_vec(),
            b"created"
        );
    }

    #[tokio::test]
    async fn etag_conflict_retains_remote_bytes() {
        let backend = TestBackend::default();
        let operator = backend.operator();
        let mutation = mutation(operator.clone());
        let _ = seed(&operator, "objects/file", b"remote").await;
        let input = source(b"local");
        let key = DirectKey::from_storage_key("objects/file").unwrap();

        let outcome = mutation
            .publish_file(
                &key,
                input.path(),
                DirectPrecondition::Etag("\"stale\"".to_owned()),
            )
            .await
            .unwrap();
        let DirectMutationOutcome::Conflict {
            observed: Some(observed),
        } = outcome
        else {
            panic!("stale replacement was not a conflict: {outcome:?}");
        };
        assert_eq!(observed.size(), 6);
        assert_eq!(
            operator.read("objects/file").await.unwrap().to_vec(),
            b"remote"
        );
        assert_eq!(
            std::fs::read(input.path()).unwrap(),
            b"local",
            "source bytes must remain unchanged"
        );
    }

    #[tokio::test]
    async fn lost_write_response_stays_unknown() {
        let backend = TestBackend::default();
        let operator = backend.operator();
        let mutation = mutation(operator.clone());
        let etag = seed(&operator, "objects/file", b"remote").await;
        let input = source(b"local");
        let key = DirectKey::from_storage_key("objects/file").unwrap();
        backend.lose_next_write_response();

        let outcome = mutation
            .publish_file(&key, input.path(), DirectPrecondition::Etag(etag))
            .await
            .unwrap();
        let DirectMutationOutcome::Unknown {
            observed: Some(observed),
            ..
        } = outcome
        else {
            panic!("lost response was not retained as Unknown: {outcome:?}");
        };
        assert_eq!(observed.size(), 5);
        assert_eq!(
            operator.read("objects/file").await.unwrap().to_vec(),
            b"local",
            "the request may have applied, so Unknown must not be downgraded"
        );
    }

    #[tokio::test]
    async fn conditional_delete_does_not_remove_a_replacement() {
        let backend = TestBackend::default();
        let operator = backend.operator();
        let mutation = mutation(operator.clone());
        let old = seed(&operator, "objects/file", b"old").await;
        let new = seed(&operator, "objects/file", b"new").await;
        let key = DirectKey::from_storage_key("objects/file").unwrap();

        let outcome = mutation
            .delete(&key, DirectPrecondition::Etag(old))
            .await
            .unwrap();
        assert!(matches!(
            outcome,
            DirectMutationOutcome::Conflict { observed: Some(_) }
        ));
        assert_eq!(
            operator.read("objects/file").await.unwrap().to_vec(),
            b"new"
        );

        let outcome = mutation
            .delete(&key, DirectPrecondition::Etag(new))
            .await
            .unwrap();
        assert_eq!(outcome, DirectMutationOutcome::Applied { accepted: None });
        assert!(operator.stat("objects/file").await.is_err());
    }

    #[tokio::test]
    async fn unsupported_primitives_are_rejected_before_effect() {
        let operator = Operator::new(Memory::default()).unwrap();
        let mutation = mutation(operator.clone());
        let input = source(b"local");
        let key = DirectKey::from_storage_key("objects/file").unwrap();

        let outcome = mutation
            .publish_file(
                &key,
                input.path(),
                DirectPrecondition::Etag("\"etag\"".to_owned()),
            )
            .await
            .unwrap();
        assert!(matches!(outcome, DirectMutationOutcome::Unsupported { .. }));
        assert!(operator.stat("objects/file").await.is_err());
    }
}
