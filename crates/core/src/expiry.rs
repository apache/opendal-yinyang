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

use crate::compatibility::{EXPIRY_REQUEST_HASH_CONTEXT, EXPIRY_REQUEST_MAGIC};
use crate::namespace::{decode, encode};
use crate::{CommitId, Error, ErrorKind, NodeId, Result, Revision};

/// Maximum number of revisions one explicit expiry request may cover.
pub const EXPIRY_CANDIDATES_MAX: usize = 256;

type ExpiryWire = (
    [u8; 8],
    [u8; 16],
    [u8; 16],
    [u8; 24],
    [u8; 32],
    Vec<[u8; 24]>,
);

/// Frozen, canonical request for expiring revision lookup in one authority.
///
/// Candidates are sorted and unique. The request id is the receipt lookup key;
/// the digest binds the authority, expected latest revision, and candidates.
#[derive(Clone, Debug)]
pub struct ExpiryRequest {
    filesystem: NodeId,
    id: CommitId,
    expected_latest: Revision,
    candidates: Vec<Revision>,
    digest: [u8; 32],
}

impl ExpiryRequest {
    pub fn new(
        filesystem: NodeId,
        id: CommitId,
        expected_latest: Revision,
        mut candidates: Vec<Revision>,
    ) -> Result<Self> {
        if candidates.is_empty() {
            return Err(Error::invalid(
                "create expiry request",
                "at least one candidate is required",
            ));
        }
        if candidates.len() > EXPIRY_CANDIDATES_MAX {
            return Err(Error::unsupported(
                "create expiry request",
                format!("more than {EXPIRY_CANDIDATES_MAX} candidates"),
            ));
        }
        candidates.sort_unstable();
        candidates.dedup();
        if candidates.binary_search(&expected_latest).is_ok() {
            return Err(Error::invalid(
                "create expiry request",
                "the current revision cannot be expired",
            ));
        }
        let mut request = Self {
            filesystem,
            id,
            expected_latest,
            candidates,
            digest: [0; 32],
        };
        request.digest = request.computed_digest()?;
        Ok(request)
    }

    pub const fn filesystem(&self) -> NodeId {
        self.filesystem
    }

    pub const fn id(&self) -> CommitId {
        self.id
    }

    pub const fn expected_latest(&self) -> Revision {
        self.expected_latest
    }

    pub fn candidates(&self) -> &[Revision] {
        &self.candidates
    }

    pub const fn digest(&self) -> [u8; 32] {
        self.digest
    }

    fn computed_digest(&self) -> Result<[u8; 32]> {
        let bytes = encode(&(
            EXPIRY_REQUEST_MAGIC,
            *self.filesystem.as_bytes(),
            self.expected_latest.to_bytes(),
            self.candidates
                .iter()
                .map(|revision| revision.to_bytes())
                .collect::<Vec<_>>(),
        ))?;
        Ok(blake3::derive_key(EXPIRY_REQUEST_HASH_CONTEXT, &bytes))
    }

    pub fn validate(&self, filesystem: NodeId) -> Result<()> {
        if self.filesystem != filesystem || self.computed_digest()? != self.digest {
            return Err(Error::invalid(
                "expiry request",
                "filesystem or canonical digest mismatch",
            ));
        }
        Ok(())
    }

    /// Persist the original request identity and exact bounded candidate set.
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        encode(&(
            EXPIRY_REQUEST_MAGIC,
            *self.filesystem.as_bytes(),
            *self.id.as_bytes(),
            self.expected_latest.to_bytes(),
            self.digest,
            self.candidates
                .iter()
                .map(|revision| revision.to_bytes())
                .collect::<Vec<_>>(),
        ))
    }

    /// Restore and authenticate one encoded expiry request without authority IO.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() > 64 * 1024 {
            return Err(Error::unsupported(
                "restore expiry request",
                "request exceeds 64 KiB",
            ));
        }
        if bytes.len() < EXPIRY_REQUEST_MAGIC.len() {
            return Err(Error::new(
                ErrorKind::Corrupt,
                "restore expiry request",
                "truncated request",
            ));
        }
        if bytes[..EXPIRY_REQUEST_MAGIC.len()] != EXPIRY_REQUEST_MAGIC {
            return Err(Error::unsupported(
                "restore expiry request",
                "unknown expiry request profile",
            ));
        }
        let (magic, filesystem, id, expected_latest, digest, candidates): ExpiryWire =
            decode(bytes)?;
        debug_assert_eq!(magic, EXPIRY_REQUEST_MAGIC);
        if candidates.is_empty() || candidates.len() > EXPIRY_CANDIDATES_MAX {
            return Err(Error::invalid(
                "restore expiry request",
                "invalid candidate count",
            ));
        }
        let candidates = candidates
            .into_iter()
            .map(Revision::from_bytes)
            .collect::<Vec<_>>();
        if candidates.windows(2).any(|window| window[0] >= window[1]) {
            return Err(Error::invalid(
                "restore expiry request",
                "candidates are not strictly ordered and unique",
            ));
        }
        let request = Self {
            filesystem: NodeId::from_bytes(filesystem),
            id: CommitId::from_bytes(id),
            expected_latest: Revision::from_bytes(expected_latest),
            candidates,
            digest,
        };
        if request
            .candidates
            .binary_search(&request.expected_latest)
            .is_ok()
            || request.computed_digest()? != request.digest
        {
            return Err(Error::invalid(
                "restore expiry request",
                "canonical digest or candidate set mismatch",
            ));
        }
        Ok(request)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn revision(value: u8) -> Revision {
        Revision::from_bytes([value; 24])
    }

    #[test]
    fn expiry_request_bytes_are_canonical_and_authenticated() {
        let authority = NodeId::from_bytes([1; 16]);
        let id = CommitId::from_bytes([2; 16]);
        let request = ExpiryRequest::new(
            authority,
            id,
            revision(3),
            vec![revision(2), revision(1), revision(1)],
        )
        .unwrap();
        assert_eq!(request.candidates(), &[revision(1), revision(2)]);
        let bytes = request.to_bytes().unwrap();
        let decoded = ExpiryRequest::from_bytes(&bytes).unwrap();
        assert_eq!(decoded.id(), id);
        assert_eq!(decoded.digest(), request.digest());

        let mut tampered = bytes.clone();
        tampered[64] ^= 1;
        assert_eq!(
            ExpiryRequest::from_bytes(&tampered).unwrap_err().kind(),
            ErrorKind::Invalid
        );
    }

    #[test]
    fn encoded_candidates_must_remain_strictly_ordered() {
        let request = ExpiryRequest::new(
            NodeId::from_bytes([1; 16]),
            CommitId::from_bytes([2; 16]),
            revision(3),
            vec![revision(1), revision(2)],
        )
        .unwrap();
        let bytes = encode(&(
            EXPIRY_REQUEST_MAGIC,
            *request.filesystem.as_bytes(),
            *request.id.as_bytes(),
            request.expected_latest.to_bytes(),
            request.digest,
            vec![revision(2).to_bytes(), revision(1).to_bytes()],
        ))
        .unwrap();
        assert_eq!(
            ExpiryRequest::from_bytes(&bytes).unwrap_err().kind(),
            ErrorKind::Invalid
        );
    }

    #[test]
    fn expiry_candidates_are_bounded() {
        let candidates = (0..=EXPIRY_CANDIDATES_MAX)
            .map(|value| {
                let mut bytes = [0; 24];
                bytes[..8].copy_from_slice(&(value as u64).to_be_bytes());
                bytes[8..].fill((value & 0xff) as u8);
                Revision::from_bytes(bytes)
            })
            .collect();
        assert_eq!(
            ExpiryRequest::new(
                NodeId::from_bytes([1; 16]),
                CommitId::from_bytes([2; 16]),
                revision(255),
                candidates,
            )
            .unwrap_err()
            .kind(),
            ErrorKind::Unsupported
        );
    }
}
