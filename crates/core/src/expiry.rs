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

use crate::compatibility::{
    EXPIRY_HASH_CONTEXT, EXPIRY_RECEIPT_MAGIC, EXPIRY_RECORD_MAGIC, EXPIRY_REFUSAL_MAGIC,
    EXPIRY_REQUEST_MAGIC,
};
use crate::namespace::{decode, encode};
use crate::{Error, NodeId, Result, Revision};
use std::collections::BTreeSet;

/// Maximum number of candidates and protected identities in one request.
pub const EXPIRY_SET_MAX: usize = 4096;

const REVISION_KEY_TAG: u8 = 0;
const OPERATION_KEY_TAG: u8 = 1;

/// Stable identity for one explicit expiry operation.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ExpiryId([u8; 16]);

impl ExpiryId {
    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    pub const fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }

    pub fn generate() -> Self {
        Self(*uuid::Uuid::new_v4().as_bytes())
    }
}

/// A canonical, bounded request to make selected retained revisions
/// unreadable. Local evidence semantics remain owned by the caller.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExpiryRequest {
    volume: NodeId,
    id: ExpiryId,
    expected_latest: Revision,
    candidates: Vec<Revision>,
    protected: Vec<Revision>,
    evidence_digest: [u8; 32],
}

impl ExpiryRequest {
    pub fn new(
        volume: NodeId,
        id: ExpiryId,
        expected_latest: Revision,
        mut candidates: Vec<Revision>,
        mut protected: Vec<Revision>,
        evidence_digest: [u8; 32],
    ) -> Result<Self> {
        if candidates.is_empty() {
            return Err(Error::invalid(
                "expire revisions",
                "at least one candidate is required",
            ));
        }
        if candidates.len() > EXPIRY_SET_MAX || protected.len() > EXPIRY_SET_MAX {
            return Err(Error::unsupported(
                "expire revisions",
                format!("candidate and protected sets are limited to {EXPIRY_SET_MAX} revisions"),
            ));
        }
        candidates.sort_unstable();
        candidates.dedup();
        protected.sort_unstable();
        protected.dedup();
        Ok(Self {
            volume,
            id,
            expected_latest,
            candidates,
            protected,
            evidence_digest,
        })
    }

    pub const fn volume(&self) -> NodeId {
        self.volume
    }

    pub const fn id(&self) -> ExpiryId {
        self.id
    }

    pub const fn expected_latest(&self) -> Revision {
        self.expected_latest
    }

    pub fn candidates(&self) -> &[Revision] {
        &self.candidates
    }

    pub fn protected(&self) -> &[Revision] {
        &self.protected
    }

    pub const fn evidence_digest(&self) -> [u8; 32] {
        self.evidence_digest
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        encode(&(
            EXPIRY_REQUEST_MAGIC,
            *self.volume.as_bytes(),
            *self.id.as_bytes(),
            self.expected_latest.to_bytes(),
            self.candidates
                .iter()
                .map(|revision| revision.to_bytes())
                .collect::<Vec<_>>(),
            self.protected
                .iter()
                .map(|revision| revision.to_bytes())
                .collect::<Vec<_>>(),
            self.evidence_digest,
        ))
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() > 16 * 1024 * 1024 {
            return Err(Error::unsupported(
                "decode expiry request",
                "request exceeds 16 MiB",
            ));
        }
        if bytes.len() < EXPIRY_REQUEST_MAGIC.len() {
            return Err(Error::corrupt(
                "decode expiry request",
                "request is truncated",
            ));
        }
        if bytes[..EXPIRY_REQUEST_MAGIC.len()] != EXPIRY_REQUEST_MAGIC {
            return Err(Error::unsupported(
                "decode expiry request",
                "unknown expiry request profile",
            ));
        }
        let (magic, volume, id, expected_latest, candidates, protected, evidence_digest): (
            [u8; 8],
            [u8; 16],
            [u8; 16],
            [u8; 24],
            Vec<[u8; 24]>,
            Vec<[u8; 24]>,
            [u8; 32],
        ) = decode(bytes)?;
        if magic != EXPIRY_REQUEST_MAGIC {
            return Err(Error::unsupported(
                "decode expiry request",
                "unknown expiry request profile",
            ));
        }
        let request = Self::new(
            NodeId::from_bytes(volume),
            ExpiryId::from_bytes(id),
            Revision::from_bytes(expected_latest),
            candidates.into_iter().map(Revision::from_bytes).collect(),
            protected.into_iter().map(Revision::from_bytes).collect(),
            evidence_digest,
        )?;
        if request.to_bytes()?.as_slice() != bytes {
            return Err(Error::invalid(
                "decode expiry request",
                "request encoding is not canonical",
            ));
        }
        Ok(request)
    }

    pub fn digest(&self) -> Result<[u8; 32]> {
        Ok(blake3::derive_key(EXPIRY_HASH_CONTEXT, &self.to_bytes()?))
    }

    pub(crate) fn validate_volume(&self, volume: NodeId) -> Result<()> {
        if self.volume != volume {
            return Err(Error::invalid(
                "expire revisions",
                "request belongs to another volume",
            ));
        }
        Ok(())
    }

    pub(crate) fn receipt(&self, marker: ExpiryMarker) -> ExpiryReceipt {
        ExpiryReceipt {
            id: self.id,
            request_digest: marker.request_digest,
            evidence_digest: marker.evidence_digest,
            before: marker.before,
            after: marker.after,
            expired: self.candidates.clone(),
        }
    }

    pub(crate) fn validate_marker(&self, marker: &ExpiryMarker) -> Result<()> {
        let digest = self.digest()?;
        if marker.operation_id != self.id || marker.request_digest != digest {
            return Err(Error::invalid(
                "expire revisions",
                "operation identity was reused with a different request",
            ));
        }
        if marker.evidence_digest != self.evidence_digest
            || marker.before != self.expected_latest
            || marker.after <= marker.before
        {
            return Err(Error::corrupt(
                "expire revisions",
                "stored operation record disagrees with the exact request",
            ));
        }
        Ok(())
    }
}

/// Authenticated state copied into each expired revision and its operation
/// record.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExpiryMarker {
    pub operation_id: ExpiryId,
    pub request_digest: [u8; 32],
    pub evidence_digest: [u8; 32],
    pub before: Revision,
    pub after: Revision,
}

impl ExpiryMarker {
    pub(crate) fn encode(self) -> Result<Vec<u8>> {
        encode(&(
            EXPIRY_RECORD_MAGIC,
            *self.operation_id.as_bytes(),
            self.request_digest,
            self.evidence_digest,
            self.before.to_bytes(),
            self.after.to_bytes(),
        ))
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < EXPIRY_RECORD_MAGIC.len() {
            return Err(Error::corrupt(
                "decode expiry record",
                "record is truncated",
            ));
        }
        if bytes[..EXPIRY_RECORD_MAGIC.len()] != EXPIRY_RECORD_MAGIC {
            return Err(Error::unsupported(
                "decode expiry record",
                "unknown expiry record profile",
            ));
        }
        let (magic, id, request_digest, evidence_digest, before, after): (
            [u8; 7],
            [u8; 16],
            [u8; 32],
            [u8; 32],
            [u8; 24],
            [u8; 24],
        ) = decode(bytes)?;
        let before = Revision::from_bytes(before);
        let after = Revision::from_bytes(after);
        if magic != EXPIRY_RECORD_MAGIC || after <= before {
            return Err(Error::corrupt(
                "decode expiry record",
                "expiry record ordering is invalid",
            ));
        }
        Ok(Self {
            operation_id: ExpiryId::from_bytes(id),
            request_digest,
            evidence_digest,
            before,
            after,
        })
    }
}

pub(crate) fn revision_key(revision: Revision) -> Vec<u8> {
    let mut key = Vec::with_capacity(25);
    key.push(REVISION_KEY_TAG);
    key.extend(revision.to_bytes());
    key
}

pub(crate) fn operation_key(id: ExpiryId) -> Vec<u8> {
    let mut key = Vec::with_capacity(17);
    key.push(OPERATION_KEY_TAG);
    key.extend(id.as_bytes());
    key
}

pub(crate) fn parse_revision_key(key: &[u8]) -> Result<Revision> {
    if key.len() != 25 || key[0] != REVISION_KEY_TAG {
        return Err(Error::corrupt(
            "decode expiry key",
            "revision key profile is invalid",
        ));
    }
    Ok(Revision::from_bytes(
        key[1..].try_into().expect("validated revision key"),
    ))
}

pub(crate) fn parse_operation_key(key: &[u8]) -> Result<ExpiryId> {
    if key.len() != 17 || key[0] != OPERATION_KEY_TAG {
        return Err(Error::corrupt(
            "decode expiry key",
            "operation key profile is invalid",
        ));
    }
    Ok(ExpiryId::from_bytes(
        key[1..].try_into().expect("validated operation key"),
    ))
}

/// One successfully published logical expiry.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExpiryReceipt {
    pub id: ExpiryId,
    pub request_digest: [u8; 32],
    pub evidence_digest: [u8; 32],
    pub before: Revision,
    pub after: Revision,
    pub expired: Vec<Revision>,
}

impl ExpiryReceipt {
    pub(crate) fn validate(&self) -> Result<()> {
        if self.after <= self.before
            || self.expired.is_empty()
            || self.expired.windows(2).any(|window| window[0] >= window[1])
        {
            return Err(Error::corrupt(
                "decode expiry receipt",
                "receipt ordering or candidate set is invalid",
            ));
        }
        Ok(())
    }

    pub(crate) fn encode(&self) -> Result<Vec<u8>> {
        encode(&(
            EXPIRY_RECEIPT_MAGIC,
            *self.id.as_bytes(),
            self.request_digest,
            self.evidence_digest,
            self.before.to_bytes(),
            self.after.to_bytes(),
            self.expired
                .iter()
                .map(|revision| revision.to_bytes())
                .collect::<Vec<_>>(),
        ))
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < EXPIRY_RECEIPT_MAGIC.len() {
            return Err(Error::corrupt(
                "decode expiry receipt",
                "receipt is truncated",
            ));
        }
        if bytes[..EXPIRY_RECEIPT_MAGIC.len()] != EXPIRY_RECEIPT_MAGIC {
            return Err(Error::unsupported(
                "decode expiry receipt",
                "unknown expiry receipt profile",
            ));
        }
        let (magic, id, request_digest, evidence_digest, before, after, expired): (
            [u8; 8],
            [u8; 16],
            [u8; 32],
            [u8; 32],
            [u8; 24],
            [u8; 24],
            Vec<[u8; 24]>,
        ) = decode(bytes)?;
        if magic != EXPIRY_RECEIPT_MAGIC {
            return Err(Error::unsupported(
                "decode expiry receipt",
                "unknown expiry receipt profile",
            ));
        }
        let receipt = Self {
            id: ExpiryId::from_bytes(id),
            request_digest,
            evidence_digest,
            before: Revision::from_bytes(before),
            after: Revision::from_bytes(after),
            expired: expired.into_iter().map(Revision::from_bytes).collect(),
        };
        receipt.validate()?;
        Ok(receipt)
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum ExpiryConflictKind {
    Current,
    Missing,
    AlreadyExpired,
    Protected,
    ProtectedMissing,
    ProtectedExpired,
}

impl ExpiryConflictKind {
    pub(crate) const fn tag(self) -> u8 {
        match self {
            Self::Current => 0,
            Self::Missing => 1,
            Self::AlreadyExpired => 2,
            Self::Protected => 3,
            Self::ProtectedMissing => 4,
            Self::ProtectedExpired => 5,
        }
    }

    pub(crate) fn from_tag(tag: u8) -> Result<Self> {
        Ok(match tag {
            0 => Self::Current,
            1 => Self::Missing,
            2 => Self::AlreadyExpired,
            3 => Self::Protected,
            4 => Self::ProtectedMissing,
            5 => Self::ProtectedExpired,
            _ => {
                return Err(Error::invalid(
                    "decode expiry refusal",
                    "unknown expiry conflict kind",
                ));
            }
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ExpiryConflict {
    pub revision: Revision,
    pub kind: ExpiryConflictKind,
}

/// Authority-side reasons why no candidate was expired. The caller maps
/// revision identities back to its own local evidence sources and diagnostics.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExpiryRefusal {
    pub current_latest: Revision,
    pub conflicts: Vec<ExpiryConflict>,
}

impl ExpiryRefusal {
    pub(crate) fn new(current_latest: Revision) -> Self {
        Self {
            current_latest,
            conflicts: Vec::new(),
        }
    }

    pub(crate) fn push(&mut self, revision: Revision, kind: ExpiryConflictKind) {
        let conflict = ExpiryConflict { revision, kind };
        if let Err(index) = self.conflicts.binary_search(&conflict) {
            self.conflicts.insert(index, conflict);
        }
    }

    pub fn is_empty(&self) -> bool {
        self.conflicts.is_empty()
    }

    pub(crate) fn encode(&self) -> Result<Vec<u8>> {
        encode(&(
            EXPIRY_REFUSAL_MAGIC,
            self.current_latest.to_bytes(),
            self.conflicts
                .iter()
                .map(|conflict| (conflict.kind.tag(), conflict.revision.to_bytes()))
                .collect::<Vec<_>>(),
        ))
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < EXPIRY_REFUSAL_MAGIC.len() {
            return Err(Error::corrupt(
                "decode expiry refusal",
                "refusal is truncated",
            ));
        }
        if bytes[..EXPIRY_REFUSAL_MAGIC.len()] != EXPIRY_REFUSAL_MAGIC {
            return Err(Error::unsupported(
                "decode expiry refusal",
                "unknown expiry refusal profile",
            ));
        }
        let (magic, current_latest, conflicts): ([u8; 8], [u8; 24], Vec<(u8, [u8; 24])>) =
            decode(bytes)?;
        if magic != EXPIRY_REFUSAL_MAGIC {
            return Err(Error::unsupported(
                "decode expiry refusal",
                "unknown expiry refusal profile",
            ));
        }
        let mut conflicts = conflicts
            .into_iter()
            .map(|(kind, revision)| {
                Ok(ExpiryConflict {
                    revision: Revision::from_bytes(revision),
                    kind: ExpiryConflictKind::from_tag(kind)?,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        conflicts.sort_unstable();
        conflicts.dedup();
        Ok(Self {
            current_latest: Revision::from_bytes(current_latest),
            conflicts,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExpiryOutcome {
    Committed(ExpiryReceipt),
    Refused(ExpiryRefusal),
    Conflict,
    Retryable,
    Unknown(ExpiryId),
}

impl ExpiryOutcome {
    pub(crate) fn encode(&self) -> Result<Vec<u8>> {
        Ok(match self {
            Self::Committed(receipt) => {
                let mut bytes = vec![0];
                bytes.extend(receipt.encode()?);
                bytes
            }
            Self::Refused(refusal) => {
                let mut bytes = vec![1];
                bytes.extend(refusal.encode()?);
                bytes
            }
            Self::Conflict => vec![2],
            Self::Retryable => vec![3],
            Self::Unknown(id) => {
                let mut bytes = vec![4];
                bytes.extend(id.as_bytes());
                bytes
            }
        })
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self> {
        let Some((&tag, body)) = bytes.split_first() else {
            return Err(Error::invalid("decode expiry outcome", "outcome is empty"));
        };
        Ok(match tag {
            0 => Self::Committed(ExpiryReceipt::decode(body)?),
            1 => Self::Refused(ExpiryRefusal::decode(body)?),
            2 if body.is_empty() => Self::Conflict,
            3 if body.is_empty() => Self::Retryable,
            4 if body.len() == 16 => Self::Unknown(ExpiryId::from_bytes(
                body.try_into().expect("validated expiry identity"),
            )),
            _ => {
                return Err(Error::invalid(
                    "decode expiry outcome",
                    "invalid expiry outcome profile",
                ));
            }
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RevisionState {
    Readable,
    Expired(ExpiryMarker),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HistoryEntry {
    pub revision: Revision,
    pub state: RevisionState,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HistoryPage {
    pub entries: Vec<HistoryEntry>,
    pub next: Option<Revision>,
}

impl HistoryPage {
    pub(crate) fn readable(revisions: Vec<Revision>, next: Option<Revision>) -> Self {
        Self {
            entries: revisions
                .into_iter()
                .map(|revision| HistoryEntry {
                    revision,
                    state: RevisionState::Readable,
                })
                .collect(),
            next,
        }
    }

    pub(crate) fn validate(&self) -> Result<()> {
        if self
            .entries
            .windows(2)
            .any(|window| window[0].revision >= window[1].revision)
        {
            return Err(Error::corrupt(
                "read history",
                "history page is not strictly ordered",
            ));
        }
        let seen = self
            .entries
            .iter()
            .map(|entry| entry.revision)
            .collect::<BTreeSet<_>>();
        if seen.len() != self.entries.len()
            || self
                .next
                .is_some_and(|next| self.entries.last().map(|entry| entry.revision) != Some(next))
        {
            return Err(Error::corrupt(
                "read history",
                "history continuation is invalid",
            ));
        }
        for entry in &self.entries {
            if let RevisionState::Expired(marker) = entry.state
                && (marker.before <= entry.revision || marker.after <= entry.revision)
            {
                return Err(Error::corrupt(
                    "read history",
                    "expiry marker disagrees with its revision",
                ));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ErrorKind;

    fn revision(sequence: u64) -> Revision {
        Revision::new(sequence)
    }

    #[test]
    fn expiry_request_round_trips_and_recomputes_digest() {
        let volume = NodeId::generate();
        let before = revision(7);
        let request = ExpiryRequest::new(
            volume,
            ExpiryId::generate(),
            before,
            vec![revision(2), revision(1)],
            vec![revision(3)],
            [9; 32],
        )
        .unwrap();
        let bytes = request.to_bytes().unwrap();
        let decoded = ExpiryRequest::from_bytes(&bytes).unwrap();
        assert_eq!(decoded, request);
        assert_eq!(decoded.digest().unwrap(), request.digest().unwrap());
    }

    #[test]
    fn expiry_request_rejects_noncanonical_and_empty_sets() {
        assert_eq!(
            ExpiryRequest::new(
                NodeId::generate(),
                ExpiryId::generate(),
                revision(1),
                vec![],
                vec![],
                [0; 32],
            )
            .unwrap_err()
            .kind(),
            ErrorKind::Invalid
        );
        let volume = NodeId::generate();
        let request = ExpiryRequest::new(
            volume,
            ExpiryId::generate(),
            revision(3),
            vec![revision(2), revision(1)],
            vec![],
            [0; 32],
        )
        .unwrap();
        let bytes = request.to_bytes().unwrap();
        let (magic, volume, id, expected, mut candidates, protected, evidence): (
            [u8; 8],
            [u8; 16],
            [u8; 16],
            [u8; 24],
            Vec<[u8; 24]>,
            Vec<[u8; 24]>,
            [u8; 32],
        ) = borsh::from_slice(&bytes).unwrap();
        candidates.reverse();
        let noncanonical =
            borsh::to_vec(&(magic, volume, id, expected, candidates, protected, evidence)).unwrap();
        assert_eq!(
            ExpiryRequest::from_bytes(&noncanonical).unwrap_err().kind(),
            ErrorKind::Invalid
        );
    }

    #[test]
    fn expiry_outcomes_round_trip() {
        let marker = ExpiryMarker {
            operation_id: ExpiryId::generate(),
            request_digest: [1; 32],
            evidence_digest: [2; 32],
            before: revision(3),
            after: revision(4),
        };
        let receipt = ExpiryReceipt {
            id: marker.operation_id,
            request_digest: marker.request_digest,
            evidence_digest: marker.evidence_digest,
            before: marker.before,
            after: marker.after,
            expired: vec![revision(1), revision(2)],
        };
        let outcome = ExpiryOutcome::Committed(receipt);
        assert_eq!(
            ExpiryOutcome::decode(&outcome.encode().unwrap()).unwrap(),
            outcome
        );
        let refusal = ExpiryRefusal {
            current_latest: revision(4),
            conflicts: vec![ExpiryConflict {
                revision: revision(1),
                kind: ExpiryConflictKind::Protected,
            }],
        };
        let outcome = ExpiryOutcome::Refused(refusal);
        assert_eq!(
            ExpiryOutcome::decode(&outcome.encode().unwrap()).unwrap(),
            outcome
        );
    }

    #[test]
    fn expiry_receipt_rejects_invalid_order() {
        let receipt = ExpiryReceipt {
            id: ExpiryId::generate(),
            request_digest: [1; 32],
            evidence_digest: [2; 32],
            before: revision(5),
            after: revision(5),
            expired: vec![revision(1)],
        };
        assert_eq!(
            ExpiryReceipt::decode(&receipt.encode().unwrap())
                .unwrap_err()
                .kind(),
            ErrorKind::Corrupt
        );
    }

    #[test]
    fn expiry_marker_rejects_invalid_ordering() {
        let bytes = encode(&(
            EXPIRY_RECORD_MAGIC,
            [1; 16],
            [2; 32],
            [3; 32],
            revision(5).to_bytes(),
            revision(5).to_bytes(),
        ))
        .unwrap();
        assert_eq!(
            ExpiryMarker::decode(&bytes).unwrap_err().kind(),
            ErrorKind::Corrupt
        );
    }
}
