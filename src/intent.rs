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

//! Shared durable intent rows and state transitions.

use rusqlite::{Connection, OptionalExtension, params, types::Type};
use yinyang_core::{CommitId, NodeId, Receipt, Revision, Transaction};

pub(crate) const TABLE: &str = "intents";

pub(crate) const COLUMNS: &[&str] = &[
    "id",
    "request_id",
    "request_digest",
    "request",
    "commit_id",
    "state",
    "receipt_revision",
    "receipt_ordinal",
    "failure",
    "metadata",
];

pub(crate) const CREATE_TABLE: &str = "
    CREATE TABLE intents(
      id BLOB PRIMARY KEY,
      request_id BLOB NOT NULL,
      request_digest BLOB NOT NULL,
      request BLOB NOT NULL,
      commit_id BLOB,
      state INTEGER NOT NULL,
      receipt_revision BLOB,
      receipt_ordinal INTEGER,
      failure BLOB,
      metadata BLOB NOT NULL
    );";

const SELECT_ONE: &str = "
    SELECT id,request_id,request_digest,request,commit_id,state,
           receipt_revision,receipt_ordinal,failure,metadata
    FROM intents WHERE id=?1";

const SELECT_ALL: &str = "
    SELECT id,request_id,request_digest,request,commit_id,state,
           receipt_revision,receipt_ordinal,failure,metadata
    FROM intents ORDER BY id";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum State {
    Prepared,
    Unknown,
    Committed,
    Conflict,
    Retryable,
    Failed,
}

impl State {
    pub const fn code(self) -> u8 {
        match self {
            Self::Prepared => 0,
            Self::Unknown => 1,
            Self::Committed => 2,
            Self::Conflict => 3,
            Self::Retryable => 4,
            Self::Failed => 5,
        }
    }

    pub const fn from_code(value: u8) -> Option<Self> {
        match value {
            0 => Some(Self::Prepared),
            1 => Some(Self::Unknown),
            2 => Some(Self::Committed),
            3 => Some(Self::Conflict),
            4 => Some(Self::Retryable),
            5 => Some(Self::Failed),
            _ => None,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct Record {
    pub id: Vec<u8>,
    pub request_id: Vec<u8>,
    pub request_digest: Vec<u8>,
    pub request: Vec<u8>,
    pub commit_id: Option<Vec<u8>>,
    pub state: State,
    pub receipt_revision: Option<Vec<u8>>,
    pub receipt_ordinal: Option<i64>,
    pub failure: Option<Vec<u8>>,
    pub metadata: Vec<u8>,
}

impl Record {
    fn from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Self> {
        let state = row.get::<_, u8>(5)?;
        let state = State::from_code(state).ok_or_else(|| {
            rusqlite::Error::FromSqlConversionFailure(
                5,
                Type::Integer,
                Box::new(InvalidState(state)),
            )
        })?;
        Ok(Self {
            id: row.get(0)?,
            request_id: row.get(1)?,
            request_digest: row.get(2)?,
            request: row.get(3)?,
            commit_id: row.get(4)?,
            state,
            receipt_revision: row.get(6)?,
            receipt_ordinal: row.get(7)?,
            failure: row.get(8)?,
            metadata: row.get(9)?,
        })
    }
}

#[derive(Debug)]
struct InvalidState(u8);

impl std::fmt::Display for InvalidState {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "invalid intent state code {}", self.0)
    }
}

impl std::error::Error for InvalidState {}

pub(crate) fn load(
    connection: &Connection,
    id: impl AsRef<[u8]>,
) -> rusqlite::Result<Option<Record>> {
    connection
        .query_row(SELECT_ONE, params![id.as_ref()], Record::from_row)
        .optional()
}

pub(crate) fn all(connection: &Connection) -> rusqlite::Result<Vec<Record>> {
    let mut statement = connection.prepare(SELECT_ALL)?;
    let rows = statement.query_map([], Record::from_row)?;
    rows.collect()
}

pub(crate) struct Prepared<'a> {
    pub id: &'a [u8],
    pub request_id: &'a [u8],
    pub request_digest: &'a [u8],
    pub request: &'a [u8],
    pub commit_id: Option<&'a [u8]>,
    pub metadata: &'a [u8],
}

pub(crate) fn insert_prepared(
    connection: &Connection,
    prepared: Prepared<'_>,
) -> rusqlite::Result<()> {
    connection
        .execute(
            "INSERT INTO intents(
                id,request_id,request_digest,request,commit_id,state,
                receipt_revision,receipt_ordinal,failure,metadata
             ) VALUES(?1,?2,?3,?4,?5,?6,NULL,NULL,NULL,?7)",
            params![
                prepared.id,
                prepared.request_id,
                prepared.request_digest,
                prepared.request,
                prepared.commit_id,
                State::Prepared.code(),
                prepared.metadata,
            ],
        )
        .map(|_| ())
}

/// Common evidence-backed intent transitions.
///
/// `Unknown` is written before dispatch. Local uncertainty may update its
/// diagnostic but cannot move it to a less ambiguous state. Only an authority
/// outcome may resolve it; a verified committed authority result intentionally
/// wins over every stored state so concurrent exact dispatchers converge.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Transition {
    /// Persist the possibility of dispatch before calling the authority.
    ClaimDispatch,
    /// A local failure established that a non-dispatched request may retry.
    LocalRetryable,
    /// A local failure made a non-dispatched request definitively unusable.
    LocalFailed,
    /// Replace the diagnostic without resolving an already dispatched request.
    StillUnknown,
    /// Record a verified committed authority result.
    Committed,
    /// Record an authority-confirmed predicate conflict.
    Conflict,
    /// Record an authority-confirmed nonpublication that may retry.
    Retryable,
    /// Record a definitive authority result that cannot be retried.
    Failed,
}

impl Transition {
    const fn target(self) -> State {
        match self {
            Self::ClaimDispatch => State::Unknown,
            Self::LocalRetryable => State::Retryable,
            Self::LocalFailed => State::Failed,
            Self::StillUnknown => State::Unknown,
            Self::Committed => State::Committed,
            Self::Conflict => State::Conflict,
            Self::Retryable => State::Retryable,
            Self::Failed => State::Failed,
        }
    }

    const fn filter(self) -> &'static str {
        match self {
            Self::ClaimDispatch => "state IN (0,1,4)",
            Self::LocalRetryable | Self::LocalFailed => "state IN (0,4)",
            Self::StillUnknown | Self::Conflict | Self::Retryable | Self::Failed => "state=1",
            Self::Committed => "1",
        }
    }
}

enum CommitIdUpdate<'a> {
    Keep,
    Set(Option<&'a [u8]>),
}

pub(crate) struct Update<'a> {
    transition: Transition,
    commit_id: CommitIdUpdate<'a>,
    receipt_revision: Option<[u8; 24]>,
    receipt_ordinal: Option<u32>,
    failure: Option<&'a [u8]>,
}

impl<'a> Update<'a> {
    pub(crate) fn new(transition: Transition) -> Self {
        Self {
            transition,
            commit_id: CommitIdUpdate::Keep,
            receipt_revision: None,
            receipt_ordinal: None,
            failure: None,
        }
    }

    pub(crate) fn commit_id(mut self, commit_id: Option<&'a [u8]>) -> Self {
        self.commit_id = CommitIdUpdate::Set(commit_id);
        self
    }

    pub(crate) fn receipt(mut self, receipt: &Receipt) -> Self {
        self.receipt_revision = Some(receipt.cursor.revision.to_bytes());
        self.receipt_ordinal = Some(receipt.cursor.ordinal);
        self
    }

    pub(crate) fn receipt_parts(
        mut self,
        revision: Option<[u8; 24]>,
        ordinal: Option<u32>,
    ) -> Self {
        self.receipt_revision = revision;
        self.receipt_ordinal = ordinal;
        self
    }

    pub(crate) fn failure(mut self, failure: Option<&'a [u8]>) -> Self {
        self.failure = failure;
        self
    }
}

/// Apply one transition to an existing row.
///
/// The caller owns the surrounding transaction and durability barrier. A
/// `false` result means the row was absent or its state did not match the
/// transition's evidence boundary.
pub(crate) fn update(
    connection: &Connection,
    id: impl AsRef<[u8]>,
    update: Update<'_>,
) -> rusqlite::Result<bool> {
    let revision = update.receipt_revision.map(|revision| revision.to_vec());
    let ordinal = update.receipt_ordinal.map(i64::from);
    let filter = update.transition.filter();
    let changed = match update.commit_id {
        CommitIdUpdate::Keep => {
            let statement = format!(
                "UPDATE intents
                 SET state=?2,receipt_revision=?3,receipt_ordinal=?4,failure=?5
                 WHERE id=?1 AND {filter}"
            );
            connection.execute(
                &statement,
                params![
                    id.as_ref(),
                    update.transition.target().code(),
                    revision,
                    ordinal,
                    update.failure,
                ],
            )?
        }
        CommitIdUpdate::Set(commit_id) => {
            let statement = format!(
                "UPDATE intents
                 SET state=?2,commit_id=?3,receipt_revision=?4,receipt_ordinal=?5,failure=?6
                 WHERE id=?1 AND {filter}"
            );
            connection.execute(
                &statement,
                params![
                    id.as_ref(),
                    update.transition.target().code(),
                    commit_id,
                    revision,
                    ordinal,
                    update.failure,
                ],
            )?
        }
    };
    Ok(changed == 1)
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct FrozenIdentity {
    pub commit_id: CommitId,
    pub digest: [u8; 32],
}

pub(crate) enum VerifyFrozenError {
    Core(yinyang_core::Error),
    Mismatch,
}

pub(crate) fn verify_frozen(
    request: &[u8],
    filesystem: NodeId,
    expected_id: [u8; 16],
    expected_digest: [u8; 32],
) -> std::result::Result<FrozenIdentity, VerifyFrozenError> {
    let (_, commit_id, digest) =
        Transaction::verify_identity(request, filesystem).map_err(VerifyFrozenError::Core)?;
    if *commit_id.as_bytes() != expected_id || digest != expected_digest {
        return Err(VerifyFrozenError::Mismatch);
    }
    Ok(FrozenIdentity { commit_id, digest })
}

pub(crate) fn receipt(
    commit_id: [u8; 16],
    request_digest: [u8; 32],
    revision: Option<[u8; 24]>,
    ordinal: Option<i64>,
) -> std::result::Result<Option<Receipt>, String> {
    match (revision, ordinal) {
        (None, None) => Ok(None),
        (Some(revision), Some(ordinal)) => {
            let ordinal =
                u32::try_from(ordinal).map_err(|_| "invalid receipt ordinal".to_owned())?;
            Ok(Some(Receipt {
                commit_id: CommitId::from_bytes(commit_id),
                request_digest,
                cursor: yinyang_core::Cursor {
                    revision: Revision::from_bytes(revision),
                    ordinal,
                },
            }))
        }
        _ => Err("incomplete receipt fields".to_owned()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn connection() -> Connection {
        let connection = Connection::open_in_memory().unwrap();
        connection.execute_batch(CREATE_TABLE).unwrap();
        connection
    }

    fn insert(connection: &Connection, id: &[u8]) {
        let digest = [7_u8; 32];
        insert_prepared(
            connection,
            Prepared {
                id,
                request_id: id,
                request_digest: &digest,
                request: id,
                commit_id: Some(id),
                metadata: &[],
            },
        )
        .unwrap();
    }

    fn state(connection: &Connection, id: &[u8]) -> State {
        load(connection, id).unwrap().unwrap().state
    }

    #[test]
    fn local_failure_never_resolves_unknown() {
        let connection = connection();
        let id = [1_u8; 16];
        insert(&connection, &id);
        assert!(update(&connection, id, Update::new(Transition::ClaimDispatch)).unwrap());
        assert_eq!(state(&connection, &id), State::Unknown);

        assert!(
            !update(
                &connection,
                id,
                Update::new(Transition::LocalFailed).failure(Some(b"local failure")),
            )
            .unwrap()
        );
        assert_eq!(state(&connection, &id), State::Unknown);
    }

    #[test]
    fn verified_commit_dominates_a_concurrent_terminal_state() {
        let connection = connection();
        let id = [2_u8; 16];
        insert(&connection, &id);
        assert!(update(&connection, id, Update::new(Transition::ClaimDispatch)).unwrap());
        assert!(
            update(
                &connection,
                id,
                Update::new(Transition::Conflict).failure(Some(b"conflict")),
            )
            .unwrap()
        );
        assert_eq!(state(&connection, &id), State::Conflict);

        assert!(
            update(
                &connection,
                id,
                Update::new(Transition::Committed)
                    .commit_id(Some(&id))
                    .failure(None),
            )
            .unwrap()
        );
        assert_eq!(state(&connection, &id), State::Committed);
    }

    #[test]
    fn retryable_work_can_claim_dispatch_again() {
        let connection = connection();
        let id = [3_u8; 16];
        insert(&connection, &id);
        assert!(update(&connection, id, Update::new(Transition::ClaimDispatch)).unwrap());
        assert!(
            update(
                &connection,
                id,
                Update::new(Transition::Retryable).failure(Some(b"retry")),
            )
            .unwrap()
        );
        assert_eq!(state(&connection, &id), State::Retryable);
        assert!(update(&connection, id, Update::new(Transition::ClaimDispatch)).unwrap());
        assert_eq!(state(&connection, &id), State::Unknown);
    }
}
