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

//! Shared durable intent table and state codec.

use yinyang_core::{Authority, CommitId, Receipt, Revision};

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

pub(crate) async fn receipt_for(
    authority: &dyn Authority,
    commit_id: CommitId,
    request_digest: [u8; 32],
) -> yinyang_core::Result<Option<Receipt>> {
    let receipt = authority.observe_latest().await?.receipt(commit_id).await?;
    if let Some(receipt) = &receipt {
        if receipt.request_digest != request_digest {
            return Err(yinyang_core::Error::new(
                yinyang_core::ErrorKind::Corrupt,
                "receipt lookup",
                "commit identity returned a different request",
            ));
        }
        if receipt.commit_id != commit_id {
            return Err(yinyang_core::Error::new(
                yinyang_core::ErrorKind::Corrupt,
                "receipt lookup",
                "receipt commit identity changed",
            ));
        }
    }
    Ok(receipt)
}
