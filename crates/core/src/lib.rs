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

//! Indexed transactional filesystem and authenticated immutable content.

mod authority;
pub mod data;
pub use authority::Authority;
mod error;
mod identity;
mod index;
pub mod namespace;
pub mod object;
pub mod service;
mod snapshot;
pub mod transaction;

pub use data::{ContentDescriptor, ContentId, DataStore, PreparedContent};
pub use error::{Error, ErrorKind, Result};
pub use identity::{CommitId, NodeId};
pub use namespace::{DirectoryEntry, Link, Node, NodeKind};
pub use object::{
    BackendProfile, ObjectFs as Fs, Outcome as CommitOutcome, Receipt, Revision, Snapshot,
};
pub use transaction::{Planner, Transaction};

#[cfg(test)]
#[path = "../tests/support/mod.rs"]
mod support;
