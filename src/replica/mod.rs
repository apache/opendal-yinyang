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

//! Linux ordinary-directory installation boundary for Managed Sync.
//!
//! Namespace publication belongs to `Sync`. This module owns local root
//! binding, observation, fd-relative materialization and retained displaced
//! bytes. It never recursively deletes a local path.

pub mod directory;
mod error;
pub(super) mod linux;
pub(super) mod state;

pub use directory::{
    BaselineEvidence, BindingEvidence, BindingState, DirectorySync, EntryEvidence, EntryKind,
    EntryState, Inspection, IntentEvidence, IntentOperation, IntentState, LocalEvidence,
    LocalScanState, LocalStatus, OutcomeEvidence, OutcomeState, Presence, RemoteAvailability,
    RemoteEvidence, RemoteStatus, Resolution, RunOutcome, StatusSummary, export_conflict_local,
};
pub use error::{Error, ErrorKind, Result};
pub use linux::{
    Artifact, ArtifactKind, CapturedContent, Conflict, ContentHash, Identity, InstallOutcome,
    LocalKind, LocalNode, LocalSnapshot, Replica, ReplicaStatus,
};
