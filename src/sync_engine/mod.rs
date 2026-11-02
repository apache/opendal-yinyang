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

//! Shared local reconciliation engine for Managed and Direct Sync.

mod adapter;
mod backup;
mod direct;
mod engine;
mod error;
mod evidence;
mod local;
mod managed;
mod managed_intent;
mod store;
mod types;

use std::collections::BTreeSet;
use std::path::Path;

pub use adapter::{
    EntryKind, FrozenRemoteRequest, PreparedRemoteRequest, RemoteAdapter, RemoteBaseline,
    RemoteCapabilities, RemoteCursor, RemoteEntry, RemoteFailure, RemoteObservation,
    RemoteOperation, RemoteOutcome, RemotePin, RemoteReceipt, RemoteRequest, SYNC_SCHEMA,
    SyncModel, hex,
};
pub use direct::{DirectAdapter, validate_namespace_entries};
pub use engine::{Resolution, RunOutcome, SyncEngine};
pub use error::{Error, ErrorKind, Result};
pub use evidence::{
    BackupInspection, BindingState, ConflictSideStatus, EngineArtifact, EngineConflict,
    EngineInspection, LocalScanState, StatusDocument,
};
pub use local::check_root_state;
pub use managed::ManagedAdapter;
pub use types::{ContentHash, Identity, LocalKind, LocalNode, LocalSnapshot, LocalStat};

/// Inspect a live engine state while validating its bound root.
pub fn inspect_state(state: &Path) -> Result<EngineInspection> {
    let (store, binding, _) = local::LocalTree::inspect(state)?;
    inspect_store(&store, &binding, state)
}

/// Inspect the engine database without opening the recorded live root.
/// Callers validating a package must validate its manifest first.
pub fn inspect_protection(state: &Path) -> Result<EngineInspection> {
    let state = std::fs::canonicalize(state)
        .map_err(|error| Error::io("canonicalize sync state", state, error))?;
    let store = store::Store::open_read_only(&state.join("sync-engine.db"))?;
    let binding = store.binding()?;
    inspect_store(&store, &binding, &state)
}

pub fn inspect_backup_package(backup: &Path) -> Result<BackupInspection> {
    backup::inspect(backup)
}

fn inspect_store(
    store: &store::Store,
    binding: &store::Binding,
    state: &Path,
) -> Result<EngineInspection> {
    let baseline_revisions = store
        .baseline()?
        .map(|baseline| {
            baseline
                .remote
                .values()
                .filter_map(|entry| entry.pin.revision())
                .map(|revision| adapter::hex(&revision))
                .collect::<BTreeSet<_>>()
        })
        .unwrap_or_default()
        .into_iter()
        .collect();
    let intent_receipt_revisions = store
        .intents()?
        .into_iter()
        .filter_map(|intent| intent.receipt_revision)
        .map(|revision| adapter::hex(&revision))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let pending_installs = store
        .installs()?
        .iter()
        .filter(|install| {
            matches!(
                install.state,
                store::InstallState::Prepared | store::InstallState::Dispatched
            )
        })
        .count() as u64;
    let artifacts = store
        .artifacts()?
        .into_iter()
        .map(|artifact| EngineArtifact {
            exportable: artifact.phase != store::ArtifactPhase::Failed
                && state.join("artifacts").join(&artifact.name).is_file(),
            id: artifact.id,
            kind: artifact.kind.as_str().to_owned(),
            phase: artifact.phase.as_str().to_owned(),
            release_event: artifact.release_event.as_str().to_owned(),
            length: artifact.length,
            error: artifact.error,
        })
        .collect();
    let conflicts = store
        .conflicts()?
        .into_iter()
        .map(|conflict| EngineConflict {
            id: conflict.id,
            path: conflict.path,
            remote_revision: conflict
                .remote_revision
                .map(|revision| adapter::hex(&revision)),
            error: conflict.error,
        })
        .collect();
    Ok(EngineInspection {
        model: binding.model,
        namespace: binding.namespace.clone(),
        root: binding.root.display().to_string(),
        cursor: store.cursor()?.map(|cursor| adapter::hex(&cursor.bytes)),
        baseline_revisions,
        intent_receipt_revisions,
        pending_installs,
        artifacts,
        conflicts,
    })
}
