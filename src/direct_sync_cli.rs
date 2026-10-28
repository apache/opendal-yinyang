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

//! CLI adapter for storage-native Direct Sync.

use crate::sync_cli::{SyncCommand, SyncSide};
use serde_json::{Value, json};
use std::path::Path;
use std::process::ExitCode;
use yinyang::direct::DirectStorageBinding;
use yinyang::direct_sync::{
    ArtifactPhase, BindingState, ConflictPresence, DirectRemoteAdapter, DirectSync, EntryState,
    EvidenceKind, Inspection, InstallState, LocalScanState, RemoteAvailability, RemoteOperation,
    Resolution, RunOutcome, STATUS_SCHEMA, WorkState,
};
use yinyang::volume::{Access, Frontend, Model, VolumeConfig};

const EXIT_UNBOUND: u8 = 3;
const EXIT_INVALID_BINDING: u8 = 4;

pub async fn run(
    config_path: &Path,
    command: SyncCommand,
) -> Result<ExitCode, Box<dyn std::error::Error>> {
    let bytes = tokio::fs::read(config_path).await.map_err(|error| {
        format!(
            "read volume configuration {}: {error}",
            config_path.display()
        )
    })?;
    let raw: Value =
        serde_json::from_slice(&bytes).map_err(|error| format!("volume configuration: {error}"))?;
    match raw.get("model").and_then(Value::as_str) {
        Some("managed") => return crate::sync_cli::run(config_path, command).await,
        Some("direct") => {}
        Some(other) => return Err(format!("unknown volume model {other:?}").into()),
        None => return Err("volume configuration is missing model".into()),
    }

    let mut config = VolumeConfig::from_json(&bytes)?;
    resolve_staging(&mut config, config_path);
    match command {
        SyncCommand::Init { directory } => init(&config, &directory).await,
        SyncCommand::Run => reconcile(&config).await,
        SyncCommand::Status { json } => status(&config, json).await,
        SyncCommand::Export {
            artifact,
            conflict,
            side,
            output,
        } => export(&config, artifact, conflict, side, &output).await,
        SyncCommand::Resolve {
            conflict,
            keep,
            content,
        } => resolve(&config, &conflict, keep, content.as_deref()).await,
        SyncCommand::Backup { .. } | SyncCommand::Restore { .. } => Err(
            "Direct Sync has no backup/restore package; use `sync status --json`, `sync export`, and the state directory together"
                .into(),
        ),
    }
}

fn resolve_staging(config: &mut VolumeConfig, config_path: &Path) {
    if config.staging.is_relative() && !config.staging.as_os_str().is_empty() {
        config.staging = config_path
            .parent()
            .unwrap_or(Path::new("."))
            .join(&config.staging);
    }
}

fn operator() -> Result<opendal::Operator, opendal::Error> {
    let config = std::env::vars()
        .filter_map(|(key, value)| {
            key.strip_prefix("YINYANG_S3_")
                .map(|key| (key.to_ascii_lowercase(), value))
        })
        .collect::<Vec<_>>();
    opendal::Operator::via_iter("s3", config)
}

fn admit(config: &VolumeConfig) -> Result<DirectRemoteAdapter, Box<dyn std::error::Error>> {
    if config.model != Model::Direct
        || config.access != Access::Sync
        || config.frontend != Frontend::Library
        || config.read_only
    {
        return Err(format!(
            "Direct Sync requires model=direct, access=sync, frontend=library, read_only=false; found model={:?} access={:?} frontend={:?} read_only={}",
            config.model, config.access, config.frontend, config.read_only
        )
        .into());
    }
    if config.staging.as_os_str().is_empty() {
        return Err("Direct Sync requires a non-empty staging path".into());
    }
    let operator = operator().map_err(|error| format!("storage configuration: {error}"))?;
    config.validate(&operator)?;
    let binding = storage_binding(config, &operator)?;
    let namespace = yinyang::volume::direct_namespace(config, operator)?;
    Ok(DirectRemoteAdapter::open(
        namespace,
        hex(binding.namespace_id()),
    )?)
}

fn storage_binding(
    config: &VolumeConfig,
    operator: &opendal::Operator,
) -> Result<DirectStorageBinding, Box<dyn std::error::Error>> {
    Ok(yinyang::volume::direct_storage_binding(config, operator)?)
}

async fn init(
    config: &VolumeConfig,
    directory: &Path,
) -> Result<ExitCode, Box<dyn std::error::Error>> {
    let remote = admit(config)?;
    DirectSync::init(remote, &config.staging, directory).await?;
    println!(
        "Direct Sync bound {} to state {}",
        directory.display(),
        config.staging.display()
    );
    Ok(ExitCode::SUCCESS)
}

async fn reconcile(config: &VolumeConfig) -> Result<ExitCode, Box<dyn std::error::Error>> {
    let remote = admit(config)?;
    let mut sync = DirectSync::open(remote, &config.staging).await?;
    let outcome = sync.run().await?;
    println!("{}", outcome_text("Direct Sync run", &outcome));
    Ok(ExitCode::SUCCESS)
}

async fn status(config: &VolumeConfig, json: bool) -> Result<ExitCode, Box<dyn std::error::Error>> {
    let (remote, remote_error) = match admit(config) {
        Ok(remote) => (Some(remote), None),
        Err(error) => (None, Some(error.to_string())),
    };
    let inspection =
        DirectSync::<DirectRemoteAdapter>::inspect(remote.as_ref(), &config.staging, remote_error)
            .await;
    let code = match inspection.binding.state {
        BindingState::Unbound => EXIT_UNBOUND,
        BindingState::Bound => 0,
        BindingState::Error => EXIT_INVALID_BINDING,
    };
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&status_document(&inspection, &config.staging))?
        );
    } else {
        print_status(&inspection);
    }
    Ok(ExitCode::from(code))
}

async fn export(
    config: &VolumeConfig,
    artifact: Option<String>,
    conflict: Option<String>,
    side: Option<SyncSide>,
    output: &Path,
) -> Result<ExitCode, Box<dyn std::error::Error>> {
    let side = match (&artifact, &conflict) {
        (Some(_), None) => None,
        (None, Some(_)) => Some(side.ok_or("--conflict requires --side local|remote")?),
        (Some(_), Some(_)) => {
            return Err("--artifact and --conflict are mutually exclusive".into());
        }
        (None, None) => {
            return Err("export requires exactly one of --artifact or --conflict".into());
        }
    };
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(output)
        .map_err(|error| {
            format!(
                "create export {} (refusing to overwrite an existing path): {error}",
                output.display()
            )
        })?;
    let result = if let Some(id) = artifact {
        DirectSync::<DirectRemoteAdapter>::export_artifact_local(&config.staging, &id, &mut file)
    } else {
        DirectSync::<DirectRemoteAdapter>::export_conflict_side_local(
            &config.staging,
            conflict.as_deref().expect("conflict selector present"),
            match side.expect("conflict side present") {
                SyncSide::Local => "local",
                SyncSide::Remote => "remote",
            },
            &mut file,
        )
    };
    drop(file);
    if let Err(error) = result {
        let _ = std::fs::remove_file(output);
        return Err(error.into());
    }
    Ok(ExitCode::SUCCESS)
}

async fn resolve(
    config: &VolumeConfig,
    conflict: &str,
    keep: Option<SyncSide>,
    content: Option<&Path>,
) -> Result<ExitCode, Box<dyn std::error::Error>> {
    if content.is_some() {
        return Err(
            "Direct Sync resolve does not accept --content; export/inspect both sides, edit the local directory explicitly, then use --keep local"
                .into(),
        );
    }
    let keep = keep.ok_or("Direct Sync resolve requires --keep local|remote")?;
    let remote = admit(config)?;
    let mut sync = DirectSync::open(remote, &config.staging).await?;
    let resolution = match keep {
        SyncSide::Local => Resolution::KeepLocal,
        SyncSide::Remote => Resolution::KeepRemote,
    };
    let outcome = sync.resolve(conflict, resolution).await?;
    println!("{}", outcome_text("Direct Sync resolve", &outcome));
    Ok(ExitCode::SUCCESS)
}

fn print_status(inspection: &Inspection) {
    println!(
        "Direct Sync: in_sync={} pending={} unknown={} conflicts={}",
        inspection.summary.in_sync,
        inspection.summary.pending,
        inspection.summary.unknown,
        inspection.summary.conflicts
    );
    if let Some(error) = &inspection.remote.error {
        println!("remote: unavailable ({error})");
    } else {
        println!(
            "remote: {:?} revision={}",
            inspection.remote.availability,
            inspection.remote.revision.as_deref().unwrap_or("<none>")
        );
    }
    for entry in &inspection.entries {
        if entry.state != EntryState::InSync {
            println!(
                "{}: {:?} work={:?} conflict={}",
                entry.path,
                entry.state,
                entry.work.state,
                entry.conflict.as_deref().unwrap_or("<none>")
            );
        }
    }
}

fn outcome_text(label: &str, outcome: &RunOutcome) -> String {
    format!(
        "{label}: in_sync={} pending={} unknown={} conflicts={} published={} materialized={} deleted_remote={} deleted_local={}",
        outcome.in_sync,
        outcome.pending,
        outcome.unknown,
        outcome.conflicts,
        outcome.published_local,
        outcome.materialized_remote,
        outcome.deleted_remote,
        outcome.deleted_local
    )
}

fn status_document(inspection: &Inspection, state: &Path) -> Value {
    json!({
        "schema": STATUS_SCHEMA,
        "binding": {
            "state": binding_state(inspection.binding.state),
            "state_directory": inspection.binding.state_directory.display().to_string(),
            "root": inspection.binding.root.as_ref().map(|path| path.display().to_string()),
            "namespace": inspection.binding.namespace,
            "same_filesystem": inspection.binding.same_filesystem,
            "error": inspection.binding.error,
        },
        "remote": {
            "availability": remote_availability(inspection.remote.availability),
            "revision": inspection.remote.revision,
            "error": inspection.remote.error,
        },
        "local": {
            "scan": local_scan_state(inspection.local.scan),
            "error": inspection.local.error,
        },
        "summary": {
            "in_sync": inspection.summary.in_sync,
            "pending": inspection.summary.pending,
            "unknown": inspection.summary.unknown,
            "conflicts": inspection.summary.conflicts,
            "failed": inspection.summary.failed,
            "retained_artifacts": inspection.summary.retained_artifacts,
        },
        "entries": inspection.entries.iter().map(|entry| json!({
            "path": entry.path,
            "state": entry_state(entry.state),
            "local": {
                "presence": presence(entry.local.presence),
                "kind": entry.local.kind.map(evidence_kind),
                "length": entry.local.length,
                "digest": entry.local.digest.map(hex),
            },
            "remote": {
                "presence": presence(entry.remote.presence),
                "kind": entry.remote.kind.map(evidence_kind),
                "length": entry.remote.length,
                "pin": entry.remote.pin.as_ref().map(pin_document),
            },
            "baseline": {
                "local_digest": entry.baseline.local_digest.map(hex),
                "local_length": entry.baseline.local_length,
                "remote": entry.baseline.remote.as_ref().map(pin_document),
            },
            "work": {
                "id": entry.work.id,
                "operation": entry.work.operation.map(remote_operation),
                "state": entry.work.state.map(work_state),
                "install_state": entry.work.install_state.map(install_state),
                "error": entry.work.error,
            },
            "conflict": entry.conflict,
        })).collect::<Vec<_>>(),
        "conflicts": inspection.conflicts.iter().map(|conflict| json!({
            "id": conflict.id,
            "path": conflict.path,
            "local": conflict_side(&conflict.local),
            "remote": conflict_side(&conflict.remote),
            "local_artifact": conflict.local_artifact,
            "remote_artifact": conflict.remote_artifact,
            "error": conflict.error,
        })).collect::<Vec<_>>(),
        "artifacts": inspection.artifacts.iter().map(|artifact| json!({
            "id": artifact.id,
            "origin": artifact.origin,
            "kind": artifact_kind(artifact.kind),
            "phase": artifact_phase(artifact.phase),
            "path": state.join("artifacts").join(&artifact.name).display().to_string(),
            "exportable": artifact.phase != ArtifactPhase::Failed
                && state.join("artifacts").join(&artifact.name).is_file(),
            "error": artifact.error,
        })).collect::<Vec<_>>(),
    })
}

fn pin_document(pin: &yinyang::direct_sync::RemotePin) -> Value {
    json!({
        "version": pin.version,
        "etag": pin.etag,
        "size": pin.size,
        "modified": pin.modified,
    })
}

fn conflict_side(side: &yinyang::direct_sync::ConflictSide) -> Value {
    json!({
        "presence": presence(side.presence),
        "kind": side.kind.map(evidence_kind),
        "length": side.length,
        "digest": side.digest.map(hex),
        "pin": side.pin.as_ref().map(pin_document),
    })
}

fn binding_state(value: BindingState) -> &'static str {
    match value {
        BindingState::Unbound => "unbound",
        BindingState::Bound => "bound",
        BindingState::Error => "error",
    }
}

fn remote_availability(value: RemoteAvailability) -> &'static str {
    match value {
        RemoteAvailability::NotChecked => "not_checked",
        RemoteAvailability::Available => "available",
        RemoteAvailability::Unavailable => "unavailable",
    }
}

fn local_scan_state(value: LocalScanState) -> &'static str {
    match value {
        LocalScanState::NotScanned => "not_scanned",
        LocalScanState::Clean => "clean",
        LocalScanState::Changed => "changed",
        LocalScanState::Failed => "failed",
    }
}

fn entry_state(value: EntryState) -> &'static str {
    match value {
        EntryState::InSync => "in_sync",
        EntryState::LocalPending => "local_pending",
        EntryState::RemotePending => "remote_pending",
        EntryState::Conflict => "conflict",
        EntryState::Unknown => "unknown",
        EntryState::Failed => "failed",
    }
}

fn presence(value: ConflictPresence) -> &'static str {
    match value {
        ConflictPresence::Present => "present",
        ConflictPresence::Absent => "absent",
    }
}

fn evidence_kind(value: EvidenceKind) -> &'static str {
    match value {
        EvidenceKind::File => "file",
        EvidenceKind::Directory => "directory",
    }
}

fn remote_operation(value: RemoteOperation) -> &'static str {
    match value {
        RemoteOperation::Publish => "publish",
        RemoteOperation::Delete => "delete",
    }
}

fn work_state(value: WorkState) -> &'static str {
    match value {
        WorkState::Prepared => "prepared",
        WorkState::Unknown => "unknown",
        WorkState::Conflict => "conflict",
        WorkState::Failed => "failed",
    }
}

fn install_state(value: InstallState) -> &'static str {
    match value {
        InstallState::Prepared => "prepared",
        InstallState::Dispatched => "dispatched",
        InstallState::Applied => "applied",
        InstallState::Conflict => "conflict",
        InstallState::Failed => "failed",
    }
}

fn artifact_kind(value: yinyang::direct_sync::ArtifactKind) -> &'static str {
    match value {
        yinyang::direct_sync::ArtifactKind::Upload => "upload",
        yinyang::direct_sync::ArtifactKind::LocalConflict => "local_conflict",
        yinyang::direct_sync::ArtifactKind::RemoteConflict => "remote_conflict",
        yinyang::direct_sync::ArtifactKind::DisplacedLocal => "displaced_local",
        yinyang::direct_sync::ArtifactKind::Resolution => "resolution",
    }
}

fn artifact_phase(value: ArtifactPhase) -> &'static str {
    match value {
        ArtifactPhase::Preparing => "preparing",
        ArtifactPhase::Retained => "retained",
        ArtifactPhase::Conflict => "conflict",
        ArtifactPhase::Failed => "failed",
    }
}

fn hex(bytes: [u8; 32]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}
