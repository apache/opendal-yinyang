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

//! Operational health diagnostics for an installed Linux volume.

use std::path::{Path, PathBuf};
use std::process::ExitCode;

use serde::Serialize;
use yinyang::direct::{DirectRuntime, DirectRuntimeInspectionState};
#[cfg(target_os = "linux")]
use yinyang::direct_sync::{
    BindingState, DirectRemote, DirectRemoteAdapter, DirectSync, LocalScanState,
    validate_remote_namespace,
};
use yinyang::runtime::ErrorKind;
use yinyang::runtime::Runtime;
use yinyang::volume::{
    Access, Frontend, Model, Publication, VolumeConfig, VolumeContext, VolumeLoadError,
    VolumeLoadStage,
};

use crate::{CliError, CliStage};

const SCHEMA: &str = "yinyang.operational-health.v1";

#[derive(Clone, Copy, Debug, Serialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
enum Health {
    Pass,
    ActionRequired,
    Info,
}

#[derive(Debug, Serialize)]
struct DoctorReport {
    schema: &'static str,
    status: Health,
    tested_host: bool,
    config: String,
    volume: VolumeReport,
    scope: DiagnosticScope,
    checks: Vec<HealthCheck>,
}

impl DoctorReport {
    fn new(path: &Path) -> Self {
        Self {
            schema: SCHEMA,
            status: Health::ActionRequired,
            tested_host: false,
            config: path.display().to_string(),
            volume: VolumeReport::default(),
            scope: DiagnosticScope {
                state_probe: "create one namespaced temporary file under the nearest existing state-directory ancestor, sync it, and remove it",
                catalog_and_publication_state: "no filesystem initialization, SQLite creation, recovery, publication, or runtime lease is performed",
                authority: "connect and observe the latest revision only; no mutation is issued",
            },
            checks: Vec::new(),
        }
    }

    fn push(
        &mut self,
        name: &'static str,
        status: Health,
        detail: impl Into<String>,
        remediation: Option<impl Into<String>>,
    ) {
        self.checks.push(HealthCheck {
            name,
            status,
            detail: detail.into(),
            remediation: remediation.map(Into::into),
        });
    }

    fn finish(&mut self, secrets: &SecretRedactor) {
        self.config = secrets.redact(&self.config);
        for check in &mut self.checks {
            check.detail = secrets.redact(&check.detail);
            check.remediation = check
                .remediation
                .as_deref()
                .map(|text| secrets.redact(text));
        }
        for value in [
            &mut self.volume.name,
            &mut self.volume.model,
            &mut self.volume.access,
            &mut self.volume.frontend,
            &mut self.volume.authority,
        ]
        .into_iter()
        .flatten()
        {
            *value = secrets.redact(value);
        }
        self.status = if self
            .checks
            .iter()
            .any(|check| check.status == Health::ActionRequired)
        {
            Health::ActionRequired
        } else {
            Health::Pass
        };
    }
}

#[derive(Debug, Default, Serialize)]
struct VolumeReport {
    name: Option<String>,
    model: Option<String>,
    access: Option<String>,
    frontend: Option<String>,
    authority: Option<String>,
}

#[derive(Debug, Serialize)]
struct DiagnosticScope {
    state_probe: &'static str,
    catalog_and_publication_state: &'static str,
    authority: &'static str,
}

#[derive(Debug, Serialize)]
struct HealthCheck {
    name: &'static str,
    status: Health,
    detail: String,
    remediation: Option<String>,
}

pub async fn run(path: &Path, json: bool) -> ExitCode {
    let secrets = SecretRedactor::capture();
    let mut report = DoctorReport::new(path);
    report.tested_host = qualified_host();
    let platform = platform_status();
    report.push(
        "platform",
        platform,
        platform_detail(),
        (platform == Health::ActionRequired).then_some(
            "use Linux for the native volume frontends, or qualify another platform before relying on it",
        ),
    );

    let context = match VolumeContext::load(path).await {
        Ok(context) => context,
        Err(error) => {
            let Some(config) = error.config() else {
                report.push(
                    "configuration",
                    Health::ActionRequired,
                    error.message(),
                    Some("pass --volume with a readable strict volume JSON document"),
                );
                return finish(report, &secrets, json);
            };
            populate_volume_report(config, &mut report);
            report.push(
                "configuration",
                Health::Pass,
                format!(
                    "volume {} uses {:?} model with {:?} access and {:?} publication",
                    config.name, config.model, config.access, config.publication
                ),
                None::<String>,
            );
            let stage_name = match error.stage() {
                VolumeLoadStage::Configuration => "configuration",
                VolumeLoadStage::Operator => "environment",
                VolumeLoadStage::Admission => "admission",
            };
            report.push(
                stage_name,
                Health::ActionRequired,
                error.message(),
                Some(load_error_remediation(&error)),
            );
            check_fuse(config, &mut report);
            check_local_state(config, &mut report).await;
            if config.model == Model::Direct && config.access == Access::Sync {
                check_direct_sync_state(
                    None,
                    config,
                    Some(error.message().to_owned()),
                    &mut report,
                )
                .await;
            }
            return finish(report, &secrets, json);
        }
    };

    populate_volume_report(&context.config, &mut report);
    report.push(
        "configuration",
        Health::Pass,
        format!(
            "volume {} uses {:?} model with {:?} access and {:?} publication",
            context.config.name,
            context.config.model,
            context.config.access,
            context.config.publication
        ),
        None::<String>,
    );

    let environment = storage_environment(&context.config).and_then(|detail| {
        validate_service_token(&context.config, context.service_token()).map(|_| detail)
    });
    let (environment_status, environment_detail, environment_remediation) = match environment {
        Ok(detail) => (Health::Pass, detail, None),
        Err(error) => (
            Health::ActionRequired,
            error.message().to_owned(),
            Some(environment_remediation(&context.config)),
        ),
    };
    let environment_ready = environment_status == Health::Pass;
    report.push(
        "environment",
        environment_status,
        environment_detail,
        environment_remediation,
    );
    report.push(
        "admission",
        Health::Pass,
        format!(
            "admitted capabilities: {}",
            context
                .capabilities
                .admitted
                .iter()
                .map(|capability| format!("{capability:?}").to_ascii_lowercase())
                .collect::<Vec<_>>()
                .join(", ")
        ),
        None::<String>,
    );

    check_fuse(&context.config, &mut report);
    check_local_state(&context.config, &mut report).await;

    if environment_ready {
        if context.config.model == Model::Direct && context.config.access == Access::Sync {
            check_direct_sync_state(Some(&context), &context.config, None, &mut report).await;
        } else if context.config.model == Model::Direct && !context.config.read_only {
            check_direct_runtime_state(&context, &mut report);
        }
        match context.config.model {
            Model::Direct => check_direct_namespace(&context, &mut report).await,
            Model::Managed => check_authority(&context, &mut report).await,
        }
    } else {
        report.push(
            if context.config.model == Model::Direct {
                "namespace"
            } else {
                "authority"
            },
            Health::ActionRequired,
            "not checked because the storage or service environment is invalid",
            Some("fix the environment check first, then rerun `yy doctor`"),
        );
        if context.config.model == Model::Direct && context.config.access == Access::Sync {
            check_direct_sync_state(
                None,
                &context.config,
                Some("the storage or service environment is invalid".to_owned()),
                &mut report,
            )
            .await;
        }
    }

    if context.config.access == Access::Sync {
        report.push(
            "sync-convergence",
            Health::Info,
            "doctor checks readiness only; finite `sync run` completion is not convergence",
            Some(format!(
                "run `yy --volume {} sync status --json` and require summary.in_sync=true",
                context.path.display()
            )),
        );
    }
    finish(report, &secrets, json)
}

fn populate_volume_report(config: &VolumeConfig, report: &mut DoctorReport) {
    report.volume = VolumeReport {
        name: Some(config.name.clone()),
        model: Some(format!("{:?}", config.model).to_ascii_lowercase()),
        access: Some(format!("{:?}", config.access).to_ascii_lowercase()),
        frontend: Some(format!("{:?}", config.frontend).to_ascii_lowercase()),
        authority: Some(
            match config.publication {
                Publication::Object => "object",
                Publication::Service => "authenticated-loopback-service",
            }
            .to_owned(),
        ),
    };
    if config.model == Model::Direct {
        report.scope = if config.access == Access::Sync {
            DiagnosticScope {
                state_probe: "probe the configured Direct Sync state directory with one temporary owner-only file, sync it, and remove it",
                catalog_and_publication_state: "inspect Direct Sync SQLite state, baseline, conflicts and retained artifacts read-only; no recovery, initialization, publication or writer lease is created",
                authority: "observe one Direct namespace; no mutation is issued",
            }
        } else if config.read_only {
            DiagnosticScope {
                state_probe: "not performed; read-only Direct has no local state path",
                catalog_and_publication_state: "recursive object listing is read-only; no object, YinYang metadata, SQLite state, recovery, publication or lease is created",
                authority: "observe one Direct namespace; no mutation is issued",
            }
        } else {
            DiagnosticScope {
                state_probe: "probe the configured Direct staging path with one temporary owner-only file, sync it, and remove it",
                catalog_and_publication_state: "inspect retained Direct runtime state read-only; no object, recovery, publication or runtime lease is created",
                authority: "observe one Direct namespace; no mutation is issued",
            }
        };
    }
}

fn load_error_remediation(error: &VolumeLoadError) -> &'static str {
    match (error.stage(), error.kind()) {
        (VolumeLoadStage::Configuration, _) => {
            "fix the strict volume JSON; unknown fields and unsupported combinations are errors"
        }
        (VolumeLoadStage::Operator, _) => {
            "complete the YINYANG_S3_* environment described by `yy capabilities`"
        }
        (VolumeLoadStage::Admission, ErrorKind::Unsupported) => {
            "select a storage profile with the required capabilities and inspect `yy capabilities`"
        }
        (VolumeLoadStage::Admission, _) => {
            "match model, access, frontend, publication, staging and read_only in the volume document"
        }
    }
}

fn environment_remediation(config: &VolumeConfig) -> &'static str {
    if config.model == Model::Direct {
        if config.read_only {
            "set YINYANG_S3_BUCKET; use a complete access-key pair or the ambient AWS credential chain with stat, read and list permission"
        } else {
            "set YINYANG_S3_BUCKET; use a complete access-key pair or the ambient AWS credential chain with stat, read, list, conditional write and conditional delete permission as required"
        }
    } else {
        "set YINYANG_S3_BUCKET; use a complete access-key pair or the ambient AWS credential chain, and provide the service token when publication=service"
    }
}

fn finish(mut report: DoctorReport, secrets: &SecretRedactor, json: bool) -> ExitCode {
    report.finish(secrets);
    let text = if json {
        serde_json::to_string_pretty(&report)
    } else {
        Ok(human_report(&report))
    };
    match text {
        Ok(text) => println!("{text}"),
        Err(error) => {
            eprintln!("yy: write doctor report: {error}");
            return ExitCode::FAILURE;
        }
    }
    if report.status == Health::Pass {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}

fn human_report(report: &DoctorReport) -> String {
    let mut output = format!(
        "doctor: {}\n",
        match report.status {
            Health::Pass => "ready",
            Health::ActionRequired => "action-required",
            Health::Info => "info",
        }
    );
    for check in &report.checks {
        let status = match check.status {
            Health::Pass => "pass",
            Health::ActionRequired => "action-required",
            Health::Info => "info",
        };
        output.push_str(&format!("[{status}] {}: {}\n", check.name, check.detail));
        if let Some(remediation) = &check.remediation {
            output.push_str(&format!("  next: {remediation}\n"));
        }
    }
    output
}

fn platform_status() -> Health {
    if cfg!(target_os = "linux") {
        if qualified_host() {
            Health::Pass
        } else {
            Health::Info
        }
    } else {
        Health::ActionRequired
    }
}

fn qualified_host() -> bool {
    let os = std::fs::read_to_string("/etc/os-release").unwrap_or_default();
    let ubuntu_2404 = os.lines().any(|line| line == "ID=ubuntu")
        && os
            .lines()
            .any(|line| matches!(line, "VERSION_ID=\"24.04\"" | "VERSION_ID=24.04"));
    cfg!(target_os = "linux") && cfg!(target_arch = "x86_64") && ubuntu_2404
}

fn platform_detail() -> String {
    let os = std::fs::read_to_string("/etc/os-release").unwrap_or_default();
    let pretty = os
        .lines()
        .find_map(|line| line.strip_prefix("PRETTY_NAME="))
        .map(|value| value.trim_matches('"').to_owned())
        .unwrap_or_else(|| std::env::consts::OS.to_owned());
    if qualified_host() {
        format!("tested host: {pretty} {}", std::env::consts::ARCH)
    } else if cfg!(target_os = "linux") {
        format!(
            "untested host: {pretty} {}; the Linux CLI remains usable, but this host is outside the Ubuntu 24.04 x86_64 qualification",
            std::env::consts::ARCH
        )
    } else {
        format!(
            "unsupported operational host: {pretty} {}; Linux volume frontends are unavailable",
            std::env::consts::ARCH
        )
    }
}

fn storage_environment(config: &VolumeConfig) -> Result<String, CliError> {
    let bucket = std::env::var("YINYANG_S3_BUCKET").unwrap_or_default();
    if bucket.trim().is_empty() {
        return Err(CliError::invalid(
            CliStage::Operator,
            "YINYANG_S3_BUCKET is not set",
        ));
    }
    let access_key = std::env::var_os("YINYANG_S3_ACCESS_KEY_ID").filter(|value| !value.is_empty());
    let secret_key =
        std::env::var_os("YINYANG_S3_SECRET_ACCESS_KEY").filter(|value| !value.is_empty());
    if access_key.is_some() != secret_key.is_some() {
        return Err(CliError::invalid(
            CliStage::Operator,
            "YINYANG_S3_ACCESS_KEY_ID and YINYANG_S3_SECRET_ACCESS_KEY must be set together",
        ));
    }
    let credentials = if access_key.is_some() {
        "an explicit access-key pair"
    } else {
        "the ambient AWS credential chain"
    };
    let endpoint = if std::env::var_os("YINYANG_S3_ENDPOINT").is_some() {
        "a configured endpoint"
    } else {
        "the provider default endpoint"
    };
    Ok(format!(
        "bucket is present for volume storage_profile={:?} with {endpoint}; storage authentication uses {credentials}",
        config.storage_profile
    ))
}

fn validate_service_token(config: &VolumeConfig, token: Option<String>) -> Result<(), CliError> {
    if config.publication != Publication::Service {
        return Ok(());
    }
    let token = token.ok_or_else(|| {
        CliError::new(
            CliStage::Operator,
            ErrorKind::PermissionDenied,
            "YINYANG_SERVICE_TOKEN is required for service publication",
        )
    })?;
    if !(32..=1024).contains(&token.len()) {
        return Err(CliError::new(
            CliStage::Operator,
            ErrorKind::PermissionDenied,
            "YINYANG_SERVICE_TOKEN must contain 32..=1024 bytes for service publication",
        ));
    }
    Ok(())
}

fn check_fuse(config: &VolumeConfig, report: &mut DoctorReport) {
    if config.frontend != Frontend::Fuse {
        return;
    }
    let device = open_fuse_device();
    report.push(
        "fuse-device",
        if device.is_ok() {
            Health::Pass
        } else {
            Health::ActionRequired
        },
        match &device {
            Ok(()) => "/dev/fuse opened read/write successfully".to_owned(),
            Err(error) => format!("/dev/fuse cannot be opened read/write: {error}"),
        },
        device.err().map(|_| {
            "install fuse3, verify the device node, and run the service in a user session permitted to open /dev/fuse"
        }),
    );
    let helper = executable_on_path("fusermount3");
    report.push(
        "fuse-helper",
        if helper {
            Health::Pass
        } else {
            Health::ActionRequired
        },
        if helper {
            "fusermount3 is available on PATH"
        } else {
            "fusermount3 is not available on PATH"
        },
        (!helper).then_some("install the Ubuntu fuse3 package"),
    );
}

fn open_fuse_device() -> Result<(), String> {
    #[cfg(target_os = "linux")]
    {
        std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open("/dev/fuse")
            .map(drop)
            .map_err(|error| error.to_string())
    }
    #[cfg(not(target_os = "linux"))]
    {
        Err("native FUSE is available only on Linux".to_owned())
    }
}

async fn check_local_state(config: &VolumeConfig, report: &mut DoctorReport) {
    if config.model == Model::Direct && config.read_only {
        report.push(
            "local-state",
            Health::Info,
            "read-only Direct uses no staging path and creates no local namespace or publication state",
            None::<String>,
        );
        return;
    }
    if config.staging.as_os_str().is_empty() {
        report.push(
            "local-state",
            Health::ActionRequired,
            "admission did not provide a local state path",
            Some("set the required staging path and rerun `yy doctor`"),
        );
        return;
    }
    match probe_directory(&config.staging) {
        Ok(path) => report.push(
            "local-state",
            Health::Pass,
            format!(
                "state path {} accepts a temporary probe file, which was removed; no catalog, publication, or lease state was touched",
                path.display()
            ),
            None::<String>,
        ),
        Err(error) => report.push(
            "local-state",
            Health::ActionRequired,
            format!("state path {}: {error}", config.staging.display()),
            Some(
                "make the state path writable outside synchronized or mounted source trees and ensure free space",
            ),
        ),
    }
    if config.model == Model::Direct {
        return;
    }
    if config.access != Access::Mount || !config.staging.join("staging.db").exists() {
        return;
    }
    match Runtime::inspect(&config.staging).await {
        Ok(status) => {
            let pending = status.handles.iter().filter(|handle| handle.pending).count();
            let frozen = status.handles.iter().filter(|handle| handle.frozen).count();
            let conflicts = status.handles.iter().filter(|handle| handle.conflict).count();
            let failed = status
                .handles
                .iter()
                .filter(|handle| handle.error.is_some())
                .count();
            let healthy = pending + frozen + conflicts + failed == 0 && status.errors.is_empty();
            report.push(
                "mount-state",
                if healthy {
                    Health::Pass
                } else {
                    Health::ActionRequired
                },
                format!(
                    "pending={pending} frozen={frozen} conflicts={conflicts} failed={failed} ledger-errors={}",
                    status.errors.len()
                ),
                (!healthy).then_some(
                    "stop the mount and recover unresolved requests first; review the retained ledger with `yy --volume CONFIG status`, then explicitly acknowledge reviewed sequences with `yy --volume CONFIG file acknowledge-errors THROUGH`; doctor health converges only after every unresolved request is resolved and reviewed errors are acknowledged",
                ),
            );
        }
        Err(error) => report.push(
            "mount-state",
            Health::ActionRequired,
            error.to_string(),
            Some(
                "stop the mount, preserve its staging directory, and inspect the SQLite/WAL state before retrying",
            ),
        ),
    }
}

#[cfg(target_os = "linux")]
async fn check_direct_sync_state(
    context: Option<&VolumeContext>,
    config: &VolumeConfig,
    remote_error: Option<String>,
    report: &mut DoctorReport,
) {
    let config_path = report.config.clone();
    let (remote, remote_error) = match context {
        Some(context) => match crate::direct_sync_cli::remote_adapter(context) {
            Ok(remote) => (Some(remote), remote_error),
            Err(error) => {
                let detail = match remote_error {
                    Some(previous) => format!("{previous}; {error}"),
                    None => error.to_string(),
                };
                (None, Some(detail))
            }
        },
        None => (None, remote_error),
    };
    let inspection =
        DirectSync::<DirectRemoteAdapter>::inspect(remote.as_ref(), &config.staging, remote_error)
            .await;
    match inspection.binding.state {
        BindingState::Unbound => report.push(
            "sync-state",
            Health::Info,
            "Direct Sync state is not initialized; doctor did not create it",
            Some(format!(
                "run `yy --volume {} sync init --directory ROOT` before the first Sync run",
                config_path
            )),
        ),
        BindingState::Error => report.push(
            "sync-state",
            Health::ActionRequired,
            inspection
                .binding
                .error
                .unwrap_or_else(|| "Direct Sync state validation failed".to_owned()),
            Some("preserve the Direct Sync state directory and correct the binding or permissions"),
        ),
        BindingState::Bound => {
            let summary = inspection.summary;
            let lifecycle_failed = inspection.local.scan == LocalScanState::Failed;
            let state_failed = summary.failed > 0;
            let detail = format!(
                "Direct Sync state is readable; in_sync={} pending={} unknown={} conflicts={} failed={} retained_artifacts={}; remote={:?}",
                summary.in_sync,
                summary.pending,
                summary.unknown,
                summary.conflicts,
                summary.failed,
                summary.retained_artifacts,
                inspection.remote.availability,
            );
            let health = if lifecycle_failed || state_failed {
                Health::ActionRequired
            } else if summary.pending > 0 || summary.unknown > 0 || summary.conflicts > 0 {
                Health::Info
            } else {
                Health::Pass
            };
            report.push(
                "sync-state",
                health,
                detail,
                (!summary.in_sync).then_some(format!(
                    "run `yy --volume {} sync status --json` and inspect pending, Unknown, conflict or failed state",
                    config_path
                )),
            );
        }
    }
}

#[cfg(not(target_os = "linux"))]
async fn check_direct_sync_state(
    _context: Option<&VolumeContext>,
    _config: &VolumeConfig,
    _remote_error: Option<String>,
    _report: &mut DoctorReport,
) {
}

fn check_direct_runtime_state(context: &VolumeContext, report: &mut DoctorReport) {
    let binding = match context.direct_storage_binding() {
        Ok(binding) => binding,
        Err(error) => {
            report.push(
                "mount-state",
                Health::ActionRequired,
                error.to_string(),
                Some("correct the Direct storage profile, bucket, root or endpoint environment"),
            );
            return;
        }
    };
    match DirectRuntime::inspect(&context.config.staging, Some(&binding)) {
        Ok(inspection) => match inspection.state {
            DirectRuntimeInspectionState::Missing
            | DirectRuntimeInspectionState::Uninitialized => report.push(
                "mount-state",
                Health::Pass,
                "Direct runtime state is not initialized; doctor did not create it",
                None::<String>,
            ),
            DirectRuntimeInspectionState::Ready => {
                let objects = inspection.objects;
                let mutations = inspection.mutations;
                let unresolved_objects = objects.pending
                    + objects.conflict
                    + objects.unknown
                    + objects.failed;
                let unresolved_mutations = mutations.staged
                    + mutations.dispatching
                    + mutations.conflict
                    + mutations.unknown
                    + mutations.destination_applied
                    + mutations.pending_source_cleanup
                    + mutations.failed
                    + mutations.source_dispatching;
                let healthy = unresolved_objects == 0 && unresolved_mutations == 0;
                report.push(
                    "mount-state",
                    if healthy {
                        Health::Pass
                    } else {
                        Health::ActionRequired
                    },
                    format!(
                        "Direct runtime profile {:?} is readable; objects: pending={} conflict={} unknown={} failed={}; mutations: staged={} dispatching={} conflict={} unknown={} destination-applied={} source-cleanup={} failed={}",
                        inspection.profile.as_deref().unwrap_or("unknown"),
                        objects.pending,
                        objects.conflict,
                        objects.unknown,
                        objects.failed,
                        mutations.staged,
                        mutations.dispatching,
                        mutations.conflict,
                        mutations.unknown,
                        mutations.destination_applied,
                        mutations.pending_source_cleanup,
                        mutations.failed
                    ),
                    (!healthy).then_some(
                        "preserve the Direct staging directory and inspect its retained objects/mutations before retrying publication",
                    ),
                );
            }
        },
        Err(error) => report.push(
            "mount-state",
            Health::ActionRequired,
            error.to_string(),
            Some(
                "preserve the Direct staging directory and correct the runtime profile, schema or storage binding before startup",
            ),
        ),
    }
}

async fn check_direct_namespace(context: &VolumeContext, report: &mut DoctorReport) {
    let config = &context.config;
    #[cfg(target_os = "linux")]
    if config.access == Access::Sync {
        let remote = match crate::direct_sync_cli::remote_adapter(context) {
            Ok(remote) => remote,
            Err(error) => {
                report.push(
                    "namespace",
                    Health::ActionRequired,
                    error.to_string(),
                    Some(direct_remediation(error.stage(), error.kind())),
                );
                return;
            }
        };
        match remote.observe().await {
            Ok(snapshot) => {
                let root_entries = snapshot
                    .entries
                    .keys()
                    .filter(|path| !path.contains('/'))
                    .count();
                let root_directories = snapshot
                    .entries
                    .values()
                    .filter(|entry| !entry.path.contains('/') && !entry.is_file())
                    .count();
                match validate_remote_namespace(&snapshot) {
                    Ok(()) => report.push(
                        "namespace",
                        Health::Pass,
                        format!(
                            "Direct Sync namespace is readable at observation {} with {} root entries and {root_directories} root directories",
                            snapshot.revision.as_deref().unwrap_or("<none>"),
                            root_entries
                        ),
                        None::<String>,
                    ),
                    Err(error) => report.push(
                        "namespace",
                        Health::ActionRequired,
                        error.to_string(),
                        Some(direct_remediation(
                            CliStage::Observe,
                            runtime_kind_from_replica(error.kind()),
                        )),
                    ),
                }
            }
            Err(error) => report.push(
                "namespace",
                Health::ActionRequired,
                error.to_string(),
                Some(direct_remediation(
                    CliStage::Observe,
                    runtime_kind_from_replica(error.kind()),
                )),
            ),
        }
        return;
    }
    let namespace = match context.direct_namespace() {
        Ok(namespace) => namespace,
        Err(error) => {
            report.push(
                "namespace",
                Health::ActionRequired,
                error.to_string(),
                Some(direct_config_remediation(config)),
            );
            return;
        }
    };
    match namespace.observe().await {
        Ok(snapshot) => {
            let root = snapshot.read_dir(snapshot.root()).unwrap_or_default();
            let directories = root.iter().filter(|node| node.is_directory()).count();
            report.push(
                "namespace",
                Health::Pass,
                format!(
                    "Direct namespace is readable at observation {} with {} root entries and {directories} root directories",
                    snapshot.revision().to_hex(),
                    root.len()
                ),
                None::<String>,
            );
        }
        Err(error) => report.push(
            "namespace",
            Health::ActionRequired,
            error.to_string(),
            Some(direct_remediation(
                CliStage::Observe,
                crate::runtime_kind_from_direct(error.kind()),
            )),
        ),
    }
}

fn direct_config_remediation(config: &VolumeConfig) -> &'static str {
    if config.read_only {
        return "select an S3 storage profile with stat, read, recursive list, version reads and conditional ETag reads";
    }
    if config.access == Access::Sync {
        return "select an S3 storage profile with Direct reads, conditional create/replace and conditional delete, then correct unrepresentable keys or collisions";
    }
    "select an S3 storage profile with Direct reads and conditional create/replace, then correct unrepresentable keys or collisions"
}

async fn check_authority(context: &VolumeContext, report: &mut DoctorReport) {
    let config = &context.config;
    match context.connect_authority().await {
        Ok(authority) => match authority.observe_latest().await {
            Ok(snapshot) => report.push(
                "authority",
                Health::Pass,
                format!(
                    "{} authority is readable at revision {}",
                    match config.publication {
                        Publication::Object => "object",
                        Publication::Service => "authenticated loopback service",
                    },
                    snapshot
                        .revision()
                        .to_bytes()
                        .iter()
                        .map(|byte| format!("{byte:02x}"))
                        .collect::<String>()
                ),
                None::<String>,
            ),
            Err(error) => report.push(
                "authority",
                Health::ActionRequired,
                error.to_string(),
                Some(authority_remediation(
                    config,
                    CliStage::Observe,
                    crate::runtime_kind_from_core(error.kind()),
                )),
            ),
        },
        Err(error) => {
            let error = CliError::from_runtime(CliStage::Connect, error);
            report.push(
                "authority",
                Health::ActionRequired,
                error.message(),
                Some(authority_remediation(config, error.stage(), error.kind())),
            );
        }
    }
}

fn authority_remediation(config: &VolumeConfig, stage: CliStage, kind: ErrorKind) -> &'static str {
    match (config.publication, stage, kind) {
        (Publication::Service, CliStage::Connect, ErrorKind::PermissionDenied) => {
            "use the same 32..=1024 byte YINYANG_SERVICE_TOKEN for `yy serve` and the client, then check `systemctl --user status yinyang-serve.service`"
        }
        (Publication::Service, CliStage::Connect, _) => {
            "start the metadata service and verify that service_address is the same loopback address it reports"
        }
        (Publication::Service, CliStage::Observe, ErrorKind::PermissionDenied) => {
            "repair metadata service authentication, its SQLite database and its service logs"
        }
        (Publication::Service, _, _) => {
            "check the metadata service log, its SQLite database and its YINYANG_S3_* content configuration"
        }
        (Publication::Object, CliStage::Connect, ErrorKind::PermissionDenied)
        | (Publication::Object, CliStage::Observe, ErrorKind::PermissionDenied) => {
            "verify the storage credentials and bucket permissions"
        }
        (Publication::Object, CliStage::Connect, _) => {
            "verify YINYANG_S3_ENDPOINT, region, DNS, TLS trust and outbound network access"
        }
        (Publication::Object, CliStage::Observe, ErrorKind::NotFound) => {
            if config.model == Model::Direct && config.read_only {
                "verify YINYANG_S3_BUCKET and YINYANG_S3_ROOT and grant the principal stat, read and list access"
            } else if config.model == Model::Direct {
                "verify YINYANG_S3_BUCKET and YINYANG_S3_ROOT and grant the principal Direct read plus conditional write, and conditional delete when required"
            } else {
                "initialize the object authority with `yy create`, or verify YINYANG_S3_BUCKET and YINYANG_S3_ROOT"
            }
        }
        (Publication::Object, CliStage::Observe, _) => {
            "verify bucket policy, namespace state and retained local SQLite state; preserve state if the outcome is uncertain"
        }
        (_, _, _) => {
            "inspect the authority state and preserve local state if the outcome is uncertain"
        }
    }
}

fn direct_remediation(stage: CliStage, kind: ErrorKind) -> &'static str {
    match (stage, kind) {
        (_, ErrorKind::InvalidName) => {
            "rename or remove the named object key; Direct rejects unrepresentable names instead of hiding objects"
        }
        (_, ErrorKind::Invalid) => {
            "repair unrepresentable names or object-key collisions; Direct rejects them instead of hiding objects"
        }
        (_, ErrorKind::Unsupported) => {
            "select an S3 storage profile with the Direct read, list, version and conditional operation primitives"
        }
        (CliStage::Connect, _) => {
            "verify YINYANG_S3_ENDPOINT, region, DNS, TLS trust and outbound network access"
        }
        (_, ErrorKind::PermissionDenied) => {
            "verify that the configured principal can stat, read and list the bucket or prefix"
        }
        (_, _) => {
            "inspect the S3 namespace and its read/list permissions; Direct never creates or repairs object metadata"
        }
    }
}

fn runtime_kind_from_replica(kind: yinyang::replica::ErrorKind) -> ErrorKind {
    use yinyang::replica::ErrorKind as Kind;
    match kind {
        Kind::Invalid => ErrorKind::Invalid,
        Kind::Unsupported => ErrorKind::Unsupported,
        Kind::Conflict => ErrorKind::Conflict,
        Kind::Corrupt => ErrorKind::Corrupt,
        Kind::Busy => ErrorKind::Busy,
        Kind::Io => ErrorKind::Io,
    }
}

fn probe_directory(path: &Path) -> Result<PathBuf, String> {
    let mut candidate = path;
    loop {
        match std::fs::metadata(candidate) {
            Ok(metadata) if metadata.is_dir() => break,
            Ok(_) => {
                return Err(format!(
                    "{} exists but is not a directory",
                    candidate.display()
                ));
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                candidate = candidate
                    .parent()
                    .ok_or_else(|| "path has no existing directory ancestor".to_owned())?;
            }
            Err(error) => return Err(error.to_string()),
        }
    }
    let name = format!(
        ".yy-doctor-probe-{}-{}",
        std::process::id(),
        uuid::Uuid::new_v4().simple()
    );
    let probe = candidate.join(name);
    let result = (|| {
        use std::io::Write as _;
        let mut options = std::fs::OpenOptions::new();
        options.create_new(true).write(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt as _;
            options.mode(0o600);
        }
        let mut file = options.open(&probe)?;
        file.write_all(b"yinyang operational probe")?;
        file.sync_all()
    })();
    let _ = std::fs::remove_file(&probe);
    result.map_err(|error: std::io::Error| error.to_string())?;
    Ok(candidate.to_owned())
}

fn executable_on_path(name: &str) -> bool {
    let Some(path) = std::env::var_os("PATH") else {
        return false;
    };
    std::env::split_paths(&path).any(|directory| {
        let candidate = directory.join(name);
        let Ok(metadata) = std::fs::metadata(&candidate) else {
            return false;
        };
        if !metadata.is_file() {
            return false;
        }
        #[cfg(target_os = "linux")]
        {
            rustix::fs::access(&candidate, rustix::fs::Access::EXEC_OK).is_ok()
        }
        #[cfg(all(unix, not(target_os = "linux")))]
        {
            use std::os::unix::fs::PermissionsExt as _;
            metadata.permissions().mode() & 0o111 != 0
        }
        #[cfg(not(unix))]
        {
            false
        }
    })
}

struct SecretRedactor {
    values: Vec<String>,
}

impl SecretRedactor {
    fn capture() -> Self {
        let values = std::env::vars_os()
            .filter_map(|(name, value)| {
                let name = name.to_string_lossy().to_ascii_uppercase();
                let sensitive = name.contains("TOKEN")
                    || name.contains("PASSWORD")
                    || name.contains("SECRET")
                    || name.contains("ACCESS_KEY");
                let value = value.to_string_lossy().into_owned();
                (sensitive && value.len() >= 4).then_some(value)
            })
            .collect();
        Self { values }
    }

    fn redact(&self, value: &str) -> String {
        let mut redacted = value.to_owned();
        for secret in &self.values {
            redacted = redacted.replace(secret, "<redacted>");
        }
        redact_url_userinfo(&redacted)
    }
}

fn redact_url_userinfo(value: &str) -> String {
    let mut remaining = value;
    let mut redacted = String::with_capacity(value.len());
    while let Some(separator) = remaining.find("://") {
        let authority_start = separator + 3;
        redacted.push_str(&remaining[..authority_start]);
        let tail = &remaining[authority_start..];
        let end = tail
            .find(['/', '?', '#', ' ', '\n', '"', '\''])
            .unwrap_or(tail.len());
        let authority = &tail[..end];
        if let Some(at) = authority.rfind('@') {
            redacted.push_str("<redacted>@");
            redacted.push_str(&authority[at + 1..]);
        } else {
            redacted.push_str(authority);
        }
        remaining = &tail[end..];
    }
    redacted.push_str(remaining);
    redacted
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn url_credentials_are_redacted() {
        assert_eq!(
            redact_url_userinfo("endpoint https://user:password@example.com/path failed"),
            "endpoint https://<redacted>@example.com/path failed"
        );
    }

    #[test]
    fn remediation_uses_error_kind_and_stage() {
        let service = VolumeConfig::from_json(
            br#"{
                "name": "service",
                "model": "managed",
                "access": "mount",
                "frontend": "library",
                "publication": "service",
                "service_address": "127.0.0.1:7447",
                "storage_profile": "minio",
                "staging": "state",
                "read_only": false
            }"#,
        )
        .unwrap();
        assert!(
            authority_remediation(&service, CliStage::Connect, ErrorKind::PermissionDenied)
                .contains("YINYANG_SERVICE_TOKEN")
        );
        assert!(
            authority_remediation(&service, CliStage::Connect, ErrorKind::Storage)
                .contains("start the metadata service")
        );
        assert!(
            direct_remediation(CliStage::Observe, ErrorKind::PermissionDenied)
                .contains("principal")
        );
        assert!(
            direct_remediation(CliStage::Connect, ErrorKind::Storage)
                .contains("YINYANG_S3_ENDPOINT")
        );
    }
}
