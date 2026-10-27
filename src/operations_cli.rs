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
use yinyang::runtime::Runtime;
use yinyang::volume::{Access, Frontend, Publication, Volume, VolumeConfig};

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

    let bytes = match tokio::fs::read(path).await {
        Ok(bytes) => bytes,
        Err(error) => {
            report.push(
                "configuration",
                Health::ActionRequired,
                format!("read {}: {error}", path.display()),
                Some("pass --volume with a readable strict volume JSON document"),
            );
            return finish(report, &secrets, json);
        }
    };
    let mut config = match VolumeConfig::from_json(&bytes) {
        Ok(config) => config,
        Err(error) => {
            report.push(
                "configuration",
                Health::ActionRequired,
                error.to_string(),
                Some("fix the volume JSON; unknown fields and unsupported combinations are errors"),
            );
            return finish(report, &secrets, json);
        }
    };
    if config.staging.is_relative() {
        config.staging = path
            .parent()
            .unwrap_or(Path::new("."))
            .join(&config.staging);
    }
    report.volume = VolumeReport {
        name: Some(config.name.clone()),
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
    report.push(
        "configuration",
        Health::Pass,
        format!(
            "volume {} uses {:?} access with {:?} publication",
            config.name, config.access, config.publication
        ),
        None::<String>,
    );

    let (environment_status, environment_detail, environment_remediation) =
        match storage_environment(&config) {
            Ok(detail) => (Health::Pass, detail, None),
            Err(detail) => (
                Health::ActionRequired,
                detail,
                Some(
                    "set YINYANG_S3_BUCKET; use a complete access-key pair or the ambient AWS credential chain, and provide the service token when publication=service",
                ),
            ),
        };
    let environment_ready = environment_status == Health::Pass;
    report.push(
        "environment",
        environment_status,
        environment_detail,
        environment_remediation,
    );

    check_fuse(&config, &mut report);
    check_local_state(&config, &mut report).await;

    if !environment_ready {
        report.push(
            "authority",
            Health::ActionRequired,
            "not checked because the storage or service environment is invalid",
            Some("fix the environment check first, then rerun `yy doctor`"),
        );
        if config.access == Access::Sync {
            report.push(
                "sync-convergence",
                Health::Info,
                "doctor checks readiness only; finite `sync run` completion is not convergence",
                Some(format!(
                    "run `yy --volume {} sync status --json` and require summary.in_sync=true",
                    path.display()
                )),
            );
        }
        return finish(report, &secrets, json);
    }

    let operator = match operator() {
        Ok(operator) => Some(operator),
        Err(error) => {
            report.push(
                "admission",
                Health::ActionRequired,
                format!("construct S3 operator: {error}"),
                Some("complete the YINYANG_S3_* environment described by `yy capabilities`"),
            );
            None
        }
    };
    let mut admitted = false;
    if let Some(operator) = operator {
        match config.validate(&operator) {
            Ok(capabilities) => {
                admitted = true;
                report.push(
                    "admission",
                    Health::Pass,
                    format!(
                        "effective capabilities: {}",
                        capabilities
                            .effective
                            .iter()
                            .map(|capability| format!("{capability:?}").to_ascii_lowercase())
                            .collect::<Vec<_>>()
                            .join(", ")
                    ),
                    None::<String>,
                );
            }
            Err(error) => report.push(
                "admission",
                Health::ActionRequired,
                error.to_string(),
                Some(
                    "match model, access, frontend, publication and storage capabilities; inspect `yy capabilities`",
                ),
            ),
        }
        if admitted {
            check_authority(&config, operator, &mut report).await;
        } else {
            report.push(
                "authority",
                Health::ActionRequired,
                "not checked because volume admission failed",
                Some("fix admission first, then rerun `yy doctor`"),
            );
        }
    } else {
        report.push(
            "authority",
            Health::ActionRequired,
            "not checked because the storage operator is invalid",
            Some("fix the environment check first, then rerun `yy doctor`"),
        );
    }

    if config.access == Access::Sync {
        report.push(
            "sync-convergence",
            Health::Info,
            "doctor checks readiness only; finite `sync run` completion is not convergence",
            Some(format!(
                "run `yy --volume {} sync status --json` and require summary.in_sync=true",
                path.display()
            )),
        );
    }
    finish(report, &secrets, json)
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

fn storage_environment(config: &VolumeConfig) -> Result<String, String> {
    let bucket = std::env::var("YINYANG_S3_BUCKET").unwrap_or_default();
    if bucket.trim().is_empty() {
        return Err("YINYANG_S3_BUCKET is not set".to_owned());
    }
    let access_key = std::env::var_os("YINYANG_S3_ACCESS_KEY_ID").filter(|value| !value.is_empty());
    let secret_key =
        std::env::var_os("YINYANG_S3_SECRET_ACCESS_KEY").filter(|value| !value.is_empty());
    if access_key.is_some() != secret_key.is_some() {
        return Err(
            "YINYANG_S3_ACCESS_KEY_ID and YINYANG_S3_SECRET_ACCESS_KEY must be set together"
                .to_owned(),
        );
    }
    if config.publication == Publication::Service {
        let token = std::env::var("YINYANG_SERVICE_TOKEN").unwrap_or_default();
        if !(32..=1024).contains(&token.len()) {
            return Err(
                "YINYANG_SERVICE_TOKEN must contain 32..=1024 bytes for service publication"
                    .to_owned(),
            );
        }
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
                    "inspect `yy --volume CONFIG status`; preserve staging until every uncertain request is resolved",
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

async fn check_authority(
    config: &VolumeConfig,
    operator: opendal::Operator,
    report: &mut DoctorReport,
) {
    let token = std::env::var("YINYANG_SERVICE_TOKEN").ok();
    match Volume::connect(config, operator, token).await {
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
                Some(authority_remediation(config, &error.to_string())),
            ),
        },
        Err(error) => report.push(
            "authority",
            Health::ActionRequired,
            error.to_string(),
            Some(authority_remediation(config, &error.to_string())),
        ),
    }
}

fn authority_remediation(config: &VolumeConfig, detail: &str) -> &'static str {
    let detail = detail.to_ascii_lowercase();
    if config.publication == Publication::Service {
        if detail.contains("token") || detail.contains("auth") {
            return "use the same 32..=1024 byte YINYANG_SERVICE_TOKEN for `yy serve` and the client, then check `systemctl --user status yinyang-serve.service`";
        }
        if detail.contains("connect")
            || detail.contains("refused")
            || detail.contains("timeout")
            || detail.contains("unavailable")
        {
            return "start the metadata service and verify that service_address is the same loopback address it reports";
        }
        return "check the metadata service log, its SQLite database and its YINYANG_S3_* content configuration";
    }
    if detail.contains("credential")
        || detail.contains("403")
        || detail.contains("forbidden")
        || detail.contains("signature")
    {
        return "verify the storage credentials and bucket permissions; a 403 does not distinguish a bad key from a policy denial";
    }
    if detail.contains("bucket") {
        return "verify YINYANG_S3_BUCKET and YINYANG_S3_ROOT and grant the principal read/write access";
    }
    if detail.contains("connect")
        || detail.contains("dns")
        || detail.contains("refused")
        || detail.contains("timeout")
    {
        return "verify YINYANG_S3_ENDPOINT, region, DNS, TLS trust and outbound network access";
    }
    "inspect the authority error, bucket policy and local SQLite state; preserve state if the outcome is uncertain"
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
}
