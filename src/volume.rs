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

//! Volume configuration loading, admission, and shared local-path validation.
use crate::direct::{self, DirectNamespace, DirectStorageBinding};
use crate::runtime::{Error, ErrorKind, Result, Runtime};
use opendal::Operator;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use yinyang_core::service::ServiceClient;
use yinyang_core::{Authority, BackendProfile, ErrorKind as CoreErrorKind, Fs};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum Capability {
    Read,
    List,
    Write,
    Append,
    Truncate,
    AtomicRename,
    StableIdentity,
    PinnedReads,
    RetainedUnlinkRead,
    RemoteFsync,
    RecoverableStaging,
}
pub type CapabilitySet = BTreeSet<Capability>;
fn managed() -> CapabilitySet {
    use Capability::*;
    [
        Read,
        List,
        Write,
        Append,
        Truncate,
        AtomicRename,
        StableIdentity,
        PinnedReads,
        RetainedUnlinkRead,
        RemoteFsync,
        RecoverableStaging,
    ]
    .into_iter()
    .collect()
}
/// Read-only storage-native Direct namespace. There is no retained-unlink-read
/// claim: an unversioned ETag-pinned descriptor becomes Stale after a delete.
fn direct_read_only() -> CapabilitySet {
    use Capability::*;
    [Read, List, PinnedReads].into_iter().collect()
}
/// Whole-object staging over storage-native conditional replacements. Direct
/// does not claim Managed stable identity, retained unlink reads, or atomic
/// rename. `RemoteFsync` means the configured object write and its condition
/// completed, not that a Managed receipt exists.
fn direct_writable() -> CapabilitySet {
    use Capability::*;
    [
        Read,
        List,
        Write,
        Append,
        Truncate,
        PinnedReads,
        RemoteFsync,
        RecoverableStaging,
    ]
    .into_iter()
    .collect()
}
fn direct_capabilities(read_only: bool) -> CapabilitySet {
    if read_only {
        direct_read_only()
    } else {
        direct_writable()
    }
}
fn read_only() -> CapabilitySet {
    use Capability::*;
    [
        Read,
        List,
        StableIdentity,
        PinnedReads,
        RetainedUnlinkRead,
        RecoverableStaging,
    ]
    .into_iter()
    .collect()
}
/// Read-only Linux FUSE frontend capabilities. The frontend serves immutable
/// pinned file versions and pinned directory scans, so it does not provide
/// recoverable staging or any write-side operation.
fn fuse() -> CapabilitySet {
    use Capability::*;
    [Read, List, StableIdentity, PinnedReads, RetainedUnlinkRead]
        .into_iter()
        .collect()
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Model {
    Managed,
    Direct,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Access {
    Mount,
    Sync,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Frontend {
    Library,
    Fuse,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Publication {
    Object,
    Service,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize)]
#[serde(rename_all = "kebab-case")]
enum RawStorageProfile {
    AmazonS3,
    Minio,
}

impl From<RawStorageProfile> for BackendProfile {
    fn from(profile: RawStorageProfile) -> Self {
        match profile {
            RawStorageProfile::AmazonS3 => BackendProfile::AmazonS3,
            RawStorageProfile::Minio => BackendProfile::Minio,
        }
    }
}

fn serialize_storage_profile<S>(
    profile: &BackendProfile,
    serializer: S,
) -> std::result::Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(backend_profile_name(*profile))
}

fn backend_profile_name(profile: BackendProfile) -> &'static str {
    match profile {
        BackendProfile::AmazonS3 => "amazon-s3",
        BackendProfile::Minio => "minio",
    }
}

/// Strict configuration: unknown fields are errors, not ignored security policy.
/// Storage credentials and RPC token are supplied separately, never serialized.
#[derive(Clone, Debug, Serialize)]
pub struct VolumeConfig {
    pub name: String,
    pub model: Model,
    pub access: Access,
    pub frontend: Frontend,
    pub publication: Publication,
    #[serde(serialize_with = "serialize_storage_profile")]
    pub storage_profile: BackendProfile,
    #[serde(skip_serializing_if = "path_is_empty")]
    pub staging: PathBuf,
    #[serde(default)]
    pub read_only: bool,
    pub service_address: Option<SocketAddr>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawVolumeConfig {
    name: String,
    model: Model,
    access: Access,
    frontend: Frontend,
    publication: Publication,
    storage_profile: RawStorageProfile,
    #[serde(default)]
    staging: PathBuf,
    #[serde(default)]
    read_only: Option<bool>,
    #[serde(default)]
    service_address: Option<SocketAddr>,
}

impl<'de> Deserialize<'de> for VolumeConfig {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = RawVolumeConfig::deserialize(deserializer)?;
        Ok(Self {
            name: raw.name,
            model: raw.model,
            access: raw.access,
            frontend: raw.frontend,
            publication: raw.publication,
            storage_profile: raw.storage_profile.into(),
            staging: raw.staging,
            read_only: raw.read_only.unwrap_or(raw.model == Model::Direct),
            service_address: raw.service_address,
        })
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct StorageCapabilities {
    pub scheme: String,
    pub stat: bool,
    pub read: bool,
    pub list: bool,
    pub list_with_recursive: bool,
    pub read_with_version: bool,
    pub read_with_if_match: bool,
    pub write: bool,
    pub write_can_multi: bool,
    pub write_can_empty: bool,
    pub write_with_if_match: bool,
    pub write_with_if_not_exists: bool,
    pub delete: bool,
    pub delete_with_if_match: bool,
}

impl StorageCapabilities {
    fn inspect(operator: &Operator) -> Self {
        let capability = operator.info().capability();
        Self {
            scheme: operator.info().scheme().to_string(),
            stat: capability.stat,
            read: capability.read,
            list: capability.list,
            list_with_recursive: capability.list_with_recursive,
            read_with_version: capability.read_with_version,
            read_with_if_match: capability.read_with_if_match,
            write: capability.write,
            write_can_multi: capability.write_can_multi,
            write_can_empty: capability.write_can_empty,
            write_with_if_match: capability.write_with_if_match,
            write_with_if_not_exists: capability.write_with_if_not_exists,
            delete: capability.delete,
            delete_with_if_match: capability.delete_with_if_match,
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct Capabilities {
    pub admitted: CapabilitySet,
    pub storage: StorageCapabilities,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum StorageRequirement {
    ManagedObject,
    ManagedService,
    DirectReadOnly,
    DirectWritable,
    DirectSync,
}

#[derive(Clone, Debug)]
pub struct Admission {
    capabilities: CapabilitySet,
    storage: StorageRequirement,
}

impl Admission {
    fn new(capabilities: CapabilitySet, storage: StorageRequirement) -> Self {
        Self {
            capabilities,
            storage,
        }
    }

    fn capabilities(&self, operator: &Operator) -> Result<Capabilities> {
        self.storage.validate(operator)?;
        Ok(Capabilities {
            admitted: self.capabilities.clone(),
            storage: StorageCapabilities::inspect(operator),
        })
    }
}

impl VolumeConfig {
    pub fn from_json(bytes: &[u8]) -> Result<Self> {
        serde_json::from_slice(bytes).map_err(|error| {
            Error::Core(yinyang_core::Error::new(
                CoreErrorKind::Invalid,
                "volume configuration",
                error.to_string(),
            ))
        })
    }
    pub fn validate(&self, operator: &Operator) -> Result<Capabilities> {
        admit(self)?.capabilities(operator)
    }
}

impl StorageRequirement {
    fn validate(self, operator: &Operator) -> Result<()> {
        if operator.info().scheme() != "s3" {
            return Err(Error::Core(yinyang_core::Error::new(
                CoreErrorKind::Unsupported,
                "volume admission",
                format!(
                    "volume storage requires scheme s3, not {}",
                    operator.info().scheme()
                ),
            )));
        }
        let capability = operator.info().capability();
        let mut missing = Vec::new();
        match self {
            Self::ManagedObject | Self::ManagedService => {
                for (supported, name) in [
                    (capability.read, "read"),
                    (capability.write, "write"),
                    (capability.write_can_multi, "multi-write"),
                    (capability.write_with_if_not_exists, "conditional-create"),
                ] {
                    if !supported {
                        missing.push(name);
                    }
                }
                if self == Self::ManagedObject && !capability.write_with_if_match {
                    missing.push("conditional-replace");
                }
            }
            Self::DirectReadOnly | Self::DirectWritable | Self::DirectSync => {
                direct::validate_operator(operator).map_err(direct_admission_error)?;
                if self != Self::DirectReadOnly {
                    for (supported, name) in [
                        (capability.write, "write"),
                        (capability.write_can_multi, "multi-write"),
                        (capability.write_can_empty, "empty-write"),
                        (capability.write_with_if_match, "conditional-replace"),
                        (capability.write_with_if_not_exists, "conditional-create"),
                    ] {
                        if !supported {
                            missing.push(name);
                        }
                    }
                }
                if self == Self::DirectSync {
                    for (supported, name) in [
                        (capability.delete, "delete"),
                        (capability.delete_with_if_match, "conditional-delete"),
                    ] {
                        if !supported {
                            missing.push(name);
                        }
                    }
                }
            }
        }
        if missing.is_empty() {
            Ok(())
        } else {
            Err(Error::Core(yinyang_core::Error::new(
                CoreErrorKind::Unsupported,
                "volume admission",
                format!(
                    "storage is missing required capabilities: {}",
                    missing.join(", ")
                ),
            )))
        }
    }
}

/// The single admission table for every volume combination.
pub fn admit(config: &VolumeConfig) -> Result<Admission> {
    if config.name.trim().is_empty() {
        return Err(invalid_admission("volume name is required"));
    }
    let has_staging = !config.staging.as_os_str().is_empty();
    match (config.model, config.read_only, has_staging) {
        (Model::Managed, _, false) => {
            return Err(invalid_admission(
                "the staging path is required for a Managed volume",
            ));
        }
        (Model::Direct, true, true) => {
            return Err(invalid_admission(
                "read-only Direct volumes do not use staging; omit the staging field",
            ));
        }
        (Model::Direct, false, false) => {
            return Err(invalid_admission(
                "writable Direct volumes require a staging path",
            ));
        }
        _ => {}
    }
    let storage = match config.model {
        Model::Managed => match (config.publication, config.service_address) {
            (Publication::Object, None) => StorageRequirement::ManagedObject,
            (Publication::Service, Some(address)) if address.ip().is_loopback() => {
                StorageRequirement::ManagedService
            }
            (Publication::Service, Some(_)) => {
                return Err(invalid_admission(
                    "service publication requires a loopback service_address",
                ));
            }
            (Publication::Service, None) => {
                return Err(invalid_admission(
                    "service publication requires service_address",
                ));
            }
            (Publication::Object, Some(_)) => {
                return Err(invalid_admission(
                    "object publication must omit service_address",
                ));
            }
        },
        Model::Direct => {
            if config.publication != Publication::Object || config.service_address.is_some() {
                return Err(invalid_admission(
                    "Direct reads and mutates the configured object namespace directly and does not use a publication service",
                ));
            }
            match (config.access, config.read_only) {
                (Access::Mount, true) => StorageRequirement::DirectReadOnly,
                (Access::Mount, false) => StorageRequirement::DirectWritable,
                (Access::Sync, false) => StorageRequirement::DirectSync,
                (Access::Sync, true) => StorageRequirement::DirectWritable,
            }
        }
    };
    let capabilities = match (
        config.model,
        config.access,
        config.frontend,
        config.read_only,
    ) {
        (Model::Managed, Access::Mount, Frontend::Library, false)
        | (Model::Managed, Access::Mount, Frontend::Fuse, false) => {
            require_linux_if_fuse(config.frontend)?;
            managed()
        }
        (Model::Managed, Access::Mount, Frontend::Library, true) => read_only(),
        (Model::Managed, Access::Mount, Frontend::Fuse, true) => {
            require_linux_if_fuse(config.frontend)?;
            fuse()
        }
        (Model::Managed, Access::Sync, Frontend::Library, false) => managed(),
        (Model::Managed, Access::Sync, _, true) => {
            return Err(invalid_admission(
                "managed sync is bidirectional; read_only must be false for access=sync",
            ));
        }
        (Model::Managed, Access::Sync, _, false) => {
            return Err(invalid_admission(
                "managed sync uses the library frontend, not an OS mount frontend",
            ));
        }
        (Model::Direct, Access::Mount, Frontend::Library, true) => direct_read_only(),
        (Model::Direct, Access::Mount, Frontend::Fuse, _) => {
            require_linux_if_fuse(config.frontend)?;
            direct_capabilities(config.read_only)
        }
        (Model::Direct, Access::Mount, Frontend::Library, false) => {
            return Err(invalid_admission(
                "writable Direct mount uses the fuse frontend; the library frontend remains read-only",
            ));
        }
        (Model::Direct, Access::Sync, Frontend::Library, false) => {
            if !cfg!(target_os = "linux") {
                return Err(invalid_admission(
                    "Direct Sync is implemented only on Linux",
                ));
            }
            direct_writable()
        }
        (Model::Direct, Access::Sync, _, true) => {
            return Err(invalid_admission(
                "Direct Sync is bidirectional; read_only must be false",
            ));
        }
        (Model::Direct, Access::Sync, _, false) => {
            return Err(invalid_admission(
                "Direct Sync uses the library frontend, not an OS mount frontend",
            ));
        }
    };
    Ok(Admission::new(capabilities, storage))
}

fn require_linux_if_fuse(frontend: Frontend) -> Result<()> {
    if frontend == Frontend::Fuse && !cfg!(target_os = "linux") {
        return Err(invalid_admission(
            "the fuse frontend is implemented only on Linux",
        ));
    }
    Ok(())
}

fn invalid_admission(message: &'static str) -> Error {
    Error::Core(yinyang_core::Error::new(
        CoreErrorKind::Invalid,
        "volume admission",
        message,
    ))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum VolumeLoadStage {
    Configuration,
    Operator,
    Admission,
}

#[derive(Debug)]
pub struct VolumeLoadError {
    stage: VolumeLoadStage,
    kind: ErrorKind,
    message: String,
    config: Option<VolumeConfig>,
}

impl VolumeLoadError {
    fn new(
        stage: VolumeLoadStage,
        kind: ErrorKind,
        message: impl Into<String>,
        config: Option<VolumeConfig>,
    ) -> Self {
        Self {
            stage,
            kind,
            message: message.into(),
            config,
        }
    }

    fn from_runtime(stage: VolumeLoadStage, error: Error, config: Option<&VolumeConfig>) -> Self {
        Self::new(stage, error.kind(), error.to_string(), config.cloned())
    }

    pub const fn stage(&self) -> VolumeLoadStage {
        self.stage
    }

    pub const fn kind(&self) -> ErrorKind {
        self.kind
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub fn config(&self) -> Option<&VolumeConfig> {
        self.config.as_ref()
    }
}

impl std::fmt::Display for VolumeLoadError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for VolumeLoadError {}

pub struct VolumeContext {
    pub path: PathBuf,
    pub config: VolumeConfig,
    pub capabilities: Capabilities,
    operator: Operator,
    service_token: Option<String>,
}

impl VolumeContext {
    /// Load, resolve, admit, and build the storage connection before any
    /// command-specific side effect.
    pub async fn load(path: &Path) -> std::result::Result<Self, VolumeLoadError> {
        let config = Self::load_config(path).await?;
        let admission = admit(&config).map_err(|error| {
            VolumeLoadError::from_runtime(VolumeLoadStage::Admission, error, Some(&config))
        })?;
        let connection = storage_connection().map_err(|error| {
            VolumeLoadError::from_runtime(VolumeLoadStage::Operator, error, Some(&config))
        })?;
        let capabilities = admission
            .capabilities(&connection.operator)
            .map_err(|error| {
                VolumeLoadError::from_runtime(VolumeLoadStage::Admission, error, Some(&config))
            })?;
        Ok(Self {
            path: path.to_owned(),
            config,
            capabilities,
            operator: connection.operator,
            service_token: connection.service_token,
        })
    }

    async fn load_config(path: &Path) -> std::result::Result<VolumeConfig, VolumeLoadError> {
        let bytes = tokio::fs::read(path).await.map_err(|error| {
            VolumeLoadError::new(
                VolumeLoadStage::Configuration,
                kind_from_io(&error),
                format!("read volume configuration {}: {error}", path.display()),
                None,
            )
        })?;
        let mut config = VolumeConfig::from_json(&bytes).map_err(|error| {
            VolumeLoadError::from_runtime(VolumeLoadStage::Configuration, error, None)
        })?;
        if config.staging.is_relative() && !config.staging.as_os_str().is_empty() {
            config.staging = path
                .parent()
                .unwrap_or(Path::new("."))
                .join(&config.staging);
        }
        Ok(config)
    }

    pub fn operator(&self) -> &Operator {
        &self.operator
    }

    pub fn service_token(&self) -> Option<String> {
        self.service_token.clone()
    }

    pub async fn connect_authority(&self) -> Result<Arc<dyn Authority>> {
        Volume::connect(
            &self.config,
            self.operator.clone(),
            self.service_token.clone(),
        )
        .await
    }

    pub fn direct_namespace(&self) -> Result<DirectNamespace> {
        direct_namespace(&self.config, self.operator.clone())
    }

    pub fn direct_storage_binding(&self) -> Result<DirectStorageBinding> {
        direct_storage_binding(&self.config, &self.operator)
    }

    pub async fn open_file_volume(&self) -> Result<Volume> {
        Volume::open(
            self.config.clone(),
            self.operator.clone(),
            self.service_token.clone(),
        )
        .await
    }
}

pub struct StorageConnection {
    operator: Operator,
    service_token: Option<String>,
}

impl StorageConnection {
    pub fn operator(&self) -> &Operator {
        &self.operator
    }

    pub fn service_token(&self) -> Option<&str> {
        self.service_token.as_deref()
    }

    pub fn into_parts(self) -> (Operator, Option<String>) {
        (self.operator, self.service_token)
    }
}

/// Build the one storage operator and read the service token once.
pub fn storage_connection() -> Result<StorageConnection> {
    let config = std::env::vars()
        .filter_map(|(key, value)| {
            key.strip_prefix("YINYANG_S3_")
                .map(|key| (key.to_ascii_lowercase(), value))
        })
        .collect::<Vec<_>>();
    let operator = Operator::via_iter("s3", config).map_err(operator_error)?;
    #[cfg(feature = "benchmark-telemetry")]
    let operator = crate::benchmark::instrument(operator);
    Ok(StorageConnection {
        operator,
        service_token: std::env::var("YINYANG_SERVICE_TOKEN").ok(),
    })
}

pub fn storage_profile_from_env() -> Result<BackendProfile> {
    match std::env::var("YINYANG_STORAGE_PROFILE").as_deref() {
        Ok("amazon-s3") => Ok(BackendProfile::AmazonS3),
        Ok("minio") => Ok(BackendProfile::Minio),
        Err(_) if std::env::var_os("YINYANG_S3_ENDPOINT").is_none() => Ok(BackendProfile::AmazonS3),
        _ => Err(Error::Core(yinyang_core::Error::new(
            CoreErrorKind::Invalid,
            "storage profile",
            "set YINYANG_STORAGE_PROFILE to amazon-s3 or minio for the configured endpoint; other S3-compatible deployments are unsupported",
        ))),
    }
}

fn operator_error(error: opendal::Error) -> Error {
    let kind = match error.kind() {
        opendal::ErrorKind::ConfigInvalid => CoreErrorKind::Invalid,
        opendal::ErrorKind::PermissionDenied => CoreErrorKind::PermissionDenied,
        opendal::ErrorKind::Unsupported => CoreErrorKind::Unsupported,
        _ => CoreErrorKind::Storage,
    };
    Error::Core(yinyang_core::Error::new(
        kind,
        "storage operator",
        error.to_string(),
    ))
}

fn kind_from_io(error: &std::io::Error) -> ErrorKind {
    match error.kind() {
        std::io::ErrorKind::NotFound => ErrorKind::NotFound,
        std::io::ErrorKind::AlreadyExists => ErrorKind::AlreadyExists,
        std::io::ErrorKind::PermissionDenied => ErrorKind::PermissionDenied,
        _ => ErrorKind::Io,
    }
}

pub struct Volume {
    pub config: VolumeConfig,
    pub capabilities: Capabilities,
    runtime: Runtime,
}
impl Volume {
    /// Opens an existing authority. Preflight rejects unsupported combinations
    /// before acquiring a staging lease or creating local state.
    pub async fn open(
        config: VolumeConfig,
        operator: Operator,
        token: Option<String>,
    ) -> Result<Self> {
        if config.model == Model::Direct {
            return Err(Error::Invalid(
                "Direct volumes use Direct Mount or Sync entry points; they do not install Managed file handles",
            ));
        }
        // The file runtime installs recoverable handles; it does not own the
        // ordinary-directory reconciliation of a Sync volume. Admission accepts
        // `access: sync` for `Volume::connect`, so reject it here before this
        // opener creates any staging or reads storage.
        if config.access == Access::Sync {
            return Err(Error::Invalid(
                "managed sync volumes are operated through the sync frontend, not the file runtime",
            ));
        }
        let capabilities = config.validate(&operator)?;
        let authority = Self::connect(&config, operator, token).await?;
        let runtime = if config.read_only {
            Runtime::open_read_only(authority, &config.staging).await?
        } else {
            Runtime::open(authority, &config.staging).await?
        };
        Ok(Self {
            config,
            capabilities,
            runtime,
        })
    }
    /// Read-only authority connection, without initializing staging state.
    pub async fn connect(
        config: &VolumeConfig,
        operator: Operator,
        token: Option<String>,
    ) -> Result<Arc<dyn Authority>> {
        config.validate(&operator)?;
        if config.model == Model::Direct {
            return Err(Error::Invalid(
                "Direct volumes do not connect to a Managed authority",
            ));
        }
        let authority: Arc<dyn Authority> = match config.publication {
            Publication::Object => Arc::new(Fs::open(operator, config.storage_profile).await?),
            Publication::Service => {
                let token = token.ok_or_else(|| {
                    Error::Core(yinyang_core::Error::new(
                        CoreErrorKind::PermissionDenied,
                        "connect service",
                        "YINYANG_SERVICE_TOKEN is required",
                    ))
                })?;
                if !(32..=1024).contains(&token.len()) {
                    return Err(Error::Core(yinyang_core::Error::new(
                        CoreErrorKind::PermissionDenied,
                        "connect service",
                        "YINYANG_SERVICE_TOKEN must contain 32..=1024 bytes",
                    )));
                }
                Arc::new(
                    ServiceClient::connect(config.service_address.unwrap(), token, operator)
                        .await?,
                )
            }
        };
        Ok(authority)
    }
    pub fn runtime(&self) -> &Runtime {
        &self.runtime
    }
}

/// Construct the storage-native namespace for an admitted Direct volume.
pub fn direct_namespace(config: &VolumeConfig, operator: Operator) -> Result<DirectNamespace> {
    config.validate(&operator)?;
    if config.model != Model::Direct {
        return Err(Error::Invalid("volume model is not direct"));
    }
    DirectNamespace::new(operator).map_err(direct_admission_error)
}

/// Build the one non-secret storage binding shared by writable Direct Mount
/// and Direct Sync. The endpoint comes from the existing operator construction
/// environment rather than a second configuration field.
pub fn direct_storage_binding(
    config: &VolumeConfig,
    operator: &Operator,
) -> Result<DirectStorageBinding> {
    if config.model != Model::Direct {
        return Err(Error::Invalid(
            "a Direct storage binding requires a direct volume",
        ));
    }
    let endpoint = match std::env::var_os("YINYANG_S3_ENDPOINT") {
        Some(value) => value
            .into_string()
            .map_err(|_| Error::Invalid("YINYANG_S3_ENDPOINT is not valid UTF-8"))?,
        None => "provider-default".to_owned(),
    };
    let profile = backend_profile_name(config.storage_profile);
    DirectStorageBinding::new(
        operator.info().scheme(),
        profile,
        operator.info().name(),
        operator.info().root(),
        endpoint,
    )
    .map_err(direct_admission_error)
}

fn path_is_empty(path: &Path) -> bool {
    path.as_os_str().is_empty()
}

fn direct_admission_error(error: direct::Error) -> Error {
    let kind = match error.kind() {
        direct::ErrorKind::Unsupported => CoreErrorKind::Unsupported,
        _ => CoreErrorKind::Invalid,
    };
    Error::Core(yinyang_core::Error::new(
        kind,
        "direct admission",
        error.to_string(),
    ))
}
