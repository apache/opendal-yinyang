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

//! Volume configuration and effective capability admission, before runtime exposure.
use crate::direct::{self, DirectNamespace, DirectStorageBinding};
use crate::runtime::{Error, Result, Runtime};
use opendal::Operator;
use serde::{Deserialize, Deserializer, Serialize};
use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use yinyang_core::service::ServiceClient;
use yinyang_core::{Authority, BackendProfile, ErrorKind as CoreErrorKind, Fs};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
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
    Macos,
    Windows,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Publication {
    Object,
    Service,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum StorageProfile {
    AmazonS3,
    Minio,
}
impl StorageProfile {
    fn backend(self) -> BackendProfile {
        match self {
            Self::AmazonS3 => BackendProfile::AmazonS3,
            Self::Minio => BackendProfile::Minio,
        }
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
    pub storage_profile: StorageProfile,
    #[serde(skip_serializing_if = "path_is_empty")]
    pub staging: PathBuf,
    #[serde(default)]
    pub read_only: bool,
    #[serde(default)]
    pub require: CapabilitySet,
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
    storage_profile: StorageProfile,
    #[serde(default)]
    staging: PathBuf,
    #[serde(default)]
    read_only: Option<bool>,
    #[serde(default)]
    require: CapabilitySet,
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
            storage_profile: raw.storage_profile,
            staging: raw.staging,
            read_only: raw.read_only.unwrap_or(raw.model == Model::Direct),
            require: raw.require,
            service_address: raw.service_address,
        })
    }
}
#[derive(Clone, Debug, Serialize)]
pub struct Capabilities {
    pub volume: CapabilitySet,
    pub storage: CapabilitySet,
    pub access: CapabilitySet,
    pub frontend: CapabilitySet,
    pub effective: CapabilitySet,
}
impl Capabilities {
    pub fn intersect(
        volume: CapabilitySet,
        storage: CapabilitySet,
        access: CapabilitySet,
        frontend: CapabilitySet,
    ) -> Self {
        let effective = volume
            .iter()
            .filter(|c| storage.contains(c) && access.contains(c) && frontend.contains(c))
            .copied()
            .collect();
        Self {
            volume,
            storage,
            access,
            frontend,
            effective,
        }
    }
}
impl VolumeConfig {
    pub fn from_json(bytes: &[u8]) -> Result<Self> {
        serde_json::from_slice(bytes)
            .map_err(|e| Error::local(format!("volume configuration: {e}")))
    }
    /// Negotiation is read-only. Storage capabilities are semantic prerequisites
    /// for Managed operations, not claims that raw S3 implements atomic rename.
    pub fn capabilities(&self, operator: &Operator) -> Capabilities {
        let volume = self.volume_capabilities();
        let cap = operator.info().capability();
        let storage = match self.model {
            Model::Managed => {
                let content = operator.info().scheme() == "s3"
                    && cap.read
                    && cap.write
                    && cap.write_can_multi
                    && cap.write_with_if_not_exists;
                let publication =
                    self.publication == Publication::Service || cap.write_with_if_match;
                if content && publication {
                    managed()
                } else {
                    CapabilitySet::new()
                }
            }
            Model::Direct => {
                let reads = operator.info().scheme() == "s3"
                    && cap.stat
                    && cap.read
                    && cap.list
                    && cap.list_with_recursive
                    && cap.read_with_version
                    && cap.read_with_if_match;
                let writes = self.read_only
                    || (cap.write
                        && cap.write_can_multi
                        && cap.write_can_empty
                        && cap.write_with_if_match
                        && cap.write_with_if_not_exists);
                let deletion =
                    self.access != Access::Sync || (cap.delete && cap.delete_with_if_match);
                if reads && writes && deletion {
                    self.volume_capabilities()
                } else {
                    CapabilitySet::new()
                }
            }
        };
        let access = self.access_capabilities();
        let frontend = self.frontend_capabilities();
        Capabilities::intersect(volume, storage, access, frontend)
    }
    fn volume_capabilities(&self) -> CapabilitySet {
        match self.model {
            Model::Managed => managed(),
            Model::Direct => direct_capabilities(self.read_only),
        }
    }
    fn access_capabilities(&self) -> CapabilitySet {
        match (self.model, self.access) {
            (Model::Direct, Access::Mount) if self.read_only => direct_read_only(),
            (Model::Direct, Access::Mount) => direct_writable(),
            (Model::Direct, Access::Sync) => direct_writable(),
            (Model::Managed, Access::Mount) if self.read_only => read_only(),
            (Model::Managed, Access::Mount | Access::Sync) => managed(),
        }
    }
    fn frontend_capabilities(&self) -> CapabilitySet {
        match (self.model, self.frontend) {
            (Model::Direct, Frontend::Library) if self.read_only || self.access == Access::Sync => {
                self.volume_capabilities()
            }
            (Model::Direct, Frontend::Library) => CapabilitySet::new(),
            (Model::Direct, Frontend::Fuse) if self.access == Access::Mount => {
                self.volume_capabilities()
            }
            (Model::Direct, Frontend::Fuse | Frontend::Macos | Frontend::Windows) => {
                CapabilitySet::new()
            }
            (Model::Managed, Frontend::Library) => managed(),
            (Model::Managed, Frontend::Fuse) if self.read_only => fuse(),
            (Model::Managed, Frontend::Fuse) => managed(),
            (Model::Managed, Frontend::Macos | Frontend::Windows) => CapabilitySet::new(),
        }
    }
    /// Capabilities the selected combination promises independent of storage
    /// support. Startup requires this intersection to survive storage and
    /// frontend negotiation, so a frontend cannot silently weaken the model.
    fn baseline(&self) -> CapabilitySet {
        let volume = self.volume_capabilities();
        let access = self.access_capabilities();
        let frontend = self.frontend_capabilities();
        volume
            .intersection(&access)
            .filter(|capability| frontend.contains(capability))
            .copied()
            .collect()
    }
    pub fn validate(&self, operator: &Operator) -> Result<Capabilities> {
        if self.name.trim().is_empty() {
            return Err(Error::Invalid("volume name is required"));
        }
        if self.model == Model::Managed && self.staging.as_os_str().is_empty() {
            return Err(Error::Invalid(
                "the staging path is required for a Managed volume",
            ));
        }
        if self.model == Model::Direct {
            if self.read_only && !self.staging.as_os_str().is_empty() {
                return Err(Error::Invalid(
                    "read-only Direct volumes do not use staging; omit the staging field",
                ));
            }
            if !self.read_only && self.staging.as_os_str().is_empty() {
                return Err(Error::Invalid(
                    "writable Direct volumes require a staging path",
                ));
            }
            if self.publication != Publication::Object || self.service_address.is_some() {
                return Err(Error::Invalid(
                    "Direct reads and mutates the configured object namespace directly and does not use a publication service",
                ));
            }
            match self.access {
                Access::Mount => match self.frontend {
                    Frontend::Library if self.read_only => {}
                    Frontend::Library => {
                        return Err(Error::Invalid(
                            "writable Direct mount uses the fuse frontend; the library frontend remains read-only",
                        ));
                    }
                    Frontend::Fuse if !cfg!(target_os = "linux") => {
                        return Err(Error::Invalid(
                            "the fuse frontend is implemented only on Linux",
                        ));
                    }
                    Frontend::Fuse => {}
                    Frontend::Macos | Frontend::Windows => {
                        return Err(Error::Invalid(
                            "macOS and Windows frontends are not implemented; select library or fuse",
                        ));
                    }
                },
                Access::Sync => {
                    if !cfg!(target_os = "linux") {
                        return Err(Error::Invalid("Direct Sync is implemented only on Linux"));
                    }
                    if self.read_only {
                        return Err(Error::Invalid(
                            "Direct Sync is bidirectional; read_only must be false",
                        ));
                    }
                    if self.frontend != Frontend::Library {
                        return Err(Error::Invalid(
                            "Direct Sync uses the library frontend, not an OS mount frontend",
                        ));
                    }
                }
            }
            direct::validate_operator(operator).map_err(direct_admission_error)?;
            let caps = self.capabilities(operator);
            if !self.baseline().is_subset(&caps.effective)
                || !self.require.is_subset(&caps.effective)
            {
                return Err(Error::Invalid(
                    "effective Direct capability intersection is missing baseline or required operations",
                ));
            }
            return Ok(caps);
        }
        match self.access {
            Access::Mount => match self.frontend {
                Frontend::Library => {}
                Frontend::Fuse if !cfg!(target_os = "linux") => {
                    return Err(Error::Invalid(
                        "the fuse frontend is implemented only on Linux",
                    ));
                }
                Frontend::Fuse => {}
                Frontend::Macos | Frontend::Windows => {
                    return Err(Error::Invalid(
                        "macOS and Windows frontends are not implemented; select library or fuse",
                    ));
                }
            },
            Access::Sync => {
                // Managed Sync installs an ordinary local directory itself; it
                // never projects through an OS mount frontend.
                if self.frontend != Frontend::Library {
                    return Err(Error::Invalid(
                        "managed sync uses the library frontend, not an OS mount frontend",
                    ));
                }
                // The frontend implements bidirectional reconciliation. A
                // read-only flag would be silently ignored while the volume
                // still published writes, so refuse it explicitly.
                if self.read_only {
                    return Err(Error::Invalid(
                        "managed sync is bidirectional; read_only must be false for access=sync",
                    ));
                }
            }
        }
        match (self.publication, self.service_address) {
            (Publication::Service, Some(addr)) if addr.ip().is_loopback() => {}
            (Publication::Object, None) => {}
            _ => {
                return Err(Error::Invalid(
                    "service publication requires a loopback address; object publication must omit it",
                ));
            }
        }
        let caps = self.capabilities(operator);
        let baseline = self.baseline();
        if !baseline.is_subset(&caps.effective) || !self.require.is_subset(&caps.effective) {
            return Err(Error::Invalid(
                "effective capability intersection is missing baseline or required operations",
            ));
        }
        Ok(caps)
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
            Publication::Object => {
                Arc::new(Fs::open(operator, config.storage_profile.backend()).await?)
            }
            Publication::Service => Arc::new(
                ServiceClient::connect(
                    config.service_address.unwrap(),
                    token.ok_or(Error::Invalid("YINYANG_SERVICE_TOKEN is required"))?,
                    operator,
                )
                .await?,
            ),
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
    let profile = match config.storage_profile {
        StorageProfile::AmazonS3 => "amazon-s3",
        StorageProfile::Minio => "minio",
    };
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
