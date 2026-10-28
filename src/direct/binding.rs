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

use super::{Error, ErrorKind, Result};

const BINDING_DOMAIN: &[u8] = b"yinyang direct storage binding v1";

/// The non-secret storage namespace identity for durable Direct state.
///
/// Decode and mount admission construct this value from configuration before
/// opening a writable runtime. It must not contain credentials or URL userinfo.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DirectStorageBinding {
    scheme: String,
    profile: String,
    bucket: String,
    root: String,
    endpoint: String,
    digest: [u8; 32],
}

impl DirectStorageBinding {
    /// Build one canonical binding from the selected non-secret configuration.
    pub fn new(
        scheme: impl Into<String>,
        profile: impl Into<String>,
        bucket: impl Into<String>,
        root: impl Into<String>,
        endpoint: impl Into<String>,
    ) -> Result<Self> {
        let scheme = scheme.into();
        let profile = profile.into();
        let bucket = bucket.into();
        let root = root.into();
        let endpoint = endpoint.into();
        if scheme != "s3" {
            return Err(Error::new(
                ErrorKind::Invalid,
                format!("Direct storage binding requires scheme s3, not {scheme:?}"),
            ));
        }
        if !matches!(profile.as_str(), "amazon-s3" | "minio") {
            return Err(Error::new(
                ErrorKind::Invalid,
                format!("Direct storage binding has unsupported profile {profile:?}"),
            ));
        }
        if bucket.is_empty() || bucket.contains('/') {
            return Err(Error::new(
                ErrorKind::Invalid,
                format!("Direct storage binding has invalid bucket {bucket:?}"),
            ));
        }
        if root.contains('\0') || root.contains('\n') || root.contains('\r') {
            return Err(Error::new(
                ErrorKind::Invalid,
                "Direct storage binding root contains a control character",
            ));
        }
        let root = format!("/{}", root.trim_matches('/'));
        if endpoint.is_empty()
            || endpoint.contains('\0')
            || endpoint.contains('\n')
            || endpoint.contains('\r')
        {
            return Err(Error::new(
                ErrorKind::Invalid,
                "Direct storage binding endpoint is empty or contains a control character",
            ));
        }
        if endpoint_has_userinfo(&endpoint) {
            return Err(Error::new(
                ErrorKind::Invalid,
                "Direct storage binding endpoint must not contain URL userinfo",
            ));
        }
        let endpoint = endpoint.trim_end_matches('/').to_owned();
        if endpoint.is_empty() {
            return Err(Error::new(
                ErrorKind::Invalid,
                "Direct storage binding endpoint has no authority",
            ));
        }

        let mut hasher = blake3::Hasher::new();
        hasher.update(BINDING_DOMAIN);
        for value in [&scheme, &profile, &bucket, &root, &endpoint] {
            hasher.update(&(value.len() as u64).to_be_bytes());
            hasher.update(value.as_bytes());
        }
        let mut digest = [0_u8; 32];
        digest.copy_from_slice(hasher.finalize().as_bytes());
        Ok(Self {
            scheme,
            profile,
            bucket,
            root,
            endpoint,
            digest,
        })
    }

    pub fn scheme(&self) -> &str {
        &self.scheme
    }

    pub fn profile(&self) -> &str {
        &self.profile
    }

    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    pub fn root(&self) -> &str {
        &self.root
    }

    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    pub(super) fn digest(&self) -> [u8; 32] {
        self.digest
    }

    pub(super) fn diagnostic(&self) -> String {
        format!(
            "scheme={} profile={} bucket={} root={} endpoint={}",
            self.scheme, self.profile, self.bucket, self.root, self.endpoint
        )
    }
}

fn endpoint_has_userinfo(endpoint: &str) -> bool {
    let authority = endpoint
        .split_once("://")
        .map_or(endpoint, |(_, authority)| authority)
        .split('/')
        .next()
        .unwrap_or_default();
    authority.contains('@')
}
