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

//! Storage fixture values shared by the real-process acceptance suites.

pub const STORAGE_VARIABLES: [&str; 5] = [
    "YINYANG_S3_BUCKET",
    "YINYANG_S3_ENDPOINT",
    "YINYANG_S3_REGION",
    "YINYANG_S3_ACCESS_KEY_ID",
    "YINYANG_S3_SECRET_ACCESS_KEY",
];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Publication {
    Object,
    Service,
}

impl Publication {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Object => "object",
            Self::Service => "service",
        }
    }
}

/// Validate the storage variables before an acceptance fixture creates state.
pub fn require_storage_environment(
    profile: &str,
    allow_missing_amazon_s3_endpoint: bool,
    context: &str,
) {
    for name in STORAGE_VARIABLES {
        if allow_missing_amazon_s3_endpoint
            && profile == "amazon-s3"
            && name == "YINYANG_S3_ENDPOINT"
            && std::env::var_os(name).is_none()
        {
            continue;
        }
        assert!(
            std::env::var_os(name).is_some(),
            "{name} is unset; {context} fails instead of skipping"
        );
    }
}

/// Build the S3 environment passed to a `yy` child process.
pub fn storage_environment(
    profile: &str,
    root: &str,
    token: &str,
    endpoint_override: Option<&str>,
    allow_missing_amazon_s3_endpoint: bool,
) -> Vec<(&'static str, String)> {
    let mut environment = Vec::with_capacity(STORAGE_VARIABLES.len() + 3);
    for name in STORAGE_VARIABLES {
        if name == "YINYANG_S3_ENDPOINT" {
            if let Some(endpoint) = endpoint_override {
                environment.push((name, endpoint.to_owned()));
                continue;
            }
            if allow_missing_amazon_s3_endpoint
                && profile == "amazon-s3"
                && std::env::var_os(name).is_none()
            {
                continue;
            }
        }
        environment.push((
            name,
            std::env::var(name)
                .unwrap_or_else(|_| panic!("{name} disappeared after storage preflight")),
        ));
    }
    environment.push(("YINYANG_STORAGE_PROFILE", profile.to_owned()));
    environment.push(("YINYANG_S3_ROOT", root.to_owned()));
    environment.push(("YINYANG_SERVICE_TOKEN", token.to_owned()));
    environment
}
