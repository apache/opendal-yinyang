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

//! Accepted writable Direct admission and native mutation scenarios.

use crate::harness::{
    Fixture, RequestCounter, effective_capabilities, local_paths, path_text, run_timeout,
};
use std::collections::BTreeSet;
use std::time::Duration;

const REQUIRED_WRITABLE_CAPABILITIES: [&str; 8] = [
    "read",
    "list",
    "write",
    "append",
    "truncate",
    "pinned-reads",
    "remote-fsync",
    "recoverable-staging",
];

const FORBIDDEN_DIRECT_CAPABILITIES: [&str; 3] =
    ["stable-identity", "atomic-rename", "retained-unlink-read"];

fn assert_writable_capabilities(capabilities: &BTreeSet<String>) {
    for required in REQUIRED_WRITABLE_CAPABILITIES {
        assert!(
            capabilities.contains(required),
            "writable Direct capabilities omitted {required:?}: {capabilities:?}"
        );
    }
    for forbidden in FORBIDDEN_DIRECT_CAPABILITIES {
        assert!(
            !capabilities.contains(forbidden),
            "writable Direct capabilities unexpectedly claimed {forbidden:?}: {capabilities:?}"
        );
    }
}

#[test]
#[ignore = "requires isolated S3; run with cargo x test-linux-direct"]
fn direct_write_admission_requires_staging_and_publishes_limited_capabilities() {
    let fixture = Fixture::new();
    fixture.write_raw("seed.txt", b"seed");
    let initial_inventory = fixture.inventory();
    let staging = fixture.path("direct-write-state");
    let config = fixture.writable_config("direct-write", "fuse", &staging);
    let local_before = local_paths(fixture.temp.path());

    let output = fixture.yy_ok(&["--volume", path_text(&config), "capabilities"]);
    let capabilities = effective_capabilities(&output.stdout);
    assert_writable_capabilities(&capabilities);

    assert!(
        !staging.exists(),
        "writable Direct capabilities created the configured staging path"
    );
    assert_eq!(
        local_paths(fixture.temp.path()),
        local_before,
        "writable Direct capabilities created local state"
    );
    assert_eq!(
        fixture.inventory(),
        initial_inventory,
        "writable Direct capabilities mutated raw storage"
    );
    assert!(
        fixture
            .inventory()
            .keys()
            .all(|key| !key.starts_with(".yinyang/")),
        "writable Direct admission created Managed metadata"
    );

    let missing_staging = fixture.config("direct-write-missing-staging", "fuse", Some(false), None);
    let requests = RequestCounter::start();
    let mut command =
        fixture.yy_command(&["--volume", path_text(&missing_staging), "capabilities"]);
    command.env("YINYANG_S3_ENDPOINT", requests.endpoint());
    let rejected = run_timeout(command, Duration::from_secs(30));
    assert!(
        !rejected.status.success(),
        "writable Direct accepted a configuration without staging"
    );
    assert!(
        rejected.text().to_ascii_lowercase().contains("staging"),
        "writable Direct staging rejection did not identify the field:\n{}",
        rejected.text()
    );
    requests.assert_no_requests("writable Direct missing staging");
    assert!(
        !staging.exists(),
        "missing-staging admission created another Direct state path"
    );
    assert_eq!(
        fixture.inventory(),
        initial_inventory,
        "missing-staging admission mutated raw storage"
    );
}
