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

//! CLI admission and error behavior for `yy ... sync`.
//!
//! The exact status schema is exercised end to end by the Managed Sync
//! acceptance suite; these tests only pin the frontend's admission rules and
//! exit-code contract so they fail fast without storage.

#![cfg(target_os = "linux")]

use std::path::{Path, PathBuf};
use std::process::{Command, Output};

fn yy(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_yy"))
        .args(args)
        // Dummy S3 settings are enough for admission: capability negotiation
        // reads declared operator capabilities and never contacts storage.
        .env("YINYANG_S3_BUCKET", "cli-admission")
        .env("YINYANG_S3_REGION", "us-east-1")
        .env("YINYANG_S3_ENDPOINT", "http://127.0.0.1:9")
        .env("YINYANG_STORAGE_PROFILE", "minio")
        .output()
        .expect("run yy")
}

fn write_config(dir: &Path, name: &str, model: &str, access: &str) -> PathBuf {
    let staging = dir.join(format!("{name}-state"));
    let config = dir.join(format!("{name}.json"));
    let value = serde_json::json!({
        "name": name,
        "model": model,
        "access": access,
        "frontend": "library",
        "publication": "object",
        "storage_profile": "minio",
        "staging": staging,
        "read_only": false,
        "require": [],
    });
    std::fs::write(&config, serde_json::to_vec_pretty(&value).unwrap()).unwrap();
    config
}

fn assert_stderr(output: &Output, needle: &str) {
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains(needle),
        "stderr did not contain {needle:?}: {stderr}"
    );
}

#[test]
fn sync_requires_a_volume() {
    let output = yy(&["sync", "status"]);
    assert!(!output.status.success());
    assert_stderr(&output, "requires --volume CONFIG.json");
}

#[test]
fn sync_rejects_a_mount_volume() {
    let temp = tempfile::tempdir().unwrap();
    let config = write_config(temp.path(), "mount", "managed", "mount");
    let output = yy(&["--volume", config.to_str().unwrap(), "sync", "status"]);
    assert!(!output.status.success());
    assert_stderr(&output, "requires a Managed volume with access=sync");
}

#[test]
fn sync_rejects_a_direct_volume() {
    let temp = tempfile::tempdir().unwrap();
    let config = write_config(temp.path(), "direct", "direct", "sync");
    let output = yy(&["--volume", config.to_str().unwrap(), "sync", "status"]);
    assert!(!output.status.success());
    assert_stderr(&output, "requires a Managed volume with access=sync");
}

#[test]
fn sync_rejects_a_read_only_volume() {
    let temp = tempfile::tempdir().unwrap();
    let config = temp.path().join("read-only.json");
    let value = serde_json::json!({
        "name": "read-only",
        "model": "managed",
        "access": "sync",
        "frontend": "library",
        "publication": "object",
        "storage_profile": "minio",
        "staging": temp.path().join("read-only-state"),
        "read_only": true,
        "require": [],
    });
    std::fs::write(&config, serde_json::to_vec_pretty(&value).unwrap()).unwrap();
    let output = yy(&["--volume", config.to_str().unwrap(), "sync", "status"]);
    assert!(!output.status.success());
    assert_stderr(&output, "read_only must be false");
}

#[test]
fn unbound_status_is_schema_valid_and_exits_three() {
    let temp = tempfile::tempdir().unwrap();
    let config = write_config(temp.path(), "unbound", "managed", "sync");
    let output = yy(&[
        "--volume",
        config.to_str().unwrap(),
        "sync",
        "status",
        "--json",
    ]);
    assert_eq!(output.status.code(), Some(3));
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["schema"], "yinyang.sync.status.v1");
    assert_eq!(value["binding"]["state"], "unbound");
}

#[test]
fn export_requires_a_selector() {
    let temp = tempfile::tempdir().unwrap();
    let config = write_config(temp.path(), "export", "managed", "sync");
    let output_path = temp.path().join("out");
    let output = yy(&[
        "--volume",
        config.to_str().unwrap(),
        "sync",
        "export",
        "--output",
        output_path.to_str().unwrap(),
    ]);
    assert!(!output.status.success());
    assert_stderr(&output, "requires exactly one of --artifact or --conflict");
}

#[test]
fn resolve_requires_a_decision() {
    let temp = tempfile::tempdir().unwrap();
    let config = write_config(temp.path(), "resolve", "managed", "sync");
    let output = yy(&[
        "--volume",
        config.to_str().unwrap(),
        "sync",
        "resolve",
        "deadbeef",
    ]);
    assert!(!output.status.success());
    assert_stderr(&output, "requires exactly one of --keep or --content");
}
