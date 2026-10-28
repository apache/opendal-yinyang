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

#[path = "compatibility/mod.rs"]
mod compatibility;
#[path = "support/process.rs"]
mod process;
#[path = "support/raw_s3.rs"]
mod raw_s3;

use compatibility::fixtures::{EXPECTED_PRODUCER, fixture_root};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;

#[test]
fn compatibility_fixture_manifest_is_immutable() {
    let root = fixture_root();
    let manifest_path = root.join("manifest.json");
    let manifest: Value = serde_json::from_slice(
        &fs::read(&manifest_path)
            .unwrap_or_else(|error| panic!("read {}: {error}", manifest_path.display())),
    )
    .expect("parse compatibility fixture manifest");
    assert_eq!(manifest["schema"], "yinyang.compatibility.fixtures.v1");
    assert_eq!(manifest["producer"]["commit"], EXPECTED_PRODUCER);
    assert!(
        manifest["producer"]["binary_sha256"]
            .as_str()
            .is_some_and(|hash| hash.len() == 64),
        "fixture manifest must retain the producer binary hash: {manifest:#}"
    );
    for component in ["object", "service", "sync"] {
        assert!(
            root.join(component).is_dir(),
            "fixture component {component} is missing"
        );
    }

    let mut listed = BTreeMap::new();
    for entry in manifest["files"].as_array().expect("manifest files array") {
        let relative = entry["path"].as_str().expect("fixture path");
        assert!(
            !relative.starts_with('/')
                && !relative
                    .split('/')
                    .any(|part| part == ".." || part.is_empty()),
            "fixture path must be normalized and relative: {relative:?}"
        );
        let length = entry["length"].as_u64().expect("fixture length");
        let sha256 = entry["sha256"].as_str().expect("fixture SHA-256");
        assert_eq!(sha256.len(), 64, "fixture SHA-256 length");
        assert!(
            listed
                .insert(relative.to_owned(), (length, sha256.to_owned()))
                .is_none(),
            "fixture manifest contains duplicate path {relative:?}"
        );
    }

    let mut actual = BTreeSet::new();
    collect_files(&root, &root, &mut actual);
    actual.remove("manifest.json");
    actual.remove("README.md");
    assert_eq!(
        listed.keys().cloned().collect::<BTreeSet<_>>(),
        actual,
        "fixture directory and immutable manifest file list disagree"
    );
    for (relative, (expected_length, expected_hash)) in listed {
        let path = root.join(&relative);
        let bytes = fs::read(&path).unwrap_or_else(|error| panic!("read {relative}: {error}"));
        assert_eq!(
            bytes.len() as u64,
            expected_length,
            "fixture length changed for {relative}"
        );
        assert_eq!(
            Sha256::digest(&bytes)
                .as_slice()
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>(),
            expected_hash,
            "fixture bytes changed for {relative}"
        );
    }
}

fn collect_files(root: &std::path::Path, path: &std::path::Path, output: &mut BTreeSet<String>) {
    for entry in
        fs::read_dir(path).unwrap_or_else(|error| panic!("read {}: {error}", path.display()))
    {
        let entry = entry.expect("read fixture entry");
        let child = entry.path();
        if entry.file_type().expect("fixture entry type").is_dir() {
            collect_files(root, &child, output);
        } else {
            output.insert(
                child
                    .strip_prefix(root)
                    .expect("fixture path is relative")
                    .to_str()
                    .expect("fixture path is UTF-8")
                    .replace('\\', "/"),
            );
        }
    }
}
