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

//! Real process and storage helpers for cross-version acceptance.

use crate::process::{Captured, run_timeout};
use crate::raw_s3::{RawInventory, RawS3};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::fs;
use std::io::{BufRead, BufReader, Read};
use std::net::{SocketAddr, TcpListener};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use super::fixtures::fixture_root;

pub const SERVICE_TOKEN: &str = "compatibility-fixture-service-token-with-at-least-32-bytes";

pub fn legacy_binary() -> PathBuf {
    let path = std::env::var_os("YY_LEGACY_BIN")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            panic!("YY_LEGACY_BIN is required; cross-version acceptance must fail rather than skip")
        });
    assert!(
        path.is_file(),
        "YY_LEGACY_BIN does not name a file: {}",
        path.display()
    );
    path
}

pub fn current_binary() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_yy"))
}

pub fn expected(component: &str) -> Value {
    let path = fixture_root().join(component).join("expected.json");
    serde_json::from_slice(
        &fs::read(&path).unwrap_or_else(|error| panic!("read {}: {error}", path.display())),
    )
    .unwrap_or_else(|error| panic!("parse {}: {error}", path.display()))
}

pub struct Materialized {
    pub temp: tempfile::TempDir,
    pub root: String,
    raw: RawS3,
}

impl Materialized {
    pub fn component(component: &str) -> Self {
        let raw = RawS3::from_env();
        let root = format!(
            "compat-runtime-{component}-{}-{}",
            std::process::id(),
            uuid::Uuid::new_v4().simple()
        );
        let storage = fixture_root().join(component).join("storage");
        materialize_tree(&storage, &storage, &raw, &root);
        Self {
            temp: tempfile::tempdir().expect("create compatibility runtime directory"),
            root,
            raw,
        }
    }

    pub fn path(&self, name: &str) -> PathBuf {
        self.temp.path().join(name)
    }

    pub fn copy_fixture(&self, component: &str, relative: &str, destination: &Path) {
        copy_tree(&fixture_root().join(component).join(relative), destination);
    }

    pub fn inventory(&self) -> RawInventory {
        self.raw.inventory(&format!("{}/", self.root))
    }

    pub fn raw_get(&self, relative: &str) -> Option<Vec<u8>> {
        self.raw.get(&format!("{}/{relative}", self.root))
    }

    pub fn raw_put(&self, relative: &str, bytes: &[u8]) {
        let key = format!("{}/{relative}", self.root);
        self.raw.put(&key, bytes);
        assert_eq!(self.raw.get(&key).as_deref(), Some(bytes));
    }

    pub fn command(&self, binary: &Path, args: &[&str]) -> Command {
        let mut command = Command::new(binary);
        command.args(args);
        for name in [
            "YINYANG_S3_BUCKET",
            "YINYANG_S3_ENDPOINT",
            "YINYANG_S3_REGION",
            "YINYANG_S3_ACCESS_KEY_ID",
            "YINYANG_S3_SECRET_ACCESS_KEY",
        ] {
            command.env(
                name,
                std::env::var(name)
                    .unwrap_or_else(|_| panic!("{name} is required for compatibility acceptance")),
            );
        }
        command.env("YINYANG_S3_ROOT", &self.root);
        command.env("YINYANG_STORAGE_PROFILE", "minio");
        command.env("YINYANG_SERVICE_TOKEN", SERVICE_TOKEN);
        command
    }

    pub fn run(&self, binary: &Path, args: &[&str]) -> Captured {
        run_timeout(self.command(binary, args), Duration::from_secs(120))
    }
}

impl Drop for Materialized {
    fn drop(&mut self) {
        let prefix = format!("{}/", self.root);
        let inventory = self.raw.inventory(&prefix);
        for key in inventory.into_keys() {
            self.raw.delete(&key);
        }
    }
}

pub fn write_config(
    path: &Path,
    name: &str,
    access: &str,
    staging: &Path,
    publication: &str,
    service: Option<SocketAddr>,
) {
    let mut document = serde_json::json!({
        "name": name,
        "model": "managed",
        "access": access,
        "frontend": "library",
        "publication": publication,
        "storage_profile": "minio",
        "staging": staging,
        "read_only": false,
        "require": [],
    });
    if let Some(service) = service {
        document["service_address"] = Value::String(service.to_string());
    }
    fs::write(path, serde_json::to_vec_pretty(&document).unwrap()).expect("write volume config");
}

pub fn rewrite_staging(source: &Path, destination: &Path, staging: &Path) {
    let mut value: Value = serde_json::from_slice(&fs::read(source).unwrap()).unwrap();
    value["staging"] = Value::String(staging.display().to_string());
    fs::write(destination, serde_json::to_vec_pretty(&value).unwrap()).unwrap();
}

pub fn assert_tree_matches(root: &Path, expected: &Value) {
    let expected = expected.as_array().expect("expected tree array");
    let mut expected_paths = BTreeSet::new();
    for entry in expected {
        let path = entry["path"].as_str().expect("expected path");
        expected_paths.insert(path.to_owned());
        let actual = root.join(path);
        match entry["kind"].as_str().expect("expected kind") {
            "directory" => assert!(
                actual.is_dir(),
                "{} must be restored as a directory",
                actual.display()
            ),
            "file" => {
                let metadata =
                    fs::metadata(&actual).unwrap_or_else(|error| panic!("stat {path}: {error}"));
                assert!(metadata.is_file(), "{path} must be restored as a file");
                assert_eq!(
                    metadata.len(),
                    entry["length"].as_u64().expect("file length"),
                    "restored length for {path}"
                );
                let bytes = fs::read(&actual).unwrap();
                assert_eq!(
                    format!("{:x}", Sha256::digest(&bytes)),
                    entry["sha256"].as_str().expect("file hash"),
                    "restored bytes for {path}"
                );
                assert_eq!(
                    metadata.permissions().mode() & 0o100 != 0,
                    entry["executable"].as_bool().expect("executable marker"),
                    "restored executable bit for {path}"
                );
            }
            kind => panic!("unexpected expected tree kind {kind}"),
        }
    }
    let mut actual_paths = BTreeSet::new();
    collect_paths(root, root, &mut actual_paths);
    assert_eq!(
        actual_paths, expected_paths,
        "restored tree paths differ from fd1338c expectation"
    );
}

pub fn copy_tree(source: &Path, destination: &Path) {
    if source.is_dir() {
        fs::create_dir_all(destination)
            .unwrap_or_else(|error| panic!("create {}: {error}", destination.display()));
        fs::set_permissions(destination, fs::metadata(source).unwrap().permissions()).unwrap();
        for entry in fs::read_dir(source).unwrap() {
            let entry = entry.unwrap();
            copy_tree(&entry.path(), &destination.join(entry.file_name()));
        }
    } else {
        if let Some(parent) = destination.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        fs::copy(source, destination).unwrap_or_else(|error| {
            panic!(
                "copy {} to {}: {error}",
                source.display(),
                destination.display()
            )
        });
        fs::set_permissions(destination, fs::metadata(source).unwrap().permissions()).unwrap();
    }
}

pub fn tree_files(root: &Path) -> Vec<(String, Vec<u8>)> {
    let mut files = Vec::new();
    collect_files(root, root, &mut files);
    files.sort_by(|left, right| left.0.cmp(&right.0));
    files
}

pub struct Service {
    child: Option<Child>,
    stderr: Option<JoinHandle<Vec<u8>>>,
    pub address: SocketAddr,
}

impl Service {
    pub fn start(materialized: &Materialized, binary: &Path, database: &Path) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("reserve service test port");
        let address = listener.local_addr().unwrap();
        drop(listener);
        let mut command = materialized.command(
            binary,
            &[
                "serve",
                "--database",
                database.to_str().expect("database path is UTF-8"),
                "--listen",
                &address.to_string(),
            ],
        );
        command.stdout(Stdio::piped()).stderr(Stdio::piped());
        let mut child = command.spawn().expect("spawn metadata service");
        let stdout = child.stdout.take().unwrap();
        let stderr = child.stderr.take().unwrap();
        let stderr = thread::spawn(move || {
            let mut reader = BufReader::new(stderr);
            let mut bytes = Vec::new();
            let _ = reader.read_to_end(&mut bytes);
            bytes
        });
        let mut line = String::new();
        BufReader::new(stdout)
            .read_line(&mut line)
            .expect("read service readiness");
        assert!(
            line.starts_with("metadata service listening on "),
            "unexpected service readiness line: {line:?}"
        );
        Self {
            child: Some(child),
            stderr: Some(stderr),
            address,
        }
    }

    pub fn stop(mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
        if let Some(stderr) = self.stderr.take() {
            let _ = stderr.join();
        }
    }
}

impl Drop for Service {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
        if let Some(stderr) = self.stderr.take() {
            let _ = stderr.join();
        }
    }
}

pub fn wait_for(path: &Path, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    while !path.exists() {
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {}",
            path.display()
        );
        thread::sleep(Duration::from_millis(25));
    }
}

fn materialize_tree(source: &Path, root: &Path, raw: &RawS3, prefix: &str) {
    for entry in
        fs::read_dir(source).unwrap_or_else(|error| panic!("read {}: {error}", source.display()))
    {
        let entry = entry.unwrap();
        let path = entry.path();
        if path.is_dir() {
            materialize_tree(&path, root, raw, prefix);
        } else {
            let relative = path.strip_prefix(root).unwrap().to_str().unwrap();
            let key = format!("{prefix}/{relative}");
            let bytes = fs::read(&path).unwrap();
            raw.put(&key, &bytes);
            assert_eq!(raw.get(&key).as_deref(), Some(bytes.as_slice()));
        }
    }
}

fn collect_paths(root: &Path, path: &Path, output: &mut BTreeSet<String>) {
    for entry in fs::read_dir(path).unwrap() {
        let entry = entry.unwrap();
        let child = entry.path();
        output.insert(
            child
                .strip_prefix(root)
                .unwrap()
                .to_str()
                .unwrap()
                .to_owned(),
        );
        if child.is_dir() {
            collect_paths(root, &child, output);
        }
    }
}

fn collect_files(root: &Path, path: &Path, output: &mut Vec<(String, Vec<u8>)>) {
    for entry in fs::read_dir(path).unwrap() {
        let entry = entry.unwrap();
        let child = entry.path();
        if child.is_dir() {
            collect_files(root, &child, output);
        } else {
            output.push((
                child
                    .strip_prefix(root)
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .to_owned(),
                fs::read(child).unwrap(),
            ));
        }
    }
}
