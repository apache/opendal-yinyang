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

//! Amazon S3 and Linux FUSE qualification for read-only Direct.
//!
//! Fixture writes use the official AWS CLI outside the mount. Only the
//! explicitly mounted `yy` process exercises the Direct implementation.

use crate::harness::{Captured, Mounted, assert_errno, path_text, run_timeout};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Duration;
use tempfile::{NamedTempFile, TempDir};

const REGION: &str = "YINYANG_QUALIFICATION_REGION";
const UNVERSIONED_BUCKET: &str = "YINYANG_DIRECT_UNVERSIONED_BUCKET";
const VERSIONED_BUCKET: &str = "YINYANG_DIRECT_VERSIONED_BUCKET";
const PAGE_SIZE: usize = 37;
const ENOENT: i32 = 2;
const EROFS: i32 = 30;
const ESTALE: i32 = 116;

fn require_real_s3_environment() {
    if !Path::new("/dev/fuse").exists() {
        panic!("/dev/fuse is unavailable; Amazon S3 Direct acceptance must fail rather than skip");
    }
    for tool in ["fusermount3", "timeout"] {
        if Command::new(tool).arg("--version").output().is_err() {
            panic!("{tool} is unavailable; Amazon S3 Direct acceptance must fail rather than skip");
        }
    }
    for name in [
        UNVERSIONED_BUCKET,
        VERSIONED_BUCKET,
        REGION,
        "YINYANG_S3_ACCESS_KEY_ID",
        "YINYANG_S3_SECRET_ACCESS_KEY",
        "YINYANG_S3_SESSION_TOKEN",
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SESSION_TOKEN",
    ] {
        if std::env::var_os(name).is_none() {
            panic!("{name} is unset; Amazon S3 Direct acceptance must fail rather than skip");
        }
    }
    for name in [
        "YINYANG_S3_ENDPOINT",
        "AWS_ENDPOINT_URL",
        "AWS_ENDPOINT_URL_S3",
    ] {
        assert!(
            std::env::var_os(name).is_none(),
            "{name} must be unset for qualification against the provider default endpoint"
        );
    }
    let unversioned = required(UNVERSIONED_BUCKET);
    let versioned = required(VERSIONED_BUCKET);
    assert_ne!(
        unversioned, versioned,
        "versioned and unversioned qualification buckets must be distinct"
    );
    let aws = aws_executable();
    let output = run_timeout(
        {
            let mut command = Command::new(&aws);
            command.arg("--version");
            command
        },
        Duration::from_secs(30),
    );
    output.success_or("AWS CLI version probe");
}

fn required(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("{name} is required"))
}

fn aws_executable() -> String {
    std::env::var("AWS_CLI_BIN").unwrap_or_else(|_| "aws".to_owned())
}

#[derive(Clone)]
struct AwsS3 {
    bucket: String,
    region: String,
    executable: String,
}

impl AwsS3 {
    fn new(bucket: impl Into<String>) -> Self {
        Self {
            bucket: bucket.into(),
            region: required(REGION),
            executable: aws_executable(),
        }
    }

    fn command(&self) -> Command {
        let mut command = Command::new(&self.executable);
        command
            .env("AWS_REGION", &self.region)
            .env("AWS_DEFAULT_REGION", &self.region)
            .env("AWS_EC2_METADATA_DISABLED", "true")
            .env("AWS_PAGER", "")
            .env_remove("AWS_ENDPOINT_URL")
            .env_remove("AWS_ENDPOINT_URL_S3");
        command
    }

    fn s3api_command(&self, operation: &str, args: &[String]) -> Command {
        let mut command = self.command();
        command
            .args(["s3api", operation, "--bucket", &self.bucket])
            .args([
                "--region",
                &self.region,
                "--no-cli-pager",
                "--output",
                "json",
            ])
            .args(args);
        command
    }

    fn s3api(&self, operation: &str, args: &[String], timeout: Duration) -> Captured {
        let mut command = self.s3api_command(operation, args);
        command.stdin(Stdio::null());
        run_timeout(command, timeout)
    }

    fn s3api_json(&self, operation: &str, args: &[String]) -> Value {
        let output = self.s3api(operation, args, Duration::from_secs(180));
        output.success_or(&format!("AWS CLI s3api {operation}"));
        serde_json::from_slice(&output.stdout).unwrap_or_else(|error| {
            panic!(
                "AWS CLI s3api {operation} returned invalid JSON: {error}\n{}",
                output.text()
            )
        })
    }

    fn versioning(&self) -> Value {
        self.s3api_json("get-bucket-versioning", &[])
    }

    fn head(&self, key: &str) -> Option<ObjectMetadata> {
        let output = self.s3api(
            "head-object",
            &["--key".to_owned(), key.to_owned()],
            Duration::from_secs(60),
        );
        if !output.status.success() {
            let text = output.text();
            if text.contains("404") || text.contains("Not Found") || text.contains("NoSuchKey") {
                return None;
            }
            panic!("AWS CLI head-object {key:?} failed:\n{text}");
        }
        let value: Value = serde_json::from_slice(&output.stdout).expect("HEAD JSON");
        Some(ObjectMetadata {
            etag: value["ETag"].as_str().map(str::to_owned),
            version: value["VersionId"]
                .as_str()
                .filter(|version| !version.is_empty() && *version != "null")
                .map(str::to_owned),
        })
    }

    fn put(
        &self,
        key: &str,
        bytes: &[u8],
        if_match: Option<&str>,
        if_none_match: Option<&str>,
    ) -> Captured {
        let mut body = NamedTempFile::new().expect("create S3 PUT body");
        body.write_all(bytes).expect("write S3 PUT body");
        body.flush().expect("flush S3 PUT body");
        let command = self.put_command(key, body.path(), if_match, if_none_match);
        run_timeout(command, Duration::from_secs(180))
    }

    fn put_command(
        &self,
        key: &str,
        body: &Path,
        if_match: Option<&str>,
        if_none_match: Option<&str>,
    ) -> Command {
        let mut args = vec![
            "--key".to_owned(),
            key.to_owned(),
            "--body".to_owned(),
            body.to_str().expect("S3 PUT body path is UTF-8").to_owned(),
        ];
        if let Some(if_match) = if_match {
            args.push("--if-match".to_owned());
            args.push(if_match.to_owned());
        }
        if let Some(if_none_match) = if_none_match {
            args.push("--if-none-match".to_owned());
            args.push(if_none_match.to_owned());
        }
        self.s3api_command("put-object", &args)
    }

    fn put_ok(&self, key: &str, bytes: &[u8], if_match: Option<&str>, if_none_match: Option<&str>) {
        self.put(key, bytes, if_match, if_none_match)
            .success_or(&format!("conditional AWS CLI PUT for {key:?}"));
    }

    fn delete(&self, key: &str, if_match: Option<&str>, version: Option<&str>) -> Captured {
        let mut args = vec!["--key".to_owned(), key.to_owned()];
        if let Some(if_match) = if_match {
            args.push("--if-match".to_owned());
            args.push(if_match.to_owned());
        }
        if let Some(version) = version {
            args.push("--version-id".to_owned());
            args.push(version.to_owned());
        }
        self.s3api("delete-object", &args, Duration::from_secs(180))
    }

    fn get(&self, key: &str, version: Option<&str>, if_match: Option<&str>) -> (Vec<u8>, Value) {
        let output_file = NamedTempFile::new().expect("create S3 GET output");
        let mut args = vec![
            "--key".to_owned(),
            key.to_owned(),
            "--outfile".to_owned(),
            output_file
                .path()
                .to_str()
                .expect("S3 GET output path is UTF-8")
                .to_owned(),
        ];
        if let Some(version) = version {
            args.push("--version-id".to_owned());
            args.push(version.to_owned());
        }
        if let Some(if_match) = if_match {
            args.push("--if-match".to_owned());
            args.push(if_match.to_owned());
        }
        let output = self.s3api("get-object", &args, Duration::from_secs(180));
        output.success_or(&format!("AWS CLI exact GET for {key:?}"));
        let value: Value = serde_json::from_slice(&output.stdout).expect("GET JSON");
        let bytes = fs::read(output_file.path()).expect("read AWS CLI GET output");
        (bytes, value)
    }

    fn list(&self, prefix: &str, max_keys: usize) -> RawListing {
        let mut continuation: Option<String> = None;
        let mut entries = BTreeMap::new();
        let mut page_sizes = Vec::new();
        loop {
            let mut args = vec![
                "--prefix".to_owned(),
                prefix.to_owned(),
                "--max-keys".to_owned(),
                max_keys.to_string(),
            ];
            if let Some(continuation) = &continuation {
                args.push("--continuation-token".to_owned());
                args.push(continuation.clone());
            }
            let value = self.s3api_json("list-objects-v2", &args);
            let contents = value["Contents"].as_array().cloned().unwrap_or_default();
            page_sizes.push(contents.len());
            for object in contents {
                let key = object["Key"].as_str().expect("listed key is a string");
                let entry = RawListEntry {
                    etag: object["ETag"].as_str().map(str::to_owned),
                    size: object["Size"].as_u64().expect("listed size"),
                    last_modified: object["LastModified"].as_str().map(str::to_owned),
                };
                assert!(
                    entries.insert(key.to_owned(), entry).is_none(),
                    "ListObjectsV2 returned duplicate key {key:?}"
                );
            }
            continuation = value["NextContinuationToken"].as_str().map(str::to_owned);
            if !value["IsTruncated"].as_bool().unwrap_or(false) {
                break;
            }
            assert!(
                continuation.is_some(),
                "truncated ListObjectsV2 response omitted NextContinuationToken"
            );
        }
        RawListing {
            entries,
            page_sizes,
        }
    }

    fn versions(&self, prefix: &str) -> Vec<VersionEntry> {
        let mut key_marker: Option<String> = None;
        let mut version_marker: Option<String> = None;
        let mut versions = Vec::new();
        loop {
            let mut args = vec!["--prefix".to_owned(), prefix.to_owned()];
            if let Some(key_marker) = &key_marker {
                args.push("--key-marker".to_owned());
                args.push(key_marker.clone());
            }
            if let Some(version_marker) = &version_marker {
                args.push("--version-id-marker".to_owned());
                args.push(version_marker.clone());
            }
            let value = self.s3api_json("list-object-versions", &args);
            for version in value["Versions"].as_array().into_iter().flatten() {
                versions.push(VersionEntry {
                    key: version["Key"].as_str().expect("version key").to_owned(),
                    version: version["VersionId"]
                        .as_str()
                        .expect("version ID")
                        .to_owned(),
                    etag: version["ETag"].as_str().map(str::to_owned),
                    delete_marker: false,
                });
            }
            for marker in value["DeleteMarkers"].as_array().into_iter().flatten() {
                versions.push(VersionEntry {
                    key: marker["Key"]
                        .as_str()
                        .expect("delete marker key")
                        .to_owned(),
                    version: marker["VersionId"]
                        .as_str()
                        .expect("delete marker version ID")
                        .to_owned(),
                    etag: None,
                    delete_marker: true,
                });
            }
            if !value["IsTruncated"].as_bool().unwrap_or(false) {
                break;
            }
            key_marker = value["NextKeyMarker"].as_str().map(str::to_owned);
            version_marker = value["NextVersionIdMarker"].as_str().map(str::to_owned);
        }
        versions
    }

    fn copy_recursive(&self, source: &Path, destination: &str) {
        let destination = format!(
            "s3://{}/{}",
            self.bucket,
            destination.trim_start_matches('/')
        );
        let mut command = self.command();
        command
            .args(["s3", "cp", "--recursive"])
            .arg(source)
            .arg(&destination)
            .args(["--only-show-errors", "--no-progress"]);
        run_timeout(command, Duration::from_secs(900))
            .success_or("AWS CLI recursive fixture upload");
    }

    fn remove_prefix(&self, prefix: &str) {
        let destination = format!("s3://{}/{}", self.bucket, prefix.trim_end_matches('/'));
        let mut command = self.command();
        command
            .args(["s3", "rm", "--recursive", &destination])
            .arg("--only-show-errors");
        run_timeout(command, Duration::from_secs(900)).success_or("AWS CLI fixture cleanup");
    }
}

struct RawListing {
    entries: BTreeMap<String, RawListEntry>,
    page_sizes: Vec<usize>,
}

#[derive(Debug, Eq, PartialEq)]
struct Inventory {
    entries: BTreeMap<String, RawListEntry>,
    page_sizes: Vec<usize>,
}

impl std::ops::Deref for Inventory {
    type Target = BTreeMap<String, RawListEntry>;

    fn deref(&self) -> &Self::Target {
        &self.entries
    }
}

#[derive(Debug, Eq, PartialEq)]
struct RawListEntry {
    etag: Option<String>,
    size: u64,
    last_modified: Option<String>,
}

#[derive(Clone, Debug)]
struct ObjectMetadata {
    etag: Option<String>,
    version: Option<String>,
}

#[derive(Clone, Debug)]
struct VersionEntry {
    key: String,
    version: String,
    etag: Option<String>,
    delete_marker: bool,
}

struct Fixture {
    temp: TempDir,
    root: String,
    bucket: String,
    aws: AwsS3,
}

impl Fixture {
    fn unversioned() -> Self {
        Self::new(required(UNVERSIONED_BUCKET))
    }

    fn versioned() -> Self {
        Self::new(required(VERSIONED_BUCKET))
    }

    fn new(bucket: String) -> Self {
        require_real_s3_environment();
        let root = format!(
            "direct-amazon-s3-{}-{}",
            std::process::id(),
            uuid::Uuid::new_v4().simple()
        );
        Self {
            temp: tempfile::tempdir().expect("create Amazon S3 Direct test directory"),
            root,
            aws: AwsS3::new(bucket.clone()),
            bucket,
        }
    }

    fn full_key(&self, key: &str) -> String {
        format!("{}/{key}", self.root)
    }

    fn inventory(&self) -> Inventory {
        let prefix = format!("{}/", self.root);
        let listing = self.aws.list(&prefix, PAGE_SIZE);
        for (key, entry) in &listing.entries {
            assert!(
                key.starts_with(&prefix),
                "raw LIST escaped fixture prefix: {key:?}"
            );
            eprintln!(
                "raw LIST evidence: key={key:?} etag={:?} size={} last_modified={:?}",
                entry.etag, entry.size, entry.last_modified
            );
        }
        assert!(
            listing.page_sizes.len() > 1 || listing.entries.len() < PAGE_SIZE,
            "raw LIST did not exercise continuation for {prefix:?}"
        );
        Inventory {
            entries: listing
                .entries
                .into_iter()
                .map(|(key, entry)| (key[prefix.len()..].to_owned(), entry))
                .collect(),
            page_sizes: listing.page_sizes,
        }
    }

    fn config(&self, name: &str) -> PathBuf {
        let config = json!({
            "name": name,
            "model": "direct",
            "access": "mount",
            "frontend": "fuse",
            "publication": "object",
            "storage_profile": "amazon-s3",
            "read_only": true,
        });
        let path = self.temp.path().join(format!("{name}.json"));
        fs::write(
            &path,
            serde_json::to_vec_pretty(&config).expect("encode Amazon S3 Direct config"),
        )
        .expect("write Amazon S3 Direct config");
        path
    }

    fn yy_command(&self, args: &[&str]) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_yy"));
        command.args(args);
        command
            .env("YINYANG_STORAGE_PROFILE", "amazon-s3")
            .env("YINYANG_S3_BUCKET", &self.bucket)
            .env("YINYANG_S3_REGION", required(REGION))
            .env("YINYANG_S3_ROOT", &self.root)
            .env(
                "YINYANG_S3_ACCESS_KEY_ID",
                required("YINYANG_S3_ACCESS_KEY_ID"),
            )
            .env(
                "YINYANG_S3_SECRET_ACCESS_KEY",
                required("YINYANG_S3_SECRET_ACCESS_KEY"),
            )
            .env(
                "YINYANG_S3_SESSION_TOKEN",
                required("YINYANG_S3_SESSION_TOKEN"),
            )
            .env("AWS_EC2_METADATA_DISABLED", "true")
            .env_remove("YINYANG_S3_ENDPOINT")
            .env_remove("AWS_ENDPOINT_URL")
            .env_remove("AWS_ENDPOINT_URL_S3");
        command
    }

    fn spawn_mount(&self, config: &Path, mountpoint: &Path) -> Mounted {
        let command = self.yy_command(&[
            "--volume",
            path_text(config),
            "mount",
            path_text(mountpoint),
        ]);
        Mounted::spawn(command, mountpoint)
    }

    fn cleanup(&self) {
        self.aws.remove_prefix(&format!("{}/", self.root));
        assert!(
            self.inventory().is_empty(),
            "Amazon S3 fixture cleanup left visible objects under {}",
            self.root
        );
    }

    fn cleanup_versions(&self) {
        for version in self.aws.versions(&format!("{}/", self.root)) {
            self.aws
                .delete(&version.key, None, Some(&version.version))
                .success_or("AWS CLI fixture version cleanup");
        }
        assert!(
            self.aws.versions(&format!("{}/", self.root)).is_empty(),
            "Amazon S3 fixture cleanup left versions under {}",
            self.root
        );
        assert!(
            self.inventory().is_empty(),
            "Amazon S3 fixture cleanup left visible objects under {}",
            self.root
        );
    }
}

fn payload(length: usize, seed: u8) -> Vec<u8> {
    let mut value = Vec::with_capacity(length);
    let mut state = u64::from(seed) | 1;
    while value.len() < length {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        value.extend_from_slice(&state.to_le_bytes());
    }
    value.truncate(length);
    value
}

fn read_fully_at(file: &File, offset: u64, length: usize) -> std::io::Result<Vec<u8>> {
    let mut output = vec![0_u8; length];
    let mut read = 0;
    while read < output.len() {
        let count = file.read_at(&mut output[read..], offset + read as u64)?;
        if count == 0 {
            output.truncate(read);
            break;
        }
        read += count;
    }
    Ok(output)
}

fn write_fixture_file(directory: &Path, name: &str, bytes: &[u8]) {
    fs::write(directory.join(name), bytes).unwrap_or_else(|error| {
        panic!(
            "write fixture file {}: {error}",
            directory.join(name).display()
        )
    });
}

fn assert_conditional_failure(output: &Captured, context: &str) -> &'static str {
    let text = output.text();
    assert!(
        !output.status.success(),
        "{context} unexpectedly succeeded:\n{text}"
    );
    if text.contains("ConditionalRequestConflict") || text.contains("(409)") {
        "conditional-conflict"
    } else if text.contains("PreconditionFailed") || text.contains("(412)") {
        "precondition-failed"
    } else {
        panic!("{context} failed without a conditional error:\n{text}");
    }
}

fn assert_delete_conditional_failure(output: &Captured, context: &str) -> &'static str {
    let text = output.text();
    assert!(
        !output.status.success(),
        "{context} unexpectedly succeeded:\n{text}"
    );
    if text.contains("PreconditionFailed") || text.contains("(412)") {
        "precondition-failed"
    } else if text.contains("ConditionalRequestConflict") || text.contains("(409)") {
        "conditional-conflict"
    } else {
        panic!("{context} failed without a conditional error:\n{text}");
    }
}

fn find_encoded(directory: &Path, raw_component: &str) -> PathBuf {
    let matches = fs::read_dir(directory)
        .unwrap_or_else(|error| panic!("read encoded Direct directory: {error}"))
        .filter_map(Result::ok)
        .filter_map(|entry| entry.file_name().into_string().ok())
        .filter(|name| percent_decode(name) == raw_component)
        .collect::<Vec<_>>();
    assert_eq!(
        matches.len(),
        1,
        "raw component {raw_component:?} must map to exactly one visible name, got {matches:?}"
    );
    PathBuf::from(matches.into_iter().next().unwrap())
}

fn percent_decode(component: &str) -> String {
    let bytes = component.as_bytes();
    let mut output = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            assert!(
                index + 2 < bytes.len(),
                "Direct name contains a truncated percent escape: {component:?}"
            );
            output.push((hex(bytes[index + 1]) << 4) | hex(bytes[index + 2]));
            index += 3;
        } else {
            output.push(bytes[index]);
            index += 1;
        }
    }
    String::from_utf8(output).expect("Direct encoded name decodes to UTF-8")
}

fn hex(value: u8) -> u8 {
    match value {
        b'0'..=b'9' => value - b'0',
        b'a'..=b'f' => value - b'a' + 10,
        b'A'..=b'F' => value - b'A' + 10,
        _ => panic!("invalid percent escape"),
    }
}

#[test]
#[ignore = "requires two real Amazon S3 buckets; run with tests/linux_direct_real_s3.py"]
fn direct_amazon_s3_complete_pagination_reaches_real_fuse() {
    let fixture = Fixture::unversioned();
    assert_eq!(
        fixture.aws.versioning(),
        json!({}),
        "the unversioned qualification bucket must never have enabled versioning"
    );

    const OBJECTS: usize = 1005;
    let source = fixture.temp.path().join("pagination");
    fs::create_dir(&source).expect("create pagination fixture directory");
    let mut payloads = BTreeMap::new();
    for index in 0..OBJECTS {
        let name = format!("object-{index:04}.bin");
        let bytes = format!("pagination-value-{index}").into_bytes();
        write_fixture_file(&source, &name, &bytes);
        payloads.insert(name, bytes);
    }
    fixture
        .aws
        .copy_recursive(&source, &format!("{}/objects/", fixture.root));

    let initial = fixture.inventory();
    assert_eq!(
        initial.keys().cloned().collect::<Vec<_>>(),
        payloads
            .keys()
            .map(|name| format!("objects/{name}"))
            .collect::<Vec<_>>(),
        "raw paginated LIST did not return the exact seeded key set"
    );
    for key in ["object-0000.bin", "object-0500.bin", "object-1004.bin"] {
        let (bytes, metadata) =
            fixture
                .aws
                .get(&fixture.full_key(&format!("objects/{key}")), None, None);
        assert_eq!(
            bytes, payloads[key],
            "raw exact GET changed real Amazon S3 bytes for {key:?}"
        );
        assert_eq!(
            metadata["ContentLength"].as_u64(),
            Some(payloads[key].len() as u64)
        );
    }
    eprintln!(
        "real Amazon S3 pagination evidence: bucket={} root={} objects={} pages={} page_sizes={:?}",
        fixture.bucket,
        fixture.root,
        initial.len(),
        initial.page_sizes.len(),
        initial.page_sizes
    );

    let config = fixture.config("amazon-s3-pagination");
    let mountpoint = fixture.temp.path().join("mnt-pagination");
    fs::create_dir(&mountpoint).expect("create pagination mountpoint");
    let mounted = fixture.spawn_mount(&config, &mountpoint);
    let visible = fs::read_dir(mounted.mountpoint().join("objects"))
        .expect("enumerate real Amazon S3 FUSE directory")
        .map(|entry| entry.expect("FUSE directory entry"))
        .map(|entry| {
            entry
                .file_name()
                .into_string()
                .expect("FUSE directory name is UTF-8")
        })
        .collect::<BTreeSet<_>>();
    assert_eq!(
        visible,
        payloads.keys().cloned().collect::<BTreeSet<_>>(),
        "FUSE did not expose every key from the >1000-object paginated observation"
    );
    for key in ["object-0000.bin", "object-0500.bin", "object-1004.bin"] {
        assert_eq!(
            fs::read(mounted.mountpoint().join("objects").join(key))
                .unwrap_or_else(|error| panic!("read mounted {key:?}: {error}")),
            payloads[key],
            "FUSE changed bytes for {key:?}"
        );
    }
    mounted.terminate();

    assert_eq!(
        fixture.inventory(),
        initial,
        "the complete paginated read-only mount changed the raw Amazon S3 inventory"
    );
    fixture.cleanup();
}

#[test]
#[ignore = "requires two real Amazon S3 buckets; run with tests/linux_direct_real_s3.py"]
fn direct_amazon_s3_file_prefix_collision_on_later_page_rejects() {
    let fixture = Fixture::unversioned();
    let source = fixture.temp.path().join("collision");
    let nested = source.join("a");
    fs::create_dir_all(&nested).expect("create collision fixture directory");

    // These keys sort between `a` and `a/b`, placing `a/b` on a later LIST page.
    for index in 0..1000 {
        let name = format!("a-{index:04}.bin");
        write_fixture_file(&source, &name, b"ordinary");
    }
    fs::write(nested.join("b"), b"nested collision").expect("write a/b fixture");
    fixture
        .aws
        .copy_recursive(&source, &format!("{}/", fixture.root));
    fixture
        .aws
        .put_ok(&fixture.full_key("a"), b"file collision", None, None);

    let initial = fixture.inventory();
    assert!(
        initial.len() > 1000,
        "collision fixture must span more than one S3 LIST page"
    );
    assert!(
        initial.contains_key("a") && initial.contains_key("a/b"),
        "real S3 LIST must expose both adversarial keys: {:?}",
        initial.keys().collect::<Vec<_>>()
    );
    assert_eq!(
        initial.keys().next_back().map(String::as_str),
        Some("a/b"),
        "a/b must be ordered after filler keys so one page cannot discover the collision"
    );
    for (key, expected) in [
        ("a", b"file collision".as_slice()),
        ("a/b", b"nested collision"),
    ] {
        let (bytes, _) = fixture.aws.get(&fixture.full_key(key), None, None);
        assert_eq!(
            bytes, expected,
            "real S3 exact GET did not preserve collision key {key:?}"
        );
    }

    let config = fixture.config("amazon-s3-collision");
    let mountpoint = fixture.temp.path().join("mnt-collision");
    fs::create_dir(&mountpoint).expect("create collision mountpoint");
    let rejected = run_timeout(
        fixture.yy_command(&[
            "--volume",
            path_text(&config),
            "mount",
            path_text(&mountpoint),
        ]),
        Duration::from_secs(300),
    );
    assert!(
        !rejected.status.success(),
        "a complete real Amazon S3 observation accepted the file-prefix collision"
    );
    let diagnostic = rejected.text();
    assert!(
        diagnostic
            .to_ascii_lowercase()
            .contains("file-prefix collision"),
        "real Amazon S3 collision rejection omitted its invariant:\n{diagnostic}"
    );
    assert!(
        diagnostic.contains("a/b"),
        "real Amazon S3 collision rejection omitted the later-page key:\n{diagnostic}"
    );
    assert_eq!(
        fixture.inventory(),
        initial,
        "collision rejection changed the raw Amazon S3 inventory"
    );
    fixture.cleanup();
}

#[test]
#[ignore = "requires two real Amazon S3 buckets; run with tests/linux_direct_real_s3.py"]
fn direct_amazon_s3_reversible_keys_and_graceful_lifecycle() {
    let fixture = Fixture::unversioned();
    let cases = [
        ("plain.txt", b"plain".as_slice()),
        ("literal%.txt", b"percent"),
        ("literal%25.txt", b"percent-encoded-spelling"),
        ("control\u{7f}", b"control"),
        ("space name.txt", b"space"),
        ("Case.txt", b"uppercase"),
        ("case.txt", b"lowercase"),
        ("café", b"nfc"),
        ("cafe\u{301}", b"nfd"),
    ];
    for (key, bytes) in cases {
        let raw_key = fixture.full_key(key);
        fixture.aws.put_ok(&raw_key, bytes, None, None);
        let (actual, _) = fixture.aws.get(&raw_key, None, None);
        assert_eq!(
            actual, bytes,
            "real Amazon S3 exact GET did not preserve key {key:?}"
        );
    }
    let initial = fixture.inventory();

    let config = fixture.config("amazon-s3-keys");
    let first_mountpoint = fixture.temp.path().join("mnt-keys-first");
    fs::create_dir(&first_mountpoint).expect("create first key mountpoint");
    let first = fixture.spawn_mount(&config, &first_mountpoint);
    let root = first.mountpoint();
    assert_eq!(fs::read(root.join("plain.txt")).unwrap(), b"plain");
    assert_eq!(
        fs::read(root.join(find_encoded(root, "literal%.txt"))).unwrap(),
        b"percent"
    );
    assert_eq!(
        fs::read(root.join(find_encoded(root, "literal%25.txt"))).unwrap(),
        b"percent-encoded-spelling"
    );
    assert_eq!(
        fs::read(root.join(find_encoded(root, "control\u{7f}"))).unwrap(),
        b"control"
    );
    assert_eq!(fs::read(root.join("space name.txt")).unwrap(), b"space");
    assert_eq!(fs::read(root.join("Case.txt")).unwrap(), b"uppercase");
    assert_eq!(fs::read(root.join("case.txt")).unwrap(), b"lowercase");
    assert_eq!(fs::read(root.join("café")).unwrap(), b"nfc");
    assert_eq!(fs::read(root.join("cafe\u{301}")).unwrap(), b"nfd");
    let write = OpenOptions::new().write(true).open(root.join("plain.txt"));
    let error = write.expect_err("real Amazon S3 Direct mount must reject writes");
    assert_errno(&error, &[EROFS], "real Amazon S3 Direct writable open");
    first.terminate();
    assert_eq!(
        fixture.inventory(),
        initial,
        "SIGTERM teardown after real key reads changed Amazon S3 storage"
    );

    let second_mountpoint = fixture.temp.path().join("mnt-keys-second");
    fs::create_dir(&second_mountpoint).expect("create second key mountpoint");
    let second = fixture.spawn_mount(&config, &second_mountpoint);
    assert_eq!(
        fs::read(second.mountpoint().join("plain.txt")).unwrap(),
        b"plain"
    );
    second.unmount();
    assert_eq!(
        fixture.inventory(),
        initial,
        "normal unmount and remount changed Amazon S3 storage"
    );
    fixture.cleanup();
}

#[test]
#[ignore = "requires two real Amazon S3 buckets; run with tests/linux_direct_real_s3.py"]
fn direct_amazon_s3_version_ids_survive_replacement_and_delete_marker() {
    let fixture = Fixture::versioned();
    assert_eq!(
        fixture.aws.versioning(),
        json!({"Status": "Enabled"}),
        "the versioned qualification bucket must enable versioning"
    );
    let key = "versioned.bin";
    let raw_key = fixture.full_key(key);
    let old = payload(192 * 1024 + 29, 0x41);
    let new = payload(48 * 1024 + 17, 0x17);
    fixture.aws.put_ok(&raw_key, &old, None, None);
    let first = fixture
        .aws
        .head(&raw_key)
        .expect("first versioned object must exist");
    let first_version = first
        .version
        .as_deref()
        .expect("versioned PUT must return a real version ID");

    let config = fixture.config("amazon-s3-versioned");
    let mountpoint = fixture.temp.path().join("mnt-versioned");
    fs::create_dir(&mountpoint).expect("create versioned mountpoint");
    let mounted = fixture.spawn_mount(&config, &mountpoint);
    let path = mounted.mountpoint().join(key);
    let old_fd = File::open(&path).expect("open first real Amazon S3 version");
    assert_eq!(
        old_fd.metadata().expect("fstat first version").len(),
        old.len() as u64
    );
    assert_eq!(
        read_fully_at(&old_fd, 0, 4096).expect("read first version prefix"),
        old[..4096]
    );

    fixture.aws.put_ok(&raw_key, &new, None, None);
    let second = fixture.aws.head(&raw_key).expect("replacement must exist");
    let second_version = second
        .version
        .as_deref()
        .expect("replacement must return a real version ID");
    assert_ne!(
        first_version, second_version,
        "versioned replacement must allocate a new version ID"
    );
    let (retained, _) = fixture.aws.get(&raw_key, Some(first_version), None);
    assert_eq!(retained, old, "real S3 did not retain the first version");
    assert_eq!(
        read_fully_at(&old_fd, (old.len() - 4096) as u64, 4096)
            .expect("read retained version tail"),
        old[old.len() - 4096..],
        "an open Direct fd must retain its exact real S3 version"
    );
    let versions = fixture.aws.versions(&fixture.full_key(key));
    assert!(
        versions.iter().any(|version| {
            version.version == first_version && version.etag.is_some() && !version.delete_marker
        }),
        "ListObjectVersions did not report the retained first version: {versions:?}"
    );

    mounted.refresh();
    mounted.wait_for_log("refresh", Duration::from_secs(60));
    let mut fresh = File::open(&path).expect("open replacement after SIGHUP");
    let mut fresh_bytes = Vec::new();
    fresh
        .read_to_end(&mut fresh_bytes)
        .expect("read replacement after SIGHUP");
    assert_eq!(fresh_bytes, new);

    fixture
        .aws
        .delete(&raw_key, None, None)
        .success_or("AWS CLI delete current versioned key");
    assert!(
        fixture.aws.head(&raw_key).is_none(),
        "delete without a version ID must create a current delete marker"
    );
    let versions = fixture.aws.versions(&fixture.full_key(key));
    assert!(
        versions.iter().any(|version| version.delete_marker),
        "ListObjectVersions did not report the real delete marker: {versions:?}"
    );
    assert_eq!(
        read_fully_at(&old_fd, (old.len() - 4096) as u64, 4096)
            .expect("read retained version after delete marker"),
        old[old.len() - 4096..],
        "a delete marker must not replace an existing pinned version"
    );
    assert_eq!(
        old_fd
            .metadata()
            .expect("fstat retained version after delete marker")
            .len(),
        old.len() as u64
    );
    mounted.refresh();
    mounted.wait_for_log("refresh", Duration::from_secs(60));
    let error = File::open(&path).expect_err("delete marker must hide a fresh Direct open");
    assert_errno(&error, &[ENOENT, ESTALE], "fresh open after delete marker");
    drop(fresh);
    drop(old_fd);
    mounted.unmount();
    fixture.cleanup_versions();
}

#[test]
#[ignore = "requires two real Amazon S3 buckets; run with tests/linux_direct_real_s3.py"]
fn direct_amazon_s3_conditional_primitives_unversioned() {
    let fixture = Fixture::unversioned();
    assert_eq!(fixture.aws.versioning(), json!({}));
    let key = fixture.full_key("conditional.bin");
    let initial = payload(96 * 1024 + 11, 0x11);
    let replacement_a = payload(128 * 1024 + 13, 0x31);
    let replacement_b = payload(64 * 1024 + 19, 0x71);

    fixture
        .aws
        .put(&key, &initial, None, Some("*"))
        .success_or("If-None-Match create on a missing key");
    let initial_head = fixture.aws.head(&key).expect("conditional create");
    let initial_etag = initial_head.etag.as_deref().expect("initial ETag");
    assert!(initial_head.version.is_none());
    let duplicate = fixture.aws.put(&key, b"duplicate", None, Some("*"));
    let duplicate_error = assert_conditional_failure(&duplicate, "If-None-Match create");
    eprintln!("conditional create collision: {duplicate_error}");
    assert_eq!(fixture.aws.get(&key, None, None).0, initial);

    let stale = fixture
        .aws
        .put(&key, b"stale replacement", Some(initial_etag), None);
    let stale_error = assert_conditional_failure(&stale, "stale If-Match replacement");
    eprintln!("stale conditional PUT: {stale_error}");
    assert_eq!(
        fixture.aws.get(&key, None, Some(initial_etag)).0,
        initial,
        "a failed conditional PUT must leave the accepted bytes intact"
    );

    let mut body_a = NamedTempFile::new().expect("create race body A");
    let mut body_b = NamedTempFile::new().expect("create race body B");
    body_a.write_all(&replacement_a).expect("write race body A");
    body_b.write_all(&replacement_b).expect("write race body B");
    body_a.flush().expect("flush race body A");
    body_b.flush().expect("flush race body B");
    let (left, right) = std::thread::scope(|scope| {
        let left = scope.spawn(|| {
            run_timeout(
                fixture
                    .aws
                    .put_command(&key, body_a.path(), Some(initial_etag), None),
                Duration::from_secs(180),
            )
        });
        let right = scope.spawn(|| {
            run_timeout(
                fixture
                    .aws
                    .put_command(&key, body_b.path(), Some(initial_etag), None),
                Duration::from_secs(180),
            )
        });
        (left.join().unwrap(), right.join().unwrap())
    });
    assert_ne!(
        left.status.success(),
        right.status.success(),
        "exactly one concurrent If-Match replacement must commit:\nleft:{}\nright:{}",
        left.text(),
        right.text()
    );
    let race_error = if left.status.success() {
        assert_conditional_failure(&right, "losing concurrent If-Match replacement")
    } else {
        assert_conditional_failure(&left, "losing concurrent If-Match replacement")
    };
    eprintln!("concurrent If-Match replacement loser: {race_error}");
    let replacement = if left.status.success() {
        replacement_a.clone()
    } else {
        replacement_b.clone()
    };
    let replacement_head = fixture.aws.head(&key).expect("replacement object");
    let replacement_etag = replacement_head.etag.as_deref().expect("replacement ETag");
    assert_eq!(fixture.aws.get(&key, None, None).0, replacement);

    let stale_delete = fixture.aws.delete(&key, Some(initial_etag), None);
    let stale_delete_error =
        assert_delete_conditional_failure(&stale_delete, "stale conditional DELETE");
    eprintln!("stale conditional DELETE: {stale_delete_error}");
    assert_eq!(
        fixture.aws.get(&key, None, Some(replacement_etag)).0,
        replacement,
        "a stale conditional DELETE must leave the replacement intact"
    );

    fixture
        .aws
        .delete(&key, Some(replacement_etag), None)
        .success_or("current ETag conditional DELETE");
    assert!(fixture.aws.head(&key).is_none());
    let missing = fixture.aws.delete(&key, Some(replacement_etag), None);
    let missing_error =
        assert_delete_conditional_failure(&missing, "conditional DELETE on a missing key");
    eprintln!("conditional DELETE missing-key semantics: {missing_error}");
    let missing_star = fixture.aws.delete(&key, Some("*"), None);
    let missing_star_error =
        assert_delete_conditional_failure(&missing_star, "wildcard DELETE on a missing key");
    eprintln!("wildcard conditional DELETE missing-key semantics: {missing_star_error}");

    let create_race_key = fixture.full_key("create-race.bin");
    let (left, right) = std::thread::scope(|scope| {
        let left = scope.spawn(|| {
            fixture
                .aws
                .put(&create_race_key, &replacement_a, None, Some("*"))
        });
        let right = scope.spawn(|| {
            fixture
                .aws
                .put(&create_race_key, &replacement_b, None, Some("*"))
        });
        (left.join().unwrap(), right.join().unwrap())
    });
    assert_ne!(
        left.status.success(),
        right.status.success(),
        "exactly one concurrent If-None-Match create must commit:\nleft:{}\nright:{}",
        left.text(),
        right.text()
    );
    let create_race_error = if left.status.success() {
        assert_conditional_failure(&right, "losing concurrent If-None-Match create")
    } else {
        assert_conditional_failure(&left, "losing concurrent If-None-Match create")
    };
    eprintln!("concurrent If-None-Match create loser: {create_race_error}");
    let create_race_expected = if left.status.success() {
        replacement_a
    } else {
        replacement_b
    };
    assert_eq!(
        fixture.aws.get(&create_race_key, None, None).0,
        create_race_expected
    );
    fixture.cleanup();
}

#[test]
#[ignore = "requires two real Amazon S3 buckets; run with tests/linux_direct_real_s3.py"]
fn direct_amazon_s3_conditional_primitives_versioned() {
    let fixture = Fixture::versioned();
    assert_eq!(fixture.aws.versioning(), json!({"Status": "Enabled"}));
    let key = fixture.full_key("conditional-versioned.bin");
    let first = b"versioned conditional first";
    let second = b"versioned conditional second";

    fixture
        .aws
        .put(&key, first, None, Some("*"))
        .success_or("versioned If-None-Match create on a missing key");
    let first_head = fixture.aws.head(&key).expect("first versioned object");
    let first_version = first_head.version.as_deref().expect("first version ID");
    let first_etag = first_head.etag.as_deref().expect("first ETag");
    let duplicate = fixture.aws.put(&key, b"duplicate", None, Some("*"));
    let duplicate_error = assert_conditional_failure(&duplicate, "versioned If-None-Match create");
    eprintln!("versioned conditional create collision: {duplicate_error}");

    fixture
        .aws
        .put(&key, second, Some(first_etag), None)
        .success_or("versioned If-Match replacement");
    let second_head = fixture.aws.head(&key).expect("second versioned object");
    let second_version = second_head.version.as_deref().expect("second version ID");
    let second_etag = second_head.etag.as_deref().expect("second ETag");
    assert_ne!(first_version, second_version);
    assert_eq!(fixture.aws.get(&key, Some(first_version), None).0, first);

    let stale_delete = fixture.aws.delete(&key, Some(first_etag), None);
    let stale_error =
        assert_delete_conditional_failure(&stale_delete, "stale versioned conditional DELETE");
    eprintln!("stale versioned conditional DELETE: {stale_error}");
    assert_eq!(
        fixture.aws.get(&key, None, Some(second_etag)).0,
        second,
        "a stale versioned conditional DELETE must leave the new current version intact"
    );
    assert!(fixture.aws.head(&key).is_some());

    fixture
        .aws
        .delete(&key, Some(second_etag), None)
        .success_or("current versioned conditional DELETE");
    assert!(fixture.aws.head(&key).is_none());
    let versions = fixture
        .aws
        .versions(&fixture.full_key("conditional-versioned.bin"));
    assert!(
        versions.iter().any(|version| version.delete_marker),
        "versioned conditional DELETE did not create a delete marker: {versions:?}"
    );
    assert_eq!(fixture.aws.get(&key, Some(first_version), None).0, first);
    assert_eq!(fixture.aws.get(&key, Some(second_version), None).0, second);

    let missing = fixture.aws.delete(&key, Some(second_etag), None);
    let missing_error = assert_delete_conditional_failure(
        &missing,
        "versioned conditional DELETE with an existing delete marker",
    );
    eprintln!(
        "versioned conditional DELETE with delete marker missing-key semantics: {missing_error}"
    );
    let missing_star = fixture.aws.delete(&key, Some("*"), None);
    let missing_star_error = assert_delete_conditional_failure(
        &missing_star,
        "versioned wildcard DELETE with an existing delete marker",
    );
    eprintln!(
        "versioned wildcard conditional DELETE delete-marker semantics: {missing_star_error}"
    );
    fixture.cleanup_versions();
}
