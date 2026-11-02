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

#![cfg(target_os = "linux")]

//! Real-process acceptance for durable one-shot directory publication.
//!
//! The fixture observes storage independently of `yy`. Its loopback HTTP gate
//! forwards normal S3 traffic and interferes only with the final conditional
//! head publication, so no production-only fault hook is required.

use futures_util::TryStreamExt as _;
use serde_json::Value;
use std::io::Read;
use std::path::Path;
use std::process::{Child, Command, ExitStatus, Stdio};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};
mod support;

use support::linux_sync::{Fixture, Publication, Tree, local_tree, runtime};
use yinyang_test_support::s3_gate;

const PROCESS_TIMEOUT: Duration = Duration::from_secs(180);

use s3_gate::{CommitAction, S3Gate};

struct Running {
    child: Child,
    stdout: Option<JoinHandle<Vec<u8>>>,
    stderr: Option<JoinHandle<Vec<u8>>>,
}

impl Running {
    fn wait(mut self) -> Captured {
        let deadline = Instant::now() + PROCESS_TIMEOUT;
        let status = loop {
            match self.child.try_wait() {
                Ok(Some(status)) => break status,
                Ok(None) if Instant::now() < deadline => thread::sleep(Duration::from_millis(25)),
                Ok(None) => {
                    let _ = self.child.kill();
                    let _ = self.child.wait();
                    panic!("yy did not exit within {PROCESS_TIMEOUT:?}");
                }
                Err(error) => panic!("wait for yy: {error}"),
            }
        };
        Captured {
            status,
            stdout: self.stdout.take().unwrap().join().unwrap_or_default(),
            stderr: self.stderr.take().unwrap().join().unwrap_or_default(),
        }
    }

    fn kill(mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        if let Some(thread) = self.stdout.take() {
            let _ = thread.join();
        }
        if let Some(thread) = self.stderr.take() {
            let _ = thread.join();
        }
    }
}

impl Drop for Running {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        if let Some(thread) = self.stdout.take() {
            let _ = thread.join();
        }
        if let Some(thread) = self.stderr.take() {
            let _ = thread.join();
        }
    }
}

struct Captured {
    status: ExitStatus,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
}

impl Captured {
    fn success(self) -> Self {
        assert!(
            self.status.success(),
            "yy failed with {}:\nstdout:\n{}\nstderr:\n{}",
            self.status,
            String::from_utf8_lossy(&self.stdout),
            String::from_utf8_lossy(&self.stderr)
        );
        self
    }

    fn json(&self) -> Value {
        serde_json::from_slice(&self.stdout).unwrap_or_else(|error| {
            panic!(
                "parse yy JSON: {error}\nstdout:\n{}\nstderr:\n{}",
                String::from_utf8_lossy(&self.stdout),
                String::from_utf8_lossy(&self.stderr)
            )
        })
    }
}

fn spawn(command: &mut Command) -> Running {
    command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = command.spawn().expect("spawn yy");
    let stdout = child.stdout.take().expect("yy stdout is piped");
    let stderr = child.stderr.take().expect("yy stderr is piped");
    Running {
        child,
        stdout: Some(thread::spawn(move || read_all(stdout))),
        stderr: Some(thread::spawn(move || read_all(stderr))),
    }
}

fn offline_yy(args: &[&str]) -> Captured {
    let output = Command::new(env!("CARGO_BIN_EXE_yy"))
        .args(args)
        .env_clear()
        .output()
        .expect("run offline yy");
    Captured {
        status: output.status,
        stdout: output.stdout,
        stderr: output.stderr,
    }
}

fn read_all(mut reader: impl Read) -> Vec<u8> {
    let mut bytes = Vec::new();
    let _ = reader.read_to_end(&mut bytes);
    bytes
}

fn write_source(root: &Path) {
    std::fs::create_dir_all(root.join("nested/empty-dir")).unwrap();
    std::fs::write(root.join("nested/hello"), b"original").unwrap();
    std::fs::write(root.join("empty"), []).unwrap();
}

fn mutate_source(root: &Path) {
    std::fs::write(root.join("nested/hello"), b"changed after dispatch").unwrap();
    std::fs::remove_file(root.join("empty")).unwrap();
    std::fs::write(root.join("late"), b"new source intent").unwrap();
}

fn publication_inspect(state: &Path, id: &str) -> Value {
    let output = offline_yy(&[
        "publication",
        "inspect",
        id,
        "--state",
        state.to_str().unwrap(),
        "--json",
    ]);
    let output = output.success();
    let value = output.json();
    assert_eq!(value["schema"], "yinyang.publication.inspect.v1");
    assert_eq!(value["intents"].as_array().unwrap().len(), 1);
    value["intents"][0].clone()
}

fn assert_state(intent: &Value, allowed: &[&str]) {
    let state = intent["state"].as_str().expect("inspection state");
    assert!(
        allowed.contains(&state),
        "unexpected durable state {state:?}: {intent:#}"
    );
}

fn assert_tree(actual: &Tree, expected: &Tree, context: &str) {
    assert_eq!(actual, expected, "{context}");
}

fn independent_operator(fixture: &Fixture) -> opendal::Operator {
    opendal::install_default();
    let command = fixture.yy_command(&[]);
    let config = command
        .get_envs()
        .filter_map(|(key, value)| {
            let key = key
                .to_str()?
                .strip_prefix("YINYANG_S3_")?
                .to_ascii_lowercase();
            Some((key, value?.to_str()?.to_owned()))
        })
        .collect::<Vec<_>>();
    opendal::Operator::via_iter("s3", config).expect("construct independent S3 operator")
}

fn delete_content_pack(fixture: &Fixture, marker: &[u8]) {
    let operator = independent_operator(fixture);
    runtime().block_on(async {
        let mut lister = operator
            .lister_with("/")
            .recursive(true)
            .await
            .expect("list isolated storage");
        while let Some(entry) = lister.try_next().await.expect("read storage entry") {
            if entry.metadata().mode() != opendal::EntryMode::FILE {
                continue;
            }
            let path = entry.path().to_owned();
            let bytes = operator
                .read(&path)
                .await
                .expect("read immutable object")
                .to_bytes();
            if bytes.windows(marker.len()).any(|window| window == marker) {
                operator.delete(&path).await.expect("delete content pack");
                return;
            }
        }
        panic!("no immutable pack contained the expected file marker");
    });
}

fn process_death_case(action: CommitAction, delayed_original_succeeds: bool) {
    let fixture = Fixture::new(Publication::Object);
    let source = fixture.path("source");
    write_source(&source);
    let original = local_tree(&source).unwrap();
    let state = fixture.path("journal");
    let commit_id = uuid::Uuid::new_v4().to_string();
    let gate = S3Gate::start(action);
    let mut command = fixture.yy_command(&[
        "publish",
        source.to_str().unwrap(),
        "--state",
        state.to_str().unwrap(),
        "--commit-id",
        &commit_id,
    ]);
    command.env("YINYANG_S3_ENDPOINT", gate.endpoint());
    let running = spawn(&mut command);
    gate.wait_intercepted();

    let receipt = fixture.yy(&["receipt", &commit_id]);
    assert_eq!(receipt.status.code(), Some(3));
    assert!(
        String::from_utf8_lossy(&receipt.stderr).contains("absence is not proof of failure"),
        "{}",
        String::from_utf8_lossy(&receipt.stderr)
    );
    let intent = publication_inspect(&state, &commit_id);
    assert_state(&intent, &["unknown"]);
    let digest = intent["request_digest"].as_str().unwrap().to_owned();

    mutate_source(&source);
    running.kill();
    gate.release();
    if delayed_original_succeeds {
        gate.wait_commit_succeeded();
        fixture.wait_remote_tree(&original, Duration::from_secs(60));
    } else {
        gate.wait_original_dropped();
        assert!(fixture.remote_tree().unwrap().is_empty());
    }

    let intent = publication_inspect(&state, &commit_id);
    assert_state(&intent, &["unknown"]);
    assert_eq!(intent["request_digest"], digest);
    fixture.yy_ok(&[
        "publication",
        "retry",
        &commit_id,
        "--state",
        state.to_str().unwrap(),
    ]);
    let intent = publication_inspect(&state, &commit_id);
    assert_state(&intent, &["committed"]);
    assert_eq!(intent["request_digest"], digest);
    assert_tree(
        &fixture.remote_tree().unwrap(),
        &original,
        "recovered publication",
    );

    let revision = fixture.yy_ok(&["receipt", &commit_id]).stdout;
    let revision = std::str::from_utf8(&revision)
        .unwrap()
        .lines()
        .find_map(|line| line.strip_prefix("revision: "))
        .unwrap()
        .to_owned();
    let restored = fixture.path("restored");
    fixture.yy_ok(&[
        "restore",
        restored.to_str().unwrap(),
        "--revision",
        &revision,
    ]);
    assert_eq!(local_tree(&restored).unwrap(), original);
}

#[test]
#[ignore = "requires an isolated S3 bucket configured with YINYANG_S3_* variables"]
fn s3_response_loss_replays_the_frozen_directory_without_rescanning_source() {
    let fixture = Fixture::new(Publication::Object);
    let source = fixture.path("source");
    write_source(&source);
    let original = local_tree(&source).unwrap();
    let state = fixture.path("journal");
    let commit_id = uuid::Uuid::new_v4().to_string();
    let gate = S3Gate::start(CommitAction::DropResponse);
    let mut command = fixture.yy_command(&[
        "publish",
        source.to_str().unwrap(),
        "--state",
        state.to_str().unwrap(),
        "--commit-id",
        &commit_id,
    ]);
    command.env("YINYANG_S3_ENDPOINT", gate.endpoint());
    let output = spawn(&mut command).wait();
    assert_eq!(
        output.status.code(),
        Some(3),
        "lost commit response must remain uncertain\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    gate.wait_intercepted();
    gate.release();
    mutate_source(&source);

    let intent = publication_inspect(&state, &commit_id);
    assert_eq!(intent["commit_id"], commit_id);
    assert_state(&intent, &["unknown"]);
    let digest = intent["request_digest"].as_str().unwrap().to_owned();
    assert_eq!(digest.len(), 64);

    let exported = fixture.path("exported");
    fixture.yy_ok(&[
        "publication",
        "export",
        &commit_id,
        "--state",
        state.to_str().unwrap(),
        "--destination",
        exported.to_str().unwrap(),
    ]);
    assert_eq!(local_tree(&exported).unwrap(), original);
    let occupied_export = fixture.yy(&[
        "publication",
        "export",
        &commit_id,
        "--state",
        state.to_str().unwrap(),
        "--destination",
        exported.to_str().unwrap(),
    ]);
    assert!(!occupied_export.status.success());
    assert_eq!(local_tree(&exported).unwrap(), original);

    let retried = fixture.yy_ok(&[
        "publication",
        "retry",
        &commit_id,
        "--state",
        state.to_str().unwrap(),
    ]);
    assert!(
        String::from_utf8_lossy(&retried.stdout).contains(&commit_id),
        "retry output must retain the original commit identity: {}",
        String::from_utf8_lossy(&retried.stdout)
    );
    let intent = publication_inspect(&state, &commit_id);
    assert_state(&intent, &["committed"]);
    assert_eq!(intent["request_digest"], digest);
    assert_tree(
        &fixture.remote_tree().unwrap(),
        &original,
        "remote publication",
    );

    let rejected = fixture.yy(&[
        "publish",
        source.to_str().unwrap(),
        "--replace",
        "--state",
        state.to_str().unwrap(),
        "--commit-id",
        &commit_id,
    ]);
    assert!(!rejected.status.success());
    assert!(
        String::from_utf8_lossy(&rejected.stderr).contains("publication retry"),
        "existing identity must point to source-free retry: {}",
        String::from_utf8_lossy(&rejected.stderr)
    );
    assert_tree(
        &fixture.remote_tree().unwrap(),
        &original,
        "rejected identity must not rescan changed source",
    );
}

#[test]
#[ignore = "requires an isolated S3 bucket configured with YINYANG_S3_* variables"]
fn s3_process_death_recovers_a_delayed_original_commit() {
    process_death_case(CommitAction::HoldBeforeCommit, true);
}

#[test]
#[ignore = "requires an isolated S3 bucket configured with YINYANG_S3_* variables"]
fn s3_process_death_replays_after_the_original_request_is_dropped() {
    process_death_case(CommitAction::DropBeforeCommit, false);
}

#[test]
#[ignore = "requires an isolated S3 bucket configured with YINYANG_S3_* variables"]
fn s3_historical_restore_and_publication_export_are_explicit_and_no_clobber() {
    let fixture = Fixture::new(Publication::Object);
    let source = fixture.path("source");
    write_source(&source);
    let original = local_tree(&source).unwrap();
    fixture.yy_ok(&["publish", source.to_str().unwrap()]);
    let first = fixture.yy_ok(&["status"]);
    let revision = std::str::from_utf8(&first.stdout)
        .unwrap()
        .lines()
        .find_map(|line| line.strip_prefix("revision: "))
        .unwrap()
        .to_owned();

    mutate_source(&source);
    fixture.yy_ok(&["publish", source.to_str().unwrap(), "--replace"]);
    let restored = fixture.path("historical");
    fixture.yy_ok(&[
        "restore",
        restored.to_str().unwrap(),
        "--revision",
        &revision,
    ]);
    assert_eq!(local_tree(&restored).unwrap(), original);

    let existing = fixture.path("occupied");
    std::fs::create_dir(&existing).unwrap();
    std::fs::write(existing.join("marker"), b"keep").unwrap();
    let refused = fixture.yy(&[
        "restore",
        existing.to_str().unwrap(),
        "--revision",
        &revision,
    ]);
    assert!(!refused.status.success());
    assert_eq!(std::fs::read(existing.join("marker")).unwrap(), b"keep");
}

#[test]
#[ignore = "requires an isolated S3 bucket configured with YINYANG_S3_* variables"]
fn s3_restore_exposes_a_partial_destination_without_overwriting() {
    let fixture = Fixture::new(Publication::Object);
    let source = fixture.path("source");
    std::fs::create_dir_all(source.join("nested/empty-dir")).unwrap();
    let marker = b"directory-recovery-partial-destination-marker";
    std::fs::write(source.join("nested/payload"), marker).unwrap();
    fixture.yy_ok(&["publish", source.to_str().unwrap()]);
    delete_content_pack(&fixture, marker);

    let destination = fixture.path("partial");
    let failed = fixture.yy(&["restore", destination.to_str().unwrap()]);
    assert!(!failed.status.success());
    assert!(destination.is_dir(), "restore created the destination root");
    assert!(
        destination.join("nested/empty-dir").is_dir(),
        "restore created verified directory structure before failing"
    );
    assert!(!destination.join("nested/payload").exists());
    std::fs::write(destination.join("partial-marker"), b"keep").unwrap();

    let refused = fixture.yy(&["restore", destination.to_str().unwrap()]);
    assert!(!refused.status.success());
    assert_eq!(
        std::fs::read(destination.join("partial-marker")).unwrap(),
        b"keep"
    );
    assert!(!destination.join("nested/payload").exists());
}
