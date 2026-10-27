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

use std::process::{Command, Output};

fn yy_command(args: &[&str]) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_yy"));
    command.args(args);
    for (name, _) in std::env::vars_os() {
        if name
            .to_str()
            .is_some_and(|name| name.starts_with("YINYANG_"))
        {
            command.env_remove(name);
        }
    }
    command
}

fn yy(args: &[&str]) -> Output {
    yy_command(args).output().expect("run yy")
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

#[test]
fn durable_publish_requires_an_explicit_identity() {
    let temp = tempfile::tempdir().unwrap();
    let state = temp.path().join("state");
    let output = yy(&[
        "publish",
        temp.path().to_str().unwrap(),
        "--state",
        state.to_str().unwrap(),
    ]);
    assert_eq!(output.status.code(), Some(2));
    assert!(
        stderr(&output).contains("--commit-id"),
        "{}",
        stderr(&output)
    );
}

#[test]
fn publication_inspect_rejects_volume_admission() {
    let temp = tempfile::tempdir().unwrap();
    let state = temp.path().join("state");
    let volume = temp.path().join("absent.json");
    let output = yy(&[
        "--volume",
        volume.to_str().unwrap(),
        "publication",
        "inspect",
        "--state",
        state.to_str().unwrap(),
        "--json",
    ]);
    assert!(!output.status.success());
    assert!(
        stderr(&output).contains("publication commands do not use --volume"),
        "{}",
        stderr(&output)
    );
}

#[test]
fn inspect_is_attempted_without_storage_configuration() {
    let temp = tempfile::tempdir().unwrap();
    let state = temp.path().join("absent-state");
    let output = yy(&[
        "publication",
        "inspect",
        "--state",
        state.to_str().unwrap(),
        "--json",
    ]);
    assert!(!output.status.success());
    let stderr = stderr(&output);
    assert!(!stderr.contains("YINYANG_STORAGE_PROFILE"), "{stderr}");
    assert!(stderr.contains("local staging"), "{stderr}");
}

#[test]
fn retry_uses_the_documented_storage_entry_point() {
    let temp = tempfile::tempdir().unwrap();
    let state = temp.path().join("state");
    let output = yy_command(&[
        "publication",
        "retry",
        "00000000-0000-4000-8000-000000000000",
        "--state",
        state.to_str().unwrap(),
    ])
    .env("YINYANG_S3_BUCKET", "publication-admission")
    .env("YINYANG_S3_REGION", "us-east-1")
    .env("YINYANG_S3_ENDPOINT", "http://127.0.0.1:9")
    .output()
    .expect("run yy");
    assert!(!output.status.success());
    assert!(
        stderr(&output).contains("set YINYANG_STORAGE_PROFILE"),
        "{}",
        stderr(&output)
    );
}
