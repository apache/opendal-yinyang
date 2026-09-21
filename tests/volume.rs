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

#[path = "../crates/core/tests/support/mod.rs"]
mod support;
use std::collections::BTreeSet;
use support::TestBackend;
use yinyang::core::{BackendProfile, Fs};
use yinyang::volume::*;
fn config(path: std::path::PathBuf) -> VolumeConfig {
    VolumeConfig {
        name: "test".into(),
        model: Model::Managed,
        access: Access::Mount,
        frontend: Frontend::Library,
        publication: Publication::Object,
        storage_profile: StorageProfile::Minio,
        staging: path,
        read_only: false,
        require: [Capability::Read, Capability::RemoteFsync]
            .into_iter()
            .collect(),
        service_address: None,
    }
}
#[tokio::test]
async fn capability_intersection_and_startup_rejection_precede_visibility() {
    let temp = tempfile::tempdir().unwrap();
    let backend = TestBackend::default();
    let cfg = config(temp.path().join("state"));
    let caps = cfg.validate(&backend.operator()).unwrap();
    let mut frontend = caps.frontend.clone();
    frontend.remove(&Capability::Append);
    let intersection = Capabilities::intersect(caps.volume, caps.storage, caps.access, frontend);
    assert!(!intersection.effective.contains(&Capability::Append));
    assert!(intersection.effective.contains(&Capability::RemoteFsync));
    for invalid in [
        VolumeConfig {
            model: Model::Direct,
            ..cfg.clone()
        },
        VolumeConfig {
            access: Access::Sync,
            ..cfg.clone()
        },
        VolumeConfig {
            frontend: Frontend::Fuse,
            ..cfg.clone()
        },
        VolumeConfig {
            read_only: true,
            ..cfg.clone()
        },
        VolumeConfig {
            publication: Publication::Service,
            service_address: Some("0.0.0.0:7447".parse().unwrap()),
            ..cfg.clone()
        },
    ] {
        assert!(
            Volume::open(invalid, backend.operator(), None)
                .await
                .is_err()
        );
        assert!(!cfg.staging.exists());
        assert!(backend.state.lock().unwrap().read_paths.is_empty());
        assert!(backend.state.lock().unwrap().objects.is_empty());
    }
    assert!(
        Volume::open(
            cfg.clone(),
            backend.operator_without_streaming_write(),
            None
        )
        .await
        .is_err()
    );
    assert!(!cfg.staging.exists());
    let mut json = serde_json::to_value(&cfg).unwrap();
    json["ignore_conflicts"] = true.into();
    assert!(VolumeConfig::from_json(&serde_json::to_vec(&json).unwrap()).is_err());
}
#[tokio::test]
async fn read_only_volume_enforces_policy_on_new_and_recovered_handles() {
    let temp = tempfile::tempdir().unwrap();
    let backend = TestBackend::default();
    Fs::create(backend.operator(), BackendProfile::Minio)
        .await
        .unwrap();
    let cfg = config(temp.path().join("state"));
    let volume = Volume::open(cfg.clone(), backend.operator(), None)
        .await
        .unwrap();
    let runtime = volume.runtime();
    runtime
        .create_file(runtime.authority().root(), "file")
        .await
        .unwrap();
    let mut handle = runtime.open_file("file", true).await.unwrap();
    handle.write(0, b"pending bytes").await.unwrap();
    let id = handle.id();
    let inspection = yinyang::runtime::Runtime::inspect(&cfg.staging)
        .await
        .unwrap();
    assert!(inspection.handles[0].pending);
    drop(handle);
    drop(volume);
    let ro = VolumeConfig {
        read_only: true,
        require: BTreeSet::new(),
        ..cfg.clone()
    };
    let volume = Volume::open(ro, backend.operator(), None).await.unwrap();
    assert!(!volume.capabilities.effective.contains(&Capability::Write));
    assert!(volume.runtime().open_file("file", true).await.is_err());
    assert!(
        volume
            .runtime()
            .create_file(volume.runtime().authority().root(), "forbidden")
            .await
            .is_err()
    );
    let mut handle = volume.runtime().recover(id).await.unwrap();
    assert_eq!(handle.read(0, 100).await.unwrap(), b"pending bytes");
    assert!(handle.append(b"forbidden").await.is_err());
    assert!(handle.fsync().await.is_err());
    drop(handle);
    drop(volume);
    let volume = Volume::open(cfg, backend.operator(), None).await.unwrap();
    let mut handle = volume.runtime().recover(id).await.unwrap();
    handle.fsync().await.unwrap();
    handle.close().await.unwrap();
}

struct Daemon(std::process::Child);
impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
#[test]
#[ignore = "requires an isolated S3 bucket configured with YINYANG_S3_* variables"]
fn s3_volume_cli_recovers_staged_writes_across_processes() {
    use std::io::{BufRead, BufReader};
    use std::process::{Command, Stdio};
    for service in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let root = format!("volume-{}", uuid::Uuid::new_v4().simple());
        let token = "test-only-service-token-at-least-32-bytes";
        let run = |args: &[&str]| {
            Command::new(env!("CARGO_BIN_EXE_yy"))
                .args(args)
                .env("YINYANG_S3_ROOT", &root)
                .env("YINYANG_STORAGE_PROFILE", "minio")
                .env("YINYANG_SERVICE_TOKEN", token)
                .output()
                .unwrap()
        };
        let mut cfg = config(temp.path().join("staging"));
        let mut daemon = None;
        if service {
            let mut child = Daemon(
                Command::new(env!("CARGO_BIN_EXE_yy"))
                    .args(["serve", "--database"])
                    .arg(temp.path().join("metadata.db"))
                    .args(["--listen", "127.0.0.1:0"])
                    .env("YINYANG_S3_ROOT", &root)
                    .env("YINYANG_STORAGE_PROFILE", "minio")
                    .env("YINYANG_SERVICE_TOKEN", token)
                    .stdout(Stdio::piped())
                    .spawn()
                    .unwrap(),
            );
            let mut line = String::new();
            BufReader::new(child.0.stdout.take().unwrap())
                .read_line(&mut line)
                .unwrap();
            cfg.publication = Publication::Service;
            cfg.service_address = Some(
                line.trim()
                    .strip_prefix("metadata service listening on ")
                    .unwrap()
                    .parse()
                    .unwrap(),
            );
            daemon = Some(child);
        } else {
            assert!(run(&["create"]).status.success());
        }
        let config_path = temp.path().join("volume.json");
        std::fs::write(&config_path, serde_json::to_vec(&cfg).unwrap()).unwrap();
        let volume = |args: &[&str]| {
            let mut full = vec!["--volume", config_path.to_str().unwrap()];
            full.extend(args);
            let output = run(&full);
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            output.stdout
        };
        let text = |args: &[&str]| String::from_utf8(volume(args)).unwrap();
        assert!(text(&["capabilities"]).contains("remote-fsync"));
        volume(&["file", "create", "file"]);
        let handle = text(&["file", "open", "file", "--write"]).trim().to_owned();
        let bytes = vec![73; 200_003];
        let source = temp.path().join("source");
        std::fs::write(&source, &bytes).unwrap();
        assert!(
            text(&["file", "write", &handle, source.to_str().unwrap()]).contains("pending: true")
        );
        // Each command has exited: publication recovers only from persisted staging.
        assert!(text(&["status"]).contains("pending: true"));
        assert!(text(&["file", "fsync", &handle]).contains("committed:"));
        assert!(text(&["status"]).contains("pending: false"));
        volume(&["file", "close", &handle]);
        let reader = text(&["file", "open", "file"]).trim().to_owned();
        assert_eq!(
            volume(&["file", "read", &reader, "--length", "200003"]),
            bytes
        );
        volume(&["file", "rename", "file", "moved"]);
        volume(&["file", "unlink", "moved"]);
        assert_eq!(
            volume(&["file", "read", &reader, "--length", "200003"]),
            bytes
        );
        volume(&["file", "close", &reader]);
        if let Some(child) = daemon.take() {
            drop(child);
            let state = text(&["status"]);
            assert!(state.contains("remote: unavailable"));
            assert!(state.contains("local handles: 0"));
        }
    }
}
