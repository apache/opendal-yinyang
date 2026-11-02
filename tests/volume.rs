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

use support::TestBackend;
use yinyang::core::Fs;
use yinyang::volume::*;
use yinyang_test_support as support;

#[allow(dead_code)] // Admission tests are Linux-gated; all-target checks still compile this helper.
fn direct_operator(backend: &TestBackend, conditional_delete: bool) -> opendal::Operator {
    backend
        .operator()
        .layer(opendal::layers::CapabilityOverrideLayer::new(
            move |mut capability| {
                capability.stat = true;
                capability.read = true;
                capability.list = true;
                capability.list_with_recursive = true;
                capability.read_with_version = true;
                capability.read_with_if_match = true;
                capability.write = true;
                capability.write_can_multi = true;
                capability.write_can_empty = true;
                capability.write_with_if_match = true;
                capability.write_with_if_not_exists = true;
                capability.delete = conditional_delete;
                capability.delete_with_if_match = conditional_delete;
                capability
            },
        ))
}

#[allow(dead_code)] // Admission tests are Linux-gated; all-target checks still compile this helper.
fn direct_config(staging: std::path::PathBuf) -> VolumeConfig {
    VolumeConfig {
        name: "objects".into(),
        model: Model::Direct,
        access: Access::Mount,
        frontend: Frontend::Fuse,
        publication: Publication::Object,
        storage_profile: StorageProfile::Minio,
        staging,
        read_only: false,
        service_address: None,
    }
}

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
        service_address: None,
    }
}
#[tokio::test]
async fn admission_precedes_side_effects_and_rejects_unknown_fields() {
    let temp = tempfile::tempdir().unwrap();
    let backend = TestBackend::default();
    let cfg = config(temp.path().join("state"));
    let caps = cfg.validate(&backend.operator()).unwrap();
    assert!(caps.admitted.contains(&Capability::Append));
    assert!(caps.admitted.contains(&Capability::RemoteFsync));
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
            name: " ".into(),
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
    let mut json = serde_json::to_value(&cfg).unwrap();
    json["require"] = serde_json::json!(["read"]);
    assert!(VolumeConfig::from_json(&serde_json::to_vec(&json).unwrap()).is_err());
}
#[tokio::test]
async fn fuse_frontend_admits_read_only_and_writable_managed_mounts() {
    let temp = tempfile::tempdir().unwrap();
    let backend = TestBackend::default();
    let ro = VolumeConfig {
        frontend: Frontend::Fuse,
        read_only: true,
        ..config(temp.path().join("state"))
    };
    if cfg!(target_os = "linux") {
        let caps = ro.validate(&backend.operator()).unwrap();
        let expected: CapabilitySet = [
            Capability::Read,
            Capability::List,
            Capability::StableIdentity,
            Capability::PinnedReads,
            Capability::RetainedUnlinkRead,
        ]
        .into_iter()
        .collect();
        assert_eq!(caps.admitted, expected);
        assert!(!caps.admitted.contains(&Capability::RecoverableStaging));
        let rw = VolumeConfig {
            read_only: false,
            ..ro.clone()
        };
        let caps = rw.validate(&backend.operator()).unwrap();
        assert!(caps.admitted.contains(&Capability::Write));
        assert!(caps.admitted.contains(&Capability::RemoteFsync));
        assert!(caps.admitted.contains(&Capability::RecoverableStaging));
    } else {
        let error = ro.validate(&backend.operator()).unwrap_err();
        assert_eq!(error.kind(), yinyang::runtime::ErrorKind::Invalid);
        assert_eq!(
            error.to_string(),
            "volume admission: the fuse frontend is implemented only on Linux"
        );
    }
    let invalid = VolumeConfig {
        service_address: Some("0.0.0.0:7447".parse().unwrap()),
        publication: Publication::Service,
        ..ro.clone()
    };
    let error = invalid.validate(&backend.operator()).unwrap_err();
    assert_eq!(error.kind(), yinyang::runtime::ErrorKind::Invalid);
    assert_eq!(
        error.to_string(),
        "volume admission: service publication requires a loopback service_address"
    );
}

#[test]
fn direct_defaults_to_read_only_and_does_not_require_staging() {
    let direct: VolumeConfig = VolumeConfig::from_json(
        br#"{
            "name": "objects",
            "model": "direct",
            "access": "mount",
            "frontend": "library",
            "publication": "object",
            "storage_profile": "minio"
        }"#,
    )
    .unwrap();
    assert!(direct.read_only, "Direct must default to read-only");
    assert!(direct.staging.as_os_str().is_empty());
    let serialized = serde_json::to_value(&direct).unwrap();
    assert!(serialized.get("staging").is_none());

    let managed = config(std::path::PathBuf::from("state"));
    let managed = VolumeConfig::from_json(&serde_json::to_vec(&managed).unwrap()).unwrap();
    assert!(
        !managed.read_only,
        "Managed must retain its existing writable default"
    );

    let with_staging: VolumeConfig = VolumeConfig::from_json(
        br#"{
            "name": "objects",
            "model": "direct",
            "access": "mount",
            "frontend": "library",
            "publication": "object",
            "storage_profile": "minio",
            "staging": "must-not-exist"
        }"#,
    )
    .unwrap();
    let backend = TestBackend::default();
    let error = with_staging.validate(&backend.operator()).unwrap_err();
    assert!(error.to_string().contains("do not use staging"));

    let error = direct.validate(&backend.operator()).unwrap_err();
    assert!(error.to_string().contains("direct admission"));
    assert!(!error.to_string().contains("local staging"));
}

#[cfg(target_os = "linux")]
#[test]
fn direct_writable_admission_requires_staging_and_conditional_primitives() {
    let backend = TestBackend::default();
    let temp = tempfile::tempdir().unwrap();
    let writable = direct_config(temp.path().join("direct-state"));

    let missing_staging = VolumeConfig {
        staging: std::path::PathBuf::new(),
        ..writable.clone()
    };
    assert!(missing_staging.validate(&backend.operator()).is_err());

    let read_only_with_staging = VolumeConfig {
        read_only: true,
        ..writable.clone()
    };
    assert!(
        read_only_with_staging
            .validate(&direct_operator(&backend, true))
            .is_err()
    );

    let library = VolumeConfig {
        frontend: Frontend::Library,
        ..writable.clone()
    };
    assert!(library.validate(&direct_operator(&backend, true)).is_err());

    let operator = direct_operator(&backend, false);
    let caps = writable.validate(&operator).unwrap();
    let expected: CapabilitySet = [
        Capability::Read,
        Capability::List,
        Capability::Write,
        Capability::Append,
        Capability::Truncate,
        Capability::PinnedReads,
        Capability::RemoteFsync,
        Capability::RecoverableStaging,
    ]
    .into_iter()
    .collect();
    assert_eq!(caps.admitted, expected);
    for unsupported in [
        Capability::StableIdentity,
        Capability::AtomicRename,
        Capability::RetainedUnlinkRead,
    ] {
        assert!(!caps.admitted.contains(&unsupported));
    }
}

#[cfg(target_os = "linux")]
#[test]
fn direct_sync_requires_conditional_delete_and_library_admission() {
    let backend = TestBackend::default();
    let temp = tempfile::tempdir().unwrap();
    let sync = VolumeConfig {
        access: Access::Sync,
        frontend: Frontend::Library,
        ..direct_config(temp.path().join("direct-sync-state"))
    };

    let caps = sync.validate(&direct_operator(&backend, true)).unwrap();
    assert!(caps.admitted.contains(&Capability::Write));
    assert!(!caps.admitted.contains(&Capability::AtomicRename));
    assert!(!caps.admitted.contains(&Capability::StableIdentity));

    assert!(
        sync.validate(&direct_operator(&backend, false)).is_err(),
        "Direct Sync must reject a backend without conditional delete"
    );
    let read_only = VolumeConfig {
        read_only: true,
        ..sync.clone()
    };
    assert!(
        read_only
            .validate(&direct_operator(&backend, true))
            .is_err()
    );
    let fuse = VolumeConfig {
        frontend: Frontend::Fuse,
        ..sync
    };
    assert!(fuse.validate(&direct_operator(&backend, true)).is_err());
}
#[tokio::test]
async fn read_only_volume_enforces_policy_on_new_and_recovered_handles() {
    let temp = tempfile::tempdir().unwrap();
    let backend = TestBackend::default();
    Fs::create(backend.operator()).await.unwrap();
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
        ..cfg.clone()
    };
    let volume = Volume::open(ro, backend.operator(), None).await.unwrap();
    assert!(!volume.capabilities.admitted.contains(&Capability::Write));
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

#[cfg(target_os = "linux")]
#[test]
fn root_state_containment_rejects_both_directions() {
    use yinyang::sync_engine::{ErrorKind, check_root_state};

    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("root");
    let state = temp.path().join("state");
    std::fs::create_dir(&root).unwrap();
    std::fs::create_dir(&state).unwrap();
    check_root_state(&root, &state).unwrap();

    let state_inside_root = root.join("state");
    let error = check_root_state(&root, &state_inside_root).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Invalid);

    let root_inside_state = state.join("root");
    let error = check_root_state(&root_inside_state, &state).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Invalid);
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn managed_sync_init_rejects_both_containment_directions() {
    use std::sync::Arc;
    use yinyang::sync_engine::{ErrorKind, ManagedAdapter, SyncEngine};

    let temp = tempfile::tempdir().unwrap();
    let backend = TestBackend::default();
    let authority: Arc<dyn yinyang::core::Authority> =
        Arc::new(Fs::create(backend.operator()).await.unwrap());

    let root = temp.path().join("root-a");
    let state = root.join("state");
    let error = SyncEngine::init(ManagedAdapter::new(authority.clone()), &state, &root)
        .await
        .err()
        .unwrap();
    assert_eq!(error.kind(), ErrorKind::Invalid);
    assert!(!state.join("sync-engine.db").exists());

    let state = temp.path().join("state-b");
    let root = state.join("root");
    let error = SyncEngine::init(ManagedAdapter::new(authority), &state, &root)
        .await
        .err()
        .unwrap();
    assert_eq!(error.kind(), ErrorKind::Invalid);
    assert!(!state.join("sync-engine.db").exists());
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn managed_sync_restore_rejects_both_containment_directions_before_creation() {
    use std::sync::Arc;
    use yinyang::sync_engine::{ErrorKind, ManagedAdapter, SyncEngine};

    let temp = tempfile::tempdir().unwrap();
    let backend = TestBackend::default();
    let authority: Arc<dyn yinyang::core::Authority> =
        Arc::new(Fs::create(backend.operator()).await.unwrap());
    let source_root = temp.path().join("source-root");
    let source_state = temp.path().join("source-state");
    std::fs::create_dir(&source_root).unwrap();
    let adapter = ManagedAdapter::new(authority.clone());
    let mut sync = SyncEngine::init(adapter.clone(), &source_state, &source_root)
        .await
        .unwrap();
    let backup = temp.path().join("backup");
    sync.backup(&backup).await.unwrap();
    drop(sync);

    for root_inside_state in [false, true] {
        let (state, root) = if root_inside_state {
            let state = temp.path().join("restore-state-a");
            (state.clone(), state.join("root"))
        } else {
            let root = temp.path().join("restore-root-b");
            (root.join("state"), root)
        };
        if root_inside_state {
            std::fs::create_dir(&state).unwrap();
        } else {
            std::fs::create_dir_all(&root).unwrap();
        }
        let error = SyncEngine::restore(
            ManagedAdapter::new(authority.clone()),
            &backup,
            &root,
            &state,
        )
        .await
        .err()
        .expect("containment must reject restore");
        assert_eq!(error.kind(), ErrorKind::Invalid, "{error}");
        assert!(!state.join("sync-engine.db").exists());
    }
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
