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

//! Installation, lifecycle and diagnostic regression through the real `yy` CLI.

#![cfg(target_os = "linux")]

use std::fs;
use std::io::{BufRead, BufReader};
use std::net::SocketAddr;
use std::os::unix::fs::{OpenOptionsExt as _, PermissionsExt as _};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

use nix::sys::signal::{Signal, kill};
use nix::unistd::Pid;
use serde_json::Value;

const SERVICE_TOKEN: &str = "yinyang-operations-test-token-0000000000000000";

#[test]
fn doctor_requires_a_volume() {
    let output = clean_yy(&["doctor", "--json"])
        .output()
        .expect("run yy doctor");
    assert!(!output.status.success());
    assert!(
        stderr(&output).contains("requires --volume CONFIG.json"),
        "{}",
        stderr(&output)
    );
}

#[test]
fn doctor_redacts_credentials_and_reports_service_action() {
    let temp = tempfile::tempdir().unwrap();
    let config = write_config(
        temp.path(),
        "service",
        "mount",
        "fuse",
        "service",
        Some("127.0.0.1:9"),
    );
    let access_key = "AKIAOPERATIONSTEST";
    let secret_key = "operations-secret-never-print";
    let output = clean_yy(&["--volume", config.to_str().unwrap(), "doctor", "--json"])
        .env("YINYANG_S3_BUCKET", "operations-diagnostic")
        .env("YINYANG_S3_REGION", "us-east-1")
        .env("YINYANG_S3_ACCESS_KEY_ID", access_key)
        .env("YINYANG_S3_SECRET_ACCESS_KEY", secret_key)
        .env("YINYANG_STORAGE_PROFILE", "amazon-s3")
        .env("YINYANG_SERVICE_TOKEN", SERVICE_TOKEN)
        .output()
        .expect("run yy doctor");
    assert!(!output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(!stdout.contains(access_key), "{stdout}");
    assert!(!stdout.contains(secret_key), "{stdout}");
    assert!(!stdout.contains(SERVICE_TOKEN), "{stdout}");
    let value: Value = serde_json::from_slice(&output.stdout).expect("doctor JSON");
    assert_eq!(value["schema"], "yinyang.operational-health.v1");
    assert_eq!(value["status"], "action_required");
    assert!(check(&value, "authority").is_some());
}

#[test]
fn doctor_uses_volume_storage_profile_without_the_global_profile() {
    let temp = tempfile::tempdir().unwrap();
    let config = write_config_with_profile(
        temp.path(),
        "minio",
        "mount",
        "fuse",
        "object",
        None,
        "minio",
    );
    let output = clean_yy(&["--volume", config.to_str().unwrap(), "doctor", "--json"])
        .env("YINYANG_S3_BUCKET", "operations-diagnostic")
        .env("YINYANG_S3_REGION", "us-east-1")
        .env("YINYANG_S3_ENDPOINT", "http://127.0.0.1:9")
        .env("YINYANG_S3_ACCESS_KEY_ID", "operations-access")
        .env("YINYANG_S3_SECRET_ACCESS_KEY", "operations-secret")
        .output()
        .expect("run yy doctor");
    assert!(!output.status.success());
    let value: Value = serde_json::from_slice(&output.stdout).expect("doctor JSON");
    assert_eq!(
        check(&value, "environment").unwrap()["status"],
        "pass",
        "{value:#}"
    );
    assert_eq!(
        check(&value, "authority").unwrap()["status"],
        "action_required",
        "{value:#}"
    );
}

#[test]
#[ignore = "requires isolated S3 settings and /dev/fuse"]
fn isolated_s3_cli_smoke() {
    cli_smoke();
}

#[test]
#[ignore = "requires real Amazon S3 settings and /dev/fuse"]
fn real_amazon_s3_cli_smoke() {
    let storage = Storage::from_env();
    assert_eq!(storage.profile, "amazon-s3");
    assert_eq!(storage.endpoint, None);
    cli_smoke_with(storage);
}

#[test]
#[ignore = "requires isolated MinIO settings, /dev/fuse, and a running systemd user manager"]
fn isolated_user_service_lifecycle() {
    let storage = Storage::from_env();
    assert_eq!(
        storage.profile, "minio",
        "the systemd lifecycle test uses the public isolated-Silo fixture, not real credentials"
    );
    require_fuse();
    let temp = tempfile::tempdir().unwrap();
    let install = Install::new(temp.path());
    let state = temp.path().join("state");
    let configs = temp.path().join("config");
    let mounts = temp.path().join("mounts");
    let work = temp.path().join("work");
    for directory in [&state, &configs, &mounts, &work] {
        fs::create_dir_all(directory).unwrap();
    }
    fs::create_dir_all(state.join("metadata")).unwrap();

    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let service_address = listener.local_addr().unwrap();
    drop(listener);
    let service_root = storage.root("systemd-service");
    let mount_root = storage.root("systemd-mount");
    let sync_root = storage.root("systemd-sync");
    install.require_ok(&storage, &work, &["create"], &mount_root);
    install.require_ok(&storage, &work, &["create"], &sync_root);
    let units = UserUnits::install(
        &install,
        &storage,
        &configs,
        &state,
        &mounts,
        service_address,
        [&service_root, &mount_root, &sync_root],
    );

    let service_config = write_config(
        &configs,
        "service",
        "mount",
        "fuse",
        "service",
        Some(&service_address.to_string()),
    );
    rewrite_staging(&service_config, &state.join("service"));
    units.start(&units.serve);
    units.wait_active(&units.serve);
    wait_doctor_ready(
        &install,
        &storage,
        &work,
        &service_config,
        &service_root,
        &units.serve,
    );
    units.restart(&units.serve);
    units.wait_active(&units.serve);
    wait_doctor_ready(
        &install,
        &storage,
        &work,
        &service_config,
        &service_root,
        &units.serve,
    );
    units.stop(&units.serve);
    assert!(state.join("metadata/metadata.db").exists());

    let mount_config = write_config(&configs, "object", "mount", "fuse", "object", None);
    rewrite_staging(&mount_config, &state.join("object"));
    fs::create_dir(mounts.join("object")).unwrap();
    units.start(&units.mount_instance("object"));
    units.wait_active(&units.mount_instance("object"));
    wait_mount(&mounts.join("object"), None);
    units.restart(&units.mount_instance("object"));
    units.wait_active(&units.mount_instance("object"));
    wait_mount(&mounts.join("object"), None);
    units.stop(&units.mount_instance("object"));
    assert!(state.join("object/staging.db").exists());

    let sync_config = write_config(&configs, "sync", "sync", "library", "object", None);
    rewrite_staging(&sync_config, &state.join("sync"));
    let local = work.join("sync-root");
    install.require_ok(
        &storage,
        &work,
        &[
            "--volume",
            sync_config.to_str().unwrap(),
            "sync",
            "init",
            "--directory",
            local.to_str().unwrap(),
        ],
        &sync_root,
    );
    fs::write(local.join("systemd.txt"), b"systemd finite sync").unwrap();
    units.start(&units.sync_instance("sync"));
    units.wait_active(&units.sync_instance("sync"));
    let status = install.run(
        &storage,
        &work,
        &[
            "--volume",
            sync_config.to_str().unwrap(),
            "sync",
            "status",
            "--json",
        ],
        &sync_root,
        None,
    );
    status.success_or("sync status after a systemd finite-sync pass");
    let status: Value = serde_json::from_slice(&status.stdout).unwrap();
    assert_eq!(status["summary"]["in_sync"], true, "{status:#}");
    units.restart(&units.sync_instance("sync"));
    units.wait_active(&units.sync_instance("sync"));
    units.stop(&units.sync_instance("sync"));
    assert!(state.join("sync/sync.db").exists());

    drop(units);
    fs::remove_dir_all(&install.prefix).unwrap();
    assert!(state.join("metadata/metadata.db").exists());
    assert!(state.join("object/staging.db").exists());
    assert!(state.join("sync/sync.db").exists());
}

fn cli_smoke() {
    cli_smoke_with(Storage::from_env());
}

fn cli_smoke_with(storage: Storage) {
    require_fuse();
    let temp = tempfile::tempdir().unwrap();
    let install = Install::new(temp.path());
    let state = temp.path().join("state");
    let configs = temp.path().join("config");
    let work = temp.path().join("work");
    fs::create_dir_all(&state).unwrap();
    fs::create_dir_all(&configs).unwrap();
    fs::create_dir_all(&work).unwrap();

    let object_root = storage.root("object");
    let object_config = write_config(&configs, "object", "mount", "fuse", "object", None);
    rewrite_staging(&object_config, &state.join("object"));
    install.require_ok(&storage, &work, &["create"], &object_root);

    let doctor = install.run(
        &storage,
        &work,
        &[
            "--volume",
            object_config.to_str().unwrap(),
            "doctor",
            "--json",
        ],
        &object_root,
        None,
    );
    doctor.success_or("doctor before starting the installed CLI");
    let doctor: Value = serde_json::from_slice(&doctor.stdout).expect("doctor JSON");
    assert_eq!(doctor["schema"], "yinyang.operational-health.v1");
    assert_eq!(doctor["status"], "pass");

    let mountpoint = work.join("object-mount");
    fs::create_dir(&mountpoint).unwrap();
    let mut mount = install.spawn(
        &storage,
        &work,
        &[
            "--volume",
            object_config.to_str().unwrap(),
            "mount",
            mountpoint.to_str().unwrap(),
        ],
        &object_root,
        None,
    );
    wait_mount(&mountpoint, Some(&mut mount));
    stop(&mut mount);
    assert!(
        state.join("object/staging.db").exists(),
        "mount stop discarded durable staging"
    );

    let sync_root = storage.root("sync");
    let sync_config = write_config(&configs, "sync", "sync", "library", "object", None);
    rewrite_staging(&sync_config, &state.join("sync"));
    install.require_ok(&storage, &work, &["create"], &sync_root);
    let local = work.join("sync-root");
    install.require_ok(
        &storage,
        &work,
        &[
            "--volume",
            sync_config.to_str().unwrap(),
            "sync",
            "init",
            "--directory",
            local.to_str().unwrap(),
        ],
        &sync_root,
    );
    fs::write(local.join("hello.txt"), b"finite sync bytes").unwrap();
    let run = install.run(
        &storage,
        &work,
        &["--volume", sync_config.to_str().unwrap(), "sync", "run"],
        &sync_root,
        None,
    );
    run.success_or("finite sync run");
    let status = install.run(
        &storage,
        &work,
        &[
            "--volume",
            sync_config.to_str().unwrap(),
            "sync",
            "status",
            "--json",
        ],
        &sync_root,
        None,
    );
    status.success_or("sync status after finite run");
    let status: Value = serde_json::from_slice(&status.stdout).expect("sync status JSON");
    assert_eq!(status["schema"], "yinyang.sync.status.v1");
    assert_eq!(
        status["summary"]["in_sync"], true,
        "sync run exit 0 alone is not convergence: {status:#}"
    );
    assert!(
        state.join("sync/sync.db").exists(),
        "finite sync did not retain durable state"
    );

    let service_root = storage.root("service");
    let service_database = state.join("metadata/metadata.db");
    fs::create_dir_all(service_database.parent().unwrap()).unwrap();
    let mut service = start_service(&install, &storage, &work, &service_database, &service_root);
    let address = wait_service(&mut service);
    let service_config = write_config(
        &configs,
        "service",
        "mount",
        "fuse",
        "service",
        Some(&address.to_string()),
    );
    rewrite_staging(&service_config, &state.join("service"));
    let doctor = install.run(
        &storage,
        &work,
        &[
            "--volume",
            service_config.to_str().unwrap(),
            "doctor",
            "--json",
        ],
        &service_root,
        Some(SERVICE_TOKEN),
    );
    doctor.success_or("doctor against the authenticated loopback service");
    let doctor: Value = serde_json::from_slice(&doctor.stdout).expect("service doctor JSON");
    assert_eq!(doctor["status"], "pass");
    assert_eq!(
        doctor["volume"]["authority"],
        "authenticated-loopback-service"
    );
    stop(&mut service);

    let mut restarted = start_service(&install, &storage, &work, &service_database, &service_root);
    let restarted_address = wait_service(&mut restarted);
    let mut restarted_doctor = install.run(
        &storage,
        &work,
        &[
            "--volume",
            service_config.to_str().unwrap(),
            "doctor",
            "--json",
        ],
        &service_root,
        Some(SERVICE_TOKEN),
    );
    if restarted_address != address {
        let text = fs::read_to_string(&service_config).unwrap().replace(
            &format!("\"service_address\": \"{address}\""),
            &format!("\"service_address\": \"{restarted_address}\""),
        );
        fs::write(&service_config, text).unwrap();
        restarted_doctor = install.run(
            &storage,
            &work,
            &[
                "--volume",
                service_config.to_str().unwrap(),
                "doctor",
                "--json",
            ],
            &service_root,
            Some(SERVICE_TOKEN),
        );
    }
    restarted_doctor.success_or("doctor after metadata-service restart");
    stop(&mut restarted);
    assert!(
        service_database.exists(),
        "metadata service stop discarded its authority database"
    );

    fs::remove_dir_all(&install.prefix).unwrap();
    assert!(local.join("hello.txt").exists());
    assert!(state.join("object/staging.db").exists());
    assert!(state.join("sync/sync.db").exists());
    assert!(service_database.exists());
    assert!(object_config.exists());
    assert!(sync_config.exists());
    assert!(service_config.exists());
}

fn require_fuse() {
    assert!(
        Path::new("/dev/fuse").exists(),
        "/dev/fuse is required by the installed-CLI lifecycle test"
    );
    assert!(
        executable_on_path("fusermount3"),
        "fusermount3 is required by the installed-CLI lifecycle test"
    );
}

fn clean_yy(args: &[&str]) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_yy"));
    command.args(args);
    remove_yinyang_environment(&mut command);
    command
}

fn remove_yinyang_environment(command: &mut Command) {
    for (name, _) in std::env::vars_os() {
        if name.to_string_lossy().starts_with("YINYANG_") {
            command.env_remove(name);
        }
    }
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

fn check<'a>(document: &'a Value, name: &str) -> Option<&'a Value> {
    document["checks"]
        .as_array()?
        .iter()
        .find(|check| check["name"] == name)
}

fn write_config(
    directory: &Path,
    name: &str,
    access: &str,
    frontend: &str,
    publication: &str,
    service_address: Option<&str>,
) -> PathBuf {
    let profile = std::env::var("YINYANG_STORAGE_PROFILE").unwrap_or_else(|_| "amazon-s3".into());
    write_config_with_profile(
        directory,
        name,
        access,
        frontend,
        publication,
        service_address,
        &profile,
    )
}

fn write_config_with_profile(
    directory: &Path,
    name: &str,
    access: &str,
    frontend: &str,
    publication: &str,
    service_address: Option<&str>,
    profile: &str,
) -> PathBuf {
    let mut value = serde_json::json!({
        "name": name,
        "model": "managed",
        "access": access,
        "frontend": frontend,
        "publication": publication,
        "storage_profile": profile,
        "staging": directory.join(format!("{name}-state")),
        "read_only": false,
        "require": [],
    });
    if let Some(address) = service_address {
        value["service_address"] = address.into();
    }
    let path = directory.join(format!("{name}.json"));
    fs::write(&path, serde_json::to_vec_pretty(&value).unwrap()).unwrap();
    path
}

fn rewrite_staging(path: &Path, staging: &Path) {
    let mut value: Value = serde_json::from_slice(&fs::read(path).unwrap()).unwrap();
    value["staging"] = staging.display().to_string().into();
    fs::write(path, serde_json::to_vec_pretty(&value).unwrap()).unwrap();
}

struct Install {
    prefix: PathBuf,
}

impl Install {
    fn new(root: &Path) -> Self {
        let prefix = root.join("prefix");
        let bin = prefix.join("bin");
        fs::create_dir_all(&bin).unwrap();
        let yy = bin.join("yy");
        fs::copy(env!("CARGO_BIN_EXE_yy"), &yy).unwrap();
        let mut permissions = fs::metadata(&yy).unwrap().permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&yy, permissions).unwrap();
        Self { prefix }
    }

    fn run(
        &self,
        storage: &Storage,
        work: &Path,
        args: &[&str],
        root: &str,
        token: Option<&str>,
    ) -> Captured {
        let mut command = self.command(storage, work, args, root, token);
        let output = command.output().expect("run installed yy");
        Captured {
            status: output.status,
            stdout: output.stdout,
            stderr: output.stderr,
        }
    }

    fn require_ok(&self, storage: &Storage, work: &Path, args: &[&str], root: &str) -> Captured {
        let output = self.run(storage, work, args, root, None);
        output.success_or(&format!("installed yy {args:?}"));
        output
    }

    fn spawn(
        &self,
        storage: &Storage,
        work: &Path,
        args: &[&str],
        root: &str,
        token: Option<&str>,
    ) -> Child {
        self.command(storage, work, args, root, token)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn installed yy")
    }

    fn command(
        &self,
        storage: &Storage,
        work: &Path,
        args: &[&str],
        root: &str,
        token: Option<&str>,
    ) -> Command {
        let mut command = Command::new(self.prefix.join("bin/yy"));
        command.args(args).current_dir(work).env_clear().envs(
            std::env::vars_os().filter(|(name, _)| !name.to_string_lossy().starts_with("YINYANG_")),
        );
        for (name, value) in storage.environment() {
            command.env(name, value);
        }
        command.env("YINYANG_S3_ROOT", root);
        if let Some(token) = token.or_else(|| storage.service_default.then_some(SERVICE_TOKEN)) {
            command.env("YINYANG_SERVICE_TOKEN", token);
        }
        command
    }
}

#[derive(Debug)]
struct Storage {
    bucket: String,
    region: String,
    profile: String,
    endpoint: Option<String>,
    access_key_id: String,
    secret_access_key: String,
    session_token: Option<String>,
    service_default: bool,
}

impl Storage {
    fn from_env() -> Self {
        let profile = required("YINYANG_STORAGE_PROFILE");
        assert!(
            matches!(profile.as_str(), "amazon-s3" | "minio"),
            "unsupported YINYANG_STORAGE_PROFILE={profile:?}"
        );
        let endpoint = std::env::var("YINYANG_S3_ENDPOINT").ok();
        if profile == "minio" {
            assert!(
                endpoint.is_some(),
                "YINYANG_S3_ENDPOINT is required for MinIO qualification"
            );
        } else {
            assert!(
                endpoint.is_none(),
                "real Amazon S3 qualification must not inherit a MinIO endpoint"
            );
        }
        Self {
            bucket: required("YINYANG_S3_BUCKET"),
            region: required("YINYANG_S3_REGION"),
            profile,
            endpoint,
            access_key_id: required("YINYANG_S3_ACCESS_KEY_ID"),
            secret_access_key: required("YINYANG_S3_SECRET_ACCESS_KEY"),
            session_token: std::env::var("YINYANG_S3_SESSION_TOKEN").ok(),
            service_default: false,
        }
    }

    fn environment(&self) -> Vec<(&'static str, String)> {
        let mut values = vec![
            ("YINYANG_S3_BUCKET", self.bucket.clone()),
            ("YINYANG_S3_REGION", self.region.clone()),
            ("YINYANG_STORAGE_PROFILE", self.profile.clone()),
            ("YINYANG_S3_ACCESS_KEY_ID", self.access_key_id.clone()),
            (
                "YINYANG_S3_SECRET_ACCESS_KEY",
                self.secret_access_key.clone(),
            ),
        ];
        if let Some(endpoint) = &self.endpoint {
            values.push(("YINYANG_S3_ENDPOINT", endpoint.clone()));
        }
        if let Some(token) = &self.session_token {
            values.push(("YINYANG_S3_SESSION_TOKEN", token.clone()));
        }
        values
    }

    fn root(&self, label: &str) -> String {
        format!(
            "linux-operations/{label}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        )
    }
}

fn required(name: &str) -> String {
    std::env::var(name)
        .unwrap_or_else(|_| panic!("{name} is required by the Linux operations smoke"))
}

fn start_service(
    install: &Install,
    storage: &Storage,
    work: &Path,
    database: &Path,
    root: &str,
) -> Child {
    install.spawn(
        storage,
        work,
        &[
            "serve",
            "--database",
            database.to_str().unwrap(),
            "--listen",
            "127.0.0.1:0",
        ],
        root,
        Some(SERVICE_TOKEN),
    )
}

fn wait_doctor_ready(
    install: &Install,
    storage: &Storage,
    work: &Path,
    config: &Path,
    root: &str,
    unit: &str,
) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let output = install.run(
            storage,
            work,
            &["--volume", config.to_str().unwrap(), "doctor", "--json"],
            root,
            Some(SERVICE_TOKEN),
        );
        if output.status.success() {
            return;
        }
        if Instant::now() >= deadline {
            let status = systemctl(&["status", unit, "--no-pager", "-l"]);
            panic!(
                "{unit} did not become doctor-ready within 30s:\nstdout:\n{}\nstderr:\n{}\nstatus:\n{}\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr),
                String::from_utf8_lossy(&status.stdout),
                String::from_utf8_lossy(&status.stderr),
            );
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn wait_service(child: &mut Child) -> SocketAddr {
    let stdout = child.stdout.take().expect("service stdout");
    let mut line = String::new();
    BufReader::new(stdout)
        .read_line(&mut line)
        .expect("read service startup line");
    line.trim()
        .strip_prefix("metadata service listening on ")
        .unwrap_or_else(|| panic!("unexpected service startup line: {line:?}"))
        .parse()
        .expect("parse service address")
}

fn wait_mount(mountpoint: &Path, mut child: Option<&mut Child>) {
    let expected = mountpoint.display().to_string();
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if let Some(child) = child.as_deref_mut() {
            if let Some(status) = child.try_wait().expect("poll mount process") {
                panic!("mount exited before becoming ready: {status}");
            }
        }
        let mounts = fs::read_to_string("/proc/mounts").unwrap();
        if mounts.lines().any(|line| {
            let mut fields = line.split_whitespace();
            fields.next() == Some(expected.as_str()) && fields.next() == Some("fuse.yinyang")
        }) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "mount did not become ready within 60s: {expected}"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

struct UserUnits {
    directory: PathBuf,
    environment: PathBuf,
    overlays: Vec<PathBuf>,
    created_config_root: bool,
    created_unit_directory: bool,
    serve: String,
    mount: String,
    sync: String,
}

impl UserUnits {
    fn install(
        install: &Install,
        storage: &Storage,
        configs: &Path,
        state: &Path,
        mounts: &Path,
        service_address: SocketAddr,
        roots: [&str; 3],
    ) -> Self {
        let directory = user_unit_directory();
        let created_unit_directory = !directory.exists();
        fs::create_dir_all(&directory).unwrap();
        let config_root = user_config_root();
        let created_config_root = !config_root.exists();
        fs::create_dir_all(&config_root).unwrap();
        let environment = config_root.join("environment");
        let overlays = [
            config_root.join("environment-service"),
            config_root.join("environment-object"),
            config_root.join("environment-sync"),
        ];
        for path in [
            environment.clone(),
            overlays[0].clone(),
            overlays[1].clone(),
            overlays[2].clone(),
            directory.join("yinyang-serve.service"),
            directory.join("yinyang-mount@.service"),
            directory.join("yinyang-sync@.service"),
        ] {
            assert!(
                !path.exists(),
                "refusing to overwrite pre-existing systemd qualification path {}",
                path.display()
            );
        }
        write_systemd_environment(
            &environment,
            storage,
            install,
            configs,
            mounts,
            state,
            service_address,
        );
        for (path, root) in overlays.iter().zip(roots) {
            write_private_file(path, format!("YINYANG_S3_ROOT={root}\n"));
        }
        let templates = Path::new(env!("CARGO_MANIFEST_DIR")).join("deploy/linux");
        let units = Self {
            directory: directory.clone(),
            environment: environment.clone(),
            overlays: overlays.to_vec(),
            created_config_root,
            created_unit_directory,
            serve: "yinyang-serve.service".to_owned(),
            mount: "yinyang-mount@.service".to_owned(),
            sync: "yinyang-sync@.service".to_owned(),
        };
        for (template, destination) in [
            ("yinyang-serve.service", &units.serve),
            ("yinyang-mount@.service", &units.mount),
            ("yinyang-sync@.service", &units.sync),
        ] {
            let source = templates.join(template);
            let target = directory.join(destination);
            fs::copy(&source, &target).unwrap();
            assert_eq!(
                fs::read(source).unwrap(),
                fs::read(target).unwrap(),
                "the systemd lifecycle test must use the exact repository template"
            );
        }
        systemctl(&["daemon-reload"]).success_or("systemctl --user daemon-reload");
        units
    }

    fn mount_instance(&self, name: &str) -> String {
        format!(
            "{}@{name}.service",
            self.mount.trim_end_matches("@.service")
        )
    }

    fn sync_instance(&self, name: &str) -> String {
        format!("{}@{name}.service", self.sync.trim_end_matches("@.service"))
    }

    fn start(&self, unit: &str) {
        systemctl(&["start", unit]).success_or(&format!("systemctl --user start {unit}"));
    }

    fn restart(&self, unit: &str) {
        systemctl(&["restart", unit]).success_or(&format!("systemctl --user restart {unit}"));
    }

    fn stop(&self, unit: &str) {
        systemctl(&["stop", unit]).success_or(&format!("systemctl --user stop {unit}"));
        let output = systemctl(&["is-active", unit]);
        assert!(
            !output.status.success(),
            "{unit} remained active after stop:\n{}",
            String::from_utf8_lossy(&output.stdout)
        );
    }

    fn wait_active(&self, unit: &str) {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let output = systemctl(&["is-active", unit]);
            if output.status.success() {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "{unit} did not become active within 30s:\nstdout:\n{}\nstderr:\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            std::thread::sleep(Duration::from_millis(100));
        }
    }
}

impl Drop for UserUnits {
    fn drop(&mut self) {
        for unit in [
            self.mount_instance("object"),
            self.sync_instance("sync"),
            self.serve.clone(),
        ] {
            let _ = systemctl(&["stop", &unit]);
        }
        for unit in [&self.serve, &self.mount, &self.sync] {
            let _ = fs::remove_file(self.directory.join(unit));
        }
        let _ = fs::remove_file(&self.environment);
        for overlay in &self.overlays {
            let _ = fs::remove_file(overlay);
        }
        if self.created_config_root {
            let _ = fs::remove_dir(&self.environment.parent().unwrap());
        }
        if self.created_unit_directory {
            let _ = fs::remove_dir(&self.directory);
        }
        let _ = systemctl(&["daemon-reload"]);
    }
}

fn user_unit_directory() -> PathBuf {
    let config = std::env::var_os("XDG_CONFIG_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".config")))
        .expect("HOME or XDG_CONFIG_HOME is required");
    config.join("systemd/user")
}

fn user_config_root() -> PathBuf {
    let home = std::env::var_os("HOME").expect("HOME is required by the user units");
    PathBuf::from(home).join(".config/yinyang")
}

fn write_systemd_environment(
    path: &Path,
    storage: &Storage,
    install: &Install,
    configs: &Path,
    mounts: &Path,
    state: &Path,
    service_address: SocketAddr,
) {
    let mut content = format!(
        "YINYANG_BIN={}\nYINYANG_CONFIG_DIR={}\nYINYANG_MOUNT_ROOT={}\nYINYANG_METADATA_DB={}\nYINYANG_SERVICE_LISTEN={service_address}\n",
        install.prefix.join("bin/yy").display(),
        configs.display(),
        mounts.display(),
        state.join("metadata/metadata.db").display(),
    );
    for (name, value) in storage.environment() {
        content.push_str(&format!("{name}={value}\n"));
    }
    content.push_str(&format!("YINYANG_SERVICE_TOKEN={SERVICE_TOKEN}\n"));
    write_private_file(path, content);
}

fn write_private_file(path: &Path, content: String) {
    use std::io::Write as _;
    let mut file = fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .open(path)
        .unwrap();
    file.write_all(content.as_bytes()).unwrap();
    file.sync_all().unwrap();
}

fn systemctl(args: &[&str]) -> Output {
    Command::new("systemctl")
        .arg("--user")
        .args(args)
        .output()
        .expect("run systemctl --user")
}

trait ExitStatusExt {
    fn success_or(&self, context: &str);
}

impl ExitStatusExt for Output {
    fn success_or(&self, context: &str) {
        assert!(
            self.status.success(),
            "{context} failed with {}:\nstdout:\n{}\nstderr:\n{}",
            self.status,
            String::from_utf8_lossy(&self.stdout),
            String::from_utf8_lossy(&self.stderr)
        );
    }
}

fn stop(child: &mut Child) {
    if let Some(status) = child.try_wait().expect("poll process") {
        assert!(status.success(), "process exited before stop: {status}");
        return;
    }
    kill(Pid::from_raw(child.id() as i32), Signal::SIGTERM).expect("send SIGTERM");
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if child.try_wait().expect("poll stopped process").is_some() {
            return;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("process did not stop within 15s after SIGTERM");
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn executable_on_path(name: &str) -> bool {
    std::env::var_os("PATH").is_some_and(|path| {
        std::env::split_paths(&path).any(|directory| {
            fs::metadata(directory.join(name))
                .map(|metadata| metadata.is_file() && metadata.permissions().mode() & 0o111 != 0)
                .unwrap_or(false)
        })
    })
}

struct Captured {
    status: std::process::ExitStatus,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
}

impl Captured {
    fn success_or(&self, context: &str) {
        assert!(
            self.status.success(),
            "{context} failed with {}:\nstdout:\n{}\nstderr:\n{}",
            self.status,
            String::from_utf8_lossy(&self.stdout),
            String::from_utf8_lossy(&self.stderr)
        );
    }
}
