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

//! Repository tasks shared by local development and CI.

use std::process::Command as StdCommand;

use clap::Parser;
use clap::Subcommand;
use clap::ValueEnum;
use clap::error::ErrorKind;

fn main() {
    let cmd = match Command::try_parse() {
        Ok(cmd) => cmd,
        Err(error) => {
            let _ = error.print();
            std::process::exit(parse_error_exit_code(error.kind()));
        }
    };
    cmd.run();
}

fn parse_error_exit_code(kind: ErrorKind) -> i32 {
    match kind {
        ErrorKind::DisplayHelp | ErrorKind::DisplayVersion => 0,
        _ => 2,
    }
}

#[derive(Parser)]
#[clap(about = "Run repository tasks.", version, propagate_version = true)]
struct Command {
    #[clap(subcommand)]
    sub: SubCommand,
}

impl Command {
    fn run(self) {
        match self.sub {
            SubCommand::Check(cmd) => cmd.run(),
            SubCommand::Licenses(cmd) => cmd.run(),
            SubCommand::Lint(cmd) => cmd.run(),
            SubCommand::Test(cmd) => cmd.run(),
            SubCommand::Macos(cmd) => cmd.run(),
            SubCommand::BenchLinuxCost(cmd) => cmd.run(),
            SubCommand::TestManagedSync(cmd) => cmd.run(),
            SubCommand::TestCompatibility(cmd) => cmd.run(),
            SubCommand::TestLinuxApplications(cmd) => cmd.run(),
            SubCommand::TestLinuxDirect(cmd) => cmd.run(),
            SubCommand::TestLinuxFuse(cmd) => cmd.run(),
            SubCommand::TestLinuxOperations(cmd) => cmd.run(),
            SubCommand::TestLinuxRecovery(cmd) => cmd.run(),
        }
    }
}

#[derive(Subcommand)]
enum SubCommand {
    #[clap(about = "Check all workspace targets.")]
    Check(CommandCheck),
    #[clap(about = "Check source headers and dependency licenses.")]
    Licenses(CommandLicenses),
    #[clap(about = "Run source and documentation linters.")]
    Lint(CommandLint),
    #[clap(about = "Run all workspace tests.")]
    Test(CommandTest),
    #[clap(
        about = "Measure Linux FUSE, Managed Sync, and one-shot transfer product costs (requires isolated S3 and /dev/fuse)."
    )]
    BenchLinuxCost(CommandBenchLinuxCost),
    #[clap(
        about = "Run real Linux Managed Sync acceptance through yy and ordinary directories (requires isolated S3 settings)."
    )]
    TestManagedSync(CommandTestManagedSync),
    #[clap(
        about = "Run fd1338c cross-version object, service, staging, Sync, and journal acceptance (requires YY_LEGACY_BIN and isolated S3)."
    )]
    TestCompatibility(CommandTestCompatibility),
    #[clap(
        about = "Run real rsync and GNU tar workflows through yy and /dev/fuse (requires isolated S3)."
    )]
    TestLinuxApplications(CommandTestLinuxApplications),
    #[clap(
        about = "Run deterministic Direct Mount and Sync acceptance through yy and /dev/fuse (requires isolated S3 settings)."
    )]
    TestLinuxDirect(CommandTestLinuxDirect),
    #[clap(
        about = "Run real Linux FUSE acceptance through yy and /dev/fuse (requires isolated S3 settings)."
    )]
    TestLinuxFuse(CommandTestLinuxFuse),
    #[clap(
        about = "Run installed Linux CLI operations through yy and /dev/fuse (requires isolated storage)."
    )]
    TestLinuxOperations(CommandTestLinuxOperations),
    #[clap(
        about = "Run isolated Linux backup, restore, pending-state, and disk-exhaustion recovery."
    )]
    TestLinuxRecovery(CommandTestLinuxRecovery),
    #[clap(about = "Build the macOS native prototypes (requires Xcode 27 and XcodeGen).")]
    Macos(CommandMacos),
}

#[derive(Parser)]
struct CommandMacos {
    #[arg(long, default_value = "target/macos")]
    output: std::path::PathBuf,
    #[arg(long, requires_all = ["team", "fskit_profile"])]
    identity: Option<String>,
    #[arg(long, requires = "identity")]
    team: Option<String>,
    #[arg(long, requires = "identity")]
    fskit_profile: Option<String>,
    #[arg(long)]
    file_provider: bool,
    #[arg(long, requires_all = ["identity", "file_provider"])]
    host_profile: Option<String>,
    #[arg(long, requires_all = ["identity", "file_provider"])]
    provider_profile: Option<String>,
    #[arg(
        long,
        requires = "file_provider",
        help = "Run real-core Swift callbacks against an isolated native config (creates a test subtree)."
    )]
    test_config: Option<std::path::PathBuf>,
}
impl CommandMacos {
    fn run(self) {
        if !cfg!(target_os = "macos") {
            panic!("native macOS build requires macOS");
        }
        if self.file_provider && self.identity.is_some() {
            assert!(
                self.host_profile.is_some() && self.provider_profile.is_some(),
                "signed File Provider builds require --host-profile and --provider-profile"
            );
        }
        let root = std::path::Path::new(env!("CARGO_WORKSPACE_DIR"));
        let output = root.join(self.output);
        let target =
            root.join(std::env::var_os("CARGO_TARGET_DIR").unwrap_or_else(|| "target".into()));
        let mut cargo = find_command("cargo");
        cargo.args(["build", "-p", "yinyang-native"]);
        run_command(cargo);
        std::fs::create_dir_all(&output).expect("create native build directory");
        let mut generate = find_command("xcodegen");
        generate.args([
            "generate",
            "--spec",
            if self.file_provider {
                "platforms/macos/provider.yml"
            } else {
                "platforms/macos/project.yml"
            },
            "--project",
        ]);
        generate.arg(&output);
        run_command(generate);
        let mut build = find_command("xcodebuild");
        build.arg("-project").arg(output.join("YinYang.xcodeproj"));
        build.args([
            "-scheme",
            "YinYang",
            "-configuration",
            "Debug",
            "-derivedDataPath",
        ]);
        build.arg(output.join("DerivedData"));
        build.args(["-destination", "platform=macOS", "build"]);
        build.arg(format!("YINYANG_SOURCE_ROOT={}", root.display()));
        build.arg(format!(
            "YINYANG_NATIVE_LIBRARY={}",
            target.join("debug/libyinyang_native.a").display()
        ));
        build.arg(format!(
            "YINYANG_BRIDGING_HEADER={}",
            root.join("crates/native/include/yinyang.h").display()
        ));
        if let Some(identity) = self.identity {
            build.arg(format!("CODE_SIGN_IDENTITY={identity}"));
            build.arg(format!(
                "DEVELOPMENT_TEAM={}",
                self.team.expect("team required")
            ));
            build.arg(format!(
                "YINYANG_FSKIT_PROFILE={}",
                self.fskit_profile.expect("profile required")
            ));
            if self.file_provider {
                build.arg(format!(
                    "YINYANG_HOST_PROFILE={}",
                    self.host_profile.unwrap()
                ));
                build.arg(format!(
                    "YINYANG_PROVIDER_PROFILE={}",
                    self.provider_profile.unwrap()
                ));
            }
        } else {
            build.arg("CODE_SIGNING_ALLOWED=NO");
        }
        run_command(build);
        if let Some(config) = self.test_config {
            let binary = output.join("provider-contract-tests");
            let mut swift = find_command("xcrun");
            swift.args(["swiftc", "-swift-version", "5", "-import-objc-header"]);
            swift.arg(root.join("crates/native/include/yinyang.h"));
            swift.args([
                "platforms/macos/Shared/NativeBridge.swift",
                "platforms/macos/FileProvider/Item.swift",
                "platforms/macos/FileProvider/Extension.swift",
                "platforms/macos/FileProvider/Enumerator.swift",
                "platforms/macos/Tests/ProviderTests.swift",
            ]);
            swift.arg(target.join("debug/libyinyang_native.a"));
            swift.args([
                "-framework",
                "Security",
                "-framework",
                "SystemConfiguration",
                "-lc++",
                "-o",
            ]);
            swift.arg(&binary);
            run_command(swift);
            let mut test = std::process::Command::new(binary);
            test.arg(root.join(config));
            run_command(test);
        }
    }
}

#[derive(Parser)]
#[clap(name = "check")]
struct CommandCheck {}

impl CommandCheck {
    fn run(self) {
        run_command(make_check_cmd());
    }
}

#[derive(Parser)]
#[clap(name = "licenses")]
struct CommandLicenses {}

impl CommandLicenses {
    fn run(self) {
        run_command(make_hawkeye_cmd());
        run_command(make_cargo_deny_cmd());
    }
}

#[derive(Parser)]
#[clap(name = "lint")]
struct CommandLint {
    #[arg(long, help = "Automatically apply available lint suggestions.")]
    fix: bool,
}

impl CommandLint {
    fn run(self) {
        run_command(make_clippy_cmd(self.fix));
        run_command(make_format_cmd(self.fix));
        run_command(make_docs_cmd());
        run_command(make_taplo_cmd(self.fix));
        run_command(make_typos_cmd());
    }
}

#[derive(Parser)]
#[clap(name = "test")]
struct CommandTest {
    #[arg(long, help = "Do not capture test output.")]
    no_capture: bool,
}

impl CommandTest {
    fn run(self) {
        run_command(make_test_cmd(self.no_capture));
    }
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum BenchProfile {
    Early,
    Full,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum BenchPublication {
    All,
    Object,
    Service,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum BenchWorkload {
    Full,
    ReadOnlyRange,
    Incremental,
    LengthChanging,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum BenchIncrementalExpectation {
    Baseline,
    Required,
}

#[derive(Parser)]
#[clap(name = "bench-linux-cost")]
struct CommandBenchLinuxCost {
    #[arg(long, value_enum, default_value = "early")]
    profile: BenchProfile,
    #[arg(
        long,
        value_enum,
        default_value = "all",
        help = "Limit authority-specific workloads while preserving the selected profile's scales."
    )]
    publication: BenchPublication,
    #[arg(
        long,
        value_enum,
        default_value = "full",
        help = "Select the complete product matrix, the matched read-only range workload, the same-length writable workload, or the length-changing writable workload."
    )]
    workload: BenchWorkload,
    #[arg(
        long,
        value_enum,
        default_value = "baseline",
        help = "For writable incremental workloads, record baseline behavior or reject payload traffic consistent with full-file fallback."
    )]
    incremental_expect: BenchIncrementalExpectation,
    #[arg(
        long,
        help = "Write JSONL evidence here; a Markdown summary uses the same path with .md."
    )]
    output: std::path::PathBuf,
}

impl CommandBenchLinuxCost {
    fn run(self) {
        if !cfg!(target_os = "linux") {
            panic!("Linux cost benchmark requires Linux");
        }
        if !std::path::Path::new("/dev/fuse").exists() {
            panic!("/dev/fuse is unavailable; refusing to benchmark without a real mount");
        }
        if which::which("fusermount3").is_err() {
            panic!("fusermount3 is unavailable; install fuse3 before running the benchmark");
        }
        for name in [
            "YINYANG_S3_BUCKET",
            "YINYANG_S3_ENDPOINT",
            "YINYANG_S3_REGION",
            "YINYANG_S3_ACCESS_KEY_ID",
            "YINYANG_S3_SECRET_ACCESS_KEY",
        ] {
            if std::env::var_os(name).is_none() {
                panic!("{name} is unset; the Linux cost benchmark fails hard instead of skipping");
            }
        }
        let profile = match self.workload {
            BenchWorkload::ReadOnlyRange
            | BenchWorkload::Incremental
            | BenchWorkload::LengthChanging => "full",
            BenchWorkload::Full => match self.profile {
                BenchProfile::Early => "early",
                BenchProfile::Full => "full",
            },
        };
        let publication = match self.publication {
            BenchPublication::All => "all",
            BenchPublication::Object => "object",
            BenchPublication::Service => "service",
        };
        let workload = match self.workload {
            BenchWorkload::Full => "full",
            BenchWorkload::ReadOnlyRange => "read-only-range",
            BenchWorkload::Incremental => "incremental",
            BenchWorkload::LengthChanging => "length-changing",
        };
        let incremental_expect = match self.incremental_expect {
            BenchIncrementalExpectation::Baseline => "baseline",
            BenchIncrementalExpectation::Required => "required",
        };
        let mut cmd = find_command("cargo");
        cmd.args([
            "test",
            "--release",
            "-p",
            "yinyang",
            "--test",
            "linux_cost",
            "linux_cost_benchmark",
            "--",
            "--ignored",
            "--nocapture",
            "--test-threads=1",
        ]);
        cmd.env("YINYANG_COST_PROFILE", profile);
        cmd.env("YINYANG_COST_WORKLOAD", workload);
        cmd.env("YINYANG_COST_PUBLICATION", publication);
        cmd.env("YINYANG_COST_INCREMENTAL_EXPECT", incremental_expect);
        cmd.env("YINYANG_COST_OUTPUT", &self.output);
        cmd.env("YINYANG_COST_SUMMARY", self.output.with_extension("md"));
        cmd.env(
            "YINYANG_COST_SOURCE_SHA",
            git_output(&["rev-parse", "HEAD"]),
        );
        cmd.env(
            "YINYANG_COST_SOURCE_DIRTY",
            if git_output(&["status", "--porcelain"]).is_empty() {
                "0"
            } else {
                "1"
            },
        );
        run_command(cmd);
    }
}

#[derive(Parser)]
#[clap(name = "test-managed-sync")]
struct CommandTestManagedSync {}

impl CommandTestManagedSync {
    fn run(self) {
        if !cfg!(target_os = "linux") {
            panic!("Managed Sync acceptance requires Linux");
        }
        for name in [
            "YINYANG_S3_BUCKET",
            "YINYANG_S3_ENDPOINT",
            "YINYANG_S3_REGION",
            "YINYANG_S3_ACCESS_KEY_ID",
            "YINYANG_S3_SECRET_ACCESS_KEY",
        ] {
            if std::env::var_os(name).is_none() {
                panic!("{name} is unset; Managed Sync acceptance fails hard instead of skipping");
            }
        }
        let mut cmd = find_command("cargo");
        cmd.args([
            "test",
            "-p",
            "yinyang",
            "--test",
            "linux_sync",
            "managed_sync_",
            "--",
            "--ignored",
            "--nocapture",
            "--test-threads=1",
        ]);
        run_command(cmd);
    }
}

#[derive(Parser)]
#[clap(name = "test-compatibility")]
struct CommandTestCompatibility {}

impl CommandTestCompatibility {
    fn run(self) {
        require_linux("cross-version compatibility acceptance");
        require_isolated_storage("cross-version compatibility acceptance");
        let legacy = std::env::var_os("YY_LEGACY_BIN").unwrap_or_else(|| {
            panic!(
                "YY_LEGACY_BIN is unset; cross-version acceptance must use the fd1338c producer binary"
            )
        });
        if !std::path::Path::new(&legacy).is_file() {
            panic!(
                "YY_LEGACY_BIN does not name a file: {}",
                std::path::Path::new(&legacy).display()
            );
        }
        run_ignored_test("compatibility", Some("compatibility_"));
    }
}

#[derive(Parser)]
#[clap(name = "test-linux-applications")]
struct CommandTestLinuxApplications {}

impl CommandTestLinuxApplications {
    fn run(self) {
        require_linux("Linux application acceptance");
        require_fuse("Linux application acceptance");
        require_isolated_storage("Linux application acceptance");
        for tool in ["git", "rsync", "tar"] {
            if which::which(tool).is_err() {
                panic!(
                    "{tool} is unavailable; Linux application acceptance requires the tested application binaries"
                );
            }
        }
        let tar = find_command("tar")
            .arg("--version")
            .output()
            .expect("run tar --version");
        assert!(
            tar.status.success() && String::from_utf8_lossy(&tar.stdout).contains("GNU tar"),
            "Linux application acceptance requires GNU tar"
        );
        run_ignored_test("linux_applications", Some("application_"));
    }
}

#[derive(Parser)]
#[clap(name = "test-linux-direct")]
struct CommandTestLinuxDirect {}

impl CommandTestLinuxDirect {
    fn run(self) {
        require_linux("Direct acceptance");
        require_fuse("Direct acceptance");
        require_isolated_storage("Direct acceptance");
        if std::env::var_os("YINYANG_DIRECT_VERSIONED_BUCKET").is_none() {
            panic!(
                "YINYANG_DIRECT_VERSIONED_BUCKET is unset; Direct acceptance must cover a true-versioning bucket"
            );
        }
        run_ignored_test_skipping("linux_direct", Some("direct_"), &["direct_amazon_s3_"]);
    }
}

#[derive(Parser)]
#[clap(name = "test-linux-fuse")]
struct CommandTestLinuxFuse {
    #[arg(
        long,
        help = "Run only the read-only acceptance stage for independent first-layer validation."
    )]
    read_only: bool,
    #[arg(
        long,
        conflicts_with = "read_only",
        help = "Run only the read-only range-hydration acceptance scenarios."
    )]
    read_only_range: bool,
}

impl CommandTestLinuxFuse {
    fn run(self) {
        if !cfg!(target_os = "linux") {
            panic!("Linux FUSE acceptance requires Linux");
        }
        if !std::path::Path::new("/dev/fuse").exists() {
            panic!(
                "/dev/fuse is unavailable; refusing to report a green acceptance run without a real mount"
            );
        }
        if which::which("fusermount3").is_err() {
            panic!(
                "fusermount3 is unavailable; install fuse3 before running Linux FUSE acceptance"
            );
        }
        for name in [
            "YINYANG_S3_BUCKET",
            "YINYANG_S3_ENDPOINT",
            "YINYANG_S3_REGION",
            "YINYANG_S3_ACCESS_KEY_ID",
            "YINYANG_S3_SECRET_ACCESS_KEY",
        ] {
            if std::env::var_os(name).is_none() {
                panic!("{name} is unset; Linux FUSE acceptance fails hard instead of skipping");
            }
        }
        let mut cmd = find_command("cargo");
        cmd.args(["test", "-p", "yinyang", "--test", "linux_fuse"]);
        if self.read_only_range {
            cmd.arg("fuse_read_only_range_");
        } else if self.read_only {
            cmd.arg("fuse_read_only_");
        } else {
            cmd.arg("fuse_");
        }
        cmd.args(["--", "--ignored", "--nocapture", "--test-threads=1"]);
        run_command(cmd);
    }
}

#[derive(Parser)]
#[clap(name = "test-linux-operations")]
struct CommandTestLinuxOperations {
    #[arg(
        long,
        conflicts_with = "systemd",
        help = "Use real Amazon S3 and run the installed CLI smoke."
    )]
    amazon_s3: bool,
    #[arg(
        long,
        conflicts_with = "amazon_s3",
        help = "Run the explicit qualified-host user-systemd lifecycle instead of the CLI smoke."
    )]
    systemd: bool,
}

impl CommandTestLinuxOperations {
    fn run(self) {
        require_linux("Linux operations acceptance");
        require_fuse("Linux operations acceptance");
        if self.amazon_s3 {
            for name in [
                "YINYANG_S3_BUCKET",
                "YINYANG_S3_REGION",
                "YINYANG_S3_ACCESS_KEY_ID",
                "YINYANG_S3_SECRET_ACCESS_KEY",
            ] {
                if std::env::var_os(name).is_none() {
                    panic!("{name} is unset; Amazon S3 operations fail hard instead of skipping");
                }
            }
        } else {
            require_isolated_storage("Linux operations acceptance");
        }
        let filter = if self.amazon_s3 {
            "real_amazon_s3_cli_smoke"
        } else if self.systemd {
            "isolated_user_service_lifecycle"
        } else {
            "isolated_s3_cli_smoke"
        };
        run_ignored_test("linux_operations", Some(filter));
    }
}

#[derive(Parser)]
#[clap(name = "test-linux-recovery")]
struct CommandTestLinuxRecovery {}

impl CommandTestLinuxRecovery {
    fn run(self) {
        require_linux("Linux recovery acceptance");
        require_isolated_storage("Linux recovery acceptance");
        if which::which("sqlite3").is_err() {
            panic!(
                "sqlite3 is unavailable; Linux recovery acceptance requires consistent SQLite backup"
            );
        }
        if !find_command("sudo")
            .args(["-n", "true"])
            .status()
            .is_ok_and(|status| status.success())
        {
            panic!(
                "passwordless sudo is unavailable; Linux recovery acceptance needs a bounded tmpfs"
            );
        }
        run_ignored_test("linux_recovery", None);
    }
}

fn require_linux(context: &str) {
    if !cfg!(target_os = "linux") {
        panic!("{context} requires Linux");
    }
}

fn require_fuse(context: &str) {
    if !std::path::Path::new("/dev/fuse").exists() {
        panic!("{context} requires /dev/fuse");
    }
    if which::which("fusermount3").is_err() {
        panic!("{context} requires fusermount3");
    }
}

fn require_isolated_storage(context: &str) {
    for name in [
        "YINYANG_S3_BUCKET",
        "YINYANG_S3_ENDPOINT",
        "YINYANG_S3_REGION",
        "YINYANG_S3_ACCESS_KEY_ID",
        "YINYANG_S3_SECRET_ACCESS_KEY",
    ] {
        if std::env::var_os(name).is_none() {
            panic!("{name} is unset; {context} fails hard instead of skipping");
        }
    }
}

fn run_ignored_test(target: &str, filter: Option<&str>) {
    run_ignored_test_skipping(target, filter, &[]);
}

fn run_ignored_test_skipping(target: &str, filter: Option<&str>, skips: &[&str]) {
    let mut cmd = find_command("cargo");
    cmd.args(["test", "-p", "yinyang", "--test", target]);
    if let Some(filter) = filter {
        cmd.arg(filter);
    }
    cmd.args(["--", "--ignored", "--nocapture", "--test-threads=1"]);
    for skip in skips {
        cmd.args(["--skip", skip]);
    }
    run_command(cmd);
}

fn find_command(cmd: &str) -> StdCommand {
    match which::which(cmd) {
        Ok(exe) => {
            let mut cmd = StdCommand::new(exe);
            cmd.current_dir(env!("CARGO_WORKSPACE_DIR"));
            cmd
        }
        Err(err) => {
            panic!("{cmd} not found: {err}");
        }
    }
}

fn git_output(args: &[&str]) -> String {
    let output = find_command("git")
        .args(args)
        .output()
        .unwrap_or_else(|error| panic!("run git {args:?}: {error}"));
    assert!(
        output.status.success(),
        "git {args:?} failed: {}",
        output.status
    );
    String::from_utf8(output.stdout)
        .expect("Git output is UTF-8")
        .trim()
        .to_owned()
}

fn ensure_installed(bin: &str, crate_name: &str) {
    if which::which(bin).is_err() {
        let mut cmd = find_command("cargo");
        cmd.args(["install", crate_name]);
        run_command(cmd);
    }
}

fn run_command(mut cmd: StdCommand) {
    println!("{cmd:?}");
    let status = cmd.status().expect("failed to execute process");
    assert!(status.success(), "command failed: {status}");
}

fn make_check_cmd() -> StdCommand {
    let mut cmd = find_command("cargo");
    cmd.env("RUSTFLAGS", "-Dwarnings");
    cmd.args(["check", "--workspace", "--all-targets"]);
    cmd
}

fn make_test_cmd(no_capture: bool) -> StdCommand {
    let mut cmd = find_command("cargo");
    cmd.args(["test", "--workspace", "--all-targets"]);
    if no_capture {
        cmd.args(["--", "--nocapture"]);
    }
    cmd
}

fn make_format_cmd(fix: bool) -> StdCommand {
    let mut cmd = find_command("cargo");
    cmd.args(["fmt", "--all"]);
    if !fix {
        cmd.arg("--check");
    }
    cmd
}

fn make_clippy_cmd(fix: bool) -> StdCommand {
    let mut cmd = find_command("cargo");
    cmd.args(["clippy", "--workspace", "--all-targets"]);
    if fix {
        cmd.args(["--allow-staged", "--allow-dirty", "--fix"]);
    } else {
        cmd.args(["--", "-D", "warnings"]);
    }
    cmd
}

fn make_docs_cmd() -> StdCommand {
    let mut cmd = find_command("cargo");
    cmd.env("RUSTDOCFLAGS", "-D warnings");
    cmd.args(["doc", "--workspace", "--no-deps"]);
    cmd
}

fn make_taplo_cmd(fix: bool) -> StdCommand {
    ensure_installed("taplo", "taplo-cli");
    let mut cmd = find_command("taplo");
    if fix {
        cmd.arg("format");
    } else {
        cmd.args(["format", "--check"]);
    }
    cmd
}

fn make_typos_cmd() -> StdCommand {
    ensure_installed("typos", "typos-cli");
    find_command("typos")
}

fn make_hawkeye_cmd() -> StdCommand {
    ensure_installed("hawkeye", "hawkeye");
    let mut cmd = find_command("hawkeye");
    cmd.arg("check");
    cmd
}

fn make_cargo_deny_cmd() -> StdCommand {
    ensure_installed("cargo-deny", "cargo-deny");
    let mut cmd = find_command("cargo");
    cmd.args(["deny", "check", "licenses"]);
    cmd
}

#[cfg(test)]
mod tests {
    use super::parse_error_exit_code;
    use clap::error::ErrorKind;

    #[test]
    fn parse_errors_are_nonzero_except_help_and_version() {
        assert_eq!(parse_error_exit_code(ErrorKind::DisplayHelp), 0);
        assert_eq!(parse_error_exit_code(ErrorKind::DisplayVersion), 0);
        assert_eq!(parse_error_exit_code(ErrorKind::UnknownArgument), 2);
    }
}
