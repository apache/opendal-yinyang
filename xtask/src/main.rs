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

fn main() {
    let cmd = Command::parse();
    cmd.run();
}

#[derive(Parser)]
#[clap(about = "Run repository tasks.")]
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
}
impl CommandMacos {
    fn run(self) {
        if !cfg!(target_os = "macos") {
            panic!("native macOS build requires macOS");
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
            "platforms/macos/project.yml",
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
        } else {
            build.arg("CODE_SIGNING_ALLOWED=NO");
        }
        run_command(build);
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
