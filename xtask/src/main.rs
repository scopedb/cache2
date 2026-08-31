// Copyright 2026 ScopeDB, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::env;
use std::ffi::{OsStr, OsString};
use std::path::Path;
use std::process::Command as StdCommand;

use cargo_metadata::{Metadata, MetadataCommand};
use clap::{Parser, Subcommand};

const PACKAGE_NAME: &str = "cache2";

fn main() {
    Command::parse().run();
}

#[derive(Parser)]
#[command(
    name = "cargo x",
    bin_name = "cargo x",
    about = "Repository workflows for C²"
)]
struct Command {
    #[command(subcommand)]
    sub: SubCommand,
}

impl Command {
    fn run(self) {
        match self.sub {
            SubCommand::Bench(command) => command.run(),
            SubCommand::Check(command) => command.run(),
            SubCommand::Lint(command) => command.run(),
            SubCommand::Test(command) => command.run(),
        }
    }
}

#[derive(Subcommand)]
enum SubCommand {
    #[command(about = "Run benchmarks, forwarding arguments to Cargo and the harness")]
    Bench(CommandBench),
    #[command(about = "Check the workspace and cache2 feature matrix")]
    Check(CommandCheck),
    #[command(about = "Run formatting, lint, documentation, package, and policy checks")]
    Lint(CommandLint),
    #[command(about = "Run workspace tests and extended library tests")]
    Test(CommandTest),
}

#[derive(Parser)]
struct CommandBench {
    #[arg(
        value_name = "CARGO_ARGS",
        allow_hyphen_values = true,
        value_terminator = "--",
        help = "Arguments passed to `cargo bench`"
    )]
    cargo_args: Vec<OsString>,
    #[arg(
        value_name = "HARNESS_ARGS",
        allow_hyphen_values = true,
        help = "Arguments after `--` passed to the benchmark harness"
    )]
    harness_args: Vec<OsString>,
}

impl CommandBench {
    fn run(self) {
        run(self.command());
    }

    fn command(self) -> StdCommand {
        let mut command = cargo();
        command.args(["bench", "--package", "benchmarks"]);
        command.args(self.cargo_args);
        if !self.harness_args.is_empty() {
            command.arg("--").args(self.harness_args);
        }
        command
    }
}

#[derive(Parser)]
struct CommandCheck;

impl CommandCheck {
    fn run(self) {
        cargo_run([
            "check",
            "--package",
            PACKAGE_NAME,
            "--all-targets",
            "--no-default-features",
        ]);
        for feature in cache2_features() {
            let mut command = cargo();
            command.args([
                "check",
                "--package",
                PACKAGE_NAME,
                "--lib",
                "--no-default-features",
                "--features",
            ]);
            command.arg(feature);
            run(command);
        }
        cargo_run(["check", "--workspace", "--all-targets", "--all-features"]);
    }
}

fn cache2_features() -> Vec<String> {
    let manifest = Path::new(env!("CARGO_WORKSPACE_DIR")).join("Cargo.toml");
    let Metadata { packages, .. } = MetadataCommand::new()
        .manifest_path(manifest)
        .no_deps()
        .exec()
        .expect("failed to get cargo metadata");
    let package = packages
        .into_iter()
        .find(|package| package.name == PACKAGE_NAME)
        .expect("failed to find cache2 package");

    let mut features = package
        .features
        .into_keys()
        .filter(|feature| feature != "default")
        .collect::<Vec<_>>();
    features.sort();
    features
}

#[derive(Parser)]
struct CommandLint;

impl CommandLint {
    fn run(self) {
        cargo_run(["fmt", "--all", "--", "--check"]);
        cargo_run([
            "clippy",
            "--workspace",
            "--all-targets",
            "--all-features",
            "--",
            "-D",
            "warnings",
        ]);

        let mut docs = cargo();
        docs.env("RUSTDOCFLAGS", "-D warnings -D missing_docs");
        docs.args(["doc", "--package", PACKAGE_NAME, "--no-deps", "--locked"]);
        run(docs);

        cargo_run([
            "package",
            "--package",
            PACKAGE_NAME,
            "--locked",
            "--allow-dirty",
        ]);
        command_run("hawkeye", ["check"]);
        cargo_run([
            "deny",
            "--all-features",
            "check",
            "advisories",
            "licenses",
            "bans",
            "sources",
        ]);
    }
}

#[derive(Parser)]
struct CommandTest;

impl CommandTest {
    fn run(self) {
        cargo_run(["test", "--workspace", "--all-features"]);
        cargo_run([
            "test",
            "--package",
            PACKAGE_NAME,
            "--lib",
            "--all-features",
            "--",
            "--ignored",
        ]);
    }
}

fn cargo() -> StdCommand {
    let executable = env::var_os("CARGO").unwrap_or_else(|| OsString::from("cargo"));
    let mut command = StdCommand::new(executable);
    command.current_dir(Path::new(env!("CARGO_WORKSPACE_DIR")));
    command
}

fn cargo_run<I, S>(args: I)
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let mut command = cargo();
    command.args(args);
    run(command);
}

fn command_run<I, S>(executable: &str, args: I)
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let mut command = StdCommand::new(executable);
    command
        .current_dir(Path::new(env!("CARGO_WORKSPACE_DIR")))
        .args(args);
    run(command);
}

fn run(mut command: StdCommand) {
    println!("{command:?}");
    match command.status() {
        Ok(status) if status.success() => {}
        Ok(status) => std::process::exit(status.code().unwrap_or(1)),
        Err(error) => fail(&format!("failed to run {command:?}: {error}")),
    }
}

fn fail(message: &str) -> ! {
    eprintln!("{message}");
    std::process::exit(2)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bench_keeps_cargo_and_harness_arguments_separate() {
        let command = Command::try_parse_from([
            "cargo x",
            "bench",
            "--bench",
            "cache",
            "--",
            "--sample-size",
            "20",
        ])
        .unwrap();
        let SubCommand::Bench(command) = command.sub else {
            panic!("expected bench command");
        };

        let args = command
            .command()
            .get_args()
            .map(OsStr::to_os_string)
            .collect::<Vec<_>>();
        assert_eq!(
            args,
            [
                "bench",
                "--package",
                "benchmarks",
                "--bench",
                "cache",
                "--",
                "--sample-size",
                "20",
            ]
            .map(OsString::from)
        );
    }
}
