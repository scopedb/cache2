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
use std::ffi::OsString;
use std::path::Path;
use std::process::Command;

const USAGE: &str = "\
Repository workflows for C²

Usage: cargo x <COMMAND> [ARGS]

Commands:
  bench [ARGS]  Run benchmarks, forwarding ARGS to `cargo bench`
  check         Check the workspace and cache2 feature matrix
  lint          Run formatting, lint, documentation, package, and policy checks
  test          Run workspace tests and extended library tests
";

fn main() {
    let mut args = env::args_os().skip(1);
    let Some(command) = args.next() else {
        print!("{USAGE}");
        return;
    };
    let remaining = args.collect::<Vec<_>>();

    match command.to_str() {
        Some("bench") => bench(&remaining),
        Some("check") => {
            require_no_args("check", &remaining);
            check();
        }
        Some("lint") => {
            require_no_args("lint", &remaining);
            lint();
        }
        Some("test") => {
            require_no_args("test", &remaining);
            test();
        }
        Some("help" | "-h" | "--help") => print!("{USAGE}"),
        Some(command) => fail(&format!("unknown command `{command}`\n\n{USAGE}")),
        None => fail("command must be valid UTF-8"),
    }
}

fn bench(args: &[OsString]) {
    let mut command = cargo();
    command.args(["bench", "--package", "benchmarks"]);
    command.args(args);
    run(command);
}

fn check() {
    cargo_run([
        "check",
        "--package",
        "cache2",
        "--all-targets",
        "--no-default-features",
    ]);
    for feature in ["benchmarking", "io-uring"] {
        cargo_run([
            "check",
            "--package",
            "cache2",
            "--lib",
            "--no-default-features",
            "--features",
            feature,
        ]);
    }
    cargo_run(["check", "--workspace", "--all-targets", "--all-features"]);
}

fn lint() {
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
    docs.args(["doc", "--package", "cache2", "--no-deps", "--locked"]);
    run(docs);

    cargo_run([
        "package",
        "--package",
        "cache2",
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

fn test() {
    cargo_run(["test", "--workspace", "--all-features"]);
    cargo_run([
        "test",
        "--package",
        "cache2",
        "--lib",
        "--all-features",
        "--",
        "--ignored",
    ]);
}

fn cargo() -> Command {
    let executable = env::var_os("CARGO").unwrap_or_else(|| OsString::from("cargo"));
    let mut command = Command::new(executable);
    command.current_dir(Path::new(env!("CARGO_WORKSPACE_DIR")));
    command
}

fn cargo_run<const N: usize>(args: [&str; N]) {
    let mut command = cargo();
    command.args(args);
    run(command);
}

fn command_run<const N: usize>(executable: &str, args: [&str; N]) {
    let mut command = Command::new(executable);
    command
        .current_dir(Path::new(env!("CARGO_WORKSPACE_DIR")))
        .args(args);
    run(command);
}

fn run(mut command: Command) {
    println!("{command:?}");
    match command.status() {
        Ok(status) if status.success() => {}
        Ok(status) => std::process::exit(status.code().unwrap_or(1)),
        Err(error) => fail(&format!("failed to run {command:?}: {error}")),
    }
}

fn require_no_args(command: &str, args: &[OsString]) {
    if !args.is_empty() {
        fail(&format!("`cargo x {command}` does not accept arguments"));
    }
}

fn fail(message: &str) -> ! {
    eprintln!("{message}");
    std::process::exit(2)
}
