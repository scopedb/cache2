# Contributing to C²

C² requires Rust 1.98.0. Run development commands from the repository root so
Cargo uses the complete workspace and the shared dependency and lint policy.

## Workspace layout

The repository separates published code from development-only consumers:

| Path                 | Purpose                                                                                       |
|----------------------|-----------------------------------------------------------------------------------------------|
| `cache2/`            | The publishable `cache2` crate, private implementation tests, and persistent-format fixtures. |
| `tests-integration/` | End-to-end tests that exercise only the public `cache2` API.                                  |
| `benchmarks/`        | Standalone benchmark targets and workload-specific harnesses.                                 |
| `examples/`          | Runnable programs that demonstrate complete integrations.                                     |
| `xtask/`             | The `cargo x` repository workflow entrypoint.                                                 |
| `fuzz/`              | The isolated `cargo-fuzz` workspace and persistent-decoder target.                            |

Keep unit tests beside the implementation when they need private access.
Behavior visible to callers belongs in `tests-integration/tests`. Format
fixtures remain under `cache2/tests/fixtures` because private decoder tests are
their primary consumers.

## Repository workflows

The `.cargo/config.toml` alias maps `cargo x` to the `xtask` package. Use these
commands before opening a pull request:

```sh
cargo x check
cargo x test
cargo x lint
```

`cargo x check` verifies the workspace and each optional `cache2` feature.
`cargo x test` runs workspace tests with all features and the ignored extended
library tests. `cargo x lint` checks Rust and fuzz-target formatting, Clippy,
public documentation, the publishable package, license headers, dependency
licenses, advisories, and sources.

The lint workflow expects the pinned CI tools to be available:

```sh
cargo install cargo-deny --version 0.20.2 --locked
cargo install hawkeye --version 7.0.0 --locked
```

Use the underlying Cargo commands directly when isolating a failure. The
release-mode test pass used by CI is:

```sh
cargo test --workspace --release --all-features
```

## Benchmarks and fuzzing

Each benchmark is an explicit target in the `benchmarks` package. Run one
target with:

```sh
cargo x bench --bench cache
```

See `BENCHMARK.md` for workload controls and qualification requirements. Run
the persistent decoder and bounded-index fuzz target with:

```sh
cargo fuzz run persistent_decoders -- -runs=10000 -max_len=16384
```

## Changelog

Update `CHANGELOG.md` for user-visible API, correctness, compatibility,
performance, or operational changes. Internal refactors, tests, documentation,
CI, tooling, and dependency maintenance do not need an entry unless they alter
observable behavior.
