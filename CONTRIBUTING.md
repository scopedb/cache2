# Contributing to C²

C² requires Rust 1.98.0. Run development commands from the repository root so Cargo uses the complete workspace and the shared dependency and lint policy.

## Workspace layout

The repository separates published code from development-only consumers:

| Path                 | Purpose                                                                                       |
|----------------------|-----------------------------------------------------------------------------------------------|
| `cache2/`            | The publishable `cache2` crate and private implementation tests.                              |
| `tests-integration/` | End-to-end tests that exercise only the public `cache2` API.                                  |
| `benchmarks/`        | Standalone benchmark targets and workload-specific harnesses.                                 |
| `examples/`          | Runnable programs that demonstrate complete integrations.                                     |
| `xtask/`             | The `cargo x` repository workflow entrypoint.                                                 |

Keep unit tests beside the implementation when they need private access. Behavior visible to callers belongs in `tests-integration/tests`.

Keep versioned format fixtures beside the module that owns their encoding and decoding. Share only the fixture parsing and assertion helpers.

## Repository workflows

The `.cargo/config.toml` alias maps `cargo x` to the `x` package in `xtask/`. Use these commands before opening a pull request:

```sh
cargo x check
cargo x test
cargo x lint
```

`cargo x check` verifies the workspace and each optional `cache2` feature. `cargo x test` runs workspace tests with all features and the ignored extended library tests. These commands use the selected toolchain. `cargo x lint` explicitly uses nightly for Rust formatting, Clippy, and public documentation, and also checks TOML formatting, spelling, the publishable package, license headers, dependency licenses, advisories, and sources.

The lint workflow uses nightly Rust and the latest releases of the lint tools so new diagnostics are caught early:

```sh
rustup toolchain install nightly --profile minimal --component rustfmt,clippy
cargo install cargo-deny --locked
cargo install hawkeye --locked
cargo install taplo-cli --locked
cargo install typos-cli --locked
```

Run `cargo x lint --fix` to apply Clippy fixes, Rust and TOML formatting, and license headers. Review the changes, then run `cargo x lint` to verify the result. The Rust import and comment style follows `rustfmt.toml`; TOML formatting follows `taplo.toml`.

CI runs nightly lint and feature checks in `check`, tests on Linux and macOS with Rust 1.98.0 and stable in `test`, and the pinned ASan/TSan suite in `safety`. The final `Required` job succeeds only when all three jobs succeed; failures, cancellations, and skipped dependencies fail the gate. Pull requests and pushes to `main`, including documentation changes, run the workflow.

Use the underlying Cargo commands directly when isolating a failure. The release-mode test pass used by CI is:

```sh
cargo test --workspace --release --all-features
```

## Rust Style

Use `module/mod.rs` for modules with child files; keep leaf modules in a single `.rs` file.

Declare restricted visibility at the module boundary and use `pub` for items in that module's API.

## Documentation

Keep each Markdown prose paragraph and list item on one source line.

## Benchmarks and property tests

Each benchmark is an explicit target in the `benchmarks` package. Run one target with:

```sh
cargo x bench --bench cache
```

See `BENCHMARK.md` for workload controls and qualification requirements. The normal test workflow also runs 10,000 QuickCheck cases for each of four properties: persistent decoders, record round trips, the fixed-map state machine, and the Region-index state machine. Inputs are capped at 16 KiB. Run that group directly with:

```sh
cargo test --package cache2 --lib property_tests::
```

## Changelog

Update `CHANGELOG.md` for user-visible API, correctness, compatibility, performance, or operational changes. Internal refactors, tests, documentation, CI, tooling, and dependency maintenance do not need an entry unless they alter observable behavior.
