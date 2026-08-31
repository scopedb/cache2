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
library tests. `cargo x lint` checks Rust formatting, Clippy, public
documentation, the publishable package, license headers, dependency licenses,
advisories, and sources.

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

## Benchmarks and property tests

Each benchmark is an explicit target in the `benchmarks` package. Run one
target with:

```sh
cargo x bench --bench cache
```

See `BENCHMARK.md` for workload controls and qualification requirements. The
normal test workflow also runs 10,000 QuickCheck cases over persistent decoders
and bounded-index probes with inputs up to 16 KiB. Isolate that test with:

```sh
cargo test --package cache2 --lib property_tests::arbitrary_persistent_bytes_never_escape_bounds
```

## Changelog

Update `CHANGELOG.md` for user-visible API, correctness, compatibility,
performance, or operational changes. Internal refactors, tests, documentation,
CI, tooling, and dependency maintenance do not need an entry unless they alter
observable behavior.
