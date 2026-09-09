# Contributing to C²

## Workspace

`cache2/` contains the published library and its private implementation tests. `tests-integration/` exercises the public API; `benchmarks/` and `examples/` contain workloads and runnable integrations. `xtask/` implements the `cargo x` development commands.

## Development

Run commands from the repository root. Use the Rust version declared in [Cargo.toml](Cargo.toml). Linting also requires nightly Rust and these tools:

```sh
rustup toolchain install nightly --profile minimal --component rustfmt,clippy
cargo install --locked cargo-deny hawkeye taplo-cli typos-cli
```

Before submitting a pull request, run:

```sh
cargo x check
cargo x test
cargo x lint
```

`cargo x` is the source of truth for validation: `check` covers the workspace and optional features, `test` includes the extended library tests, and `lint` checks formatting, code, documentation, packaging, and dependencies. Use `cargo x lint --fix` to apply supported automatic fixes, then review the diff.

See [BENCHMARK.md](BENCHMARK.md) for performance workloads and qualification.

## Changes

Follow the surrounding code and the engineering constraints in [AGENTS.md](AGENTS.md). Cover behavior changes with tests, keep public documentation current, and record user-visible changes in [CHANGELOG.md](CHANGELOG.md).
