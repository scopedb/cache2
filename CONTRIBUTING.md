# Contributing to C²

## Workspace

`cache2/` contains the published library and its private implementation tests. `tests-integration/` exercises the public API; `benchmarks/` and `examples/` contain workloads and runnable integrations. `xtask/` implements the `cargo x` development commands.

## Development

Run commands from the repository root. Use `cargo x` as the source of truth for repository workflows. Read `cargo x --help` and the relevant subcommand's `--help` before running build, test, lint, or formatting commands.

Use a Rust toolchain at or above the `rust-version` declared in [Cargo.toml](Cargo.toml). Linting also requires nightly Rust and these tools:

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

`check` covers the workspace and optional features, `test` includes the extended library tests, and `lint` checks formatting, code, documentation, packaging, and dependencies. Use `cargo x lint --fix` to apply supported automatic fixes, then review the diff.

Cover observable behavior changes with tests. See [BENCHMARK.md](BENCHMARK.md) for performance workloads and qualification.

## Design and Rust Style

Follow the surrounding code and the design constraints in [ARCHITECTURE.md](ARCHITECTURE.md), including bounded resource use, best-effort consistency, request-path priorities, and recovery guarantees.

Declare restricted visibility at module boundaries and use `pub` for items in those modules' APIs. Keep items private when only their defining module and its descendants need them. For items reachable through public modules or re-exported public types, reserve `pub` for intentional public API and use narrower visibility for internal callers.

## Documentation

Keep public documentation current and describe observable contracts. Keep each Markdown prose paragraph and list item on one source line.

## Changelog

- Update [CHANGELOG.md](CHANGELOG.md) for significant user-visible changes by comparing the final behavior with the latest release tag, not the sequence of commits in the current development cycle. Add entries under `Unreleased`, using only categories that contain entries.
- Before adding a bug-fix entry, verify from the latest release tag that the faulty behavior was shipped. If the affected API or behavior is unreleased, describe only its final contract in the relevant feature entry and omit the development-only correction.
- Include public API migrations, new capabilities, correctness or compatibility changes, and meaningful performance improvements. Exclude tests, internal refactors, documentation, CI, tooling, dependency maintenance, discarded intermediate APIs, and implementation history unless they change supported or observable behavior relative to the latest release.
- Write each entry from the user's perspective as one coherent observable change, including required migration guidance for breaking changes. Scope performance claims to the workloads supported by evidence.

## Pull Requests

Format pull request titles according to [.github/semantic.yml](.github/semantic.yml) and keep the description concise. Use a `Summary` section for routine changes and add `Design Notes` only when the design needs explanation.
