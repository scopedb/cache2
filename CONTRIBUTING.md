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

Python 3 is required for the benchmark comparison tools and their tests, which are included in `cargo x test`.

Before submitting a pull request, run:

```sh
cargo x check
cargo x test
cargo x lint
```

`check` covers the workspace and optional features, `test` includes the extended library and benchmark-tool tests, and `lint` checks formatting, code, documentation, packaging, and dependencies. Use `cargo x lint --fix` to apply supported automatic fixes, then review the diff.

See [Validation](cache2/BENCHMARK.md) for performance workloads and qualification.

## Design and Rust Style

Follow the surrounding code and the design constraints in [Architecture](cache2/ARCHITECTURE.md), including bounded resource use, best-effort consistency, request-path priorities, and recovery guarantees.

Name types by their concrete domain role. Use `Options` for editable inputs awaiting validation and `Config` for validated or resolved configuration. Keep names such as `Layout` for geometry and use specific domain nouns such as `Desc`, `Candidate`, `Victims`, or `Inspection` for runtime data. Avoid `Plan` as a type-name suffix; computing data before using it does not by itself make that data a plan. Keep related fields, local variables, and functions consistent with the type's role.

- **Imports and module layout**: Place ordinary `use` imports before `mod` declarations (`group_imports = "StdExternalCrate"` and `imports_granularity = "Item"` per [rustfmt.toml](rustfmt.toml)). Group `mod` declarations by role: public modules, private modules with immediate `pub use self::...` re-exports, private implementation modules, and `#[cfg(test)]` modules last. Import referenced symbols at the top of the module while preserving their original names without `use ... as ...` aliases, retaining module qualification only when clarifying operation context or distinguishing same-named symbols (such as `io::Error`, `tokio::time::sleep`, or `libc::flock`). Start intra-crate imports at `crate::` from defining modules; avoid inline `crate::` or `std::` fully qualified paths, explicit `super::...` imports, and private `use self::...` imports, reserving `use super::*` for test modules.
- **Visibility**: Declare restricted visibility at module boundaries (`pub` inside private parent trees; avoid `pub(super)`). Keep items private when only their defining module and its descendants need them, and reserve `pub` at re-exported public boundaries for intentional public API while using narrower visibility for internal callers.
- **Suppressions and comments**: Use `#[expect(...)]` instead of `#[allow(...)]` for intentional lint suppressions so stale exceptions fail lints. Write comments to explain non-obvious invariants, safety constraints, or external quirks rather than restating the code.

## Tests

- Cover stable behavioral contracts in `tests-integration/` and non-trivial private invariants in `cache2/`.
- Avoid asserting non-contract `Debug` or `Display` wording, mirroring production literals or obvious control flow, or adding test-only production hooks.
- Prefer deterministic coordination (barriers, notifications, or controlled scheduling) over sleeps.

## Documentation

- Keep public documentation current and describe observable contracts rather than incidental implementation details.
- Keep each Markdown prose paragraph and each list item's prose on one source line; do not wrap lines to fit a column limit.
- Format every Markdown table with aligned columns and consistent `| --- |` or `| ---: |` delimiter rows (a single space inside each pipe across header, delimiter, and data rows).
- Use repository-relative links for files within this repository rather than GitHub blob or raw URLs.
- Keep the repository [README](README.md) focused on project discovery and guide navigation. The crate-level documentation in `cache2/src/lib.rs` is the self-contained API entry point, with a compiled integration example and links to public contracts. Keep guides under `cache2/` so that version-relative links also work in the published source package.
- Keep evergreen guides ([Getting started](cache2/README.md), [Configuration](cache2/CONFIGURATION.md), [Operating a cache](cache2/OPERATIONS.md), [Architecture](cache2/ARCHITECTURE.md), [Validation](cache2/BENCHMARK.md), and [Error handling](cache2/ERRORS.md)) free of pre-1.0 migration history; record migrations in [CHANGELOG.md](CHANGELOG.md).

## Changelog

- Keep a permanent `## Unreleased` section at the top of [CHANGELOG.md](CHANGELOG.md), comparing final behavior against the latest release tag rather than listing individual commits in the development cycle.
- Group entries under `### Breaking changes`, `### Notable changes`, `### New features`, `### Improvements`, `### Performance improvements`, and `### Bug fixes` in that exact order, including only categories that contain entries.
- Write each entry from the caller's perspective as one coherent observable outcome, including required migration steps for breaking changes and workload-scoped performance notes supported by evidence.
- Verify from the latest release tag that a defect existed in shipped code before adding a `### Bug fixes` entry; describe fixes to unreleased features only in the final feature contract.
- Omit PR or issue numbers, internal refactors, tests, documentation, CI, tooling, dependency maintenance (unless changing observable behavior), discarded intermediate APIs, and unchanged-behavior disclaimers.
- For releases, freeze entries under an undated `## vX.Y.Z` heading immediately below `## Unreleased`, and add the UTC `YYYY-MM-DD` release date after publication.

## Pull Requests

Format pull request titles according to [.github/semantic.yml](.github/semantic.yml) and keep the description concise. Use a `Summary` section for routine changes and add `Design Notes` only when the design needs explanation.
