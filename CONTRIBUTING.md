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

- **Modules and imports**: Order module headers as `//!` docs, `use` imports (`group_imports = "StdExternalCrate"` and `imports_granularity = "Item"` per [rustfmt.toml](rustfmt.toml)), out-of-line `mod` declarations, and items. Use `<module>/mod.rs` for multi-file modules, and do not split cohesive files prematurely. Group `mod` declarations with blank lines between `pub mod`, private `mod` with immediate `pub use self::...;` re-exports, and implementation-only private `mod` (`#[cfg(test)]` last), sorted alphabetically within each group; re-export subsystem interfaces from `mod.rs` and attach `#[cfg]` gates at module boundaries when possible. Import symbols at the top of the file from `crate::` (avoiding `super::...` and private `use self::...`, reserving `use super::*;` for `#[cfg(test)]` modules), preserve original names with meaningful module or crate qualification (such as `io::Error`, `tokio::time::sleep`, or `libc::flock`), and avoid `use ... as ...` aliases except `use ...::Trait as _;` (inline paths are fine for collisions such as `core::error::Error` or rare hooks such as `std::panic::catch_unwind`).
- **Visibility**: Use three visibility tiers: private by default, plain `pub` inside private ancestor module trees, and `pub(crate)` explicitly reserved for internal fields, constructors, or helpers reachable through `pub mod` or `pub use` re-exported public boundaries. Avoid `pub(super)` and `pub(in ...)`, and never widen visibility solely for cross-module tests.
- **Types, traits, and attributes**: Keep each type and its primary `impl` block together (public API methods before private helpers), and reserve `len` and `is_empty` for collection or slice-like views. Use `new` for primary constructors (implementing `Default` when parameterless and `const fn` when trivial), `with_*` for alternative required inputs, and `*Builder` for multi-option construction. Do not prefix struct fields with `_` to silence unused-field warnings; name retained fields after their role and annotate unread ownership or guard fields with `#[expect(dead_code, reason = "...")]` (`PhantomData` needs neither). Derive applicable value traits (`Debug`, `Clone`, `Copy`, `PartialEq`, `Eq`, `Hash`) on plain data types and configuration enums, omitting `PartialEq` and `Eq` when field equality would expose internal layout or history, and avoid unnecessary generic bounds on custom `Debug` implementations. Omit routine `#[must_use]` on getters, queries, and consuming builders, reserving `#[must_use = "..."]` for lazy values or `&self` methods easily mistaken for mutators.
- **Suppressions and comments**: Use `#[expect(..., reason = "...")]` instead of `#[allow(...)]` for intentional lint suppressions so stale exceptions fail lints. Reserve `//` comments for non-obvious invariants, `// SAFETY:` preconditions, external quirks, or performance rationale rather than restating the code.
- **Control flow and errors**: Keep abstractions at signatures, cohesive types, and module boundaries; inside functions, prefer linear top-to-bottom control flow with guard returns, explicit `match` or `if let`, and explicit `for` loops for multi-step or fallible logic over premature helpers or uncontracted defensive glue. Use `Result` (keeping failing mutations atomic when practical) for recoverable failures, natural empty values or `Option` for absent queries, fail-fast panics for violated internal invariants, and structured error context rather than string inspection.

## Tests

- Cover stable behavioral contracts in `tests-integration/` and non-trivial private invariants in `cache2/`.
- Every test must guard a realistic regression or boundary without mirroring production literals, trivial control flow, static type guarantees, or non-contract `Debug` or `Display` wording.
- Avoid test-only production hooks, and prefer deterministic coordination (barriers, notifications, or controlled scheduling) over sleeps.

## Documentation

- Document caller-observable contracts in `///` comments without leaking incidental implementation details. Use noun phrases for types, third-person present-tense verbs for operations, periods at the end of summary sentences, inline code spans for identifiers, literals, and ranges, and public API names as intra-doc link labels that hide private module paths (such as `` [`CacheConfig`](crate::config::CacheConfig) ``).
- Include `# Errors`, `# Panics`, `# Safety`, and trailing `# Examples` (with visible imports) only when applicable, explaining straightforward return values in the summary or body (adding `# Returns` only for non-trivial outcomes).
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
