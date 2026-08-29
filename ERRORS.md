# Error handling

C² separates normal cache outcomes from failures. A miss, stale hit, L1
bypass, eviction, rejected recovery image, or transition to read miss-only mode
is not an error. Public operations return `cache2::Result<T>`, whose error type
is `cache2::Error`.

An error has three independent pieces of information:

- `Error::kind()` is the stable, actionable C² classification.
- `Error::operation()` identifies the public operation that failed.
- `Error::as_io_error()` retains the detailed cause, standard I/O kind, raw OS
  code, and source chain.

Display text is intended for people and logs. Do not parse it or use it as a
metric label; use `ErrorKind::as_str()` and `ErrorOperation::as_str()` instead.
Both enums are non-exhaustive, so external matches must include a wildcard.

## Handling request pressure

`Overloaded` is an expected result of bounded admission. A lookaside-cache
caller should normally continue through its authoritative data path or perform
a bounded retry rather than fail the application request.

```rust
use cache2::{Cache, ErrorKind, Result};

fn cache_value(cache: &Cache, key: &[u8], value: &[u8]) -> Result<()> {
    match cache.put(key, value) {
        Ok(_sequence) => Ok(()),
        Err(error) if error.kind() == ErrorKind::Overloaded => {
            // Cache admission is saturated. The authoritative write can still
            // proceed, so skipping this cache fill is safe.
            Ok(())
        }
        Err(error) => Err(error),
    }
}
```

Do not retry without a bound. C² deliberately exposes pressure instead of
building unbounded queues. With the default immediate-read policy, read-pool or
buffer pressure is `Ok(None)`. When read waiting is enabled, queue saturation,
buffer pressure, and deadline expiry are `ErrorKind::Overloaded`.

## Classifications

| `ErrorKind` | Meaning | Usual response |
| --- | --- | --- |
| `InvalidInput` | Static/runtime configuration or a request key/value is invalid. | Fix the input; retrying it unchanged cannot succeed. |
| `Unsupported` | The selected I/O engine, mode, or platform capability is unavailable. | Select a supported configuration or build target. |
| `Busy` | `open` could not acquire exclusive ownership of the cache files. | Coordinate ownership or retry later with a bound. |
| `Overloaded` | A bounded request-path slot, queue, buffer, or deadline is exhausted. | Fall through to the authoritative path or retry with a bound. |
| `ResourceExhausted` | Startup or lifecycle work could not satisfy a required allocation/resource plan. | Reduce the configured footprint or provide more resources. |
| `Unavailable` | The runtime, worker, or required service has stopped or was interrupted. | Stop using this cache instance and reopen or replace it. |
| `CorruptData` | Cache data or internal structure failed validation. | Treat the cache as disposable; inspect logs/device health and reopen it. |
| `Io` | A filesystem or device operation failed. | Inspect `io_kind()`, `raw_os_error()`, and the source chain. |
| `Internal` | A worker, synchronization primitive, or internal invariant failed without an OS error. | Record diagnostics and replace the cache instance. |

The classification is contextual. For example, `WouldBlock` during `open` is
`Busy`, while `WouldBlock` during `put` is `Overloaded`. A managed read-buffer
allocation failure during `get` is also `Overloaded`; the same low-level kind
during startup is `ResourceExhausted`. A `get` wait-deadline expiry is
`Overloaded`, while a device completion timeout reported by `drain` or close is
`Io`.

## Operation context

`ErrorOperation` mirrors the fallible public API: static validation and disk
estimation, `open`, each request operation, snapshots, draining, and both close
modes. It makes telemetry and policy precise without relying on the source
message.

```rust
use cache2::{Error, ErrorKind, ErrorOperation};

fn record(error: &Error) {
    let kind = error.kind();
    let operation = error.operation();
    eprintln!(
        "cache operation={} outcome={} io_kind={:?} os_error={:?}",
        operation.as_str(),
        kind.as_str(),
        error.io_kind(),
        error.raw_os_error(),
    );

    match (operation, kind) {
        (ErrorOperation::Open, ErrorKind::Busy) => {
            // Another process or cache instance owns the files.
        }
        (_, ErrorKind::Overloaded) => {
            // Apply the caller's bounded overload policy.
        }
        _ => {}
    }
}
```

Keys and values are intentionally absent from errors and their display text so
that diagnostics do not leak cache contents.

## Standard I/O interoperability

Existing functions that return `std::io::Result` can continue to use `?` on a
C² result. `From<cache2::Error> for std::io::Error` returns the original I/O
error, preserving its raw OS code and source chain. The conversion discards the
C² operation and classification:

```rust
use cache2::Cache;

async fn flush_cache(cache: Cache) -> std::io::Result<()> {
    cache.close_warm().await?;
    Ok(())
}
```

`Error::into_io_error()` performs the same lossless conversion explicitly. Use
`Error::into_io_error_with_context()` when retaining the C² operation and
classification in the source chain is more important. That contextual wrapper
keeps the original I/O kind, but its outer `raw_os_error()` is `None`; the
wrapped `cache2::Error` still exposes the original code.

### Migrating from the `io::Result` API

Before the structured error API, callers commonly wrote:

```text
error.kind() == std::io::ErrorKind::WouldBlock
```

Use `error.kind() == cache2::ErrorKind::Overloaded` for application policy.
Use `error.io_kind() == std::io::ErrorKind::WouldBlock` only when exact legacy
I/O behavior is required. Returning a C² error from a `std::io::Result`
function requires `Err(error.into())`; the `?` operator performs that
conversion automatically.
