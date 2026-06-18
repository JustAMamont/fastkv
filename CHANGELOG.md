# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.2.1] — 2026-06-18

### Fixed

- **Concurrent read/write starvation**: SCAN, GET, and other read commands
  no longer hang when a parallel client is performing heavy writes (BSET,
  SET with `fsync=always`). Command processing was previously inline on
  tokio worker threads, which meant `std::sync::Mutex<File>` contention
  inside the WAL writer could block every worker thread. Now each command
  batch is dispatched to `tokio::task::spawn_blocking`, so synchronous WAL
  I/O runs on the blocking thread pool while async worker threads stay
  free for socket I/O and accepting new connections.

### Changed

- `ServerContext.authenticated` changed from `Cell<bool>` to `&AtomicBool`
  so the auth flag can be safely shared across the `spawn_blocking` thread
  boundary (each command batch may execute on a different blocking
  thread). The same change applied to the io_uring server.
- New internal `OwnedCtx<N>` struct that bundles `Arc` handles to all
  shared state (store, WAL, expiry, lists, blob arena, password, auth
  flag) so it can be moved cleanly into the blocking closure.
- New `process_buffer<N>()` helper that owns the command-processing loop
  and returns `(response_bytes, should_close, remaining_bytes)`. The
  `remaining_bytes` slice (partial command) is carried over to the next
  `socket.read()` so pipelined / fragmented commands work correctly.

### Compatibility

- Wire protocol unchanged — no client changes required for the
  concurrency fix.
- Existing clients (Python sync + async, Go, Java, Node.js, Rust) work
  unchanged against a v1.2.1 server.
- Client library versions bumped to `1.2.1` only to stay in sync with
  the server; no behavioural changes in any client.

## [1.2.0] — earlier

See git history for changes prior to v1.2.1.
