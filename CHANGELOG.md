# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.2.2] — 2026-06-21

### Fixed

- **Critical: blob keys (BSET) lost their data after a WAL checkpoint
  followed by a server restart.** Symptom: `SCAN` reported the keys as
  present and `DBSIZE` was correct, but `BGET` returned `nil` for every
  blob key written before the most recent checkpoint. The underlying
  cause was in `core::checkpoint::checkpoint()`: when compacting the WAL,
  blob keys were written to the new compact WAL as **plain `SET` entries
  carrying the 33-byte `BlobRef` as their value**, instead of as `BSET`
  entries carrying the **original uncompressed** payload. After recovery
  the hash table held a `BlobRef` pointing at an offset in a freshly
  initialized (empty) blob arena, so `BlobArena::retrieve()` either
  decompressed zeroed memory or hit a hash-mismatch, both of which
  surface as `nil` from `BGET` and `blob decompression failed` from a
  raw `redis-cli GET`.

  The fix threads `Option<&BlobArena>` through `checkpoint()` and
  `spawn_checkpoint_thread()`. When the arena is available, each blob
  key is now decoded, the original payload is retrieved from the arena,
  and a proper `BSET` entry is written to the compact WAL. On recovery
  the existing BSET replay path (`WalOp::BSet` in `main.rs`) rebuilds
  the arena from the original payloads, after which `BGET` works
  exactly as before the restart.

  Three call sites were updated to pass the arena through:
  `main.rs` (periodic checkpoint thread), `cmd_bgsave` (BGSAVE/SAVE),
  and `cmd_flushall`. The `spawn_checkpoint_thread` signature gained
  an `Option<Arc<BlobArena>>` parameter (feature-gated on
  `blob-store`).

  If the arena is `None` while blob keys are present (only possible if
  a caller forgets to pass it), the previous degraded behaviour is
  preserved — the bare `BlobRef` is written as a `SET` — but a
  warning is now logged for every such key, and a `blob_keys_degraded`
  counter is reported in the checkpoint summary line.

### Added

- New regression tests in `core::checkpoint::tests`:
  - `test_checkpoint_preserves_blob_keys` — verifies that the compact
    WAL contains `BSET` entries (not `SET`) for blob keys, and that the
    original uncompressed payload is recoverable from the WAL alone.
  - `test_checkpoint_then_recover_rebuilds_arena` — end-to-end
    round-trip: write BSET entries → replay into store+arena → run
    checkpoint → drop and recreate store+arena from the compact WAL →
    assert `BGET` returns the original bytes for every key.

### Compatibility

- **Wire protocol unchanged** — no client changes required.
- The on-disk WAL format is unchanged. Old WAL files (including those
  written by v1.2.1 or earlier that contain degraded `SET` entries for
  blob keys) are still readable, but the affected keys will continue
  to return `nil` from `BGET` because the original payload was never
  persisted. **Action required**: after upgrading, re-write any
  blob keys whose data must survive future restarts (issue `BSET` for
  each key, or `BGSAVE` after the upgrade to capture them correctly).
- Client library versions bumped to `1.2.2` to stay in sync with the
  server; no behavioural changes in any client.

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
