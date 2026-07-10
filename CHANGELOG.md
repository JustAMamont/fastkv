# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.5.2] — 2026-07-10

### Fixed (critical: missing dispatch entries)

- **Sorted Set commands were unreachable over TCP.** The Sorted Set store
  (`src/core/sortedset.rs`) and its 15 unit tests were wired in correctly,
  but the central dispatcher in `src/core/server/tcp.rs::dispatch_command`
  had no `match` arms for `ZADD` / `ZSCORE` / `ZCARD` / `ZRANGE` /
  `ZREVRANGE` / `ZREVRANGEBYSCORE` / `ZREM` / `ZINCRBY`. Every Z* command
  returned `-ERR unknown command 'ZADD'` on the wire. The v1.5.1 release
  binary shipped with this bug — every previous release since v1.4.0 had
  it too. Fixed by adding the missing dispatch arms and eight `cmd_z*`
  handlers that translate between the RESP command layer and the
  lock-free `SortedSetStore` API.

- **Pub/Sub commands were unreachable over TCP.** Same root cause: the
  `PubSubRegistry` (`src/core/pubsub.rs`) and its 8 unit tests existed,
  but `SUBSCRIBE` / `UNSUBSCRIBE` / `PUBLISH` / `PUBSUB CHANNELS` /
  `PUBSUB NUMSUB` had no entries in `dispatch_command`. Fixed by adding
  a Pub/Sub-aware connection architecture (see below).

### Added

- **Pub/Sub connection architecture.** Both `TokioServer` and
  `IoUringServer` now use a dedicated writer task per connection:
  command responses and inbound pub/sub messages flow through a single
  `tokio::sync::mpsc` channel into the writer task, which owns the
  socket write half. Each `SUBSCRIBE` spawns a small forwarder task
  that pumps `broadcast::Receiver` events into the same channel. This
  means subscriber notifications never block publishers and never get
  blocked by heavy WAL writes on the same connection. The reader task
  splits pub/sub commands out of the normal batch and handles them in
  the async context (they need async `broadcast::send`); non-pubsub
  commands still go through `spawn_blocking` as before so synchronous
  WAL writes don't block the async worker thread.

  On the io_uring backend the socket is wrapped in `Arc<TcpStream>`
  (tokio-uring's `read` and `write_all` take `&self`, so both tasks can
  share it). On the Tokio backend the socket is split into
  `OwnedReadHalf` / `OwnedWriteHalf` via `into_split()`.

- **`SortedSetStore` is now sync.** The previous `async fn` signatures
  on `zadd`/`zscore`/`zcard`/`zrange`/`zrevrange`/`zrevrangebyscore`/
  `zrem`/`zincrby`/`del`/`exists` were misleading — none of them
  contained a single `.await`. Internally they all operate on
  lock-free `crossbeam_skiplist::SkipMap` and `dashmap::DashMap`, so
  they are genuinely synchronous. Removed the `async` qualifier so
  callers don't pay a no-op `Future` allocation per call.

- **`RespEncoder::encode_error` and `write_array_of_bulks` helpers.**
  Convenience wrappers used by the pub/sub handlers.

- **`tests/test_tcp_integration.rs` — 9 end-to-end tests** that spawn a
  real FastKV server in a background thread and drive commands over a
  TCP socket. Catches the entire class of "command implemented but not
  dispatched" regressions that v1.5.1 shipped with. Covers String, TTL,
  Hash, List, Sorted Set, Blob Arena, Similarity, Scan, and Pub/Sub
  command groups.

### Verification

| Profile | Result |
|---------|--------|
| `cargo test` (dev, no features) | 215 tests passed, 0 warnings |
| `cargo test --features io-uring` (dev, Linux) | 215 tests passed, 0 warnings |
| `cargo test --release` (no features) | 215 tests passed |
| `cargo test --release --features io-uring` (Linux) | 215 tests passed |
| Release binary smoke test (18 commands over TCP) | 18 passed, 0 failed |

Test breakdown: 182 lib + 8 pubsub + 15 sortedset + 9 tcp_integration +
1 doctest = 215.

## [1.5.1] — 2026-07-10

### Fixed (CI)

- **CI build broken on macOS and Windows.** The previous workflow ran
  `cargo test --release --all-features` on every platform. The only
  remaining cargo feature, `io-uring`, transitively pulls in the
  `io-uring` crate (version 0.6.4), which uses Linux-specific libc
  symbols (`MAP_POPULATE`, `SYS_io_uring_setup`, `cpu_set_t`,
  `sigset_t`, `iovec`, `msghdr`, …) and `std::os::unix::io::*`. On
  macOS the build failed with `cannot find value MAP_POPULATE in
  crate libc` (BSD libc does not export those constants); on Windows
  it failed with `cannot find unix in os` because `std::os::unix` is
  gated behind `target_os = "linux"`.
  The fix splits the `Rust unit tests` step into two:
    1. Linux (`matrix.label == 'linux-amd64'`):
       `cargo test --release --features io-uring`
    2. macOS / Windows (`matrix.label != 'linux-amd64'`):
       `cargo test --release`
  This matches the existing `Build server` step, which already gated
  `--features io-uring` on Linux only.

### Verification

Both profiles were verified locally on Linux x86_64 (kernel 5.10,
rustc 1.97.0):

| Profile | Result |
|---------|--------|
| `cargo test` (dev, no features) | 206 tests passed, 0 warnings |
| `cargo test --features io-uring` (dev, Linux) | 206 tests passed, 0 warnings |
| `cargo test --release` (no features) | 206 tests passed, 0 warnings |
| `cargo test --release --features io-uring` (Linux) | 206 tests passed, 0 warnings |

## [1.5.0] — 2026-07-10

### Changed — Distribution refactor

- **`blob-store` and `similarity` cargo features removed.** All subsystems
  (Blob Arena, SimHash/MinHash/LSH, Compressed WAL Segment) are now always
  compiled into a single binary per platform. This eliminates the combinatorial
  explosion of feature-flag build matrices and makes every binary feature-complete
  by default.
  - `Cargo.toml`: removed `optional = true` from `zstd` and `zerocopy`; removed
    `blob-store` and `similarity` from `[features]`. Only `io-uring` remains
    as a compile-time feature (it requires the Linux-only `tokio-uring` crate).
  - `src/core/mod.rs`: all `pub mod` declarations are now unconditional
    (was: gated by `#[cfg(feature = ...)]`).
  - `src/core/server/tcp.rs`: already used `Option<Arc<BlobArena>>` everywhere
    — no changes needed.
  - `src/core/server/io_uring.rs`: removed `#[cfg(not(feature = "blob-store"))]`
    duplicate definitions of `handle_client` and `with_components_no_blob`
    (these were the root cause of the E0428/E0061/E0063 compile errors when
    `cargo test --all-features` was run).
  - `src/core/checkpoint.rs`: removed 3 duplicate `#[cfg(not(feature = "blob-store"))]`
    `let count = ...` lines in tests.
  - `benches/kv_benchmark.rs`: removed all `#[cfg(feature = "blob-store")]` /
    `#[cfg(feature = "similarity")]` guards; replaced the 4-way `criterion_main!`
    cfg dispatch with a single unconditional `criterion_main!(core_benches, blob_benches, similarity_benches)`.

### Added

- **`--no-blob-store` runtime flag.** Blob Arena is enabled by default; pass
  `--no-blob-store` to disable it at server start (BSET/BGET return errors,
  saves a small amount of arena bookkeeping memory). Documented in `--help`
  and `README.md`.

### Fixed

- **`--wal-compress` was silently broken in v1.4.0.** `run_server_with_n` in
  `src/main.rs` had duplicate `let wal`, `let entries`, and `let wal_writer`
  bindings left over from the in-progress distribution refactor. The second
  binding in each pair unconditionally overwrote the first, so the
  `WalSegment`-based compressed WAL path was dead code — `--wal-compress`
  silently fell back to the raw `Wal`. Removed the duplicate bindings; the
  segment WAL path is now reachable and verified to start correctly.
- **Stale `#[cfg(not(feature = "blob-store"))]` guards in `src/main.rs`
  `run_server_with_n`** that referenced the removed `blob-store` feature
  (would have caused `unexpected_cfgs` warnings and dead branches).
- **Unreachable `_` arm in WAL recovery match** (all 6 `WalOp` variants
  now explicitly covered; `_` was dead code emitting `unreachable_patterns`
  warning).

### Tests

- Test count grew from 149 → 206 (182 lib + 8 pubsub + 15 sortedset + 1 doctest)
  because blob, simhash, minhash, lsh, and wal_segment tests are now always
  compiled and run.

### Documentation

- `README.md`: removed all `--features blob-store` / `--features similarity`
  build commands; added `--no-blob-store` to server flags table; updated
  benchmark group table (removed "Feature Flag" column); updated test counts;
  added v1.5.0 entry to Roadmap (Phase 15d).
- `src/main.rs` `print_usage`: fixed orphaned `io_uring` println (was between
  `--wal-segment-size` and env vars); documented `--no-blob-store`;
  added Pub/Sub + Sorted Sets + Blob Arena + Similarity to Features list.

## [1.4.0] — 2026-07-09

### Changed

- **Sorted Sets: rewritten to lock-free.** Replaced `RwLock<BTreeMap>` +
  `RwLock<HashMap>` with `crossbeam_skiplist::SkipMap` +
  `dashmap::DashMap`. No locks, no blocking — reads and writes are
  fully concurrent. This eliminates the main bottleneck identified
  in v1.3.0.

### Added

- Dependencies: `crossbeam-skiplist = "0.1"`, `dashmap = "6"`.

### Fixed (P0 from todo.md)

- **C1: `RespParser::parse_inline` — inline format detection.** Was
  branching by first letter (`G|S|D|P|I|H|L`); now: `*` → array,
  any ASCII → inline. Commands like `CLIENT`, `CONFIG`, `EXPIRE` now
  work in inline mode (telnet, redis-cli --pipe).

- **C8: `glob_match` — exponential backtracking DoS.** Replaced
  recursive `glob_match_impl` (O(2^n) on `***a***a***a` patterns)
  with iterative backtracking via `star_pi`/`match_ti` — O(n*m) worst
  case, no recursion.

- **C9: `KvStoreLockFree::del` — silent failure under contention.**
  Added 3 retry attempts before returning `false`. Previously, a
  concurrent writer changing the version between hash check and
  tombstone store would cause `del` to silently fail.

- **C12: `cmd_auth` — timing attack via plain `==` comparison.**
  Replaced `provided == expected.as_bytes()` with constant-time XOR
  comparison with `0xFF` padding for length mismatch.

- **io-uring: `tokio_uring::io::write_all` removed API.** Replaced
  with `stream.write_all(&b"..."[..])` — `tokio_uring::io` module is
  private in tokio-uring 0.5.

- **Warning: unreachable_code in io_uring.rs.** Added
  `#[allow(unreachable_code)]` on `Ok(())` after infinite accept loop.

## [1.3.0] — 2026-07-09

### Added

- **Pub/Sub**: `SUBSCRIBE`, `UNSUBSCRIBE`, `PUBLISH`, `PUBSUB CHANNELS`,
  `PUBSUB NUMSUB` commands via `tokio::sync::broadcast` for fan-out.
  Supports glob pattern matching for `CHANNELS`. Auto-cleanup of empty
  channels.

- **Sorted Sets** (v1.3.0, rewritten in v1.4.0): `ZADD`, `ZSCORE`,
  `ZCARD`, `ZRANGE`, `ZREVRANGE`, `ZREVRANGEBYSCORE`, `ZREM`, `ZINCRBY`.
  Supports negative indices, negative scores, and NaN.

- **23 integration tests**: 8 for Pub/Sub, 15 for Sorted Sets. All
  passing alongside 126 existing unit tests (149 total).

## [1.2.3] — 2026-06-21

### Fixed

- **Critical: LIST operations (LPUSH/RPUSH/LPOP/RPOP/LREM/LTRIM/LSET) were
  not persisted to the WAL.** All list mutations were applied in-memory but
  never written to the WAL, so list state was completely lost on every
  server restart. After restart, list keys (`LLEN`, `LRANGE`, etc.) returned
  empty results even though the keys still existed in the hash table
  (because the `0xFE` magic marker value was restored via the regular SET
  path, but the actual list contents were gone). This broke any
  application that used lists for indexes, queues, or any persistent list
  data structure.

  The fix adds `wal_list_op()` calls to all six LIST mutation commands in
  `core::server/tcp.rs`. Each command encodes its sub-operation using the
  existing `encode_list_push/pop/trim/set/rem` helpers from
  `core::list.rs`, and the existing `WalOp::ListOp` WAL entry type plus
  `replay_list_op()` recovery path (already present in `main.rs`) rebuild
  the list state correctly on restart.

  LPOP/RPOP only write to the WAL if elements were actually popped
  (no-op pops are not persisted). LREM only writes if `removed > 0`.
  LPUSH/RPUSH/LTRIM/LSET always write (they are idempotent on replay).

- **Checkpoint (BGSAVE) now persists list keys correctly.** Previously,
  when the checkpoint thread compacted the WAL, list keys were written
  as plain `SET(key, [0xFE])` entries — the magic byte survived restart
  but the list contents were lost. The fix threads `Option<&ListManager>`
  through `checkpoint()` and `spawn_checkpoint_thread()`. When the
  checkpoint encounters a list key, it now writes:
  1. `SET(key, [0xFE])` — to mark the key as a list in the hash table
  2. `LIST_OP(key, RPUSH(all_elements))` — to rebuild the contents on
     recovery via the existing `replay_list_op()` path.

  The three checkpoint call sites (`main.rs` periodic thread,
  `cmd_bgsave`, `cmd_flushall`) were updated to pass the list manager
  through.

### Added

- Python client: `get_str(key)` and `set_str(key, value)` helper methods
  on both `FastKVClient` (sync) and `FastKVAsyncClient` (async). These
  are thin wrappers around `get()` + UTF-8 decode and `set()` that
  return/accept `str` instead of `bytes`. Convenient for storing
  counters, flags, and other small string values without manual
  `.decode()` / `.encode()` boilerplate at every call site.

### Compatibility

- **Wire protocol unchanged** — no client changes required for the
  LIST WAL fix.
- **WAL format unchanged** — `WalOp::ListOp` (0x06) was already defined
  and handled by the recovery code; it just wasn't being written by
  the command handlers. Existing WAL files replay correctly.
- Old WAL files written by v1.2.2 or earlier that contain list keys
  will still have **empty lists after recovery** because the LIST_OP
  entries were never written. **Action required**: after upgrading,
  rebuild any list indexes from their source-of-truth.
- Client library versions bumped to `1.2.3` to stay in sync with the
  server; no behavioural changes in any client except the new
  `get_str`/`set_str` helpers.

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
