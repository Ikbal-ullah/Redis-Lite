# Audit Report — Redis-Lite (baseline `17481a7`)

Scope: every tracked file. Baseline built with
`clang++ -Wall -Wextra -Werror -fsanitize=address,undefined` (Apple clang 21,
macOS 26.6.2, Apple M2 Pro). Baseline unit suite: 27/27 passing — the bugs
below were **not** covered by any test.

## A. Confirmed bugs (reproduced against the baseline binary)

| # | Severity | Where | Bug | Reproduction |
|---|---|---|---|---|
| A1 | **Critical (data loss)** | `Wal::replay` (`src/wal.cpp`) | Replay stops at the *first* bad record and `ftruncate()`s everything after it. A single flipped byte in the middle of the log silently destroys every later acknowledged write and the server starts as if nothing happened. | Wrote 11 records (408 B), flipped byte 40, restarted: `recovered 1 log record(s)`, file truncated to **27 B**. 10 acknowledged writes gone, no error. |
| A2 | High (UB) | `Storage::expire` | `now_ms() + ttl_seconds * 1000` is signed overflow for large TTLs. UBSan: `signed integer overflow: 9223372036854775807 * 1000`. The wrapped (negative) deadline makes the key vanish. | `SET k v` / `EXPIRE k 9223372036854775807` → `:1`, then `GET k` → `$-1`. |
| A3 | High (protocol) | `parse_array` (`src/protocol.cpp`) | Bulk strings are framed by length but the trailing `\r\n` is never verified. Malformed frames are accepted and the stream desynchronises silently. | `*3\r\n$3\r\nSET\r\n$1\r\nkXX$1\r\nvYY` → `+OK`. |
| A4 | Medium | `EventLoop::handle_connection` | On EOF/`POLLHUP` the connection is closed immediately even if replies are still queued (half-close: `printf ... \| nc -N`). Replies already computed — including `+OK` for durable writes — can be dropped. | Code inspection; `peer_closed` forces `close_connection` regardless of `has_pending_output()`. |
| A5 | Medium | `Storage::purge_expired` | Active expiry is an unbounded O(N) scan of the whole map under an exclusive lock every second — a latency cliff proportional to dataset size, not to the number of expired keys. | Code inspection. |
| A6 | Medium (durability) | `Wal::replay` / `Wal::open` | Torn-tail `ftruncate()` is never `fsync`ed; the parent directory is never `fsync`ed after the log is created, so the file itself can disappear after power loss. | Code inspection. |
| A7 | Low | `Wal::sync` vs `Wal::append` | `append()` uses `F_FULLFSYNC` on macOS, `sync()` uses plain `fsync()`. Two different durability definitions in one class. | Code inspection. |
| A8 | Low (perf) | `EventLoop::run` | For each ready fd, `revents` is found by a linear rescan of `pfds` → O(ready × fds) per iteration. | Code inspection. |

## B. Contradictory / dead abstractions

1. **Threads + locks that buy nothing.** N I/O threads, a `shared_mutex`, a
   `wal_mutex_`, a pending-fd mutex, a self-pipe per loop and a maintenance
   thread. But every mutator holds `wal_mutex_` *across the fsync*, so writes
   are globally serialised anyway, and the I/O thread doing the fsync stalls
   every other connection it owns. The design pays the full complexity of
   multi-threading (lock ordering, TSan surface, cross-thread fd handoff) and
   gets single-writer throughput.
2. `Wal::mutex_` duplicates `Storage::wal_mutex_`; `Wal::bytes_written_` is
   never read outside a getter nobody calls.
3. `Buffer` keeps a two-cursor design to protect `string_view`s, but output
   uses a separate `std::string` + `output_sent_` scheme — two buffer
   abstractions for the same job.
4. `sdk/redis_lite.py` (391 lines) re-implements a RESP client that `redis-py`
   already provides; `scripts/benchmark.py` measures the GIL more than the
   server (its own docstring says so) and is superseded by `redis-benchmark`.
5. `EXPLAINER.md` + `INTERNALIZATION_GUIDE.md` (4,285 lines) and
   `scripts/drills.sh` document the multi-threaded design and the old WAL
   format. After this hardening they would be actively wrong.

## C. Missing features vs. the stated goals

- No `SET ... EX|PX`, no `TTL`, no `COMPACT`; the WAL grows forever.
- WAL frame does not carry a fixed header with explicit lengths/expiry; no way
  to distinguish torn tail from mid-file corruption.
- Only two durability modes (`--no-fsync` / always); no group commit.
- No crash (SIGKILL) test, no CI, Dockerfile builds but never runs sanitizers.

## D. Remediation plan (one commit each)

1. Replace the threaded server with a **single-threaded `poll(2)` loop**:
   lock-free `Storage`, per-connection `read_buf`/`write_buf`, `POLLOUT` only
   when `write_buf` is non-empty, flush-before-close on EOF (A4, A8, B1–B3).
2. **RESP2 parser** hardening (A3) + `SET EX|PX`, `TTL`, `COMPACT`, overflow-safe
   deadlines (A2), lazy + **bounded** active expiry via a deadline index (A5).
3. **New WAL**: `[CRC32][Op][KeyLen][ValLen][ExpireAtMs][Key][Val]`, torn-tail
   truncation + fsync, **fail hard on mid-file corruption** (A1, A6, A7),
   `--fsync no|group|always` with replies held until the covering fsync,
   atomic compaction (tmp + fsync + rename + dir fsync).
4. Tests under `-Werror` + ASan/UBSan, Python chaos test (20× SIGKILL + torn
   tail), `redis-benchmark` for all three modes, Dockerfile + CI.
5. Remove dead code/docs (B4, B5); add `OWNERSHIP_DRILL.md`, `BENCHMARKS.md`.

## E. Found during hardening (in the new code, before merge)

| # | Where | Bug | How it was found / fixed |
|---|---|---|---|
| E1 | `Server::handle` | A backpressure-paused connection was resumed only once per event. If that resume's `flush` drained the whole backlog, the connection was left with `input_paused` set and an empty `write_buf`: poll watched it for neither `POLLIN` nor `POLLOUT`, and the rest of the pipeline never ran. | Showed up as 60/64 replies in a stress run under CPU load. Fixed by `Server::pump` (loop process→flush). Made deterministic with a configurable `output_high_water`; the regression test fails 3/3 on the pre-fix server and passes on the fix. |
| E2 | `Server::flush_released` | After E1, `pump()` could start a group commit that appends to `released_` while it is being iterated. | Code review of the E1 fix; the loop now swaps into a local batch. |
| E3 | tests | GCC 12 `-Wrestrict` false positive (GCC bug 105329) on `"lit" + std::to_string(n)` at -O3 broke the `-Werror` Linux build. | Caught by the Docker build. Replaced with an append-based `testing::cat()` helper; warning not suppressed. |
