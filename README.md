# Redis-Lite

A Redis-compatible key-value server in C++20. It runs a single-threaded
non-blocking `poll(2)` event loop, speaks RESP2 with pipelining, and persists
to a CRC-framed write-ahead log with three durability modes. Group commit,
atomic compaction, and recovery that tells a torn tail apart from mid-file
corruption.

It works unmodified with `redis-cli`, `redis-py` and `redis-benchmark`, and has
no third-party C++ dependencies.

```
clients ─TCP─► poll() loop ─► RESP2 parser ─► Database ─► WAL ─► fsync
                   ▲                              │
                   └── replies (held until the ───┘
                       covering fsync in group mode)
```

See [OWNERSHIP_DRILL.md](OWNERSHIP_DRILL.md) for the detailed architecture,
[AUDIT_REPORT.md](AUDIT_REPORT.md) for what was wrong with the previous
version, and [BENCHMARKS.md](BENCHMARKS.md) for raw measurements.

## Build, test, run

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release && cmake --build build
./build/kvstore --port 6379 --wal wal.log --fsync group
redis-cli -p 6379 SET user:1 alice EX 60
```

Tests (warnings are errors by default; sanitizers on):

```bash
cmake -S . -B build-asan -DCMAKE_BUILD_TYPE=Debug -DKVSTORE_SANITIZE=ON
cmake --build build-asan && ctest --test-dir build-asan --output-on-failure
KVSTORE_BIN=build-asan/kvstore .venv/bin/pytest -q tests/test_redis_py.py
KVSTORE_BIN=build-asan/kvstore .venv/bin/python tests/chaos_crash_test.py --iterations 20 --fsync always
scripts/bench.sh build/kvstore     # redis-benchmark, all three fsync modes
```

| Flag | Default | Meaning |
|---|---|---|
| `--bind <addr>` | `127.0.0.1` | interface |
| `--port <port>` | `6379` | `0` picks a free port |
| `--wal <path>` | `wal.log` | write-ahead log |
| `--fsync no\|group\|always` | `always` | durability policy (below) |
| `--group-window-ms <n>` | `10` | group commit: max wait before fsync |
| `--group-max-ops <n>` | `64` | group commit: max records per fsync |

Docker (the image build runs the Release test suite; ASan/UBSan runs in native Linux/macOS CI):

```bash
docker compose up -d          # 127.0.0.1:6379, WAL on a named volume, --fsync always
```

## Commands

`PING [msg]`, `ECHO msg`, `SET key value [EX seconds | PX milliseconds]`,
`GET key`, `DEL key [key ...]`, `EXPIRE key seconds`, `TTL key`, and `COMPACT`
(rewrites the WAL down to the live keyspace). Both RESP arrays and inline
commands are accepted.

## Durability

| Mode | `+OK` is sent after | Survives process crash | Survives power loss |
|---|---|---|---|
| `no` | `write(2)` | yes (page cache) | no |
| `group` | one fsync covering ≤64 records or ≤10 ms | yes | yes |
| `always` | an fsync of that record | yes | yes |

On macOS "fsync" means `fcntl(F_FULLFSYNC)`. Plain `fsync()` there does not
flush the drive's write cache.

WAL frame, little-endian: `[CRC32 4B][Op 1B][KeyLen 4B][ValLen 4B][ExpireAtMs 8B][Key][Val]`.
The CRC covers everything after itself. Expiry is an absolute Unix-ms deadline.

Recovery: a bad final frame (torn write, garbage or zeros) is `ftruncate()`d
and the truncation is fsynced. A bad frame **followed by any valid frame** is
mid-file corruption: the server exits with code 3 and leaves the file
untouched rather than silently dropping acknowledged writes.

Compaction: write `wal.tmp` → fsync → `rename` → reopen → fsync the directory.

## Verified results

All figures come from actual runs of the final code. The raw output is in
[BENCHMARKS.md](BENCHMARKS.md). Host: Apple M2 Pro, 16 GiB, macOS 26.6.2,
redis-benchmark 8.10.1 over loopback, 50 clients, 16 B values, random keys
over 100k.

| `--fsync` | SET ops/s | SET p50 / p99 | GET ops/s | GET p50 / p99 | SET ops/s `-P 16` |
|---|---:|---:|---:|---:|---:|
| `no` | 97,847.36 | 0.367 / 1.151 ms | 101,317.12 | 0.271 / 1.047 ms | 294,637.59 |
| `group` | 3,346.94 | 14.127 / 27.199 ms | 82,767.76 | 0.311 / 1.343 ms | 15,893.20 |
| `always` | 513.35 | 94.207 / 139.135 ms | 107,273.12 | 0.263 / 0.927 ms | 602.21 |

- One `F_FULLFSYNC` on this SSD has a p50 of 2000.8 µs, which caps `always` near
  500 SET/s. Group commit keeps the same ack-after-fsync guarantee at 6.5×
  that throughput, or 26.4× with `-P 16` pipelining.
- Chaos: 20 SIGKILLs under `--fsync always` with 8 concurrent writers lost
  **0 of 6,644** acknowledged writes (and 0 of 6,640 under
  `group`). Garbage tails of 1–4096 bytes were truncated exactly. A mid-file
  byte flip made the server refuse to start (exit 3), with the WAL unchanged.
- `COMPACT` shrank a 53,000,000-byte log to 5,299,470 bytes
  in 0.039 s end to end, and all 99,990 keys came back on
  restart.
- 44 C++ tests under `-Werror` + ASan/UBSan (macOS clang 21 and Linux GCC 12),
  and 24 redis-py tests across the three modes, all passing.

## Known limitations

- Under `group`, a write is visible to other clients before its fsync. The
  writer is acknowledged only after the fsync. This matches Redis
  `appendfsync everysec` semantics for readers.
- `COMPACT` runs on the event loop and blocks it for O(live keys).
- Replay reads the entire WAL into memory.
- `poll(2)` is O(connections) per wakeup; epoll/kqueue would be O(ready).
- No auth/TLS: bind to localhost or put it behind a proxy.
