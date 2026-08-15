# kvstore

A durable key-value store in C++20: a non-blocking TCP server speaking a subset
of RESP, backed by a thread-safe hash map and an append-only write-ahead log.

No third-party dependencies. Builds with CMake and a C++20 compiler on Linux or
macOS.

```
  clients ──TCP──> event loops (poll) ──> hash map + shared_mutex
                        │                        │
                        └── N threads            └── write-ahead log ──> disk
```

## Build

```bash
cmake -S . -B build && cmake --build build
```

## Test

```bash
ctest --test-dir build --output-on-failure
```

27 cases covering protocol framing, storage semantics, log durability across
simulated crashes, concurrent access, and end-to-end behaviour over real
sockets.

## Run

```bash
./build/kvstore --port 6379 --wal wal.log
```

| Flag | Default | Meaning |
|---|---|---|
| `--bind <address>` | `127.0.0.1` | Interface to listen on |
| `--port <port>` | `6379` | TCP port; `0` picks a free one |
| `--wal <path>` | `wal.log` | Write-ahead log file |
| `--threads <n>` | `4` | I/O threads |
| `--no-fsync` | off | Skip the per-write flush — faster, not crash-safe |

Talk to it with `redis-cli -p 6379`, or with anything at hand:

```bash
printf 'SET user:1 ikbal\r\nGET user:1\r\nPING\r\n' | nc -w 2 127.0.0.1 6379
```

## Commands

| Command | Reply |
|---|---|
| `SET key value` | `+OK` |
| `GET key` | bulk string, or `$-1` if absent or expired |
| `DEL key [key ...]` | number removed |
| `EXPIRE key seconds` | `:1` if the key existed, `:0` otherwise |
| `PING [message]` | `+PONG`, or the message echoed |

Both RESP arrays (`*3\r\n$3\r\nSET\r\n...`) and plain inline lines
(`SET k v\n`) are accepted, so `redis-cli`, `nc`, and telnet all work.

## Durability

Every mutation is encoded, written, and flushed to disk **before** it becomes
visible in memory. Records are length-prefixed and CRC-32 checked, so a process
killed mid-write leaves a torn final record that recovery detects, discards, and
truncates away. Expiry deadlines are persisted as absolute timestamps, so keys
that expire while the process is down do not come back.

```bash
./build/kvstore --port 7391 --wal /tmp/demo.log &
printf 'SET user:1 ikbal\r\n' | nc -w 2 127.0.0.1 7391
kill -9 %1                     # hard crash
./build/kvstore --port 7391 --wal /tmp/demo.log &
printf 'GET user:1\r\n' | nc -w 2 127.0.0.1 7391    # -> ikbal
```

## Build options

| Option | Default | Effect |
|---|---|---|
| `KVSTORE_WERROR` | `OFF` | Treat warnings as errors |
| `KVSTORE_SANITIZE` | `OFF` | Build with ASan + UBSan |

## Layout

```
include/kvstore/
  server.hpp      sockets, event loop, connection buffers
  protocol.hpp    RESP parsing and reply encoding
  storage.hpp     hash map, locking, expiry
  wal.hpp         log record format, append, replay
src/
  main.cpp        argument parsing, startup, signal handling
  server.cpp      + command dispatch
  protocol.cpp
  storage.cpp
  wal.cpp
tests/test_main.cpp
EXPLAINER.md      architecture and the OS mechanics underneath it
```

`EXPLAINER.md` is the long-form walkthrough: what a file descriptor and a TCP
socket actually are, why network data arrives in arbitrary chunks, where a
`write()` really lands before `fsync`, why readers should not block readers, and
a line-by-line tour of the code with tracing exercises.

## Limitations

No log compaction, no authentication or TLS, no replication, string values only.
`poll` is O(connections) per iteration. See the last section of `EXPLAINER.md`
for the full list and what each would take to fix.
