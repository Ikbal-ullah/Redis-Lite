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

Ships with a terminal client, a dependency-free Python SDK, a benchmark suite,
and a container image.

## Quick start

```bash
docker compose up -d
./build/kvstore-cli SET user:1 ikbal
```

Or build from source:

```bash
cmake -S . -B build && cmake --build build
./build/kvstore --port 6379 --wal wal.log
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

## Terminal client

```bash
./build/kvstore-cli                       # interactive prompt
./build/kvstore-cli PING                  # one-shot, exits 0/1/2
printf 'SET a 1\nGET a\n' | ./build/kvstore-cli
```

Colour-coded output, quoted arguments (`SET k "hello world"`), and exit codes
suitable for scripting: `0` success, `1` error reply, `2` connection failure.
Colour is suppressed automatically when stdout is not a terminal, and when
`NO_COLOR` is set.

## Python SDK

Standard library only — no `pip install`.

```python
import sys; sys.path.insert(0, "sdk")
from redis_lite import Client

with Client(host="127.0.0.1", port=6379) as client:
    client.set("user:1", "ikbal")
    client.get("user:1")            # 'ikbal'
    client.expire("user:1", 60)     # True
    client.delete("user:1")         # 1
    client.ping()                   # 'PONG'
```

Binary-safe values, dictionary sugar (`client["k"] = "v"`), pipelining, and
typed exceptions (`CommandError`, `ConnectionError`, `ProtocolError`). A client
owns one socket and is not thread-safe — use one per thread.

## Benchmark

```bash
python3 scripts/benchmark.py --ops 10000 --threads 8
```

```
Workload          Ops   Time (s)        Req/s   p50 (ms)   p99 (ms)   max (ms)   Errors
---------------------------------------------------------------------------------------
SET            10,000      0.393       25,438      0.286      0.787      2.049        0
GET            10,000      0.382       26,157      0.278      0.777      2.386        0
MIXED 1:1      10,000      0.386       25,894      0.276      0.810      2.537        0
```

That run is against a `--no-fsync` server. With the default per-write flush,
SET drops to ~370 req/s while GET is essentially unchanged — the durability
tax, and evidence that readers are not blocked by the disk. See `EXPLAINER.md`
§6.3.

## Docker

```bash
docker compose up -d          # build, start, publish 127.0.0.1:6379
docker compose ps             # (healthy) once the PING check passes
docker compose down           # stop, keeping the data volume
docker compose down -v        # stop and delete the data
```

Multi-stage build: the compiler stage is discarded, leaving a ~136 MB runtime
image that runs as a non-root user. The test suite runs during the image build,
so a broken image fails to build. `wal.log` lives on a named volume, so data
outlives the container.

## Build options

| Option | Default | Effect |
|---|---|---|
| `KVSTORE_WERROR` | `OFF` | Treat warnings as errors |
| `KVSTORE_SANITIZE` | `OFF` | Build with ASan + UBSan |

The project is warning-free under `-Wall -Wextra -pedantic -Werror` on both
Clang and GCC; the container build enables `KVSTORE_WERROR` to keep it that way.

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
  cli_main.cpp    kvstore-cli, standalone terminal client
  protocol.cpp
  storage.cpp
  wal.cpp
sdk/redis_lite.py       dependency-free Python client
scripts/benchmark.py    throughput and latency harness
tests/test_main.cpp
Dockerfile              multi-stage build
docker-compose.yml      port binding and persistent volume
EXPLAINER.md            architecture and the OS mechanics underneath it
```

`EXPLAINER.md` is the long-form walkthrough: what a file descriptor and a TCP
socket actually are, why network data arrives in arbitrary chunks, where a
`write()` really lands before `fsync`, why readers should not block readers, and
a line-by-line tour of the code with tracing exercises.

## Limitations

No log compaction, no authentication or TLS, no replication, string values only.
`poll` is O(connections) per iteration. See the last section of `EXPLAINER.md`
for the full list and what each would take to fix.
