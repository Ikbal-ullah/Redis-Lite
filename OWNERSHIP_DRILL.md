# Ownership Drill

Goal: be able to defend every line of this system in an interview, and then
prove it by rewriting the two core files from scratch.

## 1. Architecture

```
                         ┌───────────────────────── one thread ─────────────────────────┐
                         │                                                               │
  redis-cli / redis-py   │   Server::run()  (src/server.cpp)                             │
  redis-benchmark ──TCP──┼─► poll(2) over: wake pipe │ listener │ every client fd       │
                         │        │                                                      │
                         │        ├─ accept_ready()   O_NONBLOCK, CLOEXEC, NODELAY,      │
                         │        │                   EMFILE spare-fd trick              │
                         │        ├─ handle(conn)                                        │
                         │        │    read_ready()  recv → read_buf (≤1 MiB/wakeup)     │
                         │        │    process()     parse_command() ── src/protocol.cpp │
                         │        │                    INCOMPLETE → wait for more bytes  │
                         │        │                    ERROR      → -ERR, close after    │
                         │        │                    OK         → execute_command()    │
                         │        │                                     │                │
                         │        │                     src/database.cpp ▼                │
                         │        │                   Database::set/del/expire_at        │
                         │        │                     1. Wal::append()  ─────────┐     │
                         │        │                     2. Storage::set()          │     │
                         │        │                                                │     │
                         │        │    reply → write_buf          (no / always)    │     │
                         │        │          → held_buf           (group, unsynced)│     │
                         │        │    pump()/flush() send ← write_buf; POLLOUT    │     │
                         │        │                  only while write_buf non-empty│     │
                         │        └─ run_timers()                                  │     │
                         │             maybe_group_commit(): ≥64 recs or ≥10 ms ───┤     │
                         │               → Wal::sync() → held_buf ⇒ write_buf      │     │
                         │             Storage::active_expire(now, budget=200)     │     │
                         └─────────────────────────────────────────────────────────┼─────┘
                                                                                   ▼
   src/storage.cpp                                    src/wal.cpp
   unordered_map<string, {value, expire_at}>          wal.log: [CRC32|Op|KLen|VLen|ExpAtMs|Key|Val]*
   set<(expire_at, string_view→node key)>             replay(): torn tail → ftruncate+fsync
                                                                mid-file damage → WalCorruption (exit 3)
                                                      rewrite(): tmp → fsync → rename → reopen → dir fsync
                                                      durable_sync(): F_FULLFSYNC (macOS) / fsync
```

## 2. The 10 hardest questions

**Q1. Redis-Lite is single-threaded. Why not a thread pool — and where does the throughput come from?**
The baseline had 4 I/O threads, but every write held `wal_mutex_` across the
fsync, so writes were serialised anyway, and the thread doing an fsync stalled
every connection it owned (AUDIT_REPORT B1). Log order must equal apply order,
so a WAL has one writer no matter how many threads there are. Throughput comes
from never blocking on a socket (`Server::run`, `src/server.cpp`), from
pipelining (`Server::process` loops over every whole command in `read_buf`
before writing once), and from amortising fsync (`Server::maybe_group_commit`).
The `-P 16` rows in BENCHMARKS.md show the effect. Scaling past one core would
mean sharding the keyspace across independent loops, each with its own WAL.

**Q2. Trace one `SET k v` under `--fsync group`, from bytes to `+OK`.**
`Server::handle` → `Server::read_ready` (recv until EAGAIN into `read_buf`) →
`Server::process` → `parse_command` (`src/protocol.cpp`) returns `kOk` with
`string_view`s into `read_buf` → `execute_command` → `Database::set`
(`src/database.cpp`): `Wal::append` writes the frame, then `Storage::set`
applies it. Back in `process`: `appended_seq` moved and `unsynced() > 0`, so the
`+OK` bytes are moved from `write_buf` into `held_buf` and the fd is pushed onto
`waiting_`. At 64 unsynced records (checked in `process`) or once the oldest is
10 ms old (`run_timers` → `maybe_group_commit`, with `poll_timeout_ms` waking
the loop on time), `Wal::sync` does one `F_FULLFSYNC` and every held buffer is
moved to `write_buf` → `flush_released` → `send`.

**Q3. How does recovery tell a torn tail from mid-file corruption, and when could it be wrong?**
`Wal::replay` + `valid_frame` (`src/wal.cpp`). At the first bad frame it scans
every later byte offset for a frame that parses *and* passes its CRC. If it finds
one, bytes were written after the damage, so this cannot be a torn append: it
throws `WalCorruption` and never touches the file (`main` exits 3). If there is
none, it is a torn tail: `ftruncate` plus `durable_sync`. Without the scan, a bit
flip in a length field makes a middle record look like it "runs past EOF", which
looks like a torn tail (test `corrupted_length_field_mid_file_fails_hard`). Where
it can be wrong: random garbage passes a CRC with probability about 2⁻³², and a
torn final record whose *value bytes* happen to contain a complete valid frame
would be reported as corruption. That second case fails safe (refuse to start)
rather than losing data.

**Q4. Under group commit, can a client read data that is not durable yet?**
Yes. `Database::set` applies to memory right after `write(2)`, before the group
fsync. Connection A's `+OK` is held, but connection B can `GET` the value at
once. If the machine loses power before the fsync, B has seen a write that never
happened. Redis has the same property with `appendfsync everysec`. Two fixes:
hold *every* reply, reads included, until the next sync (higher read latency),
or apply mutations to memory only after the fsync (they would need a staging
area). Writer-side guarantees are not affected: nothing is acknowledged before
it is durable.

**Q5. Why is an fsync failure fatal instead of retried?**
`Wal::sync` turns any `durable_sync` error into `WalFatal`, and `execute_command`
deliberately does not catch it. This is "fsyncgate": after a failed writeback,
Linux can mark the dirty pages clean and drop them, so a second `fsync` returns
success for data that is gone. The only safe recovery is to crash and replay
from what is actually on disk. A failed `write(2)` (e.g. ENOSPC) is different:
`Wal::append` truncates the partial frame back to `file_size_`, so later frames
cannot follow garbage, and the client gets `-ERR`. If that rollback fails too,
the result is also `WalFatal`.

**Q6. Why `F_FULLFSYNC`, and what does it cost?**
On macOS, `fsync(2)` only hands data to the drive, whose volatile cache can still
lose it on power loss. `fcntl(F_FULLFSYNC)` also flushes the drive cache
(`durable_sync`, `src/wal.cpp`). That flush is why `--fsync always` is limited
to a few hundred writes per second on this machine (BENCHMARKS.md): every SET
blocks the single thread for one device flush. Group commit exists for exactly
this reason. Some filesystems reject `F_FULLFSYNC`, so the code falls back to
`fsync`.

**Q7. Walk through a crash at each step of `COMPACT`.**
`Wal::rewrite`: (1) write `<wal>.tmp` and `durable_sync` it. A crash here leaves
the old log intact plus a stale tmp, which `Wal::replay` unlinks at startup.
(2) `rename(tmp, wal)` is the atomic commit point. (3) Open the new file
*before* closing the old fd, so appends never go to the unlinked inode. (4)
`sync_dir`: without the directory fsync, a power cut can roll the rename back.
That is still safe, because the old log is complete. A failure after the rename
is `WalFatal`. Known cost: the rewrite runs on the event loop and is O(live
keyspace). Redis avoids that stall by forking (`BGREWRITEAOF`, copy-on-write)
and buffering writes that arrive during the rewrite.

**Q8. How is expiry bounded, and why is the index safe?**
Lazy expiry: `Storage::get` erases a dead key when it is read. Active expiry:
`Storage::active_expire` pops at most `budget` (200) entries from the front of
`std::set<pair<deadline, string_view>>`. The cost is proportional to the keys
that are actually dead, never to the keyspace (audit A5). If the budget runs
out, `run_timers` schedules the next cycle immediately. The `string_view`
points at the key inside the `unordered_map` node. Nodes never move on rehash,
so the view stays valid until the node is erased, which is why `erase_it` removes
the index entry *before* the node (ASan would catch the wrong order). Evictions
are not logged: deadlines are absolute wall-clock ms, so `Database::recover`
drops anything already dead. One caveat: the wall clock can step backwards
(NTP), which keeps keys alive longer.

**Q9. A client pipelines 1 GB of GETs and never reads. What happens?**
Replies pile up in `write_buf`. Once `Connection::backlog()` reaches
`ServerConfig::output_high_water` (16 MiB), `Server::process` stops parsing
(`input_paused`) and `run` stops requesting `POLLIN` for that fd. TCP flow
control then pushes back on the client. `Server::pump` loops process→flush while
the connection is paused but the backlog has drained below the mark. That loop
fixes a real stall found under load: if a single `flush` drained the whole
backlog, the connection was left paused with nothing to send, so poll asked for
neither `POLLIN` nor `POLLOUT` and it hung forever (deterministic regression
test `fast_reader_resumes_after_backpressure_without_stalling`). A request still
incomplete past `kMaxQueryBuffer` (128 MiB) gets `-ERR` and the connection is
closed. `POLLOUT` is requested only while `write_buf` is non-empty, because an
idle socket is always writable and asking anyway would turn `poll` into a busy
loop.

**Q10. How do you know there are no fd leaks, and what happens at the fd limit?**
`Connection::~Connection` is the only place a client fd is closed, and
`close_conn` is `conns_.erase(fd)`, so every exit path (EOF, ECONNRESET,
protocol error, shutdown) releases it exactly once. In `accept_ready`, the
`Connection` takes ownership before any setup call that can throw. The test
`no_descriptor_leaks_across_connection_churn` opens 500 connections (half vanish
mid-request), then counts `/dev/fd`. At `EMFILE` the pending connection keeps the
listener readable forever, so a plain `return` would spin at 100% CPU.
`accept_ready` closes the reserved `spare_fd_`, accepts the peer and closes it,
then re-reserves the spare fd.

## 3. Rewrite these two files from scratch

Delete them, keep the headers and tests, and rebuild until
`ctest` + `tests/chaos_crash_test.py` pass under ASan/UBSan.

1. **`src/wal.cpp`**: frame encode/decode, the torn-tail vs. corruption scan,
   partial-write rollback, `F_FULLFSYNC`, and the
   tmp/fsync/rename/reopen/dir-fsync sequence. This is the durability argument.
   Tests: `tests/test_wal.cpp` (torn tail at every byte offset, mid-file flips,
   compaction).
2. **`src/server.cpp`**: the `poll` loop, partial I/O, EAGAIN/EINTR,
   half-close, backpressure, EMFILE, and group commit with `held_buf`. This is
   the concurrency argument. Tests: `tests/test_server.cpp`,
   `tests/test_redis_py.py`.

Checklist for the rewrite: no locks; the only blocking syscall is fsync; every
`write`/`send`/`recv` sits in a loop; no `string_view` outlives a buffer
mutation; no reply leaves before the record it acknowledges is durable.
