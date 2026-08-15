# Internalization Guide

**A four-hour path to complete mastery of this codebase.**

This document is self-contained. It assumes you can read C++ but have never
written a network server, never called `fsync`, and never had to reason about
what a file descriptor actually is. By the end you will be able to defend every
design decision in this repository, trace any byte from a client's socket to the
platter and back, name the exact failure mode each safeguard prevents, and
propose a credible re-architecture under interviewer pressure.

Nothing here is hand-waved. Every code excerpt is real and current. Every
performance number was measured on this machine, and the measurement command is
given so you can reproduce it. Where the implementation has a genuine weakness,
this guide names it — because being the person who already knows the weakness is
worth more in an interview than pretending there isn't one.

---

## How to use this document

### The method

Reading this document twice will not make you good at it. **Retrieval** will.
The single most robust finding in the learning-science literature is that
trying to recall something — and failing, then checking — builds far more
durable knowledge than re-reading the same passage.

So the structure is deliberate:

1. **Read** a section.
2. **Close it** and answer the checkpoint questions out loud, from memory.
3. **Check** your answer against the printed one.
4. **Run** the matching drill so the abstraction becomes something you watched
   happen.

If you skip step 2 you will finish in three hours and remember a third of it.
The checkpoints are the guide.

### Set up first (do this now, before the clock starts)

```bash
cd /Users/mdikbalmashihullah/Redis-Lite
cmake -S . -B build && cmake --build build
ctest --test-dir build --output-on-failure
```

You should see `27 passed, 0 failed`. Then confirm the drill harness works:

```bash
./scripts/drills.sh
```

Keep three terminals open for the whole session: one for the server, one for a
client, one for `git grep` and file reading.

### The schedule

| Time | Block | What you get |
|---|---|---|
| **0:00–0:10** | Orientation | The pitch, the map, what the thing *is* |
| **0:10–1:00** | **Hour 1 — The Machine** | File descriptors, sockets, TCP framing, `poll`, the storage hierarchy |
| **1:00–2:00** | **Hour 2 — The Code** | Every file, every critical function, line by line |
| **2:00–3:00** | **Hour 3 — Traces & Failures** | Five full runtime traces, race hunting, the drills |
| **3:00–4:00** | **Hour 4 — Interview** | 40 rapid-fire, 12 deep questions, 6 re-architecture problems |

If you fall behind, cut Hour 4's re-architecture problems, not the checkpoints.

### A map of the repository

```
include/kvstore/          the interfaces — read these first, they carry the contracts
  wal.hpp        133      log record format, sync policy
  storage.hpp    132      the map, the two locks, expiry
  protocol.hpp   105      RESP types, parse contract, limits
  server.hpp     233      Buffer, Connection, EventLoop, Server
src/
  wal.cpp        327      encode, append, fsync, replay, CRC-32
  storage.cpp    173      the dual-lock write path, lazy expiry
  protocol.cpp   191      the incremental parser, reply encoders
  server.cpp     625      poll loop, accept, partial reads/writes, dispatch
  main.cpp       168      argument parsing, startup order, signal handling
  cli_main.cpp   540      kvstore-cli: reply parsing, colours, exit codes
sdk/redis_lite.py 391     dependency-free Python client
scripts/
  benchmark.py   307      threaded load generator, nearest-rank percentiles
  drills.sh               eight hands-on failure drills
tests/test_main.cpp 853   27 cases
```

About 4,200 lines total. Small enough to hold entirely in your head by 4:00.

---

# ORIENTATION (0:00–0:10)

## The pitch

Learn this. It is the answer to "tell me about your project," and the first
sixty seconds decide how the rest of the interview goes.

> It's a durable key-value store — a small Redis — in C++20 with no third-party
> dependencies. Clients speak RESP over TCP. The server is non-blocking: one
> `poll` event loop per thread, and a connection is owned by exactly one thread
> for its whole life, so per-connection state needs no locking at all.
>
> The only shared mutable state is the hash map, which sits behind a
> `std::shared_mutex` so readers run concurrently and only writers serialise.
>
> Durability is a write-ahead log. Every mutation is encoded, written, and
> flushed to disk *before* it becomes visible in memory, so the client is never
> told OK for something that isn't durable. Records are length-prefixed and
> CRC-32 checked, so a process killed mid-write leaves a torn tail that recovery
> detects, discards, and truncates.
>
> The interesting design decision is that writers hold *two* locks: a plain
> mutex across both the log append and the map update — which is what makes log
> order identical to memory order — and the exclusive map lock only for the
> hash assignment itself. That keeps the millisecond-scale `fsync` out of the
> readers' critical section.

Then stop talking. That paragraph contains at least four threads an interviewer
will want to pull on, and every one of them is a thread you are prepared for.

## What the system does, in one diagram

```
   redis-cli / kvstore-cli / Python SDK
                  |
                  |  TCP :6379,  RESP wire protocol
                  v
   +-------------------------------------------------------------+
   |  Server                                                     |
   |                                                             |
   |   listener fd ---> EventLoop 0 (thread)  ---+               |
   |                    EventLoop 1 (thread)  ---+-- poll(2)     |
   |                    EventLoop 2 (thread)  ---+   loops       |
   |                    EventLoop 3 (thread)  ---+               |
   |                          |                                  |
   |                          |  each owns its connections       |
   |                          |  outright: no locking on         |
   |                          |  buffers or parse state          |
   |                          v                                  |
   |                    +------------------+                     |
   |                    |     Storage      |  <-- the ONLY       |
   |                    |  unordered_map   |      shared state   |
   |                    |  shared_mutex    |                     |
   |                    |  wal_mutex       |                     |
   |                    +--------+---------+                     |
   |                             |                               |
   |                             v                               |
   |                    +------------------+                     |
   |                    |       Wal        |  append-only        |
   |                    |  write + fsync   |  CRC-32 per record  |
   |                    +--------+---------+                     |
   |   maintenance thread        |                               |
   |   (purges expired keys)     v                               |
   +----------------------------------------------------------+ |
                                 |
                            wal.log on disk
```

Five moving parts. Commit them to memory now: **listener, event loops, storage,
log, maintenance thread.**

---

# HOUR 1 — THE MACHINE UNDERNEATH (0:10–1:00)

You cannot defend this code without knowing what the operating system is doing
beneath it. This hour is the foundation. Do not skip it because it looks like
background material — at least a third of the interview questions live here.

## 1.1 What a file descriptor actually is (10 min)

A file descriptor is **a small non-negative integer**. That is the entire type.

```c
int fd = 7;
```

It is an index into a per-process array the kernel maintains, called the file
descriptor table. Entry 7 in *your* table points at a kernel object. Entry 7 in
another process's table points at something else entirely. The number is
meaningless outside the process that owns it.

```
  YOUR PROCESS                       KERNEL OBJECTS
 +----------------+          +---------------------------------+
 | fd 0  stdin    |--------->| terminal                        |
 | fd 1  stdout   |--------->| terminal                        |
 | fd 2  stderr   |--------->| terminal                        |
 | fd 3  wal.log  |--------->| file: inode, offset 156         |
 | fd 4  listener |--------->| socket: LISTEN 0.0.0.0:6379     |
 | fd 5  wakeup r |--------->| pipe read end                   |
 | fd 6  wakeup w |--------->| pipe write end                  |
 | fd 7  client   |--------->| socket: ESTABLISHED,            |
 |                |          |   recv buffer, send buffer      |
 +----------------+          +---------------------------------+
```

Three consequences drive real decisions in this codebase:

**1. The kernel holds the state, not your process.** The read/write offset of
`wal.log` lives kernel-side. That is what lets `O_APPEND` make "seek to end,
then write" a single atomic operation — both halves happen inside the kernel
under its own locking, with no window for another writer to slip in between.

**2. Descriptors are finite and leak permanently.** Every process has a limit
(`ulimit -n`; often 256 on macOS, 1024 on Linux). Leak one per connection and
the server dies after N clients with `EMFILE`. There is no garbage collector.
This is why `Connection` closes its fd in the destructor and deletes its copy
constructor:

```cpp
Connection::~Connection() { close_fd(fd_); }

Connection(const Connection&) = delete;
Connection& operator=(const Connection&) = delete;
```

If you could copy a `Connection`, two objects would hold the same integer and
both destructors would `close()` it. The second `close()` shuts down whatever
descriptor the kernel has since recycled that number for — and the symptom is a
*completely unrelated* connection dying. Deleting the copy constructor makes
that bug uncompilable.

**3. Everything is a descriptor.** Files, sockets, pipes, terminals. `read()`
and `write()` work on all of them. That uniformity is exactly what makes the
self-pipe trick possible: `poll()` cannot wait on "a bool in memory changed,"
but it can wait on a pipe — so we turn the flag change into one byte written to
a pipe.

Verify on a running server:

```bash
./build/kvstore --port 6379 --wal wal.log &
lsof -nP -p $(pgrep kvstore) | grep -E "TCP|wal|PIPE"
```

## 1.2 What a TCP socket is at the OS level (10 min)

A socket is a kernel object with **two queues attached**: a receive buffer and a
send buffer, typically 64 KB to a few MB each.

```
    your code                     KERNEL SOCKET (fd 7)
                         +----------------------------------+
  recv(fd,...)  <------- | receive buffer  [bytes.......]   | <--- network
                         |                                  |
  send(fd,...)  -------> | send buffer     [bytes.......]   | ---> network
                         +----------------------------------+
```

This is the single most important mental correction for a beginner:

- **`recv()` does not touch the network.** It copies bytes out of the receive
  buffer, which the network card and TCP stack already deposited there,
  asynchronously, before you asked.
- **`send()` does not transmit.** It copies bytes *into* the send buffer and
  returns. The kernel transmits on its own schedule, retransmits what is lost,
  and handles acknowledgements you never see.

From which everything else follows:

- `recv()` returning 4 does not mean the peer sent 4 bytes. It means 4 bytes
  were in the buffer at that instant.
- `send()` returning 200 does not mean 200 bytes reached the peer. It means 200
  bytes *fit*. If the buffer was nearly full it takes what fits and returns
  less — a **partial write** — and the rest is your problem.

**The server lifecycle**, exactly as `Server::open_listener` performs it:

```
  socket()      create the object, get an fd
  setsockopt()  configure (SO_REUSEADDR)
  bind()        claim address:port
  listen()      start completing TCP handshakes into a backlog queue
  accept()      pop one completed connection -> a BRAND NEW fd
  recv/send     move bytes
  close()       release
```

The part that surprises people: **`accept()` returns a new descriptor.** The
listening socket is never read from or written to. It is a factory. Each
`accept()` hands back a separate, independent connection.

Two socket options this server sets, and the exact failure each prevents:

| Option | Without it |
|---|---|
| `SO_REUSEADDR` | Restarting the server fails with `EADDRINUSE` for up to two minutes, because sockets from the previous run linger in `TIME_WAIT` holding the port |
| `TCP_NODELAY` | Nagle's algorithm delays small replies ~40ms hoping to coalesce them with a follow-up packet that, for a request/response store, never comes |

## 1.3 TCP is a byte stream, not a message service (10 min)

**This is the most important sentence in network programming**, and the one
beginners disbelieve until it costs them a production outage.

TCP guarantees exactly two things: every byte you send arrives, and they arrive
**in order**. It guarantees nothing about *grouping*. The boundaries between
your `send()` calls are not transmitted. They do not exist on the wire.

A client calls `send()` **once** with 34 bytes:

```
*3\r\n$3\r\nSET\r\n$4\r\nslow\r\n$5\r\nsplit\r\n
```

Your server's `recv()` may legitimately observe any of these:

```
one segment:   [*3\r\n$3\r\nSET\r\n$4\r\nslow\r\n$5\r\nsplit\r\n]

split:         [*3\r\n$3\r\nSET\r\n$4\r\nsl]  [ow\r\n$5\r\nsplit\r\n]

byte at a time: [*] [3] [\r] [\n] [$] [3] ... 34 separate recv() calls

coalesced with the NEXT command (pipelining):
   [*3\r\n$3\r\nSET\r\n$4\r\nslow\r\n$5\r\nsplit\r\n*1\r\n$4\r\nPING\r\n]
```

**Why it splits.** Path MTU (~1500 bytes on Ethernet, so any value over ~1.4 KB
is *always* multiple segments), TCP window and congestion control, the sender's
socket buffer filling, Nagle coalescing, and simply calling `recv()` at a moment
when only half has arrived.

So **the function "read one command from the socket" cannot be written.** There
is no such operation. What you can write is:

> Accumulate bytes in a buffer. After each read, ask: does the buffer now begin
> with a complete message? If yes, take it and ask again. If no, keep the bytes
> and go back to waiting.

That is precisely `Connection::fill()` + `Connection::process()`, and it is why
`parse_command` has a `kIncomplete` status that consumes **zero** bytes.

The protocol also has to make "is it complete?" *answerable* — which is why RESP
length-prefixes every bulk string. The parser reads `$5\r\n` and then knows
exactly how many bytes to wait for, instead of scanning for a delimiter that
might legally appear inside the data.

> **Run drill 5 now.** `./scripts/drills.sh 5`
> It sends one command as 34 separate TCP writes, then three commands in one
> write. Same parser, same buffer, both correct.

## 1.4 Blocking, non-blocking, and `poll` (10 min)

A **blocking** `recv()` puts your thread to sleep until data arrives. Costs
nothing while waiting, but that thread can do nothing else. One thread per
connection works fine to a few hundred clients; at ten thousand you are paying
~8 MB of stack each and drowning the scheduler in context switches.

A **non-blocking** descriptor never sleeps:

```cpp
void set_nonblocking(int fd) {
  const int flags = ::fcntl(fd, F_GETFL, 0);        // read current flags
  if (flags < 0) throw std::system_error(errno, std::generic_category(), "fcntl F_GETFL");
  if (::fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) // add O_NONBLOCK, keep the rest
    throw std::system_error(errno, std::generic_category(), "fcntl F_SETFL");
}
```

Note it is a **read-modify-write**. A bare `F_SETFL` would clear flags you have
no business clearing.

Now if nothing blocks, `recv()` returns `-1` with `errno == EAGAIN` when there
is nothing to read. That is **not an error** — it is the descriptor saying
"empty, ask later." Which raises the obvious problem: what stops the program
spinning at 100% CPU asking every socket?

One syscall that blocks on **all of them at once**:

```c
int ready = poll(pfds, nfds, timeout_ms);
```

You hand the kernel an array of `struct pollfd`, each naming a descriptor and
the events you care about. The kernel sleeps your thread and wakes it when at
least one is ready, filling in `revents` to say which. One thread, thousands of
connections, zero busy-waiting.

```c
struct pollfd {
    int   fd;         // descriptor to watch
    short events;     // what you care about: POLLIN | POLLOUT
    short revents;    // what the kernel reports back
};
```

| Flag | Meaning |
|---|---|
| `POLLIN` | Readable — data arrived, *or* the peer closed (read will return 0) |
| `POLLOUT` | Writable — there is room in the send buffer |
| `POLLERR` | An error occurred (you cannot request it; always reported) |
| `POLLHUP` | Peer hung up (always reported) |
| `POLLNVAL` | The fd is not open — a bug in your bookkeeping |

**`poll` is level-triggered.** It reports a condition as long as it *holds*, not
only when it changes. Two direct consequences in this code:

1. The wakeup pipe must be **drained**, or `poll` reports it readable forever
   and the loop spins at 100% CPU.
2. `POLLOUT` must only be requested **when there is something to write**, since
   an idle socket is almost always writable:

```cpp
short events = POLLIN;
if (conn->has_pending_output()) events = static_cast<short>(events | POLLOUT);
```

**Why `poll` and not `epoll`/`kqueue`.** `epoll` is Linux-only; `kqueue` is
BSD/macOS-only; `poll` is POSIX and runs on both. The honest cost: `poll` passes
the entire interest list across the syscall boundary every call and the kernel
scans all of it, so each iteration is **O(total connections)**. `epoll`/`kqueue`
register the interest set once inside the kernel and return only what is ready —
**O(ready connections)**. At 50 connections the difference is noise; at 50,000
it is the whole ballgame. Swapping the backend means rewriting `EventLoop::run()`
and nothing else.

*Have that trade-off ready. It is asked almost every time.*

## 1.5 Where a write really goes: the storage hierarchy (10 min)

When `write(fd, data, n)` returns successfully, **your data is not on the
disk.** There are up to four layers between your variable and the physical
medium.

```
 +-----------------------------------------------------------------+
 | 1. USER-SPACE BUFFER    std::ofstream, FILE*, std::cout          |
 |    Lost on: process crash                       (VOLATILE)       |
 |    Escaped by: fflush(), or not using buffered I/O               |
 +-----------------------------------------------------------------+
 |             write(2) crosses into the kernel here                |
 +-----------------------------------------------------------------+
 | 2. KERNEL PAGE CACHE    the OS's copy, shared by all processes   |
 |    Lost on: power cut, kernel panic             (VOLATILE)       |
 |    SURVIVES: your process being SIGKILLed -- the kernel is       |
 |              still alive and writes it out eventually            |
 |    Escaped by: fsync()                                           |
 +-----------------------------------------------------------------+
 | 3. DRIVE WRITE CACHE    DRAM on the SSD/HDD itself               |
 |    Lost on: power cut                           (VOLATILE)       |
 |    Escaped by: fcntl(F_FULLFSYNC) on macOS                       |
 +-----------------------------------------------------------------+
 | 4. FLASH / PLATTER                              (PERSISTENT)     |
 +-----------------------------------------------------------------+
```

Layer 2 is the one people get wrong in both directions. `--no-fsync` **does**
survive `kill -9`, because the page cache belongs to the kernel and outlives
your process. It **does not** survive a power cut.

The layering exists for speed, and the gap is enormous: roughly 100 nanoseconds
to reach the page cache, versus a millisecond or more to reach flash. A store
doing one `fsync` per write is limited to a few thousand writes per second no
matter how fast its hash map is.

**The macOS detail that catches people out.** On macOS, `fsync()` only pushes
data out of the page cache to the *drive*. The drive may then hold it in its own
volatile DRAM and report success. Only `fcntl(fd, F_FULLFSYNC)` tells the drive
to flush that cache to the medium:

```cpp
#if defined(__APPLE__)
    if (::fcntl(fd_, F_FULLFSYNC) < 0) {
      if (::fsync(fd_) < 0) throw std::system_error(errno, std::generic_category(), "wal fsync");
    }
#else
    if (::fsync(fd_) < 0) throw std::system_error(errno, std::generic_category(), "wal fsync");
#endif
```

A benchmark using plain `fsync()` on a Mac and calling itself crash-safe is
wrong by an order of magnitude. **This is a great thing to say out loud in an
interview** — it demonstrates you have actually thought about durability rather
than typed the word.

> **Run drill 6 now.** `./scripts/drills.sh 6`
> Measured on this machine, single connection, 300 sequential operations:
>
> | | SET | GET |
> |---|---|---|
> | default (`F_FULLFSYNC` per write) | 482 ops/s (2.073 ms) | 14,502 ops/s (0.069 ms) |
> | `--no-fsync` | 13,735 ops/s (0.073 ms) | 46,212 ops/s (0.022 ms) |
>
> Writes collapse ~28×. Reads degrade ~3×, and for a *different* reason — a
> reader never waits on the flush (it takes only the shared map lock), but it
> does share a machine with a saturated disk. Be precise about that distinction;
> claiming reads are unaffected is an overclaim an interviewer will catch.

## 1.6 Mutexes vs. readers-writer locks (10 min)

A **mutex** (`std::mutex`) admits one thread at a time:

```
  time ------------------------------------------------>
  R1  [==read==]
  R2             [==read==]
  R3                        [==read==]
  W1                                   [=write=]
```

Three readers that could not possibly interfere ran one after another. For a
95%-read workload that throws away most of your machine.

A **readers-writer lock** (`std::shared_mutex`) has two modes:

- `std::shared_lock` — **shared/read.** Any number of threads at once.
- `std::unique_lock` — **exclusive/write.** One thread, and no readers.

```
  time ------------------------------------------------>
  R1  [==read==]
  R2  [==read==]     <- genuinely simultaneous, different cores
  R3  [==read==]
  W1               [=write=]   <- waits for readers to drain, then exclusive
  R4                        [==read==]
```

**Why concurrent reads are safe:** reading changes nothing. Two threads walking
the same hash bucket observe identical bytes. Reads become dangerous only when a
*writer* is present, because a writer can rehash the table, invalidate
iterators, or reallocate a `std::string`'s heap buffer while a reader is halfway
through copying it. Exclusive mode exists precisely to guarantee no reader is
inside the map when that happens.

**The costs**, which you should name before the interviewer does: a
`shared_mutex` is a heavier object than a plain mutex — even a read performs an
atomic increment and decrement of a reader count — and in some implementations a
continuous stream of readers can starve writers. For a read-dominated workload
it still wins, and this is a read-dominated workload.

### Checkpoint 1 — answer from memory, then check

1. What *is* a file descriptor, precisely?
2. Why does `Connection` delete its copy constructor?
3. `recv()` returns 12. What do you actually know?
4. Name three reasons a 34-byte command might arrive in two pieces.
5. What does `errno == EAGAIN` mean on a non-blocking `recv`? Is it an error?
6. Why must the wakeup pipe be drained?
7. Why is `POLLOUT` requested conditionally?
8. `write()` returned successfully. Where might the data be?
9. `--no-fsync`, then `kill -9`. Is data lost? Then power cut?
10. Why is `fsync` insufficient on macOS?

<details><summary><strong>Answers</strong></summary>

1. A small non-negative integer indexing the process's file-descriptor table;
   the kernel object holds all the actual state. Meaningless in another process.
2. Copying copies the `int`; both destructors then `close()` it. The second
   closes a descriptor the kernel has since recycled, killing an unrelated
   connection.
3. Only that 12 bytes were in the socket's receive buffer at that instant. It
   says nothing about how many the peer sent or whether a message is complete.
4. Path MTU (~1500 B); TCP window/congestion control; sender's socket buffer
   full; Nagle coalescing; you called `recv()` when only half had landed.
5. "Nothing available right now." Not an error — it is the normal end-of-
   readable signal, and the only reliable one on a non-blocking socket.
6. `poll` is level-triggered: an undrained readable pipe is re-reported every
   call, spinning the loop at 100% CPU.
7. An idle socket is almost always writable, so unconditional `POLLOUT` makes
   `poll` return instantly every time — a busy loop wearing an event loop's
   clothes.
8. A user-space buffer, the kernel page cache, the drive's DRAM cache, or the
   medium. `write()` returning only guarantees it reached the page cache.
9. `kill -9`: **no loss** — the page cache belongs to the kernel, which survives.
   Power cut: **lost**, because the page cache is volatile.
10. macOS `fsync` only flushes page cache → drive; the drive may keep it in
    volatile DRAM. `fcntl(F_FULLFSYNC)` forces it to the medium.

</details>

---

# HOUR 2 — THE CODE, LINE BY LINE (1:00–2:00)

Open each file alongside this section. Read the real code, not just the
excerpts.

## 2.1 `wal.cpp` — durability (15 min)

### The record format

```
 +----------------+----------------+------------------------------+
 | payload_len u32| crc32 u32      | payload (payload_len bytes)  |
 +----------------+----------------+------------------------------+
   little-endian    little-endian    op | key | (op-specific)
```

Payload by opcode:

```
 kSet    (1):  u8 op | u32 klen | key | u32 vlen | value | i64 expire_at_ms
 kDel    (2):  u8 op | u32 klen | key
 kExpire (3):  u8 op | u32 klen | key | i64 expire_at_ms
```

A real record, from `./scripts/drills.sh 1` after `SET user:1 ikbal`:

```
00000000: 1c00 0000 b559 10e5 0106 0000 0075 7365  .....Y.......use
00000010: 723a 3105 0000 0069 6b62 616c 0000 0000  r:1....ikbal....
00000020: 0000 0000                                ....

  1c000000            payload_len = 0x1C = 28
  b55910e5            crc32 of those 28 bytes
  01                  opcode kSet
  06000000            key length 6
  75 73 65 72 3a 31   "user:1"
  05000000            value length 5
  69 6b 62 61 6c      "ikbal"
  0000000000000000    expire_at_ms = 0 (never)

  36 bytes total = 8 header + 28 payload
```

**Why integers are written a byte at a time:**

```cpp
void put_u32(std::vector<char>& out, std::uint32_t value) {
  out.push_back(static_cast<char>(value & 0xFFu));
  out.push_back(static_cast<char>((value >> 8) & 0xFFu));
  out.push_back(static_cast<char>((value >> 16) & 0xFFu));
  out.push_back(static_cast<char>((value >> 24) & 0xFFu));
}
```

Never `memcpy` a struct into a file. That bakes in the machine's byte order
*and* the compiler's struct padding. The file outlives the machine — it gets
copied to a different box or read by a build with different alignment. The
format must be defined in bytes.

### `encode` — payload first, then header

```cpp
void Wal::encode(const WalRecord& record, std::vector<char>& out) {
  payload_.clear();
  payload_.push_back(static_cast<char>(record.op));
  put_bytes(payload_, record.key);
  switch (record.op) {
    case WalOp::kSet:    put_bytes(payload_, record.value);
                         put_i64(payload_, record.expire_at_ms); break;
    case WalOp::kExpire: put_i64(payload_, record.expire_at_ms); break;
    case WalOp::kDel:    break;
  }

  out.clear();
  put_u32(out, static_cast<std::uint32_t>(payload_.size()));
  put_u32(out, crc32(payload_.data(), payload_.size()));
  out.insert(out.end(), payload_.begin(), payload_.end());
}
```

Neither header field is computable until the payload exists — the length *is*
its size and the CRC covers its bytes. Both buffers are members, reused across
calls, so the steady-state write path performs **no allocation**.

The header and payload are assembled into **one** buffer so the record leaves in
a **single** `write()`. A record split across two writes could be torn in the
middle by a crash even though each half succeeded.

### `append` — the durability boundary

```cpp
void Wal::append(const WalRecord& record) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (fd_ < 0) throw std::runtime_error("wal append before open: " + path_);

  encode(record, scratch_);
  write_full(fd_, scratch_.data(), scratch_.size());
  bytes_written_ += scratch_.size();

  if (policy_ == SyncPolicy::kAlways) {
    /* F_FULLFSYNC on Apple, fsync elsewhere -- see 1.5 */
  }
}
```

**Returning from `append()` is the moment the caller is entitled to believe the
record survives a crash.** The flush is inside the function, before the return,
for exactly that reason. Move it later and the entire guarantee collapses.

### `write_full` — every write is a loop

```cpp
void write_full(int fd, const char* data, std::size_t length) {
  std::size_t written = 0;
  while (written < length) {
    const ssize_t n = ::write(fd, data + written, length - written);
    if (n < 0) {
      if (errno == EINTR) continue;   // a signal interrupted us; not an error
      throw std::system_error(errno, std::generic_category(), "wal write");
    }
    written += static_cast<std::size_t>(n);
  }
}
```

`write()` may write fewer bytes than asked, and may fail with `EINTR` if a
signal arrives mid-call. Both are normal. Treating `write()` as all-or-nothing
is one of the quietest ways to corrupt a file: it works for months, then one
truncated record appears the day the machine gets busy.

### `replay` — the recovery loop

The algorithm, per record:

```
  a. At least 8 bytes left for a header?          no  -> STOP
  b. Read payload_len and expected_crc
  c. Does payload_len run past end-of-file?       yes -> STOP   (TORN WRITE)
  d. crc32(payload) == expected_crc?              no  -> STOP   (BIT ROT)
  e. Decode op / key / value / deadline
     Any field running off the end?               yes -> STOP
  f. on_record(record) -> Storage::apply_locked
  g. good_offset = end of this record; continue
```

Then, critically:

```cpp
if (good_offset < contents.size()) {
  if (::ftruncate(fd, static_cast<off_t>(good_offset)) < 0) { /* throw */ }
}
```

**Two things to be able to defend:**

**Why STOP and not SKIP?** Once a record is damaged, the boundary *after* it is
unknowable — the reader would be interpreting arbitrary bytes as a length
header. And stopping is safe: the log is append-only and `fsync` completed
before the client was acknowledged, so anything after the damage was never
acknowledged to anybody.

**Why truncate?** Without it, the next `append` writes *after* the garbage, and
every future replay stops at the same damaged spot — a log that silently stops
recording while appearing to work perfectly. This is the difference between a
system that recovers and one that quietly stops being durable.

Note also `O_RDWR` on the replay path (not `O_RDONLY`) precisely because
truncation needs write access, and `O_CREAT` so a first boot with no log is an
empty log, not an error.

> **Run drills 1, 3, 4** — the format, the torn tail, the bit flip.

## 2.2 `storage.cpp` — concurrency and the dual lock (15 min)

### The two locks

```cpp
std::mutex        wal_mutex_;   // serialises MUTATORS against each other
mutable std::shared_mutex mutex_;  // guards the MAP
```

The naive design uses only the `shared_mutex`, held exclusively across the log
write and the map update. Correct, but it blocks **every reader** for the whole
duration of an `fsync` — a millisecond of every reader stalled on a disk they
are not using.

The actual write path:

```cpp
void Storage::set(std::string_view key, std::string_view value) {
  WalRecord record;
  record.op = WalOp::kSet;
  record.key.assign(key);
  record.value.assign(value);
  record.expire_at_ms = 0;                            // a plain SET clears any TTL

  std::lock_guard<std::mutex> wal_lock(wal_mutex_);   // (1) mutators queue here
  if (wal_ != nullptr) wal_->append(record);          // (2) SLOW: disk
  std::unique_lock<std::shared_mutex> lock(mutex_);   // (3) FAST: memory
  apply_locked(record);
}
```

1. **`wal_mutex_`** — only mutators contend. Held across *both* the append and
   the map update.
2. **Durability first.** If this throws, the map is untouched and the client
   gets an error rather than a false `+OK`.
3. **Exclusive map lock**, taken only *after* the fsync completed, held for a
   hash-map assignment.

**The ordering property.** Because `wal_mutex_` is held across both steps, two
concurrent writers can never append in one order and apply in the other. Log
order and memory order are identical, so replaying the log reproduces exactly
the state the server was serving.

**Deadlock freedom** is by lock ordering: mutators take `wal_mutex_` then
`mutex_`; readers take only `mutex_`. No thread ever takes them in the opposite
order, so there is no cycle. Any future method needing both must follow the same
order — that rule is the entire discipline.

Both locks are RAII guards, which matters most on the failure path: if
`append()` throws, `wal_lock`'s destructor releases `wal_mutex_` during stack
unwinding. Hand-written `lock()`/`unlock()` would leave it held forever and hang
every subsequent writer — a deadlock caused by an unrelated disk error.

### The read path

```cpp
std::optional<std::string> Storage::get(std::string_view key) const {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  auto it = map_.find(key);                    // heterogeneous: no temp string
  if (it == map_.end()) return std::nullopt;
  if (it->second.expired(now_ms())) return std::nullopt;   // lazy expiry
  return it->second.value;                     // COPIED under the lock
}
```

Three things happening at once:

**Heterogeneous lookup.** `StringHash` declares `using is_transparent = void;`,
and the map is `std::unordered_map<std::string, Entry, StringHash, std::equal_to<>>`.
Without that, `find(string_view)` would construct a temporary `std::string` — a
heap allocation and a copy — on **every single GET**, purely to throw away.
(Heterogeneous `erase()` is C++23, which is why the delete path does `find()`
then `erase(iterator)`.)

**Lazy expiry.** An expired entry is reported absent but not removed — removing
would need an exclusive lock on a `const`, shared-locked path. Memory is
reclaimed by the maintenance thread.

**The copy is the correctness boundary.** Returning `std::string_view` into
`it->second.value` would compile, pass a single-threaded test, and be a
use-after-free in production:

```
 Thread A (reader)                 Thread B (writer)
 ------------------------------------------------------------------
 shared_lock(mutex_)
 it = map_.find("k")
 view = {it->second.value}
 return view              <- lock released HERE
                                   lock_guard(wal_mutex_)
                                   wal_->append(...)
                                   unique_lock(mutex_)
                                   map_["k"].value = <64 KB value>
                                      -> std::string reallocates
                                      -> OLD HEAP BUFFER FREED
 reply_bulk(out, view)
   -> reads freed memory            <- undefined behaviour
```

Two properties make this especially nasty: short values may be stored inline by
the small-string optimisation, so short-key tests pass and only long values
corrupt (a size-dependent heisenbug); and freed memory usually still *contains*
the old bytes for a while, so it often produces correct output in testing and
garbage under load.

**The rule:** a view is safe only when you can name the object keeping its
memory alive and prove it outlives the view.

### Absolute deadlines

```cpp
record.expire_at_ms = now_ms() + ttl_seconds * 1000;
```

Two load-bearing reasons:

- **Relative TTLs cannot be logged.** Replaying "expire in 60 seconds" three
  days after a crash resurrects a key that should be long dead.
- **It must be the wall clock, not `steady_clock`.** `steady_clock`'s origin is
  unspecified and typically resets on reboot, so a persisted `steady_clock`
  deadline is meaningless next boot. The usual advice — "never use the wall
  clock for timeouts" — is right for *in-memory durations* and exactly wrong for
  *persisted deadlines*. Know that inversion; it is a great answer.

And because deadlines are absolute, **evictions are never logged**: replay
re-checks every deadline against the current clock, so an expired record is
dropped there anyway. The log stays a correct description of state without them.

> **Run drill 2** — watch a key expire *during* downtime and stay dead.

## 2.3 `protocol.cpp` — the incremental parser (12 min)

The signature is the design:

```cpp
ParseResult parse_command(std::string_view input, Command& out);
```

It takes a **view of bytes** and knows nothing about sockets. That is why the
protocol tests never open a descriptor.

| Status | Meaning | `consumed` |
|---|---|---|
| `kOk` | one whole command parsed | bytes to retire |
| `kIncomplete` | not all here yet | **0 — nothing retired** |
| `kError` | malformed; close the connection | 0 |

**`kIncomplete` consuming zero bytes is what makes the parser restartable.** The
caller leaves the bytes where they were, more arrive, and it calls the same
function again on the longer buffer. There is no saved state machine, no
half-parsed command stashed anywhere. **The buffer is the state.** Re-parsing
from the start each time costs microseconds of redundant header scanning and
buys the complete absence of resumable-parser state — which is where framing
bugs actually live.

Two encodings are accepted. The first byte decides:

```cpp
return input.front() == '*' ? parse_array(input, out) : parse_inline(input, out);
```

- **RESP arrays** — `*3\r\n$3\r\nSET\r\n$5\r\nhello\r\n$5\r\nworld\r\n`. What
  every real client sends. Length-prefixed, so values may contain spaces, CRLF,
  or NUL.
- **Inline** — `SET hello world\n`. Supported so `nc` and telnet are usable
  debugging tools.

### The bounds checks are the security boundary

```cpp
if (length < 0 || static_cast<std::size_t>(length) > kMaxBulkLength)
  return failure("Protocol error: invalid bulk length");
```

```cpp
inline constexpr std::size_t kMaxArgs        = 1024;
inline constexpr std::size_t kMaxBulkLength  = 64u * 1024u * 1024u;
inline constexpr std::size_t kMaxInlineLength = 64u * 1024u;
```

Without these, `$4294967295\r\n` is a **remote out-of-memory kill delivered in
thirteen bytes**. The parser is the first thing an unauthenticated stranger
talks to, so every number it reads is hostile until bounded.

There is also a bound on *waiting*: a client dribbling digits with no terminator
would grow the buffer forever, so:

```cpp
if (header_end == std::string_view::npos) {
  if (input.size() > 32) return failure("Protocol error: invalid multibulk length");
  return incomplete();
}
```

Thirty-two bytes is far more than any legitimate count needs; past that it is
not a slow client, it is an attack.

> **Run drill 8** — three hostile probes, all rejected, server still alive.

## 2.4 `server.cpp` — the network machine (15 min)

### `Buffer` — two cursors and one rule

```
   data_:  [ consumed | UNPARSED BYTES | free space........ ]
                      ^read_pos_       ^write_pos_
```

- `readable()` → `{data_.data() + read_pos_, write_pos_ - read_pos_}`
- `consume(n)` → `read_pos_ += n`. **Moves a cursor. Never moves bytes.**
- `ensure_writable(n)` → may `compact()` (memmove to offset 0) and may
  `resize()` (reallocate).

Why two cursors rather than `erase()` from the front: erasing is O(size) *and*,
far worse, it moves the bytes that live `string_view`s point at. `consume()`
must be O(1) and address-stable.

> **The rule:** `ensure_writable()` and `append()` invalidate every view
> previously returned by `readable()`. `consume()` does not.

`consume()` also resets both cursors to zero when the buffer empties, so a
steady request/response workload reuses offset 0 forever and never grows.

### `fill` — drain the socket

```cpp
bool Connection::fill(bool& closed) {
  closed = false;
  while (true) {
    if (input_.size() > kMaxInputBacklog) {           // 128 MB
      reply_error(output_, "ERR request too large");
      close_requested_ = true;
      return false;
    }
    input_.ensure_writable(kReadChunk);               // 16 KB
    const ssize_t n = ::recv(fd_, input_.write_ptr(), input_.writable(), 0);
    if (n > 0) { input_.commit(static_cast<std::size_t>(n)); continue; }
    if (n == 0) { closed = true; return true; }       // orderly FIN, not an error
    if (errno == EINTR) continue;
    if (errno == EAGAIN || errno == EWOULDBLOCK) return true;   // drained
    return false;                                     // real error
  }
}
```

Four distinct outcomes from one call, and conflating any two is a bug:
`n > 0` (more may follow — keep going), `n == 0` (peer sent FIN),
`EINTR` (retry), `EAGAIN` (drained — the *only* reliable end-of-readable signal).

### `process` — parse and execute

```cpp
bool Connection::process(Storage& storage) {
  Command command;
  while (true) {
    const std::string_view view = input_.readable();
    if (view.empty()) break;

    const ParseResult result = parse_command(view, command);
    if (result.status == ParseStatus::kIncomplete) break;      // keep bytes, wait
    if (result.status == ParseStatus::kError) {
      reply_error(output_, result.error);
      close_requested_ = true;
      return false;
    }
    if (command.kind != CommandKind::kEmpty)
      execute_command(command, storage, output_);              // views still valid
    input_.consume(result.consumed);                           // cursor only
  }
  return true;
}
```

The lifetime contract is honoured by *ordering*: `fill()` completes all its
`ensure_writable` calls before `process()` runs, so the buffer's storage is
frozen for the entire parse-and-execute phase. `execute_command` appends to
`output_`, never touching `input_`.

### `flush` — partial writes, the classic bug

```cpp
bool Connection::flush(bool& blocked) {
  blocked = false;
  while (output_sent_ < output_.size()) {
    const ssize_t n = ::send(fd_, output_.data() + output_sent_,
                             output_.size() - output_sent_, kSendFlags);
    if (n > 0) { output_sent_ += static_cast<std::size_t>(n); continue; }
    if (n < 0 && errno == EINTR) continue;
    if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
      blocked = true;     // keep the tail; wait for POLLOUT
      return true;
    }
    return false;
  }
  if (output_sent_ == output_.size()) { output_.clear(); output_sent_ = 0; }
  else if (output_sent_ > kOutputCompactThreshold) {
    output_.erase(0, output_sent_); output_sent_ = 0;
  }
  return true;
}
```

If the peer is slow or has stopped reading, the send buffer fills and `send()`
returns `EAGAIN` with bytes unsent. Exactly three responses exist, two of them
wrong:

- **Drop the tail** → silent protocol corruption; the client gets half a reply,
  desynchronises, and nobody logs an error.
- **Loop until it fits** → you re-invented blocking I/O inside a non-blocking
  server; one slow client stalls every other client on that thread. Trivially
  exploitable DoS: connect, pipeline, never read.
- **Keep the tail, register `POLLOUT`, resume on the next wakeup** → correct.

> **Run drill 7** — one client stalls on a 4 MB reply while others are served in
> ~34 ms.

### The self-pipe trick

A thread parked in `poll()` can only be woken by I/O on a descriptor it watches.
It cannot be woken by a bool changing. So:

```cpp
int fds[2];
if (::pipe(fds) < 0) throw std::system_error(errno, std::generic_category(), "pipe");
wake_read_fd_ = fds[0];
wake_write_fd_ = fds[1];
set_nonblocking(wake_read_fd_);
set_nonblocking(wake_write_fd_);
```

Both ends non-blocking: the reader must not stall on an empty pipe, and the
writer must not stall on a full one — a full wakeup pipe already means "you have
been woken," so a dropped byte costs nothing. This is how `stop()` and `adopt()`
reach a sleeping loop instantly.

The same mechanism handles signals in `main.cpp`. A signal handler runs at an
arbitrary instant on a borrowed stack, possibly mid-`malloc`; almost nothing is
safe to call. `write()` on a descriptor is on the short async-signal-safe list:

```cpp
extern "C" void handle_signal(int) {
  if (g_signal_pipe >= 0) {
    const char byte = 's';
    const ssize_t rc = ::write(g_signal_pipe, &byte, 1);
    (void)rc;
  }
}
```

### `SIGPIPE` must be ignored

Writing to a socket whose peer has gone away raises `SIGPIPE`, whose **default
disposition is to terminate the process**. One client disconnecting at the wrong
moment would kill the whole server. `main()` calls `std::signal(SIGPIPE,
SIG_IGN)` and the code handles the `EPIPE` that `send()` returns instead. macOS
additionally gets per-socket `SO_NOSIGPIPE`; Linux gets per-call `MSG_NOSIGNAL`.

### `accept_ready` — the error cases matter

```cpp
if (errno == EAGAIN || errno == EWOULDBLOCK) return;   // backlog empty
if (errno == EMFILE || errno == ENFILE) return;        // out of fds: refuse, stay alive
if (errno == ECONNABORTED) continue;                   // peer vanished mid-handshake
```

`EMFILE` handling is the mark of a server that has been operated: running out of
descriptors must not be fatal. `ECONNABORTED` is normal on the public internet,
not an error.

### The threading model

One listener, N event loops, one thread each. Loop 0 owns the listener; on
accept it hands the fd to the next loop round-robin and pokes it awake.

**A connection belongs to exactly one thread for its entire life**, so its
buffers and parse state need no synchronisation at all. The only shared mutable
state in the process is `Storage`. This is the memcached model, and it is much
easier to reason about than a thread pool where any thread may touch any
connection.

## 2.5 The clients (8 min)

### `cli_main.cpp`

Shares **no code** with the server — deliberately. It parses *replies*, which
the server never does, and has no use for an event loop. Linking the storage
engine into a client binary to reuse a hundred lines would be the wrong trade.

Key behaviours:

- Tokenises honouring double quotes and escapes, then sends a **RESP array**, so
  `SET greeting "hello world"` is two arguments.
- Colour only when `isatty(STDOUT_FILENO)`, and never under `NO_COLOR` or
  `TERM=dumb`.
- Exit codes: `0` success, `1` server error reply, `2` connection failure. That
  is what makes it usable as the container health check.

### `sdk/redis_lite.py`

The reply reader is the interesting part — the same restartable contract in
Python:

```python
def _read_reply(self) -> object:
    while True:
        try:
            value, end = _parse(self._buf, 0)
        except _Incomplete:
            self._fill()          # recv more, retry from offset 0
            continue
        del self._buf[:end]       # only now retire the bytes
        if isinstance(value, CommandError):
            raise value
        return value
```

`_Incomplete` is a private exception used as control flow for "need more bytes."
Surplus bytes stay in `self._buf` for the next reply, which is what makes
pipelining work.

Note `sendall`, not `send` — the same partial-write problem, solved by the
standard library.

**Thread safety:** a `Client` owns one socket and is **not** thread-safe. Sharing
one across threads interleaves bytes from different commands on the same socket
and desynchronises the reply stream — a silent failure that looks like data
corruption. The benchmark therefore builds one client per thread.

### `scripts/benchmark.py`

Two details worth defending:

```python
ready = threading.Barrier(threads + 1, timeout=...)
```

Every worker connects, then waits at the barrier. Connection setup is excluded
from the timing and the load genuinely starts at once.

```python
def percentile(self, p: float) -> float:
    ordered = sorted(self.latencies_ms)
    rank = max(1, math.ceil(p / 100.0 * len(ordered)))
    return ordered[min(rank, len(ordered)) - 1]
```

**Nearest-rank**, not interpolating. For latency you want a number some request
actually experienced, not the average of two that straddle the rank.

And the honest caveat, which you should volunteer: this measures end-to-end
latency *as a Python client experiences it* — encoding, round-trip, server work,
reply parsing. Past a handful of threads the GIL serialises the Python half and
you are measuring the interpreter, not the server.

### Checkpoint 2 — from memory

1. Why is the WAL header written *after* the payload is built?
2. Why must the whole record leave in one `write()`?
3. Why does replay STOP rather than SKIP a bad record?
4. What breaks without the `ftruncate`?
5. Why two locks instead of one?
6. State the lock ordering rule and why it prevents deadlock.
7. Why does `get()` return by value but take a `string_view`?
8. What does `is_transparent` buy?
9. Why does `kIncomplete` consume zero bytes?
10. What are the three responses to a partial `send()`, and which are wrong?
11. Why must both ends of the wakeup pipe be non-blocking?
12. Why is `SIGPIPE` ignored?

<details><summary><strong>Answers</strong></summary>

1. Neither header field is computable until the payload exists: the length is
   its size, the CRC covers its bytes.
2. A record split across two writes can be torn in the middle by a crash even
   though each individual write succeeded.
3. After a damaged record the *boundary* is unknowable — you would interpret
   arbitrary bytes as a length header. Safe to stop because anything past the
   damage was never acknowledged (fsync precedes the reply).
4. The next append lands after the garbage, and every future replay stops at the
   same spot: a log that silently stops recording while appearing healthy.
5. To keep the millisecond `fsync` out of the readers' critical section, while
   still holding one lock across both append and apply so log order equals
   memory order.
6. Always `wal_mutex_` → `mutex_`, never the reverse; readers take only
   `mutex_`. No cycle, therefore no deadlock.
7. The key is only read during the call while the caller holds it alive. The
   value's view would dangle the instant the shared lock is released and a
   writer reallocated or erased it.
8. Heterogeneous lookup: `find(string_view)` with no temporary `std::string`
   allocation on every GET.
9. So the caller can leave the bytes untouched and retry the identical call
   after more arrive — the buffer is the parser's only state.
10. Drop the tail (silent corruption); loop until sent (blocking I/O in a
    non-blocking server, DoS); keep the tail and wait for `POLLOUT` (correct).
11. The reader must not stall on an empty pipe; the writer must not stall on a
    full one — and a full pipe already means the wakeup was delivered.
12. Its default action terminates the process; one client disconnecting mid-write
    would kill the server. Handle `EPIPE` from `send()` instead.

</details>

---

# HOUR 3 — TRACES, RACES, AND FAILURES (2:00–3:00)

Trace each of these with the source open. Say the steps out loud. This is the
hour that converts recognition into recall.

## Trace A — cold start with an existing log (8 min)

```
1.  main() parses args, then std::signal(SIGPIPE, SIG_IGN)
2.  Wal wal(path, policy)              -- constructor only stores fields; no I/O
3.  Storage storage(&wal)
4.  storage.load_from_wal()
      -> unique_lock(mutex_)
      -> wal.replay(callback)
           open(path, O_RDWR | O_CREAT)   <-- RDWR so a torn tail can be cut
           fstat -> size; read whole file into a std::string
           loop: header -> length check -> CRC -> decode -> apply_locked
           ftruncate(good_offset) if anything followed the last good record
           close  (the replay fd is NOT the append fd)
      -> sweep the map, erase entries whose absolute deadline already passed
5.  wal.open()                          -- NOW opens O_WRONLY|O_CREAT|O_APPEND
6.  Server server(config, storage); server.start()
      open_listener(): socket -> SO_REUSEADDR -> bind -> listen -> getsockname
                       -> set_nonblocking
      construct N EventLoops (each creates its wakeup pipe)
      loops_[0]->set_listener(listen_fd_)
      spawn N I/O threads + 1 maintenance thread
7.  main() creates its own signal pipe, installs SIGINT/SIGTERM handlers,
    then blocks in read() on that pipe
```

**The ordering question an interviewer will ask:** why does `load_from_wal()`
run before `wal.open()` *and* before the listener exists?

Because recovery must be atomic from a client's perspective. There is no socket
to connect to yet, so nobody can observe a half-restored store. `Wal::replay`
even throws if the append descriptor is already open, making the ordering a
checked precondition rather than a convention.

## Trace B — `GET hello` split across two packets (10 min)

Client sends `*2\r\n$3\r\nGET\r\n$5\r\nhe` (19 bytes), then 200 ms later
`llo\r\n` (5 bytes).

**First wakeup:**

```
1. poll() reports POLLIN on fd 7.
2. fill(): ensure_writable(16384); recv() -> 19; commit(19)
           read_pos_=0, write_pos_=19
3. fill() loops; recv() -> -1/EAGAIN. Drained. return.
4. process(): view = readable() -> 19 bytes
     parse_command: front=='*' -> parse_array
       find("\r\n") at 2 -> count = 2  (valid, <= kMaxArgs)
       element 0: "$3\r\n" -> length 3, need 4+3+2 <= available -> args[0]="GET"
       element 1: "$5\r\n" at index 13 -> length 5
                  body = 17, need = 17 + 5 + 2 = 24
                  input.size() == 19.   19 < 24  ->  return incomplete()
5. process() breaks WITHOUT consuming. read_pos_ still 0. All 19 bytes remain.
   The half-built args vector is discarded -- it is a local.
6. output_ empty -> nothing sent. Back to poll().
```

**Second wakeup, 200 ms later:**

```
7.  fill(): ensure_writable(16384). read_pos_==0 so compact() is a no-op and
    there is already room -> NO reallocation. recv() -> 5. write_pos_=24.
8.  process(): parse_command over all 24 bytes -- FROM THE BEGINNING AGAIN.
    The parser has no memory of attempt one; it re-scans the GET header.
9.  need == 24, size == 24 -> args[1] = "hello", kOk, consumed = 24
10. execute_command -> storage.get("hello")
      shared_lock -> find -> expiry check -> copy value out -> unlock
      reply_bulk(output_, value)   or   reply_nil(output_)
11. input_.consume(24) -> read_pos_ >= write_pos_ -> BOTH reset to 0
12. flush() -> send() -> reply on the wire
```

**Say this out loud:** re-parsing from the start is deliberate. It costs a few
microseconds of redundant header scanning and buys the complete absence of
resumable-parser state.

## Trace C — two concurrent `SET`s on the same key (10 min)

T1 runs `SET k A`, T2 runs `SET k B`, simultaneously, on different I/O threads.

**The proof that log order equals memory order:**

`wal_mutex_` is a plain mutex held from before `append()` until after
`apply_locked()`. Mutual exclusion means one of T1/T2 holds it strictly before
the other. Say T1 wins:

```
 T1: acquire wal_mutex_
 T1:   append("k","A") ... fsync ...            LOG: [A]
 T1:   unique_lock(mutex_); map_["k"]="A"       MEM: k=A
 T1: release wal_mutex_
 T2: acquire wal_mutex_
 T2:   append("k","B") ... fsync ...            LOG: [A][B]
 T2:   unique_lock(mutex_); map_["k"]="B"       MEM: k=B
 T2: release wal_mutex_

 Final memory: k=B.  Replay of [A][B]: k=B.  IDENTICAL.
```

Symmetric if T2 wins, and it extends to any number of writers. Note the
`shared_mutex` alone gives you nothing here — it makes each *step* atomic but
says nothing about the *pairing* of two separate steps.

**Now delete `wal_mutex_` and keep only the exclusive map lock around
`apply_locked`, with `append` outside it:**

```
 T1 (SET k A)                T2 (SET k B)              LOG      MEMORY
 -----------------------------------------------------------------------
 append("k","A")                                       [A]      -
                             append("k","B")           [A][B]   -
                             unique_lock(mutex_)
                             map_["k"] = "B"           [A][B]   k=B
                             unlock
 unique_lock(mutex_)
 map_["k"] = "A"                                       [A][B]   k=A
 unlock
```

T1 was descheduled between its append and its apply — ordinary, and *more*
likely here because `append` ends in an `fsync` that can park the thread for
milliseconds.

**Final live state: `k = A`. Log order: A then B. Replay yields `k = B`.**

The server serves `A` to every client until it restarts, then serves `B` forever
after. Nothing crashed, nothing logged an error, and no test that does not
restart the process can see it. Silent, timing-dependent, observable only across
a restart — the worst class of bug there is.

This is exactly what `log_order_matches_memory_order_under_contention` in the
test suite asserts: six threads, 500 writes each to one key, then a fresh store
rebuilt from the log must agree with the live one.

## Trace D — a crash at four instants (10 min)

For each, answer: what did the client see, and what does the store contain after
restart?

| # | Crash point | Client saw | After restart | Consistent? |
|---|---|---|---|---|
| **A** | after `write()`, before `fsync` | nothing | absent, or a torn tail that replay truncates | **Yes** — never acknowledged |
| **B** | after `fsync`, before `apply_locked` | nothing | **present** (replay applies it) | **Yes** — an unacknowledged op may or may not take effect |
| **C** | after `apply_locked`, before the reply is sent | nothing (connection reset) | present | **Yes** |
| **D** | after the reply was sent | `+OK` | present | **Yes** |

The invariant to state precisely:

> An operation that was **acknowledged** always took effect. An operation that
> was **not acknowledged** may or may not have. The forbidden state — an
> acknowledged operation that did not take effect — is impossible because
> `fsync` completes before `Storage::set` returns and the reply is queued only
> after that.

**Now invert the order — log *after* memory — and consider case B':** memory
updated, crash before the log write.

- The client saw nothing yet, **but a concurrent reader on another thread could
  already have observed the new value** and told a different client about it.
- After restart the record is absent. **The key reverts.**
- **Inconsistent.** A value that was read and acted upon vanished. That is
  precisely the failure "write-ahead" exists to prevent.

## Trace E — backpressure (7 min)

A client pipelines requests totalling a 10 MB reply, then stops reading.

```
1. fill() reads the batch; process() appends every reply to output_ (-> 10 MB)
2. flush(): send() succeeds repeatedly until the kernel send buffer fills
   (TCP flow control shrinks the peer's window to zero)
3. send() -> -1/EAGAIN. blocked = true, return true. NOT an error.
   output_sent_ might be 256 KB of 10 MB.
4. run() rebuilds the pollfd with POLLIN | POLLOUT for this connection.
   The THREAD IS NOT STUCK -- every other connection is served normally.
5. Client eventually reads -> kernel drains -> poll reports POLLOUT
   -> flush() resumes from output_sent_
6. output_.erase(0, output_sent_) once the sent prefix exceeds 64 KB
7. All delivered -> output_.clear(), output_sent_ = 0
   -> POLLOUT dropped from the interest set next iteration
```

**The gap you must name before the interviewer does:** there is **no cap on
`output_`**. A client that pipelines aggressively and never reads makes the
server buffer without bound. The input side is bounded (`kMaxInputBacklog`,
128 MB); the output side is not. Real Redis has a per-client output buffer limit
and closes connections that exceed it. That is the fix, and knowing it is
missing is worth more than pretending it isn't.

## Race hunting — three real subtleties (10 min)

These are not in the EXPLAINER. Work them out, then check. They are exactly the
kind of thing a senior interviewer probes for.

### Race 1: `purge_expired` does not take `wal_mutex_`

```cpp
std::size_t Storage::purge_expired() {
  std::unique_lock<std::shared_mutex> lock(mutex_);   // only the map lock
  ...
}
```

**Question:** the maintenance thread can therefore run *between* a mutator's
`append` and its `apply_locked`. Does that break log-order-equals-memory-order?

<details><summary><strong>Answer</strong></summary>

**No, and the reasoning is worth internalising.**

`purge_expired` writes nothing to the log. Its only effect is removing entries
whose absolute deadline has passed. Replay independently re-checks every
deadline against the current clock (`load_from_wal` sweeps after applying), so
the eviction is *reproduced* at recovery without needing a record. The log
remains a correct description of the state.

Deadlock is also avoided: `purge_expired` takes only `mutex_` and never
`wal_mutex_`, so it cannot participate in a cycle. A mutator holding
`wal_mutex_` and waiting for `mutex_` simply waits for the purge to finish.

The subtle interleaving — purge running between append and apply — is harmless
because the two operate on disjoint outcomes: the mutator is installing a live
value, the purge is removing already-dead ones.

</details>

### Race 2: `del()` peeks under a shared lock, then applies

```cpp
bool Storage::del(std::string_view key) {
  std::lock_guard<std::mutex> wal_lock(wal_mutex_);
  {
    std::shared_lock<std::shared_mutex> peek(mutex_);
    auto it = map_.find(key);
    if (it == map_.end() || it->second.expired(now_ms())) return false;
  }                                   // <-- lock released
  if (wal_ != nullptr) wal_->append(record);
  std::unique_lock<std::shared_mutex> lock(mutex_);
  apply_locked(record);
  return true;
}
```

**Question:** the shared lock is released before the exclusive one is taken. Can
another thread change the answer in that window?

<details><summary><strong>Answer</strong></summary>

**No other *mutator* can** — they all need `wal_mutex_`, which this thread holds.

But `purge_expired` **can**, because it does not take `wal_mutex_`. The window:
the peek finds the key live (deadline not yet passed); microseconds later the
deadline passes; the maintenance thread evicts it; `apply_locked` then applies a
`kDel` to a key that is already gone (a harmless no-op), and `del()` returns
`true`.

**Impact:** `DEL` returns `1` instead of `0` for a key that expired at that exact
instant. Benign, and the log stays consistent — replaying a `kDel` for a missing
key is a no-op. Redis has the same class of race around lazy expiry.

**How you would remove it if it mattered:** hold the exclusive lock across the
peek and the apply, or make `purge_expired` take `wal_mutex_`. Both cost more
than the race does, which is why neither is done — but say that trade-off out
loud rather than appearing not to have noticed.

</details>

### Race 3: multi-key `DEL` is not atomic

```cpp
case CommandKind::kDel: {
  std::int64_t removed = 0;
  for (std::size_t i = 1; i < command.args.size(); ++i) {
    if (storage.del(command.args[i])) ++removed;   // separate lock acquisition each
  }
  reply_integer(out, removed);
}
```

<details><summary><strong>Answer</strong></summary>

Each `storage.del()` acquires and releases `wal_mutex_` independently, so
`DEL a b c` produces **three separate log records** and another client can
observe the intermediate state where `a` is gone but `b` still exists.

Real Redis executes commands atomically on a single thread, so its multi-key
`DEL` is all-or-nothing from a client's perspective.

**The fix** would be a `Storage::del_many` taking `wal_mutex_` once, writing one
multi-key record (a new opcode), and applying all deletions under one exclusive
lock. That also makes recovery atomic for the batch.

Volunteering this is a strong signal: it shows you distinguish "works" from
"has the isolation semantics I claimed."

</details>

## The memory lifetime map (5 min)

Know exactly who owns what and how long it lives.

| Object | Points into | Valid until | Enforced by |
|---|---|---|---|
| `Command::args` | the connection's `input_` buffer | `ensure_writable`/`append` on that buffer | ordering: `fill()` fully precedes `process()` |
| `Storage::get` return | nothing — it is a copy | caller's scope | returning `optional<string>` by value |
| `get(key)` parameter | caller's memory | the call | read-only during the call |
| `Buffer::readable()` | `data_` vector | next `ensure_writable`/`append` | documented rule + call ordering |
| `Wal` inside `Storage` | caller-owned `Wal` | caller guarantees | raw pointer + documented contract |
| `Connection` fd | kernel object | `~Connection()` | RAII + deleted copy ctor |

## Hands-on: run every drill (5 min)

```bash
./scripts/drills.sh all
```

Watch drills 3 and 4 especially. Seeing the log get truncated from 80 bytes back
to 65, and a single flipped byte discard exactly one record, converts the CRC
discussion from theory into something you have observed.

### Checkpoint 3

1. Why must `load_from_wal()` precede `wal.open()`?
2. In Trace B, why is there no reallocation on the second wakeup?
3. State the acknowledgement invariant in one sentence.
4. Construct the interleaving that breaks without `wal_mutex_`.
5. Which component can mutate the map without holding `wal_mutex_`, and why is
   that safe?
6. What is unbounded in this server, and what is the standard fix?

<details><summary><strong>Answers</strong></summary>

1. Recovery must be atomic from a client's view: no listener exists yet.
   `Wal::replay` throws if the append fd is open, making it a checked
   precondition.
2. `read_pos_ == 0` (nothing consumed), so `compact()` is a no-op and the 16 KB
   chunk reserved on the first wakeup still has room.
3. An acknowledged operation always took effect; an unacknowledged one may or
   may not have; an acknowledged operation that did not take effect is
   impossible.
4. T1 appends A; T2 appends B; T2 applies B; T1 applies A. Memory ends at A, log
   says A-then-B, replay yields B. Divergence visible only after a restart.
5. `purge_expired`. Safe because it writes no log records and its effect is
   independently reproduced at replay by the absolute-deadline sweep.
6. The per-connection output buffer. Fix: a per-client output buffer limit that
   closes offenders, as Redis does.

</details>

---

# HOUR 4 — THE INTERVIEW SUITE (3:00–4:00)

## 4.1 Rapid fire — 40 questions (15 min)

Cover the answers. Aim for under ten seconds each.

| # | Question | Answer |
|---|---|---|
| 1 | What is an fd? | A small int indexing the process's fd table |
| 2 | What does `accept()` return? | A **new** fd for that connection |
| 3 | `recv()` returns 0? | Peer sent FIN — orderly close, not an error |
| 4 | `recv()` returns -1/EAGAIN? | Nothing available; the drained signal |
| 5 | `recv()` returns -1/EINTR? | A signal interrupted; retry |
| 6 | Purpose of `SO_REUSEADDR`? | Rebind while old sockets sit in `TIME_WAIT` |
| 7 | Purpose of `TCP_NODELAY`? | Disable Nagle; no coalescing partner exists |
| 8 | Default `SIGPIPE` action? | Terminate the process |
| 9 | How is `SIGPIPE` handled here? | Ignored globally; `EPIPE` handled at the call |
| 10 | `poll` level- or edge-triggered? | Level |
| 11 | Cost of `poll` per iteration? | O(total fds) |
| 12 | `epoll`/`kqueue` cost? | O(ready fds) |
| 13 | Why not `epoll` here? | Linux-only; `poll` is POSIX |
| 14 | Why is the wakeup pipe drained? | Level-triggered → otherwise 100% CPU spin |
| 15 | Why non-blocking on both pipe ends? | No stall on empty read or full write |
| 16 | Only async-signal-safe thing the handler does? | `write()` one byte to a pipe |
| 17 | Why `O_APPEND`? | Makes seek-to-end + write atomic across writers |
| 18 | Header layout? | u32 length, u32 CRC-32, then payload |
| 19 | Why length before CRC? | Length finds the next boundary without decoding |
| 20 | Torn tail detection? | Declared length runs past EOF |
| 21 | Bit rot detection? | CRC mismatch |
| 22 | Stop or skip on damage? | Stop — the next boundary is unknowable |
| 23 | Why `ftruncate` after replay? | Else appends land after garbage; log silently dies |
| 24 | Why absolute deadlines? | Relative TTLs resurrect keys on replay |
| 25 | Wall clock or steady clock for deadlines? | Wall — steady's origin resets on reboot |
| 26 | Are evictions logged? | No — replay re-checks deadlines anyway |
| 27 | Which lock do readers take? | Only `shared_lock(mutex_)` |
| 28 | Which locks do writers take? | `wal_mutex_` then `unique_lock(mutex_)` |
| 29 | Why that order? | No cycle → no deadlock |
| 30 | Why is `fsync` outside the map lock? | Readers must not wait on a disk |
| 31 | Why hold `wal_mutex_` across both? | Log order == memory order |
| 32 | Does `get` return a view? | No — a copy; a view would dangle after unlock |
| 33 | What does `is_transparent` enable? | Heterogeneous lookup, no temp string |
| 34 | Is heterogeneous `erase` available? | No, that is C++23 — use `find` then `erase(it)` |
| 35 | Bytes consumed on `kIncomplete`? | Zero |
| 36 | Why cap bulk length? | `$4294967295` is a 13-byte remote OOM kill |
| 37 | Three responses to partial `send`? | Drop (corrupt), spin (DoS), buffer + `POLLOUT` (right) |
| 38 | Why `POLLOUT` only when needed? | Idle sockets are writable → busy loop |
| 39 | Threads per connection? | One owner thread for its whole life; no per-conn locks |
| 40 | Only shared mutable state? | `Storage` |

## 4.2 Twelve deep questions with full answers (25 min)

### Q1. Walk me through what happens when I type `SET foo bar` into your client.

<details><summary><strong>Answer</strong></summary>

The CLI tokenises the line honouring quotes, then encodes a RESP array:
`*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n`, and `send()`s it in a loop
until fully written.

The kernel copies it into the client socket's send buffer, TCP delivers it to
the server socket's receive buffer. The server's `poll()` returns with `POLLIN`
on that fd. `Connection::fill()` calls `recv()` into its `Buffer` repeatedly
until `EAGAIN`.

`Connection::process()` calls `parse_command` on the readable span. It sees `*`,
parses the count, then three bulk strings, producing `Command{kind=kSet, args=[
"SET","foo","bar"]}` where the args are `string_view`s **pointing into the read
buffer** — no copy, no allocation.

`execute_command` checks arity (must be 3) and calls `storage.set("foo","bar")`.

`Storage::set` builds a `WalRecord` with `expire_at_ms = 0`, then takes
`wal_mutex_`, calls `wal_->append(record)` — which encodes payload then header,
`write()`s in a loop, and `fcntl(F_FULLFSYNC)`s. **Only after that returns** does
it take the exclusive map lock and assign the entry.

Back in `execute_command`, `reply_simple(out, "OK")` appends `+OK\r\n` to the
connection's output buffer. `Connection::flush()` `send()`s it; if the kernel
takes it all, the buffer is cleared, otherwise the tail stays and `POLLOUT` is
registered.

The whole path touched one lock the client cares about, held for microseconds,
plus a millisecond of disk that no reader waited on.

</details>

### Q2. Your server holds two locks. Justify that, then tell me what it costs.

<details><summary><strong>Answer</strong></summary>

**The justification** is that the two locks protect different invariants.

`mutex_` (`shared_mutex`) protects the *map's structural integrity* — no reader
may be inside it while a writer rehashes or reallocates.

`wal_mutex_` protects an *ordering relationship between two subsystems*: it
guarantees that the sequence of records in the log matches the sequence of
mutations applied to memory. A single `shared_mutex` makes each step atomic but
says nothing about the pairing of two separate steps.

If I used only the `shared_mutex` held exclusively across both, I would be
correct but would block every reader for the duration of an `fsync` — measured
here at ~2 ms per write. Readers would be stalled on a device they never touch.

**The costs**, honestly:

1. **Writers are fully serialised**, including the fsync. Write throughput is
   capped at roughly `1 / fsync_latency` regardless of core count — measured at
   ~482 ops/s with `F_FULLFSYNC`. Group commit would fix this: batch N pending
   records, one fsync, acknowledge all N. That is the single highest-value
   change I would make.
2. **Two lock acquisitions per write** instead of one — negligible next to the
   fsync, but real if fsync is disabled.
3. **A discipline burden.** Any future method needing both must take them in the
   same order. That is a comment and a convention, not a compiler-enforced rule.
   A lock hierarchy checker or a wrapper type that can only be acquired in order
   would make it structural.
4. **`purge_expired` deliberately sits outside `wal_mutex_`**, which introduces a
   benign race where `DEL` may return 1 for a key that expired microseconds
   earlier. I accepted that because closing it costs more than the race does.

</details>

### Q3. A client sends half a command and disappears. Walk me through the resource lifecycle.

<details><summary><strong>Answer</strong></summary>

The half command sits in that connection's `Buffer`, holding at least one 16 KB
allocation. `parse_command` returns `kIncomplete` and consumes nothing, so the
bytes persist. `poll` keeps the fd in the interest set with `POLLIN`.

**If the client closed cleanly (FIN):** `poll` reports `POLLIN`; `recv()` returns
0; `fill()` sets `closed = true`; `handle_connection` sees `peer_closed` and
calls `close_connection(fd)`, which erases the `unique_ptr<Connection>` from the
map. `~Connection()` runs and `close()`s the fd. The `Buffer`'s vector is freed
by its own destructor. Complete cleanup, no explicit code.

**If the client vanished without FIN** — cable pulled, machine powered off —
**nothing happens.** That is the honest and important answer. The socket stays
`ESTABLISHED` indefinitely from our side. There is no read timeout, no idle
reaper, and TCP keepalive is not enabled. The connection leaks an fd and its
buffer until the process restarts.

**How I would fix it:** track a `last_activity` timestamp per connection and
have the maintenance thread — which already runs once a second — close
connections idle beyond a configurable timeout. Alternatively enable
`SO_KEEPALIVE` with tuned `TCP_KEEPIDLE`/`TCP_KEEPINTVL`, though the default
two-hour idle time is useless without tuning. The application-level idle timeout
is the better answer because it also bounds the memory, not just the fd.

</details>

### Q4. Why is `Storage::get` returning `optional<string>` and not `string_view`? Convince me it isn't premature pessimisation.

<details><summary><strong>Answer</strong></summary>

Because the view would be dangling by the time the caller reads it, and it is
the kind of dangling that testing does not catch.

The shared lock is released when `get` returns. After that instant another
thread may `SET` the key — causing `std::string` to reallocate and free the old
heap buffer — or `DEL` it, destroying the string outright. The caller would be
reading freed memory.

It is not premature pessimisation because there is no safe alternative that is
faster:

- Returning a view would require holding the lock across the caller's use, which
  means returning a lock guard alongside it. That is a viable design — something
  like a `LockedValue` RAII wrapper — but it extends the reader's critical
  section across reply encoding, which is worse for concurrency than one copy.
- Reference counting the values (`shared_ptr<const string>`) would let the view
  outlive the lock safely. That trades one copy for an atomic increment and
  decrement plus an extra indirection on every read, and it changes the
  allocation strategy of the whole store. For values of a few hundred bytes the
  copy usually wins.

So the copy is not laziness; it is the cheapest option that is actually correct.

Worth adding: the *key* parameter is still a `string_view`, and combined with
the transparent hasher that eliminates an allocation per GET on the lookup side.
The asymmetry is deliberate — the key's lifetime is provably the duration of the
call, the value's is not.

</details>

### Q5. Your write throughput is ~482 ops/s. Make it 10× faster without weakening durability.

<details><summary><strong>Answer</strong></summary>

**Group commit.** The bottleneck is one `F_FULLFSYNC` per write, and that cost is
almost entirely fixed latency, not per-byte.

Current: each writer takes `wal_mutex_`, writes, flushes, applies, releases. N
writers cost N flushes.

The change:

1. A writer appends its encoded record to a shared pending batch and registers
   itself as waiting on a condition variable with a batch sequence number.
2. One thread — either a dedicated log writer or whichever writer claims the
   role — takes the whole pending batch, issues **one** `write()` and **one**
   `fsync`, then publishes "batch K is durable."
3. Every writer in batch K wakes, applies its own mutation under the map lock in
   the batch's recorded order, and acknowledges.

With 64 concurrent writers you go from 64 flushes to 1. Throughput approaches
`batch_size / fsync_latency` instead of `1 / fsync_latency`. Durability is
**unchanged** — nobody is acknowledged before their bytes are flushed. Only the
*waiting* is shared.

The ordering guarantee is preserved by assigning each record its position when
it enters the batch and applying in that order.

Cost: latency for a single uncontended write is slightly worse (it may wait for
a batch to form — usually bounded by a short timer), and the code is
substantially more complex. That is the standard trade and it is what PostgreSQL,
MySQL, and Redis AOF all do.

**Second, complementary change:** if the durability requirement is "survive
process crash" rather than "survive power loss", `fdatasync` on Linux avoids
flushing inode metadata, and `appendfsync everysec` semantics (flush once per
second, bounded loss window) is what most Redis deployments actually run.

</details>

### Q6. How would you add replication?

<details><summary><strong>Answer</strong></summary>

The write-ahead log **is** the replication stream. That is the main insight, and
it is why WAL-based systems get replication almost for free.

**Design:**

1. **Give every record a monotonically increasing sequence number (LSN)**, added
   to the header. `wal_mutex_` already serialises writers, so assigning the LSN
   there is trivially correct and requires no new synchronisation.
2. **Replica connects and sends `REPLCONF LSN <n>`.** The primary opens the log,
   seeks to the offset for LSN n+1, and streams from there.
3. **Live tail-following:** after catching up from the file, the replica is
   registered as a subscriber. Each `Wal::append` — still under `wal_mutex_` —
   also pushes the encoded record onto each replica connection's output buffer.
   The existing `POLLOUT` backpressure machinery handles slow replicas without
   modification.
4. **The replica applies records through `Storage::apply_locked`**, the exact
   function recovery uses. One code path for recovery and replication means they
   cannot drift.

**The decisions an interviewer will push on:**

- **Async vs sync replication.** Async: acknowledge the client after the local
  fsync, stream to replicas in the background — fast, but a primary failure can
  lose the tail. Sync: wait for N replicas to acknowledge before replying —
  durable across machine loss, but adds a network round-trip to every write and
  stalls if a replica is down. I would default to async with a configurable
  `WAIT`-style barrier, which is Redis's model.
- **Log growth.** Streaming from offset zero is only viable while the log is
  short. This needs snapshotting: periodically dump the map to a snapshot file
  with its LSN, then a joining replica loads the snapshot and streams from that
  LSN. This is also the fix for the unbounded-log problem generally.
- **Failover and split brain.** Promoting a replica needs consensus about who is
  primary, or you get two primaries accepting writes. That is a Raft-shaped
  problem, and I would not hand-roll it — I would either use an external
  coordinator or accept manual failover.

</details>

### Q7. Your log grows forever. Fix it.

<details><summary><strong>Answer</strong></summary>

**Snapshot plus atomic rename.**

1. Take a consistent point-in-time view of the map. The cheapest correct way
   here: hold `wal_mutex_` (blocking writers, not readers) long enough to record
   the current LSN, then serialise under a shared lock. For a large map that
   pause is too long, so the better version is copy-on-write via `fork()` —
   which is exactly what Redis does for RDB: the child gets a frozen snapshot of
   the address space and the parent keeps serving, paying only page-fault copies.
2. Write the snapshot to `wal.log.tmp` — same record format, so the loader is
   the same code — and `fsync` it.
3. **`rename("wal.log.tmp", "wal.log")`.** `rename` is atomic on POSIX: a crash
   mid-compaction leaves either the complete old log or the complete new one,
   never a blend. Then `fsync` the *directory* to make the rename itself durable
   — a step people forget, and the reason some systems lose files on power cut
   despite fsyncing the data.
4. Records written during compaction must be captured — either buffered and
   appended to the new file before the rename, or written to both.

**Two things to say for credit:**

- The record format needs a **version byte** before shipping this. Right now the
  header has no version field, so a format change would misread old logs. I
  would add it as part of this work.
- Compaction should be triggered by a ratio (log size vs. live data size), not a
  timer — that is what makes it self-tuning under different write patterns.

</details>

### Q8. Why RESP and not JSON or Protocol Buffers?

<details><summary><strong>Answer</strong></summary>

The property that matters for a **streaming** protocol is that the parser can
determine "do I have a complete message?" from a bounded prefix, cheaply, and
without scanning the payload.

RESP length-prefixes everything: `$5\r\n` tells you exactly five bytes follow.
The framing decision is O(1) in the payload size, and the payload is never
inspected, so it is trivially binary-safe.

**JSON** is delimiter-based. To find the end of a message you must scan for a
balanced closing brace *while tracking string state*, because `}` inside a
string literal is not a terminator. That is O(payload) per framing attempt, and
on a partial message you either re-scan from the start every time or maintain a
resumable lexer. It also cannot carry arbitrary binary without base64, costing
33% bandwidth and a transform on every value.

**Protocol Buffers** are not self-delimiting at all — you must add your own
length prefix (`writeDelimitedTo`), at which point you have RESP's framing plus a
schema compiler and a code-generation step. For a five-command protocol, the
schema buys nothing and costs a build dependency. Protobuf earns its keep when
you have many evolving message types across teams.

RESP also has a property that is underrated: **it is human-readable enough to
debug with `nc`**. I kept inline command support specifically so `printf 'GET
k\n' | nc localhost 6379` works. That has real operational value at 3am.

The honest cost of RESP: no schema, no built-in versioning, and integers are
sent as ASCII text, which is slightly wasteful. For this workload none of those
bite.

</details>

### Q9. Ten thousand concurrent connections. What breaks first?

<details><summary><strong>Answer</strong></summary>

**In order:**

**1. The file descriptor limit.** Default `ulimit -n` is often 1024 — or 256 on
macOS. `accept()` starts returning `EMFILE` at that point. The code handles it
gracefully (refuses the connection, stays alive) rather than crashing, but the
capacity ceiling is hard. Fix: raise `nofile`, which the compose file already
does (65535 soft and hard).

**2. `poll`'s O(n) scan.** With 10,000 fds, every single iteration copies a
10,000-entry array into the kernel and the kernel scans all of it — even if one
socket is ready. At high connection counts with low per-connection activity this
dominates the CPU entirely. Fix: `epoll`/`kqueue`, which is O(ready). This is
confined to `EventLoop::run()`.

**3. Memory.** Each connection's `Buffer` grows to at least 16 KB on first read
(`kReadChunk`) and is never shrunk. 10,000 × 16 KB = 160 MB of read buffers
alone, plus output buffers. Fix: shrink idle buffers, or use a shared buffer pool
so idle connections hold nothing.

**4. The `pollfd` vector rebuild.** `run()` reconstructs the entire array every
iteration from the `unordered_map`. That is 10,000 hash-map iterations plus
allocations per loop. Fix: maintain the array incrementally, which `epoll`
removes the need for entirely.

**What does *not* break, and is worth saying:** the storage layer. Reads are
concurrent under the shared lock and connections are partitioned across threads
with no cross-thread contention on connection state. The bottleneck is squarely
the I/O multiplexing layer, which is exactly where I would spend the effort.

**And the honest ceiling:** with `fsync` on, write throughput is ~500/s
regardless of connection count. Ten thousand connections issuing writes would
queue on `wal_mutex_`. Group commit (Q5) is a prerequisite for that
configuration to make any sense.

</details>

### Q10. Is your server thread-safe? Prove it.

<details><summary><strong>Answer</strong></summary>

I will answer by enumerating **all** mutable state and showing each is either
thread-confined or lock-protected. That enumeration is the proof.

**Thread-confined (no synchronisation needed):**

- `Connection` — fd, `input_`, `output_`, `output_sent_`, `close_requested_`.
  Owned by exactly one `EventLoop`, touched only by that loop's thread. This is
  guaranteed by construction: `adopt()` transfers ownership into the loop's
  `connections_` map, and no other thread ever indexes it.
- `EventLoop::connections_`, its pollfd vectors, its wakeup fds.

**Lock-protected:**

- `Storage::map_` — `shared_mutex`. Readers shared, writers exclusive.
- `Wal::fd_`, `scratch_`, `payload_`, `bytes_written_` — `Wal::mutex_`.
- Mutation ordering — `Storage::wal_mutex_`.
- `EventLoop::pending_fds_` — `pending_mutex_`. This is the one genuine
  cross-thread handoff: loop 0's `accept_ready` pushes an fd that another loop
  pops.

**Atomics:**

- `EventLoop::running_`, `Server::running_`, `Server::next_loop_`.

**The cross-thread handoff deserves scrutiny** since it is the only place a
descriptor crosses threads. `adopt()` locks `pending_mutex_`, pushes, unlocks,
then writes a byte to the pipe. `drain_wakeup()` locks the same mutex and
`swap`s the vector out. The mutex provides the happens-before edge, so the
receiving thread sees a fully constructed value. The fd itself is just an int;
ownership transfers cleanly because loop 0 never touches it after the push.

**What I have *not* proven:** I have not run this under ThreadSanitizer. The
Apple Clang 15 TSan runtime fails during its own initialisation on this macOS
version — a trivial `int main(){return 0;}` segfaults identically — so the
verification I have is UBSan-clean plus a targeted test
(`log_order_matches_memory_order_under_contention`) that rebuilds the store from
the log after six threads contend on one key and asserts they agree. On Linux,
`-DKVSTORE_SANITIZE=ON` builds it and a TSan run is the check I would want
before claiming this in production.

*Being able to say precisely what you did and did not verify is worth more than
a confident "yes."*

</details>

### Q11. Suppose I told you `GET` latency spikes to 50 ms occasionally. How do you debug it?

<details><summary><strong>Answer</strong></summary>

**First, characterise before theorising.** Is it periodic or random? Correlated
with write load? Affecting all connections at once or one?

**The prime suspect, given this design: `purge_expired`.**

```cpp
std::size_t Storage::purge_expired() {
  std::unique_lock<std::shared_mutex> lock(mutex_);   // EXCLUSIVE
  const std::int64_t now = now_ms();
  for (auto it = map_.begin(); it != map_.end();) { ... }   // FULL SCAN
}
```

The maintenance thread takes the **exclusive** lock and scans the **entire map**
once per second. At a few million keys that scan is tens of milliseconds, during
which *every reader on every thread is blocked*. That produces exactly the
symptom: periodic (1 Hz), affecting all connections simultaneously, independent
of write load.

**How I would confirm it:** correlate the spike timestamps with the
`purge_interval_ms` period, or simply set `--purge-interval` very high and see if
the spikes vanish. Better, instrument `purge_expired` to record its own duration.

**The fix, which is what Redis does:** sample instead of scanning. Take a random
subset of, say, 20 keys; if more than 25% were expired, repeat; otherwise stop.
This bounds the work per pass to a constant, trading exact promptness for
bounded latency. Add a hard time budget per pass — a few hundred microseconds —
and release the lock between batches.

**Other candidates I would rule out:**

- **`fsync` contention** — but readers do not take `wal_mutex_`, so this would
  only show as general system slowness, not a lock stall. Distinguishable by
  checking whether spikes correlate with writes.
- **`Storage::size()`** — also a full scan under a shared lock. Shared, so it
  does not block other readers, but it blocks *writers* and burns CPU. If
  something calls it in a loop, that is a problem. It should maintain a counter
  instead.
- **Buffer reallocation** on a large value — one-off, not periodic.
- **Kernel/scheduler noise** — check whether it correlates with anything in the
  process at all.

</details>

### Q12. What is the worst code in this repository?

<details><summary><strong>Answer</strong></summary>

*(Answer this honestly. Refusing to name a weakness reads as either
inexperience or evasion.)*

**`EventLoop::run()`'s revents lookup.** After `poll` returns, it collects ready
fds, then for each one does a **linear scan of the entire pollfd vector** to find
the matching `revents`:

```cpp
for (const auto& pfd : pfds) {
  if (pfd.fd == fd) { revents = pfd.revents; break; }
}
```

That makes dispatch O(ready × total), on top of `poll`'s own O(total). With many
connections it is quadratic-ish and completely unnecessary — the index is already
known at the point the fd was collected. I wrote it defensively because handling
one connection can erase another, invalidating iterators, but the correct fix is
to store `(fd, revents)` pairs when collecting rather than re-searching. It is a
five-line change and I would make it first.

**Runner-up: the unbounded output buffer** (see Trace E). A client that pipelines
and never reads makes the server allocate without limit. The input side has
`kMaxInputBacklog`; the output side has nothing. Redis's client-output-buffer-
limit is the standard fix.

**Third: `handle_connection` can close a connection with unflushed output.** If
the peer half-closes (`shutdown(SHUT_WR)`) after pipelining a batch, and
`flush()` returns `blocked`, the `peer_closed` branch closes the connection with
replies still queued. A client doing a legitimate half-close would lose its
replies. The fix is to distinguish "peer closed its write side" from "peer is
gone" and keep flushing in the former case.

</details>

## 4.3 Re-architecture problems (15 min)

Sketch each for two minutes before reading the answer.

### P1. Add `INCR key` with correct concurrent semantics.

<details><summary><strong>Answer</strong></summary>

The trap is read-modify-write. `get` then `set` is **not** atomic — two
concurrent `INCR`s both read 5, both write 6, and one increment is lost.

It must be a single `Storage` method holding the locks across the whole
operation:

```cpp
std::optional<std::int64_t> Storage::incr(std::string_view key, std::int64_t by) {
  std::lock_guard<std::mutex> wal_lock(wal_mutex_);

  std::int64_t next = 0;
  {
    std::shared_lock<std::shared_mutex> peek(mutex_);
    auto it = map_.find(key);
    if (it != map_.end() && !it->second.expired(now_ms())) {
      if (!parse_int(it->second.value, next)) return std::nullopt;  // not an integer
    }
  }
  next += by;                                    // absent key starts at 0

  WalRecord record;
  record.op = WalOp::kSet;                       // log the RESULT, not the delta
  record.key.assign(key);
  record.value = std::to_string(next);
  record.expire_at_ms = /* preserve existing TTL */;
  if (wal_ != nullptr) wal_->append(record);

  std::unique_lock<std::shared_mutex> lock(mutex_);
  apply_locked(record);
  return next;
}
```

**The key design point: log the resulting value, not the increment.** A `kIncr`
opcode would make replay non-idempotent and order-sensitive in a way that
interacts badly with partial replay. Logging the absolute result keeps recovery a
straight re-execution.

`wal_mutex_` gives mutual exclusion against other mutators, so the read and the
write cannot interleave. Note the TTL must be preserved explicitly, since
`kSet` normally clears it.

</details>

### P2. Make `EXPIRE` precise to the millisecond and support `TTL key`.

<details><summary><strong>Answer</strong></summary>

The storage layer already stores `expire_at_ms`, so nothing changes on disk —
`PEXPIRE key <ms>` is a new command that skips the `* 1000`.

`TTL` is a read: shared lock, find, and return
`(expire_at_ms - now_ms() + 999) / 1000` rounded up, with Redis's conventions —
`-1` if the key exists with no TTL, `-2` if the key does not exist. Those two
sentinel values are the part people forget.

The subtlety worth mentioning: `TTL` must apply the same lazy-expiry check as
`GET`, or an expired-but-not-yet-purged key would report a negative TTL rather
than `-2`.

</details>

### P3. Support values larger than memory.

<details><summary><strong>Answer</strong></summary>

This changes the system from a cache into a storage engine, and the honest first
answer is that the current architecture cannot do it incrementally — the map
holds `std::string` values inline.

The design: the in-memory map holds **descriptors**, not values —
`{file_offset, length, crc}` — and values live in a separate append-only data
file. `GET` becomes a `pread()` at the offset. This is Bitcask's design, and it
is a good reference to name.

Consequences to raise unprompted:

- **Reads now hit the disk**, so the shared-lock read path must not hold the lock
  across the `pread`. Copy the descriptor under the lock, release, then read.
  That reintroduces a lifetime question: the value might be compacted away
  between the lookup and the read, which needs either refcounted file generations
  or a retry.
- **Compaction becomes mandatory**, not optional, since overwrites orphan old
  values.
- **The OS page cache does the caching**, which is usually the right answer —
  do not build your own buffer pool until you have proven the page cache is
  inadequate.

</details>

### P4. The maintenance thread is causing latency spikes. Redesign it.

<details><summary><strong>Answer</strong></summary>

Covered in Q11. Summary: replace the full scan under an exclusive lock with
Redis-style **adaptive random sampling** — sample 20 keys, evict expired ones, and
repeat only while the expired fraction exceeds ~25%. Add a hard per-pass time
budget and release the lock between batches so readers get windows.

This trades exact promptness (a key might linger a little past its deadline) for
bounded tail latency. Correctness is unaffected because the read path already
treats expired entries as absent — active expiry is purely a memory-reclamation
optimisation.

</details>

### P5. Add `MULTI`/`EXEC` transactions.

<details><summary><strong>Answer</strong></summary>

**Queue on the connection, execute under one lock acquisition.**

`MULTI` sets a flag on the `Connection`; subsequent commands are validated for
arity and queued into a per-connection vector rather than executed, each
answering `+QUEUED`. `EXEC` hands the whole batch to a new `Storage` method that
takes `wal_mutex_` **once**, appends all records, fsyncs **once**, then applies
all mutations under a single exclusive map lock.

This is strictly better than N separate commands in three ways: it is atomic
(no client sees a partial batch), it is one fsync instead of N — the same
insight as group commit — and recovery is atomic if the batch is written as one
record with a count prefix.

**What I would *not* claim:** this gives atomicity and durability, not
isolation in the serialisable sense. Reads inside the transaction happen at
`EXEC` time, so this matches Redis's model, not a database's. `WATCH` (optimistic
concurrency via key versioning) would be the next step, and requires a per-entry
version counter bumped on every mutation.

Connection state grows, so a queued-command limit is needed or `MULTI` becomes a
memory-exhaustion vector.

</details>

### P6. Port the event loop to `epoll`. What changes and what does not?

<details><summary><strong>Answer</strong></summary>

**Changes: only `EventLoop::run()` and the loop's setup/teardown.** That is the
point of the abstraction, and it is worth stating first.

- `epoll_create1(EPOLL_CLOEXEC)` once per loop, held for its lifetime.
- `epoll_ctl(ADD)` when a connection is adopted, `DEL` on close, `MOD` when the
  `POLLOUT` interest changes. This replaces rebuilding the array every iteration.
- `epoll_wait` returns only ready events, each carrying a `data.ptr` — so store
  the `Connection*` directly and the linear revents lookup (the worst code, per
  Q12) disappears entirely.

**Does not change:** `Buffer`, `Connection::fill/process/flush`, the parser,
`Storage`, `Wal`, dispatch. All of it is unaware of how readiness was discovered.

**The trap to raise:** if I also switch to **edge-triggered** (`EPOLLET`) for
performance, `fill()` and `flush()` *must* loop until `EAGAIN`, because the
kernel reports readiness only on transitions — miss it once and the connection
hangs forever. `Connection::fill` already loops to `EAGAIN`, so it is
edge-trigger-safe today. `flush` also loops correctly. But the `accept` path
would need auditing, and level-triggered is the safer default until profiling
justifies otherwise.

**Also worth noting:** `epoll` is per-thread here, so the thundering-herd
problem of multiple threads sharing one epoll fd does not arise. If it did,
`EPOLLEXCLUSIVE` is the answer.

</details>

## 4.4 Defend your choices (5 min)

Every row is a decision an interviewer may challenge. Know the *cost*, not just
the benefit.

| Decision | Why | What it costs | When you'd choose otherwise |
|---|---|---|---|
| `poll` over `epoll`/`kqueue` | POSIX; portable; simple to teach | O(n) per iteration | >1000 connections |
| Thread-per-event-loop, connection pinned | No per-connection locks | Uneven load if one connection is hot | Wildly skewed connection cost |
| `shared_mutex` on the map | Concurrent reads | Heavier than `mutex`; possible writer starvation | Write-dominated workload |
| Second `wal_mutex_` | Log order == memory order; fsync off the read path | Writers fully serialised; ordering discipline | Group commit supersedes it |
| fsync per write (default) | Zero acknowledged-write loss | ~482 writes/s | `everysec` for most real deployments |
| Absolute deadlines in the log | Survives restart correctly | Clock changes shift deadlines | Never — this one is not negotiable |
| Copy on `get` | No dangling views | One allocation per read | Refcounted values, if profiling demands |
| `string_view` args | Zero-copy parse | Strict lifetime contract | Never — the contract is enforceable |
| RESP subset | O(1) framing, binary-safe, debuggable | No schema/versioning | Many evolving message types → protobuf |
| Whole log read into memory at replay | Simple | Startup RAM ∝ log size | Large logs → stream it |
| Length + CRC per record | Detects torn and rotted records | 8 bytes overhead per record | Never |
| No log compaction | Simplicity | Unbounded growth | Any real deployment |

## 4.5 Final mastery checklist

Tick each only if you can explain it **without looking**.

**Operating system**
- [ ] What an fd is, and why leaking one is permanent
- [ ] Socket send/receive buffers; why `recv` ≠ network read
- [ ] The full socket lifecycle, and that `accept` yields a new fd
- [ ] `SO_REUSEADDR` and `TIME_WAIT`; `TCP_NODELAY` and Nagle
- [ ] Blocking vs non-blocking; `EAGAIN` is not an error
- [ ] `poll` is level-triggered, and both consequences
- [ ] `poll` O(n) vs `epoll` O(ready)
- [ ] The four storage layers; what each crash type loses
- [ ] `fsync` vs `F_FULLFSYNC` on macOS
- [ ] Async-signal-safety and the self-pipe trick
- [ ] Why `SIGPIPE` must be ignored

**Concurrency**
- [ ] `shared_mutex` semantics and cost
- [ ] Why two locks, and what each protects
- [ ] The lock-ordering rule and the deadlock argument
- [ ] The interleaving that breaks without `wal_mutex_`
- [ ] Which component skips `wal_mutex_` and why that is safe
- [ ] Why multi-key `DEL` is not atomic

**Durability**
- [ ] The record format from memory, byte by byte
- [ ] Why the payload is built before the header
- [ ] Torn tail vs bit rot: how each is detected
- [ ] Why replay stops rather than skips
- [ ] Why `ftruncate` is not optional
- [ ] Why deadlines are absolute and wall-clock
- [ ] The acknowledgement invariant, stated precisely

**C++**
- [ ] Where `string_view` is safe and where it dangles
- [ ] Why `get` returns by value
- [ ] `is_transparent` and heterogeneous lookup
- [ ] RAII on the failure path; why destructors must not throw
- [ ] Why resource owners delete their copy constructors

**Networking / protocol**
- [ ] TCP has no message boundaries — and five reasons why
- [ ] The three parse states and why `kIncomplete` consumes zero
- [ ] Why every wire length is bounds-checked
- [ ] The three responses to a partial `send`, and which two are wrong

**Weaknesses (you must be able to volunteer these)**
- [ ] Unbounded output buffer
- [ ] No log compaction
- [ ] O(ready × total) revents lookup
- [ ] Full-map scan in `purge_expired`
- [ ] No idle-connection timeout
- [ ] Not TSan-verified
- [ ] No auth, no TLS
- [ ] Half-close can drop queued replies

---

# APPENDICES

## A. Syscall reference

| Call | Returns | Watch for |
|---|---|---|
| `socket(AF_INET, SOCK_STREAM, 0)` | fd | — |
| `bind(fd, addr, len)` | 0 / -1 | `EADDRINUSE` without `SO_REUSEADDR` |
| `listen(fd, backlog)` | 0 / -1 | Backlog is a hint |
| `accept(fd, addr, &len)` | **new fd** | `EAGAIN`, `EMFILE`, `ECONNABORTED` |
| `recv(fd, buf, n, 0)` | >0 / 0 / -1 | 0 = FIN; -1 + `EAGAIN` = drained |
| `send(fd, buf, n, flags)` | bytes sent | **May be partial**; `EAGAIN`, `EPIPE` |
| `poll(pfds, n, ms)` | # ready | `EINTR`; level-triggered |
| `fcntl(fd, F_GETFL/F_SETFL)` | flags / 0 | Read-modify-write |
| `write(fd, buf, n)` | bytes written | **May be partial**; `EINTR` |
| `fsync(fd)` | 0 / -1 | macOS: use `F_FULLFSYNC` |
| `ftruncate(fd, len)` | 0 / -1 | Used to cut a torn tail |
| `pipe(fds)` | 0 / -1 | `fds[0]` read, `fds[1]` write |
| `getsockname(fd, addr, &len)` | 0 / -1 | Recovers a port-0 assignment |

## B. `errno` reference

| Code | Meaning | Handling here |
|---|---|---|
| `EAGAIN`/`EWOULDBLOCK` | Would block | Normal — stop reading / register `POLLOUT` |
| `EINTR` | Interrupted by a signal | Retry the call |
| `EPIPE` | Peer closed; we wrote | Close the connection |
| `ECONNRESET` | Peer reset | Close the connection |
| `EADDRINUSE` | Port taken | `SO_REUSEADDR`; else fatal |
| `EMFILE` | Process fd limit | Refuse the connection, stay alive |
| `ENFILE` | System fd limit | Same |
| `ECONNABORTED` | Died mid-handshake | Skip, continue accepting |

## C. RESP grammar (the implemented subset)

```
Request  := Array | Inline
Array    := '*' <count> CRLF Bulk{count}
Bulk     := '$' <len> CRLF <len bytes> CRLF
Inline   := <token> (SP <token>)* LF          (CR optional before LF)

Reply    := Simple | Error | Integer | Bulk | Null | ArrayReply
Simple   := '+' <text> CRLF                   +OK
Error    := '-' <text> CRLF                   -ERR unknown command 'X'
Integer  := ':' <number> CRLF                 :1
Bulk     := '$' <len> CRLF <bytes> CRLF       $5\r\nhello\r\n
Null     := '$' '-1' CRLF                     $-1
ArrayRep := '*' <count> CRLF Reply{count}     *0
```

| Command | Reply |
|---|---|
| `SET key value` | `+OK` |
| `GET key` | bulk, or `$-1` if absent/expired |
| `DEL key [key...]` | `:<count removed>` |
| `EXPIRE key seconds` | `:1` if existed, `:0` otherwise |
| `PING [msg]` | `+PONG`, or msg echoed as bulk |

## D. WAL format card

```
 record := header payload
 header := u32 payload_len  |  u32 crc32(payload)          (both little-endian)

 payload, by opcode:
   kSet    (0x01)  u8 op | u32 klen | key | u32 vlen | val | i64 expire_at_ms
   kDel    (0x02)  u8 op | u32 klen | key
   kExpire (0x03)  u8 op | u32 klen | key | i64 expire_at_ms

 expire_at_ms == 0  means "never expires"
 expire_at_ms is ABSOLUTE, milliseconds since the Unix epoch, WALL clock

 replay stops at: length past EOF (torn) | CRC mismatch (rot) | decode failure
 then ftruncate(good_offset)
```

## E. Numbers worth memorising

| Quantity | Value |
|---|---|
| `SET user:1 ikbal` on disk | 36 bytes (8 header + 28 payload) |
| SET, `F_FULLFSYNC`, 1 conn | 482 ops/s, 2.073 ms each |
| SET, `--no-fsync`, 1 conn | 13,735 ops/s, 0.073 ms each |
| SET, `--no-fsync`, 8 threads | 25,438 ops/s, p50 0.286 ms, p99 0.787 ms |
| GET, `--no-fsync`, 8 threads | 26,157 ops/s, p50 0.278 ms |
| SET, fsync, 8 threads | 370 ops/s, p50 23.9 ms, p99 34.5 ms |
| Durability tax on writes | ~28–69× depending on concurrency |
| Read chunk / max input backlog | 16 KB / 128 MB |
| Max bulk / args / inline | 64 MB / 1024 / 64 KB |
| Default I/O threads / purge interval | 4 / 1000 ms |
| Container image | 136 MB, non-root uid 999 |
| Clean `docker stop` | 0.27 s, exit 0 |
| Tests | 27 |

## F. Glossary

**Backpressure** — letting a slow consumer slow the producer instead of
buffering without limit.
**Bulk string** — RESP's length-prefixed binary-safe string.
**Edge-triggered** — readiness reported only on transition; requires draining to
`EAGAIN`.
**Group commit** — batching many log records into one fsync.
**Heterogeneous lookup** — searching a map with a type other than the key type,
avoiding a temporary.
**Level-triggered** — readiness reported as long as the condition holds.
**LSN** — log sequence number; the position of a record in the log.
**Nagle's algorithm** — TCP's small-packet coalescing delay.
**Page cache** — the kernel's in-memory copy of file data.
**Partial write** — `send`/`write` transferring fewer bytes than requested.
**RAII** — resource lifetime bound to object lifetime; cleanup on every exit path.
**Self-pipe trick** — turning a wakeup into a byte on a pipe so `poll` can see it.
**Small-string optimisation** — storing short strings inline, no heap.
**Thundering herd** — many threads woken for one event.
**TIME_WAIT** — the post-close state holding a port briefly.
**Torn write** — a partially written record left by a crash.
**Write-ahead logging** — recording an intent durably before applying it.

## G. The ten things most likely to trip you up

1. Saying `recv()` reads from the network. It reads from a kernel buffer.
2. Forgetting that `send()` can be partial. It is the single most common bug.
3. Claiming `fsync` guarantees durability on macOS. It does not; `F_FULLFSYNC`
   does.
4. Saying `--no-fsync` loses data on `kill -9`. It does not — the page cache is
   the kernel's.
5. Claiming reads are *unaffected* by fsync. They degrade ~3× from system-wide
   I/O pressure; what they avoid is *waiting on the flush inside the lock*.
6. Saying the `shared_mutex` alone guarantees log/memory order. It does not —
   that is `wal_mutex_`'s job.
7. Forgetting that `kIncomplete` must consume **zero** bytes.
8. Saying replay skips a bad record. It **stops** — the boundary is unknowable.
9. Forgetting `ftruncate`, and thus why the log would silently stop recording.
10. Not being able to name a weakness in your own design. Have three ready.

---

## Closing

If you can work through Hour 4 without looking, you know this system better than
most people know code they wrote themselves. The parts that will impress are not
the parts that work — everyone's project works. They are the parts where you can
say *"here is the failure mode I was preventing, here is what it costs me, and
here is what I would do differently at ten times the scale."*

Three things to have on your tongue at all times:

1. **The acknowledgement invariant.** An acknowledged operation always took
   effect; the reverse is impossible by construction.
2. **The two-lock argument.** One lock for structural integrity, one for
   cross-subsystem ordering, taken in a fixed order — and the fsync deliberately
   outside the readers' critical section.
3. **Three weaknesses, unprompted.** Unbounded output buffer, no compaction,
   O(ready × total) dispatch. Naming them first is what separates an engineer
   from a student.
