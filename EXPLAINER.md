# EXPLAINER — how this server actually works

This document assumes you can read C++ but have never written a network server,
never called `fsync`, and never had to think about what a file descriptor
really is. It explains the machine underneath the code, then the code, then
gives you five tracing exercises to check that it landed.

Nothing here is hand-waved. Every claim about the code points at a real
function you can open, and every claim about the operating system is something
you can verify with a terminal.

---

## Table of contents

1. [The byte journey, end to end](#1-the-byte-journey-end-to-end)
2. [First principles: the OS concepts you need](#2-first-principles-the-os-concepts-you-need)
   - [2.1 What a file descriptor really is](#21-what-a-file-descriptor-really-is)
   - [2.2 What a TCP socket is at the OS level](#22-what-a-tcp-socket-is-at-the-os-level)
   - [2.3 TCP stream framing: why your data arrives in pieces](#23-tcp-stream-framing-why-your-data-arrives-in-pieces)
   - [2.4 Write buffering vs. fsync: three places your data can hide](#24-write-buffering-vs-fsync-three-places-your-data-can-hide)
   - [2.5 Mutexes vs. readers-writer locks](#25-mutexes-vs-readers-writer-locks)
   - [2.6 Blocking, non-blocking, and the event loop](#26-blocking-non-blocking-and-the-event-loop)
3. [Code anatomy](#3-code-anatomy)
   - [3.1 `Buffer` — two cursors and one rule](#31-buffer--two-cursors-and-one-rule)
   - [3.2 `protocol.cpp` — the incremental parser](#32-protocolcpp--the-incremental-parser)
   - [3.3 `string_view` — where zero-copy is safe and where it is a bug](#33-string_view--where-zero-copy-is-safe-and-where-it-is-a-bug)
   - [3.4 `Storage` — the two-lock write path](#34-storage--the-two-lock-write-path)
   - [3.5 `Wal` — the record format, byte by byte](#35-wal--the-record-format-byte-by-byte)
   - [3.6 `EventLoop` — poll, the self-pipe, and partial writes](#36-eventloop--poll-the-self-pipe-and-partial-writes)
   - [3.7 RAII — the destructors doing the cleanup you never wrote](#37-raii--the-destructors-doing-the-cleanup-you-never-wrote)
4. [Crash recovery, step by step](#4-crash-recovery-step-by-step)
5. [Hands-on verification: five tracing questions](#5-hands-on-verification-five-tracing-questions)
6. [What this server does not do](#6-what-this-server-does-not-do)

---

## 1. The byte journey, end to end

One `SET user:1 ikbal` from a client, followed all the way down and back.

```
  CLIENT PROCESS                                        SERVER PROCESS
 ┌──────────────┐                                     ┌───────────────────────┐
 │ write(fd,    │                                     │                       │
 │  "*3\r\n$3   │                                     │                       │
 │  \r\nSET...")│                                     │                       │
 └──────┬───────┘                                     └───────────────────────┘
        │
        v
 ╔══════════════════════════════ KERNEL ══════════════════════════════════════╗
 ║  client socket send buffer ──> TCP/IP stack ──> loopback ──> TCP/IP stack   ║
 ║                                                        │                   ║
 ║                                     server socket receive buffer  <────────╢
 ║                                                        │                   ║
 ║   The kernel may deliver these bytes as ONE chunk, or as 4 chunks of        ║
 ║   arbitrary size, at arbitrary times. It guarantees ORDER, never SHAPE.     ║
 ╚════════════════════════════════════════════╤═══════════════════════════════╝
                                              │
                          poll() reports fd 7 │ POLLIN  ("readable now")
                                              v
 ┌────────────────────────────────────────────────────────────────────────────┐
 │ (1) FILE DESCRIPTOR         fd 7  — an int indexing this process's open     │
 │                                     file table; the handle to that socket   │
 └────────────────────────────────────────────┬───────────────────────────────┘
                                              v
 ┌────────────────────────────────────────────────────────────────────────────┐
 │ (2) MEMORY READ BUFFER      Connection::fill()  →  recv() in a loop until   │
 │     server.cpp                                     EAGAIN                   │
 │                                                                            │
 │     Buffer:  [ consumed | UNPARSED BYTES | free space ]                    │
 │                         ^read_pos       ^write_pos                         │
 │     Bytes accumulate here across as many TCP segments as it takes.          │
 └────────────────────────────────────────────┬───────────────────────────────┘
                                              v
 ┌────────────────────────────────────────────────────────────────────────────┐
 │ (3) RESP WIRE PARSER        parse_command(buffer.readable(), cmd)           │
 │     protocol.cpp                                                           │
 │            ├─ kIncomplete → stop, consume nothing, wait for more bytes      │
 │            ├─ kError      → queue "-ERR ...", close the connection          │
 │            └─ kOk         → cmd.args = ["SET","user:1","ikbal"]             │
 │                             as string_views POINTING INTO the buffer.       │
 │                             No copy. No allocation. No memcpy of the value. │
 └────────────────────────────────────────────┬───────────────────────────────┘
                                              v
 ┌────────────────────────────────────────────────────────────────────────────┐
 │ (4) DISPATCH                execute_command()  → storage.set(k, v)          │
 │     server.cpp              (the only layer that knows both protocol        │
 │                              and storage exist)                             │
 └────────────────────────────────────────────┬───────────────────────────────┘
                                              v
 ┌────────────────────────────────────────────────────────────────────────────┐
 │ (5) THREAD LOCK             lock_guard(wal_mutex_)   ← mutators queue here  │
 │     storage.cpp             Readers are NOT blocked by this.                │
 └────────────────────────────────────────────┬───────────────────────────────┘
                                              v
 ┌────────────────────────────────────────────────────────────────────────────┐
 │ (6) DISK SYNC (WAL)         Wal::append()                                   │
 │     wal.cpp                   encode → write(fd) → fsync/F_FULLFSYNC        │
 │                                                                            │
 │            ┌── the record is DURABLE the instant this returns ──┐          │
 │            │  user space buffer → kernel page cache → drive     │          │
 │            │  cache → PLATTER/FLASH                             │          │
 │            └────────────────────────────────────────────────────┘          │
 │            THIS HAPPENS BEFORE MEMORY IS TOUCHED. That is the "write        │
 │            ahead" in write-ahead log.                                       │
 └────────────────────────────────────────────┬───────────────────────────────┘
                                              v
 ┌────────────────────────────────────────────────────────────────────────────┐
 │ (7) STORAGE WRITE           unique_lock(shared_mutex)                       │
 │     storage.cpp             map_["user:1"] = {"ikbal", 0}                   │
 │                             unlock  ← held for nanoseconds, not for the     │
 │                                       milliseconds the fsync took           │
 └────────────────────────────────────────────┬───────────────────────────────┘
                                              v
 ┌────────────────────────────────────────────────────────────────────────────┐
 │ (8) RESPONSE STREAM         reply_simple(out, "OK")  → out += "+OK\r\n"     │
 │                             Connection::flush() → send()                    │
 │                                                                            │
 │             send() took everything?  → done, clear the buffer               │
 │             send() took only part?   → keep the tail, ask poll() for        │
 │                                        POLLOUT, finish it next wakeup       │
 └────────────────────────────────────────────┬───────────────────────────────┘
                                              v
 ╔═══════════════════ KERNEL: socket send buffer → TCP → client ══════════════╗
 ╚════════════════════════════════════════════════════════════════════════════╝
```

The two things worth memorising from that diagram:

- **Step 6 happens before step 7, always.** Memory never gets ahead of the log.
- **The client is told `+OK` in step 8, after step 6.** The server never
  acknowledges anything it has not already made durable.

---

## 2. First principles: the OS concepts you need

### 2.1 What a file descriptor really is

A file descriptor is a **small non-negative integer**. That is the whole type.
`int fd = 7;`

It is an index into a per-process array the kernel maintains, called the file
descriptor table. Entry 7 in *your* process's table points at a kernel object.
Entry 7 in some other process's table points at a completely different one.
The number is meaningless outside the process that owns it.

```
   YOUR PROCESS                    KERNEL
 ┌────────────────┐        ┌──────────────────────────┐
 │ fd 0  stdin    │───────>│ terminal                 │
 │ fd 1  stdout   │───────>│ terminal                 │
 │ fd 2  stderr   │───────>│ terminal                 │
 │ fd 3  wal.log  │───────>│ file: inode, offset 143  │
 │ fd 4  listener │───────>│ socket: LISTEN :6379     │
 │ fd 7  client   │───────>│ socket: ESTABLISHED,     │
 │                │        │   recv buffer, send buf  │
 └────────────────┘        └──────────────────────────┘
```

Three consequences that drive real design decisions in this codebase:

1. **The kernel object holds the state, not your process.** The read/write
   offset of `wal.log` lives in the kernel, which is why `O_APPEND` can make
   "seek to end, then write" atomic — both halves happen kernel-side, under its
   own locking, with no window for another writer to slip in.

2. **Descriptors are a finite, leakable resource.** Every process has a limit
   (`ulimit -n`, often 256 or 1024 by default on macOS). Leak one per
   connection and the server dies after N clients with `EMFILE`. There is no
   garbage collector for these. This is why `Connection`'s destructor calls
   `close()` and why `Connection` is non-copyable — two `Connection` objects
   holding the same `int` would each close it, and the second `close()` would
   shut down a descriptor the kernel has since handed to somebody else.

3. **Everything is one.** Files, sockets, pipes, terminals. `read()` and
   `write()` work on all of them. That uniformity is exactly what makes the
   self-pipe trick in `EventLoop` possible: `poll()` cannot wait on "a flag in
   memory changed", but it can wait on a pipe, so we turn the flag change into
   one byte written to a pipe.

Verify it yourself, on the running server:

```bash
lsof -nP -p $(pgrep kvstore) | head
```

### 2.2 What a TCP socket is at the OS level

A socket is a kernel object with **two queues attached**: a receive buffer and
a send buffer, each typically 64 KB–2 MB.

```
      your code                    KERNEL SOCKET (fd 7)
                            ┌────────────────────────────────┐
   recv(fd,...)  <───────── │ receive buffer  [bytes......]  │ <── network
                            │                                │
   send(fd,...)  ─────────> │ send buffer     [bytes......]  │ ──> network
                            └────────────────────────────────┘
```

`recv()` **does not touch the network.** It copies bytes out of the receive
buffer that the network card and TCP stack already deposited there,
asynchronously, before you asked. `send()` **does not transmit.** It copies
bytes into the send buffer and returns; the kernel transmits them on its own
schedule, retransmits what is lost, and waits for acknowledgements you never
see.

This is the source of the two facts that shape all network code:

- `recv()` returning 4 bytes does not mean the peer sent 4 bytes.
  It means 4 bytes were *in the buffer at that instant*.
- `send()` returning 200 does not mean 200 bytes reached the peer.
  It means 200 bytes fit in the send buffer. If the buffer was nearly full,
  `send()` takes what fits and returns a smaller number — a **partial write** —
  and you are responsible for the rest.

The lifecycle a server drives:

```
  socket()      create the object, get an fd
  setsockopt()  configure it (SO_REUSEADDR, TCP_NODELAY)
  bind()        claim an address:port
  listen()      start completing TCP handshakes into a backlog queue
  accept()      pop one completed connection, get a NEW fd for it
  recv()/send() move bytes
  close()       release
```

`accept()` returning a *new* descriptor is the part that surprises people: the
listening socket is never read from or written to. It is a factory. Each
`accept()` hands you a separate, fully independent connection.

Two options this server sets and why:

- **`SO_REUSEADDR`** — after a socket closes, the kernel keeps the port in
  `TIME_WAIT` for up to a couple of minutes to catch stray packets from the old
  connection. Without this option, restarting the server fails with
  `EADDRINUSE` during that window. Every server sets it.
- **`TCP_NODELAY`** — disables Nagle's algorithm, which holds small outgoing
  packets briefly hoping to merge them with the next one. For a store whose
  reply *is* the whole message, there is never a next one to merge with, so
  Nagle is pure added latency.

### 2.3 TCP stream framing: why your data arrives in pieces

**TCP is a byte stream, not a message service.** This is the single most
important sentence in network programming, and the one beginners disbelieve
until it bites them in production.

TCP guarantees exactly two things: every byte you send arrives, and they arrive
**in order**. It guarantees nothing whatsoever about *grouping*. The boundaries
between your `send()` calls are not transmitted. They do not exist on the wire.

A client calls `send()` once with 34 bytes:

```
*3\r\n$3\r\nSET\r\n$6\r\nuser:1\r\n$5\r\nikbal\r\n
```

Your server's `recv()` may legitimately observe **any** of these:

```
one segment:     [*3\r\n$3\r\nSET\r\n$6\r\nuser:1\r\n$5\r\nikbal\r\n]

split anywhere:  [*3\r\n$3\r\nSET\r\n$6\r\nuse]  [r:1\r\n$5\r\nikbal\r\n]

one byte at a time: [*] [3] [\r] [\n] [$] [3] ... [\r] [\n]

coalesced with the NEXT command (pipelining):
   [*3\r\n$3\r\nSET\r\n$6\r\nuser:1\r\n$5\r\nikbal\r\n*1\r\n$4\r\nPING\r\n]
```

Why it splits: the path MTU (~1500 bytes on Ethernet, so a 3 KB value is *always*
at least two segments), TCP window and congestion control, the sender's socket
buffer filling, Nagle coalescing, and the receiving application simply calling
`recv()` at a moment when only half arrived.

This means **the code that says "read a command from the socket" is a lie you
cannot write**. There is no such operation. What you can write is:

> Accumulate bytes in a buffer. After each read, ask: *does the buffer now
> begin with a complete message?* If yes, take it and repeat. If no, keep the
> bytes and go back to waiting.

That is exactly `Connection::fill()` + `Connection::process()`, and it is why
`parse_command` has a `kIncomplete` status that consumes **zero** bytes. The
protocol also has to make "is it complete?" answerable, which is why RESP
length-prefixes every bulk string: the parser reads `$5\r\n` and then knows
precisely how many bytes to wait for, instead of scanning for a delimiter that
might legally appear inside the data.

The test `partial_input_is_incomplete_until_the_last_byte` asserts this
property at its harshest: it feeds every prefix of a command, one byte longer
each time, and requires `kIncomplete` for all of them until the final byte
arrives. `server_reassembles_a_command_split_across_packets` does the same
thing over a real socket with real delays.

### 2.4 Write buffering vs. fsync: three places your data can hide

When `write(fd, data, n)` returns successfully, **your data is not on the
disk**. It is not even necessarily on its way. There are three layers of
volatile storage between your variable and the physical medium:

```
 ┌──────────────────────────────────────────────────────────────────┐
 │ 1. USER-SPACE BUFFER      e.g. std::ofstream, FILE*, std::cout    │
 │    Lost on: process crash                    (VOLATILE)           │
 │    Escaped by: fflush(), or not using buffered I/O at all         │
 ├──────────────────────────────────────────────────────────────────┤
 │            write(2) syscall crosses into the kernel here          │
 ├──────────────────────────────────────────────────────────────────┤
 │ 2. KERNEL PAGE CACHE      the OS's copy, shared by all processes  │
 │    Lost on: power cut, kernel panic          (VOLATILE)           │
 │    Survives: your process being SIGKILLed — the kernel is still   │
 │              alive and will write it out eventually               │
 │    Escaped by: fsync()                                            │
 ├──────────────────────────────────────────────────────────────────┤
 │ 3. DRIVE WRITE CACHE      DRAM on the SSD/HDD itself              │
 │    Lost on: power cut                        (VOLATILE)           │
 │    Escaped by: fcntl(F_FULLFSYNC) on macOS;                       │
 │                fsync() + barriers on Linux                        │
 ├──────────────────────────────────────────────────────────────────┤
 │ 4. THE ACTUAL FLASH / PLATTER                (PERSISTENT)         │
 └──────────────────────────────────────────────────────────────────┘
```

This layering exists for speed, and the speed difference is enormous — roughly
100 nanoseconds to reach the page cache, versus a millisecond or more to reach
flash. A store doing an `fsync` per write is therefore limited to a few
thousand writes per second no matter how fast its hash map is. That is the
price of durability, and it is why `SyncPolicy` exists as a knob rather than a
hard-coded choice.

**The macOS detail that catches people out.** On macOS, `fsync()` only pushes
data out of the kernel page cache to the *drive*. The drive is then free to sit
on it in its own volatile DRAM cache and report success. Only
`fcntl(fd, F_FULLFSYNC)` instructs the drive to flush that cache to the medium.
This is why `Wal::append` does:

```cpp
#if defined(__APPLE__)
    if (::fcntl(fd_, F_FULLFSYNC) < 0) {
      if (::fsync(fd_) < 0) { throw ...; }   // fall back if unsupported
    }
#else
    if (::fsync(fd_) < 0) { throw ...; }
#endif
```

A benchmark that uses plain `fsync()` on a Mac and calls itself crash-safe is
measuring the wrong thing by a factor of ten or more.

**Where the flush sits matters as much as whether it happens.** It is inside
`Wal::append`, before the function returns, and `Storage::set` calls
`wal_->append(...)` before it touches the map. Returning from `append()` is the
exact moment the caller is entitled to believe the record survives a crash. Move
the flush after the map update and the whole guarantee collapses.

### 2.5 Mutexes vs. readers-writer locks

A **mutex** (`std::mutex`) admits exactly one thread at a time. Everyone else
waits.

```
  time ──────────────────────────────────────────────────>
  R1  [==read==]
  R2             [==read==]
  R3                        [==read==]
  W1                                   [=write=]
```

Three readers that could not possibly interfere with each other ran one after
another for no reason. For a cache with a 95%-read workload, this throws away
most of your machine.

A **readers-writer lock** (`std::shared_mutex`) has two modes:

- `std::shared_lock` — **shared/read mode.** Any number of threads at once.
- `std::unique_lock` — **exclusive/write mode.** One thread, and no readers.

```
  time ──────────────────────────────────────────────────>
  R1  [==read==]
  R2  [==read==]        ← genuinely simultaneous, on different cores
  R3  [==read==]
  W1               [=write=]     ← waits for readers to drain, then exclusive
  R4                        [==read==]
```

Why concurrent reads are safe: reading does not change anything. Two threads
walking the same hash bucket observe identical bytes and neither disturbs the
other. Reads only become dangerous when a *writer* is present, because a writer
can rehash the table, invalidate iterators, or reallocate a `std::string`'s heap
buffer while a reader is halfway through copying it. The exclusive mode exists
precisely to guarantee no reader is inside the map while that happens.

The cost of a `shared_mutex` is that it is a heavier object than a plain mutex:
it tracks a reader count, so even a read does an atomic increment and
decrement, and writers can be starved by a continuous stream of readers under
some implementations. For a read-dominated workload it wins anyway, and this is
exactly a read-dominated workload.

**Why this server also has a second, plain mutex.** `Storage` holds both:

```cpp
std::mutex        wal_mutex_;   // serialises MUTATORS against each other
std::shared_mutex mutex_;       // guards the MAP
```

The naive design uses only the `shared_mutex` and holds it exclusively across
the log write and the map update. That is correct, but it blocks every reader
for the whole duration of an `fsync` — a millisecond of every reader stalled on
a disk they are not using.

The two-lock version fixes that:

```cpp
void Storage::set(std::string_view key, std::string_view value) {
  ...
  std::lock_guard<std::mutex> wal_lock(wal_mutex_);      // (1) mutators queue
  if (wal_ != nullptr) wal_->append(record);             // (2) slow: disk
  std::unique_lock<std::shared_mutex> lock(mutex_);      // (3) fast: memory
  apply_locked(record);
}
```

Readers only ever contend for `mutex_`, which is held for the nanoseconds of a
hash-map assignment. The millisecond-scale disk wait happens under
`wal_mutex_`, which no reader ever touches.

And it preserves the property that makes the log trustworthy: because
`wal_mutex_` is held across **both** the append and the map update, two
concurrent writers can never append in one order and apply in the other. Log
order and memory order are the same order, so replaying the log reproduces the
exact state the server was serving. The test
`log_order_matches_memory_order_under_contention` asserts this by hammering one
key from six threads and then checking that a fresh store rebuilt from the log
agrees with the live one.

**Deadlock freedom** is by lock ordering: mutators always take `wal_mutex_`
then `mutex_`; readers take only `mutex_`. No thread ever takes them in the
opposite order, so there is no cycle and therefore no deadlock. Any future
method that needs both must follow the same order — that rule is the entire
discipline.

### 2.6 Blocking, non-blocking, and the event loop

A **blocking** `recv()` puts your thread to sleep until data arrives. It costs
nothing while waiting, but that thread can do nothing else. One thread per
connection is simple and works fine to a few hundred clients; at ten thousand
you are paying ~8 MB of stack each and drowning the scheduler in context
switches.

A **non-blocking** descriptor (`fcntl(fd, F_SETFL, flags | O_NONBLOCK)`) never
sleeps. If there is nothing to read, `recv()` returns `-1` immediately and sets
`errno` to `EAGAIN`. That is not an error — it is the descriptor saying "empty,
ask again later."

Which raises the obvious problem: if nothing blocks, what stops the program
spinning at 100% CPU asking every socket "anything yet?" The answer is one
syscall that blocks on **all of them at once**:

```c
poll(pfds, n, timeout);   // "sleep until ANY of these n fds is ready"
```

You hand the kernel an array of descriptors and the events you care about
(`POLLIN` = readable, `POLLOUT` = writable). The kernel sleeps your thread and
wakes it when at least one is ready, filling in `revents` to say which. One
thread, thousands of connections, zero busy-waiting.

```
   ┌──────────────────────────────────────────┐
   │  build the pollfd array from:            │
   │    - the wakeup pipe   (always POLLIN)   │
   │    - the listener      (POLLIN = a new   │
   │                         client is queued)│
   │    - each connection   (POLLIN, plus     │
   │                         POLLOUT only if  │
   │                         we owe it bytes) │
   └────────────────┬─────────────────────────┘
                    v
              poll(...)  ← thread sleeps here, 0% CPU
                    │ kernel wakes us: "fds 4 and 9 are ready"
                    v
   ┌──────────────────────────────────────────┐
   │  listener readable → accept() in a loop  │
   │                      until EAGAIN        │
   │  client readable    → fill(), process(), │
   │                       flush()            │
   │  client writable    → flush() the        │
   │                       leftover tail      │
   └────────────────┬─────────────────────────┘
                    └──────> loop
```

Note the `POLLOUT`-only-when-needed detail in `EventLoop::run()`:

```cpp
short events = POLLIN;
if (conn->has_pending_output()) events = static_cast<short>(events | POLLOUT);
```

An idle socket is almost always writable. Asking for `POLLOUT` unconditionally
means `poll()` returns instantly, every time, forever — a busy loop that looks
like an event loop. Only ask when you actually have bytes waiting.

**Why `poll` here and not `epoll`/`kqueue`.** `epoll` is Linux-only; `kqueue`
is BSD/macOS-only. `poll` is POSIX and runs on both, which is why it is the
choice for a codebase meant to be read and built anywhere. The honest cost:
`poll` passes the entire interest list across the syscall boundary on every
call and the kernel scans all of it, so each iteration is **O(total
connections)**. `epoll`/`kqueue` register the interest set once inside the
kernel and return only what is ready — **O(ready connections)**. At 50
connections the difference is noise; at 50,000 it is the whole ballgame.
Swapping the backend means rewriting `EventLoop::run()` and nothing else,
because nothing above it knows how readiness is discovered.

**Threading model.** This server runs N event loops, one per thread. Loop 0
owns the listener; when it accepts, it hands the new descriptor to the next
loop round-robin and pokes it awake. A connection then belongs to exactly one
thread for its entire life, so its buffers and parse state need **no locks at
all**. The only shared mutable state in the process is `Storage`, and that is
where every lock in the codebase lives.

---

## 3. Code anatomy

### 3.1 `Buffer` — two cursors and one rule

`include/kvstore/server.hpp`. A `std::vector<char>` plus two indices:

```
   data_:  [ o l d   d a t a │ u n p a r s e d │ free space........ ]
                             ^read_pos_        ^write_pos_
```

- `readable()` → `{data_.data() + read_pos_, write_pos_ - read_pos_}` — the
  bytes that have arrived but not yet been parsed.
- `consume(n)` → `read_pos_ += n`. **Moves a cursor. Never moves bytes.**
- `ensure_writable(n)` → may `compact()` (memmove live bytes to offset 0) and
  may `resize()` (reallocate).

Why two cursors instead of `erase()`ing from the front: erasing from the front
of a vector is O(size) *and*, far worse here, it moves the bytes that live
`string_view`s are pointing at. `consume()` has to be O(1) and
address-stable, so reclaiming space is deferred to `ensure_writable`, which
only ever runs when no views are outstanding.

That gives the one rule this class lives by:

> **`ensure_writable()` and `append()` invalidate every view previously
> returned by `readable()`. `consume()` does not.**

`Connection::fill()` and `Connection::process()` are written around it: `fill`
does all its `ensure_writable` calls *first*, draining the socket completely,
and only then does `process` parse from a buffer whose storage is now frozen
for the duration.

`consume()` also resets both cursors to zero when the buffer empties, so a
steady request/response workload reuses offset 0 forever and never grows.

### 3.2 `protocol.cpp` — the incremental parser

The signature is the design:

```cpp
ParseResult parse_command(std::string_view input, Command& out);
```

It takes a **view of bytes** and knows nothing about sockets. That is why the
protocol tests never open a descriptor — they call a pure function on a string
literal.

Three outcomes, and the discipline is in the second one:

| status | meaning | `consumed` |
|---|---|---|
| `kOk` | one whole command parsed | how many bytes to retire |
| `kIncomplete` | not all here yet | **0 — nothing is retired** |
| `kError` | malformed; close the connection | 0 |

`kIncomplete` consuming zero bytes is what makes the parser restartable: the
caller leaves the bytes exactly where they were, more arrive, and it calls the
same function again on the longer buffer. There is no saved state machine, no
half-parsed command stashed somewhere. **The buffer is the state.** That is
both simpler and much harder to get wrong than an explicit resumable parser.

Walking the array path:

```cpp
const std::size_t header_end = input.find(kCrlf);
if (header_end == std::string_view::npos) {
  if (input.size() > 32) return failure("Protocol error: invalid multibulk length");
  return incomplete();
}
```

No `\r\n` yet, so we cannot know the argument count. But we do not wait
forever: a client that streams digits with no terminator would otherwise grow
our buffer without bound. Thirty-two bytes is far more than any legitimate
count needs, so past that it is not a slow client, it is an attack.

```cpp
const std::size_t need = body + static_cast<std::size_t>(length) + kCrlf.size();
if (input.size() < need) return incomplete();
out.args.emplace_back(input.substr(body, static_cast<std::size_t>(length)));
```

The declared length is known but the bytes are not all here — return
`kIncomplete`, consume nothing, try again after the next `recv`. When they *are*
all here, `substr` produces a view: **no allocation, no copy, regardless of
whether the value is 3 bytes or 3 megabytes.**

Every length that comes off the wire is checked against a cap before it is
used:

```cpp
if (length < 0 || static_cast<std::size_t>(length) > kMaxBulkLength)
  return failure("Protocol error: invalid bulk length");
```

Without that, `$4294967295\r\n` is a remote out-of-memory kill delivered in
thirteen bytes. The parser is the first thing an unauthenticated stranger
talks to, so every number it reads is hostile until bounded. The test
`hostile_lengths_are_rejected` covers exactly these.

### 3.3 `string_view` — where zero-copy is safe and where it is a bug

A `std::string_view` is a pointer and a length. It owns nothing, allocates
nothing, and frees nothing. It is a **borrowed** view of memory somebody else
owns — which makes it the sharpest tool in the box and the easiest one to cut
yourself on.

**Where it is right — the parser.** `Command::args` are views into the
connection's read buffer:

```
 Buffer: [ *3\r\n$3\r\nSET\r\n$6\r\nuser:1\r\n$5\r\nikbal\r\n ]
                        └─┬─┘       └──┬───┘      └──┬──┘
 args[0] ────────────────┘             │            │
 args[1] ──────────────────────────────┘            │
 args[2] ───────────────────────────────────────────┘
```

Parsing a command allocates **nothing** for the payload. A 1 MB `SET` value is
never copied into an argument object; it is copied exactly once, in
`Storage::set`, into the map entry where it actually needs to live. The lifetime
is safe because the buffer provably outlives the `Command`: both are local to
one `process()` call on one thread, and `process()` never touches the input
buffer's storage while a `Command` is alive.

**Where it would be a bug — the read path.** `Storage::get` returns
`std::optional<std::string>` — a copy — and that is deliberate:

```cpp
std::optional<std::string> Storage::get(std::string_view key) const {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  auto it = map_.find(key);
  if (it == map_.end()) return std::nullopt;
  if (it->second.expired(now_ms())) return std::nullopt;
  return it->second.value;      // copied out, under the lock
}
```

Returning `std::string_view{it->second.value}` would compile, would pass a
single-threaded test, and would be a use-after-free in production. The moment
that shared lock is released, another thread may `SET` the key — reallocating
the string's heap buffer — or `DEL` it, destroying it outright. The caller would
be reading freed memory. **The copy is not a missed optimisation; it is the
correctness boundary.** A view is only safe when you can name the thing that
keeps its memory alive and prove it outlives the view. Here, nothing does.

The **key** parameter is still a view, because it is only read during the call
while the caller demonstrably holds it alive. That is also what
`StringHash::is_transparent` buys us:

```cpp
struct StringHash {
  using is_transparent = void;
  std::size_t operator()(std::string_view key) const noexcept { ... }
};
using Map = std::unordered_map<std::string, Entry, StringHash, std::equal_to<>>;
```

Without the transparent hasher, `map_.find(some_string_view)` would construct a
temporary `std::string` — a heap allocation and a copy — on **every single
GET**, purely to throw it away after the lookup. With it, the view is hashed and
compared directly. Note that heterogeneous `erase()` is a C++23 feature, so the
delete path deliberately does `find()` then `erase(iterator)`.

### 3.4 `Storage` — the two-lock write path

```cpp
void Storage::set(std::string_view key, std::string_view value) {
  WalRecord record;
  record.op = WalOp::kSet;
  record.key.assign(key);
  record.value.assign(value);
  record.expire_at_ms = 0;                            // a plain SET clears any TTL

  std::lock_guard<std::mutex> wal_lock(wal_mutex_);   // 1
  if (wal_ != nullptr) wal_->append(record);          // 2
  std::unique_lock<std::shared_mutex> lock(mutex_);   // 3
  apply_locked(record);
}
```

1. **`wal_mutex_`** — only mutators. Pins log order to memory order (§2.5).
2. **Durability first.** If this throws, the map is untouched, the exception
   propagates to the REPL boundary, and the client gets an error rather than a
   false `+OK`. There is no state where the client was told the write succeeded
   but the record is missing.
3. **`mutex_` exclusively**, held for a hash-map assignment and nothing else.

Both locks are RAII guards, which matters most on the failure path: if
`append()` throws, `wal_lock`'s destructor releases `wal_mutex_` during stack
unwinding. Hand-written `lock()`/`unlock()` pairs would leave the mutex held
forever and hang every subsequent writer — a deadlock caused by an unrelated
disk error.

**Expiry is stored as an absolute wall-clock deadline**, never a relative TTL:

```cpp
record.expire_at_ms = now_ms() + ttl_seconds * 1000;
```

Two reasons, both load-bearing:

- **Relative TTLs cannot be logged.** Replaying "expire in 60 seconds" three
  days after a crash resurrects a key that should be long dead. Replaying
  "expire at 1755271234567" is correct at any future moment.
- **It must be the wall clock, not `steady_clock`.** `steady_clock`'s origin is
  unspecified and typically resets when the machine reboots, so a persisted
  `steady_clock` deadline is meaningless on the next boot. The usual advice —
  "never use the wall clock for timeouts, it can jump" — is right for
  *in-memory* durations and exactly wrong for *persisted* deadlines.

`get()` treats an expired entry as absent without deleting it, because deletion
would need an exclusive lock on a `const`, shared-locked path. The memory is
reclaimed by `purge_expired()`, run once a second by the server's maintenance
thread — otherwise a key written with a TTL and never read again would sit in
the map forever. Those evictions are deliberately **not** logged: replay
re-checks every deadline against the current clock, so the log stays a correct
description of the state without them.

### 3.5 `Wal` — the record format, byte by byte

```
 ┌────────────────┬────────────────┬──────────────────────────────┐
 │ payload_len u32│ crc32 u32      │ payload (payload_len bytes)  │
 └────────────────┴────────────────┴──────────────────────────────┘
   little-endian    little-endian     op | key | (op-specific)
```

Here is a real record, taken with `xxd` from a live `wal.log` after
`SET user:1 ikbal`:

```
00000000: 1c00 0000 b559 10e5 0106 0000 0075 7365  .....Y.......use
00000010: 723a 3105 0000 0069 6b62 616c 0000 0000  r:1....ikbal....
00000020: 0000 0000                                ....
            │
  1c000000  → payload_len = 0x1C = 28 bytes
  b55910e5  → crc32 of those 28 bytes
  01        → opcode kSet
  06000000  → key length 6
  user:1    → the key
  05000000  → value length 5
  ikbal     → the value
  0000000000000000 → expire_at_ms = 0 (never expires)
```

Design points worth naming:

**Integers are written one byte at a time**, never `memcpy`'d from a struct:

```cpp
void put_u32(std::vector<char>& out, std::uint32_t value) {
  out.push_back(static_cast<char>(value & 0xFFu));
  out.push_back(static_cast<char>((value >> 8) & 0xFFu));
  ...
}
```

`memcpy`ing a struct into a file bakes in that machine's byte order *and* its
struct padding. The file outlives the machine — it gets copied to a different
box, or read by a build with different alignment settings — so the format has to
be defined in bytes, not in whatever the compiler happened to lay out.

**Length first, then CRC, then payload.** The length lets the reader find the
next record boundary without understanding the payload. The CRC covers the
payload, so a bit flip is detected rather than silently applied.

**`O_APPEND` is a correctness property, not a convenience.** It makes
"seek to end, then write" a single atomic kernel operation, so two writers can
never compute the same offset and shred each other's records.

**Every `write()` is a loop:**

```cpp
void write_full(int fd, const char* data, std::size_t length) {
  std::size_t written = 0;
  while (written < length) {
    const ssize_t n = ::write(fd, data + written, length - written);
    if (n < 0) {
      if (errno == EINTR) continue;      // a signal interrupted us; not an error
      throw std::system_error(errno, std::generic_category(), "wal write");
    }
    written += static_cast<std::size_t>(n);
  }
}
```

`write()` is permitted to write fewer bytes than asked, and permitted to fail
with `EINTR` if a signal arrives mid-call. Both are normal. Treating `write()`
as all-or-nothing is one of the quietest ways to corrupt a file under load: it
works for months, then one truncated record appears the day the machine gets
busy.

### 3.6 `EventLoop` — poll, the self-pipe, and partial writes

**The self-pipe trick.** A thread parked in `poll()` can only be woken by I/O on
a descriptor it is watching. It cannot be woken by a bool changing in memory. So
the loop creates a pipe, watches the read end, and anyone who wants its
attention writes one byte to the write end:

```cpp
int fds[2];
if (::pipe(fds) < 0) throw std::system_error(...);
wake_read_fd_  = fds[0];
wake_write_fd_ = fds[1];
set_nonblocking(wake_read_fd_);
set_nonblocking(wake_write_fd_);
```

Both ends are non-blocking. The reader must not stall on an empty pipe; the
writer must not stall on a full one — and a full wakeup pipe already means "you
have been woken", so a dropped byte costs nothing. This is how `stop()` and
`adopt()` reach a sleeping loop instantly instead of waiting out a timeout.

The pipe must be **drained** when it fires:

```cpp
char scratch[64];
while (::read(wake_read_fd_, scratch, sizeof(scratch)) > 0) { }
```

`poll` is level-triggered: it reports a readable descriptor every single call
until it is actually emptied. Skip the drain and the loop spins at 100% CPU.

The same mechanism handles signals in `main.cpp`. A signal handler runs at an
arbitrary instant on a borrowed stack, possibly in the middle of `malloc`;
almost nothing is safe to call from one. `write()` on a descriptor is on the
short list that is, so the handler's entire body is one byte written to a pipe
that ordinary code is blocked reading:

```cpp
extern "C" void handle_signal(int) {
  if (g_signal_pipe >= 0) {
    const char byte = 's';
    const ssize_t rc = ::write(g_signal_pipe, &byte, 1);
    (void)rc;
  }
}
```

**Partial writes.** The single most common bug in hand-written servers:

```cpp
bool Connection::flush(bool& blocked) {
  while (output_sent_ < output_.size()) {
    const ssize_t n = ::send(fd_, output_.data() + output_sent_,
                             output_.size() - output_sent_, kSendFlags);
    if (n > 0) { output_sent_ += static_cast<std::size_t>(n); continue; }
    if (n < 0 && errno == EINTR) continue;
    if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
      blocked = true;              // keep the tail, wait for POLLOUT
      return true;
    }
    return false;                  // a real error
  }
  ...
}
```

If the peer is slow or has stopped reading, the kernel's send buffer fills and
`send()` returns `EAGAIN` with bytes still unsent. There are exactly three
things you can do, and two of them are wrong:

- **Drop the tail** → silent protocol corruption; the client gets half a reply.
- **Loop until it fits** → you have re-invented blocking I/O inside a
  non-blocking server, and one slow client now stalls every other client
  sharing that thread.
- **Keep the tail, register `POLLOUT`, finish when the kernel says there is
  room** → correct, and what this does.

**`SIGPIPE` must be ignored.** Writing to a socket whose peer has gone away
raises `SIGPIPE`, whose default disposition is to *terminate the process*. One
client disconnecting at the wrong moment would kill the entire server. So
`main()` calls `std::signal(SIGPIPE, SIG_IGN)` and the code handles the `EPIPE`
that `send()` returns instead. macOS also gets the per-socket `SO_NOSIGPIPE`
option; Linux gets the per-call `MSG_NOSIGNAL` flag.

**`accept()` in a loop until `EAGAIN`**, with `EMFILE` handled by refusing the
connection and staying alive rather than dying, and `ECONNABORTED` skipped —
that is a peer that vanished mid-handshake, which is normal on the public
internet, not an error.

### 3.7 RAII — the destructors doing the cleanup you never wrote

RAII means a resource's lifetime is tied to an object's lifetime, so cleanup
runs on **every** exit path — normal return, early return, and thrown exception
— without a single explicit call.

Every resource in this codebase is owned by something:

| Resource | Owner | Released by |
|---|---|---|
| client socket fd | `Connection` | `~Connection()` |
| connections | `EventLoop` (`unordered_map<int, unique_ptr<Connection>>`) | `~EventLoop()` clears the map |
| wakeup pipe fds | `EventLoop` | `~EventLoop()` |
| `wal.log` fd | `Wal` | `~Wal()` |
| listening socket | `Server` | `~Server()` |
| I/O threads | `Server` | `~Server()` stops and joins |
| `wal_mutex_` / `mutex_` | `lock_guard` / `unique_lock` / `shared_lock` | scope exit |
| test log files | `TempLog` | `~TempLog()` |

Three details that are easy to miss:

**Non-copyable resource owners.** `Connection`, `Wal`, `EventLoop`, `Server`
and `Storage` all delete their copy operations. A file descriptor is an
`int`; copying the object copies the integer, and then two destructors call
`close()` on it. The second `close()` shuts down whatever descriptor the kernel
has since recycled that number for — a bug that manifests as a *completely
unrelated* connection dying. Deleting the copy constructor makes that
uncompilable.

**Destructors must not throw.** `~Wal()` calls `::close(fd_)` and ignores the
result. If a destructor throws during stack unwinding from another exception,
the runtime calls `std::terminate`. There is also nobody left to tell — the
object is going away regardless.

**The `stop()` / join ordering in `~Server()`.** Threads are stopped before
they are joined, and joined before the listening socket is closed. Reverse any
of that and you either join a thread that is still parked in `poll()` (a hang),
or close a descriptor a running thread is still polling on.

The payoff shows in `main()`: there is no cleanup block. The `catch` handler
prints a message and returns, and every descriptor, thread, and mutex is
released by a destructor on the way out.

---

## 4. Crash recovery, step by step

The scenario: the server is killed with `SIGKILL` — no clean shutdown, no
destructors, no flush. Here is exactly what happens on the next start.

**Before the crash**, `Storage::set` had already guaranteed the invariant that
makes recovery possible:

> Every mutation the client was told succeeded is already on disk, in the order
> it was applied to memory.

### The restart sequence

**Step 1 — `main()` constructs the `Wal`, then the `Storage`.**

```cpp
kvstore::Wal wal(options.wal_path, ...);
kvstore::Storage storage(&wal);
const std::size_t replayed = storage.load_from_wal();
wal.open();
```

Order matters: `load_from_wal()` runs **before** `wal.open()`, and before the
listening socket exists. `Wal::replay` even throws if the append descriptor is
already open. No client can observe a half-restored store, because there is
nothing to connect to yet.

**Step 2 — `Wal::replay` opens the log `O_RDWR`.**

Read-write, not read-only, because a torn tail has to be *truncated away*, not
merely skipped. `O_CREAT` means a missing log is simply an empty one — a first
boot is not an error (`an_absent_log_is_a_valid_empty_log`).

**Step 3 — walk records from offset 0.** For each one:

```
  a. Enough bytes left for an 8-byte header?      no → stop
  b. Read payload_len and expected_crc
  c. Does payload_len run past end-of-file?       yes → stop  ← TORN WRITE
  d. crc32(payload) == expected_crc?              no  → stop  ← BIT ROT
  e. Decode op / key / value / deadline
     Any field running off the end?               yes → stop
  f. on_record(record) → Storage::apply_locked
  g. good_offset = end of this record; continue
```

Steps (c) and (d) are the crash cases, and both mean the same thing: **stop,
do not skip**. A record after a damaged one cannot be trusted to be a record at
all — the reader has lost the boundary and would be interpreting arbitrary
bytes as a length header. And it is safe to stop: because the log is append-only
and `fsync` completed before the client was acknowledged, anything after the
damage was never acknowledged to anybody.

**Step 4 — apply in order.** `apply_locked` replays `kSet` / `kDel` / `kExpire`
against the map. Replay is a straight re-execution of history, so last-writer-
wins falls out for free: `SET a 1; SET a 2` replays to `2`, and
`SET b x; DEL b` replays to no `b` at all.

Note that `apply_locked` is used by both `load_from_wal` *and* the live write
path. One function, so recovery and normal operation can never drift apart —
the classic source of "it works until you restart" bugs.

**Step 5 — truncate the garbage.**

```cpp
if (good_offset < contents.size()) {
  if (::ftruncate(fd, static_cast<off_t>(good_offset)) < 0) { ... }
}
```

Without this, the next `append` would start writing *after* the torn bytes, and
every future replay would stop at the same damaged spot — a log that silently
stops recording while appearing to work perfectly.

**Step 6 — drop what expired while the process was down.**

```cpp
const std::int64_t now = now_ms();
for (auto it = map_.begin(); it != map_.end();) {
  it = it->second.expired(now) ? map_.erase(it) : std::next(it);
}
```

Deadlines were persisted absolute, so this is a simple comparison against the
current clock. A key with a 60-second TTL set just before a crash that lasted an
hour does **not** come back to life.

**Step 7 — `wal.open()`**, then `Server::start()`, then accept clients.

### Watching it happen

```bash
./build/kvstore --port 7391 --wal /tmp/demo.log &
printf 'SET user:1 ikbal\r\nSET lang c++\r\nDEL lang\r\nSET temp gone\r\nEXPIRE temp 1\r\n' \
  | nc -w 2 127.0.0.1 7391
kill -9 %1                      # hard crash: no destructors, no clean shutdown
./build/kvstore --port 7391 --wal /tmp/demo.log &
printf 'GET user:1\r\nGET lang\r\nGET temp\r\n' | nc -w 2 127.0.0.1 7391
```

Actual output from the second run:

```
recovered 5 log record(s), 1 live key(s) from /tmp/demo.log
listening on 127.0.0.1:7391 with 4 I/O thread(s)
$5
ikbal          ← restored
$-1            ← lang: the DEL was replayed too
$-1            ← temp: its absolute deadline passed while the process was dead
```

And the torn-tail path, forced by hand:

```bash
printf '\x1c\x00\x00\x00\xff\xff\xff\xff\x01\x06\x00\x00\x00hal' >> /tmp/demo.log
# log grows 143 -> 159 bytes
./build/kvstore --port 7393 --wal /tmp/demo.log
# recovered 5 log record(s), 1 live key(s)
# log is back to 143 bytes — the torn record was detected and truncated
```

---

## 5. Hands-on verification: five tracing questions

Work through each one before reading the answer. If you can answer all five
from the code, you understand this server.

---

### Q1 — Framing

A client sends `*2\r\n$3\r\nGET\r\n$5\r\nhe` in one TCP segment, then 200 ms
later sends `llo\r\n`. Trace both wakeups: what does `recv()` return, what does
`parse_command` return, and what is in the `Buffer` at each point?

<details>
<summary><strong>Answer</strong></summary>

**First wakeup.**

1. `poll()` reports `POLLIN` on the client fd.
2. `Connection::fill()` calls `ensure_writable(16384)`, then `recv()`, which
   returns 19 — the bytes available *at that instant*. `commit(19)`.
   `read_pos_=0`, `write_pos_=19`.
3. `fill()` loops and calls `recv()` again. It returns `-1` with
   `errno == EAGAIN`. That is not an error; it means drained. Return.
4. `process()` calls `parse_command` on the 19 readable bytes.
   - `input.front() == '*'` → `parse_array`.
   - `find("\r\n")` at index 2 → count = 2. Valid, ≤ `kMaxArgs`.
   - Element 0: `$3\r\n`, length 3, needs `body + 3 + 2` bytes; available.
     `args[0] = "GET"` as a view into the buffer.
   - Element 1: `$5\r\n` at index 13, length 5. `body = 17`,
     `need = 17 + 5 + 2 = 24`. But `input.size() == 19`.
     **`19 < 24` → `return incomplete()`.**
5. `process()` sees `kIncomplete` and **breaks without consuming anything**.
   `read_pos_` is still 0. All 19 bytes stay exactly where they are. The
   partially-built `args` vector is discarded — it is a local, and it will be
   rebuilt from scratch next time.
6. `output_` is empty, so nothing is sent. The loop goes back to `poll()`.

**Second wakeup, 200 ms later.**

7. `fill()` → `ensure_writable(16384)`. `read_pos_ == 0`, so `compact()` is a
   no-op and there is already room; no reallocation. `recv()` returns 5.
   `commit(5)` → `write_pos_ = 24`.
8. `process()` calls `parse_command` on all 24 bytes — **from the beginning
   again**. The parser has no memory of the previous attempt; it re-parses the
   `GET` header from scratch. This is the design: the buffer *is* the state.
9. This time `need = 24` and `input.size() == 24`. `args[1] = "hello"`.
   Returns `kOk` with `consumed = 24`.
10. `execute_command` → `storage.get("hello")` → `reply_bulk` or `reply_nil`
    appended to `output_`.
11. `input_.consume(24)`. Since `read_pos_ >= write_pos_`, both reset to 0.
12. `flush()` sends the reply.

**The key insight:** re-parsing from the start on every attempt is deliberate.
It costs a few microseconds of redundant header scanning and buys the complete
absence of resumable-parser state — which is where framing bugs actually live.

</details>

---

### Q2 — Lifetimes

`Command::args` are `string_view`s (no copy), but `Storage::get` returns
`optional<string>` (a copy). Justify each. Then state precisely what breaks if
you swap them: (a) `args` become `std::string`, (b) `get` returns
`string_view`.

<details>
<summary><strong>Answer</strong></summary>

**Why `args` are views.** The bytes are already in the connection's read
buffer, that buffer is owned by this `Connection`, and the `Command` is a local
inside `process()` on the same thread. The buffer provably outlives the
`Command`, and `process()` never calls `ensure_writable`/`append` while a
`Command` is alive. Copying would allocate and memcpy the entire value — for a
1 MB `SET`, a wasted 1 MB copy on the way to the copy that actually matters.

**Why `get` returns a copy.** The `shared_lock` is released when `get` returns.
After that instant nothing protects the entry.

**(a) `args` as `std::string`:** still correct, just slower. Every command
allocates one string per argument. On a `SET` of a 1 MB value that is an extra
1 MB allocation and copy per request, for nothing. Correctness unaffected;
throughput materially worse.

**(b) `get` returning `string_view`:** **use-after-free.** Concretely:

```
 Thread A (reader)                  Thread B (writer)
 ─────────────────────────────────────────────────────────────────
 shared_lock(mutex_)
 it = map_.find("k")
 view = {it->second.value}
 return view                ← lock released HERE
                                    lock_guard(wal_mutex_)
                                    wal_->append(...)
                                    unique_lock(mutex_)
                                    map_["k"].value = <64 KB value>
                                       → std::string reallocates:
                                         old heap buffer FREED
 reply_bulk(out, view)
   → reads freed memory              ← undefined behaviour
```

`DEL` is worse: the entry is erased and the string destroyed outright.

Two properties make this especially nasty. First, short values may be stored
inline by the small-string optimisation, so short-key tests pass and only long
values corrupt — it looks like a size-dependent heisenbug. Second, freed memory
usually still *contains* the old bytes for a while, so it often produces
correct output in testing and garbage under production load.

**The rule:** a view is safe only when you can name the object keeping its
memory alive and prove it outlives the view. For `args` that is the read
buffer. For a value read out of a shared map under a lock you are about to
release, there is nothing — so you copy.

</details>

---

### Q3 — Durability boundaries

A power cut occurs at each of these three instants during
`SET k v`. For each: what did the client see, and what does the store contain
after restart?

- **(A)** after `write_full()` returns, before `fsync()`
- **(B)** after `fsync()` returns, before `apply_locked()`
- **(C)** after `apply_locked()`, before `flush()` sends `+OK`

<details>
<summary><strong>Answer</strong></summary>

**(A) between `write()` and `fsync()`.**
The bytes are in the kernel page cache, not on the medium. A *power cut* loses
them; a `kill -9` would not, since the kernel survives and flushes later.

- Client saw: **nothing** — `Storage::set` had not returned, so no `+OK` was
  ever queued.
- After restart: the record is **absent**, or present as a **torn tail** if the
  drive persisted part of it. Either way replay's length/CRC check stops there
  and `ftruncate` removes it.
- **Consistent.** The client never got an acknowledgement, so "did not happen"
  is a correct answer.

**(B) between `fsync()` and `apply_locked()`.**
The record is durable. Memory was never updated. This is the window that
justifies the whole design.

- Client saw: **nothing** — still inside `Storage::set`.
- After restart: replay applies the record. **The key is present.**
- **Consistent.** The client did not get an `+OK`, and the write happened
  anyway. That is allowed: an unacknowledged operation may or may not take
  effect. What is *never* allowed is the opposite — an acknowledged operation
  that did not take effect.

**(C) after the map update, before the reply is sent.**
The record is durable and memory agrees.

- Client saw: **nothing** — the TCP connection dies with the process, so the
  client gets a connection reset and a timeout, not `+OK`.
- After restart: the key is **present**.
- **Consistent**, same reasoning as (B). A well-written client retries; if `SET`
  is idempotent, the retry is harmless.

**Now invert the order — log *after* memory — and take case (B'):** memory
updated, power cut before the log write.

- Client saw: nothing yet, *but a concurrent reader on another thread could
  already have observed the new value and told a different client about it.*
- After restart: the record is absent. **The key reverts.**
- **Inconsistent.** A value that was read and acted upon has vanished. That is
  precisely the failure mode "write-ahead" exists to prevent, and it is why the
  ordering in `Storage::set` is not stylistic.

</details>

---

### Q4 — The ordering invariant

Threads T1 and T2 concurrently run `SET k A` and `SET k B`. Prove the log order
matches the memory order. Then: delete `wal_mutex_` and use only
`unique_lock(mutex_)` around `apply_locked`, keeping `wal_->append` outside it.
Construct the interleaving that breaks.

<details>
<summary><strong>Answer</strong></summary>

**The proof.** `wal_mutex_` is a plain mutex held from before `append()` until
after `apply_locked()`:

```cpp
std::lock_guard<std::mutex> wal_lock(wal_mutex_);   // acquired
if (wal_ != nullptr) wal_->append(record);          //   append
std::unique_lock<std::shared_mutex> lock(mutex_);   //   ...
apply_locked(record);                               //   apply
}                                                   // released
```

Mutual exclusion means one of T1/T2 holds it strictly before the other. Say T1
wins. Then T1's append **and** T1's apply both complete before T2 acquires it,
so T2's append and apply both follow. The log reads `A` then `B`; memory ends
at `B`. Replay applies `A` then `B` and ends at `B`. **Identical.** The
argument is symmetric, so it holds whichever thread wins, and it extends to any
number of writers.

Note the `shared_mutex` alone gives you nothing here — it makes each *step*
atomic, but says nothing about the *pairing* of two separate steps.

**The break.** Without `wal_mutex_`, append and apply are independently
serialised, and the following is legal:

```
 T1 (SET k A)                     T2 (SET k B)                LOG        MEMORY
 ────────────────────────────────────────────────────────────────────────────────
 wal_->append("k","A")                                        [A]        -
                                  wal_->append("k","B")       [A][B]     -
                                  unique_lock(mutex_)
                                  map_["k"] = "B"             [A][B]     k=B
                                  unlock
 unique_lock(mutex_)
 map_["k"] = "A"                                              [A][B]     k=A
 unlock
```

T1 was descheduled between its append and its apply — a completely ordinary
thing for a thread to do, and *more* likely here because `append` ends in an
`fsync` that can park the thread for milliseconds.

Final live state: **`k = A`**. Log order: **A then B**. Replay yields
**`k = B`**.

The server serves `A` to every client until it restarts, then serves `B`
forever after. Nothing crashed, nothing logged an error, no test that does not
restart the process can see it. This is the worst class of bug there is:
silent, timing-dependent, and only observable across a restart.

`log_order_matches_memory_order_under_contention` is the executable version of
this argument — six threads, 500 writes each to one key, then a fresh store
rebuilt from the log is required to agree with the live one. Delete
`wal_mutex_` and that test starts failing intermittently.

</details>

---

### Q5 — Backpressure

A client issues a pipelined batch whose replies total 10 MB, then stops calling
`recv()` — but keeps the socket open. Trace what the server does. Then: what
breaks if `flush()` looped until everything was sent, and what breaks if it
dropped the unsent tail?

<details>
<summary><strong>Answer</strong></summary>

**What actually happens.**

1. `fill()` reads the pipelined batch. `process()` parses each command in turn
   and appends every reply to `output_`, which grows toward 10 MB.
2. `handle_connection` calls `flush()`. `send()` succeeds repeatedly, moving
   `output_sent_` forward, until the kernel's socket send buffer fills — the
   client is not draining it, so TCP's flow control shrinks the receive window
   to zero and the sender stops.
3. `send()` returns `-1` / `EAGAIN`. `flush()` sets `blocked = true` and returns
   `true` — **not** an error. `output_sent_` is, say, 256 KB of 10 MB.
4. Back in `run()`, this connection now reports `has_pending_output() == true`,
   so its pollfd is built with `POLLIN | POLLOUT`.
5. `poll()` sleeps. Crucially, the thread is **not** stuck on this client —
   every other connection on this loop continues to be served normally.
6. When the client finally reads, the kernel drains its send buffer, `poll()`
   reports `POLLOUT`, `handle_connection` calls `flush()` again, and it resumes
   from `output_sent_`.
7. `output_.erase(0, output_sent_)` runs once the sent prefix exceeds 64 KB, so
   the buffer does not keep 10 MB of already-delivered bytes alive.
8. When it all lands, `output_.clear()` and `output_sent_ = 0`. `POLLOUT` is
   dropped from the interest set on the next iteration — otherwise `poll()`
   would return instantly forever on a writable idle socket.

**If `flush()` looped until everything was sent.** You have re-implemented
blocking I/O inside a non-blocking server. The thread spins in `send()`/`EAGAIN`
burning a core, or blocks outright — and *every other connection assigned to
that event loop is frozen* until this one client decides to read. With 4 I/O
threads, four such clients halt the entire server. This is a trivially
exploitable denial of service: connect, send one pipelined batch, never read.

**If `flush()` dropped the unsent tail.** Silent protocol corruption. The client
receives 256 KB of a 10 MB response — very likely cut mid-bulk-string. Its
parser then reads whatever follows as a length header and desynchronises
permanently. No error is reported at either end; the connection simply starts
returning nonsense. Far worse than an error, because it is undetectable from the
outside.

**What this implementation still lacks** (named honestly): there is no cap on
`output_`. A client that pipelines aggressively and never reads makes the server
buffer without bound. A production server sets a maximum output-buffer size per
client and closes any connection that exceeds it — Redis calls this the client
output buffer limit. `kMaxInputBacklog` does this for the *input* side; the
output side has no equivalent. See §6.

</details>

---

## 6. What this server does not do

Every one of these is a deliberate omission, not an oversight.

- **No log compaction.** `wal.log` grows forever. Overwriting one key a million
  times leaves a million records, and startup replays all of them. The standard
  fix is a snapshot: dump the live map to a new file, append subsequent
  mutations to it, atomically `rename()` it over the old log. `rename` is atomic
  on POSIX, so a crash mid-compaction leaves either the old log or the new one,
  never a blend.
- **No output-buffer cap.** See Q5. The input side is bounded by
  `kMaxInputBacklog`; the output side is not.
- **`poll` is O(connections) per iteration.** Fine to a few thousand
  connections; past that, `epoll`/`kqueue`. Confined to `EventLoop::run()`.
- **No authentication, no TLS.** It binds to `127.0.0.1` by default for exactly
  this reason. Do not put it on a public interface.
- **Only five commands, and only string values.** No lists, hashes, sets, or
  sorted sets. Adding a type means extending `WalOp` and versioning the record
  format.
- **No replication, no clustering, no pub/sub, no transactions.**
- **`purge_expired` scans the whole map** once a second. At millions of keys
  that is a visible latency spike; Redis instead samples a random subset
  repeatedly, trading exactness for a bounded cost per pass.
- **Sanitizer coverage is partial.** The suite runs clean under UBSan. The
  AddressSanitizer and ThreadSanitizer runtimes shipped with Apple clang 15
  fail during their own initialisation on macOS 26 — a trivial
  `int main(){return 0;}` segfaults identically — so those two were not run
  here. On Linux, or with a Homebrew LLVM, `-DKVSTORE_SANITIZE=ON` builds them
  and a TSan run over `concurrent_readers_and_writers_keep_the_store_consistent`
  is the check you want on the locking.

---

## Further reading

- Stevens, Fenner & Rudoff, *UNIX Network Programming, Vol. 1* — the reference
  for sockets, `poll`, and every error code above.
- Kerrisk, *The Linux Programming Interface*, ch. 4–5 (file I/O and buffering),
  13 (`fsync` and the page cache), 63 (I/O multiplexing).
- Man pages worth reading in full: `poll(2)`, `accept(2)`, `send(2)`,
  `fsync(2)`, `fcntl(2)` (for `F_FULLFSYNC`), `tcp(7)`.
- Redis's `redis.io/docs/reference/protocol-spec` for the full RESP grammar,
  of which this implements a subset.
