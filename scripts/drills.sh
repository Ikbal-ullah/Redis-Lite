#!/usr/bin/env bash
#
# Hands-on failure drills for the kvstore server.
#
# Each drill provokes one specific behaviour -- a torn log tail, a bit flip, a
# hard crash, backpressure from a client that stops reading -- and prints what
# actually happened. Reading about a torn write is not the same as watching
# recovery truncate one.
#
#   ./scripts/drills.sh            list the drills
#   ./scripts/drills.sh 3          run drill 3
#   ./scripts/drills.sh all        run every drill in order
#
# Requires the project to be built:  cmake -S . -B build && cmake --build build

set -u

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SERVER="$REPO_ROOT/build/kvstore"
CLI="$REPO_ROOT/build/kvstore-cli"
PORT="${DRILL_PORT:-7500}"
WORKDIR=""
SERVER_PID=""

# --- terminal formatting ----------------------------------------------------

if [ -t 1 ] && [ -z "${NO_COLOR:-}" ]; then
  BOLD=$'\033[1m'; DIM=$'\033[2m'; GREEN=$'\033[32m'; YELLOW=$'\033[33m'
  CYAN=$'\033[36m'; RED=$'\033[31m'; RESET=$'\033[0m'
else
  BOLD=""; DIM=""; GREEN=""; YELLOW=""; CYAN=""; RED=""; RESET=""
fi

banner() { printf "\n%s=== %s ===%s\n" "$BOLD$CYAN" "$1" "$RESET"; }
step()   { printf "%s-->%s %s\n" "$YELLOW" "$RESET" "$1"; }
note()   { printf "%s    %s%s\n" "$DIM" "$1" "$RESET"; }
good()   { printf "%s    %s%s\n" "$GREEN" "$1" "$RESET"; }
bad()    { printf "%s    %s%s\n" "$RED" "$1" "$RESET"; }

# --- lifecycle --------------------------------------------------------------

require_build() {
  if [ ! -x "$SERVER" ] || [ ! -x "$CLI" ]; then
    bad "build first:  cmake -S . -B build && cmake --build build"
    exit 1
  fi
}

setup() {
  WORKDIR="$(mktemp -d "${TMPDIR:-/tmp}/kvstore-drill.XXXXXX")"
}

start_server() {
  # shellcheck disable=SC2086
  "$SERVER" --port "$PORT" --wal "$WORKDIR/wal.log" $1 > "$WORKDIR/server.out" 2>&1 &
  SERVER_PID=$!
  # Wait for the listener rather than sleeping a fixed amount.
  local i
  for i in $(seq 1 50); do
    if "$CLI" --port "$PORT" PING > /dev/null 2>&1; then return 0; fi
    sleep 0.2
  done
  bad "server did not come up; log follows:"
  cat "$WORKDIR/server.out"
  return 1
}

stop_server() {
  if [ -n "$SERVER_PID" ]; then
    kill "$SERVER_PID" 2>/dev/null
    wait "$SERVER_PID" 2>/dev/null
    SERVER_PID=""
  fi
}

crash_server() {
  if [ -n "$SERVER_PID" ]; then
    kill -9 "$SERVER_PID" 2>/dev/null
    wait "$SERVER_PID" 2>/dev/null
    SERVER_PID=""
  fi
}

cleanup() {
  crash_server
  [ -n "$WORKDIR" ] && rm -rf "$WORKDIR"
}
trap cleanup EXIT INT TERM

wal_size() { wc -c < "$WORKDIR/wal.log" | tr -d ' '; }

# ===========================================================================
# Drill 1 -- read the write-ahead log with your own eyes
# ===========================================================================
drill_1() {
  banner "Drill 1: the on-disk record format"
  setup; start_server "" || return 1

  step "SET user:1 ikbal   (one record, nothing else)"
  "$CLI" --port "$PORT" SET user:1 ikbal > /dev/null
  stop_server

  note "wal.log is now $(wal_size) bytes. Hexdump:"
  echo
  xxd "$WORKDIR/wal.log"
  echo
  cat <<'ANNOTATION'
    1c000000  payload_len = 0x1C = 28 bytes, little-endian
    ????????  crc32 of the 28 payload bytes
    01        opcode kSet
    06000000  key length = 6
    user:1    the key
    05000000  value length = 5
    ikbal     the value
    00000000 00000000   expire_at_ms = 0 (never expires)

    Total 36 bytes = 8 header + 28 payload.
ANNOTATION
  good "Every integer is written a byte at a time, least significant first, so"
  good "the file means the same thing on a big-endian machine."
  cleanup
}

# ===========================================================================
# Drill 2 -- a hard crash loses nothing that was acknowledged
# ===========================================================================
drill_2() {
  banner "Drill 2: kill -9 durability"
  setup; start_server "" || return 1

  step "write three keys, delete one, expire one with a 1 second TTL"
  "$CLI" --port "$PORT" SET durable yes      > /dev/null
  "$CLI" --port "$PORT" SET doomed maybe     > /dev/null
  "$CLI" --port "$PORT" DEL doomed           > /dev/null
  "$CLI" --port "$PORT" SET fleeting soon    > /dev/null
  "$CLI" --port "$PORT" EXPIRE fleeting 1    > /dev/null

  step "kill -9 : no clean shutdown, no destructors, no flush on the way out"
  crash_server
  note "wal.log survived at $(wal_size) bytes"

  step "wait 1.5s so the TTL elapses while the process is dead"
  sleep 1.5

  step "restart from the same log"
  start_server "" || return 1
  note "$(head -1 "$WORKDIR/server.out")"

  printf "    GET durable  -> %s" "$("$CLI" --port "$PORT" GET durable)"
  printf "   %s(restored)%s\n" "$DIM" "$RESET"
  printf "    GET doomed   -> %s" "$("$CLI" --port "$PORT" GET doomed)"
  printf "   %s(the DEL replayed too)%s\n" "$DIM" "$RESET"
  printf "    GET fleeting -> %s" "$("$CLI" --port "$PORT" GET fleeting)"
  printf "   %s(absolute deadline passed during downtime)%s\n" "$DIM" "$RESET"

  good "A relative TTL in the log would have resurrected 'fleeting' here."
  cleanup
}

# ===========================================================================
# Drill 3 -- a torn tail is detected and truncated
# ===========================================================================
drill_3() {
  banner "Drill 3: torn write at the end of the log"
  setup; start_server "" || return 1

  "$CLI" --port "$PORT" SET keep yes > /dev/null
  "$CLI" --port "$PORT" SET also fine > /dev/null
  stop_server
  local before; before="$(wal_size)"
  note "clean log: $before bytes, 2 records"

  step "simulate a process killed halfway through writing record 3"
  # A plausible 8-byte header claiming 28 payload bytes, then only 7 of them.
  printf '\x1c\x00\x00\x00\xaa\xbb\xcc\xdd\x01\x06\x00\x00\x00us' >> "$WORKDIR/wal.log"
  note "log is now $(wal_size) bytes -- the last record is incomplete"

  step "restart: recovery reads until the length runs past end-of-file"
  start_server "" || return 1
  note "$(head -1 "$WORKDIR/server.out")"
  note "log truncated back to $(wal_size) bytes"

  printf "    GET keep -> %s\n" "$("$CLI" --port "$PORT" GET keep)"
  printf "    GET also -> %s\n" "$("$CLI" --port "$PORT" GET also)"

  step "prove the log is still writable at a record boundary"
  "$CLI" --port "$PORT" SET after recovery > /dev/null
  stop_server
  start_server "" || return 1
  note "$(head -1 "$WORKDIR/server.out")"
  printf "    GET after -> %s\n" "$("$CLI" --port "$PORT" GET after)"

  good "Without ftruncate, appends would land after the garbage and every"
  good "future replay would stop at the same spot -- a silently dead log."
  cleanup
}

# ===========================================================================
# Drill 4 -- a single flipped bit is caught by the checksum
# ===========================================================================
drill_4() {
  banner "Drill 4: bit rot caught by CRC-32"
  setup; start_server "" || return 1

  "$CLI" --port "$PORT" SET first good   > /dev/null
  "$CLI" --port "$PORT" SET second rotted > /dev/null
  stop_server
  note "clean log: $(wal_size) bytes, 2 records"

  step "flip one byte inside the payload of the second record"
  python3 - "$WORKDIR/wal.log" <<'PY'
import sys
path = sys.argv[1]
data = bytearray(open(path, "rb").read())
target = len(data) - 3          # inside the last record's value
before = data[target]
data[target] ^= 0xFF
open(path, "wb").write(data)
print(f"    byte at offset {target}: 0x{before:02x} -> 0x{data[target]:02x}")
PY
  note "the length header is still perfectly plausible; only the CRC disagrees"

  step "restart"
  start_server "" || return 1
  note "$(head -1 "$WORKDIR/server.out")"
  printf "    GET first  -> %s\n" "$("$CLI" --port "$PORT" GET first)"
  printf "    GET second -> %s\n" "$("$CLI" --port "$PORT" GET second)"

  good "Recovery stops at the damaged record rather than skipping it: once a"
  good "record is untrustworthy, the boundary after it is unknowable."
  cleanup
}

# ===========================================================================
# Drill 5 -- TCP has no message boundaries
# ===========================================================================
drill_5() {
  banner "Drill 5: stream framing, one byte at a time"
  setup; start_server "" || return 1

  step "send a 34-byte SET as 34 separate TCP writes, 20ms apart"
  python3 - "$PORT" <<'PY'
import socket, time, sys
port = int(sys.argv[1])
wire = b"*3\r\n$3\r\nSET\r\n$4\r\nslow\r\n$5\r\nsplit\r\n"
s = socket.create_connection(("127.0.0.1", port))
for i, byte in enumerate(wire):
    s.sendall(bytes([byte]))
    time.sleep(0.02)
print(f"    sent {len(wire)} bytes in {len(wire)} segments")
s.settimeout(5)
print(f"    server replied: {s.recv(100)!r}")
s.close()
PY
  printf "    GET slow -> %s\n" "$("$CLI" --port "$PORT" GET slow)"
  note "The parser returned kIncomplete 33 times, consuming nothing each time,"
  note "then parsed the whole command on the 34th byte."

  step "now the opposite: three commands in ONE segment (pipelining)"
  python3 - "$PORT" <<'PY'
import socket, sys
port = int(sys.argv[1])
s = socket.create_connection(("127.0.0.1", port))
s.sendall(b"*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n"
          b"*3\r\n$3\r\nSET\r\n$1\r\nb\r\n$1\r\n2\r\n"
          b"*2\r\n$3\r\nGET\r\n$1\r\na\r\n")
s.settimeout(5)
print(f"    one write, three replies: {s.recv(200)!r}")
s.close()
PY
  good "Same buffer, same parser. The number of recv() calls is unrelated to"
  good "the number of commands -- which is why kIncomplete consumes 0 bytes."
  cleanup
}

# ===========================================================================
# Drill 6 -- the durability tax, measured
# ===========================================================================
drill_6() {
  banner "Drill 6: what fsync actually costs"
  local n=300

  setup; start_server "" || return 1
  step "server WITH per-write flush (the default), $n sequential SETs"
  python3 - "$PORT" "$n" <<'PY'
import socket, sys, time
port, n = int(sys.argv[1]), int(sys.argv[2])
s = socket.create_connection(("127.0.0.1", port))
s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
def rt(cmd):
    s.sendall(cmd); return s.recv(256)
t0 = time.perf_counter()
for i in range(n):
    rt(b"*3\r\n$3\r\nSET\r\n$3\r\nk%02d\r\n$1\r\nv\r\n" % (i % 100))
w = time.perf_counter() - t0
t0 = time.perf_counter()
for i in range(n):
    rt(b"*2\r\n$3\r\nGET\r\n$3\r\nk%02d\r\n" % (i % 100))
r = time.perf_counter() - t0
print(f"    SET: {n/w:9,.0f} ops/s   ({w/n*1000:7.3f} ms each)")
print(f"    GET: {n/r:9,.0f} ops/s   ({r/n*1000:7.3f} ms each)")
s.close()
PY
  cleanup

  setup; start_server "--no-fsync" || return 1
  step "same server started with --no-fsync"
  python3 - "$PORT" "$n" <<'PY'
import socket, sys, time
port, n = int(sys.argv[1]), int(sys.argv[2])
s = socket.create_connection(("127.0.0.1", port))
s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
def rt(cmd):
    s.sendall(cmd); return s.recv(256)
t0 = time.perf_counter()
for i in range(n):
    rt(b"*3\r\n$3\r\nSET\r\n$3\r\nk%02d\r\n$1\r\nv\r\n" % (i % 100))
w = time.perf_counter() - t0
t0 = time.perf_counter()
for i in range(n):
    rt(b"*2\r\n$3\r\nGET\r\n$3\r\nk%02d\r\n" % (i % 100))
r = time.perf_counter() - t0
print(f"    SET: {n/w:9,.0f} ops/s   ({w/n*1000:7.3f} ms each)")
print(f"    GET: {n/r:9,.0f} ops/s   ({r/n*1000:7.3f} ms each)")
s.close()
PY
  good "Writes collapse by ~30x. Reads degrade far less, and for a different"
  good "reason: a reader never waits on the flush itself (it takes only the"
  good "shared map lock), but it does share a machine with a saturated disk."
  note "Be precise about this in an interview: the two-lock design removes the"
  note "flush from the read path's critical section. It cannot remove I/O"
  note "contention, cache pressure, or CPU time from the rest of the system."
  cleanup
}

# ===========================================================================
# Drill 7 -- a slow client must not stall the server
# ===========================================================================
drill_7() {
  banner "Drill 7: backpressure from a client that stops reading"
  setup; start_server "--no-fsync" || return 1

  step "store a 4 MB value, then ask for it and never read the reply"
  python3 - "$PORT" <<'PY'
import socket, sys, time
port = int(sys.argv[1])
big = b"x" * (4 * 1024 * 1024)

setter = socket.create_connection(("127.0.0.1", port))
setter.sendall(b"*3\r\n$3\r\nSET\r\n$3\r\nbig\r\n$%d\r\n" % len(big) + big + b"\r\n")
setter.recv(64)
setter.close()

# Ask for it, then go to sleep without reading. The server's send buffer
# fills, send() returns EAGAIN, and the reply sits in the connection's
# output buffer waiting for POLLOUT.
slow = socket.create_connection(("127.0.0.1", port))
slow.sendall(b"*2\r\n$3\r\nGET\r\n$3\r\nbig\r\n")
print("    slow client issued a 4 MB GET and is now ignoring the socket")
time.sleep(1.0)
PY
  note "meanwhile, a second client on the same server:"
  local t0 t1
  t0=$(python3 -c 'import time;print(time.time())')
  printf "    PING -> %s\n" "$("$CLI" --port "$PORT" PING)"
  printf "    SET/GET -> %s / %s\n" \
    "$("$CLI" --port "$PORT" SET other ok)" "$("$CLI" --port "$PORT" GET other)"
  t1=$(python3 -c 'import time;print(time.time())')
  python3 -c "print(f'    other clients served in {($t1-$t0)*1000:.1f} ms while one was blocked')"

  good "flush() returned on EAGAIN instead of looping. Had it spun until the"
  good "write completed, every client on that event loop would have frozen."
  cleanup
}

# ===========================================================================
# Drill 8 -- the protocol refuses hostile input
# ===========================================================================
drill_8() {
  banner "Drill 8: hostile input at the protocol boundary"
  setup; start_server "" || return 1

  step "claim a 4 GB bulk string in 13 bytes"
  python3 - "$PORT" <<'PY'
import socket, sys
port = int(sys.argv[1])
for probe, label in [
    (b"*1\r\n$4294967295\r\n",  "bulk length 4294967295"),
    (b"*abc\r\n",               "non-numeric array count"),
    (b"*1\r\n+PING\r\n",        "array element not a bulk string"),
]:
    s = socket.create_connection(("127.0.0.1", port)); s.settimeout(3)
    s.sendall(probe)
    try:
        reply = s.recv(200)
    except socket.timeout:
        reply = b"<no reply>"
    print(f"    {label:34s} -> {reply!r}")
    s.close()
PY
  note "the server is still alive:"
  printf "    PING -> %s\n" "$("$CLI" --port "$PORT" PING)"

  good "Every length read off the wire is bounds-checked before it is used to"
  good "size an allocation. Without kMaxBulkLength that first probe is a"
  good "remote out-of-memory kill delivered in thirteen bytes."
  cleanup
}

# ===========================================================================

usage() {
  cat <<EOF
${BOLD}kvstore failure drills${RESET}

  ./scripts/drills.sh <n>     run one drill
  ./scripts/drills.sh all     run all of them

  1  On-disk record format      hexdump a real WAL record
  2  kill -9 durability         acknowledged writes survive a hard crash
  3  Torn tail                  half-written record detected and truncated
  4  Bit rot                    flipped byte caught by CRC-32
  5  Stream framing             one command in 34 packets; three in one
  6  The fsync tax              writes collapse, reads do not
  7  Backpressure               a stalled client does not stall the server
  8  Hostile input              length limits at the protocol boundary

Set DRILL_PORT to use a port other than $PORT.
EOF
}

require_build
case "${1:-}" in
  1|2|3|4|5|6|7|8) "drill_$1" ;;
  all) for i in 1 2 3 4 5 6 7 8; do "drill_$i"; done
       banner "all drills complete" ;;
  *) usage ;;
esac
