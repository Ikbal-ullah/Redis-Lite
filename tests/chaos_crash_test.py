#!/usr/bin/env python3
"""Crash-consistency chaos test against the real server binary.

    KVSTORE_BIN=build/kvstore .venv/bin/python tests/chaos_crash_test.py \
        --iterations 20 --fsync always

Phase 1 -- SIGKILL loop. Each iteration starts the server on the same WAL,
runs concurrent writers (SET, SET EX, DEL) and SIGKILLs the server at a random
moment while requests are in flight. After restart, every write that was
*acknowledged* must be visible exactly. The one request per writer that was in
flight at the kill is ambiguous (it may or may not have committed) and is
allowed either outcome. Any other mismatch is a lost acknowledged write.

Phase 2 -- torn tail. Random garbage is appended to the WAL (what a crash
mid-append leaves). The server must truncate exactly that many bytes and lose
nothing acknowledged.

Phase 3 -- mid-file corruption. One byte in the middle of the WAL is flipped.
The server must refuse to start (exit 3) and leave the file byte-for-byte
unchanged, instead of silently truncating acknowledged data.

Note what this does and does not prove: SIGKILL destroys the process, not the
kernel page cache, so it validates the write/ack ordering and recovery code,
not the drive's flush behaviour (that would need power-cut testing).
"""

from __future__ import annotations

import argparse
import hashlib
import os
import random
import re
import sys
import tempfile
import threading
import time
from pathlib import Path

import redis
from redis.backoff import NoBackoff
from redis.retry import Retry

sys.path.insert(0, str(Path(__file__).resolve().parent))
from kvserver import Server  # noqa: E402

MISSING = object()  # "key must not exist"


class Model:
    """What the client is entitled to believe is durable."""

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.expected: dict[str, object] = {}      # key -> bytes | MISSING
        self.ambiguous: dict[str, tuple] = {}      # key -> allowed outcomes
        self.acked = 0

    def ack(self, key: str, state: object) -> None:
        with self.lock:
            self.expected[key] = state
            self.acked += 1

    def in_flight_at_crash(self, key: str, new_state: object) -> None:
        with self.lock:
            self.ambiguous[key] = (self.expected.get(key, MISSING), new_state)


def writer(port: int, prefix: str, model: Model, stop: threading.Event, seed: int) -> None:
    rng = random.Random(seed)
    # No client-side retries: a retried request after a crash would blur which
    # attempt was acknowledged.
    r = redis.Redis(host="127.0.0.1", port=port, socket_timeout=10, protocol=2,
                    retry=Retry(NoBackoff(), 0))
    mine: list[str] = []
    n = 0
    while not stop.is_set():
        roll = rng.random()
        if roll < 0.15 and mine:
            key, new_state = rng.choice(mine), MISSING
            op = lambda: r.delete(key)  # noqa: E731
        else:
            key = f"{prefix}:{n}"
            n += 1
            value = f"{key}={rng.getrandbits(64):x}".encode()
            new_state = value
            if roll < 0.30:
                op = lambda: r.set(key, value, ex=3600)  # noqa: E731
            else:
                op = lambda: r.set(key, value)  # noqa: E731
        try:
            op()
        except (redis.ConnectionError, redis.TimeoutError, ConnectionResetError, OSError):
            model.in_flight_at_crash(key, new_state)
            return
        model.ack(key, new_state)
        if new_state is MISSING:
            mine.remove(key)
        else:
            mine.append(key)


def verify(port: int, model: Model) -> tuple[int, list[str]]:
    """Returns (#keys checked, list of violations). Settles ambiguous keys."""
    r = redis.Redis(host="127.0.0.1", port=port, socket_timeout=30, protocol=2)
    keys = sorted(set(model.expected) | set(model.ambiguous))
    pipe = r.pipeline(transaction=False)
    for k in keys:
        pipe.get(k)
    actual = dict(zip(keys, pipe.execute()))
    violations = []
    for k in keys:
        got = MISSING if actual[k] is None else actual[k]
        allowed = model.ambiguous.get(k, (model.expected.get(k, MISSING),))
        if got not in allowed:
            violations.append(f"{k}: got {got!r}, allowed {allowed!r}")
        model.expected[k] = got  # whatever recovered is now the durable truth
    model.ambiguous.clear()
    r.close()
    return len(keys), violations


def parse_recovery(log: str) -> str:
    m = re.search(r"recovered .*", log)
    return m.group(0) if m else log.strip()


def phase_sigkill(wal: Path, fsync: str, iterations: int, writers: int, rng: random.Random) -> Model:
    model = Model()
    print(f"== Phase 1: {iterations} SIGKILL iterations, --fsync {fsync}, {writers} writers ==")
    total_lost = 0
    for it in range(1, iterations + 1):
        srv = Server(wal, fsync=fsync).start()
        checked, violations = verify(srv.port, model)
        if violations:
            print("\n".join(violations[:20]))
        total_lost += len(violations)

        stop = threading.Event()
        before = model.acked
        threads = [threading.Thread(target=writer, args=(srv.port, f"i{it}w{w}", model, stop,
                                                         rng.getrandbits(32)), daemon=True)
                   for w in range(writers)]
        for t in threads:
            t.start()
        time.sleep(rng.uniform(0.3, 1.0))
        srv.sigkill()  # mid-flight: writers are blocked on replies right now
        stop.set()
        for t in threads:
            t.join(timeout=15)
        recovery = parse_recovery(srv.log.decode(errors="replace"))
        print(f"iter {it:2d}: verified {checked:5d} keys, lost {len(violations)}, "
              f"acked this round {model.acked - before:5d}, in-flight at kill "
              f"{len(model.ambiguous)} | start: {recovery}")

    srv = Server(wal, fsync=fsync).start()
    checked, violations = verify(srv.port, model)
    total_lost += len(violations)
    srv.stop()
    print(f"final : verified {checked} keys after {iterations} crashes; total acknowledged "
          f"writes {model.acked}; LOST ACKNOWLEDGED WRITES: {total_lost}")
    assert total_lost == 0, "acknowledged writes were lost"
    return model


def phase_torn_tail(wal: Path, fsync: str, model: Model) -> None:
    print("== Phase 2: torn-tail garbage appended to the WAL ==")
    for size in (1, 7, 20, 21, 64, 333, 4096):
        clean = wal.stat().st_size
        with wal.open("ab") as f:
            f.write(os.urandom(size))
        srv = Server(wal, fsync=fsync).start()
        checked, violations = verify(srv.port, model)
        r = redis.Redis(host="127.0.0.1", port=srv.port, protocol=2)
        key = f"after-garbage-{size}"
        r.set(key, b"ok")  # the log must still be appendable
        model.ack(key, b"ok")
        r.close()
        srv.sigkill()
        recovery = parse_recovery(srv.log.decode(errors="replace"))
        truncated = re.search(r"truncated torn tail of (\d+) bytes", recovery)
        print(f"garbage {size:4d} B: {recovery} | verified {checked} keys, lost {len(violations)}")
        assert not violations, violations[:5]
        assert truncated and int(truncated.group(1)) == size, "wrong truncation"
        assert wal.stat().st_size > clean  # truncated back, then grew by one record
    srv = Server(wal, fsync=fsync).start()
    _, violations = verify(srv.port, model)
    srv.stop()
    assert not violations
    print("torn tail: all acknowledged writes intact, log appendable after every truncation")


def phase_mid_file_corruption(wal: Path, fsync: str) -> None:
    print("== Phase 3: single-byte corruption in the middle of the WAL ==")
    data = bytearray(wal.read_bytes())
    pos = len(data) // 2
    data[pos] ^= 0xFF
    wal.write_bytes(bytes(data))
    digest = hashlib.sha256(data).hexdigest()
    srv = Server(wal, fsync=fsync).start(expect_ready=False)
    code, out = srv.wait_exit()
    print(f"flipped byte {pos} of {len(data)}: exit code {code}: {out.strip()}")
    assert code == 3, "server must refuse to start on mid-file corruption"
    assert "corruption" in out
    assert hashlib.sha256(wal.read_bytes()).hexdigest() == digest, "file must be untouched"
    print("mid-file corruption: refused to start, WAL byte-for-byte unchanged")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--iterations", type=int, default=20)
    ap.add_argument("--fsync", default="always", choices=["no", "group", "always"])
    ap.add_argument("--writers", type=int, default=8)
    ap.add_argument("--seed", type=int, default=None)
    args = ap.parse_args()
    seed = args.seed if args.seed is not None else random.randrange(1 << 30)
    print(f"seed {seed}")
    rng = random.Random(seed)
    with tempfile.TemporaryDirectory() as tmp:
        wal = Path(tmp) / "chaos.wal"
        model = phase_sigkill(wal, args.fsync, args.iterations, args.writers, rng)
        phase_torn_tail(wal, args.fsync, model)
        phase_mid_file_corruption(wal, args.fsync)
    print("ALL CHAOS CHECKS PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
