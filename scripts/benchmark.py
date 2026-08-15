#!/usr/bin/env python3
"""Throughput and latency benchmark for the kvstore server.

Runs a fixed number of operations spread across worker threads, each with its
own connection, and reports throughput and the latency distribution.

    python3 scripts/benchmark.py                       # 10,000 ops, 8 threads
    python3 scripts/benchmark.py --ops 50000 --threads 16
    python3 scripts/benchmark.py --host 10.0.0.5 --port 6379 --value-size 4096

Reading the numbers honestly
---------------------------
This measures **end-to-end latency as a Python client experiences it**: request
encoding, the kernel round-trip, server processing, and reply parsing. It is not
a measurement of server-side service time, and it is not comparable to
`redis-benchmark`, which is C and drives far more load per core.

Two client-side effects dominate at high rates, and both flatter nobody:

  * The GIL serialises the Python half of every operation, so past a handful of
    threads you are measuring interpreter contention, not the server.
  * Every operation includes a full network round-trip. With `fsync` on every
    write -- the server's default -- SET latency is bounded below by the storage
    device, typically a millisecond or more. That is the price of durability,
    not a defect. Run the server with `--no-fsync` to see the difference.
"""

from __future__ import annotations

import argparse
import math
import os
import statistics
import sys
import threading
import time
from dataclasses import dataclass, field
from typing import Callable, List, Sequence

# Import the SDK from the sibling directory without requiring installation.
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "sdk"))

from redis_lite import Client, RedisLiteError  # noqa: E402


@dataclass
class Result:
    """Timing data for one workload."""

    name: str
    seconds: float
    latencies_ms: List[float] = field(repr=False, default_factory=list)
    errors: int = 0

    @property
    def ops(self) -> int:
        return len(self.latencies_ms)

    @property
    def rps(self) -> float:
        return self.ops / self.seconds if self.seconds > 0 else 0.0

    def percentile(self, p: float) -> float:
        """Nearest-rank percentile.

        Deliberately not an interpolating percentile: for latency you want a
        value that an actual request actually experienced, not an average of two
        that straddle the rank.
        """
        if not self.latencies_ms:
            return 0.0
        ordered = sorted(self.latencies_ms)
        rank = max(1, math.ceil(p / 100.0 * len(ordered)))
        return ordered[min(rank, len(ordered)) - 1]

    @property
    def p50(self) -> float:
        return self.percentile(50)

    @property
    def p99(self) -> float:
        return self.percentile(99)

    @property
    def mean_ms(self) -> float:
        return statistics.fmean(self.latencies_ms) if self.latencies_ms else 0.0

    @property
    def max_ms(self) -> float:
        return max(self.latencies_ms) if self.latencies_ms else 0.0


def _split_evenly(total: int, buckets: int) -> List[int]:
    """Distribute `total` across `buckets`, giving the remainder to the first."""
    base, extra = divmod(total, buckets)
    return [base + (1 if i < extra else 0) for i in range(buckets)]


def run_workload(
    name: str,
    operation: Callable[[Client, int], None],
    host: str,
    port: int,
    ops: int,
    threads: int,
    timeout: float,
) -> Result:
    """Execute `ops` operations across `threads` workers and time each one."""
    shares = _split_evenly(ops, threads)
    per_thread_latencies: List[List[float]] = [[] for _ in range(threads)]
    errors = [0] * threads
    setup_failures: List[BaseException] = []

    # Every worker waits here until all of them are connected, so connection
    # setup is excluded from the timing and the load starts genuinely at once.
    ready = threading.Barrier(threads + 1, timeout=max(30.0, timeout * threads))

    def worker(index: int, count: int, offset: int) -> None:
        try:
            client = Client(host=host, port=port, timeout=timeout)
        except BaseException as exc:  # noqa: BLE001 - reported to the caller
            setup_failures.append(exc)
            ready.abort()
            return

        samples = per_thread_latencies[index]
        try:
            ready.wait()
            for i in range(count):
                start = time.perf_counter()
                try:
                    operation(client, offset + i)
                except RedisLiteError:
                    errors[index] += 1
                    continue
                samples.append((time.perf_counter() - start) * 1000.0)
        except threading.BrokenBarrierError:
            return
        finally:
            client.close()

    workers = []
    offset = 0
    for index, count in enumerate(shares):
        thread = threading.Thread(
            target=worker, args=(index, count, offset), name=f"{name}-{index}", daemon=True
        )
        workers.append(thread)
        offset += count
        thread.start()

    try:
        ready.wait()
    except threading.BrokenBarrierError:
        for thread in workers:
            thread.join()
        raise SystemExit(f"error: worker setup failed: {setup_failures[0]}")

    started = time.perf_counter()
    for thread in workers:
        thread.join()
    elapsed = time.perf_counter() - started

    latencies = [sample for samples in per_thread_latencies for sample in samples]
    return Result(name=name, seconds=elapsed, latencies_ms=latencies, errors=sum(errors))


def format_table(results: Sequence[Result]) -> str:
    header = (
        f"{'Workload':<12}{'Ops':>9}{'Time (s)':>11}{'Req/s':>13}"
        f"{'p50 (ms)':>11}{'p99 (ms)':>11}{'max (ms)':>11}{'Errors':>9}"
    )
    lines = [header, "-" * len(header)]
    for result in results:
        lines.append(
            f"{result.name:<12}{result.ops:>9,}{result.seconds:>11.3f}{result.rps:>13,.0f}"
            f"{result.p50:>11.3f}{result.p99:>11.3f}{result.max_ms:>11.3f}{result.errors:>9}"
        )
    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Benchmark a kvstore server.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--host", default="127.0.0.1", help="server host")
    parser.add_argument("--port", type=int, default=6379, help="server port")
    parser.add_argument("--ops", type=int, default=10_000, help="operations per workload")
    parser.add_argument("--threads", type=int, default=8, help="concurrent connections")
    parser.add_argument("--value-size", type=int, default=64, help="value size in bytes")
    parser.add_argument("--warmup", type=int, default=200, help="untimed warmup operations")
    parser.add_argument("--timeout", type=float, default=10.0, help="socket timeout in seconds")
    parser.add_argument("--key-prefix", default="bench", help="prefix for generated keys")
    parser.add_argument(
        "--cleanup", action="store_true", help="delete the generated keys when finished"
    )
    args = parser.parse_args()

    if args.ops < 1:
        parser.error("--ops must be at least 1")
    if args.threads < 1:
        parser.error("--threads must be at least 1")
    if args.threads > args.ops:
        parser.error("--threads cannot exceed --ops")
    if args.value_size < 1:
        parser.error("--value-size must be at least 1")
    return args


def main() -> int:
    args = parse_args()
    value = "v" * args.value_size
    prefix = args.key_prefix

    try:
        with Client(host=args.host, port=args.port, timeout=args.timeout) as probe:
            probe.ping()
    except RedisLiteError as exc:
        print(f"error: {exc}", file=sys.stderr)
        print(
            f"hint: is the server running?  ./build/kvstore --port {args.port}",
            file=sys.stderr,
        )
        return 2

    print("kvstore benchmark")
    print(f"  target      {args.host}:{args.port}")
    print(f"  operations  {args.ops:,} per workload")
    print(f"  threads     {args.threads} (one connection each)")
    print(f"  value size  {args.value_size} bytes")
    print()

    # Warm up: fill page caches, let the allocator settle, and force the
    # server's per-connection buffers to their steady-state size, so the first
    # timed operation is not paying for all of that.
    if args.warmup > 0:
        with Client(host=args.host, port=args.port, timeout=args.timeout) as warm:
            for i in range(args.warmup):
                warm.set(f"{prefix}:warmup:{i}", value)
                warm.get(f"{prefix}:warmup:{i}")
            warm.delete(*[f"{prefix}:warmup:{i}" for i in range(args.warmup)])

    results: List[Result] = []

    results.append(
        run_workload(
            "SET",
            lambda client, i: client.set(f"{prefix}:key:{i}", value),
            args.host, args.port, args.ops, args.threads, args.timeout,
        )
    )
    # GET runs over the keys SET just wrote, so every read is a hit. A benchmark
    # that reads missing keys measures the miss path and reports a flattering
    # number for the wrong thing.
    results.append(
        run_workload(
            "GET",
            lambda client, i: client.get(f"{prefix}:key:{i}"),
            args.host, args.port, args.ops, args.threads, args.timeout,
        )
    )
    results.append(
        run_workload(
            "MIXED 1:1",
            lambda client, i: (
                client.set(f"{prefix}:key:{i}", value)
                if i % 2
                else client.get(f"{prefix}:key:{i}")
            ),
            args.host, args.port, args.ops, args.threads, args.timeout,
        )
    )

    print(format_table(results))
    print()

    total_ops = sum(result.ops for result in results)
    total_time = sum(result.seconds for result in results)
    combined = [sample for result in results for sample in result.latencies_ms]
    overall = Result(name="ALL", seconds=total_time, latencies_ms=combined)

    print("Summary")
    print(f"  Total operations   {total_ops:,}")
    print(f"  Total time         {total_time:.3f} s")
    print(f"  Requests/second    {overall.rps:,.0f}")
    print(f"  p50 latency        {overall.p50:.3f} ms")
    print(f"  p99 latency        {overall.p99:.3f} ms")
    print(f"  Mean latency       {overall.mean_ms:.3f} ms")

    if args.cleanup:
        with Client(host=args.host, port=args.port, timeout=args.timeout) as sweeper:
            batch = 500
            for start in range(0, args.ops, batch):
                keys = [f"{prefix}:key:{i}" for i in range(start, min(start + batch, args.ops))]
                sweeper.delete(*keys)
        print(f"\n  cleaned up {args.ops:,} keys")

    failures = sum(result.errors for result in results)
    if failures:
        print(f"\nwarning: {failures} operation(s) failed", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
