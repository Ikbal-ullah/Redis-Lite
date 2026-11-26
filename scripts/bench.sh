#!/usr/bin/env bash
# Runs redis-benchmark against the server in each --fsync mode and prints the
# raw output. Every figure in BENCHMARKS.md is copied from this script's output.
#
#   cmake -S . -B build -DCMAKE_BUILD_TYPE=Release && cmake --build build
#   scripts/bench.sh [path/to/kvstore] [path/to/redis-benchmark]
#
# Request counts differ per mode only so that each run takes seconds, not
# hours: under --fsync always every SET pays a full device flush.

set -euo pipefail

BIN="${1:-build/kvstore}"
RB="${2:-$(command -v redis-benchmark)}"
PORT="${BENCH_PORT:-7700}"
CLIENTS="${BENCH_CLIENTS:-50}"
WORK="$(mktemp -d)"
trap 'kill "$PID" 2>/dev/null || true; rm -rf "$WORK"' EXIT

echo "### Host"
uname -a
if command -v sysctl >/dev/null && sysctl -n machdep.cpu.brand_string >/dev/null 2>&1; then
  echo "CPU: $(sysctl -n machdep.cpu.brand_string), $(sysctl -n hw.ncpu) logical cores, $(( $(sysctl -n hw.memsize) / 1073741824 )) GiB RAM"
  sw_vers 2>/dev/null | tr '\n' ' '; echo
else
  grep -m1 'model name' /proc/cpuinfo || true; nproc
fi
"$RB" --version
echo

run_mode() {
  local mode="$1" n_set="$2" n_get="$3" n_pipe="$4"
  rm -f "$WORK/bench.wal"
  "$BIN" --port "$PORT" --wal "$WORK/bench.wal" --fsync "$mode" >"$WORK/server.log" 2>&1 &
  PID=$!
  for _ in $(seq 50); do "$RB" -p "$PORT" -n 1 -t ping -q >/dev/null 2>&1 && break; sleep 0.1; done

  echo "### --fsync $mode  (clients=$CLIENTS, value=16 B, random keys over 100k)"
  echo "\$ redis-benchmark -p $PORT -c $CLIENTS -n $n_set -r 100000 -d 16 -t set --csv"
  "$RB" -p "$PORT" -c "$CLIENTS" -n "$n_set" -r 100000 -d 16 -t set --csv 2>&1
  echo "\$ redis-benchmark -p $PORT -c $CLIENTS -n $n_get -r 100000 -d 16 -t get --csv"
  "$RB" -p "$PORT" -c "$CLIENTS" -n "$n_get" -r 100000 -d 16 -t get --csv 2>&1
  echo "\$ redis-benchmark -p $PORT -c $CLIENTS -n $n_pipe -r 100000 -d 16 -t set -P 16 --csv"
  "$RB" -p "$PORT" -c "$CLIENTS" -n "$n_pipe" -r 100000 -d 16 -t set -P 16 --csv 2>&1
  kill -TERM "$PID"; wait "$PID" || true
  echo "server log: $(tr '\n' ' ' <"$WORK/server.log")"
  echo "WAL size after run: $(wc -c <"$WORK/bench.wal" | tr -d ' ') bytes"
  echo
}

run_mode no      500000 500000 2000000
run_mode group    50000 500000  500000
run_mode always    5000 500000    5000
