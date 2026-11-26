# Benchmarks and Verification: raw output

Everything below was produced on this machine by the commands shown, on
2026-11-26, against the final code on this branch. Output is pasted verbatim
(temp-dir paths shortened to `$TMP`). Nothing is estimated. To regenerate:
`scripts/bench.sh build/kvstore`.

- **Host:** Apple M2 Pro (10 cores), 16 GiB RAM, macOS 26.6.2, APFS on the internal SSD
- **Compiler:** Apple clang 21.0.0; server binary `-O3` Release; tests built Debug + ASan/UBSan
- **Load generator:** redis-benchmark 8.10.1 on the same host over loopback (it shares CPU with the server)
- **Variance:** loopback numbers on a laptop move noticeably between runs. An
  earlier run on a pre-final build measured `group` SET at 2,673 ops/s. Only the
  final run below is cited anywhere.

## Summary (copied from the raw output below)

| `--fsync` | SET ops/s | SET p50 | SET p99 | GET ops/s | GET p50 | GET p99 | SET ops/s, `-P 16` |
|---|---:|---:|---:|---:|---:|---:|---:|
| `no` | 97,847.36 | 0.367 ms | 1.151 ms | 101,317.12 | 0.271 ms | 1.047 ms | 294,637.59 |
| `group` | 3,346.94 | 14.127 ms | 27.199 ms | 82,767.76 | 0.311 ms | 1.343 ms | 15,893.20 |
| `always` | 513.35 | 94.207 ms | 139.135 ms | 107,273.12 | 0.263 ms | 0.927 ms | 602.21 |

50 clients, 16-byte values, random keys over a 100k keyspace.

How to read this:
- **The device flush sets the price of durability.** One `F_FULLFSYNC` on this
  SSD costs a p50 of 2000.8 µs (probe below), so a single-threaded server that
  flushes every SET is capped near 1 / 2 ms ≈ 500 SET/s. The measured figure is
  513.35. Plain `fsync()` costs 21.4 µs, but on macOS it does
  not flush the drive cache, so it is not used.
- **Group commit** acknowledges a write only after an fsync covers it (the same
  guarantee as `always`), but shares one flush across up to 64 writes:
  3,346.94 vs 513.35 SET/s (6.5×), and
  with `-P 16` pipelining 15,893.20 vs 602.21
  (26.4×). Its p50 of 14.127 ms is mostly the 10 ms batching
  window: with 50 clients the 64-op trigger is never reached, so the window
  always fires.
- **GETs never wait on the disk** in any mode: sub-ms p50 everywhere.
- **SIGKILL tests cover process crashes, not power loss.** The page cache
  survives a SIGKILL, so the chaos test proves write→ack ordering and the
  recovery code, not the drive's flush behaviour.

## 1. fsync cost on this disk

Probe: 300 × (64-byte append + sync). It is a throwaway program and is not
shipped in the repo.

```
fsync()      n=300  p50=21.4 us  p99=39.5 us  max=156.6 us
F_FULLFSYNC  n=300  p50=2000.8 us  p99=7032.4 us  max=17370.9 us
```

## 2. redis-benchmark, all three modes (`scripts/bench.sh build/kvstore`)

```
### Host
Darwin Recorder 25.6.0 Darwin Kernel Version 25.6.0: Fri Jul 31 19:18:48 PDT 2026; root:xnu-12377.161.14~5/RELEASE_ARM64_T6020 arm64
CPU: Apple M2 Pro, 10 logical cores, 16 GiB RAM
ProductName:		macOS ProductVersion:		26.6.2 BuildVersion:		25G83 
redis-benchmark 8.10.1

### --fsync no  (clients=50, value=16 B, random keys over 100k)
$ redis-benchmark -p 7700 -c 50 -n 500000 -r 100000 -d 16 -t set --csv
WARNING: Could not fetch server CONFIG
"test","rps","avg_latency_ms","min_latency_ms","p50_latency_ms","p95_latency_ms","p99_latency_ms","max_latency_ms"
"SET","97847.36","0.422","0.024","0.367","0.839","1.151","16.575"
$ redis-benchmark -p 7700 -c 50 -n 500000 -r 100000 -d 16 -t get --csv
WARNING: Could not fetch server CONFIG
"test","rps","avg_latency_ms","min_latency_ms","p50_latency_ms","p95_latency_ms","p99_latency_ms","max_latency_ms"
"GET","101317.12","0.340","0.040","0.271","0.735","1.047","4.863"
$ redis-benchmark -p 7700 -c 50 -n 2000000 -r 100000 -d 16 -t set -P 16 --csv
WARNING: Could not fetch server CONFIG
"test","rps","avg_latency_ms","min_latency_ms","p50_latency_ms","p95_latency_ms","p99_latency_ms","max_latency_ms"
"SET","294637.59","2.699","0.232","2.327","4.567","7.519","41.855"
server log: recovered 0 record(s), 0 live key(s) from $TMP/bench.wal (0 bytes) listening on 127.0.0.1:7700 fsync=no shut down cleanly 
WAL size after run: 132500000 bytes

### --fsync group  (clients=50, value=16 B, random keys over 100k)
$ redis-benchmark -p 7700 -c 50 -n 50000 -r 100000 -d 16 -t set --csv
WARNING: Could not fetch server CONFIG
"test","rps","avg_latency_ms","min_latency_ms","p50_latency_ms","p95_latency_ms","p99_latency_ms","max_latency_ms"
"SET","3346.94","14.570","8.464","14.127","16.231","27.199","113.663"
$ redis-benchmark -p 7700 -c 50 -n 500000 -r 100000 -d 16 -t get --csv
WARNING: Could not fetch server CONFIG
"test","rps","avg_latency_ms","min_latency_ms","p50_latency_ms","p95_latency_ms","p99_latency_ms","max_latency_ms"
"GET","82767.76","0.397","0.048","0.311","0.855","1.343","6.303"
$ redis-benchmark -p 7700 -c 50 -n 500000 -r 100000 -d 16 -t set -P 16 --csv
WARNING: Could not fetch server CONFIG
"test","rps","avg_latency_ms","min_latency_ms","p50_latency_ms","p95_latency_ms","p99_latency_ms","max_latency_ms"
"SET","15893.20","50.137","0.896","35.551","147.071","286.207","611.327"
server log: recovered 0 record(s), 0 live key(s) from $TMP/bench.wal (0 bytes) listening on 127.0.0.1:7700 fsync=group shut down cleanly 
WAL size after run: 29150000 bytes

### --fsync always  (clients=50, value=16 B, random keys over 100k)
$ redis-benchmark -p 7700 -c 50 -n 5000 -r 100000 -d 16 -t set --csv
WARNING: Could not fetch server CONFIG
"test","rps","avg_latency_ms","min_latency_ms","p50_latency_ms","p95_latency_ms","p99_latency_ms","max_latency_ms"
"SET","513.35","96.774","3.192","94.207","130.047","139.135","255.103"
$ redis-benchmark -p 7700 -c 50 -n 500000 -r 100000 -d 16 -t get --csv
WARNING: Could not fetch server CONFIG
"test","rps","avg_latency_ms","min_latency_ms","p50_latency_ms","p95_latency_ms","p99_latency_ms","max_latency_ms"
"GET","107273.12","0.308","0.040","0.263","0.591","0.927","18.959"
$ redis-benchmark -p 7700 -c 50 -n 5000 -r 100000 -d 16 -t set -P 16 --csv
WARNING: Could not fetch server CONFIG
"test","rps","avg_latency_ms","min_latency_ms","p50_latency_ms","p95_latency_ms","p99_latency_ms","max_latency_ms"
"SET","602.21","1224.676","23.904","1259.519","1892.351","2291.711","2938.879"
server log: recovered 0 record(s), 0 live key(s) from $TMP/bench.wal (0 bytes) listening on 127.0.0.1:7700 fsync=always shut down cleanly 
WAL size after run: 530424 bytes
```

## 3. Compaction on a bloated log (`--fsync no`, 1M overwrites of 100k keys)

```
$ redis-benchmark -p 7710 -c 50 -n 1000000 -r 100000 -d 16 -t set -P 16 -q
SET: 400000.00 requests per second, p50=1.879 msec
WAL before COMPACT: 53000000 bytes
$ time redis-cli -p 7710 COMPACT
OK
redis-cli -p 7710 COMPACT  0.00s user 0.00s system 10% cpu 0.039 total
WAL after COMPACT: 5299470 bytes
--- restart after compaction:
recovered 99990 record(s), 99990 live key(s) from $TMP/c.wal (5299470 bytes)
listening on 127.0.0.1:7710 fsync=no
$ redis-cli -p 7710 GET key:000000012345 -> VXKeHogKgJ=[5V9_
```

## 4. Chaos test: 20× SIGKILL under `--fsync always` + torn tail + mid-file corruption

`KVSTORE_BIN=build-asan/kvstore .venv/bin/python tests/chaos_crash_test.py --iterations 20 --fsync always`
(server built with ASan + UBSan)

```
seed 330014941
== Phase 1: 20 SIGKILL iterations, --fsync always, 8 writers ==
iter  1: verified     0 keys, lost 0, acked this round   387, in-flight at kill 8 | start: recovered 0 record(s), 0 live key(s) from $TMP/chaos.wal (0 bytes)
iter  2: verified   325 keys, lost 0, acked this round   479, in-flight at kill 8 | start: recovered 388 record(s), 252 live key(s) from $TMP/chaos.wal (18327 bytes)
iter  3: verified   736 keys, lost 0, acked this round   262, in-flight at kill 8 | start: recovered 868 record(s), 582 live key(s) from $TMP/chaos.wal (41277 bytes)
iter  4: verified   966 keys, lost 0, acked this round   137, in-flight at kill 8 | start: recovered 1130 record(s), 764 live key(s) from $TMP/chaos.wal (53744 bytes)
iter  5: verified  1088 keys, lost 0, acked this round   158, in-flight at kill 8 | start: recovered 1268 record(s), 858 live key(s) from $TMP/chaos.wal (60206 bytes)
iter  6: verified  1235 keys, lost 0, acked this round   213, in-flight at kill 8 | start: recovered 1427 record(s), 981 live key(s) from $TMP/chaos.wal (67858 bytes)
iter  7: verified  1430 keys, lost 0, acked this round   287, in-flight at kill 8 | start: recovered 1641 record(s), 1147 live key(s) from $TMP/chaos.wal (78227 bytes)
iter  8: verified  1681 keys, lost 0, acked this round   231, in-flight at kill 8 | start: recovered 1929 record(s), 1351 live key(s) from $TMP/chaos.wal (91989 bytes)
iter  9: verified  1887 keys, lost 0, acked this round   142, in-flight at kill 8 | start: recovered 2161 record(s), 1517 live key(s) from $TMP/chaos.wal (103052 bytes)
iter 10: verified  2018 keys, lost 0, acked this round   349, in-flight at kill 8 | start: recovered 2304 record(s), 1622 live key(s) from $TMP/chaos.wal (109845 bytes)
iter 11: verified  2330 keys, lost 0, acked this round   443, in-flight at kill 8 | start: recovered 2654 record(s), 1882 live key(s) from $TMP/chaos.wal (127418 bytes)
iter 12: verified  2706 keys, lost 0, acked this round   318, in-flight at kill 8 | start: recovered 3098 record(s), 2182 live key(s) from $TMP/chaos.wal (149363 bytes)
iter 13: verified  2992 keys, lost 0, acked this round   516, in-flight at kill 8 | start: recovered 3417 record(s), 2425 live key(s) from $TMP/chaos.wal (165438 bytes)
iter 14: verified  3442 keys, lost 0, acked this round   349, in-flight at kill 8 | start: recovered 3933 record(s), 2793 live key(s) from $TMP/chaos.wal (191232 bytes)
iter 15: verified  3749 keys, lost 0, acked this round   499, in-flight at kill 8 | start: recovered 4283 record(s), 3045 live key(s) from $TMP/chaos.wal (208703 bytes)
iter 16: verified  4179 keys, lost 0, acked this round   448, in-flight at kill 8 | start: recovered 4783 record(s), 3393 live key(s) from $TMP/chaos.wal (233575 bytes)
iter 17: verified  4579 keys, lost 0, acked this round   439, in-flight at kill 8 | start: recovered 5232 record(s), 3732 live key(s) from $TMP/chaos.wal (256218 bytes)
iter 18: verified  4951 keys, lost 0, acked this round   465, in-flight at kill 8 | start: recovered 5672 record(s), 4024 live key(s) from $TMP/chaos.wal (277899 bytes)
iter 19: verified  5338 keys, lost 0, acked this round   387, in-flight at kill 8 | start: recovered 6138 record(s), 4324 live key(s) from $TMP/chaos.wal (300750 bytes)
iter 20: verified  5680 keys, lost 0, acked this round   135, in-flight at kill 8 | start: recovered 6526 record(s), 4608 live key(s) from $TMP/chaos.wal (320191 bytes)
final : verified 5800 keys after 20 crashes; total acknowledged writes 6644; LOST ACKNOWLEDGED WRITES: 0
== Phase 2: torn-tail garbage appended to the WAL ==
garbage    1 B: recovered 6662 record(s), 4698 live key(s) from $TMP/chaos.wal (326774 bytes, truncated torn tail of 1 bytes) | verified 5800 keys, lost 0
garbage    7 B: recovered 6663 record(s), 4699 live key(s) from $TMP/chaos.wal (326812 bytes, truncated torn tail of 7 bytes) | verified 5801 keys, lost 0
garbage   20 B: recovered 6664 record(s), 4700 live key(s) from $TMP/chaos.wal (326850 bytes, truncated torn tail of 20 bytes) | verified 5802 keys, lost 0
garbage   21 B: recovered 6665 record(s), 4701 live key(s) from $TMP/chaos.wal (326889 bytes, truncated torn tail of 21 bytes) | verified 5803 keys, lost 0
garbage   64 B: recovered 6666 record(s), 4702 live key(s) from $TMP/chaos.wal (326928 bytes, truncated torn tail of 64 bytes) | verified 5804 keys, lost 0
garbage  333 B: recovered 6667 record(s), 4703 live key(s) from $TMP/chaos.wal (326967 bytes, truncated torn tail of 333 bytes) | verified 5805 keys, lost 0
garbage 4096 B: recovered 6668 record(s), 4704 live key(s) from $TMP/chaos.wal (327007 bytes, truncated torn tail of 4096 bytes) | verified 5806 keys, lost 0
torn tail: all acknowledged writes intact, log appendable after every truncation
== Phase 3: single-byte corruption in the middle of the WAL ==
flipped byte 163524 of 327048: exit code 3: fatal: wal corruption at offset 163490 followed by a valid record at offset 163544; refusing to truncate $TMP/chaos.wal
mid-file corruption: refused to start, WAL byte-for-byte unchanged
ALL CHAOS CHECKS PASSED
```

The same test under `--fsync group` (`--iterations 20`, run right after):

```
final : verified 5812 keys after 20 crashes; total acknowledged writes 6640; LOST ACKNOWLEDGED WRITES: 0
ALL CHAOS CHECKS PASSED
```

## 5. Unit + socket tests (`-Wall -Wextra -Werror -fsanitize=address,undefined -fno-sanitize-recover=all`)

```
  PASS  resp_array_is_parsed
  PASS  inline_command_is_parsed
  PASS  command_names_are_case_insensitive
  PASS  every_proper_prefix_is_incomplete
  PASS  pipelined_commands_in_one_buffer_parse_in_order
  PASS  hostile_lengths_are_rejected
  PASS  bulk_string_without_trailing_crlf_is_rejected
  PASS  binary_values_are_preserved
  PASS  reply_encoders_match_the_wire_format
  PASS  set_get_del_round_trip
  PASS  lazy_expiry_hides_and_evicts_dead_keys
  PASS  plain_set_clears_ttl_and_index
  PASS  active_expiry_is_bounded_by_budget
  PASS  active_expiry_survives_rehashing
  PASS  set_with_ex_and_px_sets_deadlines
  PASS  set_rejects_bad_options
  PASS  expire_overflow_is_an_error_not_ub
  PASS  expire_ttl_and_del_semantics
  PASS  unknown_command_and_arity_errors
  PASS  frame_layout_is_exactly_as_specified
  PASS  log_replays_into_an_identical_keyspace
  PASS  keys_that_expired_during_downtime_stay_dead
  PASS  torn_tail_at_every_byte_offset_is_truncated
  PASS  garbage_tail_is_truncated_and_log_stays_appendable
  PASS  zero_filled_tail_is_treated_as_torn
  PASS  mid_file_corruption_fails_hard_and_leaves_the_file_alone
  PASS  corrupted_length_field_mid_file_fails_hard
  PASS  compaction_is_atomic_and_preserves_state
  PASS  stale_compaction_temp_file_is_ignored_and_removed
  PASS  absent_log_is_an_empty_log
  PASS  group_policy_defers_fsync_to_the_caller
  PASS  server_answers_resp_and_inline_commands
  PASS  server_reassembles_a_command_split_byte_by_byte
  PASS  server_answers_a_pipeline_sent_in_one_write
  PASS  protocol_error_replies_then_closes
  PASS  half_closed_client_still_gets_every_reply
  PASS  no_descriptor_leaks_across_connection_churn
  PASS  state_survives_a_restart
  PASS  compact_over_the_wire_shrinks_the_log
  PASS  group_commit_batches_fsyncs_and_still_acks_every_write
  PASS  group_commit_preserves_reply_order_with_reads
  PASS  many_concurrent_clients_are_served
  PASS  slow_reader_gets_backpressure_not_unbounded_memory
  PASS  fast_reader_resumes_after_backpressure_without_stalling

44 passed, 0 failed
```

```
$ ctest --test-dir build-asan --output-on-failure
100% tests passed out of 1

Total Test time (real) =   3.54 sec
```

Stability: the suite was run 30 times on an idle machine and 20 times next to
12 busy-loop processes: 0 failed runs. The loaded runs found a real bug, the
stall fixed in `Server::pump`, which the test
`fast_reader_resumes_after_backpressure_without_stalling` now pins down.

Linux: the same suite builds with 0 warnings and passes 44/44 under GCC 12
(Debian bookworm) in the Docker build stage, ASan + UBSan + LeakSanitizer.

## 6. redis-py 8.2.0 compatibility (all tests × `no`/`group`/`always`)

```
$ KVSTORE_BIN=build-asan/kvstore .venv/bin/pytest -q tests/test_redis_py.py
24 passed in 9.15s
```
