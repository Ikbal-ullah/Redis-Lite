// Keyspace semantics and command dispatch, with the clock passed explicitly.

#include <limits>
#include <string>

#include "harness.hpp"
#include "kvstore/database.hpp"
#include "kvstore/storage.hpp"

using namespace kvstore;

namespace {

std::string run(Database& db, std::initializer_list<std::string_view> args, std::int64_t now) {
  Command c;
  c.args.assign(args.begin(), args.end());
  c.name = c.args.front();
  c.kind = classify(c.name);
  std::string out;
  execute_command(c, db, out, now);
  return out;
}

}  // namespace

TEST(set_get_del_round_trip) {
  Storage s;
  s.set("a", "1", 0);
  CHECK_EQ(*s.get("a", 0), std::string("1"));
  CHECK(s.erase("a"));
  CHECK(s.get("a", 0) == nullptr);
  CHECK(!s.erase("a"));
}

TEST(lazy_expiry_hides_and_evicts_dead_keys) {
  Storage s;
  s.set("k", "v", 1000);
  CHECK(s.get("k", 999) != nullptr);
  CHECK(s.get("k", 1000) == nullptr);
  CHECK_EQ(s.size(), 0u);  // evicted on read, not just hidden
  CHECK_EQ(s.volatile_keys(), 0u);
}

TEST(plain_set_clears_ttl_and_index) {
  Storage s;
  s.set("k", "v", 5000);
  s.set("k", "v2", 0);
  CHECK_EQ(s.volatile_keys(), 0u);
  CHECK_EQ(s.ttl_ms("k", 100), -1);
}

TEST(active_expiry_is_bounded_by_budget) {
  Storage s;
  for (int i = 0; i < 1000; ++i) s.set(testing::cat("k", i), "v", 100 + i);
  s.set("forever", "v", 0);
  CHECK_EQ(s.active_expire(50, 200), 0u);      // nothing due yet
  CHECK_EQ(s.active_expire(10000, 200), 200u);  // capped at the budget
  CHECK_EQ(s.size(), 801u);
  CHECK_EQ(s.active_expire(10000, 100000), 800u);
  CHECK_EQ(s.size(), 1u);
  CHECK(s.get("forever", 10000) != nullptr);
}

TEST(active_expiry_survives_rehashing) {
  // The index stores views into map nodes; rehashing must not invalidate them.
  Storage s;
  for (int i = 0; i < 50000; ++i) s.set(testing::cat("key:", i), "v", i % 2 ? 10 : 0);
  CHECK_EQ(s.active_expire(11, 1000000), 25000u);
  CHECK_EQ(s.size(), 25000u);
}

TEST(set_with_ex_and_px_sets_deadlines) {
  Database db(nullptr);
  CHECK_EQ(run(db, {"SET", "a", "1", "EX", "10"}, 1000), std::string("+OK\r\n"));
  CHECK_EQ(run(db, {"SET", "b", "1", "px", "1500"}, 1000), std::string("+OK\r\n"));
  CHECK_EQ(run(db, {"TTL", "a"}, 1000), std::string(":10\r\n"));
  CHECK_EQ(run(db, {"TTL", "b"}, 1000), std::string(":2\r\n"));  // 1.5 s rounds to 2
  CHECK_EQ(run(db, {"GET", "b"}, 2500), std::string("$-1\r\n"));
  CHECK_EQ(run(db, {"TTL", "b"}, 2500), std::string(":-2\r\n"));
}

TEST(set_rejects_bad_options) {
  Database db(nullptr);
  CHECK_EQ(run(db, {"SET", "a", "1", "EX", "0"}, 0),
           std::string("-ERR invalid expire time in 'set' command\r\n"));
  CHECK_EQ(run(db, {"SET", "a", "1", "EX", "abc"}, 0),
           std::string("-ERR value is not an integer or out of range\r\n"));
  CHECK_EQ(run(db, {"SET", "a", "1", "XX", "1"}, 0), std::string("-ERR syntax error\r\n"));
  CHECK_EQ(run(db, {"SET", "a", "1", "EX"}, 0), std::string("-ERR syntax error\r\n"));
  CHECK_EQ(run(db, {"SET", "a"}, 0),
           std::string("-ERR wrong number of arguments for 'set' command\r\n"));
  CHECK_EQ(run(db, {"SET", "a", "1", "EX", "9223372036854775807"}, 1000),
           std::string("-ERR invalid expire time in 'set' command\r\n"));
}

TEST(expire_overflow_is_an_error_not_ub) {
  // Audit A2: the baseline computed now + secs * 1000 with signed overflow.
  Database db(nullptr);
  run(db, {"SET", "k", "v"}, 1000);
  CHECK_EQ(run(db, {"EXPIRE", "k", "9223372036854775807"}, 1000),
           std::string("-ERR invalid expire time in 'expire' command\r\n"));
  CHECK_EQ(run(db, {"GET", "k"}, 1000), std::string("$1\r\nv\r\n"));
}

TEST(expire_ttl_and_del_semantics) {
  Database db(nullptr);
  CHECK_EQ(run(db, {"EXPIRE", "missing", "10"}, 0), std::string(":0\r\n"));
  run(db, {"SET", "k", "v"}, 0);
  CHECK_EQ(run(db, {"TTL", "k"}, 0), std::string(":-1\r\n"));
  CHECK_EQ(run(db, {"EXPIRE", "k", "5"}, 0), std::string(":1\r\n"));
  CHECK_EQ(run(db, {"TTL", "k"}, 0), std::string(":5\r\n"));
  CHECK_EQ(run(db, {"EXPIRE", "k", "-1"}, 0), std::string(":1\r\n"));  // deletes
  CHECK_EQ(run(db, {"GET", "k"}, 0), std::string("$-1\r\n"));
  run(db, {"SET", "a", "1"}, 0);
  run(db, {"SET", "b", "1"}, 0);
  CHECK_EQ(run(db, {"DEL", "a", "b", "c"}, 0), std::string(":2\r\n"));
}

TEST(unknown_command_and_arity_errors) {
  Database db(nullptr);
  CHECK_EQ(run(db, {"FLUSHALL"}, 0), std::string("-ERR unknown command 'FLUSHALL'\r\n"));
  CHECK_EQ(run(db, {"GET"}, 0), std::string("-ERR wrong number of arguments for 'get' command\r\n"));
  CHECK_EQ(run(db, {"PING", "hi"}, 0), std::string("$2\r\nhi\r\n"));
}
