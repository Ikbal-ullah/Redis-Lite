// Test suite for the key-value store.
//
// The harness is fifty lines of self-registering test functions rather than a
// dependency, which keeps the build hermetic: `cmake && ctest` works on a
// machine with no package manager and no network.
//
// Coverage is deliberately weighted towards the things that are hard to get
// right and silent when wrong: incremental protocol framing, log durability
// across a simulated crash, and the ordering guarantee between the log and
// memory under concurrent writers.

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <cstring>
#include <exception>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "kvstore/protocol.hpp"
#include "kvstore/server.hpp"
#include "kvstore/storage.hpp"
#include "kvstore/wal.hpp"

// ---------------------------------------------------------------------------
// Minimal test harness
// ---------------------------------------------------------------------------

namespace testing {

struct Failure : std::exception {
  explicit Failure(std::string message) : text(std::move(message)) {}
  const char* what() const noexcept override { return text.c_str(); }
  std::string text;
};

using TestFn = void (*)();

struct Case {
  const char* name;
  TestFn fn;
};

inline std::vector<Case>& registry() {
  static std::vector<Case> cases;
  return cases;
}

struct Registrar {
  Registrar(const char* name, TestFn fn) { registry().push_back({name, fn}); }
};

template <typename T>
std::string show(const T& value) {
  std::ostringstream out;
  out << value;
  return out.str();
}

inline std::string show(bool value) { return value ? "true" : "false"; }

}  // namespace testing

#define TEST(name)                                              \
  static void name();                                           \
  static ::testing::Registrar registrar_##name(#name, name);    \
  static void name()

#define CHECK(condition)                                                          \
  do {                                                                            \
    if (!(condition)) {                                                           \
      throw ::testing::Failure(std::string(__FILE__) + ":" + std::to_string(__LINE__) + \
                               ": expected " #condition);                         \
    }                                                                             \
  } while (false)

// Both operands are copied, not bound by reference. `storage.get(k).value()` is
// a reference *into* a temporary std::optional; a const& would outlive it by
// the time the comparison runs.
#define CHECK_EQ(actual, expected)                                                    \
  do {                                                                                \
    const auto a_ = (actual);                                                         \
    const auto e_ = (expected);                                                       \
    if (!(a_ == e_)) {                                                                \
      throw ::testing::Failure(std::string(__FILE__) + ":" + std::to_string(__LINE__) + \
                               ": " #actual " == " + ::testing::show(a_) +            \
                               ", expected " + ::testing::show(e_));                  \
    }                                                                                 \
  } while (false)

// ---------------------------------------------------------------------------
// Shared fixtures
// ---------------------------------------------------------------------------

namespace {

using namespace kvstore;

// A log file that deletes itself when the test scope ends, however the scope is
// left. Test fixtures leak more often than production code, because a failing
// assertion takes an early exit that a hand-written cleanup call never sees.
class TempLog {
 public:
  TempLog() {
    static std::atomic<int> counter{0};
    auto name = "kvstore-test-" + std::to_string(::getpid()) + "-" +
                std::to_string(counter.fetch_add(1)) + ".log";
    path_ = std::filesystem::temp_directory_path() / name;
    std::error_code ignored;
    std::filesystem::remove(path_, ignored);
  }
  ~TempLog() {
    std::error_code ignored;
    std::filesystem::remove(path_, ignored);
  }

  TempLog(const TempLog&) = delete;
  TempLog& operator=(const TempLog&) = delete;

  std::string path() const { return path_.string(); }
  std::uintmax_t size() const {
    std::error_code ec;
    const auto n = std::filesystem::file_size(path_, ec);
    return ec ? 0 : n;
  }
  void truncate(std::uintmax_t bytes) const {
    std::filesystem::resize_file(path_, bytes);
  }
  void corrupt_byte_at(std::uintmax_t offset) const {
    std::fstream file(path_, std::ios::in | std::ios::out | std::ios::binary);
    file.seekp(static_cast<std::streamoff>(offset));
    char flipped = '\xFF';
    file.write(&flipped, 1);
  }

 private:
  std::filesystem::path path_;
};

std::string parse_one(std::string_view input, Command& out) {
  const ParseResult result = parse_command(input, out);
  switch (result.status) {
    case ParseStatus::kOk: return "ok";
    case ParseStatus::kIncomplete: return "incomplete";
    case ParseStatus::kError: return "error";
  }
  return "?";
}

// --- TCP client helpers ----------------------------------------------------

int dial(std::uint16_t port) {
  const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  CHECK(fd >= 0);

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port);
  CHECK(::inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr) == 1);
  CHECK(::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0);

  // A timeout, so a framing bug surfaces as a failed assertion instead of a
  // test run that hangs until CI kills it.
  timeval timeout{};
  timeout.tv_sec = 5;
  ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
  return fd;
}

void send_all(int fd, std::string_view bytes) {
  std::size_t sent = 0;
  while (sent < bytes.size()) {
    const ssize_t n = ::send(fd, bytes.data() + sent, bytes.size() - sent, 0);
    if (n < 0 && errno == EINTR) continue;
    CHECK(n > 0);
    sent += static_cast<std::size_t>(n);
  }
}

std::string recv_exact(int fd, std::size_t count) {
  std::string out;
  out.reserve(count);
  char chunk[4096];
  while (out.size() < count) {
    const ssize_t n = ::recv(fd, chunk, std::min(sizeof(chunk), count - out.size()), 0);
    if (n < 0 && errno == EINTR) continue;
    CHECK(n > 0);
    out.append(chunk, static_cast<std::size_t>(n));
  }
  return out;
}

// Reads one CRLF-terminated reply line. Error replies are variable length, so
// asking for a fixed byte count would either block forever or leave the tail in
// the socket to corrupt the next read.
std::string recv_line(int fd) {
  std::string out;
  while (out.size() < 2 || out.compare(out.size() - 2, 2, "\r\n") != 0) {
    out += recv_exact(fd, 1);
  }
  return out;
}

// Brings up a real server on an ephemeral port and tears it down on scope exit.
class ServerFixture {
 public:
  explicit ServerFixture(Storage& storage, int io_threads = 2) {
    ServerConfig config;
    config.bind_address = "127.0.0.1";
    config.port = 0;  // ask the kernel for any free port
    config.io_threads = io_threads;
    config.purge_interval_ms = 50;
    server_ = std::make_unique<Server>(config, storage);
    server_->start();
  }
  ~ServerFixture() { server_->stop(); }

  std::uint16_t port() const { return server_->port(); }

 private:
  std::unique_ptr<Server> server_;
};

}  // namespace

// ---------------------------------------------------------------------------
// Protocol
// ---------------------------------------------------------------------------

TEST(resp_array_is_parsed) {
  Command command;
  const std::string_view wire = "*3\r\n$3\r\nSET\r\n$5\r\nhello\r\n$5\r\nworld\r\n";
  CHECK_EQ(parse_one(wire, command), std::string("ok"));
  CHECK(command.kind == CommandKind::kSet);
  CHECK_EQ(command.arity(), std::size_t{3});
  CHECK_EQ(std::string(command.args[1]), std::string("hello"));
  CHECK_EQ(std::string(command.args[2]), std::string("world"));
}

TEST(inline_command_is_parsed) {
  Command command;
  CHECK_EQ(parse_one("GET foo\r\n", command), std::string("ok"));
  CHECK(command.kind == CommandKind::kGet);
  CHECK_EQ(command.arity(), std::size_t{2});
  CHECK_EQ(std::string(command.args[1]), std::string("foo"));

  // Bare newline, no carriage return -- what a terminal sends.
  CHECK_EQ(parse_one("PING\n", command), std::string("ok"));
  CHECK(command.kind == CommandKind::kPing);
}

TEST(command_names_are_case_insensitive) {
  CHECK(classify("get") == CommandKind::kGet);
  CHECK(classify("GeT") == CommandKind::kGet);
  CHECK(classify("EXPIRE") == CommandKind::kExpire);
  CHECK(classify("nope") == CommandKind::kUnknown);
}

// The core framing property: a command split across arbitrarily many TCP
// segments must parse identically to one delivered whole. Feeding it one byte
// at a time is the harshest version of that test.
TEST(partial_input_is_incomplete_until_the_last_byte) {
  const std::string wire = "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
  Command command;
  for (std::size_t prefix = 1; prefix < wire.size(); ++prefix) {
    const std::string_view partial(wire.data(), prefix);
    CHECK_EQ(parse_one(partial, command), std::string("incomplete"));
  }
  CHECK_EQ(parse_one(wire, command), std::string("ok"));
  CHECK_EQ(command.arity(), std::size_t{3});
}

TEST(pipelined_commands_are_parsed_one_at_a_time) {
  const std::string wire = "PING\r\nPING\r\n*1\r\n$4\r\nPING\r\n";
  std::string_view rest = wire;
  int seen = 0;
  Command command;
  while (!rest.empty()) {
    const ParseResult result = parse_command(rest, command);
    CHECK(result.status == ParseStatus::kOk);
    CHECK(command.kind == CommandKind::kPing);
    rest.remove_prefix(result.consumed);
    ++seen;
  }
  CHECK_EQ(seen, 3);
}

TEST(hostile_lengths_are_rejected) {
  Command command;
  // A bulk length larger than the cap: refused outright rather than reserved.
  CHECK_EQ(parse_one("*1\r\n$999999999999\r\n", command), std::string("error"));
  // A non-numeric multibulk count.
  CHECK_EQ(parse_one("*abc\r\n", command), std::string("error"));
  // An element that does not start with '$'.
  CHECK_EQ(parse_one("*1\r\n+PING\r\n", command), std::string("error"));
  // A digit stream with no terminator must not be buffered indefinitely.
  CHECK_EQ(parse_one("*11111111111111111111111111111111111111", command), std::string("error"));
}

TEST(binary_safe_values_survive_the_round_trip) {
  // A bulk string carries an explicit length, so a value containing CRLF and a
  // NUL byte is fine. This is precisely why inline commands are not enough.
  const std::string value("a\r\nb\0c", 6);
  std::string wire = "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$6\r\n";
  wire += value;
  wire += "\r\n";

  Command command;
  CHECK_EQ(parse_one(wire, command), std::string("ok"));
  CHECK_EQ(command.args[2].size(), std::size_t{6});
  CHECK(std::string(command.args[2]) == value);
}

TEST(reply_encoders_match_the_wire_format) {
  std::string out;
  reply_simple(out, "OK");
  reply_error(out, "ERR nope");
  reply_bulk(out, "hi");
  reply_nil(out);
  reply_integer(out, -7);
  CHECK_EQ(out, std::string("+OK\r\n-ERR nope\r\n$2\r\nhi\r\n$-1\r\n:-7\r\n"));
}

// ---------------------------------------------------------------------------
// Storage
// ---------------------------------------------------------------------------

TEST(set_get_del_round_trip) {
  Storage storage;
  CHECK(!storage.get("missing").has_value());

  storage.set("k", "v1");
  CHECK_EQ(storage.get("k").value(), std::string("v1"));

  storage.set("k", "v2");  // overwrite in place
  CHECK_EQ(storage.get("k").value(), std::string("v2"));
  CHECK_EQ(storage.size(), std::size_t{1});

  CHECK(storage.del("k"));
  CHECK(!storage.del("k"));  // second delete reports nothing removed
  CHECK(!storage.get("k").has_value());
}

TEST(expire_hides_the_key_once_the_deadline_passes) {
  Storage storage;
  storage.set("k", "v");
  CHECK(!storage.expire("absent", 10));
  CHECK(storage.expire("k", 1));
  CHECK(storage.get("k").has_value());

  // Deadline in the past: the key must vanish immediately.
  storage.set("j", "v");
  CHECK(storage.expire("j", -1));
  CHECK(!storage.get("j").has_value());
}

TEST(a_plain_set_clears_an_existing_ttl) {
  Storage storage;
  storage.set("k", "v");
  CHECK(storage.expire("k", 1));
  storage.set("k", "v2");
  std::this_thread::sleep_for(std::chrono::milliseconds(1100));
  // If SET had not cleared the TTL, the key would be gone by now.
  CHECK_EQ(storage.get("k").value(), std::string("v2"));
}

TEST(purge_reclaims_memory_from_keys_nobody_reads) {
  Storage storage;
  storage.set("k", "v");
  CHECK(storage.expire("k", -5) == true);  // deleted outright
  storage.set("j", "v");
  CHECK(storage.expire("j", 1));
  CHECK_EQ(storage.purge_expired(), std::size_t{0});  // not due yet
}

// ---------------------------------------------------------------------------
// Write-ahead log
// ---------------------------------------------------------------------------

TEST(log_replays_into_an_identical_store) {
  TempLog log;
  {
    Wal wal(log.path(), SyncPolicy::kAlways);
    wal.open();
    Storage storage(&wal);
    storage.set("alpha", "1");
    storage.set("beta", "2");
    storage.set("alpha", "3");  // last writer wins on replay
    storage.del("beta");
    storage.set("gamma", "4");
  }

  Wal reopened(log.path(), SyncPolicy::kAlways);
  Storage restored(&reopened);
  const std::size_t applied = restored.load_from_wal();
  CHECK_EQ(applied, std::size_t{5});
  CHECK_EQ(restored.get("alpha").value(), std::string("3"));
  CHECK(!restored.get("beta").has_value());
  CHECK_EQ(restored.get("gamma").value(), std::string("4"));
  CHECK_EQ(restored.size(), std::size_t{2});
}

// The crash case. A process killed mid-write leaves a partial record at the end
// of the file; recovery must drop exactly that record and keep everything
// before it.
TEST(a_torn_final_record_is_discarded_not_fatal) {
  TempLog log;
  {
    Wal wal(log.path(), SyncPolicy::kAlways);
    wal.open();
    Storage storage(&wal);
    storage.set("keep", "yes");
    storage.set("torn", "no");
  }

  const auto full_size = log.size();
  CHECK(full_size > 8);
  log.truncate(full_size - 5);  // chop the tail of the last record

  Wal reopened(log.path(), SyncPolicy::kAlways);
  Storage restored(&reopened);
  CHECK_EQ(restored.load_from_wal(), std::size_t{1});
  CHECK_EQ(restored.get("keep").value(), std::string("yes"));
  CHECK(!restored.get("torn").has_value());

  // Recovery also truncated the garbage away, so the log is now exactly the
  // records that survived and the next append lands on a record boundary.
  Wal appending(log.path(), SyncPolicy::kAlways);
  Storage grown(&appending);
  CHECK_EQ(grown.load_from_wal(), std::size_t{1});
  appending.open();
  grown.set("after", "recovery");
  appending.close();

  Wal third(log.path(), SyncPolicy::kAlways);
  Storage final_state(&third);
  CHECK_EQ(final_state.load_from_wal(), std::size_t{2});
  CHECK_EQ(final_state.get("after").value(), std::string("recovery"));
}

TEST(a_bit_flip_is_caught_by_the_checksum) {
  TempLog log;
  {
    Wal wal(log.path(), SyncPolicy::kAlways);
    wal.open();
    Storage storage(&wal);
    storage.set("first", "good");
    storage.set("second", "rotted");
  }

  // Flip a byte inside the payload of the second record. The length header
  // still looks plausible; only the CRC can tell.
  log.corrupt_byte_at(log.size() - 3);

  Wal reopened(log.path(), SyncPolicy::kAlways);
  Storage restored(&reopened);
  CHECK_EQ(restored.load_from_wal(), std::size_t{1});
  CHECK_EQ(restored.get("first").value(), std::string("good"));
  CHECK(!restored.get("second").has_value());
}

// Expiry deadlines are absolute, so a key whose TTL elapsed while the process
// was dead must not come back to life during replay.
TEST(keys_that_expired_during_downtime_do_not_return) {
  TempLog log;
  {
    Wal wal(log.path(), SyncPolicy::kAlways);
    wal.open();
    Storage storage(&wal);
    storage.set("ephemeral", "v");
    storage.set("permanent", "v");
    CHECK(storage.expire("ephemeral", 1));
  }

  std::this_thread::sleep_for(std::chrono::milliseconds(1100));

  Wal reopened(log.path(), SyncPolicy::kAlways);
  Storage restored(&reopened);
  restored.load_from_wal();
  CHECK(!restored.get("ephemeral").has_value());
  CHECK_EQ(restored.get("permanent").value(), std::string("v"));
  CHECK_EQ(restored.size(), std::size_t{1});
}

TEST(an_absent_log_is_a_valid_empty_log) {
  TempLog log;  // never created
  Wal wal(log.path(), SyncPolicy::kAlways);
  Storage storage(&wal);
  CHECK_EQ(storage.load_from_wal(), std::size_t{0});
  CHECK_EQ(storage.size(), std::size_t{0});
}

// ---------------------------------------------------------------------------
// Concurrency
// ---------------------------------------------------------------------------

TEST(concurrent_readers_and_writers_keep_the_store_consistent) {
  Storage storage;
  constexpr int kWriters = 4;
  constexpr int kReaders = 4;
  constexpr int kIterations = 2000;

  // Seed every key so readers always have something to find.
  for (int w = 0; w < kWriters; ++w) {
    storage.set("key" + std::to_string(w), "0");
  }

  std::atomic<bool> stop_readers{false};
  std::atomic<long> reads{0};
  std::vector<std::thread> threads;

  for (int w = 0; w < kWriters; ++w) {
    threads.emplace_back([&storage, w] {
      const std::string key = "key" + std::to_string(w);
      for (int i = 1; i <= kIterations; ++i) {
        storage.set(key, std::to_string(i));
      }
    });
  }

  for (int r = 0; r < kReaders; ++r) {
    threads.emplace_back([&storage, &stop_readers, &reads, r] {
      const std::string key = "key" + std::to_string(r % kWriters);
      while (!stop_readers.load(std::memory_order_relaxed)) {
        const auto value = storage.get(key);
        // Every observed value must be one a writer actually wrote -- never a
        // half-updated string. A torn read would show up as a parse failure or
        // an out-of-range number here.
        CHECK(value.has_value());
        const long parsed = std::stol(*value);
        CHECK(parsed >= 0 && parsed <= kIterations);
        reads.fetch_add(1, std::memory_order_relaxed);
      }
    });
  }

  for (int w = 0; w < kWriters; ++w) threads[static_cast<std::size_t>(w)].join();
  stop_readers.store(true);
  for (std::size_t i = kWriters; i < threads.size(); ++i) threads[i].join();

  CHECK(reads.load() > 0);
  for (int w = 0; w < kWriters; ++w) {
    CHECK_EQ(storage.get("key" + std::to_string(w)).value(),
             std::to_string(kIterations));
  }
}

// The ordering invariant, stated as an executable claim: whatever order
// concurrent writers to one key resolve into, replaying the log must reproduce
// exactly the state the live store ended up in. If the log append and the map
// update were not serialised together, this fails intermittently.
TEST(log_order_matches_memory_order_under_contention) {
  TempLog log;
  std::string live_value;
  {
    // kNever keeps the test fast; the ordering property under test is a
    // property of the locking, not of the flush.
    Wal wal(log.path(), SyncPolicy::kNever);
    wal.open();
    Storage storage(&wal);
    storage.set("contended", "seed");

    std::vector<std::thread> threads;
    for (int t = 0; t < 6; ++t) {
      threads.emplace_back([&storage, t] {
        for (int i = 0; i < 500; ++i) {
          storage.set("contended", "t" + std::to_string(t) + "-" + std::to_string(i));
        }
      });
    }
    for (auto& thread : threads) thread.join();

    live_value = storage.get("contended").value();
    wal.sync();
  }

  Wal reopened(log.path(), SyncPolicy::kNever);
  Storage restored(&reopened);
  restored.load_from_wal();
  CHECK_EQ(restored.get("contended").value(), live_value);
}

// ---------------------------------------------------------------------------
// End-to-end over real sockets
// ---------------------------------------------------------------------------

TEST(server_answers_commands_over_tcp) {
  Storage storage;
  ServerFixture server(storage);
  const int fd = dial(server.port());

  send_all(fd, "*1\r\n$4\r\nPING\r\n");
  CHECK_EQ(recv_exact(fd, 7), std::string("+PONG\r\n"));

  send_all(fd, "*3\r\n$3\r\nSET\r\n$5\r\nhello\r\n$5\r\nworld\r\n");
  CHECK_EQ(recv_exact(fd, 5), std::string("+OK\r\n"));

  send_all(fd, "*2\r\n$3\r\nGET\r\n$5\r\nhello\r\n");
  CHECK_EQ(recv_exact(fd, 11), std::string("$5\r\nworld\r\n"));

  send_all(fd, "*2\r\n$3\r\nGET\r\n$4\r\nnope\r\n");
  CHECK_EQ(recv_exact(fd, 5), std::string("$-1\r\n"));

  send_all(fd, "*2\r\n$3\r\nDEL\r\n$5\r\nhello\r\n");
  CHECK_EQ(recv_exact(fd, 4), std::string(":1\r\n"));

  send_all(fd, "*2\r\n$3\r\nDEL\r\n$5\r\nhello\r\n");
  CHECK_EQ(recv_exact(fd, 4), std::string(":0\r\n"));

  ::close(fd);
}

TEST(server_accepts_inline_commands_from_a_terminal) {
  Storage storage;
  ServerFixture server(storage);
  const int fd = dial(server.port());

  send_all(fd, "SET greeting hi\r\n");
  CHECK_EQ(recv_exact(fd, 5), std::string("+OK\r\n"));
  send_all(fd, "GET greeting\n");
  CHECK_EQ(recv_exact(fd, 8), std::string("$2\r\nhi\r\n"));

  ::close(fd);
}

// The socket-level version of the framing test: the same command, delivered as
// a dribble of one-byte segments with pauses in between.
TEST(server_reassembles_a_command_split_across_packets) {
  Storage storage;
  ServerFixture server(storage);
  const int fd = dial(server.port());

  const std::string wire = "*3\r\n$3\r\nSET\r\n$4\r\nslow\r\n$5\r\nsplit\r\n";
  for (char byte : wire) {
    send_all(fd, std::string_view(&byte, 1));
    std::this_thread::sleep_for(std::chrono::microseconds(200));
  }
  CHECK_EQ(recv_exact(fd, 5), std::string("+OK\r\n"));
  CHECK_EQ(storage.get("slow").value(), std::string("split"));

  ::close(fd);
}

TEST(server_pipelines_a_batch_of_commands) {
  Storage storage;
  ServerFixture server(storage);
  const int fd = dial(server.port());

  // Three requests in one segment; three replies must come back in order.
  send_all(fd,
           "*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n"
           "*3\r\n$3\r\nSET\r\n$1\r\nb\r\n$1\r\n2\r\n"
           "*2\r\n$3\r\nGET\r\n$1\r\na\r\n");
  const std::string expected = "+OK\r\n+OK\r\n$1\r\n1\r\n";
  CHECK_EQ(recv_exact(fd, expected.size()), expected);

  ::close(fd);
}

TEST(server_reports_errors_without_dying) {
  Storage storage;
  ServerFixture server(storage);
  const int fd = dial(server.port());

  send_all(fd, "NOSUCHCOMMAND\r\n");
  const std::string unknown = recv_line(fd);
  CHECK(unknown.rfind("-ERR unknown command 'NOSUCHCOMMAND'", 0) == 0);

  send_all(fd, "GET\r\n");
  const std::string arity = recv_line(fd);
  CHECK(arity.rfind("-ERR wrong number of arguments", 0) == 0);

  // Still alive and serving.
  send_all(fd, "PING\r\n");
  CHECK(recv_exact(fd, 7) == "+PONG\r\n");

  ::close(fd);
}

TEST(many_clients_are_served_at_once) {
  Storage storage;
  ServerFixture server(storage, 4);

  constexpr int kClients = 16;
  constexpr int kOpsPerClient = 50;
  std::vector<std::thread> clients;
  std::atomic<int> failures{0};

  for (int c = 0; c < kClients; ++c) {
    clients.emplace_back([&server, &failures, c] {
      try {
        const int fd = dial(server.port());
        for (int i = 0; i < kOpsPerClient; ++i) {
          const std::string key = "c" + std::to_string(c) + "k" + std::to_string(i);
          const std::string value = std::to_string(c * 1000 + i);

          std::string request = "*3\r\n$3\r\nSET\r\n$" + std::to_string(key.size()) + "\r\n" +
                                key + "\r\n$" + std::to_string(value.size()) + "\r\n" + value +
                                "\r\n";
          send_all(fd, request);
          if (recv_exact(fd, 5) != "+OK\r\n") failures.fetch_add(1);

          request = "*2\r\n$3\r\nGET\r\n$" + std::to_string(key.size()) + "\r\n" + key + "\r\n";
          send_all(fd, request);
          const std::string expected =
              "$" + std::to_string(value.size()) + "\r\n" + value + "\r\n";
          if (recv_exact(fd, expected.size()) != expected) failures.fetch_add(1);
        }
        ::close(fd);
      } catch (const std::exception&) {
        failures.fetch_add(1);
      }
    });
  }
  for (auto& client : clients) client.join();

  CHECK_EQ(failures.load(), 0);
  CHECK_EQ(storage.size(), std::size_t{kClients * kOpsPerClient});
}

// Full-stack durability: serve traffic over TCP, drop the whole server and
// store on the floor, then rebuild from nothing but wal.log.
TEST(state_survives_a_restart_end_to_end) {
  TempLog log;
  {
    Wal wal(log.path(), SyncPolicy::kAlways);
    Storage storage(&wal);
    storage.load_from_wal();
    wal.open();

    ServerFixture server(storage);
    const int fd = dial(server.port());
    send_all(fd, "SET durable yes\r\n");
    CHECK_EQ(recv_exact(fd, 5), std::string("+OK\r\n"));
    send_all(fd, "SET doomed maybe\r\n");
    CHECK_EQ(recv_exact(fd, 5), std::string("+OK\r\n"));
    send_all(fd, "DEL doomed\r\n");
    CHECK_EQ(recv_exact(fd, 4), std::string(":1\r\n"));
    ::close(fd);
  }

  Wal wal(log.path(), SyncPolicy::kAlways);
  Storage storage(&wal);
  storage.load_from_wal();
  wal.open();

  ServerFixture server(storage);
  const int fd = dial(server.port());
  send_all(fd, "GET durable\r\n");
  CHECK_EQ(recv_exact(fd, 9), std::string("$3\r\nyes\r\n"));
  send_all(fd, "GET doomed\r\n");
  CHECK_EQ(recv_exact(fd, 5), std::string("$-1\r\n"));
  ::close(fd);
}

TEST(expire_is_visible_over_the_wire) {
  Storage storage;
  ServerFixture server(storage);
  const int fd = dial(server.port());

  send_all(fd, "SET fleeting now\r\n");
  CHECK_EQ(recv_exact(fd, 5), std::string("+OK\r\n"));
  send_all(fd, "EXPIRE fleeting 1\r\n");
  CHECK_EQ(recv_exact(fd, 4), std::string(":1\r\n"));
  send_all(fd, "EXPIRE missing 1\r\n");
  CHECK_EQ(recv_exact(fd, 4), std::string(":0\r\n"));
  send_all(fd, "EXPIRE fleeting notanumber\r\n");
  CHECK(recv_line(fd).rfind("-ERR value is not an integer", 0) == 0);

  std::this_thread::sleep_for(std::chrono::milliseconds(1200));
  send_all(fd, "GET fleeting\r\n");
  CHECK_EQ(recv_exact(fd, 5), std::string("$-1\r\n"));

  ::close(fd);
}

// ---------------------------------------------------------------------------

int main() {
  // A client vanishing mid-reply must not take the test binary down with it.
  std::signal(SIGPIPE, SIG_IGN);

  int failed = 0;
  int passed = 0;
  for (const auto& test : testing::registry()) {
    try {
      test.fn();
      std::cout << "  PASS  " << test.name << "\n";
      ++passed;
    } catch (const std::exception& error) {
      std::cout << "  FAIL  " << test.name << "\n        " << error.what() << "\n";
      ++failed;
    }
  }

  std::cout << "\n" << passed << " passed, " << failed << " failed\n";
  return failed == 0 ? 0 : 1;
}
