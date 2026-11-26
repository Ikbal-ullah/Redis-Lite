// End-to-end over real TCP sockets. The server runs its single-threaded loop
// on a background thread; the test thread is the client. Server-side counters
// are only read after run() has returned and the thread is joined.

#include <arpa/inet.h>
#include <dirent.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "harness.hpp"
#include "kvstore/database.hpp"
#include "kvstore/server.hpp"
#include "kvstore/wal.hpp"

using namespace kvstore;
using testing::TempPath;

namespace {

class Client {
 public:
  explicit Client(std::uint16_t port) {
    fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    ::inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
    if (::connect(fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
      throw testing::Failure("connect failed");
    }
  }
  ~Client() { close(); }
  void close() {
    if (fd_ >= 0) ::close(fd_);
    fd_ = -1;
  }
  void send(const std::string& data) {
    std::size_t off = 0;
    while (off < data.size()) {
      const ssize_t n = ::send(fd_, data.data() + off, data.size() - off, 0);
      if (n <= 0) throw testing::Failure("send failed");
      off += static_cast<std::size_t>(n);
    }
  }
  void shutdown_write() { ::shutdown(fd_, SHUT_WR); }
  // Reads until `n` bytes arrived, EOF, or 5 s passed.
  std::string recv_exact(std::size_t n) {
    std::string out;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (out.size() < n && std::chrono::steady_clock::now() < deadline) {
      pollfd p{fd_, POLLIN, 0};
      if (::poll(&p, 1, 100) <= 0) continue;
      char buf[65536];
      const ssize_t got = ::recv(fd_, buf, sizeof(buf), 0);
      if (got <= 0) break;
      out.append(buf, static_cast<std::size_t>(got));
    }
    return out;
  }
  std::string roundtrip(const std::string& request, const std::string& expected) {
    send(request);
    return recv_exact(expected.size());
  }

 private:
  int fd_ = -1;
};

// Owns a WAL + Database + Server running on a background thread.
class Running {
 public:
  Running(const std::string& wal_path, FsyncPolicy policy, ServerConfig config = {})
      : wal_(wal_path, policy), db_(&wal_) {
    db_.recover(now_ms());
    wal_.open();
    config.port = 0;
    server_ = std::make_unique<Server>(config, db_);
    server_->listen();
    thread_ = std::thread([this] { server_->run(); });
  }
  ~Running() { stop(); }
  void stop() {
    if (thread_.joinable()) {
      server_->request_stop();
      thread_.join();
    }
  }
  std::uint16_t port() const { return server_->port(); }
  Wal& wal() { return wal_; }
  Server& server() { return *server_; }

 private:
  Wal wal_;
  Database db_;
  std::unique_ptr<Server> server_;
  std::thread thread_;
};

std::size_t open_fd_count() {
  std::size_t n = 0;
  if (DIR* d = ::opendir("/dev/fd")) {
    while (::readdir(d) != nullptr) ++n;
    ::closedir(d);
  }
  return n;
}

std::string resp(std::initializer_list<std::string> args) {
  std::string out = "*" + std::to_string(args.size()) + "\r\n";
  for (const auto& a : args) out += "$" + std::to_string(a.size()) + "\r\n" + a + "\r\n";
  return out;
}

}  // namespace

TEST(server_answers_resp_and_inline_commands) {
  TempPath path;
  Running srv(path.str(), FsyncPolicy::kAlways);
  Client c(srv.port());
  CHECK_EQ(c.roundtrip("PING\r\n", "+PONG\r\n"), std::string("+PONG\r\n"));
  CHECK_EQ(c.roundtrip(resp({"SET", "k", "v", "PX", "60000"}), "+OK\r\n"), std::string("+OK\r\n"));
  CHECK_EQ(c.roundtrip(resp({"GET", "k"}), "$1\r\nv\r\n"), std::string("$1\r\nv\r\n"));
  CHECK_EQ(c.roundtrip("TTL k\r\n", ":60\r\n"), std::string(":60\r\n"));
}

TEST(server_reassembles_a_command_split_byte_by_byte) {
  TempPath path;
  Running srv(path.str(), FsyncPolicy::kNo);
  Client c(srv.port());
  const std::string req = resp({"SET", "split", "value"});
  for (char ch : req) {
    c.send(std::string(1, ch));
    std::this_thread::sleep_for(std::chrono::microseconds(200));
  }
  CHECK_EQ(c.recv_exact(5), std::string("+OK\r\n"));
}

TEST(server_answers_a_pipeline_sent_in_one_write) {
  TempPath path;
  Running srv(path.str(), FsyncPolicy::kNo);
  Client c(srv.port());
  std::string batch, expected;
  for (int i = 0; i < 500; ++i) {
    batch += resp({"SET", "p" + std::to_string(i), std::to_string(i)});
    expected += "+OK\r\n";
  }
  for (int i = 0; i < 500; ++i) {
    batch += resp({"GET", "p" + std::to_string(i)});
    expected += "$" + std::to_string(std::to_string(i).size()) + "\r\n" + std::to_string(i) + "\r\n";
  }
  CHECK_EQ(c.roundtrip(batch, expected), expected);
}

TEST(protocol_error_replies_then_closes) {
  TempPath path;
  Running srv(path.str(), FsyncPolicy::kNo);
  Client c(srv.port());
  c.send("*1\r\n$3\r\nfooXX");
  const std::string reply = c.recv_exact(1000);  // reads until the server closes
  CHECK(reply.rfind("-ERR Protocol error", 0) == 0);
  Client other(srv.port());  // the server itself is still alive
  CHECK_EQ(other.roundtrip("PING\r\n", "+PONG\r\n"), std::string("+PONG\r\n"));
}

TEST(half_closed_client_still_gets_every_reply) {
  // Audit A4: `printf ... | nc -N` sends, then shuts down its write side.
  TempPath path;
  Running srv(path.str(), FsyncPolicy::kAlways);
  Client c(srv.port());
  std::string expected;
  std::string batch;
  for (int i = 0; i < 50; ++i) {
    batch += resp({"SET", "h" + std::to_string(i), "v"});
    expected += "+OK\r\n";
  }
  c.send(batch);
  c.shutdown_write();
  CHECK_EQ(c.recv_exact(expected.size()), expected);
}

TEST(no_descriptor_leaks_across_connection_churn) {
  TempPath path;
  Running srv(path.str(), FsyncPolicy::kNo);
  { Client warm(srv.port()); warm.roundtrip("PING\r\n", "+PONG\r\n"); }
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  const std::size_t baseline = open_fd_count();
  for (int round = 0; round < 10; ++round) {
    std::vector<std::unique_ptr<Client>> clients;
    for (int i = 0; i < 50; ++i) clients.push_back(std::make_unique<Client>(srv.port()));
    for (auto& cl : clients) CHECK_EQ(cl->roundtrip("PING\r\n", "+PONG\r\n"), std::string("+PONG\r\n"));
    // Half the clients vanish mid-request, half close cleanly.
    for (std::size_t i = 0; i < clients.size(); i += 2) clients[i]->send("*2\r\n$3\r\nGET\r\n");
  }
  // Every server-side socket must be closed once the peers are gone.
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (open_fd_count() > baseline && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }
  // <=, not ==: the warm-up socket may not have been reaped yet when the
  // baseline was sampled. A leak shows up as a count *above* the baseline.
  CHECK(open_fd_count() <= baseline);
  srv.stop();
  CHECK_EQ(srv.server().open_connections(), 0u);
}

TEST(state_survives_a_restart) {
  TempPath path;
  {
    Running srv(path.str(), FsyncPolicy::kAlways);
    Client c(srv.port());
    c.roundtrip(resp({"SET", "durable", "yes"}), "+OK\r\n");
    c.roundtrip(resp({"SET", "temp", "x", "EX", "1000"}), "+OK\r\n");
    c.roundtrip(resp({"DEL", "temp"}), ":1\r\n");
  }
  Running srv(path.str(), FsyncPolicy::kAlways);
  Client c(srv.port());
  CHECK_EQ(c.roundtrip(resp({"GET", "durable"}), "$3\r\nyes\r\n"), std::string("$3\r\nyes\r\n"));
  CHECK_EQ(c.roundtrip(resp({"GET", "temp"}), "$-1\r\n"), std::string("$-1\r\n"));
}

TEST(compact_over_the_wire_shrinks_the_log) {
  TempPath path;
  Running srv(path.str(), FsyncPolicy::kGroup);
  Client c(srv.port());
  std::string batch, expected;
  for (int i = 0; i < 200; ++i) {
    batch += resp({"SET", "same", std::to_string(i)});
    expected += "+OK\r\n";
  }
  CHECK_EQ(c.roundtrip(batch, expected), expected);
  CHECK_EQ(c.roundtrip("COMPACT\r\n", "+OK\r\n"), std::string("+OK\r\n"));
  CHECK_EQ(c.roundtrip(resp({"GET", "same"}), "$3\r\n199\r\n"), std::string("$3\r\n199\r\n"));
  srv.stop();
  CHECK(srv.wal().size_bytes() < 100);
}

TEST(group_commit_batches_fsyncs_and_still_acks_every_write) {
  TempPath path;
  ServerConfig config;
  config.group_window_ms = 10;
  config.group_max_ops = 64;
  Running srv(path.str(), FsyncPolicy::kGroup, config);
  Client c(srv.port());
  std::string batch, expected;
  for (int i = 0; i < 640; ++i) {
    batch += resp({"SET", "g" + std::to_string(i), "v"});
    expected += "+OK\r\n";
  }
  CHECK_EQ(c.roundtrip(batch, expected), expected);
  // One more write alone: acknowledged by the 10 ms window, not the count.
  const auto t0 = std::chrono::steady_clock::now();
  CHECK_EQ(c.roundtrip(resp({"SET", "lonely", "v"}), "+OK\r\n"), std::string("+OK\r\n"));
  const auto waited = std::chrono::steady_clock::now() - t0;
  CHECK(waited >= std::chrono::milliseconds(9));
  srv.stop();
  CHECK_EQ(srv.wal().appended_seq(), 641u);
  CHECK_EQ(srv.wal().unsynced(), 0u);
  CHECK(srv.wal().sync_count() <= 20u);  // vs 641 under --fsync always
  CHECK(srv.wal().sync_count() >= 10u);
}

TEST(group_commit_preserves_reply_order_with_reads) {
  TempPath path;
  Running srv(path.str(), FsyncPolicy::kGroup);
  Client c(srv.port());
  const std::string batch = resp({"GET", "x"}) + resp({"SET", "x", "1"}) + resp({"GET", "x"}) + "PING\r\n";
  const std::string expected = "$-1\r\n+OK\r\n$1\r\n1\r\n+PONG\r\n";
  CHECK_EQ(c.roundtrip(batch, expected), expected);
}

TEST(many_concurrent_clients_are_served) {
  TempPath path;
  Running srv(path.str(), FsyncPolicy::kGroup);
  std::vector<std::unique_ptr<Client>> clients;
  for (int i = 0; i < 200; ++i) clients.push_back(std::make_unique<Client>(srv.port()));
  for (std::size_t i = 0; i < clients.size(); ++i) clients[i]->send(resp({"SET", "c" + std::to_string(i), "v"}));
  for (auto& cl : clients) CHECK_EQ(cl->recv_exact(5), std::string("+OK\r\n"));
}

TEST(slow_reader_gets_backpressure_not_unbounded_memory) {
  TempPath path;
  Running srv(path.str(), FsyncPolicy::kNo);
  Client c(srv.port());
  const std::string big(1024 * 1024, 'x');
  c.roundtrip(resp({"SET", "big", big}), "+OK\r\n");
  // Ask for 64 MiB of replies without reading; then read them all.
  std::string batch;
  for (int i = 0; i < 64; ++i) batch += resp({"GET", "big"});
  c.send(batch);
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  const std::string one = "$" + std::to_string(big.size()) + "\r\n" + big + "\r\n";
  const std::string got = c.recv_exact(one.size() * 64);
  CHECK_EQ(got.size(), one.size() * 64);
  CHECK(got.compare(0, one.size(), one) == 0);
}
