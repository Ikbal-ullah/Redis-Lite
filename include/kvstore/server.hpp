// Single-threaded, non-blocking TCP server on poll(2).
//
//   clients ──TCP──> poll() loop ──> parse (RESP2, pipelined) ──> Database
//                        │                                          │
//                        │<── replies (write_buf / held_buf) <──────┤
//                        │                                          v
//                        └── timers: group-commit window,     WAL ──> fsync
//                                    active expiry cycle
//
// One thread owns everything: sockets, buffers, keyspace, WAL. There are no
// locks because there is no sharing. Like Redis, throughput comes from never
// blocking on a socket and from batching (pipelining, group commit), not from
// threads. The one blocking call is fsync, which is the point of a WAL.
//
// Per-connection state:
//   read_buf   bytes received but not yet parsed into whole commands
//   write_buf  encoded replies the kernel has not accepted yet; POLLOUT is
//              requested only while it is non-empty
//   held_buf   (--fsync group only) replies that depend on a WAL record which
//              is written but not yet fsynced. Moved to write_buf when the
//              covering group fsync completes. Once a reply is held, every
//              later reply on that connection queues behind it, preserving
//              pipeline order.
//
// poll vs epoll/kqueue: poll is portable POSIX but O(connections) per call
// because the interest set is re-sent every time. Swapping the backend touches
// Server::run() only.

#ifndef KVSTORE_SERVER_HPP
#define KVSTORE_SERVER_HPP

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "kvstore/database.hpp"

namespace kvstore {

struct ServerConfig {
  std::string bind_address = "127.0.0.1";
  std::uint16_t port = 6379;  // 0 = kernel-chosen; read back with port()
  // Group commit: fsync when the oldest unsynced record is this old ...
  int group_window_ms = 10;
  // ... or when this many records are unsynced, whichever comes first.
  std::uint64_t group_max_ops = 64;
  // Active expiry: every cycle evicts at most `expire_budget` dead keys.
  int expire_cycle_ms = 100;
  std::size_t expire_budget = 200;
  // Backpressure: stop parsing a client's pipeline while this many reply bytes
  // are queued for it.
  std::size_t output_high_water = 16u * 1024u * 1024u;
};

class Connection {
 public:
  explicit Connection(int fd) noexcept : fd_(fd) {}
  ~Connection();  // closes the descriptor: the only place it is closed
  Connection(const Connection&) = delete;
  Connection& operator=(const Connection&) = delete;

  int fd() const noexcept { return fd_; }
  bool has_output() const noexcept { return write_pos < write_buf.size(); }
  std::size_t backlog() const noexcept { return write_buf.size() - write_pos + held_buf.size(); }

  std::string read_buf;
  std::string write_buf;
  std::size_t write_pos = 0;  // bytes of write_buf already sent
  std::string held_buf;
  bool peer_eof = false;           // read() returned 0
  bool close_after_flush = false;  // protocol error: send the error, then close
  bool input_paused = false;       // stopped parsing because backlog is too big

 private:
  int fd_;
};

class Server {
 public:
  Server(ServerConfig config, Database& db);
  ~Server();
  Server(const Server&) = delete;
  Server& operator=(const Server&) = delete;

  // Binds and listens; errors surface here, synchronously.
  void listen();
  // Runs the event loop on the calling thread until request_stop(). On the
  // way out it fsyncs the WAL, releases held replies and closes every socket.
  void run();
  // Async-signal-safe and thread-safe: an atomic store plus one write(2).
  void request_stop() noexcept;

  std::uint16_t port() const noexcept { return bound_port_; }
  std::size_t open_connections() const noexcept { return conns_.size(); }

 private:
  using SteadyClock = std::chrono::steady_clock;

  void accept_ready();
  void handle(Connection& conn, short revents);
  bool read_ready(Connection& conn);
  bool pump(Connection& conn);
  void process(Connection& conn);
  bool flush(Connection& conn);
  bool should_close(const Connection& conn) const noexcept;
  void close_conn(int fd);

  bool group_mode() const noexcept;
  void maybe_group_commit(bool force);
  void flush_released();
  void run_timers();
  int poll_timeout_ms();

  ServerConfig config_;
  Database& db_;
  int listen_fd_ = -1;
  int wake_read_fd_ = -1;
  int wake_write_fd_ = -1;
  int spare_fd_ = -1;  // reserved descriptor for EMFILE recovery
  std::uint16_t bound_port_ = 0;
  std::atomic<bool> stop_{false};

  std::unordered_map<int, std::unique_ptr<Connection>> conns_;
  std::vector<int> waiting_;   // fds with a non-empty held_buf
  std::vector<int> released_;  // fds whose held_buf was just moved to write_buf
  SteadyClock::time_point next_expire_{};
};

void set_nonblocking(int fd);

}  // namespace kvstore

#endif  // KVSTORE_SERVER_HPP
