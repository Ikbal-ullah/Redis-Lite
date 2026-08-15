// Non-blocking TCP front end.
//
// Architecture
// ------------
// One listening socket, N event loops, one thread each. Event loop 0 owns the
// listener; when it accepts a connection it hands the file descriptor to the
// next loop round-robin and pokes that loop awake through a pipe.
//
//        accept()            round-robin handoff
//   listener --> loop 0 --+--> loop 1 (thread)  ] each loop owns its
//                  |      +--> loop 2 (thread)  ] connections outright:
//                  |      +--> loop 3 (thread)  ] no per-connection locking
//                  +--------> (keeps some itself)
//                                   |
//                                   v
//                          Storage (shared_mutex)
//                                   |
//                                   v
//                              WAL -> disk
//
// A connection belongs to exactly one thread for its entire life, so its
// buffers and parse state need no synchronisation at all. The only shared
// mutable state in the whole server is Storage, and that is where the locks
// live. This is the memcached model, and it is much easier to reason about
// than a thread pool where any thread may touch any connection.
//
// Why poll(2) and not epoll/kqueue
// --------------------------------
// epoll is Linux-only, kqueue is BSD/macOS-only; poll is POSIX and runs on
// both. The price is honest and worth stating: poll hands the kernel the whole
// interest list on every call and the kernel scans all of it, so each iteration
// is O(connections). At a few thousand sockets that dominates. epoll/kqueue
// keep the interest set registered in the kernel and return only what is ready,
// giving O(ready). Swapping the backend means rewriting EventLoop::run() and
// nothing else.

#ifndef KVSTORE_SERVER_HPP
#define KVSTORE_SERVER_HPP

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <vector>

#include "kvstore/protocol.hpp"
#include "kvstore/storage.hpp"

namespace kvstore {

struct ServerConfig {
  std::string bind_address = "127.0.0.1";
  // Port 0 means "let the kernel pick a free one"; read it back with port().
  std::uint16_t port = 6379;
  int io_threads = 4;
  // How often the maintenance thread sweeps expired keys.
  int purge_interval_ms = 1000;
};

// A growable byte queue: append at the back, consume from the front.
//
// The two-cursor design exists because consuming must not memmove. Retiring a
// parsed command has to be O(1), and -- more importantly -- the string_views
// inside a parsed Command point into this storage, so shifting bytes underneath
// them would leave dangling views. Reclaiming space is deferred to compact(),
// which is only called when no views are outstanding.
class Buffer {
 public:
  std::string_view readable() const noexcept {
    return {data_.data() + read_pos_, write_pos_ - read_pos_};
  }
  std::size_t size() const noexcept { return write_pos_ - read_pos_; }
  bool empty() const noexcept { return read_pos_ == write_pos_; }

  void consume(std::size_t n) noexcept;
  void append(std::string_view bytes);
  void clear() noexcept { read_pos_ = write_pos_ = 0; }

  // Guarantees at least `n` writable bytes, compacting or growing as needed.
  // Invalidates every view previously returned by readable().
  void ensure_writable(std::size_t n);
  char* write_ptr() noexcept { return data_.data() + write_pos_; }
  std::size_t writable() const noexcept { return data_.size() - write_pos_; }
  void commit(std::size_t n) noexcept { write_pos_ += n; }

 private:
  void compact() noexcept;

  std::vector<char> data_;
  std::size_t read_pos_ = 0;
  std::size_t write_pos_ = 0;
};

// One client. Owned by exactly one EventLoop, touched by exactly one thread.
class Connection {
 public:
  explicit Connection(int fd) noexcept : fd_(fd) {}
  ~Connection();

  Connection(const Connection&) = delete;
  Connection& operator=(const Connection&) = delete;

  int fd() const noexcept { return fd_; }
  Buffer& input() noexcept { return input_; }
  std::string& output() noexcept { return output_; }

  bool has_pending_output() const noexcept { return output_sent_ < output_.size(); }
  void mark_for_close() noexcept { close_requested_ = true; }
  bool close_requested() const noexcept { return close_requested_; }

  // Drains input_ into output_ by parsing and executing whole commands.
  // Returns false if the peer must be disconnected (protocol error).
  bool process(Storage& storage);

  // Pushes as much of output_ as the kernel will take. Returns false on a fatal
  // socket error.
  bool flush(bool& blocked);

  // Reads until the socket is drained. Sets `closed` when the peer hung up.
  bool fill(bool& closed);

 private:
  int fd_;
  Buffer input_;
  std::string output_;
  std::size_t output_sent_ = 0;
  bool close_requested_ = false;
};

class Server;

class EventLoop {
 public:
  explicit EventLoop(Server& server);
  ~EventLoop();

  EventLoop(const EventLoop&) = delete;
  EventLoop& operator=(const EventLoop&) = delete;

  void set_listener(int fd) noexcept { listen_fd_ = fd; }

  // Queues a freshly accepted fd for adoption and wakes the loop.
  void adopt(int fd);

  void run();
  void stop();

 private:
  void wake();
  void drain_wakeup();
  void accept_ready();
  void handle_connection(int fd, short revents);
  void close_connection(int fd);

  Server& server_;
  int listen_fd_ = -1;

  // The self-pipe. A poll() parked with an infinite timeout can only be woken
  // by I/O on a descriptor it is watching, so the loop watches the read end and
  // anyone who wants its attention writes one byte to the write end. This is
  // the standard way to make a blocking event loop interruptible.
  int wake_read_fd_ = -1;
  int wake_write_fd_ = -1;

  std::mutex pending_mutex_;
  std::vector<int> pending_fds_;

  std::unordered_map<int, std::unique_ptr<Connection>> connections_;
  std::atomic<bool> running_{false};
};

class Server {
 public:
  Server(ServerConfig config, Storage& storage);
  ~Server();

  Server(const Server&) = delete;
  Server& operator=(const Server&) = delete;

  // Binds and listens synchronously (so bind errors surface here, on the
  // caller's thread), then spawns the I/O and maintenance threads and returns.
  void start();

  // start() plus a wait for shutdown.
  void run();

  // Safe to call from another thread or from a signal handler's helper.
  void stop();

  // Valid after start(). Resolves the kernel-assigned port when config.port==0.
  std::uint16_t port() const noexcept { return bound_port_; }

  Storage& storage() noexcept { return storage_; }

 private:
  void open_listener();
  void distribute(int client_fd);
  void maintenance_loop();

  ServerConfig config_;
  Storage& storage_;

  int listen_fd_ = -1;
  std::uint16_t bound_port_ = 0;

  std::vector<std::unique_ptr<EventLoop>> loops_;
  std::vector<std::thread> io_threads_;
  std::thread maintenance_thread_;

  std::atomic<std::size_t> next_loop_{0};
  std::atomic<bool> running_{false};

  friend class EventLoop;
};

// Runs one parsed command against storage and appends the reply.
//
// Lives here, not in protocol.cpp, on purpose: protocol knows nothing about
// storage and storage knows nothing about protocol. The server is the only
// layer allowed to know about both.
void execute_command(const Command& command, Storage& storage, std::string& out);

// Puts a descriptor into non-blocking mode. Exposed for tests.
void set_nonblocking(int fd);

}  // namespace kvstore

#endif  // KVSTORE_SERVER_HPP
