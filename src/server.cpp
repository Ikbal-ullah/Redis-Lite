#include "kvstore/server.hpp"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <stdexcept>
#include <system_error>

namespace kvstore {
namespace {

constexpr std::size_t kReadChunk = 64 * 1024;
// Fairness: one wakeup reads at most this much from a single client, so a
// client firehosing a pipeline cannot starve the others. Level-triggered poll
// reports the rest next iteration.
constexpr std::size_t kMaxReadPerEvent = 1024 * 1024;
// A request still incomplete past this size is hostile or broken.
constexpr std::size_t kMaxQueryBuffer = 128u * 1024u * 1024u;
constexpr std::size_t kWriteCompactThreshold = 64 * 1024;

#if defined(MSG_NOSIGNAL)
constexpr int kSendFlags = MSG_NOSIGNAL;  // Linux: no SIGPIPE per call
#else
constexpr int kSendFlags = 0;  // macOS: SO_NOSIGPIPE per socket instead
#endif

[[noreturn]] void throw_errno(const char* what) {
  throw std::system_error(errno, std::generic_category(), what);
}

void set_cloexec(int fd) {
  const int flags = ::fcntl(fd, F_GETFD);
  if (flags < 0 || ::fcntl(fd, F_SETFD, flags | FD_CLOEXEC) < 0) throw_errno("fcntl FD_CLOEXEC");
}

}  // namespace

void set_nonblocking(int fd) {
  const int flags = ::fcntl(fd, F_GETFL, 0);
  if (flags < 0 || ::fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) throw_errno("fcntl O_NONBLOCK");
}

Connection::~Connection() {
  if (fd_ >= 0) ::close(fd_);
}

// ---------------------------------------------------------------------------
// Setup / teardown
// ---------------------------------------------------------------------------

Server::Server(ServerConfig config, Database& db) : config_(std::move(config)), db_(db) {
  int fds[2];
  if (::pipe(fds) != 0) throw_errno("pipe");
  wake_read_fd_ = fds[0];
  wake_write_fd_ = fds[1];
  for (int fd : fds) {
    set_nonblocking(fd);
    set_cloexec(fd);
  }
  spare_fd_ = ::open("/dev/null", O_RDONLY | O_CLOEXEC);
}

Server::~Server() {
  conns_.clear();
  for (int fd : {listen_fd_, wake_read_fd_, wake_write_fd_, spare_fd_}) {
    if (fd >= 0) ::close(fd);
  }
}

void Server::listen() {
  const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) throw_errno("socket");
  listen_fd_ = fd;  // owned from here on: the destructor closes it on any throw below
  set_cloexec(fd);

  const int one = 1;
  if (::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one)) != 0) {
    throw_errno("setsockopt SO_REUSEADDR");
  }
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(config_.port);
  if (::inet_pton(AF_INET, config_.bind_address.c_str(), &addr.sin_addr) != 1) {
    throw std::runtime_error("invalid bind address: " + config_.bind_address);
  }
  if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) throw_errno("bind");
  if (::listen(fd, SOMAXCONN) != 0) throw_errno("listen");

  sockaddr_in actual{};
  socklen_t len = sizeof(actual);
  if (::getsockname(fd, reinterpret_cast<sockaddr*>(&actual), &len) != 0) throw_errno("getsockname");
  bound_port_ = ntohs(actual.sin_port);
  set_nonblocking(fd);
}

void Server::request_stop() noexcept {
  stop_.store(true, std::memory_order_release);
  const char byte = 1;
  // EAGAIN means the pipe is already full of wakeups: fine to drop.
  [[maybe_unused]] const ssize_t rc = ::write(wake_write_fd_, &byte, 1);
}

bool Server::group_mode() const noexcept {
  return db_.wal() != nullptr && db_.wal()->policy() == FsyncPolicy::kGroup;
}

// ---------------------------------------------------------------------------
// Event loop
// ---------------------------------------------------------------------------

void Server::run() {
  if (listen_fd_ < 0) listen();
  std::vector<pollfd> pfds;
  next_expire_ = SteadyClock::now();

  while (!stop_.load(std::memory_order_acquire)) {
    pfds.clear();
    pfds.push_back({wake_read_fd_, POLLIN, 0});
    pfds.push_back({listen_fd_, POLLIN, 0});
    for (const auto& [fd, conn] : conns_) {
      short events = 0;
      if (!conn->peer_eof && !conn->close_after_flush && !conn->input_paused) events |= POLLIN;
      // Only while there is something to send: an idle socket is almost always
      // writable, so an unconditional POLLOUT turns poll() into a busy loop.
      if (conn->has_output()) events |= POLLOUT;
      pfds.push_back({fd, events, 0});  // POLLERR/POLLHUP are reported regardless
    }

    const int rc = ::poll(pfds.data(), static_cast<nfds_t>(pfds.size()), poll_timeout_ms());
    if (rc < 0) {
      if (errno == EINTR) continue;
      throw_errno("poll");
    }

    if (pfds[0].revents != 0) {
      char sink[64];
      while (::read(wake_read_fd_, sink, sizeof(sink)) > 0) {
      }
    }
    if (pfds[1].revents & POLLIN) accept_ready();

    // pfds holds plain fds; a handler may close its connection, so each one is
    // re-looked-up rather than reached through a cached pointer. New accepts
    // above only ever add fds that are not in this snapshot.
    for (std::size_t i = 2; i < pfds.size(); ++i) {
      if (pfds[i].revents == 0) continue;
      auto it = conns_.find(pfds[i].fd);
      if (it != conns_.end()) handle(*it->second, pfds[i].revents);
    }

    run_timers();
  }

  // Clean shutdown: nothing acknowledged-but-unsynced may be left behind.
  if (db_.wal() != nullptr) db_.wal()->sync();
  maybe_group_commit(true);
  for (auto& [fd, conn] : conns_) {
    if (conn->has_output()) flush(*conn);  // best effort; sockets are non-blocking
  }
  conns_.clear();
}

int Server::poll_timeout_ms() {
  const auto now = SteadyClock::now();
  auto deadline = next_expire_;
  if (group_mode() && db_.wal()->unsynced() > 0) {
    deadline = std::min(deadline, db_.wal()->oldest_unsynced_at() +
                                      std::chrono::milliseconds(config_.group_window_ms));
  }
  if (deadline <= now) return 0;
  // Round up so we never wake a hair early and spin on a 0 ms timeout.
  const auto wait = std::chrono::ceil<std::chrono::milliseconds>(deadline - now).count();
  return static_cast<int>(std::min<long long>(wait, 1000));
}

void Server::run_timers() {
  maybe_group_commit(false);
  flush_released();

  const auto now = SteadyClock::now();
  if (now >= next_expire_) {
    const std::size_t evicted = db_.storage().active_expire(now_ms(), config_.expire_budget);
    // Budget exhausted means more dead keys are waiting: come back right after
    // the next round of I/O instead of sleeping a full cycle.
    next_expire_ = evicted == config_.expire_budget
                       ? now
                       : now + std::chrono::milliseconds(config_.expire_cycle_ms);
  }
}

// ---------------------------------------------------------------------------
// Group commit
// ---------------------------------------------------------------------------

void Server::maybe_group_commit(bool force) {
  if (!group_mode()) return;
  Wal& wal = *db_.wal();
  if (wal.unsynced() > 0) {
    const bool window_elapsed =
        SteadyClock::now() - wal.oldest_unsynced_at() >= std::chrono::milliseconds(config_.group_window_ms);
    if (!force && !window_elapsed && wal.unsynced() < config_.group_max_ops) return;
    wal.sync();  // one fsync covers every record written since the last one
  }
  // Everything written is now durable: every held reply may go out.
  for (int fd : waiting_) {
    auto it = conns_.find(fd);
    if (it == conns_.end() || it->second->held_buf.empty()) continue;
    Connection& conn = *it->second;
    conn.write_buf += conn.held_buf;
    conn.held_buf.clear();
    released_.push_back(fd);
  }
  waiting_.clear();
}

void Server::flush_released() {
  // Push released replies out now instead of waiting a poll() round trip.
  // Swap first: pump() -> process() can trigger another group commit, which
  // appends to released_ and would invalidate a live iterator over it.
  while (!released_.empty()) {
    std::vector<int> batch;
    batch.swap(released_);
    for (int fd : batch) {
      auto it = conns_.find(fd);
      if (it == conns_.end()) continue;
      Connection& conn = *it->second;
      // pump(), not just flush(): input may be paused behind the held replies.
      if (!pump(conn) || should_close(conn)) close_conn(fd);
    }
  }
}

// ---------------------------------------------------------------------------
// Per-connection I/O
// ---------------------------------------------------------------------------

void Server::accept_ready() {
  while (true) {
    const int fd = ::accept(listen_fd_, nullptr, nullptr);
    if (fd < 0) {
      if (errno == EINTR || errno == ECONNABORTED) continue;
      if ((errno == EMFILE || errno == ENFILE) && spare_fd_ >= 0) {
        // Out of descriptors. The pending connection keeps the listener
        // readable, so returning would make poll() spin at 100% CPU. Free the
        // reserved fd, accept-and-close the peer (it sees a clean EOF), and
        // re-reserve.
        ::close(spare_fd_);
        const int victim = ::accept(listen_fd_, nullptr, nullptr);
        if (victim >= 0) ::close(victim);
        spare_fd_ = ::open("/dev/null", O_RDONLY | O_CLOEXEC);
        continue;
      }
      return;  // EAGAIN: backlog drained
    }
    auto conn = std::make_unique<Connection>(fd);  // owns fd from here on
    try {
      set_nonblocking(fd);
      set_cloexec(fd);
    } catch (const std::system_error&) {
      continue;  // conn's destructor closes fd
    }
    const int one = 1;
#if defined(SO_NOSIGPIPE)
    ::setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &one, sizeof(one));
#endif
    // Nagle would delay small replies waiting for more data that never comes.
    ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
    conns_.emplace(fd, std::move(conn));
  }
}

void Server::handle(Connection& conn, short revents) {
  const int fd = conn.fd();
  if (revents & (POLLERR | POLLNVAL)) return close_conn(fd);

  // POLLHUP is folded into the read path: there may still be unread request
  // bytes before the EOF, and recv() is what tells them apart.
  if ((revents & (POLLIN | POLLHUP)) && !conn.peer_eof) {
    if (!read_ready(conn)) return close_conn(fd);
  }
  if (!pump(conn) || should_close(conn)) close_conn(fd);
}

bool Server::pump(Connection& conn) {
  // Parse/execute, then send. If parsing paused on backpressure and the send
  // drained the backlog below the mark, go again: otherwise a connection can
  // be left with input_paused set and an empty write_buf, so poll() asks for
  // neither POLLIN nor POLLOUT and the connection hangs forever. Terminates:
  // every round either consumes input or leaves a backlog only POLLOUT clears.
  do {
    process(conn);
    if (!flush(conn)) return false;
  } while (conn.input_paused && conn.backlog() < config_.output_high_water);
  return true;
}

bool Server::read_ready(Connection& conn) {
  char chunk[kReadChunk];
  std::size_t total = 0;
  while (total < kMaxReadPerEvent) {
    const ssize_t n = ::recv(conn.fd(), chunk, sizeof(chunk), 0);
    if (n > 0) {
      conn.read_buf.append(chunk, static_cast<std::size_t>(n));
      total += static_cast<std::size_t>(n);
      continue;  // a short read is not "drained"; only EAGAIN is
    }
    if (n == 0) {
      conn.peer_eof = true;  // FIN. Still answer what was already sent.
      return true;
    }
    if (errno == EINTR) continue;
    if (errno == EAGAIN || errno == EWOULDBLOCK) return true;
    return false;  // ECONNRESET etc.
  }
  return true;
}

void Server::process(Connection& conn) {
  Wal* wal = db_.wal();
  const bool group = group_mode();
  conn.input_paused = false;
  std::size_t pos = 0;
  Command command;

  while (pos < conn.read_buf.size() && !conn.close_after_flush) {
    if (conn.backlog() >= config_.output_high_water) {
      conn.input_paused = true;
      break;
    }
    // Views in `command` point into read_buf, which is not modified until the
    // erase() after this loop.
    const std::string_view input(conn.read_buf.data() + pos, conn.read_buf.size() - pos);
    const ParseResult parsed = parse_command(input, command);
    if (parsed.status == ParseStatus::kIncomplete) break;

    std::string& out = conn.held_buf.empty() ? conn.write_buf : conn.held_buf;
    if (parsed.status == ParseStatus::kError) {
      reply_error(out, "ERR " + parsed.error);  // Redis: "-ERR Protocol error: ..."
      conn.close_after_flush = true;
      break;
    }
    pos += parsed.consumed;
    if (command.kind == CommandKind::kEmpty) continue;

    const std::uint64_t seq_before = wal ? wal->appended_seq() : 0;
    const std::size_t reply_start = out.size();
    execute_command(command, db_, out, now_ms());

    if (group && &out == &conn.write_buf && wal->appended_seq() != seq_before &&
        wal->unsynced() > 0) {
      // This reply acknowledges a record that is not durable yet: hold it,
      // and everything after it on this connection, until the group fsync.
      conn.held_buf.assign(out, reply_start, std::string::npos);
      out.resize(reply_start);
      waiting_.push_back(conn.fd());
    }
    if (group && wal->unsynced() >= config_.group_max_ops) maybe_group_commit(true);
  }

  conn.read_buf.erase(0, pos);
  if (conn.read_buf.size() > kMaxQueryBuffer) {
    reply_error(conn.held_buf.empty() ? conn.write_buf : conn.held_buf,
                "ERR Protocol error: query buffer limit exceeded");
    conn.close_after_flush = true;
  }
  if (conn.close_after_flush) conn.read_buf.clear();
}

bool Server::flush(Connection& conn) {
  while (conn.has_output()) {
    const ssize_t n = ::send(conn.fd(), conn.write_buf.data() + conn.write_pos,
                             conn.write_buf.size() - conn.write_pos, kSendFlags);
    if (n > 0) {
      conn.write_pos += static_cast<std::size_t>(n);
      continue;
    }
    if (n < 0 && errno == EINTR) continue;
    if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) break;  // POLLOUT will resume
    return false;  // EPIPE / ECONNRESET
  }
  if (conn.write_pos == conn.write_buf.size()) {
    conn.write_buf.clear();
    conn.write_pos = 0;
  } else if (conn.write_pos >= kWriteCompactThreshold && conn.write_pos * 2 >= conn.write_buf.size()) {
    conn.write_buf.erase(0, conn.write_pos);
    conn.write_pos = 0;
  }
  return true;
}

bool Server::should_close(const Connection& conn) const noexcept {
  if (conn.has_output() || !conn.held_buf.empty()) return false;  // flush first
  return conn.close_after_flush || (conn.peer_eof && !conn.input_paused);
}

void Server::close_conn(int fd) {
  conns_.erase(fd);  // ~Connection closes the fd: exactly once, on every path
}

}  // namespace kvstore
