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
#include <charconv>
#include <chrono>
#include <cstring>
#include <stdexcept>
#include <system_error>

namespace kvstore {
namespace {

// Read this much per recv() call. Large enough that a pipelined burst usually
// arrives in one syscall, small enough that an idle connection is not holding
// a megabyte of untouched buffer.
constexpr std::size_t kReadChunk = 16 * 1024;

// A single request may not exceed this. Without a ceiling, a client that opens
// a socket and streams bytes with no terminator would grow the buffer until the
// process is killed by the OOM reaper.
constexpr std::size_t kMaxInputBacklog = 128 * 1024 * 1024;

// Compact the output buffer once this many already-sent bytes have piled up in
// front of the unsent remainder.
constexpr std::size_t kOutputCompactThreshold = 64 * 1024;

// Linux can suppress SIGPIPE per-call; macOS does it per-socket instead (see
// SO_NOSIGPIPE below). Either way SIGPIPE is also ignored process-wide in
// main(), because the default action for it is to terminate the process -- one
// client closing its socket at the wrong moment would kill the whole server.
#if defined(MSG_NOSIGNAL)
constexpr int kSendFlags = MSG_NOSIGNAL;
#else
constexpr int kSendFlags = 0;
#endif

void close_fd(int& fd) noexcept {
  if (fd >= 0) {
    ::close(fd);
    fd = -1;
  }
}

}  // namespace

void set_nonblocking(int fd) {
  // Read-modify-write, never a bare F_SETFL: the descriptor may already carry
  // flags we have no business clearing.
  const int flags = ::fcntl(fd, F_GETFL, 0);
  if (flags < 0) {
    throw std::system_error(errno, std::generic_category(), "fcntl F_GETFL");
  }
  if (::fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) {
    throw std::system_error(errno, std::generic_category(), "fcntl F_SETFL");
  }
}

// ---------------------------------------------------------------------------
// Buffer
// ---------------------------------------------------------------------------

void Buffer::consume(std::size_t n) noexcept {
  read_pos_ += n;
  if (read_pos_ >= write_pos_) {
    // Fully drained: reset both cursors so the next fill starts at offset 0
    // and the buffer never creeps forward forever.
    read_pos_ = write_pos_ = 0;
  }
}

void Buffer::compact() noexcept {
  if (read_pos_ == 0) return;
  const std::size_t live = write_pos_ - read_pos_;
  if (live > 0) {
    std::memmove(data_.data(), data_.data() + read_pos_, live);
  }
  read_pos_ = 0;
  write_pos_ = live;
}

void Buffer::ensure_writable(std::size_t n) {
  if (writable() >= n) return;
  // Try reclaiming the consumed prefix before asking the allocator. A steady
  // request/response workload almost always fits after this and never grows.
  compact();
  if (writable() >= n) return;
  data_.resize(write_pos_ + n);
}

void Buffer::append(std::string_view bytes) {
  ensure_writable(bytes.size());
  std::memcpy(write_ptr(), bytes.data(), bytes.size());
  commit(bytes.size());
}

// ---------------------------------------------------------------------------
// Connection
// ---------------------------------------------------------------------------

Connection::~Connection() {
  // RAII: whichever way control leaves the owning scope -- normal return,
  // early return, or a thrown exception -- the descriptor is released exactly
  // once. Leaked descriptors are permanent; the process cannot get them back.
  close_fd(fd_);
}

bool Connection::fill(bool& closed) {
  closed = false;
  while (true) {
    if (input_.size() > kMaxInputBacklog) {
      reply_error(output_, "ERR request too large");
      close_requested_ = true;
      return false;
    }

    input_.ensure_writable(kReadChunk);
    const ssize_t n = ::recv(fd_, input_.write_ptr(), input_.writable(), 0);
    if (n > 0) {
      input_.commit(static_cast<std::size_t>(n));
      // A short read does not mean "no more data". Keep going until the kernel
      // says EAGAIN; that is the only reliable end-of-readable signal on a
      // non-blocking socket.
      continue;
    }
    if (n == 0) {
      // Orderly shutdown: the peer sent FIN. Not an error.
      closed = true;
      return true;
    }
    if (errno == EINTR) continue;
    if (errno == EAGAIN || errno == EWOULDBLOCK) return true;  // drained
    return false;                                              // real error
  }
}

bool Connection::process(Storage& storage) {
  Command command;
  while (true) {
    const std::string_view view = input_.readable();
    if (view.empty()) break;

    const ParseResult result = parse_command(view, command);
    if (result.status == ParseStatus::kIncomplete) {
      // A partial command. Leave the bytes in place; the next read appends to
      // them and the next call to process() sees the whole thing. This is the
      // entire answer to TCP stream framing.
      break;
    }
    if (result.status == ParseStatus::kError) {
      reply_error(output_, result.error);
      close_requested_ = true;
      return false;
    }

    if (command.kind != CommandKind::kEmpty) {
      // command.args are views into input_. Executing appends to output_ and
      // never touches input_, so they stay valid for the duration of the call.
      execute_command(command, storage, output_);
    }
    // Safe here and only here: consume() moves a cursor and never moves bytes,
    // and the views from this iteration are dead by now anyway.
    input_.consume(result.consumed);
  }
  return true;
}

bool Connection::flush(bool& blocked) {
  blocked = false;
  while (output_sent_ < output_.size()) {
    const ssize_t n = ::send(fd_, output_.data() + output_sent_,
                             output_.size() - output_sent_, kSendFlags);
    if (n > 0) {
      output_sent_ += static_cast<std::size_t>(n);
      continue;
    }
    if (n < 0 && errno == EINTR) continue;
    if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
      // The kernel's socket send buffer is full because the peer is slow or
      // has stopped reading. The unsent tail stays queued and the event loop
      // registers POLLOUT to be told when there is room again. Dropping it, or
      // looping until it fits, are the two classic ways to break a server.
      blocked = true;
      return true;
    }
    return false;
  }

  if (output_sent_ == output_.size()) {
    output_.clear();
    output_sent_ = 0;
  } else if (output_sent_ > kOutputCompactThreshold) {
    output_.erase(0, output_sent_);
    output_sent_ = 0;
  }
  return true;
}

// ---------------------------------------------------------------------------
// Command dispatch
// ---------------------------------------------------------------------------

void execute_command(const Command& command, Storage& storage, std::string& out) {
  switch (command.kind) {
    case CommandKind::kPing:
      if (command.arity() == 1) {
        reply_simple(out, "PONG");
      } else if (command.arity() == 2) {
        reply_bulk(out, command.args[1]);
      } else {
        reply_error(out, "ERR wrong number of arguments for 'ping' command");
      }
      return;

    case CommandKind::kSet:
      if (command.arity() != 3) {
        reply_error(out, "ERR wrong number of arguments for 'set' command");
        return;
      }
      storage.set(command.args[1], command.args[2]);
      reply_simple(out, "OK");
      return;

    case CommandKind::kGet: {
      if (command.arity() != 2) {
        reply_error(out, "ERR wrong number of arguments for 'get' command");
        return;
      }
      const auto value = storage.get(command.args[1]);
      if (value) {
        reply_bulk(out, *value);
      } else {
        reply_nil(out);
      }
      return;
    }

    case CommandKind::kDel: {
      if (command.arity() < 2) {
        reply_error(out, "ERR wrong number of arguments for 'del' command");
        return;
      }
      std::int64_t removed = 0;
      for (std::size_t i = 1; i < command.args.size(); ++i) {
        if (storage.del(command.args[i])) ++removed;
      }
      reply_integer(out, removed);
      return;
    }

    case CommandKind::kExpire: {
      if (command.arity() != 3) {
        reply_error(out, "ERR wrong number of arguments for 'expire' command");
        return;
      }
      std::int64_t seconds = 0;
      const std::string_view raw = command.args[2];
      const auto parsed = std::from_chars(raw.data(), raw.data() + raw.size(), seconds);
      if (parsed.ec != std::errc{} || parsed.ptr != raw.data() + raw.size()) {
        reply_error(out, "ERR value is not an integer or out of range");
        return;
      }
      reply_integer(out, storage.expire(command.args[1], seconds) ? 1 : 0);
      return;
    }

    case CommandKind::kCommand:
      // redis-cli issues COMMAND DOCS on connect. An empty array is a valid
      // answer and keeps the official client usable against this server.
      reply_empty_array(out);
      return;

    case CommandKind::kEmpty:
      return;

    case CommandKind::kUnknown:
      break;
  }

  std::string message = "ERR unknown command '";
  message.append(command.name);
  message += '\'';
  reply_error(out, message);
}

// ---------------------------------------------------------------------------
// EventLoop
// ---------------------------------------------------------------------------

EventLoop::EventLoop(Server& server) : server_(server) {
  int fds[2];
  if (::pipe(fds) < 0) {
    throw std::system_error(errno, std::generic_category(), "pipe");
  }
  wake_read_fd_ = fds[0];
  wake_write_fd_ = fds[1];
  // Both ends non-blocking: the reader must not stall when the pipe is empty,
  // and the writer must not stall when it is full (a full wakeup pipe already
  // means "you have been woken", so a dropped byte costs nothing).
  set_nonblocking(wake_read_fd_);
  set_nonblocking(wake_write_fd_);
}

EventLoop::~EventLoop() {
  connections_.clear();
  {
    std::lock_guard<std::mutex> lock(pending_mutex_);
    for (int fd : pending_fds_) ::close(fd);
    pending_fds_.clear();
  }
  close_fd(wake_read_fd_);
  close_fd(wake_write_fd_);
}

void EventLoop::wake() {
  const char byte = 'w';
  ssize_t rc;
  do {
    rc = ::write(wake_write_fd_, &byte, 1);
  } while (rc < 0 && errno == EINTR);
  // EAGAIN is deliberately ignored: see the constructor.
}

void EventLoop::drain_wakeup() {
  char scratch[64];
  while (::read(wake_read_fd_, scratch, sizeof(scratch)) > 0) {
    // Level-triggered poll re-reports a readable pipe forever, so it must be
    // emptied or the loop spins at 100% CPU.
  }

  std::vector<int> adopted;
  {
    std::lock_guard<std::mutex> lock(pending_mutex_);
    adopted.swap(pending_fds_);
  }
  for (int fd : adopted) {
    connections_.emplace(fd, std::make_unique<Connection>(fd));
  }
}

void EventLoop::adopt(int fd) {
  {
    std::lock_guard<std::mutex> lock(pending_mutex_);
    pending_fds_.push_back(fd);
  }
  wake();
}

void EventLoop::accept_ready() {
  while (true) {
    sockaddr_in peer{};
    socklen_t peer_len = sizeof(peer);
    const int client = ::accept(listen_fd_, reinterpret_cast<sockaddr*>(&peer), &peer_len);
    if (client < 0) {
      if (errno == EINTR) continue;
      // EAGAIN: the backlog is empty, every pending connection has been taken.
      if (errno == EAGAIN || errno == EWOULDBLOCK) return;
      // EMFILE/ENFILE: out of descriptors. Refusing this one and staying alive
      // is the only sane response; the loop will try again on the next event.
      if (errno == EMFILE || errno == ENFILE) return;
      if (errno == ECONNABORTED) continue;  // peer vanished mid-handshake
      return;
    }

    try {
      set_nonblocking(client);
      const int one = 1;
#if defined(SO_NOSIGPIPE)
      ::setsockopt(client, SOL_SOCKET, SO_NOSIGPIPE, &one, sizeof(one));
#endif
      // Nagle's algorithm delays a small packet hoping to coalesce it with the
      // next one. For a request/response store that is pure added latency: the
      // reply is the whole message and there is nothing to coalesce with.
      ::setsockopt(client, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
      server_.distribute(client);
    } catch (const std::exception&) {
      ::close(client);
    }
  }
}

void EventLoop::close_connection(int fd) { connections_.erase(fd); }

void EventLoop::handle_connection(int fd, short revents) {
  auto it = connections_.find(fd);
  if (it == connections_.end()) return;
  Connection& conn = *it->second;

  bool healthy = true;
  bool peer_closed = false;

  if (revents & (POLLERR | POLLNVAL)) {
    healthy = false;
  }

  if (healthy && (revents & POLLIN)) {
    if (!conn.fill(peer_closed)) healthy = false;
    if (healthy && !conn.process(server_.storage())) {
      // Protocol error: the error reply is already queued. Push it out below,
      // then drop the connection.
      conn.mark_for_close();
    }
  }

  bool blocked = false;
  if (healthy && conn.has_pending_output()) {
    if (!conn.flush(blocked)) healthy = false;
  }

  // POLLHUP means the peer closed its end. Anything already buffered here is
  // undeliverable, so there is no point holding the connection open.
  if (revents & POLLHUP) peer_closed = true;

  if (!healthy || peer_closed || (conn.close_requested() && !conn.has_pending_output())) {
    close_connection(fd);
  }
}

void EventLoop::run() {
  running_.store(true, std::memory_order_relaxed);

  std::vector<struct pollfd> pfds;
  std::vector<int> ready;

  while (running_.load(std::memory_order_relaxed)) {
    pfds.clear();
    pfds.push_back({wake_read_fd_, POLLIN, 0});
    if (listen_fd_ >= 0) pfds.push_back({listen_fd_, POLLIN, 0});

    for (const auto& [fd, conn] : connections_) {
      short events = POLLIN;
      // Only ask about writability when there is something to write. Asking
      // unconditionally would make poll() return immediately every time, since
      // an idle socket is almost always writable -- a busy loop.
      if (conn->has_pending_output()) events = static_cast<short>(events | POLLOUT);
      pfds.push_back({fd, events, 0});
    }

    // The timeout is a safety net, not the wakeup mechanism: the self-pipe
    // handles that. It just guarantees the loop cannot sit forever if a wakeup
    // is ever lost.
    const int rc = ::poll(pfds.data(), static_cast<nfds_t>(pfds.size()), 200);
    if (rc < 0) {
      if (errno == EINTR) continue;  // a signal arrived; not an error
      break;
    }
    if (rc == 0) continue;

    if (pfds[0].revents != 0) drain_wakeup();
    if (!running_.load(std::memory_order_relaxed)) break;

    std::size_t index = 1;
    if (listen_fd_ >= 0) {
      if (pfds[index].revents & POLLIN) accept_ready();
      ++index;
    }

    // Snapshot the ready descriptors before dispatching. Handling one
    // connection can erase it from connections_, and pfds holds plain ints, so
    // each is re-looked-up by fd rather than through a stale iterator.
    ready.clear();
    for (; index < pfds.size(); ++index) {
      if (pfds[index].revents != 0) {
        ready.push_back(pfds[index].fd);
      }
    }
    for (std::size_t i = 0, n = ready.size(); i < n; ++i) {
      const int fd = ready[i];
      auto it = connections_.find(fd);
      if (it == connections_.end()) continue;
      short revents = 0;
      for (const auto& pfd : pfds) {
        if (pfd.fd == fd) {
          revents = pfd.revents;
          break;
        }
      }
      handle_connection(fd, revents);
    }
  }

  running_.store(false, std::memory_order_relaxed);
  connections_.clear();
}

void EventLoop::stop() {
  running_.store(false, std::memory_order_relaxed);
  wake();
}

// ---------------------------------------------------------------------------
// Server
// ---------------------------------------------------------------------------

Server::Server(ServerConfig config, Storage& storage)
    : config_(std::move(config)), storage_(storage) {
  if (config_.io_threads < 1) config_.io_threads = 1;
}

Server::~Server() {
  stop();
  for (auto& thread : io_threads_) {
    if (thread.joinable()) thread.join();
  }
  if (maintenance_thread_.joinable()) maintenance_thread_.join();
  close_fd(listen_fd_);
}

void Server::open_listener() {
  // A socket is created, then configured, then bound to an address, then
  // switched into listening mode. Only after listen() does the kernel start
  // completing handshakes and queueing connections for accept().
  const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) {
    throw std::system_error(errno, std::generic_category(), "socket");
  }

  const int one = 1;
  // Without SO_REUSEADDR, restarting the server fails with EADDRINUSE for up
  // to a couple of minutes, because sockets from the previous run linger in
  // TIME_WAIT holding the port.
  if (::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one)) < 0) {
    const int saved = errno;
    ::close(fd);
    throw std::system_error(saved, std::generic_category(), "setsockopt SO_REUSEADDR");
  }

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  // Network byte order is big-endian, whatever this machine happens to be.
  // htons/ntohs are macros on some platforms, so they are called unqualified.
  addr.sin_port = htons(config_.port);
  if (::inet_pton(AF_INET, config_.bind_address.c_str(), &addr.sin_addr) != 1) {
    ::close(fd);
    throw std::runtime_error("invalid bind address: " + config_.bind_address);
  }

  if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
    const int saved = errno;
    ::close(fd);
    throw std::system_error(saved, std::generic_category(), "bind");
  }
  if (::listen(fd, SOMAXCONN) < 0) {
    const int saved = errno;
    ::close(fd);
    throw std::system_error(saved, std::generic_category(), "listen");
  }

  // With config_.port == 0 the kernel chose a free port; ask it which.
  sockaddr_in actual{};
  socklen_t actual_len = sizeof(actual);
  if (::getsockname(fd, reinterpret_cast<sockaddr*>(&actual), &actual_len) == 0) {
    bound_port_ = ntohs(actual.sin_port);
  } else {
    bound_port_ = config_.port;
  }

  // The listener is non-blocking too, so accept() can be driven by poll()
  // instead of parking a thread inside it.
  set_nonblocking(fd);
  listen_fd_ = fd;
}

void Server::start() {
  if (running_.exchange(true)) return;

  open_listener();

  loops_.reserve(static_cast<std::size_t>(config_.io_threads));
  for (int i = 0; i < config_.io_threads; ++i) {
    loops_.push_back(std::make_unique<EventLoop>(*this));
  }
  loops_.front()->set_listener(listen_fd_);

  io_threads_.reserve(loops_.size());
  for (auto& loop : loops_) {
    io_threads_.emplace_back([raw = loop.get()] { raw->run(); });
  }

  maintenance_thread_ = std::thread([this] { maintenance_loop(); });
}

void Server::distribute(int client_fd) {
  const std::size_t index = next_loop_.fetch_add(1, std::memory_order_relaxed) % loops_.size();
  loops_[index]->adopt(client_fd);
}

void Server::maintenance_loop() {
  using namespace std::chrono_literals;
  const auto slice = 50ms;
  auto elapsed = 0ms;
  const auto interval = std::chrono::milliseconds(config_.purge_interval_ms);

  while (running_.load(std::memory_order_relaxed)) {
    // Sleep in short slices instead of one long one so shutdown is prompt.
    std::this_thread::sleep_for(slice);
    elapsed += slice;
    if (elapsed < interval) continue;
    elapsed = 0ms;
    storage_.purge_expired();
  }
}

void Server::run() {
  start();
  for (auto& thread : io_threads_) {
    if (thread.joinable()) thread.join();
  }
  if (maintenance_thread_.joinable()) maintenance_thread_.join();
}

void Server::stop() {
  if (!running_.exchange(false)) return;
  for (auto& loop : loops_) loop->stop();
}

}  // namespace kvstore
