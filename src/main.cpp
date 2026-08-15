// Entry point: parse options, recover from the log, serve, shut down cleanly.

#include <unistd.h>

#include <atomic>
#include <charconv>
#include <csignal>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <string>
#include <string_view>

#include "kvstore/server.hpp"
#include "kvstore/storage.hpp"
#include "kvstore/wal.hpp"

namespace {

// The write end of a pipe the main thread is blocked reading. A signal handler
// runs on a borrowed stack at an arbitrary instant, possibly in the middle of
// malloc(); almost nothing is safe to call from one. write() on a descriptor is
// on the short list that is, so the handler's entire job is to write one byte
// and let ordinary code react to it. This is the self-pipe trick.
volatile std::sig_atomic_t g_signal_pipe = -1;

extern "C" void handle_signal(int) {
  if (g_signal_pipe >= 0) {
    const char byte = 's';
    const ssize_t rc = ::write(g_signal_pipe, &byte, 1);
    (void)rc;  // nothing useful to do with a failure here
  }
}

struct Options {
  std::string bind_address = "127.0.0.1";
  std::uint16_t port = 6379;
  std::string wal_path = "wal.log";
  int io_threads = 4;
  bool fsync_always = true;
};

[[noreturn]] void usage(const char* program, int code) {
  std::cerr << "usage: " << program << " [options]\n"
            << "  --bind <address>    interface to listen on (default 127.0.0.1)\n"
            << "  --port <port>       TCP port, 0 for an ephemeral one (default 6379)\n"
            << "  --wal <path>        write-ahead log file (default wal.log)\n"
            << "  --threads <n>       I/O threads (default 4)\n"
            << "  --no-fsync          skip the per-write flush; faster, not crash-safe\n"
            << "  --help              this message\n";
  std::exit(code);
}

bool parse_number(std::string_view text, long& out) {
  const auto result = std::from_chars(text.data(), text.data() + text.size(), out);
  return result.ec == std::errc{} && result.ptr == text.data() + text.size();
}

Options parse_options(int argc, char** argv) {
  Options options;
  for (int i = 1; i < argc; ++i) {
    const std::string_view arg = argv[i];
    auto next = [&](const char* name) -> std::string_view {
      if (i + 1 >= argc) {
        std::cerr << "missing value for " << name << "\n";
        usage(argv[0], 2);
      }
      return argv[++i];
    };

    if (arg == "--help" || arg == "-h") {
      usage(argv[0], 0);
    } else if (arg == "--bind") {
      options.bind_address = std::string(next("--bind"));
    } else if (arg == "--port") {
      long value = 0;
      if (!parse_number(next("--port"), value) || value < 0 || value > 65535) {
        std::cerr << "invalid port\n";
        usage(argv[0], 2);
      }
      options.port = static_cast<std::uint16_t>(value);
    } else if (arg == "--wal") {
      options.wal_path = std::string(next("--wal"));
    } else if (arg == "--threads") {
      long value = 0;
      if (!parse_number(next("--threads"), value) || value < 1 || value > 256) {
        std::cerr << "invalid thread count\n";
        usage(argv[0], 2);
      }
      options.io_threads = static_cast<int>(value);
    } else if (arg == "--no-fsync") {
      options.fsync_always = false;
    } else {
      std::cerr << "unknown option: " << arg << "\n";
      usage(argv[0], 2);
    }
  }
  return options;
}

}  // namespace

int main(int argc, char** argv) {
  const Options options = parse_options(argc, argv);

  // Flush every log line as it is written. When stdout is a pipe or a file it
  // is block-buffered by default, so a server killed with SIGKILL loses
  // whatever is still sitting in the buffer -- which is exactly the output you
  // need when diagnosing why it died.
  std::cout.setf(std::ios::unitbuf);

  // Writing to a socket whose peer has gone away raises SIGPIPE, and the
  // default disposition for SIGPIPE is to kill the process. A network server
  // must ignore it and deal with the EPIPE that send() returns instead.
  std::signal(SIGPIPE, SIG_IGN);

  try {
    kvstore::Wal wal(options.wal_path, options.fsync_always ? kvstore::SyncPolicy::kAlways
                                                            : kvstore::SyncPolicy::kNever);
    kvstore::Storage storage(&wal);

    // Recovery happens before the listening socket exists, so no client can
    // ever observe a half-restored store.
    const std::size_t replayed = storage.load_from_wal();
    wal.open();
    std::cout << "recovered " << replayed << " log record(s), " << storage.size()
              << " live key(s) from " << wal.path() << "\n";

    kvstore::ServerConfig config;
    config.bind_address = options.bind_address;
    config.port = options.port;
    config.io_threads = options.io_threads;

    kvstore::Server server(config, storage);
    server.start();
    std::cout << "listening on " << options.bind_address << ":" << server.port() << " with "
              << options.io_threads << " I/O thread(s)"
              << (options.fsync_always ? "" : " [fsync disabled]") << "\n";

    int pipe_fds[2];
    if (::pipe(pipe_fds) < 0) {
      std::cerr << "pipe: " << std::strerror(errno) << "\n";
      return 1;
    }
    g_signal_pipe = pipe_fds[1];
    std::signal(SIGINT, handle_signal);
    std::signal(SIGTERM, handle_signal);

    // Park here until a signal arrives. read() returns EINTR when the handler
    // runs, so the wait is a loop, not a single call.
    char byte = 0;
    ssize_t rc = 0;
    do {
      rc = ::read(pipe_fds[0], &byte, 1);
    } while (rc < 0 && errno == EINTR);

    std::cout << "\nshutting down\n";
    server.stop();
    // Server's destructor joins the threads; the Wal's closes the descriptor.
    // Nothing here needs an explicit cleanup path, including the error paths.
    ::close(pipe_fds[0]);
    ::close(pipe_fds[1]);
    return 0;
  } catch (const std::exception& error) {
    std::cerr << "fatal: " << error.what() << "\n";
    return 1;
  }
}
