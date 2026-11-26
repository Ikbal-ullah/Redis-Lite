// Entry point: parse flags, recover from the WAL, serve until SIGINT/SIGTERM.

#include <csignal>
#include <cstdlib>
#include <iostream>
#include <string>
#include <string_view>

#include "kvstore/database.hpp"
#include "kvstore/server.hpp"
#include "kvstore/wal.hpp"

namespace {

kvstore::Server* g_server = nullptr;

// Async-signal-safe: request_stop() is an atomic store plus write(2).
extern "C" void on_signal(int) {
  if (g_server != nullptr) g_server->request_stop();
}

struct Options {
  kvstore::ServerConfig server;
  std::string wal_path = "wal.log";
  kvstore::FsyncPolicy fsync = kvstore::FsyncPolicy::kAlways;
};

[[noreturn]] void usage(const char* program, int code) {
  (code == 0 ? std::cout : std::cerr)
      << "usage: " << program << " [options]\n"
      << "  --bind <addr>             interface to listen on (default 127.0.0.1)\n"
      << "  --port <port>             TCP port, 0 = ephemeral (default 6379)\n"
      << "  --wal <path>              write-ahead log (default wal.log)\n"
      << "  --fsync no|group|always   durability policy (default always)\n"
      << "  --group-window-ms <n>     group commit: max wait before fsync (default 10)\n"
      << "  --group-max-ops <n>       group commit: max records per fsync (default 64)\n";
  std::exit(code);
}

long parse_long(std::string_view text, long lo, long hi, const char* program) {
  char* end = nullptr;
  const std::string copy(text);
  const long value = std::strtol(copy.c_str(), &end, 10);
  if (copy.empty() || *end != '\0' || value < lo || value > hi) {
    std::cerr << "invalid number: " << text << "\n";
    usage(program, 2);
  }
  return value;
}

Options parse_options(int argc, char** argv) {
  Options o;
  for (int i = 1; i < argc; ++i) {
    const std::string_view arg = argv[i];
    auto value = [&]() -> std::string_view {
      if (i + 1 >= argc) usage(argv[0], 2);
      return argv[++i];
    };
    if (arg == "--help" || arg == "-h") {
      usage(argv[0], 0);
    } else if (arg == "--bind") {
      o.server.bind_address = std::string(value());
    } else if (arg == "--port") {
      o.server.port = static_cast<std::uint16_t>(parse_long(value(), 0, 65535, argv[0]));
    } else if (arg == "--wal") {
      o.wal_path = std::string(value());
    } else if (arg == "--fsync") {
      const std::string_view mode = value();
      if (mode == "no") o.fsync = kvstore::FsyncPolicy::kNo;
      else if (mode == "group") o.fsync = kvstore::FsyncPolicy::kGroup;
      else if (mode == "always") o.fsync = kvstore::FsyncPolicy::kAlways;
      else usage(argv[0], 2);
    } else if (arg == "--group-window-ms") {
      o.server.group_window_ms = static_cast<int>(parse_long(value(), 1, 10000, argv[0]));
    } else if (arg == "--group-max-ops") {
      o.server.group_max_ops = static_cast<std::uint64_t>(parse_long(value(), 1, 1 << 20, argv[0]));
    } else {
      std::cerr << "unknown option: " << arg << "\n";
      usage(argv[0], 2);
    }
  }
  return o;
}

const char* policy_name(kvstore::FsyncPolicy p) {
  switch (p) {
    case kvstore::FsyncPolicy::kNo: return "no";
    case kvstore::FsyncPolicy::kGroup: return "group";
    case kvstore::FsyncPolicy::kAlways: return "always";
  }
  return "?";
}

}  // namespace

int main(int argc, char** argv) {
  const Options options = parse_options(argc, argv);
  std::cout.setf(std::ios::unitbuf);  // a SIGKILLed server must not lose its log lines
  std::signal(SIGPIPE, SIG_IGN);      // a vanished peer is an EPIPE, not process death

  try {
    kvstore::Wal wal(options.wal_path, options.fsync);
    kvstore::Database db(&wal);

    // Recovery completes before the socket exists: no client can observe a
    // half-restored keyspace.
    const kvstore::ReplayStats stats = db.recover(kvstore::now_ms());
    wal.open();
    std::cout << "recovered " << stats.records << " record(s), " << db.storage().size()
              << " live key(s) from " << wal.path() << " (" << stats.valid_bytes << " bytes";
    if (stats.truncated_bytes > 0) std::cout << ", truncated torn tail of " << stats.truncated_bytes << " bytes";
    std::cout << ")\n";

    kvstore::Server server(options.server, db);
    server.listen();
    g_server = &server;
    std::signal(SIGINT, on_signal);
    std::signal(SIGTERM, on_signal);
    std::cout << "listening on " << options.server.bind_address << ":" << server.port()
              << " fsync=" << policy_name(options.fsync) << "\n";

    server.run();
    g_server = nullptr;
    std::cout << "shut down cleanly\n";
    return 0;
  } catch (const kvstore::WalCorruption& e) {
    std::cerr << "fatal: " << e.what() << "\n";
    return 3;
  } catch (const std::exception& e) {
    std::cerr << "fatal: " << e.what() << "\n";
    return 1;
  }
}
