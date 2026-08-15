// kvstore-cli -- an interactive terminal client.
//
// Two modes, chosen by whether a command was supplied on the argument line:
//
//   kvstore-cli                    interactive prompt, one command per line
//   kvstore-cli PING               execute once, print the reply, exit
//   echo "GET k" | kvstore-cli     read commands from a pipe, no prompt
//
// The one-shot form exits non-zero on an error reply, which makes it usable
// from shell scripts and as a container health check.
//
// This file deliberately shares no code with the server. It parses replies
// (server -> client), which the server never needs to do, and its buffering
// needs are a blocking socket and a std::string. Linking the server's event
// loop into a client binary to reuse a hundred lines would be the wrong trade.

#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cctype>
#include <cerrno>
#include <charconv>
#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <string>
#include <string_view>
#include <vector>

namespace {

constexpr std::string_view kCrlf = "\r\n";

// Guards against a malicious or malfunctioning server sending "*1\r\n" a
// million times deep and exhausting our stack.
constexpr int kMaxDepth = 16;

// ---------------------------------------------------------------------------
// Terminal colours
// ---------------------------------------------------------------------------

struct Palette {
  const char* green = "";
  const char* red = "";
  const char* dim = "";
  const char* reset = "";
};

// Colour only when stdout is a terminal. Piping into a file or another program
// must produce clean text, not escape sequences. NO_COLOR is honoured because
// users who set it mean it (see no-color.org).
Palette make_palette() {
  if (::isatty(STDOUT_FILENO) == 0) return {};
  if (std::getenv("NO_COLOR") != nullptr) return {};
  const char* term = std::getenv("TERM");
  if (term != nullptr && std::string_view(term) == "dumb") return {};
  return {"\033[32m", "\033[31m", "\033[2m", "\033[0m"};
}

// ---------------------------------------------------------------------------
// Reply model
// ---------------------------------------------------------------------------

enum class ReplyType { kSimple, kError, kInteger, kBulk, kNil, kArray };

struct Reply {
  ReplyType type = ReplyType::kNil;
  std::string text;                 // simple, error, bulk
  std::int64_t integer = 0;         // integer
  std::vector<Reply> elements;      // array
};

enum class ParseState { kOk, kIncomplete, kError };

bool parse_int(std::string_view text, std::int64_t& out) noexcept {
  if (text.empty()) return false;
  const auto result = std::from_chars(text.data(), text.data() + text.size(), out);
  return result.ec == std::errc{} && result.ptr == text.data() + text.size();
}

// Parses one reply from the front of `input`. Returns kIncomplete without
// consuming anything when the reply has not fully arrived -- the same
// restartable contract the server's request parser uses, for the same reason:
// TCP delivers arbitrary fragments, not messages.
ParseState parse_reply(std::string_view input, Reply& out, std::size_t& consumed,
                       int depth = 0) {
  if (depth > kMaxDepth) return ParseState::kError;
  if (input.empty()) return ParseState::kIncomplete;

  const std::size_t line_end = input.find(kCrlf);
  if (line_end == std::string_view::npos) {
    return input.size() > 512 ? ParseState::kError : ParseState::kIncomplete;
  }

  const char kind = input.front();
  const std::string_view head = input.substr(1, line_end - 1);
  const std::size_t after_head = line_end + kCrlf.size();

  switch (kind) {
    case '+':
      out = Reply{ReplyType::kSimple, std::string(head), 0, {}};
      consumed = after_head;
      return ParseState::kOk;

    case '-':
      out = Reply{ReplyType::kError, std::string(head), 0, {}};
      consumed = after_head;
      return ParseState::kOk;

    case ':': {
      std::int64_t value = 0;
      if (!parse_int(head, value)) return ParseState::kError;
      out = Reply{ReplyType::kInteger, {}, value, {}};
      consumed = after_head;
      return ParseState::kOk;
    }

    case '$': {
      std::int64_t length = 0;
      if (!parse_int(head, length)) return ParseState::kError;
      if (length < 0) {  // $-1 is the null bulk string
        out = Reply{ReplyType::kNil, {}, 0, {}};
        consumed = after_head;
        return ParseState::kOk;
      }
      const std::size_t body = static_cast<std::size_t>(length);
      if (input.size() < after_head + body + kCrlf.size()) return ParseState::kIncomplete;
      out = Reply{ReplyType::kBulk, std::string(input.substr(after_head, body)), 0, {}};
      consumed = after_head + body + kCrlf.size();
      return ParseState::kOk;
    }

    case '*': {
      std::int64_t count = 0;
      if (!parse_int(head, count)) return ParseState::kError;
      if (count < 0) {
        out = Reply{ReplyType::kNil, {}, 0, {}};
        consumed = after_head;
        return ParseState::kOk;
      }
      Reply array;
      array.type = ReplyType::kArray;
      array.elements.reserve(static_cast<std::size_t>(count));

      std::size_t offset = after_head;
      for (std::int64_t i = 0; i < count; ++i) {
        Reply element;
        std::size_t used = 0;
        const ParseState state =
            parse_reply(input.substr(offset), element, used, depth + 1);
        // An incomplete element makes the whole array incomplete: the caller
        // must retry from the very start once more bytes land.
        if (state != ParseState::kOk) return state;
        array.elements.push_back(std::move(element));
        offset += used;
      }
      out = std::move(array);
      consumed = offset;
      return ParseState::kOk;
    }

    default:
      return ParseState::kError;
  }
}

// ---------------------------------------------------------------------------
// Rendering
// ---------------------------------------------------------------------------

// Renders a bulk string the way a shell would want it back: printable bytes as
// they are, everything else escaped, so a value containing CRLF or a NUL cannot
// scramble the terminal.
std::string quote(std::string_view value) {
  std::string out;
  out.reserve(value.size() + 2);
  out += '"';
  for (const char raw : value) {
    const auto byte = static_cast<unsigned char>(raw);
    switch (raw) {
      case '"': out += "\\\""; break;
      case '\\': out += "\\\\"; break;
      case '\n': out += "\\n"; break;
      case '\r': out += "\\r"; break;
      case '\t': out += "\\t"; break;
      default:
        if (byte < 0x20 || byte == 0x7F) {
          static const char* kHex = "0123456789abcdef";
          out += "\\x";
          out += kHex[byte >> 4];
          out += kHex[byte & 0x0F];
        } else {
          out += raw;
        }
    }
  }
  out += '"';
  return out;
}

void print_reply(const Reply& reply, const Palette& palette, std::string indent = "") {
  switch (reply.type) {
    case ReplyType::kSimple:
      std::cout << indent << palette.green << reply.text << palette.reset << "\n";
      break;
    case ReplyType::kError:
      std::cout << indent << palette.red << "(error) " << reply.text << palette.reset << "\n";
      break;
    case ReplyType::kInteger:
      std::cout << indent << "(integer) " << reply.integer << "\n";
      break;
    case ReplyType::kBulk:
      std::cout << indent << palette.green << quote(reply.text) << palette.reset << "\n";
      break;
    case ReplyType::kNil:
      std::cout << indent << palette.dim << "(nil)" << palette.reset << "\n";
      break;
    case ReplyType::kArray:
      if (reply.elements.empty()) {
        std::cout << indent << palette.dim << "(empty array)" << palette.reset << "\n";
        break;
      }
      for (std::size_t i = 0; i < reply.elements.size(); ++i) {
        std::cout << indent << (i + 1) << ") ";
        // Nested arrays are indented; scalars continue on the numbered line.
        if (reply.elements[i].type == ReplyType::kArray) {
          std::cout << "\n";
          print_reply(reply.elements[i], palette, indent + "   ");
        } else {
          Reply scalar = reply.elements[i];
          print_reply(scalar, palette, "");
        }
      }
      break;
  }
}

// ---------------------------------------------------------------------------
// Connection
// ---------------------------------------------------------------------------

class Connection {
 public:
  ~Connection() {
    if (fd_ >= 0) ::close(fd_);
  }

  Connection() = default;
  Connection(const Connection&) = delete;
  Connection& operator=(const Connection&) = delete;

  // Resolves the host and connects. Blocking sockets are the right call for a
  // CLI: it has exactly one connection and nothing else to do while waiting.
  bool connect(const std::string& host, const std::string& port, std::string& error) {
    addrinfo hints{};
    hints.ai_family = AF_UNSPEC;  // accept IPv4 or IPv6
    hints.ai_socktype = SOCK_STREAM;

    addrinfo* results = nullptr;
    const int rc = ::getaddrinfo(host.c_str(), port.c_str(), &hints, &results);
    if (rc != 0) {
      error = "cannot resolve " + host + ":" + port + ": " + ::gai_strerror(rc);
      return false;
    }

    for (addrinfo* candidate = results; candidate != nullptr;
         candidate = candidate->ai_next) {
      const int fd = ::socket(candidate->ai_family, candidate->ai_socktype,
                              candidate->ai_protocol);
      if (fd < 0) continue;
      if (::connect(fd, candidate->ai_addr, candidate->ai_addrlen) == 0) {
        const int one = 1;
        ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
#if defined(SO_NOSIGPIPE)
        ::setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &one, sizeof(one));
#endif
        fd_ = fd;
        break;
      }
      ::close(fd);
    }

    ::freeaddrinfo(results);
    if (fd_ < 0) {
      error = "cannot connect to " + host + ":" + port + ": " + std::strerror(errno);
      return false;
    }
    return true;
  }

  // Encodes the tokens as a RESP array of bulk strings. Length-prefixing every
  // argument is what makes values containing spaces, quotes, or newlines work.
  bool send_command(const std::vector<std::string>& tokens, std::string& error) {
    // Appended piecewise rather than built with operator+: each `+` would
    // materialise a whole intermediate string, and this runs per command.
    std::string wire;
    wire += '*';
    wire += std::to_string(tokens.size());
    wire += kCrlf;
    for (const std::string& token : tokens) {
      wire += '$';
      wire += std::to_string(token.size());
      wire += kCrlf;
      wire += token;
      wire += kCrlf;
    }

    std::size_t sent = 0;
    while (sent < wire.size()) {
      const ssize_t n = ::send(fd_, wire.data() + sent, wire.size() - sent, kSendFlags);
      if (n < 0) {
        if (errno == EINTR) continue;
        error = std::string("send failed: ") + std::strerror(errno);
        return false;
      }
      sent += static_cast<std::size_t>(n);
    }
    return true;
  }

  // Reads until a complete reply has arrived, keeping any surplus bytes for the
  // next call -- a pipelined server may deliver several replies in one segment.
  bool read_reply(Reply& reply, std::string& error) {
    while (true) {
      std::size_t consumed = 0;
      const std::string_view pending(buffer_.data() + offset_, buffer_.size() - offset_);
      const ParseState state = parse_reply(pending, reply, consumed);

      if (state == ParseState::kOk) {
        offset_ += consumed;
        if (offset_ == buffer_.size()) {
          buffer_.clear();
          offset_ = 0;
        }
        return true;
      }
      if (state == ParseState::kError) {
        error = "malformed reply from server";
        return false;
      }

      // Incomplete: drop the already-consumed prefix, then read more.
      if (offset_ > 0) {
        buffer_.erase(0, offset_);
        offset_ = 0;
      }
      char chunk[8192];
      const ssize_t n = ::recv(fd_, chunk, sizeof(chunk), 0);
      if (n == 0) {
        error = "server closed the connection";
        return false;
      }
      if (n < 0) {
        if (errno == EINTR) continue;
        error = std::string("recv failed: ") + std::strerror(errno);
        return false;
      }
      buffer_.append(chunk, static_cast<std::size_t>(n));
    }
  }

 private:
#if defined(MSG_NOSIGNAL)
  static constexpr int kSendFlags = MSG_NOSIGNAL;
#else
  static constexpr int kSendFlags = 0;
#endif

  int fd_ = -1;
  std::string buffer_;
  std::size_t offset_ = 0;
};

// ---------------------------------------------------------------------------
// Input handling
// ---------------------------------------------------------------------------

// Splits a line into tokens, honouring double quotes and backslash escapes so
// that   SET greeting "hello world"   sends two arguments, not three.
// Returns false on an unterminated quote.
bool tokenize(std::string_view line, std::vector<std::string>& tokens) {
  tokens.clear();
  std::size_t i = 0;
  while (i < line.size()) {
    while (i < line.size() && (line[i] == ' ' || line[i] == '\t')) ++i;
    if (i >= line.size()) break;

    std::string token;
    if (line[i] == '"') {
      ++i;
      bool closed = false;
      while (i < line.size()) {
        if (line[i] == '\\' && i + 1 < line.size()) {
          ++i;
          switch (line[i]) {
            case 'n': token += '\n'; break;
            case 'r': token += '\r'; break;
            case 't': token += '\t'; break;
            default: token += line[i];
          }
          ++i;
          continue;
        }
        if (line[i] == '"') {
          ++i;
          closed = true;
          break;
        }
        token += line[i];
        ++i;
      }
      if (!closed) return false;
    } else {
      while (i < line.size() && line[i] != ' ' && line[i] != '\t') {
        token += line[i];
        ++i;
      }
    }
    tokens.push_back(std::move(token));
  }
  return true;
}

bool is_exit_command(const std::vector<std::string>& tokens) {
  if (tokens.size() != 1) return false;
  std::string lowered;
  for (const char c : tokens[0]) {
    lowered += static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
  }
  return lowered == "quit" || lowered == "exit";
}

struct Options {
  std::string host = "127.0.0.1";
  std::string port = "6379";
  std::vector<std::string> command;  // non-empty means one-shot mode
};

[[noreturn]] void usage(const char* program, int code) {
  std::cerr << "usage: " << program << " [--host <host>] [--port <port>] [command ...]\n"
            << "\n"
            << "  With no command, starts an interactive prompt.\n"
            << "  With a command, executes it once and exits (non-zero on an error reply).\n"
            << "\n"
            << "  --host <host>   server host (default 127.0.0.1)\n"
            << "  --port <port>   server port (default 6379)\n"
            << "  --help          this message\n";
  std::exit(code);
}

Options parse_options(int argc, char** argv) {
  Options options;
  int i = 1;
  for (; i < argc; ++i) {
    const std::string_view arg = argv[i];
    if (arg == "--help" || arg == "-h") {
      usage(argv[0], 0);
    } else if (arg == "--host") {
      if (i + 1 >= argc) usage(argv[0], 2);
      options.host = argv[++i];
    } else if (arg == "--port" || arg == "-p") {
      if (i + 1 >= argc) usage(argv[0], 2);
      options.port = argv[++i];
    } else if (arg.rfind("--", 0) == 0) {
      std::cerr << "unknown option: " << arg << "\n";
      usage(argv[0], 2);
    } else {
      break;  // first non-flag argument begins the one-shot command
    }
  }
  for (; i < argc; ++i) options.command.emplace_back(argv[i]);
  return options;
}

}  // namespace

int main(int argc, char** argv) {
  const Options options = parse_options(argc, argv);
  const Palette palette = make_palette();

  // A server that closes the connection mid-write must not kill this process.
  std::signal(SIGPIPE, SIG_IGN);

  Connection connection;
  std::string error;
  if (!connection.connect(options.host, options.port, error)) {
    std::cerr << palette.red << "(error) " << error << palette.reset << "\n";
    return 2;
  }

  auto run_once = [&](const std::vector<std::string>& tokens) -> int {
    if (!connection.send_command(tokens, error)) {
      std::cerr << palette.red << "(error) " << error << palette.reset << "\n";
      return 2;
    }
    Reply reply;
    if (!connection.read_reply(reply, error)) {
      std::cerr << palette.red << "(error) " << error << palette.reset << "\n";
      return 2;
    }
    print_reply(reply, palette);
    return reply.type == ReplyType::kError ? 1 : 0;
  };

  if (!options.command.empty()) {
    return run_once(options.command);
  }

  // Only draw a prompt for a human. When stdin is a pipe the prompt would be
  // interleaved into the captured output as noise.
  const bool interactive = ::isatty(STDIN_FILENO) != 0;
  const std::string prompt = options.host + ":" + options.port + "> ";

  std::string line;
  while (true) {
    if (interactive) std::cout << prompt << std::flush;
    if (!std::getline(std::cin, line)) {
      // End of input: Ctrl-D, or the pipe ran dry.
      if (interactive) std::cout << "\n";
      break;
    }

    std::vector<std::string> tokens;
    if (!tokenize(line, tokens)) {
      std::cerr << palette.red << "(error) unbalanced quotes" << palette.reset << "\n";
      continue;
    }
    if (tokens.empty()) continue;
    if (is_exit_command(tokens)) break;

    if (run_once(tokens) == 2) return 2;  // connection is gone; do not loop
  }
  return 0;
}
