// RESP wire protocol: bytes in, command out; replies out, bytes in.
//
// This layer knows nothing about sockets and nothing about storage. It is a
// pure function over a byte range, which is what makes it trivially testable:
// the protocol tests never open a file descriptor.
//
// Two request encodings are accepted.
//
//   1. RESP arrays of bulk strings -- what redis-cli and every real client
//      sends:
//
//          *3\r\n$3\r\nSET\r\n$5\r\nhello\r\n$5\r\nworld\r\n
//
//      Read as: an array of 3 elements; each element is a bulk string with an
//      explicit byte length, so values may contain spaces, CRLF, or NUL.
//
//   2. Inline commands -- a plain whitespace-separated line ending in \n:
//
//          SET hello world\n
//
//      Real Redis supports this and so do we, because it means `nc localhost
//      6379` and telnet are usable debugging tools.
//
// Zero-copy and its price
// -----------------------
// A parsed Command holds std::string_view args that point *into* the caller's
// buffer. Nothing is copied and nothing is allocated. The cost is a lifetime
// contract the caller must honour:
//
//     A Command is valid only until the underlying buffer is appended to,
//     compacted, or destroyed.
//
// The server satisfies this by draining the socket completely first, then
// parsing and executing from the now-frozen buffer, and only afterwards
// reading again.

#ifndef KVSTORE_PROTOCOL_HPP
#define KVSTORE_PROTOCOL_HPP

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace kvstore {

// Defensive limits. A protocol parser is the part of the server an unauthenticated
// stranger talks to first, so every length it reads off the wire is treated as
// hostile until proven otherwise. Without these, "$4294967295\r\n" is a remote
// out-of-memory kill.
inline constexpr std::size_t kMaxArgs = 1024;
inline constexpr std::size_t kMaxBulkLength = 64u * 1024u * 1024u;  // 64 MiB
inline constexpr std::size_t kMaxInlineLength = 64u * 1024u;        // 64 KiB

enum class CommandKind {
  kEmpty,  // a blank inline line; consume it and say nothing
  kPing,
  kSet,
  kGet,
  kDel,
  kExpire,
  kCommand,  // redis-cli sends COMMAND DOCS on connect
  kUnknown,
};

struct Command {
  CommandKind kind = CommandKind::kUnknown;
  std::string_view name;                // as received, for error messages
  std::vector<std::string_view> args;   // args[0] is the command name

  std::size_t arity() const noexcept { return args.size(); }
};

enum class ParseStatus {
  kOk,          // `consumed` bytes yielded one complete command
  kIncomplete,  // need more bytes; consumed == 0
  kError,       // malformed beyond recovery; connection should be closed
};

struct ParseResult {
  ParseStatus status = ParseStatus::kIncomplete;
  std::size_t consumed = 0;
  std::string error;  // populated only when status == kError
};

// Attempts to parse exactly one command from the front of `input`.
// On kOk, `out` is overwritten and `consumed` bytes may be retired.
ParseResult parse_command(std::string_view input, Command& out);

// Maps a command name to its kind, case-insensitively ("get", "GET", "GeT").
CommandKind classify(std::string_view name) noexcept;

// Reply encoders. Each appends to `out` rather than returning a string, so a
// pipelined batch of replies accumulates in one buffer and leaves in one write.
void reply_simple(std::string& out, std::string_view status);   // +OK\r\n
void reply_error(std::string& out, std::string_view message);   // -ERR ...\r\n
void reply_bulk(std::string& out, std::string_view value);      // $5\r\nhello\r\n
void reply_nil(std::string& out);                               // $-1\r\n
void reply_integer(std::string& out, std::int64_t value);       // :1\r\n
void reply_empty_array(std::string& out);                       // *0\r\n

}  // namespace kvstore

#endif  // KVSTORE_PROTOCOL_HPP
