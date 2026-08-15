#include "kvstore/protocol.hpp"

#include <algorithm>
#include <cctype>
#include <charconv>
#include <limits>

namespace kvstore {
namespace {

constexpr std::string_view kCrlf = "\r\n";

bool iequals(std::string_view a, std::string_view b) noexcept {
  if (a.size() != b.size()) return false;
  for (std::size_t i = 0; i < a.size(); ++i) {
    const auto ca = static_cast<unsigned char>(a[i]);
    const auto cb = static_cast<unsigned char>(b[i]);
    if (std::tolower(ca) != std::tolower(cb)) return false;
  }
  return true;
}

// Strict signed decimal. Rejects "12abc", "", "+3", and anything that would
// overflow -- std::from_chars reports all of it without ever throwing.
bool parse_int(std::string_view text, std::int64_t& out) noexcept {
  if (text.empty()) return false;
  const char* first = text.data();
  const char* last = text.data() + text.size();
  const auto result = std::from_chars(first, last, out);
  return result.ec == std::errc{} && result.ptr == last;
}

ParseResult incomplete() { return {ParseStatus::kIncomplete, 0, {}}; }

ParseResult failure(std::string message) {
  return {ParseStatus::kError, 0, std::move(message)};
}

// RESP arrays: *<count>\r\n followed by <count> bulk strings.
ParseResult parse_array(std::string_view input, Command& out) {
  const std::size_t header_end = input.find(kCrlf);
  if (header_end == std::string_view::npos) {
    // No terminator yet. Bound the wait so a client cannot pin memory by
    // dribbling digits forever.
    if (input.size() > 32) return failure("Protocol error: invalid multibulk length");
    return incomplete();
  }

  std::int64_t count = 0;
  if (!parse_int(input.substr(1, header_end - 1), count)) {
    return failure("Protocol error: invalid multibulk length");
  }
  if (count < 0) {
    // A null array. Nothing to execute; retire the bytes and move on.
    out.kind = CommandKind::kEmpty;
    out.args.clear();
    return {ParseStatus::kOk, header_end + kCrlf.size(), {}};
  }
  if (static_cast<std::size_t>(count) > kMaxArgs) {
    return failure("Protocol error: invalid multibulk length");
  }

  std::size_t pos = header_end + kCrlf.size();
  out.args.clear();
  out.args.reserve(static_cast<std::size_t>(count));

  for (std::int64_t i = 0; i < count; ++i) {
    if (pos >= input.size()) return incomplete();
    if (input[pos] != '$') return failure("Protocol error: expected '$'");

    const std::size_t length_end = input.find(kCrlf, pos);
    if (length_end == std::string_view::npos) {
      if (input.size() - pos > 32) return failure("Protocol error: invalid bulk length");
      return incomplete();
    }

    std::int64_t length = 0;
    if (!parse_int(input.substr(pos + 1, length_end - pos - 1), length)) {
      return failure("Protocol error: invalid bulk length");
    }
    if (length < 0 || static_cast<std::size_t>(length) > kMaxBulkLength) {
      return failure("Protocol error: invalid bulk length");
    }

    const std::size_t body = length_end + kCrlf.size();
    const std::size_t need = body + static_cast<std::size_t>(length) + kCrlf.size();
    // The whole point of an incremental parser: the declared length is known
    // but the bytes have not arrived. Report kIncomplete and consume nothing,
    // so the caller can call again unchanged after the next read().
    if (input.size() < need) return incomplete();

    // Zero-copy. The argument is a view into the caller's buffer; no allocation
    // and no memcpy happens for the payload, however large it is.
    out.args.emplace_back(input.substr(body, static_cast<std::size_t>(length)));
    pos = need;
  }

  if (out.args.empty()) {
    out.kind = CommandKind::kEmpty;
    return {ParseStatus::kOk, pos, {}};
  }

  out.name = out.args.front();
  out.kind = classify(out.name);
  return {ParseStatus::kOk, pos, {}};
}

// Inline commands: a whitespace-separated line. Kept so that `nc` and telnet
// remain usable against a running server.
ParseResult parse_inline(std::string_view input, Command& out) {
  const std::size_t newline = input.find('\n');
  if (newline == std::string_view::npos) {
    if (input.size() > kMaxInlineLength) return failure("Protocol error: too big inline request");
    return incomplete();
  }

  std::string_view line = input.substr(0, newline);
  if (!line.empty() && line.back() == '\r') line.remove_suffix(1);

  out.args.clear();
  std::size_t i = 0;
  while (i < line.size()) {
    while (i < line.size() && std::isspace(static_cast<unsigned char>(line[i]))) ++i;
    if (i >= line.size()) break;
    const std::size_t start = i;
    while (i < line.size() && !std::isspace(static_cast<unsigned char>(line[i]))) ++i;
    if (out.args.size() >= kMaxArgs) return failure("Protocol error: too many arguments");
    out.args.emplace_back(line.substr(start, i - start));
  }

  const std::size_t consumed = newline + 1;
  if (out.args.empty()) {
    out.kind = CommandKind::kEmpty;
    return {ParseStatus::kOk, consumed, {}};
  }

  out.name = out.args.front();
  out.kind = classify(out.name);
  return {ParseStatus::kOk, consumed, {}};
}

}  // namespace

CommandKind classify(std::string_view name) noexcept {
  if (iequals(name, "PING")) return CommandKind::kPing;
  if (iequals(name, "SET")) return CommandKind::kSet;
  if (iequals(name, "GET")) return CommandKind::kGet;
  if (iequals(name, "DEL")) return CommandKind::kDel;
  if (iequals(name, "EXPIRE")) return CommandKind::kExpire;
  if (iequals(name, "COMMAND")) return CommandKind::kCommand;
  return CommandKind::kUnknown;
}

ParseResult parse_command(std::string_view input, Command& out) {
  if (input.empty()) return incomplete();
  // The first byte decides the encoding. '*' can only begin a RESP array;
  // anything else is treated as an inline line.
  return input.front() == '*' ? parse_array(input, out) : parse_inline(input, out);
}

void reply_simple(std::string& out, std::string_view status) {
  out += '+';
  out += status;
  out += kCrlf;
}

void reply_error(std::string& out, std::string_view message) {
  out += '-';
  out += message;
  out += kCrlf;
}

void reply_bulk(std::string& out, std::string_view value) {
  out += '$';
  out += std::to_string(value.size());
  out += kCrlf;
  out += value;
  out += kCrlf;
}

void reply_nil(std::string& out) { out += "$-1\r\n"; }

void reply_integer(std::string& out, std::int64_t value) {
  out += ':';
  out += std::to_string(value);
  out += kCrlf;
}

void reply_empty_array(std::string& out) { out += "*0\r\n"; }

}  // namespace kvstore
