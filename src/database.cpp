#include "kvstore/database.hpp"

#include <charconv>
#include <cstddef>
#include <limits>
#include <system_error>

namespace kvstore {

ReplayStats Database::recover(std::int64_t now) {
  if (wal_ == nullptr) return {};
  const ReplayStats stats = wal_->replay([this](const WalRecordView& r) {
    switch (r.op) {
      case WalOp::kSet: storage_.set(r.key, r.value, r.expire_at_ms); break;
      case WalOp::kDel: storage_.erase(r.key); break;
      case WalOp::kExpire: storage_.set_expire(r.key, r.expire_at_ms); break;
    }
  });
  storage_.active_expire(now, std::numeric_limits<std::size_t>::max());
  return stats;
}

void Database::set(std::string_view key, std::string_view value, std::int64_t expire_at_ms) {
  if (wal_ != nullptr) wal_->append(WalOp::kSet, key, value, expire_at_ms);
  storage_.set(key, value, expire_at_ms);
}

bool Database::del(std::string_view key, std::int64_t now) {
  if (!storage_.contains(key, now)) return false;
  if (wal_ != nullptr) wal_->append(WalOp::kDel, key, {}, 0);
  storage_.erase(key);
  return true;
}

bool Database::expire_at(std::string_view key, std::int64_t expire_at_ms, std::int64_t now) {
  if (!storage_.contains(key, now)) return false;
  if (expire_at_ms <= now) return del(key, now);
  if (wal_ != nullptr) wal_->append(WalOp::kExpire, key, {}, expire_at_ms);
  storage_.set_expire(key, expire_at_ms);
  return true;
}

std::uint64_t Database::compact(std::int64_t now) {
  if (wal_ == nullptr) return 0;
  wal_->rewrite([&](const Wal::Emit& emit) {
    storage_.for_each_live(now, [&](std::string_view key, std::string_view value,
                                    std::int64_t expire_at_ms) {
      emit(WalOp::kSet, key, value, expire_at_ms);
    });
  });
  return wal_->size_bytes();
}

// ---------------------------------------------------------------------------
// Command dispatch
// ---------------------------------------------------------------------------

namespace {

bool parse_i64(std::string_view text, std::int64_t& out) noexcept {
  if (text.empty()) return false;
  const auto r = std::from_chars(text.data(), text.data() + text.size(), out);
  return r.ec == std::errc{} && r.ptr == text.data() + text.size();
}

// now + amount * unit_ms without signed overflow (the baseline had UB here).
bool deadline_from(std::int64_t now, std::int64_t amount, std::int64_t unit_ms,
                   std::int64_t& out) noexcept {
  constexpr std::int64_t kMax = std::numeric_limits<std::int64_t>::max();
  constexpr std::int64_t kMin = std::numeric_limits<std::int64_t>::min();
  if (amount > 0 && amount > (kMax - now) / unit_ms) return false;
  if (amount < 0 && amount < (kMin + now) / unit_ms) return false;
  out = now + amount * unit_ms;
  return true;
}

void wrong_arity(std::string& out, std::string_view name) {
  std::string message = "ERR wrong number of arguments for '";
  for (char c : name) message += (c >= 'A' && c <= 'Z') ? static_cast<char>(c - 'A' + 'a') : c;
  message += "' command";
  reply_error(out, message);
}

void cmd_set(const Command& c, Database& db, std::string& out, std::int64_t now) {
  if (c.arity() != 3 && c.arity() != 5) {
    if (c.arity() < 3) return wrong_arity(out, c.name);
    return reply_error(out, "ERR syntax error");
  }
  std::int64_t expire_at = 0;
  if (c.arity() == 5) {
    std::int64_t unit = 0;
    if (iequals(c.args[3], "EX")) unit = 1000;
    else if (iequals(c.args[3], "PX")) unit = 1;
    else return reply_error(out, "ERR syntax error");

    std::int64_t amount = 0;
    if (!parse_i64(c.args[4], amount)) {
      return reply_error(out, "ERR value is not an integer or out of range");
    }
    if (amount <= 0 || !deadline_from(now, amount, unit, expire_at)) {
      return reply_error(out, "ERR invalid expire time in 'set' command");
    }
  }
  db.set(c.args[1], c.args[2], expire_at);
  reply_simple(out, "OK");
}

void dispatch(const Command& c, Database& db, std::string& out, std::int64_t now) {
  switch (c.kind) {
    case CommandKind::kPing:
      if (c.arity() == 1) return reply_simple(out, "PONG");
      if (c.arity() == 2) return reply_bulk(out, c.args[1]);
      return wrong_arity(out, c.name);

    case CommandKind::kEcho:
      if (c.arity() != 2) return wrong_arity(out, c.name);
      return reply_bulk(out, c.args[1]);

    case CommandKind::kSet:
      return cmd_set(c, db, out, now);

    case CommandKind::kGet: {
      if (c.arity() != 2) return wrong_arity(out, c.name);
      const std::string* value = db.storage().get(c.args[1], now);
      return value ? reply_bulk(out, *value) : reply_nil(out);
    }

    case CommandKind::kDel: {
      if (c.arity() < 2) return wrong_arity(out, c.name);
      std::int64_t removed = 0;
      for (std::size_t i = 1; i < c.arity(); ++i) removed += db.del(c.args[i], now) ? 1 : 0;
      return reply_integer(out, removed);
    }

    case CommandKind::kExpire: {
      if (c.arity() != 3) return wrong_arity(out, c.name);
      std::int64_t seconds = 0;
      if (!parse_i64(c.args[2], seconds)) {
        return reply_error(out, "ERR value is not an integer or out of range");
      }
      std::int64_t deadline = 0;
      if (!deadline_from(now, seconds, 1000, deadline)) {
        return reply_error(out, "ERR invalid expire time in 'expire' command");
      }
      return reply_integer(out, db.expire_at(c.args[1], deadline, now) ? 1 : 0);
    }

    case CommandKind::kTtl: {
      if (c.arity() != 2) return wrong_arity(out, c.name);
      const std::int64_t ms = db.storage().ttl_ms(c.args[1], now);
      // Redis rounds the remaining time to the nearest second.
      return reply_integer(out, ms < 0 ? ms : (ms + 500) / 1000);
    }

    case CommandKind::kCompact:
      if (c.arity() != 1) return wrong_arity(out, c.name);
      db.compact(now);
      return reply_simple(out, "OK");

    case CommandKind::kCommand:
      // redis-cli sends COMMAND DOCS on connect; an empty array satisfies it.
      return reply_empty_array(out);

    case CommandKind::kEmpty:
      return;

    case CommandKind::kUnknown:
      break;
  }
  std::string message = "ERR unknown command '";
  message.append(c.name);
  message += '\'';
  reply_error(out, message);
}

}  // namespace

void execute_command(const Command& command, Database& db, std::string& out, std::int64_t now) {
  try {
    dispatch(command, db, out, now);
  } catch (const std::system_error& e) {
    // A failed write was rolled back by the WAL and the mutation was not
    // applied: safe to report and keep serving. WalFatal is not a
    // system_error and deliberately escapes.
    std::string message = "ERR persistence failure: ";
    message += e.what();
    reply_error(out, message);
  }
}

}  // namespace kvstore
