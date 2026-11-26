// Database: the one place where the WAL and the keyspace meet.
//
// Every mutation follows the same two steps, in this order:
//   1. append the record to the WAL (which, under --fsync always, also fsyncs);
//   2. apply it to Storage.
// If step 1 throws, step 2 never runs and the client gets an error: memory
// never holds a state the log cannot reproduce.
//
// Mutations that change nothing (DEL of a missing key, EXPIRE on a missing
// key) are not logged.

#ifndef KVSTORE_DATABASE_HPP
#define KVSTORE_DATABASE_HPP

#include <cstdint>
#include <string>
#include <string_view>

#include "kvstore/protocol.hpp"
#include "kvstore/storage.hpp"
#include "kvstore/wal.hpp"

namespace kvstore {

class Database {
 public:
  // `wal` may be null for a purely in-memory store. Not owned.
  explicit Database(Wal* wal) noexcept : wal_(wal) {}

  // Replays the WAL into the (empty) keyspace and drops keys whose deadline
  // passed while the process was down. Throws WalCorruption on mid-file damage.
  ReplayStats recover(std::int64_t now);

  void set(std::string_view key, std::string_view value, std::int64_t expire_at_ms);
  bool del(std::string_view key, std::int64_t now);
  // A deadline at or before `now` deletes the key (logged as DEL), as in Redis.
  bool expire_at(std::string_view key, std::int64_t expire_at_ms, std::int64_t now);
  // Rewrites the WAL to one SET per live key. Returns the new log size.
  std::uint64_t compact(std::int64_t now);

  Storage& storage() noexcept { return storage_; }
  Wal* wal() noexcept { return wal_; }

 private:
  Wal* wal_;
  Storage storage_;
};

// Runs one parsed command and appends exactly one RESP reply to `out`.
// Recoverable WAL errors (e.g. ENOSPC) become -ERR replies; WalFatal
// propagates and must stop the server.
void execute_command(const Command& command, Database& db, std::string& out, std::int64_t now);

}  // namespace kvstore

#endif  // KVSTORE_DATABASE_HPP
