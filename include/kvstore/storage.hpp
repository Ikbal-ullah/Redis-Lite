// The storage engine: a hash map, a readers-writer lock, and a write-ahead log.
//
// Threading model
// ---------------
// Readers (get, size) take a shared lock: any number of them run at once.
// Mutators (set, del, expire) take an exclusive lock: one at a time, and never
// alongside a reader.
//
// Mutators additionally take `wal_mutex_` *before* the map lock, and hold it
// across both the log append and the map update. That is what makes the log
// order and the memory order identical -- without it, two threads could append
// A-then-B to the log but apply B-then-A to the map, and a replay after a crash
// would rebuild a state the server never actually served.
//
// The lock order is always wal_mutex_ -> mutex_, never the reverse, so there is
// no cycle and no deadlock. Note that the expensive part (the fsync) happens
// while holding wal_mutex_ but *not* the map lock, so readers are never blocked
// on a disk round-trip.

#ifndef KVSTORE_STORAGE_HPP
#define KVSTORE_STORAGE_HPP

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <unordered_map>

#include "kvstore/wal.hpp"

namespace kvstore {

// Milliseconds since the Unix epoch.
//
// Deliberately the *wall* clock, not steady_clock. Expiry deadlines are written
// to disk and must still mean something after a reboot; steady_clock's origin
// is unspecified and typically resets with the machine, so a persisted
// steady_clock deadline is meaningless on the next boot.
inline std::int64_t now_ms() {
  using namespace std::chrono;
  return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}

// Transparent hasher. Its `is_transparent` typedef unlocks heterogeneous
// lookup, letting find() accept a std::string_view directly instead of forcing
// every GET to construct (and heap-allocate) a temporary std::string just to
// probe the table.
struct StringHash {
  using is_transparent = void;
  std::size_t operator()(std::string_view key) const noexcept {
    return std::hash<std::string_view>{}(key);
  }
};

class Storage {
 public:
  // `wal` may be null, in which case the store is purely in-memory. The Wal
  // must outlive the Storage; ownership stays with the caller.
  explicit Storage(Wal* wal = nullptr) noexcept : wal_(wal) {}

  Storage(const Storage&) = delete;
  Storage& operator=(const Storage&) = delete;

  // Rebuilds in-memory state from the log. Applies records without re-logging
  // them, then drops anything whose deadline has already passed. Call once, on
  // an empty store, before serving traffic.
  std::size_t load_from_wal();

  // Sets key to value and clears any existing TTL (this matches Redis: a plain
  // SET makes the key persistent again).
  void set(std::string_view key, std::string_view value);

  // Returns the value, or nullopt if absent or expired.
  //
  // Returns by value, not string_view: the view would dangle the moment the
  // shared lock is released and another thread overwrote the entry. This is the
  // exact opposite of the parser's situation, where zero-copy views are safe
  // because the buffer provably outlives them.
  std::optional<std::string> get(std::string_view key) const;

  // Returns true if the key existed (and was not already expired).
  bool del(std::string_view key);

  // Sets an absolute deadline `ttl_seconds` from now. A non-positive TTL
  // deletes the key immediately. Returns false if the key does not exist.
  bool expire(std::string_view key, std::int64_t ttl_seconds);

  // Number of live (non-expired) keys.
  std::size_t size() const;

  // Evicts every entry whose deadline has passed. Called periodically by the
  // server's maintenance thread; without it, a key that is written with a TTL
  // and never read again would occupy memory forever, since the read path only
  // expires keys lazily.
  //
  // Evictions are deliberately *not* logged: replay re-checks every deadline
  // against the current clock, so an expired record is dropped there anyway.
  std::size_t purge_expired();

 private:
  struct Entry {
    std::string value;
    std::int64_t expire_at_ms = 0;  // 0 == never expires

    bool expired(std::int64_t now) const noexcept {
      return expire_at_ms != 0 && expire_at_ms <= now;
    }
  };

  using Map = std::unordered_map<std::string, Entry, StringHash, std::equal_to<>>;

  // Applies a decoded record to the map. Caller must hold mutex_ exclusively.
  void apply_locked(const WalRecord& record);

  Wal* wal_;

  // Serializes mutators against each other and pins log order to memory order.
  // Always acquired before mutex_.
  std::mutex wal_mutex_;

  // `mutable` so the const read path can lock it.
  mutable std::shared_mutex mutex_;
  Map map_;
};

}  // namespace kvstore

#endif  // KVSTORE_STORAGE_HPP
