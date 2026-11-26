// In-memory keyspace: a hash map plus an ordered index of expiry deadlines.
//
// Single-threaded by design. The whole server runs on one event-loop thread,
// so there are no locks here at all; the class is a plain data structure that
// knows nothing about the WAL or the network. Time is always passed in, which
// makes every expiry rule unit-testable without sleeping.
//
// Expiry has two halves, as in Redis:
//   lazy    every read checks the deadline and erases a dead key on the spot;
//   active  active_expire() pops at most `budget` dead keys from the front of
//           the deadline index. The cost is bounded by the budget and by the
//           number of keys that are actually dead -- never O(keyspace).
//
// Neither kind of eviction is written to the WAL: replay re-checks every
// absolute deadline against the clock, so a dead key is dropped there too.

#ifndef KVSTORE_STORAGE_HPP
#define KVSTORE_STORAGE_HPP

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <set>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>

namespace kvstore {

// Milliseconds since the Unix epoch. Wall clock on purpose: deadlines are
// persisted, and a steady_clock origin means nothing after a reboot.
inline std::int64_t now_ms() {
  using namespace std::chrono;
  return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}

// Enables heterogeneous lookup: find(std::string_view) without allocating a
// temporary std::string on every GET.
struct StringHash {
  using is_transparent = void;
  std::size_t operator()(std::string_view key) const noexcept {
    return std::hash<std::string_view>{}(key);
  }
};

class Storage {
 public:
  // Value of a live key, or nullptr. The pointer is valid until the next
  // mutation; the caller encodes the reply immediately, so GET copies nothing.
  const std::string* get(std::string_view key, std::int64_t now);

  bool contains(std::string_view key, std::int64_t now) { return get(key, now) != nullptr; }

  // Inserts or overwrites. expire_at_ms == 0 means no expiry (and clears any
  // previous one, as a plain SET does in Redis).
  void set(std::string_view key, std::string_view value, std::int64_t expire_at_ms);

  // Removes the key if present (expired or not). Returns whether it existed.
  bool erase(std::string_view key);

  // Sets a new absolute deadline on an existing key. Returns false if absent.
  bool set_expire(std::string_view key, std::int64_t expire_at_ms);

  // Redis TTL semantics, in milliseconds: -2 absent, -1 no expiry.
  std::int64_t ttl_ms(std::string_view key, std::int64_t now);

  // Evicts up to `budget` keys whose deadline is <= now. Returns how many.
  std::size_t active_expire(std::int64_t now, std::size_t budget);

  // Keys currently held, including expired ones not yet evicted.
  std::size_t size() const noexcept { return map_.size(); }
  std::size_t volatile_keys() const noexcept { return deadlines_.size(); }

  // Visits every live entry (used by WAL compaction).
  void for_each_live(std::int64_t now,
                     const std::function<void(std::string_view key, std::string_view value,
                                              std::int64_t expire_at_ms)>& visit) const;

 private:
  struct Entry {
    std::string value;
    std::int64_t expire_at_ms = 0;  // 0 == persistent
  };
  using Map = std::unordered_map<std::string, Entry, StringHash, std::equal_to<>>;

  // (deadline, key). The key is a view of the map node's own key string:
  // unordered_map is node-based, so element addresses survive rehashing and
  // the view stays valid until that node is erased. No key is stored twice.
  using DeadlineIndex = std::set<std::pair<std::int64_t, std::string_view>>;

  void erase_it(Map::iterator it);
  void reindex(Map::iterator it, std::int64_t new_expire_at_ms);

  Map map_;
  DeadlineIndex deadlines_;
};

}  // namespace kvstore

#endif  // KVSTORE_STORAGE_HPP
