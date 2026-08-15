#include "kvstore/storage.hpp"

#include <utility>

namespace kvstore {

std::size_t Storage::load_from_wal() {
  if (wal_ == nullptr) return 0;

  // Recovery is single-threaded by construction -- nothing is serving traffic
  // yet -- but the lock is taken anyway so that apply_locked's precondition is
  // satisfied on every path, and so that this stays correct if the server ever
  // grows an online-restore feature.
  std::unique_lock<std::shared_mutex> lock(mutex_);
  const std::size_t applied = wal_->replay([this](const WalRecord& record) {
    apply_locked(record);
  });

  // Deadlines were persisted as absolute timestamps, so anything that expired
  // while the process was down is simply dropped here. This is why evictions
  // never need to be logged.
  const std::int64_t now = now_ms();
  for (auto it = map_.begin(); it != map_.end();) {
    it = it->second.expired(now) ? map_.erase(it) : std::next(it);
  }
  return applied;
}

void Storage::apply_locked(const WalRecord& record) {
  switch (record.op) {
    case WalOp::kSet: {
      auto it = map_.find(record.key);
      if (it == map_.end()) {
        map_.emplace(record.key, Entry{record.value, record.expire_at_ms});
      } else {
        it->second.value = record.value;
        it->second.expire_at_ms = record.expire_at_ms;
      }
      break;
    }
    case WalOp::kDel: {
      auto it = map_.find(record.key);
      if (it != map_.end()) map_.erase(it);
      break;
    }
    case WalOp::kExpire: {
      auto it = map_.find(record.key);
      if (it != map_.end()) it->second.expire_at_ms = record.expire_at_ms;
      break;
    }
  }
}

void Storage::set(std::string_view key, std::string_view value) {
  WalRecord record;
  record.op = WalOp::kSet;
  record.key.assign(key);
  record.value.assign(value);
  record.expire_at_ms = 0;  // a plain SET clears any existing TTL

  // Step 1: serialize against other mutators. Held across both the log write
  // and the map update, so the two orders can never diverge.
  std::lock_guard<std::mutex> wal_lock(wal_mutex_);

  // Step 2: durability first. If this throws, the map is untouched and the
  // caller sees an error -- exactly the behaviour we want, since a client must
  // never be told OK for something that is not on disk.
  if (wal_ != nullptr) wal_->append(record);

  // Step 3: publish to memory. The exclusive lock is taken only now, after the
  // fsync has already completed, so concurrent readers were never blocked on
  // the disk.
  std::unique_lock<std::shared_mutex> lock(mutex_);
  apply_locked(record);
}

std::optional<std::string> Storage::get(std::string_view key) const {
  std::shared_lock<std::shared_mutex> lock(mutex_);

  // Heterogeneous lookup: `key` is probed directly, with no temporary
  // std::string and no heap allocation on the hot read path.
  auto it = map_.find(key);
  if (it == map_.end()) return std::nullopt;

  // Lazy expiry. The entry is reported as absent but not removed, because
  // removing it would require upgrading to an exclusive lock and this is a
  // const, shared-locked path. The maintenance thread reclaims the memory.
  if (it->second.expired(now_ms())) return std::nullopt;

  // Copied out under the lock. Handing back a view would be a use-after-free
  // the moment another thread overwrote or erased the entry.
  return it->second.value;
}

bool Storage::del(std::string_view key) {
  WalRecord record;
  record.op = WalOp::kDel;
  record.key.assign(key);

  std::lock_guard<std::mutex> wal_lock(wal_mutex_);

  // Check for presence before logging, so a DEL of a missing key costs nothing
  // and reports 0 to the client. The check runs under wal_mutex_, which no
  // other mutator can hold, so the answer cannot go stale before the write.
  {
    std::shared_lock<std::shared_mutex> peek(mutex_);
    auto it = map_.find(key);
    if (it == map_.end() || it->second.expired(now_ms())) return false;
  }

  if (wal_ != nullptr) wal_->append(record);

  std::unique_lock<std::shared_mutex> lock(mutex_);
  apply_locked(record);
  return true;
}

bool Storage::expire(std::string_view key, std::int64_t ttl_seconds) {
  std::lock_guard<std::mutex> wal_lock(wal_mutex_);

  {
    std::shared_lock<std::shared_mutex> peek(mutex_);
    auto it = map_.find(key);
    if (it == map_.end() || it->second.expired(now_ms())) return false;
  }

  WalRecord record;
  record.key.assign(key);
  if (ttl_seconds <= 0) {
    // Matches Redis: a TTL in the past means delete right now.
    record.op = WalOp::kDel;
  } else {
    record.op = WalOp::kExpire;
    record.expire_at_ms = now_ms() + ttl_seconds * 1000;
  }

  if (wal_ != nullptr) wal_->append(record);

  std::unique_lock<std::shared_mutex> lock(mutex_);
  apply_locked(record);
  return true;
}

std::size_t Storage::size() const {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  const std::int64_t now = now_ms();
  std::size_t live = 0;
  for (const auto& [key, entry] : map_) {
    (void)key;
    if (!entry.expired(now)) ++live;
  }
  return live;
}

std::size_t Storage::purge_expired() {
  // No wal_mutex_ here, and no log record: this changes nothing a client can
  // observe (an expired key already reads as absent) and replay re-applies the
  // same deadline test, so the log stays a correct description of the state.
  std::unique_lock<std::shared_mutex> lock(mutex_);
  const std::int64_t now = now_ms();
  std::size_t removed = 0;
  for (auto it = map_.begin(); it != map_.end();) {
    if (it->second.expired(now)) {
      it = map_.erase(it);
      ++removed;
    } else {
      ++it;
    }
  }
  return removed;
}

}  // namespace kvstore
