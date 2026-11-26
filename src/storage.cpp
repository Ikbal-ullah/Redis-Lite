#include "kvstore/storage.hpp"

namespace kvstore {

void Storage::erase_it(Map::iterator it) {
  // Index first: its key view points into the node about to be freed.
  if (it->second.expire_at_ms != 0) {
    deadlines_.erase({it->second.expire_at_ms, std::string_view(it->first)});
  }
  map_.erase(it);
}

void Storage::reindex(Map::iterator it, std::int64_t new_expire_at_ms) {
  Entry& entry = it->second;
  if (entry.expire_at_ms == new_expire_at_ms) return;
  const std::string_view key(it->first);
  if (entry.expire_at_ms != 0) deadlines_.erase({entry.expire_at_ms, key});
  if (new_expire_at_ms != 0) deadlines_.insert({new_expire_at_ms, key});
  entry.expire_at_ms = new_expire_at_ms;
}

const std::string* Storage::get(std::string_view key, std::int64_t now) {
  auto it = map_.find(key);
  if (it == map_.end()) return nullptr;
  if (it->second.expire_at_ms != 0 && it->second.expire_at_ms <= now) {
    erase_it(it);  // lazy expiry
    return nullptr;
  }
  return &it->second.value;
}

void Storage::set(std::string_view key, std::string_view value, std::int64_t expire_at_ms) {
  auto it = map_.find(key);
  if (it == map_.end()) {
    it = map_.emplace(std::string(key), Entry{}).first;
  }
  it->second.value.assign(value);
  reindex(it, expire_at_ms);
}

bool Storage::erase(std::string_view key) {
  auto it = map_.find(key);
  if (it == map_.end()) return false;
  erase_it(it);
  return true;
}

bool Storage::set_expire(std::string_view key, std::int64_t expire_at_ms) {
  auto it = map_.find(key);
  if (it == map_.end()) return false;
  reindex(it, expire_at_ms);
  return true;
}

std::int64_t Storage::ttl_ms(std::string_view key, std::int64_t now) {
  if (get(key, now) == nullptr) return -2;
  const std::int64_t deadline = map_.find(key)->second.expire_at_ms;
  return deadline == 0 ? -1 : deadline - now;
}

std::size_t Storage::active_expire(std::int64_t now, std::size_t budget) {
  std::size_t evicted = 0;
  while (evicted < budget && !deadlines_.empty() && deadlines_.begin()->first <= now) {
    const std::string_view key = deadlines_.begin()->second;
    erase_it(map_.find(key));
    ++evicted;
  }
  return evicted;
}

void Storage::for_each_live(
    std::int64_t now,
    const std::function<void(std::string_view, std::string_view, std::int64_t)>& visit) const {
  for (const auto& [key, entry] : map_) {
    if (entry.expire_at_ms != 0 && entry.expire_at_ms <= now) continue;
    visit(key, entry.value, entry.expire_at_ms);
  }
}

}  // namespace kvstore
