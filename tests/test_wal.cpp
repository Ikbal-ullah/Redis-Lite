// WAL frame format, recovery (torn tail vs. mid-file corruption), compaction.

#include <sys/stat.h>

#include <filesystem>
#include <fstream>
#include <iterator>
#include <string>
#include <vector>

#include "harness.hpp"
#include "kvstore/database.hpp"
#include "kvstore/wal.hpp"

using namespace kvstore;
using testing::TempPath;

namespace {

std::string slurp(const std::string& path) {
  std::ifstream in(path, std::ios::binary);
  return {std::istreambuf_iterator<char>(in), {}};
}

void spit(const std::string& path, const std::string& data) {
  std::ofstream(path, std::ios::binary | std::ios::trunc) << data;
}

std::vector<std::string> replay_keys(const std::string& path, ReplayStats* stats = nullptr) {
  Wal wal(path, FsyncPolicy::kNo);
  std::vector<std::string> keys;
  const auto s = wal.replay([&](const WalRecordView& r) { keys.emplace_back(r.key); });
  if (stats) *stats = s;
  return keys;
}

// Writes n SET records k0..k{n-1} and returns the byte offset where each starts.
std::vector<std::size_t> write_records(const std::string& path, int n) {
  Wal wal(path, FsyncPolicy::kNo);
  wal.open();
  std::vector<std::size_t> starts;
  for (int i = 0; i < n; ++i) {
    starts.push_back(wal.size_bytes());
    wal.append(WalOp::kSet, testing::cat("k", i), testing::cat("value-", i), 0);
  }
  return starts;
}

}  // namespace

TEST(frame_layout_is_exactly_as_specified) {
  std::string frame;
  encode_record(frame, WalOp::kSet, "ab", "xyz", 0x0102030405060708LL);
  CHECK_EQ(frame.size(), kWalHeaderSize + 5);
  CHECK_EQ(static_cast<int>(frame[4]), 1);                 // op
  CHECK_EQ(static_cast<int>(frame[5]), 2);                 // key_len LE
  CHECK_EQ(static_cast<int>(frame[9]), 3);                 // val_len LE
  CHECK_EQ(static_cast<int>(frame[13]), 0x08);             // expire_at LE, low byte first
  CHECK_EQ(static_cast<int>(frame[20]), 0x01);
  CHECK_EQ(frame.substr(21), std::string("abxyz"));
  const auto crc = static_cast<std::uint32_t>(static_cast<unsigned char>(frame[0])) |
                   static_cast<std::uint32_t>(static_cast<unsigned char>(frame[1])) << 8 |
                   static_cast<std::uint32_t>(static_cast<unsigned char>(frame[2])) << 16 |
                   static_cast<std::uint32_t>(static_cast<unsigned char>(frame[3])) << 24;
  CHECK_EQ(crc, crc32(frame.data() + 4, frame.size() - 4));
  CHECK_EQ(crc32("123456789", 9), 0xCBF43926u);  // the standard CRC-32 check value
}

TEST(log_replays_into_an_identical_keyspace) {
  TempPath path;
  {
    Wal wal(path.str(), FsyncPolicy::kAlways);
    Database db(&wal);
    db.recover(1000);
    wal.open();
    db.set("a", "1", 0);
    db.set("b", "2", 50000);
    db.set("c", "3", 0);
    db.del("c", 1000);
    db.expire_at("a", 90000, 1000);
    CHECK_EQ(wal.sync_count(), 5u);  // always: one fsync per record
  }
  Wal wal(path.str(), FsyncPolicy::kNo);
  Database db(&wal);
  const auto stats = db.recover(2000);
  CHECK_EQ(stats.records, 5u);
  CHECK_EQ(stats.truncated_bytes, 0u);
  CHECK_EQ(db.storage().size(), 2u);
  CHECK_EQ(db.storage().ttl_ms("a", 2000), 88000);
  CHECK_EQ(db.storage().ttl_ms("b", 2000), 48000);
}

TEST(keys_that_expired_during_downtime_stay_dead) {
  TempPath path;
  {
    Wal wal(path.str(), FsyncPolicy::kNo);
    Database db(&wal);
    wal.open();
    db.set("short", "v", 1500);
    db.set("long", "v", 999999);
  }
  Wal wal(path.str(), FsyncPolicy::kNo);
  Database db(&wal);
  db.recover(2000);
  CHECK(db.storage().get("short", 2000) == nullptr);
  CHECK(db.storage().get("long", 2000) != nullptr);
  CHECK_EQ(db.storage().size(), 1u);
}

TEST(torn_tail_at_every_byte_offset_is_truncated) {
  // Simulate a crash at every possible point inside the final append.
  TempPath path;
  const auto starts = write_records(path.str(), 4);
  const std::string full = slurp(path.str());
  for (std::size_t cut = starts.back() + 1; cut < full.size(); ++cut) {
    spit(path.str(), full.substr(0, cut));
    ReplayStats stats;
    const auto keys = replay_keys(path.str(), &stats);
    CHECK_EQ(keys.size(), 3u);
    CHECK_EQ(stats.truncated_bytes, cut - starts.back());
    CHECK_EQ(std::filesystem::file_size(path.str()), starts.back());  // really truncated
  }
}

TEST(garbage_tail_is_truncated_and_log_stays_appendable) {
  TempPath path;
  write_records(path.str(), 3);
  const auto clean_size = std::filesystem::file_size(path.str());
  std::string garbage;
  for (int i = 0; i < 300; ++i) garbage += static_cast<char>((i * 131 + 7) & 0xFF);
  spit(path.str(), slurp(path.str()) + garbage);

  ReplayStats stats;
  CHECK_EQ(replay_keys(path.str(), &stats).size(), 3u);
  CHECK_EQ(stats.truncated_bytes, garbage.size());
  CHECK_EQ(std::filesystem::file_size(path.str()), clean_size);
  {
    Wal wal(path.str(), FsyncPolicy::kNo);
    wal.open();
    wal.append(WalOp::kSet, "after", "x", 0);
  }
  CHECK_EQ(replay_keys(path.str()).size(), 4u);
}

TEST(zero_filled_tail_is_treated_as_torn) {
  // Some filesystems extend the size before the data lands: a tail of zeros.
  TempPath path;
  write_records(path.str(), 2);
  spit(path.str(), slurp(path.str()) + std::string(4096, '\0'));
  ReplayStats stats;
  CHECK_EQ(replay_keys(path.str(), &stats).size(), 2u);
  CHECK_EQ(stats.truncated_bytes, 4096u);
}

TEST(mid_file_corruption_fails_hard_and_leaves_the_file_alone) {
  // Audit A1: the baseline silently truncated every record after a bit flip.
  TempPath path;
  const auto starts = write_records(path.str(), 10);
  std::string data = slurp(path.str());
  data[starts[3] + kWalHeaderSize] ^= 0x01;  // flip one bit in record 3's key
  spit(path.str(), data);

  bool thrown = false;
  try {
    replay_keys(path.str());
  } catch (const WalCorruption& e) {
    thrown = true;
    CHECK_EQ(e.offset(), starts[3]);
  }
  CHECK(thrown);
  CHECK_EQ(slurp(path.str()), data);  // byte-for-byte untouched
}

TEST(corrupted_length_field_mid_file_fails_hard) {
  // A flipped length can make a record look like it runs past EOF -- which
  // looks like a torn tail. The forward scan for a later valid frame catches it.
  TempPath path;
  const auto starts = write_records(path.str(), 10);
  std::string data = slurp(path.str());
  data[starts[2] + 8] = static_cast<char>(0x7F);  // key_len high byte
  spit(path.str(), data);
  CHECK_THROWS(replay_keys(path.str()), WalCorruption);
}

TEST(compaction_is_atomic_and_preserves_state) {
  TempPath path;
  {
    Wal wal(path.str(), FsyncPolicy::kAlways);
    Database db(&wal);
    db.recover(1000);
    wal.open();
    for (int i = 0; i < 1000; ++i) db.set("hot", std::to_string(i), 0);
    db.set("ttl", "v", 500000);
    db.set("gone", "v", 0);
    db.del("gone", 1000);
    const auto before = wal.size_bytes();
    const auto after = db.compact(1000);
    CHECK(after < before / 100);
    CHECK(!std::filesystem::exists(path.str() + ".tmp"));
    CHECK_EQ(wal.unsynced(), 0u);
    db.set("post", "compact", 0);  // appends go to the new file, not the unlinked one
  }
  Wal wal(path.str(), FsyncPolicy::kNo);
  Database db(&wal);
  const auto stats = db.recover(1000);
  CHECK_EQ(stats.records, 3u);  // hot, ttl, post
  CHECK_EQ(*db.storage().get("hot", 1000), std::string("999"));
  CHECK_EQ(db.storage().ttl_ms("ttl", 1000), 499000);
  CHECK(db.storage().get("gone", 1000) == nullptr);
  CHECK(db.storage().get("post", 1000) != nullptr);
}

TEST(stale_compaction_temp_file_is_ignored_and_removed) {
  TempPath path;
  write_records(path.str(), 2);
  spit(path.str() + ".tmp", "half-written garbage from a crash mid-compaction");
  CHECK_EQ(replay_keys(path.str()).size(), 2u);
  CHECK(!std::filesystem::exists(path.str() + ".tmp"));
}

TEST(absent_log_is_an_empty_log) {
  TempPath path;
  ReplayStats stats;
  CHECK(replay_keys(path.str(), &stats).empty());
  CHECK_EQ(stats.records, 0u);
}

TEST(group_policy_defers_fsync_to_the_caller) {
  TempPath path;
  Wal wal(path.str(), FsyncPolicy::kGroup);
  wal.open();
  for (int i = 0; i < 10; ++i) wal.append(WalOp::kSet, "k", "v", 0);
  CHECK_EQ(wal.unsynced(), 10u);
  CHECK_EQ(wal.sync_count(), 0u);
  wal.sync();
  CHECK_EQ(wal.unsynced(), 0u);
  CHECK_EQ(wal.sync_count(), 1u);
}
