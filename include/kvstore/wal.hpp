// Write-Ahead Log: an append-only, crash-recoverable record of every mutation.
//
// The contract is simple and absolute: a mutation is "committed" the instant
// append() returns, because append() does not return until the bytes are on
// stable storage. Memory is updated *after* that point, never before. If the
// process dies at any instant, the log is either missing the record (the
// mutation never happened) or contains it (the mutation is replayed at
// startup). There is no third state where the log and memory disagree
// permanently.
//
// On-disk format. Every record is:
//
//     +----------------+----------------+---------------------------+
//     | payload_len u32| crc32 u32      | payload (payload_len bytes)|
//     +----------------+----------------+---------------------------+
//
// Both header fields are little-endian, written byte-by-byte so the file is
// portable across architectures. The CRC covers the payload only; a torn or
// bit-rotted record fails the check and everything from that point on is
// discarded (see replay()).
//
// Payload layout, by opcode:
//
//     Set:    u8 op | u32 key_len | key | u32 val_len | val | i64 expire_at_ms
//     Del:    u8 op | u32 key_len | key
//     Expire: u8 op | u32 key_len | key | i64 expire_at_ms
//
// Note that expiry is stored as an *absolute* deadline in milliseconds since
// the Unix epoch, never as a relative TTL. Logging "expire in 60 seconds" would
// be a correctness bug: replaying that record three days after a crash would
// resurrect a key that should have died. See EXPLAINER.md.

#ifndef KVSTORE_WAL_HPP
#define KVSTORE_WAL_HPP

#include <cstdint>
#include <functional>
#include <mutex>
#include <string>
#include <string_view>
#include <vector>

namespace kvstore {

enum class WalOp : std::uint8_t {
  kSet = 1,
  kDel = 2,
  kExpire = 3,
};

// One decoded log record. `value` is meaningful only for kSet; `expire_at_ms`
// only for kSet and kExpire, where 0 means "no expiry".
struct WalRecord {
  WalOp op{};
  std::string key;
  std::string value;
  std::int64_t expire_at_ms = 0;
};

// How aggressively to push bytes to the physical device.
//
//   kAlways - flush on every append. Slow (one device round-trip per write),
//             and the only setting that survives a power cut with zero loss.
//   kNever  - hand the bytes to the kernel and return. Survives a process
//             crash (the page cache outlives the process) but not a machine
//             crash. Useful for tests and bulk loads.
enum class SyncPolicy {
  kAlways,
  kNever,
};

class Wal {
 public:
  explicit Wal(std::string path, SyncPolicy policy = SyncPolicy::kAlways);
  ~Wal();

  // A Wal owns a file descriptor, which is a unique resource. Copying it would
  // mean two objects racing to close() the same integer.
  Wal(const Wal&) = delete;
  Wal& operator=(const Wal&) = delete;
  Wal(Wal&&) = delete;
  Wal& operator=(Wal&&) = delete;

  // Reads the log from offset 0 and invokes `on_record` for each intact
  // record, in the order they were written. Stops at the first record that is
  // truncated or fails its CRC, then ftruncate()s the file back to the end of
  // the last good record so subsequent appends stay aligned.
  //
  // Returns the number of records applied. Must be called before open().
  std::size_t replay(const std::function<void(const WalRecord&)>& on_record);

  // Opens the log for appending, creating it if absent. Throws std::system_error
  // on failure.
  void open();

  // Encodes, writes, and (per policy) flushes one record. Blocks until the
  // data is durable. Thread-safe.
  void append(const WalRecord& record);

  // Forces any buffered bytes to the physical device.
  void sync();

  void close();

  const std::string& path() const noexcept { return path_; }

  // Bytes appended by this process since open(). Diagnostics only.
  std::uint64_t bytes_written() const noexcept { return bytes_written_; }

 private:
  void encode(const WalRecord& record, std::vector<char>& out) const;

  std::string path_;
  SyncPolicy policy_;
  int fd_ = -1;

  // Guards fd_ and scratch_. This protects the Wal's own internals; it says
  // nothing about the order in which callers' mutations become visible in
  // memory. That ordering is the caller's job (see Storage::wal_mutex_).
  mutable std::mutex mutex_;

  // Reused across appends so the steady-state write path does not allocate.
  std::vector<char> scratch_;

  std::uint64_t bytes_written_ = 0;
};

}  // namespace kvstore

#endif  // KVSTORE_WAL_HPP
