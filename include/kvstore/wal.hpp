// Write-ahead log: an append-only, CRC-framed record of every mutation.
//
// On-disk frame (all integers little-endian, written byte by byte):
//
//   offset  size  field
//   ------  ----  -----------------------------------------------------------
//        0     4  crc32      CRC-32 (IEEE) of bytes [4, 21 + key_len + val_len)
//        4     1  op         1 = SET, 2 = DEL, 3 = EXPIRE
//        5     4  key_len
//        9     4  val_len    0 for DEL and EXPIRE
//       13     8  expire_at  absolute Unix time in ms; 0 = no expiry
//       21     .  key bytes
//        .     .  value bytes
//
// Expiry is an *absolute* wall-clock deadline. A relative TTL replayed three
// days after a crash would resurrect a key that should be long dead.
//
// Recovery rules (Wal::replay):
//   * A bad frame (short, CRC mismatch, impossible lengths/opcode) with no
//     valid frame anywhere after it is a torn tail: the process died mid-
//     append. It was never acknowledged, so it is ftruncate()d away and the
//     truncation is fsynced.
//   * A bad frame *followed by a valid frame* cannot be a torn append: the
//     bytes after it were written later. That is mid-file corruption, and
//     replay throws WalCorruption without touching the file. Silently cutting
//     the log there would destroy acknowledged writes.
//
// Durability policies:
//   kNo     write(2) only. Survives a process crash (the page cache outlives
//           the process), not a power cut.
//   kGroup  write(2) per record; one fsync covers a batch. The *server* holds
//           replies until the covering fsync completes (see server.cpp), so an
//           acknowledged write is exactly as durable as under kAlways.
//   kAlways write(2) + fsync per record, inside append().
//
// "fsync" on macOS means fcntl(F_FULLFSYNC): plain fsync() there only pushes
// data to the drive's volatile cache. See durable_sync() in wal.cpp.

#ifndef KVSTORE_WAL_HPP
#define KVSTORE_WAL_HPP

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

namespace kvstore {

enum class WalOp : std::uint8_t {
  kSet = 1,
  kDel = 2,
  kExpire = 3,
};

enum class FsyncPolicy { kNo, kGroup, kAlways };

inline constexpr std::size_t kWalHeaderSize = 21;
// Any key or value longer than this in a frame header is treated as garbage.
// Matches the protocol's bulk-string ceiling: nothing larger can be written.
inline constexpr std::uint32_t kWalMaxField = 64u * 1024u * 1024u;

// A decoded record. Views point into replay's read buffer and are valid only
// for the duration of the callback.
struct WalRecordView {
  WalOp op{};
  std::string_view key;
  std::string_view value;
  std::int64_t expire_at_ms = 0;
};

struct ReplayStats {
  std::size_t records = 0;
  std::uint64_t valid_bytes = 0;
  std::uint64_t truncated_bytes = 0;  // torn tail removed, 0 if the log was clean
};

// Thrown by replay() on corruption that is not a torn tail. The log is left
// byte-for-byte untouched so an operator can inspect it.
class WalCorruption : public std::runtime_error {
 public:
  WalCorruption(const std::string& what, std::uint64_t offset)
      : std::runtime_error(what), offset_(offset) {}
  std::uint64_t offset() const noexcept { return offset_; }

 private:
  std::uint64_t offset_;
};

// Thrown when the log is in an unknown state and continuing could acknowledge
// a write that is not durable: a failed fsync (the kernel may already have
// dropped the dirty pages, so a retry can falsely succeed), or a failed
// rollback/reopen. Callers must stop serving; the process exits.
class WalFatal : public std::runtime_error {
 public:
  using std::runtime_error::runtime_error;
};

// Appends one encoded frame to `out`. Exposed for tests and compaction.
void encode_record(std::string& out, WalOp op, std::string_view key, std::string_view value,
                   std::int64_t expire_at_ms);

std::uint32_t crc32(const char* data, std::size_t length) noexcept;

class Wal {
 public:
  using Clock = std::chrono::steady_clock;

  Wal(std::string path, FsyncPolicy policy);
  ~Wal();

  Wal(const Wal&) = delete;
  Wal& operator=(const Wal&) = delete;

  // Replays every intact frame through `on_record`, in write order, then
  // truncates a torn tail. Throws WalCorruption on mid-file damage. Must be
  // called before open(). Removes a stale `<path>.tmp` left by a crash during
  // compaction.
  ReplayStats replay(const std::function<void(const WalRecordView&)>& on_record);

  // Opens for appending (creating the file and fsyncing its directory if it
  // did not exist).
  void open();
  void close() noexcept;

  // Writes one frame. Under kAlways also fsyncs before returning. On a failed
  // write the file is truncated back to its previous length so a partial frame
  // can never be followed by later frames (which replay would rightly call
  // mid-file corruption); then std::system_error is thrown and the caller must
  // not apply the mutation. Returns the record's sequence number (1-based).
  std::uint64_t append(WalOp op, std::string_view key, std::string_view value,
                       std::int64_t expire_at_ms);

  // fsyncs everything appended so far. A failed fsync is unrecoverable (the
  // kernel may have already dropped the dirty pages), so it throws and the
  // server exits rather than retrying.
  void sync();

  // Group-commit bookkeeping (meaningful under kGroup).
  std::uint64_t appended_seq() const noexcept { return appended_seq_; }
  std::uint64_t synced_seq() const noexcept { return synced_seq_; }
  std::uint64_t unsynced() const noexcept { return appended_seq_ - synced_seq_; }
  Clock::time_point oldest_unsynced_at() const noexcept { return oldest_unsynced_at_; }

  // Atomically replaces the log with the frames that `produce` emits:
  // write <path>.tmp, fsync it, rename over <path>, fsync the directory, reopen.
  // A crash at any instant leaves either the old log or the new one, complete.
  // Everything appended before the call is considered synced afterwards.
  using Emit = std::function<void(WalOp, std::string_view, std::string_view, std::int64_t)>;
  void rewrite(const std::function<void(const Emit&)>& produce);

  FsyncPolicy policy() const noexcept { return policy_; }
  const std::string& path() const noexcept { return path_; }
  std::uint64_t size_bytes() const noexcept { return file_size_; }
  std::uint64_t sync_count() const noexcept { return sync_count_; }

 private:
  std::string path_;
  FsyncPolicy policy_;
  int fd_ = -1;
  std::uint64_t file_size_ = 0;

  std::uint64_t appended_seq_ = 0;
  std::uint64_t synced_seq_ = 0;
  Clock::time_point oldest_unsynced_at_{};
  std::uint64_t sync_count_ = 0;

  std::string scratch_;  // reused frame buffer: no allocation in steady state
};

}  // namespace kvstore

#endif  // KVSTORE_WAL_HPP
