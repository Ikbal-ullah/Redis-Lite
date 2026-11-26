#include "kvstore/wal.hpp"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <optional>
#include <system_error>

namespace kvstore {
namespace {

// Rewrite output is flushed to the temp file in chunks of this size, so
// compacting a large store never materialises the whole new log in memory.
constexpr std::size_t kRewriteChunk = 1u << 20;

[[noreturn]] void throw_errno(const std::string& what) {
  throw std::system_error(errno, std::generic_category(), what);
}

constexpr std::array<std::uint32_t, 256> make_crc_table() {
  std::array<std::uint32_t, 256> table{};
  for (std::uint32_t i = 0; i < 256; ++i) {
    std::uint32_t c = i;
    for (int bit = 0; bit < 8; ++bit) c = (c & 1u) ? (0xEDB88320u ^ (c >> 1)) : (c >> 1);
    table[i] = c;
  }
  return table;
}
constexpr auto kCrcTable = make_crc_table();

void put_u32(char* p, std::uint32_t v) noexcept {
  for (int i = 0; i < 4; ++i) p[i] = static_cast<char>((v >> (8 * i)) & 0xFFu);
}
void put_u64(char* p, std::uint64_t v) noexcept {
  for (int i = 0; i < 8; ++i) p[i] = static_cast<char>((v >> (8 * i)) & 0xFFu);
}
std::uint32_t get_u32(const char* p) noexcept {
  std::uint32_t v = 0;
  for (int i = 0; i < 4; ++i) v |= static_cast<std::uint32_t>(static_cast<unsigned char>(p[i])) << (8 * i);
  return v;
}
std::uint64_t get_u64(const char* p) noexcept {
  std::uint64_t v = 0;
  for (int i = 0; i < 8; ++i) v |= static_cast<std::uint64_t>(static_cast<unsigned char>(p[i])) << (8 * i);
  return v;
}

// write(2) may write less than asked and may be interrupted; loop until done.
void write_full(int fd, const char* data, std::size_t length) {
  while (length > 0) {
    const ssize_t n = ::write(fd, data, length);
    if (n < 0) {
      if (errno == EINTR) continue;
      throw_errno("wal write");
    }
    data += n;
    length -= static_cast<std::size_t>(n);
  }
}

// The only call that makes bytes survive a power cut.
//   Linux: fsync(2).
//   macOS: fsync(2) only hands data to the drive, whose volatile cache may
//          still lose it; F_FULLFSYNC asks the drive to flush that cache too.
//          Some filesystems (e.g. network mounts) reject it; fall back then.
void durable_sync(int fd) {
#if defined(__APPLE__)
  if (::fcntl(fd, F_FULLFSYNC) == 0) return;
#endif
  while (::fsync(fd) != 0) {
    if (errno != EINTR) throw_errno("fsync");
  }
}

std::string parent_dir(const std::string& path) {
  const auto slash = path.find_last_of('/');
  if (slash == std::string::npos) return ".";
  if (slash == 0) return "/";
  return path.substr(0, slash);
}

// A rename or a newly created file is a change to the *directory*; until the
// directory itself is fsynced, a power cut can roll the entry back.
void sync_dir(const std::string& file_path) {
  const std::string dir = parent_dir(file_path);
  const int fd = ::open(dir.c_str(), O_RDONLY | O_CLOEXEC);
  if (fd < 0) throw_errno("open dir " + dir);
  try {
    durable_sync(fd);
  } catch (...) {
    ::close(fd);
    throw;
  }
  ::close(fd);
}

// Validates the frame starting at `offset`. Returns its total length, or
// nullopt if it is short, malformed, or fails its checksum.
std::optional<std::size_t> valid_frame(std::string_view data, std::size_t offset,
                                       WalRecordView* out) noexcept {
  if (data.size() - offset < kWalHeaderSize) return std::nullopt;
  const char* p = data.data() + offset;
  const auto op = static_cast<unsigned char>(p[4]);
  const std::uint32_t key_len = get_u32(p + 5);
  const std::uint32_t val_len = get_u32(p + 9);
  if (op < 1 || op > 3) return std::nullopt;
  if (key_len > kWalMaxField || val_len > kWalMaxField) return std::nullopt;
  if (op != static_cast<unsigned char>(WalOp::kSet) && val_len != 0) return std::nullopt;

  const std::size_t total = kWalHeaderSize + key_len + val_len;
  if (data.size() - offset < total) return std::nullopt;
  if (crc32(p + 4, total - 4) != get_u32(p)) return std::nullopt;

  if (out != nullptr) {
    out->op = static_cast<WalOp>(op);
    out->key = std::string_view(p + kWalHeaderSize, key_len);
    out->value = std::string_view(p + kWalHeaderSize + key_len, val_len);
    out->expire_at_ms = static_cast<std::int64_t>(get_u64(p + 13));
  }
  return total;
}

}  // namespace

std::uint32_t crc32(const char* data, std::size_t length) noexcept {
  std::uint32_t crc = 0xFFFFFFFFu;
  for (std::size_t i = 0; i < length; ++i) {
    crc = kCrcTable[(crc ^ static_cast<unsigned char>(data[i])) & 0xFFu] ^ (crc >> 8);
  }
  return crc ^ 0xFFFFFFFFu;
}

void encode_record(std::string& out, WalOp op, std::string_view key, std::string_view value,
                   std::int64_t expire_at_ms) {
  const std::size_t start = out.size();
  out.resize(start + kWalHeaderSize);
  char* h = out.data() + start;
  h[4] = static_cast<char>(op);
  put_u32(h + 5, static_cast<std::uint32_t>(key.size()));
  put_u32(h + 9, static_cast<std::uint32_t>(value.size()));
  put_u64(h + 13, static_cast<std::uint64_t>(expire_at_ms));
  out.append(key);
  out.append(value);
  // CRC last: it covers everything after itself, including the payload.
  put_u32(out.data() + start, crc32(out.data() + start + 4, out.size() - start - 4));
}

Wal::Wal(std::string path, FsyncPolicy policy) : path_(std::move(path)), policy_(policy) {}

Wal::~Wal() { close(); }

void Wal::close() noexcept {
  if (fd_ >= 0) {
    ::close(fd_);
    fd_ = -1;
  }
}

ReplayStats Wal::replay(const std::function<void(const WalRecordView&)>& on_record) {
  if (fd_ >= 0) throw std::logic_error("wal replay after open");

  // A crash during rewrite() can leave a half-written temp file. The real log
  // was never replaced (rename is the commit point), so the temp is garbage.
  ::unlink((path_ + ".tmp").c_str());

  ReplayStats stats;
  const int fd = ::open(path_.c_str(), O_RDWR | O_CLOEXEC);
  if (fd < 0) {
    if (errno == ENOENT) return stats;  // no log yet: empty store
    throw_errno("open wal " + path_);
  }

  // Read the whole log. Simple and fast for logs that fit in memory, which is
  // a precondition of an in-memory store anyway; compaction keeps it bounded.
  std::string data;
  try {
    struct stat st {};
    if (::fstat(fd, &st) != 0) throw_errno("fstat wal");
    data.resize(static_cast<std::size_t>(st.st_size));
    std::size_t got = 0;
    while (got < data.size()) {
      const ssize_t n = ::read(fd, data.data() + got, data.size() - got);
      if (n < 0 && errno == EINTR) continue;
      if (n < 0) throw_errno("read wal");
      if (n == 0) break;
      got += static_cast<std::size_t>(n);
    }
    data.resize(got);

    std::size_t offset = 0;
    WalRecordView record;
    while (offset < data.size()) {
      const auto length = valid_frame(data, offset, &record);
      if (length) {
        on_record(record);
        ++stats.records;
        offset += *length;
        continue;
      }
      // Bad frame. If *any* valid frame starts after it, the damage is not at
      // the tail and truncating would destroy acknowledged writes.
      for (std::size_t probe = offset + 1; probe + kWalHeaderSize <= data.size(); ++probe) {
        if (valid_frame(data, probe, nullptr)) {
          throw WalCorruption("wal corruption at offset " + std::to_string(offset) +
                                  " followed by a valid record at offset " +
                                  std::to_string(probe) + "; refusing to truncate " + path_,
                              offset);
        }
      }
      break;  // torn tail
    }

    stats.valid_bytes = offset;
    stats.truncated_bytes = data.size() - offset;
    if (stats.truncated_bytes > 0) {
      if (::ftruncate(fd, static_cast<off_t>(offset)) != 0) throw_errno("truncate wal");
      durable_sync(fd);  // the truncation must itself be durable
    }
  } catch (...) {
    ::close(fd);
    throw;
  }
  ::close(fd);
  return stats;
}

void Wal::open() {
  if (fd_ >= 0) return;
  struct stat st {};
  const bool existed = ::stat(path_.c_str(), &st) == 0;
  const int fd = ::open(path_.c_str(), O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC, 0644);
  if (fd < 0) throw_errno("open wal " + path_);
  if (::fstat(fd, &st) != 0) {
    ::close(fd);
    throw_errno("fstat wal");
  }
  fd_ = fd;
  file_size_ = static_cast<std::uint64_t>(st.st_size);
  if (!existed) sync_dir(path_);  // make the new directory entry durable
}

std::uint64_t Wal::append(WalOp op, std::string_view key, std::string_view value,
                          std::int64_t expire_at_ms) {
  if (fd_ < 0) throw std::logic_error("wal append before open");

  scratch_.clear();
  encode_record(scratch_, op, key, value, expire_at_ms);
  try {
    write_full(fd_, scratch_.data(), scratch_.size());
  } catch (const std::system_error&) {
    // Roll back any partial frame (e.g. ENOSPC halfway through). If even that
    // fails the log has an unknown tail and we cannot keep accepting writes.
    if (::ftruncate(fd_, static_cast<off_t>(file_size_)) != 0) {
      throw WalFatal("wal: write failed and rollback truncate failed");
    }
    throw;
  }
  file_size_ += scratch_.size();

  if (appended_seq_ == synced_seq_) oldest_unsynced_at_ = Clock::now();
  ++appended_seq_;
  if (policy_ == FsyncPolicy::kAlways) sync();
  return appended_seq_;
}

void Wal::sync() {
  if (fd_ < 0 || synced_seq_ == appended_seq_) return;
  try {
    durable_sync(fd_);
  } catch (const std::system_error& e) {
    throw WalFatal(std::string("wal fsync failed: ") + e.what());
  }
  ++sync_count_;
  synced_seq_ = appended_seq_;
}

void Wal::rewrite(const std::function<void(const Emit&)>& produce) {
  if (fd_ < 0) throw std::logic_error("wal rewrite before open");
  const std::string tmp = path_ + ".tmp";

  const int tmp_fd = ::open(tmp.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
  if (tmp_fd < 0) throw_errno("open " + tmp);

  std::uint64_t written = 0;
  try {
    std::string chunk;
    chunk.reserve(kRewriteChunk);
    const Emit emit = [&](WalOp op, std::string_view key, std::string_view value,
                          std::int64_t expire_at_ms) {
      encode_record(chunk, op, key, value, expire_at_ms);
      if (chunk.size() >= kRewriteChunk) {
        write_full(tmp_fd, chunk.data(), chunk.size());
        written += chunk.size();
        chunk.clear();
      }
    };
    produce(emit);
    write_full(tmp_fd, chunk.data(), chunk.size());
    written += chunk.size();
    durable_sync(tmp_fd);  // 1. new contents durable before they become visible
  } catch (...) {
    ::close(tmp_fd);
    ::unlink(tmp.c_str());
    throw;
  }
  ::close(tmp_fd);

  // 2. The commit point. rename(2) is atomic: any observer (including replay
  //    after a crash) sees the complete old log or the complete new one.
  if (::rename(tmp.c_str(), path_.c_str()) != 0) {
    const int saved = errno;
    ::unlink(tmp.c_str());
    throw std::system_error(saved, std::generic_category(), "rename " + tmp);
  }

  // 3. Our fd still points at the old, now-unlinked inode. Switch to the new
  //    file before anything else can fail, so appends never go to a ghost.
  const int new_fd = ::open(path_.c_str(), O_WRONLY | O_APPEND | O_CLOEXEC);
  if (new_fd < 0) throw WalFatal("wal: reopen after rewrite failed");
  ::close(fd_);
  fd_ = new_fd;
  file_size_ = written;
  // Every mutation appended so far is reflected in the new, fsynced file.
  synced_seq_ = appended_seq_;

  // 4. Make the rename itself survive a power cut.
  try {
    sync_dir(path_);
  } catch (const std::system_error& e) {
    throw WalFatal(std::string("wal: directory fsync after rewrite failed: ") + e.what());
  }
}

}  // namespace kvstore
