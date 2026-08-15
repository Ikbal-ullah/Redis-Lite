#include "kvstore/wal.hpp"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <cstring>
#include <stdexcept>
#include <system_error>

namespace kvstore {
namespace {

constexpr std::size_t kHeaderSize = 8;  // u32 length + u32 crc

// A single corrupted record must not be mistaken for a valid one, and a
// truncated tail must not be mistaken for corruption of everything after it.
// CRC-32 (the IEEE 802.3 polynomial, reflected) catches both cheaply. The table
// is built once on first use; the loop is the textbook byte-at-a-time variant.
const std::array<std::uint32_t, 256>& crc_table() {
  static const std::array<std::uint32_t, 256> table = [] {
    std::array<std::uint32_t, 256> t{};
    for (std::uint32_t i = 0; i < 256; ++i) {
      std::uint32_t c = i;
      for (int bit = 0; bit < 8; ++bit) {
        c = (c & 1u) ? (0xEDB88320u ^ (c >> 1)) : (c >> 1);
      }
      t[i] = c;
    }
    return t;
  }();
  return table;
}

std::uint32_t crc32(const char* data, std::size_t length) {
  const auto& table = crc_table();
  std::uint32_t crc = 0xFFFFFFFFu;
  for (std::size_t i = 0; i < length; ++i) {
    const auto byte = static_cast<unsigned char>(data[i]);
    crc = table[(crc ^ byte) & 0xFFu] ^ (crc >> 8);
  }
  return crc ^ 0xFFFFFFFFu;
}

// Integers go to disk one byte at a time, least significant first. Never
// memcpy a struct or an int into a file: the byte order and the padding are
// properties of the machine that wrote it, and the file outlives the machine.
void put_u32(std::vector<char>& out, std::uint32_t value) {
  out.push_back(static_cast<char>(value & 0xFFu));
  out.push_back(static_cast<char>((value >> 8) & 0xFFu));
  out.push_back(static_cast<char>((value >> 16) & 0xFFu));
  out.push_back(static_cast<char>((value >> 24) & 0xFFu));
}

void put_i64(std::vector<char>& out, std::int64_t value) {
  const auto raw = static_cast<std::uint64_t>(value);
  for (int shift = 0; shift < 64; shift += 8) {
    out.push_back(static_cast<char>((raw >> shift) & 0xFFu));
  }
}

std::uint32_t get_u32(const char* p) {
  return static_cast<std::uint32_t>(static_cast<unsigned char>(p[0])) |
         (static_cast<std::uint32_t>(static_cast<unsigned char>(p[1])) << 8) |
         (static_cast<std::uint32_t>(static_cast<unsigned char>(p[2])) << 16) |
         (static_cast<std::uint32_t>(static_cast<unsigned char>(p[3])) << 24);
}

std::int64_t get_i64(const char* p) {
  std::uint64_t raw = 0;
  for (int i = 0; i < 8; ++i) {
    raw |= static_cast<std::uint64_t>(static_cast<unsigned char>(p[i])) << (i * 8);
  }
  return static_cast<std::int64_t>(raw);
}

void put_bytes(std::vector<char>& out, std::string_view bytes) {
  put_u32(out, static_cast<std::uint32_t>(bytes.size()));
  out.insert(out.end(), bytes.begin(), bytes.end());
}

// A single write() is permitted to write fewer bytes than asked, and is
// permitted to fail with EINTR if a signal arrives mid-call. Both are normal.
// Every direct write to a descriptor must therefore be a loop, and forgetting
// this is one of the most common ways a "working" system corrupts data under
// load.
void write_full(int fd, const char* data, std::size_t length) {
  std::size_t written = 0;
  while (written < length) {
    const ssize_t n = ::write(fd, data + written, length - written);
    if (n < 0) {
      if (errno == EINTR) continue;
      throw std::system_error(errno, std::generic_category(), "wal write");
    }
    written += static_cast<std::size_t>(n);
  }
}

// Reads exactly `length` bytes or reports how many were actually available.
std::size_t read_upto(int fd, char* dest, std::size_t length) {
  std::size_t total = 0;
  while (total < length) {
    const ssize_t n = ::read(fd, dest + total, length - total);
    if (n < 0) {
      if (errno == EINTR) continue;
      throw std::system_error(errno, std::generic_category(), "wal read");
    }
    if (n == 0) break;  // end of file
    total += static_cast<std::size_t>(n);
  }
  return total;
}

}  // namespace

Wal::Wal(std::string path, SyncPolicy policy)
    : path_(std::move(path)), policy_(policy) {}

Wal::~Wal() {
  // Destructors must not throw. If the final close fails there is nobody left
  // to tell, so swallow it -- the process is going away regardless.
  if (fd_ >= 0) {
    ::close(fd_);
    fd_ = -1;
  }
}

void Wal::open() {
  std::lock_guard<std::mutex> lock(mutex_);
  if (fd_ >= 0) return;

  // O_APPEND is not a convenience here, it is a correctness property: the
  // kernel makes the seek-to-end and the write a single atomic step, so two
  // writers can never land on the same offset and shred each other's records.
  const int fd = ::open(path_.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
  if (fd < 0) {
    throw std::system_error(errno, std::generic_category(), "open wal " + path_);
  }
  fd_ = fd;
}

void Wal::close() {
  std::lock_guard<std::mutex> lock(mutex_);
  if (fd_ >= 0) {
    ::close(fd_);
    fd_ = -1;
  }
}

void Wal::encode(const WalRecord& record, std::vector<char>& out) const {
  out.clear();
  // Reserve the header, fill the payload, then patch the header in place --
  // the length and CRC are not known until the payload exists.
  out.resize(kHeaderSize, '\0');

  out.push_back(static_cast<char>(record.op));
  put_bytes(out, record.key);
  switch (record.op) {
    case WalOp::kSet:
      put_bytes(out, record.value);
      put_i64(out, record.expire_at_ms);
      break;
    case WalOp::kExpire:
      put_i64(out, record.expire_at_ms);
      break;
    case WalOp::kDel:
      break;
  }

  const std::size_t payload_length = out.size() - kHeaderSize;
  const std::uint32_t crc = crc32(out.data() + kHeaderSize, payload_length);

  std::vector<char> header;
  header.reserve(kHeaderSize);
  put_u32(header, static_cast<std::uint32_t>(payload_length));
  put_u32(header, crc);
  std::copy(header.begin(), header.end(), out.begin());
}

void Wal::append(const WalRecord& record) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (fd_ < 0) {
    throw std::runtime_error("wal append before open: " + path_);
  }

  encode(record, scratch_);
  write_full(fd_, scratch_.data(), scratch_.size());
  bytes_written_ += scratch_.size();

  if (policy_ == SyncPolicy::kAlways) {
    // Deliberately inside the lock. Returning from append() is the moment the
    // caller is entitled to believe the record is durable, so the flush cannot
    // be deferred past it.
#if defined(__APPLE__)
    // On macOS, fsync() only guarantees the data has left the kernel's page
    // cache for the *drive*; the drive is free to keep it in its own volatile
    // write cache. F_FULLFSYNC additionally tells the drive to flush that
    // cache to the physical medium. It is markedly slower and it is the only
    // call on this platform that actually survives a power cut.
    if (::fcntl(fd_, F_FULLFSYNC) < 0) {
      if (::fsync(fd_) < 0) {
        throw std::system_error(errno, std::generic_category(), "wal fsync");
      }
    }
#else
    if (::fsync(fd_) < 0) {
      throw std::system_error(errno, std::generic_category(), "wal fsync");
    }
#endif
  }
}

void Wal::sync() {
  std::lock_guard<std::mutex> lock(mutex_);
  if (fd_ < 0) return;
  if (::fsync(fd_) < 0) {
    throw std::system_error(errno, std::generic_category(), "wal fsync");
  }
}

std::size_t Wal::replay(const std::function<void(const WalRecord&)>& on_record) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (fd_ >= 0) {
    throw std::runtime_error("wal replay after open: " + path_);
  }

  // O_RDWR because a torn tail has to be truncated away, not just skipped.
  const int fd = ::open(path_.c_str(), O_RDWR | O_CREAT, 0644);
  if (fd < 0) {
    throw std::system_error(errno, std::generic_category(), "open wal " + path_);
  }

  struct stat st {};
  if (::fstat(fd, &st) < 0) {
    const int saved = errno;
    ::close(fd);
    throw std::system_error(saved, std::generic_category(), "fstat wal");
  }

  const auto file_size = static_cast<std::size_t>(st.st_size);
  std::string contents(file_size, '\0');
  const std::size_t got = file_size == 0 ? 0 : read_upto(fd, contents.data(), file_size);
  contents.resize(got);

  std::size_t offset = 0;       // cursor into contents
  std::size_t good_offset = 0;  // end of the last fully valid record
  std::size_t applied = 0;

  while (offset + kHeaderSize <= contents.size()) {
    const char* header = contents.data() + offset;
    const std::uint32_t payload_length = get_u32(header);
    const std::uint32_t expected_crc = get_u32(header + 4);

    // A length that runs past EOF means the process died mid-write. That is
    // expected, not an error: the client never got an acknowledgement for it.
    if (payload_length > contents.size() - offset - kHeaderSize) break;

    const char* payload = header + kHeaderSize;
    if (crc32(payload, payload_length) != expected_crc) break;

    // The payload is intact; now decode it. A decode failure here means a
    // structurally valid but semantically impossible record, so stop too.
    std::size_t p = 0;
    auto remaining = [&] { return payload_length - p; };
    auto take_bytes = [&](std::string& dest) -> bool {
      if (remaining() < 4) return false;
      const std::uint32_t length = get_u32(payload + p);
      p += 4;
      if (remaining() < length) return false;
      dest.assign(payload + p, length);
      p += length;
      return true;
    };
    auto take_i64 = [&](std::int64_t& dest) -> bool {
      if (remaining() < 8) return false;
      dest = get_i64(payload + p);
      p += 8;
      return true;
    };

    if (remaining() < 1) break;
    const auto op = static_cast<WalOp>(static_cast<unsigned char>(payload[p]));
    p += 1;

    WalRecord record;
    record.op = op;
    bool ok = take_bytes(record.key);
    if (ok) {
      switch (op) {
        case WalOp::kSet:
          ok = take_bytes(record.value) && take_i64(record.expire_at_ms);
          break;
        case WalOp::kExpire:
          ok = take_i64(record.expire_at_ms);
          break;
        case WalOp::kDel:
          break;
        default:
          ok = false;  // unknown opcode
          break;
      }
    }
    if (!ok) break;

    on_record(record);
    ++applied;
    offset += kHeaderSize + payload_length;
    good_offset = offset;
  }

  // Chop off whatever followed the last intact record. Leaving the garbage in
  // place would mean the next append starts mid-record, and every subsequent
  // replay would stop at the same spot -- a log that silently stops recording.
  if (good_offset < contents.size()) {
    if (::ftruncate(fd, static_cast<off_t>(good_offset)) < 0) {
      const int saved = errno;
      ::close(fd);
      throw std::system_error(saved, std::generic_category(), "truncate wal");
    }
  }

  ::close(fd);
  return applied;
}

}  // namespace kvstore
