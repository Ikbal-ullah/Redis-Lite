// A dependency-free test harness: self-registering TEST()s, CHECK/CHECK_EQ,
// and a few fixtures shared across the test files.

#ifndef KVSTORE_TEST_HARNESS_HPP
#define KVSTORE_TEST_HARNESS_HPP

#include <atomic>
#include <cstdint>
#include <exception>
#include <filesystem>
#include <sstream>
#include <string>
#include <unistd.h>
#include <vector>

namespace testing {

struct Failure : std::exception {
  explicit Failure(std::string m) : text(std::move(m)) {}
  const char* what() const noexcept override { return text.c_str(); }
  std::string text;
};

struct Case {
  const char* name;
  void (*fn)();
};

inline std::vector<Case>& registry() {
  static std::vector<Case> cases;
  return cases;
}

struct Registrar {
  Registrar(const char* name, void (*fn)()) { registry().push_back({name, fn}); }
};

template <typename T>
std::string show(const T& v) {
  std::ostringstream out;
  out << v;
  return out.str();
}
inline std::string show(bool v) { return v ? "true" : "false"; }

}  // namespace testing

#define TEST(name)                                           \
  static void name();                                        \
  static ::testing::Registrar registrar_##name(#name, name); \
  static void name()

#define CHECK(cond)                                                                           \
  do {                                                                                        \
    if (!(cond)) {                                                                            \
      throw ::testing::Failure(std::string(__FILE__) + ":" + std::to_string(__LINE__) +       \
                               ": expected " #cond);                                          \
    }                                                                                         \
  } while (false)

#define CHECK_EQ(actual, expected)                                                            \
  do {                                                                                        \
    const auto a_ = (actual);                                                                 \
    const auto e_ = (expected);                                                               \
    if (!(a_ == e_)) {                                                                        \
      throw ::testing::Failure(std::string(__FILE__) + ":" + std::to_string(__LINE__) + ": " \
                               #actual " == " + ::testing::show(a_) + ", expected " +         \
                               ::testing::show(e_));                                          \
    }                                                                                         \
  } while (false)

#define CHECK_THROWS(expr, type)            \
  do {                                      \
    bool thrown_ = false;                   \
    try {                                   \
      (void)(expr);                         \
    } catch (const type&) {                 \
      thrown_ = true;                       \
    }                                       \
    CHECK(thrown_ && "expected " #type);    \
  } while (false)

namespace testing {

// A WAL path in the temp dir, removed (with its .tmp sibling) on scope exit.
class TempPath {
 public:
  TempPath() {
    static std::atomic<int> counter{0};
    path_ = (std::filesystem::temp_directory_path() /
             ("kvstore-test-" + std::to_string(::getpid()) + "-" +
              std::to_string(counter.fetch_add(1)) + ".wal"))
                .string();
    cleanup();
  }
  ~TempPath() { cleanup(); }
  TempPath(const TempPath&) = delete;
  TempPath& operator=(const TempPath&) = delete;
  const std::string& str() const { return path_; }

 private:
  void cleanup() {
    std::error_code ec;
    std::filesystem::remove(path_, ec);
    std::filesystem::remove(path_ + ".tmp", ec);
  }
  std::string path_;
};

}  // namespace testing

#endif  // KVSTORE_TEST_HARNESS_HPP
