// Test runner. Optional argument: a substring filter on test names.

#include <csignal>
#include <iostream>
#include <string_view>

#include "harness.hpp"

int main(int argc, char** argv) {
  std::signal(SIGPIPE, SIG_IGN);
  const std::string_view filter = argc > 1 ? argv[1] : "";
  int passed = 0;
  int failed = 0;
  for (const auto& test : testing::registry()) {
    if (!filter.empty() && std::string_view(test.name).find(filter) == std::string_view::npos) continue;
    try {
      test.fn();
      ++passed;
      std::cout << "  PASS  " << test.name << "\n";
    } catch (const std::exception& e) {
      ++failed;
      std::cout << "  FAIL  " << test.name << "\n        " << e.what() << "\n";
    }
  }
  std::cout << "\n" << passed << " passed, " << failed << " failed\n";
  return failed == 0 ? 0 : 1;
}
