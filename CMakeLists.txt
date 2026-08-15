cmake_minimum_required(VERSION 3.20)

project(kvstore
        VERSION 1.0.0
        DESCRIPTION "A durable key-value store with a write-ahead log and a non-blocking TCP front end"
        LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
# Refuse GNU extensions: -std=c++20, not -std=gnu++20. If the code compiles
# only with extensions enabled, it is not portable C++ and we want to know.
set(CMAKE_CXX_EXTENSIONS OFF)

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  set(CMAKE_BUILD_TYPE RelWithDebInfo CACHE STRING "Build type" FORCE)
endif()

option(KVSTORE_WERROR "Treat compiler warnings as errors" OFF)
option(KVSTORE_SANITIZE "Build with the address and undefined-behaviour sanitizers" OFF)

add_library(kvstore_warnings INTERFACE)
target_compile_options(kvstore_warnings INTERFACE
  -Wall -Wextra -pedantic
  -Wshadow                 # a local silently hiding a member is almost always a bug
  -Wconversion             # implicit narrowing, e.g. ssize_t -> size_t
  -Wsign-conversion
  -Wnon-virtual-dtor
  -Wold-style-cast
  -Wdouble-promotion
  -Wformat=2)
if(KVSTORE_WERROR)
  target_compile_options(kvstore_warnings INTERFACE -Werror)
endif()

add_library(kvstore_sanitizers INTERFACE)
if(KVSTORE_SANITIZE)
  # ASan catches use-after-free on the buffers; TSan (built separately) is the
  # tool for the locking. They cannot be combined in one binary.
  target_compile_options(kvstore_sanitizers INTERFACE -fsanitize=address,undefined -fno-omit-frame-pointer)
  target_link_options(kvstore_sanitizers INTERFACE -fsanitize=address,undefined)
endif()

# Everything except main() lives in a library so the tests can link the real
# implementation instead of a copy of it.
add_library(kvstore_core STATIC
  src/protocol.cpp
  src/storage.cpp
  src/wal.cpp
  src/server.cpp)

target_include_directories(kvstore_core PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>)

find_package(Threads REQUIRED)
target_link_libraries(kvstore_core
  PUBLIC Threads::Threads
  PRIVATE kvstore_warnings kvstore_sanitizers)

add_executable(kvstore src/main.cpp)
target_link_libraries(kvstore PRIVATE kvstore_core kvstore_warnings kvstore_sanitizers)

# The client shares no code with the server on purpose: it parses replies, which
# the server never does, and it has no use for an event loop. Keeping it
# standalone means the CLI binary does not carry the storage engine around.
add_executable(kvstore-cli src/cli_main.cpp)
target_link_libraries(kvstore-cli PRIVATE kvstore_warnings kvstore_sanitizers)

include(GNUInstallDirs)
install(TARGETS kvstore kvstore-cli RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})

enable_testing()

add_executable(kvstore_tests tests/test_main.cpp)
target_link_libraries(kvstore_tests PRIVATE kvstore_core kvstore_warnings kvstore_sanitizers)

add_test(NAME kvstore_tests COMMAND kvstore_tests)
# The concurrency and recovery cases sleep on real clocks; give them headroom
# on a loaded machine without letting a genuine hang run forever.
set_tests_properties(kvstore_tests PROPERTIES TIMEOUT 120)
