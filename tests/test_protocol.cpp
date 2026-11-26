// RESP2 parser and reply encoders: no sockets, no storage.

#include <string>

#include "harness.hpp"
#include "kvstore/protocol.hpp"

using namespace kvstore;

TEST(resp_array_is_parsed) {
  Command c;
  const std::string in = "*3\r\n$3\r\nSET\r\n$5\r\nhello\r\n$5\r\nworld\r\n";
  const auto r = parse_command(in, c);
  CHECK(r.status == ParseStatus::kOk);
  CHECK_EQ(r.consumed, in.size());
  CHECK(c.kind == CommandKind::kSet);
  CHECK_EQ(c.arity(), 3u);
  CHECK_EQ(std::string(c.args[2]), std::string("world"));
}

TEST(inline_command_is_parsed) {
  Command c;
  const auto r = parse_command("ttl  mykey\r\n", c);
  CHECK(r.status == ParseStatus::kOk);
  CHECK(c.kind == CommandKind::kTtl);
  CHECK_EQ(std::string(c.args[1]), std::string("mykey"));
}

TEST(command_names_are_case_insensitive) {
  CHECK(classify("gEt") == CommandKind::kGet);
  CHECK(classify("compact") == CommandKind::kCompact);
  CHECK(classify("EXPIRE") == CommandKind::kExpire);
  CHECK(classify("GETX") == CommandKind::kUnknown);
}

TEST(every_proper_prefix_is_incomplete) {
  // Feeding the stream one byte at a time is the worst case of TCP framing.
  const std::string in = "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$10\r\n0123456789\r\n";
  for (std::size_t n = 0; n < in.size(); ++n) {
    Command c;
    const auto r = parse_command(std::string_view(in).substr(0, n), c);
    CHECK(r.status == ParseStatus::kIncomplete);
    CHECK_EQ(r.consumed, 0u);
  }
  Command c;
  CHECK(parse_command(in, c).status == ParseStatus::kOk);
}

TEST(pipelined_commands_in_one_buffer_parse_in_order) {
  std::string in;
  for (int i = 0; i < 100; ++i) in += testing::cat("*2\r\n$3\r\nGET\r\n$", std::to_string(i).size(), "\r\n", i, "\r\n");
  in += "PING\r\n";
  std::string_view rest = in;
  for (int i = 0; i < 100; ++i) {
    Command c;
    const auto r = parse_command(rest, c);
    CHECK(r.status == ParseStatus::kOk);
    CHECK_EQ(std::string(c.args[1]), std::to_string(i));
    rest.remove_prefix(r.consumed);
  }
  Command c;
  CHECK(parse_command(rest, c).status == ParseStatus::kOk);
  CHECK(c.kind == CommandKind::kPing);
}

TEST(hostile_lengths_are_rejected) {
  Command c;
  CHECK(parse_command("*99999999\r\n", c).status == ParseStatus::kError);
  CHECK(parse_command("*1\r\n$4294967296\r\n", c).status == ParseStatus::kError);
  CHECK(parse_command("*1\r\n$-5\r\n", c).status == ParseStatus::kError);
  CHECK(parse_command("*x\r\n", c).status == ParseStatus::kError);
  CHECK(parse_command("*1\r\n:1\r\n", c).status == ParseStatus::kError);
}

TEST(bulk_string_without_trailing_crlf_is_rejected) {
  // Audit A3: the baseline accepted this and desynchronised the stream.
  Command c;
  const auto r = parse_command("*3\r\n$3\r\nSET\r\n$1\r\nkXX$1\r\nvYY", c);
  CHECK(r.status == ParseStatus::kError);
}

TEST(binary_values_are_preserved) {
  const std::string value("a\r\nb\0c", 6);
  const std::string in = testing::cat("*2\r\n$4\r\nECHO\r\n$6\r\n", value, "\r\n");
  Command c;
  CHECK(parse_command(in, c).status == ParseStatus::kOk);
  CHECK_EQ(std::string(c.args[1]), value);
}

TEST(reply_encoders_match_the_wire_format) {
  std::string out;
  reply_simple(out, "OK");
  reply_error(out, "ERR x");
  reply_bulk(out, "hi");
  reply_nil(out);
  reply_integer(out, -2);
  reply_empty_array(out);
  CHECK_EQ(out, std::string("+OK\r\n-ERR x\r\n$2\r\nhi\r\n$-1\r\n:-2\r\n*0\r\n"));
}
