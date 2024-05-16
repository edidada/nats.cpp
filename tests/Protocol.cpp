// Copyright (C) 2024-2028 Jun Zhang and nats.cpp contributors
// SPDX-License-Identifier: MIT
#include <Protocol.hpp>

#include <boost/asio/detached.hpp>
#include <boost/cobalt/main.hpp>
#include <boost/cobalt/run.hpp>
#include <boost/cobalt/spawn.hpp>
#include <boost/ut.hpp>

#include <map>

namespace ut = boost::ut;
using namespace ut;
using namespace ut::spec;
using namespace nats;

namespace nats {

ParseResult _parseControlLine(buf_view_t data);
Header _parseInfoHeader(str_view sv);
ParseResult _parseMsgFields(std::vector<str_view> fields);
str_view _processPayload(buf_view_t data);

} // namespace nats

namespace protocol {
ut::suite<"[protocol]"> _ = [] {
  describe("_parseInfoHeader") = [] {
    it("should parse the header and fill Header fields") = [=] mutable {
      // zero
      auto res = _parseInfoHeader("");
      expect(res.size() == 0_u);
      // errors, non-exhaustive, just some common sense cases
      res = _parseInfoHeader(":");
      expect(res.size() == 1_u and res.contains(hdr::Error));
      res = _parseInfoHeader("\"server_id\":"); // no value
      expect(res.size() == 1_u and res.contains(hdr::Error));
      // res = p._parseInfoHeader("server_id:\"server id\""); // field no quote
      // expect(res.size() == 1_u and res.contains(hdr::Error));

      // single field
      res = _parseInfoHeader(R"("server_id":"server id")");
      expect(res.size() == 1_u and
             std::get<std::string>(res[hdr::ServerId]) == "server id");
      res = _parseInfoHeader(R"("port":8888)");
      expect(res.size() == 1_u and std::get<int>(res[hdr::Port]) == 8888);
      res = _parseInfoHeader(R"("client_id":123456789)");
      expect(res.size() == 1_u and
             std::get<uint64_t>(res[hdr::ClientId]) == 123456789);
      res = _parseInfoHeader(R"("headers":true)");
      expect(res.size() == 1_u and std::get<bool>(res[hdr::Headers]) == true);
      res = _parseInfoHeader(R"("connect_urls":["1.2.3.4:55","abc","567"])");
      expect(res.size() == 1_u and
             eq(std::get<std::vector<std::string>>(res[hdr::ConnectUrls]),
                std::vector{"1.2.3.4:55"s, "abc"s, "567"s}));
      // multiple fields
      res = _parseInfoHeader(
          R"("server_id":"server id","connect_urls":["1.2.3.4:55","abc","567"],"port":8888,"client_id":123456789,"headers":true)");
      expect(res.size() == 5_u);
      expect(std::get<std::string>(res[hdr::ServerId]) == "server id");
      expect(std::get<std::vector<std::string>>(res[hdr::ConnectUrls]) ==
             std::vector{"1.2.3.4:55"s, "abc"s, "567"s});
      // expect(std::get<int>(res[hdr::Port]) == 8888);
      expect(std::get<uint64_t>(res[hdr::ClientId]) == 123456789);
      expect(std::get<bool>(res[hdr::Headers]) == true);
    };
  };

  describe("_parseControlLine") = [] {
    it("should parse the control line, give result, and set state") = [=] mutable {
      auto res = _parseControlLine("\r\n"sv);
      expect(res.op == ParseResult::None and res.err() == err::InvalidMsg);

      res = _parseControlLine("+OK\r\n"sv);
      expect(res.op == ParseResult::Ok and res.err() == Error::none());
      res = _parseControlLine("-ERR\r\n"sv);
      expect(res.op == ParseResult::Err and res.err() == Error(""s));
      res = _parseControlLine("-ERR  \t  'error description' \t \r\n "sv);
      expect(res.op == ParseResult::Err and
             res.err() == Error("'error description'"s));
      res = _parseControlLine("PING\r\n"sv);
      expect(res.op == ParseResult::Ping and res.err() == Error::none());
      res = _parseControlLine("PONG\r\n"sv);
      expect(res.op == ParseResult::Pong and res.err() == Error::none());
      res = _parseControlLine(
          R"(INFO    {"server_id":"server id","port":8888,"client_id":12345,ldm:true})"s +
          "\r\n");
      expect(res.op == ParseResult::Info and res.err() == Error::none());
      expect(res.get<std::string>(hdr::ServerId) == "server id"sv);
      expect(res.get<int>(hdr::Port) == 8888);
      expect(res.get<uint64_t>(hdr::ClientId) == 12345);
      expect(res.get<bool>(hdr::Ldm) == true);
    };
  };

  describe("parse") = [] {
    it("should parse the byte stream, and yield ParseResult") = [] {};
  };
};

task_t<void> test_parse() {
  // set up a large (4k) buffer, so that we can push first, then pull result
  channel_t<buf_view_t> chan{MaxControlLineSize};

  co_await [](channel_t<buf_view_t> &chan) -> promise_t<bool> {
    co_await chan.write("+OK\r\n"sv);
    auto parser = parse(chan);
    ParseResult res = co_await parser;
    expect(res.op == ParseResult::Ok and res.err() == Error::none());

    co_await chan.write("-ERR\r\n"sv);
    res = co_await parser;
    expect(res.op == ParseResult::Err and res.err() == Error(""s));

    co_await chan.write("-ERR  \t  'error description' \t \r\n "sv);
    res = co_await parser;
    expect(res.op == ParseResult::Err and
           res.err() == Error("'error description'"s));

    co_await chan.write("PING\r\n"sv);
    res = co_await parser;
    expect(res.op == ParseResult::Ping and res.err() == Error::none());

    co_await chan.write("PONG\r\n"sv);
    res = co_await parser;
    expect(res.op == ParseResult::Pong and res.err() == Error::none());

    co_await chan.write("INFO {}\r\n");
    res = co_await parser;
    expect(res.op == ParseResult::Info and res.err() == Error::none());

    // auto line = "INFO {\"server_id\":\"server id\","
    //             "\"port\":8888,\"client_id\":12345,\"ldm\":true}\r\n";
    // co_await chan.write(line);
    // res = co_await parser;
    // expect(res.op == ParseResult::Info and res.err() == Error::none());

    co_return true;
  }(chan);

  chan.close();
  co_return;
}

} // namespace protocol

int main() {
  cobalt::run(protocol::test_parse());

  return 0;
}

// cobalt::main co_main(int, char *[]) {
//   co_await protocol::test_parse();
//
//   co_return 0;
// }
