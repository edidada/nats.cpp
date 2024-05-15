// Copyright (C) 2024-2028 Jun Zhang and nats.cpp contributors
// SPDX-License-Identifier: MIT
#include <Protocol.hpp>

#include <boost/ut.hpp>
#include <map>

namespace rng = std::ranges;
namespace vws = std::views;
namespace ut = boost::ut;
using namespace ut;
using namespace ut::spec;

namespace protocol {
ut::suite<"[protocol]"> _ = [] {
  using namespace nats;

  describe("_parseInfoHeader") = [] {
    Parser p;
    it("should parse the header and fill Header fields") = [=] mutable {
      // zero
      auto res = p._parseInfoHeader("");
      expect(res.size() == 0_u);
      // errors, non-exhaustive, just some common sense cases
      res = p._parseInfoHeader(":");
      expect(res.size() == 1_u and res.contains(hdr::Error));
      res = p._parseInfoHeader("\"server_id\":"); // no value
      expect(res.size() == 1_u and res.contains(hdr::Error));
      // res = p._parseInfoHeader("server_id:\"server id\""); // field no quote
      // expect(res.size() == 1_u and res.contains(hdr::Error));

      // single field
      res = p._parseInfoHeader(R"("server_id":"server id")");
      expect(res.size() == 1_u and
             std::get<std::string>(res[hdr::ServerId]) == "server id");
      res = p._parseInfoHeader(R"("port":8888)");
      expect(res.size() == 1_u and std::get<int>(res[hdr::Port]) == 8888);
      res = p._parseInfoHeader(R"("client_id":123456789)");
      expect(res.size() == 1_u and
             std::get<uint64_t>(res[hdr::ClientId]) == 123456789);
      res = p._parseInfoHeader(R"("headers":true)");
      expect(res.size() == 1_u and std::get<bool>(res[hdr::Headers]) == true);
      res = p._parseInfoHeader(R"("connect_urls":["1.2.3.4:55","abc","567"])");
      expect(res.size() == 1_u and
             eq(std::get<std::vector<std::string>>(res[hdr::ConnectUrls]),
                std::vector{"1.2.3.4:55"s, "abc"s, "567"s}));
      // multiple fields
      res = p._parseInfoHeader(
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
    Parser p;
    it("should parse the control line, give result, and set state") =
        [=] mutable {
          auto res = p._parseControlLine("\r\n"sv);
          expect(res.op == ParseResult::None and res.err() == err::InvalidMsg);

          res = p._parseControlLine("+OK\r\n"sv);
          expect(res.op == ParseResult::Ok and res.err() == Error::none());
          res = p._parseControlLine("-ERR\r\n"sv);
          expect(res.op == ParseResult::Err and res.err() == Error::none());
          res = p._parseControlLine("-ERR  \t  'error description' \t \r\n "sv);
          expect(res.op == ParseResult::Err and res.err() == Error::none());
          expect(res.payload == "'error description'"sv);
          res = p._parseControlLine("PING\r\n"sv);
          expect(res.op == ParseResult::Ping and res.err() == Error::none());
          res = p._parseControlLine("PONG\r\n"sv);
          expect(res.op == ParseResult::Pong and res.err() == Error::none());
          // res = p._parseControlLine(R"(
          //   INFO    {"server_id":"server
          //   id","port":8888,"client_id":12345,ldm:true}
          //   )"s + "\r\n");
          // expect(res.op == ParseResult::Info and res.err() == Error::none());
          // expect(res.get<std::string>(hdr::ServerId) == "server id"sv);
        };
  };
};

} // namespace protocol
