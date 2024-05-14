// Copyright (C) 2024-2028 Jun Zhang and nats.cpp contributors
// SPDX-License-Identifier: MIT
#include <Protocol.hpp>

#include <boost/ut.hpp>

namespace rng = std::ranges;
namespace vws = std::views;
namespace ut = boost::ut;
using namespace ut;
using namespace ut::spec;

namespace protocol {

ut::suite<"[protocol]"> _ = [] {
  using namespace nats;
  using Result = Parser::Result;

  describe("_parseControlLine") = [] {
    Parser p;
    it("should parse the control line, give result, and set state") =
        [=] mutable {
          auto res = p._parseControlLine("\r\n"sv);
          expect(res.op == Result::None and res.error == err::InvalidMsg);

          res = p._parseControlLine("+OK\r\n"sv);
          expect(res.op == Result::Ok and res.error == Error::none());
          res = p._parseControlLine("-ERR\r\n"sv);
          expect(res.op == Result::Err and res.error == Error::none());
          res = p._parseControlLine("-ERR  \t  'error description' \t \r\n"sv);
          expect(res.op == Result::Err and res.error == Error::none());
          expect(res.payload == "'error description'"sv);
          res = p._parseControlLine("PING\r\n"sv);
          expect(res.op == Result::Ping and res.error == Error::none());
          res = p._parseControlLine("PONG\r\n"sv);
          expect(res.op == Result::Pong and res.error == Error::none());
          res = p._parseControlLine(
              "INFO \t   {\"option_name\":option_value} \t  \r\n"sv);
          expect(res.op == Result::Info and res.error == Error::none());
          expect(res.payload == "{\"option_name\":option_value}"sv);
        };
  };
};

} // namespace protocol
