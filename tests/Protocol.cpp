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

  describe("_parseControlLine") = [] {
    Parser p;
    it("should parse the control line, give result, and set state") = [] {
      str_view line = "PING\r\n";
      //
    };
  };
};

} // namespace protocol
