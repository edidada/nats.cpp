// Copyright (C) 2024-2028 Jun Zhang and nats.cpp contributors
// SPDX-License-Identifier: MIT
#include <Common.hpp>

#include <boost/ut.hpp>
#include <icecream.hpp>

namespace rng = std::ranges;
namespace vws = std::views;
namespace ut = boost::ut;
using namespace ut;
using namespace ut::spec;

namespace common {

ut::suite<"[common]"> _ = [] {
  using namespace nats;
  using namespace std::literals;

  describe("trim") = [] {
    auto str = " \t\r  \t \n   ab cd\tef\rgh\nij  \r \t\n  "sv;

    it("trim_front should remove spaces from start of string") = [=] {
      auto res = trim_front(""sv, ""sv);
      expect(res == ""sv);
      res = trim_front(""sv, "\r \t\n"sv);
      expect(res == ""sv);
      res = trim_front("   \r   \t  \n \r\n  \t"sv, "\r \t\n"sv);
      expect(res == ""sv);

      res = trim_front(str, " \t\r\n"sv);
      expect(res == "ab cd\tef\rgh\nij  \r \t\n  "sv);
    };

    it("trim_back should remove spaces from end of string") = [=] {
      auto res = trim_back(""sv, ""sv);
      expect(res == ""sv);
      res = trim_back(""sv, "\r \t\n"sv);
      expect(res == ""sv);
      res = trim_back("   \r   \t  \n \r\n  \t"sv, "\r \t\n"sv);
      expect(res == ""sv);

      res = trim_back(str, " \t\r\n"sv);
      expect(res == " \t\r  \t \n   ab cd\tef\rgh\nij"sv);
    };

    it("trim should remove spaces from both side of string") = [=] {
      auto res = trim(""sv, ""sv);
      expect(res == ""sv);
      res = trim(""sv, "\r \t\n"sv);
      expect(res == ""sv);
      res = trim(str, " \t\r\n"sv);
      expect(res == "ab cd\tef\rgh\nij"sv);
    };
  };

  describe("split by pattern") = [] {
    it("should split string with pattern as a whole") = [] {
      auto fields = split(by_pattern, ""sv, ""sv);
      expect(fields.size() == 0_u);

      fields = split(by_pattern, ""sv, "  "sv);
      expect(fields.size() == 0_u);

      fields = split(by_pattern, " "sv, "  "sv);
      expect(fields.size() == 1_u);
      expect(fields[0] == " "sv);

      fields = split(by_pattern, "  "sv, "  "sv);
      expect(fields.size() == 0_u);

      fields = split(by_pattern, "   "sv, "  "sv);
      expect(fields.size() == 1_u);
      expect(fields[0] == " "sv);

      fields = split(by_pattern, "  abc  efg "sv, "  "sv);
      expect(fields.size() == 2_u);
      expect(fields[0] == "abc"sv and fields[1] == "efg "sv);

      fields = split(by_pattern, "abc \t efg  hij\t \t"sv, " \t"sv);
      expect(fields.size() == 2_u);
      expect(fields[0] == "abc"sv and fields[1] == " efg  hij\t"sv);
    };
  };

  describe("split by delims") = [] {
    it("should split string with any of char in delims") = [] {
      auto fields = split(by_delim, ""sv, ""sv);
      expect(fields.size() == 0_u);

      fields = split(by_delim, ""sv, " \t"sv);
      expect(fields.size() == 0_u);
      fields = split(by_delim, " "sv, " \t"sv);
      expect(fields.size() == 0_u);
      fields = split(by_delim, " \t "sv, " \t"sv);
      expect(fields.size() == 0_u);
      fields = split(by_delim, "\t  \t \t \t"sv, " \t"sv);
      expect(fields.size() == 0_u);

      fields = split(by_delim, "abc  efg\t hij\t\tklm"sv, " \t"sv);
      expect(fields.size() == 4_u);
      expect(fields[0] == "abc"sv and fields[1] == "efg"sv and
             fields[2] == "hij"sv and fields[3] == "klm"sv);

      fields = split(by_delim, "  abc  efg\t hij\t\tklm   "sv, " \t"sv);
      expect(fields.size() == 4_u);
      expect(fields[0] == "abc"sv and fields[1] == "efg"sv and
             fields[2] == "hij"sv and fields[3] == "klm"sv);
    };
  };
};
} // namespace common
