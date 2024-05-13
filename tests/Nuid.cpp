// Copyright (C) 2024-2028 Jun Zhang and nats.cpp contributors
// SPDX-License-Identifier: MIT
#include <Nuid.hpp>

#include <boost/ut.hpp>

#include <unordered_set>

namespace rng = std::ranges;
namespace vws = std::views;
namespace ut = boost::ut;
using namespace ut;
using namespace ut::spec;

namespace nuid {

ut::suite<"[nuid]"> _ = [] {
  using namespace nats;

  describe("Nuid::next") = [] {
    auto check_unique = [](auto &&nuid, auto &&ids) {
      constexpr size_t Iterations = 10'000; // iter > 10000 takes too long
      if constexpr (requires { ids.reserver(Iterations); })
        ids.reserver(Iterations);

      bool unique{true};
      size_t i = 0;
      for (; i < Iterations; ++i) {
        auto id = nuid.next();
        if (auto find = rng::find(ids, id); find == ids.end()) [[likely]] {
          ids.insert(id);
        } else {
          unique = false;
          break;
        }
      }
      return std::make_tuple(unique, i);
    };

    it("should generate different ids across many calls") = [=] {
      {
        std::unordered_set<std::string> ids;
        Nuid<nats::nuid::Base62> nuid;
        auto [unique, iters] = check_unique(nuid, ids);
        expect(unique == true)
            << "Nuid<Base62>: duplicated after " << iters << "iterations";
      }
      // {
      //   std::unordered_set<std::string> ids;
      //   Nuid<nats::nuid::Rfc4648_Base64> nuid;
      //   auto [unique, iters] = check_unique(nuid, ids);
      //   expect(unique == true) << "Nuid<Rfc4648_Base64>: duplicated after "
      //                          << iters << "iterations";
      // }
    };
  };
};

} // namespace nuid
