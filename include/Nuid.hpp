// Copyright (C) 2024-2028 Jun Zhang and nats.cpp contributors
// SPDX-License-Identifier: MIT
#pragma once
#include <Common.hpp>
#include <icecream.hpp>

#include <array>
#include <cstring>
#include <random>
#include <ranges>

#include <sys/random.h>

namespace nats {

namespace nuid {

struct Base62 {
  constexpr static std::string_view Digits =
      "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

  constexpr static size_t SeqLen = 10;
  constexpr static uint64_t MaxSeq = 839'299'365'868'340'224; // Base ^ SeqLen
  constexpr static size_t PrefixLen = 12;

  constexpr static uint64_t MinInc = 33;
  constexpr static uint64_t MaxInc = 333;

  static std::lldiv_t div(uint64_t x) {
    return std::lldiv(static_cast<long long>(x),
                      static_cast<long long>(Digits.size()));
  }
};

// TODO: has problem, duplicates after 64 iterations
// struct Rfc4648_Base64 {
//   constexpr static std::string_view Digits =
//       "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
//
//   constexpr static size_t SeqLen = 10;
//   constexpr static uint64_t MaxSeq = 1'152'921'504'606'846'976; // Base ^
//   SeqLen constexpr static size_t PrefixLen = 12;
//
//   constexpr static uint64_t MinInc = 34;
//   constexpr static uint64_t MaxInc = 372;
//
//   static auto div(uint64_t x) {
//     struct Result {
//       size_t quot;
//       size_t rem;
//     };
//     return Result{
//         .quot = x & 0xFFFF'FFFF'FFFF'FFC0,
//         .rem = x & 0x3F,
//     };
//   }
// };

}; // namespace nuid

// -----
// Nuid
// -----
template <typename Base> struct Nuid {
  using Base_t = Base;
  constexpr static size_t BaseSize = Base::Digits.size();
  constexpr static size_t TotalLen = Base::PrefixLen + Base::SeqLen;
  constexpr static uint64_t Inc = Base::MaxInc - Base::MinInc;

  using uniform_dist = std::uniform_int_distribution<std::mt19937::result_type>;

  Nuid() {
    reset_seq();
    randomize_prefix();
  }

  std::string next() {
    seq_ += inc_;
    if (seq_ >= Base_t::MaxSeq) [[unlikely]] {
      reset_seq();
      randomize_prefix();
    }
    auto buf = std::array<char, TotalLen>{};
    // copy prefix to buf
    std::ranges::copy(prefix_.begin(), prefix_.end(), buf.begin());
    // fill the rest of bu in base62
    generate_postseq(seq_, {buf.begin() + prefix_.size(), Base_t::SeqLen});
    return {buf.begin(), buf.end()};
  }

  void generate_postseq(size_t seq, std::span<char> buf) {
    namespace rng = std::ranges;
    namespace vws = std::views;
    // in fact, the `vws::reverse` call is not necessary
    // but as the entropy tend to concentrate at the beginning of the iteration,
    // using a `reverse` helps to make sure the difference occur at tail of the
    // generated string, hence easier for human eyes to check out
    rng::for_each(vws::reverse(buf), [seq](char &c) mutable {
      auto dv = Base_t::div(seq);
      c = Base_t::Digits[dv.rem];
      seq = static_cast<size_t>(dv.quot);
    });
  }

  void randomize_prefix() {
    // draw entropy from getrandom (effectively /dev/random) for prefix
    std::array<uint8_t, Base_t::PrefixLen> random_bytes;
    if (auto bytes_copied =
            getrandom(random_bytes.data(), Base_t::PrefixLen, GRND_RANDOM);
        bytes_copied != Base_t::PrefixLen)
      panic(std::format("nuid: failed to generate crypto random number: {}",
                        bytes_copied < 0 ? strerror(errno)
                                         : std::to_string(bytes_copied)));

    for (auto [idx, c] : std::views::enumerate(random_bytes))
      prefix_[static_cast<size_t>(idx)] = Base_t::Digits[c % BaseSize];
  }

  void reset_seq() {
    seq_ = seq_dist_(rng_);
    inc_ = Base_t::MinInc + inc_dist_(rng_);
  }

  // random device, generators
  std::random_device randev_{};
  std::mt19937 rng_{randev_()};
  uniform_dist seq_dist_{0, Base_t::MaxSeq};
  uniform_dist inc_dist_{0, Inc};

  uint64_t seq_, inc_;
  std::array<char, Base_t::PrefixLen> prefix_;
};

} // namespace nats
