// Copyright (C) 2024-2028 Jun Zhang and nats.cpp contributors
// SPDX-License-Identifier: MIT
#include <Disruptor.hpp>

#include <boost/ut.hpp>
#include <future>
#include <ranges>
#include <tuple>
#include <vector>

namespace rng = std::ranges;
namespace vws = std::views;
namespace ut = boost::ut;
using namespace ut;
using namespace ut::spec;

namespace shared_ring {
ut::suite<"[shared_ring]"> _ = [] {
  using namespace nats;

  describe("difference()") = [] {
    it("returns the difference of two sequences") = [] {
      Seq_t a{1}, b{2}, c{1}, d{0};
      expect(diff(a, a) == 0_l);
      expect(diff(a, b) == -1_l);
      expect(diff(b, a) == 1_l);
      expect(diff(c, a) == 0_l);
      expect(diff(d, a) == -1_l);
      expect(diff(a, d) == 1_l);
      expect(diff(d, d) == 0_l);
    };
  };

  describe("seq_t min()") = [] {
    it("returns the slowest (min) running sequence of an array") = [] {
      AtomicSeq_t seqs[] = {10, 7, 4, 3, 6, 9};
      auto seq_ptrs = seqs |
                      vws::transform([](AtomicSeq_t &seq) { return &seq; }) |
                      rng::to<std::vector>();

      auto min_seq = min(std::span{seq_ptrs});
      expect(min_seq == 3_u);
      expect(minAfter(1, std::span{seq_ptrs}) == 3_u);
      expect(minAfter(2, std::span{seq_ptrs}) == 3_u);
      expect(minAfter(3, std::span{seq_ptrs}) == 3_u);

      expect(minAfter(4, std::span{seq_ptrs}) == 3_u);
      expect(minAfter(5, std::span{seq_ptrs}) == 4_u);
      expect(minAfter(8, std::span{seq_ptrs}) == 7_u);
      expect(minAfter(11, std::span{seq_ptrs}) == 10_u);
    };
  };

  describe("seqRange") = [] {
    it("gives a range") = [] {
      auto r = SeqRange(3, 5);
      expect(r.size() == 5_u);
      expect(r.first() == 3_u);
      expect(r.end() == 8_u);
      expect(r.last() == 7_u);
      expect(r[0] == 3_u and r[1] == 4_u and r[4] == 7_u);
    };
  };

  describe("RingBuffer") = [] {
    it("operates as ring") = [] {
      RingBuffer<storage::Dynamic<int>> d(8);
      d[0] = 5, d[1] = 4, d[2] = 3, d[3] = 2, d[4] = 1;
      expect(d.size() == 8_u);
      expect(d[0] == 5_u and d[1] == 4_u and d[2] == 3_u and d[3] == 2_u and
             d[4] == 1_u);

      RingBuffer<storage::Static<int, 8>> s;
      static_assert(s.size() == 8);
      s[0] = 8, s[7] = 1;
      expect(s.size() == 8_u);
      expect(s[0] == 8 and s[7] == 1);
    };
  };

  describe("wait policy") = [] {
    it("wait_until_published, unconditional") = [] {
      auto wait_unconditional = [](WaitPolicy auto &&wait, Seq_t seq,
                                   std::span<AtomicSeq_t> seqs) {
        auto seq_ptrs =
            seqs |
            vws::transform([](auto &seq) -> AtomicSeq_t * { return &seq; }) |
            rng::to<std::vector>();
        bool published{false};
        auto res = std::async([&] mutable {
          // should block until all seqs pass 4
          Seq_t min = wait.waitUntilPublished(
              seq, std::span{seq_ptrs.data(), seq_ptrs.size()});
          published = true;
          return min;
        });
        for (auto &seq : seqs)
          seq.store(6, std::memory_order::release);

        expect(res.get() == 6_u);
        expect(published == true);
      };

      AtomicSeq_t seqs[] = {1, 2, 3};
      wait_unconditional(wait_policy::Spin{}, 4, seqs);
      wait_unconditional(wait_policy::Blocking{}, 4, seqs);
    };

    it("spin policy, wait_until_published, with timeout") = [] {
      auto wait_until = [](WaitPolicy auto &&wait, Seq_t seq,
                           std::span<AtomicSeq_t> seqs) {
        auto seq_ptrs = seqs | vws::transform([](auto &seq) { return &seq; }) |
                        rng::to<std::vector>();
        bool timeout{false};
        auto res = std::async([&] {
          // should block until all seqs pass 4
          Seq_t min = wait.waitUntilPublished(seq, std::span{seq_ptrs},
                                              std::chrono::milliseconds(1));
          timeout = true;
          return min;
        });
        // seqs values not changed, so the wait gets timeout and return
        expect(res.get() == 1_u);
        expect(timeout == true);
      };

      AtomicSeq_t seqs[] = {1, 2, 3};
      wait_until(wait_policy::Spin{}, 4, seqs);
      wait_until(wait_policy::Blocking{}, 4, seqs);
    };
  };

  describe("claim policy") = [] {
    it("") = [] {
      wait_policy::Spin spin;
      claim_policy::SingleThreaded st{64, spin};
      //
    };
  };
};
} // namespace shared_ring
