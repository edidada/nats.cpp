// Copyright (C) 2024-2028 Jun Zhang and nats.cpp contributors
// SPDX-License-Identifier: MIT
#define ANKERL_NANOBENCH_IMPLEMENT
#include <nanobench.h>

#include <Disruptor.hpp>

#include <cstdint>
#include <iostream>
#include <thread>
#include <vector>

namespace nb = ankerl::nanobench;

template <typename WaitPolicy, template <typename T> typename ClaimPolicy>
size_t CalcOps(size_t buffer_size, size_t iter_count, size_t producer_count) {
  WaitPolicy wait_policy;
  ClaimPolicy<WaitPolicy> claim_policy(buffer_size, wait_policy);
  nats::SeqBarrier<WaitPolicy> consumed(wait_policy);
  claim_policy.add_claim_barrier(consumed);
  nats::RingBuffer<nats::storage::Dynamic<size_t>> buffer(buffer_size);

  std::vector<std::jthread> producers;
  producers.reserve(producer_count);

  const size_t exp = producer_count * (iter_count * (iter_count - 1)) / 2;
  const auto start = std::chrono::high_resolution_clock::now();

  // producers
  for (size_t i = 0; i < producer_count; ++i) {
    producers.emplace_back([&] {
      for (size_t i = 0; i < iter_count; ++i) {
        const auto seq = claim_policy.claim_one();
        buffer[seq] = i;
        claim_policy.publish(seq);
      }
    });
  }
  // consumer
  size_t sum = 0;
  nats::Seq_t next_to_read = 0;
  size_t items_remaining = iter_count * producer_count;
  while (items_remaining > 0) {
    const auto available =
        claim_policy.wait_until_published(next_to_read, next_to_read - 1);
    do {
      sum += buffer[next_to_read];
      --items_remaining;
    } while (next_to_read++ != available);
  }
}

int main() {}
