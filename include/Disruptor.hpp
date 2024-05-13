// Copyright (C) 2024-2028 Jun Zhang and nats.cpp contributors
// SPDX-License-Identifier: MIT
#pragma once
#include <Common.hpp>

#include <algorithm>
#include <atomic>
#include <cassert>
#include <chrono>
#include <concepts>
#include <memory>
#include <new>
#include <optional>
#include <ranges>
#include <semaphore>
#include <span>
#include <thread>

namespace nats {

namespace config {

inline constexpr size_t CacheLineSize =
    std::hardware_destructive_interference_size;
inline constexpr size_t PageSize = 4096;

} // namespace config

namespace rng = std::ranges;
namespace vws = std::views;
using Seq_t = size_t;
using SeqDiff_t = ssize_t;
using AtomicSeq_t = std::atomic<Seq_t>;

constexpr SeqDiff_t diff(Seq_t a, Seq_t b) noexcept {
  return static_cast<SeqDiff_t>(a - b);
}

// calculate the min(seqs), or the slowest running sequence
// the assumption is, even the fastest (max) runner,
// doesn't catch the slowest (min)
constexpr Seq_t min(std::span<const AtomicSeq_t *const> seqs) noexcept {
  pre(seqs.size() > 0);

  auto load = [](const AtomicSeq_t *const seq) -> Seq_t {
    return seq->load(std::memory_order::acquire);
  };

  auto seq_vals = vws::transform(seqs, load);
  return *rng::min_element(seq_vals);
}

// the slowest (minimum) sequence that running before min
// so if all seqs are > min, then equivalent to min(seqs)
// if min turns out to be running among seqs, return some seq < min
//
// in short: find someone running slower then me, if I'm the slowest, then
// find the slowest of all those before me
constexpr Seq_t minAfter(Seq_t min,
                         std::span<const AtomicSeq_t *const> seqs) noexcept {
  pre(seqs.size() > 0);
  SeqDiff_t min_delta = diff(seqs[0]->load(std::memory_order::acquire), min);
  for (size_t i = 1; i < seqs.size() and min_delta >= 0; ++i) {
    Seq_t seq = seqs[i]->load(std::memory_order::acquire);
    min_delta = std::min(min_delta, diff(seq, min));
  }
  return std::plus{}(min, min_delta); // avoid warning of mixed signedness
}

template <typename T>
concept WaitPolicy =
    requires(T t, Seq_t seq, std::span<const AtomicSeq_t *const> seqs) {
      t.waitUntilPublished(seq, seqs);
    };

namespace wait_policy {

struct Blocking {
  // block until all seqs catch up seq (min_seq of seqs > seq)
  Seq_t waitUntilPublished(Seq_t seq,
                           std::span<const AtomicSeq_t *const> seqs) {
    pre(not seqs.empty());
    // this is equivalent to conditional_variable+mutex,
    // but simpler and (hopefully) faster
    while (true) {
      Seq_t min_seq;
      {
        auto lock = std::lock_guard{mutex_};
        min_seq = minAfter(seq, seqs);
      }
      if (diff(min_seq, seq) >= 0)
        return min_seq;
      signal_.acquire();
    }
  }

  // wait for seqs to catch up, block for `dur` timeout, if timeout,
  // the return is min_after(seq, seqs), i.e. one of seqs that's before seq
  template <typename Rep, typename Period>
  Seq_t waitUntilPublished(Seq_t seq, std::span<const AtomicSeq_t *const> seqs,
                           std::chrono::duration<Rep, Period> dur) {
    pre(not seqs.empty());
    Seq_t min_seq;
    {
      auto lock = std::lock_guard{mutex_};
      min_seq = minAfter(seq, seqs);
    }
    if (diff(min_seq, seq) >= 0)
      return min_seq;
    signal_.try_acquire_for(dur);
    return min_seq;
  }

  std::binary_semaphore signal_{0};
  std::mutex mutex_{};
};

struct Spin {
  struct Spinner {
    Spinner() noexcept { reset(); }
    void reset() noexcept {
      value_ = std::thread::hardware_concurrency() > 1 ? 0 : 10;
    }
    // wait for a short time
    void spinOnce() {
      if (nextSpinWillYield()) {
        if (uint32_t count = value_ - 10; count % 20 == 19)
          std::this_thread::sleep_for(std::chrono::milliseconds(1));
        else
          std::this_thread::yield();
      } else {
        uint32_t count = uint32_t(4) << value_;
        while (count-- != 0)
          yieldProcessor();
      }
      value_ = value_ == 0xFFFF'FFFF ? 10 : value_ + 1;
    }

    bool nextSpinWillYield() const noexcept { return value_ >= 10; }
    static void yieldProcessor() noexcept {}

    uint32_t value_;
  };

  // wait until all seqs have at least published the specified seq value
  // or: (I'm running too fast, wait until all seqs surpass me)
  Seq_t waitUntilPublished(Seq_t seq,
                           std::span<const AtomicSeq_t *const> seqs) {
    pre(not seqs.empty());
    Spinner spinner;
    Seq_t min_seq = minAfter(seq, seqs);
    while (diff(min_seq, seq) < 0) {
      spinner.spinOnce();
      min_seq = minAfter(seq, seqs);
    }
    return min_seq;
  }

  template <typename Rep, typename Period>
  Seq_t waitUntilPublished(Seq_t seq, std::span<const AtomicSeq_t *const> seqs,
                           std::chrono::duration<Rep, Period> dur) {
    return waitUntilPublished(seq, seqs,
                              std::chrono::high_resolution_clock::now() + dur);
  }

  template <typename Clock, typename Duration>
  Seq_t waitUntilPublished(Seq_t seq, std::span<const AtomicSeq_t *const> seqs,
                           std::chrono::time_point<Clock, Duration> until) {
    pre(not seqs.empty());
    Spinner spinner;
    Seq_t min_seq = minAfter(seq, seqs);
    while (diff(min_seq, seq) < 0) {
      if (spinner.nextSpinWillYield() and until < Clock::now())
        return min_seq; // timeout
      spinner.spinOnce();
      min_seq = minAfter(seq, seqs);
    }
    return min_seq;
  }

  void signalAllWhenBlocking() {}
};

} // namespace wait_policy

// ---------
// SeqRange
// ---------
struct SeqRange {
  constexpr SeqRange() noexcept : first_{0}, size_{0} {}
  constexpr SeqRange(Seq_t first, size_t size) noexcept
      : first_{first}, size_{size} {}
  constexpr Seq_t first() const noexcept { return first_; }
  constexpr Seq_t last() const noexcept { return end() - 1; }

  constexpr Seq_t begin() const noexcept { return first(); }
  constexpr Seq_t end() const noexcept {
    return static_cast<Seq_t>(first_ + size_);
  }
  constexpr size_t size() const noexcept { return size_; }
  constexpr Seq_t operator[](size_t index) const noexcept {
    return static_cast<Seq_t>(first_ + index);
  }

  Seq_t first_;
  size_t size_;
};

// -----------
// SeqBarrier
// -----------
template <WaitPolicy WaitPolicy> //
struct SeqBarrierGroup;

template <typename WaitPolicy> //
struct SeqBarrier {
  constexpr SeqBarrier(WaitPolicy &wait_policy) noexcept
      : wait_policy_{wait_policy}, last_published_{static_cast<Seq_t>(-1)} {}

  constexpr Seq_t lastPublished() const noexcept {
    return last_published_.load(std::memory_order::acquire);
  }

  void publish(Seq_t seq) {
    last_published_.store(seq, std::memory_order::release);
    wait_policy_.signal_all_when_blocking();
  }

  // block the calling thread until the specified sequence number is published
  Seq_t waitUntilPublished(Seq_t seq) const {
    if (Seq_t curr = lastPublished(); diff(curr, seq) >= 0)
      return curr;
    return wait_policy_.waitUntilPublished(seq, {&last_published_, 1});
  }

  template <typename Rep, typename Period>
  Seq_t waitUntilPublished(Seq_t seq,
                           std::chrono::duration<Rep, Period> dur) const {
    if (Seq_t curr = lastPublished(); diff(curr, seq) >= 0)
      return curr;
    return wait_policy_.waitUntilPublished(seq, {&last_published_, 1}, dur);
  }

  WaitPolicy &wait_policy_;
  alignas(config::CacheLineSize) AtomicSeq_t last_published_;
};

// ----------------
// SeqBarrierGroup
// ----------------
template <WaitPolicy WaitPolicy> //
struct SeqBarrierGroup {
  SeqBarrierGroup(WaitPolicy &wait_policy) : wait_policy_{wait_policy} {}

  // add a seq barrier to the group
  void add(const SeqBarrier<WaitPolicy> &barrier) {
    pre(&barrier.wait_policy == &wait_policy_);
    seqs_.push_back(&barrier.last_published_);
  }
  void add(const SeqBarrierGroup<WaitPolicy> &barrier_group) {
    // append_range
    seqs_.insert(seqs_.end(), barrier_group.seqs_.begin(),
                 barrier_group.seqs_.end());
  }

  Seq_t lastPublished() const {
    pre(not seqs_.empty());
    return min(seqs_);
  }

  Seq_t waitUntilPublished(Seq_t seq) const {
    pre(not seqs_.empty());
    if (Seq_t curr = minAfter(seq, seqs_); diff(curr, seq) >= 0)
      return curr;
    return wait_policy_.waitUntilPublished(seq, seqs_);
  }

  template <typename Rep, typename Period>
  Seq_t waitUntilPublished(Seq_t seq,
                           std::chrono::duration<Rep, Period> dur) const {
    pre(not seqs_.empty());
    if (Seq_t curr = minAfter(seq, seqs_); diff(curr, seq) >= 0)
      return curr;
    return wait_policy_.waitUntilPublished(seq, seqs_, dur);
  }

  WaitPolicy &wait_policy_;
  std::vector<const AtomicSeq_t *> seqs_;
};

// ---------------
// claim policies
// ---------------
namespace claim_policy {

// SingleThreaded
template <WaitPolicy WaitPolicy> //
struct SingleThreaded {
  SingleThreaded(size_t buffer_size, WaitPolicy &wait_policy)
      : buffer_size_{buffer_size}, next_to_claim_{0},
        claim_barrier_{wait_policy}, read_barrier_{wait_policy} {
    pre(buffer_size > 0 and (buffer_size & (buffer_size - 1)) == 0);
  }

  size_t bufferSize() const noexcept { return buffer_size_; }
  void addClaimBarrier(SeqBarrier<WaitPolicy> &barrier) {
    claim_barrier_.add(barrier);
  }
  void addClaimBarrier(SeqBarrierGroup<WaitPolicy> &barrier) {
    claim_barrier_.add(barrier);
  }

  // claim a single slot in the ring buffer
  // blocks the caller until a slot is available
  Seq_t claimOne() {
    claim_barrier_.waitUntilPublished(
        static_cast<Seq_t>(next_to_claim_ - buffer_size_));
    return next_to_claim_++;
  }

  // claim up to `count` consecutive slots in the ring buffer
  // blocks the caller until at least one slot is available
  SeqRange claim(size_t count) {
    Seq_t claimable =
        static_cast<Seq_t>(claim_barrier_.waitUntilPublished(static_cast<Seq_t>(
                               next_to_claim_ - buffer_size_)) +
                           buffer_size_);
    SeqDiff_t df = diff(claimable, next_to_claim_);
    contract_assert(df >= 0);

    size_t available = static_cast<size_t>(df + 1);
    count = std::min(count, available);
    return SeqRange{next_to_claim_ += count, count};
  }

  // try claim up to `count` consecutive slots
  // if at least one slot is available, return the range
  std::optional<SeqRange> tryClaim(size_t count) {
    Seq_t seq =
        static_cast<Seq_t>(claim_barrier_.lastPublished() + buffer_size_);
    SeqDiff_t df = diff(seq, next_to_claim_);
    if (df < 0)
      return std::nullopt;
    size_t available = static_cast<size_t>(df + 1);
    count = std::min(count, available);
    return SeqRange{next_to_claim_ += count, count};
  }

  template <typename Rep, typename Period>
  std::optional<SeqRange> tryClaimFor(size_t count,
                                      std::chrono::duration<Rep, Period> dur) {
    if (auto range = tryClaim(count); range.has_value())
      return range;

    Seq_t claimable = static_cast<Seq_t>(
        claim_barrier_.waitUntilPublished(
            static_cast<Seq_t>(next_to_claim_ - buffer_size_), dur) +
        buffer_size_);
    return tryClaim(claimable, next_to_claim_);
  }

  void publish(Seq_t seq) { read_barrier_.publish(seq); }

  Seq_t lastPublished() const noexcept { return read_barrier_.lastPublished(); }

  // block the caller until the specified seq has been published
  // by the writer thread
  Seq_t waitUntilPublished(Seq_t seq) const {
    return read_barrier_.waitUntilPublished(seq);
  }
  Seq_t waitUntilPublished(Seq_t seq, Seq_t last_knonwn_published) const {
    return waitUntilPublished(seq);
  }
  template <typename Rep, typename Period>
  Seq_t waitUntilPublished(Seq_t seq,
                           std::chrono::duration<Rep, Period> dur) const {
    return read_barrier_.waitUntilPublished(seq, dur);
  }
  template <typename Rep, typename Period>
  Seq_t waitUntilPublished(Seq_t seq, Seq_t last_knonwn_published,
                           std::chrono::duration<Rep, Period> dur) const {
    return read_barrier_.waitUntilPublished(seq, dur);
  }

  const size_t buffer_size_;
  Seq_t next_to_claim_;
  Seq_t last_knonwn_claimable;
  SeqBarrierGroup<WaitPolicy> claim_barrier_;
  SeqBarrier<WaitPolicy> read_barrier_;
};

// MultiThreaded
template <WaitPolicy WaitPolicy> //
struct MultiThreaded {

  MultiThreaded(size_t buffer_size, WaitPolicy &wait_policy)
      : index_mask_{buffer_size - 1}, buffer_size_{buffer_size},
        wait_policy_{wait_policy}, claim_barrier_{wait_policy},
        published_{std::make_unique<AtomicSeq_t[]>(buffer_size)},
        next_claimable_{0} {
    // buffer_size must be power of 2
    pre(buffer_size_ > 0 and (buffer_size_ & (buffer_size_ - 1)) == 0);
    for (Seq_t i = 0; i < buffer_size_; ++i)
      published_[i].store(static_cast<Seq_t>(i - buffer_size_),
                          std::memory_order::release);
  }

  constexpr size_t bufferSize() const noexcept { return buffer_size_; }
  void addClaimBarrier(SeqBarrier<WaitPolicy> &barrier) {
    claim_barrier_.add(barrier);
  }
  void addClaimBarrier(SeqBarrierGroup<WaitPolicy> &barrier) {
    claim_barrier_.add(barrier);
  }

  // claim a single slot for writing
  Seq_t claimOne() {
    Seq_t seq = next_claimable_.fetch_add(1, std::memory_order::relaxed);
    claim_barrier_.waitUntilPublished(static_cast<Seq_t>(seq - buffer_size_));
    return seq;
  }

  // claim up to `count` consecutive slots for writing
  // block the caller until at least 1 items are available
  SeqRange claim(size_t count) {
    count = std::min(count, buffer_size_);
    Seq_t seq = next_claimable_.fetch_add(count, std::memory_order::relaxed);
    SeqRange range{seq, count};
    claim_barrier_.waitUntilPublished(
        static_cast<Seq_t>(range.last() - buffer_size_));
    return range;
  }

  std::optional<SeqRange> tryClaim(size_t count) {
    Seq_t published =
        static_cast<Seq_t>(claim_barrier_.lastPublished() + buffer_size_);
    Seq_t seq = next_claimable_.load(std::memory_order::relaxed);
    do {
      SeqDiff_t df = diff(published, seq);
      if (df < 0)
        return std::nullopt;
      count = std::min(count, static_cast<size_t>(df + 1));
    } while (not next_claimable_.compare_exchange_weak(
        seq, static_cast<Seq_t>(seq + count), std::memory_order::relaxed,
        std::memory_order::relaxed));

    return SeqRange{seq, count};
  }

  template <typename Rep, typename Period>
  std::optional<SeqRange> tryClaimFor(size_t count,
                                      std::chrono::duration<Rep, Period> dur) {
    return tryClaimUntil(count,
                         std::chrono::high_resolution_clock::now() + dur);
  }

  template <typename Clock, typename Duration>
  std::optional<SeqRange>
  tryClaimUntil(size_t count, std::chrono::time_point<Clock, Duration> until) {
    Seq_t published =
        static_cast<Seq_t>(claim_barrier_.lastPublished() + buffer_size_);
    Seq_t seq = next_claimable_.load(std::memory_order::relaxed);
    size_t reduced_count;
    do {
      SeqDiff_t df = diff(published, seq);
      if (df < 0) {
        published = static_cast<Seq_t>(
            claim_barrier_.waitUntilPublished(
                static_cast<Seq_t>(seq - buffer_size_), until) +
            buffer_size_);
        df = diff(published, seq);
        if (df < 0)
          return std::nullopt; // timeout
      }
      reduced_count = std::min(count, static_cast<Seq_t>(df + 1));
    } while (not next_claimable_.compare_exchange_weak(
        seq, static_cast<Seq_t>(seq + reduced_count),
        std::memory_order::relaxed, std::memory_order::relaxed));

    return SeqRange{seq, reduced_count};
  }

  void publish(Seq_t seq) noexcept {
    _setPublished(seq);
    wait_policy_.signal_all_when_blocking();
  }

  void publish(const SeqRange &range) noexcept {
    for (size_t i = 0, j = range.size(); i < j; ++i)
      _setPublished(range[i]);
    wait_policy_.signal_all_when_blocking();
  }

  Seq_t lastPublishedAfter(Seq_t last_knonwn_published) const noexcept {
    Seq_t seq = last_knonwn_published + 1;
    while (_isPublished(seq))
      last_knonwn_published = seq++;
    return last_knonwn_published;
  }

  // block the caller until the specified seq has been published
  // called by reader threads waiting to consume items written to the ring
  // return: the seq of the latest available published seq, guaranteed to be
  // equal or later than the specified seq
  Seq_t waitUntilPublished(Seq_t seq, Seq_t last_knonwn_published) const {
    pre(diff(seq, last_knonwn_published) > 0);

    for (Seq_t curr = last_knonwn_published + 1; diff(curr, seq) <= 0; ++curr) {
      if (not _isPublished(curr))
        wait_policy_.waitUntilPublished(curr,
                                        {&published_[curr & index_mask_]});
    }
    return lastPublishedAfter(seq);
  }

  template <typename Rep, typename Period>
  Seq_t waitUntilPublished(Seq_t seq, Seq_t last_knonwn_published,
                           std::chrono::duration<Rep, Period> dur) const {
    return waitUntilPublished(seq, last_knonwn_published,
                              std::chrono::high_resolution_clock::now() + dur);
  }

  template <typename Clock, typename Duration>
  Seq_t
  waitUntilPublished(Seq_t seq, Seq_t last_knonwn_published,
                     std::chrono::time_point<Clock, Duration> until) const {
    pre(diff(seq, last_knonwn_published) > 0);

    for (Seq_t curr = last_knonwn_published + 1; diff(curr, seq) <= 0; ++curr) {
      if (not _isPublished(seq)) {
        Seq_t result = wait_policy_.waitUntilPublished(
            seq, {&published_[curr & index_mask_]}, until);
        if (diff(result, curr) < 0)
          return curr - 1; // timeout curr is the first not-published seq
      }
    }
    return lastPublishedAfter(seq);
  }

  // private
  bool _isPublished(Seq_t seq) const noexcept {
    return published_[seq & index_mask_].load(std::memory_order::acquire) ==
           seq;
  }
  void _setPublished(Seq_t seq) {
    auto &entry = published_[seq & index_mask_];
    contract_assert(entry.load(std::memory_order::relaxed) ==
                    static_cast<Seq_t>(seq - buffer_size_));
    entry.store(seq, std::memory_order::release);
  }

  const Seq_t index_mask_;
  const size_t buffer_size_;
  WaitPolicy &wait_policy_;
  SeqBarrierGroup<WaitPolicy> claim_barrier_;
  const std::unique_ptr<AtomicSeq_t[]> published_;
  alignas(config::CacheLineSize) AtomicSeq_t next_claimable_;
};

} // namespace claim_policy

template <typename T>
concept ClaimPolicy = true;

// -----------------
// storage policies
// -----------------
namespace storage {

template <typename T, size_t Size> //
struct Static {
  using value_type = T;
  using reference = T &;
  using const_reference = const T &;

  constexpr static size_t size = Size;
  constexpr static size_t mask = size - 1;
  static_assert(size > 0 and (size & mask) == 0);

  T data[size];
};

template <typename T, auto = 0> //
struct Dynamic {
  using value_type = T;
  using reference = T &;
  using const_reference = const T &;

  Dynamic(size_t size)
      : size{size}, mask{size - 1}, data{std::make_unique<T[]>(size)} {
    pre(size > 0 and (size & mask) == 0);
  }

  const size_t size;
  const size_t mask;
  std::unique_ptr<T[]> data;
};

} // namespace storage

template <typename T>
concept Storage = requires(T t) {
  true; // TODO
};

// -----------
// RingBuffer
// -----------
template <Storage Storage> //
struct RingBuffer {
  using const_reference = Storage::const_reference;
  using reference = Storage::reference;
  using value_type = Storage::value_type;

  RingBuffer(const RingBuffer &) = delete;
  auto operator=(const RingBuffer &) = delete;
  RingBuffer(RingBuffer &&) = default;
  RingBuffer &operator=(RingBuffer &&) = default;

  constexpr RingBuffer() : storage_{} {}
  constexpr RingBuffer(size_t size) : storage_{size} {}

  constexpr size_t size() const noexcept { return storage_.size; }
  constexpr reference operator[](Seq_t seq) {
    return storage_.data[seq & storage_.mask];
  }

  Storage storage_;
};

} // namespace nats
