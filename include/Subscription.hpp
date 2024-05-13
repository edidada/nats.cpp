// Copyright (C) 2024-2028 Jun Zhang and nats.cpp contributors
// SPDX-License-Identifier: MIT
#pragma once
#include <Common.hpp>
#include <Msg.hpp>

#include <queue>

namespace nats {

// -------------
// Subscription
// -------------
struct Subscription {
  Conn conn;
  uint64_t sid;
  std::string subject;
  std::string queue;

  uint64_t max_msgs;
  uint64_t received;
  MsgHandler cb;
  bool closed;

  // per subscription message processor
  uint64_t pending_msgs_limit;
  uint64_t pending_bytes_limit;
  std::queue<Msg> pending_queue;
  uint64_t pending_size;

  // JSI

  // should only be available when callback is not provided
  promise_t<Msg> nextMsg(timeout_t timeout = 1s);
  generator_t<Msg> messages();

  // removes interest in a subject, but will process pending msgs
  promise_t<void> drain();
  promise_t<void> unsubscribe(size_t limit);
};

} // namespace nats
