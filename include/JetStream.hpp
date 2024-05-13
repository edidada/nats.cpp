// SPDX-License-Identifier: MIT
#pragma once

#include "nats.hpp"

namespace jetstream {

using nats::Error;
using nats::expected;
using nats::Header;

struct ConsumeContext {};

struct MessagesContext {};

struct Msg {};

struct ConsumerInfo {};

struct Consumer {
  expected<ConsumeContext, Error> consume(auto handler);
  expected<MessagesContext, Error> messages(auto... pull_messages_opt);
  expected<Msg, Error> next(auto &&...fetch_opt);
  expected<ConsumerInfo, Error> info(auto context);
  ConsumerInfo cached_info();
};

struct StreamInfo {};

struct RawStreamMsg {
  std::string subject;
  uint64_t sequence;
  Header header;
  std::span<char> data;
  uint64_t time;
};

struct ConsumerConfig {};

using ConsumerNameLister = std::vector<std::string>;

struct Stream {
  expected<StreamInfo, Error> info(auto context, auto &&...stream_info_opt);
  StreamInfo cached_info();
  Error purge(auto ctx, auto &&...stream_purge_opt);
  expected<RawStreamMsg, Error> get_msg(auto ctx, uint64_t seq,
                                        auto &&...get_msg_opt);
  expected<RawStreamMsg, Error>
  get_last_msg_for_subject(auto ctx, std::string_view subject);
  Error delete_msg(auto ctx, uint64_t seq);
  Error secure_delete_msg(auto ctx, uint64_t seq);

  // ConsumerManager
  expected<Consumer, Error> creat_or_update_consumer(auto ctx,
                                                     ConsumerConfig const &cfg);
  expected<Consumer, Error> creat_consumer(auto ctx, ConsumerConfig const &cfg);
  expected<Consumer, Error> update_consumer(auto ctx,
                                            ConsumerConfig const &cfg);
  expected<Consumer, Error> ordered_consumer(auto ctx,
                                             ConsumerConfig const &cfg);
  expected<Consumer, Error> consumer(std::string_view);
  Error delete_consumer(auto ctx, std::string_view consumer);
  Error list_consumers(auto ctx);
  ConsumerNameLister consumer_names(auto ctx);
};

struct Context {
  expected<Stream, Error> stream(std::string_view);
};

template <typename Connection> //
expected<Context, Error> create(Connection &c);

} // namespace jetstream
