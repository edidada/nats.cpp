// Copyright (C) 2024-2028 Jun Zhang and nats.cpp contributors
// SPDX-License-Identifier: MIT
#pragma once
#include <Common.hpp>

#include <span>
#include <string>
#include <vector>

namespace nats {

namespace ack {

constexpr str_view Ack = "+ACK";
constexpr str_view Nak = "-NAK";
constexpr str_view Progress = "+WPI";
constexpr str_view Term = "+TERM";
// reply meta data
constexpr str_view Prefix0 = "$JS";
constexpr str_view Prefix1 = "ACK";
constexpr auto Domain = 2;
constexpr auto AccHash = 3;
constexpr auto Stream = 4;
constexpr auto Consumer = 5;
constexpr auto NumDelivered = 6;
constexpr auto StreamSeq = 7;
constexpr auto ConsumerSeq = 8;
constexpr auto Timestamp = 9;
constexpr auto NumPending = 10;

} // namespace ack

// ----------------
// Msg and friends
// ----------------
struct Msg {
  constexpr static str_view hdrLine = "NATS/1.0\r\n";
  constexpr static str_view crlf = "\r\n";
  constexpr static auto hdrPreEnd = hdrLine.size() - crlf.size();
  constexpr static auto statusHdr = "Status";
  constexpr static auto descrHdr = "Description";
  constexpr static auto lastConsumerSeqHdr = "Nats-Last-Consumer";
  constexpr static auto lastStreamSeqHdr = "Nats-Last-Stream";
  constexpr static auto consumerStalledHdr = "Nats-Consumer-Stalled";
  constexpr static auto noResponders = "503";
  constexpr static auto noMessagesSts = "404";
  constexpr static auto reqTimeoutSts = "408";
  constexpr static auto jetStream409Sts = "409";
  constexpr static auto controlMsg = "100";
  constexpr static auto statusLen = 3; // e.g. 20x, 40x, 50x

  std::string subject;
  std::string reply;
  Header header;
  std::span<byte> data;
  spy<Subscription> sub;
  // internal
  spy<Msg> next;
  int wsz;
  // spy<barrierInfo> barrier;
  uint32_t ackd;

  size_t size() {
    if (wsz)
      return wsz;

    auto hdr_bytes = header_bytes().value();
    return subject.size() + reply.size() + hdr_bytes.size() + data.size();
  }

  std::expected<std::vector<byte>, Error> header_bytes() {
    if (header.empty())
      return {};

    // auto b = bytes::Buffer{};
    // auto res = b.write_string(hdrLine);
    // if (not res.has_value())
    //   return std::unexpected(err::BadHeaderMsg);
    //
    // auto err = http::Header(header).write(b);
    // if (err.has_error())
    //   return std::unexpected(err::BadHeaderMsg);
    //
    // res = b.write_string(crlf);
    // if (not res.has_value())
    //   return std::unexpected(err::BadHeaderMsg);
    //
    // return b.bytes();
  }

  promise_t<void> respond(buf_view_t data);
  promise_t<void> ack();
  promise_t<Msg> ackSync(timeout_t timeout = 1s);
  promise_t<void> nak(timeout_t delay = 0ms);
  // acknowledges a message delivered by JetStream is still being worked on
  promise_t<void> inProgress();
  // terminates a message delivered by JetStream and disables redeliveries
  promise_t<void> term();
};

} // namespace nats
