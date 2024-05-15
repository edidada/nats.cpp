// Copyright (C) 2024-2028 Jun Zhang and nats.cpp contributors
// SPDX-License-Identifier: MIT
#pragma once
#include <Common.hpp>

#include <format>
#include <string>

namespace nats {

namespace op {

constexpr std::string INFO = "INFO";
constexpr std::string CONNECT = "CONNECT";
constexpr std::string MSG = "MSG";
constexpr std::string PING = "PING";
constexpr std::string PONG = "PONG";
constexpr std::string OK = "+OK";
constexpr std::string ERR = "-ERR";

constexpr std::string PUB = "PUB";
constexpr std::string HPUB = "HPUB";
constexpr std::string SUB = "SUB";
constexpr std::string UNSUB = "UNSUB";

} // namespace op

constexpr std::string MSG_END = "\n";
constexpr std::string CRLF = "\r\n";
constexpr std::string SPC = " ";

constexpr std::string OK = op::OK + CRLF;
constexpr std::string PING = op::PING + CRLF;
constexpr std::string PONG = op::PONG + CRLF;

using sid_t = size_t;

constexpr std::string pub_cmd(str_view subject, str_view reply,
                              str_view payload) {
  auto head = std::format("{} {} {} {}{}", //
                          op::PUB, subject, reply, payload.length(), CRLF);
  return std::format("{}{}{}", head, payload, CRLF);
}

constexpr std::string hpub_cmd(str_view subject, str_view reply,
                               std::string hdr, str_view payload) {
  auto hdr_len = hdr.length();
  auto total_size = payload.length() + hdr_len;
  auto head = std::format("{} {} {} {} {}{}", //
                          op::HPUB, subject, reply, hdr_len, total_size, CRLF);
  return std::format("{}{}{}{}", head, hdr, payload, CRLF);
}

constexpr std::string sub_cmd(str_view subject, str_view queue, sid_t sid) {
  return std::format("{} {} {} {}{}", op::SUB, subject, queue, sid, CRLF);
}

constexpr std::string unsub_cmd(sid_t sid, int max_msgs) {
  std::string max_msgs_str = max_msgs == 0 ? "" : std::to_string(max_msgs);
  return std::format("{} {} {}{}", op::UNSUB, sid, max_msgs_str, CRLF);
}

// this result is basically for `Msg` type of messages, which is the most
// frequent/important 'happy path', for other cases, most fields are empty,
// but those paths are relatively far fewer
struct ParseResult {
  enum Op { None, Msg, Info, Ping, Pong, Ok, Err };

  Op op{None};
  size_t bytes{};
  str_view subject{};
  str_view sid{};
  str_view reply_to{}; // optional
  str_view payload{};  // optional
  Header header{};     // optional

  template <typename T> T get(std::string const &field) {
    auto find = header.find(field);
    return find == header.end() ? T{} : std::get<T>(find->second);
  }
  template <typename T> T set(std::string const &field, T const &value) {
    header[field] = Value(value);
  }

  Error err() { return get<Error>(hdr::Error); }
};

struct Parser {
  constexpr static auto MaxControlLineSize = 4096;
  enum struct State {
    AwaitingControlLine = 1,
    AwaitingMsgPayload = 2,
  };

  void reset() {
    buf_.clear();
    state_ = State::AwaitingControlLine;
    needed_ = 0;
    header_needed_ = 0;
  }

  promise_t<ParseResult> parse(buf_view_t data);

  Header _parseInfoHeader(str_view sv);
  ParseResult _parseControlLine(buf_view_t data);
  ParseResult _parseMsgFields(std::vector<str_view> fields);
  void _processPayload(buf_view_t data);

  buf_t buf_;
  State state_;
  size_t needed_;
  size_t header_needed_;
};

} // namespace nats
