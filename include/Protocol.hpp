// Copyright (C) 2024-2028 Jun Zhang and nats.cpp contributors
// SPDX-License-Identifier: MIT
#pragma once
#include <Common.hpp>

#include <any>
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

struct Parser {
  constexpr static auto MaxControlLineSize = 4096;
  enum struct State {
    AwaitingControlLine = 1,
    AwaitingMsgPayload = 2,
  };

  // this result is basically for `Msg` type of messages, which is the most
  // frequent/important 'happy path', for other cases, most fields are empty,
  // but those paths are relatively far fewer
  struct Result {
    enum Op { None, Msg, Info, Ping, Pong, Ok, Err };

    Op op{None};
    str_view subject{};
    str_view sid{};
    str_view reply_to{}; // optional
    size_t bytes{};
    str_view payload{}; // optional
    Error error{Error::none()};
  };

  void reset() {
    buf_.clear();
    state_ = State::AwaitingControlLine;
    needed_ = 0;
    header_needed_ = 0;
    msg_arg_.clear();
  }

  promise_t<Result> parse(buf_view_t data) {
    switch (state_) {
      using enum State;
    case AwaitingControlLine: {
      Result result = _parseControlLine(data);
      if (state_ == AwaitingMsgPayload)
        ; // async read payload and fill result
      break;
    }
    case AwaitingMsgPayload:
      break;
    }
  }

  Result _parseControlLine(buf_view_t data) {
    auto sv = str_view{data};
    auto control_line = sv.substr(0, sv.find(CRLF));
    // Nats protocol conventions ---
    // Field Delimiters: The fields of NATS protocol messages are delimited by
    // whitespace characters (space) or (tab). Multiple whitespace characters
    // will be treated as a single field delimiter.
    auto p = control_line.find_first_of(" \t");
    p = p == str_view::npos ? control_line.size() : p;

    if (auto cmd = control_line.substr(0, p), rest = control_line.substr(p);
        cmd == op::MSG) {
      return _parseMsgFields(split(by_delim, rest, " \t\r\n"sv));
    } else if (cmd == op::PING) {
      return {.op = Result::Ping};
    } else if (cmd == op::PONG) {
      return {.op = Result::Pong};
    } else if (cmd == op::OK) {
      return {.op = Result::Ok};
    } else if (cmd == op::INFO) {
      return {.op = Result::Info, .payload = trim(rest, " \t\r\n"sv)};
    } else if (cmd == op::ERR) {
      return {.op = Result::Err, .payload = trim(rest, " \t\r\n"sv)};
    } else {
      return {.error = err::InvalidMsg};
    }
  }

  Result _parseMsgFields(std::vector<str_view> fields) {
    auto size = fields.size();
    if (size != 3 and size != 4)
      return {.error = err::InvalidMsg};
    bool has_reply_to = size == 4;
    str_view reply_to = has_reply_to ? fields[2] : "";
    auto bytes_str = has_reply_to ? fields[3] : fields[2];
    size_t bytes = 0;
    if (auto res = std::from_chars(bytes_str.begin(), bytes_str.end(), bytes);
        res.ec != std::errc{})
      return {.error = err::InvalidMsg};

    state_ = State::AwaitingMsgPayload;
    return {
        .op = Result::Msg,
        .subject = fields[0],
        .sid = fields[1],
        .reply_to = reply_to,
        .bytes = bytes,
    };
  }

  void _processPayload(buf_view_t data) {}

  buf_t buf_;
  State state_;
  size_t needed_;
  size_t header_needed_;
  std::unordered_map<std::string, std::any> msg_arg_;
};

} // namespace nats
