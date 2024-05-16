// Copyright (C) 2024-2028 Jun Zhang and nats.cpp contributors
// SPDX-License-Identifier: MIT
#include <Protocol.hpp>

#include <charconv>
#include <set>

namespace nats {

ParseResult _parseControlLine(buf_view_t data);
Header _parseInfoHeader(str_view sv);
ParseResult _parseMsgFields(std::vector<str_view> fields);
str_view _processPayload(buf_view_t data);

generator_t<ParseResult> parse(channel_t<buf_view_t> &chan) {
  enum struct Awaiting {
    ControlLine,
    MsgPayload,
  } state = Awaiting::ControlLine;
  buf_view_t input{};
  ParseResult result{};

  while (chan.is_open()) {
    switch (state) {
    case Awaiting::ControlLine:
      input = co_await chan.read();
      result = _parseControlLine(input);
      if (result.op == ParseResult::Msg)
        state = Awaiting::MsgPayload;
      if (state != Awaiting::MsgPayload)
        co_yield result;
      break;
    case Awaiting::MsgPayload:
      input = co_await chan.read();
      result.payload = _processPayload(input);
      co_yield result;
      state = Awaiting::ControlLine;
      result.reset();
      break;
    }
  }
  co_return ParseResult{.header = {{hdr::Error, "channel closed"}}};
}

// -----------
// to_hdr_val
// -----------
template <typename T> HeaderValue to_hdr_val(str_view value);
template <> HeaderValue to_hdr_val<bool>(str_view value) {
  return value == "true";
}
template <> HeaderValue to_hdr_val<int>(str_view value) {
  int val;
  std::from_chars(value.begin(), value.end(), val);
  return val;
}
template <> HeaderValue to_hdr_val<uint64_t>(str_view value) {
  uint64_t val;
  std::from_chars(value.begin(), value.end(), val);
  return val;
}
template <> HeaderValue to_hdr_val<std::string>(str_view value) {
  return std::string{value};
}
template <> HeaderValue to_hdr_val<std::vector<std::string>>(str_view value) {
  // 1. split by ','
  // 2. remove begin and end '"' from each segment
  // 3. to vector<string>
  return split(by_delim, value, ",") |
         vws::transform([](auto sv) { return sv.substr(1, sv.size() - 2); }) |
         rng::to<std::vector<std::string>>();
}

Error assign_hdr_val(Header &hdr, std::string const &field,
                     std::string const &value) {
  static auto str_fields = std::set{
      hdr::ServerId, hdr::ServerName, hdr::Version, hdr::Go,
      hdr::Host,     hdr::GitCommit,  hdr::IP,      hdr::ClientIP,
      hdr::Nonce,    hdr::Cluster,    hdr::Domain,
  };
  static auto int_fields = std::set{
      hdr::Port,
      hdr::MaxPayload,
      hdr::Proto,
  };
  static auto uint64_fields = std::set{hdr::ClientId};
  static auto bool_fields = std::set{
      hdr::Headers,      hdr::AuthRequired, hdr::TlsRequired, hdr::TlsVerify,
      hdr::TlsAvailable, hdr::Ldm,          hdr::JetStream,
  };
  static auto strlist_fields = std::set{hdr::ConnectUrls, hdr::WsConnectUrls};

  if (str_fields.contains(field))
    hdr[field] = to_hdr_val<std::string>(value);
  else if (int_fields.contains(field))
    hdr[field] = to_hdr_val<int>(value);
  else if (uint64_fields.contains(field))
    hdr[field] = to_hdr_val<uint64_t>(value);
  else if (bool_fields.contains(field))
    hdr[field] = to_hdr_val<bool>(value);
  else if (strlist_fields.contains(field))
    hdr[field] = to_hdr_val<std::vector<std::string>>(value);
  else
    return err::InvalidMsg;
  return Error::none();
}

str_view _processPayload(buf_view_t data) {
  auto end = data.find("\r\n");
  return data.substr(0, end);
}

// header parsing doesn't need to be blazingly fast
// we use easier implementation
Header _parseInfoHeader(str_view sv) {
  enum State { FieldName, List, Value };

  Header hdr;
  std::string field_name, value_str;
  State state = FieldName;
  str_view::size_type end{};

  while (sv.size() > 0) {
    switch (state) {
    case FieldName:
      end = sv.find_first_of(":");
      if (end == sv.npos or end == sv.size() - 1) // no value, error
        return {{hdr::Error, err::InvalidMsg}};
      field_name = trim(sv.substr(0, end), " \"");
      sv = sv.substr(end + 1); // skip ':'
      state = sv[0] == '[' ? List : Value;
      break;
    case Value:
      end = sv.find_first_of(",");
      value_str = trim(sv.substr(0, end), "\"");
      assign_hdr_val(hdr, field_name, value_str);
      if (end == sv.npos)
        return hdr;
      sv = sv.substr(end + 1);
      state = FieldName;
      break;
    case List:
      end = sv.find_first_of("]");
      if (end == sv.npos) // closing ']' not found, invalid
        return {{hdr::Error, err::InvalidMsg}};
      value_str = sv.substr(1, end - 1);
      assign_hdr_val(hdr, field_name, value_str);
      if (end == sv.size() - 1) // ']' is the end of header
        return hdr;
      sv = sv.substr(end + 2); // skip "],"
      state = FieldName;
      break;
    }
  }

  return hdr;
}

ParseResult _parseControlLine(buf_view_t data) {
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
    return {.op = ParseResult::Ping};
  } else if (cmd == op::PONG) {
    return {.op = ParseResult::Pong};
  } else if (cmd == op::OK) {
    return {.op = ParseResult::Ok};
  } else if (cmd == op::INFO) {
    return {
        .op = ParseResult::Info,
        .header = _parseInfoHeader(
            trim_back(trim_front(rest, "{ \t\r\n"), "} \t\r\n")),
    };
  } else if (cmd == op::ERR) {
    return {
        .op = ParseResult::Err,
        .header = {{hdr::Error, Error(std::string(trim(rest, " \t\r\n"sv)))}},
    };
  } else {
    return {.header = {{hdr::Error, err::InvalidMsg}}};
  }
}

ParseResult _parseMsgFields(std::vector<str_view> fields) {
  auto size = fields.size();
  if (size != 3 and size != 4)
    return {.header = {{hdr::Error, err::InvalidMsg}}};
  bool has_reply_to = size == 4;
  str_view reply_to = has_reply_to ? fields[2] : "";
  auto bytes_str = has_reply_to ? fields[3] : fields[2];
  size_t bytes = 0;
  if (auto res = std::from_chars(bytes_str.begin(), bytes_str.end(), bytes);
      res.ec != std::errc{})
    return {.header = {{hdr::Error, err::InvalidMsg}}};

  return {
      .op = ParseResult::Msg,
      .bytes = bytes,
      .subject = fields[0],
      .sid = fields[1],
      .reply_to = reply_to,
  };
}

} // namespace nats
