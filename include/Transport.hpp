// Copyright (C) 2024-2028 Jun Zhang and nats.cpp contributors
// SPDX-License-Identifier: MIT
#pragma once
#include <Common.hpp>

namespace nats {

struct ParseResult;
struct SslContext;

// ----------
// Transport
// ----------
struct Transport {
  virtual promise_t<void> connect(ParseResult const &uri, size_t buf_size,
                                  timeout_t timeout) = 0;

  virtual promise_t<void> connectTls(std::variant<std::string, ParseResult> uri,
                                     SslContext &ssl_context,
                                     size_t buffer_size, timeout_t timeout) = 0;

  virtual void write(buf_view_t payload) = 0;
  virtual void writelines(std::vector<buf_view_t> const &payload) = 0;

  virtual buf_t read(size_t buf_size) = 0;
  virtual buf_t readline() = 0;

  virtual void drain() = 0;
  virtual void waitClosed() = 0;
  virtual void close() = 0;
  virtual bool atEof() = 0;

  virtual bool connected() = 0;
};

struct TcpTransport : Transport {
  promise_t<void> connect(ParseResult const &uri, size_t buf_size,
                          timeout_t timeout) override final {}
};

struct WebSocketTransport : Transport {};

} // namespace nats
