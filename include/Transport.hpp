// Copyright (C) 2024-2028 Jun Zhang and nats.cpp contributors
// SPDX-License-Identifier: MIT
#pragma once
#include <Common.hpp>
#include <Protocol.hpp>

#include <boost/asio/basic_socket.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/websocket/stream.hpp>

namespace nats {

namespace beast = boost::beast;
using asio::ip::tcp;
using ssl_context_t = asio::ssl::context;

struct ParseResult;

// ----------
// Transport
// ----------
struct Transport {
  virtual ~Transport() = default;
  virtual promise_t<void> connect(ParseResult const &uri, size_t buf_size,
                                  duration_t timeout) = 0;

  virtual promise_t<void> connectTls(std::variant<std::string, ParseResult> uri,
                                     ssl_context_t &ssl_context,
                                     size_t buffer_size,
                                     duration_t timeout) = 0;

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

namespace transport {

namespace ssl = asio::ssl;
using asio::ip::tcp;

using executor_t = cobalt::use_op_t::executor_with_default<cobalt::executor>;
using socket_t = tcp::socket::rebind_executor<executor_t>::other;
using ssl_socket_t = ssl::stream<socket_t>;
using acceptor_t = tcp::acceptor::rebind_executor<executor_t>::other;
using websocket_t = beast::websocket::stream<ssl_socket_t>;

struct Tcp : Transport {
  using stream_t = beast::tcp_stream;

  promise_t<void> connect(ParseResult const &uri, size_t buf_size,
                          duration_t timeout) override final;
  promise_t<void> connectTls(std::variant<std::string, ParseResult> uri,
                             ssl_context_t &ssl_context, size_t buffer_size,
                             duration_t timeout) override final;

  void write(buf_view_t payload) override final;
  void writelines(std::vector<buf_view_t> const &payload) override final;

  buf_t read(size_t buf_size) override final;
  buf_t readline() override final;

  void drain() override final;
  void waitClosed() override final;
  void close() override final;
  bool atEof() override final;
  bool connected() override final;

  stream_t stream_;
};

struct WebSocket : Transport {
  using stream_t = beast::websocket::stream<tcp::socket>;

  promise_t<void> connect(ParseResult const &uri, size_t buf_size,
                          duration_t timeout) override final {}
  promise_t<void> connectTls(std::variant<std::string, ParseResult> uri,
                             ssl_context_t &ssl_context, size_t buffer_size,
                             duration_t timeout) override final {}

  void write(buf_view_t payload) override final {}
  void writelines(std::vector<buf_view_t> const &payload) override final {}

  buf_t read(size_t buf_size) override final {}
  buf_t readline() override final {}

  void drain() override final {}
  void waitClosed() override final {}
  void close() override final {}
  bool atEof() override final {}

  bool connected() override final {}

  stream_t stream_;
};

} // namespace transport

} // namespace nats
