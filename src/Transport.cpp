// Copyright (C) 2024-2028 Jun Zhang and nats.cpp contributors
// SPDX-License-Identifier: MIT
#include <Transport.hpp>

#include <boost/asio.hpp>
#include <boost/asio/use_awaitable.hpp>

namespace nats::transport {

promise_t<void> Tcp::connect(ParseResult const &uri, size_t buf_size,
                             duration_t timeout) {}

promise_t<void> Tcp::connectTls(std::variant<std::string, ParseResult> uri,
                                ssl_context_t &ssl_context, size_t buffer_size,
                                duration_t timeout) {}

promise_t<void> Tcp::write(buf_view_t payload) {
  co_await asio::async_write(stream_, asio::buffer(payload),
                             asio::use_awaitable);
}

void Tcp::writelines(std::vector<buf_view_t> const &payload) {}

buf_t Tcp::read(size_t buf_size) {}
buf_t Tcp::readline() {}

void Tcp::drain() {}
void Tcp::waitClosed() {}
void Tcp::close() {}
bool Tcp::atEof() {}

bool Tcp::connected() {}

} // namespace nats::transport
