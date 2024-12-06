// Copyright (C) 2024-2028 Jun Zhang and nats.cpp contributors
// SPDX-License-Identifier: MIT
#include <Client.hpp>
#include <Transport.hpp>

#include <boost/asio/steady_timer.hpp>

#include <print>
#include <random>

namespace nats {

void setupServerPool(std::vector<std::string> const &servers);
void setupNkeysConnect();

std::unique_ptr<Transport> makeTransport(Server::Uri const &uri) {
  if (uri.scheme == "ws" or uri.scheme == "wss")
    return std::make_unique<transport::WebSocket>();
  else
    return std::make_unique<transport::Tcp>();
}

// look up in the pool for an available server and try to connect
promise_t<ServerPool::iterator> ServerPool::selectNextServer(iterator curr) {
  while (not servers.empty()) {
    // if too many reconnect fails, we consider the server dead, so discard it
    // else we believe current server is temporarily sleepy, so move to next
    if (max_reconnect_attempts > 0 and
        curr->reconnects > max_reconnect_attempts)
      curr = servers.erase(curr); // returned value is curr->next
    else
      ++curr;

    // backoff connecting if we attempted recently
    if (curr->last_attempt) {
      auto now = clock_t::now();
      auto reconnect_at = curr->last_attempt.value() + reconnect_time_wait;
      if (now < reconnect_at) {
        asio::steady_timer timer{co_await asio::this_coro::executor,
                                 reconnect_at};
        co_await timer.async_wait(cobalt::use_op);
      }
    }

    curr->last_attempt = clock_t::now();
    if (not transport)
      transport = makeTransport(curr->uri);
    if (curr->uri.scheme == "wss")
      co_await transport->connectTls();
    else
      co_await transport->connect(curr->uri, default_::BufSize,
                                  connect_timeout);
    co_return curr;
  };

  co_return end();
}

auto ServerPool::pickServer() -> std::expected<iterator, Error> {
  if (servers.empty())
    return std::unexpected{err::NoServers};
  return servers.begin();
}

Error ServerPool::setup() {
  srv_pool.reserve(defaults::srv_pool_size);
  urls.reserve(defaults::srv_pool_size);

  // create srv objects from each url string in opts.servers
  // and ad them to the pool.
  for (auto const &url : opts.servers) {
    if (auto err = add_url_to_pool(url, false, false); err.has_error())
      return err;
  }

  if (not opts.no_randomize)
    shuffle_pool(0);

  if (opts.url != proto::_EMPTY_) {
    if (auto err = add_url_to_pool(opts.url, false, false); err.has_error())
      return err;
    // opts.url is tried first
    if (not srv_pool.empty())
      std::swap(srv_pool[0], srv_pool[srv_pool.size() - 1]);
  } else if (srv_pool.empty()) {
    // place default url is pool is empty
    if (auto err = add_url_to_pool(defaults::URL, false, false);
        err.has_error())
      return err;
  }

  // check for scheme hint to move to TLS mode
  for (auto srv : srv_pool) {
    if (srv->url->scheme == tls::Scheme or srv->url->scheme == ws::SchemeTLS) {
      opts.secure = true;
      if (not opts.tls_config.has_value())
        opts.tls_config = tls::Config{.min_version = tls::VersionTLS12};
    }
  }
  return pick_server();
}

std::string Conn::conn_scheme() {
  if (ws)
    return opts.secure ? ws::SchemeTLS : ws::Scheme;
  return opts.secure ? tls::Scheme : "nats";
}

bool Conn::host_is_ip(url::Url const &u) {
  return not net::parse_ip(u.hostname()).empty();
}

Error Conn::add_url_to_pool(std::string_view surl, bool implicit,
                            bool save_tls_name) {}

// -------
// Client
// -------
promise_t<void> defaultOnErr(Client &, Error &e) {
  std::println("nats: error encountered - {}", e.what());
  co_return;
}

promise_t<void> Client::connect(Options const &opts) {
  opts_ = opts;
  setupServerPool(opts_.servers);
  opts_.cb.err =
      opts.cb.err ? opts.cb.err : ConnectCallbacks::Err(defaultOnErr);

  bool server_auth_configured = rng::any_of(server_pool_, [](Server const &s) {
    return s.uri.username.size() > 0 or s.uri.password.size() > 0;
  });
  opts_.auth_configured = opts_.user.size() > 0 or opts_.password.size() > 0 or
                          opts_.token.size() > 0 or server_auth_configured;

  if (opts_.user_credentials or opts_.nkeys_seed.size() > 0 or
      opts_.nkeys_seed_str.size() > 0) {
    opts_.auth_configured = true;
    setupNkeysConnect();
  }

  // queue used to trigger flushes to the socket
  // TODO: flush_queue = channel_t<byte>{opts_.flusher_queue_size};
  // max size of buffer used for flushing cmds to the server
  // opts_.pending_size;

  if (not opts_.no_randomize) {
    std::random_device rd;
    std::mt19937 gen{rd()};
    rng::shuffle(server_pool_, gen);
  }

  co_return;
}

promise_t<void> Client::close() {}

} // namespace nats
