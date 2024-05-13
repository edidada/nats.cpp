#include <algorithm>
#include <iostream>
#include <sstream>

#include "conn.hpp"
#include "types.hpp"
#include "ws.hpp"

namespace nats {

std::vector<std::string> process_url_string(std::string_view url) {
  auto urls = strings::split(url, ",");
  auto j = 0;
  for (auto s : urls) {
    auto u = strings::trim_spaces(s);
    if (u.size() > 0) {
      urls[j] = u;
      j++;
    }
  }
  return {urls.begin(), urls.begin() + j};
}

expected<std::unique_ptr<Conn>, Error>
connect(std::string_view url, std::function<Error(Options &)> opt) {
  auto opts = get_default_options();
  opts.servers = process_url_string(url);
  auto err = opt(opts);
  if (err.has_error())
    return unexpected(err);
  return opts.connect();
}

void default_err_handler(spy<Conn> nc, spy<Subscription> sub, Error &err) {
  uint64_t cid;
  if (nc) {
    auto _ = std::lock_guard{nc->mu};
    cid = nc->info.cid;
  }
  std::ostringstream err_str;
  if (sub) {
    std::string subject;
    {
      auto _ = std::lock_guard{sub->mu};
      subject = sub->jsi ? sub->jsi->psubj : sub->subject;
    }
    err_str << err.what() << " on connection [" << cid
            << "] for subscription on " << subject << "\n";
  } else {
    err_str << err.what() << " on connection [" << cid << "]\n";
  }
  std::cerr << err_str.str() << std::endl;
}

expected<std::unique_ptr<Conn>, Error> Options::connect() {
  auto nc = std::make_unique<Conn>();
  nc->opts = *this;
  auto &opts = nc->opts;

  using namespace std::chrono_literals;

  if (opts.max_pings_out == 0)
    opts.max_pings_out = defaults::MaxPingOut;

  if (opts.sub_chan_len == 0)
    opts.sub_chan_len = defaults::MaxChanLen;

  if (opts.reconnect_buf_size == 0)
    opts.reconnect_buf_size = defaults::ReconnectBufSize;

  if (opts.timeout == 0s)
    opts.timeout = defaults::Timeout;

  if (opts.user_jwt and not opts.nkey.empty())
    return unexpected(Err::NkeyAndUser);

  if (not opts.nkey.empty() and opts.signature_cb)
    return unexpected(Err::NkeyButNoSigCB);

  if (not opts.dialer)
    opts.dialer = std::make_shared<net::Dialer>(opts.timeout);

  if (opts.tls_handshake_first)
    opts.secure = true;

  if (auto err = nc->setup_server_pool(); err.has_error())
    return unexpected(err);

  nc->ach = std::make_unique<asyncCallbacksHandler>();

  if (not opts.async_error_cb)
    opts.async_error_cb = default_err_handler;

  nc->new_reader_writer();

  auto connection_established = nc->connect();
  if (not connection_established.has_value())
    return unexpected(connection_established.error());

  go([&nc] { nc->ach->async_cb_dispatcher(); });

  if (connection_established.value() and opts.connected_cb)
    nc->ach->push([pnc = nc.get()] { pnc->opts.connected_cb(pnc); });

  return nc;
}

// ---------------------------------------------------
// Conn

std::tuple<int, spy<srv>> Conn::current_server() {
  auto find = std::ranges::find(srv_pool, current);
  if (current == nullptr or find == srv_pool.end())
    return {-1, nullptr};
  else
    return {find - srv_pool.begin(), *find};
}

expected<spy<srv>, Error> Conn::select_next_server() {
  auto [i, s] = current_server();
  if (i < 0)
    return unexpected(Err::NoServers);
  srv_pool.erase(srv_pool.begin() + i);
  auto max_reconnect = opts.max_reconnect;
  if (max_reconnect < 0 or s->reconnects < max_reconnect)
    srv_pool.push_back(s);
  if (srv_pool.empty()) {
    current = nullptr;
    return unexpected(Err::NoServers);
  }
  current = srv_pool[0];
  return current;
}

Error Conn::pick_server() {
  current = nullptr;
  if (srv_pool.empty())
    return Err::NoServers;
  for (auto s : srv_pool) {
    if (s != nullptr) {
      current = s;
      return {};
    }
  }
  return Err::NoServers;
}

Error Conn::setup_server_pool() {
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
} // namespace nats
