// Copyright (C) 2024-2028 Jun Zhang and nats.cpp contributors
// SPDX-License-Identifier: MIT
#pragma once
#include <Common.hpp>

#include <cstdint>
#include <list>
#include <optional>
#include <string>

namespace nats {

struct Transport;

// ------------------------------
// Server
// a helper to hold server state
// ------------------------------
struct Server {
  struct Uri {
    std::string scheme;
    std::string username;
    std::string password;
  };
  struct Version {
    std::optional<int> major;
    std::optional<int> minor;
    std::optional<int> patch;
    optional_str dev;
  };

  Uri uri;
  uint32_t reconnects;
  std::optional<timepoint_t> last_attempt;
  bool did_connect;
  bool discovered;
  optional_str tls_name;
  optional_str server_version;
};

struct ServerPool {
  using Servers_t = std::list<Server>;

  using iterator = Servers_t::iterator;
  using const_iterator = Servers_t::const_iterator;

  iterator end() noexcept { return servers.end(); }
  const_iterator end() const noexcept { return servers.end(); }

  std::expected<iterator, Error> pickServer();
  promise_t<iterator> selectNextServer(iterator curr);
  void setup();

  Servers_t servers;
  std::unique_ptr<Transport> transport;
  uint16_t max_reconnect_attempts;
  duration_t connect_timeout;
  duration_t reconnect_time_wait;
};

// -------
// Client
// -------
struct Client {
  enum struct Status {
    Disconnected = 0,
    Connected,
    Closed,
    Reconnecting,
    Connecting,
    DrainingSubs,
    DrainingPubs,
  };

  struct Options {
    std::vector<std::string> servers{default_::Url};
    std::string name{};
    ConnectCallbacks cb{};

    bool pedantic{};
    bool verbose{};
    bool allow_reconnect{};

    duration_t connect_timeout = default_::ConnectTimeout;
    duration_t reconnect_wait = default_::ReconnectWait;
    uint16_t max_reconnect = default_::MaxReconnect;
    duration_t ping_interval = default_::PingInterval;
    uint16_t max_outstanding_pings = default_::MaxOutstandingPings;
    bool no_randomize = false;
    uint16_t flusher_queue_size = default_::FlusherQueueSize;
    bool no_echo = false;

    spy<SslContext> tls;
    std::string tls_hostname{};
    bool tls_handshake_first = false;
    std::string user{};
    std::string password{};
    std::string token{};
    bool auth_configured = false;
    duration_t drain_timeout = default_::DrainTimeout;
    SslHandlers ssl_cb;

    std::optional<Credentials> user_credentials{};
    std::string nkeys_seed{};
    std::string nkeys_seed_str{};
    std::string inbox_prefix = default_::InboxPrefix;
    size_t pending_size = default_::PendingSize;
    duration_t flush_timeout = default_::FlushTimeout;
  };

  constexpr static std::string_view to_strv(Status status) {
    using enum Status;
    switch (status) {
    case Disconnected:
      return "DISCONNECTED";
    case Connected:
      return "CONNECTED";
    case Closed:
      return "CLOSED";
    case Reconnecting:
      return "RECONNECTING";
    case Connecting:
      return "CONNECTING";
    case DrainingSubs:
      return "DRAINING_SUBS";
    case DrainingPubs:
      return "DRAINING_PUBS";
    default:
      return "unknown status";
    };
  }

  promise_t<void> connect(Options const &opts);
  promise_t<void> close();
  promise_t<void> drain();

  promise_t<void> publish(str_view subject, buf_view_t payload, str_view reply,
                          Header const &headers = {});

  promise_t<Subscription> subscribe(str_view subject, str_view queue
                                    // other options
  );

  promise_t<Msg> request(str_view subject, buf_view_t payload = {},
                         duration_t = 500ms, bool old_style = false,
                         Header const &headers = {});

  // create a unique inbox used for requests/subscriptions
  std::string newInbox();

  promise_t<void> flush(duration_t timeout = default_::FlushTimeout);

  JetStreamContext jetstream();
  JetStreamManager jsm();

  std::tuple<int, Server &> current_server();

  Options opts_;
  std::vector<Server> server_pool_;
  channel_t<byte> flush_queue_;
};

} // namespace nats
