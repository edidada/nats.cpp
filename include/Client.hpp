// Copyright (C) 2024-2028 Jun Zhang and nats.cpp contributors
// SPDX-License-Identifier: MIT
#pragma once
#include <Common.hpp>

#include <cstdint>
#include <optional>
#include <string>

namespace nats {

// ------------------------------
// Srv
// a helper to hold server state
// ------------------------------
struct Srv {
  std::string uri;
  uint32_t reconnects;
  std::optional<double> last_attempt;
  bool did_connect;
  bool discovered;
  optional_str tls_name;
  optional_str server_version;
};

struct ServerVersion {
  std::optional<int> major;
  std::optional<int> minor;
  std::optional<int> patch;
  optional_str dev;
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

  promise_t<void> connect();
  promise_t<void> close();
  promise_t<void> drain();

  promise_t<void> publish(str_view subject, buf_view_t payload, str_view reply,
                          Header const &headers = {});

  promise_t<Subscription> subscribe(str_view subject, str_view queue
                                    // other options
  );

  promise_t<Msg> request(str_view subject, buf_view_t payload = {},
                         timeout_t = 500ms, bool old_style = false,
                         Header const &headers = {});

  // create a unique inbox used for requests/subscriptions
  std::string newInbox();

  promise_t<void> flush(timeout_t timeout = default_::FlushTimeout);

  JetStreamContext jetstream();
  JetStreamManager jsm();
};

} // namespace nats
