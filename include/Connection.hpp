// SPDX-License-Identifier: MIT
#pragma once

#include <charconv>
#include <exception>
#include <functional>
#include <mutex>
#include <span>
#include <unordered_map>

#include <asio.hpp>

#include "types.hpp"

namespace nats {

template <typename... Args>
std::string format(std::string_view fmt, Args &&...args) {
  char buf[1024];
  sprintf(buf, fmt.data(), args...);
  return buf;
}

struct ConnectConfig {
  std::string addr;
  std::string port;

  bool verbose{false};
  bool pedantic{false};

  std::string user;
  std::string password;
  std::string auth_token;

  std::string to_string() const {
    constexpr std::string_view name = "nats-cpp";
    constexpr std::string_view lang = "cpp";
    constexpr std::string_view version = "1.0.0";

    std::ostringstream ss;
    ss << std::boolalpha << "CONNECT {"  //
       << "\"verbose\":" << verbose      //
       << ",\"pedantic\":" << pedantic   //
       << ",\"name\":\"" << name << "\"" //
       << ",\"lang\":\"" << lang << "\"" //
       << ",\"version\":\"" << version << "\"";
    if (not user.empty())
      ss << ",\"user\":\"" << user << "\"";
    if (not password.empty())
      ss << ",\"password\":\"" << password << "\"";
    if (not auth_token.empty())
      ss << ",\"auth_token\":\"" << auth_token << "\"";
    ss << "}\r\n";
    return ss.str();
  }
};

using asio::io_context;
using asio::ip::tcp;

template <typename Stream> //
class Connection {
  struct Subscription {
    using on_message = std::function< //
        void(std::string_view subject, std::string_view reply_to,
             std::string &&data)>;

    uint64_t sid;
    on_message callback;
  };

public:
  Connection() = default;
  Connection(Connection const &) = delete;
  auto operator=(Connection const &) = delete;
  Connection(Connection &&other)
      : stream_{std::exchange(other.stream_, {})}, subs_mutex_{},
        subs_{std::exchange(subs_, {})},
        stop_{std::exchange(other.connected_, {})},
        connected_{std::exchange(other.connected_, {})},
        sid_{std::exchange(other.sid_, {})} {}
  Connection &operator=(Connection &&) = delete;
  ~Connection() { close(); }

  std::function<Error(void)> on_ping = [this] {
    stream_ << "PONG\r\n";
    stream_.flush();
    return Error{};
  };
  std::function<Error(void)> on_pong;
  std::function<Error(void)> on_ok;
  std::function<Error(std::string_view)> on_info;
  std::function<Error(Error)> on_error;
  std::function<void(Connection &)> on_connected;

  Stream &stream() { return stream_; }

  Error publish(std::string_view subject, std::string_view reply_to,
                std::span<char const> data) {
    if (not connected_)
      return {"not connected"};

    stream_ << "PUB " << subject;
    if (not reply_to.empty())
      stream_ << " " << reply_to;
    stream_ << " " << data.data() << "\r\n";
    stream_.flush();
    return {};
  }

  Error unsubscribe(uint64_t subs_id) {
    {
      auto _ = std::lock_guard{subs_mutex_};
      if (auto find = subs_.find(subs_id); find == subs_.end())
        return {format("subscription not found: %s", subs_id)};
      else
        subs_.erase(find);
    }

    stream_ << "UNSUB " << subs_id << "\r\n";
    return {};
  }

  expected<uint64_t, Error>
  subscribe(std::string_view subject, std::string_view queue,
            typename Subscription::on_message const &callback) {
    auto sid = next_sid();
    stream_ << "SUB " << subject;
    if (not queue.empty())
      stream_ << " " << queue;
    stream_ << " " << sid << "\r\n";

    {
      auto _ = std::lock_guard{subs_mutex_};
      subs_.insert({sid, Subscription{.sid = sid, .callback = callback}});
    }
    return sid;
  }

  Error run(std::string_view port, std::string_view addr = "localhost") {
    auto cfg = ConnectConfig{
        .addr = addr.data(),
        .port = port.data(),
        .user = "",
        .password = "",
        .auth_token = "",
    };
    return run(cfg);
  }

  void close() {
    stop_ = true;
    if constexpr (requires { stream_.close(); })
      stream_.close();
  }

  Error run(ConnectConfig const &cfg) {
    for (;;) {
      if (stop_)
        return {};

      if (not connected_) {
        if (auto ec = connect(cfg); ec.has_error()) {
          std::this_thread::sleep_for(std::chrono::seconds(3));
          continue;
        }
        connected_ = true;

        if (on_connected)
          on_connected(*this);
      }

      if (std::string header; std::getline(stream_, header))
        on_header(header);
      else {
        // TODO handle error;
      }
    }
  }

  Error connect(ConnectConfig const &cfg) {
    if constexpr (requires { stream_.connect(cfg.addr, cfg.port); })
      stream_.connect(cfg.addr, cfg.port);
    stream_ << cfg.to_string();
    stream_.flush();
    return {};
  }

private:
  Error on_message(std::string_view subject, std::string_view sid_str,
                   std::string_view reply_to, size_t n) {
    using namespace std::string_literals;

    size_t sid = 0;
    auto [_, ec] = std::from_chars(sid_str.begin(), sid_str.end(), sid);
    if (ec != std::errc{})
      return {"can't parse sid"};

    Subscription sub;
    {
      auto _ = std::lock_guard{subs_mutex_};
      auto find = subs_.find(sid);
      if (find == subs_.end())
        return {"subscription not found on topic: "s + subject.data()};
      sub = find->second;
    }

    if (std::string buf(n, '\0'); stream_.read(buf.data(), n))
      sub.callback(subject, reply_to, buf.data());

    return {};
  }

  Error on_header(std::string const &header) {
    auto split = [](std::string_view str,
                    std::string_view delims) -> std::vector<std::string_view> {
      std::vector<std::string_view> output;
      output.reserve(str.size() / 2);
      for (auto first = str.data(), second = str.data(),
                last = first + str.size();
           second != last && first != last; first = second + 1) {
        second = std::find_first_of(first, last, std::cbegin(delims),
                                    std::cend(delims));
        if (first != second) {
          output.emplace_back(first, second - first);
        }
      }

      return output;
    };

    auto p = header.find_first_of(" ");

    if (p == std::string_view::npos) {
      if (header.size() != 4 and header.size() != 3)
        return {"protocol vialation from server"};
      p = header.size();
    }

    auto cmd = header.substr(0, p);

    if (cmd == "MSG") {
      ++p; // space
      auto info = header.substr(p, header.size() - p);
      auto results = split(info, " ");
      auto size = results.size();
      if (size < 3 or size > 4)
        return {"unexpected message format"};
      bool reply_to = size == 4;
      auto bytes_n_str = reply_to ? results[3] : results[2];

      size_t bytes_n = 0;
      if (auto res =
              std::from_chars(bytes_n_str.begin(), bytes_n_str.end(), bytes_n);
          res.ec != std::errc{}) {
        return {nats::format("can't parse int in headers: %d", bytes_n_str)};
      }

      return reply_to ? on_message(results[0], results[1], results[2], bytes_n)
                      : on_message(results[0], results[1], "", bytes_n);
    } else if (cmd == "INFO") {
      ++p; // space
      return on_info ? on_info(header.substr(p, header.size() - p)) : Error{};
    } else if (cmd == "PING") {
      return on_ping ? on_ping() : Error{};
    } else if (cmd == "PONG") {
      return on_pong ? on_pong() : Error{};
    } else if (cmd == "OK") {
      return on_ok ? on_ok() : Error{};
    } else if (cmd == "-ERR") {
      ++p; // space
      return Error(header.substr(p, header.size() - p));
    } else {
      return {format("unknown message type: %s", cmd.data())};
    }
  }

  uint64_t next_sid() { return sid_++; }

private:
  Stream stream_;
  std::mutex subs_mutex_;
  std::unordered_map<uint64_t, Subscription> subs_{};
  bool stop_{false};
  bool connected_{false};
  uint64_t sid_{0};
};

using TcpConnection = Connection<tcp::iostream>;

inline auto run(TcpConnection &conn, std::string_view port = "4222",
                std::string_view addr = "localhost") {
  return std::thread([&conn](std::string_view port,
                             std::string_view addr) { conn.run(port, addr); },
                     port, addr);
}

template <typename Stream>
expected<Connection<Stream>, Error> connect(std::string_view url) {}

} // namespace nats
