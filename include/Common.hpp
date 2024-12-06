// Copyright (C) 2024-2028 Jun Zhang and nats.cpp contributors
// SPDX-License-Identifier: MIT
#pragma once
#include <boost/cobalt/channel.hpp>
#include <boost/cobalt/generator.hpp>
#include <boost/cobalt/promise.hpp>
#include <boost/cobalt/task.hpp>
#include <icecream.hpp>

#include <chrono>
#include <cstdint>
#include <expected>
#include <functional>
#include <map>
#include <ranges>
#include <source_location>
#include <stacktrace>
#include <string>
#include <unordered_map>
#include <vector>

namespace nats {

namespace rng = std::ranges;
namespace vws = std::views;
namespace asio = boost::asio;
namespace cobalt = boost::cobalt;

using namespace std::literals;

// ------------------------------
// typedefs and forward declares
// ------------------------------
template <typename T> using own = T *;
template <typename T> using spy = T *;

using byte = char;
using optional_str = std::optional<std::string>;
using buf_view_t = std::string_view;
using str_view = std::string_view;
using buf_t = std::vector<byte>;

using clock_t = std::chrono::steady_clock;
using duration_t = std::chrono::milliseconds;
using timepoint_t = std::chrono::time_point<clock_t>;

template <typename T> //
using channel_t = cobalt::channel<T>;

template <typename Return> //
using task_t = cobalt::task<Return>;

template <typename Return> //
using promise_t = cobalt::promise<Return>;

template <typename Yield, typename Push = void>
using generator_t = cobalt::generator<Yield, Push>;

struct JetStreamContext;
struct JetStreamManager;

struct Subscription;
struct Msg;
struct Conn;
struct Client;

using Credentials =
    std::variant<std::string, std::tuple<std::string, std::string>>;

using MsgHandler = std::function<void(spy<Msg>)>;

// ----------
// Contracts
// ----------
#ifdef NDEBUG
#define pre(x) [[assume(x)]]
#define contract_assert(x) [[assume(x)]]
// #define post(x) [[assume(x)]]
#else
#define pre(x) assert(x)
#define contract_assert(x) assert(x)
// #define post(x) assert(x)
#endif

// ---------------
// default values
// ---------------
namespace default_ {

constexpr auto Version = "1.0.0";
constexpr auto Url = "nats://127.0.0.1:4222";
constexpr auto Port = 4222;
constexpr auto MaxReconnect = 60;
constexpr auto ReconnectWait = 2s;
constexpr auto ReconnectJitter = 100ms;
constexpr auto ReconnectJitterTLS = 1s;
constexpr auto ConnectTimeout = 2s;
constexpr auto PingInterval = 2min;
constexpr auto MaxOutstandingPings = 2;
constexpr auto MaxQueueSize = 64 * 1024;           // 64k
constexpr auto ReconnectBufSize = 8 * 1024 * 1024; // 8MB
constexpr auto RequestQueueSize = 8;
constexpr auto DrainTimeout = 30s;
constexpr auto FlushTimeout = 1min;
constexpr auto LangString = "C++";
constexpr auto InboxPrefix = "_INBOX";
constexpr size_t PendingSize = 2 * 1024 * 1024; // 2MB

constexpr auto ScratchSize = 512;
constexpr auto BufSize = 32768;
constexpr auto FlusherQueueSize = 1;
constexpr auto SrvPoolSize = 4;
constexpr auto NuidSize = 22;
constexpr auto WsPortString = "80";
constexpr auto WssPortString = "443";
constexpr auto PortString = "4222";

} // namespace default_

// ----------
// utilities
// ----------

template <typename T>
concept StringLike = std::convertible_to<T, std::string> or
                     std::convertible_to<T, std::string_view>;

template <StringLike Str> //
constexpr std::decay_t<Str> trim_front(Str &&str, str_view spaces) {
  auto sv = str_view{str};
  auto begin = sv.find_first_not_of(spaces);
  begin = begin == sv.npos ? sv.size() : begin;
  return sv.substr(begin);
}

template <StringLike Str> //
constexpr std::decay_t<Str> trim_back(Str &&str, str_view spaces) {
  auto sv = str_view{str};
  auto reversed = vws::reverse(sv);
  auto non_space = [spaces](char c) { return not spaces.contains(c); };
  auto end = rng::find_if(reversed, non_space);
  return sv.substr(0, static_cast<size_t>(std::distance(end, reversed.end())));
}

template <StringLike Str> //
constexpr std::decay_t<Str> trim(Str &&str, str_view spaces) {
  return trim_back(trim_front(str, spaces), spaces);
}

constexpr struct ByDelims_t {
} by_delim;
constexpr struct ByPattern_t {
} by_pattern;

template <StringLike Str>
std::vector<std::decay_t<Str>> split(ByPattern_t, Str &&str, str_view pattern) {
  auto rng_to_str = [](auto const &r) {
    return std::decay_t<Str>{r.begin(), r.end()};
  };
  auto non_empty = [](auto const &r) { return not r.empty(); };
  return str | vws::split(pattern) | vws::transform(rng_to_str) |
         vws::filter(non_empty) | rng::to<std::vector>();
}

template <StringLike Str>
std::vector<std::decay_t<Str>> split(ByDelims_t, Str &&str, str_view delims) {
  enum State { Word, Delim };
  using Str_t = std::decay_t<Str>;

  std::vector<Str_t> result;
  auto begin = std::begin(str), end = std::end(str);
  if (begin == end or delims.size() == 0)
    return result;

  State state = delims.contains(*begin) ? Delim : Word;
  str_view::size_type word_start = 0, word_end = 0;

  // use a simple state machine, that helps tracking where we are
  for (auto subs = str_view{begin, end}; subs.size() > 0;) {
    switch (state) {
    case Word:
      word_end = subs.find_first_of(delims);
      if (word_start < word_end)
        result.emplace_back(subs.substr(0, word_end));
      if (word_end == subs.npos)
        return result;
      subs = subs.substr(word_end);
      state = Delim;
      break;
    case Delim:
      word_start = subs.find_first_not_of(delims);
      if (word_start == subs.npos)
        return result;
      subs = subs.substr(word_start);
      state = Word;
      word_start = 0;
      break;
    }
  }
  return result;
}

// ---------------
// Error handling
// ---------------
class Error {
public:
  enum Code : std::uint16_t {
    None = 0,
    Other,
    // ...
  };

  Error() : Error(None) {}
  Error(std::string const &what) : Error(Other, what) {}
  Error(Code code, std::string const &what = "") : code_{code}, what_{what} {}

  Error(Error const &) = default;
  Error(Error &&) = default;
  Error &operator=(Error const &) = default;
  Error &operator=(Error &&) = default;

  Code code() const noexcept { return code_; }
  // mutable, user can add information
  std::string &what() noexcept { return what_; }

  static Error const &none() {
    static Error none;
    return none;
  }

  bool operator==(Error const &other) const {
    return code_ == other.code_ and what_ == other.what_;
  }
  // for convenience in ordered containers
  bool operator<(Error const &other) const { return code_ < other.code_; }

private:
  Code code_{None};
  std::string what_{};
};

class Except : public Error {
public:
  Except(std::string const &what) : Error(what) {}
  Except(Code code, std::string const &what = "")
      : Error(code, what), where_{std::source_location::current()},
        who_{std::stacktrace::current()} {}

  Except(Code code, std::string const &what, std::source_location const &where,
         std::stacktrace const &who)
      : Error(code, what), where_{where}, who_{who} {}

  std::source_location const &where() const noexcept { return where_; }
  std::stacktrace const &who() const noexcept { return who_; }

private:
  std::source_location where_{};
  std::stacktrace who_{};
};

// ----- interop with std::expected -----
template <typename T>
auto unexpected(
    Error::Code code, T value,
    std::source_location const &where = std::source_location::current(),
    std::stacktrace const &who = std::stacktrace::current()) {
  return std::unexpected{
      Error(code, std::format("error: {}", value), where, who)};
}

inline void
panic(std::string const &what,
      std::source_location const &where = std::source_location::current(),
      std::stacktrace const &who = std::stacktrace::current()) {
  throw Except(Error::Other, what, where, who);
}

namespace err {

constexpr auto STALE_CONNECTION = "stale connection";
constexpr auto PERMISSIONS_ERR = "permissions violation";
constexpr auto AUTHORIZATION_ERR = "authorization violation";
constexpr auto AUTHORIZATION_EXPIRED_ERR = "user authorization expired";
constexpr auto AUTHORIZATION_REVOKED_ERR = "user authorization revoked";
constexpr auto ACCOUNT_AUTHORIZATION_EXPIRED_ERR =
    "account authorization expired";
constexpr auto MAX_CONNECTION_ERR = "maximum connections exceeded";

inline auto ConnectionClosed = Error("nats: connection closed");
inline auto ConnectionDraining = Error("nats: connection draining");
inline auto DrainTimeout = Error("nats: draining connection timed out");
inline auto ConnectionReconnecting = Error("nats: connection reconnecting");
inline auto SecureConnRequired = Error("nats: secure connection required");
inline auto SecureConnWanted = Error("nats: secure connection not available");
inline auto BadSubscription = Error("nats: invalid subscription");
inline auto TypeSubscription = Error("nats: invalid subscription type");
inline auto BadSubject = Error("nats: invalid subject");
inline auto BadQueueName = Error("nats: invalid queue name");
inline auto SlowConsumer = Error("nats: slow consumer, messages dropped");
inline auto Timeout = Error("nats: timeout");
inline auto BadTimeout = Error("nats: timeout invalid");
inline auto Authorization = Error("nats: authorization violation");
inline auto AuthExpired = Error("nats: authentication expired");
inline auto AuthRevoked = Error("nats: authentication revoked");
inline auto AccountAuthExpired = Error("nats: account authentication expired");
inline auto NoServers = Error("nats: no servers available for connection");
inline auto JsonParse = Error("nats: connect message, json parse error");
inline auto ChanArg = Error("nats: argument needs to be a channel type");
inline auto MaxPayload = Error("nats: maximum payload exceeded");
inline auto MaxMessages = Error("nats: maximum messages delivered");
inline auto SyncSubRequired =
    Error("nats: illegal call on an async subscription");
inline auto MultipleTLSConfigs =
    Error("nats: multiple tls.Configs not allowed");
inline auto ClientCertOrRootCAsRequired =
    Error("nats: at least one of certCB or rootCAsCB must be set");
inline auto NoInfoReceived =
    Error("nats: protocol exception, INFO not received");
inline auto ReconnectBufExceeded =
    Error("nats: outbound buffer limit exceeded");
inline auto InvalidConnection = Error("nats: invalid connection");
inline auto InvalidMsg = Error("nats: invalid message or message nil");
inline auto InvalidArg = Error("nats: invalid argument");
inline auto InvalidContext = Error("nats: invalid context");
inline auto NoDeadlineContext = Error("nats: context requires a deadline");
inline auto NoEchoNotSupported =
    Error("nats: no echo option not supported by this server");
inline auto ClientIDNotSupported =
    Error("nats: client ID not supported by this server");
inline auto UserButNoSigCB =
    Error("nats: user callback defined without a signature handler");
inline auto NkeyButNoSigCB =
    Error("nats: nkey defined without a signature handler");
inline auto NoUserCB = Error("nats: user callback not defined");
inline auto NkeyAndUser = Error("nats: user callback and nkey defined");
inline auto NkeysNotSupported =
    Error("nats: nkeys not supported by the server");
inline auto StaleConnection = Error("nats: stale connection");
inline auto TokenAlreadySet = Error("nats: token and token handler both set");
inline auto MsgNotBound =
    Error("nats: message is not bound to subscription/connection");
inline auto MsgNoReply = Error("nats: message does not have a reply");
inline auto ClientIPNotSupported =
    Error("nats: client IP not supported by this server");
inline auto Disconnected = Error("nats: server is disconnected");
inline auto HeadersNotSupported =
    Error("nats: headers not supported by this server");
inline auto BadHeaderMsg = Error("nats: message could not decode headers");
inline auto NoResponders = Error("nats: no responders available for request");
inline auto MaxConnectionsExceeded =
    Error("nats: server maximum connections exceeded");
inline auto ConnectionNotTLS = Error("nats: connection is not tls");

} // namespace err

namespace hdr {

constexpr std::string ServerId = "server_id";
constexpr std::string ServerName = "server_name";
constexpr std::string Version = "version";
constexpr std::string Go = "go";
constexpr std::string Host = "host";
constexpr std::string Port = "port";
constexpr std::string Headers = "headers";
constexpr std::string MaxPayload = "max_payload";
constexpr std::string Proto = "proto";
constexpr std::string ClientId = "client_id";
constexpr std::string AuthRequired = "auth_required";
constexpr std::string TlsRequired = "tls_required";
constexpr std::string TlsVerify = "tls_verify";
constexpr std::string TlsAvailable = "tls_available";
constexpr std::string ConnectUrls = "connect_urls";
constexpr std::string WsConnectUrls = "ws_connect_urls";
constexpr std::string Ldm = "ldm";
constexpr std::string GitCommit = "git_commit";
constexpr std::string JetStream = "jetstream";
constexpr std::string IP = "ip";
constexpr std::string ClientIP = "client_ip";
constexpr std::string Nonce = "nonce";
constexpr std::string Cluster = "cluster";
constexpr std::string Domain = "domain";

constexpr std::string Error = "error";

}; // namespace hdr

using HeaderValue = std::variant<std::vector<std::string>, std::string, int,
                                 bool, uint64_t, Error>;
using Header = std::map<std::string, HeaderValue>;

// -----------
// Statistics
// -----------
struct Statistics {
  uint64_t in_msgs;
  uint64_t out_msgs;
  uint64_t in_bytes;
  uint64_t out_bytes;
  uint64_t reconnects;
};

struct ConnectCallbacks {
  using Err = std::function<promise_t<void>(Client &, Error &)>;
  using Disconnected = std::function<promise_t<void>(spy<Conn>)>;
  using Closed = std::function<promise_t<void>(spy<Conn>)>;
  using Discovered = std::function<promise_t<void>(spy<Conn>)>;
  using Reconnected = std::function<promise_t<void>(spy<Conn>)>;

  Err err;
  Disconnected disconnected;
  Closed closed;
  Discovered discovered;
  Reconnected reconnected;
};

struct SslHandlers {
  using SignatureHandler = std::function<void(std::span<byte>, Error)>;
  using UserJWTHandler = std::function<void(std::string const &, Error &)>;
  using CredentialsHandler = std::function<void(Credentials const &)>;
};

} // namespace nats
