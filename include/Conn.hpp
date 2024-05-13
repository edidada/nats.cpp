// SPDX-License-Identifier: MIT
#pragma once

#include <array>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <shared_mutex>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "types.hpp"

namespace nats {

auto go(auto) {} // TODO

struct Conn : Statistics {
  std::shared_mutex mu;
  Options opts;
  sync::WaitGroup wg;
  std::vector<spy<srv>> srv_pool;
  spy<srv> current;
  std::unordered_map<std::string, std::string> urls;
  net::Conn conn;
  spy<Writer> bw;
  spy<Reader> br;
  chan<any> fch;
  serverInfo info;
  int64_t ssid;
  sync::RWMutex subs_mu;
  std::unordered_map<int64_t, spy<Subscription>> subs;
  std::unique_ptr<asyncCallbacksHandler> ach;
  std::vector<chan<any>> pongs;
  std::array<byte, defaults::scratch_size> scratch;
  Status status;
  std::unordered_map<Status, std::vector<chan<Status>>> stat_listeners;
  bool initc;
  Error err;
  spy<parseState> ps;
  spy<time::Timer> ptmr;
  int pout;
  bool ar; // abort reconnect
  chan<any> rqch;
  bool ws; // true if websocket connection

  // new style response handler
  std::string resp_sub;        // the wildcard subject
  std::string resp_sub_prefix; // the wildcard prefix including trailing .
  int resp_sub_len; // the length of the wildcard prefix excluding trailing .
  spy<Subscription> resp_mux; // a single response subscription
  std::unordered_map<std::string,
                     chan<spy<Msg>>>
      resp_map;         // request map for response msg channels
  rand::Rand resp_rand; // used for generating suffix

  // msg filters for testing, protected by subs_mu
  std::unordered_map<std::string, msgFilter> filters;

  // methods:
  std::tuple<int, spy<srv>> current_server();
  expected<spy<srv>, Error> select_next_server();
  Error pick_server();

  Error setup_server_pool();
  std::string conn_scheme();
  bool host_is_ip(url::Url const &u);
  Error add_url_to_pool(std::string_view surl, bool implicit,
                        bool save_tls_name);
  void shuffle_pool(int offset);
  void new_reader_writer();
  void bind_to_new_conn();
  expected<bool, Error> connect();
  io::Writer new_writer();
};

std::vector<std::string> process_url_string(std::string_view url);

auto connect(std::string_view url, std::function<Error(Options &)> opt)
    -> expected<std::unique_ptr<Conn>, Error>;

struct js {};

struct createConsumerRequest {};

struct jsSub {
  spy<js> js_;

  std::string nms;

  std::string psubj;
  std::string consumer;
  std::string stream;
  std::string deliver;
  bool pull;
  bool dc;
  bool ack_none;

  uint64_t pending;

  bool ordered;
  uint64_t dseq;
  uint64_t sseq;
  spy<createConsumerRequest> ccreq;

  // heartbeat and flow control from push consumers
  spy<time::Timer> hbc;
  std::chrono::seconds hbi;
  bool active;
  std::string cmeta;
  std::string fcr;
  uint64_t fcd;
  uint64_t fciseq;
  spy<time::Timer> csfct;

  // cancellation function to cancel context on drain/unsubscribe
  std::function<void()> cancel;
};

struct SubscriptionType {};

struct Subscription {
  std::mutex mu;
  int64_t sid;

  std::string subject;
  std::string queue;

  spy<jsSub> jsi;

  uint64_t delivered;
  uint64_t max;
  spy<Conn> conn;
  MsgHandler mcb;
  chan<spy<Msg>> mch;
  bool closed;
  bool sc;
  bool conn_closed;
  bool draining;
  SubStatus status;
  std::unordered_map<chan<SubStatus>, std::vector<SubStatus>> stat_listeners;

  // type of subscription
  SubscriptionType typ;

  // async linked list
  spy<Msg> p_head;
  spy<Msg> p_tail;
  std::condition_variable p_cond;
  std::function<void(std::string subject)> p_done;

  // pending stats, async subscriptions, high-speed etc.
  int p_msgs;
  int p_bytes;
  int p_msgs_max;
  int p_bytes_max;
  int p_msgs_limit;
  int p_bytes_limit;
  int dropped;
};

} // namespace nats
