#pragma once
#include "ipc/transport.hpp"
#include <atomic>
#include <cstring>
#include <memory>
#include <mutex>
#include <set>
#include <shared_mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <sys/socket.h>
#include <unistd.h>

namespace ipc {

// Central broker daemon. Accepts many clients on a stream socket and routes
// between them: PUBLISH goes to every subscriber of the topic (except the
// sender), and it owns the authoritative key/value store that clients SET into
// and GET/watch out of. It is payload-agnostic — a topic/key is just a string
// and the value is opaque bytes; no C++ message types leak in here.
//
// ponytail: thread-per-client + global locks. Fine for a robotics node graph
// (tens of long-lived clients). Move to epoll + sharded locks only if client
// count or fan-out throughput actually demands it.
class BrokerServer {
public:
  explicit BrokerServer(const std::string &path = default_broker_addr()) {
    listen_fd_ = unix_listen(path);
    if (listen_fd_ < 0)
      throw std::runtime_error("broker listen failed: " + path);
    running_.store(true);
    accept_thread_ = std::thread([this] { accept_loop(); });
  }

  ~BrokerServer() {
    running_.store(false);
    ::shutdown(listen_fd_, SHUT_RDWR); // unblock accept()
    if (accept_thread_.joinable())
      accept_thread_.join();
    ::close(listen_fd_);
    { // unblock every client reader so its loop can exit
      std::lock_guard<std::mutex> lk(clients_mu_);
      for (auto &c : clients_) {
        std::lock_guard<std::mutex> s(c->send_mu);
        if (c->fd >= 0)
          ::shutdown(c->fd, SHUT_RDWR);
      }
    }
    for (auto &t : reader_threads_)
      if (t.joinable())
        t.join();
  }

  BrokerServer(const BrokerServer &) = delete;
  BrokerServer &operator=(const BrokerServer &) = delete;

private:
  struct Client {
    int fd;
    std::mutex send_mu;
    std::set<std::string> topics;   // exact subscriptions — for disconnect cleanup
    std::set<std::string> patterns; // wildcard subscriptions — same
    std::set<std::string> keys;     // watched kv keys — for disconnect cleanup
    explicit Client(int f) : fd(f) {}
  };
  using ClientPtr = std::shared_ptr<Client>;

  void accept_loop() {
    while (running_.load()) {
      int cfd = unix_accept(listen_fd_);
      if (cfd < 0) {
        if (!running_.load())
          break;
        continue;
      }
      auto c = std::make_shared<Client>(cfd);
      std::lock_guard<std::mutex> lk(clients_mu_);
      clients_.push_back(c);
      // ponytail: finished reader threads linger here until server shutdown
      // (no reconnect churn in v1). Add reaping if clients cycle a lot.
      reader_threads_.emplace_back([this, c] { client_loop(c); });
    }
  }

  void client_loop(ClientPtr c) {
    FrameReader reader(c->fd);
    Frame f;
    while (running_.load() && reader.next(f))
      handle_frame(c, f);
    remove_client(c);
  }

  void handle_frame(const ClientPtr &c, const Frame &f) {
    switch (f.kind) {
    case MSG_SUBSCRIBE: {
      if (topic_has_wildcard(f.name)) {
        std::unique_lock<std::shared_mutex> lk(subs_mu_);
        pattern_subs_[f.name].insert(c);
        c->patterns.insert(f.name);
        break; // no retained replay for patterns (see route_publish note)
      }
      {
        std::unique_lock<std::shared_mutex> lk(subs_mu_);
        subs_[f.name].insert(c);
        c->topics.insert(f.name); // only this client's reader thread touches it
      }
      // Deliver the topic's retained last-value, if any, so a subscriber that
      // joined after a retained publish still gets the current state. Only
      // topics published with MSG_PUBLISH_RETAIN have one; plain pub/sub topics
      // don't, so their late subscribers miss what they weren't around for. A
      // subscribe racing a retained publish may deliver it twice — last-value
      // semantics make that idempotent.
      std::vector<char> retained;
      bool have = false;
      {
        std::lock_guard<std::mutex> lk(retained_mu_);
        auto it = retained_.find(f.name);
        if (it != retained_.end()) {
          retained = it->second;
          have = true;
        }
      }
      if (have)
        send_to(*c, MSG_PUBLISH, f.name, retained.data(), retained.size());
      break;
    }
    case MSG_UNSUBSCRIBE: {
      std::unique_lock<std::shared_mutex> lk(subs_mu_);
      if (topic_has_wildcard(f.name)) {
        auto it = pattern_subs_.find(f.name);
        if (it != pattern_subs_.end()) {
          it->second.erase(c);
          if (it->second.empty())
            pattern_subs_.erase(it); // else a dead entry lingers forever
        }
        c->patterns.erase(f.name);
      } else {
        auto it = subs_.find(f.name);
        if (it != subs_.end()) {
          it->second.erase(c);
          if (it->second.empty())
            subs_.erase(it);
        }
        c->topics.erase(f.name);
      }
      break;
    }
    case MSG_PUBLISH:
      route_publish(c, f);
      break;
    case MSG_PUBLISH_RETAIN:
      route_publish(c, f);
      { // store the last-value for replay to future subscribers
        std::lock_guard<std::mutex> lk(retained_mu_);
        retained_[f.name].assign(f.payload.begin(), f.payload.end());
      }
      break;
    case MSG_KV_SET: {
      // Serialize the store write and its pushes under one lock so a watcher
      // observes SETs and its own GET reply in a single consistent order (a
      // reply can't slip out after a later update). ponytail: global kv lock
      // held across pushes — add per-key versioning if this ever stalls.
      std::lock_guard<std::mutex> lk(kv_mu_);
      kv_[f.name].assign(f.payload.begin(), f.payload.end());
      auto it = watchers_.find(f.name);
      if (it != watchers_.end())
        for (const auto &w : it->second)
          send_to(*w, MSG_KV_UPDATE, f.name, f.payload.data(),
                  f.payload.size());
      break;
    }
    case MSG_KV_GET: {
      if (f.payload.size() < 4)
        break;
      uint32_t id;
      std::memcpy(&id, f.payload.data(), 4);
      std::lock_guard<std::mutex> lk(kv_mu_);
      watchers_[f.name].insert(c); // register-then-read is atomic under the lock
      c->keys.insert(f.name);
      auto it = kv_.find(f.name);
      std::vector<char> pl; // [u32 id][u8 found][value]
      append_u32(pl, id);
      if (it != kv_.end()) {
        pl.push_back(1);
        pl.insert(pl.end(), it->second.begin(), it->second.end());
      } else {
        pl.push_back(0);
      }
      send_to(*c, MSG_KV_REPLY, f.name, pl.data(), pl.size());
      break;
    }
    case MSG_PING:
      send_to(*c, MSG_PONG, f.name, f.payload.data(), f.payload.size());
      break;
    case MSG_INFO: {
      if (f.payload.size() < 4)
        break;
      std::string text = build_info();
      std::vector<char> pl(f.payload.begin(), f.payload.begin() + 4); // req_id
      pl.insert(pl.end(), text.begin(), text.end());
      send_to(*c, MSG_INFO, "", pl.data(), pl.size());
      break;
    }
    default:
      break;
    }
  }

  // Human-readable daemon status: client/subscription/retained/kv counts plus
  // per-topic subscriber tallies. Read-only snapshot under the relevant locks.
  std::string build_info() {
    size_t nclients;
    {
      std::lock_guard<std::mutex> lk(clients_mu_);
      nclients = clients_.size();
    }
    std::string topics_detail;
    size_t ntopics = 0, nsubs = 0, npatterns = 0;
    {
      std::shared_lock<std::shared_mutex> lk(subs_mu_);
      for (const auto &e : subs_) {
        ntopics++;
        nsubs += e.second.size();
        topics_detail +=
            "\n    " + e.first + "=" + std::to_string(e.second.size());
      }
      npatterns = pattern_subs_.size();
    }
    size_t nretained;
    {
      std::lock_guard<std::mutex> lk(retained_mu_);
      nretained = retained_.size();
    }
    size_t nkeys, nwatched;
    {
      std::lock_guard<std::mutex> lk(kv_mu_);
      nkeys = kv_.size();
      nwatched = watchers_.size(); // keys with at least one live watcher
    }
    std::string s;
    s += "clients: " + std::to_string(nclients) + "\n";
    s += "topics: " + std::to_string(ntopics) + " (" + std::to_string(nsubs) +
         " subscriptions)" + topics_detail + "\n";
    s += "patterns: " + std::to_string(npatterns) + "\n";
    s += "retained: " + std::to_string(nretained) + "\n";
    s += "kv_keys: " + std::to_string(nkeys) + "\n";
    s += "kv_watchers: " + std::to_string(nwatched);
    return s;
  }

  void remove_client(const ClientPtr &c) {
    {
      std::unique_lock<std::shared_mutex> lk(subs_mu_);
      for (const auto &t : c->topics) {
        auto it = subs_.find(t);
        if (it != subs_.end()) {
          it->second.erase(c);
          // Drop the whole entry once nobody is left: an empty set would
          // otherwise stay for the daemon's life, and route_publish's linear
          // pattern scan would walk the dead pattern ones on every publish.
          if (it->second.empty())
            subs_.erase(it);
        }
      }
      for (const auto &p : c->patterns) {
        auto it = pattern_subs_.find(p);
        if (it != pattern_subs_.end()) {
          it->second.erase(c);
          if (it->second.empty())
            pattern_subs_.erase(it);
        }
      }
    }
    {
      std::lock_guard<std::mutex> lk(kv_mu_);
      for (const auto &k : c->keys) {
        auto it = watchers_.find(k);
        if (it != watchers_.end()) {
          it->second.erase(c);
          if (it->second.empty())
            watchers_.erase(it);
        }
      }
    }
    { // close under send_mu so no send_to races a reused fd
      std::lock_guard<std::mutex> lk(c->send_mu);
      if (c->fd >= 0)
        ::close(c->fd);
      c->fd = -1;
    }
    std::lock_guard<std::mutex> lk(clients_mu_);
    for (auto it = clients_.begin(); it != clients_.end(); ++it)
      if (it->get() == c.get()) {
        clients_.erase(it);
        break;
      }
  }

  // Fan a publish out to every subscriber of the topic except the sender —
  // exact subscribers plus any pattern subscriber whose wildcard matches. A set
  // dedupes so a client subscribed both ways gets a single copy.
  // ponytail: patterns are matched by a linear scan per publish. Fine for a
  // handful; build a level-trie if pattern count ever gets large.
  void route_publish(const ClientPtr &c, const Frame &f) {
    std::set<ClientPtr> targets; // keeps subscribers alive + dedupes
    {
      std::shared_lock<std::shared_mutex> lk(subs_mu_);
      auto it = subs_.find(f.name);
      if (it != subs_.end())
        targets.insert(it->second.begin(), it->second.end());
      for (const auto &ps : pattern_subs_)
        if (topic_matches(ps.first, f.name))
          targets.insert(ps.second.begin(), ps.second.end());
    }
    for (const auto &t : targets)
      if (t.get() != c.get()) // noLocal: don't echo to the publisher
        send_to(*t, MSG_PUBLISH, f.name, f.payload.data(), f.payload.size());
  }

  void send_to(Client &c, uint8_t kind, const std::string &name,
               const char *payload, size_t plen) {
    auto buf = build_frame(kind, name, payload, plen);
    std::lock_guard<std::mutex> lk(c.send_mu);
    if (c.fd >= 0)
      write_full(c.fd, buf.data(), buf.size());
  }

  int listen_fd_;
  std::atomic<bool> running_{false};
  std::thread accept_thread_;

  std::mutex clients_mu_;
  std::vector<ClientPtr> clients_;
  std::vector<std::thread> reader_threads_; // joined at shutdown

  std::shared_mutex subs_mu_; // guards both subs_ and pattern_subs_
  std::unordered_map<std::string, std::set<ClientPtr>> subs_;
  std::unordered_map<std::string, std::set<ClientPtr>> pattern_subs_;

  // Last-value per topic, for topics published with MSG_PUBLISH_RETAIN. Its own
  // lock so the publish hot path keeps routing under a shared_lock.
  std::mutex retained_mu_;
  std::unordered_map<std::string, std::vector<char>> retained_;

  std::mutex kv_mu_;
  std::unordered_map<std::string, std::vector<char>> kv_;
  std::unordered_map<std::string, std::set<ClientPtr>> watchers_;
};

} // namespace ipc
