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
    std::set<std::string> topics; // subscribed topics — for disconnect cleanup
    std::set<std::string> keys;   // watched kv keys — for disconnect cleanup
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
      auto it = subs_.find(f.name);
      if (it != subs_.end())
        it->second.erase(c);
      c->topics.erase(f.name);
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
    default:
      break;
    }
  }

  void remove_client(const ClientPtr &c) {
    {
      std::unique_lock<std::shared_mutex> lk(subs_mu_);
      for (const auto &t : c->topics) {
        auto it = subs_.find(t);
        if (it != subs_.end())
          it->second.erase(c);
      }
    }
    {
      std::lock_guard<std::mutex> lk(kv_mu_);
      for (const auto &k : c->keys) {
        auto it = watchers_.find(k);
        if (it != watchers_.end())
          it->second.erase(c);
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

  // Fan a publish out to every subscriber of the topic except the sender.
  void route_publish(const ClientPtr &c, const Frame &f) {
    std::vector<ClientPtr> targets; // snapshot keeps subscribers alive
    {
      std::shared_lock<std::shared_mutex> lk(subs_mu_);
      auto it = subs_.find(f.name);
      if (it != subs_.end())
        for (const auto &s : it->second)
          if (s.get() != c.get()) // noLocal: don't echo to the publisher
            targets.push_back(s);
    }
    for (const auto &t : targets)
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

  std::shared_mutex subs_mu_;
  std::unordered_map<std::string, std::set<ClientPtr>> subs_;

  // Last-value per topic, for topics published with MSG_PUBLISH_RETAIN. Its own
  // lock so the publish hot path keeps routing under a shared_lock.
  std::mutex retained_mu_;
  std::unordered_map<std::string, std::vector<char>> retained_;

  std::mutex kv_mu_;
  std::unordered_map<std::string, std::vector<char>> kv_;
  std::unordered_map<std::string, std::set<ClientPtr>> watchers_;
};

} // namespace ipc
