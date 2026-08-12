#pragma once
#include "ipc/transport.hpp"
#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <future>
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
#include <sys/time.h>
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
  explicit BrokerServer(const std::string &path = default_broker_addr(),
                        std::set<uid_t> allowed_uids = {})
      : path_(path), allowed_uids_(std::move(allowed_uids)) {
    // An allow-list only works if the listed uids can reach the peercred check
    // at all, so the kernel gate has to open up when there is one.
    listen_fd_ = unix_listen(path, 128, allowed_uids_.empty() ? 0600 : 0666);
    if (listen_fd_ < 0)
      throw std::runtime_error("broker listen failed: " + path);
    running_.store(true);
    accept_thread_ = std::thread([this] { accept_loop(); });
    sweep_thread_ = std::thread([this] { sweep_loop(); });
  }

  ~BrokerServer() {
    running_.store(false);
    { // notify under the lock so the store above can't be missed between the
      // sweep's running_ check and its wait_for
      std::lock_guard<std::mutex> lk(sweep_mu_);
      sweep_cv_.notify_all();
    }
    sweep_thread_.join(); // before the client fds go: the sweep pushes to them
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
    // An std::async future's destructor blocks until its task finishes — that
    // is the join. Safe here: the accept thread is already joined, so this is
    // the only thread touching readers_.
    readers_.clear();
    // Don't leave the socket file behind. unix_listen() already unlinks a stale
    // path before binding, so the only loser is a second daemon that stole this
    // address while we were running — it was already broken by that theft.
    ::unlink(path_.c_str());
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

  // How long a blocking write to a client may stall before we give up on it.
  static constexpr int SEND_TIMEOUT_SEC = 2;
  // How often expired keys are swept out of the store.
  static constexpr int SWEEP_MS = 100;

  // The credentials come from the kernel, not the peer, so a client cannot
  // forge them. Same-uid is the boundary being defended and there is no point
  // going finer: another process running as you can ptrace you anyway.
  bool peer_allowed(int fd) {
    ucred cr;
    socklen_t len = sizeof(cr);
    if (::getsockopt(fd, SOL_SOCKET, SO_PEERCRED, &cr, &len) != 0)
      return false; // no credentials to judge, so no entry
    return allowed_uids_.empty() ? cr.uid == ::geteuid()
                                 : allowed_uids_.count(cr.uid) != 0;
  }

  void accept_loop() {
    while (running_.load()) {
      int cfd = unix_accept(listen_fd_);
      if (cfd < 0) {
        if (!running_.load())
          break;
        continue;
      }
      if (!peer_allowed(cfd)) { // before the fd is used for anything at all
        ::close(cfd);
        continue;
      }
      timeval tv{SEND_TIMEOUT_SEC, 0}; // bound how long a wedged peer can stall
      ::setsockopt(cfd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
      // Reap finished readers so a flapping client doesn't leak one per
      // reconnect. A ready future's destructor returns promptly. readers_ is
      // touched only by this thread and by the destructor once this thread is
      // joined — keep it that way and it needs no lock.
      readers_.erase(std::remove_if(readers_.begin(), readers_.end(),
                                    [](std::future<void> &r) {
                                      return r.wait_for(std::chrono::seconds(
                                                 0)) ==
                                             std::future_status::ready;
                                    }),
                     readers_.end());
      auto c = std::make_shared<Client>(cfd);
      std::lock_guard<std::mutex> lk(clients_mu_);
      clients_.push_back(c);
      readers_.push_back(
          std::async(std::launch::async, [this, c] { client_loop(c); }));
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
      { // store the last-value for replay to future subscribers — or, with an
        // empty payload, clear it (MQTT convention)
        std::lock_guard<std::mutex> lk(retained_mu_);
        if (f.payload.empty())
          retained_.erase(f.name);
        else
          retained_[f.name].assign(f.payload.begin(), f.payload.end());
      }
      break;
    case MSG_KV_SET: {
      // Serialize the store write and its pushes under one lock so a watcher
      // observes SETs and its own GET reply in a single consistent order (a
      // reply can't slip out after a later update). That ordering guarantee is
      // why the lock stays held across the pushes. SO_SNDTIMEO bounds each
      // individual send(), not the whole frame: a watcher that reads nothing is
      // dropped within a couple of timeouts, but one that drains slowly keeps
      // every send making progress, so the stall is frame_size/drain_rate (and
      // x watchers), not a fixed 2s.
      // ponytail: bounded stall, not no stall. The real fix is a per-client
      // outbox thread with a bounded queue and a drop policy — do that when any
      // daemon-wide stall is too much, or once KV values stop being KB-scale.
      std::lock_guard<std::mutex> lk(kv_mu_);
      store(f.name, f.payload.data(), f.payload.size(), 0);
      break;
    }
    case MSG_KV_SETEX: {
      if (f.payload.size() < 4)
        break;
      uint32_t ttl_ms;
      std::memcpy(&ttl_ms, f.payload.data(), 4);
      std::lock_guard<std::mutex> lk(kv_mu_);
      store(f.name, f.payload.data() + 4, f.payload.size() - 4, ttl_ms);
      break;
    }
    case MSG_KV_DEL: {
      std::lock_guard<std::mutex> lk(kv_mu_); // same ordering story as KV_SET
      kv_.erase(f.name);
      expires_.erase(f.name); // else the deadline outlives the key it belonged to
      push_kv_del(f.name);
      break;
    }
    case MSG_KV_GET: {
      if (f.payload.size() < 4)
        break;
      uint32_t id;
      std::memcpy(&id, f.payload.data(), 4);
      std::lock_guard<std::mutex> lk(kv_mu_);
      // Before the watch is registered, or the requester would be sent a KV_DEL
      // for a key it never saw.
      drop_if_expired(f.name, std::chrono::steady_clock::now());
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
    case MSG_KV_INCR: {
      // Values are opaque bytes, so incr fixes one representation: a
      // native-endian int64 in exactly 8 bytes (what wire_codec<int64_t> and
      // Python struct "=q" produce). An absent key counts as 0; anything else
      // stored under the key is a type error, not a coercion.
      if (f.payload.size() < 12)
        break;
      uint32_t id;
      int64_t delta;
      std::memcpy(&id, f.payload.data(), 4);
      std::memcpy(&delta, f.payload.data() + 4, 8);
      std::lock_guard<std::mutex> lk(kv_mu_);
      drop_if_expired(f.name, std::chrono::steady_clock::now());
      int64_t cur = 0;
      auto it = kv_.find(f.name);
      if (it != kv_.end()) {
        if (it->second.size() != sizeof(cur)) {
          send_kv_result(*c, f.name, id, false, nullptr, 0);
          break;
        }
        std::memcpy(&cur, it->second.data(), sizeof(cur));
      }
      // Wrap-around is fine, but signed overflow is UB — add as unsigned and
      // reinterpret the bits.
      uint64_t sum = static_cast<uint64_t>(cur) + static_cast<uint64_t>(delta);
      char val[sizeof(sum)];
      std::memcpy(val, &sum, sizeof(sum));
      // expires_ untouched: incr does not clear a TTL (Redis semantics).
      kv_[f.name].assign(val, val + sizeof(val));
      push_kv_update(f.name, val, sizeof(val));
      send_kv_result(*c, f.name, id, true, val, sizeof(val));
      break;
    }
    case MSG_KV_SETNX: {
      if (f.payload.size() < 8)
        break;
      uint32_t id, ttl_ms;
      std::memcpy(&id, f.payload.data(), 4);
      std::memcpy(&ttl_ms, f.payload.data() + 4, 4);
      std::lock_guard<std::mutex> lk(kv_mu_);
      drop_if_expired(f.name, std::chrono::steady_clock::now());
      bool acquired = kv_.find(f.name) == kv_.end();
      if (acquired) // ttl and all: a holder that dies without deleting the key
                    // must not deadlock every other node forever
        store(f.name, f.payload.data() + 8, f.payload.size() - 8, ttl_ms);
      send_kv_result(*c, f.name, id, acquired, nullptr, 0);
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

  void send_kv_result(Client &c, const std::string &key, uint32_t id, bool ok,
                      const char *val, size_t n) {
    std::vector<char> pl; // [u32 id][u8 ok][value]
    append_u32(pl, id);
    pl.push_back(ok ? 1 : 0);
    if (n)
      pl.insert(pl.end(), val, val + n);
    send_to(c, MSG_KV_RESULT, key, pl.data(), pl.size());
  }

  // ---- store mutations (caller holds kv_mu_) -------------------------------

  void push_kv_update(const std::string &key, const char *val, size_t n) {
    auto it = watchers_.find(key);
    if (it != watchers_.end())
      for (const auto &w : it->second)
        send_to(*w, MSG_KV_UPDATE, key, val, n);
  }

  void push_kv_del(const std::string &key) {
    auto it = watchers_.find(key);
    if (it != watchers_.end())
      for (const auto &w : it->second)
        send_to(*w, MSG_KV_DEL, key, nullptr, 0);
  }

  void store(const std::string &key, const char *val, size_t n,
             uint32_t ttl_ms) {
    kv_[key].assign(val, val + n);
    if (ttl_ms)
      expires_[key] = std::chrono::steady_clock::now() +
                      std::chrono::milliseconds(ttl_ms);
    else
      expires_.erase(key); // a plain set clears an existing TTL (Redis semantics)
    push_kv_update(key, val, n);
  }

  // Drop the key if its deadline has passed, telling its watchers. Returns
  // whether it was dropped.
  bool drop_if_expired(const std::string &key,
                       std::chrono::steady_clock::time_point now) {
    auto it = expires_.find(key);
    if (it == expires_.end() || it->second > now)
      return false;
    expires_.erase(it);
    kv_.erase(key);
    push_kv_del(key);
    return true;
  }

  // Expire keys in the background. Lazy expiry on read is not enough: a client
  // serves get() from its local cache once it watches a key, so an expired key
  // nothing round-trips for would never be noticed — and "key absent ⇒ node
  // gone" is exactly what a TTL is used for. Pushes under kv_mu_ like
  // MSG_KV_SET, so it inherits the same bounded-stall ceiling documented there.
  void sweep_loop() {
    std::unique_lock<std::mutex> lk(sweep_mu_);
    while (running_.load()) {
      sweep_cv_.wait_for(lk, std::chrono::milliseconds(SWEEP_MS));
      if (!running_.load())
        break;
      auto now = std::chrono::steady_clock::now();
      std::vector<std::string> due; // collected first: dropping erases entries
      std::lock_guard<std::mutex> kv_lk(kv_mu_);
      for (const auto &e : expires_)
        if (e.second <= now)
          due.push_back(e.first);
      for (const auto &k : due)
        drop_if_expired(k, now);
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
    size_t nkeys, nwatched, nexpiring;
    {
      std::lock_guard<std::mutex> lk(kv_mu_);
      nkeys = kv_.size();
      nwatched = watchers_.size(); // keys with at least one live watcher
      nexpiring = expires_.size();
    }
    std::string s;
    s += "clients: " + std::to_string(nclients) + "\n";
    s += "topics: " + std::to_string(ntopics) + " (" + std::to_string(nsubs) +
         " subscriptions)" + topics_detail + "\n";
    s += "patterns: " + std::to_string(npatterns) + "\n";
    s += "retained: " + std::to_string(nretained) + "\n";
    s += "kv_keys: " + std::to_string(nkeys) + "\n";
    s += "kv_watchers: " + std::to_string(nwatched) + "\n";
    s += "kv_expiring: " + std::to_string(nexpiring);
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
    if (c.fd < 0)
      return;
    if (!write_full(c.fd, buf.data(), buf.size()))
      // Gone, or wedged past SEND_TIMEOUT_SEC — and a timed-out write may have
      // emitted a partial frame, so that stream is desynced either way and
      // dropping it is the only correct response. Shutdown unwinds the client's
      // reader loop; remove_client() does the cleanup and owns the close.
      ::shutdown(c.fd, SHUT_RDWR);
  }

  std::string path_; // listen address, unlinked on shutdown
  int listen_fd_;
  // Empty means "whoever runs the daemon, and nobody else". A non-empty set is
  // the complete list rather than an addition to it, so excluding yourself is
  // possible — and, if you wrote it, deliberate.
  std::set<uid_t> allowed_uids_;
  std::atomic<bool> running_{false};
  std::thread accept_thread_;

  std::mutex clients_mu_;
  std::vector<ClientPtr> clients_;
  std::vector<std::future<void>> readers_; // accept thread only; see accept_loop

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
  // Deadline per TTL'd key; keys set without a TTL have no entry at all.
  std::unordered_map<std::string, std::chrono::steady_clock::time_point>
      expires_;

  std::mutex sweep_mu_; // only pairs with sweep_cv_, so shutdown is prompt
  std::condition_variable sweep_cv_;
  std::thread sweep_thread_;
};

} // namespace ipc
