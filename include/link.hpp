#pragma once
#include "chappe.hpp"
#include "ipc/transport.hpp"
#include <atomic>
#include <map>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace chappe {

// Joins this device's bus to one peer device's bus over TCP.
//
// Cross-device is a daemon per device rather than one daemon with remote
// clients: local traffic never touches the network, frames stay zero-copy where
// they can actually be mapped, and a partition leaves each device's own bus
// working. The daemon therefore never learns TCP, and keeps its unix socket and
// its SO_PEERCRED uid gate.
//
// This is a protocol relay, not an application, so it speaks frames straight to
// the local daemon instead of going through Node — which is what lets it act on
// KV_UPDATE/KV_DEL the moment they arrive, where a Node would bury them in its
// cache and leave the link polling for changes.
//
// There is no auth on the wire. Run links over a private network (WireGuard,
// SSH, a VLAN); the address tcp_listen binds is the whole access-control story.
class Link {
public:
  // Link-to-link only. It never reaches a daemon, so it sits well clear of the
  // MSG_ range the client protocol uses rather than extending that enum.
  static constexpr uint8_t MSG_LINK_HELLO = 200;

  struct Config {
    std::string socket = default_addr();
    std::vector<std::string> topics; // wildcard patterns, forwarded both ways
    std::vector<std::string> keys;   // exact key names, forwarded both ways
    // Carry on despite a peer whose ABI differs. Only safe when every forwarded
    // payload is self-describing — strings and raw bytes survive a layout or
    // byte-order difference; a struct going through the default wire_codec
    // does not.
    bool allow_abi_mismatch = false;
  };

  // `peer_fd` is an already-connected socket to the peer link — whoever built
  // it decided listen-vs-connect, which keeps that policy out of here and lets
  // a test hand over a socketpair. Ownership transfers only on success: if this
  // throws, the caller still owns the fd.
  Link(Config cfg, int peer_fd)
      : cfg_(std::move(cfg)), peer_fd_(peer_fd) {
    // Sent before anything else, so it is the first frame on the wire and the
    // peer can vet us before a single data frame reaches its daemon. Read the
    // reply in peer_loop rather than blocking here: both ends run this same
    // constructor, and two links waiting for each other would never start.
    uint64_t mine = abi_fingerprint();
    char fp[sizeof(mine)];
    std::memcpy(fp, &mine, sizeof(mine));
    if (!send_frame(peer_fd_, MSG_LINK_HELLO, VERSION, fp, sizeof(fp)))
      throw std::runtime_error("link: peer hung up before the handshake");

    local_fd_ = unix_connect(cfg_.socket);
    if (local_fd_ < 0)
      throw std::runtime_error("link: cannot reach broker at " + cfg_.socket);
    for (const auto &t : cfg_.topics)
      send_frame(local_fd_, MSG_SUBSCRIBE, t, nullptr, 0);
    // A get is what registers a watch, and its reply also seeds the peer with
    // whatever the key already held. Keys are exact names, not patterns: the
    // store has no prefix-watch, so there is nothing to enumerate against.
    for (uint32_t id = 0; id < cfg_.keys.size(); id++) {
      std::vector<char> pl;
      append_u32(pl, id);
      send_frame(local_fd_, MSG_KV_GET, cfg_.keys[id], pl.data(), pl.size());
    }
    running_.store(true);
    try {
      local_thread_ = std::thread([this] { local_loop(); });
      peer_thread_ = std::thread([this] { peer_loop(); });
    } catch (...) { // same story as Server: no destructor runs from here
      stop();
      ::close(local_fd_);
      throw;
    }
  }

  ~Link() {
    stop();
    ::close(local_fd_);
    ::close(peer_fd_);
  }

  Link(const Link &) = delete;
  Link &operator=(const Link &) = delete;

  // False once either side hangs up. Losing a link is not recovered from here —
  // the process exits and a supervisor restarts it, like the Redis bridge.
  bool alive() const { return running_.load(); }

  // Why the link stopped, empty if it just lost its peer. Safe to read once
  // alive() is false: it is written before the store that clears running_, so
  // that store/load pair orders it.
  const std::string &error() const { return error_; }

private:
  void fail(std::string why) { error_ = std::move(why); }

  static bool send_frame(int fd, uint8_t kind, const std::string &name,
                         const char *payload, size_t n) {
    auto buf = build_frame(kind, name, payload, n);
    return write_full(fd, buf.data(), buf.size());
  }

  bool forwards_topic(const std::string &t) const {
    for (const auto &p : cfg_.topics)
      if (topic_matches(p, t))
        return true;
    return false;
  }

  bool forwards_key(const std::string &k) const {
    for (const auto &c : cfg_.keys)
      if (c == k)
        return true;
    return false;
  }

  // ---- echo suppression ---------------------------------------------------
  // A value applied locally on the peer's behalf comes straight back as a
  // KV_UPDATE, because the daemon pushes to every watcher including the writer.
  // Publishes need no such care — route_publish is noLocal — but forwarding
  // that KV echo would ping-pong the key between the two devices forever.

  void expect_echo(const std::string &key, std::string val, bool del) {
    std::lock_guard<std::mutex> lk(echo_mu_);
    (del ? echo_del_[key] : echo_set_[{key, std::move(val)}])++;
  }

  bool consume_echo(const std::string &key, std::string val, bool del) {
    std::lock_guard<std::mutex> lk(echo_mu_);
    if (del) {
      auto it = echo_del_.find(key);
      if (it == echo_del_.end())
        return false;
      if (--it->second == 0)
        echo_del_.erase(it);
      return true;
    }
    auto it = echo_set_.find({key, std::move(val)});
    if (it == echo_set_.end())
      return false;
    if (--it->second == 0)
      echo_set_.erase(it);
    return true;
  }

  // ---- the two directions -------------------------------------------------
  // Each fd has exactly one writer thread — the local reader writes to the
  // peer, the peer reader writes to the daemon — so neither needs a send lock.

  void forward_kv(const std::string &key, const char *val, size_t n) {
    if (consume_echo(key, std::string(val, n), false))
      return;
    send_frame(peer_fd_, MSG_KV_SET, key, val, n);
  }

  void local_loop() {
    FrameReader reader(local_fd_);
    Frame f;
    while (running_.load() && reader.next(f)) {
      switch (f.kind) {
      case MSG_PUBLISH:
        if (forwards_topic(f.name))
          send_frame(peer_fd_, MSG_PUBLISH, f.name, f.payload.data(),
                     f.payload.size());
        break;
      case MSG_KV_REPLY: // our startup get: [u32 id][u8 found][value]
        if (f.payload.size() >= 5 && f.payload[4])
          forward_kv(f.name, f.payload.data() + 5, f.payload.size() - 5);
        break;
      case MSG_KV_UPDATE:
        forward_kv(f.name, f.payload.data(), f.payload.size());
        break;
      case MSG_KV_DEL: // also how a ttl expiry reaches a watcher
        if (!consume_echo(f.name, "", true))
          send_frame(peer_fd_, MSG_KV_DEL, f.name, nullptr, 0);
        break;
      }
    }
    running_.store(false);
    ::shutdown(peer_fd_, SHUT_RDWR); // unblock the other loop
  }

  // The peer's hello must be the very first frame it sends, so checking it
  // before the loop body means nothing from an incompatible peer is ever
  // applied — there is no window to race.
  bool peer_accepted(const Frame &f) {
    if (f.kind != MSG_LINK_HELLO || f.payload.size() != sizeof(uint64_t)) {
      fail("peer did not open with a handshake — is it a chappe_link?");
      return false;
    }
    uint64_t theirs;
    std::memcpy(&theirs, f.payload.data(), sizeof(theirs));
    if (theirs == abi_fingerprint())
      return true;
    if (cfg_.allow_abi_mismatch)
      return true;
    fail("peer ABI differs (peer runs chappe " + f.name +
         "). Raw struct payloads would decode into garbage across this link: "
         "rebuild both ends for the same ABI, or pass --allow-abi-mismatch if "
         "everything forwarded is strings or bytes.");
    return false;
  }

  void peer_loop() {
    FrameReader reader(peer_fd_);
    Frame f;
    if (!reader.next(f) || !peer_accepted(f)) {
      running_.store(false);
      ::shutdown(local_fd_, SHUT_RDWR);
      return;
    }
    while (running_.load() && reader.next(f)) {
      // Filtered on arrival too, not just on send: a peer configured with
      // patterns we don't share would otherwise inject topics this device never
      // agreed to carry.
      switch (f.kind) {
      case MSG_PUBLISH:
        if (forwards_topic(f.name))
          send_frame(local_fd_, MSG_PUBLISH, f.name, f.payload.data(),
                     f.payload.size());
        break;
      case MSG_KV_SET:
        if (forwards_key(f.name)) {
          expect_echo(f.name, std::string(f.payload.begin(), f.payload.end()),
                      false);
          send_frame(local_fd_, MSG_KV_SET, f.name, f.payload.data(),
                     f.payload.size());
        }
        break;
      case MSG_KV_DEL:
        if (forwards_key(f.name)) {
          expect_echo(f.name, "", true);
          send_frame(local_fd_, MSG_KV_DEL, f.name, nullptr, 0);
        }
        break;
      }
    }
    running_.store(false);
    ::shutdown(local_fd_, SHUT_RDWR);
  }

  void stop() {
    running_.store(false);
    ::shutdown(local_fd_, SHUT_RDWR);
    ::shutdown(peer_fd_, SHUT_RDWR);
    if (local_thread_.joinable())
      local_thread_.join();
    if (peer_thread_.joinable())
      peer_thread_.join();
  }

  Config cfg_;
  int peer_fd_;
  int local_fd_ = -1;
  std::string error_;
  std::atomic<bool> running_{false};
  std::thread local_thread_, peer_thread_;

  std::mutex echo_mu_;
  std::map<std::pair<std::string, std::string>, int> echo_set_;
  std::map<std::string, int> echo_del_;
};

} // namespace chappe
