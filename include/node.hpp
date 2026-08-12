#pragma once
#include "broker.hpp"
#include "ipc/frame_handle.hpp"
#include "ipc/shm_ring.hpp"
#include "ipc/transport.hpp"
#include "threadpool.hpp"
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstring>
#include <functional>
#include <future>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <sys/socket.h>
#include <unistd.h>

// A client of the broker daemon (see broker_server.hpp). One connection per
// node; a background reader thread delivers incoming publishes to local
// handlers and services kv replies. The type-safe surface (publish/subscribe/
// set/get keyed on a C++ message type) is a client-side convenience over the
// daemon's string-topic routing: Topic<T>::name is the wire topic and
// wire_codec<T> is the payload.
class Node {
  using Receiver = std::function<void(const char *, size_t)>;
  // Pattern handlers are untyped: a wildcard spans message types, so the handler
  // gets the concrete topic plus raw bytes and decodes based on the topic.
  using PatternReceiver =
      std::function<void(const std::string &, const char *, size_t)>;
  using KvPromise = std::promise<std::optional<std::vector<char>>>;

public:
  Node(std::string name, size_t threads = 0)
      : name_(std::move(name)),
        pool_(threads ? std::make_unique<ThreadPool>(threads) : nullptr) {}

  ~Node() {
    running_.store(false);
    if (reader_.joinable()) {
      { // unblock a reader parked in recv(); harmless if it's mid-backoff
        std::lock_guard<std::mutex> lk(send_mu_);
        if (fd_ >= 0)
          ::shutdown(fd_, SHUT_RDWR);
      }
      reader_.join();
    }
    { // reader owns fd_ in its loop, but close here too in case it never ran
      std::lock_guard<std::mutex> lk(send_mu_);
      if (fd_ >= 0)
        ::close(fd_);
      fd_ = -1;
    }
    // Members destruct in reverse declaration order after this body: pool_
    // (drains queued frame tasks) before rings_/frame_drops_, which those
    // tasks touch. reader_ is already joined, so nothing new is enqueued.
  }

  Node(const Node &) = delete;
  Node &operator=(const Node &) = delete;

  // Connect to a broker daemon and start the reader thread. Defaults to the
  // well-known address ($BROKER_SOCKET or /tmp/broker.sock). Handlers may be
  // registered before connect() — their subscriptions are flushed to the daemon
  // here. The initial connect must succeed (throws otherwise); if the connection
  // later drops, the reader thread transparently reconnects to the same address
  // and resubscribes.
  void connect(const std::string &path = ipc::default_broker_addr()) {
    if (started_)
      throw std::logic_error("node already connected");
    int fd = ipc::unix_connect(path);
    if (fd < 0)
      throw std::runtime_error("node connect failed for '" + path + "'");
    path_ = path;
    fd_ = fd;
    started_ = true;
    connected_.store(true);
    running_.store(true);
    reader_ = std::thread([this] { run(); });
    // Same job as after a reconnect: send SUBSCRIBE for everything already
    // registered. The kv cache it clears is necessarily empty at first connect.
    resubscribe();
  }

  // True while a live connection to the daemon exists (false during a reconnect
  // backoff window).
  bool connected() const noexcept { return connected_.load(); }

  // ---- pub/sub --------------------------------------------------------------

  template <typename F> void subscribe(F &&handler) {
    using T = msg_t<std::decay_t<F>>;
    register_topic<T>(std::function<void(const T &)>(std::forward<F>(handler)));
  }

  // Subscribe to a wildcard pattern ('/'-separated levels, '+' = one level,
  // '*' = the rest). The handler gets each matching message's concrete topic
  // and raw bytes — untyped, since a pattern can span message types. Pool-aware
  // like subscribe(). Regular pub/sub topics only (not frame rings).
  template <typename F> void subscribe_pattern(const std::string &pattern, F &&handler) {
    bool first;
    {
      std::lock_guard<std::mutex> lk(subs_mu_);
      auto &v = pattern_subs_[pattern];
      first = v.empty();
      v.push_back(PatternReceiver(std::forward<F>(handler)));
    }
    if (first)
      send(ipc::MSG_SUBSCRIBE, pattern, nullptr, 0);
  }

  // Stop receiving T's topic: drops every local handler for it, not just one —
  // the wire subscription is per-topic, so per-handler granularity isn't worth
  // the bookkeeping. No-op if T was never subscribed.
  template <typename T> void unsubscribe() {
    static_assert(Topic<T>::name != nullptr,
                  "unsubscribe needs MAKE_TOPIC(T, \"...\")");
    {
      // Erase the entry, not just its handlers: a lingering empty vector would
      // make resubscribe() re-subscribe after a reconnect.
      std::lock_guard<std::mutex> lk(subs_mu_);
      if (!subs_.erase(Topic<T>::name))
        return;
    }
    send(ipc::MSG_UNSUBSCRIBE, Topic<T>::name, nullptr, 0);
  }

  // unsubscribe() for a wildcard pattern — same all-handlers semantics.
  void unsubscribe_pattern(const std::string &pattern) {
    {
      std::lock_guard<std::mutex> lk(subs_mu_);
      if (!pattern_subs_.erase(pattern))
        return;
    }
    send(ipc::MSG_UNSUBSCRIBE, pattern, nullptr, 0);
  }

  // With retain=true the daemon keeps this as topic T's last value and replays
  // it to any node that subscribes later — use it for state/status topics a
  // late joiner needs the current value of. Default (retain=false) is classic
  // pub/sub: a subscriber only gets messages published while it was subscribed.
  template <typename T> void publish(const T &msg, bool retain = false) {
    require_connected();
    static_assert(Topic<T>::name != nullptr,
                  "publish<T> needs MAKE_TOPIC(T, \"...\")");
    std::vector<char> bytes;
    ipc::wire_codec<T>::encode(msg, bytes);
    send(retain ? ipc::MSG_PUBLISH_RETAIN : ipc::MSG_PUBLISH, Topic<T>::name,
         bytes.data(), bytes.size());
  }

  // Forget T's retained last-value, so nodes subscribing from now on are
  // replayed nothing. It is a zero-length retained publish, so current
  // subscribers do see an empty publish: the default POD codec rejects 0 bytes
  // so those handlers skip it, but codecs that accept 0 bytes
  // (wire_codec<std::string>, wire_codec<std::vector<T>>, custom ones) fire
  // with a default-constructed T.
  template <typename T> void clear_retained() {
    require_connected();
    static_assert(Topic<T>::name != nullptr,
                  "clear_retained<T> needs MAKE_TOPIC(T, \"...\")");
    send(ipc::MSG_PUBLISH_RETAIN, Topic<T>::name, nullptr, 0);
  }

  // Round-trip barrier: returns once the daemon has processed every frame this
  // node has sent so far. Handy after subscribe() to guarantee the daemon is
  // routing this node's topics before a peer starts publishing. Returns
  // immediately (nothing to flush) if called during a reconnect window.
  void sync() {
    require_connected();
    uint32_t id = next_req_.fetch_add(1);
    auto fut = register_pending(id);
    char b[4];
    std::memcpy(b, &id, 4);
    if (!send(ipc::MSG_PING, "", b, 4))
      fulfill(id, std::nullopt); // disconnected: don't block on a dropped ping
    fut.get();
  }

  // Query the daemon for a human-readable status snapshot (connected clients,
  // per-topic subscriber counts, retained/kv totals). Empty string if not
  // connected.
  std::string info() {
    require_connected();
    uint32_t id = next_req_.fetch_add(1);
    auto fut = register_pending(id);
    char b[4];
    std::memcpy(b, &id, 4);
    if (!send(ipc::MSG_INFO, "", b, 4))
      fulfill(id, std::nullopt);
    auto reply = fut.get();
    if (!reply)
      return "";
    return std::string(reply->begin(), reply->end());
  }

  // ---- frame (shm) API ------------------------------------------------------
  // A frame topic is a message type deriving from ipc::FrameHandle, registered
  // with MAKE_TOPIC. The backing ring's shm name is derived from Topic<T>::name,
  // so a producer's create_frame_ring<T>() and a consumer's attach_frame_ring<T>()
  // agree on the segment with no shared config. Only the small FrameHandle rides
  // the broker; the pixels move producer->consumer directly through the ring.

  template <typename T>
  void create_frame_ring(size_t slot_size, uint32_t num_slots) {
    static_assert(std::is_base_of_v<ipc::FrameHandle, T>,
                  "frame topic type must derive from ipc::FrameHandle");
    static_assert(Topic<T>::name != nullptr,
                  "frame topic type needs MAKE_TOPIC(T, \"...\")");
    rings_.emplace(Topic<T>::name, ipc::SharedMemoryRing::create(
                                       ring_shm_name<T>(), slot_size, num_slots));
  }

  template <typename T> void attach_frame_ring() {
    static_assert(std::is_base_of_v<ipc::FrameHandle, T>,
                  "frame topic type must derive from ipc::FrameHandle");
    static_assert(Topic<T>::name != nullptr,
                  "frame topic type needs MAKE_TOPIC(T, \"...\")");
    rings_.emplace(Topic<T>::name,
                   ipc::SharedMemoryRing::attach(ring_shm_name<T>()));
  }

  // Producer publish. `writer(void* data, size_t size)` fills the slot in place
  // (zero-copy). Returns false if every slot is held by a consumer (frame
  // dropped). Throws if T has no ring registered. If `writer` throws, the slot
  // is returned to the free pool (not leaked) and the exception propagates.
  template <typename T, typename WriterFn>
  bool publish_frame(uint64_t timestamp_ns, uint32_t width, uint32_t height,
                     uint32_t stride, WriterFn &&writer) {
    static_assert(std::is_base_of_v<ipc::FrameHandle, T>,
                  "frame topic type must derive from ipc::FrameHandle");
    static_assert(std::is_trivially_copyable_v<T>,
                  "frame message type must be trivially copyable");
    require_connected();
    auto it = rings_.find(Topic<T>::name);
    if (it == rings_.end())
      throw std::logic_error(std::string("publish_frame: no ring for topic '") +
                             Topic<T>::name + "'");

    auto handle = it->second.acquire_write();
    if (!handle.valid)
      return false; // genuinely starved — caller decides to retry or drop

    try {
      std::forward<WriterFn>(writer)(handle.data, handle.size);
    } catch (...) {
      it->second.abandon(handle.idx); // don't leak the slot as WRITING
      throw;
    }
    it->second.publish(handle.idx);

    T msg{};
    static_cast<ipc::FrameHandle &>(msg) =
        ipc::FrameHandle{timestamp_ns, width, height, stride};
    std::vector<char> bytes;
    ipc::wire_codec<T>::encode(msg, bytes);
    send(ipc::MSG_PUBLISH, Topic<T>::name, bytes.data(), bytes.size());
    return true;
  }

  // Consumer subscribe. `handler(const T&, ipc::ShmSlotView&)` runs with an
  // already-retained slot, RAII-released after it returns. Pool-aware, same as
  // subscribe(). The slot is fetched via retain_latest() at handler time, so
  // under async lag it may be a NEWER frame than the FrameHandle describes.
  template <typename T, typename HandlerFn>
  void subscribe_frame(HandlerFn &&handler) {
    static_assert(std::is_base_of_v<ipc::FrameHandle, T>,
                  "frame topic type must derive from ipc::FrameHandle");
    std::string key = Topic<T>::name;
    std::function<void(const T &)> h =
        [this, key, handler = std::forward<HandlerFn>(handler)](
            const T &fh) mutable {
          auto it = rings_.find(key);
          if (it == rings_.end()) {
            record_frame_drop();
            return; // no ring attached on this node
          }
          ipc::ShmSlotView view = it->second.retain_latest();
          if (!view) {
            record_frame_drop();
            return; // nothing ready, or producer already reclaimed it
          }
          handler(fh, view);
        };
    register_topic<T>(std::move(h));
  }

  // Frames dropped by subscribe_frame handlers (no ring attached, or nothing
  // ready at handler time) — observability for a real-time pipeline.
  uint64_t frame_drops() const noexcept {
    return frame_drops_->load(std::memory_order_relaxed);
  }

  // ---- get/set (daemon-backed store with a read-through cache) --------------
  // set<T> writes the authoritative value in the daemon. get<T> serves from a
  // local cache once warm; the first get for a key round-trips to the daemon
  // and also subscribes to future updates, so later sets by any node are pushed
  // in and cached — subsequent gets read locally with no round-trip.

  // A non-zero `ttl` makes the daemon delete the key that long after this set
  // and push the deletion to watchers, so "key present" means "the writer was
  // alive within the last ttl". A set without one clears any TTL the key had.
  template <typename T>
  void set(const std::string &key, const T &val,
           std::chrono::milliseconds ttl = std::chrono::milliseconds::zero()) {
    require_connected();
    uint32_t ms = ttl_to_wire(ttl);
    std::vector<char> bytes;
    if (ms)
      ipc::append_u32(bytes, ms);
    ipc::wire_codec<T>::encode(val, bytes);
    send(ms ? ipc::MSG_KV_SETEX : ipc::MSG_KV_SET, key, bytes.data(),
         bytes.size());
  }

  // How long a reply to incr/setnx is waited for. Unlike get/sync/info, these
  // verbs are new: a daemon predating them ignores the frame and never answers,
  // so the wait has to be bounded or the caller hangs for good. Matches the
  // Python client's `timeout=5.0`.
  static constexpr auto REPLY_TIMEOUT = std::chrono::seconds(5);

  // Counter/sequencer primitive: add `by` to the key's value in the daemon and
  // return the new total. It happens under the daemon's lock, so concurrent
  // nodes can't lose an increment the way a get/modify/set pair would. The
  // value is a native-endian int64 in exactly 8 bytes — get<int64_t>() reads
  // it — and an absent key counts as 0. nullopt means the key holds something
  // that isn't one of those (nothing was changed), or the link is down.
  std::optional<int64_t> incr(const std::string &key, int64_t by = 1) {
    require_connected();
    uint32_t id = next_req_.fetch_add(1);
    auto fut = register_pending(id);
    std::vector<char> pl;
    ipc::append_u32(pl, id);
    ipc::wire_codec<int64_t>::encode(by, pl);
    if (!send(ipc::MSG_KV_INCR, key, pl.data(), pl.size()))
      fulfill(id, std::nullopt); // disconnected: don't block on a dropped request
    if (fut.wait_for(REPLY_TIMEOUT) != std::future_status::ready) {
      fulfill(id, std::nullopt); // unregister, or pending_ grows per timeout
      return std::nullopt;
    }
    auto reply = fut.get();
    int64_t out;
    if (!reply ||
        !ipc::wire_codec<int64_t>::decode(reply->data(), reply->size(), out))
      return std::nullopt;
    return out;
  }

  // Lock / leader-election primitive: set the key only if it is unset, and
  // return whether this node was the one that won it. Pass a `ttl` — it is what
  // stops a holder that dies without del()ing the key from deadlocking every
  // other node forever.
  template <typename T>
  bool setnx(const std::string &key, const T &val,
             std::chrono::milliseconds ttl = std::chrono::milliseconds::zero()) {
    require_connected();
    uint32_t id = next_req_.fetch_add(1);
    auto fut = register_pending(id);
    std::vector<char> pl;
    ipc::append_u32(pl, id);
    ipc::append_u32(pl, ttl_to_wire(ttl));
    ipc::wire_codec<T>::encode(val, pl);
    if (!send(ipc::MSG_KV_SETNX, key, pl.data(), pl.size()))
      fulfill(id, std::nullopt);
    if (fut.wait_for(REPLY_TIMEOUT) != std::future_status::ready) {
      fulfill(id, std::nullopt);
      return false;
    }
    // has_value(), not a truth test: a win carries zero value bytes.
    return fut.get().has_value();
  }

  // Remove the key from the daemon's store. Every watcher (this node included)
  // is pushed the deletion and drops its cached value.
  void del(const std::string &key) {
    require_connected();
    send(ipc::MSG_KV_DEL, key, nullptr, 0);
  }

  template <typename T> std::optional<T> get(const std::string &key) {
    require_connected();
    { // warm path: once watched, the local cache is authoritative for this key
      std::lock_guard<std::mutex> lk(kv_mu_);
      if (kv_watched_.count(key))
        return decode_cached<T>(key);
    }
    // cold path: request the value + start watching, then block for the reply.
    uint32_t id = next_req_.fetch_add(1);
    auto fut = register_pending(id);
    std::vector<char> pl;
    ipc::append_u32(pl, id);
    if (!send(ipc::MSG_KV_GET, key, pl.data(), pl.size()))
      fulfill(id, std::nullopt); // disconnected: don't block on a dropped request

    auto reply = fut.get(); // caching happens on the reader thread, in order
    if (!reply)
      return std::nullopt;
    T out{};
    if (!ipc::wire_codec<T>::decode(reply->data(), reply->size(), out))
      return std::nullopt;
    return out;
  }

  // Drain the thread pool — useful in tests to wait for async handlers.
  void drain() {
    if (pool_)
      pool_->drain();
  }

  const std::string &name() const { return name_; }

private:
  template <typename T> static std::string ring_shm_name() {
    // POSIX shm names take a single leading '/'; a '/' inside the name would
    // imply a nonexistent subdir. Topics may be '/'-hierarchical, so map inner
    // '/' to '_'. ponytail: "cam/front" and "cam_front" would collide — don't
    // name two topics that way.
    std::string s = std::string("/broker_") + Topic<T>::name;
    for (size_t i = 1; i < s.size(); i++)
      if (s[i] == '/')
        s[i] = '_';
    return s;
  }

  void record_frame_drop() noexcept {
    frame_drops_->fetch_add(1, std::memory_order_relaxed);
  }

  void require_connected() const {
    if (!started_)
      throw std::logic_error("node is not connected to a broker");
  }

  // Returns true if the frame was written. A false means we're in a reconnect
  // window (fd_ < 0): fire-and-forget publishes are simply dropped; request/
  // reply callers (get/sync) use the return to avoid blocking on a lost frame.
  bool send(uint8_t kind, const std::string &name, const char *payload,
            size_t plen) {
    auto buf = ipc::build_frame(kind, name, payload, plen);
    std::lock_guard<std::mutex> lk(send_mu_);
    if (fd_ < 0)
      return false;
    return ipc::write_full(fd_, buf.data(), buf.size());
  }

  template <typename T> void register_topic(std::function<void(const T &)> user) {
    static_assert(Topic<T>::name != nullptr,
                  "subscribe needs MAKE_TOPIC(T, \"...\")");
    std::string name = Topic<T>::name;
    Receiver recv = [user = std::move(user)](const char *d, size_t n) {
      T msg{};
      if (ipc::wire_codec<T>::decode(d, n, msg))
        user(msg);
    };
    bool first;
    {
      std::lock_guard<std::mutex> lk(subs_mu_);
      auto &v = subs_[name];
      first = v.empty();
      v.push_back(std::move(recv));
    }
    if (first)
      send(ipc::MSG_SUBSCRIBE, name, nullptr, 0);
  }

  // The wire carries a ttl as u32 milliseconds, so narrow it here instead of
  // letting a cast do it quietly: a negative ttl would come out as ~49 days,
  // and one of 2^32 ms as 0 — which the daemon reads as "clear the ttl", the
  // exact opposite of what was asked for.
  static uint32_t ttl_to_wire(std::chrono::milliseconds ttl) {
    if (ttl.count() <= 0)
      return 0;
    return static_cast<uint32_t>(std::min<int64_t>(
        ttl.count(), std::numeric_limits<uint32_t>::max()));
  }

  std::future<std::optional<std::vector<char>>> register_pending(uint32_t id) {
    auto prom = std::make_shared<KvPromise>();
    auto fut = prom->get_future();
    std::lock_guard<std::mutex> lk(pending_mu_);
    pending_[id] = std::move(prom);
    return fut;
  }

  template <typename T> std::optional<T> decode_cached(const std::string &key) {
    auto it = kv_cache_.find(key);
    if (it == kv_cache_.end())
      return std::nullopt;
    T out{};
    if (!ipc::wire_codec<T>::decode(it->second.data(), it->second.size(), out))
      return std::nullopt;
    return out;
  }

  void dispatch(const std::string &name, std::vector<char> &payload) {
    std::vector<Receiver> fns;
    std::vector<PatternReceiver> pfns; // pattern handlers matching this topic
    {
      std::lock_guard<std::mutex> lk(subs_mu_);
      auto it = subs_.find(name);
      if (it != subs_.end())
        fns = it->second;
      for (const auto &ps : pattern_subs_)
        if (ipc::topic_matches(ps.first, name))
          pfns.insert(pfns.end(), ps.second.begin(), ps.second.end());
    }
    if (fns.empty() && pfns.empty())
      return;
    auto buf = std::make_shared<std::vector<char>>(std::move(payload));
    for (auto &fn : fns) {
      if (pool_)
        pool_->enqueue([fn, buf] { fn(buf->data(), buf->size()); });
      else
        fn(buf->data(), buf->size());
    }
    for (auto &fn : pfns) {
      if (pool_)
        pool_->enqueue([fn, name, buf] { fn(name, buf->data(), buf->size()); });
      else
        fn(name, buf->data(), buf->size());
    }
  }

  void handle_kv_reply(ipc::Frame &f) {
    if (f.payload.size() < 5)
      return;
    uint32_t id;
    std::memcpy(&id, f.payload.data(), 4);
    bool found = f.payload[4] != 0;
    std::optional<std::vector<char>> val;
    if (found)
      val.emplace(f.payload.begin() + 5, f.payload.end());
    { // cache on the reader thread so reply/update ordering stays consistent
      std::lock_guard<std::mutex> lk(kv_mu_);
      kv_watched_.insert(f.name);
      if (found)
        kv_cache_[f.name] = *val;
      else
        kv_cache_.erase(f.name);
    }
    fulfill(id, std::move(val));
  }

  void handle_kv_update(ipc::Frame &f) {
    std::lock_guard<std::mutex> lk(kv_mu_);
    kv_watched_.insert(f.name);
    kv_cache_[f.name] = f.payload;
  }

  void fulfill(uint32_t id, std::optional<std::vector<char>> val) {
    std::shared_ptr<KvPromise> prom;
    {
      std::lock_guard<std::mutex> lk(pending_mu_);
      auto it = pending_.find(id);
      if (it == pending_.end())
        return;
      prom = std::move(it->second);
      pending_.erase(it);
    }
    prom->set_value(std::move(val));
  }

  void fail_pending() {
    std::lock_guard<std::mutex> lk(pending_mu_);
    for (auto &e : pending_)
      e.second->set_value(std::nullopt);
    pending_.clear();
  }

  // Connection manager: read frames until the link drops, then reconnect to the
  // same address and resubscribe, until the node is closed. The first fd comes
  // from connect(); later ones from reconnect_backoff().
  void run() {
    int fd = fd_; // established by connect()
    int rejected_ms = 10;
    while (running_.load()) {
      auto opened = std::chrono::steady_clock::now();
      read_until_closed(fd);

      { // link is down — tear down the fd and unblock any in-flight request
        std::lock_guard<std::mutex> lk(send_mu_);
        connected_.store(false);
        if (fd_ >= 0)
          ::close(fd_);
        fd_ = -1;
      }
      { // The daemon's watches died with the link, so the cache stops being
        // authoritative here rather than after the reconnect: for a TTL'd key
        // "still cached" would otherwise mean "still alive" for the whole
        // outage. A get() in the window misses, fails to send, and reports the
        // key absent — the safe answer for a heartbeat or a lock.
        std::lock_guard<std::mutex> lk(kv_mu_);
        kv_cache_.clear();
        kv_watched_.clear();
      }
      fail_pending();
      if (!running_.load())
        return;

      // A link that dropped as soon as it opened means the daemon hung up on
      // us — an access-control denial looks exactly like this — and connect()
      // keeps succeeding, so nothing else in this loop would ever sleep. Back
      // off, or the reject cycle spins a core here and floods the daemon's
      // accept thread, which is the one admitting everyone else.
      if (std::chrono::steady_clock::now() - opened < std::chrono::seconds(1)) {
        backoff_sleep(rejected_ms);
        rejected_ms = std::min(rejected_ms * 2, 1000);
      } else {
        rejected_ms = 10;
      }

      fd = reconnect_backoff();
      if (fd < 0)
        return; // closed while backing off
      {
        std::lock_guard<std::mutex> lk(send_mu_);
        fd_ = fd;
        connected_.store(true);
      }
      resubscribe();
    }
  }

  void read_until_closed(int fd) {
    ipc::FrameReader reader(fd);
    ipc::Frame f;
    while (running_.load() && reader.next(f)) {
      switch (f.kind) {
      case ipc::MSG_PUBLISH:
        dispatch(f.name, f.payload);
        break;
      case ipc::MSG_KV_REPLY:
        handle_kv_reply(f);
        break;
      case ipc::MSG_KV_UPDATE:
        handle_kv_update(f);
        break;
      case ipc::MSG_KV_RESULT: {
        // Unlike MSG_KV_REPLY this must not cache or mark the key watched: the
        // daemon registered no watcher for an incr/setnx, so a cached value
        // would never be refreshed and every later get() would read it.
        if (f.payload.size() < 5)
          break;
        uint32_t id;
        std::memcpy(&id, f.payload.data(), 4);
        std::optional<std::vector<char>> val;
        if (f.payload[4])
          val.emplace(f.payload.begin() + 5, f.payload.end());
        fulfill(id, std::move(val));
        break;
      }
      case ipc::MSG_KV_DEL: {
        // Stay in kv_watched_: the cache is still authoritative for this key, so
        // a later get() reads "absent" locally instead of round-tripping.
        std::lock_guard<std::mutex> lk(kv_mu_);
        kv_cache_.erase(f.name);
        break;
      }
      case ipc::MSG_PONG:
        if (f.payload.size() >= 4) {
          uint32_t id;
          std::memcpy(&id, f.payload.data(), 4);
          fulfill(id, std::nullopt);
        }
        break;
      case ipc::MSG_INFO:
        if (f.payload.size() >= 4) {
          uint32_t id;
          std::memcpy(&id, f.payload.data(), 4);
          fulfill(id, std::vector<char>(f.payload.begin() + 4, f.payload.end()));
        }
        break;
      default:
        break;
      }
    }
  }

  // Retry unix_connect with exponential backoff (10ms -> 1s cap), bailing out
  // promptly if the node is closed mid-wait. Returns the new fd, or -1 on close.
  // Slept in short slices so close() isn't held up for the whole delay.
  void backoff_sleep(int delay_ms) {
    for (int slept = 0; slept < delay_ms && running_.load(); slept += 20)
      std::this_thread::sleep_for(
          std::chrono::milliseconds(std::min(20, delay_ms - slept)));
  }

  int reconnect_backoff() {
    int delay_ms = 10;
    while (running_.load()) {
      int fd = ipc::unix_connect(path_);
      if (fd >= 0)
        return fd;
      backoff_sleep(delay_ms);
      delay_ms = std::min(delay_ms * 2, 1000);
    }
    return -1;
  }

  // Re-establish server-side state after a reconnect: re-send SUBSCRIBE for
  // every topic we hold a handler for. The KV cache was already dropped when
  // the link went down, so the next get() cold-fetches and re-registers its
  // watch (values pushed only after re-watching would otherwise be missed).
  void resubscribe() {
    std::vector<std::string> topics;
    {
      std::lock_guard<std::mutex> lk(subs_mu_);
      for (const auto &kv : subs_)
        topics.push_back(kv.first);
      for (const auto &kv : pattern_subs_) // wildcards resubscribe too
        topics.push_back(kv.first);
    }
    for (const auto &t : topics)
      send(ipc::MSG_SUBSCRIBE, t, nullptr, 0);
  }

  std::string name_;
  std::string path_;             // address to (re)connect to
  bool started_ = false;         // connect() succeeded at least once
  int fd_ = -1;                  // current socket, -1 during a reconnect window
  std::atomic<bool> running_{false};
  std::atomic<bool> connected_{false};
  std::mutex send_mu_;           // guards fd_ and serializes writes

  // rings_ + frame_drops_ before pool_: queued frame tasks touch them as the
  // pool drains during destruction, so they must outlive it.
  std::unordered_map<std::string, ipc::SharedMemoryRing> rings_;
  std::unique_ptr<std::atomic<uint64_t>> frame_drops_ =
      std::make_unique<std::atomic<uint64_t>>(0);
  std::unique_ptr<ThreadPool> pool_;

  std::mutex subs_mu_; // guards subs_ and pattern_subs_
  std::unordered_map<std::string, std::vector<Receiver>> subs_;
  std::unordered_map<std::string, std::vector<PatternReceiver>> pattern_subs_;

  std::mutex kv_mu_;
  std::unordered_map<std::string, std::vector<char>> kv_cache_;
  std::unordered_set<std::string> kv_watched_;

  std::mutex pending_mu_;
  std::unordered_map<uint32_t, std::shared_ptr<KvPromise>> pending_;
  std::atomic<uint32_t> next_req_{0};

  std::thread reader_; // joined explicitly in the destructor before members go
};
