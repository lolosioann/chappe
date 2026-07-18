#pragma once
#include "broker.hpp"
#include "ipc/frame_handle.hpp"
#include "ipc/shm_ring.hpp"
#include "ipc/transport.hpp"
#include "threadpool.hpp"
#include <atomic>
#include <cstring>
#include <functional>
#include <future>
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
  using KvPromise = std::promise<std::optional<std::vector<char>>>;

public:
  Node(std::string name, size_t threads = 0)
      : name_(std::move(name)),
        pool_(threads ? std::make_unique<ThreadPool>(threads) : nullptr) {}

  ~Node() {
    if (fd_ >= 0) {
      running_.store(false);
      ::shutdown(fd_, SHUT_RDWR); // unblock the reader's recv
      if (reader_.joinable())
        reader_.join();
      ::close(fd_);
    }
    // Members destruct in reverse declaration order after this body: pool_
    // (drains queued frame tasks) before rings_/frame_drops_, which those
    // tasks touch. reader_ is already joined, so nothing new is enqueued.
  }

  Node(const Node &) = delete;
  Node &operator=(const Node &) = delete;

  // Connect to a broker daemon listening on `path` and start the reader thread.
  void connect(const std::string &path) {
    if (fd_ >= 0)
      throw std::logic_error("node already connected");
    int fd = ipc::unix_connect(path);
    if (fd < 0)
      throw std::runtime_error("node connect failed for '" + path + "'");
    fd_ = fd;
    running_.store(true);
    reader_ = std::thread([this] { read_loop(); });
  }

  bool connected() const noexcept { return fd_ >= 0; }

  // ---- pub/sub --------------------------------------------------------------

  template <typename F> void subscribe(F &&handler) {
    using T = msg_t<std::decay_t<F>>;
    register_topic<T>(std::function<void(const T &)>(std::forward<F>(handler)));
  }

  template <typename T> void publish(const T &msg) {
    require_connected();
    static_assert(Topic<T>::name != nullptr,
                  "publish<T> needs MAKE_TOPIC(T, \"...\")");
    std::vector<char> bytes;
    ipc::wire_codec<T>::encode(msg, bytes);
    send(ipc::MSG_PUBLISH, Topic<T>::name, bytes.data(), bytes.size());
  }

  // Round-trip barrier: returns once the daemon has processed every frame this
  // node has sent so far. Handy after subscribe() to guarantee the daemon is
  // routing this node's topics before a peer starts publishing (no reconnect/
  // resubscribe in v1, so early publishes are otherwise lost).
  void sync() {
    require_connected();
    uint32_t id = next_req_.fetch_add(1);
    auto fut = register_pending(id);
    char b[4];
    std::memcpy(b, &id, 4);
    send(ipc::MSG_PING, "", b, 4);
    fut.get();
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
  // dropped). Throws if T has no ring registered.
  // NOTE: if `writer` throws, the slot is never published and stays WRITING —
  // the C layer has no abandon path, so that slot is permanently lost. Keep the
  // writer effectively noexcept.
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

    std::forward<WriterFn>(writer)(handle.data, handle.size);
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

  template <typename T> void set(const std::string &key, const T &val) {
    require_connected();
    std::vector<char> bytes;
    ipc::wire_codec<T>::encode(val, bytes);
    send(ipc::MSG_KV_SET, key, bytes.data(), bytes.size());
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
    send(ipc::MSG_KV_GET, key, pl.data(), pl.size());

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
    return std::string("/broker_") + Topic<T>::name; // POSIX shm: leading '/'
  }

  void record_frame_drop() noexcept {
    frame_drops_->fetch_add(1, std::memory_order_relaxed);
  }

  void require_connected() const {
    if (fd_ < 0)
      throw std::logic_error("node is not connected to a broker");
  }

  void send(uint8_t kind, const std::string &name, const char *payload,
            size_t plen) {
    auto buf = ipc::build_frame(kind, name, payload, plen);
    std::lock_guard<std::mutex> lk(send_mu_);
    ipc::write_full(fd_, buf.data(), buf.size());
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
    {
      std::lock_guard<std::mutex> lk(subs_mu_);
      auto it = subs_.find(name);
      if (it == subs_.end())
        return;
      fns = it->second;
    }
    auto buf = std::make_shared<std::vector<char>>(std::move(payload));
    for (auto &fn : fns) {
      if (pool_)
        pool_->enqueue([fn, buf] { fn(buf->data(), buf->size()); });
      else
        fn(buf->data(), buf->size());
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

  void read_loop() {
    ipc::Frame f;
    while (running_.load() && ipc::read_frame(fd_, f)) {
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
      case ipc::MSG_PONG:
        if (f.payload.size() >= 4) {
          uint32_t id;
          std::memcpy(&id, f.payload.data(), 4);
          fulfill(id, std::nullopt);
        }
        break;
      default:
        break;
      }
    }
    fail_pending(); // connection gone — unblock any waiting get()/sync()
  }

  std::string name_;
  int fd_ = -1;
  std::atomic<bool> running_{false};
  std::mutex send_mu_;

  // rings_ + frame_drops_ before pool_: queued frame tasks touch them as the
  // pool drains during destruction, so they must outlive it.
  std::unordered_map<std::string, ipc::SharedMemoryRing> rings_;
  std::unique_ptr<std::atomic<uint64_t>> frame_drops_ =
      std::make_unique<std::atomic<uint64_t>>(0);
  std::unique_ptr<ThreadPool> pool_;

  std::mutex subs_mu_;
  std::unordered_map<std::string, std::vector<Receiver>> subs_;

  std::mutex kv_mu_;
  std::unordered_map<std::string, std::vector<char>> kv_cache_;
  std::unordered_set<std::string> kv_watched_;

  std::mutex pending_mu_;
  std::unordered_map<uint32_t, std::shared_ptr<KvPromise>> pending_;
  std::atomic<uint32_t> next_req_{0};

  std::thread reader_; // joined explicitly in the destructor before members go
};
