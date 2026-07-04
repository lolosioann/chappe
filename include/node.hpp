#pragma once
#include "broker.hpp"
#include "ipc/frame_handle.hpp"
#include "ipc/shm_ring.hpp"
#include "ipc/transport.hpp"
#include "threadpool.hpp"
#include <atomic>
#include <functional>
#include <memory>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <vector>

template <typename... Topics> class Node {
  std::string name_;
  Broker<Topics...> &broker_;
  // Frame rings + drop counter must outlive pool_: queued frame tasks touch
  // them while ~ThreadPool drains, so they are declared before pool_ and thus
  // destroyed after it.
  std::unordered_map<std::string, ipc::SharedMemoryRing> rings_;
  std::unique_ptr<std::atomic<uint64_t>> frame_drops_ =
      std::make_unique<std::atomic<uint64_t>>(0);
  std::unique_ptr<ThreadPool> pool_;      // destroyed after guards_
  std::vector<SubscriptionGuard> guards_; // destroyed before pool_
  // Declared last -> destroyed first: the bridge's reader thread stops
  // publishing into the broker before the rest of the node tears down.
  std::unique_ptr<ipc::SocketBridge<Topics...>> bridge_;

public:
  Node(std::string name, Broker<Topics...> &broker, size_t threads = 0)
      : name_(std::move(name)), broker_(broker),
        pool_(threads ? std::make_unique<ThreadPool>(threads) : nullptr) {}

  template <typename F> void subscribe(F &&handler) {
    using T = msg_t<std::decay_t<F>>;

    std::function<void(const T &)> h(std::forward<F>(handler));

    if (pool_) {
      h = [this, inner = std::move(h)](const T &msg) {
        pool_->enqueue([inner, msg] { inner(msg); });
      };
    }

    guards_.emplace_back(broker_.subscribe(std::move(h)));
  }

  template <typename T> void publish(const T &msg) { broker_.publish(msg); }

  // ---- frame (shm) API ------------------------------------------------------
  // A frame topic is a message type deriving from ipc::FrameHandle, registered
  // with MAKE_TOPIC. The backing ring's shm name is derived from Topic<T>::name,
  // so a producer's create_frame_ring<T>() and a consumer's attach_frame_ring<T>()
  // in another process agree on the segment with no shared config.

  // Producer: this node owns topic T's ring.
  template <typename T>
  void create_frame_ring(size_t slot_size, uint32_t num_slots) {
    static_assert(std::is_base_of_v<ipc::FrameHandle, T>,
                  "frame topic type must derive from ipc::FrameHandle");
    static_assert(Topic<T>::name != nullptr,
                  "frame topic type needs MAKE_TOPIC(T, \"...\")");
    rings_.emplace(Topic<T>::name, ipc::SharedMemoryRing::create(
                                       ring_shm_name<T>(), slot_size, num_slots));
  }

  // Consumer: attach to topic T's ring created by another process.
  template <typename T> void attach_frame_ring() {
    static_assert(std::is_base_of_v<ipc::FrameHandle, T>,
                  "frame topic type must derive from ipc::FrameHandle");
    static_assert(Topic<T>::name != nullptr,
                  "frame topic type needs MAKE_TOPIC(T, \"...\")");
    rings_.emplace(Topic<T>::name,
                   ipc::SharedMemoryRing::attach(ring_shm_name<T>()));
  }

  // Producer publish. `writer(void* data, size_t size)` fills the slot in place
  // (zero-copy — decode/render straight into it). Returns false if every slot is
  // held by a consumer (frame dropped). Throws if T has no ring registered.
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
    broker_.publish(msg);
    return true;
  }

  // Consumer subscribe. `handler(const T&, ipc::ShmSlotView&)` runs with an
  // already-retained slot, RAII-released after it returns. Pool-aware: with
  // worker threads the handler runs on the pool, same as subscribe(). The slot
  // is fetched via retain_latest() at handler time, so under async lag it may be
  // a NEWER frame than the FrameHandle metadata describes.
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

    if (pool_) {
      h = [this, inner = std::move(h)](const T &msg) {
        pool_->enqueue([inner, msg] { inner(msg); });
      };
    }
    guards_.emplace_back(broker_.template subscribe<T>(std::move(h)));
  }

  // Frames dropped by subscribe_frame handlers (no ring attached, or nothing
  // ready at handler time) — observability for a real-time pipeline.
  uint64_t frame_drops() const noexcept {
    return frame_drops_->load(std::memory_order_relaxed);
  }

  // ---- socket bridge API ----------------------------------------------------
  // Bridge this node's broker to a peer process over a stream socket. Topics
  // registered with bridge_forward<T>() are serialized to the peer; any named
  // topic arriving from the peer is published into this node's broker. One
  // bridge per node.

  // Attach an already-connected stream socket (Unix or TCP). Takes ownership.
  void bridge_attach(int fd) { set_bridge(fd); }

  // Connect to a peer already listening on `path`.
  void bridge_connect(const std::string &path) {
    int fd = ipc::unix_connect(path);
    if (fd < 0)
      throw std::runtime_error("bridge_connect failed for '" + path + "'");
    set_bridge(fd);
  }

  // Listen on `path` and block until one peer connects.
  void bridge_listen(const std::string &path) {
    int lfd = ipc::unix_listen(path);
    if (lfd < 0)
      throw std::runtime_error("bridge_listen failed for '" + path + "'");
    int fd = ipc::unix_accept(lfd);
    ::close(lfd);
    if (fd < 0)
      throw std::runtime_error("bridge_accept failed for '" + path + "'");
    set_bridge(fd);
  }

  // Forward local publishes of topic T to the bridged peer.
  template <typename T> void bridge_forward() {
    if (!bridge_)
      throw std::logic_error("bridge_forward: no bridge established");
    bridge_->template forward<T>();
  }

  bool bridged() const noexcept { return bridge_ != nullptr; }

  // drain the thread pool — useful in tests to wait for async handlers
  void drain() {
    if (pool_)
      pool_->drain();
  }

  const std::string &name() const { return name_; }

  Node(const Node &) = delete;
  Node &operator=(const Node &) = delete;
  Node(Node &&) = default;
  Node &operator=(Node &&) = default;

private:
  template <typename T> static std::string ring_shm_name() {
    return std::string("/broker_") + Topic<T>::name; // POSIX shm: leading '/'
  }
  void record_frame_drop() noexcept {
    frame_drops_->fetch_add(1, std::memory_order_relaxed);
  }
  void set_bridge(int fd) {
    if (fd < 0)
      throw std::runtime_error("bridge: invalid socket fd");
    if (bridge_) {
      ::close(fd);
      throw std::logic_error("node already has a bridge");
    }
    bridge_ = std::make_unique<ipc::SocketBridge<Topics...>>(broker_, fd);
  }
};
