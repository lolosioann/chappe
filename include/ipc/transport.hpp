#pragma once
#include "broker.hpp"
#include <atomic>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <functional>
#include <mutex>
#include <string>
#include <thread>
#include <type_traits>
#include <unordered_map>
#include <vector>

#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

namespace ipc {

// ---- wire codec -----------------------------------------------------------
// Turns a message into bytes and back. The default handles any trivially
// copyable type (ints, floats, bools, flat POD structs, std::array) as raw
// bytes — same as FrameHandle's transport story. For anything with heap/
// pointers (std::string, std::vector, nested), specialize wire_codec<T>.
// The frame carries the payload length, so codecs don't length-prefix a single
// trailing variable field themselves.

template <typename T> struct wire_codec {
  static_assert(std::is_trivially_copyable_v<T>,
                "no wire_codec<T>: T is not trivially copyable — specialize "
                "wire_codec<T> to serialize it");
  static void encode(const T &v, std::vector<char> &out) {
    const char *p = reinterpret_cast<const char *>(&v);
    out.insert(out.end(), p, p + sizeof(T));
  }
  static bool decode(const char *data, size_t n, T &out) {
    if (n != sizeof(T))
      return false;
    std::memcpy(&out, data, sizeof(T));
    return true;
  }
};

// std::string — payload is the raw characters.
template <> struct wire_codec<std::string> {
  static void encode(const std::string &v, std::vector<char> &out) {
    out.insert(out.end(), v.begin(), v.end());
  }
  static bool decode(const char *data, size_t n, std::string &out) {
    out.assign(data, n);
    return true;
  }
};

// std::vector<T> for trivially-copyable T — payload is the packed elements.
template <typename T> struct wire_codec<std::vector<T>> {
  static_assert(std::is_trivially_copyable_v<T>,
                "wire_codec<vector<T>>: element T must be trivially copyable — "
                "specialize for element types that aren't");
  static void encode(const std::vector<T> &v, std::vector<char> &out) {
    const char *p = reinterpret_cast<const char *>(v.data());
    out.insert(out.end(), p, p + v.size() * sizeof(T));
  }
  static bool decode(const char *data, size_t n, std::vector<T> &out) {
    if (n % sizeof(T) != 0)
      return false;
    out.resize(n / sizeof(T));
    if (n)
      std::memcpy(out.data(), data, n);
    return true;
  }
};

// ---- Unix socket helpers --------------------------------------------------
// Transport is just a connected byte-stream fd; these produce one. Swap in a
// TCP variant later without touching SocketBridge.

inline int unix_connect(const std::string &path) {
  int fd = ::socket(AF_UNIX, SOCK_STREAM, 0);
  if (fd < 0)
    return -1;
  sockaddr_un addr{};
  addr.sun_family = AF_UNIX;
  std::strncpy(addr.sun_path, path.c_str(), sizeof(addr.sun_path) - 1);
  if (::connect(fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) != 0) {
    ::close(fd);
    return -1;
  }
  return fd;
}

inline int unix_listen(const std::string &path) {
  ::unlink(path.c_str());
  int fd = ::socket(AF_UNIX, SOCK_STREAM, 0);
  if (fd < 0)
    return -1;
  sockaddr_un addr{};
  addr.sun_family = AF_UNIX;
  std::strncpy(addr.sun_path, path.c_str(), sizeof(addr.sun_path) - 1);
  if (::bind(fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) != 0 ||
      ::listen(fd, 1) != 0) {
    ::close(fd);
    return -1;
  }
  return fd;
}

inline int unix_accept(int listen_fd) { return ::accept(listen_fd, nullptr, nullptr); }

// Topic this thread is currently injecting from the wire (or nullptr). Used so
// the forwarding subscription skips echoing that exact topic straight back out,
// while still forwarding *other* topics a handler may publish in reaction.
inline thread_local const char *tls_injecting_topic = nullptr;

// ---- SocketBridge ---------------------------------------------------------
// Mirrors selected topics of a Broker across a connected socket. forward<T>()
// makes local publishes of T go to the peer; incoming frames for any named
// topic in the pack are deserialized and published locally. Wire framing:
//   [u32 topic_len][topic][u32 payload_len][payload]
// u32s are native-endian — fine same-host/same-arch; use network order if this
// ever spans architectures.
template <typename... Topics> class SocketBridge {
public:
  SocketBridge(Broker<Topics...> &broker, int fd) : broker_(broker), fd_(fd) {
    (this->template register_receiver<Topics>(), ...);
    reader_ = std::thread([this] { read_loop(); });
  }

  ~SocketBridge() {
    guards_.clear();             // stop forwarding local publishes
    running_.store(false);
    ::shutdown(fd_, SHUT_RDWR);  // unblock the reader's recv
    if (reader_.joinable())
      reader_.join();
    ::close(fd_);
  }

  SocketBridge(const SocketBridge &) = delete;
  SocketBridge &operator=(const SocketBridge &) = delete;

  // Forward topic T outbound. Call during setup (not thread-safe vs itself).
  template <typename T> void forward() {
    static_assert(Topic<T>::name != nullptr,
                  "forward<T>() needs MAKE_TOPIC(T, \"...\")");
    guards_.emplace_back(broker_.template subscribe<T>([this](const T &msg) {
      if (tls_injecting_topic == Topic<T>::name)
        return; // don't echo the very topic we just injected from the wire
      std::vector<char> payload;
      wire_codec<T>::encode(msg, payload);
      send_frame(Topic<T>::name, payload);
    }));
  }

private:
  template <typename T> void register_receiver() {
    if constexpr (Topic<T>::name != nullptr) {
      receivers_[Topic<T>::name] = [this](const char *data, size_t n) {
        T msg;
        if (!wire_codec<T>::decode(data, n, msg))
          return;
        tls_injecting_topic = Topic<T>::name;
        try {
          broker_.publish(msg);
        } catch (...) {
          // a bad local subscriber must not kill the transport
        }
        tls_injecting_topic = nullptr;
      };
    }
  }

  void send_frame(const char *topic, const std::vector<char> &payload) {
    uint32_t tlen = static_cast<uint32_t>(std::strlen(topic));
    uint32_t plen = static_cast<uint32_t>(payload.size());
    std::vector<char> buf;
    buf.reserve(8 + tlen + plen);
    append_u32(buf, tlen);
    buf.insert(buf.end(), topic, topic + tlen);
    append_u32(buf, plen);
    buf.insert(buf.end(), payload.begin(), payload.end());
    std::lock_guard<std::mutex> lk(send_mu_);
    write_full(buf.data(), buf.size());
  }

  void read_loop() {
    while (running_.load()) {
      uint32_t tlen, plen;
      if (!read_u32(tlen))
        break;
      std::string topic(tlen, '\0');
      if (tlen && !read_full(&topic[0], tlen))
        break;
      if (!read_u32(plen))
        break;
      std::vector<char> payload(plen);
      if (plen && !read_full(payload.data(), plen))
        break;
      auto it = receivers_.find(topic);
      if (it != receivers_.end())
        it->second(payload.data(), plen);
    }
  }

  bool write_full(const char *p, size_t n) {
    size_t off = 0;
    while (off < n) {
      ssize_t k = ::send(fd_, p + off, n - off, MSG_NOSIGNAL);
      if (k < 0) {
        if (errno == EINTR)
          continue;
        return false;
      }
      if (k == 0)
        return false;
      off += static_cast<size_t>(k);
    }
    return true;
  }

  bool read_full(char *p, size_t n) {
    size_t off = 0;
    while (off < n) {
      ssize_t k = ::recv(fd_, p + off, n - off, 0);
      if (k < 0) {
        if (errno == EINTR)
          continue;
        return false;
      }
      if (k == 0)
        return false; // peer closed
      off += static_cast<size_t>(k);
    }
    return true;
  }

  bool read_u32(uint32_t &v) {
    char b[4];
    if (!read_full(b, 4))
      return false;
    std::memcpy(&v, b, 4);
    return true;
  }

  static void append_u32(std::vector<char> &buf, uint32_t v) {
    char b[4];
    std::memcpy(b, &v, 4);
    buf.insert(buf.end(), b, b + 4);
  }

  Broker<Topics...> &broker_;
  int fd_;
  std::mutex send_mu_;
  std::atomic<bool> running_{true};
  std::unordered_map<std::string, std::function<void(const char *, size_t)>>
      receivers_;
  std::vector<SubscriptionGuard> guards_;
  std::thread reader_;
};

} // namespace ipc
