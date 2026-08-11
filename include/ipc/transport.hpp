#pragma once
#include <cerrno>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <string>
#include <type_traits>
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

// ---- frame kinds ----------------------------------------------------------
// The client<->daemon protocol. name/payload meaning is per-kind (see the
// table below); everything rides the same [kind][name][payload] frame.
//   SUBSCRIBE/UNSUBSCRIBE  name=topic         payload=—
//   PUBLISH                name=topic         payload=message bytes
//   KV_SET                 name=key           payload=value bytes
//   KV_GET                 name=key           payload=[u32 req_id]
//   KV_REPLY               name=key           payload=[u32 req_id][u8 found][value]
//   KV_UPDATE              name=key           payload=value bytes (push to watchers)
//   KV_DEL                 name=key           payload=—  (both directions)
//   PING/PONG              name=—             payload=[u32 req_id]  (round-trip barrier)
enum : uint8_t {
  MSG_SUBSCRIBE = 0,
  MSG_UNSUBSCRIBE = 1,
  MSG_PUBLISH = 2,
  MSG_KV_SET = 3,
  MSG_KV_GET = 4,
  MSG_KV_REPLY = 5,
  MSG_KV_UPDATE = 6,
  MSG_PING = 7,
  MSG_PONG = 8,
  // Like MSG_PUBLISH but the daemon also stores the payload as the topic's
  // retained last-value and replays it to future subscribers. Client->daemon
  // only; the daemon routes it onward (and replays it) as a plain MSG_PUBLISH.
  // A zero-length payload clears the retained value instead of storing one
  // (MQTT convention) and still routes onward as an empty publish.
  MSG_PUBLISH_RETAIN = 9,
  // Introspection. Client->daemon: payload=[u32 req_id]. Daemon->client reply:
  // payload=[u32 req_id][status text].
  MSG_INFO = 10,
  // Client->daemon: erase the key, then push the same frame to its watchers.
  // Daemon->client: the key is gone; drop the cached value but keep watching.
  MSG_KV_DEL = 11,
};

// Upper bound on a single frame's name or payload length. A peer sending a
// bogus/huge length must not make the reader allocate gigabytes and take the
// whole daemon down with a bad_alloc — over this, we drop the connection.
// Generous: only control/metadata crosses the socket (pixels go via shm), so
// real frames are KB-scale; 64 MB is pure headroom.
static constexpr uint32_t MAX_FRAME_BYTES = 64u * 1024 * 1024;

// ---- topic patterns -------------------------------------------------------
// Bash-path-like wildcards over '/'-separated topic levels (e.g. "cam/front",
// "sensor/imu/accel"): '+' matches exactly one level, '*' matches the remaining
// levels (zero or more) and is only meaningful as the last level. A subscribe
// whose topic contains a wildcard is a pattern sub; the same matcher runs on
// the daemon (which subscribers to route to) and the client (which local
// handlers to dispatch to).

inline std::vector<std::string> topic_levels(const std::string &s) {
  std::vector<std::string> out;
  size_t start = 0;
  while (true) {
    size_t sep = s.find('/', start);
    if (sep == std::string::npos) {
      out.push_back(s.substr(start));
      break;
    }
    out.push_back(s.substr(start, sep - start));
    start = sep + 1;
  }
  return out;
}

inline bool topic_has_wildcard(const std::string &s) {
  return s.find('+') != std::string::npos || s.find('*') != std::string::npos;
}

inline bool topic_matches(const std::string &pattern, const std::string &topic) {
  auto p = topic_levels(pattern);
  auto t = topic_levels(topic);
  size_t i = 0;
  for (; i < p.size(); ++i) {
    if (p[i] == "*")
      return true; // matches all remaining levels, including none
    if (i >= t.size())
      return false; // pattern has more levels than the topic
    if (p[i] == "+")
      continue; // matches exactly this one level
    if (p[i] != t[i])
      return false;
  }
  return i == t.size(); // both fully consumed
}

// ---- Unix socket helpers --------------------------------------------------
// Transport is just a connected byte-stream fd; these produce one. Swap in a
// TCP variant later without touching the client or daemon.

// Well-known broker socket. Both the daemon and its clients default to this, so
// the common case never names a path; $BROKER_SOCKET overrides it per-deployment.
inline std::string default_broker_addr() {
  const char *env = ::getenv("BROKER_SOCKET");
  return env ? std::string(env) : std::string("/tmp/broker.sock");
}

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

inline int unix_listen(const std::string &path, int backlog = 128) {
  ::unlink(path.c_str());
  int fd = ::socket(AF_UNIX, SOCK_STREAM, 0);
  if (fd < 0)
    return -1;
  sockaddr_un addr{};
  addr.sun_family = AF_UNIX;
  std::strncpy(addr.sun_path, path.c_str(), sizeof(addr.sun_path) - 1);
  if (::bind(fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) != 0 ||
      ::listen(fd, backlog) != 0) {
    ::close(fd);
    return -1;
  }
  return fd;
}

inline int unix_accept(int listen_fd) {
  return ::accept(listen_fd, nullptr, nullptr);
}

// ---- framing --------------------------------------------------------------
// Wire frame: [u8 kind][u32 name_len][name][u32 payload_len][payload].
// u32s are native-endian — fine same-host/same-arch; use network order if this
// ever spans architectures.

struct Frame {
  uint8_t kind;
  std::string name;
  std::vector<char> payload;
};

inline bool write_full(int fd, const char *p, size_t n) {
  size_t off = 0;
  while (off < n) {
    ssize_t k = ::send(fd, p + off, n - off, MSG_NOSIGNAL);
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

inline void append_u32(std::vector<char> &buf, uint32_t v) {
  char b[4];
  std::memcpy(b, &v, 4);
  buf.insert(buf.end(), b, b + 4);
}

// Build a complete frame in one buffer so a single locked write_full emits it
// atomically (no interleaving with a concurrent sender on the same fd).
inline std::vector<char> build_frame(uint8_t kind, const std::string &name,
                                     const char *payload, size_t plen) {
  std::vector<char> buf;
  buf.reserve(1 + 4 + name.size() + 4 + plen);
  buf.push_back(static_cast<char>(kind));
  append_u32(buf, static_cast<uint32_t>(name.size()));
  buf.insert(buf.end(), name.begin(), name.end());
  append_u32(buf, static_cast<uint32_t>(plen));
  if (plen)
    buf.insert(buf.end(), payload, payload + plen);
  return buf;
}

// Buffered frame reader. The old per-field read_frame did one recv() per field
// (5 syscalls/frame); this recv()s into a chunk and parses frames out of it, so
// the common case is ~1 syscall per frame — the dominant cost on the daemon's
// hot path. One reader per fd, owned by that fd's read loop: it may buffer bytes
// of the *next* frame, so nothing else may recv() on the same fd.
//
// ponytail: the buffer grows to the largest frame seen and never shrinks. Fine
// here — only control/metadata crosses the socket (pixels go via shm), so frames
// are small. Add a shrink-after-large if a huge one-off ever bloats it.
class FrameReader {
public:
  explicit FrameReader(int fd) : fd_(fd), buf_(4096) {}

  // Read one full frame into `out`. Returns false on EOF or socket error.
  bool next(Frame &out) {
    if (!require(1))
      return false;
    uint8_t kind = static_cast<uint8_t>(buf_[pos_++]);
    uint32_t nlen;
    if (!read_u32(nlen) || nlen > MAX_FRAME_BYTES)
      return false; // oversized/garbage length -> drop the connection
    if (!require(nlen))
      return false;
    out.name.assign(buf_.data() + pos_, nlen);
    pos_ += nlen;
    uint32_t plen;
    if (!read_u32(plen) || plen > MAX_FRAME_BYTES)
      return false;
    if (!require(plen))
      return false;
    out.payload.assign(buf_.begin() + pos_, buf_.begin() + pos_ + plen);
    pos_ += plen;
    out.kind = kind;
    return true;
  }

private:
  bool read_u32(uint32_t &v) {
    if (!require(4))
      return false;
    std::memcpy(&v, buf_.data() + pos_, 4);
    pos_ += 4;
    return true;
  }

  // Ensure at least `n` unparsed bytes sit contiguously at buf_[pos_].
  bool require(size_t n) {
    while (end_ - pos_ < n)
      if (!fill(n))
        return false;
    return true;
  }

  // Compact consumed bytes, grow to fit `need`, then recv() once.
  bool fill(size_t need) {
    if (pos_ > 0) {
      size_t rem = end_ - pos_;
      if (rem)
        std::memmove(buf_.data(), buf_.data() + pos_, rem);
      pos_ = 0;
      end_ = rem;
    }
    if (buf_.size() < need)
      buf_.resize(need);
    if (buf_.size() == end_)
      buf_.resize(buf_.size() * 2); // leave room for recv
    ssize_t k = ::recv(fd_, buf_.data() + end_, buf_.size() - end_, 0);
    if (k < 0)
      return errno == EINTR; // retry on next require() iteration
    if (k == 0)
      return false; // peer closed
    end_ += static_cast<size_t>(k);
    return true;
  }

  int fd_;
  std::vector<char> buf_;
  size_t pos_ = 0; // next unparsed byte
  size_t end_ = 0; // one past last valid byte
};

} // namespace ipc
