#pragma once
#include <cerrno>
#include <cstdint>
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
};

// ---- Unix socket helpers --------------------------------------------------
// Transport is just a connected byte-stream fd; these produce one. Swap in a
// TCP variant later without touching the client or daemon.

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

inline bool read_full(int fd, char *p, size_t n) {
  size_t off = 0;
  while (off < n) {
    ssize_t k = ::recv(fd, p + off, n - off, 0);
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

inline void append_u32(std::vector<char> &buf, uint32_t v) {
  char b[4];
  std::memcpy(b, &v, 4);
  buf.insert(buf.end(), b, b + 4);
}

inline bool read_u32(int fd, uint32_t &v) {
  char b[4];
  if (!read_full(fd, b, 4))
    return false;
  std::memcpy(&v, b, 4);
  return true;
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

inline bool read_frame(int fd, Frame &out) {
  char kind;
  if (!read_full(fd, &kind, 1))
    return false;
  uint32_t nlen;
  if (!read_u32(fd, nlen))
    return false;
  out.name.assign(nlen, '\0');
  if (nlen && !read_full(fd, &out.name[0], nlen))
    return false;
  uint32_t plen;
  if (!read_u32(fd, plen))
    return false;
  out.payload.assign(plen, 0);
  if (plen && !read_full(fd, out.payload.data(), plen))
    return false;
  out.kind = static_cast<uint8_t>(kind);
  return true;
}

} // namespace ipc
