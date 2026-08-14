// tests/test_transport.cpp — broker daemon routing + kv store over the wire.
#include "chappe.hpp"
#include "server.hpp"
#include "node.hpp"
#include "test.hpp"
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

using namespace chappe;

// ---- Messages --------------------------------------------------------------

struct Cmd {
  int32_t value;
}; // trivially copyable -> default codec
struct Imu {
  float ax, ay, az;
}; // trivially copyable
struct Flag {
  bool on;
}; // trivially copyable
struct Samples {
  std::vector<int32_t> v;
}; // dynamic list -> user codec
struct Pose {
  int32_t id;
  std::string label;
}; // mixed composite -> user codec

MAKE_TOPIC(Cmd, "cmd");
MAKE_TOPIC(Imu, "imu");
MAKE_TOPIC(Flag, "flag");
MAKE_TOPIC(Samples, "samples");
MAKE_TOPIC(Pose, "pose");

// ---- User codecs for the composite types -----------------------------------

namespace chappe {
template <> struct wire_codec<Samples> {
  static void encode(const Samples &s, std::vector<char> &out) {
    wire_codec<std::vector<int32_t>>::encode(s.v, out);
  }
  static bool decode(const char *d, size_t n, Samples &out) {
    return wire_codec<std::vector<int32_t>>::decode(d, n, out.v);
  }
};
template <> struct wire_codec<Pose> {
  static void encode(const Pose &p, std::vector<char> &out) {
    const char *pid = reinterpret_cast<const char *>(&p.id);
    out.insert(out.end(), pid, pid + sizeof(p.id));
    out.insert(out.end(), p.label.begin(), p.label.end());
  }
  static bool decode(const char *d, size_t n, Pose &out) {
    if (n < sizeof(int32_t))
      return false;
    std::memcpy(&out.id, d, sizeof(int32_t));
    out.label.assign(d + sizeof(int32_t), n - sizeof(int32_t));
    return true;
  }
};
} // namespace chappe

// ---- test-side inbox -------------------------------------------------------

template <class T> struct Sink {
  std::mutex m;
  std::condition_variable cv;
  std::vector<T> got;
  void push(const T &v) {
    {
      std::lock_guard<std::mutex> l(m);
      got.push_back(v);
    }
    cv.notify_all();
  }
  bool wait_count(size_t n) {
    std::unique_lock<std::mutex> l(m);
    return cv.wait_for(l, std::chrono::seconds(2),
                       [&] { return got.size() >= n; });
  }
  T at(size_t i) {
    std::lock_guard<std::mutex> l(m);
    return got[i];
  }
  size_t size() {
    std::lock_guard<std::mutex> l(m);
    return got.size();
  }
};

static std::string sock_path(const char *tag) {
  return std::string("/tmp/chappe_tp_") + tag + "_" +
         std::to_string(::getpid()) + ".sock";
}

// ---- Tests -----------------------------------------------------------------

// The daemon routes pod / list / string-composite payloads verbatim; codecs run
// only on the clients.
void test_transport_types() {
  auto p = sock_path("types");
  chappe::Server server(p);
  Node a("a");
  Node b("b");
  a.connect(p);
  b.connect(p);

  Sink<Cmd> s_cmd;
  Sink<Imu> s_imu;
  Sink<Flag> s_flag;
  Sink<Samples> s_samp;
  Sink<Pose> s_pose;
  b.subscribe([&](const Cmd &m) { s_cmd.push(m); });
  b.subscribe([&](const Imu &m) { s_imu.push(m); });
  b.subscribe([&](const Flag &m) { s_flag.push(m); });
  b.subscribe([&](const Samples &m) { s_samp.push(m); });
  b.subscribe([&](const Pose &m) { s_pose.push(m); });
  b.sync();

  a.publish(Cmd{42});
  a.publish(Imu{1.5f, 2.5f, 9.8f});
  a.publish(Flag{true});
  a.publish(Samples{{1, 2, 3, 4, 5}});
  a.publish(Pose{7, "front-left"});

  ASSERT_TRUE(s_cmd.wait_count(1));
  ASSERT_EQ(s_cmd.at(0).value, 42);
  ASSERT_TRUE(s_imu.wait_count(1));
  ASSERT_EQ(s_imu.at(0).az, 9.8f);
  ASSERT_TRUE(s_flag.wait_count(1));
  ASSERT_EQ(s_flag.at(0).on, true);
  ASSERT_TRUE(s_samp.wait_count(1));
  Samples got = s_samp.at(0);
  ASSERT_EQ(got.v.size(), (size_t)5);
  ASSERT_EQ(got.v[4], 5);
  ASSERT_TRUE(s_pose.wait_count(1));
  ASSERT_EQ(s_pose.at(0).id, 7);
  ASSERT_EQ(s_pose.at(0).label, std::string("front-left"));
}

// noLocal: a publisher that also subscribes to a topic does not receive its own
// publishes; other subscribers do.
void test_transport_no_echo() {
  auto p = sock_path("echo");
  chappe::Server server(p);
  Node a("a");
  Node b("b");
  a.connect(p);
  b.connect(p);

  std::atomic<int> a_got{0};
  Sink<Cmd> b_sink;
  a.subscribe([&](const Cmd &) { a_got++; });
  b.subscribe([&](const Cmd &c) { b_sink.push(c); });
  a.sync();
  b.sync();

  a.publish(Cmd{1});

  ASSERT_TRUE(b_sink.wait_count(1)); // peer received it
  ASSERT_EQ(b_sink.at(0).value, 1);
  std::this_thread::sleep_for(std::chrono::milliseconds(50)); // let any echo land
  ASSERT_EQ(a_got.load(), 0);        // publisher did not
}

// Store lives in the daemon: cold get round-trips, later sets are pushed into
// the reader's cache so warm gets are local; unknown key is nullopt.
void test_kv_store() {
  auto p = sock_path("kv");
  chappe::Server server(p);
  Node w("writer");
  Node r("reader");
  w.connect(p);
  r.connect(p);

  w.set<float>("speed", 3.5f);
  w.sync(); // SET is applied before the reader's cold get

  auto v = r.get<float>("speed"); // cold: round-trip to daemon
  ASSERT_TRUE(v.has_value());
  ASSERT_EQ(*v, 3.5f);

  auto self = w.get<float>("speed"); // writer reads its own value back
  ASSERT_TRUE(self.has_value());
  ASSERT_EQ(*self, 3.5f);

  w.set<float>("speed", 9.0f); // pushed to the watching reader

  std::optional<float> v2;
  for (int i = 0; i < 400; i++) {
    v2 = r.get<float>("speed"); // warm: reads cache, updated by the push
    if (v2 && *v2 == 9.0f)
      break;
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }
  ASSERT_TRUE(v2.has_value());
  ASSERT_EQ(*v2, 9.0f);

  w.set<std::string>("mode", "race");
  w.sync();
  auto m = r.get<std::string>("mode");
  ASSERT_TRUE(m.has_value());
  ASSERT_EQ(*m, std::string("race"));

  ASSERT_TRUE(!r.get<int32_t>("nope").has_value()); // unknown key
}

void test_kv_requires_connection() {
  Node n("lone");
  bool threw = false;
  try {
    n.set<int32_t>("k", 1);
  } catch (const std::logic_error &) {
    threw = true;
  }
  ASSERT_TRUE(threw);
}

// info() returns a daemon status snapshot reflecting current subscriptions,
// patterns, and kv keys.
void test_info() {
  auto p = sock_path("info");
  chappe::Server server(p);
  Node a("a");
  Node b("b");
  a.connect(p);
  b.connect(p);
  b.subscribe([](const Cmd &) {});
  b.subscribe_pattern("cam/*", [](const std::string &, const char *, size_t) {});
  b.set<int32_t>("k", 1);
  b.sync(); // b's SUBSCRIBE/KV_SET are processed before a queries

  std::string s = a.info();
  ASSERT_TRUE(s.find("clients: 2") != std::string::npos);
  ASSERT_TRUE(s.find("cmd=1") != std::string::npos);       // exact topic listed
  ASSERT_TRUE(s.find("patterns: 1") != std::string::npos); // one wildcard sub
  ASSERT_TRUE(s.find("kv_keys: 1") != std::string::npos);
}

// Retained publish is replayed to a subscriber that joins AFTER it — the fix
// for the publish-before-subscribe race. A non-retained publish is not.
void test_retained_delivery() {
  auto p = sock_path("retain");
  chappe::Server server(p);
  Node pub("pub");
  pub.connect(p);

  pub.publish(Cmd{7}, /*retain=*/true); // retained: stored by the daemon
  pub.publish(Imu{1, 2, 3});            // not retained: gone once routed
  pub.sync();                           // both processed before we subscribe

  Node late("late");
  late.connect(p);
  Sink<Cmd> s_cmd;
  Sink<Imu> s_imu;
  late.subscribe([&](const Cmd &m) { s_cmd.push(m); });
  late.subscribe([&](const Imu &m) { s_imu.push(m); });

  ASSERT_TRUE(s_cmd.wait_count(1));     // retained Cmd replayed on subscribe
  ASSERT_EQ(s_cmd.at(0).value, 7);

  late.sync();                          // fence: any Imu replay would be here
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  ASSERT_EQ(s_imu.size(), (size_t)0);   // non-retained Imu was NOT replayed

  // A later retained publish overwrites the stored value.
  pub.publish(Cmd{9}, /*retain=*/true);
  ASSERT_TRUE(s_cmd.wait_count(2));     // live delivery to the now-subscriber
  Node late2("late2");
  late2.connect(p);
  Sink<Cmd> s2;
  late2.subscribe([&](const Cmd &m) { s2.push(m); });
  ASSERT_TRUE(s2.wait_count(1));
  ASSERT_EQ(s2.at(0).value, 9);         // newest retained value, not 7
}

// Wildcard pattern subscriptions: '+' matches one level, '*' the rest.
void test_pattern_subscribe() {
  auto p = sock_path("pattern");
  chappe::Server server(p);
  Node pub("pub");
  Node sub("sub");
  pub.connect(p);
  sub.connect(p);

  std::mutex m;
  std::vector<std::string> star_hits, plus_hits;
  // "cam/*" matches cam and everything under it; "cam/+" only one level under.
  sub.subscribe_pattern("cam/*", [&](const std::string &topic, const char *, size_t) {
    std::lock_guard<std::mutex> l(m);
    star_hits.push_back(topic);
  });
  sub.subscribe_pattern("cam/+", [&](const std::string &topic, const char *, size_t) {
    std::lock_guard<std::mutex> l(m);
    plus_hits.push_back(topic);
  });
  sub.sync();

  // Publish arbitrary topic strings (typed publish is exact-topic only) by
  // sending raw PUBLISH frames.
  auto raw_publish = [&](const std::string &topic) {
    int fd = chappe::unix_connect(p);
    ASSERT_TRUE(fd >= 0);
    auto frame = chappe::build_frame(chappe::MSG_PUBLISH, topic, "x", 1);
    chappe::write_full(fd, frame.data(), frame.size());
    ::close(fd);
  };
  raw_publish("cam/front");        // matches cam/* and cam/+
  raw_publish("cam/front/left");   // matches cam/* only (two levels under)
  raw_publish("lidar/top");        // matches neither

  auto count = [&](std::vector<std::string> &v, size_t n) {
    for (int i = 0; i < 200; i++) {
      { std::lock_guard<std::mutex> l(m); if (v.size() >= n) break; }
      std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    std::lock_guard<std::mutex> l(m);
    return v.size();
  };

  ASSERT_EQ(count(star_hits, 2), (size_t)2); // cam/front + cam/front/left
  std::this_thread::sleep_for(std::chrono::milliseconds(30));
  {
    std::lock_guard<std::mutex> l(m);
    ASSERT_EQ(plus_hits.size(), (size_t)1); // only cam/front (single level)
    ASSERT_EQ(plus_hits[0], std::string("cam/front"));
    ASSERT_EQ(star_hits.size(), (size_t)2);
  }
}

// A node survives a daemon restart: after the daemon dies and a new one binds
// the same address, the node reconnects and resubscribes, and delivery resumes.
void test_reconnect_resubscribe() {
  auto p = sock_path("reconnect");
  auto server = std::make_unique<chappe::Server>(p);

  Node sub("sub");
  Node pub("pub");
  sub.connect(p);
  pub.connect(p);

  std::atomic<int> got{0};
  sub.subscribe([&](const Cmd &) { got.fetch_add(1); });
  sub.sync();

  pub.publish(Cmd{1});
  for (int i = 0; i < 200 && got.load() == 0; i++)
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  ASSERT_EQ(got.load(), 1); // baseline delivery works

  // Kill the daemon; both nodes' readers detect the drop and start reconnecting.
  server.reset();
  for (int i = 0; i < 200 && (sub.connected() || pub.connected()); i++)
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  ASSERT_TRUE(!sub.connected()); // observed the disconnect

  // Bring the daemon back on the same address.
  server = std::make_unique<chappe::Server>(p);

  // Once both have reconnected (and sub has resubscribed), a fresh publish must
  // reach the handler. Retry to absorb the async reconnect/resubscribe ordering.
  int before = got.load();
  bool delivered = false;
  for (int i = 0; i < 600 && !delivered; i++) {
    pub.publish(Cmd{2}); // dropped until pub reconnects, then routed
    for (int j = 0; j < 4 && !delivered; j++) {
      if (got.load() > before)
        delivered = true;
      else
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
  }
  ASSERT_TRUE(delivered); // resubscribed after reconnect, delivery resumed
}

// A frame whose declared length exceeds MAX_FRAME_BYTES must drop only that
// connection, not crash the daemon — other clients keep working.
void test_oversized_frame_drops_client() {
  auto p = sock_path("oversized");
  chappe::Server server(p);
  Node good("good");
  good.connect(p);
  Sink<Cmd> s;
  good.subscribe([&](const Cmd &m) { s.push(m); });
  good.sync();

  // Hand-craft a frame with a bogus 1 GB payload length and send it raw.
  int fd = chappe::unix_connect(p);
  ASSERT_TRUE(fd >= 0);
  std::vector<char> frame;
  frame.push_back((char)chappe::MSG_PUBLISH);
  chappe::append_u32(frame, 3);                 // name_len
  const char *nm = "cmd";
  frame.insert(frame.end(), nm, nm + 3);
  chappe::append_u32(frame, 1u << 30);          // payload_len = 1 GB (bogus)
  chappe::write_full(fd, frame.data(), frame.size());
  ::close(fd); // daemon should drop this client, not allocate 1 GB and die

  std::this_thread::sleep_for(std::chrono::milliseconds(50));

  // Daemon still alive and routing: a normal publish still reaches `good`.
  Node pub("pub");
  pub.connect(p);
  pub.publish(Cmd{123});
  ASSERT_TRUE(s.wait_count(1));
  ASSERT_EQ(s.at(0).value, 123);
}

// The cross-device link speaks the same frames over TCP, so the framing has to
// survive a stream that arrives in arbitrary chunks rather than a tidy unix
// datagram-ish one. Loopback, ephemeral port, no daemon involved.
void test_tcp_round_trip() {
  int srv = chappe::tcp_listen("127.0.0.1", 0);
  ASSERT_TRUE(srv >= 0);
  sockaddr_in bound{};
  socklen_t blen = sizeof(bound);
  ASSERT_EQ(::getsockname(srv, reinterpret_cast<sockaddr *>(&bound), &blen), 0);
  uint16_t port = ntohs(bound.sin_port);
  timeval tv{5, 0}; // accept must fail the test, never hang the whole suite
  ::setsockopt(srv, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

  std::string big(70000, 'z'); // spans several reads, so the reader must buffer
  std::atomic<int> wfd{-1};    // asserted after the join: the counters in
  std::thread writer([&] {     // test.hpp are plain ints, not thread-safe
    int fd = chappe::tcp_connect("127.0.0.1", port);
    wfd.store(fd);
    if (fd < 0)
      return;
    auto f1 = chappe::build_frame(chappe::MSG_PUBLISH, "cam/front", "abc", 3);
    auto f2 = chappe::build_frame(chappe::MSG_KV_SET, "k", big.data(), big.size());
    chappe::write_full(fd, f1.data(), f1.size());
    chappe::write_full(fd, f2.data(), f2.size());
    ::close(fd);
  });

  int c = chappe::tcp_accept(srv);
  ASSERT_TRUE(c >= 0);
  chappe::FrameReader reader(c);
  chappe::Frame f;
  ASSERT_TRUE(reader.next(f));
  ASSERT_EQ((int)f.kind, (int)chappe::MSG_PUBLISH);
  ASSERT_EQ(f.name, std::string("cam/front"));
  ASSERT_EQ(std::string(f.payload.begin(), f.payload.end()), std::string("abc"));
  ASSERT_TRUE(reader.next(f));
  ASSERT_EQ((int)f.kind, (int)chappe::MSG_KV_SET);
  ASSERT_EQ(f.payload.size(), big.size());
  ASSERT_TRUE(std::string(f.payload.begin(), f.payload.end()) == big);

  writer.join();
  ASSERT_TRUE(wfd.load() >= 0);
  ::close(c);
  ::close(srv);
}

// Nagle would hold small frames back waiting for more to coalesce, which is
// exactly wrong for a control bus — so the option is not optional.
void test_tcp_sets_nodelay() {
  int srv = chappe::tcp_listen("127.0.0.1", 0);
  ASSERT_TRUE(srv >= 0);
  sockaddr_in bound{};
  socklen_t blen = sizeof(bound);
  ::getsockname(srv, reinterpret_cast<sockaddr *>(&bound), &blen);
  timeval tv{5, 0}; // accept must fail the test, never hang the whole suite
  ::setsockopt(srv, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

  int fd = chappe::tcp_connect("127.0.0.1", ntohs(bound.sin_port));
  ASSERT_TRUE(fd >= 0);
  int c = chappe::tcp_accept(srv);
  ASSERT_TRUE(c >= 0);
  for (int side : {fd, c}) { // the accepted socket doesn't inherit it
    int on = 0;
    socklen_t len = sizeof(on);
    ASSERT_EQ(::getsockopt(side, IPPROTO_TCP, TCP_NODELAY, &on, &len), 0);
    ASSERT_TRUE(on != 0);
    int alive = 0;
    len = sizeof(alive);
    ::getsockopt(side, SOL_SOCKET, SO_KEEPALIVE, &alive, &len);
    ASSERT_TRUE(alive != 0);
  }
  ::close(fd);
  ::close(c);
  ::close(srv);
}

// A link pointed at a port nobody is listening on must report failure, not hand
// back a broken fd the caller would then read garbage from.
void test_tcp_connect_refused() {
  int srv = chappe::tcp_listen("127.0.0.1", 0);
  ASSERT_TRUE(srv >= 0);
  sockaddr_in bound{};
  socklen_t blen = sizeof(bound);
  ::getsockname(srv, reinterpret_cast<sockaddr *>(&bound), &blen);
  uint16_t port = ntohs(bound.sin_port);
  ::close(srv); // now nothing is listening there

  ASSERT_EQ(chappe::tcp_connect("127.0.0.1", port), -1);
}

// A C++ node can read what a Python node published: the payload is bytes like
// any other, wearing an envelope json_payload() recognises. Byte-for-byte what
// python/chappe/__init__.py produces — test_chappe.py holds the two magics
// together, this holds the C++ half's behaviour.
void test_json_payload() {
  std::string serialized = std::string("\xc7" "ch\x01", 4) + R"({"gear": 3})";
  auto body = chappe::json_payload(serialized);
  ASSERT_TRUE(body.has_value());
  ASSERT_EQ(std::string(*body), std::string(R"({"gear": 3})"));

  // A raw payload is not one: a POD, a frame handle, or bytes sent as bytes.
  int32_t pod = 42;
  ASSERT_TRUE(!chappe::json_payload(reinterpret_cast<const char *>(&pod),
                                    sizeof(pod)).has_value());
  ASSERT_TRUE(!chappe::json_payload(std::string("plain")).has_value());

  // Shorter than the magic must not read past the end.
  ASSERT_TRUE(!chappe::json_payload(std::string("\xc7" "c", 2)).has_value());
  // An empty body is still an envelope, not a miss.
  auto empty = chappe::json_payload(std::string("\xc7" "ch\x01", 4));
  ASSERT_TRUE(empty.has_value());
  ASSERT_EQ(empty->size(), (size_t)0);
}

int main() {
  test_case("json_payload finds a python value, ignores raw bytes",
            test_json_payload);
  test_case("daemon carries pod/list/string/composite", test_transport_types);
  test_case("publish is not echoed to the publisher", test_transport_no_echo);
  test_case("daemon-backed get/set with read-through cache", test_kv_store);
  test_case("get/set requires a connection", test_kv_requires_connection);
  test_case("info() reports daemon status", test_info);
  test_case("retained publish replays to late subscriber", test_retained_delivery);
  test_case("wildcard pattern subscriptions", test_pattern_subscribe);
  test_case("node reconnects and resubscribes after daemon restart",
            test_reconnect_resubscribe);
  test_case("oversized frame drops client, not daemon",
            test_oversized_frame_drops_client);
  test_case("frames round-trip over tcp", test_tcp_round_trip);
  test_case("tcp sockets get nodelay and keepalive", test_tcp_sets_nodelay);
  test_case("tcp connect to a dead port fails", test_tcp_connect_refused);
  return test_summary();
}
