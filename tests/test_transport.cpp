// tests/test_transport.cpp — broker daemon routing + kv store over the wire.
#include "broker.hpp"
#include "broker_server.hpp"
#include "node.hpp"
#include "test.hpp"
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

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

namespace ipc {
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
} // namespace ipc

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
  return std::string("/tmp/broker_tp_") + tag + "_" +
         std::to_string(::getpid()) + ".sock";
}

// ---- Tests -----------------------------------------------------------------

// The daemon routes pod / list / string-composite payloads verbatim; codecs run
// only on the clients.
void test_transport_types() {
  auto p = sock_path("types");
  ipc::BrokerServer server(p);
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
  ipc::BrokerServer server(p);
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
  ipc::BrokerServer server(p);
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

// Retained publish is replayed to a subscriber that joins AFTER it — the fix
// for the publish-before-subscribe race. A non-retained publish is not.
void test_retained_delivery() {
  auto p = sock_path("retain");
  ipc::BrokerServer server(p);
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

// A frame whose declared length exceeds MAX_FRAME_BYTES must drop only that
// connection, not crash the daemon — other clients keep working.
void test_oversized_frame_drops_client() {
  auto p = sock_path("oversized");
  ipc::BrokerServer server(p);
  Node good("good");
  good.connect(p);
  Sink<Cmd> s;
  good.subscribe([&](const Cmd &m) { s.push(m); });
  good.sync();

  // Hand-craft a frame with a bogus 1 GB payload length and send it raw.
  int fd = ipc::unix_connect(p);
  ASSERT_TRUE(fd >= 0);
  std::vector<char> frame;
  frame.push_back((char)ipc::MSG_PUBLISH);
  ipc::append_u32(frame, 3);                 // name_len
  const char *nm = "cmd";
  frame.insert(frame.end(), nm, nm + 3);
  ipc::append_u32(frame, 1u << 30);          // payload_len = 1 GB (bogus)
  ipc::write_full(fd, frame.data(), frame.size());
  ::close(fd); // daemon should drop this client, not allocate 1 GB and die

  std::this_thread::sleep_for(std::chrono::milliseconds(50));

  // Daemon still alive and routing: a normal publish still reaches `good`.
  Node pub("pub");
  pub.connect(p);
  pub.publish(Cmd{123});
  ASSERT_TRUE(s.wait_count(1));
  ASSERT_EQ(s.at(0).value, 123);
}

int main() {
  test_case("daemon carries pod/list/string/composite", test_transport_types);
  test_case("publish is not echoed to the publisher", test_transport_no_echo);
  test_case("daemon-backed get/set with read-through cache", test_kv_store);
  test_case("get/set requires a connection", test_kv_requires_connection);
  test_case("retained publish replays to late subscriber", test_retained_delivery);
  test_case("oversized frame drops client, not daemon",
            test_oversized_frame_drops_client);
  return test_summary();
}
