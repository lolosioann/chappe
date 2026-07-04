// tests/test_transport.cpp
#include "broker.hpp"
#include "ipc/transport.hpp"
#include "test.hpp"
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <mutex>
#include <string>
#include <sys/socket.h>
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
struct Ack {
  int32_t n;
}; // used to fence the no-echo test

MAKE_TOPIC(Cmd, "cmd");
MAKE_TOPIC(Imu, "imu");
MAKE_TOPIC(Flag, "flag");
MAKE_TOPIC(Samples, "samples");
MAKE_TOPIC(Pose, "pose");
MAKE_TOPIC(Ack, "ack");

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

using Bus = Broker<Cmd, Imu, Flag, Samples, Pose, Ack>;

// ---- Tests -----------------------------------------------------------------

void test_transport_types() {
  int sv[2];
  ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, sv), 0);

  Bus a, b;
  ipc::SocketBridge<Cmd, Imu, Flag, Samples, Pose, Ack> bridgeA(a, sv[0]);
  ipc::SocketBridge<Cmd, Imu, Flag, Samples, Pose, Ack> bridgeB(b, sv[1]);
  bridgeA.forward<Cmd>();
  bridgeA.forward<Imu>();
  bridgeA.forward<Flag>();
  bridgeA.forward<Samples>();
  bridgeA.forward<Pose>();

  Sink<Cmd> s_cmd;
  Sink<Imu> s_imu;
  Sink<Flag> s_flag;
  Sink<Samples> s_samp;
  Sink<Pose> s_pose;
  auto g1 = b.subscribe<Cmd>([&](const Cmd &m) { s_cmd.push(m); });
  auto g2 = b.subscribe<Imu>([&](const Imu &m) { s_imu.push(m); });
  auto g3 = b.subscribe<Flag>([&](const Flag &m) { s_flag.push(m); });
  auto g4 = b.subscribe<Samples>([&](const Samples &m) { s_samp.push(m); });
  auto g5 = b.subscribe<Pose>([&](const Pose &m) { s_pose.push(m); });

  a.publish(Cmd{42});
  a.publish(Imu{1.5f, 2.5f, 9.8f});
  a.publish(Flag{true});
  a.publish(Samples{{1, 2, 3, 4, 5}});
  a.publish(Pose{7, "front-left"});

  ASSERT_TRUE(s_cmd.wait_count(1));
  ASSERT_EQ(s_cmd.at(0).value, 42);

  ASSERT_TRUE(s_imu.wait_count(1));
  ASSERT_EQ(s_imu.at(0). az, 9.8f);

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

// Both ends forward Cmd. Publishing on A must reach B exactly once and must NOT
// echo back to A. Fenced deterministically: B emits an Ack in reaction (a
// *different* topic, so it forwards), and once A sees the Ack the in-order
// socket guarantees any echo would already have arrived.
void test_transport_no_echo() {
  int sv[2];
  ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, sv), 0);

  Bus a, b;
  ipc::SocketBridge<Cmd, Imu, Flag, Samples, Pose, Ack> bridgeA(a, sv[0]);
  ipc::SocketBridge<Cmd, Imu, Flag, Samples, Pose, Ack> bridgeB(b, sv[1]);
  bridgeA.forward<Cmd>();
  bridgeA.forward<Ack>();
  bridgeB.forward<Cmd>();
  bridgeB.forward<Ack>();

  int a_cmd = 0;
  auto ga = a.subscribe<Cmd>([&](const Cmd &) { a_cmd++; });
  Sink<Ack> s_ack;
  auto gack = a.subscribe<Ack>([&](const Ack &m) { s_ack.push(m); });
  // B reacts to a received Cmd by emitting an Ack.
  auto gb = b.subscribe<Cmd>([&](const Cmd &) { b.publish(Ack{99}); });

  a.publish(Cmd{1}); // local delivery -> a_cmd == 1; forwarded to B

  ASSERT_TRUE(s_ack.wait_count(1)); // B got the Cmd and its Ack came back
  ASSERT_EQ(s_ack.at(0).n, 99);
  ASSERT_EQ(a_cmd, 1); // no echo of Cmd looped back to A
}

int main() {
  test_case("transport carries pod/list/string/composite", test_transport_types);
  test_case("bidirectional forward does not echo", test_transport_no_echo);
  return test_summary();
}
