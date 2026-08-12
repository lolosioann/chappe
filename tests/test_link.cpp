// tests/test_link.cpp — the cross-device link, with both "devices" in one
// process: two daemons on two unix sockets, two links joined by a socketpair
// standing in for the TCP hop (transport.hpp's TCP helpers are covered on their
// own in test_transport.cpp; what matters here is the forwarding logic).
#include "broker.hpp"
#include "broker_server.hpp"
#include "link.hpp"
#include "node.hpp"
#include "test.hpp"
#include <atomic>
#include <chrono>
#include <memory>
#include <string>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>

struct Cmd {
  int32_t value;
};
MAKE_TOPIC(Cmd, "cmd/stop");

struct Priv {
  int32_t value;
};
MAKE_TOPIC(Priv, "private/thing");

static std::string sock_path(const char *tag) {
  return std::string("/tmp/broker_link_") + tag + "_" +
         std::to_string(::getpid()) + ".sock";
}

static bool wait_until(const std::function<bool()> &pred, int timeout_ms = 2000) {
  for (int waited = 0; waited < timeout_ms; waited += 10) {
    if (pred())
      return true;
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  return pred();
}

// Two devices, each with its own daemon and link. Everything a test needs to
// say is "a on device A, b on device B".
struct TwoDevices {
  std::string pa, pb;
  std::unique_ptr<ipc::BrokerServer> da, db;
  std::unique_ptr<ipc::BrokerLink> la, lb;

  // The two sides take separate configs on purpose: the link's filters only
  // ever bite on what ARRIVES from the peer. Outbound is already limited by
  // what it subscribed and which keys it watches, so a filter test needs the
  // far side to forward something this side never agreed to carry.
  TwoDevices(const char *tag, std::vector<std::string> topics,
             std::vector<std::string> keys,
             std::vector<std::string> btopics = {},
             std::vector<std::string> bkeys = {}) {
    pa = sock_path((std::string(tag) + "a").c_str());
    pb = sock_path((std::string(tag) + "b").c_str());
    da = std::make_unique<ipc::BrokerServer>(pa);
    db = std::make_unique<ipc::BrokerServer>(pb);
    int sv[2];
    ::socketpair(AF_UNIX, SOCK_STREAM, 0, sv);
    ipc::BrokerLink::Config ca{pa, topics, keys},
        cb{pb, btopics.empty() ? topics : btopics,
           bkeys.empty() ? keys : bkeys};
    la = std::make_unique<ipc::BrokerLink>(ca, sv[0]);
    lb = std::make_unique<ipc::BrokerLink>(cb, sv[1]);
  }
};

// A publish on one device reaches a subscriber on the other, and the link
// carries it in both directions.
void test_link_forwards_publish() {
  TwoDevices d("pub", {"cmd/*"}, {});
  Node a("a"), b("b");
  a.connect(d.pa);
  b.connect(d.pb);

  std::atomic<int> a_got{0}, b_got{0};
  a.subscribe([&](const Cmd &c) { a_got.store(c.value); });
  b.subscribe([&](const Cmd &c) { b_got.store(c.value); });
  a.sync();
  b.sync();

  a.publish(Cmd{7});
  ASSERT_TRUE(wait_until([&] { return b_got.load() == 7; }));
  b.publish(Cmd{9});
  ASSERT_TRUE(wait_until([&] { return a_got.load() == 9; }));
}

// A topic the receiving side never agreed to carry is dropped on arrival, even
// though the sending side happily forwards it.
void test_link_ignores_unlisted_topic() {
  TwoDevices d("filter", {"cmd/*", "private/*"}, {}, {"cmd/*"}, {});
  Node a("a"), b("b");
  a.connect(d.pa);
  b.connect(d.pb);

  std::atomic<int> b_priv{0}, b_cmd{0};
  b.subscribe([&](const Priv &) { b_priv.fetch_add(1); });
  b.subscribe([&](const Cmd &c) { b_cmd.store(c.value); });
  b.sync();

  a.publish(Priv{1});  // device A's link forwards it; device B's must not
  a.publish(Cmd{2});   // the control: proves the link is up and carrying
  ASSERT_TRUE(wait_until([&] { return b_cmd.load() == 2; }));
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  ASSERT_EQ(b_priv.load(), 0);
}

// A key set on one device shows up on the other, in both directions over the
// same pair of links, and settles rather than drifting.
void test_link_forwards_kv() {
  TwoDevices d("kv", {}, {"state/mode"});
  Node a("a"), b("b");
  a.connect(d.pa);
  b.connect(d.pb);

  a.set<std::string>("state/mode", "race");
  ASSERT_TRUE(wait_until([&] {
    return b.get<std::string>("state/mode").value_or("") == "race";
  }));

  b.set<std::string>("state/mode", "idle");
  ASSERT_TRUE(wait_until([&] {
    return a.get<std::string>("state/mode").value_or("") == "idle";
  }));

  std::this_thread::sleep_for(std::chrono::milliseconds(300));
  ASSERT_EQ(a.get<std::string>("state/mode").value_or(""), std::string("idle"));
  ASSERT_EQ(b.get<std::string>("state/mode").value_or(""), std::string("idle"));
}

// The real test of the echo suppression. A forwarded echo bounces the key
// between devices carrying the SAME value each time, so the settled state looks
// perfectly fine while traffic runs forever — the symptom is volume, not a
// wrong answer. So count updates: one set must produce a handful, not a stream.
void test_link_kv_does_not_ping_pong() {
  TwoDevices d("pingpong", {}, {"state/mode"});
  Node a("a");
  a.connect(d.pa);

  // Raw watcher on device A: the get registers the watch, and every later
  // change then arrives as its own KV_UPDATE frame, which is what we count.
  int fd = ipc::unix_connect(d.pa);
  ASSERT_TRUE(fd >= 0);
  timeval tv{0, 200000}; // a quiet gap is what ends the count
  ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

  std::vector<char> pl;
  ipc::append_u32(pl, 1);
  auto req =
      ipc::build_frame(ipc::MSG_KV_GET, "state/mode", pl.data(), pl.size());
  ipc::write_full(fd, req.data(), req.size());

  // Wait for the reply before setting anything. These are separate connections,
  // so the daemon is free to apply the set below before it ever registers this
  // watch — in which case the update we are counting would never be sent.
  ipc::FrameReader reader(fd);
  ipc::Frame f;
  ASSERT_TRUE(reader.next(f));
  ASSERT_EQ((int)f.kind, (int)ipc::MSG_KV_REPLY);

  a.set<std::string>("state/mode", "race");

  int updates = 0;
  auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(1);
  while (std::chrono::steady_clock::now() < deadline && reader.next(f))
    if (f.kind == ipc::MSG_KV_UPDATE)
      updates++;
  ::close(fd);

  ASSERT_TRUE(updates >= 1); // the set itself reached the watcher
  ASSERT_TRUE(updates <= 3); // a loop would run until the deadline
}

// A key already set before the links came up is seeded across, because the
// link's startup get is answered with the current value.
void test_link_seeds_existing_key() {
  auto pa = sock_path("seeda"), pb = sock_path("seedb");
  ipc::BrokerServer da(pa), db(pb);
  Node a("a");
  a.connect(pa);
  a.set<std::string>("state/mode", "preset");
  a.sync();

  int sv[2];
  ::socketpair(AF_UNIX, SOCK_STREAM, 0, sv);
  ipc::BrokerLink::Config ca{pa, {}, {"state/mode"}}, cb{pb, {}, {"state/mode"}};
  ipc::BrokerLink la(ca, sv[0]), lb(cb, sv[1]);

  Node b("b");
  b.connect(pb);
  ASSERT_TRUE(wait_until([&] {
    return b.get<std::string>("state/mode").value_or("") == "preset";
  }));
}

// A deletion crosses too — which is also how a ttl expiry reaches the far side,
// since the daemon pushes expiry to watchers as a KV_DEL.
void test_link_forwards_delete() {
  TwoDevices d("del", {}, {"state/mode"});
  Node a("a"), b("b");
  a.connect(d.pa);
  b.connect(d.pb);

  a.set<std::string>("state/mode", "race");
  ASSERT_TRUE(wait_until([&] {
    return b.get<std::string>("state/mode").value_or("") == "race";
  }));
  a.del("state/mode");
  ASSERT_TRUE(wait_until(
      [&] { return !b.get<std::string>("state/mode").has_value(); }));
}

// Same for keys: the receiving link drops a set for a key it was not told to
// carry, however eagerly the far side forwards it.
void test_link_ignores_unlisted_key() {
  TwoDevices d("keyfilter", {}, {"state/mode", "state/secret"}, {},
               {"state/mode"});
  Node a("a"), b("b");
  a.connect(d.pa);
  b.connect(d.pb);

  a.set<std::string>("state/secret", "hidden");
  a.set<std::string>("state/mode", "race"); // the control, forwarded by both
  ASSERT_TRUE(wait_until([&] {
    return b.get<std::string>("state/mode").value_or("") == "race";
  }));
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  ASSERT_TRUE(!b.get<std::string>("state/secret").has_value());
}

int main() {
  test_case("a publish crosses the link both ways", test_link_forwards_publish);
  test_case("an unlisted topic stays on its device",
            test_link_ignores_unlisted_topic);
  test_case("a kv set crosses the link both ways", test_link_forwards_kv);
  test_case("a forwarded key does not ping-pong",
            test_link_kv_does_not_ping_pong);
  test_case("a key set before the link is seeded across",
            test_link_seeds_existing_key);
  test_case("a kv delete crosses the link", test_link_forwards_delete);
  test_case("an unlisted key stays on its device",
            test_link_ignores_unlisted_key);
  return test_summary();
}
