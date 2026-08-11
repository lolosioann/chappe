// tests/test_node.cpp
#include "broker.hpp"
#include "broker_server.hpp"
#include "node.hpp"
#include "test.hpp"
#include <atomic>
#include <chrono>
#include <memory>
#include <string>
#include <sys/socket.h>
#include <sys/time.h>
#include <thread>
#include <unistd.h>
#include <vector>

// ---- Messages --------------------------------------------------------------

struct Cmd {
  int value;
};
struct Event {
  std::string name;
};

MAKE_TOPIC(Cmd, "cmd");
MAKE_TOPIC(Event, "event");

namespace ipc {
template <> struct wire_codec<Event> {
  static void encode(const Event &e, std::vector<char> &out) {
    out.insert(out.end(), e.name.begin(), e.name.end());
  }
  static bool decode(const char *d, size_t n, Event &out) {
    out.name.assign(d, n);
    return true;
  }
};
} // namespace ipc

static std::string sock_path(const char *tag) {
  return std::string("/tmp/broker_node_") + tag + "_" +
         std::to_string(::getpid()) + ".sock";
}

// spin until `pred` or the timeout; keeps tests robust against async delivery
template <typename P> static bool wait_until(P pred, int timeout_ms = 2000) {
  for (int i = 0; i < timeout_ms / 5; i++) {
    if (pred())
      return true;
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }
  return pred();
}

// ---- Tests -----------------------------------------------------------------

void test_node_name() {
  Node node("controller");
  ASSERT_EQ(node.name(), std::string("controller"));
}

void test_two_nodes_communicate() {
  auto p = sock_path("comm");
  ipc::BrokerServer server(p);
  Node sender("sender");
  Node receiver("receiver");
  sender.connect(p);
  receiver.connect(p);

  std::atomic<int> received{0};
  receiver.subscribe([&received](const Cmd &c) { received += c.value; });
  receiver.sync(); // subscription is live before we publish

  sender.publish(Cmd{5});
  sender.publish(Cmd{3});

  ASSERT_TRUE(wait_until([&] { return received.load() == 8; }));
}

void test_node_multiple_topic_subscriptions() {
  auto p = sock_path("multi");
  ipc::BrokerServer server(p);
  Node pub("pub");
  Node node("multi");
  pub.connect(p);
  node.connect(p);

  std::atomic<int> cmd_count{0};
  std::atomic<int> event_count{0};
  node.subscribe([&cmd_count](const Cmd &) { cmd_count++; });
  node.subscribe([&event_count](const Event &) { event_count++; });
  node.sync();

  pub.publish(Cmd{0});
  pub.publish(Cmd{0});
  pub.publish(Event{"e"});

  ASSERT_TRUE(wait_until([&] { return cmd_count.load() == 2; }));
  ASSERT_TRUE(wait_until([&] { return event_count.load() == 1; }));
}

void test_node_async_dispatch() {
  auto p = sock_path("async");
  ipc::BrokerServer server(p);
  Node pub("pub");
  Node node("async_node", 2); // 2 worker threads
  pub.connect(p);
  node.connect(p);

  std::atomic<int> count{0};
  node.subscribe([&count](const Cmd &c) { count += c.value; });
  node.sync();

  for (int i = 0; i < 10; i++)
    pub.publish(Cmd{1});

  ASSERT_TRUE(wait_until([&] { return count.load() == 10; }));
  node.drain();
  ASSERT_EQ(count.load(), 10);
}

// async node with slow handlers is destroyed while work is in flight — must
// join its pool cleanly, no hang or crash.
void test_async_node_teardown_clean() {
  auto p = sock_path("teardown");
  ipc::BrokerServer server(p);
  Node pub("pub");
  pub.connect(p);

  std::atomic<int> count{0};
  {
    Node node("async", 2);
    node.connect(p);
    node.subscribe([&count](const Cmd &) {
      std::this_thread::sleep_for(std::chrono::milliseconds(2));
      count++;
    });
    node.sync();

    for (int i = 0; i < 10; i++)
      pub.publish(Cmd{0});

    wait_until([&] { return count.load() == 10; });
  } // node destroyed → reader stops, pool joins cleanly

  ASSERT_EQ(count.load(), 10);
}

void test_publish_requires_connection() {
  Node n("lone");
  bool threw = false;
  try {
    n.publish(Cmd{1});
  } catch (const std::logic_error &) {
    threw = true;
  }
  ASSERT_TRUE(threw);
}

// A handler registered before connect() is flushed to the daemon by connect(),
// so the first publish after it is routed here.
void test_subscribe_before_connect() {
  auto p = sock_path("presub");
  ipc::BrokerServer server(p);
  Node sub("sub");
  Node pub("pub");

  std::atomic<int> got{0};
  sub.subscribe([&got](const Cmd &c) { got += c.value; }); // before connect()
  sub.connect(p);
  sub.sync(); // the flushed subscription is live before we publish
  pub.connect(p);

  pub.publish(Cmd{6});
  ASSERT_TRUE(wait_until([&] { return got.load() == 6; }));
}

// The daemon drops a topic once its last subscriber disconnects.
void test_topic_dropped_when_last_subscriber_leaves() {
  auto p = sock_path("reap");
  ipc::BrokerServer server(p);
  Node probe("probe");
  probe.connect(p);
  {
    Node sub("sub");
    sub.connect(p);
    sub.subscribe([](const Cmd &) {});
    sub.sync();
    std::string s = probe.info();
    ASSERT_TRUE(s.find("cmd=1") != std::string::npos);
    ASSERT_TRUE(s.find("topics: 1") != std::string::npos);
  } // sub disconnects; the daemon cleans up on its reader thread

  ASSERT_TRUE(wait_until([&] {
    std::string s = probe.info();
    return s.find("cmd=") == std::string::npos &&
           s.find("topics: 0") != std::string::npos;
  }));
}

// Same for the kv side: a get() registers a watcher, and the entry must go when
// its last watcher disconnects — not linger as an empty set for the daemon's
// life. kv_watchers counts entries in watchers_, so it distinguishes the two.
void test_watcher_dropped_when_last_watcher_leaves() {
  auto p = sock_path("watchreap");
  ipc::BrokerServer server(p);
  Node probe("probe");
  probe.connect(p);
  probe.set<int>("k", 1);
  {
    Node watcher("watcher");
    watcher.connect(p);
    ASSERT_EQ(watcher.get<int>("k").value(), 1); // cold get starts the watch
    ASSERT_TRUE(probe.info().find("kv_watchers: 1") != std::string::npos);
  } // watcher disconnects

  ASSERT_TRUE(wait_until([&] {
    return probe.info().find("kv_watchers: 0") != std::string::npos;
  }));
  ASSERT_TRUE(probe.info().find("kv_keys: 1") != std::string::npos); // key stays
}

// unsubscribe()/unsubscribe_pattern() stop delivery and clear the daemon-side
// subscription.
void test_unsubscribe() {
  auto p = sock_path("unsub");
  ipc::BrokerServer server(p);
  Node pub("pub");
  Node sub("sub");
  pub.connect(p);
  sub.connect(p);

  std::atomic<int> got{0};
  sub.subscribe([&got](const Cmd &) { got++; });
  sub.sync();
  pub.publish(Cmd{1});
  ASSERT_TRUE(wait_until([&] { return got.load() == 1; }));

  sub.unsubscribe<Cmd>();
  sub.sync();
  pub.publish(Cmd{2});
  // Fence: pub's PONG means the daemon routed that publish, and sub's PONG is
  // written after anything it would have been routed — so nothing is in flight.
  pub.sync();
  sub.sync();
  ASSERT_EQ(got.load(), 1);
  ASSERT_TRUE(sub.info().find("cmd=") == std::string::npos);

  // Same for a wildcard subscription ("+" matches the one-level topic "cmd").
  std::atomic<int> phits{0};
  sub.subscribe_pattern("+", [&phits](const std::string &, const char *,
                                      size_t) { phits++; });
  sub.sync();
  pub.publish(Cmd{3});
  ASSERT_TRUE(wait_until([&] { return phits.load() == 1; }));

  sub.unsubscribe_pattern("+");
  sub.sync();
  pub.publish(Cmd{4});
  pub.sync();
  sub.sync();
  ASSERT_EQ(phits.load(), 1);
  ASSERT_TRUE(sub.info().find("patterns: 0") != std::string::npos);
}

// A reconnect must not resurrect an unsubscribed topic: unsubscribe() erases
// the handler map entry, so resubscribe() has nothing to re-send for it.
void test_unsubscribe_survives_reconnect() {
  auto p = sock_path("unsubrecon");
  auto server = std::make_unique<ipc::BrokerServer>(p);
  Node pub("pub");
  Node sub("sub");
  pub.connect(p);
  sub.connect(p);

  std::atomic<int> cmds{0};
  std::atomic<int> events{0};
  sub.subscribe([&cmds](const Cmd &) { cmds++; });
  sub.subscribe([&events](const Event &) { events++; }); // control: kept
  sub.sync();
  sub.unsubscribe<Cmd>();
  sub.sync();

  server.reset();
  ASSERT_TRUE(wait_until([&] { return !sub.connected() && !pub.connected(); }));
  server = std::make_unique<ipc::BrokerServer>(p);

  // The control topic flowing again means both nodes reconnected and sub's
  // resubscribe() ran to completion — cmd's SUBSCRIBE would be in that same
  // burst, so what the daemon knows now is final.
  ASSERT_TRUE(wait_until(
      [&] {
        pub.publish(Event{"e"}); // dropped until pub is back, then routed
        return events.load() > 0;
      },
      5000));
  sub.sync();
  std::string s = sub.info();
  ASSERT_TRUE(s.find("event=1") != std::string::npos);
  ASSERT_TRUE(s.find("cmd=") == std::string::npos);

  pub.publish(Cmd{1});
  pub.sync();
  sub.sync();
  ASSERT_EQ(cmds.load(), 0);
}

// A client that subscribes and then never reads must not wedge the daemon: its
// blocked sends time out, it gets dropped, and healthy clients keep being
// served throughout.
void test_slow_consumer_dropped() {
  auto p = sock_path("slow");
  ipc::BrokerServer server(p);
  Node probe("probe");
  probe.connect(p);

  // Raw client: subscribes to "wedge", watches "live", and reads nothing ever.
  int wedged = ipc::unix_connect(p);
  ASSERT_TRUE(wedged >= 0);
  auto sf = ipc::build_frame(ipc::MSG_SUBSCRIBE, "wedge", nullptr, 0);
  ipc::write_full(wedged, sf.data(), sf.size());
  std::vector<char> req;
  ipc::append_u32(req, 1); // req_id of a KV_GET whose reply is never read
  auto gf = ipc::build_frame(ipc::MSG_KV_GET, "live", req.data(), req.size());
  ipc::write_full(wedged, gf.data(), gf.size());
  ASSERT_TRUE(wait_until(
      [&] { return probe.info().find("wedge=1") != std::string::npos; }));

  // Blast 4 MB at it, far past any socket buffer. Our own writes stall once the
  // daemon's thread for them is stuck on the wedged peer, so bound them too.
  std::thread blaster([&] {
    int fd = ipc::unix_connect(p);
    timeval tv{4, 0};
    ::setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
    std::vector<char> big(64 * 1024, 'x');
    auto f =
        ipc::build_frame(ipc::MSG_PUBLISH, "wedge", big.data(), big.size());
    for (int i = 0; i < 64; i++)
      if (!ipc::write_full(fd, f.data(), f.size()))
        break;
    ::close(fd);
  });

  // No deadlock: a kv round-trip still completes. "live" is watched by the
  // wedged client, so this SET pushes to it while holding the store lock — a
  // stall bounded by the send timeout instead of a permanent one.
  probe.set<int>("live", 1);
  auto v = probe.get<int>("live");
  ASSERT_TRUE(v.has_value());
  ASSERT_EQ(*v, 1);
  blaster.join();

  // ...and the wedged client is gone, leaving only probe.
  ASSERT_TRUE(wait_until(
      [&] { return probe.info().find("clients: 1") != std::string::npos; },
      8000));
  ::close(wedged);
}

// del() erases the key in the daemon and pushes the deletion to watchers, whose
// cached value goes from present to absent.
void test_kv_delete() {
  auto p = sock_path("kvdel");
  auto server = std::make_unique<ipc::BrokerServer>(p);
  Node a("a");
  Node b("b");
  a.connect(p);
  b.connect(p);

  a.set<int>("k", 5);
  a.sync();
  auto warm = b.get<int>("k"); // cold read: caches the value, starts watching
  ASSERT_TRUE(warm.has_value());
  ASSERT_EQ(*warm, 5);

  a.del("k");
  ASSERT_TRUE(wait_until([&] { return !b.get<int>("k").has_value(); }));

  Node fresh("fresh"); // a node that never saw the key: cold read of a dead key
  fresh.connect(p);
  ASSERT_TRUE(!fresh.get<int>("k").has_value());

  // b keeps watching, so its own cache is the answer: with the daemon gone
  // there is nothing to round-trip to and the read still resolves to absent.
  server.reset();
  ASSERT_TRUE(wait_until([&] { return !b.connected(); }));
  ASSERT_TRUE(!b.get<int>("k").has_value());
}

// clear_retained<T>() forgets the stored last-value: subscribers that join
// after it are replayed nothing.
void test_clear_retained() {
  auto p = sock_path("clearret");
  ipc::BrokerServer server(p);
  Node pub("pub");
  pub.connect(p);
  pub.publish(Cmd{7}, /*retain=*/true);
  pub.sync();

  std::atomic<int> early{0};
  {
    Node late("late");
    late.connect(p);
    late.subscribe([&early](const Cmd &c) { early += c.value; });
    ASSERT_TRUE(wait_until([&] { return early.load() == 7; })); // replayed
  }
  ASSERT_TRUE(pub.info().find("retained: 1") != std::string::npos);

  pub.clear_retained<Cmd>();
  pub.sync();
  ASSERT_TRUE(pub.info().find("retained: 0") != std::string::npos);

  std::atomic<int> after{0};
  Node late2("late2");
  late2.connect(p);
  late2.subscribe([&after](const Cmd &) { after++; });
  late2.sync(); // any replay would have been written before this PONG
  ASSERT_EQ(after.load(), 0);
}

// ---- Main ------------------------------------------------------------------

int main() {
  test_case("node reports correct name", test_node_name);
  test_case("two nodes communicate through the broker",
            test_two_nodes_communicate);
  test_case("node subscribes to multiple topics",
            test_node_multiple_topic_subscriptions);
  test_case("async node dispatches on thread pool", test_node_async_dispatch);
  test_case("async node tears down cleanly", test_async_node_teardown_clean);
  test_case("publish requires a connection", test_publish_requires_connection);
  test_case("subscribe before connect is flushed on connect",
            test_subscribe_before_connect);
  test_case("topic is dropped when its last subscriber leaves",
            test_topic_dropped_when_last_subscriber_leaves);
  test_case("kv watcher entry is dropped when its last watcher leaves",
            test_watcher_dropped_when_last_watcher_leaves);
  test_case("unsubscribe stops delivery", test_unsubscribe);
  test_case("unsubscribe is not undone by a reconnect",
            test_unsubscribe_survives_reconnect);
  test_case("slow consumer is dropped, daemon keeps serving",
            test_slow_consumer_dropped);
  test_case("kv delete clears the store and watchers' caches", test_kv_delete);
  test_case("clear_retained stops the replay to late subscribers",
            test_clear_retained);
  return test_summary();
}
