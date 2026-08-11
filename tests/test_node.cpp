// tests/test_node.cpp
#include "broker.hpp"
#include "broker_server.hpp"
#include "node.hpp"
#include "test.hpp"
#include <atomic>
#include <chrono>
#include <string>
#include <thread>
#include <unistd.h>

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

// spin until `pred` or a 2s timeout; keeps tests robust against async delivery
template <typename P> static bool wait_until(P pred) {
  for (int i = 0; i < 400; i++) {
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
  test_case("topic is dropped when its last subscriber leaves",
            test_topic_dropped_when_last_subscriber_leaves);
  test_case("kv watcher entry is dropped when its last watcher leaves",
            test_watcher_dropped_when_last_watcher_leaves);
  return test_summary();
}
