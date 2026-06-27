// tests/test_node.cpp
#include "broker.hpp"
#include "node.hpp"
#include "test.hpp"
#include <atomic>
#include <string>

// ---- Messages --------------------------------------------------------------

struct Cmd {
  int value;
};
struct Event {
  std::string name;
};

MAKE_TOPIC(Cmd, "cmd");
MAKE_TOPIC(Event, "event");

// ---- Tests -----------------------------------------------------------------

void test_node_name() {
  Broker<Cmd> broker;
  Node<Cmd> node("controller", broker);
  ASSERT_EQ(node.name(), std::string("controller"));
}

void test_node_subscribe_and_publish() {
  Broker<Cmd> broker;
  Node<Cmd> node("a", broker);
  int count = 0;

  node.subscribe([&count](const Cmd &c) { count += c.value; });

  node.publish(Cmd{3});
  node.publish(Cmd{7});

  ASSERT_EQ(count, 10);
}

void test_node_unsubscribes_on_destruction() {
  Broker<Cmd> broker;
  int count = 0;

  {
    Node<Cmd> node("a", broker);
    node.subscribe([&count](const Cmd &) { count++; });
    broker.publish(Cmd{0});
  } // node destroyed → all guards released

  broker.publish(Cmd{0}); // should not reach handler

  ASSERT_EQ(count, 1);
}

void test_two_nodes_communicate() {
  Broker<Cmd, Event> broker;

  Node<Cmd, Event> sender("sender", broker);
  Node<Cmd, Event> receiver("receiver", broker);

  int received = 0;
  receiver.subscribe([&received](const Cmd &c) { received += c.value; });

  sender.publish(Cmd{5});
  sender.publish(Cmd{3});

  ASSERT_EQ(received, 8);
}

void test_node_multiple_topic_subscriptions() {
  Broker<Cmd, Event> broker;
  Node<Cmd, Event> node("multi", broker);

  int cmd_count = 0;
  int event_count = 0;

  node.subscribe([&cmd_count](const Cmd &) { cmd_count++; });
  node.subscribe([&event_count](const Event &) { event_count++; });

  broker.publish(Cmd{0});
  broker.publish(Cmd{0});
  broker.publish(Event{"e"});

  ASSERT_EQ(cmd_count, 2);
  ASSERT_EQ(event_count, 1);
}

void test_node_async_dispatch() {
  Broker<Cmd> broker;
  Node<Cmd> node("async_node", broker, 2); // 2 worker threads
  std::atomic<int> count{0};

  node.subscribe([&count](const Cmd &c) { count += c.value; });

  for (int i = 0; i < 10; i++)
    broker.publish(Cmd{1});

  node.drain(); // wait for all async handlers to complete

  ASSERT_EQ(count.load(), 10);
}

void test_async_node_unsubscribes_cleanly() {
  Broker<Cmd> broker;
  std::atomic<int> count{0};

  {
    Node<Cmd> node("async", broker, 2);
    node.subscribe([&count](const Cmd &) {
      std::this_thread::sleep_for(std::chrono::milliseconds(5));
      count++;
    });

    for (int i = 0; i < 10; i++)
      broker.publish(Cmd{0});

    node.drain();
    // node destroyed → guards released, pool joins workers cleanly
  }

  broker.publish(Cmd{0}); // silent — node is gone

  ASSERT_EQ(count.load(), 10);
}

void test_sync_and_async_nodes_share_broker() {
  Broker<Cmd> broker;

  Node<Cmd> sync_node("sync", broker, 0);   // synchronous
  Node<Cmd> async_node("async", broker, 2); // async

  std::atomic<int> sync_count{0};
  std::atomic<int> async_count{0};

  sync_node.subscribe([&sync_count](const Cmd &) { sync_count++; });
  async_node.subscribe([&async_count](const Cmd &) { async_count++; });

  for (int i = 0; i < 20; i++)
    broker.publish(Cmd{0});

  async_node.drain();

  ASSERT_EQ(sync_count.load(), 20);
  ASSERT_EQ(async_count.load(), 20);
}

// ---- Main ------------------------------------------------------------------

int main() {
  test_case("node reports correct name", test_node_name);

  test_case("node subscribe and publish", test_node_subscribe_and_publish);

  test_case("node unsubscribes all on destruction",
            test_node_unsubscribes_on_destruction);

  test_case("two nodes communicate through shared broker",
            test_two_nodes_communicate);

  test_case("node subscribes to multiple topics",
            test_node_multiple_topic_subscriptions);

  test_case("async node dispatches on thread pool", test_node_async_dispatch);

  test_case("async node unsubscribes and joins cleanly",
            test_async_node_unsubscribes_cleanly);

  test_case("sync and async nodes share one broker",
            test_sync_and_async_nodes_share_broker);

  return test_summary();
}
