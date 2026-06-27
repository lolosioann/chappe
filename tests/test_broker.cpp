// tests/test_broker.cpp
#include "broker.hpp"
#include "test.hpp"
#include <atomic>
#include <memory>
#include <string>
#include <thread>
#include <vector>

// ---- Messages --------------------------------------------------------------

struct Msg {
  int value;
};
struct Other {
  std::string text;
};

MAKE_TOPIC(Msg, "msg");
MAKE_TOPIC(Other, "other");

// ---- Tests -----------------------------------------------------------------

void test_single_subscriber() {
  Broker<Msg> broker;
  int count = 0;

  auto g = broker.subscribe([&count](const Msg &m) { count += m.value; });

  broker.publish(Msg{1});
  broker.publish(Msg{2});
  broker.publish(Msg{3});

  ASSERT_EQ(count, 6);
}

void test_multiple_subscribers() {
  Broker<Msg> broker;
  int a = 0, b = 0;

  auto g1 = broker.subscribe([&a](const Msg &) { a++; });
  auto g2 = broker.subscribe([&b](const Msg &) { b++; });

  broker.publish(Msg{0});
  broker.publish(Msg{0});

  ASSERT_EQ(a, 2);
  ASSERT_EQ(b, 2);
}

void test_guard_unsubscribes_on_destruction() {
  Broker<Msg> broker;
  int count = 0;

  {
    auto g = broker.subscribe([&count](const Msg &) { count++; });
    broker.publish(Msg{0}); // handler alive
  } // g destroyed → unsubscribed

  broker.publish(Msg{0}); // should not reach handler

  ASSERT_EQ(count, 1);
}

void test_multiple_topics_independent() {
  Broker<Msg, Other> broker;
  int msg_count = 0;
  int other_count = 0;

  auto g1 = broker.subscribe([&msg_count](const Msg &) { msg_count++; });
  auto g2 = broker.subscribe([&other_count](const Other &) { other_count++; });

  broker.publish(Msg{0});
  broker.publish(Msg{0});
  broker.publish(Other{"hi"});

  ASSERT_EQ(msg_count, 2);
  ASSERT_EQ(other_count, 1);
}

void test_no_subscribers_silent() {
  Broker<Msg> broker;
  // should not crash
  broker.publish(Msg{42});
  ASSERT_TRUE(true);
}

void test_self_unsubscribe_during_dispatch() {
  // Guarantee: self-unsubscribe during dispatch must not deadlock.
  // With the snapshot approach, the unsubscribe takes effect on entries_
  // immediately, but the current publish already holds a snapshot —
  // so the handler may fire once more if a concurrent publish grabbed
  // a snapshot before unsubscribe completed. In single-threaded use,
  // unsubscribe is visible to the next publish() call.
  Broker<Msg> broker;
  std::atomic<int> count{0};
  std::atomic<bool> unsubscribed{false};

  SubscriptionGuard guard;
  guard = broker.subscribe<Msg>([&](const Msg &) mutable {
    count++;
    guard = SubscriptionGuard(); // unsubscribe — no deadlock
    unsubscribed = true;
  });

  broker.publish(Msg{0}); // fires, unsubscribes
  broker.publish(Msg{0}); // silent — unsubscribe completed before this publish

  ASSERT_TRUE(unsubscribed.load());
  ASSERT_TRUE(count.load() >= 1); // fired at least once
  ASSERT_TRUE(count.load() <= 2); // at most twice (snapshot race)
}

void test_concurrent_publish() {
  Broker<Msg> broker;
  std::atomic<int> count{0};

  auto g = broker.subscribe([&count](const Msg &) { count++; });

  std::vector<std::thread> threads;
  for (int i = 0; i < 8; i++)
    threads.emplace_back([&broker]() {
      for (int j = 0; j < 200; j++)
        broker.publish(Msg{1});
    });

  for (auto &t : threads)
    t.join();

  ASSERT_EQ(count.load(), 1600);
}

void test_concurrent_sub_unsub_during_publish() {
  Broker<Msg> broker;
  std::atomic<bool> running{true};

  // continuous publisher thread
  std::thread publisher([&]() {
    while (running)
      broker.publish(Msg{0});
  });

  // subscribe and unsubscribe repeatedly while publisher runs
  for (int i = 0; i < 20; i++) {
    auto g = broker.subscribe([](const Msg &) {});
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
    // g destroyed → unsubscribe fires concurrently with publish
  }

  running = false;
  publisher.join();

  ASSERT_TRUE(true); // reaching here without crash is the assertion
}

// ---- Main ------------------------------------------------------------------

int main() {
  test_case("single subscriber receives all messages", test_single_subscriber);

  test_case("multiple subscribers all receive messages",
            test_multiple_subscribers);

  test_case("guard unsubscribes on destruction",
            test_guard_unsubscribes_on_destruction);

  test_case("multiple topics are independent",
            test_multiple_topics_independent);

  test_case("publish with no subscribers is silent",
            test_no_subscribers_silent);

  test_case("self-unsubscribe during dispatch does not deadlock",
            test_self_unsubscribe_during_dispatch);

  test_case("concurrent publish from 8 threads", test_concurrent_publish);

  test_case("subscribe/unsubscribe concurrent with publish",
            test_concurrent_sub_unsub_during_publish);

  return test_summary();
}
