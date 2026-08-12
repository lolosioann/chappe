// tests/test_threadpool.cpp
#include "test.hpp"
#include "threadpool.hpp"
#include <atomic>
#include <chrono>
#include <thread>
#include <vector>

// ---- Tests -----------------------------------------------------------------

void test_executes_all_tasks() {
  ThreadPool pool(4);
  std::atomic<int> count{0};

  for (int i = 0; i < 1000; i++)
    pool.enqueue([&count] { count++; });

  pool.drain();
  ASSERT_EQ(count.load(), 1000);
}

void test_single_thread() {
  ThreadPool pool(1);
  std::atomic<int> count{0};

  for (int i = 0; i < 100; i++)
    pool.enqueue([&count] { count++; });

  pool.drain();
  ASSERT_EQ(count.load(), 100);
}

void test_tasks_run_concurrently() {
  // verify all tasks complete and run in parallel via timing
  ThreadPool pool(4);
  std::atomic<int> done{0};

  auto start = std::chrono::steady_clock::now();

  for (int i = 0; i < 8; i++)
    pool.enqueue([&done] {
      std::this_thread::sleep_for(std::chrono::milliseconds(20));
      done++;
    });

  // sleep long enough for all 8 tasks to finish on 4 workers (~40ms)
  std::this_thread::sleep_for(std::chrono::milliseconds(120));

  auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                     std::chrono::steady_clock::now() - start)
                     .count();

  ASSERT_EQ(done.load(), 8);
  ASSERT_TRUE(elapsed < 160); // sequential would be 160ms, parallel ~40ms
}

void test_drain_waits_for_completion() {
  ThreadPool pool(2);
  std::atomic<bool> finished{false};

  pool.enqueue([&finished] {
    std::this_thread::sleep_for(std::chrono::milliseconds(30));
    finished = true;
  });

  // The pool has a spare worker, so anything that only queued a marker behind
  // the sleeping task would come back immediately and see finished == false.
  pool.drain();
  ASSERT_TRUE(finished.load());
}

void test_destructor_finishes_queued_tasks() {
  std::atomic<int> count{0};
  {
    ThreadPool pool(2);
    for (int i = 0; i < 50; i++)
      pool.enqueue([&count] { count++; });
    // destructor joins workers — all tasks must complete
  }
  ASSERT_EQ(count.load(), 50);
}

void test_enqueue_from_multiple_threads() {
  ThreadPool pool(4);
  std::atomic<int> count{0};

  std::vector<std::thread> producers;
  for (int i = 0; i < 8; i++)
    producers.emplace_back([&pool, &count] {
      for (int j = 0; j < 100; j++)
        pool.enqueue([&count] { count++; });
    });

  for (auto &t : producers)
    t.join();
  pool.drain();

  ASSERT_EQ(count.load(), 800);
}

// ---- Main ------------------------------------------------------------------

int main() {
  test_case("executes all enqueued tasks", test_executes_all_tasks);

  test_case("works correctly with single worker thread", test_single_thread);

  test_case("tasks run concurrently across workers",
            test_tasks_run_concurrently);

  test_case("drain() waits for all tasks to complete",
            test_drain_waits_for_completion);

  test_case("destructor completes all queued tasks",
            test_destructor_finishes_queued_tasks);

  test_case("enqueue from multiple producer threads",
            test_enqueue_from_multiple_threads);

  return test_summary();
}
