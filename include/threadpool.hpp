#pragma once
#include <atomic>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

namespace chappe {

class ThreadPool {
  std::vector<std::thread> workers_;
  std::queue<std::function<void()>> tasks_;
  std::mutex mutex_;
  std::condition_variable cv_;
  std::condition_variable idle_cv_;
  size_t active_ = 0; // taken off the queue but not finished yet
  bool stop_ = false;

  void workerLoop() {
    while (true) {
      std::function<void()> task;
      {
        std::unique_lock lock(mutex_);
        cv_.wait(lock, [this] { return stop_ || !tasks_.empty(); });
        if (stop_ && tasks_.empty())
          return;
        task = std::move(tasks_.front());
        tasks_.pop();
        active_++;
      }
      task();
      {
        std::lock_guard lock(mutex_);
        active_--;
      }
      idle_cv_.notify_all();
    }
  }

public:
  explicit ThreadPool(size_t n_threads) {
    for (size_t i = 0; i < n_threads; i++)
      workers_.emplace_back([this] { workerLoop(); });
  }

  ~ThreadPool() {
    {
      std::lock_guard lock(mutex_);
      stop_ = true;
    }
    cv_.notify_all();
    for (auto &t : workers_)
      t.join();
  }

  void enqueue(std::function<void()> task) {
    {
      std::lock_guard lock(mutex_);
      tasks_.push(std::move(task));
    }
    cv_.notify_one();
  }

  // Wait until every task queued so far has finished. The in-flight count is
  // what makes this true with more than one worker: a sentinel task at the back
  // of the queue only proves *a* worker got that far, not that the others had
  // finished what they were already running.
  void drain() {
    std::unique_lock lock(mutex_);
    idle_cv_.wait(lock, [this] { return tasks_.empty() && active_ == 0; });
  }

  ThreadPool(const ThreadPool &) = delete;
  ThreadPool &operator=(const ThreadPool &) = delete;
};

} // namespace chappe
