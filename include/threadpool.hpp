#pragma once
#include <atomic>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

class ThreadPool {
  std::vector<std::thread> workers_;
  std::queue<std::function<void()>> tasks_;
  std::mutex mutex_;
  std::condition_variable cv_;
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
      }
      task();
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

  // wait until all currently queued tasks have been executed
  void drain() {
    std::mutex m;
    std::condition_variable cv;
    bool done = false;

    enqueue([&] {
      std::lock_guard lk(m);
      done = true;
      cv.notify_one();
    });

    std::unique_lock lk(m);
    cv.wait(lk, [&] { return done; });
  }

  ThreadPool(const ThreadPool &) = delete;
  ThreadPool &operator=(const ThreadPool &) = delete;
};
