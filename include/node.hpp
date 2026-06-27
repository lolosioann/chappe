#pragma once
#include "broker.hpp"
#include "threadpool.hpp"
#include <memory>
#include <string>
#include <vector>

template <typename... Topics> class Node {
  std::string name_;
  Broker<Topics...> &broker_;
  std::unique_ptr<ThreadPool> pool_;      // destroyed after guards_
  std::vector<SubscriptionGuard> guards_; // destroyed first

public:
  Node(std::string name, Broker<Topics...> &broker, size_t threads = 0)
      : name_(std::move(name)), broker_(broker),
        pool_(threads ? std::make_unique<ThreadPool>(threads) : nullptr) {}

  template <typename F> void subscribe(F &&handler) {
    using T = msg_t<std::decay_t<F>>;

    std::function<void(const T &)> h(std::forward<F>(handler));

    if (pool_) {
      h = [this, inner = std::move(h)](const T &msg) {
        pool_->enqueue([inner, msg] { inner(msg); });
      };
    }

    guards_.emplace_back(broker_.subscribe(std::move(h)));
  }

  template <typename T> void publish(const T &msg) { broker_.publish(msg); }

  // drain the thread pool — useful in tests to wait for async handlers
  void drain() {
    if (pool_)
      pool_->drain();
  }

  const std::string &name() const { return name_; }

  Node(const Node &) = delete;
  Node &operator=(const Node &) = delete;
  Node(Node &&) = default;
  Node &operator=(Node &&) = default;
};
