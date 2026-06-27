#pragma once
#include <algorithm>
#include <cstdint>
#include <functional>
#include <mutex>
#include <shared_mutex>
#include <tuple>
#include <type_traits>
#include <vector>

// ---- Topic -----------------------------------------------------------------

template <typename T> struct Topic {
  static constexpr const char *name = nullptr;
};

#define MAKE_TOPIC(Type, TopicName)                                            \
  template <> struct Topic<Type> {                                             \
    static constexpr const char *name = TopicName;                             \
  }

// ---- TopicState ------------------------------------------------------------

template <typename T> struct TopicState {
  using HandlerFn = std::function<void(const T &)>;

  struct Entry {
    uint64_t id;
    HandlerFn handler;
  };

  uint64_t next_id_ = 0;
  std::vector<Entry> entries_;
  mutable std::shared_mutex mutex_;

  uint64_t subscribe(HandlerFn fn) {
    std::unique_lock lock(mutex_);
    uint64_t id = next_id_++;
    entries_.push_back(Entry{id, std::move(fn)});
    return id;
  }

  void unsubscribe(uint64_t id) {
    std::unique_lock lock(mutex_);
    entries_.erase(std::remove_if(entries_.begin(), entries_.end(),
                                  [id](const Entry &e) { return e.id == id; }),
                   entries_.end());
  }

  void publish(const T &msg) {
    std::vector<Entry> snapshot;
    {
      std::shared_lock lock(mutex_);
      snapshot = entries_;
    }
    for (const Entry &e : snapshot)
      e.handler(msg);
  }
};

// ---- SubscriptionGuard -----------------------------------------------------

class SubscriptionGuard {
  std::function<void()> unsubscribe_;

public:
  explicit SubscriptionGuard(std::function<void()> fn)
      : unsubscribe_(std::move(fn)) {}

  // default constructible as empty guard
  SubscriptionGuard() = default;

  ~SubscriptionGuard() {
    if (unsubscribe_)
      unsubscribe_();
  }

  SubscriptionGuard(const SubscriptionGuard &) = delete;
  SubscriptionGuard &operator=(const SubscriptionGuard &) = delete;
  SubscriptionGuard(SubscriptionGuard &&) = default;
  SubscriptionGuard &operator=(SubscriptionGuard &&) = default;
};

// ---- Callable type deduction -----------------------------------------------

template <typename F>
struct callable_arg : callable_arg<decltype(&F::operator())> {};

template <typename R, typename A> struct callable_arg<R (*)(A)> {
  using type = A;
};

template <typename C, typename R, typename A>
struct callable_arg<R (C::*)(A) const> {
  using type = A;
}; // non-mutable lambda

template <typename C, typename R, typename A> struct callable_arg<R (C::*)(A)> {
  using type = A;
}; // mutable lambda

template <typename F>
using msg_t = std::decay_t<typename callable_arg<F>::type>;

// ---- Broker ----------------------------------------------------------------

template <typename... Topics> class Broker {
  std::tuple<TopicState<Topics>...> state_;

  template <typename T> TopicState<T> &state_for() {
    return std::get<TopicState<T>>(state_);
  }

public:
  // explicit topic type: broker.subscribe<Msg>(fn)
  // needed for mutable lambdas where deduction fails
  template <typename T, typename F> SubscriptionGuard subscribe(F fn) {
    uint64_t id =
        state_for<T>().subscribe(std::function<void(const T &)>(std::move(fn)));
    return SubscriptionGuard([this, id]() { state_for<T>().unsubscribe(id); });
  }

  // deduced topic type: broker.subscribe(fn)
  // works for non-mutable lambdas and plain function pointers
  template <typename F> SubscriptionGuard subscribe(F fn) {
    using T = msg_t<F>;
    return subscribe<T>(std::move(fn));
  }

  template <typename T> void publish(const T &msg) {
    state_for<T>().publish(msg);
  }
};
