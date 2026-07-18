#pragma once
#include <type_traits>

// Topic + handler-type deduction helpers shared by the Node client and the
// wire layer. Routing itself lives in the broker daemon (see broker_server.hpp)
// — a topic is just a string on the wire; these helpers map a C++ message type
// to that string and let subscribe() deduce the type from a handler.

// ---- Topic -----------------------------------------------------------------

template <typename T> struct Topic {
  static constexpr const char *name = nullptr;
};

#define MAKE_TOPIC(Type, TopicName)                                            \
  template <> struct Topic<Type> {                                             \
    static constexpr const char *name = TopicName;                             \
  }

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
