#pragma once
#include <type_traits>

// Topic + handler-type deduction helpers shared by the Node client and the
// wire layer. Routing itself lives in the broker daemon (see server.hpp)
// — a topic is just a string on the wire; these helpers map a C++ message type
// to that string and let subscribe() deduce the type from a handler.

namespace chappe {

// The same literal lives in the Makefile (VERSION, for chappe.pc and the CMake
// config) and in python/chappe/__init__.py (__version__). Three copies rather
// than a generated header, because generating one would put a build step in
// front of a header-only library; test_chappe.py fails if they drift apart.
constexpr const char *VERSION = "3.0.0";

// ---- Topic -----------------------------------------------------------------

template <typename T> struct Topic {
  static constexpr const char *name = nullptr;
};

} // namespace chappe

// Opens namespace chappe itself, so it must be used at global scope — a
// specialization can only be declared in the namespace of the template it
// specializes, and requiring `namespace chappe {}` at every call site would be
// worse than this restriction.
#define MAKE_TOPIC(Type, TopicName)                                            \
  namespace chappe {                                                           \
  template <> struct Topic<Type> {                                             \
    static constexpr const char *name = TopicName;                             \
  };                                                                           \
  }                                                                            \
  static_assert(true, "MAKE_TOPIC needs a trailing semicolon")

namespace chappe {

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

} // namespace chappe
