#pragma once
#include <functional>
#include <iostream>
#include <string>

// ---- counters --------------------------------------------------------------

inline int test_passes = 0;
inline int test_failures = 0;

// ---- assertions ------------------------------------------------------------

template <typename A, typename B>
void assert_eq(const A &a, const B &b, const char *expr, const char *file,
               int line) {
  if (a == b) {
    std::cout << "  PASS  " << expr << "\n";
    test_passes++;
  } else {
    std::cout << "  FAIL  " << expr << "  (got " << a << ", expected " << b
              << ")"
              << "  [" << file << ":" << line << "]\n";
    test_failures++;
  }
}

template <typename A, typename B>
void assert_neq(const A &a, const B &b, const char *expr, const char *file,
                int line) {
  if (a != b) {
    std::cout << "  PASS  " << expr << "\n";
    test_passes++;
  } else {
    std::cout << "  FAIL  " << expr << "  (both equal " << a << ")"
              << "  [" << file << ":" << line << "]\n";
    test_failures++;
  }
}

inline void assert_true(bool x, const char *expr, const char *file, int line) {
  assert_eq(x, true, expr, file, line);
}

#define ASSERT_EQ(a, b) assert_eq((a), (b), #a " == " #b, __FILE__, __LINE__)
#define ASSERT_NEQ(a, b) assert_neq((a), (b), #a " != " #b, __FILE__, __LINE__)
#define ASSERT_TRUE(x) assert_true((x), #x, __FILE__, __LINE__)

// ---- test case -------------------------------------------------------------

inline void test_case(const std::string &name, std::function<void()> fn) {
  std::cout << "\n[ " << name << " ]\n";
  fn();
}

// ---- summary ---------------------------------------------------------------

inline int test_summary() {
  std::cout << "\n--------------------------\n";
  std::cout << "passed : " << test_passes << "\n";
  std::cout << "failed : " << test_failures << "\n";
  return test_failures > 0 ? 1 : 0;
}
