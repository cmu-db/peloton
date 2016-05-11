//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// auto_test.cpp
//
// Identification: tests/language/auto_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "gtest/gtest.h"

#include <iostream>
#include <cmath>
#include <typeinfo>

#include "harness.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Auto Test
// Based on http://en.cppreference.com/w/cpp/language/auto
//===--------------------------------------------------------------------===//

// the return type is the type of operator+(T, U)
template <class T, class U>
auto add(T t, U u) -> decltype(t + u) {
  return t + u;
}

// same as: double (*get_fun(int))(double)
auto get_fun(int arg) -> double (*)(double) {
  switch (arg) {
    case 1:
      return std::fabs;
    case 2:
      return std::sin;
    default:
      return std::cos;
  }
}

TEST(AutoTests, BasicTest) {
  auto a = 1 + 2;
  LOG_INFO("type of a: %s", typeid(a).name());
  auto b = add(1, 1.2);
  LOG_INFO("type of b: %s", typeid(b).name());
  auto d = {1, 2};
  LOG_INFO("type of d: %s", typeid(d).name());

  auto my_lambda = [](int x) { return x + 3; };
  LOG_INFO("my_lambda: %d",  my_lambda(5));

  auto my_fun = get_fun(2);
  LOG_INFO("type of my_fun: %s", typeid(my_fun).name());

  LOG_INFO("my_fun: %lf", my_fun(3));
}

}  // End test namespace
}  // End peloton namespace
