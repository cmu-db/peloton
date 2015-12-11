//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// auto_test.cpp
//
// Identification: tests/common/feature_test.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "gtest/gtest.h"

#include <iostream>
#include <cmath>
#include <typeinfo>

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Auto Test
// Based on http://en.cppreference.com/w/cpp/language/auto
//===--------------------------------------------------------------------===//

// the return type is the type of operator+(T, U)
template<class T, class U>
auto add(T t, U u) -> decltype(t + u) {
  return t + u;
}

// same as: double (*get_fun(int))(double)
auto get_fun(int arg) -> double (*)(double) {
  switch (arg)
  {
    case 1: return std::fabs;
    case 2: return std::sin;
    default: return std::cos;
  }
}


TEST(AutoTests, BasicTest) {

  auto a = 1 + 2;
  std::cout << "type of a: " << typeid(a).name() << '\n';
  auto b = add(1, 1.2);
  std::cout << "type of b: " << typeid(b).name() << '\n';
  //auto int c; //compile-time error
  auto d = {1, 2};
  std::cout << "type of d: " << typeid(d).name() << '\n';

  auto my_lambda = [](int x) { return x + 3; };
  std::cout << "my_lambda: " << my_lambda(5) << '\n';

  auto my_fun = get_fun(2);
  std::cout << "type of my_fun: " << typeid(my_fun).name() << '\n';
  std::cout << "my_fun: " << my_fun(3) << '\n';

}

}  // End test namespace
}  // End peloton namespace
