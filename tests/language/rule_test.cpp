//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// rule_test.cpp
//
// Identification: tests/language/rule_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "gtest/gtest.h"

#include <iostream>
#include <cstring>

#include "harness.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Rule Test
// Based on http://en.cppreference.com/w/cpp/language/rule_of_three
//===--------------------------------------------------------------------===//

class RuleOfThree {
 public:
  // user-defined constructor
  RuleOfThree(const char* arg) : cstring(new char[std::strlen(arg) + 1]) {
    LOG_INFO("Constructor ");

    std::strcpy(cstring, arg);  // populate
  }

  // destructor
  ~RuleOfThree() {
    LOG_INFO("Destructor ");

    delete[] cstring;  // deallocate
  }

  // copy constructor
  RuleOfThree(const RuleOfThree& other) {
    LOG_INFO("Copy Constructor ");

    cstring = new char[std::strlen(other.cstring) + 1];
    std::strcpy(cstring, other.cstring);
  }

  // copy assignment
  RuleOfThree& operator=(const RuleOfThree& other) {
    LOG_INFO("Copy Assignment ");

    char* tmp_cstring = new char[std::strlen(other.cstring) + 1];
    std::strcpy(tmp_cstring, other.cstring);
    delete[] cstring;
    cstring = tmp_cstring;
    return *this;
  }

  // Alternatively, reuse destructor and copy ctor
  //  RuleOfThree& operator=(RuleOfThree other)
  //  {
  //      std::swap(cstring, other.cstring);
  //      return *this;
  //  }

 private:
  // raw pointer used as a handle to a dynamically-allocated memory block
  char* cstring;
};

TEST(RuleTests, RuleOfThreeTest) {
  RuleOfThree a("foo");

  RuleOfThree b(a);

  RuleOfThree c("bar");

  c = b;

  RuleOfThree d = c;
}

class RuleOfFive {
 public:
  RuleOfFive(const char* arg) : cstring(new char[std::strlen(arg) + 1]) {
    LOG_INFO("Constructor ");

    std::strcpy(cstring, arg);  // populate
  }

  ~RuleOfFive() {
    LOG_INFO("Destructor ");

    delete[] cstring;  // deallocate
  }

  // copy constructor
  RuleOfFive(const RuleOfFive& other) {
    LOG_INFO("Copy Constructor ");

    cstring = new char[std::strlen(other.cstring) + 1];
    std::strcpy(cstring, other.cstring);
  }

  // move constructor
  RuleOfFive(RuleOfFive&& other) : cstring(other.cstring) {
    LOG_INFO("Move Constructor ");

    other.cstring = nullptr;
  }

  // copy assignment
  RuleOfFive& operator=(const RuleOfFive& other) {
    LOG_INFO("Copy Assignment ");

    char* tmp_cstring = new char[std::strlen(other.cstring) + 1];
    std::strcpy(tmp_cstring, other.cstring);
    delete[] cstring;
    cstring = tmp_cstring;
    return *this;
  }

  // move assignment
  RuleOfFive& operator=(RuleOfFive&& other) {
    LOG_INFO("Move Assignment ");

    delete[] cstring;
    cstring = other.cstring;
    other.cstring = nullptr;
    return *this;
  }

  // Alternatively, replace both assignment operators with
  //  RuleOfFive& operator=(RuleOfFive other)
  //  {
  //      std::swap(cstring, other.cstring);
  //      return *this;
  //  }

 private:
  // raw pointer used as a handle to a dynamically-allocated memory block
  char* cstring;
};

TEST(RuleTests, RuleOfFiveTest) {
  RuleOfFive a("foo");

  RuleOfFive b(std::move(a));

  RuleOfFive c("bar");

  c = std::move(b);

  RuleOfFive d = std::move(c);
}

}  // End test namespace
}  // End peloton namespace
