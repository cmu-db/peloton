//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// rule_test.cpp
//
// Identification: tests/common/feature_test.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "gtest/gtest.h"

#include <iostream>
#include <cstring>

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Rule Test
// Based on http://en.cppreference.com/w/cpp/language/rule_of_three
//===--------------------------------------------------------------------===//

class RuleOfThree
{
 public:

  // user-defined constructor
  RuleOfThree(const char* arg)
 : cstring(new char[std::strlen(arg)+1]) {
    std::cout << "Constructor \n";

    std::strcpy(cstring, arg); // populate
  }

  // destructor
  ~RuleOfThree() {
    std::cout << "Destructor \n";

    delete[] cstring;  // deallocate
  }

  // copy constructor
  RuleOfThree(const RuleOfThree& other) {
    std::cout << "Copy Constructor \n";

    cstring = new char[std::strlen(other.cstring) + 1];
    std::strcpy(cstring, other.cstring);
  }

  // copy assignment
  RuleOfThree& operator=(const RuleOfThree& other) {
    std::cout << "Copy Assignment \n";

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

class RuleOfFive
{
 public:

  RuleOfFive(const char* arg)
 : cstring(new char[std::strlen(arg)+1]) {
    std::cout << "Constructor \n";

    std::strcpy(cstring, arg); // populate
  }

  ~RuleOfFive() {
    std::cout << "Destructor \n";

    delete[] cstring;  // deallocate
  }

  // copy constructor
  RuleOfFive(const RuleOfFive& other)   {
    std::cout << "Copy Constructor \n";

    cstring = new char[std::strlen(other.cstring) + 1];
    std::strcpy(cstring, other.cstring);
  }

  // move constructor
  RuleOfFive(RuleOfFive&& other) : cstring(other.cstring) {
    std::cout << "Move Constructor \n";

    other.cstring = nullptr;
  }

  // copy assignment
  RuleOfFive& operator=(const RuleOfFive& other) {
    std::cout << "Copy Assignment \n";

    char* tmp_cstring = new char[std::strlen(other.cstring) + 1];
    std::strcpy(tmp_cstring, other.cstring);
    delete[] cstring;
    cstring = tmp_cstring;
    return *this;
  }

  // move assignment
  RuleOfFive& operator=(RuleOfFive&& other) {
    std::cout << "Move Assignment \n";

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
