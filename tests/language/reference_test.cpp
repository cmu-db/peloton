//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// reference_test.cpp
//
// Identification: tests/language/reference_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <utility>
#include <sstream>
#include <iostream>

#include "harness.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Namespace Test
// Based on http://en.cppreference.com/w/cpp/language/reference_initialization
//===--------------------------------------------------------------------===//

struct S {
  int mi;
  const std::pair<int, int>& mp;  // reference member

  friend std::ostream& operator<<(std::ostream& output,
                                  const S& s UNUSED_ATTRIBUTE) {
    output << "S \n";
    return output;
  }
};

void foo(int) {}

struct A {
  friend std::ostream& operator<<(std::ostream& output,
                                  const A& a UNUSED_ATTRIBUTE) {
    output << "A \n";
    return output;
  }
};

struct B : A {
  int n;
  operator int&() { return n; };

  friend std::ostream& operator<<(std::ostream& output,
                                  const B& b UNUSED_ATTRIBUTE) {
    output << "B \n";
    return output;
  }
};

B bar() { return B(); }

// int& bad_r; // error: no initializer
extern int& ext_r;  // OK

class ReferenceTests : public PelotonTest {};

TEST_F(ReferenceTests, BasicTest) {
  // lvalues
  int n = 1;
  int& r1 = n;          // lvalue reference to the object n
  const int& cr(n);     // reference can be more cv-qualified
  volatile int& cv{n};  // any initializer syntax can be used
  int& r2 = r1;         // another lvalue reference to the object n
  //    int& bad = cr; // error: less cv-qualified
  int& r3 = const_cast<int&>(cr);  // const_cast is needed

  //B b;
  //A& base_ref = b;  // reference to base subobject
  // int& converted_ref = b; // reference to the result of a conversion

  // rvalues
  //  int& bad = 1; // error: cannot bind lvalue ref to rvalue
  const int& cref = 1;  // bound to rvalue
  int&& rref = 1;       // bound to rvalue

  //const A& cref2 = bar();  // reference to A subobject of B temporary
  //A&& rref2 = bar();       // same

  int&& xref = static_cast<int&&>(n);  // bind directly to n
  //  int&& copy_ref = n; // error: can't bind to an lvalue
  double&& copy_ref = n;  // bind to an rvalue temporary with value 1.0

  // restrictions on temporary lifetimes
  // std::ostream& buf_ref = std::ostringstream() << 'a'; // the ostringstream
  // temporary
  // was bound to the left operand of operator<<, but its lifetime
  // ended at the semicolon: buf_ref is now a dangling reference.

  //S a{1, {2, 3}};  // temporary pair {2,3} bound to the reference member
  // a.mp and its lifetime is extended to match a
  // (Note: does not compile in C++17)
  S* p = new S{1, {2, 3}};  // temporary pair {2,3} bound to the reference
  // member a.mp, but its lifetime ended at the semicolon
  //  p->mp is a dangling reference

  LOG_INFO("%d", cv);
  LOG_INFO("%d", r2);
  LOG_INFO("%d", r3);

  LOG_INFO("%d", cref);
  LOG_INFO("%d", rref);

  LOG_INFO("%d", xref);
  LOG_INFO("%lf", copy_ref);

  delete p;
}

}  // End test namespace
}  // End peloton namespace
