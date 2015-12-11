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

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Namespace Test
// Based on http://en.cppreference.com/w/cpp/language/namespace
//===--------------------------------------------------------------------===//

namespace vec {

template< typename T >
class vector {
 public:

  bool operator==(const vector<T>& rhs __attribute__((unused))){
    return true;
  }

};

} // of vec


TEST(NamespaceTests, BasicTest) {

  std::vector<int> v1; // Standard vector.
  vec::vector<int> v2; // User defined vector.

  // Error: v1 and v2 are different object's type.
  // v1 = v2;

  {
    using namespace std;
    vector<int> v3; // Same as std::vector

    EXPECT_EQ(v1, v3); // OK
  }

  {
    using vec::vector;
    vector<int> v4; // Same as vec::vector

    EXPECT_EQ(v2, v4); // OK
  }
}

}  // End test namespace
}  // End peloton namespace
