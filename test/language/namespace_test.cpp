//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// namespace_test.cpp
//
// Identification: test/language/namespace_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/harness.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Namespace Test
// Based on http://en.cppreference.com/w/cpp/language/namespace
//===--------------------------------------------------------------------===//

namespace vec {

template <typename T>
class vector {
 public:
  bool operator==(const vector<T>& rhs UNUSED_ATTRIBUTE) { return true; }
};

}  // of vec

class NamespaceTests : public PelotonTest {};

TEST_F(NamespaceTests, BasicTest) {
  std::vector<int> v1;  // Standard vector.

  {
    using namespace std;
    vector<int> v3;  // Same as std::vector

    EXPECT_EQ(v1, v3);  // OK
  }
}

}  // End test namespace
}  // End peloton namespace
