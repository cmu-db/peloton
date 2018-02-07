//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// set_util_test.cpp
//
// Identification: test/set_util_test.cpp
//
// Copyright (c) 2015-18, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/harness.h"
#include "common/logger.h"

#include <set>

#include "util/set_util.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// SetUtil Test
//===--------------------------------------------------------------------===//

class SetUtilTests : public PelotonTest {};

TEST_F(SetUtilTests, DisjointTest) {
  // Check sorted sets
  std::set<int> setA{1, 2, 3, 4};
  std::set<int> setB{4, 5, 6, 7};
  EXPECT_FALSE(SetUtil::isDisjoint(setA, setB));

  std::set<int> setC{9, 8, 10};
  EXPECT_TRUE(SetUtil::isDisjoint(setA, setC));

  std::set<int> setD{4, 1, 3, 2};
  EXPECT_FALSE(SetUtil::isDisjoint(setB, setD));

  std::set<int> setE;
  EXPECT_TRUE(SetUtil::isDisjoint(setD, setE));
}

}  // namespace test
}  // namespace peloton
