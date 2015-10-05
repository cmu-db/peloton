//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// cache_test.cpp
//
// Identification: tests/common/cache_test.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "gtest/gtest.h"
#include "backend/common/cache.h"
#include "backend/planner/abstract_plan.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Cache Test
//===--------------------------------------------------------------------===//

TEST(CacheTest, Basic) {

  Cache<uint32_t, planner::AbstractPlan *> cache(5);

  EXPECT_EQ(0, cache.size());
}

TEST(CacheTest, Find) {
  Cache<uint32_t, planner::AbstractPlan *> cache(5);

  EXPECT_EQ(cache.end(), cache.find(1));

}

}  // End test namespace
}  // End peloton namespace
