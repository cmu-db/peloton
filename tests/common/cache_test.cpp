//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// cache_test.cpp
//
// Identification: tests/common/cache_test.cpp
//
// Copyright (c) 201CACHE_SIZE, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "gtest/gtest.h"
#include "backend/common/cache.h"
#include "backend/common/logger.h"
#include "backend/planner/mock_plan.h"

#include "backend/bridge/dml/mapper/mapper.h"

#include <unordered_set>

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Cache Test
//===--------------------------------------------------------------------===//

#define CACHE_SIZE 5


static void fill(std::vector<std::shared_ptr<const planner::AbstractPlan> >& vec, int n) {
  for (int i = 0; i < n; i++) {
    std::shared_ptr<const planner::AbstractPlan> plan(new MockPlan(), bridge::PlanTransformer::CleanPlan);
    vec.push_back(std::move(plan));
  }
}

/**
 * Test basic functionality
 *
 */
TEST(CacheTest, Basic) {

  Cache<uint32_t, const planner::AbstractPlan> cache(
      CACHE_SIZE, bridge::PlanTransformer::CleanPlan);

  EXPECT_EQ(0, cache.size());
  EXPECT_EQ(true, cache.empty());
}

/**
 *  Test find operation
 *
 */
TEST(CacheTest, Find) {
  Cache<uint32_t, const planner::AbstractPlan> cache(
      CACHE_SIZE, bridge::PlanTransformer::CleanPlan);

  EXPECT_EQ(cache.end(), cache.find(1));
}

/**
 * Test insert operation
 *
 */
TEST(CacheTest, Insert) {
  Cache<uint32_t, const planner::AbstractPlan> cache(
      CACHE_SIZE, bridge::PlanTransformer::CleanPlan);

  std::vector<std::shared_ptr<const planner::AbstractPlan> > plans;
  fill(plans, CACHE_SIZE);

  cache.insert(std::make_pair(0, plans[0]));

  auto cache_it = cache.find(0);

  EXPECT_EQ((*cache_it).get(), plans[0].get());

  for (int i = 1; i < CACHE_SIZE; i++)
    cache.insert(std::make_pair(i, plans[i]));

  EXPECT_EQ(CACHE_SIZE, cache.size());
  EXPECT_EQ(false, cache.empty());
}

/**
 * Test iterator function
 */
TEST(CacheTest, Iterator) {
  Cache<uint32_t, const planner::AbstractPlan> cache(
      CACHE_SIZE, bridge::PlanTransformer::CleanPlan);

  std::vector<std::shared_ptr<const planner::AbstractPlan> > plans;
  fill(plans, CACHE_SIZE);
  std::unordered_set<const planner::AbstractPlan *> set;

  for (int i = 0; i < CACHE_SIZE; i++)
    cache.insert(std::make_pair(i, plans[i]));

  for (auto plan : cache) {
    set.insert(plan.get());
  }

  EXPECT_EQ(CACHE_SIZE, set.size());
  EXPECT_EQ(false, cache.empty());
  for (int i = 0; i < CACHE_SIZE; i++) {
    EXPECT_NE(set.end(), set.find(plans[i].get()));
  }
}

/**
 * Test eviction
 *
 * Try to insert 2 times of the capacity of the cache
 * The cache should keep the most recent half
 */
TEST(CacheTest, EvictionByInsert) {
  Cache<uint32_t, const planner::AbstractPlan> cache(
      CACHE_SIZE, bridge::PlanTransformer::CleanPlan);

  std::vector<std::shared_ptr<const planner::AbstractPlan> > plans;
  fill(plans, CACHE_SIZE * 2);
  std::unordered_set<const planner::AbstractPlan *> set;

  for (int i = 0; i < 2 * CACHE_SIZE; i++)
    cache.insert(std::make_pair(i, plans[i]));

  for (auto plan : cache) {
    set.insert(plan.get());
  }

  EXPECT_EQ(CACHE_SIZE, set.size());
  EXPECT_EQ(false, cache.empty());
  for (int i = 0; i < CACHE_SIZE; i++) {
    EXPECT_EQ(set.end(), set.find(plans[i].get()));
  }

  for (int i = CACHE_SIZE; i < 2 * CACHE_SIZE; i++) {
    EXPECT_NE(set.end(), set.find(plans[i].get()));
  }
}

/**
 * Test eviction
 *
 * Try to insert 2 times of the capacity of the cache
 * The cache should keep the most recent half
 */
TEST(CacheTest, EvictionWithAccessing) {
  Cache<uint32_t, const planner::AbstractPlan> cache(
      CACHE_SIZE, bridge::PlanTransformer::CleanPlan);

  std::vector<std::shared_ptr<const planner::AbstractPlan> > plans;
  fill(plans, CACHE_SIZE * 2);
  std::unordered_set<const planner::AbstractPlan *> set;

  int i = 0;
  /* insert 0,1,2,3,4,5,6,7
   *
   * The cache should keep 3,4,5,6,7
   * */
  for (; i < CACHE_SIZE * 1.5; i++)
    cache.insert(std::make_pair(i, plans[i]));
  for (auto plan : cache) {
    set.insert(plan.get());
  }

  EXPECT_EQ(CACHE_SIZE, set.size());
  EXPECT_EQ(false, cache.empty());

  int diff = CACHE_SIZE / 2;

  /* read 4, 3
   * */
  for (int idx = CACHE_SIZE - 1; idx > CACHE_SIZE - diff - 1; idx--) {
    auto it = cache.find(idx);
    EXPECT_NE(it, cache.end());
    EXPECT_EQ((*it).get(), plans[idx].get());
  }

  /* Insert 8, 9 */
  for (; i < CACHE_SIZE * 2; i++)
    cache.insert(std::make_pair(i, plans[i]));

  set.clear();
  for (auto plan : cache) {
    set.insert(plan.get());
  }

  EXPECT_EQ(CACHE_SIZE, set.size());
  EXPECT_EQ(false, cache.empty());

  i = 0;
  for (; i < CACHE_SIZE - diff; i++) {
    EXPECT_EQ(set.end(), set.find(plans[i].get()));
  }

  for (; i < CACHE_SIZE; i++) {
    EXPECT_NE(set.end(), set.find(plans[i].get()));
  }

  for (; i < CACHE_SIZE + diff; i++) {
    EXPECT_EQ(set.end(), set.find(plans[i].get()));
  }

  for (; i < CACHE_SIZE * 2; i++) {
    EXPECT_NE(set.end(), set.find(plans[i].get()));
  }

  i = 0;
  for (; i < CACHE_SIZE; i++) {
    cache.insert(std::make_pair(i, plans[i]));
  }

  set.clear();
  for (auto plan : cache) {
    set.insert(plan.get());
  }

  EXPECT_EQ(CACHE_SIZE, cache.size());
  EXPECT_EQ(false, cache.empty());

  i = 0;
  for (; i < CACHE_SIZE; i++) {
    EXPECT_NE(set.end(), set.find(plans[i].get()));
  }
}

/**
 * Test eviction
 *
 * Try to insert 2 times of the capacity of the cache
 * The cache should keep the most recent half
 */
TEST(CacheTest, Updating) {
  Cache<uint32_t, const planner::AbstractPlan> cache(
      CACHE_SIZE, bridge::PlanTransformer::CleanPlan);

  std::vector<std::shared_ptr<const planner::AbstractPlan> > plans;
  fill(plans, CACHE_SIZE * 2);

  std::unordered_set<const planner::AbstractPlan *> set;

  /* insert 0,1,2,3,4,5,6,7
   *
   * The cache should keep 3,4,5,6,7
   * */
  int i = 0;
  for (; i < CACHE_SIZE * 1.5; i++)
    cache.insert(std::make_pair(i, plans[i]));

  for (auto plan : cache) {
    set.insert(plan.get());
  }

  EXPECT_EQ(CACHE_SIZE, set.size());
  EXPECT_EQ(false, cache.empty());

  int diff = CACHE_SIZE / 2;

  std::vector<std::shared_ptr<const planner::AbstractPlan> > plans2;
  fill(plans2, diff);
  /* update 4, 3
   * */
  for (int idx = CACHE_SIZE - 1, j = 0; idx > CACHE_SIZE - diff - 1;
      idx--, j++) {
    cache.insert(std::make_pair(idx, plans2[j]));
  }

  for (; i < CACHE_SIZE * 2; i++)
    cache.insert(std::make_pair(i, plans[i]));

  set.clear();
  for (auto plan : cache) {
    set.insert(plan.get());
  }

  EXPECT_EQ(CACHE_SIZE, set.size());
  EXPECT_EQ(false, cache.empty());

  i = 0;
  for (; i < CACHE_SIZE - diff; i++) {
    EXPECT_EQ(set.end(), set.find(plans[i].get()));
  }

  for (int j = 0; i < CACHE_SIZE; i++, j++) {
    EXPECT_EQ(set.end(), set.find(plans[i].get()));
    EXPECT_NE(set.end(), set.find(plans2[j].get()));
  }

  for (; i < CACHE_SIZE + diff; i++) {
    EXPECT_EQ(set.end(), set.find(plans[i].get()));
  }

  for (; i < CACHE_SIZE * 2; i++) {
    EXPECT_NE(set.end(), set.find(plans[i].get()));
  }
}

}  // End test namespace
}  // End peloton namespace
