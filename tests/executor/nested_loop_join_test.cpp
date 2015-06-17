/**
 * @brief Test cases for nested loop join node.
 *
 * Copyright(c) 2015, CMU
 */

#include <memory>

#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "common/types.h"
#include "executor/logical_tile.h"
#include "executor/logical_tile_factory.h"
#include "executor/nested_loop_join_executor.h"
#include "planner/nested_loop_join_node.h"

#include "executor/executor_tests_util.h"
#include "harness.h"

using ::testing::NotNull;
using ::testing::Return;

namespace nstore {
namespace test {

// Cartesian Product Test
TEST(NestedLoopJoinTests, CartesianProductTest) {

  // Create plan node.
  //planner::NestedLoopJoinNode node(nullptr);

  // Run the executor
  //executor::NestedLoopJoinExecutor executor(&node);

  //EXPECT_TRUE(executor.Init());
}

} // namespace test
} // namespace nstore
