//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// append_test.cpp
//
// Identification: tests/executor/append_test.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "backend/planner/append_plan.h"


#include "backend/common/types.h"
#include "backend/executor/logical_tile.h"
#include "backend/executor/logical_tile_factory.h"
#include "backend/executor/append_executor.h"
#include "backend/storage/data_table.h"

#include "executor/executor_tests_util.h"
#include "executor/mock_executor.h"
#include "harness.h"

using ::testing::NotNull;
using ::testing::Return;

namespace peloton {
namespace test {

namespace {

void RunTest(executor::AppendExecutor &executor, size_t expected_num_tuples) {
  EXPECT_TRUE(executor.Init());

  std::vector<std::unique_ptr<executor::LogicalTile>> result_tiles;
  while (executor.Execute()) {
    result_tiles.emplace_back(executor.GetOutput());
  }

  size_t actual_num_tuples_returned = 0;
  for (auto &tile : result_tiles) {
    actual_num_tuples_returned += tile->GetTupleCount();
  }

  EXPECT_EQ(expected_num_tuples, actual_num_tuples_returned);
}

TEST(AppendTests, AppendTwoTest) {
  // Create the plan node
  planner::AppendPlan node;

  // Create and set up executor
  executor::AppendExecutor executor(&node, nullptr);

  MockExecutor child_executor1;
  MockExecutor child_executor2;

  executor.AddChild(&child_executor1);
  executor.AddChild(&child_executor2);

  EXPECT_CALL(child_executor1, DInit()).WillOnce(Return(true));

  EXPECT_CALL(child_executor2, DInit()).WillOnce(Return(true));

  EXPECT_CALL(child_executor1, DExecute())
      .WillOnce(Return(true))
      .WillOnce(Return(false));

  EXPECT_CALL(child_executor2, DExecute())
      .WillOnce(Return(true))
      .WillOnce(Return(true))
      .WillOnce(Return(false));

  size_t tile_size = 10;
  std::unique_ptr<storage::DataTable> data_table(
      ExecutorTestsUtil::CreateTable(tile_size));
  ExecutorTestsUtil::PopulateTable(data_table.get(), tile_size * 5, false,
                                   false, false);

  std::unique_ptr<executor::LogicalTile> ltile0(
      executor::LogicalTileFactory::WrapTileGroup(data_table->GetTileGroup(0)));

  std::unique_ptr<executor::LogicalTile> ltile1(
      executor::LogicalTileFactory::WrapTileGroup(data_table->GetTileGroup(1)));

  std::unique_ptr<executor::LogicalTile> ltile2(
      executor::LogicalTileFactory::WrapTileGroup(data_table->GetTileGroup(2)));

  EXPECT_CALL(child_executor1, GetOutput()).WillOnce(Return(ltile0.release()));

  EXPECT_CALL(child_executor2, GetOutput())
      .WillOnce(Return(ltile1.release()))
      .WillOnce(Return(ltile2.release()));

  RunTest(executor, tile_size * 3);
}
}
}
}
