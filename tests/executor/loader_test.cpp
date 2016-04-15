//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// loader_test.cpp
//
// Identification: tests/executor/loader_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include <string>
#include <utility>
#include <vector>
#include <atomic>

#include "harness.h"

#include "backend/catalog/schema.h"
#include "backend/common/value_factory.h"
#include "backend/common/pool.h"
#include "backend/concurrency/transaction_manager_factory.h"

#include "backend/executor/executor_context.h"
#include "backend/executor/insert_executor.h"
#include "backend/executor/logical_tile_factory.h"
#include "backend/expression/expression_util.h"
#include "backend/expression/tuple_value_expression.h"
#include "backend/expression/comparison_expression.h"
#include "backend/expression/abstract_expression.h"
#include "backend/storage/tile.h"
#include "backend/storage/tile_group.h"
#include "backend/storage/table_factory.h"

#include "executor_tests_util.h"
#include "executor/mock_executor.h"

#include "backend/planner/insert_plan.h"

using ::testing::NotNull;
using ::testing::Return;

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Loader Tests
//===--------------------------------------------------------------------===//

class LoaderTests : public PelotonTest {};

std::atomic<int> tuple_id;

//===------------------------------===//
// Utility
//===------------------------------===//

/**
 * Cook a ProjectInfo object from a tuple.
 * Simply use a ConstantValueExpression for each attribute.
 */
std::unique_ptr<const planner::ProjectInfo>
MakeProjectInfoFromTuple(const storage::Tuple *tuple) {
  planner::ProjectInfo::TargetList target_list;
  planner::ProjectInfo::DirectMapList direct_map_list;

  for (oid_t col_id = START_OID; col_id < tuple->GetColumnCount(); col_id++) {
    auto value = tuple->GetValue(col_id);
    auto expression = expression::ExpressionUtil::ConstantValueFactory(value);
    target_list.emplace_back(col_id, expression);
  }

  return std::unique_ptr<const planner::ProjectInfo>(
      new planner::ProjectInfo(std::move(target_list),
                               std::move(direct_map_list)));
}

void InsertTuple(storage::DataTable *table, VarlenPool *pool,
                 oid_t tilegroup_count_per_loader) {
  auto &txn_manager = concurrency::OptimisticTxnManager::GetInstance();

  oid_t tuple_count = tilegroup_count_per_loader * DEFAULT_TUPLES_PER_TILEGROUP;

  // Start a txn for each insert
  auto txn = txn_manager.BeginTransaction();
  std::unique_ptr<storage::Tuple> tuple(
      ExecutorTestsUtil::GetTuple(table, ++tuple_id, pool));

  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));

  auto project_info = MakeProjectInfoFromTuple(tuple.get());

  planner::InsertPlan node(table, std::move(project_info));

  // Insert the desired # of tuples
  for (oid_t tuple_itr = 0; tuple_itr < tuple_count; tuple_itr++) {
    executor::InsertExecutor executor(&node, context.get());
    executor.Execute();
  }

  txn_manager.CommitTransaction();
}

TEST_F(LoaderTests, LoadingTest) {
  // We are going to simply load tile groups concurrently in this test
  oid_t tuples_per_tilegroup = DEFAULT_TUPLES_PER_TILEGROUP;
  bool build_indexes = false;

  // Control the scale
  oid_t loader_threads_count = 2;
  oid_t tilegroup_count_per_loader = 1;

  // Each tuple size ~40 B.
  oid_t tuple_size = 41;

  std::unique_ptr<storage::DataTable> data_table(
      ExecutorTestsUtil::CreateTable(tuples_per_tilegroup, build_indexes));

  auto testing_pool = TestingHarness::GetInstance().GetTestingPool();

  LaunchParallelTest(loader_threads_count, InsertTuple, data_table.get(),
                     testing_pool, tilegroup_count_per_loader);

  auto expected_tile_group_count =
      loader_threads_count * tilegroup_count_per_loader + 1;
  auto bytes_to_megabytes_converter = (1024 * 1024);

  EXPECT_EQ(data_table->GetTileGroupCount(), expected_tile_group_count);

  LOG_INFO("Dataset size : %lu MB \n",
           (expected_tile_group_count * tuples_per_tilegroup * tuple_size) /
           bytes_to_megabytes_converter);
}

}  // namespace test
}  // namespace peloton
