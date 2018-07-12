//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// loader_test.cpp
//
// Identification: test/executor/loader_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include <memory>
#include <string>
#include <utility>
#include <vector>
#include <atomic>

#include "executor/testing_executor_util.h"
#include "common/harness.h"

#include "catalog/schema.h"
#include "type/value_factory.h"
#include "concurrency/transaction_manager_factory.h"

#include "executor/executor_context.h"
#include "executor/insert_executor.h"
#include "executor/logical_tile_factory.h"
#include "expression/expression_util.h"
#include "expression/tuple_value_expression.h"
#include "expression/comparison_expression.h"
#include "expression/abstract_expression.h"
#include "storage/tile.h"
#include "storage/tile_group.h"
#include "storage/table_factory.h"

#include "executor/mock_executor.h"

#include "planner/insert_plan.h"

using ::testing::NotNull;
using ::testing::Return;

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Loader Tests
//===--------------------------------------------------------------------===//

class LoaderTests : public PelotonTest {};

std::atomic<int> loader_tuple_id;

//===------------------------------===//
// Utility
//===------------------------------===//

/**
 * Cook a ProjectInfo object from a tuple.
 * Simply use a ConstantValueExpression for each attribute.
 */
static std::unique_ptr<const planner::ProjectInfo> MakeProjectInfoFromTuple(
    const storage::Tuple *tuple) {
  TargetList target_list;
  DirectMapList direct_map_list;

  for (oid_t col_id = START_OID; col_id < tuple->GetColumnCount(); col_id++) {
    type::Value value = (
        tuple->GetValue(col_id));
    auto expression = expression::ExpressionUtil::ConstantValueFactory(value);
    planner::DerivedAttribute attribute{expression};
    target_list.emplace_back(col_id, attribute);
  }

  return std::unique_ptr<const planner::ProjectInfo>(new planner::ProjectInfo(
      std::move(target_list), std::move(direct_map_list)));
}

void InsertTuple(storage::DataTable *table, type::AbstractPool *pool,
                 oid_t tilegroup_count_per_loader,
                 UNUSED_ATTRIBUTE uint64_t thread_itr) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

  oid_t tuple_count = tilegroup_count_per_loader * TEST_TUPLES_PER_TILEGROUP;

  // Start a txn for each insert
  auto txn = txn_manager.BeginTransaction();
  std::unique_ptr<storage::Tuple> tuple(
      TestingExecutorUtil::GetTuple(table, ++loader_tuple_id, pool));

  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));

  auto project_info = MakeProjectInfoFromTuple(tuple.get());

  planner::InsertPlan node(table, std::move(project_info));

  // Insert the desired # of tuples
  for (oid_t tuple_itr = 0; tuple_itr < tuple_count; tuple_itr++) {
    executor::InsertExecutor executor(&node, context.get());
    executor.Execute();
  }

  txn_manager.CommitTransaction(txn);
}

TEST_F(LoaderTests, LoadingTest) {
  // We are going to simply load tile groups concurrently in this test
  // WARNING: This test may potentially run for a long time if
  // TEST_TUPLES_PER_TILEGROUP is large, consider rewrite the test or hard
  // code the number of tuples per tile group in this test
  oid_t tuples_per_tilegroup = TEST_TUPLES_PER_TILEGROUP;
  bool build_indexes = false;

  // Control the scale
  oid_t loader_threads_count = 1;
  oid_t tilegroup_count_per_loader = 1002;

  // Each tuple size ~40 B.
  UNUSED_ATTRIBUTE oid_t tuple_size = 41;

  std::unique_ptr<storage::DataTable> data_table(
      TestingExecutorUtil::CreateTable(tuples_per_tilegroup, build_indexes));

  auto testing_pool = TestingHarness::GetInstance().GetTestingPool();

  LaunchParallelTest(loader_threads_count, InsertTuple, data_table.get(),
                     testing_pool, tilegroup_count_per_loader);

  auto expected_tile_group_count = 0;

  int total_tuple_count = loader_threads_count * tilegroup_count_per_loader * TEST_TUPLES_PER_TILEGROUP;
  int max_cached_tuple_count =
      TEST_TUPLES_PER_TILEGROUP * storage::DataTable::GetActiveTileGroupCount();
  int max_unfill_cached_tuple_count =
      (TEST_TUPLES_PER_TILEGROUP - 1) *
      storage::DataTable::GetActiveTileGroupCount();

  if (total_tuple_count - max_cached_tuple_count <= 0) {
    if (total_tuple_count <= max_unfill_cached_tuple_count) {
      expected_tile_group_count = storage::DataTable::GetActiveTileGroupCount();
    } else {
      expected_tile_group_count =
          storage::DataTable::GetActiveTileGroupCount() + total_tuple_count -
          max_unfill_cached_tuple_count;
    }
  } else {
    int filled_tile_group_count = total_tuple_count / max_cached_tuple_count * storage::DataTable::GetActiveTileGroupCount();
    
    if (total_tuple_count - filled_tile_group_count * TEST_TUPLES_PER_TILEGROUP - max_unfill_cached_tuple_count <= 0) {
      expected_tile_group_count = filled_tile_group_count +
                                  storage::DataTable::GetActiveTileGroupCount();
    } else {
      expected_tile_group_count =
          filled_tile_group_count +
          storage::DataTable::GetActiveTileGroupCount() +
          (total_tuple_count - filled_tile_group_count -
           max_unfill_cached_tuple_count);
    }
  }

  UNUSED_ATTRIBUTE auto bytes_to_megabytes_converter = (1024 * 1024);

  EXPECT_EQ(data_table->GetTileGroupCount(), expected_tile_group_count);

  LOG_TRACE("Dataset size : %u MB \n",
            (expected_tile_group_count * tuples_per_tilegroup * tuple_size) /
                bytes_to_megabytes_converter);
}

}  // namespace test
}  // namespace peloton
