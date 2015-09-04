//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// index_scan_test.cpp
//
// Identification: tests/executor/index_scan_test.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "backend/planner/index_scan_plan.h"


#include "backend/common/types.h"
#include "backend/executor/logical_tile.h"
#include "backend/executor/logical_tile_factory.h"
#include "backend/executor/index_scan_executor.h"
#include "backend/storage/data_table.h"

#include "executor/executor_tests_util.h"
#include "harness.h"

using ::testing::NotNull;
using ::testing::Return;

namespace peloton {
namespace test {

// Index scan of table with index predicate.
TEST(IndexScanTests, IndexPredicateTest) {
  // First, generate the table with index
  std::unique_ptr<storage::DataTable> data_table(
      ExecutorTestsUtil::CreateAndPopulateTable());

  // Column ids to be added to logical tile after scan.
  std::vector<oid_t> column_ids({0, 1, 3});

  //===--------------------------------------------------------------------===//
  // ATTR 0 <= 110
  //===--------------------------------------------------------------------===//

  auto index = data_table->GetIndex(0);
  std::vector<oid_t> key_column_ids;
  std::vector<ExpressionType> expr_types;
  std::vector<Value> values;
  std::vector<expression::AbstractExpression *>runtime_keys;

  key_column_ids.push_back(0);
  expr_types.push_back(ExpressionType::EXPRESSION_TYPE_COMPARE_LESSTHANOREQUALTO);
  values.push_back(ValueFactory::GetIntegerValue(110));

  // Create index scan desc

  planner::IndexScanPlan::IndexScanDesc index_scan_desc(index, key_column_ids,
                                                        expr_types, values, runtime_keys);


  expression::AbstractExpression *predicate = nullptr;

  // Create plan node.
  planner::IndexScanPlan node(data_table.get(), predicate, column_ids,
                              index_scan_desc);

  auto &txn_manager = concurrency::TransactionManager::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));

  // Run the executor
  executor::IndexScanExecutor executor(&node, context.get());
  int expected_num_tiles = 3;

  EXPECT_TRUE(executor.Init());

  std::vector<std::unique_ptr<executor::LogicalTile>> result_tiles;

  for (int i = 0; i < expected_num_tiles; i++) {
    EXPECT_TRUE(executor.Execute());
    std::unique_ptr<executor::LogicalTile> result_tile(executor.GetOutput());
    EXPECT_THAT(result_tile, NotNull());
    result_tiles.emplace_back(result_tile.release());
  }

  EXPECT_FALSE(executor.Execute());
  EXPECT_EQ(result_tiles.size(), expected_num_tiles);
  EXPECT_EQ(result_tiles[0].get()->GetTupleCount(), 5);
  EXPECT_EQ(result_tiles[1].get()->GetTupleCount(), 5);
  EXPECT_EQ(result_tiles[2].get()->GetTupleCount(), 2);

  txn_manager.CommitTransaction(txn);
}

TEST(IndexScanTests, MultiColumnPredicateTest) {
  // First, generate the table with index
  std::unique_ptr<storage::DataTable> data_table(
      ExecutorTestsUtil::CreateAndPopulateTable());

  // Column ids to be added to logical tile after scan.
  std::vector<oid_t> column_ids({0, 1, 3});

  //===--------------------------------------------------------------------===//
  // ATTR 1 > 50 & ATTR 0 < 70
  //===--------------------------------------------------------------------===//

  auto index = data_table->GetIndex(1);
  std::vector<oid_t> key_column_ids;
  std::vector<ExpressionType> expr_types;
  std::vector<Value> values;
  std::vector<expression::AbstractExpression *>runtime_keys;

  key_column_ids.push_back(1);
  key_column_ids.push_back(0);
  expr_types.push_back(ExpressionType::EXPRESSION_TYPE_COMPARE_GREATERTHAN);
  expr_types.push_back(ExpressionType::EXPRESSION_TYPE_COMPARE_LESSTHAN);
  values.push_back(ValueFactory::GetIntegerValue(50));
  values.push_back(ValueFactory::GetIntegerValue(70));

  // Create index scan desc

  planner::IndexScanPlan::IndexScanDesc index_scan_desc(index, key_column_ids,
                                                        expr_types, values, runtime_keys);

  expression::AbstractExpression *predicate = nullptr;

  // Create plan node.
  planner::IndexScanPlan node(data_table.get(), predicate, column_ids,
                              index_scan_desc);

  auto &txn_manager = concurrency::TransactionManager::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));

  // Run the executor
  executor::IndexScanExecutor executor(&node, context.get());
  int expected_num_tiles = 1;

  EXPECT_TRUE(executor.Init());

  std::vector<std::unique_ptr<executor::LogicalTile>> result_tiles;

  for (int i = 0; i < expected_num_tiles; i++) {
    EXPECT_TRUE(executor.Execute());
    std::unique_ptr<executor::LogicalTile> result_tile(executor.GetOutput());
    EXPECT_THAT(result_tile, NotNull());
    result_tiles.emplace_back(result_tile.release());
  }

  EXPECT_FALSE(executor.Execute());
  EXPECT_EQ(result_tiles.size(), expected_num_tiles);
  EXPECT_EQ(result_tiles[0].get()->GetTupleCount(), 2);

  txn_manager.CommitTransaction(txn);
}

}  // namespace test
}  // namespace peloton
