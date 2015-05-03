/**
 * @brief Test cases for insert node.
 *
 * Copyright(c) 2015, CMU
 */

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "harness.h"

#include "common/exception.h"
#include "common/value.h"
#include "common/value_factory.h"
#include "executor/insert_executor.h"
#include "executor/logical_tile.h"
#include "executor/logical_tile_factory.h"
#include "planner/insert_node.h"
#include "storage/backend_vm.h"
#include "storage/tile.h"
#include "storage/tile_group.h"

#include "executor/executor_tests_util.h"

#include <atomic>

namespace nstore {
namespace test {

std::atomic<int> tuple_id;

void InsertTuple(storage::Table *table){

  planner::InsertNode node(table);
  const txn_id_t txn_id = 1000;
  Context context(txn_id);

  for(int tuple_itr = 0 ; tuple_itr < 500 ; tuple_itr++) {
    storage::Tuple *tuple = ExecutorTestsUtil::GetTuple(table, ++tuple_id);
    executor::InsertExecutor executor(&node, &context, tuple);
    executor.Execute();
  }

  std::cout << "Done. \n";
}

// Insert a tuple into a table
TEST(InsertTests, BasicTests) {

  // Create insert node for this test.
  storage::Table *table = ExecutorTestsUtil::CreateTable();
  planner::InsertNode node(table);

  // Pass through insert executor.
  const txn_id_t txn_id = 1000;
  Context context(txn_id);
  storage::Tuple *tuple;

  tuple = ExecutorTestsUtil::GetNullTuple(table);
  executor::InsertExecutor executor(&node, &context, tuple);

  try{
    executor.Execute();
  }
  catch(ConstraintException& ce){
    std::cout << ce.what();
  }

  tuple = ExecutorTestsUtil::GetTuple(table, ++tuple_id);
  executor::InsertExecutor executor2(&node, &context, tuple);
  executor2.Execute();

  try{
    executor2.Execute();
  }
  catch(ConstraintException& ce){
    std::cout << ce.what();
  }

  LaunchParallelTest(8, InsertTuple, table);

  auto pkey_index = table->GetIndex(0);
  auto sec_index = table->GetIndex(1);

  std::cout << "PKEY INDEX :: " << pkey_index->Size() << "\n";
  std::cout << "SEC INDEX :: " << sec_index->Size() << "\n";

  std::cout << (*table);
}

} // namespace test
} // namespace nstore
