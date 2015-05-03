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

namespace nstore {
namespace test {

// Insert a tuple into a table
TEST(InsertTests, BasicTests) {
  // Create insert node for this test.
  storage::Table *table = ExecutorTestsUtil::CreateTable();
  planner::InsertNode node(table);

  std::cout << (*table);

  // Pass through concat executor.
  const txn_id_t txn_id = 1000;
  Context context(txn_id);

  const oid_t tuple_id = 1;
  storage::Tuple *tuple;

  tuple = ExecutorTestsUtil::GetNullTuple(table);
  executor::InsertExecutor executor(&node, &context, tuple);

  try{
    executor.Execute();
  }
  catch(ConstraintException& ce){
    std::cout << ce.what();
  }

  tuple = ExecutorTestsUtil::GetTuple(table, tuple_id);
  executor::InsertExecutor executor2(&node, &context, tuple);

  executor2.Execute();

  try{
    executor2.Execute();
  }
  catch(ConstraintException& ce){
    std::cout << ce.what();
  }

  std::cout << (*table);

}

} // namespace test
} // namespace nstore
