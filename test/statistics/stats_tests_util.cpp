//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// transaction_tests_util.cpp
//
// Identification: tests/concurrency/transaction_tests_util.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "statistics/stats_tests_util.h"
#include "backend/executor/executor_context.h"
#include "backend/executor/delete_executor.h"
#include "backend/executor/insert_executor.h"
#include "backend/executor/seq_scan_executor.h"
#include "backend/executor/update_executor.h"
#include "backend/executor/logical_tile_factory.h"
#include "backend/expression/expression_util.h"
#include "backend/storage/tile.h"
#include "executor/mock_executor.h"
#include "backend/planner/delete_plan.h"
#include "backend/planner/insert_plan.h"
#include "executor/executor_tests_util.h"

namespace peloton {
namespace test {

storage::Tuple StatsTestsUtil::PopulateTuple(const catalog::Schema *schema,
                                             int first_col_val,
                                             int second_col_val,
                                             int third_col_val,
                                             int fourth_col_val) {
  auto testing_pool = TestingHarness::GetInstance().GetTestingPool();
  storage::Tuple tuple(schema, true);
  tuple.SetValue(
      0, ValueFactory::GetIntegerValue(first_col_val), testing_pool);

  tuple.SetValue(
      1, ValueFactory::GetIntegerValue(second_col_val), testing_pool);

  tuple.SetValue(2, ValueFactory::GetDoubleValue(third_col_val), testing_pool);

  Value string_value =
      ValueFactory::GetStringValue(std::to_string(fourth_col_val));
  tuple.SetValue(3, string_value, testing_pool);
  return tuple;
}

}
}
