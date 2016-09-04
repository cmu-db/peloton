//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// stats_tests_util.cpp
//
// Identification: tests/include/statistics/stats_tests_util.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "statistics/stats_tests_util.h"
#include "executor/executor_context.h"
#include "executor/delete_executor.h"
#include "executor/insert_executor.h"
#include "executor/seq_scan_executor.h"
#include "executor/update_executor.h"
#include "executor/logical_tile_factory.h"
#include "expression/expression_util.h"
#include "storage/tile.h"
#include "executor/mock_executor.h"
#include "planner/delete_plan.h"
#include "planner/insert_plan.h"
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
  tuple.SetValue(0, common::ValueFactory::GetIntegerValue(first_col_val), testing_pool);

  tuple.SetValue(1, common::ValueFactory::GetIntegerValue(second_col_val),
                 testing_pool);

  tuple.SetValue(2, common:: ValueFactory::GetDoubleValue(third_col_val), testing_pool);

  common::VarlenValue string_value =
	common::ValueFactory::GetVarcharValue(std::to_string(fourth_col_val));
  tuple.SetValue(3, string_value, testing_pool);
  return tuple;
}
}  // namespace test
}  // namespace peloton
