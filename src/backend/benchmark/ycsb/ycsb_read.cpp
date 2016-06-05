//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// workload.cpp
//
// Identification: benchmark/tpcc/workload.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//



#include <memory>
#include <string>
#include <unordered_map>
#include <vector>
#include <chrono>
#include <iostream>
#include <ctime>
#include <cassert>
#include <thread>
#include <algorithm>
#include <random>
#include <cstddef>
#include <limits>

#include "backend/benchmark/ycsb/ycsb_workload.h"
#include "backend/benchmark/ycsb/ycsb_configuration.h"
#include "backend/benchmark/ycsb/ycsb_loader.h"

#include "backend/catalog/manager.h"
#include "backend/catalog/schema.h"

#include "backend/common/types.h"
#include "backend/common/value.h"
#include "backend/common/value_factory.h"
#include "backend/common/logger.h"
#include "backend/common/timer.h"
#include "backend/common/generator.h"

#include "backend/concurrency/transaction.h"
#include "backend/concurrency/transaction_manager_factory.h"

#include "backend/executor/executor_context.h"
#include "backend/executor/abstract_executor.h"
#include "backend/executor/logical_tile.h"
#include "backend/executor/logical_tile_factory.h"
#include "backend/executor/materialization_executor.h"
#include "backend/executor/update_executor.h"
#include "backend/executor/index_scan_executor.h"
#include "backend/executor/insert_executor.h"

#include "backend/expression/abstract_expression.h"
#include "backend/expression/constant_value_expression.h"
#include "backend/expression/tuple_value_expression.h"
#include "backend/expression/comparison_expression.h"
#include "backend/expression/expression_util.h"
#include "backend/expression/container_tuple.h"

#include "backend/index/index_factory.h"

#include "backend/logging/log_manager.h"

#include "backend/planner/abstract_plan.h"
#include "backend/planner/materialization_plan.h"
#include "backend/planner/insert_plan.h"
#include "backend/planner/update_plan.h"
#include "backend/planner/index_scan_plan.h"

#include "backend/storage/data_table.h"
#include "backend/storage/table_factory.h"



namespace peloton {
namespace benchmark {
namespace ycsb {

ReadPlans PrepareReadPlan() {

  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(nullptr));

  std::vector<oid_t> key_column_ids;
  std::vector<ExpressionType> expr_types;
  key_column_ids.push_back(0);
  expr_types.push_back(ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);

  std::vector<Value> values;

  std::vector<expression::AbstractExpression *> runtime_keys;

  auto ycsb_pkey_index = user_table->GetIndexWithOid(user_table_pkey_index_oid);

  planner::IndexScanPlan::IndexScanDesc index_scan_desc(
      ycsb_pkey_index, key_column_ids, expr_types, values, runtime_keys);

  // Create plan node.
  auto predicate = nullptr;

  // Column ids to be added to logical tile after scan.
  std::vector<oid_t> column_ids;
  oid_t column_count = state.column_count + 1;

  for (oid_t col_itr = 0; col_itr < column_count; col_itr++) {
    column_ids.push_back(col_itr);
  }  

  planner::IndexScanPlan index_scan_node(
      user_table, predicate, column_ids, index_scan_desc);


  executor::IndexScanExecutor *index_scan_executor =
      new executor::IndexScanExecutor(&index_scan_node, context.get());

  index_scan_executor->Init();

  ReadPlans read_plans;
  read_plans.index_scan_executor_ = index_scan_executor;
  
  return read_plans;
}


bool RunRead(ReadPlans &read_plans, ZipfDistribution &zipf) {
  

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  
  auto txn = txn_manager.BeginTransaction();

  read_plans.ResetState();

  std::vector<Value> values;

  auto lookup_key = zipf.GetNextNumber();

  values.push_back(ValueFactory::GetIntegerValue(lookup_key));

  // printf("perform read, key = %d\n", (int)lookup_key);
  
  read_plans.index_scan_executor_->SetValues(values);

  /////////////////////////////////////////////////////////
  // EXECUTE
  /////////////////////////////////////////////////////////

  auto ret_result = ExecuteReadTest(read_plans.index_scan_executor_);

  if (txn->GetResult() != Result::RESULT_SUCCESS) {
    txn_manager.AbortTransaction();
    return false;
  }

  if (ret_result.size() != 1) {
    assert(false);
  }

  // transaction passed execution.
  assert(txn->GetResult() == Result::RESULT_SUCCESS);

  auto result = txn_manager.CommitTransaction();

  if (result == Result::RESULT_SUCCESS) {
    return true;
    
  } else {
    // transaction failed commitment.
    assert(result == Result::RESULT_ABORTED ||
           result == Result::RESULT_FAILURE);
    return false;
  }
}

}
}
}