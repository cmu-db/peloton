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

MixedPlans PrepareMixedPlan() {

  /////////////////////////////////////////////////////////
  // INDEX SCAN + PREDICATE
  /////////////////////////////////////////////////////////

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

  oid_t column_count = state.column_count + 1;
  
  oid_t begin_read_column_id = 1;
  oid_t end_read_column_id = begin_read_column_id + state.read_column_count - 1;
  
  std::vector<oid_t> read_column_ids;
  for (oid_t col_itr = begin_read_column_id; col_itr <= end_read_column_id; col_itr++) {
    read_column_ids.push_back(col_itr);
  }
  //printf("read column id size=%d\n", (int)read_column_ids.size());
  // Create and set up index scan executor
  planner::IndexScanPlan index_scan_node(
      user_table, predicate, read_column_ids, index_scan_desc);

  executor::IndexScanExecutor *index_scan_executor =
      new executor::IndexScanExecutor(&index_scan_node, nullptr);

  index_scan_executor->Init();

  /////////////////////////////////////////////////////////
  // UPDATE
  /////////////////////////////////////////////////////////

  oid_t begin_update_column_id = 1;
  oid_t end_update_column_id = begin_update_column_id + state.update_column_count - 1;

  std::vector<oid_t> update_column_ids;
  for (oid_t col_itr = begin_update_column_id; col_itr <= end_update_column_id; col_itr++) {
    update_column_ids.push_back(col_itr);
  }
  //printf("update column id size=%d\n", (int)update_column_ids.size());

  planner::IndexScanPlan update_index_scan_node(
      user_table, predicate, update_column_ids, index_scan_desc);
  
  executor::IndexScanExecutor *update_index_scan_executor =
      new executor::IndexScanExecutor(&index_scan_node, nullptr);

  TargetList target_list;
  DirectMapList direct_map_list;

  // update multiple attributes
  for (oid_t col_itr = 0; col_itr < column_count; col_itr++) {
    if (col_itr < begin_update_column_id || col_itr > end_update_column_id) {
      direct_map_list.emplace_back(col_itr,
                                   std::pair<oid_t, oid_t>(0, col_itr));
    }
  }
  //printf("direct_map_list size=%d\n", (int)direct_map_list.size());

  std::unique_ptr<const planner::ProjectInfo> project_info(
      new planner::ProjectInfo(std::move(target_list),
                               std::move(direct_map_list)));
  planner::UpdatePlan update_node(user_table, std::move(project_info));

  executor::UpdateExecutor *update_executor = 
      new executor::UpdateExecutor(&update_node, nullptr);

  update_executor->AddChild(update_index_scan_executor);

  update_executor->Init();

  MixedPlans mixed_plans;

  mixed_plans.index_scan_executor_ = index_scan_executor;

  mixed_plans.update_index_scan_executor_ = update_index_scan_executor;

  mixed_plans.update_executor_ = update_executor;

  return mixed_plans;
}
  
bool RunMixed(MixedPlans &mixed_plans, ZipfDistribution &zipf, fast_random &rng) {

  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(nullptr));

  mixed_plans.SetContext(context.get());

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

  auto txn = txn_manager.BeginTransaction();



  for (int i = 0; i < state.operation_count; i++) {

    auto rng_val = rng.next_uniform();

    if (rng_val < state.update_ratio) {
      /////////////////////////////////////////////////////////
      // PERFORM UPDATE
      /////////////////////////////////////////////////////////

      mixed_plans.update_index_scan_executor_->ResetState();

      // set up parameter values
      std::vector<Value> values;

      auto lookup_key = zipf.GetNextNumber();

      values.push_back(ValueFactory::GetIntegerValue(lookup_key));

      mixed_plans.update_index_scan_executor_->SetValues(values);

      TargetList target_list;

      oid_t begin_update_column_id = 1;
      oid_t end_update_column_id = begin_update_column_id + state.update_column_count - 1;

      for (oid_t col_itr = begin_update_column_id; col_itr <= end_update_column_id; ++col_itr) {
        int update_raw_value = col_itr;
    
        Value update_val = ValueFactory::GetIntegerValue(update_raw_value);

        target_list.emplace_back(
            col_itr, expression::ExpressionUtil::ConstantValueFactory(update_val));
      }

      mixed_plans.update_executor_->SetTargetList(target_list);

      ExecuteUpdateTest(mixed_plans.update_executor_);

      if (txn->GetResult() != Result::RESULT_SUCCESS) {
        txn_manager.AbortTransaction();
        return false;
      }

    } else {
      /////////////////////////////////////////////////////////
      // PERFORM READ
      /////////////////////////////////////////////////////////

      mixed_plans.index_scan_executor_->ResetState();

      // set up parameter values
      std::vector<Value> values;

      auto lookup_key = zipf.GetNextNumber();

      values.push_back(ValueFactory::GetIntegerValue(lookup_key));

      mixed_plans.index_scan_executor_->SetValues(values);

      auto ret_result = ExecuteReadTest(mixed_plans.index_scan_executor_);

      if (txn->GetResult() != Result::RESULT_SUCCESS) {
        txn_manager.AbortTransaction();
        return false;
      }

      if (ret_result.size() != 1) {
        assert(false);
      }
    }
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