//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// ycsb_mixed.cpp
//
// Identification: src/main/ycsb/ycsb_mixed.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
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

#include "benchmark/ycsb/ycsb_workload.h"
#include "benchmark/ycsb/ycsb_configuration.h"
#include "benchmark/ycsb/ycsb_loader.h"

#include "catalog/manager.h"
#include "catalog/schema.h"

#include "common/types.h"
#include "common/value.h"
#include "common/value_factory.h"
#include "common/logger.h"
#include "common/timer.h"
#include "common/generator.h"

#include "concurrency/transaction.h"
#include "concurrency/transaction_manager_factory.h"

#include "executor/executor_context.h"
#include "executor/abstract_executor.h"
#include "executor/logical_tile.h"
#include "executor/logical_tile_factory.h"
#include "executor/materialization_executor.h"
#include "executor/update_executor.h"
#include "executor/index_scan_executor.h"
#include "executor/insert_executor.h"

#include "expression/abstract_expression.h"
#include "expression/constant_value_expression.h"
#include "expression/tuple_value_expression.h"
#include "expression/comparison_expression.h"
#include "expression/expression_util.h"
#include "expression/container_tuple.h"

#include "index/index_factory.h"

#include "logging/log_manager.h"

#include "planner/abstract_plan.h"
#include "planner/materialization_plan.h"
#include "planner/insert_plan.h"
#include "planner/update_plan.h"
#include "planner/index_scan_plan.h"

#include "storage/data_table.h"
#include "storage/table_factory.h"

namespace peloton {
namespace benchmark {
namespace ycsb {

  
bool RunMixed(ZipfDistribution &zipf, FastRandom &rng) {

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

  concurrency::Transaction *txn = txn_manager.BeginTransaction();

  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));

  // Column ids to be added to logical tile.
  std::vector<oid_t> column_ids;
  oid_t column_count = state.column_count + 1;

  // read all the attributes in a tuple.
  for (oid_t col_itr = 0; col_itr < column_count; col_itr++) {
    column_ids.push_back(col_itr);
  }

  // Create and set up index scan executor
  std::vector<oid_t> key_column_ids;
  std::vector<ExpressionType> expr_types;

  key_column_ids.push_back(0);
  expr_types.push_back(ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);

  std::vector<expression::AbstractExpression *> runtime_keys;
  
  for (int i = 0; i < state.operation_count; i++) {

    auto rng_val = rng.NextUniform();

    if (rng_val < state.update_ratio) {
      /////////////////////////////////////////////////////////
      // PERFORM UPDATE
      /////////////////////////////////////////////////////////

      // set up parameter values
      std::vector<common::Value > values;

      auto lookup_key = zipf.GetNextNumber();

      values.push_back(common::ValueFactory::GetIntegerValue(lookup_key).Copy());

      auto ycsb_pkey_index = user_table->GetIndexWithOid(user_table_pkey_index_oid);
    
      planner::IndexScanPlan::IndexScanDesc index_scan_desc(
          ycsb_pkey_index, key_column_ids, expr_types, values, runtime_keys);

      // Create plan node.
      auto predicate = nullptr;

      planner::IndexScanPlan index_scan_node(user_table, predicate, column_ids,
                                             index_scan_desc);

      // Run the executor
      executor::IndexScanExecutor index_scan_executor(&index_scan_node,
                                                      context.get());

      TargetList target_list;
      DirectMapList direct_map_list;

      // update multiple attributes
      for (oid_t col_itr = 0; col_itr < column_count; col_itr++) {
        if (col_itr == 1) {
          if (state.string_mode == true) {

            std::string update_raw_value(100, 'a');
            common::Value update_val = common::ValueFactory::GetVarcharValue(update_raw_value).Copy();
            target_list.emplace_back(
                  col_itr, expression::ExpressionUtil::ConstantValueFactory(update_val));             

          } else {

            int update_raw_value = 1;
            common::Value update_val = common::ValueFactory::GetIntegerValue(update_raw_value).Copy();
            target_list.emplace_back(
                  col_itr, expression::ExpressionUtil::ConstantValueFactory(update_val)); 
          }
        }
        else {
          direct_map_list.emplace_back(col_itr,
                                       std::pair<oid_t, oid_t>(0, col_itr));
        }
      }

      std::unique_ptr<const planner::ProjectInfo> project_info(
          new planner::ProjectInfo(std::move(target_list),
                                   std::move(direct_map_list)));
      planner::UpdatePlan update_node(user_table, std::move(project_info));

      executor::UpdateExecutor update_executor(&update_node, context.get());

      update_executor.AddChild(&index_scan_executor);

      ExecuteUpdate(&update_executor);

      if (txn->GetResult() != Result::RESULT_SUCCESS) {
        txn_manager.AbortTransaction(txn);
        return false;
      }

    } else {
      /////////////////////////////////////////////////////////
      // PERFORM READ
      /////////////////////////////////////////////////////////

      // set up parameter values
      std::vector<common::Value > values;

      auto lookup_key = zipf.GetNextNumber();

      values.push_back(common::ValueFactory::GetIntegerValue(lookup_key).Copy());

      auto ycsb_pkey_index = user_table->GetIndexWithOid(user_table_pkey_index_oid);
    
      planner::IndexScanPlan::IndexScanDesc index_scan_desc(
          ycsb_pkey_index, key_column_ids, expr_types, values, runtime_keys);

      // Create plan node.
      auto predicate = nullptr;

      planner::IndexScanPlan index_scan_node(user_table, predicate, column_ids,
                                             index_scan_desc);

      // Run the executor
      executor::IndexScanExecutor index_scan_executor(&index_scan_node,
                                                      context.get());

      
      
      ExecuteRead(&index_scan_executor);

      if (txn->GetResult() != Result::RESULT_SUCCESS) {
        txn_manager.AbortTransaction(txn);
        return false;
      }
    }
  }

  // transaction passed execution.
  PL_ASSERT(txn->GetResult() == Result::RESULT_SUCCESS);

  auto result = txn_manager.CommitTransaction(txn);

  if (result == Result::RESULT_SUCCESS) {
    return true;
    
  } else {
    // transaction failed commitment.
    PL_ASSERT(result == Result::RESULT_ABORTED ||
           result == Result::RESULT_FAILURE);
    return false;
  }
}

}
}
}
