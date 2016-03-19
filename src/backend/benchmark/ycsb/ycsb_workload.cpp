//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// workload.cpp
//
// Identification: benchmark/ycsb/workload.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/benchmark/ycsb/ycsb_workload.h"

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

#include "backend/expression/expression_util.h"
#include "backend/brain/clusterer.h"
#include "backend/catalog/manager.h"
#include "backend/catalog/schema.h"
#include "backend/common/types.h"
#include "backend/common/value.h"
#include "backend/common/value_factory.h"
#include "backend/common/logger.h"
#include "backend/concurrency/transaction.h"

#include "backend/executor/executor_context.h"
#include "backend/executor/abstract_executor.h"
#include "backend/executor/aggregate_executor.h"
#include "backend/executor/seq_scan_executor.h"
#include "backend/executor/logical_tile.h"
#include "backend/executor/logical_tile_factory.h"
#include "backend/executor/materialization_executor.h"
#include "backend/executor/projection_executor.h"
#include "backend/executor/insert_executor.h"
#include "backend/executor/update_executor.h"
#include "backend/executor/nested_loop_join_executor.h"

#include "backend/expression/abstract_expression.h"
#include "backend/expression/constant_value_expression.h"
#include "backend/expression/tuple_value_expression.h"
#include "backend/expression/comparison_expression.h"
#include "backend/expression/conjunction_expression.h"

#include "backend/index/index_factory.h"

#include "backend/planner/abstract_plan.h"
#include "backend/planner/aggregate_plan.h"
#include "backend/planner/materialization_plan.h"
#include "backend/planner/seq_scan_plan.h"
#include "backend/planner/insert_plan.h"
#include "backend/planner/update_plan.h"
#include "backend/planner/projection_plan.h"
#include "backend/planner/nested_loop_join_plan.h"

#include "backend/storage/tile.h"
#include "backend/storage/tile_group.h"
#include "backend/storage/tile_group_header.h"
#include "backend/storage/data_table.h"
#include "backend/storage/table_factory.h"
#include "ycsb_loader.h"

namespace peloton {
namespace benchmark {
namespace ycsb {

std::ofstream out("outputfile.summary");

static void WriteOutput(double duration) {
  // Convert to ms
  duration *= 1000;

  std::cout << "----------------------------------------------------------\n";
  std::cout << state.update_ratio << " "
      << state.scale_factor << " "
      << state.column_count << " :: ";
  std::cout << duration << " ms\n";

  out << state.column_count << " ";
  out << state.update_ratio << " ";
  out << state.scale_factor << " ";
  out << duration << "\n";
  out.flush();
}

// Tuple id counter
oid_t ycsb_tuple_counter = -1000000;

expression::AbstractExpression *CreatePredicate(const int lower_bound) {
  // ATTR0 >= LOWER_BOUND

  // First, create tuple value expression.
  expression::AbstractExpression *tuple_value_expr =
      expression::ExpressionUtil::TupleValueFactory(0, 0);

  // Second, create constant value expression.
  Value constant_value = ValueFactory::GetIntegerValue(lower_bound);

  expression::AbstractExpression *constant_value_expr =
      expression::ExpressionUtil::ConstantValueFactory(constant_value);

  // Finally, link them together using an greater than expression.
  expression::AbstractExpression *predicate = expression::ExpressionUtil::ComparisonFactory(
      EXPRESSION_TYPE_COMPARE_GREATERTHANOREQUALTO, tuple_value_expr,
      constant_value_expr);

  return predicate;
}

static int GetLowerBound() {
  const int tuple_count = state.scale_factor * DEFAULT_TUPLES_PER_TILEGROUP;
  const int lower_bound = tuple_count;

  return lower_bound;
}

static void ExecuteTest(std::vector<executor::AbstractExecutor *> &executors) {
  std::chrono::time_point<std::chrono::system_clock> start, end;

  auto txn_count = state.transactions;
  bool status = false;

  start = std::chrono::system_clock::now();

  // Run these many transactions
  for (oid_t txn_itr = 0; txn_itr < txn_count; txn_itr++) {

    // Run all the executors
    for (auto executor : executors) {
      status = executor->Init();
      if (status == false) throw Exception("Init failed");

      std::vector<std::unique_ptr<executor::LogicalTile>> result_tiles;

      while (executor->Execute() == true) {
        std::unique_ptr<executor::LogicalTile> result_tile(
            executor->GetOutput());
        result_tiles.emplace_back(result_tile.release());
      }

      // Execute stuff
      executor->Execute();
    }
  }


  end = std::chrono::system_clock::now();
  std::chrono::duration<double> elapsed_seconds = end - start;
  double time_per_transaction = ((double)elapsed_seconds.count()) / txn_count;

  WriteOutput(time_per_transaction);
}

void RunWorkload() {
  const int lower_bound = GetLowerBound();
  const bool is_inlined = true;
  auto &txn_manager = concurrency::TransactionManager::GetInstance();

  auto txn = txn_manager.BeginTransaction();

  /////////////////////////////////////////////////////////
  // SEQ SCAN + PREDICATE
  /////////////////////////////////////////////////////////

  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));

  // Column ids to be added to logical tile after scan.
  std::vector<oid_t> column_ids;
  oid_t column_count = state.column_count;

  for (oid_t col_itr = 0; col_itr < column_count; col_itr++) {
    column_ids.push_back(col_itr);
  }

  // Create and set up seq scan executor
  auto predicate = CreatePredicate(lower_bound);
  planner::SeqScanPlan seq_scan_node(ycsb_table, predicate, column_ids);

  executor::SeqScanExecutor seq_scan_executor(&seq_scan_node, context.get());

  /////////////////////////////////////////////////////////
  // MATERIALIZE
  /////////////////////////////////////////////////////////

  // Create and set up materialization executor
  std::vector<catalog::Column> output_columns;
  std::unordered_map<oid_t, oid_t> old_to_new_cols;
  oid_t col_itr = 0;
  for (auto column_id : column_ids) {
    auto column =
        catalog::Column(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER),
                        "" + std::to_string(column_id), is_inlined);
    output_columns.push_back(column);

    old_to_new_cols[col_itr] = col_itr;
    col_itr++;
  }

  std::unique_ptr<catalog::Schema> output_schema(
      new catalog::Schema(output_columns));
  bool physify_flag = true;  // is going to create a physical tile
  planner::MaterializationPlan mat_node(old_to_new_cols,
                                        output_schema.release(), physify_flag);

  executor::MaterializationExecutor mat_executor(&mat_node, nullptr);
  mat_executor.AddChild(&seq_scan_executor);

  /////////////////////////////////////////////////////////
  // INSERT
  /////////////////////////////////////////////////////////

  std::vector<Value> values;
  Value insert_val = ValueFactory::GetIntegerValue(++ycsb_tuple_counter);

  planner::ProjectInfo::TargetList target_list;
  planner::ProjectInfo::DirectMapList direct_map_list;

  for (auto col_id = 0; col_id <= state.column_count; col_id++) {
    auto expression = expression::ExpressionUtil::ConstantValueFactory(insert_val);
    target_list.emplace_back(col_id, expression);
  }

  auto project_info = new planner::ProjectInfo(std::move(target_list),
                                               std::move(direct_map_list));

  auto orig_tuple_count = state.scale_factor * DEFAULT_TUPLES_PER_TILEGROUP;
  auto bulk_insert_count = state.update_ratio * orig_tuple_count;

  planner::InsertPlan insert_node(ycsb_table, project_info,
                                  bulk_insert_count);
  executor::InsertExecutor insert_executor(&insert_node, context.get());

  /////////////////////////////////////////////////////////
  // EXECUTE
  /////////////////////////////////////////////////////////

  std::vector<executor::AbstractExecutor *> executors;
  executors.push_back(&mat_executor);
  executors.push_back(&insert_executor);

  ExecuteTest(executors);

  txn_manager.CommitTransaction(txn);
}


}  // namespace ycsb
}  // namespace benchmark
}  // namespace peloton
