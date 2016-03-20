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
#include "backend/concurrency/transaction.h"

#include "backend/executor/executor_context.h"
#include "backend/executor/abstract_executor.h"
#include "backend/executor/logical_tile.h"
#include "backend/executor/logical_tile_factory.h"
#include "backend/executor/materialization_executor.h"
#include "backend/executor/update_executor.h"
#include "backend/executor/index_scan_executor.h"

#include "backend/expression/abstract_expression.h"
#include "backend/expression/constant_value_expression.h"
#include "backend/expression/tuple_value_expression.h"
#include "backend/expression/comparison_expression.h"

#include "backend/index/index_factory.h"

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

std::ofstream out("outputfile.summary");

static void WriteOutput(double stat);

Timer<std::ratio<1, 1> > timer;

/////////////////////////////////////////////////////////
// TRANSACTION TYPES
/////////////////////////////////////////////////////////

void RunRead();

/////////////////////////////////////////////////////////
// WORKLOAD
/////////////////////////////////////////////////////////

void RunWorkload() {
  auto txn_count = state.transactions;

  // Reset timer
  timer.Reset();

  // Run these many transactions
  for (oid_t txn_itr = 0; txn_itr < txn_count; txn_itr++) {
    RunRead();
  }

  double throughput = txn_count/(timer.GetDuration());

  WriteOutput(throughput);
}

static void WriteOutput(double stat) {
  std::cout << "----------------------------------------------------------\n";
  std::cout << state.update_ratio << " "
      << state.scale_factor << " "
      << state.column_count << " :: ";
  std::cout << stat << " tps\n";

  out << state.column_count << " ";
  out << state.update_ratio << " ";
  out << state.scale_factor << " ";
  out << stat << "\n";
  out.flush();
}

/////////////////////////////////////////////////////////
// HARNESS
/////////////////////////////////////////////////////////

static void ExecuteTest(std::vector<executor::AbstractExecutor *> &executors) {
  std::chrono::time_point<std::chrono::system_clock> start, end;
  bool status = false;

  timer.Start();

  // Run all the executors
  for (auto executor : executors) {
    status = executor->Init();
    if (status == false) {
      throw Exception("Init failed");
    }

    std::vector<std::unique_ptr<executor::LogicalTile>> result_tiles;

    // Execute stuff
    while (executor->Execute() == true) {
      std::unique_ptr<executor::LogicalTile> result_tile(
          executor->GetOutput());
      result_tiles.emplace_back(result_tile.release());
    }
  }

  timer.Stop();

}

/////////////////////////////////////////////////////////
// TRANSACTIONS
/////////////////////////////////////////////////////////

void RunRead() {
  auto &txn_manager = concurrency::TransactionManager::GetInstance();

  auto txn = txn_manager.BeginTransaction();

  /////////////////////////////////////////////////////////
  // INDEX SCAN + PREDICATE
  /////////////////////////////////////////////////////////

  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));

  // Column ids to be added to logical tile after scan.
  std::vector<oid_t> column_ids;
  oid_t column_count = state.column_count + 1;

  for (oid_t col_itr = 0; col_itr < column_count; col_itr++) {
    column_ids.push_back(col_itr);
  }

  // Create and set up index scan executor

  std::vector<oid_t> key_column_ids;
  std::vector<ExpressionType> expr_types;
  std::vector<Value> values;
  std::vector<expression::AbstractExpression *> runtime_keys;

  auto tuple_count = state.scale_factor * DEFAULT_TUPLES_PER_TILEGROUP;
  auto lookup_key = rand() % tuple_count;

  key_column_ids.push_back(0);
  expr_types.push_back(
      ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);
  values.push_back(ValueFactory::GetIntegerValue(lookup_key));

  auto ycsb_pkey_index = ycsb_table->GetIndexWithOid(YCSB_TABLE_PKEY_INDEX_OID);

  planner::IndexScanPlan::IndexScanDesc index_scan_desc(
      ycsb_pkey_index, key_column_ids, expr_types, values, runtime_keys);

  // Create plan node.
  auto predicate = nullptr;

  planner::IndexScanPlan index_scan_node(ycsb_table,
                                         predicate, column_ids,
                                         index_scan_desc);

  // Run the executor
  executor::IndexScanExecutor index_scan_executor(&index_scan_node,
                                                  context.get());

  /////////////////////////////////////////////////////////
  // MATERIALIZE
  /////////////////////////////////////////////////////////

  // Create and set up materialization executor
  std::unordered_map<oid_t, oid_t> old_to_new_cols;
  for (oid_t col_itr = 0; col_itr < column_count; col_itr++) {
    old_to_new_cols[col_itr] = col_itr;
  }

  auto output_schema = catalog::Schema::CopySchema(ycsb_table->GetSchema());
  bool physify_flag = true;  // is going to create a physical tile
  planner::MaterializationPlan mat_node(old_to_new_cols,
                                        output_schema,
                                        physify_flag);

  executor::MaterializationExecutor mat_executor(&mat_node, nullptr);
  mat_executor.AddChild(&index_scan_executor);

  /////////////////////////////////////////////////////////
  // EXECUTE
  /////////////////////////////////////////////////////////

  std::vector<executor::AbstractExecutor *> executors;
  executors.push_back(&mat_executor);

  ExecuteTest(executors);

  txn_manager.CommitTransaction(txn);
}

}  // namespace ycsb
}  // namespace benchmark
}  // namespace peloton
