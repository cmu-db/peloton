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


#include "backend/benchmark/tpcc/tpcc_workload.h"
#include "backend/benchmark/tpcc/tpcc_configuration.h"
#include "backend/benchmark/tpcc/tpcc_loader.h"

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
#include "backend/executor/nested_loop_join_executor.h"
#include "backend/executor/aggregate_executor.h"

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
#include "backend/planner/project_info.h"
#include "backend/planner/nested_loop_join_plan.h"
#include "backend/planner/aggregate_plan.h"

#include "backend/storage/data_table.h"
#include "backend/storage/table_factory.h"



namespace peloton {
namespace benchmark {
namespace tpcc {

bool RunStockLevel() {
  /*
     "STOCK_LEVEL": {
     "getOId": "SELECT D_NEXT_O_ID FROM DISTRICT WHERE D_W_ID = ? AND D_ID = ?",
     "getStockCount": "SELECT COUNT(DISTINCT(OL_I_ID)) FROM ORDER_LINE, STOCK  WHERE OL_W_ID = ? AND OL_D_ID = ? AND OL_O_ID < ? AND OL_O_ID >= ? AND S_W_ID = ? AND S_I_ID = OL_I_ID AND S_QUANTITY < ?
     }
   */
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  std::unique_ptr<executor::ExecutorContext> context(
    new executor::ExecutorContext(txn));

  // Prepare random data
  int w_id = GetRandomInteger(0, state.warehouse_count - 1);
  int d_id = GetRandomInteger(0, state.districts_per_warehouse - 1);
  int threshold = GetRandomInteger(stock_min_threshold, stock_max_threshold);

  LOG_INFO("getOId: SELECT D_NEXT_O_ID FROM DISTRICT WHERE D_W_ID = ? AND D_ID = ?");

  // Construct index scan executor
  std::vector<oid_t> district_column_ids = {COL_IDX_D_NEXT_O_ID};
  std::vector<oid_t> district_key_column_ids = {COL_IDX_D_W_ID, COL_IDX_D_ID};
  std::vector<ExpressionType> district_expr_types;
  std::vector<Value> district_key_values;
  std::vector<expression::AbstractExpression *> runtime_keys;

  district_expr_types.push_back(ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);
  district_key_values.push_back(ValueFactory::GetSmallIntValue(w_id));
  district_expr_types.push_back(ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);
  district_key_values.push_back(ValueFactory::GetTinyIntValue(d_id));

  auto district_pkey_index = district_table->GetIndexWithOid(district_table_pkey_index_oid);
  planner::IndexScanPlan::IndexScanDesc district_index_scan_desc(
    district_pkey_index, district_key_column_ids, district_expr_types,
    district_key_values, runtime_keys
  );

  expression::AbstractExpression *predicate = nullptr;
  planner::IndexScanPlan district_index_scan_node(
    district_table, predicate,
    district_column_ids, district_index_scan_desc
  );
  executor::IndexScanExecutor district_index_scan_executor(&district_index_scan_node, context.get());

  auto districts = ExecuteReadTest(&district_index_scan_executor);
  if (txn->GetResult() != Result::RESULT_SUCCESS) {
    txn_manager.AbortTransaction();
    return false;
  }
  assert(districts.size() == 1);

  Value o_id = districts[0][0];

  LOG_INFO("getStockCount: SELECT COUNT(DISTINCT(OL_I_ID)) FROM ORDER_LINE, STOCK  WHERE OL_W_ID = ? AND OL_D_ID = ? AND OL_O_ID < ? AND OL_O_ID >= ? AND S_W_ID = ? AND S_I_ID = OL_I_ID AND S_QUANTITY < ?");

  //////////////////////////////////////////////////////////////////
  ///////////// Construct left table index scan ////////////////////
  //////////////////////////////////////////////////////////////////
  std::vector<oid_t> order_line_column_ids = {COL_IDX_OL_I_ID};
  std::vector<oid_t> order_line_key_column_ids = {COL_IDX_OL_W_ID, COL_IDX_OL_D_ID, COL_IDX_OL_O_ID, COL_IDX_OL_O_ID};
  std::vector<ExpressionType> order_line_expr_types;
  std::vector<Value> order_line_key_values;

  order_line_expr_types.push_back(ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);
  order_line_key_values.push_back(ValueFactory::GetSmallIntValue(w_id));
  order_line_expr_types.push_back(ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);
  order_line_key_values.push_back(ValueFactory::GetTinyIntValue(d_id));
  order_line_expr_types.push_back(ExpressionType::EXPRESSION_TYPE_COMPARE_LESSTHAN);
  order_line_key_values.push_back(o_id);
  order_line_expr_types.push_back(ExpressionType::EXPRESSION_TYPE_COMPARE_GREATERTHANOREQUALTO);
  order_line_key_values.push_back(ValueFactory::GetIntegerValue(
    ValuePeeker::PeekInteger(o_id) - 20));

  auto order_line_pkey_index = order_line_table->GetIndexWithOid(order_line_table_pkey_index_oid);
  planner::IndexScanPlan::IndexScanDesc order_line_index_scan_desc(
    order_line_pkey_index, order_line_key_column_ids, order_line_expr_types,
    order_line_key_values, runtime_keys);

  predicate = nullptr;

  planner::IndexScanPlan order_line_index_scan_node(order_line_table,
    predicate, order_line_column_ids, order_line_index_scan_desc);

  executor::IndexScanExecutor order_line_index_scan_executor(&order_line_index_scan_node, context.get());

  //////////////////////////////////////////////////////////////////
  ///////////// Construct right table index scan ///////////////////
  //////////////////////////////////////////////////////////////////
  std::vector<oid_t> stock_column_ids = {COL_IDX_S_I_ID};

  std::vector<oid_t> stock_key_column_ids = {COL_IDX_S_W_ID};
  std::vector<ExpressionType> stock_expr_types;
  std::vector<Value> stock_key_values;

  stock_expr_types.push_back(
      ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);
  stock_key_values.push_back(ValueFactory::GetSmallIntValue(w_id));

  auto stock_pkey_index = stock_table->GetIndexWithOid(
      stock_table_pkey_index_oid);
  
  planner::IndexScanPlan::IndexScanDesc stock_index_scan_desc(
      stock_pkey_index, stock_key_column_ids, stock_expr_types,
      stock_key_values, runtime_keys);

  // Add predicate S_QUANTITY < threshold
  auto tuple_val_expr = expression::ExpressionUtil::TupleValueFactory(VALUE_TYPE_INTEGER, 0, COL_IDX_S_QUANTITY);
  auto constant_val_expr = expression::ExpressionUtil::ConstantValueFactory(
      ValueFactory::GetIntegerValue(threshold));
  predicate = expression::ExpressionUtil::ComparisonFactory(
    EXPRESSION_TYPE_COMPARE_EQUAL, tuple_val_expr, constant_val_expr);

  planner::IndexScanPlan stock_index_scan_node(stock_table, predicate,
                                               stock_column_ids,
                                               stock_index_scan_desc);

  executor::IndexScanExecutor stock_index_scan_executor(&stock_index_scan_node,
                                                            context.get());

  ////////////////////////////////////////////////
  ////////////// Join ////////////////////////////
  ////////////////////////////////////////////////
  // Schema
  auto order_line_table_schema = order_line_table->GetSchema();
  std::shared_ptr<const catalog::Schema> join_schema(new catalog::Schema({
    order_line_table_schema->GetColumn(COL_IDX_OL_I_ID)
  }));
  // Projection
  TargetList target_list;
  DirectMapList direct_map_list = {{0, {0, 0}}}; // ORDER_LINE.OL_I_ID
  std::unique_ptr<const planner::ProjectInfo> projection(
      new planner::ProjectInfo(std::move(target_list),
                               std::move(direct_map_list)));
  // Predicate left.0 == right.0
  auto left_table_attr0 = expression::ExpressionUtil::TupleValueFactory(VALUE_TYPE_INTEGER, 0, 0);
  auto right_table_attr0 = expression::ExpressionUtil::TupleValueFactory(VALUE_TYPE_INTEGER, 1, 0);
  std::unique_ptr<const expression::AbstractExpression> join_predicate( expression::ExpressionUtil::ComparisonFactory(
    EXPRESSION_TYPE_COMPARE_EQUAL, left_table_attr0, right_table_attr0));

  // Join
  planner::NestedLoopJoinPlan join_plan(JOIN_TYPE_INNER, std::move(join_predicate), std::move(projection), join_schema);
  executor::NestedLoopJoinExecutor join_executor(&join_plan, context.get());
  join_executor.AddChild(&order_line_index_scan_executor);

  ////////////////////////////////////////////////
  ////////////// Aggregator //////////////////////
  ////////////////////////////////////////////////
  std::vector<oid_t> group_by_columns;
  DirectMapList aggregate_direct_map_list = {{0, {1, 0}}};
  TargetList aggregate_target_list;

  std::unique_ptr<const planner::ProjectInfo> aggregate_projection(
      new planner::ProjectInfo(std::move(aggregate_target_list),
                               std::move(aggregate_direct_map_list)));

  std::vector<planner::AggregatePlan::AggTerm> agg_terms;
  bool distinct = true;
  planner::AggregatePlan::AggTerm count_distinct(
      EXPRESSION_TYPE_AGGREGATE_COUNT,
      expression::ExpressionUtil::TupleValueFactory(VALUE_TYPE_INTEGER, 0, 0),
      distinct);
  agg_terms.push_back(count_distinct);

  // The schema is the same as the join operator
  std::shared_ptr<const catalog::Schema> aggregate_schema(new catalog::Schema({
    order_line_table_schema->GetColumn(COL_IDX_OL_I_ID)
  }));

  planner::AggregatePlan count_distinct_node(
    std::move(aggregate_projection), nullptr, std::move(agg_terms),
    std::move(group_by_columns), aggregate_schema,
    AGGREGATE_TYPE_PLAIN);

  executor::AggregateExecutor count_distinct_executor(&count_distinct_node, context.get());
  count_distinct_executor.AddChild(&join_executor);

  auto count_result = ExecuteReadTest(&count_distinct_executor);
  if (txn->GetResult() != Result::RESULT_SUCCESS) {
    txn_manager.AbortTransaction();
    return false;
  }
  assert(count_result.size() == 1);

  assert(txn->GetResult() == Result::RESULT_SUCCESS);

  auto result = txn_manager.CommitTransaction();

  if (result == Result::RESULT_SUCCESS) {
    return true;
  } else {
    return false;
  }

  return true;
}

}
}
}