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
#include "backend/catalog/column.h"

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
#include "backend/executor/limit_executor.h"
#include "backend/executor/aggregate_executor.h"
#include "backend/executor/delete_executor.h"

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
#include "backend/planner/limit_plan.h"
#include "backend/planner/project_info.h"
#include "backend/planner/aggregate_plan.h"
#include "backend/planner/delete_plan.h"

#include "backend/storage/data_table.h"
#include "backend/storage/table_factory.h"



namespace peloton {
namespace benchmark {
namespace tpcc {

bool RunDelivery(const size_t &thread_id){
  /*
   "DELIVERY": {
   "getNewOrder": "SELECT NO_O_ID FROM NEW_ORDER WHERE NO_D_ID = ? AND NO_W_ID = ? AND NO_O_ID > -1 LIMIT 1", #
   "deleteNewOrder": "DELETE FROM NEW_ORDER WHERE NO_D_ID = ? AND NO_W_ID = ? AND NO_O_ID = ?", # d_id, w_id, no_o_id
   "getCId": "SELECT O_C_ID FROM ORDERS WHERE O_ID = ? AND O_D_ID = ? AND O_W_ID = ?", # no_o_id, d_id, w_id
   "updateOrders": "UPDATE ORDERS SET O_CARRIER_ID = ? WHERE O_ID = ? AND O_D_ID = ? AND O_W_ID = ?", # o_carrier_id, no_o_id, d_id, w_id
   "updateOrderLine": "UPDATE ORDER_LINE SET OL_DELIVERY_D = ? WHERE OL_O_ID = ? AND OL_D_ID = ? AND OL_W_ID = ?", # o_entry_d, no_o_id, d_id, w_id
   "sumOLAmount": "SELECT SUM(OL_AMOUNT) FROM ORDER_LINE WHERE OL_O_ID = ? AND OL_D_ID = ? AND OL_W_ID = ?", # no_o_id, d_id, w_id
   "updateCustomer": "UPDATE CUSTOMER SET C_BALANCE = C_BALANCE + ? WHERE C_ID = ? AND C_D_ID = ? AND C_W_ID = ?", # ol_total, c_id, d_id, w_id
   }
   */

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));


  int warehouse_id = GenerateWarehouseId(thread_id);
  int o_carrier_id = GetRandomInteger(orders_min_carrier_id, orders_max_carrier_id);
  int64_t ol_delivery_d = time(0);

  // TODO: cache the plan nodes. How Peloton support prepared statments?

  for (int d_id = 0; d_id < state.districts_per_warehouse; ++d_id) {
    LOG_INFO("getNewOrder: SELECT NO_O_ID FROM NEW_ORDER WHERE NO_D_ID = ? AND NO_W_ID = ? AND NO_O_ID > -1 LIMIT 1");

    // Construct index scan executor
    std::vector<oid_t> new_order_column_ids = {COL_IDX_NO_O_ID};
    std::vector<oid_t> new_order_key_column_ids = {COL_IDX_NO_D_ID, COL_IDX_O_W_ID, COL_IDX_NO_O_ID};
    std::vector<ExpressionType> new_order_expr_types;
    std::vector<Value> new_order_key_values;
    std::vector<expression::AbstractExpression *> runtime_keys;

    new_order_expr_types.push_back(ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);
    new_order_key_values.push_back(ValueFactory::GetTinyIntValue(d_id));
    new_order_expr_types.push_back(ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);
    new_order_key_values.push_back(ValueFactory::GetIntegerValue(warehouse_id));
    new_order_expr_types.push_back(ExpressionType::EXPRESSION_TYPE_COMPARE_GREATERTHAN);    
    new_order_key_values.push_back(ValueFactory::GetIntegerValue(-1));

    // Get the index
    auto new_order_pkey_index = new_order_table->GetIndexWithOid(new_order_table_pkey_index_oid);
    planner::IndexScanPlan::IndexScanDesc new_order_idex_scan_desc(
      new_order_pkey_index, new_order_key_column_ids, new_order_expr_types,
      new_order_key_values, runtime_keys);

    auto predicate = nullptr;

    planner::IndexScanPlan new_order_idex_scan_node(new_order_table,
      predicate, new_order_column_ids, new_order_idex_scan_desc);

    executor::IndexScanExecutor new_order_index_scan_executor(
      &new_order_idex_scan_node, context.get());

    // Construct limit executor
    size_t limit = 1;
    size_t offset = 0;
    planner::LimitPlan limit_node(limit, offset);
    executor::LimitExecutor limit_executor(&limit_node, context.get());
    limit_executor.AddChild(&new_order_index_scan_executor);

    auto new_order_ids = ExecuteReadTest(&limit_executor);
    if (txn->GetResult() != Result::RESULT_SUCCESS) {
      txn_manager.AbortTransaction();
      return false;
    }

    assert(new_order_ids.size() == 1);
    assert(new_order_ids[0].size() == 1);

    // result: NO_O_ID
    auto no_o_id = new_order_ids[0][0];

    LOG_INFO("getCId: SELECT O_C_ID FROM ORDERS WHERE O_ID = ? AND O_D_ID = ? AND O_W_ID = ?");

    std::vector<oid_t> orders_column_ids = {COL_IDX_O_C_ID};
    std::vector<oid_t> orders_key_column_ids = {COL_IDX_O_ID, COL_IDX_O_D_ID, COL_IDX_O_W_ID};
    std::vector<ExpressionType> orders_expr_types;
    std::vector<Value> orders_key_values;

    orders_expr_types.push_back(ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);
    orders_key_values.push_back(no_o_id);
    orders_expr_types.push_back(ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);
    orders_key_values.push_back(ValueFactory::GetTinyIntValue(d_id));
    orders_expr_types.push_back(ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);
    orders_key_values.push_back(ValueFactory::GetIntegerValue(warehouse_id));

    // Get the index
    auto orders_pkey_index = orders_table->GetIndexWithOid(orders_table_pkey_index_oid);
    planner::IndexScanPlan::IndexScanDesc orders_index_scan_desc(
      orders_pkey_index, orders_key_column_ids, orders_expr_types,
      orders_key_values, runtime_keys);

    predicate = nullptr;

    // Create the index scan plan node
    planner::IndexScanPlan orders_index_scan_node(orders_table,
      predicate, orders_column_ids, orders_index_scan_desc);

    // Create the executors
    executor::IndexScanExecutor orders_index_scan_executor(
      &orders_index_scan_node, context.get());

    auto orders_ids = ExecuteReadTest(&orders_index_scan_executor);
    if (txn->GetResult() != Result::RESULT_SUCCESS) {
      txn_manager.AbortTransaction();
      return false;
    }

    assert(orders_ids.size() == 1);
    assert(orders_ids[0].size() == 1);

    // Result: O_C_ID
    auto c_id = orders_ids[0][0];

    LOG_INFO("sumOLAmount: SELECT SUM(OL_AMOUNT) FROM ORDER_LINE WHERE OL_O_ID = ? AND OL_D_ID = ? AND OL_W_ID = ?");

    // Construct index scan executor
    std::vector<oid_t> order_line_column_ids = {COL_IDX_OL_AMOUNT};
    std::vector<oid_t> order_line_key_column_ids = {COL_IDX_OL_O_ID, COL_IDX_OL_D_ID, COL_IDX_OL_W_ID};
    std::vector<ExpressionType> order_line_expr_types;
    std::vector<Value> order_line_key_values;

    order_line_expr_types.push_back(ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);
    order_line_key_values.push_back(no_o_id);
    order_line_expr_types.push_back(ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);
    order_line_key_values.push_back(ValueFactory::GetTinyIntValue(d_id));
    order_line_expr_types.push_back(ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);
    order_line_key_values.push_back(ValueFactory::GetIntegerValue(warehouse_id));

    auto order_line_pkey_index = order_line_table->GetIndexWithOid(order_line_table_pkey_index_oid);
    planner::IndexScanPlan::IndexScanDesc order_line_index_scan_desc(
      order_line_pkey_index, order_line_key_column_ids, order_line_expr_types,
      order_line_key_values, runtime_keys);

    predicate = nullptr;

    planner::IndexScanPlan order_line_index_scan_node(order_line_table,
      predicate, order_line_column_ids, order_line_index_scan_desc);

    executor::IndexScanExecutor order_line_index_scan_executor(&order_line_index_scan_node, context.get());

    // Construct aggregate executor
    std::vector<oid_t> group_by_columns;
    // Prepare proj info
    DirectMapList direct_map_list = {{0, {1, 0}}};
    std::unique_ptr<const planner::ProjectInfo> proj_info(
      new planner::ProjectInfo(TargetList(),
                              std::move(direct_map_list)));
    // Set up aggregators
    std::vector<planner::AggregatePlan::AggTerm> agg_terms;
    planner::AggregatePlan::AggTerm sum(
      EXPRESSION_TYPE_AGGREGATE_SUM,
      expression::ExpressionUtil::TupleValueFactory(VALUE_TYPE_INTEGER, 0, 0));
      // Should this be the column id of the output tuple or original tuple?
    agg_terms.push_back(sum);

    predicate = nullptr;

    auto orders_table_schema = orders_table->GetSchema();
    std::vector<catalog::Column> columns = {orders_table_schema->GetColumn(COL_IDX_OL_AMOUNT)};

    std::shared_ptr<const catalog::Schema> output_table_schema(
      new catalog::Schema(columns));

    // Create the aggregate plan
    planner::AggregatePlan sum_node(
      std::move(proj_info), predicate, std::move(agg_terms),
      std::move(group_by_columns), output_table_schema,
      AGGREGATE_TYPE_PLAIN);

    executor::AggregateExecutor sum_executor(&sum_node, context.get());
    sum_executor.AddChild(&order_line_index_scan_executor);

    auto sum_result = ExecuteReadTest(&sum_executor);
    if (txn->GetResult() != Result::RESULT_SUCCESS) {
      txn_manager.AbortTransaction();
      return false;
    }

    assert(sum_result.size() == 1);
    assert(sum_result[0].size() == 1);

    auto ol_total = sum_result[0][0];

    LOG_INFO("deleteNewOrder: DELETE FROM NEW_ORDER WHERE NO_D_ID = ? AND NO_W_ID = ? AND NO_O_ID = ?");

    // Construct index scan executor
    std::vector<oid_t> new_order_column_ids2 = {0};
    std::vector<oid_t> new_order_key_column_ids2 = {COL_IDX_NO_D_ID, COL_IDX_NO_W_ID, COL_IDX_NO_O_ID};
    std::vector<ExpressionType> new_order_expr_types2;
    std::vector<Value> new_order_key_values2;

    new_order_expr_types2.push_back(ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);
    new_order_key_values2.push_back(ValueFactory::GetTinyIntValue(d_id));
    new_order_expr_types2.push_back(ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);
    new_order_key_values2.push_back(ValueFactory::GetIntegerValue(warehouse_id));
    new_order_expr_types2.push_back(ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);    
    new_order_key_values2.push_back(no_o_id);

    // Get the index
    auto new_order_pkey_index2 = new_order_table->GetIndexWithOid(new_order_table_pkey_index_oid);
    planner::IndexScanPlan::IndexScanDesc new_order_idex_scan_desc2(
      new_order_pkey_index2, new_order_key_column_ids2, new_order_expr_types2,
      new_order_key_values2, runtime_keys);

    predicate = nullptr;

    // Create index scan plan node
    planner::IndexScanPlan new_order_idex_scan_node2(new_order_table,
      predicate, new_order_column_ids2, new_order_idex_scan_desc2);

    // Create executors
    executor::IndexScanExecutor new_order_index_scan_executor2(
      &new_order_idex_scan_node2, context.get());

    // Construct delete executor
    bool truncate = false;
    planner::DeletePlan new_order_delete_node(new_order_table, truncate);
    executor::DeleteExecutor new_order_delete_executor(&new_order_delete_node, context.get());
    new_order_delete_executor.AddChild(&new_order_index_scan_executor2);

    ExecuteDeleteTest(&new_order_delete_executor);
    if (txn->GetResult() != Result::RESULT_SUCCESS) {
      txn_manager.AbortTransaction();
      return false;
    }

    LOG_INFO("updateOrders: UPDATE ORDERS SET O_CARRIER_ID = ? WHERE O_ID = ? AND O_D_ID = ? AND O_W_ID = ?");

    // Construct index scan executor
    std::vector<oid_t> orders_column_ids2 = {COL_IDX_O_CARRIER_ID};

    predicate = nullptr;

    // Reuse the index scan desc created above since nothing different
    planner::IndexScanPlan orders_index_scan_node2(
      orders_table, predicate, orders_column_ids2, orders_index_scan_desc);

    executor::IndexScanExecutor orders_index_scan_executor2(
      &orders_index_scan_node2, context.get());

    // Construct update executor
    TargetList orders_target_list;
    DirectMapList orders_direct_map_list;

    size_t orders_column_count = 8;
    for (oid_t col_itr = 0; col_itr < orders_column_count; col_itr++) {
      // Skip O_CARRIER_ID
      if (col_itr != COL_IDX_O_CARRIER_ID)
        orders_direct_map_list.emplace_back(col_itr,
          std::make_pair(0, col_itr));
    }

    Value orders_update_val = ValueFactory::GetIntegerValue(
      o_carrier_id);
    orders_target_list.emplace_back(
      COL_IDX_O_CARRIER_ID, expression::ExpressionUtil::ConstantValueFactory(
        orders_update_val));

    std::unique_ptr<const planner::ProjectInfo> orders_project_info(
      new planner::ProjectInfo(std::move(orders_target_list), 
                               std::move(orders_direct_map_list)));
    planner::UpdatePlan orders_update_node(orders_table, std::move(orders_project_info));

    executor::UpdateExecutor orders_update_executor(&orders_update_node,
      context.get());
    orders_update_executor.AddChild(&orders_index_scan_executor2);

    ExecuteUpdateTest(&orders_update_executor);
    if (txn->GetResult() != Result::RESULT_SUCCESS) {
      txn_manager.AbortTransaction();
      return false;
    }

    LOG_INFO("updateOrderLine: UPDATE ORDER_LINE SET OL_DELIVERY_D = ? WHERE OL_O_ID = ? AND OL_D_ID = ? AND OL_W_ID = ?");

    // Construct index scan executor
    std::vector<oid_t> order_line_column_ids2 = {COL_IDX_OL_DELIVERY_D};

    predicate = nullptr;
    planner::IndexScanPlan order_line_index_scan_node2(order_line_table, predicate, order_line_column_ids2, order_line_index_scan_desc);
    executor::IndexScanExecutor order_line_index_scan_executor2(
      &order_line_index_scan_node2, context.get());

    // Construct update executor
    TargetList order_line_target_list;
    DirectMapList order_line_direct_map_list;

    size_t order_line_column_count = 10;
    for (oid_t col_itr = 0; col_itr < order_line_column_count; col_itr++) {
      // Skip OL_DELIVERY_D
      if (col_itr != COL_IDX_OL_DELIVERY_D)
        order_line_direct_map_list.emplace_back(col_itr,
          std::make_pair(0, col_itr));
    }

    Value order_line_update_val = ValueFactory::GetTimestampValue(
      ol_delivery_d);
    order_line_target_list.emplace_back(
      COL_IDX_OL_DELIVERY_D, expression::ExpressionUtil::ConstantValueFactory(
        order_line_update_val));

    std::unique_ptr<const planner::ProjectInfo> order_line_project_info(
      new planner::ProjectInfo(std::move(order_line_target_list), 
                               std::move(order_line_direct_map_list)));
    planner::UpdatePlan order_line_update_node(order_line_table, std::move(order_line_project_info));

    executor::UpdateExecutor order_line_update_executor(&order_line_update_node,
      context.get());
    order_line_update_executor.AddChild(&order_line_index_scan_executor2);

    ExecuteUpdateTest(&order_line_update_executor);
    if (txn->GetResult() != Result::RESULT_SUCCESS) {
      txn_manager.AbortTransaction();
      return false;
    }    

    LOG_INFO("updateCustomer: UPDATE CUSTOMER SET C_BALANCE = C_BALANCE + ? WHERE C_ID = ? AND C_D_ID = ? AND C_W_ID = ?");

    // Construct index scan executor
    std::vector<oid_t> customer_column_ids = {COL_IDX_C_BALANCE};
    std::vector<oid_t> customer_key_column_ids = {COL_IDX_C_ID, COL_IDX_C_D_ID, COL_IDX_C_W_ID};
    std::vector<ExpressionType> customer_expr_types;
    std::vector<Value> customer_key_values;

    customer_expr_types.push_back(ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);
    customer_key_values.push_back(c_id);
    customer_expr_types.push_back(
      ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);
    customer_key_values.push_back(ValueFactory::GetTinyIntValue(d_id));
    customer_expr_types.push_back(
      ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);
    customer_key_values.push_back(ValueFactory::GetIntegerValue(warehouse_id));

    auto customer_pkey_index = customer_table->GetIndexWithOid(customer_table_pkey_index_oid);

    planner::IndexScanPlan::IndexScanDesc customer_index_scan_desc(customer_pkey_index, customer_key_column_ids, customer_expr_types,
      customer_key_values, runtime_keys);

    predicate = nullptr;
    planner::IndexScanPlan customer_index_scan_node(customer_table, predicate,
      customer_column_ids, customer_index_scan_desc);

    executor::IndexScanExecutor customer_index_scan_executor(&customer_index_scan_node, context.get());

    // Construct update executor
    TargetList customer_target_list;
    DirectMapList customer_direct_map_list;

    size_t customer_column_count = 21;
    for (oid_t col_itr = 0; col_itr < customer_column_count; col_itr++) {
      // Skip OL_DELIVERY_D
      if (col_itr != COL_IDX_C_BALANCE)
        customer_direct_map_list.emplace_back(col_itr,
          std::make_pair(0, col_itr));
    }

    // Expressions
    // Tuple value expression
    auto tuple_val_expr = expression::ExpressionUtil::TupleValueFactory(
      VALUE_TYPE_INTEGER, 0, COL_IDX_C_BALANCE);
    // Constant value expression
    auto constant_val_expr = expression::ExpressionUtil::ConstantValueFactory(
      ol_total);
    // + operator expression
    auto plus_operator_expr = expression::ExpressionUtil::OperatorFactory(
      EXPRESSION_TYPE_OPERATOR_PLUS, VALUE_TYPE_INTEGER, tuple_val_expr, constant_val_expr);

    customer_target_list.emplace_back(
      COL_IDX_C_BALANCE, plus_operator_expr);

    std::unique_ptr<const planner::ProjectInfo> customer_project_info(
      new planner::ProjectInfo(std::move(customer_target_list), 
                               std::move(customer_direct_map_list)));
    planner::UpdatePlan customer_update_node(customer_table, std::move(customer_project_info));

    executor::UpdateExecutor customer_update_executor(&customer_update_node,
      context.get());
    customer_update_executor.AddChild(&customer_index_scan_executor);

    ExecuteUpdateTest(&customer_update_executor);
    if (txn->GetResult() != Result::RESULT_SUCCESS) {
      txn_manager.AbortTransaction();
      return false;
    }
  }

  assert(txn->GetResult() == Result::RESULT_SUCCESS);

  auto result = txn_manager.CommitTransaction();

  if (result == Result::RESULT_SUCCESS) {
    return true;
  } else {
    return false;
  }
}

}
}
}
