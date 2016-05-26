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
#include "backend/executor/order_by_executor.h"
#include "backend/executor/limit_executor.h"

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
#include "backend/planner/order_by_plan.h"
#include "backend/planner/limit_plan.h"

#include "backend/storage/data_table.h"
#include "backend/storage/table_factory.h"



namespace peloton {
namespace benchmark {
namespace tpcc {

bool RunOrderStatus(){
  /*
    "ORDER_STATUS": {
    "getCustomerByCustomerId": "SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_BALANCE FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?", # w_id, d_id, c_id
    "getCustomersByLastName": "SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_BALANCE FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_LAST = ? ORDER BY C_FIRST", # w_id, d_id, c_last
    "getLastOrder": "SELECT O_ID, O_CARRIER_ID, O_ENTRY_D FROM ORDERS WHERE O_W_ID = ? AND O_D_ID = ? AND O_C_ID = ? ORDER BY O_ID DESC LIMIT 1", # w_id, d_id, c_id
    "getOrderLines": "SELECT OL_SUPPLY_W_ID, OL_I_ID, OL_QUANTITY, OL_AMOUNT, OL_DELIVERY_D FROM ORDER_LINE WHERE OL_W_ID = ? AND OL_D_ID = ? AND OL_O_ID = ?", # w_id, d_id, o_id
    }
   */

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  std::unique_ptr<executor::ExecutorContext> context(
    new executor::ExecutorContext(txn));

  // Generate w_id, d_id, c_id, c_last
  int w_id = GetRandomInteger(0, state.warehouse_count - 1);
  int d_id = GetRandomInteger(0, state.districts_per_warehouse - 1);

  int c_id = -1;
  std::string c_last;

  if (GetRandomInteger(1, 100) <= 60) {
    c_last = GetRandomLastName(state.customers_per_district);
  } else {
    c_id = GetNURand(1023, 0, state.customers_per_district - 1);
  }

  // Run queries
  if (c_id != -1) {
    LOG_INFO("getCustomerByCustomerId: SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_BALANCE FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?  # w_id, d_id, c_id");
    // Construct index scan executor
    std::vector<oid_t> customer_column_ids = 
      {COL_IDX_C_ID, COL_IDX_C_FIRST, COL_IDX_C_MIDDLE, 
       COL_IDX_C_LAST, COL_IDX_C_BALANCE};
    std::vector<oid_t> customer_key_column_ids = {COL_IDX_C_W_ID, COL_IDX_C_D_ID, COL_IDX_C_ID};
    std::vector<ExpressionType> customer_expr_types;
    std::vector<Value> customer_key_values;
    std::vector<expression::AbstractExpression *> runtime_keys;

    customer_expr_types.push_back(ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);
    customer_key_values.push_back(ValueFactory::GetSmallIntValue(w_id));
    customer_expr_types.push_back(
      ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);
    customer_key_values.push_back(ValueFactory::GetTinyIntValue(d_id));
    customer_expr_types.push_back(
      ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);
    customer_key_values.push_back(ValueFactory::GetIntegerValue(c_id));

    auto customer_pkey_index = customer_table->GetIndexWithOid(customer_table_pkey_index_oid);

    planner::IndexScanPlan::IndexScanDesc customer_index_scan_desc(customer_pkey_index, customer_key_column_ids, customer_expr_types,
      customer_key_values, runtime_keys);

    auto predicate = nullptr;
    planner::IndexScanPlan customer_index_scan_node(customer_table, predicate,
      customer_column_ids, customer_index_scan_desc);

    executor::IndexScanExecutor customer_index_scan_executor(&customer_index_scan_node, context.get());

    auto result = ExecuteReadTest(&customer_index_scan_executor);
    if (txn->GetResult() != Result::RESULT_SUCCESS) {
      txn_manager.AbortTransaction();
      return false;
    }

    assert(result.size() > 0);
    assert(result[0].size() > 0);
  } else {
    LOG_INFO("getCustomersByLastName: SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_BALANCE FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_LAST = ? ORDER BY C_FIRST, # w_id, d_id, c_last");
    // Construct index scan executor
    std::vector<oid_t> customer_column_ids = 
      {COL_IDX_C_ID, COL_IDX_C_FIRST, COL_IDX_C_MIDDLE, 
       COL_IDX_C_LAST, COL_IDX_C_BALANCE};
    std::vector<oid_t> customer_key_column_ids = {COL_IDX_C_W_ID, COL_IDX_C_D_ID, COL_IDX_C_LAST};
    std::vector<ExpressionType> customer_expr_types;
    std::vector<Value> customer_key_values;
    std::vector<expression::AbstractExpression *> runtime_keys;

    customer_expr_types.push_back(ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);
    customer_key_values.push_back(ValueFactory::GetSmallIntValue(w_id));
    customer_expr_types.push_back(
      ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);
    customer_key_values.push_back(ValueFactory::GetTinyIntValue(d_id));
    customer_expr_types.push_back(
      ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);
    customer_key_values.push_back(ValueFactory::GetStringValue(c_last));

    auto customer_skey_index = customer_table->GetIndexWithOid(customer_table_skey_index_oid);

    planner::IndexScanPlan::IndexScanDesc customer_index_scan_desc(customer_skey_index, customer_key_column_ids, customer_expr_types,
      customer_key_values, runtime_keys);

    auto predicate = nullptr;
    planner::IndexScanPlan customer_index_scan_node(customer_table, predicate,
      customer_column_ids, customer_index_scan_desc);

    executor::IndexScanExecutor customer_index_scan_executor(&customer_index_scan_node, context.get());

    // Construct order by executor
    std::vector<oid_t> sort_keys = {1};
    std::vector<bool> descend_flags = {false};
    std::vector<oid_t> output_columns = {0,1,2,3,4};

    planner::OrderByPlan customer_order_by_node(sort_keys, descend_flags, output_columns);

    executor::OrderByExecutor customer_order_by_executor(&customer_order_by_node, context.get());

    customer_order_by_executor.AddChild(&customer_index_scan_executor);

    auto result = ExecuteReadTest(&customer_order_by_executor);
    if (txn->GetResult() != Result::RESULT_SUCCESS) {
      txn_manager.AbortTransaction();
      return false;
    }

    assert(result.size() > 0);
    // Get the middle one
    size_t name_count = result.size();
    auto &customer = result[name_count/2];
    assert(customer.size() > 0);
    c_id = ValuePeeker::PeekInteger(customer[0]);
  }

  assert(c_id >= 0);

  LOG_INFO("getLastOrder: SELECT O_ID, O_CARRIER_ID, O_ENTRY_D FROM ORDERS WHERE O_W_ID = ? AND O_D_ID = ? AND O_C_ID = ? ORDER BY O_ID DESC LIMIT 1, # w_id, d_id, c_id");

  // Construct index scan executor
  std::vector<oid_t> orders_column_ids = {COL_IDX_O_ID
  , COL_IDX_O_CARRIER_ID, COL_IDX_O_ENTRY_D};
  std::vector<oid_t> orders_key_column_ids = {COL_IDX_O_W_ID, COL_IDX_O_D_ID, COL_IDX_O_C_ID};
  std::vector<ExpressionType> orders_expr_types;
  std::vector<Value> orders_key_values;
  std::vector<expression::AbstractExpression *> runtime_keys;

  orders_expr_types.push_back(ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);
  orders_key_values.push_back(ValueFactory::GetSmallIntValue(w_id));
  orders_expr_types.push_back(ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);
  orders_key_values.push_back(ValueFactory::GetTinyIntValue(d_id));
  orders_expr_types.push_back(ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);
  orders_key_values.push_back(ValueFactory::GetIntegerValue(c_id));

  // Get the index
  auto orders_skey_index = orders_table->GetIndexWithOid(orders_table_skey_index_oid);
  planner::IndexScanPlan::IndexScanDesc orders_index_scan_desc(
    orders_skey_index, orders_key_column_ids, orders_expr_types,
    orders_key_values, runtime_keys);

  auto predicate = nullptr;

  planner::IndexScanPlan orders_index_scan_node(orders_table,
    predicate, orders_column_ids, orders_index_scan_desc);

  executor::IndexScanExecutor orders_index_scan_executor(
    &orders_index_scan_node, context.get());

  // Construct order by executor
  std::vector<oid_t> sort_keys = {0};
  std::vector<bool> descend_flags = {true};
  std::vector<oid_t> output_columns = {0,1,2};

  planner::OrderByPlan orders_order_by_node(sort_keys, descend_flags, output_columns);

  executor::OrderByExecutor orders_order_by_executor(&orders_order_by_node, context.get());
  orders_order_by_executor.AddChild(&orders_index_scan_executor);

  // Construct limit executor
  size_t limit = 1;
  size_t offset = 0;
  planner::LimitPlan limit_node(limit, offset);
  executor::LimitExecutor limit_executor(&limit_node, context.get());
  limit_executor.AddChild(&orders_order_by_executor);

  auto orders = ExecuteReadTest(&orders_order_by_executor);
  if (txn->GetResult() != Result::RESULT_SUCCESS) {
    txn_manager.AbortTransaction();
    return false;
  }

  if (orders.size() != 0) {
    LOG_INFO("getOrderLines: SELECT OL_SUPPLY_W_ID, OL_I_ID, OL_QUANTITY, OL_AMOUNT, OL_DELIVERY_D FROM ORDER_LINE WHERE OL_W_ID = ? AND OL_D_ID = ? AND OL_O_ID = ?, # w_id, d_id, o_id");
    
    // Construct index scan executor
    std::vector<oid_t> order_line_column_ids = {COL_IDX_OL_SUPPLY_W_ID, COL_IDX_OL_I_ID, COL_IDX_OL_QUANTITY, COL_IDX_OL_AMOUNT, COL_IDX_OL_DELIVERY_D};
    std::vector<oid_t> order_line_key_column_ids = {COL_IDX_OL_W_ID, COL_IDX_OL_D_ID, COL_IDX_OL_O_ID};
    std::vector<ExpressionType> order_line_expr_types;
    std::vector<Value> order_line_key_values;

    order_line_expr_types.push_back(ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);
    order_line_key_values.push_back(ValueFactory::GetSmallIntValue(w_id));
    order_line_expr_types.push_back(ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);
    order_line_key_values.push_back(ValueFactory::GetTinyIntValue(d_id));
    order_line_expr_types.push_back(ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);
    order_line_key_values.push_back(orders[0][0]);

    auto order_line_pkey_index = order_line_table->GetIndexWithOid(order_line_table_pkey_index_oid);
    planner::IndexScanPlan::IndexScanDesc order_line_index_scan_desc(
      order_line_pkey_index, order_line_key_column_ids, order_line_expr_types,
      order_line_key_values, runtime_keys);

    predicate = nullptr;

    planner::IndexScanPlan order_line_index_scan_node(order_line_table,
      predicate, order_line_column_ids, order_line_index_scan_desc);

    executor::IndexScanExecutor order_line_index_scan_executor(&order_line_index_scan_node, context.get());

    ExecuteReadTest(&order_line_index_scan_executor);
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