//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tpcc_order_status.cpp
//
// Identification: src/main/tpcc/tpcc_order_status.cpp
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


#include "benchmark/tpcc/tpcc_workload.h"
#include "benchmark/tpcc/tpcc_configuration.h"
#include "benchmark/tpcc/tpcc_loader.h"

#include "catalog/manager.h"
#include "catalog/schema.h"

#include "common/internal_types.h"
#include "type/value.h"
#include "type/value_factory.h"
#include "common/logger.h"
#include "common/timer.h"
#include "common/generator.h"

#include "concurrency/transaction_context.h"
#include "concurrency/transaction_manager_factory.h"

#include "executor/executor_context.h"
#include "executor/abstract_executor.h"
#include "executor/logical_tile.h"
#include "executor/logical_tile_factory.h"
#include "executor/materialization_executor.h"
#include "executor/update_executor.h"
#include "executor/index_scan_executor.h"
#include "executor/insert_executor.h"
#include "executor/order_by_executor.h"
#include "executor/limit_executor.h"

#include "expression/abstract_expression.h"
#include "expression/constant_value_expression.h"
#include "expression/tuple_value_expression.h"
#include "expression/comparison_expression.h"
#include "expression/expression_util.h"
#include "common/container_tuple.h"

#include "index/index_factory.h"

#include "logging/log_manager.h"

#include "planner/abstract_plan.h"
#include "planner/materialization_plan.h"
#include "planner/insert_plan.h"
#include "planner/update_plan.h"
#include "planner/index_scan_plan.h"
#include "planner/order_by_plan.h"
#include "planner/limit_plan.h"

#include "storage/data_table.h"
#include "storage/table_factory.h"



namespace peloton {
namespace benchmark {
namespace tpcc {

bool RunOrderStatus(const size_t &thread_id){
  /*
    "ORDER_STATUS": {
    "getCustomerByCustomerId": "SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_BALANCE FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?", # w_id, d_id, c_id
    "getCustomersByLastName": "SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_BALANCE FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_LAST = ? ORDER BY C_FIRST", # w_id, d_id, c_last
    "getLastOrder": "SELECT O_ID, O_CARRIER_ID, O_ENTRY_D FROM ORDERS WHERE O_W_ID = ? AND O_D_ID = ? AND O_C_ID = ? ORDER BY O_ID DESC LIMIT 1", # w_id, d_id, c_id
    "getOrderLines": "SELECT OL_SUPPLY_W_ID, OL_I_ID, OL_QUANTITY, OL_AMOUNT, OL_DELIVERY_D FROM ORDER_LINE WHERE OL_W_ID = ? AND OL_D_ID = ? AND OL_O_ID = ?", # w_id, d_id, o_id
    }
   */

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction(thread_id);

  std::unique_ptr<executor::ExecutorContext> context(
    new executor::ExecutorContext(txn));

  // Generate w_id, d_id, c_id, c_last
  //int w_id = GetRandomInteger(0, state.warehouse_count - 1);
  int w_id = GenerateWarehouseId(thread_id);
  int d_id = GetRandomInteger(0, state.districts_per_warehouse - 1);

  int c_id = -1;
  std::string c_last;

  // if (GetRandomInteger(1, 100) <= 60) {
  //   c_last = GetRandomLastName(state.customers_per_district);
  // } else {
    c_id = GetNURand(1023, 0, state.customers_per_district - 1);
  // }

  // Run queries
  if (c_id != -1) {
    LOG_TRACE("getCustomerByCustomerId: SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_BALANCE FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?  # w_id, d_id, c_id");
    // Construct index scan executor
    std::vector<oid_t> customer_column_ids = 
      {COL_IDX_C_ID, COL_IDX_C_FIRST, COL_IDX_C_MIDDLE, 
       COL_IDX_C_LAST, COL_IDX_C_BALANCE};
    std::vector<oid_t> customer_key_column_ids = {COL_IDX_C_W_ID, COL_IDX_C_D_ID, COL_IDX_C_ID};
    std::vector<ExpressionType> customer_expr_types;
    std::vector<type::Value > customer_key_values;
    std::vector<expression::AbstractExpression *> runtime_keys;

    customer_expr_types.push_back(ExpressionType::COMPARE_EQUAL);
    customer_key_values.push_back(type::ValueFactory::GetIntegerValue(w_id).Copy());
    customer_expr_types.push_back(
      ExpressionType::COMPARE_EQUAL);
    customer_key_values.push_back(type::ValueFactory::GetIntegerValue(d_id).Copy());
    customer_expr_types.push_back(
      ExpressionType::COMPARE_EQUAL);
    customer_key_values.push_back(type::ValueFactory::GetIntegerValue(c_id).Copy());

    auto customer_pkey_index = customer_table->GetIndexWithOid(customer_table_pkey_index_oid);

    planner::IndexScanPlan::IndexScanDesc customer_index_scan_desc(customer_pkey_index, customer_key_column_ids, customer_expr_types,
      customer_key_values, runtime_keys);

    auto predicate = nullptr;
    planner::IndexScanPlan customer_index_scan_node(customer_table, predicate,
      customer_column_ids, customer_index_scan_desc);

    executor::IndexScanExecutor customer_index_scan_executor(&customer_index_scan_node, context.get());

    auto result = ExecuteRead(&customer_index_scan_executor);
    if (txn->GetResult() != ResultType::SUCCESS) {
      txn_manager.AbortTransaction(txn);
      return false;
    }

    if (result.size() == 0) {
      LOG_ERROR("wrong result size : %lu", result.size());
      PL_ASSERT(false);
    }
    if (result[0].size() == 0) {
      LOG_ERROR("wrong result[0] size : %lu", result[0].size());
      PL_ASSERT(false);
    }
  } else {
    LOG_ERROR("getCustomersByLastName: SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_BALANCE FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_LAST = ? ORDER BY C_FIRST, # w_id, d_id, c_last");
    // Construct index scan executor
    std::vector<oid_t> customer_column_ids = 
      {COL_IDX_C_ID, COL_IDX_C_FIRST, COL_IDX_C_MIDDLE, 
       COL_IDX_C_LAST, COL_IDX_C_BALANCE};
    std::vector<oid_t> customer_key_column_ids = {COL_IDX_C_W_ID, COL_IDX_C_D_ID, COL_IDX_C_LAST};
    std::vector<ExpressionType> customer_expr_types;
    std::vector<type::Value > customer_key_values;
    std::vector<expression::AbstractExpression *> runtime_keys;

    customer_expr_types.push_back(ExpressionType::COMPARE_EQUAL);
    customer_key_values.push_back(type::ValueFactory::GetIntegerValue(w_id).Copy());
    customer_expr_types.push_back(
      ExpressionType::COMPARE_EQUAL);
    customer_key_values.push_back(type::ValueFactory::GetIntegerValue(d_id).Copy());
    customer_expr_types.push_back(
      ExpressionType::COMPARE_EQUAL);
    customer_key_values.push_back(type::ValueFactory::GetVarcharValue(c_last).Copy());

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
    
    auto result = ExecuteRead(&customer_order_by_executor);
    if (txn->GetResult() != ResultType::SUCCESS) {
      txn_manager.AbortTransaction(txn);
      return false;
    }

    PL_ASSERT(result.size() > 0);
    // Get the middle one
    size_t name_count = result.size();
    auto &customer = result[name_count/2];
    PL_ASSERT(customer.size() > 0);
    c_id = type::ValuePeeker::PeekInteger(customer[0]);
  }

  if (c_id < 0) {
    LOG_ERROR("wrong c_id");
    PL_ASSERT(false);
  }

  LOG_TRACE("getLastOrder: SELECT O_ID, O_CARRIER_ID, O_ENTRY_D FROM ORDERS WHERE O_W_ID = ? AND O_D_ID = ? AND O_C_ID = ? ORDER BY O_ID DESC LIMIT 1, # w_id, d_id, c_id");

  // Construct index scan executor
  std::vector<oid_t> orders_column_ids = {COL_IDX_O_ID
  , COL_IDX_O_CARRIER_ID, COL_IDX_O_ENTRY_D};
  std::vector<oid_t> orders_key_column_ids = {COL_IDX_O_W_ID, COL_IDX_O_D_ID, COL_IDX_O_C_ID};
  std::vector<ExpressionType> orders_expr_types;
  std::vector<type::Value > orders_key_values;
  std::vector<expression::AbstractExpression *> runtime_keys;

  orders_expr_types.push_back(ExpressionType::COMPARE_EQUAL);
  orders_key_values.push_back(type::ValueFactory::GetIntegerValue(w_id).Copy());
  orders_expr_types.push_back(ExpressionType::COMPARE_EQUAL);
  orders_key_values.push_back(type::ValueFactory::GetIntegerValue(d_id).Copy());
  orders_expr_types.push_back(ExpressionType::COMPARE_EQUAL);
  orders_key_values.push_back(type::ValueFactory::GetIntegerValue(c_id).Copy());

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

  auto orders = ExecuteRead(&orders_order_by_executor);
  if (txn->GetResult() != ResultType::SUCCESS) {
    txn_manager.AbortTransaction(txn);
    return false;
  }

  if (orders.size() != 0) {
    LOG_TRACE("getOrderLines: SELECT OL_SUPPLY_W_ID, OL_I_ID, OL_QUANTITY, OL_AMOUNT, OL_DELIVERY_D FROM ORDER_LINE WHERE OL_W_ID = ? AND OL_D_ID = ? AND OL_O_ID = ?, # w_id, d_id, o_id");
    
    // Construct index scan executor
    std::vector<oid_t> order_line_column_ids = {COL_IDX_OL_SUPPLY_W_ID, COL_IDX_OL_I_ID, COL_IDX_OL_QUANTITY, COL_IDX_OL_AMOUNT, COL_IDX_OL_DELIVERY_D};
    std::vector<oid_t> order_line_key_column_ids = {COL_IDX_OL_W_ID, COL_IDX_OL_D_ID, COL_IDX_OL_O_ID};
    std::vector<ExpressionType> order_line_expr_types;
    std::vector<type::Value > order_line_key_values;

    order_line_expr_types.push_back(ExpressionType::COMPARE_EQUAL);
    order_line_key_values.push_back(type::ValueFactory::GetIntegerValue(w_id).Copy());
    order_line_expr_types.push_back(ExpressionType::COMPARE_EQUAL);
    order_line_key_values.push_back(type::ValueFactory::GetIntegerValue(d_id).Copy());
    order_line_expr_types.push_back(ExpressionType::COMPARE_EQUAL);
    order_line_key_values.push_back(orders[0][0]);

    auto order_line_skey_index = order_line_table->GetIndexWithOid(order_line_table_skey_index_oid);
    planner::IndexScanPlan::IndexScanDesc order_line_index_scan_desc(
      order_line_skey_index, order_line_key_column_ids, order_line_expr_types,
      order_line_key_values, runtime_keys);

    predicate = nullptr;

    planner::IndexScanPlan order_line_index_scan_node(order_line_table,
      predicate, order_line_column_ids, order_line_index_scan_desc);

    executor::IndexScanExecutor order_line_index_scan_executor(&order_line_index_scan_node, context.get());

    ExecuteRead(&order_line_index_scan_executor);
    if (txn->GetResult() != ResultType::SUCCESS) {
      txn_manager.AbortTransaction(txn);
      return false;
    }
  }

  PL_ASSERT(txn->GetResult() == ResultType::SUCCESS);

  auto result = txn_manager.CommitTransaction(txn);

  if (result == ResultType::SUCCESS) {
    return true;
  } else {
    return false;
  }
}

}
}
}
