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

OrderStatusPlans PrepareOrderStatusPlan() {

  std::vector<expression::AbstractExpression *> runtime_keys;

  /////////////////////////////////////////////////////////
  // PLAN FOR CUSTOMER
  /////////////////////////////////////////////////////////


  std::vector<oid_t> customer_column_ids = {0, 3, 4, 5, 16};
  
  std::vector<oid_t> customer_pkey_column_ids = {0, 1, 2};
  std::vector<ExpressionType> customer_pexpr_types;
  customer_pexpr_types.push_back(
    ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);
  customer_pexpr_types.push_back(
    ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);
  customer_pexpr_types.push_back(
    ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);
  
  std::vector<Value> customer_pkey_values;
  
  auto customer_pkey_index = customer_table->GetIndexWithOid(customer_table_pkey_index_oid);

  planner::IndexScanPlan::IndexScanDesc customer_pindex_scan_desc(
    customer_pkey_index, customer_pkey_column_ids, customer_pexpr_types,
    customer_pkey_values, runtime_keys);

  planner::IndexScanPlan customer_pindex_scan_node(customer_table, nullptr,
    customer_column_ids, customer_pindex_scan_desc);

  executor::IndexScanExecutor *customer_pindex_scan_executor =
      new executor::IndexScanExecutor(&customer_pindex_scan_node, nullptr);

  customer_pindex_scan_executor->Init();


  std::vector<oid_t> customer_key_column_ids = {1, 2, 5};
  std::vector<ExpressionType> customer_expr_types;
  customer_expr_types.push_back(
    ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);
  customer_expr_types.push_back(
    ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);
  customer_expr_types.push_back(
    ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);
  
  std::vector<Value> customer_key_values;
  
  auto customer_skey_index = customer_table->GetIndexWithOid(customer_table_skey_index_oid);

  planner::IndexScanPlan::IndexScanDesc customer_index_scan_desc(
    customer_skey_index, customer_key_column_ids, customer_expr_types,
    customer_key_values, runtime_keys);

  planner::IndexScanPlan customer_index_scan_node(customer_table, nullptr,
    customer_column_ids, customer_index_scan_desc);

  executor::IndexScanExecutor *customer_index_scan_executor =
      new executor::IndexScanExecutor(&customer_index_scan_node, nullptr);

  customer_index_scan_executor->Init();

  // TODO: temporarily disable order_by executor,
  // as i haven't check whether it can directly perform caching. --yingjun
  // Construct order by executor
  // std::vector<oid_t> sort_keys = {1};
  // std::vector<bool> descend_flags = {false};
  // std::vector<oid_t> output_columns = {0,1,2,3,4};

  // planner::OrderByPlan customer_order_by_node(sort_keys, descend_flags, output_columns);

  // executor::OrderByExecutor customer_order_by_executor(&customer_order_by_node, context.get());

  // customer_order_by_executor.AddChild(&customer_index_scan_executor);


  /////////////////////////////////////////////////////////
  // PLAN FOR ORDERS
  /////////////////////////////////////////////////////////


  /*
    "ORDER_STATUS": {
    "getLastOrder": "SELECT O_ID, O_CARRIER_ID, O_ENTRY_D FROM ORDERS WHERE O_W_ID = ? AND O_D_ID = ? AND O_C_ID = ? ORDER BY O_ID DESC LIMIT 1", # w_id, d_id, c_id
    "getOrderLines": "SELECT OL_SUPPLY_W_ID, OL_I_ID, OL_QUANTITY, OL_AMOUNT, OL_DELIVERY_D FROM ORDER_LINE WHERE OL_W_ID = ? AND OL_D_ID = ? AND OL_O_ID = ?", # w_id, d_id, o_id
    }
   */

  // Construct index scan executor
  std::vector<oid_t> orders_key_column_ids = {1, 2, 3};
  std::vector<ExpressionType> orders_expr_types;
  
  orders_expr_types.push_back(ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);
  orders_expr_types.push_back(ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);
  orders_expr_types.push_back(ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);
  
  std::vector<Value> orders_key_values;
  
  // Get the index
  auto orders_skey_index = orders_table->GetIndexWithOid(orders_table_skey_index_oid);
  
  planner::IndexScanPlan::IndexScanDesc orders_index_scan_desc(
    orders_skey_index, orders_key_column_ids, orders_expr_types,
    orders_key_values, runtime_keys);

  std::vector<oid_t> orders_column_ids = {0, 4, 5};

  planner::IndexScanPlan orders_index_scan_node(orders_table, nullptr, 
    orders_column_ids, orders_index_scan_desc);

  executor::IndexScanExecutor *orders_index_scan_executor =
      new executor::IndexScanExecutor(&orders_index_scan_node, nullptr);

  orders_index_scan_executor->Init();

  // // Construct order by executor
  // std::vector<oid_t> sort_keys = {0};
  // std::vector<bool> descend_flags = {true};
  // std::vector<oid_t> output_columns = {0,1,2};

  // planner::OrderByPlan orders_order_by_node(sort_keys, descend_flags, output_columns);

  // executor::OrderByExecutor orders_order_by_executor(&orders_order_by_node, context.get());
  // orders_order_by_executor.AddChild(&orders_index_scan_executor);

  // // Construct limit executor
  // size_t limit = 1;
  // size_t offset = 0;
  // planner::LimitPlan limit_node(limit, offset);
  // executor::LimitExecutor limit_executor(&limit_node, context.get());
  // limit_executor.AddChild(&orders_order_by_executor);

  /////////////////////////////////////////////////////////
  // PLAN FOR ORDER_LINES
  /////////////////////////////////////////////////////////

  // Construct index scan executor
  std::vector<oid_t> order_line_key_column_ids = {COL_IDX_OL_W_ID, COL_IDX_OL_D_ID, COL_IDX_OL_O_ID};
  std::vector<ExpressionType> order_line_expr_types;
  
  order_line_expr_types.push_back(ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);
  order_line_expr_types.push_back(ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);
  order_line_expr_types.push_back(ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);
  
  std::vector<Value> order_line_key_values;

  auto order_line_pkey_index = order_line_table->GetIndexWithOid(order_line_table_pkey_index_oid);
  
  planner::IndexScanPlan::IndexScanDesc order_line_index_scan_desc(
    order_line_pkey_index, order_line_key_column_ids, order_line_expr_types,
    order_line_key_values, runtime_keys);

  std::vector<oid_t> order_line_column_ids = {COL_IDX_OL_SUPPLY_W_ID, COL_IDX_OL_I_ID, COL_IDX_OL_QUANTITY, COL_IDX_OL_AMOUNT, COL_IDX_OL_DELIVERY_D};

  planner::IndexScanPlan order_line_index_scan_node(order_line_table, nullptr, 
    order_line_column_ids, order_line_index_scan_desc);

  executor::IndexScanExecutor *order_line_index_scan_executor =
      new executor::IndexScanExecutor(&order_line_index_scan_node, nullptr);

  order_line_index_scan_executor->Init();

  /////////////////////////////////////////////////////////

  OrderStatusPlans order_status_plans;

  order_status_plans.customer_pindex_scan_executor_ = customer_pindex_scan_executor;

  order_status_plans.customer_index_scan_executor_ = customer_index_scan_executor;

  order_status_plans.orders_index_scan_executor_ = orders_index_scan_executor;
  
  // order_status_plans.order_line_index_scan_executor_ = order_line_index_scan_executor;

  return order_status_plans;
}

bool RunOrderStatus(OrderStatusPlans &order_status_plans){
  /*
    "ORDER_STATUS": {
    "getCustomerByCustomerId": "SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_BALANCE FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?", # w_id, d_id, c_id
    "getCustomersByLastName": "SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_BALANCE FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_LAST = ? ORDER BY C_FIRST", # w_id, d_id, c_last
    "getLastOrder": "SELECT O_ID, O_CARRIER_ID, O_ENTRY_D FROM ORDERS WHERE O_W_ID = ? AND O_D_ID = ? AND O_C_ID = ? ORDER BY O_ID DESC LIMIT 1", # w_id, d_id, c_id
    "getOrderLines": "SELECT OL_SUPPLY_W_ID, OL_I_ID, OL_QUANTITY, OL_AMOUNT, OL_DELIVERY_D FROM ORDER_LINE WHERE OL_W_ID = ? AND OL_D_ID = ? AND OL_O_ID = ?", # w_id, d_id, o_id
    }
   */

  LOG_TRACE("-------------------------------------");

  /////////////////////////////////////////////////////////
  // PREPARE ARGUMENTS
  /////////////////////////////////////////////////////////

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

  /////////////////////////////////////////////////////////
  // BEGIN TRANSACTION
  /////////////////////////////////////////////////////////

  std::unique_ptr<executor::ExecutorContext> context(
    new executor::ExecutorContext(nullptr));

  order_status_plans.SetContext(context.get());

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

  auto txn = txn_manager.BeginTransaction();

  // Run queries
  if (c_id != -1) {
    LOG_TRACE("getCustomerByCustomerId: SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_BALANCE FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?  # w_id, d_id, c_id");
    // Construct index scan executor
    
    order_status_plans.customer_pindex_scan_executor_->ResetState();

    std::vector<Value> customer_key_values;

    customer_key_values.push_back(ValueFactory::GetIntegerValue(c_id));
    customer_key_values.push_back(ValueFactory::GetIntegerValue(d_id));
    customer_key_values.push_back(ValueFactory::GetIntegerValue(w_id));

    order_status_plans.customer_pindex_scan_executor_->SetValues(customer_key_values);

    auto result = ExecuteReadTest(order_status_plans.customer_pindex_scan_executor_);

    if (txn->GetResult() != Result::RESULT_SUCCESS) {
      LOG_INFO("abort transaction");
      txn_manager.AbortTransaction();
      return false;
    }

    assert(result.size() > 0);
    assert(result[0].size() > 0);

  } else {
    LOG_TRACE("getCustomersByLastName: SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_BALANCE FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_LAST = ? ORDER BY C_FIRST, # w_id, d_id, c_last");
    // Construct index scan executor
    
    order_status_plans.customer_index_scan_executor_->ResetState();

    std::vector<Value> customer_key_values;
    
    customer_key_values.push_back(ValueFactory::GetIntegerValue(d_id));
    customer_key_values.push_back(ValueFactory::GetIntegerValue(w_id));
    customer_key_values.push_back(ValueFactory::GetStringValue(c_last));

    order_status_plans.customer_index_scan_executor_->SetValues(customer_key_values);

    auto result = ExecuteReadTest(order_status_plans.customer_index_scan_executor_);

    if (txn->GetResult() != Result::RESULT_SUCCESS) {
      LOG_INFO("abort transaction");
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

  // LOG_INFO("getLastOrder: SELECT O_ID, O_CARRIER_ID, O_ENTRY_D FROM ORDERS WHERE O_W_ID = %d AND O_D_ID = %d AND O_C_ID = %d ORDER BY O_ID DESC LIMIT 1, # w_id, d_id, c_id", w_id, d_id, c_id);

  // // Construct index scan executor

  // order_status_plans.orders_index_scan_executor_->ResetState();

  // std::vector<Value> orders_key_values;

  // orders_key_values.push_back(ValueFactory::GetIntegerValue(c_id));
  // orders_key_values.push_back(ValueFactory::GetIntegerValue(d_id));
  // orders_key_values.push_back(ValueFactory::GetIntegerValue(w_id));

  // order_status_plans.orders_index_scan_executor_->SetValues(orders_key_values);

  // auto orders = ExecuteReadTest(order_status_plans.orders_index_scan_executor_);

  // if (txn->GetResult() != Result::RESULT_SUCCESS) {
  //   LOG_INFO("abort transaction");
  //   txn_manager.AbortTransaction();
  //   return false;
  // }

  //LOG_INFO("o_id=%d", ValuePeeker::PeekInteger(orders[0][0]));

  // if (orders.size() != 0) {
  //   LOG_TRACE("getOrderLines: SELECT OL_SUPPLY_W_ID, OL_I_ID, OL_QUANTITY, OL_AMOUNT, OL_DELIVERY_D FROM ORDER_LINE WHERE OL_W_ID = ? AND OL_D_ID = ? AND OL_O_ID = ?, # w_id, d_id, o_id");
    
  //   // Construct index scan executor

  //   order_status_plans.order_line_index_scan_executor_->ResetState();

  //   std::vector<Value> order_line_key_values;

  //   order_line_key_values.push_back(ValueFactory::GetIntegerValue(w_id));
  //   order_line_key_values.push_back(ValueFactory::GetIntegerValue(d_id));
  //   order_line_key_values.push_back(orders[0][0]);

  //   order_status_plans.order_line_index_scan_executor_->SetValues(order_line_key_values);

  //   ExecuteReadTest(order_status_plans.order_line_index_scan_executor_);

  //   if (txn->GetResult() != Result::RESULT_SUCCESS) {
  //     LOG_INFO("abort transaction");
  //     txn_manager.AbortTransaction();
  //     return false;
  //   }
  // }

  assert(txn->GetResult() == Result::RESULT_SUCCESS);

  auto result = txn_manager.CommitTransaction();

  if (result == Result::RESULT_SUCCESS) {
    return true;
  } else {
    assert(result == Result::RESULT_ABORTED || 
           result == Result::RESULT_FAILURE);
    return false;
  }
}

}
}
}